// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::Visit;

fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn src_dir() -> std::path::PathBuf {
    Path::new(manifest_dir()).join("src")
}

fn rs_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(rs_files(&path));
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn rel(path: &Path) -> String {
    path.strip_prefix(manifest_dir())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn item_attributes(item: &syn::Item) -> &[syn::Attribute] {
    match item {
        syn::Item::Const(item) => &item.attrs,
        syn::Item::Enum(item) => &item.attrs,
        syn::Item::ExternCrate(item) => &item.attrs,
        syn::Item::Fn(item) => &item.attrs,
        syn::Item::ForeignMod(item) => &item.attrs,
        syn::Item::Impl(item) => &item.attrs,
        syn::Item::Macro(item) => &item.attrs,
        syn::Item::Mod(item) => &item.attrs,
        syn::Item::Static(item) => &item.attrs,
        syn::Item::Struct(item) => &item.attrs,
        syn::Item::Trait(item) => &item.attrs,
        syn::Item::TraitAlias(item) => &item.attrs,
        syn::Item::Type(item) => &item.attrs,
        syn::Item::Union(item) => &item.attrs,
        syn::Item::Use(item) => &item.attrs,
        syn::Item::Verbatim(_) => &[],
        _ => &[],
    }
}

fn cfg_possibilities_without_test(meta: &syn::Meta) -> (bool, bool) {
    match meta {
        syn::Meta::Path(path) if path.is_ident("test") => (false, true),
        syn::Meta::Path(_) | syn::Meta::NameValue(_) => (true, true),
        syn::Meta::List(list) => {
            let Some(operator) = list.path.get_ident().map(ToString::to_string) else {
                return (true, true);
            };
            if !matches!(operator.as_str(), "all" | "any" | "not") {
                return (true, true);
            }
            let Ok(children) = Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated
                .parse2(list.tokens.clone())
            else {
                return (true, true);
            };
            let possibilities = children
                .iter()
                .map(cfg_possibilities_without_test)
                .collect::<Vec<_>>();
            match operator.as_str() {
                "all" => (
                    possibilities.iter().all(|(can_be_true, _)| *can_be_true),
                    possibilities.iter().any(|(_, can_be_false)| *can_be_false),
                ),
                "any" => (
                    possibilities.iter().any(|(can_be_true, _)| *can_be_true),
                    possibilities.iter().all(|(_, can_be_false)| *can_be_false),
                ),
                "not" if possibilities.len() == 1 => {
                    let (can_be_true, can_be_false) = possibilities[0];
                    (can_be_false, can_be_true)
                }
                _ => (true, true),
            }
        }
    }
}

fn requires_test_configuration(attributes: &[syn::Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        if !attribute.path().is_ident("cfg") {
            return false;
        }
        attribute
            .parse_args::<syn::Meta>()
            .is_ok_and(|predicate| !cfg_possibilities_without_test(&predicate).0)
    })
}

fn impl_item_attributes(item: &syn::ImplItem) -> &[syn::Attribute] {
    match item {
        syn::ImplItem::Const(item) => &item.attrs,
        syn::ImplItem::Fn(item) => &item.attrs,
        syn::ImplItem::Type(item) => &item.attrs,
        syn::ImplItem::Macro(item) => &item.attrs,
        syn::ImplItem::Verbatim(_) => &[],
        _ => &[],
    }
}

fn trait_item_attributes(item: &syn::TraitItem) -> &[syn::Attribute] {
    match item {
        syn::TraitItem::Const(item) => &item.attrs,
        syn::TraitItem::Fn(item) => &item.attrs,
        syn::TraitItem::Type(item) => &item.attrs,
        syn::TraitItem::Macro(item) => &item.attrs,
        syn::TraitItem::Verbatim(_) => &[],
        _ => &[],
    }
}

fn foreign_item_attributes(item: &syn::ForeignItem) -> &[syn::Attribute] {
    match item {
        syn::ForeignItem::Fn(item) => &item.attrs,
        syn::ForeignItem::Static(item) => &item.attrs,
        syn::ForeignItem::Type(item) => &item.attrs,
        syn::ForeignItem::Macro(item) => &item.attrs,
        syn::ForeignItem::Verbatim(_) => &[],
        _ => &[],
    }
}

fn expression_attributes(expression: &syn::Expr) -> &[syn::Attribute] {
    match expression {
        syn::Expr::Array(expression) => &expression.attrs,
        syn::Expr::Assign(expression) => &expression.attrs,
        syn::Expr::Async(expression) => &expression.attrs,
        syn::Expr::Await(expression) => &expression.attrs,
        syn::Expr::Binary(expression) => &expression.attrs,
        syn::Expr::Block(expression) => &expression.attrs,
        syn::Expr::Break(expression) => &expression.attrs,
        syn::Expr::Call(expression) => &expression.attrs,
        syn::Expr::Cast(expression) => &expression.attrs,
        syn::Expr::Closure(expression) => &expression.attrs,
        syn::Expr::Const(expression) => &expression.attrs,
        syn::Expr::Continue(expression) => &expression.attrs,
        syn::Expr::Field(expression) => &expression.attrs,
        syn::Expr::ForLoop(expression) => &expression.attrs,
        syn::Expr::Group(expression) => &expression.attrs,
        syn::Expr::If(expression) => &expression.attrs,
        syn::Expr::Index(expression) => &expression.attrs,
        syn::Expr::Infer(expression) => &expression.attrs,
        syn::Expr::Let(expression) => &expression.attrs,
        syn::Expr::Lit(expression) => &expression.attrs,
        syn::Expr::Loop(expression) => &expression.attrs,
        syn::Expr::Macro(expression) => &expression.attrs,
        syn::Expr::Match(expression) => &expression.attrs,
        syn::Expr::MethodCall(expression) => &expression.attrs,
        syn::Expr::Paren(expression) => &expression.attrs,
        syn::Expr::Path(expression) => &expression.attrs,
        syn::Expr::Range(expression) => &expression.attrs,
        syn::Expr::RawAddr(expression) => &expression.attrs,
        syn::Expr::Reference(expression) => &expression.attrs,
        syn::Expr::Repeat(expression) => &expression.attrs,
        syn::Expr::Return(expression) => &expression.attrs,
        syn::Expr::Struct(expression) => &expression.attrs,
        syn::Expr::Try(expression) => &expression.attrs,
        syn::Expr::TryBlock(expression) => &expression.attrs,
        syn::Expr::Tuple(expression) => &expression.attrs,
        syn::Expr::Unary(expression) => &expression.attrs,
        syn::Expr::Unsafe(expression) => &expression.attrs,
        syn::Expr::Verbatim(_) => &[],
        syn::Expr::While(expression) => &expression.attrs,
        syn::Expr::Yield(expression) => &expression.attrs,
        _ => &[],
    }
}

#[derive(Default)]
struct ProductionInventory {
    identifiers: Vec<String>,
    paths: Vec<Vec<String>>,
    type_declarations: BTreeMap<String, usize>,
    function_declarations: BTreeMap<String, usize>,
}

impl ProductionInventory {
    fn from_source(source: &str, path: &str) -> Self {
        let file = syn::parse_file(source)
            .unwrap_or_else(|error| panic!("parse production Rust source {path}: {error}"));
        let mut inventory = Self::default();
        inventory.visit_file(&file);
        inventory
    }

    fn contains_identifier(&self, identifier: &str) -> bool {
        self.identifiers
            .iter()
            .any(|candidate| candidate == identifier)
    }

    fn contains_path_suffix(&self, suffix: &[&str]) -> bool {
        self.paths.iter().any(|path| {
            path.len() >= suffix.len()
                && path[path.len() - suffix.len()..]
                    .iter()
                    .map(String::as_str)
                    .eq(suffix.iter().copied())
        })
    }

    fn type_declaration_count(&self, name: &str) -> usize {
        self.type_declarations.get(name).copied().unwrap_or(0)
    }

    fn function_declaration_count(&self, name: &str) -> usize {
        self.function_declarations.get(name).copied().unwrap_or(0)
    }

    fn record_macro_tokens(&mut self, tokens: &impl ToString) {
        let text = tokens.to_string();
        for identifier in
            text.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        {
            if !identifier.is_empty()
                && identifier
                    .as_bytes()
                    .first()
                    .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
            {
                self.identifiers.push(identifier.to_string());
            }
        }

        let compact = text
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        for candidate in compact.split(|character: char| {
            !character.is_ascii_alphanumeric() && character != '_' && character != ':'
        }) {
            let path = candidate
                .split("::")
                .filter(|segment| !segment.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            if path.len() >= 2 {
                self.paths.push(path);
            }
        }
    }
}

impl<'ast> Visit<'ast> for ProductionInventory {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        if requires_test_configuration(item_attributes(item)) {
            return;
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        if requires_test_configuration(impl_item_attributes(item)) {
            return;
        }
        syn::visit::visit_impl_item(self, item);
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        if requires_test_configuration(trait_item_attributes(item)) {
            return;
        }
        syn::visit::visit_trait_item(self, item);
    }

    fn visit_foreign_item(&mut self, item: &'ast syn::ForeignItem) {
        if requires_test_configuration(foreign_item_attributes(item)) {
            return;
        }
        syn::visit::visit_foreign_item(self, item);
    }

    fn visit_field(&mut self, field: &'ast syn::Field) {
        if requires_test_configuration(&field.attrs) {
            return;
        }
        syn::visit::visit_field(self, field);
    }

    fn visit_variant(&mut self, variant: &'ast syn::Variant) {
        if requires_test_configuration(&variant.attrs) {
            return;
        }
        syn::visit::visit_variant(self, variant);
    }

    fn visit_arm(&mut self, arm: &'ast syn::Arm) {
        if requires_test_configuration(&arm.attrs) {
            return;
        }
        syn::visit::visit_arm(self, arm);
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        if requires_test_configuration(&local.attrs) {
            return;
        }
        syn::visit::visit_local(self, local);
    }

    fn visit_expr(&mut self, expression: &'ast syn::Expr) {
        if requires_test_configuration(expression_attributes(expression)) {
            return;
        }
        syn::visit::visit_expr(self, expression);
    }

    fn visit_stmt_macro(&mut self, statement: &'ast syn::StmtMacro) {
        if requires_test_configuration(&statement.attrs) {
            return;
        }
        syn::visit::visit_stmt_macro(self, statement);
    }

    fn visit_ident(&mut self, identifier: &'ast syn::Ident) {
        self.identifiers.push(identifier.to_string());
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        self.paths.push(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        );
        syn::visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        self.record_macro_tokens(&item.tokens);
        syn::visit::visit_macro(self, item);
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        *self
            .type_declarations
            .entry(item.ident.to_string())
            .or_default() += 1;
        syn::visit::visit_item_struct(self, item);
    }

    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        *self
            .type_declarations
            .entry(item.ident.to_string())
            .or_default() += 1;
        syn::visit::visit_item_enum(self, item);
    }

    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        *self
            .type_declarations
            .entry(item.ident.to_string())
            .or_default() += 1;
        syn::visit::visit_item_type(self, item);
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        *self
            .function_declarations
            .entry(item.sig.ident.to_string())
            .or_default() += 1;
        syn::visit::visit_item_fn(self, item);
    }
}

struct ProductionSource {
    path: String,
    inventory: ProductionInventory,
}

fn production_sources() -> Vec<ProductionSource> {
    rs_files(&src_dir())
        .into_iter()
        .map(|path| {
            let source_path = rel(&path);
            let source = fs::read_to_string(path).expect("read production Rust source");
            ProductionSource {
                inventory: ProductionInventory::from_source(&source, &source_path),
                path: source_path,
            }
        })
        .collect()
}

fn production_occurrences(symbol: &str) -> Vec<String> {
    production_sources()
        .into_iter()
        .filter_map(|source| {
            source
                .inventory
                .contains_identifier(symbol)
                .then_some(source.path)
        })
        .collect()
}

#[test]
fn production_inventory_filters_nested_test_cfg_and_scans_macro_tokens() {
    let source = r#"
struct Owner {
    live: (),
    #[cfg(test)]
    test_field: RuntimeFilterPlanResult,
    #[cfg(all(test, feature = "compat"))]
    compat_test_field: PlannedRuntimeFilter,
    #[cfg(any(test, feature = "compat"))]
    production_possible_field: RuntimeFilterGraphProjection,
}

enum Mode {
    Live,
    #[cfg(test)]
    TestOnly(RuntimeFilterProbe),
}

impl Owner {
    #[cfg(test)]
    fn plan_runtime_filters() {}

    #[cfg(all(test, feature = "compat"))]
    fn project_runtime_filters() {}

    #[cfg(not(test))]
    fn production_method() {}
}

trait Contract {
    #[cfg(test)]
    fn test_trait_method(&self, _: RuntimeFilterBuildIntent);
}

unsafe extern "C" {
    #[cfg(test)]
    fn test_foreign_function(value: RuntimeFilterBuild);
}

fn live() {
    #[cfg(test)]
    consume::<RuntimeFilterPlanResult>();

    #[cfg(all(test, feature = "compat"))]
    test_only!(proto::novarocks::RuntimeFilterBuild);
}

macro_rules! production_macro {
    () => { proto::novarocks::RuntimeFilterBuild };
}
"#;
    let inventory = ProductionInventory::from_source(source, "synthetic.rs");

    for excluded in [
        "RuntimeFilterPlanResult",
        "PlannedRuntimeFilter",
        "RuntimeFilterProbe",
        "plan_runtime_filters",
        "project_runtime_filters",
        "RuntimeFilterBuildIntent",
        "test_foreign_function",
        "consume",
        "test_only",
    ] {
        assert!(
            !inventory.contains_identifier(excluded),
            "test-only identifier must be excluded: {excluded}"
        );
    }
    for included in [
        "RuntimeFilterGraphProjection",
        "production_method",
        "RuntimeFilterBuild",
    ] {
        assert!(
            inventory.contains_identifier(included),
            "production-capable identifier must be included: {included}"
        );
    }
    assert!(
        inventory.contains_path_suffix(&["proto", "novarocks", "RuntimeFilterBuild"]),
        "macro token paths must be included in the production inventory"
    );
}

#[test]
fn rfd5b_native_encoder_preserves_project_filter_aggregate_union_exchange_scan_values_attachments()
{
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/protocol/native/encode/plan.rs"),
    )
    .expect("read native plan encoder");
    let body = source
        .split("pub(super) fn encode_node_with_context")
        .nth(1)
        .expect("node encoder")
        .split("fn apply_sealed_node_output_columns")
        .next()
        .expect("node encoder body");
    assert!(
        body.contains(
            "runtime_filter_binding_ids: src\n            .runtime_filter_binding_ids\n            .iter()"
        ),
        "generic DistributedNode encoding must copy sealed binding ids directly for Project, Filter, Aggregate, Union, Exchange, Scan, and Values payloads"
    );
    assert_eq!(
        body.matches("runtime_filter_binding_ids").count(),
        2,
        "generic node encoding must neither derive nor filter Project, Filter, Aggregate, Union, Exchange, Scan, or Values attachments by node kind"
    );
}

#[test]
fn rfd5b_preparation_has_one_binding_materialization_owner() {
    let sources = production_sources();
    let owners = sources
        .iter()
        .filter_map(|source| {
            let count = source
                .inventory
                .type_declaration_count("RuntimeFilterBindingTable");
            (count > 0).then(|| format!("{} ({count})", source.path))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        owners,
        ["src/coordinator/prepare/runtime_filter_binding.rs (1)"],
        "the fragment-local binding table must have one preparation owner"
    );

    let legacy = sources
        .iter()
        .filter(|source| source.path.starts_with("src/coordinator/prepare/"))
        .filter(|source| {
            source
                .inventory
                .contains_identifier("RuntimeFilterGraphProjection")
                || source
                    .inventory
                    .contains_identifier("project_runtime_filters")
        })
        .map(|source| source.path.as_str())
        .collect::<Vec<_>>();
    assert!(
        legacy.is_empty(),
        "preparation must materialize bindings only through the exact table owner; legacy owners: {legacy:?}"
    );
}

#[test]
fn rfd5b_runtime_filter_graph_projection_has_no_production_owner() {
    let mut violations = production_occurrences("RuntimeFilterGraphProjection");
    violations.extend(production_occurrences("project_runtime_filters"));
    violations.sort();
    violations.dedup();
    assert!(
        violations.is_empty(),
        "legacy Graph projection remains production-reachable from: {violations:?}"
    );
}

#[test]
fn rfd5b_runtime_filter_plan_result_has_no_production_owner() {
    let mut violations = Vec::new();
    for symbol in [
        "PlannedRuntimeFilter",
        "RuntimeFilterPlanResult",
        "plan_runtime_filters",
    ] {
        violations.extend(
            production_occurrences(symbol)
                .into_iter()
                .map(|source| format!("{source}: {symbol}")),
        );
    }
    assert!(
        violations.is_empty(),
        "legacy scheduler runtime-filter carrier remains:\n{}",
        violations.join("\n")
    );
}

#[test]
fn rfd5b_native_encoder_never_emits_old_rf_fields_or_intents() {
    let path = "src/protocol/native/encode/plan.rs";
    let source =
        fs::read_to_string(Path::new(manifest_dir()).join(path)).expect("read native plan encoder");
    let inventory = ProductionInventory::from_source(&source, path);
    let violations = [
        "build_runtime_filters",
        "probe_runtime_filters",
        "RuntimeFilterBuildIntent",
        "GraphRuntimeFilterBuild",
        "GraphRuntimeFilterProbe",
    ]
    .into_iter()
    .filter(|term| inventory.contains_identifier(term))
    .collect::<Vec<_>>();
    assert!(
        violations.is_empty(),
        "native encoder still emits legacy RF wire terms: {violations:?}"
    );
}

#[test]
fn rfd5b_production_never_references_old_proto_tombstones() {
    let mut violations = Vec::new();
    for source in production_sources() {
        for tombstone in [
            "RuntimeFilterBuild",
            "RuntimeFilterProbe",
            "RuntimeFilterBuildIntent",
        ] {
            for suffix in [
                vec!["plan", tombstone],
                vec!["novarocks", tombstone],
                vec!["novarocks", "plan", tombstone],
            ] {
                if source.inventory.contains_path_suffix(&suffix) {
                    violations.push(format!("{}: {}", source.path, suffix.join("::")));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "production code references old generated proto tombstones:\n{}",
        violations.join("\n")
    );
}

#[test]
fn rfd5b_native_lowering_never_calls_tree_pushdown() {
    let violations = rs_files(&src_dir().join("lower/novarocks"))
        .into_iter()
        .filter_map(|path| {
            let source_path = rel(&path);
            let source = fs::read_to_string(path).expect("read native lowering source");
            ProductionInventory::from_source(&source, &source_path)
                .contains_identifier("push_down_local_runtime_filters")
                .then_some(source_path)
        })
        .collect::<Vec<_>>();
    assert!(
        violations.is_empty(),
        "Native lowering still reaches the legacy placement traversal: {violations:?}"
    );
}

#[test]
fn rfd5b_legacy_pushdown_is_compat_private() {
    let expected_owner = "src/lower/compat/runtime_filter_pushdown.rs";
    assert!(
        Path::new(manifest_dir()).join(expected_owner).is_file(),
        "missing compat-private traversal owner"
    );

    let mut declarations = Vec::new();
    let mut outside_compat = Vec::new();
    for source in production_sources() {
        let count = source
            .inventory
            .function_declaration_count("push_down_local_runtime_filters");
        if count > 0 {
            declarations.push(format!("{} ({count})", source.path));
        }
        if source
            .inventory
            .contains_identifier("push_down_local_runtime_filters")
            && !source.path.starts_with("src/lower/compat/")
        {
            outside_compat.push(source.path);
        }
    }
    assert_eq!(
        declarations,
        [format!("{expected_owner} (1)")],
        "legacy placement traversal must have one compat-private declaration"
    );
    assert!(
        outside_compat.is_empty(),
        "legacy placement traversal escapes compat: {outside_compat:?}"
    );
}

#[test]
fn rfd5b_native_scheduler_never_populates_legacy_destinations_or_builder_count() {
    let mut violations = Vec::new();
    for source_path in [
        "src/coordinator/scheduler/mod.rs",
        "src/coordinator/execution.rs",
        "src/engine/mod.rs",
    ] {
        let source = fs::read_to_string(Path::new(manifest_dir()).join(source_path))
            .unwrap_or_else(|_| panic!("read {source_path}"));
        let inventory = ProductionInventory::from_source(&source, source_path);
        for forbidden in [
            "RuntimeFilterPlanResult",
            "PlannedRuntimeFilter",
            "plan_runtime_filters",
            "populate_runtime_filter_params",
            "runtime_filter_builder_num",
            "probe_side_filters",
            "build_side_filters",
        ] {
            if inventory.contains_identifier(forbidden) {
                violations.push(format!("{source_path}: {forbidden}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "Native scheduling still populates legacy RF sidecars:\n{}",
        violations.join("\n")
    );
}

#[test]
fn rfd5b_native_hub_worker_calls_are_zero() {
    let mut violations = Vec::new();
    for source_path in [
        "src/service/native_fragment_service.rs",
        "src/lower/novarocks/fragment/mod.rs",
        "src/protocol/native/encode/instance.rs",
    ] {
        let source = fs::read_to_string(Path::new(manifest_dir()).join(source_path))
            .unwrap_or_else(|_| panic!("read {source_path}"));
        let inventory = ProductionInventory::from_source(&source, source_path);
        for forbidden in [
            "runtime_filter_params_from_native",
            "encode_runtime_filter_params",
            "runtime_filter_hub",
            "runtime_filter_worker",
            "set_runtime_filter_params",
            "get_runtime_filter_params",
        ] {
            if inventory.contains_identifier(forbidden) {
                violations.push(format!("{source_path}: {forbidden}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "Native fragment paths still expose legacy Hub/Worker inputs:\n{}",
        violations.join("\n")
    );
}
