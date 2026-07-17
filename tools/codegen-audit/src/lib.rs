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

mod cfg;
mod module_graph;
mod production;
mod retirement;
mod tokens;

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use syn::visit::Visit;

use crate::module_graph::{GraphOptions, build_module_graph};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Violation {
    source: String,
    message: String,
}

impl Violation {
    pub fn new(source: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.source, self.message)
    }
}

struct NativeDependencyVisitor {
    condition: crate::cfg::CfgExpr,
    violations: Vec<Violation>,
    source: String,
}

impl<'ast> Visit<'ast> for NativeDependencyVisitor {
    crate::production::production_pruning_methods!();

    fn visit_path(&mut self, path: &'ast syn::Path) {
        let joined = path
            .segments
            .iter()
            .map(|segment| crate::tokens::ident_text(&segment.ident))
            .collect::<Vec<_>>()
            .join("::");
        for forbidden in [
            "OptimizerPhysicalNode",
            "optimizer::operator::Operator",
            "optimizer::physical_tree",
        ] {
            if joined.contains(forbidden) {
                self.violations.push(Violation::new(
                    &self.source,
                    format!("native encoder production code references {forbidden}"),
                ));
            }
        }
        syn::visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        self.check_tokens(item.tokens.clone());
        syn::visit::visit_macro(self, item);
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        self.check_tokens(quote::quote!(#item));
        syn::visit::visit_item_use(self, item);
    }
}

impl NativeDependencyVisitor {
    fn check_tokens(&mut self, tokens: proc_macro2::TokenStream) {
        for forbidden in [
            &["OptimizerPhysicalNode"][..],
            &["optimizer", "operator", "Operator"][..],
            &["optimizer", "physical_tree"][..],
        ] {
            if crate::tokens::contains_path(tokens.clone(), forbidden) {
                self.violations.push(Violation::new(
                    &self.source,
                    format!(
                        "native encoder production macro tokens reference {}",
                        forbidden.join("::")
                    ),
                ));
            }
        }
    }
}

pub fn audit_native_encoder(repo: &Path) -> Result<Vec<Violation>> {
    let encoder = repo.join("src/protocol/native/encode");
    let graph = build_module_graph(
        &encoder,
        &[(
            encoder.join("mod.rs"),
            vec![
                "crate".to_string(),
                "protocol".to_string(),
                "native".to_string(),
                "encode".to_string(),
            ],
        )],
        GraphOptions {
            forbid_production_path: true,
        },
    )?;
    let mut violations = Vec::new();
    for unit in graph.units {
        let mut visitor = NativeDependencyVisitor {
            condition: unit.condition,
            violations: Vec::new(),
            source: unit.path.display().to_string(),
        };
        visitor.visit_file(&unit.file);
        violations.extend(visitor.violations);
    }

    let seal = fs::read_to_string(repo.join("src/sql/planner/distributed/seal.rs"))
        .context("read distributed plan seal owner")?;
    if seal.contains("scalar_arena") {
        violations.push(Violation::new(
            "src/sql/planner/distributed/seal.rs",
            "DistributedPlan must not carry scalar_arena",
        ));
    }
    let planner_root = repo.join("src/sql/planner");
    for path in rust_files(&planner_root)? {
        let source = fs::read_to_string(&path)?;
        if source.contains("enum DistributedPlanKind") {
            violations.push(Violation::new(
                path.display().to_string(),
                "DistributedPlanKind must not be reintroduced",
            ));
        }
        if source.contains("struct PlanNodeStats") {
            violations.push(Violation::new(
                path.display().to_string(),
                "migration PlanNodeStats must not be reintroduced",
            ));
        }
    }
    if !repo
        .join("src/sql/planner/optimizer_bridge/id_binding.rs")
        .is_file()
    {
        violations.push(Violation::new(
            "src/sql/planner/optimizer_bridge/id_binding.rs",
            "id binding verification must remain under planner::optimizer_bridge",
        ));
    }
    violations.sort();
    violations.dedup();
    Ok(violations)
}

pub fn audit_sql_codegen_retirement(repo: &Path) -> Result<Vec<Violation>> {
    let retired_source = ["src", "sql", "codegen"].join("/");
    let mut violations = physical_retirement_violations(repo)?;
    for retired in [&retired_source, &format!("{retired_source}.rs")] {
        if fs::symlink_metadata(repo.join(retired)).is_ok() {
            violations.push(Violation::new(retired, "retired path still exists"));
        }
    }
    violations.extend(audit_reachable_sql_retirement(repo)?);
    violations.sort();
    violations.dedup();
    Ok(violations)
}

fn audit_reachable_sql_retirement(repo: &Path) -> Result<Vec<Violation>> {
    let src = repo.join("src");
    let graph = build_module_graph(
        &src,
        &[
            (src.join("lib.rs"), vec!["crate".to_string()]),
            (src.join("main.rs"), vec!["crate".to_string()]),
        ],
        GraphOptions {
            forbid_production_path: false,
        },
    )?;
    let mut violations = retirement::audit_graph(&graph)?;
    violations.sort();
    violations.dedup();
    Ok(violations)
}

fn physical_retirement_violations(repo: &Path) -> Result<Vec<Violation>> {
    fn visit(path: &Path, repo: &Path, violations: &mut Vec<Violation>) -> Result<()> {
        for entry in fs::read_dir(path)
            .with_context(|| format!("read physical audit directory {}", path.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if matches!(
                    path.file_name().and_then(|name| name.to_str()),
                    Some("target" | "__pycache__")
                ) {
                    continue;
                }
                visit(&path, repo, violations)?;
                continue;
            }
            let bytes = fs::read(&path)
                .with_context(|| format!("read physical audit source {}", path.display()))?;
            let found = if path.extension().is_some_and(|extension| extension == "rs") {
                let source = std::str::from_utf8(&bytes)
                    .with_context(|| format!("decode Rust source {}", path.display()))?;
                let file = syn::parse_file(source)
                    .with_context(|| format!("parse physical Rust source {}", path.display()))?;
                retirement::physical_rust_source_mentions_retired(source, &file)
            } else {
                retirement::normalized_text_mentions_retired(&String::from_utf8_lossy(&bytes))
            };
            if found {
                violations.push(Violation::new(
                    path.strip_prefix(repo)
                        .unwrap_or(&path)
                        .display()
                        .to_string(),
                    "contains a physical reference to the retired SQL encoder owner",
                ));
            }
        }
        Ok(())
    }

    let mut violations = Vec::new();
    for root in ["src", "tools"] {
        let path = repo.join(root);
        if path.is_dir() {
            visit(&path, repo, &mut violations)?;
        }
    }
    violations.sort();
    violations.dedup();
    Ok(violations)
}

pub fn audit_repo(repo: &Path) -> Result<Vec<Violation>> {
    let mut violations = audit_native_encoder(repo)?;
    violations.extend(audit_sql_codegen_retirement(repo)?);
    violations.sort();
    violations.dedup();
    Ok(violations)
}

fn rust_files(root: &Path) -> Result<Vec<PathBuf>> {
    fn visit(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(path)
            .with_context(|| format!("read source directory {}", path.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit(&path, files)?;
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn write_native_fixture(repo: &Path, encode_mod: &str) -> Result<()> {
    fs::create_dir_all(repo.join("src/protocol/native/encode"))?;
    fs::create_dir_all(repo.join("src/sql/planner/distributed"))?;
    fs::create_dir_all(repo.join("src/sql/planner/optimizer_bridge"))?;
    fs::write(repo.join("src/protocol/native/encode/mod.rs"), encode_mod)?;
    fs::write(repo.join("src/sql/planner/distributed/seal.rs"), "")?;
    fs::write(
        repo.join("src/sql/planner/optimizer_bridge/id_binding.rs"),
        "",
    )?;
    Ok(())
}

fn write_retirement_fixture(repo: &Path, lib: &str) -> Result<()> {
    fs::create_dir_all(repo.join("src"))?;
    fs::write(repo.join("src/lib.rs"), lib)?;
    Ok(())
}

fn retired_rust_path() -> String {
    ["crate", "sql", "codegen"].join("::")
}

fn retired_source_path() -> String {
    ["src", "sql", "codegen"].join("/")
}

pub fn run_self_tests() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        r#"
#[cfg(test)]
macro_rules! test_helper { () => {} }
fn forbidden(_: OptimizerPhysicalNode) {}
"#,
    )?;
    assert!(!audit_native_encoder(fixture.path())?.is_empty());

    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        r#"
mod layer { mod hidden; }
"#,
    )?;
    fs::create_dir_all(fixture.path().join("src/protocol/native/encode/layer"))?;
    fs::write(
        fixture
            .path()
            .join("src/protocol/native/encode/layer/hidden.rs"),
        "fn forbidden(_: OptimizerPhysicalNode) {}",
    )?;
    assert!(!audit_native_encoder(fixture.path())?.is_empty());

    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        "#[cfg_attr(not(test), cfg(test))] mod hidden;\nfn production() {}",
    )?;
    fs::write(
        fixture.path().join("src/protocol/native/encode/hidden.rs"),
        "fn forbidden(_: OptimizerPhysicalNode) {}",
    )?;
    assert!(audit_native_encoder(fixture.path())?.is_empty());

    for attribute in [
        r#"#[cfg_attr(not(test), path = "prod.rs")] mod owner;"#,
        r#"#[cfg_attr(not(test), cfg_attr(feature = "prod", path = "prod.rs"))] mod owner;"#,
        r#"#[cfg_attr(any(test, feature = "prod"), path = "prod.rs")] mod owner;"#,
        r#"#[cfg_attr(all(not(test), any(feature = "a", feature = "b")), path = "prod.rs")] mod owner;"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), attribute)?;
        fs::write(
            fixture.path().join("src/protocol/native/encode/owner.rs"),
            "fn production() {}",
        )?;
        fs::write(
            fixture.path().join("src/protocol/native/encode/prod.rs"),
            "fn forbidden(_: OptimizerPhysicalNode) {}",
        )?;
        assert!(audit_native_encoder(fixture.path()).is_err());
    }

    for attribute in [
        r#"#[cfg_attr(test, path = "tests.rs")] mod owner;"#,
        r#"#[cfg_attr(test, cfg_attr(not(test), path = "never.rs"))] mod owner;"#,
        r#"#[cfg_attr(not(not(test)), path = "tests.rs")] mod owner;"#,
        r#"
#[doc = "cfg_attr(not(test), path = prod.rs)"]
// #[path = "prod.rs"]
mod owner;
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), attribute)?;
        fs::write(
            fixture.path().join("src/protocol/native/encode/owner.rs"),
            "fn production() {}",
        )?;
        assert!(audit_native_encoder(fixture.path())?.is_empty());
    }

    let invalid_retirement = [
        "pub mod sql { pub mod codegen; }",
        "pub mod r#sql { pub mod r#codegen; }",
        "pub mod sql { use crate::protocol as codegen; }",
        "pub mod sql { include!(\"codegen/mod.rs\"); }",
        r#"
macro_rules! source_loader {
    () => { include!(concat!("code", "gen/mod.rs")); };
}
macro_rules! inject_with {
    ("safe", $m:ident) => { println!("safe"); };
    ("load", $m:ident) => { $m!(); };
}
inject_with!("load", source_loader);
"#,
        r#"
use std::include as inject;
macro_rules! inner { ($m:tt) => { $m!("codegen/mod.rs"); }; }
macro_rules! outer { ($m:path) => { inner!($m); }; }
outer!(inject);
"#,
        r#"unknown_wrapper!(include);"#,
    ];
    for source in invalid_retirement {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), source)?;
        let detected = match audit_sql_codegen_retirement(fixture.path()) {
            Ok(violations) => !violations.is_empty(),
            Err(_) => true,
        };
        assert!(detected, "retirement fixture escaped: {source}");
    }

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        r#"
mod macros;
mod sql { crate::inject_with!("load", include); }
"#,
    )?;
    fs::write(
        fixture.path().join("src/macros.rs"),
        r#"
#[macro_export]
macro_rules! source_loader {
    () => { include!("codegen/mod.rs"); };
}
#[macro_export]
macro_rules! inject_with {
    ("safe", $m:ident) => { println!("safe"); };
    ("load", $m:path) => { $m!(); };
}
"#,
    )?;
    fs::write(
        fixture.path().join("src/lib.rs"),
        r#"
mod macros;
mod sql { crate::inject_with!("load", crate::source_loader); }
"#,
    )?;
    assert!(!audit_sql_codegen_retirement(fixture.path())?.is_empty());

    let valid_retirement = [
        r#"
#[cfg(test)]
macro_rules! inject_with { ($m:ident) => { $m!("codegen/mod.rs"); }; }
#[cfg(test)]
inject_with!(include);
"#,
        r#"
mod runtime {
    macro_rules! inject_with { ($m:ident) => { $m!("safe.rs"); }; }
    inject_with!(include);
}
"#,
        r#"
macro_rules! ordinary { ("load", $m:ident) => { $m!("ordinary"); }; }
ordinary!("load", println);
"#,
        r#"
macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
macro_rules! inject_with {
    ("safe", $m:ident) => { println!("safe"); };
    ("load", $m:ident) => { $m!(); };
}
inject_with!("safe", source_loader);
"#,
        r#"
macro_rules! collect {
    ($($value:expr),*) => { vec![$($value),*] };
}
fn values() { let _ = collect!(1, 2, 3); }
"#,
    ];
    for source in valid_retirement {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), source)?;
        assert!(
            audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "valid retirement fixture rejected: {source}"
        );
    }

    for source in [
        r#"
struct Owner;
impl Owner {
    #[cfg(test)]
    fn test_only(_: OptimizerPhysicalNode) {}
}
"#,
        r#"
trait Owner {
    #[cfg_attr(not(test), cfg(test))]
    fn test_only(_: OptimizerPhysicalNode);
}
"#,
        r#"
unsafe extern "C" {
    #[cfg(test)]
    fn test_only(value: OptimizerPhysicalNode);
}
"#,
        r#"
fn owner() {
    #[cfg(test)]
    let _: OptimizerPhysicalNode = unreachable!();
    #[cfg(test)]
    { let _: OptimizerPhysicalNode = unreachable!(); }
}
"#,
        r#"
fn owner() {
    match 0 {
        #[cfg(test)]
        _ => { let _: OptimizerPhysicalNode = unreachable!(); }
        _ => {}
    }
}
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), source)?;
        assert!(
            audit_native_encoder(fixture.path())?.is_empty(),
            "test-only native AST escaped production pruning: {source}"
        );
    }

    for source in [
        r#"
struct Owner;
impl Owner {
    fn production(_: OptimizerPhysicalNode) {}
}
"#,
        r#"fn production(_: r#OptimizerPhysicalNode) {}"#,
        r#"
trait Owner {
    fn production(_: OptimizerPhysicalNode);
}
"#,
        r#"
unsafe extern "C" {
    fn production(value: OptimizerPhysicalNode);
}
"#,
        r#"
fn owner() {
    let _: OptimizerPhysicalNode = unreachable!();
}
"#,
        r#"
fn owner() {
    match 0 {
        _ => { let _: OptimizerPhysicalNode = unreachable!(); }
    }
}
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), source)?;
        assert!(
            !audit_native_encoder(fixture.path())?.is_empty(),
            "production native AST was not audited: {source}"
        );
    }

    for template in [
        r#"
struct Owner;
impl Owner {
    #[cfg(test)]
    fn test_only(_: PATH::Legacy) {}
}
"#,
        r#"
trait Owner {
    #[cfg_attr(not(test), cfg(test))]
    fn test_only(_: PATH::Legacy);
}
"#,
        r#"
unsafe extern "C" {
    #[cfg(test)]
    fn test_only(value: PATH::Legacy);
}
"#,
        r#"
fn owner() {
    #[cfg(test)]
    let _: PATH::Legacy = unreachable!();
    #[cfg(test)]
    { let _: PATH::Legacy = unreachable!(); }
}
"#,
        r#"
fn owner() {
    match 0 {
        #[cfg(test)]
        _ => { let _: PATH::Legacy = unreachable!(); }
        _ => {}
    }
}
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(
            fixture.path(),
            &template.replace("PATH", &retired_rust_path()),
        )?;
        assert!(
            audit_reachable_sql_retirement(fixture.path())?.is_empty(),
            "test-only retirement AST escaped production pruning: {template}"
        );
    }

    for template in [
        r#"
struct Owner;
impl Owner {
    fn production(_: PATH::Legacy) {}
}
"#,
        r#"
trait Owner {
    fn production(_: PATH::Legacy);
}
"#,
        r#"
unsafe extern "C" {
    fn production(value: PATH::Legacy);
}
"#,
        r#"
fn owner() {
    let _: PATH::Legacy = unreachable!();
}
"#,
        r#"
fn owner() {
    match 0 {
        _ => { let _: PATH::Legacy = unreachable!(); }
    }
}
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(
            fixture.path(),
            &template.replace("PATH", &retired_rust_path()),
        )?;
        assert!(
            !audit_reachable_sql_retirement(fixture.path())?.is_empty(),
            "production retirement AST was not audited: {template}"
        );
    }

    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        r#"
#[cfg(feature = "owner")]
mod hidden;
"#,
    )?;
    fs::write(
        fixture.path().join("src/protocol/native/encode/hidden.rs"),
        r#"
#[cfg(not(feature = "owner"))]
fn hidden(_: OptimizerPhysicalNode) {}
"#,
    )?;
    assert!(
        audit_native_encoder(fixture.path())?.is_empty(),
        "external module ancestor cfg was not propagated"
    );

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        r#"
#[cfg(feature = "owner")]
mod hidden;
"#,
    )?;
    fs::write(
        fixture.path().join("src/hidden.rs"),
        format!(
            r#"
#[cfg(not(feature = "owner"))]
fn hidden(_: {}::Legacy) {{}}
"#,
            retired_rust_path()
        ),
    )?;
    assert!(
        audit_reachable_sql_retirement(fixture.path())?.is_empty(),
        "external retirement module ancestor cfg was not propagated"
    );

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        r#"
#[cfg(feature = "owner")]
mod outer {
    #[cfg_attr(not(feature = "owner"), path = "codegen.rs")]
    mod hidden;
}
"#,
    )?;
    fs::create_dir_all(fixture.path().join("src/outer"))?;
    fs::write(
        fixture.path().join("src/outer/hidden.rs"),
        "pub struct Hidden;",
    )?;
    assert!(
        audit_reachable_sql_retirement(fixture.path())?.is_empty(),
        "ancestor cfg must make the nested retired path activation impossible"
    );

    for source in [
        r#"macro_rules! hidden { () => { type Leak = OptimizerPhysicalNode; }; }"#,
        r#"macro_rules! hidden { () => { type Leak = r#OptimizerPhysicalNode; }; }"#,
        r#"macro_rules! hidden { () => { type Leak = optimizer::operator::Operator; }; }"#,
        r#"macro_rules! hidden { () => { use optimizer::physical_tree::Leak; }; }"#,
        r#"unknown!(OptimizerPhysicalNode);"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), source)?;
        assert!(
            !audit_native_encoder(fixture.path())?.is_empty(),
            "native macro token escaped audit: {source}"
        );
    }

    for source in [
        r#"
#[cfg(test)]
macro_rules! hidden { () => { type Leak = OptimizerPhysicalNode; }; }
"#,
        r#"macro_rules! noise { () => { "OptimizerPhysicalNode optimizer::physical_tree"; }; }"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), source)?;
        assert!(
            audit_native_encoder(fixture.path())?.is_empty(),
            "non-production native macro token was rejected: {source}"
        );
    }

    for template in [
        r#"macro_rules! hidden { () => { type Leak = PATH::Legacy; }; }"#,
        r#"macro_rules! hidden { () => { use PATH::Legacy; }; }"#,
        r#"unknown!(PATH::Legacy);"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(
            fixture.path(),
            &template.replace("PATH", &retired_rust_path()),
        )?;
        assert!(
            !audit_reachable_sql_retirement(fixture.path())?.is_empty(),
            "retirement macro token escaped audit: {template}"
        );
    }

    for template in [
        r#"
#[cfg(test)]
macro_rules! hidden { () => { type Leak = PATH::Legacy; }; }
"#,
        r#"macro_rules! noise { () => { "PATH::Legacy"; }; }"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(
            fixture.path(),
            &template.replace("PATH", &retired_rust_path()),
        )?;
        assert!(
            audit_reachable_sql_retirement(fixture.path())?.is_empty(),
            "non-production retirement macro token was rejected: {template}"
        );
    }

    for source in [
        r#"
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
    #[macro_export]
    macro_rules! inject { ($m:path) => { $m!(); }; }
}
self::inject!(crate::source_loader);
"#,
        r#"
mod sql {
    macro_rules! inject { ($m:path) => { $m!(); }; }
    mod nested { super::inject!(crate::source_loader); }
}
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
}
"#,
        r#"
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
    #[macro_export]
    macro_rules! inject { ($m:path) => { $m!(); }; }
}
use crate::inject as loader;
loader!(crate::source_loader);
"#,
        r#"
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
    #[macro_export]
    macro_rules! inject { ($m:path) => { $m!(); }; }
}
pub use crate::inject as loader;
self::loader!(crate::source_loader);
"#,
        r#"
macro_rules! inject {
    ("safe" $tag:ident $m:path) => { println!("safe"); };
    ("load" $tag:ident $m:path) => { $m!("codegen/mod.rs"); };
}
inject!("load" tag include);
"#,
        r#"
macro_rules! inject {
    ($($m:path),+) => { $m!("codegen/mod.rs"); };
}
inject!(println);
"#,
        r#"
macro_rules! inject {
    (($m:path)) => { $m!("codegen/mod.rs"); };
}
inject!((println));
"#,
        r#"
use std::include as loader;
unknown_wrapper!(self::loader);
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), source)?;
        assert!(
            !audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "qualified include wrapper escaped audit: {source}"
        );
    }

    for source in [
        r#"
use crate::later as loader;
pub use crate::inject as later;
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
    #[macro_export]
    macro_rules! inject { ($m:path) => { $m!(); }; }
}
loader!(crate::source_loader);
"#,
        r#"
pub use crate::{inject as later};
use self::later as loader;
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
    #[macro_export]
    macro_rules! inject { ($m:path) => { $m!(); }; }
}
loader!(crate::source_loader);
"#,
        r#"
use crate::middle as loader;
pub use crate::later as middle;
pub use crate::inject as later;
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
    #[macro_export]
    macro_rules! inject { ($m:path) => { $m!(); }; }
}
loader!(crate::source_loader);
"#,
        r#"
use self::right as left;
use self::left as right;
left!(println);
"#,
        r#"
mod helpers {
    #[macro_export]
    macro_rules! source_loader { () => { include!("codegen/mod.rs"); }; }
    #[macro_export]
    macro_rules! inject { ($m:path) => { $m!(); }; }
}
mod sql {
    use super::{later as loader};
    pub use crate::inject as later;
    loader!(crate::source_loader);
}
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), source)?;
        assert!(
            !audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "order-independent alias graph escaped audit: {source}"
        );
    }

    for source in [
        r#"
#[cfg(feature = "owner")]
fn owner() {
    #[cfg(not(feature = "owner"))]
    let _: OptimizerPhysicalNode = unreachable!();
}
"#,
        r#"
#[cfg(feature = "owner")]
impl Owner {
    #[cfg_attr(feature = "owner", cfg(not(feature = "owner")))]
    fn hidden(_: OptimizerPhysicalNode) {}
}
"#,
        r#"
#[cfg(feature = "owner")]
trait OwnerTrait {
    #[cfg(not(feature = "owner"))]
    fn hidden(_: OptimizerPhysicalNode);
}
"#,
        r#"
#[cfg(feature = "owner")]
fn owner() {
    #[cfg(not(feature = "owner"))]
    { let _: OptimizerPhysicalNode = unreachable!(); }
}
"#,
        r#"
#[cfg(feature = "owner")]
fn owner() {
    match 0 {
        #[cfg(not(feature = "owner"))]
        _ => { let _: OptimizerPhysicalNode = unreachable!(); }
    }
}
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), source)?;
        assert!(
            audit_native_encoder(fixture.path())?.is_empty(),
            "ancestor cfg conjunction was not propagated: {source}"
        );
    }

    for template in [
        r#"
#[cfg(feature = "owner")]
fn owner() {
    #[cfg(not(feature = "owner"))]
    let _: PATH::Legacy = unreachable!();
}
"#,
        r#"
#[cfg(feature = "owner")]
impl Owner {
    #[cfg_attr(feature = "owner", cfg(not(feature = "owner")))]
    fn hidden(_: PATH::Legacy) {}
}
"#,
        r#"
#[cfg(feature = "owner")]
trait OwnerTrait {
    #[cfg(not(feature = "owner"))]
    fn hidden(_: PATH::Legacy);
}
"#,
        r#"
#[cfg(feature = "owner")]
fn owner() {
    #[cfg(not(feature = "owner"))]
    { let _: PATH::Legacy = unreachable!(); }
}
"#,
        r#"
#[cfg(feature = "owner")]
fn owner() {
    match 0 {
        #[cfg(not(feature = "owner"))]
        _ => { let _: PATH::Legacy = unreachable!(); }
    }
}
"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(
            fixture.path(),
            &template.replace("PATH", &retired_rust_path()),
        )?;
        assert!(
            audit_reachable_sql_retirement(fixture.path())?.is_empty(),
            "ancestor retirement cfg conjunction was not propagated: {template}"
        );
    }

    for source in [
        r#"fn forbidden(_: r#OptimizerPhysicalNode) {}"#,
        r#"fn forbidden(_: r#optimizer::r#operator::r#Operator) {}"#,
        r#"use r#optimizer::r#physical_tree::Leak;"#,
    ] {
        let fixture = tempfile::tempdir()?;
        write_native_fixture(fixture.path(), source)?;
        assert!(
            !audit_native_encoder(fixture.path())?.is_empty(),
            "raw native identifier escaped audit: {source}"
        );
    }

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        &format!(
            "use {}::Legacy;",
            ["crate", "r#sql", "r#codegen"].join("::")
        ),
    )?;
    assert!(
        !audit_reachable_sql_retirement(fixture.path())?.is_empty(),
        "raw retirement namespace escaped AST audit"
    );

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        r#"
use std::include as r#loader;
unknown_wrapper!(r#loader);
"#,
    )?;
    assert!(
        !audit_sql_codegen_retirement(fixture.path())?.is_empty(),
        "raw include alias escaped restricted macro audit"
    );

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        r#"
#[cfg_attr(test, path = "codegen.rs")]
mod owner;
"#,
    )?;
    fs::write(fixture.path().join("src/owner.rs"), "pub struct Owner;")?;
    assert!(
        audit_sql_codegen_retirement(fixture.path())?.is_empty(),
        "test-only path activation must not resurrect the retired source"
    );

    for relative in ["src/orphan.rs", "tools/audit_helper.py"] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), "pub struct Owner;")?;
        let path = fixture.path().join(relative);
        fs::create_dir_all(path.parent().unwrap())?;
        let source = if path.extension().is_some_and(|extension| extension == "rs") {
            format!("use {}::Legacy;", retired_rust_path())
        } else {
            format!("# unreachable physical reference: {}", retired_rust_path())
        };
        fs::write(&path, source)?;
        assert!(
            !audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "physical retirement inventory missed {relative}"
        );
    }

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(fixture.path(), "pub struct Owner;")?;
    fs::write(
        fixture.path().join("src/orphan.rs"),
        format!(
            "use {}::Legacy;",
            ["crate", "r#sql", "r#codegen"].join("::")
        ),
    )?;
    assert!(
        !audit_sql_codegen_retirement(fixture.path())?.is_empty(),
        "raw unreachable Rust namespace escaped physical inventory"
    );

    for source in [
        format!("unknown!({});", ["crate", "r#sql", "r#codegen"].join(" / ")),
        format!("unknown!({});", ["r#sql", "r#codegen"].join(" / ")),
        format!(
            "include!(r\"{}\");",
            ["src", "r#sql", "r#codegen", "mod.rs"].join("\\")
        ),
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), "pub struct Owner;")?;
        fs::write(fixture.path().join("src/orphan.rs"), &source)?;
        assert!(
            !audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "normalized Rust physical source escaped inventory: {source}"
        );
    }

    for source in [
        format!(
            "// {} {}",
            retired_rust_path(),
            ["src", "r#sql", "r#codegen"].join("\\")
        ),
        format!(
            "const NOTE: &str = r\"{} {}\";",
            retired_rust_path(),
            ["src", "r#sql", "r#codegen"].join("\\")
        ),
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), "pub struct Owner;")?;
        fs::write(fixture.path().join("src/orphan.rs"), &source)?;
        assert!(
            audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "Rust comment or ordinary string must not count as physical ownership: {source}"
        );
    }

    for source in [
        ["src", "sql", "codegen"].join("\\"),
        ["src", "r#sql", "r#codegen"].join("/"),
        ["crate", "r#sql", "r#codegen"].join("::"),
    ] {
        let fixture = tempfile::tempdir()?;
        write_retirement_fixture(fixture.path(), "pub struct Owner;")?;
        fs::create_dir_all(fixture.path().join("tools"))?;
        fs::write(fixture.path().join("tools/audit_helper.txt"), source)?;
        assert!(
            !audit_sql_codegen_retirement(fixture.path())?.is_empty(),
            "normalized non-Rust retirement source escaped physical inventory"
        );
    }

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(fixture.path(), "pub struct Owner;")?;
    let retired = fixture.path().join(retired_source_path());
    fs::create_dir_all(&retired)?;
    assert!(!audit_sql_codegen_retirement(fixture.path())?.is_empty());

    let fixture = tempfile::tempdir()?;
    write_native_fixture(
        fixture.path(),
        "#[cfg(not(test, feature = \"broken\"))] fn uncertain() {}",
    )?;
    assert!(audit_native_encoder(fixture.path()).is_err());

    let fixture = tempfile::tempdir()?;
    write_retirement_fixture(
        fixture.path(),
        "#[cfg(not(test, feature = \"broken\"))] fn uncertain() {}",
    )?;
    assert!(audit_sql_codegen_retirement(fixture.path()).is_err());
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn shared_self_tests_pass() {
        super::run_self_tests().unwrap();
    }
}
