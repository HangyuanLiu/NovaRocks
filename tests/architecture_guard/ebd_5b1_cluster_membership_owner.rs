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

use std::collections::BTreeSet;
use std::path::Path;

use super::{
    manifest_dir, rel, rs_files, rust_all_source_canonical_paths, rust_lexically_sanitized,
    rust_module_items, rust_production_canonical_paths, rust_production_use_statements,
    rust_sanitized_production_text, rust_source_module_segments, rust_source_tokens,
};

const OWNER: &str = "src/coordinator/cluster/mod.rs";

fn retired_owner() -> String {
    ["src/runtime/", "backend_registry.rs"].concat()
}

fn ebd_5b1_dependency_path(canonical: &[String]) -> Vec<String> {
    let source_module =
        rust_source_module_segments(OWNER).expect("cluster owner path must resolve");
    if canonical.starts_with(&source_module) && canonical.len() > source_module.len() {
        return canonical[source_module.len()..].to_vec();
    }
    canonical.to_vec()
}

fn ebd_5b1_local_roots(source: &str) -> BTreeSet<String> {
    let production = rust_sanitized_production_text(source);
    let tokens = rust_source_tokens(&production)
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    let mut production_roots = BTreeSet::new();
    let mut brace_depth = 0usize;
    for index in 0..tokens.len() {
        match tokens[index].as_str() {
            "{" => brace_depth += 1,
            "}" => brace_depth = brace_depth.saturating_sub(1),
            "enum" | "mod" | "struct" | "trait" | "type" | "union" if brace_depth == 0 => {
                if let Some(ident) = tokens.get(index + 1) {
                    production_roots.insert(ident.clone());
                }
            }
            _ => {}
        }
    }
    let file = syn::parse_file(source).expect("cluster owner source must parse");
    let mut roots = BTreeSet::from(["Self".to_string()]);
    for item in file.items {
        let ident = match item {
            syn::Item::Enum(item) => Some(item.ident),
            syn::Item::Mod(item) => Some(item.ident),
            syn::Item::Struct(item) => Some(item.ident),
            syn::Item::Trait(item) => Some(item.ident),
            syn::Item::Type(item) => Some(item.ident),
            syn::Item::Union(item) => Some(item.ident),
            _ => None,
        };
        if let Some(ident) = ident.filter(|ident| production_roots.contains(&ident.to_string())) {
            roots.insert(ident.to_string());
        }
    }
    roots
}

fn allowed_std_path(path: &[String]) -> bool {
    let allowed: &[&[&str]] = &[
        &["std", "collections", "BTreeMap"],
        &["std", "collections", "BTreeMap", "new"],
        &["std", "collections", "HashMap"],
        &["std", "collections", "HashMap", "new"],
        &["std", "net", "SocketAddr"],
        &["std", "sync", "Arc"],
        &["std", "sync", "Mutex"],
        &["std", "sync", "Mutex", "new"],
        &["std", "sync", "OnceLock"],
        &["std", "sync", "OnceLock", "new"],
    ];
    allowed.iter().any(|expected| {
        path.len() == expected.len()
            && path
                .iter()
                .zip(expected.iter())
                .all(|(actual, expected)| actual == expected)
    })
}

fn ebd_5b1_allowed_prelude_path(path: &[String]) -> bool {
    matches!(
        path,
        [root] if matches!(root.as_str(), "String" | "Vec")
    ) || matches!(
        path,
        [root, child]
            if matches!(
                (root.as_str(), child.as_str()),
                ("String", "new") | ("Vec", "new")
            )
    )
}

fn ebd_5b1_source_indirection_violations(source: &str) -> Vec<String> {
    let production = rust_sanitized_production_text(source);
    let tokens = rust_source_tokens(&production)
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    let mut violations = Vec::new();
    if tokens.windows(2).any(|pair| pair == ["extern", "crate"]) {
        violations.push("cluster owner contains extern crate".to_string());
    }
    if tokens.windows(2).any(|pair| pair == ["include", "!"]) {
        violations.push("cluster owner contains include! source indirection".to_string());
    }
    for module in rust_module_items(&production) {
        if module.is_external {
            violations.push(format!(
                "cluster owner contains external module declaration {}",
                module.name
            ));
        }
        if module.name == "std" {
            violations.push("cluster owner must not shadow the std root".to_string());
        }
    }
    violations
}

fn ebd_5b1_cluster_owner_dependency_violations(source: &str) -> Vec<String> {
    let local_roots = ebd_5b1_local_roots(source);
    let mut violations = rust_production_canonical_paths(source, OWNER)
        .into_iter()
        .map(|path| ebd_5b1_dependency_path(&path))
        .filter(|path| {
            !path.first().is_some_and(|root| local_roots.contains(root))
                && !allowed_std_path(path)
                && !ebd_5b1_allowed_prelude_path(path)
        })
        .map(|path| format!("forbidden cluster dependency: {}", path.join("::")))
        .collect::<Vec<_>>();
    violations.extend(ebd_5b1_source_indirection_violations(source));
    violations.sort();
    violations.dedup();
    violations
}

fn ebd_5b1_retired_path_violations(source_rel: &str, source: &str) -> Vec<String> {
    let retired = ["crate", "runtime", "backend_registry"];
    let mut violations = rust_all_source_canonical_paths(source, source_rel)
        .into_iter()
        .filter(|path| path.starts_with(&retired.map(str::to_string)))
        .map(|path| format!("{source_rel} uses retired path {}", path.join("::")))
        .collect::<Vec<_>>();
    let lexical = rust_lexically_sanitized(source);
    let tokens = rust_source_tokens(&lexical)
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    if tokens
        .windows(3)
        .any(|triple| triple == ["extern", "crate", "self"])
    {
        violations.push(format!("{source_rel} defines an alternate crate root"));
    }
    let direct = ["crate::runtime::", "backend_registry"].concat();
    if lexical.contains(&direct) {
        violations.push(format!("{source_rel} spells the retired path"));
    }
    violations.sort();
    violations.dedup();
    violations
}

fn ebd_5b1_scheduler_snapshot_violations(source: &str) -> Vec<String> {
    let production = rust_sanitized_production_text(source);
    let mut violations = ["type LiveBackend =", "struct LiveBackendSnapshot"]
        .into_iter()
        .filter(|definition| production.contains(definition))
        .map(|definition| format!("scheduler still owns {definition}"))
        .collect::<Vec<_>>();
    let cluster_prefix = "crate::coordinator::cluster";
    violations.extend(
        rust_production_use_statements(source)
            .into_iter()
            .filter(|import| {
                !import.starts_with("private|")
                    && import
                        .split_once('|')
                        .is_some_and(|(_, path)| path.starts_with(cluster_prefix))
            })
            .map(|import| format!("scheduler re-exports cluster contract: {import}")),
    );
    violations.sort();
    violations.dedup();
    violations
}

fn ebd_5b1_consumer_violations(
    source_rel: &str,
    source: &str,
    symbol: &str,
    include_tests: bool,
) -> Vec<String> {
    let paths = if include_tests {
        rust_all_source_canonical_paths(source, source_rel)
    } else {
        rust_production_canonical_paths(source, source_rel)
    };
    let expected = ["crate", "coordinator", "cluster", symbol].map(str::to_string);
    let sanitized = if include_tests {
        rust_lexically_sanitized(source)
    } else {
        rust_sanitized_production_text(source)
    };
    let tokens = rust_source_tokens(&sanitized)
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    let canonical_import = format!("|crate::coordinator::cluster::{symbol}");
    let has_production_import = !include_tests
        && rust_production_use_statements(source)
            .iter()
            .any(|import| import.contains(&canonical_import));
    let required_identifier_count = if include_tests || has_production_import {
        2
    } else {
        1
    };
    let mut violations = Vec::new();
    if !paths.iter().any(|path| path.starts_with(&expected)) {
        violations.push(format!(
            "{source_rel} does not consume canonical cluster symbol {symbol}"
        ));
    }
    if tokens
        .iter()
        .filter(|token| token.as_str() == symbol)
        .count()
        < required_identifier_count
    {
        violations.push(format!(
            "{source_rel} has no non-import use of cluster symbol {symbol}"
        ));
    }
    if tokens.windows(2).any(|pair| pair == ["if", "false"])
        || tokens
            .windows(4)
            .any(|window| window == ["cfg", "(", "any", "("])
    {
        violations.push(format!("{source_rel} contains a fake dead consumer block"));
    }
    violations
}

fn ebd_5b1_owner_definition_violations(sources: &[(&str, &str)]) -> Vec<String> {
    let required = [
        ("type", "BeId"),
        ("enum", "BackendState"),
        ("struct", "BackendEntry"),
        ("enum", "HeartbeatOutcome"),
        ("enum", "RegistryEvent"),
        ("struct", "BackendRegistry"),
        ("type", "LiveBackend"),
        ("struct", "LiveBackendSnapshot"),
    ];
    let mut violations = Vec::new();
    for (kind, name) in required {
        let owners = sources
            .iter()
            .flat_map(|(source_rel, source)| {
                let tokens = rust_source_tokens(&rust_sanitized_production_text(source))
                    .into_iter()
                    .map(|token| token.text)
                    .collect::<Vec<_>>();
                tokens
                    .windows(2)
                    .filter(|pair| pair == &[kind, name])
                    .map(|_| *source_rel)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if owners != [OWNER] {
            violations.push(format!(
                "{kind} {name} owners must be [{OWNER}], actual={owners:?}"
            ));
        }
    }
    violations
}

fn ebd_5b1_runtime_module_violations(source: &str) -> Vec<String> {
    rust_module_items(source)
        .into_iter()
        .filter(|module| module.name == "backend_registry")
        .map(|_| "runtime retains backend_registry module/forwarder".to_string())
        .collect()
}

fn ebd_5b1_cluster_membership_boundary_violations(repo: &Path) -> Vec<String> {
    let owner_path = repo.join(OWNER);
    let retired_owner = retired_owner();
    let mut violations = Vec::new();
    if !owner_path.is_file() {
        violations.push(format!("missing canonical owner {OWNER}"));
    }
    if repo.join(&retired_owner).exists() {
        violations.push(format!("retired owner still exists {retired_owner}"));
    }

    let owner = owner_path
        .is_file()
        .then(|| std::fs::read_to_string(&owner_path).expect("read cluster owner"))
        .unwrap_or_default();
    let production = rust_sanitized_production_text(&owner);
    for required in [
        "pub(crate) type BeId",
        "pub(crate) enum BackendState",
        "pub(crate) struct BackendEntry",
        "pub(crate) enum HeartbeatOutcome",
        "pub(crate) enum RegistryEvent",
        "pub(crate) struct BackendRegistry",
        "pub(crate) type LiveBackend",
        "pub(crate) struct LiveBackendSnapshot",
    ] {
        if !production.contains(required) {
            violations.push(format!("canonical owner missing {required}"));
        }
    }
    violations.extend(ebd_5b1_cluster_owner_dependency_violations(&owner));

    let scheduler = std::fs::read_to_string(repo.join("src/coordinator/scheduler/mod.rs"))
        .expect("read scheduler");
    violations.extend(ebd_5b1_scheduler_snapshot_violations(&scheduler));

    let required_consumers = [
        ("src/coordinator/scheduler/mod.rs", "LiveBackend", false),
        (
            "src/coordinator/scheduler/mod.rs",
            "LiveBackendSnapshot",
            false,
        ),
        ("src/coordinator/execution.rs", "backend_registry", false),
        ("src/coordinator/execution.rs", "BeId", false),
        ("src/runtime/heartbeat_mgr.rs", "BackendRegistry", false),
        ("src/runtime/heartbeat_mgr.rs", "HeartbeatOutcome", false),
        ("src/runtime/heartbeat_mgr.rs", "RegistryEvent", false),
        ("src/runtime/registry_cleanup.rs", "RegistryEvent", false),
        ("src/engine/backend_ops.rs", "BackendRegistry", false),
        ("src/engine/backend_ops.rs", "BackendState", false),
        ("src/engine/mod.rs", "backend_registry", false),
        ("src/engine/mod.rs", "LiveBackendSnapshot", false),
        ("src/service/metrics_http.rs", "BackendState", false),
        ("src/service/metrics_http.rs", "backend_registry", false),
        (
            "src/runtime_filter/deployment/compiler.rs",
            "LiveBackendSnapshot",
            false,
        ),
        (
            "src/runtime_filter/deployment/routing_shard.rs",
            "LiveBackendSnapshot",
            false,
        ),
        (
            "src/runtime_filter/router/role_graph.rs",
            "LiveBackendSnapshot",
            true,
        ),
        (
            "src/runtime_filter/service/mod.rs",
            "LiveBackendSnapshot",
            true,
        ),
        (
            "src/runtime_filter/service/m4_conformance_tests.rs",
            "LiveBackendSnapshot",
            true,
        ),
    ];
    for (source_rel, symbol, include_tests) in required_consumers {
        let text = std::fs::read_to_string(repo.join(source_rel))
            .unwrap_or_else(|error| panic!("read {source_rel}: {error}"));
        violations.extend(ebd_5b1_consumer_violations(
            source_rel,
            &text,
            symbol,
            include_tests,
        ));
    }

    let mut production_sources = Vec::new();
    for path in rs_files(&repo.join("src"))
        .into_iter()
        .chain(rs_files(&repo.join("tests")))
    {
        let source_rel = rel(&path);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {source_rel}: {error}"));
        violations.extend(ebd_5b1_retired_path_violations(&source_rel, &text));
        if source_rel.starts_with("src/") {
            production_sources.push((source_rel, text));
        }
    }
    let owner_sources = production_sources
        .iter()
        .map(|(source_rel, text)| (source_rel.as_str(), text.as_str()))
        .collect::<Vec<_>>();
    violations.extend(ebd_5b1_owner_definition_violations(&owner_sources));

    let backend_id = std::fs::read_to_string(repo.join("src/runtime/backend_id.rs"))
        .expect("read BE-local backend id");
    let backend_id = rust_sanitized_production_text(&backend_id);
    if !backend_id.contains("static BACKEND_ID: AtomicI64 = AtomicI64::new(-1)") {
        violations
            .push("runtime backend_id lost exact `static BACKEND_ID: AtomicI64` owner".to_string());
    }
    let runtime_mod =
        std::fs::read_to_string(repo.join("src/runtime/mod.rs")).expect("read runtime module");
    violations.extend(ebd_5b1_runtime_module_violations(&runtime_mod));
    violations.sort();
    violations.dedup();
    violations
}

#[test]
fn ebd_5b1_detector_accepts_std_only_owner() {
    let source = r#"
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
static REGISTRY: OnceLock<Mutex<Option<Arc<BackendRegistry>>>> = OnceLock::new();
enum BackendState { Live }
struct BackendRegistry {
    entries: Mutex<BTreeMap<u32, SocketAddr>>,
    endpoints: HashMap<SocketAddr, u32>,
}
impl BackendRegistry {
    fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            endpoints: HashMap::new(),
        }
    }
    fn live() -> BackendState { BackendState::Live }
}
"#;
    assert!(ebd_5b1_cluster_owner_dependency_violations(source).is_empty());
}

#[test]
fn ebd_5b1_detector_rejects_secondary_owner_alias_indirection_and_decoys() {
    for source in [
        "use crate::common::types::UniqueId;",
        "use tracing::info;",
        "use anyhow::Result;",
        "use crate::sql::planner::Plan;",
        "use iceberg::spec::TableMetadata;",
        "use std::fs::File;",
        "#[cfg(test)] mod tracing { pub fn info() {} } use tracing::info;",
        "extern crate self as novarocks;",
        "extern /* split */ crate self as novarocks;",
        "#[path = \"shadow.rs\"] mod shadow;",
        "# [ path = \"shadow.rs\" ] mod shadow;",
        "include!(\"shadow.rs\");",
        "include /* split */ ! (\"shadow.rs\");",
        "mod std { pub mod collections { pub struct BTreeMap; } }",
    ] {
        assert!(
            !ebd_5b1_cluster_owner_dependency_violations(source).is_empty(),
            "fixture must be rejected: {source}"
        );
    }
    for decoy in [
        "// use crate::sql::planner::Plan;",
        "const NOTE: &str = \"use tracing::info;\";",
        "const RAW: &str = r#\"include!(\\\"shadow.rs\\\");\"#;",
    ] {
        assert!(
            ebd_5b1_cluster_owner_dependency_violations(decoy).is_empty(),
            "comment/string/raw-string decoy must be ignored: {decoy}"
        );
    }

    let retired_crate_path = ["crate::runtime::", "backend_registry"].concat();
    let retired_root_path = ["root::runtime::", "backend_registry"].concat();
    let bypasses = [
        ["use crate::runtime::{", "backend_registry as membership};"].concat(),
        format!("use {retired_crate_path}::*;"),
        format!("extern crate self as root; use {retired_root_path}::BackendRegistry;"),
        format!("macro_rules! leak {{ () => {{ {retired_crate_path}::BackendRegistry }}; }}"),
        format!("#[cfg(test)] use {retired_crate_path}::BackendRegistry;"),
    ];
    for bypass in &bypasses {
        assert!(
            !ebd_5b1_retired_path_violations("src/fixture.rs", bypass).is_empty(),
            "retired-path bypass must be rejected: {bypass}"
        );
    }
    let decoys = [
        format!("// use {retired_crate_path}::BackendRegistry;"),
        format!("const NOTE: &str = \"{retired_crate_path}\";"),
        format!("const RAW: &str = r#\"{retired_crate_path}\"#;"),
    ];
    for decoy in &decoys {
        assert!(
            ebd_5b1_retired_path_violations("src/fixture.rs", decoy).is_empty(),
            "retired-path decoy must be ignored: {decoy}"
        );
    }

    for reexport in [
        "pub use crate::coordinator::cluster::LiveBackendSnapshot;",
        "pub(crate) use crate::coordinator::cluster as membership;",
        "use crate::coordinator::cluster as membership; pub(super) use membership::LiveBackend;",
    ] {
        assert!(
            !ebd_5b1_scheduler_snapshot_violations(reexport).is_empty(),
            "scheduler re-export bypass must be rejected: {reexport}"
        );
    }
    assert!(
        ebd_5b1_scheduler_snapshot_violations(
            "use crate::coordinator::cluster::{LiveBackend, LiveBackendSnapshot};"
        )
        .is_empty(),
        "private scheduler consumption must remain allowed"
    );

    let owner = r#"
type BeId = u32;
enum BackendState {}
struct BackendEntry;
enum HeartbeatOutcome {}
enum RegistryEvent {}
struct BackendRegistry;
type LiveBackend = (usize, std::net::SocketAddr);
struct LiveBackendSnapshot;
"#;
    assert!(ebd_5b1_owner_definition_violations(&[(OWNER, owner)]).is_empty());
    assert!(
        !ebd_5b1_owner_definition_violations(&[
            (OWNER, owner),
            ("src/runtime/shadow.rs", "struct BackendRegistry;"),
        ])
        .is_empty(),
        "secondary production owner must be rejected"
    );

    let forwarding = [
        "pub mod ",
        "backend_registry",
        " { pub use crate::coordinator::cluster::*; }",
    ]
    .concat();
    assert!(
        !ebd_5b1_runtime_module_violations(&forwarding).is_empty(),
        "runtime forwarding module must be rejected"
    );

    let live_snapshot = "LiveBackendSnapshot";
    let real_consumer = format!(
        "use crate::coordinator::cluster::{live_snapshot}; \
         fn consume(value: &{live_snapshot}) {{ let _ = value.entries(); }}"
    );
    assert!(
        ebd_5b1_consumer_violations(
            "src/coordinator/scheduler/mod.rs",
            &real_consumer,
            live_snapshot,
            false,
        )
        .is_empty()
    );
    for fake in [
        format!("use crate::coordinator::cluster::{live_snapshot};"),
        format!(
            "use crate::coordinator::cluster::{live_snapshot}; \
             fn fake() {{ if false {{ let _: Option<{live_snapshot}> = None; }} }}"
        ),
    ] {
        assert!(
            !ebd_5b1_consumer_violations(
                "src/coordinator/scheduler/mod.rs",
                &fake,
                live_snapshot,
                false,
            )
            .is_empty(),
            "unused import and fake dead block must not satisfy consumer non-vacuity"
        );
    }
}

#[test]
fn ebd_5b1_cluster_membership_has_one_canonical_owner() {
    let violations = ebd_5b1_cluster_membership_boundary_violations(Path::new(manifest_dir()));
    assert!(
        violations.is_empty(),
        "EBD-5B1 cluster membership boundary violations:\n{}",
        violations.join("\n")
    );
}
