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

use std::path::Path;

use quote::ToTokens;

use super::{
    manifest_dir, rel, rs_files, rust_all_source_canonical_paths, rust_lexically_sanitized,
    rust_production_canonical_paths, rust_production_use_statements,
    rust_sanitized_production_text, rust_source_tokens,
};

const LIFECYCLE_OWNER: &str = "src/coordinator/cluster/lifecycle.rs";
const CLEANUP_OWNER: &str = "src/coordinator/cluster/query_cleanup.rs";
const TRANSPORT_OWNER: &str = "src/service/cluster_heartbeat.rs";
const PROTECTED_OWNER_SYMBOLS: [&str; 5] = [
    "RegistryEventSink",
    "run_heartbeat_round",
    "spawn_heartbeat_manager",
    "QueryCleanupSink",
    "grpc_heartbeat",
];

fn definitely_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Ident>()
                .is_ok_and(|predicate| predicate == "test")
    })
}

fn old_runtime_owner_paths() -> [String; 2] {
    [
        ["src/runtime/", "heartbeat_mgr.rs"].concat(),
        ["src/runtime/", "registry_cleanup.rs"].concat(),
    ]
}

fn retired_runtime_path_violations(source_rel: &str, source: &str) -> Vec<String> {
    let retired = [
        ["crate", "runtime", "heartbeat_mgr"],
        ["crate", "runtime", "registry_cleanup"],
    ];
    rust_all_source_canonical_paths(source, source_rel)
        .into_iter()
        .filter(|path| {
            retired
                .iter()
                .any(|prefix| path.starts_with(&prefix.map(str::to_string)))
        })
        .map(|path| format!("{source_rel} uses retired path {}", path.join("::")))
        .collect()
}

fn top_level_definitions(source: &str) -> Vec<(String, String)> {
    let file = syn::parse_file(source).expect("EBD-5B2 source must parse");
    file.items
        .into_iter()
        .filter(|item| {
            let attrs = match item {
                syn::Item::Fn(item) => &item.attrs,
                syn::Item::Struct(item) => &item.attrs,
                syn::Item::Trait(item) => &item.attrs,
                syn::Item::Type(item) => &item.attrs,
                _ => return true,
            };
            !definitely_cfg_test(attrs)
        })
        .filter_map(|item| match item {
            syn::Item::Fn(item) => Some(("fn".to_string(), item.sig.ident.to_string())),
            syn::Item::Struct(item) => Some(("struct".to_string(), item.ident.to_string())),
            syn::Item::Trait(item) => Some(("trait".to_string(), item.ident.to_string())),
            syn::Item::Type(item) => Some(("type".to_string(), item.ident.to_string())),
            _ => None,
        })
        .collect()
}

fn named_declaration_surfaces(source: &str, symbol: &str) -> Vec<String> {
    let tokens = rust_source_tokens(&rust_sanitized_production_text(source))
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    tokens
        .windows(2)
        .filter(|pair| {
            matches!(pair[0].as_str(), "fn" | "struct" | "trait" | "type") && pair[1] == symbol
        })
        .map(|pair| pair[0].clone())
        .collect()
}

fn forwarding_surface_violations(source_rel: &str, source: &str) -> Vec<String> {
    let allowed_cluster_exports = [
        "pub(crate)|lifecycle::RegistryEventSink",
        "pub(crate)|lifecycle::run_heartbeat_round",
        "pub(crate)|lifecycle::spawn_heartbeat_manager",
        "pub(crate)|query_cleanup::QueryCleanupSink",
    ];
    let mut violations = rust_production_use_statements(source)
        .into_iter()
        .filter(|import| !import.starts_with("private|"))
        .filter(|import| {
            PROTECTED_OWNER_SYMBOLS.iter().any(|symbol| {
                import
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .any(|token| token == *symbol)
            }) || (import.ends_with("::*")
                && (import.contains("coordinator::cluster")
                    || import.contains("cluster::lifecycle")
                    || import.contains("cluster::query_cleanup")))
        })
        .filter(|import| {
            source_rel != "src/coordinator/cluster/mod.rs"
                || !allowed_cluster_exports.contains(&import.as_str())
        })
        .map(|import| format!("{source_rel} visibly forwards heartbeat owner surface {import}"))
        .collect::<Vec<_>>();

    // The use parser sees top-level items. Also scan production macro bodies so
    // a macro-generated visible forwarding surface cannot bypass the ledger.
    let file = syn::parse_file(source).expect("EBD-5B2 forwarding source must parse");
    for item in file.items {
        let syn::Item::Macro(item) = item else {
            continue;
        };
        if definitely_cfg_test(&item.attrs) {
            continue;
        }
        let macro_tokens = rust_source_tokens(&item.mac.tokens.to_string())
            .into_iter()
            .map(|token| token.text)
            .collect::<Vec<_>>();
        if macro_tokens.iter().any(|token| token == "pub")
            && macro_tokens.iter().any(|token| token == "use")
            && PROTECTED_OWNER_SYMBOLS
                .iter()
                .any(|symbol| macro_tokens.iter().any(|token| token == symbol))
        {
            violations.push(format!(
                "{source_rel} contains macro-visible heartbeat owner forwarding"
            ));
        }
    }
    violations.sort();
    violations.dedup();
    violations
}

fn owner_wrapper_violations(source_rel: &str, source: &str) -> Vec<String> {
    let imports = rust_production_use_statements(source);
    let mut protected_names = PROTECTED_OWNER_SYMBOLS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect::<Vec<_>>();
    let mut imports_protected_glob = false;
    for import in imports {
        let Some((_, path)) = import.split_once('|') else {
            continue;
        };
        let references_owner = PROTECTED_OWNER_SYMBOLS.iter().any(|symbol| {
            path.split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .any(|token| token == *symbol)
        });
        if references_owner {
            let exposed = path
                .rsplit_once(" as ")
                .map(|(_, alias)| alias)
                .or_else(|| path.rsplit("::").next())
                .unwrap_or(path);
            protected_names.push(exposed.to_string());
        }
        if path.ends_with("::*") && path.contains("coordinator::cluster") {
            imports_protected_glob = true;
        }
    }

    fn exact_owner_item_allowed(
        source_rel: &str,
        module_path: &[String],
        visibility: &syn::Visibility,
        kind: &str,
        name: &str,
    ) -> bool {
        let crate_visible = matches!(
            visibility,
            syn::Visibility::Restricted(restricted)
                if restricted.in_token.is_none()
                    && restricted.path.segments.len() == 1
                    && restricted.path.segments[0].ident == "crate"
        );
        module_path.is_empty()
            && crate_visible
            && matches!(
                (source_rel, kind, name),
                (LIFECYCLE_OWNER, "trait", "RegistryEventSink")
                    | (LIFECYCLE_OWNER, "fn", "run_heartbeat_round")
                    | (LIFECYCLE_OWNER, "fn", "spawn_heartbeat_manager")
                    | (CLEANUP_OWNER, "struct", "QueryCleanupSink")
                    | (TRANSPORT_OWNER, "fn", "grpc_heartbeat")
                    | ("src/engine/backend_ops.rs", "fn", "ensure_backend_registry")
                    | (
                        "src/engine/backend_ops.rs",
                        "fn",
                        "wait_for_configured_backends_live"
                    )
            )
    }

    fn scan_items(
        source_rel: &str,
        items: &[syn::Item],
        module_path: &mut Vec<String>,
        protected_names: &[String],
        imports_protected_glob: bool,
        violations: &mut Vec<String>,
    ) {
        for item in items {
            if let syn::Item::Mod(module) = item {
                if definitely_cfg_test(&module.attrs) {
                    continue;
                }
                if let Some((_, nested)) = &module.content {
                    module_path.push(module.ident.to_string());
                    scan_items(
                        source_rel,
                        nested,
                        module_path,
                        protected_names,
                        imports_protected_glob,
                        violations,
                    );
                    module_path.pop();
                }
                continue;
            }
            if let syn::Item::Macro(item) = item {
                if definitely_cfg_test(&item.attrs) {
                    continue;
                }
                let macro_tokens = rust_source_tokens(&item.mac.tokens.to_string())
                    .into_iter()
                    .map(|token| token.text)
                    .collect::<Vec<_>>();
                let exposes_surface = macro_tokens.iter().any(|token| token == "pub")
                    || macro_tokens.iter().any(|token| token == "type");
                let references_owner = imports_protected_glob
                    || protected_names
                        .iter()
                        .any(|protected| macro_tokens.iter().any(|token| token == protected));
                if exposes_surface && references_owner {
                    violations.push(format!(
                        "{source_rel} exposes heartbeat owner through production macro"
                    ));
                }
                continue;
            }
            if let syn::Item::Impl(item) = item {
                if definitely_cfg_test(&item.attrs) {
                    continue;
                }
                let trait_surface = item.trait_.is_some();
                for associated in &item.items {
                    let (attrs, visibility, kind, name, tokens) = match associated {
                        syn::ImplItem::Fn(item) => (
                            item.attrs.as_slice(),
                            Some(&item.vis),
                            "associated fn",
                            item.sig.ident.to_string(),
                            item.to_token_stream().to_string(),
                        ),
                        syn::ImplItem::Const(item) => (
                            item.attrs.as_slice(),
                            Some(&item.vis),
                            "associated const",
                            item.ident.to_string(),
                            item.to_token_stream().to_string(),
                        ),
                        syn::ImplItem::Type(item) => (
                            item.attrs.as_slice(),
                            Some(&item.vis),
                            "associated type",
                            item.ident.to_string(),
                            item.to_token_stream().to_string(),
                        ),
                        syn::ImplItem::Macro(item) => (
                            item.attrs.as_slice(),
                            None,
                            "associated macro",
                            item.mac.path.to_token_stream().to_string(),
                            item.mac.tokens.to_string(),
                        ),
                        _ => continue,
                    };
                    if definitely_cfg_test(attrs)
                        || visibility.is_some_and(|visibility| {
                            matches!(visibility, syn::Visibility::Inherited) && !trait_surface
                        })
                    {
                        continue;
                    }
                    let item_tokens = rust_source_tokens(&tokens)
                        .into_iter()
                        .map(|token| token.text)
                        .collect::<Vec<_>>();
                    if imports_protected_glob
                        || protected_names
                            .iter()
                            .any(|protected| item_tokens.iter().any(|token| token == protected))
                    {
                        violations.push(format!(
                            "{source_rel} exposes heartbeat owner through {kind} {name}"
                        ));
                    }
                }
                continue;
            }

            let (attrs, visibility, kind, name, tokens, always_a_surface) = match item {
                syn::Item::Fn(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "fn",
                    item.sig.ident.to_string(),
                    item.to_token_stream().to_string(),
                    false,
                ),
                syn::Item::Struct(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "struct",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    false,
                ),
                syn::Item::Enum(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "enum",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    false,
                ),
                syn::Item::Union(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "union",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    false,
                ),
                syn::Item::Const(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "const",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    false,
                ),
                syn::Item::Static(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "static",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    false,
                ),
                syn::Item::Trait(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "trait",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    false,
                ),
                syn::Item::Type(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "type",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    true,
                ),
                syn::Item::TraitAlias(item) => (
                    item.attrs.as_slice(),
                    &item.vis,
                    "trait alias",
                    item.ident.to_string(),
                    item.to_token_stream().to_string(),
                    true,
                ),
                _ => continue,
            };
            if definitely_cfg_test(attrs)
                || (!always_a_surface && matches!(visibility, syn::Visibility::Inherited))
                || exact_owner_item_allowed(source_rel, module_path, visibility, kind, &name)
            {
                continue;
            }
            let item_tokens = rust_source_tokens(&tokens)
                .into_iter()
                .map(|token| token.text)
                .collect::<Vec<_>>();
            if imports_protected_glob
                || protected_names
                    .iter()
                    .any(|protected| item_tokens.iter().any(|token| token == protected))
            {
                violations.push(format!(
                    "{source_rel} exposes heartbeat owner through {kind} {name}"
                ));
            }
        }
    }

    let file = syn::parse_file(source).expect("EBD-5B2 wrapper source must parse");
    let mut violations = Vec::new();
    let mut module_path = Vec::new();
    scan_items(
        source_rel,
        &file.items,
        &mut module_path,
        &protected_names,
        imports_protected_glob,
        &mut violations,
    );
    violations
}

fn canonical_owner_violations(sources: &[(&str, &str)]) -> Vec<String> {
    let required = [
        ("trait", "RegistryEventSink", LIFECYCLE_OWNER),
        ("fn", "run_heartbeat_round", LIFECYCLE_OWNER),
        ("fn", "spawn_heartbeat_manager", LIFECYCLE_OWNER),
        ("struct", "QueryCleanupSink", CLEANUP_OWNER),
        ("fn", "grpc_heartbeat", TRANSPORT_OWNER),
    ];
    let mut violations = Vec::new();
    for (kind, symbol, expected_owner) in required {
        let owners = sources
            .iter()
            .flat_map(|(source_rel, source)| {
                top_level_definitions(source)
                    .into_iter()
                    .filter(move |(actual_kind, actual_symbol)| {
                        actual_kind == kind && actual_symbol == symbol
                    })
                    .map(move |_| *source_rel)
            })
            .collect::<Vec<_>>();
        if owners != [expected_owner] {
            violations.push(format!(
                "{kind} {symbol} owners must be [{expected_owner}], actual={owners:?}"
            ));
        }
        let declaration_surfaces = sources
            .iter()
            .flat_map(|(source_rel, source)| {
                named_declaration_surfaces(source, symbol)
                    .into_iter()
                    .map(move |actual_kind| (*source_rel, actual_kind))
            })
            .collect::<Vec<_>>();
        if declaration_surfaces != [(expected_owner, kind.to_string())] {
            violations.push(format!(
                "{symbol} declaration surfaces must be [({expected_owner}, {kind})], actual={declaration_surfaces:?}"
            ));
        }
    }
    for (source_rel, source) in sources {
        violations.extend(forwarding_surface_violations(source_rel, source));
        violations.extend(owner_wrapper_violations(source_rel, source));
    }
    violations
}

fn transport_leak_violations(source_rel: &str, source: &str) -> Vec<String> {
    let forbidden_roots: &[&[&str]] = &[
        &["crate", "service"],
        &["crate", "proto"],
        &["tokio"],
        &["tonic"],
    ];
    let mut violations = rust_production_canonical_paths(source, source_rel)
        .into_iter()
        .filter(|path| {
            forbidden_roots.iter().any(|prefix| {
                path.len() >= prefix.len()
                    && path
                        .iter()
                        .zip(prefix.iter())
                        .all(|(actual, expected)| actual == expected)
            })
        })
        .map(|path| format!("{source_rel} contains transport path {}", path.join("::")))
        .collect::<Vec<_>>();
    let production_tokens = rust_source_tokens(&rust_sanitized_production_text(source))
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    for symbol in [
        "tokio",
        "tonic",
        "NovaRocksGrpcRemoteClient",
        "HeartbeatRequest",
        "HeartbeatResponse",
        "observe_heartbeat_rtt",
    ] {
        if production_tokens.iter().any(|token| token == symbol) {
            violations.push(format!("{source_rel} contains transport symbol {symbol}"));
        }
    }
    violations
}

fn transport_reverse_ownership_violations(source: &str) -> Vec<String> {
    let production_tokens = rust_source_tokens(&rust_sanitized_production_text(source))
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    let forbidden = [
        "BackendRegistry",
        "RegistryEvent",
        "RegistryEventSink",
        "QueryCleanupSink",
        "backend_registry",
        "all_for_heartbeat",
        "apply_heartbeat_result",
        "run_heartbeat_round",
        "spawn_heartbeat_manager",
        "timeout_retries",
        "missed_heartbeats",
        "mark_query_failed",
        "on_backend_failed",
        "cancel_query_by_id",
    ];
    forbidden
        .into_iter()
        .filter(|symbol| production_tokens.iter().any(|token| token == symbol))
        .map(|symbol| format!("heartbeat transport reverse-owns lifecycle symbol {symbol}"))
        .collect()
}

fn ebd_5b2_boundary_violations(repo: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    for owner in [LIFECYCLE_OWNER, CLEANUP_OWNER, TRANSPORT_OWNER] {
        if !repo.join(owner).is_file() {
            violations.push(format!("missing canonical owner {owner}"));
        }
    }
    for old in old_runtime_owner_paths() {
        if repo.join(&old).exists() {
            violations.push(format!("retired runtime owner still exists {old}"));
        }
    }

    let runtime_mod =
        std::fs::read_to_string(repo.join("src/runtime/mod.rs")).expect("read runtime module");
    let runtime_tokens = rust_source_tokens(&rust_lexically_sanitized(&runtime_mod))
        .into_iter()
        .map(|token| token.text)
        .collect::<Vec<_>>();
    for retired in ["heartbeat_mgr", "registry_cleanup"] {
        if runtime_tokens.iter().any(|token| token == retired) {
            violations.push(format!("runtime retains retired module {retired}"));
        }
    }

    let mut production_sources = Vec::new();
    for path in rs_files(&repo.join("src"))
        .into_iter()
        .chain(rs_files(&repo.join("tests")))
    {
        let source_rel = rel(&path);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {source_rel}: {error}"));
        violations.extend(retired_runtime_path_violations(&source_rel, &source));
        if source_rel.starts_with("src/") {
            production_sources.push((source_rel, source));
        }
    }
    let owner_sources = production_sources
        .iter()
        .map(|(source_rel, source)| (source_rel.as_str(), source.as_str()))
        .collect::<Vec<_>>();
    violations.extend(canonical_owner_violations(&owner_sources));

    if repo.join(LIFECYCLE_OWNER).is_file() {
        let lifecycle = std::fs::read_to_string(repo.join(LIFECYCLE_OWNER))
            .expect("read heartbeat lifecycle owner");
        violations.extend(transport_leak_violations(LIFECYCLE_OWNER, &lifecycle));
    }

    if repo.join(CLEANUP_OWNER).is_file() {
        let cleanup_source =
            std::fs::read_to_string(repo.join(CLEANUP_OWNER)).expect("read query cleanup owner");
        violations.extend(transport_leak_violations(CLEANUP_OWNER, &cleanup_source));
        let cleanup = rust_source_tokens(&rust_sanitized_production_text(&cleanup_source))
            .into_iter()
            .map(|token| token.text)
            .collect::<Vec<_>>();
        for required in [
            "mark_query_failed",
            "on_backend_failed",
            "cancel_query_by_id",
        ] {
            if !cleanup.iter().any(|token| token == required) {
                violations.push(format!("query cleanup owner missing {required}"));
            }
        }
    }

    if repo.join(TRANSPORT_OWNER).is_file() {
        let transport_source = std::fs::read_to_string(repo.join(TRANSPORT_OWNER))
            .expect("read heartbeat transport owner");
        violations.extend(transport_reverse_ownership_violations(&transport_source));
        let transport = rust_source_tokens(&rust_sanitized_production_text(&transport_source))
            .into_iter()
            .map(|token| token.text)
            .collect::<Vec<_>>();
        for required in [
            "NovaRocksGrpcRemoteClient",
            "HeartbeatRequest",
            "HeartbeatResponse",
            "observe_heartbeat_rtt",
        ] {
            if !transport.iter().any(|token| token == required) {
                violations.push(format!("heartbeat transport owner missing {required}"));
            }
        }
    }

    violations.sort();
    violations.dedup();
    violations
}

#[test]
fn ebd_5b2_detector_rejects_retired_path_aliases_and_ignores_decoys() {
    for source in [
        "use crate::runtime::heartbeat_mgr as manager;",
        "use crate::runtime::{self as rt}; use rt::registry_cleanup::QueryCleanupSink;",
        "use crate::runtime::{heartbeat_mgr::{self as manager}};",
    ] {
        assert!(
            !retired_runtime_path_violations("src/example.rs", source).is_empty(),
            "fixture must be rejected: {source}"
        );
    }
    for decoy in [
        "// use crate::runtime::heartbeat_mgr;",
        "const NOTE: &str = \"crate::runtime::registry_cleanup\";",
        "const RAW: &str = r#\"use crate::runtime::heartbeat_mgr;\"#;",
    ] {
        assert!(
            retired_runtime_path_violations("src/example.rs", decoy).is_empty(),
            "decoy must be ignored: {decoy}"
        );
    }
}

#[test]
fn ebd_5b2_detector_rejects_transport_leaks_duplicate_owners_and_test_decoys() {
    for leak in [
        "use crate::service as svc; fn leak() { let _ = svc::grpc_client::connect(); }",
        "use crate::proto::novarocks::HeartbeatRequest;",
        "use tokio as runtime; fn leak() { runtime::spawn(async {}); }",
        "use tonic::transport::Channel;",
        "fn wrapper() { NovaRocksGrpcRemoteClient::connect_blocking(todo!()); }",
    ] {
        assert!(
            !transport_leak_violations(LIFECYCLE_OWNER, leak).is_empty(),
            "transport leakage must be rejected: {leak}"
        );
    }
    for decoy in [
        "#[cfg(test)] use crate::service::grpc_client::NovaRocksGrpcRemoteClient;",
        "const NOTE: &str = \"HeartbeatRequest tonic tokio\";",
    ] {
        assert!(
            transport_leak_violations(LIFECYCLE_OWNER, decoy).is_empty(),
            "test/string transport decoy must be ignored: {decoy}"
        );
    }

    let lifecycle =
        "trait RegistryEventSink {} fn run_heartbeat_round() {} fn spawn_heartbeat_manager() {}";
    let cleanup = "struct QueryCleanupSink;";
    let transport = "fn grpc_heartbeat() {}";
    assert!(
        canonical_owner_violations(&[
            (LIFECYCLE_OWNER, lifecycle),
            (CLEANUP_OWNER, cleanup),
            (TRANSPORT_OWNER, transport),
        ])
        .is_empty()
    );
    assert!(
        !canonical_owner_violations(&[
            (LIFECYCLE_OWNER, lifecycle),
            (CLEANUP_OWNER, cleanup),
            (TRANSPORT_OWNER, transport),
            ("src/service/shadow.rs", "fn run_heartbeat_round() {}"),
        ])
        .is_empty(),
        "duplicate production owner must be rejected"
    );
    for forwarding in [
        "type QueryCleanupSink = crate::coordinator::cluster::QueryCleanupSink;",
        "pub type LegacyCleanup = crate::coordinator::cluster::QueryCleanupSink;",
        "pub fn legacy_round() { crate::coordinator::cluster::run_heartbeat_round(); }",
        "use crate::coordinator::cluster::QueryCleanupSink as Cleanup; pub type LegacyCleanup = Cleanup;",
        "pub mod compat { pub type LegacyCleanup = crate::coordinator::cluster::QueryCleanupSink; }",
        "pub(crate) struct LegacyHeartbeat; impl LegacyHeartbeat { pub(crate) fn run_round() { crate::coordinator::cluster::run_heartbeat_round(); } }",
        "use crate::coordinator::cluster::run_heartbeat_round as round; pub(crate) struct LegacyHeartbeat; impl LegacyHeartbeat { pub(crate) fn run_round() { round(); } }",
        "pub(crate) trait LegacyHeartbeat { fn run_round(); } pub(crate) struct Compat; impl LegacyHeartbeat for Compat { fn run_round() { crate::coordinator::cluster::run_heartbeat_round(); } }",
        "use crate::coordinator::cluster::run_heartbeat_round as round; pub(crate) trait LegacyHeartbeat { fn run_round(); } pub(crate) struct Compat; impl LegacyHeartbeat for Compat { fn run_round() { round(); } }",
        "pub(crate) trait LegacyCleanup { type Cleanup; } pub(crate) struct Compat; impl LegacyCleanup for Compat { type Cleanup = crate::coordinator::cluster::QueryCleanupSink; }",
        "pub use crate::coordinator::cluster::QueryCleanupSink;",
        "macro_rules! owner { () => { struct QueryCleanupSink; } }",
        "macro_rules! forward { () => { pub use crate::coordinator::cluster::QueryCleanupSink; } }",
        "macro_rules! wrapper { () => { pub(crate) fn legacy_round() { crate::coordinator::cluster::run_heartbeat_round(); } } }",
    ] {
        assert!(
            !canonical_owner_violations(&[
                (LIFECYCLE_OWNER, lifecycle),
                (CLEANUP_OWNER, cleanup),
                (TRANSPORT_OWNER, transport),
                ("src/service/shadow.rs", forwarding),
            ])
            .is_empty(),
            "owner forwarding surface must be rejected: {forwarding}"
        );
    }
    assert!(
        forwarding_surface_violations(
            "src/coordinator/cluster/mod.rs",
            "pub(crate) use query_cleanup::QueryCleanupSink;",
        )
        .is_empty(),
        "canonical cluster-root export must remain allowed"
    );
    assert!(
        !forwarding_surface_violations(
            "src/coordinator/cluster/mod.rs",
            "pub(crate) use query_cleanup::QueryCleanupSink as Cleanup;",
        )
        .is_empty(),
        "cluster-root alias export must be rejected"
    );
    assert!(
        !owner_wrapper_violations(
            "src/coordinator/cluster/mod.rs",
            "pub(crate) type LegacyCleanup = query_cleanup::QueryCleanupSink;",
        )
        .is_empty(),
        "cluster root must not receive a whole-file wrapper exemption"
    );
    assert!(
        !owner_wrapper_violations(
            "src/engine/backend_ops.rs",
            "pub(crate) fn legacy_round() { crate::coordinator::cluster::run_heartbeat_round(); }",
        )
        .is_empty(),
        "composition owner must allow only exact orchestration consumers"
    );
    assert!(
        !owner_wrapper_violations(
            "src/engine/backend_ops.rs",
            "mod compat { pub(crate) fn ensure_backend_registry() { crate::coordinator::cluster::spawn_heartbeat_manager(); } }",
        )
        .is_empty(),
        "inline modules must not reuse an allowlisted top-level consumer name"
    );
    assert!(
        !owner_wrapper_violations(
            "src/engine/backend_ops.rs",
            "pub fn ensure_backend_registry() { crate::coordinator::cluster::spawn_heartbeat_manager(); }",
        )
        .is_empty(),
        "canonical consumers must retain their exact crate visibility"
    );
    assert!(
        owner_wrapper_violations(
            "src/engine/backend_ops.rs",
            "pub(crate) fn ensure_backend_registry() { crate::coordinator::cluster::spawn_heartbeat_manager(); }",
        )
        .is_empty(),
        "canonical application-composition consumer must remain allowed"
    );
    assert!(
        forwarding_surface_violations(
            "src/service/shadow.rs",
            "pub(crate) struct Helper; macro_rules! harmless { () => {} } use crate::coordinator::cluster::QueryCleanupSink;",
        )
        .is_empty(),
        "nearby public items must not turn a private import into a visible forwarding"
    );
    assert!(
        canonical_owner_violations(&[
            (LIFECYCLE_OWNER, lifecycle),
            (CLEANUP_OWNER, cleanup),
            (TRANSPORT_OWNER, transport),
            (
                "src/service/shadow.rs",
                "#[cfg(test)] fn run_heartbeat_round() {}",
            ),
        ])
        .is_empty(),
        "test-only owner decoy must be ignored"
    );

    for reverse_owner in [
        "fn grpc_heartbeat() { spawn_heartbeat_manager(); }",
        "struct BackendRegistry;",
        "fn cleanup() { mark_query_failed(); cancel_query_by_id(); }",
        "fn reverse() { backend_registry().unwrap().all_for_heartbeat(); }",
        "fn reverse(registry: &R) { registry.apply_heartbeat_result(1, outcome); }",
    ] {
        assert!(
            !transport_reverse_ownership_violations(reverse_owner).is_empty(),
            "transport reverse ownership must be rejected: {reverse_owner}"
        );
    }
}

#[test]
fn ebd_5b2_heartbeat_lifecycle_has_canonical_owners() {
    let violations = ebd_5b2_boundary_violations(Path::new(manifest_dir()));
    assert!(
        violations.is_empty(),
        "EBD-5B2 heartbeat lifecycle boundary violations:\n{}",
        violations.join("\n")
    );
}
