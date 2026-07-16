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

use std::fs;

use super::{
    production_rs_files_from_entries, rel, rust_production_canonical_paths,
    rust_sanitized_production_text, rust_use_tokens, src_dir,
};

const FORBIDDEN_STATE_STORE_OWNERS: &[&str] = &[
    "catalog",
    "connector",
    "coordinator",
    "dictionary",
    "dml",
    "engine",
    "frontend",
    "meta",
    "mv",
    "sql",
    "table_maintenance",
];

const FORBIDDEN_STATE_STORE_TOKENS: &[&str] = &[
    "DictionaryDefinition",
    "IcebergTable",
    "MaterializedView",
    "MetaStoreProvider",
    "TPlanNode",
    "apache_avro",
];

const SQLITE_ONLY_EXTERNAL_OWNERS: &[&str] = &["fs2", "rusqlite"];
const SQLITE_ONLY_FFI_TOKENS: &[&str] = &["SQLITE_BUSY", "SQLITE_BUSY_SNAPSHOT"];
const FOUNDATIONDB_EXTERNAL_OWNERS: &[&str] = &["foundationdb", "foundationdb_sys"];
const FOUNDATIONDB_RAW_TOKENS: &[&str] = &[
    "FdbError",
    "FdbResult",
    "DatabaseOption",
    "NetworkOption",
    "TransactionOption",
    "MutationType",
    "Versionstamp",
];
const FOUNDATIONDB_FORBIDDEN_OWNER_TOKENS: &[&str] = &[
    "run",
    "transact",
    "on_error",
    "watch",
    "tuple",
    "directory",
    "fallback",
];

#[derive(Clone)]
struct GuardSource {
    path: String,
    text: String,
}

impl GuardSource {
    fn new(path: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            text: text.into(),
        }
    }
}

fn is_state_store_source(path: &str) -> bool {
    path.starts_with("src/state_store/")
}

fn is_connector_source(path: &str) -> bool {
    path.starts_with("src/connector/")
}

fn is_state_store_sqlite_source(path: &str) -> bool {
    path == "src/state_store/sqlite.rs" || path.starts_with("src/state_store/sqlite/")
}

fn is_state_store_foundationdb_source(path: &str) -> bool {
    path.starts_with("src/state_store/foundationdb/")
}

fn is_foundationdb_native_owner(path: &str) -> bool {
    is_state_store_foundationdb_source(path) || path == "src/state_store/runtime.rs"
}

fn path_starts_with(path: &[String], prefix: &[&str]) -> bool {
    path.len() >= prefix.len()
        && path
            .iter()
            .zip(prefix)
            .all(|(actual, expected)| actual == expected)
}

fn has_unqualified_path(tokens: &[String], owner: &str) -> bool {
    tokens
        .windows(2)
        .any(|tokens| tokens[0] == owner && tokens[1] == "::")
}

fn declares_module(tokens: &[String], owner: &str) -> bool {
    tokens.windows(3).any(|tokens| {
        tokens[0] == "mod" && tokens[1] == owner && matches!(tokens[2].as_str(), ";" | "{")
    })
}

fn references_absolute_or_extern_owner(tokens: &[String], owner: &str) -> bool {
    tokens
        .windows(2)
        .any(|tokens| tokens[0] == "::" && tokens[1] == owner)
        || tokens
            .windows(3)
            .any(|tokens| tokens[0] == "extern" && tokens[1] == "crate" && tokens[2] == owner)
}

fn state_store_boundary_violations(sources: &[GuardSource]) -> Vec<String> {
    let mut violations = Vec::new();
    for source in sources {
        let production = rust_sanitized_production_text(&source.text);
        let paths = rust_production_canonical_paths(&production, &source.path);
        let production_tokens = rust_use_tokens(&production);

        if is_state_store_source(&source.path) {
            for path in &paths {
                if path_starts_with(path, &["crate"])
                    && path
                        .get(1)
                        .is_some_and(|owner| FORBIDDEN_STATE_STORE_OWNERS.contains(&owner.as_str()))
                {
                    violations.push(format!(
                        "state-store-forbidden-owner: {} -> {}",
                        source.path,
                        path.join("::")
                    ));
                }
            }

            let tokens = rust_use_tokens(&production);
            for forbidden in FORBIDDEN_STATE_STORE_TOKENS {
                if tokens.iter().any(|token| token == forbidden) {
                    violations.push(format!(
                        "state-store-forbidden-token: {} -> {forbidden}",
                        source.path
                    ));
                }
            }
        }

        for path in &paths {
            if is_connector_source(&source.path)
                && path_starts_with(path, &["crate", "state_store"])
            {
                violations.push(format!(
                    "connector-state-store-dependency: {} -> {}",
                    source.path,
                    path.join("::")
                ));
            }

            if source.path != "src/state_store/mod.rs"
                && !is_state_store_sqlite_source(&source.path)
                && path_starts_with(path, &["crate", "state_store", "sqlite"])
            {
                violations.push(format!(
                    "state-store-sqlite-import-outside-owner: {} -> {}",
                    source.path,
                    path.join("::")
                ));
            }

            if is_state_store_source(&source.path) && !is_state_store_sqlite_source(&source.path) {
                for owner in SQLITE_ONLY_EXTERNAL_OWNERS {
                    if let Some(owner_index) = path.iter().position(|segment| segment == owner)
                        && (!declares_module(&production_tokens, owner)
                            || references_absolute_or_extern_owner(&production_tokens, owner))
                    {
                        violations.push(format!(
                            "state-store-sqlite-external-outside-owner: {} -> {}",
                            source.path,
                            path[owner_index..].join("::")
                        ));
                    }
                }
            }
        }

        if is_state_store_source(&source.path) && !is_state_store_sqlite_source(&source.path) {
            for owner in SQLITE_ONLY_EXTERNAL_OWNERS {
                if production_tokens.iter().any(|token| token == owner)
                    && (!declares_module(&production_tokens, owner)
                        || references_absolute_or_extern_owner(&production_tokens, owner))
                {
                    violations.push(format!(
                        "state-store-sqlite-external-outside-owner: {} -> {owner}",
                        source.path
                    ));
                }
            }
            for token in SQLITE_ONLY_FFI_TOKENS {
                if production_tokens.iter().any(|actual| actual == token) {
                    violations.push(format!(
                        "state-store-sqlite-ffi-outside-owner: {} -> {token}",
                        source.path
                    ));
                }
            }
        }

        if !is_foundationdb_native_owner(&source.path) {
            for path in &paths {
                for owner in FOUNDATIONDB_EXTERNAL_OWNERS {
                    if let Some(owner_index) = path.iter().position(|segment| segment == owner)
                        && (!declares_module(&production_tokens, owner)
                            || references_absolute_or_extern_owner(&production_tokens, owner))
                    {
                        violations.push(format!(
                            "state-store-foundationdb-native-outside-owner: {} -> {}",
                            source.path,
                            path[owner_index..].join("::")
                        ));
                    }
                }
            }
            for owner in FOUNDATIONDB_EXTERNAL_OWNERS {
                if production_tokens.iter().any(|token| token == owner)
                    && (!declares_module(&production_tokens, owner)
                        || references_absolute_or_extern_owner(&production_tokens, owner))
                {
                    violations.push(format!(
                        "state-store-foundationdb-native-outside-owner: {} -> {owner}",
                        source.path
                    ));
                }
            }
            for token in FOUNDATIONDB_RAW_TOKENS {
                if production_tokens.iter().any(|actual| actual == token) {
                    violations.push(format!(
                        "state-store-foundationdb-token-outside-owner: {} -> {token}",
                        source.path
                    ));
                }
            }
        }

        if is_foundationdb_native_owner(&source.path) {
            for token in FOUNDATIONDB_FORBIDDEN_OWNER_TOKENS {
                let is_member_api = matches!(*token, "run" | "transact" | "on_error");
                let present = if is_member_api {
                    production_tokens
                        .windows(2)
                        .enumerate()
                        .any(|(index, tokens)| {
                            if !matches!(tokens[0].as_str(), "." | "::") || tokens[1] != *token {
                                return false;
                            }
                            let explicit_network_runner = *token == "run"
                                && source.path == "src/state_store/runtime.rs"
                                && tokens[0] == "::"
                                && index > 0
                                && production_tokens[index - 1] == "NetworkRunner";
                            !explicit_network_runner
                        })
                } else {
                    production_tokens.iter().any(|actual| actual == token)
                };
                if present {
                    violations.push(format!(
                        "state-store-foundationdb-forbidden-api: {} -> {token}",
                        source.path
                    ));
                }
            }
        }
        if is_connector_source(&source.path)
            && paths.iter().any(|path| path.as_slice() == ["crate", "*"])
            && has_unqualified_path(&production_tokens, "state_store")
            && !declares_module(&production_tokens, "state_store")
        {
            violations.push(format!(
                "connector-state-store-dependency: {} -> crate::* + state_store::",
                source.path
            ));
        }
    }
    violations.sort();
    violations.dedup();
    violations
}

#[test]
fn state_store_owner_is_non_vacuous_and_obeys_boundary() {
    let src = src_dir();
    let owner = src.join("state_store");
    assert!(
        owner.is_dir(),
        "state store owner must exist at {}",
        rel(&owner)
    );

    let files = production_rs_files_from_entries(&src, &[src.join("lib.rs"), src.join("main.rs")]);
    let owner_files = files
        .iter()
        .filter(|path| path.starts_with(&owner))
        .collect::<Vec<_>>();
    assert!(
        !owner_files.is_empty(),
        "state store owner must contain reachable production Rust sources"
    );

    let sources = files
        .iter()
        .map(|path| {
            GuardSource::new(
                rel(path),
                fs::read_to_string(path).expect("read state store source"),
            )
        })
        .collect::<Vec<_>>();
    let violations = state_store_boundary_violations(&sources);
    assert!(
        violations.is_empty(),
        "state store architecture boundary failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn state_store_boundary_detector_rejects_forbidden_imports() {
    let sources = [
        GuardSource::new("src/state_store/contract.rs", "use crate::meta::*;"),
        GuardSource::new("src/state_store/contract.rs", "use crate::connector::*;"),
        GuardSource::new(
            "src/connector/state_store.rs",
            "use crate::state_store::StateStoreConfig;",
        ),
        GuardSource::new(
            "src/catalog/reexport.rs",
            "pub use crate::state_store::sqlite::*;",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);

    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::meta::*")),
        "meta dependency must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::connector::*")),
        "connector dependency must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::state_store::sqlite::*")),
        "sqlite adapter re-export must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("connector-state-store-dependency")),
        "connector dependency on state store must be rejected: {violations:?}"
    );
}

#[test]
fn state_store_boundary_detector_rejects_canonical_alias_group_and_glob_bypasses() {
    let sources = [
        GuardSource::new(
            "src/state_store/contract.rs",
            "use crate::{meta as metadata}; fn leak() { metadata::MetaStoreProvider::open(); }",
        ),
        GuardSource::new(
            "src/state_store/contract.rs",
            "use crate::*; fn leak() { meta::MetaStoreProvider::open(); }",
        ),
        GuardSource::new(
            "src/catalog/reexport.rs",
            "use crate::state_store as durable; pub use durable::sqlite::*;",
        ),
        GuardSource::new(
            "src/connector/state_store.rs",
            "use crate::*; fn leak(_: state_store::StateStoreConfig) {}",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);

    for expected in [
        "state-store-forbidden-owner: src/state_store/contract.rs -> crate::meta",
        "state-store-sqlite-import-outside-owner: src/catalog/reexport.rs -> crate::state_store::sqlite::*",
        "connector-state-store-dependency: src/connector/state_store.rs -> crate::* + state_store::",
    ] {
        assert!(
            violations.iter().any(|violation| violation == expected),
            "canonical alias/group/glob dependency escaped detection: expected={expected}, violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_rejects_each_forbidden_token() {
    let fixtures = [
        ("MetaStoreProvider", "use crate::safe::MetaStoreProvider;"),
        ("apache_avro", "use apache_avro::Schema;"),
        ("TPlanNode", "use crate::safe::TPlanNode;"),
        ("IcebergTable", "use crate::safe::IcebergTable;"),
        ("MaterializedView", "use crate::safe::MaterializedView;"),
        (
            "DictionaryDefinition",
            "use crate::safe::DictionaryDefinition;",
        ),
    ];

    for (token, text) in fixtures {
        let violations = state_store_boundary_violations(&[GuardSource::new(
            "src/state_store/contract.rs",
            text,
        )]);
        assert!(
            violations.iter().any(|item| item
                == &format!("state-store-forbidden-token: src/state_store/contract.rs -> {token}")),
            "forbidden token {token} must be rejected: {violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_denylist_matches_the_ss1_contract() {
    assert_eq!(
        FORBIDDEN_STATE_STORE_OWNERS,
        [
            "catalog",
            "connector",
            "coordinator",
            "dictionary",
            "dml",
            "engine",
            "frontend",
            "meta",
            "mv",
            "sql",
            "table_maintenance",
        ]
    );
    assert_eq!(
        FORBIDDEN_STATE_STORE_TOKENS,
        [
            "DictionaryDefinition",
            "IcebergTable",
            "MaterializedView",
            "MetaStoreProvider",
            "TPlanNode",
            "apache_avro",
        ]
    );
}

#[test]
fn state_store_boundary_detector_allows_sqlite_adapter_internal_imports() {
    let sources = [
        GuardSource::new(
            "src/state_store/sqlite/mod.rs",
            "use self::schema::SqliteSchema;",
        ),
        GuardSource::new(
            "src/state_store/sqlite/transaction.rs",
            "use super::schema::SqliteSchema;",
        ),
        GuardSource::new(
            "src/state_store/mod.rs",
            "pub use crate::state_store::sqlite::SqliteStateStore;",
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_rejects_sqlite_external_crates_and_ffi_tokens_outside_owner() {
    let sources = [
        GuardSource::new("src/state_store/contract.rs", "use rusqlite::Connection;"),
        GuardSource::new(
            "src/state_store/config.rs",
            "use rusqlite::{Connection as Db, ffi::*};",
        ),
        GuardSource::new("src/state_store/runner.rs", "use fs2::FileExt as LockExt;"),
        GuardSource::new(
            "src/state_store/remote/mod.rs",
            "use fs2::*; const CODE: i32 = SQLITE_BUSY_SNAPSHOT;",
        ),
        GuardSource::new(
            "src/state_store/future_provider.rs",
            "extern crate rusqlite as db; fn leak(_: db::Connection) {}",
        ),
        GuardSource::new(
            "src/state_store/runner.rs",
            "extern crate fs2 as locks; fn leak<T: locks::FileExt>() {}",
        ),
        GuardSource::new(
            "src/state_store/shadowed_rusqlite.rs",
            "mod rusqlite {} use ::rusqlite::{Connection as Db}; fn leak(_: Db) {}",
        ),
        GuardSource::new(
            "src/state_store/shadowed_fs2.rs",
            "mod fs2 {} extern crate fs2 as locks; fn leak<T: locks::FileExt>() {}",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);
    for (path, dependency) in [
        ("src/state_store/contract.rs", "rusqlite::Connection"),
        ("src/state_store/config.rs", "rusqlite::ffi::*"),
        ("src/state_store/runner.rs", "fs2::FileExt"),
        ("src/state_store/remote/mod.rs", "fs2::*"),
        ("src/state_store/remote/mod.rs", "SQLITE_BUSY_SNAPSHOT"),
        ("src/state_store/future_provider.rs", "rusqlite"),
        ("src/state_store/runner.rs", "fs2"),
        ("src/state_store/shadowed_rusqlite.rs", "rusqlite"),
        ("src/state_store/shadowed_fs2.rs", "fs2"),
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(path) && violation.contains(dependency)),
            "SQLite-only dependency escaped at {path}: {dependency}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_allows_truly_local_shadow_modules() {
    let sources = [
        GuardSource::new(
            "src/state_store/local_rusqlite.rs",
            "mod rusqlite { pub struct Connection; } \
             use rusqlite::Connection; fn local(_: Connection) {}",
        ),
        GuardSource::new(
            "src/state_store/local_fs2.rs",
            "mod fs2 { pub trait FileExt {} } \
             use fs2::FileExt; fn local<T: FileExt>() {}",
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_allows_sqlite_external_crates_and_ffi_tokens_in_owner() {
    let sources = [GuardSource::new(
        "src/state_store/sqlite/txn.rs",
        "use rusqlite::{Connection, ffi::*}; use fs2::FileExt; \
         const BUSY: i32 = SQLITE_BUSY; const SNAPSHOT: i32 = SQLITE_BUSY_SNAPSHOT;",
    )];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_rejects_foundationdb_native_leaks_outside_owner() {
    let sources = [
        GuardSource::new(
            "src/state_store/config.rs",
            "use foundationdb::options::NetworkOption;",
        ),
        GuardSource::new(
            "src/state_store/runner.rs",
            "extern crate foundationdb_sys as fdb; fn leak(_: FdbError) {}",
        ),
        GuardSource::new(
            "src/connector/state_store.rs",
            "use crate::safe::Versionstamp;",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);
    for (path, token) in [
        ("src/state_store/config.rs", "foundationdb"),
        ("src/state_store/config.rs", "NetworkOption"),
        ("src/state_store/runner.rs", "foundationdb_sys"),
        ("src/state_store/runner.rs", "FdbError"),
        ("src/connector/state_store.rs", "Versionstamp"),
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(path) && violation.contains(token)),
            "FoundationDB native detail escaped at {path}: {token}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_allows_foundationdb_native_details_only_in_owner() {
    let sources = [
        GuardSource::new(
            "src/state_store/foundationdb/mod.rs",
            "use foundationdb::{Database, FdbError, options::TransactionOption};",
        ),
        GuardSource::new(
            "src/state_store/runtime.rs",
            "use foundationdb::{api::NetworkRunner, options::NetworkOption}; \
             fn start(runner: NetworkRunner) { unsafe { NetworkRunner::run(runner); } }",
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}

#[test]
fn state_store_boundary_detector_rejects_foundationdb_owner_domain_and_forbidden_apis() {
    let sources = [
        GuardSource::new(
            "src/state_store/foundationdb/mod.rs",
            "use crate::engine::Engine; fn bad(db: Database) { db.run(); db.transact(); Database::run(); }",
        ),
        GuardSource::new(
            "src/state_store/foundationdb/txn.rs",
            "fn bad(tx: Transaction) { tx.on_error(); tx.watch(); tuple(); directory(); fallback(); }",
        ),
    ];

    let violations = state_store_boundary_violations(&sources);
    for token in [
        "crate::engine",
        "run",
        "transact",
        "on_error",
        "watch",
        "tuple",
        "directory",
        "fallback",
    ] {
        assert!(
            violations.iter().any(|violation| violation.contains(token)),
            "forbidden FoundationDB owner token escaped: {token}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_boundary_detector_rejects_foundationdb_runtime_forbidden_apis() {
    let violations = state_store_boundary_violations(&[GuardSource::new(
        "src/state_store/runtime.rs",
        "fn bad(db: Database, tx: Transaction) { \
         db.run(); Database::run(); db.transact(); tx.on_error(); tx.watch(); \
         tuple(); directory(); fallback(); }",
    )]);

    for token in [
        "run",
        "transact",
        "on_error",
        "watch",
        "tuple",
        "directory",
        "fallback",
    ] {
        let expected_suffix = format!(" -> {token}");
        assert!(
            violations
                .iter()
                .any(|violation| violation.ends_with(&expected_suffix)),
            "forbidden FoundationDB runtime token escaped: {token}; violations={violations:?}"
        );
    }
}

#[test]
fn state_store_foundationdb_boundary_denylist_matches_ss2_contract() {
    assert_eq!(
        FOUNDATIONDB_EXTERNAL_OWNERS,
        ["foundationdb", "foundationdb_sys"]
    );
    assert_eq!(
        FOUNDATIONDB_RAW_TOKENS,
        [
            "FdbError",
            "FdbResult",
            "DatabaseOption",
            "NetworkOption",
            "TransactionOption",
            "MutationType",
            "Versionstamp",
        ]
    );
    assert_eq!(
        FOUNDATIONDB_FORBIDDEN_OWNER_TOKENS,
        [
            "run",
            "transact",
            "on_error",
            "watch",
            "tuple",
            "directory",
            "fallback",
        ]
    );
}

#[test]
fn state_store_foundationdb_provider_variant_is_feature_independent() {
    let config = src_dir().join("state_store/config.rs");
    let syntax = syn::parse_file(&fs::read_to_string(&config).expect("read state store config"))
        .expect("parse state store config");
    let provider = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(item) if item.ident == "StateStoreProviderConfig" => Some(item),
            _ => None,
        })
        .expect("StateStoreProviderConfig must exist in production config");
    let foundationdb = provider
        .variants
        .iter()
        .find(|variant| variant.ident == "Foundationdb")
        .expect("Foundationdb provider variant must exist when the feature is off");

    assert!(
        foundationdb
            .attrs
            .iter()
            .all(|attribute| !attribute.path().is_ident("cfg")),
        "Foundationdb config variant must not be feature-gated"
    );
}

#[test]
fn foundationdb_workflow_covers_each_gate_owner_exactly_once() {
    let workflow_path = src_dir()
        .parent()
        .expect("workspace root")
        .join(".github/workflows/foundationdb-state-store.yml");
    let workflow = fs::read_to_string(&workflow_path).expect("read FoundationDB workflow");

    assert_eq!(
        workflow.matches("runs-on: ubuntu-24.04").count(),
        1,
        "FoundationDB must have one dedicated Linux production gate"
    );
    assert_eq!(
        workflow
            .matches("run: tools/ci/foundationdb-provider.sh")
            .count(),
        1,
        "the dedicated workflow must invoke the production gate exactly once"
    );

    for owner in [
        "src/common/app_config.rs",
        "novarocks.toml.example",
        "tests/common/mod.rs",
        "tests/architecture_guard/state_store_boundary.rs",
        "tests/cluster_mvp.rs",
    ] {
        let trigger = format!("      - \"{owner}\"");
        assert_eq!(
            workflow.lines().filter(|line| *line == trigger).count(),
            1,
            "FoundationDB gate owner `{owner}` must trigger the unique Linux gate exactly once"
        );
    }
}

#[test]
fn foundationdb_operator_readme_matches_structured_commit_log_contract() {
    let readme_path = src_dir()
        .parent()
        .expect("workspace root")
        .join("docker/foundationdb/README.md");
    let readme = fs::read_to_string(&readme_path).expect("read FoundationDB operator guide");
    let normalized = readme.split_whitespace().collect::<Vec<_>>().join(" ");

    for field in [
        "`transaction_id`",
        "`phase`",
        "`native_error_code`",
        "`category`",
    ] {
        assert!(
            readme.contains(field),
            "FoundationDB operator log allowlist must name exact commit field {field}"
        );
    }
    for forbidden in [
        "logical keys or values",
        "cluster-file contents",
        "raw keyspace UUID",
    ] {
        assert!(
            normalized.contains(forbidden),
            "FoundationDB operator log denylist must retain `{forbidden}`"
        );
    }
}

#[test]
fn state_store_boundary_detector_ignores_non_production_noise() {
    let sources = [
        GuardSource::new(
            "src/state_store/contract.rs",
            r#"
// use crate::meta::*;
// use crate::safe::{MetaStoreProvider, TPlanNode, MaterializedView};
// use apache_avro::Schema;
const EXAMPLE: &str = "crate::connector::{IcebergTable, DictionaryDefinition}";
const CONNECTOR_EXAMPLE: &str = "crate::state_store::StateStoreConfig";
"#,
        ),
        GuardSource::new(
            "src/state_store/contract.rs",
            r#"
#[cfg(test)]
mod tests {
    use apache_avro::Schema;
    use crate::safe::{
        DictionaryDefinition, IcebergTable, MaterializedView, MetaStoreProvider, TPlanNode,
    };
    use crate::state_store::sqlite::*;
}
"#,
        ),
        GuardSource::new(
            "src/connector/state_store.rs",
            r#"
// use crate::state_store::StateStoreConfig;
const EXAMPLE: &str = "crate::state_store::StateStoreConfig";
#[cfg(test)]
use crate::state_store::StateStoreConfig;
"#,
        ),
        GuardSource::new(
            "src/connector/local.rs",
            r#"
use crate::*;
mod state_store {
    pub fn local_helper() {}
}
fn use_local_module() { state_store::local_helper(); }
"#,
        ),
    ];

    assert!(state_store_boundary_violations(&sources).is_empty());
}
