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

fn path_starts_with(path: &[String], prefix: &[&str]) -> bool {
    path.len() >= prefix.len()
        && path
            .iter()
            .zip(prefix)
            .all(|(actual, expected)| actual == expected)
}

fn state_store_boundary_violations(sources: &[GuardSource]) -> Vec<String> {
    let mut violations = Vec::new();
    for source in sources {
        let production = rust_sanitized_production_text(&source.text);
        let paths = rust_production_canonical_paths(&production, &source.path);

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
                && path_starts_with(path, &["crate", "state_store", "sqlite"])
            {
                violations.push(format!(
                    "state-store-sqlite-import-outside-owner: {} -> {}",
                    source.path,
                    path.join("::")
                ));
            }
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
}

#[test]
fn state_store_boundary_detector_ignores_non_production_noise() {
    let source = GuardSource::new(
        "src/state_store/contract.rs",
        r#"
// use crate::meta::*;
const EXAMPLE: &str = "crate::connector::IcebergTable";
#[cfg(test)]
mod tests {
    use crate::meta::MetaStoreProvider;
    use crate::state_store::sqlite::*;
}
"#,
    );

    assert!(state_store_boundary_violations(&[source]).is_empty());
}
