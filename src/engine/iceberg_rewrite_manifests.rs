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

//! Standalone-mode iceberg `ALTER TABLE x REWRITE MANIFESTS` entry point.
//!
//! Routes from `mod.rs::execute_in_context` for any iceberg target. Synchronous
//! execution; OCC retry via `commit::retry::commit_with_retry`.
//!
//! Mirrors `iceberg_truncate.rs` structurally: resolve catalog entry →
//! build Hadoop catalog handle → `block_on_iceberg` → `run_rewrite_manifests`.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::{StandaloneState, StatementResult};

pub(crate) fn execute_iceberg_rewrite_manifests(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<StatementResult, String> {
    debug_assert_eq!(target.backend_name, "iceberg");

    // 1. Resolve catalog entry + build iceberg-rust Catalog handle.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    // Invalidate any cached table state so we always see the latest metadata.
    entry.invalidate_table_cache(&target.namespace, &target.table);

    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );

    // 2. Execute asynchronously inside the iceberg tokio runtime.
    block_on_iceberg(async move {
        crate::connector::iceberg::commit::rewrite_manifests::run_rewrite_manifests(
            catalog,
            table_ident,
        )
        .await
    })?
    .map_err(|e| {
        format!(
            "REWRITE MANIFESTS failed for {}.{}.{}: {e}",
            target.catalog, target.namespace, target.table
        )
    })?;

    Ok(StatementResult::Ok)
}
