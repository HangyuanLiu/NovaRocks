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

//! Standalone-mode iceberg `ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '<ts>'`
//! entry point.
//!
//! Routes from `mod.rs::execute_in_context` for any iceberg target. Synchronous
//! execution; no OCC retry needed (ORPHAN does not commit a new metadata.json).
//!
//! Mirrors `iceberg_expire_snapshots.rs` structurally: resolve catalog entry →
//! build Hadoop catalog handle → `block_on_iceberg` → `run_remove_orphan_files`.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::remove_orphan_files::run_remove_orphan_files;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::statement::AlterTableRemoveOrphanFilesStmt;
use crate::engine::{StandaloneState, StatementResult};

/// Execute `ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '<ts>'` for an
/// iceberg-backed table.
///
/// Resolves the catalog entry from `state`, builds a Hadoop catalog handle,
/// and runs `run_remove_orphan_files` inside the iceberg tokio runtime.
///
/// On success logs the outcome (deleted file count + scanned count) at INFO
/// level and returns `StatementResult::Ok`.
pub(crate) fn execute_iceberg_remove_orphan_files(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    stmt: &AlterTableRemoveOrphanFilesStmt,
) -> Result<StatementResult, String> {
    debug_assert_eq!(
        target.backend_name, "iceberg",
        "execute_iceberg_remove_orphan_files called with non-iceberg backend"
    );

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

    let older_than_ms = stmt.older_than_ms;

    // Retrieve the object-store config (Some for S3/OSS catalogs, None for local).
    let object_store_config = entry.object_store_config().cloned();

    // 2. Execute asynchronously inside the iceberg tokio runtime.
    let outcome = block_on_iceberg(async move {
        run_remove_orphan_files(
            catalog,
            table_ident,
            older_than_ms,
            object_store_config.as_ref(),
        )
        .await
    })?
    .map_err(|e| {
        format!(
            "REMOVE ORPHAN FILES failed for {}.{}.{}: {e}",
            target.catalog, target.namespace, target.table
        )
    })?;

    tracing::info!(
        deleted_count = outcome.deleted_count,
        scanned_count = outcome.scanned_count,
        catalog = %target.catalog,
        namespace = %target.namespace,
        table = %target.table,
        older_than_ms = older_than_ms,
        "remove_orphan_files: completed"
    );

    Ok(StatementResult::Ok)
}
