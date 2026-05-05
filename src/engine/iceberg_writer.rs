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

//! Standalone-mode iceberg INSERT INTO / INSERT OVERWRITE entry point.
//!
//! Routes from `insert_flow::run_insert` for any iceberg target whose source
//! is `FromQuery`, plus all iceberg targets when `overwrite = true`.
//!
//! Phase 1 scope (per spec §0.4):
//! * `INSERT INTO iceberg ... SELECT ...` — handled here.
//! * `INSERT OVERWRITE iceberg ... SELECT ...` — handled here.
//! * `INSERT INTO iceberg VALUES (...)` — keeps using the existing fast-append
//!   helper at `connector::iceberg::catalog::registry::insert_rows`.
//! * `INSERT OVERWRITE iceberg VALUES (...)` — rejected with a clear error;
//!   future Phase 1.x can lift this if the use case arises.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    CleanupPathMapper, CommitOpKind, IcebergCommitCollector, RunInput, WrittenFile,
    ensure_iceberg_write_supported, ensure_no_equality_deletes,
    ensure_overwrite_single_partition_spec, run_iceberg_commit,
};
use crate::connector::starrocks::managed::mv_refresh::query_result_to_chunks;
use crate::connector::starrocks::managed::mv_refresh_iceberg::write_chunks_as_iceberg_data_files;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::{StandaloneState, StatementResult};
use crate::exec::chunk::Chunk;
use crate::sql::parser::ast::InsertSource;

pub(crate) fn execute_iceberg_insert_or_overwrite(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    _resolved: &ResolvedTable,
    _insert_columns: &[String],
    source: &InsertSource,
    overwrite: bool,
) -> Result<StatementResult, String> {
    debug_assert_eq!(target.backend_name, "iceberg");

    // Phase 1 scope: Only FromQuery is supported on this new path. Other
    // sources fall back to the existing literal-INSERT iceberg path
    // (fast-append only) when overwrite=false; OVERWRITE for them is
    // rejected explicitly so the caller learns the limit early.
    let query = match source {
        InsertSource::FromQuery(q) => q.as_ref(),
        InsertSource::Values(_) | InsertSource::SelectLiteralRow(_) => {
            return Err(
                "phase 1 INSERT OVERWRITE iceberg requires a SELECT source; \
                        VALUES is not yet supported on the OVERWRITE path"
                    .to_string(),
            );
        }
        InsertSource::UnionAll(_) => {
            return Err(
                "phase 1 INSERT OVERWRITE iceberg does not support UNION ALL sources".to_string(),
            );
        }
        InsertSource::GenerateSeriesSelect(_) => {
            return Err(
                "phase 1 INSERT OVERWRITE iceberg does not support generate_series sources"
                    .to_string(),
            );
        }
    };
    debug_assert!(overwrite || matches!(source, InsertSource::FromQuery(_)));

    // 1. Resolve catalog entry + build iceberg-rust Catalog handle.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table =
        block_on_iceberg(async { catalog.load_table(&table_ident).await })?.map_err(|e| {
            format!(
                "load iceberg table {target_str}: {e}",
                target_str = target_string(target)
            )
        })?;

    // 2. Pre-lowering validators.
    let _write_mode = ensure_iceberg_write_supported(&table)?;
    if overwrite {
        ensure_overwrite_single_partition_spec(&table)?;
        ensure_no_equality_deletes(&table)?;
    }

    // 3. Run the SELECT and convert to chunks.
    let chunks = run_select_to_chunks(state, target, query)?;

    // 4. Write data files. Empty input → no-op for INSERT INTO; for OVERWRITE
    //    an empty SELECT means "clear the table" so we still go through
    //    OverwriteCommit which handles the empty-written + non-empty-base case.
    let data_files: Vec<DataFile> = if chunks.iter().all(|c| c.batch.num_rows() == 0) {
        Vec::new()
    } else {
        block_on_iceberg(async { write_chunks_as_iceberg_data_files(&table, &chunks).await })??
    };

    // 5. Build the collector and inject every written file.
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        if overwrite {
            CommitOpKind::Overwrite
        } else {
            CommitOpKind::FastAppend
        },
        table_ident,
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let default_spec_id = metadata.default_partition_spec_id();
    for df in data_files {
        let wf = data_file_to_written_file(&df, default_spec_id)?;
        collector.inject_written_file(wf);
    }

    // 6. Build the OpenDAL Operator + FileIO.
    let abort_cleanup = build_abort_cleanup_for_catalog_entry(&entry)?;
    let file_io = table.file_io().clone();

    // 7. Drive commit + abort cleanup on failure.
    let _outcome = block_on_iceberg(async {
        run_iceberg_commit(RunInput {
            collector: collector.clone(),
            catalog: catalog.clone(),
            table,
            fs: abort_cleanup.fs,
            file_io,
            cleanup_path_mapper: abort_cleanup.path_mapper,
        })
        .await
    })??;

    // 8. Invalidate the iceberg entry's table cache so subsequent SELECTs
    //    see the new snapshot. The standalone catalog rebuilds its TableDef
    //    on the next register_iceberg_tables_for_query call.
    invalidate_iceberg_caches(state, target)?;

    Ok(StatementResult::Ok)
}

pub(crate) fn invalidate_iceberg_caches(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<(), String> {
    {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        let entry = registry.get(&target.catalog)?;
        entry.invalidate_table_cache(&target.namespace, &target.table);
    }
    {
        let mut local = state
            .catalog
            .write()
            .map_err(|e| format!("standalone catalog write lock: {e}"))?;
        let _ = local.drop_table(&target.namespace, &target.table);
    }
    Ok(())
}

fn target_string(t: &TargetBackend) -> String {
    format!("{}.{}.{}", t.catalog, t.namespace, t.table)
}

pub(crate) fn data_file_to_written_file(
    df: &DataFile,
    partition_spec_id: i32,
) -> Result<WrittenFile, String> {
    Ok(WrittenFile {
        path: df.file_path().to_string(),
        format: df.file_format(),
        content: df.content_type(),
        partition_values: df.partition().clone(),
        partition_spec_id,
        record_count: df.record_count(),
        file_size_in_bytes: df.file_size_in_bytes(),
        split_offsets: df.split_offsets().map(|s| s.to_vec()).unwrap_or_default(),
        column_sizes: df.column_sizes().clone(),
        value_counts: df.value_counts().clone(),
        null_value_counts: df.null_value_counts().clone(),
        key_metadata: df.key_metadata().map(|s| s.to_vec()),
        referenced_data_file: df.referenced_data_file().map(|s| s.to_string()),
        equality_ids: df.equality_ids(),
    })
}

pub(crate) fn run_select_to_chunks(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    query: &sqlparser::ast::Query,
) -> Result<Vec<Chunk>, String> {
    // Force-refresh every iceberg table referenced by the SELECT. The
    // simpler `register_iceberg_tables_for_query` skips already-registered
    // tables, but the table backing the INSERT may have been mutated by a
    // prior statement in the same session and the cached `TableDef` would
    // miss the new files. Refreshing here is mandatory before running the
    // SELECT so it sees all data files committed up to this point.
    crate::engine::query_prep::refresh_external_tables_for_query(
        state,
        None,
        &target.namespace,
        query,
    )?;

    // The SELECT may use 3-part `catalog.database.table` names (the INSERT
    // target itself uses one). Strip the catalog prefix before analysis so
    // we feed the analyzer 2-part names — it does not understand catalog-
    // qualified references on its own. This mirrors the standalone SELECT
    // dispatcher's handling of three-part names.
    let mut rewritten = query.clone();
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut rewritten);

    let result = {
        let in_mem = state.catalog.read().expect("standalone catalog read lock");
        crate::engine::execute_query(
            &rewritten,
            &in_mem,
            &target.namespace,
            state.exchange_port,
            None,
        )?
    };
    query_result_to_chunks(result)
}

pub(crate) struct AbortCleanupOperator {
    pub(crate) fs: opendal::Operator,
    pub(crate) path_mapper: Option<CleanupPathMapper>,
}

pub(crate) fn build_abort_cleanup_for_catalog_entry(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<AbortCleanupOperator, String> {
    if let Some(s3_config) = entry.object_store_config() {
        let fs = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for iceberg abort cleanup: {e}"))?;
        let bucket = s3_config.bucket.clone();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            crate::connector::iceberg::catalog::add_files::parse_s3_path(path)
                .ok()
                .and_then(|(actual_bucket, key)| {
                    if actual_bucket == bucket {
                        Some(key)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| path.to_string())
        });
        return Ok(AbortCleanupOperator {
            fs,
            path_mapper: Some(mapper),
        });
    }

    let builder = opendal::services::Fs::default().root("/");
    let fs = opendal::Operator::new(builder)
        .map_err(|e| format!("build local-FS operator failed: {e}"))?
        .finish();
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok(AbortCleanupOperator {
        fs,
        path_mapper: Some(mapper),
    })
}
