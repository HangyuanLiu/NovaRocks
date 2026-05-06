//! Phase4a: projection/filter materialized views backed by Iceberg tables in
//! the NovaRocks-internal `__nova_mv__` catalog. Aggregate shapes (phase4b)
//! and any unsupported MV definitions are rejected here.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::spec::{DataFile, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};

use crate::connector::iceberg::changes::plan_changes;
use crate::connector::iceberg::commit::{
    CleanupPathMapper, CommitOpKind, IcebergCommitCollector, RunInput, run_iceberg_commit,
};
use crate::connector::iceberg::data_writer::write_record_batches_as_data_files;
use crate::connector::starrocks::managed::mv_ddl::{
    alloc_id, analyze_mv_select, extract_base_table_refs, find_or_create_managed_database, now_ms,
    resolve_mv_name, validate_mv_partition_columns,
};
use crate::connector::starrocks::managed::mv_iceberg_catalog::{
    NOVA_MV_CATALOG_NAME, build_nova_mv_catalog,
};
use crate::connector::starrocks::managed::mv_refresh::{
    load_current_iceberg_base_table, query_result_to_chunks, run_mv_full_select_chunks,
    single_snapshot_map, single_table_uuid_map,
};
use crate::connector::starrocks::managed::mv_shape::{
    IncrementalMvShape, classify_incremental_mv_query,
};
use crate::connector::starrocks::managed::store::{
    InsertIcebergMvRowRequest, ManagedTableKind, ManagedTableState, StoredManagedTable,
    UpdateMvIcebergRefreshMetadataRequest,
};
use crate::engine::mv_flow::execute_query_for_mv_incremental_refresh;
use crate::engine::query_prep::IcebergFileForQuery;
use crate::engine::{StandaloneState, StatementResult};
use crate::runtime::global_async_runtime::data_block_on;
use crate::sql::analysis::OutputColumn;
use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
};

pub(crate) fn create_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required for iceberg mv".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required for iceberg mv".to_string())?;

    // 1. Analyze and classify shape — phase4a only accepts projection/filter.
    let analysis = analyze_mv_select(state, current_database, &stmt.select_query)?;
    validate_mv_partition_columns(stmt.partition_by.as_deref(), &analysis.output_columns)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
    let shape = classify_incremental_mv_query(&stmt.select_query)?;
    if !matches!(shape, IncrementalMvShape::ProjectionFilter(_)) {
        return Err(
            "phase4a iceberg-backed materialized views support only projection/filter shapes; aggregates are phase4b"
                .to_string(),
        );
    }

    // IVM Phase-2 PRIMARY KEY validation. Only runs when the user opted in
    // by writing `PRIMARY KEY (...)` in the DDL; otherwise behavior is
    // unchanged. Reuses the same descriptor + validator as the managed-
    // lake-stored path in mv_ddl::create_mv.
    if let Some(pk_cols) = stmt.primary_key.as_deref() {
        if base_refs.len() != 1 {
            return Err(
                "PRIMARY KEY on materialized view requires exactly one iceberg base table"
                    .to_string(),
            );
        }
        let base_ref = &base_refs[0];
        let loaded = load_current_iceberg_base_table(state, base_ref)?;
        let descriptor =
            crate::connector::starrocks::managed::mv_ddl::descriptor_from_loaded(&loaded);
        crate::connector::starrocks::managed::mv_ddl::validate_ivm_primary_key(
            pk_cols,
            &descriptor,
        )
        .map_err(|e| e.to_string())?;
    }

    // 2. Allocate a managed-lake table_id for the MV (register it in the
    //    managed-lake `tables` row so SHOW MATERIALIZED VIEWS works uniformly,
    //    but no tablets, schemas, partitions, or indexes are allocated for the
    //    iceberg storage path).
    let mv_id = allocate_iceberg_mv_table_row(state, &db_name, &mv_name)?;

    // 3. Build Iceberg schema from analyzed output columns.
    let schema = build_iceberg_schema_from_outputs(&analysis.output_columns)?;

    // 4. Create namespace + table in __nova_mv__.
    let catalog = build_nova_mv_catalog(&cfg)?;
    let ns = NamespaceIdent::from_strs([&db_name])
        .map_err(|e| format!("namespace ident `{db_name}` failed: {e}"))?;
    data_block_on(async {
        if !catalog
            .namespace_exists(&ns)
            .await
            .map_err(|e| e.to_string())?
        {
            catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .map_err(|e| format!("create namespace `{db_name}` in __nova_mv__ failed: {e}"))?;
        }
        let creation = TableCreation::builder()
            .name(mv_name.clone())
            .schema(schema)
            .build();
        catalog
            .create_table(&ns, creation)
            .await
            .map_err(|e| format!("create iceberg mv table failed: {e}"))?;
        Ok::<_, String>(())
    })??;

    // 5. Persist mv row in SQLite.
    metadata_store.insert_iceberg_mv_row(InsertIcebergMvRowRequest {
        mv_id,
        select_sql: stmt.select_sql.clone(),
        base_table_refs: base_refs,
        primary_key_columns: stmt.primary_key.clone().unwrap_or_default(),
        iceberg_table_identifier: format!("{NOVA_MV_CATALOG_NAME}.{db_name}.{mv_name}"),
        created_at_ms: now_ms(),
    })?;

    // 6. Register the MV in the in-memory catalog so that subsequent
    //    `state.catalog.read().get(&db, &mv)` calls return Ok and the
    //    duplicate-detection pre-check in create_mv works correctly.
    //    Iceberg-backed MVs have no tablet storage, so we build a TableDef
    //    directly from the analyzed output columns rather than going through
    //    ManagedLakeCatalog::rebuild (which requires a tablet schema row).
    {
        let table_def = {
            let columns = analysis
                .output_columns
                .iter()
                .map(|col| ColumnDef {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    nullable: col.nullable,
                    write_default: None,
                })
                .collect();
            TableDef {
                name: mv_name.clone(),
                columns,
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                storage: TableStorage::S3ParquetFiles {
                    files: vec![],
                    cloud_properties: Default::default(),
                },
            }
        };
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        catalog.create_database(&db_name)?;
        catalog.register(&db_name, table_def)?;
    }

    Ok(StatementResult::Ok)
}

/// Refresh an iceberg-backed materialized view.
///
/// Strategy dispatch:
/// - (None, None)         → no-op (base table is empty / has no snapshot)
/// - (None, Some(cur))    → first refresh: run SELECT, write parquet, commit snapshot
/// - (Some(p), Some(c)) p == c → no-op metadata refresh (bump last_refresh_ms)
/// - (Some(p), Some(c)) p != c → incremental: append-delta SELECT → fast-append MV snapshot
/// - (Some(p), None)      → fail-fast (base snapshot was garbage-collected)
pub(crate) fn refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required for iceberg mv refresh".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required for iceberg mv refresh".to_string())?;

    // Load the MV row from SQLite.
    let snapshot = metadata_store.load_snapshot()?.managed;
    let expected_id = format!("{NOVA_MV_CATALOG_NAME}.{db_name}.{mv_name}");
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|mv| mv.iceberg_table_identifier.as_deref() == Some(expected_id.as_str()))
        .cloned()
        .ok_or_else(|| {
            format!("iceberg materialized view {db_name}.{mv_name} has no materialized_views row")
        })?;

    // We only handle single-base-table MVs in phase4a.
    let [base_ref] = mv_row.base_table_refs.as_slice() else {
        return Err(
            "iceberg materialized view refresh requires exactly one base table reference"
                .to_string(),
        );
    };

    // Load the base iceberg table to get its current snapshot.
    let loaded = load_current_iceberg_base_table(state, base_ref)?;
    let current_snapshot_id = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let current_table_uuid = loaded.table.metadata().uuid().to_string();
    let previous_snapshot_id = mv_row.last_refresh_snapshots.get(&base_ref.fqn()).copied();

    if let Some(previous_uuid) = mv_row.last_refresh_table_uuids.get(&base_ref.fqn())
        && previous_uuid != &current_table_uuid
    {
        tracing::info!(
            "iceberg mv {db_name}.{mv_name}: base table identity changed from {previous_uuid} to {current_table_uuid}; rebuilding with overwrite"
        );
        return rebuild_iceberg_mv(
            state,
            &cfg,
            metadata_store,
            &db_name,
            &mv_name,
            &mv_row,
            base_ref,
            current_snapshot_id,
            &current_table_uuid,
        );
    }

    match (previous_snapshot_id, current_snapshot_id) {
        // Base table has no snapshot yet — nothing to refresh.
        (None, None) => {
            tracing::info!(
                "iceberg mv {db_name}.{mv_name}: base table has no snapshot; skipping refresh"
            );
            Ok(StatementResult::Ok)
        }

        // First refresh: base table now has a snapshot but we haven't run yet.
        (None, Some(cur)) => first_refresh_iceberg_mv(
            state,
            &cfg,
            metadata_store,
            &db_name,
            &mv_name,
            &mv_row,
            base_ref,
            cur,
            &current_table_uuid,
        ),

        // No-op: base table snapshot has not advanced.
        (Some(prev), Some(cur)) if prev == cur => {
            tracing::info!(
                "iceberg mv {db_name}.{mv_name}: base snapshot {cur} unchanged; updating metadata only"
            );
            let snapshots = single_snapshot_map(base_ref, cur);
            let table_uuids = single_table_uuid_map(base_ref, &current_table_uuid);
            metadata_store.update_mv_iceberg_refresh_metadata(
                UpdateMvIcebergRefreshMetadataRequest {
                    table_id: mv_row.mv_id,
                    last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
                    snapshots,
                    table_uuids,
                    iceberg_snapshot_id: mv_row.last_refreshed_iceberg_snapshot_id.unwrap_or(0),
                },
            )?;
            Ok(StatementResult::Ok)
        }

        // Incremental: base snapshot has advanced.
        (Some(prev), Some(cur)) => incremental_refresh_iceberg_mv(
            state,
            &cfg,
            metadata_store,
            &db_name,
            &mv_name,
            &mv_row,
            base_ref,
            prev,
            cur,
            &loaded.table,
            &current_table_uuid,
        ),

        // Previous snapshot no longer reachable.
        (Some(prev), None) => Err(format!(
            "cannot refresh iceberg materialized view {db_name}.{mv_name}: \
             previously-refreshed base snapshot {prev} is no longer reachable"
        )),
    }
}

/// Execute the first refresh of an iceberg-backed MV.
///
/// Steps:
/// 1. Run the MV's SELECT against the base table.
/// 2. Write the resulting chunks as Iceberg/Parquet data files.
/// 3. Commit a fast-append snapshot.
/// 4. Update SQLite metadata.
///
/// On failure after writing but before commit, we attempt a best-effort
/// rollback by dropping the iceberg table.  The SQLite mv_row is only
/// updated after a successful commit, so a failure leaves the mv_row
/// pointing at a non-existent (or empty) iceberg table.  Task 9 (DROP MV)
/// is responsible for cleaning up such orphaned rows.
#[allow(clippy::too_many_arguments)]
fn first_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    metadata_store: &crate::connector::starrocks::managed::store::SqliteMetadataStore,
    db_name: &str,
    mv_name: &str,
    mv_row: &crate::connector::starrocks::managed::store::StoredMaterializedView,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    base_snapshot_id: i64,
    current_table_uuid: &str,
) -> Result<StatementResult, String> {
    // 1. Run SELECT and collect chunks.
    let chunks = run_mv_full_select_chunks(state, db_name, &mv_row.select_sql)?;
    let total_rows: i64 = chunks.iter().map(|c| c.batch.num_rows() as i64).sum();

    // If the base table is currently empty, do not commit an empty Iceberg
    // snapshot.  Leave the mv_row in pre-refresh state so the next REFRESH
    // can re-enter first-refresh once the base table has data.
    if total_rows == 0 {
        tracing::info!(
            "iceberg mv {db_name}.{mv_name}: first refresh produced 0 rows; \
             skipping snapshot commit so next REFRESH can retry"
        );
        return Ok(StatementResult::Ok);
    }

    // 2–3. Write data files and commit snapshot inside an async block.
    let catalog = build_nova_mv_catalog(cfg)?;
    let ident = TableIdent::from_strs([db_name, mv_name])
        .map_err(|e| format!("build mv iceberg ident failed: {e}"))?;

    let result: Result<i64, String> = data_block_on(async {
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load iceberg mv table for first refresh failed: {e}"))?;

        let data_files = write_chunks_as_iceberg_data_files(&table, &chunks).await?;
        let snapshot_id = commit_fast_append(&table, &catalog, data_files).await?;
        Ok(snapshot_id)
    })?;
    let new_snapshot_id = match result {
        Ok(id) => id,
        Err(e) => {
            // Best-effort rollback: drop the (partial) iceberg table so it does
            // not linger after a failed first refresh.  The SQLite mv_row still
            // points at this table; Task 9 (DROP MV) will clean that up if the
            // rollback itself fails.
            let rollback_result = data_block_on(async {
                catalog.drop_table(&ident).await.map_err(|e| e.to_string())
            });
            match rollback_result {
                Ok(Ok(())) => {} // rollback succeeded
                Ok(Err(rollback_err)) | Err(rollback_err) => {
                    tracing::warn!(
                        "iceberg mv {db_name}.{mv_name}: best-effort rollback drop_table failed: \
                         {rollback_err}; table may remain as an orphan until DROP MATERIALIZED VIEW"
                    );
                }
            }
            return Err(format!(
                "first refresh failed (rolled back best-effort): {e}"
            ));
        }
    };

    // 4. Persist refresh metadata in SQLite.
    let snapshots = single_snapshot_map(base_ref, base_snapshot_id);
    let table_uuids = single_table_uuid_map(base_ref, current_table_uuid);
    metadata_store.update_mv_iceberg_refresh_metadata(UpdateMvIcebergRefreshMetadataRequest {
        table_id: mv_row.mv_id,
        last_refresh_rows: total_rows,
        snapshots,
        table_uuids,
        iceberg_snapshot_id: new_snapshot_id,
    })?;

    // 5. Update the in-memory catalog so subsequent SELECTs can read the data.
    if let Err(e) = update_iceberg_mv_in_catalog(state, cfg, db_name, mv_name) {
        tracing::warn!(
            "iceberg mv {db_name}.{mv_name}: catalog update after first refresh failed: {e}; \
             SELECT may return stale results until server restart"
        );
    }

    tracing::info!(
        "iceberg mv {db_name}.{mv_name}: first refresh complete: \
         rows={total_rows} iceberg_snapshot={new_snapshot_id}"
    );
    Ok(StatementResult::Ok)
}

#[allow(clippy::too_many_arguments)]
fn rebuild_iceberg_mv(
    state: &Arc<StandaloneState>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    metadata_store: &crate::connector::starrocks::managed::store::SqliteMetadataStore,
    db_name: &str,
    mv_name: &str,
    mv_row: &crate::connector::starrocks::managed::store::StoredMaterializedView,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    base_snapshot_id: Option<i64>,
    current_table_uuid: &str,
) -> Result<StatementResult, String> {
    let chunks = run_mv_full_select_chunks(state, db_name, &mv_row.select_sql)?;
    let total_rows: i64 = chunks.iter().map(|c| c.batch.num_rows() as i64).sum();

    let catalog = build_nova_mv_catalog(cfg)?;
    let ident =
        TableIdent::from_strs([db_name, mv_name]).map_err(|e| format!("table ident: {e}"))?;
    let new_snapshot_id = data_block_on(async {
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load iceberg mv table for rebuild failed: {e}"))?;
        let data_files = if chunks.iter().all(|c| c.batch.num_rows() == 0) {
            Vec::new()
        } else {
            write_chunks_as_iceberg_data_files(&table, &chunks).await?
        };
        commit_overwrite_iceberg_mv(&table, &catalog, cfg, &ident, data_files).await
    })??;

    let snapshots = base_snapshot_id
        .map(|snapshot_id| single_snapshot_map(base_ref, snapshot_id))
        .unwrap_or_default();
    metadata_store.update_mv_iceberg_refresh_metadata(UpdateMvIcebergRefreshMetadataRequest {
        table_id: mv_row.mv_id,
        last_refresh_rows: total_rows,
        snapshots,
        table_uuids: single_table_uuid_map(base_ref, current_table_uuid),
        iceberg_snapshot_id: new_snapshot_id,
    })?;

    if let Err(e) = update_iceberg_mv_in_catalog(state, cfg, db_name, mv_name) {
        tracing::warn!(
            "iceberg mv {db_name}.{mv_name}: catalog update after rebuild failed: {e}; \
             SELECT may return stale results until server restart"
        );
    }

    Ok(StatementResult::Ok)
}

async fn commit_overwrite_iceberg_mv(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn Catalog>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    ident: &TableIdent,
    data_files: Vec<DataFile>,
) -> Result<i64, String> {
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::Overwrite,
        ident.clone(),
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let default_spec_id = metadata.default_partition_spec_id();
    for df in data_files {
        collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
            &df,
            default_spec_id,
        )?);
    }

    let object_store_config = cfg.s3.to_object_store_config();
    let fs = crate::fs::object_store::build_oss_operator(&object_store_config)
        .map_err(|e| format!("build S3 operator for iceberg mv overwrite cleanup: {e}"))?;
    let bucket = object_store_config.bucket.clone();
    let path_mapper: CleanupPathMapper = Arc::new(move |path| {
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

    run_iceberg_commit(RunInput {
        collector,
        catalog: catalog.clone(),
        table: table.clone(),
        fs,
        file_io: table.file_io().clone(),
        cleanup_path_mapper: Some(path_mapper),
        cow_update_sidecar: None,
        target_ref: "main".to_string(),
    })
    .await
    .map(|outcome| outcome.new_snapshot_id)
}

/// Execute the incremental refresh of an iceberg-backed MV.
///
/// Steps:
/// 1. Plan the change batch from `previous_snapshot_id` to `current_snapshot_id`.
/// 2. Run the MV SELECT scoped to the inserts only.
/// 3. If the delta yields 0 rows, advance lineage without committing an empty snapshot.
/// 4. Otherwise: verify MV iceberg table is in the expected state (inconsistent-state guard),
///    write data files, commit fast-append, and update SQLite metadata.
///
/// On failure after writing data files but before commit, no rollback is attempted.
/// SQLite metadata is only updated after a successful commit, so the prior snapshot
/// remains current and a subsequent REFRESH MATERIALIZED VIEW will retry the same
/// delta range idempotently. Stranded Parquet files are orphaned until the warehouse
/// is garbage-collected.
#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    metadata_store: &crate::connector::starrocks::managed::store::SqliteMetadataStore,
    db_name: &str,
    mv_name: &str,
    mv_row: &crate::connector::starrocks::managed::store::StoredMaterializedView,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    base_table: &iceberg::table::Table,
    current_table_uuid: &str,
) -> Result<StatementResult, String> {
    // 1. Plan the change batch.
    let batch = plan_changes(base_table, previous_snapshot_id, &[]).map_err(|e| e.to_string())?;
    if !batch.deletes.is_empty() || !batch.equality_deletes.is_empty() {
        return Err(format!(
            "iceberg-stored materialized view incremental refresh does not yet support \
             delete snapshots; {} position-delete file(s), {} equality-delete file(s) seen in lineage",
            batch.deletes.len(),
            batch.equality_deletes.len()
        ));
    }
    if batch.current_snapshot_id != current_snapshot_id {
        return Err(format!(
            "iceberg mv incremental refresh: change batch snapshot mismatch (expected {current_snapshot_id}, got {})",
            batch.current_snapshot_id,
        ));
    }

    // 2. Run the MV SELECT scoped to the inserts only.
    let added_files: Vec<IcebergFileForQuery> = batch
        .inserts
        .iter()
        .map(|f| IcebergFileForQuery {
            path: f.path.clone(),
            size: f.size,
            record_count: f.record_count,
            partition_spec_id: f.partition_spec_id,
            partition_key: f.partition_key.clone(),
            first_row_id: f.first_row_id,
            data_sequence_number: f.data_sequence_number,
        })
        .collect();
    let chunks = execute_query_for_mv_incremental_refresh(
        state,
        db_name,
        &mv_row.select_sql,
        base_ref,
        added_files,
    )
    .and_then(query_result_to_chunks)?;
    let added_rows = chunks
        .iter()
        .map(|c| c.batch.num_rows() as i64)
        .sum::<i64>();

    // 3. Empty delta: no new rows → advance lineage without committing an empty snapshot.
    if added_rows == 0 {
        tracing::info!(
            "iceberg mv {db_name}.{mv_name}: incremental refresh delta has 0 rows; \
             advancing lineage to base snapshot {current_snapshot_id} without new iceberg snapshot"
        );
        metadata_store.update_mv_iceberg_refresh_metadata(
            UpdateMvIcebergRefreshMetadataRequest {
                table_id: mv_row.mv_id,
                last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
                snapshots: single_snapshot_map(base_ref, current_snapshot_id),
                table_uuids: single_table_uuid_map(base_ref, current_table_uuid),
                iceberg_snapshot_id: mv_row.last_refreshed_iceberg_snapshot_id.unwrap_or(0),
            },
        )?;
        return Ok(StatementResult::Ok);
    }

    // 4. Write and commit — guarded by inconsistent-state check first.
    let catalog = build_nova_mv_catalog(cfg)?;
    let ident =
        TableIdent::from_strs([db_name, mv_name]).map_err(|e| format!("table ident: {e}"))?;
    let new_snapshot_id = data_block_on(async {
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load iceberg mv table for incremental refresh failed: {e}"))?;

        // Inconsistent-state guard: the MV's iceberg current snapshot must match
        // what SQLite recorded at the last refresh.  If they diverge, the metadata is
        // out-of-sync (manual edit, prior crash, etc.) — fail fast rather than corrupt lineage.
        let prior_snapshot = table.metadata().current_snapshot().map(|s| s.snapshot_id());
        if prior_snapshot != mv_row.last_refreshed_iceberg_snapshot_id {
            return Err(format!(
                "iceberg mv `{db_name}.{mv_name}` is in inconsistent state: \
                 sqlite recorded snapshot {:?} but iceberg current is {:?}; \
                 manual reconcile required (drop and recreate)",
                mv_row.last_refreshed_iceberg_snapshot_id, prior_snapshot,
            ));
        }

        let written = write_chunks_as_iceberg_data_files(&table, &chunks).await?;
        commit_fast_append(&table, &catalog, written).await
    })??;

    let new_total_rows = mv_row.last_refresh_rows.unwrap_or(0) + added_rows;
    metadata_store.update_mv_iceberg_refresh_metadata(UpdateMvIcebergRefreshMetadataRequest {
        table_id: mv_row.mv_id,
        last_refresh_rows: new_total_rows,
        snapshots: single_snapshot_map(base_ref, current_snapshot_id),
        table_uuids: single_table_uuid_map(base_ref, current_table_uuid),
        iceberg_snapshot_id: new_snapshot_id,
    })?;

    // Update the in-memory catalog so subsequent SELECTs can read all data files.
    if let Err(e) = update_iceberg_mv_in_catalog(state, cfg, db_name, mv_name) {
        tracing::warn!(
            "iceberg mv {db_name}.{mv_name}: catalog update after incremental refresh failed: {e}; \
             SELECT may return stale results until server restart"
        );
    }

    tracing::info!(
        "iceberg mv {db_name}.{mv_name}: incremental refresh complete: \
         added_rows={added_rows} total_rows={new_total_rows} iceberg_snapshot={new_snapshot_id}"
    );
    Ok(StatementResult::Ok)
}

/// Write `chunks` into the given iceberg table as Parquet data files.
///
/// Returns the list of written `DataFile` descriptors. If `chunks` is empty
/// or all chunks contain zero rows, returns an empty vec.
///
/// The RecordBatches are re-cast to an Arrow schema annotated with the
/// Iceberg field ids that the `ParquetWriterBuilder` requires (it matches
/// columns by field-id metadata by default).
pub(crate) async fn write_chunks_as_iceberg_data_files(
    table: &iceberg::table::Table,
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<iceberg::spec::DataFile>, String> {
    write_record_batches_as_data_files(table, chunks.iter().map(|chunk| chunk.batch.clone())).await
}

/// Commit a fast-append transaction adding `data_files` to `table`.
///
/// Returns the snapshot id of the newly created snapshot.
pub(crate) async fn commit_fast_append(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn iceberg::Catalog>,
    data_files: Vec<iceberg::spec::DataFile>,
) -> Result<i64, String> {
    let txn = Transaction::new(table);
    let action = txn.fast_append().add_data_files(data_files);
    let txn = action
        .apply(txn)
        .map_err(|e| format!("iceberg fast_append apply failed: {e}"))?;
    let updated_table = txn
        .commit(catalog.as_ref())
        .await
        .map_err(|e| format!("iceberg transaction commit failed: {e}"))?;
    updated_table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| {
            "iceberg commit succeeded but resulting table has no current snapshot".to_string()
        })
}

pub(crate) fn drop_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required".to_string())?;

    // Look up the MV by its iceberg table identifier in SQLite.
    let snapshot = metadata_store.load_snapshot()?.managed;
    let expected_id = format!("{NOVA_MV_CATALOG_NAME}.{db_name}.{mv_name}");
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|m| m.iceberg_table_identifier.as_deref() == Some(expected_id.as_str()))
        .cloned();
    let Some(mv_row) = mv_row else {
        if stmt.if_exists {
            return Ok(StatementResult::Ok);
        }
        return Err(format!(
            "materialized view `{db_name}.{mv_name}` does not exist"
        ));
    };

    // 1. Remove SQLite rows atomically (materialized_views + tables) first.
    //    SQLite is the source of truth; iceberg drop follows.
    metadata_store.delete_iceberg_mv_row(mv_row.mv_id)?;

    // 2. Remove the in-memory catalog entry so subsequent queries do not
    //    resolve the dropped MV.
    {
        let mut catalog = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        // Ignore "unknown table" errors — the entry may already be absent
        // if the MV was registered in a prior session that did not survive.
        if let Err(e) = catalog.drop_table(&db_name, &mv_name) {
            tracing::warn!(
                "iceberg mv {db_name}.{mv_name}: in-memory catalog drop_table returned error \
                 (already gone or other): {e}"
            );
        }
    }

    // 3. Remove the stale entry from the in-memory managed_lake snapshot so
    //    that a same-session second DROP or a CREATE-after-DROP does not
    //    observe the old tables row.
    {
        let mut managed = state
            .managed_lake
            .write()
            .expect("standalone managed lake write lock");
        let mut snapshot = managed.snapshot.clone();
        snapshot.tables.retain(|t| t.table_id != mv_row.mv_id);
        managed.snapshot = snapshot;
    }

    // 4. Drop the iceberg table (deletes warehouse files best-effort via
    //    HadoopFileSystemCatalog::drop_table → FileIO::delete_prefix; file
    //    I/O errors are logged as warnings and do not propagate). The ??
    //    here therefore only surfaces data_block_on infrastructure errors.
    let catalog = build_nova_mv_catalog(&cfg)?;
    let ident =
        TableIdent::from_strs([&db_name, &mv_name]).map_err(|e| format!("table ident: {e}"))?;
    data_block_on(async { catalog.drop_table(&ident).await.map_err(|e| e.to_string()) })??;

    tracing::info!("iceberg mv {db_name}.{mv_name}: dropped successfully");
    Ok(StatementResult::Ok)
}

/// Build an Iceberg `Schema` from the MV's analyzed output columns.
/// Each column is mapped to a primitive Iceberg type; nullable columns become
/// optional fields, non-nullable columns become required fields.
fn build_iceberg_schema_from_outputs(output_columns: &[OutputColumn]) -> Result<Schema, String> {
    let mut fields = Vec::with_capacity(output_columns.len());
    for (idx, col) in output_columns.iter().enumerate() {
        let id = (idx + 1) as i32;
        let primitive = arrow_data_type_to_iceberg_primitive(&col.data_type)?;
        let field: Arc<NestedField> = if col.nullable {
            NestedField::optional(id, &col.name, Type::Primitive(primitive)).into()
        } else {
            NestedField::required(id, &col.name, Type::Primitive(primitive)).into()
        };
        fields.push(field);
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg mv schema failed: {e}"))
}

/// Map an Arrow `DataType` to an Iceberg `PrimitiveType`. Returns an error
/// for types that cannot be represented as Iceberg primitive columns.
fn arrow_data_type_to_iceberg_primitive(
    arrow_type: &arrow::datatypes::DataType,
) -> Result<PrimitiveType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match arrow_type {
        DataType::Boolean => PrimitiveType::Boolean,
        // Promote narrow integer types — Iceberg has no Int8/Int16 primitive.
        DataType::Int8 | DataType::Int16 => PrimitiveType::Int,
        DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
        DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
        DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
        DataType::Decimal128(precision, scale) => {
            let scale_u32 = u32::try_from(*scale).map_err(|_| {
                format!("iceberg-backed mv: Decimal128 negative scale {scale} is not supported")
            })?;
            PrimitiveType::Decimal {
                precision: *precision as u32,
                scale: scale_u32,
            }
        }
        DataType::Decimal256(_, _) => {
            return Err(
                "iceberg-backed mv: Decimal256 (precision > 38) is not supported by Iceberg; \
                 use DECIMAL with precision <= 38"
                    .to_string(),
            );
        }
        DataType::FixedSizeBinary(16) => {
            return Err(
                "iceberg-backed mv: LARGEINT (FixedSizeBinary(16)) is not supported in \
                 iceberg-backed MV; use BIGINT or DECIMAL"
                    .to_string(),
            );
        }
        other => {
            return Err(format!(
                "iceberg-backed mv: unsupported column type `{other:?}`"
            ));
        }
    })
}

/// After a successful REFRESH, update `state.catalog` with the current Iceberg
/// data files so that subsequent `SELECT * FROM mv` queries read the refreshed
/// data rather than the empty `files: vec![]` placeholder set at CREATE time.
///
/// This function loads the MV iceberg table, enumerates its data files, and
/// re-registers the MV in the in-memory catalog (overwrite allowed by
/// `InMemoryCatalog::register`). Column metadata is read from the existing
/// catalog entry that was set at CREATE time.
fn update_iceberg_mv_in_catalog(
    state: &Arc<StandaloneState>,
    cfg: &crate::connector::starrocks::managed::config::ManagedLakeConfig,
    db_name: &str,
    mv_name: &str,
) -> Result<(), String> {
    // Read existing column metadata from the in-memory catalog (set at CREATE time).
    let columns: Vec<ColumnDef> = {
        let guard = state.catalog.read().expect("standalone catalog read lock");
        match guard.get(db_name, mv_name) {
            Ok(def) => def.columns,
            Err(_) => {
                // MV not yet registered (e.g. fresh session after restart).
                // Derive columns from the iceberg table schema below.
                vec![]
            }
        }
    };

    let nova_catalog = build_nova_mv_catalog(cfg)?;
    let ident =
        TableIdent::from_strs([db_name, mv_name]).map_err(|e| format!("table ident: {e}"))?;

    // Load table and extract data files.
    // `extract_data_files_with_stats` is a sync wrapper that calls `block_on_iceberg`
    // internally. We call it outside of any outer `data_block_on` async block to avoid
    // nested block_on usage.
    let table = data_block_on(async {
        nova_catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load mv table for catalog update: {e}"))
    })??;
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(&table)?;

    // Build the appropriate TableStorage depending on whether the warehouse is S3 or local.
    let warehouse = cfg.mv_iceberg_warehouse();
    let is_s3 = warehouse.starts_with("s3://")
        || warehouse.starts_with("s3a://")
        || warehouse.starts_with("oss://");

    let storage = if is_s3 {
        let mut cloud_properties = std::collections::BTreeMap::new();
        cloud_properties.insert("aws.s3.endpoint".to_string(), cfg.s3.endpoint.clone());
        cloud_properties.insert(
            "aws.s3.access_key".to_string(),
            cfg.s3.access_key_id.clone(),
        );
        cloud_properties.insert(
            "aws.s3.secret_key".to_string(),
            cfg.s3.access_key_secret.clone(),
        );
        if let Some(true) = cfg.s3.enable_path_style_access {
            cloud_properties.insert(
                "aws.s3.enable_path_style_access".to_string(),
                "true".to_string(),
            );
        }
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|f| S3FileInfo {
                    path: f.path,
                    size: f.size,
                    row_count: f.record_count,
                    column_stats: f.column_stats,
                    partition_spec_id: f.partition_spec_id,
                    partition_key: f.partition_key,
                    first_row_id: f.first_row_id,
                    data_sequence_number: f.data_sequence_number,
                    delete_files: f.delete_files,
                    manifest_path: None,
                    partition_values: vec![],
                })
                .collect(),
            cloud_properties,
        }
    } else {
        // Local filesystem warehouse (file:// URI).
        //
        // We use S3ParquetFiles with empty cloud_properties instead of
        // LocalParquetFile for two reasons:
        //   1. LocalParquetFile is a single-path variant; after a second
        //      incremental REFRESH writes a second Parquet file, all but
        //      the first would be silently dropped.
        //   2. The standalone scan path in fs/path.rs classifies "file://"
        //      URIs as ScanPathScheme::Local and handles them correctly, so
        //      S3ParquetFiles with file:// paths does not require any S3
        //      credentials and reads from the local filesystem.
        if data_files.is_empty() {
            // No data files yet (empty delta) — leave unchanged.
            tracing::debug!(
                "update_iceberg_mv_in_catalog: {db_name}.{mv_name} has no data files after refresh; \
                 catalog storage not updated"
            );
            return Ok(());
        }
        TableStorage::S3ParquetFiles {
            files: data_files
                .into_iter()
                .map(|f| S3FileInfo {
                    path: f.path,
                    size: f.size,
                    row_count: f.record_count,
                    column_stats: f.column_stats,
                    partition_spec_id: f.partition_spec_id,
                    partition_key: f.partition_key,
                    first_row_id: f.first_row_id,
                    data_sequence_number: f.data_sequence_number,
                    delete_files: f.delete_files,
                    manifest_path: None,
                    partition_values: vec![],
                })
                .collect(),
            // No cloud credentials needed — the file:// scan path reads
            // directly from the local filesystem.
            cloud_properties: std::collections::BTreeMap::new(),
        }
    };

    let table_def = TableDef {
        name: mv_name.to_string(),
        columns,
        iceberg_row_lineage_metadata_columns: vec![],
        iceberg_table: None,
        storage,
    };

    let mut catalog_guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    // create_database is idempotent — ok if already exists.
    let _ = catalog_guard.create_database(db_name);
    catalog_guard
        .register(db_name, table_def)
        .map_err(|e| format!("update iceberg mv {db_name}.{mv_name} in standalone catalog: {e}"))
}

/// Allocate a managed-lake `tables` row for an Iceberg-backed MV so that
/// `SHOW MATERIALIZED VIEWS` and the name-collision check work uniformly
/// across both storage engines. No tablets, schemas, partitions, or
/// indexes are allocated for the iceberg storage path.
fn allocate_iceberg_mv_table_row(
    state: &Arc<StandaloneState>,
    db_name: &str,
    mv_name: &str,
) -> Result<i64, String> {
    use crate::connector::starrocks::managed::ddl::{
        initialize_global_meta_if_needed, reclaim_dropping_table_for_reuse,
    };

    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required".to_string())?;
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let mut snapshot = managed.snapshot.clone();
    initialize_global_meta_if_needed(&mut snapshot, cfg);
    let database = find_or_create_managed_database(&mut snapshot, db_name);
    reclaim_dropping_table_for_reuse(&mut snapshot, database.db_id, mv_name)?;
    let table_id = alloc_id(&mut snapshot.global.next_table_id);
    snapshot.tables.push(StoredManagedTable {
        table_id,
        db_id: database.db_id,
        name: mv_name.to_string(),
        keys_type: "DUP_KEYS".to_string(),
        bucket_num: 0,
        current_schema_id: 0,
        state: ManagedTableState::Active,
        kind: ManagedTableKind::MaterializedView,
    });
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required".to_string())?;
    metadata_store.replace_managed_snapshot(&snapshot)?;
    managed.snapshot = snapshot;
    Ok(table_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;

    fn output_col(name: &str, ty: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
    }

    #[test]
    fn build_iceberg_schema_maps_int_bigint_string() {
        let cols = vec![
            output_col("k", DataType::Int32, false),
            output_col("v", DataType::Int64, true),
            output_col("s", DataType::Utf8, true),
        ];
        let schema = build_iceberg_schema_from_outputs(&cols).expect("schema");
        assert_eq!(schema.as_struct().fields().len(), 3);
        assert_eq!(schema.as_struct().fields()[0].name, "k");
        assert!(schema.as_struct().fields()[0].required);
        assert_eq!(schema.as_struct().fields()[1].name, "v");
        assert!(!schema.as_struct().fields()[1].required);
        assert_eq!(schema.as_struct().fields()[2].name, "s");
        assert!(!schema.as_struct().fields()[2].required);
    }

    #[test]
    fn arrow_data_type_to_iceberg_rejects_unsupported_types() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Map(
            std::sync::Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::empty()),
                false,
            )),
            false,
        ))
        .unwrap_err();
        assert!(err.to_lowercase().contains("unsupported"));
    }

    #[test]
    fn arrow_decimal_negative_scale_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Decimal128(10, -2)).unwrap_err();
        assert!(err.contains("negative scale"));
    }

    #[test]
    fn arrow_int8_int16_promote_to_iceberg_int() {
        use iceberg::spec::PrimitiveType;
        assert_eq!(
            arrow_data_type_to_iceberg_primitive(&DataType::Int8).unwrap(),
            PrimitiveType::Int
        );
        assert_eq!(
            arrow_data_type_to_iceberg_primitive(&DataType::Int16).unwrap(),
            PrimitiveType::Int
        );
    }

    #[test]
    fn arrow_decimal256_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Decimal256(40, 2)).unwrap_err();
        assert!(err.contains("Decimal256"));
    }

    #[test]
    fn arrow_fixed_size_binary_16_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::FixedSizeBinary(16)).unwrap_err();
        assert!(err.contains("LARGEINT"));
    }

    /// End-to-end round-trip: write chunks to a local iceberg table and commit
    /// a fast-append snapshot, then verify the snapshot is current after reload.
    #[test]
    fn write_chunks_round_trip_through_iceberg_table() {
        use crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog;
        use iceberg::Catalog;
        use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());

        // Build FileIO using LocalFsStorageFactory — same pattern as build_file_io
        // in mv_iceberg_catalog.rs.
        let file_io = FileIOBuilder::new(
            StdArc::new(LocalFsStorageFactory) as StdArc<dyn iceberg::io::StorageFactory>
        )
        .build();
        let catalog: StdArc<dyn Catalog> = StdArc::new(HadoopFileSystemCatalog::new(
            file_io.clone(),
            warehouse.clone(),
        ));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();

            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![
                    StdArc::new(iceberg::spec::NestedField::required(
                        1,
                        "k",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                    )),
                    StdArc::new(iceberg::spec::NestedField::optional(
                        2,
                        "v",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                    )),
                ])
                .build()
                .unwrap();

            let creation = iceberg::TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .build();
            let table = catalog.create_table(&ns, creation).await.unwrap();

            // Build a RecordBatch and wrap it in a minimal Chunk.
            let arrow_schema = StdArc::new(ArrowSchema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    StdArc::new(Int32Array::from(vec![1, 2, 3])),
                    StdArc::new(Int64Array::from(vec![Some(10), Some(20), None])),
                ],
            )
            .unwrap();

            // Build a Chunk by deriving ChunkSchema from the RecordBatch arrow schema
            // and synthetic slot ids.
            use crate::common::ids::SlotId;
            use crate::exec::chunk::ChunkSchema;
            let chunk_schema_ref = ChunkSchema::try_ref_from_schema_and_slot_ids(
                &arrow_schema,
                &[SlotId(0), SlotId(1)],
            )
            .expect("chunk schema");
            let chunk = crate::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema_ref);

            let written = write_chunks_as_iceberg_data_files(&table, &[chunk])
                .await
                .unwrap();
            assert!(
                !written.is_empty(),
                "at least one data file should be written"
            );

            let snapshot_id = commit_fast_append(&table, &catalog, written).await.unwrap();
            assert!(snapshot_id != 0, "snapshot id must be non-zero");

            // Reload from catalog and confirm snapshot matches.
            let reloaded = catalog
                .load_table(&iceberg::TableIdent::from_strs(["test_ns", "t"]).unwrap())
                .await
                .unwrap();
            assert_eq!(
                reloaded
                    .metadata()
                    .current_snapshot()
                    .map(|s| s.snapshot_id()),
                Some(snapshot_id),
            );
        });
    }

    #[test]
    fn write_chunks_populates_partition_data_for_partitioned_table() {
        use crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog;
        use iceberg::Catalog;
        use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
        use iceberg::spec::{Transform, UnboundPartitionSpec};

        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}/wh", dir.path().display());
        let file_io = FileIOBuilder::new(
            StdArc::new(LocalFsStorageFactory) as StdArc<dyn iceberg::io::StorageFactory>
        )
        .build();
        let catalog: StdArc<dyn Catalog> = StdArc::new(HadoopFileSystemCatalog::new(
            file_io.clone(),
            warehouse.clone(),
        ));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .unwrap();

            let schema = iceberg::spec::Schema::builder()
                .with_fields(vec![
                    StdArc::new(iceberg::spec::NestedField::required(
                        1,
                        "k",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                    )),
                    StdArc::new(iceberg::spec::NestedField::optional(
                        2,
                        "v",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                    )),
                ])
                .build()
                .unwrap();
            let partition_spec = UnboundPartitionSpec::builder()
                .with_spec_id(0)
                .add_partition_field(1, "k_identity", Transform::Identity)
                .unwrap()
                .build();

            let creation = iceberg::TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .partition_spec(partition_spec)
                .build();
            let table = catalog.create_table(&ns, creation).await.unwrap();

            let arrow_schema = StdArc::new(ArrowSchema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    StdArc::new(Int32Array::from(vec![1, 2, 3])),
                    StdArc::new(Int64Array::from(vec![Some(10), Some(20), None])),
                ],
            )
            .unwrap();

            use crate::common::ids::SlotId;
            use crate::exec::chunk::ChunkSchema;
            let chunk_schema_ref = ChunkSchema::try_ref_from_schema_and_slot_ids(
                &arrow_schema,
                &[SlotId(0), SlotId(1)],
            )
            .expect("chunk schema");
            let chunk = crate::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema_ref);

            let written = write_chunks_as_iceberg_data_files(&table, &[chunk])
                .await
                .unwrap();
            assert_eq!(written.len(), 3);
            assert!(
                written
                    .iter()
                    .all(|data_file| data_file.partition().fields().len() == 1)
            );
            assert!(
                written
                    .iter()
                    .all(|data_file| data_file.record_count() == 1)
            );

            let snapshot_id = commit_fast_append(&table, &catalog, written).await.unwrap();
            assert!(snapshot_id != 0, "snapshot id must be non-zero");
        });
    }
}
