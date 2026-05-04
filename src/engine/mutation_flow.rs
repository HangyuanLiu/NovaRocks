use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::arrow::{ArrowReaderBuilder, schema_to_arrow_schema};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, IcebergUpdateMode, MutationSidecar, MutationSidecarFile,
    RunInput, run_iceberg_commit, select_iceberg_update_mode,
};
use crate::connector::iceberg::data_writer::{RowLineageColumns, RowLineageWriteBatch};
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::UpdateStmt;

pub(crate) fn execute_update_statement(
    state: &Arc<StandaloneState>,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    if stmt.source.is_some() {
        return Err("UPDATE ... FROM is implemented in a later stage".to_string());
    }

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;
    validate_update_assignments(&stmt.assignments, &target_columns, &partition_columns)?;

    let mode = select_iceberg_update_mode(&table)?;
    match mode {
        IcebergUpdateMode::CopyOnWrite => {
            execute_cow_update(state, &target, catalog, table_ident, table, stmt, entry)
        }
        IcebergUpdateMode::MergeOnRead => {
            execute_mor_update(state, &target, &table, stmt, current_database)
        }
    }
}

fn execute_mor_update(
    _state: &Arc<StandaloneState>,
    _target: &crate::engine::backend_resolver::TargetBackend,
    _table: &iceberg::table::Table,
    _stmt: &UpdateStmt,
    _current_database: &str,
) -> Result<StatementResult, String> {
    Err("merge-on-read UPDATE is implemented in the next stage".to_string())
}

fn execute_cow_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    stmt: &UpdateStmt,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<StatementResult, String> {
    let target_alias = stmt.alias.as_deref();
    let target_sql = if let Some(alias) = target_alias {
        format!("{} AS {alias}", target.table)
    } else {
        target.table.clone()
    };
    let assignments_sql = stmt
        .assignments
        .iter()
        .map(|assignment| (assignment.column.as_str(), assignment.value.to_string()))
        .collect::<Vec<_>>();
    let assignments_sql = assignments_sql
        .iter()
        .map(|(column, expr)| (*column, expr.as_str()))
        .collect::<Vec<_>>();
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias.unwrap_or(""),
        None,
        &assignments_sql,
        where_sql.as_deref(),
    );
    let matched =
        execute_update_match_query(state, Some(&target.catalog), &match_sql, &target.namespace)?;
    let (data_files, sidecar) = block_on_iceberg(async {
        write_cow_update_files(&table, &matched, entry.object_store_config()).await
    })??;

    if data_files.is_empty() {
        return Ok(StatementResult::Ok);
    }

    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let file_io = table.file_io().clone();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::CowUpdate,
        table_ident,
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for df in data_files {
        collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
            &df,
            metadata.default_partition_spec_id(),
        )?);
    }

    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let _outcome = block_on_iceberg(async {
        run_iceberg_commit(RunInput {
            collector: collector.clone(),
            catalog: catalog.clone(),
            table,
            fs: abort_cleanup.fs,
            file_io,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_sidecar: Some(sidecar),
        })
        .await
    })??;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, target)?;
    Ok(StatementResult::Ok)
}

struct MatchedUpdateBatch {
    row_ids: Vec<i64>,
    file_paths: Vec<String>,
    row_positions: Vec<i64>,
    old_rows: RecordBatch,
    new_rows: RecordBatch,
}

fn execute_update_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
) -> Result<MatchedUpdateBatch, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal UPDATE match query was not a SELECT".to_string());
    };
    crate::engine::query_prep::refresh_external_tables_for_query(
        state,
        current_catalog,
        current_database,
        &query,
    )?;
    let result = {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        crate::engine::execute_query(
            &query,
            &catalog,
            current_database,
            state.exchange_port,
            None,
        )?
    };
    matched_update_batch_from_query_result(result)
}

fn matched_update_batch_from_query_result(
    result: crate::engine::QueryResult,
) -> Result<MatchedUpdateBatch, String> {
    let Some(first_chunk) = result.chunks.first() else {
        return empty_matched_update_batch();
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|chunk| chunk.batch.clone())
        .collect::<Vec<_>>();
    let batch = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate UPDATE match batches failed: {e}"))?;
    if batch.num_rows() == 0 {
        return empty_matched_update_batch();
    }

    let file_col = cast(required_column(&batch, "__nr_file")?, &DataType::Utf8)
        .map_err(|e| format!("cast __nr_file to Utf8 failed: {e}"))?;
    let pos_col = cast(required_column(&batch, "__nr_pos")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_pos to Int64 failed: {e}"))?;
    let row_id_col = cast(required_column(&batch, "__nr_row_id")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_row_id to Int64 failed: {e}"))?;
    let file_arr = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "__nr_file was not Utf8 after cast".to_string())?;
    let pos_arr = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_pos was not Int64 after cast".to_string())?;
    let row_id_arr = row_id_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_row_id was not Int64 after cast".to_string())?;

    let mut file_paths = Vec::with_capacity(batch.num_rows());
    let mut row_positions = Vec::with_capacity(batch.num_rows());
    let mut row_ids = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if file_arr.is_null(row) || pos_arr.is_null(row) || row_id_arr.is_null(row) {
            return Err("UPDATE match query produced null row identity columns".to_string());
        }
        file_paths.push(file_arr.value(row).to_string());
        row_positions.push(pos_arr.value(row));
        row_ids.push(row_id_arr.value(row));
    }

    let old_indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.name().starts_with("__nr_"))
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    let old_fields = old_indices
        .iter()
        .map(|idx| batch.schema().field(*idx).clone())
        .collect::<Vec<_>>();
    let old_schema = Arc::new(Schema::new(old_fields));
    let old_columns = old_indices
        .iter()
        .map(|idx| batch.column(*idx).clone())
        .collect::<Vec<_>>();
    let old_rows = RecordBatch::try_new(old_schema.clone(), old_columns)
        .map_err(|e| format!("build UPDATE old-row batch failed: {e}"))?;

    let mut new_columns = Vec::with_capacity(old_schema.fields().len());
    for (old_idx, field) in old_indices.iter().zip(old_schema.fields().iter()) {
        let new_name = format!("__nr_new_{}", field.name());
        let column = match batch.schema().index_of(&new_name) {
            Ok(idx) => cast(batch.column(idx), field.data_type()).map_err(|e| {
                format!(
                    "cast UPDATE assignment column `{new_name}` to {:?} failed: {e}",
                    field.data_type()
                )
            })?,
            Err(_) => batch.column(*old_idx).clone(),
        };
        new_columns.push(column);
    }
    let new_rows = RecordBatch::try_new(old_schema, new_columns)
        .map_err(|e| format!("build UPDATE new-row batch failed: {e}"))?;

    Ok(MatchedUpdateBatch {
        row_ids,
        file_paths,
        row_positions,
        old_rows,
        new_rows,
    })
}

fn empty_matched_update_batch() -> Result<MatchedUpdateBatch, String> {
    let schema = Arc::new(Schema::empty());
    let empty = RecordBatch::new_empty(schema);
    Ok(MatchedUpdateBatch {
        row_ids: Vec::new(),
        file_paths: Vec::new(),
        row_positions: Vec::new(),
        old_rows: empty.clone(),
        new_rows: empty,
    })
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| format!("UPDATE match query missing `{name}` column"))?;
    Ok(batch.column(idx))
}

async fn write_cow_update_files(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<(Vec<iceberg::spec::DataFile>, MutationSidecar), String> {
    if matched.row_ids.is_empty() {
        return Ok((Vec::new(), empty_sidecar(table)?));
    }
    validate_unique_target_row_ids(&matched.row_ids)?;
    let rewrite_files = build_cow_rewrite_batches(table, matched, object_store_config).await?;
    let mut data_files = Vec::new();
    let mut data_files_by_old_file = Vec::with_capacity(rewrite_files.len());
    for rewrite in &rewrite_files {
        let written =
            crate::connector::iceberg::data_writer::write_row_lineage_batches_as_data_files(
                table,
                std::slice::from_ref(&rewrite.batch),
            )
            .await?;
        if written.is_empty() {
            return Err(format!(
                "COW UPDATE rewrite for data file `{}` produced no data files",
                rewrite.old_file
            ));
        }
        data_files.extend(written.clone());
        data_files_by_old_file.push((rewrite.old_file.clone(), written));
    }
    let sidecar = build_cow_sidecar(table, matched, &data_files_by_old_file, &rewrite_files)?;
    Ok((data_files, sidecar))
}

struct CowRewriteFile {
    old_file: String,
    batch: RowLineageWriteBatch,
}

#[derive(Default)]
struct CowRewriteAccumulator {
    pieces: Vec<RecordBatch>,
    row_ids: Vec<i64>,
    last_updated: Vec<Option<i64>>,
}

async fn build_cow_rewrite_batches(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<CowRewriteFile>, String> {
    let touched_files = matched.file_paths.iter().cloned().collect::<HashSet<_>>();
    let updated_by_row_id = matched
        .row_ids
        .iter()
        .enumerate()
        .map(|(idx, row_id)| (*row_id, idx))
        .collect::<HashMap<_, _>>();
    let existing_deletes_by_file =
        crate::engine::delete_flow::load_existing_delete_visibility_by_data_file(
            table,
            object_store_config,
        )?;
    let lineage_by_file = load_data_file_lineage(table)?;

    let user_schema = Arc::new(
        schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?,
    );
    let mut select_cols = vec!["_file".to_string(), "_pos".to_string()];
    for field in table.metadata().current_schema().as_struct().fields() {
        select_cols.push(field.name.clone());
    }
    let scan = table
        .scan()
        .select(select_cols)
        .build()
        .map_err(|e| format!("build COW UPDATE TableScan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("COW UPDATE TableScan::plan_files failed: {e}"))?;
    let cleaned_tasks = task_stream.map(|task_result| {
        task_result.map(|mut task| {
            task.deletes.clear();
            task.predicate = None;
            task
        })
    });
    let arrow_reader = ArrowReaderBuilder::new(table.file_io().clone())
        .with_row_group_filtering_enabled(false)
        .with_row_selection_enabled(false)
        .build();
    let mut stream = arrow_reader
        .read(Box::pin(cleaned_tasks))
        .map_err(|e| format!("COW UPDATE ArrowReader::read failed: {e}"))?;

    let new_sequence_number = table.metadata().last_sequence_number() + 1;
    let mut accumulators = BTreeMap::<String, CowRewriteAccumulator>::new();
    let mut seen_updated_row_ids = HashSet::new();

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| format!("COW UPDATE scan stream error: {e}"))?;
        collect_cow_rewrite_rows_from_batch(
            &batch,
            &user_schema,
            matched,
            &touched_files,
            &updated_by_row_id,
            &existing_deletes_by_file,
            &lineage_by_file,
            new_sequence_number,
            &mut accumulators,
            &mut seen_updated_row_ids,
        )?;
    }

    for row_id in &matched.row_ids {
        if !seen_updated_row_ids.contains(row_id) {
            return Err(format!(
                "UPDATE matched _row_id={row_id}, but the row was not visible during COW rewrite"
            ));
        }
    }
    if accumulators.is_empty() {
        return Err("COW UPDATE matched rows but produced no replacement rows".to_string());
    }
    let mut out = Vec::with_capacity(accumulators.len());
    for (old_file, acc) in accumulators {
        let user_batch = concat_batches(&user_schema, acc.pieces.iter()).map_err(|e| {
            format!("concatenate COW UPDATE replacement rows for `{old_file}` failed: {e}")
        })?;
        out.push(CowRewriteFile {
            old_file,
            batch: RowLineageWriteBatch {
                user_batch,
                lineage: RowLineageColumns {
                    row_ids: Int64Array::from(acc.row_ids),
                    last_updated_sequence_numbers: Int64Array::from(acc.last_updated),
                },
            },
        });
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn collect_cow_rewrite_rows_from_batch(
    batch: &RecordBatch,
    user_schema: &arrow::datatypes::SchemaRef,
    matched: &MatchedUpdateBatch,
    touched_files: &HashSet<String>,
    updated_by_row_id: &HashMap<i64, usize>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    lineage_by_file: &HashMap<String, DataFileLineage>,
    new_sequence_number: i64,
    accumulators: &mut BTreeMap<String, CowRewriteAccumulator>,
    seen_updated_row_ids: &mut HashSet<i64>,
) -> Result<(), String> {
    let file_col = cast(required_column(batch, "_file")?, &DataType::Utf8)
        .map_err(|e| format!("cast _file to Utf8 failed: {e}"))?;
    let pos_col = cast(required_column(batch, "_pos")?, &DataType::Int64)
        .map_err(|e| format!("cast _pos to Int64 failed: {e}"))?;
    let file_arr = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "_file was not Utf8 after cast".to_string())?;
    let pos_arr = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "_pos was not Int64 after cast".to_string())?;
    let old_user_batch = user_batch_from_scan_batch(batch, user_schema)?;

    for row in 0..batch.num_rows() {
        if file_arr.is_null(row) || pos_arr.is_null(row) {
            return Err("COW UPDATE scan produced null row identity columns".to_string());
        }
        let file_path = file_arr.value(row);
        if !touched_files.contains(file_path) {
            continue;
        }
        if !crate::engine::delete_flow::data_file_row_is_visible(
            batch,
            row,
            file_path,
            pos_arr.value(row),
            existing_deletes_by_file,
        )? {
            continue;
        }
        let lineage = lineage_by_file.get(file_path).ok_or_else(|| {
            format!("COW UPDATE scan file `{file_path}` is missing row-lineage metadata")
        })?;
        let row_id = lineage
            .first_row_id
            .checked_add(pos_arr.value(row))
            .ok_or_else(|| format!("COW UPDATE row id overflow in `{file_path}`"))?;
        let piece = if let Some(matched_idx) = updated_by_row_id.get(&row_id).copied() {
            seen_updated_row_ids.insert(row_id);
            matched.new_rows.slice(matched_idx, 1)
        } else {
            old_user_batch.slice(row, 1)
        };
        let last_updated = if updated_by_row_id.contains_key(&row_id) {
            Some(new_sequence_number)
        } else {
            lineage.data_sequence_number
        };
        let acc = accumulators.entry(file_path.to_string()).or_default();
        acc.pieces.push(piece);
        acc.row_ids.push(row_id);
        acc.last_updated.push(last_updated);
    }
    Ok(())
}

fn user_batch_from_scan_batch(
    batch: &RecordBatch,
    user_schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, String> {
    let mut columns = Vec::with_capacity(user_schema.fields().len());
    for field in user_schema.fields() {
        let idx = batch
            .schema()
            .index_of(field.name())
            .map_err(|_| format!("COW UPDATE scan missing `{}` column", field.name()))?;
        let column = cast(batch.column(idx), field.data_type()).map_err(|e| {
            format!(
                "cast COW UPDATE scan column `{}` to {:?} failed: {e}",
                field.name(),
                field.data_type()
            )
        })?;
        columns.push(column);
    }
    RecordBatch::try_new(user_schema.clone(), columns)
        .map_err(|e| format!("build COW UPDATE user batch failed: {e}"))
}

#[derive(Clone, Copy)]
struct DataFileLineage {
    first_row_id: i64,
    data_sequence_number: Option<i64>,
}

fn load_data_file_lineage(
    table: &iceberg::table::Table,
) -> Result<HashMap<String, DataFileLineage>, String> {
    let files = crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(table)?;
    let mut out = HashMap::with_capacity(files.len());
    for file in files {
        let first_row_id = file.first_row_id.ok_or_else(|| {
            format!(
                "COW UPDATE requires first_row_id for iceberg data file `{}`",
                file.path
            )
        })?;
        out.insert(
            file.path,
            DataFileLineage {
                first_row_id,
                data_sequence_number: file.data_sequence_number,
            },
        );
    }
    Ok(out)
}

fn build_cow_sidecar(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    data_files_by_old_file: &[(String, Vec<iceberg::spec::DataFile>)],
    rewrite_files: &[CowRewriteFile],
) -> Result<MutationSidecar, String> {
    let base_snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| "COW UPDATE requires a current snapshot".to_string())?;
    let replacement_row_ids_by_old_file = rewrite_files
        .iter()
        .map(|rewrite| {
            let row_ids = (0..rewrite.batch.lineage.row_ids.len())
                .map(|idx| rewrite.batch.lineage.row_ids.value(idx))
                .collect::<Vec<_>>();
            (rewrite.old_file.clone(), row_ids)
        })
        .collect::<BTreeMap<_, _>>();
    let new_files_by_old_file = data_files_by_old_file
        .iter()
        .map(|(old_file, files)| {
            (
                old_file.clone(),
                files
                    .iter()
                    .map(|df| df.file_path().to_string())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut rows_by_file = BTreeMap::<String, Vec<i64>>::new();
    for (file, row_id) in matched.file_paths.iter().zip(matched.row_ids.iter()) {
        rows_by_file.entry(file.clone()).or_default().push(*row_id);
    }
    let mut touched_data_files = Vec::with_capacity(rows_by_file.len());
    for (old_file, matched_row_ids) in rows_by_file {
        let new_files = new_files_by_old_file.get(&old_file).ok_or_else(|| {
            format!("COW UPDATE sidecar missing replacement data files for `{old_file}`")
        })?;
        let row_ids = replacement_row_ids_by_old_file
            .get(&old_file)
            .ok_or_else(|| {
                format!("COW UPDATE sidecar missing replacement row ids for `{old_file}`")
            })?;
        for row_id in &matched_row_ids {
            if !row_ids.contains(row_id) {
                return Err(format!(
                    "COW UPDATE replacement rows for `{old_file}` are missing updated row id {row_id}"
                ));
            }
        }
        touched_data_files.push(MutationSidecarFile {
            old_file,
            new_files: new_files.clone(),
            row_ids: row_ids.clone(),
        });
    }
    Ok(MutationSidecar::update(
        IcebergUpdateMode::CopyOnWrite,
        base_snapshot_id,
        table.metadata().uuid().to_string(),
        matched.row_ids.clone(),
        touched_data_files,
    ))
}

fn empty_sidecar(table: &iceberg::table::Table) -> Result<MutationSidecar, String> {
    Ok(MutationSidecar::update(
        IcebergUpdateMode::CopyOnWrite,
        table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0),
        table.metadata().uuid().to_string(),
        Vec::new(),
        Vec::new(),
    ))
}

fn iceberg_table_columns(
    table: &iceberg::table::Table,
) -> Result<Vec<crate::engine::catalog::ColumnDef>, String> {
    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    Ok(arrow_schema
        .fields()
        .iter()
        .map(|field| crate::engine::catalog::ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
        .collect())
}

fn iceberg_partition_source_columns(table: &iceberg::table::Table) -> Result<Vec<String>, String> {
    let schema = table.metadata().current_schema();
    let mut out = Vec::new();
    for field in table.metadata().default_partition_spec().fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "partition source field id {} is missing from iceberg schema",
                field.source_id
            )
        })?;
        out.push(source.name.clone());
    }
    Ok(out)
}

fn validate_update_assignments(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[crate::engine::catalog::ColumnDef],
    partition_columns: &[String],
) -> Result<(), String> {
    let target_names = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let partition_names = partition_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        let name = assignment.column.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "UPDATE cannot assign reserved Iceberg metadata column `{}`",
                assignment.column
            ));
        }
        if !target_names.contains(&name) {
            return Err(format!(
                "UPDATE assignment references unknown target column `{}`",
                assignment.column
            ));
        }
        if partition_names.contains(&name) {
            return Err(format!(
                "UPDATE cannot modify Iceberg partition column `{}` in the first implementation",
                assignment.column
            ));
        }
        if !seen.insert(name) {
            return Err(format!(
                "UPDATE assignment lists target column `{}` more than once",
                assignment.column
            ));
        }
    }
    Ok(())
}

fn validate_unique_target_row_ids(row_ids: &[i64]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for row_id in row_ids {
        if !seen.insert(*row_id) {
            return Err(format!(
                "UPDATE source matched target row _row_id={} more than once; deduplicate the source before retrying",
                row_id
            ));
        }
    }
    Ok(())
}

fn build_update_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: Option<&str>,
    assignments_sql: &[(&str, &str)],
    where_sql: Option<&str>,
) -> String {
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            column.to_string()
        } else {
            format!("{target_alias}.{column}")
        }
    };
    let star = if target_alias.is_empty() {
        "*".to_string()
    } else {
        format!("{target_alias}.*")
    };
    let mut select_items = vec![
        format!("{} AS __nr_file", qualify("_file")),
        format!("{} AS __nr_pos", qualify("_pos")),
        format!("{} AS __nr_row_id", qualify("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            qualify("_last_updated_sequence_number")
        ),
        star,
    ];
    for (column, expr) in assignments_sql {
        select_items.push(format!("{expr} AS __nr_new_{column}"));
    }
    let mut sql = format!("SELECT {} FROM {target_sql}", select_items.join(", "));
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(pred) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(pred);
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::ColumnDef;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "_row_id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &[],
        )
        .expect_err("must reject");
        assert!(err.contains("reserved Iceberg metadata column"), "{err}");
    }

    #[test]
    fn reject_partition_column_update() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &["id".to_string()],
        )
        .expect_err("must reject");
        assert!(err.contains("partition column"), "{err}");
    }

    #[test]
    fn duplicate_row_ids_are_rejected() {
        let err = validate_unique_target_row_ids(&[7, 8, 7]).expect_err("duplicate");
        assert!(err.contains("_row_id=7"), "{err}");
    }

    #[test]
    fn update_match_query_projects_identity_columns() {
        let sql = build_update_match_query_sql(
            "ice.db1.t AS t",
            "t",
            Some("staging.s AS s"),
            &[("v", "s.v")],
            Some("t.id = s.id"),
        );
        assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
        assert!(sql.contains("s.v AS __nr_new_v"), "{sql}");
        assert!(sql.contains("WHERE t.id = s.id"), "{sql}");
    }
}
