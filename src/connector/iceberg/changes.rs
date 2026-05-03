//! Errors and (in later PRs) data structures for iceberg snapshot-lineage
//! change planning under IVM Phase 2. This file is the home of the new
//! `plan_changes` entrypoint that PR-2 will introduce; PR-1 only lands the
//! error enum so that CREATE-time PRIMARY KEY validation has a stable type
//! to return.

/// All failure modes the iceberg change-planning and IVM CREATE/REFRESH
/// paths can surface. STRICT fail-fast: every variant is a hard rejection,
/// not a fallback signal. Variants not constructed in this PR are reserved
/// for PR-2 (`plan_changes` lineage walk) and PR-3/4 (runtime checks).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChangeError {
    /// `previous_snapshot` referenced by stored MV state is no longer
    /// reachable from the current snapshot's parent chain (e.g. expired).
    LineageBroken { previous_snapshot: i64 },

    /// Snapshot operation is not understood or not in scope for this phase
    /// (e.g. `overwrite`, vendor-specific ops).
    UnsupportedOperation { snapshot_id: i64, op: String },

    /// Equality-delete file encountered; only position-deletes are in scope.
    EqualityDeleteUnsupported { snapshot_id: i64 },

    /// Schema evolution between `previous_snapshot` and `current_snapshot`
    /// (or any unsupported schema-related rejection at CREATE time).
    SchemaEvolutionUnsupported { detail: String },

    /// REPLACE snapshot failed the compaction-only sanity checks (records
    /// changed / schema-id changed / no added or no removed files).
    ReplaceValidationFailed { snapshot_id: i64, reason: String },

    /// CREATE-time: PRIMARY KEY column does not exist on the iceberg base
    /// table.
    PrimaryKeyMissingFromBase { pk_col: String },

    /// CREATE-time: PRIMARY KEY column is nullable on the base table.
    PrimaryKeyNullable { pk_col: String },

    /// CREATE-time: PRIMARY KEY column has a non-hashable scalar type.
    PrimaryKeyTypeUnsupported { pk_col: String, ty: String },

    /// Runtime: PRIMARY KEY column observed NULL in a base row at refresh
    /// time. Not constructed in PR-1.
    PrimaryKeyValueNull { row_info: String },

    /// CREATE-time: iceberg base table is not format-version 2.
    IcebergFormatUnsupported { format_version: i32 },

    /// Catch-all for invariant violations the codebase should never hit;
    /// constructing one is a bug, not a user error.
    InternalInconsistency(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum IcebergChangePolicySignal {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}

pub(crate) fn policy_signal_from_change_error(err: &ChangeError) -> IcebergChangePolicySignal {
    match err {
        ChangeError::UnsupportedOperation { op, .. } if op == "overwrite" => {
            IcebergChangePolicySignal::FullRefresh {
                reason: "insert overwrite requires full refresh".to_string(),
            }
        }
        ChangeError::LineageBroken { .. } => IcebergChangePolicySignal::FullRefresh {
            reason: "previous snapshot is not reachable".to_string(),
        },
        ChangeError::EqualityDeleteUnsupported { .. } => IcebergChangePolicySignal::Unsupported {
            reason: "equality delete is not supported by IVM".to_string(),
        },
        ChangeError::SchemaEvolutionUnsupported { detail } => {
            IcebergChangePolicySignal::Unsupported {
                reason: format!("schema evolution is not supported by IVM: {detail}"),
            }
        }
        ChangeError::ReplaceValidationFailed { reason, .. } => {
            IcebergChangePolicySignal::Unsupported {
                reason: format!("replace snapshot is not a safe compaction: {reason}"),
            }
        }
        other => IcebergChangePolicySignal::Unsupported {
            reason: other.to_string(),
        },
    }
}

impl std::fmt::Display for ChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeError::LineageBroken { previous_snapshot } => write!(
                f,
                "iceberg lineage broken: previous snapshot {previous_snapshot} is unreachable from current snapshot"
            ),
            ChangeError::UnsupportedOperation { snapshot_id, op } => {
                if op == "overwrite" {
                    write!(
                        f,
                        "iceberg snapshot {snapshot_id} is an INSERT OVERWRITE; IVM cannot bridge across an overwrite snapshot. \
                         Either rewrite the workload as DELETE + INSERT, or DROP and re-CREATE the materialized view to reset its lineage."
                    )
                } else {
                    write!(
                        f,
                        "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
                    )
                }
            }
            ChangeError::EqualityDeleteUnsupported { snapshot_id } => write!(
                f,
                "iceberg snapshot {snapshot_id} contains equality-delete files; not supported in this phase"
            ),
            ChangeError::SchemaEvolutionUnsupported { detail } => {
                write!(f, "iceberg schema evolution not supported: {detail}")
            }
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => write!(
                f,
                "iceberg REPLACE snapshot {snapshot_id} failed compaction validation: {reason}"
            ),
            ChangeError::PrimaryKeyMissingFromBase { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` does not exist on the iceberg base table"
            ),
            ChangeError::PrimaryKeyNullable { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` must be NOT NULL on the iceberg base table"
            ),
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, ty } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` has unsupported type `{ty}`; only hashable scalar types are allowed"
            ),
            ChangeError::PrimaryKeyValueNull { row_info } => {
                write!(f, "PRIMARY KEY value is NULL in base row: {row_info}")
            }
            ChangeError::IcebergFormatUnsupported { format_version } => write!(
                f,
                "iceberg base table format-version {format_version} is not supported; IVM requires v2 or v3"
            ),
            ChangeError::InternalInconsistency(detail) => {
                write!(f, "internal inconsistency: {detail}")
            }
        }
    }
}

impl std::error::Error for ChangeError {}

/// Reference to a single data file added to the table by an `Append`
/// snapshot. Row-lineage metadata is preserved so incremental MV refresh can
/// expose Iceberg v3 metadata columns while scanning only the appended files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

/// Reference to a single position-delete file added to the table by a
/// `Delete` snapshot. PR-2 only reports these on the lineage path; the
/// reverse-projection that turns each (delete_file, pos) pair back into
/// the original base row lives in PR-3.
///
/// `referenced_data_file` carries the iceberg `DataFile.referenced_data_file`
/// field — a position-delete file MAY declare a single data file that all
/// of its rows target, in which case readers can short-circuit the join.
/// When `None`, every delete row carries its own `file_path` cell and the
/// reader must read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub referenced_data_file: Option<String>,
    /// `Parquet` for v2 position-delete files, `Puffin` for v3 deletion-vector
    /// files. Other variants are rejected at construction.
    pub file_format: iceberg::spec::DataFileFormat,
    /// Required when `file_format == Puffin`: byte offset of the
    /// `deletion-vector-v1` blob inside the Puffin file. Must be `None` when
    /// `file_format == Parquet`.
    pub content_offset: Option<i64>,
    /// Required when `file_format == Puffin`: byte length of the
    /// `deletion-vector-v1` blob inside the Puffin file. Must be `None` when
    /// `file_format == Parquet`.
    pub content_size_in_bytes: Option<i64>,
}

/// Reference to a single equality-delete file added to the table. Unlike
/// position deletes, equality deletes do not name row positions; reverse
/// projection must scan older data files in the same partition and keep rows
/// whose equality-key tuple appears in the delete file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EqualityDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub equality_ids: Vec<i32>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
}

fn equality_delete_applies_to_data_file(
    delete_file: &EqualityDeleteRef,
    data_sequence_number: Option<i64>,
    data_partition_spec_id: Option<i32>,
    data_partition_key: Option<&str>,
) -> bool {
    if let (Some(delete_sequence), Some(data_sequence)) =
        (delete_file.sequence_number, data_sequence_number)
        && delete_sequence <= data_sequence
    {
        return false;
    }
    if let Some(delete_partition) = delete_file.partition_key.as_deref() {
        if let (Some(delete_spec_id), Some(data_spec_id)) =
            (delete_file.partition_spec_id, data_partition_spec_id)
            && delete_spec_id != data_spec_id
        {
            return false;
        }
        if data_partition_key != Some(delete_partition) {
            return false;
        }
    }
    true
}

fn iceberg_partition_key(partition: &iceberg::spec::Struct) -> Option<String> {
    if partition.fields().is_empty() {
        None
    } else {
        Some(format!("{partition:?}"))
    }
}

impl PositionDeleteRef {
    /// Verify the file_format / content_offset / content_size_in_bytes /
    /// referenced_data_file fields are mutually consistent. Returns
    /// `ChangeError::InternalInconsistency` on any mismatch.
    pub(crate) fn validate_invariants(&self) -> Result<(), ChangeError> {
        use iceberg::spec::DataFileFormat;
        match self.file_format {
            DataFileFormat::Parquet => {
                if self.content_offset.is_some() || self.content_size_in_bytes.is_some() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "PositionDeleteRef {} has Parquet file_format but content_offset/size set",
                        self.delete_file_path
                    )));
                }
            }
            DataFileFormat::Puffin => {
                if self.referenced_data_file.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing referenced_data_file",
                        self.delete_file_path
                    )));
                }
                if self.content_offset.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing content_offset",
                        self.delete_file_path
                    )));
                }
                if self.content_size_in_bytes.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing content_size_in_bytes",
                        self.delete_file_path
                    )));
                }
            }
            other => {
                return Err(ChangeError::InternalInconsistency(format!(
                    "PositionDeleteRef {} has unsupported file_format {:?}",
                    self.delete_file_path, other
                )));
            }
        }
        Ok(())
    }
}

/// Output of `plan_changes`: a flattened, in-order projection of every
/// data-file insert and every position-delete-file ref in the lineage
/// from `previous_snapshot_id` (exclusive) to `current_snapshot_id`
/// (inclusive). REPLACE compaction snapshots are validated and skipped;
/// they contribute to neither vector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergChangeBatch {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub inserts: Vec<DataFileRef>,
    pub deletes: Vec<PositionDeleteRef>,
    pub equality_deletes: Vec<EqualityDeleteRef>,
}

/// Per-row Change action: this row got inserted or deleted relative to
/// the previous MV refresh state. Carried alongside the row contents
/// through the materialize-changes pipeline so the aggregate path can
/// route inserts and deletes differently (insert → positive delta;
/// delete → negative delta after sign-flip).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChangeAction {
    Insert,
    Delete,
}

/// Output of `materialize_changes`: separate `QueryResult` streams for
/// the insert side and the delete side. Both are produced by running
/// the MV's SELECT statement against a one-shot in-memory catalog
/// whose base table has been replaced by the relevant subset of rows
/// (insert files, or deleted-rows-as-temp-parquet). Aggregate semantics
/// (WHERE, GROUP BY, projection) are honored uniformly because the SQL
/// is the same on both branches.
///
/// Either branch may be the empty `QueryResult` (no rows / no chunks)
/// if the corresponding file list was empty.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct MaterializedChanges {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub inserts: crate::engine::QueryResult,
    pub deletes: crate::engine::QueryResult,
}

/// One unit of work the file-collection phase needs to perform for a
/// single snapshot in the lineage. `Replace` snapshots are validated by
/// `classify_snapshot` itself and never produce a `LineageAction` —
/// they're silently absorbed once the validator passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LineageAction {
    /// Walk the snapshot's data manifests, collect entries with
    /// `added_snapshot_id == this`, project to `DataFileRef`.
    CollectInserts { snapshot_id: i64 },
    /// Walk the snapshot's delete manifests, collect entries whose
    /// `DataFile.content_type()` is `PositionDeletes`. Reject equality
    /// deletes.
    CollectDeletes { snapshot_id: i64 },
}

/// Output of `classify_lineage`: a chronologically-ordered list of
/// actions to execute against snapshots from `previous_snapshot_id`
/// (exclusive) to `current_snapshot_id` (inclusive). Replace snapshots
/// validated and skipped during classification do not appear here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LineagePlan {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub actions: Vec<LineageAction>,
}

/// Pure per-snapshot decision. Returns:
/// - `Ok(Some(action))` when the snapshot contributes work to the file
///   collector,
/// - `Ok(None)` when the snapshot is a validated REPLACE compaction and
///   should be silently absorbed,
/// - `Err(ChangeError)` for OVERWRITE, REPLACE-validation failure, etc.
///
/// `parent` is required for REPLACE (the validator compares
/// `total-records` and `schema_id` against the parent). It can be
/// `None` for any other operation; passing `None` for REPLACE
/// produces a `ReplaceValidationFailed` error.
fn classify_snapshot(
    snapshot: &iceberg::spec::Snapshot,
    parent: Option<&iceberg::spec::Snapshot>,
) -> Result<Option<LineageAction>, ChangeError> {
    use iceberg::spec::Operation;
    let snapshot_id = snapshot.snapshot_id();
    match &snapshot.summary().operation {
        Operation::Append => Ok(Some(LineageAction::CollectInserts { snapshot_id })),
        Operation::Delete => Ok(Some(LineageAction::CollectDeletes { snapshot_id })),
        Operation::Replace => {
            let parent = parent.ok_or_else(|| ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason: "REPLACE snapshot has no parent reachable for compaction validation"
                    .to_string(),
            })?;
            validate_replace_snapshot(snapshot, parent)?;
            Ok(None)
        }
        Operation::Overwrite => Err(ChangeError::UnsupportedOperation {
            snapshot_id,
            op: "overwrite".to_string(),
        }),
    }
}

/// Validate that a `Replace` snapshot is a compaction (file rewrite that
/// preserves logical content). A passing REPLACE leaves `total-records`
/// unchanged, contributes both `added-data-files` and `deleted-data-files`
/// counters, and does not change the schema. Anything else is rejected.
fn validate_replace_snapshot(
    snapshot: &iceberg::spec::Snapshot,
    parent: &iceberg::spec::Snapshot,
) -> Result<(), ChangeError> {
    let snap_props = &snapshot.summary().additional_properties;
    let parent_props = &parent.summary().additional_properties;

    let snap_records = snap_props
        .get("total-records")
        .and_then(|s| s.parse::<i64>().ok());
    let parent_records = parent_props
        .get("total-records")
        .and_then(|s| s.parse::<i64>().ok());
    match (snap_records, parent_records) {
        (Some(a), Some(b)) if a == b => {}
        (Some(a), Some(b)) => {
            return Err(ChangeError::ReplaceValidationFailed {
                snapshot_id: snapshot.snapshot_id(),
                reason: format!("total-records changed across REPLACE: parent={b}, replace={a}"),
            });
        }
        _ => {
            return Err(ChangeError::ReplaceValidationFailed {
                snapshot_id: snapshot.snapshot_id(),
                reason:
                    "REPLACE snapshot summary is missing `total-records`; cannot prove compaction"
                        .to_string(),
            });
        }
    }

    let added = snap_props
        .get("added-data-files")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let removed = snap_props
        .get("deleted-data-files")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    if added <= 0 || removed <= 0 {
        return Err(ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!(
                "REPLACE snapshot must report both added-data-files (>0) and \
                 deleted-data-files (>0); got added={added}, deleted={removed}"
            ),
        });
    }

    if snapshot.schema_id() != parent.schema_id() {
        return Err(ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!(
                "REPLACE snapshot schema-id {:?} differs from parent {:?}; schema evolution \
                 across compaction is not in scope",
                snapshot.schema_id(),
                parent.schema_id(),
            ),
        });
    }
    Ok(())
}

/// Walk the parent chain from `current_snapshot` back to
/// `previous_snapshot_id`, dispatching each node through
/// `classify_snapshot`. Performs no I/O.
///
/// Errors:
/// - `LineageBroken` when `previous_snapshot_id` is not an ancestor of
///   the current snapshot (its metadata entry has been pruned, or the
///   chain runs off its root).
/// - `UnsupportedOperation` / `ReplaceValidationFailed` propagated from
///   `classify_snapshot`.
pub(crate) fn classify_lineage(
    metadata: &iceberg::spec::TableMetadata,
    previous_snapshot_id: i64,
) -> Result<LineagePlan, ChangeError> {
    let current_snapshot = metadata.current_snapshot().ok_or_else(|| {
        ChangeError::InternalInconsistency(
            "classify_lineage: table has no current snapshot".to_string(),
        )
    })?;
    let current_snapshot_id = current_snapshot.snapshot_id();

    if current_snapshot_id == previous_snapshot_id {
        return Ok(LineagePlan {
            previous_snapshot_id,
            current_snapshot_id,
            actions: Vec::new(),
        });
    }

    if metadata.snapshot_by_id(previous_snapshot_id).is_none() {
        return Err(ChangeError::LineageBroken {
            previous_snapshot: previous_snapshot_id,
        });
    }

    let mut actions_reversed: Vec<LineageAction> = Vec::new();
    let mut cursor = current_snapshot_id;
    loop {
        if cursor == previous_snapshot_id {
            break;
        }
        let snapshot_ref = metadata
            .snapshot_by_id(cursor)
            .ok_or(ChangeError::LineageBroken {
                previous_snapshot: previous_snapshot_id,
            })?;
        let snapshot = snapshot_ref.as_ref();
        let parent_id = snapshot.parent_snapshot_id();
        let parent = parent_id
            .and_then(|id| metadata.snapshot_by_id(id))
            .map(|sr| sr.as_ref());

        if let Some(action) = classify_snapshot(snapshot, parent)? {
            actions_reversed.push(action);
        }

        match parent_id {
            Some(id) => cursor = id,
            None => {
                // Walked off the root without finding previous_snapshot_id.
                return Err(ChangeError::LineageBroken {
                    previous_snapshot: previous_snapshot_id,
                });
            }
        }
    }

    actions_reversed.reverse();
    Ok(LineagePlan {
        previous_snapshot_id,
        current_snapshot_id,
        actions: actions_reversed,
    })
}

/// Public entrypoint for snapshot-lineage change planning. Walks the
/// lineage from `previous_snapshot_id` (exclusive) to the table's
/// current snapshot (inclusive), classifies each snapshot operation,
/// and assembles `IcebergChangeBatch { inserts, deletes }`.
///
/// The `_pk_columns` parameter is reserved for future delete-side row-id
/// computation; snapshot lineage planning itself does not need it yet.
pub(crate) fn plan_changes(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    _pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    let metadata = table.metadata();
    let current_snapshot_id = metadata
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| {
            ChangeError::InternalInconsistency(
                "plan_changes: table has no current snapshot".to_string(),
            )
        })?;

    let plan = classify_lineage(metadata, previous_snapshot_id)?;
    if plan.actions.is_empty() {
        return Ok(IcebergChangeBatch {
            previous_snapshot_id,
            current_snapshot_id,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: Vec::new(),
        });
    }

    let file_io = table.file_io();
    let collect = collect_files(metadata, file_io, &plan.actions);
    let (inserts, deletes, equality_deletes) =
        crate::connector::iceberg::catalog::registry::block_on_iceberg(collect).map_err(
            |e| ChangeError::InternalInconsistency(format!("plan_changes runtime: {e}")),
        )??;

    Ok(IcebergChangeBatch {
        previous_snapshot_id,
        current_snapshot_id,
        inserts,
        deletes,
        equality_deletes,
    })
}

/// Top-level entry: take an `IcebergChangeBatch`, produce a
/// `MaterializedChanges` whose `inserts` and `deletes` branches each
/// hold the result of running the MV's SELECT statement against the
/// relevant subset of base-table rows.
///
/// The `_pk_columns` parameter is reserved for future delete-side row-id
/// computation when aggregate apply-changes needs stable row identity.
pub(crate) fn materialize_changes(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    base_table: &iceberg::table::Table,
    batch: IcebergChangeBatch,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    _pk_columns: &[String],
) -> Result<MaterializedChanges, String> {
    let inserts = if batch.inserts.is_empty() {
        crate::engine::QueryResult::empty()
    } else {
        let added_files: Vec<crate::engine::query_prep::IcebergFileForQuery> = batch
            .inserts
            .iter()
            .map(|f| crate::engine::query_prep::IcebergFileForQuery {
                path: f.path.clone(),
                size: f.size,
                record_count: f.record_count,
                first_row_id: f.first_row_id,
                data_sequence_number: f.data_sequence_number,
            })
            .collect();
        crate::engine::mv_flow::execute_query_for_mv_incremental_refresh(
            state,
            current_database,
            sql,
            base_ref,
            added_files,
        )?
    };

    let deletes = if batch.deletes.is_empty() && batch.equality_deletes.is_empty() {
        crate::engine::QueryResult::empty()
    } else {
        let factory = build_factory_for_table(base_table, object_store_config)?;
        let size_lookup = |path: &str| -> Option<u64> {
            // We do not currently carry the per-data-file size index across
            // this boundary; the parquet reader reads metadata by HEAD when
            // we pass `None`. This is only an optimization opportunity.
            let _ = path;
            None
        };
        let mut deleted_rows = if batch.deletes.is_empty() {
            Vec::new()
        } else {
            crate::connector::iceberg::scan_deletes::scan_deletes_with_path_normalizer(
                &batch.deletes,
                &factory,
                base_table.file_io(),
                size_lookup,
                |path| normalize_delete_projection_path(path, object_store_config),
            )
            .map_err(|e| e.to_string())?
        };
        if !batch.equality_deletes.is_empty() {
            deleted_rows.extend(scan_equality_delete_rows_for_table(
                base_table,
                &batch.equality_deletes,
                &factory,
                object_store_config,
            )?);
        }
        crate::engine::mv_flow::execute_query_for_mv_incremental_deletes(
            state,
            current_database,
            sql,
            base_ref,
            deleted_rows,
        )?
    };

    Ok(MaterializedChanges {
        previous_snapshot_id: batch.previous_snapshot_id,
        current_snapshot_id: batch.current_snapshot_id,
        inserts,
        deletes,
    })
}

#[derive(Clone, Debug)]
struct EqualityDeleteDataFileRef {
    path: String,
    size: Option<u64>,
    data_sequence_number: Option<i64>,
    partition_spec_id: Option<i32>,
    partition_key: Option<String>,
}

fn scan_equality_delete_rows_for_table(
    table: &iceberg::table::Table,
    equality_deletes: &[EqualityDeleteRef],
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if equality_deletes.is_empty() {
        return Ok(Vec::new());
    }
    let data_files = collect_current_data_files_for_equality_delete(table)?;
    let mut out = Vec::new();
    for data_file in data_files {
        let applicable_deletes = equality_deletes
            .iter()
            .filter(|delete_file| {
                equality_delete_applies_to_data_file(
                    delete_file,
                    data_file.data_sequence_number,
                    data_file.partition_spec_id,
                    data_file.partition_key.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        if applicable_deletes.is_empty() {
            continue;
        }

        let delete_specs = applicable_deletes
            .iter()
            .map(|delete_file| {
                Ok(
                    crate::connector::iceberg::position_delete::IcebergDeleteFileSpec {
                        path: normalize_delete_projection_path(
                            &delete_file.delete_file_path,
                            object_store_config,
                        )
                        .map_err(|e| e.to_string())?,
                        file_format: crate::descriptors::THdfsFileFormat::PARQUET,
                        file_content: crate::types::TIcebergFileContent::EQUALITY_DELETES,
                        length: if delete_file.delete_file_size > 0 {
                            Some(delete_file.delete_file_size as u64)
                        } else {
                            None
                        },
                        content_offset: None,
                        content_size_in_bytes: None,
                    },
                )
            })
            .collect::<Result<Vec<_>, String>>()?;
        let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
            &delete_specs,
            factory,
        )?;
        out.extend(
            crate::connector::iceberg::equality_delete::read_data_file_matching_equality_deletes_with_path_normalizer(
                &data_file.path,
                data_file.size,
                &sets,
                factory,
                |path| {
                    normalize_delete_projection_path(path, object_store_config)
                        .map_err(|e| e.to_string())
                },
            )?,
        );
    }
    Ok(out)
}

fn collect_current_data_files_for_equality_delete(
    table: &iceberg::table::Table,
) -> Result<Vec<EqualityDeleteDataFileRef>, String> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};

    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().ok_or_else(|| {
        "collect equality-delete data files: table has no current snapshot".to_string()
    })?;
    let file_io = table.file_io();
    crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| {
                format!("load manifest list for equality-delete reverse projection: {e}")
            })?;
        let mut out = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
                format!(
                    "load data manifest {} for equality-delete reverse projection: {e}",
                    manifest_file.manifest_path
                )
            })?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let df = entry.data_file();
                if df.content_type() != DataContentType::Data {
                    continue;
                }
                out.push(EqualityDeleteDataFileRef {
                    path: df.file_path().to_string(),
                    size: Some(df.file_size_in_bytes()),
                    data_sequence_number: Some(
                        entry
                            .sequence_number()
                            .unwrap_or(manifest_file.sequence_number),
                    ),
                    partition_spec_id: Some(manifest_file.partition_spec_id),
                    partition_key: iceberg_partition_key(df.partition()),
                });
            }
        }
        Ok(out)
    })
    .map_err(|e| format!("collect equality-delete data files runtime: {e}"))?
}

/// Build a filesystem factory that can read both data files and
/// position-delete files for the given iceberg base table. We use the
/// same `OpendalRangeReaderFactory` shape as the HDFS scan path
/// (`build_fs_operator` for local FS, S3/cloud-credentialled operator
/// when the catalog has cloud properties).
fn build_factory_for_table(
    table: &iceberg::table::Table,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<crate::fs::opendal::OpendalRangeReaderFactory, String> {
    let location = table.metadata().location();
    let scheme = crate::fs::path::classify_scan_paths(std::iter::once(location))
        .map_err(|e| format!("classify iceberg delete reverse projection path: {e}"))?;
    let operator = match scheme {
        crate::fs::path::ScanPathScheme::Local => crate::fs::opendal::build_fs_operator("/")
            .map_err(|e| format!("build local fs operator for delete reverse projection: {e}"))?,
        crate::fs::path::ScanPathScheme::Oss => {
            let cfg = object_store_config.ok_or_else(|| {
                format!(
                    "missing object store config for delete reverse projection: table_location={location}"
                )
            })?;
            crate::fs::oss::build_oss_operator(cfg).map_err(|e| {
                format!("build object-store operator for delete reverse projection: {e}")
            })?
        }
        crate::fs::path::ScanPathScheme::Hdfs => {
            let paths = vec![location.to_string()];
            let resolved = crate::fs::hdfs::resolve_hdfs_scan_paths(&paths)
                .map_err(|e| format!("resolve hdfs path for delete reverse projection: {e}"))?;
            crate::fs::hdfs::build_hdfs_operator(&resolved.name_node, resolved.user.as_deref())
                .map_err(|e| format!("build hdfs operator for delete reverse projection: {e}"))?
        }
    };
    crate::fs::opendal::OpendalRangeReaderFactory::from_operator(operator)
        .map_err(|e| format!("build opendal range reader factory: {e}"))
}

fn normalize_delete_projection_path(
    path: &str,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<String, ChangeError> {
    let scheme = crate::fs::path::classify_scan_paths(std::iter::once(path)).map_err(|e| {
        ChangeError::InternalInconsistency(format!(
            "classify iceberg delete reverse projection path {path}: {e}"
        ))
    })?;
    match scheme {
        crate::fs::path::ScanPathScheme::Local => {
            Ok(path.strip_prefix("file://").unwrap_or(path).to_string())
        }
        crate::fs::path::ScanPathScheme::Oss => {
            let cfg = object_store_config.ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "missing object store config for delete reverse projection path {path}"
                ))
            })?;
            crate::fs::oss::normalize_oss_path(path, &cfg.bucket, &cfg.root).map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "normalize object-store delete reverse projection path {path}: {e}"
                ))
            })
        }
        crate::fs::path::ScanPathScheme::Hdfs => {
            let paths = vec![path.to_string()];
            let resolved = crate::fs::hdfs::resolve_hdfs_scan_paths(&paths).map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "normalize hdfs delete reverse projection path {path}: {e}"
                ))
            })?;
            resolved.paths.into_iter().next().ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "normalize hdfs delete reverse projection path {path}: empty result"
                ))
            })
        }
    }
}

/// Async file collection for one `LineagePlan`. Loads each snapshot's
/// manifest list, walks data manifests for `CollectInserts` actions, and
/// walks delete manifests for `CollectDeletes` actions. Order of the
/// returned `(inserts, deletes)` matches the lineage order in `actions`.
async fn collect_files(
    metadata: &iceberg::spec::TableMetadata,
    file_io: &iceberg::io::FileIO,
    actions: &[LineageAction],
) -> Result<
    (
        Vec<DataFileRef>,
        Vec<PositionDeleteRef>,
        Vec<EqualityDeleteRef>,
    ),
    ChangeError,
> {
    use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType, ManifestStatus};

    let mut inserts: Vec<DataFileRef> = Vec::new();
    let mut deletes: Vec<PositionDeleteRef> = Vec::new();
    let mut equality_deletes: Vec<EqualityDeleteRef> = Vec::new();

    for action in actions {
        let snapshot_id = match action {
            LineageAction::CollectInserts { snapshot_id }
            | LineageAction::CollectDeletes { snapshot_id } => *snapshot_id,
        };
        let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            ChangeError::InternalInconsistency(format!(
                "collect_files: snapshot {snapshot_id} no longer in metadata"
            ))
        })?;
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "load manifest list for snapshot {snapshot_id}: {e}"
                ))
            })?;

        match action {
            LineageAction::CollectInserts { .. } => {
                for manifest_file in manifest_list.entries() {
                    if manifest_file.content != ManifestContentType::Data {
                        continue;
                    }
                    if manifest_file.added_snapshot_id != snapshot_id {
                        continue;
                    }
                    let mut next_manifest_first_row_id = manifest_file
                        .first_row_id
                        .map(|v| {
                            i64::try_from(v).map_err(|_| {
                                ChangeError::InternalInconsistency(format!(
                                    "manifest first_row_id too large in snapshot {snapshot_id}: {v}"
                                ))
                            })
                        })
                        .transpose()?;
                    let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
                        ChangeError::InternalInconsistency(format!(
                            "load data manifest {} for snapshot {snapshot_id}: {e}",
                            manifest_file.manifest_path
                        ))
                    })?;
                    for entry in manifest.entries() {
                        if entry.status == ManifestStatus::Deleted {
                            return Err(ChangeError::InternalInconsistency(format!(
                                "data manifest entry has DELETED status in snapshot {snapshot_id}: {}",
                                entry.data_file().file_path()
                            )));
                        }
                        if entry.status != ManifestStatus::Added {
                            continue;
                        }
                        if entry.snapshot_id() != Some(snapshot_id) {
                            continue;
                        }
                        let df = entry.data_file();
                        if df.content_type() != DataContentType::Data {
                            continue;
                        }
                        let record_count = i64::try_from(df.record_count()).unwrap_or(i64::MAX);
                        let first_row_id = df.first_row_id().or(next_manifest_first_row_id);
                        if let Some(next) = next_manifest_first_row_id.as_mut() {
                            *next = next.checked_add(record_count).ok_or_else(|| {
                                ChangeError::InternalInconsistency(format!(
                                    "first_row_id overflow in manifest {}",
                                    manifest_file.manifest_path
                                ))
                            })?;
                        }
                        inserts.push(DataFileRef {
                            path: df.file_path().to_string(),
                            size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                            record_count: Some(record_count),
                            first_row_id,
                            data_sequence_number: Some(
                                entry
                                    .sequence_number()
                                    .unwrap_or(manifest_file.sequence_number),
                            ),
                        });
                    }
                }
            }
            LineageAction::CollectDeletes { .. } => {
                for manifest_file in manifest_list.entries() {
                    if manifest_file.content != ManifestContentType::Deletes {
                        continue;
                    }
                    if manifest_file.added_snapshot_id != snapshot_id {
                        continue;
                    }
                    let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
                        ChangeError::InternalInconsistency(format!(
                            "load delete manifest {} for snapshot {snapshot_id}: {e}",
                            manifest_file.manifest_path
                        ))
                    })?;
                    for entry in manifest.entries() {
                        if entry.status != ManifestStatus::Added {
                            continue;
                        }
                        if entry.snapshot_id() != Some(snapshot_id) {
                            continue;
                        }
                        let df = entry.data_file();
                        match df.content_type() {
                            DataContentType::PositionDeletes => {
                                let r = match df.file_format() {
                                    DataFileFormat::Parquet => PositionDeleteRef {
                                        delete_file_path: df.file_path().to_string(),
                                        delete_file_size: i64::try_from(df.file_size_in_bytes())
                                            .unwrap_or(i64::MAX),
                                        record_count: Some(
                                            i64::try_from(df.record_count()).unwrap_or(i64::MAX),
                                        ),
                                        referenced_data_file: df.referenced_data_file(),
                                        file_format: DataFileFormat::Parquet,
                                        content_offset: None,
                                        content_size_in_bytes: None,
                                    },
                                    DataFileFormat::Puffin => {
                                        let referenced =
                                            df.referenced_data_file().ok_or_else(|| {
                                                ChangeError::InternalInconsistency(format!(
                                                    "Puffin DV {} in snapshot {snapshot_id} missing referenced_data_file",
                                                    df.file_path()
                                                ))
                                            })?;
                                        let offset = df.content_offset().ok_or_else(|| {
                                            ChangeError::InternalInconsistency(format!(
                                                "Puffin DV {} in snapshot {snapshot_id} missing content_offset",
                                                df.file_path()
                                            ))
                                        })?;
                                        let length =
                                            df.content_size_in_bytes().ok_or_else(|| {
                                                ChangeError::InternalInconsistency(format!(
                                                    "Puffin DV {} in snapshot {snapshot_id} missing content_size_in_bytes",
                                                    df.file_path()
                                                ))
                                            })?;
                                        PositionDeleteRef {
                                            delete_file_path: df.file_path().to_string(),
                                            delete_file_size: i64::try_from(
                                                df.file_size_in_bytes(),
                                            )
                                            .unwrap_or(i64::MAX),
                                            record_count: Some(
                                                i64::try_from(df.record_count())
                                                    .unwrap_or(i64::MAX),
                                            ),
                                            referenced_data_file: Some(referenced),
                                            file_format: DataFileFormat::Puffin,
                                            content_offset: Some(offset),
                                            content_size_in_bytes: Some(length),
                                        }
                                    }
                                    other => {
                                        return Err(ChangeError::InternalInconsistency(format!(
                                            "delete manifest in snapshot {snapshot_id} has unsupported file_format {:?}: {}",
                                            other,
                                            df.file_path()
                                        )));
                                    }
                                };
                                r.validate_invariants()?;
                                deletes.push(r);
                            }
                            DataContentType::EqualityDeletes => {
                                if df.file_format() != DataFileFormat::Parquet {
                                    return Err(ChangeError::InternalInconsistency(format!(
                                        "equality-delete file in snapshot {snapshot_id} has unsupported file_format {:?}: {}",
                                        df.file_format(),
                                        df.file_path()
                                    )));
                                }
                                let equality_ids = df.equality_ids().ok_or_else(|| {
                                    ChangeError::InternalInconsistency(format!(
                                        "equality-delete file {} in snapshot {snapshot_id} missing equality_ids",
                                        df.file_path()
                                    ))
                                })?;
                                if equality_ids.is_empty() {
                                    return Err(ChangeError::InternalInconsistency(format!(
                                        "equality-delete file {} in snapshot {snapshot_id} has empty equality_ids",
                                        df.file_path()
                                    )));
                                }
                                equality_deletes.push(EqualityDeleteRef {
                                    delete_file_path: df.file_path().to_string(),
                                    delete_file_size: i64::try_from(df.file_size_in_bytes())
                                        .unwrap_or(i64::MAX),
                                    record_count: Some(
                                        i64::try_from(df.record_count()).unwrap_or(i64::MAX),
                                    ),
                                    equality_ids,
                                    sequence_number: Some(
                                        entry
                                            .sequence_number()
                                            .unwrap_or(manifest_file.sequence_number),
                                    ),
                                    partition_spec_id: Some(manifest_file.partition_spec_id),
                                    partition_key: iceberg_partition_key(df.partition()),
                                });
                            }
                            DataContentType::Data => {
                                return Err(ChangeError::InternalInconsistency(format!(
                                    "delete manifest contains DATA file in snapshot {snapshot_id}: {}",
                                    df.file_path()
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok((inserts, deletes, equality_deletes))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{Operation, Snapshot, Summary};

    use super::{
        ChangeError, IcebergChangePolicySignal, LineageAction, classify_snapshot,
        normalize_delete_projection_path, policy_signal_from_change_error,
        validate_replace_snapshot,
    };

    use crate::connector::iceberg::catalog::registry::{
        IcebergCatalogEntry, build_catalog_entry, create_namespace, create_table, insert_rows,
        load_table,
    };
    use crate::fs::object_store::ObjectStoreConfig;
    use crate::sql::{Literal, SqlType, TableColumnDef};

    use super::plan_changes;

    #[test]
    fn overwrite_error_policy_signal_is_full_refresh() {
        let err = ChangeError::UnsupportedOperation {
            snapshot_id: 1,
            op: "overwrite".to_string(),
        };
        assert_eq!(
            policy_signal_from_change_error(&err),
            IcebergChangePolicySignal::FullRefresh {
                reason: "insert overwrite requires full refresh".to_string(),
            }
        );
    }

    #[test]
    fn equality_delete_policy_signal_is_unsupported() {
        let err = ChangeError::EqualityDeleteUnsupported { snapshot_id: 2 };
        assert_eq!(
            policy_signal_from_change_error(&err),
            IcebergChangePolicySignal::Unsupported {
                reason: "equality delete is not supported by IVM".to_string(),
            }
        );
    }

    #[test]
    fn equality_delete_ref_applies_only_to_older_data_file_in_same_partition() {
        let delete = super::EqualityDeleteRef {
            delete_file_path: "/tmp/delete.parquet".to_string(),
            delete_file_size: 12,
            record_count: Some(1),
            equality_ids: vec![1],
            sequence_number: Some(9),
            partition_spec_id: Some(3),
            partition_key: Some("Struct([A])".to_string()),
        };

        assert!(super::equality_delete_applies_to_data_file(
            &delete,
            Some(8),
            Some(3),
            Some("Struct([A])")
        ));
        assert!(!super::equality_delete_applies_to_data_file(
            &delete,
            Some(9),
            Some(3),
            Some("Struct([A])")
        ));
        assert!(!super::equality_delete_applies_to_data_file(
            &delete,
            Some(8),
            Some(4),
            Some("Struct([A])")
        ));
        assert!(!super::equality_delete_applies_to_data_file(
            &delete,
            Some(8),
            Some(3),
            Some("Struct([B])")
        ));
    }

    #[test]
    fn unpartitioned_equality_delete_ref_applies_as_global_delete() {
        let delete = super::EqualityDeleteRef {
            delete_file_path: "/tmp/delete.parquet".to_string(),
            delete_file_size: 12,
            record_count: Some(1),
            equality_ids: vec![1],
            sequence_number: Some(9),
            partition_spec_id: Some(0),
            partition_key: None,
        };

        assert!(super::equality_delete_applies_to_data_file(
            &delete,
            Some(8),
            Some(3),
            Some("Struct([A])")
        ));
        assert!(!super::equality_delete_applies_to_data_file(
            &delete,
            Some(9),
            Some(3),
            Some("Struct([A])")
        ));
    }

    fn test_hadoop_catalog_entry(catalog_name: &str, warehouse_uri: &str) -> IcebergCatalogEntry {
        build_catalog_entry(
            catalog_name,
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse_uri.to_string(),
                ),
            ],
        )
        .expect("catalog entry")
    }

    /// Build a synthetic `Snapshot` whose summary carries the given
    /// operation and properties. `manifest_list` and timestamps get
    /// throwaway-but-positive values; the classifier never reads them.
    /// schema_id is encoded in the summary's `schema_id` only when the
    /// caller passes it via the builder.
    fn snap(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
        properties: &[(&str, &str)],
        schema_id: i32,
    ) -> Snapshot {
        let mut props: HashMap<String, String> = HashMap::new();
        for (k, v) in properties {
            props.insert((*k).to_string(), (*v).to_string());
        }
        // iceberg-rust 0.9 `Snapshot::with_parent_snapshot_id` is generated
        // by typed_builder without `strip_option`, so its setter takes
        // `Option<i64>` directly. We always call it (passing `None` when
        // there's no parent) because TypedBuilder's type-state means we
        // can't reassign the builder across optional setters.
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
            .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: props,
            })
            .with_schema_id(schema_id)
            .build()
    }

    fn replace_props(
        total_records: i64,
        added_files: i64,
        deleted_files: i64,
    ) -> Vec<(&'static str, String)> {
        vec![
            ("total-records", total_records.to_string()),
            ("added-data-files", added_files.to_string()),
            ("deleted-data-files", deleted_files.to_string()),
        ]
    }

    fn test_object_store_config() -> ObjectStoreConfig {
        ObjectStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "lake".to_string(),
            root: "warehouse".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    #[test]
    fn normalize_delete_projection_path_uses_object_store_config_for_s3_uri() {
        let cfg = test_object_store_config();
        let path = normalize_delete_projection_path(
            "s3://lake/warehouse/db/orders/data.parquet",
            Some(&cfg),
        )
        .expect("normalize");
        assert_eq!(path, "db/orders/data.parquet");
    }

    #[test]
    fn normalize_delete_projection_path_rejects_s3_uri_without_object_store_config() {
        let err =
            normalize_delete_projection_path("s3://lake/warehouse/db/orders/data.parquet", None)
                .expect_err("must reject");
        assert!(format!("{err}").contains("missing object store config"));
    }

    #[test]
    fn display_primary_key_missing() {
        let e = ChangeError::PrimaryKeyMissingFromBase {
            pk_col: "order_id".to_string(),
        };
        let s = format!("{e}");
        assert!(s.contains("order_id"), "{s}");
        assert!(s.to_lowercase().contains("primary key"), "{s}");
    }

    #[test]
    fn display_iceberg_format_unsupported() {
        let e = ChangeError::IcebergFormatUnsupported { format_version: 1 };
        let s = format!("{e}");
        assert!(s.contains("format-version 1"), "{s}");
        assert!(s.to_lowercase().contains("v2"), "{s}");
    }

    #[test]
    fn classify_snapshot_append_emits_collect_inserts() {
        let s = snap(7, Some(1), Operation::Append, &[], 0);
        let action = classify_snapshot(&s, None).expect("ok");
        assert_eq!(
            action,
            Some(LineageAction::CollectInserts { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_snapshot_delete_emits_collect_deletes() {
        let s = snap(7, Some(1), Operation::Delete, &[], 0);
        let action = classify_snapshot(&s, None).expect("ok");
        assert_eq!(
            action,
            Some(LineageAction::CollectDeletes { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_snapshot_overwrite_is_rejected() {
        let s = snap(7, Some(1), Operation::Overwrite, &[], 0);
        let err = classify_snapshot(&s, None).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::UnsupportedOperation { snapshot_id: 7, ref op } if op == "overwrite"
        ));
    }

    #[test]
    fn classify_snapshot_replace_compaction_is_skipped() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let action = classify_snapshot(&s, Some(&parent)).expect("ok");
        assert_eq!(action, None);
    }

    #[test]
    fn classify_snapshot_replace_without_parent_is_rejected() {
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, None, Operation::Replace, &props, 0);
        let err = classify_snapshot(&s, None).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("parent"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_record_count_change_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(101, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("total-records"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_missing_total_records_is_rejected() {
        // Parent has total-records, REPLACE doesn't. Validator can't prove the
        // record count is unchanged → reject.
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let s = snap(
            2,
            Some(1),
            Operation::Replace,
            &[("added-data-files", "3"), ("deleted-data-files", "5")],
            0,
        );
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("total-records"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_missing_added_or_removed_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 0, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("added-data-files"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_schema_id_change_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        // schema_id 7 ≠ parent's 0.
        let s = snap(2, Some(1), Operation::Replace, &props, 7);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("schema"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn plan_changes_collects_inserts_after_previous_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
            }],
            None,
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");
        let batch = plan_changes(&loaded.table, previous, &[]).expect("plan");
        assert_eq!(batch.previous_snapshot_id, previous);
        assert_eq!(
            batch.current_snapshot_id,
            loaded
                .table
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id()
        );
        assert!(!batch.inserts.is_empty());
        assert!(batch.deletes.is_empty());
        assert!(batch.equality_deletes.is_empty());
        let returned_rows: i64 = batch
            .inserts
            .iter()
            .map(|f| f.record_count.unwrap_or_default())
            .sum();
        assert_eq!(returned_rows, 1);
    }

    #[test]
    fn position_delete_ref_validates_parquet_with_no_content_offset() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/x.parquet".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: None,
        };
        r.validate_invariants().expect("ok");
    }

    #[test]
    fn position_delete_ref_rejects_parquet_with_content_offset() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/x.parquet".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: Some(0),
            content_size_in_bytes: None,
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn position_delete_ref_rejects_parquet_with_content_size() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/x.parquet".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: Some(120),
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn position_delete_ref_validates_puffin_with_full_metadata() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/dv.puffin".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: Some("/tmp/data.parquet".to_string()),
            file_format: iceberg::spec::DataFileFormat::Puffin,
            content_offset: Some(4),
            content_size_in_bytes: Some(120),
        };
        r.validate_invariants().expect("ok");
    }

    #[test]
    fn position_delete_ref_rejects_puffin_missing_offset() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/dv.puffin".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: Some("/tmp/data.parquet".to_string()),
            file_format: iceberg::spec::DataFileFormat::Puffin,
            content_offset: None,
            content_size_in_bytes: Some(120),
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn position_delete_ref_rejects_puffin_missing_referenced_data_file() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/dv.puffin".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Puffin,
            content_offset: Some(4),
            content_size_in_bytes: Some(120),
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn plan_changes_rejects_pruned_previous_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
            }],
            None,
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");

        let pruned_metadata = loaded
            .table
            .metadata()
            .clone()
            .into_builder(None)
            .remove_snapshots(&[previous])
            .build()
            .expect("pruned metadata")
            .metadata;
        let pruned_table = iceberg::table::Table::builder()
            .file_io(loaded.table.file_io().clone())
            .metadata(std::sync::Arc::new(pruned_metadata))
            .identifier(loaded.table.identifier().clone())
            .build()
            .expect("pruned table");

        let err = plan_changes(&pruned_table, previous, &[]).expect_err("should fail");
        assert!(
            matches!(err, ChangeError::LineageBroken { previous_snapshot } if previous_snapshot == previous)
        );
    }
}
