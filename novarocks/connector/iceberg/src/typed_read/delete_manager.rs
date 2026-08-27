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

//! Provider-lifetime Iceberg delete state and the per-split row filter.
//!
//! One [`DeleteManager`] lives for one BE fragment instance and scan node, and
//! every page source under it shares that one manager. A split already carries
//! the complete delete closure FE planning froze for its data file, so nothing
//! here decides *which* deletes apply to a file. What lives here is the part a
//! single split cannot own: loading one delete artifact at most once, grouping
//! what was loaded so sibling splits reuse it, and turning that state into one
//! row verdict per page.
//!
//! Three rules from the read design shape the code:
//!
//! * a delete removes a data row only when its data sequence number is
//!   strictly greater than the data file's -- an equality delete without a
//!   known data sequence number is bad data, never an assumed zero;
//! * a data split carries at most one deletion vector, and a deletion vector
//!   replaces position-delete files instead of joining them;
//! * anything this stack cannot prove fails closed -- a delete that cannot be
//!   classified, keyed, or loaded is an error, never a silently skipped
//!   filter.
//!
//! Nothing here produces a digest, a content id, or state that outlives the
//! manager: the cache is a within-attempt optimization over immutable Iceberg
//! artifacts and can be dropped at any time without changing a result.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{Array, BooleanArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{FileReadContext, FsAccessHandle};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use roaring::RoaringTreemap;

use crate::access_binding::IcebergReadBinding;
use crate::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent as PhysicalDeleteContent,
    IcebergFileFormat as PhysicalDeleteFormat, validate_delete_apply_cost,
};
use crate::file_reader::equality_delete::{
    EqualityDeleteSet, equality_delete_keep_mask, load_equality_delete_sets_with_context,
};
use crate::iceberg::spec::Schema;
use crate::position_delete::load_position_deletes_with_context;

use super::column_handle::{IcebergColumnHandle, corrupt, unsupported};
use super::split::{IcebergDeleteFile, IcebergDeleteFileContent, IcebergFileFormat, IcebergSplit};

/// How the delete verdict is spent.
///
/// Ordinary reads exclude deleted rows. IVM reverse projection needs the rows
/// a delete removes instead, so the equality verdict is inverted -- and only
/// the equality verdict: a position-deleted row is gone from both answers.
///
/// A change window's reverse side asks a third question neither of those can
/// express: *which rows did this window remove*. That is the inverse of
/// position exclusion -- the newly applied artifacts name exactly the rows to
/// emit, not the rows to hide -- minus whatever the artifacts that already
/// applied at the lower endpoint had removed, because those rows were not
/// visible there either and the window's difference does not own them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteEvaluationMode {
    ExcludeDeleted,
    EqualityMatchOnly,
    /// Keep exactly the rows one change window removed.
    ///
    /// The data split itself carries no exclusion closure in this mode: both
    /// sets are named here, so one split never carries two contradictory
    /// delete meanings.
    SelectRemovedRows {
        selected: RemovedRowSelection,
        /// Artifacts already applied at the window's lower endpoint. The rows
        /// they name were invisible there, so they are subtracted rather than
        /// emitted.
        previously_applied: Vec<IcebergDeleteFile>,
    },
}

/// Which rows of one data file a change window removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemovedRowSelection {
    /// The data file is gone at the upper endpoint, so every row it still had
    /// at the lower one was removed.
    WholeFile,
    /// Exactly the rows these newly applied artifacts name.
    NamedBy(Vec<IcebergDeleteFile>),
}

/// The verdict shape one mode resolves to, decided before any I/O.
///
/// It is separate from the mode because the mode owns the artifact lists the
/// loading borrows, while this outlives them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerdictShape {
    ExcludeDeleted,
    EqualityMatchOnly,
    SelectNamedRows,
    SelectWholeFile,
}

/// One grouping scope of the manager's delete state.
///
/// The design groups an unpartitioned table into a single manager scope and a
/// partitioned table by `(spec id, partition data)`. Both are the same rule:
/// an unpartitioned table encodes every data file's partition as the same
/// empty struct, so all of its splits land in one scope on their own and no
/// separate "is this table partitioned" fact has to be guessed from a split.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeleteScope {
    partition_spec_id: i32,
    partition_data_json: Arc<str>,
}

impl DeleteScope {
    fn of(split: &IcebergSplit) -> Self {
        Self {
            partition_spec_id: split.partition_spec_id(),
            partition_data_json: Arc::from(split.partition_data_json()),
        }
    }
}

/// The cache identity of one equality-delete filter.
///
/// Equality field IDs are canonicalized into table-schema order before they
/// reach this key, so two delete files that name the same columns in different
/// manifest order share one filter instead of building two.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct EqualityCacheKey {
    scope: DeleteScope,
    equality_field_ids: Vec<i32>,
}

/// The cache identity of one loaded position-delete artifact.
///
/// A position-delete file is filtered by the data file it is read for, and a
/// deletion vector is addressed by a byte range inside its Puffin container,
/// so both facts belong to the identity of what was loaded.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PositionCacheKey {
    delete_path: Arc<str>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    data_file_path: Arc<str>,
}

/// One split's equality deletes, grouped by their schema-ordered key.
type EqualityGroups<'a> = BTreeMap<Vec<i32>, Vec<&'a IcebergDeleteFile>>;

/// One side's loaded equality artifacts and the key columns they must read,
/// keyed by top-level schema position.
type LoadedEqualityDeletes = (
    Vec<Arc<LoadedEqualityDelete>>,
    BTreeMap<usize, IcebergColumnHandle>,
);

/// Everything the loading of one side needs.
///
/// A change window's reverse side loads two sides in one open, so grouping
/// the per-side inputs keeps that from becoming an eight-argument call whose
/// two invocations differ only in the middle.
struct SideLoad<'a, 'b> {
    split: &'a IcebergSplit,
    scope: &'a DeleteScope,
    closure: &'a GatedClosure<'b>,
    position_work: &'a [(PositionCacheKey, &'b IcebergDeleteFile)],
    table_schema: &'a Schema,
    access: Option<&'a FsAccessHandle>,
}

/// One equality-delete file after decoding.
struct LoadedEqualityDelete {
    path: Arc<str>,
    data_sequence_number: i64,
    keys: EqualityDeleteSet,
}

/// Everything loaded under one equality key in one scope.
///
/// The index is immutable once published: a split that has to add an artifact
/// rebuilds it and swaps the `Arc`. Filters therefore hold a frozen view that
/// a later split cannot widen underneath them, and the common case -- nothing
/// new to load -- costs one pointer clone.
struct EqualityDeleteIndex {
    /// The hidden columns every filter under this key must read, in
    /// table-schema order.
    columns: Vec<IcebergColumnHandle>,
    /// Loaded artifacts by delete path. One path is loaded at most once for
    /// the whole lifetime of the manager.
    artifacts: HashMap<Arc<str>, Arc<LoadedEqualityDelete>>,
    /// The highest delete sequence number loaded under this key. `None` until
    /// the first artifact lands; there is no zero to fall back on.
    highest_loaded_sequence: Option<i64>,
}

#[derive(Default)]
struct DeleteManagerState {
    equality: HashMap<EqualityCacheKey, Arc<EqualityDeleteIndex>>,
    position: HashMap<PositionCacheKey, Arc<RoaringTreemap>>,
    /// How many delete artifacts this manager actually read. Observability
    /// only: it never participates in a verdict.
    loaded_artifacts: usize,
}

/// Provider-lifetime Iceberg delete state: one BE fragment instance and scan
/// node, shared by every page source underneath it.
pub struct DeleteManager {
    binding: IcebergReadBinding,
    context: FileReadContext,
    state: Mutex<DeleteManagerState>,
}

impl DeleteManager {
    /// Build a manager over the provider's filesystem binding and the
    /// fragment-scoped read context that owns its cancellation and deadline.
    pub fn new(binding: IcebergReadBinding, context: FileReadContext) -> Self {
        Self {
            binding,
            context,
            state: Mutex::new(DeleteManagerState::default()),
        }
    }

    /// How many delete artifacts this manager has read so far.
    pub fn loaded_artifacts(&self) -> Result<usize, ConnectorError> {
        Ok(self.lock_state()?.loaded_artifacts)
    }

    /// Load and cache every delete artifact applicable to one split, returning
    /// the per-split filter.
    ///
    /// All grouping, sequence gating, and I/O happen once here rather than
    /// per page. The state lock is held across the loads so one artifact is
    /// read exactly once even when sibling splits open concurrently; that
    /// serializes the first opens of a scan node and is the price of the
    /// once-only guarantee.
    pub fn open_split(
        &self,
        split: &IcebergSplit,
        table_schema: &Schema,
        mode: DeleteEvaluationMode,
    ) -> Result<SplitDeleteFilter, ConnectorError> {
        let shape = verdict_shape(&mode);
        let (applied, previously_applied) = evaluation_inputs(split, &mode)?;

        // Classification and the cost bound both run before any I/O, so an
        // illegal or oversized closure never opens a file. One data file has
        // one physical delete state however a change window divides it, so
        // the structural rules are proved over both sides together: a
        // deletion vector that already applied and one that newly applies
        // would be two answers for the same rows.
        DeleteClosure::classify(
            split.path(),
            applied.iter().chain(previously_applied.iter()),
        )?;
        validate_split_delete_cost(split, applied, previously_applied)?;

        let data_sequence_number = split.data_sequence_number();
        let applied = GatedClosure::of(split, applied, data_sequence_number, table_schema)?;
        let previously = GatedClosure::of(
            split,
            previously_applied,
            data_sequence_number,
            table_schema,
        )?;

        let scope = DeleteScope::of(split);
        let mut state = self.lock_state()?;

        let applied_position_work = position_work(split, &applied.position_files);
        let previously_position_work = position_work(split, &previously.position_files);

        // Resolve access exactly once, over exactly the artifacts still
        // missing. A split whose closure is fully cached opens no handle at
        // all, which is what makes a second split of the same data file free.
        let mut pending_paths = Vec::new();
        for side in [&applied, &previously] {
            collect_pending_paths(&state, &scope, side, &mut pending_paths);
        }
        for work in [&applied_position_work, &previously_position_work] {
            for (key, delete) in work {
                if !state.position.contains_key(key) {
                    pending_paths.push(delete.path());
                }
            }
        }
        let access = if pending_paths.is_empty() {
            None
        } else {
            Some(
                self.binding
                    .resolve_access_for_locations(pending_paths.iter().copied())?,
            )
        };

        let mut hidden_columns: BTreeMap<usize, IcebergColumnHandle> = BTreeMap::new();
        let applied = self.resolve_side(
            &mut state,
            SideLoad {
                split,
                scope: &scope,
                closure: &applied,
                position_work: &applied_position_work,
                table_schema,
                access: access.as_ref(),
            },
            &mut hidden_columns,
        )?;
        let previously = self.resolve_side(
            &mut state,
            SideLoad {
                split,
                scope: &scope,
                closure: &previously,
                position_work: &previously_position_work,
                table_schema,
                access: access.as_ref(),
            },
            &mut hidden_columns,
        )?;

        let verdict = match shape {
            VerdictShape::ExcludeDeleted => FilterVerdict::ExcludeDeleted(applied),
            VerdictShape::EqualityMatchOnly => FilterVerdict::EqualityMatchOnly(applied),
            VerdictShape::SelectNamedRows => FilterVerdict::SelectRemovedRows {
                selected: SelectedRows::NamedBy(applied),
                previously_applied: previously,
            },
            VerdictShape::SelectWholeFile => FilterVerdict::SelectRemovedRows {
                selected: SelectedRows::WholeFile,
                previously_applied: previously,
            },
        };

        Ok(SplitDeleteFilter {
            verdict,
            data_file_path: Arc::from(split.path()),
            hidden_columns: hidden_columns.into_values().collect(),
        })
    }

    /// Load one side's artifacts and fold its equality key columns into the
    /// shared hidden suffix.
    fn resolve_side(
        &self,
        state: &mut DeleteManagerState,
        load: SideLoad<'_, '_>,
        hidden_columns: &mut BTreeMap<usize, IcebergColumnHandle>,
    ) -> Result<ResolvedDeletes, ConnectorError> {
        let deleted_positions =
            self.load_position_deletes(state, load.split, load.position_work, load.access)?;
        let (equality, side_hidden) = self.load_equality_deletes(
            state,
            load.scope,
            &load.closure.equality_groups,
            load.table_schema,
            load.access,
        )?;
        hidden_columns.extend(side_hidden);
        Ok(ResolvedDeletes {
            deleted_positions,
            equality,
        })
    }

    fn load_position_deletes(
        &self,
        state: &mut DeleteManagerState,
        split: &IcebergSplit,
        work: &[(PositionCacheKey, &IcebergDeleteFile)],
        access: Option<&FsAccessHandle>,
    ) -> Result<Arc<RoaringTreemap>, ConnectorError> {
        let mut parts = Vec::with_capacity(work.len());
        for (key, delete) in work {
            if let Some(cached) = state.position.get(key) {
                parts.push(Arc::clone(cached));
                continue;
            }
            let access = access.ok_or_else(|| missing_access(delete))?;
            let spec = physical_delete_spec(delete)?;
            let loaded = load_position_deletes_with_context(
                std::slice::from_ref(&spec),
                split.path(),
                access,
                &self.context,
            )
            .map_err(|error| {
                corrupt(format!(
                    "load iceberg position deletes from {} for {}: {error}",
                    delete.path(),
                    split.path()
                ))
            })?;
            let loaded = Arc::new(loaded);
            state.position.insert(key.clone(), Arc::clone(&loaded));
            state.loaded_artifacts += 1;
            parts.push(loaded);
        }

        // One artifact is by far the common case, and sharing it avoids
        // copying a bitmap that the cache already owns.
        Ok(match parts.len() {
            0 => Arc::new(RoaringTreemap::new()),
            1 => Arc::clone(&parts[0]),
            _ => {
                let mut merged = RoaringTreemap::new();
                for part in &parts {
                    merged |= part.as_ref();
                }
                Arc::new(merged)
            }
        })
    }

    fn load_equality_deletes(
        &self,
        state: &mut DeleteManagerState,
        scope: &DeleteScope,
        groups: &EqualityGroups<'_>,
        table_schema: &Schema,
        access: Option<&FsAccessHandle>,
    ) -> Result<LoadedEqualityDeletes, ConnectorError> {
        let mut applications = Vec::new();
        // Keyed by top-level schema position so the hidden suffix is ordered
        // by the table schema and carries each column exactly once, however
        // many equality keys asked for it.
        let mut hidden_columns: BTreeMap<usize, IcebergColumnHandle> = BTreeMap::new();

        for (equality_field_ids, deletes) in groups {
            let cache_key = EqualityCacheKey {
                scope: scope.clone(),
                equality_field_ids: equality_field_ids.clone(),
            };
            let mut index = match state.equality.get(&cache_key) {
                Some(index) => Arc::clone(index),
                None => Arc::new(EqualityDeleteIndex {
                    columns: equality_columns(equality_field_ids, table_schema)?,
                    artifacts: HashMap::new(),
                    highest_loaded_sequence: None,
                }),
            };

            let missing = deletes
                .iter()
                .copied()
                .filter(|delete| !index.artifacts.contains_key(delete.path()))
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                let mut next = EqualityDeleteIndex {
                    columns: index.columns.clone(),
                    artifacts: index.artifacts.clone(),
                    highest_loaded_sequence: index.highest_loaded_sequence,
                };
                for delete in missing {
                    let access = access.ok_or_else(|| missing_access(delete))?;
                    let keys = self.load_equality_delete(delete, access)?;
                    let sequence = delete.data_sequence_number();
                    next.highest_loaded_sequence = Some(
                        next.highest_loaded_sequence
                            .map_or(sequence, |highest| highest.max(sequence)),
                    );
                    next.artifacts.insert(
                        Arc::from(delete.path()),
                        Arc::new(LoadedEqualityDelete {
                            path: Arc::from(delete.path()),
                            data_sequence_number: sequence,
                            keys,
                        }),
                    );
                    state.loaded_artifacts += 1;
                }
                index = Arc::new(next);
                state.equality.insert(cache_key, Arc::clone(&index));
            }

            // The filter sees exactly this split's own gated closure, never
            // whatever a sibling split happened to load into the same key.
            for delete in deletes {
                let artifact = index.artifacts.get(delete.path()).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        format!(
                            "iceberg equality-delete artifact {} vanished from its loaded index",
                            delete.path()
                        ),
                    )
                })?;
                applications.push(Arc::clone(artifact));
            }
            for column in &index.columns {
                let position = top_level_schema_position(table_schema, column.base_field_id())?;
                hidden_columns.insert(position, column.clone());
            }
        }

        Ok((applications, hidden_columns))
    }

    fn load_equality_delete(
        &self,
        delete: &IcebergDeleteFile,
        access: &FsAccessHandle,
    ) -> Result<EqualityDeleteSet, ConnectorError> {
        let spec = physical_delete_spec(delete)?;
        let mut sets = load_equality_delete_sets_with_context(
            std::slice::from_ref(&spec),
            access,
            &self.context,
        )
        .map_err(|error| {
            corrupt(format!(
                "load iceberg equality deletes from {}: {error}",
                delete.path()
            ))
        })?;
        if sets.len() != 1 {
            return Err(corrupt(format!(
                "iceberg equality-delete file {} decoded into {} key sets, expected exactly one",
                delete.path(),
                sets.len()
            )));
        }
        Ok(sets.remove(0))
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, DeleteManagerState>, ConnectorError> {
        self.state.lock().map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("iceberg delete manager state lock: {error}"),
            )
        })
    }
}

/// One side's loaded delete state.
struct ResolvedDeletes {
    deleted_positions: Arc<RoaringTreemap>,
    equality: Vec<Arc<LoadedEqualityDelete>>,
}

impl ResolvedDeletes {
    fn is_empty(&self) -> bool {
        self.deleted_positions.is_empty() && self.equality.is_empty()
    }

    fn names_position(&self, position: u64) -> bool {
        self.deleted_positions.contains(position)
    }
}

/// The rows one change window removed from a data file.
enum SelectedRows {
    WholeFile,
    NamedBy(ResolvedDeletes),
}

/// What the loaded state means for a row, resolved once at open time.
enum FilterVerdict {
    ExcludeDeleted(ResolvedDeletes),
    EqualityMatchOnly(ResolvedDeletes),
    SelectRemovedRows {
        selected: SelectedRows,
        previously_applied: ResolvedDeletes,
    },
}

/// The row verdict for one split.
pub struct SplitDeleteFilter {
    verdict: FilterVerdict,
    data_file_path: Arc<str>,
    hidden_columns: Vec<IcebergColumnHandle>,
}

impl SplitDeleteFilter {
    /// Columns the page source must additionally read so the equality deletes
    /// can be evaluated.
    ///
    /// They form the hidden suffix after the ordered output columns and are
    /// dropped again once the filter has run. Each one must reach
    /// [`Self::evaluate`] carrying its Iceberg field ID, because that -- not
    /// its name or its position -- is how a delete key binds to a data column.
    pub fn required_hidden_columns(&self) -> &[IcebergColumnHandle] {
        &self.hidden_columns
    }

    /// Whether this split has nothing to filter, so the page source can skip
    /// reading positions and hidden columns entirely.
    pub fn is_empty(&self) -> bool {
        match &self.verdict {
            FilterVerdict::ExcludeDeleted(applied) => applied.is_empty(),
            // Reverse projection keeps only the rows an equality delete
            // removes, so an absent equality filter means "keep nothing" --
            // never "keep everything".
            FilterVerdict::EqualityMatchOnly(_) => false,
            // A file that is gone at the upper endpoint had every one of its
            // rows removed, so with nothing already applied at the lower
            // endpoint there is genuinely nothing to subtract.
            FilterVerdict::SelectRemovedRows {
                selected: SelectedRows::WholeFile,
                previously_applied,
            } => previously_applied.is_empty(),
            // A named selection keeps only what its artifacts name, so an
            // absent filter would mean "keep nothing", never "keep all".
            FilterVerdict::SelectRemovedRows {
                selected: SelectedRows::NamedBy(_),
                ..
            } => false,
        }
    }

    /// Decide which rows survive.
    ///
    /// `batch` is the fully materialized page including the hidden suffix, and
    /// `absolute_positions` are file-level, zero-based row positions. The two
    /// must line up exactly: a page whose positions are missing or short
    /// cannot be judged, and guessing would delete the wrong rows.
    pub fn evaluate(
        &self,
        batch: &RecordBatch,
        absolute_positions: &UInt64Array,
    ) -> Result<BooleanArray, ConnectorError> {
        let rows = batch.num_rows();
        if absolute_positions.len() != rows {
            return Err(corrupt(format!(
                "iceberg row-position count mismatch for {}: positions={} rows={rows}",
                self.data_file_path,
                absolute_positions.len()
            )));
        }
        if rows == 0 {
            return Ok(BooleanArray::from(Vec::<bool>::new()));
        }
        for row in 0..rows {
            if absolute_positions.is_null(row) {
                return Err(corrupt(format!(
                    "iceberg row {row} of {} has no absolute row position",
                    self.data_file_path
                )));
            }
        }

        let keep = match &self.verdict {
            FilterVerdict::ExcludeDeleted(applied) => {
                let equality_deleted = self.equality_verdict(batch, rows, &applied.equality)?;
                (0..rows)
                    .map(|row| {
                        !applied.names_position(absolute_positions.value(row))
                            && !equality_deleted[row]
                    })
                    .collect::<Vec<_>>()
            }
            FilterVerdict::EqualityMatchOnly(applied) => {
                let equality_deleted = self.equality_verdict(batch, rows, &applied.equality)?;
                (0..rows)
                    .map(|row| {
                        !applied.names_position(absolute_positions.value(row))
                            && equality_deleted[row]
                    })
                    .collect::<Vec<_>>()
            }
            FilterVerdict::SelectRemovedRows {
                selected,
                previously_applied,
            } => {
                // Rows the lower endpoint had already lost were never visible
                // there, so the window's difference does not own them -- this
                // is what keeps an equality-deleted split from re-emitting a
                // row a position delete of the same window already named.
                let already_gone =
                    self.equality_verdict(batch, rows, &previously_applied.equality)?;
                let named = match selected {
                    SelectedRows::WholeFile => vec![true; rows],
                    SelectedRows::NamedBy(newly) => {
                        let newly_equality = self.equality_verdict(batch, rows, &newly.equality)?;
                        (0..rows)
                            .map(|row| {
                                newly.names_position(absolute_positions.value(row))
                                    || newly_equality[row]
                            })
                            .collect::<Vec<_>>()
                    }
                };
                (0..rows)
                    .map(|row| {
                        named[row]
                            && !previously_applied.names_position(absolute_positions.value(row))
                            && !already_gone[row]
                    })
                    .collect::<Vec<_>>()
            }
        };
        Ok(BooleanArray::from(keep))
    }

    /// Which rows at least one of the given equality deletes matches.
    ///
    /// Each artifact is evaluated on its own so the shared decode, field-ID
    /// lookup, and scalar-key comparison in `equality_delete` stay the single
    /// authority on what "the same key" means.
    fn equality_verdict(
        &self,
        batch: &RecordBatch,
        rows: usize,
        equality: &[Arc<LoadedEqualityDelete>],
    ) -> Result<Vec<bool>, ConnectorError> {
        let mut deleted = vec![false; rows];
        for application in equality {
            let keep = equality_delete_keep_mask(batch, std::slice::from_ref(&application.keys))
                .map_err(|error| {
                    corrupt(format!(
                        "apply iceberg equality delete {} to {}: {error}",
                        application.path, self.data_file_path
                    ))
                })?;
            let Some(keep) = keep else {
                continue;
            };
            if keep.len() != rows {
                return Err(corrupt(format!(
                    "iceberg equality delete {} produced {} verdicts for {rows} rows of {}",
                    application.path,
                    keep.len(),
                    self.data_file_path
                )));
            }
            for (row, keep) in keep.into_iter().enumerate() {
                deleted[row] |= !keep;
            }
        }
        Ok(deleted)
    }
}

impl std::fmt::Debug for ResolvedDeletes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedDeletes")
            .field("deleted_positions", &self.deleted_positions.len())
            .field(
                "equality",
                &self
                    .equality
                    .iter()
                    .map(|application| {
                        (application.path.as_ref(), application.data_sequence_number)
                    })
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl std::fmt::Debug for SelectedRows {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WholeFile => formatter.write_str("WholeFile"),
            Self::NamedBy(newly) => formatter.debug_tuple("NamedBy").field(newly).finish(),
        }
    }
}

impl std::fmt::Debug for FilterVerdict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExcludeDeleted(applied) => formatter
                .debug_tuple("ExcludeDeleted")
                .field(applied)
                .finish(),
            Self::EqualityMatchOnly(applied) => formatter
                .debug_tuple("EqualityMatchOnly")
                .field(applied)
                .finish(),
            Self::SelectRemovedRows {
                selected,
                previously_applied,
            } => formatter
                .debug_struct("SelectRemovedRows")
                .field("selected", selected)
                .field("previously_applied", previously_applied)
                .finish(),
        }
    }
}

impl std::fmt::Debug for SplitDeleteFilter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SplitDeleteFilter")
            .field("verdict", &self.verdict)
            .field("data_file_path", &self.data_file_path)
            .field("hidden_columns", &self.hidden_columns.len())
            .finish()
    }
}

/// The verdict shape one mode resolves to.
const fn verdict_shape(mode: &DeleteEvaluationMode) -> VerdictShape {
    match mode {
        DeleteEvaluationMode::ExcludeDeleted => VerdictShape::ExcludeDeleted,
        DeleteEvaluationMode::EqualityMatchOnly => VerdictShape::EqualityMatchOnly,
        DeleteEvaluationMode::SelectRemovedRows { selected, .. } => match selected {
            RemovedRowSelection::WholeFile => VerdictShape::SelectWholeFile,
            RemovedRowSelection::NamedBy(_) => VerdictShape::SelectNamedRows,
        },
    }
}

/// The two artifact sets one mode asks the manager to load.
///
/// An ordinary read has exactly one: the split's own closure. A change
/// window's reverse side has two, and both are named by the mode -- the data
/// split must carry none, because a split that also carried an exclusion
/// closure would state two contradictory delete meanings for the same rows.
fn evaluation_inputs<'a>(
    split: &'a IcebergSplit,
    mode: &'a DeleteEvaluationMode,
) -> Result<(&'a [IcebergDeleteFile], &'a [IcebergDeleteFile]), ConnectorError> {
    match mode {
        DeleteEvaluationMode::ExcludeDeleted | DeleteEvaluationMode::EqualityMatchOnly => {
            Ok((split.deletes(), &[]))
        }
        DeleteEvaluationMode::SelectRemovedRows {
            selected,
            previously_applied,
        } => {
            if !split.deletes().is_empty() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "iceberg data file {} names the rows a change window removed, so it must carry no exclusion closure of its own",
                        split.path()
                    ),
                ));
            }
            let selected = match selected {
                RemovedRowSelection::WholeFile => &[][..],
                RemovedRowSelection::NamedBy(newly) => newly.as_slice(),
            };
            Ok((selected, previously_applied.as_slice()))
        }
    }
}

/// One side's classified, sequence-gated closure -- everything decided before
/// any I/O.
struct GatedClosure<'a> {
    position_files: Vec<&'a IcebergDeleteFile>,
    equality_groups: EqualityGroups<'a>,
}

impl<'a> GatedClosure<'a> {
    fn of(
        split: &IcebergSplit,
        deletes: &'a [IcebergDeleteFile],
        data_sequence_number: Option<i64>,
        table_schema: &Schema,
    ) -> Result<Self, ConnectorError> {
        let closure = DeleteClosure::classify(split.path(), deletes.iter())?;
        if !closure.equality_files.is_empty() && data_sequence_number.is_none() {
            return Err(corrupt(format!(
                "iceberg data file {} carries equality deletes but no data sequence number",
                split.path()
            )));
        }
        Ok(Self {
            position_files: closure.applicable_position_deletes(split, data_sequence_number),
            equality_groups: equality_groups_in_schema_order(
                &closure.applicable_equality_deletes(data_sequence_number),
                table_schema,
            )?,
        })
    }
}

/// Pair each applicable position delete with its cache identity.
fn position_work<'a>(
    split: &IcebergSplit,
    position_files: &[&'a IcebergDeleteFile],
) -> Vec<(PositionCacheKey, &'a IcebergDeleteFile)> {
    position_files
        .iter()
        .map(|delete| (position_cache_key(split, delete), *delete))
        .collect()
}

/// The equality-delete paths of one side that this manager has not read yet.
fn collect_pending_paths<'a>(
    state: &DeleteManagerState,
    scope: &DeleteScope,
    closure: &GatedClosure<'a>,
    pending: &mut Vec<&'a str>,
) {
    for (equality_field_ids, deletes) in &closure.equality_groups {
        let cache_key = EqualityCacheKey {
            scope: scope.clone(),
            equality_field_ids: equality_field_ids.clone(),
        };
        let index = state.equality.get(&cache_key);
        for delete in deletes {
            if index.is_none_or(|index| !index.artifacts.contains_key(delete.path())) {
                pending.push(delete.path());
            }
        }
    }
}

/// A split's declared delete closure, classified before any I/O.
struct DeleteClosure<'a> {
    /// Parquet position-delete files.
    position_files: Vec<&'a IcebergDeleteFile>,
    /// The single Puffin deletion vector, when the split has one.
    deletion_vector: Option<&'a IcebergDeleteFile>,
    equality_files: Vec<&'a IcebergDeleteFile>,
}

impl<'a> DeleteClosure<'a> {
    fn classify<I>(data_file_path: &str, deletes: I) -> Result<Self, ConnectorError>
    where
        I: IntoIterator<Item = &'a IcebergDeleteFile>,
    {
        let mut closure = Self {
            position_files: Vec::new(),
            deletion_vector: None,
            equality_files: Vec::new(),
        };
        for delete in deletes {
            match delete.content() {
                IcebergDeleteFileContent::PositionDeletes => match delete.format() {
                    IcebergFileFormat::Puffin => {
                        // A deletion vector is the complete position-delete
                        // state of one data file. A second one would be a
                        // second answer for the same rows, with no rule that
                        // says which wins.
                        if let Some(existing) = closure.deletion_vector {
                            return Err(corrupt(format!(
                                "iceberg data file {} carries more than one deletion vector: {} and {}",
                                data_file_path,
                                existing.path(),
                                delete.path()
                            )));
                        }
                        closure.deletion_vector = Some(delete);
                    }
                    IcebergFileFormat::Parquet => closure.position_files.push(delete),
                    IcebergFileFormat::Orc | IcebergFileFormat::Avro => {
                        return Err(unsupported(format!(
                            "iceberg position-delete file {} is neither parquet nor a puffin deletion vector",
                            delete.path()
                        )));
                    }
                },
                IcebergDeleteFileContent::EqualityDeletes => match delete.format() {
                    IcebergFileFormat::Parquet => closure.equality_files.push(delete),
                    IcebergFileFormat::Puffin => {
                        return Err(corrupt(format!(
                            "iceberg equality-delete file {} claims the puffin deletion-vector format",
                            delete.path()
                        )));
                    }
                    IcebergFileFormat::Orc | IcebergFileFormat::Avro => {
                        return Err(unsupported(format!(
                            "iceberg equality-delete file {} is not in the parquet format",
                            delete.path()
                        )));
                    }
                },
            }
        }
        // A deletion vector supersedes the position-delete files of its data
        // file; both present means the manifest state is mid-migration or
        // wrong, and picking either one would silently change the answer.
        if let Some(deletion_vector) = closure.deletion_vector
            && !closure.position_files.is_empty()
        {
            return Err(corrupt(format!(
                "iceberg data file {} carries deletion vector {} alongside {} position-delete file(s)",
                data_file_path,
                deletion_vector.path(),
                closure.position_files.len()
            )));
        }
        Ok(closure)
    }

    /// The position deletes that can still remove a row of this data file.
    fn applicable_position_deletes(
        &self,
        split: &IcebergSplit,
        data_sequence_number: Option<i64>,
    ) -> Vec<&'a IcebergDeleteFile> {
        self.position_files
            .iter()
            .copied()
            .chain(self.deletion_vector)
            .filter(|delete| delete_outranks_data(delete, data_sequence_number))
            .filter(|delete| !row_position_bounds_exclude_file(delete, split.file_record_count()))
            .collect()
    }

    fn applicable_equality_deletes(
        &self,
        data_sequence_number: Option<i64>,
    ) -> Vec<&'a IcebergDeleteFile> {
        self.equality_files
            .iter()
            .copied()
            .filter(|delete| delete_outranks_data(delete, data_sequence_number))
            .collect()
    }
}

/// Whether one delete descriptor can remove a row of this data file at all.
///
/// FE planning already froze this decision from the same rule, so re-checking
/// it here is a guard rather than a second policy: a split that reached a
/// worker carrying a delete it cannot outrank would otherwise delete live
/// rows.
fn delete_outranks_data(delete: &IcebergDeleteFile, data_sequence_number: Option<i64>) -> bool {
    match data_sequence_number {
        Some(data_sequence_number) => delete.data_sequence_number() > data_sequence_number,
        // Without a data sequence number there is no evidence that this delete
        // is older than the data, and dropping it would resurrect a deleted
        // row. Equality deletes never reach here: `open_split` rejects that
        // combination as bad data before the gate runs.
        None => true,
    }
}

/// Whether a delete file's row-position bounds prove it names no row of this
/// data file.
///
/// The bounds are the smallest and largest `pos` the delete file carries
/// across every data file it references, so a smallest position at or beyond
/// this file's last row proves nothing inside `[0, record_count)` can match.
/// Bounds are consulted here and nowhere else: they can only remove work, and
/// can never conclude that a row is deleted.
fn row_position_bounds_exclude_file(delete: &IcebergDeleteFile, file_record_count: i64) -> bool {
    // An uncounted or empty data file gives the bounds nothing to be compared
    // against; absence of a count is not evidence.
    if file_record_count <= 0 {
        return false;
    }
    delete
        .row_position_lower_bound()
        .is_some_and(|lower| lower >= file_record_count)
}

/// Group a split's equality deletes by their equality key, canonicalized into
/// table-schema order.
fn equality_groups_in_schema_order<'a>(
    deletes: &[&'a IcebergDeleteFile],
    table_schema: &Schema,
) -> Result<EqualityGroups<'a>, ConnectorError> {
    let mut groups = EqualityGroups::new();
    for delete in deletes {
        let key = schema_ordered_equality_key(delete, table_schema)?;
        groups.entry(key).or_default().push(delete);
    }
    Ok(groups)
}

/// The equality field IDs of one delete file, in table-schema order.
///
/// Ordering is derived from the frozen table schema rather than trusted from
/// the manifest, so the cache key of a filter is the same whichever order a
/// writer recorded the IDs in.
fn schema_ordered_equality_key(
    delete: &IcebergDeleteFile,
    table_schema: &Schema,
) -> Result<Vec<i32>, ConnectorError> {
    let mut positions = Vec::with_capacity(delete.equality_field_ids().len());
    for field_id in delete.equality_field_ids() {
        let position = top_level_schema_position(table_schema, *field_id).map_err(|_| {
            corrupt(format!(
                "iceberg equality-delete file {} names field id {field_id}, which is not a top-level field of the frozen table schema",
                delete.path()
            ))
        })?;
        if positions.iter().any(|(existing, _)| *existing == position) {
            return Err(corrupt(format!(
                "iceberg equality-delete file {} names field id {field_id} more than once",
                delete.path()
            )));
        }
        positions.push((position, *field_id));
    }
    positions.sort_unstable();
    Ok(positions
        .into_iter()
        .map(|(_, field_id)| field_id)
        .collect())
}

fn equality_columns(
    equality_field_ids: &[i32],
    table_schema: &Schema,
) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
    equality_field_ids
        .iter()
        .map(|field_id| IcebergColumnHandle::base_column_of(table_schema, *field_id))
        .collect()
}

fn top_level_schema_position(
    table_schema: &Schema,
    field_id: i32,
) -> Result<usize, ConnectorError> {
    table_schema
        .as_struct()
        .fields()
        .iter()
        .position(|field| field.id == field_id)
        .ok_or_else(|| {
            corrupt(format!(
                "iceberg field id {field_id} is not a top-level field of the frozen table schema"
            ))
        })
}

fn position_cache_key(split: &IcebergSplit, delete: &IcebergDeleteFile) -> PositionCacheKey {
    PositionCacheKey {
        delete_path: Arc::from(delete.path()),
        content_offset: delete.content_offset(),
        content_size_in_bytes: delete.content_size_in_bytes(),
        data_file_path: Arc::from(split.path()),
    }
}

/// Project one frozen delete descriptor onto the crate's physical read spec.
fn physical_delete_spec(
    delete: &IcebergDeleteFile,
) -> Result<IcebergDeleteFileSpec, ConnectorError> {
    let length = u64::try_from(delete.file_size_in_bytes()).map_err(|_| {
        corrupt(format!(
            "iceberg delete file {} has a negative file size",
            delete.path()
        ))
    })?;
    let file_format = match delete.format() {
        IcebergFileFormat::Parquet => PhysicalDeleteFormat::Parquet,
        IcebergFileFormat::Puffin => PhysicalDeleteFormat::Puffin,
        IcebergFileFormat::Orc | IcebergFileFormat::Avro => {
            return Err(unsupported(format!(
                "iceberg delete file {} is neither parquet nor puffin",
                delete.path()
            )));
        }
    };
    let file_content = match delete.content() {
        IcebergDeleteFileContent::PositionDeletes => PhysicalDeleteContent::PositionDeletes,
        IcebergDeleteFileContent::EqualityDeletes => PhysicalDeleteContent::EqualityDeletes,
    };
    Ok(IcebergDeleteFileSpec {
        path: delete.path().to_string(),
        file_format,
        file_content,
        length: Some(length),
        content_offset: delete.content_offset(),
        content_size_in_bytes: delete.content_size_in_bytes(),
        referenced_data_file: delete.referenced_data_file().map(str::to_string),
    })
}

/// Reuse the crate's single delete-apply cost bound.
///
/// Restating the 1024-file / 512-MiB limits here would create a second
/// authority that could drift from the one every other reader is admitted
/// against. The projection below carries only what the bound reads -- the data
/// file's identity and each attached delete's size -- and is handed to nothing
/// else.
///
/// Both sides are bounded together: a change window's reverse side opens its
/// newly applied and previously applied artifacts in one read, so charging it
/// for only one of them would admit twice the work the bound allows.
fn validate_split_delete_cost(
    split: &IcebergSplit,
    applied: &[IcebergDeleteFile],
    previously_applied: &[IcebergDeleteFile],
) -> Result<(), ConnectorError> {
    let mut delete_files = Vec::with_capacity(applied.len() + previously_applied.len());
    for delete in applied.iter().chain(previously_applied.iter()) {
        let file_format = match delete.format() {
            IcebergFileFormat::Parquet => crate::scan_model::IcebergDeleteFileFormat::Parquet,
            IcebergFileFormat::Puffin => crate::scan_model::IcebergDeleteFileFormat::Puffin,
            IcebergFileFormat::Orc | IcebergFileFormat::Avro => {
                return Err(unsupported(format!(
                    "iceberg delete file {} is neither parquet nor puffin",
                    delete.path()
                )));
            }
        };
        let file_content = match delete.content() {
            IcebergDeleteFileContent::PositionDeletes => {
                crate::scan_model::IcebergDeleteFileContent::Position
            }
            IcebergDeleteFileContent::EqualityDeletes => {
                crate::scan_model::IcebergDeleteFileContent::Equality
            }
        };
        delete_files.push(crate::scan_model::IcebergDeleteFileInfo {
            path: delete.path().to_string(),
            file_format,
            file_content,
            length: Some(delete.file_size_in_bytes()),
            content_offset: delete.content_offset(),
            content_size_in_bytes: delete.content_size_in_bytes(),
            sequence_number: Some(delete.data_sequence_number()),
            partition_spec_id: Some(split.partition_spec_id()),
            partition_key: None,
            referenced_data_file: delete.referenced_data_file().map(str::to_string),
            equality_column_names: Vec::new(),
            equality_field_ids: delete.equality_field_ids().to_vec(),
        });
    }
    validate_delete_apply_cost(&crate::scan_model::IcebergDataFileInfo {
        path: split.path().to_string(),
        size: split.file_size(),
        row_count: Some(split.file_record_count()),
        column_stats: None,
        partition_spec_id: Some(split.partition_spec_id()),
        partition_key: None,
        first_row_id: split.file_first_row_id(),
        data_sequence_number: split.data_sequence_number(),
        ivm_change_op: None,
        included_positions: None,
        delete_files,
        manifest_path: None,
        partition_values: Vec::new(),
    })
}

fn missing_access(delete: &IcebergDeleteFile) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        format!(
            "iceberg delete file {} needs to be loaded but no filesystem access was resolved for it",
            delete.path()
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc as StdArc;
    use std::time::{Duration, Instant};

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::read_stack::{SplitWeight, TupleDomain};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::commit::DeletionVector;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Type};
    use crate::position_delete::{FILE_PATH_COLUMN, POS_COLUMN};

    use super::super::split::{IcebergDeleteFileParams, IcebergSplitParams};
    use super::*;

    const DATA_FILE: &str = "/warehouse/data/a.parquet";
    const OTHER_DATA_FILE: &str = "/warehouse/data/b.parquet";

    struct Fixture {
        // The Tokio runtime must outlive every handle the binding cloned out
        // of it, so the fixture owns it for the whole test.
        _runtime: tokio::runtime::Runtime,
        directory: tempfile::TempDir,
        manager: DeleteManager,
    }

    impl Fixture {
        fn new() -> Self {
            let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
            let file_runtime: StdArc<dyn FileIoRuntime> =
                StdArc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
            let task_spawner: StdArc<dyn FileTaskSpawner> =
                StdArc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
            let binding =
                IcebergReadBinding::new(None, FsAccessResolver::new(), file_runtime, task_spawner);
            let context = binding
                .file_read_context(
                    FileCancellation::new(),
                    Instant::now() + Duration::from_secs(60),
                )
                .expect("build file read context");
            Self {
                _runtime: runtime,
                directory: tempfile::tempdir().expect("create temporary directory"),
                manager: DeleteManager::new(binding, context),
            }
        }

        fn path(&self, name: &str) -> std::path::PathBuf {
            self.directory.path().join(name)
        }

        fn loaded_artifacts(&self) -> usize {
            self.manager.loaded_artifacts().expect("loaded artifacts")
        }
    }

    fn table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                StdArc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                StdArc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                StdArc::new(NestedField::optional(
                    3,
                    "kind",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("valid iceberg schema")
    }

    fn identified(name: &str, data_type: DataType, field_id: i32) -> Field {
        Field::new(name, data_type, true).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())]
                .into_iter()
                .collect(),
        )
    }

    /// A data page of `id`/`name`/`kind`, tagged with Iceberg field IDs the way
    /// the physical reader hands one to the filter.
    fn data_batch(ids: &[i64], kinds: &[&str]) -> RecordBatch {
        let schema = StdArc::new(ArrowSchema::new(vec![
            identified("id", DataType::Int64, 1),
            identified("name", DataType::Utf8, 2),
            identified("kind", DataType::Utf8, 3),
        ]));
        let names = ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>();
        RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(ids.to_vec())),
                StdArc::new(StringArray::from_iter_values(&names)),
                StdArc::new(StringArray::from(kinds.to_vec())),
            ],
        )
        .expect("build data batch")
    }

    fn positions(values: &[u64]) -> UInt64Array {
        UInt64Array::from(values.to_vec())
    }

    fn keeps(mask: &BooleanArray) -> Vec<bool> {
        (0..mask.len()).map(|row| mask.value(row)).collect()
    }

    fn file_size(path: &Path) -> i64 {
        i64::try_from(fs::metadata(path).expect("delete file metadata").len())
            .expect("delete file size fits in i64")
    }

    fn write_position_delete_parquet(path: &Path, rows: &[(&str, i64)]) {
        let schema = StdArc::new(ArrowSchema::new(vec![
            Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new(POS_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            StdArc::clone(&schema),
            vec![
                StdArc::new(StringArray::from(
                    rows.iter().map(|(path, _)| *path).collect::<Vec<_>>(),
                )),
                StdArc::new(Int64Array::from(
                    rows.iter().map(|(_, pos)| *pos).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("build position-delete batch");
        write_parquet(path, schema, batch);
    }

    /// An equality-delete file over one long column, tagged with its field ID.
    fn write_equality_delete_parquet(path: &Path, field_id: i32, name: &str, values: &[i64]) {
        let schema = StdArc::new(ArrowSchema::new(vec![identified(
            name,
            DataType::Int64,
            field_id,
        )]));
        let batch = RecordBatch::try_new(
            StdArc::clone(&schema),
            vec![StdArc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("build equality-delete batch");
        write_parquet(path, schema, batch);
    }

    /// An equality-delete file over `(kind, id)`, deliberately in the order the
    /// writer chose rather than the table's.
    fn write_kind_id_equality_delete_parquet(path: &Path, rows: &[(&str, i64)]) {
        let schema = StdArc::new(ArrowSchema::new(vec![
            identified("kind", DataType::Utf8, 3),
            identified("id", DataType::Int64, 1),
        ]));
        let batch = RecordBatch::try_new(
            StdArc::clone(&schema),
            vec![
                StdArc::new(StringArray::from(
                    rows.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
                )),
                StdArc::new(Int64Array::from(
                    rows.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("build equality-delete batch");
        write_parquet(path, schema, batch);
    }

    fn write_parquet(path: &Path, schema: StdArc<ArrowSchema>, batch: RecordBatch) {
        let file = fs::File::create(path).expect("create delete file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
        writer.write(&batch).expect("write delete batch");
        writer.close().expect("close parquet writer");
    }

    /// Write a deletion-vector blob behind a byte prefix, the way a Puffin
    /// container places one, and return its content range.
    fn write_deletion_vector(path: &Path, deleted: &[u64], prefix: usize) -> (i64, i64) {
        let mut vector = DeletionVector::new();
        for position in deleted {
            vector.insert(*position).expect("insert deleted position");
        }
        let payload = vector
            .to_iceberg_payload()
            .expect("encode deletion vector payload");
        let mut bytes = vec![0_u8; prefix];
        bytes.extend_from_slice(&payload);
        fs::write(path, &bytes).expect("write deletion vector file");
        (
            i64::try_from(prefix).expect("prefix fits in i64"),
            i64::try_from(payload.len()).expect("payload length fits in i64"),
        )
    }

    struct DeleteBuilder {
        content: IcebergDeleteFileContent,
        path: String,
        format: IcebergFileFormat,
        file_size_in_bytes: i64,
        equality_field_ids: Vec<i32>,
        row_position_lower_bound: Option<i64>,
        row_position_upper_bound: Option<i64>,
        data_sequence_number: i64,
        content_offset: Option<i64>,
        content_size_in_bytes: Option<i64>,
    }

    impl DeleteBuilder {
        fn position(path: &Path, data_sequence_number: i64) -> Self {
            Self {
                content: IcebergDeleteFileContent::PositionDeletes,
                path: path.to_string_lossy().to_string(),
                format: IcebergFileFormat::Parquet,
                file_size_in_bytes: file_size(path),
                equality_field_ids: Vec::new(),
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                data_sequence_number,
                content_offset: None,
                content_size_in_bytes: None,
            }
        }

        fn deletion_vector(path: &Path, data_sequence_number: i64, range: (i64, i64)) -> Self {
            Self {
                content: IcebergDeleteFileContent::PositionDeletes,
                path: path.to_string_lossy().to_string(),
                format: IcebergFileFormat::Puffin,
                file_size_in_bytes: file_size(path),
                equality_field_ids: Vec::new(),
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                data_sequence_number,
                content_offset: Some(range.0),
                content_size_in_bytes: Some(range.1),
            }
        }

        fn equality(path: &Path, data_sequence_number: i64, equality_field_ids: Vec<i32>) -> Self {
            Self {
                content: IcebergDeleteFileContent::EqualityDeletes,
                path: path.to_string_lossy().to_string(),
                format: IcebergFileFormat::Parquet,
                file_size_in_bytes: file_size(path),
                equality_field_ids,
                row_position_lower_bound: None,
                row_position_upper_bound: None,
                data_sequence_number,
                content_offset: None,
                content_size_in_bytes: None,
            }
        }

        fn with_row_position_bounds(mut self, lower: i64, upper: i64) -> Self {
            self.row_position_lower_bound = Some(lower);
            self.row_position_upper_bound = Some(upper);
            self
        }

        fn with_file_size(mut self, file_size_in_bytes: i64) -> Self {
            self.file_size_in_bytes = file_size_in_bytes;
            self
        }

        fn build(self) -> IcebergDeleteFile {
            IcebergDeleteFile::try_new(IcebergDeleteFileParams {
                content: self.content,
                path: self.path,
                format: self.format,
                record_count: 1,
                file_size_in_bytes: self.file_size_in_bytes,
                equality_field_ids: self.equality_field_ids,
                row_position_lower_bound: self.row_position_lower_bound,
                row_position_upper_bound: self.row_position_upper_bound,
                data_sequence_number: self.data_sequence_number,
                content_offset: self.content_offset,
                content_size_in_bytes: self.content_size_in_bytes,
                referenced_data_file: None,
                decryption_data: None,
            })
            .expect("valid delete descriptor")
        }
    }

    fn split_of(
        path: &str,
        data_sequence_number: Option<i64>,
        deletes: Vec<IcebergDeleteFile>,
    ) -> IcebergSplit {
        split_range(path, 0, 1024, data_sequence_number, deletes)
    }

    fn split_range(
        path: &str,
        start: i64,
        length: i64,
        data_sequence_number: Option<i64>,
        deletes: Vec<IcebergDeleteFile>,
    ) -> IcebergSplit {
        IcebergSplit::try_new(IcebergSplitParams {
            path: path.to_string(),
            start,
            length,
            file_size: 1024,
            file_record_count: 3,
            file_format: IcebergFileFormat::Parquet,
            partition_spec_id: 0,
            partition_data_json: "{}".to_string(),
            deletes,
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number,
            file_first_row_id: None,
            decryption_data: None,
            split_weight: SplitWeight::STANDARD,
            affinity_key: None,
        })
        .expect("valid iceberg split")
    }

    #[test]
    fn a_delete_applies_only_above_the_data_sequence_number() {
        let fixture = Fixture::new();
        let stale = fixture.path("stale.parquet");
        let fresh = fixture.path("fresh.parquet");
        write_position_delete_parquet(&stale, &[(DATA_FILE, 0)]);
        write_position_delete_parquet(&fresh, &[(DATA_FILE, 2)]);

        let split = split_of(
            DATA_FILE,
            Some(5),
            vec![
                // Equal is not greater: this one was committed no later than
                // the data file and cannot touch it.
                DeleteBuilder::position(&stale, 5).build(),
                DeleteBuilder::position(&fresh, 6).build(),
            ],
        );
        let filter = fixture
            .manager
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect("open split");

        let mask = filter
            .evaluate(
                &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                &positions(&[0, 1, 2]),
            )
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, true, false]);
        // The gate runs before I/O, so the stale file is never opened.
        assert_eq!(fixture.loaded_artifacts(), 1);
    }

    #[test]
    fn an_equality_delete_without_a_data_sequence_number_is_corrupt_data() {
        let fixture = Fixture::new();
        let deletes = fixture.path("equality.parquet");
        write_equality_delete_parquet(&deletes, 1, "id", &[2]);

        let split = split_of(
            DATA_FILE,
            None,
            vec![DeleteBuilder::equality(&deletes, 7, vec![1]).build()],
        );
        let error = fixture
            .manager
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect_err("missing data sequence number must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(fixture.loaded_artifacts(), 0);
    }

    #[test]
    fn one_delete_path_loads_once_for_two_splits_of_the_same_file() {
        let fixture = Fixture::new();
        let deletes = fixture.path("deletes.parquet");
        write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

        let schema = table_schema();
        let first = split_range(
            DATA_FILE,
            0,
            512,
            Some(1),
            vec![DeleteBuilder::position(&deletes, 9).build()],
        );
        let second = split_range(
            DATA_FILE,
            512,
            512,
            Some(1),
            vec![DeleteBuilder::position(&deletes, 9).build()],
        );

        let first = fixture
            .manager
            .open_split(&first, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect("open first split");
        assert_eq!(fixture.loaded_artifacts(), 1);
        let second = fixture
            .manager
            .open_split(&second, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect("open second split");
        assert_eq!(fixture.loaded_artifacts(), 1);

        let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);
        for filter in [&first, &second] {
            let mask = filter
                .evaluate(&batch, &positions(&[0, 1, 2]))
                .expect("evaluate");
            assert_eq!(keeps(&mask), vec![true, false, true]);
        }
    }

    #[test]
    fn position_deletes_match_absolute_positions_of_their_own_data_file() {
        let fixture = Fixture::new();
        let deletes = fixture.path("deletes.parquet");
        write_position_delete_parquet(
            &deletes,
            &[(DATA_FILE, 2), (OTHER_DATA_FILE, 1), (DATA_FILE, 5)],
        );

        let schema = table_schema();
        let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);

        let mine = split_of(
            DATA_FILE,
            Some(1),
            vec![DeleteBuilder::position(&deletes, 9).build()],
        );
        let mask = fixture
            .manager
            .open_split(&mine, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect("open split")
            .evaluate(&batch, &positions(&[0, 1, 2]))
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, true, false]);

        // Position 5 belongs to the same data file but to another page.
        let mask = fixture
            .manager
            .open_split(&mine, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect("reopen split")
            .evaluate(&batch, &positions(&[4, 5, 6]))
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, false, true]);

        // The other file's row 1 must not follow the delete file across.
        let theirs = split_of(
            OTHER_DATA_FILE,
            Some(1),
            vec![DeleteBuilder::position(&deletes, 9).build()],
        );
        let mask = fixture
            .manager
            .open_split(&theirs, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect("open other split")
            .evaluate(&batch, &positions(&[0, 1, 2]))
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, false, true]);
    }

    #[test]
    fn row_position_bounds_prune_safely_and_never_delete() {
        let fixture = Fixture::new();
        let deletes = fixture.path("deletes.parquet");
        // Consistent with its bounds: every entry names another data file, so
        // loading it would delete nothing here either.
        write_position_delete_parquet(&deletes, &[(OTHER_DATA_FILE, 50), (OTHER_DATA_FILE, 60)]);

        let split = split_of(
            DATA_FILE,
            Some(1),
            vec![
                DeleteBuilder::position(&deletes, 9)
                    .with_row_position_bounds(50, 60)
                    .build(),
            ],
        );
        let filter = fixture
            .manager
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect("open split");

        // A three-row data file cannot hold position 50, so the artifact is
        // never opened -- and pruning removed work, not rows.
        assert_eq!(fixture.loaded_artifacts(), 0);
        assert!(filter.is_empty());
        let mask = filter
            .evaluate(
                &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                &positions(&[0, 1, 2]),
            )
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, true, true]);
    }

    #[test]
    fn one_deletion_vector_applies_and_a_second_one_fails_closed() {
        let fixture = Fixture::new();
        let first = fixture.path("first.puffin");
        let second = fixture.path("second.puffin");
        let first_range = write_deletion_vector(&first, &[1], 24);
        let second_range = write_deletion_vector(&second, &[2], 8);

        let schema = table_schema();
        let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);

        let single = split_of(
            DATA_FILE,
            Some(1),
            vec![DeleteBuilder::deletion_vector(&first, 9, first_range).build()],
        );
        let mask = fixture
            .manager
            .open_split(&single, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect("open split")
            .evaluate(&batch, &positions(&[0, 1, 2]))
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, false, true]);

        let two = split_of(
            DATA_FILE,
            Some(1),
            vec![
                DeleteBuilder::deletion_vector(&first, 9, first_range).build(),
                DeleteBuilder::deletion_vector(&second, 10, second_range).build(),
            ],
        );
        let error = fixture
            .manager
            .open_split(&two, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect_err("two deletion vectors must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn a_deletion_vector_beside_a_position_delete_file_fails_closed() {
        let fixture = Fixture::new();
        let vector_path = fixture.path("dv.puffin");
        let range = write_deletion_vector(&vector_path, &[1], 16);
        let positions_path = fixture.path("positions.parquet");
        write_position_delete_parquet(&positions_path, &[(DATA_FILE, 2)]);

        let split = split_of(
            DATA_FILE,
            Some(1),
            vec![
                DeleteBuilder::deletion_vector(&vector_path, 9, range).build(),
                DeleteBuilder::position(&positions_path, 9).build(),
            ],
        );
        let error = fixture
            .manager
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect_err("ambiguous position-delete state must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn an_equality_field_id_outside_the_schema_is_corrupt_data() {
        let fixture = Fixture::new();
        let deletes = fixture.path("equality.parquet");
        write_equality_delete_parquet(&deletes, 99, "dropped", &[2]);

        let split = split_of(
            DATA_FILE,
            Some(1),
            vec![DeleteBuilder::equality(&deletes, 9, vec![99]).build()],
        );
        let error = fixture
            .manager
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect_err("unknown equality field id must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(fixture.loaded_artifacts(), 0);
    }

    #[test]
    fn equality_keys_and_hidden_columns_follow_the_table_schema_order() {
        let fixture = Fixture::new();
        let deletes = fixture.path("equality.parquet");
        write_kind_id_equality_delete_parquet(&deletes, &[("beta", 2)]);

        // The manifest recorded the IDs as (kind, id); the table says (id, kind).
        let split = split_of(
            DATA_FILE,
            Some(1),
            vec![DeleteBuilder::equality(&deletes, 9, vec![3, 1]).build()],
        );
        let schema = table_schema();
        let filter = fixture
            .manager
            .open_split(&split, &schema, DeleteEvaluationMode::ExcludeDeleted)
            .expect("open split");

        assert_eq!(
            filter
                .required_hidden_columns()
                .iter()
                .map(IcebergColumnHandle::base_field_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        let cache_key = fixture
            .manager
            .lock_state()
            .expect("state")
            .equality
            .keys()
            .map(|key| key.equality_field_ids.clone())
            .collect::<Vec<_>>();
        assert_eq!(cache_key, vec![vec![1, 3]]);

        let mask = filter
            .evaluate(
                &data_batch(&[1, 2, 3], &["beta", "beta", "beta"]),
                &positions(&[0, 1, 2]),
            )
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, false, true]);
    }

    #[test]
    fn the_manager_tracks_the_highest_loaded_sequence_per_equality_key() {
        let fixture = Fixture::new();
        let older = fixture.path("older.parquet");
        let newer = fixture.path("newer.parquet");
        write_equality_delete_parquet(&older, 1, "id", &[1]);
        write_equality_delete_parquet(&newer, 1, "id", &[3]);

        let split = split_of(
            DATA_FILE,
            Some(1),
            vec![
                DeleteBuilder::equality(&older, 4, vec![1]).build(),
                DeleteBuilder::equality(&newer, 11, vec![1]).build(),
            ],
        );
        let filter = fixture
            .manager
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect("open split");

        let highest = fixture
            .manager
            .lock_state()
            .expect("state")
            .equality
            .values()
            .map(|index| index.highest_loaded_sequence)
            .collect::<Vec<_>>();
        assert_eq!(highest, vec![Some(11)]);

        let mask = filter
            .evaluate(
                &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                &positions(&[0, 1, 2]),
            )
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![false, true, false]);
    }

    #[test]
    fn equality_match_only_inverts_the_equality_verdict_and_nothing_else() {
        let fixture = Fixture::new();
        let equality = fixture.path("equality.parquet");
        let position = fixture.path("positions.parquet");
        write_equality_delete_parquet(&equality, 1, "id", &[2]);
        write_position_delete_parquet(&position, &[(DATA_FILE, 0)]);

        let schema = table_schema();
        let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);
        let deletes = || {
            vec![
                DeleteBuilder::equality(&equality, 9, vec![1]).build(),
                DeleteBuilder::position(&position, 9).build(),
            ]
        };

        let exclude = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(1), deletes()),
                &schema,
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect("open split")
            .evaluate(&batch, &positions(&[0, 1, 2]))
            .expect("evaluate");
        assert_eq!(keeps(&exclude), vec![false, false, true]);

        let matched = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(1), deletes()),
                &schema,
                DeleteEvaluationMode::EqualityMatchOnly,
            )
            .expect("open split")
            .evaluate(&batch, &positions(&[0, 1, 2]))
            .expect("evaluate");
        // Row 1 is the only equality match; row 0 stays out because the
        // position verdict is not inverted with it.
        assert_eq!(keeps(&matched), vec![false, true, false]);
    }

    #[test]
    fn a_split_without_equality_deletes_needs_no_hidden_columns() {
        let fixture = Fixture::new();
        let deletes = fixture.path("deletes.parquet");
        write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

        let schema = table_schema();
        let with_positions = fixture
            .manager
            .open_split(
                &split_of(
                    DATA_FILE,
                    Some(1),
                    vec![DeleteBuilder::position(&deletes, 9).build()],
                ),
                &schema,
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect("open split");
        assert!(with_positions.required_hidden_columns().is_empty());
        assert!(!with_positions.is_empty());

        let without_deletes = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(1), Vec::new()),
                &schema,
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect("open split");
        assert!(without_deletes.required_hidden_columns().is_empty());
        assert!(without_deletes.is_empty());

        // Reverse projection with nothing to match keeps nothing, so it is
        // never reported as an empty filter.
        let reverse = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(1), Vec::new()),
                &schema,
                DeleteEvaluationMode::EqualityMatchOnly,
            )
            .expect("open split");
        assert!(!reverse.is_empty());
        let mask = reverse
            .evaluate(
                &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                &positions(&[0, 1, 2]),
            )
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![false, false, false]);
    }

    #[test]
    fn an_oversized_delete_set_is_rejected_by_the_cost_bound() {
        let fixture = Fixture::new();
        let deletes = fixture.path("deletes.parquet");
        write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

        const THREE_HUNDRED_MIB: i64 = 300 * 1024 * 1024;
        let split = split_of(
            DATA_FILE,
            Some(1),
            vec![
                DeleteBuilder::position(&deletes, 9)
                    .with_file_size(THREE_HUNDRED_MIB)
                    .build(),
                DeleteBuilder::position(&deletes, 10)
                    .with_file_size(THREE_HUNDRED_MIB)
                    .build(),
            ],
        );
        let error = fixture
            .manager
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect_err("oversized delete set must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(fixture.loaded_artifacts(), 0);
    }

    #[test]
    fn a_page_whose_positions_do_not_match_its_rows_is_corrupt_data() {
        let fixture = Fixture::new();
        let deletes = fixture.path("deletes.parquet");
        write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

        let filter = fixture
            .manager
            .open_split(
                &split_of(
                    DATA_FILE,
                    Some(1),
                    vec![DeleteBuilder::position(&deletes, 9).build()],
                ),
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .expect("open split");

        let error = filter
            .evaluate(
                &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                &positions(&[0, 1]),
            )
            .expect_err("short position array must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    // --------------------------------------------------- change-window reverse

    #[test]
    fn a_named_selection_keeps_the_newly_deleted_rows_minus_what_was_already_gone() {
        let fixture = Fixture::new();
        let previously = fixture.path("previously.parquet");
        let newly = fixture.path("newly.parquet");
        write_position_delete_parquet(&previously, &[(DATA_FILE, 0)]);
        write_position_delete_parquet(&newly, &[(DATA_FILE, 0), (DATA_FILE, 2)]);

        let filter = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(5), Vec::new()),
                &table_schema(),
                DeleteEvaluationMode::SelectRemovedRows {
                    selected: RemovedRowSelection::NamedBy(vec![
                        DeleteBuilder::position(&newly, 7).build(),
                    ]),
                    previously_applied: vec![DeleteBuilder::position(&previously, 6).build()],
                },
            )
            .expect("open split");

        // Row 0 was already invisible at the lower endpoint, row 1 survives at
        // the upper one, and row 2 is the only row this window removed.
        let mask = filter
            .evaluate(
                &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                &positions(&[0, 1, 2]),
            )
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![false, false, true]);
        assert!(!filter.is_empty());
    }

    #[test]
    fn a_whole_file_selection_subtracts_a_previously_applied_equality_delete() {
        let fixture = Fixture::new();
        let previously = fixture.path("previously.parquet");
        write_equality_delete_parquet(&previously, 1, "id", &[2]);

        let filter = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(5), Vec::new()),
                &table_schema(),
                DeleteEvaluationMode::SelectRemovedRows {
                    selected: RemovedRowSelection::WholeFile,
                    previously_applied: vec![
                        DeleteBuilder::equality(&previously, 6, vec![1]).build(),
                    ],
                },
            )
            .expect("open split");

        // The previously applied side needs its key column read too, so it
        // contributes to the same hidden suffix the applied side does.
        assert_eq!(filter.required_hidden_columns().len(), 1);
        let mask = filter
            .evaluate(
                &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                &positions(&[0, 1, 2]),
            )
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, false, true]);
    }

    #[test]
    fn a_whole_file_selection_with_nothing_already_applied_filters_nothing() {
        let fixture = Fixture::new();
        let filter = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(5), Vec::new()),
                &table_schema(),
                DeleteEvaluationMode::SelectRemovedRows {
                    selected: RemovedRowSelection::WholeFile,
                    previously_applied: Vec::new(),
                },
            )
            .expect("open split");

        // Every row the file still had at the lower endpoint left the relation,
        // so the page source can skip reading positions entirely.
        assert!(filter.is_empty());
        assert_eq!(fixture.loaded_artifacts(), 0);
    }

    #[test]
    fn a_reverse_side_split_that_also_carries_an_exclusion_closure_is_rejected() {
        let fixture = Fixture::new();
        let deletes = fixture.path("deletes.parquet");
        write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

        // One split cannot say both "hide these rows" and "these are exactly
        // the rows to emit" about the same data file.
        let error = fixture
            .manager
            .open_split(
                &split_of(
                    DATA_FILE,
                    Some(5),
                    vec![DeleteBuilder::position(&deletes, 6).build()],
                ),
                &table_schema(),
                DeleteEvaluationMode::SelectRemovedRows {
                    selected: RemovedRowSelection::WholeFile,
                    previously_applied: Vec::new(),
                },
            )
            .expect_err("a reverse-side split names its deletes as variant facts");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.loaded_artifacts(), 0);
    }

    #[test]
    fn one_data_file_never_has_two_deletion_vectors_across_the_two_change_window_sides() {
        let fixture = Fixture::new();
        let previously = fixture.path("previously.puffin");
        let newly = fixture.path("newly.puffin");
        let previously_range = write_deletion_vector(&previously, &[0], 4);
        let newly_range = write_deletion_vector(&newly, &[0, 2], 4);

        // A rewritten deletion vector would put one on each side. Nothing here
        // can prove the replacement covers what it replaced, so the structural
        // rule is proved over both sides together rather than per side.
        let error = fixture
            .manager
            .open_split(
                &split_of(DATA_FILE, Some(5), Vec::new()),
                &table_schema(),
                DeleteEvaluationMode::SelectRemovedRows {
                    selected: RemovedRowSelection::NamedBy(vec![
                        DeleteBuilder::deletion_vector(&newly, 7, newly_range).build(),
                    ]),
                    previously_applied: vec![
                        DeleteBuilder::deletion_vector(&previously, 6, previously_range).build(),
                    ],
                },
            )
            .expect_err("two deletion vectors are two answers for the same rows");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("more than one deletion vector"));
        assert_eq!(fixture.loaded_artifacts(), 0);
    }
}
