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

//! The Iceberg side of the role-facing typed connector read boundary.
//!
//! This module adapts the concrete Iceberg control model in [`crate::typed_read`]
//! onto the two coordinator-side traits an engine role holds:
//! [`TypedConnectorMetadata`] and [`TypedConnectorSplitManager`]. The engine
//! never links this crate: it hands over protocol-validated carriers and
//! receives them back, and only this file converts between a carrier and the
//! concrete Iceberg type inside it.
//!
//! Three properties are load-bearing here:
//!
//! * **the snapshot is pinned once.** `get_table_handle` resolves a reference,
//!   a snapshot id, or the current snapshot exactly once and freezes the answer
//!   into the handle. Nothing downstream -- pushdown, split enumeration, or a
//!   worker -- resolves it again or falls back to a later snapshot.
//! * **no secret crosses the boundary.** Only table properties this file can
//!   prove are non-secret reach the handle; a worker resolves object-store
//!   credentials from its own authorized configuration.
//! * **nothing is guessed.** An unknown system-relation suffix is absence, a
//!   missing manifest fact is an error, and a split that fails wire validation
//!   is an error rather than a silently dropped unit of work.
//!
//! It is additive: the existing [`crate::control_provider`] control path is
//! untouched and keeps its own resolution rules.
// Design: ADR-0114 (docs/adr/ADR-0114-trino-aligned-typed-connector-read-stack.md)

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use novarocks_proto::FieldPath;
use novarocks_proto::connector_read::{
    CatalogTableHandle, ConnectorRelation, ScanAssignment, TypedChangeWindow, TypedColumnBinding,
    TypedConnectorMetadata, TypedConnectorSplitManager, TypedConnectorSplitSource,
    TypedFilterApplication, TypedLimitApplication, TypedRelationVersion, TypedSystemTablePlan,
    ValidatedColumnHandle, ValidatedConnectorSplit, WireConstraint, WireDynamicFilterSnapshot,
    decode_tuple_domain as decode_wire_tuple_domain,
    encode_tuple_domain as encode_wire_tuple_domain,
};
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{
    Assignment, Bound, ConnectorExpression, ConnectorSession, ConnectorSplitBatch,
    ConnectorSplitSource, ConnectorTableHandle as _, ConnectorValue, Constraint, Domain,
    DynamicFilterSnapshot, OrderedAssignments, SchemaTableName, SystemTableDistribution,
    TupleDomain,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceIncarnation,
};

use crate::control_runtime::IcebergControlRuntime;
use crate::file_pruning::file_may_satisfy_physical_predicates;
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, Datum, FormatVersion, Literal, ManifestStatus, NestedField,
    PrimitiveLiteral, PrimitiveType, Schema, SchemaRef, StructType, TableMetadata, Type,
};
use crate::iceberg::table::Table;
use crate::loaded_table::IcebergPhysicalTable;
use crate::read_model::{IcebergReadFile, IcebergReadSnapshot};
use crate::ref_snapshot::resolve_branch_head_snapshot_id;
use crate::row_lineage_synth::{
    ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL,
    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, ICEBERG_RESERVED_FIELD_ID_ROW_ID,
    ICEBERG_ROW_ID_COL,
};
use crate::scan_model::{
    IcebergDataFileInfo, IcebergPartitionFieldValue, IcebergPartitionValue,
    IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
    IcebergPhysicalPredicateValue,
};
use crate::typed_read::column_handle::{corrupt, from_protocol, invalid, unsupported};
use crate::typed_read::{
    HiveTransactionHandle, ICEBERG_CHANGE_OP_COLUMN, IcebergChangeSplit,
    IcebergChangeWindowEndpoints, IcebergChangeWindowHandle, IcebergChangeWindowHandleParams,
    IcebergChangeWindowSplitSource, IcebergColumnHandle, IcebergDeleteFileFacts, IcebergFileFormat,
    IcebergPlannedDataFile, IcebergSplit, IcebergSplitSource, IcebergSplitSourceOptions,
    IcebergTableHandle, IcebergTableHandleParams, change_op_column_handle,
    decode_tuple_domain as decode_iceberg_tuple_domain,
    encode_tuple_domain as encode_iceberg_tuple_domain, plan_change_window_splits,
};

/// The Iceberg table property carrying the default name mapping.
const NAME_MAPPING_PROPERTY: &str = "schema.name-mapping.default";

/// Table properties this boundary is willing to put on a worker-visible handle.
///
/// Every entry is a split-planning knob defined by the Iceberg table-format
/// specification, so none of them can name credential material. Anything else
/// -- a catalog client option, an object-store setting, a vendor extension this
/// process cannot classify -- is dropped rather than carried: a property whose
/// non-secret status cannot be *proven* has no business on the wire, and the
/// reader resolves its own authorized access separately.
const READER_VISIBLE_TABLE_PROPERTIES: [&str; 4] = [
    "read.split.metadata-target-size",
    "read.split.open-file-cost",
    "read.split.planning-lookback",
    "read.split.target-size",
];

/// Iceberg's reserved field ID for the `_file` metadata column.
const RESERVED_FIELD_ID_FILE_PATH: i32 = i32::MAX - 1;

/// Iceberg's reserved field ID for the `_partition` metadata column.
const RESERVED_FIELD_ID_PARTITION: i32 = i32::MAX - 5;

/// Iceberg's reserved field ID for `pos` in the position-delete schema.
///
/// A position-delete file publishes its row-position bounds under this ID, so
/// it is how the manifest walk recovers them without opening the delete file.
const RESERVED_FIELD_ID_DELETE_FILE_POS: i32 = i32::MAX - 102;

/// The field ID this connector assigns to `$file_modified_time`.
///
/// Iceberg reserves IDs 2147483447..=2147483646 for metadata columns but
/// assigns no ID to a file's modification time: the object store, not the table
/// format, owns that fact. Taking the lowest ID of the reserved block keeps it
/// from ever colliding with a format-assigned metadata column or with a real
/// table field, which the format keeps below the block.
const RESERVED_FIELD_ID_FILE_MODIFIED_TIME: i32 = i32::MAX - 200;

/// The Iceberg field name behind the `$partition` pseudo-column.
const ICEBERG_PARTITION_COL: &str = "_partition";

/// The Iceberg field name behind the `$file_modified_time` pseudo-column.
const ICEBERG_FILE_MODIFIED_TIME_COL: &str = "_file_modified_time";

/// SQL-visible name of the data file path pseudo-column.
pub const PSEUDO_COLUMN_PATH: &str = "$path";

/// SQL-visible name of the partition-value pseudo-column.
pub const PSEUDO_COLUMN_PARTITION: &str = "$partition";

/// SQL-visible name of the file modification-time pseudo-column.
pub const PSEUDO_COLUMN_FILE_MODIFIED_TIME: &str = "$file_modified_time";

/// SQL-visible name of the Iceberg v3 row-id pseudo-column.
pub const PSEUDO_COLUMN_ROW_ID: &str = "$row_id";

/// SQL-visible name of the Iceberg v3 row-lineage sequence pseudo-column.
pub const PSEUDO_COLUMN_LAST_UPDATED_SEQUENCE_NUMBER: &str = "$last_updated_sequence_number";

/// One Iceberg system relation reachable as `<table>$<suffix>`.
///
/// The set is closed by the wire contract plus `Partitions`, which is not a
/// worker-visible reference of its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergSystemRelation {
    Files,
    Entries,
    Snapshots,
    History,
    Refs,
    Manifests,
    /// `$partitions` aggregates the very rows `$files` produces for the same
    /// pinned snapshot. It is deliberately absent from the wire's system-table
    /// enum: minting a seventh reference kind would give the aggregation its
    /// own resolution path, and the two could then disagree about which
    /// snapshot they describe.
    Partitions,
}

impl IcebergSystemRelation {
    /// The immutable reference kind a worker is given.
    const fn system_table_type(self) -> dto::IcebergSystemTableType {
        match self {
            // `$partitions` binds to the same pinned FILES plan it aggregates.
            Self::Files | Self::Partitions => dto::IcebergSystemTableType::Files,
            Self::Entries => dto::IcebergSystemTableType::Entries,
            Self::Snapshots => dto::IcebergSystemTableType::Snapshots,
            Self::History => dto::IcebergSystemTableType::History,
            Self::Refs => dto::IcebergSystemTableType::Refs,
            Self::Manifests => dto::IcebergSystemTableType::Manifests,
        }
    }

    /// Where the relation runs.
    ///
    /// FILES walks every manifest of the snapshot, which is real distributable
    /// I/O. The other five read only the single pinned metadata file, so
    /// spreading them over the cluster would multiply one small read rather
    /// than divide any work.
    const fn distribution(self) -> SystemTableDistribution {
        match self {
            Self::Files | Self::Partitions => SystemTableDistribution::AllNodes,
            Self::Entries | Self::Snapshots | Self::History | Self::Refs | Self::Manifests => {
                SystemTableDistribution::SingleCoordinator
            }
        }
    }
}

/// The coordinator-side Iceberg adapter for one catalog instance generation.
///
/// It is constructed per transaction, exactly like Trino's per-transaction
/// `ConnectorMetadata`: the transaction marker it carries is stamped onto every
/// handle it mints, so a handle can never outlive the transaction that framed
/// it.
#[derive(Clone)]
pub struct IcebergTypedBoundary {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    transaction: HiveTransactionHandle,
    runtime: Arc<IcebergControlRuntime>,
    split_source_options: IcebergSplitSourceOptions,
}

impl IcebergTypedBoundary {
    /// The composition-root entry point.
    ///
    /// `runtime` is the same control generation the existing
    /// [`crate::control_provider::IcebergControlProvider`] holds, so both paths
    /// observe one catalog client and one physical-table cache.
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        transaction: HiveTransactionHandle,
        runtime: Arc<IcebergControlRuntime>,
    ) -> Self {
        Self {
            descriptor,
            incarnation,
            transaction,
            runtime,
            split_source_options: IcebergSplitSourceOptions::default(),
        }
    }

    /// Session knobs that change how files are cut, never what they contain.
    #[must_use]
    pub const fn with_split_source_options(mut self, options: IcebergSplitSourceOptions) -> Self {
        self.split_source_options = options;
        self
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    /// Load one relation, distinguishing absence from a control-plane failure.
    ///
    /// The cache is invalidated first because this is the boundary's single
    /// observation point for catalog truth: another engine can advance a REST
    /// or Hadoop table between statements, and a cached physical table would
    /// otherwise pin a snapshot that is already superseded before the statement
    /// even starts.
    fn load_relation(
        &self,
        name: &SchemaTableName,
    ) -> Result<Option<IcebergPhysicalTable>, ConnectorError> {
        self.runtime
            .control_state()
            .invalidate_table(name.schema_name(), name.table_name());
        self.load_pinned_relation(name).map(Some).or_else(|error| {
            if error.kind() == ConnectorErrorKind::NotFound {
                Ok(None)
            } else {
                Err(error)
            }
        })
    }

    /// Load one relation whose snapshot is already pinned.
    ///
    /// Deliberately does not invalidate: re-observing catalog truth here could
    /// only replace the metadata the pinned snapshot was chosen from, and the
    /// pinned id is passed explicitly to every walk below.
    fn load_pinned_relation(
        &self,
        name: &SchemaTableName,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        self.runtime
            .load_table_classified(name.schema_name(), name.table_name())
            .map_err(|(kind, message)| ConnectorError::new(kind, message))
    }

    /// Stamp one relation with this catalog instance and transaction.
    fn wrap_relation(
        &self,
        relation: dto::catalog_table_handle::Relation,
    ) -> Result<CatalogTableHandle, ConnectorError> {
        let raw = dto::CatalogTableHandle {
            catalog_name: self.descriptor.instance_id.as_str().to_string(),
            instance_incarnation: self.incarnation.to_bytes().to_vec(),
            transaction: Some(self.transaction.to_transaction_handle_proto()),
            relation: Some(relation),
        };
        CatalogTableHandle::parse(raw, FieldPath::root("catalog_table_handle"))
            .map_err(from_protocol)
    }

    fn wrap_table(&self, handle: IcebergTableHandle) -> Result<CatalogTableHandle, ConnectorError> {
        self.wrap_relation(dto::catalog_table_handle::Relation::Table(
            handle.to_table_handle_proto(),
        ))
    }

    /// Reject a handle that belongs to another catalog or another generation.
    ///
    /// An incarnation mismatch means the catalog was replaced under the plan;
    /// serving it would silently read a different physical catalog than the one
    /// the handle was frozen against.
    fn ensure_owned(&self, table: &CatalogTableHandle) -> Result<(), ConnectorError> {
        if table.catalog_name() != self.descriptor.instance_id.as_str() {
            return Err(invalid(format!(
                "iceberg catalog {} received a handle for catalog {}",
                self.descriptor.instance_id.as_str(),
                table.catalog_name()
            )));
        }
        if table.instance_incarnation() != self.incarnation.to_bytes() {
            return Err(invalid(
                "iceberg catalog table handle names another instance incarnation",
            ));
        }
        Ok(())
    }

    /// The concrete DATA-relation handle behind a validated carrier.
    /// The data table handle a pushdown would apply to, if this relation has
    /// one.
    ///
    /// Absent means this relation accepts no pushdown at all, which is what
    /// `None` means to every `apply_*` caller: the engine keeps the whole
    /// predicate, projection or limit. Refusing instead would fail the scan,
    /// even though not accepting a pushdown is always a legal answer. An
    /// ownership or decoding failure is still an error.
    fn pushdown_table_handle(
        &self,
        table: &CatalogTableHandle,
    ) -> Result<Option<IcebergTableHandle>, ConnectorError> {
        self.ensure_owned(table)?;
        match table.relation() {
            ConnectorRelation::Table(handle) => {
                IcebergTableHandle::from_table_handle_proto(handle).map(Some)
            }
            ConnectorRelation::TableFunction(_)
            | ConnectorRelation::ChangeWindow(_)
            | ConnectorRelation::SystemTable(_)
            | ConnectorRelation::TableExecute(_)
            | ConnectorRelation::MergeTable(_) => Ok(None),
        }
    }

    fn data_table_handle(
        &self,
        table: &CatalogTableHandle,
    ) -> Result<IcebergTableHandle, ConnectorError> {
        self.ensure_owned(table)?;
        match table.relation() {
            ConnectorRelation::Table(handle) => IcebergTableHandle::from_table_handle_proto(handle),
            ConnectorRelation::TableFunction(_) => Err(unsupported(
                "an iceberg table-function relation has no data table handle",
            )),
            ConnectorRelation::ChangeWindow(_) => Err(unsupported(
                "an iceberg change-window relation has no data table handle",
            )),
            ConnectorRelation::SystemTable(_) => Err(unsupported(
                "an iceberg system relation has no data table handle",
            )),
            ConnectorRelation::TableExecute(_) => Err(unsupported(
                "an iceberg table-execute relation has no data table handle",
            )),
            ConnectorRelation::MergeTable(_) => Err(unsupported(
                "an iceberg merge relation has no data table handle",
            )),
        }
    }

    /// Walk the pinned snapshot and turn its surviving files into planning input.
    fn planned_files(
        &self,
        handle: &IcebergTableHandle,
        snapshot_id: i64,
        constraint: &WireConstraint,
    ) -> Result<Vec<IcebergPlannedDataFile>, ConnectorError> {
        let schema = handle.parse_table_schema()?;
        let static_predicate = handle
            .effective_predicate()?
            .intersect(&wire_domain_to_iceberg(constraint.summary())?)?;
        if static_predicate.is_none() {
            // Planning already proved the scan reads nothing, so no manifest
            // needs to be opened at all.
            return Ok(Vec::new());
        }
        let predicates = physical_predicates(&static_predicate, &schema);

        let physical = self.load_pinned_relation(handle.schema_table_name())?;
        let table = physical.table.clone();
        let (read_snapshot, facts) = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { plan_pinned_snapshot(table, snapshot_id).await })
            .map_err(unavailable)?
            .map_err(unavailable)?;

        let mut planned = Vec::with_capacity(read_snapshot.files.len());
        for read_file in read_snapshot.files {
            if !predicates.is_empty()
                && !file_may_satisfy_physical_predicates(
                    &pruning_view(handle, &schema, &read_file)?,
                    &predicates,
                )
            {
                continue;
            }
            planned.push(planned_data_file(read_file, &facts)?);
        }
        Ok(planned)
    }

    /// The visible files of one change-window endpoint.
    ///
    /// No predicate is applied: a change window is a difference of two endpoint
    /// row sets, and pruning one endpoint's files without pruning the other's
    /// identically would turn a survived file into a spurious add or removal.
    fn change_window_endpoint_files(
        &self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<Vec<IcebergPlannedDataFile>, ConnectorError> {
        let table = table.clone();
        let (read_snapshot, facts) = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { plan_pinned_snapshot(table, snapshot_id).await })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        read_snapshot
            .files
            .into_iter()
            .map(|read_file| planned_data_file(read_file, &facts))
            .collect()
    }

    /// The concrete change-window handle behind a validated carrier.
    fn change_window_handle(
        &self,
        table: &CatalogTableHandle,
    ) -> Result<IcebergChangeWindowHandle, ConnectorError> {
        self.ensure_owned(table)?;
        match table.relation() {
            ConnectorRelation::ChangeWindow(handle) => {
                IcebergChangeWindowHandle::from_change_window_handle_proto(handle)
            }
            ConnectorRelation::Table(_) => Err(unsupported(
                "an iceberg data relation is not a change window",
            )),
            ConnectorRelation::TableFunction(_) => Err(unsupported(
                "an iceberg table-function relation is not a change window",
            )),
            ConnectorRelation::SystemTable(_) => Err(unsupported(
                "an iceberg system relation is not a change window",
            )),
            ConnectorRelation::TableExecute(_) => Err(unsupported(
                "an iceberg table-execute relation is not a change window",
            )),
            ConnectorRelation::MergeTable(_) => Err(unsupported(
                "an iceberg merge relation is not a change window",
            )),
        }
    }

    /// Enumerate one frozen change window as the difference of its endpoints.
    fn change_window_split_source(
        &self,
        handle: &IcebergChangeWindowHandle,
    ) -> Result<IcebergChangeWindowSplitSource, ConnectorError> {
        let physical = self.load_pinned_relation(handle.schema_table_name())?;
        let schema = handle.parse_table_schema()?;
        let partition_types = change_window_partition_types(physical.table.metadata(), &schema)?;
        let from_visible = self
            .change_window_endpoint_files(&physical.table, handle.from_snapshot_id_exclusive())?;
        let to_visible =
            self.change_window_endpoint_files(&physical.table, handle.to_snapshot_id_inclusive())?;
        let plan = plan_change_window_splits(
            handle,
            IcebergChangeWindowEndpoints {
                from_visible: &from_visible,
                to_visible: &to_visible,
            },
            &partition_types,
            self.split_source_options,
        )?;
        Ok(IcebergChangeWindowSplitSource::new(plan))
    }
}

/// Pair one snapshot's read view of a data file with its manifest facts.
fn planned_data_file(
    read_file: IcebergReadFile,
    facts: &ManifestFacts,
) -> Result<IcebergPlannedDataFile, ConnectorError> {
    let data_facts = facts.data.get(&read_file.path).ok_or_else(|| {
        corrupt(format!(
            "iceberg data file {} has no manifest entry in the pinned snapshot",
            read_file.path
        ))
    })?;
    let mut delete_facts = BTreeMap::new();
    for delete in &read_file.deletes {
        let fact = facts.deletes.get(&delete.path).ok_or_else(|| {
            corrupt(format!(
                "iceberg delete file {} has no manifest entry in the pinned snapshot",
                delete.path
            ))
        })?;
        delete_facts.insert(delete.path.clone(), fact.clone());
    }
    Ok(IcebergPlannedDataFile {
        file_format: IcebergFileFormat::from_data_file_format(data_facts.file_format)?,
        split_offsets: data_facts.split_offsets.clone(),
        key_metadata: data_facts.key_metadata.clone(),
        // This boundary proves no per-file value bound of its own.
        // Coordinator-side pruning already used the manifest facts in their
        // native, untyped form; re-encoding those bytes as typed domain values
        // would invent a type the manifest never recorded, so the truthful
        // statistics domain here is "all".
        file_statistics_domain: TupleDomain::all(),
        decryption_data: None,
        delete_facts,
        read_file,
    })
}

impl std::fmt::Debug for IcebergTypedBoundary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergTypedBoundary")
            .field("instance_id", &self.descriptor.instance_id)
            .field("runtime", &self.runtime)
            .field("split_source_options", &self.split_source_options)
            .finish()
    }
}

impl TypedConnectorMetadata for IcebergTypedBoundary {
    fn get_table_handle(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        version: TypedRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
        if system_relation_of(name.table_name()).is_some() {
            // A `<table>$<suffix>` relation exists, but not as a DATA table.
            // Returning a table handle here would give the same relation two
            // execution shapes; the engine asks `get_system_table_plan` next.
            return Ok(None);
        }
        let Some(physical) = self.load_relation(name)? else {
            return Ok(None);
        };
        let metadata = physical.table.metadata();
        let snapshot_id = pin_snapshot(metadata, version, reference)?;
        self.wrap_table(pinned_table_handle(name, metadata, snapshot_id)?)
            .map(Some)
    }

    fn get_column_bindings(
        &self,
        _session: &ConnectorSession,
        table: &CatalogTableHandle,
    ) -> Result<Vec<TypedColumnBinding>, ConnectorError> {
        self.ensure_owned(table)?;
        // A change window exposes a different column set from the data
        // relation it differences, so the two are answered separately rather
        // than by one shape that would have to be right for both.
        match table.relation() {
            ConnectorRelation::ChangeWindow(_) => {
                change_window_column_bindings(&self.change_window_handle(table)?)
            }
            ConnectorRelation::Table(_)
            | ConnectorRelation::TableFunction(_)
            | ConnectorRelation::SystemTable(_)
            | ConnectorRelation::TableExecute(_)
            | ConnectorRelation::MergeTable(_) => {
                let handle = self.data_table_handle(table)?;
                let schema = handle.parse_table_schema()?;
                let fields = schema.as_struct().fields();
                let mut bindings = Vec::with_capacity(fields.len() + 5);
                for field in fields {
                    let column = IcebergColumnHandle::base_column(field.as_ref())?;
                    bindings.push(TypedColumnBinding::new(
                        &field.name,
                        iceberg_column_to_wire(&column)?,
                        false,
                    ));
                }
                for (name, column) in metadata_pseudo_columns(&handle, &schema)? {
                    bindings.push(TypedColumnBinding::new(
                        name,
                        iceberg_column_to_wire(&column)?,
                        true,
                    ));
                }
                Ok(bindings)
            }
        }
    }

    fn apply_filter(
        &self,
        _session: &ConnectorSession,
        table: &CatalogTableHandle,
        constraint: &WireConstraint,
    ) -> Result<Option<TypedFilterApplication>, ConnectorError> {
        let Some(handle) = self.pushdown_table_handle(table)? else {
            return Ok(None);
        };
        let applied = handle.apply_filter(&wire_constraint_to_iceberg(constraint)?)?;
        if applied.handle() == &handle {
            // The connector accepted nothing, so the engine keeps the whole
            // predicate; reporting an unchanged handle would only re-plan.
            return Ok(None);
        }
        let remaining_expression = applied.remaining_expression().cloned();
        // The residual constraint is the whole thing the engine must still
        // evaluate: the domains Iceberg could not enforce, plus the expression
        // it never accepted. Both halves keep the caller's own assignments, so
        // every variable in the expression stays bound.
        let remaining_constraint = Constraint::try_new(
            iceberg_domain_to_wire(applied.remaining_filter())?,
            remaining_expression
                .clone()
                .unwrap_or_else(ConnectorExpression::constant_true),
            constraint.assignments().clone(),
        )?;
        let wrapped = self.wrap_table(applied.into_handle())?;
        Ok(Some(TypedFilterApplication::new(
            wrapped,
            remaining_constraint,
            remaining_expression,
        )))
    }

    fn apply_projection(
        &self,
        _session: &ConnectorSession,
        table: &CatalogTableHandle,
        assignments: &[ScanAssignment],
    ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
        let Some(handle) = self.pushdown_table_handle(table)? else {
            return Ok(None);
        };
        let applied = handle.apply_projection(&ordered_assignments(assignments)?)?;
        if applied.handle() == &handle {
            return Ok(None);
        }
        self.wrap_table(applied.into_handle()).map(Some)
    }

    fn apply_limit(
        &self,
        _session: &ConnectorSession,
        table: &CatalogTableHandle,
        limit: u64,
    ) -> Result<Option<TypedLimitApplication>, ConnectorError> {
        let Some(handle) = self.pushdown_table_handle(table)? else {
            return Ok(None);
        };
        let applied = handle.apply_limit(limit)?;
        if applied.handle() == &handle {
            return Ok(None);
        }
        // `limit_guaranteed` is the concrete handle's own answer. Iceberg can
        // only guarantee a zero-row bound, because deletes are applied per
        // split and no split knows how many rows its siblings produced.
        let limit_guaranteed = applied.limit_guaranteed();
        let wrapped = self.wrap_table(applied.into_handle())?;
        Ok(Some(TypedLimitApplication::new(wrapped, limit_guaranteed)))
    }

    fn get_system_table_plan(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<TypedSystemTablePlan>, ConnectorError> {
        let Some((base_table, relation)) = system_relation_of(name.table_name()) else {
            // An unrecognized suffix is not a system relation of this
            // connector. Guessing one would turn a plain typo into a metadata
            // scan of some other relation.
            return Ok(None);
        };
        let base_name = SchemaTableName::try_new(name.schema_name(), &base_table)?;
        let Some(physical) = self.load_relation(&base_name)? else {
            return Ok(None);
        };
        let metadata_file_location = physical
            .table
            .metadata_location()
            .ok_or_else(|| {
                corrupt(format!(
                    "iceberg relation {}.{base_table} has no metadata file location",
                    name.schema_name()
                ))
            })?
            .to_string();
        let metadata = physical.table.metadata();
        let reference = dto::IcebergSystemTableReference {
            schema_table_name: Some(dto::SchemaTableName {
                schema_name: name.schema_name().to_string(),
                // The reference names the relation whose metadata file is
                // frozen, not the `$suffix` spelling that selected it.
                table_name: base_table,
            }),
            system_table_type: relation.system_table_type() as i32,
            metadata_file_location,
            table_uuid: metadata.uuid().to_string(),
            snapshot_id: metadata.current_snapshot_id(),
        };
        let wrapped = self.wrap_relation(dto::catalog_table_handle::Relation::SystemTable(
            dto::ConnectorSystemTableReference {
                reference: Some(dto::connector_system_table_reference::Reference::Iceberg(
                    reference,
                )),
            },
        ))?;
        Ok(Some(TypedSystemTablePlan::new(
            wrapped,
            relation.distribution(),
        )))
    }

    fn get_change_window_plan(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        window: TypedChangeWindow,
    ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
        if system_relation_of(name.table_name()).is_some() {
            // A `<table>$<suffix>` relation is a view of one pinned metadata
            // file. It has no row history at all, so it exposes no change
            // window -- which is absence, not a window this connector failed
            // to serve.
            return Ok(None);
        }
        let Some(physical) = self.load_relation(name)? else {
            return Ok(None);
        };
        let Some(handle) = pinned_change_window_handle(name, physical.table.metadata(), window)?
        else {
            return Ok(None);
        };
        self.wrap_relation(dto::catalog_table_handle::Relation::ChangeWindow(
            handle.to_change_window_handle_proto(),
        ))
        .map(Some)
    }
}

impl TypedConnectorSplitManager for IcebergTypedBoundary {
    fn get_splits(
        &self,
        _session: &ConnectorSession,
        table: &CatalogTableHandle,
        columns: &[ScanAssignment],
        dynamic_filter_columns: &BTreeSet<ValidatedColumnHandle>,
        constraint: &WireConstraint,
    ) -> Result<Box<dyn TypedConnectorSplitSource>, ConnectorError> {
        self.ensure_owned(table)?;
        // Every dynamic-filter column must be one this connector can name. A
        // column it cannot decode could never be matched against a split, so
        // accepting it would silently disable the filter instead of failing.
        for column in dynamic_filter_columns {
            wire_column_to_iceberg(column)?;
        }

        // A change window enumerates the difference of two endpoints, which is
        // a different question from cutting one pinned snapshot's files. It
        // gets its own enumerator rather than a mode of this one.
        if let ConnectorRelation::ChangeWindow(_) = table.relation() {
            let handle = self.change_window_handle(table)?;
            return Ok(Box::new(IcebergTypedSplitSource::new(
                self.change_window_split_source(&handle)?,
            )));
        }

        let handle = self.data_table_handle(table)?;

        // The split source decides its partition-only fast path from the
        // handle's projected column set, and a zero-column projection qualifies
        // for it. The scan's ordered assignments are the output authority, so
        // enumeration reads the projection from them; the handle that travels
        // to the workers is left exactly as planning froze it.
        let enumeration_handle = if columns.is_empty() {
            handle.clone()
        } else {
            handle
                .apply_projection(&ordered_assignments(columns)?)?
                .into_handle()
        };

        let files = match handle.snapshot_id() {
            // A relation with no pinned snapshot reads zero rows. That is a
            // fact of the relation, not a reason to resolve a later snapshot.
            None => Vec::new(),
            Some(snapshot_id) => self.planned_files(&handle, snapshot_id, constraint)?,
        };
        let source =
            IcebergSplitSource::try_new(&enumeration_handle, files, self.split_source_options)?;
        Ok(Box::new(IcebergTypedSplitSource::new(source)))
    }
}

/// The wire-facing view of one Iceberg split source.
///
/// It owns nothing but the conversion: each concrete split is encoded into the
/// neutral scheduler envelope and validated, and the dynamic-filter snapshot is
/// lowered to the concrete column type on the way in.
#[derive(Debug)]
pub struct IcebergTypedSplitSource<S> {
    inner: S,
    closed: bool,
}

impl<S> IcebergTypedSplitSource<S> {
    pub const fn new(inner: S) -> Self {
        Self {
            inner,
            closed: false,
        }
    }
}

/// A concrete Iceberg split that knows its own neutral scheduler envelope.
///
/// The trait exists so the wire-facing wrapper below stays one implementation
/// for both enumerated split kinds. It is deliberately not blanket: a third
/// split kind has to state how it encodes before it can be wrapped.
pub trait IcebergWireSplit {
    fn to_wire_split_proto(&self) -> dto::ConnectorSplit;
}

impl IcebergWireSplit for IcebergSplit {
    fn to_wire_split_proto(&self) -> dto::ConnectorSplit {
        self.to_connector_split_proto()
    }
}

impl IcebergWireSplit for IcebergChangeSplit {
    fn to_wire_split_proto(&self) -> dto::ConnectorSplit {
        self.to_connector_split_proto()
    }
}

impl<S> TypedConnectorSplitSource for IcebergTypedSplitSource<S>
where
    S: ConnectorSplitSource<Column = IcebergColumnHandle> + Send,
    S::Split: IcebergWireSplit,
{
    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &WireDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ValidatedConnectorSplit>, ConnectorError> {
        if self.closed {
            return Ok(ConnectorSplitBatch::finished());
        }
        // Today the frontend produces no runtime feedback, so this snapshot is
        // the complete, unconstrained one. It is forwarded exactly as received:
        // inventing a tighter predicate, or a reason to wait for one, would
        // fabricate a blocked scan.
        let snapshot = DynamicFilterSnapshot::new(
            wire_domain_to_iceberg(dynamic_filter.current_predicate())?,
            dynamic_filter.is_complete(),
        );
        let batch = self.inner.next_batch(max_size, &snapshot)?;
        // An empty batch means "nothing right now"; only this flag ends
        // enumeration.
        let no_more_splits = batch.no_more_splits();
        let concrete = batch.into_splits();
        let mut splits = Vec::with_capacity(concrete.len());
        for split in concrete {
            // A split that fails validation is an error, never a skip: dropping
            // one would silently return fewer rows than the query asked for.
            splits.push(
                ValidatedConnectorSplit::parse(
                    split.to_wire_split_proto(),
                    FieldPath::root("connector_split"),
                )
                .map_err(from_protocol)?,
            );
        }
        Ok(ConnectorSplitBatch::new(splits, no_more_splits))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.inner.is_finished()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.inner.close()
    }
}

// ---------------------------------------------------------------------------
// Snapshot pinning and handle construction
// ---------------------------------------------------------------------------

/// Resolve the requested version to exactly one snapshot, once.
///
/// A branch or tag is resolved here, by the connector, from the same metadata
/// the rest of the handle is frozen from. The engine never sees a reference
/// name again, so no later stage can resolve it to a different snapshot.
fn pin_snapshot(
    metadata: &TableMetadata,
    version: TypedRelationVersion,
    reference: Option<&str>,
) -> Result<Option<i64>, ConnectorError> {
    match version {
        TypedRelationVersion::Current => {
            if reference.is_some() {
                return Err(invalid(
                    "iceberg current-version read must not name a reference",
                ));
            }
            Ok(metadata.current_snapshot_id())
        }
        TypedRelationVersion::SnapshotId(snapshot_id) => {
            if reference.is_some() {
                return Err(invalid(
                    "iceberg snapshot-id read must not also name a reference",
                ));
            }
            metadata
                .snapshot_by_id(snapshot_id)
                .map(|_| Some(snapshot_id))
                .ok_or_else(|| not_found(format!("iceberg snapshot {snapshot_id} does not exist")))
        }
        TypedRelationVersion::Reference => {
            let reference = reference
                .ok_or_else(|| invalid("iceberg reference read requires a branch or tag name"))?;
            // `None` is an unborn branch: it exists and reads zero rows, which
            // is not the same as falling back to the current snapshot.
            resolve_branch_head_snapshot_id(metadata, reference).map_err(not_found)
        }
    }
}

/// The schema the pinned snapshot was written under.
///
/// Schema evolution after the snapshot must not retype a frozen read, so the
/// snapshot's own schema is used -- the same one the manifest walk resolves.
fn pinned_schema(
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
) -> Result<SchemaRef, ConnectorError> {
    match snapshot_id {
        Some(snapshot_id) => metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| not_found(format!("iceberg snapshot {snapshot_id} does not exist")))?
            .schema(metadata)
            .map_err(|error| {
                corrupt(format!(
                    "iceberg snapshot {snapshot_id} names a schema the table metadata does not carry: {error}"
                ))
            }),
        // A relation with no snapshot reads zero rows; its current schema is
        // the only schema that has ever existed.
        None => Ok(metadata.current_schema().clone()),
    }
}

/// Freeze one worker-visible DATA relation handle.
fn pinned_table_handle(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
) -> Result<IcebergTableHandle, ConnectorError> {
    let schema = pinned_schema(metadata, snapshot_id)?;
    let mut partition_spec_jsons = BTreeMap::new();
    for spec in metadata.partition_specs_iter() {
        let json = serde_json::to_string(spec.as_ref()).map_err(|error| {
            corrupt(format!(
                "iceberg partition spec {} cannot be encoded: {error}",
                spec.spec_id()
            ))
        })?;
        partition_spec_jsons.insert(spec.spec_id(), json);
    }
    let spec_id = metadata.default_partition_spec_id();
    if !partition_spec_jsons.contains_key(&spec_id) {
        return Err(corrupt(format!(
            "iceberg table metadata names default partition spec {spec_id} but does not carry it"
        )));
    }
    let table_schema_json = serde_json::to_string(schema.as_ref())
        .map_err(|error| corrupt(format!("iceberg table schema cannot be encoded: {error}")))?;

    IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: name.clone(),
        snapshot_id,
        table_schema_json,
        spec_id: Some(spec_id),
        partition_spec_jsons,
        format_version: metadata.format_version() as i32,
        // Pushdown has not been offered yet; both predicates start unrestricted
        // rather than at some assumed default.
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: BTreeSet::new(),
        name_mapping_json: metadata.properties().get(NAME_MAPPING_PROPERTY).cloned(),
        table_location: metadata.location().to_string(),
        storage_properties: reader_visible_storage_properties(metadata.properties()),
    })
}

/// Freeze one worker-visible change-window relation handle.
///
/// The two answers this returns are deliberately different questions.
/// `Ok(None)` is a fact about the *relation*: a v1 table has no delete files at
/// all, so it can never express a row that stopped being visible, and this
/// connector exposes no change window over one. Every error below is a fact
/// about *this window*, and each is raised now rather than becoming a
/// difference that quietly means something else:
///
/// * an endpoint that is not in the metadata cannot be differenced at all;
/// * `to` must descend from `from`, because the two must be points on one
///   history for their difference to be the window's changes rather than the
///   distance between two unrelated branches;
/// * the handle carries exactly one schema, so the endpoints must agree on the
///   field identities it describes.
///
/// Both endpoints are pinned here, exactly as [`pinned_table_handle`] pins one
/// snapshot: nothing downstream resolves either of them again.
fn pinned_change_window_handle(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    window: TypedChangeWindow,
) -> Result<Option<IcebergChangeWindowHandle>, ConnectorError> {
    match metadata.format_version() {
        FormatVersion::V2 | FormatVersion::V3 => {}
        FormatVersion::V1 => return Ok(None),
    }

    let from = window.from_snapshot_id();
    let to = window.to_snapshot_id();
    for snapshot_id in [from, to] {
        if metadata.snapshot_by_id(snapshot_id).is_none() {
            return Err(not_found(format!(
                "iceberg snapshot {snapshot_id} does not exist"
            )));
        }
    }
    if !snapshot_descends_from(metadata, to, from) {
        return Err(unsupported(format!(
            "iceberg snapshot {to} does not descend from snapshot {from}, so the two are not endpoints of one change window"
        )));
    }

    let from_schema = pinned_schema(metadata, Some(from))?;
    let to_schema = pinned_schema(metadata, Some(to))?;
    if !struct_types_share_field_identities(from_schema.as_struct(), to_schema.as_struct()) {
        // A rename preserves every field ID, so one frozen schema still
        // describes both endpoints. Anything else does not, and the single
        // `table_schema_json` this handle carries would misdescribe one side.
        return Err(unsupported(format!(
            "iceberg snapshots {from} and {to} do not share field identities, so one change-window schema cannot describe both"
        )));
    }

    let fields = to_schema.as_struct().fields();
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        columns.push(IcebergColumnHandle::base_column(field.as_ref())?);
    }
    let table_schema_json = serde_json::to_string(to_schema.as_ref())
        .map_err(|error| corrupt(format!("iceberg table schema cannot be encoded: {error}")))?;

    // Every spec, not the default one: a window spans two snapshots, so files
    // written under an older spec can appear on either side of the difference
    // and each split must be decodable from the handle alone.
    let mut partition_spec_jsons = BTreeMap::new();
    for spec in metadata.partition_specs_iter() {
        let json = serde_json::to_string(spec.as_ref()).map_err(|error| {
            corrupt(format!(
                "iceberg partition spec {} cannot be encoded: {error}",
                spec.spec_id()
            ))
        })?;
        partition_spec_jsons.insert(spec.spec_id(), json);
    }

    IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
        schema_table_name: name.clone(),
        table_schema_json,
        columns,
        name_mapping_json: metadata.properties().get(NAME_MAPPING_PROPERTY).cloned(),
        from_snapshot_id_exclusive: from,
        to_snapshot_id_inclusive: to,
        partition_spec_jsons,
    })
    .map(Some)
}

/// Whether `descendant` reaches `ancestor` by following parent pointers.
///
/// The walk is bounded by the number of snapshots the metadata carries, so a
/// corrupted parent cycle ends the search instead of hanging the coordinator.
fn snapshot_descends_from(metadata: &TableMetadata, descendant: i64, ancestor: i64) -> bool {
    let mut cursor = Some(descendant);
    for _ in 0..=metadata.snapshots().len() {
        let Some(snapshot_id) = cursor else {
            return false;
        };
        if snapshot_id == ancestor {
            return true;
        }
        cursor = metadata
            .snapshot_by_id(snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    false
}

/// Whether two schemas describe the same fields, ignoring their names.
///
/// Field IDs and types are what a read binds to, so a pure rename leaves a
/// frozen read valid; anything else changes what a column *is*.
fn struct_types_share_field_identities(previous: &StructType, next: &StructType) -> bool {
    let previous = previous.fields();
    let next = next.fields();
    previous.len() == next.len()
        && previous.iter().zip(next.iter()).all(|(previous, next)| {
            previous.id == next.id
                && previous.required == next.required
                && types_share_field_identities(&previous.field_type, &next.field_type)
        })
}

fn types_share_field_identities(previous: &Type, next: &Type) -> bool {
    match (previous, next) {
        (Type::Primitive(previous), Type::Primitive(next)) => previous == next,
        (Type::Struct(previous), Type::Struct(next)) => {
            struct_types_share_field_identities(previous, next)
        }
        (Type::List(previous), Type::List(next)) => {
            previous.element_field.id == next.element_field.id
                && previous.element_field.required == next.element_field.required
                && types_share_field_identities(
                    &previous.element_field.field_type,
                    &next.element_field.field_type,
                )
        }
        (Type::Map(previous), Type::Map(next)) => {
            previous.key_field.id == next.key_field.id
                && previous.value_field.id == next.value_field.id
                && previous.value_field.required == next.value_field.required
                && types_share_field_identities(
                    &previous.key_field.field_type,
                    &next.key_field.field_type,
                )
                && types_share_field_identities(
                    &previous.value_field.field_type,
                    &next.value_field.field_type,
                )
        }
        (Type::Primitive(_) | Type::Struct(_) | Type::List(_) | Type::Map(_), _) => false,
    }
}

/// The partition type of every spec the relation carries, keyed by spec id.
///
/// Enumeration needs these to encode a data file's frozen partition values;
/// the change-window handle itself carries no spec, so they are resolved from
/// the same metadata the endpoints were pinned from.
fn change_window_partition_types(
    metadata: &TableMetadata,
    schema: &Schema,
) -> Result<BTreeMap<i32, Type>, ConnectorError> {
    let mut partition_types = BTreeMap::new();
    for spec in metadata.partition_specs_iter() {
        let partition_type = spec.partition_type(schema).map_err(|error| {
            corrupt(format!(
                "iceberg partition spec {} does not bind to the change window's frozen schema: {error}",
                spec.spec_id()
            ))
        })?;
        partition_types.insert(spec.spec_id(), Type::Struct(partition_type));
    }
    Ok(partition_types)
}

/// The columns a change-window relation exposes.
///
/// They are the frozen relation's own fields plus `__change_op`. The sign is
/// visible rather than hidden because it is the point of the relation: a change
/// row without it says a row differs between the endpoints but not in which
/// direction. It is also the one column no file can supply -- the split variant
/// is its only source -- so it is appended, never bound to a table field.
fn change_window_column_bindings(
    handle: &IcebergChangeWindowHandle,
) -> Result<Vec<TypedColumnBinding>, ConnectorError> {
    let schema = handle.parse_table_schema()?;
    let fields = schema.as_struct().fields();
    let mut bindings = Vec::with_capacity(fields.len() + 1);
    for field in fields {
        if field.name == ICEBERG_CHANGE_OP_COLUMN {
            // Two columns of the same name would make the relation ambiguous,
            // and silently renaming either one would hide a real collision.
            return Err(unsupported(format!(
                "iceberg relation {}.{} has a field named {ICEBERG_CHANGE_OP_COLUMN}, which a change window reserves for its sign",
                handle.schema_table_name().schema_name(),
                handle.schema_table_name().table_name()
            )));
        }
        let column = IcebergColumnHandle::base_column(field.as_ref())?;
        bindings.push(TypedColumnBinding::new(
            &field.name,
            iceberg_column_to_wire(&column)?,
            false,
        ));
    }
    bindings.push(TypedColumnBinding::new(
        ICEBERG_CHANGE_OP_COLUMN,
        iceberg_column_to_wire(&change_op_column_handle()?)?,
        false,
    ));
    Ok(bindings)
}

/// Keep only table properties provably safe to hand a worker.
fn reader_visible_storage_properties(
    properties: &HashMap<String, String>,
) -> BTreeMap<String, String> {
    READER_VISIBLE_TABLE_PROPERTIES
        .iter()
        .filter_map(|key| {
            properties
                .get(*key)
                .map(|value| ((*key).to_string(), value.clone()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Column bindings
// ---------------------------------------------------------------------------

/// The hidden Iceberg metadata columns, paired with their SQL-visible names.
///
/// Each one is addressable by name but never part of `SELECT *`. The SQL name
/// is the `$`-prefixed spelling; the identity behind it is the Iceberg reserved
/// field, so a rename of a real table column can never collide with one.
fn metadata_pseudo_columns(
    handle: &IcebergTableHandle,
    schema: &Schema,
) -> Result<Vec<(&'static str, IcebergColumnHandle)>, ConnectorError> {
    let spec_id = handle.spec_id().ok_or_else(|| {
        corrupt("iceberg table handle carries no default partition spec for $partition")
    })?;
    let partition_type = handle
        .parse_partition_spec(spec_id)?
        .partition_type(schema)
        .map_err(|error| {
            corrupt(format!(
                "iceberg partition spec {spec_id} does not bind to the frozen table schema: {error}"
            ))
        })?;

    Ok(vec![
        (
            PSEUDO_COLUMN_PATH,
            pseudo_column(
                RESERVED_FIELD_ID_FILE_PATH,
                ICEBERG_FILE_PATH_COL,
                Type::Primitive(PrimitiveType::String),
            )?,
        ),
        (
            PSEUDO_COLUMN_PARTITION,
            pseudo_column(
                RESERVED_FIELD_ID_PARTITION,
                ICEBERG_PARTITION_COL,
                Type::Struct(partition_type),
            )?,
        ),
        (
            PSEUDO_COLUMN_FILE_MODIFIED_TIME,
            pseudo_column(
                RESERVED_FIELD_ID_FILE_MODIFIED_TIME,
                ICEBERG_FILE_MODIFIED_TIME_COL,
                Type::Primitive(PrimitiveType::Timestamptz),
            )?,
        ),
        (
            PSEUDO_COLUMN_ROW_ID,
            pseudo_column(
                ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                ICEBERG_ROW_ID_COL,
                Type::Primitive(PrimitiveType::Long),
            )?,
        ),
        (
            PSEUDO_COLUMN_LAST_UPDATED_SEQUENCE_NUMBER,
            pseudo_column(
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                ICEBERG_LAST_UPDATED_SEQ_COL,
                Type::Primitive(PrimitiveType::Long),
            )?,
        ),
    ])
}

/// A metadata column is always optional: a v2 table has no row lineage, and a
/// synthesized column is absent rather than wrong when the fact is unavailable.
fn pseudo_column(
    field_id: i32,
    name: &str,
    field_type: Type,
) -> Result<IcebergColumnHandle, ConnectorError> {
    IcebergColumnHandle::base_column(&NestedField::optional(field_id, name, field_type))
}

// ---------------------------------------------------------------------------
// Wire <-> concrete conversions
// ---------------------------------------------------------------------------

fn wire_column_to_iceberg(
    column: &ValidatedColumnHandle,
) -> Result<IcebergColumnHandle, ConnectorError> {
    IcebergColumnHandle::from_column_handle_proto(column.as_proto())
}

fn iceberg_column_to_wire(
    column: &IcebergColumnHandle,
) -> Result<ValidatedColumnHandle, ConnectorError> {
    ValidatedColumnHandle::parse(
        column.to_column_handle_proto(),
        FieldPath::root("column_handle"),
    )
    .map_err(from_protocol)
}

fn wire_domain_to_iceberg(
    domain: &TupleDomain<ValidatedColumnHandle>,
) -> Result<TupleDomain<IcebergColumnHandle>, ConnectorError> {
    decode_iceberg_tuple_domain(&encode_wire_tuple_domain(domain))
}

fn iceberg_domain_to_wire(
    domain: &TupleDomain<IcebergColumnHandle>,
) -> Result<TupleDomain<ValidatedColumnHandle>, ConnectorError> {
    decode_wire_tuple_domain(
        &encode_iceberg_tuple_domain(domain),
        FieldPath::root("tuple_domain"),
    )
    .map_err(from_protocol)
}

fn wire_constraint_to_iceberg(
    constraint: &WireConstraint,
) -> Result<Constraint<IcebergColumnHandle>, ConnectorError> {
    let mut assignments = BTreeMap::new();
    for (variable, column) in constraint.assignments() {
        assignments.insert(Arc::clone(variable), wire_column_to_iceberg(column)?);
    }
    Constraint::try_new(
        wire_domain_to_iceberg(constraint.summary())?,
        constraint.expression().clone(),
        assignments,
    )
}

fn ordered_assignments(
    assignments: &[ScanAssignment],
) -> Result<OrderedAssignments<IcebergColumnHandle>, ConnectorError> {
    let mut ordered = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        ordered.push(Assignment::try_new(
            assignment.variable(),
            wire_column_to_iceberg(assignment.column())?,
            assignment.value_type(),
        )?);
    }
    OrderedAssignments::try_new(ordered)
}

// ---------------------------------------------------------------------------
// System relation naming
// ---------------------------------------------------------------------------

/// Split `<table>$<suffix>` into its base relation and system relation.
///
/// Returns `None` for a name with no `$` and for an unknown suffix; both are
/// "this is not one of my system relations", never a guess.
fn system_relation_of(table_name: &str) -> Option<(String, IcebergSystemRelation)> {
    let (base_table, suffix) = table_name.rsplit_once('$')?;
    if base_table.is_empty() {
        return None;
    }
    let relation = match suffix.trim().to_ascii_uppercase().as_str() {
        "FILES" => IcebergSystemRelation::Files,
        "ENTRIES" => IcebergSystemRelation::Entries,
        "SNAPSHOTS" => IcebergSystemRelation::Snapshots,
        "HISTORY" => IcebergSystemRelation::History,
        "REFS" => IcebergSystemRelation::Refs,
        "MANIFESTS" => IcebergSystemRelation::Manifests,
        "PARTITIONS" => IcebergSystemRelation::Partitions,
        _ => return None,
    };
    Some((base_table.to_string(), relation))
}

// ---------------------------------------------------------------------------
// Manifest facts for split production
// ---------------------------------------------------------------------------

/// Manifest facts about one data file that the read view does not carry.
#[derive(Clone, Debug)]
struct DataFileManifestFacts {
    file_format: DataFileFormat,
    split_offsets: Vec<i64>,
    key_metadata: Vec<u8>,
}

/// Everything one manifest walk contributes beyond the read view.
#[derive(Debug, Default)]
struct ManifestFacts {
    data: HashMap<String, DataFileManifestFacts>,
    deletes: HashMap<String, IcebergDeleteFileFacts>,
}

/// Build the read view of one pinned snapshot together with its manifest facts.
///
/// The two walks are kept in one call so a single pass over the manifest list
/// serves both, and so neither half can silently describe a different snapshot.
async fn plan_pinned_snapshot(
    table: Table,
    snapshot_id: i64,
) -> Result<(IcebergReadSnapshot, ManifestFacts), String> {
    let read_snapshot = crate::read_snapshot::build_read_snapshot_at(&table, snapshot_id).await?;
    let facts = collect_manifest_facts(&table, snapshot_id).await?;
    Ok((read_snapshot, facts))
}

async fn collect_manifest_facts(table: &Table, snapshot_id: i64) -> Result<ManifestFacts, String> {
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("iceberg snapshot {snapshot_id} is absent from table metadata"))?;
    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|error| format!("load manifest list: {error}"))?;

    let mut facts = ManifestFacts::default();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|error| format!("load manifest: {error}"))?;
        for entry in manifest.entries() {
            if entry.status == ManifestStatus::Deleted {
                continue;
            }
            let data_file = entry.data_file();
            match data_file.content_type() {
                DataContentType::Data => {
                    facts.data.insert(
                        data_file.file_path().to_string(),
                        DataFileManifestFacts {
                            file_format: data_file.file_format(),
                            split_offsets: data_file
                                .split_offsets()
                                .map(<[i64]>::to_vec)
                                .unwrap_or_default(),
                            key_metadata: data_file
                                .key_metadata()
                                .map(<[u8]>::to_vec)
                                .unwrap_or_default(),
                        },
                    );
                }
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    // A delete file's encryption material has nowhere to go in
                    // the split contract, so an encrypted one must fail here
                    // rather than travel with its key silently dropped.
                    if data_file.key_metadata().is_some_and(|key| !key.is_empty()) {
                        return Err(format!(
                            "iceberg encrypted delete file {} is not supported by the connector read stack",
                            data_file.file_path()
                        ));
                    }
                    facts.deletes.insert(
                        data_file.file_path().to_string(),
                        IcebergDeleteFileFacts {
                            record_count: i64::try_from(data_file.record_count()).map_err(
                                |_| {
                                    format!(
                                        "iceberg delete file {} declares an unrepresentable record count",
                                        data_file.file_path()
                                    )
                                },
                            )?,
                            row_position_lower_bound: row_position_bound(data_file.lower_bounds()),
                            row_position_upper_bound: row_position_bound(data_file.upper_bounds()),
                            decryption_data: None,
                        },
                    );
                }
            }
        }
    }
    Ok(facts)
}

/// The row-position bound a position-delete file publishes for `pos`.
fn row_position_bound(bounds: &HashMap<i32, Datum>) -> Option<i64> {
    match bounds.get(&RESERVED_FIELD_ID_DELETE_FILE_POS)?.literal() {
        PrimitiveLiteral::Long(value) => Some(*value),
        // Any other physical literal is not a row position; treating it as one
        // would hand the reader a bound it cannot honor.
        PrimitiveLiteral::Boolean(_)
        | PrimitiveLiteral::Int(_)
        | PrimitiveLiteral::Float(_)
        | PrimitiveLiteral::Double(_)
        | PrimitiveLiteral::String(_)
        | PrimitiveLiteral::Binary(_)
        | PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => None,
    }
}

// ---------------------------------------------------------------------------
// Static file pruning
// ---------------------------------------------------------------------------

/// The manifest-only view `file_pruning` judges.
///
/// Only the facts pruning reads are filled in: identity partition values and
/// column statistics. Everything else stays absent so no consumer can mistake
/// this for a planning record.
fn pruning_view(
    handle: &IcebergTableHandle,
    schema: &Schema,
    read_file: &IcebergReadFile,
) -> Result<IcebergDataFileInfo, ConnectorError> {
    let partition_values = match (
        read_file.partition_spec_id,
        read_file.partition_values.as_ref(),
    ) {
        (Some(spec_id), Some(values)) => {
            let spec = handle.parse_partition_spec(spec_id)?;
            let mut decoded = Vec::with_capacity(spec.fields().len());
            for (index, field) in spec.fields().iter().enumerate() {
                let source_column = schema
                    .field_by_id(field.source_id)
                    .map(|source| source.name.clone())
                    .unwrap_or_else(|| format!("#{}", field.source_id));
                decoded.push(IcebergPartitionFieldValue {
                    source_column,
                    field_name: field.name.clone(),
                    transform: partition_transform_name(&field.transform),
                    value: values
                        .fields()
                        .get(index)
                        .and_then(|literal| literal.as_ref())
                        .and_then(partition_value_of),
                });
            }
            decoded
        }
        // Without both a spec and its values, identity-partition pruning cannot
        // judge anything; statistics pruning still applies.
        _ => Vec::new(),
    };

    Ok(IcebergDataFileInfo {
        path: read_file.path.clone(),
        size: read_file.size,
        row_count: read_file.record_count,
        column_stats: read_file.column_stats.clone(),
        partition_spec_id: read_file.partition_spec_id,
        partition_key: read_file.partition_key.clone(),
        partition_values,
        // Nothing below is read by pruning. Each stays absent so this value
        // cannot be mistaken for a planning record of the file.
        first_row_id: None,
        data_sequence_number: None,
        ivm_change_op: None,
        included_positions: None,
        delete_files: Vec::new(),
        manifest_path: None,
    })
}

fn partition_transform_name(transform: &crate::iceberg::spec::Transform) -> String {
    match transform {
        crate::iceberg::spec::Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn partition_value_of(literal: &Literal) -> Option<IcebergPartitionValue> {
    let Literal::Primitive(value) = literal else {
        return None;
    };
    match value {
        PrimitiveLiteral::Boolean(value) => Some(IcebergPartitionValue::Boolean(*value)),
        PrimitiveLiteral::Int(value) => Some(IcebergPartitionValue::Int32(*value)),
        PrimitiveLiteral::Long(value) => Some(IcebergPartitionValue::Int64(*value)),
        PrimitiveLiteral::Float(value) => Some(IcebergPartitionValue::Float(value.0)),
        PrimitiveLiteral::Double(value) => Some(IcebergPartitionValue::Double(value.0)),
        PrimitiveLiteral::String(value) => Some(IcebergPartitionValue::String(value.clone())),
        PrimitiveLiteral::Binary(value) => Some(IcebergPartitionValue::Binary(value.clone())),
        // A decimal, a sentinel, or an unsigned 128-bit literal has no
        // comparable projection here, and "cannot judge" must keep the file.
        PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => None,
    }
}

/// Project a frozen predicate onto the physical predicates `file_pruning` reads.
///
/// Pruning must be sound, so every column this cannot express contributes
/// nothing and the file is kept. Two conditions in particular bar a column:
/// a nested or non-primitive column has no manifest statistics of its own, and
/// a domain that admits NULL cannot be judged from bounds -- a NULL partition
/// value would satisfy it while comparing as no value at all.
fn physical_predicates(
    predicate: &TupleDomain<IcebergColumnHandle>,
    schema: &Schema,
) -> Vec<IcebergPhysicalPredicate> {
    let Some(domains) = predicate.domains() else {
        return Vec::new();
    };
    let mut predicates = Vec::new();
    for (column, domain) in domains {
        if !column.is_base_column() || domain.null_allowed() {
            continue;
        }
        let Some(field) = schema.field_by_id(column.base_field_id()) else {
            continue;
        };
        if !matches!(field.field_type.as_ref(), Type::Primitive(_)) {
            continue;
        }
        for physical_domain in physical_domains(domain) {
            predicates.push(IcebergPhysicalPredicate {
                field_id: column.base_field_id(),
                column: field.name.clone(),
                domain: physical_domain,
            });
        }
    }
    predicates
}

/// The conjunction of physical domains one column domain implies.
fn physical_domains(domain: &Domain) -> Vec<IcebergPhysicalPredicateDomain> {
    if let Some(values) = domain.values().discrete_values() {
        let mut discrete = Vec::with_capacity(values.len());
        for value in values {
            let Some(value) = physical_value(value) else {
                return Vec::new();
            };
            discrete.push(value);
        }
        if discrete.is_empty() {
            return Vec::new();
        }
        return vec![IcebergPhysicalPredicateDomain::DiscreteSet { values: discrete }];
    }

    let ranges = domain.values().ranges();
    // A union of two or more ranges is not a conjunction of bounds, so nothing
    // sound can be emitted from it without widening it into something weaker.
    let [range] = ranges else {
        return Vec::new();
    };
    let mut result = Vec::with_capacity(2);
    for (bound, inclusive_op, exclusive_op) in [
        (
            range.low(),
            IcebergPhysicalPredicateOp::Ge,
            IcebergPhysicalPredicateOp::Gt,
        ),
        (
            range.high(),
            IcebergPhysicalPredicateOp::Le,
            IcebergPhysicalPredicateOp::Lt,
        ),
    ] {
        let (op, value) = match bound {
            Bound::Unbounded => continue,
            Bound::Inclusive(value) => (inclusive_op, value),
            Bound::Exclusive(value) => (exclusive_op, value),
        };
        let Some(value) = physical_value(value) else {
            continue;
        };
        result.push(IcebergPhysicalPredicateDomain::Range { op, value });
    }
    result
}

/// Project one typed value onto the literal domain manifest pruning compares in.
///
/// ADR-0018 limits that domain to boolean, int32, int64 and date; every other
/// type is "cannot judge", which keeps the file.
fn physical_value(value: &ConnectorValue) -> Option<IcebergPhysicalPredicateValue> {
    match value {
        ConnectorValue::Boolean(value) => Some(IcebergPhysicalPredicateValue::Boolean(*value)),
        ConnectorValue::Integer(value) => Some(IcebergPhysicalPredicateValue::Int32(*value)),
        ConnectorValue::BigInt(value) => Some(IcebergPhysicalPredicateValue::Int64(*value)),
        ConnectorValue::Date(value) => Some(IcebergPhysicalPredicateValue::Date32(*value)),
        // No Iceberg field is eight-bit, so a tiny int can only be an
        // engine-derived column, which no manifest carries statistics for.
        ConnectorValue::TinyInt(_)
        | ConnectorValue::Real(_)
        | ConnectorValue::Double(_)
        | ConnectorValue::Decimal { .. }
        | ConnectorValue::TimeMicros(_)
        | ConnectorValue::TimestampMicros(_)
        | ConnectorValue::TimestampTzMicros(_)
        | ConnectorValue::TimestampNanos(_)
        | ConnectorValue::TimestampTzNanos(_)
        | ConnectorValue::Varchar(_)
        | ConnectorValue::Varbinary(_)
        | ConnectorValue::Uuid(_)
        | ConnectorValue::Fixed(_) => None,
    }
}

fn not_found(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;
    use std::sync::Arc as StdArc;
    use std::time::SystemTime;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::read_stack::{ConnectorSplitBatch, ConnectorValueType, ValueSet};
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::spec::{
        FormatVersion, Operation, PartitionSpec, Snapshot, SnapshotReference, SnapshotRetention,
        SortOrder, Summary, TableMetadataBuilder, Transform,
    };
    use crate::iceberg::{NamespaceIdent, TableCreation};
    use crate::resources::IcebergControlResources;

    use super::*;

    /// One control generation over a temporary Hadoop warehouse.
    struct Fixture {
        _warehouse: tempfile::TempDir,
        executor: tokio::runtime::Runtime,
        boundary: IcebergTypedBoundary,
        runtime: Arc<IcebergControlRuntime>,
    }

    fn fixture() -> Fixture {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            StdArc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            StdArc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let boundary = IcebergTypedBoundary::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            },
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
            HiveTransactionHandle::new(true, [3; 16]),
            Arc::clone(&runtime),
        );
        Fixture {
            _warehouse: warehouse,
            executor,
            boundary,
            runtime,
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
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
                StdArc::new(NestedField::optional(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("schema")
    }

    impl Fixture {
        fn create_table(
            &self,
            namespace: &str,
            table: &str,
            properties: StdHashMap<String, String>,
        ) {
            let catalog = Arc::clone(self.runtime.catalog());
            let namespace_name = namespace.to_string();
            let table_name = table.to_string();
            self.executor.block_on(async move {
                let namespace = NamespaceIdent::new(namespace_name);
                if !catalog
                    .namespace_exists(&namespace)
                    .await
                    .expect("namespace exists")
                {
                    catalog
                        .create_namespace(&namespace, StdHashMap::new())
                        .await
                        .expect("create namespace");
                }
                catalog
                    .create_table(
                        &namespace,
                        TableCreation::builder()
                            .name(table_name)
                            .schema(table_schema())
                            .format_version(FormatVersion::V2)
                            .properties(properties)
                            .build(),
                    )
                    .await
                    .expect("create table");
            });
        }
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q1", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn name(schema: &str, table: &str) -> SchemaTableName {
        SchemaTableName::try_new(schema, table).expect("schema table name")
    }

    /// Pure metadata with two snapshots, a tag, and a credential-shaped
    /// property, built without touching a catalog.
    fn metadata_with_history() -> TableMetadata {
        let schema = table_schema();
        let spec = PartitionSpec::builder(StdArc::new(schema.clone()))
            .with_spec_id(0)
            .add_partition_field("region", "region", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let properties = StdHashMap::from([
            ("read.split.target-size".to_string(), "1048576".to_string()),
            (
                "s3.secret-access-key".to_string(),
                "super-secret".to_string(),
            ),
            ("write.format.default".to_string(), "parquet".to_string()),
        ]);
        let mut builder = TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            "file:///typed-boundary-test".to_string(),
            FormatVersion::V2,
            properties,
        )
        .expect("metadata builder");
        // 12 descends from 11: the two are points on one history, which is what
        // lets a change window treat them as endpoints of the same window.
        for (snapshot_id, parent_snapshot_id) in [(11_i64, None), (12, Some(11_i64))] {
            builder = builder
                .add_snapshot(
                    Snapshot::builder()
                        .with_snapshot_id(snapshot_id)
                        .with_parent_snapshot_id(parent_snapshot_id)
                        .with_sequence_number(snapshot_id)
                        .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
                        .with_manifest_list(format!(
                            "file:///typed-boundary-test/metadata/snap-{snapshot_id}.avro"
                        ))
                        .with_summary(Summary {
                            operation: Operation::Append,
                            additional_properties: StdHashMap::new(),
                        })
                        .with_schema_id(0)
                        .build(),
                )
                .expect("add snapshot");
        }
        builder = builder
            .set_ref(
                "history_tag",
                SnapshotReference::new(
                    11,
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                ),
            )
            .expect("set tag");
        builder
            .set_ref(
                "main",
                SnapshotReference::new(12, SnapshotRetention::branch(None, None, None)),
            )
            .expect("set main")
            .build()
            .expect("metadata")
            .metadata
    }

    fn long_domain(value: i64) -> Domain {
        Domain::new(
            ValueSet::of_values(
                ConnectorValueType::BigInt,
                vec![ConnectorValue::BigInt(value)],
            )
            .expect("value set"),
            false,
        )
    }

    fn string_domain(value: &str) -> Domain {
        Domain::new(
            ValueSet::of_values(
                ConnectorValueType::Varchar,
                vec![ConnectorValue::Varchar(StdArc::from(value))],
            )
            .expect("value set"),
            false,
        )
    }

    #[test]
    fn a_table_handle_round_trips_through_the_wire_carrier() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());

        let wrapped = fixture
            .boundary
            .get_table_handle(
                &session(),
                &name("db", "t"),
                TypedRelationVersion::Current,
                None,
            )
            .expect("get table handle")
            .expect("relation exists");

        assert_eq!(wrapped.catalog_name(), "ice");
        assert_eq!(wrapped.instance_incarnation(), [7; 16]);

        // Re-parsing the same bytes must yield the same carrier, and the
        // carrier must yield back the same concrete handle.
        let reparsed = CatalogTableHandle::parse(
            wrapped.as_proto().clone(),
            FieldPath::root("catalog_table_handle"),
        )
        .expect("reparse");
        assert_eq!(reparsed, wrapped);

        let concrete = fixture
            .boundary
            .data_table_handle(&wrapped)
            .expect("concrete handle");
        assert_eq!(concrete.schema_table_name().schema_name(), "db");
        assert_eq!(concrete.schema_table_name().table_name(), "t");
        assert_eq!(concrete.format_version(), 2);
        // A freshly created table has no snapshot: that reads zero rows, it is
        // not an invitation to resolve a later one.
        assert_eq!(concrete.snapshot_id(), None);
        assert!(concrete.parse_table_schema().is_ok());
    }

    #[test]
    fn a_missing_relation_is_absent_rather_than_an_error() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());

        assert!(
            fixture
                .boundary
                .get_table_handle(
                    &session(),
                    &name("db", "absent"),
                    TypedRelationVersion::Current,
                    None,
                )
                .expect("get table handle")
                .is_none()
        );
    }

    #[test]
    fn a_data_relation_never_answers_a_system_relation_name() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());

        assert!(
            fixture
                .boundary
                .get_table_handle(
                    &session(),
                    &name("db", "t$files"),
                    TypedRelationVersion::Current,
                    None,
                )
                .expect("get table handle")
                .is_none()
        );
    }

    #[test]
    fn a_snapshot_id_and_a_named_reference_both_pin_without_a_second_resolution() {
        let metadata = metadata_with_history();
        assert_eq!(metadata.current_snapshot_id(), Some(12));

        // Time travel pins the requested snapshot, not the current one.
        assert_eq!(
            pin_snapshot(&metadata, TypedRelationVersion::SnapshotId(11), None).expect("pin"),
            Some(11)
        );
        // The connector resolves the reference; the engine never sees the name
        // again, so nothing downstream can resolve it differently.
        assert_eq!(
            pin_snapshot(
                &metadata,
                TypedRelationVersion::Reference,
                Some("history_tag"),
            )
            .expect("pin"),
            Some(11)
        );
        assert_eq!(
            pin_snapshot(&metadata, TypedRelationVersion::Current, None).expect("pin"),
            Some(12)
        );

        // The pinned answer is what the handle carries, and it survives the
        // wire round trip unchanged.
        let handle =
            pinned_table_handle(&name("db", "t"), &metadata, Some(11)).expect("pinned handle");
        assert_eq!(handle.snapshot_id(), Some(11));
        let decoded = IcebergTableHandle::from_table_handle_proto(&handle.to_table_handle_proto())
            .expect("decode");
        assert_eq!(decoded.snapshot_id(), Some(11));

        // Absent snapshots and inconsistent version requests fail closed.
        assert_eq!(
            pin_snapshot(&metadata, TypedRelationVersion::SnapshotId(99), None)
                .expect_err("absent snapshot")
                .kind(),
            ConnectorErrorKind::NotFound
        );
        assert!(pin_snapshot(&metadata, TypedRelationVersion::Reference, None).is_err());
        assert!(
            pin_snapshot(
                &metadata,
                TypedRelationVersion::Current,
                Some("history_tag")
            )
            .is_err()
        );
        assert!(pin_snapshot(&metadata, TypedRelationVersion::Reference, Some("absent")).is_err());
    }

    #[test]
    fn a_credential_shaped_storage_property_never_reaches_the_handle() {
        let metadata = metadata_with_history();
        let handle =
            pinned_table_handle(&name("db", "t"), &metadata, Some(12)).expect("pinned handle");

        assert_eq!(
            handle.storage_properties().get("read.split.target-size"),
            Some(&"1048576".to_string())
        );
        assert!(
            !handle
                .storage_properties()
                .contains_key("s3.secret-access-key")
        );
        // A property that is merely non-secret is still dropped unless the
        // reader is known to need it.
        assert!(
            !handle
                .storage_properties()
                .contains_key("write.format.default")
        );
        assert!(
            handle
                .storage_properties()
                .keys()
                .all(|key| READER_VISIBLE_TABLE_PROPERTIES.contains(&key.as_str()))
        );
    }

    #[test]
    fn column_bindings_follow_schema_order_and_hide_the_metadata_columns() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());
        let wrapped = fixture
            .boundary
            .get_table_handle(
                &session(),
                &name("db", "t"),
                TypedRelationVersion::Current,
                None,
            )
            .expect("get table handle")
            .expect("relation exists");

        let bindings = fixture
            .boundary
            .get_column_bindings(&session(), &wrapped)
            .expect("column bindings");

        let visible = bindings
            .iter()
            .filter(|binding| !binding.is_hidden())
            .map(TypedColumnBinding::name)
            .collect::<Vec<_>>();
        assert_eq!(visible, vec!["id", "region", "amount"]);

        let hidden = bindings
            .iter()
            .filter(|binding| binding.is_hidden())
            .map(TypedColumnBinding::name)
            .collect::<Vec<_>>();
        assert_eq!(
            hidden,
            vec![
                PSEUDO_COLUMN_PATH,
                PSEUDO_COLUMN_PARTITION,
                PSEUDO_COLUMN_FILE_MODIFIED_TIME,
                PSEUDO_COLUMN_ROW_ID,
                PSEUDO_COLUMN_LAST_UPDATED_SEQUENCE_NUMBER,
            ]
        );

        // Every binding is a usable predicate key, and the pseudo-columns keep
        // their Iceberg reserved field IDs rather than borrowing a table field.
        let path = wire_column_to_iceberg(
            bindings
                .iter()
                .find(|binding| binding.name() == PSEUDO_COLUMN_PATH)
                .expect("path binding")
                .column(),
        )
        .expect("concrete column");
        assert_eq!(path.base_field_id(), RESERVED_FIELD_ID_FILE_PATH);
        assert_eq!(path.base_column_identity().name(), ICEBERG_FILE_PATH_COL);
    }

    #[test]
    fn filter_pushdown_splits_enforced_from_unenforced_and_keeps_the_residual() {
        let fixture = fixture();
        let metadata = metadata_with_history();
        let handle =
            pinned_table_handle(&name("db", "t"), &metadata, Some(12)).expect("pinned handle");
        let wrapped = fixture.boundary.wrap_table(handle).expect("wrap");

        let schema = table_schema();
        let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region");
        let amount = IcebergColumnHandle::base_column_of(&schema, 3).expect("amount");
        let summary = TupleDomain::with_column_domains(BTreeMap::from([
            (
                iceberg_column_to_wire(&region).expect("wire region"),
                string_domain("emea"),
            ),
            (
                iceberg_column_to_wire(&amount).expect("wire amount"),
                long_domain(5),
            ),
        ]))
        .expect("summary");

        let applied = fixture
            .boundary
            .apply_filter(&session(), &wrapped, &Constraint::of_summary(summary))
            .expect("apply filter")
            .expect("something was accepted");

        let pushed = fixture
            .boundary
            .data_table_handle(applied.handle())
            .expect("pushed handle");
        // `region` is an identity partition column, so planning enforces it.
        assert_eq!(
            pushed.enforced_predicate().columns().collect::<Vec<_>>(),
            vec![&region]
        );
        assert_eq!(
            pushed.unenforced_predicate().columns().collect::<Vec<_>>(),
            vec![&amount]
        );
        // The unenforced half survives as the engine's residual.
        let residual = applied
            .remaining_constraint()
            .summary()
            .columns()
            .map(|column| wire_column_to_iceberg(column).expect("concrete"))
            .collect::<Vec<_>>();
        assert_eq!(residual, vec![amount]);
        assert!(applied.remaining_expression().is_none());

        // Offering the very same filter again accepts nothing.
        let summary = TupleDomain::with_column_domains(BTreeMap::from([(
            iceberg_column_to_wire(&region).expect("wire region"),
            string_domain("emea"),
        )]))
        .expect("summary");
        assert!(
            fixture
                .boundary
                .apply_filter(
                    &session(),
                    applied.handle(),
                    &Constraint::of_summary(summary)
                )
                .expect("apply filter")
                .is_none()
        );
    }

    #[test]
    fn a_limit_iceberg_cannot_guarantee_reports_that_it_cannot() {
        let fixture = fixture();
        let metadata = metadata_with_history();
        let handle =
            pinned_table_handle(&name("db", "t"), &metadata, Some(12)).expect("pinned handle");
        let wrapped = fixture.boundary.wrap_table(handle).expect("wrap");

        let applied = fixture
            .boundary
            .apply_limit(&session(), &wrapped, 10)
            .expect("apply limit")
            .expect("limit accepted");
        // Deletes are applied per split, so no split knows how many rows its
        // siblings produced: the engine must keep its own limit operator.
        assert!(!applied.limit_guaranteed());
        assert_eq!(
            fixture
                .boundary
                .data_table_handle(applied.handle())
                .expect("handle")
                .limit(),
            Some(10)
        );

        // The zero-row bound is the one Iceberg can guarantee.
        let zero = fixture
            .boundary
            .apply_limit(&session(), &wrapped, 0)
            .expect("apply limit")
            .expect("limit accepted");
        assert!(zero.limit_guaranteed());

        // A wider limit narrows to nothing new, so nothing is accepted.
        assert!(
            fixture
                .boundary
                .apply_limit(&session(), applied.handle(), 100)
                .expect("apply limit")
                .is_none()
        );
    }

    #[test]
    fn every_system_table_suffix_maps_to_its_own_distribution() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());

        let expected = [
            (
                "t$files",
                SystemTableDistribution::AllNodes,
                dto::IcebergSystemTableType::Files,
            ),
            (
                "t$entries",
                SystemTableDistribution::SingleCoordinator,
                dto::IcebergSystemTableType::Entries,
            ),
            (
                "t$snapshots",
                SystemTableDistribution::SingleCoordinator,
                dto::IcebergSystemTableType::Snapshots,
            ),
            (
                "t$history",
                SystemTableDistribution::SingleCoordinator,
                dto::IcebergSystemTableType::History,
            ),
            (
                "t$refs",
                SystemTableDistribution::SingleCoordinator,
                dto::IcebergSystemTableType::Refs,
            ),
            (
                "t$manifests",
                SystemTableDistribution::SingleCoordinator,
                dto::IcebergSystemTableType::Manifests,
            ),
        ];
        for (table, distribution, system_table_type) in expected {
            let plan = fixture
                .boundary
                .get_system_table_plan(&session(), &name("db", table))
                .expect("system table plan")
                .unwrap_or_else(|| panic!("{table} has a plan"));
            assert_eq!(plan.distribution(), distribution, "{table}");
            match plan.handle().relation() {
                ConnectorRelation::SystemTable(reference) => {
                    // An irrefutable binding, not a widened match: adding a
                    // second reference variant makes this pattern refutable and
                    // therefore a compile error here.
                    let dto::connector_system_table_reference::Reference::Iceberg(iceberg) =
                        reference.reference.as_ref().expect("variant");
                    assert_eq!(
                        iceberg.system_table_type, system_table_type as i32,
                        "{table}"
                    );
                    // The reference names the base relation, and freezes the
                    // exact metadata file it was resolved from.
                    assert_eq!(
                        iceberg.schema_table_name.as_ref().expect("name").table_name,
                        "t"
                    );
                    assert!(!iceberg.metadata_file_location.is_empty());
                    assert_eq!(iceberg.table_uuid.len(), 36);
                }
                ConnectorRelation::Table(_)
                | ConnectorRelation::TableFunction(_)
                | ConnectorRelation::ChangeWindow(_)
                | ConnectorRelation::TableExecute(_)
                | ConnectorRelation::MergeTable(_) => {
                    panic!("{table} must resolve to a system relation")
                }
            }
        }
    }

    #[test]
    fn partitions_binds_to_the_same_pinned_files_plan() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());

        let files = fixture
            .boundary
            .get_system_table_plan(&session(), &name("db", "t$files"))
            .expect("files plan")
            .expect("files plan exists");
        let partitions = fixture
            .boundary
            .get_system_table_plan(&session(), &name("db", "t$partitions"))
            .expect("partitions plan")
            .expect("partitions plan exists");

        // `$partitions` is an aggregation over `$files` of the same pinned
        // snapshot, so it must be the very same reference rather than a second
        // resolution of the same relation.
        assert_eq!(partitions.distribution(), SystemTableDistribution::AllNodes);
        assert_eq!(partitions.handle(), files.handle());
    }

    /// Not accepting a pushdown is always a legal answer; refusing one is not.
    /// A relation that has no data table handle to push into must report that
    /// it accepted nothing, or every scan of it fails before it can read.
    #[test]
    fn a_relation_with_no_pushdown_accepts_nothing_instead_of_refusing() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());
        let system_table = fixture
            .boundary
            .get_system_table_plan(&session(), &name("db", "t$snapshots"))
            .expect("snapshots plan")
            .expect("snapshots plan exists")
            .into_handle();

        assert!(
            fixture
                .boundary
                .apply_filter(
                    &session(),
                    &system_table,
                    &Constraint::of_summary(
                        novarocks_spi::connector::read_stack::TupleDomain::all()
                    )
                )
                .expect("a system relation accepts no filter rather than refusing")
                .is_none()
        );
        assert!(
            fixture
                .boundary
                .apply_projection(&session(), &system_table, &[])
                .expect("a system relation accepts no projection rather than refusing")
                .is_none()
        );
        assert!(
            fixture
                .boundary
                .apply_limit(&session(), &system_table, 10)
                .expect("a system relation accepts no limit rather than refusing")
                .is_none()
        );
    }

    #[test]
    fn an_unknown_suffix_is_not_a_system_relation() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());

        assert!(
            fixture
                .boundary
                .get_system_table_plan(&session(), &name("db", "t$nonsense"))
                .expect("system table plan")
                .is_none()
        );
        assert!(
            fixture
                .boundary
                .get_system_table_plan(&session(), &name("db", "t"))
                .expect("system table plan")
                .is_none()
        );
        assert!(system_relation_of("$files").is_none());
    }

    #[test]
    fn a_relation_with_no_snapshot_enumerates_nothing_and_closes_idempotently() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());
        let wrapped = fixture
            .boundary
            .get_table_handle(
                &session(),
                &name("db", "t"),
                TypedRelationVersion::Current,
                None,
            )
            .expect("get table handle")
            .expect("relation exists");

        let mut source = fixture
            .boundary
            .get_splits(
                &session(),
                &wrapped,
                &[],
                &BTreeSet::new(),
                &Constraint::of_summary(TupleDomain::all()),
            )
            .expect("split source");

        let batch = source
            .next_batch(8, &WireDynamicFilterSnapshot::all_complete())
            .expect("batch");
        assert!(batch.is_empty());
        assert!(batch.no_more_splits());
        assert!(source.is_finished());
        assert!(source.close().is_ok());
        assert!(source.close().is_ok());
    }

    /// A split source whose batches are scripted, so the wrapper's own
    /// contract can be exercised without a warehouse full of Parquet files.
    struct ScriptedSplitSource {
        batches: std::collections::VecDeque<ConnectorSplitBatch<IcebergSplit>>,
        closes: usize,
    }

    impl ConnectorSplitSource for ScriptedSplitSource {
        type Split = IcebergSplit;
        type Column = IcebergColumnHandle;

        fn next_batch(
            &mut self,
            _max_size: usize,
            _dynamic_filter: &DynamicFilterSnapshot<Self::Column>,
        ) -> Result<ConnectorSplitBatch<Self::Split>, ConnectorError> {
            Ok(self
                .batches
                .pop_front()
                .unwrap_or_else(ConnectorSplitBatch::finished))
        }

        fn is_finished(&self) -> bool {
            self.batches.is_empty()
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes += 1;
            self.batches.clear();
            Ok(())
        }
    }

    fn sample_split(path: &str) -> IcebergSplit {
        use crate::typed_read::IcebergSplitParams;
        use novarocks_spi::connector::read_stack::{STANDARD_SPLIT_WEIGHT_RAW, SplitWeight};

        IcebergSplit::try_new(IcebergSplitParams {
            path: path.to_string(),
            start: 0,
            length: 128,
            file_size: 128,
            file_record_count: 10,
            file_format: IcebergFileFormat::Parquet,
            partition_spec_id: 0,
            partition_data_json: "{}".to_string(),
            deletes: Vec::new(),
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(1),
            file_first_row_id: None,
            decryption_data: None,
            split_weight: SplitWeight::try_from_raw(STANDARD_SPLIT_WEIGHT_RAW)
                .expect("standard split weight"),
            affinity_key: Some(path.to_string()),
        })
        .expect("split")
    }

    #[test]
    fn the_wrapper_validates_every_split_and_never_ends_on_an_empty_batch() {
        let mut source = IcebergTypedSplitSource::new(ScriptedSplitSource {
            batches: std::collections::VecDeque::from([
                // An empty batch means "nothing right now".
                ConnectorSplitBatch::empty(),
                ConnectorSplitBatch::new(vec![sample_split("s3://bucket/a.parquet")], false),
                ConnectorSplitBatch::new(vec![sample_split("s3://bucket/b.parquet")], true),
            ]),
            closes: 0,
        });
        let filter = WireDynamicFilterSnapshot::all_complete();

        let first = source.next_batch(8, &filter).expect("first batch");
        assert!(first.is_empty());
        assert!(!first.no_more_splits());
        assert!(!source.is_finished());

        let second = source.next_batch(8, &filter).expect("second batch");
        assert_eq!(second.splits().len(), 1);
        assert!(!second.no_more_splits());
        // Each concrete split reaches the engine as a validated wire split.
        let split = &second.splits()[0];
        assert!(split.is_remotely_accessible());
        assert_eq!(split.affinity_key(), Some("s3://bucket/a.parquet"));
        assert_eq!(
            split.category(),
            novarocks_proto::connector_read::SplitCategory::Data
        );
        assert_eq!(
            IcebergSplit::from_connector_split_proto(split.as_proto())
                .expect("decode")
                .path(),
            "s3://bucket/a.parquet"
        );

        let third = source.next_batch(8, &filter).expect("third batch");
        assert!(third.no_more_splits());

        assert!(source.close().is_ok());
        assert!(source.close().is_ok());
        // A closed source stays finished and enumerates nothing more.
        assert!(source.is_finished());
        let after_close = source.next_batch(8, &filter).expect("after close");
        assert!(after_close.is_empty());
        assert!(after_close.no_more_splits());
    }

    #[test]
    fn a_handle_from_another_catalog_generation_is_refused() {
        let fixture = fixture();
        let metadata = metadata_with_history();
        let handle =
            pinned_table_handle(&name("db", "t"), &metadata, Some(12)).expect("pinned handle");
        let wrapped = fixture.boundary.wrap_table(handle).expect("wrap");

        let other = IcebergTypedBoundary::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            },
            ConnectorInstanceIncarnation::from_bytes([9; 16]),
            HiveTransactionHandle::new(true, [3; 16]),
            Arc::clone(&fixture.runtime),
        );
        assert!(other.data_table_handle(&wrapped).is_err());
    }

    #[test]
    fn physical_predicates_only_leave_what_a_manifest_can_judge() {
        let schema = table_schema();
        let amount = IcebergColumnHandle::base_column_of(&schema, 3).expect("amount");
        let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region");

        let predicate = TupleDomain::with_column_domains(BTreeMap::from([
            (amount.clone(), long_domain(5)),
            // A varchar has no projection onto the manifest bound domain.
            (region.clone(), string_domain("emea")),
        ]))
        .expect("predicate");
        let predicates = physical_predicates(&predicate, &schema);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].field_id, 3);
        assert_eq!(predicates[0].column, "amount");

        // A NULL-admitting domain is never judged from bounds: a NULL value
        // satisfies it while comparing as no value at all.
        let nullable = TupleDomain::with_column_domains(BTreeMap::from([(
            amount,
            Domain::new(
                ValueSet::of_values(ConnectorValueType::BigInt, vec![ConnectorValue::BigInt(5)])
                    .expect("value set"),
                true,
            ),
        )]))
        .expect("predicate");
        assert!(physical_predicates(&nullable, &schema).is_empty());
    }

    #[test]
    fn a_change_window_handle_pins_both_endpoints_and_survives_the_wire() {
        let fixture = fixture();
        let metadata = metadata_with_history();
        let handle = pinned_change_window_handle(
            &name("db", "t"),
            &metadata,
            TypedChangeWindow::new(11, 12),
        )
        .expect("pinned change window")
        .expect("a v2 relation exposes change windows");

        assert_eq!(handle.from_snapshot_id_exclusive(), 11);
        assert_eq!(handle.to_snapshot_id_inclusive(), 12);
        // The relation's own fields are the window's ordered output columns;
        // the sign is not one of them, because no file carries it.
        assert_eq!(handle.columns().len(), 3);
        assert!(handle.parse_table_schema().is_ok());

        let wrapped = fixture
            .boundary
            .wrap_relation(dto::catalog_table_handle::Relation::ChangeWindow(
                handle.to_change_window_handle_proto(),
            ))
            .expect("wrap");
        assert!(matches!(
            wrapped.relation(),
            ConnectorRelation::ChangeWindow(_)
        ));
        // Re-parsing the same bytes yields the same carrier, and the carrier
        // yields back the same pinned endpoints.
        let reparsed = CatalogTableHandle::parse(
            wrapped.as_proto().clone(),
            FieldPath::root("catalog_table_handle"),
        )
        .expect("reparse");
        assert_eq!(reparsed, wrapped);
        let decoded = fixture
            .boundary
            .change_window_handle(&wrapped)
            .expect("concrete change window");
        assert_eq!(decoded, handle);
        // A data relation and a change window are different questions, so
        // neither carrier answers the other one's decoder.
        assert!(fixture.boundary.data_table_handle(&wrapped).is_err());
    }

    #[test]
    fn a_window_the_connector_cannot_serve_is_a_typed_error_rather_than_absence() {
        let fixture = fixture();
        fixture.create_table("db", "t", StdHashMap::new());

        // The relation exists but has no snapshots at all, so neither endpoint
        // can be pinned. That is a window this connector cannot serve, which is
        // not the same answer as "this relation has no change windows".
        assert_eq!(
            fixture
                .boundary
                .get_change_window_plan(
                    &session(),
                    &name("db", "t"),
                    TypedChangeWindow::new(11, 12)
                )
                .expect_err("unservable window")
                .kind(),
            ConnectorErrorKind::NotFound
        );

        // Absence stays absence: a relation that does not exist, and a system
        // relation that has no row history, both answer `None`.
        assert!(
            fixture
                .boundary
                .get_change_window_plan(
                    &session(),
                    &name("db", "absent"),
                    TypedChangeWindow::new(11, 12),
                )
                .expect("missing relation")
                .is_none()
        );
        assert!(
            fixture
                .boundary
                .get_change_window_plan(
                    &session(),
                    &name("db", "t$files"),
                    TypedChangeWindow::new(11, 12),
                )
                .expect("system relation")
                .is_none()
        );
    }

    #[test]
    fn a_v1_relation_exposes_no_change_window_at_all_rather_than_a_failed_one() {
        // v1 has no delete files, so no row of a v1 table can ever stop being
        // visible: the relation has no change windows to expose. That is a fact
        // about the relation, so it is absence -- and it is decided before the
        // endpoints are even looked at, which is why this metadata carries
        // none at all and still answers `None` instead of "snapshot missing".
        let metadata = TableMetadataBuilder::new(
            table_schema(),
            PartitionSpec::builder(StdArc::new(table_schema()))
                .with_spec_id(0)
                .build()
                .expect("partition spec")
                .into_unbound(),
            SortOrder::unsorted_order(),
            "file:///typed-boundary-v1".to_string(),
            FormatVersion::V1,
            StdHashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;

        assert!(
            pinned_change_window_handle(
                &name("db", "t"),
                &metadata,
                TypedChangeWindow::new(11, 12),
            )
            .expect("a v1 relation is absence, never an error")
            .is_none()
        );
    }

    #[test]
    fn unrelated_snapshots_and_equal_endpoints_are_never_one_change_window() {
        let metadata = metadata_with_history();

        // 11 is the parent of 12, so the window runs forwards and only
        // forwards: differencing an unrelated pair would report the distance
        // between two histories rather than one window's changes.
        assert!(
            pinned_change_window_handle(
                &name("db", "t"),
                &metadata,
                TypedChangeWindow::new(12, 11),
            )
            .is_err()
        );
        assert_eq!(
            pinned_change_window_handle(
                &name("db", "t"),
                &metadata,
                TypedChangeWindow::new(11, 99),
            )
            .expect_err("absent endpoint")
            .kind(),
            ConnectorErrorKind::NotFound
        );
        // Two equal endpoints have an empty difference by definition, so a
        // window over them is a request that means nothing.
        assert!(
            pinned_change_window_handle(
                &name("db", "t"),
                &metadata,
                TypedChangeWindow::new(12, 12),
            )
            .is_err()
        );
    }

    #[test]
    fn change_window_bindings_are_the_relation_fields_plus_the_derived_sign() {
        let fixture = fixture();
        let metadata = metadata_with_history();
        let handle = pinned_change_window_handle(
            &name("db", "t"),
            &metadata,
            TypedChangeWindow::new(11, 12),
        )
        .expect("pinned change window")
        .expect("a v2 relation exposes change windows");
        let wrapped = fixture
            .boundary
            .wrap_relation(dto::catalog_table_handle::Relation::ChangeWindow(
                handle.to_change_window_handle_proto(),
            ))
            .expect("wrap");

        let bindings = fixture
            .boundary
            .get_column_bindings(&session(), &wrapped)
            .expect("column bindings");

        // The relation's own fields in schema order, then the sign. Every one
        // is visible: a change row without its sign says that something
        // differs between the endpoints but not in which direction.
        assert_eq!(
            bindings
                .iter()
                .map(TypedColumnBinding::name)
                .collect::<Vec<_>>(),
            vec!["id", "region", "amount", ICEBERG_CHANGE_OP_COLUMN]
        );
        assert!(bindings.iter().all(|binding| !binding.is_hidden()));

        // The sign is a reserved metadata identity, never a table field: no
        // rename of a real column can collide with it, and nothing binds it to
        // a physical field of a data file.
        let sign = wire_column_to_iceberg(
            bindings
                .iter()
                .find(|binding| binding.name() == ICEBERG_CHANGE_OP_COLUMN)
                .expect("sign binding")
                .column(),
        )
        .expect("concrete column");
        assert_eq!(
            sign.base_field_id(),
            crate::typed_read::ICEBERG_CHANGE_OP_FIELD_ID
        );
        assert!(
            metadata
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .all(|field| field.id != crate::typed_read::ICEBERG_CHANGE_OP_FIELD_ID)
        );
    }
}
