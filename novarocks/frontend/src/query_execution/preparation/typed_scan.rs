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

//! Frontend lowering of one SQL scan into a typed connector scan node.
//!
//! Preparation freezes the relation, resolves every output column to a
//! connector column, offers filter/projection/limit pushdown, and builds the
//! protocol-validated scan source. It deliberately enumerates no split:
//! enumeration is lazy and owned by the execution round, so a prepared scan
//! holds only the split manager it will later be driven through.
//!
//! Ordered assignments are the sole output-order authority. A connector may
//! narrow its own handle in response to a projection, but it never reorders,
//! adds, or removes an output column.
// Design: ADR-0114 (docs/adr/ADR-0114-trino-aligned-typed-connector-read-stack.md)

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_proto::FieldPath;
use novarocks_proto::connector_read::{
    ConnectorRelationKind, ConnectorTableScanSource, ScanAssignment, TypedChangeWindow,
    TypedColumnBinding, TypedConnectorSplitManager, TypedRelationVersion, ValidatedColumnHandle,
    WireConstraint, encode_connector_expression, encode_tuple_domain, encode_value_type,
};
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{
    ConnectorSession, ConnectorValueType, SchemaTableName, TupleDomain,
};
use novarocks_sql::plan_read::PlanScanNode;

use crate::connector::typed_control_registry::TypedConnectorControl;
use crate::query_execution::connector_domain::{CatalogHandle, TableHandle, TableScanNode};

use super::scan::ResolvedScanColumn;
use super::typed_predicate::{lower_scan_predicates, scan_output_value_type};

/// Reader batch budgets for a typed scan.
///
/// The typed scan source carries its own budgets and the protocol rejects a
/// zero, so preparation names bounded defaults here rather than inventing one
/// at the wire encoder. `4096` rows matches every other frontend read budget.
const DEFAULT_MAX_BATCH_ROWS: u64 = 4096;
const DEFAULT_MAX_BATCH_BYTES: u64 = 8 * 1024 * 1024;

/// Prefix of the per-scan assignment variable. The scan output ordinal makes
/// the name unique within one scan without depending on the column's SQL name,
/// which may be an alias.
const SCAN_VARIABLE_PREFIX: &str = "v";

/// Which relation family preparation asks the connector to freeze, and the
/// exact pin that names it.
///
/// The variant decides which control entry point is called, so a lane can never
/// reach a family it did not ask for.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TypedRelationFreeze<'a> {
    /// One relation as of a point in time, optionally reached through a
    /// connector-resolved branch or tag name.
    Table {
        version: TypedRelationVersion,
        reference: Option<&'a str>,
    },
    /// The set difference between the rows visible at two snapshots.
    ///
    /// Both endpoints are pinned by the frozen handle. A row written and
    /// deleted inside the window is invisible at both endpoints and is
    /// therefore not part of the window: this is a difference of two visible
    /// row sets, never a replay of the manifests between them.
    ChangeWindow(TypedChangeWindow),
}

impl TypedRelationFreeze<'_> {
    /// The relation family this freeze must produce.
    pub(crate) const fn relation_kind(self) -> ConnectorRelationKind {
        match self {
            Self::Table { .. } => ConnectorRelationKind::Table,
            Self::ChangeWindow(_) => ConnectorRelationKind::ChangeWindow,
        }
    }
}

/// One SQL scan lowered against one installed typed connector control.
pub(crate) struct PreparedTypedScan {
    /// The fragment-plan scan node, carrying the frozen relation handle and
    /// the ordered assignments.
    pub(crate) table_scan: TableScanNode,
    /// The connector's lazy split enumerator entry point. Preparation never
    /// calls it; the execution round does.
    pub(crate) split_manager: Arc<dyn TypedConnectorSplitManager>,
    /// The constraint that was offered to the connector, kept so the round
    /// driver enumerates splits under exactly what planning pushed down.
    pub(crate) constraint: WireConstraint,
    /// Ordinals into `PlanScanNode::predicates` that have no exact domain
    /// representation at all, so the SQL side must still evaluate them.
    ///
    /// A conjunct the connector declined is not listed here: it was
    /// representable, and it travels as the scan source's
    /// `unenforced_predicate`, which the backend reader applies.
    pub(crate) residual_ordinals: Vec<usize>,
    /// Whether the connector guarantees the pushed-down limit. Only the
    /// connector's own answer sets this; an absent or declined limit leaves it
    /// false so the engine keeps its own limit operator.
    pub(crate) limit_guaranteed: bool,
}

impl std::fmt::Debug for PreparedTypedScan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The split manager is a connector-owned trait object with no debug
        // surface; everything the engine itself decided is shown.
        formatter
            .debug_struct("PreparedTypedScan")
            .field("table_scan", &self.table_scan)
            .field("constraint", &self.constraint)
            .field("residual_ordinals", &self.residual_ordinals)
            .field("limit_guaranteed", &self.limit_guaranteed)
            .finish_non_exhaustive()
    }
}

/// Lower one SQL scan into a typed connector scan node.
///
/// `physical_columns` is the scan's ordered *physical* output: the columns the
/// connector itself produces. A synthetic output — a VARIANT path column, for
/// instance — is deliberately absent, because the connector has no column for
/// it; the backend materializes those on top of the physical read slots.
///
/// `dynamic_filters` pairs a runtime-filter id with the scan output column
/// name it constrains. A name this scan does not output is an error: silently
/// dropping the binding would leave the filter's producer waiting on a
/// consumer that never applies it.
#[expect(
    clippy::too_many_arguments,
    reason = "Every argument is a distinct frozen fact of one scan; grouping them would hide which of them the connector sees."
)]
pub(crate) fn prepare_typed_scan(
    session: &ConnectorSession,
    catalog: CatalogHandle,
    control: &TypedConnectorControl,
    plan_node_id: i32,
    scan: &PlanScanNode,
    physical_columns: &[ResolvedScanColumn],
    relation: &SchemaTableName,
    freeze: TypedRelationFreeze<'_>,
    limit: Option<u64>,
    dynamic_filters: &[(u32, String)],
) -> Result<PreparedTypedScan, String> {
    let metadata = control.metadata();
    let relation_name = format!("{}.{}", relation.schema_name(), relation.table_name());

    // 1. Freeze the relation the lane asked for. Admission already resolved
    //    this name, so a connector that now reports nothing means the pin is
    //    gone rather than that the query referenced an unknown table.
    let mut handle = match freeze {
        TypedRelationFreeze::Table { version, reference } => metadata
            .get_table_handle(session, relation, version, reference)
            .map_err(|error| {
                format!("typed scan cannot freeze relation {relation_name}: {error}")
            })?
            .ok_or_else(|| {
                format!(
                    "typed scan relation {relation_name} is no longer resolvable after admission pinned it"
                )
            })?,
        TypedRelationFreeze::ChangeWindow(window) => metadata
            .get_change_window_plan(session, relation, window)
            .map_err(|error| {
                format!(
                    "typed scan cannot freeze the change window of relation {relation_name} from snapshot {} to snapshot {}: {error}",
                    window.from_snapshot_id(),
                    window.to_snapshot_id()
                )
            })?
            .ok_or_else(|| {
                format!(
                    "typed scan relation {relation_name} exposes no change window from snapshot {} to snapshot {}",
                    window.from_snapshot_id(),
                    window.to_snapshot_id()
                )
            })?,
    };

    // 2. Resolve the relation's columns.
    let column_bindings = metadata
        .get_column_bindings(session, &handle)
        .map_err(|error| {
            format!("typed scan cannot read the columns of relation {relation_name}: {error}")
        })?;

    // 3. Build the ordered assignments. `scan.columns` order is the output
    //    authority, and the connector produces exactly its physical subset, so
    //    the scan's own order is walked once and never sorted or deduplicated.
    //    Taking `physical_columns` order instead would reorder the assignments
    //    whenever a refresh lane resolved its physical columns in some other
    //    order than the plan node outputs them.
    let physical_by_column_id = physical_columns
        .iter()
        .map(|column| (column.planner.column_id, column))
        .collect::<BTreeMap<_, _>>();
    let ordered_physical = scan
        .columns
        .iter()
        .filter_map(|column| physical_by_column_id.get(&column.column_id).copied())
        .collect::<Vec<_>>();
    // A physical column the scan does not output has no output slot to fill,
    // and dropping it would silently shift every later assignment.
    if ordered_physical.len() != physical_columns.len() {
        return Err(format!(
            "typed scan of relation {relation_name} resolved {} physical columns but the scan outputs only {} of them",
            physical_columns.len(),
            ordered_physical.len()
        ));
    }
    let mut assignments = Vec::with_capacity(ordered_physical.len());
    let mut columns_by_name: BTreeMap<String, ValidatedColumnHandle> = BTreeMap::new();
    let mut value_types_by_name: BTreeMap<String, ConnectorValueType> = BTreeMap::new();
    let mut variables_by_name: BTreeMap<String, String> = BTreeMap::new();
    for (ordinal, output) in ordered_physical.iter().enumerate() {
        // The connector is asked for the column by its own schema spelling;
        // the planner name beside it may be an alias and is never provider
        // identity.
        let binding = unique_binding(&column_bindings, &output.source.name, &relation_name)?;
        // `TypedColumnBinding` carries column identity, not column type, so
        // the assignment's exact type is the engine's own declared type. A
        // type with no exact typed counterpart is rejected rather than
        // approximated: the connector would otherwise filter and decode
        // against a type the query never stated.
        let value_type = scan_output_value_type(&output.planner.data_type).ok_or_else(|| {
            format!(
                "typed scan output column '{}' of relation {relation_name} has engine type {:?}, which has no exact typed connector counterpart",
                output.source.name, output.planner.data_type
            )
        })?;
        let variable = format!("{SCAN_VARIABLE_PREFIX}{ordinal}");
        let assignment = ScanAssignment::parse(
            dto::ScanAssignment {
                variable: variable.clone(),
                column: Some(binding.column().as_proto().clone()),
                value_type: Some(encode_value_type(value_type)),
            },
            FieldPath::root("scan_assignment"),
        )
        .map_err(|error| {
            let column_name = &output.source.name;
            format!(
                "typed scan assignment for output column '{column_name}' of relation {relation_name}: {error}"
            )
        })?;
        // Predicate and dynamic-filter lookups are by planner output column
        // name, because that is the name the plan's own column references
        // resolve to. Two outputs sharing a name would make those lookups
        // ambiguous, and picking either one would push down a predicate about
        // the other.
        if columns_by_name
            .insert(output.planner.name.clone(), binding.column().clone())
            .is_some()
        {
            return Err(format!(
                "typed scan of relation {relation_name} outputs column '{}' more than once",
                output.planner.name
            ));
        }
        value_types_by_name.insert(output.planner.name.clone(), value_type);
        variables_by_name.insert(output.planner.name.clone(), variable);
        assignments.push(assignment);
    }

    // 4. Lower the scan's own conjuncts into the offered summary.
    let lowered = lower_scan_predicates(scan, &columns_by_name, &value_types_by_name);
    let constraint = WireConstraint::of_summary(lowered.summary.clone());

    // 5. Offer the filter. Whatever the connector hands back stays the
    //    reader's own work: `unenforced_predicate` is applied by the backend
    //    scan, `enforced_predicate` records only what the connector took.
    let (enforced_predicate, unenforced_predicate, remaining_expression) = match metadata
        .apply_filter(session, &handle, &constraint)
        .map_err(|error| {
            format!("typed scan filter pushdown on relation {relation_name} failed: {error}")
        })? {
        // Nothing was accepted, so the engine keeps the whole predicate.
        None => (TupleDomain::all(), lowered.summary.clone(), None),
        Some(application) => {
            let unenforced = application.remaining_constraint().summary().clone();
            let remaining_expression = application
                .remaining_expression()
                .filter(|expression| !expression.is_constant_true())
                .cloned();
            // Enforcement is claimed only for a column the connector kept
            // whole. A column it handed back partially is covered by its own
            // guarantee for the complement, and by the reader for the rest.
            let enforced = lowered
                .summary
                .filter_columns(|column| unenforced.domain_for(column).is_none());
            handle = application.into_handle();
            (enforced, unenforced, remaining_expression)
        }
    };

    // 6. Offer the projection. A narrowed handle is a pushdown fact; the
    //    ordered assignments above remain the output authority either way.
    if let Some(narrowed) = metadata
        .apply_projection(session, &handle, &assignments)
        .map_err(|error| {
            format!("typed scan projection pushdown on relation {relation_name} failed: {error}")
        })?
    {
        handle = narrowed;
    }

    // 7. Offer the limit. Only the connector's own answer may drop the
    //    engine's limit operator.
    let mut limit_guaranteed = false;
    if let Some(limit) = limit
        && let Some(application) =
            metadata
                .apply_limit(session, &handle, limit)
                .map_err(|error| {
                    format!("typed scan limit pushdown on relation {relation_name} failed: {error}")
                })?
    {
        limit_guaranteed = application.limit_guaranteed();
        handle = application.into_handle();
    }

    // 8. Bind the dynamic filters and validate the whole source.
    let dynamic_filter_bindings =
        bind_dynamic_filters(dynamic_filters, &variables_by_name, &relation_name)?;
    // Both families this preparation freezes are enumerated: a table and a
    // change window each produce splits the round drives at runtime. Reading a
    // relation whole belongs to a system relation resolved to one task, which
    // this preparation cannot freeze, so naming the variants here keeps adding
    // one a compile error rather than a scan that reads nothing.
    let work_source = match freeze {
        TypedRelationFreeze::Table { .. } | TypedRelationFreeze::ChangeWindow(_) => {
            dto::ScanWorkSource::RuntimeSplits
        }
    };
    let source = dto::ConnectorTableScanSource {
        table: Some(handle.as_proto().clone()),
        assignments: assignments
            .iter()
            .map(|assignment| assignment.as_proto().clone())
            .collect(),
        enforced_predicate: Some(encode_tuple_domain(&enforced_predicate)),
        unenforced_predicate: Some(encode_tuple_domain(&unenforced_predicate)),
        remaining_expression: remaining_expression
            .as_ref()
            .map(encode_connector_expression),
        dynamic_filters: dynamic_filter_bindings,
        max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
        max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
        work_source: work_source as i32,
    };
    let source = ConnectorTableScanSource::parse(
        source,
        FieldPath::root("connector_table_scan_source"),
    )
    .map_err(|error| {
        format!(
            "typed scan source for plan node {plan_node_id} of relation {relation_name}: {error}"
        )
    })?;

    let table_scan = TableScanNode::new(plan_node_id, TableHandle::new(catalog, handle), source)
        .map_err(|error| format!("typed scan node {plan_node_id}: {error}"))?;

    // 9. Take the enumerator entry point without enumerating anything.
    Ok(PreparedTypedScan {
        table_scan,
        split_manager: control.splits(),
        constraint,
        residual_ordinals: lowered.residual_ordinals,
        limit_guaranteed,
    })
}

/// The one connector column an output column names.
///
/// Matching is case-insensitive because the SQL and connector spellings of one
/// column may differ, but it must be unambiguous: two connector columns whose
/// names differ only in case give no basis for choosing between them.
fn unique_binding<'a>(
    bindings: &'a [TypedColumnBinding],
    name: &str,
    relation_name: &str,
) -> Result<&'a TypedColumnBinding, String> {
    let mut matched = bindings
        .iter()
        .filter(|binding| binding.name().eq_ignore_ascii_case(name));
    let binding = matched.next().ok_or_else(|| {
        format!(
            "typed scan output column '{name}' has no column binding in relation {relation_name}"
        )
    })?;
    if matched.next().is_some() {
        return Err(format!(
            "typed scan output column '{name}' matches more than one column binding in relation {relation_name}"
        ));
    }
    Ok(binding)
}

fn bind_dynamic_filters(
    dynamic_filters: &[(u32, String)],
    variables_by_name: &BTreeMap<String, String>,
    relation_name: &str,
) -> Result<Vec<dto::DynamicFilterBinding>, String> {
    dynamic_filters
        .iter()
        .map(|(filter_id, column_name)| {
            let variable = variables_by_name.get(column_name).ok_or_else(|| {
                format!(
                    "typed scan dynamic filter {filter_id} names column '{column_name}', which the scan of relation {relation_name} does not output"
                )
            })?;
            Ok(dto::DynamicFilterBinding {
                filter_id: *filter_id,
                variable: variable.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use arrow::datatypes::DataType;
    use novarocks_proto::connector_read::{
        CatalogTableHandle, ConnectorRelation, TypedConnectorMetadata, TypedConnectorSplitSource,
        TypedFilterApplication, TypedLimitApplication, TypedSystemTablePlan,
    };
    use novarocks_spi::connector::read_stack::{ConnectorValue, Domain, ValueSet};
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use novarocks_sql::plan_read::{BinOp, OutputColumn};
    use novarocks_types::schema::ColumnDef;

    use super::super::scan::ResolvedScanColumnKind;
    use super::super::typed_predicate::test_support::{
        binary, column_handle, column_ref, int_literal, output, scan,
    };
    use super::*;

    /// What the stub connector does with a filter offer.
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum FilterBehavior {
        /// Accept nothing.
        Decline,
        /// Accept the whole offered summary.
        AcceptAll,
        /// Accept nothing about `id`, keeping it in the remaining constraint.
        LeaveIdUnenforced,
    }

    /// What the stub connector does with a limit offer.
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum LimitBehavior {
        Decline,
        AcceptWithoutGuarantee,
        Guarantee,
    }

    struct StubControl {
        handle: Option<CatalogTableHandle>,
        change_window: Option<CatalogTableHandle>,
        bindings: Vec<TypedColumnBinding>,
        filter: FilterBehavior,
        limit: LimitBehavior,
        splits_requested: AtomicUsize,
        /// Every change window the stub was asked to freeze.
        change_windows_requested: Mutex<Vec<TypedChangeWindow>>,
    }

    impl StubControl {
        fn new() -> Self {
            Self {
                handle: Some(catalog_table_handle()),
                change_window: Some(catalog_change_window_handle()),
                bindings: vec![
                    TypedColumnBinding::new("id", column_handle(1, "id"), false),
                    TypedColumnBinding::new("category", column_handle(2, "category"), false),
                ],
                filter: FilterBehavior::AcceptAll,
                limit: LimitBehavior::Decline,
                splits_requested: AtomicUsize::new(0),
                change_windows_requested: Mutex::new(Vec::new()),
            }
        }
    }

    impl TypedConnectorMetadata for StubControl {
        fn get_table_handle(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _version: TypedRelationVersion,
            _reference: Option<&str>,
        ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
            Ok(self.handle.clone())
        }

        fn get_column_bindings(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
        ) -> Result<Vec<TypedColumnBinding>, ConnectorError> {
            Ok(self.bindings.clone())
        }

        fn apply_filter(
            &self,
            _session: &ConnectorSession,
            table: &CatalogTableHandle,
            constraint: &WireConstraint,
        ) -> Result<Option<TypedFilterApplication>, ConnectorError> {
            match self.filter {
                FilterBehavior::Decline => Ok(None),
                FilterBehavior::AcceptAll => Ok(Some(TypedFilterApplication::new(
                    table.clone(),
                    WireConstraint::of_summary(TupleDomain::all()),
                    None,
                ))),
                FilterBehavior::LeaveIdUnenforced => {
                    let id = column_handle(1, "id");
                    let remaining = constraint.summary().filter_columns(|column| *column == id);
                    Ok(Some(TypedFilterApplication::new(
                        table.clone(),
                        WireConstraint::of_summary(remaining),
                        None,
                    )))
                }
            }
        }

        fn apply_projection(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _assignments: &[ScanAssignment],
        ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
            Ok(None)
        }

        fn apply_limit(
            &self,
            _session: &ConnectorSession,
            table: &CatalogTableHandle,
            _limit: u64,
        ) -> Result<Option<TypedLimitApplication>, ConnectorError> {
            match self.limit {
                LimitBehavior::Decline => Ok(None),
                LimitBehavior::AcceptWithoutGuarantee => {
                    Ok(Some(TypedLimitApplication::new(table.clone(), false)))
                }
                LimitBehavior::Guarantee => {
                    Ok(Some(TypedLimitApplication::new(table.clone(), true)))
                }
            }
        }

        fn get_system_table_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
        ) -> Result<Option<TypedSystemTablePlan>, ConnectorError> {
            Ok(None)
        }

        fn get_change_window_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            window: TypedChangeWindow,
        ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
            self.change_windows_requested
                .lock()
                .expect("change window log")
                .push(window);
            Ok(self.change_window.clone())
        }
    }

    impl TypedConnectorSplitManager for StubControl {
        fn get_splits(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _columns: &[ScanAssignment],
            _dynamic_filter_columns: &BTreeSet<ValidatedColumnHandle>,
            _constraint: &WireConstraint,
        ) -> Result<Box<dyn TypedConnectorSplitSource>, ConnectorError> {
            self.splits_requested.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "the stub control enumerates no split",
            ))
        }
    }

    fn all_domain() -> dto::TupleDomain {
        dto::TupleDomain {
            none: false,
            column_domains: Vec::new(),
        }
    }

    fn catalog_table_handle() -> CatalogTableHandle {
        CatalogTableHandle::parse(
            dto::CatalogTableHandle {
                catalog_name: "ice".to_owned(),
                instance_incarnation: vec![1_u8; 16],
                transaction: Some(dto::ConnectorTransactionHandle {
                    handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                        dto::HiveTransactionHandle {
                            auto_commit: true,
                            uuid: vec![2_u8; 16],
                        },
                    )),
                }),
                relation: Some(dto::catalog_table_handle::Relation::Table(
                    dto::ConnectorTableHandle {
                        handle: Some(dto::connector_table_handle::Handle::Iceberg(
                            dto::IcebergTableHandle {
                                schema_table_name: Some(dto::SchemaTableName {
                                    schema_name: "db".to_owned(),
                                    table_name: "t".to_owned(),
                                }),
                                snapshot_id: Some(7),
                                table_schema_json: "{}".to_owned(),
                                spec_id: None,
                                partition_spec_jsons: BTreeMap::new(),
                                format_version: 2,
                                unenforced_predicate: Some(all_domain()),
                                enforced_predicate: Some(all_domain()),
                                limit: None,
                                projected_columns: Vec::new(),
                                name_mapping_json: None,
                                table_location: "s3://bucket/table".to_owned(),
                                storage_properties: BTreeMap::new(),
                            },
                        )),
                    },
                )),
            },
            FieldPath::root("catalog_table_handle"),
        )
        .expect("valid catalog table handle")
    }

    /// The stub's frozen change window, pinned to both endpoints.
    fn catalog_change_window_handle() -> CatalogTableHandle {
        CatalogTableHandle::parse(
            dto::CatalogTableHandle {
                catalog_name: "ice".to_owned(),
                instance_incarnation: vec![1_u8; 16],
                transaction: Some(dto::ConnectorTransactionHandle {
                    handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                        dto::HiveTransactionHandle {
                            auto_commit: true,
                            uuid: vec![2_u8; 16],
                        },
                    )),
                }),
                relation: Some(dto::catalog_table_handle::Relation::ChangeWindow(
                    dto::ConnectorChangeWindowHandle {
                        handle: Some(dto::connector_change_window_handle::Handle::Iceberg(
                            dto::IcebergChangeWindowHandle {
                                schema_table_name: Some(dto::SchemaTableName {
                                    schema_name: "db".to_owned(),
                                    table_name: "t".to_owned(),
                                }),
                                table_schema_json: "{}".to_owned(),
                                columns: vec![
                                    iceberg_column(1, "id"),
                                    iceberg_column(2, "category"),
                                ],
                                name_mapping_json: None,
                                from_snapshot_id_exclusive: 6,
                                to_snapshot_id_inclusive: 7,
                            },
                        )),
                    },
                )),
            },
            FieldPath::root("catalog_table_handle"),
        )
        .expect("valid catalog change window handle")
    }

    fn iceberg_column(field_id: i32, name: &str) -> dto::IcebergColumnHandle {
        dto::IcebergColumnHandle {
            base_column_identity: Some(dto::ColumnIdentity {
                field_id,
                name: name.to_owned(),
                category: dto::ColumnIdentityCategory::Primitive as i32,
                children: Vec::new(),
            }),
            base_type_json: "\"long\"".to_owned(),
            field_id_path: Vec::new(),
            type_json: "\"long\"".to_owned(),
            nullable: true,
            comment: None,
        }
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q1", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("valid session")
    }

    fn relation() -> SchemaTableName {
        SchemaTableName::try_new("db", "t").expect("valid relation name")
    }

    fn outputs() -> Vec<OutputColumn> {
        vec![
            output(1, "id", DataType::Int32, false),
            output(3, "category", DataType::Utf8, true),
        ]
    }

    /// Pair each planner output with the physical column it resolved to, the
    /// way `resolve_physical_columns` does before preparation runs.
    fn physical(columns: &[OutputColumn]) -> Vec<ResolvedScanColumn> {
        columns
            .iter()
            .map(|planner| ResolvedScanColumn {
                planner: planner.clone(),
                source: ColumnDef {
                    name: planner.name.clone(),
                    data_type: planner.data_type.clone(),
                    nullable: planner.nullable,
                    write_default: None,
                    logical_type: None,
                },
                kind: ResolvedScanColumnKind::PhysicalTableColumn,
            })
            .collect()
    }

    fn id_predicate() -> Vec<novarocks_sql::plan_read::TypedExpr> {
        vec![binary(
            column_ref(1, "id", DataType::Int32, false),
            BinOp::Eq,
            int_literal(7),
        )]
    }

    fn prepare(
        stub: Arc<StubControl>,
        columns: Vec<OutputColumn>,
        predicates: Vec<novarocks_sql::plan_read::TypedExpr>,
        limit: Option<u64>,
        dynamic_filters: &[(u32, String)],
    ) -> Result<PreparedTypedScan, String> {
        let physical_columns = physical(&columns);
        prepare_with_physical_columns(
            stub,
            columns,
            &physical_columns,
            predicates,
            TypedRelationFreeze::Table {
                version: TypedRelationVersion::Current,
                reference: None,
            },
            limit,
            dynamic_filters,
        )
    }

    /// The helper mirrors `prepare_typed_scan`'s own frozen inputs.
    fn prepare_with_physical_columns(
        stub: Arc<StubControl>,
        columns: Vec<OutputColumn>,
        physical_columns: &[ResolvedScanColumn],
        predicates: Vec<novarocks_sql::plan_read::TypedExpr>,
        freeze: TypedRelationFreeze<'_>,
        limit: Option<u64>,
        dynamic_filters: &[(u32, String)],
    ) -> Result<PreparedTypedScan, String> {
        let control = TypedConnectorControl::new(stub.clone(), stub);
        prepare_typed_scan(
            &session(),
            CatalogHandle::new("ice", [1; 16]),
            &control,
            11,
            &scan(columns, predicates),
            physical_columns,
            &relation(),
            freeze,
            limit,
            dynamic_filters,
        )
    }

    #[test]
    fn assignments_keep_the_scan_column_order_and_are_uniquely_named() {
        let prepared = prepare(
            Arc::new(StubControl::new()),
            outputs(),
            Vec::new(),
            None,
            &[],
        )
        .expect("prepared scan");
        let assignments = prepared.table_scan.source().assignments();
        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].variable(), "v0");
        assert_eq!(assignments[1].variable(), "v1");
        assert_eq!(assignments[0].column(), &column_handle(1, "id"));
        assert_eq!(assignments[1].column(), &column_handle(2, "category"));
        assert_eq!(assignments[0].value_type(), ConnectorValueType::Integer);
        assert_eq!(assignments[1].value_type(), ConnectorValueType::Varchar);

        // Reversing the scan output order reverses the assignments: the scan
        // is the authority, not the connector's own schema order.
        let mut reversed = outputs();
        reversed.reverse();
        let prepared = prepare(
            Arc::new(StubControl::new()),
            reversed,
            Vec::new(),
            None,
            &[],
        )
        .expect("prepared scan");
        let assignments = prepared.table_scan.source().assignments();
        assert_eq!(assignments[0].column(), &column_handle(2, "category"));
        assert_eq!(assignments[1].column(), &column_handle(1, "id"));
    }

    #[test]
    fn an_output_column_with_no_binding_is_an_error() {
        let mut stub = StubControl::new();
        stub.bindings.retain(|binding| binding.name() != "category");
        let error = prepare(Arc::new(stub), outputs(), Vec::new(), None, &[])
            .expect_err("an unbound output column cannot be dropped");
        assert!(
            error.contains("'category'") && error.contains("no column binding"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn an_output_column_with_no_exact_typed_counterpart_is_an_error() {
        let columns = vec![output(1, "id", DataType::Int16, false)];
        let error = prepare(Arc::new(StubControl::new()), columns, Vec::new(), None, &[])
            .expect_err("an inexact engine type cannot be approximated");
        assert!(
            error.contains("no exact typed connector counterpart"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_missing_relation_names_the_relation() {
        let mut stub = StubControl::new();
        stub.handle = None;
        let error = prepare(Arc::new(stub), outputs(), Vec::new(), None, &[])
            .expect_err("a vanished pin is an error");
        assert!(
            error.contains("db.t") && error.contains("no longer resolvable"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_dynamic_filter_naming_an_unknown_column_is_an_error() {
        let error = prepare(
            Arc::new(StubControl::new()),
            outputs(),
            Vec::new(),
            None,
            &[(4, "absent".to_owned())],
        )
        .expect_err("an unbound dynamic filter cannot be dropped");
        assert!(
            error.contains("dynamic filter 4") && error.contains("'absent'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_bound_dynamic_filter_reaches_the_scan_node() {
        let prepared = prepare(
            Arc::new(StubControl::new()),
            outputs(),
            Vec::new(),
            None,
            &[(4, "category".to_owned())],
        )
        .expect("prepared scan");
        let bindings = prepared.table_scan.source().dynamic_filters();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].filter_id(), 4);
        assert_eq!(bindings[0].variable(), "v1");
        assert_eq!(
            prepared.table_scan.dynamic_filter_columns(),
            BTreeSet::from([column_handle(2, "category")])
        );
    }

    #[test]
    fn a_declined_filter_keeps_the_whole_predicate_as_engine_work() {
        let mut stub = StubControl::new();
        stub.filter = FilterBehavior::Decline;
        let prepared =
            prepare(Arc::new(stub), outputs(), id_predicate(), None, &[]).expect("prepared scan");
        let source = prepared.table_scan.source();
        // Nothing is claimed as connector-enforced, and the reader keeps the
        // whole offered summary.
        assert!(source.enforced_predicate().is_all());
        assert_eq!(
            source
                .unenforced_predicate()
                .domain_for(&column_handle(1, "id")),
            Some(&Domain::single_value(ConnectorValue::Integer(7)).expect("single value"))
        );
        // The conjunct was representable, so it is not a SQL-level residual.
        assert!(prepared.residual_ordinals.is_empty());
    }

    #[test]
    fn an_accepted_filter_records_the_offered_summary_as_enforced() {
        let prepared = prepare(
            Arc::new(StubControl::new()),
            outputs(),
            id_predicate(),
            None,
            &[],
        )
        .expect("prepared scan");
        let source = prepared.table_scan.source();
        assert_eq!(
            source
                .enforced_predicate()
                .domain_for(&column_handle(1, "id")),
            Some(&Domain::single_value(ConnectorValue::Integer(7)).expect("single value"))
        );
        assert!(source.unenforced_predicate().is_all());
    }

    #[test]
    fn a_column_the_connector_hands_back_is_never_claimed_as_enforced() {
        let mut stub = StubControl::new();
        stub.filter = FilterBehavior::LeaveIdUnenforced;
        let predicates = vec![
            binary(
                column_ref(1, "id", DataType::Int32, false),
                BinOp::Eq,
                int_literal(7),
            ),
            binary(
                column_ref(3, "category", DataType::Utf8, true),
                BinOp::Eq,
                super::super::typed_predicate::test_support::text_literal("a"),
            ),
        ];
        let prepared =
            prepare(Arc::new(stub), outputs(), predicates, None, &[]).expect("prepared scan");
        let source = prepared.table_scan.source();
        let id = column_handle(1, "id");
        let category = column_handle(2, "category");
        assert!(source.enforced_predicate().domain_for(&id).is_none());
        assert_eq!(
            source.enforced_predicate().domain_for(&category),
            Some(&Domain::new(
                ValueSet::of_values(
                    ConnectorValueType::Varchar,
                    vec![ConnectorValue::Varchar(Arc::from("a"))],
                )
                .expect("valid set"),
                false,
            ))
        );
        assert!(source.unenforced_predicate().domain_for(&id).is_some());
        assert!(
            source
                .unenforced_predicate()
                .domain_for(&category)
                .is_none()
        );
    }

    #[test]
    fn an_unrepresentable_predicate_stays_a_sql_residual() {
        let predicates = vec![binary(
            column_ref(1, "id", DataType::Int32, false),
            BinOp::Add,
            int_literal(1),
        )];
        let prepared = prepare(
            Arc::new(StubControl::new()),
            outputs(),
            predicates,
            None,
            &[],
        )
        .expect("prepared scan");
        assert_eq!(prepared.residual_ordinals, vec![0]);
        assert!(prepared.table_scan.source().enforced_predicate().is_all());
    }

    #[test]
    fn a_limit_the_connector_cannot_guarantee_leaves_it_unguaranteed() {
        let mut stub = StubControl::new();
        stub.limit = LimitBehavior::AcceptWithoutGuarantee;
        let prepared =
            prepare(Arc::new(stub), outputs(), Vec::new(), Some(10), &[]).expect("prepared scan");
        assert!(!prepared.limit_guaranteed);

        let mut stub = StubControl::new();
        stub.limit = LimitBehavior::Decline;
        let prepared =
            prepare(Arc::new(stub), outputs(), Vec::new(), Some(10), &[]).expect("prepared scan");
        assert!(!prepared.limit_guaranteed);

        let mut stub = StubControl::new();
        stub.limit = LimitBehavior::Guarantee;
        let prepared =
            prepare(Arc::new(stub), outputs(), Vec::new(), Some(10), &[]).expect("prepared scan");
        assert!(prepared.limit_guaranteed);

        // No limit was offered, so nothing can be guaranteed.
        let mut stub = StubControl::new();
        stub.limit = LimitBehavior::Guarantee;
        let prepared =
            prepare(Arc::new(stub), outputs(), Vec::new(), None, &[]).expect("prepared scan");
        assert!(!prepared.limit_guaranteed);
    }

    #[test]
    fn preparation_enumerates_no_split() {
        let stub = Arc::new(StubControl::new());
        let prepared = prepare(
            Arc::clone(&stub),
            outputs(),
            id_predicate(),
            Some(10),
            &[(4, "category".to_owned())],
        )
        .expect("prepared scan");
        assert_eq!(stub.splits_requested.load(Ordering::SeqCst), 0);
        // The enumerator is handed over untouched for the execution round.
        assert!(
            prepared
                .split_manager
                .get_splits(
                    &session(),
                    prepared.table_scan.source().table(),
                    prepared.table_scan.source().assignments(),
                    &prepared.table_scan.dynamic_filter_columns(),
                    &prepared.constraint,
                )
                .is_err()
        );
        assert_eq!(stub.splits_requested.load(Ordering::SeqCst), 1);
    }

    /// A synthetic output — a VARIANT path column — has no connector column at
    /// all. Only the physical columns are assigned; the backend materializes
    /// the synthetic one on top of those read slots.
    #[test]
    fn a_synthetic_output_column_is_not_assigned() {
        let mut columns = outputs();
        let physical_columns = physical(&columns);
        columns.push(output(9, "__nr_var_v_0", DataType::LargeBinary, true));

        let prepared = prepare_with_physical_columns(
            Arc::new(StubControl::new()),
            columns,
            &physical_columns,
            Vec::new(),
            TypedRelationFreeze::Table {
                version: TypedRelationVersion::Current,
                reference: None,
            },
            None,
            &[],
        )
        .expect("a synthetic output must not be offered to the connector");
        let assignments = prepared.table_scan.source().assignments();
        assert_eq!(
            assignments.len(),
            2,
            "only the physical columns are assigned"
        );
        assert_eq!(assignments[0].column(), &column_handle(1, "id"));
        assert_eq!(assignments[1].column(), &column_handle(2, "category"));
    }

    /// A change window is frozen through its own control entry point and the
    /// frozen handle pins both endpoints.
    #[test]
    fn a_change_window_freezes_through_the_change_window_entry_point() {
        let stub = Arc::new(StubControl::new());
        let columns = outputs();
        let physical_columns = physical(&columns);
        let prepared = prepare_with_physical_columns(
            Arc::clone(&stub),
            columns,
            &physical_columns,
            Vec::new(),
            TypedRelationFreeze::ChangeWindow(TypedChangeWindow::new(6, 7)),
            None,
            &[],
        )
        .expect("prepared change-window scan");

        assert_eq!(
            *stub
                .change_windows_requested
                .lock()
                .expect("change window log"),
            vec![TypedChangeWindow::new(6, 7)],
            "the window preparation asked for is the window the scan names"
        );
        assert_eq!(
            prepared.table_scan.table().relation_kind(),
            ConnectorRelationKind::ChangeWindow
        );
        let ConnectorRelation::ChangeWindow(window) =
            prepared.table_scan.table().handle().relation()
        else {
            panic!("a change-window freeze produces a change-window relation");
        };
        let Some(dto::connector_change_window_handle::Handle::Iceberg(iceberg)) =
            window.handle.as_ref()
        else {
            panic!("the stub freezes an Iceberg change window");
        };
        assert_eq!(iceberg.from_snapshot_id_exclusive, 6);
        assert_eq!(iceberg.to_snapshot_id_inclusive, 7);
    }

    /// A relation the connector does not expose a change window over is a
    /// refusal that names both endpoints, never a silent full-table read.
    #[test]
    fn a_relation_without_a_change_window_is_an_error() {
        let mut stub = StubControl::new();
        stub.change_window = None;
        let columns = outputs();
        let physical_columns = physical(&columns);
        let error = prepare_with_physical_columns(
            Arc::new(stub),
            columns,
            &physical_columns,
            Vec::new(),
            TypedRelationFreeze::ChangeWindow(TypedChangeWindow::new(6, 7)),
            None,
            &[],
        )
        .expect_err("an absent change window cannot fall back to the table");
        assert!(
            error.contains("db.t")
                && error.contains("exposes no change window from snapshot 6 to snapshot 7"),
            "unexpected error: {error}"
        );
    }
}
