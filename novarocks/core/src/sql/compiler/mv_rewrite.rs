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

//! Immutable materialized-view rewrite facts frozen by application admission.
//!
//! The compiler uses this value as data only. Repository enumeration and
//! connector/catalog reads happen before construction, in the application
//! facade, so one statement never observes a changing MV definition set.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::sql::binding::SqlTableBindingId;
use crate::sql::catalog::PlannerTableProvider;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::cascades_rules::mv_rewrite::{
    MvRewriteCandidate, descriptor::SpjgDescriptor,
};
use crate::sql::planner::logical::LogicalPlanNode;
use crate::sql::planner::table::ScanSource;

use super::{SqlFunctionCatalog, SqlStatisticsPlan, SqlStatisticsSnapshot};

/// One base-table snapshot admitted for an incremental MV refresh.
///
/// The compiler identifies a base by its canonical identity and never asks a
/// connector for a newer snapshot.  The application converts its provider
/// lease into this value before calling the compiler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseSnapshot {
    pub(crate) table: novarocks_catalog::identifier::TableIdentity,
    pub(crate) snapshot_id: i64,
    pub(crate) table_uuid: String,
}

/// SQL classification of the two physical aggregate-state roles used by the
/// incremental refresh plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvAggregateStateRole {
    Single,
    RetractionCount,
}

/// One visible target output in an aggregate refresh layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateVisibleColumn {
    pub(crate) name: String,
    pub(crate) data_type: arrow::datatypes::DataType,
    pub(crate) nullable: bool,
}

/// One physical state column in an aggregate refresh layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateStateColumn {
    pub(crate) name: String,
    pub(crate) data_type: arrow::datatypes::DataType,
    pub(crate) nullable: bool,
    pub(crate) visible_source_index: usize,
    pub(crate) aggregate_index: usize,
    pub(crate) function: crate::sql::mv_refresh::AggregateFunctionKind,
    pub(crate) state_role: SqlImvAggregateStateRole,
    pub(crate) count_star: bool,
}

/// SQL-only aggregate IMV layout frozen by application admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateLayout {
    pub(crate) row_id_column_name: String,
    pub(crate) visible_columns: Vec<SqlImvAggregateVisibleColumn>,
    pub(crate) state_columns: Vec<SqlImvAggregateStateColumn>,
    pub(crate) group_key_source_indexes: Vec<usize>,
    pub(crate) physical_column_names: Vec<String>,
    pub(crate) aggregate_input_types: Vec<Option<arrow::datatypes::DataType>>,
}

/// Aggregate-shape facts required by SQL rewrite construction.  The original
/// persisted SELECT and the application aggregate-state implementation stay
/// outside the compiler boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateShape {
    pub(crate) group_key_count: usize,
    pub(crate) visible_outputs: Vec<crate::sql::mv_refresh::VisibleAggregateOutput>,
}

/// The aggregate facts admitted for an IMV refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateExecutionLayout {
    pub(crate) shape: SqlImvAggregateShape,
    pub(crate) layout: SqlImvAggregateLayout,
}

/// SQL-owned lineage kind recorded in an immutable MV contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvExpressionKind {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvQualifiedFieldLineage {
    pub(crate) table_fqn: String,
    pub(crate) qualifier_at_create: String,
    pub(crate) field_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvExpressionLineage {
    pub(crate) kind: SqlImvExpressionKind,
    pub(crate) referenced_base_field_ids: Vec<i32>,
    pub(crate) referenced_base_fields: Vec<SqlImvQualifiedFieldLineage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvOutputColumnLineage {
    pub(crate) expression: SqlImvExpressionLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseField {
    pub(crate) field_id: i32,
    pub(crate) name_at_create: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseContract {
    pub(crate) table_fqn: String,
    pub(crate) alias_at_create: Option<String>,
    pub(crate) fields: Vec<SqlImvBaseField>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvJoinContractKind {
    InnerEquiJoin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvJoinPredicateLineage {
    pub(crate) left: SqlImvQualifiedFieldLineage,
    pub(crate) right: SqlImvQualifiedFieldLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvJoinContract {
    pub(crate) kind: SqlImvJoinContractKind,
    pub(crate) predicates: Vec<SqlImvJoinPredicateLineage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvAggregateStateRoleContract {
    Single,
    RetractionCount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateStateColumnContract {
    pub(crate) column_name: String,
    pub(crate) type_signature: String,
    pub(crate) role: SqlImvAggregateStateRoleContract,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateContract {
    pub(crate) state_layout_version: u16,
    pub(crate) row_id_column_name: String,
    pub(crate) state_columns: Vec<SqlImvAggregateStateColumnContract>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvTargetVisibleColumn {
    pub(crate) output_name: String,
    pub(crate) target_field_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvHiddenApplyKey {
    pub(crate) column_name: String,
    pub(crate) source: crate::sql::planner::vocabulary::ApplyKeySource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBranchContract {
    pub(crate) branch_id_column_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionContract {
    pub(crate) target_spec_id: i32,
    pub(crate) fields: Vec<SqlImvPartitionField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionField {
    pub(crate) partition_field_name: String,
    pub(crate) source_target_field_id: i32,
    pub(crate) transform: SqlImvPartitionTransform,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
}

/// Plan-time, SQL-owned partition derivation facts.  Execution converts this
/// abstract transform into a connector-specific representation after compile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionDerivationSpec {
    pub(crate) target_spec_id: i32,
    pub(crate) fields: Vec<SqlImvPartitionDerivationField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionDerivationField {
    pub(crate) partition_field_name: String,
    pub(crate) source_target_field_id: i32,
    pub(crate) output_index: usize,
    pub(crate) transform: SqlImvPartitionTransform,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvTargetContract {
    pub(crate) visible_columns: Vec<SqlImvTargetVisibleColumn>,
    pub(crate) hidden_apply_key: SqlImvHiddenApplyKey,
    pub(crate) partition: Option<SqlImvPartitionContract>,
}

/// Immutable SQL projection of the persisted MV schema contract.  Persistence
/// adapters must translate their serialized form before compiler entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvSchemaContract {
    pub(crate) bases: Vec<SqlImvBaseContract>,
    pub(crate) output_columns: Vec<SqlImvOutputColumnLineage>,
    pub(crate) join: Option<SqlImvJoinContract>,
    pub(crate) aggregate: Option<SqlImvAggregateContract>,
    pub(crate) branch: Option<SqlImvBranchContract>,
    pub(crate) target: SqlImvTargetContract,
}

/// Immutable, query-scoped facts consumed by incremental-MV rewrite rules.
///
/// This is the SQL boundary for refresh planning.  It intentionally contains
/// no repository, connector table, metadata payload, lease, callback, or
/// application context.  The persisted schema contract is still carried as a
/// value until its persistence vocabulary moves under the SQL owner; the
/// compiler never uses it to access application state.
#[derive(Clone, Debug)]
pub(crate) struct SqlImvRewriteSnapshot {
    pub(crate) target: novarocks_catalog::identifier::TableIdentity,
    /// Exact request-local target materialization. Every target-state and
    /// target-locator scan produced by the rewrite carries this token, so
    /// preparation cannot silently reacquire a newer target generation.
    pub(crate) target_binding: SqlTableBindingId,
    pub(crate) mv_id: i64,
    pub(crate) base_snapshots: Arc<[SqlImvBaseSnapshot]>,
    pub(crate) previous_snapshot_ids: BTreeMap<String, i64>,
    pub(crate) previous_table_uuids: BTreeMap<String, String>,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) target_table_uuid: String,
    /// SQL-safe target field facts projected by the application.  This avoids
    /// exposing an Iceberg schema or Iceberg default-literal values to SQL.
    pub(crate) target_columns: Arc<[novarocks_catalog::schema::ColumnDef]>,
    /// SQL projection of the persisted MV planning contract frozen at
    /// admission. Resolving or mutating the serialized contract remains in
    /// the application facade.
    pub(crate) schema_contract: Arc<SqlImvSchemaContract>,
    /// Aggregate shape/layout was derived from the admitted MV definition by
    /// application before compiler entry.  Non-aggregate refreshes use None.
    pub(crate) aggregate_execution: Option<SqlImvAggregateExecutionLayout>,
}

impl SqlImvRewriteSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_frozen_parts(
        target: novarocks_catalog::identifier::TableIdentity,
        target_binding: SqlTableBindingId,
        mv_id: i64,
        base_snapshots: Arc<[SqlImvBaseSnapshot]>,
        previous_snapshot_ids: BTreeMap<String, i64>,
        previous_table_uuids: BTreeMap<String, String>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        target_columns: Arc<[novarocks_catalog::schema::ColumnDef]>,
        schema_contract: Arc<SqlImvSchemaContract>,
        aggregate_execution: Option<SqlImvAggregateExecutionLayout>,
    ) -> Result<Self, String> {
        if base_snapshots.is_empty() {
            return Err("IMV rewrite snapshot has no base table snapshots".to_string());
        }
        for base in base_snapshots.iter() {
            if base.table_uuid.trim().is_empty() {
                return Err(format!(
                    "IMV rewrite snapshot base {} has an empty table UUID",
                    base.table.fqn()
                ));
            }
            if let Some(previous_uuid) = previous_table_uuids.get(&base.table.fqn())
                && previous_uuid != &base.table_uuid
            {
                return Err(format!(
                    "base table identity changed for {}; incremental refresh unsafe, rebuild the MV",
                    base.table.fqn()
                ));
            }
        }
        if target_columns.is_empty() {
            return Err("IMV rewrite snapshot target has no SQL column facts".to_string());
        }
        Ok(Self {
            target,
            target_binding,
            mv_id,
            base_snapshots,
            previous_snapshot_ids,
            previous_table_uuids,
            target_snapshot_id,
            target_table_uuid,
            target_columns,
            schema_contract,
            aggregate_execution,
        })
    }

    pub(crate) fn aggregate_shape_and_layout_for_execution(
        &self,
    ) -> Result<(SqlImvAggregateShape, SqlImvAggregateLayout), String> {
        self.aggregate_execution
            .as_ref()
            .map(|layout| (layout.shape.clone(), layout.layout.clone()))
            .ok_or_else(|| {
                format!(
                    "IMV rewrite snapshot for {} has no aggregate execution layout",
                    self.target.fqn()
                )
            })
    }

    pub(crate) fn base_snapshot_for_identity(
        &self,
        table: &novarocks_catalog::identifier::TableIdentity,
    ) -> Option<&SqlImvBaseSnapshot> {
        self.base_snapshots.iter().find(|base| {
            base.table.catalog.eq_ignore_ascii_case(&table.catalog)
                && base.table.namespace.eq_ignore_ascii_case(&table.namespace)
                && base.table.table.eq_ignore_ascii_case(&table.table)
        })
    }

    pub(crate) fn base_snapshot_for_parts(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<&SqlImvBaseSnapshot> {
        self.base_snapshots.iter().find(|base| {
            base.table.catalog.eq_ignore_ascii_case(catalog)
                && base.table.namespace.eq_ignore_ascii_case(namespace)
                && base.table.table.eq_ignore_ascii_case(table)
        })
    }
}

#[cfg(test)]
pub(crate) fn test_target_binding() -> SqlTableBindingId {
    use std::num::{NonZeroU32, NonZeroU64};

    SqlTableBindingId::new(
        crate::sql::binding::SqlTableBindingScopeId::new(NonZeroU64::new(1).unwrap()),
        NonZeroU32::new(1).unwrap(),
    )
}

/// SQL-only incremental IMV fixture for rewrite-rule tests that need an
/// extension payload but do not exercise application persistence conversion.
///
/// The prior and admitted snapshots deliberately describe one exact
/// incremental window. Tests that exercise delta or version rewriting must
/// never rely on a synthetic first-refresh fallback.
#[cfg(test)]
pub(crate) fn test_incremental_snapshot() -> Arc<SqlImvRewriteSnapshot> {
    let base = novarocks_catalog::identifier::TableIdentity::new("ice", "db", "b");
    let target = novarocks_catalog::identifier::TableIdentity::new("ice", "db", "mv");
    let mut previous_snapshot_ids = BTreeMap::new();
    previous_snapshot_ids.insert(base.fqn(), 11);
    let mut previous_table_uuids = BTreeMap::new();
    previous_table_uuids.insert(base.fqn(), "uuid-b".to_string());
    Arc::new(
        SqlImvRewriteSnapshot::from_frozen_parts(
            target,
            test_target_binding(),
            1,
            Arc::from(vec![SqlImvBaseSnapshot {
                table: base,
                snapshot_id: 22,
                table_uuid: "uuid-b".to_string(),
            }]),
            previous_snapshot_ids,
            previous_table_uuids,
            Some(1),
            "target-uuid".to_string(),
            Arc::from(vec![novarocks_catalog::schema::ColumnDef {
                name: "k".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }]),
            Arc::new(SqlImvSchemaContract {
                bases: Vec::new(),
                output_columns: Vec::new(),
                join: None,
                aggregate: None,
                branch: None,
                target: SqlImvTargetContract {
                    visible_columns: Vec::new(),
                    hidden_apply_key: SqlImvHiddenApplyKey {
                        column_name: "__nova_base_row_id".to_string(),
                        source: crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId,
                    },
                    partition: None,
                },
            }),
            None,
        )
        .expect("SQL-only test IMV snapshot"),
    )
}

/// SQL-only scan fixture for rewrite-rule tests.  Test plans must exercise the
/// same tokenized scan vocabulary as production compiler artifacts; connector
/// table metadata belongs to application-owned preparation tests.
#[cfg(test)]
pub(crate) fn test_scan_source(kind: crate::sql::planner::table::SqlScanKind) -> ScanSource {
    test_scan_source_for("ice", "db", "b", kind)
}

/// SQL-only scan fixture with an explicit canonical table identity. Tests
/// comparing physical table identity must not collapse unrelated tables into
/// the shared default fixture identity.
#[cfg(test)]
pub(crate) fn test_scan_source_for(
    catalog: &str,
    namespace: &str,
    table: &str,
    kind: crate::sql::planner::table::SqlScanKind,
) -> ScanSource {
    ScanSource::Sql(crate::sql::planner::table::SqlScanSource::new(
        test_target_binding(),
        crate::sql::planner::table::SqlTableIdentity {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        },
        kind,
    ))
}

#[cfg(test)]
pub(crate) fn test_data_scan_source() -> ScanSource {
    test_scan_source(crate::sql::planner::table::SqlScanKind::Data {
        version: crate::sql::planner::table::SqlTableVersionSelector::Current,
    })
}

#[cfg(test)]
pub(crate) fn test_data_scan_source_for(catalog: &str, namespace: &str, table: &str) -> ScanSource {
    test_scan_source_for(
        catalog,
        namespace,
        table,
        crate::sql::planner::table::SqlScanKind::Data {
            version: crate::sql::planner::table::SqlTableVersionSelector::Current,
        },
    )
}

#[cfg(test)]
pub(crate) fn test_delta_scan_source(from_snapshot_id: i64, to_snapshot_id: i64) -> ScanSource {
    test_scan_source(crate::sql::planner::table::SqlScanKind::Delta {
        from_snapshot_id,
        to_snapshot_id,
    })
}

/// Build aggregate-refresh facts without persisted records or connector
/// metadata. Rule tests vary these compiler-facing values directly.
#[cfg(test)]
pub(crate) fn test_aggregate_snapshot(
    state_columns: Vec<SqlImvAggregateStateColumnContract>,
    partition: Option<SqlImvPartitionContract>,
    branch: Option<SqlImvBranchContract>,
) -> Arc<SqlImvRewriteSnapshot> {
    let mut snapshot = (*test_incremental_snapshot()).clone();
    snapshot.schema_contract = Arc::new(SqlImvSchemaContract {
        bases: vec![SqlImvBaseContract {
            table_fqn: "ice.db.b".to_string(),
            alias_at_create: None,
            fields: vec![
                SqlImvBaseField {
                    field_id: 1,
                    name_at_create: "k".to_string(),
                },
                SqlImvBaseField {
                    field_id: 2,
                    name_at_create: "v".to_string(),
                },
            ],
        }],
        output_columns: Vec::new(),
        join: None,
        aggregate: Some(SqlImvAggregateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: state_columns.clone(),
        }),
        branch,
        target: SqlImvTargetContract {
            visible_columns: vec![
                SqlImvTargetVisibleColumn {
                    output_name: "k".to_string(),
                    target_field_id: 100,
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: 101,
                },
            ],
            hidden_apply_key: SqlImvHiddenApplyKey {
                column_name: "__row_id__".to_string(),
                source: crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId,
            },
            partition,
        },
    });
    snapshot.aggregate_execution = Some(SqlImvAggregateExecutionLayout {
        shape: SqlImvAggregateShape {
            group_key_count: 1,
            visible_outputs: vec![
                crate::sql::mv_refresh::VisibleAggregateOutput::GroupKey(0),
                crate::sql::mv_refresh::VisibleAggregateOutput::Aggregate(0),
            ],
        },
        layout: SqlImvAggregateLayout {
            row_id_column_name: "__row_id__".to_string(),
            visible_columns: vec![
                SqlImvAggregateVisibleColumn {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
                SqlImvAggregateVisibleColumn {
                    name: "s".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                },
            ],
            state_columns: state_columns
                .iter()
                .enumerate()
                .map(|(index, column)| SqlImvAggregateStateColumn {
                    name: column.column_name.clone(),
                    data_type: if column.type_signature == "long" {
                        arrow::datatypes::DataType::Int64
                    } else {
                        arrow::datatypes::DataType::Binary
                    },
                    nullable: column.role == SqlImvAggregateStateRoleContract::Single,
                    visible_source_index: 1,
                    aggregate_index: index,
                    function: crate::sql::mv_refresh::AggregateFunctionKind::Sum,
                    state_role: match column.role {
                        SqlImvAggregateStateRoleContract::Single => {
                            SqlImvAggregateStateRole::Single
                        }
                        SqlImvAggregateStateRoleContract::RetractionCount => {
                            SqlImvAggregateStateRole::RetractionCount
                        }
                    },
                    count_star: false,
                })
                .collect(),
            group_key_source_indexes: vec![0],
            physical_column_names: state_columns
                .iter()
                .map(|column| column.column_name.clone())
                .collect(),
            aggregate_input_types: state_columns
                .iter()
                .map(|_| Some(arrow::datatypes::DataType::Int64))
                .collect(),
        },
    });
    let mut target_columns = vec![
        novarocks_catalog::schema::ColumnDef {
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "s".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "__row_id__".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ];
    target_columns.extend(state_columns.iter().map(|column| {
        novarocks_catalog::schema::ColumnDef {
            name: column.column_name.clone(),
            data_type: if column.type_signature == "long" {
                arrow::datatypes::DataType::Int64
            } else {
                arrow::datatypes::DataType::Binary
            },
            nullable: column.role == SqlImvAggregateStateRoleContract::Single,
            write_default: None,
            logical_type: None,
        }
    }));
    if let Some(branch) = snapshot.schema_contract.branch.as_ref() {
        target_columns.push(novarocks_catalog::schema::ColumnDef {
            name: branch.branch_id_column_name.clone(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        });
    }
    snapshot.target_columns = Arc::from(target_columns);
    Arc::new(snapshot)
}

/// SQL-owned join fixture for rewrite rules. It has no persistence, provider,
/// or application-context dependency.
#[cfg(test)]
pub(crate) fn test_join_snapshot(aggregate: bool) -> Arc<SqlImvRewriteSnapshot> {
    let qualified =
        |table_fqn: &str, qualifier_at_create: &str, field_id| SqlImvQualifiedFieldLineage {
            table_fqn: table_fqn.to_string(),
            qualifier_at_create: qualifier_at_create.to_string(),
            field_id,
        };
    let base_contract = |table_fqn: &str, alias_at_create: &str| SqlImvBaseContract {
        table_fqn: table_fqn.to_string(),
        alias_at_create: Some(alias_at_create.to_string()),
        fields: vec![
            SqlImvBaseField {
                field_id: 1,
                name_at_create: "k".to_string(),
            },
            SqlImvBaseField {
                field_id: 2,
                name_at_create: "v".to_string(),
            },
        ],
    };
    let state_columns = vec![
        SqlImvAggregateStateColumnContract {
            column_name: "__agg_state_s".to_string(),
            type_signature: "binary".to_string(),
            role: SqlImvAggregateStateRoleContract::Single,
        },
        SqlImvAggregateStateColumnContract {
            column_name: "__agg_state___ivm_row_count".to_string(),
            type_signature: "long".to_string(),
            role: SqlImvAggregateStateRoleContract::RetractionCount,
        },
    ];
    let schema_contract = Arc::new(SqlImvSchemaContract {
        bases: vec![
            base_contract("ice.db.l", "l"),
            base_contract("ice.db.r", "r"),
        ],
        output_columns: vec![
            SqlImvOutputColumnLineage {
                expression: SqlImvExpressionLineage {
                    kind: SqlImvExpressionKind::Column,
                    referenced_base_field_ids: Vec::new(),
                    referenced_base_fields: vec![qualified("ice.db.l", "l", 1)],
                },
            },
            SqlImvOutputColumnLineage {
                expression: SqlImvExpressionLineage {
                    kind: SqlImvExpressionKind::Column,
                    referenced_base_field_ids: Vec::new(),
                    referenced_base_fields: vec![qualified("ice.db.r", "r", 2)],
                },
            },
        ],
        join: Some(SqlImvJoinContract {
            kind: SqlImvJoinContractKind::InnerEquiJoin,
            predicates: vec![SqlImvJoinPredicateLineage {
                left: qualified("ice.db.l", "l", 1),
                right: qualified("ice.db.r", "r", 1),
            }],
        }),
        aggregate: aggregate.then(|| SqlImvAggregateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: state_columns.clone(),
        }),
        branch: Some(SqlImvBranchContract {
            branch_id_column_name: "__branch_id__".to_string(),
        }),
        target: SqlImvTargetContract {
            visible_columns: vec![
                SqlImvTargetVisibleColumn {
                    output_name: "k".to_string(),
                    target_field_id: 100,
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: 101,
                },
            ],
            hidden_apply_key: SqlImvHiddenApplyKey {
                column_name: "__row_id__".to_string(),
                source: crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId,
            },
            partition: None,
        },
    });
    let aggregate_execution = aggregate.then(|| SqlImvAggregateExecutionLayout {
        shape: SqlImvAggregateShape {
            group_key_count: 1,
            visible_outputs: vec![
                crate::sql::mv_refresh::VisibleAggregateOutput::GroupKey(0),
                crate::sql::mv_refresh::VisibleAggregateOutput::Aggregate(0),
            ],
        },
        layout: SqlImvAggregateLayout {
            row_id_column_name: "__row_id__".to_string(),
            visible_columns: vec![
                SqlImvAggregateVisibleColumn {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
                SqlImvAggregateVisibleColumn {
                    name: "s".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                },
            ],
            state_columns: state_columns
                .iter()
                .enumerate()
                .map(|(aggregate_index, column)| SqlImvAggregateStateColumn {
                    name: column.column_name.clone(),
                    data_type: if column.type_signature == "long" {
                        arrow::datatypes::DataType::Int64
                    } else {
                        arrow::datatypes::DataType::Binary
                    },
                    nullable: column.role == SqlImvAggregateStateRoleContract::Single,
                    visible_source_index: 1,
                    aggregate_index,
                    function: crate::sql::mv_refresh::AggregateFunctionKind::Sum,
                    state_role: match column.role {
                        SqlImvAggregateStateRoleContract::Single => {
                            SqlImvAggregateStateRole::Single
                        }
                        SqlImvAggregateStateRoleContract::RetractionCount => {
                            SqlImvAggregateStateRole::RetractionCount
                        }
                    },
                    count_star: false,
                })
                .collect(),
            group_key_source_indexes: vec![0],
            physical_column_names: state_columns
                .iter()
                .map(|column| column.column_name.clone())
                .collect(),
            aggregate_input_types: state_columns
                .iter()
                .map(|_| Some(arrow::datatypes::DataType::Int64))
                .collect(),
        },
    });
    let mut target_columns = vec![
        novarocks_catalog::schema::ColumnDef {
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "s".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "__row_id__".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "__branch_id__".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ];
    target_columns.extend(state_columns.iter().map(|column| {
        novarocks_catalog::schema::ColumnDef {
            name: column.column_name.clone(),
            data_type: if column.type_signature == "long" {
                arrow::datatypes::DataType::Int64
            } else {
                arrow::datatypes::DataType::Binary
            },
            nullable: column.role == SqlImvAggregateStateRoleContract::Single,
            write_default: None,
            logical_type: None,
        }
    }));
    Arc::new(
        SqlImvRewriteSnapshot::from_frozen_parts(
            novarocks_catalog::identifier::TableIdentity::new("ice", "db", "mv"),
            test_target_binding(),
            42,
            Arc::from(vec![
                SqlImvBaseSnapshot {
                    table: novarocks_catalog::identifier::TableIdentity::new("ice", "db", "l"),
                    snapshot_id: 22,
                    table_uuid: "uuid-l".to_string(),
                },
                SqlImvBaseSnapshot {
                    table: novarocks_catalog::identifier::TableIdentity::new("ice", "db", "r"),
                    snapshot_id: 44,
                    table_uuid: "uuid-r".to_string(),
                },
            ]),
            BTreeMap::from([("ice.db.l".to_string(), 11), ("ice.db.r".to_string(), 33)]),
            BTreeMap::from([
                ("ice.db.l".to_string(), "uuid-l".to_string()),
                ("ice.db.r".to_string(), "uuid-r".to_string()),
            ]),
            Some(99),
            "uuid-tgt".to_string(),
            Arc::from(target_columns),
            schema_contract,
            aggregate_execution,
        )
        .expect("SQL-only join test snapshot"),
    )
}

#[cfg(test)]
pub(crate) fn test_branch_union_snapshot() -> Arc<SqlImvRewriteSnapshot> {
    let mut snapshot = (*test_aggregate_snapshot(
        vec![
            SqlImvAggregateStateColumnContract {
                column_name: "__agg_state_s".to_string(),
                type_signature: "binary".to_string(),
                role: SqlImvAggregateStateRoleContract::Single,
            },
            SqlImvAggregateStateColumnContract {
                column_name: "__agg_state___ivm_row_count".to_string(),
                type_signature: "long".to_string(),
                role: SqlImvAggregateStateRoleContract::RetractionCount,
            },
        ],
        None,
        Some(SqlImvBranchContract {
            branch_id_column_name: "__branch_id__".to_string(),
        }),
    ))
    .clone();
    snapshot.schema_contract = Arc::new(SqlImvSchemaContract {
        bases: vec![SqlImvBaseContract {
            table_fqn: "ice.db.b".to_string(),
            alias_at_create: None,
            fields: vec![
                SqlImvBaseField {
                    field_id: 1,
                    name_at_create: "region".to_string(),
                },
                SqlImvBaseField {
                    field_id: 2,
                    name_at_create: "amount".to_string(),
                },
            ],
        }],
        output_columns: Vec::new(),
        join: None,
        aggregate: snapshot.schema_contract.aggregate.clone(),
        branch: snapshot.schema_contract.branch.clone(),
        target: SqlImvTargetContract {
            visible_columns: vec![
                SqlImvTargetVisibleColumn {
                    output_name: "region".to_string(),
                    target_field_id: 100,
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: 101,
                },
            ],
            hidden_apply_key: SqlImvHiddenApplyKey {
                column_name: "__row_id__".to_string(),
                source: crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId,
            },
            partition: None,
        },
    });
    if let Some(layout) = snapshot.aggregate_execution.as_mut() {
        layout.layout.visible_columns[0].name = "region".to_string();
        layout.layout.visible_columns[1].name = "s".to_string();
    }
    snapshot.target_columns = Arc::from(vec![
        novarocks_catalog::schema::ColumnDef {
            name: "region".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "s".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "__row_id__".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "__agg_state_s".to_string(),
            data_type: arrow::datatypes::DataType::Binary,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "__agg_state___ivm_row_count".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: "__branch_id__".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ]);
    Arc::new(snapshot)
}

/// SQL-only aggregate join fixture whose visible group key follows the
/// branch-union test plans. The base-table and target-state identities remain
/// the same immutable join snapshot facts.
#[cfg(test)]
pub(crate) fn test_region_join_snapshot() -> Arc<SqlImvRewriteSnapshot> {
    let mut snapshot = (*test_join_snapshot(true)).clone();
    Arc::make_mut(&mut snapshot.schema_contract)
        .target
        .visible_columns[0]
        .output_name = "region".to_string();
    if let Some(layout) = snapshot.aggregate_execution.as_mut() {
        layout.layout.visible_columns[0].name = "region".to_string();
    }
    snapshot.target_columns = Arc::from(
        snapshot
            .target_columns
            .iter()
            .cloned()
            .map(|mut column| {
                if column.name.eq_ignore_ascii_case("k") {
                    column.name = "region".to_string();
                }
                column
            })
            .collect::<Vec<_>>(),
    );
    Arc::new(snapshot)
}

/// The maximum number of successfully prepared candidates considered by one
/// statement. Failed or stale definitions do not consume this budget.
pub(crate) const MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES: usize = 16;

/// An optional-rewrite failure recorded by the SQL kernel. The application
/// owns logging policy and may render these diagnostics without handing the
/// compiler an ambient logger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvRewriteDiagnostic {
    pub(crate) mv_id: Option<i64>,
    pub(crate) message: String,
}

pub(crate) struct SqlMvRewritePreparation {
    pub(crate) candidates: Vec<MvRewriteCandidate>,
    pub(crate) diagnostics: Vec<SqlMvRewriteDiagnostic>,
}

/// One captured base-table identity at statement admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MvRewriteBaseTableState {
    Resolved {
        snapshot_id: Option<i64>,
        table_uuid: Option<String>,
    },
    Unavailable(String),
}

/// Immutable facts required to assess one persisted MV as a rewrite candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvRewriteDefinition {
    pub(crate) mv_id: i64,
    pub(crate) select_sql: String,
    pub(crate) base_table_refs: Vec<String>,
    pub(crate) storage_engine: String,
    pub(crate) target_catalog: Option<String>,
    pub(crate) target_namespace: Option<String>,
    pub(crate) target_table: Option<String>,
    pub(crate) last_refresh_snapshots: BTreeMap<String, i64>,
    pub(crate) last_refresh_table_uuids: BTreeMap<String, String>,
    /// Per-base-table reads (including failures) captured while admission
    /// froze this definition. The map is keyed by canonical `cat.ns.tbl`.
    pub(crate) base_table_states: BTreeMap<String, MvRewriteBaseTableState>,
}

/// Repository-order-preserving MV definition snapshot for one compiler request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MvRewriteDefinitionIndex {
    definitions: Vec<MvRewriteDefinition>,
}

impl MvRewriteDefinitionIndex {
    pub(crate) fn new(definitions: Vec<MvRewriteDefinition>) -> Self {
        Self { definitions }
    }

    pub(crate) fn definitions(&self) -> &[MvRewriteDefinition] {
        &self.definitions
    }
}

struct PreparedMvRewriteCandidate {
    mv_name: String,
    mv: SpjgDescriptor,
    mv_scalars: crate::sql::optimizer::scalar::ScalarArena,
    target_database: String,
    target_table: crate::sql::planner::table::TableDef,
}

/// Prepare optional MV rewrite candidates from one immutable, repository-order
/// definition index. This is deliberately SQL-owned: application admission
/// freezes definitions and base-table observations, while parse/analyze,
/// descriptor construction, statistics, and warn-and-skip selection happen in
/// the canonical compiler kernel.
pub(crate) fn prepare_candidates(
    definitions: &MvRewriteDefinitionIndex,
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
    statistics_context: &dyn SqlStatisticsSnapshot,
    query_stats: &mut SqlStatisticsPlan,
    optimizer_settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
) -> SqlMvRewritePreparation {
    if !optimizer_settings.mv_rewrite_enabled() {
        return SqlMvRewritePreparation {
            candidates: Vec::new(),
            diagnostics: Vec::new(),
        };
    }

    let mut query_fqns = Vec::new();
    collect_iceberg_fqns(logical, &mut query_fqns);
    if query_fqns.is_empty() {
        return SqlMvRewritePreparation {
            candidates: Vec::new(),
            diagnostics: Vec::new(),
        };
    }

    let mut candidates = Vec::new();
    let mut diagnostics = Vec::new();
    for definition in definitions.definitions() {
        if candidates.len() >= MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES {
            diagnostics.push(SqlMvRewriteDiagnostic {
                mv_id: None,
                message: format!(
                    "mv rewrite: candidate cap {MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES} reached, rest skipped"
                ),
            });
            break;
        }
        if definition.storage_engine != "iceberg"
            || !definition
                .base_table_refs
                .iter()
                .any(|base| query_fqns.contains(base))
        {
            continue;
        }
        match build_candidate(
            analyzer_catalog,
            current_database,
            definition,
            factory,
            functions,
        ) {
            Ok(Some(candidate)) => {
                let (label, stats) = statistics_context
                    .collect_table_statistics(&candidate.target_database, &candidate.target_table);
                let target_stats_ref = query_stats.add_stats(label, stats);
                candidates.push(MvRewriteCandidate {
                    mv_name: candidate.mv_name,
                    mv: candidate.mv,
                    mv_scalars: candidate.mv_scalars,
                    target_database: candidate.target_database,
                    target_table: candidate.target_table,
                    target_stats_ref,
                });
            }
            Ok(None) => {}
            Err(error) => diagnostics.push(SqlMvRewriteDiagnostic {
                mv_id: Some(definition.mv_id),
                message: format!("mv rewrite: skipping frozen candidate: {error}"),
            }),
        }
    }
    SqlMvRewritePreparation {
        candidates,
        diagnostics,
    }
}

fn build_candidate(
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    definition: &MvRewriteDefinition,
    factory: &mut ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
) -> Result<Option<PreparedMvRewriteCandidate>, String> {
    if definition.last_refresh_snapshots.is_empty() || !definition_is_fresh(definition)? {
        return Ok(None);
    }

    let select = parse_select_query(&definition.select_sql)?;
    let (resolved, ctes, returned) =
        crate::sql::analyzer::analyze_with_factory_and_function_catalog(
            &select,
            analyzer_catalog,
            current_database,
            factory.clone(),
            functions,
        )?;
    let mut returned = returned;
    let mv_logical = crate::sql::planner::plan_query(resolved, ctes, &mut returned)?;
    let mut mv_scalars = crate::sql::optimizer::scalar::ScalarArena::new();
    let mv_opt_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &mv_logical,
        &mut mv_scalars,
    )?;
    let mv = SpjgDescriptor::from_opt_expr(&mv_opt_expr, &mut mv_scalars)?;
    if mv.joins.is_some() {
        return Ok(None);
    }
    let Some(scan_fqn) = scan_fqn(&mv.table.source) else {
        return Ok(None);
    };
    if !definition.base_table_refs.contains(&scan_fqn) {
        return Err(format!(
            "mv select resolved to {scan_fqn}, not in recorded base refs"
        ));
    }
    let (Some(catalog), Some(namespace), Some(table)) = (
        &definition.target_catalog,
        &definition.target_namespace,
        &definition.target_table,
    ) else {
        return Ok(None);
    };
    let target_table = analyzer_catalog
        .resolve_table_for_analysis(Some(catalog), namespace, table)?
        .planner;
    let mut names = mv
        .outputs
        .iter()
        .map(|output| output.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Ok(None);
    }
    *factory = returned;
    Ok(Some(PreparedMvRewriteCandidate {
        mv_name: table.to_string(),
        mv,
        mv_scalars,
        target_database: namespace.to_string(),
        target_table,
    }))
}

fn parse_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(|error| format!("stored MV SELECT normalize error: {error}"))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("stored MV SQL parse error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn definition_is_fresh(definition: &MvRewriteDefinition) -> Result<bool, String> {
    for base in &definition.base_table_refs {
        let Some(pinned_snapshot) = definition.last_refresh_snapshots.get(base) else {
            return Ok(false);
        };
        match definition.base_table_states.get(base) {
            Some(MvRewriteBaseTableState::Resolved {
                snapshot_id,
                table_uuid,
            }) => {
                if *snapshot_id != Some(*pinned_snapshot) {
                    return Ok(false);
                }
                if let Some(pinned_uuid) = definition.last_refresh_table_uuids.get(base)
                    && table_uuid.as_deref() != Some(pinned_uuid.as_str())
                {
                    return Ok(false);
                }
            }
            Some(MvRewriteBaseTableState::Unavailable(error)) => {
                return Err(format!("read frozen base table {base}: {error}"));
            }
            None => return Err(format!("missing frozen base table state for {base}")),
        }
    }
    Ok(true)
}

fn collect_iceberg_fqns(plan: &LogicalPlanNode, output: &mut Vec<String>) {
    if let crate::sql::planner::logical::LogicalPlanKind::Scan(scan) = &plan.kind
        && let Some(fqn) = scan_fqn(&scan.table.source)
    {
        if !output.contains(&fqn) {
            output.push(fqn);
        }
    }
    for child in &plan.children {
        collect_iceberg_fqns(child, output);
    }
}

fn scan_fqn(source: &ScanSource) -> Option<String> {
    match source {
        ScanSource::Sql(source) => match source.kind {
            crate::sql::planner::table::SqlScanKind::Data { .. }
            | crate::sql::planner::table::SqlScanKind::FrozenInputSet { .. } => Some(format!(
                "{}.{}.{}",
                source.table.catalog, source.table.namespace, source.table.table
            )),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frozen_definition(state: MvRewriteBaseTableState) -> MvRewriteDefinition {
        MvRewriteDefinition {
            mv_id: 1,
            select_sql: "select 1".to_string(),
            base_table_refs: vec!["iceberg.db.base".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("iceberg".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv_target".to_string()),
            last_refresh_snapshots: BTreeMap::from([("iceberg.db.base".to_string(), 42)]),
            last_refresh_table_uuids: BTreeMap::from([(
                "iceberg.db.base".to_string(),
                "original-uuid".to_string(),
            )]),
            base_table_states: BTreeMap::from([("iceberg.db.base".to_string(), state)]),
        }
    }

    #[test]
    fn sqlx1_mv_rewrite_definition_index_preserves_application_order() {
        let index = MvRewriteDefinitionIndex::new(vec![
            MvRewriteDefinition {
                mv_id: 7,
                select_sql: "select 1".to_string(),
                base_table_refs: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                last_refresh_snapshots: BTreeMap::new(),
                last_refresh_table_uuids: BTreeMap::new(),
                base_table_states: BTreeMap::new(),
            },
            MvRewriteDefinition {
                mv_id: 3,
                select_sql: "select 2".to_string(),
                base_table_refs: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                last_refresh_snapshots: BTreeMap::new(),
                last_refresh_table_uuids: BTreeMap::new(),
                base_table_states: BTreeMap::new(),
            },
        ]);

        assert_eq!(
            index
                .definitions()
                .iter()
                .map(|definition| definition.mv_id)
                .collect::<Vec<_>>(),
            vec![7, 3]
        );
    }

    #[test]
    fn sqlx2_mv_frozen_snapshot_and_uuid_decide_candidate_freshness() {
        let fresh = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(42),
            table_uuid: Some("original-uuid".to_string()),
        });
        let stale = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(43),
            table_uuid: Some("original-uuid".to_string()),
        });
        let recreated = frozen_definition(MvRewriteBaseTableState::Resolved {
            snapshot_id: Some(42),
            table_uuid: Some("replacement-uuid".to_string()),
        });

        assert_eq!(definition_is_fresh(&fresh), Ok(true));
        assert_eq!(definition_is_fresh(&stale), Ok(false));
        assert_eq!(definition_is_fresh(&recreated), Ok(false));
    }

    #[test]
    fn sqlx2_mv_frozen_read_failure_stays_a_warn_and_skip_input() {
        let unavailable = frozen_definition(MvRewriteBaseTableState::Unavailable(
            "catalog unavailable".to_string(),
        ));

        assert!(matches!(
            definition_is_fresh(&unavailable),
            Err(error) if error.contains("catalog unavailable")
        ));
    }

    #[test]
    fn sqlx2_mv_candidate_limit_is_sixteen_successes() {
        assert_eq!(MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES, 16);
    }
}
