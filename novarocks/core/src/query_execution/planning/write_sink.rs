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

//! Application projection for SQL terminal write contracts.
//!
//! The compiler receives the resulting [`SqlWritePlanInput`] only. Provider
//! metadata never crosses this boundary: the request-local binding retains a
//! sealed write preparation and SQL sees only its Arrow layout and field
//! tokens.

use novarocks_catalog::schema::ColumnDef;

use super::bindings::{
    QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
    QueryWriteTargetAdmission,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorWriteInputShape, ConnectorWritePreparation,
};
use novarocks_sql::analysis::TypedExpr;
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planner::distributed::write::contract::{
    ConnectorWriteInputBinding, SqlWritePlanInput, SqlWriteSinkContract, SqlWriteSinkMode,
    SqlWriteSinkTargetContract, SqlWriteTargetField,
};
use novarocks_sql::planner::table::{ScanSource, SqlScanKind, SqlScanSource, TableDef};
use novarocks_sql::planning::dml::{
    DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget, DmlWriteTargetField,
};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;

/// Project one already-admitted write target into the SQL compiler boundary.
///
/// The token must belong to `bindings` and name a materialization that retains
/// the same exact planning lease selected at admission.  A failure is terminal
/// for submission: this factory never substitutes a new table, metadata
/// version, or connector generation.
pub(crate) fn sql_write_plan_input_from_admitted_binding(
    bindings: &QueryTableBindingStore,
    binding: SqlTableBindingId,
    mode: SqlWriteSinkMode,
    input_columns: Vec<ColumnDef>,
    input: ConnectorWriteInputBinding,
    root_output_exprs: Option<Vec<TypedExpr>>,
) -> Result<SqlWritePlanInput, String> {
    let captured = bindings.binding(binding)?;
    captured.admission.exact_planning_lease().map_err(|_| {
        "SQL write target binding is missing its admission planning lease".to_string()
    })?;
    let preparation = &admitted_write_target(&captured)?.preparation;
    preparation
        .validate()
        .map_err(|error| format!("validate SQL write preparation: {error}"))?;
    validate_mode(mode, preparation.input())?;
    let target = SqlWriteSinkTargetContract::try_new(
        binding,
        novarocks_sql::planner::table::SqlTableIdentity {
            catalog: captured.resolved.catalog.identity.catalog.clone(),
            namespace: captured.resolved.catalog.identity.namespace.clone(),
            table: captured.resolved.catalog.identity.table.clone(),
        },
        preparation
            .input()
            .fields()
            .into_iter()
            .map(|field| SqlWriteTargetField {
                token: field.token(),
                column: ColumnDef {
                    name: field.field().name().to_string(),
                    data_type: field.field().data_type().clone(),
                    nullable: field.field().is_nullable(),
                    write_default: None,
                    logical_type: None,
                },
                is_hidden: false,
            })
            .collect(),
    )?;
    Ok(SqlWritePlanInput {
        contract: SqlWriteSinkContract::try_new(mode, target, input_columns)?,
        input,
        root_output_exprs,
    })
}

/// Construct the complete SQL terminal contract for one admitted Iceberg
/// target.  Callers deliberately cannot provide a hand-made input schema:
/// hidden row-lineage and position-delete columns are derived from the exact
/// metadata retained beside the request-local token.
pub(crate) fn sql_write_plan_input_for_admitted_target(
    bindings: &QueryTableBindingStore,
    binding: SqlTableBindingId,
    mode: SqlWriteSinkMode,
    input: ConnectorWriteInputBinding,
    root_output_exprs: Option<Vec<TypedExpr>>,
) -> Result<SqlWritePlanInput, String> {
    let captured = bindings.binding(binding)?;
    let preparation = &admitted_write_target(&captured)?.preparation;
    let input_columns = admitted_write_input_columns(preparation)?;
    sql_write_plan_input_from_admitted_binding(
        bindings,
        binding,
        mode,
        input_columns,
        input,
        root_output_exprs,
    )
}

/// Project an admitted write target into the opaque DML planning boundary.
///
/// This is intentionally separate from the legacy internal compiler helper:
/// new application entry points must not receive the private planner write
/// contract.  The request-local binding keeps the same exact planning lease
/// and provider preparation through terminal planning and fragment setup.
pub(crate) fn dml_write_plan_input_for_admitted_target(
    bindings: &QueryTableBindingStore,
    binding: SqlTableBindingId,
    mode: DmlWriteSinkMode,
    input: novarocks_sql::plan_read::ConnectorWriteInputBinding,
) -> Result<DmlWritePlanInput, String> {
    let captured = bindings.binding(binding)?;
    captured.admission.exact_planning_lease().map_err(|_| {
        "SQL write target binding is missing its admission planning lease".to_string()
    })?;
    let preparation = &admitted_write_target(&captured)?.preparation;
    preparation
        .validate()
        .map_err(|error| format!("validate SQL write preparation: {error}"))?;
    validate_mode(mode.into(), preparation.input())?;
    DmlWritePlanInput::try_new(
        mode,
        DmlWriteTarget {
            binding,
            catalog: captured.resolved.catalog.identity.catalog.clone(),
            namespace: captured.resolved.catalog.identity.namespace.clone(),
            table: captured.resolved.catalog.identity.table.clone(),
            fields: preparation
                .input()
                .fields()
                .into_iter()
                .map(|field| DmlWriteTargetField {
                    token: field.token(),
                    column: ColumnDef {
                        name: field.field().name().to_string(),
                        data_type: field.field().data_type().clone(),
                        nullable: field.field().is_nullable(),
                        write_default: None,
                        logical_type: None,
                    },
                    is_hidden: false,
                })
                .collect(),
        },
        admitted_write_input_columns(preparation)?,
        input,
    )
}

/// Reserve a SQL write token for a sealed Provider preparation.  The exact
/// planning lease and opaque table handle remain paired by the preparation;
/// this function does not inspect either provider-owned value.
pub(crate) fn admit_prepared_connector_write_target(
    bindings: &QueryTableBindingStore,
    identity: novarocks_sql::planner::table::SqlTableIdentity,
    preparation: ConnectorWritePreparation,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    preparation
        .validate()
        .map_err(|error| format!("validate connector write preparation: {error}"))?;
    let descriptor = planning_lease.binding().descriptor();
    if !descriptor
        .instance_id
        .as_str()
        .eq_ignore_ascii_case(preparation.table().owner().as_str())
    {
        return Err(
            "connector write preparation does not match its admission planning lease".to_string(),
        );
    }
    let columns = admitted_write_input_columns(&preparation)?;
    let key = QueryTableBindingKey::write_target(
        &identity.catalog,
        &identity.namespace,
        &identity.table,
        preparation.digest(),
    );
    bindings.resolve_or_insert_with_id(key, |binding| {
        let planner = novarocks_sql::planner::table::TableDef {
            name: identity.table.clone(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: novarocks_sql::planner::table::ScanSource::Sql(
                novarocks_sql::planner::table::SqlScanSource::new(
                    binding,
                    identity.clone(),
                    novarocks_sql::planner::table::SqlScanKind::Data {
                        version: novarocks_sql::planner::table::SqlTableVersionSelector::Current,
                    },
                ),
            ),
        };
        Ok(QueryTableBinding {
            resolved: novarocks_sql::catalog::ResolvedAnalyzerTable::from_planner(
                Some(&identity.catalog),
                &identity.namespace,
                planner,
            ),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Exact(planning_lease),
            // This token represents a terminal write target, not a read
            // source.  Do not invent a synthetic Iceberg file scan merely to
            // prove admission; the provider-owned write table below is the
            // exact SQL write-target contract.
            scan_materialization: None,
            mv_target_read: None,
            write_target_admission: Some(QueryWriteTargetAdmission {
                preparation: preparation.clone(),
            }),
            frozen_snapshot_materializations: std::collections::BTreeMap::new(),
            admitted_change_scans: std::collections::BTreeMap::new(),
        })
    })
}

/// Reserve a terminal write token for a synthetic frozen connector identity.
/// The SQL-facing identity carries no provider handle; the preparation and
/// exact lease remain in the application-owned binding store.
pub(crate) fn admit_prepared_frozen_connector_write_target(
    bindings: &QueryTableBindingStore,
    identity: FrozenConnectorScanIdentity,
    preparation: ConnectorWritePreparation,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    admit_prepared_connector_write_target(
        bindings,
        novarocks_sql::planner::table::SqlTableIdentity {
            catalog: identity.catalog().to_string(),
            namespace: identity.namespace().to_string(),
            table: identity.table().to_string(),
        },
        preparation,
        planning_lease,
    )
}

fn admitted_write_target(
    binding: &QueryTableBinding,
) -> Result<&crate::query_execution::planning::bindings::QueryWriteTargetAdmission, String> {
    binding
        .write_target_admission
        .as_ref()
        .ok_or_else(|| "SQL write target binding is missing admitted write facts".to_string())
}

fn admitted_write_input_columns(
    preparation: &ConnectorWritePreparation,
) -> Result<Vec<ColumnDef>, String> {
    Ok(preparation
        .input()
        .fields()
        .into_iter()
        .map(|field| ColumnDef {
            name: field.field().name().to_string(),
            data_type: field.field().data_type().clone(),
            nullable: field.field().is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect())
}

fn validate_mode(mode: SqlWriteSinkMode, input: &ConnectorWriteInputShape) -> Result<(), String> {
    let matches = matches!(
        (mode, input),
        (
            SqlWriteSinkMode::Data,
            ConnectorWriteInputShape::Data { .. }
        ) | (
            SqlWriteSinkMode::RowLineageData,
            ConnectorWriteInputShape::RowLineage { .. }
        ) | (
            SqlWriteSinkMode::PositionDeletes,
            ConnectorWriteInputShape::PositionDelete { .. }
        ) | (
            SqlWriteSinkMode::DeletionVectors,
            ConnectorWriteInputShape::DeletionVector { .. }
        ) | (
            SqlWriteSinkMode::EqualityDeletes,
            ConnectorWriteInputShape::EqualityDelete { .. }
        )
    );
    matches.then_some(()).ok_or_else(|| {
        "SQL write sink mode does not match its Provider-signed input shape".to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    use crate::query_execution::planning::bindings::{QueryTableBinding, QueryTableBindingKey};
    use novarocks_sql::catalog::ResolvedAnalyzerTable;
    use novarocks_sql::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };

    #[test]
    fn sqlx2_write_binding_factory_rejects_cross_request_token() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let second = QueryTableBindingStore::try_new().expect("second store");
        let token = first
            .resolve_or_insert_with_id(
                QueryTableBindingKey::strict_base("iceberg", "analytics", "orders"),
                |binding| {
                    Ok(QueryTableBinding::local(
                        ResolvedAnalyzerTable::from_planner(
                            Some("iceberg"),
                            "analytics",
                            TableDef {
                                name: "orders".to_string(),
                                columns: vec![ColumnDef {
                                    name: "order_id".to_string(),
                                    data_type: DataType::Int64,
                                    nullable: false,
                                    write_default: None,
                                    logical_type: None,
                                }],
                                iceberg_row_lineage_metadata_columns: Vec::new(),
                                source: ScanSource::Sql(SqlScanSource::new(
                                    binding,
                                    SqlTableIdentity {
                                        catalog: "iceberg".to_string(),
                                        namespace: "analytics".to_string(),
                                        table: "orders".to_string(),
                                    },
                                    SqlScanKind::ConnectorRead,
                                )),
                            },
                        ),
                        binding,
                    ))
                },
            )
            .expect("first token");

        let error = sql_write_plan_input_from_admitted_binding(
            &second,
            token,
            SqlWriteSinkMode::Data,
            Vec::new(),
            ConnectorWriteInputBinding::RootOutputByOrdinal,
            None,
        )
        .expect_err("foreign token must fail");
        assert!(error.contains("different request"));
    }
}
