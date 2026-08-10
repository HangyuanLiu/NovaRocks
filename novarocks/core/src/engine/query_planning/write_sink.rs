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
use crate::sql::analysis::TypedExpr;
use crate::sql::binding::SqlTableBindingId;
use crate::sql::planner::distributed::write::contract::{
    ConnectorWriteInputBinding, SqlWritePlanInput, SqlWriteSinkContract, SqlWriteSinkMode,
    SqlWriteSinkTargetContract, SqlWriteTargetField,
};
use crate::sql::planner::table::{ScanSource, SqlScanKind, SqlScanSource, TableDef};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorWriteInputShape, ConnectorWritePreparation,
};

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
        crate::sql::planner::table::SqlTableIdentity {
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

/// Reserve a SQL write token for a sealed Provider preparation.  The exact
/// planning lease and opaque table handle remain paired by the preparation;
/// this function does not inspect either provider-owned value.
pub(crate) fn admit_prepared_connector_write_target(
    bindings: &QueryTableBindingStore,
    identity: crate::sql::planner::table::SqlTableIdentity,
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
        let planner = crate::sql::planner::table::TableDef {
            name: identity.table.clone(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::sql::planner::table::ScanSource::Sql(
                crate::sql::planner::table::SqlScanSource::new(
                    binding,
                    identity.clone(),
                    crate::sql::planner::table::SqlScanKind::Data {
                        version: crate::sql::planner::table::SqlTableVersionSelector::Current,
                    },
                ),
            ),
        };
        Ok(QueryTableBinding {
            resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
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
            delta_runtime_plans: std::collections::BTreeMap::new(),
        })
    })
}

fn admitted_write_target(
    binding: &QueryTableBinding,
) -> Result<&crate::engine::query_planning::bindings::QueryWriteTargetAdmission, String> {
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

    use crate::engine::query_planning::bindings::{QueryTableBinding, QueryTableBindingKey};
    use crate::sql::catalog::ResolvedAnalyzerTable;
    use crate::sql::planner::table::{
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

    #[test]
    fn sqlx2_write_binding_preserves_hidden_mv_target_fields() {
        let schema = novarocks_connector_iceberg::iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                        1,
                        "region",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::String,
                        ),
                    ),
                ),
                Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                        2,
                        "__nova_base_row_id",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Long,
                        ),
                    ),
                ),
            ])
            .build()
            .expect("target schema");
        let fields = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(&schema)
            .expect("target columns");

        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["region", "__nova_base_row_id"]
        );
    }
}
