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
//! The compiler receives the resulting [`SqlWritePlanInput`] only.  Iceberg
//! metadata is read here from the exact request-local binding selected during
//! admission; it is not retained in the SQL plan and this module never
//! performs a current/latest connector acquisition.

use crate::connector::iceberg::write_contract::IcebergWriteSinkSpec;
use arrow::datatypes::DataType;
use novarocks_catalog::schema::ColumnDef;

use super::bindings::{
    QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::sql::analysis::TypedExpr;
use crate::sql::binding::SqlTableBindingId;
use crate::sql::planner::distributed::write::contract::{
    ConnectorWriteInputBinding, SqlWritePlanInput, SqlWriteSinkContract, SqlWriteSinkMode,
    SqlWriteSinkTargetContract,
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
    let admission = admitted_write_target(&captured)?;
    let identity = admission.identity.clone();
    let resolved_identity = &captured.resolved.catalog.identity;
    if !resolved_identity
        .catalog
        .eq_ignore_ascii_case(&identity.catalog)
        || !resolved_identity
            .namespace
            .eq_ignore_ascii_case(&identity.namespace)
        || !resolved_identity
            .table
            .eq_ignore_ascii_case(&identity.table)
    {
        return Err(
            "SQL write target binding identity differs from its admitted table".to_string(),
        );
    }
    let target = SqlWriteSinkTargetContract::try_new(
        binding,
        identity,
        admission.snapshot_id,
        admission.fields.clone(),
        admission.partition.clone(),
    )?;
    let position_delete_output = match mode {
        SqlWriteSinkMode::PositionDeletes | SqlWriteSinkMode::DeletionVectors => {
            Some(admission.position_delete_output.clone())
        }
        SqlWriteSinkMode::Data
        | SqlWriteSinkMode::RowLineageData
        | SqlWriteSinkMode::EqualityDeletes => None,
    };
    Ok(SqlWritePlanInput {
        contract: SqlWriteSinkContract::try_new(
            mode,
            target,
            input_columns,
            position_delete_output,
        )?,
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
    let admission = admitted_write_target(&captured)?;
    let input_columns = admitted_write_input_columns(mode, admission)?;
    sql_write_plan_input_from_admitted_binding(
        bindings,
        binding,
        mode,
        input_columns,
        input,
        root_output_exprs,
    )
}

/// Admit a frozen application-owned Iceberg write target before SQL planning.
/// The legacy write lifecycle still owns its commit collector and provider
/// payload, but cannot manufacture a SQL token: it must pair that frozen
/// target with the exact control lease captured at admission.
/// Build a row-lineage writer envelope from a connector materialization that
/// was already resolved through an exact planning lease.
///
/// This is intentionally an application-only adapter.  It keeps concrete
/// metadata out of SQL while ensuring a DML change-stream writer cannot mix a
/// catalog-loaded table with a newer provider generation.  The caller admits
/// the returned envelope into its query-local store before invoking the SQL
/// compiler.
pub(crate) fn row_lineage_sink_spec_from_frozen_materialization(
    materialization: &crate::connector::iceberg::provider::IcebergQueryTableMaterialization,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<IcebergWriteSinkSpec, String> {
    crate::connector::iceberg::provider::row_lineage_sink_spec_from_frozen_materialization(
        materialization,
        entry,
    )
}

pub(crate) fn admit_frozen_iceberg_write_target(
    bindings: &QueryTableBindingStore,
    sink_spec: &IcebergWriteSinkSpec,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    admit_frozen_iceberg_write_target_materialization(
        bindings,
        crate::connector::iceberg::provider::iceberg_write_target_admission_from_frozen_table(
            &sink_spec.iceberg,
        )?,
        planning_lease,
    )
}

/// Admit a writer token from an already frozen target materialization.
///
/// MV refresh has a target-state locator scan before it constructs a terminal
/// write.  The write must retain the same frozen table identity and planning
/// lease, but it deliberately receives its own token so its physical schema
/// cannot be confused with the locator scan's schema.
pub(crate) fn admit_frozen_iceberg_write_target_materialization(
    bindings: &QueryTableBindingStore,
    admission: crate::engine::query_planning::bindings::QueryWriteTargetAdmission,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    let descriptor = planning_lease.binding().descriptor();
    if !descriptor
        .instance_id
        .as_str()
        .eq_ignore_ascii_case(&admission.identity.catalog)
    {
        return Err(
            "frozen Iceberg write target does not match its admission planning lease".to_string(),
        );
    }
    // The binding carries the actual table schema, not the writer-input
    // layout. Position/DV writers consume row-identity columns such as
    // `_file` and `_pos`, while MV targets carry hidden apply/lineage/state
    // fields in their physical table schema. Both facts are frozen in the
    // admitted metadata and must remain distinct.
    let columns = admission.target_columns.clone();
    let key = QueryTableBindingKey::write_target(
        &admission.identity.catalog,
        &admission.identity.namespace,
        &admission.identity.table,
    );
    bindings.resolve_or_insert_with_id(key, |binding| {
        let identity = admission.identity.clone();
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
            write_target_admission: Some(admission),
            frozen_snapshot_materializations: std::collections::BTreeMap::new(),
            delta_runtime_plans: std::collections::BTreeMap::new(),
        })
    })
}

/// Rehydrate the provider-private writer carrier only after SQL planning has
/// sealed its tokenized contract.  This adapter reads no catalog state: all
/// table metadata comes from the exact binding selected at admission.
pub(crate) fn iceberg_write_sink_spec_from_admitted_sql_input(
    bindings: &QueryTableBindingStore,
    input: &SqlWritePlanInput,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<IcebergWriteSinkSpec, String> {
    let captured = bindings.binding(input.contract.target.binding)?;
    let admission = admitted_write_target(&captured)?;
    if !admission
        .identity
        .catalog
        .eq_ignore_ascii_case(&input.contract.target.table.catalog)
        || !admission
            .identity
            .namespace
            .eq_ignore_ascii_case(&input.contract.target.table.namespace)
        || !admission
            .identity
            .table
            .eq_ignore_ascii_case(&input.contract.target.table.table)
    {
        return Err("SQL write contract target differs from its admitted binding".to_string());
    }
    crate::connector::iceberg::provider::iceberg_write_sink_spec_from_admitted_handle(
        &admission.table,
        input,
        entry,
    )
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
    mode: SqlWriteSinkMode,
    admission: &crate::engine::query_planning::bindings::QueryWriteTargetAdmission,
) -> Result<Vec<ColumnDef>, String> {
    match mode {
        SqlWriteSinkMode::Data | SqlWriteSinkMode::EqualityDeletes => {
            Ok(admission.target_columns.clone())
        }
        SqlWriteSinkMode::RowLineageData => {
            let mut columns = admission.target_columns.clone();
            columns.push(ColumnDef {
                name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            });
            columns.push(ColumnDef {
                name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            });
            Ok(columns)
        }
        SqlWriteSinkMode::PositionDeletes | SqlWriteSinkMode::DeletionVectors => {
            let mut columns = vec![
                ColumnDef {
                    name: crate::exec::row_position::ICEBERG_FILE_PATH_COL.to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: crate::exec::row_position::ICEBERG_ROW_POS_COL.to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ];
            for field in &admission.position_delete_output.partition_source_fields {
                columns.push(ColumnDef {
                    name: field.source_column_name.clone(),
                    data_type: field.data_type.clone(),
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                });
            }
            Ok(columns)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
