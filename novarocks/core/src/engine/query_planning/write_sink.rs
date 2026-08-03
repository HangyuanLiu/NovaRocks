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

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use novarocks_catalog::schema::ColumnDef;

use super::bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::sql::analysis::TypedExpr;
use crate::sql::binding::SqlTableBindingId;
use crate::sql::planner::distributed::write::contract::{
    ConnectorWriteInputBinding, SqlPositionDeleteOutputDescriptor, SqlPositionDeleteOutputField,
    SqlPositionDeletePartitionSourceField, SqlWritePartitionContract, SqlWritePartitionField,
    SqlWritePartitionTransform, SqlWritePlanInput, SqlWriteSinkContract, SqlWriteSinkMode,
    SqlWriteSinkTargetContract, SqlWriteTargetField,
};
use crate::sql::planner::table::SqlTableIdentity;

/// Provider-private write facts retained by the application until native
/// writer registration.  SQL never receives this value: it sees the paired
/// `SqlWritePlanInput` constructed from its exact request-local binding.
#[derive(Clone, Debug)]
pub(crate) struct IcebergWriteSinkSpec {
    pub(crate) mode: IcebergWriteSinkMode,
    pub(crate) iceberg: crate::connector::iceberg::scan_model::IcebergTableInfo,
    pub(crate) target_columns: Vec<ColumnDef>,
    pub(crate) table_location: String,
    pub(crate) data_location: String,
    pub(crate) target_partition_spec_id: i32,
    pub(crate) cloud_properties: BTreeMap<String, String>,
    pub(crate) file_format: String,
    pub(crate) compression: IcebergWriteFileCompression,
    pub(crate) position_delete_output_descriptor: Option<
        crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
    >,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergWriteFileCompression {
    Snappy,
}

impl IcebergWriteSinkSpec {
    /// Update only application-owned frozen provider facts.  The SQL target
    /// contract is rebuilt from the exact binding before planning, so there
    /// is no second planner table or scan source to keep in sync.
    pub(crate) fn set_planned_snapshot_id(
        &mut self,
        planned_snapshot_id: Option<i64>,
    ) -> Result<(), String> {
        self.iceberg.current_snapshot_id = planned_snapshot_id;
        Ok(())
    }

    pub(crate) fn sql_mode(&self) -> SqlWriteSinkMode {
        match self.mode {
            IcebergWriteSinkMode::Data => SqlWriteSinkMode::Data,
            IcebergWriteSinkMode::RowLineageData => SqlWriteSinkMode::RowLineageData,
            IcebergWriteSinkMode::PositionDeletes => SqlWriteSinkMode::PositionDeletes,
            IcebergWriteSinkMode::DeletionVectors => SqlWriteSinkMode::DeletionVectors,
            IcebergWriteSinkMode::EqualityDeletes => SqlWriteSinkMode::EqualityDeletes,
        }
    }
}

pub(crate) fn transform_to_sink_string(transform: &iceberg::spec::Transform) -> String {
    transform.to_string()
}

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
    if captured.planning_lease.is_none() {
        return Err("SQL write target binding is missing its admission planning lease".to_string());
    }
    let materialization = captured
        .scan_materialization
        .as_ref()
        .ok_or_else(|| "SQL write target binding is missing admitted provider facts".to_string())?;
    let table = admitted_iceberg_table(materialization)?;
    let identity = SqlTableIdentity {
        catalog: table.catalog.clone(),
        namespace: table.namespace.clone(),
        table: table.table.clone(),
    };
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
    let serialized = table.serialized_metadata.as_deref().ok_or_else(|| {
        "SQL write target binding is missing frozen Iceberg table metadata".to_string()
    })?;
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| format!("decode admitted Iceberg write target metadata: {error}"))?;
    let fields = sql_write_target_fields(&captured.resolved.planner.columns, table)?;
    let partition = sql_write_partition_contract(&metadata)?;
    let target = SqlWriteSinkTargetContract::try_new(
        binding,
        identity,
        table.current_snapshot_id,
        fields,
        partition.clone(),
    )?;
    let position_delete_output = match mode {
        SqlWriteSinkMode::PositionDeletes | SqlWriteSinkMode::DeletionVectors => {
            Some(sql_position_delete_descriptor(
                &metadata,
                &captured.resolved.planner.columns,
                &partition,
            )?)
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
    let materialization = captured
        .scan_materialization
        .as_ref()
        .ok_or_else(|| "SQL write target binding is missing admitted provider facts".to_string())?;
    let table = admitted_iceberg_table(materialization)?;
    let metadata = admitted_iceberg_metadata(table)?;
    let input_columns =
        admitted_write_input_columns(mode, &captured.resolved.planner.columns, &metadata)?;
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
pub(crate) fn admit_frozen_iceberg_write_target(
    bindings: &QueryTableBindingStore,
    sink_spec: &IcebergWriteSinkSpec,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    admit_frozen_iceberg_write_target_materialization(
        bindings,
        sink_spec.iceberg.clone(),
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
    table: crate::connector::iceberg::scan_model::IcebergTableInfo,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    let descriptor = planning_lease.binding().descriptor();
    if !descriptor
        .instance_id
        .as_str()
        .eq_ignore_ascii_case(&table.catalog)
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
    let columns = admitted_write_target_columns(&table)?;
    let key = QueryTableBindingKey::write_target(&table.catalog, &table.namespace, &table.table);
    bindings.resolve_or_insert_with_id(key, |binding| {
        let identity = SqlTableIdentity {
            catalog: table.catalog.clone(),
            namespace: table.namespace.clone(),
            table: table.table.clone(),
        };
        let planner = crate::sql::planner::table::TableDef {
            name: table.table.clone(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::sql::planner::table::ScanSource::Sql(
                crate::sql::planner::table::SqlScanSource::new(
                    binding,
                    identity,
                    crate::sql::planner::table::SqlScanKind::Data {
                        version: crate::sql::planner::table::SqlTableVersionSelector::Current,
                    },
                ),
            ),
        };
        Ok(QueryTableBinding {
            resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                Some(&table.catalog),
                &table.namespace,
                planner,
            ),
            statistics_pin: None,
            planning_lease: Some(planning_lease),
            scan_materialization: Some(QueryScanMaterialization::IcebergDataFiles {
                table,
                files: Vec::new(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
            }),
            delta_runtime_plans: std::collections::BTreeMap::new(),
        })
    })
}

/// Preserve the complete physical writer schema selected by admission.
///
/// SQL visibility is a catalog-resolution concern.  A terminal write instead
/// needs every field the exact writer contract consumes, including SQL-owned
/// hidden row-lineage and MV state columns.
fn admitted_write_target_columns(
    table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
) -> Result<Vec<ColumnDef>, String> {
    let metadata = admitted_iceberg_metadata(table)?;
    write_target_columns_from_iceberg_schema(metadata.current_schema())
}

fn write_target_columns_from_iceberg_schema(
    schema: &iceberg::spec::Schema,
) -> Result<Vec<ColumnDef>, String> {
    crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(schema)
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
    let materialization = captured
        .scan_materialization
        .as_ref()
        .ok_or_else(|| "SQL write target binding is missing admitted provider facts".to_string())?;
    let table = admitted_iceberg_table(materialization)?.clone();
    let metadata = admitted_iceberg_metadata(&table)?;
    if !table
        .catalog
        .eq_ignore_ascii_case(&input.contract.target.table.catalog)
        || !table
            .namespace
            .eq_ignore_ascii_case(&input.contract.target.table.namespace)
        || !table
            .table
            .eq_ignore_ascii_case(&input.contract.target.table.table)
    {
        return Err("SQL write contract target differs from its admitted binding".to_string());
    }
    let mode = iceberg_write_sink_mode(input.contract.mode);
    let mut iceberg = table;
    if matches!(mode, IcebergWriteSinkMode::RowLineageData) {
        iceberg.schema.fields.extend([
            crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
                field_id: crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
            crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
                field_id: crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
        ]);
    }
    let position_delete_output_descriptor = input
        .contract
        .position_delete_output
        .as_ref()
        .map(position_delete_descriptor_from_sql)
        .transpose()?;
    let table_location = metadata.location().to_string();
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", table_location.trim_end_matches('/')));
    Ok(IcebergWriteSinkSpec {
        mode,
        iceberg,
        target_columns: input.contract.input_columns.clone(),
        table_location,
        data_location,
        target_partition_spec_id: metadata.default_partition_spec_id(),
        cloud_properties: entry.cloud_properties_map(),
        file_format: "parquet".to_string(),
        compression: IcebergWriteFileCompression::Snappy,
        position_delete_output_descriptor,
    })
}

fn iceberg_write_sink_mode(mode: SqlWriteSinkMode) -> IcebergWriteSinkMode {
    match mode {
        SqlWriteSinkMode::Data => IcebergWriteSinkMode::Data,
        SqlWriteSinkMode::RowLineageData => IcebergWriteSinkMode::RowLineageData,
        SqlWriteSinkMode::PositionDeletes => IcebergWriteSinkMode::PositionDeletes,
        SqlWriteSinkMode::DeletionVectors => IcebergWriteSinkMode::DeletionVectors,
        SqlWriteSinkMode::EqualityDeletes => IcebergWriteSinkMode::EqualityDeletes,
    }
}

fn position_delete_descriptor_from_sql(
    descriptor: &SqlPositionDeleteOutputDescriptor,
) -> Result<
    crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
    String,
> {
    Ok(crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput {
        file_path: crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField {
            output_expr_index: descriptor.file_path.output_expr_index,
            name: descriptor.file_path.name.clone(),
            data_type: descriptor.file_path.data_type.clone(),
            field_id: descriptor.file_path.field_id,
        },
        pos: crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField {
            output_expr_index: descriptor.pos.output_expr_index,
            name: descriptor.pos.name.clone(),
            data_type: descriptor.pos.data_type.clone(),
            field_id: descriptor.pos.field_id,
        },
        partition_source_fields: descriptor
            .partition_source_fields
            .iter()
            .map(|field| crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField {
                output_expr_index: field.output_expr_index,
                source_column_name: field.source_column_name.clone(),
                partition_field_name: field.partition_field_name.clone(),
                transform_expr: field.transform.sql_name(),
                source_field_id: field.source_field_id,
                data_type: field.data_type.clone(),
            })
            .collect(),
        target_partition_spec_id: descriptor.target_partition_spec_id,
    })
}

fn admitted_iceberg_metadata(
    table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
) -> Result<iceberg::spec::TableMetadata, String> {
    let serialized = table.serialized_metadata.as_deref().ok_or_else(|| {
        "SQL write target binding is missing frozen Iceberg table metadata".to_string()
    })?;
    serde_json::from_str(serialized)
        .map_err(|error| format!("decode admitted Iceberg write target metadata: {error}"))
}

fn admitted_iceberg_table(
    materialization: &QueryScanMaterialization,
) -> Result<&crate::connector::iceberg::scan_model::IcebergTableInfo, String> {
    match materialization {
        QueryScanMaterialization::IcebergDataFiles { table, .. }
        | QueryScanMaterialization::IcebergMvTarget { table, .. } => Ok(table),
        _ => Err("SQL write target binding is not an admitted Iceberg table".to_string()),
    }
}

fn admitted_write_input_columns(
    mode: SqlWriteSinkMode,
    target_columns: &[ColumnDef],
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<ColumnDef>, String> {
    match mode {
        SqlWriteSinkMode::Data | SqlWriteSinkMode::EqualityDeletes => Ok(target_columns.to_vec()),
        SqlWriteSinkMode::RowLineageData => {
            let mut columns = target_columns.to_vec();
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
            let schema = metadata.current_schema();
            for field in metadata.default_partition_spec().fields() {
                let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                    format!(
                        "admitted Iceberg write target partition source field id {} is missing",
                        field.source_id
                    )
                })?;
                let column = target_columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&source.name))
                    .ok_or_else(|| {
                        format!(
                            "admitted Iceberg write target partition source {} is absent from its SQL schema",
                            source.name
                        )
                    })?;
                columns.push(column.clone());
            }
            Ok(columns)
        }
    }
}

fn sql_write_target_fields(
    columns: &[ColumnDef],
    table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
) -> Result<Vec<SqlWriteTargetField>, String> {
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        let field = table
            .schema
            .fields
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(&column.name))
            .ok_or_else(|| {
                format!(
                    "admitted Iceberg write target is missing field identity for column {}",
                    column.name
                )
            })?;
        fields.push(SqlWriteTargetField {
            field_id: field.field_id,
            column: column.clone(),
            is_hidden: false,
        });
    }
    Ok(fields)
}

fn sql_write_partition_contract(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<SqlWritePartitionContract, String> {
    let partition_spec = metadata.default_partition_spec();
    let fields = partition_spec
        .fields()
        .iter()
        .map(|field| {
            Ok(SqlWritePartitionField {
                name: field.name.clone(),
                source_field_id: field.source_id,
                transform: sql_write_partition_transform(&field.transform)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(SqlWritePartitionContract {
        spec_id: metadata.default_partition_spec_id(),
        fields,
    })
}

fn sql_position_delete_descriptor(
    metadata: &iceberg::spec::TableMetadata,
    columns: &[ColumnDef],
    partition: &SqlWritePartitionContract,
) -> Result<SqlPositionDeleteOutputDescriptor, String> {
    let schema = metadata.current_schema();
    let partition_source_fields = partition
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let source = schema.field_by_id(field.source_field_id).ok_or_else(|| {
                format!(
                    "admitted Iceberg write target partition field {} has unknown source id {}",
                    field.name, field.source_field_id
                )
            })?;
            let column = columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&source.name))
                .ok_or_else(|| {
                    format!(
                        "admitted Iceberg write target partition source {} is absent from its SQL schema",
                        source.name
                    )
                })?;
            Ok(SqlPositionDeletePartitionSourceField {
                output_expr_index: index + 2,
                source_column_name: source.name.clone(),
                partition_field_name: field.name.clone(),
                transform: field.transform.clone(),
                source_field_id: field.source_field_id,
                data_type: column.data_type.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(SqlPositionDeleteOutputDescriptor {
        file_path: SqlPositionDeleteOutputField {
            output_expr_index: 0,
            name: "file_path".to_string(),
            data_type: DataType::Utf8,
            field_id: crate::connector::iceberg::position_delete_descriptor::ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        },
        pos: SqlPositionDeleteOutputField {
            output_expr_index: 1,
            name: "pos".to_string(),
            data_type: DataType::Int64,
            field_id: crate::connector::iceberg::position_delete_descriptor::ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        },
        partition_source_fields,
        target_partition_spec_id: partition.spec_id,
    })
}

fn sql_write_partition_transform(
    transform: &iceberg::spec::Transform,
) -> Result<SqlWritePartitionTransform, String> {
    Ok(match transform {
        iceberg::spec::Transform::Identity => SqlWritePartitionTransform::Identity,
        iceberg::spec::Transform::Year => SqlWritePartitionTransform::Year,
        iceberg::spec::Transform::Month => SqlWritePartitionTransform::Month,
        iceberg::spec::Transform::Day => SqlWritePartitionTransform::Day,
        iceberg::spec::Transform::Hour => SqlWritePartitionTransform::Hour,
        iceberg::spec::Transform::Bucket(buckets) => SqlWritePartitionTransform::Bucket {
            buckets: u32::try_from(*buckets)
                .map_err(|_| format!("Iceberg partition bucket count {buckets} is invalid"))?,
        },
        iceberg::spec::Transform::Truncate(width) => SqlWritePartitionTransform::Truncate {
            width: u32::try_from(*width)
                .map_err(|_| format!("Iceberg partition truncate width {width} is invalid"))?,
        },
        iceberg::spec::Transform::Void => SqlWritePartitionTransform::Void,
        unsupported => {
            return Err(format!(
                "Iceberg write target uses unsupported partition transform {unsupported}"
            ));
        }
    })
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
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "region",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    "__nova_base_row_id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("target schema");
        let fields = write_target_columns_from_iceberg_schema(&schema).expect("target columns");

        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["region", "__nova_base_row_id"]
        );
    }
}
