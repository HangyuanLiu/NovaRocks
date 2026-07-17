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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use iceberg::spec::TableMetadata;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Compression;

use crate::connector::iceberg::commit::EqualityDeleteColumn;
use crate::connector::iceberg::delete_file::IcebergFileFormat;
use crate::connector::iceberg::position_delete_descriptor::{
    PositionDeleteDescriptorInput, PositionDeleteExpectedBinding, PositionDeleteOutputField,
    PositionDeletePartitionSourceField, bind_position_delete_descriptor,
};
use crate::connector::iceberg::schema::{
    IcebergPartitionInfo, IcebergSchemaDescriptor, IcebergSchemaFieldDescriptor,
    IcebergTableColumn, IcebergTableDescriptor, apply_field_id_recursive, build_full_output_schema,
};
use crate::connector::iceberg::sink::build_staged_file_io;
use crate::connector::iceberg::sink_plan::{
    IcebergSinkFactoryInput, IcebergSinkMode, IcebergSinkObjectStoreConfig, IcebergSinkPlan,
    PositionDeleteDataFilePartition,
};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use crate::fs::object_store_credentials::{ObjectStoreCredentials, ObjectStoreCredentialsSource};
use crate::lower::compat::expr::lower_t_expr;
use crate::lower::compat::layout::Layout;
use crate::runtime::global_async_runtime::data_block_on;
use crate::thrift::{data_sinks, descriptors, exprs, types};

type PartitionExprs = (Vec<String>, Vec<String>, Vec<String>, Vec<exprs::TExpr>);

pub(crate) fn iceberg_sink_mode_for_type(t: data_sinks::TDataSinkType) -> IcebergSinkMode {
    match t {
        data_sinks::TDataSinkType::ICEBERG_DELETE_SINK => IcebergSinkMode::PositionDeletes,
        data_sinks::TDataSinkType::ICEBERG_DV_SINK => IcebergSinkMode::DeletionVectors,
        data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => IcebergSinkMode::EqualityDeletes,
        _ => IcebergSinkMode::Data,
    }
}

pub(crate) fn lower_iceberg_sink_factory_input(
    sink: &data_sinks::TIcebergTableSink,
    mode: IcebergSinkMode,
    output_exprs: &[exprs::TExpr],
    layout: &Layout,
    desc_tbl: &descriptors::TDescriptorTable,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<IcebergSinkFactoryInput, String> {
    let mut arena = ExprArena::default();
    let lowered_output_exprs =
        lower_output_exprs(output_exprs, &mut arena, layout, last_query_id, fe_addr)?;

    let thrift_iceberg_table = resolve_iceberg_table(desc_tbl, sink.target_table_id)?;
    let iceberg_table = iceberg_table_descriptor_from_thrift(&thrift_iceberg_table)?;
    let target_partition_spec_id = sink.target_partition_spec_id.unwrap_or(0);

    let (
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        mut partition_exprs,
    ) = build_partition_exprs(&thrift_iceberg_table)?;

    let target_table_metadata = parse_target_table_metadata(&iceberg_table, mode)?;
    let position_delete_binding = if matches!(
        mode,
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
    ) {
        let target_schema = build_full_output_schema(&iceberg_table)?;
        let metadata = target_table_metadata.as_ref().ok_or_else(|| {
            format!(
                "iceberg {:?} sink requires serialized target table metadata",
                mode
            )
        })?;
        let partition_source_field_ids =
            partition_source_field_ids_from_metadata(metadata, &partition_source_column_names)?;
        let desc = position_delete_descriptor_input_from_thrift(
            sink.position_delete_output_descriptor.as_ref(),
            output_exprs,
        )
        .map_err(|err| err.to_bracketed_user_message())?;
        let expected = PositionDeleteExpectedBinding {
            target_partition_spec_id,
            partition_source_column_names: partition_source_column_names.clone(),
            partition_column_names: partition_column_names.clone(),
            partition_transform_exprs: transform_exprs.clone(),
            partition_source_field_ids,
            output_expr_count: output_exprs.len(),
        };
        let binding = bind_position_delete_descriptor(&desc, &expected)
            .map_err(|err| err.to_bracketed_user_message())?;
        Some((target_schema, binding))
    } else {
        None
    };

    let (output_schema, target_schema, equality_delete_columns) = match mode {
        IcebergSinkMode::Data => {
            let target_schema = build_full_output_schema(&iceberg_table)?;
            if output_exprs.len() != target_schema.fields().len() {
                return Err(format!(
                    "iceberg sink output expr count mismatch: exprs={} columns={}",
                    output_exprs.len(),
                    target_schema.fields().len()
                ));
            }
            (Arc::clone(&target_schema), target_schema, Vec::new())
        }
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors => {
            let (target_schema, binding) = position_delete_binding
                .as_ref()
                .expect("position delete binding must exist for delete-like sink");
            (
                Arc::clone(&binding.output_schema),
                Arc::clone(target_schema),
                Vec::new(),
            )
        }
        IcebergSinkMode::EqualityDeletes => {
            if !partition_column_names.is_empty() {
                return Err(
                    "iceberg equality-delete sink currently supports only unpartitioned tables"
                        .to_string(),
                );
            }
            validate_equality_delete_unpartitioned_target_metadata(
                &iceberg_table,
                target_partition_spec_id,
            )?;
            let (schema, columns) = build_equality_delete_output_schema(&iceberg_table)?;
            if output_exprs.len() != schema.fields().len() {
                return Err(format!(
                    "iceberg equality-delete sink expects {} output exprs \
                    (one per equality-key column); got {}",
                    schema.fields().len(),
                    output_exprs.len(),
                ));
            }
            (Arc::clone(&schema), schema, columns)
        }
    };
    let row_lineage_data =
        mode == IcebergSinkMode::Data && schema_has_reserved_row_lineage_columns(&target_schema)?;
    let output_column_names = match mode {
        IcebergSinkMode::Data => output_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors => {
            position_delete_binding
                .as_ref()
                .expect("position delete binding must exist for delete-like sink")
                .1
                .output_column_names
                .clone()
        }
        IcebergSinkMode::EqualityDeletes => output_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
    };
    if !partition_exprs.is_empty() {
        let slot_map = build_column_slot_map(output_exprs, &output_column_names)?;
        update_partition_expr_slot_refs(&mut partition_exprs, &slot_map, &thrift_iceberg_table)?;
    }
    let lowered_partition_exprs =
        lower_partition_exprs(&partition_exprs, &mut arena, layout, last_query_id, fe_addr)?;

    let table_location = sink
        .location
        .clone()
        .ok_or_else(|| "iceberg sink missing table location".to_string())?;
    let data_location = resolve_data_location(sink)?;
    let object_store_s3 = resolve_sink_s3_config(sink, &data_location)?;
    let target_snapshot_id = iceberg_table.current_snapshot_id;
    let position_delete_data_file_partitions = if matches!(
        mode,
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
    ) {
        let metadata = target_table_metadata
            .as_ref()
            .ok_or_else(|| "iceberg delete sink missing target table metadata".to_string())?;
        build_position_delete_data_file_partition_index(
            metadata,
            target_snapshot_id,
            &table_location,
            object_store_s3.as_ref(),
        )?
    } else {
        HashMap::new()
    };
    let (file_format, report_file_format) = validate_iceberg_sink_file_format(
        sink.file_format
            .as_ref()
            .ok_or_else(|| "iceberg sink missing file_format".to_string())?,
    )?;
    let compression = map_parquet_compression(
        sink.compression_type
            .ok_or_else(|| "iceberg sink missing compression_type".to_string())?,
    )?;

    let plan = IcebergSinkPlan {
        mode,
        table_location,
        data_location,
        target_partition_spec_id,
        target_table_metadata,
        target_snapshot_id,
        position_delete_data_file_partitions,
        object_store_s3,
        file_format,
        report_file_format,
        compression,
        output_schema,
        target_schema,
        equality_delete_columns,
        row_lineage_data,
        output_exprs: lowered_output_exprs,
        partition_exprs: lowered_partition_exprs,
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        position_delete_binding: position_delete_binding.map(|(_, binding)| binding),
    };

    Ok(IcebergSinkFactoryInput {
        name: "ICEBERG_TABLE_SINK".to_string(),
        arena,
        plan,
    })
}

fn lower_output_exprs(
    output_exprs: &[exprs::TExpr],
    arena: &mut ExprArena,
    layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<ExprId>, String> {
    if output_exprs.is_empty() {
        return Err("iceberg sink missing output exprs".to_string());
    }
    let mut ids = Vec::with_capacity(output_exprs.len());
    for expr in output_exprs {
        let id = lower_t_expr(expr, arena, layout, last_query_id, fe_addr)?;
        ids.push(id);
    }
    Ok(ids)
}

fn lower_partition_exprs(
    partition_exprs: &[exprs::TExpr],
    arena: &mut ExprArena,
    layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<ExprId>, String> {
    let mut ids = Vec::with_capacity(partition_exprs.len());
    for expr in partition_exprs {
        let id = lower_t_expr(expr, arena, layout, last_query_id, fe_addr)?;
        ids.push(id);
    }
    Ok(ids)
}

fn resolve_iceberg_table(
    desc_tbl: &descriptors::TDescriptorTable,
    table_id: Option<i64>,
) -> Result<descriptors::TIcebergTable, String> {
    let table_id = table_id.ok_or_else(|| "iceberg sink missing target_table_id".to_string())?;
    let tables = desc_tbl
        .table_descriptors
        .as_ref()
        .ok_or_else(|| "descriptor table missing table_descriptors".to_string())?;
    for table in tables {
        if table.id == table_id {
            let iceberg = table
                .iceberg_table
                .as_ref()
                .ok_or_else(|| "table descriptor missing iceberg_table".to_string())?;
            return Ok(iceberg.clone());
        }
    }
    Err(format!(
        "iceberg table descriptor not found for table_id={table_id}"
    ))
}

fn iceberg_schema_field_descriptor_from_thrift(
    field: &descriptors::TIcebergSchemaField,
) -> Result<IcebergSchemaFieldDescriptor, String> {
    let name = field
        .name
        .clone()
        .ok_or_else(|| "iceberg schema field missing name".to_string())?;
    let children = field
        .children
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|child| iceberg_schema_field_descriptor_from_thrift(child.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(IcebergSchemaFieldDescriptor {
        name,
        field_id: field.field_id,
        children,
        initial_default_json: field.initial_default_json.clone(),
    })
}

fn iceberg_schema_descriptor_from_thrift(
    schema: &descriptors::TIcebergSchema,
) -> Result<IcebergSchemaDescriptor, String> {
    let fields = schema
        .fields
        .as_ref()
        .ok_or_else(|| "iceberg schema missing fields".to_string())?
        .iter()
        .map(iceberg_schema_field_descriptor_from_thrift)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(IcebergSchemaDescriptor { fields })
}

fn iceberg_table_descriptor_from_thrift(
    iceberg: &descriptors::TIcebergTable,
) -> Result<IcebergTableDescriptor, String> {
    let columns = iceberg
        .columns
        .as_ref()
        .ok_or_else(|| "iceberg table missing columns".to_string())?
        .iter()
        .map(|column| {
            let data_type = column
                .type_desc
                .as_ref()
                .and_then(crate::types::arrow_thrift::thrift_desc_to_arrow_type)
                .ok_or_else(|| {
                    format!("iceberg column {} missing type_desc", column.column_name)
                })?;
            Ok(IcebergTableColumn {
                name: column.column_name.clone(),
                data_type,
                nullable: column.is_allow_null.unwrap_or(true),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let iceberg_schema = iceberg
        .iceberg_schema
        .as_ref()
        .map(iceberg_schema_descriptor_from_thrift)
        .transpose()?;
    let equality_delete_schema = iceberg
        .iceberg_equal_delete_schema
        .as_ref()
        .map(iceberg_schema_descriptor_from_thrift)
        .transpose()?;
    let partition_info = iceberg
        .partition_info
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|info| {
            Ok(IcebergPartitionInfo {
                source_column_name: info.source_column_name.clone().ok_or_else(|| {
                    "iceberg partition_info missing source_column_name".to_string()
                })?,
                partition_column_name: info.partition_column_name.clone().ok_or_else(|| {
                    "iceberg partition_info missing partition_column_name".to_string()
                })?,
                transform_expr: info
                    .transform_expr
                    .clone()
                    .ok_or_else(|| "iceberg partition_info missing transform_expr".to_string())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(IcebergTableDescriptor {
        columns,
        iceberg_schema,
        equality_delete_schema,
        partition_info,
        current_snapshot_id: iceberg.current_snapshot_id,
        serialized_metadata: iceberg.serialized_metadata.clone(),
    })
}

fn parse_target_table_metadata(
    iceberg: &IcebergTableDescriptor,
    mode: IcebergSinkMode,
) -> Result<Option<TableMetadata>, String> {
    let serialized = match mode {
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors => {
            Some(iceberg.serialized_metadata.as_ref().ok_or_else(|| {
                format!(
                    "iceberg {:?} sink requires serialized target table metadata",
                    mode
                )
            })?)
        }
        IcebergSinkMode::Data | IcebergSinkMode::EqualityDeletes => {
            iceberg.serialized_metadata.as_ref()
        }
    };
    let Some(serialized) = serialized else {
        return Ok(None);
    };
    serde_json::from_str::<TableMetadata>(serialized)
        .map(Some)
        .map_err(|e| format!("parse iceberg {:?} target metadata failed: {e}", mode))
}

fn validate_equality_delete_unpartitioned_target_metadata(
    iceberg: &IcebergTableDescriptor,
    target_partition_spec_id: i32,
) -> Result<(), String> {
    let Some(serialized) = iceberg.serialized_metadata.as_ref() else {
        return Ok(());
    };
    let metadata = serde_json::from_str::<TableMetadata>(serialized)
        .map_err(|e| format!("parse iceberg equality-delete target metadata failed: {e}"))?;
    let spec = metadata
        .partition_spec_by_id(target_partition_spec_id)
        .ok_or_else(|| {
            format!(
                "iceberg equality-delete sink target partition spec id {target_partition_spec_id} \
                not found in table metadata"
            )
        })?;
    if !spec.fields().is_empty() {
        return Err(format!(
            "iceberg equality-delete sink currently supports only unpartitioned tables; \
            target partition spec id {target_partition_spec_id} has {} fields",
            spec.fields().len()
        ));
    }
    Ok(())
}

fn build_equality_delete_output_schema(
    iceberg: &IcebergTableDescriptor,
) -> Result<(SchemaRef, Vec<EqualityDeleteColumn>), String> {
    let columns = &iceberg.columns;
    if columns.is_empty() {
        return Err(
            "iceberg equality-delete sink requires at least one equality column".to_string(),
        );
    }

    let key_fields = iceberg
        .equality_delete_schema
        .as_ref()
        .ok_or_else(|| {
            "iceberg equality-delete sink requires iceberg_equal_delete_schema projected key fields"
                .to_string()
        })?
        .fields
        .as_slice();
    if key_fields.is_empty() {
        return Err(
            "iceberg equality-delete sink requires at least one equality column".to_string(),
        );
    }

    let mut fields = Vec::with_capacity(key_fields.len());
    let mut equality_columns = Vec::with_capacity(key_fields.len());
    for schema_field in key_fields {
        let column = columns
            .iter()
            .find(|column| column.name == schema_field.name)
            .ok_or_else(|| {
                format!(
                    "iceberg equality-delete column {} missing column descriptor",
                    schema_field.name
                )
            })?;
        let field = Field::new(
            column.name.clone(),
            column.data_type.clone(),
            column.nullable,
        );
        let field = apply_field_id_recursive(field, schema_field)?;
        let field_id = arrow_field_id(&field)?;
        equality_columns.push(EqualityDeleteColumn {
            name: field.name().to_string(),
            field_id,
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        });
        fields.push(field);
    }

    Ok((Arc::new(Schema::new(fields)), equality_columns))
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "iceberg sink field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|e| {
        format!(
            "iceberg sink field {} has invalid parquet field id {raw}: {e}",
            field.name()
        )
    })
}

fn partition_source_field_ids_from_metadata(
    metadata: &TableMetadata,
    source_column_names: &[String],
) -> Result<Vec<i32>, String> {
    let target_schema = metadata.current_schema();
    source_column_names
        .iter()
        .map(|source_name| {
            target_schema
                .field_by_name_case_insensitive(source_name)
                .map(|field| field.id)
                .ok_or_else(|| {
                    format!(
                        "iceberg sink partition source column {source_name} missing from target metadata schema"
                    )
                })
        })
        .collect()
}

fn schema_has_reserved_row_lineage_columns(schema: &Schema) -> Result<bool, String> {
    let mut has_row_id = false;
    let mut has_last_updated = false;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
            has_row_id = matches!(arrow_field_id(field), Ok(ICEBERG_RESERVED_FIELD_ID_ROW_ID));
        } else if field
            .name()
            .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        {
            has_last_updated = matches!(
                arrow_field_id(field),
                Ok(ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
            );
        }
    }
    Ok(has_row_id && has_last_updated)
}

fn build_partition_exprs(iceberg: &descriptors::TIcebergTable) -> Result<PartitionExprs, String> {
    let mut partition_source_column_names = Vec::new();
    let mut partition_column_names = Vec::new();
    let mut transform_exprs = Vec::new();
    let mut exprs = Vec::new();
    if let Some(partition_info) = iceberg.partition_info.as_ref() {
        for info in partition_info {
            let name = info.partition_column_name.clone().ok_or_else(|| {
                "iceberg partition_info missing partition_column_name".to_string()
            })?;
            let transform = info
                .transform_expr
                .clone()
                .ok_or_else(|| "iceberg partition_info missing transform_expr".to_string())?;
            let expr = info
                .partition_expr
                .clone()
                .ok_or_else(|| "iceberg partition_info missing partition_expr".to_string())?;
            let source_name = info
                .source_column_name
                .clone()
                .ok_or_else(|| "iceberg partition_info missing source_column_name".to_string())?;
            partition_source_column_names.push(source_name);
            partition_column_names.push(name);
            transform_exprs.push(transform);
            exprs.push(expr);
        }
    }
    Ok((
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        exprs,
    ))
}

fn build_column_slot_map(
    output_exprs: &[exprs::TExpr],
    output_column_names: &[String],
) -> Result<HashMap<String, exprs::TExprNode>, String> {
    if output_column_names.len() != output_exprs.len() {
        return Err(format!(
            "iceberg sink output column count mismatch: columns={} output_exprs={}",
            output_column_names.len(),
            output_exprs.len()
        ));
    }

    let mut map = HashMap::new();
    for (col_name, expr) in output_column_names.iter().zip(output_exprs.iter()) {
        let mut slot_ref = None;
        for node in &expr.nodes {
            if node.node_type == exprs::TExprNodeType::SLOT_REF {
                slot_ref = Some(node.clone());
                break;
            }
        }
        let slot_ref = slot_ref
            .ok_or_else(|| format!("output expr for column {col_name} missing SLOT_REF node"))?;
        map.insert(col_name.clone(), slot_ref);
    }
    Ok(map)
}

fn update_partition_expr_slot_refs(
    partition_exprs: &mut [exprs::TExpr],
    column_slot_map: &HashMap<String, exprs::TExprNode>,
    iceberg: &descriptors::TIcebergTable,
) -> Result<(), String> {
    let Some(partition_info) = iceberg.partition_info.as_ref() else {
        return Ok(());
    };
    if partition_exprs.len() != partition_info.len() {
        return Err(format!(
            "partition expr count mismatch: exprs={} partition_info={}",
            partition_exprs.len(),
            partition_info.len()
        ));
    }
    for (expr, info) in partition_exprs.iter_mut().zip(partition_info.iter()) {
        let source_name = info
            .source_column_name
            .as_ref()
            .ok_or_else(|| "partition_info missing source_column_name".to_string())?;
        let slot_ref = column_slot_map.get(source_name).ok_or_else(|| {
            format!(
                "partition source column {} missing slot_ref in output exprs",
                source_name
            )
        })?;
        let mut replaced = false;
        for node in &mut expr.nodes {
            if node.node_type == exprs::TExprNodeType::SLOT_REF {
                *node = slot_ref.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            return Err(format!(
                "partition expr for {} missing SLOT_REF node",
                source_name
            ));
        }
    }
    Ok(())
}

fn resolve_data_location(sink: &data_sinks::TIcebergTableSink) -> Result<String, String> {
    if let Some(loc) = sink.data_location.as_ref().filter(|s| !s.is_empty()) {
        return Ok(loc.clone());
    }
    let location = sink
        .location
        .as_ref()
        .ok_or_else(|| "iceberg sink missing table location".to_string())?;
    let base = location.trim_end_matches('/');
    Ok(format!("{base}/data"))
}

fn resolve_sink_s3_config(
    sink: &data_sinks::TIcebergTableSink,
    data_location: &str,
) -> Result<Option<IcebergSinkObjectStoreConfig>, String> {
    if !crate::fs::access::is_object_store_location_parse_only(data_location)
        .map_err(|e| format!("parse iceberg sink data_location {data_location}: {e}"))?
    {
        return Ok(None);
    }
    let (bucket, _data_root) = crate::fs::access::parse_object_store_path_parse_only(data_location)
        .map_err(|e| {
            format!("parse iceberg sink object-store data_location {data_location}: {e}")
        })?;
    let cloud = sink.cloud_configuration.as_ref().ok_or_else(|| {
        format!(
            "iceberg sink object-store path requires cloud_configuration: data_location={data_location}"
        )
    })?;
    let props = cloud
        .cloud_properties
        .as_ref()
        .ok_or_else(|| "iceberg sink cloud_configuration.cloud_properties is empty".to_string())?;

    let credentials = ObjectStoreCredentials::from_aws_s3_properties(
        ObjectStoreCredentialsSource::IcebergSinkCloudProperties,
        props,
    )?;

    Ok(Some(IcebergSinkObjectStoreConfig::from_credentials(
        bucket,
        credentials,
    )))
}

fn build_position_delete_data_file_partition_index(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
    table_location: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
) -> Result<HashMap<String, PositionDeleteDataFilePartition>, String> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};

    let Some(snapshot_id) = target_snapshot_id.or_else(|| metadata.current_snapshot_id()) else {
        return Ok(HashMap::new());
    };
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        format!("iceberg delete sink target snapshot id {snapshot_id} not found in table metadata")
    })?;
    let file_io = build_staged_file_io(table_location, s3_config)?;
    data_block_on(async {
        let manifest_list = snapshot
            .load_manifest_list(&file_io, metadata)
            .await
            .map_err(|e| format!("load iceberg position-delete target manifest list: {e}"))?;
        let mut index = HashMap::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(&file_io).await.map_err(|e| {
                format!(
                    "load iceberg position-delete data manifest {} failed: {e}",
                    manifest_file.manifest_path
                )
            })?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::Data {
                    continue;
                }
                let partition = PositionDeleteDataFilePartition {
                    partition_spec_id: manifest_file.partition_spec_id,
                    partition_values: data_file.partition().clone(),
                };
                insert_position_delete_data_file_partition(
                    &mut index,
                    data_file.file_path().to_string(),
                    partition,
                )?;
            }
        }
        Ok(index)
    })?
}

fn insert_position_delete_data_file_partition(
    index: &mut HashMap<String, PositionDeleteDataFilePartition>,
    path: String,
    partition: PositionDeleteDataFilePartition,
) -> Result<(), String> {
    match index.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
            Ok(())
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            Err(format!(
                "iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ))
        }
    }
}

fn descriptor_error(message: impl Into<String>) -> crate::common::engine_error::EngineError {
    crate::common::engine_error::EngineError::unsupported_position_delete_descriptor(message)
}

fn required_output_index(
    label: &str,
    index: Option<i32>,
) -> Result<usize, crate::common::engine_error::EngineError> {
    let index =
        index.ok_or_else(|| descriptor_error(format!("{label} output_expr_index is missing")))?;
    usize::try_from(index)
        .map_err(|_| descriptor_error(format!("{label} output expr index is negative: {index}")))
}

fn arrow_data_type_from_type_desc(
    label: &str,
    type_desc: Option<&types::TTypeDesc>,
) -> Result<DataType, crate::common::engine_error::EngineError> {
    type_desc
        .and_then(crate::types::arrow_thrift::thrift_desc_to_arrow_type)
        .ok_or_else(|| descriptor_error(format!("{label} type_desc is missing")))
}

fn output_expr_root_data_type(
    label: &str,
    output_exprs: &[exprs::TExpr],
    output_expr_index: usize,
) -> Result<DataType, crate::common::engine_error::EngineError> {
    let expr = output_exprs.get(output_expr_index).ok_or_else(|| {
        descriptor_error(format!(
            "{label} output expr index out of bounds: index={output_expr_index}, exprs={}",
            output_exprs.len()
        ))
    })?;
    let root = expr.nodes.first().ok_or_else(|| {
        descriptor_error(format!("{label} output expr {output_expr_index} is empty"))
    })?;
    arrow_data_type_from_type_desc(label, Some(&root.type_))
}

fn output_field_from_thrift(
    label: &str,
    field: Option<&data_sinks::TIcebergPositionDeleteOutputField>,
    output_exprs: &[exprs::TExpr],
) -> Result<PositionDeleteOutputField, crate::common::engine_error::EngineError> {
    let field = field.ok_or_else(|| descriptor_error(format!("{label} descriptor is missing")))?;
    let output_expr_index = required_output_index(label, field.output_expr_index)?;
    Ok(PositionDeleteOutputField {
        output_expr_index,
        name: field
            .name
            .clone()
            .ok_or_else(|| descriptor_error(format!("{label} name is missing")))?,
        data_type: output_expr_root_data_type(label, output_exprs, output_expr_index)?,
        field_id: field
            .field_id
            .ok_or_else(|| descriptor_error(format!("{label} field_id is missing")))?,
    })
}

fn position_delete_descriptor_input_from_thrift(
    desc: Option<&data_sinks::TIcebergPositionDeleteOutputDescriptor>,
    output_exprs: &[exprs::TExpr],
) -> Result<PositionDeleteDescriptorInput, crate::common::engine_error::EngineError> {
    let desc =
        desc.ok_or_else(|| descriptor_error("position delete output descriptor is missing"))?;
    let partition_source_fields = desc
        .partition_source_fields
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|field| {
            let output_expr_index =
                required_output_index("partition source", field.output_expr_index)?;
            let source_column_name = field
                .source_column_name
                .clone()
                .ok_or_else(|| descriptor_error("partition source column name is missing"))?;
            Ok(PositionDeletePartitionSourceField {
                output_expr_index,
                source_column_name: source_column_name.clone(),
                partition_field_name: field.partition_field_name.clone().ok_or_else(|| {
                    descriptor_error(format!(
                        "partition field name is missing for {source_column_name}"
                    ))
                })?,
                transform_expr: field.transform_expr.clone().ok_or_else(|| {
                    descriptor_error(format!(
                        "partition transform is missing for {source_column_name}"
                    ))
                })?,
                source_field_id: field.source_field_id.ok_or_else(|| {
                    descriptor_error(format!(
                        "partition source field id is missing for {source_column_name}"
                    ))
                })?,
                data_type: output_expr_root_data_type(
                    source_column_name.as_str(),
                    output_exprs,
                    output_expr_index,
                )?,
            })
        })
        .collect::<Result<Vec<_>, crate::common::engine_error::EngineError>>()?;

    Ok(PositionDeleteDescriptorInput {
        file_path: output_field_from_thrift("file_path", desc.file_path.as_ref(), output_exprs)?,
        pos: output_field_from_thrift("pos", desc.pos.as_ref(), output_exprs)?,
        partition_source_fields,
        target_partition_spec_id: desc
            .target_partition_spec_id
            .ok_or_else(|| descriptor_error("target partition spec id is missing"))?,
    })
}

fn map_parquet_compression(compression: types::TCompressionType) -> Result<Compression, String> {
    use types::TCompressionType as C;
    match compression {
        C::NO_COMPRESSION => Ok(Compression::UNCOMPRESSED),
        C::SNAPPY => Ok(Compression::SNAPPY),
        C::LZ4 | C::LZ4_FRAME => Ok(Compression::LZ4),
        C::ZSTD => Ok(Compression::ZSTD(Default::default())),
        C::GZIP | C::ZLIB | C::DEFLATE => Ok(Compression::GZIP(Default::default())),
        C::BROTLI => Ok(Compression::BROTLI(Default::default())),
        C::LZO => Ok(Compression::LZO),
        other => Err(format!(
            "unsupported compression type for iceberg parquet sink: {:?}",
            other
        )),
    }
}

fn validate_iceberg_sink_file_format(
    file_format: &str,
) -> Result<(IcebergFileFormat, String), String> {
    if file_format.to_lowercase() != "parquet" {
        return Err(format!(
            "iceberg sink does not support {} files; NovaRocks currently only supports Parquet for Iceberg writes",
            file_format
        ));
    }
    Ok((IcebergFileFormat::Parquet, file_format.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::position_delete_descriptor::{
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
    };
    use std::collections::BTreeMap;

    fn thrift_type_desc(primitive: types::TPrimitiveType) -> types::TTypeDesc {
        crate::types::arrow_thrift::thrift_type_desc_from_primitive(primitive)
    }

    fn slot_expr(slot_id: i32, primitive: types::TPrimitiveType) -> exprs::TExpr {
        crate::lower::compat::test_support::build_slot_ref_texpr(
            slot_id,
            1,
            thrift_type_desc(primitive),
        )
    }

    fn required_descriptor() -> data_sinks::TIcebergPositionDeleteOutputDescriptor {
        data_sinks::TIcebergPositionDeleteOutputDescriptor::new(
            Some(data_sinks::TIcebergPositionDeleteOutputField::new(
                Some(0),
                Some("file_path".to_string()),
                Some(thrift_type_desc(types::TPrimitiveType::VARCHAR)),
                Some(ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID),
            )),
            Some(data_sinks::TIcebergPositionDeleteOutputField::new(
                Some(1),
                Some("pos".to_string()),
                Some(thrift_type_desc(types::TPrimitiveType::BIGINT)),
                Some(ICEBERG_POSITION_DELETE_POS_FIELD_ID),
            )),
            Some(Vec::new()),
            Some(7),
        )
    }

    fn position_delete_required_schema_for_tests() -> SchemaRef {
        let desc = PositionDeleteDescriptorInput {
            file_path: PositionDeleteOutputField {
                output_expr_index: 0,
                name: ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
                data_type: DataType::Utf8,
                field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            },
            pos: PositionDeleteOutputField {
                output_expr_index: 1,
                name: ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
                data_type: DataType::Int64,
                field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            },
            partition_source_fields: Vec::new(),
            target_partition_spec_id: 0,
        };
        crate::connector::iceberg::position_delete_descriptor::output_schema_from_descriptor(&desc)
            .expect("required position delete schema")
    }

    fn test_int_column(name: &str) -> descriptors::TColumn {
        descriptors::TColumn::new(
            name.to_string(),
            None::<types::TColumnType>,
            None::<types::TAggregationType>,
            None::<bool>,
            Some(false),
            None::<String>,
            None::<bool>,
            None::<exprs::TExpr>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<types::TAggStateDesc>,
            None::<i32>,
            Some(thrift_type_desc(types::TPrimitiveType::INT)),
            None::<exprs::TExpr>,
        )
    }

    fn test_varchar_column(name: &str) -> descriptors::TColumn {
        descriptors::TColumn::new(
            name.to_string(),
            None::<types::TColumnType>,
            None::<types::TAggregationType>,
            None::<bool>,
            Some(true),
            None::<String>,
            None::<bool>,
            None::<exprs::TExpr>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<types::TAggStateDesc>,
            None::<i32>,
            Some(thrift_type_desc(types::TPrimitiveType::VARCHAR)),
            None::<exprs::TExpr>,
        )
    }

    fn test_iceberg_schema_field(name: &str, field_id: i32) -> descriptors::TIcebergSchemaField {
        descriptors::TIcebergSchemaField::new(
            Some(field_id),
            Some(name.to_string()),
            None::<String>,
            None::<Vec<Box<descriptors::TIcebergSchemaField>>>,
        )
    }

    fn test_unpartitioned_desc_table_with_equality_delete_schema(
        table_id: i64,
        table_location: &str,
    ) -> descriptors::TDescriptorTable {
        let iceberg_table = descriptors::TIcebergTable::new(
            Some(table_location.to_string()),
            Some(vec![
                test_int_column("id"),
                test_varchar_column("category"),
                test_int_column("amount"),
            ]),
            Some(descriptors::TIcebergSchema::new(Some(vec![
                test_iceberg_schema_field("id", 11),
                test_iceberg_schema_field("category", 12),
                test_iceberg_schema_field("amount", 13),
            ]))),
            None::<Vec<String>>,
            None::<descriptors::TCompressedPartitionMap>,
            None::<std::collections::BTreeMap<i64, descriptors::THdfsPartition>>,
            Some(descriptors::TIcebergSchema::new(Some(vec![
                test_iceberg_schema_field("id", 11),
                test_iceberg_schema_field("category", 12),
            ]))),
            None::<Vec<descriptors::TIcebergPartitionInfo>>,
            None::<descriptors::TSortOrder>,
            None::<String>,
            None::<i64>,
        );
        let table = descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::ICEBERG_TABLE,
            3,
            0,
            "orders".to_string(),
            "db".to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            Some(iceberg_table),
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        );
        descriptors::TDescriptorTable::new(
            None::<Vec<descriptors::TSlotDescriptor>>,
            Vec::new(),
            Some(vec![table]),
            None::<bool>,
        )
    }

    fn test_unpartitioned_desc_table_without_equality_delete_schema(
        table_id: i64,
        table_location: &str,
    ) -> descriptors::TDescriptorTable {
        let iceberg_table = descriptors::TIcebergTable::new(
            Some(table_location.to_string()),
            Some(vec![
                test_int_column("id"),
                test_varchar_column("category"),
                test_int_column("amount"),
            ]),
            Some(descriptors::TIcebergSchema::new(Some(vec![
                test_iceberg_schema_field("id", 11),
                test_iceberg_schema_field("category", 12),
                test_iceberg_schema_field("amount", 13),
            ]))),
            None::<Vec<String>>,
            None::<descriptors::TCompressedPartitionMap>,
            None::<std::collections::BTreeMap<i64, descriptors::THdfsPartition>>,
            None::<descriptors::TIcebergSchema>,
            None::<Vec<descriptors::TIcebergPartitionInfo>>,
            None::<descriptors::TSortOrder>,
            None::<String>,
            None::<i64>,
        );
        let table = descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::ICEBERG_TABLE,
            3,
            0,
            "orders".to_string(),
            "db".to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            Some(iceberg_table),
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        );
        descriptors::TDescriptorTable::new(
            None::<Vec<descriptors::TSlotDescriptor>>,
            Vec::new(),
            Some(vec![table]),
            None::<bool>,
        )
    }

    fn test_partitioned_desc_table_with_equality_delete_schema_and_no_partition_info(
        table_id: i64,
        table_location: &str,
        metadata: &iceberg::spec::TableMetadata,
    ) -> descriptors::TDescriptorTable {
        let iceberg_table = descriptors::TIcebergTable::new(
            Some(table_location.to_string()),
            Some(vec![test_int_column("id")]),
            Some(descriptors::TIcebergSchema::new(Some(vec![
                test_iceberg_schema_field("id", 42),
            ]))),
            None::<Vec<String>>,
            None::<descriptors::TCompressedPartitionMap>,
            None::<std::collections::BTreeMap<i64, descriptors::THdfsPartition>>,
            Some(descriptors::TIcebergSchema::new(Some(vec![
                test_iceberg_schema_field("id", 42),
            ]))),
            None::<Vec<descriptors::TIcebergPartitionInfo>>,
            None::<descriptors::TSortOrder>,
            Some(serde_json::to_string(metadata).expect("serialize metadata")),
            None::<i64>,
        );
        let table = descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::ICEBERG_TABLE,
            1,
            0,
            "orders".to_string(),
            "db".to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            Some(iceberg_table),
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        );
        descriptors::TDescriptorTable::new(
            None::<Vec<descriptors::TSlotDescriptor>>,
            Vec::new(),
            Some(vec![table]),
            None::<bool>,
        )
    }

    fn test_partitioned_desc_table_with_metadata(
        table_id: i64,
        table_location: &str,
        metadata: &iceberg::spec::TableMetadata,
    ) -> descriptors::TDescriptorTable {
        test_partitioned_desc_table_with_metadata_and_snapshot_id(
            table_id,
            table_location,
            metadata,
            None,
        )
    }

    fn test_partitioned_desc_table_with_metadata_and_snapshot_id(
        table_id: i64,
        table_location: &str,
        metadata: &iceberg::spec::TableMetadata,
        current_snapshot_id: Option<i64>,
    ) -> descriptors::TDescriptorTable {
        let partition_expr = slot_expr(3, types::TPrimitiveType::INT);
        let iceberg_table = descriptors::TIcebergTable::new(
            Some(table_location.to_string()),
            Some(vec![test_int_column("id")]),
            None::<descriptors::TIcebergSchema>,
            None::<Vec<String>>,
            None::<descriptors::TCompressedPartitionMap>,
            None::<std::collections::BTreeMap<i64, descriptors::THdfsPartition>>,
            None::<descriptors::TIcebergSchema>,
            Some(vec![descriptors::TIcebergPartitionInfo::new(
                Some("id".to_string()),
                Some("id_part".to_string()),
                Some("identity".to_string()),
                Some(partition_expr),
            )]),
            None::<descriptors::TSortOrder>,
            Some(serde_json::to_string(metadata).expect("serialize metadata")),
            current_snapshot_id,
        );
        let table = descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::ICEBERG_TABLE,
            1,
            0,
            "orders".to_string(),
            "db".to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            Some(iceberg_table),
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        );
        descriptors::TDescriptorTable::new(
            None::<Vec<descriptors::TSlotDescriptor>>,
            Vec::new(),
            Some(vec![table]),
            None::<bool>,
        )
    }

    fn test_iceberg_table_sink(
        table_id: i64,
        table_location: &str,
    ) -> data_sinks::TIcebergTableSink {
        data_sinks::TIcebergTableSink::new(
            Some(table_location.to_string()),
            Some("parquet".to_string()),
            Some(table_id),
            Some(std::convert::TryFrom::try_from(3).expect("SNAPPY compression")),
            Some(false),
            None::<crate::thrift::cloud_configuration::TCloudConfiguration>,
            None::<i64>,
            Some(1),
            Some(format!("{table_location}/data")),
            Some(7),
            None::<data_sinks::TIcebergPositionDeleteOutputDescriptor>,
        )
    }

    fn s3_cloud_configuration(
        entries: &[(&str, &str)],
    ) -> crate::thrift::cloud_configuration::TCloudConfiguration {
        let cloud_properties = entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<BTreeMap<_, _>>();
        crate::thrift::cloud_configuration::TCloudConfiguration::new(
            None::<crate::thrift::cloud_configuration::TCloudType>,
            None::<Vec<crate::thrift::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties),
            None::<bool>,
        )
    }

    fn test_position_delete_descriptor(
        target_partition_spec_id: i32,
        include_partition_source: bool,
    ) -> data_sinks::TIcebergPositionDeleteOutputDescriptor {
        let mut descriptor = data_sinks::TIcebergPositionDeleteOutputDescriptor::new(
            Some(data_sinks::TIcebergPositionDeleteOutputField::new(
                Some(0),
                Some("file_path".to_string()),
                Some(thrift_type_desc(types::TPrimitiveType::VARCHAR)),
                Some(ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID),
            )),
            Some(data_sinks::TIcebergPositionDeleteOutputField::new(
                Some(1),
                Some("pos".to_string()),
                Some(thrift_type_desc(types::TPrimitiveType::BIGINT)),
                Some(ICEBERG_POSITION_DELETE_POS_FIELD_ID),
            )),
            Some(Vec::new()),
            Some(target_partition_spec_id),
        );
        let partitions = if include_partition_source {
            vec![data_sinks::TIcebergPositionDeletePartitionSourceField::new(
                Some(2),
                Some("id".to_string()),
                Some("id_part".to_string()),
                Some("identity".to_string()),
                Some(42),
            )]
        } else {
            Vec::new()
        };
        descriptor.partition_source_fields = Some(partitions);
        descriptor
    }

    fn test_delete_output_exprs(include_partition_source: bool) -> Vec<exprs::TExpr> {
        let mut exprs = vec![
            slot_expr(1, types::TPrimitiveType::VARCHAR),
            slot_expr(2, types::TPrimitiveType::BIGINT),
        ];
        if include_partition_source {
            exprs.push(slot_expr(3, types::TPrimitiveType::INT));
        }
        exprs
    }

    fn lower_test_sink_input(
        sink: data_sinks::TIcebergTableSink,
        mode: IcebergSinkMode,
        output_exprs: &[exprs::TExpr],
        layout: &Layout,
        desc_tbl: &descriptors::TDescriptorTable,
    ) -> Result<IcebergSinkFactoryInput, String> {
        lower_iceberg_sink_factory_input(&sink, mode, output_exprs, layout, desc_tbl, None, None)
    }

    fn test_identity_partition_metadata(
        table_location: &str,
        _target_schema: SchemaRef,
        partition_spec_id: i32,
    ) -> iceberg::spec::TableMetadata {
        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                    42,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ))])
                .build()
                .expect("iceberg schema"),
        );
        let partition_spec = iceberg::spec::UnboundPartitionSpec::builder()
            .with_spec_id(partition_spec_id)
            .add_partition_field(42, "id_part", iceberg::spec::Transform::Identity)
            .expect("identity partition field")
            .build();
        iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            table_location.to_string(),
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .add_current_schema(iceberg_schema.as_ref().clone())
        .expect("add current schema")
        .add_default_partition_spec(partition_spec)
        .expect("add identity partition spec")
        .build()
        .expect("target metadata")
        .metadata
    }

    #[test]
    fn iceberg_sink_mode_for_type_maps_delete_sinks() {
        assert_eq!(
            iceberg_sink_mode_for_type(data_sinks::TDataSinkType::ICEBERG_DV_SINK),
            IcebergSinkMode::DeletionVectors
        );
        assert_eq!(
            iceberg_sink_mode_for_type(data_sinks::TDataSinkType::ICEBERG_DELETE_SINK),
            IcebergSinkMode::PositionDeletes
        );
        assert_eq!(
            iceberg_sink_mode_for_type(data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK),
            IcebergSinkMode::EqualityDeletes
        );
        assert_eq!(
            iceberg_sink_mode_for_type(data_sinks::TDataSinkType::ICEBERG_TABLE_SINK),
            IcebergSinkMode::Data
        );
    }

    #[test]
    fn sink_s3_config_uses_shared_credentials_aliases_and_policy() {
        let table_location = "s3://bucket-a/warehouse/table-a";
        let mut sink = test_iceberg_table_sink(1, table_location);
        sink.cloud_configuration = Some(s3_cloud_configuration(&[
            ("aws.s3.endpoint_url", " http://localhost:9000 "),
            ("aws.s3.accessKeyId", " ak "),
            ("aws.s3.accessKeySecret", " sk "),
            ("aws.s3.sessionToken", " token "),
            ("aws.s3.region", " us-east-1 "),
            ("aws.s3.enable_path_style_access", "yes"),
            ("aws.s3.max_retries", "7"),
            ("aws.s3.retry_min_delay_ms", "11"),
            ("aws.s3.retry_max_delay_ms", "99"),
            ("aws.s3.request_timeout_ms", "1234"),
            ("aws.s3.io_timeout_ms", "5678"),
        ]));

        let s3 = resolve_sink_s3_config(
            &sink,
            sink.data_location
                .as_deref()
                .expect("test sink has data location"),
        )
        .expect("resolve s3 config")
        .expect("s3 config");

        assert_eq!(s3.bucket, "bucket-a");
        assert_eq!(s3.endpoint, "http://localhost:9000");
        assert_eq!(s3.access_key_id, "ak");
        assert_eq!(s3.access_key_secret, "sk");
        assert_eq!(s3.session_token.as_deref(), Some("token"));
        assert_eq!(s3.region.as_deref(), Some("us-east-1"));
        assert_eq!(s3.enable_path_style_access, Some(true));
        assert_eq!(s3.retry_max_times, Some(7));
        assert_eq!(s3.retry_min_delay_ms, Some(11));
        assert_eq!(s3.retry_max_delay_ms, Some(99));
        assert_eq!(s3.timeout_ms, Some(1234));
        assert_eq!(s3.io_timeout_ms, Some(5678));

        let object_store_config = s3.to_object_store_config();
        assert_eq!(object_store_config.session_token.as_deref(), Some("token"));
        assert_eq!(object_store_config.retry_max_times, Some(7));
        assert_eq!(object_store_config.retry_min_delay_ms, Some(11));
        assert_eq!(object_store_config.retry_max_delay_ms, Some(99));
        assert_eq!(object_store_config.timeout_ms, Some(1234));
        assert_eq!(object_store_config.io_timeout_ms, Some(5678));
    }

    #[test]
    fn sink_s3_config_rejects_invalid_path_style_property() {
        let table_location = "s3://bucket-a/warehouse/table-a";
        let mut sink = test_iceberg_table_sink(1, table_location);
        sink.cloud_configuration = Some(s3_cloud_configuration(&[
            ("aws.s3.endpoint", "http://localhost:9000"),
            ("aws.s3.access_key", "ak"),
            ("aws.s3.secret_key", "sk"),
            ("aws.s3.enable_path_style_access", "maybe"),
        ]));

        let err = resolve_sink_s3_config(
            &sink,
            sink.data_location
                .as_deref()
                .expect("test sink has data location"),
        )
        .expect_err("invalid path style should fail");

        assert!(
            err.contains(
                "iceberg_sink_cloud_properties object-store property aws.s3.enable_path_style_access has invalid boolean value: maybe"
            ),
            "{err}"
        );
    }

    #[test]
    fn position_delete_descriptor_uses_output_expr_root_type() {
        let desc = required_descriptor();
        let output_exprs = vec![
            slot_expr(1, types::TPrimitiveType::BIGINT),
            slot_expr(2, types::TPrimitiveType::BIGINT),
        ];

        let domain = position_delete_descriptor_input_from_thrift(Some(&desc), &output_exprs)
            .expect("domain descriptor");

        assert_eq!(domain.file_path.data_type, DataType::Int64);
        assert_eq!(domain.pos.data_type, DataType::Int64);
    }

    #[test]
    fn equality_delete_lowering_projects_key_schema_with_equality_ids() {
        let table_id = 102;
        let table_location = "file:///warehouse/equality-delete-lower";
        let data_location = format!("{table_location}/custom-data");
        let desc_tbl =
            test_unpartitioned_desc_table_with_equality_delete_schema(table_id, table_location);
        let mut sink = test_iceberg_table_sink(table_id, table_location);
        sink.data_location = Some(data_location.clone());
        sink.target_partition_spec_id = Some(0);
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let output_exprs = vec![
            slot_expr(1, types::TPrimitiveType::INT),
            slot_expr(2, types::TPrimitiveType::VARCHAR),
        ];

        let input = lower_test_sink_input(
            sink,
            IcebergSinkMode::EqualityDeletes,
            &output_exprs,
            &layout,
            &desc_tbl,
        )
        .expect("equality-delete sink input");

        assert_eq!(input.plan.data_location, data_location);
        assert_eq!(
            input
                .plan
                .output_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "category"]
        );
        assert_eq!(
            input
                .plan
                .equality_delete_columns
                .iter()
                .map(|column| column.field_id)
                .collect::<Vec<_>>(),
            vec![11, 12]
        );
    }

    #[test]
    fn equality_delete_lowering_requires_projected_key_schema() {
        let table_id = 103;
        let table_location = "file:///warehouse/equality-delete-no-projected-schema";
        let desc_tbl =
            test_unpartitioned_desc_table_without_equality_delete_schema(table_id, table_location);
        let mut sink = test_iceberg_table_sink(table_id, table_location);
        sink.target_partition_spec_id = Some(0);
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let output_exprs = vec![
            slot_expr(1, types::TPrimitiveType::INT),
            slot_expr(2, types::TPrimitiveType::VARCHAR),
            slot_expr(3, types::TPrimitiveType::INT),
        ];

        let err = match lower_test_sink_input(
            sink,
            IcebergSinkMode::EqualityDeletes,
            &output_exprs,
            &layout,
            &desc_tbl,
        ) {
            Ok(_) => panic!("equality-delete sink should require projected key schema"),
            Err(err) => err,
        };

        assert!(
            err.contains("iceberg_equal_delete_schema"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn equality_delete_lowering_rejects_partitioned_metadata_spec_without_partition_info() {
        let table_id = 104;
        let table_location = "file:///warehouse/equality-delete-partitioned-metadata";
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let metadata =
            test_identity_partition_metadata(table_location, Arc::clone(&target_schema), 7);
        let desc_tbl =
            test_partitioned_desc_table_with_equality_delete_schema_and_no_partition_info(
                table_id,
                table_location,
                &metadata,
            );
        let mut sink = test_iceberg_table_sink(table_id, table_location);
        sink.target_partition_spec_id = Some(metadata.default_partition_spec_id());
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let output_exprs = vec![slot_expr(1, types::TPrimitiveType::INT)];

        let err = match lower_test_sink_input(
            sink,
            IcebergSinkMode::EqualityDeletes,
            &output_exprs,
            &layout,
            &desc_tbl,
        ) {
            Ok(_) => panic!("equality-delete sink should reject partitioned target spec"),
            Err(err) => err,
        };

        assert!(err.contains("unpartitioned"), "unexpected error: {err}");
    }

    #[test]
    fn position_delete_lowering_requires_descriptor() {
        let table_id = 101;
        let table_location = "file:///warehouse/position-delete-no-descriptor";
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let metadata =
            test_identity_partition_metadata(table_location, Arc::clone(&target_schema), 7);
        let desc_tbl =
            test_partitioned_desc_table_with_metadata(table_id, table_location, &metadata);
        let sink = test_iceberg_table_sink(table_id, table_location);
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };

        let err = match lower_test_sink_input(
            sink,
            IcebergSinkMode::PositionDeletes,
            &test_delete_output_exprs(true),
            &layout,
            &desc_tbl,
        ) {
            Ok(_) => panic!("position-delete sink should require descriptor"),
            Err(err) => err,
        };

        assert!(err.contains("UnsupportedPositionDeleteDescriptor"), "{err}");
        assert!(err.contains("descriptor is missing"), "{err}");
    }

    #[test]
    fn position_delete_lowering_rejects_descriptor_order_mismatch() {
        let table_id = 102;
        let table_location = "file:///warehouse/position-delete-order";
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let metadata =
            test_identity_partition_metadata(table_location, Arc::clone(&target_schema), 7);
        let desc_tbl =
            test_partitioned_desc_table_with_metadata(table_id, table_location, &metadata);
        let mut sink = test_iceberg_table_sink(table_id, table_location);
        let mut descriptor = test_position_delete_descriptor(7, true);
        descriptor.file_path.as_mut().unwrap().output_expr_index = Some(1);
        sink.position_delete_output_descriptor = Some(descriptor);
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };

        let err = match lower_test_sink_input(
            sink,
            IcebergSinkMode::PositionDeletes,
            &test_delete_output_exprs(true),
            &layout,
            &desc_tbl,
        ) {
            Ok(_) => panic!("position-delete sink should reject descriptor order mismatch"),
            Err(err) => err,
        };

        assert!(err.contains("UnsupportedPositionDeleteDescriptor"), "{err}");
        assert!(err.contains("file_path output_expr_index"), "{err}");
    }

    #[test]
    fn deletion_vector_lowering_requires_position_delete_input_shape() {
        let table_id = 99;
        let table_location = "file:///warehouse/dv-shape";
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let metadata =
            test_identity_partition_metadata(table_location, Arc::clone(&target_schema), 7);
        let desc_tbl =
            test_partitioned_desc_table_with_metadata(table_id, table_location, &metadata);
        let mut sink = test_iceberg_table_sink(table_id, table_location);
        sink.position_delete_output_descriptor = Some(test_position_delete_descriptor(7, true));
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let output_exprs = test_delete_output_exprs(false);

        let err = match lower_test_sink_input(
            sink,
            IcebergSinkMode::DeletionVectors,
            &output_exprs,
            &layout,
            &desc_tbl,
        ) {
            Ok(_) => panic!("deletion-vector sink should require partition source output exprs"),
            Err(err) => err,
        };

        assert!(
            err.contains("output expr count mismatch")
                || err.contains("output expr index out of bounds"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deletion_vector_lowering_uses_position_delete_schema_and_metadata() {
        let table_id = 100;
        let table_location = "file:///warehouse/dv-metadata";
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let metadata =
            test_identity_partition_metadata(table_location, Arc::clone(&target_schema), 7);
        let desc_tbl =
            test_partitioned_desc_table_with_metadata(table_id, table_location, &metadata);
        let mut sink = test_iceberg_table_sink(table_id, table_location);
        sink.position_delete_output_descriptor = Some(test_position_delete_descriptor(7, true));
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let output_exprs = test_delete_output_exprs(true);

        let input = lower_test_sink_input(
            sink,
            IcebergSinkMode::DeletionVectors,
            &output_exprs,
            &layout,
            &desc_tbl,
        )
        .expect("deletion-vector sink input");

        assert_eq!(
            input.plan.output_schema,
            position_delete_required_schema_for_tests()
        );
        assert!(input.plan.target_table_metadata.is_some());
    }

    #[test]
    fn iceberg_sink_lowering_carries_descriptor_snapshot_id_into_plan() {
        let table_id = 101;
        let table_location = "file:///warehouse/sink-plan-snapshot";
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "42".to_string(),
            )])),
        ]));
        let metadata =
            test_identity_partition_metadata(table_location, Arc::clone(&target_schema), 7);
        let desc_tbl = test_partitioned_desc_table_with_metadata_and_snapshot_id(
            table_id,
            table_location,
            &metadata,
            Some(101),
        );
        let sink = test_iceberg_table_sink(table_id, table_location);
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let output_exprs = vec![slot_expr(3, types::TPrimitiveType::INT)];

        let input = lower_test_sink_input(
            sink,
            IcebergSinkMode::Data,
            &output_exprs,
            &layout,
            &desc_tbl,
        )
        .expect("iceberg sink input");

        assert_eq!(input.plan.target_snapshot_id, Some(101));
    }

    #[test]
    fn validated_file_format_preserves_report_string_case() {
        let (domain_format, report_format) =
            validate_iceberg_sink_file_format("PARQUET").expect("format");

        assert_eq!(domain_format, IcebergFileFormat::Parquet);
        assert_eq!(report_format, "PARQUET");
    }
}
