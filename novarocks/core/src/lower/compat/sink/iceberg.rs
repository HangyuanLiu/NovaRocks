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
        position_delete_data_file_partition_index_input: None,
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
