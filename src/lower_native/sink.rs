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

//! Native proto sink lowering.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use iceberg::spec::TableMetadata;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Compression;

use super::decode_type;
use super::expr::lower_proto_expr;
use crate::common::ids::SlotId;
use crate::connector::iceberg::commit::EqualityDeleteColumn;
use crate::connector::iceberg::delete_file::IcebergFileFormat;
use crate::connector::iceberg::position_delete_descriptor::{
    PositionDeleteDescriptorInput, PositionDeleteExpectedBinding, bind_position_delete_descriptor,
};
use crate::connector::iceberg::schema::{
    IcebergSchemaDescriptor, IcebergSchemaFieldDescriptor, IcebergTableColumn,
    IcebergTableDescriptor, apply_field_id_recursive, build_full_output_schema,
};
use crate::connector::iceberg::sink::build_staged_file_io;
use crate::connector::iceberg::sink_plan::{
    IcebergSinkFactoryInput, IcebergSinkMode, IcebergSinkObjectStoreConfig, IcebergSinkPlan,
    PositionDeleteDataFilePartition,
};
use crate::exec::expr::function::lookup_function;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use crate::fs::object_store_credentials::{ObjectStoreCredentials, ObjectStoreCredentialsSource};
use crate::proto::{common, expr, plan};
use crate::runtime::global_async_runtime::data_block_on;

pub(crate) fn lower_iceberg_write_sink_factory_input(
    sink: &plan::IcebergWriteFragmentSink,
    fragment_output_exprs: &[expr::Expr],
    fragment_output_columns: &[common::OutputColumn],
    layout: &super::layout::Layout,
) -> Result<(IcebergSinkFactoryInput, IcebergSinkMode), String> {
    let spec = sink
        .spec
        .as_ref()
        .ok_or_else(|| "native Iceberg write sink missing spec".to_string())?;
    let mode = iceberg_sink_mode_from_native(spec.mode)?;

    let mut arena = ExprArena::default();
    let lowered_output_exprs = if fragment_output_exprs.is_empty() {
        lower_output_columns_as_slot_refs(fragment_output_columns, sink.input.as_ref(), &mut arena)?
    } else {
        lower_output_exprs(fragment_output_exprs, &mut arena, layout)?
    };

    let target_table = spec
        .target_table
        .as_ref()
        .ok_or_else(|| "native Iceberg write sink missing target_table".to_string())?;
    let writer_columns = if spec.target_columns.is_empty() {
        target_table.columns.as_slice()
    } else {
        spec.target_columns.as_slice()
    };
    if lowered_output_exprs.len() != writer_columns.len() {
        return Err(format!(
            "native Iceberg write sink input column count {} does not match target column count {}",
            lowered_output_exprs.len(),
            writer_columns.len()
        ));
    }

    let iceberg_table = spec
        .iceberg
        .as_ref()
        .ok_or_else(|| "native Iceberg write sink missing iceberg table info".to_string())?;
    let iceberg_table =
        iceberg_table_descriptor_from_native(iceberg_table, &target_table.columns, mode)?;
    let target_partition_spec_id = spec.target_partition_spec_id;

    let target_table_metadata = parse_target_table_metadata(&iceberg_table, mode)?;
    let (partition_source_column_names, partition_column_names, transform_exprs) =
        partition_info_from_metadata(target_table_metadata.as_ref(), target_partition_spec_id)?;
    let position_delete_binding = if matches!(
        mode,
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
    ) {
        let target_schema = build_full_output_schema(&iceberg_table)?;
        let metadata = target_table_metadata.as_ref().ok_or_else(|| {
            format!(
                "native Iceberg {:?} sink requires serialized target table metadata",
                mode
            )
        })?;
        let partition_source_field_ids =
            partition_source_field_ids_from_metadata(metadata, &partition_source_column_names)?;
        let expected = PositionDeleteExpectedBinding {
            target_partition_spec_id,
            partition_source_column_names: partition_source_column_names.clone(),
            partition_column_names: partition_column_names.clone(),
            partition_transform_exprs: transform_exprs.clone(),
            partition_source_field_ids,
            output_expr_count: lowered_output_exprs.len(),
        };
        let binding = bind_position_delete_descriptor_from_native(
            spec.position_delete_output_descriptor.as_ref(),
            expected,
        )?;
        Some((target_schema, binding))
    } else {
        None
    };

    let (output_schema, target_schema, equality_delete_columns) = match mode {
        IcebergSinkMode::Data => {
            let target_schema = build_full_output_schema(&iceberg_table)?;
            if lowered_output_exprs.len() != target_schema.fields().len() {
                return Err(format!(
                    "native Iceberg sink output expr count mismatch: exprs={} columns={}",
                    lowered_output_exprs.len(),
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
            validate_equality_delete_unpartitioned_target_metadata(
                &iceberg_table,
                target_partition_spec_id,
            )?;
            let (schema, columns) = build_equality_delete_output_schema(&iceberg_table)?;
            if lowered_output_exprs.len() != schema.fields().len() {
                return Err(format!(
                    "native Iceberg equality-delete sink expects {} output exprs; got {}",
                    schema.fields().len(),
                    lowered_output_exprs.len()
                ));
            }
            (Arc::clone(&schema), schema, columns)
        }
    };
    let partition_exprs = if mode == IcebergSinkMode::Data {
        build_partition_exprs_from_output_exprs(
            &partition_source_column_names,
            &transform_exprs,
            writer_columns,
            &lowered_output_exprs,
            &mut arena,
        )?
    } else {
        Vec::new()
    };

    let row_lineage_data =
        mode == IcebergSinkMode::Data && schema_has_reserved_row_lineage_columns(&target_schema)?;
    let table_location = if spec.table_location.is_empty() {
        iceberg_table_location(iceberg_table.serialized_metadata.as_deref()).unwrap_or_else(|| {
            spec.iceberg
                .as_ref()
                .map(|t| t.location.clone())
                .unwrap_or_default()
        })
    } else {
        spec.table_location.clone()
    };
    if table_location.is_empty() {
        return Err("native Iceberg write sink missing table location".to_string());
    }
    let data_location = if spec.data_location.is_empty() {
        format!("{}/data", table_location.trim_end_matches('/'))
    } else {
        spec.data_location.clone()
    };
    let object_store_s3 = resolve_native_sink_s3_config(&data_location, &spec.cloud_properties)?;
    let target_snapshot_id = iceberg_table.current_snapshot_id;
    let position_delete_data_file_partitions = if matches!(
        mode,
        IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
    ) {
        let metadata = target_table_metadata.as_ref().ok_or_else(|| {
            "native Iceberg delete sink missing target table metadata".to_string()
        })?;
        build_position_delete_data_file_partition_index(
            metadata,
            target_snapshot_id,
            &table_location,
            object_store_s3.as_ref(),
        )?
    } else {
        HashMap::new()
    };
    let (file_format, report_file_format) = validate_iceberg_sink_file_format(&spec.file_format)?;
    let compression = map_native_compression(spec.compression)?;

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
        partition_exprs,
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        position_delete_binding: position_delete_binding.map(|(_, binding)| binding),
    };

    Ok((
        IcebergSinkFactoryInput {
            name: "ICEBERG_TABLE_SINK".to_string(),
            arena,
            plan,
        },
        mode,
    ))
}

fn lower_output_exprs(
    output_exprs: &[expr::Expr],
    arena: &mut ExprArena,
    layout: &super::layout::Layout,
) -> Result<Vec<ExprId>, String> {
    if output_exprs.is_empty() {
        return Err("native Iceberg sink missing output exprs".to_string());
    }
    output_exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            lower_proto_expr(expr, arena, layout)
                .map_err(|err| format!("native Iceberg sink output_exprs[{idx}]: {err}"))
        })
        .collect()
}

fn lower_output_columns_as_slot_refs(
    output_columns: &[common::OutputColumn],
    input: Option<&plan::IcebergWriteInputBinding>,
    arena: &mut ExprArena,
) -> Result<Vec<ExprId>, String> {
    let selected = match input.and_then(|input| input.kind.as_ref()) {
        Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(true)) | None => {
            output_columns.iter().collect::<Vec<_>>()
        }
        Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(false)) => {
            return Err(
                "native Iceberg write sink root_output_by_ordinal marker must be true".to_string(),
            );
        }
        Some(plan::iceberg_write_input_binding::Kind::OutputOrdinals(ordinals)) => ordinals
            .values
            .iter()
            .map(|ordinal| {
                let idx = usize::try_from(*ordinal).map_err(|_| {
                    format!("native Iceberg write sink output ordinal {ordinal} overflows usize")
                })?;
                output_columns.get(idx).ok_or_else(|| {
                    format!("native Iceberg write sink output ordinal {ordinal} is out of range")
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    if selected.is_empty() {
        return Err("native Iceberg write sink requires at least one output column".to_string());
    }
    selected
        .into_iter()
        .map(|column| {
            let data_type = column
                .r#type
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "native Iceberg write sink output column {} missing type",
                        column.name
                    )
                })
                .and_then(decode_type)?;
            Ok(arena.push_typed(ExprNode::SlotId(SlotId::new(column.column_id)), data_type))
        })
        .collect()
}

fn iceberg_sink_mode_from_native(value: i32) -> Result<IcebergSinkMode, String> {
    let mode = plan::IcebergWriteSinkMode::try_from(value)
        .map_err(|_| format!("unknown native IcebergWriteSinkMode value {value}"))?;
    match mode {
        plan::IcebergWriteSinkMode::Data | plan::IcebergWriteSinkMode::RowLineageData => {
            Ok(IcebergSinkMode::Data)
        }
        plan::IcebergWriteSinkMode::PositionDeletes => Ok(IcebergSinkMode::PositionDeletes),
        plan::IcebergWriteSinkMode::DeletionVectors => Ok(IcebergSinkMode::DeletionVectors),
        plan::IcebergWriteSinkMode::EqualityDeletes => Ok(IcebergSinkMode::EqualityDeletes),
        plan::IcebergWriteSinkMode::Unspecified => {
            Err("native Iceberg write sink mode is unspecified".to_string())
        }
    }
}

fn iceberg_table_descriptor_from_native(
    table: &plan::IcebergTableInfo,
    target_columns: &[plan::ColumnDef],
    mode: IcebergSinkMode,
) -> Result<IcebergTableDescriptor, String> {
    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| "native Iceberg write sink target schema missing".to_string())?;
    let iceberg_schema = IcebergSchemaDescriptor {
        fields: schema
            .fields
            .iter()
            .map(iceberg_schema_field_descriptor_from_native)
            .collect(),
    };
    let columns = target_columns
        .iter()
        .map(column_def_to_table_column)
        .collect::<Result<Vec<_>, _>>()?;
    let equality_delete_schema =
        (mode == IcebergSinkMode::EqualityDeletes).then_some(IcebergSchemaDescriptor {
            fields: iceberg_schema
                .fields
                .iter()
                .filter(|field| columns.iter().any(|column| column.name == field.name))
                .cloned()
                .collect(),
        });
    Ok(IcebergTableDescriptor {
        columns,
        iceberg_schema: Some(iceberg_schema),
        equality_delete_schema,
        partition_info: Vec::new(),
        current_snapshot_id: table.current_snapshot_id,
        serialized_metadata: table.serialized_metadata.clone(),
    })
}

fn iceberg_schema_field_descriptor_from_native(
    field: &plan::IcebergSchemaFieldDef,
) -> IcebergSchemaFieldDescriptor {
    IcebergSchemaFieldDescriptor {
        name: field.name.clone(),
        field_id: Some(field.field_id),
        children: field
            .children
            .iter()
            .map(iceberg_schema_field_descriptor_from_native)
            .collect(),
        initial_default_json: field.initial_default_json.clone(),
    }
}

fn column_def_to_table_column(column: &plan::ColumnDef) -> Result<IcebergTableColumn, String> {
    let data_type = column
        .data_type
        .as_ref()
        .ok_or_else(|| format!("native Iceberg column {} missing data_type", column.name))
        .and_then(decode_type)?;
    Ok(IcebergTableColumn {
        name: column.name.clone(),
        data_type,
        nullable: column.nullable,
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
                    "native Iceberg {:?} sink requires serialized target table metadata",
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
        .map_err(|e| {
            format!(
                "parse native Iceberg {:?} target metadata failed: {e}",
                mode
            )
        })
}

fn iceberg_table_location(serialized_metadata: Option<&str>) -> Option<String> {
    let serialized = serialized_metadata?;
    let value = serde_json::from_str::<serde_json::Value>(serialized).ok()?;
    value
        .get("location")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn partition_info_from_metadata(
    metadata: Option<&TableMetadata>,
    target_partition_spec_id: i32,
) -> Result<(Vec<String>, Vec<String>, Vec<String>), String> {
    let Some(metadata) = metadata else {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    };
    let spec = metadata
        .partition_spec_by_id(target_partition_spec_id)
        .ok_or_else(|| {
            format!(
                "native Iceberg write sink target partition spec id {target_partition_spec_id} not found"
            )
        })?;
    let schema = metadata.current_schema();
    let mut source_names = Vec::with_capacity(spec.fields().len());
    let mut partition_names = Vec::with_capacity(spec.fields().len());
    let mut transforms = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "native Iceberg write sink partition source field id {} not found",
                field.source_id
            )
        })?;
        source_names.push(source.name.clone());
        partition_names.push(field.name.clone());
        transforms.push(field.transform.to_string());
    }
    Ok((source_names, partition_names, transforms))
}

fn build_partition_exprs_from_output_exprs(
    partition_source_column_names: &[String],
    transform_exprs: &[String],
    target_columns: &[plan::ColumnDef],
    output_exprs: &[ExprId],
    arena: &mut ExprArena,
) -> Result<Vec<ExprId>, String> {
    if partition_source_column_names.len() != transform_exprs.len() {
        return Err(format!(
            "native Iceberg write sink partition metadata mismatch: sources={} transforms={}",
            partition_source_column_names.len(),
            transform_exprs.len()
        ));
    }
    if target_columns.len() != output_exprs.len() {
        return Err(format!(
            "native Iceberg write sink partition expr source mismatch: columns={} output_exprs={}",
            target_columns.len(),
            output_exprs.len()
        ));
    }

    let mut expr_by_column_name = HashMap::with_capacity(target_columns.len());
    for (column, expr) in target_columns.iter().zip(output_exprs.iter().copied()) {
        expr_by_column_name.insert(column.name.to_ascii_lowercase(), expr);
    }

    partition_source_column_names
        .iter()
        .zip(transform_exprs.iter())
        .enumerate()
        .map(|(idx, (source_name, transform))| {
            let source_expr = expr_by_column_name
                .get(&source_name.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "native Iceberg write sink partition source column {} is not in target output columns",
                        source_name
                    )
                })?;
            build_partition_expr_from_transform(transform, source_expr, arena)
                .map_err(|err| format!("native Iceberg write sink partition expr[{idx}]: {err}"))
        })
        .collect()
}

fn build_partition_expr_from_transform(
    transform: &str,
    source_expr: ExprId,
    arena: &mut ExprArena,
) -> Result<ExprId, String> {
    let normalized = transform.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "identity" => Ok(source_expr),
        "void" => push_partition_transform_call(
            "__iceberg_transform_void",
            vec![source_expr],
            DataType::Null,
            arena,
        ),
        "year" => push_partition_transform_call(
            "__iceberg_transform_year",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        "month" => push_partition_transform_call(
            "__iceberg_transform_month",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        "day" => push_partition_transform_call(
            "__iceberg_transform_day",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        "hour" => push_partition_transform_call(
            "__iceberg_transform_hour",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        value if value.starts_with("bucket[") && value.ends_with(']') => {
            let width = parse_transform_width(value, "bucket")?;
            let width_expr = arena.push_typed(
                ExprNode::Literal(LiteralValue::Int64(width)),
                DataType::Int64,
            );
            push_partition_transform_call(
                "__iceberg_transform_bucket",
                vec![source_expr, width_expr],
                DataType::Int32,
                arena,
            )
        }
        value if value.starts_with("truncate[") && value.ends_with(']') => {
            let width = parse_transform_width(value, "truncate")?;
            let width_expr = arena.push_typed(
                ExprNode::Literal(LiteralValue::Int64(width)),
                DataType::Int64,
            );
            let source_type = arena
                .data_type(source_expr)
                .cloned()
                .ok_or_else(|| "partition source expression missing data type".to_string())?;
            push_partition_transform_call(
                "__iceberg_transform_truncate",
                vec![source_expr, width_expr],
                source_type,
                arena,
            )
        }
        other => Err(format!("unsupported Iceberg partition transform {other}")),
    }
}

fn parse_transform_width(transform: &str, name: &str) -> Result<i64, String> {
    let prefix = format!("{name}[");
    let raw = transform
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| format!("invalid Iceberg {name} transform syntax: {transform}"))?;
    let width = raw
        .parse::<i64>()
        .map_err(|e| format!("invalid Iceberg {name} transform width {raw}: {e}"))?;
    if width <= 0 {
        return Err(format!(
            "Iceberg {name} transform width must be positive, got {width}"
        ));
    }
    Ok(width)
}

fn push_partition_transform_call(
    name: &str,
    args: Vec<ExprId>,
    data_type: DataType,
    arena: &mut ExprArena,
) -> Result<ExprId, String> {
    let kind = lookup_function(name)
        .ok_or_else(|| format!("native Iceberg partition transform function {name} is missing"))?;
    Ok(arena.push_typed(ExprNode::FunctionCall { kind, args }, data_type))
}

fn validate_equality_delete_unpartitioned_target_metadata(
    iceberg: &IcebergTableDescriptor,
    target_partition_spec_id: i32,
) -> Result<(), String> {
    let Some(serialized) = iceberg.serialized_metadata.as_ref() else {
        return Ok(());
    };
    let metadata = serde_json::from_str::<TableMetadata>(serialized)
        .map_err(|e| format!("parse native Iceberg equality-delete target metadata failed: {e}"))?;
    let spec = metadata
        .partition_spec_by_id(target_partition_spec_id)
        .ok_or_else(|| {
            format!(
                "native Iceberg equality-delete sink target partition spec id {target_partition_spec_id} not found"
            )
        })?;
    if !spec.fields().is_empty() {
        return Err(format!(
            "native Iceberg equality-delete sink currently supports only unpartitioned tables; \
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
        return Err("native Iceberg equality-delete sink requires equality columns".to_string());
    }
    let key_fields = iceberg
        .equality_delete_schema
        .as_ref()
        .ok_or_else(|| {
            "native Iceberg equality-delete sink requires projected key fields".to_string()
        })?
        .fields
        .as_slice();
    if key_fields.is_empty() {
        return Err("native Iceberg equality-delete sink requires equality columns".to_string());
    }
    let mut fields = Vec::with_capacity(key_fields.len());
    let mut equality_columns = Vec::with_capacity(key_fields.len());
    for schema_field in key_fields {
        let column = columns
            .iter()
            .find(|column| column.name == schema_field.name)
            .ok_or_else(|| {
                format!(
                    "native Iceberg equality-delete column {} missing descriptor",
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
                        "native Iceberg sink partition source column {source_name} missing from target metadata schema"
                    )
                })
        })
        .collect()
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "native Iceberg sink field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|e| {
        format!(
            "native Iceberg sink field {} has invalid parquet field id {raw}: {e}",
            field.name()
        )
    })
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

fn resolve_native_sink_s3_config(
    data_location: &str,
    cloud_properties: &HashMap<String, String>,
) -> Result<Option<IcebergSinkObjectStoreConfig>, String> {
    if !crate::fs::access::is_object_store_location_parse_only(data_location)
        .map_err(|e| format!("parse native Iceberg sink data_location {data_location}: {e}"))?
    {
        return Ok(None);
    }
    let (bucket, _data_root) = crate::fs::access::parse_object_store_path_parse_only(data_location)
        .map_err(|e| {
            format!("parse native Iceberg sink object-store data_location {data_location}: {e}")
        })?;
    if cloud_properties.is_empty() {
        return Err(format!(
            "native Iceberg sink object-store path requires cloud_properties: data_location={data_location}"
        ));
    }
    let cloud_properties = cloud_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let credentials = ObjectStoreCredentials::from_aws_s3_properties(
        ObjectStoreCredentialsSource::IcebergSinkCloudProperties,
        &cloud_properties,
    )?;
    Ok(Some(IcebergSinkObjectStoreConfig::from_credentials(
        bucket,
        credentials,
    )))
}

fn validate_iceberg_sink_file_format(
    file_format: &str,
) -> Result<(IcebergFileFormat, String), String> {
    if !file_format.eq_ignore_ascii_case("parquet") {
        return Err(format!(
            "native Iceberg sink does not support {file_format} files; only Parquet is supported"
        ));
    }
    Ok((IcebergFileFormat::Parquet, file_format.to_string()))
}

fn map_native_compression(value: i32) -> Result<Compression, String> {
    let compression = plan::IcebergWriteFileCompression::try_from(value)
        .map_err(|_| format!("unknown native IcebergWriteFileCompression value {value}"))?;
    match compression {
        plan::IcebergWriteFileCompression::Snappy => Ok(Compression::SNAPPY),
        plan::IcebergWriteFileCompression::Unspecified => {
            Err("native Iceberg write file compression is unspecified".to_string())
        }
    }
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
        format!(
            "native Iceberg delete sink target snapshot id {snapshot_id} not found in table metadata"
        )
    })?;
    let file_io = build_staged_file_io(table_location, s3_config)?;
    data_block_on(async {
        let manifest_list = snapshot
            .load_manifest_list(&file_io, metadata)
            .await
            .map_err(|e| {
                format!("load native Iceberg position-delete target manifest list: {e}")
            })?;
        let mut index = HashMap::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(&file_io).await.map_err(|e| {
                format!(
                    "load native Iceberg position-delete data manifest {} failed: {e}",
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
                "native Iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ))
        }
    }
}

fn position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
) -> Result<PositionDeleteDescriptorInput, String> {
    let desc =
        desc.ok_or_else(|| "native position delete output descriptor is missing".to_string())?;
    let file_path = desc
        .file_path
        .as_ref()
        .ok_or_else(|| "native position delete file_path descriptor is missing".to_string())?;
    let pos = desc
        .pos
        .as_ref()
        .ok_or_else(|| "native position delete pos descriptor is missing".to_string())?;
    Ok(PositionDeleteDescriptorInput {
        file_path: position_delete_output_field_from_native("file_path", file_path)?,
        pos: position_delete_output_field_from_native("pos", pos)?,
        partition_source_fields: desc
            .partition_source_fields
            .iter()
            .map(position_delete_partition_source_field_from_native)
            .collect::<Result<Vec<_>, _>>()?,
        target_partition_spec_id: desc.target_partition_spec_id,
    })
}

fn bind_position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
    expected: PositionDeleteExpectedBinding,
) -> Result<
    crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorBinding,
    String,
> {
    let desc = position_delete_descriptor_from_native(desc)?;
    bind_position_delete_descriptor(&desc, &expected).map_err(|err| err.to_bracketed_user_message())
}

fn position_delete_output_field_from_native(
    label: &str,
    field: &plan::PositionDeleteOutputField,
) -> Result<crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField, String>
{
    let output_expr_index = usize::try_from(field.output_expr_index)
        .map_err(|_| format!("native position delete {label} output_expr_index overflows usize"))?;
    let data_type = field
        .data_type
        .as_ref()
        .ok_or_else(|| format!("native position delete {label} data_type is missing"))
        .and_then(decode_type)?;
    Ok(
        crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField {
            output_expr_index,
            name: field.name.clone(),
            data_type,
            field_id: field.field_id,
        },
    )
}

fn position_delete_partition_source_field_from_native(
    field: &plan::PositionDeletePartitionSourceField,
) -> Result<
    crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField,
    String,
> {
    let output_expr_index = usize::try_from(field.output_expr_index).map_err(|_| {
        "native position delete partition source output_expr_index overflows usize".to_string()
    })?;
    let data_type = field
        .data_type
        .as_ref()
        .ok_or_else(|| "native position delete partition source data_type is missing".to_string())
        .and_then(decode_type)?;
    Ok(
        crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField {
            output_expr_index,
            source_column_name: field.source_column_name.clone(),
            partition_field_name: field.partition_field_name.clone(),
            transform_expr: field.transform_expr.clone(),
            source_field_id: field.source_field_id,
            data_type,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::function::FunctionKind;
    use crate::sql::codegen::proto_encode::types::encode_type;

    fn column_def(name: &str) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: None,
            nullable: true,
            write_default_json: None,
            logical_type: None,
        }
    }

    fn typed_column_def(name: &str, data_type: DataType, nullable: bool) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: Some(encode_type(&data_type).expect("encode type")),
            nullable,
            write_default_json: None,
            logical_type: None,
        }
    }

    fn output_column(id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id: id,
            name: name.to_string(),
            r#type: Some(encode_type(&data_type).expect("encode type")),
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, name: &str, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(encode_type(&data_type).expect("encode type")),
            nullable: false,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id: id,
                qualifier: None,
                column: Some(name.to_string()),
            })),
        }
    }

    fn native_identity_partition_metadata(
        table_location: &str,
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

    fn native_iceberg_table_info(serialized_metadata: String) -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: Some("uuid-t".to_string()),
            current_snapshot_id: None,
            schema_id: 1,
            location: "file:///warehouse/t".to_string(),
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![plan::IcebergSchemaFieldDef {
                    field_id: 42,
                    name: "id".to_string(),
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                }],
            }),
            serialized_metadata: Some(serialized_metadata),
            serialized_metadata_rows: None,
        }
    }

    fn position_delete_descriptor(partition_spec_id: i32) -> plan::PositionDeleteDescriptorInput {
        use crate::connector::iceberg::position_delete_descriptor::{
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        };

        plan::PositionDeleteDescriptorInput {
            file_path: Some(plan::PositionDeleteOutputField {
                output_expr_index: 0,
                name: ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
                data_type: Some(encode_type(&DataType::Utf8).expect("encode type")),
                field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            }),
            pos: Some(plan::PositionDeleteOutputField {
                output_expr_index: 1,
                name: ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
                data_type: Some(encode_type(&DataType::Int64).expect("encode type")),
                field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            }),
            partition_source_fields: vec![plan::PositionDeletePartitionSourceField {
                output_expr_index: 2,
                source_column_name: "id".to_string(),
                partition_field_name: "id_part".to_string(),
                transform_expr: "identity".to_string(),
                source_field_id: 42,
                data_type: Some(encode_type(&DataType::Int32).expect("encode type")),
            }],
            target_partition_spec_id: partition_spec_id,
        }
    }

    #[test]
    fn partition_expr_identity_reuses_source_expr() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(7)), DataType::Int64);

        let exprs = build_partition_exprs_from_output_exprs(
            &[String::from("id")],
            &[String::from("identity")],
            &[column_def("id")],
            &[source],
            &mut arena,
        )
        .expect("partition expr");

        assert_eq!(exprs, vec![source]);
    }

    #[test]
    fn partition_expr_bucket_and_truncate_build_transform_calls() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(7)), DataType::Int64);

        let bucket = build_partition_expr_from_transform("bucket[16]", source, &mut arena)
            .expect("bucket expr");
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(bucket) else {
            panic!("expected bucket transform function call");
        };
        assert_eq!(*kind, FunctionKind::IcebergTransformBucket);
        assert_eq!(args[0], source);
        assert!(matches!(
            arena.node(args[1]),
            Some(ExprNode::Literal(LiteralValue::Int64(16)))
        ));
        assert_eq!(arena.data_type(bucket), Some(&DataType::Int32));

        let truncate = build_partition_expr_from_transform("truncate[4]", source, &mut arena)
            .expect("truncate expr");
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(truncate) else {
            panic!("expected truncate transform function call");
        };
        assert_eq!(*kind, FunctionKind::IcebergTransformTruncate);
        assert_eq!(args[0], source);
        assert!(matches!(
            arena.node(args[1]),
            Some(ExprNode::Literal(LiteralValue::Int64(4)))
        ));
        assert_eq!(arena.data_type(truncate), Some(&DataType::Int64));
    }

    #[test]
    fn partition_expr_rejects_missing_source_column() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(7)), DataType::Int64);

        let err = build_partition_exprs_from_output_exprs(
            &[String::from("missing")],
            &[String::from("identity")],
            &[column_def("id")],
            &[source],
            &mut arena,
        )
        .unwrap_err();

        assert!(err.contains("partition source column missing"), "{err}");
    }

    #[test]
    fn deletion_vector_sink_lowers_position_delete_descriptor() {
        let table_location = "file:///warehouse/t";
        let metadata = native_identity_partition_metadata(table_location, 7);
        let partition_spec_id = metadata.default_partition_spec_id();
        let serialized_metadata = serde_json::to_string(&metadata).expect("metadata json");
        let sink = plan::IcebergWriteFragmentSink {
            descriptor_database: "db".to_string(),
            spec: Some(plan::IcebergWriteSinkSpec {
                mode: plan::IcebergWriteSinkMode::DeletionVectors as i32,
                target_table_id: 99,
                target_table: Some(plan::TableDef {
                    name: "t".to_string(),
                    columns: vec![typed_column_def("id", DataType::Int32, false)],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: None,
                }),
                iceberg: Some(native_iceberg_table_info(serialized_metadata)),
                target_columns: vec![
                    typed_column_def(
                        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                        DataType::Utf8,
                        false,
                    ),
                    typed_column_def(
                        crate::exec::row_position::ICEBERG_ROW_POS_COL,
                        DataType::Int64,
                        false,
                    ),
                    typed_column_def("id", DataType::Int32, false),
                ],
                table_location: table_location.to_string(),
                data_location: format!("{table_location}/data"),
                target_partition_spec_id: partition_spec_id,
                cloud_properties: HashMap::new(),
                file_format: "parquet".to_string(),
                compression: plan::IcebergWriteFileCompression::Snappy as i32,
                position_delete_output_descriptor: Some(position_delete_descriptor(
                    partition_spec_id,
                )),
            }),
            input: Some(plan::IcebergWriteInputBinding {
                kind: Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(true)),
            }),
        };
        let output_columns = vec![
            output_column(
                10,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
            ),
            output_column(
                11,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
            ),
            output_column(12, "id", DataType::Int32),
        ];
        let layout = crate::lower_native::layout::layout_from_output_columns(&output_columns)
            .expect("layout");
        let output_exprs = vec![
            column_ref_expr(
                10,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
            ),
            column_ref_expr(
                11,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
            ),
            column_ref_expr(12, "id", DataType::Int32),
        ];

        let (input, mode) =
            lower_iceberg_write_sink_factory_input(&sink, &output_exprs, &output_columns, &layout)
                .expect("native deletion-vector sink input");

        assert_eq!(mode, IcebergSinkMode::DeletionVectors);
        assert!(input.plan.position_delete_binding.is_some());
        assert_eq!(
            input
                .plan
                .output_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["file_path", "pos"]
        );
        assert_eq!(
            input
                .plan
                .target_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id"]
        );
        assert!(input.plan.position_delete_data_file_partitions.is_empty());
    }
}
