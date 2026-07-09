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

mod equality_delete;
mod metadata;
mod partition;
mod position_delete;

use std::collections::HashMap;
use std::sync::Arc;

use super::decode_type;
use super::expr::lower_proto_expr;
use crate::common::ids::SlotId;
use crate::connector::iceberg::position_delete_descriptor::PositionDeleteExpectedBinding;
use crate::connector::iceberg::schema::build_full_output_schema;
use crate::connector::iceberg::sink_plan::{
    IcebergSinkFactoryInput, IcebergSinkMode, IcebergSinkPlan,
};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::proto::{common, expr, plan};

use self::equality_delete::{
    build_equality_delete_output_schema, validate_equality_delete_unpartitioned_target_metadata,
};
use self::metadata::{
    iceberg_table_descriptor_from_native, iceberg_table_location, map_native_compression,
    parse_target_table_metadata, resolve_native_sink_s3_config,
    schema_has_reserved_row_lineage_columns, validate_iceberg_sink_file_format,
};
use self::partition::{
    build_partition_exprs_from_output_exprs, partition_info_from_metadata,
    partition_source_field_ids_from_metadata,
};
use self::position_delete::{
    bind_position_delete_descriptor_from_native, build_position_delete_data_file_partition_index,
};

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
