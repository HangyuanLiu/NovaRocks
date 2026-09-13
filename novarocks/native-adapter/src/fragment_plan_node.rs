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

//! Pure Native fragment physical-node projections.
//!
//! This module owns wire DTO to immutable Execution program projection. It
//! receives already-lowered children and has no Backend runtime, task-context,
//! Connector, or runtime-filter authority.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_execution::exec::chunk::{
    Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema, SlotLayout,
};
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode, cast_array_to_target};
use novarocks_execution::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use novarocks_execution::exec::node::limit::LimitNode;
use novarocks_execution::exec::node::project::ProjectNode;
use novarocks_execution::exec::node::repeat::RepeatNode;
use novarocks_execution::exec::node::set_op::{SetOpKind, SetOpNode};
use novarocks_execution::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use novarocks_execution::exec::node::union_all::UnionAllNode;
use novarocks_execution::exec::node::values::ValuesNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_plan_codec::native_type::decode_type;
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::{common as proto_common, expr, plan};
use novarocks_types::SlotId;

use crate::fragment_error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use crate::fragment_expression::{NativeExpressionInputLayout, decode_expr_at};
use crate::fragment_layout::decode_output_layout;

pub fn decode_zero_input_expression(
    e: &expr::Expr,
    path: FieldPath,
    arena: &mut ExprArena,
) -> Result<ExprId, NativeFragmentDecodeError> {
    decode_expr_at(e, path, arena, &NativeExpressionInputLayout::default())
        .map_err(|e| NativeFragmentDecodeError::from(e.into_protocol()))
}

/// Lowers a zero-input Native `ValuesNode` into an immutable Execution chunk.
pub fn lower_values_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    values: &plan::ValuesNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    _children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let columns = if values.columns.is_empty() {
        &physical.output_columns
    } else {
        &values.columns
    };
    let columns_path = if values.columns.is_empty() {
        physical_output_path
    } else {
        path.clone().field("columns")
    };
    let output_layout =
        decode_output_layout(columns, columns_path).map_err(NativeFragmentDecodeError::from)?;
    let layout = SlotLayout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let chunk = materialize_values_chunk(
        &values.rows,
        columns,
        output_schema.clone(),
        arena,
        path.clone(),
    )?;
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        },
        layout,
        output_schema,
    })
}

/// Materializes immutable values using only the native wire expression decoder.
pub fn materialize_values_chunk(
    rows: &[plan::ExprList],
    columns: &[proto_common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
    path: FieldPath,
) -> Result<Chunk, NativeFragmentDecodeError> {
    if columns.is_empty() {
        return NativeFragmentDecodeError::map_invalid(
            path.field("rows"),
            empty_chunk_with_row_count(rows.len().max(1)),
        );
    }
    if rows.is_empty() {
        let batch = RecordBatch::new_empty(output_schema.arrow_schema_ref());
        return NativeFragmentDecodeError::map_invalid(
            path.field("rows"),
            Chunk::try_new_with_chunk_schema(batch, output_schema),
        );
    }
    let column_count = columns.len();
    if output_schema.slots().len() != column_count {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("columns"),
            format!(
                "ValuesNode output schema width mismatch: columns={}, schema_slots={}",
                column_count,
                output_schema.slots().len()
            ),
        ));
    }
    let target_types = output_schema
        .slots()
        .iter()
        .map(|slot| slot.data_type().clone())
        .collect::<Vec<_>>();
    let mut arrays_by_column = vec![Vec::<ArrayRef>::with_capacity(rows.len()); column_count];
    let one_row = NativeFragmentDecodeError::map_invalid(
        path.clone().field("rows"),
        empty_chunk_with_row_count(1),
    )?;

    for (row_idx, row) in rows.iter().enumerate() {
        if row.values.len() != column_count {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("rows").index(row_idx).field("values"),
                format!(
                    "ValuesNode row {row_idx} width mismatch: expected {column_count}, got {}",
                    row.values.len()
                ),
            ));
        }
        for (col_idx, expr) in row.values.iter().enumerate() {
            let expr_path = path
                .clone()
                .field("rows")
                .index(row_idx)
                .field("values")
                .index(col_idx);
            let expr_id = decode_zero_input_expression(expr, expr_path.clone(), arena)?;
            let array = arena
                .eval(expr_id, &one_row)
                .map_err(|err| NativeFragmentDecodeError::invalid_value(expr_path.clone(), err))?;
            if array.len() != 1 {
                return Err(NativeFragmentDecodeError::inconsistent(
                    expr_path.clone(),
                    format!(
                        "ValuesNode row {row_idx} column {col_idx} evaluated to {} rows, expected 1",
                        array.len()
                    ),
                ));
            }
            let array = NativeFragmentDecodeError::map_invalid(
                expr_path,
                normalize_values_array(row_idx, col_idx, array, &target_types[col_idx]),
            )?;
            arrays_by_column[col_idx].push(array);
        }
    }

    let columns = arrays_by_column
        .into_iter()
        .enumerate()
        .map(|(col_idx, parts)| {
            let refs = parts
                .iter()
                .map(|part| part.as_ref() as &dyn Array)
                .collect::<Vec<_>>();
            concat(&refs).map_err(|err| format!("ValuesNode column {col_idx} concat failed: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| NativeFragmentDecodeError::invalid_value(path.clone().field("rows"), err))?;
    NativeFragmentDecodeError::map_invalid(
        path.field("rows"),
        Chunk::try_new_with_columns(output_schema, columns),
    )
}

/// Lowers a Native `GenerateSeriesNode` whose synthetic input is a Values chunk.
pub fn lower_generate_series_node(
    node: &plan::DistributedNode,
    generate_series: &plan::GenerateSeriesNode,
    path: FieldPath,
    _children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    if generate_series.step == 0 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("step"),
            "GenerateSeriesNode step must not be zero",
        ));
    }
    let param_slots = NativeFragmentDecodeError::map_invalid(
        path.clone().field("output_column_id"),
        generate_series_param_slots(generate_series.output_column_id),
    )?;
    let param_columns = vec![
        bigint_output_column(param_slots[0].as_u32(), "generate_series_start", false),
        bigint_output_column(param_slots[1].as_u32(), "generate_series_end", false),
        bigint_output_column(param_slots[2].as_u32(), "generate_series_step", false),
    ];
    let input_schema = int64_chunk_schema(
        &[
            (param_slots[0], "generate_series_start"),
            (param_slots[1], "generate_series_end"),
            (param_slots[2], "generate_series_step"),
        ],
        path.clone().field("output_column_id"),
    )?;
    let rows = vec![plan::ExprList {
        values: vec![
            int64_literal_expr(generate_series.start),
            int64_literal_expr(generate_series.end),
            int64_literal_expr(generate_series.step),
        ],
    }];
    let input_chunk =
        materialize_values_chunk(&rows, &param_columns, input_schema, arena, path.clone())?;

    let output_columns = [bigint_output_column(
        generate_series.output_column_id,
        if generate_series.column_name.is_empty() {
            "generate_series"
        } else {
            &generate_series.column_name
        },
        false,
    )];
    let output_slot = SlotId::new(generate_series.output_column_id);
    let layout = SlotLayout::for_slots([output_slot]);
    let output_schema = int64_chunk_schema(
        &[(output_slot, output_columns[0].name.as_str())],
        path.clone().field("output_column_id"),
    )?;
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: input_chunk,
                        node_id: node.node_id,
                    }),
                }),
                node_id: node.node_id,
                function_name: "generate_series".to_string(),
                param_slots: param_slots.to_vec(),
                outer_slots: Vec::new(),
                fn_result_slots: vec![SlotId::new(generate_series.output_column_id)],
                fn_result_required: true,
                is_left_join: false,
                param_types: vec![DataType::Int64, DataType::Int64, DataType::Int64],
                ret_types: vec![DataType::Int64],
                output_chunk_schema: output_schema.clone(),
                output_slot_sources: vec![TableFunctionOutputSlot::Result { index: 0 }],
            }),
        },
        layout,
        output_schema,
    })
}

fn normalize_values_array(
    row_idx: usize,
    col_idx: usize,
    array: ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type || matches!(target_type, DataType::Null) {
        return Ok(array);
    }
    cast_array_to_target(&array, target_type).map_err(|err| {
        format!(
            "ValuesNode row {row_idx} column {col_idx} cast from {:?} to {:?} failed: {err}",
            array.data_type(),
            target_type
        )
    })
}

fn empty_chunk_with_row_count(row_count: usize) -> Result<Chunk, String> {
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch = RecordBatch::try_new_with_options(schema, Vec::new(), &options)
        .map_err(|err| format!("build empty values input chunk failed: {err}"))?;
    Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
}

fn int64_chunk_schema(
    slots: &[(SlotId, &str)],
    source_path: FieldPath,
) -> Result<ChunkSchemaRef, NativeFragmentDecodeError> {
    let slots = slots
        .iter()
        .map(|(slot_id, name)| {
            ChunkSlotSchema::try_new_with_field(
                *slot_id,
                Field::new(*name, DataType::Int64, false),
                None,
                None,
            )
        })
        .collect::<Result<Vec<_>, _>>();
    let slots = NativeFragmentDecodeError::map_invalid(source_path.clone(), slots)?;
    NativeFragmentDecodeError::map_invalid(source_path, ChunkSchema::try_new(slots)).map(Arc::new)
}

fn generate_series_param_slots(output_column_id: u32) -> Result<[SlotId; 3], String> {
    let mut slot = u32::MAX;
    let mut slots = Vec::with_capacity(3);
    while slots.len() < 3 {
        if slot != output_column_id {
            slots.push(SlotId::new(slot));
        }
        slot = slot
            .checked_sub(1)
            .ok_or_else(|| "GenerateSeriesNode could not allocate internal slots".to_string())?;
    }
    Ok([slots[0], slots[1], slots[2]])
}

fn bigint_output_column(column_id: u32, name: &str, nullable: bool) -> proto_common::OutputColumn {
    proto_common::OutputColumn {
        column_id,
        name: name.to_string(),
        r#type: Some(bigint_type_desc()),
        nullable,
        is_internal: false,
    }
}

fn bigint_type_desc() -> proto_common::TypeDesc {
    proto_common::TypeDesc {
        kind: Some(proto_common::type_desc::Kind::Scalar(
            proto_common::ScalarType {
                r#type: proto_common::PrimitiveType::Bigint as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
                time_zone: None,
            },
        )),
    }
}

fn int64_literal_expr(value: i64) -> expr::Expr {
    expr::Expr {
        r#type: Some(bigint_type_desc()),
        nullable: false,
        kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
            value: Some(proto_common::LiteralValue {
                value: Some(proto_common::literal_value::Value::IntValue(value)),
            }),
        })),
    }
}

/// Lowers a Native `RepeatNode` after Backend recursion has supplied its child.
pub fn lower_repeat_node(
    node: &plan::DistributedNode,
    repeat: &plan::RepeatNode,
    path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let decoded = (|| -> Result<NativeLoweredPlanNode, NativeFragmentLeafDecodeError> {
        let child = children.pop().expect("validated RepeatNode child");
        let repeat_times = repeat.grouping_ids.len();
        if repeat_times == 0 {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "grouping_ids",
                "RepeatNode grouping_ids is empty",
            ));
        }
        if repeat.repeat_column_ref_ids.len() != repeat_times {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "repeat_column_ref_ids",
                format!(
                    "RepeatNode repeat_column_ref_ids size mismatch: expected {}, got {}",
                    repeat_times,
                    repeat.repeat_column_ref_ids.len()
                ),
            ));
        }
        let all_slot_ids = repeat
            .all_rollup_column_ids
            .iter()
            .copied()
            .map(SlotId::new)
            .collect::<Vec<_>>();
        let all_slot_set = all_slot_ids.iter().copied().collect::<HashSet<_>>();
        let null_slot_ids = repeat
            .repeat_column_ref_ids
            .iter()
            .enumerate()
            .map(|(idx, keep_ids)| {
                let keep = keep_ids
                    .values
                    .iter()
                    .copied()
                    .map(SlotId::new)
                    .collect::<HashSet<_>>();
                for (value_index, slot) in
                    keep_ids.values.iter().copied().map(SlotId::new).enumerate()
                {
                    if !all_slot_set.contains(&slot) {
                        return Err(NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::InvalidValue,
                            "repeat_column_ref_ids",
                            format!(
                                "RepeatNode keep set {idx} contains unknown rollup slot {slot}"
                            ),
                        )
                        .append_index(idx)
                        .append_field("values")
                        .append_index(value_index));
                    }
                }
                let mut nulls = all_slot_ids
                    .iter()
                    .copied()
                    .filter(|slot| !keep.contains(slot))
                    .collect::<Vec<_>>();
                nulls.sort_by_key(|slot| slot.as_u32());
                Ok(nulls)
            })
            .collect::<Result<Vec<_>, NativeFragmentLeafDecodeError>>()?;
        let grouping_slot_ids = repeat
            .grouping_fn_ids
            .iter()
            .map(|entry| SlotId::new(entry.value))
            .collect::<Vec<_>>();
        let grouping_list = repeat_grouping_values(repeat)?;
        let (layout, output_schema) =
            repeat_output_layout_and_schema(&child, &repeat.grouping_fn_ids, &grouping_slot_ids)?;
        Ok(NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::Repeat(RepeatNode {
                    input: Box::new(child.node),
                    node_id: node.node_id,
                    null_slot_ids,
                    grouping_slot_ids,
                    grouping_list,
                    repeat_times,
                }),
            },
            layout,
            output_schema,
        })
    })();
    decoded.map_err(|error| error.into_native(path))
}

fn repeat_output_layout_and_schema(
    child: &NativeLoweredPlanNode,
    grouping_fn_ids: &[plan::NamedUInt32],
    grouping_slot_ids: &[SlotId],
) -> Result<(SlotLayout, ChunkSchemaRef), NativeFragmentLeafDecodeError> {
    let mut slots = child.output_schema.slots().to_vec();
    let mut output_slot_ids = child.layout.order().to_vec();
    for (idx, slot_id) in grouping_slot_ids.iter().copied().enumerate() {
        if child.layout.contains_slot(slot_id) || output_slot_ids.contains(&slot_id) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::DuplicateField,
                "grouping_fn_ids",
                format!("RepeatNode grouping slot {slot_id} duplicates input slot"),
            )
            .append_index(idx)
            .append_field("value"));
        }
        let name = grouping_fn_ids
            .get(idx)
            .map(|entry| entry.name.as_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("__grouping_fn");
        slots.push(ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new(name, DataType::Int64, true),
            None,
            None,
        ));
        output_slot_ids.push(slot_id);
    }
    let layout = SlotLayout::for_slots(output_slot_ids);
    let output_schema = Arc::new(ChunkSchema::try_new(slots).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "grouping_fn_ids",
            error,
        )
    })?);
    Ok((layout, output_schema))
}

fn repeat_grouping_values(
    repeat: &plan::RepeatNode,
) -> Result<Vec<Vec<i64>>, NativeFragmentLeafDecodeError> {
    if repeat.grouping_fn_ids.len() != repeat.grouping_fn_arg_ids.len() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "grouping_fn_arg_ids",
            format!(
                "RepeatNode grouping fn length mismatch: ids={} arg_ids={}",
                repeat.grouping_fn_ids.len(),
                repeat.grouping_fn_arg_ids.len()
            ),
        ));
    }
    let repeat_times = repeat.grouping_ids.len();
    let keep_sets = repeat
        .repeat_column_ref_ids
        .iter()
        .map(|ids| ids.values.iter().copied().collect::<HashSet<_>>())
        .collect::<Vec<_>>();
    repeat
        .grouping_fn_arg_ids
        .iter()
        .enumerate()
        .map(|(idx, args)| {
            if args.values.len() > 63 {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::OutOfRange,
                    "grouping_fn_arg_ids",
                    format!(
                        "RepeatNode grouping_fn_arg_ids[{idx}] has too many arguments: {}",
                        args.values.len()
                    ),
                )
                .append_index(idx)
                .append_field("values"));
            }
            let mut values = Vec::with_capacity(repeat_times);
            for keep in &keep_sets {
                let mut value = 0i64;
                for (arg_idx, column_id) in args.values.iter().enumerate() {
                    if !keep.contains(column_id) {
                        let reverse_bit_pos = args.values.len() - 1 - arg_idx;
                        value |= 1i64 << reverse_bit_pos;
                    }
                }
                values.push(value);
            }
            Ok(values)
        })
        .collect()
}

pub fn lower_set_op_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    set_op: &plan::SetOpNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let kind = plan::PlanSetOpKind::try_from(set_op.kind).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("kind"),
            format!("SetOpNode unknown kind {}", set_op.kind),
        )
    })?;
    let (output_columns, output_columns_path) = if set_op.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (&set_op.output_columns, path.clone().field("output_columns"))
    };
    let output_layout = decode_output_layout(output_columns, output_columns_path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let layout = SlotLayout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let inputs = normalize_set_op_inputs(
        node.node_id,
        children,
        &set_op.child_output_columns,
        output_columns,
        output_columns_path,
        output_schema.clone(),
        path.clone(),
        arena,
    )?;
    match kind {
        plan::PlanSetOpKind::UnionAll => Ok(NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: node.node_id,
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Intersect => Ok(NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Intersect,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Except => Ok(NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Except,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::UnionDistinct => Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("kind"),
            "UnionDistinct native proto node lowering is not implemented",
        )),
        plan::PlanSetOpKind::Unspecified => Err(NativeFragmentDecodeError::invalid_enum(
            path.field("kind"),
            "SetOpNode kind is unspecified",
        )),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
fn normalize_set_op_inputs(
    node_id: i32,
    children: Vec<NativeLoweredPlanNode>,
    child_output_columns: &[plan::OutputColumnList],
    output_columns: &[proto_common::OutputColumn],
    output_columns_path: FieldPath,
    output_schema: ChunkSchemaRef,
    path: FieldPath,
    arena: &mut ExprArena,
) -> Result<Vec<ExecNode>, NativeFragmentDecodeError> {
    if child_output_columns.is_empty() {
        return normalize_set_op_inputs_by_position(
            node_id,
            children,
            output_columns,
            output_columns_path,
            output_schema,
            path,
            arena,
        );
    }
    if child_output_columns.len() != children.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("child_output_columns"),
            format!(
                "SetOpNode child_output_columns size mismatch: expected {}, got {}",
                children.len(),
                child_output_columns.len()
            ),
        ));
    }
    let output_layout = decode_output_layout(output_columns, output_columns_path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let output_slots = output_layout.slot_ids().to_vec();
    let output_slot_schemas = output_layout.slot_schemas().to_vec();
    children.into_iter().zip(child_output_columns.iter()).enumerate().map(|(idx, (child, child_columns))| {
        let child_path = path.clone().field("child_output_columns").index(idx).field("columns");
        if child_columns.columns.len() != output_columns.len() { return Err(NativeFragmentDecodeError::inconsistent(child_path.clone(), format!("SetOpNode child {idx} output width mismatch: expected {}, got {}", output_columns.len(), child_columns.columns.len()))); }
        let expected_child_layout = SlotLayout::for_slots(decode_output_layout(&child_columns.columns, child_path.clone()).map_err(NativeFragmentDecodeError::from)?.slot_ids().iter().copied());
        if expected_child_layout.order() != child.layout.order() { return Err(NativeFragmentDecodeError::inconsistent(child_path.clone(), format!("SetOpNode child {idx} output columns do not match child layout: columns={:?} layout={:?}", expected_child_layout.order(), child.layout.order()))); }
        let exprs = child_columns.columns.iter().enumerate().map(|(col_idx, col)| {
            let slot = SlotId::new(col.column_id);
            let data_type = col.r#type.as_ref().ok_or_else(|| NativeFragmentDecodeError::missing(child_path.clone().index(col_idx).field("type"), format!("SetOpNode child {idx} column {} type missing", col.column_id)))?;
            let data_type = NativeFragmentDecodeError::map_invalid(child_path.clone().index(col_idx).field("type"), decode_type(data_type))?;
            Ok(arena.push_typed(ExprNode::SlotId(slot), data_type))
        }).collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
        Ok(ExecNode { kind: ExecNodeKind::Project(ProjectNode { input: Box::new(child.node), node_id, is_subordinate: true, exprs, expr_slot_ids: output_slots.clone(), expr_slot_schemas: Some(output_slot_schemas.clone()), output_indices: None, output_chunk_schema: output_schema.clone() }) })
    }).collect()
}

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
fn normalize_set_op_inputs_by_position(
    node_id: i32,
    children: Vec<NativeLoweredPlanNode>,
    output_columns: &[proto_common::OutputColumn],
    output_columns_path: FieldPath,
    output_schema: ChunkSchemaRef,
    path: FieldPath,
    arena: &mut ExprArena,
) -> Result<Vec<ExecNode>, NativeFragmentDecodeError> {
    let output_layout = decode_output_layout(output_columns, output_columns_path)
        .map_err(NativeFragmentDecodeError::from)?;
    let output_slots = output_layout.slot_ids().to_vec();
    let output_slot_schemas = output_layout.slot_schemas().to_vec();
    children.into_iter().enumerate().map(|(idx, child)| {
        if child.layout.order().len() != output_slots.len() { return Err(NativeFragmentDecodeError::inconsistent(path.clone().field("child_output_columns").index(idx), format!("SetOpNode child {idx} width mismatch without child_output_columns: expected {}, got {}", output_slots.len(), child.layout.order().len()))); }
        if child.layout.order() == output_slots.as_slice() { return Ok(child.node); }
        let exprs = child.layout.order().iter().copied().map(|slot| {
            let data_type = child.output_schema.slot(slot).ok_or_else(|| NativeFragmentDecodeError::inconsistent(path.clone().field("child_output_columns").index(idx), format!("SetOpNode child {idx} slot {} missing from child output schema", slot)))?.data_type().clone();
            Ok(arena.push_typed(ExprNode::SlotId(slot), data_type))
        }).collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
        Ok(ExecNode { kind: ExecNodeKind::Project(ProjectNode { input: Box::new(child.node), node_id, is_subordinate: true, exprs, expr_slot_ids: output_slots.clone(), expr_slot_schemas: Some(output_slot_schemas.clone()), output_indices: None, output_chunk_schema: output_schema.clone() }) })
    }).collect()
}

/// Validates a Native `RedistributeNode` while preserving its immutable child program.
pub fn lower_redistribute_node(
    physical: &plan::PlanNode,
    redistribute: &plan::RedistributeNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated RedistributeNode child");
    let mode = redistribute
        .mode
        .as_ref()
        .and_then(|mode| mode.mode.as_ref())
        .ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("mode").field("mode"),
                "RedistributeNode mode missing",
            )
        })?;
    match mode {
        plan::redistribute_mode::Mode::Gather(true)
        | plan::redistribute_mode::Mode::Broadcast(true) => {}
        plan::redistribute_mode::Mode::Hash(hash) => {
            if hash.cols.is_empty() {
                return Err(NativeFragmentDecodeError::missing(
                    path.clone().field("mode").field("hash").field("cols"),
                    "RedistributeNode hash mode requires cols",
                ));
            }
            for col in &hash.cols {
                NativeFragmentDecodeError::map_invalid(
                    path.clone().field("mode").field("hash").field("cols"),
                    child.layout.resolve_column_id(*col).ok_or_else(|| {
                        format!("ColumnRef column_id={col} not found in input layout")
                    }),
                )?;
            }
        }
        plan::redistribute_mode::Mode::Gather(false)
        | plan::redistribute_mode::Mode::Broadcast(false) => {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone().field("mode"),
                "RedistributeNode boolean mode must be true",
            ));
        }
    }
    let input = NativeExpressionInputLayout::from_slot_ids(child.layout.order().iter().copied());
    for (idx, expr) in redistribute.partition_exprs.iter().enumerate() {
        decode_expr_at(
            expr,
            path.clone().field("partition_exprs").index(idx),
            arena,
            &input,
        )
        .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))?;
    }
    let (output_columns, output_path) = if redistribute.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (
            &redistribute.output_columns,
            path.clone().field("output_columns"),
        )
    };
    if output_columns.is_empty() {
        return Ok(child);
    }
    let output_layout = decode_output_layout(output_columns, output_path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let layout = SlotLayout::for_slots(output_layout.slot_ids().iter().copied());
    if layout.order() != child.layout.order() {
        return Err(NativeFragmentDecodeError::inconsistent(
            output_path.clone(),
            format!(
                "RedistributeNode output columns must preserve child order: child={:?} output={:?}",
                child.layout.order(),
                layout.order()
            ),
        ));
    }
    Ok(NativeLoweredPlanNode {
        node: child.node,
        layout,
        output_schema: output_layout.chunk_schema(),
    })
}

/// One fully lowered physical node and its immutable output contract.
#[derive(Clone, Debug)]
pub struct NativeLoweredPlanNode {
    pub node: ExecNode,
    pub layout: SlotLayout,
    pub output_schema: ChunkSchemaRef,
}

/// Lowers a Native `LimitNode` after Backend recursion has supplied its child.
pub fn lower_limit_node(
    node: &plan::DistributedNode,
    limit_node: &plan::LimitNode,
    path: FieldPath,
    node_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated LimitNode child");
    let payload_limit = parse_optional_nonnegative_i64(limit_node.limit, "LimitNode.limit")
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "limit", error)
                .into_native(path.clone())
        })?;
    let outer_limit = parse_distributed_limit(node.limit, "LimitNode DistributedNode.limit")
        .map_err(|error| {
            NativeFragmentDecodeError::out_of_range(node_path.field("limit"), error)
        })?;
    let limit = merge_limits("LimitNode", payload_limit, outer_limit).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "limit",
            error,
        )
        .into_native(path.clone())
    })?;
    let offset = parse_optional_nonnegative_i64(limit_node.offset, "LimitNode.offset")
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "offset", error)
                .into_native(path)
        })?
        .unwrap_or(0);
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Limit(LimitNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                limit,
                offset,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

/// Lowers a Native `AssertOneRowNode` after Backend recursion has supplied its child.
pub fn lower_assert_one_row_node(
    node: &plan::DistributedNode,
    assert: &plan::AssertOneRowNode,
    path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let decoded = (|| -> Result<NativeLoweredPlanNode, NativeFragmentLeafDecodeError> {
        let child = children.pop().expect("validated AssertOneRowNode child");
        let desired_num_rows = parse_optional_nonnegative_i64(
            assert.desired_num_rows,
            "AssertOneRowNode.desired_num_rows",
        )
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::OutOfRange,
                "desired_num_rows",
                error,
            )
        })?
        .or(Some(1));
        let assertion = lower_row_count_assertion(assert.assertion)?;
        let mode = if assert.group_key_column_ids.is_empty() {
            if !assert.group_key_labels.is_empty() || assert.keyed_message_prefix.is_some() {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::MissingField,
                    "group_key_column_ids",
                    "AssertOneRowNode group_key_column_ids is required when keyed metadata is present",
                ));
            }
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string: Some(assert.subquery_text.clone()),
            }
        } else {
            if desired_num_rows != Some(1) || !matches!(assertion, Assertion::Le) {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "assertion",
                    "AssertOneRowNode keyed assertions only support desired_num_rows <= 1",
                ));
            }
            if !assert.group_key_labels.is_empty()
                && assert.group_key_labels.len() != assert.group_key_column_ids.len()
            {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "group_key_labels",
                    format!(
                        "AssertOneRowNode group_key_labels length mismatch: key_columns={} labels={}",
                        assert.group_key_column_ids.len(),
                        assert.group_key_labels.len()
                    ),
                ));
            }
            let key_slots = assert
                .group_key_column_ids
                .iter()
                .enumerate()
                .map(|(index, column_id)| {
                    child
                        .layout
                        .resolve_column_id(*column_id)
                        .ok_or_else(|| {
                            format!("ColumnRef column_id={column_id} not found in input layout")
                        })
                        .map_err(|error| {
                            NativeFragmentLeafDecodeError::at_field(
                                ProtocolErrorKind::InvalidValue,
                                "group_key_column_ids",
                                format!("AssertOneRowNode group key: {error}"),
                            )
                            .append_index(index)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key_labels = if assert.group_key_labels.is_empty() {
                assert
                    .group_key_column_ids
                    .iter()
                    .map(|column_id| format!("column_{column_id}"))
                    .collect()
            } else {
                assert.group_key_labels.clone()
            };
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix: assert
                    .keyed_message_prefix
                    .clone()
                    .unwrap_or_else(|| "assert_num_rows failed".to_string()),
            }
        };
        Ok(NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                    input: Box::new(child.node),
                    node_id: node.node_id,
                    mode,
                }),
            },
            layout: child.layout,
            output_schema: child.output_schema,
        })
    })();
    decoded.map_err(|error| error.into_native(path))
}

fn lower_row_count_assertion(value: i32) -> Result<Assertion, NativeFragmentLeafDecodeError> {
    match value {
        value if value == plan::RowCountAssertion::Unspecified as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Eq as i32 => Ok(Assertion::Eq),
        value if value == plan::RowCountAssertion::Ne as i32 => Ok(Assertion::Ne),
        value if value == plan::RowCountAssertion::Lt as i32 => Ok(Assertion::Lt),
        value if value == plan::RowCountAssertion::Le as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Gt as i32 => Ok(Assertion::Gt),
        value if value == plan::RowCountAssertion::Ge as i32 => Ok(Assertion::Ge),
        other => Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "assertion",
            format!("AssertOneRowNode assertion {other} is not supported"),
        )),
    }
}

/// Parses an optional native non-negative integer into a platform index.
pub fn parse_optional_nonnegative_i64(
    value: Option<i64>,
    label: &str,
) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            if value < 0 {
                Err(format!("{label} must be >= 0, got {value}"))
            } else {
                Ok(value as usize)
            }
        })
        .transpose()
}

/// Parses the `DistributedNode.limit` sentinel form.
pub fn parse_distributed_limit(value: i64, label: &str) -> Result<Option<usize>, String> {
    if value == -1 {
        Ok(None)
    } else if value < 0 {
        Err(format!("{label} must be -1 or >= 0, got {value}"))
    } else {
        Ok(Some(value as usize))
    }
}

/// Combines physical and distributed limit declarations fail-closed.
pub fn merge_limits(
    node_kind: &str,
    payload_limit: Option<usize>,
    outer_limit: Option<usize>,
) -> Result<Option<usize>, String> {
    match (payload_limit, outer_limit) {
        (Some(left), Some(right)) if left != right => Err(format!(
            "{node_kind} payload limit {left} conflicts with DistributedNode.limit {right}"
        )),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        NativeLoweredPlanNode, lower_assert_one_row_node, lower_limit_node, merge_limits,
        parse_distributed_limit, parse_optional_nonnegative_i64,
    };
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema, SlotLayout};
    use novarocks_execution::exec::node::assert::AssertNumRowsMode;
    use novarocks_execution::exec::node::values::ValuesNode;
    use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
    use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
    use novarocks_proto_models::plan;
    use novarocks_types::SlotId;

    fn child() -> NativeLoweredPlanNode {
        let output_schema = Arc::new(ChunkSchema::empty());
        NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::try_new_with_columns(Arc::clone(&output_schema), Vec::new())
                        .expect("empty values chunk"),
                    node_id: 1,
                }),
            },
            layout: SlotLayout::for_slots(Vec::new()),
            output_schema,
        }
    }

    fn child_with_key() -> NativeLoweredPlanNode {
        let slot = SlotId::new(1);
        let output_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot,
                Field::new("id", DataType::Int64, false),
                None,
                None,
            )])
            .expect("keyed chunk schema"),
        );
        NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::try_new_with_columns(
                        Arc::clone(&output_schema),
                        vec![Arc::new(Int64Array::from(vec![10]))],
                    )
                    .expect("keyed values chunk"),
                    node_id: 1,
                }),
            },
            layout: SlotLayout::for_slots(vec![slot]),
            output_schema,
        }
    }

    #[test]
    fn limit_shapes_preserve_native_sentinel_and_conflict_rules() {
        assert_eq!(
            parse_optional_nonnegative_i64(Some(5), "limit"),
            Ok(Some(5))
        );
        assert_eq!(parse_optional_nonnegative_i64(None, "limit"), Ok(None));
        assert!(parse_optional_nonnegative_i64(Some(-1), "limit").is_err());
        assert_eq!(
            parse_distributed_limit(-1, "DistributedNode.limit"),
            Ok(None)
        );
        assert_eq!(
            parse_distributed_limit(0, "DistributedNode.limit"),
            Ok(Some(0))
        );
        assert!(parse_distributed_limit(-2, "DistributedNode.limit").is_err());
        assert_eq!(merge_limits("LimitNode", Some(2), Some(2)), Ok(Some(2)));
        assert!(merge_limits("LimitNode", Some(2), Some(3)).is_err());
    }

    #[test]
    fn lower_limit_preserves_native_shape_and_error_paths() {
        let root = FieldPath::root("plan_fragment").field("root");
        let node = plan::DistributedNode {
            node_id: 7,
            limit: -1,
            ..Default::default()
        };
        let lowered = lower_limit_node(
            &node,
            &plan::LimitNode {
                limit: Some(3),
                offset: Some(1),
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("limit"),
            root.clone(),
            vec![child()],
        )
        .expect("valid limit");
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected LimitNode");
        };
        assert_eq!(limit.node_id, 7);
        assert_eq!(limit.limit, Some(3));
        assert_eq!(limit.offset, 1);

        let error = lower_limit_node(
            &node,
            &plan::LimitNode {
                limit: Some(-2),
                offset: None,
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("limit"),
            root.clone(),
            vec![child()],
        )
        .expect_err("negative payload limit must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.limit.limit"
        );

        let outer_error = lower_limit_node(
            &plan::DistributedNode {
                node_id: 7,
                limit: -2,
                ..Default::default()
            },
            &plan::LimitNode::default(),
            root.clone()
                .field("payload")
                .field("physical")
                .field("limit"),
            root,
            vec![child()],
        )
        .expect_err("negative distributed limit must fail");
        assert_eq!(
            outer_error
                .protocol()
                .expect("protocol error")
                .path()
                .to_string(),
            "plan_fragment.root.limit"
        );
    }

    #[test]
    fn lower_assert_one_row_preserves_global_contract_and_error_path() {
        let root = FieldPath::root("plan_fragment").field("root");
        let lowered = lower_assert_one_row_node(
            &plan::DistributedNode {
                node_id: 9,
                ..Default::default()
            },
            &plan::AssertOneRowNode {
                subquery_text: "subquery".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                ..Default::default()
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("assert_one_row"),
            vec![child()],
        )
        .expect("valid global assertion");
        let ExecNodeKind::AssertNumRows(assertion) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        assert_eq!(assertion.node_id, 9);

        let error = lower_assert_one_row_node(
            &plan::DistributedNode {
                node_id: 9,
                ..Default::default()
            },
            &plan::AssertOneRowNode {
                assertion: 999,
                ..Default::default()
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("assert_one_row"),
            vec![child()],
        )
        .expect_err("unknown assertion must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.assertion"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn lower_assert_one_row_preserves_keyed_contract_and_paths() {
        let root = FieldPath::root("plan_fragment").field("root");
        let path = root
            .clone()
            .field("payload")
            .field("physical")
            .field("assert_one_row");
        let node = plan::DistributedNode {
            node_id: 9,
            ..Default::default()
        };
        let lowered = lower_assert_one_row_node(
            &node,
            &plan::AssertOneRowNode {
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![1],
                group_key_labels: vec!["_row_id".to_string()],
                keyed_message_prefix: Some("MOR UPDATE matched target row".to_string()),
                ..Default::default()
            },
            path.clone(),
            vec![child_with_key()],
        )
        .expect("valid keyed assertion");
        let ExecNodeKind::AssertNumRows(assertion) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        let AssertNumRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            message_prefix,
        } = assertion.mode
        else {
            panic!("expected keyed assertion");
        };
        assert_eq!(key_slots, vec![SlotId::new(1)]);
        assert_eq!(key_labels, vec!["_row_id"]);
        assert_eq!(message_prefix, "MOR UPDATE matched target row");

        let negative = lower_assert_one_row_node(
            &node,
            &plan::AssertOneRowNode {
                desired_num_rows: Some(-1),
                ..Default::default()
            },
            path.clone(),
            vec![child_with_key()],
        )
        .expect_err("negative desired rows must fail");
        assert_eq!(
            negative
                .protocol()
                .expect("protocol error")
                .path()
                .to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.desired_num_rows"
        );

        let missing_key = lower_assert_one_row_node(
            &node,
            &plan::AssertOneRowNode {
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![999],
                ..Default::default()
            },
            path,
            vec![child_with_key()],
        )
        .expect_err("unknown group key must fail");
        assert_eq!(
            missing_key
                .protocol()
                .expect("protocol error")
                .path()
                .to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.group_key_column_ids[0]"
        );
    }
}

#[cfg(test)]
mod values_and_generate_series_tests {
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;
    use novarocks_plan_codec::encode_native_type as encode_type;

    use super::*;

    fn type_desc(data_type: &DataType) -> proto_common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> proto_common::OutputColumn {
        proto_common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable,
            is_internal: false,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(proto_common::LiteralValue {
                    value: Some(proto_common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn null_literal() -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Null)),
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(proto_common::LiteralValue {
                    value: Some(proto_common::literal_value::Value::NullValue(true)),
                }),
            })),
        }
    }

    fn node(node_id: i32) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            limit: -1,
            ..Default::default()
        }
    }

    #[test]
    fn values_materialize_through_adapter_owned_zero_input_decoder() {
        let columns = vec![output_column(1, "id", DataType::Int64, true)];
        let physical = plan::PlanNode {
            output_columns: columns.clone(),
            ..Default::default()
        };
        let values = plan::ValuesNode {
            rows: vec![
                plan::ExprList {
                    values: vec![int_literal(10)],
                },
                plan::ExprList {
                    values: vec![null_literal()],
                },
            ],
            columns: columns.clone(),
        };
        let mut arena = ExprArena::default();
        let lowered = lower_values_node(
            &node(10),
            &physical,
            &values,
            FieldPath::root("plan_fragment").field("values"),
            FieldPath::root("plan_fragment").field("output_columns"),
            Vec::new(),
            &mut arena,
        )
        .expect("lower values");
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.chunk.len(), 2);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
        let column = values
            .chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("id column");
        assert_eq!(column.data_type(), &DataType::Int64);
        let column = column.as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!(column.value(0), 10);
        assert!(column.is_null(1));
    }

    #[test]
    fn zero_column_values_keep_seed_row_semantics() {
        let physical = plan::PlanNode::default();
        let values = plan::ValuesNode {
            rows: Vec::new(),
            columns: Vec::new(),
        };
        let mut arena = ExprArena::default();
        let lowered = lower_values_node(
            &node(10),
            &physical,
            &values,
            FieldPath::root("plan_fragment").field("values"),
            FieldPath::root("plan_fragment").field("output_columns"),
            Vec::new(),
            &mut arena,
        )
        .expect("lower empty zero-column values");
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.chunk.len(), 1);
        assert!(lowered.layout.order().is_empty());
        assert!(lowered.output_schema.slot_ids().is_empty());
    }

    #[test]
    fn generate_series_uses_adapter_owned_synthetic_values() {
        let mut arena = ExprArena::default();
        let lowered = lower_generate_series_node(
            &node(20),
            &plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 2,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: 9,
            },
            FieldPath::root("plan_fragment").field("generate_series"),
            Vec::new(),
            &mut arena,
        )
        .expect("lower generate series");
        let ExecNodeKind::TableFunction(table_function) = lowered.node.kind else {
            panic!("expected TableFunction");
        };
        assert_eq!(table_function.function_name, "generate_series");
        assert_eq!(lowered.layout.order(), &[SlotId::new(9)]);
        let ExecNodeKind::Values(input) = table_function.input.kind else {
            panic!("expected synthetic Values input");
        };
        for (slot, expected) in table_function.param_slots.iter().zip([1, 5, 2]) {
            let column = input
                .chunk
                .column_by_slot_id(*slot)
                .expect("parameter column");
            let values = column.as_any().downcast_ref::<Int64Array>().expect("int64");
            assert_eq!(values.value(0), expected);
        }

        let err = lower_generate_series_node(
            &node(20),
            &plan::GenerateSeriesNode {
                step: 0,
                output_column_id: 9,
                ..Default::default()
            },
            FieldPath::root("plan_fragment").field("generate_series"),
            Vec::new(),
            &mut arena,
        )
        .expect_err("zero step must fail");
        assert!(err.to_string().contains("step must not be zero"));
    }
}

#[cfg(test)]
mod repeat_projection_tests {
    use arrow::array::Int64Array;

    use super::*;

    fn child() -> NativeLoweredPlanNode {
        let slots = vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("a", DataType::Int64, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("b", DataType::Int64, true),
                None,
                None,
            ),
        ];
        let output_schema = Arc::new(ChunkSchema::try_new(slots).expect("schema"));
        NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::try_new_with_columns(
                        Arc::clone(&output_schema),
                        vec![
                            Arc::new(Int64Array::from(vec![1])),
                            Arc::new(Int64Array::from(vec![2])),
                        ],
                    )
                    .expect("chunk"),
                    node_id: 1,
                }),
            },
            layout: SlotLayout::for_slots([SlotId::new(1), SlotId::new(2)]),
            output_schema,
        }
    }

    fn node() -> plan::DistributedNode {
        plan::DistributedNode {
            node_id: 20,
            limit: -1,
            ..Default::default()
        }
    }

    fn repeat() -> plan::RepeatNode {
        plan::RepeatNode {
            repeat_column_ref_ids: vec![
                plan::UInt32List { values: vec![1, 2] },
                plan::UInt32List { values: vec![1] },
                plan::UInt32List { values: vec![2] },
                plan::UInt32List { values: Vec::new() },
            ],
            grouping_ids: vec![0, 1, 2, 3],
            all_rollup_column_ids: vec![1, 2],
            grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1, 2] }],
            grouping_fn_ids: vec![plan::NamedUInt32 {
                name: "__grouping_fn_0".to_string(),
                value: 9,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn repeat_projection_preserves_grouping_bit_order_and_schema() {
        let lowered = lower_repeat_node(
            &node(),
            &repeat(),
            FieldPath::root("plan_fragment").field("repeat"),
            vec![child()],
        )
        .expect("lower repeat");
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.grouping_list, vec![vec![0, 1, 2, 3]]);
        assert_eq!(
            lowered.layout.order(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(9)]
        );
        assert_eq!(
            lowered
                .output_schema
                .field(2)
                .expect("grouping field")
                .name(),
            "__grouping_fn_0"
        );
    }

    #[test]
    fn repeat_projection_preserves_exact_invalid_wire_paths() {
        let mut empty = repeat();
        empty.grouping_ids.clear();
        empty.repeat_column_ref_ids.clear();
        let error = lower_repeat_node(
            &node(),
            &empty,
            FieldPath::root("plan_fragment").field("repeat"),
            vec![child()],
        )
        .expect_err("empty grouping ids fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.repeat.grouping_ids"
        );

        let mut unknown = repeat();
        unknown.repeat_column_ref_ids[0].values = vec![999];
        let error = lower_repeat_node(
            &node(),
            &unknown,
            FieldPath::root("plan_fragment").field("repeat"),
            vec![child()],
        )
        .expect_err("unknown rollup slot fails");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.repeat.repeat_column_ref_ids[0].values[0]"
        );
    }
}

#[cfg(test)]
mod redistribute_projection_tests {
    use arrow::array::Int64Array;
    use novarocks_plan_codec::encode_native_type as encode_type;

    use super::*;

    fn child() -> NativeLoweredPlanNode {
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("id", DataType::Int64, true),
                None,
                None,
            )])
            .expect("schema"),
        );
        NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::try_new_with_columns(
                        Arc::clone(&schema),
                        vec![Arc::new(Int64Array::from(vec![1]))],
                    )
                    .expect("chunk"),
                    node_id: 1,
                }),
            },
            layout: SlotLayout::for_slots([SlotId::new(1)]),
            output_schema: schema,
        }
    }

    fn output_column() -> proto_common::OutputColumn {
        proto_common::OutputColumn {
            column_id: 1,
            name: "id".to_string(),
            r#type: Some(encode_type(&DataType::Int64).expect("type")),
            nullable: true,
            is_internal: false,
        }
    }

    #[test]
    fn redistribute_projection_preserves_child_program_and_refuses_invalid_modes() {
        let physical = plan::PlanNode {
            output_columns: vec![output_column()],
            ..Default::default()
        };
        let mut arena = ExprArena::default();
        let lowered = lower_redistribute_node(
            &physical,
            &plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Gather(true)),
                }),
                output_columns: vec![output_column()],
                ..Default::default()
            },
            FieldPath::root("plan_fragment").field("redistribute"),
            FieldPath::root("plan_fragment").field("output_columns"),
            vec![child()],
            &mut arena,
        )
        .expect("lower redistribute");
        assert!(matches!(lowered.node.kind, ExecNodeKind::Values(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);

        let error = lower_redistribute_node(
            &physical,
            &plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Gather(false)),
                }),
                ..Default::default()
            },
            FieldPath::root("plan_fragment").field("redistribute"),
            FieldPath::root("plan_fragment").field("output_columns"),
            vec![child()],
            &mut arena,
        )
        .expect_err("false gather fails");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.redistribute.mode"
        );
    }

    #[test]
    fn redistribute_hash_mode_requires_columns() {
        let mut arena = ExprArena::default();
        let error = lower_redistribute_node(
            &plan::PlanNode::default(),
            &plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Hash(
                        plan::RedistributeHash {
                            cols: Vec::new(),
                            source: 0,
                        },
                    )),
                }),
                ..Default::default()
            },
            FieldPath::root("plan_fragment").field("redistribute"),
            FieldPath::root("plan_fragment").field("output_columns"),
            vec![child()],
            &mut arena,
        )
        .expect_err("empty hash columns fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.redistribute.mode.hash.cols"
        );
    }
}
