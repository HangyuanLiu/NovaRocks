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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_execution::exec::chunk::{
    Chunk, ChunkFieldSchema, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema, SlotLayout,
};
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode, cast_array_to_target};
use novarocks_execution::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use novarocks_execution::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use novarocks_execution::exec::node::filter::FilterNode;
use novarocks_execution::exec::node::limit::LimitNode;
use novarocks_execution::exec::node::project::ProjectNode;
use novarocks_execution::exec::node::repeat::RepeatNode;
use novarocks_execution::exec::node::set_op::{SetOpKind, SetOpNode};
use novarocks_execution::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use novarocks_execution::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use novarocks_execution::exec::node::union_all::UnionAllNode;
use novarocks_execution::exec::node::values::ValuesNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_plan_codec::native_type::decode_type;
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::{common as proto_common, expr, plan};
use novarocks_spi::connector::ConnectorRowMutationEffect;
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

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
pub fn lower_sort_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    sort: &plan::SortNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated SortNode child");
    let input = NativeExpressionInputLayout::from_slot_ids(child.layout.order().iter().copied());
    let (output_columns, output_columns_path) = if sort.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (&sort.output_columns, path.clone().field("output_columns"))
    };
    let order_by = lower_sort_items(
        "SortNode",
        &sort.items,
        path.clone().field("items"),
        arena,
        &input,
    )?;
    let limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_distributed_limit(node.limit, "SortNode DistributedNode.limit"),
    )?;
    let offset = NativeFragmentDecodeError::map_invalid(
        path.clone().field("offset"),
        parse_optional_nonnegative_i64(sort.offset, "SortNode.offset"),
    )?
    .unwrap_or(0);
    let topn_type = NativeFragmentDecodeError::map_invalid(
        path.clone().field("topn_type"),
        parse_sort_topn_type(sort.topn_type),
    )?;
    let partition_exprs = sort
        .analytic_partition_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let expr = decode_expr_at(
                expr,
                path.clone().field("analytic_partition_by").index(idx),
                arena,
                &input,
            )
            .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))?;
            Ok(SortExpression {
                expr,
                asc: true,
                nulls_first: true,
            })
        })
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    let partition_limit = sort.partition_limit.map(|value| value as usize);
    let use_top_n = partition_limit.is_some();
    if use_top_n && topn_type != SortTopNType::RowNumber && offset != 0 {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("offset"),
            format!(
                "SortNode node_id={} topn_type {:?} requires offset=0, got {}",
                node.node_id, topn_type, offset
            ),
        ));
    }
    let sort_node = ExecNode {
        kind: ExecNodeKind::Sort(SortNode {
            input: Box::new(child.node),
            node_id: node.node_id,
            use_top_n,
            order_by,
            limit,
            offset,
            topn_type,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            partition_exprs,
            partition_limit,
        }),
    };
    let sorted = NativeLoweredPlanNode {
        node: sort_node,
        layout: child.layout.clone(),
        output_schema: child.output_schema.clone(),
    };
    if output_columns.is_empty() {
        return Ok(sorted);
    }

    let output_layout = decode_output_layout(output_columns, output_columns_path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let layout = SlotLayout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    if layout.order() == child.layout.order() {
        return Ok(NativeLoweredPlanNode {
            node: sorted.node,
            layout,
            output_schema,
        });
    }

    build_slot_projection(
        "SortNode",
        sorted,
        output_columns,
        output_columns_path,
        node.node_id,
        arena,
    )
}

fn parse_sort_topn_type(value: Option<i32>) -> Result<SortTopNType, NativeFragmentLeafDecodeError> {
    let Some(value) = value else {
        return Ok(SortTopNType::RowNumber);
    };
    match plan::SortTopNType::try_from(value).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "topn_type",
            format!("SortNode unknown topn_type {value}"),
        )
    })? {
        plan::SortTopNType::SortTopnTypeUnspecified | plan::SortTopNType::SortTopnTypeRowNumber => {
            Ok(SortTopNType::RowNumber)
        }
        plan::SortTopNType::SortTopnTypeRank => Ok(SortTopNType::Rank),
        plan::SortTopNType::SortTopnTypeDenseRank => Ok(SortTopNType::DenseRank),
    }
}

fn build_slot_projection(
    label: &str,
    input: NativeLoweredPlanNode,
    output_columns: &[proto_common::OutputColumn],
    path: FieldPath,
    node_id: i32,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let output_layout = decode_output_layout(output_columns, path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let layout = SlotLayout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let expr_slot_schemas = output_layout.slot_schemas().to_vec();
    let mut exprs = Vec::with_capacity(layout.order().len());
    for slot in layout.order().iter().copied() {
        if !input.layout.contains_slot(slot) {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "{label} output column id {} has no input slot",
                    slot.as_u32()
                ),
            ));
        }
        exprs.push(arena.push(ExprNode::SlotId(slot)));
    }
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(input.node),
                node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: layout.order().to_vec(),
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: None,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

pub fn lower_table_function_node(
    node: &plan::DistributedNode,
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated TableFunctionNode child");
    let input = NativeExpressionInputLayout::from_slot_ids(child.layout.order().iter().copied());
    validate_table_function_signature(table_function, path.clone())?;

    let param_slots = table_function_param_slots(
        &child.layout,
        &table_function.output_columns,
        &table_function.args,
        path.clone(),
    )?;
    let (param_types, param_slot_schemas) =
        table_function_param_schemas(table_function, &param_slots, path.clone())?;
    let result_slot_schemas = decode_output_layout(
        &table_function.output_columns,
        path.clone().field("output_columns"),
    )
    .map_err(NativeFragmentDecodeError::from)?
    .slot_schemas()
    .to_vec();
    let ret_types = table_function_result_types(table_function, path.clone())?;

    let mut project_exprs = Vec::with_capacity(child.layout.order().len() + param_slots.len());
    let mut project_slot_ids = Vec::with_capacity(project_exprs.capacity());
    let mut project_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + param_slot_schemas.len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        project_exprs
            .push(arena.push_typed(ExprNode::SlotId(slot_id), slot_schema.data_type().clone()));
        project_slot_ids.push(slot_id);
        project_slot_schemas.push(slot_schema.clone());
    }
    for ((idx, arg), slot_schema) in table_function
        .args
        .iter()
        .enumerate()
        .zip(param_slot_schemas.iter())
    {
        let expr = decode_expr_at(arg, path.clone().field("args").index(idx), arena, &input)
            .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))?;
        project_exprs.push(expr);
        project_slot_ids.push(slot_schema.slot_id());
        project_slot_schemas.push(slot_schema.clone());
    }
    let project_output_schema = Arc::new(NativeFragmentDecodeError::map_invalid(
        path.clone().field("args"),
        ChunkSchema::try_new(project_slot_schemas),
    )?);

    let mut output_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut output_slot_sources =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut outer_slots = Vec::with_capacity(child.output_schema.slots().len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        outer_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Outer { slot: slot_id });
    }
    let mut fn_result_slots = Vec::with_capacity(result_slot_schemas.len());
    for (idx, slot_schema) in result_slot_schemas.iter().enumerate() {
        let slot_id = slot_schema.slot_id();
        fn_result_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Result { index: idx });
    }
    let output_schema = Arc::new(NativeFragmentDecodeError::map_invalid(
        path.clone().field("output_columns"),
        ChunkSchema::try_new(output_slot_schemas),
    )?);
    let layout = SlotLayout::for_slots(output_schema.slot_ids().iter().copied());

    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Project(ProjectNode {
                        input: Box::new(child.node),
                        node_id: node.node_id,
                        is_subordinate: true,
                        exprs: project_exprs,
                        expr_slot_ids: project_slot_ids,
                        expr_slot_schemas: Some(project_output_schema.slots().to_vec()),
                        output_indices: None,
                        output_chunk_schema: project_output_schema,
                    }),
                }),
                node_id: node.node_id,
                function_name: table_function.function_name.clone(),
                param_slots,
                outer_slots,
                fn_result_slots,
                fn_result_required: true,
                is_left_join: table_function.is_left_join,
                param_types,
                ret_types,
                output_chunk_schema: output_schema.clone(),
                output_slot_sources,
            }),
        },
        layout,
        output_schema,
    })
}

fn validate_table_function_signature(
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let function_name = table_function.function_name.to_ascii_lowercase();
    let param_types = table_function_arg_types(table_function, path.clone())?;
    let ret_types = table_function_result_types(table_function, path.clone())?;
    match function_name.as_str() {
        "unnest" => NativeFragmentDecodeError::map_invalid(
            path,
            validate_unnest_table_function(&param_types, &ret_types),
        ),
        "unnest_bitmap" => {
            NativeFragmentDecodeError::map_invalid(
                path.clone(),
                validate_table_function_arity("unnest_bitmap", &param_types, &ret_types, 1, 1),
            )?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("args").index(0).field("type"),
                    format!(
                        "table function unnest_bitmap param 0 expects Binary, got {:?}",
                        param_types.first()
                    ),
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Int64)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("output_columns").index(0).field("type"),
                    format!(
                        "table function unnest_bitmap return type expects Int64, got {:?}",
                        ret_types.first()
                    ),
                ));
            }
            Ok(())
        }
        "subdivide_bitmap" => {
            NativeFragmentDecodeError::map_invalid(
                path.clone(),
                validate_table_function_arity("subdivide_bitmap", &param_types, &ret_types, 2, 1),
            )?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("args").index(0).field("type"),
                    format!(
                        "table function subdivide_bitmap param 0 expects Binary, got {:?}",
                        param_types.first()
                    ),
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Binary)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("output_columns").index(0).field("type"),
                    format!(
                        "table function subdivide_bitmap return type expects Binary, got {:?}",
                        ret_types.first()
                    ),
                ));
            }
            Ok(())
        }
        "generate_series" => {
            if !(param_types.len() == 2 || param_types.len() == 3) || ret_types.len() != 1 {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone(),
                    format!(
                        "table function generate_series expects 2 or 3 args and 1 output, got args={} outputs={}",
                        param_types.len(),
                        ret_types.len()
                    ),
                ));
            }
            if !ret_types.iter().all(is_table_function_integer_type) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("output_columns").index(0).field("type"),
                    format!(
                        "table function generate_series return type expects integer, got {:?}",
                        ret_types.first()
                    ),
                ));
            }
            for (idx, param_type) in param_types.iter().enumerate() {
                if !is_table_function_integer_type(param_type) {
                    return Err(NativeFragmentDecodeError::invalid_value(
                        path.clone().field("args").index(idx).field("type"),
                        format!(
                            "table function generate_series param {idx} expects integer, got {param_type:?}"
                        ),
                    ));
                }
            }
            Ok(())
        }
        _ => Err(NativeFragmentDecodeError::unsupported(
            path.field("function_name"),
            format!(
                "unsupported native table function: {}",
                table_function.function_name
            ),
        )),
    }
}

fn validate_unnest_table_function(
    param_types: &[DataType],
    ret_types: &[DataType],
) -> Result<(), String> {
    if param_types.is_empty() {
        return Err("table function unnest requires at least one argument".to_string());
    }
    if param_types.len() != ret_types.len() {
        return Err(format!(
            "table function unnest output column count mismatch: args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    for (idx, (param_type, ret_type)) in param_types.iter().zip(ret_types.iter()).enumerate() {
        let DataType::List(item_field) = param_type else {
            return Err(format!(
                "table function unnest param {idx} expects List, got {param_type:?}"
            ));
        };
        if item_field.data_type() != ret_type {
            return Err(format!(
                "table function unnest result type mismatch for param {idx}: item={:?} output={:?}",
                item_field.data_type(),
                ret_type
            ));
        }
    }
    Ok(())
}

fn validate_table_function_arity(
    name: &str,
    param_types: &[DataType],
    ret_types: &[DataType],
    expected_params: usize,
    expected_results: usize,
) -> Result<(), String> {
    if param_types.len() != expected_params || ret_types.len() != expected_results {
        return Err(format!(
            "table function {name} expects {expected_params} args and {expected_results} outputs, got args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    Ok(())
}

fn is_table_function_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn table_function_arg_types(
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
) -> Result<Vec<DataType>, NativeFragmentDecodeError> {
    table_function
        .args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            let type_desc = arg.r#type.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("args").index(idx).field("type"),
                    format!("TableFunctionNode arg {idx} type missing"),
                )
            })?;
            NativeFragmentDecodeError::map_invalid(
                path.clone().field("args").index(idx).field("type"),
                novarocks_plan_codec::native_type::decode_type(type_desc),
            )
        })
        .collect()
}

fn table_function_result_types(
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
) -> Result<Vec<DataType>, NativeFragmentDecodeError> {
    table_function
        .output_columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let column_path = path.clone().field("output_columns").index(idx);
            let type_desc = column.r#type.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    column_path.clone().field("type"),
                    format!(
                        "TableFunctionNode output column {} '{}' type missing",
                        idx, column.name
                    ),
                )
            })?;
            NativeFragmentDecodeError::map_invalid(
                column_path.field("type"),
                novarocks_plan_codec::native_type::decode_type(type_desc),
            )
        })
        .collect()
}

fn table_function_param_schemas(
    table_function: &plan::TableFunctionNode,
    param_slots: &[SlotId],
    path: FieldPath,
) -> Result<(Vec<DataType>, Vec<ChunkSlotSchema>), NativeFragmentDecodeError> {
    let mut param_types = Vec::with_capacity(table_function.args.len());
    let mut slot_schemas = Vec::with_capacity(table_function.args.len());
    for (idx, (arg, slot_id)) in table_function
        .args
        .iter()
        .zip(param_slots.iter())
        .enumerate()
    {
        let arg_path = path.clone().field("args").index(idx);
        let type_desc = arg.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                arg_path.clone().field("type"),
                format!("TableFunctionNode arg {idx} type missing"),
            )
        })?;
        let data_type = NativeFragmentDecodeError::map_invalid(
            arg_path.clone().field("type"),
            novarocks_plan_codec::native_type::decode_type(type_desc),
        )?;
        let field = novarocks_plan_codec::native_type::decode_field_type(
            &format!("__tf_arg_{idx}"),
            arg.nullable,
            type_desc,
        )
        .map_err(|err| {
            NativeFragmentDecodeError::invalid_value(arg_path.clone().field("type"), err)
        })?;
        slot_schemas.push(NativeFragmentDecodeError::map_invalid(
            arg_path.field("type"),
            ChunkSchema::slot_schema_from_arrow_field(*slot_id, &field),
        )?);
        param_types.push(data_type);
    }
    Ok((param_types, slot_schemas))
}

fn table_function_param_slots(
    input_layout: &SlotLayout,
    output_columns: &[proto_common::OutputColumn],
    args: &[expr::Expr],
    path: FieldPath,
) -> Result<Vec<SlotId>, NativeFragmentDecodeError> {
    let mut used = input_layout
        .order()
        .iter()
        .map(|slot| slot.as_u32())
        .collect::<HashSet<_>>();
    used.extend(output_columns.iter().map(|column| column.column_id));
    let mut slot = u32::MAX;
    let mut slots = Vec::with_capacity(args.len());
    while slots.len() < args.len() {
        if used.insert(slot) {
            slots.push(SlotId::new(slot));
        }
        slot = slot.checked_sub(1).ok_or_else(|| {
            NativeFragmentDecodeError::out_of_range(
                path.clone().field("args"),
                "TableFunctionNode could not allocate internal slots",
            )
        })?;
    }
    Ok(slots)
}

pub fn lower_project_node(
    node: &plan::DistributedNode,
    project: &plan::ProjectNode,
    path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated ProjectNode child");
    let input = NativeExpressionInputLayout::from_slot_ids(child.layout.order().iter().copied());
    let project_outputs = project_output_plan(project, &child.layout, path.clone())?;
    let layout = project_outputs.layout.clone();
    let output_schema = Arc::clone(&project_outputs.output_schema);
    let expr_slot_schemas = project_outputs.computed_slot_schemas.clone();

    let exprs = project_outputs
        .computed_item_indices
        .iter()
        .map(|idx| {
            let item = project.items.get(*idx).ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("items").index(*idx),
                    "native ProjectNode item is missing",
                )
            })?;
            let expr = item.expr.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("items").index(*idx).field("expr"),
                    "native ProjectNode item requires expr",
                )
            })?;
            decode_expr_at(
                expr,
                path.clone().field("items").index(*idx).field("expr"),
                arena,
                &input,
            )
            .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))
        })
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    let expr_slot_ids = project_outputs.computed_slot_ids;

    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: false,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: project_outputs.output_indices,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

struct ProjectOutputPlan {
    computed_item_indices: Vec<usize>,
    computed_slot_ids: Vec<SlotId>,
    computed_slot_schemas: Vec<ChunkSlotSchema>,
    layout: SlotLayout,
    output_schema: ChunkSchemaRef,
    output_indices: Option<Vec<usize>>,
}

fn project_output_plan(
    project: &plan::ProjectNode,
    input_layout: &SlotLayout,
    path: FieldPath,
) -> Result<ProjectOutputPlan, NativeFragmentDecodeError> {
    let decoded = (|| -> Result<ProjectOutputPlan, NativeFragmentLeafDecodeError> {
        let item_outputs = project
            .items
            .iter()
            .enumerate()
            .map(project_item_output)
            .collect::<Result<Vec<_>, _>>()?;
        let input_column_ids = input_layout
            .order()
            .iter()
            .map(|slot| slot.as_u32())
            .collect::<HashSet<_>>();
        let output_column_id_candidates = item_outputs
            .iter()
            .map(|item| item.output_column_id)
            .collect::<HashSet<_>>();
        let mut used_output_column_ids = HashSet::new();
        let mut used_compute_column_ids = input_column_ids.clone();
        let mut next_synthetic_column_id = output_column_id_candidates
            .iter()
            .chain(used_compute_column_ids.iter())
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let mut first_expr_index_by_column_id = HashMap::new();
        let mut computed_item_indices = Vec::new();
        let mut computed_slot_ids = Vec::new();
        let mut computed_slot_schemas = Vec::new();
        let mut output_slot_schemas = Vec::with_capacity(project.items.len());
        let mut output_indices = Vec::with_capacity(project.items.len());
        let mut needs_output_indices = false;

        for item in item_outputs {
            let preferred_compute_column_id = item.preferred_compute_column_id;
            let mut compute_column_id = if item.can_reuse_input_slot
                || !input_column_ids.contains(&preferred_compute_column_id)
            {
                preferred_compute_column_id
            } else {
                allocate_project_synthetic_column_id(
                    &mut next_synthetic_column_id,
                    &mut used_output_column_ids,
                    &mut used_compute_column_ids,
                )
                .map_err(project_synthetic_id_error)?
            };
            if !item.can_reuse_input_slot && used_compute_column_ids.contains(&compute_column_id) {
                compute_column_id = allocate_project_synthetic_column_id(
                    &mut next_synthetic_column_id,
                    &mut used_output_column_ids,
                    &mut used_compute_column_ids,
                )
                .map_err(project_synthetic_id_error)?;
            }

            let (computed_idx, is_duplicate_compute) = if item.can_reuse_input_slot
                && let Some(computed_idx) = first_expr_index_by_column_id.get(&compute_column_id)
            {
                (*computed_idx, true)
            } else {
                let computed_idx = computed_slot_ids.len();
                first_expr_index_by_column_id.insert(compute_column_id, computed_idx);
                used_compute_column_ids.insert(compute_column_id);
                computed_item_indices.push(item.item_index);
                let compute_slot_id = SlotId::new(compute_column_id);
                computed_slot_ids.push(compute_slot_id);
                computed_slot_schemas.push(ChunkSlotSchema::new_with_field(
                    compute_slot_id,
                    item.field.clone(),
                    Some(item.field_schema.clone()),
                    None,
                ));
                (computed_idx, false)
            };

            let output_column_id = if used_output_column_ids.insert(item.output_column_id) {
                item.output_column_id
            } else {
                allocate_project_synthetic_column_id(
                    &mut next_synthetic_column_id,
                    &mut used_output_column_ids,
                    &mut used_compute_column_ids,
                )
                .map_err(project_synthetic_id_error)?
            };
            output_slot_schemas.push(ChunkSlotSchema::new_with_field(
                SlotId::new(output_column_id),
                item.field,
                Some(item.field_schema),
                None,
            ));
            if is_duplicate_compute
                || computed_idx != output_indices.len()
                || compute_column_id != output_column_id
            {
                needs_output_indices = true;
            }
            output_indices.push(computed_idx);
        }

        let layout =
            SlotLayout::for_slots(output_slot_schemas.iter().map(ChunkSlotSchema::slot_id));
        let output_schema = ChunkSchema::try_new(output_slot_schemas)
            .map(Arc::new)
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "items",
                    error,
                )
            })?;
        Ok(ProjectOutputPlan {
            computed_item_indices,
            computed_slot_ids,
            computed_slot_schemas,
            layout,
            output_schema,
            output_indices: needs_output_indices.then_some(output_indices),
        })
    })();
    decoded.map_err(|error| error.into_native(path))
}

fn project_synthetic_id_error(error: String) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "items", error)
}

fn allocate_project_synthetic_column_id(
    next_synthetic_column_id: &mut u32,
    used_output_column_ids: &mut HashSet<u32>,
    used_compute_column_ids: &mut HashSet<u32>,
) -> Result<u32, String> {
    while used_output_column_ids.contains(next_synthetic_column_id)
        || used_compute_column_ids.contains(next_synthetic_column_id)
    {
        *next_synthetic_column_id = next_synthetic_column_id
            .checked_add(1)
            .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    }
    let synthetic = *next_synthetic_column_id;
    used_output_column_ids.insert(synthetic);
    used_compute_column_ids.insert(synthetic);
    *next_synthetic_column_id = next_synthetic_column_id
        .checked_add(1)
        .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    Ok(synthetic)
}

struct ProjectItemOutput {
    item_index: usize,
    preferred_compute_column_id: u32,
    output_column_id: u32,
    can_reuse_input_slot: bool,
    field: Field,
    field_schema: ChunkFieldSchema,
}

fn project_item_output(
    (idx, item): (usize, &plan::ProjectItem),
) -> Result<ProjectItemOutput, NativeFragmentLeafDecodeError> {
    let expr = item.expr.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "items",
            format!("ProjectNode item {idx} expr missing"),
        )
        .append_index(idx)
        .append_field("expr")
    })?;
    let r#type = expr.r#type.clone().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "items",
            format!("ProjectNode item {idx} expr type missing"),
        )
        .append_index(idx)
        .append_field("expr")
        .append_field("type")
    })?;
    let type_error = |error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "items", error)
            .append_index(idx)
            .append_field("expr")
            .append_field("type")
    };
    let field = novarocks_plan_codec::native_type::decode_field_type(
        &item.output_name,
        expr.nullable,
        &r#type,
    )
    .map_err(type_error)?;
    let field_schema = ChunkFieldSchema::from_field(&field).map_err(type_error)?;
    let (preferred_compute_column_id, can_reuse_input_slot) = match expr.kind.as_ref() {
        Some(expr::expr::Kind::ColumnRef(column)) => (column.column_id, true),
        _ => (item.output_column_id, false),
    };
    Ok(ProjectItemOutput {
        item_index: idx,
        preferred_compute_column_id,
        output_column_id: item.output_column_id,
        can_reuse_input_slot,
        field,
        field_schema,
    })
}

pub fn lower_filter_node(
    node: &plan::DistributedNode,
    filter: &plan::FilterNode,
    path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated FilterNode child");
    let predicate = filter.predicate.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("predicate"),
            "native FilterNode requires predicate",
        )
    })?;
    let input = NativeExpressionInputLayout::from_slot_ids(child.layout.order().iter().copied());
    let predicate = decode_expr_at(predicate, path.field("predicate"), arena, &input)
        .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))?;
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Filter(FilterNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                predicate,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

pub fn lower_topn_node(
    node: &plan::DistributedNode,
    topn: &plan::TopNNode,
    path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated TopNNode child");
    let payload_limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_optional_nonnegative_i64(topn.limit, "TopNNode.limit"),
    )?;
    let outer_limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_distributed_limit(node.limit, "TopNNode DistributedNode.limit"),
    )?;
    let limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        merge_limits("TopNNode", payload_limit, outer_limit),
    )?;
    if limit.is_none() {
        return Err(NativeFragmentDecodeError::missing(
            path.clone().field("limit"),
            "TopNNode requires a non-negative limit",
        ));
    }
    let offset = NativeFragmentDecodeError::map_invalid(
        path.clone().field("offset"),
        parse_optional_nonnegative_i64(topn.offset, "TopNNode.offset"),
    )?
    .unwrap_or(0);
    let phase = plan::TopNPhase::try_from(topn.phase).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("phase"),
            format!("TopNNode unknown phase {}", topn.phase),
        )
    })?;
    if phase == plan::TopNPhase::TopnPhaseUnspecified {
        return Err(NativeFragmentDecodeError::invalid_enum(
            path.clone().field("phase"),
            "TopNNode phase is unspecified",
        ));
    }
    if topn.is_split && phase == plan::TopNPhase::TopnPhaseFinal {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("is_split"),
            "TopNNode final split must be represented as ExchangeReceiver TopNSplit",
        ));
    }
    let input = NativeExpressionInputLayout::from_slot_ids(child.layout.order().iter().copied());
    let order_by = lower_sort_items(
        "TopNNode",
        &topn.items,
        path.clone().field("items"),
        arena,
        &input,
    )?;
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                use_top_n: true,
                order_by,
                limit,
                offset,
                topn_type: SortTopNType::RowNumber,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs: Vec::new(),
                partition_limit: None,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_sort_items(
    node_kind: &str,
    items: &[expr::SortItem],
    path: FieldPath,
    arena: &mut ExprArena,
    input: &NativeExpressionInputLayout,
) -> Result<Vec<SortExpression>, NativeFragmentDecodeError> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let item_path = path.clone().index(idx);
            let expr = item.expr.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    item_path.clone().field("expr"),
                    format!("{node_kind} sort item {idx} expr missing"),
                )
            })?;
            let expr = decode_expr_at(expr, item_path.field("expr"), arena, input)
                .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))?;
            Ok(SortExpression {
                expr,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

pub fn lower_sort_items_for_layout(
    node_kind: &str,
    items: &[expr::SortItem],
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &SlotLayout,
) -> Result<Vec<SortExpression>, NativeFragmentDecodeError> {
    let input = NativeExpressionInputLayout::from_slot_ids(input_layout.order().iter().copied());
    lower_sort_items(node_kind, items, path, arena, &input)
}

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
pub fn lower_change_event_expand_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    expand: &plan::ChangeEventExpandNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
    arena: &mut ExprArena,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children
        .pop()
        .expect("validated ChangeEventExpandNode child");
    let (output_columns, output_columns_path) = if expand.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (&expand.output_columns, path.clone().field("output_columns"))
    };
    let output_layout = decode_output_layout(output_columns, output_columns_path)
        .map_err(NativeFragmentDecodeError::from)?;
    let layout = SlotLayout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let output_slot_ids = layout.order().to_vec();
    let output_set = output_slot_ids.iter().copied().collect::<HashSet<_>>();
    let effect_slot_id = SlotId::new(expand.effect_column_id);
    if !output_set.contains(&effect_slot_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("effect_column_id"),
            format!(
                "ChangeEventExpandNode effect_column_id {} is not in outputs",
                expand.effect_column_id
            ),
        ));
    }
    let effect_field = output_schema.slot(effect_slot_id).ok_or_else(|| {
        NativeFragmentDecodeError::inconsistent(
            path.clone().field("effect_column_id"),
            format!(
                "ChangeEventExpandNode effect_column_id {} missing from output schema",
                expand.effect_column_id
            ),
        )
    })?;
    if effect_field.data_type() != &DataType::Int8 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("effect_column_id"),
            format!(
                "ChangeEventExpandNode effect_column_id {} must be Int8, got {:?}",
                expand.effect_column_id,
                effect_field.data_type()
            ),
        ));
    }

    let input = NativeExpressionInputLayout::from_slot_ids(child.layout.order().iter().copied());
    let mut events = Vec::with_capacity(expand.events.len());
    for (event_idx, event) in expand.events.iter().enumerate() {
        let event_path = path.clone().field("events").index(event_idx);
        let effect = change_event_effect(event.effect, event_path.clone().field("effect"))?;
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| {
                decode_expr_at(expr, event_path.clone().field("predicate"), arena, &input)
                    .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))
            })
            .transpose()?;
        let assignments = event
            .assignments
            .iter()
            .enumerate()
            .map(|(assign_idx, assignment)| {
                let slot_id = SlotId::new(assignment.output_column_id);
                if !output_set.contains(&slot_id) {
                    return Err(NativeFragmentDecodeError::inconsistent(event_path.clone().field("assignments").index(assign_idx).field("output_column_id"), format!(
                        "ChangeEventExpandNode event {event_idx} assignment {assign_idx} output column {} is not in outputs",
                        assignment.output_column_id
                    )));
                }
                let expr = assignment
                    .expr
                    .as_ref()
                    .map(|expr| {
                        decode_expr_at(
                            expr,
                            event_path
                                .clone()
                                .field("assignments")
                                .index(assign_idx)
                                .field("expr"),
                            arena,
                            &input,
                        )
                        .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))
                    })
                    .transpose()?;
                Ok(ChangeEventRuntimeOutputExpr {
                    output_slot_id: slot_id,
                    expr,
                })
            })
            .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
        events.push(ChangeEventRuntimeSpec {
            predicate,
            effect,
            assignments,
        });
    }

    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::ChangeEventExpand(ChangeEventExpandNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                events,
                output_slot_ids,
                output_chunk_schema: output_schema.clone(),
                effect_slot_id,
            }),
        },
        layout,
        output_schema,
    })
}

fn change_event_effect(
    value: i32,
    path: FieldPath,
) -> Result<ConnectorRowMutationEffect, NativeFragmentDecodeError> {
    match plan::RowMutationEffect::try_from(value).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone(),
            format!("unknown row mutation effect {value}"),
        )
    })? {
        plan::RowMutationEffect::Delete => Ok(ConnectorRowMutationEffect::Delete),
        plan::RowMutationEffect::Replace => Ok(ConnectorRowMutationEffect::Replace),
        plan::RowMutationEffect::Insert => Ok(ConnectorRowMutationEffect::Insert),
        plan::RowMutationEffect::Unspecified => Err(NativeFragmentDecodeError::invalid_enum(
            path,
            "row mutation effect is unspecified",
        )),
    }
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
