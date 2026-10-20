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
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::expression_contract;
use crate::exec::expr::function::array::common as array_common;
use crate::exec::expr::{ExprId, ExprNode};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{Array, ArrayRef, MapArray, UInt32Array, make_array, new_empty_array};
use arrow::compute::{cast, concat, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use novarocks_types::SlotId;
use std::collections::HashSet;
use std::sync::Arc;

pub fn eval_map_apply<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() < 2 {
        return Err(expression_contract(format_args!(
            "map_apply expects at least 2 arguments, got {}",
            args.len()
        )));
    }

    let (lambda_id, map_args) = match arena.node(args[0]) {
        Some(ExprNode::LambdaFunction { .. }) => (args[0], &args[1..]),
        _ => match args.last().and_then(|id| arena.node(*id)) {
            Some(ExprNode::LambdaFunction { .. }) => {
                (args[args.len() - 1], &args[..args.len() - 1])
            }
            _ => {
                return Err(expression_contract(
                    "map_apply expects a lambda function as the first or last argument",
                ));
            }
        },
    };

    if map_args.is_empty() {
        return Err(expression_contract(
            "map_apply expects at least one map argument",
        ));
    }

    let output_type = arena
        .data_type(expr)
        .cloned()
        .ok_or_else(|| expression_contract("map_apply missing output type"))?;
    let (map_field, output_fields, ordered) = match output_type {
        DataType::Map(field, ordered) => {
            let DataType::Struct(fields) = field.data_type().clone() else {
                return Err(expression_contract(
                    "map_apply output map entries type must be Struct",
                ));
            };
            if fields.len() != 2 {
                return Err(expression_contract(
                    "map_apply output map entries type must have 2 fields",
                ));
            }
            (field, fields, ordered)
        }
        other => {
            return Err(expression_contract(format_args!(
                "map_apply output type must be Map, got {:?}",
                other
            )));
        }
    };

    let (lambda_body, lambda_args, common_sub_exprs) = match arena.node(lambda_id) {
        Some(ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            ..
        }) => (*body, arg_slots.clone(), common_sub_exprs.clone()),
        _ => {
            return Err(expression_contract(
                "map_apply expects lambda function expression",
            ));
        }
    };

    let expected_lambda_args = map_args
        .len()
        .checked_mul(2)
        .ok_or_else(|| expression_contract("map_apply lambda argument count overflow"))?;
    if lambda_args.len() != expected_lambda_args {
        return Err(expression_contract(format_args!(
            "map_apply expects {} lambda arguments, got {}",
            expected_lambda_args,
            lambda_args.len()
        )));
    }

    let num_rows = chunk.len();
    let mut input_maps = Vec::with_capacity(map_args.len());
    for arg in map_args {
        let array = arena.eval(*arg, chunk)?;
        let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            expression_contract(format_args!(
                "map_apply expects MapArray, got {:?}",
                array.data_type()
            ))
        })?;
        if map.len() != 1 && map.len() != num_rows {
            return Err(expression_contract(format_args!(
                "map_apply input length mismatch: expected 1 or {}, got {}",
                num_rows,
                map.len()
            )));
        }
        input_maps.push(array);
    }

    let mut row_valid = vec![true; num_rows];
    let mut row_lengths = vec![0usize; num_rows];
    let mut total_elements = 0usize;
    for row in 0..num_rows {
        let mut len_opt: Option<usize> = None;
        let mut row_is_null = false;
        for map_arr in &input_maps {
            let map = map_arr
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| expression_contract("map_apply internal map downcast failed"))?;
            let map_row = super::common::row_index(row, map.len());
            if map.is_null(map_row) {
                row_is_null = true;
                break;
            }
            let offsets = map.value_offsets();
            let len = (offsets[map_row + 1] - offsets[map_row]) as usize;
            if let Some(prev) = len_opt {
                if prev != len {
                    return Err(array_common::length_mismatch_failure(
                        "map_apply",
                        row,
                        prev,
                        len,
                    ));
                }
            } else {
                len_opt = Some(len);
            }
        }

        if row_is_null {
            row_valid[row] = false;
            row_lengths[row] = 0;
            continue;
        }

        let len = len_opt.unwrap_or(0);
        row_lengths[row] = len;
        total_elements = total_elements
            .checked_add(len)
            .ok_or_else(|| expression_contract("map_apply total element count overflow"))?;
    }

    if total_elements == 0 {
        return build_empty_output_map(output_fields, map_field, ordered, &row_valid, num_rows);
    }

    let mut flat_columns = Vec::<ArrayRef>::with_capacity(input_maps.len() * 2);
    for map_arr in &input_maps {
        let map = map_arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| expression_contract("map_apply internal map downcast failed"))?;
        let flat_keys = flatten_map_component(map, &row_valid, true)?;
        let flat_values = flatten_map_component(map, &row_valid, false)?;
        if flat_keys.len() != total_elements || flat_values.len() != total_elements {
            return Err(expression_contract(
                "map_apply flattened input length mismatch",
            ));
        }
        flat_columns.push(flat_keys);
        flat_columns.push(flat_values);
    }

    let captured_slots =
        collect_captured_slots(arena, lambda_body, &common_sub_exprs, &lambda_args);
    let captured_columns =
        replicate_captured_columns(chunk, &captured_slots, &row_lengths, total_elements)?;

    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut slot_ids = Vec::new();
    let mut seen: HashSet<SlotId> = HashSet::new();

    for (idx, slot_id) in lambda_args.iter().enumerate() {
        if !seen.insert(*slot_id) {
            return Err(expression_contract(format_args!(
                "duplicate lambda argument slot id: {}",
                slot_id
            )));
        }
        let col = flat_columns[idx].clone();
        fields.push(Field::new(
            format!("lambda_arg_{idx}"),
            col.data_type().clone(),
            true,
        ));
        slot_ids.push(*slot_id);
        columns.push(col);
    }

    for (slot_id, col) in captured_columns {
        if !seen.insert(slot_id) {
            return Err(expression_contract(format_args!(
                "lambda captured slot id conflicts: {}",
                slot_id
            )));
        }
        fields.push(Field::new(
            format!("lambda_capture_{slot_id}"),
            col.data_type().clone(),
            true,
        ));
        slot_ids.push(slot_id);
        columns.push(col);
    }

    let mut lambda_chunk = build_chunk_from_columns(&fields, &columns, &slot_ids)?;

    for (idx, (slot_id, expr_id)) in common_sub_exprs.iter().enumerate() {
        if !seen.insert(*slot_id) {
            return Err(expression_contract(format_args!(
                "lambda common sub expr slot id conflicts: {}",
                slot_id
            )));
        }
        let col = arena.eval(*expr_id, &lambda_chunk)?;
        if col.len() != total_elements {
            return Err(expression_contract(format_args!(
                "lambda common sub expr length mismatch: expected {}, got {}",
                total_elements,
                col.len()
            )));
        }
        fields.push(Field::new(
            format!("lambda_common_{idx}"),
            col.data_type().clone(),
            true,
        ));
        slot_ids.push(*slot_id);
        columns.push(col);
        lambda_chunk = build_chunk_from_columns(&fields, &columns, &slot_ids)?;
    }

    let lambda_result = arena.eval(lambda_body, &lambda_chunk)?;
    if lambda_result.len() != total_elements {
        return Err(expression_contract(format_args!(
            "lambda body length mismatch: expected {}, got {}",
            total_elements,
            lambda_result.len()
        )));
    }
    let lambda_map = lambda_result
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| expression_contract("map_apply lambda must return MAP type"))?;

    let mut lambda_keys = lambda_map.keys().clone();
    let mut lambda_values = lambda_map.values().clone();
    if lambda_keys.data_type() != output_fields[0].data_type() {
        lambda_keys = cast(&lambda_keys, output_fields[0].data_type())
            .map_err(crate::exec::expr::cast::cast_arrow_failure)?;
    }
    if lambda_values.data_type() != output_fields[1].data_type() {
        lambda_values = cast(&lambda_values, output_fields[1].data_type())
            .map_err(crate::exec::expr::cast::cast_arrow_failure)?;
    }

    let keys_data = lambda_keys.to_data();
    let values_data = lambda_values.to_data();
    let mut out_keys = MutableArrayData::new(vec![&keys_data], false, 0);
    let mut out_values = MutableArrayData::new(vec![&values_data], false, 0);

    let lambda_offsets = lambda_map.value_offsets();
    let mut out_offsets = Vec::with_capacity(num_rows + 1);
    out_offsets.push(0_i32);
    let mut current = 0i32;
    let mut null_builder = NullBufferBuilder::new(num_rows);
    let mut lambda_row_cursor = 0usize;

    for row in 0..num_rows {
        if !row_valid[row] {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let len = row_lengths[row];
        let mut picked_key_rows = Vec::<usize>::new();

        for offset in 0..len {
            let lambda_row = lambda_row_cursor + offset;
            if lambda_map.is_null(lambda_row) {
                continue;
            }
            let start = lambda_offsets[lambda_row] as usize;
            let end = lambda_offsets[lambda_row + 1] as usize;
            if end <= start {
                continue;
            }
            if end - start != 1 {
                return Err(array_common::lambda_value_failure(
                    "map_apply lambda result must contain exactly one key-value pair",
                ));
            }
            let key_row = start;
            let mut duplicated = false;
            for prev_key_row in &picked_key_rows {
                if super::common::compare_keys_at(
                    &lambda_keys,
                    key_row,
                    &lambda_keys,
                    *prev_key_row,
                )? {
                    duplicated = true;
                    break;
                }
            }
            if duplicated {
                continue;
            }
            let next =
                novarocks_functions::aggregate_scalar::checked_map_output_offset(current, 1)?;
            out_keys.extend(0, key_row, key_row + 1);
            out_values.extend(0, key_row, key_row + 1);
            picked_key_rows.push(key_row);
            current = next;
        }

        lambda_row_cursor += len;
        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    if lambda_row_cursor != total_elements {
        return Err(expression_contract(
            "map_apply internal lambda cursor mismatch",
        ));
    }

    let out_keys = make_array(out_keys.freeze());
    let out_values = make_array(out_values.freeze());
    let entries =
        arrow::array::StructArray::try_new(output_fields, vec![out_keys, out_values], None)
            .map_err(|source| {
                crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                    "map_apply entries",
                    source,
                )
            })?;
    let out = MapArray::try_new(
        map_field,
        OffsetBuffer::new(out_offsets.into()),
        entries,
        null_builder.finish(),
        ordered,
    )
    .map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "map_apply constructor",
            source,
        )
    })?;
    Ok(Arc::new(out))
}

fn build_empty_output_map(
    output_fields: arrow::datatypes::Fields,
    map_field: Arc<Field>,
    ordered: bool,
    row_valid: &[bool],
    num_rows: usize,
) -> ExecutionResult<ArrayRef> {
    let out_keys = new_empty_array(output_fields[0].data_type());
    let out_values = new_empty_array(output_fields[1].data_type());
    let entries =
        arrow::array::StructArray::try_new(output_fields, vec![out_keys, out_values], None)
            .map_err(|source| {
                crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                    "map_apply entries",
                    source,
                )
            })?;
    let mut offsets = vec![0_i32; num_rows + 1];
    let mut null_builder = NullBufferBuilder::new(num_rows);
    for row in 0..num_rows {
        offsets[row + 1] = offsets[row];
        if row_valid[row] {
            null_builder.append_non_null();
        } else {
            null_builder.append_null();
        }
    }
    let out = MapArray::try_new(
        map_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        null_builder.finish(),
        ordered,
    )
    .map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "map_apply constructor",
            source,
        )
    })?;
    Ok(Arc::new(out))
}

fn flatten_map_component(
    map: &MapArray,
    row_valid: &[bool],
    keys: bool,
) -> ExecutionResult<ArrayRef> {
    let values = if keys { map.keys() } else { map.values() };
    let offsets = map.value_offsets();
    let mut slices = Vec::<ArrayRef>::new();
    for (row, valid) in row_valid.iter().enumerate() {
        if !*valid {
            continue;
        }
        let map_row = super::common::row_index(row, map.len());
        let start = offsets[map_row] as i64;
        let end = offsets[map_row + 1] as i64;
        if end < start {
            return Err(expression_contract("map_apply map offsets are invalid"));
        }
        let len = (end - start) as usize;
        if len == 0 {
            continue;
        }
        slices.push(values.slice(start as usize, len));
    }
    if slices.is_empty() {
        return Ok(new_empty_array(values.data_type()));
    }
    let refs: Vec<&dyn Array> = slices.iter().map(|arr| arr.as_ref()).collect();
    concat(&refs).map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "map_apply Arrow operation",
            source,
        )
    })
}

fn collect_captured_slots<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    lambda_body: ExprId,
    common_sub_exprs: &[(SlotId, ExprId)],
    lambda_args: &[SlotId],
) -> Vec<SlotId> {
    let mut referenced = HashSet::new();
    collect_slot_ids(arena, lambda_body, &mut referenced);
    for (_, expr_id) in common_sub_exprs {
        collect_slot_ids(arena, *expr_id, &mut referenced);
    }

    let mut excluded: HashSet<SlotId> = lambda_args.iter().copied().collect();
    for (slot_id, _) in common_sub_exprs {
        excluded.insert(*slot_id);
    }

    let mut captured: Vec<SlotId> = referenced
        .into_iter()
        .filter(|slot_id| !excluded.contains(slot_id))
        .collect();
    captured.sort_by_key(|slot| slot.0);
    captured
}

fn collect_slot_ids<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    out: &mut HashSet<SlotId>,
) {
    let mut stack = vec![expr_id];
    while let Some(id) = stack.pop() {
        let Some(node) = arena.node(id) else { continue };
        match node {
            ExprNode::Constant(_) | ExprNode::Literal(_) => {}
            ExprNode::SlotId(slot_id) => {
                out.insert(*slot_id);
            }
            ExprNode::ArrayExpr { elements } => {
                for child in elements {
                    stack.push(*child);
                }
            }
            ExprNode::StructExpr { fields } => {
                for child in fields {
                    stack.push(*child);
                }
            }
            ExprNode::LambdaFunction { .. } => {
                // Do not descend into nested lambdas when collecting captures.
            }
            ExprNode::DictDecode { child, .. } => {
                stack.push(*child);
            }
            ExprNode::Cast(child, _)
            | ExprNode::CastTime(child, _)
            | ExprNode::CastTimeFromDatetime(child, _)
            | ExprNode::Not(child)
            | ExprNode::IsNull(child)
            | ExprNode::IsNotNull(child)
            | ExprNode::Clone(child) => {
                stack.push(*child);
            }
            ExprNode::Add(a, b, _)
            | ExprNode::Sub(a, b, _)
            | ExprNode::Mul(a, b, _)
            | ExprNode::Div(a, b, _)
            | ExprNode::Mod(a, b, _)
            | ExprNode::Eq(a, b)
            | ExprNode::EqForNull(a, b)
            | ExprNode::Ne(a, b)
            | ExprNode::Lt(a, b)
            | ExprNode::Le(a, b)
            | ExprNode::Gt(a, b)
            | ExprNode::Ge(a, b)
            | ExprNode::And(a, b)
            | ExprNode::Or(a, b) => {
                stack.push(*a);
                stack.push(*b);
            }
            ExprNode::In { child, values, .. } => {
                stack.push(*child);
                for value in values {
                    stack.push(*value);
                }
            }
            ExprNode::Case { children, .. } => {
                for child in children {
                    stack.push(*child);
                }
            }
            ExprNode::FunctionCall { args, .. } => {
                for arg in args {
                    stack.push(*arg);
                }
            }
        }
    }
}

fn replicate_captured_columns(
    chunk: &Chunk,
    captured_slots: &[SlotId],
    row_lengths: &[usize],
    total_elements: usize,
) -> ExecutionResult<Vec<(SlotId, ArrayRef)>> {
    if captured_slots.is_empty() {
        return Ok(Vec::new());
    }

    let mut indices: Vec<u32> = Vec::with_capacity(total_elements);
    for (row, len) in row_lengths.iter().enumerate() {
        for _ in 0..*len {
            indices.push(array_common::index_u32(row)?);
        }
    }
    let idx_array = UInt32Array::from(indices);

    let mut out = Vec::with_capacity(captured_slots.len());
    for slot_id in captured_slots {
        let col = chunk.column_by_slot_id(*slot_id)?;
        let flat = if total_elements == 0 {
            new_empty_array(col.data_type())
        } else {
            array_common::take_values(col.as_ref(), &idx_array, "higher-order take")?
        };
        if flat.len() != total_elements {
            return Err(expression_contract(format_args!(
                "captured column length mismatch: expected {}, got {}",
                total_elements,
                flat.len()
            )));
        }
        out.push((*slot_id, flat));
    }
    Ok(out)
}

fn build_chunk_from_columns(
    fields: &[Field],
    columns: &[ArrayRef],
    slot_ids: &[SlotId],
) -> ExecutionResult<Chunk> {
    let schema = Arc::new(Schema::new(fields.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), columns.to_vec()).map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "map_apply Arrow operation",
            source,
        )
    })?;
    Ok(Chunk::try_new_with_chunk_schema(
        batch,
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)?,
    )?)
}
