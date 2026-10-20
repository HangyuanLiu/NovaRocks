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
use arrow::array::{Array, ArrayRef, ListArray, UInt32Array, new_empty_array};
use arrow::compute::{concat, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use novarocks_types::SlotId;
use std::collections::HashSet;
use std::sync::Arc;

pub fn eval_array_map<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    id: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() < 2 {
        return Err(expression_contract(format_args!(
            "array_map expects at least 2 arguments, got {}",
            args.len()
        )));
    }

    let (lambda_id, array_args) = match arena.node(args[0]) {
        Some(ExprNode::LambdaFunction { .. }) => (args[0], &args[1..]),
        _ => match args.last().and_then(|id| arena.node(*id)) {
            Some(ExprNode::LambdaFunction { .. }) => {
                (args[args.len() - 1], &args[..args.len() - 1])
            }
            _ => {
                return Err(expression_contract(
                    "array_map expects a lambda function as the first or last argument",
                ));
            }
        },
    };

    let output_type = arena
        .data_type(id)
        .cloned()
        .ok_or_else(|| expression_contract("array_map missing output type"))?;
    let (output_field, item_type) = match output_type {
        DataType::List(field) => {
            let item_type = field.data_type().clone();
            (field, item_type)
        }
        other => {
            return Err(expression_contract(format_args!(
                "array_map output type must be List, got {:?}",
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
                "array_map expects lambda function expression",
            ));
        }
    };

    if lambda_args.len() != array_args.len() {
        return Err(expression_contract(
            "Lambda arguments should equal to lambda input arrays.",
        ));
    }

    let num_rows = chunk.len();
    let mut input_arrays = Vec::with_capacity(array_args.len());
    for expr_id in array_args {
        let array = arena.eval(*expr_id, chunk)?;
        if array.len() != num_rows {
            return Err(expression_contract(format_args!(
                "array_map input length mismatch: expected {}, got {}",
                num_rows,
                array.len()
            )));
        }
        input_arrays.push(array);
    }

    let mut list_refs = Vec::with_capacity(input_arrays.len());
    for array in &input_arrays {
        if array.data_type() == &DataType::Null {
            list_refs.push(None);
            continue;
        }
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            expression_contract(format_args!(
                "array_map input must be ListArray, got {:?}",
                array.data_type()
            ))
        })?;
        list_refs.push(Some(list));
    }

    let mut row_valid = vec![true; num_rows];
    let mut row_lengths = vec![0usize; num_rows];
    let mut total_elements: usize = 0;

    for row in 0..num_rows {
        let mut len_opt: Option<usize> = None;
        let mut row_null = false;
        for list in &list_refs {
            let Some(list) = list else {
                row_null = true;
                break;
            };
            if list.is_null(row) {
                row_null = true;
                break;
            }
            let offsets = list.value_offsets();
            let start = offsets[row] as i64;
            let end = offsets[row + 1] as i64;
            if end < start {
                return Err(expression_contract("array_map list offsets are invalid"));
            }
            let len = (end - start) as usize;
            if let Some(prev) = len_opt {
                if prev != len {
                    return Err(array_common::length_mismatch_failure(
                        "array_map",
                        row,
                        prev,
                        len,
                    ));
                }
            } else {
                len_opt = Some(len);
            }
        }

        if row_null {
            row_valid[row] = false;
            row_lengths[row] = 0;
            continue;
        }

        let len = len_opt.unwrap_or(0);
        row_lengths[row] = len;
        total_elements = novarocks_functions::aggregate_scalar::checked_list_output_offset(
            total_elements as i32,
            len,
        )? as usize;
    }

    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    for row in 0..num_rows {
        if row_valid[row] {
            current += row_lengths[row] as i64;
            if current > i32::MAX as i64 {
                return Err(expression_contract("array_map offset overflow"));
            }
        }
        offsets.push(current as i32);
    }

    let mut null_builder = NullBufferBuilder::new(num_rows);
    for valid in &row_valid {
        if *valid {
            null_builder.append_non_null();
        } else {
            null_builder.append_null();
        }
    }
    let nulls = null_builder.finish();

    if total_elements == 0 {
        let values = new_empty_array(&item_type);
        let list = ListArray::try_new(
            output_field,
            OffsetBuffer::new(offsets.into()),
            values,
            nulls,
        )
        .map_err(|source| {
            crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                "array_map constructor",
                source,
            )
        })?;
        return Ok(Arc::new(list));
    }

    let mut source_fields = chunk
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    let mut source_columns = chunk.columns().to_vec();
    let mut source_slot_ids = chunk.chunk_schema().slot_ids().to_vec();
    let mut source_chunk = chunk.clone();
    let mut source_row_common_sub_exprs = vec![None; common_sub_exprs.len()];
    for (idx, (slot_id, expr_id)) in common_sub_exprs.iter().enumerate() {
        let Some(source_col) = try_eval_expr_on_source_rows(arena, *expr_id, &source_chunk)? else {
            continue;
        };
        source_fields.push(Field::new(
            format!("lambda_common_source_{idx}"),
            source_col.data_type().clone(),
            true,
        ));
        source_slot_ids.push(*slot_id);
        source_columns.push(source_col.clone());
        source_chunk = build_chunk_from_columns(&source_fields, &source_columns, &source_slot_ids)?;
        source_row_common_sub_exprs[idx] = Some(source_col);
    }

    if let Some(row_level_result) = try_eval_expr_on_source_rows(arena, lambda_body, &source_chunk)?
    {
        let flat_result =
            replicate_array_by_row_lengths(&row_level_result, &row_lengths, total_elements)?;
        let list = ListArray::try_new(
            output_field,
            OffsetBuffer::new(offsets.into()),
            flat_result,
            nulls,
        )
        .map_err(|source| {
            crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                "array_map constructor",
                source,
            )
        })?;
        return Ok(Arc::new(list));
    }

    let mut flat_args = Vec::with_capacity(list_refs.len());
    for list in &list_refs {
        let Some(list) = list else {
            continue;
        };
        let flat = flatten_list_values(list, &row_valid)?;
        if flat.len() != total_elements {
            return Err(expression_contract(
                "array_map flattened argument length mismatch",
            ));
        }
        flat_args.push(flat);
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
        let col = flat_args[idx].clone();
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
        let col = if let Some(source_col) = &source_row_common_sub_exprs[idx] {
            replicate_array_by_row_lengths(source_col, &row_lengths, total_elements)?
        } else {
            match eval_common_sub_expr_on_source_rows(
                arena,
                *expr_id,
                &source_chunk,
                &lambda_chunk,
                &row_lengths,
                total_elements,
            )? {
                CommonSubExprEval::SourceRows(source_col, flat_col) => {
                    source_fields.push(Field::new(
                        format!("lambda_common_source_{idx}"),
                        source_col.data_type().clone(),
                        true,
                    ));
                    source_slot_ids.push(*slot_id);
                    source_columns.push(source_col);
                    source_chunk = build_chunk_from_columns(
                        &source_fields,
                        &source_columns,
                        &source_slot_ids,
                    )?;
                    flat_col
                }
                CommonSubExprEval::FlatRows(flat_col) => flat_col,
            }
        };
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

    let result = arena.eval(lambda_body, &lambda_chunk)?;
    if result.len() != total_elements {
        return Err(expression_contract(format_args!(
            "lambda body length mismatch: expected {}, got {}",
            total_elements,
            result.len()
        )));
    }
    if result.data_type() != &item_type {
        return Err(expression_contract(format_args!(
            "array_map result type mismatch: expected {:?}, got {:?}",
            item_type,
            result.data_type()
        )));
    }

    let list = ListArray::try_new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        result,
        nulls,
    )
    .map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "array_map constructor",
            source,
        )
    })?;
    Ok(Arc::new(list))
}

fn flatten_list_values(list: &ListArray, row_valid: &[bool]) -> ExecutionResult<ArrayRef> {
    let values = list.values();
    let offsets = list.value_offsets();
    let mut slices: Vec<ArrayRef> = Vec::new();
    for (row, valid) in row_valid.iter().enumerate() {
        if !*valid {
            continue;
        }
        let start = offsets[row] as i64;
        let end = offsets[row + 1] as i64;
        if end < start {
            return Err(expression_contract("array_map list offsets are invalid"));
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
            "array_map Arrow operation",
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
    let bound = HashSet::new();
    collect_slot_ids(arena, lambda_body, &bound, &mut referenced);
    for (_, expr_id) in common_sub_exprs {
        collect_slot_ids(arena, *expr_id, &bound, &mut referenced);
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
    bound: &HashSet<SlotId>,
    out: &mut HashSet<SlotId>,
) {
    let mut stack = vec![expr_id];
    while let Some(id) = stack.pop() {
        let Some(node) = arena.node(id) else { continue };
        match node {
            ExprNode::Constant(_) | ExprNode::Literal(_) => {}
            ExprNode::SlotId(slot_id) => {
                if !bound.contains(slot_id) {
                    out.insert(*slot_id);
                }
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
            ExprNode::LambdaFunction {
                body,
                arg_slots,
                common_sub_exprs,
                ..
            } => {
                let mut nested_bound = bound.clone();
                for slot_id in arg_slots {
                    nested_bound.insert(*slot_id);
                }
                for (slot_id, _) in common_sub_exprs {
                    nested_bound.insert(*slot_id);
                }
                collect_slot_ids(arena, *body, &nested_bound, out);
                for (_, expr_id) in common_sub_exprs {
                    collect_slot_ids(arena, *expr_id, &nested_bound, out);
                }
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

enum CommonSubExprEval {
    SourceRows(ArrayRef, ArrayRef),
    FlatRows(ArrayRef),
}

fn try_eval_expr_on_source_rows<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    source_chunk: &Chunk,
) -> ExecutionResult<Option<ArrayRef>> {
    if !can_eval_on_source_rows(arena, expr_id, source_chunk) {
        return Ok(None);
    }
    let source_result = arena.eval(expr_id, source_chunk)?;

    let source_result = match source_result.len() {
        len if len == source_chunk.len() => source_result,
        1 => broadcast_array(&source_result, source_chunk.len())?,
        other => {
            return Err(expression_contract(format_args!(
                "array_map source result length {other} differs from its batch"
            )));
        }
    };

    Ok(Some(source_result))
}

fn eval_common_sub_expr_on_source_rows<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    source_chunk: &Chunk,
    lambda_chunk: &Chunk,
    row_lengths: &[usize],
    total_elements: usize,
) -> ExecutionResult<CommonSubExprEval> {
    if can_eval_on_source_rows(arena, expr_id, source_chunk) {
        let source_col = arena.eval(expr_id, source_chunk)?;
        let source_col = match source_col.len() {
            len if len == source_chunk.len() => source_col,
            1 => broadcast_array(&source_col, source_chunk.len())?,
            other => {
                return Err(expression_contract(format_args!(
                    "lambda common sub expr source length mismatch: expected {} or 1, got {}",
                    source_chunk.len(),
                    other
                )));
            }
        };
        let flat_col = replicate_array_by_row_lengths(&source_col, row_lengths, total_elements)?;
        return Ok(CommonSubExprEval::SourceRows(source_col, flat_col));
    }

    let flat_col = arena.eval(expr_id, lambda_chunk)?;
    Ok(CommonSubExprEval::FlatRows(flat_col))
}

fn broadcast_array(array: &ArrayRef, len: usize) -> ExecutionResult<ArrayRef> {
    if array.len() == len {
        return Ok(array.clone());
    }
    if array.is_empty() {
        return Ok(new_empty_array(array.data_type()));
    }
    if array.len() != 1 {
        return Err(expression_contract(format_args!(
            "cannot broadcast array of length {} to {}",
            array.len(),
            len
        )));
    }
    if len == 0 {
        return Ok(new_empty_array(array.data_type()));
    }
    let indices = UInt32Array::from(vec![0_u32; len]);
    array_common::take_values(array.as_ref(), &indices, "higher-order take")
}

fn replicate_array_by_row_lengths(
    array: &ArrayRef,
    row_lengths: &[usize],
    total_elements: usize,
) -> ExecutionResult<ArrayRef> {
    if total_elements == 0 {
        return Ok(new_empty_array(array.data_type()));
    }
    if array.len() != 1 && array.len() != row_lengths.len() {
        return Err(expression_contract(format_args!(
            "cannot replicate array with length {} for {} source rows",
            array.len(),
            row_lengths.len()
        )));
    }

    let mut indices: Vec<u32> = Vec::with_capacity(total_elements);
    for (row, len) in row_lengths.iter().enumerate() {
        let src_row = if array.len() == 1 { 0 } else { row };
        let src_row = array_common::index_u32(src_row)?;
        for _ in 0..*len {
            indices.push(src_row);
        }
    }
    let idx_array = UInt32Array::from(indices);
    array_common::take_values(array.as_ref(), &idx_array, "higher-order take")
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
            "array_map Arrow operation",
            source,
        )
    })?;
    Ok(Chunk::try_new_with_chunk_schema(
        batch,
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)?,
    )?)
}

fn can_eval_on_source_rows<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    source_chunk: &Chunk,
) -> bool {
    let mut referenced = HashSet::new();
    collect_slot_ids(arena, expr_id, &HashSet::new(), &mut referenced);
    referenced
        .iter()
        .all(|slot| source_chunk.chunk_schema().slot_ids().contains(slot))
}

#[cfg(test)]
mod source_row_properties {
    use super::*;
    use crate::exec::expr::{ExprArena, LiteralValue, function::FunctionKind};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    fn source() -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        Chunk::try_new_with_chunk_schema(
            batch,
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn missing_lambda_slot_is_selected_by_binding_but_real_source_failure_propagates() {
        let chunk = source();
        let mut arena = ExprArena::default();
        let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        assert!(
            try_eval_expr_on_source_rows(&arena, missing, &chunk)
                .unwrap()
                .is_none()
        );
        let message = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("actual source error".to_string())),
            DataType::Utf8,
        );
        let raised = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("raise_error"),
                args: vec![message],
            },
            DataType::Boolean,
        );
        let failure = try_eval_expr_on_source_rows(&arena, raised, &chunk).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 518);
        assert!(failure.to_string().contains("actual source error"));
    }
}
