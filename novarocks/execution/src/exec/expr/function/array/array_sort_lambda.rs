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
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, UInt32Array, make_array,
};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use novarocks_types::SlotId;
use std::cmp::Ordering;
use std::sync::Arc;

fn build_chunk_from_columns(
    fields: &[Field],
    columns: &[ArrayRef],
    slot_ids: &[SlotId],
) -> ExecutionResult<Chunk> {
    let schema = Arc::new(Schema::new(fields.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), columns.to_vec()).map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "array_sort_lambda Arrow operation",
            source,
        )
    })?;
    Ok(Chunk::try_new_with_chunk_schema(
        batch,
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)?,
    )?)
}

fn sign_i128(value: i128) -> i8 {
    match value.cmp(&0) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn sign_f64(value: f64) -> ExecutionResult<i8> {
    if !value.is_finite() {
        return Err(array_common::lambda_value_failure(
            "array_sort comparator returned non-finite value",
        ));
    }
    Ok(match value.partial_cmp(&0.0).unwrap_or(Ordering::Equal) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    })
}

fn comparator_sign(array: &ArrayRef, idx: usize) -> ExecutionResult<i8> {
    if array.is_null(idx) {
        return Err(array_common::lambda_value_failure(
            "array_sort comparator returned NULL",
        ));
    }
    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(sign_i128(i128::from(arr.value(idx))))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(sign_i128(i128::from(arr.value(idx))))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(sign_i128(i128::from(arr.value(idx))))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(sign_i128(i128::from(arr.value(idx))))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            sign_f64(f64::from(arr.value(idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            sign_f64(arr.value(idx))
        }
        DataType::Decimal128(_, _) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(sign_i128(arr.value(idx)))
        }
        dt if novarocks_types::largeint::is_largeint_data_type(dt) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    expression_contract("array_sort comparator failed to downcast LARGEINT")
                })?;
            Ok(sign_i128(novarocks_types::largeint::value_at(arr, idx)?))
        }
        other => Err(expression_contract(format_args!(
            "array_sort comparator must return numeric type, got {:?}",
            other
        ))),
    }
}

fn validate_comparator(matrix: &[i8], len: usize, nulls: &[bool]) -> ExecutionResult<()> {
    let pairs = array_common::comparator_pair_count(len)?;
    if matrix.len() != pairs || nulls.len() != len {
        return Err(expression_contract(
            "array_sort comparator matrix shape differs",
        ));
    }
    let at = |i: usize, j: usize| matrix[i * len + j];

    for (i, is_null) in nulls.iter().enumerate().take(len) {
        if at(i, i) != 0 {
            if *is_null && at(i, i) > 0 {
                continue;
            }
            if at(i, i) > 0 {
                return Err(array_common::lambda_value_failure(
                    "Comparator violates incomparability transitivity.",
                ));
            }
            return Err(array_common::lambda_value_failure(
                "Comparator violates irreflexivity.",
            ));
        }
    }

    for i in 0..len {
        for j in 0..len {
            if i == j {
                continue;
            }
            if nulls[i] && nulls[j] {
                continue;
            }
            let ij = at(i, j);
            let ji = at(j, i);
            if (ij < 0 && ji != 1)
                || (ij > 0 && ji != -1)
                || (ij == 0 && ji != 0)
                || (ji == 0 && ij != 0)
            {
                return Err(array_common::lambda_value_failure(
                    "Comparator violates asymmetry.",
                ));
            }
        }
    }

    for i in 0..len {
        for j in 0..len {
            for k in 0..len {
                if nulls[i] || nulls[j] || nulls[k] {
                    continue;
                }
                if at(i, j) < 0 && at(j, k) < 0 && at(i, k) >= 0 {
                    return Err(array_common::lambda_value_failure(
                        "Comparator violates transitivity.",
                    ));
                }
                if at(i, j) == 0 && at(j, k) == 0 && at(i, k) != 0 {
                    return Err(array_common::lambda_value_failure(
                        "Comparator violates incomparability transitivity.",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn insertion_sort_indices(matrix: &[i8], len: usize, indices: &mut [usize]) {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            let prev = indices[j - 1];
            let curr = indices[j];
            let sign = matrix[prev * len + curr];
            if sign > 0 {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
}

fn broadcast_array(array: &ArrayRef, len: usize) -> ExecutionResult<ArrayRef> {
    if array.len() == len {
        return Ok(array.clone());
    }
    if array.len() != 1 {
        return Err(expression_contract(format_args!(
            "array_sort comparator result length mismatch: expected {} or 1, got {}",
            len,
            array.len()
        )));
    }
    let indices = UInt32Array::from(vec![0_u32; len]);
    array_common::take_values(array.as_ref(), &indices, "higher-order take")
}

fn eval_comparator_matrix<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    lambda_body: ExprId,
    lambda_args: &[SlotId],
    common_sub_exprs: &[(SlotId, ExprId)],
    values: &ArrayRef,
    start: usize,
    len: usize,
) -> ExecutionResult<Vec<i8>> {
    let pair_count = array_common::comparator_pair_count(len)?;
    if pair_count == 0 {
        return Ok(Vec::new());
    }

    let mut left_indices = Vec::with_capacity(pair_count);
    let mut right_indices = Vec::with_capacity(pair_count);
    for left in 0..len {
        let left_idx = array_common::index_u32(start + left)?;
        for right in 0..len {
            left_indices.push(left_idx);
            right_indices.push(array_common::index_u32(start + right)?);
        }
    }

    let left_values = array_common::take_values(
        values.as_ref(),
        &UInt32Array::from(left_indices),
        "higher-order take",
    )?;
    let right_values = array_common::take_values(
        values.as_ref(),
        &UInt32Array::from(right_indices),
        "higher-order take",
    )?;

    let mut fields = vec![
        Field::new("left", values.data_type().clone(), true),
        Field::new("right", values.data_type().clone(), true),
    ];
    let mut columns = vec![left_values, right_values];
    let mut slot_ids = vec![lambda_args[0], lambda_args[1]];
    let mut lambda_chunk = build_chunk_from_columns(&fields, &columns, &slot_ids)?;

    for (idx, (slot_id, expr_id)) in common_sub_exprs.iter().enumerate() {
        let col = arena.eval(*expr_id, &lambda_chunk)?;
        let col = broadcast_array(&col, pair_count)?;
        fields.push(Field::new(
            format!("common_{idx}"),
            col.data_type().clone(),
            true,
        ));
        columns.push(col);
        slot_ids.push(*slot_id);
        lambda_chunk = build_chunk_from_columns(&fields, &columns, &slot_ids)?;
    }

    let signs = broadcast_array(&arena.eval(lambda_body, &lambda_chunk)?, pair_count)?;
    if matches!(signs.data_type(), DataType::Boolean) {
        let typed = signs
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| {
                expression_contract("array_sort comparator failed to downcast BooleanArray")
            })?;
        let mut matrix = Vec::with_capacity(pair_count);
        for left in 0..len {
            for right in 0..len {
                let idx = left * len + right;
                let rev_idx = right * len + left;
                if typed.is_null(idx) || typed.is_null(rev_idx) {
                    return Err(array_common::lambda_value_failure(
                        "array_sort comparator returned NULL",
                    ));
                }
                let left_before_right = typed.value(idx);
                let right_before_left = typed.value(rev_idx);
                let sign = if left_before_right {
                    -1
                } else if right_before_left {
                    1
                } else {
                    0
                };
                matrix.push(sign);
            }
        }
        return Ok(matrix);
    }

    let mut matrix = Vec::with_capacity(pair_count);
    for idx in 0..pair_count {
        matrix.push(comparator_sign(&signs, idx)?);
    }
    Ok(matrix)
}

pub fn eval_array_sort_lambda<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 2 {
        return Err(expression_contract(format_args!(
            "array_sort_lambda expects at least 2 arguments, got {}",
            args.len()
        )));
    }

    let (array_id, lambda_id) = match arena.node(args[0]) {
        Some(ExprNode::LambdaFunction { .. }) => {
            if args.len() != 2 {
                return Err(expression_contract(
                    "array_sort_lambda expects ARRAY and lambda arguments",
                ));
            }
            (args[1], args[0])
        }
        _ => match args.last().and_then(|id| arena.node(*id)) {
            Some(ExprNode::LambdaFunction { .. }) => (args[0], args[args.len() - 1]),
            _ => {
                return Err(expression_contract(
                    "array_sort_lambda expects a lambda function",
                ));
            }
        },
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
                "array_sort_lambda expects lambda function expression",
            ));
        }
    };
    if lambda_args.len() != 2 {
        return Err(expression_contract(
            "array_sort_lambda expects comparator with exactly two arguments",
        ));
    }

    let arr = arena.eval(array_id, chunk)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        expression_contract(format_args!(
            "array_sort_lambda expects ListArray, got {:?}",
            arr.data_type()
        ))
    })?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        Some(other) => {
            return Err(expression_contract(format_args!(
                "array_sort_lambda output must be List, got {other:?}"
            )));
        }
        None => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(expression_contract(format_args!(
                    "array_sort_lambda output type must be List, got {:?}",
                    other
                )));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut values = list.values().clone();
    if values.data_type() != &target_item_type {
        values = cast(&values, &target_item_type)
            .map_err(crate::exec::expr::cast::cast_arrow_failure)?;
    }

    array_common::validate_rows(list.len(), chunk.len(), "array_sort_lambda input")?;
    let values_data = values.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data], false, 0);
    let offsets = list.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current = 0i32;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let len = end.saturating_sub(start);
        if len > 1 {
            let matrix = eval_comparator_matrix(
                arena,
                lambda_body,
                &lambda_args,
                &common_sub_exprs,
                &values,
                start,
                len,
            )?;
            let nulls = (0..len)
                .map(|idx| values.is_null(start + idx))
                .collect::<Vec<_>>();
            validate_comparator(&matrix, len, &nulls)?;
            let mut positions: Vec<usize> = (0..len).collect();
            insertion_sort_indices(&matrix, len, &mut positions);
            for pos in positions {
                let idx = start + pos;
                let next =
                    novarocks_functions::aggregate_scalar::checked_list_output_offset(current, 1)?;
                mutable.extend(0, idx, idx + 1);
                current = next;
            }
        } else {
            for idx in start..end {
                let next =
                    novarocks_functions::aggregate_scalar::checked_list_output_offset(current, 1)?;
                mutable.extend(0, idx, idx + 1);
                current = next;
            }
        }

        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let out_values = make_array(mutable.freeze());
    let out = ListArray::try_new(
        output_field,
        OffsetBuffer::new(out_offsets.into()),
        out_values,
        null_builder.finish(),
    )
    .map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "array_sort_lambda constructor",
            source,
        )
    })?;
    Ok(Arc::new(out) as ArrayRef)
}
