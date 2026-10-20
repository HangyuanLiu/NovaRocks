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
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;
use crate::exec::expr::expression_contract;
use crate::exec::expr::function::array::common as array_common;
use crate::runtime::fragment::ExecutionResult;
use arrow::array::{Array, ArrayRef, ListArray, StringArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

fn eval_reverse_list<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    list: &ListArray,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        Some(other) => {
            return Err(expression_contract(format_args!(
                "reverse ARRAY output must be List, got {other:?}"
            )));
        }
        None => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(expression_contract(format_args!(
                    "reverse ARRAY output type must be List, got {:?}",
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

    array_common::validate_rows(list.len(), chunk.len(), "reverse ARRAY input")?;
    let values_data = values.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data], false, 0);
    let offsets = list.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current = 0i32;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let row_idx = if list.len() == 1 { 0 } else { row };
        if list.is_null(row_idx) {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        for idx in (start..end).rev() {
            let next =
                novarocks_functions::aggregate_scalar::checked_list_output_offset(current, 1)?;
            mutable.extend(0, idx, idx + 1);
            current = next;
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
            "reverse ARRAY constructor",
            source,
        )
    })?;
    Ok(Arc::new(out) as ArrayRef)
}

pub fn eval_reverse<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(
            "reverse requires exactly one prepared argument",
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    if let Some(list) = input.as_any().downcast_ref::<ListArray>() {
        return eval_reverse_list(arena, expr, list, chunk);
    }

    let s_arr = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("reverse expects string or array"))?;
    array_common::validate_rows(s_arr.len(), chunk.len(), "reverse string input")?;
    let mut output_offset = 0;
    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let row_idx = if s_arr.len() == 1 { 0 } else { row };
        if s_arr.is_null(row_idx) {
            out.push(None);
            continue;
        }
        output_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
            output_offset,
            s_arr.value(row_idx).len(),
        )?;
        let val = novarocks_functions::string_reverse_shared::reverse_utf8(s_arr.value(row_idx));
        out.push(Some(val));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
