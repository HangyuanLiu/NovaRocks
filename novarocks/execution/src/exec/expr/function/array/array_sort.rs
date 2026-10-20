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
use crate::exec::expr::expression_contract;
use crate::exec::expr::{ExprArena, ExprId};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{Array, ArrayRef, ListArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::cmp::Ordering;
use std::sync::Arc;

fn insertion_sort_indices(values: &ArrayRef, indices: &mut [usize]) -> ExecutionResult<()> {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            let prev = indices[j - 1];
            let curr = indices[j];
            let ord = super::common::compare_values_ordered(values, prev, curr)?;
            if ord == Ordering::Greater {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

pub fn eval_array_sort<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(
            "array_sort has an invalid prepared argument count",
        ));
    }
    let arr = arena.eval(args[0], chunk)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        expression_contract(format_args!(
            "array_sort expects ListArray, got {:?}",
            arr.data_type()
        ))
    })?;

    super::common::validate_rows(list.len(), chunk.len(), "array_sort input")?;
    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        Some(other) => {
            return Err(expression_contract(format_args!(
                "array_sort output must be List, got {other:?}"
            )));
        }
        None => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(expression_contract(format_args!(
                    "array_sort output type must be List, got {:?}",
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
        let mut null_indices = Vec::<usize>::new();
        let mut non_null_indices = Vec::<usize>::new();
        for idx in start..end {
            if super::common::is_logically_null(values.as_ref(), idx) {
                null_indices.push(idx);
            } else {
                non_null_indices.push(idx);
            }
        }
        insertion_sort_indices(&values, &mut non_null_indices)?;

        for idx in null_indices.into_iter().chain(non_null_indices.into_iter()) {
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
            "array_sort constructor",
            source,
        )
    })?;
    Ok(Arc::new(out) as ArrayRef)
}
