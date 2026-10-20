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
use novarocks_types::value::bitmap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Int64Builder, ListBuilder};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprId, expression_contract};

fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}

pub fn eval_bitmap_to_array<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(
            "bitmap_to_array requires exactly one argument",
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    let arr = input
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            expression_contract(format_args!(
                "bitmap_to_array expects BITMAP/BINARY input, got {:?}",
                input.data_type()
            ))
        })?;

    let mut builder = ListBuilder::new(Int64Builder::new());
    let mut count = 0i32;
    crate::exec::expr::function::array::common::validate_rows(
        arr.len(),
        chunk.len(),
        "bitmap_to_array input",
    )?;
    for row in 0..chunk.len() {
        let idx = row_index(row, arr.len());
        if arr.is_null(idx) {
            builder.append(false);
            continue;
        }
        let values = bitmap::decode_bitmap(arr.value(idx))?;
        count =
            novarocks_functions::aggregate_scalar::checked_list_output_offset(count, values.len())?;
        for value in values {
            let value = i64::try_from(value).map_err(|source| {
                super::bitmap_functions::bitmap_numeric_range(
                    format_args!("bitmap_to_array value {value} exceeds BIGINT range"),
                    source,
                )
            })?;
            builder.values().append_value(value);
        }
        builder.append(true);
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}
