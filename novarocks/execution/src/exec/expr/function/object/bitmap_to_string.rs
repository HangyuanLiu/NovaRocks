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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, StringBuilder};
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprId, expression_contract};

fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}

pub fn eval_bitmap_to_string<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(
            "bitmap_to_string requires exactly one argument",
        ));
    }
    let input = arena.eval(args[0], chunk)?;

    // NullArray (literal NULL) → all-null output
    if input.data_type() == &DataType::Null {
        let mut builder = StringBuilder::new();
        for _ in 0..chunk.len() {
            builder.append_null();
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let arr = input
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            expression_contract(format_args!(
                "bitmap_to_string expects BITMAP/BINARY input, got {:?}",
                input.data_type()
            ))
        })?;
    let mut builder = StringBuilder::new();
    let mut bytes = 0i32;
    crate::exec::expr::function::array::common::validate_rows(
        arr.len(),
        chunk.len(),
        "bitmap_to_string input",
    )?;
    for row in 0..chunk.len() {
        let idx = row_index(row, arr.len());
        if arr.is_null(idx) {
            builder.append_null();
            continue;
        }
        let out = novarocks_functions::bitmap_to_string_core::render(arr.value(idx))?;
        bytes =
            novarocks_functions::aggregate_scalar::checked_utf8_output_offset(bytes, out.len())?;
        builder.append_value(out);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
