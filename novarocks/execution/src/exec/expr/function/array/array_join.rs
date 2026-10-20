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
use crate::runtime::fragment::ExecutionResult;
use arrow::array::{Array, ArrayRef, ListArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_array_join<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if !matches!(args.len(), 2 | 3) {
        return Err(expression_contract(
            "array_join requires two or three prepared arguments",
        ));
    }
    let list_arr = arena.eval(args[0], chunk)?;
    let sep_arr = arena.eval(args[1], chunk)?;
    let null_replace_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };

    let list = list_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            expression_contract(format_args!(
                "array_join expects ListArray, got {:?}",
                list_arr.data_type()
            ))
        })?;

    let mut list_values = list.values().clone();
    if list_values.data_type() != &DataType::Utf8 {
        list_values = cast(&list_values, &DataType::Utf8)
            .map_err(crate::exec::expr::cast::cast_arrow_failure)?;
    }
    let list_values = list_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract("array_join failed to downcast list values to StringArray")
        })?;

    let mut sep_cast = sep_arr.clone();
    if sep_cast.data_type() != &DataType::Utf8 {
        sep_cast = cast(&sep_cast, &DataType::Utf8)
            .map_err(crate::exec::expr::cast::cast_arrow_failure)?;
    }
    let sep = sep_cast
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("array_join failed to downcast sep to StringArray"))?;

    let null_replace_cast = if let Some(arr) = null_replace_arr {
        let mut casted = arr;
        if casted.data_type() != &DataType::Utf8 {
            casted = cast(&casted, &DataType::Utf8)
                .map_err(crate::exec::expr::cast::cast_arrow_failure)?;
        }
        Some(casted)
    } else {
        None
    };
    let null_replace = null_replace_cast
        .as_ref()
        .map(|arr| {
            arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                expression_contract("array_join failed to downcast null_replace to StringArray")
            })
        })
        .transpose()?;

    super::common::validate_rows(list.len(), chunk.len(), "array_join input")?;
    super::common::validate_rows(sep.len(), chunk.len(), "array_join separator")?;
    if let Some(replacement) = null_replace {
        super::common::validate_rows(replacement.len(), chunk.len(), "array_join replacement")?;
    }
    let list_offsets = list.value_offsets();
    let mut output_offset = 0;
    let mut out = Vec::<Option<String>>::with_capacity(chunk.len());

    for row in 0..chunk.len() {
        let list_row = super::common::row_index(row, list.len());
        let sep_row = super::common::row_index(row, sep.len());
        let null_replace_row = null_replace.map(|arr| super::common::row_index(row, arr.len()));

        if list.is_null(list_row)
            || sep.is_null(sep_row)
            || (null_replace_row.is_some()
                && null_replace.unwrap().is_null(null_replace_row.unwrap()))
        {
            out.push(None);
            continue;
        }

        let start = list_offsets[list_row] as usize;
        let end = list_offsets[list_row + 1] as usize;
        let sep_val = sep.value(sep_row);
        let null_replace_val = null_replace_row.map(|idx| null_replace.unwrap().value(idx));

        let mut parts = Vec::<&str>::new();
        for idx in start..end {
            if list_values.is_null(idx) {
                if let Some(v) = null_replace_val {
                    parts.push(v);
                }
            } else {
                parts.push(list_values.value(idx));
            }
        }
        // Check the exact output before join allocates it; the original buffers
        // remain borrowed, including repeated NULL replacement and separators.
        for part in &parts {
            output_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                output_offset,
                part.len(),
            )?;
        }
        for _ in 1..parts.len() {
            output_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                output_offset,
                sep_val.len(),
            )?;
        }
        out.push(Some(parts.join(sep_val)));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
