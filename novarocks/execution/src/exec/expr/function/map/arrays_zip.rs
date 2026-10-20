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
use crate::exec::expr::{ExprId, expression_contract};
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, ListArray, StringBuilder, StructArray, make_array,
    new_empty_array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

fn format_float_for_varchar(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    format!("{value}")
}

fn cast_float_to_utf8(value_arr: &ArrayRef) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let mut out = StringBuilder::new();
    let mut bytes = 0i32;
    match value_arr.data_type() {
        DataType::Float64 => {
            let arr = value_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| expression_contract("arrays_zip failed to downcast Float64Array"))?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    out.append_null();
                } else {
                    let text = format_float_for_varchar(arr.value(idx));
                    bytes = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                        bytes,
                        text.len(),
                    )?;
                    out.append_value(text);
                }
            }
        }
        DataType::Float32 => {
            let arr = value_arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| expression_contract("arrays_zip failed to downcast Float32Array"))?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    out.append_null();
                } else {
                    let text = format_float_for_varchar(arr.value(idx) as f64);
                    bytes = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                        bytes,
                        text.len(),
                    )?;
                    out.append_value(text);
                }
            }
        }
        _ => {
            return Err(expression_contract(format_args!(
                "arrays_zip float->utf8 cast got non-float input: {:?}",
                value_arr.data_type()
            )));
        }
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

pub fn eval_arrays_zip<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.is_empty() {
        return Err(expression_contract(
            "arrays_zip expects at least one argument",
        ));
    }

    let mut list_arrays: Vec<Option<ArrayRef>> = Vec::with_capacity(args.len());
    let mut value_arrays: Vec<ArrayRef> = Vec::with_capacity(args.len());
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        if arr.data_type() == &DataType::Null {
            list_arrays.push(None);
            value_arrays.push(new_empty_array(&DataType::Null));
            continue;
        }
        let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            expression_contract(format_args!(
                "arrays_zip expects ListArray, got {:?}",
                arr.data_type()
            ))
        })?;
        crate::exec::expr::function::array::common::validate_rows(
            arr.len(),
            chunk.len(),
            "arrays_zip input",
        )?;
        value_arrays.push(list.values().clone());
        list_arrays.push(Some(arr));
    }

    let (list_field, struct_fields) = resolve_output_type(arena.data_type(expr), &value_arrays)?;
    if struct_fields.len() != value_arrays.len() {
        return Err(expression_contract(format_args!(
            "arrays_zip output struct field count mismatch: expected {}, got {}",
            value_arrays.len(),
            struct_fields.len()
        )));
    }

    for (idx, value_arr) in value_arrays.iter_mut().enumerate() {
        let expected = struct_fields[idx].data_type();
        if value_arr.data_type() != expected {
            *value_arr = if matches!(expected, DataType::Utf8)
                && matches!(value_arr.data_type(), DataType::Float32 | DataType::Float64)
            {
                cast_float_to_utf8(value_arr)?
            } else {
                cast(value_arr, expected).map_err(crate::exec::expr::cast::cast_arrow_failure)?
            };
        }
    }

    let value_data: Vec<arrow_data::ArrayData> = value_arrays.iter().map(|a| a.to_data()).collect();
    let mut mutables: Vec<MutableArrayData> = value_data
        .iter()
        .map(|data| MutableArrayData::new(vec![data], true, 0))
        .collect();

    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let mut row_indices = Vec::with_capacity(list_arrays.len());
        let mut row_is_null = false;
        let mut max_len = 0usize;

        for list_arr in &list_arrays {
            let Some(list_arr) = list_arr else {
                row_is_null = true;
                break;
            };
            let list = list_arr
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| expression_contract("arrays_zip internal list downcast failed"))?;
            let row_idx = super::common::row_index(row, list.len());
            row_indices.push(row_idx);
            if list.is_null(row_idx) {
                row_is_null = true;
                break;
            }
            let offsets = list.value_offsets();
            let len = (offsets[row_idx + 1] - offsets[row_idx]) as usize;
            if len > max_len {
                max_len = len;
            }
        }

        if row_is_null {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        current = novarocks_functions::aggregate_scalar::checked_list_output_offset(
            current as i32,
            max_len,
        )? as i64;
        for element_idx in 0..max_len {
            for (array_idx, list_arr) in list_arrays.iter().enumerate() {
                let Some(list_arr) = list_arr else {
                    continue;
                };
                let list = list_arr
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        expression_contract("arrays_zip internal list downcast failed")
                    })?;
                let row_idx = row_indices[array_idx];
                let offsets = list.value_offsets();
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                let index = start + element_idx;
                if index < end {
                    mutables[array_idx].extend(0, index, index + 1);
                } else {
                    mutables[array_idx].extend_nulls(1);
                }
            }
        }

        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let struct_columns: Vec<ArrayRef> = mutables
        .into_iter()
        .map(|m| make_array(m.freeze()))
        .collect();
    let out_struct =
        StructArray::try_new(struct_fields, struct_columns, None).map_err(|source| {
            crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                "arrays_zip StructArray",
                source,
            )
        })?;
    let out = ListArray::try_new(
        list_field,
        OffsetBuffer::new(out_offsets.into()),
        Arc::new(out_struct),
        null_builder.finish(),
    )
    .map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "arrays_zip ListArray",
            source,
        )
    })?;
    Ok(Arc::new(out))
}

fn resolve_output_type(
    output_type: Option<&DataType>,
    values: &[ArrayRef],
) -> crate::runtime::fragment::ExecutionResult<(Arc<Field>, Fields)> {
    match output_type {
        Some(DataType::List(item_field)) => {
            let DataType::Struct(fields) = item_field.data_type() else {
                return Err(expression_contract(format_args!(
                    "arrays_zip output element type must be Struct, got {:?}",
                    item_field.data_type()
                )));
            };
            Ok((item_field.clone(), fields.clone()))
        }
        Some(other) => Err(expression_contract(format_args!(
            "arrays_zip output type must be List, got {:?}",
            other
        ))),
        None => {
            let fields: Fields = values
                .iter()
                .enumerate()
                .map(|(idx, value)| {
                    Arc::new(Field::new(
                        format!("col{}", idx + 1),
                        value.data_type().clone(),
                        true,
                    ))
                })
                .collect();
            let struct_type = DataType::Struct(fields.clone());
            Ok((Arc::new(Field::new("item", struct_type, true)), fields))
        }
    }
}
