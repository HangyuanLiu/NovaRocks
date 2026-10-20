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
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, StringArray,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

fn sum_i64_rows<T, F>(
    list: &ListArray,
    values: &T,
    chunk_len: usize,
    mut value_at: F,
) -> ExecutionResult<Vec<Option<i64>>>
where
    T: Array,
    F: FnMut(&T, usize) -> i64,
{
    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk_len);
    for row in 0..chunk_len {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut sum = 0_i64;
        let mut has_value = false;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            has_value = true;
            sum = sum.checked_add(value_at(values, idx)).ok_or_else(|| {
                super::common::numeric_failure("array_sum integer accumulation overflow")
            })?;
        }
        out.push(has_value.then_some(sum));
    }
    Ok(out)
}

fn sum_f64_rows<T, F>(
    list: &ListArray,
    values: &T,
    chunk_len: usize,
    mut value_at: F,
) -> Vec<Option<f64>>
where
    T: Array,
    F: FnMut(&T, usize) -> f64,
{
    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk_len);
    for row in 0..chunk_len {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut sum = 0.0_f64;
        let mut has_value = false;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            has_value = true;
            sum += value_at(values, idx);
        }
        out.push(has_value.then_some(sum));
    }
    out
}

fn sum_utf8_rows(list: &ListArray, values: &StringArray, chunk_len: usize) -> Vec<Option<f64>> {
    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk_len);
    for row in 0..chunk_len {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut sum = 0.0_f64;
        let mut has_value = false;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            if let Ok(value) = values.value(idx).trim().parse::<f64>()
                && value.is_finite()
            {
                has_value = true;
                sum += value;
            }
        }
        out.push(has_value.then_some(sum));
    }
    out
}

fn sum_i128_rows<T, F>(
    list: &ListArray,
    values: &T,
    chunk_len: usize,
    mut value_at: F,
) -> ExecutionResult<Vec<Option<i128>>>
where
    T: Array,
    F: FnMut(&T, usize) -> ExecutionResult<i128>,
{
    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk_len);
    for row in 0..chunk_len {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut sum = 0_i128;
        let mut has_value = false;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            has_value = true;
            let v = value_at(values, idx)?;
            sum = sum.checked_add(v).ok_or_else(|| {
                super::common::numeric_failure("array_sum overflow in i128 accumulation")
            })?;
        }
        out.push(has_value.then_some(sum));
    }
    Ok(out)
}

fn rescale_decimal_value(value: i128, from_scale: i8, to_scale: i8) -> ExecutionResult<i128> {
    if from_scale == to_scale {
        return Ok(value);
    }
    if to_scale > from_scale {
        let factor = 10_i128
            .checked_pow((i16::from(to_scale) - i16::from(from_scale)) as u32)
            .ok_or_else(|| super::common::numeric_failure("array_sum decimal rescale overflow"))?;
        return value
            .checked_mul(factor)
            .ok_or_else(|| super::common::numeric_failure("array_sum decimal rescale overflow"));
    }
    let divisor = 10_i128
        .checked_pow((i16::from(from_scale) - i16::from(to_scale)) as u32)
        .ok_or_else(|| super::common::numeric_failure("array_sum decimal rescale overflow"))?;
    Ok(value / divisor)
}

pub fn eval_array_sum<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(
            "array_sum has an invalid prepared argument count",
        ));
    }
    let arr = arena.eval(args[0], chunk)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        expression_contract(format_args!(
            "array_sum expects ListArray, got {:?}",
            arr.data_type()
        ))
    })?;
    super::common::validate_rows(list.len(), chunk.len(), "array_sum input")?;
    let values = list.values();

    let out = match values.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Arc::new(Int64Array::from(sum_i64_rows(
                list,
                values,
                chunk.len(),
                |a, idx| {
                    if a.value(idx) { 1 } else { 0 }
                },
            )?)) as ArrayRef
        }
        arrow::datatypes::DataType::Int8 => {
            let values = values.as_any().downcast_ref::<Int8Array>().unwrap();
            Arc::new(Int64Array::from(sum_i64_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as i64,
            )?)) as ArrayRef
        }
        arrow::datatypes::DataType::Int16 => {
            let values = values.as_any().downcast_ref::<Int16Array>().unwrap();
            Arc::new(Int64Array::from(sum_i64_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as i64,
            )?)) as ArrayRef
        }
        arrow::datatypes::DataType::Int32 => {
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            Arc::new(Int64Array::from(sum_i64_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as i64,
            )?)) as ArrayRef
        }
        arrow::datatypes::DataType::Int64 => {
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            Arc::new(Int64Array::from(sum_i64_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx),
            )?)) as ArrayRef
        }
        arrow::datatypes::DataType::Float32 => {
            let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
            Arc::new(Float64Array::from(sum_f64_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Float64 => {
            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
            Arc::new(Float64Array::from(sum_f64_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx),
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Utf8 => {
            let values = values.as_any().downcast_ref::<StringArray>().unwrap();
            Arc::new(Float64Array::from(sum_utf8_rows(list, values, chunk.len()))) as ArrayRef
        }
        arrow::datatypes::DataType::Decimal128(precision, scale) => {
            let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let sums = sum_i128_rows(list, values, chunk.len(), |a, idx| Ok(a.value(idx)))?;
            let (out_precision, out_scale) = match arena.data_type(expr) {
                Some(DataType::Decimal128(p, s)) => (*p, *s),
                _ => (*precision, *scale),
            };
            let sums = if out_scale == *scale {
                sums
            } else {
                let mut adjusted = Vec::with_capacity(sums.len());
                for sum in sums {
                    let value = match sum {
                        Some(v) => Some(rescale_decimal_value(v, *scale, out_scale)?),
                        None => None,
                    };
                    adjusted.push(value);
                }
                adjusted
            };
            let out = Decimal128Array::from(sums)
                .with_precision_and_scale(out_precision, out_scale)
                .map_err(|source| {
                    crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                        "array_sum decimal metadata",
                        source,
                    )
                })?;
            Arc::new(out) as ArrayRef
        }
        arrow::datatypes::DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let values = novarocks_types::largeint::as_fixed_size_binary_array(
                values,
                "array_sum LARGEINT values",
            )?;
            let sums = sum_i128_rows(list, values, chunk.len(), |a, idx| {
                novarocks_types::largeint::value_at(a, idx).map_err(ExecutionFailure::from)
            })?;
            novarocks_types::largeint::array_from_i128(&sums)?
        }
        other => {
            return Err(expression_contract(format_args!(
                "array_sum unsupported element type: {:?}",
                other
            )));
        }
    };

    let adjusted_output_type = arena
        .data_type(expr)
        .map(|t| super::common::adjust_legacy_decimalv2_target_type(values.data_type(), t));
    Ok((super::common::cast_output(out, adjusted_output_type.as_ref(), "array_sum"))?)
}

#[cfg(test)]
mod numeric_properties {
    use super::*;
    use arrow::array::ListBuilder;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn signed_integer_overflow_is_a_typed_failure_instead_of_profile_dependent_wrap() {
        let mut b = ListBuilder::new(arrow::array::Int64Builder::new());
        b.values().append_value(i64::MAX);
        b.values().append_value(1);
        b.append(true);
        let list = b.finish();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        let error = sum_i64_rows(&list, values, 1, |a, i| a.value(i)).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(
            error.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        assert_eq!(rescale_decimal_value(-125, 2, 1).unwrap(), -12);
        assert!(rescale_decimal_value(i128::MAX, 0, 1).is_err());
        assert!(rescale_decimal_value(1, i8::MIN, i8::MAX).is_err());
    }
}
