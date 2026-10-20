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
    Int16Array, Int32Array, Int64Array, ListArray,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

fn avg_rows<T, F>(
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
        let count = (end - start) as i64;
        let mut has_value = false;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            has_value = true;
            sum += value_at(values, idx);
        }
        if count == 0 || !has_value {
            out.push(None);
        } else {
            out.push(Some(sum / count as f64));
        }
    }
    out
}

fn pow10_i128(exp: u32) -> ExecutionResult<i128> {
    10_i128
        .checked_pow(exp)
        .ok_or_else(|| super::common::numeric_failure("array_avg decimal scale overflow"))
}

fn div_round_half_up_i128(numerator: i128, denominator: i128) -> ExecutionResult<i128> {
    if denominator == 0 {
        return Err(expression_contract("array_avg division by zero"));
    }
    let quotient = numerator
        .checked_div(denominator)
        .ok_or_else(|| super::common::numeric_failure("array_avg decimal division overflow"))?;
    let remainder = numerator
        .checked_rem(denominator)
        .ok_or_else(|| super::common::numeric_failure("array_avg decimal remainder overflow"))?;
    if remainder == 0 {
        return Ok(quotient);
    }

    if remainder.unsigned_abs() < denominator.unsigned_abs().div_ceil(2) {
        return Ok(quotient);
    }
    quotient
        .checked_add(numerator.signum() * denominator.signum())
        .ok_or_else(|| super::common::numeric_failure("array_avg decimal rounding overflow"))
}

fn avg_decimal_rows(
    list: &ListArray,
    values: &Decimal128Array,
    chunk_len: usize,
    input_scale: i8,
    output_scale: i8,
) -> ExecutionResult<Vec<Option<i128>>> {
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
        let count = i128::try_from(end - start).map_err(|source| {
            super::common::numeric_conversion_failure("array_avg count", source)
        })?;
        let mut sum = 0_i128;
        let mut has_value = false;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            has_value = true;
            sum = sum.checked_add(values.value(idx)).ok_or_else(|| {
                super::common::numeric_failure("array_avg decimal accumulation overflow")
            })?;
        }

        if count == 0 || !has_value {
            out.push(None);
            continue;
        }

        let (numerator, denominator) = if output_scale >= input_scale {
            let factor = pow10_i128((i16::from(output_scale) - i16::from(input_scale)) as u32)?;
            let numerator = sum.checked_mul(factor).ok_or_else(|| {
                super::common::numeric_failure("array_avg decimal rescale overflow")
            })?;
            (numerator, count)
        } else {
            let factor = pow10_i128((i16::from(input_scale) - i16::from(output_scale)) as u32)?;
            let denominator = count.checked_mul(factor).ok_or_else(|| {
                super::common::numeric_failure("array_avg decimal rescale overflow")
            })?;
            (sum, denominator)
        };
        out.push(Some(div_round_half_up_i128(numerator, denominator)?));
    }
    Ok(out)
}

pub fn eval_array_avg<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(
            "array_avg has an invalid prepared argument count",
        ));
    }
    let arr = arena.eval(args[0], chunk)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        expression_contract(format_args!(
            "array_avg expects ListArray, got {:?}",
            arr.data_type()
        ))
    })?;
    super::common::validate_rows(list.len(), chunk.len(), "array_avg input")?;
    let values = list.values();

    let out = match values.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| if a.value(idx) { 1.0 } else { 0.0 },
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int8 => {
            let values = values.as_any().downcast_ref::<Int8Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int16 => {
            let values = values.as_any().downcast_ref::<Int16Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int32 => {
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int64 => {
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Float32 => {
            let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Float64 => {
            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx),
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Decimal128(precision, scale) => {
            let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let (out_precision, out_scale) = match arena.data_type(expr) {
                Some(DataType::Decimal128(p, s)) => (*p, *s),
                _ => (*precision, *scale),
            };
            let avgs = avg_decimal_rows(list, values, chunk.len(), *scale, out_scale)?;
            let out = Decimal128Array::from(avgs)
                .with_precision_and_scale(out_precision, out_scale)
                .map_err(|source| {
                    crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                        "array_avg decimal metadata",
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
                "array_avg LARGEINT values",
            )?;
            let mut converted = Vec::with_capacity(values.len());
            for idx in 0..values.len() {
                if values.is_null(idx) {
                    converted.push(None);
                } else {
                    let value = novarocks_types::largeint::value_at(values, idx)?;
                    converted.push(Some(value as f64));
                }
            }
            let converted = Float64Array::from(converted);
            Arc::new(Float64Array::from(avg_rows(
                list,
                &converted,
                chunk.len(),
                |a, idx| a.value(idx),
            ))) as ArrayRef
        }
        other => {
            return Err(expression_contract(format_args!(
                "array_avg unsupported element type: {:?}",
                other
            )));
        }
    };

    Ok((super::common::cast_output(out, arena.data_type(expr), "array_avg"))?)
}

#[cfg(test)]
mod numeric_properties {
    use super::*;
    use arrow::array::{Decimal128Builder, ListBuilder};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn half_up_keeps_negative_ties_and_all_supported_integer_extremes() {
        for (n, d, expected) in [
            (5, 2, 3),
            (-5, 2, -3),
            (4, 3, 1),
            (-4, 3, -1),
            (i128::MIN, 1, i128::MIN),
            (i128::MAX, i128::MAX, 1),
            (i128::MIN, i128::MAX, -1),
        ] {
            assert_eq!(div_round_half_up_i128(n, d).unwrap(), expected);
        }
        assert_eq!(
            div_round_half_up_i128(i128::MIN, -1)
                .unwrap_err()
                .failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        assert_eq!(
            div_round_half_up_i128(1, 0)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
    #[test]
    fn decimal_average_preserves_null_element_denominator_and_null_rows() {
        let mut b =
            ListBuilder::new(Decimal128Builder::new().with_data_type(DataType::Decimal128(10, 2)));
        b.values().append_value(500);
        b.values().append_null();
        b.append(true);
        b.append(false);
        b.append(true);
        let list = b.finish();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(
            avg_decimal_rows(&list, values, 3, 2, 2).unwrap(),
            vec![Some(250), None, None]
        );
    }
}
