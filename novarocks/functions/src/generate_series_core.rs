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
//! ONE original GenerateSeries expansion, integer reader and return conversion.
//! Arguments are already evaluated. No arena, task, AST or function-name dispatch.
use crate::largeint;
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow_schema::DataType;
use std::sync::Arc;

/// Closed original diagnostic authors. This is not a runtime function dispatcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegerDiagnosticContext {
    GenerateSeries,
    SubdivideBitmap,
}
impl IntegerDiagnosticContext {
    fn name(self) -> &'static str {
        match self {
            Self::GenerateSeries => "generate_series",
            Self::SubdivideBitmap => "subdivide_bitmap",
        }
    }
}
pub const MAX_TABLE_FUNCTION_OUTPUT_ROWS: usize = u32::MAX as usize;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeriesObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum SeriesFailure<E> {
    Data(String),
    Control(E),
}
impl<E> From<String> for SeriesFailure<E> {
    fn from(message: String) -> Self {
        Self::Data(message)
    }
}
#[derive(Debug)]
pub struct SeriesExpansion {
    pub row_counts: Vec<usize>,
    pub values: Vec<Option<i128>>,
    pub total_rows: usize,
}
/// The raw shell preserves its original String error and no-observer lifecycle.
pub fn expand(
    start_col: &ArrayRef,
    end_col: &ArrayRef,
    step_col: Option<&ArrayRef>,
    num_rows: usize,
    is_left_join: bool,
) -> Result<SeriesExpansion, String> {
    match expand_observed(
        start_col,
        end_col,
        step_col,
        num_rows,
        is_left_join,
        &mut |_| Ok::<(), std::convert::Infallible>(()),
    ) {
        Ok(value) => Ok(value),
        Err(SeriesFailure::Data(message)) => Err(message),
        Err(SeriesFailure::Control(never)) => match never {},
    }
}
/// The omitted-step recipe is the original two-argument Some(1) author.
/// A compiled source must freeze that recipe, not synthesize an argument value.
pub fn expand_observed<E>(
    start_col: &ArrayRef,
    end_col: &ArrayRef,
    step_col: Option<&ArrayRef>,
    num_rows: usize,
    is_left_join: bool,
    observe: &mut dyn FnMut(SeriesObservation) -> Result<(), E>,
) -> Result<SeriesExpansion, SeriesFailure<E>> {
    observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
    let mut row_counts = Vec::with_capacity(num_rows);
    let mut series_values: Vec<Option<i128>> = Vec::new();
    let mut total_output_rows = 0usize;
    for row in 0..num_rows {
        observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
        let start = integer_argument(start_col, row, 0, IntegerDiagnosticContext::GenerateSeries)?;
        let end = integer_argument(end_col, row, 1, IntegerDiagnosticContext::GenerateSeries)?;
        let step = match step_col.as_ref() {
            Some(col) => integer_argument(col, row, 2, IntegerDiagnosticContext::GenerateSeries)?,
            None => Some(1),
        };
        match (start, end, step) {
            (Some(start), Some(end), Some(step)) => {
                if step == 0 {
                    return Err("table function generate_series step size cannot equal zero"
                        .to_string()
                        .into());
                }
                let count = count(start, end, step)?;
                if count == 0 {
                    if is_left_join {
                        checked_add_output_rows(&mut total_output_rows, 1)?;
                        row_counts.push(1);
                        series_values.push(None);
                    } else {
                        row_counts.push(0);
                    }
                    continue;
                }

                checked_add_output_rows(&mut total_output_rows, count)?;
                row_counts.push(count);
                let mut current = start;
                for _ in 0..count {
                    observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
                    series_values.push(Some(current));
                    current = current.checked_add(step).ok_or_else(|| {
                        format!(
                            "table function generate_series value overflow: current={} step={}",
                            current, step
                        )
                    })?;
                }
            }
            _ => {
                if is_left_join {
                    checked_add_output_rows(&mut total_output_rows, 1)?;
                    row_counts.push(1);
                    series_values.push(None);
                } else {
                    row_counts.push(0);
                }
            }
        }
    }

    if total_output_rows == 0 {
        return Ok(SeriesExpansion {
            row_counts,
            values: series_values,
            total_rows: total_output_rows,
        });
    }
    if series_values.len() != total_output_rows {
        return Err(format!(
            "table function generate_series internal output size mismatch: values={} rows={}",
            series_values.len(),
            total_output_rows
        )
        .into());
    }

    observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
    Ok(SeriesExpansion {
        row_counts,
        values: series_values,
        total_rows: total_output_rows,
    })
}
pub fn integer_argument(
    array: &ArrayRef,
    row: usize,
    arg_idx: usize,
    diagnostic: IntegerDiagnosticContext,
) -> Result<Option<i128>, String> {
    let fn_name = diagnostic.name();
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast Int8Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast Int16Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast Int32Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast Int64Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::UInt8Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast UInt8Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::UInt16Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast UInt16Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::UInt32Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast UInt32Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .ok_or_else(|| format!("table function {fn_name} downcast UInt64Array failed"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i128::from(arr.value(row))))
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    format!("table function {fn_name} downcast FixedSizeBinaryArray failed")
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let value = largeint::i128_from_be_bytes(arr.value(row))
                    .map_err(|e| format!("table function {fn_name} decode LARGEINT failed: {e}"))?;
                Ok(Some(value))
            }
        }
        other => Err(format!(
            "table function {fn_name} arg {arg_idx} expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {:?}",
            other
        )),
    }
}

/// Return conversion is deliberately separate: the raw shell first replicates outer rows.
pub fn result_column(
    values: Vec<Option<i128>>,
    ret_types: &[DataType],
) -> Result<ArrayRef, String> {
    match result_column_observed(values, ret_types, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    }) {
        Ok(value) => Ok(value),
        Err(SeriesFailure::Data(message)) => Err(message),
        Err(SeriesFailure::Control(never)) => match never {},
    }
}
pub fn result_column_observed<E>(
    values: Vec<Option<i128>>,
    ret_types: &[DataType],
    observe: &mut dyn FnMut(SeriesObservation) -> Result<(), E>,
) -> Result<ArrayRef, SeriesFailure<E>> {
    if ret_types.len() != 1 {
        return Err(format!(
            "table function generate_series expects 1 return type, got {}",
            ret_types.len()
        )
        .into());
    }
    match ret_types
        .first()
        .ok_or_else(|| "table function generate_series missing return type".to_string())?
    {
        DataType::Int8 => {
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let mut out_i8 = Vec::with_capacity(values.len());
            for value in values {
                observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
                let v = match value {
                    Some(v) => Some(i8::try_from(v).map_err(|_| {
                        format!(
                            "table function generate_series value out of TINYINT range: {v}"
                        )
                    })?),
                    None => None,
                };
                out_i8.push(v);
            }
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let out = Arc::new(Int8Array::from(out_i8)) as ArrayRef;
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            Ok(out)
        }
        DataType::Int16 => {
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let mut out_i16 = Vec::with_capacity(values.len());
            for value in values {
                observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
                let v = match value {
                    Some(v) => Some(i16::try_from(v).map_err(|_| {
                        format!(
                            "table function generate_series value out of SMALLINT range: {v}"
                        )
                    })?),
                    None => None,
                };
                out_i16.push(v);
            }
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let out = Arc::new(Int16Array::from(out_i16)) as ArrayRef;
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            Ok(out)
        }
        DataType::Int32 => {
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let mut out_i32 = Vec::with_capacity(values.len());
            for value in values {
                observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
                let v = match value {
                    Some(v) => Some(i32::try_from(v).map_err(|_| {
                        format!("table function generate_series value out of INT range: {v}")
                    })?),
                    None => None,
                };
                out_i32.push(v);
            }
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let out = Arc::new(Int32Array::from(out_i32)) as ArrayRef;
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            Ok(out)
        }
        DataType::Int64 => {
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let mut out_i64 = Vec::with_capacity(values.len());
            for value in values {
                observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
                let v = match value {
                    Some(v) => Some(i64::try_from(v).map_err(|_| {
                        format!("table function generate_series value out of BIGINT range: {v}")
                    })?),
                    None => None,
                };
                out_i64.push(v);
            }
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let out = Arc::new(Int64Array::from(out_i64)) as ArrayRef;
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            Ok(out)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            largeint::array_from_i128_observed(&values, &mut |event| {
                observe(match event {
                    largeint::LargeIntObservation::Step => SeriesObservation::Step,
                    largeint::LargeIntObservation::OpaqueBoundary => SeriesObservation::OpaqueBoundary,
                })
            }).map_err(SeriesFailure::Control)?.map_err(SeriesFailure::Data)
        }
        other => Err(format!(
            "table function generate_series return type expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {:?}",
            other
        ).into()),
    }
}
pub fn count(start: i128, end: i128, step: i128) -> Result<usize, String> {
    if step > 0 {
        if start > end {
            return Ok(0);
        }
        let diff = end - start;
        let count = diff / step + 1;
        usize::try_from(count)
            .map_err(|_| format!("table function generate_series count overflow: {count}"))
    } else {
        if start < end {
            return Ok(0);
        }
        let diff = start - end;
        let step_abs = step.abs();
        let count = diff / step_abs + 1;
        usize::try_from(count)
            .map_err(|_| format!("table function generate_series count overflow: {count}"))
    }
}
pub fn checked_add_output_rows(total: &mut usize, rows: usize) -> Result<(), String> {
    *total = total
        .checked_add(rows)
        .ok_or_else(|| "table function output too large".to_string())?;
    if *total > MAX_TABLE_FUNCTION_OUTPUT_ROWS {
        return Err("table function output too large".to_string());
    }
    Ok(())
}

#[cfg(test)]
#[path = "generate_series_core_tests.rs"]
mod tests;
