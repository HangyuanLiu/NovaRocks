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
#[path = "generate_series_error.rs"]
mod error;
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow_schema::DataType;
pub use error::SeriesError;
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
    Cause(SeriesError),
    Control(E),
}
impl<E> From<SeriesError> for SeriesFailure<E> {
    fn from(cause: SeriesError) -> Self {
        Self::Cause(cause)
    }
}
#[derive(Debug)]
pub struct SeriesExpansion {
    pub row_counts: Vec<usize>,
    pub values: Vec<Option<i128>>,
    pub total_rows: usize,
}
/// The raw caller uses the same typed cause with an infallible observer.
pub fn expand(
    start_col: &ArrayRef,
    end_col: &ArrayRef,
    step_col: Option<&ArrayRef>,
    num_rows: usize,
    is_left_join: bool,
) -> Result<SeriesExpansion, SeriesError> {
    match expand_observed(
        start_col,
        end_col,
        step_col,
        num_rows,
        is_left_join,
        &mut |_| Ok::<(), std::convert::Infallible>(()),
    ) {
        Ok(value) => Ok(value),
        Err(SeriesFailure::Cause(cause)) => Err(cause),
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
                    return Err(SeriesError::ZeroStep.into());
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
                for ordinal in 0..count {
                    observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
                    series_values.push(Some(current));
                    if ordinal + 1 < count {
                        current = current
                            .checked_add(step)
                            .ok_or(SeriesError::ValueProgress { current, step })?;
                    }
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
        return Err(SeriesError::OutputLength {
            values: series_values.len(),
            rows: total_output_rows,
        }
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
) -> Result<Option<i128>, SeriesError> {
    if row >= array.len() {
        return Err(SeriesError::ArgumentBounds {
            argument: arg_idx,
            row,
            rows: array.len(),
        });
    }
    let array_type = || SeriesError::ArrayType {
        function: diagnostic,
        argument: arg_idx,
        data_type: array.data_type().clone(),
    };
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
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
                .ok_or_else(array_type)?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let bytes = arr.value(row);
                let bytes: [u8; 16] = bytes.try_into().map_err(|_| SeriesError::LargeIntWidth {
                    function: diagnostic,
                    argument: arg_idx,
                    bytes: bytes.len(),
                })?;
                let value = i128::from_be_bytes(bytes);
                Ok(Some(value))
            }
        }
        other => Err(SeriesError::ArgumentType {
            function: diagnostic,
            argument: arg_idx,
            data_type: other.clone(),
        }),
    }
}

/// Return conversion is deliberately separate: the raw shell first replicates outer rows.
pub fn result_column(
    values: Vec<Option<i128>>,
    ret_types: &[DataType],
) -> Result<ArrayRef, SeriesError> {
    match result_column_observed(values, ret_types, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    }) {
        Ok(value) => Ok(value),
        Err(SeriesFailure::Cause(cause)) => Err(cause),
        Err(SeriesFailure::Control(never)) => match never {},
    }
}
pub fn result_column_observed<E>(
    values: Vec<Option<i128>>,
    ret_types: &[DataType],
    observe: &mut dyn FnMut(SeriesObservation) -> Result<(), E>,
) -> Result<ArrayRef, SeriesFailure<E>> {
    let [data_type] = ret_types else {
        return Err(SeriesError::ReturnCount {
            actual: ret_types.len(),
        }
        .into());
    };
    match data_type {
        DataType::Int8 => {
            observe(SeriesObservation::OpaqueBoundary).map_err(SeriesFailure::Control)?;
            let mut out_i8 = Vec::with_capacity(values.len());
            for value in values {
                observe(SeriesObservation::Step).map_err(SeriesFailure::Control)?;
                let v = match value {
                    Some(v) => Some(i8::try_from(v).map_err(|_| SeriesError::ValueOutOfRange {
                        value: v,
                        data_type: data_type.clone(),
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
                    Some(v) => {
                        Some(i16::try_from(v).map_err(|_| SeriesError::ValueOutOfRange {
                            value: v,
                            data_type: data_type.clone(),
                        })?)
                    }
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
                    Some(v) => {
                        Some(i32::try_from(v).map_err(|_| SeriesError::ValueOutOfRange {
                            value: v,
                            data_type: data_type.clone(),
                        })?)
                    }
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
                    Some(v) => {
                        Some(i64::try_from(v).map_err(|_| SeriesError::ValueOutOfRange {
                            value: v,
                            data_type: data_type.clone(),
                        })?)
                    }
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
                    largeint::LargeIntObservation::OpaqueBoundary => {
                        SeriesObservation::OpaqueBoundary
                    }
                })
            })
            .map_err(SeriesFailure::Control)
        }
        other => Err(SeriesError::ReturnType {
            data_type: other.clone(),
        }
        .into()),
    }
}
/// Count in an unsigned distance domain: two signed endpoints can be 2^128-1 apart.
pub fn count(start: i128, end: i128, step: i128) -> Result<usize, SeriesError> {
    if step == 0 {
        return Err(SeriesError::ZeroStep);
    }
    if (step > 0 && start > end) || (step < 0 && start < end) {
        return Ok(0);
    }
    let required = start
        .abs_diff(end)
        .checked_div(step.unsigned_abs())
        .expect("nonzero step")
        .checked_add(1);
    required
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(SeriesError::OutputRows { required })
}

pub fn checked_add_output_rows(total: &mut usize, rows: usize) -> Result<(), SeriesError> {
    let required = (*total as u128).checked_add(rows as u128);
    let next = total
        .checked_add(rows)
        .filter(|next| *next <= MAX_TABLE_FUNCTION_OUTPUT_ROWS)
        .ok_or(SeriesError::OutputRows { required })?;
    *total = next;
    Ok(())
}

#[cfg(test)]
#[path = "generate_series_core_tests.rs"]
mod tests;
