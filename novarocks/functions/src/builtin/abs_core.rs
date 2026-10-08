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

//! ONE original ABS computation body; selected callers only project addresses.
use crate::{KernelFailure, largeint};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array,
};
use arrow_buffer::i256;
use arrow_cast::cast;
use arrow_schema::DataType;
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub enum AbsObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum AbsError {
    Legacy(String),
    Kernel(KernelFailure),
}
impl From<String> for AbsError {
    fn from(value: String) -> Self {
        Self::Legacy(value)
    }
}
impl From<KernelFailure> for AbsError {
    fn from(value: KernelFailure) -> Self {
        Self::Kernel(value)
    }
}
impl std::fmt::Display for AbsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(value) => f.write_str(value),
            Self::Kernel(error) => write!(f, "{error}"),
        }
    }
}
fn build_largeint(
    values: &[Option<i128>],
    observe: &mut dyn FnMut(AbsObservation) -> Result<(), KernelFailure>,
) -> Result<ArrayRef, AbsError> {
    largeint::array_from_i128_observed(values, &mut |observation| {
        observe(match observation {
            largeint::LargeIntObservation::Step => AbsObservation::Step,
            largeint::LargeIntObservation::OpaqueBoundary => AbsObservation::OpaqueBoundary,
        })
    })?
    .map_err(AbsError::Legacy)
}
fn build_output(
    observe: &mut dyn FnMut(AbsObservation) -> Result<(), KernelFailure>,
    build: impl FnOnce() -> Result<ArrayRef, String>,
) -> Result<ArrayRef, AbsError> {
    observe(AbsObservation::OpaqueBoundary)?;
    let out = build()?;
    observe(AbsObservation::OpaqueBoundary)?;
    Ok(out)
}
fn eval_abs_largeint_output(
    value_arr: ArrayRef,
    observe: &mut dyn FnMut(AbsObservation) -> Result<(), KernelFailure>,
) -> Result<ArrayRef, AbsError> {
    match value_arr.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == &DataType::Int64 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int64)
                    .map_err(|e| format!("abs: failed to cast input to Int64: {}", e))?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "abs: failed to downcast to Int64Array".to_string())?;

            observe(AbsObservation::OpaqueBoundary)?;

            let mut values = Vec::with_capacity(int_arr.len());

            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..int_arr.len() {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let widened = i128::from(int_arr.value(i));
                    let absolute = widened.checked_abs().ok_or_else(|| {
                        "abs: widened BIGINT result does not fit LARGEINT".to_string()
                    })?;
                    values.push(Some(absolute));
                }
            }
            build_largeint(&values, observe)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let largeint_arr = largeint::as_fixed_size_binary_array(&value_arr, "abs")?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(largeint_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..largeint_arr.len() {
                observe(AbsObservation::Step)?;
                if largeint_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = largeint::value_at(largeint_arr, i)?;
                    // Preserve the explicit two's-complement LARGEINT boundary contract.
                    values.push(Some(v.wrapping_abs()));
                }
            }
            build_largeint(&values, observe)
        }
        DataType::Null => {
            let values = vec![None; value_arr.len()];
            build_largeint(&values, observe)
        }
        other => Err(AbsError::Legacy(format!(
            "abs: unsupported input type {:?} for LARGEINT output",
            other
        ))),
    }
}

/// Evaluate abs function.
/// Supports:
/// - abs(int): returns int (absolute value with planned output type)
/// - abs(float): returns float (absolute value with planned output type)
/// - abs(decimal): returns decimal (absolute value)
///
/// Implementation aligns with StarRocks BE:
/// - Execute according to FE-declared output type.
/// - Surface overflow explicitly when planned output type cannot represent ABS result.
pub fn evaluate_abs_core(
    value_arr: ArrayRef,
    output_type: &DataType,
    observe: &mut dyn FnMut(AbsObservation) -> Result<(), KernelFailure>,
) -> Result<ArrayRef, AbsError> {
    // Execute ABS according to FE-declared output type. This avoids type guessing and keeps
    // overflow behavior aligned with planned return type (for example BIGINT -> LARGEINT).
    match output_type {
        t if largeint::is_largeint_data_type(t) => eval_abs_largeint_output(value_arr, observe),
        DataType::Int8 => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == &DataType::Int8 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int8)
                    .map_err(|e| format!("abs: failed to cast input to Int8: {}", e))?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "abs: failed to downcast to Int8Array".to_string())?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(int_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..int_arr.len() {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        "abs overflow on Int8 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            build_output(
                observe,
                || Ok(Arc::new(Int8Array::from(values)) as ArrayRef),
            )
        }
        DataType::Int16 => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == &DataType::Int16 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int16)
                    .map_err(|e| format!("abs: failed to cast input to Int16: {}", e))?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "abs: failed to downcast to Int16Array".to_string())?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(int_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..int_arr.len() {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        "abs overflow on Int16 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            build_output(observe, || {
                Ok(Arc::new(Int16Array::from(values)) as ArrayRef)
            })
        }
        DataType::Int32 => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == &DataType::Int32 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int32)
                    .map_err(|e| format!("abs: failed to cast input to Int32: {}", e))?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "abs: failed to downcast to Int32Array".to_string())?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(int_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..int_arr.len() {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        "abs overflow on Int32 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            build_output(observe, || {
                Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
            })
        }
        DataType::Int64 => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == &DataType::Int64 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int64)
                    .map_err(|e| format!("abs: failed to cast input to Int64: {}", e))?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "abs: failed to downcast to Int64Array".to_string())?;

            let len = int_arr.len();
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(len);
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..len {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = int_arr.value(i);
                    let v_abs = v.checked_abs().ok_or_else(|| {
                        "abs overflow on Int64 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            build_output(observe, || {
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            })
        }
        DataType::Float32 => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == &DataType::Float32 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Float32)
                    .map_err(|e| format!("abs: failed to cast input to Float32: {}", e))?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let float_arr = casted
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "abs: failed to downcast to Float32Array".to_string())?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(float_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..float_arr.len() {
                observe(AbsObservation::Step)?;
                if float_arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some(float_arr.value(i).abs()));
                }
            }
            build_output(observe, || {
                Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
            })
        }
        DataType::Float64 => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == &DataType::Float64 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Float64)
                    .map_err(|e| format!("abs: failed to cast input to Float64: {}", e))?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let float_arr = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "abs: failed to downcast to Float64Array".to_string())?;

            let len = float_arr.len();
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(len);
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..len {
                observe(AbsObservation::Step)?;
                if float_arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some(float_arr.value(i).abs()));
                }
            }
            build_output(observe, || {
                Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
            })
        }
        DataType::Decimal128(out_precision, out_scale) => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == output_type {
                value_arr
            } else {
                cast(&value_arr, output_type).map_err(|e| {
                    format!(
                        "abs: failed to cast input from {:?} to {:?}: {}",
                        value_arr.data_type(),
                        output_type,
                        e
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let dec_arr = casted
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "abs: failed to downcast to Decimal128Array".to_string())?;

            let len = dec_arr.len();
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(len);
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..len {
                observe(AbsObservation::Step)?;
                if dec_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = dec_arr.value(i);
                    let v_abs = v
                        .checked_abs()
                        .ok_or_else(|| "abs overflow on Decimal128 minimum".to_string())?;
                    values.push(Some(v_abs));
                }
            }
            observe(AbsObservation::OpaqueBoundary)?;
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| format!("abs: failed to create Decimal128Array: {}", e))?;
            let out = Arc::new(array) as ArrayRef;
            observe(AbsObservation::OpaqueBoundary)?;
            Ok(out)
        }
        DataType::Decimal256(out_precision, out_scale) => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == output_type {
                value_arr
            } else {
                cast(&value_arr, output_type).map_err(|e| {
                    format!(
                        "abs: failed to cast input from {:?} to {:?}: {}",
                        value_arr.data_type(),
                        output_type,
                        e
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let dec_arr = casted
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "abs: failed to downcast to Decimal256Array".to_string())?;

            let len = dec_arr.len();
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values: Vec<Option<i256>> = Vec::with_capacity(len);
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..len {
                observe(AbsObservation::Step)?;
                if dec_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = dec_arr.value(i);
                    let v_abs = if v.is_negative() {
                        v.checked_neg()
                            .ok_or_else(|| "abs overflow on Decimal256 minimum".to_string())?
                    } else {
                        v
                    };
                    values.push(Some(v_abs));
                }
            }
            observe(AbsObservation::OpaqueBoundary)?;
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| format!("abs: failed to create Decimal256Array: {}", e))?;
            let out = Arc::new(array) as ArrayRef;
            observe(AbsObservation::OpaqueBoundary)?;
            Ok(out)
        }
        other => Err(AbsError::Legacy(format!(
            "abs: unsupported output type from FE plan: {:?}",
            other
        ))),
    }
}
