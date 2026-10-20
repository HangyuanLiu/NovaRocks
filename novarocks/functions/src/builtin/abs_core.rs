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
pub struct AbsError(Box<AbsCause>);
#[derive(Debug)]
enum AbsCause {
    Contract(novarocks_failure_contract::BoundedDetail),
    Numeric(&'static str),
    Slice(std::array::TryFromSliceError),
    Arrow {
        role: AbsArrowRole,
        context: novarocks_failure_contract::BoundedDetail,
        source: arrow_schema::ArrowError,
    },
    Kernel(KernelFailure),
}
#[derive(Clone, Copy, Debug)]
enum AbsArrowRole {
    Cast,
    Construct,
    Gather,
}
impl AbsError {
    fn contract(detail: impl std::fmt::Display) -> Self {
        Self(Box::new(AbsCause::Contract(
            novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        )))
    }
    fn numeric(detail: &'static str) -> Self {
        Self(Box::new(AbsCause::Numeric(detail)))
    }
    fn arrow(
        role: AbsArrowRole,
        context: impl std::fmt::Display,
        source: arrow_schema::ArrowError,
    ) -> Self {
        Self(Box::new(AbsCause::Arrow {
            role,
            context: novarocks_failure_contract::BoundedDetail::truncating_display(context),
            source,
        }))
    }
    pub(super) fn gather(source: arrow_schema::ArrowError) -> Self {
        Self::arrow(AbsArrowRole::Gather, "ABS selected gather failed", source)
    }
    pub fn into_kernel(self) -> KernelFailure {
        match *self.0 {
            AbsCause::Kernel(cause) => cause,
            cause => KernelFailure::from_domain(Self(Box::new(cause))),
        }
    }
}
impl From<KernelFailure> for AbsError {
    fn from(source: KernelFailure) -> Self {
        Self(Box::new(AbsCause::Kernel(source)))
    }
}
impl std::fmt::Display for AbsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            AbsCause::Contract(detail) => f.write_str(detail.as_str()),
            AbsCause::Numeric(detail) => f.write_str(detail),
            AbsCause::Slice(source) => write!(f, "abs: invalid LARGEINT fixed width: {source}"),
            AbsCause::Arrow {
                context, source, ..
            } => write!(f, "{}: {source}", context.as_str()),
            AbsCause::Kernel(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for AbsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.as_ref() {
            AbsCause::Slice(source) => Some(source),
            AbsCause::Arrow { source, .. } => Some(source),
            AbsCause::Kernel(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for AbsError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, code) = match self.0.as_ref() {
            AbsCause::Kernel(source) => return source.failure_fact(),
            AbsCause::Numeric(_) => {
                return crate::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            AbsCause::Arrow {
                role: AbsArrowRole::Cast,
                source: ArrowError::ArithmeticOverflow(_),
                ..
            } => return crate::DataFailureReason::NumericOutOfRange.failure_fact(),
            // The existing safe Arrow input cast reports malformed row values
            // through its CastError; frozen carrier/constructor refusals do not.
            AbsCause::Arrow {
                role: AbsArrowRole::Cast,
                source: ArrowError::CastError(_),
                ..
            } => return crate::DataFailureReason::InvalidValue.failure_fact(),
            AbsCause::Contract(_) | AbsCause::Slice(_) => (FailureClass::Invariant, 242),
            AbsCause::Arrow {
                source: ArrowError::MemoryError(_),
                ..
            }
            | AbsCause::Arrow {
                role: AbsArrowRole::Gather,
                source: ArrowError::ArithmeticOverflow(_),
                ..
            } => (FailureClass::Resource, 243),
            AbsCause::Arrow {
                source: ArrowError::OffsetOverflowError(_),
                ..
            } => (FailureClass::Representation, 244),
            AbsCause::Arrow {
                source: ArrowError::IoError(..),
                ..
            } => (FailureClass::Unexpected, 245),
            AbsCause::Arrow { .. } => (FailureClass::Invariant, 246),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if class == FailureClass::Representation {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(40).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for AbsError {}
fn largeint_view(array: &ArrayRef) -> Result<&arrow_array::FixedSizeBinaryArray, AbsError> {
    let array = array
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .ok_or_else(|| AbsError::contract("abs: expected FixedSizeBinaryArray"))?;
    if array.value_length() != largeint::LARGEINT_BYTE_WIDTH {
        return Err(AbsError::contract(format_args!(
            "abs: expected FixedSizeBinary({}), got FixedSizeBinary({})",
            largeint::LARGEINT_BYTE_WIDTH,
            array.value_length()
        )));
    }
    Ok(array)
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
    })
    .map_err(AbsError::from)
}
fn build_output(
    observe: &mut dyn FnMut(AbsObservation) -> Result<(), KernelFailure>,
    build: impl FnOnce() -> Result<ArrayRef, AbsError>,
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
                cast(&value_arr, &DataType::Int64).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        "abs: failed to cast input to Int64",
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Int64Array"))?;

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
                        AbsError::numeric("abs: widened BIGINT result does not fit LARGEINT")
                    })?;
                    values.push(Some(absolute));
                }
            }
            build_largeint(&values, observe)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let largeint_arr = largeint_view(&value_arr)?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(largeint_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..largeint_arr.len() {
                observe(AbsObservation::Step)?;
                if largeint_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = i128::from_be_bytes(
                        largeint_arr
                            .value(i)
                            .try_into()
                            .map_err(|source| AbsError(Box::new(AbsCause::Slice(source))))?,
                    );
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
        other => Err(AbsError::contract(format_args!(
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
                cast(&value_arr, &DataType::Int8).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        "abs: failed to cast input to Int8",
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Int8Array"))?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(int_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..int_arr.len() {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        AbsError::numeric(
                            "abs overflow on Int8 minimum; FE should promote result type",
                        )
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
                cast(&value_arr, &DataType::Int16).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        "abs: failed to cast input to Int16",
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Int16Array"))?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(int_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..int_arr.len() {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        AbsError::numeric(
                            "abs overflow on Int16 minimum; FE should promote result type",
                        )
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
                cast(&value_arr, &DataType::Int32).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        "abs: failed to cast input to Int32",
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Int32Array"))?;
            observe(AbsObservation::OpaqueBoundary)?;
            let mut values = Vec::with_capacity(int_arr.len());
            observe(AbsObservation::OpaqueBoundary)?;
            for i in 0..int_arr.len() {
                observe(AbsObservation::Step)?;
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        AbsError::numeric(
                            "abs overflow on Int32 minimum; FE should promote result type",
                        )
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
                cast(&value_arr, &DataType::Int64).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        "abs: failed to cast input to Int64",
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Int64Array"))?;

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
                        AbsError::numeric(
                            "abs overflow on Int64 minimum; FE should promote result type",
                        )
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
                cast(&value_arr, &DataType::Float32).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        "abs: failed to cast input to Float32",
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let float_arr = casted
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Float32Array"))?;
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
                cast(&value_arr, &DataType::Float64).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        "abs: failed to cast input to Float64",
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let float_arr = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Float64Array"))?;

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
                cast(&value_arr, output_type).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        format_args!(
                            "abs: failed to cast input from {:?} to {:?}",
                            value_arr.data_type(),
                            output_type
                        ),
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let dec_arr = casted
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Decimal128Array"))?;

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
                        .ok_or_else(|| AbsError::numeric("abs overflow on Decimal128 minimum"))?;
                    values.push(Some(v_abs));
                }
            }
            observe(AbsObservation::OpaqueBoundary)?;
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Construct,
                        "abs: failed to create Decimal128Array",
                        source,
                    )
                })?;
            let out = Arc::new(array) as ArrayRef;
            observe(AbsObservation::OpaqueBoundary)?;
            Ok(out)
        }
        DataType::Decimal256(out_precision, out_scale) => {
            observe(AbsObservation::OpaqueBoundary)?;
            let casted = if value_arr.data_type() == output_type {
                value_arr
            } else {
                cast(&value_arr, output_type).map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Cast,
                        format_args!(
                            "abs: failed to cast input from {:?} to {:?}",
                            value_arr.data_type(),
                            output_type
                        ),
                        source,
                    )
                })?
            };
            observe(AbsObservation::OpaqueBoundary)?;
            let dec_arr = casted
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| AbsError::contract("abs: failed to downcast to Decimal256Array"))?;

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
                        v.checked_neg().ok_or_else(|| {
                            AbsError::numeric("abs overflow on Decimal256 minimum")
                        })?
                    } else {
                        v
                    };
                    values.push(Some(v_abs));
                }
            }
            observe(AbsObservation::OpaqueBoundary)?;
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|source| {
                    AbsError::arrow(
                        AbsArrowRole::Construct,
                        "abs: failed to create Decimal256Array",
                        source,
                    )
                })?;
            let out = Arc::new(array) as ArrayRef;
            observe(AbsObservation::OpaqueBoundary)?;
            Ok(out)
        }
        other => Err(AbsError::contract(format_args!(
            "abs: unsupported output type from FE plan: {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn abs_keeps_overflow_numeric_and_all_other_sources_outer() {
        let array = Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef;
        let failure = evaluate_abs_core(array, &DataType::Int64, &mut |_| Ok(())).unwrap_err();
        assert_eq!(
            failure.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        let source = AbsError::gather(arrow_schema::ArrowError::MemoryError(
            "numeric overflow cancelled".into(),
        ));
        assert_eq!(source.failure_fact().class(), FailureClass::Resource);
        assert!(source.source().unwrap().is::<arrow_schema::ArrowError>());
        let contract = AbsError::contract("cancelled numeric overflow quota exceeded");
        assert_eq!(contract.failure_fact().class(), FailureClass::Invariant);
        let original = KernelFailure::Cancelled;
        assert_eq!(AbsError::from(original.clone()).into_kernel(), original);
    }
}
