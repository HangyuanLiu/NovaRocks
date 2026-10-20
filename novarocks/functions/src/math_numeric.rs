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
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow_schema::DataType;

#[derive(Clone, Debug)]
pub enum NumericArrayView<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
    Float64(&'a Float64Array),
    Float32(&'a Float32Array),
    Decimal128(&'a Decimal128Array, i8),
    Null(usize),
}

impl<'a> NumericArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, NumericArrayFailure> {
        match array.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(|| NumericArrayFailure::new("failed to downcast to Int64Array")),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(|| NumericArrayFailure::new("failed to downcast to Int32Array")),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(|| NumericArrayFailure::new("failed to downcast to Int16Array")),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(|| NumericArrayFailure::new("failed to downcast to Int8Array")),
            DataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or_else(|| NumericArrayFailure::new("failed to downcast to Float64Array")),
            DataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(Self::Float32)
                .ok_or_else(|| NumericArrayFailure::new("failed to downcast to Float32Array")),
            DataType::Decimal128(_, scale) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(|arr| Self::Decimal128(arr, *scale))
                .ok_or_else(|| NumericArrayFailure::new("failed to downcast to Decimal128Array")),
            DataType::Null => Ok(Self::Null(array.len())),
            other => Err(NumericArrayFailure::new(format_args!(
                "unsupported numeric type: {:?}",
                other
            ))),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NumericArrayView::Int64(arr) => arr.len(),
            NumericArrayView::Int32(arr) => arr.len(),
            NumericArrayView::Int16(arr) => arr.len(),
            NumericArrayView::Int8(arr) => arr.len(),
            NumericArrayView::Float64(arr) => arr.len(),
            NumericArrayView::Float32(arr) => arr.len(),
            NumericArrayView::Decimal128(arr, _) => arr.len(),
            NumericArrayView::Null(len) => *len,
        }
    }

    pub fn value_f64(&self, row: usize) -> Option<f64> {
        match self {
            NumericArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Float64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            NumericArrayView::Float32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Decimal128(arr, scale) => (!arr.is_null(row)).then(|| {
                let v = arr.value(row) as f64;
                let divisor = 10_f64.powi(*scale as i32);
                v / divisor
            }),
            NumericArrayView::Null(_) => None,
        }
    }

    pub fn value_i64(&self, row: usize) -> Option<i64> {
        match self {
            NumericArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            NumericArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            NumericArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            NumericArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            NumericArrayView::Float64(arr) => (!arr.is_null(row))
                .then(|| arr.value(row))
                .filter(|v| v.is_finite())
                .map(|v| v as i64),
            NumericArrayView::Float32(arr) => (!arr.is_null(row))
                .then(|| arr.value(row) as f64)
                .filter(|v| v.is_finite())
                .map(|v| v as i64),
            NumericArrayView::Decimal128(arr, scale) => (!arr.is_null(row)).then(|| {
                let v = arr.value(row) as f64;
                let divisor = 10_f64.powi(*scale as i32);
                (v / divisor) as i64
            }),
            NumericArrayView::Null(_) => None,
        }
    }
}

pub fn value_at_f64(view: &NumericArrayView<'_>, row: usize, len: usize) -> Option<f64> {
    if view.len() == 1 && len > 1 {
        view.value_f64(0)
    } else {
        view.value_f64(row)
    }
}

pub fn value_at_i64(view: &NumericArrayView<'_>, row: usize, len: usize) -> Option<i64> {
    if view.len() == 1 && len > 1 {
        view.value_i64(0)
    } else {
        view.value_i64(row)
    }
}

use arrow_array::builder::{Float32Builder, Float64Builder};
use arrow_cast::cast;
use std::sync::Arc;

/// Work observation grants no allocation capacity. Opaque boundaries cover
/// the original Arrow builder/cast calls; Step covers each original row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MathNumericObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum MathNumericError {
    Carrier(NumericArrayFailure),
    OutputCast(arrow_schema::ArrowError),
    Gather(arrow_schema::ArrowError),
    Kernel(crate::KernelFailure),
}
impl From<crate::KernelFailure> for MathNumericError {
    fn from(error: crate::KernelFailure) -> Self {
        Self::Kernel(error)
    }
}
impl std::fmt::Display for MathNumericError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Carrier(cause) => cause.fmt(f),
            Self::OutputCast(cause) => write!(f, "math: failed to cast output: {cause}"),
            Self::Gather(cause) => write!(f, "numeric unary selected gather failed: {cause}"),
            Self::Kernel(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for MathNumericError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Carrier(cause) => Some(cause),
            Self::OutputCast(cause) | Self::Gather(cause) => Some(cause),
            Self::Kernel(cause) => Some(cause),
        }
    }
}
impl MathNumericError {
    pub fn into_kernel(self) -> crate::KernelFailure {
        match self {
            Self::Kernel(cause) => cause,
            cause => crate::KernelFailure::from_domain(cause),
        }
    }
}

fn sanitize_non_finite_float_output(
    out: ArrayRef,
    observe: &mut dyn FnMut(MathNumericObservation) -> Result<(), crate::KernelFailure>,
) -> Result<ArrayRef, MathNumericError> {
    match out.data_type() {
        DataType::Float64 => {
            let Some(arr) = out.as_any().downcast_ref::<Float64Array>() else {
                return Err(MathNumericError::Carrier(NumericArrayFailure::new(
                    "math numeric output has a foreign frozen floating carrier",
                )));
            };
            observe(MathNumericObservation::OpaqueBoundary)?;
            let mut builder = Float64Builder::new();
            for row in 0..arr.len() {
                observe(MathNumericObservation::Step)?;
                if arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = arr.value(row);
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            observe(MathNumericObservation::OpaqueBoundary)?;
            let values = Arc::new(builder.finish()) as ArrayRef;
            observe(MathNumericObservation::OpaqueBoundary)?;
            Ok(values)
        }
        DataType::Float32 => {
            let Some(arr) = out.as_any().downcast_ref::<Float32Array>() else {
                return Err(MathNumericError::Carrier(NumericArrayFailure::new(
                    "math numeric output has a foreign frozen floating carrier",
                )));
            };
            observe(MathNumericObservation::OpaqueBoundary)?;
            let mut builder = Float32Builder::new();
            for row in 0..arr.len() {
                observe(MathNumericObservation::Step)?;
                if arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = arr.value(row);
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            observe(MathNumericObservation::OpaqueBoundary)?;
            let values = Arc::new(builder.finish()) as ArrayRef;
            observe(MathNumericObservation::OpaqueBoundary)?;
            Ok(values)
        }
        _ => Ok(out),
    }
}

/// Original legacy projection, including full Arrow diagnostic text.
pub fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
) -> Result<ArrayRef, MathNumericError> {
    cast_output_observed(out, output_type, &mut |_| Ok(()))
}
/// The SAME sanitizer and safe Arrow cast with selected work observations.
pub fn cast_output_observed(
    out: ArrayRef,
    output_type: Option<&DataType>,
    observe: &mut dyn FnMut(MathNumericObservation) -> Result<(), crate::KernelFailure>,
) -> Result<ArrayRef, MathNumericError> {
    let out = sanitize_non_finite_float_output(out, observe)?;
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    observe(MathNumericObservation::OpaqueBoundary)?;
    let casted = cast(&out, target).map_err(MathNumericError::OutputCast)?;
    observe(MathNumericObservation::OpaqueBoundary)?;
    sanitize_non_finite_float_output(casted, observe)
}

#[derive(Debug)]
pub struct NumericArrayFailure(novarocks_failure_contract::BoundedDetail);
impl NumericArrayFailure {
    pub fn new(detail: impl std::fmt::Display) -> Self {
        Self(novarocks_failure_contract::BoundedDetail::truncating_display(detail))
    }
}
impl std::fmt::Display for NumericArrayFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for NumericArrayFailure {}
impl novarocks_failure_contract::FailureProjection for NumericArrayFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            novarocks_failure_contract::FailureReason::new(
                novarocks_failure_contract::domain::KERNEL,
                std::num::NonZeroU16::new(237).unwrap(),
            ),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for NumericArrayFailure {}
impl novarocks_failure_contract::FailureProjection for MathNumericError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let source = match self {
            Self::Carrier(cause) => return cause.failure_fact(),
            Self::Kernel(cause) => return cause.failure_fact(),
            Self::OutputCast(cause) | Self::Gather(cause) => cause,
        };
        // Safe output projection has no lawful row-data error channel. A
        // selected gather's arithmetic overflow describes copy geometry.
        let (class, code) = match source {
            ArrowError::MemoryError(_) => (FailureClass::Resource, 238),
            ArrowError::ArithmeticOverflow(_) if matches!(self, Self::Gather(_)) => {
                (FailureClass::Resource, 238)
            }
            ArrowError::OffsetOverflowError(_) => (FailureClass::Representation, 239),
            ArrowError::IoError(..) => (FailureClass::Unexpected, 240),
            _ => (FailureClass::Invariant, 241),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(source, ArrowError::OffsetOverflowError(_)) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(39).unwrap(),
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
impl novarocks_failure_contract::OuterFailureProjection for MathNumericError {}
#[cfg(test)]
mod source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn numeric_math_keeps_actual_arrow_source_and_unknown_offset_facts() {
        let source = MathNumericError::OutputCast(arrow_schema::ArrowError::MemoryError(
            "numeric overflow cancelled".into(),
        ));
        assert_eq!(source.failure_fact().class(), FailureClass::Resource);
        assert!(source.source().unwrap().is::<arrow_schema::ArrowError>());
        let offset = MathNumericError::Gather(arrow_schema::ArrowError::OffsetOverflowError(0));
        assert_eq!(offset.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            offset.failure_fact().representation().unwrap().required,
            None
        );
    }
    #[test]
    fn numeric_view_contract_failure_does_not_retain_nested_field_or_array() {
        let field = Arc::new(arrow_schema::Field::new("item", DataType::Int32, true));
        let weak = Arc::downgrade(&field);
        let array = arrow_array::new_empty_array(&DataType::List(field));
        let array_weak = Arc::downgrade(&array);
        let cause = NumericArrayView::new(&array).unwrap_err();
        drop(array);
        assert!(weak.upgrade().is_none());
        assert!(array_weak.upgrade().is_none());
        assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
    }
}
