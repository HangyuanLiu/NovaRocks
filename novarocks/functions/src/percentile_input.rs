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

//! The original percentile input readers; the enum selects diagnostics only.
use crate::largeint;
use arrow_array::*;
use arrow_schema::DataType;
#[derive(Clone, Copy, Debug)]
pub enum PercentileInputDiagnostic<'a> {
    ExactUpdate,
    ExactMerge,
    Hash,
    ApproxRaw,
    LegacyLabel(&'a str),
}
impl std::fmt::Display for PercentileInputDiagnostic<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ExactUpdate => "percentile_disc_cont_update",
            Self::ExactMerge => "percentile_disc_cont_merge",
            Self::Hash => "percentile_hash",
            Self::ApproxRaw => "percentile_approx_raw",
            Self::LegacyLabel(label) => label,
        })
    }
}
pub enum PercentileNumericFailure<'a> {
    Downcast(&'static str),
    Unsupported(&'a DataType),
    // The sole LARGEINT byte reader owns this original raw diagnostic. The
    // accurately selected FSB16 carrier cannot encounter a width mismatch.
    LargeInt(crate::largeint::LargeIntCarrierFailure),
}
impl PercentileNumericFailure<'_> {
    pub fn message<'a>(
        &'a self,
        context: PercentileInputDiagnostic<'a>,
    ) -> PercentileNumericMessage<'a> {
        PercentileNumericMessage {
            failure: self,
            context,
        }
    }
}
pub struct PercentileNumericMessage<'a> {
    failure: &'a PercentileNumericFailure<'a>,
    context: PercentileInputDiagnostic<'a>,
}
impl std::fmt::Display for PercentileNumericMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.failure {
            PercentileNumericFailure::Downcast(kind) => {
                write!(f, "{}: failed to downcast {}", self.context, kind)
            }
            PercentileNumericFailure::Unsupported(ty) => write!(
                f,
                "{}: unsupported numeric input type {:?}",
                self.context, ty
            ),
            PercentileNumericFailure::LargeInt(error) => error.fmt(f),
        }
    }
}
pub fn numeric_value_at(
    array: &ArrayRef,
    row: usize,
    context: PercentileInputDiagnostic<'_>,
) -> Result<Option<f64>, crate::KernelFailure> {
    numeric_value_at_with_failure(array, row, &mut |failure| failure.into_failure(context))
}
pub fn numeric_value_at_with_failure<E>(
    array: &ArrayRef,
    row: usize,
    fail: &mut impl FnMut(PercentileNumericFailure<'_>) -> E,
) -> Result<Option<f64>, E> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("Int8Array")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("Int16Array")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("Int32Array")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("Int64Array")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("Float32Array")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("Float64Array")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("Decimal128Array")))?;
            let divisor = 10_f64.powi(*scale as i32);
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64 / divisor))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| fail(PercentileNumericFailure::Downcast("FixedSizeBinaryArray")))?;
            Ok((!arr.is_null(row)).then_some(
                largeint::value_at(arr, row)
                    .map_err(|error| fail(PercentileNumericFailure::LargeInt(error)))?
                    as f64,
            ))
        }
        other => Err(fail(PercentileNumericFailure::Unsupported(other))),
    }
}

pub enum PercentilePayloadFailure<'a> {
    Downcast(&'static str),
    Unsupported(&'a DataType),
}
impl PercentilePayloadFailure<'_> {
    pub fn message<'a>(
        &'a self,
        context: PercentileInputDiagnostic<'a>,
    ) -> PercentilePayloadMessage<'a> {
        PercentilePayloadMessage {
            failure: self,
            context,
        }
    }
}
pub struct PercentilePayloadMessage<'a> {
    failure: &'a PercentilePayloadFailure<'a>,
    context: PercentileInputDiagnostic<'a>,
}
impl std::fmt::Display for PercentilePayloadMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.failure {
            PercentilePayloadFailure::Downcast(kind) => {
                write!(f, "{}: failed to downcast {}", self.context, kind)
            }
            PercentilePayloadFailure::Unsupported(ty) => write!(
                f,
                "{}: unsupported percentile payload type {:?}",
                self.context, ty
            ),
        }
    }
}
pub fn payload_bytes_at<'a>(
    array: &'a ArrayRef,
    row: usize,
    context: PercentileInputDiagnostic<'_>,
) -> Result<Option<&'a [u8]>, crate::KernelFailure> {
    payload_bytes_at_with_failure(array, row, &mut |failure| failure.into_failure(context))
}
pub fn payload_bytes_at_with_failure<'a, E>(
    array: &'a ArrayRef,
    row: usize,
    fail: &mut impl FnMut(PercentilePayloadFailure<'_>) -> E,
) -> Result<Option<&'a [u8]>, E> {
    match array.data_type() {
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("BinaryArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("StringArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row).as_bytes()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("LargeBinaryArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("LargeStringArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row).as_bytes()))
        }
        other => Err(fail(PercentilePayloadFailure::Unsupported(other))),
    }
}

/// Freeze only a bounded contract diagnostic at the reader frontier; retaining
/// the carrier or its DataType graph would prolong unrelated field ownership.
#[derive(Debug)]
struct PercentileReaderContract {
    code: std::num::NonZeroU16,
    detail: novarocks_failure_contract::BoundedDetail,
}
impl std::fmt::Display for PercentileReaderContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.detail.fmt(f)
    }
}
impl std::error::Error for PercentileReaderContract {}
impl novarocks_failure_contract::FailureProjection for PercentileReaderContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, self.code),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for PercentileReaderContract {}
impl PercentileNumericFailure<'_> {
    pub fn into_failure(self, context: PercentileInputDiagnostic<'_>) -> crate::KernelFailure {
        if let Self::LargeInt(source) = self {
            return source.into();
        }
        crate::KernelFailure::from_domain(PercentileReaderContract {
            code: std::num::NonZeroU16::new(394).unwrap(),
            detail: novarocks_failure_contract::BoundedDetail::truncating_display(
                self.message(context),
            ),
        })
    }
}
impl PercentilePayloadFailure<'_> {
    pub fn into_failure(self, context: PercentileInputDiagnostic<'_>) -> crate::KernelFailure {
        crate::KernelFailure::from_domain(PercentileReaderContract {
            code: std::num::NonZeroU16::new(395).unwrap(),
            detail: novarocks_failure_contract::BoundedDetail::truncating_display(
                self.message(context),
            ),
        })
    }
}

#[cfg(test)]
mod source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::sync::Arc;
    #[test]
    fn percentile_input_physical_contract_keeps_no_field_or_payload_graph() {
        let field = Arc::new(arrow_schema::Field::new(
            "private-field",
            DataType::Int64,
            true,
        ));
        let weak = Arc::downgrade(&field);
        let array = Arc::new(StructArray::new(
            vec![field].into(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
            None,
        )) as ArrayRef;
        let cause = numeric_value_at_with_failure(&array, 0, &mut |failure| {
            failure.into_failure(PercentileInputDiagnostic::Hash)
        })
        .unwrap_err();
        assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(cause.failure_fact().reason().code(), 394);
        drop(array);
        assert!(weak.upgrade().is_none());
        let array = Arc::new(Int32Array::from(vec![7])) as ArrayRef;
        let weak = Arc::downgrade(&array);
        let cause = payload_bytes_at_with_failure(&array, 0, &mut |failure| {
            failure.into_failure(PercentileInputDiagnostic::ApproxRaw)
        })
        .unwrap_err();
        assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(cause.failure_fact().reason().code(), 395);
        drop(array);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn percentile_input_largeint_forwards_actual_cause_and_slice_source() {
        let original = largeint::i128_from_be_bytes(&[1, 2, 3]).unwrap_err();
        let expected = original.failure_fact();
        let cause = PercentileNumericFailure::LargeInt(original)
            .into_failure(PercentileInputDiagnostic::Hash);
        assert_eq!(cause.failure_fact(), expected);
        let mut source: &(dyn std::error::Error + 'static) = &cause;
        while !source.is::<std::array::TryFromSliceError>() {
            source = source
                .source()
                .expect("actual slice conversion source retained");
        }
    }
    #[test]
    fn percentile_input_supported_numeric_null_and_f32_rules_are_unchanged() {
        let array =
            Arc::new(Float64Array::from(vec![None, Some(-0.0), Some(f64::NAN)])) as ArrayRef;
        let read = |row| {
            numeric_value_at_with_failure(&array, row, &mut |failure| {
                failure.into_failure(PercentileInputDiagnostic::Hash)
            })
        };
        assert_eq!(read(0).unwrap(), None);
        assert_eq!(read(1).unwrap().unwrap().to_bits(), (-0.0f64).to_bits());
        assert!(read(2).unwrap().unwrap().is_nan());
    }
}
