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
use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{ArrayRef, FixedSizeBinaryArray};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, domain,
};
use std::sync::Arc;
#[derive(Clone, Debug)]
pub enum LargeIntCarrierFailure {
    ExpectedArray(BoundedDetail),
    ArrayWidth {
        context: BoundedDetail,
        actual: i32,
    },
    ValueWidth {
        actual: usize,
        source: std::array::TryFromSliceError,
    },
}
impl std::fmt::Display for LargeIntCarrierFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpectedArray(context) => {
                write!(f, "{}: expected FixedSizeBinaryArray", context.as_str())
            }
            Self::ArrayWidth { context, actual } => write!(
                f,
                "{}: expected FixedSizeBinary({}), got FixedSizeBinary({actual})",
                context.as_str(),
                LARGEINT_BYTE_WIDTH
            ),
            Self::ValueWidth { actual, .. } => write!(
                f,
                "invalid LARGEINT byte length: expected {}, got {actual}",
                LARGEINT_BYTE_WIDTH
            ),
        }
    }
}
impl std::error::Error for LargeIntCarrierFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ValueWidth { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for LargeIntCarrierFailure {
    fn failure_fact(&self) -> FailureFact {
        let code = match self {
            Self::ExpectedArray(_) => 292,
            Self::ArrayWidth { .. } => 293,
            Self::ValueWidth { .. } => 294,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl OuterFailureProjection for LargeIntCarrierFailure {}
impl From<LargeIntCarrierFailure> for crate::KernelFailure {
    fn from(source: LargeIntCarrierFailure) -> Self {
        Self::from_domain(source)
    }
}

pub use novarocks_type_contract::{LARGEINT_BYTE_WIDTH, is_largeint_data_type};

pub fn i128_to_be_bytes(value: i128) -> [u8; 16] {
    value.to_be_bytes()
}

pub fn i128_from_be_bytes(bytes: &[u8]) -> Result<i128, LargeIntCarrierFailure> {
    let bytes: [u8; 16] =
        bytes
            .try_into()
            .map_err(|source| LargeIntCarrierFailure::ValueWidth {
                actual: bytes.len(),
                source,
            })?;
    Ok(i128::from_be_bytes(bytes))
}

#[derive(Clone, Copy, Debug)]
pub enum LargeIntObservation {
    Step,
    OpaqueBoundary,
}
pub fn array_from_i128(values: &[Option<i128>]) -> Result<ArrayRef, LargeIntCarrierFailure> {
    match array_from_i128_observed(values, &mut |_| Ok::<(), std::convert::Infallible>(())) {
        Ok(result) => Ok(result),
        Err(never) => match never {},
    }
}
/// ONE original builder body; raw callers use an infallible no-op observer,
/// selected callers preserve their exact typed refusal cause.
pub fn array_from_i128_observed<E>(
    values: &[Option<i128>],
    observe: &mut dyn FnMut(LargeIntObservation) -> Result<(), E>,
) -> Result<ArrayRef, E> {
    const {
        assert!(LARGEINT_BYTE_WIDTH as usize == std::mem::size_of::<i128>());
    }
    observe(LargeIntObservation::OpaqueBoundary)?;
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), LARGEINT_BYTE_WIDTH);
    observe(LargeIntObservation::OpaqueBoundary)?;
    for value in values {
        observe(LargeIntObservation::Step)?;
        match value {
            Some(v) => {
                // Both the builder width and the serialized value width are
                // fixed by the i128 carrier contract, independent of row data.
                builder
                    .append_value(i128_to_be_bytes(*v))
                    .expect("i128 serialization has the builder's exact fixed width");
            }
            None => builder.append_null(),
        }
    }
    observe(LargeIntObservation::OpaqueBoundary)?;
    let out = Arc::new(builder.finish()) as ArrayRef;
    observe(LargeIntObservation::OpaqueBoundary)?;
    Ok(out)
}

pub fn as_fixed_size_binary_array<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<&'a FixedSizeBinaryArray, LargeIntCarrierFailure> {
    let arr = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| LargeIntCarrierFailure::ExpectedArray(BoundedDetail::truncating(context)))?;
    if arr.value_length() != LARGEINT_BYTE_WIDTH {
        return Err(LargeIntCarrierFailure::ArrayWidth {
            context: BoundedDetail::truncating(context),
            actual: arr.value_length(),
        });
    }
    Ok(arr)
}

pub fn value_at(arr: &FixedSizeBinaryArray, row: usize) -> Result<i128, LargeIntCarrierFailure> {
    i128_from_be_bytes(arr.value(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn largeint_carrier_keeps_actual_width_cause_and_all_i128_values() {
        for value in [i128::MIN, -1, 0, 1, i128::MAX] {
            assert_eq!(i128_from_be_bytes(&value.to_be_bytes()).unwrap(), value);
        }
        for size in [0, 15, 17] {
            let error = i128_from_be_bytes(&vec![0; size]).unwrap_err();
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
            assert!(
                error
                    .source()
                    .unwrap()
                    .is::<std::array::TryFromSliceError>()
            );
            let LargeIntCarrierFailure::ValueWidth { actual, .. } = error else {
                panic!("actual width source")
            };
            assert_eq!(actual, size);
        }
        let values = array_from_i128(&[Some(i128::MIN), None, Some(i128::MAX)]).unwrap();
        let values = as_fixed_size_binary_array(&values, "carrier property").unwrap();
        assert_eq!(value_at(values, 0).unwrap(), i128::MIN);
        assert_eq!(value_at(values, 2).unwrap(), i128::MAX);
    }
    #[test]
    fn largeint_scalar_reader_contract_cannot_become_value_data_or_pin_schema_fields() {
        use crate::aggregate_scalar::ScalarReadFailure;
        use arrow_schema::{DataType, Field};
        let field = Arc::new(Field::new(
            "large metadata".repeat(100_000),
            DataType::Int64,
            false,
        ));
        let weak = Arc::downgrade(&field);
        let ty = DataType::Struct(vec![field].into());
        let failure = ScalarReadFailure::Unsupported(&ty).into_kernel();
        drop(ty);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert!(failure.to_string().len() <= 512);
        let wrong = i128_from_be_bytes(&[0; 15]).unwrap_err();
        let fact = wrong.failure_fact();
        let forwarded = ScalarReadFailure::Existing(&wrong).into_kernel();
        assert_eq!(forwarded.failure_fact(), fact);
    }
}
