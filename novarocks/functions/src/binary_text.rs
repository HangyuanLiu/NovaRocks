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

//! The original Arrow safe Binary-to-Utf8 author shared by legacy and selected CAST.
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::DataType;

/// Exact original Arrow call; invalid UTF-8 remains a successful NULL.
/// Allocation, validation and copying inside Arrow are opaque work.
pub fn cast_array(array: &ArrayRef) -> Result<ArrayRef, BinaryTextFailure> {
    arrow_cast::cast(array.as_ref(), &DataType::Utf8).map_err(BinaryTextFailure::Arrow)
}

/// The original author on one already selected row, with no replacement decoder.
pub fn value_text(array: &ArrayRef, row: usize) -> Result<Option<String>, BinaryTextFailure> {
    let out = cast_array(&array.slice(row, 1))?;
    let text = out
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| BinaryTextFailure::Carrier)?;
    Ok(if text.is_null(0) {
        None
    } else {
        Some(text.value(0).to_string())
    })
}

#[derive(Debug)]
pub enum BinaryTextFailure {
    Arrow(arrow_schema::ArrowError),
    Carrier,
}
impl std::fmt::Display for BinaryTextFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arrow(cause) => cause.fmt(f),
            Self::Carrier => f.write_str("binary text author returned a foreign carrier"),
        }
    }
}
impl std::error::Error for BinaryTextFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for BinaryTextFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        // This author invokes safe Binary -> Utf8: malformed UTF8 is NULL.
        // An error cannot be relabeled as a user-data failure of that profile.
        let (class, code) = match self {
            Self::Arrow(ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_)) => {
                (FailureClass::Resource, 213)
            }
            Self::Arrow(ArrowError::OffsetOverflowError(_)) => (FailureClass::Representation, 214),
            Self::Arrow(ArrowError::IoError(..)) => (FailureClass::Unexpected, 215),
            _ => (FailureClass::Invariant, 216),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(self, Self::Arrow(ArrowError::OffsetOverflowError(_))) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(35).unwrap(),
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
impl novarocks_failure_contract::OuterFailureProjection for BinaryTextFailure {}
#[cfg(test)]
mod domain_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn safe_binary_text_preserves_invalid_null_and_actual_arrow_source() {
        use std::error::Error;
        let array: ArrayRef =
            std::sync::Arc::new(arrow_array::BinaryArray::from_vec(vec![&[255][..], b"ok"]));
        let weak = std::sync::Arc::downgrade(&array);
        assert_eq!(value_text(&array, 0).unwrap(), None);
        assert_eq!(value_text(&array, 1).unwrap(), Some("ok".into()));
        // The scalar API's profile is checked by its caller; exercise an actual
        // unsupported Arrow carrier separately without inventing a decoder.
        let cause = arrow_cast::cast(
            &arrow_array::BooleanArray::from(vec![true]),
            &DataType::Struct(
                vec![std::sync::Arc::new(arrow_schema::Field::new(
                    "x",
                    DataType::Int32,
                    false,
                ))]
                .into(),
            ),
        )
        .unwrap_err();
        let failure = BinaryTextFailure::Arrow(cause);
        assert!(
            failure
                .source()
                .unwrap()
                .downcast_ref::<arrow_schema::ArrowError>()
                .is_some()
        );
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        drop(array);
        assert!(weak.upgrade().is_none());
    }
}
