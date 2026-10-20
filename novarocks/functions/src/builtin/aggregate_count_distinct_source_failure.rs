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
//! COUNT DISTINCT owns its set encoding and carrier facts; no text classifier.
use crate::KernelFailure;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, RepresentationBoundary, RepresentationFacts, RepresentationLayout,
    domain,
};
use std::{
    fmt,
    num::{NonZeroU16, TryFromIntError},
};
#[derive(Clone, Copy, Debug)]
pub(super) enum Length {
    Bytes,
    DecimalText,
    Children,
    SetCount,
    KeyBytes,
}
impl Length {
    fn code(self) -> u16 {
        match self {
            Self::Bytes => 79,
            Self::DecimalText => 80,
            Self::Children => 81,
            Self::SetCount => 82,
            Self::KeyBytes => 83,
        }
    }
}
#[derive(Debug)]
enum CountDistinctFailure {
    Contract(BoundedDetail),
    Malformed,
    Cardinality {
        required: usize,
        source: TryFromIntError,
    },
    Format {
        kind: Length,
        required: usize,
        source: TryFromIntError,
    },
}
impl fmt::Display for CountDistinctFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::Malformed => f.write_str("invalid distinct set encoding"),
            Self::Cardinality { required, .. } => {
                write!(f, "COUNT DISTINCT cardinality {required} exceeds Int64")
            }
            Self::Format { kind, required, .. } => write!(
                f,
                "COUNT DISTINCT {kind:?} length {required} exceeds u32 state encoding"
            ),
        }
    }
}
impl std::error::Error for CountDistinctFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cardinality { source, .. } | Self::Format { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for CountDistinctFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Contract(_) => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(339).unwrap()),
            ),
            Self::Malformed => crate::DataFailureReason::InvalidEncoding.failure_fact(),
            Self::Cardinality { .. } => crate::DataFailureReason::NumericOutOfRange.failure_fact(),
            Self::Format { kind, required, .. } => FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(338).unwrap()),
            )
            .with_representation(RepresentationFacts {
                // A set serialization request may be split by its owner; the
                // codec cannot claim that the entire set is a minimum unit.
                boundary: if matches!(kind, Length::SetCount) {
                    RepresentationBoundary::SingleOutput
                } else {
                    RepresentationBoundary::MinimumUnit
                },
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: NonZeroU16::new(kind.code()).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(u64::from(u32::MAX)),
                representable_prefix: None,
            }),
        }
    }
}
impl OuterFailureProjection for CountDistinctFailure {}
pub(super) fn contract(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(CountDistinctFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn malformed() -> KernelFailure {
    KernelFailure::from_domain(CountDistinctFailure::Malformed)
}
pub(super) fn cardinality(required: usize) -> Result<i64, KernelFailure> {
    i64::try_from(required).map_err(|source| {
        KernelFailure::from_domain(CountDistinctFailure::Cardinality { required, source })
    })
}
pub(super) fn encoded_len(kind: Length, required: usize) -> Result<u32, KernelFailure> {
    u32::try_from(required).map_err(|source| {
        KernelFailure::from_domain(CountDistinctFailure::Format {
            kind,
            required,
            source,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn count_key_and_set_headers_keep_actual_format_extent_without_saturating_length_words() {
        for kind in [
            Length::Bytes,
            Length::DecimalText,
            Length::Children,
            Length::SetCount,
            Length::KeyBytes,
        ] {
            let Ok(limit) = usize::try_from(u32::MAX) else {
                continue;
            };
            assert_eq!(encoded_len(kind, limit).unwrap(), u32::MAX);
            let Some(required) = limit.checked_add(1) else {
                continue;
            };
            let cause = encoded_len(kind, required).unwrap_err();
            let fact = cause.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            let facts = fact.representation().unwrap();
            assert_eq!(facts.layout.code.get(), kind.code());
            assert_eq!(facts.required, Some(required as u64));
            assert_eq!(facts.extent_limit, Some(u64::from(u32::MAX)));
            assert_eq!(
                facts.boundary,
                if matches!(kind, Length::SetCount) {
                    RepresentationBoundary::SingleOutput
                } else {
                    RepresentationBoundary::MinimumUnit
                }
            );
            assert_eq!(facts.representable_prefix, None);
            let mut source = cause.source();
            let mut real_conversion = false;
            while let Some(actual) = source {
                real_conversion |= actual.is::<TryFromIntError>();
                source = actual.source();
            }
            assert!(real_conversion);
        }
    }
    #[test]
    fn a_rejected_count_source_schema_keeps_no_field_owner() {
        let field = std::sync::Arc::new(arrow_schema::Field::new(
            "value resource numeric".repeat(500),
            arrow_schema::DataType::Int64,
            true,
        ));
        let weak = std::sync::Arc::downgrade(&field);
        let ty = arrow_schema::DataType::FixedSizeList(field.clone(), 1);
        let error = super::super::validate_source_type(&ty).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
        drop(ty);
        drop(field);
        assert!(weak.upgrade().is_none());
    }
}
