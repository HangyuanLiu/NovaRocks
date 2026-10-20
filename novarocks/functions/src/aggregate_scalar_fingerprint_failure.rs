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

//! Fingerprint format words and the actual contiguous Rust byte-vector layout.
use crate::KernelFailure;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, OuterFailureProjection,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, ResourceDimension,
    ResourceFacts, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Clone, Copy, Debug)]
pub(super) enum LengthKind {
    Bytes,
    Children,
    Entries,
}
impl LengthKind {
    fn layout(self) -> u16 {
        match self {
            Self::Bytes => 65,
            Self::Children => 66,
            Self::Entries => 67,
        }
    }
}
#[derive(Debug)]
enum FingerprintExtentFailure {
    Format {
        kind: LengthKind,
        len: usize,
        source: std::num::TryFromIntError,
    },
    Backing {
        current: usize,
        additional: usize,
    },
}
impl fmt::Display for FingerprintExtentFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format { kind, len, .. } => write!(
                f,
                "aggregate scalar fingerprint {kind:?} extent {len} exceeds u32 length"
            ),
            Self::Backing {
                current,
                additional,
            } => write!(
                f,
                "aggregate scalar fingerprint byte extent {} exceeds Rust contiguous allocation layout",
                *current as u128 + *additional as u128
            ),
        }
    }
}
impl std::error::Error for FingerprintExtentFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Format { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for FingerprintExtentFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Format { kind, len, .. } => FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(325).unwrap()),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: NonZeroU16::new(kind.layout()).unwrap(),
                },
                required: u64::try_from(*len).ok(),
                extent_limit: Some(u32::MAX as u64),
                representable_prefix: None,
            }),
            Self::Backing {
                current,
                additional,
            } => crate::allocation_failure::rust_array_backing_failure_fact(
                FailureReason::new(domain::KERNEL, NonZeroU16::new(326).unwrap()),
                *current as u128 + *additional as u128,
                RepresentationBoundary::MinimumUnit,
            ),
        }
    }
}
impl OuterFailureProjection for FingerprintExtentFailure {}
pub(super) fn checked_encoded_len_add(
    total: &mut usize,
    additional: usize,
) -> Result<(), KernelFailure> {
    let next = total
        .checked_add(additional)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or_else(|| {
            KernelFailure::from_domain(FingerprintExtentFailure::Backing {
                current: *total,
                additional,
            })
        })?;
    *total = next;
    Ok(())
}
pub(super) fn checked_u32_len(kind: LengthKind, len: usize) -> Result<u32, KernelFailure> {
    u32::try_from(len).map_err(|source| {
        KernelFailure::from_domain(FingerprintExtentFailure::Format { kind, len, source })
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn fingerprint_length_words_are_format_boundaries_with_actual_conversion_source() {
        for kind in [LengthKind::Bytes, LengthKind::Children, LengthKind::Entries] {
            let Ok(limit) = usize::try_from(u32::MAX) else {
                continue;
            };
            assert_eq!(checked_u32_len(kind, limit).unwrap(), u32::MAX);
            let Some(required) = limit.checked_add(1) else {
                continue;
            };
            let error = checked_u32_len(kind, required).unwrap_err();
            let fact = error.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            let facts = fact.representation().unwrap();
            assert_eq!(facts.required, Some(required as u64));
            assert_eq!(facts.extent_limit, Some(u32::MAX as u64));
            assert_eq!(facts.boundary, RepresentationBoundary::MinimumUnit);
            assert_eq!(facts.representable_prefix, None);
            let mut source = error.source();
            let mut found = false;
            while let Some(actual) = source {
                found |= actual.is::<std::num::TryFromIntError>();
                source = actual.source();
            }
            assert!(found);
        }
    }
    #[test]
    fn fingerprint_byte_layout_failure_does_not_advance_the_original_extent_or_invent_quota() {
        let mut total = isize::MAX as usize;
        checked_encoded_len_add(&mut total, 0).unwrap();
        let error = checked_encoded_len_add(&mut total, 1).unwrap_err();
        assert_eq!(total, isize::MAX as usize);
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let facts = fact.representation().unwrap();
        assert_eq!(facts.required, Some(isize::MAX as u64 + 1));
        assert_eq!(facts.extent_limit, Some(isize::MAX as u64));
        assert_eq!(facts.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(facts.representable_prefix, None);
        assert!(fact.resource().is_none());
        let mut total = usize::MAX;
        let error = checked_encoded_len_add(&mut total, 1).unwrap_err();
        assert_eq!(total, usize::MAX);
        assert_eq!(error.failure_fact().class(), FailureClass::Representation);
    }
}

#[cfg(test)]
mod comparison_source_properties {
    use super::*;
    use crate::aggregate_scalar::{AggScalarValue, ScalarOrdering, ScalarWork};
    use crate::aggregate_scalar_fingerprint::{
        compare_scalar_values, compare_scalar_values_partial,
    };
    #[test]
    fn nested_owned_comparison_keeps_unordered_value_and_kind_policy_separate_from_failure() {
        let left = AggScalarValue::List(vec![Some(AggScalarValue::Float64(f64::NAN))]);
        let right = AggScalarValue::List(vec![Some(AggScalarValue::Float64(1.0))]);
        assert_eq!(
            compare_scalar_values_partial(&left, &right, &mut ScalarWork::new(None)).unwrap(),
            ScalarOrdering::UnorderedFloat
        );
        assert_eq!(
            compare_scalar_values(&left, &right, &mut ScalarWork::new(None))
                .unwrap_err()
                .failure_fact(),
            crate::DataFailureReason::InvalidValue.failure_fact()
        );
        let right = AggScalarValue::Int64(1);
        assert_eq!(
            compare_scalar_values_partial(&left, &right, &mut ScalarWork::new(None)).unwrap(),
            ScalarOrdering::MismatchedKinds
        );
        assert_eq!(
            compare_scalar_values(&left, &right, &mut ScalarWork::new(None))
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
    #[test]
    fn actual_owned_text_comparison_forwards_refusal_before_it_can_be_used_as_ordering() {
        struct Refuse(crate::KernelFailure);
        impl crate::KernelEvaluationControl for Refuse {
            fn checkpoint(&self, _: u32) -> Result<(), crate::KernelFailure> {
                Err(self.0.clone())
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), crate::KernelFailure> {
                panic!("no wait")
            }
        }
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = Refuse(cause.clone());
            let mut work = crate::kernel_input::EvaluationCheckpoints::new(&control);
            let failure = compare_scalar_values_partial(
                &AggScalarValue::Utf8("a".into()),
                &AggScalarValue::Utf8("b".into()),
                &mut ScalarWork::new(Some(&mut work)),
            )
            .unwrap_err();
            assert_eq!(failure, cause);
        }
    }
}
