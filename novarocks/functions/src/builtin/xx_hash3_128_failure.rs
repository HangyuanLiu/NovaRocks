// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Original seed-zero XXH3 stream and byte-layout normalization shared by both callers.
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, ResourceDimension,
    ResourceFacts, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
pub(super) struct HashArrayFailure(Box<HashCause>);
#[derive(Debug)]
enum HashCause {
    Contract(BoundedDetail),
    Normalize(ArrowError),
    Gather(ArrowError),
    Output(ArrowError),
    Layout {
        requested: u128,
        source: std::alloc::LayoutError,
    },
}
impl HashArrayFailure {
    pub(super) fn contract(detail: impl fmt::Display) -> Self {
        Self(Box::new(HashCause::Contract(
            BoundedDetail::truncating_display(detail),
        )))
    }
    pub(super) fn normalize(source: ArrowError) -> Self {
        Self(Box::new(HashCause::Normalize(source)))
    }
    pub(super) fn gather(source: ArrowError) -> Self {
        Self(Box::new(HashCause::Gather(source)))
    }
    pub(super) fn output(source: ArrowError) -> Self {
        Self(Box::new(HashCause::Output(source)))
    }
    pub(super) fn layout<T>(count: usize, source: std::alloc::LayoutError) -> Self {
        Self(Box::new(HashCause::Layout {
            requested: count as u128 * size_of::<T>() as u128,
            source,
        }))
    }
    pub(super) fn into_kernel(self) -> KernelFailure {
        KernelFailure::from_domain(self)
    }
}
impl fmt::Display for HashArrayFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            HashCause::Contract(detail) => f.write_str(detail.as_str()),
            HashCause::Normalize(source) => {
                write!(f, "xx_hash3_128: byte normalization failed: {source}")
            }
            HashCause::Gather(source) => {
                write!(f, "xx_hash3_128: selected byte projection failed: {source}")
            }
            HashCause::Output(source) => write!(f, "xx_hash3_128: failed to cast output: {source}"),
            HashCause::Layout { requested, source } => write!(
                f,
                "xx_hash3_128: {requested} byte backing request is not addressable: {source}"
            ),
        }
    }
}
impl Error for HashArrayFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.0.as_ref() {
            HashCause::Normalize(source)
            | HashCause::Gather(source)
            | HashCause::Output(source) => Some(source),
            HashCause::Layout { source, .. } => Some(source),
            HashCause::Contract(_) => None,
        }
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for HashArrayFailure {
    fn failure_fact(&self) -> FailureFact {
        let source = match self.0.as_ref() {
            HashCause::Contract(_) => return fact(FailureClass::Invariant, 272),
            HashCause::Layout { requested, .. } => {
                return crate::allocation_failure::rust_array_backing_failure_fact(
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(267).unwrap()),
                    *requested,
                    RepresentationBoundary::SingleOutput,
                );
            }
            HashCause::Normalize(source)
            | HashCause::Gather(source)
            | HashCause::Output(source) => source,
        };
        match source {
            ArrowError::MemoryError(_) => fact(FailureClass::Resource, 268),
            ArrowError::ArithmeticOverflow(_)
                if matches!(self.0.as_ref(), HashCause::Gather(_)) =>
            {
                fact(FailureClass::Resource, 268)
            }
            // Pinned Arrow's exact LargeUtf8->Utf8 / LargeBinary->Binary cast
            // has one ComputeError author: an offset cannot fit i32. This is
            // known from this call, never inferred from the diagnostic words.
            ArrowError::ComputeError(_) if matches!(self.0.as_ref(), HashCause::Normalize(_)) => {
                representation()
            }
            ArrowError::OffsetOverflowError(_) => representation(),
            ArrowError::IoError(..) => fact(FailureClass::Unexpected, 270),
            _ => fact(FailureClass::Invariant, 271),
        }
    }
}
fn representation() -> FailureFact {
    fact(FailureClass::Representation, 269).with_representation(RepresentationFacts {
        boundary: RepresentationBoundary::SingleOutput,
        layout: RepresentationLayout {
            domain: domain::KERNEL,
            code: NonZeroU16::new(55).unwrap(),
        },
        required: None,
        extent_limit: None,
        representable_prefix: None,
    })
}
impl novarocks_failure_contract::OuterFailureProjection for HashArrayFailure {}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_role_preserves_exact_arrow_cause_without_classifying_words() {
        let source = HashArrayFailure::normalize(ArrowError::ComputeError(
            "cancelled numeric overflow".into(),
        ));
        assert_eq!(source.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            source.failure_fact().representation().unwrap().boundary,
            RepresentationBoundary::SingleOutput
        );
        assert!(source.source().unwrap().is::<ArrowError>());
        let same_words = HashArrayFailure::output(ArrowError::ComputeError(
            "cancelled numeric overflow".into(),
        ));
        assert_eq!(same_words.failure_fact().class(), FailureClass::Invariant);
        let source = HashArrayFailure::gather(ArrowError::MemoryError("invalid input".into()));
        assert_eq!(source.failure_fact().class(), FailureClass::Resource);
    }
    #[test]
    fn hash_actual_layout_refusal_has_exact_request_and_original_source() {
        let count = usize::MAX;
        let cause = HashArrayFailure::layout::<[u8; 16]>(
            count,
            std::alloc::Layout::array::<[u8; 16]>(count).unwrap_err(),
        );
        assert_eq!(cause.failure_fact().class(), FailureClass::Representation);
        let extent = cause.failure_fact().representation().unwrap();
        assert_eq!(extent.required, None);
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert!(cause.failure_fact().resource().is_none());
        assert!(cause.source().unwrap().is::<std::alloc::LayoutError>());
    }
}
