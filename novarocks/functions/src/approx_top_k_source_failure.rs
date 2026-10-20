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
//! ApproxTopK owns its original state codec and physical result layout facts.
use crate::KernelFailure;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, RepresentationBoundary, RepresentationFacts, RepresentationLayout,
    domain,
};
use std::{
    error::Error,
    fmt,
    num::{NonZeroU16, TryFromIntError},
};
#[derive(Clone, Copy, Debug)]
pub(super) enum Length {
    Utf8,
    DecimalText,
    Binary,
    Struct,
    Map,
    List,
    K,
    Counter,
    StateCount,
    ValueBytes,
}
impl Length {
    fn layout(self) -> u16 {
        match self {
            Self::Utf8 => 84,
            Self::DecimalText => 85,
            Self::Binary => 86,
            Self::Struct => 87,
            Self::Map => 88,
            Self::List => 89,
            Self::K => 90,
            Self::Counter => 91,
            Self::StateCount => 92,
            Self::ValueBytes => 93,
        }
    }
}
#[derive(Debug)]
enum TopKSourceFailure {
    Contract(BoundedDetail),
    Malformed(BoundedDetail),
    Utf8 {
        label: &'static str,
        source: std::str::Utf8Error,
    },
    Constructor(arrow_schema::ArrowError),
    Format {
        kind: Length,
        required: usize,
        source: TryFromIntError,
    },
}
impl fmt::Display for TopKSourceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) | Self::Malformed(detail) => detail.fmt(f),
            Self::Utf8 { label, source } => write!(f, "approx_top_k decode {label}: {source}"),
            Self::Constructor(source) => write!(f, "approx_top_k output construction: {source}"),
            Self::Format { kind, required, .. } => write!(
                f,
                "approx_top_k {kind:?} length {required} exceeds u32 state encoding"
            ),
        }
    }
}
impl Error for TopKSourceFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Utf8 { source, .. } => Some(source),
            Self::Constructor(source) => Some(source),
            Self::Format { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for TopKSourceFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 346),
            Self::Malformed(_) | Self::Utf8 { .. } => {
                return crate::DataFailureReason::InvalidEncoding.failure_fact();
            }
            Self::Constructor(arrow_schema::ArrowError::MemoryError(_)) => {
                (FailureClass::Resource, 348)
            }
            Self::Constructor(_) => (FailureClass::Invariant, 347),
            Self::Format { kind, required, .. } => {
                return FailureFact::new(
                    FailureClass::Representation,
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(349).unwrap()),
                )
                .with_representation(RepresentationFacts {
                    boundary: if matches!(kind, Length::StateCount) {
                        RepresentationBoundary::SingleOutput
                    } else {
                        RepresentationBoundary::MinimumUnit
                    },
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(kind.layout()).unwrap(),
                    },
                    required: u64::try_from(*required).ok(),
                    extent_limit: Some(u32::MAX as u64),
                    representable_prefix: None,
                });
            }
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
impl OuterFailureProjection for TopKSourceFailure {}
pub(super) fn contract(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(TopKSourceFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn malformed(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(TopKSourceFailure::Malformed(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn utf8(label: &'static str, source: std::str::Utf8Error) -> KernelFailure {
    KernelFailure::from_domain(TopKSourceFailure::Utf8 { label, source })
}
pub(super) fn constructor(source: arrow_schema::ArrowError) -> KernelFailure {
    KernelFailure::from_domain(TopKSourceFailure::Constructor(source))
}
pub(super) fn length(kind: Length, required: usize) -> Result<u32, KernelFailure> {
    u32::try_from(required).map_err(|source| {
        KernelFailure::from_domain(TopKSourceFailure::Format {
            kind,
            required,
            source,
        })
    })
}
type DecimalParseSource = <arrow_buffer::i256 as std::str::FromStr>::Err;
#[derive(Debug)]
struct DecimalParse(DecimalParseSource);
impl fmt::Display for DecimalParse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "approx_top_k decode decimal256: {}", self.0)
    }
}
impl Error for DecimalParse {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for DecimalParse {
    fn failure_fact(&self) -> FailureFact {
        crate::DataFailureReason::InvalidEncoding.failure_fact()
    }
}
impl OuterFailureProjection for DecimalParse {}
pub(super) fn decimal(source: DecimalParseSource) -> KernelFailure {
    KernelFailure::from_domain(DecimalParse(source))
}

#[cfg(test)]
mod properties {
    use super::*;
    #[test]
    fn actual_topk_format_narrowing_keeps_each_extent_and_integer_source() {
        for kind in [
            Length::Utf8,
            Length::DecimalText,
            Length::Binary,
            Length::Struct,
            Length::Map,
            Length::List,
            Length::K,
            Length::Counter,
            Length::StateCount,
            Length::ValueBytes,
        ] {
            assert_eq!(length(kind, u32::MAX as usize).unwrap(), u32::MAX);
            if let Some(required) = (u32::MAX as usize).checked_add(1) {
                let failure = length(kind, required).unwrap_err();
                let facts = failure.failure_fact().representation().unwrap();
                assert_eq!(facts.required, Some(required as u64));
                assert_eq!(facts.extent_limit, Some(u32::MAX as u64));
                assert_eq!(facts.layout.code.get(), kind.layout());
                assert_eq!(facts.representable_prefix, None);
                assert_eq!(
                    facts.boundary,
                    if matches!(kind, Length::StateCount) {
                        RepresentationBoundary::SingleOutput
                    } else {
                        RepresentationBoundary::MinimumUnit
                    }
                );
                let KernelFailure::Source(source) = failure else {
                    panic!("real conversion source required")
                };
                assert!(
                    source
                        .source()
                        .unwrap()
                        .source()
                        .unwrap()
                        .is::<TryFromIntError>()
                );
            }
        }
    }
}
