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
//! The original recursive BY codec and winner transition, generic over host storage.
//! BY state encoding owns its byte validity and fixed-width format facts.
use crate::KernelFailure;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, RepresentationBoundary, RepresentationFacts, RepresentationLayout,
    domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Clone, Copy, Debug)]
pub(super) enum ValueLength {
    Utf8,
    Struct,
    Map,
    List,
    DecimalText,
    Binary,
}
impl ValueLength {
    fn code(self) -> u16 {
        match self {
            Self::Utf8 => 68,
            Self::Struct => 69,
            Self::Map => 70,
            Self::List => 71,
            Self::DecimalText => 72,
            Self::Binary => 73,
        }
    }
}
#[derive(Debug)]
enum ByStateFailure {
    Malformed(BoundedDetail),
    Utf8(std::str::Utf8Error),
    Format {
        kind: ValueLength,
        required: usize,
        source: std::num::TryFromIntError,
    },
}
impl fmt::Display for ByStateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(detail) => detail.fmt(f),
            Self::Utf8(source) => source.fmt(f),
            Self::Format { kind, required, .. } => write!(
                f,
                "max_by/min_by {kind:?} length {required} exceeds u32 state encoding"
            ),
        }
    }
}
impl Error for ByStateFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Utf8(source) => Some(source),
            Self::Format { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for ByStateFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Malformed(_) | Self::Utf8(_) => {
                crate::DataFailureReason::InvalidEncoding.failure_fact()
            }
            Self::Format { kind, required, .. } => FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(330).unwrap()),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
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
impl OuterFailureProjection for ByStateFailure {}
pub(super) fn malformed(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(ByStateFailure::Malformed(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn utf8(source: std::str::Utf8Error) -> KernelFailure {
    KernelFailure::from_domain(ByStateFailure::Utf8(source))
}
pub(super) fn encoded_len(kind: ValueLength, required: usize) -> Result<u32, KernelFailure> {
    u32::try_from(required).map_err(|source| {
        KernelFailure::from_domain(ByStateFailure::Format {
            kind,
            required,
            source,
        })
    })
}
#[derive(Debug)]
struct DecimalParseFailure<E>(E);
impl<E: Error> fmt::Display for DecimalParseFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("max_by/min_by decimal256 decode failed")
    }
}
impl<E: Error + 'static> Error for DecimalParseFailure<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl<E> FailureProjection for DecimalParseFailure<E> {
    fn failure_fact(&self) -> FailureFact {
        crate::DataFailureReason::InvalidEncoding.failure_fact()
    }
}
impl<E: Error + Send + Sync + 'static> OuterFailureProjection for DecimalParseFailure<E> {}
pub(super) fn decimal_parse<E: Error + Send + Sync + 'static>(source: E) -> KernelFailure {
    KernelFailure::from_domain(DecimalParseFailure(source))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn state_length_words_keep_real_conversion_sources_and_distinct_layouts() {
        for kind in [
            ValueLength::Utf8,
            ValueLength::Struct,
            ValueLength::Map,
            ValueLength::List,
            ValueLength::DecimalText,
            ValueLength::Binary,
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
            let representation = fact.representation().unwrap();
            assert_eq!(representation.layout.code.get(), kind.code());
            assert_eq!(representation.required, Some(required as u64));
            assert_eq!(representation.extent_limit, Some(u64::from(u32::MAX)));
            assert_eq!(representation.representable_prefix, None);
            let mut source = cause.source();
            let mut real_conversion = false;
            while let Some(actual) = source {
                real_conversion |= actual.is::<std::num::TryFromIntError>();
                source = actual.source();
            }
            assert!(real_conversion);
        }
    }
}
