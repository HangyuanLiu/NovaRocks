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
//! TIME array formatting has no lawful row-error channel. Invalid duration
//! input uses the existing NULL rule. The separate duration-cast result keeps
//! actual temporal-value formatter Data distinct from these outer causes.
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
pub enum TimeTextError {
    Contract(BoundedDetail),
    Arrow(ArrowError),
    OffsetNarrow(ArrowError),
    Kernel(KernelFailure),
    Extent {
        required: u128,
        largest: u128,
        prefix: usize,
    },
    Backing {
        required: u128,
        source: std::alloc::LayoutError,
    },
}
impl From<KernelFailure> for TimeTextError {
    fn from(source: KernelFailure) -> Self {
        Self::Kernel(source)
    }
}
impl TimeTextError {
    pub fn contract(detail: impl fmt::Display) -> Self {
        Self::Contract(BoundedDetail::truncating_display(detail))
    }
    pub(super) fn backing<T>(count: usize, source: std::alloc::LayoutError) -> Self {
        Self::Backing {
            required: count as u128 * size_of::<T>() as u128,
            source,
        }
    }
    pub fn into_kernel(self) -> KernelFailure {
        match self {
            Self::Kernel(source) => source,
            source => KernelFailure::from_domain(source),
        }
    }
}
impl fmt::Display for TimeTextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::Arrow(source) | Self::OffsetNarrow(source) => {
                write!(f, "TIME source cast failed: {source}")
            }
            Self::Kernel(source) => source.fmt(f),
            Self::Extent { required, .. } => write!(
                f,
                "TIME UTF8 output requires {required} bytes beyond its offset limit"
            ),
            Self::Backing { required, source } => write!(
                f,
                "TIME output backing requires {required} bytes beyond the Rust allocation extent: {source}"
            ),
        }
    }
}
impl Error for TimeTextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow(source) | Self::OffsetNarrow(source) => Some(source),
            Self::Kernel(source) => Some(source),
            Self::Backing { source, .. } => Some(source),
            _ => None,
        }
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for TimeTextError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Kernel(source) => source.failure_fact(),
            Self::Contract(_) => fact(FailureClass::Invariant, 279),
            Self::Backing { required, .. } => {
                crate::allocation_failure::rust_array_backing_failure_fact(
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(285).unwrap()),
                    *required,
                    RepresentationBoundary::SingleOutput,
                )
            }
            Self::Extent {
                required,
                largest,
                prefix,
            } => fact(FailureClass::Representation, 284).with_representation(RepresentationFacts {
                boundary: if *largest > i32::MAX as u128 {
                    RepresentationBoundary::MinimumUnit
                } else {
                    RepresentationBoundary::SingleOutput
                },
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: NonZeroU16::new(57).unwrap(),
                },
                required: u64::try_from(if *largest > i32::MAX as u128 {
                    *largest
                } else {
                    *required
                })
                .ok(),
                extent_limit: Some(i32::MAX as u64),
                representable_prefix: u64::try_from(*prefix).ok(),
            }),
            Self::OffsetNarrow(_) => {
                fact(FailureClass::Representation, 281).with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(57).unwrap(),
                    },
                    required: None,
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: None,
                })
            }
            Self::Arrow(source) => match source {
                ArrowError::MemoryError(_) => fact(FailureClass::Resource, 280),
                ArrowError::OffsetOverflowError(_) => fact(FailureClass::Representation, 281)
                    .with_representation(RepresentationFacts {
                        boundary: RepresentationBoundary::SingleOutput,
                        layout: RepresentationLayout {
                            domain: domain::KERNEL,
                            code: NonZeroU16::new(57).unwrap(),
                        },
                        required: None,
                        extent_limit: None,
                        representable_prefix: None,
                    }),
                ArrowError::IoError(..) => fact(FailureClass::Unexpected, 282),
                _ => fact(FailureClass::Invariant, 283),
            },
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for TimeTextError {}

/// Only the concrete temporal-value formatter's CastError enters this channel.
/// Unsupported frozen types and allocation/format limits remain outer errors.
#[derive(Debug)]
pub struct TimeTextDataFailure {
    source: ArrowError,
    detail: BoundedDetail,
}
impl TimeTextDataFailure {
    pub(super) fn new(source: ArrowError) -> Self {
        let detail = BoundedDetail::truncating_display(&source);
        Self { source, detail }
    }
    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
    pub fn row_error(&self, ordinal: usize) -> crate::RowDataError {
        crate::RowDataError::with_reason(
            ordinal,
            crate::DataFailureReason::InvalidValue,
            self.detail(),
        )
    }
}
impl fmt::Display for TimeTextDataFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail())
    }
}
impl Error for TimeTextDataFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for TimeTextDataFailure {
    fn failure_fact(&self) -> FailureFact {
        crate::DataFailureReason::InvalidValue.failure_fact()
    }
}
#[derive(Debug)]
pub enum DurationCastError {
    Outer(TimeTextError),
    Data(TimeTextDataFailure),
}
impl From<KernelFailure> for DurationCastError {
    fn from(source: KernelFailure) -> Self {
        Self::Outer(TimeTextError::Kernel(source))
    }
}
impl From<TimeTextError> for DurationCastError {
    fn from(source: TimeTextError) -> Self {
        Self::Outer(source)
    }
}
impl fmt::Display for DurationCastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Outer(source) => source.fmt(f),
            Self::Data(source) => source.fmt(f),
        }
    }
}
impl Error for DurationCastError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Outer(source) => Some(source),
            Self::Data(source) => Some(source),
        }
    }
}
impl FailureProjection for DurationCastError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Outer(source) => source.failure_fact(),
            Self::Data(source) => source.failure_fact(),
        }
    }
}
