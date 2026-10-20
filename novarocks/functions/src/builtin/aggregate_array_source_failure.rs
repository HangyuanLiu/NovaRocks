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
//! ARRAY aggregate owns frozen shape, actual construction and whole-value facts.
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, domain,
};
use std::{
    fmt,
    num::{NonZeroU16, TryFromIntError},
};
#[derive(Debug)]
enum ArrayAggregateFailure {
    Contract(BoundedDetail),
    ShapeMismatch(BoundedDetail),
    Constructor(ArrowError),
    Narrow {
        destination: &'static str,
        source: TryFromIntError,
    },
}
impl fmt::Display for ArrayAggregateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) | Self::ShapeMismatch(detail) => detail.fmt(f),
            Self::Constructor(source) => source.fmt(f),
            Self::Narrow { destination, .. } => write!(f, "{destination} overflow"),
        }
    }
}
impl std::error::Error for ArrayAggregateFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(_) | Self::ShapeMismatch(_) => None,
            Self::Constructor(source) => Some(source),
            Self::Narrow { source, .. } => Some(source),
        }
    }
}
impl FailureProjection for ArrayAggregateFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Contract(_) | Self::ShapeMismatch(_) => (FailureClass::Invariant, 340),
            Self::Constructor(ArrowError::MemoryError(_)) => (FailureClass::Resource, 342),
            Self::Constructor(_) => (FailureClass::Invariant, 341),
            Self::Narrow { .. } => {
                return crate::DataFailureReason::NumericOutOfRange.failure_fact();
            }
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
impl OuterFailureProjection for ArrayAggregateFailure {}
pub(super) fn contract(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(ArrayAggregateFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn constructor(source: ArrowError) -> KernelFailure {
    KernelFailure::from_domain(ArrayAggregateFailure::Constructor(source))
}
pub(super) fn narrow(destination: &'static str, source: TryFromIntError) -> KernelFailure {
    KernelFailure::from_domain(ArrayAggregateFailure::Narrow {
        destination,
        source,
    })
}

pub(super) fn shape_mismatch(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(ArrayAggregateFailure::ShapeMismatch(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn is_shape_mismatch(error: &KernelFailure) -> bool {
    use std::error::Error;
    matches!(error, KernelFailure::Source(cause)
        if cause.source().and_then(|source| source.downcast_ref::<ArrayAggregateFailure>())
            .is_some_and(|source| matches!(source, ArrayAggregateFailure::ShapeMismatch(_))))
}
