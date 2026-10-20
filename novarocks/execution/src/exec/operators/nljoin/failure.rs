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
//! Nested-loop join owns its build/probe contracts and actual batch filters.
//! Expression and construction causes are forwarded without rendering them.
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::fragment::ExecutionReason;
use arrow::error::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
enum NlJoinFailure {
    Contract(BoundedDetail),
    Filter(ArrowError),
}
pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(NlJoinFailure::Contract(BoundedDetail::truncating_display(
        detail,
    )))
}
pub(super) fn filter(source: ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(NlJoinFailure::Filter(source))
}
impl fmt::Display for NlJoinFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::Filter(source) => write!(f, "nljoin batch filter failed: {source}"),
        }
    }
}
impl Error for NlJoinFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Filter(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for NlJoinFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, ExecutionReason::NlJoinContract),
            Self::Filter(ArrowError::MemoryError(_)) => (
                FailureClass::Resource,
                ExecutionReason::NlJoinFilterAllocation,
            ),
            Self::Filter(ArrowError::OffsetOverflowError(_)) => (
                FailureClass::Representation,
                ExecutionReason::NlJoinFilterOffsets,
            ),
            Self::Filter(ArrowError::IoError(..)) => {
                (FailureClass::Unexpected, ExecutionReason::NlJoinFilterIo)
            }
            // Filtering admitted arrays cannot turn their values into SQL errors.
            Self::Filter(_) => (
                FailureClass::Invariant,
                ExecutionReason::NlJoinFilterContract,
            ),
        };
        let fact = FailureFact::new(class, code.reason());
        if class == FailureClass::Representation {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                // Arrow reports offset refusal without identifying the batch's
                // column or concrete width. Do not guess a byte/entry limit.
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(33).unwrap(),
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
