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

//! Set-operation state and output-gather facts; callees keep their own causes.
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::fragment::ExecutionReason;
use arrow::error::ArrowError;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, RepresentationBoundary, RepresentationFacts,
    RepresentationLayout, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
pub(super) enum SetStateFailure {
    ZeroColumns {
        node: &'static str,
    },
    MissingRow {
        build: bool,
        row: usize,
    },
    GroupSequence {
        node: &'static str,
        expected: usize,
        actual: usize,
    },
    GroupBounds {
        node: &'static str,
        group: usize,
        markers: usize,
    },
    Gather(ArrowError),
}
impl fmt::Display for SetStateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroColumns { node } => write!(f, "{node} with zero columns is unsupported"),
            Self::MissingRow { build, row } => write!(
                f,
                "distinct set serialized {} row {row} missing",
                if *build { "build" } else { "probe" }
            ),
            Self::GroupSequence {
                node,
                expected,
                actual,
            } => write!(
                f,
                "{node} group id sequence mismatch: expected {expected}, got {actual}"
            ),
            Self::GroupBounds {
                node,
                group,
                markers,
            } => write!(f, "{node} group id {group} is outside {markers} markers"),
            Self::Gather(source) => write!(f, "distinct set Arrow take failed: {source}"),
        }
    }
}
impl std::error::Error for SetStateFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Gather(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for SetStateFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::ZeroColumns { .. } => (
                FailureClass::Invariant,
                ExecutionReason::DistinctSetZeroColumns,
            ),
            Self::MissingRow { .. } => (
                FailureClass::Invariant,
                ExecutionReason::DistinctSetMissingRow,
            ),
            Self::GroupSequence { .. } => (
                FailureClass::Invariant,
                ExecutionReason::DistinctSetGroupSequence,
            ),
            Self::GroupBounds { .. } => (
                FailureClass::Invariant,
                ExecutionReason::DistinctSetGroupBounds,
            ),
            Self::Gather(ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_)) => (
                FailureClass::Resource,
                ExecutionReason::DistinctSetGatherAllocation,
            ),
            Self::Gather(ArrowError::OffsetOverflowError(_)) => (
                FailureClass::Representation,
                ExecutionReason::DistinctSetGatherOffsets,
            ),
            Self::Gather(ArrowError::IoError(..)) => (
                FailureClass::Unexpected,
                ExecutionReason::DistinctSetGatherIo,
            ),
            Self::Gather(_) => (
                FailureClass::Invariant,
                ExecutionReason::DistinctSetGatherContract,
            ),
        };
        let fact = FailureFact::new(class, code.reason());
        if class == FailureClass::Representation {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(34).unwrap(),
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
pub(super) fn fail(cause: SetStateFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(cause)
}
