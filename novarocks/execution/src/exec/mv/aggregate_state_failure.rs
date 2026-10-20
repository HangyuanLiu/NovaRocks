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

//! Aggregate MV state owns physical-row validity and merge arithmetic facts.
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::error::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{
    fmt,
    num::{NonZeroU16, TryFromIntError},
};
#[derive(Clone, Copy, Debug)]
pub(super) enum RowSet {
    Physical,
    Active,
}
#[derive(Debug)]
enum AggregateStateFailure {
    Contract(BoundedDetail),
    InvalidState(BoundedDetail),
    Duplicate {
        row_set: RowSet,
        row_id: BoundedDetail,
    },
    Constructor(ArrowError),
    RowCount {
        required: usize,
        source: TryFromIntError,
    },
    CountOverflow {
        current: i64,
        incoming: i64,
    },
    ScaleOverflow {
        sum: i128,
        factor: i128,
    },
    SlotIndex {
        required: usize,
        source: TryFromIntError,
    },
}
impl fmt::Display for AggregateStateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) | Self::InvalidState(detail) => detail.fmt(f),
            Self::Duplicate {
                row_set: RowSet::Physical,
                row_id,
            } => write!(
                f,
                "aggregate MV state corruption: duplicate row id `{row_id}`"
            ),
            Self::Duplicate {
                row_set: RowSet::Active,
                row_id,
            } => write!(
                f,
                "active aggregate MV state corruption: duplicate active MV row id `{row_id}`"
            ),
            Self::Constructor(source) => write!(
                f,
                "aggregate MV physical batch construction failed: {source}"
            ),
            Self::RowCount { required, .. } => {
                write!(f, "aggregate MV row count {required} does not fit Int64")
            }
            Self::CountOverflow { current, incoming } => write!(
                f,
                "aggregate MV retraction count overflow: {current} + {incoming}"
            ),
            Self::ScaleOverflow { sum, factor } => write!(
                f,
                "aggregate MV Decimal128 scale overflow: {sum} * {factor}"
            ),
            Self::SlotIndex { required, .. } => write!(
                f,
                "aggregate MV slot index {required} exceeds u32 identity extent"
            ),
        }
    }
}
impl std::error::Error for AggregateStateFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Constructor(source) => Some(source),
            Self::RowCount { source, .. } | Self::SlotIndex { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for AggregateStateFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidState(_) | Self::Duplicate { .. } => {
                novarocks_functions::DataFailureReason::InvalidEncoding.failure_fact()
            }
            Self::RowCount { .. } | Self::CountOverflow { .. } | Self::ScaleOverflow { .. } => {
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            }
            Self::Contract(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::MvAggregatePhysicalContract.reason(),
            ),
            Self::Constructor(ArrowError::MemoryError(_)) => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::MvAggregatePhysicalAllocation.reason(),
            ),
            Self::Constructor(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::MvAggregatePhysicalConstructor.reason(),
            ),
            Self::SlotIndex { required, .. } => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::MvAggregatePhysicalSlotExtent.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(41).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(u64::from(u32::MAX)),
                representable_prefix: None,
            }),
        }
    }
}
pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(AggregateStateFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn invalid_state(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(AggregateStateFailure::InvalidState(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn duplicate(row_set: RowSet, row_id: &str) -> ExecutionFailure {
    ExecutionFailure::from_domain(AggregateStateFailure::Duplicate {
        row_set,
        row_id: BoundedDetail::truncating_display(row_id),
    })
}
pub(super) fn constructor(source: ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(AggregateStateFailure::Constructor(source))
}
pub(super) fn row_count(required: usize) -> ExecutionResult<i64> {
    i64::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(AggregateStateFailure::RowCount { required, source })
    })
}
pub(super) fn slot_index(required: usize) -> ExecutionResult<u32> {
    u32::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(AggregateStateFailure::SlotIndex { required, source })
    })
}
pub(super) fn count_overflow(current: i64, incoming: i64) -> ExecutionFailure {
    ExecutionFailure::from_domain(AggregateStateFailure::CountOverflow { current, incoming })
}
pub(super) fn scale_overflow(sum: i128, factor: i128) -> ExecutionFailure {
    ExecutionFailure::from_domain(AggregateStateFailure::ScaleOverflow { sum, factor })
}
