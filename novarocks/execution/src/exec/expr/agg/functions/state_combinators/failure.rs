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

//! State combinators own their counter arithmetic and aggregate Binary output.
use crate::runtime::fragment::ExecutionReason;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
enum StateCombinatorFailure {
    CountOverflow {
        current: i64,
        delta: i64,
        context: BoundedDetail,
    },
    SumOverflow {
        current: i128,
        incoming: i128,
        operation: SumOperation,
        context: BoundedDetail,
    },
    BinaryOutput {
        total: u128,
        largest: u128,
    },
}
impl fmt::Display for StateCombinatorFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CountOverflow {
                current,
                delta,
                context,
            } => write!(
                f,
                "{context} overflow while adding count delta {delta} to {current}"
            ),
            Self::SumOverflow {
                operation, context, ..
            } => write!(
                f,
                "{context} overflow while {}",
                match operation {
                    SumOperation::RowCount => "adding row_count delta",
                    SumOperation::Sum => "adding sum delta",
                    SumOperation::Change => "applying change_op",
                }
            ),
            Self::BinaryOutput { total, largest } => write!(
                f,
                "aggregate state Binary output extent {total} (largest state {largest}) exceeds Int32 offsets"
            ),
        }
    }
}
impl std::error::Error for StateCombinatorFailure {}
impl FailureProjection for StateCombinatorFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::CountOverflow { .. } | Self::SumOverflow { .. } => {
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            }
            Self::BinaryOutput { total, largest } => {
                let minimum = *largest > i32::MAX as u128;
                FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::MvAggregateStateOutputExtent.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: if minimum {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: NonZeroU16::new(40).unwrap(),
                    },
                    required: u64::try_from(if minimum { *largest } else { *total }).ok(),
                    extent_limit: Some(i32::MAX as u64),
                    // Aggregate output groups do not establish a committed input prefix.
                    representable_prefix: None,
                })
            }
        }
    }
}
pub(super) fn add_count(current: i64, delta: i64, context: &str) -> ExecutionResult<i64> {
    current.checked_add(delta).ok_or_else(|| {
        ExecutionFailure::from_domain(StateCombinatorFailure::CountOverflow {
            current,
            delta,
            context: BoundedDetail::truncating(context),
        })
    })
}
#[derive(Default)]
pub(super) struct BinaryOutput {
    total: u128,
    largest: u128,
}
impl BinaryOutput {
    pub(super) fn append(&mut self, bytes: usize) -> ExecutionResult<()> {
        let total = self.total.checked_add(bytes as u128).ok_or_else(|| {
            ExecutionFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        let largest = self.largest.max(bytes as u128);
        if total > i32::MAX as u128 {
            return Err(ExecutionFailure::from_domain(
                StateCombinatorFailure::BinaryOutput { total, largest },
            ));
        }
        self.total = total;
        self.largest = largest;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn state_counter_arithmetic_keeps_value_overflow_and_signed_retractions() {
        assert_eq!(add_count(0, -1, "signed count").unwrap(), -1);
        assert_eq!(add_count(i64::MIN, i64::MAX, "count").unwrap(), -1);
        for (current, delta) in [(i64::MAX, 1), (i64::MIN, -1)] {
            let error =
                add_count(current, delta, "resource cancelled invalid encoding").unwrap_err();
            assert_eq!(
                error.failure_fact(),
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            );
        }
    }
    #[test]
    fn aggregate_state_offsets_do_not_invent_an_input_prefix_or_advance_on_refusal() {
        let mut output = BinaryOutput::default();
        output.append(i32::MAX as usize).unwrap();
        let error = output.append(1).unwrap_err();
        let fact = error.failure_fact();
        let repr = fact.representation().unwrap();
        assert_eq!(repr.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(repr.representable_prefix, None);
        assert_eq!(output.total, i32::MAX as u128);
        let error = BinaryOutput::default()
            .append(i32::MAX as usize + 1)
            .unwrap_err();
        assert_eq!(
            error.failure_fact().representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum SumOperation {
    RowCount,
    Sum,
    Change,
}
pub(super) fn sum_overflow(
    current: i128,
    incoming: i128,
    operation: SumOperation,
    context: &str,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(StateCombinatorFailure::SumOverflow {
        current,
        incoming,
        operation,
        context: BoundedDetail::truncating(context),
    })
}
