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

//! State scalar computations author value, shape and output-format facts.
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::fragment::ExecutionReason;
use arrow::error::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
enum StateScalarFailure {
    Contract(BoundedDetail),
    InvalidState(BoundedDetail),
    Numeric(BoundedDetail),
    Value(BoundedDetail),
    Constructor(ArrowError),
    NumericConversion(std::num::TryFromIntError),
    OutputExtent {
        total: u128,
        largest: u128,
        prefix_rows: Option<u64>,
        layout: StateOffsetLayout,
    },
}
impl fmt::Display for StateScalarFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail)
            | Self::InvalidState(detail)
            | Self::Numeric(detail)
            | Self::Value(detail) => detail.fmt(f),
            Self::Constructor(source) => source.fmt(f),
            Self::NumericConversion(source) => source.fmt(f),
            Self::OutputExtent {
                total,
                largest,
                layout,
                ..
            } => write!(
                f,
                "MV state output extent {total} (largest value {largest}) exceeds {layout:?} offsets"
            ),
        }
    }
}
impl std::error::Error for StateScalarFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Constructor(source) => Some(source),
            Self::NumericConversion(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for StateScalarFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Contract(_) => (
                FailureClass::Invariant,
                ExecutionReason::MvStateScalarContract,
            ),
            Self::InvalidState(_) => {
                return novarocks_functions::DataFailureReason::InvalidEncoding.failure_fact();
            }
            Self::Numeric(_) | Self::NumericConversion(_) => {
                return novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            Self::Value(_) => {
                return novarocks_functions::DataFailureReason::InvalidValue.failure_fact();
            }
            Self::Constructor(ArrowError::MemoryError(_)) => (
                FailureClass::Resource,
                ExecutionReason::MvStateScalarAllocation,
            ),
            Self::Constructor(_) => (
                FailureClass::Invariant,
                ExecutionReason::MvStateScalarConstructor,
            ),
            Self::OutputExtent {
                total,
                largest,
                prefix_rows,
                layout,
            } => {
                let minimum = *largest > layout.limit();
                return FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::MvStateScalarOutputExtent.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: if minimum {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: NonZeroU16::new(layout.code()).unwrap(),
                    },
                    required: u64::try_from(if minimum { *largest } else { *total }).ok(),
                    extent_limit: Some(layout.limit() as u64),
                    // Every preceding output row passed this same append check.
                    representable_prefix: *prefix_rows,
                });
            }
        };
        FailureFact::new(class, code.reason())
    }
}
pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(StateScalarFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn invalid_state(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(StateScalarFailure::InvalidState(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn numeric(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(StateScalarFailure::Numeric(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn value(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(StateScalarFailure::Value(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn constructor(source: ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(StateScalarFailure::Constructor(source))
}
pub(super) fn numeric_conversion(source: std::num::TryFromIntError) -> ExecutionFailure {
    ExecutionFailure::from_domain(StateScalarFailure::NumericConversion(source))
}
#[derive(Debug, Clone, Copy, Default)]
enum StateOffsetLayout {
    #[default]
    Binary,
    Utf8,
    LargeUtf8,
}
impl StateOffsetLayout {
    fn limit(self) -> u128 {
        match self {
            Self::LargeUtf8 => i64::MAX as u128,
            _ => i32::MAX as u128,
        }
    }
    fn code(self) -> u16 {
        match self {
            Self::Binary => 36,
            Self::Utf8 => 37,
            Self::LargeUtf8 => 38,
        }
    }
}
#[derive(Default)]
pub(super) struct StateOutputTally {
    total: u128,
    largest: u128,
    rows: u128,
    layout: StateOffsetLayout,
}
impl StateOutputTally {
    pub(super) fn utf8(large: bool) -> Self {
        Self {
            layout: if large {
                StateOffsetLayout::LargeUtf8
            } else {
                StateOffsetLayout::Utf8
            },
            ..Self::default()
        }
    }

    /// These scalar emitters produce exactly one state per logical input row.
    /// Only successful preceding appends establish the useful input prefix.
    pub(super) fn append(&mut self, bytes: usize) -> Result<(), ExecutionFailure> {
        let total = self.total.checked_add(bytes as u128).ok_or_else(|| {
            ExecutionFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        let largest = self.largest.max(bytes as u128);
        if total > self.layout.limit() {
            return Err(ExecutionFailure::from_domain(
                StateScalarFailure::OutputExtent {
                    total,
                    largest,
                    prefix_rows: u64::try_from(self.rows).ok(),
                    layout: self.layout,
                },
            ));
        }
        let rows = self.rows.checked_add(1).ok_or_else(|| {
            ExecutionFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        self.total = total;
        self.largest = largest;
        self.rows = rows;
        Ok(())
    }
}
#[cfg(test)]
mod source_properties {
    use super::*;
    #[test]
    fn mv_state_offsets_prove_prefix_from_successful_rows_and_do_not_advance_on_refusal() {
        let mut tally = StateOutputTally::default();
        tally.append(0).unwrap();
        tally.append(i32::MAX as usize).unwrap();
        let batch = tally.append(1).unwrap_err().failure_fact();
        let facts = batch.representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(facts.representable_prefix, Some(2));
        assert_eq!(facts.required, Some(i32::MAX as u64 + 1));
        assert_eq!((tally.rows, tally.total), (2, i32::MAX as u128));
        let mut tally = StateOutputTally::default();
        tally.append(0).unwrap();
        let unit = tally
            .append(i32::MAX as usize + 1)
            .unwrap_err()
            .failure_fact();
        let facts = unit.representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(facts.representable_prefix, Some(1));
        assert_eq!(tally.rows, 1);
    }
    #[test]
    fn mv_state_numeric_values_and_frozen_contracts_project_different_facts() {
        assert_eq!(
            numeric("count overflow").failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        assert_eq!(
            invalid_state("encoded count mismatch").failure_fact(),
            novarocks_functions::DataFailureReason::InvalidEncoding.failure_fact()
        );
        assert_eq!(
            contract("frozen output shape mismatch")
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
}
