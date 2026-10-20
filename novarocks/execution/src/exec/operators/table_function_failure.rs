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
//! Table-function runtime facts. Frozen binding failures use FragmentBindingError.
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, RepresentationBoundary, RepresentationFacts,
    RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Debug)]
pub(super) enum TableFunctionFailure {
    InvalidOffsets {
        row: usize,
        start: i32,
        end: i32,
    },
    InputLength {
        argument: usize,
        expected: usize,
        actual: usize,
    },
    MissingList,
    OutputRowExtent {
        required: Option<u128>,
    },
    BitmapValueRange {
        value: u64,
    },
    Arrow(arrow::error::ArrowError),
    InputRowExtent {
        row: usize,
    },
}
impl fmt::Display for TableFunctionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOffsets { row, start, end } => write!(
                f,
                "table function unnest list offsets are invalid at row {row}: {start}..{end}"
            ),
            Self::InputLength {
                argument,
                expected,
                actual,
            } => write!(
                f,
                "table function unnest param {argument} length mismatch: expected {expected}, got {actual}"
            ),
            Self::OutputRowExtent { .. } => f.write_str("table function output too large"),
            Self::MissingList => {
                f.write_str("table function unnest requires at least one list param")
            }
            Self::BitmapValueRange { value } => write!(
                f,
                "table function unnest_bitmap value out of BIGINT range: {value}"
            ),
            Self::Arrow(error) => error.fmt(f),
            Self::InputRowExtent { row } => write!(
                f,
                "table function input row {row} exceeds its UInt32 index representation"
            ),
        }
    }
}
impl Error for TableFunctionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow(error) => Some(error),
            _ => None,
        }
    }
}
impl FailureProjection for TableFunctionFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self {
            Self::InvalidOffsets { .. } => (
                FailureClass::Invariant,
                ExecutionReason::TableFunctionOffsets,
            ),
            Self::InputLength { .. } => (
                FailureClass::Invariant,
                ExecutionReason::TableFunctionInputLength,
            ),
            Self::OutputRowExtent { .. } => (
                FailureClass::Representation,
                ExecutionReason::TableFunctionOutputRowExtent,
            ),
            Self::MissingList => (
                FailureClass::Invariant,
                ExecutionReason::TableFunctionMissingList,
            ),
            Self::BitmapValueRange { .. } => {
                return novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            Self::Arrow(_) => (FailureClass::Invariant, ExecutionReason::TableFunctionArrow),
            Self::InputRowExtent { .. } => (
                FailureClass::Representation,
                ExecutionReason::TableFunctionInputRowExtent,
            ),
        };
        let fact = FailureFact::new(class, reason.reason());
        match self {
            Self::OutputRowExtent { required } => fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(4).unwrap(),
                },
                required: required.and_then(|rows| u64::try_from(rows).ok()),
                extent_limit: Some(u64::from(u32::MAX)),
                representable_prefix: None,
            }),
            Self::InputRowExtent { row } => fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(3).unwrap(),
                },
                required: u64::try_from(*row).ok().and_then(|row| row.checked_add(1)),
                extent_limit: Some(u64::from(u32::MAX) + 1),
                representable_prefix: None,
            }),
            _ => fact,
        }
    }
}
impl From<TableFunctionFailure> for ExecutionFailure {
    fn from(cause: TableFunctionFailure) -> Self {
        Self::from_domain(cause)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn complete_table_output_extent_and_numeric_bitmap_value_have_distinct_causes() {
        assert_eq!(
            super::super::output_rows(&[u32::MAX as usize]).unwrap(),
            u32::MAX as usize
        );
        let refusal = super::super::output_rows(&[u32::MAX as usize, 1, 2]).unwrap_err();
        assert_eq!(refusal.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            refusal.failure_fact().representation().unwrap().required,
            Some(u64::from(u32::MAX) + 3)
        );
        let range = TableFunctionFailure::BitmapValueRange { value: u64::MAX };
        assert_eq!(range.failure_fact().class(), FailureClass::Data);
        assert_eq!(range.failure_fact().representation(), None);
    }
}
