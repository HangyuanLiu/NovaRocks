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

//! Aggregate output contracts and actual primitive constructors.
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::fragment::ExecutionReason;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
enum OutputFailure {
    Type {
        expected: &'static str,
        actual: BoundedDetail,
    },
    Utf8(std::str::Utf8Error),
    Decimal(ArrowError),
    TextExtent {
        total: u128,
        largest: u128,
    },
}
impl fmt::Display for OutputFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Type { expected, actual } => write!(
                f,
                "{expected} aggregate output type mismatch: {}",
                actual.as_str()
            ),
            Self::Utf8(source) => {
                write!(f, "aggregate UTF-8 state contains invalid bytes: {source}")
            }
            Self::Decimal(source) => {
                write!(f, "aggregate decimal output construction failed: {source}")
            }
            Self::TextExtent { total, largest } => write!(
                f,
                "aggregate UTF-8 output extent {total} (largest value {largest}) exceeds Int32 offsets"
            ),
        }
    }
}
impl std::error::Error for OutputFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Utf8(source) => Some(source),
            Self::Decimal(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for OutputFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Type { .. } => (
                FailureClass::Invariant,
                ExecutionReason::AggregateOutputType,
            ),
            Self::Utf8(_) => (
                FailureClass::Invariant,
                ExecutionReason::AggregateOutputUtf8,
            ),
            Self::Decimal(ArrowError::MemoryError(_)) => (
                FailureClass::Resource,
                ExecutionReason::AggregateOutputDecimalAllocation,
            ),
            // The fixed-width constructor validates declared precision/scale;
            // it does not cast the aggregate's active numeric values.
            Self::Decimal(_) => (
                FailureClass::Invariant,
                ExecutionReason::AggregateOutputDecimalContract,
            ),
            Self::TextExtent { total, largest } => {
                let minimum = *largest > i32::MAX as u128;
                return FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::AggregateOutputTextExtent.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: if minimum {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: NonZeroU16::new(35).unwrap(),
                    },
                    required: u64::try_from(if minimum { *largest } else { *total }).ok(),
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: None,
                });
            }
        };
        FailureFact::new(class, code.reason())
    }
}
pub(super) fn wrong_type(expected: &'static str, actual: &DataType) -> ExecutionFailure {
    ExecutionFailure::from_domain(OutputFailure::Type {
        expected,
        actual: BoundedDetail::truncating_display(format_args!("{actual:?}")),
    })
}
pub(super) fn utf8(source: std::str::Utf8Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(OutputFailure::Utf8(source))
}
pub(super) fn decimal(source: ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(OutputFailure::Decimal(source))
}
#[derive(Default)]
pub(super) struct TextTally {
    total: u128,
    largest: u128,
}
impl TextTally {
    pub(super) fn append(&mut self, bytes: usize) -> Result<(), ExecutionFailure> {
        self.total = self.total.checked_add(bytes as u128).ok_or_else(|| {
            ExecutionFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        self.largest = self.largest.max(bytes as u128);
        if self.total > i32::MAX as u128 {
            return Err(ExecutionFailure::from_domain(OutputFailure::TextExtent {
                total: self.total,
                largest: self.largest,
            }));
        }
        Ok(())
    }
}
