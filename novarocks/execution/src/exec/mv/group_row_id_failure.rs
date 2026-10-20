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

//! Group-row identity encoding owns its carrier and output-format facts.
use crate::runtime::fragment::ExecutionReason;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
enum GroupRowIdFailure {
    Contract(BoundedDetail),
    Utf8Extent {
        total: u128,
        unit: u128,
        prefix: usize,
    },
}
impl fmt::Display for GroupRowIdFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::Utf8Extent { total, unit, .. } => write!(
                f,
                "aggregate MV row id Utf8 extent {total} (current row {unit}) exceeds Int32 offsets"
            ),
        }
    }
}
impl std::error::Error for GroupRowIdFailure {}
impl FailureProjection for GroupRowIdFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Contract(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::MvGroupRowIdContract.reason(),
            ),
            Self::Utf8Extent {
                total,
                unit,
                prefix,
            } => {
                let minimum = *unit > i32::MAX as u128;
                FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::MvGroupRowIdOutputExtent.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: if minimum {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: NonZeroU16::new(39).unwrap(),
                    },
                    required: u64::try_from(if minimum { *unit } else { *total }).ok(),
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: u64::try_from(*prefix).ok(),
                })
            }
        }
    }
}
pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(GroupRowIdFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn layout() -> ExecutionFailure {
    ExecutionFailure::from_domain(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
}
pub(super) fn check_utf8_extent(total: u128, unit: u128, prefix: usize) -> ExecutionResult<()> {
    if total > i32::MAX as u128 {
        return Err(ExecutionFailure::from_domain(
            GroupRowIdFailure::Utf8Extent {
                total,
                unit,
                prefix,
            },
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn group_identity_offsets_keep_batch_and_minimum_unit_distinct() {
        check_utf8_extent(i32::MAX as u128, 1, 8).unwrap();
        let batch = check_utf8_extent(i32::MAX as u128 + 1, 1, 8)
            .unwrap_err()
            .failure_fact();
        let facts = batch.representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(facts.representable_prefix, Some(8));
        let unit = check_utf8_extent(i32::MAX as u128 + 1, i32::MAX as u128 + 1, 0)
            .unwrap_err()
            .failure_fact();
        assert_eq!(
            unit.representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
    }
}
