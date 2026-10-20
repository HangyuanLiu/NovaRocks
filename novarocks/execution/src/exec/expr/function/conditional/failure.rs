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
//! Conditional expression failures preserve their source and frozen type facts.
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts,
};
impl From<novarocks_functions::control_values::AssemblyFailure> for ExecutionFailure {
    fn from(source: novarocks_functions::control_values::AssemblyFailure) -> Self {
        source.into_kernel().into()
    }
}
#[derive(Debug)]
struct ConditionalContext {
    context: BoundedDetail,
    source: ExecutionFailure,
}
impl std::fmt::Display for ConditionalContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.context.as_str(), self.source)
    }
}
impl std::error::Error for ConditionalContext {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for ConditionalContext {
    fn failure_fact(&self) -> FailureFact {
        self.source.failure_fact()
    }
}
pub(super) fn context(
    context: impl std::fmt::Display,
    source: ExecutionFailure,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(ConditionalContext {
        context: BoundedDetail::truncating_display(context),
        source,
    })
}
pub(super) fn arrow(
    context: impl std::fmt::Display,
    source: arrow::error::ArrowError,
) -> ExecutionFailure {
    novarocks_functions::control_values::AssemblyFailure::arrow(context, source).into()
}
#[derive(Debug)]
struct AssertionFailed(BoundedDetail);
impl std::fmt::Display for AssertionFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for AssertionFailed {}
impl FailureProjection for AssertionFailed {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            ExecutionReason::AssertionFailed.reason(),
        )
    }
}
pub(super) fn assertion(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(AssertionFailed(BoundedDetail::truncating_display(detail)))
}
#[derive(Debug)]
struct ConditionalBacking {
    source: std::alloc::LayoutError,
    requested: u128,
}
impl std::fmt::Display for ConditionalBacking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "conditional temporary backing requires {} bytes: {}",
            self.requested, self.source
        )
    }
}
impl std::error::Error for ConditionalBacking {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for ConditionalBacking {
    fn failure_fact(&self) -> FailureFact {
        novarocks_functions::allocation_failure::rust_array_backing_failure_fact(
            ExecutionReason::ConditionalBacking.reason(),
            self.requested,
            novarocks_failure_contract::RepresentationBoundary::SingleOutput,
        )
    }
}
pub(super) fn preflight<T>(rows: usize) -> ExecutionResult<()> {
    std::alloc::Layout::array::<T>(rows)
        .map(|_| ())
        .map_err(|source| {
            ExecutionFailure::from_domain(ConditionalBacking {
                source,
                requested: rows as u128 * std::mem::size_of::<T>() as u128,
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conditional_failure_authors_keep_assertion_data_and_physical_extent_separate() {
        let failure = assertion("cancelled numeric overflow resource exhausted");
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(
            failure.failure_fact().reason(),
            ExecutionReason::AssertionFailed.reason()
        );
        let failure = preflight::<Option<u64>>(usize::MAX).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Representation);
        let extent = failure.failure_fact().representation().unwrap();
        assert_eq!(extent.required, None);
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert!(failure.failure_fact().resource().is_none());
    }
}
