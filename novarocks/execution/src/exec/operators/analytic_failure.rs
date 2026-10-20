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
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{
    error::Error,
    fmt,
    num::{NonZeroU16, TryFromIntError},
};

#[derive(Debug)]
struct AnalyticGuard {
    detail: BoundedDetail,
    parameter: bool,
}
impl fmt::Display for AnalyticGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail.as_str())
    }
}
impl Error for AnalyticGuard {}
impl FailureProjection for AnalyticGuard {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = if self.parameter {
            (FailureClass::Data, ExecutionReason::AnalyticParameter)
        } else {
            (FailureClass::Invariant, ExecutionReason::AnalyticContract)
        };
        FailureFact::new(class, reason.reason())
    }
}
pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticGuard {
        detail: BoundedDetail::truncating_display(detail),
        parameter: false,
    })
}
pub(super) fn parameter(detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticGuard {
        detail: BoundedDetail::truncating(detail),
        parameter: true,
    })
}

#[derive(Debug)]
struct AnalyticArrowFailure {
    source: arrow::error::ArrowError,
    cast: bool,
}
impl fmt::Display for AnalyticArrowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}
impl Error for AnalyticArrowFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for AnalyticArrowFailure {
    fn failure_fact(&self) -> FailureFact {
        use arrow::error::ArrowError;
        let (class, reason) = match (&self.source, self.cast) {
            (ArrowError::MemoryError(_), _) => (
                FailureClass::Resource,
                ExecutionReason::AnalyticArrowAllocation,
            ),
            (
                ArrowError::CastError(_)
                | ArrowError::ParseError(_)
                | ArrowError::ArithmeticOverflow(_)
                | ArrowError::DivideByZero,
                true,
            ) => (FailureClass::Data, ExecutionReason::AnalyticArrowValue),
            _ => (
                FailureClass::Invariant,
                ExecutionReason::AnalyticArrowContract,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}
pub(super) fn arrow(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticArrowFailure {
        source,
        cast: false,
    })
}
pub(super) fn cast(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticArrowFailure { source, cast: true })
}

#[derive(Debug)]
struct AnalyticContext {
    stage: Option<&'static str>,
    call: Option<usize>,
    source: ExecutionFailure,
}
impl fmt::Display for AnalyticContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(call) = self.call {
            write!(f, "window function #{call}: {}", self.source)
        } else {
            write!(f, "{}: {}", self.stage.unwrap(), self.source)
        }
    }
}
impl Error for AnalyticContext {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for AnalyticContext {
    fn failure_fact(&self) -> FailureFact {
        self.source.failure_fact()
    }
}
pub(super) fn stage(stage: &'static str, source: ExecutionFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticContext {
        stage: Some(stage),
        call: None,
        source,
    })
}
pub(super) fn call(call: usize, source: ExecutionFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticContext {
        stage: None,
        call: Some(call),
        source,
    })
}

#[derive(Debug)]
enum AnalyticNumericFailure {
    SumNarrow(TryFromIntError),
    Overflow(&'static str),
}
impl fmt::Display for AnalyticNumericFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SumNarrow(_) => f.write_str("sum overflow"),
            Self::Overflow(operation) => write!(f, "{operation} overflow"),
        }
    }
}
impl Error for AnalyticNumericFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SumNarrow(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for AnalyticNumericFailure {
    fn failure_fact(&self) -> FailureFact {
        novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
    }
}
pub(super) fn sum_narrow(source: TryFromIntError) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticNumericFailure::SumNarrow(source))
}
pub(super) fn numeric_overflow(operation: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(AnalyticNumericFailure::Overflow(operation))
}

#[derive(Debug)]
struct AnalyticExtentFailure {
    required: usize,
    key: bool,
    source: TryFromIntError,
}
impl fmt::Display for AnalyticExtentFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "analytic {} extent {} exceeds u32",
            if self.key { "key field" } else { "row index" },
            self.required
        )
    }
}
impl Error for AnalyticExtentFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for AnalyticExtentFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Representation,
            if self.key {
                ExecutionReason::AnalyticKeyExtent
            } else {
                ExecutionReason::AnalyticRowIndexExtent
            }
            .reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: if self.key {
                RepresentationBoundary::MinimumUnit
            } else {
                RepresentationBoundary::SingleOutput
            },
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: NonZeroU16::new(if self.key { 46 } else { 47 }).unwrap(),
            },
            required: u64::try_from(self.required).ok(),
            extent_limit: Some(u32::MAX as u64),
            representable_prefix: None,
        })
    }
}
fn extent(required: usize, key: bool) -> ExecutionResult<u32> {
    u32::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(AnalyticExtentFailure {
            required,
            key,
            source,
        })
    })
}
pub(super) fn key_extent(required: usize) -> ExecutionResult<u32> {
    extent(required, true)
}
pub(super) fn row_index(required: usize) -> ExecutionResult<u32> {
    extent(required, false)
}

#[cfg(test)]
mod properties {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::sync::Arc;

    #[test]
    fn actual_invalid_arrow_constructor_keeps_source_after_field_and_input_exit() {
        let field = Arc::new(Field::new("ResourceExhausted", DataType::Int32, true));
        let weak_field = Arc::downgrade(&field);
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let weak_input = Arc::downgrade(&input);
        let source = StructArray::try_new(
            Fields::from(vec![field]),
            vec![Arc::clone(&input), Arc::clone(&input)],
            None,
        )
        .unwrap_err();
        let failure = arrow(source);
        drop(input);
        assert!(weak_input.upgrade().is_none());
        assert!(weak_field.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 508);
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<arrow::error::ArrowError>()
        );
    }
    #[test]
    fn checked_numeric_and_u32_limits_project_the_actual_producing_operations() {
        let source = i64::try_from(i128::MAX).unwrap_err();
        let failure = sum_narrow(source);
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 41);
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<TryFromIntError>()
        );
        assert_eq!(key_extent(u32::MAX as usize).unwrap(), u32::MAX);
        assert_eq!(row_index(u32::MAX as usize).unwrap(), u32::MAX);
        if let Some(required) = (u32::MAX as usize).checked_add(1) {
            for failure in [
                key_extent(required).unwrap_err(),
                row_index(required).unwrap_err(),
            ] {
                let fact = failure.failure_fact();
                let rep = fact.representation().unwrap();
                assert_eq!(rep.required, Some(required as u64));
                assert_eq!(rep.extent_limit, Some(u32::MAX as u64));
                assert_eq!(rep.representable_prefix, None);
                assert!(fact.resource().is_none());
                assert!(
                    failure
                        .source()
                        .unwrap()
                        .source()
                        .unwrap()
                        .source()
                        .unwrap()
                        .is::<TryFromIntError>()
                );
            }
        }
    }
    #[test]
    fn stage_and_call_context_forward_original_fact_without_owning_input_or_scope() {
        let source = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
        let fact = source.failure_fact();
        let failure = stage("concat_batches", call(7, source.clone()));
        assert_eq!(failure.failure_fact(), fact);
        assert_eq!(
            failure.to_string(),
            format!("concat_batches: window function #7: {source}")
        );
    }
}
