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
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, RepresentationBoundary, RepresentationFacts,
    RepresentationLayout, domain,
};

/// Retain the actual Arrow cause, and only the conversion facts this call owns.
/// In particular, a known large-byte narrowing uses Arrow's ComputeError for
/// offset refusal; arbitrary ComputeError messages cannot authorize that class.
#[derive(Debug)]
struct SubstituteCastFailure {
    source: ArrowError,
    offset_narrow: bool,
    temporal_value: bool,
}
impl std::fmt::Display for SubstituteCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "substitute cast error: {}", self.source)
    }
}
impl std::error::Error for SubstituteCastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for SubstituteCastFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self.source {
            ArrowError::CastError(_) if self.temporal_value => {
                return novarocks_functions::DataFailureReason::InvalidValue.failure_fact();
            }
            ArrowError::MemoryError(_) => {
                (FailureClass::Resource, ExecutionReason::ArrayCastAllocation)
            }
            ArrowError::OffsetOverflowError(_) => (
                FailureClass::Representation,
                ExecutionReason::ArrayCastOffset,
            ),
            ArrowError::ComputeError(_) if self.offset_narrow => (
                FailureClass::Representation,
                ExecutionReason::ArrayCastOffset,
            ),
            ArrowError::IoError(..) => (
                FailureClass::Unexpected,
                ExecutionReason::ArrayCastUnexpected,
            ),
            _ => (FailureClass::Invariant, ExecutionReason::ArrayCastContract),
        };
        let fact = FailureFact::new(class, reason.reason());
        if class == FailureClass::Representation {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(31).unwrap(),
                },
                required: None,
                extent_limit: self.offset_narrow.then_some(i32::MAX as u64),
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}

pub fn eval_substitute<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "substitute requires one argument",
            )
            .into(),
        );
    }
    let output_type = super::common::frozen_type(arena.arena(), expr)?;
    let arr = arena.eval(args[0], chunk)?;
    if arr.data_type() == output_type {
        return Ok(arr);
    }
    let offset_narrow = matches!(
        (arr.data_type(), output_type),
        (DataType::LargeUtf8, DataType::Utf8) | (DataType::LargeBinary, DataType::Binary)
    );
    let temporal_value = matches!(output_type, DataType::Utf8 | DataType::LargeUtf8)
        && matches!(
            arr.data_type(),
            DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
                | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        );
    arrow::compute::cast(&arr, output_type).map_err(|source| {
        ExecutionFailure::from_domain(SubstituteCastFailure {
            source,
            offset_narrow,
            temporal_value,
        })
    })
}
