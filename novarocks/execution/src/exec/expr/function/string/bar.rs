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
use crate::exec::expr::{ExprId, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::array::ArrayRef;
use arrow::array::StringArray;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, RepresentationBoundary, RepresentationFacts,
    RepresentationLayout, ResourceDimension, ResourceFacts, domain,
};
use std::sync::Arc;

use super::common::{IntArgArray, downcast_int_arg_array};

const BAR_CHAR: &str = "\u{2593}";

fn const_i64_arg(arr: IntArgArray<'_>, name: &'static str) -> ExecutionResult<i64> {
    if arr.len() == 0 {
        return Err(ExecutionFailure::from_domain(BarFailure::NonConstant(name)));
    }
    if arr.is_null(0) {
        return Err(ExecutionFailure::from_domain(BarFailure::NonConstant(name)));
    }
    let first = arr.value(0);
    for row in 1..arr.len() {
        if arr.is_null(row) || arr.value(row) != first {
            return Err(ExecutionFailure::from_domain(BarFailure::NonConstant(name)));
        }
    }
    Ok(first)
}

pub fn eval_bar<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let _ = expr;
    if args.len() != 4 {
        return Err(expression_contract("bar requires four prepared arguments"));
    }
    let size_eval = arena.eval(args[0], chunk)?;
    let min_eval = arena.eval(args[1], chunk)?;
    let max_eval = arena.eval(args[2], chunk)?;
    let width_eval = arena.eval(args[3], chunk)?;

    if chunk.is_empty() {
        return Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new())));
    }
    let size_arr = downcast_int_arg_array(&size_eval, "bar")?;
    let min_arr = downcast_int_arg_array(&min_eval, "bar")?;
    let max_arr = downcast_int_arg_array(&max_eval, "bar")?;
    let width_arr = downcast_int_arg_array(&width_eval, "bar")?;

    let min = const_i64_arg(min_arr, "argument[min]")?;
    let max = const_i64_arg(max_arr, "argument[max]")?;
    let width = const_i64_arg(width_arr, "argument[width]")?;

    if min >= max {
        return Err(ExecutionFailure::from_domain(BarFailure::Bounds {
            min,
            max,
        }));
    }
    if width <= 0 {
        return Err(ExecutionFailure::from_domain(BarFailure::Width(width)));
    }

    let mut out = Vec::with_capacity(chunk.len());
    let mut output_offset = 0;
    for row in 0..chunk.len() {
        if size_arr.is_null(row) {
            out.push(None);
            continue;
        }

        let size = size_arr.value(row);
        if size < min {
            return Err(ExecutionFailure::from_domain(BarFailure::Below {
                size,
                min,
            }));
        }
        if size > max {
            return Err(ExecutionFailure::from_domain(BarFailure::Above {
                size,
                max,
            }));
        }

        let bar_width = projected_width(size, min, max, width);
        let byte_len = bar_extent(bar_width)?;
        output_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
            output_offset,
            byte_len,
        )?;
        out.push(Some(render_bar(bar_width, byte_len)?));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[derive(Debug)]
enum BarFailure {
    NonConstant(&'static str),
    Bounds {
        min: i64,
        max: i64,
    },
    Width(i64),
    Below {
        size: i64,
        min: i64,
    },
    Above {
        size: i64,
        max: i64,
    },
    Extent {
        required: u128,
    },
    Allocation {
        requested: usize,
        source: std::collections::TryReserveError,
    },
}
impl std::fmt::Display for BarFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonConstant(name) => write!(f, "bar: {name} must be constant"),
            Self::Bounds { min, max } => write!(f, "bar requirement: min < max ({min} >= {max})"),
            Self::Width(width) => write!(f, "bar requirement: width > 0 ({width})"),
            Self::Below { size, min } => write!(f, "bar requirement: size >= min ({size} < {min})"),
            Self::Above { size, max } => write!(f, "bar requirement: size <= max ({size} > {max})"),
            Self::Extent { required } => write!(
                f,
                "bar value requires {required} UTF-8 bytes, exceeding i32 offsets"
            ),
            Self::Allocation { requested, source } => {
                write!(f, "bar output allocation of {requested} bytes: {source}")
            }
        }
    }
}
impl std::error::Error for BarFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Allocation { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for BarFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Extent { required } => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::StringBarExtent.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(48).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(i32::MAX as u64),
                representable_prefix: None,
            }),
            Self::Allocation { requested, .. } => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::StringBarAllocation.reason(),
            )
            .with_resource(ResourceFacts {
                request_scope: None,
                constraint_scope: None,
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*requested).ok(),
                required_growth: None,
                limit: None,
                live: None,
                committed: None,
            }),
            _ => FailureFact::new(FailureClass::Data, ExecutionReason::StringBarInput.reason()),
        }
    }
}
fn projected_width(size: i64, min: i64, max: i64, width: i64) -> usize {
    let ratio = ((i128::from(size) - i128::from(min)) as f64
        / (i128::from(max) - i128::from(min)) as f64)
        .min(1.0);
    (ratio * width as f64) as usize
}
fn bar_extent(width: usize) -> ExecutionResult<usize> {
    let required = width as u128 * BAR_CHAR.len() as u128;
    if required > i32::MAX as u128 {
        return Err(ExecutionFailure::from_domain(BarFailure::Extent {
            required,
        }));
    }
    Ok(required as usize)
}
fn render_bar(width: usize, byte_len: usize) -> ExecutionResult<String> {
    let mut value = String::new();
    if width == 0 {
        return Ok(value);
    }
    value.try_reserve_exact(byte_len).map_err(|source| {
        ExecutionFailure::from_domain(BarFailure::Allocation {
            requested: byte_len,
            source,
        })
    })?;
    value.push_str(BAR_CHAR);
    // Every source range ends on a complete BAR_CHAR, and the one reservation
    // covers the final byte extent. Doubling keeps the original repeat cost.
    while value.len() < byte_len {
        let copy = value.len().min(byte_len - value.len());
        value.extend_from_within(..copy);
    }
    Ok(value)
}
#[cfg(test)]
mod source_properties {
    use super::*;
    #[test]
    fn legal_extreme_integer_bounds_use_exact_differences_without_wrapping() {
        assert_eq!(projected_width(i64::MIN, i64::MIN, i64::MAX, 20), 0);
        assert_eq!(projected_width(i64::MAX, i64::MIN, i64::MAX, 20), 20);
        assert_eq!(projected_width(0, i64::MIN, i64::MAX, 20), 10);
        for width in [0, 1, 2, 3, 17, 128] {
            assert_eq!(
                render_bar(width, bar_extent(width).unwrap()).unwrap(),
                BAR_CHAR.repeat(width)
            );
        }
    }
    #[test]
    fn actual_utf8_minimum_unit_extent_is_refused_before_materialization() {
        let maximum = i32::MAX as usize / BAR_CHAR.len();
        assert_eq!(bar_extent(maximum).unwrap(), maximum * BAR_CHAR.len());
        let failure = bar_extent(maximum + 1).unwrap_err();
        let fact = failure.failure_fact();
        let rep = fact.representation().unwrap();
        assert_eq!(rep.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(rep.required, Some(((maximum + 1) * BAR_CHAR.len()) as u64));
        assert_eq!(rep.extent_limit, Some(i32::MAX as u64));
        assert_eq!(rep.representable_prefix, None);
        assert!(fact.resource().is_none());
    }
}
