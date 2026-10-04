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

//! Actual window signature, ordered channels and explicit frame options.

use std::sync::Arc;

use novarocks_functions::{
    AggregateKernelPhase, AggregateOrderKey, AggregatePreparationOptions,
    AggregateWindowPreparationOptions, ConstantPolicy, FunctionArgument, FunctionBindingRequest,
    FunctionBindingSelection, FunctionSpecializationFailure, KernelFailure,
    MAX_CALL_EFFECT_ARGUMENTS, PureCallPreparation, ScopedExpressionEffects, WindowCallOptions,
};
use novarocks_physical_plan::{
    AggregatePhase, BoundFunction, ConstantPools, ExprId, ExprKind, ExprNode, Fragment,
    NullOrdering, SortDirection, WindowBound, window_offset_observed,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionKind, FunctionValueType,
};

use super::{
    expression_occurrences::ExpressionOccurrenceError,
    physical_call_arguments::{PhysicalArgumentError, author_physical_argument_observed},
    physical_scalar_requests::author_scalar_result_selection_observed,
};

#[derive(Debug)]
pub(crate) enum PhysicalWindowRequestError {
    Control(CompileControlError),
    Argument(PhysicalArgumentError),
    Function(ExpressionOccurrenceError),
    InvalidSource(&'static str),
    MissingArgument(ExprId),
    NonconstantFrameOffset(ExprId),
    TooManyArguments,
}
impl From<CompileControlError> for PhysicalWindowRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<PhysicalArgumentError> for PhysicalWindowRequestError {
    fn from(error: PhysicalArgumentError) -> Self {
        match error {
            PhysicalArgumentError::Control(cause) => Self::Control(cause),
            other => Self::Argument(other),
        }
    }
}
impl From<KernelFailure> for PhysicalWindowRequestError {
    fn from(error: KernelFailure) -> Self {
        match ExpressionOccurrenceError::function(FunctionSpecializationFailure::Kernel(error)) {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Function(other),
        }
    }
}

#[derive(Debug)]
pub(crate) struct AuthoredPhysicalWindowRequest<'a> {
    source: &'a ExprNode,
    function: &'a BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    logical_count: usize,
    result_constraint: &'a FunctionValueType,
    window: WindowCallOptions,
    aggregate: Option<AggregatePreparationOptions>,
}
impl AuthoredPhysicalWindowRequest<'_> {
    pub const fn source(&self) -> &ExprNode {
        self.source
    }
    pub const fn function(&self) -> &BoundFunction {
        self.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_count,
            expected_result_type: Some(self.result_constraint),
        }
    }
    pub fn preparation(&self, arguments: ScopedExpressionEffects) -> PureCallPreparation {
        match &self.aggregate {
            Some(aggregate) => PureCallPreparation::AggregateWindow {
                arguments,
                options: AggregateWindowPreparationOptions {
                    aggregate: aggregate.clone(),
                    window: self.window,
                },
            },
            None => PureCallPreparation::Window {
                arguments,
                options: self.window,
            },
        }
    }
}

/// Static channels are args then function ORDER BY. Frame offsets remain
/// independent runtime-use children, never logical function arguments.
/// Exact offset facts reuse the sole physical constant consumer; None frame
/// stays None. The caller owns original source/result/lexical admission, entry,
/// footer and opaque request/selection/options coexistence, not a host grant.
pub(crate) fn author_physical_window_request_observed<'a>(
    source: &'a ExprNode,
    fragment: &Fragment,
    pools: &ConstantPools,
    literal_policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalWindowRequest<'a>, PhysicalWindowRequestError> {
    work.step()?;
    let ExprKind::WindowCall {
        function,
        distinct,
        args,
        function_order_by,
        frame,
        ignore_nulls,
        aggregate_binding,
    } = &source.kind
    else {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window request is not an actual WindowCall",
        ));
    };
    let count = args.len().checked_add(function_order_by.len());
    work.step()?;
    let count = count
        .filter(|&count| count <= MAX_CALL_EFFECT_ARGUMENTS)
        .ok_or(PhysicalWindowRequestError::TooManyArguments)?;
    let exact_count = count == function.argument_types.len();
    work.step()?;
    if !exact_count {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window channels differ from their selected signature",
        ));
    }
    let aggregate = match (function.kind, aggregate_binding.as_deref()) {
        (FunctionKind::Window, None) if !distinct && function_order_by.is_empty() => None,
        (FunctionKind::Aggregate, Some(binding)) => {
            let exact = binding.phase == AggregatePhase::Single
                && binding.logical_argument_count as usize == args.len();
            work.step()?;
            if !exact {
                return Err(PhysicalWindowRequestError::InvalidSource(
                    "aggregate OVER requires its exact Single logical argument count",
                ));
            }
            work.flush()?;
            let mut keys = Vec::new();
            keys.try_reserve_exact(function_order_by.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for key in function_order_by {
                keys.push(AggregateOrderKey {
                    ascending: key.direction == SortDirection::Ascending,
                    nulls_first: key.null_ordering == NullOrdering::First,
                });
                work.step()?;
            }
            work.flush()?;
            Some(AggregatePreparationOptions {
                phase: AggregateKernelPhase::Single,
                distinct: *distinct,
                order_keys: keys.into(),
                state_input_type: None,
            })
        }
        _ => {
            return Err(PhysicalWindowRequestError::InvalidSource(
                "window source kind and lifecycle binding differ",
            ));
        }
    };
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for id in args
        .iter()
        .copied()
        .chain(function_order_by.iter().map(|key| key.expr))
    {
        let argument = fragment.expressions().get(id);
        work.step()?;
        let argument = argument.ok_or(PhysicalWindowRequestError::MissingArgument(id))?;
        arguments.push(author_physical_argument_observed(
            argument,
            pools,
            literal_policy,
            CompilePhase::FunctionSpecialization,
            work,
        )?);
        work.step()?;
    }
    let selected =
        author_scalar_result_selection_observed(function, aggregate_binding.as_deref(), work)?;
    let frame = frame
        .as_ref()
        .map(|frame| {
            Ok::<_, PhysicalWindowRequestError>(novarocks_type_contract::WindowFrame {
                units: frame.units,
                start: bound(frame.start, fragment, pools, work)?,
                end: bound(frame.end, fragment, pools, work)?,
                exclusion: frame.exclusion,
            })
        })
        .transpose()?;
    work.flush()?;
    let window = WindowCallOptions::try_new(frame, *ignore_nulls, work.control())?;
    work.flush()?;
    Ok(AuthoredPhysicalWindowRequest {
        source,
        function,
        selected,
        arguments,
        logical_count: args.len(),
        result_constraint: &source.ty,
        window,
        aggregate,
    })
}

fn bound(
    bound: WindowBound,
    fragment: &Fragment,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::WindowBound<u64>, PhysicalWindowRequestError> {
    use novarocks_type_contract::WindowBound as Bound;
    work.step()?;
    Ok(match bound {
        Bound::UnboundedPreceding => Bound::UnboundedPreceding,
        Bound::CurrentRow => Bound::CurrentRow,
        Bound::UnboundedFollowing => Bound::UnboundedFollowing,
        Bound::Preceding(id) | Bound::Following(id) => {
            let offset = window_offset_observed(fragment, pools, id, work)
                .map_err(PhysicalArgumentError::from)?;
            work.step()?;
            let offset = offset.ok_or(PhysicalWindowRequestError::NonconstantFrameOffset(id))?;
            if matches!(bound, Bound::Preceding(_)) {
                Bound::Preceding(offset)
            } else {
                Bound::Following(offset)
            }
        }
    })
}
