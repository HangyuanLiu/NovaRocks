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
    FunctionBindingSelection, FunctionResultType, FunctionSpecializationFailure, KernelFailure,
    MAX_CALL_EFFECT_ARGUMENTS, PureCallPreparation, ScopedExpressionEffects, WindowCallOptions,
};
use novarocks_physical_plan::{
    AggregatePhase, BoundFunction, ConstantPools, ExprId, ExprKind, ExprNode, Fragment,
    NullOrdering, SortDirection, WindowBound, window_offset_observed,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionKind,
    FunctionValueType, ValueTypeError,
};

use super::{
    expression_occurrences::ExpressionOccurrenceError,
    lowered_draft::{
        CanonicalCallOperationalRequest, CheckedExpressionLogicalSourceEntry, SqlExpressionCallKind,
    },
    physical_call_arguments::{
        PhysicalArgumentError, argument_types_exact_observed, author_physical_argument_observed,
    },
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

impl From<ValueTypeError> for PhysicalWindowRequestError {
    fn from(error: ValueTypeError) -> Self {
        Self::from(PhysicalArgumentError::from(error))
    }
}

#[derive(Debug)]
enum WindowRequestArguments<'a> {
    OwnedPhysical {
        arguments: Vec<FunctionArgument>,
        logical_count: usize,
        result_constraint: &'a FunctionValueType,
    },
    BorrowedCanonical {
        request: &'a Arc<CanonicalCallOperationalRequest>,
        decimal_policy: DecimalOverflowPolicy,
        constant_policy: ConstantPolicy,
    },
}

#[derive(Debug)]
pub(crate) struct AuthoredPhysicalWindowRequest<'a> {
    source: &'a ExprNode,
    function: &'a BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: WindowRequestArguments<'a>,
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
        match &self.arguments {
            WindowRequestArguments::OwnedPhysical {
                arguments,
                logical_count,
                result_constraint,
            } => FunctionBindingRequest {
                arguments,
                logical_argument_count: *logical_count,
                expected_result_type: Some(result_constraint),
            },
            WindowRequestArguments::BorrowedCanonical { request, .. } => request.request(),
        }
    }
    pub fn captured_decimal_overflow_policy(&self) -> Option<DecimalOverflowPolicy> {
        match &self.arguments {
            WindowRequestArguments::OwnedPhysical { .. } => None,
            WindowRequestArguments::BorrowedCanonical { decimal_policy, .. } => {
                Some(*decimal_policy)
            }
        }
    }
    pub fn captured_constant_policy(&self) -> Option<ConstantPolicy> {
        match &self.arguments {
            WindowRequestArguments::OwnedPhysical { .. } => None,
            WindowRequestArguments::BorrowedCanonical {
                constant_policy, ..
            } => Some(*constant_policy),
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
    let aggregate = aggregate_options_observed(
        function,
        *distinct,
        args.len(),
        function_order_by,
        aggregate_binding.as_deref(),
        work,
    )?;
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
    let window = window_options_observed(frame.as_ref(), *ignore_nulls, fragment, pools, work)?;
    Ok(AuthoredPhysicalWindowRequest {
        source,
        function,
        selected,
        arguments: WindowRequestArguments::OwnedPhysical {
            arguments,
            logical_count: args.len(),
            result_constraint: &source.ty,
        },
        window,
        aggregate,
    })
}

/// Borrow the original emitter's operational channels and the same selected
/// Arc. Only frame offsets consult the original physical constant consumer;
/// logical arguments are neither projected again nor admitted as literals.
/// The caller owns immutable source/catalog/pool admission and the footer.
pub(crate) fn author_physical_window_request_from_journal_observed<'source>(
    entry: &CheckedExpressionLogicalSourceEntry<'source>,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalWindowRequest<'source>, PhysicalWindowRequestError> {
    let is_window = entry.kind() == SqlExpressionCallKind::Window;
    work.step()?;
    if !is_window {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window journal has another source kind",
        ));
    }
    let canonical = entry.canonical_operational();
    work.step()?;
    let canonical = canonical.ok_or(PhysicalWindowRequestError::InvalidSource(
        "window journal has no canonical operational request",
    ))?;
    let belongs = canonical.belongs_to(entry.captured());
    work.step()?;
    if !belongs {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical request has another original capture",
        ));
    }
    let source = entry.source();
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
    let request = canonical.request();
    let selected = canonical.selected();
    let complete_count = request.logical_argument_count == args.len()
        && request.arguments.len() == count
        && selected.argument_types.len() == count;
    work.step()?;
    if !complete_count {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window operational channel counts differ",
        ));
    }
    work.flush()?;
    let original = entry.captured().binding().resolved();
    let identity = original.function_id == function.function_id
        && original.kind == function.kind
        && original.selected.overload == function.overload
        && selected.overload == function.overload;
    work.step()?;
    work.flush()?;
    if !identity {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical binding has another identity",
        ));
    }
    signature_observed(function, selected, work)?;
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical result is not scalar",
        ));
    };
    if !source.ty.exactly_equals_observed(result, || {
        work.step().map_err(PhysicalWindowRequestError::from)
    })? {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window source result differs from canonical result",
        ));
    }
    match (&selected.aggregate, aggregate_binding.as_deref()) {
        (None, None) => {}
        (Some(state), Some(binding)) => {
            signature_observed(&binding.function, selected, work)?;
            work.flush()?;
            let identity = binding.function.function_id == function.function_id
                && binding.function.kind == function.kind
                && state.state_argument_contract == binding.state_argument_contract
                && state.state_format == binding.state_format;
            work.step()?;
            work.flush()?;
            if !identity
                || !state
                    .intermediate_type
                    .exactly_equals_observed(&binding.intermediate_type, || {
                        work.step().map_err(PhysicalWindowRequestError::from)
                    })?
            {
                return Err(PhysicalWindowRequestError::InvalidSource(
                    "window aggregate canonical state metadata differs",
                ));
            }
        }
        _ => {
            return Err(PhysicalWindowRequestError::InvalidSource(
                "window aggregate canonical header differs",
            ));
        }
    }
    let aggregate = aggregate_options_observed(
        function,
        *distinct,
        args.len(),
        function_order_by,
        aggregate_binding.as_deref(),
        work,
    )?;
    let window =
        window_options_observed(frame.as_ref(), *ignore_nulls, entry.fragment(), pools, work)?;
    work.flush()?;
    let selected = Arc::clone(selected);
    work.step()?;
    work.flush()?;
    Ok(AuthoredPhysicalWindowRequest {
        source,
        function,
        selected,
        arguments: WindowRequestArguments::BorrowedCanonical {
            request: canonical,
            decimal_policy: entry.captured().binding().decimal_overflow_policy(),
            constant_policy: entry.captured().constant_policy(),
        },
        window,
        aggregate,
    })
}

fn signature_observed(
    function: &BoundFunction,
    selected: &FunctionBindingSelection,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalWindowRequestError> {
    work.flush()?;
    let header = function.overload == selected.overload
        && function.argument_types.len() == selected.argument_types.len();
    work.step()?;
    work.flush()?;
    if !header {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical selected signature differs",
        ));
    }
    for (left, right) in function
        .argument_types
        .iter()
        .zip(selected.argument_types.iter())
    {
        if !argument_types_exact_observed::<PhysicalWindowRequestError>(left, right, work)? {
            return Err(PhysicalWindowRequestError::InvalidSource(
                "window canonical selected argument differs",
            ));
        }
    }
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical result is not scalar",
        ));
    };
    if !function.result_type.exactly_equals_observed(result, || {
        work.step().map_err(PhysicalWindowRequestError::from)
    })? {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical selected result differs",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "physical_window_journal_tests.rs"]
mod journal_tests;

fn aggregate_options_observed(
    function: &BoundFunction,
    distinct: bool,
    logical_count: usize,
    function_order_by: &[novarocks_physical_plan::SortExpr],
    aggregate_binding: Option<&novarocks_physical_plan::AggregateBinding>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<AggregatePreparationOptions>, PhysicalWindowRequestError> {
    let aggregate = match (function.kind, aggregate_binding) {
        (FunctionKind::Window, None) if !distinct && function_order_by.is_empty() => None,
        (FunctionKind::Aggregate, Some(binding)) => {
            let exact = binding.phase == AggregatePhase::Single
                && binding.logical_argument_count as usize == logical_count;
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
                distinct,
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
    Ok(aggregate)
}

fn window_options_observed(
    frame: Option<&novarocks_physical_plan::WindowFrame>,
    ignore_nulls: bool,
    fragment: &Fragment,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<WindowCallOptions, PhysicalWindowRequestError> {
    let frame = frame
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
    let window = WindowCallOptions::try_new(frame, ignore_nulls, work.control())?;
    work.flush()?;
    Ok(window)
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
