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

//! Actual aggregate update requests, separate from merge-state inputs.

use std::{alloc::Layout, sync::Arc};

use novarocks_functions::{
    AggregateKernelPhase, AggregateOrderKey, AggregatePreparationOptions, ConstantPolicy,
    FunctionArgument, FunctionBindingRequest, FunctionBindingSelection, MAX_CALL_EFFECT_ARGUMENTS,
    PureCallPreparation, ScopedExpressionEffects,
};
use novarocks_physical_plan::{
    AggregateBinding, AggregateCall, AggregatePhase, BoundFunction, ConstantPools, ExprId,
    Fragment, NodeKind, NullOrdering, PhysicalCallSite, PhysicalNode, SortDirection, TopNReduction,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionKind,
};

use super::{
    lowered_draft::CanonicalAggregateOperationalRequest,
    physical_call_arguments::{PhysicalArgumentError, author_physical_argument_observed},
    physical_scalar_requests::author_scalar_result_selection_observed,
};

#[derive(Debug)]
pub(crate) enum PhysicalAggregateRequestError {
    Control(CompileControlError),
    Argument(PhysicalArgumentError),
    InvalidSource(&'static str),
    MissingArgument(ExprId),
    MissingLogicalSource(AggregatePhase),
}
impl From<CompileControlError> for PhysicalAggregateRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for PhysicalAggregateRequestError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        PhysicalArgumentError::from(error).into()
    }
}
impl From<PhysicalArgumentError> for PhysicalAggregateRequestError {
    fn from(error: PhysicalArgumentError) -> Self {
        match error {
            PhysicalArgumentError::Control(cause) => Self::Control(cause),
            other => Self::Argument(other),
        }
    }
}

/// Borrows the actual immutable node and call. The same selected Arc feeds
/// refinement and fresh preparation; signature facts grant no capability.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalAggregateUpdateRequest<'a> {
    source: &'a AggregateCall,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    selected: Arc<FunctionBindingSelection>,
    arguments: AggregateUpdateArguments<'a>,
    options: AggregatePreparationOptions,
}
#[derive(Debug)]
enum AggregateUpdateArguments<'a> {
    OwnedPhysical(Vec<FunctionArgument>),
    Captured(&'a crate::binding::CapturedAggregateLogicalRequest),
    Canonical {
        captured: &'a crate::binding::CapturedAggregateLogicalRequest,
        canonical: &'a CanonicalAggregateOperationalRequest,
    },
}
impl AuthoredPhysicalAggregateUpdateRequest<'_> {
    pub const fn source(&self) -> &AggregateCall {
        self.source
    }
    pub const fn node(&self) -> &PhysicalNode {
        self.node
    }
    pub const fn site(&self) -> PhysicalCallSite {
        self.site
    }
    pub const fn binding(&self) -> &AggregateBinding {
        &self.source.binding
    }
    pub const fn function(&self) -> &BoundFunction {
        &self.source.binding.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        match &self.arguments {
            AggregateUpdateArguments::OwnedPhysical(arguments) => FunctionBindingRequest {
                arguments,
                logical_argument_count: self.source.arguments.len(),
                // A Partial output carries state; the selected SQL result remains
                // the binding's final result, independently of the output layout.
                expected_result_type: Some(&self.source.binding.function.result_type),
            },
            AggregateUpdateArguments::Captured(captured) => captured.request(),
            AggregateUpdateArguments::Canonical { canonical, .. } => canonical.request(),
        }
    }
    /// None identifies the independent direct-physical component. It supplies
    /// no inferred policy for callers requiring an authenticated journal loan.
    pub fn captured_decimal_overflow_policy(
        &self,
    ) -> Option<novarocks_type_contract::DecimalOverflowPolicy> {
        match &self.arguments {
            AggregateUpdateArguments::OwnedPhysical(_) => None,
            AggregateUpdateArguments::Captured(captured)
            | AggregateUpdateArguments::Canonical { captured, .. } => {
                Some(captured.binding().decimal_overflow_policy())
            }
        }
    }
    pub fn captured_constant_policy(&self) -> Option<ConstantPolicy> {
        match &self.arguments {
            AggregateUpdateArguments::OwnedPhysical(_) => None,
            AggregateUpdateArguments::Captured(captured)
            | AggregateUpdateArguments::Canonical { captured, .. } => {
                Some(captured.constant_policy())
            }
        }
    }
    pub fn preparation(&self, arguments: ScopedExpressionEffects) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments,
            options: self.options.clone(),
        }
    }
}

/// Preserve logical channels followed by function ORDER BY channels, with
/// every occurrence authored from its actual expression. Single/Partial only:
/// a merge state's signature cannot recover the original logical request.
/// Existing phase, input/domain and sequence laws remain with their mandatory
/// physical and installed owners. No legacy effects or environment are used.
///
/// The caller owns entry, ordinary/success footer, per-call policy/environment
/// and admission of source and opaque nested clones. Layout checks and fallible
/// reserves do not provide a host grant. Control/resource refusals return
/// directly on the original meter, without a later observation here.
pub(crate) fn author_physical_aggregate_update_request_observed<'a>(
    source: &'a AggregateCall,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    fragment: &Fragment,
    pools: &ConstantPools,
    literal_policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalAggregateUpdateRequest<'a>, PhysicalAggregateRequestError> {
    work.flush()?;
    let original = fragment.nodes().get(&node.id);
    work.step()?;
    work.flush()?;
    if !original.is_some_and(|original| std::ptr::eq(original, node)) {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate request node is not the original fragment node",
        ));
    }
    let actual = match (site, &node.kind) {
        (PhysicalCallSite::Aggregate { node: id, call }, NodeKind::Aggregate { calls, .. })
            if id == node.id =>
        {
            usize::try_from(call)
                .ok()
                .and_then(|ordinal| calls.get(ordinal))
        }
        (
            PhysicalCallSite::TopNState { node: id, call },
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            },
        ) if id == node.id => usize::try_from(call)
            .ok()
            .and_then(|ordinal| calls.get(ordinal)),
        _ => None,
    };
    let associated = actual.is_some_and(|actual| std::ptr::eq(actual, source));
    work.step()?;
    if !associated {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate request call differs from its exact node site",
        ));
    }
    let aggregate = source.binding.function.kind == FunctionKind::Aggregate;
    work.step()?;
    if !aggregate {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate request carries a different function kind",
        ));
    }
    let phase = match source.binding.phase {
        AggregatePhase::Single => Ok(AggregateKernelPhase::Single),
        AggregatePhase::Partial { .. } => Ok(AggregateKernelPhase::Partial),
        phase @ (AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. }) => {
            Err(PhysicalAggregateRequestError::MissingLogicalSource(phase))
        }
    };
    work.step()?;
    let phase = phase?;
    let count = source
        .arguments
        .len()
        .checked_add(source.order_by.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if count > MAX_CALL_EFFECT_ARGUMENTS
        || source.binding.function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let logical = usize::try_from(source.binding.logical_argument_count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let exact_count =
        logical == source.arguments.len() && count == source.binding.function.argument_types.len();
    work.step()?;
    if !exact_count {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate logical and ORDER BY counts differ from its selected signature",
        ));
    }
    Layout::array::<FunctionArgument>(count)
        .and_then(|_| Layout::array::<AggregateOrderKey>(source.order_by.len()))
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for id in source
        .arguments
        .iter()
        .copied()
        .chain(source.order_by.iter().map(|key| key.expr))
    {
        let argument = fragment.expressions().get(id);
        work.step()?;
        let argument = argument.ok_or(PhysicalAggregateRequestError::MissingArgument(id))?;
        arguments.push(author_physical_argument_observed(
            argument,
            pools,
            literal_policy,
            CompilePhase::FunctionSpecialization,
            work,
        )?);
        work.step()?;
    }
    work.flush()?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(source.order_by.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for key in source.order_by.iter() {
        keys.push(AggregateOrderKey {
            ascending: key.direction == SortDirection::Ascending,
            nulls_first: key.null_ordering == NullOrdering::First,
        });
        work.step()?;
    }
    let selected = author_scalar_result_selection_observed(
        &source.binding.function,
        Some(&source.binding),
        work,
    )?;
    work.flush()?;
    let options = AggregatePreparationOptions {
        phase,
        distinct: source.distinct,
        order_keys: keys.into(),
        state_input_type: None,
    };
    work.flush()?;
    Ok(AuthoredPhysicalAggregateUpdateRequest {
        source,
        node,
        site,
        selected,
        arguments: AggregateUpdateArguments::OwnedPhysical(arguments),
        options,
    })
}

/// Borrow the original admitted logical request for a producer-certified
/// Single/Partial update. Single requires the same-emission operational request
/// and selected Arc, associated with the original capture revision and binding.
/// Partial retains its original captured request. Physical expression channels
/// identify runtime roots; they never recreate logical source provenance. ORDER
/// flags come from the exact actual call, and policies remain with the capture.
///
/// Caller entry, ordinary footer and opaque clone/coexistence admission remain
/// mandatory; original typed control refusals return without a footer here.
pub(crate) fn author_physical_aggregate_update_request_from_journal_observed<'source>(
    entry: &super::lowered_draft::CheckedAggregateLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalAggregateUpdateRequest<'source>, PhysicalAggregateRequestError> {
    use super::lowered_draft::AggregateRuntimeDemand;
    let source = entry.source();
    let phase = match entry.phase() {
        AggregatePhase::Single => Ok(AggregateKernelPhase::Single),
        AggregatePhase::Partial { .. } => Ok(AggregateKernelPhase::Partial),
        phase => Err(PhysicalAggregateRequestError::MissingLogicalSource(phase)),
    };
    work.step()?;
    let phase = phase?;
    let update =
        entry.runtime() == AggregateRuntimeDemand::Update && source.binding.phase == entry.phase();
    work.step()?;
    if !update {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "journal update request differs from its original runtime demand",
        ));
    }
    let captured = entry.captured();
    let canonical = if phase == AggregateKernelPhase::Single {
        let canonical = entry.canonical();
        work.step()?;
        let canonical = canonical.ok_or(PhysicalAggregateRequestError::InvalidSource(
            "single aggregate journal has no same-emission operational request",
        ))?;
        let belongs = canonical.belongs_to(captured);
        work.step()?;
        if !belongs {
            return Err(PhysicalAggregateRequestError::InvalidSource(
                "single aggregate operational request has a different capture revision or binding",
            ));
        }
        Some(canonical)
    } else {
        None
    };
    let request = match canonical {
        Some(canonical) => canonical.request(),
        None => captured.request(),
    };
    let count = source
        .arguments
        .len()
        .checked_add(source.order_by.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if count > MAX_CALL_EFFECT_ARGUMENTS
        || request.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
        || source.binding.function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let matching = request.logical_argument_count == source.arguments.len()
        && request.arguments.len() == count
        && source.binding.function.argument_types.len() == count;
    work.step()?;
    if !matching {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "journal update logical and ORDER channels differ from the captured request",
        ));
    }
    Layout::array::<AggregateOrderKey>(source.order_by.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let selected = match canonical {
        Some(canonical) => {
            selected_correspondence_observed(
                captured,
                canonical.selected(),
                request.logical_argument_count,
                source,
                work,
            )?;
            // Retain the exact same-emission Arc for refinement and preparation.
            // An Arc clone is not a second selected-signature author or host grant.
            work.flush()?;
            let selected = Arc::clone(canonical.selected());
            work.step()?;
            work.flush()?;
            selected
        }
        None => captured_selected_correspondence_observed(captured, source, work)?,
    };
    work.flush()?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(source.order_by.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for key in source.order_by.iter() {
        keys.push(AggregateOrderKey {
            ascending: key.direction == SortDirection::Ascending,
            nulls_first: key.null_ordering == NullOrdering::First,
        });
        work.step()?;
    }
    work.flush()?;
    let options = AggregatePreparationOptions {
        phase,
        distinct: source.distinct,
        order_keys: keys.into(),
        state_input_type: None,
    };
    work.flush()?;
    let arguments = match canonical {
        Some(canonical) => AggregateUpdateArguments::Canonical {
            captured,
            canonical: canonical.as_ref(),
        },
        None => AggregateUpdateArguments::Captured(captured),
    };
    Ok(AuthoredPhysicalAggregateUpdateRequest {
        source,
        node: entry.node(),
        site: entry.site(),
        selected,
        arguments,
        options,
    })
}

/// A merge borrows its original producer journal and logical request. Runtime
/// state is a separate source loan; it never becomes a selected SQL argument.
/// The one authored selected Arc is shared by refinement and fresh preparation.
pub(crate) struct AuthoredPhysicalAggregateMergeRequest<'entry, 'source> {
    entry: &'entry super::lowered_draft::CheckedAggregateLogicalSourceEntry<'source>,
    state_id: ExprId,
    state: &'source novarocks_physical_plan::ExprNode,
    phase: AggregateKernelPhase,
    selected: Arc<FunctionBindingSelection>,
}
impl AuthoredPhysicalAggregateMergeRequest<'_, '_> {
    pub fn source(&self) -> &AggregateCall {
        self.entry.source()
    }
    pub fn node(&self) -> &PhysicalNode {
        self.entry.node()
    }
    pub fn fragment(&self) -> &Fragment {
        self.entry.fragment()
    }
    pub fn site(&self) -> PhysicalCallSite {
        self.entry.site()
    }
    pub fn function(&self) -> &BoundFunction {
        &self.source().binding.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub const fn state_id(&self) -> ExprId {
        self.state_id
    }
    pub const fn state(&self) -> &novarocks_physical_plan::ExprNode {
        self.state
    }
    pub const fn phase(&self) -> AggregateKernelPhase {
        self.phase
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        self.entry.captured().request()
    }
    pub fn decimal_overflow_policy(&self) -> novarocks_type_contract::DecimalOverflowPolicy {
        self.entry.captured().binding().decimal_overflow_policy()
    }
    pub fn preparation(&self, arguments: ScopedExpressionEffects) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments,
            options: AggregatePreparationOptions {
                phase: self.phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: Some(self.state.ty.clone()),
            },
        }
    }
}

/// Selected correspondence checks signature facts only. Source certification
/// comes exclusively from the sealed original producer journal. The caller
/// admits exact-type comparison scratch and opaque metadata/Arc clones, and
/// owns entry and the ordinary footer on this same meter.
pub(crate) fn author_physical_aggregate_merge_request_observed<'entry, 'source>(
    entry: &'entry super::lowered_draft::CheckedAggregateLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalAggregateMergeRequest<'entry, 'source>, PhysicalAggregateRequestError> {
    use super::lowered_draft::AggregateRuntimeDemand;
    let source = entry.source();
    let phase = match entry.phase() {
        AggregatePhase::Intermediate { .. } => Ok(AggregateKernelPhase::Intermediate),
        AggregatePhase::Final { .. } => Ok(AggregateKernelPhase::Final),
        _ => Err(PhysicalAggregateRequestError::InvalidSource(
            "merge request has an update phase",
        )),
    };
    work.step()?;
    let phase = phase?;
    let state_id = match entry.runtime() {
        AggregateRuntimeDemand::ExpressionState(id) => Some(id),
        _ => None,
    };
    work.step()?;
    let state_id = state_id.ok_or(PhysicalAggregateRequestError::InvalidSource(
        "merge request has no original expression state",
    ))?;
    let channels = source.arguments.as_ref() == [state_id]
        && source.order_by.is_empty()
        && !source.distinct
        && source.binding.phase == entry.phase();
    work.step()?;
    if !channels {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge request differs from its original state channels",
        ));
    }
    let state = entry.fragment().expressions().get(state_id);
    work.step()?;
    let state = state.ok_or(PhysicalAggregateRequestError::MissingArgument(state_id))?;
    let selected = captured_selected_correspondence_observed(entry.captured(), source, work)?;
    Ok(AuthoredPhysicalAggregateMergeRequest {
        entry,
        state_id,
        state,
        phase,
        selected,
    })
}

/// The one signature comparison author serves journal updates and merges.
/// Equality verifies selected correspondence; only the original sealed journal
/// establishes logical source provenance. This helper does not inspect state
/// expression domains or infer nullable coercions.
fn captured_selected_correspondence_observed(
    captured: &crate::binding::CapturedAggregateLogicalRequest,
    source: &AggregateCall,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<FunctionBindingSelection>, PhysicalAggregateRequestError> {
    selected_correspondence_observed(
        captured,
        &captured.binding().resolved().selected,
        captured.request().logical_argument_count,
        source,
        work,
    )?;
    // Partial and merge retain their existing physical signature projection.
    // Only Single consumes the original same-emission selected Arc.
    Ok(author_scalar_result_selection_observed(
        &source.binding.function,
        Some(&source.binding),
        work,
    )?)
}

fn selected_correspondence_observed(
    captured: &crate::binding::CapturedAggregateLogicalRequest,
    selected: &FunctionBindingSelection,
    logical_argument_count: usize,
    source: &AggregateCall,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalAggregateRequestError> {
    use novarocks_functions::FunctionResultType;
    let resolved = captured.binding().resolved();
    let function = &source.binding.function;
    if selected.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        || function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let identity = resolved.function_id == function.function_id
        && resolved.kind == FunctionKind::Aggregate
        && function.kind == FunctionKind::Aggregate
        && selected.overload == function.overload
        && selected.overload == resolved.selected.overload
        && usize::try_from(source.binding.logical_argument_count).ok()
            == Some(resolved.logical_argument_count)
        && resolved.logical_argument_count == logical_argument_count
        && selected.argument_types.len() == function.argument_types.len();
    work.step()?;
    if !identity {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured logical identity or arity",
        ));
    }
    for (original, actual) in selected.argument_types.iter().zip(&function.argument_types) {
        work.flush()?;
        let matching = merge_argument_type_matches_observed(original, actual, work)?;
        work.flush()?;
        if !matching {
            return Err(PhysicalAggregateRequestError::InvalidSource(
                "merge binding differs from its captured full argument signature",
            ));
        }
    }
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge source selected a relation result",
        ));
    };
    work.flush()?;
    let matching = result
        .exactly_equals_observed::<PhysicalAggregateRequestError>(&function.result_type, || {
            work.step().map_err(PhysicalAggregateRequestError::from)
        })?;
    work.flush()?;
    if !matching {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured final result type",
        ));
    }
    let aggregate = selected.aggregate.as_ref();
    work.step()?;
    let aggregate = aggregate.ok_or(PhysicalAggregateRequestError::InvalidSource(
        "merge logical source has no aggregate state contract",
    ))?;
    let format = aggregate.state_format == source.binding.state_format;
    work.step()?;
    if !format {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured state format",
        ));
    }
    work.flush()?;
    let matching = aggregate
        .intermediate_type
        .exactly_equals_observed::<PhysicalAggregateRequestError>(
            &source.binding.intermediate_type,
            || work.step().map_err(PhysicalAggregateRequestError::from),
        )?;
    work.flush()?;
    if !matching {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured intermediate type",
        ));
    }
    // No local state-domain/nullability inference: the original Functions owner
    // validates the actual state loan and aligns its cloned options exactly.
    Ok(())
}

fn merge_argument_type_matches_observed(
    left: &novarocks_type_contract::FunctionArgumentType,
    right: &novarocks_type_contract::FunctionArgumentType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, PhysicalAggregateRequestError> {
    use novarocks_type_contract::FunctionArgumentType;
    work.step()?;
    match (left, right) {
        (FunctionArgumentType::Value(left), FunctionArgumentType::Value(right)) => left
            .exactly_equals_observed(right, || {
                work.step().map_err(PhysicalAggregateRequestError::from)
            }),
        (
            FunctionArgumentType::Lambda {
                parameter_types: left,
                result_type: left_result,
            },
            FunctionArgumentType::Lambda {
                parameter_types: right,
                result_type: right_result,
            },
        ) => {
            let same_count = left.len() == right.len();
            work.step()?;
            if !same_count {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                if !left.exactly_equals_observed(right, || {
                    work.step().map_err(PhysicalAggregateRequestError::from)
                })? {
                    return Ok(false);
                }
            }
            left_result.exactly_equals_observed(right_result, || {
                work.step().map_err(PhysicalAggregateRequestError::from)
            })
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
#[path = "physical_aggregate_journal_update_tests.rs"]
mod journal_update_tests;
