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
    arguments: Vec<FunctionArgument>,
    options: AggregatePreparationOptions,
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
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.source.arguments.len(),
            // A Partial output carries state; the selected SQL result remains
            // the binding's final result, independently of the output layout.
            expected_result_type: Some(&self.source.binding.function.result_type),
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
        arguments,
        options,
    })
}
