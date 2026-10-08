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

//! Fresh installed window owner facts with actual child and frame scope.

use std::{collections::BTreeMap, sync::Arc};

use novarocks_functions::{
    CallEffectInput, PureCallSpecialization, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_physical_plan::{ExprId, ExprKind, ExprNode, FrozenPhysicalCall, PhysicalCallSite};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, ControlShape, DecimalOverflowPolicy,
    EffectContractError, ExpressionControlFlow, ExpressionUseId, FunctionKind,
    SemanticParameterRef, SemanticParameters,
};

use super::{
    expression_occurrences::ExpressionOccurrenceError,
    physical_window_requests::AuthoredPhysicalWindowRequest,
};
use crate::compiler::SqlFunctionCatalog;

#[cfg(test)]
#[path = "physical_window_occurrences_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) enum PhysicalWindowOccurrenceError {
    Control(CompileControlError),
    Function(ExpressionOccurrenceError),
    Effects(EffectContractError),
    MissingUse(ExpressionUseId),
    MissingChildEffects(ExpressionUseId),
    InvalidSource(&'static str),
    UnsupportedAbi(PureKernelAbi),
}
impl From<CompileControlError> for PhysicalWindowOccurrenceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<EffectContractError> for PhysicalWindowOccurrenceError {
    fn from(error: EffectContractError) -> Self {
        Self::Effects(error)
    }
}
impl From<ExpressionOccurrenceError> for PhysicalWindowOccurrenceError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Function(other),
        }
    }
}

pub(crate) struct PhysicalWindowOccurrenceInput<'a> {
    pub source: &'a ExprNode,
    pub request: &'a AuthoredPhysicalWindowRequest<'a>,
    pub flow: &'a ExpressionControlFlow<ExprId>,
    pub use_id: ExpressionUseId,
    pub child_effects: &'a BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

#[derive(Debug)]
pub(crate) struct FreshPhysicalWindowOccurrence {
    pub frozen: FrozenPhysicalCall,
    pub preparation: PureCallSpecialization,
}

/// All ordered children contribute their original scoped effects, including
/// the independent frame bounds. Only actual function channels appear in the
/// call's argument-use list. No frame membership, partition state or runtime
/// instance is created. The caller owns same-source flow/static validation,
/// child-first ordering, entry/footer and prior admission of opaque clones.
pub(crate) fn prepare_physical_window_occurrence_observed(
    input: PhysicalWindowOccurrenceInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalWindowOccurrence, PhysicalWindowOccurrenceError> {
    let invocation = input.flow.uses().get(&input.use_id);
    work.step()?;
    let invocation = invocation.ok_or(PhysicalWindowOccurrenceError::MissingUse(input.use_id))?;
    let matching = std::ptr::eq(input.source, input.request.source())
        && invocation.definition == input.source.id
        && invocation.control == ControlShape::Eager;
    work.step()?;
    if !matching || !matches!(input.source.kind, ExprKind::WindowCall { .. }) {
        return Err(PhysicalWindowOccurrenceError::InvalidSource(
            "window occurrence has different actual source or control",
        ));
    }
    let function = input.request.function();
    work.flush()?;
    let declaration = functions
        .pure_overload_declaration_observed(
            &function.function_id,
            function.kind,
            &input.request.selected().overload,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let abi = declaration.implementation().abi;
    let correct_abi = matches!(
        (function.kind, abi),
        (FunctionKind::Window, PureKernelAbi::WindowV1)
            | (FunctionKind::Aggregate, PureKernelAbi::AggregateWindowV1)
    );
    work.step()?;
    if !correct_abi {
        return Err(PhysicalWindowOccurrenceError::UnsupportedAbi(abi));
    }
    let channel_count = input.request.request().arguments.len();
    work.flush()?;
    let mut argument_uses = Vec::new();
    argument_uses
        .try_reserve_exact(channel_count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let mut children = ScopedExpressionEffects::pure_value(invocation.context);
    let mut ordinal = 0;
    input
        .source
        .kind
        .expression_references_observed(|definition| {
            let child_id = invocation.arguments.get(ordinal).copied();
            work.step()?;
            let child_id = child_id.ok_or(PhysicalWindowOccurrenceError::InvalidSource(
                "window source child has no actual edge",
            ))?;
            let child = input.flow.uses().get(&child_id);
            work.step()?;
            let matching = child.is_some_and(|child| child.definition == definition);
            work.step()?;
            if !matching {
                return Err(PhysicalWindowOccurrenceError::InvalidSource(
                    "window source child order differs from actual edge",
                ));
            }
            let effects = input.child_effects.get(&child_id).copied();
            work.step()?;
            children = children.join_control_argument(
                effects.ok_or(PhysicalWindowOccurrenceError::MissingChildEffects(child_id))?,
                input.flow,
                ordinal,
            )?;
            if ordinal < channel_count {
                argument_uses.push(Some(child_id));
            }
            ordinal += 1;
            work.step()?;
            Ok::<_, PhysicalWindowOccurrenceError>(())
        })?;
    let complete = ordinal == invocation.arguments.len() && argument_uses.len() == channel_count;
    work.step()?;
    if !complete {
        return Err(PhysicalWindowOccurrenceError::InvalidSource(
            "window argument-use coverage differs",
        ));
    }
    let call = CallEffectInput {
        context: invocation.context,
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function.function_id,
        kind: function.kind,
        selected: input.request.selected().as_ref(),
        request: input.request.request(),
        environment: input.environment,
        parameters: input.parameters,
        decimal_overflow_policy: input.decimal_overflow_policy,
        proof_scope: input.proof_scope,
    };
    work.flush()?;
    let options = input.request.preparation(children);
    work.flush()?;
    let preparation = functions
        .prepare_fresh_selected(
            call,
            Arc::clone(input.request.selected()),
            options,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let frozen = FrozenPhysicalCall {
        regexp_count_pattern_source: None,
        temporal_source: None,
        site: PhysicalCallSite::Expression(input.use_id),
        context: invocation.context,
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalWindowOccurrence {
        frozen,
        preparation,
    })
}
