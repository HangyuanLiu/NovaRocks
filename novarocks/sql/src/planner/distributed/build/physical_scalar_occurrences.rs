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

//! Fresh exact-owner preparation for one actual scalar/control occurrence.

use std::{collections::BTreeMap, sync::Arc};

use novarocks_functions::{
    CallEffectInput, PureCallPreparation, PureCallSpecialization, PureKernelAbi,
    ScopedExpressionEffects,
};
use novarocks_physical_plan::{ExprId, ExprKind, ExprNode, FrozenPhysicalCall, PhysicalCallSite};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, ControlShape, DecimalOverflowPolicy,
    EffectContractError, ExpressionControlFlow, ExpressionUseId, SemanticParameterRef,
    SemanticParameters,
};

use super::{
    expression_occurrences::{ExpressionOccurrenceError, scalar_shape},
    physical_scalar_requests::AuthoredPhysicalScalarRequest,
};
use crate::compiler::SqlFunctionCatalog;

#[cfg(test)]
#[path = "physical_scalar_occurrences_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) enum PhysicalScalarOccurrenceError {
    Control(CompileControlError),
    Occurrence(ExpressionOccurrenceError),
    Effects(EffectContractError),
    MissingUse(ExpressionUseId),
    MissingChildEffects(ExpressionUseId),
    InvalidSource(&'static str),
    UnsupportedAbi(PureKernelAbi),
}
impl From<CompileControlError> for PhysicalScalarOccurrenceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExpressionOccurrenceError> for PhysicalScalarOccurrenceError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Occurrence(other),
        }
    }
}
impl From<EffectContractError> for PhysicalScalarOccurrenceError {
    fn from(error: EffectContractError) -> Self {
        Self::Effects(error)
    }
}

/// The source policy, exact environment and proof scope are authored separately
/// by the SQL source scope. No package default, legacy binding field, possible
/// environment-key list or frozen-call claim supplies these inputs.
pub(crate) struct PhysicalScalarOccurrenceInput<'a> {
    pub source: &'a ExprNode,
    pub request: &'a AuthoredPhysicalScalarRequest<'a>,
    pub flow: &'a ExpressionControlFlow<ExprId>,
    pub use_id: ExpressionUseId,
    pub child_effects: &'a BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

#[derive(Debug)]
pub(crate) struct FreshPhysicalScalarOccurrence {
    pub frozen: FrozenPhysicalCall,
    pub preparation: PureCallSpecialization,
}

/// Prepare only the actual installed ScalarV1 or ControlIntrinsicV1 owner.
/// The caller validated the same-source definition/flow pairing and orders
/// children before parents. Static requests may be shared by definitions;
/// each actual use keeps its own context, guarded children and complete facts.
/// TypeOnly retains all static arguments without inventing runtime uses.
///
/// The original join_control_argument author verifies ordered child context,
/// demand and guard domains. Whole call facts come from the prepared contract;
/// the composed scoped summary is retained only for parent effect composition.
/// Runtime instances are never created. Caller admission covers argument-use
/// storage, opaque metadata clones and retained immutable preparation. The
/// caller owns entry/ordinary footer; nested primary refusals have no after.
pub(crate) fn prepare_physical_scalar_occurrence_observed(
    input: PhysicalScalarOccurrenceInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalScalarOccurrence, PhysicalScalarOccurrenceError> {
    work.step()?;
    let invocation = input.flow.uses().get(&input.use_id);
    work.step()?;
    let invocation = invocation.ok_or(PhysicalScalarOccurrenceError::MissingUse(input.use_id))?;
    let ExprKind::FunctionCall { function, args } = &input.source.kind else {
        return Err(PhysicalScalarOccurrenceError::InvalidSource(
            "scalar occurrence source is not an actual function call",
        ));
    };
    let same_source = invocation.definition == input.source.id
        && std::ptr::eq(function, input.request.function());
    work.step()?;
    if !same_source {
        return Err(PhysicalScalarOccurrenceError::InvalidSource(
            "scalar occurrence and static request have different source definitions",
        ));
    }
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
    if !matches!(
        abi,
        PureKernelAbi::ScalarV1 | PureKernelAbi::ControlIntrinsicV1
    ) {
        return Err(PhysicalScalarOccurrenceError::UnsupportedAbi(abi));
    }
    let shape = scalar_shape(declaration.effects().argument_control, args.len());
    work.step()?;
    let shape = shape.ok_or(PhysicalScalarOccurrenceError::InvalidSource(
        "installed scalar owner has no scalar control shape",
    ))?;
    let type_only = shape == ControlShape::TypeOnly;
    let correct_shape = invocation.control == shape
        && invocation.arguments.len() == if type_only { 0 } else { args.len() };
    work.step()?;
    if !correct_shape {
        return Err(PhysicalScalarOccurrenceError::InvalidSource(
            "actual scalar control differs from its installed owner",
        ));
    }
    work.flush()?;
    let mut argument_uses = Vec::new();
    argument_uses
        .try_reserve_exact(args.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let mut children = ScopedExpressionEffects::pure_value(invocation.context);
    for (ordinal, &definition) in args.iter().enumerate() {
        if type_only {
            argument_uses.push(None);
        } else {
            let child_id = invocation.arguments[ordinal];
            let child = input.flow.uses().get(&child_id);
            work.step()?;
            let child = child.ok_or(PhysicalScalarOccurrenceError::MissingUse(child_id))?;
            let correct_definition = child.definition == definition;
            work.step()?;
            if !correct_definition {
                return Err(PhysicalScalarOccurrenceError::InvalidSource(
                    "ordered scalar argument use has a different source definition",
                ));
            }
            let effects = input.child_effects.get(&child_id).copied();
            work.step()?;
            children = children.join_control_argument(
                effects.ok_or(PhysicalScalarOccurrenceError::MissingChildEffects(child_id))?,
                input.flow,
                ordinal,
            )?;
            argument_uses.push(Some(child_id));
        }
        work.step()?;
    }
    let options = match abi {
        PureKernelAbi::ScalarV1 => PureCallPreparation::Scalar {
            arguments: children,
        },
        PureKernelAbi::ControlIntrinsicV1 => PureCallPreparation::ControlIntrinsic {
            arguments: children,
        },
        _ => unreachable!("only the two admitted installed ABIs reach preparation"),
    };
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
        site: PhysicalCallSite::Expression(input.use_id),
        context: invocation.context,
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalScalarOccurrence {
        frozen,
        preparation,
    })
}
