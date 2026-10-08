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
    pub definitions: Option<&'a novarocks_physical_plan::ExprArena>,
    pub temporal_source: Option<&'a novarocks_type_contract::TemporalSourcePlan<ExprId>>,
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
    let shape = match declaration.effects().argument_control {
        novarocks_type_contract::ArgumentControl::TemporalSource(kind) => {
            let source =
                input
                    .temporal_source
                    .ok_or(PhysicalScalarOccurrenceError::InvalidSource(
                        "missing same-emission temporal source facts",
                    ))?;
            let definitions = novarocks_physical_plan::temporal_source_definitions_observed(
                kind,
                input
                    .definitions
                    .ok_or(PhysicalScalarOccurrenceError::InvalidSource(
                        "missing original temporal source arena",
                    ))?,
                args,
                work,
            )
            .map_err(|error| match error {
                novarocks_physical_plan::TemporalSourceProjectionError::Control(cause) => {
                    PhysicalScalarOccurrenceError::Control(cause)
                }
                _ => PhysicalScalarOccurrenceError::InvalidSource(
                    "invalid original temporal source projection",
                ),
            })?;
            source.validate(&definitions).map_err(|_| {
                PhysicalScalarOccurrenceError::InvalidSource(
                    "stale temporal source roles, definitions or facts",
                )
            })?;
            Some(ControlShape::TemporalSource(source.facts.shape()))
        }
        control => {
            if input.temporal_source.is_some() {
                return Err(PhysicalScalarOccurrenceError::InvalidSource(
                    "temporal sources on an ordinary scalar call",
                ));
            }
            scalar_shape(control, args.len())
        }
    };
    work.step()?;
    let shape = shape.ok_or(PhysicalScalarOccurrenceError::InvalidSource(
        "installed scalar owner has no scalar control shape",
    ))?;
    let type_only = shape == ControlShape::TypeOnly;
    let correct_shape = invocation.control == shape
        && invocation.arguments.len()
            == if type_only {
                0
            } else {
                input
                    .temporal_source
                    .map_or(args.len(), |source| source.sources.len())
            };
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
    let mut source_channels = Vec::new();
    if let Some(source) = input.temporal_source {
        source_channels
            .try_reserve_exact(source.sources.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
    }
    let mut children = ScopedExpressionEffects::pure_value(invocation.context);
    let count = input
        .temporal_source
        .map_or(args.len(), |source| source.sources.len());
    for ordinal in 0..count {
        let definition = input.temporal_source.map_or_else(
            || args[ordinal],
            |source| source.sources[ordinal].definition,
        );
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
            if let Some(source) = input.temporal_source {
                let channel = &source.sources[ordinal];
                if channel.use_id != child_id {
                    return Err(PhysicalScalarOccurrenceError::InvalidSource(
                        "temporal source use differs from its ordered argument edge",
                    ));
                }
                let node = input
                    .definitions
                    .ok_or(PhysicalScalarOccurrenceError::InvalidSource(
                        "missing temporal source arena",
                    ))?
                    .get(definition)
                    .ok_or(PhysicalScalarOccurrenceError::InvalidSource(
                        "missing temporal source definition",
                    ))?;
                source_channels.push(novarocks_functions::TemporalSourceChannel {
                    role: channel.role,
                    context: child.context,
                    value_type: &node.ty,
                });
            }
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
    let regexp_count_pattern_source = if function.function_id.as_str()
        == "builtin.scalar/regexp_count/v1"
    {
        let definitions = input
            .definitions
            .ok_or(PhysicalScalarOccurrenceError::InvalidSource(
                "regexp_count requires the original emitted definition arena",
            ))?;
        Some(
            novarocks_physical_plan::regexp_count_pattern_source_observed(definitions, args, work)
                .map_err(|error| match error {
                    novarocks_physical_plan::TemporalSourceProjectionError::Control(cause) => {
                        PhysicalScalarOccurrenceError::Control(cause)
                    }
                    novarocks_physical_plan::TemporalSourceProjectionError::Invalid(message) => {
                        PhysicalScalarOccurrenceError::InvalidSource(message)
                    }
                })?,
        )
    } else {
        None
    };
    let call = CallEffectInput {
        context: invocation.context,
        argument_uses: match input.temporal_source {
            Some(source) => novarocks_functions::CallArgumentUses::TemporalSources {
                facts: &source.facts,
                channels: &source_channels,
            },
            None => match regexp_count_pattern_source {
                Some(source) => novarocks_functions::CallArgumentUses::RegexpCountPattern {
                    source,
                    channels: &argument_uses,
                },
                None => novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
            },
        },
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
        regexp_count_pattern_source,
        temporal_source: input.temporal_source.cloned(),
    };
    work.flush()?;
    Ok(FreshPhysicalScalarOccurrence {
        frozen,
        preparation,
    })
}
