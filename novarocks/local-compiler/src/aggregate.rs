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

//! Lower one admitted physical Aggregate into the local aggregate owner and
//! prepare each of its calls from the exact frozen relational facts.
//!
//! Every call is prepared through its installed pure owner with the frozen
//! binding, the original request, the frozen call effects and the actual
//! effects of its argument roots. No implementation is selected by name: the
//! display name copied into the local call is a diagnostic tag only. The
//! physical grouping guarantee travels as a compiled program fact, because a
//! node's finalization does not imply it.

use crate::{
    assert_rows::reserve_vec, expressions::ExpressionLoweringError, lowering::FragmentCompileError,
};
use arrow_schema::{Field, Schema};
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregateOverloadIdentity,
    AggregatePreparationOptions, CallEffectInput, FunctionBindingSelection, FunctionResultType,
    PureCallPreparation, PureCallSpecialization, PureEngineFunctionCatalog,
    ResolvedAggregateSignature, ScopedExpressionEffects,
};
use novarocks_local_program::{
    CompiledAggregate, CompiledAggregateGrouping, ProgramCallSite, ProgramExprId, ProgramNodeId,
    ProgramNodeKind, StaticAggregateCall, StaticAggregateOrder, StaticLayout,
};
use novarocks_physical_plan::{
    AggregateCall, AggregateGrouping, AggregatePhase, ExprId, ExpressionRootRole,
    ExpressionRootSite, FragmentPackage, FunctionArgumentType, NodeId, NodeKind,
    PhysicalCallDefinition, PhysicalCallSite, PhysicalNode,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, ExpressionEffects, ExpressionUseId,
    FunctionKind, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, sync::Arc};

/// The local kernel phase of one physical call phase. Sequence identity stays
/// a checked physical fact; the local owner needs only the lifecycle.
pub(crate) const fn kernel_phase(phase: AggregatePhase) -> AggregateKernelPhase {
    match phase {
        AggregatePhase::Single => AggregateKernelPhase::Single,
        AggregatePhase::Partial { .. } => AggregateKernelPhase::Partial,
        AggregatePhase::Intermediate { .. } => AggregateKernelPhase::Intermediate,
        AggregatePhase::Final { .. } => AggregateKernelPhase::Final,
    }
}

pub(crate) struct LoweredAggregate {
    pub kind: ProgramNodeKind,
    pub layout: StaticLayout,
    pub fact: CompiledAggregate,
}

/// Lower one Aggregate whose output is its group values followed by one value
/// per call, in call order. Function ORDER BY has no local owner yet and is an
/// explicit refusal.
pub(crate) fn lower_aggregate(
    package: &FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
) -> Result<LoweredAggregate, FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(package, node, input, expressions, slots, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn lower_core(
    package: &FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<LoweredAggregate, FragmentCompileError> {
    let NodeKind::Aggregate {
        group_by,
        calls,
        grouping,
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid("Aggregate kind differs"));
    };
    let width = group_by
        .len()
        .checked_add(calls.len())
        .ok_or(FragmentCompileError::Invalid("aggregate width exhausted"))?;
    if node.inputs.len() != 1 || node.output.columns.len() != width || slots.len() != width {
        return Err(FragmentCompileError::Invalid(
            "aggregate input, output or channel width differs",
        ));
    }
    // The output is exactly the group values followed by the call outputs.
    let expected = group_by
        .iter()
        .map(|(_, value)| *value)
        .chain(calls.iter().map(|call| call.output));
    for (actual, expected) in node.output.columns.iter().zip(expected) {
        work.step()?;
        if *actual != expected {
            return Err(FragmentCompileError::Invalid(
                "aggregate output is not its group values followed by its call outputs",
            ));
        }
    }
    let values = package.fragment().values();
    let definitions = package.fragment().expressions();
    let mut keys = Vec::new();
    reserve_vec(&mut keys, group_by.len(), work)?;
    for (expression, value) in group_by.iter() {
        let definition = definitions
            .get(*expression)
            .ok_or(FragmentCompileError::Invalid("missing group definition"))?;
        let output = values
            .get(value)
            .ok_or(FragmentCompileError::Invalid("missing group value"))?;
        work.flush()?;
        let same = definition
            .ty
            .exactly_equals_observed::<FragmentCompileError>(&output.ty, || {
                work.step().map_err(Into::into)
            })?;
        if !same {
            return Err(FragmentCompileError::Invalid(
                "group value type differs from its definition",
            ));
        }
        keys.push(
            *expressions
                .get(expression)
                .ok_or(FragmentCompileError::Invalid(
                    "missing lowered group definition",
                ))?,
        );
        work.step()?;
    }
    let mut functions = Vec::new();
    reserve_vec(&mut functions, calls.len(), work)?;
    let mut finalizing = None;
    let mut merging = true;
    for call in calls.iter() {
        let phase = call.binding.phase;
        if finalizing.replace(phase.produces_final_result()) == Some(!phase.produces_final_result())
        {
            return Err(FragmentCompileError::Invalid(
                "aggregate mixes finalizing and non-finalizing calls",
            ));
        }
        merging &= !phase.consumes_logical_arguments();
        functions.push(lower_call(node, call, expressions, work)?);
        work.step()?;
    }
    // A finalizing call requires a complete grouping. A node without calls
    // finalizes exactly when its groups are complete.
    let need_finalize = finalizing.unwrap_or(*grouping == AggregateGrouping::Complete);
    if need_finalize && *grouping != AggregateGrouping::Complete {
        return Err(FragmentCompileError::Invalid(
            "finalizing aggregate has a partial grouping",
        ));
    }
    let result = package
        .result()
        .filter(|result| result.output == node.output);
    let mut fields: Vec<Field> = Vec::new();
    reserve_vec(&mut fields, width, work)?;
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        let ty = &values
            .get(value)
            .ok_or(FragmentCompileError::Invalid(
                "missing aggregate output type",
            ))?
            .ty;
        // Full result labels are authoritative only when this aggregate's
        // entire ordered output is the result port.
        let name = match result {
            Some(result) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing aggregate result label",
                    ))?;
                field.alias.as_deref().unwrap_or(&field.name).to_owned()
            }
            None => format!("local_aggregate_{}_{}", node.id.get(), ordinal),
        };
        work.flush()?;
        let field = ty.try_to_field(name);
        work.flush()?;
        fields.push(field?);
        work.step()?;
    }
    work.flush()?;
    let layout = StaticLayout::try_new_for_compile(
        Arc::new(Schema::new(fields)),
        Arc::from(slots),
        work.control(),
    )?;
    work.flush()?;
    Ok(LoweredAggregate {
        kind: ProgramNodeKind::Aggregate {
            input,
            group_by: keys,
            functions,
            need_finalize,
            input_is_intermediate: !calls.is_empty() && merging,
            // Runtime-filter graphs are refused before lowering; streaming
            // pre-aggregation is a local freedom this compiler does not take.
            topn_filters: Vec::new(),
            streaming_preaggregation_mode: None,
        },
        layout,
        fact: CompiledAggregate {
            grouping: match grouping {
                AggregateGrouping::Partial => CompiledAggregateGrouping::Partial,
                AggregateGrouping::Complete => CompiledAggregateGrouping::Complete,
            },
        },
    })
}

fn lower_call(
    node: &PhysicalNode,
    call: &AggregateCall,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<StaticAggregateCall, FragmentCompileError> {
    if !call.order_by.is_empty() {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "aggregate function ORDER BY",
        });
    }
    let binding = &call.binding;
    let function = &binding.function;
    if function.kind != FunctionKind::Aggregate {
        return Err(FragmentCompileError::Invalid(
            "aggregate call binds a non-aggregate function",
        ));
    }
    let mut inputs = Vec::new();
    reserve_vec(&mut inputs, call.arguments.len(), work)?;
    for argument in call.arguments.iter() {
        inputs.push(
            *expressions
                .get(argument)
                .ok_or(FragmentCompileError::Invalid(
                    "missing lowered aggregate argument",
                ))?,
        );
        work.step()?;
    }
    let mut argument_types = Vec::new();
    reserve_vec(&mut argument_types, function.argument_types.len(), work)?;
    for argument in function.argument_types.iter() {
        let FunctionArgumentType::Value(value) = argument else {
            return Err(FragmentCompileError::Invalid(
                "aggregate channel is not a value",
            ));
        };
        work.flush()?;
        argument_types.push(value.data_type.clone());
        work.step()?;
    }
    let overload =
        AggregateOverloadIdentity::try_new(function.overload.as_str()).map_err(|error| {
            FragmentCompileError::Owner {
                phase: "aggregate overload identity",
                error: Box::new(error),
            }
        })?;
    work.flush()?;
    let name: Arc<str> = Arc::from(function.function_id.as_str());
    work.flush()?;
    Ok(StaticAggregateCall {
        // A diagnostic tag only; the frozen preparation owns the call.
        name,
        inputs,
        input_is_intermediate: !binding.phase.consumes_logical_arguments(),
        types: None,
        order: StaticAggregateOrder {
            is_asc_order: Vec::new(),
            nulls_first: Vec::new(),
            is_distinct: call.distinct,
            group_concat_max_len: None,
        },
        resolved: ResolvedAggregateSignature {
            overload,
            argument_types,
            intermediate_type: binding.intermediate_type.data_type.clone(),
            output_type: function.result_type.data_type.clone(),
            state_format: binding.state_format.clone(),
        },
    })
}

/// Prepare every frozen Aggregate call of the fragment through its exact
/// installed owner. `effects` holds the prepared effects of every expression
/// occurrence, so each argument root contributes its actual effects; `nodes`
/// names the local node each physical aggregate lowers to.
pub(crate) fn prepare_aggregate_calls(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    nodes: &BTreeMap<NodeId, ProgramNodeId>,
    tokens: &mut BTreeMap<ProgramCallSite, PureCallSpecialization>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionLoweringError> {
    let mut prepared = 0usize;
    for node in package.fragment().nodes().values() {
        work.step()?;
        let NodeKind::Aggregate { calls, .. } = &node.kind else {
            continue;
        };
        let local = *nodes.get(&node.id).ok_or(ExpressionLoweringError::Invalid(
            "aggregate node has no local node",
        ))?;
        for (ordinal, call) in calls.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                ExpressionLoweringError::Invalid("aggregate call ordinal exhausted")
            })?;
            let token = prepare_call(package, functions, effects, node, ordinal, call, work)?;
            if tokens
                .insert(
                    ProgramCallSite::Aggregate {
                        node: local,
                        call: ordinal,
                    },
                    token,
                )
                .is_some()
            {
                return Err(ExpressionLoweringError::Invalid(
                    "aggregate call site is prepared twice",
                ));
            }
            prepared += 1;
            work.step()?;
        }
    }
    // Every frozen relational aggregate claim names one actual call.
    let mut frozen = 0usize;
    for site in package.calls().entries().keys() {
        if matches!(site, PhysicalCallSite::Aggregate { .. }) {
            frozen += 1;
        }
        work.step()?;
    }
    if frozen != prepared {
        return Err(ExpressionLoweringError::Invalid(
            "frozen aggregate calls differ from the actual aggregate calls",
        ));
    }
    Ok(())
}

fn prepare_call(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    node: &PhysicalNode,
    ordinal: u32,
    call: &AggregateCall,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PureCallSpecialization, ExpressionLoweringError> {
    let site = PhysicalCallSite::Aggregate {
        node: node.id,
        call: ordinal,
    };
    if !call.order_by.is_empty() {
        return Err(ExpressionLoweringError::UnsupportedCall(site));
    }
    let binding = &call.binding;
    let function = &binding.function;
    if function.kind != FunctionKind::Aggregate {
        return Err(ExpressionLoweringError::Invalid(
            "aggregate call binds a non-aggregate function",
        ));
    }
    let frozen = package
        .calls()
        .entries()
        .get(&site)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing frozen aggregate call",
        ))?;
    let source_request = package
        .fragment()
        .call_requests()
        .get(PhysicalCallDefinition::Relational(site));
    work.step()?;
    let source_request = source_request.ok_or(ExpressionLoweringError::Invalid(
        "missing original aggregate call request",
    ))?;
    let request = crate::original_requests::materialize_call_request_observed(
        source_request,
        package.constants(),
        work,
    )?;
    work.flush()?;
    // The selection is the frozen binding itself, never a re-resolution.
    let selection = Arc::new(FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Scalar(function.result_type.clone()),
        aggregate: Some(AggregateBindingSelection {
            state_argument_contract: binding.state_argument_contract,
            intermediate_type: binding.intermediate_type.clone(),
            state_format: binding.state_format.clone(),
        }),
    });
    work.flush()?;
    functions.metadata().validate_frozen_selection(
        &function.function_id,
        FunctionKind::Aggregate,
        selection.as_ref(),
        request.request(),
        work.control(),
    )?;
    work.flush()?;
    let phase = kernel_phase(binding.phase);
    let roots = package.expression_uses();
    let flow = roots.flow();
    // Each argument root is an independent unguarded Value occurrence whose
    // definition is the call's ordered argument.
    let mut argument_uses = Vec::new();
    reserve_vec(&mut argument_uses, call.arguments.len(), work)?;
    let mut contexts = Vec::new();
    reserve_vec(&mut contexts, call.arguments.len(), work)?;
    let mut children = ExpressionEffects::PURE_VALUE;
    for (argument, &definition) in call.arguments.iter().enumerate() {
        let argument = u32::try_from(argument).map_err(|_| {
            ExpressionLoweringError::Invalid("aggregate argument ordinal exhausted")
        })?;
        let root = ExpressionRootSite {
            node: node.id,
            role: ExpressionRootRole::AggregateArgument {
                call: ordinal,
                argument,
            },
        };
        let use_id = *roots
            .bindings()
            .get(&root)
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate argument root has no actual use",
            ))?;
        let invocation = flow
            .uses()
            .get(&use_id)
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate argument root has no invocation",
            ))?;
        let domain = flow.domains().get(&invocation.context.domain);
        work.step()?;
        let exact = invocation.definition == definition
            && invocation.context.use_id == use_id
            && invocation.context.demand == EvaluationDemand::Value
            && domain.is_some_and(|domain| domain.parent.is_none() && domain.guard.is_none());
        if !exact {
            return Err(ExpressionLoweringError::Invalid(
                "aggregate argument root differs from its actual call argument",
            ));
        }
        let summary = effects
            .get(&use_id)
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate argument effects were not prepared",
            ))?;
        children = children.join(summary.for_use(invocation.context)?);
        argument_uses.push(Some(use_id));
        contexts.push(invocation.context);
        work.step()?;
    }
    let (argument_uses_shape, options) = if phase.consumes_logical_arguments() {
        (
            novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
            AggregatePreparationOptions {
                phase,
                distinct: call.distinct,
                order_keys: Arc::from([]),
                state_input_type: None,
            },
        )
    } else {
        // A merging phase reads exactly one state root of its own type.
        let ([state], [context]) = (call.arguments.as_ref(), contexts.as_slice()) else {
            return Err(ExpressionLoweringError::Invalid(
                "merging aggregate phase requires exactly one state argument",
            ));
        };
        if call.distinct {
            return Err(ExpressionLoweringError::Invalid(
                "merging aggregate phase repeats DISTINCT",
            ));
        }
        let state_type = &package
            .fragment()
            .expressions()
            .get(*state)
            .ok_or(ExpressionLoweringError::Invalid(
                "missing aggregate state definition",
            ))?
            .ty;
        work.flush()?;
        (
            novarocks_functions::CallArgumentUses::AggregateMerge {
                phase,
                state_context: *context,
                state_input_type: state_type,
            },
            AggregatePreparationOptions {
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: Some(state_type.clone()),
            },
        )
    };
    work.flush()?;
    let input = CallEffectInput {
        context: frozen.context,
        argument_uses: argument_uses_shape,
        function_id: &function.function_id,
        kind: FunctionKind::Aggregate,
        selected: selection.as_ref(),
        request: request.request(),
        environment: &frozen.effects.environment,
        parameters: package.parameters(),
        decimal_overflow_policy: frozen.decimal_overflow_policy,
        proof_scope: frozen.effects.proof_scope,
    };
    let token = functions.prepare_frozen(
        input,
        Arc::clone(&selection),
        &frozen.effects,
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::primitive(frozen.context, children),
            options,
        },
        work.control(),
    )?;
    work.flush()?;
    Ok(token)
}
