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

//! Fresh exact-owner table facts from independent actual argument roots.

use std::{collections::BTreeMap, sync::Arc};

use novarocks_functions::{
    CallEffectInput, PureCallPreparation, PureCallSpecialization, PureKernelAbi,
    ScopedExpressionEffects,
};
use novarocks_physical_plan::{
    ExpressionRootRole, ExpressionRootSite, Fragment, FrozenPhysicalCall, NodeKind,
    PhysicalCallSite, PhysicalNode,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, EffectContractError, EvaluationDemand, ExpressionEffectContext,
    ExpressionEffects, ExpressionUseId, FunctionKind, SemanticParameterRef, SemanticParameters,
};

use super::{
    expression_occurrences::{AuthoredPhysicalOccurrences, ExpressionOccurrenceError},
    physical_table_requests::AuthoredPhysicalTableRequest,
};
use crate::compiler::SqlFunctionCatalog;

#[cfg(test)]
#[path = "physical_table_occurrences_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) enum PhysicalTableOccurrenceError {
    Control(CompileControlError),
    Function(ExpressionOccurrenceError),
    Effects(EffectContractError),
    MissingChildEffects(ExpressionUseId),
    InvalidSource(&'static str),
    UnsupportedAbi(PureKernelAbi),
}
impl From<CompileControlError> for PhysicalTableOccurrenceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExpressionOccurrenceError> for PhysicalTableOccurrenceError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Function(other),
        }
    }
}
impl From<EffectContractError> for PhysicalTableOccurrenceError {
    fn from(error: EffectContractError) -> Self {
        Self::Effects(error)
    }
}

/// The original immutable table node, static request and occurrence topology
/// are separate loans. Source policy/environment/proof are explicit; legacy
/// binding semantics and a package default do not supply them.
pub(crate) struct PhysicalTableOccurrenceInput<'a> {
    pub fragment: &'a Fragment,
    pub source: &'a PhysicalNode,
    pub request: &'a AuthoredPhysicalTableRequest<'a>,
    pub occurrences: &'a AuthoredPhysicalOccurrences,
    pub child_effects: &'a BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

#[derive(Debug)]
pub(crate) struct FreshPhysicalTableOccurrence {
    pub frozen: FrozenPhysicalCall,
    pub preparation: PureCallSpecialization,
}

/// Prepare the installed TableV1 owner for this exact physical occurrence.
/// Each ordered argument is a separate root/domain, including repeated ExprId
/// definitions. Verify its original site, definition, demand and scoped summary
/// before joining neutral conservative effects into the table context. This
/// does not relocate a child use or authorize hoisting/caching across domains.
///
/// The caller admits all source, temporary argument-use storage, opaque owner
/// lookups/clones and retained preparation before entry. It also validates the
/// complete same immutable source/root-flow pairing and supplies child-first
/// summaries. This leaf neither reconstructs a flow nor creates a runtime
/// table instance, lateral/LEFT assembly, output layout or package authority.
/// The caller owns entry and success/ordinary footer; primary control returns
/// directly through the existing nested failure mapper.
pub(crate) fn prepare_physical_table_occurrence_observed(
    input: PhysicalTableOccurrenceInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalTableOccurrence, PhysicalTableOccurrenceError> {
    let actual = input.fragment.nodes().get(&input.source.id);
    work.step()?;
    let same_source = actual.is_some_and(|node| std::ptr::eq(node, input.source))
        && std::ptr::eq(input.source, input.request.source())
        && input.occurrences.root_uses.roots().fragment() == input.fragment.id();
    work.step()?;
    if !same_source {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "table occurrence and request have different actual source loans",
        ));
    }
    let NodeKind::TableFunction {
        function,
        arguments,
        ..
    } = &input.source.kind
    else {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "table occurrence source is not an actual table function",
        ));
    };
    let same_function = std::ptr::eq(function, input.request.function())
        && input.request.request().arguments.len() == arguments.len();
    work.step()?;
    if !same_function {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "table occurrence has different binding or channel coverage",
        ));
    }
    let site = PhysicalCallSite::Table {
        node: input.source.id,
    };
    let context = table_context(input.occurrences, site, work)?;
    work.flush()?;
    let declaration = functions
        .pure_overload_declaration_observed(
            &function.function_id,
            FunctionKind::Table,
            &input.request.selected().overload,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let abi = declaration.implementation().abi;
    let correct = abi == PureKernelAbi::TableV1;
    work.step()?;
    if !correct {
        return Err(PhysicalTableOccurrenceError::UnsupportedAbi(abi));
    }
    let correct_control = declaration.effects().argument_control == ArgumentControl::Table;
    work.step()?;
    if !correct_control {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "installed table owner has different argument control",
        ));
    }
    work.flush()?;
    let mut argument_uses = Vec::new();
    argument_uses
        .try_reserve_exact(arguments.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let roots = &input.occurrences.root_uses;
    let mut children = ExpressionEffects::PURE_VALUE;
    for (ordinal, &definition) in arguments.iter().enumerate() {
        let ordinal = u32::try_from(ordinal);
        work.step()?;
        let ordinal = ordinal.map_err(|_| {
            PhysicalTableOccurrenceError::InvalidSource(
                "table argument ordinal is not representable",
            )
        })?;
        let root_site = ExpressionRootSite {
            node: input.source.id,
            role: ExpressionRootRole::TableFunctionArgument { argument: ordinal },
        };
        let root = roots.roots().sites().get(&root_site);
        work.step()?;
        let matching = root
            .is_some_and(|root| root.expr == definition && root.demand == EvaluationDemand::Value);
        work.step()?;
        if !matching {
            return Err(PhysicalTableOccurrenceError::InvalidSource(
                "ordered table argument root differs from actual source",
            ));
        }
        let use_id = roots.bindings().get(&root_site).copied();
        work.step()?;
        let use_id = use_id.ok_or(PhysicalTableOccurrenceError::InvalidSource(
            "ordered table argument root has no actual use",
        ))?;
        let invocation = roots.flow().uses().get(&use_id);
        work.step()?;
        let matching = invocation.is_some_and(|invocation| {
            invocation.definition == definition
                && invocation.context.use_id == use_id
                && invocation.context.demand == EvaluationDemand::Value
        });
        work.step()?;
        if !matching {
            return Err(PhysicalTableOccurrenceError::InvalidSource(
                "table argument invocation differs from its original root",
            ));
        }
        let invocation = invocation.expect("checked original invocation");
        let domain = roots.flow().domains().get(&invocation.context.domain);
        work.step()?;
        let unguarded = invocation.context.domain != context.domain
            && domain.is_some_and(|domain| domain.parent.is_none() && domain.guard.is_none());
        work.step()?;
        if !unguarded {
            return Err(PhysicalTableOccurrenceError::InvalidSource(
                "table argument root has a guarded or absent domain",
            ));
        }
        let summary = input.child_effects.get(&use_id).copied();
        work.step()?;
        let summary = summary.ok_or(PhysicalTableOccurrenceError::MissingChildEffects(use_id))?;
        // for_use retains the child's exact context. Only the neutral result
        // of this check is joined; no scoped cross-domain join is permitted.
        let actual_effects = summary.for_use(invocation.context);
        work.step()?;
        children = children.join(actual_effects?);
        argument_uses.push(Some(use_id));
        work.step()?;
    }
    let call = CallEffectInput {
        context,
        argument_uses: &argument_uses,
        function_id: &function.function_id,
        kind: FunctionKind::Table,
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
            PureCallPreparation::Table {
                arguments: ScopedExpressionEffects::primitive(context, children),
            },
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let frozen = FrozenPhysicalCall {
        site,
        context,
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalTableOccurrence {
        frozen,
        preparation,
    })
}

/// Relational contexts are not flow invocations. Keep their original authored
/// disjoint use namespace and unguarded Value domain, with exactly one context
/// for this actual Table site. The complete topology remains caller-validated.
fn table_context(
    occurrences: &AuthoredPhysicalOccurrences,
    site: PhysicalCallSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ExpressionEffectContext, PhysicalTableOccurrenceError> {
    let mut found = None;
    for &(actual_site, context) in &occurrences.relational_contexts {
        let matching = actual_site == site;
        work.step()?;
        if matching {
            let repeated = found.replace(context).is_some();
            work.step()?;
            if repeated {
                return Err(PhysicalTableOccurrenceError::InvalidSource(
                    "table site has repeated relational contexts",
                ));
            }
        }
    }
    let context = found.ok_or(PhysicalTableOccurrenceError::InvalidSource(
        "table site has no original relational context",
    ))?;
    for &(other_site, other) in &occurrences.relational_contexts {
        let colliding = other_site != site
            && (other.use_id == context.use_id || other.domain == context.domain);
        work.step()?;
        if colliding {
            return Err(PhysicalTableOccurrenceError::InvalidSource(
                "table context shares another relational use identity or domain",
            ));
        }
    }
    let flow = occurrences.root_uses.flow();
    for invocation in flow.uses().values() {
        let shares_domain = invocation.context.domain == context.domain;
        work.step()?;
        if shares_domain {
            return Err(PhysicalTableOccurrenceError::InvalidSource(
                "table context borrows an expression invocation domain",
            ));
        }
    }
    let expression_collision = flow.uses().contains_key(&context.use_id);
    work.step()?;
    let domain = flow.domains().get(&context.domain);
    work.step()?;
    let matching = !expression_collision
        && context.demand == EvaluationDemand::Value
        && domain.is_some_and(|domain| domain.parent.is_none() && domain.guard.is_none());
    work.step()?;
    if !matching {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "table context differs from original disjoint Value domain",
        ));
    }
    Ok(context)
}
