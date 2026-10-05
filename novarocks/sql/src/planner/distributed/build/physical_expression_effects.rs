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

//! Fresh runtime-occurrence effects from actual ordered source/use edges.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_functions::{
    ArithmeticPrepareError, CastOperation, CastPrepareError, ComparisonPrepareError,
    ConstantPolicy, PreparedArithmeticRecipe, PreparedCastRecipe, PreparedComparisonRecipe,
    PreparedNullSafeComparisonRecipe, ScopedExpressionEffects,
};
use novarocks_physical_plan::{
    BinaryOperator, ConstantPools, ExprId, ExprKind, ExprNode, Fragment, FrozenPhysicalCall,
    PhysicalRootUses, RootUseBindingError, UnaryOperator,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EffectContractError, ExpressionUseId, PureCompileControl, SemanticParameterError,
    SemanticParameterRef, SemanticParameterValue, SemanticParameters, ValueTypeError,
    arrow_data_types_exact_observed,
};

use super::{
    physical_call_arguments::{PhysicalArgumentError, author_physical_argument_observed},
    physical_scalar_occurrences::{
        PhysicalScalarOccurrenceError, PhysicalScalarOccurrenceInput,
        prepare_physical_scalar_occurrence_observed,
    },
    physical_scalar_requests::{
        AuthoredPhysicalScalarRequest, PhysicalScalarRequestError,
        author_physical_scalar_request_observed,
    },
    physical_window_occurrences::{
        PhysicalWindowOccurrenceError, PhysicalWindowOccurrenceInput,
        prepare_physical_window_occurrence_observed,
    },
    physical_window_requests::{
        AuthoredPhysicalWindowRequest, PhysicalWindowRequestError,
        author_physical_window_request_observed,
    },
};
use crate::compiler::SqlFunctionCatalog;

#[cfg(test)]
#[path = "physical_expression_effects_tests.rs"]
mod tests;

/// SQL-authored facts for one invocation. The actual immutable source loan
/// prevents a stale journal entry from authenticating rewritten contents by ID.
/// Definitions may have several uses with different scopes and policies.
pub(crate) struct PhysicalCallSourceScope<'a> {
    pub source: &'a ExprNode,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub environment: &'a [SemanticParameterRef],
    pub proof_scope: CallProofScope,
}

pub(crate) struct PhysicalExpressionEffectsInput<'a> {
    pub fragment: &'a Fragment,
    pub roots: &'a PhysicalRootUses,
    pub constants: &'a ConstantPools,
    pub parameters: &'a SemanticParameters,
    pub literal_policy: ConstantPolicy,
    pub call_scopes: &'a BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'a>>,
}

/// These are expression-use facts only. Relational/HOF lifecycles,
/// dead-definition capability admission and complete Package publication
/// remain mandatory separate owners; this result is not an executable plan.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalExpressionEffects {
    pub summaries: BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    pub calls: Vec<FrozenPhysicalCall>,
}

#[derive(Debug)]
pub(crate) enum PhysicalExpressionEffectsError {
    Control(CompileControlError),
    Roots(RootUseBindingError),
    Argument(PhysicalArgumentError),
    Request(PhysicalScalarRequestError),
    Scalar(PhysicalScalarOccurrenceError),
    WindowRequest(PhysicalWindowRequestError),
    Window(PhysicalWindowOccurrenceError),
    Cast(CastPrepareError),
    Arithmetic(ArithmeticPrepareError),
    Comparison(ComparisonPrepareError),
    Effects(EffectContractError),
    Parameter(SemanticParameterError),
    Type(ValueTypeError),
    MissingCallScope(ExpressionUseId),
    InvalidSource(&'static str),
    UnsupportedExpression(ExprId),
}
impl From<CompileControlError> for PhysicalExpressionEffectsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<EffectContractError> for PhysicalExpressionEffectsError {
    fn from(error: EffectContractError) -> Self {
        Self::Effects(error)
    }
}
impl From<SemanticParameterError> for PhysicalExpressionEffectsError {
    fn from(error: SemanticParameterError) -> Self {
        Self::Parameter(error)
    }
}
impl From<ValueTypeError> for PhysicalExpressionEffectsError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
macro_rules! source_error {
    ($error:ident, $variant:ident) => {
        impl From<$error> for PhysicalExpressionEffectsError {
            fn from(error: $error) -> Self {
                match error {
                    $error::Control(cause) => Self::Control(cause),
                    other => Self::$variant(other),
                }
            }
        }
    };
}
source_error!(RootUseBindingError, Roots);
source_error!(PhysicalArgumentError, Argument);
source_error!(PhysicalScalarRequestError, Request);
source_error!(PhysicalScalarOccurrenceError, Scalar);
source_error!(PhysicalWindowRequestError, WindowRequest);
source_error!(PhysicalWindowOccurrenceError, Window);

enum AuthoredPhysicalExpressionRequest<'a> {
    Scalar(AuthoredPhysicalScalarRequest<'a>),
    Window(AuthoredPhysicalWindowRequest<'a>),
}
macro_rules! recipe_error {
    ($error:ty, $variant:ident) => {
        impl From<$error> for PhysicalExpressionEffectsError {
            fn from(error: $error) -> Self {
                match error.control_error() {
                    Some(cause) => Self::Control(cause),
                    None => Self::$variant(error),
                }
            }
        }
    };
}
recipe_error!(CastPrepareError, Cast);
recipe_error!(ArithmeticPrepareError, Arithmetic);
recipe_error!(ComparisonPrepareError, Comparison);

/// Revalidate actual root/control correspondence, then walk borrowed use edges
/// child-first. Numeric identity order has no dependency meaning. Each use is
/// authored once; shared definitions retain distinct invocation contexts.
/// Static requests are shared only within this original immutable source loan.
/// No old summary, legacy binding effect or package default supplies a fact.
///
/// Caller admission covers maps, bounded stack, request/type clones and owner
/// preparation coexistence. Cooperative work and fallible Vec storage are not
/// a funding grant. Primary nested control returns without a completion check;
/// success and ordinary errors observe the original footer.
pub(crate) fn author_physical_expression_effects_observed(
    input: PhysicalExpressionEffectsInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalExpressionEffects, PhysicalExpressionEffectsError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        work.flush()?;
        input.roots.validate_fragment(input.fragment, control)?;
        work.flush()?;
        let flow = input.roots.flow();
        let definitions = input.fragment.expressions();
        // Exact scope coverage is checked before any owner preparation. A
        // matching topology is not proof of matching immutable source contents.
        for (&id, scope) in input.call_scopes {
            let source = flow
                .uses()
                .get(&id)
                .and_then(|use_| definitions.get(use_.definition));
            work.step()?;
            let matching_source = source.is_some_and(|source| {
                std::ptr::eq(source, scope.source)
                    && matches!(
                        source.kind,
                        ExprKind::FunctionCall { .. } | ExprKind::WindowCall { .. }
                    )
            });
            work.step()?;
            if !matching_source {
                return Err(PhysicalExpressionEffectsError::InvalidSource(
                    "call scope does not loan this actual source invocation",
                ));
            }
        }
        let mut requests = BTreeMap::new();
        for (&id, invocation) in flow.uses() {
            let source = definitions.get(invocation.definition).ok_or(
                PhysicalExpressionEffectsError::InvalidSource("missing source definition"),
            )?;
            work.step()?;
            if matches!(
                source.kind,
                ExprKind::FunctionCall { .. } | ExprKind::WindowCall { .. }
            ) {
                let has_scope = input.call_scopes.contains_key(&id);
                work.step()?;
                if !has_scope {
                    return Err(PhysicalExpressionEffectsError::MissingCallScope(id));
                }
                if let std::collections::btree_map::Entry::Vacant(entry) = requests.entry(source.id)
                {
                    let request = if matches!(source.kind, ExprKind::FunctionCall { .. }) {
                        AuthoredPhysicalExpressionRequest::Scalar(
                            author_physical_scalar_request_observed(
                                source,
                                definitions,
                                input.constants,
                                input.literal_policy,
                                &mut work,
                            )?,
                        )
                    } else {
                        AuthoredPhysicalExpressionRequest::Window(
                            author_physical_window_request_observed(
                                source,
                                input.fragment,
                                input.constants,
                                input.literal_policy,
                                &mut work,
                            )?,
                        )
                    };
                    entry.insert(request);
                    work.step()?;
                }
            }
            work.step()?;
        }
        let mut summaries = BTreeMap::new();
        let mut calls = Vec::new();
        let mut active = BTreeSet::new();
        let mut stack = Vec::new();
        work.flush()?;
        stack
            .try_reserve_exact(novarocks_type_contract::MAX_CONTROL_DEPTH)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        calls
            .try_reserve_exact(input.call_scopes.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for &root in flow.root_use_ids() {
            active.insert(root);
            stack.push((root, 0usize));
            work.step()?;
            while let Some(&(id, ordinal)) = stack.last() {
                let invocation =
                    flow.uses()
                        .get(&id)
                        .ok_or(PhysicalExpressionEffectsError::InvalidSource(
                            "missing actual use",
                        ))?;
                work.step()?;
                if let Some(&child) = invocation.arguments.get(ordinal) {
                    stack.last_mut().expect("current borrowed frame").1 += 1;
                    let completed = summaries.contains_key(&child);
                    work.step()?;
                    let unique = !completed && active.insert(child);
                    work.step()?;
                    if !unique {
                        return Err(PhysicalExpressionEffectsError::InvalidSource(
                            "actual use edges contain a shared or cyclic invocation",
                        ));
                    }
                    let too_deep = stack.len() == novarocks_type_contract::MAX_CONTROL_DEPTH;
                    work.step()?;
                    if too_deep {
                        return Err(PhysicalExpressionEffectsError::InvalidSource(
                            "actual use depth exceeds the checked control bound",
                        ));
                    }
                    stack.push((child, 0));
                    work.step()?;
                    continue;
                }
                let source = definitions.get(invocation.definition).ok_or(
                    PhysicalExpressionEffectsError::InvalidSource("missing actual definition"),
                )?;
                work.step()?;
                let summary = if let Some(request) = requests.get(&source.id) {
                    let scope = &input.call_scopes[&id];
                    let (frozen, summary) = match request {
                        AuthoredPhysicalExpressionRequest::Scalar(request) => {
                            let fresh = prepare_physical_scalar_occurrence_observed(
                                PhysicalScalarOccurrenceInput {
                                    source,
                                    request,
                                    flow,
                                    use_id: id,
                                    child_effects: &summaries,
                                    parameters: input.parameters,
                                    environment: scope.environment,
                                    decimal_overflow_policy: scope.decimal_overflow_policy,
                                    proof_scope: scope.proof_scope,
                                },
                                functions,
                                &mut work,
                            )?;
                            (fresh.frozen, fresh.preparation.effects())
                        }
                        AuthoredPhysicalExpressionRequest::Window(request) => {
                            let fresh = prepare_physical_window_occurrence_observed(
                                PhysicalWindowOccurrenceInput {
                                    source,
                                    request,
                                    flow,
                                    use_id: id,
                                    child_effects: &summaries,
                                    parameters: input.parameters,
                                    environment: scope.environment,
                                    decimal_overflow_policy: scope.decimal_overflow_policy,
                                    proof_scope: scope.proof_scope,
                                },
                                functions,
                                &mut work,
                            )?;
                            (fresh.frozen, fresh.preparation.effects())
                        }
                    };
                    calls.push(frozen);
                    work.step()?;
                    summary
                } else {
                    let mut summary = primitive_own_effects(
                        source,
                        invocation.context,
                        &input,
                        control,
                        &mut work,
                    )?;
                    // Reuse the source's sole ordered child vocabulary, rather
                    // than reconstruct CASE ordinals or eager operand lists.
                    let mut next = 0;
                    source.kind.expression_references_observed(|definition| {
                        let child = invocation.arguments.get(next).copied();
                        work.step()?;
                        let child = child.ok_or(PhysicalExpressionEffectsError::InvalidSource(
                            "source argument has no actual invocation edge",
                        ))?;
                        let child_use = flow.uses().get(&child);
                        work.step()?;
                        if child_use.is_none_or(|use_| use_.definition != definition) {
                            return Err(PhysicalExpressionEffectsError::InvalidSource(
                                "source and actual argument order differ",
                            ));
                        }
                        let effects = summaries.get(&child).copied();
                        work.step()?;
                        summary = summary.join_control_argument(
                            effects.ok_or(PhysicalExpressionEffectsError::InvalidSource(
                                "actual child effects were not authored",
                            ))?,
                            flow,
                            next,
                        )?;
                        next += 1;
                        work.step()?;
                        Ok::<_, PhysicalExpressionEffectsError>(())
                    })?;
                    if next != invocation.arguments.len() {
                        return Err(PhysicalExpressionEffectsError::InvalidSource(
                            "actual invocation has extra argument edges",
                        ));
                    }
                    work.step()?;
                    summary
                };
                summaries.insert(id, summary);
                active.remove(&id);
                stack.pop();
                work.step()?;
            }
        }
        let complete =
            summaries.len() == flow.uses().len() && calls.len() == input.call_scopes.len();
        work.step()?;
        if !complete {
            return Err(PhysicalExpressionEffectsError::InvalidSource(
                "actual expression effects have incomplete coverage",
            ));
        }
        Ok(AuthoredPhysicalExpressionEffects { summaries, calls })
    })();
    if matches!(&result, Err(PhysicalExpressionEffectsError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn primitive_own_effects(
    source: &ExprNode,
    context: novarocks_type_contract::ExpressionEffectContext,
    input: &PhysicalExpressionEffectsInput<'_>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScopedExpressionEffects, PhysicalExpressionEffectsError> {
    let operand =
        |id| {
            input.fragment.expressions().get(id).ok_or(
                PhysicalExpressionEffectsError::InvalidSource("missing primitive operand"),
            )
        };
    let allow = |reference| match input.parameters.require(reference)? {
        SemanticParameterValue::AllowThrowException(value) => Ok(*value),
        _ => Err(PhysicalExpressionEffectsError::InvalidSource(
            "primitive parameter is not ALLOW_THROW_EXCEPTION",
        )),
    };
    work.step()?;
    Ok(match &source.kind {
        ExprKind::Value(_) => materialized_value_effects(context),
        ExprKind::Literal(_) | ExprKind::Constant(_) => {
            // Resolve/construct the actual leaf even when it is not a function
            // argument. NULL remains a checked constant, never a missing fact.
            author_physical_argument_observed(
                source,
                input.constants,
                input.literal_policy,
                CompilePhase::FunctionSpecialization,
                work,
            )?;
            ScopedExpressionEffects::pure_value(context)
        }
        ExprKind::Unary {
            op: UnaryOperator::Not,
            ..
        }
        | ExprKind::IsNull { .. }
        | ExprKind::Conjunction { .. }
        | ExprKind::Disjunction { .. }
        | ExprKind::Case { .. } => ScopedExpressionEffects::pure_value(context),
        ExprKind::Cast {
            expr,
            target,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            if !arrow_data_types_exact_observed::<PhysicalExpressionEffectsError>(
                target,
                &source.ty.data_type,
                || work.step().map_err(Into::into),
            )? {
                return Err(PhysicalExpressionEffectsError::InvalidSource(
                    "cast source result differs from its explicit target",
                ));
            }
            let child = operand(*expr);
            work.step()?;
            let child = child?;
            let allow = allow(*allow_throw_exception);
            work.step()?;
            let allow = allow?;
            work.flush()?;
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &child.ty,
                &source.ty,
                *decimal_overflow_policy,
                allow,
                control,
            )?;
            work.flush()?;
            recipe.own_effects(context)
        }
        ExprKind::Binary {
            op,
            left,
            right,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            let left = operand(*left);
            work.step()?;
            let left = left?;
            let right = operand(*right);
            work.step()?;
            let right = right?;
            if let Some(operator) = op.arithmetic_operator() {
                let reference =
                    allow_throw_exception.ok_or(PhysicalExpressionEffectsError::InvalidSource(
                        "arithmetic has no explicit ALLOW_THROW_EXCEPTION reference",
                    ));
                work.step()?;
                let allow = allow(reference?);
                work.step()?;
                let allow = allow?;
                work.flush()?;
                let recipe = PreparedArithmeticRecipe::try_new(
                    operator,
                    &left.ty,
                    &right.ty,
                    &source.ty,
                    *decimal_overflow_policy,
                    allow,
                    control,
                )?;
                work.flush()?;
                recipe.own_effects(context)
            } else if let Some(operator) = op.comparison_operator() {
                work.flush()?;
                let recipe =
                    PreparedComparisonRecipe::try_new(operator, &left.ty, &right.ty, control)?;
                work.flush()?;
                recipe.own_effects(context)
            } else if *op == BinaryOperator::EqForNull {
                work.flush()?;
                let recipe =
                    PreparedNullSafeComparisonRecipe::try_new(&left.ty, &right.ty, control)?;
                work.flush()?;
                recipe.own_effects(context)
            } else {
                return Err(PhysicalExpressionEffectsError::UnsupportedExpression(
                    source.id,
                ));
            }
        }
        _ => {
            return Err(PhysicalExpressionEffectsError::UnsupportedExpression(
                source.id,
            ));
        }
    })
}

/// The original Value leaf author is shared by authenticated materialized
/// Writer channels. Upstream evaluation effects stay with their own operators.
pub(crate) fn materialized_value_effects(
    context: novarocks_type_contract::ExpressionEffectContext,
) -> ScopedExpressionEffects {
    ScopedExpressionEffects::pure_value(context)
}
