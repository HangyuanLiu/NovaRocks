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

//! Complete fresh call-table composition for supported physical lifecycles.

use std::collections::BTreeMap;

use novarocks_functions::{ConstantPolicy, ScopedExpressionEffects};
use novarocks_physical_plan::{
    AggregateCall, ConstantPools, Fragment, FrozenCallError, FrozenFragmentCalls, NodeKind,
    PhysicalCallBinding, PhysicalCallSite, PhysicalNode, visit_relational_calls_observed,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    ExpressionUseId, MAX_CONTROL_USE_REFERENCES, PureCompileControl, SemanticParameterRef,
    SemanticParameters,
};

use super::{
    expression_occurrences::{AuthoredPhysicalOccurrences, ExpressionOccurrenceError},
    physical_aggregate_occurrences::{
        PhysicalAggregateOccurrenceError, PhysicalAggregateOccurrenceInput,
        prepare_physical_aggregate_occurrence_observed,
    },
    physical_aggregate_requests::{
        PhysicalAggregateRequestError, author_physical_aggregate_update_request_observed,
    },
    physical_expression_effects::{
        PhysicalCallSourceScope, PhysicalExpressionEffectsError, PhysicalExpressionEffectsInput,
        author_physical_expression_effects_observed,
    },
    physical_table_occurrences::{
        PhysicalTableOccurrenceError, PhysicalTableOccurrenceInput,
        prepare_physical_table_occurrence_observed,
    },
    physical_table_requests::{PhysicalTableRequestError, author_physical_table_request_observed},
};
use crate::compiler::SqlFunctionCatalog;

pub(crate) struct PhysicalRelationalCallSourceScope<'a> {
    pub source: &'a PhysicalNode,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub environment: &'a [SemanticParameterRef],
    pub proof_scope: CallProofScope,
}

pub(crate) struct PhysicalFragmentEffectsInput<'a> {
    pub fragment: &'a Fragment,
    pub occurrences: &'a AuthoredPhysicalOccurrences,
    pub constants: &'a ConstantPools,
    pub parameters: &'a SemanticParameters,
    pub literal_policy: ConstantPolicy,
    pub expression_scopes: &'a BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'a>>,
    pub relational_scopes: &'a BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
}

/// This immutable call table covers every actual call, or composition fails.
/// It is not complete source admission or a PhysicalPlan/Package publication.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalFragmentEffects {
    pub summaries: BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    pub calls: FrozenFragmentCalls,
}

#[derive(Debug)]
pub(crate) enum PhysicalFragmentEffectsError {
    Control(CompileControlError),
    Occurrence(ExpressionOccurrenceError),
    Expressions(PhysicalExpressionEffectsError),
    AggregateRequest(PhysicalAggregateRequestError),
    Aggregate(PhysicalAggregateOccurrenceError),
    TableRequest(PhysicalTableRequestError),
    Table(PhysicalTableOccurrenceError),
    Calls(FrozenCallError),
    MissingScope(PhysicalCallSite),
    InvalidSource(&'static str),
    UnsupportedLifecycle(PhysicalCallSite),
}
impl From<CompileControlError> for PhysicalFragmentEffectsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
macro_rules! nested_error {
    ($error:ident, $variant:ident) => {
        impl From<$error> for PhysicalFragmentEffectsError {
            fn from(error: $error) -> Self {
                match error {
                    $error::Control(cause) => Self::Control(cause),
                    other => Self::$variant(other),
                }
            }
        }
    };
}
nested_error!(ExpressionOccurrenceError, Occurrence);
nested_error!(PhysicalExpressionEffectsError, Expressions);
nested_error!(PhysicalAggregateRequestError, AggregateRequest);
nested_error!(PhysicalAggregateOccurrenceError, Aggregate);
nested_error!(PhysicalTableRequestError, TableRequest);
nested_error!(PhysicalTableOccurrenceError, Table);
nested_error!(FrozenCallError, Calls);

/// Compose the original expression roots first and then each actual relational
/// call from the sole visitor. Missing, extra or foreign source scopes refuse;
/// unsupported lifecycle owners never receive a fabricated scalar/table path.
/// Only the original complete call-table author publishes exact coverage.
///
/// Source/static/result/all-definition and global plan gates remain mandatory.
/// The caller supplies the same immutable topology and admits opaque maps,
/// clones, retained facts, nested validation scratch and preparation coexistence.
/// This component grants no runtime capability, storage allowance or host grant.
/// Success and ordinary failures observe the footer; nested control is primary.
pub(crate) fn author_physical_fragment_effects_observed(
    input: PhysicalFragmentEffectsInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        if input.relational_scopes.len() > MAX_CONTROL_USE_REFERENCES {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        // Validate exact relational scope coverage against the original call
        // grammar before preparing any expression or relational owner.
        let mut relational_count = 0usize;
        visit_relational_calls_observed(input.fragment, &mut work, |site, binding, work| {
            let node = match site {
                PhysicalCallSite::Table { node }
                | PhysicalCallSite::Aggregate { node, .. }
                | PhysicalCallSite::TopNState { node, .. } => node,
                _ => return Err(PhysicalFragmentEffectsError::UnsupportedLifecycle(site)),
            };
            let actual = input.fragment.nodes().get(&node);
            work.step()?;
            let scope = input.relational_scopes.get(&site);
            work.step()?;
            let scope = scope.ok_or(PhysicalFragmentEffectsError::MissingScope(site))?;
            let original_binding = match binding {
                PhysicalCallBinding::Table(function) => {
                    matches!(&scope.source.kind, NodeKind::TableFunction { function: original, .. } if std::ptr::eq(original, function))
                }
                PhysicalCallBinding::Aggregate(binding) => aggregate_source(scope.source, site)
                    .is_ok_and(|source| std::ptr::eq(&source.binding, binding)),
                _ => false,
            };
            let same =
                actual.is_some_and(|actual| std::ptr::eq(actual, scope.source)) && original_binding;
            work.step()?;
            if !same {
                return Err(PhysicalFragmentEffectsError::InvalidSource(
                    "relational scope does not loan this actual source call",
                ));
            }
            relational_count = relational_count
                .checked_add(1)
                .filter(|&count| count <= MAX_CONTROL_USE_REFERENCES)
                .ok_or(CompileControlError::ResourceExhausted)?;
            work.step()?;
            Ok(())
        })?;
        let exact_scopes = relational_count == input.relational_scopes.len();
        work.step()?;
        if !exact_scopes {
            return Err(PhysicalFragmentEffectsError::InvalidSource(
                "relational source scopes contain an extra call site",
            ));
        }
        work.flush()?;
        let expressions = author_physical_expression_effects_observed(
            PhysicalExpressionEffectsInput {
                fragment: input.fragment,
                roots: &input.occurrences.root_uses,
                constants: input.constants,
                parameters: input.parameters,
                literal_policy: input.literal_policy,
                call_scopes: input.expression_scopes,
            },
            functions,
            control,
        )?;
        work.flush()?;
        let mut calls = expressions.calls;
        let total = calls
            .len()
            .checked_add(relational_count)
            .filter(|&count| count <= MAX_CONTROL_USE_REFERENCES);
        work.step()?;
        total.ok_or(CompileControlError::ResourceExhausted)?;
        work.flush()?;
        calls
            .try_reserve_exact(relational_count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
        visit_relational_calls_observed(input.fragment, &mut work, |site, binding, work| {
            let scope = input.relational_scopes.get(&site);
            work.step()?;
            let scope = scope.ok_or(PhysicalFragmentEffectsError::MissingScope(site))?;
            let frozen = match binding {
                PhysicalCallBinding::Table(_) => {
                    let request = author_physical_table_request_observed(
                        scope.source,
                        input.fragment,
                        input.constants,
                        input.literal_policy,
                        work,
                    )?;
                    let fresh = prepare_physical_table_occurrence_observed(
                        PhysicalTableOccurrenceInput {
                            fragment: input.fragment,
                            source: scope.source,
                            request: &request,
                            occurrences: input.occurrences,
                            child_effects: &expressions.summaries,
                            parameters: input.parameters,
                            environment: scope.environment,
                            decimal_overflow_policy: scope.decimal_overflow_policy,
                            proof_scope: scope.proof_scope,
                        },
                        functions,
                        work,
                    )?;
                    fresh.frozen
                }
                PhysicalCallBinding::Aggregate(_) => {
                    let source = aggregate_source(scope.source, site)?;
                    let request = author_physical_aggregate_update_request_observed(
                        source,
                        scope.source,
                        site,
                        input.fragment,
                        input.constants,
                        input.literal_policy,
                        work,
                    )?;
                    let fresh = prepare_physical_aggregate_occurrence_observed(
                        PhysicalAggregateOccurrenceInput {
                            fragment: input.fragment,
                            node: scope.source,
                            source,
                            request: &request,
                            occurrences: input.occurrences,
                            child_effects: &expressions.summaries,
                            parameters: input.parameters,
                            environment: scope.environment,
                            decimal_overflow_policy: scope.decimal_overflow_policy,
                            proof_scope: scope.proof_scope,
                        },
                        functions,
                        work,
                    )?;
                    fresh.frozen
                }
                _ => return Err(PhysicalFragmentEffectsError::UnsupportedLifecycle(site)),
            };
            calls.push(frozen);
            work.step()?;
            Ok(())
        })?;
        work.flush()?;
        let calls = FrozenFragmentCalls::try_new(
            input.fragment,
            &input.occurrences.root_uses,
            calls,
            control,
        )?;
        work.flush()?;
        Ok(AuthoredPhysicalFragmentEffects {
            summaries: expressions.summaries,
            calls,
        })
    })();
    if matches!(&result, Err(PhysicalFragmentEffectsError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// Borrow the original call selected by the sole visitor's node/ordinal site.
/// The sealed request still verifies this exact call/node/source association.
fn aggregate_source(
    node: &PhysicalNode,
    site: PhysicalCallSite,
) -> Result<&AggregateCall, PhysicalFragmentEffectsError> {
    let (ordinal, calls) = match (site, &node.kind) {
        (PhysicalCallSite::Aggregate { node: id, call }, NodeKind::Aggregate { calls, .. })
            if id == node.id =>
        {
            (call, calls)
        }
        (
            PhysicalCallSite::TopNState { node: id, call },
            NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                ..
            },
        ) if id == node.id => (call, calls),
        _ => {
            return Err(PhysicalFragmentEffectsError::InvalidSource(
                "aggregate site differs from the original node kind",
            ));
        }
    };
    usize::try_from(ordinal)
        .ok()
        .and_then(|ordinal| calls.get(ordinal))
        .ok_or(PhysicalFragmentEffectsError::InvalidSource(
            "aggregate site has no original ordered call",
        ))
}
