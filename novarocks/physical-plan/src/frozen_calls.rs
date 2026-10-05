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

//! Complete call-site effect claims carried by one immutable fragment.
//!
//! This table validates occurrence coverage, context and public shape only.
//! It is not a proof from an installed owner: admission must recompute every
//! complete fact with the exact selected overload before preparing a kernel.

use crate::{
    AggregateBinding, BoundFunction, BoundTableFunction, ExprKind, Fragment, FragmentId, NodeId,
    NodeKind, PhysicalRootUses, RootUseBindingError, TopNReduction,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompileControlError,
    CompilePhase, DecimalOverflowPolicy, EffectContractError, EvaluationDemand,
    ExpressionEffectContext, ExpressionUseId, FunctionEffectDeclaration, FunctionKind,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl, SemanticParameterRef,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

/// A position in the actual frozen definition, not an overload name or a
/// flattened argument ordinal. Relational calls are not scalar invocations.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PhysicalCallSite {
    Expression(ExpressionUseId),
    Aggregate { node: NodeId, call: u32 },
    TopNState { node: NodeId, call: u32 },
    WriterPartial { node: NodeId, call: u32 },
    WriterFinal { node: NodeId, call: u32 },
    Table { node: NodeId },
}

/// Claims from the FE's exact owner. Expression contexts must match the
/// control graph. Relational use IDs are disjoint from expression use IDs;
/// their unguarded domain is explicitly present in the same domain table.
/// The compiler separately proves that domain's operator/lifecycle meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenPhysicalCall {
    pub site: PhysicalCallSite,
    pub context: ExpressionEffectContext,
    pub effects: CallEffects,
    /// Authored by this call's SQL scope, independently of effect claims and
    /// environment parameters. Compilation must not infer a package default.
    pub decimal_overflow_policy: DecimalOverflowPolicy,
}

/// Borrow the real binding; this table never copies a second signature DSL.
#[derive(Clone, Copy, Debug)]
pub enum PhysicalCallBinding<'a> {
    Scalar(&'a BoundFunction),
    Window {
        function: &'a BoundFunction,
        aggregate: Option<&'a AggregateBinding>,
    },
    Aggregate(&'a AggregateBinding),
    Table(&'a BoundTableFunction),
}
impl PhysicalCallBinding<'_> {
    pub const fn kind(self) -> FunctionKind {
        match self {
            Self::Scalar(function) | Self::Window { function, .. } => function.kind,
            Self::Aggregate(binding) => binding.function.kind,
            Self::Table(_) => FunctionKind::Table,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenFragmentCalls {
    fragment: FragmentId,
    entries: Arc<BTreeMap<PhysicalCallSite, FrozenPhysicalCall>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenCallError {
    MissingLegacyMetadata(crate::MissingLegacyBindingMetadata),
    Control(CompileControlError),
    Roots(RootUseBindingError),
    TooManyItems,
    WrongFragment,
    DuplicateSite,
    InvalidSite,
    MissingSite(PhysicalCallSite),
    WrongContext,
    SharedUse,
    InvalidDomain,
    WrongProofScope,
    WrongControl,
    InvalidEffects(EffectContractError),
    /// A claimed broadcast output repeats an invocation whose complete
    /// occurrence facts do not establish replica equivalence.
    ReplicaEquivalence(PhysicalCallSite),
}
impl fmt::Display for FrozenCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid frozen call table: {self:?}")
    }
}
impl std::error::Error for FrozenCallError {}
impl From<CompileControlError> for FrozenCallError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

// Ordinary validation exits observe their completed tail. A refusal from this
// or an exact nested owner is already primary and never receives another call.
fn finish_frozen_calls<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, FrozenCallError>,
) -> Result<T, FrozenCallError> {
    if matches!(&result, Err(FrozenCallError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

impl FrozenFragmentCalls {
    pub fn try_new(
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        calls: Vec<FrozenPhysicalCall>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            if calls.len() > MAX_CONTROL_USE_REFERENCES {
                return Err(FrozenCallError::TooManyItems);
            }
            let mut entries = BTreeMap::new();
            for call in calls {
                if entries.insert(call.site, call).is_some() {
                    return Err(FrozenCallError::DuplicateSite);
                }
                work.step()?;
            }
            let value = Self {
                fragment: fragment.id(),
                entries: Arc::new(entries),
            };
            work.flush()?;
            value.validate_fragment(fragment, uses, control)?;
            Ok(value)
        })();
        finish_frozen_calls(work, result)
    }

    /// Recheck claims against this package's current snapshot. Changing a
    /// selected overload with the same public shape can pass this structural
    /// check; exact owner refinement and frozen comparison remain mandatory.
    pub fn validate_fragment(
        &self,
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        control: &dyn PureCompileControl,
    ) -> Result<(), FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            if uses.roots().fragment() != fragment.id() {
                return Err(FrozenCallError::Roots(RootUseBindingError::WrongFragment));
            }
            if self.fragment != fragment.id() {
                return Err(FrozenCallError::WrongFragment);
            }
            work.flush()?;
            uses.validate_fragment(fragment, control)
                .map_err(|error| match error {
                    RootUseBindingError::Control(error) => FrozenCallError::Control(error),
                    error => FrozenCallError::Roots(error),
                })?;
            // The same invocation budget covers scalar graph references and real
            // relational calls. It does not multiply by an arena's maximum ID.
            let mut references = uses.flow().use_reference_count();
            let mut special_ids = BTreeSet::new();
            let mut visited = 0usize;
            visit_calls(fragment, uses, &mut work, |site, binding, work| {
                let call = self
                    .entries
                    .get(&site)
                    .ok_or(FrozenCallError::MissingSite(site))?;
                if matches!(binding, PhysicalCallBinding::Aggregate(_))
                    && binding.kind() != FunctionKind::Aggregate
                {
                    return Err(FrozenCallError::InvalidEffects(
                        EffectContractError::KindMismatch,
                    ));
                }
                match site {
                    PhysicalCallSite::Expression(id) => {
                        let invocation = &uses.flow().uses()[&id];
                        if call.context != invocation.context {
                            return Err(FrozenCallError::WrongContext);
                        }
                        match binding {
                            PhysicalCallBinding::Scalar(_) => {
                                if !call
                                    .effects
                                    .argument_control
                                    .matches_scalar_shape(invocation.control)
                                {
                                    return Err(FrozenCallError::WrongControl);
                                }
                            }
                            PhysicalCallBinding::Window { .. } => {
                                if !matches!(
                                    call.effects.argument_control,
                                    ArgumentControl::Aggregate | ArgumentControl::Window
                                ) {
                                    return Err(FrozenCallError::WrongControl);
                                }
                            }
                            PhysicalCallBinding::Aggregate(_) | PhysicalCallBinding::Table(_) => {
                                unreachable!()
                            }
                        }
                    }
                    PhysicalCallSite::Aggregate { .. }
                    | PhysicalCallSite::TopNState { .. }
                    | PhysicalCallSite::WriterPartial { .. }
                    | PhysicalCallSite::WriterFinal { .. }
                    | PhysicalCallSite::Table { .. } => {
                        references = references
                            .checked_add(1)
                            .ok_or(FrozenCallError::TooManyItems)?;
                        if references > MAX_CONTROL_USE_REFERENCES {
                            return Err(FrozenCallError::TooManyItems);
                        }
                        if call.context.demand != EvaluationDemand::Value {
                            return Err(FrozenCallError::WrongContext);
                        }
                        if uses.flow().uses().contains_key(&call.context.use_id)
                            || !special_ids.insert(call.context.use_id)
                        {
                            return Err(FrozenCallError::SharedUse);
                        }
                        let domain = uses
                            .flow()
                            .domains()
                            .get(&call.context.domain)
                            .ok_or(FrozenCallError::InvalidDomain)?;
                        if domain.parent.is_some() || domain.guard.is_some() {
                            return Err(FrozenCallError::InvalidDomain);
                        }
                    }
                }
                if call.effects.proof_scope != CallProofScope::Unconditional
                    && call.effects.proof_scope != CallProofScope::Domain(call.context.domain)
                {
                    return Err(FrozenCallError::WrongProofScope);
                }
                validate_public_effect_shape(binding.kind(), &call.effects, work)?;
                visited += 1;
                Ok(())
            })?;
            if visited != self.entries.len() {
                return Err(FrozenCallError::InvalidSite);
            }
            Ok(())
        })();
        finish_frozen_calls(work, result)
    }

    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn entries(&self) -> &BTreeMap<PhysicalCallSite, FrozenPhysicalCall> {
        &self.entries
    }
    pub fn parameter_references(&self) -> impl Iterator<Item = SemanticParameterRef> + '_ {
        self.entries
            .values()
            .flat_map(|call| call.effects.environment.iter().copied())
    }

    pub(crate) fn dynamic_items_observed(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<usize, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            let mut items = self.entries.len();
            for call in self.entries.values() {
                items = items
                    .checked_add(call.effects.environment.len())
                    .ok_or(FrozenCallError::TooManyItems)?;
                work.step()?;
            }
            Ok(items)
        })();
        finish_frozen_calls(work, result)
    }

    /// Lookup the real binding for this occurrence. This is not admission; the
    /// caller still validates this complete table against the same snapshot.
    pub fn binding<'a>(
        &self,
        fragment: &'a Fragment,
        uses: &PhysicalRootUses,
        site: PhysicalCallSite,
    ) -> Option<PhysicalCallBinding<'a>> {
        if self.fragment != fragment.id() || !self.entries.contains_key(&site) {
            return None;
        }
        match site {
            PhysicalCallSite::Expression(id) => {
                let definition = fragment
                    .expressions()
                    .get(uses.flow().uses().get(&id)?.definition)?;
                match &definition.kind {
                    ExprKind::FunctionCall { function, .. } => {
                        Some(PhysicalCallBinding::Scalar(function))
                    }
                    ExprKind::WindowCall {
                        function,
                        aggregate_binding,
                        ..
                    } => Some(PhysicalCallBinding::Window {
                        function,
                        aggregate: aggregate_binding.as_deref(),
                    }),
                    _ => None,
                }
            }
            PhysicalCallSite::Aggregate { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::Aggregate { calls, .. } => Some(PhysicalCallBinding::Aggregate(
                        &calls.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::TopNState { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::TopN {
                        reduction: TopNReduction::GroupedStates { calls, .. },
                        ..
                    } => Some(PhysicalCallBinding::Aggregate(
                        &calls.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::WriterPartial { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::TableWriter { target } => Some(PhysicalCallBinding::Aggregate(
                        &target.partial_aggregates.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::WriterFinal { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::TableFinish(finish) => Some(PhysicalCallBinding::Aggregate(
                        &finish.final_aggregates.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::Table { node } => match &fragment.nodes().get(&node)?.kind {
                NodeKind::TableFunction { function, .. } => {
                    Some(PhysicalCallBinding::Table(function))
                }
                _ => None,
            },
        }
    }
}

fn validate_public_effect_shape(
    kind: FunctionKind,
    effects: &CallEffects,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FrozenCallError> {
    // At most one actual reference for each closed environment key. This is
    // only shape validation, never a manufactured implementation declaration.
    let mut references = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for reference in &effects.environment {
        if !references.insert(*reference) || !keys.insert(reference.expected_key) {
            return Err(FrozenCallError::InvalidEffects(
                EffectContractError::InvalidEnvironmentReference,
            ));
        }
        work.step()?;
    }
    let shape = FunctionEffectDeclaration {
        value_stability: effects.value_stability,
        own_row_error: effects.own_row_error,
        failure_behavior: effects.failure_behavior,
        null_behavior: effects.null_behavior,
        argument_control: effects.argument_control,
        instance_state: effects.instance_state,
        observable_effects: effects.observable_effects,
        environment_dependencies: keys.into_iter().collect(),
    };
    shape
        .validate(kind)
        .map_err(FrozenCallError::InvalidEffects)
}

/// Visit each actual call occurrence using the caller's original work scope.
///
/// Root-use correspondence is rechecked against this fragment before any call
/// is exposed. Bindings are borrowed from that fragment, in the same order as
/// the frozen-call validator. This port does not infer effects or create call
/// facts, and root-use correspondence is not a content-identity proof.
///
/// The caller admits source, delegated root-validation scratch and callback
/// allocations, owns entry and finish, and propagates a typed control refusal
/// immediately without a later finish.
/// Delegated root validation retains the same control; no budget is reset.
pub fn visit_physical_calls_observed<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    uses: &PhysicalRootUses,
    work: &mut CompileCheckpoints<'_>,
    visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    if uses.roots().fragment() != fragment.id() {
        return Err(FrozenCallError::Roots(RootUseBindingError::WrongFragment).into());
    }
    work.flush().map_err(FrozenCallError::Control)?;
    let checked = uses
        .validate_fragment(fragment, work.control())
        .map_err(|error| match error {
            RootUseBindingError::Control(error) => FrozenCallError::Control(error),
            error => FrozenCallError::Roots(error),
        });
    if let Err(FrozenCallError::Control(error)) = checked {
        return Err(FrozenCallError::Control(error).into());
    }
    work.flush().map_err(FrozenCallError::Control)?;
    checked?;
    visit_calls(fragment, uses, work, visit)
}

fn visit_calls<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    uses: &PhysicalRootUses,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    for (id, invocation) in uses.flow().uses() {
        let expression = fragment
            .expressions()
            .get(invocation.definition)
            .ok_or(FrozenCallError::InvalidSite)?;
        match &expression.kind {
            ExprKind::FunctionCall { function, .. } => visit(
                PhysicalCallSite::Expression(*id),
                PhysicalCallBinding::Scalar(function),
                work,
            )?,
            ExprKind::WindowCall {
                function,
                aggregate_binding,
                ..
            } => visit(
                PhysicalCallSite::Expression(*id),
                PhysicalCallBinding::Window {
                    function,
                    aggregate: aggregate_binding.as_deref(),
                },
                work,
            )?,
            ExprKind::Value(_)
            | ExprKind::LambdaParameter { .. }
            | ExprKind::Literal(_)
            | ExprKind::Constant(_)
            | ExprKind::Unary { .. }
            | ExprKind::Binary { .. }
            | ExprKind::Conjunction { .. }
            | ExprKind::Disjunction { .. }
            | ExprKind::Lambda { .. }
            | ExprKind::Cast { .. }
            | ExprKind::IsNull { .. }
            | ExprKind::InList { .. }
            | ExprKind::Between { .. }
            | ExprKind::Like { .. }
            | ExprKind::Case { .. }
            | ExprKind::IsTruthValue { .. } => {}
        }
        work.step().map_err(FrozenCallError::Control)?;
    }
    visit_relational_calls_observed(fragment, work, visit)
}

/// Visit the original non-expression call sites before root-flow construction.
/// This is the same relational half of the complete physical call visitor:
/// zero-argument calls and materialized writer channels still have occurrences.
/// It borrows actual bindings only; structure, phase, selected owner facts and
/// runtime domains remain separate obligations. The caller admits the source
/// and callback allocations, owns entry/finish and preserves the first refusal.
pub fn visit_relational_calls_observed<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    for node in fragment.nodes().values() {
        match &node.kind {
            NodeKind::Aggregate { calls, .. } => {
                for (call, item) in calls.iter().enumerate() {
                    visit(
                        PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            } => {
                for (call, item) in calls.iter().enumerate() {
                    visit(
                        PhysicalCallSite::TopNState {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TableWriter { target } => {
                for (call, item) in target.partial_aggregates.iter().enumerate() {
                    visit(
                        PhysicalCallSite::WriterPartial {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TableFinish(finish) => {
                for (call, item) in finish.final_aggregates.iter().enumerate() {
                    visit(
                        PhysicalCallSite::WriterFinal {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TableFunction { function, .. } => visit(
                PhysicalCallSite::Table { node: node.id },
                PhysicalCallBinding::Table(function),
                work,
            )?,
            NodeKind::Scan { .. }
            | NodeKind::Filter { .. }
            | NodeKind::Project { .. }
            | NodeKind::HashJoin { .. }
            | NodeKind::NestLoopJoin { .. }
            | NodeKind::Sort { .. }
            | NodeKind::TopN {
                reduction: TopNReduction::Rows,
                ..
            }
            | NodeKind::Limit { .. }
            | NodeKind::Window(_)
            | NodeKind::SetOp { .. }
            | NodeKind::Values { .. }
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::GenerateSeries { .. }
            | NodeKind::AssertOneRow(_)
            | NodeKind::ChangeEventExpand { .. }
            | NodeKind::ExchangeSource { .. } => {}
        }
        work.step().map_err(FrozenCallError::Control)?;
    }
    Ok(())
}
fn ordinal(value: usize) -> Result<u32, FrozenCallError> {
    u32::try_from(value).map_err(|_| FrozenCallError::TooManyItems)
}

#[cfg(test)]
mod tests;

mod property_proof;
mod replica;
pub(crate) use property_proof::OccurrencePropertyProof;
pub use property_proof::{PropertyProofProjectionFacts, PropertyProofProjectionLimits};
