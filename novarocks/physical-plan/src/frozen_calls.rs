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
    CompilePhase, EffectContractError, EvaluationDemand, ExpressionEffectContext, ExpressionUseId,
    FunctionEffectDeclaration, FunctionKind, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
    SemanticParameterRef,
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

impl FrozenFragmentCalls {
    pub fn try_new(
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        calls: Vec<FrozenPhysicalCall>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
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
        value.validate_fragment(fragment, uses, control)?;
        work.finish()?;
        Ok(value)
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
        if uses.roots().fragment() != fragment.id() {
            return Err(FrozenCallError::Roots(RootUseBindingError::WrongFragment));
        }
        if self.fragment != fragment.id() {
            return Err(FrozenCallError::WrongFragment);
        }
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
        work.finish()?;
        Ok(())
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
        let mut items = self.entries.len();
        for call in self.entries.values() {
            items = items
                .checked_add(call.effects.environment.len())
                .ok_or(FrozenCallError::TooManyItems)?;
            work.step()?;
        }
        work.finish()?;
        Ok(items)
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

fn visit_calls<'a>(
    fragment: &'a Fragment,
    uses: &PhysicalRootUses,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenCallError>,
) -> Result<(), FrozenCallError> {
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
        work.step()?;
    }
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
                    work.step()?;
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
                    work.step()?;
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
                    work.step()?;
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
                    work.step()?;
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
        work.step()?;
    }
    Ok(())
}
fn ordinal(value: usize) -> Result<u32, FrozenCallError> {
    u32::try_from(value).map_err(|_| FrozenCallError::TooManyItems)
}

#[cfg(test)]
mod tests;
