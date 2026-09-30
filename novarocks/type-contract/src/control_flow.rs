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

//! Flat expression use/control domains. A shared definition is not a shared
//! invocation: every use retains its own demand and strong branch domain.
use crate::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, PureCompileControl,
};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::Arc,
};

/// Borrowed membership view of one validated immutable definition arena.
/// Count and membership must describe the same backing. Both operations are
/// bounded and perform no I/O; the view is never retained in a control graph.
pub trait ExpressionDefinitionMembership<D> {
    fn definition_count(&self) -> usize;
    fn contains_definition(&self, definition: D) -> bool;
}

/// Existing expression-arena and invocation limits, shared by both projections.
pub const MAX_CONTROL_DEFINITIONS: usize = 262_144;
pub const MAX_CONTROL_DEPTH: usize = 96;
pub const MAX_CONTROL_USE_REFERENCES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlShape {
    Eager,
    TypeOnly,
    /// The exact higher-order call has already established the invocation
    /// guard. Ordered local definitions require Value, followed by the body
    /// which inherits the wrapper's demand. A lambda with no locals has one
    /// argument: its body.
    LambdaBody,
    Conjunction,
    Disjunction,
    If,
    Coalesce,
    Case {
        simple: bool,
        arms: u32,
        has_else: bool,
    },
    HigherOrder {
        body_ordinal: u32,
        body_demand: crate::EvaluationDemand,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardKind {
    IfThen,
    IfElse,
    CoalesceAfterNull { ordinal: u32 },
    CaseWhen { arm: u32 },
    CaseThen { arm: u32 },
    CaseElse,
    LambdaInvocation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DomainGuard {
    pub owner: ExpressionUseId,
    pub kind: GuardKind,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpressionEvaluationDomain {
    pub id: EvaluationDomainId,
    pub parent: Option<EvaluationDomainId>,
    pub guard: Option<DomainGuard>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpressionInvocation<D> {
    pub context: ExpressionEffectContext,
    pub definition: D,
    pub control: ControlShape,
    pub arguments: Box<[ExpressionUseId]>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpressionControlFlow<D> {
    domains: Arc<BTreeMap<EvaluationDomainId, ExpressionEvaluationDomain>>,
    uses: Arc<BTreeMap<ExpressionUseId, ExpressionInvocation<D>>>,
    roots: Arc<[ExpressionUseId]>,
    use_references: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpressionControlFlowError {
    Control(CompileControlError),
    TooManyItems,
    DuplicateIdentity,
    InvalidReference,
    InvalidGuard,
    InvalidControlShape,
    InvalidDemand,
    SharedUse,
    Cycle,
    TooDeep,
}
impl fmt::Display for ExpressionControlFlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid expression control flow: {self:?}")
    }
}
impl std::error::Error for ExpressionControlFlowError {}
impl From<CompileControlError> for ExpressionControlFlowError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl<D: Copy> ExpressionControlFlow<D> {
    /// The caller supplies the count and exact membership of an already
    /// validated immutable definition arena. Sparse maps use their actual
    /// keys; dense arenas check ordinals without allocating an ID set. This
    /// borrowed O(1)/O(log n) lookup is called only during validation and is
    /// never retained by the graph. It performs no I/O or runtime lookup.
    /// The caller also checks the exact definition/control correspondence.
    /// No instance Selection or mutable state is stored here.
    pub fn try_new(
        domains: Vec<ExpressionEvaluationDomain>,
        uses: Vec<ExpressionInvocation<D>>,
        definitions: &impl ExpressionDefinitionMembership<D>,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ExpressionControlFlowError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let definition_count = definitions.definition_count();
        if domains.len() > MAX_CONTROL_DEFINITIONS
            || uses.len() > MAX_CONTROL_DEFINITIONS
            || definition_count > MAX_CONTROL_DEFINITIONS
        {
            return Err(ExpressionControlFlowError::TooManyItems);
        }
        let mut domain_index = BTreeMap::new();
        for domain in domains {
            if domain_index.insert(domain.id, domain).is_some() {
                return Err(ExpressionControlFlowError::DuplicateIdentity);
            }
            work.step()?;
        }
        let mut use_index = BTreeMap::new();
        let mut references = uses.len();
        if references > MAX_CONTROL_USE_REFERENCES {
            return Err(ExpressionControlFlowError::TooManyItems);
        }
        for value in uses {
            references = references
                .checked_add(value.arguments.len())
                .ok_or(ExpressionControlFlowError::TooManyItems)?;
            if references > MAX_CONTROL_USE_REFERENCES {
                return Err(ExpressionControlFlowError::TooManyItems);
            }
            if !definitions.contains_definition(value.definition)
                || !domain_index.contains_key(&value.context.domain)
            {
                return Err(ExpressionControlFlowError::InvalidReference);
            }
            if use_index.insert(value.context.use_id, value).is_some() {
                return Err(ExpressionControlFlowError::DuplicateIdentity);
            }
            work.step()?;
        }
        let mut children: BTreeMap<EvaluationDomainId, Vec<_>> = BTreeMap::new();
        let mut ready = VecDeque::new();
        let mut depths = BTreeMap::new();
        for domain in domain_index.values() {
            match (domain.parent, domain.guard) {
                (None, None) => {
                    depths.insert(domain.id, 1usize);
                    ready.push_back(domain.id);
                }
                (Some(parent), Some(guard)) => {
                    let owner = use_index
                        .get(&guard.owner)
                        .ok_or(ExpressionControlFlowError::InvalidReference)?;
                    if parent != owner.context.domain || !domain_index.contains_key(&parent) {
                        return Err(ExpressionControlFlowError::InvalidGuard);
                    }
                    children.entry(parent).or_default().push(domain.id);
                }
                _ => return Err(ExpressionControlFlowError::InvalidGuard),
            }
            work.step()?;
        }
        let mut visited = 0usize;
        while let Some(parent) = ready.pop_front() {
            visited += 1;
            let depth = depths[&parent];
            if depth > MAX_CONTROL_DEPTH {
                return Err(ExpressionControlFlowError::TooDeep);
            }
            for child in children.get(&parent).into_iter().flatten() {
                depths.insert(*child, depth + 1);
                ready.push_back(*child);
                work.step()?;
            }
            work.step()?;
        }
        if visited != domain_index.len() {
            return Err(ExpressionControlFlowError::Cycle);
        }
        let mut indegrees: BTreeMap<ExpressionUseId, usize> = BTreeMap::new();
        for id in use_index.keys() {
            indegrees.insert(*id, 0);
            work.step()?;
        }
        let mut used_guards = BTreeMap::new();
        for owner in use_index.values() {
            validate_arity(owner.control, owner.arguments.len())?;
            for (ordinal, argument) in owner.arguments.iter().enumerate() {
                let child = use_index
                    .get(argument)
                    .ok_or(ExpressionControlFlowError::InvalidReference)?;
                if child.context.demand
                    != control_argument_demand(
                        owner.control,
                        owner.arguments.len(),
                        ordinal,
                        owner.context.demand,
                    )
                {
                    return Err(ExpressionControlFlowError::InvalidDemand);
                }
                let expected = control_argument_guard(owner.control, ordinal);
                let domain = &domain_index[&child.context.domain];
                match expected {
                    None if child.context.domain == owner.context.domain => {}
                    Some(kind)
                        if domain.parent == Some(owner.context.domain)
                            && domain.guard
                                == Some(DomainGuard {
                                    owner: owner.context.use_id,
                                    kind,
                                }) =>
                    {
                        used_guards.insert(domain.id, ());
                    }
                    _ => return Err(ExpressionControlFlowError::InvalidGuard),
                }
                let degree = indegrees.get_mut(argument).unwrap();
                *degree += 1;
                if *degree > 1 {
                    return Err(ExpressionControlFlowError::SharedUse);
                }
                work.step()?;
            }
            work.step()?;
        }
        for domain in domain_index.values() {
            if domain.guard.is_some() && !used_guards.contains_key(&domain.id) {
                return Err(ExpressionControlFlowError::InvalidGuard);
            }
            work.step()?;
        }
        let mut ready = VecDeque::new();
        let mut use_depths = BTreeMap::new();
        let mut roots = Vec::new();
        for (id, degree) in &indegrees {
            if *degree == 0 {
                roots.push(*id);
                ready.push_back(*id);
                use_depths.insert(*id, 1usize);
            }
            work.step()?;
        }
        let mut visited = 0usize;
        while let Some(id) = ready.pop_front() {
            visited += 1;
            let depth = use_depths[&id];
            if depth > MAX_CONTROL_DEPTH {
                return Err(ExpressionControlFlowError::TooDeep);
            }
            for child in &use_index[&id].arguments {
                let degree = indegrees.get_mut(child).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    ready.push_back(*child);
                    use_depths.insert(*child, depth + 1);
                }
                work.step()?;
            }
            work.step()?;
        }
        if visited != use_index.len() {
            return Err(ExpressionControlFlowError::Cycle);
        }
        work.finish()?;
        Ok(Self {
            domains: Arc::new(domain_index),
            uses: Arc::new(use_index),
            roots: roots.into(),
            use_references: references,
        })
    }
    /// Every invocation with no parent argument edge, in stable ID order.
    /// A physical projection must bind these roots to exact operator sites.
    pub fn root_use_ids(&self) -> &[ExpressionUseId] {
        &self.roots
    }
    pub fn domains(&self) -> &BTreeMap<EvaluationDomainId, ExpressionEvaluationDomain> {
        &self.domains
    }
    pub fn uses(&self) -> &BTreeMap<ExpressionUseId, ExpressionInvocation<D>> {
        &self.uses
    }
    /// Invocation entries plus their ordered argument references, counted by
    /// the same bounded constructor. No unobserved edge scan is needed later.
    pub const fn use_reference_count(&self) -> usize {
        self.use_references
    }
}
fn validate_arity(shape: ControlShape, count: usize) -> Result<(), ExpressionControlFlowError> {
    let valid = match shape {
        ControlShape::Eager => true,
        ControlShape::TypeOnly => count == 0,
        ControlShape::LambdaBody => count > 0,
        ControlShape::Conjunction | ControlShape::Disjunction => count > 0,
        ControlShape::If => count == 3,
        ControlShape::Coalesce => count > 0,
        ControlShape::HigherOrder { body_ordinal, .. } => (body_ordinal as usize) < count,
        ControlShape::Case {
            simple,
            arms,
            has_else,
        } => {
            arms > 0
                && (arms as usize).checked_mul(2).and_then(|count| {
                    count.checked_add(usize::from(simple) + usize::from(has_else))
                }) == Some(count)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ExpressionControlFlowError::InvalidControlShape)
    }
}
fn control_argument_guard(shape: ControlShape, ordinal: usize) -> Option<GuardKind> {
    match shape {
        ControlShape::Eager
        | ControlShape::TypeOnly
        | ControlShape::LambdaBody
        | ControlShape::Conjunction
        | ControlShape::Disjunction => None,
        ControlShape::If => match ordinal {
            0 => None,
            1 => Some(GuardKind::IfThen),
            _ => Some(GuardKind::IfElse),
        },
        ControlShape::Coalesce => (ordinal > 0).then_some(GuardKind::CoalesceAfterNull {
            ordinal: ordinal as u32,
        }),
        ControlShape::HigherOrder { body_ordinal, .. } => {
            (ordinal == body_ordinal as usize).then_some(GuardKind::LambdaInvocation)
        }
        ControlShape::Case { simple, arms, .. } => {
            if simple && ordinal == 0 {
                return None;
            }
            let ordinal = ordinal - usize::from(simple);
            if ordinal == arms as usize * 2 {
                Some(GuardKind::CaseElse)
            } else if ordinal.is_multiple_of(2) {
                Some(GuardKind::CaseWhen {
                    arm: (ordinal / 2) as u32,
                })
            } else {
                Some(GuardKind::CaseThen {
                    arm: (ordinal / 2) as u32,
                })
            }
        }
    }
}

fn control_argument_demand(
    shape: ControlShape,
    count: usize,
    ordinal: usize,
    owner: crate::EvaluationDemand,
) -> crate::EvaluationDemand {
    use crate::EvaluationDemand::{TruthOnly, Value};
    match shape {
        ControlShape::Eager | ControlShape::TypeOnly | ControlShape::Coalesce => Value,
        ControlShape::Conjunction | ControlShape::Disjunction => owner,
        ControlShape::LambdaBody => {
            if ordinal + 1 == count {
                owner
            } else {
                Value
            }
        }
        ControlShape::If => {
            if ordinal == 0 {
                TruthOnly
            } else {
                owner
            }
        }
        ControlShape::HigherOrder {
            body_ordinal,
            body_demand,
        } => {
            if ordinal == body_ordinal as usize {
                body_demand
            } else {
                Value
            }
        }
        ControlShape::Case { simple, arms, .. } => {
            if simple && ordinal == 0 {
                return Value;
            }
            let ordinal = ordinal - usize::from(simple);
            if ordinal == arms as usize * 2 || ordinal % 2 == 1 {
                owner
            } else if simple {
                Value
            } else {
                TruthOnly
            }
        }
    }
}

/// Exact control semantics of one argument occurrence. Validate arity and
/// ordinal before deriving guards; callers cannot manufacture an else arm or
/// an out-of-range lambda/body use from an unchecked control shape.
pub fn control_argument_semantics(
    shape: ControlShape,
    argument_count: usize,
    ordinal: usize,
    owner: crate::EvaluationDemand,
) -> Result<(crate::EvaluationDemand, Option<GuardKind>), ExpressionControlFlowError> {
    if argument_count > MAX_CONTROL_USE_REFERENCES {
        return Err(ExpressionControlFlowError::TooManyItems);
    }
    validate_arity(shape, argument_count)?;
    if ordinal >= argument_count {
        return Err(ExpressionControlFlowError::InvalidReference);
    }
    Ok((
        control_argument_demand(shape, argument_count, ordinal, owner),
        control_argument_guard(shape, ordinal),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvaluationDemand, MAX_UNOBSERVED_COMPILE_WORK};
    use std::{cell::Cell, sync::Mutex};

    #[test]
    fn lambda_wrapper_preserves_body_demand_without_another_guard() {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            assert_eq!(
                control_argument_semantics(ControlShape::LambdaBody, 1, 0, demand),
                Ok((demand, None))
            );
        }
        assert_eq!(
            control_argument_semantics(ControlShape::LambdaBody, 0, 0, EvaluationDemand::TruthOnly),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for ordinal in 0..3 {
                assert_eq!(
                    control_argument_semantics(ControlShape::LambdaBody, 3, ordinal, demand),
                    Ok((
                        if ordinal == 2 {
                            demand
                        } else {
                            EvaluationDemand::Value
                        },
                        None
                    ))
                );
            }
        }
        assert_eq!(
            control_argument_semantics(ControlShape::LambdaBody, 1, 1, EvaluationDemand::TruthOnly),
            Err(ExpressionControlFlowError::InvalidReference)
        );
    }

    struct Definitions {
        count: usize,
        lookups: Cell<usize>,
    }
    impl ExpressionDefinitionMembership<u32> for Definitions {
        fn definition_count(&self) -> usize {
            self.count
        }
        fn contains_definition(&self, id: u32) -> bool {
            self.lookups.set(self.lookups.get() + 1);
            matches!(id, 1000 | u32::MAX)
        }
    }
    struct Control {
        observations: Mutex<Vec<(CompilePhase, u32)>>,
        fail: Option<CompileControlError>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.observations.lock().unwrap().push((phase, units));
            if units > 0
                && let Some(failure) = self.fail
            {
                return Err(failure);
            }
            Ok(())
        }
    }
    fn control(fail: Option<CompileControlError>) -> Control {
        Control {
            observations: Mutex::default(),
            fail,
        }
    }
    fn definitions(count: usize) -> Definitions {
        Definitions {
            count,
            lookups: Cell::new(0),
        }
    }
    fn root() -> ExpressionEvaluationDomain {
        ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(u32::MAX),
            parent: None,
            guard: None,
        }
    }
    fn invocation(id: u32, definition: u32) -> ExpressionInvocation<u32> {
        ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(id),
                domain: root().id,
                demand: EvaluationDemand::Value,
            },
            definition,
            control: ControlShape::Eager,
            arguments: Box::default(),
        }
    }
    #[test]
    fn sparse_definition_membership_and_exact_roots_do_not_use_maximum_id() {
        let mut parent = invocation(50, u32::MAX);
        parent.arguments = vec![ExpressionUseId::new(3)].into_boxed_slice();
        let graph = ExpressionControlFlow::try_new(
            vec![root()],
            vec![parent, invocation(3, 1000), invocation(u32::MAX, 1000)],
            &definitions(2),
            CompilePhase::Validate,
            &control(None),
        )
        .unwrap();
        assert_eq!(
            graph.root_use_ids(),
            &[ExpressionUseId::new(50), ExpressionUseId::new(u32::MAX)]
        );
        assert_eq!(graph.uses().len(), 3);
        assert_eq!(graph.use_reference_count(), 4);
        assert_eq!(
            ExpressionControlFlow::try_new(
                vec![root()],
                vec![invocation(0, 1)],
                &definitions(2),
                CompilePhase::Validate,
                &control(None)
            )
            .unwrap_err(),
            ExpressionControlFlowError::InvalidReference
        );
    }
    #[test]
    fn excessive_definition_count_is_rejected_before_membership_lookup() {
        let definitions = definitions(MAX_CONTROL_DEFINITIONS + 1);
        assert_eq!(
            ExpressionControlFlow::try_new(
                vec![root()],
                vec![invocation(0, 1000)],
                &definitions,
                CompilePhase::Validate,
                &control(None)
            )
            .unwrap_err(),
            ExpressionControlFlowError::TooManyItems
        );
        assert_eq!(definitions.lookups.get(), 0);
    }
    #[test]
    fn uses_and_argument_edges_share_one_bounded_expansion_budget() {
        let count = MAX_CONTROL_USE_REFERENCES / 2;
        let mut parent = invocation(u32::MAX, 1000);
        parent.arguments = (0..count as u32).map(ExpressionUseId::new).collect();
        let mut uses = (0..count as u32)
            .map(|id| invocation(id, 1000))
            .collect::<Vec<_>>();
        uses.push(parent);
        let definitions = definitions(2);
        assert_eq!(
            ExpressionControlFlow::try_new(
                vec![root()],
                uses,
                &definitions,
                CompilePhase::Validate,
                &control(None)
            )
            .unwrap_err(),
            ExpressionControlFlowError::TooManyItems
        );
        // The oversized parent was rejected without walking its child edges.
        assert_eq!(definitions.lookups.get(), count);
    }
    #[test]
    fn control_failures_preserve_phase_and_bounded_work_classification() {
        for phase in [CompilePhase::Validate, CompilePhase::LowerProgram] {
            for failure in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let owner = control(Some(failure));
                let uses = (0..1000).map(|id| invocation(id, 1000)).collect();
                assert_eq!(
                    ExpressionControlFlow::try_new(
                        vec![root()],
                        uses,
                        &definitions(2),
                        phase,
                        &owner
                    )
                    .unwrap_err(),
                    ExpressionControlFlowError::Control(failure)
                );
                assert_eq!(
                    *owner.observations.lock().unwrap(),
                    vec![(phase, 0), (phase, MAX_UNOBSERVED_COMPILE_WORK)]
                );
            }
        }
    }
    #[test]
    fn unchecked_argument_shapes_and_ordinals_cannot_create_guard_domains() {
        assert_eq!(
            control_argument_semantics(ControlShape::If, 3, 3, EvaluationDemand::Value),
            Err(ExpressionControlFlowError::InvalidReference)
        );
        assert_eq!(
            control_argument_semantics(ControlShape::If, 4, 3, EvaluationDemand::Value),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
        assert_eq!(
            control_argument_semantics(
                ControlShape::Case {
                    simple: true,
                    arms: 1,
                    has_else: false
                },
                3,
                2,
                EvaluationDemand::TruthOnly
            ),
            Ok((
                EvaluationDemand::TruthOnly,
                Some(GuardKind::CaseThen { arm: 0 })
            ))
        );
        assert_eq!(
            control_argument_semantics(
                ControlShape::HigherOrder {
                    body_ordinal: 3,
                    body_demand: EvaluationDemand::Value
                },
                3,
                0,
                EvaluationDemand::Value
            ),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
        assert_eq!(
            control_argument_semantics(ControlShape::TypeOnly, 1, 0, EvaluationDemand::Value),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
    }
}
