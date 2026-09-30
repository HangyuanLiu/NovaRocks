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
use crate::{MAX_STATIC_EXPRESSION_DEPTH, MAX_STATIC_EXPRESSIONS, ProgramExprId};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, PureCompileControl,
};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlShape {
    Eager,
    TypeOnly,
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
        body_demand: novarocks_type_contract::EvaluationDemand,
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
pub struct ProgramEvaluationDomain {
    pub id: EvaluationDomainId,
    pub parent: Option<EvaluationDomainId>,
    pub guard: Option<DomainGuard>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramExpressionUse {
    pub context: ExpressionEffectContext,
    pub definition: ProgramExprId,
    pub control: ControlShape,
    pub arguments: Box<[ExpressionUseId]>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramControlFlow {
    domains: Arc<BTreeMap<EvaluationDomainId, ProgramEvaluationDomain>>,
    uses: Arc<BTreeMap<ExpressionUseId, ProgramExpressionUse>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramControlFlowError {
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
impl fmt::Display for ProgramControlFlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid program control flow: {self:?}")
    }
}
impl std::error::Error for ProgramControlFlowError {}
impl From<CompileControlError> for ProgramControlFlowError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl ProgramControlFlow {
    /// `definition_count` names the already-validated dense expression arena.
    /// The compiler checks the definition's exact owner/control shape before
    /// this structural constructor. No instance Selection is stored here.
    pub fn try_new(
        domains: Vec<ProgramEvaluationDomain>,
        uses: Vec<ProgramExpressionUse>,
        definition_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramControlFlowError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        if domains.len() > MAX_STATIC_EXPRESSIONS
            || uses.len() > MAX_STATIC_EXPRESSIONS
            || definition_count > MAX_STATIC_EXPRESSIONS
        {
            return Err(ProgramControlFlowError::TooManyItems);
        }
        let mut domain_index = BTreeMap::new();
        for domain in domains {
            if domain_index.insert(domain.id, domain).is_some() {
                return Err(ProgramControlFlowError::DuplicateIdentity);
            }
            work.step()?;
        }
        let mut use_index = BTreeMap::new();
        let mut references = uses.len();
        if references > crate::MAX_PROGRAM_EXPANDED_OCCURRENCES {
            return Err(ProgramControlFlowError::TooManyItems);
        }
        for value in uses {
            references = references
                .checked_add(value.arguments.len())
                .ok_or(ProgramControlFlowError::TooManyItems)?;
            if references > crate::MAX_PROGRAM_EXPANDED_OCCURRENCES {
                return Err(ProgramControlFlowError::TooManyItems);
            }
            if value.definition.index() >= definition_count
                || !domain_index.contains_key(&value.context.domain)
            {
                return Err(ProgramControlFlowError::InvalidReference);
            }
            if use_index.insert(value.context.use_id, value).is_some() {
                return Err(ProgramControlFlowError::DuplicateIdentity);
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
                        .ok_or(ProgramControlFlowError::InvalidReference)?;
                    if parent != owner.context.domain || !domain_index.contains_key(&parent) {
                        return Err(ProgramControlFlowError::InvalidGuard);
                    }
                    children.entry(parent).or_default().push(domain.id);
                }
                _ => return Err(ProgramControlFlowError::InvalidGuard),
            }
            work.step()?;
        }
        let mut visited = 0usize;
        while let Some(parent) = ready.pop_front() {
            visited += 1;
            let depth = depths[&parent];
            if depth > MAX_STATIC_EXPRESSION_DEPTH {
                return Err(ProgramControlFlowError::TooDeep);
            }
            for child in children.get(&parent).into_iter().flatten() {
                depths.insert(*child, depth + 1);
                ready.push_back(*child);
                work.step()?;
            }
            work.step()?;
        }
        if visited != domain_index.len() {
            return Err(ProgramControlFlowError::Cycle);
        }
        let mut indegrees: BTreeMap<ExpressionUseId, usize> = BTreeMap::new();
        for id in use_index.keys() {
            indegrees.insert(*id, 0);
            work.step()?;
        }
        let mut used_guards = BTreeMap::new();
        for owner in use_index.values() {
            validate_arity(owner)?;
            for (ordinal, argument) in owner.arguments.iter().enumerate() {
                let child = use_index
                    .get(argument)
                    .ok_or(ProgramControlFlowError::InvalidReference)?;
                if child.context.demand
                    != argument_demand(owner.control, ordinal, owner.context.demand)
                {
                    return Err(ProgramControlFlowError::InvalidDemand);
                }
                let expected = argument_guard(owner.control, ordinal);
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
                    _ => return Err(ProgramControlFlowError::InvalidGuard),
                }
                let degree = indegrees.get_mut(argument).unwrap();
                *degree += 1;
                if *degree > 1 {
                    return Err(ProgramControlFlowError::SharedUse);
                }
                work.step()?;
            }
            work.step()?;
        }
        for domain in domain_index.values() {
            if domain.guard.is_some() && !used_guards.contains_key(&domain.id) {
                return Err(ProgramControlFlowError::InvalidGuard);
            }
            work.step()?;
        }
        let mut ready = VecDeque::new();
        let mut use_depths = BTreeMap::new();
        for (id, degree) in &indegrees {
            if *degree == 0 {
                ready.push_back(*id);
                use_depths.insert(*id, 1usize);
            }
            work.step()?;
        }
        let mut visited = 0usize;
        while let Some(id) = ready.pop_front() {
            visited += 1;
            let depth = use_depths[&id];
            if depth > MAX_STATIC_EXPRESSION_DEPTH {
                return Err(ProgramControlFlowError::TooDeep);
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
            return Err(ProgramControlFlowError::Cycle);
        }
        work.finish()?;
        Ok(Self {
            domains: Arc::new(domain_index),
            uses: Arc::new(use_index),
        })
    }
    pub fn domains(&self) -> &BTreeMap<EvaluationDomainId, ProgramEvaluationDomain> {
        &self.domains
    }
    pub fn uses(&self) -> &BTreeMap<ExpressionUseId, ProgramExpressionUse> {
        &self.uses
    }
}
fn validate_arity(owner: &ProgramExpressionUse) -> Result<(), ProgramControlFlowError> {
    let count = owner.arguments.len();
    let valid = match owner.control {
        ControlShape::Eager => true,
        ControlShape::TypeOnly => count == 0,
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
        Err(ProgramControlFlowError::InvalidControlShape)
    }
}
fn argument_guard(shape: ControlShape, ordinal: usize) -> Option<GuardKind> {
    match shape {
        ControlShape::Eager
        | ControlShape::TypeOnly
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

fn argument_demand(
    shape: ControlShape,
    ordinal: usize,
    owner: novarocks_type_contract::EvaluationDemand,
) -> novarocks_type_contract::EvaluationDemand {
    use novarocks_type_contract::EvaluationDemand::{TruthOnly, Value};
    match shape {
        ControlShape::Eager | ControlShape::TypeOnly | ControlShape::Coalesce => Value,
        ControlShape::Conjunction | ControlShape::Disjunction => owner,
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

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::EvaluationDemand;
    struct Control(bool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
            if self.0 && work > 0 {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    fn root() -> ProgramEvaluationDomain {
        ProgramEvaluationDomain {
            id: EvaluationDomainId::new(u32::MAX),
            parent: None,
            guard: None,
        }
    }
    fn value(id: u32, domain: EvaluationDomainId) -> ProgramExpressionUse {
        ProgramExpressionUse {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(id),
                domain,
                demand: EvaluationDemand::Value,
            },
            definition: ProgramExprId::new(0),
            control: ControlShape::Eager,
            arguments: Box::default(),
        }
    }
    fn branch(id: u32, kind: GuardKind) -> ProgramEvaluationDomain {
        ProgramEvaluationDomain {
            id: EvaluationDomainId::new(id),
            parent: Some(root().id),
            guard: Some(DomainGuard {
                owner: ExpressionUseId::new(0),
                kind,
            }),
        }
    }
    #[test]
    fn if_retains_distinct_occurrences_and_strong_branch_domains() {
        let mut owner = value(0, root().id);
        owner.control = ControlShape::If;
        owner.arguments = vec![
            ExpressionUseId::new(1),
            ExpressionUseId::new(2),
            ExpressionUseId::new(3),
        ]
        .into_boxed_slice();
        let then = branch(1, GuardKind::IfThen);
        let otherwise = branch(2, GuardKind::IfElse);
        let mut condition = value(1, root().id);
        condition.context.demand = EvaluationDemand::TruthOnly;
        let uses = vec![owner, condition, value(2, then.id), value(3, otherwise.id)];
        let flow = ProgramControlFlow::try_new(
            vec![root(), then, otherwise],
            uses.clone(),
            1,
            &Control(false),
        )
        .unwrap();
        // All four uses share one immutable definition; no invocation is merged.
        assert_eq!(flow.uses().len(), 4);
        let mut invalid = uses;
        invalid[2].context.domain = root().id;
        assert_eq!(
            ProgramControlFlow::try_new(vec![root(), then, otherwise], invalid, 1, &Control(false))
                .unwrap_err(),
            ProgramControlFlowError::InvalidGuard
        );
    }
    #[test]
    fn shared_definition_does_not_share_use_or_demand() {
        let mut truth = value(1, root().id);
        truth.context.demand = EvaluationDemand::TruthOnly;
        let value_use = value(2, root().id);
        let flow =
            ProgramControlFlow::try_new(vec![root()], vec![truth, value_use], 1, &Control(false))
                .unwrap();
        assert_ne!(
            flow.uses()[&ExpressionUseId::new(1)].context.demand,
            flow.uses()[&ExpressionUseId::new(2)].context.demand
        );
        let mut owner = value(0, root().id);
        owner.arguments = vec![ExpressionUseId::new(1), ExpressionUseId::new(1)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(
                vec![root()],
                vec![owner, value(1, root().id)],
                1,
                &Control(false)
            )
            .unwrap_err(),
            ProgramControlFlowError::SharedUse
        );
    }
    #[test]
    fn type_only_control_has_no_value_invocation_edges() {
        let mut owner = value(0, root().id);
        owner.control = ControlShape::TypeOnly;
        ProgramControlFlow::try_new(vec![root()], vec![owner.clone()], 1, &Control(false)).unwrap();
        owner.arguments = vec![ExpressionUseId::new(1)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(
                vec![root()],
                vec![owner, value(1, root().id)],
                1,
                &Control(false)
            )
            .unwrap_err(),
            ProgramControlFlowError::InvalidControlShape
        );
    }
    #[test]
    fn null_sensitive_arguments_stop_truth_only_propagation() {
        for shape in [
            ControlShape::Eager,
            ControlShape::Coalesce,
            ControlShape::Conjunction,
            ControlShape::Disjunction,
        ] {
            let mut owner = value(0, root().id);
            owner.control = shape;
            owner.context.demand = EvaluationDemand::TruthOnly;
            owner.arguments = vec![ExpressionUseId::new(1)].into_boxed_slice();
            let mut child = value(1, root().id);
            child.context.demand = argument_demand(shape, 0, owner.context.demand);
            ProgramControlFlow::try_new(
                vec![root()],
                vec![owner.clone(), child.clone()],
                1,
                &Control(false),
            )
            .unwrap();
            child.context.demand = if child.context.demand == EvaluationDemand::Value {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            };
            assert_eq!(
                ProgramControlFlow::try_new(vec![root()], vec![owner, child], 1, &Control(false))
                    .unwrap_err(),
                ProgramControlFlowError::InvalidDemand
            );
        }
    }
    #[test]
    fn invocation_bound_counts_zero_argument_root_uses() {
        let cap = crate::MAX_PROGRAM_EXPANDED_OCCURRENCES;
        let uses = (0..cap as u32).map(|id| value(id, root().id)).collect();
        ProgramControlFlow::try_new(vec![root()], uses, 1, &Control(false)).unwrap();
        let uses = (0..=cap as u32).map(|id| value(id, root().id)).collect();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], uses, 1, &Control(false)).unwrap_err(),
            ProgramControlFlowError::TooManyItems
        );
    }
    #[test]
    fn cycles_invalid_sparse_ids_and_control_cancellation_fail_closed() {
        let mut cycle = value(0, root().id);
        cycle.arguments = vec![ExpressionUseId::new(0)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], vec![cycle], 1, &Control(false)).unwrap_err(),
            ProgramControlFlowError::Cycle
        );
        let mut missing = value(0, root().id);
        missing.arguments = vec![ExpressionUseId::new(u32::MAX)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], vec![missing], 1, &Control(false))
                .unwrap_err(),
            ProgramControlFlowError::InvalidReference
        );
        let uses = (0..1000).map(|id| value(id, root().id)).collect();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], uses, 1, &Control(true)).unwrap_err(),
            ProgramControlFlowError::Control(CompileControlError::Cancelled)
        );
    }
    #[test]
    fn coalesce_and_case_arguments_have_exact_guard_ordinals() {
        for (shape, guards) in [
            (
                ControlShape::Coalesce,
                vec![
                    None,
                    Some(GuardKind::CoalesceAfterNull { ordinal: 1 }),
                    Some(GuardKind::CoalesceAfterNull { ordinal: 2 }),
                ],
            ),
            (
                ControlShape::Case {
                    simple: true,
                    arms: 1,
                    has_else: true,
                },
                vec![
                    None,
                    Some(GuardKind::CaseWhen { arm: 0 }),
                    Some(GuardKind::CaseThen { arm: 0 }),
                    Some(GuardKind::CaseElse),
                ],
            ),
            (
                ControlShape::Case {
                    simple: false,
                    arms: 1,
                    has_else: false,
                },
                vec![
                    Some(GuardKind::CaseWhen { arm: 0 }),
                    Some(GuardKind::CaseThen { arm: 0 }),
                ],
            ),
        ] {
            let mut domains = vec![root()];
            let mut owner = value(0, root().id);
            owner.control = shape;
            let mut uses = Vec::new();
            let mut arguments = Vec::new();
            for (ordinal, guard) in guards.iter().enumerate() {
                let domain = if let Some(guard) = guard {
                    let domain = branch(ordinal as u32, *guard);
                    domains.push(domain);
                    domain.id
                } else {
                    root().id
                };
                arguments.push(ExpressionUseId::new(ordinal as u32 + 1));
                let mut child = value(ordinal as u32 + 1, domain);
                child.context.demand = argument_demand(shape, ordinal, owner.context.demand);
                uses.push(child);
            }
            owner.arguments = arguments.into_boxed_slice();
            uses.push(owner);
            ProgramControlFlow::try_new(domains, uses, 1, &Control(false)).unwrap();
        }
    }
}
