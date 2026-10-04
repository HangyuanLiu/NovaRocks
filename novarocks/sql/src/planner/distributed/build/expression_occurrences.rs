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

//! Author actual expression uses from one original physical definition graph.
//! Relational call facts, selected preparation and final package publication
//! remain separate obligations; this graph alone authorizes no execution.

use novarocks_functions::FunctionSpecializationFailure;
use novarocks_physical_plan::{
    ExprId, ExprKind, ExpressionRootError, Fragment, FrozenCallError, PhysicalCallBinding,
    PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses, RootUseBindingError,
    visit_relational_calls_observed,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    DomainGuard, EvaluationDomainId, ExpressionControlFlow, ExpressionControlFlowError,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    MAX_CONTROL_DEFINITIONS, MAX_CONTROL_DEPTH, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
    control_argument_semantics,
};

use crate::compiler::SqlFunctionCatalog;

#[derive(Debug)]
pub(crate) enum ExpressionOccurrenceError {
    Control(CompileControlError),
    Roots(ExpressionRootError),
    RootBinding(RootUseBindingError),
    Flow(ExpressionControlFlowError),
    Function(FunctionSpecializationFailure),
    MissingDefinition(ExprId),
    InvalidFunctionControl(ExprId),
    TooManyItems,
    Calls(FrozenCallError),
    InvalidRelationalControl(PhysicalCallSite),
}
impl From<CompileControlError> for ExpressionOccurrenceError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<FrozenCallError> for ExpressionOccurrenceError {
    fn from(value: FrozenCallError) -> Self {
        match value {
            FrozenCallError::Control(error) => Self::Control(error),
            error => Self::Calls(error),
        }
    }
}

/// Expression and relational contexts authored from the same original source.
/// These identities and domains are topology, not refined call-effect facts.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalOccurrences {
    pub root_uses: PhysicalRootUses,
    pub relational_contexts: Vec<(PhysicalCallSite, ExpressionEffectContext)>,
}

impl ExpressionOccurrenceError {
    fn flow(error: ExpressionControlFlowError) -> Self {
        match error {
            ExpressionControlFlowError::Control(error) => Self::Control(error),
            error => Self::Flow(error),
        }
    }
    fn function(error: FunctionSpecializationFailure) -> Self {
        match error {
            FunctionSpecializationFailure::Control(error) => Self::Control(error),
            FunctionSpecializationFailure::Binding(
                novarocks_functions::FunctionBindingError::Control(error),
            ) => Self::Control(error),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::Cancelled,
            ) => Self::Control(CompileControlError::Cancelled),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::DeadlineExceeded,
            ) => Self::Control(CompileControlError::DeadlineExceeded),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::ResourceExhausted,
            ) => Self::Control(CompileControlError::ResourceExhausted),
            error => Self::Function(error),
        }
    }
}

/// Author every actual root and ordered child occurrence, using the original
/// root/child grammar and exact installed scalar overload declarations. Shared
/// definitions are expanded into independent uses; no maximum ExprId indexing
/// or cross-use cache is involved. Static TypeOnly arguments create no runtime
/// invocation. All caller-owned maps, vectors and owner lookups require prior
/// admission; bounded cooperative work is not a memory funding grant.
pub(crate) fn author_physical_occurrences_observed(
    fragment: &Fragment,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalOccurrences, ExpressionOccurrenceError> {
    let mut author = Author {
        fragment,
        functions,
        work: CompileCheckpoints::try_new(control, CompilePhase::Validate)?,
        domains: Vec::new(),
        uses: Vec::new(),
        references: 0,
    };
    let result = (|| {
        let definitions = fragment.expressions().len();
        author.work.step()?;
        if definitions > MAX_CONTROL_DEFINITIONS {
            return Err(ExpressionOccurrenceError::TooManyItems);
        }
        author.work.flush()?;
        let roots =
            PhysicalExpressionRoots::try_new(fragment, control).map_err(|error| match error {
                ExpressionRootError::Control(error) => ExpressionOccurrenceError::Control(error),
                error => ExpressionOccurrenceError::Roots(error),
            })?;
        let mut bindings = Vec::new();
        bindings
            .try_reserve_exact(roots.sites().len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for (site, root) in roots.sites() {
            let domain = author.domain(None, None)?;
            let id = author.invocation(root.expr, domain, root.demand, 1)?;
            bindings.push((*site, id));
            author.work.step()?;
        }
        let mut relational_contexts = Vec::new();
        let expression_use_count = author.uses.len();
        let domains = &mut author.domains;
        visit_relational_calls_observed(fragment, &mut author.work, |site, binding, work| {
            if relational_contexts.len() == MAX_CONTROL_USE_REFERENCES
                || domains.len() == MAX_CONTROL_DEFINITIONS
            {
                return Err(ExpressionOccurrenceError::TooManyItems);
            }
            let (function_id, kind, overload, expected_control) = match binding {
                PhysicalCallBinding::Aggregate(binding) => (
                    &binding.function.function_id,
                    binding.function.kind,
                    &binding.function.overload,
                    ArgumentControl::Aggregate,
                ),
                PhysicalCallBinding::Table(function) => (
                    &function.function_id,
                    novarocks_type_contract::FunctionKind::Table,
                    &function.overload,
                    ArgumentControl::Table,
                ),
                _ => return Err(ExpressionOccurrenceError::InvalidRelationalControl(site)),
            };
            work.flush()?;
            let declaration = functions
                .pure_overload_declaration_observed(function_id, kind, overload, control)
                .map_err(ExpressionOccurrenceError::function)?;
            let matches_control = declaration.effects().argument_control == expected_control;
            work.step()?;
            if !matches_control {
                return Err(ExpressionOccurrenceError::InvalidRelationalControl(site));
            }
            let domain = EvaluationDomainId::new(domains.len() as u32);
            // Both expression and relational counts are independently bounded
            // by 65536, so their disjoint namespace fits u32 without wrapping.
            let id =
                ExpressionUseId::new((expression_use_count + relational_contexts.len()) as u32);
            domains
                .try_reserve(1)
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            relational_contexts
                .try_reserve(1)
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            domains.push(ExpressionEvaluationDomain {
                id: domain,
                parent: None,
                guard: None,
            });
            relational_contexts.push((
                site,
                ExpressionEffectContext {
                    use_id: id,
                    domain,
                    demand: novarocks_type_contract::EvaluationDemand::Value,
                },
            ));
            work.step()?;
            Ok(())
        })?;
        author.work.flush()?;
        let flow = ExpressionControlFlow::try_new(
            std::mem::take(&mut author.domains),
            std::mem::take(&mut author.uses),
            fragment.expressions(),
            CompilePhase::Validate,
            control,
        )
        .map_err(ExpressionOccurrenceError::flow)?;
        author.work.flush()?;
        let root_uses =
            PhysicalRootUses::try_new(fragment, flow, bindings, control).map_err(|error| {
                match error {
                    RootUseBindingError::Control(error) => {
                        ExpressionOccurrenceError::Control(error)
                    }
                    error => ExpressionOccurrenceError::RootBinding(error),
                }
            })?;
        Ok(AuthoredPhysicalOccurrences {
            root_uses,
            relational_contexts,
        })
    })();
    if matches!(&result, Err(ExpressionOccurrenceError::Control(_))) {
        return result;
    }
    author.work.finish()?;
    result
}

#[cfg(test)]
fn author_expression_occurrences_observed(
    fragment: &Fragment,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<PhysicalRootUses, ExpressionOccurrenceError> {
    author_physical_occurrences_observed(fragment, functions, control).map(|value| value.root_uses)
}

struct Author<'a> {
    fragment: &'a Fragment,
    functions: &'a dyn SqlFunctionCatalog,
    work: CompileCheckpoints<'a>,
    domains: Vec<ExpressionEvaluationDomain>,
    uses: Vec<ExpressionInvocation<ExprId>>,
    references: usize,
}
impl Author<'_> {
    fn reference(&mut self) -> Result<(), ExpressionOccurrenceError> {
        if self.references == MAX_CONTROL_USE_REFERENCES {
            return Err(ExpressionOccurrenceError::TooManyItems);
        }
        self.references += 1;
        self.work.step()?;
        Ok(())
    }
    fn domain(
        &mut self,
        parent: Option<EvaluationDomainId>,
        guard: Option<DomainGuard>,
    ) -> Result<EvaluationDomainId, ExpressionOccurrenceError> {
        if self.domains.len() == MAX_CONTROL_DEFINITIONS {
            return Err(ExpressionOccurrenceError::TooManyItems);
        }
        let id = EvaluationDomainId::new(self.domains.len() as u32);
        self.domains
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.domains
            .push(ExpressionEvaluationDomain { id, parent, guard });
        self.work.step()?;
        Ok(id)
    }
    fn invocation(
        &mut self,
        definition: ExprId,
        domain: EvaluationDomainId,
        demand: novarocks_type_contract::EvaluationDemand,
        depth: usize,
    ) -> Result<ExpressionUseId, ExpressionOccurrenceError> {
        if depth > MAX_CONTROL_DEPTH {
            return Err(ExpressionOccurrenceError::Flow(
                ExpressionControlFlowError::TooDeep,
            ));
        }
        self.reference()?;
        let node = self.fragment.expressions().get(definition);
        self.work.step()?;
        let node = node.ok_or(ExpressionOccurrenceError::MissingDefinition(definition))?;
        let shape = if let Some(shape) = node
            .kind
            .intrinsic_control_shape()
            .map_err(ExpressionOccurrenceError::flow)?
        {
            shape
        } else {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                return Err(ExpressionOccurrenceError::InvalidFunctionControl(
                    definition,
                ));
            };
            self.work.flush()?;
            let declaration = self
                .functions
                .pure_overload_declaration_observed(
                    &function.function_id,
                    function.kind,
                    &function.overload,
                    self.work.control(),
                )
                .map_err(ExpressionOccurrenceError::function)?;
            let shape = scalar_shape(declaration.effects().argument_control, args.len());
            self.work.step()?;
            shape.ok_or(ExpressionOccurrenceError::InvalidFunctionControl(
                definition,
            ))?
        };
        let id = ExpressionUseId::new(self.uses.len() as u32);
        self.uses
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand,
            },
            definition,
            control: shape,
            arguments: Box::default(),
        });
        self.work.step()?;
        let mut children = Vec::new();
        if shape != ControlShape::TypeOnly {
            node.kind.expression_references_observed(|child| {
                self.reference()?;
                children
                    .try_reserve(1)
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                children.push(child);
                Ok::<_, ExpressionOccurrenceError>(())
            })?;
        }
        let mut arguments = Vec::new();
        arguments
            .try_reserve_exact(children.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for (ordinal, child) in children.iter().enumerate() {
            let (child_demand, guard) =
                control_argument_semantics(shape, children.len(), ordinal, demand)
                    .map_err(ExpressionOccurrenceError::flow)?;
            let child_domain = if let Some(kind) = guard {
                self.domain(Some(domain), Some(DomainGuard { owner: id, kind }))?
            } else {
                domain
            };
            let child_use = self.invocation(*child, child_domain, child_demand, depth + 1)?;
            arguments.push(child_use);
            self.work.step()?;
        }
        self.uses[id.get() as usize].arguments = arguments.into_boxed_slice();
        self.work.step()?;
        Ok(id)
    }
}

// Only the installed owner supplies this enum. Case arity follows the same
// ordered operand/WHEN/THEN/ELSE vocabulary checked by the control graph.
fn scalar_shape(control: ArgumentControl, count: usize) -> Option<ControlShape> {
    Some(match control {
        ArgumentControl::Eager => ControlShape::Eager,
        ArgumentControl::TypeOnly => ControlShape::TypeOnly,
        ArgumentControl::If => ControlShape::If,
        ArgumentControl::Coalesce => ControlShape::Coalesce,
        ArgumentControl::SimpleCase | ArgumentControl::SearchedCase => {
            let simple = control == ArgumentControl::SimpleCase;
            let tail = count.checked_sub(usize::from(simple))?;
            ControlShape::Case {
                simple,
                arms: u32::try_from(tail / 2).ok()?,
                has_else: tail % 2 == 1,
            }
        }
        ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand,
        } => ControlShape::HigherOrder {
            body_ordinal,
            body_demand,
        },
        ArgumentControl::Aggregate | ArgumentControl::Window | ArgumentControl::Table => {
            return None;
        }
    })
}

#[cfg(test)]
#[path = "expression_occurrences_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "relational_occurrences_tests.rs"]
mod relational_tests;
