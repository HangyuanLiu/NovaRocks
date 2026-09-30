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

//! Typed projection of the flat v2 control component. This operates on DTOs
//! after resource-safe carrier admission; it is not a Prost first-allocation
//! preflight or the complete fragment-package codec.

use novarocks_physical_plan::{
    ExprId, ExpressionRootRole, ExpressionRootSite, Fragment, JoinSide, NodeId, PhysicalRootUses,
    RootUseBindingError,
};
use novarocks_proto_models::physical_control_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, DomainGuard,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionControlFlowError,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    GuardKind, MAX_CONTROL_DEFINITIONS, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlCodecError {
    Control(CompileControlError),
    InvalidShape(&'static str),
    Flow(ExpressionControlFlowError),
    Roots(RootUseBindingError),
}
impl fmt::Display for ControlCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::InvalidShape(message) => f.write_str(message),
            Self::Flow(error) => error.fmt(f),
            Self::Roots(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ControlCodecError {}
impl From<CompileControlError> for ControlCodecError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExpressionControlFlowError> for ControlCodecError {
    fn from(error: ExpressionControlFlowError) -> Self {
        match error {
            ExpressionControlFlowError::Control(error) => Self::Control(error),
            error => Self::Flow(error),
        }
    }
}
impl From<RootUseBindingError> for ControlCodecError {
    fn from(error: RootUseBindingError) -> Self {
        match error {
            RootUseBindingError::Control(error) => Self::Control(error),
            error => Self::Roots(error),
        }
    }
}
fn required(value: Option<u32>, message: &'static str) -> Result<u32, ControlCodecError> {
    value.ok_or(ControlCodecError::InvalidShape(message))
}
fn encode_demand(demand: EvaluationDemand) -> i32 {
    match demand {
        EvaluationDemand::Value => wire::EvaluationDemand::Value as i32,
        EvaluationDemand::TruthOnly => wire::EvaluationDemand::TruthOnly as i32,
    }
}
fn decode_demand(demand: i32) -> Result<EvaluationDemand, ControlCodecError> {
    match wire::EvaluationDemand::try_from(demand) {
        Ok(wire::EvaluationDemand::Value) => Ok(EvaluationDemand::Value),
        Ok(wire::EvaluationDemand::TruthOnly) => Ok(EvaluationDemand::TruthOnly),
        _ => Err(ControlCodecError::InvalidShape(
            "unknown or unspecified evaluation demand",
        )),
    }
}
fn encode_shape(shape: ControlShape) -> wire::ControlShape {
    use wire::control_shape::Kind;
    let simple = match shape {
        ControlShape::Eager => Some(wire::SimpleControl::Eager),
        ControlShape::TypeOnly => Some(wire::SimpleControl::TypeOnly),
        ControlShape::LambdaBody => Some(wire::SimpleControl::LambdaBody),
        ControlShape::Conjunction => Some(wire::SimpleControl::Conjunction),
        ControlShape::Disjunction => Some(wire::SimpleControl::Disjunction),
        ControlShape::If => Some(wire::SimpleControl::If),
        ControlShape::Coalesce => Some(wire::SimpleControl::Coalesce),
        ControlShape::Case { .. } | ControlShape::HigherOrder { .. } => None,
    };
    let kind = match (simple, shape) {
        (Some(simple), _) => Kind::Simple(simple as i32),
        (
            None,
            ControlShape::Case {
                simple,
                arms,
                has_else,
            },
        ) => Kind::CaseControl(wire::CaseControl {
            simple,
            arms,
            has_else,
        }),
        (
            None,
            ControlShape::HigherOrder {
                body_ordinal,
                body_demand,
            },
        ) => Kind::HigherOrder(wire::HigherOrderControl {
            body_ordinal,
            body_demand: encode_demand(body_demand),
        }),
        (None, _) => unreachable!("all fixed control shapes have simple encoding"),
    };
    wire::ControlShape { kind: Some(kind) }
}
fn decode_shape(shape: &wire::ControlShape) -> Result<ControlShape, ControlCodecError> {
    use wire::control_shape::Kind;
    match shape
        .kind
        .as_ref()
        .ok_or(ControlCodecError::InvalidShape("control kind is missing"))?
    {
        Kind::Simple(simple) => match wire::SimpleControl::try_from(*simple) {
            Ok(wire::SimpleControl::Eager) => Ok(ControlShape::Eager),
            Ok(wire::SimpleControl::TypeOnly) => Ok(ControlShape::TypeOnly),
            Ok(wire::SimpleControl::LambdaBody) => Ok(ControlShape::LambdaBody),
            Ok(wire::SimpleControl::Conjunction) => Ok(ControlShape::Conjunction),
            Ok(wire::SimpleControl::Disjunction) => Ok(ControlShape::Disjunction),
            Ok(wire::SimpleControl::If) => Ok(ControlShape::If),
            Ok(wire::SimpleControl::Coalesce) => Ok(ControlShape::Coalesce),
            _ => Err(ControlCodecError::InvalidShape(
                "unknown or unspecified control shape",
            )),
        },
        Kind::CaseControl(case) => Ok(ControlShape::Case {
            simple: case.simple,
            arms: case.arms,
            has_else: case.has_else,
        }),
        Kind::HigherOrder(higher) => Ok(ControlShape::HigherOrder {
            body_ordinal: higher.body_ordinal,
            body_demand: decode_demand(higher.body_demand)?,
        }),
    }
}
fn encode_guard(guard: DomainGuard) -> wire::DomainGuard {
    use wire::domain_guard::Kind;
    let kind = match guard.kind {
        GuardKind::IfThen => Kind::Simple(wire::SimpleGuard::IfThen as i32),
        GuardKind::IfElse => Kind::Simple(wire::SimpleGuard::IfElse as i32),
        GuardKind::CaseElse => Kind::Simple(wire::SimpleGuard::CaseElse as i32),
        GuardKind::LambdaInvocation => Kind::Simple(wire::SimpleGuard::LambdaInvocation as i32),
        GuardKind::CoalesceAfterNull { ordinal } => Kind::CoalesceAfterNullOrdinal(ordinal),
        GuardKind::CaseWhen { arm } => Kind::CaseWhenArm(arm),
        GuardKind::CaseThen { arm } => Kind::CaseThenArm(arm),
    };
    wire::DomainGuard {
        owner_use_id: Some(guard.owner.get()),
        kind: Some(kind),
    }
}
fn decode_guard(guard: &wire::DomainGuard) -> Result<DomainGuard, ControlCodecError> {
    use wire::domain_guard::Kind;
    let owner = ExpressionUseId::new(required(guard.owner_use_id, "guard owner is missing")?);
    let kind = match guard
        .kind
        .as_ref()
        .ok_or(ControlCodecError::InvalidShape("guard kind is missing"))?
    {
        Kind::Simple(simple) => match wire::SimpleGuard::try_from(*simple) {
            Ok(wire::SimpleGuard::IfThen) => GuardKind::IfThen,
            Ok(wire::SimpleGuard::IfElse) => GuardKind::IfElse,
            Ok(wire::SimpleGuard::CaseElse) => GuardKind::CaseElse,
            Ok(wire::SimpleGuard::LambdaInvocation) => GuardKind::LambdaInvocation,
            _ => {
                return Err(ControlCodecError::InvalidShape(
                    "unknown or unspecified guard kind",
                ));
            }
        },
        Kind::CoalesceAfterNullOrdinal(ordinal) => {
            GuardKind::CoalesceAfterNull { ordinal: *ordinal }
        }
        Kind::CaseWhenArm(arm) => GuardKind::CaseWhen { arm: *arm },
        Kind::CaseThenArm(arm) => GuardKind::CaseThen { arm: *arm },
    };
    Ok(DomainGuard { owner, kind })
}
fn encode_site(site: ExpressionRootSite) -> wire::RootSite {
    use wire::root_site::Role;
    let role = match site.role {
        ExpressionRootRole::ScanResidual { predicate } => Role::ScanResidual(predicate),
        ExpressionRootRole::ScanDerived { derived } => Role::ScanDerived(derived),
        ExpressionRootRole::FilterPredicate { predicate } => Role::FilterPredicate(predicate),
        ExpressionRootRole::ProjectOutput { expression } => Role::ProjectOutput(expression),
        ExpressionRootRole::AggregateGroup { group } => Role::AggregateGroup(group),
        ExpressionRootRole::AggregateArgument { call, argument } => {
            Role::AggregateArgument(wire::CallArgumentRoot { call, argument })
        }
        ExpressionRootRole::AggregateOrder { call, key } => {
            Role::AggregateOrder(wire::CallOrderRoot { call, key })
        }
        ExpressionRootRole::JoinKey { key, side } => Role::JoinKey(wire::JoinKeyRoot {
            key,
            side: match side {
                JoinSide::Left => wire::JoinSide::Left as i32,
                JoinSide::Right => wire::JoinSide::Right as i32,
            },
        }),
        ExpressionRootRole::HashJoinResidual => Role::HashJoinResidual(wire::Empty {}),
        ExpressionRootRole::NestLoopPredicate => Role::NestLoopPredicate(wire::Empty {}),
        ExpressionRootRole::SortOrder { key } => Role::SortOrder(key),
        ExpressionRootRole::SortPartition { key } => Role::SortPartition(key),
        ExpressionRootRole::TopNOrder { key } => Role::TopNOrder(key),
        ExpressionRootRole::TopNGroup { group } => Role::TopNGroup(group),
        ExpressionRootRole::TopNStateArgument { call, argument } => {
            Role::TopNStateArgument(wire::CallArgumentRoot { call, argument })
        }
        ExpressionRootRole::TopNStateOrder { call, key } => {
            Role::TopNStateOrder(wire::CallOrderRoot { call, key })
        }
        ExpressionRootRole::WindowPartition { key } => Role::WindowPartition(key),
        ExpressionRootRole::WindowOrder { key } => Role::WindowOrder(key),
        ExpressionRootRole::WindowCall { call } => Role::WindowCall(call),
        ExpressionRootRole::ValuesCell { row, column } => {
            Role::ValuesCell(wire::ValuesRoot { row, column })
        }
        ExpressionRootRole::SeriesStart => Role::SeriesStart(wire::Empty {}),
        ExpressionRootRole::SeriesStop => Role::SeriesStop(wire::Empty {}),
        ExpressionRootRole::SeriesStep => Role::SeriesStep(wire::Empty {}),
        ExpressionRootRole::TableFunctionArgument { argument } => {
            Role::TableFunctionArgument(argument)
        }
        ExpressionRootRole::UnpivotConstant { mapping, constant } => {
            Role::UnpivotConstant(wire::MappingConstantRoot { mapping, constant })
        }
        ExpressionRootRole::ChangePredicate { event } => Role::ChangePredicate(event),
        ExpressionRootRole::ChangeAssignment { event, assignment } => {
            Role::ChangeAssignment(wire::ChangeAssignmentRoot { event, assignment })
        }
        ExpressionRootRole::FinishUnpivotConstant { mapping, constant } => {
            Role::FinishUnpivotConstant(wire::MappingConstantRoot { mapping, constant })
        }
    };
    wire::RootSite {
        node_id: Some(site.node.get()),
        role: Some(role),
    }
}
fn decode_site(site: &wire::RootSite) -> Result<ExpressionRootSite, ControlCodecError> {
    use wire::root_site::Role;
    let node = NodeId::new(required(site.node_id, "root node is missing")?);
    let role = match site
        .role
        .as_ref()
        .ok_or(ControlCodecError::InvalidShape("root role is missing"))?
    {
        Role::ScanResidual(value) => ExpressionRootRole::ScanResidual { predicate: *value },
        Role::ScanDerived(value) => ExpressionRootRole::ScanDerived { derived: *value },
        Role::FilterPredicate(value) => ExpressionRootRole::FilterPredicate { predicate: *value },
        Role::ProjectOutput(value) => ExpressionRootRole::ProjectOutput { expression: *value },
        Role::AggregateGroup(value) => ExpressionRootRole::AggregateGroup { group: *value },
        Role::AggregateArgument(value) => ExpressionRootRole::AggregateArgument {
            call: value.call,
            argument: value.argument,
        },
        Role::AggregateOrder(value) => ExpressionRootRole::AggregateOrder {
            call: value.call,
            key: value.key,
        },
        Role::JoinKey(value) => ExpressionRootRole::JoinKey {
            key: value.key,
            side: match wire::JoinSide::try_from(value.side) {
                Ok(wire::JoinSide::Left) => JoinSide::Left,
                Ok(wire::JoinSide::Right) => JoinSide::Right,
                _ => {
                    return Err(ControlCodecError::InvalidShape(
                        "unknown or unspecified join side",
                    ));
                }
            },
        },
        Role::HashJoinResidual(_) => ExpressionRootRole::HashJoinResidual,
        Role::NestLoopPredicate(_) => ExpressionRootRole::NestLoopPredicate,
        Role::SortOrder(value) => ExpressionRootRole::SortOrder { key: *value },
        Role::SortPartition(value) => ExpressionRootRole::SortPartition { key: *value },
        Role::TopNOrder(value) => ExpressionRootRole::TopNOrder { key: *value },
        Role::TopNGroup(value) => ExpressionRootRole::TopNGroup { group: *value },
        Role::TopNStateArgument(value) => ExpressionRootRole::TopNStateArgument {
            call: value.call,
            argument: value.argument,
        },
        Role::TopNStateOrder(value) => ExpressionRootRole::TopNStateOrder {
            call: value.call,
            key: value.key,
        },
        Role::WindowPartition(value) => ExpressionRootRole::WindowPartition { key: *value },
        Role::WindowOrder(value) => ExpressionRootRole::WindowOrder { key: *value },
        Role::WindowCall(value) => ExpressionRootRole::WindowCall { call: *value },
        Role::ValuesCell(value) => ExpressionRootRole::ValuesCell {
            row: value.row,
            column: value.column,
        },
        Role::SeriesStart(_) => ExpressionRootRole::SeriesStart,
        Role::SeriesStop(_) => ExpressionRootRole::SeriesStop,
        Role::SeriesStep(_) => ExpressionRootRole::SeriesStep,
        Role::TableFunctionArgument(value) => {
            ExpressionRootRole::TableFunctionArgument { argument: *value }
        }
        Role::UnpivotConstant(value) => ExpressionRootRole::UnpivotConstant {
            mapping: value.mapping,
            constant: value.constant,
        },
        Role::ChangePredicate(value) => ExpressionRootRole::ChangePredicate { event: *value },
        Role::ChangeAssignment(value) => ExpressionRootRole::ChangeAssignment {
            event: value.event,
            assignment: value.assignment,
        },
        Role::FinishUnpivotConstant(value) => ExpressionRootRole::FinishUnpivotConstant {
            mapping: value.mapping,
            constant: value.constant,
        },
    };
    Ok(ExpressionRootSite { node, role })
}

/// Canonical table order follows the checked sparse IDs and typed root sites.
/// Definition sharing retains distinct invocation occurrences and demands.
pub fn encode_expression_control(
    roots: &PhysicalRootUses,
    control: &dyn PureCompileControl,
) -> Result<wire::ExpressionControl, ControlCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let flow = roots.flow();
    let mut domains = Vec::with_capacity(flow.domains().len());
    for domain in flow.domains().values() {
        domains.push(wire::EvaluationDomain {
            id: domain.id.get(),
            parent_domain_id: domain.parent.map(EvaluationDomainId::get),
            guard: domain.guard.map(encode_guard),
        });
        work.step()?;
    }
    let mut uses = Vec::with_capacity(flow.uses().len());
    for invocation in flow.uses().values() {
        let mut arguments = Vec::with_capacity(invocation.arguments.len());
        for argument in &invocation.arguments {
            arguments.push(argument.get());
            work.step()?;
        }
        uses.push(wire::ExpressionUse {
            id: invocation.context.use_id.get(),
            definition_id: Some(invocation.definition.get()),
            domain_id: Some(invocation.context.domain.get()),
            demand: encode_demand(invocation.context.demand),
            control: Some(encode_shape(invocation.control)),
            argument_use_ids: arguments,
        });
        work.step()?;
    }
    let mut bindings = Vec::with_capacity(roots.bindings().len());
    for (site, id) in roots.bindings() {
        bindings.push(wire::RootBinding {
            use_id: Some(id.get()),
            site: Some(encode_site(*site)),
        });
        work.step()?;
    }
    work.finish()?;
    Ok(wire::ExpressionControl {
        domains,
        uses,
        roots: bindings,
    })
}

/// Bounded DTO-to-public projection, followed by the same common graph and
/// actual fragment correspondence validators. No eager or missing-ID defaults.
pub fn decode_expression_control(
    fragment: &Fragment,
    input: &wire::ExpressionControl,
    control: &dyn PureCompileControl,
) -> Result<PhysicalRootUses, ControlCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    if input.domains.len() > MAX_CONTROL_DEFINITIONS
        || input.uses.len() > MAX_CONTROL_USE_REFERENCES
        || input.roots.len() > MAX_CONTROL_USE_REFERENCES
    {
        return Err(ExpressionControlFlowError::TooManyItems.into());
    }
    let mut references = input.uses.len();
    for invocation in &input.uses {
        references = references
            .checked_add(invocation.argument_use_ids.len())
            .ok_or(ExpressionControlFlowError::TooManyItems)?;
        if references > MAX_CONTROL_USE_REFERENCES {
            return Err(ExpressionControlFlowError::TooManyItems.into());
        }
        work.step()?;
    }
    let mut domains = Vec::with_capacity(input.domains.len());
    for domain in &input.domains {
        domains.push(ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(domain.id),
            parent: domain.parent_domain_id.map(EvaluationDomainId::new),
            guard: domain.guard.as_ref().map(decode_guard).transpose()?,
        });
        work.step()?;
    }
    let mut uses = Vec::with_capacity(input.uses.len());
    for invocation in &input.uses {
        let mut arguments = Vec::with_capacity(invocation.argument_use_ids.len());
        for argument in &invocation.argument_use_ids {
            arguments.push(ExpressionUseId::new(*argument));
            work.step()?;
        }
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(invocation.id),
                domain: EvaluationDomainId::new(required(
                    invocation.domain_id,
                    "use domain is missing",
                )?),
                demand: decode_demand(invocation.demand)?,
            },
            definition: ExprId::new(required(
                invocation.definition_id,
                "use definition is missing",
            )?),
            control: decode_shape(
                invocation
                    .control
                    .as_ref()
                    .ok_or(ControlCodecError::InvalidShape("use control is missing"))?,
            )?,
            arguments: arguments.into_boxed_slice(),
        });
        work.step()?;
    }
    let mut roots = Vec::with_capacity(input.roots.len());
    for binding in &input.roots {
        roots.push((
            decode_site(
                binding
                    .site
                    .as_ref()
                    .ok_or(ControlCodecError::InvalidShape("root site is missing"))?,
            )?,
            ExpressionUseId::new(required(binding.use_id, "root use is missing")?),
        ));
        work.step()?;
    }
    work.finish()?;
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        control,
    )?;
    Ok(PhysicalRootUses::try_new(fragment, flow, roots, control)?)
}

#[cfg(test)]
mod tests;
