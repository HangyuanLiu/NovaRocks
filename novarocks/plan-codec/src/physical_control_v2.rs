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
    ExprId, ExpressionRootError, ExpressionRootRole, ExpressionRootSite, Fragment, JoinSide,
    NodeId, PhysicalRootUses, RootUseBindingError, physical_expression_roots_header_resource_facts,
    root_use_binding_header_resource_facts,
};
use novarocks_proto_models::physical_control_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, DomainGuard,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionControlFlowError,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    GuardKind, MAX_CONTROL_DEFINITIONS, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
};
use std::{alloc::Layout, fmt};
mod resources;
use novarocks_type_contract::{
    ControlOwnedResourceFacts, ControlResourceError, control_resource_add, control_resource_mul,
    expression_control_flow_edge_resource_facts, expression_control_flow_header_resource_facts,
};
pub use resources::{ControlProjectionFacts, ControlProjectionLimits};
use resources::{Model, unbounded};

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
            RootUseBindingError::Control(error)
            | RootUseBindingError::Roots(ExpressionRootError::Control(error)) => {
                Self::Control(error)
            }
            error => Self::Roots(error),
        }
    }
}
impl From<ControlResourceError> for ControlCodecError {
    fn from(e: ControlResourceError) -> Self {
        match e {
            ControlResourceError::Control(c) => Self::Control(c),
            ControlResourceError::SourceModel(m) => Self::InvalidShape(m),
        }
    }
}
// The original facade observes only its original mapper loops. Parent-owned
// admission additionally brackets allocations in the same mapper body.
fn reserve<T>(
    v: &mut Vec<T>,
    n: usize,
    observed: bool,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), ControlCodecError> {
    if observed {
        w.flush()?;
    }
    v.try_reserve_exact(n)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    if observed {
        w.step()?;
        w.flush()?;
    }
    Ok(())
}
fn required(value: Option<u32>, message: &'static str) -> Result<u32, ControlCodecError> {
    value.ok_or(ControlCodecError::InvalidShape(message))
}
pub(crate) fn encode_demand(demand: EvaluationDemand) -> i32 {
    match demand {
        EvaluationDemand::Value => wire::EvaluationDemand::Value as i32,
        EvaluationDemand::TruthOnly => wire::EvaluationDemand::TruthOnly as i32,
    }
}
pub(crate) fn decode_demand(demand: i32) -> Result<EvaluationDemand, ControlCodecError> {
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
pub(crate) fn encode_site(site: ExpressionRootSite) -> wire::RootSite {
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
pub(crate) fn decode_site(site: &wire::RootSite) -> Result<ExpressionRootSite, ControlCodecError> {
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
    let mut model = encode_model(roots, 0, unbounded())?;
    let encoded = encode_in(roots, &mut model, &mut |_| Ok(()), false, &mut work);
    if let Err(ControlCodecError::Control(c)) = &encoded {
        return Err((*c).into());
    }
    work.finish()?;
    Ok(encoded?.0)
}

/// Full Control component sender using the actual immutable fragment and root
/// source, the caller's work scope and one union source invoice. No footer.
pub fn encode_expression_control_observed(
    fragment: &Fragment,
    roots: &PhysicalRootUses,
    source_retained_bytes: usize,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ExpressionControl, ControlProjectionFacts), ControlCodecError> {
    let mut m = encode_model(roots, source_retained_bytes, limits)?;
    m.planned_child = physical_expression_roots_header_resource_facts(fragment)?;
    m.gate(admit)?;
    m.floor(roots.source_retained_floor()?)?;
    let mut child = ControlOwnedResourceFacts::default();
    let mut ordinary = None;
    let checked = roots.validate_fragment_in(
        fragment,
        &mut |f| {
            child = *f;
            match m.gate_with(*f, admit) {
                Ok(_) => Ok(()),
                Err(e) => {
                    let c = match e {
                        ControlCodecError::Control(c) => c,
                        _ => CompileControlError::ResourceExhausted,
                    };
                    ordinary = Some(e);
                    Err(c)
                }
            }
        },
        work,
    );
    if let Some(e) = ordinary {
        return Err(e);
    }
    checked?;
    m.resources.merge(child)?;
    m.planned_child = ControlOwnedResourceFacts::default();
    encode_in(roots, &mut m, admit, true, work)
}
fn encode_model(
    roots: &PhysicalRootUses,
    b: usize,
    limits: ControlProjectionLimits,
) -> Result<Model, ControlCodecError> {
    let f = roots.flow();
    let mut m = Model::new(
        f.domains().len(),
        f.use_reference_count(),
        roots.bindings().len(),
        b,
        limits,
    );
    m.resources
        .buffer::<wire::EvaluationDomain>(f.domains().len(), 1)?;
    m.resources
        .buffer::<wire::ExpressionUse>(f.uses().len(), 1)?;
    m.resources
        .buffer::<wire::RootBinding>(roots.bindings().len(), 1)?;
    // One non-empty argument buffer per use is the maximum request count. The
    // total payload is the cached actual reference count, not a source-B guess.
    let edges = f.use_reference_count() - f.uses().len();
    m.resources.buffer::<u32>(edges, 1)?;
    if edges > 0 {
        m.resources.merge(ControlOwnedResourceFacts {
            allocation_requests_upper_bound: f.uses().len().saturating_sub(1),
            ..Default::default()
        })?;
    }
    m.resources.work(control_resource_mul(
        control_resource_add(
            control_resource_add(f.domains().len(), f.use_reference_count())?,
            roots.bindings().len(),
        )?,
        16,
    )?)?;
    Ok(m)
}
fn encode_in(
    roots: &PhysicalRootUses,
    m: &mut Model,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ExpressionControl, ControlProjectionFacts), ControlCodecError> {
    m.gate(admit)?;
    let flow = roots.flow();
    let mut domains = Vec::new();
    reserve(&mut domains, flow.domains().len(), observed, work)?;
    for domain in flow.domains().values() {
        domains.push(wire::EvaluationDomain {
            id: domain.id.get(),
            parent_domain_id: domain.parent.map(EvaluationDomainId::get),
            guard: domain.guard.map(encode_guard),
        });
        work.step()?;
    }
    let mut uses = Vec::new();
    reserve(&mut uses, flow.uses().len(), observed, work)?;
    for invocation in flow.uses().values() {
        if observed {
            work.flush()?;
        }
        let mut arguments = Vec::new();
        reserve(&mut arguments, invocation.arguments.len(), observed, work)?;
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
    let mut bindings = Vec::new();
    reserve(&mut bindings, roots.bindings().len(), observed, work)?;
    for (site, id) in roots.bindings() {
        bindings.push(wire::RootBinding {
            use_id: Some(id.get()),
            site: Some(encode_site(*site)),
        });
        work.step()?;
    }

    let facts = m.gate(admit)?;
    Ok((
        wire::ExpressionControl {
            domains,
            uses,
            roots: bindings,
        },
        facts,
    ))
}

/// Original convenience facade. The new observed component below borrows a
/// parent scope and never invokes another owner through this facade.
pub fn decode_expression_control(
    fragment: &Fragment,
    input: &wire::ExpressionControl,
    control: &dyn PureCompileControl,
) -> Result<PhysicalRootUses, ControlCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let initial = original_decode_header(input);
    if let Err(error) = initial {
        work.finish()?;
        return Err(error);
    }
    let mut m = decode_model(fragment, input, 0, unbounded())?;
    let projected = project_decode_in(input, &mut m, &mut |_| Ok(()), None, false, &mut work);
    if let Err(ControlCodecError::Control(c)) = &projected {
        return Err((*c).into());
    }
    work.finish()?;
    let (domains, uses, bindings) = projected?;
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        control,
    )?;
    Ok(PhysicalRootUses::try_new(
        fragment, flow, bindings, control,
    )?)
}

fn boxed<T>(
    v: Vec<T>,
    observed: bool,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[T]>, ControlCodecError> {
    if observed {
        w.flush()?;
    }
    let v = v.into_boxed_slice();
    if observed {
        w.step()?;
    }
    if observed {
        w.flush()?;
    }
    Ok(v)
}
fn original_decode_header(input: &wire::ExpressionControl) -> Result<(), ControlCodecError> {
    if input.domains.len() > MAX_CONTROL_DEFINITIONS
        || input.uses.len() > MAX_CONTROL_USE_REFERENCES
        || input.roots.len() > MAX_CONTROL_USE_REFERENCES
    {
        return Err(ExpressionControlFlowError::TooManyItems.into());
    }
    Ok(())
}
fn decode_model(
    fragment: &Fragment,
    input: &wire::ExpressionControl,
    b: usize,
    limits: ControlProjectionLimits,
) -> Result<Model, ControlCodecError> {
    let mut m = Model::new(
        input.domains.len(),
        input.uses.len(),
        input.roots.len(),
        b,
        limits,
    );
    m.resources
        .buffer::<ExpressionEvaluationDomain>(input.domains.len(), 1)?;
    m.resources
        .buffer::<ExpressionInvocation<ExprId>>(input.uses.len(), 1)?;
    m.resources
        .buffer::<(ExpressionRootSite, ExpressionUseId)>(input.roots.len(), 1)?;
    m.resources.work(control_resource_mul(
        control_resource_add(
            control_resource_add(input.domains.len(), input.uses.len())?,
            input.roots.len(),
        )?,
        16,
    )?)?;
    let mut future = novarocks_type_contract::ControlResourceCounter::default();
    future.merge(expression_control_flow_header_resource_facts::<ExprId>(
        input.domains.len(),
        input.uses.len(),
    )?)?;
    future.merge(root_use_binding_header_resource_facts(input.roots.len())?)?;
    future.merge(physical_expression_roots_header_resource_facts(fragment)?)?;
    m.future = future.facts();
    Ok(m)
}
pub fn decode_expression_control_observed(
    fragment: &Fragment,
    input: &wire::ExpressionControl,
    source_retained_bytes: usize,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(PhysicalRootUses, ControlProjectionFacts), ControlCodecError> {
    let mut m = decode_model(fragment, input, source_retained_bytes, limits)?;
    m.gate(admit)?;
    let backing_floor = control_resource_add(
        control_resource_add(
            Layout::array::<wire::EvaluationDomain>(input.domains.capacity())
                .map_err(|_| CompileControlError::ResourceExhausted)?
                .size(),
            Layout::array::<wire::ExpressionUse>(input.uses.capacity())
                .map_err(|_| CompileControlError::ResourceExhausted)?
                .size(),
        )?,
        Layout::array::<wire::RootBinding>(input.roots.capacity())
            .map_err(|_| CompileControlError::ResourceExhausted)?
            .size(),
    )?;
    let floor = control_resource_add(
        std::mem::size_of::<wire::ExpressionControl>(),
        backing_floor,
    )?;
    m.floor(floor)?;
    let (domains, uses, bindings) =
        project_decode_in(input, &mut m, admit, Some(floor), true, work)?;
    // Replace the known future geometry by this same author's actual prefix;
    // completed child contributions enter cumulative resources only once.
    let mut root_plan = novarocks_type_contract::ControlResourceCounter::default();
    root_plan.merge(root_use_binding_header_resource_facts(input.roots.len())?)?;
    root_plan.merge(physical_expression_roots_header_resource_facts(fragment)?)?;
    m.future = root_plan.facts();
    let mut planned = novarocks_type_contract::ControlResourceCounter::default();
    planned.merge(expression_control_flow_header_resource_facts::<ExprId>(
        input.domains.len(),
        input.uses.len(),
    )?)?;
    planned.merge(expression_control_flow_edge_resource_facts(
        m.counts[1] - input.uses.len(),
        input.domains.len(),
        input.uses.len(),
    )?)?;
    m.planned_child = planned.facts();
    let mut child = ControlOwnedResourceFacts::default();
    let mut ordinary = None;
    let flow = ExpressionControlFlow::try_new_in(
        domains,
        uses,
        fragment.expressions(),
        &mut |f| {
            child = *f;
            match m.gate_with(*f, admit) {
                Ok(_) => Ok(()),
                Err(e) => {
                    let c = match e {
                        ControlCodecError::Control(c) => c,
                        _ => CompileControlError::ResourceExhausted,
                    };
                    ordinary = Some(e);
                    Err(c)
                }
            }
        },
        work,
    );
    if let Some(e) = ordinary {
        return Err(e);
    }
    let flow = flow?;
    m.resources.merge(child)?;
    child = ControlOwnedResourceFacts::default();
    m.planned_child = root_plan.facts();
    m.future = ControlOwnedResourceFacts::default();
    let roots = PhysicalRootUses::try_new_in(
        fragment,
        flow,
        bindings,
        &mut |f| {
            child = *f;
            match m.gate_with(*f, admit) {
                Ok(_) => Ok(()),
                Err(e) => {
                    let c = match e {
                        ControlCodecError::Control(c) => c,
                        _ => CompileControlError::ResourceExhausted,
                    };
                    ordinary = Some(e);
                    Err(c)
                }
            }
        },
        work,
    );
    if let Some(e) = ordinary {
        return Err(e);
    }
    let roots = roots?;
    m.resources.merge(child)?;
    m.planned_child = ControlOwnedResourceFacts::default();
    let facts = m.gate(admit)?;
    Ok((roots, facts))
}
type ControlProjectionParts = (
    Vec<ExpressionEvaluationDomain>,
    Vec<ExpressionInvocation<ExprId>>,
    Vec<(ExpressionRootSite, ExpressionUseId)>,
);
fn project_decode_in(
    input: &wire::ExpressionControl,
    m: &mut Model,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    mut source_floor: Option<usize>,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ControlProjectionParts, ControlCodecError> {
    m.gate(admit)?;
    (|| {
        original_decode_header(input)?;
        let mut references = input.uses.len();
        for invocation in &input.uses {
            references = references
                .checked_add(invocation.argument_use_ids.len())
                .ok_or(ExpressionControlFlowError::TooManyItems)?;
            if references > MAX_CONTROL_USE_REFERENCES {
                return Err(ExpressionControlFlowError::TooManyItems.into());
            }
            m.counts[1] = references;
            m.resources
                .buffer::<ExpressionUseId>(invocation.argument_use_ids.len(), 2)?;
            m.resources
                .work(control_resource_mul(invocation.argument_use_ids.len(), 16)?)?;
            let mut future = novarocks_type_contract::ControlResourceCounter::default();
            future.merge(m.future)?;
            future.merge(expression_control_flow_edge_resource_facts(
                invocation.argument_use_ids.len(),
                input.domains.len(),
                input.uses.len(),
            )?)?;
            m.future = future.facts();
            m.gate(admit)?;
            if let Some(floor) = &mut source_floor {
                // Each actual argument Vec owns disjoint backing in this DTO.
                // This is a necessary floor, not a private-capacity estimate.
                let backing = Layout::array::<u32>(invocation.argument_use_ids.capacity())
                    .map_err(|_| CompileControlError::ResourceExhausted)?
                    .size();
                *floor = control_resource_add(*floor, backing)?;
                m.floor(*floor)?;
            }
            work.step()?;
        }
        let mut domains = Vec::new();
        reserve(&mut domains, input.domains.len(), observed, work)?;
        for domain in &input.domains {
            domains.push(ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(domain.id),
                parent: domain.parent_domain_id.map(EvaluationDomainId::new),
                guard: domain.guard.as_ref().map(decode_guard).transpose()?,
            });
            work.step()?;
        }
        let mut uses = Vec::new();
        reserve(&mut uses, input.uses.len(), observed, work)?;
        for invocation in &input.uses {
            let mut arguments = Vec::new();
            reserve(
                &mut arguments,
                invocation.argument_use_ids.len(),
                observed,
                work,
            )?;
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
                arguments: boxed(arguments, observed, work)?,
            });
            work.step()?;
        }
        let mut roots = Vec::new();
        reserve(&mut roots, input.roots.len(), observed, work)?;
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
        Ok((domains, uses, roots))
    })()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "physical_control_v2/owned_tests.rs"]
mod owned_tests;
