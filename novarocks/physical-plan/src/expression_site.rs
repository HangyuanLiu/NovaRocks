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

//! Exact runtime expression root fields in one immutable fragment.
//!
//! Field indices are frozen-plan positions, never ordinals in a flattened
//! expression list. Proof references do not create invocation sites.

use crate::{
    AggregateCall, ExprUse, Fragment, FragmentId, JoinKind, JoinSide, NodeId, NodeKind, SortMode,
    TopNReduction, ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExpressionRootRole {
    ScanResidual {
        predicate: u32,
    },
    ScanDerived {
        derived: u32,
    },
    FilterPredicate {
        predicate: u32,
    },
    ProjectOutput {
        expression: u32,
    },
    AggregateGroup {
        group: u32,
    },
    AggregateArgument {
        call: u32,
        argument: u32,
    },
    AggregateOrder {
        call: u32,
        key: u32,
    },
    JoinKey {
        key: u32,
        side: JoinSide,
    },
    HashJoinResidual,
    NestLoopPredicate,
    SortOrder {
        key: u32,
    },
    SortPartition {
        key: u32,
    },
    TopNOrder {
        key: u32,
    },
    TopNGroup {
        group: u32,
    },
    TopNStateArgument {
        call: u32,
        argument: u32,
    },
    TopNStateOrder {
        call: u32,
        key: u32,
    },
    WindowPartition {
        key: u32,
    },
    WindowOrder {
        key: u32,
    },
    WindowCall {
        call: u32,
    },
    ValuesCell {
        row: u32,
        column: u32,
    },
    SeriesStart,
    SeriesStop,
    SeriesStep,
    TableFunctionArgument {
        argument: u32,
    },
    UnpivotConstant {
        mapping: u32,
        constant: u32,
    },
    ChangePredicate {
        event: u32,
    },
    /// Evaluated only for this event's exact selected rows, after its predicate.
    ChangeAssignment {
        event: u32,
        assignment: u32,
    },
    /// Evaluated only for the mapping selected by its exact write-target group.
    FinishUnpivotConstant {
        mapping: u32,
        constant: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExpressionRootSite {
    pub node: NodeId,
    pub role: ExpressionRootRole,
}

/// Complete root-field projection, including repeated uses of one definition.
/// This checks root ownership/demand; control and function-owner correspondence
/// remain separate compilation obligations. It owns no Selection or instances.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalExpressionRoots {
    fragment: FragmentId,
    sites: Arc<BTreeMap<ExpressionRootSite, ExprUse>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpressionRootError {
    Control(CompileControlError),
    TooManyRoots,
    InvalidDerivedValue,
    InvalidExpressionOwner,
    InvalidExpressionScope,
    DuplicateSite,
}
impl fmt::Display for ExpressionRootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid physical expression root: {self:?}")
    }
}
impl std::error::Error for ExpressionRootError {}
impl From<CompileControlError> for ExpressionRootError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

struct RootCollector<'a> {
    fragment: &'a Fragment,
    sites: BTreeMap<ExpressionRootSite, ExprUse>,
    work: CompileCheckpoints<'a>,
}
impl RootCollector<'_> {
    fn add(
        &mut self,
        node: NodeId,
        role: ExpressionRootRole,
        expr: crate::ExprId,
        demand: EvaluationDemand,
    ) -> Result<(), ExpressionRootError> {
        if self.sites.len() == MAX_CONTROL_USE_REFERENCES {
            return Err(ExpressionRootError::TooManyRoots);
        }
        let definition = self
            .fragment
            .expressions()
            .get(expr)
            .filter(|definition| definition.owner == node)
            .ok_or(ExpressionRootError::InvalidExpressionOwner)?;
        if definition.lambda_scope.is_some() {
            return Err(ExpressionRootError::InvalidExpressionScope);
        }
        if self
            .sites
            .insert(ExpressionRootSite { node, role }, ExprUse { expr, demand })
            .is_some()
        {
            return Err(ExpressionRootError::DuplicateSite);
        }
        self.work.step()?;
        Ok(())
    }
    fn calls(
        &mut self,
        node: NodeId,
        calls: &[AggregateCall],
        topn: bool,
    ) -> Result<(), ExpressionRootError> {
        use ExpressionRootRole::*;
        for (call, definition) in calls.iter().enumerate() {
            let call = ordinal(call)?;
            for (argument, expr) in definition.arguments.iter().enumerate() {
                let argument = ordinal(argument)?;
                self.add(
                    node,
                    if topn {
                        TopNStateArgument { call, argument }
                    } else {
                        AggregateArgument { call, argument }
                    },
                    *expr,
                    EvaluationDemand::Value,
                )?;
            }
            for (key, item) in definition.order_by.iter().enumerate() {
                let key = ordinal(key)?;
                self.add(
                    node,
                    if topn {
                        TopNStateOrder { call, key }
                    } else {
                        AggregateOrder { call, key }
                    },
                    item.expr,
                    EvaluationDemand::Value,
                )?;
            }
            self.work.step()?;
        }
        Ok(())
    }
}
fn ordinal(value: usize) -> Result<u32, ExpressionRootError> {
    u32::try_from(value).map_err(|_| ExpressionRootError::TooManyRoots)
}
fn join_demand(kind: JoinKind) -> EvaluationDemand {
    if kind == JoinKind::NullAwareLeftAnti {
        EvaluationDemand::Value
    } else {
        EvaluationDemand::TruthOnly
    }
}

impl PhysicalExpressionRoots {
    pub fn try_new(
        fragment: &Fragment,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ExpressionRootError> {
        use EvaluationDemand::{TruthOnly, Value};
        use ExpressionRootRole::*;
        let mut collector = RootCollector {
            fragment,
            sites: BTreeMap::new(),
            work: CompileCheckpoints::try_new(control, CompilePhase::Validate)?,
        };
        for node in fragment.nodes().values() {
            let id = node.id;
            match &node.kind {
                NodeKind::Scan {
                    residuals,
                    derived_values,
                    ..
                } => {
                    for (predicate, expr) in residuals.iter().enumerate() {
                        collector.add(
                            id,
                            ScanResidual {
                                predicate: ordinal(predicate)?,
                            },
                            *expr,
                            TruthOnly,
                        )?;
                    }
                    for (derived, value) in derived_values.iter().enumerate() {
                        let Some(crate::ValueDef {
                            origin: ValueOrigin::Expr { node: owner, expr },
                            ..
                        }) = fragment.values().get(value)
                        else {
                            return Err(ExpressionRootError::InvalidDerivedValue);
                        };
                        if *owner != id {
                            return Err(ExpressionRootError::InvalidDerivedValue);
                        }
                        collector.add(
                            id,
                            ScanDerived {
                                derived: ordinal(derived)?,
                            },
                            *expr,
                            Value,
                        )?;
                    }
                }
                NodeKind::Filter { predicates } => {
                    for (predicate, expr) in predicates.iter().enumerate() {
                        collector.add(
                            id,
                            FilterPredicate {
                                predicate: ordinal(predicate)?,
                            },
                            *expr,
                            TruthOnly,
                        )?;
                    }
                }
                NodeKind::Project { expressions } => {
                    for (expression, (expr, _)) in expressions.iter().enumerate() {
                        collector.add(
                            id,
                            ProjectOutput {
                                expression: ordinal(expression)?,
                            },
                            *expr,
                            Value,
                        )?;
                    }
                }
                NodeKind::Aggregate {
                    group_by, calls, ..
                } => {
                    for (group, (expr, _)) in group_by.iter().enumerate() {
                        collector.add(
                            id,
                            AggregateGroup {
                                group: ordinal(group)?,
                            },
                            *expr,
                            Value,
                        )?;
                    }
                    collector.calls(id, calls, false)?;
                }
                NodeKind::HashJoin {
                    kind,
                    keys,
                    residual,
                    ..
                } => {
                    for (key, definition) in keys.iter().enumerate() {
                        let key = ordinal(key)?;
                        collector.add(
                            id,
                            JoinKey {
                                key,
                                side: JoinSide::Left,
                            },
                            definition.left,
                            Value,
                        )?;
                        collector.add(
                            id,
                            JoinKey {
                                key,
                                side: JoinSide::Right,
                            },
                            definition.right,
                            Value,
                        )?;
                    }
                    if let Some(expr) = residual {
                        collector.add(id, HashJoinResidual, *expr, join_demand(*kind))?;
                    }
                }
                NodeKind::NestLoopJoin {
                    kind, predicate, ..
                } => {
                    if let Some(expr) = predicate {
                        collector.add(id, NestLoopPredicate, *expr, join_demand(*kind))?;
                    }
                }
                NodeKind::Sort { order_by, mode } => {
                    for (key, item) in order_by.iter().enumerate() {
                        collector.add(id, SortOrder { key: ordinal(key)? }, item.expr, Value)?;
                    }
                    match mode {
                        SortMode::Global => {}
                        SortMode::Analytic { partition_by }
                        | SortMode::PartitionTopN { partition_by, .. } => {
                            for (key, item) in partition_by.iter().enumerate() {
                                collector.add(
                                    id,
                                    SortPartition { key: ordinal(key)? },
                                    item.expr,
                                    Value,
                                )?;
                            }
                        }
                    }
                }
                NodeKind::TopN {
                    order_by,
                    reduction,
                    ..
                } => {
                    for (key, item) in order_by.iter().enumerate() {
                        collector.add(id, TopNOrder { key: ordinal(key)? }, item.expr, Value)?;
                    }
                    if let TopNReduction::GroupedStates {
                        group_by, calls, ..
                    } = reduction
                    {
                        for (group, (expr, _)) in group_by.iter().enumerate() {
                            collector.add(
                                id,
                                TopNGroup {
                                    group: ordinal(group)?,
                                },
                                *expr,
                                Value,
                            )?;
                        }
                        collector.calls(id, calls, true)?;
                    }
                }
                NodeKind::Window(spec) => {
                    for (key, item) in spec.partition_by.iter().enumerate() {
                        collector.add(
                            id,
                            WindowPartition { key: ordinal(key)? },
                            item.expr,
                            Value,
                        )?;
                    }
                    for (key, item) in spec.order_by.iter().enumerate() {
                        collector.add(id, WindowOrder { key: ordinal(key)? }, item.expr, Value)?;
                    }
                    for (call, item) in spec.expressions.iter().enumerate() {
                        collector.add(
                            id,
                            WindowCall {
                                call: ordinal(call)?,
                            },
                            item.expression,
                            Value,
                        )?;
                    }
                }
                NodeKind::Values { rows } => {
                    for (row, cells) in rows.iter().enumerate() {
                        for (column, expr) in cells.iter().enumerate() {
                            collector.add(
                                id,
                                ValuesCell {
                                    row: ordinal(row)?,
                                    column: ordinal(column)?,
                                },
                                *expr,
                                Value,
                            )?;
                        }
                        collector.work.step()?;
                    }
                }
                NodeKind::GenerateSeries { start, stop, step } => {
                    collector.add(id, SeriesStart, *start, Value)?;
                    collector.add(id, SeriesStop, *stop, Value)?;
                    if let Some(expr) = step {
                        collector.add(id, SeriesStep, *expr, Value)?;
                    }
                }
                NodeKind::TableFunction { arguments, .. } => {
                    for (argument, expr) in arguments.iter().enumerate() {
                        collector.add(
                            id,
                            TableFunctionArgument {
                                argument: ordinal(argument)?,
                            },
                            *expr,
                            Value,
                        )?;
                    }
                }
                NodeKind::Unpivot { spec } => {
                    for (mapping, item) in spec.mappings.iter().enumerate() {
                        for (constant, item) in item.constants.iter().enumerate() {
                            if let crate::UnpivotConstant::Scalar(expr) = item {
                                collector.add(
                                    id,
                                    ExpressionRootRole::UnpivotConstant {
                                        mapping: ordinal(mapping)?,
                                        constant: ordinal(constant)?,
                                    },
                                    *expr,
                                    Value,
                                )?;
                            }
                            collector.work.step()?;
                        }
                        collector.work.step()?;
                    }
                }
                NodeKind::ChangeEventExpand { events, .. } => {
                    for (event, item) in events.iter().enumerate() {
                        let event = ordinal(event)?;
                        if let Some(expr) = item.predicate {
                            collector.add(id, ChangePredicate { event }, expr, TruthOnly)?;
                        }
                        for (assignment, (_, expr)) in item.assignments.iter().enumerate() {
                            if let Some(expr) = expr {
                                collector.add(
                                    id,
                                    ChangeAssignment {
                                        event,
                                        assignment: ordinal(assignment)?,
                                    },
                                    *expr,
                                    Value,
                                )?;
                            }
                            collector.work.step()?;
                        }
                        collector.work.step()?;
                    }
                }
                NodeKind::TableFinish(spec) => {
                    if let Some(unpivot) = &spec.grouped_unpivot {
                        for (mapping, item) in unpivot.mappings.iter().enumerate() {
                            for (constant, item) in item.constants.iter().enumerate() {
                                if let crate::UnpivotConstant::Scalar(expr) = item {
                                    collector.add(
                                        id,
                                        FinishUnpivotConstant {
                                            mapping: ordinal(mapping)?,
                                            constant: ordinal(constant)?,
                                        },
                                        *expr,
                                        Value,
                                    )?;
                                }
                                collector.work.step()?;
                            }
                            collector.work.step()?;
                        }
                    }
                }
                NodeKind::Limit { .. }
                | NodeKind::SetOp { .. }
                | NodeKind::Repeat { .. }
                | NodeKind::AssertOneRow(_)
                | NodeKind::ExchangeSource { .. }
                | NodeKind::TableWriter { .. } => {}
            }
            collector.work.step()?;
        }
        collector.work.finish()?;
        Ok(Self {
            fragment: fragment.id(),
            sites: Arc::new(collector.sites),
        })
    }
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn sites(&self) -> &BTreeMap<ExpressionRootSite, ExprUse> {
        &self.sites
    }
}

/// Root-use binding and intrinsic definition correspondence in one fragment.
/// The pure compiler separately validates exact installed function controls
/// and operator/lifecycle row domains; an intrinsic shape proves neither.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRootUses {
    roots: PhysicalExpressionRoots,
    flow: novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
    bindings: Arc<BTreeMap<ExpressionRootSite, novarocks_type_contract::ExpressionUseId>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootUseBindingError {
    Control(CompileControlError),
    IncompleteCoverage,
    InvalidSite,
    InvalidUse,
    DuplicateSite,
    SharedUse,
    WrongDefinition,
    WrongDemand,
    GuardedRoot,
    Roots(ExpressionRootError),
    WrongControl,
    WrongArguments,
}
impl fmt::Display for RootUseBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid physical root-use binding: {self:?}")
    }
}
impl std::error::Error for RootUseBindingError {}
impl From<CompileControlError> for RootUseBindingError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl PhysicalRootUses {
    pub fn try_new(
        fragment: &Fragment,
        flow: novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
        bindings: Vec<(ExpressionRootSite, novarocks_type_contract::ExpressionUseId)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, RootUseBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let roots =
            PhysicalExpressionRoots::try_new(fragment, control).map_err(|error| match error {
                ExpressionRootError::Control(error) => RootUseBindingError::Control(error),
                error => RootUseBindingError::Roots(error),
            })?;
        if bindings.len() != roots.sites.len() || bindings.len() != flow.root_use_ids().len() {
            return Err(RootUseBindingError::IncompleteCoverage);
        }
        let mut sites = BTreeMap::new();
        let mut use_ids = std::collections::BTreeSet::new();
        for (site, id) in bindings {
            let root = roots
                .sites
                .get(&site)
                .ok_or(RootUseBindingError::InvalidSite)?;
            if flow.root_use_ids().binary_search(&id).is_err() {
                return Err(RootUseBindingError::InvalidUse);
            }
            let invocation = &flow.uses()[&id];
            if root.expr != invocation.definition {
                return Err(RootUseBindingError::WrongDefinition);
            }
            if root.demand != invocation.context.demand {
                return Err(RootUseBindingError::WrongDemand);
            }
            let domain = &flow.domains()[&invocation.context.domain];
            if domain.parent.is_some() || domain.guard.is_some() {
                return Err(RootUseBindingError::GuardedRoot);
            }
            if sites.insert(site, id).is_some() {
                return Err(RootUseBindingError::DuplicateSite);
            }
            if !use_ids.insert(id) {
                return Err(RootUseBindingError::SharedUse);
            }
            work.step()?;
        }
        validate_definition_correspondence(fragment, &flow, &mut work)?;
        work.finish()?;
        Ok(Self {
            roots,
            flow,
            bindings: Arc::new(sites),
        })
    }
    pub const fn roots(&self) -> &PhysicalExpressionRoots {
        &self.roots
    }
    pub const fn flow(&self) -> &novarocks_type_contract::ExpressionControlFlow<crate::ExprId> {
        &self.flow
    }
    pub fn bindings(
        &self,
    ) -> &BTreeMap<ExpressionRootSite, novarocks_type_contract::ExpressionUseId> {
        &self.bindings
    }
}

fn validate_definition_correspondence(
    fragment: &Fragment,
    flow: &novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), RootUseBindingError> {
    use crate::ExprKind;
    use novarocks_type_contract::ControlShape;
    for invocation in flow.uses().values() {
        let definition = fragment
            .expressions()
            .get(invocation.definition)
            .ok_or(RootUseBindingError::WrongDefinition)?;
        if invocation.context.demand == novarocks_type_contract::EvaluationDemand::TruthOnly
            && (definition.ty.data_type != arrow_schema::DataType::Boolean
                || definition.ty.logical_type
                    != novarocks_type_contract::ValueLogicalType::Physical)
        {
            return Err(RootUseBindingError::WrongDemand);
        }
        let intrinsic = match &definition.kind {
            ExprKind::Conjunction { .. } => Some(ControlShape::Conjunction),
            ExprKind::Disjunction { .. } => Some(ControlShape::Disjunction),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => Some(ControlShape::Case {
                simple: operand.is_some(),
                arms: u32::try_from(when_then.len())
                    .map_err(|_| RootUseBindingError::WrongArguments)?,
                has_else: else_expr.is_some(),
            }),
            ExprKind::Lambda { .. } => Some(ControlShape::LambdaBody),
            ExprKind::FunctionCall { .. } => None,
            ExprKind::Value(_)
            | ExprKind::LambdaParameter { .. }
            | ExprKind::Literal(_)
            | ExprKind::Unary { .. }
            | ExprKind::Binary { .. }
            | ExprKind::Cast { .. }
            | ExprKind::IsNull { .. }
            | ExprKind::InList { .. }
            | ExprKind::Between { .. }
            | ExprKind::Like { .. }
            | ExprKind::IsTruthValue { .. }
            | ExprKind::WindowCall { .. } => Some(ControlShape::Eager),
        };
        if intrinsic.is_some_and(|expected| invocation.control != expected)
            || (intrinsic.is_none()
                && matches!(
                    invocation.control,
                    ControlShape::Conjunction
                        | ControlShape::Disjunction
                        | ControlShape::LambdaBody
                ))
        {
            return Err(RootUseBindingError::WrongControl);
        }
        let mut ordinal = 0usize;
        // TypeOnly static arguments still belong to the checked definition,
        // but they are not runtime argument invocations.
        if !matches!(
            (&definition.kind, invocation.control),
            (ExprKind::FunctionCall { .. }, ControlShape::TypeOnly)
        ) {
            definition
                .kind
                .expression_references_observed(|definition| {
                    work.step()?;
                    let argument = invocation
                        .arguments
                        .get(ordinal)
                        .ok_or(RootUseBindingError::WrongArguments)?;
                    if flow.uses()[argument].definition != definition {
                        return Err(RootUseBindingError::WrongArguments);
                    }
                    ordinal += 1;
                    Ok(())
                })?;
        }
        if ordinal != invocation.arguments.len() {
            return Err(RootUseBindingError::WrongArguments);
        }
        if let (ExprKind::FunctionCall { args, .. }, ControlShape::HigherOrder { body_ordinal, .. }) =
            (&definition.kind, invocation.control)
            && !args
                .get(body_ordinal as usize)
                .and_then(|id| fragment.expressions().get(*id))
                .is_some_and(|body| matches!(body.kind, ExprKind::Lambda { .. }))
        {
            return Err(RootUseBindingError::WrongArguments);
        }
        work.step()?;
    }
    Ok(())
}
