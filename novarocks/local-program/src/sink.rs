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

//! Pure output topology. Destination addresses, transmitters and queues are
//! task-owned bindings; only branch semantics and frozen expressions live here.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_types::SlotId;

use crate::{ImmutableExpressions, ProgramExprId, StaticLayout};

/// Output semantics survive local compilation independently of destination
/// addresses and provider execution capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramOutputRole {
    Relational,
    ProviderRoute {
        target: novarocks_connector_contract::WriteTargetOrdinal,
    },
    LogicalSummary,
    WriterResults,
}

/// Ordinary streams retain their frozen historic hash mapping. The two new
/// variants use the versioned instance/driver domains at their respective
/// boundaries; FALSE/NULL applicability uses a per-row round-robin cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticStreamRouting {
    Ordinary,
    HierarchicalHashV1,
    ConditionalKeyV1 { applies: ProgramExprId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaticStreamContract {
    pub role: ProgramOutputRole,
    pub delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
    pub routing: StaticStreamRouting,
}

pub const MAX_STATIC_SINK_BRANCHES: usize = 4_096;
pub const MAX_STATIC_SINK_COLUMNS: usize = 4_096;
pub const MAX_STATIC_SINK_EXPRESSIONS: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticSinkError {
    InvalidDestinationNode,
    DuplicateOutputColumn,
    TooManyColumns,
    TooManyExpressions,
    InvalidPartitionExpressions,
    InvalidExpression,
    EmptyBranches,
    TooManyBranches,
    SplitArityMismatch,
    InvalidRouter,
    InvalidSummaryLayout,
    InvalidDeliveryRequirement,
}

impl fmt::Display for StaticSinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid static sink: {self:?}")
    }
}

impl std::error::Error for StaticSinkError {}

/// Sink shape failures and original request-control failures stay distinct.
/// Validation retains no control capability in the immutable sink.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SinkCompileError {
    Sink(StaticSinkError),
    Control(CompileControlError),
}
impl From<StaticSinkError> for SinkCompileError {
    fn from(error: StaticSinkError) -> Self {
        Self::Sink(error)
    }
}
impl From<CompileControlError> for SinkCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for SinkCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sink(error) => error.fmt(f),
            Self::Control(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SinkCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sink(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StaticStreamBranch {
    contract: StaticStreamContract,
    dest_node_id: i32,
    partition_type: DataStreamPartitionType,
    partition_exprs: Arc<[ProgramExprId]>,
    output_columns: Arc<[SlotId]>,
    limit: Option<i64>,
}

impl StaticStreamBranch {
    pub fn try_new(
        dest_node_id: i32,
        partition_type: DataStreamPartitionType,
        partition_exprs: Vec<ProgramExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
        contract: StaticStreamContract,
    ) -> Result<Self, StaticSinkError> {
        if dest_node_id < 0 {
            return Err(StaticSinkError::InvalidDestinationNode);
        }
        if partition_exprs.len() > MAX_STATIC_SINK_EXPRESSIONS {
            return Err(StaticSinkError::TooManyExpressions);
        }
        if output_columns.len() > MAX_STATIC_SINK_COLUMNS {
            return Err(StaticSinkError::TooManyColumns);
        }
        if !partition_type.requires_exprs() && !partition_exprs.is_empty() {
            return Err(StaticSinkError::InvalidPartitionExpressions);
        }
        if !matches!(contract.routing, StaticStreamRouting::Ordinary)
            && (partition_type != DataStreamPartitionType::HashPartitioned
                || partition_exprs.is_empty())
        {
            return Err(StaticSinkError::InvalidPartitionExpressions);
        }
        if contract.delivery == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
            && limit.is_some()
        {
            return Err(StaticSinkError::InvalidPartitionExpressions);
        }
        if contract.role != ProgramOutputRole::Relational
            && contract.delivery
                != novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
        {
            return Err(StaticSinkError::InvalidDeliveryRequirement);
        }
        if output_columns
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != output_columns.len()
        {
            return Err(StaticSinkError::DuplicateOutputColumn);
        }
        Ok(Self {
            contract,
            dest_node_id,
            partition_type,
            partition_exprs: Arc::from(partition_exprs),
            output_columns: Arc::from(output_columns),
            limit,
        })
    }

    pub const fn dest_node_id(&self) -> i32 {
        self.dest_node_id
    }
    pub const fn contract(&self) -> StaticStreamContract {
        self.contract
    }
    pub const fn partition_type(&self) -> DataStreamPartitionType {
        self.partition_type
    }
    pub fn partition_exprs(&self) -> &[ProgramExprId] {
        &self.partition_exprs
    }
    pub fn output_columns(&self) -> &[SlotId] {
        &self.output_columns
    }
    pub const fn limit(&self) -> Option<i64> {
        self.limit
    }
}

/// One immutable arena per stream sink. Multicast and split branches share
/// that arena, preserving exact expression IDs without per-branch duplication.
#[derive(Clone, Debug)]
pub struct StaticRouterRoute {
    pub branch: u32,
    pub accepted_effects: Arc<[novarocks_connector_contract::ConnectorRowMutationEffect]>,
}

#[derive(Clone, Debug)]
pub enum StaticSinkProgram {
    Result,
    RootResult(Arc<novarocks_result_contract::RootOutputContract>),
    Noop,
    DataStream {
        branch: StaticStreamBranch,
        arena: Arc<ImmutableExpressions>,
    },
    MultiCastDataStream {
        branches: Arc<[StaticStreamBranch]>,
        arena: Arc<ImmutableExpressions>,
    },
    ChangeStreamRouter {
        branches: Arc<[StaticStreamBranch]>,
        arena: Arc<ImmutableExpressions>,
        effect_slot: SlotId,
        routes: Arc<[StaticRouterRoute]>,
        summary_branch: u32,
        summary_layout: StaticLayout,
    },
    SplitDataStream {
        branches: Arc<[StaticStreamBranch]>,
        split_exprs: Arc<[ProgramExprId]>,
        arena: Arc<ImmutableExpressions>,
        fanout: bool,
    },
}

impl StaticSinkProgram {
    pub fn validate(&self) -> Result<(), StaticSinkError> {
        self.validate_core(&mut || Ok::<(), StaticSinkError>(()))
    }

    /// Observe the actual branch/reference validation with the original request
    /// control. Already checked branch construction and Arc ownership do not
    /// establish allocation authorization or cooperative library expansion.
    pub fn validate_for_compile(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<(), SinkCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = self.validate_core(&mut || work.step().map_err(SinkCompileError::Control));
        if matches!(&result, Err(SinkCompileError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn validate_core<E: From<StaticSinkError>>(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Result | Self::RootResult(_) | Self::Noop => Ok(()),
            Self::DataStream { branch, arena } => validate_branch_core(branch, arena, observe),
            Self::MultiCastDataStream { branches, arena } => {
                validate_branches_core(branches, arena, observe)
            }
            Self::ChangeStreamRouter {
                branches,
                arena,
                routes,
                summary_branch,
                summary_layout,
                ..
            } => validate_router_core(
                branches,
                arena,
                routes,
                *summary_branch,
                summary_layout,
                observe,
            ),
            Self::SplitDataStream {
                branches,
                split_exprs,
                arena,
                ..
            } => validate_split_core(branches, split_exprs, arena, observe),
        }
    }

    pub fn try_data_stream(
        branch: StaticStreamBranch,
        arena: Arc<ImmutableExpressions>,
    ) -> Result<Self, StaticSinkError> {
        validate_branch(&branch, &arena)?;
        Ok(Self::DataStream { branch, arena })
    }

    pub fn try_multicast(
        branches: Vec<StaticStreamBranch>,
        arena: Arc<ImmutableExpressions>,
    ) -> Result<Self, StaticSinkError> {
        validate_branches(&branches, &arena)?;
        Ok(Self::MultiCastDataStream {
            branches: Arc::from(branches),
            arena,
        })
    }

    pub fn try_split(
        branches: Vec<StaticStreamBranch>,
        split_exprs: Vec<ProgramExprId>,
        arena: Arc<ImmutableExpressions>,
        fanout: bool,
    ) -> Result<Self, StaticSinkError> {
        validate_split_core(&branches, &split_exprs, &arena, &mut || {
            Ok::<(), StaticSinkError>(())
        })?;
        Ok(Self::SplitDataStream {
            branches: Arc::from(branches),
            split_exprs: Arc::from(split_exprs),
            arena,
            fanout,
        })
    }

    pub fn try_change_stream_router(
        branches: Vec<StaticStreamBranch>,
        arena: Arc<ImmutableExpressions>,
        effect_slot: SlotId,
        routes: Vec<StaticRouterRoute>,
        summary_branch: u32,
        summary_layout: StaticLayout,
    ) -> Result<Self, StaticSinkError> {
        validate_router_core(
            &branches,
            &arena,
            &routes,
            summary_branch,
            &summary_layout,
            &mut || Ok::<(), StaticSinkError>(()),
        )?;
        Ok(Self::ChangeStreamRouter {
            branches: Arc::from(branches),
            arena,
            effect_slot,
            routes: Arc::from(routes),
            summary_branch,
            summary_layout,
        })
    }

    pub fn branches(&self) -> &[StaticStreamBranch] {
        match self {
            Self::DataStream { branch, .. } => std::slice::from_ref(branch),
            Self::MultiCastDataStream { branches, .. }
            | Self::SplitDataStream { branches, .. }
            | Self::ChangeStreamRouter { branches, .. } => branches,
            Self::Result | Self::RootResult(_) | Self::Noop => &[],
        }
    }

    pub fn arena(&self) -> Option<&Arc<ImmutableExpressions>> {
        match self {
            Self::DataStream { arena, .. }
            | Self::MultiCastDataStream { arena, .. }
            | Self::SplitDataStream { arena, .. }
            | Self::ChangeStreamRouter { arena, .. } => Some(arena),
            Self::Result | Self::RootResult(_) | Self::Noop => None,
        }
    }

    pub fn split_exprs(&self) -> &[ProgramExprId] {
        match self {
            Self::SplitDataStream { split_exprs, .. } => split_exprs,
            _ => &[],
        }
    }

    pub const fn fanout(&self) -> bool {
        matches!(self, Self::SplitDataStream { fanout: true, .. })
    }
}

fn validate_router_core<E: From<StaticSinkError>>(
    branches: &[StaticStreamBranch],
    arena: &ImmutableExpressions,
    routes: &[StaticRouterRoute],
    summary_branch: u32,
    summary_layout: &StaticLayout,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    validate_branches_core(branches, arena, observe)?;
    let summary_index =
        usize::try_from(summary_branch).map_err(|_| StaticSinkError::InvalidRouter)?;
    let summary = branches
        .get(summary_index)
        .ok_or(StaticSinkError::InvalidRouter)?;
    observe()?;
    if routes.is_empty()
        || routes.len().checked_add(1) != Some(branches.len())
        || summary.contract.role != ProgramOutputRole::LogicalSummary
        || summary.contract.delivery
            != novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
        || summary.partition_type != DataStreamPartitionType::Unpartitioned
        || !matches!(summary.contract.routing, StaticStreamRouting::Ordinary)
        || !summary.partition_exprs.is_empty()
        || summary.limit.is_some()
        || summary.output_columns() != summary_layout.slots()
    {
        return Err(StaticSinkError::InvalidRouter.into());
    }
    if summary_layout.slots().len() != 3 || summary_layout.schema().fields().len() != 3 {
        return Err(StaticSinkError::InvalidSummaryLayout.into());
    }
    for field in summary_layout.schema().fields() {
        let valid = field.data_type() == &arrow_schema::DataType::Int64 && !field.is_nullable();
        observe()?;
        if !valid {
            return Err(StaticSinkError::InvalidSummaryLayout.into());
        }
    }
    let mut covered = BTreeSet::new();
    let mut targets = BTreeSet::new();
    covered.insert(summary_index);
    for route in routes {
        let index = usize::try_from(route.branch).map_err(|_| StaticSinkError::InvalidRouter)?;
        let branch = branches.get(index).ok_or(StaticSinkError::InvalidRouter)?;
        let ProgramOutputRole::ProviderRoute { target } = branch.contract.role else {
            return Err(StaticSinkError::InvalidRouter.into());
        };
        let valid = covered.insert(index)
            && targets.insert(target)
            && branch.contract.delivery
                == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
            && !route.accepted_effects.is_empty();
        observe()?;
        if !valid {
            return Err(StaticSinkError::InvalidRouter.into());
        }
        let mut effects = BTreeSet::new();
        for effect in route.accepted_effects.iter() {
            let unique = effects.insert(*effect);
            observe()?;
            if !unique {
                return Err(StaticSinkError::InvalidRouter.into());
            }
        }
    }
    // Cardinality plus unique in-range occurrences proves complete coverage.
    if covered.len() != branches.len() {
        return Err(StaticSinkError::InvalidRouter.into());
    }
    Ok(())
}

fn validate_branches(
    branches: &[StaticStreamBranch],
    arena: &ImmutableExpressions,
) -> Result<(), StaticSinkError> {
    validate_branches_core(branches, arena, &mut || Ok::<(), StaticSinkError>(()))
}
fn validate_branches_core<E: From<StaticSinkError>>(
    branches: &[StaticStreamBranch],
    arena: &ImmutableExpressions,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    let empty = branches.is_empty();
    observe()?;
    if empty {
        return Err(StaticSinkError::EmptyBranches.into());
    }
    let too_many = branches.len() > MAX_STATIC_SINK_BRANCHES;
    observe()?;
    if too_many {
        return Err(StaticSinkError::TooManyBranches.into());
    }
    for branch in branches {
        validate_branch_core(branch, arena, observe)?;
        // The branch's checked reference walk is now complete.
        observe()?;
    }
    Ok(())
}
fn validate_branch(
    branch: &StaticStreamBranch,
    arena: &ImmutableExpressions,
) -> Result<(), StaticSinkError> {
    validate_branch_core(branch, arena, &mut || Ok::<(), StaticSinkError>(()))
}
fn validate_branch_core<E: From<StaticSinkError>>(
    branch: &StaticStreamBranch,
    arena: &ImmutableExpressions,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    if let StaticStreamRouting::ConditionalKeyV1 { applies } = branch.contract.routing {
        let valid = arena
            .node(applies)
            .is_some_and(|node| node.data_type() == &arrow_schema::DataType::Boolean);
        observe()?;
        if !valid {
            return Err(StaticSinkError::InvalidExpression.into());
        }
    }
    for id in branch.partition_exprs.iter() {
        let valid = arena.node(*id).is_some_and(|node| {
            matches!(branch.contract.routing, StaticStreamRouting::Ordinary)
                || novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1
                    .supports_partition_key(node.data_type())
        });
        observe()?;
        if !valid {
            return Err(StaticSinkError::InvalidExpression.into());
        }
    }
    Ok(())
}
fn validate_split_core<E: From<StaticSinkError>>(
    branches: &[StaticStreamBranch],
    split_exprs: &[ProgramExprId],
    arena: &ImmutableExpressions,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    validate_branches_core(branches, arena, observe)?;
    let arity_matches = branches.len() == split_exprs.len();
    observe()?;
    if !arity_matches {
        return Err(StaticSinkError::SplitArityMismatch.into());
    }
    for id in split_exprs {
        let valid = arena.node(*id).is_some();
        observe()?;
        if !valid {
            return Err(StaticSinkError::InvalidExpression.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::DataType;

    use super::*;
    use crate::{StaticExprKind, StaticExprNode};

    fn arena() -> Arc<ImmutableExpressions> {
        Arc::new(
            ImmutableExpressions::try_new(
                vec![StaticExprNode::new(
                    StaticExprKind::SlotId(SlotId::new(1)),
                    DataType::Int64,
                    None,
                )],
                false,
                HashMap::new(),
                None,
            )
            .unwrap(),
        )
    }

    #[test]
    fn exact_stream_and_split_semantics_share_one_pure_arena() {
        let arena = arena();
        let branch = StaticStreamBranch::try_new(
            9,
            DataStreamPartitionType::HashPartitioned,
            vec![ProgramExprId::new(0)],
            vec![SlotId::new(1)],
            Some(7),
            crate::StaticStreamContract {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                routing: crate::StaticStreamRouting::Ordinary,
            },
        )
        .unwrap();
        let split = StaticSinkProgram::try_split(
            vec![branch.clone(), branch],
            vec![ProgramExprId::new(0); 2],
            Arc::clone(&arena),
            true,
        )
        .unwrap();
        assert!(Arc::ptr_eq(split.arena().unwrap(), &arena));
        assert_eq!(split.branches().len(), 2);
        assert_eq!(split.branches()[0].limit(), Some(7));
        assert!(split.fanout());
    }

    #[test]
    fn invalid_branch_and_split_fail_before_runtime_binding() {
        let arena = arena();
        let duplicate = StaticStreamBranch::try_new(
            9,
            DataStreamPartitionType::Random,
            vec![],
            vec![SlotId::new(1), SlotId::new(1)],
            None,
            crate::StaticStreamContract {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                routing: crate::StaticStreamRouting::Ordinary,
            },
        );
        assert!(matches!(
            duplicate,
            Err(StaticSinkError::DuplicateOutputColumn)
        ));
        let branch = StaticStreamBranch::try_new(
            9,
            DataStreamPartitionType::HashPartitioned,
            vec![ProgramExprId::new(0)],
            vec![SlotId::new(1)],
            None,
            crate::StaticStreamContract {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                routing: crate::StaticStreamRouting::Ordinary,
            },
        )
        .unwrap();
        assert!(matches!(
            StaticSinkProgram::try_split(vec![branch.clone()], vec![], Arc::clone(&arena), false),
            Err(StaticSinkError::SplitArityMismatch)
        ));
        assert!(matches!(
            StaticSinkProgram::try_multicast(vec![], Arc::clone(&arena)),
            Err(StaticSinkError::EmptyBranches)
        ));
        assert!(matches!(
            StaticSinkProgram::try_data_stream(
                StaticStreamBranch::try_new(
                    9,
                    DataStreamPartitionType::HashPartitioned,
                    vec![ProgramExprId::new(2)],
                    vec![],
                    None,
                    crate::StaticStreamContract {
                        role: crate::ProgramOutputRole::Relational,
                        delivery:
                            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                        routing: crate::StaticStreamRouting::Ordinary
                    }
                )
                .unwrap(),
                arena
            ),
            Err(StaticSinkError::InvalidExpression)
        ));
    }
    struct Control {
        trace: std::sync::Mutex<Vec<u32>>,
        stop: Option<(usize, novarocks_type_contract::CompileControlError)>,
    }
    impl Control {
        fn new(stop: Option<(usize, CompileControlError)>) -> Self {
            Self {
                trace: std::sync::Mutex::new(Vec::new()),
                stop,
            }
        }
        fn trace(&self) -> Vec<u32> {
            self.trace.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::LowerProgram);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let index = trace.len();
            trace.push(units);
            if let Some((at, cause)) = self.stop.clone()
                && at == index
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn branch(expressions: Vec<ProgramExprId>) -> StaticStreamBranch {
        StaticStreamBranch::try_new(
            9,
            DataStreamPartitionType::HashPartitioned,
            expressions,
            Vec::new(),
            None,
            crate::StaticStreamContract {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                routing: crate::StaticStreamRouting::Ordinary,
            },
        )
        .unwrap()
    }
    fn all_causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn assert_refusal_at_each_checkpoint(sink: &StaticSinkProgram) -> Vec<u32> {
        let baseline = Control::new(None);
        sink.validate_for_compile(&baseline).unwrap();
        let trace = baseline.trace();
        for at in 0..trace.len() {
            for cause in all_causes() {
                let control = Control::new(Some((at, cause.clone())));
                assert_eq!(
                    sink.validate_for_compile(&control),
                    Err(SinkCompileError::Control(cause))
                );
                assert_eq!(
                    control.trace(),
                    trace[..=at],
                    "first failure must not recheck control"
                );
            }
        }
        trace
    }

    #[test]
    fn compile_partition_reference_walk_observes_actual_quantum_and_tail() {
        let sink =
            StaticSinkProgram::try_data_stream(branch(vec![ProgramExprId::new(0); 320]), arena())
                .unwrap();
        let trace = assert_refusal_at_each_checkpoint(&sink);
        assert_eq!(trace, [0, 256, 64]);
        sink.validate().unwrap();
    }

    #[test]
    fn compile_branch_and_split_walks_use_one_original_control_scope() {
        let sink = StaticSinkProgram::try_split(
            vec![branch(Vec::new()); 320],
            vec![ProgramExprId::new(0); 320],
            arena(),
            true,
        )
        .unwrap();
        let trace = assert_refusal_at_each_checkpoint(&sink);
        // Two branch-count checks, 320 completed branches, one arity check,
        // and 320 actual split-reference checks, without a per-branch reset.
        assert_eq!(trace, [0, 256, 256, 131]);
        assert!(sink.fanout());
        assert!(
            sink.branches()
                .iter()
                .all(|branch| branch.limit().is_none() && branch.output_columns().is_empty())
        );
    }

    #[test]
    fn compile_noop_and_result_keep_empty_success_and_observe_entry_finish() {
        for sink in [StaticSinkProgram::Noop, StaticSinkProgram::Result] {
            sink.validate().unwrap();
            assert_eq!(assert_refusal_at_each_checkpoint(&sink), [0, 0]);
            assert!(sink.arena().is_none());
            assert!(sink.branches().is_empty());
        }
        let sink = StaticSinkProgram::try_data_stream(branch(Vec::new()), arena()).unwrap();
        assert_eq!(assert_refusal_at_each_checkpoint(&sink), [0, 0]);
    }

    #[test]
    fn compile_ordinary_error_tail_keeps_exact_sink_failure_and_control_cause() {
        let cases = [
            (
                StaticSinkProgram::MultiCastDataStream {
                    branches: Arc::from([]),
                    arena: arena(),
                },
                StaticSinkError::EmptyBranches,
                1,
            ),
            (
                StaticSinkProgram::SplitDataStream {
                    branches: Arc::from([branch(Vec::new())]),
                    split_exprs: Arc::from([]),
                    arena: arena(),
                    fanout: false,
                },
                StaticSinkError::SplitArityMismatch,
                4,
            ),
            (
                StaticSinkProgram::DataStream {
                    branch: branch(vec![ProgramExprId::new(1)]),
                    arena: arena(),
                },
                StaticSinkError::InvalidExpression,
                1,
            ),
        ];
        for (sink, expected, units) in cases {
            assert_eq!(sink.validate(), Err(expected));
            let control = Control::new(None);
            assert_eq!(
                sink.validate_for_compile(&control),
                Err(SinkCompileError::Sink(expected))
            );
            assert_eq!(control.trace(), [0, units]);
            for cause in all_causes() {
                let control = Control::new(Some((1, cause.clone())));
                assert_eq!(
                    sink.validate_for_compile(&control),
                    Err(SinkCompileError::Control(cause))
                );
                assert_eq!(control.trace(), [0, units]);
            }
        }
    }

    #[test]
    fn compile_preserves_original_branch_limit_and_shared_arena_contract() {
        let source = arena();
        let branch = StaticStreamBranch::try_new(
            7,
            DataStreamPartitionType::HashPartitioned,
            vec![ProgramExprId::new(0)],
            vec![SlotId::new(9), SlotId::new(3)],
            Some(-1),
            crate::StaticStreamContract {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                routing: crate::StaticStreamRouting::Ordinary,
            },
        )
        .unwrap();
        let sink =
            StaticSinkProgram::try_multicast(vec![branch.clone(), branch], source.clone()).unwrap();
        sink.validate_for_compile(&Control::new(None)).unwrap();
        sink.validate().unwrap();
        assert!(Arc::ptr_eq(sink.arena().unwrap(), &source));
        assert_eq!(
            sink.branches()[0].output_columns(),
            [SlotId::new(9), SlotId::new(3)]
        );
        assert_eq!(sink.branches()[0].limit(), Some(-1));
        let max = StaticSinkProgram::MultiCastDataStream {
            branches: vec![branch_empty(); MAX_STATIC_SINK_BRANCHES].into(),
            arena: source.clone(),
        };
        max.validate_for_compile(&Control::new(None)).unwrap();
        let over = StaticSinkProgram::MultiCastDataStream {
            branches: vec![branch_empty(); MAX_STATIC_SINK_BRANCHES + 1].into(),
            arena: source,
        };
        assert_eq!(over.validate(), Err(StaticSinkError::TooManyBranches));
        assert_eq!(
            over.validate_for_compile(&Control::new(None)),
            Err(SinkCompileError::Sink(StaticSinkError::TooManyBranches))
        );
    }
    fn branch_empty() -> StaticStreamBranch {
        branch(Vec::new())
    }

    #[test]
    fn compile_error_sources_retain_typed_original_owners() {
        use std::error::Error;
        let sink = SinkCompileError::Sink(StaticSinkError::InvalidExpression);
        assert_eq!(
            sink.source().unwrap().downcast_ref::<StaticSinkError>(),
            Some(&StaticSinkError::InvalidExpression)
        );
        let control = SinkCompileError::Control(CompileControlError::DeadlineExceeded);
        assert_eq!(
            control
                .source()
                .unwrap()
                .downcast_ref::<CompileControlError>(),
            Some(&CompileControlError::DeadlineExceeded)
        );
    }
}

#[cfg(test)]
mod router_contract_tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use novarocks_connector_contract::{ConnectorRowMutationEffect, WriteTargetOrdinal};
    use std::collections::HashMap;

    fn fixture() -> (
        Vec<StaticStreamBranch>,
        Arc<ImmutableExpressions>,
        Vec<StaticRouterRoute>,
        StaticLayout,
    ) {
        let summary = StaticLayout::try_new(
            Arc::new(Schema::new(vec![
                Field::new("inserted", DataType::Int64, false),
                Field::new("replaced", DataType::Int64, false),
                Field::new("deleted", DataType::Int64, false),
            ])),
            Arc::from([SlotId::new(10), SlotId::new(11), SlotId::new(12)]),
        )
        .unwrap();
        let branch = |node, columns, role| {
            StaticStreamBranch::try_new(
                node,
                DataStreamPartitionType::Unpartitioned,
                vec![],
                columns,
                None,
                StaticStreamContract {
                    role,
                    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain,
                    routing: StaticStreamRouting::Ordinary,
                },
            )
            .unwrap()
        };
        let branches = vec![
            branch(
                1,
                vec![SlotId::new(2)],
                ProgramOutputRole::ProviderRoute {
                    target: WriteTargetOrdinal::try_new(0).unwrap(),
                },
            ),
            branch(
                2,
                summary.slots().to_vec(),
                ProgramOutputRole::LogicalSummary,
            ),
        ];
        let arena =
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap());
        let routes = vec![StaticRouterRoute {
            branch: 0,
            accepted_effects: Arc::from([ConnectorRowMutationEffect::Insert]),
        }];
        (branches, arena, routes, summary)
    }
    #[test]
    fn router_keeps_a_separate_summary_layout_and_covers_branches_once() {
        let (branches, arena, routes, summary) = fixture();
        let sink = StaticSinkProgram::try_change_stream_router(
            branches.clone(),
            arena.clone(),
            SlotId::new(1),
            routes.clone(),
            1,
            summary.clone(),
        )
        .unwrap();
        assert_eq!(sink.branches()[1].output_columns(), summary.slots());
        let mut duplicate = routes;
        duplicate.push(duplicate[0].clone());
        assert!(
            StaticSinkProgram::try_change_stream_router(
                branches,
                arena,
                SlotId::new(1),
                duplicate,
                1,
                summary
            )
            .is_err()
        );
    }
    #[test]
    fn summary_cannot_claim_a_provider_route_or_nullable_counters() {
        let (mut branches, arena, routes, summary) = fixture();
        branches[1].contract.role = ProgramOutputRole::Relational;
        assert!(
            StaticSinkProgram::try_change_stream_router(
                branches,
                arena,
                SlotId::new(1),
                routes,
                1,
                summary
            )
            .is_err()
        );
        let (branches, arena, routes, _) = fixture();
        let nullable = StaticLayout::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
            ])),
            Arc::from([SlotId::new(10), SlotId::new(11), SlotId::new(12)]),
        )
        .unwrap();
        assert!(
            StaticSinkProgram::try_change_stream_router(
                branches,
                arena,
                SlotId::new(1),
                routes,
                1,
                nullable
            )
            .is_err()
        );
    }
}

#[cfg(test)]
#[path = "sink/stream_contract_tests.rs"]
mod stream_contract_tests;
