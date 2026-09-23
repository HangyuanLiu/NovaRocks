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

//! The stage and task graph derived from the frozen static schedule.
//!
//! One fragment becomes one stage and one fragment instance becomes one task.
//! Both ids are minted here, once, in the schedule's own deterministic order,
//! and neither is reused inside a query execution.
//!
//! The kernel key is not minted here. `FragmentInstancePlacement::finst_id` is
//! already the one derivation of a fragment instance id in this crate, so a
//! descriptor carries that exact value for itself and for every destination
//! and source of its topology. Deriving it a second time would create a
//! second authority over the same fact; the only thing this builder adds is a
//! collision check, which is what makes the identity-to-kernel-key mapping
//! provably one-to-one rather than assumed to be.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::native::fragment_encoder::frozen::FragmentArtifact;
use crate::query_execution::artifact::FragmentId;
use crate::query_execution::artifact::NativePlacementAssignment;
use crate::query_execution::attempt_plan_facts::AttemptPartitionKind;
use novarocks_execution::exec::fragment::program::FragmentNodeId;
use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    ExchangeEdgeId, PlanNodeId, QueryContextRef, StageRef, TaskIdentity,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::UniqueId;
use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};

use super::creation::{
    AttemptTopology, EdgeDestination, EdgeProducer, SharedEdge, TaskCreationFacts,
    TaskCreationSeed, TaskTopologyView,
};
use super::error::{CapacityBound, TaskExecutionError, schedule_error};
use crate::query_execution::artifact::{BoundManifestPartitionKind, TaskManifestBinding};
use crate::query_execution::schedule::FragmentInstancePlacement;
use crate::query_execution::schedule::SchedulingPlan;

/// What one fragment instance is created from.
///
/// The frozen fragment is shared by every instance of its fragment; only the
/// assignment is this instance's own.
#[derive(Debug)]
pub struct FragmentPlanFacts {
    pub(crate) fragment: Arc<FragmentArtifact>,
    pub(crate) assignment: NativePlacementAssignment,
}

/// Binds each static sink branch of one producer fragment to the logical edge
/// the task graph minted for it.
///
/// The branches are read from the frozen fragment's typed facts in declared
/// order; the edges are the producer stage's own. Every task of the fragment
/// produces on the same logical edges, so one binding serves all of them.
fn bind_sink_edges(
    fragment: &FragmentArtifact,
    outbound: &[&EdgeNode],
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
) -> Result<Arc<[ExchangeEdgeId]>, TaskExecutionError> {
    let targets = fragment.facts().sink_targets();
    if targets.len() != outbound.len() {
        return Err(TaskExecutionError::Schedule(
            "static sink count differs from the task's outbound edge count".to_owned(),
        ));
    }
    let mut used = BTreeSet::new();
    let mut sink_edges = Vec::with_capacity(targets.len());
    for target in targets {
        let fragment_id = target.target_fragment_id();
        let node_id = target.target_exchange_node_id();
        let target_stage = stage_of_fragment.get(&fragment_id).ok_or_else(|| {
            TaskExecutionError::Schedule(format!(
                "static sink targets absent fragment {fragment_id}"
            ))
        })?;
        let matching = outbound
            .iter()
            .filter(|edge| {
                edge.destination_node_id().get() == node_id
                    && edge.consumer_stage() == *target_stage
            })
            .collect::<Vec<_>>();
        let [edge] = matching.as_slice() else {
            return Err(TaskExecutionError::Schedule(format!(
                "static sink target fragment {fragment_id} node {node_id} does not identify exactly one task edge"
            )));
        };
        if !used.insert(edge.edge_id()) {
            return Err(TaskExecutionError::Schedule(format!(
                "task edge {} is bound to multiple static sinks",
                edge.edge_id()
            )));
        }
        sink_edges.push(edge.edge_id());
    }
    Ok(sink_edges.into())
}

/// Where a fragment instance's creation facts come from.
///
/// Keyed by instance, not merely by fragment. Every placement of a fragment
/// shares its frozen fragment, while its assignment carries that instance's
/// own ordinal, width and initial scan ranges. The graph binds sink branches
/// to edges only after it has minted the edges.
pub trait FragmentPlanSource {
    /// Moves one instance's creation facts out of the source.
    ///
    /// Taken rather than borrowed: an instance's initial scan ranges are held
    /// by exactly one owner at a time, and after the graph binds them into a
    /// creation seed the source no longer has a copy.
    fn take_plan(
        &mut self,
        fragment_id: FragmentId,
        instance_index: usize,
    ) -> Result<FragmentPlanFacts, TaskExecutionError>;
}

/// Immutable per-task facts, ids only.
///
/// The descriptor is deliberately absent: it is moved into the owning
/// `RemoteTask` exactly once, and everything that only needs an address reads
/// this instead of holding a second copy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskNode {
    identity: TaskIdentity,
    context: QueryContextRef,
    fragment_id: FragmentId,
    instance_index: usize,
    fragment_instance_id: UniqueId,
    split_plan_nodes: Vec<PlanNodeId>,
}

impl TaskNode {
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn task_id(&self) -> TaskId {
        self.identity.task_id()
    }

    pub const fn stage_id(&self) -> StageId {
        self.identity.stage_id()
    }

    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub const fn instance_index(&self) -> usize {
        self.instance_index
    }

    /// The execution kernel's key for this task, taken from the frozen
    /// schedule rather than derived again.
    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    /// The plan nodes of this task that accept split assignments.
    pub fn split_plan_nodes(&self) -> &[PlanNodeId] {
        &self.split_plan_nodes
    }
}

/// One stage: the complete, frozen task set of one fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageNode {
    stage: StageRef,
    fragment_id: FragmentId,
    tasks: Vec<TaskId>,
}

impl StageNode {
    pub const fn stage(&self) -> StageRef {
        self.stage
    }

    pub const fn stage_id(&self) -> StageId {
        self.stage.stage_id()
    }

    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub fn tasks(&self) -> &[TaskId] {
        &self.tasks
    }
}

/// One frozen push exchange edge: the producers of one stage and the complete
/// destination set of one exchange node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeNode {
    edge_id: ExchangeEdgeId,
    producer_stage: StageId,
    consumer_stage: StageId,
    destination_node_id: FragmentNodeId,
    producers: Vec<TaskIdentity>,
    destinations: Vec<TaskIdentity>,
}

impl EdgeNode {
    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    pub const fn producer_stage(&self) -> StageId {
        self.producer_stage
    }

    pub const fn consumer_stage(&self) -> StageId {
        self.consumer_stage
    }

    pub const fn destination_node_id(&self) -> FragmentNodeId {
        self.destination_node_id
    }

    pub fn producers(&self) -> &[TaskIdentity] {
        &self.producers
    }

    /// Every destination whose creation must be acknowledged before this edge
    /// may be opened.
    pub fn destinations(&self) -> &[TaskIdentity] {
        &self.destinations
    }
}

/// The complete stage and task graph of one query execution attempt.
#[derive(Debug)]
pub struct TaskGraph {
    execution_id: QueryExecutionId,
    frontend_process_id: FrontendProcessId,
    root_task: TaskId,
    tasks: BTreeMap<TaskId, TaskNode>,
    stages: BTreeMap<StageId, StageNode>,
    edges: BTreeMap<ExchangeEdgeId, EdgeNode>,
    producer_stages: BTreeMap<StageId, BTreeSet<StageId>>,
    contexts: BTreeSet<QueryContextRef>,
    seeds: BTreeMap<TaskId, TaskCreationSeed>,
}

impl TaskGraph {
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn frontend_process_id(&self) -> FrontendProcessId {
        self.frontend_process_id
    }

    pub const fn root_task(&self) -> TaskId {
        self.root_task
    }

    pub fn root_identity(&self) -> TaskIdentity {
        self.tasks
            .get(&self.root_task)
            .expect("a validated graph contains its root task")
            .identity()
    }

    pub fn task(&self, task_id: TaskId) -> Option<&TaskNode> {
        self.tasks.get(&task_id)
    }

    pub fn tasks(&self) -> impl ExactSizeIterator<Item = &TaskNode> + '_ {
        self.tasks.values()
    }

    pub fn stage(&self, stage_id: StageId) -> Option<&StageNode> {
        self.stages.get(&stage_id)
    }

    pub fn stages(&self) -> impl ExactSizeIterator<Item = &StageNode> + '_ {
        self.stages.values()
    }

    pub fn edges(&self) -> impl ExactSizeIterator<Item = &EdgeNode> + '_ {
        self.edges.values()
    }

    pub fn edge(&self, edge_id: ExchangeEdgeId) -> Option<&EdgeNode> {
        self.edges.get(&edge_id)
    }

    /// The stages that feed `stage_id`, which are exactly the stages this one
    /// releases when it stops consuming.
    pub fn producer_stages(&self, stage_id: StageId) -> impl Iterator<Item = StageId> + '_ {
        self.producer_stages
            .get(&stage_id)
            .into_iter()
            .flatten()
            .copied()
    }

    /// Every query context this attempt establishes: exactly one per backend
    /// process carrying at least one task.
    pub fn contexts(&self) -> impl ExactSizeIterator<Item = &QueryContextRef> + '_ {
        self.contexts.iter()
    }

    /// Takes the creation seeds out so each one is owned by exactly one task
    /// from here on.
    pub(crate) fn into_seeds(mut self) -> (Self, BTreeMap<TaskId, TaskCreationSeed>) {
        let seeds = std::mem::take(&mut self.seeds);
        (self, seeds)
    }

    /// The creation seed of one task, while the graph still owns it.
    #[cfg(test)]
    pub(crate) fn seed(&self, task_id: TaskId) -> Option<&TaskCreationSeed> {
        self.seeds.get(&task_id)
    }
}

/// Everything the graph builder reads.
#[derive(Clone, Debug)]
pub struct TaskGraphInputs<'a> {
    pub execution_id: QueryExecutionId,
    pub frontend_process_id: FrontendProcessId,
    pub root_fragment_id: FragmentId,
    pub placements: &'a BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    pub fragment_edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    pub backend_process_ids: &'a BTreeMap<usize, BackendProcessId>,
    pub transport_budget: TransportBudget,
}

impl<'a> TaskGraphInputs<'a> {
    /// Reads the frozen placement result of one validated schedule.
    #[allow(
        dead_code,
        reason = "The task protocol is not routed into production yet; this reads the crate-private schedule for the transport cutover and for this module's tests."
    )]
    pub(crate) fn from_schedule(
        execution_id: QueryExecutionId,
        frontend_process_id: FrontendProcessId,
        schedule: &'a SchedulingPlan,
        fragment_edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
        backend_process_ids: &'a BTreeMap<usize, BackendProcessId>,
        transport_budget: TransportBudget,
    ) -> Self {
        Self {
            execution_id,
            frontend_process_id,
            root_fragment_id: schedule.root_fragment_id,
            placements: &schedule.by_fragment,
            fragment_edges,
            backend_process_ids,
            transport_budget,
        }
    }
}

/// One exchange node's merged sender set.
///
/// Sender ordinals and the sender count are derived once per exchange node
/// over every incoming edge, so the producer's outbound count and the
/// consumer's `expected_sender_count` are equal by construction. A union whose
/// exchange node is fed by two fragments therefore produces two edges over one
/// shared ordinal space, rather than two edges each claiming to be the whole
/// sender set.
struct SenderSet {
    ordinals: BTreeMap<TaskId, u32>,
    count: NonZeroU32,
    partitioning: DataStreamPartitionType,
}

/// Builds the stage and task graph of one attempt from its frozen schedule.
pub fn build_task_graph(
    inputs: TaskGraphInputs<'_>,
    plans: &mut dyn FragmentPlanSource,
) -> Result<TaskGraph, TaskExecutionError> {
    if inputs.placements.is_empty() {
        return Err(TaskExecutionError::Schedule(
            "schedule places no fragment".to_owned(),
        ));
    }
    if !inputs.placements.contains_key(&inputs.root_fragment_id) {
        return Err(TaskExecutionError::Schedule(format!(
            "root fragment {} is absent from the schedule",
            inputs.root_fragment_id
        )));
    }

    // Stage ids follow the schedule's own ascending fragment order and task
    // ids come from one counter spanning the attempt, so neither is reused
    // inside this query execution.
    let mut stage_of_fragment = BTreeMap::<FragmentId, StageId>::new();
    let mut next_stage = 1_u32;
    for &fragment_id in inputs.placements.keys() {
        stage_of_fragment.insert(
            fragment_id,
            StageId::new(next_stage)
                .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?,
        );
        next_stage = next_stage
            .checked_add(1)
            .ok_or_else(|| TaskExecutionError::Schedule("stage id space exhausted".to_owned()))?;
    }

    let mut tasks = BTreeMap::<TaskId, TaskNode>::new();
    let mut task_of_instance = BTreeMap::<(FragmentId, usize), TaskId>::new();
    let mut stage_tasks = BTreeMap::<StageId, Vec<TaskId>>::new();
    let mut endpoints = BTreeMap::<TaskId, RuntimeEndpoint>::new();
    let mut contexts = BTreeSet::<QueryContextRef>::new();
    let mut context_task_counts = BTreeMap::<QueryContextRef, usize>::new();
    let mut kernel_keys = BTreeMap::<UniqueId, TaskId>::new();
    let mut next_task = 1_u32;

    for (&fragment_id, placements) in inputs.placements {
        if placements.is_empty() {
            return Err(TaskExecutionError::Schedule(format!(
                "fragment {fragment_id} has no placement"
            )));
        }
        let stage_id = stage_of_fragment[&fragment_id];
        for placement in placements {
            let backend_process_id = *inputs
                .backend_process_ids
                .get(&placement.backend_idx)
                .ok_or(TaskExecutionError::UnknownBackend {
                    backend_idx: placement.backend_idx,
                })?;
            let task_id = TaskId::new(next_task)
                .map_err(|error| schedule_error(placement, error.to_string()))?;
            next_task = next_task.checked_add(1).ok_or_else(|| {
                TaskExecutionError::Schedule("task id space exhausted".to_owned())
            })?;
            if let Some(previous) = kernel_keys.insert(placement.finst_id, task_id) {
                return Err(TaskExecutionError::KernelKeyCollision {
                    first: previous,
                    second: task_id,
                });
            }
            let identity =
                TaskIdentity::new(inputs.execution_id, stage_id, task_id, backend_process_id);
            let context = QueryContextRef::new(
                inputs.execution_id,
                inputs.frontend_process_id,
                backend_process_id,
            );
            let tasks_in_context = context_task_counts.entry(context).or_default();
            *tasks_in_context += 1;
            if *tasks_in_context > inputs.transport_budget.max_tasks_per_context() {
                return Err(CapacityBound::TasksPerContext {
                    limit: inputs.transport_budget.max_tasks_per_context(),
                }
                .into());
            }
            contexts.insert(context);
            let split_plan_nodes = placement
                .scan_ranges
                .keys()
                .map(|&node_id| {
                    PlanNodeId::new(node_id)
                        .map_err(|error| schedule_error(placement, error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if task_of_instance
                .insert((fragment_id, placement.instance_index), task_id)
                .is_some()
            {
                return Err(schedule_error(placement, "instance index repeats"));
            }
            endpoints.insert(task_id, placement.endpoint.clone());
            tasks.insert(
                task_id,
                TaskNode {
                    identity,
                    context,
                    fragment_id,
                    instance_index: placement.instance_index,
                    fragment_instance_id: placement.finst_id,
                    split_plan_nodes,
                },
            );
            stage_tasks.entry(stage_id).or_default().push(task_id);
        }
    }

    let root_task = *task_of_instance
        .get(&(inputs.root_fragment_id, 0))
        .ok_or_else(|| {
            TaskExecutionError::Schedule(format!(
                "root fragment {} has no instance zero",
                inputs.root_fragment_id
            ))
        })?;
    let sender_sets = build_sender_sets(&inputs, &stage_of_fragment, &task_of_instance)?;
    let (edges, shared_edges) = build_schedule_edges(
        &inputs,
        &stage_of_fragment,
        &task_of_instance,
        &tasks,
        &endpoints,
        &sender_sets,
    )?;
    let views = task_topology_views(&tasks, &edges);
    let topology = Arc::new(AttemptTopology::try_new(
        shared_edges,
        &views.values().collect::<Vec<_>>(),
    )?);

    let mut seeds = BTreeMap::<TaskId, TaskCreationSeed>::new();
    let mut sink_bindings = BTreeMap::<StageId, Arc<[ExchangeEdgeId]>>::new();
    for (&fragment_id, placements) in inputs.placements {
        for placement in placements {
            let facts = plans.take_plan(fragment_id, placement.instance_index)?;
            let task_id = task_of_instance[&(fragment_id, placement.instance_index)];
            let node = &tasks[&task_id];
            let seed = bind_creation_seed(
                &topology,
                &views,
                &edges,
                &stage_of_fragment,
                &mut sink_bindings,
                node,
                facts,
                inputs.transport_budget,
            )?;
            seeds.insert(task_id, seed);
        }
    }

    let mut producer_stages = BTreeMap::<StageId, BTreeSet<StageId>>::new();
    for edge in edges.values() {
        producer_stages
            .entry(edge.consumer_stage)
            .or_default()
            .insert(edge.producer_stage);
    }

    let stages = stage_tasks
        .into_iter()
        .map(|(stage_id, task_ids)| {
            let fragment_id = *stage_of_fragment
                .iter()
                .find(|&(_, &stage)| stage == stage_id)
                .expect("every stage came from a fragment")
                .0;
            (
                stage_id,
                StageNode {
                    stage: StageRef::new(inputs.execution_id, stage_id),
                    fragment_id,
                    tasks: task_ids,
                },
            )
        })
        .collect();

    Ok(TaskGraph {
        execution_id: inputs.execution_id,
        frontend_process_id: inputs.frontend_process_id,
        root_task,
        tasks,
        stages,
        edges,
        producer_stages,
        contexts,
        seeds,
    })
}

/// Every task's place in the shared topology: the edges it produces on, in
/// edge order, and per consumed exchange node, every edge feeding it.
fn task_topology_views(
    tasks: &BTreeMap<TaskId, TaskNode>,
    edges: &BTreeMap<ExchangeEdgeId, EdgeNode>,
) -> BTreeMap<TaskId, TaskTopologyView> {
    let task_of_identity = tasks
        .iter()
        .map(|(&task_id, node)| (node.identity, task_id))
        .collect::<BTreeMap<_, _>>();
    let mut outbound = BTreeMap::<TaskId, Vec<ExchangeEdgeId>>::new();
    let mut inbound = BTreeMap::<TaskId, BTreeMap<FragmentNodeId, Vec<ExchangeEdgeId>>>::new();
    for edge in edges.values() {
        for producer in &edge.producers {
            outbound
                .entry(task_of_identity[producer])
                .or_default()
                .push(edge.edge_id);
        }
        for destination in &edge.destinations {
            inbound
                .entry(task_of_identity[destination])
                .or_default()
                .entry(edge.destination_node_id)
                .or_default()
                .push(edge.edge_id);
        }
    }
    tasks
        .keys()
        .map(|&task_id| {
            (
                task_id,
                TaskTopologyView::new(
                    outbound.remove(&task_id).unwrap_or_default(),
                    inbound.remove(&task_id).unwrap_or_default(),
                ),
            )
        })
        .collect()
}

/// Binds one task's facts into its creation seed.
///
/// The static plan's own encoded size is checked here, before anything is
/// sent: the backend bounds the static plan together with the task
/// assignment, so a static plan already over that bound can never become a
/// legal creation. The combined bound is checked again once the assignment's
/// exact size is known.
#[expect(
    clippy::too_many_arguments,
    reason = "Each input is a separate frozen fact of the attempt graph."
)]
fn bind_creation_seed(
    topology: &Arc<AttemptTopology>,
    views: &BTreeMap<TaskId, TaskTopologyView>,
    edges: &BTreeMap<ExchangeEdgeId, EdgeNode>,
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
    sink_bindings: &mut BTreeMap<StageId, Arc<[ExchangeEdgeId]>>,
    node: &TaskNode,
    facts: FragmentPlanFacts,
    transport_budget: TransportBudget,
) -> Result<TaskCreationSeed, TaskExecutionError> {
    let FragmentPlanFacts {
        fragment,
        assignment,
    } = facts;
    if fragment.facts().fragment_id() != node.fragment_id {
        return Err(TaskExecutionError::Schedule(format!(
            "task {} of fragment {} was handed the frozen plan of fragment {}",
            node.identity,
            node.fragment_id,
            fragment.facts().fragment_id()
        )));
    }
    let static_bytes = fragment.content().len();
    if static_bytes > transport_budget.max_descriptor_encoded_bytes() {
        return Err(CapacityBound::DescriptorBytes {
            limit: transport_budget.max_descriptor_encoded_bytes(),
            actual: static_bytes,
        }
        .into());
    }
    let view = views.get(&node.task_id()).cloned().ok_or_else(|| {
        TaskExecutionError::Schedule(format!("task {} has no view", node.identity))
    })?;
    let sink_edges = match sink_bindings.get(&node.stage_id()) {
        Some(binding) => Arc::clone(binding),
        None => {
            let outbound = view
                .outbound()
                .iter()
                .map(|edge_id| &edges[edge_id])
                .collect::<Vec<_>>();
            let binding = bind_sink_edges(&fragment, &outbound, stage_of_fragment)?;
            sink_bindings.insert(node.stage_id(), Arc::clone(&binding));
            binding
        }
    };
    Ok(TaskCreationSeed::new(
        Arc::clone(topology),
        view,
        fragment,
        sink_edges,
        TaskCreationFacts {
            identity: node.identity,
            context: node.context,
            kernel_key: node.fragment_instance_id,
            pipeline_dop: assignment.pipeline_dop(),
            split_plan_nodes: node.split_plan_nodes.clone(),
            instance_ordinal: assignment.instance_ordinal(),
            initial_scan_ranges: assignment.into_initial_scan_ranges(),
        },
    ))
}

/// Builds the Task protocol graph from the exact application-bound manifest.
///
/// Unlike [`build_task_graph`], this path mints no identity and consults no
/// scheduling plan. Every Task, context, endpoint, edge id, and sender ordinal
/// is projected from the manifest that already joined the actor schedule to
/// its originating topology snapshot.
pub(crate) fn build_task_graph_from_manifest(
    manifest: &TaskManifestBinding,
    plans: &mut dyn FragmentPlanSource,
    transport_budget: TransportBudget,
) -> Result<TaskGraph, TaskExecutionError> {
    let frontend_process_id = manifest
        .contexts()
        .first()
        .ok_or_else(|| TaskExecutionError::Schedule("attempt manifest has no context".to_owned()))?
        .context()
        .frontend_process_id();
    let mut endpoints_by_process = BTreeMap::new();
    let mut contexts = BTreeSet::new();
    for context in manifest.contexts() {
        if context.context().query_execution_id() != manifest.execution()
            || context.context().frontend_process_id() != frontend_process_id
            || context.context().backend_process_id() != context.backend().process_id()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest context {} disagrees with its execution or backend binding",
                context.context()
            )));
        }
        if endpoints_by_process
            .insert(
                context.backend().process_id(),
                context.backend().endpoint().clone(),
            )
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats backend process {}",
                context.backend().process_id()
            )));
        }
        contexts.insert(context.context());
    }

    let mut tasks = BTreeMap::<TaskId, TaskNode>::new();
    let mut tasks_by_identity = BTreeMap::<TaskIdentity, TaskId>::new();
    let mut stage_fragments = BTreeMap::<StageId, FragmentId>::new();
    let mut stage_tasks = BTreeMap::<StageId, Vec<TaskId>>::new();
    let mut kernel_keys = BTreeMap::<UniqueId, TaskId>::new();
    let mut context_task_counts = BTreeMap::<QueryContextRef, usize>::new();
    for task in manifest.tasks() {
        let identity = task.identity();
        if identity.query_execution_id() != manifest.execution()
            || identity.backend_process_id() != task.context().backend_process_id()
            || !contexts.contains(&task.context())
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest task {identity} disagrees with its execution or context"
            )));
        }
        match stage_fragments.insert(identity.stage_id(), task.fragment_id()) {
            Some(fragment) if fragment != task.fragment_id() => {
                return Err(TaskExecutionError::Schedule(format!(
                    "attempt manifest stage {} names both fragments {fragment} and {}",
                    identity.stage_id(),
                    task.fragment_id()
                )));
            }
            _ => {}
        }
        let count = context_task_counts.entry(task.context()).or_default();
        *count += 1;
        if *count > transport_budget.max_tasks_per_context() {
            return Err(CapacityBound::TasksPerContext {
                limit: transport_budget.max_tasks_per_context(),
            }
            .into());
        }
        if kernel_keys
            .insert(task.fragment_instance_id(), identity.task_id())
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats fragment instance {}",
                task.fragment_instance_id()
            )));
        }
        let split_plan_nodes = task
            .scan_work()
            .iter()
            .map(|work| {
                PlanNodeId::new(work.scan().node_id())
                    .map_err(|error| TaskExecutionError::Schedule(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let node = TaskNode {
            identity,
            context: task.context(),
            fragment_id: task.fragment_id(),
            instance_index: task.instance_index(),
            fragment_instance_id: task.fragment_instance_id(),
            split_plan_nodes,
        };
        if tasks.insert(identity.task_id(), node).is_some()
            || tasks_by_identity
                .insert(identity, identity.task_id())
                .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats task identity {identity}"
            )));
        }
        stage_tasks
            .entry(identity.stage_id())
            .or_default()
            .push(identity.task_id());
    }
    let root_task = tasks_by_identity
        .get(&manifest.root())
        .copied()
        .ok_or_else(|| {
            TaskExecutionError::Schedule("attempt manifest root is absent".to_owned())
        })?;

    let mut edges = BTreeMap::<ExchangeEdgeId, EdgeNode>::new();
    let mut shared_edges = Vec::with_capacity(manifest.edges().len());
    for edge in manifest.edges() {
        let producer_stage = edge
            .producers()
            .first()
            .ok_or_else(|| {
                TaskExecutionError::Schedule("manifest edge has no producer".to_owned())
            })?
            .task()
            .stage_id();
        let consumer_stage = edge
            .destinations()
            .first()
            .ok_or_else(|| {
                TaskExecutionError::Schedule("manifest edge has no destination".to_owned())
            })?
            .stage_id();
        let node_id = FragmentNodeId::new(edge.target_exchange_node_id());
        let mut producers = Vec::with_capacity(edge.producers().len());
        for producer in edge.producers() {
            let producer_task = *tasks_by_identity.get(&producer.task()).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "manifest edge {} names missing producer {}",
                    edge.edge_id(),
                    producer.task()
                ))
            })?;
            producers.push(EdgeProducer::new(
                producer.task(),
                tasks[&producer_task].fragment_instance_id,
                producer.sender_ordinal(),
            ));
        }
        let mut destinations = Vec::with_capacity(edge.destinations().len());
        for destination in edge.destinations() {
            let task_id = *tasks_by_identity.get(destination).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "manifest edge {} names missing destination {destination}",
                    edge.edge_id()
                ))
            })?;
            let endpoint = endpoints_by_process
                .get(&destination.backend_process_id())
                .ok_or_else(|| {
                    TaskExecutionError::Schedule(format!(
                        "manifest destination {destination} has no frozen endpoint"
                    ))
                })?;
            destinations.push(EdgeDestination::new(
                *destination,
                tasks[&task_id].fragment_instance_id,
                endpoint.clone(),
            ));
        }
        shared_edges.push(SharedEdge::new(
            edge.edge_id(),
            node_id,
            manifest_partition_type(edge.partition_kind()),
            edge.sender_count(),
            producers,
            destinations,
        ));
        if edges
            .insert(
                edge.edge_id(),
                EdgeNode {
                    edge_id: edge.edge_id(),
                    producer_stage,
                    consumer_stage,
                    destination_node_id: node_id,
                    producers: edge.producers().iter().map(|p| p.task()).collect(),
                    destinations: edge.destinations().to_vec(),
                },
            )
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats exchange edge {}",
                edge.edge_id()
            )));
        }
    }
    let views = task_topology_views(&tasks, &edges);
    let topology = Arc::new(AttemptTopology::try_new(
        shared_edges,
        &views.values().collect::<Vec<_>>(),
    )?);

    let mut stage_of_fragment = BTreeMap::new();
    for (&stage_id, &fragment_id) in &stage_fragments {
        if stage_of_fragment.insert(fragment_id, stage_id).is_some() {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats fragment {fragment_id} in multiple stages"
            )));
        }
    }
    let mut seeds = BTreeMap::new();
    let mut sink_bindings = BTreeMap::<StageId, Arc<[ExchangeEdgeId]>>::new();
    for task in manifest.tasks() {
        let facts = plans.take_plan(task.fragment_id(), task.instance_index())?;
        let task_id = task.identity().task_id();
        let seed = bind_creation_seed(
            &topology,
            &views,
            &edges,
            &stage_of_fragment,
            &mut sink_bindings,
            &tasks[&task_id],
            facts,
            transport_budget,
        )?;
        seeds.insert(task_id, seed);
    }

    let stages = stage_tasks
        .into_iter()
        .map(|(stage_id, task_ids)| {
            (
                stage_id,
                StageNode {
                    stage: StageRef::new(manifest.execution(), stage_id),
                    fragment_id: stage_fragments[&stage_id],
                    tasks: task_ids,
                },
            )
        })
        .collect();
    let mut producer_stages = BTreeMap::<StageId, BTreeSet<StageId>>::new();
    for edge in edges.values() {
        producer_stages
            .entry(edge.consumer_stage)
            .or_default()
            .insert(edge.producer_stage);
    }
    Ok(TaskGraph {
        execution_id: manifest.execution(),
        frontend_process_id,
        root_task,
        tasks,
        stages,
        edges,
        producer_stages,
        contexts,
        seeds,
    })
}

fn build_sender_sets(
    inputs: &TaskGraphInputs<'_>,
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
    task_of_instance: &BTreeMap<(FragmentId, usize), TaskId>,
) -> Result<BTreeMap<(FragmentId, i32), SenderSet>, TaskExecutionError> {
    let mut grouped = BTreeMap::<(FragmentId, i32), Vec<FragmentId>>::new();
    let mut partitionings = BTreeMap::<(FragmentId, i32), DataStreamPartitionType>::new();
    for edge in inputs.fragment_edges {
        if !stage_of_fragment.contains_key(&edge.source_fragment_id)
            || !stage_of_fragment.contains_key(&edge.target_fragment_id)
        {
            continue;
        }
        let key = (edge.target_fragment_id, edge.target_exchange_node_id);
        let partitioning = partition_type(edge.partition_kind);
        match partitionings.get(&key) {
            Some(&frozen) if frozen != partitioning => {
                return Err(TaskExecutionError::Schedule(format!(
                    "exchange node {} of fragment {} is fed with {} and {}",
                    edge.target_exchange_node_id,
                    edge.target_fragment_id,
                    frozen.display_name(),
                    partitioning.display_name()
                )));
            }
            Some(_) => {}
            None => {
                partitionings.insert(key, partitioning);
            }
        }
        let sources = grouped.entry(key).or_default();
        if sources.contains(&edge.source_fragment_id) {
            return Err(TaskExecutionError::Schedule(format!(
                "fragment {} reaches exchange node {} of fragment {} twice",
                edge.source_fragment_id, edge.target_exchange_node_id, edge.target_fragment_id
            )));
        }
        sources.push(edge.source_fragment_id);
    }

    grouped
        .into_iter()
        .map(|(key, mut sources)| {
            sources.sort_unstable();
            let mut ordinals = BTreeMap::new();
            let mut next_ordinal = 0_u32;
            for source_fragment_id in sources {
                let mut instance_index = 0_usize;
                while let Some(&task_id) =
                    task_of_instance.get(&(source_fragment_id, instance_index))
                {
                    ordinals.insert(task_id, next_ordinal);
                    next_ordinal += 1;
                    instance_index += 1;
                }
            }
            let count = NonZeroU32::new(next_ordinal).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "exchange node {} of fragment {} has no sender",
                    key.1, key.0
                ))
            })?;
            let partitioning = partitionings[&key];
            Ok((
                key,
                SenderSet {
                    ordinals,
                    count,
                    partitioning,
                },
            ))
        })
        .collect()
}

type ScheduleEdges = (BTreeMap<ExchangeEdgeId, EdgeNode>, Vec<SharedEdge>);

fn build_schedule_edges(
    inputs: &TaskGraphInputs<'_>,
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
    task_of_instance: &BTreeMap<(FragmentId, usize), TaskId>,
    tasks: &BTreeMap<TaskId, TaskNode>,
    endpoints: &BTreeMap<TaskId, RuntimeEndpoint>,
    sender_sets: &BTreeMap<(FragmentId, i32), SenderSet>,
) -> Result<ScheduleEdges, TaskExecutionError> {
    let mut edges = BTreeMap::<ExchangeEdgeId, EdgeNode>::new();
    let mut shared_edges = Vec::new();
    let mut next_edge = 1_u32;

    // Edges follow the frozen order of the fragment-edge list, so the same
    // schedule always mints the same edge ids.
    for fragment_edge in inputs.fragment_edges {
        let (Some(&producer_stage), Some(&consumer_stage)) = (
            stage_of_fragment.get(&fragment_edge.source_fragment_id),
            stage_of_fragment.get(&fragment_edge.target_fragment_id),
        ) else {
            continue;
        };
        let node_id = FragmentNodeId::new(fragment_edge.target_exchange_node_id);
        let senders = &sender_sets[&(
            fragment_edge.target_fragment_id,
            fragment_edge.target_exchange_node_id,
        )];
        let edge_id = ExchangeEdgeId::new(next_edge)
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        next_edge = next_edge
            .checked_add(1)
            .ok_or_else(|| TaskExecutionError::Schedule("edge id space exhausted".to_owned()))?;

        let consumers = instances_of(task_of_instance, fragment_edge.target_fragment_id);
        if consumers.is_empty() {
            return Err(TaskExecutionError::Schedule(format!(
                "exchange edge into fragment {} has no destination",
                fragment_edge.target_fragment_id
            )));
        }
        let producers = instances_of(task_of_instance, fragment_edge.source_fragment_id);
        if producers.is_empty() {
            return Err(TaskExecutionError::Schedule(format!(
                "exchange edge out of fragment {} has no producer",
                fragment_edge.source_fragment_id
            )));
        }
        let shared_producers = producers
            .iter()
            .map(|&producer| {
                let sender_ordinal = *senders.ordinals.get(&producer).ok_or_else(|| {
                    TaskExecutionError::Schedule(format!(
                        "producer task {producer} is absent from the sender set of exchange node {}",
                        fragment_edge.target_exchange_node_id
                    ))
                })?;
                Ok(EdgeProducer::new(
                    tasks[&producer].identity,
                    tasks[&producer].fragment_instance_id,
                    sender_ordinal,
                ))
            })
            .collect::<Result<Vec<_>, TaskExecutionError>>()?;
        let shared_destinations = consumers
            .iter()
            .map(|&consumer| {
                EdgeDestination::new(
                    tasks[&consumer].identity,
                    tasks[&consumer].fragment_instance_id,
                    endpoints[&consumer].clone(),
                )
            })
            .collect::<Vec<_>>();
        shared_edges.push(SharedEdge::new(
            edge_id,
            node_id,
            senders.partitioning,
            senders.count,
            shared_producers,
            shared_destinations,
        ));

        edges.insert(
            edge_id,
            EdgeNode {
                edge_id,
                producer_stage,
                consumer_stage,
                destination_node_id: node_id,
                producers: producers
                    .iter()
                    .map(|producer| tasks[producer].identity)
                    .collect(),
                destinations: consumers
                    .iter()
                    .map(|consumer| tasks[consumer].identity)
                    .collect(),
            },
        );
    }
    Ok((edges, shared_edges))
}

fn instances_of(
    task_of_instance: &BTreeMap<(FragmentId, usize), TaskId>,
    fragment_id: FragmentId,
) -> Vec<TaskId> {
    let mut instances = Vec::new();
    let mut instance_index = 0_usize;
    while let Some(&task_id) = task_of_instance.get(&(fragment_id, instance_index)) {
        instances.push(task_id);
        instance_index += 1;
    }
    instances
}

const fn partition_type(kind: AttemptPartitionKind) -> DataStreamPartitionType {
    match kind {
        AttemptPartitionKind::Unpartitioned => DataStreamPartitionType::Unpartitioned,
        AttemptPartitionKind::Random => DataStreamPartitionType::Random,
        AttemptPartitionKind::Hash => DataStreamPartitionType::HashPartitioned,
    }
}

const fn manifest_partition_type(kind: BoundManifestPartitionKind) -> DataStreamPartitionType {
    match kind {
        BoundManifestPartitionKind::Unpartitioned => DataStreamPartitionType::Unpartitioned,
        BoundManifestPartitionKind::Random => DataStreamPartitionType::Random,
        BoundManifestPartitionKind::Hash => DataStreamPartitionType::HashPartitioned,
    }
}
