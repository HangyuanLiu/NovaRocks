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

//! Input-order liveness analysis over the final, carrier-neutral physical graph.
//!
//! Every operator has separate completion (C) and output-backpressure (B)
//! vertices. C follows all upstream inputs, while B propagates only across
//! pipelined inputs in the opposite direction. The sole ordinary bridge is
//! C(op) -> B(op): an operator cannot finish while its output remains blocked.
//! A gate adds B(input) -> gate -> C(prerequisite). This represents the same
//! intersection of completion ancestry and backpressure ancestry without
//! materializing their transitive closures for every gate.
//!
//! Set-op gates use a chain of prefix-completion vertices, so waiting for all
//! preceding inputs also needs only linear graph space. Absorption boundaries
//! do not erase an input's own admission gate. Shared fragment roots and local
//! node identities remain shared on every branch.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use novarocks_physical_plan::{
    Edge, EdgeId, Fragment, FragmentId, NodeId, NodeKind, PhysicalNode, RuntimeFilter,
    RuntimeFilterApplyPoint, RuntimeFilterConsumerActivation, RuntimeFilterConsumerTarget,
    RuntimeFilterId, RuntimeFilterProducerTarget, SetOperationKind,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct OperatorRef {
    pub fragment: FragmentId,
    pub node: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct FilterConsumerRef {
    pub filter: RuntimeFilterId,
    pub ordinal: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum WaitNode {
    Completion(OperatorRef),
    Backpressure(OperatorRef),
    InputPrefix {
        operator: OperatorRef,
        ordinal: usize,
    },
    Input {
        operator: OperatorRef,
        ordinal: usize,
    },
    Filter(RuntimeFilterId),
    FilterConsumer(FilterConsumerRef),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum WaitEdgeKind {
    Backpressure,
    DataWait,
    FilterWait(FilterConsumerRef),
}

/// A closed directed path, including the repeated first vertex at the end.
/// The edge kinds retain the actual dependency which justified each step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CycleWitness {
    pub nodes: Box<[WaitNode]>,
    pub edges: Box<[BTreeSet<WaitEdgeKind>]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WaitResolution {
    pub filter_downgrades: Box<[FilterConsumerRef]>,
    pub data_cycles: Box<[CycleWitness]>,
}

impl WaitResolution {
    pub fn is_acyclic(&self) -> bool {
        self.data_cycles.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InputConsumption {
    Pipelined,
    DrainedBeforeOutput,
}

/// Only actual absorption boundaries stop backpressure. New or uncertain
/// operator kinds conservatively propagate it until their input contract is
/// described here. This table does not grant an input admission permission.
pub(super) fn input_consumption(kind: &NodeKind, ordinal: usize) -> InputConsumption {
    match kind {
        NodeKind::HashJoin { build_side, .. } if ordinal == build_side.input_ordinal() as usize => {
            InputConsumption::DrainedBeforeOutput
        }
        NodeKind::NestLoopJoin { .. } if ordinal == 1 => InputConsumption::DrainedBeforeOutput,
        // The installed aggregate route has no streaming preaggregation
        // (local-compiler/src/aggregate.rs), including partial grouping.
        NodeKind::Aggregate { .. } | NodeKind::Sort { .. } | NodeKind::TopN { .. } => {
            InputConsumption::DrainedBeforeOutput
        }
        NodeKind::SetOp {
            kind: SetOperationKind::Intersect | SetOperationKind::Except,
            ..
        } => InputConsumption::DrainedBeforeOutput,
        _ => InputConsumption::Pipelined,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WaitGraphError {
    MissingNode(OperatorRef),
    MissingEdge(EdgeId),
    MissingFragment(FragmentId),
    InvalidInput {
        operator: OperatorRef,
        ordinal: usize,
    },
    InvalidFilterProducer {
        filter: RuntimeFilterId,
        operator: OperatorRef,
    },
    UnresolvableFilterConsumer(FilterConsumerRef),
}

impl fmt::Display for WaitGraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode(node) => write!(f, "wait graph refers to missing operator {node:?}"),
            Self::MissingEdge(edge) => write!(f, "wait graph refers to missing edge {edge:?}"),
            Self::MissingFragment(fragment) => {
                write!(f, "wait graph refers to missing fragment {fragment:?}")
            }
            Self::InvalidInput { operator, ordinal } => {
                write!(f, "wait graph operator {operator:?} has no input {ordinal}")
            }
            Self::InvalidFilterProducer { filter, operator } => {
                write!(
                    f,
                    "runtime filter {filter:?} has an invalid build endpoint {operator:?}"
                )
            }
            Self::UnresolvableFilterConsumer(consumer) => {
                write!(
                    f,
                    "runtime-filter wait cannot be downgraded for {consumer:?}"
                )
            }
        }
    }
}

impl std::error::Error for WaitGraphError {}

type Dependencies = BTreeMap<WaitNode, BTreeMap<WaitNode, BTreeSet<WaitEdgeKind>>>;

#[derive(Clone, Debug, Default)]
pub(super) struct WaitGraph {
    dependencies: Dependencies,
    non_downgradable: BTreeSet<FilterConsumerRef>,
}

impl WaitGraph {
    pub fn build(
        fragments: &BTreeMap<FragmentId, Fragment>,
        edges: &BTreeMap<EdgeId, Edge>,
        filters: &[RuntimeFilter],
    ) -> Result<Self, WaitGraphError> {
        let plan = PlanView { fragments, edges };
        let mut graph = Self::default();
        for fragment in fragments.values() {
            for node in fragment.nodes().values() {
                let operator = OperatorRef {
                    fragment: fragment.id(),
                    node: node.id,
                };
                graph.add_edge(
                    WaitNode::Completion(operator),
                    WaitNode::Backpressure(operator),
                    WaitEdgeKind::DataWait,
                );
                for ordinal in 0..node.inputs.len() {
                    let input = plan.input(operator, ordinal)?;
                    plan.node(input)?;
                    graph.add_operator_input(
                        operator,
                        input,
                        input_consumption(&node.kind, ordinal) == InputConsumption::Pipelined,
                    );
                }
                if let Some(source) = plan.exchange_input(node)? {
                    plan.node(source)?;
                    graph.add_operator_input(operator, source, true);
                }
                match &node.kind {
                    NodeKind::HashJoin { build_side, .. } => {
                        graph.add_input_gate(
                            &plan,
                            operator,
                            build_side.opposite().input_ordinal() as usize,
                            build_side.input_ordinal() as usize,
                        )?;
                    }
                    NodeKind::NestLoopJoin { .. } => graph.add_input_gate(&plan, operator, 0, 1)?,
                    NodeKind::SetOp {
                        kind: SetOperationKind::Intersect | SetOperationKind::Except,
                        ..
                    } => graph.add_ordered_prefix(&plan, operator, node.inputs.len())?,
                    _ => {}
                }
            }
        }
        for filter in filters {
            let filter_node = WaitNode::Filter(filter.id);
            graph.dependencies.entry(filter_node).or_default();
            for producer in &filter.producers {
                let operator = OperatorRef {
                    fragment: producer.endpoint.fragment,
                    node: producer.endpoint.node,
                };
                let node = plan.node(operator)?;
                let input = match (&producer.target, &node.kind) {
                    (
                        RuntimeFilterProducerTarget::JoinBuildKey { .. },
                        NodeKind::HashJoin { build_side, .. },
                    ) => build_side.input_ordinal() as usize,
                    (
                        RuntimeFilterProducerTarget::AggregateTopNKey { .. },
                        NodeKind::Aggregate { .. },
                    ) => 0,
                    _ => {
                        return Err(WaitGraphError::InvalidFilterProducer {
                            filter: filter.id,
                            operator,
                        });
                    }
                };
                let root = plan.input(operator, input)?;
                graph.add_edge(
                    filter_node,
                    WaitNode::Completion(root),
                    WaitEdgeKind::DataWait,
                );
            }
            for (ordinal, consumer) in filter.consumers.iter().enumerate() {
                if consumer.activation != RuntimeFilterConsumerActivation::BlockingSnapshot {
                    continue;
                }
                let consumer_ref = FilterConsumerRef {
                    filter: filter.id,
                    ordinal,
                };
                let gate = WaitNode::FilterConsumer(consumer_ref);
                let operator = OperatorRef {
                    fragment: consumer.endpoint.fragment,
                    node: consumer.endpoint.node,
                };
                let root = match consumer.apply_point {
                    RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                        plan.input(operator, input_ordinal as usize)?
                    }
                    RuntimeFilterApplyPoint::NodeOutput | RuntimeFilterApplyPoint::ScanSource => {
                        plan.node(operator)?;
                        operator
                    }
                };
                graph.add_edge(
                    WaitNode::Backpressure(root),
                    gate,
                    WaitEdgeKind::Backpressure,
                );
                graph.add_edge(gate, filter_node, WaitEdgeKind::FilterWait(consumer_ref));
                if matches!(
                    consumer.target,
                    RuntimeFilterConsumerTarget::AggregateTopNScanField { .. }
                ) {
                    graph.non_downgradable.insert(consumer_ref);
                }
            }
        }
        Ok(graph)
    }

    fn add_operator_input(&mut self, operator: OperatorRef, input: OperatorRef, pipelined: bool) {
        self.add_edge(
            WaitNode::Completion(operator),
            WaitNode::Completion(input),
            WaitEdgeKind::DataWait,
        );
        if pipelined {
            self.add_edge(
                WaitNode::Backpressure(input),
                WaitNode::Backpressure(operator),
                WaitEdgeKind::Backpressure,
            );
        }
    }

    fn add_input_gate(
        &mut self,
        plan: &PlanView<'_>,
        operator: OperatorRef,
        ordinal: usize,
        awaited: usize,
    ) -> Result<(), WaitGraphError> {
        let gate = WaitNode::Input { operator, ordinal };
        let input = plan.input(operator, ordinal)?;
        let prerequisite = plan.input(operator, awaited)?;
        self.add_edge(
            WaitNode::Backpressure(input),
            gate,
            WaitEdgeKind::Backpressure,
        );
        self.add_edge(
            gate,
            WaitNode::Completion(prerequisite),
            WaitEdgeKind::DataWait,
        );
        Ok(())
    }

    fn add_ordered_prefix(
        &mut self,
        plan: &PlanView<'_>,
        operator: OperatorRef,
        inputs: usize,
    ) -> Result<(), WaitGraphError> {
        let mut previous = None;
        for ordinal in 0..inputs {
            let root = plan.input(operator, ordinal)?;
            let prefix = WaitNode::InputPrefix { operator, ordinal };
            self.add_edge(prefix, WaitNode::Completion(root), WaitEdgeKind::DataWait);
            if let Some(previous) = previous {
                self.add_edge(prefix, previous, WaitEdgeKind::DataWait);
                let gate = WaitNode::Input { operator, ordinal };
                // The target input's absorption property does not suppress
                // its own stage-admission dependency.
                self.add_edge(
                    WaitNode::Backpressure(root),
                    gate,
                    WaitEdgeKind::Backpressure,
                );
                self.add_edge(gate, previous, WaitEdgeKind::DataWait);
            }
            previous = Some(prefix);
        }
        Ok(())
    }

    fn add_edge(&mut self, from: WaitNode, to: WaitNode, kind: WaitEdgeKind) {
        self.dependencies.entry(to).or_default();
        self.dependencies
            .entry(from)
            .or_default()
            .entry(to)
            .or_default()
            .insert(kind);
    }

    /// Remove only optional RF waits occurring inside a cyclic component,
    /// then recompute the whole graph. Required data waits are never removed.
    pub fn resolve(&mut self) -> Result<WaitResolution, WaitGraphError> {
        let components = self.components();
        let mut component_by_node = BTreeMap::new();
        for (ordinal, component) in components.iter().enumerate() {
            for node in component {
                component_by_node.insert(*node, ordinal);
            }
        }
        let mut downgrades = BTreeSet::new();
        for (from, destinations) in &self.dependencies {
            for (to, kinds) in destinations {
                if component_by_node[from] == component_by_node[to] {
                    for kind in kinds {
                        if let WaitEdgeKind::FilterWait(consumer) = kind {
                            if self.non_downgradable.contains(consumer) {
                                return Err(WaitGraphError::UnresolvableFilterConsumer(*consumer));
                            }
                            downgrades.insert(*consumer);
                        }
                    }
                }
            }
        }
        for destinations in self.dependencies.values_mut() {
            destinations.retain(|_, kinds| {
                kinds.retain(|kind| match kind {
                    WaitEdgeKind::FilterWait(consumer) => !downgrades.contains(consumer),
                    WaitEdgeKind::Backpressure | WaitEdgeKind::DataWait => true,
                });
                !kinds.is_empty()
            });
        }
        let data_cycles = self
            .components()
            .into_iter()
            .filter_map(|component| self.cycle_witness(&component))
            .collect::<Vec<_>>();
        Ok(WaitResolution {
            filter_downgrades: downgrades
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            data_cycles: data_cycles.into_boxed_slice(),
        })
    }

    /// Iterative Kosaraju traversal keeps physical-plan depth off the Rust stack.
    fn components(&self) -> Vec<BTreeSet<WaitNode>> {
        let mut reverse = BTreeMap::<WaitNode, BTreeSet<WaitNode>>::new();
        for (node, dependencies) in &self.dependencies {
            reverse.entry(*node).or_default();
            for dependency in dependencies.keys() {
                reverse.entry(*dependency).or_default().insert(*node);
            }
        }
        let mut visited = BTreeSet::new();
        let mut finish_order = Vec::with_capacity(reverse.len());
        for start in self.dependencies.keys() {
            if visited.contains(start) {
                continue;
            }
            let mut stack = vec![(*start, false)];
            while let Some((node, exiting)) = stack.pop() {
                if exiting {
                    finish_order.push(node);
                    continue;
                }
                if !visited.insert(node) {
                    continue;
                }
                stack.push((node, true));
                if let Some(next) = self.dependencies.get(&node) {
                    stack.extend(next.keys().rev().map(|dependency| (*dependency, false)));
                }
            }
        }
        let mut assigned = BTreeSet::new();
        let mut components = Vec::new();
        while let Some(start) = finish_order.pop() {
            if assigned.contains(&start) {
                continue;
            }
            let mut component = BTreeSet::new();
            let mut stack = vec![start];
            while let Some(node) = stack.pop() {
                if !assigned.insert(node) {
                    continue;
                }
                component.insert(node);
                if let Some(next) = reverse.get(&node) {
                    stack.extend(next.iter().rev().copied());
                }
            }
            components.push(component);
        }
        components
    }

    fn cycle_witness(&self, component: &BTreeSet<WaitNode>) -> Option<CycleWitness> {
        let start = *component.first()?;
        let next = self
            .dependencies
            .get(&start)?
            .keys()
            .copied()
            .find(|node| component.contains(node))?;
        let mut previous = BTreeMap::new();
        let mut pending = VecDeque::from([next]);
        let mut visited = BTreeSet::from([next]);
        while let Some(node) = pending.pop_front() {
            if node == start {
                break;
            }
            for destination in self.dependencies[&node].keys() {
                if component.contains(destination) && visited.insert(*destination) {
                    previous.insert(*destination, node);
                    pending.push_back(*destination);
                }
            }
        }
        let mut path = vec![start];
        let mut current = start;
        while current != next {
            current = previous[&current];
            path.push(current);
        }
        path.reverse();
        path.insert(0, start);
        let edges = path
            .windows(2)
            .map(|pair| self.dependencies[&pair[0]][&pair[1]].clone())
            .collect::<Vec<_>>();
        Some(CycleWitness {
            nodes: path.into_boxed_slice(),
            edges: edges.into_boxed_slice(),
        })
    }
}

struct PlanView<'a> {
    fragments: &'a BTreeMap<FragmentId, Fragment>,
    edges: &'a BTreeMap<EdgeId, Edge>,
}

impl PlanView<'_> {
    fn node(&self, operator: OperatorRef) -> Result<&PhysicalNode, WaitGraphError> {
        self.fragments
            .get(&operator.fragment)
            .and_then(|fragment| fragment.nodes().get(&operator.node))
            .ok_or(WaitGraphError::MissingNode(operator))
    }

    fn input(&self, operator: OperatorRef, ordinal: usize) -> Result<OperatorRef, WaitGraphError> {
        let node = self.node(operator)?;
        let input = node
            .inputs
            .get(ordinal)
            .ok_or(WaitGraphError::InvalidInput { operator, ordinal })?;
        Ok(OperatorRef {
            fragment: operator.fragment,
            node: *input,
        })
    }

    fn exchange_input(&self, node: &PhysicalNode) -> Result<Option<OperatorRef>, WaitGraphError> {
        let NodeKind::ExchangeSource { edge, .. } = &node.kind else {
            return Ok(None);
        };
        let edge = self
            .edges
            .get(edge)
            .ok_or(WaitGraphError::MissingEdge(*edge))?;
        let source = self
            .fragments
            .get(&edge.source.fragment)
            .ok_or(WaitGraphError::MissingFragment(edge.source.fragment))?;
        Ok(Some(OperatorRef {
            fragment: source.id(),
            node: source.root(),
        }))
    }
}

#[cfg(test)]
#[path = "wait_graph_tests.rs"]
mod tests;
