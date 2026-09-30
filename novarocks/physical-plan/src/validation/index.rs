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

use super::*;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{
    AggregatePhase, EdgeId, ExprId, Fragment, FragmentId, NodeId, NodeKind, PhysicalNode, ValueId,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct ValuePortIndex {
    pub(crate) occurrences: BTreeMap<ValueId, usize>,
}

impl ValuePortIndex {
    pub(crate) fn new(values: &[ValueId]) -> Self {
        let mut occurrences = BTreeMap::new();
        for value in values {
            *occurrences.entry(*value).or_default() += 1;
        }
        Self { occurrences }
    }

    pub(crate) fn contains(&self, value: &ValueId) -> bool {
        self.occurrences.contains_key(value)
    }
}

pub(crate) struct FragmentValidationIndexes {
    pub(crate) output_ports: BTreeMap<NodeId, Arc<ValuePortIndex>>,
    pub(crate) visible_inputs: BTreeMap<NodeId, VisibleInputIndex>,
}

pub(crate) enum VisibleInputIndex {
    Empty,
    One(Arc<ValuePortIndex>),
    Many(Box<[Arc<ValuePortIndex>]>),
}

impl VisibleInputIndex {
    pub(crate) fn contains(&self, value: &ValueId) -> bool {
        match self {
            Self::Empty => false,
            Self::One(port) => port.contains(value),
            Self::Many(ports) => ports.iter().any(|port| port.contains(value)),
        }
    }
}

impl FragmentValidationIndexes {
    pub(crate) fn new(fragment: &Fragment) -> Self {
        let output_ports = fragment
            .nodes()
            .values()
            .map(|node| (node.id, Arc::new(ValuePortIndex::new(&node.output.columns))))
            .collect::<BTreeMap<_, _>>();
        let mut visible_inputs = BTreeMap::new();
        for node in fragment.nodes().values() {
            // A scan's own expressions read the provider's columns and the
            // ones it derives from them while reading -- a residual over a
            // variant path is evaluated against the path, not against the
            // bytes it was read out of.
            let visible = if let NodeKind::Scan {
                provider_outputs,
                derived_values,
                ..
            } = &node.kind
            {
                VisibleInputIndex::One(Arc::new(ValuePortIndex::new(
                    &provider_outputs
                        .iter()
                        .map(|(_, value)| *value)
                        .chain(derived_values.iter().copied())
                        .collect::<Vec<_>>(),
                )))
            } else {
                let ports = node
                    .inputs
                    .iter()
                    .filter_map(|input| output_ports.get(input).cloned())
                    .collect::<Vec<_>>();
                match ports.as_slice() {
                    [] => VisibleInputIndex::Empty,
                    [port] => VisibleInputIndex::One(port.clone()),
                    _ => VisibleInputIndex::Many(ports.into_boxed_slice()),
                }
            };
            visible_inputs.insert(node.id, visible);
        }
        Self {
            output_ports,
            visible_inputs,
        }
    }

    pub(crate) fn output(&self, node: NodeId) -> Option<&ValuePortIndex> {
        self.output_ports.get(&node).map(Arc::as_ref)
    }

    pub(crate) fn visible_input(&self, node: NodeId) -> Option<&VisibleInputIndex> {
        self.visible_inputs.get(&node)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ValueMappingIndex {
    pub(crate) by_destination: BTreeMap<ValueId, BTreeMap<Option<ValueId>, usize>>,
}

impl ValueMappingIndex {
    pub(crate) fn from_pairs(mapping: &[(ValueId, ValueId)]) -> Self {
        Self::from_pairs_iter(mapping.iter().copied())
    }

    pub(crate) fn from_pairs_iter(mapping: impl IntoIterator<Item = (ValueId, ValueId)>) -> Self {
        let mut index = Self::default();
        for (source, destination) in mapping {
            index.insert(Some(source), destination);
        }
        index
    }

    pub(crate) fn insert(&mut self, source: Option<ValueId>, destination: ValueId) {
        *self
            .by_destination
            .entry(destination)
            .or_default()
            .entry(source)
            .or_default() += 1;
    }

    pub(crate) fn contains(&self, source: ValueId, destination: ValueId) -> bool {
        self.by_destination
            .get(&destination)
            .is_some_and(|sources| sources.contains_key(&Some(source)))
    }

    pub(crate) fn resolve(
        &self,
        destination: ValueId,
        allow_identical_duplicates: bool,
    ) -> Option<ValueId> {
        let sources = self.by_destination.get(&destination)?;
        if sources.len() != 1 {
            return None;
        }
        let (source, occurrences) = sources.first_key_value()?;
        if !allow_identical_duplicates && *occurrences != 1 {
            return None;
        }
        *source
    }
}

pub(crate) struct SemanticTraceWorkBudget {
    pub(crate) remaining: usize,
}

impl SemanticTraceWorkBudget {
    pub(crate) const fn new(limits: &PlanLimits) -> Self {
        Self {
            remaining: limits.plan_semantic_trace_work,
        }
    }

    pub(crate) fn charge(&mut self, work: usize) -> bool {
        let Some(remaining) = self.remaining.checked_sub(work) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

#[derive(Default)]
pub(crate) struct SemanticTraceIndexes {
    pub(crate) ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    pub(crate) edges: BTreeMap<EdgeId, ValueMappingIndex>,
    pub(crate) projects: BTreeMap<(FragmentId, NodeId), ValueMappingIndex>,
    pub(crate) unions: BTreeMap<(FragmentId, NodeId, usize), ValueMappingIndex>,
    pub(crate) aggregate_sequences:
        BTreeMap<(FragmentId, NodeId), BTreeMap<crate::AggregateSequenceId, Option<usize>>>,
}

#[derive(Default)]
pub(crate) struct RuntimeFilterLineageIndexes {
    pub(crate) parents: BTreeMap<FragmentId, BTreeMap<NodeId, Option<NodeId>>>,
    pub(crate) ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    pub(crate) apply_ports:
        BTreeMap<(FragmentId, NodeId, RuntimeFilterApplyPortKey), Option<ValuePortIndex>>,
    pub(crate) scan_provider_ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    pub(crate) build_frontiers: BTreeMap<(FragmentId, NodeId), RuntimeFilterFrontierIndex>,
}

pub(crate) struct RuntimeFilterFrontierIndex {
    pub(crate) build: BTreeSet<EdgeId>,
    pub(crate) non_build: BTreeSet<EdgeId>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RuntimeFilterApplyPortKey {
    NodeInput(u32),
    NodeOutput,
    ScanSource,
}

impl From<crate::RuntimeFilterApplyPoint> for RuntimeFilterApplyPortKey {
    fn from(value: crate::RuntimeFilterApplyPoint) -> Self {
        match value {
            crate::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                Self::NodeInput(input_ordinal)
            }
            crate::RuntimeFilterApplyPoint::NodeOutput => Self::NodeOutput,
            crate::RuntimeFilterApplyPoint::ScanSource => Self::ScanSource,
        }
    }
}

impl RuntimeFilterLineageIndexes {
    pub(crate) fn apply_port_contains_all(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        apply_point: crate::RuntimeFilterApplyPoint,
        values: &[ValueId],
    ) -> Option<bool> {
        if apply_point == crate::RuntimeFilterApplyPoint::ScanSource {
            return matches!(node.kind, NodeKind::Scan { .. }).then(|| {
                values
                    .iter()
                    .all(|value| self.scan_provider_contains(fragment, node, *value))
            });
        }
        let port = self
            .apply_ports
            .entry((fragment.id(), node.id, apply_point.into()))
            .or_insert_with(|| {
                let available = match apply_point {
                    crate::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                        usize::try_from(input_ordinal)
                            .ok()
                            .and_then(|ordinal| node.inputs.get(ordinal))
                            .and_then(|input| fragment.nodes().get(input))
                            .map(|input| input.output.columns.as_ref())
                    }
                    crate::RuntimeFilterApplyPoint::NodeOutput => {
                        Some(node.output.columns.as_ref())
                    }
                    crate::RuntimeFilterApplyPoint::ScanSource => unreachable!(),
                };
                available.map(ValuePortIndex::new)
            })
            .as_ref()?;
        Some(values.iter().all(|value| port.contains(value)))
    }

    pub(crate) fn scan_provider_contains(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        value: ValueId,
    ) -> bool {
        let NodeKind::Scan {
            provider_outputs, ..
        } = &node.kind
        else {
            return false;
        };
        self.scan_provider_ports
            .entry((fragment.id(), node.id))
            .or_insert_with(|| {
                ValuePortIndex::new(
                    &provider_outputs
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Vec<_>>(),
                )
            })
            .contains(&value)
    }

    pub(crate) fn build_frontier(
        &mut self,
        fragment: &Fragment,
        root: NodeId,
        inbound_edges: &BTreeSet<EdgeId>,
    ) -> &RuntimeFilterFrontierIndex {
        self.build_frontiers
            .entry((fragment.id(), root))
            .or_insert_with(|| {
                let build = collect_subtree_exchange_edges(fragment, root);
                let non_build = inbound_edges.difference(&build).copied().collect();
                RuntimeFilterFrontierIndex { build, non_build }
            })
    }

    pub(crate) fn port_contains(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        value: ValueId,
    ) -> bool {
        self.ports
            .entry((fragment.id(), node.id))
            .or_insert_with(|| ValuePortIndex::new(&node.output.columns))
            .contains(&value)
    }
}

impl SemanticTraceIndexes {
    pub(crate) fn aggregate_sequence_call<'a>(
        &mut self,
        fragment: FragmentId,
        node: NodeId,
        calls: &'a [crate::AggregateCall],
        sequence: crate::AggregateSequenceId,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<&'a crate::AggregateCall> {
        let key = (fragment, node);
        if let Entry::Vacant(entry) = self.aggregate_sequences.entry(key) {
            if !budget.charge(calls.len()) {
                return None;
            }
            let mut by_sequence = BTreeMap::new();
            for (ordinal, call) in calls.iter().enumerate() {
                let Some(call_sequence) = call.binding.phase.sequence() else {
                    continue;
                };
                if matches!(call.binding.phase, AggregatePhase::Final { .. }) {
                    continue;
                }
                match by_sequence.entry(call_sequence) {
                    Entry::Vacant(entry) => {
                        entry.insert(Some(ordinal));
                    }
                    Entry::Occupied(mut entry) => {
                        entry.insert(None);
                    }
                }
            }
            entry.insert(by_sequence);
        }
        let ordinal = self
            .aggregate_sequences
            .get(&key)?
            .get(&sequence)?
            .as_ref()?;
        calls.get(*ordinal)
    }

    pub(crate) fn ensure_port(
        &mut self,
        key: (FragmentId, NodeId),
        values: &[ValueId],
        budget: &mut SemanticTraceWorkBudget,
    ) -> bool {
        if let Entry::Vacant(entry) = self.ports.entry(key) {
            if !budget.charge(values.len()) {
                return false;
            }
            entry.insert(ValuePortIndex::new(values));
        }
        true
    }

    pub(crate) fn map_edge_values(
        &mut self,
        edge: EdgeId,
        mapping: &[(ValueId, ValueId)],
        values: &[ValueId],
        allow_identical_duplicates: bool,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        if let Entry::Vacant(entry) = self.edges.entry(edge) {
            if !budget.charge(mapping.len()) {
                return None;
            }
            entry.insert(ValueMappingIndex::from_pairs(mapping));
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let index = self.edges.get(&edge)?;
        values
            .iter()
            .map(|value| index.resolve(*value, allow_identical_duplicates))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn map_union_values(
        &mut self,
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: usize,
        outputs: &[ValueId],
        input: &[ValueId],
        values: &[ValueId],
        allow_identical_duplicates: bool,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        if outputs.len() != input.len() {
            return None;
        }
        let key = (fragment, node, input_ordinal);
        if let Entry::Vacant(entry) = self.unions.entry(key) {
            if !budget.charge(outputs.len()) {
                return None;
            }
            let mut index = ValueMappingIndex::default();
            for (destination, source) in outputs.iter().copied().zip(input.iter().copied()) {
                index.insert(Some(source), destination);
            }
            entry.insert(index);
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let index = self.unions.get(&key)?;
        values
            .iter()
            .map(|value| index.resolve(*value, allow_identical_duplicates))
            .collect()
    }

    pub(crate) fn map_project_values(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        child: &PhysicalNode,
        expressions: &[(ExprId, ValueId)],
        values: &[ValueId],
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        let child_key = (fragment.id(), child.id);
        if !self.ensure_port(child_key, &child.output.columns, budget) {
            return None;
        }
        let project_key = (fragment.id(), node.id);
        if let Entry::Vacant(entry) = self.projects.entry(project_key) {
            if !budget.charge(expressions.len()) {
                return None;
            }
            let mut index = ValueMappingIndex::default();
            for (expression, output) in expressions {
                index.insert(
                    crate::expression_value(fragment.expressions(), *expression),
                    *output,
                );
            }
            entry.insert(index);
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let child_values = self.ports.get(&child_key)?;
        let expression_sources = self.projects.get(&project_key)?;
        values
            .iter()
            .map(|expected| {
                if child_values.contains(expected) {
                    Some(*expected)
                } else {
                    expression_sources.resolve(*expected, false)
                }
            })
            .collect()
    }
}

pub(crate) fn bounded_count(
    errors: &mut ValidationContext,
    path: &str,
    actual: usize,
    maximum: usize,
) {
    if actual > maximum {
        errors.push(ValidationError::resource_limit(
            path,
            format!("contains {actual} items, exceeding {maximum}"),
        ));
    }
}
