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

//! Original state emissions and ordered transport links, retained only by SQL.
//! These facts certify provenance, never cross-phase state compatibility.

use super::*;
use novarocks_physical_plan::{EdgeId, NodeId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AggregateStateEndpoint {
    pub fragment: FragmentId,
    pub node: NodeId,
    pub value: ValueId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateStateTransport {
    Project,
    Stream(EdgeId),
    UnionAll,
    PartialTopN,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AggregateStateLink {
    pub input_ordinal: u32,
    pub mapping_ordinal: u32,
    pub source: AggregateStateEndpoint,
}

#[derive(Debug)]
enum StateOrigin {
    Emission(PhysicalCallSite),
    Transport(AggregateStateTransport, Box<[AggregateStateLink]>),
}
#[derive(Debug)]
struct StateSource {
    endpoint: AggregateStateEndpoint,
    origin: StateOrigin,
}

#[derive(Debug, Default)]
pub(crate) struct AggregateStateSources {
    // Dense indices reflect actual insertions, never a sparse physical ID.
    sources: Vec<StateSource>,
    index: BTreeMap<AggregateStateEndpoint, usize>,
    inputs: BTreeMap<(FragmentId, PhysicalCallSite), AggregateStateEndpoint>,
    links: usize,
}
impl AggregateStateSources {
    pub fn contains_observed(
        &self,
        endpoint: AggregateStateEndpoint,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, SqlSourceJournalError> {
        work.flush()?;
        let present = self.index.contains_key(&endpoint);
        work.step()?;
        Ok(present)
    }
    fn insert_observed(
        &mut self,
        endpoint: AggregateStateEndpoint,
        origin: StateOrigin,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        work.flush()?;
        let duplicate = self.index.contains_key(&endpoint);
        work.step()?;
        if duplicate {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate state output is duplicated",
            ));
        }
        self.sources
            .len()
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.sources
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        let index = self.sources.len();
        self.sources.push(StateSource { endpoint, origin });
        work.flush()?;
        // The original caller admits BTree storage and retained sources.
        self.index.insert(endpoint, index);
        work.step()?;
        Ok(())
    }
    pub fn emission_observed(
        &mut self,
        endpoint: AggregateStateEndpoint,
        site: PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        self.insert_observed(endpoint, StateOrigin::Emission(site), work)
    }
    pub fn transport_observed(
        &mut self,
        endpoint: AggregateStateEndpoint,
        kind: AggregateStateTransport,
        links: Vec<AggregateStateLink>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        if links.is_empty() {
            return Ok(());
        }
        let next = self
            .links
            .checked_add(links.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        // Each source must already exist: the immutable provenance graph is a
        // backward-only DAG, not a cloned or recursively flattened snapshot.
        for link in &links {
            if !self.contains_observed(link.source, work)? {
                return Err(SqlSourceJournalError::InvalidSource(
                    "aggregate state transport has no original source",
                ));
            }
        }
        work.flush()?;
        let links = links.into_boxed_slice();
        work.step()?;
        self.insert_observed(endpoint, StateOrigin::Transport(kind, links), work)?;
        self.links = next;
        Ok(())
    }
    pub fn input_observed(
        &mut self,
        fragment: FragmentId,
        site: PhysicalCallSite,
        endpoint: AggregateStateEndpoint,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        work.flush()?;
        let key = (fragment, site);
        let duplicate = self.inputs.contains_key(&key);
        work.step()?;
        if duplicate {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate state input is duplicated",
            ));
        }
        work.flush()?;
        self.inputs.insert(key, endpoint);
        work.step()?;
        Ok(())
    }
}

/// A loan into the same original journal. Merge still uses its own logical
/// request; this graph exposes every original emission without choosing one.
#[derive(Clone, Copy)]
pub(crate) struct CheckedAggregateStateInputs<'a> {
    owner: &'a SqlAuthoredPhysicalPlan,
    root: AggregateStateEndpoint,
}
impl<'a> CheckedAggregateStateInputs<'a> {
    pub const fn root(&self) -> AggregateStateEndpoint {
        self.root
    }
    /// Visit each actual graph node once. Ordered links, including repeated
    /// occurrences, remain in the original graph and are separately visited.
    /// Reconvergent paths never cause exponential request clones or traversal.
    pub fn visit_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
        mut emission: impl FnMut(
            CheckedAggregateLogicalSourceEntry<'a>,
            AggregateStateEndpoint,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
        mut transport: impl FnMut(
            AggregateStateEndpoint,
            AggregateStateTransport,
            &[AggregateStateLink],
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
    ) -> Result<(), SqlSourceJournalError> {
        let sources = &self.owner.call_sources.state_sources;
        if !sources.contains_observed(self.root, work)? {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate state input has no original emitted route",
            ));
        }
        // Dense scratch is bounded by actual stored emissions/transport nodes.
        // It is caller-owned invoice scratch, not a formal allocation grant.
        work.flush()?;
        let mut pending = Vec::new();
        pending
            .try_reserve(sources.sources.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        let mut queued = BTreeSet::new();
        pending.push(self.root);
        work.step()?;
        work.flush()?;
        queued.insert(self.root);
        work.step()?;
        while let Some(endpoint) = pending.pop() {
            work.flush()?;
            let index = sources.index.get(&endpoint).copied();
            work.step()?;
            let source = index.and_then(|index| sources.sources.get(index));
            work.step()?;
            let source = source.ok_or(SqlSourceJournalError::InvalidSource(
                "aggregate state input has no original emitted route",
            ))?;
            match &source.origin {
                StateOrigin::Emission(site) => {
                    work.flush()?;
                    let fragment = self.owner.plan.fragments().get(&endpoint.fragment);
                    work.step()?;
                    let fragment = fragment.ok_or(SqlSourceJournalError::MissingEntry)?;
                    work.flush()?;
                    let node = fragment.nodes().get(&endpoint.node);
                    work.step()?;
                    let node = node.ok_or(SqlSourceJournalError::MissingEntry)?;
                    let call = match (site, &node.kind) {
                        (
                            PhysicalCallSite::Aggregate { node: id, call },
                            NodeKind::Aggregate { calls, .. },
                        ) if *id == node.id => calls.get(*call as usize),
                        _ => None,
                    };
                    work.step()?;
                    let call = call.ok_or(SqlSourceJournalError::InvalidSource(
                        "aggregate state route differs from its actual emission",
                    ))?;
                    let same_output = call.output == source.endpoint.value
                        && !call.binding.phase.produces_final_result();
                    work.step()?;
                    if !same_output {
                        return Err(SqlSourceJournalError::InvalidSource(
                            "aggregate state route loans a result or foreign output",
                        ));
                    }
                    let entry = self
                        .owner
                        .checked_aggregate_source_observed(fragment, node, *site, call, work)?;
                    let input = if entry.phase().consumes_logical_arguments() {
                        None
                    } else {
                        Some(check_state_inputs_observed(&entry, work)?.root)
                    };
                    emission(entry, endpoint, work)?;
                    if let Some(input) = input {
                        enqueue_observed(sources, input, &mut queued, &mut pending, work)?;
                    }
                }
                StateOrigin::Transport(kind, links) => {
                    // Exact ordinals and repeated links are borrowed rather
                    // than deduplicated into a set of logical requests.
                    check_transport_observed(self.owner, endpoint, *kind, links, work)?;
                    transport(endpoint, *kind, links, work)?;
                    for link in links.iter().rev() {
                        enqueue_observed(sources, link.source, &mut queued, &mut pending, work)?;
                    }
                }
            }
        }
        Ok(())
    }
}
/// Check the retained record against this immutable emission, not against a
/// separately inferred state grammar or implementation compatibility rule.
fn check_transport_observed(
    owner: &SqlAuthoredPhysicalPlan,
    endpoint: AggregateStateEndpoint,
    kind: AggregateStateTransport,
    links: &[AggregateStateLink],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlSourceJournalError> {
    work.flush()?;
    let fragment = owner.plan.fragments().get(&endpoint.fragment);
    work.step()?;
    let fragment = fragment.ok_or(SqlSourceJournalError::MissingEntry)?;
    work.flush()?;
    let node = fragment.nodes().get(&endpoint.node);
    work.step()?;
    let node = node.ok_or(SqlSourceJournalError::MissingEntry)?;
    let edge = if let AggregateStateTransport::Stream(id) = kind {
        work.flush()?;
        let edge = owner.plan.edges().get(&id);
        work.step()?;
        Some(edge.ok_or(SqlSourceJournalError::MissingEntry)?)
    } else {
        None
    };
    for link in links {
        let ordinal = link.mapping_ordinal as usize;
        let local_input = link.source.fragment == endpoint.fragment
            && node.inputs.get(link.input_ordinal as usize) == Some(&link.source.node);
        let output = node.output.columns.get(ordinal) == Some(&endpoint.value);
        let same = match (&node.kind, kind) {
            (NodeKind::Project { expressions }, AggregateStateTransport::Project) => {
                let pair = expressions.get(ordinal).copied();
                work.step()?;
                work.flush()?;
                let expression =
                    pair.and_then(|(expression, _)| fragment.expressions().get(expression));
                // ExprArena is an opaque owner lookup on the original meter.
                work.step()?;
                local_input && output && pair.is_some_and(|(_, value)| value == endpoint.value)
                    && expression.is_some_and(|expr| matches!(expr.kind, novarocks_physical_plan::ExprKind::Value(value) if value == link.source.value))
            }
            (
                NodeKind::SetOp {
                    kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                    input_mappings,
                },
                AggregateStateTransport::UnionAll,
            ) => {
                local_input
                    && output
                    && input_mappings
                        .get(link.input_ordinal as usize)
                        .and_then(|mapping| mapping.get(ordinal))
                        == Some(&link.source.value)
            }
            (
                NodeKind::TopN {
                    phase: novarocks_physical_plan::TopNPhase::Partial { .. },
                    reduction: novarocks_physical_plan::TopNReduction::Rows,
                    ..
                },
                AggregateStateTransport::PartialTopN,
            ) => local_input && output && link.source.value == endpoint.value,
            (
                NodeKind::ExchangeSource { edge: id, .. },
                AggregateStateTransport::Stream(expected),
            ) => {
                let edge = edge.ok_or(SqlSourceJournalError::MissingEntry)?;
                work.flush()?;
                let sender = owner.plan.fragments().get(&link.source.fragment);
                work.step()?;
                *id == expected
                    && edge.kind == novarocks_physical_plan::EdgeKind::Stream
                    && edge.destination.fragment == endpoint.fragment
                    && edge.destination.node == endpoint.node
                    && edge.source.fragment == link.source.fragment
                    && sender.is_some_and(|fragment| fragment.root() == link.source.node)
                    && link.input_ordinal == 0
                    && output
                    && edge.source.projection.get(ordinal) == Some(&link.source.value)
                    && edge.destination.receive_mapping.get(ordinal)
                        == Some(&(link.source.value, endpoint.value))
            }
            _ => false,
        };
        work.step()?;
        if !same {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate state transport differs from its original mapping",
            ));
        }
    }
    Ok(())
}

fn enqueue_observed(
    sources: &AggregateStateSources,
    endpoint: AggregateStateEndpoint,
    queued: &mut BTreeSet<AggregateStateEndpoint>,
    pending: &mut Vec<AggregateStateEndpoint>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlSourceJournalError> {
    if !sources.contains_observed(endpoint, work)? {
        return Err(SqlSourceJournalError::InvalidSource(
            "aggregate state input has no original emitted route",
        ));
    }
    work.flush()?;
    let fresh = queued.insert(endpoint);
    work.step()?;
    if fresh {
        work.flush()?;
        pending
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        pending.push(endpoint);
        work.step()?;
    }
    Ok(())
}

pub(super) fn check_state_inputs_observed<'a>(
    entry: &CheckedAggregateLogicalSourceEntry<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedAggregateStateInputs<'a>, SqlSourceJournalError> {
    let AggregateRuntimeDemand::ExpressionState(expression) = entry.runtime else {
        return Err(SqlSourceJournalError::InvalidSource(
            "aggregate update has no merge state inputs",
        ));
    };
    work.flush()?;
    let endpoint = entry
        .owner
        .call_sources
        .state_sources
        .inputs
        .get(&(entry.fragment.id(), entry.site))
        .copied();
    work.step()?;
    let endpoint = endpoint.ok_or(SqlSourceJournalError::InvalidSource(
        "aggregate merge has no original state input association",
    ))?;
    work.flush()?;
    let actual = entry.fragment.expressions().get(expression);
    work.step()?;
    if endpoint.fragment != entry.fragment.id() || entry.node.inputs.as_ref() != [endpoint.node] || !actual.is_some_and(|expr| matches!(expr.kind, novarocks_physical_plan::ExprKind::Value(value) if value == endpoint.value)) {
        return Err(SqlSourceJournalError::InvalidSource("aggregate state association differs from its actual merge channel"));
    }
    work.flush()?;
    let child = entry.fragment.nodes().get(&endpoint.node);
    work.step()?;
    let child = child.ok_or(SqlSourceJournalError::MissingEntry)?;
    let mut present = false;
    for value in &child.output.columns {
        work.step()?;
        present |= *value == endpoint.value;
    }
    if !present {
        return Err(SqlSourceJournalError::InvalidSource(
            "aggregate state input is absent from its original child port",
        ));
    }
    Ok(CheckedAggregateStateInputs {
        owner: entry.owner,
        root: endpoint,
    })
}
