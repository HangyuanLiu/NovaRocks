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

use crate::resource::{
    CutResourcePreflight, CutResourceUsage, MAX_PLAN_DERIVED_CUT_BYTES, MAX_PLAN_DERIVED_CUT_ITEMS,
};
use crate::{
    CutImport, CutValue, Edge, EdgeId, Fragment, FragmentCuts, FragmentId, FragmentSink,
    InboundFragmentCut, NodeId, NodeKind, OutboundFragmentCut, PhysicalPlan, RequiredContracts,
    ValueOrigin,
};

/// Derive the explicit cut contract used to validate one fragment without the
/// rest of the plan graph.
pub fn fragment_cuts(plan: &PhysicalPlan, fragment_id: FragmentId) -> Option<FragmentCuts> {
    let derivation = FragmentCutDerivation::new(plan, &PlanLimits::FROZEN)?;
    derivation.derive(plan, fragment_id)
}

/// Derive every independently verifiable fragment cut in one indexed pass.
pub fn derive_fragment_cuts(plan: &PhysicalPlan) -> Option<BTreeMap<FragmentId, FragmentCuts>> {
    let mut errors = ValidationContext::new();
    let derivation = FragmentCutDerivation::new(plan, &PlanLimits::FROZEN)?;
    let mut total_items = 0usize;
    let mut total_bytes = 0usize;
    for fragment in plan.fragments().keys().copied() {
        let usage = preflight_fragment_cut_resources(plan, fragment, &derivation, &mut errors)?;
        total_items = total_items.saturating_add(usage.items);
        total_bytes = total_bytes.saturating_add(usage.bytes);
    }
    if !errors.is_empty()
        || total_items > MAX_PLAN_DERIVED_CUT_ITEMS
        || total_bytes > MAX_PLAN_DERIVED_CUT_BYTES
    {
        return None;
    }
    plan.fragments()
        .keys()
        .copied()
        .map(|fragment| Some((fragment, derivation.derive_preflighted(plan, fragment)?)))
        .collect()
}

pub(crate) struct FragmentCutDerivation {
    pub(crate) provenance: PlanSourceProvenance,
    pub(crate) inbound: BTreeMap<FragmentId, Vec<EdgeId>>,
    pub(crate) outbound: BTreeMap<FragmentId, Vec<EdgeId>>,
    pub(crate) change_stream_writers: BTreeMap<EdgeId, crate::ChangeStreamWriterCut>,
    pub(crate) proof_hulls: Vec<RuntimeFilterProofHull>,
    pub(crate) proof_hull_by_fragment: BTreeMap<FragmentId, usize>,
}

pub(crate) struct RuntimeFilterProofHull {
    pub(crate) fragments: BTreeSet<FragmentId>,
    pub(crate) edges: BTreeSet<EdgeId>,
    pub(crate) filters: BTreeSet<crate::RuntimeFilterId>,
}

#[derive(Default)]
pub(crate) struct RuntimeFilterBuildDependencyClosure {
    pub(crate) fragments: BTreeSet<FragmentId>,
    pub(crate) edges: BTreeSet<EdgeId>,
    pub(crate) sites: BTreeSet<(FragmentId, NodeId)>,
}

#[derive(Default)]
pub(crate) struct RuntimeFilterBuildDependencyCache {
    pub(crate) by_root: BTreeMap<(FragmentId, NodeId), RuntimeFilterBuildDependencyClosure>,
    pub(crate) source_sinks: Option<SourceSinkEdgeIndex>,
}

pub(crate) struct RuntimeFilterBuildExpansion<'a> {
    pub(crate) fragments: &'a mut BTreeSet<FragmentId>,
    pub(crate) edges: &'a mut BTreeSet<EdgeId>,
    pub(crate) dependency_sites: &'a mut BTreeSet<(FragmentId, NodeId)>,
    pub(crate) new_dependency_sites: &'a mut Vec<(FragmentId, NodeId)>,
    pub(crate) expanded_build_roots: &'a mut BTreeSet<(FragmentId, NodeId)>,
    pub(crate) cache: &'a mut RuntimeFilterBuildDependencyCache,
    pub(crate) work_budget: &'a mut SemanticTraceWorkBudget,
}

impl FragmentCutDerivation {
    pub(crate) fn new(plan: &PhysicalPlan, limits: &PlanLimits) -> Option<Self> {
        let provenance = source_provenance_index(plan)?;
        let mut inbound = BTreeMap::<FragmentId, Vec<EdgeId>>::new();
        let mut outbound = BTreeMap::<FragmentId, Vec<EdgeId>>::new();
        for edge in plan.edges().values() {
            inbound
                .entry(edge.destination.fragment)
                .or_default()
                .push(edge.id);
            outbound
                .entry(edge.source.fragment)
                .or_default()
                .push(edge.id);
        }
        let mut change_stream_writers = BTreeMap::new();
        for fragment in plan.fragments().values() {
            let FragmentSink::Router { routes, .. } = fragment.sink() else {
                continue;
            };
            for route in routes {
                let Some(edge) = plan.edges().get(&route.edge) else {
                    continue;
                };
                if edge.kind != crate::EdgeKind::ChangeStreamRouter
                    || edge.source.fragment != fragment.id()
                {
                    continue;
                }
                if let Some(proof) = change_stream_writer_cut(route, edge) {
                    change_stream_writers.insert(edge.id, proof);
                }
            }
        }
        let mut proof_hulls = Vec::new();
        let mut proof_hull_by_fragment = BTreeMap::new();
        let mut proof_hull_by_filter_set = BTreeMap::<Box<[crate::RuntimeFilterId]>, usize>::new();
        let mut build_dependency_cache = RuntimeFilterBuildDependencyCache::default();
        let mut proof_work_budget = SemanticTraceWorkBudget::new(limits);
        for fragment in plan.fragments().values() {
            let mut key = fragment.runtime_filters().to_vec();
            key.sort_unstable();
            let key = key.into_boxed_slice();
            let index = if let Some(index) = proof_hull_by_filter_set.get(&key) {
                *index
            } else {
                let mut fragments = BTreeSet::new();
                let mut edges = BTreeSet::new();
                let filters = extend_runtime_filter_proof_hull(
                    plan,
                    fragment.runtime_filters().iter().copied(),
                    &mut fragments,
                    &mut edges,
                    &mut build_dependency_cache,
                    &mut proof_work_budget,
                )?;
                for edge in &edges {
                    let edge = plan.edges().get(edge)?;
                    fragments.extend([edge.source.fragment, edge.destination.fragment]);
                }
                let index = proof_hulls.len();
                proof_hulls.push(RuntimeFilterProofHull {
                    fragments,
                    edges,
                    filters,
                });
                proof_hull_by_filter_set.insert(key, index);
                index
            };
            proof_hull_by_fragment.insert(fragment.id(), index);
        }
        Some(Self {
            provenance,
            inbound,
            outbound,
            change_stream_writers,
            proof_hulls,
            proof_hull_by_fragment,
        })
    }

    pub(crate) fn derive(
        &self,
        plan: &PhysicalPlan,
        fragment_id: FragmentId,
    ) -> Option<FragmentCuts> {
        let mut errors = ValidationContext::new();
        preflight_fragment_cut_resources(plan, fragment_id, self, &mut errors)?;
        if !errors.is_empty() {
            return None;
        }
        self.derive_preflighted(plan, fragment_id)
    }

    pub(crate) fn derive_preflighted(
        &self,
        plan: &PhysicalPlan,
        fragment_id: FragmentId,
    ) -> Option<FragmentCuts> {
        fragment_cuts_with_provenance(
            plan,
            fragment_id,
            &self.provenance,
            self.inbound
                .get(&fragment_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            self.outbound
                .get(&fragment_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            self,
            self.proof_hull(fragment_id)?,
        )
    }

    pub(crate) fn proof_hull(&self, fragment_id: FragmentId) -> Option<&RuntimeFilterProofHull> {
        self.proof_hulls
            .get(*self.proof_hull_by_fragment.get(&fragment_id)?)
    }

    pub(crate) fn change_stream_writer(
        &self,
        edge: EdgeId,
    ) -> Option<crate::ChangeStreamWriterCut> {
        self.change_stream_writers.get(&edge).cloned()
    }
}

pub(crate) fn preflight_fragment_cut_resources(
    plan: &PhysicalPlan,
    fragment_id: FragmentId,
    derivation: &FragmentCutDerivation,
    errors: &mut ValidationContext,
) -> Option<CutResourceUsage> {
    let fragment = plan.fragments().get(&fragment_id)?;
    let inbound = derivation
        .inbound
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let outbound = derivation
        .outbound
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let path = format!("fragments[{}].cuts.preflight", fragment_id.get());
    let mut usage = CutResourcePreflight::new();
    usage.add_items(inbound.len() + outbound.len());
    for (edge_id, is_outbound) in inbound
        .iter()
        .map(|edge| (edge, false))
        .chain(outbound.iter().map(|edge| (edge, true)))
    {
        let edge = plan.edges().get(edge_id)?;
        let source = plan.fragments().get(&edge.source.fragment)?;
        let source_binding_count = derivation.provenance.binding_count(edge.source.fragment)?;
        usage.add_items(edge.destination.receive_mapping.len() * if is_outbound { 2 } else { 1 });
        usage.add_items(source_binding_count);
        usage.add_distribution(&edge.partitioning.source);
        usage.add_distribution(&edge.partitioning.destination);
        for binding in derivation.provenance.binding_refs(edge.source.fragment)? {
            usage.add_source(binding, &path);
        }
        for (source_value, _) in &edge.destination.receive_mapping {
            let ty = &source.values().get(source_value)?.ty;
            usage.add_value_type(ty, &path, errors);
            if is_outbound {
                usage.add_value_type(ty, &path, errors);
            }
        }
        if let Some(proof) = derivation.change_stream_writer(edge.id) {
            usage.add_items(proof.fields.len());
        }
        if let Some(proof) = writer_result_cut(plan, edge) {
            usage.add_items(proof.fields.len());
            for field in &proof.fields {
                usage.add_bytes(field.name.len());
                usage.add_value_type(&field.ty, &path, errors);
            }
        }
    }
    let artifacts = fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Scan { relation, .. } => Some(relation.artifact_inputs()),
            _ => None,
        })
        .flatten()
        .map(|requirement| requirement.artifact)
        .collect::<BTreeSet<_>>();
    usage.add_items(artifacts.len());
    for artifact in artifacts {
        usage.add_artifact(plan.artifact_refs().get(&artifact)?, &path, errors);
    }
    usage.add_items(fragment.runtime_filters().len());
    for filter in fragment.runtime_filters() {
        usage.add_filter(plan.runtime_filters().get(filter)?, &path, errors);
    }
    let proof_hull = derivation.proof_hull(fragment_id)?;
    usage.add_items(proof_hull.fragments.len() + proof_hull.edges.len() + proof_hull.filters.len());
    for proof_fragment in &proof_hull.fragments {
        usage.add_fragment(plan.fragments().get(proof_fragment)?, errors);
    }
    for proof_edge in &proof_hull.edges {
        usage.add_edge(plan.edges().get(proof_edge)?);
    }
    for proof_filter in &proof_hull.filters {
        usage.add_filter(plan.runtime_filters().get(proof_filter)?, &path, errors);
    }
    Some(usage.validate(&format!("{path}.resources"), errors))
}

pub(crate) fn fragment_cuts_with_provenance(
    plan: &PhysicalPlan,
    fragment_id: FragmentId,
    provenance: &PlanSourceProvenance,
    inbound_edges: &[EdgeId],
    outbound_edges: &[EdgeId],
    derivation: &FragmentCutDerivation,
    proof_hull: &RuntimeFilterProofHull,
) -> Option<FragmentCuts> {
    let fragment = plan.fragments().get(&fragment_id)?;
    let inbound = inbound_edges
        .iter()
        .map(|edge| plan.edges().get(edge))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|edge| {
            let source = plan.fragments().get(&edge.source.fragment)?;
            let source_bindings = provenance.bindings(edge.source.fragment)?;
            let has_source_free_rows = provenance.has_source_free_rows(edge.source.fragment)?;
            let imports = edge
                .destination
                .receive_mapping
                .iter()
                .map(|(source_value, destination)| {
                    Some(CutImport {
                        source: CutValue {
                            value: *source_value,
                            ty: source.values().get(source_value)?.ty.clone(),
                        },
                        destination: *destination,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(InboundFragmentCut {
                edge: edge.id,
                kind: edge.kind,
                source_fragment: edge.source.fragment,
                destination_node: edge.destination.node,
                imports: imports.into_boxed_slice(),
                partitioning: edge.partitioning.clone(),
                source_bindings: source_bindings.into_boxed_slice(),
                has_source_free_rows,
                change_stream_writer: derivation.change_stream_writer(edge.id),
                writer_result: writer_result_cut(plan, edge),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let outbound = outbound_edges
        .iter()
        .map(|edge| plan.edges().get(edge))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|edge| {
            let source_bindings = provenance.bindings(edge.source.fragment)?;
            let has_source_free_rows = provenance.has_source_free_rows(edge.source.fragment)?;
            let projection = edge
                .source
                .projection
                .iter()
                .map(|value| {
                    Some(CutValue {
                        value: *value,
                        ty: fragment.values().get(value)?.ty.clone(),
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(OutboundFragmentCut {
                edge: edge.id,
                kind: edge.kind,
                destination_fragment: edge.destination.fragment,
                projection: projection.into_boxed_slice(),
                destination_imports: edge
                    .destination
                    .receive_mapping
                    .iter()
                    .map(|(source, destination)| {
                        Some(CutImport {
                            source: CutValue {
                                value: *source,
                                ty: fragment.values().get(source)?.ty.clone(),
                            },
                            destination: *destination,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?
                    .into_boxed_slice(),
                partitioning: edge.partitioning.clone(),
                source_bindings: source_bindings.into_boxed_slice(),
                has_source_free_rows,
                change_stream_writer: derivation.change_stream_writer(edge.id),
                writer_result: writer_result_cut(plan, edge),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let mut artifact_refs = BTreeMap::new();
    for requirement in fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Scan { relation, .. } => Some(relation.artifact_inputs()),
            _ => None,
        })
        .flatten()
    {
        artifact_refs.insert(
            requirement.artifact,
            plan.artifact_refs().get(&requirement.artifact)?.clone(),
        );
    }
    let runtime_filters = fragment
        .runtime_filters()
        .iter()
        .map(|id| plan.runtime_filters().get(id).cloned())
        .collect::<Option<Vec<_>>>()?;
    let runtime_filter_proof = crate::RuntimeFilterProofGraph {
        fragments: proof_hull
            .fragments
            .iter()
            .copied()
            .map(|id| plan.fragments().get(&id).cloned())
            .collect::<Option<Vec<_>>>()?
            .into_boxed_slice(),
        edges: proof_hull
            .edges
            .iter()
            .copied()
            .map(|id| plan.edges().get(&id).cloned())
            .collect::<Option<Vec<_>>>()?
            .into_boxed_slice(),
        filters: proof_hull
            .filters
            .iter()
            .copied()
            .map(|id| plan.runtime_filters().get(&id).cloned())
            .collect::<Option<Vec<_>>>()?
            .into_boxed_slice(),
    };
    Some(FragmentCuts {
        inbound: inbound.into_boxed_slice(),
        outbound: outbound.into_boxed_slice(),
        artifact_refs: artifact_refs.into_values().collect(),
        runtime_filters: runtime_filters.into_boxed_slice(),
        runtime_filter_proof,
    })
}

pub(crate) fn change_stream_writer_cut(
    route: &crate::ChangeStreamRoute,
    edge: &Edge,
) -> Option<crate::ChangeStreamWriterCut> {
    if route.input_mapping.len() != edge.destination.receive_mapping.len() {
        return None;
    }
    let fields = route
        .input_mapping
        .iter()
        .zip(edge.destination.receive_mapping.iter())
        .map(|((token, route_source), (mapped_source, destination))| {
            (route_source == mapped_source).then_some(crate::ChangeStreamWriterCutField {
                token: *token,
                source: *route_source,
                destination: *destination,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(crate::ChangeStreamWriterCut {
        route_id: route.route_id,
        write_target_ordinal: route.write_target_ordinal,
        fields: fields.into_boxed_slice(),
    })
}

pub(crate) fn writer_result_cut(
    plan: &PhysicalPlan,
    edge: &Edge,
) -> Option<crate::WriterResultCut> {
    if edge.kind != crate::EdgeKind::Stream {
        return None;
    }
    let source = plan.fragments().get(&edge.source.fragment)?;
    let root = source.nodes().get(&source.root())?;
    let NodeKind::TableWriter { target } = &root.kind else {
        return None;
    };
    if !matches!(source.sink(), FragmentSink::Stream { edge: sink_edge } if *sink_edge == edge.id)
        || target.output_schema.fields.len() != edge.destination.receive_mapping.len()
        || root.output.columns.as_ref() != edge.source.projection.as_ref()
    {
        return None;
    }
    let fields = target
        .output_schema
        .fields
        .iter()
        .zip(&edge.destination.receive_mapping)
        .map(|(field, (mapped_source, destination))| {
            (field.value == *mapped_source).then_some(crate::WriterResultCutField {
                source: field.value,
                destination: *destination,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(crate::WriterResultCut {
        write_target_ordinal: target.write_target_ordinal,
        schema_revision: target.output_schema.revision,
        fields: fields.into_boxed_slice(),
    })
}

pub(crate) fn extend_runtime_filter_build_dependencies(
    plan: &PhysicalPlan,
    producer: &crate::RuntimeFilterProducer,
    expansion: &mut RuntimeFilterBuildExpansion<'_>,
) -> bool {
    let Some(fragment) = plan.fragments().get(&producer.endpoint.fragment) else {
        return false;
    };
    let Some(node) = fragment.nodes().get(&producer.endpoint.node) else {
        return false;
    };
    let build_root = match (&producer.target, &node.kind) {
        (
            crate::RuntimeFilterProducerTarget::JoinBuildKey { .. },
            NodeKind::HashJoin { build_side, .. },
        ) => usize::try_from(build_side.input_ordinal())
            .ok()
            .and_then(|ordinal| node.inputs.get(ordinal))
            .copied(),
        (
            crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. },
            NodeKind::Aggregate { .. },
        ) => node.inputs.first().copied(),
        _ => None,
    };
    let Some(build_root) = build_root else {
        return false;
    };

    let root = (fragment.id(), build_root);
    if !expansion.expanded_build_roots.insert(root) {
        return true;
    }
    if expansion.cache.source_sinks.is_none() {
        expansion.cache.source_sinks = Some(SourceSinkEdgeIndex::new(plan));
    }

    if let Entry::Vacant(entry) = expansion.cache.by_root.entry(root) {
        let mut closure = RuntimeFilterBuildDependencyClosure::default();
        let mut pending = vec![root];
        let mut expanded_multicast_sources = BTreeSet::new();
        while let Some((fragment_id, node_id)) = pending.pop() {
            if !closure.sites.insert((fragment_id, node_id)) {
                continue;
            }
            if !expansion.work_budget.charge(1) {
                return false;
            }
            closure.fragments.insert(fragment_id);
            let Some(fragment) = plan.fragments().get(&fragment_id) else {
                return false;
            };
            if expanded_multicast_sources.insert(fragment_id)
                && let FragmentSink::Multicast { edges } = fragment.sink()
                && edges.len() >= 2
            {
                for edge in edges {
                    let Some(edge_contract) = plan.edges().get(edge) else {
                        return false;
                    };
                    if !expansion
                        .cache
                        .source_sinks
                        .as_ref()
                        .is_some_and(|sinks| sinks.owns(edge_contract))
                    {
                        return false;
                    }
                    let Some(destination) =
                        plan.fragments().get(&edge_contract.destination.fragment)
                    else {
                        return false;
                    };
                    closure.edges.insert(*edge);
                    closure.fragments.insert(destination.id());
                    pending.push((destination.id(), destination.root()));
                }
            }
            let Some(node) = fragment.nodes().get(&node_id) else {
                return false;
            };
            if let NodeKind::ExchangeSource { edge, .. } = &node.kind {
                let Some(edge_contract) = plan.edges().get(edge) else {
                    return false;
                };
                if edge_contract.destination.fragment != fragment_id
                    || edge_contract.destination.node != node_id
                {
                    return false;
                }
                closure.edges.insert(*edge);
                closure.fragments.extend([
                    edge_contract.source.fragment,
                    edge_contract.destination.fragment,
                ]);
                let Some(source) = plan.fragments().get(&edge_contract.source.fragment) else {
                    return false;
                };
                if !expansion
                    .cache
                    .source_sinks
                    .as_ref()
                    .is_some_and(|sinks| sinks.owns(edge_contract))
                {
                    return false;
                }
                pending.push((source.id(), source.root()));
            }
            pending.extend(node.inputs.iter().map(|input| (fragment_id, *input)));
        }
        entry.insert(closure);
    }

    let Some(closure) = expansion.cache.by_root.get(&root) else {
        return false;
    };
    let merge_work = closure
        .sites
        .len()
        .saturating_add(closure.fragments.len())
        .saturating_add(closure.edges.len());
    if !expansion.work_budget.charge(merge_work) {
        return false;
    }
    for site in &closure.sites {
        if expansion.dependency_sites.insert(*site) {
            expansion.new_dependency_sites.push(*site);
        }
    }
    expansion
        .fragments
        .extend(closure.fragments.iter().copied());
    expansion.edges.extend(closure.edges.iter().copied());
    true
}

pub(crate) fn extend_runtime_filter_proof_hull(
    plan: &PhysicalPlan,
    seed_filters: impl IntoIterator<Item = crate::RuntimeFilterId>,
    fragments: &mut BTreeSet<FragmentId>,
    edges: &mut BTreeSet<EdgeId>,
    build_dependency_cache: &mut RuntimeFilterBuildDependencyCache,
    work_budget: &mut SemanticTraceWorkBudget,
) -> Option<BTreeSet<crate::RuntimeFilterId>> {
    let mut seeds = BTreeSet::new();
    for filter in seed_filters {
        if !work_budget.charge(1) {
            return None;
        }
        seeds.insert(filter);
    }
    let mut included = BTreeSet::new();
    let mut expanded = BTreeSet::new();
    let mut include_queue = seeds.iter().copied().collect::<Vec<_>>();
    let mut active_queue = include_queue.clone();
    let mut dependency_sites = BTreeSet::new();
    let mut expanded_build_roots = BTreeSet::new();
    let mut blocking_at = BTreeMap::<(FragmentId, NodeId), Vec<crate::RuntimeFilterId>>::new();

    while !include_queue.is_empty() || !active_queue.is_empty() {
        while let Some(filter_id) = include_queue.pop() {
            if !included.insert(filter_id) {
                continue;
            }
            let filter = plan.runtime_filters().get(&filter_id)?;
            let static_work = filter
                .equality_witnesses
                .len()
                .saturating_add(filter.producers.len())
                .saturating_add(filter.consumers.len())
                .saturating_add(filter.producers.iter().fold(0usize, |work, producer| {
                    work.saturating_add(producer.progress.build_edges.len())
                        .saturating_add(producer.progress.non_build_edges.len())
                }))
                .saturating_add(filter.consumers.iter().fold(0usize, |work, consumer| {
                    work.saturating_add(match &consumer.target {
                        crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => 0,
                        crate::RuntimeFilterConsumerTarget::ScanField { lineage, .. }
                        | crate::RuntimeFilterConsumerTarget::AggregateTopNScanField {
                            lineage,
                            ..
                        } => lineage.len(),
                    })
                }));
            if !work_budget.charge(static_work) {
                return None;
            }
            fragments.extend(
                filter
                    .equality_witnesses
                    .iter()
                    .map(|witness| witness.fragment),
            );
            fragments.extend(
                filter
                    .producers
                    .iter()
                    .map(|producer| producer.endpoint.fragment),
            );
            fragments.extend(
                filter
                    .consumers
                    .iter()
                    .map(|consumer| consumer.endpoint.fragment),
            );
            for producer in &filter.producers {
                edges.extend(
                    producer
                        .progress
                        .build_edges
                        .iter()
                        .chain(&producer.progress.non_build_edges)
                        .copied(),
                );
            }
            for consumer in &filter.consumers {
                if consumer.activation == crate::RuntimeFilterConsumerActivation::BlockingSnapshot {
                    let site = (consumer.endpoint.fragment, consumer.endpoint.node);
                    blocking_at.entry(site).or_default().push(filter_id);
                    if dependency_sites.contains(&site) {
                        active_queue.push(filter_id);
                    }
                }
                let lineage = match &consumer.target {
                    crate::RuntimeFilterConsumerTarget::ScanField { lineage, .. }
                    | crate::RuntimeFilterConsumerTarget::AggregateTopNScanField {
                        lineage, ..
                    } => lineage,
                    crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => continue,
                };
                for step in lineage {
                    match step {
                        crate::RuntimeFilterLineageStep::FilterPassThrough { fragment, .. }
                        | crate::RuntimeFilterLineageStep::SortPassThrough { fragment, .. }
                        | crate::RuntimeFilterLineageStep::ProjectIdentity { fragment, .. }
                        | crate::RuntimeFilterLineageStep::JoinEquality { fragment, .. }
                        | crate::RuntimeFilterLineageStep::JoinOutputPassThrough {
                            fragment, ..
                        }
                        | crate::RuntimeFilterLineageStep::AggregateGroupKey { fragment, .. }
                        | crate::RuntimeFilterLineageStep::UnionAllBranch { fragment, .. } => {
                            fragments.insert(*fragment);
                        }
                        crate::RuntimeFilterLineageStep::ExchangeMapping { edge, .. } => {
                            edges.insert(*edge);
                        }
                    }
                }
            }
        }

        while let Some(filter_id) = active_queue.pop() {
            if !expanded.insert(filter_id) {
                continue;
            }
            let filter = plan.runtime_filters().get(&filter_id)?;
            if !work_budget.charge(filter.producers.len()) {
                return None;
            }
            for producer in &filter.producers {
                let mut new_sites = Vec::new();
                let mut expansion = RuntimeFilterBuildExpansion {
                    fragments,
                    edges,
                    dependency_sites: &mut dependency_sites,
                    new_dependency_sites: &mut new_sites,
                    expanded_build_roots: &mut expanded_build_roots,
                    cache: build_dependency_cache,
                    work_budget,
                };
                if !extend_runtime_filter_build_dependencies(plan, producer, &mut expansion) {
                    return None;
                }
                for site in new_sites {
                    if let Some(blocked_filters) = blocking_at.get(&site) {
                        if !work_budget.charge(blocked_filters.len()) {
                            return None;
                        }
                        active_queue.extend(blocked_filters.iter().copied());
                    }
                    let dependency_fragment = plan.fragments().get(&site.0)?;
                    if !work_budget.charge(dependency_fragment.runtime_filters().len()) {
                        return None;
                    }
                    include_queue.extend(dependency_fragment.runtime_filters().iter().copied());
                }
            }
        }
    }

    Some(included)
}

pub(crate) fn validate_fragment_cuts_into(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    validate_runtime_filter_proof: bool,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].cuts", fragment.id().get());
    let local_provenance = fragment_source_provenance(fragment, cuts);
    bounded_count(
        errors,
        &format!("{path}.inbound"),
        cuts.inbound.len(),
        errors.limits().plan_edges,
    );
    bounded_count(
        errors,
        &format!("{path}.outbound"),
        cuts.outbound.len(),
        errors.limits().plan_edges,
    );
    bounded_count(
        errors,
        &format!("{path}.artifact_refs"),
        cuts.artifact_refs.len(),
        errors.limits().plan_artifact_refs,
    );
    bounded_count(
        errors,
        &format!("{path}.runtime_filters"),
        cuts.runtime_filters.len(),
        errors.limits().plan_runtime_filters,
    );
    let mut inbound_ids = BTreeSet::new();
    for cut in &cuts.inbound {
        bounded_count(
            errors,
            &format!("{path}.inbound.imports"),
            cut.imports.len(),
            errors.limits().fragment_values,
        );
        bounded_count(
            errors,
            &format!("{path}.inbound.source_bindings"),
            cut.source_bindings.len(),
            errors.limits().plan_artifact_refs,
        );
        for source in &cut.source_bindings {
            validate_read_reference(&source.source, &path, errors);
            if source.selection_digest == [0; 32] {
                errors.push(ValidationError::new(
                    &path,
                    "upstream source binding has a zero selection digest",
                ));
            }
        }
        if !inbound_ids.insert(cut.edge) {
            errors.push(ValidationError::new(&path, "duplicate inbound edge"));
        }
        if cut.source_fragment == fragment.id() {
            errors.push(ValidationError::new(
                &path,
                "inbound cut has an invalid peer identity",
            ));
        }
        match fragment.nodes().get(&cut.destination_node) {
            Some(node)
                if matches!(
                    &node.kind,
                    NodeKind::ExchangeSource { edge, imports }
                        if *edge == cut.edge
                            && imports.len() == cut.imports.len()
                            && imports.iter().zip(&cut.imports).all(
                                |((source, destination), cut)| {
                                    *source == cut.source.value && *destination == cut.destination
                                }
                            )
                ) =>
            {
                if node.output_properties.distribution != cut.partitioning.destination
                    || node.output_properties.row_multiplicity
                        != cut.partitioning.destination_multiplicity
                    || !node.output_properties.ordering.is_empty()
                {
                    errors.push(ValidationError::new(
                        &path,
                        "exchange source properties differ from its inbound cut",
                    ));
                }
            }
            Some(_) => errors.push(ValidationError::new(
                &path,
                "inbound cut does not match its exchange source node",
            )),
            None => errors.push(ValidationError::new(
                &path,
                "inbound cut destination node is not defined",
            )),
        }
        validate_distribution(
            fragment,
            &cut.partitioning.destination,
            "inbound_cut.destination_partitioning",
            errors,
        );
        validate_mapped_partitioning(
            &cut.partitioning,
            &cut.imports
                .iter()
                .map(|import| (import.source.value, import.destination))
                .collect::<Vec<_>>(),
            &path,
            errors,
        );
        for import in &cut.imports {
            match fragment.values().get(&import.destination) {
                // The imported column may admit null the sender never writes;
                // it is declared by the statement's column layout, not by the
                // value that fills it. It may not declare the reverse.
                Some(value)
                    if value.ty.data_type == import.source.ty.data_type
                        && (value.ty.nullable || !import.source.ty.nullable)
                        && import_origin_matches(
                            &value.origin,
                            cut.edge,
                            cut.kind,
                            cut.source_fragment,
                            import.source.value,
                        ) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "inbound cut type or destination origin is inconsistent",
                )),
                None => errors.push(ValidationError::new(
                    &path,
                    "inbound cut destination value is not defined",
                )),
            }
        }
        validate_inbound_change_stream_writer(fragment, cut, &path, errors);
        validate_inbound_writer_result_structure(fragment, cut, &path, errors);
    }
    let expected_inbound_list = fragment
        .nodes()
        .values()
        .filter_map(|node| match node.kind {
            NodeKind::ExchangeSource { edge, .. } => Some(edge),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_inbound = expected_inbound_list
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if expected_inbound.len() != expected_inbound_list.len() {
        errors.push(ValidationError::new(
            &path,
            "more than one exchange source claims the same inbound edge",
        ));
    }
    if inbound_ids != expected_inbound {
        errors.push(ValidationError::new(
            &path,
            "inbound cuts differ from the fragment exchange sources",
        ));
    }
    let mut inbound_indexes = BTreeMap::new();
    for cut in &cuts.inbound {
        inbound_indexes.entry(cut.edge).or_insert_with(|| {
            (
                cut,
                ValueMappingIndex::from_pairs_iter(
                    cut.imports
                        .iter()
                        .map(|import| (import.source.value, import.destination)),
                ),
            )
        });
    }
    for value in fragment.values().values() {
        let found = match value.origin {
            ValueOrigin::ExchangeImport { edge, source_value } => {
                inbound_indexes.get(&edge).is_some_and(|(cut, imports)| {
                    cut.kind != crate::EdgeKind::CteMulticast
                        && imports.contains(source_value, value.id)
                })
            }
            ValueOrigin::CteImport {
                edge,
                producer_fragment,
                producer_value,
            } => inbound_indexes.get(&edge).is_some_and(|(cut, imports)| {
                cut.kind == crate::EdgeKind::CteMulticast
                    && cut.source_fragment == producer_fragment
                    && imports.contains(producer_value, value.id)
            }),
            _ => continue,
        };
        if !found {
            errors.push(ValidationError::new(
                &path,
                "cross-fragment import is absent from the inbound cuts",
            ));
        }
    }

    let root_values = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| ValuePortIndex::new(&root.output.columns));
    let root_multiplicity = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output_properties.row_multiplicity);
    let mut outbound_ids = BTreeSet::new();
    for cut in &cuts.outbound {
        bounded_count(
            errors,
            &format!("{path}.outbound.projection"),
            cut.projection.len(),
            errors.limits().fragment_values,
        );
        bounded_count(
            errors,
            &format!("{path}.outbound.source_bindings"),
            cut.source_bindings.len(),
            errors.limits().plan_artifact_refs,
        );
        for source in &cut.source_bindings {
            validate_read_reference(&source.source, &path, errors);
            if source.selection_digest == [0; 32] {
                errors.push(ValidationError::new(
                    &path,
                    "outbound source binding has a zero selection digest",
                ));
            }
        }
        if cut.has_source_free_rows != local_provenance.has_source_free_rows
            || !same_source_bindings(&cut.source_bindings, &local_provenance.bindings)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound source provenance differs from the fragment's exact inputs",
            ));
        }
        if !outbound_ids.insert(cut.edge) {
            errors.push(ValidationError::new(&path, "duplicate outbound edge"));
        }
        if cut.destination_fragment == fragment.id() {
            errors.push(ValidationError::new(
                &path,
                "outbound cut has an invalid peer",
            ));
        }
        for projected in &cut.projection {
            match fragment.values().get(&projected.value) {
                Some(value) if value.ty != projected.ty => errors.push(ValidationError::new(
                    &path,
                    "outbound cut type differs from its source value",
                )),
                Some(_) => {}
                None => errors.push(ValidationError::new(
                    &path,
                    "outbound cut source value is not defined",
                )),
            }
        }
        if cut.destination_imports.len() != cut.projection.len()
            || cut
                .projection
                .iter()
                .zip(&cut.destination_imports)
                .any(|(projected, import)| projected != &import.source)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound cut projection differs from its destination import mapping",
            ));
        }
        validate_mapped_partitioning(
            &cut.partitioning,
            &cut.destination_imports
                .iter()
                .map(|import| (import.source.value, import.destination))
                .collect::<Vec<_>>(),
            &path,
            errors,
        );
        if root_values.as_ref().is_some_and(|root_values| {
            cut.projection
                .iter()
                .any(|projected| !root_values.contains(&projected.value))
        }) {
            errors.push(ValidationError::new(
                &path,
                "outbound cut projects a value absent from the fragment root output",
            ));
        }
        if root_multiplicity
            .is_some_and(|multiplicity| multiplicity != cut.partitioning.source_multiplicity)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound cut row multiplicity differs from the fragment root",
            ));
        }
        validate_outbound_writer_result(fragment, cut, &path, errors);
        validate_distribution(
            fragment,
            &cut.partitioning.source,
            "outbound_cut.source_partitioning",
            errors,
        );
        if root_values.as_ref().is_some_and(|root_values| {
            distribution_values(&cut.partitioning.source)
                .iter()
                .any(|value| !root_values.contains(value))
        }) {
            errors.push(ValidationError::new(
                &path,
                "outbound partition key is absent from the fragment root output",
            ));
        }
    }
    let sink_edges = match fragment.sink() {
        FragmentSink::Stream { edge } => vec![*edge],
        FragmentSink::Multicast { edges } => edges.to_vec(),
        FragmentSink::Router { routes, .. } => routes.iter().map(|route| route.edge).collect(),
        FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => Vec::new(),
    };
    let sink_edge_ids = sink_edges.iter().copied().collect::<BTreeSet<_>>();
    if sink_edge_ids.len() != sink_edges.len() {
        errors.push(ValidationError::new(
            &path,
            "fragment sink destinations contain duplicate edge occurrences",
        ));
    }
    if sink_edge_ids != outbound_ids {
        errors.push(ValidationError::new(
            &path,
            "outbound cuts differ from the fragment sink destinations",
        ));
    }
    let outbound_by_edge = cuts
        .outbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    if let FragmentSink::Router { routes, .. } = fragment.sink() {
        for route in routes {
            let cut = outbound_by_edge.get(&route.edge).copied();
            if cut.is_none_or(|cut| {
                !cut.projection
                    .iter()
                    .map(|value| value.value)
                    .eq(route.input_mapping.iter().map(|(_, value)| *value))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "router edge projection differs from its exact route input sequence",
                ));
            }
            if let Some(cut) = cut {
                validate_router_partitioning(route, &cut.partitioning.source, &path, errors);
                let proof_matches = cut.change_stream_writer.as_ref().is_some_and(|proof| {
                    proof.route_id == route.route_id
                        && proof.write_target_ordinal == route.write_target_ordinal
                        && proof.fields.len() == route.input_mapping.len()
                        && proof.fields.len() == cut.destination_imports.len()
                        && proof
                            .fields
                            .iter()
                            .zip(&route.input_mapping)
                            .zip(&cut.destination_imports)
                            .all(|((proof, (token, source)), import)| {
                                proof.token == *token
                                    && proof.source == *source
                                    && proof.source == import.source.value
                                    && proof.destination == import.destination
                            })
                });
                if !proof_matches {
                    errors.push(ValidationError::new(
                        &path,
                        "router outbound cut lacks its exact destination writer proof",
                    ));
                }
            }
        }
    }
    for cut in &cuts.outbound {
        if cut.kind != crate::EdgeKind::ChangeStreamRouter && cut.change_stream_writer.is_some() {
            errors.push(ValidationError::new(
                &path,
                "non-router outbound cut carries a change-stream writer proof",
            ));
        }
    }
    validate_fragment_writer_results(fragment, cuts, &path, errors);
    if let FragmentSink::SealedArtifact(spec) = fragment.sink()
        && (local_provenance.has_source_free_rows
            || local_provenance.bindings.len() != 1
            || local_provenance
                .bindings
                .values()
                .any(|source| source != &spec.source))
    {
        errors.push(ValidationError::new(
            &path,
            "artifact inputs are not derived exclusively from the exact source binding",
        ));
    }
    validate_fragment_artifact_cuts(fragment, cuts, &path, errors);
    if validate_runtime_filter_proof {
        validate_fragment_runtime_filter_cuts(fragment, cuts, &path, errors);
    }
}

pub(crate) fn validate_inbound_change_stream_writer(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    if cut.kind != crate::EdgeKind::ChangeStreamRouter {
        if cut.change_stream_writer.is_some() {
            errors.push(ValidationError::new(
                path,
                "non-router inbound cut carries a change-stream writer proof",
            ));
        }
        return;
    }
    let Some(proof) = &cut.change_stream_writer else {
        errors.push(ValidationError::new(
            path,
            "router inbound cut lacks its destination writer proof",
        ));
        return;
    };
    let writer = fragment.nodes().get(&fragment.root());
    let target = writer.and_then(|writer| match &writer.kind {
        NodeKind::TableWriter { target } if writer.inputs.as_ref() == [cut.destination_node] => {
            Some(target)
        }
        _ => None,
    });
    let Some(target) = target else {
        errors.push(ValidationError::new(
            path,
            "router inbound cut receiver is not the direct root table writer input",
        ));
        return;
    };
    if proof.route_id == crate::ConnectorWriteRouteId::from_bytes([0; 32])
        || proof.write_target_ordinal != target.write_target_ordinal
        || proof.fields.len() != cut.imports.len()
        || proof.fields.len() != target.target_fields.len()
        || !proof
            .fields
            .iter()
            .zip(&cut.imports)
            .zip(&target.target_fields)
            .all(|((proof, import), target)| {
                proof.source == import.source.value
                    && proof.destination == import.destination
                    && proof.token == target.token
                    && proof.destination == target.input
            })
    {
        errors.push(ValidationError::new(
            path,
            "router inbound cut proof differs from its exact table writer contract",
        ));
    }
}

pub(crate) fn validate_inbound_writer_result_structure(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    if cut.kind != crate::EdgeKind::Stream {
        if cut.writer_result.is_some() {
            errors.push(ValidationError::new(
                path,
                "non-stream inbound cut carries a writer result proof",
            ));
        }
        return;
    }
    let Some(proof) = &cut.writer_result else {
        return;
    };
    let matches = proof.schema_revision == crate::WRITER_MULTIPLEX_SCHEMA_REVISION
        && proof.fields.len() == cut.imports.len()
        && proof
            .fields
            .iter()
            .zip(&cut.imports)
            .all(|(field, import)| {
                field.source == import.source.value
                    && field.destination == import.destination
                    && field.ty == import.source.ty
                    && fragment
                        .values()
                        .get(&field.destination)
                        .is_some_and(|value| value.ty == field.ty)
            });
    if !matches {
        errors.push(ValidationError::new(
            path,
            "inbound writer result proof differs from its stream import contract",
        ));
    }
}

pub(crate) fn validate_outbound_writer_result(
    fragment: &Fragment,
    cut: &OutboundFragmentCut,
    path: &str,
    errors: &mut ValidationContext,
) {
    if cut.kind != crate::EdgeKind::Stream {
        if cut.writer_result.is_some() {
            errors.push(ValidationError::new(
                path,
                "non-stream outbound cut carries a writer result proof",
            ));
        }
        return;
    }
    let root = fragment.nodes().get(&fragment.root());
    let target = root.and_then(|root| match &root.kind {
        NodeKind::TableWriter { target } => Some(target),
        _ => None,
    });
    match (target, &cut.writer_result) {
        (None, None) => {}
        (None, Some(_)) => errors.push(ValidationError::new(
            path,
            "non-writer stream carries a writer result proof",
        )),
        (Some(_), None) => errors.push(ValidationError::new(
            path,
            "table writer stream lacks its writer result proof",
        )),
        (Some(target), Some(proof)) => {
            let fields_match = proof.write_target_ordinal == target.write_target_ordinal
                && proof.schema_revision == target.output_schema.revision
                && proof.fields.len() == target.output_schema.fields.len()
                && proof.fields.len() == cut.destination_imports.len()
                && proof
                    .fields
                    .iter()
                    .zip(&target.output_schema.fields)
                    .zip(&cut.destination_imports)
                    .all(|((proof, field), import)| {
                        proof.source == field.value
                            && proof.destination == import.destination
                            && import.source.value == field.value
                            && proof.name == field.name
                            && proof.ty == field.ty
                            && proof.ty == import.source.ty
                            && proof.role == field.role
                    });
            if !fields_match {
                errors.push(ValidationError::new(
                    path,
                    "outbound writer result proof differs from its exact table writer schema",
                ));
            }
        }
    }
}

pub(crate) fn validate_fragment_writer_results(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationContext,
) {
    let inbound_by_edge = cuts
        .inbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    let mut consumed_inbound = BTreeMap::<EdgeId, usize>::new();
    let mut consumed_writers = BTreeMap::<NodeId, usize>::new();
    if let Some(root) = fragment.nodes().get(&fragment.root())
        && matches!(root.kind, NodeKind::TableWriter { .. })
    {
        for cut in &cuts.outbound {
            if cut.writer_result.is_some() {
                *consumed_writers.entry(root.id).or_default() += 1;
            }
        }
    }
    for finish_node in fragment
        .nodes()
        .values()
        .filter(|node| matches!(node.kind, NodeKind::TableFinish(_)))
    {
        if finish_node.id != fragment.root() {
            errors.push(ValidationError::new(
                path,
                "table finish must be the root of its fragment",
            ));
        }
        let NodeKind::TableFinish(finish) = &finish_node.kind else {
            unreachable!();
        };
        let finish_values = finish
            .input_schema
            .fields
            .iter()
            .map(|field| field.value)
            .collect::<Box<[_]>>();
        let mut pending = finish_node
            .inputs
            .iter()
            .map(|input| (*input, finish_values.clone()))
            .collect::<Vec<_>>();
        let mut visited = BTreeSet::new();
        let mut ordinals = Vec::new();
        while let Some((node_id, expected_values)) = pending.pop() {
            if !visited.insert(node_id) {
                errors.push(ValidationError::new(
                    path,
                    "writer relation reaches table finish through more than one local path",
                ));
                continue;
            }
            let Some(node) = fragment.nodes().get(&node_id) else {
                continue;
            };
            match &node.kind {
                NodeKind::TableWriter { target } => {
                    *consumed_writers.entry(node.id).or_default() += 1;
                    ordinals.push(target.write_target_ordinal);
                    if !writer_schema_matches_finish_values(
                        &target.output_schema,
                        &finish.input_schema,
                        &expected_values,
                    ) {
                        errors.push(ValidationError::new(
                            path,
                            "local table writer fields do not map exactly to its table finish input roles",
                        ));
                    }
                }
                NodeKind::ExchangeSource { edge, .. } => {
                    let proof = inbound_by_edge
                        .get(edge)
                        .and_then(|cut| cut.writer_result.as_ref());
                    let Some(proof) = proof else {
                        errors.push(ValidationError::new(
                            path,
                            "table finish stream lacks an upstream writer result proof",
                        ));
                        continue;
                    };
                    *consumed_inbound.entry(*edge).or_default() += 1;
                    ordinals.push(proof.write_target_ordinal);
                    if !writer_result_proof_matches_finish(
                        proof,
                        &finish.input_schema,
                        &expected_values,
                    ) {
                        errors.push(ValidationError::new(
                            path,
                            "upstream writer result fields do not map exactly to its table finish input roles",
                        ));
                    }
                }
                NodeKind::SetOp {
                    kind: crate::SetOperationKind::UnionAll,
                    input_mappings,
                } => {
                    if node.output.columns.as_ref() != expected_values.as_ref()
                        || input_mappings.len() != node.inputs.len()
                        || input_mappings
                            .iter()
                            .any(|mapping| mapping.len() != expected_values.len())
                    {
                        errors.push(ValidationError::new(
                            path,
                            "writer UnionAll does not preserve the exact finish field occurrences",
                        ));
                        continue;
                    }
                    pending.extend(
                        node.inputs
                            .iter()
                            .copied()
                            .zip(input_mappings.iter().cloned()),
                    );
                }
                _ => errors.push(ValidationError::new(
                    path,
                    "table finish input contains a non-preserving writer relation node",
                )),
            }
        }
        ordinals.sort_unstable();
        if ordinals.as_slice() != finish.expected_target_ordinals.as_ref() {
            errors.push(ValidationError::new(
                path,
                "table finish expected targets differ from its fragment cut writer proofs",
            ));
        }
    }
    for cut in &cuts.inbound {
        if cut.writer_result.is_some() && consumed_inbound.get(&cut.edge).copied() != Some(1) {
            errors.push(ValidationError::new(
                path,
                "inbound writer result proof must feed exactly one local table finish",
            ));
        }
    }
    for writer in fragment
        .nodes()
        .values()
        .filter(|node| matches!(node.kind, NodeKind::TableWriter { .. }))
    {
        if consumed_writers.get(&writer.id).copied() != Some(1) {
            errors.push(ValidationError::new(
                path,
                "table writer must feed exactly one local finish or writer result stream",
            ));
        }
    }
}

pub(crate) fn validate_fragment_artifact_cuts(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationContext,
) {
    let mut supplied = BTreeMap::new();
    for artifact in &cuts.artifact_refs {
        if supplied.insert(artifact.id, artifact).is_some() {
            errors.push(ValidationError::new(
                path,
                "duplicate artifact reference in fragment cuts",
            ));
        }
        validate_artifact_ref(artifact, errors);
    }
    let requirements = fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Scan { relation, .. } => Some(relation.artifact_inputs()),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    let expected = requirements
        .iter()
        .map(|requirement| requirement.artifact)
        .collect::<BTreeSet<_>>();
    if supplied.keys().copied().collect::<BTreeSet<_>>() != expected {
        errors.push(ValidationError::new(
            path,
            "artifact references in fragment cuts differ from relation requirements",
        ));
    }
    for requirement in requirements {
        let Some(artifact) = supplied.get(&requirement.artifact) else {
            continue;
        };
        if artifact.kind != requirement.kind
            || artifact.format != requirement.format
            || artifact.schema != requirement.schema
            || artifact.source != requirement.source
            || artifact.coverage != requirement.required_coverage
        {
            errors.push(ValidationError::new(
                path,
                format!(
                    "artifact {} differs from the relation's exact input requirement",
                    requirement.artifact.get()
                ),
            ));
        }
    }
}

pub(crate) fn validate_fragment_runtime_filter_cuts(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationContext,
) {
    let mut lineage_indexes = RuntimeFilterLineageIndexes::default();
    let inbound_edges = cuts
        .inbound
        .iter()
        .map(|cut| cut.edge)
        .collect::<BTreeSet<_>>();
    let expected = fragment
        .runtime_filters()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if expected.len() != fragment.runtime_filters().len() {
        errors.push(ValidationError::new(
            path,
            "fragment has duplicate runtime-filter identities",
        ));
    }
    let supplied = cuts
        .runtime_filters
        .iter()
        .map(|filter| filter.id)
        .collect::<BTreeSet<_>>();
    if supplied.len() != cuts.runtime_filters.len() || supplied != expected {
        errors.push(ValidationError::new(
            path,
            "runtime filters in fragment cuts differ from fragment attachments",
        ));
    }
    validate_runtime_filter_proof_graph(fragment, cuts, path, errors);
    for filter in &cuts.runtime_filters {
        if !validate_runtime_filter_shape(filter, path, errors) {
            continue;
        }
        let witnesses = runtime_filter_witness_index(&filter.equality_witnesses);
        for witness in &filter.equality_witnesses {
            if witness.fragment == fragment.id() {
                validate_runtime_filter_equality_witness(
                    fragment,
                    witness,
                    &filter.domain,
                    path,
                    errors,
                );
            }
        }
        let mut local_endpoint_count = 0_usize;
        for producer in &filter.producers {
            if producer.endpoint.fragment == fragment.id() {
                local_endpoint_count += 1;
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &producer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &producer.endpoint,
                    producer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_producer_target(
                    fragment,
                    &witnesses,
                    producer,
                    &filter.domain,
                    filter.reduction,
                    path,
                    errors,
                );
                validate_runtime_filter_join_coverage(fragment, filter, producer, path, errors);
                validate_runtime_filter_producer_progress(
                    fragment,
                    producer,
                    &inbound_edges,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        for consumer in &filter.consumers {
            if consumer.endpoint.fragment == fragment.id() {
                local_endpoint_count += 1;
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &consumer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &consumer.endpoint,
                    consumer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_consumer_semantics(
                    fragment,
                    &witnesses,
                    &filter.producers,
                    &filter.domain,
                    consumer,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        if local_endpoint_count == 0 {
            errors.push(ValidationError::new(
                path,
                "attached runtime filter has no endpoint in this fragment",
            ));
        }
    }
}

pub(crate) fn validate_runtime_filter_join_coverage(
    fragment: &Fragment,
    filter: &crate::RuntimeFilter,
    producer: &crate::RuntimeFilterProducer,
    path: &str,
    errors: &mut ValidationContext,
) {
    if !matches!(
        producer.target,
        crate::RuntimeFilterProducerTarget::JoinBuildKey { .. }
    ) || producer.contribution_kinds.as_ref()
        != [
            crate::RuntimeFilterContributionKind::ValueDomainDelta,
            crate::RuntimeFilterContributionKind::ProducerClosed,
        ]
    {
        return;
    }
    let Some(node) = fragment.nodes().get(&producer.endpoint.node) else {
        return;
    };
    let NodeKind::HashJoin { distribution, .. } = node.kind else {
        return;
    };
    let matches_shape = |coverage: &crate::RuntimeFilterCoverage| match distribution {
        crate::JoinDistribution::BroadcastBuild => matches!(
            coverage.nodes.as_ref(),
            [crate::RuntimeFilterCoverageNode::Witness(witness), crate::RuntimeFilterCoverageNode::AnyOf { children }]
                if *witness == producer.witness && children.as_ref() == [0] && coverage.root == 1
        ),
        crate::JoinDistribution::Partitioned => matches!(
            coverage.nodes.as_ref(),
            [crate::RuntimeFilterCoverageNode::Witness(witness), crate::RuntimeFilterCoverageNode::AllOf { children }]
                if *witness == producer.witness && children.as_ref() == [0] && coverage.root == 1
        ),
        crate::JoinDistribution::Colocated | crate::JoinDistribution::Singleton => matches!(
            coverage.nodes.as_ref(),
            [crate::RuntimeFilterCoverageNode::Witness(witness)]
                if *witness == producer.witness && coverage.root == 0
        ),
    };
    if !matches_shape(&filter.availability_coverage) || !matches_shape(&filter.terminal_coverage) {
        errors.push(ValidationError::new(
            path,
            "runtime filter coverage shape differs from its exact join execution mode",
        ));
    }
}

pub(crate) fn validate_runtime_filter_proof_graph(
    local_fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationContext,
) {
    let mut lineage_indexes = RuntimeFilterLineageIndexes::default();
    let mut root_port_indexes = BTreeMap::new();
    let proof_fragments = cuts
        .runtime_filter_proof
        .fragments
        .iter()
        .map(|fragment| (fragment.id(), fragment.clone()))
        .collect::<BTreeMap<_, _>>();
    let proof_edges = cuts
        .runtime_filter_proof
        .edges
        .iter()
        .map(|edge| (edge.id, edge.clone()))
        .collect::<BTreeMap<_, _>>();
    let proof_filters = cuts
        .runtime_filter_proof
        .filters
        .iter()
        .map(|filter| (filter.id, filter.clone()))
        .collect::<BTreeMap<_, _>>();
    if proof_fragments.len() != cuts.runtime_filter_proof.fragments.len()
        || proof_edges.len() != cuts.runtime_filter_proof.edges.len()
        || proof_filters.len() != cuts.runtime_filter_proof.filters.len()
    {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph contains duplicate fragment, edge, or filter identities",
        ));
        return;
    }
    if cuts.runtime_filters.is_empty() {
        if !proof_fragments.is_empty() || !proof_edges.is_empty() || !proof_filters.is_empty() {
            errors.push(ValidationError::new(
                path,
                "runtime-filter proof graph is non-empty without an attached filter",
            ));
        }
        return;
    }

    let proof_plan = PhysicalPlan::from(crate::PhysicalPlanParts {
        version: crate::PlanVersionId::try_new([1; 16])
            .expect("the proof-only plan version is non-zero"),
        fragments: proof_fragments.clone(),
        edges: proof_edges.clone(),
        runtime_filters: proof_filters.clone(),
        result_port: None,
        artifact_refs: BTreeMap::new(),
        required: RequiredContracts::default(),
        annotations: Box::default(),
    });
    let attachments = runtime_filter_attachment_index(&proof_plan);
    let inbound_edges = runtime_filter_inbound_edge_index(&proof_plan);
    if cuts
        .runtime_filters
        .iter()
        .any(|filter| proof_filters.get(&filter.id) != Some(filter))
    {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph changes a locally attached filter contract",
        ));
        return;
    }
    let mut required_fragments = BTreeSet::new();
    let mut required_edges = BTreeSet::new();
    let mut build_dependency_cache = RuntimeFilterBuildDependencyCache::default();
    let mut proof_work_budget = SemanticTraceWorkBudget::new(errors.limits());
    let Some(required_filters) = extend_runtime_filter_proof_hull(
        &proof_plan,
        cuts.runtime_filters.iter().map(|filter| filter.id),
        &mut required_fragments,
        &mut required_edges,
        &mut build_dependency_cache,
        &mut proof_work_budget,
    ) else {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph omits a join-build execution dependency",
        ));
        return;
    };
    for edge in &required_edges {
        let Some(edge) = proof_edges.get(edge) else {
            continue;
        };
        required_fragments.extend([edge.source.fragment, edge.destination.fragment]);
    }
    if proof_fragments.keys().copied().collect::<BTreeSet<_>>() != required_fragments
        || proof_edges.keys().copied().collect::<BTreeSet<_>>() != required_edges
        || proof_filters.keys().copied().collect::<BTreeSet<_>>() != required_filters
        || proof_fragments.get(&local_fragment.id()) != Some(local_fragment)
    {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph differs from the exact referenced subgraph",
        ));
        return;
    }

    for fragment in proof_fragments.values() {
        validate_fragment_into(fragment, errors);
    }
    for edge in proof_plan.edges().values() {
        validate_edge(&proof_plan, edge, &mut root_port_indexes, errors);
    }
    validate_runtime_filter_proof_edge_source_sinks(&proof_plan, path, errors);
    for filter in proof_plan.runtime_filters().values() {
        if !validate_runtime_filter_shape(filter, path, errors) {
            continue;
        }
        let witnesses = runtime_filter_witness_index(&filter.equality_witnesses);
        for witness in &filter.equality_witnesses {
            match proof_plan.fragments().get(&witness.fragment) {
                Some(fragment) => validate_runtime_filter_equality_witness(
                    fragment,
                    witness,
                    &filter.domain,
                    path,
                    errors,
                ),
                None => errors.push(ValidationError::new(
                    path,
                    "runtime-filter proof graph omits an equality fragment",
                )),
            }
        }
        for producer in &filter.producers {
            validate_runtime_filter_attachment(
                &proof_plan,
                &attachments,
                filter.id,
                &producer.endpoint,
                path,
                errors,
            );
            if let Some(fragment) = proof_plan.fragments().get(&producer.endpoint.fragment) {
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &producer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &producer.endpoint,
                    producer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_producer_target(
                    fragment,
                    &witnesses,
                    producer,
                    &filter.domain,
                    filter.reduction,
                    path,
                    errors,
                );
                validate_runtime_filter_join_coverage(fragment, filter, producer, path, errors);
                validate_runtime_filter_producer_progress(
                    fragment,
                    producer,
                    inbound_edges
                        .get(&fragment.id())
                        .unwrap_or(&BTreeSet::new()),
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        for consumer in &filter.consumers {
            validate_runtime_filter_attachment(
                &proof_plan,
                &attachments,
                filter.id,
                &consumer.endpoint,
                path,
                errors,
            );
            if let Some(fragment) = proof_plan.fragments().get(&consumer.endpoint.fragment) {
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &consumer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &consumer.endpoint,
                    consumer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_consumer_semantics(
                    fragment,
                    &witnesses,
                    &filter.producers,
                    &filter.domain,
                    consumer,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
            validate_runtime_filter_consumer_lineage(
                &proof_plan,
                &witnesses,
                &filter.producers,
                consumer,
                path,
                &mut lineage_indexes,
                errors,
            );
        }
    }
    validate_runtime_filter_wait_graph(&proof_plan, path, errors);
}
