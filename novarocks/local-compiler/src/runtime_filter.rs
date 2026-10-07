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

//! Admit and lower one fragment's runtime-filter endpoints into their local
//! program sites.
//!
//! The package's binding table is the only source of binding identities: one
//! entry per local endpoint, plan-global and never recomputed here. The
//! channel identity of every site is its physical runtime filter, which is
//! also the `filter_id` a scan's frozen dynamic filter names.
//!
//! This milestone lowers exactly two endpoint shapes of a complete-once
//! membership (SetUnion) filter:
//!
//! - a hash join's build-key producer, which becomes a `Join` runtime-filter
//!   producer over its own `build_keys[k]`. The local build input is the
//!   physical build side, so the producer key is the existing `JoinBuildKey`
//!   root and gets no root of its own;
//! - a blocking scan-source consumer of a scan field, which becomes a `Scan`
//!   runtime-filter consumer keyed by a compiler-authored slot read of the
//!   scan's own output occurrence, rooted at `RuntimeFilter { binding }`
//!   where `binding` is the consumer's index among that scan's sites.
//!
//! Each site has exactly one `RuntimeFilter` binding requirement. Every other
//! domain, lifecycle, reduction, producer target, consumer target, apply point
//! and activation is refused by name, before any channel or expression exists.

use crate::{
    channels::{NodeChannels, ResolvedInput, UnionChannelSource},
    lowering::FragmentCompileError,
    union_flow::RuntimeFilterKeyRoot,
};
use novarocks_connector_contract::ConnectorReadProgramRecipe;
use novarocks_local_program::{
    BindingRequirement, FilterConsumerActivation, FilterConsumerAtExpr, FilterNullSemantics,
    FilterProducerKind, FilterReduction, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExprId, ProgramNodeId, StaticFilterConsumer, StaticFilterContract, StaticFilterProducer,
};
use novarocks_physical_plan::{
    FragmentPackage, NodeId, NodeKind, RuntimeFilter, RuntimeFilterApplyPoint,
    RuntimeFilterArtifactCapability, RuntimeFilterBindingCut, RuntimeFilterBindingRole,
    RuntimeFilterCompletion, RuntimeFilterConsumerActivation, RuntimeFilterConsumerTarget,
    RuntimeFilterContributionKind, RuntimeFilterDomain, RuntimeFilterLifecycle,
    RuntimeFilterNullSemantics, RuntimeFilterProducerTarget, RuntimeFilterReduction, ValueType,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::collections::{BTreeMap, BTreeSet};

/// One admitted hash-join build-key membership producer.
struct PlannedProducer {
    requirement: i32,
    key_ordinal: usize,
    producer: StaticFilterProducer,
}

/// One admitted blocking scan-source membership consumer of the scan output
/// occurrence `ordinal`, typed exactly as its membership key.
struct PlannedConsumer {
    requirement: i32,
    ordinal: usize,
    ty: ValueType,
    consumer: StaticFilterConsumer,
}

/// Every local runtime-filter endpoint of one fragment, admitted and keyed by
/// its physical node, each node's sites in binding-table order. Lowering takes
/// each node's sites exactly once.
pub(crate) struct PlannedRuntimeFilters {
    producers: BTreeMap<NodeId, Vec<PlannedProducer>>,
    consumers: BTreeMap<NodeId, Vec<PlannedConsumer>>,
}

/// The sites one lowered scan owns.
pub(crate) struct ScanFilterSites {
    pub filters: Vec<FilterConsumerAtExpr>,
    pub requirements: Vec<BindingRequirement>,
    pub roots: Vec<RuntimeFilterKeyRoot>,
}

/// The producers one lowered hash join owns: each build-key ordinal with its
/// static producer, and their requirements.
pub(crate) struct JoinFilterSites {
    pub producers: Vec<(usize, StaticFilterProducer)>,
    pub requirements: Vec<BindingRequirement>,
}

fn unsupported(node: NodeId, feature: &'static str) -> FragmentCompileError {
    FragmentCompileError::Unsupported {
        node: Some(node),
        feature,
    }
}

fn static_filter(error: impl std::error::Error + Send + Sync + 'static) -> FragmentCompileError {
    FragmentCompileError::Owner {
        phase: "runtime filter",
        error: Box::new(error),
    }
}

/// The complete-once membership contract every lowered site of `filter`
/// shares. `node` names the endpoint a refusal reports.
fn membership_contract(
    filter: &RuntimeFilter,
    node: NodeId,
) -> Result<(StaticFilterContract, FilterNullSemantics), FragmentCompileError> {
    let RuntimeFilterDomain::Membership { ty, null_semantics } = &filter.domain else {
        return Err(unsupported(node, "ordered-domain runtime filter"));
    };
    if filter.lifecycle != RuntimeFilterLifecycle::CompleteOnce {
        return Err(unsupported(
            node,
            "monotonic-update runtime-filter lifecycle",
        ));
    }
    if filter.reduction != RuntimeFilterReduction::SetUnion {
        return Err(unsupported(
            node,
            "runtime-filter reduction other than set union",
        ));
    }
    let null_semantics = match null_semantics {
        RuntimeFilterNullSemantics::NeverMatches => FilterNullSemantics::NeverMatches,
        RuntimeFilterNullSemantics::NullSafeEqual => FilterNullSemantics::NullSafeEqual,
    };
    let contract = StaticFilterContract::membership(&ty.data_type, null_semantics)
        .map_err(|_| unsupported(node, "runtime-filter membership key type"))?;
    Ok((contract, null_semantics))
}

fn contract_type(
    contract: &StaticFilterContract,
) -> Result<&arrow_schema::DataType, FragmentCompileError> {
    match contract {
        StaticFilterContract::Membership { data_type, .. } => Ok(data_type),
        StaticFilterContract::Ordered { .. } => Err(FragmentCompileError::Invalid(
            "membership runtime filter has an ordered contract",
        )),
    }
}

/// Admit every local endpoint the package's binding table names, and check
/// that each scan's frozen dynamic filters are exactly its admitted consumers.
pub(crate) fn plan_runtime_filters(
    package: &FragmentPackage,
    reads: &BTreeMap<NodeId, ConnectorReadProgramRecipe>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PlannedRuntimeFilters, FragmentCompileError> {
    let fragment = package.fragment();
    let cuts = package.cuts();
    let mut filters = BTreeMap::new();
    for filter in cuts.runtime_filters.iter() {
        filters.insert(filter.id, filter);
        work.step()?;
    }
    for id in fragment.runtime_filters() {
        let supplied = filters.contains_key(id);
        work.step()?;
        if !supplied {
            return Err(FragmentCompileError::Invalid(
                "attached runtime filter has no supplied contract",
            ));
        }
    }
    let mut planned = PlannedRuntimeFilters {
        producers: BTreeMap::new(),
        consumers: BTreeMap::new(),
    };
    let mut bound = BTreeSet::new();
    for binding in cuts.runtime_filter_bindings.iter() {
        work.step()?;
        let filter = *filters
            .get(&binding.filter)
            .ok_or(FragmentCompileError::Invalid(
                "runtime-filter binding names no supplied runtime filter",
            ))?;
        if !bound.insert(binding.binding_id) {
            return Err(FragmentCompileError::Invalid(
                "runtime-filter binding identity is not unique",
            ));
        }
        match binding.role {
            RuntimeFilterBindingRole::Producer(index) => {
                plan_producer(package, filter, binding, index, &mut planned, work)?
            }
            RuntimeFilterBindingRole::Consumer(index) => {
                plan_consumer(package, filter, binding, index, &mut planned, work)?
            }
        }
    }
    // A scan's frozen dynamic filters name exactly the filters its admitted
    // consumers read, each over the provider column its consumer keys.
    for node in fragment.nodes().values() {
        let NodeKind::Scan {
            provider_outputs, ..
        } = &node.kind
        else {
            continue;
        };
        work.step()?;
        let Some(recipe) = reads.get(&node.id) else {
            continue;
        };
        let scan = recipe.frozen().scan();
        let consumers = planned
            .consumers
            .get(&node.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if consumers.is_empty() && scan.dynamic_filters().is_empty() {
            continue;
        }
        let mut expected = BTreeMap::new();
        for consumer in consumers {
            let value = node.output.columns[consumer.ordinal];
            if expected
                .insert(consumer.consumer.channel_id(), value)
                .is_some_and(|other| other != value)
            {
                return Err(FragmentCompileError::Invalid(
                    "one runtime filter keys two scan columns",
                ));
            }
            work.step()?;
        }
        // Assignment `i` names provider output `i`.
        let mut assigned = BTreeMap::new();
        for (ordinal, assignment) in scan.assignments().iter().enumerate() {
            assigned.entry(assignment.variable()).or_insert(ordinal);
            work.step()?;
        }
        let mut actual = BTreeSet::new();
        for dynamic in scan.dynamic_filters() {
            let value = assigned
                .get(dynamic.variable())
                .and_then(|ordinal| provider_outputs.get(*ordinal))
                .map(|(_, value)| *value);
            work.step()?;
            if value.is_none() || expected.get(&dynamic.filter_id()) != value.as_ref() {
                return Err(FragmentCompileError::Invalid(
                    "scan dynamic filter binds no admitted runtime-filter consumer column",
                ));
            }
            actual.insert(dynamic.filter_id());
        }
        if actual.len() != expected.len() {
            return Err(FragmentCompileError::Invalid(
                "scan dynamic filters differ from its admitted runtime-filter consumers",
            ));
        }
    }
    Ok(planned)
}

fn requirement_id(
    binding: &RuntimeFilterBindingCut,
    node: NodeId,
) -> Result<i32, FragmentCompileError> {
    i32::try_from(binding.binding_id).map_err(|_| {
        unsupported(
            node,
            "runtime-filter binding identity outside the program requirement range",
        )
    })
}

fn plan_producer(
    package: &FragmentPackage,
    filter: &RuntimeFilter,
    binding: &RuntimeFilterBindingCut,
    index: usize,
    planned: &mut PlannedRuntimeFilters,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let fragment = package.fragment();
    let producer = filter
        .producers
        .get(index)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter binding names an absent producer",
        ))?;
    let node = producer.endpoint.node;
    if producer.endpoint.fragment != fragment.id() {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter binding names a remote producer",
        ));
    }
    let RuntimeFilterProducerTarget::JoinBuildKey { equality } = producer.target else {
        return Err(unsupported(node, "Aggregate TopN runtime-filter producer"));
    };
    let (contract, null_semantics) = membership_contract(filter, node)?;
    // A fenced final domain is published by a separate completion owner; only
    // the delta-and-close membership producer is lowered.
    if producer
        .contribution_kinds
        .contains(&RuntimeFilterContributionKind::FinalDomainShard)
        || producer.completion == RuntimeFilterCompletion::FencedCommittedDomain
    {
        return Err(unsupported(
            node,
            "fenced final-domain runtime-filter producer",
        ));
    }
    if producer.contribution_kinds.as_ref()
        != [
            RuntimeFilterContributionKind::ValueDomainDelta,
            RuntimeFilterContributionKind::ProducerClosed,
        ]
        || producer.completion != RuntimeFilterCompletion::ProducerClosed
    {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter producer contributions differ from a membership producer",
        ));
    }
    let witness = filter
        .equality_witnesses
        .iter()
        .find(|witness| witness.id == equality)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter producer names an absent equality witness",
        ))?;
    work.step()?;
    if witness.fragment != fragment.id() || witness.join != node {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter producer witness names another join",
        ));
    }
    let join = fragment
        .nodes()
        .get(&node)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter producer names an absent node",
        ))?;
    let NodeKind::HashJoin {
        keys, build_side, ..
    } = &join.kind
    else {
        return Err(unsupported(
            node,
            "runtime-filter producer outside a hash join",
        ));
    };
    // The local build input is the physical build side, so only a witness
    // whose domain is the build side names a local build key.
    if witness.domain_side != *build_side {
        return Err(unsupported(
            node,
            "runtime-filter witness domain side differs from the join build side",
        ));
    }
    if producer.apply_point
        != (RuntimeFilterApplyPoint::NodeInput {
            input_ordinal: build_side.input_ordinal(),
        })
    {
        return Err(unsupported(
            node,
            "runtime-filter producer apply point other than its join build input",
        ));
    }
    let key_ordinal =
        usize::try_from(witness.key_ordinal).map_err(|_| CompileControlError::ResourceExhausted)?;
    let key = keys.get(key_ordinal).ok_or(FragmentCompileError::Invalid(
        "runtime-filter witness names an absent join key",
    ))?;
    let build_key = match build_side {
        novarocks_physical_plan::JoinSide::Left => key.left,
        novarocks_physical_plan::JoinSide::Right => key.right,
    };
    let key_type = &fragment
        .expressions()
        .get(build_key)
        .ok_or(FragmentCompileError::Invalid("missing join key definition"))?
        .ty
        .data_type;
    if key_type != contract_type(&contract)? {
        return Err(unsupported(
            node,
            "runtime-filter build key type differs from its membership type",
        ));
    }
    let key_semantics = if key.null_safe {
        FilterNullSemantics::NullSafeEqual
    } else {
        FilterNullSemantics::NeverMatches
    };
    if key_semantics != null_semantics {
        return Err(unsupported(
            node,
            "runtime-filter NULL semantics differ from the join key equality",
        ));
    }
    let requirement = requirement_id(binding, node)?;
    let producer = StaticFilterProducer::try_new(
        binding.binding_id,
        filter.id.get(),
        FilterProducerKind::Membership,
        contract,
        FilterReduction::SetUnion,
    )
    .map_err(static_filter)?;
    planned
        .producers
        .entry(node)
        .or_default()
        .push(PlannedProducer {
            requirement,
            key_ordinal,
            producer,
        });
    work.step()?;
    Ok(())
}

fn plan_consumer(
    package: &FragmentPackage,
    filter: &RuntimeFilter,
    binding: &RuntimeFilterBindingCut,
    index: usize,
    planned: &mut PlannedRuntimeFilters,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let fragment = package.fragment();
    let consumer = filter
        .consumers
        .get(index)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter binding names an absent consumer",
        ))?;
    let node = consumer.endpoint.node;
    if consumer.endpoint.fragment != fragment.id() {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter binding names a remote consumer",
        ));
    }
    match consumer.target {
        RuntimeFilterConsumerTarget::ScanField { .. } => {}
        RuntimeFilterConsumerTarget::JoinProbeKey { .. } => {
            return Err(unsupported(node, "join probe-key runtime-filter consumer"));
        }
        RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => {
            return Err(unsupported(node, "Aggregate TopN runtime-filter consumer"));
        }
    }
    let (contract, _) = membership_contract(filter, node)?;
    if consumer.apply_point != RuntimeFilterApplyPoint::ScanSource {
        return Err(unsupported(
            node,
            "runtime-filter consumer at a node input or output",
        ));
    }
    match consumer.activation {
        RuntimeFilterConsumerActivation::BlockingSnapshot => {}
        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { .. } => {
            return Err(unsupported(
                node,
                "start-unfiltered runtime-filter consumer activation",
            ));
        }
        RuntimeFilterConsumerActivation::NonBlockingLive { .. } => {
            return Err(unsupported(
                node,
                "non-blocking live runtime-filter consumer activation",
            ));
        }
    }
    if consumer.capabilities.as_ref()
        != [
            RuntimeFilterArtifactCapability::Membership,
            RuntimeFilterArtifactCapability::EmptyDomain,
        ]
    {
        return Err(unsupported(
            node,
            "runtime-filter consumer capabilities other than membership and empty domain",
        ));
    }
    let scan = fragment
        .nodes()
        .get(&node)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter consumer names an absent node",
        ))?;
    if !matches!(scan.kind, NodeKind::Scan { .. }) {
        return Err(unsupported(
            node,
            "runtime-filter consumer outside a scan source",
        ));
    }
    let [value] = consumer.endpoint.values.as_ref() else {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter consumer endpoint is not one value",
        ));
    };
    let mut ordinal = None;
    for (index, output) in scan.output.columns.iter().enumerate() {
        work.step()?;
        if output == value {
            ordinal = Some(index);
            break;
        }
    }
    let ordinal = ordinal
        .ok_or_else(|| unsupported(node, "runtime-filter consumer value is not a scan output"))?;
    let ty = fragment
        .values()
        .get(value)
        .ok_or(FragmentCompileError::Invalid(
            "missing runtime-filter consumer value",
        ))?
        .ty
        .clone();
    if &ty.data_type != contract_type(&contract)? {
        return Err(unsupported(
            node,
            "runtime-filter scan key type differs from its membership type",
        ));
    }
    let requirement = requirement_id(binding, node)?;
    let consumer = StaticFilterConsumer::try_new(
        binding.binding_id,
        filter.id.get(),
        FilterConsumerActivation::BlockingSnapshot,
        contract,
        FilterReduction::SetUnion,
    )
    .map_err(static_filter)?;
    planned
        .consumers
        .entry(node)
        .or_default()
        .push(PlannedConsumer {
            requirement,
            ordinal,
            ty,
            consumer,
        });
    work.step()?;
    Ok(())
}

impl PlannedRuntimeFilters {
    /// The number of compiler-authored consumer key definitions and roots.
    pub(crate) fn consumer_count(&self) -> Result<usize, FragmentCompileError> {
        let mut count = 0usize;
        for consumers in self.consumers.values() {
            count = count
                .checked_add(consumers.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        Ok(count)
    }

    /// Every scan consumer's key read: the planned slot and channel of the
    /// scan output occurrence it keys, typed exactly as that occurrence.
    pub(crate) fn consumer_keys(
        &self,
        nodes: &BTreeMap<NodeId, NodeChannels>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<BTreeMap<NodeId, Vec<UnionChannelSource>>, FragmentCompileError> {
        let mut keys = BTreeMap::new();
        for (&scan, consumers) in &self.consumers {
            let planned = nodes.get(&scan).ok_or(FragmentCompileError::Invalid(
                "runtime-filter consumer scan has no planned channels",
            ))?;
            let mut reads = Vec::new();
            crate::assert_rows::reserve_vec(&mut reads, consumers.len(), work)?;
            for consumer in consumers {
                let slot =
                    *planned
                        .slots
                        .get(consumer.ordinal)
                        .ok_or(FragmentCompileError::Invalid(
                            "runtime-filter consumer occurrence has no slot",
                        ))?;
                reads.push(UnionChannelSource {
                    input: ResolvedInput {
                        slot,
                        source: ProgramChannelSite::Layout {
                            node: planned.local,
                            role: ProgramChannelLayoutRole::NodeOutput,
                            ordinal: u32::try_from(consumer.ordinal)
                                .map_err(|_| CompileControlError::ResourceExhausted)?,
                        },
                    },
                    ty: consumer.ty.clone(),
                });
                work.step()?;
            }
            keys.insert(scan, reads);
        }
        Ok(keys)
    }

    /// Take one lowered scan's consumer sites. `definitions` are its key
    /// reads, as expression lowering authored them in binding order.
    pub(crate) fn take_scan(
        &mut self,
        scan: NodeId,
        local: ProgramNodeId,
        definitions: Option<&Vec<ProgramExprId>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ScanFilterSites, FragmentCompileError> {
        let consumers = self.consumers.remove(&scan).unwrap_or_default();
        let definitions = definitions.map(Vec::as_slice).unwrap_or_default();
        if definitions.len() != consumers.len() {
            return Err(FragmentCompileError::Invalid(
                "runtime-filter consumer keys differ from their scan's consumers",
            ));
        }
        let mut sites = ScanFilterSites {
            filters: Vec::new(),
            requirements: Vec::new(),
            roots: Vec::new(),
        };
        crate::assert_rows::reserve_vec(&mut sites.filters, consumers.len(), work)?;
        crate::assert_rows::reserve_vec(&mut sites.requirements, consumers.len(), work)?;
        crate::assert_rows::reserve_vec(&mut sites.roots, consumers.len(), work)?;
        for (binding, (consumer, &definition)) in consumers.into_iter().zip(definitions).enumerate()
        {
            sites.roots.push(RuntimeFilterKeyRoot {
                node: local,
                binding: u32::try_from(binding)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                definition,
                source: ProgramChannelSite::Layout {
                    node: local,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: u32::try_from(consumer.ordinal)
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                },
            });
            sites.requirements.push(BindingRequirement::RuntimeFilter {
                binding_id: consumer.requirement,
            });
            sites.filters.push(FilterConsumerAtExpr {
                expr_id: definition,
                consumer: consumer.consumer,
            });
            work.step()?;
        }
        Ok(sites)
    }

    /// Take one lowered hash join's producer sites.
    pub(crate) fn take_join(
        &mut self,
        join: NodeId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<JoinFilterSites, FragmentCompileError> {
        let planned = self.producers.remove(&join).unwrap_or_default();
        let mut sites = JoinFilterSites {
            producers: Vec::new(),
            requirements: Vec::new(),
        };
        crate::assert_rows::reserve_vec(&mut sites.producers, planned.len(), work)?;
        crate::assert_rows::reserve_vec(&mut sites.requirements, planned.len(), work)?;
        for producer in planned {
            sites.requirements.push(BindingRequirement::RuntimeFilter {
                binding_id: producer.requirement,
            });
            sites
                .producers
                .push((producer.key_ordinal, producer.producer));
            work.step()?;
        }
        Ok(sites)
    }

    /// Every admitted endpoint was taken by exactly its lowered owner.
    pub(crate) fn finish(&self) -> Result<(), FragmentCompileError> {
        if !self.producers.is_empty() || !self.consumers.is_empty() {
            return Err(FragmentCompileError::Invalid(
                "runtime-filter endpoint has no lowered site",
            ));
        }
        Ok(())
    }
}
