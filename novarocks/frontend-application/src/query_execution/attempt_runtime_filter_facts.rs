// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! What deploying one plan's runtime filters reads about it.
//!
//! Three tables: the channels an artifact can be built for, where each
//! binding sits and what role it plays there, and which exchange edges must
//! close before a producer is done. Both plan representations state them, so
//! deployment stops depending on which one built the execution.

use std::collections::BTreeMap;

use novarocks_physical_plan::{
    EdgeId, FragmentId as PhysicalFragmentId, PhysicalPlan, RuntimeFilter,
    RuntimeFilterApplyPoint as PhysicalApplyPoint, RuntimeFilterCoverage,
    RuntimeFilterCoverageNode, RuntimeFilterProducer,
    RuntimeFilterProducerTarget as PhysicalProducerTarget,
};
use novarocks_plan_codec::{
    PhysicalV1RuntimeFilterBindingRole, physical_v1_runtime_filter_bindings,
    physical_v1_runtime_filter_comparator_digest,
};
use novarocks_sql::planning::query_execution::{
    SqlPreparedRuntimeFilterFacts, SqlRuntimeFilterApplyPoint, SqlRuntimeFilterArtifactCapability,
    SqlRuntimeFilterBindingFacts, SqlRuntimeFilterBindingRoleFacts, SqlRuntimeFilterChannelFacts,
    SqlRuntimeFilterCompletionRequirement, SqlRuntimeFilterConsumerActivation,
    SqlRuntimeFilterConsumerTarget, SqlRuntimeFilterContributionKind,
    SqlRuntimeFilterCoverageFacts, SqlRuntimeFilterDeploymentBindingFacts,
    SqlRuntimeFilterFrontierEdgeFacts, SqlRuntimeFilterJoinProgressFacts,
    SqlRuntimeFilterLateApplyGranularity, SqlRuntimeFilterLifecycleFacts,
    SqlRuntimeFilterLogicalDomainFacts, SqlRuntimeFilterNullOrder, SqlRuntimeFilterNullSemantics,
    SqlRuntimeFilterOrderKeyFacts, SqlRuntimeFilterPolicyFacts, SqlRuntimeFilterProducerTarget,
    SqlRuntimeFilterReductionFacts, SqlRuntimeFilterSortDirection,
};

/// One plan's runtime filters, as the attempt that deploys them reads them.
#[derive(Clone, Debug, Default)]
pub(crate) struct AttemptRuntimeFilterFacts {
    bindings: BTreeMap<u32, Vec<SqlRuntimeFilterBindingFacts>>,
    channels: Vec<SqlRuntimeFilterChannelFacts>,
    deployment_bindings: Vec<SqlRuntimeFilterDeploymentBindingFacts>,
    join_progress: Vec<SqlRuntimeFilterJoinProgressFacts>,
}

impl AttemptRuntimeFilterFacts {
    pub(crate) fn from_prepared(prepared: &SqlPreparedRuntimeFilterFacts) -> Self {
        let mut bindings = BTreeMap::<u32, Vec<SqlRuntimeFilterBindingFacts>>::new();
        for binding in prepared.deployment_bindings() {
            bindings.entry(binding.fragment_id).or_default();
        }
        for fragment_id in bindings.keys().copied().collect::<Vec<_>>() {
            bindings.insert(
                fragment_id,
                prepared.bindings_for_fragment(fragment_id).to_vec(),
            );
        }
        Self {
            bindings,
            channels: prepared.channels().to_vec(),
            deployment_bindings: prepared.deployment_bindings().to_vec(),
            join_progress: prepared.join_progress().to_vec(),
        }
    }

    /// The same facts, from a completed plan's own runtime-filter contract.
    ///
    /// The per-fragment binding table is deliberately absent: a completed
    /// plan's fragments already carry their encoded bindings, written by the
    /// same numbering this projection joins on, so nothing needs to encode
    /// them a second time from these facts.
    pub(crate) fn from_completed(plan: &PhysicalPlan) -> Result<Self, String> {
        let mut channels = Vec::with_capacity(plan.runtime_filters().len());
        for filter in plan.runtime_filters().values() {
            channels.push(completed_channel(filter)?);
        }
        let mut deployment_bindings = Vec::new();
        let mut join_progress = Vec::new();
        for binding in physical_v1_runtime_filter_bindings(plan)? {
            let filter = &plan.runtime_filters()[&binding.filter];
            let (role, witness) = match binding.role {
                PhysicalV1RuntimeFilterBindingRole::Producer(index) => {
                    let producer = &filter.producers[index];
                    join_progress.push(completed_join_progress(
                        plan,
                        filter,
                        producer,
                        binding.binding_id,
                        binding.fragment,
                    )?);
                    (
                        completed_producer_role(filter, producer)?,
                        Some(producer.witness.get()),
                    )
                }
                PhysicalV1RuntimeFilterBindingRole::Consumer(index) => {
                    (completed_consumer_role(&filter.consumers[index])?, None)
                }
            };
            deployment_bindings.push(SqlRuntimeFilterDeploymentBindingFacts {
                binding_id: binding.binding_id,
                channel_id: filter.id.get(),
                fragment_id: binding.fragment.get(),
                node_id: wire_node_id(binding.node.get())?,
                coverage_witness_id: witness,
                role,
            });
        }
        Ok(Self {
            bindings: BTreeMap::new(),
            channels,
            deployment_bindings,
            join_progress,
        })
    }

    pub(crate) fn bindings_for_fragment(
        &self,
        fragment_id: u32,
    ) -> &[SqlRuntimeFilterBindingFacts] {
        self.bindings
            .get(&fragment_id)
            .map_or(&[][..], Vec::as_slice)
    }

    pub(crate) fn channels(&self) -> &[SqlRuntimeFilterChannelFacts] {
        &self.channels
    }

    pub(crate) fn deployment_bindings(&self) -> &[SqlRuntimeFilterDeploymentBindingFacts] {
        &self.deployment_bindings
    }

    pub(crate) fn join_progress(&self) -> &[SqlRuntimeFilterJoinProgressFacts] {
        &self.join_progress
    }

    pub(crate) fn has_channels(&self) -> bool {
        !self.channels.is_empty()
    }
}

fn wire_node_id(node: u32) -> Result<i32, String> {
    i32::try_from(node).map_err(|_| format!("runtime filter node {node} exceeds the wire identity"))
}

fn completed_channel(filter: &RuntimeFilter) -> Result<SqlRuntimeFilterChannelFacts, String> {
    use novarocks_physical_plan::{
        RuntimeFilterDomain, RuntimeFilterLifecycle, RuntimeFilterNullSemantics,
    };

    let logical_domain = match &filter.domain {
        RuntimeFilterDomain::Membership { ty, null_semantics } => {
            SqlRuntimeFilterLogicalDomainFacts::Membership {
                value_type: ty.data_type.clone(),
                null_semantics: match null_semantics {
                    RuntimeFilterNullSemantics::NeverMatches => {
                        SqlRuntimeFilterNullSemantics::NeverMatches
                    }
                    RuntimeFilterNullSemantics::NullSafeEqual => {
                        SqlRuntimeFilterNullSemantics::NullSafeEqual
                    }
                },
            }
        }
        RuntimeFilterDomain::Ordered { key, inclusive, .. } => {
            let comparator_digest = physical_v1_runtime_filter_comparator_digest(filter)?
                .ok_or_else(|| {
                    format!(
                        "ordered runtime filter {} has no comparator digest",
                        filter.id.get()
                    )
                })?;
            SqlRuntimeFilterLogicalDomainFacts::Ordered {
                keys: vec![SqlRuntimeFilterOrderKeyFacts {
                    data_type: key.ty.data_type.clone(),
                    direction: match key.direction {
                        novarocks_physical_plan::SortDirection::Ascending => {
                            SqlRuntimeFilterSortDirection::Ascending
                        }
                        novarocks_physical_plan::SortDirection::Descending => {
                            SqlRuntimeFilterSortDirection::Descending
                        }
                    },
                    null_order: match key.null_ordering {
                        novarocks_physical_plan::NullOrdering::First => {
                            SqlRuntimeFilterNullOrder::First
                        }
                        novarocks_physical_plan::NullOrdering::Last => {
                            SqlRuntimeFilterNullOrder::Last
                        }
                    },
                }],
                inclusive: *inclusive,
                comparator_digest,
            }
        }
    };
    Ok(SqlRuntimeFilterChannelFacts {
        channel_id: filter.id.get(),
        logical_domain,
        lifecycle: match filter.lifecycle {
            RuntimeFilterLifecycle::CompleteOnce => SqlRuntimeFilterLifecycleFacts::CompleteOnce,
            RuntimeFilterLifecycle::MonotonicUpdates => {
                SqlRuntimeFilterLifecycleFacts::MonotonicUpdates
            }
        },
        availability_coverage: coverage_tree(&filter.availability_coverage)?,
        terminal_coverage: coverage_tree(&filter.terminal_coverage)?,
        reduction: completed_reduction(filter)?,
        allowed_contribution_kinds: completed_contribution_kinds(filter),
        required_consumer_capabilities: completed_consumer_capabilities(filter),
        policy: SqlRuntimeFilterPolicyFacts {
            max_contribution_bytes: filter.policy.max_contribution_bytes,
            max_artifact_bytes: filter.policy.max_artifact_bytes,
            deadline_ms: filter.policy.deadline_ms,
            max_retries: filter.policy.max_retries,
        },
    })
}

/// Every contribution kind some producer of this filter may send, once each.
///
/// The channel states what it accepts; each producer states what it sends.
/// A completed plan says the second, so the first is their union -- listed in
/// one fixed order so two plans with the same producers describe the same
/// channel.
fn completed_contribution_kinds(filter: &RuntimeFilter) -> Vec<SqlRuntimeFilterContributionKind> {
    use novarocks_physical_plan::RuntimeFilterContributionKind as Physical;

    let mut seen = [false; 5];
    for producer in &filter.producers {
        for kind in &producer.contribution_kinds {
            seen[match kind {
                Physical::ValueDomainDelta => 0,
                Physical::FinalDomainShard => 1,
                Physical::OrderedBoundUpdate => 2,
                // The plan names the shape of what a TopN producer sends; the
                // runtime that receives it has always called that a top-k
                // summary.
                Physical::FinalOrderedHullShard => 3,
                Physical::ProducerClosed => 4,
            }] = true;
        }
    }
    [
        SqlRuntimeFilterContributionKind::ValueDomainDelta,
        SqlRuntimeFilterContributionKind::FinalDomainShard,
        SqlRuntimeFilterContributionKind::OrderedBoundUpdate,
        SqlRuntimeFilterContributionKind::TopKSummary,
        SqlRuntimeFilterContributionKind::ProducerClosed,
    ]
    .into_iter()
    .enumerate()
    .filter_map(|(index, kind)| seen[index].then_some(kind))
    .collect()
}

/// Every artifact capability some consumer of this filter needs, once each.
fn completed_consumer_capabilities(
    filter: &RuntimeFilter,
) -> Vec<SqlRuntimeFilterArtifactCapability> {
    use novarocks_physical_plan::RuntimeFilterArtifactCapability as Physical;

    let mut seen = [false; 3];
    for consumer in &filter.consumers {
        for capability in &consumer.capabilities {
            seen[match capability {
                Physical::Membership => 0,
                Physical::OrderedRange => 1,
                Physical::EmptyDomain => 2,
            }] = true;
        }
    }
    [
        SqlRuntimeFilterArtifactCapability::Membership,
        SqlRuntimeFilterArtifactCapability::OrderedRange,
        SqlRuntimeFilterArtifactCapability::EmptyDomain,
    ]
    .into_iter()
    .enumerate()
    .filter_map(|(index, capability)| seen[index].then_some(capability))
    .collect()
}

/// How contributions to this filter are combined.
///
/// An ordered hull is the shape a TopN producer contributes, and the runtime
/// merges it as a top-k summary whose k is that producer's own limit. The
/// plan states the shape and the limit separately rather than repeating the
/// limit in the reduction, so it is read back from the producer here -- the
/// same derivation the wire encoder makes.
fn completed_reduction(filter: &RuntimeFilter) -> Result<SqlRuntimeFilterReductionFacts, String> {
    use novarocks_physical_plan::RuntimeFilterReduction as Physical;

    Ok(match filter.reduction {
        Physical::SetUnion => SqlRuntimeFilterReductionFacts::SetUnion,
        Physical::TightenOrderedBound => SqlRuntimeFilterReductionFacts::TightenOrderedBound,
        Physical::UnionOrderedHull => {
            let limit = filter
                .producers
                .iter()
                .find_map(|producer| match producer.target {
                    PhysicalProducerTarget::AggregateTopNKey { limit, .. } => Some(limit),
                    PhysicalProducerTarget::JoinBuildKey { .. } => None,
                })
                .ok_or_else(|| {
                    format!(
                        "ordered-hull runtime filter {} has no Aggregate TopN producer",
                        filter.id.get()
                    )
                })?;
            let k = u32::try_from(limit).map_err(|_| {
                format!(
                    "runtime filter {} top-k limit exceeds the deployed identity",
                    filter.id.get()
                )
            })?;
            SqlRuntimeFilterReductionFacts::MergeTopKSummary { k }
        }
    })
}

/// Expand one coverage arena into the tree its consumer reads.
///
/// The arena is bounded and non-recursive, and every composite child precedes
/// its parent, so this walk terminates without a visited set.
fn coverage_tree(
    coverage: &RuntimeFilterCoverage,
) -> Result<SqlRuntimeFilterCoverageFacts, String> {
    fn expand(
        coverage: &RuntimeFilterCoverage,
        index: u32,
    ) -> Result<SqlRuntimeFilterCoverageFacts, String> {
        let node = coverage
            .nodes
            .get(index as usize)
            .ok_or_else(|| format!("runtime filter coverage names absent node {index}"))?;
        Ok(match node {
            RuntimeFilterCoverageNode::Witness(witness) => {
                SqlRuntimeFilterCoverageFacts::LeafWitnessId(witness.get())
            }
            RuntimeFilterCoverageNode::AllOf { children } => SqlRuntimeFilterCoverageFacts::AllOf(
                children
                    .iter()
                    .map(|child| {
                        if *child >= index {
                            return Err(format!(
                                "runtime filter coverage node {index} names child {child} that does not precede it"
                            ));
                        }
                        expand(coverage, *child)
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            ),
            RuntimeFilterCoverageNode::AnyOf { children } => SqlRuntimeFilterCoverageFacts::AnyOf(
                children
                    .iter()
                    .map(|child| {
                        if *child >= index {
                            return Err(format!(
                                "runtime filter coverage node {index} names child {child} that does not precede it"
                            ));
                        }
                        expand(coverage, *child)
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            ),
        })
    }

    expand(coverage, coverage.root)
}

fn apply_point(point: PhysicalApplyPoint) -> SqlRuntimeFilterApplyPoint {
    match point {
        // A scan-source filter is applied to what the scan produces, which is
        // the node's output; the provider-side pruning it also enables is
        // named by the scan's own binding rather than by this apply point.
        PhysicalApplyPoint::NodeOutput | PhysicalApplyPoint::ScanSource => {
            SqlRuntimeFilterApplyPoint::NodeOutput
        }
        PhysicalApplyPoint::NodeInput { .. } => SqlRuntimeFilterApplyPoint::NodeInput,
    }
}

fn completed_producer_role(
    filter: &RuntimeFilter,
    producer: &RuntimeFilterProducer,
) -> Result<SqlRuntimeFilterBindingRoleFacts, String> {
    use novarocks_physical_plan::RuntimeFilterCompletion;

    let target = match producer.target {
        PhysicalProducerTarget::JoinBuildKey { equality } => {
            let witness = filter
                .equality_witnesses
                .iter()
                .find(|candidate| candidate.id == equality)
                .ok_or_else(|| {
                    format!(
                        "runtime filter {} producer names absent equality witness {}",
                        filter.id.get(),
                        equality.get()
                    )
                })?;
            SqlRuntimeFilterProducerTarget::JoinBuildKey {
                ordinal: witness.key_ordinal,
            }
        }
        PhysicalProducerTarget::AggregateTopNKey {
            group_key_ordinal,
            limit,
            ..
        } => SqlRuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal,
            limit: u32::try_from(limit).map_err(|_| {
                format!(
                    "runtime filter {} top-k limit exceeds the deployed identity",
                    filter.id.get()
                )
            })?,
        },
    };
    Ok(SqlRuntimeFilterBindingRoleFacts::Producer {
        contribution_kinds: completed_contribution_kinds_of(producer),
        completion_requirement: match producer.completion {
            RuntimeFilterCompletion::ProducerClosed => {
                SqlRuntimeFilterCompletionRequirement::ProducerClosed
            }
            RuntimeFilterCompletion::FencedCommittedDomain => {
                SqlRuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen
            }
        },
        target,
    })
}

fn completed_contribution_kinds_of(
    producer: &RuntimeFilterProducer,
) -> Vec<SqlRuntimeFilterContributionKind> {
    use novarocks_physical_plan::RuntimeFilterContributionKind as Physical;

    producer
        .contribution_kinds
        .iter()
        .map(|kind| match kind {
            Physical::ValueDomainDelta => SqlRuntimeFilterContributionKind::ValueDomainDelta,
            Physical::FinalDomainShard => SqlRuntimeFilterContributionKind::FinalDomainShard,
            Physical::OrderedBoundUpdate => SqlRuntimeFilterContributionKind::OrderedBoundUpdate,
            Physical::FinalOrderedHullShard => SqlRuntimeFilterContributionKind::TopKSummary,
            Physical::ProducerClosed => SqlRuntimeFilterContributionKind::ProducerClosed,
        })
        .collect()
}

fn completed_consumer_role(
    consumer: &novarocks_physical_plan::RuntimeFilterConsumer,
) -> Result<SqlRuntimeFilterBindingRoleFacts, String> {
    use novarocks_physical_plan::{
        LateApplyGranularity, RuntimeFilterArtifactCapability as PhysicalCapability,
        RuntimeFilterConsumerActivation as PhysicalActivation,
        RuntimeFilterConsumerTarget as PhysicalConsumerTarget,
    };

    let activation = match consumer.activation {
        PhysicalActivation::BlockingSnapshot => {
            SqlRuntimeFilterConsumerActivation::BlockingSnapshot
        }
        PhysicalActivation::NonBlockingLive { late_apply }
        | PhysicalActivation::StartUnfilteredThenApplyComplete { late_apply } => {
            SqlRuntimeFilterConsumerActivation::NonBlockingLive(match late_apply {
                LateApplyGranularity::Row => SqlRuntimeFilterLateApplyGranularity::Row,
                LateApplyGranularity::Batch => SqlRuntimeFilterLateApplyGranularity::Batch,
                LateApplyGranularity::RowGroup => SqlRuntimeFilterLateApplyGranularity::RowGroup,
                LateApplyGranularity::Split => SqlRuntimeFilterLateApplyGranularity::Split,
                LateApplyGranularity::File => SqlRuntimeFilterLateApplyGranularity::File,
            })
        }
    };
    let target = match &consumer.target {
        PhysicalConsumerTarget::JoinProbeKey { .. } => {
            SqlRuntimeFilterConsumerTarget::DirectInput {
                input_ordinal: match consumer.apply_point {
                    PhysicalApplyPoint::NodeInput { input_ordinal } => input_ordinal,
                    PhysicalApplyPoint::NodeOutput | PhysicalApplyPoint::ScanSource => {
                        return Err(
                            "a join probe runtime filter applies to a named join input".to_string()
                        );
                    }
                },
            }
        }
        PhysicalConsumerTarget::ScanField { .. }
        | PhysicalConsumerTarget::AggregateTopNScanField { .. } => {
            SqlRuntimeFilterConsumerTarget::SourceBoundary { scan_domain: None }
        }
    };
    Ok(SqlRuntimeFilterBindingRoleFacts::Consumer {
        capabilities: consumer
            .capabilities
            .iter()
            .map(|capability| match capability {
                PhysicalCapability::Membership => SqlRuntimeFilterArtifactCapability::Membership,
                PhysicalCapability::OrderedRange => {
                    SqlRuntimeFilterArtifactCapability::OrderedRange
                }
                PhysicalCapability::EmptyDomain => SqlRuntimeFilterArtifactCapability::EmptyDomain,
            })
            .collect(),
        activation,
        target,
    })
}

/// Which exchange edges must close before this producer is done.
///
/// The plan names them as edge identities; deployment names them the way it
/// watches them, by the fragment that sends and the exchange node that
/// receives.
fn completed_join_progress(
    plan: &PhysicalPlan,
    filter: &RuntimeFilter,
    producer: &RuntimeFilterProducer,
    binding_id: u32,
    fragment: PhysicalFragmentId,
) -> Result<SqlRuntimeFilterJoinProgressFacts, String> {
    Ok(SqlRuntimeFilterJoinProgressFacts::Proven {
        channel_id: filter.id.get(),
        producer_binding_id: binding_id,
        producer_fragment_id: fragment.get(),
        join_node_id: wire_node_id(producer.endpoint.node.get())?,
        build_frontier: frontier_edges(plan, &producer.progress.build_edges)?,
        non_build_inputs: frontier_edges(plan, &producer.progress.non_build_edges)?,
    })
}

fn frontier_edges(
    plan: &PhysicalPlan,
    edges: &[EdgeId],
) -> Result<Vec<SqlRuntimeFilterFrontierEdgeFacts>, String> {
    edges
        .iter()
        .map(|edge_id| {
            let edge = plan
                .edges()
                .get(edge_id)
                .ok_or_else(|| format!("runtime filter names absent edge {}", edge_id.get()))?;
            Ok(SqlRuntimeFilterFrontierEdgeFacts {
                source_fragment_id: edge.source.fragment.get(),
                target_exchange_node_id: wire_node_id(edge.destination.node.get())?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        JoinSide, NodeId, RuntimeFilterCompletion, RuntimeFilterConsumer,
        RuntimeFilterContributionKind as PhysicalContributionKind, RuntimeFilterDomain,
        RuntimeFilterEndpoint, RuntimeFilterEqualityWitness, RuntimeFilterEqualityWitnessId,
        RuntimeFilterId, RuntimeFilterKind, RuntimeFilterLifecycle, RuntimeFilterNullSemantics,
        RuntimeFilterPolicy, RuntimeFilterProducerProgress, RuntimeFilterReduction,
        RuntimeFilterWitnessId, TopNPhase, ValueId, ValueType,
    };

    use super::*;

    fn witness_coverage(witness: u32) -> RuntimeFilterCoverage {
        RuntimeFilterCoverage {
            nodes: Box::from([RuntimeFilterCoverageNode::Witness(
                RuntimeFilterWitnessId::new(witness),
            )]),
            root: 0,
        }
    }

    fn join_build_producer() -> RuntimeFilterProducer {
        RuntimeFilterProducer {
            witness: RuntimeFilterWitnessId::new(1),
            endpoint: RuntimeFilterEndpoint {
                fragment: PhysicalFragmentId::new(1),
                node: NodeId::new(7),
                values: Box::from([ValueId::new(1)]),
            },
            apply_point: PhysicalApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                PhysicalContributionKind::FinalDomainShard,
                PhysicalContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::FencedCommittedDomain,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::from([]),
                non_build_edges: Box::from([]),
            },
            target: PhysicalProducerTarget::JoinBuildKey {
                equality: RuntimeFilterEqualityWitnessId::new(1),
            },
        }
    }

    fn membership_filter(producers: Vec<RuntimeFilterProducer>) -> RuntimeFilter {
        RuntimeFilter {
            id: RuntimeFilterId::new(3),
            kind: RuntimeFilterKind::InList,
            domain: RuntimeFilterDomain::Membership {
                ty: ValueType::new(DataType::Int64, false),
                null_semantics: RuntimeFilterNullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            reduction: RuntimeFilterReduction::SetUnion,
            availability_coverage: witness_coverage(1),
            terminal_coverage: witness_coverage(1),
            equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
                id: RuntimeFilterEqualityWitnessId::new(1),
                fragment: PhysicalFragmentId::new(1),
                join: NodeId::new(7),
                key_ordinal: 2,
                domain_side: JoinSide::Right,
            }]),
            producers: producers.into_boxed_slice(),
            consumers: Box::from([]),
            policy: RuntimeFilterPolicy {
                max_contribution_bytes: 1 << 20,
                max_artifact_bytes: 1 << 22,
                deadline_ms: 250,
                max_retries: 2,
            },
        }
    }

    /// The plan states the shape of an ordered-hull reduction and the TopN
    /// producer's limit separately. Deployment needs both as one value, and
    /// reads the limit from the producer rather than from a second copy.
    #[test]
    fn an_ordered_hull_reduction_takes_its_k_from_its_top_n_producer() {
        let mut producer = join_build_producer();
        producer.target = PhysicalProducerTarget::AggregateTopNKey {
            group_key_ordinal: 0,
            topn: NodeId::new(9),
            phase: TopNPhase::Single,
            order_key_ordinal: 0,
            limit: 25,
            offset: 0,
            direction: novarocks_physical_plan::SortDirection::Ascending,
            null_ordering: novarocks_physical_plan::NullOrdering::First,
        };
        let mut filter = membership_filter(vec![producer]);
        filter.reduction = RuntimeFilterReduction::UnionOrderedHull;

        let reduction = completed_reduction(&filter).expect("a top-n producer states the limit");
        assert!(matches!(
            reduction,
            SqlRuntimeFilterReductionFacts::MergeTopKSummary { k: 25 }
        ));
    }

    /// Without a TopN producer there is no k, and inventing one would deploy
    /// a summary the runtime would merge against the wrong bound.
    #[test]
    fn an_ordered_hull_reduction_without_a_top_n_producer_is_refused() {
        let mut filter = membership_filter(vec![join_build_producer()]);
        filter.reduction = RuntimeFilterReduction::UnionOrderedHull;

        let error = completed_reduction(&filter).expect_err("no producer states a limit");
        assert!(error.contains("no Aggregate TopN producer"), "{error}");
    }

    /// The channel accepts the union of what its producers send, once each
    /// and in one fixed order, so two plans with the same producers describe
    /// the same channel.
    #[test]
    fn a_channel_accepts_each_contribution_kind_its_producers_send_once() {
        let mut second = join_build_producer();
        second.contribution_kinds = Box::from([
            PhysicalContributionKind::ProducerClosed,
            PhysicalContributionKind::ValueDomainDelta,
            PhysicalContributionKind::FinalOrderedHullShard,
        ]);
        let filter = membership_filter(vec![join_build_producer(), second]);

        let kinds = completed_contribution_kinds(&filter)
            .into_iter()
            .map(|kind| format!("{kind:?}"))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "ValueDomainDelta".to_string(),
                "FinalDomainShard".to_string(),
                "TopKSummary".to_string(),
                "ProducerClosed".to_string(),
            ],
            "an ordered-hull shard is what the runtime calls a top-k summary"
        );
    }

    /// A join build key names an equality witness; deployment names the key
    /// ordinal that witness proves, because that is what it matches against
    /// the join it is attached to.
    #[test]
    fn a_join_build_producer_deploys_the_key_ordinal_its_witness_proves() {
        let filter = membership_filter(vec![join_build_producer()]);
        let role = completed_producer_role(&filter, &filter.producers[0])
            .expect("the witness this producer names is in the filter");
        let SqlRuntimeFilterBindingRoleFacts::Producer { target, .. } = role else {
            panic!("a producer binding has a producer role");
        };
        assert!(matches!(
            target,
            SqlRuntimeFilterProducerTarget::JoinBuildKey { ordinal: 2 }
        ));
    }

    /// A producer naming a witness this filter does not have is refused: the
    /// key ordinal would otherwise be read off whichever witness happened to
    /// be first.
    #[test]
    fn a_producer_naming_an_absent_equality_witness_is_refused() {
        let mut filter = membership_filter(vec![join_build_producer()]);
        filter.equality_witnesses = Box::from([]);
        let error = completed_producer_role(&filter, &filter.producers[0])
            .expect_err("an absent witness has no key ordinal");
        assert!(error.contains("absent equality witness"), "{error}");
    }

    /// A probe-side consumer applies to one named join input. Applying it
    /// anywhere else would filter rows the join never compares.
    #[test]
    fn a_join_probe_consumer_deploys_the_input_it_applies_to() {
        let consumer = RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: PhysicalFragmentId::new(1),
                node: NodeId::new(7),
                values: Box::from([ValueId::new(2)]),
            },
            apply_point: PhysicalApplyPoint::NodeInput { input_ordinal: 0 },
            capabilities: Box::from([
                novarocks_physical_plan::RuntimeFilterArtifactCapability::Membership,
            ]),
            activation: novarocks_physical_plan::RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: novarocks_physical_plan::RuntimeFilterConsumerTarget::JoinProbeKey {
                equality: RuntimeFilterEqualityWitnessId::new(1),
            },
        };
        let role = completed_consumer_role(&consumer).expect("a probe key names its input");
        let SqlRuntimeFilterBindingRoleFacts::Consumer { target, .. } = role else {
            panic!("a consumer binding has a consumer role");
        };
        assert!(matches!(
            target,
            SqlRuntimeFilterConsumerTarget::DirectInput { input_ordinal: 0 }
        ));
    }

    /// Coverage is carried as an arena and read as a tree.
    #[test]
    fn coverage_expands_from_its_arena_in_the_order_it_was_written() {
        let coverage = RuntimeFilterCoverage {
            nodes: Box::from([
                RuntimeFilterCoverageNode::Witness(RuntimeFilterWitnessId::new(4)),
                RuntimeFilterCoverageNode::Witness(RuntimeFilterWitnessId::new(5)),
                RuntimeFilterCoverageNode::AnyOf {
                    children: Box::from([0, 1]),
                },
            ]),
            root: 2,
        };
        let tree = coverage_tree(&coverage).expect("children precede their parent");
        let SqlRuntimeFilterCoverageFacts::AnyOf(children) = tree else {
            panic!("the root is an any-of");
        };
        assert_eq!(children.len(), 2);
        assert!(matches!(
            children[0],
            SqlRuntimeFilterCoverageFacts::LeafWitnessId(4)
        ));
        assert!(matches!(
            children[1],
            SqlRuntimeFilterCoverageFacts::LeafWitnessId(5)
        ));
    }

    /// A composite that names a node at or after itself is refused rather
    /// than walked, because walking it need not terminate.
    #[test]
    fn coverage_that_names_a_child_after_its_parent_is_refused() {
        let coverage = RuntimeFilterCoverage {
            nodes: Box::from([
                RuntimeFilterCoverageNode::AllOf {
                    children: Box::from([1]),
                },
                RuntimeFilterCoverageNode::Witness(RuntimeFilterWitnessId::new(6)),
            ]),
            root: 0,
        };
        let error = coverage_tree(&coverage).expect_err("a forward child is not walkable");
        assert!(error.contains("does not precede it"), "{error}");
    }
}
