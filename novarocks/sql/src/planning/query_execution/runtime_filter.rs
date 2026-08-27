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

//! Immutable runtime-filter facts for execution preparation.
//!
//! SQL keeps the graph, contract, coverage, and progress trees private. Core
//! receives only these sealed values and supplies physical scan ordinals for
//! already pinned connector reads.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;

use crate::plan_read::{ColumnId, DistributedPlan, TypedExpr};
use crate::planner::runtime_filter::contract::*;
use crate::planner::runtime_filter::coverage::Coverage;
use crate::planner::runtime_filter::graph::*;
use crate::planner::runtime_filter::progress::{FrontierSkip, JoinBuildProgressCatalog};

#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterFactsDraft {
    bindings: BTreeMap<u32, Vec<SqlRuntimeFilterBindingFacts>>,
    channels: Vec<SqlRuntimeFilterChannelFacts>,
    deployment_bindings: Vec<SqlRuntimeFilterDeploymentBindingFacts>,
    join_progress: Vec<SqlRuntimeFilterJoinProgressFacts>,
    source_requests: Vec<SqlRuntimeFilterSourceScanRequest>,
}

impl SqlRuntimeFilterFactsDraft {
    pub fn source_scan_requests(
        &self,
    ) -> impl ExactSizeIterator<Item = &SqlRuntimeFilterSourceScanRequest> {
        self.source_requests.iter()
    }
}

#[derive(Clone, Debug)]
pub struct SqlPreparedRuntimeFilterFacts {
    bindings: BTreeMap<u32, Vec<SqlRuntimeFilterBindingFacts>>,
    channels: Vec<SqlRuntimeFilterChannelFacts>,
    deployment_bindings: Vec<SqlRuntimeFilterDeploymentBindingFacts>,
    join_progress: Vec<SqlRuntimeFilterJoinProgressFacts>,
}

impl SqlPreparedRuntimeFilterFacts {
    pub fn bindings_for_fragment(&self, fragment_id: u32) -> &[SqlRuntimeFilterBindingFacts] {
        self.bindings
            .get(&fragment_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
    pub fn channels(&self) -> &[SqlRuntimeFilterChannelFacts] {
        &self.channels
    }
    pub fn deployment_bindings(&self) -> &[SqlRuntimeFilterDeploymentBindingFacts] {
        &self.deployment_bindings
    }
    pub fn join_progress(&self) -> &[SqlRuntimeFilterJoinProgressFacts] {
        &self.join_progress
    }
    pub fn has_channels(&self) -> bool {
        !self.channels.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterSourceScanRequest {
    pub binding_id: u32,
    pub fragment_id: u32,
    pub node_id: i32,
    pub column_id: ColumnId,
    pub data_type: DataType,
    pub nullable: bool,
}

/// The frozen scan-domain contract one source-boundary consumer resolved to.
///
/// It names no column: preparation binds the filter onto the scan's own typed
/// carrier by column handle, so the only thing left to agree on here is the
/// exact type and nullability the consumer expression was built against.
#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterSourceResolution {
    pub binding_id: u32,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterBindingFacts {
    pub binding_id: u32,
    pub channel_id: u32,
    pub node_id: i32,
    pub apply_point: SqlRuntimeFilterApplyPoint,
    pub expression: TypedExpr,
    pub logical_domain: SqlRuntimeFilterLogicalDomainFacts,
    pub reduction: SqlRuntimeFilterReductionFacts,
    pub role: SqlRuntimeFilterBindingRoleFacts,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterApplyPoint {
    NodeInput,
    NodeOutput,
}
#[derive(Clone, Debug)]
pub enum SqlRuntimeFilterLogicalDomainFacts {
    Membership {
        value_type: DataType,
        null_semantics: SqlRuntimeFilterNullSemantics,
    },
    Ordered {
        keys: Vec<SqlRuntimeFilterOrderKeyFacts>,
        inclusive: bool,
        comparator_digest: [u8; 32],
    },
}
#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterOrderKeyFacts {
    pub data_type: DataType,
    pub direction: SqlRuntimeFilterSortDirection,
    pub null_order: SqlRuntimeFilterNullOrder,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterSortDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterNullOrder {
    First,
    Last,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterReductionFacts {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: u32 },
}
#[derive(Clone, Debug)]
pub enum SqlRuntimeFilterBindingRoleFacts {
    Producer {
        contribution_kinds: Vec<SqlRuntimeFilterContributionKind>,
        completion_requirement: SqlRuntimeFilterCompletionRequirement,
        target: SqlRuntimeFilterProducerTarget,
    },
    Consumer {
        capabilities: Vec<SqlRuntimeFilterArtifactCapability>,
        activation: SqlRuntimeFilterConsumerActivation,
        target: SqlRuntimeFilterConsumerTarget,
    },
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterContributionKind {
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    TopKSummary,
    ProducerClosed,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterCompletionRequirement {
    ProducerClosed,
    FencedCommittedDomainFrozen,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive(SqlRuntimeFilterLateApplyGranularity),
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterLateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterProducerTarget {
    JoinBuildKey { ordinal: u32 },
    AggregateTopNKey { group_key_ordinal: u32, limit: u32 },
}
#[derive(Clone, Debug)]
pub enum SqlRuntimeFilterConsumerTarget {
    DirectInput {
        input_ordinal: u32,
    },
    SourceBoundary {
        scan_domain: Option<SqlRuntimeFilterScanDomainTarget>,
    },
}
#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterScanDomainTarget {
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterChannelFacts {
    pub channel_id: u32,
    pub logical_domain: SqlRuntimeFilterLogicalDomainFacts,
    pub lifecycle: SqlRuntimeFilterLifecycleFacts,
    pub availability_coverage: SqlRuntimeFilterCoverageFacts,
    pub terminal_coverage: SqlRuntimeFilterCoverageFacts,
    pub reduction: SqlRuntimeFilterReductionFacts,
    pub allowed_contribution_kinds: Vec<SqlRuntimeFilterContributionKind>,
    pub required_consumer_capabilities: Vec<SqlRuntimeFilterArtifactCapability>,
    pub policy: SqlRuntimeFilterPolicyFacts,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterLifecycleFacts {
    CompleteOnce,
    MonotonicUpdates,
}
#[derive(Clone, Debug)]
pub enum SqlRuntimeFilterCoverageFacts {
    LeafWitnessId(u32),
    AllOf(Vec<SqlRuntimeFilterCoverageFacts>),
    AnyOf(Vec<SqlRuntimeFilterCoverageFacts>),
}
#[derive(Clone, Copy, Debug)]
pub struct SqlRuntimeFilterPolicyFacts {
    pub max_contribution_bytes: u64,
    pub max_artifact_bytes: u64,
    pub deadline_ms: u64,
    pub max_retries: u32,
}
#[derive(Clone, Debug)]
pub struct SqlRuntimeFilterDeploymentBindingFacts {
    pub binding_id: u32,
    pub channel_id: u32,
    pub fragment_id: u32,
    pub node_id: i32,
    pub coverage_witness_id: Option<u32>,
    pub role: SqlRuntimeFilterBindingRoleFacts,
}
#[derive(Clone, Debug)]
pub enum SqlRuntimeFilterJoinProgressFacts {
    Proven {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        build_frontier: Vec<SqlRuntimeFilterFrontierEdgeFacts>,
        non_build_inputs: Vec<SqlRuntimeFilterFrontierEdgeFacts>,
    },
    Skipped {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        reason: SqlRuntimeFilterJoinProgressSkipReason,
    },
}
#[derive(Clone, Copy, Debug)]
pub struct SqlRuntimeFilterFrontierEdgeFacts {
    pub source_fragment_id: u32,
    pub target_exchange_node_id: i32,
}
#[derive(Clone, Copy, Debug)]
pub enum SqlRuntimeFilterJoinProgressSkipReason {
    NoRfSides,
    MissingChild,
    UnauditedNode { node_id: i32 },
}

pub fn project_runtime_filter_facts(
    plan: &DistributedPlan,
) -> Result<SqlRuntimeFilterFactsDraft, String> {
    project_from_sealed_parts(
        plan.runtime_filter_graph(),
        plan.fragments(),
        plan.runtime_filter_join_progress(),
    )
}

fn project_from_sealed_parts(
    graph: &RuntimeFilterGraph,
    fragments: &[crate::planner::distributed::PlanFragment],
    progress: &JoinBuildProgressCatalog,
) -> Result<SqlRuntimeFilterFactsDraft, String> {
    let mut attached = BTreeSet::new();
    let mut bindings = BTreeMap::<u32, Vec<_>>::new();
    let mut requests = Vec::new();
    fn visit(
        node: &crate::planner::distributed::DistributedNode,
        graph: &RuntimeFilterGraph,
        attached: &mut BTreeSet<u32>,
        bindings: &mut BTreeMap<u32, Vec<SqlRuntimeFilterBindingFacts>>,
        requests: &mut Vec<SqlRuntimeFilterSourceScanRequest>,
    ) -> Result<(), String> {
        for binding_id in &node.runtime_filter_binding_ids {
            if !attached.insert(binding_id.get()) {
                return Err(format!(
                    "runtime filter binding id={} is attached more than once",
                    binding_id.get()
                ));
            }
            let binding = graph.binding(*binding_id).ok_or_else(|| {
                format!(
                    "runtime filter attachment references unknown binding id={}",
                    binding_id.get()
                )
            })?;
            if binding.location.fragment_id.get() != node.fragment_id
                || binding.location.node_id.get() != node.node_id
            {
                return Err(format!(
                    "runtime filter binding id={} location does not match attachment",
                    binding_id.get()
                ));
            }
            let channel = graph.channel(binding.channel_id).ok_or_else(|| {
                format!(
                    "runtime filter binding id={} references unknown channel id={}",
                    binding_id.get(),
                    binding.channel_id.get()
                )
            })?;
            let domain = domain(&channel.logical_domain);
            let expression_matches = match &channel.logical_domain {
                RuntimeFilterLogicalDomain::Membership { value_type, .. } => {
                    value_type == &binding.expression.data_type
                }
                RuntimeFilterLogicalDomain::OrderedBound(order) => {
                    order.keys.len() == 1 && order.keys[0].data_type == binding.expression.data_type
                }
            };
            if !expression_matches {
                return Err(format!(
                    "runtime filter binding id={} expression type does not match channel domain",
                    binding_id.get()
                ));
            }
            let role = match (&binding.role, binding.apply_point) {
                (RuntimeFilterBindingRole::Producer(requirement), ApplyPoint::NodeOutput) => {
                    role_producer(requirement)
                }
                (RuntimeFilterBindingRole::Consumer(requirement), ApplyPoint::NodeInput) => {
                    role_consumer(binding, requirement, requests)
                }
                (RuntimeFilterBindingRole::Producer(_), point) => {
                    return Err(format!(
                        "runtime filter producer binding id={} must use NodeOutput, found {point:?}",
                        binding_id.get()
                    ));
                }
                (RuntimeFilterBindingRole::Consumer(_), point) => {
                    return Err(format!(
                        "runtime filter consumer binding id={} must use NodeInput, found {point:?}",
                        binding_id.get()
                    ));
                }
            };
            bindings
                .entry(node.fragment_id)
                .or_default()
                .push(SqlRuntimeFilterBindingFacts {
                    binding_id: binding_id.get(),
                    channel_id: binding.channel_id.get(),
                    node_id: node.node_id,
                    apply_point: match binding.apply_point {
                        ApplyPoint::NodeInput => SqlRuntimeFilterApplyPoint::NodeInput,
                        ApplyPoint::NodeOutput => SqlRuntimeFilterApplyPoint::NodeOutput,
                    },
                    expression: binding.expression.clone(),
                    logical_domain: domain,
                    reduction: reduction(channel.reduction_requirement),
                    role,
                });
        }
        for child in &node.children {
            visit(child, graph, attached, bindings, requests)?;
        }
        Ok(())
    }
    for fragment in fragments {
        visit(
            &fragment.root,
            graph,
            &mut attached,
            &mut bindings,
            &mut requests,
        )?;
    }
    if let Some(binding) = graph
        .bindings()
        .find(|binding| !attached.contains(&binding.binding_id.get()))
    {
        return Err(format!(
            "runtime filter graph binding id={} has no node attachment",
            binding.binding_id.get()
        ));
    }
    // The walk above appends in plan-tree order, so a fragment whose upper node
    // carries the larger binding id emits its bindings out of numeric order. The
    // wire contract keys bindings by a strictly increasing id, so canonicalize
    // here: `attached` already rejected duplicates, which makes the sorted order
    // strictly increasing rather than merely non-decreasing.
    for fragment_bindings in bindings.values_mut() {
        fragment_bindings.sort_by_key(|binding| binding.binding_id);
    }
    let channels = graph.channels().map(channel_facts).collect();
    let deployment_bindings = graph.bindings().map(deployment_binding_facts).collect();
    let join_progress = join_progress(graph, progress);
    Ok(SqlRuntimeFilterFactsDraft {
        bindings,
        channels,
        deployment_bindings,
        join_progress,
        source_requests: requests,
    })
}

pub fn finalize_runtime_filter_facts(
    mut draft: SqlRuntimeFilterFactsDraft,
    resolutions: impl IntoIterator<Item = SqlRuntimeFilterSourceResolution>,
) -> Result<SqlPreparedRuntimeFilterFacts, String> {
    let resolutions = resolutions
        .into_iter()
        .map(|value| (value.binding_id, value))
        .collect::<BTreeMap<_, _>>();
    for request in &draft.source_requests {
        let resolution = resolutions.get(&request.binding_id).ok_or_else(|| {
            format!(
                "runtime filter binding id={} has no pinned scan-domain resolution",
                request.binding_id
            )
        })?;
        if resolution.data_type != request.data_type || resolution.nullable != request.nullable {
            return Err(format!(
                "runtime filter binding id={} scan-domain resolution type/nullability drifted",
                request.binding_id
            ));
        }
        let binding = draft
            .bindings
            .get_mut(&request.fragment_id)
            .and_then(|values| {
                values
                    .iter_mut()
                    .find(|binding| binding.binding_id == request.binding_id)
            })
            .ok_or_else(|| {
                format!(
                    "runtime filter binding id={} disappeared from projected facts",
                    request.binding_id
                )
            })?;
        let SqlRuntimeFilterBindingRoleFacts::Consumer {
            target: SqlRuntimeFilterConsumerTarget::SourceBoundary { scan_domain },
            ..
        } = &mut binding.role
        else {
            return Err(format!(
                "runtime filter binding id={} has inconsistent source scan target",
                request.binding_id
            ));
        };
        *scan_domain = Some(SqlRuntimeFilterScanDomainTarget {
            data_type: resolution.data_type.clone(),
            nullable: resolution.nullable,
        });
    }
    if resolutions.len() != draft.source_requests.len() {
        return Err(
            "runtime filter source scan resolutions do not match projected requests".to_string(),
        );
    }
    Ok(SqlPreparedRuntimeFilterFacts {
        bindings: draft.bindings,
        channels: draft.channels,
        deployment_bindings: draft.deployment_bindings,
        join_progress: draft.join_progress,
    })
}

fn domain(value: &RuntimeFilterLogicalDomain) -> SqlRuntimeFilterLogicalDomainFacts {
    match value {
        RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } => SqlRuntimeFilterLogicalDomainFacts::Membership {
            value_type: value_type.clone(),
            null_semantics: match null_semantics {
                NullSemantics::NeverMatches => SqlRuntimeFilterNullSemantics::NeverMatches,
                NullSemantics::NullSafeEqual => SqlRuntimeFilterNullSemantics::NullSafeEqual,
            },
        },
        RuntimeFilterLogicalDomain::OrderedBound(order) => {
            SqlRuntimeFilterLogicalDomainFacts::Ordered {
                keys: order
                    .keys
                    .iter()
                    .map(|key| SqlRuntimeFilterOrderKeyFacts {
                        data_type: key.data_type.clone(),
                        direction: match key.direction {
                            SortDirection::Ascending => SqlRuntimeFilterSortDirection::Ascending,
                            SortDirection::Descending => SqlRuntimeFilterSortDirection::Descending,
                        },
                        null_order: match key.null_order {
                            NullOrder::First => SqlRuntimeFilterNullOrder::First,
                            NullOrder::Last => SqlRuntimeFilterNullOrder::Last,
                        },
                    })
                    .collect(),
                inclusive: order.inclusive,
                comparator_digest: order.comparator_digest.get(),
            }
        }
    }
}
fn reduction(value: ReductionRequirement) -> SqlRuntimeFilterReductionFacts {
    match value {
        ReductionRequirement::SetUnion => SqlRuntimeFilterReductionFacts::SetUnion,
        ReductionRequirement::TightenOrderedBound => {
            SqlRuntimeFilterReductionFacts::TightenOrderedBound
        }
        ReductionRequirement::MergeTopKSummary(v) => {
            SqlRuntimeFilterReductionFacts::MergeTopKSummary { k: v.k().get() }
        }
    }
}
fn kinds(values: &BTreeSet<ContributionKind>) -> Vec<SqlRuntimeFilterContributionKind> {
    values
        .iter()
        .map(|value| match value {
            ContributionKind::ValueDomainDelta => {
                SqlRuntimeFilterContributionKind::ValueDomainDelta
            }
            ContributionKind::FinalDomainShard => {
                SqlRuntimeFilterContributionKind::FinalDomainShard
            }
            ContributionKind::OrderedBoundUpdate => {
                SqlRuntimeFilterContributionKind::OrderedBoundUpdate
            }
            ContributionKind::TopKSummary => SqlRuntimeFilterContributionKind::TopKSummary,
            ContributionKind::ProducerClosed => SqlRuntimeFilterContributionKind::ProducerClosed,
        })
        .collect()
}
fn capabilities(values: &BTreeSet<ArtifactCapability>) -> Vec<SqlRuntimeFilterArtifactCapability> {
    values
        .iter()
        .map(|value| match value {
            ArtifactCapability::Membership => SqlRuntimeFilterArtifactCapability::Membership,
            ArtifactCapability::OrderedRange => SqlRuntimeFilterArtifactCapability::OrderedRange,
            ArtifactCapability::EmptyDomain => SqlRuntimeFilterArtifactCapability::EmptyDomain,
        })
        .collect()
}
fn completion(value: CompletionRequirement) -> SqlRuntimeFilterCompletionRequirement {
    match value {
        CompletionRequirement::ProducerClosed => {
            SqlRuntimeFilterCompletionRequirement::ProducerClosed
        }
        CompletionRequirement::FencedFinalDomain(_) => {
            SqlRuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen
        }
    }
}
fn activation(value: ConsumerActivation) -> SqlRuntimeFilterConsumerActivation {
    match value {
        ConsumerActivation::BlockingSnapshot => {
            SqlRuntimeFilterConsumerActivation::BlockingSnapshot
        }
        ConsumerActivation::NonBlockingLive { late_apply } => {
            SqlRuntimeFilterConsumerActivation::NonBlockingLive(match late_apply {
                LateApplyGranularity::Row => SqlRuntimeFilterLateApplyGranularity::Row,
                LateApplyGranularity::Batch => SqlRuntimeFilterLateApplyGranularity::Batch,
                LateApplyGranularity::RowGroup => SqlRuntimeFilterLateApplyGranularity::RowGroup,
                LateApplyGranularity::Split => SqlRuntimeFilterLateApplyGranularity::Split,
                LateApplyGranularity::File => SqlRuntimeFilterLateApplyGranularity::File,
            })
        }
    }
}
fn producer_target(value: ProducerBindingTarget) -> SqlRuntimeFilterProducerTarget {
    match value {
        ProducerBindingTarget::JoinBuildKey { ordinal } => {
            SqlRuntimeFilterProducerTarget::JoinBuildKey {
                ordinal: ordinal as u32,
            }
        }
        ProducerBindingTarget::AggregateTopNKey {
            group_key_ordinal,
            limit,
        } => SqlRuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal: group_key_ordinal as u32,
            limit: limit.get(),
        },
    }
}
fn role_producer(value: &ProducerRequirement) -> SqlRuntimeFilterBindingRoleFacts {
    SqlRuntimeFilterBindingRoleFacts::Producer {
        contribution_kinds: kinds(&value.contribution_kinds),
        completion_requirement: completion(value.completion_requirement),
        target: producer_target(value.target),
    }
}
fn role_consumer(
    binding: &RuntimeFilterBindingSpec,
    value: &ConsumerRequirement,
    requests: &mut Vec<SqlRuntimeFilterSourceScanRequest>,
) -> SqlRuntimeFilterBindingRoleFacts {
    let target = match &value.target {
        ConsumerBindingTarget::DirectInput { input_ordinal } => {
            SqlRuntimeFilterConsumerTarget::DirectInput {
                input_ordinal: *input_ordinal as u32,
            }
        }
        ConsumerBindingTarget::SourceBoundary { scan_domain } => {
            if let Some(target) = scan_domain {
                requests.push(SqlRuntimeFilterSourceScanRequest {
                    binding_id: binding.binding_id.get(),
                    fragment_id: binding.location.fragment_id.get(),
                    node_id: binding.location.node_id.get(),
                    column_id: target.column_id,
                    data_type: target.data_type.clone(),
                    nullable: target.nullable,
                });
            }
            SqlRuntimeFilterConsumerTarget::SourceBoundary { scan_domain: None }
        }
    };
    SqlRuntimeFilterBindingRoleFacts::Consumer {
        capabilities: capabilities(&value.capabilities),
        activation: activation(value.activation),
        target,
    }
}
fn coverage(value: &Coverage) -> SqlRuntimeFilterCoverageFacts {
    match value {
        Coverage::Leaf(value) => SqlRuntimeFilterCoverageFacts::LeafWitnessId(value.get()),
        Coverage::AllOf(values) => {
            SqlRuntimeFilterCoverageFacts::AllOf(values.iter().map(coverage).collect())
        }
        Coverage::AnyOf(values) => {
            SqlRuntimeFilterCoverageFacts::AnyOf(values.iter().map(coverage).collect())
        }
    }
}
fn channel_facts(value: &RuntimeFilterChannelSpec) -> SqlRuntimeFilterChannelFacts {
    SqlRuntimeFilterChannelFacts {
        channel_id: value.channel_id.get(),
        logical_domain: domain(&value.logical_domain),
        lifecycle: match value.lifecycle {
            RuntimeFilterLifecycle::CompleteOnce => SqlRuntimeFilterLifecycleFacts::CompleteOnce,
            RuntimeFilterLifecycle::MonotonicUpdates => {
                SqlRuntimeFilterLifecycleFacts::MonotonicUpdates
            }
        },
        availability_coverage: coverage(&value.availability_coverage),
        terminal_coverage: coverage(&value.terminal_coverage),
        reduction: reduction(value.reduction_requirement),
        allowed_contribution_kinds: kinds(&value.allowed_contribution_kinds),
        required_consumer_capabilities: capabilities(&value.required_consumer_capabilities),
        policy: SqlRuntimeFilterPolicyFacts {
            max_contribution_bytes: value.policy.max_contribution_bytes,
            max_artifact_bytes: value.policy.max_artifact_bytes,
            deadline_ms: value.policy.deadline_ms,
            max_retries: value.policy.max_retries,
        },
    }
}
fn deployment_binding_facts(
    binding: &RuntimeFilterBindingSpec,
) -> SqlRuntimeFilterDeploymentBindingFacts {
    let role = match &binding.role {
        RuntimeFilterBindingRole::Producer(value) => role_producer(value),
        RuntimeFilterBindingRole::Consumer(value) => SqlRuntimeFilterBindingRoleFacts::Consumer {
            capabilities: capabilities(&value.capabilities),
            activation: activation(value.activation),
            target: match &value.target {
                ConsumerBindingTarget::DirectInput { input_ordinal } => {
                    SqlRuntimeFilterConsumerTarget::DirectInput {
                        input_ordinal: *input_ordinal as u32,
                    }
                }
                ConsumerBindingTarget::SourceBoundary { .. } => {
                    SqlRuntimeFilterConsumerTarget::SourceBoundary { scan_domain: None }
                }
            },
        },
    };
    SqlRuntimeFilterDeploymentBindingFacts {
        binding_id: binding.binding_id.get(),
        channel_id: binding.channel_id.get(),
        fragment_id: binding.location.fragment_id.get(),
        node_id: binding.location.node_id.get(),
        coverage_witness_id: binding.coverage_witness_id.map(|value| value.get()),
        role,
    }
}
fn join_progress(
    graph: &RuntimeFilterGraph,
    progress: &JoinBuildProgressCatalog,
) -> Vec<SqlRuntimeFilterJoinProgressFacts> {
    graph
        .bindings()
        .filter_map(|binding| {
            let RuntimeFilterBindingRole::Producer(_) = &binding.role else {
                return None;
            };
            let key = (
                binding.channel_id,
                binding.binding_id,
                binding.location.fragment_id.get(),
            );
            if let Some(proof) = progress.get(&key) {
                Some(SqlRuntimeFilterJoinProgressFacts::Proven {
                    channel_id: proof.channel.get(),
                    producer_binding_id: proof.producer_binding.get(),
                    producer_fragment_id: proof.producer_fragment,
                    join_node_id: proof.join_node_id,
                    build_frontier: proof
                        .build_frontier
                        .iter()
                        .map(|edge| SqlRuntimeFilterFrontierEdgeFacts {
                            source_fragment_id: edge.source_fragment,
                            target_exchange_node_id: edge.target_exchange_node,
                        })
                        .collect(),
                    non_build_inputs: proof
                        .non_build_inputs
                        .iter()
                        .map(|edge| SqlRuntimeFilterFrontierEdgeFacts {
                            source_fragment_id: edge.source_fragment,
                            target_exchange_node_id: edge.target_exchange_node,
                        })
                        .collect(),
                })
            } else {
                progress
                    .skipped(&key)
                    .map(|skip| SqlRuntimeFilterJoinProgressFacts::Skipped {
                        channel_id: binding.channel_id.get(),
                        producer_binding_id: binding.binding_id.get(),
                        producer_fragment_id: binding.location.fragment_id.get(),
                        join_node_id: skip.join_node_id,
                        reason: match skip.rule {
                            FrontierSkip::NoRfSides => {
                                SqlRuntimeFilterJoinProgressSkipReason::NoRfSides
                            }
                            FrontierSkip::MissingChild => {
                                SqlRuntimeFilterJoinProgressSkipReason::MissingChild
                            }
                            FrontierSkip::UnauditedNode { node_id } => {
                                SqlRuntimeFilterJoinProgressSkipReason::UnauditedNode { node_id }
                            }
                        },
                    })
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::analysis::{ExprKind, LiteralValue};
    use crate::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::planner::payload::PlanValuesNode;
    use crate::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use crate::planner::runtime_filter::progress::{FrontierEdge, JoinBuildProgressProof};

    fn expression(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn node(
        node_id: i32,
        binding_ids: Vec<BindingId>,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id: 7,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: binding_ids,
            children,
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
        }
    }

    fn fragment(root: DistributedNode) -> PlanFragment {
        PlanFragment {
            fragment_id: 7,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    fn graph() -> RuntimeFilterGraph {
        graph_with_binding_ids(2, 3)
    }

    /// The producer always sits at parent node 10 and the consumer at child
    /// node 11, so passing a producer id above the consumer id builds a plan
    /// whose tree walk visits binding ids in decreasing order.
    fn graph_with_binding_ids(producer: u32, consumer: u32) -> RuntimeFilterGraph {
        let channel_id = ChannelId::new(1);
        let producer_id = BindingId::new(producer);
        let consumer_id = BindingId::new(consumer);
        let location = |node_id| PlanLocation {
            fragment_id: PlanFragmentId::new(7),
            node_id: PlanNodeId::new(node_id),
        };
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(RuntimeFilterChannelSpec {
                channel_id,
                logical_domain: RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: NullSemantics::NeverMatches,
                },
                lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                availability_coverage: Coverage::AllOf(vec![Coverage::Leaf(
                    CoverageWitnessId::new(9),
                )]),
                terminal_coverage: Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(9))]),
                reduction_requirement: ReductionRequirement::SetUnion,
                allowed_contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                required_consumer_capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                policy: RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 64,
                    max_artifact_bytes: 128,
                    deadline_ms: 50,
                    max_retries: 1,
                },
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: producer_id,
                channel_id,
                coverage_witness_id: Some(CoverageWitnessId::new(9)),
                location: location(10),
                expression: expression(2),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: BTreeSet::from([
                        ContributionKind::ValueDomainDelta,
                        ContributionKind::ProducerClosed,
                    ]),
                    completion_requirement: CompletionRequirement::ProducerClosed,
                    target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
                }),
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: consumer_id,
                channel_id,
                coverage_witness_id: None,
                location: location(11),
                expression: expression(3),
                apply_point: ApplyPoint::NodeInput,
                role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                    capabilities: BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    activation: ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    target: ConsumerBindingTarget::SourceBoundary {
                        scan_domain: Some(RuntimeFilterSemanticScanDomainTarget {
                            column_id: ColumnId::new_for_test(42),
                            data_type: DataType::Int64,
                            nullable: false,
                        }),
                    },
                }),
            })
            .unwrap();
        graph
    }

    fn attached_fragments() -> Vec<PlanFragment> {
        attached_fragments_with_binding_ids(2, 3)
    }

    fn attached_fragments_with_binding_ids(producer: u32, consumer: u32) -> Vec<PlanFragment> {
        vec![fragment(node(
            10,
            vec![BindingId::new(producer)],
            vec![node(11, vec![BindingId::new(consumer)], Vec::new())],
        ))]
    }

    #[test]
    fn projection_rejects_missing_attachment() {
        let error = project_from_sealed_parts(
            &graph(),
            &[fragment(node(10, Vec::new(), Vec::new()))],
            &JoinBuildProgressCatalog::new(),
        )
        .unwrap_err();
        assert!(error.contains("has no node attachment"), "{error}");
    }

    #[test]
    fn projection_orders_fragment_bindings_by_id_not_plan_tree_order() {
        // Nested-join shape: the parent node carries the larger binding id, so
        // the plan-tree walk appends 3 before 2. The wire encoder keys bindings
        // by a strictly increasing id and rejects anything else, so the
        // projection owns the canonical order.
        let draft = project_from_sealed_parts(
            &graph_with_binding_ids(3, 2),
            &attached_fragments_with_binding_ids(3, 2),
            &JoinBuildProgressCatalog::new(),
        )
        .unwrap();

        let ids = draft.bindings[&7]
            .iter()
            .map(|binding| binding.binding_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn finalization_rejects_unresolved_source_boundary() {
        let draft = project_from_sealed_parts(
            &graph(),
            &attached_fragments(),
            &JoinBuildProgressCatalog::new(),
        )
        .unwrap();
        let error = finalize_runtime_filter_facts(draft, Vec::new()).unwrap_err();
        assert_eq!(
            error,
            "runtime filter binding id=3 has no pinned scan-domain resolution"
        );
    }

    #[test]
    fn finalization_projects_channel_binding_progress_and_coverage() {
        let mut progress = JoinBuildProgressCatalog::new();
        progress.insert_proof(
            (ChannelId::new(1), BindingId::new(2), 7),
            JoinBuildProgressProof {
                channel: ChannelId::new(1),
                producer_binding: BindingId::new(2),
                producer_fragment: 7,
                join_node_id: 10,
                build_frontier: vec![FrontierEdge {
                    source_fragment: 5,
                    target_exchange_node: 51,
                }],
                non_build_inputs: vec![FrontierEdge {
                    source_fragment: 6,
                    target_exchange_node: 61,
                }],
            },
        );
        let draft = project_from_sealed_parts(&graph(), &attached_fragments(), &progress).unwrap();
        let facts = finalize_runtime_filter_facts(
            draft,
            vec![SqlRuntimeFilterSourceResolution {
                binding_id: 3,
                data_type: DataType::Int64,
                nullable: false,
            }],
        )
        .unwrap();
        assert_eq!(facts.channels().len(), 1);
        assert!(matches!(
            facts.channels()[0].availability_coverage,
            SqlRuntimeFilterCoverageFacts::AllOf(_)
        ));
        assert_eq!(facts.deployment_bindings().len(), 2);
        assert!(matches!(
            facts.join_progress(),
            [SqlRuntimeFilterJoinProgressFacts::Proven {
                channel_id: 1,
                producer_binding_id: 2,
                producer_fragment_id: 7,
                ..
            }]
        ));
        let consumer = facts
            .bindings_for_fragment(7)
            .iter()
            .find(|binding| binding.binding_id == 3)
            .unwrap();
        assert!(
            matches!(&consumer.role, SqlRuntimeFilterBindingRoleFacts::Consumer { target: SqlRuntimeFilterConsumerTarget::SourceBoundary { scan_domain: Some(target) }, .. } if target.data_type == DataType::Int64 && !target.nullable)
        );
    }
}
