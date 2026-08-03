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

//! Borrow-only runtime-filter facts for the Frontend semantic encoder.

use crate::proto::{common, expr};
use crate::protocol::native::encode::expr::encode_expr;
use crate::protocol::native::type_mapping::encode_type;
use crate::query_execution::schedule::SchedulingPlan;
use crate::sql::planner::distributed::FragmentEdge;
use crate::sql::planner::runtime_filter::coverage::Coverage;
use crate::sql::planner::runtime_filter::graph::{
    ApplyPoint, ConsumerBindingTarget, ProducerBindingTarget, RuntimeFilterBindingRole,
    RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
};

use super::projection::PreparedFragmentSet;
use super::runtime_filter_binding::{
    PreparedReductionContract, PreparedRuntimeFilterBinding, PreparedRuntimeFilterBindingRole,
    PreparedRuntimeFilterContract,
};

#[derive(Clone, Copy)]
pub struct RuntimeFilterBindingFactsView<'a> {
    prepared: &'a PreparedFragmentSet,
}

impl<'a> RuntimeFilterBindingFactsView<'a> {
    pub(crate) const fn new(prepared: &'a PreparedFragmentSet) -> Self {
        Self { prepared }
    }

    pub fn fragments(
        self,
    ) -> impl ExactSizeIterator<Item = RuntimeFilterBindingFragmentFactsView<'a>> + 'a {
        self.prepared
            .scheduling_view()
            .fragments()
            .map(|fragment| RuntimeFilterBindingFragmentFactsView { fragment })
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterBindingFragmentFactsView<'a> {
    fragment: &'a super::projection::PreparedFragment,
}

impl<'a> RuntimeFilterBindingFragmentFactsView<'a> {
    pub fn fragment_id(self) -> u32 {
        self.fragment.fragment_id()
    }

    pub fn bindings(self) -> impl ExactSizeIterator<Item = RuntimeFilterBindingFacts<'a>> + 'a {
        self.fragment
            .runtime_filter_bindings()
            .bindings()
            .map(|binding| RuntimeFilterBindingFacts { binding })
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterBindingFacts<'a> {
    binding: &'a PreparedRuntimeFilterBinding,
}

impl<'a> RuntimeFilterBindingFacts<'a> {
    pub fn binding_id(self) -> u32 {
        self.binding.binding_id().get()
    }

    pub fn channel_id(self) -> u32 {
        self.binding.channel_id().get()
    }

    pub fn node_id(self) -> i32 {
        self.binding.node_id().get()
    }

    pub fn apply_point(self) -> RuntimeFilterApplyPoint {
        match self.binding.apply_point() {
            ApplyPoint::NodeInput => RuntimeFilterApplyPoint::NodeInput,
            ApplyPoint::NodeOutput => RuntimeFilterApplyPoint::NodeOutput,
        }
    }

    /// Core owns this generic TypedExpr-to-wire leaf projection. The Frontend
    /// receives no TypedExpr or native encoder context.
    pub fn expression(self) -> Result<expr::Expr, String> {
        encode_expr(self.binding.expression())
    }

    pub fn contract(self) -> RuntimeFilterContractFacts<'a> {
        match self.binding.contract() {
            PreparedRuntimeFilterContract::Membership {
                canonical_schema,
                schema_digest,
            } => RuntimeFilterContractFacts::Membership {
                canonical_schema,
                schema_digest: schema_digest.bytes(),
            },
            PreparedRuntimeFilterContract::Ordered {
                keys,
                comparator_digest,
                order_contract_digest,
            } => RuntimeFilterContractFacts::Ordered {
                keys,
                comparator_digest: comparator_digest.get(),
                order_contract_digest: order_contract_digest.bytes(),
            },
        }
    }

    pub fn reduction(self) -> RuntimeFilterReductionFacts {
        match self.binding.reduction() {
            PreparedReductionContract::SetUnion => RuntimeFilterReductionFacts::SetUnion,
            PreparedReductionContract::TightenOrderedBound => {
                RuntimeFilterReductionFacts::TightenOrderedBound
            }
            PreparedReductionContract::MergeTopKSummary { k, contract_digest } => {
                RuntimeFilterReductionFacts::MergeTopKSummary {
                    k: k.get(),
                    contract_digest: contract_digest.bytes(),
                }
            }
        }
    }

    pub fn role(self) -> RuntimeFilterBindingRoleFacts {
        match self.binding.role() {
            PreparedRuntimeFilterBindingRole::Producer {
                contribution_kinds,
                completion_requirement,
                target,
            } => RuntimeFilterBindingRoleFacts::Producer {
                contribution_kinds: contribution_kinds
                    .iter()
                    .copied()
                    .map(RuntimeFilterContributionKind::from_sql)
                    .collect(),
                completion_requirement: RuntimeFilterCompletionRequirement::from_sql(
                    *completion_requirement,
                ),
                target: RuntimeFilterProducerTarget::from_sql(*target),
            },
            PreparedRuntimeFilterBindingRole::Consumer {
                capabilities,
                activation,
                target,
            } => RuntimeFilterBindingRoleFacts::Consumer {
                capabilities: capabilities
                    .iter()
                    .copied()
                    .map(RuntimeFilterArtifactCapability::from_sql)
                    .collect(),
                activation: RuntimeFilterConsumerActivation::from_sql(*activation),
                target: RuntimeFilterConsumerTarget::from_sql(*target),
            },
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterApplyPoint {
    NodeInput,
    NodeOutput,
}

pub enum RuntimeFilterContractFacts<'a> {
    Membership {
        canonical_schema: &'a [u8],
        schema_digest: [u8; 32],
    },
    Ordered {
        keys: &'a [crate::runtime_filter::port::ordered_bound::RuntimeOrderKey],
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    },
}

impl RuntimeFilterContractFacts<'_> {
    pub fn ordered_keys(&self) -> Vec<RuntimeFilterOrderKeyFacts> {
        match self {
            Self::Membership { .. } => Vec::new(),
            Self::Ordered { keys, .. } => keys
                .iter()
                .map(|key| RuntimeFilterOrderKeyFacts {
                    r#type: encode_type(key.data_type())
                        .expect("sealed order-key type is encodable"),
                    direction: RuntimeFilterSortDirection::from_runtime(key.direction()),
                    null_order: RuntimeFilterNullOrder::from_runtime(key.null_order()),
                })
                .collect(),
        }
    }
}

pub struct RuntimeFilterOrderKeyFacts {
    pub r#type: common::TypeDesc,
    pub direction: RuntimeFilterSortDirection,
    pub null_order: RuntimeFilterNullOrder,
}

pub enum RuntimeFilterReductionFacts {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: u32, contract_digest: [u8; 32] },
}

pub enum RuntimeFilterBindingRoleFacts {
    Producer {
        contribution_kinds: Vec<RuntimeFilterContributionKind>,
        completion_requirement: RuntimeFilterCompletionRequirement,
        target: RuntimeFilterProducerTarget,
    },
    Consumer {
        capabilities: Vec<RuntimeFilterArtifactCapability>,
        activation: RuntimeFilterConsumerActivation,
        target: RuntimeFilterConsumerTarget,
    },
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterContributionKind {
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    TopKSummary,
    ProducerClosed,
}

impl RuntimeFilterContributionKind {
    fn from_sql(value: crate::sql::planner::runtime_filter::contract::ContributionKind) -> Self {
        use crate::sql::planner::runtime_filter::contract::ContributionKind;
        match value {
            ContributionKind::ValueDomainDelta => Self::ValueDomainDelta,
            ContributionKind::FinalDomainShard => Self::FinalDomainShard,
            ContributionKind::OrderedBoundUpdate => Self::OrderedBoundUpdate,
            ContributionKind::TopKSummary => Self::TopKSummary,
            ContributionKind::ProducerClosed => Self::ProducerClosed,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterCompletionRequirement {
    ProducerClosed,
    FencedCommittedDomainFrozen,
}

impl RuntimeFilterCompletionRequirement {
    fn from_sql(
        value: crate::sql::planner::runtime_filter::contract::CompletionRequirement,
    ) -> Self {
        use crate::sql::planner::runtime_filter::contract::{
            CompletionFenceKind, CompletionRequirement,
        };
        match value {
            CompletionRequirement::ProducerClosed => Self::ProducerClosed,
            CompletionRequirement::FencedFinalDomain(
                CompletionFenceKind::CommittedDomainFrozen,
            ) => Self::FencedCommittedDomainFrozen,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}

impl RuntimeFilterArtifactCapability {
    fn from_sql(value: crate::sql::planner::runtime_filter::contract::ArtifactCapability) -> Self {
        use crate::sql::planner::runtime_filter::contract::ArtifactCapability;
        match value {
            ArtifactCapability::Membership => Self::Membership,
            ArtifactCapability::OrderedRange => Self::OrderedRange,
            ArtifactCapability::EmptyDomain => Self::EmptyDomain,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive(RuntimeFilterLateApplyGranularity),
}

impl RuntimeFilterConsumerActivation {
    fn from_sql(value: crate::sql::planner::runtime_filter::contract::ConsumerActivation) -> Self {
        use crate::sql::planner::runtime_filter::contract::{
            ConsumerActivation, LateApplyGranularity,
        };
        match value {
            ConsumerActivation::BlockingSnapshot => Self::BlockingSnapshot,
            ConsumerActivation::NonBlockingLive { late_apply } => {
                Self::NonBlockingLive(match late_apply {
                    LateApplyGranularity::Row => RuntimeFilterLateApplyGranularity::Row,
                    LateApplyGranularity::Batch => RuntimeFilterLateApplyGranularity::Batch,
                    LateApplyGranularity::RowGroup => RuntimeFilterLateApplyGranularity::RowGroup,
                    LateApplyGranularity::Split => RuntimeFilterLateApplyGranularity::Split,
                    LateApplyGranularity::File => RuntimeFilterLateApplyGranularity::File,
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterLateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterProducerTarget {
    JoinBuildKey { ordinal: u32 },
    AggregateTopNKey { group_key_ordinal: u32, limit: u32 },
}

impl RuntimeFilterProducerTarget {
    fn from_sql(value: ProducerBindingTarget) -> Self {
        match value {
            ProducerBindingTarget::JoinBuildKey { ordinal } => Self::JoinBuildKey {
                ordinal: u32::try_from(ordinal).expect("sealed ordinal fits u32"),
            },
            ProducerBindingTarget::AggregateTopNKey {
                group_key_ordinal,
                limit,
            } => Self::AggregateTopNKey {
                group_key_ordinal: u32::try_from(group_key_ordinal)
                    .expect("sealed group ordinal fits u32"),
                limit: limit.get(),
            },
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterConsumerTarget {
    DirectInputOrdinal(u32),
    SourceBoundary,
}

impl RuntimeFilterConsumerTarget {
    fn from_sql(value: ConsumerBindingTarget) -> Self {
        match value {
            ConsumerBindingTarget::DirectInput { input_ordinal } => Self::DirectInputOrdinal(
                u32::try_from(input_ordinal).expect("sealed input ordinal fits u32"),
            ),
            ConsumerBindingTarget::SourceBoundary => Self::SourceBoundary,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterSortDirection {
    Ascending,
    Descending,
}

impl RuntimeFilterSortDirection {
    fn from_sql(value: crate::sql::planner::runtime_filter::contract::SortDirection) -> Self {
        match value {
            crate::sql::planner::runtime_filter::contract::SortDirection::Ascending => {
                Self::Ascending
            }
            crate::sql::planner::runtime_filter::contract::SortDirection::Descending => {
                Self::Descending
            }
        }
    }

    fn from_runtime(value: crate::runtime_filter::model::contract::SortDirection) -> Self {
        match value {
            crate::runtime_filter::model::contract::SortDirection::Ascending => Self::Ascending,
            crate::runtime_filter::model::contract::SortDirection::Descending => Self::Descending,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterNullOrder {
    First,
    Last,
}

impl RuntimeFilterNullOrder {
    fn from_sql(value: crate::sql::planner::runtime_filter::contract::NullOrder) -> Self {
        match value {
            crate::sql::planner::runtime_filter::contract::NullOrder::First => Self::First,
            crate::sql::planner::runtime_filter::contract::NullOrder::Last => Self::Last,
        }
    }

    fn from_runtime(value: crate::runtime_filter::model::contract::NullOrder) -> Self {
        match value {
            crate::runtime_filter::model::contract::NullOrder::First => Self::First,
            crate::runtime_filter::model::contract::NullOrder::Last => Self::Last,
        }
    }
}

/// Borrow-only deployment facts projected from the sealed SQL plan and the
/// already validated schedule. This is intentionally not a graph facade:
/// every public result is a narrow immutable fact value.
#[derive(Clone, Copy)]
pub struct RuntimeFilterDeploymentFactsView<'a> {
    prepared: &'a PreparedFragmentSet,
    schedule: &'a SchedulingPlan,
}

impl<'a> RuntimeFilterDeploymentFactsView<'a> {
    pub(crate) const fn new(
        prepared: &'a PreparedFragmentSet,
        schedule: &'a SchedulingPlan,
    ) -> Self {
        Self { prepared, schedule }
    }

    pub fn channels(self) -> impl Iterator<Item = RuntimeFilterChannelDeploymentFacts<'a>> + 'a {
        self.prepared
            .runtime_filter_graph()
            .channels()
            .map(|channel| RuntimeFilterChannelDeploymentFacts {
                prepared: self.prepared,
                channel,
            })
    }

    pub fn bindings(self) -> impl Iterator<Item = RuntimeFilterDeploymentBindingFacts<'a>> + 'a {
        self.prepared
            .runtime_filter_graph()
            .bindings()
            .map(|binding| RuntimeFilterDeploymentBindingFacts { binding })
    }

    pub fn placements(self) -> impl Iterator<Item = RuntimeFilterValidatedPlacementFacts> + 'a {
        self.schedule
            .by_fragment
            .values()
            .flatten()
            .map(|placement| RuntimeFilterValidatedPlacementFacts {
                fragment_id: placement.fragment_id,
                instance_index: placement.instance_index,
                fragment_instance_id: placement.finst_id,
                backend_idx: placement.backend_idx,
            })
    }

    pub fn fragment_edges(
        self,
    ) -> impl ExactSizeIterator<Item = RuntimeFilterFragmentEdgeFacts> + 'a {
        self.prepared
            .scheduling_view()
            .edges()
            .iter()
            .map(RuntimeFilterFragmentEdgeFacts::from_fragment_edge)
    }

    /// Each producer tuple has at most one sealed proof or skip provenance.
    /// The source bindings are BTreeMap ordered, so this iterator is stable.
    pub fn join_progress(self) -> impl Iterator<Item = RuntimeFilterJoinProgressFacts> + 'a {
        let catalog = self.prepared.runtime_filter_join_progress();
        self.prepared
            .runtime_filter_graph()
            .bindings()
            .filter_map(move |binding| {
                let RuntimeFilterBindingRole::Producer(_) = &binding.role else {
                    return None;
                };
                let key = (
                    binding.channel_id,
                    binding.binding_id,
                    binding.location.fragment_id.get(),
                );
                if let Some(proof) = catalog.get(&key) {
                    return Some(RuntimeFilterJoinProgressFacts::Proven {
                        channel_id: proof.channel.get(),
                        producer_binding_id: proof.producer_binding.get(),
                        producer_fragment_id: proof.producer_fragment,
                        join_node_id: proof.join_node_id,
                        build_frontier: proof
                            .build_frontier
                            .iter()
                            .map(RuntimeFilterFrontierEdgeFacts::from_sql)
                            .collect(),
                        non_build_inputs: proof
                            .non_build_inputs
                            .iter()
                            .map(RuntimeFilterFrontierEdgeFacts::from_sql)
                            .collect(),
                    });
                }
                catalog
                    .skipped(&key)
                    .map(|skip| RuntimeFilterJoinProgressFacts::Skipped {
                        channel_id: binding.channel_id.get(),
                        producer_binding_id: binding.binding_id.get(),
                        producer_fragment_id: binding.location.fragment_id.get(),
                        join_node_id: skip.join_node_id,
                        reason: RuntimeFilterJoinProgressSkipReason::from_sql(skip.rule),
                    })
            })
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterChannelDeploymentFacts<'a> {
    prepared: &'a PreparedFragmentSet,
    channel: &'a RuntimeFilterChannelSpec,
}

impl RuntimeFilterChannelDeploymentFacts<'_> {
    pub fn channel_id(self) -> u32 {
        self.channel.channel_id.get()
    }

    pub fn logical_domain(self) -> RuntimeFilterDeploymentLogicalDomainFacts {
        let binding = self.canonical_binding();
        match &self.channel.logical_domain {
            crate::sql::planner::runtime_filter::contract::RuntimeFilterLogicalDomain::Membership {
                value_type,
                null_semantics,
            } => RuntimeFilterDeploymentLogicalDomainFacts::Membership {
                value_type: encode_type(value_type).expect("sealed runtime-filter type is encodable"),
                null_semantics: RuntimeFilterNullSemantics::from_sql(*null_semantics),
                canonical_schema: match binding.contract() {
                    PreparedRuntimeFilterContract::Membership {
                        canonical_schema, ..
                    } => canonical_schema.to_vec(),
                    PreparedRuntimeFilterContract::Ordered { .. } => {
                        unreachable!("sealed channel domain and binding contract agree")
                    }
                },
                schema_digest: match binding.contract() {
                    PreparedRuntimeFilterContract::Membership { schema_digest, .. } => {
                        schema_digest.bytes()
                    }
                    PreparedRuntimeFilterContract::Ordered { .. } => {
                        unreachable!("sealed channel domain and binding contract agree")
                    }
                },
            },
            crate::sql::planner::runtime_filter::contract::RuntimeFilterLogicalDomain::OrderedBound(
                order,
            ) => RuntimeFilterDeploymentLogicalDomainFacts::Ordered {
                value_type: encode_type(
                    &order
                        .keys
                        .first()
                        .expect("sealed ordered runtime-filter domain has a key")
                        .data_type,
                )
                .expect("sealed runtime-filter ordered value type is encodable"),
                keys: order
                    .keys
                    .iter()
                    .map(|key| RuntimeFilterOrderKeyFacts {
                        r#type: encode_type(&key.data_type)
                            .expect("sealed runtime-filter order-key type is encodable"),
                        direction: RuntimeFilterSortDirection::from_sql(key.direction),
                        null_order: RuntimeFilterNullOrder::from_sql(key.null_order),
                    })
                    .collect(),
                comparator_digest: order.comparator_digest.get(),
                order_contract_digest: match binding.contract() {
                    PreparedRuntimeFilterContract::Ordered {
                        order_contract_digest,
                        ..
                    } => order_contract_digest.bytes(),
                    PreparedRuntimeFilterContract::Membership { .. } => {
                        unreachable!("sealed channel domain and binding contract agree")
                    }
                },
            },
        }
    }

    pub fn lifecycle(self) -> RuntimeFilterDeploymentLifecycleFacts {
        match self.channel.lifecycle {
            crate::sql::planner::runtime_filter::contract::RuntimeFilterLifecycle::CompleteOnce => {
                RuntimeFilterDeploymentLifecycleFacts::CompleteOnce
            }
            crate::sql::planner::runtime_filter::contract::RuntimeFilterLifecycle::MonotonicUpdates => {
                RuntimeFilterDeploymentLifecycleFacts::MonotonicUpdates
            }
        }
    }

    pub fn availability_coverage(self) -> RuntimeFilterCoverageFacts {
        RuntimeFilterCoverageFacts::from_sql(&self.channel.availability_coverage)
    }

    pub fn terminal_coverage(self) -> RuntimeFilterCoverageFacts {
        RuntimeFilterCoverageFacts::from_sql(&self.channel.terminal_coverage)
    }

    pub fn reduction(self) -> RuntimeFilterDeploymentReductionFacts {
        match self.canonical_binding().reduction() {
            PreparedReductionContract::SetUnion => RuntimeFilterDeploymentReductionFacts::SetUnion,
            PreparedReductionContract::TightenOrderedBound => {
                RuntimeFilterDeploymentReductionFacts::TightenOrderedBound
            }
            PreparedReductionContract::MergeTopKSummary { k, contract_digest } => {
                RuntimeFilterDeploymentReductionFacts::MergeTopKSummary {
                    k: k.get(),
                    contract_digest: contract_digest.bytes(),
                }
            }
        }
    }

    pub fn allowed_contribution_kinds(self) -> Vec<RuntimeFilterContributionKind> {
        self.channel
            .allowed_contribution_kinds
            .iter()
            .copied()
            .map(RuntimeFilterContributionKind::from_sql)
            .collect()
    }

    pub fn required_consumer_capabilities(self) -> Vec<RuntimeFilterArtifactCapability> {
        self.channel
            .required_consumer_capabilities
            .iter()
            .copied()
            .map(RuntimeFilterArtifactCapability::from_sql)
            .collect()
    }

    pub fn policy(self) -> RuntimeFilterPolicyFacts {
        RuntimeFilterPolicyFacts {
            max_contribution_bytes: self.channel.policy.max_contribution_bytes,
            max_artifact_bytes: self.channel.policy.max_artifact_bytes,
            deadline_ms: self.channel.policy.deadline_ms,
            max_retries: self.channel.policy.max_retries,
        }
    }

    fn canonical_binding(&self) -> &PreparedRuntimeFilterBinding {
        self.prepared
            .scheduling_view()
            .fragments()
            .flat_map(|fragment| fragment.runtime_filter_bindings().bindings())
            .find(|binding| binding.channel_id() == self.channel.channel_id)
            .expect("sealed runtime-filter channel has a materialized binding")
    }
}

pub enum RuntimeFilterDeploymentLogicalDomainFacts {
    Membership {
        value_type: common::TypeDesc,
        null_semantics: RuntimeFilterNullSemantics,
        canonical_schema: Vec<u8>,
        schema_digest: [u8; 32],
    },
    Ordered {
        value_type: common::TypeDesc,
        keys: Vec<RuntimeFilterOrderKeyFacts>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    },
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}

impl RuntimeFilterNullSemantics {
    fn from_sql(value: crate::sql::planner::runtime_filter::contract::NullSemantics) -> Self {
        match value {
            crate::sql::planner::runtime_filter::contract::NullSemantics::NeverMatches => {
                Self::NeverMatches
            }
            crate::sql::planner::runtime_filter::contract::NullSemantics::NullSafeEqual => {
                Self::NullSafeEqual
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterDeploymentLifecycleFacts {
    CompleteOnce,
    MonotonicUpdates,
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterDeploymentReductionFacts {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: u32, contract_digest: [u8; 32] },
}

pub enum RuntimeFilterCoverageFacts {
    LeafWitnessId(u32),
    AllOf(Vec<RuntimeFilterCoverageFacts>),
    AnyOf(Vec<RuntimeFilterCoverageFacts>),
}

impl RuntimeFilterCoverageFacts {
    fn from_sql(coverage: &Coverage) -> Self {
        match coverage {
            Coverage::Leaf(witness) => Self::LeafWitnessId(witness.get()),
            Coverage::AllOf(children) => Self::AllOf(children.iter().map(Self::from_sql).collect()),
            Coverage::AnyOf(children) => Self::AnyOf(children.iter().map(Self::from_sql).collect()),
        }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterPolicyFacts {
    pub max_contribution_bytes: u64,
    pub max_artifact_bytes: u64,
    pub deadline_ms: u64,
    pub max_retries: u32,
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterDeploymentBindingFacts<'a> {
    binding: &'a RuntimeFilterBindingSpec,
}

impl RuntimeFilterDeploymentBindingFacts<'_> {
    pub fn binding_id(self) -> u32 {
        self.binding.binding_id.get()
    }

    pub fn channel_id(self) -> u32 {
        self.binding.channel_id.get()
    }

    pub fn fragment_id(self) -> u32 {
        self.binding.location.fragment_id.get()
    }

    pub fn node_id(self) -> i32 {
        self.binding.location.node_id.get()
    }

    pub fn coverage_witness_id(self) -> Option<u32> {
        self.binding
            .coverage_witness_id
            .map(|witness| witness.get())
    }

    pub fn role(self) -> RuntimeFilterDeploymentBindingRoleFacts {
        match &self.binding.role {
            RuntimeFilterBindingRole::Producer(requirement) => {
                RuntimeFilterDeploymentBindingRoleFacts::Producer {
                    contribution_kinds: requirement
                        .contribution_kinds
                        .iter()
                        .copied()
                        .map(RuntimeFilterContributionKind::from_sql)
                        .collect(),
                    completion_requirement: RuntimeFilterCompletionRequirement::from_sql(
                        requirement.completion_requirement,
                    ),
                    target: RuntimeFilterProducerTarget::from_sql(requirement.target),
                }
            }
            RuntimeFilterBindingRole::Consumer(requirement) => {
                RuntimeFilterDeploymentBindingRoleFacts::Consumer {
                    capabilities: requirement
                        .capabilities
                        .iter()
                        .copied()
                        .map(RuntimeFilterArtifactCapability::from_sql)
                        .collect(),
                    activation: RuntimeFilterConsumerActivation::from_sql(requirement.activation),
                    target: RuntimeFilterConsumerTarget::from_sql(requirement.target),
                }
            }
        }
    }
}

pub enum RuntimeFilterDeploymentBindingRoleFacts {
    Producer {
        contribution_kinds: Vec<RuntimeFilterContributionKind>,
        completion_requirement: RuntimeFilterCompletionRequirement,
        target: RuntimeFilterProducerTarget,
    },
    Consumer {
        capabilities: Vec<RuntimeFilterArtifactCapability>,
        activation: RuntimeFilterConsumerActivation,
        target: RuntimeFilterConsumerTarget,
    },
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterValidatedPlacementFacts {
    fragment_id: u32,
    instance_index: usize,
    fragment_instance_id: crate::common::types::UniqueId,
    backend_idx: usize,
}

impl RuntimeFilterValidatedPlacementFacts {
    pub const fn fragment_id(self) -> u32 {
        self.fragment_id
    }

    pub const fn instance_index(self) -> usize {
        self.instance_index
    }

    pub const fn fragment_instance_id(self) -> crate::common::types::UniqueId {
        self.fragment_instance_id
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterFragmentEdgeFacts {
    source_fragment_id: u32,
    target_fragment_id: u32,
    target_exchange_node_id: i32,
}

impl RuntimeFilterFragmentEdgeFacts {
    fn from_fragment_edge(edge: &FragmentEdge) -> Self {
        Self {
            source_fragment_id: edge.source_fragment_id,
            target_fragment_id: edge.target_fragment_id,
            target_exchange_node_id: edge.target_exchange_node_id,
        }
    }

    pub const fn source_fragment_id(self) -> u32 {
        self.source_fragment_id
    }

    pub const fn target_fragment_id(self) -> u32 {
        self.target_fragment_id
    }

    pub const fn target_exchange_node_id(self) -> i32 {
        self.target_exchange_node_id
    }
}

pub enum RuntimeFilterJoinProgressFacts {
    Proven {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        build_frontier: Vec<RuntimeFilterFrontierEdgeFacts>,
        non_build_inputs: Vec<RuntimeFilterFrontierEdgeFacts>,
    },
    Skipped {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        reason: RuntimeFilterJoinProgressSkipReason,
    },
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterFrontierEdgeFacts {
    pub source_fragment_id: u32,
    pub target_exchange_node_id: i32,
}

impl RuntimeFilterFrontierEdgeFacts {
    fn from_sql(edge: &crate::sql::planner::runtime_filter::progress::FrontierEdge) -> Self {
        Self {
            source_fragment_id: edge.source_fragment,
            target_exchange_node_id: edge.target_exchange_node,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterJoinProgressSkipReason {
    NoRfSides,
    MissingChild,
    UnauditedNode { node_id: i32 },
}

impl RuntimeFilterJoinProgressSkipReason {
    fn from_sql(value: crate::sql::planner::runtime_filter::progress::FrontierSkip) -> Self {
        match value {
            crate::sql::planner::runtime_filter::progress::FrontierSkip::NoRfSides => {
                Self::NoRfSides
            }
            crate::sql::planner::runtime_filter::progress::FrontierSkip::MissingChild => {
                Self::MissingChild
            }
            crate::sql::planner::runtime_filter::progress::FrontierSkip::UnauditedNode {
                node_id,
            } => Self::UnauditedNode { node_id },
        }
    }
}
