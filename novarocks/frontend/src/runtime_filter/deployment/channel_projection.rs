//! Frontend-local projection of scheduled runtime-filter channel facts.
//!
//! This module deliberately accepts scalar and Protocol-shaped facts rather
//! than a SQL graph or a Core install domain.  The Frontend deployment compiler
//! owns the policy that assembles those facts.  The result is the existing
//! Protocol `RuntimeFilterChannelDeployment` carried opaquely by Core and
//! semantically decoded by Backend.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use novarocks_protocol::{common, filter, plan};
use prost::Message;

/// One producer authority owned by one participant.
///
/// `expected_fragment_instances` must be the complete (already frozen) set
/// for this participant and binding.  An aggregator therefore receives its
/// query-global set in one input, while a normal producer receives only its
/// local instances.
#[derive(Clone, Debug)]
pub(crate) struct ProducerInstallInput {
    pub(crate) backend_idx: usize,
    pub(crate) binding_id: u32,
    pub(crate) coverage_witness_id: u32,
    pub(crate) expected_fragment_instances: Vec<common::UniqueId>,
}

/// One consumer authority owned by one participant.
///
/// The profile is a Frontend-compiled physical artifact profile.  Its digest
/// and profile id are transported unchanged; this projection neither imports
/// nor reconstructs Core's artifact domain.
#[derive(Clone, Debug)]
pub(crate) struct ConsumerInstallInput {
    pub(crate) backend_idx: usize,
    pub(crate) binding_id: u32,
    pub(crate) activation: plan::RuntimeFilterConsumerActivation,
    pub(crate) capabilities: Vec<i32>,
    pub(crate) artifact_profile: filter::RuntimeFilterConsumerArtifactProfile,
    pub(crate) route_edge_ids: Vec<u32>,
    pub(crate) expected_fragment_instances: Vec<common::UniqueId>,
}

/// The owner which materializes an outbound artifact for a routed delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboundMaterializationOwner {
    DirectSource,
    Aggregator,
}

impl OutboundMaterializationOwner {
    const fn to_wire(self) -> i32 {
        match self {
            Self::DirectSource => {
                filter::RuntimeFilterOutboundMaterializationOwner::DirectSource as i32
            }
            Self::Aggregator => {
                filter::RuntimeFilterOutboundMaterializationOwner::Aggregator as i32
            }
        }
    }
}

/// A routed consumer delivery used to derive a source participant's outbound
/// materialization group.
///
/// `target_binding_id` identifies the target's consumer profile.  Routes with
/// no materialization (for example producer-to-aggregator transport) are not
/// supplied here; they belong only to the Frontend routing projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChannelRouteFact {
    pub(crate) route_edge_id: u32,
    pub(crate) source_backend_idx: usize,
    pub(crate) target_backend_idx: usize,
    pub(crate) target_binding_id: u32,
    pub(crate) materialization_owner: OutboundMaterializationOwner,
}

/// Complete Frontend-local input for one logical runtime-filter channel.
///
/// The header fields are copied into every participant-local core channel.
/// Producer/consumer entries are already placement-expanded.  Route facts
/// derive outbound materialization groups from target consumer profiles.
#[derive(Clone, Debug)]
pub(crate) struct ChannelInstallInput {
    pub(crate) channel_id: u32,
    pub(crate) logical_domain: filter::RuntimeFilterLogicalDomain,
    pub(crate) lifecycle: i32,
    pub(crate) availability_coverage: filter::RuntimeFilterCoverage,
    pub(crate) terminal_coverage: filter::RuntimeFilterCoverage,
    pub(crate) reduction: plan::RuntimeFilterReductionContract,
    pub(crate) allowed_contribution_kinds: Vec<i32>,
    pub(crate) completion_requirement: i32,
    pub(crate) policy: filter::RuntimeFilterPolicyRequirement,
    pub(crate) core_budget: filter::RuntimeFilterCoreBudget,
    pub(crate) materialization_policy: filter::RuntimeFilterMaterializationPolicy,
    pub(crate) producers: Vec<ProducerInstallInput>,
    pub(crate) consumers: Vec<ConsumerInstallInput>,
    pub(crate) routes: Vec<ChannelRouteFact>,
}

/// Project ordered Frontend-local channel facts into participant-local Core
/// install DTOs.  All ordering is canonical by backend, channel, binding, and
/// route id.  Invalid topology facts fail before an install DTO is emitted.
pub(crate) fn project_channel_installs(
    inputs: impl IntoIterator<Item = ChannelInstallInput>,
) -> Result<BTreeMap<usize, Vec<filter::RuntimeFilterChannelDeployment>>, ChannelProjectionError> {
    let mut inputs = inputs.into_iter().collect::<Vec<_>>();
    inputs.sort_by_key(|input| input.channel_id);

    let mut result = BTreeMap::<usize, BTreeMap<u32, ParticipantChannel>>::new();
    let mut seen_channels = BTreeSet::new();
    for input in inputs {
        validate_header(&input)?;
        if !seen_channels.insert(input.channel_id) {
            return Err(ChannelProjectionError::DuplicateChannel(input.channel_id));
        }

        let mut consumers =
            BTreeMap::<(usize, u32), filter::RuntimeFilterConsumerDeployment>::new();
        for consumer in &input.consumers {
            let deployment = consumer_to_wire(consumer)?;
            let key = (consumer.backend_idx, consumer.binding_id);
            if consumers.insert(key, deployment).is_some() {
                return Err(ChannelProjectionError::DuplicateConsumer {
                    channel_id: input.channel_id,
                    backend_idx: key.0,
                    binding_id: key.1,
                });
            }
        }

        for producer in &input.producers {
            let deployment = producer_to_wire(producer)?;
            let participant = result
                .entry(producer.backend_idx)
                .or_default()
                .entry(input.channel_id)
                .or_insert_with(|| ParticipantChannel::new(&input));
            if participant
                .producers
                .insert(producer.binding_id, deployment)
                .is_some()
            {
                return Err(ChannelProjectionError::DuplicateProducer {
                    channel_id: input.channel_id,
                    backend_idx: producer.backend_idx,
                    binding_id: producer.binding_id,
                });
            }
        }
        for ((backend_idx, binding_id), consumer) in &consumers {
            let participant = result
                .entry(*backend_idx)
                .or_default()
                .entry(input.channel_id)
                .or_insert_with(|| ParticipantChannel::new(&input));
            participant.consumers.insert(*binding_id, consumer.clone());
        }

        let mut seen_route_edges = BTreeSet::new();
        for route in input.routes.iter().copied() {
            if route.route_edge_id == 0 {
                return Err(ChannelProjectionError::ZeroRouteEdge {
                    channel_id: input.channel_id,
                });
            }
            if !seen_route_edges.insert(route.route_edge_id) {
                return Err(ChannelProjectionError::DuplicateRouteEdge {
                    channel_id: input.channel_id,
                    route_edge_id: route.route_edge_id,
                });
            }
            let consumer = consumers
                .get(&(route.target_backend_idx, route.target_binding_id))
                .ok_or(ChannelProjectionError::UnknownRouteConsumer {
                    channel_id: input.channel_id,
                    backend_idx: route.target_backend_idx,
                    binding_id: route.target_binding_id,
                })?;
            let source = result
                .get_mut(&route.source_backend_idx)
                .and_then(|channels| channels.get_mut(&input.channel_id))
                .ok_or(ChannelProjectionError::UnknownRouteSource {
                    channel_id: input.channel_id,
                    backend_idx: route.source_backend_idx,
                })?;
            source.add_outbound_group(
                route,
                consumer
                    .artifact_profile
                    .as_ref()
                    .expect("consumer projection always supplies profile"),
            )?;
        }
    }

    Ok(result
        .into_iter()
        .map(|(backend_idx, channels)| {
            let channels = channels
                .into_values()
                .map(ParticipantChannel::into_wire)
                .collect();
            (backend_idx, channels)
        })
        .collect())
}

#[derive(Clone)]
struct ParticipantChannel {
    header: ChannelHeader,
    producers: BTreeMap<u32, filter::RuntimeFilterProducerDeployment>,
    consumers: BTreeMap<u32, filter::RuntimeFilterConsumerDeployment>,
    outbound_groups: BTreeMap<Vec<u8>, OutboundGroup>,
}

impl ParticipantChannel {
    fn new(input: &ChannelInstallInput) -> Self {
        Self {
            header: ChannelHeader::from(input),
            producers: BTreeMap::new(),
            consumers: BTreeMap::new(),
            outbound_groups: BTreeMap::new(),
        }
    }

    fn add_outbound_group(
        &mut self,
        route: ChannelRouteFact,
        profile: &filter::RuntimeFilterConsumerArtifactProfile,
    ) -> Result<(), ChannelProjectionError> {
        let profile_key = profile.encode_to_vec();
        let group = self
            .outbound_groups
            .entry(profile_key)
            .or_insert_with(|| OutboundGroup {
                owner: route.materialization_owner,
                profile: profile.clone(),
                route_edge_ids: BTreeSet::new(),
            });
        if group.owner != route.materialization_owner {
            return Err(ChannelProjectionError::MaterializationOwnerConflict {
                channel_id: self.header.channel_id,
                profile_id: profile.profile_id.clone(),
            });
        }
        if !group.route_edge_ids.insert(route.route_edge_id) {
            return Err(ChannelProjectionError::DuplicateRouteEdge {
                channel_id: self.header.channel_id,
                route_edge_id: route.route_edge_id,
            });
        }
        Ok(())
    }

    fn into_wire(self) -> filter::RuntimeFilterChannelDeployment {
        filter::RuntimeFilterChannelDeployment {
            channel_id: self.header.channel_id,
            logical_domain: Some(self.header.logical_domain),
            lifecycle: self.header.lifecycle,
            availability_coverage: Some(self.header.availability_coverage),
            terminal_coverage: Some(self.header.terminal_coverage),
            reduction: Some(self.header.reduction),
            allowed_contribution_kinds: self.header.allowed_contribution_kinds,
            completion_requirement: self.header.completion_requirement,
            policy: Some(self.header.policy),
            core_budget: Some(self.header.core_budget),
            materialization_policy: Some(self.header.materialization_policy),
            producers: self.producers.into_values().collect(),
            consumers: self.consumers.into_values().collect(),
            outbound_materialization_groups: self
                .outbound_groups
                .into_values()
                .map(|group| filter::RuntimeFilterOutboundMaterializationGroup {
                    owner: group.owner.to_wire(),
                    artifact_profile: Some(group.profile),
                    route_edge_ids: group.route_edge_ids.into_iter().collect(),
                })
                .collect(),
        }
    }
}

#[derive(Clone)]
struct ChannelHeader {
    channel_id: u32,
    logical_domain: filter::RuntimeFilterLogicalDomain,
    lifecycle: i32,
    availability_coverage: filter::RuntimeFilterCoverage,
    terminal_coverage: filter::RuntimeFilterCoverage,
    reduction: plan::RuntimeFilterReductionContract,
    allowed_contribution_kinds: Vec<i32>,
    completion_requirement: i32,
    policy: filter::RuntimeFilterPolicyRequirement,
    core_budget: filter::RuntimeFilterCoreBudget,
    materialization_policy: filter::RuntimeFilterMaterializationPolicy,
}

impl From<&ChannelInstallInput> for ChannelHeader {
    fn from(input: &ChannelInstallInput) -> Self {
        Self {
            channel_id: input.channel_id,
            logical_domain: input.logical_domain.clone(),
            lifecycle: input.lifecycle,
            availability_coverage: input.availability_coverage.clone(),
            terminal_coverage: input.terminal_coverage.clone(),
            reduction: input.reduction.clone(),
            allowed_contribution_kinds: input.allowed_contribution_kinds.clone(),
            completion_requirement: input.completion_requirement,
            policy: input.policy.clone(),
            core_budget: input.core_budget.clone(),
            materialization_policy: input.materialization_policy.clone(),
        }
    }
}

#[derive(Clone)]
struct OutboundGroup {
    owner: OutboundMaterializationOwner,
    profile: filter::RuntimeFilterConsumerArtifactProfile,
    route_edge_ids: BTreeSet<u32>,
}

fn validate_header(input: &ChannelInstallInput) -> Result<(), ChannelProjectionError> {
    if input.channel_id == 0 {
        return Err(ChannelProjectionError::ZeroChannel);
    }
    if input.logical_domain.value_type.is_none() || input.logical_domain.contract.is_none() {
        return Err(ChannelProjectionError::MissingLogicalDomain {
            channel_id: input.channel_id,
        });
    }
    if input.availability_coverage.kind.is_none() || input.terminal_coverage.kind.is_none() {
        return Err(ChannelProjectionError::MissingCoverage {
            channel_id: input.channel_id,
        });
    }
    if input.reduction.kind.is_none() {
        return Err(ChannelProjectionError::MissingReduction {
            channel_id: input.channel_id,
        });
    }
    if input.policy.max_contribution_bytes == 0
        || input.policy.max_artifact_bytes == 0
        || input.policy.deadline_ms == 0
        || input.core_budget.max_reducer_bytes == 0
        || input.materialization_policy.max_concurrent_jobs == 0
    {
        return Err(ChannelProjectionError::InvalidPolicy {
            channel_id: input.channel_id,
        });
    }
    Ok(())
}

fn producer_to_wire(
    input: &ProducerInstallInput,
) -> Result<filter::RuntimeFilterProducerDeployment, ChannelProjectionError> {
    if input.binding_id == 0 || input.coverage_witness_id == 0 {
        return Err(ChannelProjectionError::InvalidProducer {
            backend_idx: input.backend_idx,
            binding_id: input.binding_id,
        });
    }
    Ok(filter::RuntimeFilterProducerDeployment {
        binding_id: input.binding_id,
        coverage_witness_id: input.coverage_witness_id,
        expected_fragment_instances: unique_instances(
            input.backend_idx,
            input.binding_id,
            &input.expected_fragment_instances,
        )?,
    })
}

fn consumer_to_wire(
    input: &ConsumerInstallInput,
) -> Result<filter::RuntimeFilterConsumerDeployment, ChannelProjectionError> {
    if input.binding_id == 0
        || input.capabilities.is_empty()
        || input.artifact_profile.profile_id.is_empty()
        || input.route_edge_ids.is_empty()
    {
        return Err(ChannelProjectionError::InvalidConsumer {
            backend_idx: input.backend_idx,
            binding_id: input.binding_id,
        });
    }
    let route_edge_ids = input
        .route_edge_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if route_edge_ids.len() != input.route_edge_ids.len() || route_edge_ids.contains(&0) {
        return Err(ChannelProjectionError::InvalidConsumer {
            backend_idx: input.backend_idx,
            binding_id: input.binding_id,
        });
    }
    Ok(filter::RuntimeFilterConsumerDeployment {
        binding_id: input.binding_id,
        activation: Some(input.activation.clone()),
        capabilities: input.capabilities.clone(),
        artifact_profile: Some(input.artifact_profile.clone()),
        route_edge_ids: route_edge_ids.into_iter().collect(),
        expected_fragment_instances: unique_instances(
            input.backend_idx,
            input.binding_id,
            &input.expected_fragment_instances,
        )?,
    })
}

fn unique_instances(
    backend_idx: usize,
    binding_id: u32,
    instances: &[common::UniqueId],
) -> Result<Vec<common::UniqueId>, ChannelProjectionError> {
    let values = instances
        .iter()
        .map(|id| (id.hi, id.lo))
        .collect::<BTreeSet<_>>();
    if values.is_empty() || values.len() != instances.len() {
        return Err(ChannelProjectionError::InvalidInstances {
            backend_idx,
            binding_id,
        });
    }
    Ok(values
        .into_iter()
        .map(|(hi, lo)| common::UniqueId { hi, lo })
        .collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChannelProjectionError {
    ZeroChannel,
    DuplicateChannel(u32),
    MissingLogicalDomain {
        channel_id: u32,
    },
    MissingCoverage {
        channel_id: u32,
    },
    MissingReduction {
        channel_id: u32,
    },
    InvalidPolicy {
        channel_id: u32,
    },
    InvalidProducer {
        backend_idx: usize,
        binding_id: u32,
    },
    InvalidConsumer {
        backend_idx: usize,
        binding_id: u32,
    },
    InvalidInstances {
        backend_idx: usize,
        binding_id: u32,
    },
    DuplicateProducer {
        channel_id: u32,
        backend_idx: usize,
        binding_id: u32,
    },
    DuplicateConsumer {
        channel_id: u32,
        backend_idx: usize,
        binding_id: u32,
    },
    ZeroRouteEdge {
        channel_id: u32,
    },
    DuplicateRouteEdge {
        channel_id: u32,
        route_edge_id: u32,
    },
    UnknownRouteConsumer {
        channel_id: u32,
        backend_idx: usize,
        binding_id: u32,
    },
    UnknownRouteSource {
        channel_id: u32,
        backend_idx: usize,
    },
    MaterializationOwnerConflict {
        channel_id: u32,
        profile_id: Vec<u8>,
    },
}

impl fmt::Display for ChannelProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter channel install projection failed: {self:?}"
        )
    }
}

impl std::error::Error for ChannelProjectionError {}

#[cfg(test)]
mod tests {
    use novarocks_protocol::{common, filter, plan};

    use super::{
        ChannelInstallInput, ChannelProjectionError, ChannelRouteFact, ConsumerInstallInput,
        OutboundMaterializationOwner, ProducerInstallInput, project_channel_installs,
    };

    fn instance(lo: i64) -> common::UniqueId {
        common::UniqueId { hi: 1, lo }
    }

    fn profile() -> filter::RuntimeFilterConsumerArtifactProfile {
        filter::RuntimeFilterConsumerArtifactProfile {
            accepted_kinds: vec![filter::RuntimeFilterArtifactKind::ValueSet as i32],
            bloom_hash_contract: None,
            order_contract_digest: None,
            profile_id: vec![7; 32],
        }
    }

    fn channel() -> ChannelInstallInput {
        ChannelInstallInput {
            channel_id: 7,
            logical_domain: filter::RuntimeFilterLogicalDomain {
                value_type: Some(common::TypeDesc {
                    kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                        r#type: common::PrimitiveType::Bigint as i32,
                        ..Default::default()
                    })),
                }),
                contract: Some(plan::RuntimeFilterContract {
                    kind: Some(plan::runtime_filter_contract::Kind::Membership(
                        plan::RuntimeFilterMembershipContract {
                            canonical_schema: vec![1],
                            schema_digest: vec![2; 32],
                        },
                    )),
                }),
            },
            lifecycle: filter::RuntimeFilterLifecycle::CompleteOnce as i32,
            availability_coverage: filter::RuntimeFilterCoverage {
                kind: Some(filter::runtime_filter_coverage::Kind::LeafWitnessId(11)),
            },
            terminal_coverage: filter::RuntimeFilterCoverage {
                kind: Some(filter::runtime_filter_coverage::Kind::LeafWitnessId(11)),
            },
            reduction: plan::RuntimeFilterReductionContract {
                kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true,
                )),
            },
            allowed_contribution_kinds: vec![
                plan::RuntimeFilterContributionKind::ValueDomainDelta as i32,
                plan::RuntimeFilterContributionKind::ProducerClosed as i32,
            ],
            completion_requirement: plan::RuntimeFilterCompletionRequirement::ProducerClosed as i32,
            policy: filter::RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 100,
                max_retries: 2,
            },
            core_budget: filter::RuntimeFilterCoreBudget {
                max_reducer_bytes: 128,
            },
            materialization_policy: filter::RuntimeFilterMaterializationPolicy {
                bloom_bits_per_key: 8,
                bloom_hash_count: 5,
                bloom_seed: 17,
                bloom_algorithm_version: 1,
                max_total_retained_bytes: 128,
                max_scratch_bytes_per_job: 64,
                max_concurrent_jobs: 1,
            },
            producers: vec![ProducerInstallInput {
                backend_idx: 2,
                binding_id: 10,
                coverage_witness_id: 11,
                expected_fragment_instances: vec![instance(4)],
            }],
            consumers: vec![ConsumerInstallInput {
                backend_idx: 3,
                binding_id: 20,
                activation: plan::RuntimeFilterConsumerActivation {
                    kind: Some(
                        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true),
                    ),
                },
                capabilities: vec![plan::RuntimeFilterArtifactCapability::Membership as i32],
                artifact_profile: profile(),
                route_edge_ids: vec![31],
                expected_fragment_instances: vec![instance(5)],
            }],
            routes: vec![ChannelRouteFact {
                route_edge_id: 31,
                source_backend_idx: 2,
                target_backend_idx: 3,
                target_binding_id: 20,
                materialization_owner: OutboundMaterializationOwner::DirectSource,
            }],
        }
    }

    #[test]
    fn projects_headers_bindings_and_outbound_groups_in_canonical_order() {
        let projected = project_channel_installs([channel()]).expect("projection succeeds");
        let source = &projected[&2][0];
        assert_eq!(source.channel_id, 7);
        assert_eq!(source.producers.len(), 1);
        assert_eq!(source.outbound_materialization_groups.len(), 1);
        assert_eq!(
            source.outbound_materialization_groups[0].route_edge_ids,
            vec![31]
        );
        let target = &projected[&3][0];
        assert_eq!(target.consumers.len(), 1);
        assert_eq!(target.consumers[0].route_edge_ids, vec![31]);
    }

    #[test]
    fn rejects_route_without_a_target_consumer() {
        let mut input = channel();
        input.routes[0].target_binding_id = 99;
        assert!(matches!(
            project_channel_installs([input]),
            Err(ChannelProjectionError::UnknownRouteConsumer { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_participant_binding_authority() {
        let mut input = channel();
        input.producers.push(input.producers[0].clone());
        assert!(matches!(
            project_channel_installs([input]),
            Err(ChannelProjectionError::DuplicateProducer { .. })
        ));
    }
}
