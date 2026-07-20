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

//! Query-scoped production ingress for inbound runtime-filter producer envelopes.
//!
//! This is the thin seam between the gRPC `RuntimeFilterEnvelope` wire adapter
//! and a live query's `RuntimeFilterService`. It does exactly three things and
//! reaches no further into the runtime-filter internals:
//!
//! 1. look up the query's already-installed service (lookup-only: it never
//!    creates, revives, or renews a query),
//! 2. dispatch the decoded producer envelope into that service, and
//! 3. map the typed dispatch result back onto the transport-level
//!    `RuntimeFilterIngressResult`.
//!
//! On a lookup miss it answers with a stable adapter-owned `[query-unavailable]`
//! rejection whose shape matches the typed Core dispatch-error taxonomy, so
//! producers observe one uniform rejection surface.

use std::sync::Arc;

use crate::runtime::query_context::{QueryContextManager, QueryId, query_context_manager};
use crate::runtime_filter::port::transport::{
    RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress, RuntimeFilterIngressResult,
};
use crate::runtime_filter::service::{
    InboundProducerDispatchError, InboundProducerDispatchOutcome,
};

// Adapter-owned rejection for a query that is neither active nor within delivery
// grace. It mirrors the typed dispatch-error Display shape
// (`runtime filter ingress rejected [<prefix>]: <detail>`) so the query-miss case
// and the six typed Core dispatch errors surface under one rejection taxonomy.
const QUERY_UNAVAILABLE_REJECTION: &str = "runtime filter ingress rejected [query-unavailable]: \
     runtime filter query is not active or in delivery grace";

/// Production ingress bound to the process-global query context manager.
pub(crate) fn query_scoped_runtime_filter_envelope_ingress() -> Arc<dyn RuntimeFilterEnvelopeIngress>
{
    Arc::new(QueryScopedRuntimeFilterEnvelopeIngress {
        manager: query_context_manager(),
    })
}

/// Component-test constructor that binds the ingress to an isolated manager so a
/// test can register and install its own query without touching global state.
#[cfg(test)]
pub(crate) fn query_scoped_runtime_filter_envelope_ingress_with_manager(
    manager: Arc<QueryContextManager>,
) -> Arc<dyn RuntimeFilterEnvelopeIngress> {
    Arc::new(QueryScopedRuntimeFilterEnvelopeIngress { manager })
}

struct QueryScopedRuntimeFilterEnvelopeIngress {
    manager: Arc<QueryContextManager>,
}

impl RuntimeFilterEnvelopeIngress for QueryScopedRuntimeFilterEnvelopeIngress {
    fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
        let query_id = QueryId {
            hi: envelope.query_id().hi,
            lo: envelope.query_id().lo,
        };
        let Some(service) = self.manager.runtime_filter_service_for_ingress(query_id) else {
            return RuntimeFilterIngressResult::rejected(QUERY_UNAVAILABLE_REJECTION)
                .expect("query-unavailable reason is non-empty");
        };
        ingress_result_for_dispatch(service.dispatch_inbound_producer(envelope))
    }
}

/// Maps a typed inbound producer dispatch result onto the transport ingress
/// result. This is the single mapping the adapter performs after a live service
/// is found; the six stable dispatch-error prefixes flow through unchanged via
/// the error's `Display`.
fn ingress_result_for_dispatch(
    dispatched: Result<InboundProducerDispatchOutcome, InboundProducerDispatchError>,
) -> RuntimeFilterIngressResult {
    match dispatched {
        Ok(InboundProducerDispatchOutcome::Accepted) => RuntimeFilterIngressResult::accepted(),
        Ok(InboundProducerDispatchOutcome::Duplicate) => RuntimeFilterIngressResult::duplicate(),
        Err(error) => RuntimeFilterIngressResult::rejected(error.to_string())
            .expect("typed inbound dispatch error has a non-empty reason"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::datatypes::DataType;

    use super::{
        ingress_result_for_dispatch, query_scoped_runtime_filter_envelope_ingress_with_manager,
    };
    use crate::common::types::UniqueId;
    use crate::proto;
    use crate::runtime::query_context::{QueryContextManager, QueryId};
    use crate::runtime_filter::codec::contribution::{
        ContributionCodecExpectation, RuntimeFilterContribution, encode_contribution,
    };
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, NullSemantics, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, MaterializationPolicy, ProducerDeployment,
        RuntimeFilterChannelDeployment, RuntimeFilterCoreBudget, RuntimeFilterInstallView,
        RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::producer::InstallOutcome;
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
        RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
    };
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, ProducerOpenMetadata, RuntimeFilterAcceptStatus,
        RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind, RuntimeFilterIngressResult,
        RuntimeFilterRouteIdentity,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
    use crate::runtime_filter::service::{
        InboundProducerDispatchError, InboundProducerDispatchErrorKind,
    };
    use crate::service::grpc_runtime_filter_adapter::handle_runtime_filter_envelope;

    // The registered query the adapter looks up. Its `UniqueId` projection is the
    // envelope query id; `hi`/`lo` are arbitrary non-zero coordinates.
    const QUERY: QueryId = QueryId { hi: 71, lo: 72 };
    const QUERY_UID: UniqueId = UniqueId { hi: 71, lo: 72 };
    // Loopback install coordinates (fixed epoch 9 / participant 3 mirror the
    // `runtime_filter::service` loopback-install fixture).
    const EPOCH: u64 = 9;
    const CHANNEL: u32 = 1;
    const PRODUCER_BINDING: u32 = 1;
    const CONSUMER_BINDING: u32 = 2;
    const WITNESS: u32 = 11;
    const PRODUCER_FINST: UniqueId = UniqueId { hi: 1, lo: 2 };
    const CONSUMER_FINST: UniqueId = UniqueId { hi: 1, lo: 3 };

    const QUERY_UNAVAILABLE_REASON: &str = "runtime filter ingress rejected [query-unavailable]: \
         runtime filter query is not active or in delivery grace";

    // --- manager / install scaffolding --------------------------------------------------------

    fn registered_manager() -> Arc<QueryContextManager> {
        let manager = QueryContextManager::new_for_test();
        manager
            .get_or_register_native(
                QUERY,
                false,
                Duration::from_secs(30),
                Duration::from_secs(30),
            )
            .expect("register native query context");
        manager
    }

    fn installed_manager() -> Arc<QueryContextManager> {
        let manager = registered_manager();
        let service = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("registered query exposes a runtime filter service");
        assert_eq!(
            service
                .install(loopback_membership_install(membership_deployment(4096)))
                .expect("valid loopback install"),
            InstallOutcome::Installed,
        );
        manager
    }

    fn membership_consumer() -> ConsumerDeployment {
        ConsumerDeployment::new(
            ConsumerActivation::BlockingSnapshot,
            BTreeSet::from([ArtifactCapability::Membership]),
            RouteEdgeId::new(40),
            BTreeSet::from([CONSUMER_FINST]),
        )
    }

    fn membership_deployment(max_contribution_bytes: u64) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(WITNESS);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(CHANNEL),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(PRODUCER_BINDING),
                ProducerDeployment::new(witness, BTreeSet::from([PRODUCER_FINST])),
            )]),
            BTreeMap::from([(BindingId::new(CONSUMER_BINDING), membership_consumer())]),
        )
    }

    // Production-shaped local loopback install: an explicit producer -> aggregator
    // inbound edge per producer binding, which is what lets `dispatch_inbound_producer`
    // authorize a contribution and reach the real Core.
    fn loopback_membership_install(
        channel: RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterParticipantInstall {
        let epoch = DeploymentEpoch::new(EPOCH);
        let participant = RuntimeFilterParticipantId::new(3);
        let channel_id = channel.channel_id();
        let mut local_roles = BTreeSet::from([RuntimeFilterRouteRole::Aggregator]);
        let mut producer_instances = BTreeMap::new();
        let mut inbound_edges = Vec::new();
        let mut outbound_edges = Vec::new();
        for (index, (binding_id, producer)) in channel.producers().iter().enumerate() {
            local_roles.insert(RuntimeFilterRouteRole::Producer(*binding_id));
            for fragment_instance_id in producer.expected_fragment_instances() {
                producer_instances.insert((*binding_id, *fragment_instance_id), participant);
            }
            let edge = RuntimeFilterRoutingEdgeView::new(
                channel_id,
                RouteEdgeId::new(u32::try_from(index).unwrap() + 1),
                RuntimeFilterRouteEndpointView::new(
                    participant,
                    RuntimeFilterRouteRole::Producer(*binding_id),
                ),
                RuntimeFilterRouteEndpointView::new(
                    participant,
                    RuntimeFilterRouteRole::Aggregator,
                ),
                RuntimeFilterRoutePeer::Loopback,
                BTreeSet::from([
                    RuntimeFilterEnvelopeKind::Contribution,
                    RuntimeFilterEnvelopeKind::ProducerClosed,
                ]),
            )
            .unwrap();
            inbound_edges.push(edge.clone());
            outbound_edges.push(edge);
        }
        local_roles.extend(
            channel
                .consumers()
                .keys()
                .copied()
                .map(RuntimeFilterRouteRole::Consumer),
        );
        let routing_channel = RuntimeFilterChannelRoutingView::new(
            channel_id,
            local_roles,
            producer_instances,
            inbound_edges,
            outbound_edges,
        )
        .unwrap();
        let routing_shard = RuntimeFilterRoutingShard::new(
            epoch,
            participant,
            BTreeMap::from([(channel_id, routing_channel)]),
        )
        .unwrap();
        let core_view = RuntimeFilterInstallView::new(
            epoch,
            participant,
            BTreeMap::from([(channel_id, channel)]),
        );
        RuntimeFilterParticipantInstall::new(core_view, routing_shard)
    }

    // --- contribution / envelope builders -----------------------------------------------------

    fn membership_schema() -> ArtifactMembershipSchema {
        ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NullSafeEqual).unwrap()
    }

    fn membership_contribution(value: i64) -> RuntimeFilterContribution {
        RuntimeFilterContribution::Membership(ValueDomainDelta::new(
            MembershipValues::int64([value]),
            false,
        ))
    }

    fn encode_membership(value: i64) -> ([u8; 32], Vec<u8>) {
        encode_contribution(
            &membership_contribution(value),
            ContributionCodecExpectation::Membership(&membership_schema()),
            usize::MAX,
        )
        .unwrap()
        .into_parts()
    }

    #[allow(clippy::too_many_arguments)]
    fn producer_envelope(
        kind: RuntimeFilterEnvelopeKind,
        epoch: u64,
        binding: u32,
        fragment_instance_id: UniqueId,
        partition: u32,
        sequence: u64,
        producer_open: Option<u32>,
        schema_digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        let route = ContributionRouteIdentity::try_new(
            BindingId::new(binding),
            fragment_instance_id,
            PartitionId::new(partition),
            ProducerSequence::new(sequence),
        )
        .expect("valid contribution route identity");
        RuntimeFilterEnvelope::try_new(
            kind,
            QUERY_UID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(epoch),
            RuntimeFilterRouteIdentity::contribution(route),
            producer_open.map(|count| {
                ProducerOpenMetadata::try_new(count).expect("nonzero partition count")
            }),
            &schema_digest,
            payload,
        )
        .expect("valid producer envelope")
    }

    fn contribution_envelope(
        partition: u32,
        sequence: u64,
        count: u32,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        producer_envelope(
            RuntimeFilterEnvelopeKind::Contribution,
            EPOCH,
            PRODUCER_BINDING,
            PRODUCER_FINST,
            partition,
            sequence,
            Some(count),
            digest,
            payload,
        )
    }

    fn closed_envelope(
        epoch: u64,
        binding: u32,
        partition: u32,
        sequence: u64,
        count: u32,
        digest: [u8; 32],
    ) -> RuntimeFilterEnvelope {
        producer_envelope(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            epoch,
            binding,
            PRODUCER_FINST,
            partition,
            sequence,
            Some(count),
            digest,
            Vec::new(),
        )
    }

    fn assert_rejected_prefix(result: &RuntimeFilterIngressResult, prefix: &str) {
        assert_eq!(
            result.accept_status(),
            RuntimeFilterAcceptStatus::Rejected,
            "expected a rejection carrying {prefix}"
        );
        let reason = result
            .rejection_reason()
            .expect("a rejected ingress result carries a reason");
        assert!(
            reason.starts_with("runtime filter ingress rejected "),
            "reason {reason:?} must carry the ingress rejection shape"
        );
        assert!(
            reason.contains(prefix),
            "reason {reason:?} must carry the {prefix} prefix"
        );
    }

    // --- tests --------------------------------------------------------------------------------

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_query_miss_is_rejected_query_unavailable() {
        // Empty manager: QUERY is never registered.
        let manager = QueryContextManager::new_for_test();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let result = ingress.accept(closed_envelope(EPOCH, PRODUCER_BINDING, 0, 0, 1, [0; 32]));

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Rejected);
        assert_eq!(result.rejection_reason(), Some(QUERY_UNAVAILABLE_REASON));
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_installed_query_accepts_membership_contribution()
     {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        let (digest, payload) = encode_membership(7);

        let result = ingress.accept(contribution_envelope(0, 0, 1, digest, payload));

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Accepted);
        assert_eq!(result.rejection_reason(), None);
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_exact_replay_is_duplicate() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        let (digest, payload) = encode_membership(7);

        assert_eq!(
            ingress
                .accept(contribution_envelope(0, 0, 1, digest, payload.clone()))
                .accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );
        assert_eq!(
            ingress
                .accept(contribution_envelope(0, 0, 1, digest, payload))
                .accept_status(),
            RuntimeFilterAcceptStatus::Duplicate,
        );
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_uninstalled_query_is_deployment_unavailable() {
        // Registered but never installed: dispatch reaches the service and fails
        // fast under the deployment-unavailable prefix.
        let manager = registered_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let result = ingress.accept(contribution_envelope(0, 0, 1, [0; 32], vec![1]));

        assert_rejected_prefix(&result, "[deployment-unavailable]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_stale_epoch_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let result = ingress.accept(closed_envelope(
            EPOCH - 1,
            PRODUCER_BINDING,
            0,
            0,
            1,
            [0; 32],
        ));

        assert_rejected_prefix(&result, "[stale-epoch]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_route_contract_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        // Unknown producer binding: authorization rejects before the codec step.
        let result = ingress.accept(closed_envelope(
            EPOCH,
            PRODUCER_BINDING + 100,
            0,
            0,
            1,
            [0; 32],
        ));

        assert_rejected_prefix(&result, "[route-contract]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_codec_contract_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        // Correctly-routed contribution with a non-NRFC (bad-magic) payload frame.
        let result = ingress.accept(contribution_envelope(0, 0, 1, [0; 32], vec![0u8; 20]));

        assert_rejected_prefix(&result, "[codec-contract]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_producer_contract_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        // The producer-close digest matches the installed membership contract, so
        // dispatch clears the codec gate and rejects on the invalid partition id.
        let (digest, _payload) = encode_membership(7);

        let result = ingress.accept(closed_envelope(EPOCH, PRODUCER_BINDING, 5, 0, 1, digest));

        assert_rejected_prefix(&result, "[producer-contract]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_service_unavailable_prefix() {
        // ServiceUnavailable is unreachable through the live producer-submit path
        // (dispatch holds an active installation), so exercise the same mapping the
        // adapter performs on any typed dispatch error.
        let result = ingress_result_for_dispatch(Err(InboundProducerDispatchError::new(
            InboundProducerDispatchErrorKind::ServiceUnavailable,
            "runtime filter service is uninstalled or cancelled",
        )));

        assert_rejected_prefix(&result, "[service-unavailable]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_wire_malformed_is_invalid_argument_before_lookup()
     {
        // A wire-malformed protobuf (default => Unspecified kind) fails wire
        // validation before the adapter is consulted. If the query-scoped adapter
        // (bound to an empty manager) were reached, it would return a normal
        // query-unavailable rejection response instead of a gRPC error.
        let manager = QueryContextManager::new_for_test();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let error = handle_runtime_filter_envelope(
            ingress,
            proto::filter::RuntimeFilterEnvelope::default(),
        )
        .expect_err("malformed wire envelope must be an InvalidArgument gRPC error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
