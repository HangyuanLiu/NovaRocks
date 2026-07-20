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

use std::error::Error;
use std::fmt;

use crate::runtime_filter::codec::contribution::{
    ContributionCodecExpectation, RuntimeFilterContribution, decode_contribution,
    semantic_contribution_bytes,
};
use crate::runtime_filter::port::identity::ProducerStreamId;
use crate::runtime_filter::port::producer::{
    ProducerHandle, RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::routing::RuntimeFilterRouteContractError;
use crate::runtime_filter::port::transport::{RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind};

use super::registry::InboundProducerContract;
use super::{OpenedProducer, RuntimeFilterService};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundProducerDispatchOutcome {
    Accepted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundProducerDispatchErrorKind {
    DeploymentUnavailable,
    StaleEpoch,
    RouteContract,
    CodecContract,
    ProducerContract,
    ServiceUnavailable,
}

impl InboundProducerDispatchErrorKind {
    pub(crate) const fn prefix(self) -> &'static str {
        match self {
            Self::DeploymentUnavailable => "[deployment-unavailable]",
            Self::StaleEpoch => "[stale-epoch]",
            Self::RouteContract => "[route-contract]",
            Self::CodecContract => "[codec-contract]",
            Self::ProducerContract => "[producer-contract]",
            Self::ServiceUnavailable => "[service-unavailable]",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboundProducerDispatchError {
    kind: InboundProducerDispatchErrorKind,
    detail: String,
}

impl InboundProducerDispatchError {
    pub(crate) fn new(kind: InboundProducerDispatchErrorKind, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        assert!(
            !detail.is_empty(),
            "inbound rejection detail must not be empty"
        );
        Self { kind, detail }
    }

    pub(crate) const fn kind(&self) -> InboundProducerDispatchErrorKind {
        self.kind
    }
}

impl fmt::Display for InboundProducerDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter ingress rejected {}: {}",
            self.kind.prefix(),
            self.detail
        )
    }
}

impl Error for InboundProducerDispatchError {}

impl RuntimeFilterService {
    pub(crate) fn dispatch_inbound_producer(
        &self,
        envelope: RuntimeFilterEnvelope,
    ) -> Result<InboundProducerDispatchOutcome, InboundProducerDispatchError> {
        let route = envelope.route_identity().as_contribution().ok_or_else(|| {
            ingress_error(
                InboundProducerDispatchErrorKind::RouteContract,
                "producer envelope requires contribution identity",
            )
        })?;
        let operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = self.registry.active_installation().ok_or_else(|| {
            ingress_error(
                InboundProducerDispatchErrorKind::DeploymentUnavailable,
                "runtime filter deployment is not active",
            )
        })?;
        installed
            .role_router()
            .authorize_contribution(
                envelope.deployment_epoch(),
                envelope.channel_id(),
                route.producer_binding_id(),
                route.fragment_instance_id(),
                envelope.kind(),
            )
            .map_err(map_route_error)?;
        let producer = installed
            .producer(route.producer_binding_id())
            .ok_or_else(|| {
                ingress_error(
                    InboundProducerDispatchErrorKind::RouteContract,
                    "producer binding is not installed",
                )
            })?;
        if producer.channel_id() != envelope.channel_id() {
            return Err(ingress_error(
                InboundProducerDispatchErrorKind::RouteContract,
                "producer binding belongs to another channel",
            ));
        }

        let local_partition_count = envelope
            .producer_open()
            .ok_or_else(|| {
                ingress_error(
                    InboundProducerDispatchErrorKind::RouteContract,
                    "producer envelope is missing open metadata",
                )
            })?
            .local_partition_count()
            .get();
        let contract = producer.inbound_contract();
        let contribution = match envelope.kind() {
            RuntimeFilterEnvelopeKind::Contribution => {
                let expectation = contribution_expectation(
                    contract,
                    route.producer_binding_id(),
                    route.fragment_instance_id(),
                    route.partition_id(),
                    route.sequence(),
                );
                let contribution = decode_contribution(
                    envelope.payload(),
                    envelope.schema_digest(),
                    expectation,
                    contract.limits().max_encoded_bytes(),
                )
                .map_err(map_codec_error)?;
                let semantic_bytes =
                    semantic_contribution_bytes(&contribution).map_err(map_codec_error)?;
                if semantic_bytes > contract.limits().max_contribution_bytes() {
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::CodecContract,
                        "contribution exceeds the installed semantic byte budget",
                    ));
                }
                Some(contribution)
            }
            RuntimeFilterEnvelopeKind::ProducerClosed => {
                if !envelope.payload().is_empty() {
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::CodecContract,
                        "producer-close envelope must not carry a payload",
                    ));
                }
                if envelope.schema_digest() != &contract.schema_digest() {
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::CodecContract,
                        "producer-close digest does not match the installed producer contract",
                    ));
                }
                None
            }
            _ => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::RouteContract,
                    "envelope kind is not valid for producer ingress",
                ));
            }
        };

        producer
            .channel
            .preflight_remote_open(
                route.producer_binding_id(),
                route.fragment_instance_id(),
                local_partition_count,
                route.partition_id(),
            )
            .map_err(map_producer_error)?;
        let OpenedProducer { handle, outcome } = self
            .open_producer_locked(
                &installed,
                route.producer_binding_id(),
                route.fragment_instance_id(),
                local_partition_count,
                contract.port_kind(),
            )
            .map_err(map_producer_error)?;
        if outcome == SubmitOutcome::TerminalNoop {
            drop(operation);
            return Ok(InboundProducerDispatchOutcome::Accepted);
        }
        let partition_id = route.partition_id();
        let sequence = route.sequence();
        drop(operation);

        let outcome = match (handle, contribution) {
            (
                ProducerHandle::Membership(handle),
                Some(RuntimeFilterContribution::Membership(delta)),
            ) => handle.submit(partition_id, sequence, delta),
            (
                ProducerHandle::OrderedBound(handle),
                Some(RuntimeFilterContribution::OrderedBound(update)),
            ) => handle.submit_bound(partition_id, sequence, update),
            (
                ProducerHandle::TopKSummary(handle),
                Some(RuntimeFilterContribution::TopKSummary(summary)),
            ) => handle.submit_summary(partition_id, sequence, summary),
            (
                ProducerHandle::FinalDomain(handle),
                Some(RuntimeFilterContribution::FinalDomain(shard)),
            ) => handle.complete(partition_id, sequence, shard),
            (ProducerHandle::Membership(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            (ProducerHandle::OrderedBound(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            (ProducerHandle::TopKSummary(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            (ProducerHandle::FinalDomain(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            _ => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::ProducerContract,
                    "decoded contribution does not match the installed producer port",
                ));
            }
        }
        .map_err(map_producer_error)?;
        Ok(dispatch_outcome(outcome))
    }
}

fn contribution_expectation<'a>(
    contract: &'a InboundProducerContract,
    binding_id: crate::runtime_filter::model::contract::BindingId,
    fragment_instance_id: crate::common::types::UniqueId,
    partition_id: crate::runtime_filter::port::identity::PartitionId,
    sequence: crate::runtime_filter::port::identity::ProducerSequence,
) -> ContributionCodecExpectation<'a> {
    match contract {
        InboundProducerContract::Membership { schema, .. } => {
            ContributionCodecExpectation::Membership(schema)
        }
        InboundProducerContract::OrderedBound { contract, .. } => {
            ContributionCodecExpectation::OrderedBound(contract)
        }
        InboundProducerContract::TopKSummary { contract, .. } => {
            ContributionCodecExpectation::TopKSummary(contract)
        }
        InboundProducerContract::FinalDomain { contract, .. } => {
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream: ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
                sequence,
            }
        }
    }
}

fn ingress_error(
    kind: InboundProducerDispatchErrorKind,
    detail: impl Into<String>,
) -> InboundProducerDispatchError {
    InboundProducerDispatchError::new(kind, detail)
}

fn map_route_error(error: RuntimeFilterRouteContractError) -> InboundProducerDispatchError {
    let kind = if matches!(error, RuntimeFilterRouteContractError::StaleEpoch { .. }) {
        InboundProducerDispatchErrorKind::StaleEpoch
    } else {
        InboundProducerDispatchErrorKind::RouteContract
    };
    ingress_error(kind, error.to_string())
}

fn map_codec_error(
    error: crate::runtime_filter::codec::contribution::ContributionCodecError,
) -> InboundProducerDispatchError {
    ingress_error(
        InboundProducerDispatchErrorKind::CodecContract,
        error.to_string(),
    )
}

fn map_producer_error(error: RuntimeContractViolation) -> InboundProducerDispatchError {
    let kind = if error.kind() == RuntimeContractViolationKind::ServiceUnavailable {
        InboundProducerDispatchErrorKind::ServiceUnavailable
    } else {
        InboundProducerDispatchErrorKind::ProducerContract
    };
    ingress_error(kind, error.to_string())
}

const fn dispatch_outcome(outcome: SubmitOutcome) -> InboundProducerDispatchOutcome {
    if matches!(outcome, SubmitOutcome::Duplicate) {
        InboundProducerDispatchOutcome::Duplicate
    } else {
        InboundProducerDispatchOutcome::Accepted
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::{DeploymentEpoch, PartitionId, ProducerSequence};
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, ProducerOpenMetadata, RuntimeFilterEnvelope,
        RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity,
    };

    use super::{InboundProducerDispatchOutcome, RuntimeFilterService};

    struct Clock;
    impl RuntimeFilterClock for Clock {
        fn now(&self) -> Instant {
            Instant::now()
        }
    }
    struct Events;
    impl RuntimeFilterEventSink for Events {
        fn record(&self, _: RuntimeFilterEvent) {}
    }
    struct Memory;
    impl RuntimeFilterMemoryAccount for Memory {
        fn try_consume(&self, _: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }
        fn release(&self, _: usize) {}
    }

    #[test]
    fn inbound_producer_dispatch_requires_an_active_installed_route() {
        let service = RuntimeFilterService::new_with_dependencies(
            UniqueId { hi: 1, lo: 1 },
            Arc::new(Clock),
            Arc::new(Events),
            Arc::new(Memory),
        );
        let route = ContributionRouteIdentity::try_new(
            BindingId::new(1),
            UniqueId { hi: 1, lo: 2 },
            PartitionId::new(0),
            ProducerSequence::new(0),
        )
        .unwrap();
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Contribution,
            UniqueId { hi: 1, lo: 1 },
            ChannelId::new(1),
            DeploymentEpoch::new(1),
            RuntimeFilterRouteIdentity::contribution(route),
            Some(ProducerOpenMetadata::try_new(1).unwrap()),
            &[0; 32],
            vec![1],
        )
        .unwrap();
        assert_eq!(
            service.dispatch_inbound_producer(envelope).unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
    }
}
