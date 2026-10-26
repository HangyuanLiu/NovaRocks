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
//! Native exchange wire projection and route settlement.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
};
use novarocks_execution_contract::ExchangeStreamIdentity;
use novarocks_proto_models as proto;
use novarocks_types::UniqueId;
use novarocks_worker::{InboundFrameClaim, NormalClosedInbound, TaskInboundCapabilities};

/// The exact admitted stream and sender-count claim, checked before any live
/// or closed-route authority is consulted.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ExchangeRouteQuery {
    pub stream: ExchangeStreamIdentity,
    pub sender_count: u32,
}

/// What one exchange-ingress authority says about one inbound frame.
///
/// The question is three-valued on purpose. Two owners are wired at once --
/// the fragment-based query lifecycle and the task substrate -- and each holds
/// a disjoint set of destinations. Collapsing this into `Result` would make
/// "this destination is not mine" indistinguishable from "this route is
/// illegal", so a destination nobody holds would be reported as the refusal of
/// whichever authority happened to be asked, and a destination two authorities
/// hold would be settled by whichever answered first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeRouteClaim {
    /// This authority holds no destination matching the frame. It is not a
    /// permission and not a refusal: it is a statement of non-ownership.
    NotHeld,
    /// This authority holds the destination and the frame's route is legal.
    Authorized,
    NormallyClosed(NormalClosedInbound),
    /// This authority holds the destination and the frame's route is illegal.
    Refused(String),
}

pub enum ExchangeRouteDelivery {
    Delivered,
    NormallyClosed(NormalClosedInbound),
    Refused(String),
    Receiver(novarocks_execution::runtime::exchange::ExchangeFailure),
}

impl ExchangeRouteClaim {
    const fn claims_destination(&self) -> bool {
        matches!(
            self,
            Self::Authorized | Self::NormallyClosed(_) | Self::Refused(_)
        )
    }
}

/// One owner of exchange destinations on this backend.
///
/// An authority answers only about destinations it holds. It never speaks for
/// another owner's destinations and never falls back to another owner's
/// verdict; the caller composes the answers.
pub trait ExchangeRouteAuthority: Send + Sync + 'static {
    /// A stable name used in refusal text, so an operator reading a rejected
    /// frame can tell which owner decided it.
    fn authority_name(&self) -> &'static str;

    fn claim_exchange_route(&self, query: ExchangeRouteQuery) -> ExchangeRouteClaim;

    fn deliver_exchange_route(
        &self,
        query: ExchangeRouteQuery,
        receiver_port: &dyn ExchangeReceiverPort,
        key: ExchangeReceiverKey,
        frame: ExchangeReceiverFrame,
    ) -> ExchangeRouteDelivery {
        match self.claim_exchange_route(query) {
            ExchangeRouteClaim::Authorized => match receiver_port.push(key, frame) {
                Ok(()) => ExchangeRouteDelivery::Delivered,
                Err(error) => ExchangeRouteDelivery::Receiver(error),
            },
            ExchangeRouteClaim::NormallyClosed(proof) => {
                ExchangeRouteDelivery::NormallyClosed(proof)
            }
            ExchangeRouteClaim::Refused(detail) => ExchangeRouteDelivery::Refused(detail),
            ExchangeRouteClaim::NotHeld => ExchangeRouteDelivery::Refused(
                "destination owner withdrew its route before delivery".to_owned(),
            ),
        }
    }
}

/// Native projection of the Worker-owned task inbound-capability verdict.
///
/// The Worker owns frozen destination topology and the capability lifecycle;
/// this adapter only translates its three-valued admission answer into the
/// Native exchange route contract. Keeping that translation beside the wire
/// handler prevents a Backend role service from becoming a second wire owner.
pub struct TaskInboundCapabilitiesRouteAuthority(Arc<TaskInboundCapabilities>);

impl TaskInboundCapabilitiesRouteAuthority {
    pub fn new(capabilities: Arc<TaskInboundCapabilities>) -> Self {
        Self(capabilities)
    }
}

impl ExchangeRouteAuthority for TaskInboundCapabilitiesRouteAuthority {
    fn authority_name(&self) -> &'static str {
        "the task substrate"
    }

    fn claim_exchange_route(&self, query: ExchangeRouteQuery) -> ExchangeRouteClaim {
        match self.0.claim_stream(query.stream) {
            Ok(admission) if admission.sender_count() == query.sender_count => {
                ExchangeRouteClaim::Authorized
            }
            Ok(_) => ExchangeRouteClaim::Refused(
                "exchange sender count differs from frozen topology".into(),
            ),
            Err(InboundFrameClaim::NotHeld) => ExchangeRouteClaim::NotHeld,
            Err(InboundFrameClaim::NormallyClosed(proof))
                if proof.sender_count() == query.sender_count =>
            {
                ExchangeRouteClaim::NormallyClosed(proof)
            }
            Err(InboundFrameClaim::NormallyClosed(_)) => ExchangeRouteClaim::Refused(
                "closed exchange sender count differs from frozen topology".into(),
            ),
            Err(InboundFrameClaim::Refused(detail)) => ExchangeRouteClaim::Refused(detail),
            Err(InboundFrameClaim::Authorized) => {
                unreachable!("successful stream claims return admission")
            }
        }
    }

    fn deliver_exchange_route(
        &self,
        query: ExchangeRouteQuery,
        receiver_port: &dyn ExchangeReceiverPort,
        key: ExchangeReceiverKey,
        frame: ExchangeReceiverFrame,
    ) -> ExchangeRouteDelivery {
        if frame.stream != query.stream
            || frame.sender_count != query.sender_count
            || key.fragment_instance_id != query.stream.destination_fragment_instance_id()
            || key.node_id != query.stream.destination_node().get()
        {
            return ExchangeRouteDelivery::Refused(
                "exchange delivery differs from its claimed stream".into(),
            );
        }
        match self.0.with_claimed_stream(query.stream, |admission| {
            if admission.sender_count() != query.sender_count {
                return Err(ExchangeRouteDelivery::Refused(
                    "exchange sender count differs from frozen topology".into(),
                ));
            }
            receiver_port
                .push(key, frame)
                .map_err(ExchangeRouteDelivery::Receiver)
        }) {
            Ok(Ok(())) => ExchangeRouteDelivery::Delivered,
            Ok(Err(error)) => error,
            Err(InboundFrameClaim::NormallyClosed(proof))
                if proof.sender_count() == query.sender_count =>
            {
                ExchangeRouteDelivery::NormallyClosed(proof)
            }
            Err(InboundFrameClaim::NormallyClosed(_)) => ExchangeRouteDelivery::Refused(
                "closed exchange sender count differs from frozen topology".into(),
            ),
            Err(InboundFrameClaim::Refused(detail)) => ExchangeRouteDelivery::Refused(detail),
            Err(InboundFrameClaim::NotHeld) => ExchangeRouteDelivery::Refused(
                "task destination was withdrawn before delivery".into(),
            ),
            Err(InboundFrameClaim::Authorized) => {
                unreachable!("successful stream claims invoke delivery")
            }
        }
    }
}

/// Native exchange ingress composition.
///
/// This owns only wire-facing ports. Each route authority and the receiver it
/// delegates to remain owned by their respective runtime domains.
#[derive(Clone)]
pub struct NativeExchangeDataPlane {
    receiver_port: Arc<dyn ExchangeReceiverPort>,
    route_authorities: Vec<Arc<dyn ExchangeRouteAuthority>>,
    unit_capabilities: Option<Arc<TaskInboundCapabilities>>,
    unit_backend: Option<novarocks_types::BackendProcessId>,
    unit_publisher: Arc<
        OnceLock<
            Result<
                Arc<crate::exchange_grant_subscription::ExchangeGrantPublisher>,
                novarocks_execution_contract::failure::FailureFact,
            >,
        >,
    >,
}

impl NativeExchangeDataPlane {
    pub fn new(
        receiver_port: Arc<dyn ExchangeReceiverPort>,
        route_authorities: Vec<Arc<dyn ExchangeRouteAuthority>>,
    ) -> Self {
        Self {
            receiver_port,
            route_authorities,
            unit_capabilities: None,
            unit_backend: None,
            unit_publisher: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn with_unit_capabilities(
        mut self,
        capabilities: Arc<TaskInboundCapabilities>,
        backend: novarocks_types::BackendProcessId,
    ) -> Self {
        self.unit_capabilities = Some(capabilities);
        self.unit_backend = Some(backend);
        self
    }

    /// Incremental construction leaves the legacy receiver unchanged. A
    /// missing unit listener is a real refusal of the new capability, not a
    /// successful empty subscription. Cutover installs this at startup.
    pub(crate) fn initialize_unit_publications(
        &self,
    ) -> Result<
        Arc<crate::exchange_grant_subscription::ExchangeGrantPublisher>,
        novarocks_execution_contract::failure::FailureFact,
    > {
        self.unit_publisher
            .get_or_init(|| {
                crate::exchange_grant_subscription::ExchangeGrantPublisher::install(Arc::clone(
                    &self.receiver_port,
                ))
            })
            .clone()
    }

    pub(crate) fn deliver_unit(
        &self,
        raw: proto::novarocks::DeliverExchangeUnitRequest,
        caller: novarocks_types::BackendProcessId,
    ) -> Result<proto::novarocks::DeliverExchangeUnitResponse, tonic::Status> {
        use novarocks_execution::runtime::fragment::io::exchange_unit::{
            ExchangeDemandEntry, ExchangeDemandOperation, ExchangeUnitReceiverFrame,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let frame = codec::decode_delivery_request(
            raw,
            novarocks_proto_codec::FieldPath::root("exchange_unit_request"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let stream = frame.stream;
        self.check_unit_pair(codec::ExchangeProcessPair::for_stream(stream), caller)?;
        let sequence = frame.item.sequence();
        let outcome = match &self.unit_capabilities {
            Some(capabilities) => {
                let key = ExchangeReceiverKey { fragment_instance_id: stream.destination_fragment_instance_id(), node_id: stream.destination_node().get() };
                let next = frame.next_demand;
                let delivery = capabilities.with_claimed_stream(stream, |admission| {
                    if admission.sender_count() != frame.sender_count { return Err(unit_route_failure(460)); }
                    let mut receipt = self.receiver_port.receive_unit(key, ExchangeUnitReceiverFrame {
                        stream, sender_count: frame.sender_count, sender_id: frame.sender_id,
                        backend_number: frame.backend_number, item: frame.item,
                        payload: frame.payload, next_demand: None,
                    })?;
                    if let Some(demand) = next {
                        let observation = self.receiver_port.register_unit_demand(key, ExchangeDemandEntry {
                            stream, operation: ExchangeDemandOperation::Register(demand),
                        })?;
                        if observation.stream != stream { return Err(unit_route_failure(462)); }
                        receipt.facts = observation.facts;
                    }
                    validate_unit_facts(stream, receipt.facts)?;
                    Ok(receipt)
                });
                match delivery {
                    Ok(Ok(receipt)) => codec::DecodedExchangeDeliveryOutcome::Observed { verdict: receipt.verdict, facts: receipt.facts },
                    Ok(Err(fact)) => codec::DecodedExchangeDeliveryOutcome::Refused(fact),
                    Err(claim) => codec::DecodedExchangeDeliveryOutcome::Refused(unit_claim_failure(claim)),
                }
            }
            None => codec::DecodedExchangeDeliveryOutcome::Refused(novarocks_execution::runtime::fragment::io::exchange_unit::unit_transport_not_installed()),
        };
        Ok(codec::encode_delivery_response(stream, sequence, outcome))
    }

    pub(crate) fn register_unit_demands(
        &self,
        raw: proto::novarocks::RegisterExchangeDemandRequest,
        caller: novarocks_types::BackendProcessId,
    ) -> Result<proto::novarocks::RegisterExchangeDemandResponse, tonic::Status> {
        use novarocks_execution::runtime::fragment::io::exchange_unit::{
            ExchangeDemandEntry, ExchangeDemandOperation,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let batch = codec::decode_demand_request(
            &raw,
            novarocks_proto_codec::FieldPath::root("exchange_demand_request"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        self.check_unit_pair(batch.pair, caller)?;
        let mut outcomes = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let stream = entry.stream;
            let outcome = match &self.unit_capabilities {
                None => Err(novarocks_execution::runtime::fragment::io::exchange_unit::unit_transport_not_installed()),
                Some(capabilities) => match capabilities.with_claimed_stream(stream, |_| {
                    let observation = self.receiver_port.register_unit_demand(ExchangeReceiverKey {
                        fragment_instance_id: stream.destination_fragment_instance_id(), node_id: stream.destination_node().get(),
                    }, ExchangeDemandEntry {
                        stream, operation: match entry.operation {
                            codec::ExchangeDemandOperation::Demand(demand) => ExchangeDemandOperation::Register(demand),
                            codec::ExchangeDemandOperation::StopSender => ExchangeDemandOperation::StopSender,
                        },
                    })?;
                    if observation.stream != stream { return Err(unit_route_failure(462)); }
                    validate_unit_facts(stream, observation.facts)?;
                    Ok(observation.facts)
                }) {
                    Ok(outcome) => outcome,
                    Err(claim) => Err(unit_claim_failure(claim)),
                },
            };
            outcomes.push(codec::DecodedExchangeDemandResult { stream, outcome });
        }
        Ok(codec::encode_demand_response(&outcomes))
    }

    pub(crate) fn subscribe_unit_grants(
        &self,
        raw: proto::novarocks::SubscribeExchangeGrantsRequest,
        caller: novarocks_types::BackendProcessId,
        ownership: Option<Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<crate::exchange_grant_subscription::ExchangeGrantStream, tonic::Status> {
        use novarocks_task_codec::exchange_unit_codec as codec;
        let subscription = codec::decode_grant_subscription(
            &raw,
            novarocks_proto_codec::FieldPath::root("exchange_grant_subscription"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        self.check_unit_pair(subscription.pair, caller)?;
        let publisher = self
            .initialize_unit_publications()
            .map_err(crate::exchange_grant_subscription::failure_status)?;
        publisher.subscribe(subscription, ownership)
    }

    fn check_unit_pair(
        &self,
        pair: novarocks_task_codec::exchange_unit_codec::ExchangeProcessPair,
        caller: novarocks_types::BackendProcessId,
    ) -> Result<(), tonic::Status> {
        if caller != pair.sender {
            return Err(tonic::Status::permission_denied(
                "exchange sender differs from authenticated backend process",
            ));
        }
        let backend = self.unit_backend.ok_or_else(|| {
            tonic::Status::failed_precondition("exchange unit backend capability is not installed")
        })?;
        if backend != pair.receiver {
            return Err(tonic::Status::failed_precondition(
                "exchange destination differs from this backend process",
            ));
        }
        Ok(())
    }

    pub fn transmit(
        &self,
        request: proto::novarocks::ExchangeRequest,
    ) -> proto::novarocks::ExchangeResponse {
        let authorities: Vec<&dyn ExchangeRouteAuthority> =
            self.route_authorities.iter().map(Arc::as_ref).collect();
        handle_transmit_chunk(self.receiver_port.as_ref(), &authorities, request)
    }
}

pub(crate) fn authenticated_unit_sender<T>(
    request: &tonic::Request<T>,
) -> Result<novarocks_types::BackendProcessId, tonic::Status> {
    match request
        .extensions()
        .get::<novarocks_native_trust::AuthenticatedNativeCaller>()
        .and_then(|caller| caller.process_identity())
    {
        Some(novarocks_native_trust::NativeProcessIdentity::Backend(sender)) => Ok(sender),
        _ => Err(tonic::Status::unauthenticated(
            "exchange requires an authenticated backend process identity",
        )),
    }
}

fn unit_route_failure(code: u16) -> novarocks_execution_contract::failure::FailureFact {
    crate::exchange_grant_subscription::exchange_failure(
        novarocks_execution_contract::failure::FailureClass::Invariant,
        code,
    )
}

fn unit_claim_failure(
    claim: InboundFrameClaim,
) -> novarocks_execution_contract::failure::FailureFact {
    match claim {
        // The legacy close carrier has a different sequence domain. Only
        // P06's simultaneous author/codec migration can project it as units.
        InboundFrameClaim::NormallyClosed(_) => {
            crate::exchange_grant_subscription::exchange_failure(
                novarocks_execution_contract::failure::FailureClass::Unsupported,
                461,
            )
        }
        InboundFrameClaim::NotHeld
        | InboundFrameClaim::Refused(_)
        | InboundFrameClaim::Authorized => unit_route_failure(460),
    }
}

fn validate_unit_facts(
    stream: ExchangeStreamIdentity,
    facts: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts,
) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
    use novarocks_task_codec::exchange_unit_codec as codec;
    codec::decode_stream_state(
        &codec::encode_stream_state(codec::DecodedExchangeStreamState { stream, facts }),
        novarocks_proto_codec::FieldPath::root("exchange_receiver_facts"),
    )
    .map(|_| ())
    .map_err(|_| unit_route_failure(462))
}

/// Why one inbound exchange frame is not admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ExchangeRouteRefusal {
    /// No authority is wired at all, so no owner could have been asked.
    NoAuthority,
    /// Every wired authority disclaimed the destination.
    NoOwner,
    /// The one authority that holds the destination refused the route.
    Refused {
        authority: &'static str,
        detail: String,
    },
    /// More than one authority holds the destination. This is a composition
    /// conflict, not a race to be won: whichever answer were taken, one owner
    /// would be executing frames the other believes it owns.
    Conflict { authorities: Vec<&'static str> },
}

impl fmt::Display for ExchangeRouteRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAuthority => {
                formatter.write_str("exchange ingress has no route authority on this backend")
            }
            Self::NoOwner => formatter
                .write_str("no active exchange owner on this backend holds this destination"),
            Self::Refused { authority, detail } => {
                write!(
                    formatter,
                    "{authority} holds this destination and refused the route: {detail}"
                )
            }
            Self::Conflict { authorities } => write!(
                formatter,
                "this destination is claimed by more than one exchange owner ({}), so no owner may admit the frame",
                authorities.join(", ")
            ),
        }
    }
}

/// Settles one frame against every wired authority, requiring exactly one
/// claimant.
///
/// Zero claimants is a refusal rather than a permission, and two claimants is
/// a refusal rather than a first-answer win, so the order the authorities are
/// wired in cannot change the answer.
fn settle_exchange_route(
    authorities: &[&dyn ExchangeRouteAuthority],
    query: ExchangeRouteQuery,
) -> Result<usize, ExchangeRouteRefusal> {
    if authorities.is_empty() {
        return Err(ExchangeRouteRefusal::NoAuthority);
    }
    let mut claimants: Vec<(usize, &'static str, ExchangeRouteClaim)> = Vec::new();
    for (index, authority) in authorities.iter().enumerate() {
        let claim = authority.claim_exchange_route(query);
        if claim.claims_destination() {
            claimants.push((index, authority.authority_name(), claim));
        }
    }
    match claimants.len() {
        0 => Err(ExchangeRouteRefusal::NoOwner),
        1 => match claimants.pop().expect("one claimant") {
            (index, _, ExchangeRouteClaim::Authorized | ExchangeRouteClaim::NormallyClosed(_)) => {
                Ok(index)
            }
            (_, authority, ExchangeRouteClaim::Refused(detail)) => {
                Err(ExchangeRouteRefusal::Refused { authority, detail })
            }
            (_, _, ExchangeRouteClaim::NotHeld) => {
                unreachable!("a disclaimed route is not collected as a claimant")
            }
        },
        _ => Err(ExchangeRouteRefusal::Conflict {
            authorities: claimants.into_iter().map(|(_, name, _)| name).collect(),
        }),
    }
}

fn ok_common_status() -> proto::common::Status {
    proto::common::Status {
        code: 0,
        message: String::new(),
    }
}

fn error_common_status(message: impl Into<String>) -> proto::common::Status {
    proto::common::Status {
        code: 1,
        message: message.into(),
    }
}

pub fn handle_transmit_chunk(
    receiver_port: &dyn ExchangeReceiverPort,
    authorities: &[&dyn ExchangeRouteAuthority],
    params: proto::novarocks::ExchangeRequest,
) -> proto::novarocks::ExchangeResponse {
    let mut response = proto::novarocks::ExchangeResponse {
        ack_sequence: params.sequence,
        status: Some(ok_common_status()),
        normal_closed: None,
    };

    let destination_fragment_instance_id = UniqueId::new(params.finst_id_hi, params.finst_id_lo);
    let source_fragment_instance_id =
        UniqueId::new(params.source_finst_id_hi, params.source_finst_id_lo);
    if destination_fragment_instance_id == UniqueId::new(0, 0)
        || source_fragment_instance_id == UniqueId::new(0, 0)
    {
        response.status = Some(error_common_status(
            "exchange ingress requires non-zero source and destination fragment instance IDs",
        ));
        return response;
    }
    if params.node_id < 0 {
        response.status = Some(error_common_status(
            "exchange ingress requires a non-negative destination node ID",
        ));
        return response;
    }
    if params.sender_count == 0 || params.sender_ordinal >= params.sender_count {
        response.status = Some(error_common_status(
            "exchange ingress requires a sender ordinal below a non-zero sender count",
        ));
        return response;
    }
    let stream = match params
        .stream
        .as_ref()
        .ok_or_else(|| "exchange ingress requires an exact stream identity".to_owned())
        .and_then(|wire| {
            novarocks_task_codec::exchange_flow_codec::decode_stream(
                wire,
                novarocks_proto_codec::FieldPath::root("exchange_request").field("stream"),
            )
            .map_err(|e| e.to_string())
        }) {
        Ok(stream) => stream,
        Err(error) => {
            response.status = Some(error_common_status(error));
            return response;
        }
    };
    if stream.destination_fragment_instance_id() != destination_fragment_instance_id
        || stream.destination_node().get() != params.node_id
        || stream.source().fragment_instance_id() != source_fragment_instance_id
        || stream.source().sender_ordinal() != params.sender_ordinal
        || params.sequence < 0
    {
        response.status = Some(error_common_status(
            "exchange envelope differs from its exact stream identity or sequence",
        ));
        return response;
    }
    let query = ExchangeRouteQuery {
        stream,
        sender_count: params.sender_count,
    };
    let owner = match settle_exchange_route(authorities, query) {
        Ok(index) => index,
        Err(refusal) => {
            response.status = Some(error_common_status(format!(
                "exchange ingress route rejected: {refusal}"
            )));
            return response;
        }
    };

    let key = ExchangeReceiverKey {
        fragment_instance_id: destination_fragment_instance_id,
        node_id: params.node_id,
    };
    let frame = ExchangeReceiverFrame {
        stream,
        source_fragment_instance_id,
        sender_ordinal: params.sender_ordinal,
        sender_count: params.sender_count,
        sender_id: params.sender_id,
        backend_number: params.be_number,
        sequence: params.sequence,
        eos: params.eos,
        payload: params.payload,
    };
    match authorities[owner].deliver_exchange_route(query, receiver_port, key, frame) {
        ExchangeRouteDelivery::Delivered => {}
        ExchangeRouteDelivery::NormallyClosed(proof) => {
            response.normal_closed = Some(
                novarocks_task_codec::exchange_flow_codec::encode_normal_close(
                    proof.close(),
                    proof.sender_count(),
                ),
            );
        }
        ExchangeRouteDelivery::Receiver(error) => {
            // This is the existing final wire projection, not error classification.
            response.status = Some(error_common_status(format!(
                "exchange ingress failed: {error}"
            )));
        }
        ExchangeRouteDelivery::Refused(detail) => {
            response.status = Some(error_common_status(format!(
                "exchange ingress failed: {detail}"
            )));
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::{
        ExchangeRouteAuthority, ExchangeRouteClaim, ExchangeRouteQuery, handle_transmit_chunk,
    };
    use novarocks_execution::runtime::fragment::io::UnavailableExchangeReceiverPort;
    use novarocks_proto_models as proto;

    /// An authority with a fixed answer, so a composition test can state
    /// exactly which owners claim a destination.
    struct FixedAuthority {
        name: &'static str,
        claim: ExchangeRouteClaim,
    }

    impl FixedAuthority {
        fn new(name: &'static str, claim: ExchangeRouteClaim) -> Self {
            Self { name, claim }
        }
    }

    impl ExchangeRouteAuthority for FixedAuthority {
        fn authority_name(&self) -> &'static str {
            self.name
        }

        fn claim_exchange_route(&self, _query: ExchangeRouteQuery) -> ExchangeRouteClaim {
            self.claim.clone()
        }
    }

    fn frame() -> proto::novarocks::ExchangeRequest {
        proto::novarocks::ExchangeRequest {
            stream: Some({
                let mut stream = crate::exchange_transmitter::tests::frame_and_proof()
                    .0
                    .stream
                    .unwrap();
                stream = novarocks_execution_contract::ExchangeStreamIdentity::try_new(
                    novarocks_execution_contract::ExchangeSource::new(
                        stream.source().task(),
                        novarocks_types::UniqueId::new(3, 4),
                        0,
                    ),
                    stream.destination(),
                    novarocks_types::UniqueId::new(1, 2),
                    novarocks_execution_contract::FragmentNodeId::new(7),
                )
                .unwrap();
                novarocks_task_codec::exchange_flow_codec::encode_stream(stream)
            }),
            finst_id_hi: 1,
            finst_id_lo: 2,
            node_id: 7,
            source_finst_id_hi: 3,
            source_finst_id_lo: 4,
            sender_ordinal: 0,
            sender_count: 1,
            sender_id: 11,
            be_number: 0,
            eos: false,
            sequence: 42,
            payload: vec![0xff],
        }
    }

    fn transmit(
        authorities: &[&dyn ExchangeRouteAuthority],
        request: proto::novarocks::ExchangeRequest,
    ) -> proto::common::Status {
        handle_transmit_chunk(&UnavailableExchangeReceiverPort, authorities, request)
            .status
            .expect("status")
    }

    #[test]
    fn exchange_route_rejection_happens_before_receiver_delivery() {
        let lifecycle = FixedAuthority::new(
            "the fragment query lifecycle",
            ExchangeRouteClaim::Refused("the route is absent from the manifest".to_string()),
        );
        let response = handle_transmit_chunk(
            &UnavailableExchangeReceiverPort,
            &[&lifecycle as &dyn ExchangeRouteAuthority],
            frame(),
        );

        assert_eq!(response.ack_sequence, 42);
        let status = response.status.expect("status");
        assert_eq!(status.code, 1);
        assert!(status.message.contains("route rejected"));
    }

    #[test]
    fn pre_lnp9_exchange_shape_is_rejected_before_route_authorization() {
        let lifecycle = FixedAuthority::new(
            "the fragment query lifecycle",
            ExchangeRouteClaim::Refused("unreachable".to_string()),
        );
        let status = transmit(
            &[&lifecycle as &dyn ExchangeRouteAuthority],
            proto::novarocks::ExchangeRequest {
                finst_id_hi: 1,
                finst_id_lo: 2,
                node_id: 7,
                sender_id: 11,
                be_number: 0,
                eos: false,
                sequence: 42,
                payload: vec![0xff],
                ..Default::default()
            },
        );

        assert_eq!(status.code, 1);
        assert!(status.message.contains("requires non-zero source"));
    }

    /// Catches a data plane that authorizes a frame from a single authority's
    /// silence. Before the task substrate was wired as its own authority the
    /// lifecycle owner answered for every destination, so a task-protocol
    /// destination -- which it has no manifest for -- was reported as a route
    /// absent from every manifest, and every distributed query with an
    /// exchange failed.
    #[test]
    fn a_destination_no_authority_holds_is_refused_as_an_absent_owner() {
        let lifecycle =
            FixedAuthority::new("the fragment query lifecycle", ExchangeRouteClaim::NotHeld);
        let tasks = FixedAuthority::new("the task substrate", ExchangeRouteClaim::NotHeld);
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );

        assert_eq!(status.code, 1);
        assert!(
            status
                .message
                .contains("no active exchange owner on this backend holds this destination"),
            "unexpected refusal: {}",
            status.message
        );
        // A destination nobody holds must not name an owner: the wire may not
        // tell "not created yet" from "not yours".
        assert!(!status.message.contains("the task substrate"));
        assert!(!status.message.contains("the fragment query lifecycle"));
    }

    /// The task substrate's claim is what admits a task-protocol frame, and it
    /// admits it while the lifecycle owner disclaims the same destination.
    #[test]
    fn a_destination_one_authority_holds_is_settled_by_that_authority() {
        let lifecycle =
            FixedAuthority::new("the fragment query lifecycle", ExchangeRouteClaim::NotHeld);
        let tasks = FixedAuthority::new("the task substrate", ExchangeRouteClaim::Authorized);
        // The receiver port is unavailable, so an authorized frame fails at
        // delivery rather than at authorization. That is the proof the route
        // was admitted.
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );
        assert_eq!(status.code, 1);
        assert!(
            status.message.contains("exchange ingress failed"),
            "unexpected status: {}",
            status.message
        );
        assert!(!status.message.contains("route rejected"));
    }

    /// A refusal must say which owner decided it, because the two owners
    /// refuse for unrelated reasons and an operator reading only "absent from
    /// every manifest" would be told the wrong thing on the task path.
    #[test]
    fn the_holding_authority_is_named_in_its_own_refusal() {
        let lifecycle =
            FixedAuthority::new("the fragment query lifecycle", ExchangeRouteClaim::NotHeld);
        let tasks = FixedAuthority::new(
            "the task substrate",
            ExchangeRouteClaim::Refused("its topology froze 3 senders".to_string()),
        );
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );

        assert_eq!(status.code, 1);
        assert!(
            status.message.contains("the task substrate")
                && status.message.contains("its topology froze 3 senders"),
            "unexpected refusal: {}",
            status.message
        );
    }

    /// Two owners holding one destination is a composition conflict. Taking
    /// either answer would let one owner run frames the other believes it
    /// owns, so the gate closes -- and the order the owners are wired in must
    /// not change that.
    #[test]
    fn two_authorities_holding_one_destination_close_the_gate_rather_than_race() {
        let lifecycle = FixedAuthority::new(
            "the fragment query lifecycle",
            ExchangeRouteClaim::Authorized,
        );
        let tasks = FixedAuthority::new("the task substrate", ExchangeRouteClaim::Authorized);
        for order in [
            [
                &lifecycle as &dyn ExchangeRouteAuthority,
                &tasks as &dyn ExchangeRouteAuthority,
            ],
            [
                &tasks as &dyn ExchangeRouteAuthority,
                &lifecycle as &dyn ExchangeRouteAuthority,
            ],
        ] {
            let status = transmit(&order, frame());
            assert_eq!(status.code, 1);
            assert!(
                status
                    .message
                    .contains("claimed by more than one exchange owner"),
                "unexpected refusal: {}",
                status.message
            );
        }

        // One authorizing and one refusing is the same conflict: a refusal is
        // a claim on the destination, so it does not lose to a permission.
        let refusing = FixedAuthority::new(
            "the task substrate",
            ExchangeRouteClaim::Refused("its topology froze 3 senders".to_string()),
        );
        let status = transmit(
            &[
                &lifecycle as &dyn ExchangeRouteAuthority,
                &refusing as &dyn ExchangeRouteAuthority,
            ],
            frame(),
        );
        assert_eq!(status.code, 1);
        assert!(
            status
                .message
                .contains("claimed by more than one exchange owner"),
            "unexpected refusal: {}",
            status.message
        );
    }

    #[test]
    fn a_data_plane_with_no_authority_admits_nothing() {
        let status = transmit(&[], frame());
        assert_eq!(status.code, 1);
        assert!(
            status
                .message
                .contains("exchange ingress has no route authority"),
            "unexpected refusal: {}",
            status.message
        );
    }

    #[test]
    fn an_invalid_sender_set_is_rejected_before_any_authority_is_asked() {
        struct PanickingAuthority;

        impl ExchangeRouteAuthority for PanickingAuthority {
            fn authority_name(&self) -> &'static str {
                "the panicking owner"
            }

            fn claim_exchange_route(&self, _query: ExchangeRouteQuery) -> ExchangeRouteClaim {
                unreachable!("a structurally invalid sender set never reaches an authority")
            }
        }

        let authority = PanickingAuthority;
        for (sender_ordinal, sender_count) in [(0, 0), (2, 2), (5, 3)] {
            let status = transmit(
                &[&authority as &dyn ExchangeRouteAuthority],
                proto::novarocks::ExchangeRequest {
                    sender_ordinal,
                    sender_count,
                    ..frame()
                },
            );
            assert_eq!(status.code, 1);
            assert!(
                status
                    .message
                    .contains("requires a sender ordinal below a non-zero sender count"),
                "unexpected refusal for ordinal {sender_ordinal} of {sender_count}: {}",
                status.message
            );
        }
    }
    #[test]
    fn missing_or_mismatched_exact_stream_is_refused_before_route_authority() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Counting(AtomicUsize);
        impl ExchangeRouteAuthority for Counting {
            fn authority_name(&self) -> &'static str {
                "counting"
            }
            fn claim_exchange_route(&self, _: ExchangeRouteQuery) -> ExchangeRouteClaim {
                self.0.fetch_add(1, Ordering::SeqCst);
                ExchangeRouteClaim::Authorized
            }
        }
        let authority = Counting(AtomicUsize::new(0));
        for mutation in 0..4 {
            let mut request = frame();
            match mutation {
                0 => request.stream = None,
                1 => request.stream.as_mut().unwrap().destination_finst_id_lo += 1,
                2 => request.stream.as_mut().unwrap().sender_ordinal += 1,
                _ => request.sequence = -1,
            }
            assert_ne!(transmit(&[&authority], request).code, 0);
        }
        assert_eq!(authority.0.load(Ordering::SeqCst), 0);
    }
}
