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

use std::{fmt, sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::native_ingress::NativeIngressOwnership;
use novarocks_execution::runtime::fragment::io::exchange_receiver::{
    ClaimedExchangeFrame, ExchangePermitAdmission,
};
use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
};
use novarocks_execution_contract::{ExchangePermitGrant, ExchangeStreamIdentity, FragmentNodeId};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models as proto;
use novarocks_types::BackendProcessId;
use novarocks_types::UniqueId;
use novarocks_worker::{InboundFrameClaim, NormalClosedInbound, TaskInboundCapabilities};

/// Exactly the addressing fields one inbound exchange frame carries.
///
/// The frame has no query, attempt, or task identity in it, so every field
/// here is a kernel key or a wire counter. An authority resolves those against
/// its own frozen facts; nothing downstream may derive authority from them.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ExchangeRouteQuery {
    pub destination_fragment_instance_id: UniqueId,
    pub destination_node_id: i32,
    pub source_fragment_instance_id: UniqueId,
    pub sender_ordinal: u32,
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

    fn claim_exact_stream(
        &self,
        _stream: ExchangeStreamIdentity,
    ) -> Result<novarocks_worker::InboundFrameAdmission, ExchangeRouteClaim> {
        Err(ExchangeRouteClaim::Refused(
            "route authority has no exact stream admission".into(),
        ))
    }

    fn claim_permitted_frame(
        &self,
        _stream: ExchangeStreamIdentity,
        _receiver: &dyn ExchangeReceiverPort,
        _grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, ExchangeRouteDelivery> {
        Err(ExchangeRouteDelivery::Refused(
            "route authority has no permitted-frame claim".into(),
        ))
    }

    #[cfg(test)]
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

    fn claim_exact_stream(
        &self,
        stream: ExchangeStreamIdentity,
    ) -> Result<novarocks_worker::InboundFrameAdmission, ExchangeRouteClaim> {
        self.0.claim_stream(stream).map_err(worker_claim)
    }

    fn claim_permitted_frame(
        &self,
        stream: ExchangeStreamIdentity,
        receiver: &dyn ExchangeReceiverPort,
        grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, ExchangeRouteDelivery> {
        match self
            .0
            .with_claimed_stream(stream, |_| receiver.claim(grant))
        {
            Ok(result) => result.map_err(ExchangeRouteDelivery::Receiver),
            Err(InboundFrameClaim::NormallyClosed(proof)) => {
                Err(ExchangeRouteDelivery::NormallyClosed(proof))
            }
            Err(InboundFrameClaim::Refused(detail)) => Err(ExchangeRouteDelivery::Refused(detail)),
            Err(InboundFrameClaim::NotHeld) => Err(ExchangeRouteDelivery::Refused(
                "task destination was withdrawn before permit claim".into(),
            )),
            Err(InboundFrameClaim::Authorized) => {
                unreachable!("authorized stream returns its frozen admission")
            }
        }
    }

    fn claim_exchange_route(&self, query: ExchangeRouteQuery) -> ExchangeRouteClaim {
        match self.0.claim_frame(
            query.destination_fragment_instance_id,
            FragmentNodeId::new(query.destination_node_id),
            query.source_fragment_instance_id,
            query.sender_ordinal,
            query.sender_count,
        ) {
            InboundFrameClaim::NotHeld => ExchangeRouteClaim::NotHeld,
            InboundFrameClaim::Authorized => ExchangeRouteClaim::Authorized,
            InboundFrameClaim::NormallyClosed(proof) => ExchangeRouteClaim::NormallyClosed(proof),
            InboundFrameClaim::Refused(detail) => ExchangeRouteClaim::Refused(detail),
        }
    }

    #[cfg(test)]
    fn deliver_exchange_route(
        &self,
        query: ExchangeRouteQuery,
        receiver_port: &dyn ExchangeReceiverPort,
        key: ExchangeReceiverKey,
        frame: ExchangeReceiverFrame,
    ) -> ExchangeRouteDelivery {
        match self.0.deliver_frame_with_receiver_error(
            query.destination_fragment_instance_id,
            FragmentNodeId::new(query.destination_node_id),
            query.source_fragment_instance_id,
            query.sender_ordinal,
            query.sender_count,
            || receiver_port.push(key, frame),
        ) {
            novarocks_worker::InboundFrameDelivery::Claim(InboundFrameClaim::Authorized) => {
                ExchangeRouteDelivery::Delivered
            }
            novarocks_worker::InboundFrameDelivery::Claim(InboundFrameClaim::NormallyClosed(
                proof,
            )) => ExchangeRouteDelivery::NormallyClosed(proof),
            novarocks_worker::InboundFrameDelivery::Claim(InboundFrameClaim::Refused(detail)) => {
                ExchangeRouteDelivery::Refused(detail)
            }
            novarocks_worker::InboundFrameDelivery::Claim(InboundFrameClaim::NotHeld) => {
                ExchangeRouteDelivery::Refused(
                    "task destination was withdrawn before delivery".to_owned(),
                )
            }
            novarocks_worker::InboundFrameDelivery::Receiver(error) => {
                ExchangeRouteDelivery::Receiver(error)
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
}

impl NativeExchangeDataPlane {
    pub fn new(
        receiver_port: Arc<dyn ExchangeReceiverPort>,
        route_authorities: Vec<Arc<dyn ExchangeRouteAuthority>>,
    ) -> Self {
        Self {
            receiver_port,
            route_authorities,
        }
    }

    pub async fn acquire(
        &self,
        request: proto::novarocks::AcquireExchangePermitRequest,
        caller: BackendProcessId,
        stop: CancellationToken,
        deadline: Instant,
    ) -> proto::novarocks::AcquireExchangePermitResponse {
        let result = self.acquire_inner(request, caller, stop, deadline).await;
        match result {
            Ok(result) => proto::novarocks::AcquireExchangePermitResponse {
                status: Some(ok_common_status()),
                result: Some(result),
            },
            Err(detail) => proto::novarocks::AcquireExchangePermitResponse {
                status: Some(error_common_status(detail)),
                result: None,
            },
        }
    }

    async fn acquire_inner(
        &self,
        request: proto::novarocks::AcquireExchangePermitRequest,
        caller: BackendProcessId,
        stop: CancellationToken,
        deadline: Instant,
    ) -> Result<proto::novarocks::acquire_exchange_permit_response::Result, String> {
        let request = novarocks_task_codec::exchange_flow_codec::decode_permit_request(
            &request,
            FieldPath::root("exchange_permit"),
        )
        .map_err(|error| error.to_string())?;
        authenticate_source(request.stream, caller)?;
        let (owner, admission) = match settle_exact_stream(&self.route_authorities, request.stream)?
        {
            SettledStream::Authorized { owner, admission } => (owner, admission),
            SettledStream::Closed(proof) => {
                return Ok(
                    proto::novarocks::acquire_exchange_permit_response::Result::NormalClosed(
                        novarocks_task_codec::exchange_flow_codec::encode_normal_close(
                            proof.close(),
                            proof.sender_count(),
                        ),
                    ),
                );
            }
        };
        let _ = owner; // Admission is frozen; the receiver rechecks its own stream state after each wait.
        match self
            .receiver_port
            .acquire(request, stop, deadline)
            .await
            .map_err(|error| error.to_string())?
        {
            ExchangePermitAdmission::Granted(grant) => Ok(
                proto::novarocks::acquire_exchange_permit_response::Result::Granted(
                    novarocks_task_codec::exchange_flow_codec::encode_permit_grant(grant),
                ),
            ),
            ExchangePermitAdmission::Closed(close) => {
                if close.stream != request.stream {
                    return Err("receiver close differs from the exact permit stream".into());
                }
                Ok(
                    proto::novarocks::acquire_exchange_permit_response::Result::NormalClosed(
                        novarocks_task_codec::exchange_flow_codec::encode_normal_close(
                            close,
                            admission.sender_count(),
                        ),
                    ),
                )
            }
        }
    }

    pub async fn transmit(
        &self,
        request: proto::novarocks::ExchangeRequest,
        caller: BackendProcessId,
        ownership: Arc<NativeIngressOwnership>,
    ) -> proto::novarocks::ExchangeResponse {
        let sequence = request.sequence;
        match self.transmit_inner(request, caller, ownership).await {
            Ok(normal_closed) => proto::novarocks::ExchangeResponse {
                ack_sequence: sequence,
                status: Some(ok_common_status()),
                normal_closed,
            },
            Err(detail) => proto::novarocks::ExchangeResponse {
                ack_sequence: sequence,
                status: Some(error_common_status(detail)),
                normal_closed: None,
            },
        }
    }

    async fn transmit_inner(
        &self,
        request: proto::novarocks::ExchangeRequest,
        caller: BackendProcessId,
        ownership: Arc<NativeIngressOwnership>,
    ) -> Result<Option<proto::novarocks::ExchangeNormalClosed>, String> {
        let stream = novarocks_task_codec::exchange_flow_codec::decode_stream(
            request
                .stream
                .as_ref()
                .ok_or_else(|| "exchange frame omits its exact stream".to_string())?,
            FieldPath::root("exchange_frame").field("stream"),
        )
        .map_err(|error| error.to_string())?;
        authenticate_source(stream, caller)?;
        let (owner, admission) = match settle_exact_stream(&self.route_authorities, stream)? {
            SettledStream::Authorized { owner, admission } => (owner, admission),
            SettledStream::Closed(proof) => {
                novarocks_task_codec::exchange_flow_codec::validate_frame_envelope(
                    &request,
                    stream,
                    proof.sender_count(),
                    FieldPath::root("exchange_frame"),
                )
                .map_err(|error| error.to_string())?;
                return Ok(Some(
                    novarocks_task_codec::exchange_flow_codec::encode_normal_close(
                        proof.close(),
                        proof.sender_count(),
                    ),
                ));
            }
        };
        let token = novarocks_task_codec::exchange_flow_codec::decode_token(
            &request.permit_token,
            FieldPath::root("exchange_frame").field("permit_token"),
        )
        .map_err(|error| error.to_string())?;
        let grant = self
            .receiver_port
            .lookup_grant(stream, token)
            .map_err(|error| error.to_string())?;
        novarocks_task_codec::exchange_flow_codec::validate_permitted_frame(
            &request,
            grant,
            admission.sender_count(),
            FieldPath::root("exchange_frame"),
        )
        .map_err(|error| error.to_string())?;
        let mut claim = match self.route_authorities[owner].claim_permitted_frame(
            stream,
            self.receiver_port.as_ref(),
            grant,
        ) {
            Ok(claim) => claim,
            Err(ExchangeRouteDelivery::NormallyClosed(proof)) => {
                return Ok(Some(
                    novarocks_task_codec::exchange_flow_codec::encode_normal_close(
                        proof.close(),
                        proof.sender_count(),
                    ),
                ));
            }
            Err(ExchangeRouteDelivery::Receiver(error)) => return Err(error.to_string()),
            Err(ExchangeRouteDelivery::Refused(detail)) => return Err(detail),
            Err(ExchangeRouteDelivery::Delivered) => {
                unreachable!("a permit claim returns a linear ticket")
            }
        };
        claim
            .retain_transport_lifetime(ownership)
            .map_err(|error| error.to_string())?;
        // No Worker lock crosses this await. The original finite CPU closure
        // holds the claim and transport ownership until actual decode exit.
        self.receiver_port
            .accept(claim, request.payload)
            .await
            .map_err(|error| error.to_string())?;
        Ok(None)
    }
}

fn worker_claim(claim: InboundFrameClaim) -> ExchangeRouteClaim {
    match claim {
        InboundFrameClaim::NotHeld => ExchangeRouteClaim::NotHeld,
        InboundFrameClaim::NormallyClosed(proof) => ExchangeRouteClaim::NormallyClosed(proof),
        InboundFrameClaim::Refused(detail) => ExchangeRouteClaim::Refused(detail),
        InboundFrameClaim::Authorized => ExchangeRouteClaim::Authorized,
    }
}

fn authenticate_source(
    stream: ExchangeStreamIdentity,
    caller: BackendProcessId,
) -> Result<(), String> {
    if stream.source().task().backend_process_id() != caller {
        return Err("exchange source Task differs from the authenticated backend process".into());
    }
    Ok(())
}

enum SettledStream {
    Authorized {
        owner: usize,
        admission: novarocks_worker::InboundFrameAdmission,
    },
    Closed(NormalClosedInbound),
}

fn settle_exact_stream(
    authorities: &[Arc<dyn ExchangeRouteAuthority>],
    stream: ExchangeStreamIdentity,
) -> Result<SettledStream, String> {
    let mut selected = None;
    for (owner, authority) in authorities.iter().enumerate() {
        let claim = authority.claim_exact_stream(stream);
        if matches!(claim, Err(ExchangeRouteClaim::NotHeld)) {
            continue;
        }
        if selected.is_some() {
            return Err("multiple exchange owners hold the exact stream destination".into());
        }
        selected = Some((owner, claim));
    }
    match selected {
        Some((owner, Ok(admission))) => Ok(SettledStream::Authorized { owner, admission }),
        Some((_, Err(ExchangeRouteClaim::NormallyClosed(proof)))) => {
            Ok(SettledStream::Closed(proof))
        }
        Some((_, Err(ExchangeRouteClaim::Refused(detail)))) => Err(detail),
        Some((_, Err(_))) => Err("exact stream owner omitted its frozen admission".into()),
        None => Err("no active exchange owner holds the exact stream destination".into()),
    }
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

#[cfg(test)]
fn handle_transmit_chunk(
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
    let query = ExchangeRouteQuery {
        destination_fragment_instance_id,
        destination_node_id: params.node_id,
        source_fragment_instance_id,
        sender_ordinal: params.sender_ordinal,
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

    fn exact_task_fixture() -> (
        std::sync::Arc<novarocks_worker::TaskInboundCapabilities>,
        novarocks_execution_contract::TaskDescriptor,
        novarocks_execution_contract::ExchangePermitRequest,
    ) {
        use novarocks_execution_contract::{
            ExchangeDeliveryRequirement, ExchangeFrameGeometry, ExchangeFrameKind,
            ExchangeFrameSequence, ExchangeInbound, ExchangePermitRequest, ExchangeSource,
            ExchangeStreamIdentity, ExchangeTopology, FragmentNodeId, TaskDescriptor, TaskIdentity,
        };
        use novarocks_types::{
            UniqueId,
            identity::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId},
        };
        let execution =
            QueryExecutionId::new(QueryId::new(61, 62), AttemptId::new(1).unwrap()).unwrap();
        let task = |stage| {
            TaskIdentity::new(
                execution,
                StageId::new(stage).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            )
        };
        let destination = task(2);
        let source = ExchangeSource::new(task(1), UniqueId::new(11, 12), 0);
        let node = FragmentNodeId::new(7);
        let kernel = UniqueId::new(21, 22);
        let descriptor = TaskDescriptor::try_new(
            destination,
            kernel,
            std::num::NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            ExchangeTopology::try_new(
                Vec::new(),
                vec![
                    ExchangeInbound::try_new(
                        node,
                        vec![source],
                        ExchangeDeliveryRequirement::DemandDriven,
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let capabilities = novarocks_worker::TaskInboundCapabilities::with_limits(8, 65536);
        capabilities
            .install(std::sync::Arc::new(descriptor.clone()))
            .unwrap();
        let request = ExchangePermitRequest {
            stream: ExchangeStreamIdentity::try_new(source, destination, kernel, node).unwrap(),
            sequence: ExchangeFrameSequence::FIRST,
            kind: ExchangeFrameKind::Data,
            geometry: ExchangeFrameGeometry::try_new(17, 33, 11).unwrap(),
        };
        (capabilities, descriptor, request)
    }

    #[test]
    fn native_async_acquire_checks_signed_process_before_receiver_or_close() {
        use super::{NativeExchangeDataPlane, TaskInboundCapabilitiesRouteAuthority};
        use std::{
            sync::Arc,
            time::{Duration, Instant},
        };
        let (capabilities, descriptor, request) = exact_task_fixture();
        let plane = NativeExchangeDataPlane::new(
            Arc::new(UnavailableExchangeReceiverPort),
            vec![Arc::new(TaskInboundCapabilitiesRouteAuthority::new(
                capabilities.clone(),
            ))],
        );
        let runtime = crate::backend_test_support::test_backend_data_runtime();
        let call = |caller| {
            runtime.block_on(plane.acquire(
                novarocks_task_codec::exchange_flow_codec::encode_permit_request(request),
                caller,
                tokio_util::sync::CancellationToken::new(),
                Instant::now() + Duration::from_secs(1),
            ))
        };
        let wrong = call(novarocks_types::BackendProcessId::new_v7());
        assert_eq!(wrong.status.unwrap().code, 1);
        assert!(wrong.result.is_none());
        // The same signed caller check remains mandatory after retirement.
        capabilities.close_normally(&descriptor, || {}).unwrap();
        let wrong_closed = call(novarocks_types::BackendProcessId::new_v7());
        assert_eq!(wrong_closed.status.unwrap().code, 1);
        assert!(wrong_closed.result.is_none());
        let closed = call(request.stream.source().task().backend_process_id());
        assert_eq!(closed.status.unwrap().code, 0);
        let Some(proto::novarocks::acquire_exchange_permit_response::Result::NormalClosed(proof)) =
            closed.result
        else {
            panic!("expected actual retained close")
        };
        let close = novarocks_task_codec::exchange_flow_codec::decode_normal_close(
            &proof,
            request.stream,
            1,
            novarocks_proto_codec::FieldPath::root("close"),
        )
        .unwrap();
        assert_eq!(close.stream, request.stream);
        assert_eq!(
            close.disposition,
            novarocks_execution_contract::ExchangeCloseDisposition::Withdrawn
        );
    }

    #[test]
    fn native_async_acquire_rejects_incomplete_or_unheld_exact_stream() {
        use super::NativeExchangeDataPlane;
        use std::{
            sync::Arc,
            time::{Duration, Instant},
        };
        let (_, _, request) = exact_task_fixture();
        let plane =
            NativeExchangeDataPlane::new(Arc::new(UnavailableExchangeReceiverPort), Vec::new());
        let runtime = crate::backend_test_support::test_backend_data_runtime();
        for raw in [
            proto::novarocks::AcquireExchangePermitRequest::default(),
            novarocks_task_codec::exchange_flow_codec::encode_permit_request(request),
        ] {
            let response = runtime.block_on(plane.acquire(
                raw,
                request.stream.source().task().backend_process_id(),
                tokio_util::sync::CancellationToken::new(),
                Instant::now() + Duration::from_secs(1),
            ));
            assert_eq!(response.status.unwrap().code, 1);
            assert!(response.result.is_none());
        }
    }

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
            permit_token: Vec::new(),
            stream: None,
            frame_kind: novarocks_proto_models::novarocks::ExchangeFrameKind::Unspecified as i32,
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
}
