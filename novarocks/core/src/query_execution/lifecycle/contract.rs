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

use std::sync::Arc;
use std::time::Duration;

use super::identity::AttemptId;
use super::identity::QueryExecutionId;
use super::manifest::{
    ExchangeRouteManifest, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantManifestDigest, ParticipantQueryOptions, ParticipantRole, QueryControlEndpoint,
    RuntimeFilterContribution,
};
use crate::common::types::UniqueId;
use crate::proto::{common, filter, novarocks};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLifecycleErrorCode {
    InvalidManifest,
    Conflict,
    StaleBackend,
    Capacity,
    Terminated,
    Transport,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLifecycleError {
    code: QueryLifecycleErrorCode,
    detail: String,
}

impl QueryLifecycleError {
    pub fn new(code: QueryLifecycleErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub(crate) fn invalid_manifest(detail: impl Into<String>) -> Self {
        Self::new(QueryLifecycleErrorCode::InvalidManifest, detail)
    }

    pub const fn code(&self) -> QueryLifecycleErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for QueryLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for QueryLifecycleError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryControlEvent {
    ControlReady,
    HeartbeatAck { sequence: u64 },
    LocalFailure { code: String, detail: String },
    TerminationAccepted { reason: QueryTerminationReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryControlCommand {
    Heartbeat { sequence: u64, sent_mono_ns: u64 },
    Abort { reason: String },
    Finalize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminationReason {
    CoordinatorAbort,
    CoordinatorFinalize,
    CoordinatorStreamLost,
    CoordinatorHeartbeatTimeout,
    LocalFailure,
    PreStartTimeout,
}

pub trait BackendQueryControl: Send + Sync + 'static {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError>;

    fn abort(&self, reason: String) -> Result<(), QueryLifecycleError>;

    fn finalize(&self) -> Result<(), QueryLifecycleError>;

    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError>;
}

pub struct QueryControlAttachment {
    pub control: Arc<dyn BackendQueryControl>,
    pub events: tokio::sync::mpsc::Receiver<QueryControlEvent>,
}

#[derive(Clone, Debug)]
pub struct QueryInitRequest {
    manifest: ParticipantManifest,
    digest: ParticipantManifestDigest,
}

impl QueryInitRequest {
    pub fn new(
        manifest: ParticipantManifest,
        digest: ParticipantManifestDigest,
    ) -> Result<Self, QueryLifecycleError> {
        if manifest.digest() != digest {
            return Err(QueryLifecycleError::invalid_manifest(
                "participant manifest digest does not match canonical projection",
            ));
        }
        Ok(Self { manifest, digest })
    }

    pub fn from_manifest(manifest: ParticipantManifest) -> Self {
        let digest = manifest.digest();
        Self { manifest, digest }
    }

    pub const fn manifest(&self) -> &ParticipantManifest {
        &self.manifest
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub fn into_parts(self) -> (ParticipantManifest, ParticipantManifestDigest) {
        (self.manifest, self.digest)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryInitOutcome {
    Applied,
    AlreadyApplied,
    RejectedConflict,
    RejectedStaleBackend,
    RejectedCapacity,
    RejectedInvalidManifest,
    RejectedTerminated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryInitAck {
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
    outcome: QueryInitOutcome,
}

impl QueryInitAck {
    pub const fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        outcome: QueryInitOutcome,
    ) -> Self {
        Self {
            execution_id,
            digest,
            outcome,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub const fn outcome(&self) -> QueryInitOutcome {
        self.outcome
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryControlAttach {
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
    frontend_owner_epoch: u64,
}

impl QueryControlAttach {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        frontend_owner_epoch: u64,
    ) -> Result<Self, QueryLifecycleError> {
        if frontend_owner_epoch == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "frontend owner epoch must be nonzero",
            ));
        }
        Ok(Self {
            execution_id,
            digest,
            frontend_owner_epoch,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub const fn frontend_owner_epoch(&self) -> u64 {
        self.frontend_owner_epoch
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAbortRequest {
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
    reason: String,
}

impl QueryAbortRequest {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        reason: impl Into<String>,
    ) -> Result<Self, QueryLifecycleError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(QueryLifecycleError::invalid_manifest(
                "abort reason must not be empty",
            ));
        }
        Ok(Self {
            execution_id,
            digest,
            reason,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminationAck {
    execution_id: QueryExecutionId,
    accepted_reason: QueryTerminationReason,
}

impl QueryTerminationAck {
    pub const fn new(
        execution_id: QueryExecutionId,
        accepted_reason: QueryTerminationReason,
    ) -> Self {
        Self {
            execution_id,
            accepted_reason,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn accepted_reason(&self) -> QueryTerminationReason {
        self.accepted_reason
    }
}

pub trait QueryLifecycleIngress: Send + Sync + 'static {
    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck;

    fn abort_query(&self, request: QueryAbortRequest) -> QueryTerminationAck;

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError>;
}

pub fn encode_query_init_request(
    request: &QueryInitRequest,
) -> Result<novarocks::InitQueryRequest, QueryLifecycleError> {
    Ok(novarocks::InitQueryRequest {
        manifest: Some(encode_participant_manifest(request.manifest())?),
        init_digest: request.digest().as_bytes().to_vec(),
    })
}

pub fn decode_query_init_request(
    request: &novarocks::InitQueryRequest,
) -> Result<QueryInitRequest, QueryLifecycleError> {
    let manifest = request
        .manifest
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("participant manifest is required"))
        .and_then(decode_participant_manifest)?;
    let digest = ParticipantManifestDigest::try_from_slice(&request.init_digest)?;
    QueryInitRequest::new(manifest, digest)
}

pub fn encode_query_init_response(response: &QueryInitAck) -> novarocks::InitQueryResponse {
    novarocks::InitQueryResponse {
        execution_id: Some(encode_execution_id(response.execution_id())),
        init_digest: response.digest().as_bytes().to_vec(),
        outcome: encode_init_outcome(response.outcome()),
    }
}

pub fn decode_query_init_response(
    response: &novarocks::InitQueryResponse,
) -> Result<QueryInitAck, QueryLifecycleError> {
    Ok(QueryInitAck::new(
        decode_required_execution_id(response.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&response.init_digest)?,
        decode_init_outcome(response.outcome)?,
    ))
}

pub fn encode_abort_query_request(request: &QueryAbortRequest) -> novarocks::AbortQueryRequest {
    novarocks::AbortQueryRequest {
        execution_id: Some(encode_execution_id(request.execution_id())),
        init_digest: request.digest().as_bytes().to_vec(),
        reason: request.reason().to_string(),
    }
}

pub fn decode_abort_query_request(
    request: &novarocks::AbortQueryRequest,
) -> Result<QueryAbortRequest, QueryLifecycleError> {
    QueryAbortRequest::new(
        decode_required_execution_id(request.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&request.init_digest)?,
        request.reason.clone(),
    )
}

pub fn encode_abort_query_response(
    response: &QueryTerminationAck,
) -> novarocks::AbortQueryResponse {
    novarocks::AbortQueryResponse {
        execution_id: Some(encode_execution_id(response.execution_id())),
        accepted_reason: encode_termination_reason(response.accepted_reason()),
    }
}

pub fn decode_abort_query_response(
    response: &novarocks::AbortQueryResponse,
) -> Result<QueryTerminationAck, QueryLifecycleError> {
    Ok(QueryTerminationAck::new(
        decode_required_execution_id(response.execution_id.as_ref())?,
        decode_termination_reason(response.accepted_reason)?,
    ))
}

pub fn encode_query_control_attach(attach: &QueryControlAttach) -> novarocks::QueryControlRequest {
    novarocks::QueryControlRequest {
        command: Some(novarocks::query_control_request::Command::Attach(
            novarocks::QueryControlAttach {
                execution_id: Some(encode_execution_id(attach.execution_id())),
                init_digest: attach.digest().as_bytes().to_vec(),
                frontend_owner_epoch: attach.frontend_owner_epoch(),
            },
        )),
    }
}

pub fn decode_query_control_attach(
    request: &novarocks::QueryControlRequest,
) -> Result<QueryControlAttach, QueryLifecycleError> {
    let Some(novarocks::query_control_request::Command::Attach(attach)) = request.command.as_ref()
    else {
        return Err(QueryLifecycleError::invalid_manifest(
            "query control request must contain attach",
        ));
    };
    QueryControlAttach::new(
        decode_required_execution_id(attach.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&attach.init_digest)?,
        attach.frontend_owner_epoch,
    )
}

pub fn encode_query_control_command(
    command: &QueryControlCommand,
) -> novarocks::QueryControlRequest {
    let command = match command {
        QueryControlCommand::Heartbeat {
            sequence,
            sent_mono_ns,
        } => {
            novarocks::query_control_request::Command::Heartbeat(novarocks::QueryControlHeartbeat {
                sequence: *sequence,
                sent_mono_ns: *sent_mono_ns,
            })
        }
        QueryControlCommand::Abort { reason } => {
            novarocks::query_control_request::Command::Abort(novarocks::QueryControlAbort {
                reason: reason.clone(),
            })
        }
        QueryControlCommand::Finalize => {
            novarocks::query_control_request::Command::Finalize(novarocks::QueryControlFinalize {})
        }
    };
    novarocks::QueryControlRequest {
        command: Some(command),
    }
}

pub fn decode_query_control_command(
    request: &novarocks::QueryControlRequest,
) -> Result<QueryControlCommand, QueryLifecycleError> {
    match request.command.as_ref() {
        Some(novarocks::query_control_request::Command::Heartbeat(heartbeat)) => {
            Ok(QueryControlCommand::Heartbeat {
                sequence: heartbeat.sequence,
                sent_mono_ns: heartbeat.sent_mono_ns,
            })
        }
        Some(novarocks::query_control_request::Command::Abort(abort))
            if !abort.reason.trim().is_empty() =>
        {
            Ok(QueryControlCommand::Abort {
                reason: abort.reason.clone(),
            })
        }
        Some(novarocks::query_control_request::Command::Finalize(_)) => {
            Ok(QueryControlCommand::Finalize)
        }
        Some(novarocks::query_control_request::Command::Abort(_)) => Err(
            QueryLifecycleError::invalid_manifest("query control abort reason must not be empty"),
        ),
        Some(novarocks::query_control_request::Command::Attach(_)) => Err(
            QueryLifecycleError::invalid_manifest("attach is not a query control command"),
        ),
        None => Err(QueryLifecycleError::invalid_manifest(
            "query control command is required",
        )),
    }
}

pub fn encode_query_control_event(event: &QueryControlEvent) -> novarocks::QueryControlResponse {
    let event = match event {
        QueryControlEvent::ControlReady => {
            novarocks::query_control_response::Event::ControlReady(novarocks::QueryControlReady {})
        }
        QueryControlEvent::HeartbeatAck { sequence } => {
            novarocks::query_control_response::Event::HeartbeatAck(
                novarocks::QueryControlHeartbeatAck {
                    sequence: *sequence,
                },
            )
        }
        QueryControlEvent::LocalFailure { code, detail } => {
            novarocks::query_control_response::Event::LocalFailure(
                novarocks::QueryControlLocalFailure {
                    code: code.clone(),
                    detail: detail.clone(),
                },
            )
        }
        QueryControlEvent::TerminationAccepted { reason } => {
            novarocks::query_control_response::Event::TerminationAccepted(
                novarocks::QueryControlTerminationAccepted {
                    reason: encode_termination_reason(*reason),
                },
            )
        }
    };
    novarocks::QueryControlResponse { event: Some(event) }
}

pub fn decode_query_control_event(
    response: &novarocks::QueryControlResponse,
) -> Result<QueryControlEvent, QueryLifecycleError> {
    match response.event.as_ref() {
        Some(novarocks::query_control_response::Event::ControlReady(_)) => {
            Ok(QueryControlEvent::ControlReady)
        }
        Some(novarocks::query_control_response::Event::HeartbeatAck(ack)) => {
            Ok(QueryControlEvent::HeartbeatAck {
                sequence: ack.sequence,
            })
        }
        Some(novarocks::query_control_response::Event::LocalFailure(failure))
            if !failure.code.trim().is_empty() && !failure.detail.trim().is_empty() =>
        {
            Ok(QueryControlEvent::LocalFailure {
                code: failure.code.clone(),
                detail: failure.detail.clone(),
            })
        }
        Some(novarocks::query_control_response::Event::TerminationAccepted(accepted)) => {
            Ok(QueryControlEvent::TerminationAccepted {
                reason: decode_termination_reason(accepted.reason)?,
            })
        }
        Some(novarocks::query_control_response::Event::LocalFailure(_)) => {
            Err(QueryLifecycleError::invalid_manifest(
                "local failure code and detail must not be empty",
            ))
        }
        None => Err(QueryLifecycleError::invalid_manifest(
            "query control event is required",
        )),
    }
}

fn encode_participant_manifest(
    manifest: &ParticipantManifest,
) -> Result<novarocks::ParticipantManifest, QueryLifecycleError> {
    Ok(novarocks::ParticipantManifest {
        execution_id: Some(encode_execution_id(manifest.execution_id())),
        backend: Some(encode_backend_identity(manifest.backend())),
        participant_roles: manifest
            .roles()
            .iter()
            .map(|role| match role {
                ParticipantRole::FragmentExecutor => 1,
                ParticipantRole::RuntimeFilterService => 2,
            })
            .collect(),
        expected_fragment_instance_ids: manifest
            .expected_fragment_instance_ids()
            .iter()
            .copied()
            .map(encode_unique_id)
            .collect(),
        query_options: Some(
            crate::protocol::native::encode::instance::encode_query_options(
                manifest.query_options().native(),
            ),
        ),
        query_deadline_unix_ms: manifest.query_deadline_unix_ms(),
        exchange_routes: manifest
            .exchange_routes()
            .iter()
            .map(encode_exchange_route)
            .collect(),
        runtime_filter: manifest
            .runtime_filter()
            .map(|contribution| {
                encode_runtime_filter_contribution(manifest.execution_id(), contribution)
            })
            .transpose()?,
        pre_start_timeout_ms: u64::try_from(manifest.pre_start_timeout().as_millis())
            .expect("validated pre-start timeout fits in u64 milliseconds"),
        report_endpoint: Some(encode_endpoint(manifest.report_endpoint())),
    })
}

fn decode_participant_manifest(
    manifest: &novarocks::ParticipantManifest,
) -> Result<ParticipantManifest, QueryLifecycleError> {
    let execution_id = decode_required_execution_id(manifest.execution_id.as_ref())?;
    let backend = manifest
        .backend
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::invalid_manifest("participant backend identity is required")
        })
        .and_then(decode_backend_identity)?;
    let roles = manifest
        .participant_roles
        .iter()
        .copied()
        .map(decode_participant_role)
        .collect::<Result<Vec<_>, _>>()?;
    let expected_fragment_instance_ids = manifest
        .expected_fragment_instance_ids
        .iter()
        .map(decode_unique_id)
        .collect::<Result<Vec<_>, _>>()?;
    let query_options = manifest
        .query_options
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("query options are required"))
        .and_then(|wire| {
            crate::protocol::native::decode::decode_query_options(wire)
                .map(ParticipantQueryOptions::new)
                .map_err(|error| QueryLifecycleError::invalid_manifest(error.to_string()))
        })?;
    let exchange_routes = manifest
        .exchange_routes
        .iter()
        .map(decode_exchange_route)
        .collect::<Result<Vec<_>, _>>()?;
    let runtime_filter = manifest
        .runtime_filter
        .as_ref()
        .map(|contribution| decode_runtime_filter_contribution(execution_id, contribution))
        .transpose()?;
    let report_endpoint = manifest
        .report_endpoint
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("report endpoint is required"))
        .and_then(decode_endpoint)?;

    ParticipantManifest::new(
        execution_id,
        backend,
        roles,
        expected_fragment_instance_ids,
        query_options,
        manifest.query_deadline_unix_ms,
        exchange_routes,
        runtime_filter,
        Duration::from_millis(manifest.pre_start_timeout_ms),
        report_endpoint,
    )
}

fn encode_execution_id(execution_id: QueryExecutionId) -> novarocks::QueryExecutionId {
    novarocks::QueryExecutionId {
        query_id: Some(common::UniqueId {
            hi: execution_id.query_id().high(),
            lo: execution_id.query_id().low(),
        }),
        attempt_id: execution_id.attempt_id().get(),
    }
}

fn decode_required_execution_id(
    execution_id: Option<&novarocks::QueryExecutionId>,
) -> Result<QueryExecutionId, QueryLifecycleError> {
    let execution_id = execution_id
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("query execution id is required"))?;
    let query_id = execution_id
        .query_id
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("query id is required"))?;
    QueryExecutionId::new(
        crate::query_execution::contract::QueryId::new(query_id.hi, query_id.lo),
        AttemptId::new(execution_id.attempt_id)?,
    )
}

fn encode_unique_id(id: UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: id.hi,
        lo: id.lo,
    }
}

fn decode_unique_id(id: &common::UniqueId) -> Result<UniqueId, QueryLifecycleError> {
    if id.hi == 0 && id.lo == 0 {
        return Err(QueryLifecycleError::invalid_manifest(
            "unique id must be nonzero",
        ));
    }
    Ok(UniqueId {
        hi: id.hi,
        lo: id.lo,
    })
}

fn encode_endpoint(endpoint: &QueryControlEndpoint) -> novarocks::QueryControlEndpoint {
    novarocks::QueryControlEndpoint {
        host: endpoint.host().to_string(),
        port: u32::from(endpoint.port()),
    }
}

fn decode_endpoint(
    endpoint: &novarocks::QueryControlEndpoint,
) -> Result<QueryControlEndpoint, QueryLifecycleError> {
    let port = u16::try_from(endpoint.port).map_err(|_| {
        QueryLifecycleError::invalid_manifest("query control endpoint port exceeds u16 range")
    })?;
    QueryControlEndpoint::new(endpoint.host.clone(), port)
}

fn encode_backend_identity(
    backend: &ParticipantBackendIdentity,
) -> novarocks::ParticipantBackendIdentity {
    novarocks::ParticipantBackendIdentity {
        backend_id: backend.backend_id(),
        endpoint: Some(encode_endpoint(backend.endpoint())),
        start_epoch: backend.start_epoch(),
    }
}

fn decode_backend_identity(
    backend: &novarocks::ParticipantBackendIdentity,
) -> Result<ParticipantBackendIdentity, QueryLifecycleError> {
    let endpoint = backend
        .endpoint
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::invalid_manifest("participant backend endpoint is required")
        })
        .and_then(decode_endpoint)?;
    ParticipantBackendIdentity::new(backend.backend_id, endpoint, backend.start_epoch)
}

fn decode_participant_role(role: i32) -> Result<ParticipantRole, QueryLifecycleError> {
    match role {
        1 => Ok(ParticipantRole::FragmentExecutor),
        2 => Ok(ParticipantRole::RuntimeFilterService),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown participant role {value}"
        ))),
    }
}

fn encode_exchange_route(route: &ExchangeRouteManifest) -> novarocks::ExchangeRouteManifest {
    novarocks::ExchangeRouteManifest {
        source_fragment_instance_id: Some(encode_unique_id(route.source_fragment_instance_id())),
        destination_fragment_instance_id: Some(encode_unique_id(
            route.destination_fragment_instance_id(),
        )),
        destination_node_id: route.destination_node_id(),
        sender_ordinal: route.sender_ordinal(),
        sender_count: route.sender_count(),
    }
}

fn decode_exchange_route(
    route: &novarocks::ExchangeRouteManifest,
) -> Result<ExchangeRouteManifest, QueryLifecycleError> {
    let source = route.source_fragment_instance_id.as_ref().ok_or_else(|| {
        QueryLifecycleError::invalid_manifest(
            "exchange route source fragment instance id is required",
        )
    })?;
    let destination = route
        .destination_fragment_instance_id
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::invalid_manifest(
                "exchange route destination fragment instance id is required",
            )
        })?;
    ExchangeRouteManifest::new(
        decode_unique_id(source)?,
        decode_unique_id(destination)?,
        route.destination_node_id,
        route.sender_ordinal,
        route.sender_count,
    )
}

fn encode_runtime_filter_contribution(
    execution_id: QueryExecutionId,
    contribution: &RuntimeFilterContribution,
) -> Result<novarocks::RuntimeFilterContribution, QueryLifecycleError> {
    let envelope = crate::protocol::native::encode_participant_install(
        execution_id.query_id().into_unique_id(),
        contribution.lifecycle(),
        contribution.install(),
    )
    .map_err(|error| QueryLifecycleError::invalid_manifest(error.to_string()))?;
    Ok(novarocks::RuntimeFilterContribution {
        participant_id: contribution.participant_id(),
        lifecycle: envelope.lifecycle,
        install: envelope.install,
        contribution_digest: contribution.digest().to_vec(),
    })
}

fn decode_runtime_filter_contribution(
    execution_id: QueryExecutionId,
    contribution: &novarocks::RuntimeFilterContribution,
) -> Result<RuntimeFilterContribution, QueryLifecycleError> {
    let digest: [u8; 32] = contribution
        .contribution_digest
        .as_slice()
        .try_into()
        .map_err(|_| {
            QueryLifecycleError::invalid_manifest(
                "runtime filter contribution digest must be 32 bytes",
            )
        })?;
    let envelope = filter::InstallRuntimeFilterDeploymentRequest {
        query_id: Some(common::UniqueId {
            hi: execution_id.query_id().high(),
            lo: execution_id.query_id().low(),
        }),
        deployment_epoch: execution_id.attempt_id().get(),
        participant_id: contribution.participant_id,
        lifecycle: contribution.lifecycle.clone(),
        install: contribution.install.clone(),
    };
    let decoded = crate::protocol::native::decode_participant_install(&envelope)
        .map_err(|error| QueryLifecycleError::invalid_manifest(error.to_string()))?;
    RuntimeFilterContribution::new(
        contribution.participant_id,
        decoded.lifecycle,
        decoded.install,
        digest,
    )
}

fn encode_init_outcome(outcome: QueryInitOutcome) -> i32 {
    match outcome {
        QueryInitOutcome::Applied => 1,
        QueryInitOutcome::AlreadyApplied => 2,
        QueryInitOutcome::RejectedConflict => 3,
        QueryInitOutcome::RejectedStaleBackend => 4,
        QueryInitOutcome::RejectedCapacity => 5,
        QueryInitOutcome::RejectedInvalidManifest => 6,
        QueryInitOutcome::RejectedTerminated => 7,
    }
}

fn decode_init_outcome(outcome: i32) -> Result<QueryInitOutcome, QueryLifecycleError> {
    match outcome {
        1 => Ok(QueryInitOutcome::Applied),
        2 => Ok(QueryInitOutcome::AlreadyApplied),
        3 => Ok(QueryInitOutcome::RejectedConflict),
        4 => Ok(QueryInitOutcome::RejectedStaleBackend),
        5 => Ok(QueryInitOutcome::RejectedCapacity),
        6 => Ok(QueryInitOutcome::RejectedInvalidManifest),
        7 => Ok(QueryInitOutcome::RejectedTerminated),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown query init outcome {value}"
        ))),
    }
}

fn encode_termination_reason(reason: QueryTerminationReason) -> i32 {
    match reason {
        QueryTerminationReason::CoordinatorAbort => 1,
        QueryTerminationReason::CoordinatorFinalize => 2,
        QueryTerminationReason::CoordinatorStreamLost => 3,
        QueryTerminationReason::CoordinatorHeartbeatTimeout => 4,
        QueryTerminationReason::LocalFailure => 5,
        QueryTerminationReason::PreStartTimeout => 6,
    }
}

fn decode_termination_reason(reason: i32) -> Result<QueryTerminationReason, QueryLifecycleError> {
    match reason {
        1 => Ok(QueryTerminationReason::CoordinatorAbort),
        2 => Ok(QueryTerminationReason::CoordinatorFinalize),
        3 => Ok(QueryTerminationReason::CoordinatorStreamLost),
        4 => Ok(QueryTerminationReason::CoordinatorHeartbeatTimeout),
        5 => Ok(QueryTerminationReason::LocalFailure),
        6 => Ok(QueryTerminationReason::PreStartTimeout),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown query termination reason {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::{QueryInitRequest, decode_query_init_request, encode_query_init_request};
    use crate::exec::spill::{SpillConfig, SpillMode};
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::identity::{AttemptId, QueryExecutionId};
    use crate::query_execution::lifecycle::manifest::{
        ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions, ParticipantRole,
        QueryControlEndpoint, RuntimeFilterContribution,
    };
    use crate::runtime::query_options::{QueryCacheOptions, QueryOptions};
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::install::{
        RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 42),
            AttemptId::new(7).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn service_only_request() -> QueryInitRequest {
        let participant = RuntimeFilterParticipantId::new(3);
        let epoch = DeploymentEpoch::new(7);
        let install = RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(epoch, participant, BTreeMap::new()),
            RuntimeFilterRoutingShard::new(epoch, participant, BTreeMap::new())
                .expect("empty routing shard is structurally valid"),
        );
        let lifecycle = crate::protocol::native::RuntimeFilterQueryLifecycleOptions {
            delivery_expire: Duration::from_secs(5),
            query_expire: Duration::from_secs(30),
            transport_retry_interval: Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: Duration::from_secs(2),
            transport_max_pending_entries: 1024,
            transport_max_pending_bytes: 1 << 20,
        };
        let contribution = RuntimeFilterContribution::new(3, lifecycle, install, [0x5a; 32])
            .expect("valid contribution");
        let options = QueryOptions {
            batch_size: Some(4096),
            query_timeout: Some(120),
            query_delivery_timeout: Some(60),
            enable_profile: true,
            runtime_profile_report_interval: Some(10),
            pipeline_dop: Some(4),
            exec_mem_limit: Some(1 << 30),
            connector_io_tasks_per_scan_operator: Some(8),
            orc_use_column_names: true,
            enable_file_metacache: true,
            enable_file_pagecache: true,
            enable_parquet_reader_page_index: true,
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(500),
            allow_throw_exception: true,
            group_concat_max_len: Some(1024),
            enable_join_runtime_bitset_filter: Some(true),
            global_runtime_filter_build_max_size: Some(1 << 20),
            cache: QueryCacheOptions {
                enable_scan_datacache: true,
                enable_populate_datacache: true,
                enable_datacache_async_populate_mode: true,
                enable_datacache_io_adaptor: true,
                enable_cache_select: true,
                datacache_evict_probability: Some(10),
                datacache_priority: Some(2),
                datacache_ttl_seconds: Some(300),
                datacache_sharing_work_period: Some(30),
            },
            spill: Some(SpillConfig {
                enable_spill: true,
                spill_mode: SpillMode::Force,
                spill_mem_limit_threshold: Some(0.75),
                spill_operator_min_bytes: Some(1024),
                spill_operator_max_bytes: Some(8192),
                spill_encode_level: Some(3),
                enable_spill_buffer_read: Some(true),
                max_spill_read_buffer_bytes_per_driver: Some(16384),
                spill_mem_table_size: Some(512),
                spill_mem_table_num: Some(2),
            }),
            ..Default::default()
        };
        let manifest = ParticipantManifest::new(
            execution_id(),
            ParticipantBackendIdentity::new(
                2,
                QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
                11,
            )
            .expect("valid backend"),
            [ParticipantRole::RuntimeFilterService],
            [],
            ParticipantQueryOptions::new(options),
            10_000,
            [],
            Some(contribution),
            Duration::from_secs(30),
            QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
        )
        .expect("valid service-only manifest");
        QueryInitRequest::from_manifest(manifest)
    }

    #[test]
    fn proto_query_lifecycle_round_trips_all_query_options() {
        let request = service_only_request();
        let wire = encode_query_init_request(&request).expect("request encodes");
        let decoded = decode_query_init_request(&wire).expect("request decodes");

        assert_eq!(decoded.manifest(), request.manifest());
        assert_eq!(decoded.digest(), request.digest());
        let options = decoded.manifest().query_options().native();
        assert!(options.orc_use_column_names);
        assert!(options.enable_file_metacache);
        assert!(options.enable_file_pagecache);
        assert!(options.enable_parquet_reader_page_index);
    }

    #[test]
    fn proto_query_lifecycle_rejects_unknown_role() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.manifest.as_mut().expect("manifest").participant_roles = vec![99];

        assert!(decode_query_init_request(&wire).is_err());
    }

    #[test]
    fn proto_query_lifecycle_rejects_missing_execution_id() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.manifest.as_mut().expect("manifest").execution_id = None;

        assert!(decode_query_init_request(&wire).is_err());
    }

    #[test]
    fn proto_query_lifecycle_rejects_wrong_digest_length() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.init_digest.pop();

        assert!(decode_query_init_request(&wire).is_err());
    }

    #[test]
    fn proto_query_lifecycle_rejects_zero_attempt() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.manifest
            .as_mut()
            .expect("manifest")
            .execution_id
            .as_mut()
            .expect("execution id")
            .attempt_id = 0;

        assert!(decode_query_init_request(&wire).is_err());
    }
}
