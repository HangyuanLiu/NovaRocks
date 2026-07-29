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

use std::collections::{BTreeMap, VecDeque};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::query_execution::backend::LiveBackendTarget;
use crate::query_execution::lifecycle::contract::{
    decode_abort_query_response, decode_query_control_event, decode_query_init_response,
    encode_abort_query_request, encode_query_control_attach, encode_query_control_command,
    encode_query_init_request,
};
use crate::query_execution::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlCommand, QueryControlEvent,
    QueryControlSession, QueryInitAck, QueryInitRequest, QueryLifecycleTarget,
    QueryLifecycleTransport, QueryLifecycleTransportError, QueryLifecycleTransportErrorKind,
    QueryTerminationAck,
};
use crate::runtime::global_async_runtime::{data_block_on, data_runtime_handle};
use crate::service::grpc_client::{NovaRocksGrpcRemoteClient, QueryLifecycleRpcError};

const QUERY_CONTROL_CHANNEL_CAPACITY: usize = 32;

struct GrpcQueryLifecycleTransport {
    backends: BTreeMap<usize, GrpcQueryLifecycleBackend>,
}

struct GrpcQueryLifecycleBackend {
    target: QueryLifecycleTarget,
    client: NovaRocksGrpcRemoteClient,
}

pub fn new_grpc_query_lifecycle_transport(
    backends: &[LiveBackendTarget],
) -> Result<Arc<dyn QueryLifecycleTransport>, String> {
    if backends.is_empty() {
        return Err("gRPC query lifecycle transport requires at least one backend".to_string());
    }
    let mut clients = BTreeMap::new();
    for backend in backends {
        let target = QueryLifecycleTarget::new(
            backend.backend_idx(),
            backend.endpoint(),
            backend.start_epoch(),
        );
        if clients.contains_key(&backend.backend_idx()) {
            return Err(format!("duplicate backend_idx {}", backend.backend_idx()));
        }
        clients.insert(
            backend.backend_idx(),
            GrpcQueryLifecycleBackend {
                target,
                client: NovaRocksGrpcRemoteClient::new(backend.endpoint())?,
            },
        );
    }
    Ok(Arc::new(GrpcQueryLifecycleTransport { backends: clients }))
}

impl GrpcQueryLifecycleTransport {
    fn backend(
        &self,
        target: QueryLifecycleTarget,
    ) -> Result<&GrpcQueryLifecycleBackend, QueryLifecycleTransportError> {
        let backend = self.backends.get(&target.backend_idx()).ok_or_else(|| {
            transport_error(
                QueryLifecycleTransportErrorKind::Unavailable,
                format!(
                    "backend {} is absent from the frozen lifecycle topology",
                    target.backend_idx()
                ),
            )
        })?;
        if backend.target != target {
            return Err(transport_error(
                QueryLifecycleTransportErrorKind::Unavailable,
                format!(
                    "backend {} target changed from {}@{} to {}@{}",
                    target.backend_idx(),
                    backend.target.endpoint(),
                    backend.target.start_epoch(),
                    target.endpoint(),
                    target.start_epoch()
                ),
            ));
        }
        Ok(backend)
    }
}

impl QueryLifecycleTransport for GrpcQueryLifecycleTransport {
    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryInitRequest,
        timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError> {
        let backend = self.backend(target)?;
        validate_init_target(target, &request)?;
        let execution_id = request.manifest().execution_id();
        let digest = request.digest();
        let wire = encode_query_init_request(&request)
            .map_err(|error| invalid_response("encode InitQuery request", error))?;
        let response = data_block_on(backend.client.init_query_async(wire, timeout))
            .map_err(|error| unavailable("drive InitQuery", error))?
            .map_err(|error| init_rpc_error("InitQuery", error))?;
        let ack = decode_query_init_response(&response)
            .map_err(|error| invalid_response("decode InitQuery response", error))?;
        if ack.execution_id() != execution_id {
            return Err(transport_error(
                QueryLifecycleTransportErrorKind::InvalidResponse,
                "InitQuery acknowledgement execution id mismatch",
            ));
        }
        if ack.digest() != digest {
            return Err(transport_error(
                QueryLifecycleTransportErrorKind::InvalidResponse,
                "InitQuery acknowledgement digest mismatch",
            ));
        }
        if let Some(token) = claim_backend_fault(target.backend_idx(), "init-ack-drop") {
            eprintln!(
                "NOVAROCKS_QUERY_INIT_ACK_DROPPED execution_id={}:{}:{} backend_index={} token={token}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                target.backend_idx()
            );
            return Err(transport_error(
                QueryLifecycleTransportErrorKind::DeadlineExceeded,
                format!(
                    "runner-owned InitAck drop after backend {} applied execution {}:{}:{}",
                    target.backend_idx(),
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    execution_id.attempt_id().get()
                ),
            ));
        }
        Ok(ack)
    }

    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: QueryControlAttach,
        timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
        let backend = self.backend(target)?;
        let (command_tx, command_rx) = mpsc::channel(QUERY_CONTROL_CHANNEL_CAPACITY);
        command_tx
            .try_send(encode_query_control_attach(&attach))
            .map_err(|error| {
                transport_error(
                    QueryLifecycleTransportErrorKind::Unavailable,
                    format!("enqueue QueryControl Attach: {error}"),
                )
            })?;
        let stream = data_block_on(
            backend
                .client
                .attach_query_control_async(ReceiverStream::new(command_rx), timeout),
        )
        .map_err(|error| unavailable("drive QueryControl Attach", error))?
        .map_err(|error| rpc_error("QueryControl Attach", error, false))?;
        let (event_tx, event_rx) = mpsc::channel(QUERY_CONTROL_CHANNEL_CAPACITY);
        let commands = Arc::new(Mutex::new(QueryControlCommandState::new(command_tx)));
        let commands_for_bridge = Arc::clone(&commands);
        let bridge = data_runtime_handle()
            .map_err(|error| unavailable("start QueryControl bridge", error))?
            .spawn(run_query_control_bridge(
                stream,
                event_tx,
                commands_for_bridge,
            ));
        Ok(Arc::new(GrpcQueryControlSession {
            target,
            execution_id: attach.execution_id(),
            commands,
            events: Mutex::new(event_rx),
            bridge: Mutex::new(Some(bridge)),
            heartbeat_stopped: AtomicBool::new(false),
        }))
    }

    fn abort_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryAbortRequest,
        timeout: Duration,
    ) -> Result<QueryTerminationAck, QueryLifecycleTransportError> {
        let backend = self.backend(target)?;
        let execution_id = request.execution_id();
        let wire = encode_abort_query_request(&request);
        let response = data_block_on(backend.client.abort_query_async(wire, timeout))
            .map_err(|error| unavailable("drive AbortQuery", error))?
            .map_err(|error| rpc_error("AbortQuery", error, false))?;
        let ack = decode_abort_query_response(&response)
            .map_err(|error| invalid_response("decode AbortQuery response", error))?;
        if ack.execution_id() != execution_id {
            return Err(transport_error(
                QueryLifecycleTransportErrorKind::InvalidResponse,
                "AbortQuery acknowledgement execution id mismatch",
            ));
        }
        Ok(ack)
    }
}

struct GrpcQueryControlSession {
    target: QueryLifecycleTarget,
    execution_id: crate::query_execution::lifecycle::QueryExecutionId,
    commands: Arc<Mutex<QueryControlCommandState>>,
    events: Mutex<mpsc::Receiver<Result<QueryControlEvent, QueryLifecycleTransportError>>>,
    bridge: Mutex<Option<tokio::task::JoinHandle<()>>>,
    heartbeat_stopped: AtomicBool,
}

impl QueryControlSession for GrpcQueryControlSession {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError> {
        if matches!(command, QueryControlCommand::Heartbeat { .. })
            && (self.heartbeat_stopped.load(Ordering::Acquire)
                || claim_backend_fault(self.target.backend_idx(), "heartbeat-stop").is_some_and(
                    |token| {
                        self.heartbeat_stopped.store(true, Ordering::Release);
                        eprintln!(
                            "NOVAROCKS_QUERY_CONTROL_HEARTBEAT_STOPPED execution_id={}:{}:{} backend_index={} token={token}",
                            self.execution_id.query_id().high(),
                            self.execution_id.query_id().low(),
                            self.execution_id.attempt_id().get(),
                            self.target.backend_idx()
                        );
                        true
                    },
                ))
        {
            return Ok(());
        }
        let mut state = self
            .commands
            .lock()
            .map_err(|_| unavailable("lock QueryControl command channel", "poisoned lock"))?;
        let sender = state.sender.as_ref().ok_or_else(|| {
            state.terminal_error.clone().unwrap_or_else(|| {
                transport_error(
                    QueryLifecycleTransportErrorKind::StreamClosed,
                    "query control stream is closed",
                )
            })
        })?;
        if state.pending.len() >= QUERY_CONTROL_CHANNEL_CAPACITY {
            return Err(transport_error(
                QueryLifecycleTransportErrorKind::Backpressure,
                "query control pending command capacity is exhausted",
            ));
        }
        sender
            .try_send(encode_query_control_command(&command))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => transport_error(
                    QueryLifecycleTransportErrorKind::Backpressure,
                    "query control command capacity is exhausted",
                ),
                mpsc::error::TrySendError::Closed(_) => transport_error(
                    QueryLifecycleTransportErrorKind::StreamClosed,
                    "query control command stream is closed",
                ),
            })?;
        state
            .pending
            .push_back(PendingQueryControlCommand::from(&command));
        Ok(())
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, QueryLifecycleTransportError> {
        let mut events = self
            .events
            .lock()
            .map_err(|_| unavailable("lock QueryControl event channel", "poisoned lock"))?;
        data_block_on(async {
            tokio::time::timeout(timeout, events.recv())
                .await
                .map_err(|_| {
                    transport_error(
                        QueryLifecycleTransportErrorKind::DeadlineExceeded,
                        "query control event receive deadline exceeded",
                    )
                })?
                .ok_or_else(|| {
                    transport_error(
                        QueryLifecycleTransportErrorKind::StreamClosed,
                        "query control event stream is closed",
                    )
                })?
        })
        .map_err(|error| unavailable("drive QueryControl event receive", error))?
    }
}

#[cfg(debug_assertions)]
fn claim_backend_fault(backend_index: usize, kind: &str) -> Option<String> {
    let root = crate::common::config::sql_test_query_lifecycle_fault_dir()?;
    let path = root.join(format!("be-{backend_index}.{kind}.trigger"));
    let token = std::fs::read_to_string(&path).ok()?;
    let token = token.trim();
    if token.is_empty()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return None;
    }
    std::fs::remove_file(&path).ok()?;
    Some(token.to_string())
}

#[cfg(not(debug_assertions))]
fn claim_backend_fault(_backend_index: usize, _kind: &str) -> Option<String> {
    None
}

impl Drop for GrpcQueryControlSession {
    fn drop(&mut self) {
        if let Ok(mut state) = self.commands.lock() {
            state.close(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "query control session was dropped",
            ));
        }
        if let Ok(bridge) = self.bridge.get_mut()
            && let Some(bridge) = bridge.take()
        {
            bridge.abort();
        }
    }
}

async fn run_query_control_bridge(
    mut stream: tonic::Streaming<crate::proto::novarocks::QueryControlResponse>,
    events: mpsc::Sender<Result<QueryControlEvent, QueryLifecycleTransportError>>,
    commands: Arc<Mutex<QueryControlCommandState>>,
) {
    loop {
        let decoded = match stream.message().await {
            Ok(Some(response)) => match decode_query_control_event(&response) {
                Ok(event) => Ok(event),
                Err(error) => Err(invalid_response("decode QueryControl event", error)),
            },
            Ok(None) => Err(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "query control response stream closed",
            )),
            Err(status) => Err(status_error("QueryControl stream", status, true)),
        };
        let (next, terminal) = prepare_query_control_event(decoded, &commands);
        if events.send(next).await.is_err() {
            close_query_control_commands(
                &commands,
                transport_error(
                    QueryLifecycleTransportErrorKind::StreamClosed,
                    "query control event receiver was dropped",
                ),
            );
            break;
        }
        if terminal {
            break;
        }
    }
}

struct QueryControlCommandState {
    sender: Option<mpsc::Sender<crate::proto::novarocks::QueryControlRequest>>,
    pending: VecDeque<PendingQueryControlCommand>,
    terminal_error: Option<QueryLifecycleTransportError>,
}

impl QueryControlCommandState {
    fn new(sender: mpsc::Sender<crate::proto::novarocks::QueryControlRequest>) -> Self {
        Self {
            sender: Some(sender),
            pending: VecDeque::new(),
            terminal_error: None,
        }
    }

    fn close(&mut self, error: QueryLifecycleTransportError) {
        self.sender.take();
        self.terminal_error.get_or_insert(error);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingQueryControlCommand {
    Heartbeat { sequence: u64 },
    Abort,
    Finalize,
}

impl From<&QueryControlCommand> for PendingQueryControlCommand {
    fn from(command: &QueryControlCommand) -> Self {
        match command {
            QueryControlCommand::Heartbeat { sequence, .. } => Self::Heartbeat {
                sequence: *sequence,
            },
            QueryControlCommand::Abort { .. } => Self::Abort,
            QueryControlCommand::Finalize => Self::Finalize,
        }
    }
}

fn prepare_query_control_event(
    decoded: Result<QueryControlEvent, QueryLifecycleTransportError>,
    commands: &Mutex<QueryControlCommandState>,
) -> (
    Result<QueryControlEvent, QueryLifecycleTransportError>,
    bool,
) {
    let mut state = match commands.lock() {
        Ok(state) => state,
        Err(_) => {
            return (
                Err(unavailable(
                    "lock QueryControl command state",
                    "poisoned lock",
                )),
                true,
            );
        }
    };
    let next = match decoded {
        Ok(event) => match validate_query_control_event(&event, &mut state) {
            Ok(()) => Ok(event),
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    };
    let terminal = matches!(
        next,
        Ok(QueryControlEvent::LocalFailure { .. })
            | Ok(QueryControlEvent::TerminationAccepted { .. })
            | Err(_)
    );
    if terminal {
        let terminal_error = terminal_command_error(&next);
        state.close(terminal_error);
    }
    (next, terminal)
}

fn validate_query_control_event(
    event: &QueryControlEvent,
    state: &mut QueryControlCommandState,
) -> Result<(), QueryLifecycleTransportError> {
    match event {
        QueryControlEvent::ControlReady | QueryControlEvent::LocalFailure { .. } => Ok(()),
        QueryControlEvent::HeartbeatAck { sequence } => match state.pending.front() {
            Some(PendingQueryControlCommand::Heartbeat {
                sequence: expected,
            }) if sequence == expected => {
                state.pending.pop_front();
                Ok(())
            }
            Some(expected) => Err(invalid_response(
                "validate QueryControl HeartbeatAck",
                format!("expected {expected:?}, received heartbeat sequence {sequence}"),
            )),
            None => Err(invalid_response(
                "validate QueryControl HeartbeatAck",
                format!("received unsolicited heartbeat sequence {sequence}"),
            )),
        },
        QueryControlEvent::TerminationAccepted { reason } => match state.pending.front() {
            Some(PendingQueryControlCommand::Abort) => {
                state.pending.pop_front();
                Ok(())
            }
            Some(PendingQueryControlCommand::Finalize)
                if *reason == crate::query_execution::lifecycle::QueryTerminationReason::CoordinatorFinalize =>
            {
                state.pending.pop_front();
                Ok(())
            }
            Some(expected) => Err(invalid_response(
                "validate QueryControl TerminationAccepted",
                format!("expected {expected:?}, received termination reason {reason:?}"),
            )),
            None => Err(invalid_response(
                "validate QueryControl TerminationAccepted",
                format!("received unsolicited termination reason {reason:?}"),
            )),
        },
    }
}

fn terminal_command_error(
    event: &Result<QueryControlEvent, QueryLifecycleTransportError>,
) -> QueryLifecycleTransportError {
    match event {
        Err(error) => error.clone(),
        Ok(QueryControlEvent::LocalFailure { code, detail }) => transport_error(
            QueryLifecycleTransportErrorKind::StreamClosed,
            format!("query control stream terminated by local failure {code}: {detail}"),
        ),
        Ok(QueryControlEvent::TerminationAccepted { reason }) => transport_error(
            QueryLifecycleTransportErrorKind::StreamClosed,
            format!("query control stream terminated with {reason:?}"),
        ),
        Ok(_) => transport_error(
            QueryLifecycleTransportErrorKind::StreamClosed,
            "query control stream terminated",
        ),
    }
}

fn close_query_control_commands(
    commands: &Mutex<QueryControlCommandState>,
    error: QueryLifecycleTransportError,
) {
    if let Ok(mut state) = commands.lock() {
        state.close(error);
    }
}

fn validate_init_target(
    target: QueryLifecycleTarget,
    request: &QueryInitRequest,
) -> Result<(), QueryLifecycleTransportError> {
    let identity = request.manifest().backend();
    let backend_id = usize::try_from(identity.backend_id()).map_err(|_| {
        transport_error(
            QueryLifecycleTransportErrorKind::InvalidResponse,
            "InitQuery backend id exceeds usize",
        )
    })?;
    let endpoint_ip = IpAddr::from_str(identity.endpoint().host()).map_err(|error| {
        transport_error(
            QueryLifecycleTransportErrorKind::InvalidResponse,
            format!("InitQuery backend endpoint is not an IP address: {error}"),
        )
    })?;
    let endpoint = std::net::SocketAddr::new(endpoint_ip, identity.endpoint().port());
    if backend_id != target.backend_idx()
        || endpoint != target.endpoint()
        || identity.start_epoch() != target.start_epoch()
    {
        return Err(transport_error(
            QueryLifecycleTransportErrorKind::InvalidResponse,
            format!(
                "InitQuery manifest backend identity does not match frozen target {} {}@{}",
                target.backend_idx(),
                target.endpoint(),
                target.start_epoch()
            ),
        ));
    }
    Ok(())
}

fn rpc_error(
    operation: &str,
    error: QueryLifecycleRpcError,
    stream: bool,
) -> QueryLifecycleTransportError {
    match error {
        QueryLifecycleRpcError::PreSubmission(detail) => {
            transport_error(QueryLifecycleTransportErrorKind::Unavailable, detail)
        }
        QueryLifecycleRpcError::PostSubmissionDeadlineExceeded(detail) => {
            transport_error(QueryLifecycleTransportErrorKind::DeadlineExceeded, detail)
        }
        QueryLifecycleRpcError::PostSubmissionStatus(status) => {
            status_error(operation, status, stream)
        }
    }
}

fn init_rpc_error(operation: &str, error: QueryLifecycleRpcError) -> QueryLifecycleTransportError {
    match error {
        QueryLifecycleRpcError::PreSubmission(detail) => {
            transport_error(QueryLifecycleTransportErrorKind::Unavailable, detail)
        }
        QueryLifecycleRpcError::PostSubmissionDeadlineExceeded(detail) => {
            transport_error(QueryLifecycleTransportErrorKind::DeadlineExceeded, detail)
        }
        QueryLifecycleRpcError::PostSubmissionStatus(status)
            if matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::Unknown
            ) =>
        {
            transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                format!(
                    "{operation} rpc outcome is unknown after status {:?}: {}",
                    status.code(),
                    status.message()
                ),
            )
        }
        QueryLifecycleRpcError::PostSubmissionStatus(status) => {
            status_error(operation, status, false)
        }
    }
}

fn status_error(
    operation: &str,
    status: tonic::Status,
    stream: bool,
) -> QueryLifecycleTransportError {
    let kind = if stream {
        match status.code() {
            tonic::Code::InvalidArgument | tonic::Code::DataLoss => {
                QueryLifecycleTransportErrorKind::InvalidResponse
            }
            _ => QueryLifecycleTransportErrorKind::StreamClosed,
        }
    } else {
        match status.code() {
            tonic::Code::DeadlineExceeded => QueryLifecycleTransportErrorKind::DeadlineExceeded,
            tonic::Code::ResourceExhausted => QueryLifecycleTransportErrorKind::Backpressure,
            tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::Unknown => {
                QueryLifecycleTransportErrorKind::Unavailable
            }
            _ => QueryLifecycleTransportErrorKind::InvalidResponse,
        }
    };
    transport_error(
        kind,
        format!(
            "{operation} rpc status {:?}: {}",
            status.code(),
            status.message()
        ),
    )
}

fn invalid_response(context: &str, error: impl std::fmt::Display) -> QueryLifecycleTransportError {
    transport_error(
        QueryLifecycleTransportErrorKind::InvalidResponse,
        format!("{context}: {error}"),
    )
}

fn unavailable(context: &str, error: impl std::fmt::Display) -> QueryLifecycleTransportError {
    transport_error(
        QueryLifecycleTransportErrorKind::Unavailable,
        format!("{context}: {error}"),
    )
}

fn transport_error(
    kind: QueryLifecycleTransportErrorKind,
    detail: impl Into<String>,
) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::query_execution::backend::LiveBackendTarget;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::{
        AttemptId, BackendQueryControl, ParticipantBackendIdentity, ParticipantManifest,
        ParticipantQueryOptions, ParticipantRole, QueryAbortRequest, QueryControlAttach,
        QueryControlAttachment, QueryControlCommand, QueryControlEndpoint, QueryControlEvent,
        QueryExecutionId, QueryInitAck, QueryInitOutcome, QueryInitRequest, QueryLifecycleError,
        QueryLifecycleErrorCode, QueryLifecycleIngress, QueryLifecycleTarget,
        QueryLifecycleTransportErrorKind, QueryTerminationAck, QueryTerminationReason,
    };
    use crate::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
    use crate::runtime::query_options::QueryOptions;
    use crate::service::grpc_client::QueryLifecycleRpcError;
    use crate::service::grpc_server::{GrpcService, rejecting_test_native_fragment_ingress};
    use futures::stream;

    use super::{
        PendingQueryControlCommand, QueryControlCommandState, init_rpc_error,
        new_grpc_query_lifecycle_transport, prepare_query_control_event,
    };

    #[test]
    fn grpc_query_lifecycle_client_rejects_empty_topology() {
        let error = match new_grpc_query_lifecycle_transport(&[]) {
            Ok(_) => panic!("an empty frozen topology must be rejected"),
            Err(error) => error,
        };

        assert!(error.contains("at least one backend"));
    }

    #[test]
    fn grpc_query_lifecycle_client_rejects_duplicate_backend_identity() {
        let endpoint: SocketAddr = "127.0.0.1:19090".parse().expect("valid endpoint");
        let error = match new_grpc_query_lifecycle_transport(&[
            LiveBackendTarget::new(7, endpoint, 11),
            LiveBackendTarget::new(7, endpoint, 11),
        ]) {
            Ok(_) => panic!("duplicate backend identities must be rejected"),
            Err(error) => error,
        };

        assert!(error.contains("duplicate backend_idx 7"));
    }

    #[test]
    fn grpc_query_lifecycle_client_classifies_post_submission_init_loss_as_unknown() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::Cancelled,
            tonic::Code::Unknown,
        ] {
            let error = init_rpc_error(
                "InitQuery",
                QueryLifecycleRpcError::PostSubmissionStatus(tonic::Status::new(
                    code,
                    "lost acknowledgement",
                )),
            );
            assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed);
            assert!(error.is_unknown_init_outcome());
        }
    }

    #[test]
    fn grpc_query_lifecycle_client_closes_commands_while_preparing_terminal_event() {
        let (command_tx, _command_rx) = tokio::sync::mpsc::channel(32);
        let commands = Mutex::new(QueryControlCommandState::new(command_tx));
        commands
            .lock()
            .expect("command state")
            .pending
            .push_back(PendingQueryControlCommand::Finalize);

        let (next, terminal) = prepare_query_control_event(
            Ok(QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorFinalize,
            }),
            &commands,
        );

        assert!(terminal);
        assert_eq!(
            next.expect("terminal event remains observable"),
            QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorFinalize,
            }
        );
        let state = commands.lock().expect("command state");
        assert!(
            state.sender.is_none(),
            "command sender must close before terminal event publication"
        );
        assert_eq!(
            state
                .terminal_error
                .as_ref()
                .expect("terminal command error")
                .kind(),
            QueryLifecycleTransportErrorKind::StreamClosed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_query_lifecycle_client_live_loopback_preserves_identity_and_orders_events() {
        let ingress = Arc::new(LoopbackLifecycleIngress::default());
        let (address, shutdown, server) = spawn_loopback(GrpcService::with_fragment_execution(
            rejecting_test_native_fragment_ingress(),
            ingress.clone(),
            Arc::new(AcceptingReportHandler),
        ))
        .await;
        let live = LiveBackendTarget::new(7, address, 77);
        let transport =
            new_grpc_query_lifecycle_transport(&[live]).expect("create production transport");
        let target = QueryLifecycleTarget::new(7, address, 77);
        let request = init_request(target, 801);
        let execution_id = request.manifest().execution_id();
        let digest = request.digest();

        let ack = transport
            .init_query(target, request, Duration::from_secs(2))
            .expect("InitQuery succeeds over live gRPC");
        assert_eq!(ack.execution_id(), execution_id);
        assert_eq!(ack.digest(), digest);
        assert_eq!(
            ingress
                .initialized_backend
                .lock()
                .expect("initialized backend")
                .clone(),
            Some(
                ParticipantBackendIdentity::new(
                    7,
                    QueryControlEndpoint::new(address.ip().to_string(), address.port())
                        .expect("endpoint"),
                    77,
                )
                .expect("identity"),
            )
        );

        let session = transport
            .attach_control(
                target,
                QueryControlAttach::new(execution_id, digest, 9).expect("attach"),
                Duration::from_secs(2),
            )
            .expect("attach live control stream");
        assert_eq!(
            session
                .recv_timeout(Duration::from_secs(2))
                .expect("ControlReady"),
            QueryControlEvent::ControlReady
        );
        session
            .send(QueryControlCommand::Heartbeat {
                sequence: 41,
                sent_mono_ns: 123,
            })
            .expect("send heartbeat");
        assert_eq!(
            session
                .recv_timeout(Duration::from_secs(2))
                .expect("HeartbeatAck"),
            QueryControlEvent::HeartbeatAck { sequence: 41 }
        );
        session
            .send(QueryControlCommand::Finalize)
            .expect("send finalize");
        assert_eq!(
            session
                .recv_timeout(Duration::from_secs(2))
                .expect("TerminationAccepted"),
            QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorFinalize,
            }
        );
        let abort_ack = transport
            .abort_query(
                target,
                QueryAbortRequest::new(execution_id, digest, "idempotent cleanup")
                    .expect("abort request"),
                Duration::from_secs(2),
            )
            .expect("AbortQuery succeeds over live gRPC");
        assert_eq!(abort_ack.execution_id(), execution_id);
        assert_eq!(
            abort_ack.accepted_reason(),
            QueryTerminationReason::CoordinatorAbort
        );

        let _ = shutdown.send(());
        server.await.expect("join loopback server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_query_lifecycle_client_bounds_inflight_and_reports_stream_reset() {
        let gate = Arc::new(HeartbeatGate::default());
        let ingress = Arc::new(LoopbackLifecycleIngress {
            gate: Some(Arc::clone(&gate)),
            ..Default::default()
        });
        let (address, shutdown, server) = spawn_loopback(GrpcService::with_fragment_execution(
            rejecting_test_native_fragment_ingress(),
            ingress,
            Arc::new(AcceptingReportHandler),
        ))
        .await;
        let live = LiveBackendTarget::new(7, address, 88);
        let transport =
            new_grpc_query_lifecycle_transport(&[live]).expect("create production transport");
        let target = QueryLifecycleTarget::new(7, address, 88);
        let request = init_request(target, 802);
        let execution_id = request.manifest().execution_id();
        let digest = request.digest();
        transport
            .init_query(target, request, Duration::from_secs(2))
            .expect("InitQuery");
        let session = transport
            .attach_control(
                target,
                QueryControlAttach::new(execution_id, digest, 10).expect("attach"),
                Duration::from_secs(2),
            )
            .expect("attach");
        assert_eq!(
            session
                .recv_timeout(Duration::from_secs(2))
                .expect("ControlReady"),
            QueryControlEvent::ControlReady
        );

        for sequence in 0..super::QUERY_CONTROL_CHANNEL_CAPACITY {
            session
                .send(QueryControlCommand::Heartbeat {
                    sequence: sequence as u64,
                    sent_mono_ns: sequence as u64,
                })
                .expect("bounded inflight command");
        }
        let error = session
            .send(QueryControlCommand::Heartbeat {
                sequence: 99,
                sent_mono_ns: 99,
            })
            .expect_err("the 33rd unacknowledged command must backpressure");
        assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::Backpressure);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !gate.entered.load(Ordering::Acquire) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "server never paused in heartbeat"
            );
            tokio::task::yield_now().await;
        }
        gate.release.store(true, Ordering::Release);
        let error = session
            .recv_timeout(Duration::from_secs(2))
            .expect_err("server reset must terminate the session");
        assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed);

        let _ = shutdown.send(());
        server.await.expect("join loopback server");
    }

    fn init_request(target: QueryLifecycleTarget, query_low: i64) -> QueryInitRequest {
        let execution_id = QueryExecutionId::new(
            QueryId::new(0x514c_4302, query_low),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        QueryInitRequest::from_manifest(
            ParticipantManifest::new(
                execution_id,
                ParticipantBackendIdentity::new(
                    target.backend_idx() as u64,
                    QueryControlEndpoint::new(
                        target.endpoint().ip().to_string(),
                        target.endpoint().port(),
                    )
                    .expect("backend endpoint"),
                    target.start_epoch(),
                )
                .expect("backend identity"),
                [ParticipantRole::FragmentExecutor],
                [crate::common::types::UniqueId {
                    hi: query_low,
                    lo: 1,
                }],
                ParticipantQueryOptions::new(QueryOptions::default()),
                10_000,
                [],
                None,
                Duration::from_secs(30),
                QueryControlEndpoint::new("127.0.0.1", 9031).expect("report endpoint"),
            )
            .expect("participant manifest"),
        )
    }

    async fn spawn_loopback(
        service: GrpcService,
    ) -> (
        SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let address = listener.local_addr().expect("loopback address");
        let incoming = stream::unfold(listener, |listener| async {
            let item = listener.accept().await.map(|(stream, _)| stream);
            Some((item, listener))
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    crate::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                        service,
                    ),
                )
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve loopback");
        });
        (address, shutdown_tx, server)
    }

    struct AcceptingReportHandler;

    impl NativeReportHandler for AcceptingReportHandler {
        fn handle_native_report(
            &self,
            _report: crate::proto::novarocks::ExecStatusReport,
        ) -> Result<(), NativeReportHandlerError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct LoopbackLifecycleIngress {
        initialized: Mutex<
            Option<(
                QueryExecutionId,
                crate::query_execution::lifecycle::ParticipantManifestDigest,
            )>,
        >,
        initialized_backend: Mutex<Option<ParticipantBackendIdentity>>,
        attached: AtomicBool,
        gate: Option<Arc<HeartbeatGate>>,
    }

    impl QueryLifecycleIngress for LoopbackLifecycleIngress {
        fn bind_backend_identity(&self, _backend_id: u64) -> Result<(), QueryLifecycleError> {
            Ok(())
        }

        fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
            let execution_id = request.manifest().execution_id();
            let digest = request.digest();
            *self
                .initialized_backend
                .lock()
                .expect("initialized backend") = Some(request.manifest().backend().clone());
            *self.initialized.lock().expect("initialized") = Some((execution_id, digest));
            QueryInitAck::new(execution_id, digest, QueryInitOutcome::Applied)
        }

        fn abort_query(
            &self,
            request: crate::query_execution::lifecycle::QueryAbortRequest,
        ) -> Result<QueryTerminationAck, QueryLifecycleError> {
            Ok(QueryTerminationAck::new(
                request.execution_id(),
                QueryTerminationReason::CoordinatorAbort,
            ))
        }

        fn attach_control(
            &self,
            attach: QueryControlAttach,
        ) -> Result<QueryControlAttachment, QueryLifecycleError> {
            if *self.initialized.lock().expect("initialized")
                != Some((attach.execution_id(), attach.digest()))
            {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Conflict,
                    "attach identity or digest mismatch",
                ));
            }
            if self.attached.swap(true, Ordering::AcqRel) {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Conflict,
                    "already attached",
                ));
            }
            let (events, receiver) = tokio::sync::mpsc::channel(32);
            events
                .try_send(QueryControlEvent::ControlReady)
                .expect("ControlReady");
            Ok(QueryControlAttachment {
                control: Arc::new(LoopbackControl {
                    events,
                    gate: self.gate.clone(),
                }),
                events: receiver,
            })
        }
    }

    struct LoopbackControl {
        events: tokio::sync::mpsc::Sender<QueryControlEvent>,
        gate: Option<Arc<HeartbeatGate>>,
    }

    impl BackendQueryControl for LoopbackControl {
        fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError> {
            if let Some(gate) = &self.gate {
                gate.entered.store(true, Ordering::Release);
                while !gate.release.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Transport,
                    "reset test stream",
                ));
            }
            self.events
                .try_send(QueryControlEvent::HeartbeatAck { sequence })
                .map_err(|error| {
                    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, error.to_string())
                })
        }

        fn abort(&self, _reason: String) -> Result<(), QueryLifecycleError> {
            self.events
                .try_send(QueryControlEvent::TerminationAccepted {
                    reason: QueryTerminationReason::CoordinatorAbort,
                })
                .map_err(|error| {
                    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, error.to_string())
                })
        }

        fn finalize(&self) -> Result<(), QueryLifecycleError> {
            self.events
                .try_send(QueryControlEvent::TerminationAccepted {
                    reason: QueryTerminationReason::CoordinatorFinalize,
                })
                .map_err(|error| {
                    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, error.to_string())
                })
        }

        fn coordinator_lost(
            &self,
            _reason: QueryTerminationReason,
        ) -> Result<(), QueryLifecycleError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct HeartbeatGate {
        entered: AtomicBool,
        release: AtomicBool,
    }
}
