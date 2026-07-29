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

use tokio_stream::wrappers::ReceiverStream;

use crate::proto::novarocks;
use crate::query_execution::lifecycle::contract::{
    decode_abort_query_request, decode_query_control_attach, decode_query_control_command,
    decode_query_init_request, encode_abort_query_response, encode_query_control_event,
    encode_query_init_response,
};
use crate::query_execution::lifecycle::{
    BackendQueryControl, QueryControlCommand, QueryControlEvent, QueryInitOutcome,
    QueryLifecycleError, QueryLifecycleErrorCode, QueryLifecycleIngress, QueryTerminationReason,
};

const CONTROL_STREAM_CAPACITY: usize = 16;

pub(crate) type QueryControlResponseStream =
    ReceiverStream<Result<novarocks::QueryControlResponse, tonic::Status>>;

pub(crate) fn handle_init_query(
    ingress: &dyn QueryLifecycleIngress,
    request: novarocks::InitQueryRequest,
) -> novarocks::InitQueryResponse {
    match decode_query_init_request(&request) {
        Ok(request) => encode_query_init_response(&ingress.init_query(request)),
        Err(_) => novarocks::InitQueryResponse {
            execution_id: request.manifest.and_then(|manifest| manifest.execution_id),
            init_digest: request.init_digest,
            outcome: encode_init_outcome(QueryInitOutcome::RejectedInvalidManifest),
        },
    }
}

pub(crate) fn handle_abort_query(
    ingress: &dyn QueryLifecycleIngress,
    request: novarocks::AbortQueryRequest,
) -> novarocks::AbortQueryResponse {
    match decode_abort_query_request(&request) {
        Ok(request) => encode_abort_query_response(&ingress.abort_query(request)),
        Err(_) => novarocks::AbortQueryResponse {
            execution_id: request.execution_id,
            accepted_reason: novarocks::QueryTerminationReason::Unspecified as i32,
        },
    }
}

pub(crate) async fn handle_query_control_stream(
    ingress: Arc<dyn QueryLifecycleIngress>,
    mut inbound: tonic::Streaming<novarocks::QueryControlRequest>,
) -> Result<QueryControlResponseStream, tonic::Status> {
    let first = inbound
        .message()
        .await
        .map_err(|error| tonic::Status::invalid_argument(format!("read attach frame: {error}")))?
        .ok_or_else(|| tonic::Status::failed_precondition("first frame must be Attach"))?;
    if !matches!(
        first.command,
        Some(novarocks::query_control_request::Command::Attach(_))
    ) {
        return Err(tonic::Status::failed_precondition(
            "first frame must be Attach",
        ));
    }
    let attach = decode_query_control_attach(&first).map_err(status_from_lifecycle_error)?;
    let attachment = ingress
        .attach_control(attach)
        .map_err(status_from_lifecycle_error)?;
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(CONTROL_STREAM_CAPACITY);
    let lease = CoordinatorLease::new(attachment.control);
    tokio::spawn(run_attached_control_stream(
        inbound,
        lease,
        attachment.events,
        outbound_tx,
    ));
    Ok(ReceiverStream::new(outbound_rx))
}

async fn run_attached_control_stream(
    mut inbound: tonic::Streaming<novarocks::QueryControlRequest>,
    mut lease: CoordinatorLease,
    mut events: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    outbound: tokio::sync::mpsc::Sender<Result<novarocks::QueryControlResponse, tonic::Status>>,
) {
    let Some(first_event) = events.recv().await else {
        let _ = outbound
            .send(Err(tonic::Status::internal(
                "query control event stream closed before ControlReady",
            )))
            .await;
        return;
    };
    if first_event != QueryControlEvent::ControlReady {
        let _ = outbound
            .send(Err(tonic::Status::internal(
                "query control event stream did not begin with ControlReady",
            )))
            .await;
        return;
    }
    if outbound
        .send(Ok(encode_query_control_event(&first_event)))
        .await
        .is_err()
    {
        return;
    }

    let mut awaiting_graceful_termination = false;
    loop {
        if awaiting_graceful_termination {
            let Some(event) = events.recv().await else {
                break;
            };
            let termination_accepted =
                matches!(event, QueryControlEvent::TerminationAccepted { .. });
            if outbound
                .send(Ok(encode_query_control_event(&event)))
                .await
                .is_err()
            {
                break;
            }
            if termination_accepted {
                lease.mark_graceful();
                break;
            }
            continue;
        }
        tokio::select! {
            inbound_message = inbound.message() => {
                let request = match inbound_message {
                    Ok(Some(request)) => request,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = outbound
                            .send(Err(tonic::Status::invalid_argument(format!(
                                "read query control command: {error}"
                            ))))
                            .await;
                        break;
                    }
                };
                if matches!(
                    request.command,
                    Some(novarocks::query_control_request::Command::Attach(_))
                ) {
                    let _ = outbound
                        .send(Err(tonic::Status::already_exists(
                            "Attach may appear exactly once",
                        )))
                        .await;
                    break;
                }
                let command = match decode_query_control_command(&request) {
                    Ok(command) => command,
                    Err(error) => {
                        let _ = outbound.send(Err(status_from_lifecycle_error(error))).await;
                        break;
                    }
                };
                let result = match command {
                    QueryControlCommand::Heartbeat { sequence, .. } => {
                        lease.control().heartbeat(sequence)
                    }
                    QueryControlCommand::Abort { reason } => {
                        awaiting_graceful_termination = true;
                        lease.control().abort(reason)
                    }
                    QueryControlCommand::Finalize => {
                        awaiting_graceful_termination = true;
                        lease.control().finalize()
                    }
                };
                if let Err(error) = result {
                    let _ = outbound.send(Err(status_from_lifecycle_error(error))).await;
                    break;
                }
            }
            event = events.recv() => {
                let Some(event) = event else {
                    break;
                };
                let termination_accepted =
                    matches!(event, QueryControlEvent::TerminationAccepted { .. });
                if outbound
                    .send(Ok(encode_query_control_event(&event)))
                    .await
                    .is_err()
                {
                    break;
                }
                if termination_accepted && awaiting_graceful_termination {
                    lease.mark_graceful();
                    break;
                }
            }
        }
    }
}

struct CoordinatorLease {
    control: Arc<dyn BackendQueryControl>,
    graceful: bool,
}

impl CoordinatorLease {
    fn new(control: Arc<dyn BackendQueryControl>) -> Self {
        Self {
            control,
            graceful: false,
        }
    }

    fn control(&self) -> &dyn BackendQueryControl {
        self.control.as_ref()
    }

    fn mark_graceful(&mut self) {
        self.graceful = true;
    }
}

impl Drop for CoordinatorLease {
    fn drop(&mut self) {
        if !self.graceful {
            let _ = self
                .control
                .coordinator_lost(QueryTerminationReason::CoordinatorStreamLost);
        }
    }
}

pub(crate) fn status_from_lifecycle_error(error: QueryLifecycleError) -> tonic::Status {
    let detail = error.detail().to_string();
    match error.code() {
        QueryLifecycleErrorCode::InvalidManifest => tonic::Status::invalid_argument(detail),
        QueryLifecycleErrorCode::Conflict => tonic::Status::already_exists(detail),
        QueryLifecycleErrorCode::StaleBackend | QueryLifecycleErrorCode::Terminated => {
            tonic::Status::failed_precondition(detail)
        }
        QueryLifecycleErrorCode::Capacity => tonic::Status::resource_exhausted(detail),
        QueryLifecycleErrorCode::Transport => tonic::Status::unavailable(detail),
        QueryLifecycleErrorCode::Internal => tonic::Status::internal(detail),
    }
}

fn encode_init_outcome(outcome: QueryInitOutcome) -> i32 {
    match outcome {
        QueryInitOutcome::Applied => novarocks::QueryInitOutcome::QueryInitApplied as i32,
        QueryInitOutcome::AlreadyApplied => {
            novarocks::QueryInitOutcome::QueryInitAlreadyApplied as i32
        }
        QueryInitOutcome::RejectedConflict => {
            novarocks::QueryInitOutcome::QueryInitRejectedConflict as i32
        }
        QueryInitOutcome::RejectedStaleBackend => {
            novarocks::QueryInitOutcome::QueryInitRejectedStaleBackend as i32
        }
        QueryInitOutcome::RejectedCapacity => {
            novarocks::QueryInitOutcome::QueryInitRejectedCapacity as i32
        }
        QueryInitOutcome::RejectedInvalidManifest => {
            novarocks::QueryInitOutcome::QueryInitRejectedInvalidManifest as i32
        }
        QueryInitOutcome::RejectedTerminated => {
            novarocks::QueryInitOutcome::QueryInitRejectedTerminated as i32
        }
    }
}
