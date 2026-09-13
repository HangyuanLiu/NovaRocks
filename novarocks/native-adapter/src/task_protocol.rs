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

//! Native task-protocol RPC boundary.
//!
//! This is the adapter-owned contract between generated Native gRPC handlers
//! and a role-local task owner. The RPC service stays thin: it hands a wire
//! request to this port and encodes what comes back, so nothing above it
//! interprets a wire shape and nothing below it names one.
//!
//! Five entry points, matching the five RPCs: one batched mutation, one status
//! subscription, two typed observation reads, and the root result data plane.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use novarocks_execution_contract::task_execution::context_convergence::QueryContextConvergenceCursor;
use novarocks_execution_contract::task_execution::identity::{
    QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::operation::{
    FetchTaskDynamicFilters, GetFinalTaskInfo, OperationOutcome, ResultByteLimit,
};
use novarocks_execution_contract::task_execution::status::{SafeDetail, TaskFailureCategory};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::operation::{
    decode_context_aware_subscribe_task_status, decode_fetch_dynamic_filters,
    decode_get_final_task_info, encode_context_convergence_event, encode_operation_outcome,
    encode_receipt, encode_status_event, encode_task_gone_event,
};
use novarocks_task_codec::status::encode_final_task_info;
use novarocks_task_codec::{
    domain::encode_task_dynamic_filter_domain, identity::encode_task_identity,
};
use tokio_stream::Stream;

use crate::task_protocol_fault;
use novarocks_worker::{
    ContextConvergenceCursorError, DynamicFilterReadOutcome, FinalTaskInfoOutcome, HostRejection,
    OperationReceipt, TaskDynamicFilterRead, TaskStatusEvent, TaskStatusSource,
    TaskStatusSubscriptionPosition,
};

/// Server-side status event stream of one logical query-by-backend
/// subscription.
pub type TaskStatusEventStream =
    Pin<Box<dyn Stream<Item = Result<proto::TaskStatusStreamEvent, tonic::Status>> + Send>>;

/// The acknowledgement body carried by one task-operation receipt.
pub type TaskOperationReceiptAck = proto::task_operation_receipt::Ack;

/// A validated root-result read passed from the Native wire adapter to the
/// role-local result owner.
#[derive(Clone, Copy, Debug)]
pub struct TaskResultReadRequest {
    identity: TaskIdentity,
    max_wait: Duration,
    acknowledged_packet_sequence: Option<i64>,
    max_result_bytes: ResultByteLimit,
}

impl TaskResultReadRequest {
    fn new(
        identity: TaskIdentity,
        max_wait: Duration,
        acknowledged_packet_sequence: Option<i64>,
        max_result_bytes: ResultByteLimit,
    ) -> Self {
        Self {
            identity,
            max_wait,
            acknowledged_packet_sequence,
            max_result_bytes,
        }
    }

    /// The exact task identity whose root-result responsibility is queried.
    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }

    /// The bounded wait accepted by the Native request.
    pub const fn max_wait(self) -> Duration {
        self.max_wait
    }

    /// The packet acknowledged by this read, if any.
    pub const fn acknowledged_packet_sequence(self) -> Option<i64> {
        self.acknowledged_packet_sequence
    }

    /// The already validated payload ceiling for this response.
    pub const fn max_result_bytes(self) -> ResultByteLimit {
        self.max_result_bytes
    }
}

/// One role-local result-buffer observation, before Native response encoding.
#[derive(Debug)]
pub enum TaskResultRead {
    /// One retained payload packet, including the retained EOS packet.
    Ready {
        packet_sequence: i64,
        end_of_stream: bool,
        payload: Bytes,
    },
    /// The exact acknowledged EOS was consumed or replayed.
    EndOfStream { packet_sequence: i64 },
    /// No result is ready before the accepted read bound.
    NotReady,
    /// A settled, in-band failure or refusal from the role-local owner.
    Error { detail: String },
}

/// A role-local invariant failure while answering a validated result read.
#[derive(Debug)]
pub struct TaskResultReadError {
    detail: String,
}

impl TaskResultReadError {
    /// Builds an error whose detail is safe for the Native transport boundary.
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    /// The safe detail exposed as an internal Native status.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Role-local authority over root-result routing, buffering, and drain facts.
#[tonic::async_trait]
pub trait TaskResultReader: Send + Sync {
    /// Reads one exact root-result packet after Native wire validation.
    async fn read_task_result(
        &self,
        request: TaskResultReadRequest,
    ) -> Result<TaskResultRead, TaskResultReadError>;
}

/// Role-local authority over task observation facts after Native request
/// validation. These reads never mutate registry state or renew a lease.
pub trait TaskObservationReader: Send + Sync {
    /// Reads a task's dynamic-filter advertisement from the role-local owner.
    fn read_task_dynamic_filters(
        &self,
        request: &FetchTaskDynamicFilters,
    ) -> DynamicFilterReadOutcome;

    /// Reads a task's terminal diagnostic information from the role-local owner.
    fn read_final_task_info(&self, request: &GetFinalTaskInfo) -> FinalTaskInfoOutcome;
}

/// Role-local authority over the observation channel for one held query
/// context. The channel and every retained status fact remain role-owned.
pub trait TaskStatusSubscriptionReader: Send + Sync {
    /// Returns the source only when this role currently holds the exact context.
    fn task_status_source(&self, context: QueryContextRef) -> Option<Arc<TaskStatusSource>>;
}

/// Decodes, captures, and starts one Native task-status subscription.
pub fn subscribe_task_status(
    reader: &dyn TaskStatusSubscriptionReader,
    request: proto::SubscribeTaskStatusRequest,
) -> Result<TaskStatusEventStream, tonic::Status> {
    let (context, cursors, context_convergence_cursor) =
        decode_context_aware_subscribe_task_status(
            &request,
            FieldPath::root("subscribe_task_status"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let source = reader.task_status_source(context).ok_or_else(|| {
        tonic::Status::failed_precondition(
            "subscribe names a query context this backend does not hold",
        )
    })?;
    // The catch-up frames are captured before the stream exists, so a cursor
    // that is behind cannot miss a version published between the two.
    let catch_up = source
        .subscribe_context_aware(context, &cursors, context_convergence_cursor)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    task_status_event_stream(
        source,
        catch_up,
        context,
        cursors,
        context_convergence_cursor,
    )
}

/// Decodes, delegates, classifies, and encodes one dynamic-filter observation.
pub fn fetch_task_dynamic_filters(
    reader: &dyn TaskObservationReader,
    request: proto::FetchTaskDynamicFiltersRequest,
) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status> {
    // An observation read carries no envelope on the wire, so this Native
    // boundary mints its non-replayable operation identity after validation.
    let read = decode_fetch_dynamic_filters(
        &request,
        TaskOperationId::new_v7(),
        FieldPath::root("fetch_task_dynamic_filters"),
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let identity = read.identity();
    let receipt = reader.read_task_dynamic_filters(&read);
    // `Accepted` carries a version newer than the caller acknowledged and
    // `Idempotent` says it is already current. This response has no outcome
    // field, so every other receipt is a refusal, not an empty answer.
    if !matches!(
        receipt.outcome(),
        OperationOutcome::Accepted | OperationOutcome::Idempotent
    ) {
        return Err(tonic::Status::failed_precondition(
            receipt.detail().map_or("", SafeDetail::as_str).to_owned(),
        ));
    }
    encode_dynamic_filter_read(identity, receipt.acknowledgement()).map_err(host_rejection_status)
}

/// Decodes, delegates, and encodes one final-task observation without
/// reclassifying the role owner's receipt.
pub fn get_final_task_info(
    reader: &dyn TaskObservationReader,
    request: proto::GetFinalTaskInfoRequest,
) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status> {
    let read = decode_get_final_task_info(
        &request,
        TaskOperationId::new_v7(),
        FieldPath::root("get_final_task_info"),
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    Ok(encode_final_task_info_response(
        &reader.read_final_task_info(&read),
    ))
}

/// Decodes, delegates, and encodes one Native root-result read.
pub async fn fetch_task_result(
    reader: &dyn TaskResultReader,
    request: proto::FetchTaskResultRequest,
) -> Result<proto::FetchResultResponse, tonic::Status> {
    use proto::fetch_result_response::Status as FetchStatus;

    let (identity, max_wait, acknowledged, max_result_bytes) =
        novarocks_task_codec::operation::decode_fetch_task_result(
            &request,
            novarocks_proto_codec::FieldPath::root("fetch_task_result"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let acknowledged_packet_sequence = acknowledged
        .map(|sequence| {
            i64::try_from(sequence.get()).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "acknowledged result packet sequence {} exceeds the wire response range",
                    sequence.get()
                ))
            })
        })
        .transpose()?;
    let read = reader
        .read_task_result(TaskResultReadRequest::new(
            identity,
            max_wait,
            acknowledged_packet_sequence,
            max_result_bytes,
        ))
        .await
        .map_err(|error| tonic::Status::internal(error.detail().to_owned()))?;
    Ok(match read {
        TaskResultRead::Ready {
            packet_sequence,
            end_of_stream,
            payload,
        } => task_result_response(
            FetchStatus::Ready,
            String::new(),
            packet_sequence,
            end_of_stream,
            payload,
        ),
        TaskResultRead::EndOfStream { packet_sequence } => task_result_response(
            FetchStatus::Eof,
            String::new(),
            packet_sequence,
            true,
            Bytes::new(),
        ),
        TaskResultRead::NotReady => {
            task_result_response(FetchStatus::NotReady, String::new(), 0, false, Bytes::new())
        }
        TaskResultRead::Error { detail } => {
            task_result_response(FetchStatus::Error, detail, 0, false, Bytes::new())
        }
    })
}

fn task_result_response(
    status: proto::fetch_result_response::Status,
    message: String,
    packet_sequence: i64,
    end_of_stream: bool,
    result_arrow_ipc: Bytes,
) -> proto::FetchResultResponse {
    proto::FetchResultResponse {
        status: status as i32,
        message,
        packet_seq: packet_sequence,
        eos: end_of_stream,
        result_arrow_ipc,
    }
}

/// Maps a role-owner rejection onto the status code of a wire read that has no
/// in-band outcome field.
pub fn host_rejection_status(rejection: HostRejection) -> tonic::Status {
    let detail = rejection.detail().as_str().to_owned();
    match rejection.category() {
        TaskFailureCategory::Protocol => tonic::Status::invalid_argument(detail),
        TaskFailureCategory::ResourceExhausted => tonic::Status::resource_exhausted(detail),
        TaskFailureCategory::Execution
        | TaskFailureCategory::Exchange
        | TaskFailureCategory::Internal => tonic::Status::internal(detail),
    }
}

/// Projects one task's dynamic-filter advertisement onto its Native read
/// response without treating an unprojectable payload as an empty version.
pub fn encode_dynamic_filter_read(
    identity: TaskIdentity,
    read: Option<&TaskDynamicFilterRead>,
) -> Result<proto::FetchTaskDynamicFiltersResponse, HostRejection> {
    let (version, domains) = match read {
        Some(read) => {
            let domain = encode_task_dynamic_filter_domain(read.version(), read.payload().as_ref())
                .map_err(|error| {
                    HostRejection::new(
                        TaskFailureCategory::Internal,
                        format!(
                            "dynamic filter version {} cannot be projected: {error}",
                            read.version().get()
                        ),
                    )
                })?;
            (read.version().get(), vec![domain])
        }
        None => (0, Vec::new()),
    };
    Ok(proto::FetchTaskDynamicFiltersResponse {
        identity: Some(encode_task_identity(identity)),
        version,
        domains,
    })
}

/// Encodes a final-task observation without reclassifying the Worker receipt.
pub fn encode_final_task_info_response(
    receipt: &FinalTaskInfoOutcome,
) -> proto::GetFinalTaskInfoResponse {
    let result = match receipt.acknowledgement() {
        Some(info) => {
            proto::get_final_task_info_response::Result::Info(encode_final_task_info(info))
        }
        // Losing final info costs diagnostics only, so why it is missing is
        // reported as an outcome rather than as an error.
        None => proto::get_final_task_info_response::Result::Unavailable(encode_operation_outcome(
            receipt.outcome(),
        )),
    };
    proto::GetFinalTaskInfoResponse {
        result: Some(result),
    }
}

/// Encodes one role-owner receipt without inventing an acknowledgement whose
/// state the Native wire cannot represent.
pub fn encode_operation_receipt<T>(
    receipt: &OperationReceipt<T>,
    encode_ack: impl FnOnce(&T) -> Option<TaskOperationReceiptAck>,
) -> Result<proto::TaskOperationReceipt, tonic::Status> {
    let ack = match receipt.acknowledgement() {
        Some(body) => Some(encode_ack(body).ok_or_else(|| {
            tonic::Status::internal(
                "operation acknowledgement reports a state with no wire representation",
            )
        })?),
        None => None,
    };
    Ok(encode_receipt(
        receipt.operation_id(),
        receipt.outcome(),
        receipt.detail().map_or("", SafeDetail::as_str),
        ack,
    ))
}

/// Builds the server stream for one status subscription after its role owner
/// has resolved the exact context and captured the initial frames.
pub fn task_status_event_stream(
    source: Arc<TaskStatusSource>,
    catch_up: Vec<TaskStatusEvent>,
    context: QueryContextRef,
    task_cursors: Vec<novarocks_execution_contract::task_execution::status::TaskStatusCursor>,
    context_convergence_cursor: Option<QueryContextConvergenceCursor>,
) -> Result<TaskStatusEventStream, tonic::Status> {
    if task_protocol_fault::task_status_subscription_dropped(context)? {
        // The subscription was established and is then torn down from the
        // stream body, which is what a lost stream looks like. Cursors are
        // read-only, so the resubscription loses no frame.
        return Ok(Box::pin(tokio_stream::once(Err(
            tonic::Status::unavailable(
                "runner-owned task status stream dropped after the subscription was established",
            ),
        ))));
    }
    Ok(Box::pin(TaskStatusSubscription::new(
        source,
        catch_up,
        context,
        task_cursors,
        context_convergence_cursor,
    )))
}

/// The server side of one logical status subscription.
///
/// It holds the context's observation channel and nothing else, so dropping it
/// — a client that went away, a coordinator that will resubscribe by cursor —
/// cancels no task, aborts no context, and changes no owner state. Waiting is
/// parked on the channel's own notify rather than sampled on a timer, because
/// terminal delivery is on the critical path of every query's completion.
struct TaskStatusSubscription {
    source: Arc<TaskStatusSource>,
    catch_up: VecDeque<TaskStatusEvent>,
    context: QueryContextRef,
    position: Arc<Mutex<TaskStatusSubscriptionPosition>>,
    /// The parked wait for the next frame. It owns its own handle to the
    /// source, so polling never borrows across the await.
    pending: Option<
        Pin<
            Box<
                dyn Future<Output = Result<Option<TaskStatusEvent>, ContextConvergenceCursorError>>
                    + Send,
            >,
        >,
    >,
}

impl TaskStatusSubscription {
    fn new(
        source: Arc<TaskStatusSource>,
        catch_up: Vec<TaskStatusEvent>,
        context: QueryContextRef,
        task_cursors: Vec<novarocks_execution_contract::task_execution::status::TaskStatusCursor>,
        context_convergence_cursor: Option<QueryContextConvergenceCursor>,
    ) -> Self {
        Self {
            source,
            catch_up: catch_up.into(),
            context,
            position: Arc::new(Mutex::new(TaskStatusSubscriptionPosition::new(
                &task_cursors,
                context_convergence_cursor,
            ))),
            pending: None,
        }
    }

    fn note_delivered(&mut self, event: &TaskStatusEvent) {
        self.position
            .lock()
            .expect("task status subscription position")
            .note_delivered(event);
    }
}

impl Stream for TaskStatusSubscription {
    type Item = Result<proto::TaskStatusStreamEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(event) = this.catch_up.pop_front() {
            this.note_delivered(&event);
            return Poll::Ready(Some(Ok(encode_task_status_event(&event))));
        }
        if this.pending.is_none() {
            let source = Arc::clone(&this.source);
            let context = this.context;
            let position = Arc::clone(&this.position);
            this.pending = Some(Box::pin(async move {
                source
                    .next_subscription_event_owned(context, &position)
                    .await
            }));
        }
        let pending = this.pending.as_mut().expect("a wait was just installed");
        match pending.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(event)) => {
                this.pending = None;
                Poll::Ready(event.map(|event| {
                    this.note_delivered(&event);
                    Ok(encode_task_status_event(&event))
                }))
            }
            Poll::Ready(Err(error)) => {
                this.pending = None;
                Poll::Ready(Some(Err(tonic::Status::invalid_argument(
                    error.to_string(),
                ))))
            }
        }
    }
}

fn encode_task_status_event(event: &TaskStatusEvent) -> proto::TaskStatusStreamEvent {
    if let TaskStatusEvent::ContextConvergence(receipt) = event {
        return encode_context_convergence_event(*receipt);
    }
    let (identity, mut encoded) = match event {
        TaskStatusEvent::Status(status) => (status.identity(), encode_status_event(status)),
        TaskStatusEvent::Gone(identity) => (*identity, encode_task_gone_event(*identity)),
        TaskStatusEvent::ContextConvergence(_) => unreachable!("handled above"),
    };
    // Claimed on the frame that is about to leave this process, which is the
    // only place the observation names a backend process the frontend will
    // check. Both the catch-up frames and the live ones are encoded here, so
    // no delivery path escapes it.
    task_protocol_fault::task_status_foreign_process(identity, &mut encoded);
    encoded
}

/// The Native task-protocol ingress port.
///
/// Every method takes the wire request and returns the wire response, because
/// this is the wire boundary. A `tonic::Status` here means the request could
/// not be understood at all; a request that was understood and refused comes
/// back as a typed receipt or outcome inside a successful response, which is
/// what lets a frontend classify it without reading an error message.
#[tonic::async_trait]
pub trait TaskExecutionIngress: Send + Sync {
    /// Applies a per-backend batch, one receipt per item in request order.
    ///
    /// A batch gives its items no atomicity and no shared verdict: a partial
    /// failure leaves every other item exactly as its own receipt reports.
    fn apply_task_operations(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    /// Opens one logical subscription, resuming from the given per-task
    /// cursors.
    ///
    /// Observation only: it creates nothing, freezes no task set, and owns no
    /// admission, edge-open, terminal, or cancel authority.
    fn subscribe_task_status(
        &self,
        request: proto::SubscribeTaskStatusRequest,
    ) -> Result<TaskStatusEventStream, tonic::Status>;

    fn fetch_task_dynamic_filters(
        &self,
        request: proto::FetchTaskDynamicFiltersRequest,
    ) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status>;

    fn get_final_task_info(
        &self,
        request: proto::GetFinalTaskInfoRequest,
    ) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status>;

    /// Polls the root task's result stream.
    ///
    /// Unlike the fragment-instance-addressed form it replaces, the request
    /// names an exact task, so it is fenced against a replaced backend
    /// process before it reaches a result buffer.
    async fn fetch_task_result(
        &self,
        request: proto::FetchTaskResultRequest,
    ) -> Result<proto::FetchResultResponse, tonic::Status>;
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{
        Bytes, TaskResultRead, TaskResultReadError, TaskResultReadRequest, TaskResultReader,
        fetch_task_result, proto, task_result_response,
    };
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_task_codec::operation::{
        MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES, NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use proto::fetch_result_response::Status as FetchStatus;

    #[test]
    fn maximum_legal_root_result_fits_the_actual_grpc_response_envelope() {
        let payload_bytes = usize::try_from(MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES)
            .expect("the Native result ceiling fits usize");
        let response = task_result_response(
            FetchStatus::Ready,
            String::new(),
            i64::MAX,
            true,
            Bytes::from(vec![0_u8; payload_bytes]),
        );
        let encoded_bytes = response.encoded_len();
        assert!(
            encoded_bytes <= NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
            "the maximum legal payload produces a {encoded_bytes}-byte response above the {}-byte decode ceiling",
            NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
        );
    }

    #[test]
    fn root_result_reader_output_is_encoded_without_a_backend_type() {
        struct ReadyReader;

        #[tonic::async_trait]
        impl TaskResultReader for ReadyReader {
            async fn read_task_result(
                &self,
                request: TaskResultReadRequest,
            ) -> Result<TaskResultRead, TaskResultReadError> {
                assert_eq!(request.max_wait(), std::time::Duration::from_millis(17));
                assert_eq!(request.acknowledged_packet_sequence(), Some(3));
                assert_eq!(request.max_result_bytes().get(), 4096);
                Ok(TaskResultRead::Ready {
                    packet_sequence: 4,
                    end_of_stream: false,
                    payload: Bytes::from_static(b"result"),
                })
            }
        }

        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(2).expect("attempt id"))
                .expect("execution id"),
            StageId::new(3).expect("stage id"),
            TaskId::new(4).expect("task id"),
            BackendProcessId::new_v7(),
        );
        let response = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("task result adapter runtime")
            .block_on(fetch_task_result(
                &ReadyReader,
                proto::FetchTaskResultRequest {
                    root_task: Some(novarocks_task_codec::identity::encode_task_identity(
                        identity,
                    )),
                    max_wait_millis: 17,
                    acknowledged_packet_sequence: Some(3),
                    max_result_bytes: 4096,
                },
            ))
            .expect("a valid role result is encoded");
        assert_eq!(response.status, FetchStatus::Ready as i32);
        assert_eq!(response.packet_seq, 4);
        assert!(!response.eos);
        assert_eq!(response.result_arrow_ipc, Bytes::from_static(b"result"));
    }
}
