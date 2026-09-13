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

use novarocks_execution_contract::task_execution::context_convergence::QueryContextConvergenceCursor;
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
use novarocks_execution_contract::task_execution::status::{SafeDetail, TaskFailureCategory};
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::operation::{
    encode_context_convergence_event, encode_receipt, encode_status_event, encode_task_gone_event,
};
use tokio_stream::Stream;

use crate::task_protocol_fault;
use novarocks_worker::{
    ContextConvergenceCursorError, HostRejection, OperationReceipt, TaskStatusEvent,
    TaskStatusSource, TaskStatusSubscriptionPosition,
};

/// Server-side status event stream of one logical query-by-backend
/// subscription.
pub type TaskStatusEventStream =
    Pin<Box<dyn Stream<Item = Result<proto::TaskStatusStreamEvent, tonic::Status>> + Send>>;

/// The acknowledgement body carried by one task-operation receipt.
pub type TaskOperationReceiptAck = proto::task_operation_receipt::Ack;

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
