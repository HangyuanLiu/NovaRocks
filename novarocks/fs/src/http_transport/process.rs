// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original finite HTTP process rows. The root owns the one service join.

use std::any::Any;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use http::{Request, Response, Uri};
use opendal::{Buffer, HttpBody};
use tokio::runtime::Handle;
use tokio::task::JoinError;

use super::connection::{
    H2Upload, HttpConnection, HttpConnectionError, HttpHandshakeFuture, HttpResponseFuture,
    HttpResponseOutcome, RawHttpError, UploadFailure, UploadStep,
};
use super::connector::ConnectedHttpIo;
use super::process_cell::HttpProcessCell;
use super::response::{PayloadHandoff, ResponseAction, ResponseSource, StopReason};
use super::route::PreparedHttpRoute;
use crate::{FileError, FileErrorKind, FileIoRuntime, FileResult, FileTask};

type PanicPayload = Box<dyn Any + Send + 'static>;

/// Supplied by the original process profile. These are storage dimensions,
/// not Work/Joint/physical grants or a complete Native profile constructor.
#[derive(Clone, Copy)]
pub(crate) struct HttpProcessLimits {
    pub(crate) connections: NonZeroUsize,
    pub(crate) requests: NonZeroUsize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HttpSubmitError {
    Closed,
    Full,
    StaleConnection,
    SourceEmpty,
    GenerationExhausted,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HttpRequestFailure {
    SourceFailed,
    Abandoned,
}

/// Minted only by the original fixed C row. Reuse requires actual old C/R
/// objects to exit; its nonzero generation is never wrapped or reconstructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HttpConnectionToken {
    index: usize,
    generation: NonZeroU64,
}
impl HttpConnectionToken {
    fn new(index: usize, generation: NonZeroU64) -> Self {
        Self { index, generation }
    }
    pub(crate) fn index(self) -> usize {
        self.index
    }
    pub(crate) fn generation(self) -> NonZeroU64 {
        self.generation
    }
}

pub(crate) struct HttpConnectionInput {
    pub(crate) io: ConnectedHttpIo,
    pub(crate) route: PreparedHttpRoute,
}
/// The original caller must retain this source in its stable receiving slot
/// until publication succeeds. It already owns the original request custody.
pub(crate) struct HttpRequestInput<G, H> {
    pub(crate) connection: HttpConnectionToken,
    pub(crate) request: Option<Request<Buffer>>,
    pub(crate) route: PreparedHttpRoute,
    pub(crate) handoff: H,
    // All original request/handoff fields drop before this custody alias.
    pub(crate) source_guard: Arc<G>,
}
pub(crate) struct HttpResponseEnvelope {
    pub(crate) response: Response<HttpBody>,
    pub(crate) original_uri: Uri,
    pub(crate) is_head: bool,
}

struct Signal {
    service: AtomicWaker,
    observer: AtomicWaker,
}
impl Signal {
    fn wake(&self) {
        self.service.wake();
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Free,
    Queued,
    Active,
}
struct ConnectionIngress {
    generation: u64,
    phase: Phase,
    accepting: bool,
    hard_close: bool,
    token: Option<HttpConnectionToken>,
    input: Option<HttpConnectionInput>,
}
struct RequestIngress<G, H> {
    generation: u64,
    phase: Phase,
    input: Option<HttpRequestInput<G, H>>,
    reply: Option<Arc<Reply>>,
}
struct IngressRows<G, H> {
    closed: bool,
    connections: Box<[ConnectionIngress]>,
    requests: Box<[RequestIngress<G, H>]>,
}
struct Ingress<G, H> {
    rows: Mutex<IngressRows<G, H>>,
    signal: Arc<Signal>,
}

/// Clone view of the same fixed input inventory; it never takes a process-cell
/// lease, polls a foreign Future, or acquires a different identity/quota table.
pub(crate) struct HttpSubmission<G, H> {
    ingress: Weak<Ingress<G, H>>,
}
impl<G, H> Clone for HttpSubmission<G, H> {
    fn clone(&self) -> Self {
        Self {
            ingress: self.ingress.clone(),
        }
    }
}

/// Move outside the caller's external lock before finish/Drop. The same strong
/// ingress tail survives success and refusal. Clear pending before first wake.
#[must_use]
pub(crate) struct HttpPublish<T, G, H> {
    result: Option<Result<T, HttpSubmitError>>,
    notification_pending: bool,
    tail: Option<Arc<Ingress<G, H>>>,
}
impl<T, G, H> HttpPublish<T, G, H> {
    fn notify(&mut self) {
        if self.notification_pending {
            self.notification_pending = false;
            if let Some(tail) = &self.tail {
                tail.signal.wake();
            }
        }
    }
    pub(crate) fn finish(mut self) -> Result<T, HttpSubmitError> {
        self.notify();
        self.result
            .take()
            .expect("HTTP publication returns its original result once")
    }
}
impl<T, G, H> Drop for HttpPublish<T, G, H> {
    fn drop(&mut self) {
        self.notify();
    }
}

impl<G, H> HttpSubmission<G, H> {
    pub(crate) fn connect_in(
        &self,
        source: &mut Option<HttpConnectionInput>,
    ) -> HttpPublish<HttpConnectionToken, G, H> {
        let Some(tail) = self.ingress.upgrade() else {
            return HttpPublish {
                result: Some(Err(HttpSubmitError::Closed)),
                notification_pending: false,
                tail: None,
            };
        };
        let result = {
            let mut rows = tail
                .rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if source.is_none() {
                Err(HttpSubmitError::SourceEmpty)
            } else if rows.closed {
                Err(HttpSubmitError::Closed)
            } else if let Some(index) = rows.connections.iter().position(|r| r.phase == Phase::Free)
            {
                let row = &mut rows.connections[index];
                if let Some(generation) = row.generation.checked_add(1).and_then(NonZeroU64::new) {
                    let token = HttpConnectionToken::new(index, generation);
                    row.generation = generation.get();
                    row.accepting = true;
                    row.hard_close = false;
                    row.token = Some(token);
                    row.input = source.take();
                    row.phase = Phase::Queued;
                    Ok(token)
                } else {
                    Err(HttpSubmitError::GenerationExhausted)
                }
            } else {
                Err(HttpSubmitError::Full)
            }
        };
        let notify = result.is_ok();
        HttpPublish {
            result: Some(result),
            notification_pending: notify,
            tail: Some(tail),
        }
    }

    pub(crate) fn request_in(
        &self,
        source: &mut Option<HttpRequestInput<G, H>>,
    ) -> HttpPublish<HttpReplyTicket, G, H> {
        let Some(tail) = self.ingress.upgrade() else {
            return HttpPublish {
                result: Some(Err(HttpSubmitError::Closed)),
                notification_pending: false,
                tail: None,
            };
        };
        let result = {
            let mut rows = tail
                .rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match source.as_ref() {
                None => Err(HttpSubmitError::SourceEmpty),
                Some(_) if rows.closed => Err(HttpSubmitError::Closed),
                Some(input) => {
                    let valid = rows
                        .connections
                        .get(input.connection.index())
                        .is_some_and(|r| {
                            r.phase != Phase::Free
                                && r.generation == input.connection.generation().get()
                                && r.accepting
                        });
                    if !valid {
                        Err(HttpSubmitError::StaleConnection)
                    } else if let Some(index) =
                        rows.requests.iter().position(|r| r.phase == Phase::Free)
                    {
                        let row = &mut rows.requests[index];
                        if let Some(generation) = row.generation.checked_add(1) {
                            let reply = Arc::new(Reply {
                                slot: Mutex::new(ReplySlot {
                                    value: None,
                                    abandoned: false,
                                }),
                                reader: AtomicWaker::new(),
                                signal: Arc::clone(&tail.signal),
                            });
                            let ticket = HttpReplyTicket {
                                reply: Arc::clone(&reply),
                                delivered: false,
                            };
                            row.generation = generation;
                            row.reply = Some(reply);
                            row.input = source.take();
                            row.phase = Phase::Queued;
                            Ok(ticket)
                        } else {
                            Err(HttpSubmitError::GenerationExhausted)
                        }
                    } else {
                        Err(HttpSubmitError::Full)
                    }
                }
            }
        };
        let notify = result.is_ok();
        HttpPublish {
            result: Some(result),
            notification_pending: notify,
            tail: Some(tail),
        }
    }
}

struct ReplySlot {
    value: Option<Result<HttpResponseEnvelope, HttpRequestFailure>>,
    abandoned: bool,
}
struct Reply {
    slot: Mutex<ReplySlot>,
    reader: AtomicWaker,
    signal: Arc<Signal>,
}
#[must_use]
pub(crate) struct HttpReplyTicket {
    reply: Arc<Reply>,
    delivered: bool,
}
impl Future for HttpReplyTicket {
    type Output = Result<HttpResponseEnvelope, HttpRequestFailure>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.delivered, "HTTP reply ticket polled after delivery");
        this.reply.reader.register(cx.waker());
        let value = this
            .reply
            .slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .value
            .take();
        if let Some(value) = value {
            this.delivered = true;
            let outcome = Poll::Ready(value);
            this.reply.signal.wake();
            outcome
        } else {
            Poll::Pending
        }
    }
}
impl Drop for HttpReplyTicket {
    fn drop(&mut self) {
        if !self.delivered {
            self.reply
                .slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .abandoned = true;
        }
        self.reply.signal.wake();
    }
}

pub(crate) enum HttpProcessFailure {
    Connection(HttpConnectionError),
    Response(RawHttpError),
    Upload(UploadFailure),
    Panic(PanicPayload),
}
pub(crate) enum HttpRequestFailureRef<'a> {
    Source(&'a super::response::ResponseFailure),
    Operation(&'a HttpProcessFailure),
    Notification(&'a (dyn Any + Send)),
    Cleanup(&'a (dyn Any + Send)),
}
struct ConnectionRow {
    h2: bool,
    token: Option<HttpConnectionToken>,
    input: Option<HttpConnectionInput>,
    handshake: Option<HttpHandshakeFuture>,
    connection: Option<HttpConnection>,
    closing: bool,
    hard_closing: bool,
    stopped: bool,
    failure: Option<HttpProcessFailure>,
    cleanup_panic: Option<PanicPayload>,
}
struct RequestRow<G, H: PayloadHandoff> {
    h2: bool,
    upload: Option<H2Upload>,
    token: Option<HttpConnectionToken>,
    input: Option<HttpRequestInput<G, H>>,
    reply: Option<Arc<Reply>>,
    future: Option<HttpResponseFuture>,
    outcome: Option<HttpResponseOutcome>,
    source: Option<ResponseSource<Arc<G>, H>>,
    retained_guard: Option<Arc<G>>,
    raw_complete: bool,
    reset: bool,
    reply_published: bool,
    failure: Option<HttpProcessFailure>,
    cleanup_panic: Option<PanicPayload>,
    notification_panic: Option<PanicPayload>,
}
impl<G, H: PayloadHandoff> RequestRow<G, H> {
    fn empty() -> Self {
        Self {
            h2: false,
            upload: None,
            token: None,
            input: None,
            reply: None,
            future: None,
            outcome: None,
            source: None,
            retained_guard: None,
            raw_complete: false,
            reset: false,
            reply_published: false,
            failure: None,
            cleanup_panic: None,
            notification_panic: None,
        }
    }
}
impl ConnectionRow {
    fn empty() -> Self {
        Self {
            h2: false,
            token: None,
            input: None,
            handshake: None,
            connection: None,
            closing: false,
            hard_closing: false,
            stopped: false,
            failure: None,
            cleanup_panic: None,
        }
    }
}

pub(crate) struct HttpProcessState<G, H: PayloadHandoff> {
    ingress: Arc<Ingress<G, H>>,
    connections: Box<[ConnectionRow]>,
    requests: Box<[RequestRow<G, H>]>,
}

fn storage<T>(count: usize, mut make: impl FnMut() -> T) -> FileResult<Box<[T]>> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(count).map_err(|error| {
        FileError::with_source(
            FileErrorKind::ResourceExhausted,
            "reserve original HTTP process rows",
            error,
        )
    })?;
    rows.resize_with(count, &mut make);
    Ok(rows.into_boxed_slice())
}

impl<G: Send + Sync + 'static, H: PayloadHandoff> HttpProcessState<G, H> {
    pub(crate) fn connection_failure(
        &self,
        token: HttpConnectionToken,
    ) -> Option<&HttpProcessFailure> {
        let row = self.connections.get(token.index())?;
        (row.token? == token).then_some(())?;
        row.failure.as_ref()
    }
    /// A raw source installs its own body/handoff/poll cause before waking the
    /// reader. If that wake also panics, the original source cause remains first.
    pub(crate) fn request_primary_failures(
        &self,
    ) -> impl Iterator<Item = (HttpConnectionToken, HttpRequestFailureRef<'_>)> {
        self.requests.iter().filter_map(|r| {
            let token = r.token?;
            let cause = if let Some(failure) = r.source.as_ref().and_then(|s| s.failure()) {
                HttpRequestFailureRef::Source(failure)
            } else if let Some(failure) = r.failure.as_ref() {
                HttpRequestFailureRef::Operation(failure)
            } else if let Some(payload) = r.notification_panic.as_deref() {
                HttpRequestFailureRef::Notification(payload)
            } else {
                HttpRequestFailureRef::Cleanup(r.cleanup_panic.as_deref()?)
            };
            Some((token, cause))
        })
    }
    pub(crate) fn request_failures(
        &self,
    ) -> impl Iterator<Item = (HttpConnectionToken, &HttpProcessFailure)> {
        self.requests
            .iter()
            .filter_map(|r| Some((r.token?, r.failure.as_ref()?)))
    }
    pub(crate) fn response_failures(
        &self,
    ) -> impl Iterator<Item = (HttpConnectionToken, &super::response::ResponseFailure)> {
        self.requests
            .iter()
            .filter_map(|r| Some((r.token?, r.source.as_ref()?.failure()?)))
    }
    pub(crate) fn retained_counts(&self) -> (usize, usize) {
        (
            self.connections
                .iter()
                .filter(|r| r.token.is_some())
                .count(),
            self.requests.iter().filter(|r| r.token.is_some()).count(),
        )
    }
    pub(crate) fn cleanup_panics(
        &self,
    ) -> impl Iterator<Item = (HttpConnectionToken, &(dyn Any + Send))> {
        self.connections
            .iter()
            .filter_map(|r| Some((r.token?, r.cleanup_panic.as_deref()?)))
            .chain(
                self.requests
                    .iter()
                    .filter_map(|r| Some((r.token?, r.cleanup_panic.as_deref()?))),
            )
            .chain(
                self.requests
                    .iter()
                    .filter_map(|r| Some((r.token?, r.notification_panic.as_deref()?))),
            )
    }

    fn receive(&mut self) -> bool {
        let mut progress = false;
        let mut ingress = self
            .ingress
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let closed = ingress.closed;
        for (index, slot) in ingress.connections.iter_mut().enumerate() {
            if slot.phase == Phase::Queued {
                let row = &mut self.connections[index];
                assert!(
                    row.token.is_none() && row.input.is_none(),
                    "queued connection must own the original free runtime row"
                );
                row.token = slot.token;
                row.h2 = slot.input.as_ref().is_some_and(|input| {
                    input.io.negotiated_protocol == super::connector::NegotiatedHttpProtocol::H2
                });
                row.input = slot.input.take();
                slot.phase = Phase::Active;
                progress = true;
            }
            if slot.phase != Phase::Free {
                self.connections[index].closing |= !slot.accepting || closed;
                self.connections[index].hard_closing |= slot.hard_close || closed;
            }
        }
        for (index, slot) in ingress.requests.iter_mut().enumerate() {
            if slot.phase == Phase::Queued {
                let row = &mut self.requests[index];
                assert!(
                    row.token.is_none() && row.input.is_none() && row.reply.is_none(),
                    "queued request must own the original free runtime row"
                );
                row.input = slot.input.take();
                row.reply = slot.reply.take();
                row.token = row.input.as_ref().map(|v| v.connection);
                row.h2 = self.connections[row.token.unwrap().index()].h2;
                slot.phase = Phase::Active;
                progress = true;
            }
        }
        progress
    }

    fn stop_admission(&mut self, token: HttpConnectionToken, hard: bool) {
        let mut ingress = self
            .ingress
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = ingress.connections.get_mut(token.index()) {
            if slot.generation == token.generation().get() {
                slot.accepting = false;
                slot.hard_close |= hard;
            }
        }
        self.connections[token.index()].closing = true;
        self.connections[token.index()].hard_closing |= hard;
    }

    fn poll_connection_row(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        let row = &mut self.connections[index];
        if row.token.is_none() {
            return false;
        }
        if row.hard_closing || row.failure.is_some() || row.cleanup_panic.is_some() {
            return false;
        }
        if row.handshake.is_none() && row.connection.is_none() && row.input.is_some() {
            let input = row
                .input
                .take()
                .expect("original connection input installed");
            row.handshake = Some(HttpConnection::handshake(input.io, &input.route));
        }
        if let Some(future) = row.handshake.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Pending => return false,
                Poll::Ready(Ok(connection)) => row.connection = Some(connection),
                Poll::Ready(Err(error)) => {
                    row.failure = Some(HttpProcessFailure::Connection(error))
                }
            }
            drop(row.handshake.take());
            return true;
        }
        if let Some(connection) = row.connection.as_mut() {
            if !connection.driver_completed() {
                if let Poll::Ready(result) = connection.poll_connection(cx) {
                    match result {
                        Ok(()) => row.closing = true,
                        Err(error) => {
                            row.failure = Some(HttpProcessFailure::Connection(
                                HttpConnectionError::from(error),
                            ));
                            row.hard_closing = true;
                        }
                    }
                    return true;
                }
            }
        }
        false
    }

    fn publish(
        row: &mut RequestRow<G, H>,
        value: Result<HttpResponseEnvelope, HttpRequestFailure>,
    ) {
        let reply = row
            .reply
            .as_ref()
            .expect("original request reply installed");
        {
            let mut slot = reply
                .slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(slot.value.is_none(), "HTTP reply slot published twice");
            slot.value = Some(value);
        }
        row.reply_published = true;
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| reply.reader.wake())) {
            row.notification_panic = Some(payload);
        }
    }

    /// Reset only this actual h2 stream. All operations are outside State
    /// locks. Protocol state exits here; queued Bytes independently retain G.
    /// Preserve an observed failure in the source/row for health disposition.
    fn stop_h2_request(row: &mut RequestRow<G, H>) -> bool {
        if row.reset {
            return false;
        }
        row.reset = true; // A panic must never cause an automatic re-poll/retry.
        if row.retained_guard.is_none() {
            row.retained_guard = row
                .input
                .as_ref()
                .map(|input| Arc::clone(&input.source_guard));
        }
        if let Some(upload) = row.upload.as_mut() {
            upload.reset();
        }
        drop((
            row.upload.take(),
            row.future.take(),
            row.outcome.take(),
            row.input.take(),
        ));
        if let Some(source) = row.source.as_mut() {
            source.dispose_after_stream_reset();
        }
        row.raw_complete = true;
        true
    }

    fn poll_request_row(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        let row = &mut self.requests[index];
        let Some(token) = row.token else {
            return false;
        };
        let connection = &mut self.connections[token.index()];
        if row.failure.is_some() || row.cleanup_panic.is_some() {
            return false;
        }
        let abandoned = row.reply.as_ref().is_some_and(|reply| {
            reply
                .slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .abandoned
        });
        if row.h2 && abandoned {
            let progress = Self::stop_h2_request(row);
            if !row.reply_published {
                Self::publish(row, Err(HttpRequestFailure::Abandoned));
            }
            return progress;
        }
        if abandoned {
            connection.closing = true;
            // A request abandoned before headers has no Incoming to reset.
            // Keep its original send future/input until exact C exit,
            // but do not wait for its own absent headers as a peer obligation.
            if row.source.is_none() {
                row.reset = true;
            }
            let unreceived = row.reply.as_ref().and_then(|reply| {
                reply
                    .slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .value
                    .take()
            });
            // The raw source is already in this row before a public body view
            // can be here. Dropping its unreceived ticket merely abandons it.
            drop(unreceived);
            if !row.reply_published {
                Self::publish(row, Err(HttpRequestFailure::Abandoned));
            }
            if row.future.is_none() && row.source.is_none() {
                drop(row.input.take());
                row.raw_complete = true;
            }
        }
        // Once pre-header abandonment is committed, retain the original send
        // future without polling it again. Deliberately dropping the driver
        // would otherwise turn our own close into a new transport failure.
        // retire_sources drops this exact future only after driver/IO exit.
        if row.reset && (row.h2 || row.source.is_none()) {
            return false;
        }
        if connection.hard_closing {
            return false;
        }
        if row.future.is_none() && row.source.is_none() && row.input.is_some() && !abandoned {
            let Some(sender) = connection.connection.as_mut() else {
                return false;
            };
            match sender.poll_ready(cx) {
                Poll::Pending => return false,
                Poll::Ready(Err(error)) => {
                    row.failure = Some(HttpProcessFailure::Connection(error));
                    return true;
                }
                Poll::Ready(Ok(())) => {}
            }
            // The original R owns every actual H1/H2 request object.
            let input = row.input.as_mut().unwrap();
            match sender.start_send_in(
                &mut input.request,
                &input.route,
                &input.source_guard,
                &mut row.upload,
            ) {
                Ok(future) => row.future = Some(future),
                Err(error) => {
                    row.failure = Some(HttpProcessFailure::Connection(error));
                    return true;
                }
            }
        }
        let mut progress = false;
        if let Some(upload) = row.upload.as_mut().filter(|u| !u.complete()) {
            match upload.poll_progress(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(UploadStep::Progress | UploadStep::Complete)) => progress = true,
                Poll::Ready(Err(error)) => {
                    row.failure = Some(HttpProcessFailure::Upload(error));
                    return true;
                }
            }
        }
        if let Some(future) = row.future.as_mut() {
            if let Poll::Ready(outcome) = Pin::new(future).poll(cx) {
                // Keep the actual source error visible before any completed
                // future destructor or reply notification callback can unwind.
                match outcome {
                    HttpResponseOutcome {
                        response: Err(error),
                        ..
                    } => {
                        row.failure = Some(HttpProcessFailure::Response(error));
                    }
                    outcome => row.outcome = Some(outcome),
                }
                drop(row.future.take());
            } else {
                return progress;
            }
        }
        if let Some(outcome) = row.outcome.take() {
            match outcome.response {
                Ok(response) => {
                    let (parts, raw) = response.into_parts();
                    let input = row.input.take().unwrap();
                    let guard = input.source_guard;
                    row.retained_guard = Some(Arc::clone(&guard));
                    let (source, body) = ResponseSource::new(raw, guard, input.handoff, None);
                    row.source = Some(source);
                    let envelope = HttpResponseEnvelope {
                        response: Response::from_parts(parts, body),
                        original_uri: outcome.original_uri,
                        is_head: outcome.is_head,
                    };
                    if !row.reply_published {
                        Self::publish(row, Ok(envelope));
                    } else {
                        drop(envelope);
                    }
                }
                Err(error) => {
                    row.failure = Some(HttpProcessFailure::Response(error));
                    return true;
                }
            }
            return true;
        }
        if row.raw_complete {
            if row.upload.as_ref().is_some_and(H2Upload::complete) {
                drop(row.upload.take());
                // Only metadata views remain. Actual queued Bytes, if any,
                // independently keep their original paid custody.
                drop(row.retained_guard.take());
                return true;
            }
            return progress;
        }
        if let Some(source) = row.source.as_mut() {
            match source.poll_progress(cx) {
                Poll::Pending => {}
                Poll::Ready(ResponseAction::Progress) => return true,
                Poll::Ready(ResponseAction::RawComplete) => {
                    row.raw_complete = true;
                    if row.h2 {
                        if row.upload.as_ref().is_some_and(H2Upload::complete) {
                            drop(row.upload.take());
                            drop(row.retained_guard.take());
                        }
                    } else {
                        drop(row.retained_guard.take());
                    }
                    return true;
                }
                Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Abandoned)) => {
                    if row.h2 {
                        return Self::stop_h2_request(row);
                    }
                    // Keep the SAME original guard in this R row. Local Incoming
                    // Drop is not a whole-stream/IO exit receipt. It only
                    // retires local recv custody under this continued owner.
                    source.dispose_after_connection_exit();
                    row.reset = true;
                    row.raw_complete = true;
                    connection.closing = true;
                    return true;
                }
                Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Failed)) => {
                    if row.h2 {
                        return Self::stop_h2_request(row);
                    }
                    connection.hard_closing = true;
                    return true;
                }
            }
        }
        progress
    }

    fn reap_request(&mut self, index: usize, connection_stopped: bool) -> bool {
        let row = &mut self.requests[index];
        let Some(_) = row.token else {
            return false;
        };
        if row.failure.is_some() || row.cleanup_panic.is_some() || row.notification_panic.is_some()
        {
            return false;
        }
        if row
            .source
            .as_ref()
            .is_some_and(|source| source.failure().is_some())
        {
            return false;
        }
        if row.reset && !row.h2 && !connection_stopped {
            return false;
        }
        if row.upload.is_some()
            || row.future.is_some()
            || row.outcome.is_some()
            || row.input.is_some()
        {
            return false;
        }
        if !row.raw_complete {
            return false;
        }
        if let Some(reply) = row.reply.as_ref() {
            let abandoned = reply
                .slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .abandoned;
            if abandoned {
                let value = reply
                    .slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .value
                    .take();
                drop(value);
            }
        }
        if row
            .source
            .as_mut()
            .is_some_and(|source| !source.view_exited())
        {
            return false;
        }
        if row
            .reply
            .as_mut()
            .is_some_and(|reply| Arc::get_mut(reply).is_none())
        {
            return false;
        }
        drop(row.source.take());
        drop(row.retained_guard.take());
        drop(row.reply.take());
        *row = RequestRow::empty();
        self.ingress
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requests[index]
            .phase = Phase::Free;
        true
    }

    fn close_connection_objects(&mut self, index: usize) -> bool {
        let token = match self.connections[index].token {
            Some(token) => token,
            None => return false,
        };
        let other_sources_live = self.requests.iter().any(|r| {
            r.token == Some(token)
                && !r.reset
                && (!r.raw_complete || r.upload.as_ref().is_some_and(|u| !u.complete()))
                && r.failure.is_none()
        });
        let row = &mut self.connections[index];
        if !row.hard_closing && (!row.closing || other_sources_live) {
            return false;
        }
        if row.cleanup_panic.is_some() {
            return false;
        }
        let owned = (
            row.handshake.take(),
            row.input.take(),
            row.connection.take(),
        );
        if owned.0.is_some() || owned.1.is_some() || owned.2.is_some() {
            // Tuple drop glue retires all original owning objects on unwind.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(owned))) {
                row.cleanup_panic = Some(payload);
            }
            return true;
        }
        row.stopped = true;
        false
    }

    fn retire_sources(&mut self, index: usize) -> bool {
        let Some(token) = self.connections[index].token else {
            return false;
        };
        if !self.connections[index].stopped || self.connections[index].cleanup_panic.is_some() {
            return false;
        }
        let mut progress = false;
        for row in self.requests.iter_mut().filter(|r| r.token == Some(token)) {
            if row.cleanup_panic.is_some() {
                continue;
            }
            // Original failure payloads remain until the real health owner
            // explicitly requests disposition, even though the driver stopped.
            if row.failure.is_some()
                || row.notification_panic.is_some()
                || row
                    .source
                    .as_ref()
                    .is_some_and(|source| source.failure().is_some())
            {
                continue;
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                if let Some(source) = row.source.as_mut() {
                    if !row.raw_complete {
                        source.dispose_after_connection_exit();
                        row.raw_complete = true;
                        progress = true;
                    }
                } else if !row.raw_complete {
                    row.raw_complete = true;
                    progress = true;
                }
                let had_objects = row.future.is_some()
                    || row.outcome.is_some()
                    || row.input.is_some()
                    || row.upload.is_some();
                drop(row.upload.take());
                drop(row.future.take());
                drop(row.outcome.take());
                drop(row.input.take());
                progress |= had_objects;
                if !row.reply_published {
                    Self::publish(row, Err(HttpRequestFailure::SourceFailed));
                    progress = true;
                }
                let abandoned = row.reply.as_ref().is_some_and(|reply| {
                    reply
                        .slot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .abandoned
                });
                if abandoned {
                    let value = row.reply.as_ref().and_then(|reply| {
                        reply
                            .slot
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .value
                            .take()
                    });
                    progress |= value.is_some();
                    drop(value);
                }
                if row
                    .source
                    .as_mut()
                    .is_some_and(|source| source.view_exited())
                {
                    // This also actually drops H. It may own an original
                    // handoff future, so raw=None alone does not prove exit.
                    drop(row.source.take());
                    progress = true;
                }
            }));
            if let Err(payload) = result {
                row.cleanup_panic = Some(payload);
            }
        }
        progress
    }

    fn dispose_failures(&mut self, index: usize) {
        let Some(token) = self.connections[index].token else {
            return;
        };
        // Explicit original health-owner disposition. No failure payload is
        // formatted, silently cleared, or destroyed under either State lock.
        let row = &mut self.connections[index];
        let causes = (row.failure.take(), row.cleanup_panic.take());
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(causes))) {
            row.cleanup_panic = Some(payload);
        }
        for request in self.requests.iter_mut().filter(|r| r.token == Some(token)) {
            let causes = (
                request.failure.take(),
                request.cleanup_panic.take(),
                request.notification_panic.take(),
            );
            let result = catch_unwind(AssertUnwindSafe(|| {
                drop(causes);
                if request
                    .source
                    .as_ref()
                    .is_some_and(|source| source.failure().is_some())
                {
                    request
                        .source
                        .as_mut()
                        .unwrap()
                        .dispose_after_connection_exit();
                    request.raw_complete = true;
                    request.reset = true;
                }
            }));
            if let Err(payload) = result {
                request.cleanup_panic = Some(payload);
            }
        }
    }

    fn poll_turn(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut progress = self.receive();
        for index in 0..self.connections.len() {
            let result = catch_unwind(AssertUnwindSafe(|| self.poll_connection_row(index, cx)));
            match result {
                Ok(value) => progress |= value,
                Err(payload) => {
                    if self.connections[index].failure.is_none() {
                        self.connections[index].failure = Some(HttpProcessFailure::Panic(payload));
                    } else {
                        self.connections[index].cleanup_panic = Some(payload);
                    }
                    self.connections[index].hard_closing = true;
                    progress = true;
                }
            }
            if self.connections[index].hard_closing
                || self.connections[index].failure.is_some()
                || self.connections[index].cleanup_panic.is_some()
            {
                if let Some(token) = self.connections[index].token {
                    self.stop_admission(token, true);
                }
            } else if self.connections[index].closing {
                if let Some(token) = self.connections[index].token {
                    self.stop_admission(token, false);
                }
            }
        }
        for index in 0..self.requests.len() {
            let result = catch_unwind(AssertUnwindSafe(|| self.poll_request_row(index, cx)));
            match result {
                Ok(value) => progress |= value,
                Err(payload) => {
                    if self.requests[index].failure.is_none() {
                        self.requests[index].failure = Some(HttpProcessFailure::Panic(payload));
                    } else {
                        self.requests[index].cleanup_panic = Some(payload);
                    }
                    progress = true;
                }
            }
            if self.requests[index].failure.is_some() && !self.requests[index].reply_published {
                Self::publish(
                    &mut self.requests[index],
                    Err(HttpRequestFailure::SourceFailed),
                );
            }
            if let Some(token) = self.requests[index].token {
                let row = &self.requests[index];
                let source_failure = row.source.as_ref().and_then(|s| s.failure());
                let failed = row.failure.is_some()
                    || row.notification_panic.is_some()
                    || row.cleanup_panic.is_some()
                    || source_failure.is_some();
                let connection_failure = match row.failure.as_ref() {
                    Some(HttpProcessFailure::Connection(
                        HttpConnectionError::Invalid(_)
                        | HttpConnectionError::MissingRequest
                        | HttpConnectionError::NotReady,
                    )) => false,
                    Some(HttpProcessFailure::Connection(HttpConnectionError::H2(error))) => {
                        error.is_io() || error.is_go_away()
                    }
                    Some(HttpProcessFailure::Connection(_) | HttpProcessFailure::Panic(_)) => true,
                    _ => false,
                };
                let isolate = row.h2
                    && !connection_failure
                    && row.cleanup_panic.is_none()
                    && row.notification_panic.is_none()
                    && !matches!(
                        source_failure,
                        Some(super::response::ResponseFailure::PollPanic(_))
                    );
                if failed && isolate {
                    match catch_unwind(AssertUnwindSafe(|| {
                        Self::stop_h2_request(&mut self.requests[index])
                    })) {
                        Ok(value) => progress |= value,
                        Err(payload) => {
                            self.requests[index].cleanup_panic = Some(payload);
                            self.stop_admission(token, true);
                        }
                    }
                } else if failed {
                    self.stop_admission(token, true);
                } else if self.connections[token.index()].closing {
                    self.stop_admission(token, false);
                }
            }
        }
        for index in 0..self.connections.len() {
            let result = catch_unwind(AssertUnwindSafe(|| self.close_connection_objects(index)));
            match result {
                Ok(value) => progress |= value,
                Err(payload) => {
                    self.connections[index].cleanup_panic = Some(payload);
                }
            }
            let result = catch_unwind(AssertUnwindSafe(|| self.retire_sources(index)));
            match result {
                Ok(value) => progress |= value,
                Err(payload) => {
                    self.connections[index].cleanup_panic = Some(payload);
                }
            }
        }
        for index in 0..self.requests.len() {
            let stopped = self.requests[index]
                .token
                .is_some_and(|t| self.connections[t.index()].stopped);
            match catch_unwind(AssertUnwindSafe(|| self.reap_request(index, stopped))) {
                Ok(value) => progress |= value,
                Err(payload) => self.requests[index].cleanup_panic = Some(payload),
            }
        }
        for index in 0..self.connections.len() {
            let row = &mut self.connections[index];
            if let Some(token) = row.token {
                if row.stopped
                    && row.failure.is_none()
                    && row.cleanup_panic.is_none()
                    && !self.requests.iter().any(|r| r.token == Some(token))
                {
                    {
                        let mut rows = self
                            .ingress
                            .rows
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let slot = &mut rows.connections[index];
                        slot.phase = Phase::Free;
                        slot.token = None;
                    }
                    *row = ConnectionRow::empty();
                    progress = true;
                }
            }
        }
        let done = self
            .ingress
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
            && self.connections.iter().all(|r| r.token.is_none())
            && self.requests.iter().all(|r| r.token.is_none());
        if done {
            Poll::Ready(())
        } else {
            if progress {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

struct Service<G, H: PayloadHandoff> {
    cell: Arc<HttpProcessCell<HttpProcessState<G, H>>>,
    ingress: Arc<Ingress<G, H>>,
}
impl<G: Send + Sync + 'static, H: PayloadHandoff> Future for Service<G, H> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.ingress.signal.service.register(cx.waker());
        let result = self.cell.with_state(|state| state.poll_turn(cx));
        // Observers can retry a busy read only after this real lease exit.
        self.ingress.signal.observer.wake();
        match result {
            Ok(result) => result,
            Err(_) => Poll::Pending,
        }
    }
}

/// The role/process owner retains Self outside every cancellable shutdown await.
/// The sole service task captures a cell view, never this join/root itself.
#[must_use]
pub(crate) struct HttpProcessRoot<G, H: PayloadHandoff> {
    driver: Option<FileTask>,
    driver_failure: Option<JoinError>,
    cell: Option<Arc<HttpProcessCell<HttpProcessState<G, H>>>>,
    ingress: Arc<Ingress<G, H>>,
    file_runtime: Arc<dyn FileIoRuntime>,
    joined: bool,
    drained: bool,
}
impl<G: Send + Sync + 'static, H: PayloadHandoff> HttpProcessRoot<G, H> {
    pub(crate) fn try_new(
        handle: Handle,
        file_runtime: Arc<dyn FileIoRuntime>,
        limits: HttpProcessLimits,
    ) -> FileResult<(Self, HttpSubmission<G, H>)> {
        let signal = Arc::new(Signal {
            service: AtomicWaker::new(),
            observer: AtomicWaker::new(),
        });
        let ingress = Arc::new(Ingress {
            signal,
            rows: Mutex::new(IngressRows {
                closed: false,
                connections: storage(limits.connections.get(), || ConnectionIngress {
                    generation: 0,
                    phase: Phase::Free,
                    accepting: false,
                    hard_close: false,
                    token: None,
                    input: None,
                })?,
                requests: storage(limits.requests.get(), || RequestIngress {
                    generation: 0,
                    phase: Phase::Free,
                    input: None,
                    reply: None,
                })?,
            }),
        });
        let state = HttpProcessState {
            ingress: Arc::clone(&ingress),
            connections: storage(limits.connections.get(), ConnectionRow::empty)?,
            requests: storage(limits.requests.get(), RequestRow::empty)?,
        };
        let cell = Arc::new(HttpProcessCell::new(Box::new(state)));
        let service = Service {
            cell: Arc::clone(&cell),
            ingress: Arc::clone(&ingress),
        };
        let driver = FileTask::new(handle.spawn(service));
        let submission = HttpSubmission {
            ingress: Arc::downgrade(&ingress),
        };
        Ok((
            Self {
                driver: Some(driver),
                driver_failure: None,
                cell: Some(cell),
                ingress,
                file_runtime,
                joined: false,
                drained: false,
            },
            submission,
        ))
    }
    pub(crate) fn file_runtime(&self) -> &Arc<dyn FileIoRuntime> {
        &self.file_runtime
    }
    pub(crate) fn close(&self) {
        self.ingress
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        self.ingress.signal.wake();
    }
    /// Observe and accept THESE exact original failures in one synchronous
    /// lease before their actual disposition. No queued/stale boolean ack can
    /// erase a new failure arriving between health observation and cleanup.
    /// This is not an exit proof; every actual source/IO still gates reuse.
    pub(crate) fn poll_dispose_failures(
        &self,
        cx: &mut Context<'_>,
        token: HttpConnectionToken,
        accept: impl FnOnce(&HttpProcessState<G, H>) -> bool,
    ) -> Poll<bool> {
        self.ingress.signal.observer.register(cx.waker());
        let result = self.cell.as_ref().unwrap().with_state(|state| {
            let exact = state
                .connections
                .get(token.index())
                .and_then(|r| r.token)
                .is_some_and(|source| source == token);
            if exact && accept(state) {
                state.dispose_failures(token.index());
                true
            } else {
                false
            }
        });
        self.ingress.signal.wake();
        match result {
            Ok(result) => Poll::Ready(result),
            Err(_) => Poll::Pending,
        }
    }
    pub(crate) fn inspect<R>(
        &self,
        cx: &mut Context<'_>,
        visit: impl FnOnce(&HttpProcessState<G, H>) -> R,
    ) -> Poll<R> {
        self.ingress.signal.observer.register(cx.waker());
        let result = self.cell.as_ref().unwrap().with_state(|state| visit(state));
        // Return of an observer lease also wakes a potentially busy service.
        self.ingress.signal.wake();
        match result {
            Ok(result) => Poll::Ready(result),
            Err(_) => Poll::Pending,
        }
    }
    /// Cancellation leaves the original FileTask, state and opaque errors here.
    /// No timeout is an exit receipt; caller may bound this borrowed poll await.
    pub(crate) fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), JoinError>> {
        self.close();
        if !self.joined {
            match self.driver.as_mut().unwrap().poll_drain(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(outcome) => {
                    self.joined = true;
                    if let Err(error) = outcome {
                        self.driver_failure = Some(error);
                    }
                }
            }
        }
        if self.driver_failure.is_some() {
            // Recovery uses the SAME state only after the one original join.
            let result = self
                .cell
                .as_ref()
                .unwrap()
                .with_state(|state| state.poll_turn(cx));
            if !matches!(result, Ok(Poll::Ready(()))) {
                return Poll::Pending;
            }
        }
        self.drained = true;
        match self.driver_failure.take() {
            Some(error) => Poll::Ready(Err(error)),
            None => Poll::Ready(Ok(())),
        }
    }
}
impl<G, H: PayloadHandoff> Drop for HttpProcessRoot<G, H> {
    fn drop(&mut self) {
        if !self.drained {
            // Close the SAME original ingress before retaining failed custody.
            // The service may still hold a strong ingress view after root loss.
            // No foreign wake/Drop runs before the join/state are retained.
            self.ingress
                .rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed = true;
            // Illegal root loss is fail-closed: retain the ORIGINAL join and
            // charged state to process exit, not a detached JoinHandle Drop or
            // a synthetic clean shutdown. Normal ownership must poll_shutdown.
            std::mem::forget(self.driver.take());
            std::mem::forget(self.cell.take());
            std::mem::forget(self.driver_failure.take());
        }
    }
}

#[cfg(test)]
mod tests;
