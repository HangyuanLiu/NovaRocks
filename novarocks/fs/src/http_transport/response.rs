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

//! One original request row owns the raw response. Its public body is a ticket.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use super::connection::{RawBody, RawHttpError};
use bytes::Bytes;
use futures::Stream;
use futures::task::AtomicWaker;
use http_body::{Body, Frame};
use opendal::{Buffer, Error, ErrorKind, HttpBody};

use crate::{FileError, FileResult};

/// The caller's receive envelope already covers one raw frame before polling.
/// Handoff transfers that same backing to the actual Work, without charging it
/// twice. The returned guard must also cover one Bytes::from_owner retention
/// cell. Pending keeps the received frame in the original process envelope.
/// A refused handoff is a resource/control failure, never Unsupported payload.
/// Pending must register a real wake and only wait on independently releasable
/// capacity; this request's own retained payload must not cause self-wait.
pub(crate) trait PayloadHandoff: Send + 'static {
    type Guard: Send + 'static;
    fn poll_handoff(
        &mut self,
        cx: &mut Context<'_>,
        received: &Bytes,
    ) -> Poll<FileResult<Self::Guard>>;
}

pub(crate) enum ResponseFailure {
    Body(RawHttpError),
    Headers(FileError),
    Handoff(FileError),
    PollPanic(Box<dyn Any + Send + 'static>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StopReason {
    Abandoned,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseAction {
    Progress,
    /// Only this root's raw Incoming/source lease exited; view metadata and
    /// payload aliases have their own original lifetimes.
    RawComplete,
    /// Keep this row and all its custody. The original connection owner must
    /// quiesce this precise connection, preserving its other active requests.
    NeedsQuiesce(StopReason),
}

// Keep every raw object in one drop-glue unit. Even if one destructor unwinds,
// its remaining fields exit before RawLease's local source guard is released.
struct RawObjects {
    body: Option<RawBody>,
    frame: Option<Frame<Bytes>>,
    failure: Option<ResponseFailure>,
}
struct RawLease<G> {
    objects: Option<RawObjects>,
    guard: Option<G>,
}
impl<G> Drop for RawLease<G> {
    fn drop(&mut self) {
        let guard = self.guard.take();
        drop(self.objects.take());
        drop(guard);
    }
}

// Field order is deliberate; backing exits before its original retention.
struct PayloadOwner<G> {
    bytes: Bytes,
    _guard: G,
}
impl<G> AsRef<[u8]> for PayloadOwner<G> {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

#[derive(Clone, Copy)]
enum End {
    Open,
    Eos,
    Failed,
}
struct Slot {
    data: Option<Bytes>,
    abandoned: bool,
    end: End,
}
struct Shared {
    slot: Mutex<Slot>,
    reader: AtomicWaker,
    source: AtomicWaker,
}
impl Shared {
    fn slot(&self) -> MutexGuard<'_, Slot> {
        self.slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct Ticket {
    shared: Arc<Shared>,
    terminal: bool,
}
impl Stream for Ticket {
    type Item = opendal::Result<Buffer>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminal {
            return Poll::Ready(None);
        }
        this.shared.reader.register(cx.waker());
        let (data, end) = {
            let mut slot = this.shared.slot();
            (slot.data.take(), slot.end)
        };
        if let Some(data) = data {
            // Original bytes/retention are installed in the return value before
            // a foreign source wake can panic. A panic drops that same value.
            let result = Poll::Ready(Some(Ok(Buffer::from(data))));
            this.shared.source.wake();
            return result;
        }
        match end {
            End::Open => Poll::Pending,
            End::Eos => {
                this.terminal = true;
                Poll::Ready(None)
            }
            End::Failed => {
                this.terminal = true;
                // Only a bounded public marker. The exact cause stays in the
                // original source; its health/control owner must consume it.
                Poll::Ready(Some(Err(Error::new(
                    ErrorKind::Unexpected,
                    "HTTP response remains in its original failed source",
                )
                .set_permanent())))
            }
        }
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.shared.slot().abandoned = true;
        // Do not take/drop the slot or the raw body on consumer cancellation.
        self.shared.source.wake();
    }
}

/// The original fixed request row stores this value and drives bounded turns.
/// No runtime, task, ledger, quota or global registry is created here.
#[must_use]
pub(crate) struct ResponseSource<G, H: PayloadHandoff> {
    raw: Option<RawLease<G>>,
    handoff: Option<H>,
    shared: Arc<Shared>,
    complete: bool,
    disposed: bool,
}
impl<G: Send + 'static, H: PayloadHandoff> ResponseSource<G, H> {
    pub(crate) fn new(
        body: impl Into<RawBody>,
        source_guard: G,
        handoff: H,
        length: Option<u64>,
    ) -> (Self, HttpBody) {
        let shared = Arc::new(Shared {
            slot: Mutex::new(Slot {
                data: None,
                abandoned: false,
                end: End::Open,
            }),
            reader: AtomicWaker::new(),
            source: AtomicWaker::new(),
        });
        let ticket = Ticket {
            shared: Arc::clone(&shared),
            terminal: false,
        };
        (
            Self {
                raw: Some(RawLease {
                    objects: Some(RawObjects {
                        body: Some(body.into()),
                        frame: None,
                        failure: None,
                    }),
                    guard: Some(source_guard),
                }),
                handoff: Some(handoff),
                shared,
                complete: false,
                disposed: false,
            },
            HttpBody::new(ticket, length),
        )
    }

    /// The original row must not be recycled while an old ticket still owns
    /// its metadata. RawComplete alone is not a request-row exit receipt.
    pub(crate) fn view_exited(&mut self) -> bool {
        Arc::get_mut(&mut self.shared).is_some()
    }

    pub(crate) fn failure(&self) -> Option<&ResponseFailure> {
        self.raw.as_ref()?.objects.as_ref()?.failure.as_ref()
    }

    /// Install the raw source before fallible response-header projection. A
    /// second cause is returned to its actual caller, never silently destroyed.
    pub(crate) fn fail_headers(&mut self, error: FileError) -> Result<(), FileError> {
        if self.complete || self.disposed || self.failure().is_some() {
            return Err(error);
        }
        self.raw.as_mut().unwrap().objects.as_mut().unwrap().failure =
            Some(ResponseFailure::Headers(error));
        self.publish_failure();
        Ok(())
    }

    fn publish_failure(&self) {
        self.shared.slot().end = End::Failed;
        self.shared.reader.wake();
    }

    /// At most one actual raw frame or one handoff is processed per turn.
    /// Progress means the original driver must schedule its next finite turn.
    /// An unwound Incoming is retained and never polled again.
    pub(crate) fn poll_progress(&mut self, cx: &mut Context<'_>) -> Poll<ResponseAction> {
        self.shared.source.register(cx.waker());
        if self.complete || self.disposed {
            return Poll::Ready(ResponseAction::RawComplete);
        }
        if self.failure().is_some() {
            return Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Failed));
        }
        {
            let slot = self.shared.slot();
            if slot.abandoned {
                return Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Abandoned));
            }
            if slot.data.is_some() {
                return Poll::Pending;
            }
        }
        let objects = self.raw.as_mut().unwrap().objects.as_mut().unwrap();
        if let Some(frame) = objects.frame.as_ref() {
            if let Some(data) = frame.data_ref() {
                if data.is_empty() {
                    // ReadStream reserves an empty Buffer for EOS. Retire a
                    // legal empty DATA frame here without publishing false EOS.
                    drop(objects.frame.take());
                    return Poll::Ready(ResponseAction::Progress);
                }
                let handed = catch_unwind(AssertUnwindSafe(|| {
                    self.handoff.as_mut().unwrap().poll_handoff(cx, data)
                }));
                let guard = match handed {
                    Ok(Poll::Pending) => return Poll::Pending,
                    Ok(Poll::Ready(Ok(guard))) => guard,
                    Ok(Poll::Ready(Err(error))) => {
                        objects.failure = Some(ResponseFailure::Handoff(error));
                        self.publish_failure();
                        return Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Failed));
                    }
                    Err(payload) => {
                        objects.failure = Some(ResponseFailure::PollPanic(payload));
                        self.publish_failure();
                        return Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Failed));
                    }
                };
                let data = objects.frame.take().unwrap().into_data().ok().unwrap();
                let retained = Bytes::from_owner(PayloadOwner {
                    bytes: data,
                    _guard: guard,
                });
                {
                    let mut slot = self.shared.slot();
                    debug_assert!(slot.data.is_none());
                    slot.data = Some(retained);
                }
                self.shared.reader.wake();
            } else {
                // Trailers are not payload for OpenDAL's data stream. Drop
                // this actual frame outside the slot lock before further poll.
                drop(objects.frame.take());
            }
            return Poll::Ready(ResponseAction::Progress);
        }
        match catch_unwind(AssertUnwindSafe(|| {
            Pin::new(objects.body.as_mut().expect("live original raw body")).poll_frame(cx)
        })) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(Some(Ok(frame)))) => {
                objects.frame = Some(frame);
                Poll::Ready(ResponseAction::Progress)
            }
            Ok(Poll::Ready(Some(Err(error)))) => {
                objects.failure = Some(ResponseFailure::Body(error));
                self.publish_failure();
                Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Failed))
            }
            Err(payload) => {
                objects.failure = Some(ResponseFailure::PollPanic(payload));
                self.publish_failure();
                Poll::Ready(ResponseAction::NeedsQuiesce(StopReason::Failed))
            }
            Ok(Poll::Ready(None)) => {
                // The slot was empty, and all prior DATA was transferred. Raw
                // objects actually drop before releasing the source guard/EOS.
                drop(self.raw.take());
                self.complete = true;
                self.shared.slot().end = End::Eos;
                self.shared.reader.wake();
                Poll::Ready(ResponseAction::RawComplete)
            }
        }
    }

    /// Public h2 reset was issued by this exact R. Drop this R's raw protocol
    /// refs without closing the shared connection. An observed failure remains
    /// in its original lease until explicit health-owner disposition.
    pub(crate) fn dispose_after_stream_reset(&mut self) {
        let pending = {
            let mut slot = self.shared.slot();
            slot.end = End::Failed;
            slot.data.take()
        };
        drop(pending);
        if self.failure().is_some() {
            let objects = self.raw.as_mut().unwrap().objects.as_mut().unwrap();
            drop((objects.body.take(), objects.frame.take()));
        } else {
            drop(self.raw.take());
        }
        self.disposed = true;
        self.shared.reader.wake();
    }

    /// The original process owner calls this only after the precise abnormal
    /// connection/stream cleanup obligation has actually exited or been paid
    /// over to its original owner. This method mints no exit receipt or grant.
    /// Dropping the caller's await or public body must never call it. The row
    /// still stays installed until view_exited; this does not consume it.
    pub(crate) fn dispose_after_connection_exit(&mut self) {
        let pending = {
            let mut slot = self.shared.slot();
            slot.end = End::Failed;
            slot.data.take()
        };
        drop(pending);
        drop(self.raw.take());
        self.disposed = true;
        self.shared.reader.wake();
    }
}
impl<G, H: PayloadHandoff> Drop for ResponseSource<G, H> {
    fn drop(&mut self) {
        if !self.complete && !self.disposed {
            // Illegal request-root loss: retain only this already-paid row's
            // original custody to process exit. This is not normal shutdown.
            // Do not call foreign wakers before preserving the original state.
            std::mem::forget(self.raw.take());
            std::mem::forget(self.handoff.take());
            std::mem::forget(Arc::clone(&self.shared));
        }
    }
}

#[cfg(test)]
mod tests;
