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

//! Receiver-owned linear admission, exact streams and genuine input exhaustion.
use super::*;
use crate::runtime::{
    fragment::ExecutionFailure,
    mem_tracker::TrackedBytes,
    observable::ObserverSubscription,
    operator_byte_credit::{
        CreditAcquire, HolderAcquire, OperatorByteCredit, PortCredit, PortCreditSpec, QueuePermit,
        StagingAcquire, StagingCredit, StagingPermit,
    },
};
use novarocks_execution_contract::{
    ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeFrameGeometry,
    ExchangeFrameKind, ExchangeFrameSequence, ExchangePermitGrant, ExchangePermitRequest,
    ExchangePermitToken, ExchangeSource, ExchangeStreamClose, ExchangeStreamIdentity,
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    future::Future,
    pin::Pin,
    sync::Weak,
    task::{Context, Poll, Waker},
};
use tokio_util::sync::CancellationToken;

pub type ExchangeReceiveFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ExchangeFailure>> + Send + 'a>>;
#[derive(Clone, Copy, Debug)]
pub enum ExchangePermitAdmission {
    Granted(ExchangePermitGrant),
    Closed(ExchangeStreamClose),
}
#[derive(Clone, Copy, Debug)]
pub struct ExchangeFrameProvenance {
    pub source: ExchangeSource,
    pub sequence: ExchangeFrameSequence,
    pub kind: ExchangeFrameKind,
}
pub struct ExchangeSourcedChunk {
    pub chunk: Chunk,
    pub provenance: ExchangeFrameProvenance,
}
pub enum ExchangeSourcedPopResult {
    Chunk(ExchangeSourcedChunk),
    Finished(ExchangeRecvStats),
}
#[derive(Clone, Debug)]
pub struct ExchangeReceiverCloseSnapshot {
    pub streams: Box<[ExchangeStreamClose]>,
    pub active_acquires: usize,
    pub unused_grants: usize,
    pub actual_jobs: usize,
}

struct PoolSlot {
    reserved: bool,
    waiting_r: bool,
    previous_r: Option<usize>,
    next_r: Option<usize>,
    waker: Option<Waker>,
    expiry: Option<(Instant, Weak<NetworkReceiver>, usize, ExchangePermitToken)>,
}
struct PoolState {
    slots: Vec<PoolSlot>,
    head: Option<usize>,
    tail: Option<usize>,
}
struct Pool {
    staging: StagingCredit,
    state: Mutex<PoolState>,
    expiry_changed: Arc<tokio::sync::Notify>,
    stopped: CancellationToken,
    subscription: Mutex<Option<ObserverSubscription>>,
    _metadata: TrackedBytes,
}
/// One BE transient pool and one fixed metadata/flow table. Its host is the
/// installed native runtime; this owner never creates a Tokio runtime.
#[derive(Clone)]
pub struct ExchangeReceivePool(Arc<Pool>);
impl ExchangeReceivePool {
    pub fn bind(
        staging_bytes: usize,
        max_streams: usize,
        parent: Arc<MemTracker>,
        host: &tokio::runtime::Handle,
    ) -> Result<Self, ExchangeFailure> {
        if max_streams == 0 {
            return Err(exchange_contract(
                "receiver pool needs exact finite flow positions",
            ));
        }
        let metadata = std::alloc::Layout::array::<PoolSlot>(max_streams)
            .map_err(|_| exchange_contract("receiver flow table extent overflow"))?
            .size()
            .checked_add(std::mem::size_of::<Pool>())
            .ok_or_else(|| exchange_contract("receiver pool metadata extent overflow"))?;
        let charge = TrackedBytes::try_new(metadata, parent.clone())?;
        let staging = StagingCredit::bind(staging_bytes, 1, parent)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(max_streams)
            .map_err(failure::allocation)?;
        for _ in 0..max_streams {
            slots.push(PoolSlot {
                reserved: false,
                waiting_r: false,
                previous_r: None,
                next_r: None,
                waker: None,
                expiry: None,
            });
        }
        let pool = Arc::new(Pool {
            staging,
            state: Mutex::new(PoolState {
                slots,
                head: None,
                tail: None,
            }),
            expiry_changed: Arc::new(tokio::sync::Notify::new()),
            stopped: CancellationToken::new(),
            subscription: Mutex::new(None),
            _metadata: charge,
        });
        let weak = Arc::downgrade(&pool);
        let subscription = pool
            .staging
            .observable()
            .try_subscribe(Arc::new(move || {
                if let Some(pool) = weak.upgrade() {
                    pool.wake_r();
                }
            }))
            .map_err(|error| ExchangeFailure::from(ExecutionFailure::from_domain(error)))?;
        *pool
            .subscription
            .lock()
            .expect("receiver pool subscription") = Some(subscription);
        let weak = Arc::downgrade(&pool);
        let changed = pool.expiry_changed.clone();
        let stop = pool.stopped.clone();
        host.spawn(async move {
            loop {
                let notified = changed.notified();
                let Some(pool) = weak.upgrade() else { return; };
                pool.reap(Instant::now());
                let next = pool.state.lock().expect("receiver pool lock").slots.iter().filter_map(|slot| slot.expiry.as_ref().map(|entry| entry.0)).min();
                drop(pool);
                if let Some(at) = next {
                    tokio::select! { _ = stop.cancelled() => return, _ = notified => {}, _ = tokio::time::sleep_until(at.into()) => {} }
                } else { tokio::select! { _ = stop.cancelled() => return, _ = notified => {} } }
            }
        });
        Ok(Self(pool))
    }
    fn reserve(&self, count: usize) -> Result<Vec<usize>, ExchangeFailure> {
        let mut state = self.0.state.lock().expect("receiver pool lock");
        if state.slots.iter().filter(|slot| !slot.reserved).count() < count {
            return Err(ExchangeFailure::from(ExecutionFailure::from_domain(
                crate::runtime::operator_byte_credit::CreditFailure::Capacity {
                    requested: count,
                    limit: state.slots.iter().filter(|slot| !slot.reserved).count(),
                },
            )));
        }
        let mut positions = Vec::new();
        positions
            .try_reserve_exact(count)
            .map_err(failure::allocation)?;
        for (index, slot) in state.slots.iter_mut().enumerate() {
            if !slot.reserved {
                slot.reserved = true;
                positions.push(index);
                if positions.len() == count {
                    break;
                }
            }
        }
        Ok(positions)
    }
}
impl Pool {
    fn wake_all(&self) {
        let count = self.state.lock().expect("receiver pool lock").slots.len();
        for index in 0..count {
            let waker = self.state.lock().expect("receiver pool lock").slots[index]
                .waker
                .take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
    fn register(&self, index: usize, waker: &Waker) {
        let old = self.state.lock().expect("receiver pool lock").slots[index]
            .waker
            .replace(waker.clone());
        drop(old);
    }
    fn wake_position(&self, index: usize) {
        let waker = self.state.lock().expect("receiver pool lock").slots[index]
            .waker
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn wake_r(&self) {
        let waker = {
            let mut state = self.state.lock().expect("receiver pool lock");
            state.head.and_then(|head| state.slots[head].waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn unlink_r(state: &mut PoolState, index: usize) {
        if !state.slots[index].waiting_r {
            return;
        }
        let previous = state.slots[index].previous_r;
        let next = state.slots[index].next_r;
        if let Some(previous) = previous {
            state.slots[previous].next_r = next;
        } else {
            state.head = next;
        }
        if let Some(next) = next {
            state.slots[next].previous_r = previous;
        } else {
            state.tail = previous;
        }
        state.slots[index].waiting_r = false;
        state.slots[index].previous_r = None;
        state.slots[index].next_r = None;
    }
    fn want_r(&self, index: usize) -> bool {
        let mut state = self.state.lock().expect("receiver pool lock");
        if !state.slots[index].waiting_r {
            let tail = state.tail;
            state.slots[index].waiting_r = true;
            state.slots[index].previous_r = tail;
            if let Some(tail) = tail {
                state.slots[tail].next_r = Some(index);
            } else {
                state.head = Some(index);
            }
            state.tail = Some(index);
        }
        state.head == Some(index)
    }
    fn took_r(&self, index: usize) {
        let mut state = self.state.lock().expect("receiver pool lock");
        Self::unlink_r(&mut state, index);
    }
    fn clear(&self, index: usize) {
        let waker = {
            let mut state = self.state.lock().expect("receiver pool lock");
            Self::unlink_r(&mut state, index);
            state.slots[index].expiry = None;
            state.slots[index].waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        self.expiry_changed.notify_one();
        self.wake_r();
    }
    fn reap(&self, now: Instant) {
        let count = self.state.lock().expect("receiver pool lock").slots.len();
        for slot in 0..count {
            let expired = {
                let mut state = self.state.lock().expect("receiver pool lock");
                if state.slots[slot]
                    .expiry
                    .as_ref()
                    .is_some_and(|entry| entry.0 <= now)
                {
                    state.slots[slot].expiry.take()
                } else {
                    None
                }
            };
            if let Some((_, receiver, index, token)) = expired {
                if let Some(receiver) = receiver.upgrade() {
                    receiver.expire(index, token);
                }
            }
        }
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.stopped.cancel();
        self.expiry_changed.notify_one();
    }
}

pub struct ExchangeReceiverNetworkSpec {
    pub streams: Box<[ExchangeStreamIdentity]>,
    pub delivery: ExchangeDeliveryRequirement,
    pub kind: ExchangeFrameKind,
    pub per_contributor_bytes: usize,
    pub contributors: usize,
    pub workspace_bytes: usize,
    pub queue_positions: usize,
    pub holder_positions: usize,
    pub minimum_frame_bytes: usize,
    pub max_wire_bytes: u64,
    pub max_chunks: usize,
    pub permit_ttl: Duration,
}
/// Opaque execution capacity, created by the bound builder; adapters cannot
/// manufacture Q/H or recover capacity from the public token.
#[derive(Clone)]
pub struct ExchangeReceiverNetworkContract(pub(super) Arc<NetworkReceiver>);
struct Ticket {
    request: ExchangePermitRequest,
    queue: Option<QueuePermit>,
    staging: Option<StagingPermit>,
}
enum Phase {
    Idle,
    Waiting(Ticket),
    Granted {
        ticket: Ticket,
        token: ExchangePermitToken,
        expires: Instant,
    },
    Claimed,
    End(ExchangeFrameSequence),
    Closed,
}
struct Stream {
    identity: ExchangeStreamIdentity,
    next: ExchangeFrameSequence,
    phase: Phase,
}
struct QueuedFrame {
    provenance: ExchangeFrameProvenance,
    chunks: VecDeque<Chunk>,
    queue: Option<QueuePermit>,
    holder: Option<ValueTypeStorageLifetime>,
    holder_wait: Option<(Arc<Observable>, u64)>,
}
struct NetworkState {
    streams: Vec<Stream>,
    queue: VecDeque<QueuedFrame>,
    cursor: usize,
    closed: bool,
    failure: Option<ExchangeFailure>,
    consumed: bool,
    stats: ExchangeRecvStats,
}
pub(super) struct NetworkReceiver {
    state: Mutex<NetworkState>,
    pool: ExchangeReceivePool,
    positions: Vec<usize>,
    credit: OperatorByteCredit,
    port: PortCredit,
    observable: Arc<Observable>,
    subscription: Mutex<Option<ObserverSubscription>>,
    delivery: ExchangeDeliveryRequirement,
    pub(super) kind: ExchangeFrameKind,
    max_wire_bytes: u64,
    max_chunks: usize,
    permit_ttl: Duration,
    actual_jobs: AtomicUsize,
    _metadata: Arc<TrackedBytes>,
}
impl ExchangeReceiverNetworkContract {
    pub fn bind(
        pool: ExchangeReceivePool,
        spec: ExchangeReceiverNetworkSpec,
        parent: Arc<MemTracker>,
    ) -> Result<Self, ExchangeFailure> {
        if spec.streams.is_empty()
            || spec.max_chunks == 0
            || spec.max_wire_bytes == 0
            || spec.permit_ttl.is_zero()
            || spec.kind == ExchangeFrameKind::End
        {
            return Err(exchange_contract(
                "network receiver requires complete finite input facts",
            ));
        }
        let first = spec.streams[0];
        let mut unique = std::collections::BTreeSet::new();
        for stream in &spec.streams {
            if stream.destination() != first.destination()
                || stream.destination_node() != first.destination_node()
                || stream.destination_fragment_instance_id()
                    != first.destination_fragment_instance_id()
                || !unique.insert(*stream)
            {
                return Err(exchange_contract(
                    "network input stream set is not exact and unique",
                ));
            }
        }
        let metadata = std::alloc::Layout::array::<Stream>(spec.streams.len())
            .map_err(|_| exchange_contract("receiver stream state extent overflow"))?
            .size()
            .checked_add(
                std::alloc::Layout::array::<QueuedFrame>(spec.queue_positions)
                    .map_err(|_| exchange_contract("receiver queue extent overflow"))?
                    .size(),
            )
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<NetworkReceiver>()))
            .ok_or_else(|| exchange_contract("receiver metadata extent overflow"))?;
        let metadata = metadata
            .checked_add(
                Observable::bounded_backing_bytes(129)
                    .map_err(|error| ExchangeFailure::from(ExecutionFailure::from_domain(error)))?,
            )
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Observable>()))
            .ok_or_else(|| exchange_contract("receiver observable metadata extent overflow"))?;
        let charge = Arc::new(TrackedBytes::try_new(metadata, parent.clone())?);
        let observable = Arc::new(
            Observable::try_bounded_retained(129, charge.clone())
                .map_err(|error| ExchangeFailure::from(ExecutionFailure::from_domain(error)))?,
        );
        let credit = OperatorByteCredit::bind(
            spec.per_contributor_bytes,
            spec.contributors,
            spec.workspace_bytes
                .checked_add(metadata)
                .ok_or_else(|| exchange_contract("receiver workspace overflow"))?,
            &[PortCreditSpec {
                queue_positions: spec.queue_positions,
                holder_positions: spec.holder_positions,
                minimum_frame_bytes: spec.minimum_frame_bytes,
                waiters: 1,
            }],
            parent,
            Some(observable.clone()),
        )?;
        let port = credit.port(0)?;
        let mut streams = Vec::new();
        streams
            .try_reserve_exact(spec.streams.len())
            .map_err(failure::allocation)?;
        for identity in spec.streams {
            streams.push(Stream {
                identity,
                next: ExchangeFrameSequence::FIRST,
                phase: Phase::Idle,
            });
        }
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(spec.queue_positions)
            .map_err(failure::allocation)?;
        let positions = pool.reserve(streams.len())?;
        let receiver = Arc::new(NetworkReceiver {
            state: Mutex::new(NetworkState {
                streams,
                queue,
                cursor: 0,
                closed: false,
                failure: None,
                consumed: false,
                stats: ExchangeRecvStats::default(),
            }),
            pool,
            positions,
            credit,
            port,
            observable,
            subscription: Mutex::new(None),
            delivery: spec.delivery,
            kind: spec.kind,
            max_wire_bytes: spec.max_wire_bytes,
            max_chunks: spec.max_chunks,
            permit_ttl: spec.permit_ttl,
            actual_jobs: AtomicUsize::new(0),
            _metadata: charge,
        });
        let weak = Arc::downgrade(&receiver);
        let subscription = receiver
            .port
            .observable()
            .try_subscribe(Arc::new(move || {
                if let Some(receiver) = weak.upgrade() {
                    for position in &receiver.positions {
                        receiver.pool.0.wake_position(*position);
                    }
                    receiver.observable.notify_observers();
                }
            }))
            .map_err(|error| ExchangeFailure::from(ExecutionFailure::from_domain(error)))?;
        *receiver
            .subscription
            .lock()
            .expect("receiver credit subscription") = Some(subscription);
        Ok(Self(receiver))
    }
    pub(crate) fn reject_local_ingress(&self) {
        self.0.fail(exchange_contract(
            "local ingress cannot bypass a native receiver permit contract",
        ));
    }
    pub(crate) fn stop(&self) {
        self.0.fail(exchange_stopped());
    }
    pub fn source_count(&self) -> usize {
        self.0.positions.len()
    }
    pub fn observable(&self) -> Arc<Observable> {
        self.0.observable.clone()
    }
    pub fn acquire(
        &self,
        request: ExchangePermitRequest,
        stop: CancellationToken,
        deadline: Instant,
    ) -> ExchangeReceiveFuture<'_, ExchangePermitAdmission> {
        Box::pin(async move {
            request
                .validate()
                .map_err(|error| exchange_contract(error))?;
            let index = self.0.begin(request)?;
            let mut guard = AcquireGuard {
                receiver: self.0.clone(),
                index,
                complete: false,
            };
            let result = tokio::select! {
                biased;
                _ = stop.cancelled() => Err(exchange_stopped()),
                _ = tokio::time::sleep_until(deadline.into()) => Err(exchange_contract("exchange permit acquisition deadline exceeded")),
                result = futures::future::poll_fn(|context| self.0.poll_acquire(index, context)) => result,
            };
            guard.complete = matches!(
                result,
                Ok(ExchangePermitAdmission::Granted(_)) | Ok(ExchangePermitAdmission::Closed(_))
            );
            result
        })
    }
    pub fn lookup_grant(
        &self,
        stream: ExchangeStreamIdentity,
        token: ExchangePermitToken,
    ) -> Result<ExchangePermitGrant, ExchangeFailure> {
        let state = self.0.state.lock().expect("receiver network lock");
        let entry = state
            .streams
            .iter()
            .find(|entry| entry.identity == stream)
            .ok_or_else(|| exchange_contract("token stream is outside frozen membership"))?;
        if !state.closed
            && let Phase::Granted {
                ticket,
                token: actual,
                expires,
            } = &entry.phase
            && *actual == token
            && *expires > Instant::now()
        {
            return Ok(ExchangePermitGrant {
                request: ticket.request,
                token,
            });
        }
        Err(exchange_contract(
            "exchange token is not a current unused grant",
        ))
    }
    pub fn claim(
        &self,
        grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, ExchangeFailure> {
        self.0.claim(grant)
    }
    pub fn close_snapshot(&self) -> Option<ExchangeReceiverCloseSnapshot> {
        let state = self.0.state.lock().expect("receiver network lock");
        if !state.closed && !state.consumed {
            return None;
        }
        drop(state);
        Some(self.0.snapshot())
    }
    pub fn withdraw(&self) -> Result<ExchangeReceiverCloseSnapshot, ExchangeFailure> {
        self.0.withdraw()
    }
}
struct AcquireGuard {
    receiver: Arc<NetworkReceiver>,
    index: usize,
    complete: bool,
}
impl Drop for AcquireGuard {
    fn drop(&mut self) {
        if !self.complete {
            let ticket = {
                let mut state = self.receiver.state.lock().expect("receiver network lock");
                match state.streams[self.index].phase {
                    Phase::Waiting(_) => Some(std::mem::replace(
                        &mut state.streams[self.index].phase,
                        Phase::Idle,
                    )),
                    _ => None,
                }
            };
            self.receiver
                .pool
                .0
                .clear(self.receiver.positions[self.index]);
            drop(ticket);
            self.receiver.pool.0.wake_all();
        }
    }
}
/// Release-only original ingress count custody. Implementations may retain
/// only their finite request-position/byte RAII owner, never a Task, Provider,
/// execution runtime, callback or runnable capability.
pub trait ExchangeTransportHold: Send + Sync {}

/// A single successful claim. Closing or expiring its receiver never releases
/// these tickets: only actual handler/codec scope exit can drop this owner.
pub struct ClaimedExchangeFrame {
    receiver: Arc<NetworkReceiver>,
    index: usize,
    ticket: Option<Ticket>,
    committed: bool,
    transport_lifetime: Option<Arc<dyn ExchangeTransportHold>>,
}
impl ClaimedExchangeFrame {
    /// Retains the original ingress request ownership through the CPU scope.
    /// This is release-only custody and grants no execution or memory authority.
    pub fn retain_transport_lifetime(
        &mut self,
        ownership: Arc<dyn ExchangeTransportHold>,
    ) -> Result<(), ExchangeFailure> {
        if self.transport_lifetime.is_some() {
            return Err(exchange_contract(
                "claimed frame already has original ingress custody",
            ));
        }
        self.transport_lifetime = Some(ownership);
        Ok(())
    }
    pub fn request(&self) -> ExchangePermitRequest {
        self.ticket.as_ref().expect("live claimed ticket").request
    }
}
/// One queued codec job owns its raw backing before its permit custody in
/// field drop order. Cancellation before execution and a failed decode both
/// destroy payload first, then return staging and the original ingress slot.
pub(crate) struct ClaimedExchangeJob {
    payload: Vec<u8>,
    claim: ClaimedExchangeFrame,
}
impl ClaimedExchangeJob {
    pub(crate) fn new(claim: ClaimedExchangeFrame, payload: Vec<u8>) -> Self {
        Self { payload, claim }
    }
    pub(crate) fn finish(
        &mut self,
        registry: &ExecutionExchangeRegistry,
    ) -> Result<(), ExchangeFailure> {
        if self.payload.capacity() as u64 > self.claim.request().geometry.wire_bytes() {
            return Err(exchange_payload(
                "raw frame backing exceeds its claimed wire bound",
            ));
        }
        registry.finish_claimed_frame_inner(&mut self.claim, &self.payload)
    }
}
impl Drop for ClaimedExchangeFrame {
    fn drop(&mut self) {
        if !self.committed {
            self.receiver.fail(exchange_contract(
                "claimed exchange frame exited before successful enqueue",
            ));
        }
        drop(self.ticket.take());
        drop(self.transport_lifetime.take());
        self.receiver.actual_jobs.fetch_sub(1, Ordering::AcqRel);
        self.receiver.observable.notify_observers();
    }
}
impl NetworkReceiver {
    fn wake_q(&self) {
        let index = {
            let state = self.state.lock().expect("receiver network lock");
            (0..state.streams.len())
                .map(|step| (state.cursor + step) % state.streams.len())
                .find(|index| {
                    matches!(
                        state.streams[*index].phase,
                        Phase::Waiting(Ticket { queue: None, .. })
                    )
                })
        };
        if let Some(index) = index {
            self.pool.0.wake_position(self.positions[index]);
        }
    }
    fn begin(&self, request: ExchangePermitRequest) -> Result<usize, ExchangeFailure> {
        let mut state = self.state.lock().expect("receiver network lock");
        let index = state
            .streams
            .iter()
            .position(|stream| stream.identity == request.stream)
            .ok_or_else(|| exchange_contract("permit stream is outside the frozen source set"))?;
        if state.closed {
            return Ok(index);
        }
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        if request.sequence != state.streams[index].next
            || !matches!(state.streams[index].phase, Phase::Idle)
        {
            return Err(exchange_contract(
                "exchange permit duplicate, gap, active request or post-End frame",
            ));
        }
        if request.kind != ExchangeFrameKind::End && request.kind != self.kind {
            return Err(exchange_contract(
                "exchange frame kind differs from frozen input role",
            ));
        }
        if request.geometry.wire_bytes() > self.max_wire_bytes {
            return Err(exchange_contract(
                "exchange frame wire extent exceeds the input hard limit",
            ));
        }
        let bytes = usize::try_from(request.geometry.decoded_bytes())
            .map_err(|_| exchange_contract("exchange queue extent exceeds host"))?;
        if bytes > self.port.max_frame_bytes() {
            return Err(ExchangeFailure::from(ExecutionFailure::from_domain(
                crate::runtime::operator_byte_credit::CreditFailure::Capacity {
                    requested: bytes,
                    limit: self.port.max_frame_bytes(),
                },
            )));
        }
        state.streams[index].phase = Phase::Waiting(Ticket {
            request,
            queue: None,
            staging: None,
        });
        Ok(index)
    }
    fn poll_acquire(
        self: &Arc<Self>,
        index: usize,
        context: &mut Context<'_>,
    ) -> Poll<Result<ExchangePermitAdmission, ExchangeFailure>> {
        self.pool.0.register(self.positions[index], context.waker());
        let mut state = self.state.lock().expect("receiver network lock");
        if state.closed {
            return Poll::Ready(Ok(ExchangePermitAdmission::Closed(ExchangeStreamClose {
                stream: state.streams[index].identity,
                disposition: if state.consumed {
                    let Phase::End(final_sequence) = state.streams[index].phase else {
                        return Poll::Ready(Err(exchange_contract("consumed stream has no End")));
                    };
                    ExchangeCloseDisposition::Consumed { final_sequence }
                } else {
                    ExchangeCloseDisposition::Withdrawn
                },
            })));
        }
        if let Some(error) = &state.failure {
            return Poll::Ready(Err(error.clone()));
        }
        let first = (0..state.streams.len())
            .map(|step| (state.cursor + step) % state.streams.len())
            .find(|index| {
                matches!(
                    state.streams[*index].phase,
                    Phase::Waiting(Ticket { queue: None, .. })
                )
            });
        let phase = std::mem::replace(&mut state.streams[index].phase, Phase::Claimed);
        let Phase::Waiting(mut ticket) = phase else {
            state.streams[index].phase = phase;
            return Poll::Ready(Err(exchange_contract(
                "permit waiter no longer owns its request",
            )));
        };
        let result = (|| {
            if ticket.request.kind != ExchangeFrameKind::End && ticket.queue.is_none() {
                if first != Some(index) {
                    return Ok(false);
                }
                match self
                    .port
                    .try_queue(ticket.request.geometry.decoded_bytes() as usize)?
                {
                    CreditAcquire::Granted(queue) => {
                        ticket.queue = Some(queue);
                        state.cursor = (index + 1) % state.streams.len();
                    }
                    CreditAcquire::Pending(_) => return Ok(false),
                    CreditAcquire::Closed => return Err(exchange_stopped()),
                }
            }
            let staging = ticket.request.geometry.staging_bytes();
            if staging != 0 && ticket.staging.is_none() {
                if !self.pool.0.want_r(self.positions[index]) {
                    return Ok(false);
                }
                match self.pool.0.staging.try_acquire(
                    usize::try_from(staging)
                        .map_err(|_| exchange_contract("exchange staging extent exceeds host"))?,
                )? {
                    StagingAcquire::Granted(permit) => {
                        ticket.staging = Some(permit);
                        self.pool.0.took_r(self.positions[index]);
                    }
                    StagingAcquire::Pending(_) => return Ok(false),
                    StagingAcquire::Closed => return Err(exchange_stopped()),
                }
            }
            Ok(true)
        })();
        match result {
            Ok(true) => {
                let token = ExchangePermitToken::try_from_bytes(*uuid::Uuid::new_v4().as_bytes())
                    .expect("UUID is nonzero");
                let expires = Instant::now()
                    .checked_add(self.permit_ttl)
                    .unwrap_or(Instant::now());
                let grant = ExchangePermitGrant {
                    request: ticket.request,
                    token,
                };
                state.streams[index].phase = Phase::Granted {
                    ticket,
                    token,
                    expires,
                };
                self.pool.0.state.lock().expect("receiver pool lock").slots
                    [self.positions[index]]
                    .expiry = Some((expires, Arc::downgrade(self), index, token));
                drop(state);
                self.pool.0.expiry_changed.notify_one();
                self.wake_q();
                self.pool.0.wake_r();
                Poll::Ready(Ok(ExchangePermitAdmission::Granted(grant)))
            }
            Ok(false) => {
                state.streams[index].phase = Phase::Waiting(ticket);
                Poll::Pending
            }
            Err(error) => {
                state.streams[index].phase = Phase::Idle;
                drop(state);
                drop(ticket);
                self.pool.0.clear(self.positions[index]);
                Poll::Ready(Err(error))
            }
        }
    }
    fn claim(
        self: &Arc<Self>,
        grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, ExchangeFailure> {
        let mut state = self.state.lock().expect("receiver network lock");
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        let index = state
            .streams
            .iter()
            .position(|stream| stream.identity == grant.request.stream)
            .ok_or_else(|| exchange_contract("token stream is outside frozen membership"))?;
        let valid = !state.closed
            && matches!(&state.streams[index].phase, Phase::Granted { ticket, token, expires } if ticket.request == grant.request && *token == grant.token && *expires > Instant::now());
        if !valid {
            return Err(exchange_contract(
                "exchange token is expired, reused, revoked or bound to another request",
            ));
        }
        let Phase::Granted { ticket, .. } =
            std::mem::replace(&mut state.streams[index].phase, Phase::Claimed)
        else {
            unreachable!()
        };
        self.actual_jobs.fetch_add(1, Ordering::AcqRel);
        drop(state);
        self.pool.0.clear(self.positions[index]);
        Ok(ClaimedExchangeFrame {
            receiver: self.clone(),
            index,
            ticket: Some(ticket),
            committed: false,
            transport_lifetime: None,
        })
    }
    fn expire(&self, index: usize, token: ExchangePermitToken) {
        let expired = {
            let mut state = self.state.lock().expect("receiver network lock");
            if matches!(&state.streams[index].phase, Phase::Granted { token: current, .. } if *current == token)
            {
                Some(std::mem::replace(
                    &mut state.streams[index].phase,
                    Phase::Idle,
                ))
            } else {
                None
            }
        };
        drop(expired);
        self.pool.0.wake_all();
    }
    fn fail(&self, error: ExchangeFailure) {
        self.close_with_failure(Some(error));
    }
    fn close_with_failure(&self, error: Option<ExchangeFailure>) {
        let (count, queue) = {
            let mut state = self.state.lock().expect("receiver network lock");
            if state.failure.is_none() {
                state.failure = error;
            }
            state.closed = true;
            (state.streams.len(), std::mem::take(&mut state.queue))
        };
        // Detach then destroy outside locks. Claimed tickets stay with their
        // actual jobs; close only revokes requests which have not been claimed.
        drop(queue);
        for index in 0..count {
            let phase = {
                let mut state = self.state.lock().expect("receiver network lock");
                if matches!(
                    state.streams[index].phase,
                    Phase::Waiting(_) | Phase::Granted { .. }
                ) {
                    Some(std::mem::replace(
                        &mut state.streams[index].phase,
                        Phase::Closed,
                    ))
                } else {
                    None
                }
            };
            self.pool.0.clear(self.positions[index]);
            drop(phase);
        }
        self.credit.close();
        self.pool.0.wake_all();
        self.observable.notify_observers();
    }
    fn snapshot(&self) -> ExchangeReceiverCloseSnapshot {
        let state = self.state.lock().expect("receiver network lock");
        ExchangeReceiverCloseSnapshot {
            streams: state
                .streams
                .iter()
                .map(|stream| ExchangeStreamClose {
                    stream: stream.identity,
                    disposition: if state.consumed {
                        if let Phase::End(final_sequence) = stream.phase {
                            ExchangeCloseDisposition::Consumed { final_sequence }
                        } else {
                            ExchangeCloseDisposition::Withdrawn
                        }
                    } else {
                        ExchangeCloseDisposition::Withdrawn
                    },
                })
                .collect(),
            active_acquires: state
                .streams
                .iter()
                .filter(|stream| matches!(stream.phase, Phase::Waiting(_)))
                .count(),
            unused_grants: state
                .streams
                .iter()
                .filter(|stream| matches!(stream.phase, Phase::Granted { .. }))
                .count(),
            actual_jobs: self.actual_jobs.load(Ordering::Acquire),
        }
    }
    fn withdraw(&self) -> Result<ExchangeReceiverCloseSnapshot, ExchangeFailure> {
        let consumed = self.state.lock().expect("receiver network lock").consumed;
        if !consumed {
            self.close_with_failure(
                (self.delivery == ExchangeDeliveryRequirement::MustDrain).then(|| {
                    exchange_contract(
                        "MustDrain input was withdrawn before actual Source exhaustion",
                    )
                }),
            );
        }
        let snapshot = self.snapshot();
        if !consumed && self.delivery == ExchangeDeliveryRequirement::MustDrain {
            return Err(exchange_contract(
                "MustDrain input was withdrawn before actual Source exhaustion",
            ));
        }
        Ok(snapshot)
    }
}
impl Drop for NetworkReceiver {
    fn drop(&mut self) {
        for index in &self.positions {
            self.pool.0.clear(*index);
            self.pool.0.state.lock().expect("receiver pool lock").slots[*index].reserved = false;
        }
        self.pool.0.wake_all();
    }
}

impl ExecutionExchangeRegistry {
    pub fn register_network_receiver(
        &self,
        key: ExchangeKey,
        contract: ExchangeReceiverNetworkContract,
    ) -> Result<(), ExchangeFailure> {
        let receiver = get_or_create(self, key);
        let mut state = receiver.mu.lock().expect("exchange receiver lock");
        let network = &contract.0;
        let network_state = network.state.lock().expect("receiver network lock");
        let first = network_state.streams[0].identity;
        if first.destination_fragment_instance_id()
            != novarocks_types::UniqueId::new(key.finst_id_hi, key.finst_id_lo)
            || first.destination_node().get() != key.node_id
            || state.network.is_some()
            || !state.chunks.is_empty()
            || !state.finished.is_empty()
        {
            return Err(exchange_contract(
                "network registration differs from exact receiver or follows ingress",
            ));
        }
        if state.expected.is_none() || state.expected_senders != network_state.streams.len() {
            return Err(exchange_contract(
                "network registration differs from frozen schema/source count",
            ));
        }
        drop(network_state);
        state.network = Some(contract);
        Ok(())
    }
    pub fn network_receiver(
        &self,
        key: ExchangeKey,
    ) -> Result<ExchangeReceiverNetworkContract, ExchangeFailure> {
        let receiver = self
            .receivers
            .lock()
            .expect("exchange registry lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| exchange_contract("network receiver is not registered"))?;
        let contract = receiver.mu.lock().expect("exchange receiver lock").network.clone().ok_or_else(|| exchange_contract("native receiver has no permit contract; local profile grants no network admission"))?;
        Ok(contract)
    }
    pub fn claim_exchange_frame(
        &self,
        grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, ExchangeFailure> {
        let key = ExchangeKey {
            finst_id_hi: grant
                .request
                .stream
                .destination_fragment_instance_id()
                .high(),
            finst_id_lo: grant
                .request
                .stream
                .destination_fragment_instance_id()
                .low(),
            node_id: grant.request.stream.destination_node().get(),
        };
        self.network_receiver(key)?.claim(grant)
    }
    /// Called only by a bounded CPU job. This routine never waits for queue,
    /// staging, downstream consumption or a network operation.
    pub fn finish_claimed_frame(
        &self,
        claim: ClaimedExchangeFrame,
        payload: Vec<u8>,
    ) -> Result<(), ExchangeFailure> {
        let mut job = ClaimedExchangeJob::new(claim, payload);
        job.finish(self)
    }
    fn finish_claimed_frame_inner(
        &self,
        claim: &mut ClaimedExchangeFrame,
        payload: &[u8],
    ) -> Result<(), ExchangeFailure> {
        let request = claim.request();
        if payload.len() as u64 > request.geometry.wire_bytes() {
            return Err(exchange_payload(
                "raw frame backing exceeds its claimed wire bound",
            ));
        }
        let receiver = claim.receiver.clone();
        let chunks = if request.kind == ExchangeFrameKind::End {
            if !payload.is_empty() {
                return Err(exchange_payload("End carries payload"));
            }
            Vec::new()
        } else {
            let key = ExchangeKey {
                finst_id_hi: request.stream.destination_fragment_instance_id().high(),
                finst_id_lo: request.stream.destination_fragment_instance_id().low(),
                node_id: request.stream.destination_node().get(),
            };
            let declaration = self
                .receivers
                .lock()
                .expect("exchange registry lock")
                .get(&key)
                .cloned()
                .ok_or_else(|| exchange_contract("claimed receiver is no longer registered"))?;
            let expected = declaration
                .mu
                .lock()
                .expect("exchange receiver lock")
                .expected
                .clone()
                .ok_or_else(|| exchange_contract("claimed receiver lost its frozen schema"))?;
            let actual = super::result_ipc_preflight::preflight_network_exchange_decode(
                &payload,
                request.geometry,
                receiver.max_chunks,
                expected.chunk_schema.arrow_schema_ref().as_ref(),
            )?;
            if actual.wire_bytes() > request.geometry.wire_bytes()
                || actual.decoded_bytes() > request.geometry.decoded_bytes()
                || actual.decode_scratch_bytes() > request.geometry.decode_scratch_bytes()
            {
                return Err(exchange_payload(
                    "actual IPC geometry exceeds its claimed queue/staging bounds",
                ));
            }
            let key = ExchangeKey {
                finst_id_hi: request.stream.destination_fragment_instance_id().high(),
                finst_id_lo: request.stream.destination_fragment_instance_id().low(),
                node_id: request.stream.destination_node().get(),
            };
            let sender = ExchangeSenderIdentity::native(
                request.stream.source().fragment_instance_id(),
                request.stream.source().sender_ordinal(),
            );
            self.decode_chunks_for_sender(key, sender, &payload)?
        };
        if chunks.len() > receiver.max_chunks {
            return Err(exchange_payload(
                "decoded frame exceeds its admitted chunk count",
            ));
        }
        let mut backing = 0usize;
        for chunk in &chunks {
            backing = backing
                .checked_add(chunk.retained_operator_geometry(
                    crate::exec::chunk::OperatorStorageLimits {
                        bytes: request.geometry.decoded_bytes() as usize,
                        nodes: 65_536,
                        depth: 128,
                        columns: 4_096,
                    },
                )?)
                .ok_or_else(|| exchange_payload("decoded retained backing extent overflow"))?;
        }
        if backing as u64 > request.geometry.decoded_bytes() {
            return Err(exchange_payload(
                "decoded retained backing exceeds claimed Q",
            ));
        }
        let ticket = claim.ticket.as_mut().expect("claimed frame ticket");
        let mut state = receiver.state.lock().expect("receiver network lock");
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        if state.closed
            || !matches!(state.streams[claim.index].phase, Phase::Claimed)
            || state.streams[claim.index].next != request.sequence
        {
            return Err(exchange_contract(
                "claimed frame lost exact stream admission",
            ));
        }
        let next = if request.kind == ExchangeFrameKind::End {
            request.sequence
        } else {
            request.sequence.checked_next().map_err(exchange_contract)?
        };
        // Capacity and positions were reserved before granting the token.
        if !chunks.is_empty() {
            if state.queue.len() == state.queue.capacity() {
                return Err(exchange_contract(
                    "pre-admitted network queue position is unavailable",
                ));
            }
            let rows = chunks.iter().map(Chunk::len).sum::<usize>();
            state.stats.chunks_received += chunks.len() as u128;
            state.stats.rows_received += rows as u128;
            state.queue.push_back(QueuedFrame {
                provenance: ExchangeFrameProvenance {
                    source: request.stream.source(),
                    sequence: request.sequence,
                    kind: request.kind,
                },
                chunks: chunks.into(),
                queue: ticket.queue.take(),
                holder: None,
                holder_wait: None,
            });
        }
        state.stats.request_received += 1;
        state.stats.bytes_received += payload.len() as u128;
        state.streams[claim.index].next = next;
        state.streams[claim.index].phase = if request.kind == ExchangeFrameKind::End {
            Phase::End(request.sequence)
        } else {
            Phase::Idle
        };
        claim.committed = true;
        drop(state);
        // The job retains staging, original ingress and actual-job debt until
        // its raw backing is destroyed after the CPU result is published.
        receiver.observable.notify_observers();
        Ok(())
    }
}
impl NetworkReceiver {
    pub(super) fn snapshot_receiver(&self) -> ExchangeReceiverSnapshot {
        let state = self.state.lock().expect("receiver network lock");
        ExchangeReceiverSnapshot {
            expected_senders: state.streams.len(),
            finished_senders: state
                .streams
                .iter()
                .filter(|stream| matches!(stream.phase, Phase::End(_)))
                .count(),
            queued_chunks: state.queue.iter().map(|frame| frame.chunks.len()).sum(),
            queued_rows: state
                .queue
                .iter()
                .flat_map(|frame| frame.chunks.iter())
                .map(Chunk::len)
                .sum(),
            canceled: state.failure.is_some(),
        }
    }
    pub(super) fn ready(&self) -> bool {
        let state = self.state.lock().expect("receiver network lock");
        if state.closed || state.failure.is_some() {
            return true;
        }
        if let Some(frame) = state.queue.front() {
            return frame
                .holder_wait
                .as_ref()
                .is_none_or(|(observable, generation)| observable.generation() != *generation);
        }
        state
            .streams
            .iter()
            .all(|stream| matches!(stream.phase, Phase::End(_)))
            && self.actual_jobs.load(Ordering::Acquire) == 0
    }
    pub(super) fn pop(&self) -> Result<Option<ExchangeSourcedPopResult>, ExchangeFailure> {
        let mut state = self.state.lock().expect("receiver network lock");
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        if let Some(front) = state.queue.front_mut() {
            if let Some(queue) = front.queue.take() {
                match queue.try_into_holder() {
                    HolderAcquire::Granted(holder) => {
                        front.holder = Some(holder.into_storage_lifetime());
                        front.holder_wait = None;
                    }
                    HolderAcquire::Pending { queue, wait } => {
                        front.queue = Some(queue);
                        front.holder_wait = Some((wait.observable, wait.observed_generation));
                        return Ok(None);
                    }
                    HolderAcquire::Closed(queue) => {
                        front.queue = Some(queue);
                        return Err(exchange_stopped());
                    }
                }
            }
            let mut chunk = front
                .chunks
                .pop_front()
                .ok_or_else(|| exchange_contract("queued frame has no chunk"))?;
            let provenance = front.provenance;
            let holder = front
                .holder
                .as_ref()
                .ok_or_else(|| exchange_contract("dequeued frame has no prepaid holder"))?
                .clone();
            chunk.retain_operator_holder::<ExchangeFailure>(holder, |_| Ok(()))?;
            let retired = if front.chunks.is_empty() {
                state.queue.pop_front()
            } else {
                None
            };
            drop(state);
            drop(retired);
            return Ok(Some(ExchangeSourcedPopResult::Chunk(
                ExchangeSourcedChunk { chunk, provenance },
            )));
        }
        if state
            .streams
            .iter()
            .all(|stream| matches!(stream.phase, Phase::End(_)))
            && self.actual_jobs.load(Ordering::Acquire) == 0
        {
            // Only this actual Source poll authors Consumed. End acceptance and
            // ordinary ACK never call this branch.
            state.consumed = true;
            state.closed = true;
            return Ok(Some(ExchangeSourcedPopResult::Finished(
                state.stats.clone(),
            )));
        }
        Ok(None)
    }
}
impl ExchangeReceiverHandle {
    pub fn withdraw_network_input(
        &self,
    ) -> Result<Option<ExchangeReceiverCloseSnapshot>, ExchangeFailure> {
        let network = self
            .receiver
            .mu
            .lock()
            .expect("exchange receiver lock")
            .network
            .clone();
        network.map(|network| network.withdraw()).transpose()
    }

    pub fn try_pop_next_sourced(
        &self,
    ) -> Result<Option<ExchangeSourcedPopResult>, ExchangeFailure> {
        let network = self
            .receiver
            .mu
            .lock()
            .expect("exchange receiver lock")
            .network
            .clone()
            .ok_or_else(|| exchange_contract("source-bound pop requires a frozen network input"))?;
        network.0.pop()
    }
    pub fn network_close_snapshot(&self) -> Option<ExchangeReceiverCloseSnapshot> {
        let network = self
            .receiver
            .mu
            .lock()
            .expect("exchange receiver lock")
            .network
            .clone()?;
        let state = network.0.state.lock().expect("receiver network lock");
        if !state.closed && !state.consumed {
            return None;
        }
        drop(state);
        Some(network.0.snapshot())
    }
}

impl ExchangeFailure {
    pub(crate) fn metadata_channel(error: tokio::sync::oneshot::error::RecvError) -> Self {
        failure::metadata("receiver CPU result channel", error)
    }
    pub(crate) fn unpermitted_native_frame() -> Self {
        exchange_contract("native exchange frame requires a receiver-owned single-use permit")
    }
}

#[cfg(test)]
#[path = "network_receiver_tests.rs"]
mod tests;
