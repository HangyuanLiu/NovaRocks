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

//! One context-owned root stream. Task retirement does not close this owner.
//! Logical ACKs remove queue entries; a Bytes backing keeps its retained-pool
//! credit and physical position until its last real alias is destroyed.

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{
    ResultWriteAdmission, ResultWriteCredit, RootProducerState, RootResultWriteSpec,
};
use novarocks_execution::runtime::observable::{Observable, ObserverSubscription};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_execution_contract::ordered_retained_stream::{
    OfferOutcome, OrderedRetainedStream, RetainedItemKind, RetainedStreamSnapshot,
    RetainedStreamWindow, StreamError,
};
use novarocks_execution_contract::root_lifetime::{RootResultLifetime, RootRetentionClose};
use novarocks_execution_contract::root_result::{
    RootReadOutcome, RootResultData, RootResultEnd, RootResultRead, RootResultReply,
};
use novarocks_result_contract::{RootOutputKind, RootProfileV1};
use tokio::sync::Notify;

use crate::result_buffer::{ResultBufferKey, ResultRetainedBudget};
use crate::{TaskStatusSource, WorkerResultRetainedLimits};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootChannelError {
    Identity,
    Closed,
    Capacity,
    Credit,
    Producer,
    Payload,
    RowCount,
    Stream(StreamError),
}
pub enum ContextRootRoute {
    Read(RootChannelRead),
    AwaitTerminalControl { accepted_consumed: u64 },
    Preparing,
    UnknownRoot,
    Mismatch,
    Busy,
}
impl std::fmt::Display for RootChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(error) => error.fmt(f),
            _ => write!(f, "bounded root channel rejected operation: {self:?}"),
        }
    }
}
impl std::error::Error for RootChannelError {}
impl From<StreamError> for RootChannelError {
    fn from(error: StreamError) -> Self {
        Self::Stream(error)
    }
}

struct PhysicalOwners {
    segments: AtomicUsize,
    deliveries: AtomicUsize,
    retained_reservations: AtomicUsize,
    fixed_metadata_bytes: AtomicUsize,
    fixed_metadata_holders: AtomicUsize,
    changed: Notify,
    writable: Arc<Observable>,
    progress: OnceLock<Weak<TaskStatusSource>>,
    // Fixed metadata follows all physical aliases, including End-only sends.
    _fixed_credit: ResultWriteCredit,
    // The original wallet itself also outlives the final physical alias.
    _budget_owner: Arc<ResultRetainedBudget>,
}

/// A structural sub-allocation of the already charged fixed root envelope.
/// This issues no allocator wallet or MEM funding; hosts check the real
/// capacity of their finite driver/producer metadata before constructing it.
#[must_use = "retain root metadata coverage through the actual owner's exit"]
pub struct RootMetadataReservation {
    bytes: usize,
    owner: Arc<PhysicalOwners>,
}
impl Drop for RootMetadataReservation {
    fn drop(&mut self) {
        self.owner
            .fixed_metadata_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.owner
            .fixed_metadata_holders
            .fetch_sub(1, Ordering::AcqRel);
        self.owner.wake();
    }
}
impl PhysicalOwners {
    fn wake(&self) {
        self.changed.notify_waiters();
        if let Some(source) = self.progress.get().and_then(Weak::upgrade) {
            source.note_progress();
        }
        // Context release progress must not depend on callback success.
        self.writable.notify_observers();
    }
}
struct PhysicalSegmentPosition(Arc<PhysicalOwners>);
struct PhysicalReservationPosition(Arc<PhysicalOwners>, bool);
impl Drop for PhysicalReservationPosition {
    fn drop(&mut self) {
        self.0.retained_reservations.fetch_sub(1, Ordering::AcqRel);
        if self.1 {
            self.0.wake();
        } else if let Some(source) = self.0.progress.get().and_then(Weak::upgrade) {
            // An unsuccessful tentative admission returned no usable
            // resource. It must not invalidate a producer's blocked-credit
            // generation and cause an endless retry/notification loop.
            source.note_progress();
        }
    }
}
struct RetainedReservationOwner {
    credit: Mutex<ResultWriteCredit>,
    _position: PhysicalReservationPosition,
}
impl RetainedReservationOwner {
    fn release(&self, released: usize) {
        let mut credit = self.credit.lock().unwrap();
        let retained = credit
            .bytes()
            .checked_sub(released)
            .expect("root credit releases its own bytes");
        credit
            .shrink_to(retained)
            .expect("root credit only shrinks");
    }
}
impl Drop for PhysicalSegmentPosition {
    fn drop(&mut self) {
        self.0.segments.fetch_sub(1, Ordering::AcqRel);
        self.0.wake();
    }
}

/// Owns capacity before allocating an unpublished segment. There is one
/// active builder; queued and retired backing positions are independently
/// bounded. Publishing transfers this very allocation without a copy.
pub struct RootSegmentBuilder {
    // The backing is destroyed before its credit and physical position.
    backing: Vec<u8>,
    _credit: ResultWriteCredit,
    position: PhysicalSegmentPosition,
    active: ActiveBuilderPosition,
}
impl RootSegmentBuilder {
    pub fn output(&mut self) -> &mut [u8] {
        self.output_at(0)
    }
    /// Initialize no more than one encoding quantum. Reserving a segment
    /// must not perform a whole-megabyte zero-fill in the driver's turn.
    pub fn output_at(&mut self, offset: usize) -> &mut [u8] {
        assert!(offset <= self.backing.len() && offset <= RootProfileV1::SEGMENT_BYTES);
        let end = (offset + RootProfileV1::EMIT_BYTES_PER_TURN).min(RootProfileV1::SEGMENT_BYTES);
        if self.backing.len() < end {
            self.backing.resize(end, 0);
        }
        &mut self.backing[offset..end]
    }
    pub fn capacity_bytes(&self) -> usize {
        self.backing.capacity()
    }
    fn into_body(mut self, bytes: usize) -> Bytes {
        self.backing.truncate(bytes);
        Bytes::from_owner(self)
    }
}
impl AsRef<[u8]> for RootSegmentBuilder {
    fn as_ref(&self) -> &[u8] {
        &self.backing
    }
}
struct ActiveBuilderPosition(Option<Weak<RootResultChannel>>);
impl Drop for ActiveBuilderPosition {
    fn drop(&mut self) {
        if let Some(channel) = self.0.take().and_then(|channel| channel.upgrade()) {
            channel.state.lock().unwrap().active_builder = false;
            channel.physical.wake();
        }
    }
}

struct ChannelState {
    stream: OrderedRetainedStream,
    payloads: VecDeque<RootResultData>,
    retiring_payloads: Option<VecDeque<RootResultData>>,
    lifetime: RootResultLifetime,
    context_owned: bool,
    producer_started: bool,
    producer_exited: bool,
    finishing: bool,
    failed: Option<RootChannelError>,
    output_rows: u64,
    active_builder: bool,
}

pub struct RootResultChannel {
    spec: RootResultWriteSpec,
    budget: Arc<ResultRetainedBudget>,
    limits: WorkerResultRetainedLimits,
    physical: Arc<PhysicalOwners>,
    state: Mutex<ChannelState>,
    _budget_wake: ObserverSubscription,
}
impl RootResultChannel {
    // This is part of the fixed 1MiB grant, not an additional charge. The
    // concrete core types/queue capacities are checked against it at open.
    const CORE_METADATA_CAPACITY: usize = RootProfileV1::ENVELOPE_BYTES;
    // One installed producer bridge and one scoped host observer. Driver
    // readiness lives in the separate RootInputAuthority registry.
    const WRITABLE_CALLBACK_CAPACITY: usize = 2;
    pub fn try_open(
        spec: RootResultWriteSpec,
        budget: Arc<ResultRetainedBudget>,
        limits: WorkerResultRetainedLimits,
    ) -> Result<Arc<Self>, RootChannelError> {
        let fixed =
            NativeResultSupportGeometry::V1.root_fixed_schema_cursor_driver_capacity_bytes as usize;
        let schema_bytes = match spec.contract.output() {
            novarocks_result_contract::FrozenRootOutput::ClientRows(schema) => {
                schema.backing_bytes()
            }
            _ => 0,
        };
        let metadata_bytes = schema_bytes
            .checked_add(Self::CORE_METADATA_CAPACITY)
            .filter(|bytes| *bytes <= fixed)
            .ok_or(RootChannelError::Capacity)?;
        let credit = match budget
            .try_reserve(
                ResultBufferKey::Task(spec.task),
                limits.per_root().get(),
                fixed,
            )
            .map_err(|_| RootChannelError::Credit)?
        {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => return Err(RootChannelError::Capacity),
        };
        let stream = OrderedRetainedStream::try_new(RetainedStreamWindow::try_new(
            RootProfileV1::DATA_POSITIONS,
            RootProfileV1::DATA_POSITIONS * RootProfileV1::SEGMENT_BYTES,
        )?)?;
        let mut payloads = VecDeque::new();
        payloads
            .try_reserve_exact(RootProfileV1::DATA_POSITIONS)
            .map_err(|_| RootChannelError::Capacity)?;
        // Core object layouts, queue spare and callback/credit Arc headers.
        // Shared process registries remain owned by the retained-budget
        // composition; this root's slots and temporary callback references
        // are conservatively covered here, before the root is accepted.
        let core_bytes = std::mem::size_of::<Self>()
            + std::mem::size_of::<PhysicalOwners>()
            + std::mem::size_of::<novarocks_result_contract::RootOutputContract>()
            + std::mem::size_of::<Observable>()
            + stream.metadata_backing_bytes()
            + payloads.capacity() * std::mem::size_of::<RootResultData>()
            + std::mem::size_of::<ResultBufferKey>()
            + std::mem::size_of::<Weak<ResultRetainedBudget>>()
            + Observable::bounded_backing_bytes(Self::WRITABLE_CALLBACK_CAPACITY)
                .map_err(|_| RootChannelError::Capacity)?
            + 32 * std::mem::size_of::<usize>();
        if core_bytes > Self::CORE_METADATA_CAPACITY {
            return Err(RootChannelError::Capacity);
        }
        let physical = Arc::new(PhysicalOwners {
            segments: AtomicUsize::new(0),
            deliveries: AtomicUsize::new(0),
            retained_reservations: AtomicUsize::new(0),
            fixed_metadata_bytes: AtomicUsize::new(metadata_bytes),
            fixed_metadata_holders: AtomicUsize::new(0),
            changed: Notify::new(),
            writable: Arc::new(
                Observable::try_bounded(Self::WRITABLE_CALLBACK_CAPACITY)
                    .map_err(|_| RootChannelError::Capacity)?,
            ),
            progress: OnceLock::new(),
            _fixed_credit: credit,
            _budget_owner: Arc::clone(&budget),
        });
        let weak = Arc::downgrade(&physical);
        let budget_wake = budget.writable_observable().subscribe(Arc::new(move || {
            if let Some(physical) = weak.upgrade() {
                physical.wake();
            }
        }));
        Ok(Arc::new(Self {
            state: Mutex::new(ChannelState {
                lifetime: RootResultLifetime::new(spec.task),
                stream,
                payloads,
                retiring_payloads: None,
                context_owned: false,
                producer_started: false,
                producer_exited: false,
                finishing: false,
                failed: None,
                output_rows: 0,
                active_builder: false,
            }),
            spec,
            budget,
            limits,
            physical,
            _budget_wake: budget_wake,
        }))
    }
    pub fn spec(&self) -> &RootResultWriteSpec {
        &self.spec
    }
    pub fn try_reserve_metadata(
        &self,
        bytes: usize,
    ) -> Result<RootMetadataReservation, RootChannelError> {
        let state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        if bytes < std::mem::size_of::<RootMetadataReservation>() {
            return Err(RootChannelError::Capacity);
        }
        let geometry = NativeResultSupportGeometry::V1;
        let positions =
            (geometry.root_maximum_root_drivers + geometry.root_live_send_holders + 1) as usize;
        self.physical
            .fixed_metadata_holders
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < positions).then_some(n + 1)
            })
            .map_err(|_| RootChannelError::Capacity)?;
        let limit = geometry.root_fixed_schema_cursor_driver_capacity_bytes as usize;
        if self
            .physical
            .fixed_metadata_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= limit)
            })
            .is_err()
        {
            self.physical
                .fixed_metadata_holders
                .fetch_sub(1, Ordering::AcqRel);
            return Err(RootChannelError::Capacity);
        }
        drop(state);
        let reservation = RootMetadataReservation {
            bytes,
            owner: Arc::clone(&self.physical),
        };
        Ok(reservation)
    }
    pub fn writable_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.physical.writable)
    }
    pub fn try_reserve(&self, bytes: usize) -> Result<ResultWriteAdmission, RootChannelError> {
        if !self.state.lock().unwrap().lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        let geometry = NativeResultSupportGeometry::V1;
        // One scratch owner, one input and the exact independent send copies.
        // Small reservations cannot grow an unbounded list of credit objects.
        let positions = (2 + geometry.root_live_send_holders
            * geometry.root_independent_payload_copies_per_send) as usize;
        if self
            .physical
            .retained_reservations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < positions).then_some(n + 1)
            })
            .is_err()
        {
            return Ok(ResultWriteAdmission::Blocked);
        }
        let mut position = PhysicalReservationPosition(Arc::clone(&self.physical), false);
        let granted = self
            .budget
            .try_reserve(
                ResultBufferKey::Task(self.spec.task),
                self.limits.per_root().get(),
                bytes,
            )
            .map_err(|_| RootChannelError::Credit)?;
        let ResultWriteAdmission::Granted(credit) = granted else {
            return Ok(ResultWriteAdmission::Blocked);
        };
        if !self.state.lock().unwrap().lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        position.1 = true;
        let owner = RetainedReservationOwner {
            credit: Mutex::new(credit),
            _position: position,
        };
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            move |released| owner.release(released),
        )))
    }
    /// Only the context's registry binds its original progress source. This
    /// callback acquires no registry lock and cannot reopen a closed route.
    pub fn bind_context_progress(
        &self,
        source: &Arc<TaskStatusSource>,
    ) -> Result<(), RootChannelError> {
        if let Some(bound) = self.physical.progress.get() {
            if bound.ptr_eq(&Arc::downgrade(source)) {
                return Ok(());
            }
            return Err(RootChannelError::Identity);
        }
        self.physical
            .progress
            .set(Arc::downgrade(source))
            .map_err(|_| RootChannelError::Identity)
    }
    /// Called by the creation transaction after the exact context takes this
    /// channel. A provisional preparation channel is never a readable route.
    pub fn mark_context_owned(&self) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        state.context_owned = true;
        Self::try_handoff(&mut state);
        Ok(())
    }
    pub fn start_producer(self: &Arc<Self>) -> Result<RootProducerExit, RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if state.producer_started || !state.lifetime.allows_read() {
            return Err(RootChannelError::Producer);
        }
        state.producer_started = true;
        Ok(RootProducerExit {
            channel: Arc::clone(self),
        })
    }
    pub fn note_rows(&self, rows: u64) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() || state.finishing {
            return Err(RootChannelError::Closed);
        }
        let Some(total) = state.output_rows.checked_add(rows) else {
            state.failed = Some(RootChannelError::RowCount);
            return Err(RootChannelError::RowCount);
        };
        state.output_rows = total;
        Ok(())
    }
    pub fn request_finish(&self) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        state.finishing = true;
        drop(state);
        self.physical.wake();
        Ok(())
    }
    pub fn is_closed(&self) -> bool {
        !self.state.lock().unwrap().lifetime.allows_read()
    }
    pub fn producer_state(&self) -> RootProducerState {
        let state = self.state.lock().unwrap();
        if let Some(error) = state.failed {
            return RootProducerState::Failed(error.to_string());
        }
        if state.lifetime.root_finished() {
            RootProducerState::ContextHeld
        } else if state.finishing {
            RootProducerState::Finishing
        } else {
            RootProducerState::Accepting
        }
    }
    pub fn try_segment(self: &Arc<Self>) -> Result<Option<RootSegmentBuilder>, RootChannelError> {
        self.try_segment_allocating(|backing, capacity| {
            backing
                .try_reserve_exact(capacity)
                .map_err(|_| RootChannelError::Capacity)
        })
    }
    fn try_segment_allocating(
        self: &Arc<Self>,
        allocate: impl FnOnce(&mut Vec<u8>, usize) -> Result<(), RootChannelError>,
    ) -> Result<Option<RootSegmentBuilder>, RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if self.spec.contract.kind() == RootOutputKind::CountOnly {
            return Err(RootChannelError::Payload);
        }
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        if state.failed.is_some() || state.lifetime.end().is_some() {
            return Err(RootChannelError::Producer);
        }
        if state.active_builder
            || state.stream.snapshot().data_positions == RootProfileV1::DATA_POSITIONS
        {
            return Ok(None);
        }
        let geometry = NativeResultSupportGeometry::V1;
        let limit = (geometry.root_active_segment_positions
            + geometry.root_queued_segment_positions
            + geometry.root_retired_segment_tail_positions) as usize;
        if self.physical.segments.load(Ordering::Acquire) >= limit {
            return Ok(None);
        }
        let capacity = geometry.root_segment_backing_capacity_bytes as usize;
        let credit = match self
            .budget
            .try_reserve(
                ResultBufferKey::Task(self.spec.task),
                self.limits.per_root().get(),
                capacity,
            )
            .map_err(|_| RootChannelError::Credit)?
        {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => return Ok(None),
        };
        self.physical.segments.fetch_add(1, Ordering::AcqRel);
        state.active_builder = true;
        drop(state);
        // Allocation failure destroys real owners and invokes readiness
        // callbacks. No such callback may run under the channel mutex.
        let mut builder = RootSegmentBuilder {
            backing: Vec::new(),
            _credit: credit,
            position: PhysicalSegmentPosition(Arc::clone(&self.physical)),
            active: ActiveBuilderPosition(Some(Arc::downgrade(self))),
        };
        allocate(&mut builder.backing, capacity)?;
        if builder.backing.capacity() > builder._credit.bytes() {
            return Err(RootChannelError::Capacity);
        }
        Ok(Some(builder))
    }
    /// Publication consumes the sole builder. Data(+End) is immutable; a
    /// later finish publishes a separate End instead of changing old Data.
    pub fn publish_segment(
        &self,
        mut builder: RootSegmentBuilder,
        bytes: usize,
        end: bool,
    ) -> Result<(), RootChannelError> {
        if !Arc::ptr_eq(&builder.position.0, &self.physical) {
            return Err(RootChannelError::Identity);
        }
        let mut state = self.state.lock().unwrap();
        if !state.active_builder {
            return Err(RootChannelError::Producer);
        }
        state.active_builder = false;
        builder.active.0 = None;
        if state.failed.is_some() {
            return Err(RootChannelError::Producer);
        }
        if bytes == 0
            || bytes > builder.backing.len()
            || bytes > RootProfileV1::SEGMENT_BYTES
            || self.spec.contract.kind() == RootOutputKind::CountOnly
        {
            return Err(RootChannelError::Payload);
        }
        if end && !state.finishing {
            return Err(RootChannelError::Producer);
        }
        let published = state.stream.publish_data(bytes, end)?;
        let terminal = published.end_sequence.map(|sequence| RootResultEnd {
            sequence: NonZeroU64::new(sequence).unwrap(),
            output_rows: state.output_rows,
        });
        let data = RootResultData::try_new(
            self.spec.contract.kind(),
            NonZeroU64::new(published.data_sequence.unwrap()).unwrap(),
            builder.into_body(bytes),
            terminal,
        )
        .map_err(|_| RootChannelError::Payload)?;
        state.payloads.push_back(data);
        if let Some(terminal) = terminal {
            state
                .lifetime
                .end_published(terminal)
                .map_err(|_| RootChannelError::Producer)?;
        }
        drop(state);
        self.physical.wake();
        Ok(())
    }
    pub fn publish_end(&self) -> Result<(), RootChannelError> {
        let mut state = self.state.lock().unwrap();
        if !state.finishing || state.active_builder || state.failed.is_some() {
            return Err(RootChannelError::Producer);
        }
        let published = state.stream.publish_end()?;
        let end = RootResultEnd {
            sequence: NonZeroU64::new(published.end_sequence.unwrap()).unwrap(),
            output_rows: state.output_rows,
        };
        state
            .lifetime
            .end_published(end)
            .map_err(|_| RootChannelError::Producer)?;
        drop(state);
        self.physical.wake();
        Ok(())
    }
    pub fn snapshot(&self) -> RetainedStreamSnapshot {
        self.state.lock().unwrap().stream.snapshot()
    }
    pub fn physical_idle(&self) -> bool {
        let state = self.state.lock().unwrap();
        (!state.producer_started || state.producer_exited)
            && self.physical.segments.load(Ordering::Acquire) == 0
            && self.physical.deliveries.load(Ordering::Acquire) == 0
            && self.physical.retained_reservations.load(Ordering::Acquire) == 0
            && self.physical.fixed_metadata_holders.load(Ordering::Acquire) == 0
    }
    pub fn close(&self, reason: RootRetentionClose) {
        self.seal_reads(reason);
        self.finish_seal();
    }
    /// The registry uses this under its context fence. Dropping payloads and
    /// invoking readiness callbacks is deferred until that fence is unlocked.
    pub fn seal_reads(&self, reason: RootRetentionClose) {
        let mut state = self.state.lock().unwrap();
        state.lifetime.close(reason);
        state.stream.seal();
        if state.retiring_payloads.is_none() {
            state.retiring_payloads = Some(std::mem::take(&mut state.payloads));
        }
    }
    pub fn finish_seal(&self) {
        let retired = self.state.lock().unwrap().retiring_payloads.take();
        drop(retired);
        self.physical.wake();
    }
    /// Mint the read owner while the registry still holds its context fence.
    /// A looked-up Arc alone does not prove an admitted read or join lifetime.
    pub fn begin_read(
        self: &Arc<Self>,
        read: &RootResultRead,
    ) -> Result<RootChannelRead, RootChannelError> {
        if read.root_task() != self.spec.task
            || read.profile() != self.spec.contract.profile()
            || read.kind() != self.spec.contract.kind()
        {
            return Err(RootChannelError::Identity);
        }
        let state = self.state.lock().unwrap();
        if !state.context_owned {
            return Err(RootChannelError::Identity);
        }
        if !state.lifetime.allows_read() {
            return Err(RootChannelError::Closed);
        }
        let limit = NativeResultSupportGeometry::V1.root_live_send_holders as usize;
        self.physical
            .deliveries
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < limit).then_some(n + 1)
            })
            .map_err(|_| RootChannelError::Capacity)?;
        let guard = Arc::new(RootDeliveryOwner {
            physical: Arc::clone(&self.physical),
            registered: true,
        });
        Ok(RootChannelRead {
            channel: Arc::clone(self),
            request: read.clone(),
            guard,
        })
    }
    pub async fn read(
        self: &Arc<Self>,
        read: &RootResultRead,
    ) -> Result<RootResultDelivery, RootChannelError> {
        match self.begin_read(read) {
            Ok(admitted) => admitted.read().await,
            Err(RootChannelError::Closed) => {
                let consumed = self
                    .state
                    .lock()
                    .unwrap()
                    .stream
                    .snapshot()
                    .consumed_through;
                Ok(RootResultDelivery {
                    reply: self.reply(consumed, RootReadOutcome::AwaitTerminalControl),
                    guard: Arc::new(RootDeliveryOwner {
                        physical: Arc::clone(&self.physical),
                        registered: false,
                    }),
                })
            }
            Err(error) => Err(error),
        }
    }
    async fn read_admitted(
        &self,
        read: &RootResultRead,
        guard: Arc<RootDeliveryOwner>,
    ) -> Result<RootResultDelivery, RootChannelError> {
        let deadline = tokio::time::Instant::now() + read.max_wait();
        loop {
            let notified = self.physical.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let reply = self.read_once(read, &guard)?;
            if !matches!(reply.outcome, RootReadOutcome::NotReady)
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(RootResultDelivery { reply, guard });
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Ok(RootResultDelivery { reply, guard });
            }
        }
    }
    fn read_once(
        &self,
        read: &RootResultRead,
        guard: &Arc<RootDeliveryOwner>,
    ) -> Result<RootResultReply, RootChannelError> {
        let (reply, retired) = {
            let mut state = self.state.lock().unwrap();
            if !state.context_owned {
                return Err(RootChannelError::Identity);
            }
            if !state.lifetime.allows_read() {
                return Ok(self.reply(
                    state.stream.snapshot().consumed_through,
                    RootReadOutcome::AwaitTerminalControl,
                ));
            }
            let ack = state.stream.acknowledge(read.consumed())?;
            // Move at most two queue owners out of the lock before destroying
            // them; credit callbacks and physical wakeups never re-enter it.
            let mut retired: [Option<RootResultData>; 2] = [None, None];
            for slot in &mut retired {
                if state
                    .payloads
                    .front()
                    .is_some_and(|data| data.sequence().get() <= ack.accepted_consumed)
                {
                    *slot = state.payloads.pop_front();
                }
            }
            let outcome = (|| {
                Ok(match read.wanted() {
                    None => RootReadOutcome::AckOnly,
                    Some(wanted) => match state.stream.offer(wanted.get())? {
                        OfferOutcome::Retired { .. } => RootReadOutcome::Retired,
                        OfferOutcome::NotReady { .. } if state.failed.is_some() => {
                            RootReadOutcome::AwaitTerminalControl
                        }
                        OfferOutcome::NotReady { .. } => RootReadOutcome::NotReady,
                        OfferOutcome::Ready { item, .. } => match item.kind {
                            RetainedItemKind::Data { .. } => {
                                let data = state
                                    .payloads
                                    .iter()
                                    .find(|data| data.sequence().get() == item.sequence)
                                    .ok_or(RootChannelError::Payload)?;
                                let body = Bytes::from_owner(OfferedPayload {
                                    backing: data.body().clone(),
                                    _delivery: Arc::clone(guard),
                                });
                                RootReadOutcome::Data(
                                    RootResultData::try_new(
                                        data.kind(),
                                        data.sequence(),
                                        body,
                                        data.end_after_data(),
                                    )
                                    .map_err(|_| RootChannelError::Payload)?,
                                )
                            }
                            RetainedItemKind::End => RootReadOutcome::End(
                                state.lifetime.end().ok_or(RootChannelError::Producer)?,
                            ),
                        },
                    },
                })
            })();
            (
                outcome.map(|outcome| self.reply(ack.accepted_consumed, outcome)),
                retired,
            )
        };
        drop(retired);
        self.physical.writable.notify_observers();
        reply
    }
    fn reply(&self, accepted_consumed: u64, outcome: RootReadOutcome) -> RootResultReply {
        RootResultReply {
            root_task: self.spec.task,
            profile: self.spec.contract.profile(),
            kind: self.spec.contract.kind(),
            accepted_consumed,
            outcome,
        }
    }
    fn try_handoff(state: &mut ChannelState) {
        if state.context_owned
            && state.producer_exited
            && state.lifetime.end().is_some()
            && state.lifetime.allows_read()
        {
            state
                .lifetime
                .handoff_to_context()
                .expect("exact root context takes completed producer");
        }
    }
}

/// The producer stores this guard after all input/encoder/scratch fields, or
/// explicitly destroys those fields first. Dropping a deadline does not drop
/// this guard while the producer still executes.
pub struct RootProducerExit {
    channel: Arc<RootResultChannel>,
}
impl Drop for RootProducerExit {
    fn drop(&mut self) {
        let mut state = self.channel.state.lock().unwrap();
        state.producer_exited = true;
        state.lifetime.encoder_exited();
        if state.lifetime.end().is_none() && state.lifetime.allows_read() {
            state.failed = Some(RootChannelError::Producer);
        }
        RootResultChannel::try_handoff(&mut state);
        drop(state);
        self.channel.physical.wake();
    }
}
pub struct RootDeliveryOwner {
    physical: Arc<PhysicalOwners>,
    registered: bool,
}
struct OfferedPayload {
    // Even a caller that retains only a Bytes alias keeps this delivery slot.
    backing: Bytes,
    _delivery: Arc<RootDeliveryOwner>,
}
impl AsRef<[u8]> for OfferedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.backing
    }
}
impl Drop for RootDeliveryOwner {
    fn drop(&mut self) {
        if self.registered {
            self.physical.deliveries.fetch_sub(1, Ordering::AcqRel);
            self.physical.wake();
        }
    }
}
pub struct RootChannelRead {
    channel: Arc<RootResultChannel>,
    request: RootResultRead,
    guard: Arc<RootDeliveryOwner>,
}
impl RootChannelRead {
    pub async fn read(self) -> Result<RootResultDelivery, RootChannelError> {
        self.channel.read_admitted(&self.request, self.guard).await
    }
}
/// Native moves the owner into its response-body/H2 lifetime. Returning from
/// a handler or serializing a protobuf is not a physical send-exit receipt.
pub struct RootResultDelivery {
    reply: RootResultReply,
    guard: Arc<RootDeliveryOwner>,
}
impl RootResultDelivery {
    pub fn reply(&self) -> &RootResultReply {
        &self.reply
    }
    pub fn into_parts(self) -> (RootResultReply, Arc<RootDeliveryOwner>) {
        (self.reply, self.guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn channel(output: FrozenRootOutput) -> (Arc<RootResultChannel>, Arc<ResultRetainedBudget>) {
        let limits =
            WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, 4 * 1024 * 1024 * 1024).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, output)),
            },
            Arc::clone(&budget),
            limits,
        )
        .unwrap();
        (channel, budget)
    }
    fn facts() -> FrozenRootOutput {
        FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1)
    }
    fn read(channel: &RootResultChannel, wanted: Option<u64>, consumed: u64) -> RootResultRead {
        RootResultRead::try_new(
            channel.spec.task,
            channel.spec.contract.profile(),
            channel.spec.contract.kind(),
            wanted.map(|n| NonZeroU64::new(n).unwrap()),
            consumed,
            Duration::from_millis(1),
        )
        .unwrap()
    }
    fn publish(channel: &Arc<RootResultChannel>, body: &[u8], end: bool) {
        let mut builder = channel.try_segment().unwrap().unwrap();
        builder.output()[..body.len()].copy_from_slice(body);
        channel.publish_segment(builder, body.len(), end).unwrap();
    }
    fn data(delivery: &RootResultDelivery) -> &RootResultData {
        let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
            panic!("expected Data");
        };
        data
    }
    #[test]
    fn allocation_failure_releases_owners_without_a_callback_under_channel_lock() {
        let (channel, budget) = channel(facts());
        let before = budget.retained_bytes_for_test();
        let observed = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&observed);
        let weak = Arc::downgrade(&channel);
        let _registration = channel.writable_observable().subscribe(Arc::new(move || {
            let channel = weak.upgrade().unwrap();
            let _ = channel.snapshot();
            let _ = channel.producer_state();
            counted.fetch_add(1, Ordering::SeqCst);
        }));
        assert!(matches!(
            channel.try_segment_allocating(|_, _| Err(RootChannelError::Capacity)),
            Err(RootChannelError::Capacity),
        ));
        assert!(observed.load(Ordering::SeqCst) > 0);
        assert_eq!(budget.retained_bytes_for_test(), before);
        assert!(channel.physical_idle());
        let builder = channel
            .try_segment()
            .unwrap()
            .expect("failed allocation returned its position");
        drop(builder);
        assert!(channel.physical_idle());
    }
    #[test]
    fn schema_spare_and_core_metadata_share_the_same_fixed_grant() {
        use novarocks_result_contract::{
            ClientRenderSchema, NativeRenderType, RenderColumn, RenderField, RenderPresentation,
        };
        let mut name = String::with_capacity(RootProfileV1::SCHEMA_BACKING_BYTES - 1024);
        name.push('x');
        let schema = ClientRenderSchema::try_new(
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: None,
                name,
                field: RenderField {
                    native_type: NativeRenderType::String,
                    presentation: RenderPresentation::ScalarText,
                    nullable: true,
                },
            }],
            1,
        )
        .expect("wire is tiny and schema's separate limit is valid");
        assert!(schema.backing_bytes() > RootProfileV1::SCHEMA_BACKING_BYTES - 1024);
        let limits =
            WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, 4 * 1024 * 1024 * 1024).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        assert!(matches!(
            RootResultChannel::try_open(
                RootResultWriteSpec {
                    task: task(),
                    contract: Arc::new(RootOutputContract::new(
                        RootProfileId::V1,
                        FrozenRootOutput::ClientRows(schema)
                    )),
                },
                Arc::clone(&budget),
                limits
            ),
            Err(RootChannelError::Capacity)
        ));
        assert_eq!(
            budget.retained_bytes_for_test(),
            0,
            "combined bound is checked before allocating the channel"
        );
    }
    #[test]
    fn host_metadata_suballocations_are_bounded_and_hold_until_actual_drop() {
        let (channel, budget) = channel(facts());
        let half = (1024 * 1024 - RootResultChannel::CORE_METADATA_CAPACITY) / 2;
        let first = channel.try_reserve_metadata(half).unwrap();
        let second = channel.try_reserve_metadata(half).unwrap();
        assert!(matches!(
            channel.try_reserve_metadata(32),
            Err(RootChannelError::Capacity)
        ));
        assert_eq!(
            budget.retained_bytes_for_test(),
            1024 * 1024,
            "suballocation does not create another wallet or charge"
        );
        channel.close(RootRetentionClose::ContextReleased);
        assert!(!channel.physical_idle());
        drop(first);
        assert!(!channel.physical_idle());
        drop(second);
        assert!(channel.physical_idle());
        assert!(matches!(
            channel.try_reserve_metadata(32),
            Err(RootChannelError::Closed)
        ));
    }
    #[test]
    fn tiny_metadata_claims_cannot_create_unbounded_control_owners() {
        let (channel, _) = channel(facts());
        let geometry = NativeResultSupportGeometry::V1;
        let positions =
            (geometry.root_maximum_root_drivers + geometry.root_live_send_holders + 1) as usize;
        let mut claims = Vec::with_capacity(positions);
        for _ in 0..positions {
            claims.push(
                channel
                    .try_reserve_metadata(std::mem::size_of::<RootMetadataReservation>())
                    .unwrap(),
            );
        }
        assert!(matches!(
            channel.try_reserve_metadata(std::mem::size_of::<RootMetadataReservation>()),
            Err(RootChannelError::Capacity)
        ));
        assert!(matches!(
            channel.try_reserve_metadata(1),
            Err(RootChannelError::Capacity)
        ));
        drop(claims);
        assert!(channel.physical_idle());
        drop(
            channel
                .try_reserve_metadata(32)
                .expect("physical exit returns metadata position"),
        );
    }
    #[test]
    fn blocked_raw_admission_does_not_emit_a_false_capacity_transition() {
        let (channel, _) = channel(facts());
        let ResultWriteAdmission::Granted(credit) = channel
            .try_reserve(256 * 1024 * 1024 - 1024 * 1024)
            .unwrap()
        else {
            panic!("complete remaining retained capacity");
        };
        let observable = channel.writable_observable();
        let generation = observable.generation();
        for _ in 0..128 {
            assert!(matches!(
                channel.try_reserve(1).unwrap(),
                ResultWriteAdmission::Blocked
            ));
        }
        assert_eq!(
            observable.generation(),
            generation,
            "failed tentative positions cannot cause a retry spin"
        );
        assert_eq!(
            channel
                .physical
                .retained_reservations
                .load(Ordering::Acquire),
            1
        );
        drop(credit);
        assert_ne!(
            observable.generation(),
            generation,
            "actual capacity return still wakes waiters"
        );
    }
    #[tokio::test]
    async fn immutable_replay_atomic_end_and_finish_ignore_consumer_ack() {
        let (channel, budget) = channel(facts());
        channel.mark_context_owned().unwrap();
        let producer = channel.start_producer().unwrap();
        channel.note_rows(7).unwrap();
        channel.request_finish().unwrap();
        publish(&channel, b"abc", true);
        assert_eq!(channel.producer_state(), RootProducerState::Finishing);
        drop(producer);
        assert_eq!(channel.producer_state(), RootProducerState::ContextHeld);
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let replay = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert_eq!(first.reply(), replay.reply());
        assert_eq!(data(&first).sequence().get(), 1);
        assert_eq!(data(&first).end_after_data().unwrap().sequence.get(), 2);
        assert_eq!(data(&first).end_after_data().unwrap().output_rows, 7);
        // Closing is legal without any final ACK and cannot erase RootFinished.
        channel.close(RootRetentionClose::ContextReleased);
        assert_eq!(channel.snapshot().consumed_through, 0);
        assert!(!channel.physical_idle());
        drop(first);
        drop(replay);
        assert!(channel.physical_idle());
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
        drop(channel);
        assert_eq!(budget.retained_bytes_for_test(), 0);
    }
    #[tokio::test]
    async fn applied_ack_survives_not_ready_and_lost_reply_with_backing_held() {
        let (channel, budget) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&first).body().clone();
        drop(first);
        let held = budget.retained_bytes_for_test();
        let lost = channel.read(&read(&channel, Some(2), 1)).await.unwrap();
        assert_eq!(lost.reply().accepted_consumed, 1);
        assert_eq!(lost.reply().outcome, RootReadOutcome::NotReady);
        assert_eq!(channel.snapshot().data_positions, 0);
        assert_eq!(
            budget.retained_bytes_for_test(),
            held,
            "ACK is not physical release"
        );
        drop(lost);
        let retry = channel.read(&read(&channel, Some(1), 1)).await.unwrap();
        assert_eq!(retry.reply().outcome, RootReadOutcome::Retired);
        assert_eq!(retry.reply().accepted_consumed, 1);
        drop(retry);
        drop(alias);
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
    }
    #[tokio::test]
    async fn aliases_hold_delivery_positions_after_handler_and_ack() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&first).body().clone();
        drop(first);
        drop(channel.read(&read(&channel, None, 1)).await.unwrap());
        publish(&channel, b"b", false);
        let second = channel.read(&read(&channel, Some(2), 1)).await.unwrap();
        let other_alias = data(&second).body().clone();
        drop(second);
        assert!(matches!(
            channel.read(&read(&channel, None, 2)).await,
            Err(RootChannelError::Capacity)
        ));
        assert_eq!(channel.physical.deliveries.load(Ordering::Acquire), 2);
        channel.close(RootRetentionClose::ContextReleased);
        assert_eq!(channel.physical.segments.load(Ordering::Acquire), 2);
        assert!(!channel.physical_idle());
        drop(alias);
        drop(other_alias);
        assert!(channel.physical_idle());
    }
    #[tokio::test]
    async fn full_window_has_reserved_standalone_end_and_rejects_bad_ack_gap() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"a", false);
        publish(&channel, b"b", false);
        assert!(channel.try_segment().unwrap().is_none());
        channel.request_finish().unwrap();
        channel.publish_end().unwrap();
        assert_eq!(channel.snapshot().end_sequence, Some(3));
        assert!(matches!(
            channel.read(&read(&channel, Some(2), 0)).await,
            Err(RootChannelError::Stream(StreamError::OfferGap))
        ));
        assert!(matches!(
            channel.read(&read(&channel, None, 1)).await,
            Err(RootChannelError::Stream(
                StreamError::AcknowledgementBeyondOffered
            ))
        ));
        let first = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert!(data(&first).end_after_data().is_none());
        drop(first);
        let second = channel.read(&read(&channel, Some(2), 1)).await.unwrap();
        assert!(data(&second).end_after_data().is_none());
        drop(second);
        let end = channel.read(&read(&channel, Some(3), 2)).await.unwrap();
        assert!(matches!(
            end.reply().outcome,
            RootReadOutcome::End(RootResultEnd { output_rows: 0, .. })
        ));
    }
    #[tokio::test]
    async fn context_seal_wakes_long_poll_and_waits_actual_guard_exit() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        let request = RootResultRead::try_new(
            channel.spec.task,
            channel.spec.contract.profile(),
            channel.spec.contract.kind(),
            NonZeroU64::new(1),
            0,
            Duration::from_millis(1000),
        )
        .unwrap();
        let reader = Arc::clone(&channel);
        let job = tokio::spawn(async move { reader.read(&request).await.unwrap() });
        tokio::task::yield_now().await;
        assert_eq!(channel.physical.deliveries.load(Ordering::Acquire), 1);
        channel.close(RootRetentionClose::LeaseExpired);
        let delivery = tokio::time::timeout(Duration::from_millis(100), job)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            delivery.reply().outcome,
            RootReadOutcome::AwaitTerminalControl
        );
        assert!(!channel.physical_idle());
        drop(delivery);
        assert!(channel.physical_idle());
    }
    #[tokio::test]
    async fn provisional_channel_and_mismatched_kind_are_never_routes() {
        let (channel, _) = channel(facts());
        assert!(matches!(
            channel.read(&read(&channel, Some(1), 0)).await,
            Err(RootChannelError::Identity)
        ));
        channel.mark_context_owned().unwrap();
        let mismatch = RootResultRead::try_new(
            channel.spec.task,
            RootProfileId::V1,
            RootOutputKind::CountOnly,
            NonZeroU64::new(1),
            0,
            Duration::from_millis(1),
        )
        .unwrap();
        assert!(matches!(
            channel.read(&mismatch).await,
            Err(RootChannelError::Identity)
        ));
        assert_eq!(channel.physical.deliveries.load(Ordering::Acquire), 0);
    }
    #[tokio::test]
    async fn count_zero_overflow_and_actual_producer_exit() {
        let (channel, _) = channel(FrozenRootOutput::CountOnly);
        channel.mark_context_owned().unwrap();
        let producer = channel.start_producer().unwrap();
        assert!(matches!(
            channel.try_segment(),
            Err(RootChannelError::Payload)
        ));
        channel.note_rows(u64::MAX).unwrap();
        channel.request_finish().unwrap();
        channel.publish_end().unwrap();
        assert_eq!(channel.producer_state(), RootProducerState::Finishing);
        drop(producer);
        let end = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert!(matches!(
            end.reply().outcome,
            RootReadOutcome::End(RootResultEnd {
                output_rows: u64::MAX,
                ..
            })
        ));
        drop(end);
        let (empty, _) = super::tests::channel(FrozenRootOutput::CountOnly);
        empty.mark_context_owned().unwrap();
        let producer = empty.start_producer().unwrap();
        empty.request_finish().unwrap();
        empty.publish_end().unwrap();
        drop(producer);
        assert_eq!(empty.producer_state(), RootProducerState::ContextHeld);
        let end = empty.read(&read(&empty, Some(1), 0)).await.unwrap();
        assert!(matches!(
            end.reply().outcome,
            RootReadOutcome::End(RootResultEnd { output_rows: 0, .. })
        ));
        let (overflow, _) = super::tests::channel(FrozenRootOutput::CountOnly);
        overflow.note_rows(u64::MAX).unwrap();
        assert_eq!(overflow.note_rows(1), Err(RootChannelError::RowCount));
        overflow.request_finish().unwrap();
        assert_eq!(overflow.publish_end(), Err(RootChannelError::Producer));
        assert!(matches!(
            overflow.producer_state(),
            RootProducerState::Failed(_)
        ));
    }
    #[test]
    fn unused_builder_returns_capacity_only_after_actual_backing_drop() {
        let (channel, budget) = channel(facts());
        let initial = budget.retained_bytes_for_test();
        let builder = channel.try_segment().unwrap().unwrap();
        assert!(builder.capacity_bytes() >= RootProfileV1::SEGMENT_BYTES);
        assert!(channel.try_segment().unwrap().is_none());
        assert!(budget.retained_bytes_for_test() > initial);
        drop(builder);
        assert_eq!(budget.retained_bytes_for_test(), initial);
        assert_eq!(channel.physical.segments.load(Ordering::Acquire), 0);
        assert!(channel.try_segment().unwrap().is_some());
    }
    #[tokio::test]
    async fn failed_producer_drop_cannot_publish_success_or_fake_exit() {
        let (channel, _) = channel(facts());
        channel.mark_context_owned().unwrap();
        let producer = channel.start_producer().unwrap();
        assert!(!channel.physical_idle());
        drop(producer);
        assert!(matches!(
            channel.producer_state(),
            RootProducerState::Failed(_)
        ));
        let reply = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
        drop(reply);
        assert!(channel.physical_idle());
    }
    #[test]
    fn all_channel_and_segment_capacity_uses_original_shared_pool() {
        let limits = WorkerResultRetainedLimits::try_new(2 * 1024 * 1024, 2 * 1024 * 1024).unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(2 * 1024 * 1024).unwrap());
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, facts())),
            },
            Arc::clone(&budget),
            limits,
        )
        .unwrap();
        assert!(
            channel.try_segment().unwrap().is_none(),
            "segment envelope is part of retained capacity"
        );
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
        let ResultWriteAdmission::Granted(credit) = channel.try_reserve(1024 * 1024).unwrap()
        else {
            panic!("remaining shared capacity");
        };
        assert_eq!(budget.retained_bytes_for_test(), 2 * 1024 * 1024);
        assert!(matches!(
            channel.try_reserve(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        drop(credit);
        drop(channel);
        assert_eq!(budget.retained_bytes_for_test(), 0);
    }

    #[tokio::test]
    async fn a_builder_admitted_before_failure_cannot_publish_data_or_end() {
        for with_end in [false, true] {
            let (channel, _) = channel(facts());
            channel.mark_context_owned().unwrap();
            let producer = channel.start_producer().unwrap();
            let mut builder = channel.try_segment().unwrap().unwrap();
            builder.output()[0] = b'x';
            channel.note_rows(u64::MAX).unwrap();
            assert_eq!(channel.note_rows(1), Err(RootChannelError::RowCount));
            channel.request_finish().unwrap();
            assert_eq!(
                channel.publish_segment(builder, 1, with_end),
                Err(RootChannelError::Producer)
            );
            assert!(channel.snapshot().end_sequence.is_none());
            drop(producer);
            assert!(!channel.state.lock().unwrap().lifetime.root_finished());
            let reply = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
            assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
        }
    }

    #[test]
    fn raw_retained_credit_and_its_control_position_follow_actual_drop() {
        let (channel, budget) = channel(facts());
        let ResultWriteAdmission::Granted(mut credit) = channel.try_reserve(4096).unwrap() else {
            panic!("admitted credit");
        };
        channel.close(RootRetentionClose::ContextReleased);
        assert!(!channel.physical_idle());
        assert_eq!(
            channel
                .physical
                .retained_reservations
                .load(Ordering::Acquire),
            1
        );
        credit.shrink_to(0).unwrap();
        assert!(
            !channel.physical_idle(),
            "credit control owner is still live even after shrinking its bytes"
        );
        drop(credit);
        assert!(channel.physical_idle());
        assert_eq!(budget.retained_bytes_for_test(), 1024 * 1024);
    }

    #[tokio::test]
    async fn channel_metadata_and_wallet_follow_last_response_body_alias() {
        let (channel, budget) = channel(facts());
        channel.mark_context_owned().unwrap();
        publish(&channel, b"x", false);
        let delivery = channel.read(&read(&channel, Some(1), 0)).await.unwrap();
        let alias = data(&delivery).body().clone();
        drop(delivery);
        channel.close(RootRetentionClose::ContextReleased);
        drop(channel);
        let expected = 1024 * 1024
            + NativeResultSupportGeometry::V1.root_segment_backing_capacity_bytes as usize;
        assert_eq!(
            budget.retained_bytes_for_test(),
            expected,
            "metadata cannot be returned while response aliases hold it"
        );
        let weak_budget = Arc::downgrade(&budget);
        drop(budget);
        assert!(weak_budget.upgrade().is_some());
        drop(alias);
        assert!(weak_budget.upgrade().is_none());
    }

    #[test]
    fn even_tiny_credits_have_finite_physical_object_positions() {
        let (channel, _) = channel(facts());
        let mut credits = Vec::new();
        for _ in 0..6 {
            let ResultWriteAdmission::Granted(credit) = channel.try_reserve(1).unwrap() else {
                panic!("finite credit position");
            };
            credits.push(credit);
        }
        assert!(matches!(
            channel.try_reserve(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        assert!(!channel.physical_idle());
        drop(credits);
        assert!(channel.physical_idle());
    }
}
