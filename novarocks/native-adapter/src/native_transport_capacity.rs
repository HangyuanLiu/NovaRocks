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

//! Startup-prepaid original HTTP/2 backing stock for a BE process.
//!
//! These explicit workspace caps do not advertise V1 support. TLS internals,
//! other stream/task/queue scaffolds, the caller's enclosing factory closure, and
//! independent request/body/message owners still require composition proofs.
//! A slot is returned by the final carrier's physical exit, never by a deadline,
//! connection completion, configuration replacement, or logical cancellation.

use crate::native_channel_identity::InlineNativeChannelIdentity;
use crate::native_connection_key_capacity::{
    NativeConnectionKeyCapacity, NativeConnectionKeyToken,
};
use crate::native_incoming_key_capacity::{
    NativeIncomingKey, NativeIncomingKeyCapacity, NativeIncomingKeyToken,
};
use bytes::Bytes;
use hyper::http::header::HeaderMapAllocationPool;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::Layout;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tonic::transport::Http2ConnectionConfig;

/// Independent original stock; Control cannot consume or lend Data positions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportClass {
    /// Incoming FE data and both directions of peer exchange/runtime filters.
    Data,
    /// Incoming FE lifecycle, conservative outgoing reports, and handshakes.
    Control,
}

#[derive(Clone, Copy, Debug)]
struct Dimensions {
    data_positions: usize,
    control_positions: usize,
    data_acquisitions: usize,
    control_acquisitions: usize,
    frame: usize,
    header: usize,
    writer: usize,
    streams: usize,
    events: usize,
    data_buffers: usize,
    goaway_buffers: usize,
    field_bytes: usize,
    field_positions: usize,
    maps: usize,
    map_keys: usize,
    map_extras: usize,
    table_bytes: usize,
    server_task_bound: usize,
    stream_task_bound: usize,
    connection_bound: usize,
    stock_bound: usize,
}

const VACANT: u8 = 0;
const ACQUIRING: u8 = 1;
const INITIAL_COMPLETE: u8 = 2;
const LIVE: u8 = 3;
const RETIRING: u8 = 4;

struct ConnectionRecord {
    claimed: AtomicBool,
    generation: AtomicU64,
    phase: AtomicU8,
    incoming: Mutex<Option<(NativeIncomingKey, NativeIncomingKeyToken)>>,
}

struct StockCore {
    // Backings precede credit in declaration order. No Weak StockCore escapes.
    slots: Vec<ConnectionRecord>,
    dimensions: Dimensions,
    acquisitions: [AtomicUsize; 2],
    listener_registrations: [AtomicBool; 2],
    channel_cache_claimed: AtomicBool,
    connection_keys: NativeConnectionKeyCapacity,
    incoming_keys: NativeIncomingKeyCapacity,
    credit: ResultWriteCredit,
    // Keep the existing issuer alive through the credit's release callback.
    _budget: Arc<ResultRetainedBudget>,
}

/// A strong-only cloneable original-stock handle, rather than an outer Arc.
///
/// The last handle uses Arc::into_inner: the Core Arc allocation exits before
/// the moved Core's fixed slot Vec and its original process credit. Pool and
/// field aliases retain this same handle through one prepaid Bytes exit guard.
pub struct NativeTransportCapacityFactory {
    core: Option<Arc<StockCore>>,
}

impl Clone for NativeTransportCapacityFactory {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(self.core())),
        }
    }
}

impl Drop for NativeTransportCapacityFactory {
    fn drop(&mut self) {
        if let Some(core) = self.core.take() {
            drop(Arc::into_inner(core));
        }
    }
}

impl fmt::Debug for NativeTransportCapacityFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeTransportCapacityFactory")
            .field("dimensions", &self.core().dimensions)
            .finish_non_exhaustive()
    }
}

struct SlotExit {
    factory: NativeTransportCapacityFactory,
    index: usize,
    generation: u64,
    key_token: Option<NativeConnectionKeyToken>,
}

impl Drop for SlotExit {
    fn drop(&mut self) {
        // Bytes first drops its owner and frees its complete wrapper allocation.
        // The key kernel serializes its bounded physical-exit transition.
        // Acquire in the next stock claim observes
        // all preceding physical retirement before constructing fresh pools.
        let record = &self.factory.core().slots[self.index];
        assert_eq!(
            record.generation.load(Ordering::Acquire),
            self.generation,
            "exact physical connection generation"
        );
        if let Some(token) = self.key_token {
            self.factory
                .core()
                .connection_keys
                .exit(token)
                .expect("exact physical key generation exited once");
        }
        if let Some((_, token)) = record
            .incoming
            .lock()
            .expect("original incoming record lock")
            .take()
        {
            self.factory
                .core()
                .incoming_keys
                .exit(token)
                .expect("exact original incoming key exit");
        }
        record.phase.store(VACANT, Ordering::Release);
        record.claimed.store(false, Ordering::Release);
    }
}

/// Original records contain no Channel, task, socket or observer alias.
/// The observer and ten pools retain the same carrier until actual exit.
struct NativeLifecycleObserver {
    factory: NativeTransportCapacityFactory,
    index: usize,
    generation: u64,
    key_token: Option<NativeConnectionKeyToken>,
}

impl NativeLifecycleObserver {
    fn record(&self) -> io::Result<&ConnectionRecord> {
        let record = &self.factory.core().slots[self.index];
        if !record.claimed.load(Ordering::Acquire)
            || record.generation.load(Ordering::Acquire) != self.generation
        {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(record)
    }
}

impl h2::ConnectionLifecycleObserver for NativeLifecycleObserver {
    fn on_initial_settings_complete(&self) -> io::Result<()> {
        self.record()?
            .phase
            .compare_exchange(
                ACQUIRING,
                INITIAL_COMPLETE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| io::ErrorKind::ConnectionAborted.into())
    }

    fn on_acquisition_complete(&self) -> io::Result<()> {
        self.record()?
            .phase
            .compare_exchange(INITIAL_COMPLETE, LIVE, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::Error::from(io::ErrorKind::ConnectionAborted))?;
        if let Some(token) = self.key_token
            && let Err(error) = self.factory.core().connection_keys.install(token)
        {
            let _ = self.on_retiring();
            return Err(error);
        }
        Ok(())
    }

    fn on_retiring(&self) -> io::Result<()> {
        let record = self.record()?;
        // Serialize seal and retirement on the original physical record.
        let incoming = record
            .incoming
            .lock()
            .map_err(|_| io::ErrorKind::InvalidData)?;
        record.phase.store(RETIRING, Ordering::Release);
        if let Some((_, token)) = *incoming {
            self.factory.core().incoming_keys.retire(token)?;
        }
        if let Some(token) = self.key_token {
            self.factory.core().connection_keys.retire(token)?;
        }
        Ok(())
    }
}

impl Drop for NativeLifecycleObserver {
    fn drop(&mut self) {
        // Includes unpolled attempts that never obtained a bound IO lease.
        let _ = h2::ConnectionLifecycleObserver::on_retiring(self);
    }
}

/// Same original carrier and physical generation used by final SlotExit.
/// This handle owns no independent grant or connection registry.
#[derive(Clone)]
pub(crate) struct NativeIncomingConnectionBinding {
    factory: NativeTransportCapacityFactory,
    index: usize,
    generation: u64,
    _original: Bytes,
}

impl NativeIncomingConnectionBinding {
    pub(crate) fn seal(&self, key: NativeIncomingKey) -> io::Result<()> {
        let record = &self.factory.core().slots[self.index];
        let mut incoming = record
            .incoming
            .lock()
            .map_err(|_| io::ErrorKind::InvalidData)?;
        if !record.claimed.load(Ordering::Acquire)
            || record.generation.load(Ordering::Acquire) != self.generation
            || record.phase.load(Ordering::Acquire) != LIVE
        {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        if let Some((existing, _)) = *incoming {
            return if existing == key {
                Ok(())
            } else {
                Err(io::ErrorKind::ConnectionAborted.into())
            };
        }
        let token = self.factory.core().incoming_keys.claim(key)?;
        *incoming = Some((key, token));
        Ok(())
    }
}

struct AcquisitionExit {
    factory: NativeTransportCapacityFactory,
    class: TransportClass,
}

struct ListenerRegistrationExit {
    factory: NativeTransportCapacityFactory,
    class: TransportClass,
}

impl Drop for ListenerRegistrationExit {
    fn drop(&mut self) {
        assert!(
            self.factory.core().listener_registrations[class_index(self.class)]
                .swap(false, Ordering::AcqRel),
            "original listener registration exits once"
        );
    }
}

impl Drop for AcquisitionExit {
    fn drop(&mut self) {
        // The owned Bytes wrapper has physically exited before this callback.
        // Incoming and every outgoing attempt use these same process counters.
        let previous = self.factory.core().acquisitions[class_index(self.class)]
            .fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "live acquisition position");
    }
}

fn class_index(class: TransportClass) -> usize {
    match class {
        TransportClass::Data => 0,
        TransportClass::Control => 1,
    }
}

#[cfg(test)]
#[path = "native_key_capacity_owner_tests.rs"]
mod key_owner_tests;

fn invalid() -> io::Error {
    io::ErrorKind::InvalidInput.into()
}

fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(invalid)
}

fn mul(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b).ok_or_else(invalid)
}

fn value(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid())
}

fn arc_bytes<T>() -> io::Result<usize> {
    // Pinned Rust ArcInner: two AtomicUsize counters, then aligned T.
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .map(|(layout, _)| layout.pad_to_align().size())
        .map_err(|_| invalid())
}

impl Dimensions {
    fn frozen() -> io::Result<Self> {
        if !cfg!(all(
            target_pointer_width = "64",
            any(target_vendor = "apple", target_os = "linux")
        )) {
            return Err(invalid());
        }
        let g = NativeResultSupportGeometry::V1;
        let frontends = value(g.transport_authenticated_live_frontends_per_backend)?;
        let connecting = value(g.transport_connecting_positions_per_lane)?;
        let closing = value(g.transport_closing_positions_per_lane)?;
        let fe_live = add(
            add(
                value(g.transport_connections_per_frontend_backend_result)?,
                value(g.transport_connections_per_frontend_backend_observation)?,
            )?,
            value(g.transport_connections_per_frontend_backend_submission)?,
        )?;
        // FE data is incoming only: 2 * (4 + 4 + 2 + 3 * (1 + 1)).
        let incoming_fe_data = mul(frontends, add(fe_live, mul(3, add(connecting, closing)?)?)?)?;
        let exchange = add(
            value(g.transport_exchange_connections_per_peer)?,
            add(
                value(g.transport_exchange_connecting_positions_per_peer)?,
                value(g.transport_exchange_closing_positions_per_peer)?,
            )?,
        )?;
        let filters = add(
            value(g.transport_runtime_filter_connections_per_peer)?,
            add(
                value(g.transport_runtime_filter_connecting_positions_per_peer)?,
                value(g.transport_runtime_filter_closing_positions_per_peer)?,
            )?,
        )?;
        // Both peer directions conservatively include all 32 peer positions:
        // 2 * 32 * ((2 + 1 + 1) + (1 + 1 + 1)) = 448.
        let peer_data = mul(
            mul(2, value(g.transport_maximum_live_backends)?)?,
            add(exchange, filters)?,
        )?;
        let data_floor = add(
            add(incoming_fe_data, peer_data)?,
            value(g.transport_data_handshake_positions)?,
        )?;
        // Incoming lifecycle and conservative outgoing report lanes: 6 + 6.
        let control_lane = mul(
            frontends,
            add(
                value(g.transport_connections_per_frontend_backend_lifecycle_control)?,
                add(connecting, closing)?,
            )?,
        )?;
        // Backend announce/report uses the BE data client runtime in the
        // distinct outgoing FE direction. Reserve another 2 * (1 + 1 + 1)
        // there, rather than borrowing incoming Control positions.
        let data_positions = add(data_floor, control_lane)?;
        let control_positions = add(
            mul(2, control_lane)?,
            value(g.transport_control_handshake_positions)?,
        )?;
        let frame = value(g.transport_h2_frame_bytes)?;
        let header = value(g.transport_h2_header_bytes)?;
        let streams = value(g.transport_streams_per_connection)?;
        let field_bytes = value(g.transport_h2_connection_receive_window_bytes)?;
        if frame != 16384 || header != 16384 || streams != 128 || field_bytes % frame != 0 {
            return Err(invalid());
        }
        let mut dimensions = Self {
            data_positions,
            control_positions,
            data_acquisitions: value(g.transport_data_handshake_positions)?,
            control_acquisitions: value(g.transport_control_handshake_positions)?,
            frame,
            header,
            writer: value(g.transport_h2_send_buffer_bytes)?,
            streams,
            events: mul(2, streams)?,
            data_buffers: field_bytes / frame,
            goaway_buffers: 1,
            field_bytes,
            field_positions: mul(32, streams)?,
            // Four BE request/fallback/initial/trailer maps, plus one received
            // trailer, fallible clone, owning drain, and queued-header headroom
            // per stream. Extra claims refuse; this is not a caller-path proof.
            maps: mul(8, streams)?,
            map_keys: 16,
            map_extras: 16,
            // Required pre-ACK incoming HPACK capacity even when advertising 0.
            table_bytes: 4096,
            server_task_bound: 0,
            stream_task_bound: 0,
            connection_bound: 0,
            stock_bound: 0,
        };
        let mut bound = h2::ReceiveFrameBuffer::allocation_capacity_bound(frame)?;
        bound = add(
            bound,
            h2::ReceiveHeaderBlockBuffer::allocation_capacity_bound(header)?,
        )?;
        bound = add(
            bound,
            h2::ReceiveHeaderFieldPool::allocation_capacity_bound(
                dimensions.field_bytes,
                dimensions.field_positions,
                header - 32,
            )?,
        )?;
        bound = add(
            bound,
            h2::ReceiveHeaderTableBuffer::allocation_capacity_bound(4096)?,
        )?;
        bound = add(
            bound,
            h2::SendHeaderBlockPool::allocation_capacity_bound(header)?,
        )?;
        bound = add(
            bound,
            h2::SendFrameBuffer::allocation_capacity_bound(dimensions.writer, frame)?,
        )?;
        bound = add(
            bound,
            h2::ReceiveBufferPool::allocation_capacity_bound(dimensions.data_buffers, frame)?,
        )?;
        bound = add(
            bound,
            h2::ReceiveBufferPool::allocation_capacity_bound(1, frame)?,
        )?;
        bound = add(
            bound,
            HeaderMapAllocationPool::allocation_capacity_bound(
                dimensions.maps,
                dimensions.map_keys,
                dimensions.map_extras,
            )
            .map_err(|_| invalid())?,
        )?;
        bound = add(
            bound,
            Bytes::owner_with_exit_guard_metadata_size::<Bytes, SlotExit>(),
        )?;
        bound = add(
            bound,
            h2::StreamStoreBuffer::allocation_capacity_bound(streams, streams)?,
        )?;
        bound = add(
            bound,
            h2::ConnectionLifecycle::allocation_capacity_bound::<NativeLifecycleObserver>()?,
        )?;
        // Prepay the concrete Native IO Box before TCP/TLS can construct it.
        // Both directions and all installed profiles share this stock, so its
        // maximum covers the actual concrete types, not the trait-object handle.
        // Rustls's private buffers and Tokio socket registration remain separate.
        let native_io_box = [
            novarocks_native_trust::NativeTransportMode::Disabled,
            novarocks_native_trust::NativeTransportMode::Automatic,
            novarocks_native_trust::NativeTransportMode::Pem,
        ]
        .into_iter()
        .flat_map(|mode| {
            [
                novarocks_native_trust::NativeIoDirection::Client,
                novarocks_native_trust::NativeIoDirection::Server,
            ]
            .into_iter()
            .map(move |direction| {
                novarocks_native_trust::native_io_box_layout(mode, direction).size()
            })
        })
        .max()
        .expect("closed Native IO profile set");
        bound = add(bound, native_io_box)?;
        // Tonic's Connector additionally Boxes this exact Native connector
        // response. Its outer IO guard must cover this Box's actual dealloc.
        bound = add(
            bound,
            Layout::new::<hyper_util::rt::TokioIo<novarocks_native_trust::BoxedNativeIo>>().size(),
        )?;
        // A socket's reactor registration outlives TcpStream Drop. Its private
        // strong-only handle retains this same carrier until actual Arc/PAL
        // deallocation at the reactor's safe retirement point.
        let registration = tokio::net::TcpStream::registration_allocation_capacity_bound()?;
        bound = add(bound, registration)?;
        // This is the actual production constructor's return type, including
        // Tokio's existing large-future Box branch and complete TaskCell Layout.
        dimensions.server_task_bound =
            crate::native_server::native_server_task_allocation_capacity_bound()?;
        bound = add(bound, dimensions.server_task_bound)?;
        dimensions.stream_task_bound =
            crate::native_server::native_server_stream_task_allocation_capacity_bound()?;
        let stream_tasks =
            crate::native_task_executor::NativeTaskExecutor::allocation_capacity_bound(
                streams,
                dimensions.stream_task_bound,
            )?;
        // Independently check this actual task/position/carrier subgraph
        // against the frozen bookkeeping envelope, not just the aggregate
        // connection ceiling. Other stream scaffolds still need composition.
        if stream_tasks > mul(streams, value(g.transport_stream_bookkeeping_bytes)?)? {
            return Err(invalid());
        }
        bound = add(bound, stream_tasks)?;
        let stream_extra = add(
            add(
                value(g.transport_stream_bookkeeping_bytes)?,
                value(g.transport_idle_decoder_bytes)?,
            )?,
            value(g.transport_header_raw_and_expanded_bytes)?,
        )?;
        let envelope = add(
            value(g.transport_connection_all_independent_backings_bytes)?,
            mul(streams, stream_extra)?,
        )?;
        if bound > envelope {
            return Err(invalid());
        }
        dimensions.connection_bound = bound;
        let positions = add(data_positions, control_positions)?;
        let slots = Layout::array::<ConnectionRecord>(positions)
            .map_err(|_| invalid())?
            .size();
        let mut stock = add(mul(bound, positions)?, arc_bytes::<StockCore>()?)?;
        stock = add(stock, slots)?;
        // Data and Control each have one independent listener registration,
        // including its original carrier. They do not borrow connection slots.
        stock = add(
            stock,
            mul(
                2,
                add(
                    registration,
                    Bytes::owner_with_exit_guard_metadata_size::<Bytes, ListenerRegistrationExit>(),
                )?,
            )?,
        )?;
        stock = add(
            stock,
            NativeConnectionKeyCapacity::additional_backing_bytes()?,
        )?;
        stock = add(
            stock,
            mul(
                add(
                    dimensions.data_acquisitions,
                    dimensions.control_acquisitions,
                )?,
                Bytes::owner_with_exit_guard_metadata_size::<Bytes, AcquisitionExit>(),
            )?,
        )?;
        stock = add(
            stock,
            NativeIncomingKeyCapacity::additional_backing_bytes()?,
        )?;
        // Each fixed physical record owns its lazy platform Mutex backing.
        stock = add(
            stock,
            mul(
                add(dimensions.data_positions, dimensions.control_positions)?,
                NativeIncomingKeyCapacity::additional_backing_bytes()?,
            )?,
        )?;
        // Exact process-credit callback capture at the existing issuer seam.
        // Callback retirement/observer backing belongs to that existing issuer;
        // this receipt does not claim the issuer's whole allocation graph.
        stock = add(stock, Layout::new::<Weak<ResultRetainedBudget>>().size())?;
        stock = add(
            stock,
            crate::native_channel_cache::NativeChannelCache::allocation_capacity_bound()?,
        )?;
        if stock > value(g.root_joint_retained_bytes_per_process)? {
            return Err(invalid());
        }
        dimensions.stock_bound = stock;
        Ok(dimensions)
    }
}

impl NativeTransportCapacityFactory {
    fn core(&self) -> &Arc<StockCore> {
        self.core
            .as_ref()
            .expect("live original transport stock handle")
    }

    /// Compute the complete startup stock receipt before growing root owners.
    /// The receipt covers maximum live pool backings, not eagerly allocated
    /// unused pool buffers, allocator caches, sockets, or enclosing callers.
    #[cfg(test)]
    pub fn allocation_capacity_bound() -> io::Result<usize> {
        Ok(Dimensions::frozen()?.stock_bound)
    }

    /// Pregrant the finite stock from the existing BE process retained budget.
    /// Must run before root producer startup and before dial/TLS/listener I/O.
    pub fn try_new(budget: Arc<ResultRetainedBudget>) -> io::Result<Self> {
        let dimensions = Dimensions::frozen()?;
        let admission = budget
            .try_reserve_process(dimensions.stock_bound)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let ResultWriteAdmission::Granted(credit) = admission else {
            return Err(io::ErrorKind::WouldBlock.into());
        };
        let positions = add(dimensions.data_positions, dimensions.control_positions)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(positions)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        for _ in 0..positions {
            let record = ConnectionRecord {
                claimed: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                phase: AtomicU8::new(VACANT),
                incoming: Mutex::new(None),
            };
            // Initialize the platform backing under the pregranted stock.
            drop(
                record
                    .incoming
                    .lock()
                    .map_err(|_| io::ErrorKind::InvalidData)?,
            );
            slots.push(record);
        }
        Ok(Self {
            core: Some(Arc::new(StockCore {
                slots,
                dimensions,
                acquisitions: [AtomicUsize::new(0), AtomicUsize::new(0)],
                listener_registrations: [AtomicBool::new(false), AtomicBool::new(false)],
                channel_cache_claimed: AtomicBool::new(false),
                connection_keys: NativeConnectionKeyCapacity::new()?,
                incoming_keys: NativeIncomingKeyCapacity::new()?,
                credit,
                _budget: budget,
            })),
        })
    }

    /// Startup bytes kept reserved through the final actual pool/carrier exit.
    pub fn reserved_bytes(&self) -> usize {
        self.core().credit.bytes()
    }

    /// Claim the already funded registration for this class's one listener.
    /// Must run before Tokio registers the listener. The registration keeps
    /// this capability through reactor retirement and its final physical exit.
    pub(crate) fn try_listener_registration_owner(
        &self,
        class: TransportClass,
    ) -> io::Result<Bytes> {
        self.core().listener_registrations[class_index(class)]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        Ok(Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            ListenerRegistrationExit {
                factory: self.clone(),
                class,
            },
        ))
    }

    pub(crate) fn validate_server_task_capacity(&self, actual_bound: usize) -> io::Result<()> {
        if actual_bound > self.core().dimensions.server_task_bound {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn validate_stream_task_capacity(&self, actual_bound: usize) -> io::Result<()> {
        if actual_bound > self.core().dimensions.stream_task_bound {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn server_stream_executor(
        &self,
        owner: Bytes,
    ) -> io::Result<crate::native_task_executor::NativeTaskExecutor> {
        let d = self.core().dimensions;
        crate::native_task_executor::NativeTaskExecutor::with_original(
            d.streams,
            d.stream_task_bound,
            owner,
        )
    }

    pub(crate) fn claim_channel_cache(&self) -> io::Result<()> {
        self.core()
            .channel_cache_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| io::ErrorKind::WouldBlock.into())
    }

    pub(crate) fn release_channel_cache(&self) {
        assert!(
            self.core()
                .channel_cache_claimed
                .swap(false, Ordering::AcqRel),
            "original cache backing exited once"
        );
    }

    /// Checked per-position backing receipt, including its carrier wrapper.
    pub fn connection_capacity_bytes(&self) -> usize {
        self.core().dimensions.connection_bound
    }

    fn range(&self, class: TransportClass) -> std::ops::Range<usize> {
        let d = self.core().dimensions;
        match class {
            TransportClass::Data => 0..d.data_positions,
            TransportClass::Control => d.data_positions..d.data_positions + d.control_positions,
        }
    }

    /// Fixed stock size for this class; it does not count actual handshakes.
    pub fn positions(&self, class: TransportClass) -> usize {
        self.range(class).len()
    }

    /// A concurrent snapshot of physically returned positions, not a grant.
    pub fn available_positions(&self, class: TransportClass) -> usize {
        self.range(class)
            .filter(|&index| {
                !self.core().slots[index].claimed.load(Ordering::Acquire)
                    && self.core().slots[index].generation.load(Ordering::Acquire) != u64::MAX
            })
            .count()
    }

    /// Shared incoming/outgoing acquisition positions, distinct from live pool
    /// stock. A snapshot does not itself authorize connector or listener I/O.
    #[cfg(test)]
    pub fn available_acquisitions(&self, class: TransportClass) -> usize {
        let capacity = self.acquisition_positions(class);
        let used = self.core().acquisitions[class_index(class)].load(Ordering::Acquire);
        capacity - used
    }

    pub fn acquisition_positions(&self, class: TransportClass) -> usize {
        let d = self.core().dimensions;
        match class {
            TransportClass::Data => d.data_acquisitions,
            TransportClass::Control => d.control_acquisitions,
        }
    }

    fn claim_acquisition(&self, class: TransportClass) -> io::Result<Bytes> {
        let capacity = self.acquisition_positions(class);
        self.core().acquisitions[class_index(class)]
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(1).filter(|&next| next <= capacity)
            })
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        Ok(Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            AcquisitionExit {
                factory: self.clone(),
                class,
            },
        ))
    }

    fn claim(
        &self,
        class: TransportClass,
        key: Option<InlineNativeChannelIdentity>,
    ) -> io::Result<(Bytes, usize, u64, Option<NativeConnectionKeyToken>)> {
        struct Rollback<'a> {
            capacity: &'a NativeConnectionKeyCapacity,
            token: Option<NativeConnectionKeyToken>,
        }
        impl Drop for Rollback<'_> {
            fn drop(&mut self) {
                if let Some(token) = self.token.take() {
                    self.capacity
                        .exit(token)
                        .expect("unpublished key claim exits once");
                }
            }
        }
        let mut rollback = Rollback {
            capacity: &self.core().connection_keys,
            token: key
                .map(|key| self.core().connection_keys.claim(key))
                .transpose()?,
        };
        for index in self.range(class) {
            let record = &self.core().slots[index];
            if record
                .claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let generation = match record.generation.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |generation| generation.checked_add(1),
                ) {
                    Ok(previous) => previous + 1,
                    Err(_) => {
                        // An exhausted generation never wraps into an old family.
                        record.claimed.store(false, Ordering::Release);
                        continue;
                    }
                };
                record.phase.store(ACQUIRING, Ordering::Release);
                let key_token = rollback.token.take();
                return Ok((
                    Bytes::from_owner_with_exit_guard(
                        Bytes::new(),
                        SlotExit {
                            factory: self.clone(),
                            index,
                            generation,
                            key_token,
                        },
                    ),
                    index,
                    generation,
                    key_token,
                ));
            }
        }
        Err(io::ErrorKind::WouldBlock.into())
    }

    /// Claim one prepaid original position and construct ten fresh capabilities.
    /// Reconnect must call this again. No exhausted class, map, field, or buffer
    /// obtains spare budget or falls back to ordinary unowned storage.
    /// Endpoint windows/adaptive mode/pending queue must be set by its caller.
    pub fn try_config(&self, class: TransportClass) -> io::Result<Http2ConnectionConfig> {
        self.try_config_inner(class, None)
    }

    /// Every BE-origin factory invocation, including internal Tonic reconnect,
    /// obtains its exact peer/lane position before constructing fresh pools.
    pub(crate) fn try_config_for_key(
        &self,
        class: TransportClass,
        key: InlineNativeChannelIdentity,
    ) -> io::Result<Http2ConnectionConfig> {
        if class != TransportClass::Data {
            return Err(invalid());
        }
        self.try_config_inner(class, Some(key))
    }

    pub(crate) fn try_incoming_config(
        &self,
        class: TransportClass,
    ) -> io::Result<(Http2ConnectionConfig, NativeIncomingConnectionBinding)> {
        self.try_config_bound(class, None)
    }

    fn try_config_inner(
        &self,
        class: TransportClass,
        key: Option<InlineNativeChannelIdentity>,
    ) -> io::Result<Http2ConnectionConfig> {
        self.try_config_bound(class, key).map(|(config, _)| config)
    }

    fn try_config_bound(
        &self,
        class: TransportClass,
        key: Option<InlineNativeChannelIdentity>,
    ) -> io::Result<(Http2ConnectionConfig, NativeIncomingConnectionBinding)> {
        let d = self.core().dimensions;
        let acquisition_owner = self.claim_acquisition(class)?;
        let (owner, index, generation, key_token) = self.claim(class, key)?;
        let connection_lifecycle = h2::ConnectionLifecycle::new(
            NativeLifecycleObserver {
                factory: self.clone(),
                index,
                generation,
                key_token,
            },
            owner.clone(),
        )?;
        let binding = NativeIncomingConnectionBinding {
            factory: self.clone(),
            index,
            generation,
            _original: owner.clone(),
        };
        Ok((
            Http2ConnectionConfig {
                acquisition_owner: Some(acquisition_owner),
                io_owner: Some(owner.clone()),
                connection_lifecycle: Some(connection_lifecycle),
                stream_store_buffer: Some(h2::StreamStoreBuffer::new(
                    d.streams,
                    d.streams,
                    owner.clone(),
                )?),
                initial_settings_timeout: Some(std::time::Duration::from_millis(
                    NativeResultSupportGeometry::V1.transport_handshake_deadline_ms,
                )),
                max_frame_size: Some(d.frame as u32),
                max_header_list_size: Some(d.header as u32),
                max_receive_header_block_size: Some(d.header),
                header_table_size: Some(0),
                max_send_header_table_size: Some(0),
                send_header_block_pool: Some(h2::SendHeaderBlockPool::new(
                    d.header,
                    owner.clone(),
                )?),
                max_receive_buffered_events: Some(d.events),
                max_send_buffer_size: Some(d.writer),
                retain_data_payloads: true,
                receive_buffer_pool: Some(h2::ReceiveBufferPool::new(
                    d.data_buffers,
                    d.frame,
                    owner.clone(),
                )?),
                receive_frame_buffer: Some(h2::ReceiveFrameBuffer::new(d.frame, owner.clone())?),
                receive_header_block_buffer: Some(h2::ReceiveHeaderBlockBuffer::new(
                    d.header,
                    owner.clone(),
                )?),
                receive_header_field_pool: Some(h2::ReceiveHeaderFieldPool::new(
                    d.field_bytes,
                    d.field_positions,
                    d.header - 32,
                    owner.clone(),
                )?),
                receive_header_table_buffer: Some(h2::ReceiveHeaderTableBuffer::new(
                    d.table_bytes,
                    owner.clone(),
                )?),
                receive_header_map_pool: Some(
                    HeaderMapAllocationPool::new(d.maps, d.map_keys, d.map_extras, owner.clone())
                        .map_err(|_| invalid())?,
                ),
                send_frame_buffer: Some(h2::SendFrameBuffer::new(
                    d.writer,
                    d.frame,
                    owner.clone(),
                )?),
                receive_goaway_buffer_pool: Some(h2::ReceiveBufferPool::new(
                    d.goaway_buffers,
                    d.frame,
                    owner,
                )?),
            },
            binding,
        ))
    }
}

/// Install a factory configuration on an accepted Hyper server connection.
/// All validation precedes builder mutation. Binding remains h2's once-only
/// handshake operation; this helper performs no I/O and supplies no TLS owner.
/// Pass only a configuration returned by this factory: public pool dimensions
/// do not attest the construction receipt of arbitrary externally supplied pools.
pub fn configure_server<E>(
    builder: &mut hyper::server::conn::http2::Builder<E>,
    config: &Http2ConnectionConfig,
) -> io::Result<()> {
    let d = Dimensions::frozen()?;
    let g = NativeResultSupportGeometry::V1;
    if config.io_owner.is_none()
        || config.connection_lifecycle.is_none()
        || config.acquisition_owner.is_none()
        || config.max_frame_size != Some(d.frame as u32)
        || config.initial_settings_timeout
            != Some(std::time::Duration::from_millis(
                g.transport_handshake_deadline_ms,
            ))
        || config.max_header_list_size != Some(d.header as u32)
        || config.max_receive_header_block_size != Some(d.header)
        || config.header_table_size != Some(0)
        || config.max_send_header_table_size != Some(0)
        || config.max_receive_buffered_events != Some(d.events)
        || config.max_send_buffer_size != Some(d.writer)
        || !config.retain_data_payloads
        || config
            .receive_frame_buffer
            .as_ref()
            .is_none_or(|p| p.max_payload_bytes() != d.frame)
        || config
            .receive_header_block_buffer
            .as_ref()
            .is_none_or(|p| p.max_encoded_bytes() != d.header)
        || config.receive_header_field_pool.as_ref().is_none_or(|p| {
            p.capacity_bytes() != d.field_bytes
                || p.field_positions() != d.field_positions
                || p.max_field_bytes() != d.header - 32
        })
        || config
            .receive_header_table_buffer
            .as_ref()
            .is_none_or(|p| p.max_table_bytes() != d.table_bytes)
        || config
            .send_header_block_pool
            .as_ref()
            .is_none_or(|p| p.max_header_list_size() != d.header)
        || config
            .send_frame_buffer
            .as_ref()
            .is_none_or(|p| p.capacity_bytes() != d.writer || p.max_payload_bytes() != d.frame)
        || config.receive_buffer_pool.as_ref().is_none_or(|p| {
            p.buffer_positions() != d.data_buffers || p.buffer_capacity_bytes() != d.frame
        })
        || config.receive_goaway_buffer_pool.as_ref().is_none_or(|p| {
            p.buffer_positions() != d.goaway_buffers || p.buffer_capacity_bytes() != d.frame
        })
        || config.stream_store_buffer.as_ref().is_none_or(|buffer| {
            buffer.max_resident_streams() != d.streams || buffer.max_waiters() != d.streams
        })
        || config.receive_header_map_pool.is_none()
    {
        return Err(invalid());
    }
    config
        .connection_lifecycle
        .as_ref()
        .unwrap()
        .retain_acquisition_owner(config.acquisition_owner.as_ref().unwrap().clone())?;
    builder
        .initial_stream_window_size(g.transport_h2_stream_receive_window_bytes as u32)
        .initial_connection_window_size(g.transport_h2_connection_receive_window_bytes as u32)
        .adaptive_window(g.transport_h2_adaptive_window)
        .max_concurrent_streams(d.streams as u32)
        .max_pending_accept_reset_streams(g.transport_h2_pending_resets as usize)
        .max_local_error_reset_streams(g.transport_h2_pending_resets as usize)
        .max_frame_size(d.frame as u32)
        .max_header_list_size(d.header as u32)
        .max_receive_header_block_size(d.header)
        .header_table_size(0)
        .max_send_header_table_size(0)
        .max_receive_buffered_events(d.events)
        .max_send_buf_size(d.writer)
        .retain_data_payloads(true);
    builder.connection_lifecycle(config.connection_lifecycle.as_ref().unwrap().clone());
    builder.stream_store_buffer(config.stream_store_buffer.as_ref().unwrap().clone());
    builder.receive_frame_buffer(config.receive_frame_buffer.as_ref().unwrap().clone());
    builder
        .receive_header_block_buffer(config.receive_header_block_buffer.as_ref().unwrap().clone());
    builder.receive_header_field_pool(config.receive_header_field_pool.as_ref().unwrap().clone());
    builder
        .receive_header_table_buffer(config.receive_header_table_buffer.as_ref().unwrap().clone());
    builder.receive_header_map_pool(config.receive_header_map_pool.as_ref().unwrap().clone());
    builder.send_header_block_pool(config.send_header_block_pool.as_ref().unwrap().clone());
    builder.send_frame_buffer(config.send_frame_buffer.as_ref().unwrap().clone());
    builder.receive_buffer_pool(config.receive_buffer_pool.as_ref().unwrap().clone());
    builder.receive_goaway_buffer_pool(config.receive_goaway_buffer_pool.as_ref().unwrap().clone());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::http::{HeaderMap, HeaderValue};
    use std::convert::Infallible;
    use std::num::NonZeroUsize;

    fn factory() -> (
        NativeTransportCapacityFactory,
        Arc<ResultRetainedBudget>,
        usize,
    ) {
        let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
        (
            NativeTransportCapacityFactory::try_new(budget.clone()).unwrap(),
            budget,
            bytes,
        )
    }

    fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
        assert!(matches!(
            budget.try_reserve_process(bytes).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }

    fn released(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap()
        else {
            panic!("original stock must be physically returned");
        };
        drop(credit);
    }

    #[test]
    fn actual_lifecycle_phases_hold_position_until_final_original_alias_exit() {
        let (factory, budget, bytes) = factory();
        let index = factory.range(TransportClass::Data).start;
        let record = &factory.core().slots[index];
        let config = factory.try_config(TransportClass::Data).unwrap();
        let lifecycle = config.connection_lifecycle.as_ref().unwrap().clone();
        let lease = lifecycle.bind().unwrap();
        assert_eq!(record.generation.load(Ordering::Acquire), 1);
        assert_eq!(record.phase.load(Ordering::Acquire), ACQUIRING);
        assert!(lifecycle.on_acquisition_complete().is_err());
        lease.on_initial_settings_complete().unwrap();
        assert_eq!(record.phase.load(Ordering::Acquire), INITIAL_COMPLETE);
        lifecycle.on_acquisition_complete().unwrap();
        assert_eq!(record.phase.load(Ordering::Acquire), LIVE);
        let pool_alias = config.receive_frame_buffer.as_ref().unwrap().clone();
        drop(config);
        drop(lease);
        assert_eq!(record.phase.load(Ordering::Acquire), RETIRING);
        assert!(lifecycle.on_acquisition_complete().is_err());
        drop(lifecycle);
        assert_eq!(factory.available_positions(TransportClass::Data), 517);
        assert_eq!(record.phase.load(Ordering::Acquire), RETIRING);
        drop(pool_alias);
        assert_eq!(record.phase.load(Ordering::Acquire), VACANT);
        assert_eq!(factory.available_positions(TransportClass::Data), 518);
        drop(factory);
        released(&budget, bytes);
    }

    #[test]
    fn returned_position_changes_generation_and_refuses_old_observer_events() {
        use h2::ConnectionLifecycleObserver;
        let (factory, budget, bytes) = factory();
        let index = factory.range(TransportClass::Control).start;
        let first = factory.try_config(TransportClass::Control).unwrap();
        let stale = NativeLifecycleObserver {
            factory: factory.clone(),
            index,
            generation: 1,
            key_token: None,
        };
        drop(first);
        let second = factory.try_config(TransportClass::Control).unwrap();
        let record = &factory.core().slots[index];
        assert_eq!(record.generation.load(Ordering::Acquire), 2);
        assert_eq!(record.phase.load(Ordering::Acquire), ACQUIRING);
        assert_eq!(
            stale.on_initial_settings_complete().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            stale.on_acquisition_complete().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            stale.on_retiring().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(stale);
        assert_eq!(record.phase.load(Ordering::Acquire), ACQUIRING);
        drop(second);
        drop(factory);
        released(&budget, bytes);
    }

    #[test]
    fn exhausted_physical_generation_never_wraps_and_rolls_back_acquisition() {
        let (factory, budget, bytes) = factory();
        for index in factory.range(TransportClass::Data) {
            factory.core().slots[index]
                .generation
                .store(u64::MAX, Ordering::Release);
        }
        assert_eq!(factory.available_positions(TransportClass::Data), 0);
        assert_eq!(
            factory.try_config(TransportClass::Data).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(factory.available_acquisitions(TransportClass::Data), 32);
        for index in factory.range(TransportClass::Data) {
            let record = &factory.core().slots[index];
            assert!(!record.claimed.load(Ordering::Acquire));
            assert_eq!(record.generation.load(Ordering::Acquire), u64::MAX);
            assert_eq!(record.phase.load(Ordering::Acquire), VACANT);
        }
        let control = factory.try_config(TransportClass::Control).unwrap();
        drop(control);
        drop(factory);
        released(&budget, bytes);
    }

    #[test]
    fn checked_stock_receipt_fits_frozen_process_and_connection_envelopes() {
        let d = Dimensions::frozen().unwrap();
        eprintln!(
            "Checked original stock: connection_pool_bytes={} process_stock_bytes={} data_positions={} control_positions={} server_task_backings_bytes={} stream_task_backings_bytes={} stream_task_pool_bytes={}",
            d.connection_bound,
            d.stock_bound,
            d.data_positions,
            d.control_positions,
            d.server_task_bound,
            d.stream_task_bound,
            crate::native_task_executor::NativeTaskExecutor::allocation_capacity_bound(
                d.streams,
                d.stream_task_bound,
            )
            .unwrap()
        );
        assert_eq!((d.data_positions, d.control_positions), (518, 20));
        assert_eq!(
            (d.maps, d.data_buffers, d.field_positions),
            (1024, 64, 4096)
        );
        assert!(d.connection_bound <= 2 * 1024 * 1024 + 128 * 44 * 1024);
        assert!(d.stock_bound <= 4 * 1024 * 1024 * 1024);
        assert!(add(usize::MAX, 1).is_err());
        assert!(mul(usize::MAX, 2).is_err());
    }

    #[test]
    fn startup_refuses_short_original_budget_without_partial_stock() {
        let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes - 1).unwrap());
        assert!(NativeTransportCapacityFactory::try_new(budget.clone()).is_err());
        released(&budget, bytes - 1);
    }

    #[test]
    fn strong_handle_clones_keep_one_original_startup_grant() {
        let (factory, budget, bytes) = factory();
        let alias = factory.clone();
        assert_eq!(factory.reserved_bytes(), bytes);
        assert_eq!(alias.reserved_bytes(), bytes);
        assert_eq!(
            factory.connection_capacity_bytes(),
            alias.connection_capacity_bytes()
        );
        drop(factory);
        held(&budget, bytes);
        assert_eq!(alias.available_positions(TransportClass::Data), 518);
        drop(alias);
        released(&budget, bytes);
    }

    #[test]
    fn control_and_data_stock_cannot_borrow_or_fall_back() {
        let (factory, budget, bytes) = factory();
        let leases: Vec<_> = (0..518)
            .map(|_| factory.claim(TransportClass::Data, None).unwrap())
            .collect();
        assert_eq!(factory.available_positions(TransportClass::Data), 0);
        assert_eq!(
            factory.try_config(TransportClass::Data).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            factory.available_acquisitions(TransportClass::Data),
            32,
            "stock refusal must roll back its preceding acquisition claim"
        );
        assert_eq!(factory.available_positions(TransportClass::Control), 20);
        let control = factory.try_config(TransportClass::Control).unwrap();
        assert_eq!(factory.available_positions(TransportClass::Control), 19);
        held(&budget, bytes);
        drop(leases);
        assert_eq!(factory.available_positions(TransportClass::Data), 518);
        drop(control);
        assert_eq!(factory.available_positions(TransportClass::Control), 20);
        drop(factory);
        released(&budget, bytes);
    }

    #[tokio::test]
    async fn exhausted_actual_outbound_endpoint_never_calls_connector() {
        let (factory, budget, bytes) = factory();
        let leases: Vec<_> = (0..factory.positions(TransportClass::Data))
            .map(|_| factory.claim(TransportClass::Data, None).unwrap())
            .collect();
        let runtime = crate::backend_test_support::test_backend_data_runtime()
            .with_transport_capacity(factory.clone())
            .unwrap();
        let endpoint = novarocks_types::NativeEndpoint::from_host_port("127.0.0.1", 1).unwrap();
        let endpoint =
            crate::native_client::capacity_endpoint(&runtime, &endpoint, TransportClass::Data)
                .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let connector = tower::service_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<hyper_util::rt::TokioIo<tokio::io::DuplexStream>, _>(io::Error::from(
                    io::ErrorKind::ConnectionRefused,
                ))
            }
        });
        assert!(endpoint.connect_with_connector(connector).await.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "stock refusal must precede TCP/TLS connector I/O"
        );
        assert_eq!(factory.available_positions(TransportClass::Control), 20);
        held(&budget, bytes);
        drop(leases);
        assert_eq!(factory.available_positions(TransportClass::Data), 518);
        drop(endpoint);
        drop(runtime);
        drop(factory);
        released(&budget, bytes);
    }

    #[test]
    fn final_independent_header_value_alias_retains_slot_and_startup_credit() {
        let (factory, budget, bytes) = factory();
        let config = factory.try_config(TransportClass::Data).unwrap();
        let fields = config
            .receive_header_field_pool
            .as_ref()
            .unwrap()
            .allocation_pool();
        let maps = config.receive_header_map_pool.as_ref().unwrap();
        fields.try_bind_once().unwrap();
        maps.try_bind_connection_with_fields(fields).unwrap();
        let field = fields
            .try_fill(9, |out| {
                out.copy_from_slice(b"original!");
                Ok::<_, Infallible>(())
            })
            .unwrap();
        let value = HeaderValue::from_maybe_shared(field).unwrap();
        let alias = value.clone();
        let mut headers = HeaderMap::try_from_allocation_pool(maps).unwrap();
        headers.try_insert("x-original", value).unwrap();
        drop(config);
        assert_eq!(factory.available_positions(TransportClass::Data), 517);
        drop(headers);
        assert_eq!(factory.available_positions(TransportClass::Data), 517);
        assert_eq!(alias.as_bytes(), b"original!");
        held(&budget, bytes);
        drop(factory);
        held(&budget, bytes);
        drop(alias);
        released(&budget, bytes);
    }

    #[test]
    fn returned_position_constructs_fresh_once_bound_capabilities() {
        let (factory, budget, bytes) = factory();
        let first = factory.try_config(TransportClass::Control).unwrap();
        let raw = first.receive_frame_buffer.as_ref().unwrap();
        let encoded = first.receive_header_block_buffer.as_ref().unwrap();
        // Public geometry alone does not bind raw/encoded buffers. Bind the
        // original field/map families through their actual transport APIs.
        assert_eq!(raw.max_payload_bytes(), 16384);
        assert_eq!(encoded.max_encoded_bytes(), 16384);
        let fields = first
            .receive_header_field_pool
            .as_ref()
            .unwrap()
            .allocation_pool();
        fields.try_bind_once().unwrap();
        assert!(fields.try_bind_once().is_err());
        let map_alias = first.receive_header_map_pool.as_ref().unwrap().clone();
        map_alias.try_bind_connection_with_fields(fields).unwrap();
        assert!(map_alias.try_bind_connection().is_err());
        drop(first);
        assert_eq!(factory.available_positions(TransportClass::Control), 19);
        drop(map_alias);
        assert_eq!(factory.available_positions(TransportClass::Control), 20);
        let second = factory.try_config(TransportClass::Control).unwrap();
        second
            .receive_header_field_pool
            .as_ref()
            .unwrap()
            .allocation_pool()
            .try_bind_once()
            .unwrap();
        second
            .receive_header_map_pool
            .as_ref()
            .unwrap()
            .try_bind_connection_with_fields(
                second
                    .receive_header_field_pool
                    .as_ref()
                    .unwrap()
                    .allocation_pool(),
            )
            .unwrap();
        drop(second);
        drop(factory);
        released(&budget, bytes);
    }

    #[test]
    fn independent_listener_registration_aliases_keep_the_original_startup_stock() {
        let (factory, budget, bytes) = factory();
        let data = factory
            .try_listener_registration_owner(TransportClass::Data)
            .unwrap();
        let control = factory
            .try_listener_registration_owner(TransportClass::Control)
            .unwrap();
        assert_eq!(
            factory
                .try_listener_registration_owner(TransportClass::Data)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let alias = data.clone();
        drop(data);
        assert!(
            factory
                .try_listener_registration_owner(TransportClass::Data)
                .is_err()
        );
        drop(alias);
        let data = factory
            .try_listener_registration_owner(TransportClass::Data)
            .unwrap();
        drop(factory);
        held(&budget, bytes);
        drop(control);
        held(&budget, bytes);
        drop(data);
        released(&budget, bytes);
    }

    #[test]
    fn original_io_alias_holds_physical_position_until_its_last_exit() {
        let (factory, budget, bytes) = factory();
        let mut config = factory.try_config(TransportClass::Data).unwrap();
        let owner = config.io_owner.take().unwrap();
        let alias = owner.clone();
        drop(config);
        drop(owner);
        assert_eq!(factory.available_positions(TransportClass::Data), 517);
        drop(alias);
        assert_eq!(factory.available_positions(TransportClass::Data), 518);
        drop(factory);
        released(&budget, bytes);
    }

    #[test]
    fn server_configuration_rejects_missing_original_io_before_mutation() {
        let (factory, budget, bytes) = factory();
        let mut config = factory.try_config(TransportClass::Data).unwrap();
        let mut builder =
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        let owner = config.io_owner.take().unwrap();
        assert_eq!(
            configure_server(&mut builder, &config).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        config.io_owner = Some(owner);
        configure_server(&mut builder, &config).unwrap();
        drop(config);
        assert_eq!(factory.available_positions(TransportClass::Data), 517);
        drop(builder);
        assert_eq!(factory.available_positions(TransportClass::Data), 518);
        drop(factory);
        released(&budget, bytes);
    }

    #[test]
    fn server_configuration_rejects_missing_original_capability_before_mutation() {
        let (factory, budget, bytes) = factory();
        let mut config = factory.try_config(TransportClass::Data).unwrap();
        let mut builder =
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        configure_server(&mut builder, &config).unwrap();
        let fields = config.receive_header_field_pool.take().unwrap();
        assert_eq!(
            configure_server(&mut builder, &config).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        drop(config);
        drop(fields);
        // The successfully configured builder still owns all original pools.
        assert_eq!(factory.available_positions(TransportClass::Data), 517);
        drop(builder);
        assert_eq!(factory.available_positions(TransportClass::Data), 518);
        drop(factory);
        released(&budget, bytes);
    }
}
