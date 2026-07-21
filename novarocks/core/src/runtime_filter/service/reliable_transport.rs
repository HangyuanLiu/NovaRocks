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

//! Sender-side reliable transport for the delivery Router's remote leg.
//!
//! Every remote envelope the Service emits (an artifact bundle or an `Unavailable`
//! sentinel today; `Contribution`/`ProducerClosed` once RFD-6 converges the
//! producer leg here) flows through this query-scoped transport instead of going
//! fire-and-forget. The transport:
//!
//! * buffers each already-serialized [`EncodedArtifactFrame`] keyed by its delivery
//!   route identity (`route_edge_id` + a transport-assigned monotonic `sequence`),
//!   holding the frame behind an [`Arc`] so one logical envelope that fans out to
//!   several routes is serialized once and shared, then acked per route;
//! * releases a buffered frame when its ack arrives ([`Self::on_ack`]) — `Accepted`
//!   and `Duplicate` both release and never re-transmit; `Rejected` releases but is
//!   surfaced as a running-contract corruption rather than silently swallowed;
//! * re-hands unacked frames to the underlying sink on an explicit tick
//!   ([`Self::drive_retries`]) under a bounded attempt count, and, once a frame
//!   outlives its deadline, drops it and reports the route as *failed open* — the
//!   route degrades but the query neither errors nor panics (runtime filters are an
//!   optimization, never a correctness dependency).
//!
//! Retry and deadline timing are driven by the injected [`RuntimeFilterClock`] and
//! an explicit `drive_retries(now)` call: there is no background thread and no live
//! timer here. The live network sender and the production tick loop are RFD-6; this
//! task builds only the substrate and exercises it through an injectable fake sink.
//!
//! The transport is kind-agnostic: it keys, waits, and releases purely by delivery
//! route identity. It never inspects the producer's semantic kind (Join / TopN /
//! aggregate) to decide routing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::runtime_filter::codec::artifact::EncodedArtifactFrame;
use crate::runtime_filter::port::identity::{ProducerSequence, RouteEdgeId};
use crate::runtime_filter::port::routing::RuntimeFilterRemoteRoute;
use crate::runtime_filter::port::support::RuntimeFilterClock;
use crate::runtime_filter::port::transport::{
    DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterRouteIdentity,
};
use crate::runtime_filter::router::remote::ArtifactRemoteSink;

/// Bounded retry / deadline / buffer policy for the reliable transport.
///
/// `max_attempts` caps the total number of times a frame is handed to the sink
/// (the initial send plus retries), bounding network chatter. `deadline` caps how
/// long a frame may stay buffered before it is released and the route fails open,
/// bounding buffer lifetime. The two limits are independent: exhausting the attempt
/// count stops re-transmission but keeps the frame buffered until the deadline, so
/// an ack that finally arrives before the deadline still releases cleanly.
///
/// `max_pending_entries` and `max_pending_bytes` are the M3 Task 4 self-owned buffer
/// ceilings. They bound the sender-side buffer purely through the transport's OWN
/// counters — RF buffer memory is deliberately NOT wired into the global MemTracker
/// this milestone. Offering a frame that would exceed either returns
/// [`ReliableSendOutcome::ResourceLimit`] instead of buffering: an explicit resource
/// rejection, distinct from the deadline fail-open degradation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReliableTransportPolicy {
    retry_interval: Duration,
    max_attempts: u32,
    deadline: Duration,
    max_pending_entries: usize,
    max_pending_bytes: usize,
}

impl ReliableTransportPolicy {
    pub(crate) fn new(
        retry_interval: Duration,
        max_attempts: u32,
        deadline: Duration,
        max_pending_entries: usize,
        max_pending_bytes: usize,
    ) -> Self {
        assert!(
            max_attempts >= 1,
            "reliable transport must allow at least the initial send"
        );
        assert!(
            max_pending_entries >= 1,
            "reliable transport must be able to buffer at least one frame"
        );
        Self {
            retry_interval,
            max_attempts,
            deadline,
            max_pending_entries,
            max_pending_bytes,
        }
    }
}

// Sane defaults for the not-yet-wired production driver. RFD-6 sources the real
// values from the query deadline and cluster RPC policy; until the live sender and
// tick loop exist these constants only shape test-free production construction.
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

// Self-owned buffer ceilings (M3 Task 4). These are query-scoped SOFTWARE SAFETY
// caps, not cluster-topology quantities: they must NOT be sized to the live BE count
// (no single-BE assumption), so they are generous fixed constants that a healthy
// broadcast fan-out and its unacked backlog stay far below. They exist only to stop
// pathological unbounded growth (a retry storm, a peer that never acks). Bounding is
// self-owned via the transport's own counters; RF buffer memory stays out of the
// global MemTracker this milestone.
//
// `DEFAULT_MAX_PENDING_ENTRIES`: an in-flight backlog this deep (65536 unacked remote
// deliveries for one query, across all its channels) is already pathological; real
// ack cadence keeps the live buffer far smaller even when broadcasting to a large
// cluster.
const DEFAULT_MAX_PENDING_ENTRIES: usize = 1 << 16;
// `DEFAULT_MAX_PENDING_BYTES`: 256 MiB of DISTINCT buffered serialized frames per
// query. Each frame is itself bounded by its channel's `max_artifact_bytes` wire
// ceiling, and a broadcast frame shared across routes is metered once (see
// `PendingBuffer`), so a handful of in-flight artifact versions stay well under this.
const DEFAULT_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

impl Default for ReliableTransportPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_RETRY_INTERVAL,
            DEFAULT_MAX_ATTEMPTS,
            DEFAULT_DEADLINE,
            DEFAULT_MAX_PENDING_ENTRIES,
            DEFAULT_MAX_PENDING_BYTES,
        )
    }
}

/// Buffer key: a delivery route identity reduced to its hashable coordinates. Both
/// components are non-zero (the route edge id is validated at route construction and
/// the transport assigns sequences starting at 1), so it round-trips losslessly to a
/// [`DeliveryRouteIdentity`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PendingKey {
    route_edge_id: RouteEdgeId,
    sequence: ProducerSequence,
}

impl PendingKey {
    fn from_delivery(identity: &DeliveryRouteIdentity) -> Self {
        Self {
            route_edge_id: identity.route_edge_id(),
            sequence: identity.sequence(),
        }
    }

    fn into_route_identity(self) -> RuntimeFilterRouteIdentity {
        RuntimeFilterRouteIdentity::delivery(
            DeliveryRouteIdentity::try_new(self.route_edge_id, self.sequence)
                .expect("pending keys carry validated non-zero delivery coordinates"),
        )
    }
}

/// A buffered in-flight frame awaiting acknowledgement.
struct PendingEntry {
    frame: Arc<EncodedArtifactFrame>,
    route: RuntimeFilterRemoteRoute,
    attempts: u32,
    first_sent_at: Instant,
    last_sent_at: Instant,
}

/// Which self-owned transport ceiling a `send` would exceed. Kept distinct so a
/// later task's structured degradation event can name the limit that tripped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportResourceLimit {
    /// Buffering another frame would exceed the pending-entry count ceiling.
    PendingEntries,
    /// Buffering another distinct frame would exceed the buffered serialized-byte
    /// ceiling.
    SerializedBytes,
}

/// Outcome of offering a frame to the reliable transport ([`ReliableEnvelopeTransport::send`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReliableSendOutcome {
    /// Buffered for ack-release + bounded retry and handed to the sink once; carries
    /// the delivery route identity the transport stamped so a later ack can address
    /// exactly this in-flight frame.
    Buffered(RuntimeFilterRouteIdentity),
    /// Refused: a self-owned ceiling (pending-entry count or buffered serialized
    /// bytes) would be exceeded. The frame was NOT buffered and NOT put on the wire.
    /// This is an EXPLICIT resource rejection — a first-class outcome, not a silent
    /// drop and not the deadline fail-open degradation. The caller degrades the route.
    ResourceLimit(TransportResourceLimit),
}

#[cfg(test)]
impl ReliableSendOutcome {
    /// The stamped delivery identity of a buffered send, or a panic if the send was
    /// refused. Test convenience for the common "expected to buffer" call site.
    fn expect_buffered(self) -> RuntimeFilterRouteIdentity {
        match self {
            ReliableSendOutcome::Buffered(identity) => identity,
            ReliableSendOutcome::ResourceLimit(limit) => {
                panic!("expected a buffered send, got ResourceLimit({limit:?})")
            }
        }
    }
}

/// The query-scoped in-flight buffer plus its self-owned counters.
///
/// Byte metering is per unique frame ALLOCATION, not per entry: a broadcast frame
/// that fans out to several routes is one [`Arc`] allocation shared across entries,
/// so its serialized bytes are counted once. Counting per entry would count a shared
/// frame N times, misrepresenting real memory and rejecting legitimate wide fan-out.
/// `frame_refs` keys on the frame allocation address (`Arc::as_ptr` as `usize`); while
/// a frame has at least one buffered entry we hold an `Arc` to it, so its address is
/// stable and unique, and `bytes` is adjusted only on the 0<->1 reference transition.
#[derive(Default)]
struct PendingBuffer {
    entries: HashMap<PendingKey, PendingEntry>,
    bytes: usize,
    frame_refs: HashMap<usize, usize>,
}

impl PendingBuffer {
    /// Admit a new entry under the ceilings. On success the entry is inserted and its
    /// frame's bytes are metered (once per unique allocation); on a ceiling breach
    /// nothing is inserted and the tripped limit is returned. Every `send` stamps a
    /// fresh monotonic sequence, so `key` is always genuinely new — this never
    /// overwrites, so idempotency is not a concern on the sender buffer.
    fn admit(
        &mut self,
        key: PendingKey,
        entry: PendingEntry,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(), TransportResourceLimit> {
        if self.entries.len() >= max_entries {
            return Err(TransportResourceLimit::PendingEntries);
        }
        let allocation = Arc::as_ptr(&entry.frame) as usize;
        let is_new_allocation = !self.frame_refs.contains_key(&allocation);
        // Only a genuinely-new allocation adds bytes; a broadcast frame already
        // buffered for another route adds none, so wide fan-out of one frame is never
        // byte-rejected once the first route fits.
        let added = if is_new_allocation {
            entry.frame.payload().len()
        } else {
            0
        };
        if self.bytes.saturating_add(added) > max_bytes {
            return Err(TransportResourceLimit::SerializedBytes);
        }
        *self.frame_refs.entry(allocation).or_insert(0) += 1;
        self.bytes += added;
        // Underscore-bound so the assertion's variable is not flagged unused in release
        // builds, where `debug_assert!` compiles out.
        let _previous = self.entries.insert(key, entry);
        debug_assert!(
            _previous.is_none(),
            "reliable transport stamps a fresh sequence per send, so keys are unique"
        );
        Ok(())
    }

    /// Release the accounting for an entry's frame: drop one reference, and when the
    /// last reference to an allocation goes away, reclaim its bytes.
    fn release(&mut self, frame: &Arc<EncodedArtifactFrame>) {
        let allocation = Arc::as_ptr(frame) as usize;
        if let Some(refs) = self.frame_refs.get_mut(&allocation) {
            *refs -= 1;
            if *refs == 0 {
                self.frame_refs.remove(&allocation);
                self.bytes = self.bytes.saturating_sub(frame.payload().len());
            }
        }
    }
}

/// The result of applying an ack to the buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnvelopeAckOutcome {
    /// The buffered frame was released after an `Accepted` ack.
    Released,
    /// The peer reported it had already seen this delivery; the frame is released
    /// and never re-transmitted.
    ReleasedOnDuplicate,
    /// The peer rejected the delivery. The frame is released (retry stops), but this
    /// is a running-contract corruption for the route, surfaced rather than swallowed
    /// — a later task turns it into a structured event.
    Rejected,
    /// No buffered frame matched the acked identity: a duplicate or out-of-order ack
    /// for an entry already released. A no-op.
    Unknown,
}

/// The result of one [`ReliableEnvelopeTransport::drive_retries`] tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReliableTransportTick {
    retried: usize,
    failed_open: Vec<RuntimeFilterRouteIdentity>,
}

impl ReliableTransportTick {
    /// How many buffered frames were re-handed to the sink on this tick.
    pub(crate) fn retried(&self) -> usize {
        self.retried
    }

    /// The delivery route identities that outlived their deadline on this tick and
    /// were released. Their routes are degraded (failed open); the query is
    /// unaffected. A later task emits a structured degradation event for each.
    pub(crate) fn failed_open(&self) -> &[RuntimeFilterRouteIdentity] {
        &self.failed_open
    }

    /// True when the tick neither retried nor failed anything open.
    pub(crate) fn is_quiescent(&self) -> bool {
        self.retried == 0 && self.failed_open.is_empty()
    }
}

/// Production placeholder sink. The live network sender lands in RFD-6; until then
/// the remote leg still buffers for ack-release and retry but transmits into a
/// no-op, so nothing is actually put on the wire yet.
struct InertArtifactRemoteSink;

impl ArtifactRemoteSink for InertArtifactRemoteSink {
    fn deliver_remote(&self, _route: &RuntimeFilterRemoteRoute, _frame: &EncodedArtifactFrame) {}
}

/// Query-scoped sender-side reliable transport. See the module docs for the model.
pub(crate) struct ReliableEnvelopeTransport {
    sink: Arc<dyn ArtifactRemoteSink>,
    // Test-only override so a service assembled with the inert production sink can be
    // pointed at a recording / drivable fake without threading the sink through the
    // ~30 `new_with_dependencies` call sites. Mirrors the service's other
    // `Mutex<Option<..>>` test seams.
    #[cfg(test)]
    sink_override: Mutex<Option<Arc<dyn ArtifactRemoteSink>>>,
    clock: Arc<dyn RuntimeFilterClock>,
    policy: ReliableTransportPolicy,
    pending: Mutex<PendingBuffer>,
    next_sequence: AtomicU64,
}

impl ReliableEnvelopeTransport {
    pub(crate) fn new(
        sink: Arc<dyn ArtifactRemoteSink>,
        clock: Arc<dyn RuntimeFilterClock>,
        policy: ReliableTransportPolicy,
    ) -> Self {
        Self {
            sink,
            #[cfg(test)]
            sink_override: Mutex::new(None),
            clock,
            policy,
            pending: Mutex::new(PendingBuffer::default()),
            next_sequence: AtomicU64::new(1),
        }
    }

    /// Assemble the production transport for a query: the inert sink (no live sender
    /// until RFD-6) and the default bounded-retry policy.
    pub(crate) fn for_query(clock: Arc<dyn RuntimeFilterClock>) -> Self {
        Self::new(
            Arc::new(InertArtifactRemoteSink),
            clock,
            ReliableTransportPolicy::default(),
        )
    }

    /// Offer `frame` for reliable delivery to `route`: buffer it for ack-release and
    /// bounded retry and hand it to the underlying sink once, UNLESS a self-owned
    /// buffer ceiling would be exceeded.
    ///
    /// On success returns [`ReliableSendOutcome::Buffered`] with the delivery route
    /// identity the transport stamped, so an ack can later address exactly this
    /// in-flight frame. When the pending-entry count or the buffered serialized-byte
    /// ceiling would be exceeded the frame is NOT buffered and NOT transmitted, and
    /// [`ReliableSendOutcome::ResourceLimit`] is returned — an explicit resource
    /// rejection the caller degrades the route on, distinct from the deadline fail-open.
    pub(crate) fn send(
        &self,
        route: &RuntimeFilterRemoteRoute,
        frame: Arc<EncodedArtifactFrame>,
    ) -> ReliableSendOutcome {
        let sequence = ProducerSequence::new(self.next_sequence.fetch_add(1, Ordering::Relaxed));
        let key = PendingKey {
            route_edge_id: route.route_edge_id(),
            sequence,
        };
        let now = self.clock.now();
        {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Err(limit) = pending.admit(
                key,
                PendingEntry {
                    frame: Arc::clone(&frame),
                    route: route.clone(),
                    attempts: 1,
                    first_sent_at: now,
                    last_sent_at: now,
                },
                self.policy.max_pending_entries,
                self.policy.max_pending_bytes,
            ) {
                // Over a self-owned ceiling: the frame is neither buffered nor put on
                // the wire. Surface the explicit rejection; the caller degrades the route.
                return ReliableSendOutcome::ResourceLimit(limit);
            }
        }
        // Transmit outside the buffer lock: a re-entrant or blocking sink must not be
        // able to stall a concurrent ack or retry tick that needs the buffer.
        self.resolve_sink().deliver_remote(route, &frame);
        ReliableSendOutcome::Buffered(key.into_route_identity())
    }

    /// Apply an ack for `identity` with `status`, releasing the matching buffered
    /// frame if present.
    pub(crate) fn on_ack(
        &self,
        identity: &RuntimeFilterRouteIdentity,
        status: RuntimeFilterAcceptStatus,
    ) -> EnvelopeAckOutcome {
        let Some(delivery) = identity.as_delivery() else {
            // Only delivery frames are buffered today; `Contribution` acks converge
            // here in RFD-6. A non-delivery identity therefore matches nothing.
            return EnvelopeAckOutcome::Unknown;
        };
        let key = PendingKey::from_delivery(delivery);
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match pending.entries.remove(&key) {
            // Already released, or a duplicate / out-of-order ack for an identity that
            // is no longer in flight. A no-op — never a re-delivery.
            None => EnvelopeAckOutcome::Unknown,
            Some(entry) => {
                pending.release(&entry.frame);
                match status {
                    RuntimeFilterAcceptStatus::Accepted => EnvelopeAckOutcome::Released,
                    RuntimeFilterAcceptStatus::Duplicate => EnvelopeAckOutcome::ReleasedOnDuplicate,
                    RuntimeFilterAcceptStatus::Rejected => EnvelopeAckOutcome::Rejected,
                }
            }
        }
    }

    /// Advance the transport to `now`: re-hand due unacked frames to the sink under
    /// the bounded attempt count, and release + fail open any frame past its
    /// deadline. Explicit and side-effect-scoped — no background thread.
    pub(crate) fn drive_retries(&self, now: Instant) -> ReliableTransportTick {
        let mut to_send: Vec<(RuntimeFilterRemoteRoute, Arc<EncodedArtifactFrame>)> = Vec::new();
        let mut failed_open: Vec<PendingKey> = Vec::new();
        {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mut expired: Vec<PendingKey> = Vec::new();
            for (key, entry) in pending.entries.iter_mut() {
                // Deadline wins over retry: past the deadline the frame is dropped and
                // the route fails open. No panic, no error surfaced to the query.
                if now.saturating_duration_since(entry.first_sent_at) >= self.policy.deadline {
                    expired.push(*key);
                    continue;
                }
                // Under the attempt bound and past the retry interval: re-hand it. Once
                // the count is exhausted the frame stays buffered until its deadline, so
                // a late ack can still release it cleanly.
                if entry.attempts < self.policy.max_attempts
                    && now.saturating_duration_since(entry.last_sent_at)
                        >= self.policy.retry_interval
                {
                    entry.attempts += 1;
                    entry.last_sent_at = now;
                    to_send.push((entry.route.clone(), Arc::clone(&entry.frame)));
                }
            }
            // Remove the deadline-expired frames, reclaiming their byte accounting.
            for key in expired {
                if let Some(entry) = pending.entries.remove(&key) {
                    pending.release(&entry.frame);
                    failed_open.push(key);
                }
            }
        }
        // Re-hand retries outside the buffer lock (see `send`). The re-hand order
        // within a tick is unspecified (it follows HashMap iteration), so no caller
        // may depend on it; only `failed_open` is sorted below for determinism.
        let sink = self.resolve_sink();
        for (route, frame) in &to_send {
            sink.deliver_remote(route, frame);
        }
        failed_open.sort_unstable();
        ReliableTransportTick {
            retried: to_send.len(),
            failed_open: failed_open
                .into_iter()
                .map(PendingKey::into_route_identity)
                .collect(),
        }
    }

    /// Resolve the sink to transmit through: the test override when installed,
    /// otherwise the sink the transport was constructed with.
    fn resolve_sink(&self) -> Arc<dyn ArtifactRemoteSink> {
        #[cfg(test)]
        {
            if let Some(sink) = self
                .sink_override
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
            {
                return Arc::clone(sink);
            }
        }
        Arc::clone(&self.sink)
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .len()
    }

    /// The distinct serialized-frame bytes currently buffered (per-unique-allocation;
    /// a broadcast frame counts once). Test seam for the self-owned byte ceiling and
    /// the release-to-zero teardown assertion.
    #[cfg(test)]
    fn pending_bytes(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .bytes
    }

    /// Point the transport at a fake sink. Test-only seam used by service-level
    /// delivery tests that build the service with the inert production sink.
    #[cfg(test)]
    pub(crate) fn set_sink_for_test(&self, sink: Arc<dyn ArtifactRemoteSink>) {
        *self
            .sink_override
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sink);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::{
        EnvelopeAckOutcome, ReliableEnvelopeTransport, ReliableSendOutcome,
        ReliableTransportPolicy, TransportResourceLimit,
    };
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::codec::artifact::EncodedArtifactFrame;
    use crate::runtime_filter::model::contract::BindingId;
    use crate::runtime_filter::port::identity::{RouteEdgeId, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::routing::{RuntimeFilterRemoteRoute, RuntimeFilterRouteRole};
    use crate::runtime_filter::port::support::RuntimeFilterClock;
    use crate::runtime_filter::port::transport::RuntimeFilterAcceptStatus;
    use crate::runtime_filter::router::remote::ArtifactRemoteSink;

    /// Drivable fake sink: records every (route edge, frame) it is handed so a test
    /// can assert exact send / retry counts and compare the transmitted bytes.
    #[derive(Default)]
    struct RecordingSink {
        sends: Mutex<Vec<(RouteEdgeId, EncodedArtifactFrame)>>,
    }

    impl ArtifactRemoteSink for RecordingSink {
        fn deliver_remote(&self, route: &RuntimeFilterRemoteRoute, frame: &EncodedArtifactFrame) {
            self.sends
                .lock()
                .unwrap()
                .push((route.route_edge_id(), frame.clone()));
        }
    }

    impl RecordingSink {
        fn count(&self) -> usize {
            self.sends.lock().unwrap().len()
        }

        fn edges(&self) -> Vec<RouteEdgeId> {
            self.sends.lock().unwrap().iter().map(|(e, _)| *e).collect()
        }

        fn frames(&self) -> Vec<(RouteEdgeId, EncodedArtifactFrame)> {
            self.sends.lock().unwrap().clone()
        }
    }

    /// Manually advanced clock: the transport reads `now()`; the test moves time.
    struct ManualClock(Mutex<Instant>);

    impl RuntimeFilterClock for ManualClock {
        fn now(&self) -> Instant {
            *self.0.lock().unwrap()
        }
    }

    impl ManualClock {
        fn new(start: Instant) -> Self {
            Self(Mutex::new(start))
        }

        fn advance(&self, by: Duration) {
            let mut guard = self.0.lock().unwrap();
            *guard += by;
        }
    }

    struct Harness {
        transport: ReliableEnvelopeTransport,
        sink: Arc<RecordingSink>,
        clock: Arc<ManualClock>,
    }

    fn harness(policy: ReliableTransportPolicy) -> Harness {
        let sink = Arc::new(RecordingSink::default());
        let clock = Arc::new(ManualClock::new(Instant::now()));
        let transport = ReliableEnvelopeTransport::new(sink.clone(), clock.clone(), policy);
        Harness {
            transport,
            sink,
            clock,
        }
    }

    // Roomy buffer ceilings so the pre-existing retry/ack/deadline tests never trip a
    // resource limit; the `transport_bounded_*` tests set tight ceilings explicitly.
    const ROOMY_MAX_ENTRIES: usize = 1024;
    const ROOMY_MAX_BYTES: usize = 1 << 30;

    fn policy(retry_ms: u64, max_attempts: u32, deadline_ms: u64) -> ReliableTransportPolicy {
        policy_bounded(
            retry_ms,
            max_attempts,
            deadline_ms,
            ROOMY_MAX_ENTRIES,
            ROOMY_MAX_BYTES,
        )
    }

    fn policy_bounded(
        retry_ms: u64,
        max_attempts: u32,
        deadline_ms: u64,
        max_pending_entries: usize,
        max_pending_bytes: usize,
    ) -> ReliableTransportPolicy {
        ReliableTransportPolicy::new(
            Duration::from_millis(retry_ms),
            max_attempts,
            Duration::from_millis(deadline_ms),
            max_pending_entries,
            max_pending_bytes,
        )
    }

    // A frame whose serialized payload is exactly `bytes` long, tagged by `tag` so
    // distinct sends can be told apart. Byte-ceiling tests size payloads against a cap.
    fn frame_sized(tag: u8, bytes: usize) -> Arc<EncodedArtifactFrame> {
        Arc::new(EncodedArtifactFrame::from_parts_for_test(
            [tag; 32],
            vec![tag; bytes],
        ))
    }

    fn route(edge: u32) -> RuntimeFilterRemoteRoute {
        RuntimeFilterRemoteRoute::new(
            RouteEdgeId::new(edge),
            RuntimeFilterParticipantId::new(7),
            RuntimeEndpoint::new("10.0.0.7", 9060).unwrap(),
            RuntimeFilterRouteRole::Consumer(BindingId::new(edge)),
        )
        .unwrap()
    }

    fn frame(tag: u8) -> Arc<EncodedArtifactFrame> {
        Arc::new(EncodedArtifactFrame::from_parts_for_test(
            [tag; 32],
            vec![tag, tag, tag],
        ))
    }

    #[test]
    fn reliable_transport_send_buffers_frame_and_hands_it_to_the_sink() {
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy(100, 3, 10_000));
        let payload = frame(1);

        let identity = transport
            .send(&route(30), Arc::clone(&payload))
            .expect_buffered();

        // Buffered exactly once.
        assert_eq!(transport.pending_len(), 1);
        // Handed to the sink exactly once with the authorized edge and the bytes.
        let sends = sink.frames();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].0, RouteEdgeId::new(30));
        assert_eq!(&sends[0].1, payload.as_ref());
        // The stamped identity is a delivery identity on the same edge.
        assert_eq!(
            identity.as_delivery().unwrap().route_edge_id(),
            RouteEdgeId::new(30)
        );
    }

    #[test]
    fn reliable_transport_accepted_ack_releases_the_buffered_envelope() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));

        let identity = transport.send(&route(30), frame(1)).expect_buffered();
        assert_eq!(transport.pending_len(), 1);

        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);

        // A second ack for the already-released identity is a no-op (duplicate arrival).
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Unknown
        );

        // A released frame is never retried, however far the clock advances.
        clock.advance(Duration::from_millis(500));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn reliable_transport_duplicate_ack_releases_without_redelivery() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));

        let identity = transport.send(&route(30), frame(1)).expect_buffered();
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Duplicate),
            EnvelopeAckOutcome::ReleasedOnDuplicate
        );
        assert_eq!(transport.pending_len(), 0);

        clock.advance(Duration::from_millis(500));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1, "a duplicate-acked frame is never re-sent");
    }

    #[test]
    fn reliable_transport_missing_ack_retries_up_to_the_bounded_count() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 100_000));

        transport.send(&route(30), frame(1));
        assert_eq!(sink.count(), 1);

        // A tick before the retry interval elapses re-sends nothing.
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1);

        // First interval → retry #1 (2 sends total).
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 2);

        // Second interval → retry #2, reaching the bound of 3 total sends.
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 3);

        // Third interval → attempt count exhausted; no more sends, still buffered
        // because the (much larger) deadline has not yet elapsed.
        clock.advance(Duration::from_millis(100));
        let tick = transport.drive_retries(clock.now());
        assert_eq!(tick.retried(), 0);
        assert!(tick.failed_open().is_empty());
        assert_eq!(sink.count(), 3, "retry count is strictly bounded");
        assert_eq!(transport.pending_len(), 1);
    }

    #[test]
    fn reliable_transport_exhausted_retries_still_release_on_a_late_ack() {
        // The composed M3 semantic in one sequence: exhausting the attempt count stops
        // retransmission but keeps the frame buffered until its (far-off) deadline, so
        // an ack arriving after exhaustion still releases cleanly.
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 2, 100_000));

        let identity = transport.send(&route(30), frame(1)).expect_buffered();
        assert_eq!(sink.count(), 1);

        // One interval → the single allowed retry, reaching the bound of 2 sends.
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 2);

        // Attempt count now exhausted: further ticks are quiescent (no retransmit) yet
        // the entry stays buffered because the deadline is nowhere near.
        for _ in 0..3 {
            clock.advance(Duration::from_millis(100));
            let tick = transport.drive_retries(clock.now());
            assert_eq!(tick.retried(), 0);
            assert!(tick.failed_open().is_empty());
        }
        assert_eq!(sink.count(), 2, "no retransmit past the attempt bound");
        assert_eq!(transport.pending_len(), 1);

        // A late ack (well before the deadline) still releases the buffered frame.
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);

        // And nothing is re-sent afterwards.
        clock.advance(Duration::from_millis(100));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 2);
    }

    #[test]
    fn reliable_transport_out_of_order_and_duplicate_acks_are_handled() {
        let Harness {
            transport,
            sink: _sink,
            clock: _clock,
        } = harness(policy(100, 3, 10_000));

        // Two envelopes to the SAME route get distinct sequences.
        let first = transport.send(&route(30), frame(1)).expect_buffered();
        let second = transport.send(&route(30), frame(2)).expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_ne!(
            first.as_delivery().unwrap().sequence(),
            second.as_delivery().unwrap().sequence()
        );

        // Ack the second delivery first (out of order): only its entry releases.
        assert_eq!(
            transport.on_ack(&second, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 1);

        // A duplicate ack for the already-released second delivery is a no-op.
        assert_eq!(
            transport.on_ack(&second, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Unknown
        );
        assert_eq!(transport.pending_len(), 1);

        // The still-pending first delivery releases independently.
        assert_eq!(
            transport.on_ack(&first, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);
    }

    #[test]
    fn reliable_transport_deadline_releases_and_fails_open_without_error() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 10, 250));

        let identity = transport.send(&route(30), frame(1)).expect_buffered();

        // A retry fires before the deadline.
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 2);
        assert_eq!(transport.pending_len(), 1);

        // Crossing the deadline releases the frame and reports it failed open — no
        // panic, no error surfaced to the query.
        clock.advance(Duration::from_millis(150));
        let tick = transport.drive_retries(clock.now());
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(tick.retried(), 0);
        assert_eq!(tick.failed_open(), &[identity]);

        // Once failed open, further ticks neither re-send nor re-report.
        clock.advance(Duration::from_millis(1_000));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 2);
    }

    #[test]
    fn reliable_transport_rejected_ack_stops_retry_and_surfaces_the_rejection() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 5, 10_000));

        let identity = transport.send(&route(30), frame(1)).expect_buffered();
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Rejected),
            EnvelopeAckOutcome::Rejected
        );
        assert_eq!(transport.pending_len(), 0);

        // A rejected frame is released, so retries stop for it.
        clock.advance(Duration::from_millis(500));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn reliable_transport_broadcast_fanout_shares_one_frame_and_acks_independently() {
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy(100, 3, 10_000));

        let payload = frame(1);
        assert_eq!(Arc::strong_count(&payload), 1);

        // One serialized frame fans out to two routes.
        let route_a = transport
            .send(&route(30), Arc::clone(&payload))
            .expect_buffered();
        let route_b = transport
            .send(&route(31), Arc::clone(&payload))
            .expect_buffered();

        // The single frame is shared (not re-serialized): caller + 2 buffered clones.
        assert_eq!(Arc::strong_count(&payload), 3);
        assert_eq!(transport.pending_len(), 2);

        // Both routes received the identical bytes.
        let mut edges = sink.edges();
        edges.sort_unstable();
        assert_eq!(edges, vec![RouteEdgeId::new(30), RouteEdgeId::new(31)]);
        for (_, transmitted) in sink.frames() {
            assert_eq!(&transmitted, payload.as_ref());
        }

        // Acking one route releases only its entry; the shared frame Arc drops by one.
        assert_eq!(
            transport.on_ack(&route_a, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 1);
        assert_eq!(Arc::strong_count(&payload), 2);

        assert_eq!(
            transport.on_ack(&route_b, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(Arc::strong_count(&payload), 1);
    }

    // ==============================================================================
    // M3 Task 4: self-owned buffer ceilings -> explicit ResourceLimit rejection.
    // ==============================================================================

    #[test]
    fn transport_bounded_retry_queue_rejects_new_frame_at_entry_ceiling() {
        // Entry-count ceiling of 2: the buffer admits two in-flight frames, then a
        // third genuinely-new frame is refused with an explicit ResourceLimit — never
        // silently dropped, never buffered beyond the cap.
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, 2, ROOMY_MAX_BYTES));

        let first = transport.send(&route(30), frame(1)).expect_buffered();
        let _second = transport.send(&route(31), frame(2)).expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(sink.count(), 2);

        // The third send is refused: not buffered, not put on the wire.
        assert_eq!(
            transport.send(&route(32), frame(3)),
            ReliableSendOutcome::ResourceLimit(TransportResourceLimit::PendingEntries),
        );
        assert_eq!(
            transport.pending_len(),
            2,
            "a refused frame is not buffered"
        );
        assert_eq!(sink.count(), 2, "a refused frame is not transmitted");

        // Releasing an in-flight frame frees a slot; the next send is admitted again.
        assert_eq!(
            transport.on_ack(&first, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released,
        );
        assert_eq!(transport.pending_len(), 1);
        let _third = transport.send(&route(32), frame(3)).expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(sink.count(), 3);
    }

    #[test]
    fn transport_bounded_serialized_buffer_rejects_new_frame_at_byte_ceiling() {
        // Byte ceiling of 10: distinct frames are admitted until their serialized
        // bytes would exceed the cap, then a new frame is refused with an explicit
        // ResourceLimit. The cap rejects only strictly-greater, so the exact-fit send
        // is still admitted.
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, ROOMY_MAX_ENTRIES, 10));

        let _a = transport
            .send(&route(30), frame_sized(1, 6))
            .expect_buffered();
        assert_eq!(transport.pending_bytes(), 6);

        // 6 + 6 = 12 > 10: refused on the byte ceiling.
        assert_eq!(
            transport.send(&route(31), frame_sized(2, 6)),
            ReliableSendOutcome::ResourceLimit(TransportResourceLimit::SerializedBytes),
        );
        assert_eq!(
            transport.pending_len(),
            1,
            "a byte-refused frame is not buffered"
        );
        assert_eq!(transport.pending_bytes(), 6);

        // 6 + 4 = 10 == cap: admitted.
        let _b = transport
            .send(&route(32), frame_sized(3, 4))
            .expect_buffered();
        assert_eq!(transport.pending_bytes(), 10);
        assert_eq!(sink.count(), 2);

        // Any further distinct byte is refused.
        assert_eq!(
            transport.send(&route(33), frame_sized(4, 1)),
            ReliableSendOutcome::ResourceLimit(TransportResourceLimit::SerializedBytes),
        );
    }

    #[test]
    fn transport_bounded_broadcast_frame_meters_shared_bytes_once() {
        // A broadcast frame fans out to several routes as one shared allocation, so
        // its serialized bytes are metered once. Under a per-entry meter both routes
        // would exceed a 10-byte cap (2 x 6 = 12); metering per allocation keeps it at
        // 6 and admits the whole fan-out.
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, ROOMY_MAX_ENTRIES, 10));
        let payload = frame_sized(9, 6);

        let route_a = transport
            .send(&route(30), Arc::clone(&payload))
            .expect_buffered();
        let route_b = transport
            .send(&route(31), Arc::clone(&payload))
            .expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(
            transport.pending_bytes(),
            6,
            "a shared frame is metered once, not per route"
        );
        assert_eq!(sink.count(), 2);

        // The shared allocation (and its bytes) survive until the last reference drops.
        assert_eq!(
            transport.on_ack(&route_a, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released,
        );
        assert_eq!(transport.pending_bytes(), 6);
        assert_eq!(
            transport.on_ack(&route_b, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released,
        );
        assert_eq!(transport.pending_bytes(), 0);
    }

    #[test]
    fn transport_bounded_release_returns_counts_to_zero() {
        // The self-owned counters live inside the query-scoped transport, so releasing
        // every in-flight frame (as query teardown does when the service is destroyed)
        // returns both the entry count and the buffered bytes to zero — the buffer
        // never grows without bound and never leaks.
        let Harness {
            transport,
            sink: _sink,
            clock,
        } = harness(policy_bounded(
            100,
            10,
            250,
            ROOMY_MAX_ENTRIES,
            ROOMY_MAX_BYTES,
        ));

        let a = transport
            .send(&route(30), frame_sized(1, 5))
            .expect_buffered();
        let b = transport
            .send(&route(31), frame_sized(2, 7))
            .expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(transport.pending_bytes(), 12);

        // Ack-release frees both entries and reclaims their bytes.
        transport.on_ack(&a, RuntimeFilterAcceptStatus::Accepted);
        transport.on_ack(&b, RuntimeFilterAcceptStatus::Duplicate);
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);

        // The deadline fail-open path also reclaims bytes, not just entries.
        let _c = transport
            .send(&route(32), frame_sized(3, 9))
            .expect_buffered();
        assert_eq!(transport.pending_bytes(), 9);
        clock.advance(Duration::from_millis(300));
        let tick = transport.drive_retries(clock.now());
        assert_eq!(tick.failed_open().len(), 1);
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(
            transport.pending_bytes(),
            0,
            "deadline fail-open reclaims buffered bytes"
        );
    }
}
