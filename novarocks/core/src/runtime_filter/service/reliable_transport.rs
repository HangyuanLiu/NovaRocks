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

/// Bounded retry / deadline policy for the reliable transport.
///
/// `max_attempts` caps the total number of times a frame is handed to the sink
/// (the initial send plus retries), bounding network chatter. `deadline` caps how
/// long a frame may stay buffered before it is released and the route fails open,
/// bounding buffer lifetime. The two limits are independent: exhausting the attempt
/// count stops re-transmission but keeps the frame buffered until the deadline, so
/// an ack that finally arrives before the deadline still releases cleanly.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReliableTransportPolicy {
    retry_interval: Duration,
    max_attempts: u32,
    deadline: Duration,
}

impl ReliableTransportPolicy {
    pub(crate) fn new(retry_interval: Duration, max_attempts: u32, deadline: Duration) -> Self {
        assert!(
            max_attempts >= 1,
            "reliable transport must allow at least the initial send"
        );
        Self {
            retry_interval,
            max_attempts,
            deadline,
        }
    }
}

// Sane defaults for the not-yet-wired production driver. RFD-6 sources the real
// values from the query deadline and cluster RPC policy; until the live sender and
// tick loop exist these constants only shape test-free production construction.
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

impl Default for ReliableTransportPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_RETRY_INTERVAL,
            DEFAULT_MAX_ATTEMPTS,
            DEFAULT_DEADLINE,
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
    pending: Mutex<HashMap<PendingKey, PendingEntry>>,
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
            pending: Mutex::new(HashMap::new()),
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

    /// Buffer `frame` for reliable delivery to `route` and hand it to the underlying
    /// sink once. Returns the delivery route identity the transport stamped, so an
    /// ack can later address exactly this in-flight frame.
    pub(crate) fn send(
        &self,
        route: &RuntimeFilterRemoteRoute,
        frame: Arc<EncodedArtifactFrame>,
    ) -> RuntimeFilterRouteIdentity {
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
            pending.insert(
                key,
                PendingEntry {
                    frame: Arc::clone(&frame),
                    route: route.clone(),
                    attempts: 1,
                    first_sent_at: now,
                    last_sent_at: now,
                },
            );
        }
        // Transmit outside the buffer lock: a re-entrant or blocking sink must not be
        // able to stall a concurrent ack or retry tick that needs the buffer.
        self.resolve_sink().deliver_remote(route, &frame);
        key.into_route_identity()
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
        match pending.remove(&key) {
            // Already released, or a duplicate / out-of-order ack for an identity that
            // is no longer in flight. A no-op — never a re-delivery.
            None => EnvelopeAckOutcome::Unknown,
            Some(_entry) => match status {
                RuntimeFilterAcceptStatus::Accepted => EnvelopeAckOutcome::Released,
                RuntimeFilterAcceptStatus::Duplicate => EnvelopeAckOutcome::ReleasedOnDuplicate,
                RuntimeFilterAcceptStatus::Rejected => EnvelopeAckOutcome::Rejected,
            },
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
            pending.retain(|key, entry| {
                // Deadline wins over retry: past the deadline the frame is dropped and
                // the route fails open. No panic, no error surfaced to the query.
                if now.saturating_duration_since(entry.first_sent_at) >= self.policy.deadline {
                    failed_open.push(*key);
                    return false;
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
                true
            });
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
            .len()
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

    use super::{EnvelopeAckOutcome, ReliableEnvelopeTransport, ReliableTransportPolicy};
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

    fn policy(retry_ms: u64, max_attempts: u32, deadline_ms: u64) -> ReliableTransportPolicy {
        ReliableTransportPolicy::new(
            Duration::from_millis(retry_ms),
            max_attempts,
            Duration::from_millis(deadline_ms),
        )
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

        let identity = transport.send(&route(30), Arc::clone(&payload));

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

        let identity = transport.send(&route(30), frame(1));
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

        let identity = transport.send(&route(30), frame(1));
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

        let identity = transport.send(&route(30), frame(1));
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
        let first = transport.send(&route(30), frame(1));
        let second = transport.send(&route(30), frame(2));
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

        let identity = transport.send(&route(30), frame(1));

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

        let identity = transport.send(&route(30), frame(1));
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
        let route_a = transport.send(&route(30), Arc::clone(&payload));
        let route_b = transport.send(&route(31), Arc::clone(&payload));

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
}
