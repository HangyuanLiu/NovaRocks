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

//! The per-(task attempt, plan node) dynamic split queue.
//!
//! Responsibilities:
//! - Owns the runtime stream of splits a scan receives after its plan was
//!   frozen, one queue per plan node inside one task attempt.
//! - Enforces the wire contract locally: monotonic sequences, idempotent exact
//!   replay, typed conflict on a rewritten sequence, an idempotent terminal
//!   marker, and a retained-byte ceiling that a sender can use for
//!   backpressure.
//!
//! Key exported interfaces:
//! - Types: `SplitQueueRegistry`, `TaskAttemptSplitQueues`, `SplitQueue`,
//!   `TaskAttemptKey`, `SplitQueueConfig`, `SplitPoll`, `SplitOfferOutcome`,
//!   `SplitQueueStats`, `SplitQueueError`, `SplitQueueErrorKind`,
//!   `ScheduledSplitFacts`.
//!
//! Current limitations:
//! - This is queue ownership only. It creates no lifecycle entry, extends no
//!   retention, and performs no cross-attempt recovery: an attempt's queues are
//!   born with the attempt and die with it.
//!
//! Provider neutrality: the queue moves validated wire splits and reads only
//! their sequence, plan node, canonical bytes, and retained size. It never
//! inspects a provider variant, so this file compiles with no provider crate in
//! the dependency graph.
// Design: ADR-0114 (docs/adr/ADR-0114-trino-aligned-typed-connector-read-stack.md)

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_proto::connector_read::{
    MAX_ASSIGNMENT_RETAINED_BYTES, ScheduledSplit, SplitAssignment,
};
use novarocks_types::{QueryExecutionId, UniqueId};

use crate::runtime::observable::Observable;

/// Per-entry overhead of one replay record: the sequence key plus the vector
/// header that owns its canonical bytes.
const REPLAY_ENTRY_OVERHEAD_BYTES: u64 = (size_of::<u64>() + size_of::<Vec<u8>>()) as u64;

/// The facts this queue needs from one scheduled split.
///
/// The queue is generic over this narrow view rather than over the wire type
/// directly. Production always supplies the validated wire value (the default
/// type parameter everywhere below), and the trait makes provider neutrality
/// structural: ordering, replay, and budget rules cannot reach a provider
/// variant because the view does not expose one.
pub trait ScheduledSplitFacts: Send + Sync + 'static {
    /// Monotonic within one (task attempt, plan node).
    fn sequence_id(&self) -> u64;

    /// The plan node whose queue this split belongs to.
    fn plan_node_id(&self) -> i32;

    /// The bytes an exact replay must reproduce.
    fn canonical_bytes(&self) -> &[u8];

    /// Retained-byte estimate charged against the queue's ceiling.
    fn retained_size_in_bytes(&self) -> u64;
}

impl ScheduledSplitFacts for ScheduledSplit {
    fn sequence_id(&self) -> u64 {
        ScheduledSplit::sequence_id(self)
    }

    fn plan_node_id(&self) -> i32 {
        ScheduledSplit::plan_node_id(self)
    }

    fn canonical_bytes(&self) -> &[u8] {
        ScheduledSplit::canonical_bytes(self)
    }

    fn retained_size_in_bytes(&self) -> u64 {
        // The provider declares the retained size of its own decoded payload.
        // The canonical encoding is charged on top because the queue owns that
        // copy whatever the declaration says, and because a zero declaration
        // must still cost something rather than making a split free.
        ScheduledSplit::split(self)
            .retained_size_in_bytes()
            .saturating_add(ScheduledSplit::canonical_bytes(self).len() as u64)
    }
}

/// Why one split offer was rejected.
///
/// Every variant is terminal for that offer: the queue is never partially
/// updated, and a rejected sequence is never silently overwritten.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SplitQueueErrorKind {
    /// The assignment names a plan node other than the queue it reached.
    PlanNodeMismatch,
    /// The same sequence was already accepted carrying different canonical
    /// bytes, the batch did not strictly increase, or the sequence is below the
    /// accepted high-water mark and was never seen.
    SequenceConflict,
    /// A previously unseen sequence arrived after this plan node's terminal
    /// marker.
    AfterNoMoreSplits,
    /// The queue was closed; it accepts nothing further.
    Closed,
    /// The offer would push retained bytes past the configured ceiling.
    ResourceExhausted,
}

impl fmt::Display for SplitQueueErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PlanNodeMismatch => "plan node mismatch",
            Self::SequenceConflict => "sequence conflict",
            Self::AfterNoMoreSplits => "split after no-more-splits",
            Self::Closed => "queue closed",
            Self::ResourceExhausted => "resource exhausted",
        })
    }
}

/// A typed rejection of one split offer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitQueueError {
    kind: SplitQueueErrorKind,
    plan_node_id: i32,
    sequence_id: Option<u64>,
    detail: String,
}

impl SplitQueueError {
    pub fn new(
        kind: SplitQueueErrorKind,
        plan_node_id: i32,
        sequence_id: Option<u64>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            plan_node_id,
            sequence_id,
            detail: detail.into(),
        }
    }

    pub const fn kind(&self) -> SplitQueueErrorKind {
        self.kind
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub const fn sequence_id(&self) -> Option<u64> {
        self.sequence_id
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for SplitQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} for plan node {}",
            self.kind, self.plan_node_id
        )?;
        if let Some(sequence_id) = self.sequence_id {
            write!(formatter, " sequence {sequence_id}")?;
        }
        write!(formatter, ": {}", self.detail)
    }
}

impl std::error::Error for SplitQueueError {}

/// Budgets one plan-node queue enforces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SplitQueueConfig {
    /// Ceiling on the retained bytes of splits waiting to be polled. This is
    /// the backpressure signal: a sender that ignores the reported queue depth
    /// is rejected rather than allowed to grow the queue without bound.
    pub max_queued_bytes: u64,
    /// Ceiling on the replay record, which keeps the canonical bytes of every
    /// accepted sequence for this attempt.
    ///
    /// The record cannot shrink while the attempt lives: telling an exact
    /// replay apart from a rewritten sequence needs the original bytes, and a
    /// digest would reintroduce the content id the wire contract deliberately
    /// does not have. Bounding it is therefore the only honest option, and
    /// exceeding the bound is a typed resource error, not a silent truncation.
    pub max_replay_record_bytes: u64,
}

impl Default for SplitQueueConfig {
    fn default() -> Self {
        Self {
            max_queued_bytes: MAX_ASSIGNMENT_RETAINED_BYTES,
            max_replay_record_bytes: MAX_ASSIGNMENT_RETAINED_BYTES,
        }
    }
}

/// What one accepted offer changed.
///
/// `max_accepted_sequence` and `no_more_splits` are exactly the per-plan-node
/// acknowledgement the sender needs to decide what to retransmit.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SplitOfferOutcome {
    /// Sequences newly enqueued by this offer, in offer order.
    pub enqueued: Vec<u64>,
    /// Sequences recognized as exact replays; they enqueued nothing.
    pub replayed: Vec<u64>,
    /// The highest sequence this plan node has accepted.
    pub max_accepted_sequence: Option<u64>,
    /// Whether this plan node has seen its terminal marker.
    pub no_more_splits: bool,
}

/// Observable depth of one plan-node queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SplitQueueStats {
    pub queued_splits: usize,
    pub queued_bytes: u64,
    pub accepted_splits: usize,
    pub replay_record_bytes: u64,
    pub max_accepted_sequence: Option<u64>,
    pub no_more_splits: bool,
    pub closed: bool,
}

/// The result of asking a queue for its next split.
///
/// Three states, because an empty queue is not end of stream: only a drained
/// terminal marker or a close is.
#[derive(Debug)]
pub enum SplitPoll<S = ScheduledSplit> {
    /// One split is ready to run.
    Ready(S),
    /// Nothing right now. The caller parks and is woken through
    /// [`SplitQueue::observable`].
    Blocked,
    /// No split will ever be produced again: drained after the terminal marker,
    /// or closed.
    Exhausted,
}

struct SplitQueueState<S> {
    pending: VecDeque<S>,
    /// Canonical bytes of every sequence accepted for this plan node. See
    /// [`SplitQueueConfig::max_replay_record_bytes`] for why it is retained.
    accepted: BTreeMap<u64, Vec<u8>>,
    queued_bytes: u64,
    replay_record_bytes: u64,
    no_more_splits: bool,
    closed: bool,
}

/// What the classification pass decided about one offered split.
enum SplitDecision {
    Enqueue { retained: u64, record: u64 },
    Replay,
}

/// One plan node's runtime split queue inside one task attempt.
///
/// Created empty: a task scan may legally start with zero splits and receive
/// all of them later, or receive none at all.
pub struct SplitQueue<S = ScheduledSplit> {
    plan_node_id: i32,
    config: SplitQueueConfig,
    state: Mutex<SplitQueueState<S>>,
    observable: Arc<Observable>,
}

impl<S: ScheduledSplitFacts> SplitQueue<S> {
    /// An empty queue for one plan node.
    pub fn new(plan_node_id: i32, config: SplitQueueConfig) -> Arc<Self> {
        Arc::new(Self {
            plan_node_id,
            config,
            state: Mutex::new(SplitQueueState {
                pending: VecDeque::new(),
                accepted: BTreeMap::new(),
                queued_bytes: 0,
                replay_record_bytes: 0,
                no_more_splits: false,
                closed: false,
            }),
            observable: Arc::new(Observable::new()),
        })
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub const fn config(&self) -> SplitQueueConfig {
        self.config
    }

    /// Waiters register here. Armed on a new split, on the terminal marker, on
    /// the transition to exhausted, and on close.
    pub fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }

    /// Accept one plan node's batch.
    ///
    /// The whole batch is classified before anything is committed, so a
    /// rejected offer leaves the queue byte-for-byte as it was.
    pub fn offer_splits(
        &self,
        plan_node_id: i32,
        splits: Vec<S>,
        no_more_splits: bool,
    ) -> Result<SplitOfferOutcome, SplitQueueError> {
        if plan_node_id != self.plan_node_id {
            return Err(self.error(
                SplitQueueErrorKind::PlanNodeMismatch,
                None,
                format!(
                    "assignment for plan node {plan_node_id} reached the queue of plan node {}",
                    self.plan_node_id
                ),
            ));
        }

        let notify = self.observable.defer_notify();
        let mut state = self.state.lock().expect("split queue lock");
        if state.closed {
            return Err(self.error(
                SplitQueueErrorKind::Closed,
                None,
                "the queue is closed and accepts no further split",
            ));
        }

        let high_water = state.accepted.keys().next_back().copied();
        let mut decisions = Vec::with_capacity(splits.len());
        let mut added_queued_bytes: u64 = 0;
        let mut added_record_bytes: u64 = 0;
        let mut previous: Option<u64> = None;

        for split in &splits {
            let sequence_id = split.sequence_id();
            if split.plan_node_id() != self.plan_node_id {
                return Err(self.error(
                    SplitQueueErrorKind::PlanNodeMismatch,
                    Some(sequence_id),
                    "scheduled split belongs to another plan node",
                ));
            }
            // The wire contract already requires strictly increasing sequences
            // inside one assignment; re-checking keeps that guarantee for every
            // caller of this generic entry point.
            if let Some(previous) = previous
                && sequence_id <= previous
            {
                return Err(self.error(
                    SplitQueueErrorKind::SequenceConflict,
                    Some(sequence_id),
                    "offered sequences must strictly increase within one batch",
                ));
            }
            previous = Some(sequence_id);

            match state.accepted.get(&sequence_id) {
                Some(recorded) => {
                    if recorded.as_slice() != split.canonical_bytes() {
                        return Err(self.error(
                            SplitQueueErrorKind::SequenceConflict,
                            Some(sequence_id),
                            "sequence was already accepted with different canonical bytes",
                        ));
                    }
                    // An exact replay is idempotent. It stays legal after the
                    // terminal marker, because the batch that carried the
                    // marker is exactly what a sender retransmits.
                    decisions.push(SplitDecision::Replay);
                }
                None => {
                    if let Some(high_water) = high_water
                        && sequence_id <= high_water
                    {
                        return Err(self.error(
                            SplitQueueErrorKind::SequenceConflict,
                            Some(sequence_id),
                            "sequence is below the accepted high-water mark and was never seen",
                        ));
                    }
                    if state.no_more_splits {
                        return Err(self.error(
                            SplitQueueErrorKind::AfterNoMoreSplits,
                            Some(sequence_id),
                            "plan node already saw its terminal marker",
                        ));
                    }
                    let retained = split.retained_size_in_bytes();
                    let record = (split.canonical_bytes().len() as u64)
                        .saturating_add(REPLAY_ENTRY_OVERHEAD_BYTES);
                    added_queued_bytes = added_queued_bytes.saturating_add(retained);
                    added_record_bytes = added_record_bytes.saturating_add(record);
                    decisions.push(SplitDecision::Enqueue { retained, record });
                }
            }
        }

        let projected_queued = state.queued_bytes.saturating_add(added_queued_bytes);
        if projected_queued > self.config.max_queued_bytes {
            return Err(self.error(
                SplitQueueErrorKind::ResourceExhausted,
                None,
                format!(
                    "offer would retain {projected_queued} queued bytes, above the ceiling of {}",
                    self.config.max_queued_bytes
                ),
            ));
        }
        let projected_record = state.replay_record_bytes.saturating_add(added_record_bytes);
        if projected_record > self.config.max_replay_record_bytes {
            return Err(self.error(
                SplitQueueErrorKind::ResourceExhausted,
                None,
                format!(
                    "offer would retain {projected_record} replay-record bytes, above the ceiling of {}",
                    self.config.max_replay_record_bytes
                ),
            ));
        }

        let mut outcome = SplitOfferOutcome::default();
        for (split, decision) in splits.into_iter().zip(decisions) {
            let sequence_id = split.sequence_id();
            match decision {
                SplitDecision::Enqueue { retained, record } => {
                    state
                        .accepted
                        .insert(sequence_id, split.canonical_bytes().to_vec());
                    state.queued_bytes = state.queued_bytes.saturating_add(retained);
                    state.replay_record_bytes = state.replay_record_bytes.saturating_add(record);
                    state.pending.push_back(split);
                    outcome.enqueued.push(sequence_id);
                }
                SplitDecision::Replay => outcome.replayed.push(sequence_id),
            }
        }

        // The terminal marker is idempotent: only the first transition wakes.
        let marked_terminal = no_more_splits && !state.no_more_splits;
        if marked_terminal {
            state.no_more_splits = true;
        }
        outcome.max_accepted_sequence = state.accepted.keys().next_back().copied();
        outcome.no_more_splits = state.no_more_splits;

        if !outcome.enqueued.is_empty() || marked_terminal {
            notify.arm();
        }
        Ok(outcome)
    }

    /// Take the next split, or report why there is none.
    pub fn poll(&self) -> SplitPoll<S> {
        let notify = self.observable.defer_notify();
        let mut state = self.state.lock().expect("split queue lock");
        if state.closed {
            return SplitPoll::Exhausted;
        }
        if let Some(split) = state.pending.pop_front() {
            state.queued_bytes = state
                .queued_bytes
                .saturating_sub(split.retained_size_in_bytes());
            // Popping the last split of a terminated plan node is itself a
            // state change siblings must observe, otherwise a driver parked on
            // "blocked" would never learn that the queue is now exhausted.
            if state.pending.is_empty() && state.no_more_splits {
                notify.arm();
            }
            return SplitPoll::Ready(split);
        }
        if state.no_more_splits {
            return SplitPoll::Exhausted;
        }
        SplitPoll::Blocked
    }

    /// Close the queue and wake every waiter exactly once.
    ///
    /// This is also the cancellation path. It is idempotent, drops any split
    /// still queued (nothing will run it), and leaves no waiter parked.
    pub fn close(&self) {
        let notify = self.observable.defer_notify();
        let mut state = self.state.lock().expect("split queue lock");
        if state.closed {
            return;
        }
        state.closed = true;
        state.pending.clear();
        state.queued_bytes = 0;
        drop(state);
        notify.arm();
    }

    /// Whether more splits may still arrive. Mirrors the scan dispatch latch so
    /// a source operator can combine both the same way.
    pub fn has_more(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        !state.closed && !state.no_more_splits
    }

    pub fn queue_empty(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.pending.is_empty()
    }

    /// Whether the queue can never produce another split.
    pub fn is_exhausted(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.closed || (state.no_more_splits && state.pending.is_empty())
    }

    pub fn no_more_splits(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.no_more_splits
    }

    pub fn is_closed(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.closed
    }

    pub fn stats(&self) -> SplitQueueStats {
        let state = self.state.lock().expect("split queue lock");
        SplitQueueStats {
            queued_splits: state.pending.len(),
            queued_bytes: state.queued_bytes,
            accepted_splits: state.accepted.len(),
            replay_record_bytes: state.replay_record_bytes,
            max_accepted_sequence: state.accepted.keys().next_back().copied(),
            no_more_splits: state.no_more_splits,
            closed: state.closed,
        }
    }

    fn error(
        &self,
        kind: SplitQueueErrorKind,
        sequence_id: Option<u64>,
        detail: impl Into<String>,
    ) -> SplitQueueError {
        SplitQueueError::new(kind, self.plan_node_id, sequence_id, detail)
    }
}

impl SplitQueue<ScheduledSplit> {
    /// Accept one validated wire assignment.
    pub fn offer(
        &self,
        assignment: &SplitAssignment,
    ) -> Result<SplitOfferOutcome, SplitQueueError> {
        self.offer_splits(
            assignment.plan_node_id(),
            assignment.splits().to_vec(),
            assignment.no_more_splits(),
        )
    }
}

/// The task attempt one set of queues belongs to.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskAttemptKey {
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
}

impl TaskAttemptKey {
    pub const fn new(execution_id: QueryExecutionId, fragment_instance_id: UniqueId) -> Self {
        Self {
            execution_id,
            fragment_instance_id,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }
}

impl fmt::Display for TaskAttemptKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "query={} attempt={} finst={}",
            self.execution_id.query_id(),
            self.execution_id.attempt_id().get(),
            self.fragment_instance_id
        )
    }
}

/// Every plan-node queue of exactly one task attempt.
///
/// The attempt id is part of the key, so a retry never reaches these queues: it
/// keys a different entry, which is allocated fresh with an empty replay map
/// and an empty sequence space. There is no reopen, so a handle to a closed
/// attempt can never be revived into a live one.
pub struct TaskAttemptSplitQueues<S = ScheduledSplit> {
    key: TaskAttemptKey,
    config: SplitQueueConfig,
    nodes: Mutex<HashMap<i32, Arc<SplitQueue<S>>>>,
    closed: AtomicBool,
}

impl<S: ScheduledSplitFacts> TaskAttemptSplitQueues<S> {
    fn new(key: TaskAttemptKey, config: SplitQueueConfig) -> Arc<Self> {
        Arc::new(Self {
            key,
            config,
            nodes: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        })
    }

    pub const fn key(&self) -> TaskAttemptKey {
        self.key
    }

    pub const fn config(&self) -> SplitQueueConfig {
        self.config
    }

    /// The queue for one plan node, created empty on first use.
    ///
    /// Both sides call this: the scan operator when it starts, possibly before
    /// any split has arrived, and the split receiver when the first assignment
    /// lands. After the attempt is closed this hands back a closed queue rather
    /// than growing the map, so a late caller observes termination instead of a
    /// queue that can never be served.
    pub fn queue(&self, plan_node_id: i32) -> Arc<SplitQueue<S>> {
        if self.closed.load(Ordering::Acquire) {
            let queue = SplitQueue::new(plan_node_id, self.config);
            queue.close();
            return queue;
        }
        let mut nodes = self.nodes.lock().expect("task attempt split queues lock");
        if self.closed.load(Ordering::Acquire) {
            let queue = SplitQueue::new(plan_node_id, self.config);
            queue.close();
            return queue;
        }
        Arc::clone(
            nodes
                .entry(plan_node_id)
                .or_insert_with(|| SplitQueue::new(plan_node_id, self.config)),
        )
    }

    /// The queue for one plan node, without creating it.
    pub fn existing_queue(&self, plan_node_id: i32) -> Option<Arc<SplitQueue<S>>> {
        let nodes = self.nodes.lock().expect("task attempt split queues lock");
        nodes.get(&plan_node_id).map(Arc::clone)
    }

    pub fn plan_node_ids(&self) -> Vec<i32> {
        let nodes = self.nodes.lock().expect("task attempt split queues lock");
        let mut ids: Vec<i32> = nodes.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn stats(&self) -> Vec<(i32, SplitQueueStats)> {
        let queues: Vec<(i32, Arc<SplitQueue<S>>)> = {
            let nodes = self.nodes.lock().expect("task attempt split queues lock");
            nodes
                .iter()
                .map(|(plan_node_id, queue)| (*plan_node_id, Arc::clone(queue)))
                .collect()
        };
        let mut stats: Vec<(i32, SplitQueueStats)> = queues
            .into_iter()
            .map(|(plan_node_id, queue)| (plan_node_id, queue.stats()))
            .collect();
        stats.sort_unstable_by_key(|(plan_node_id, _)| *plan_node_id);
        stats
    }

    /// Close every queue of this attempt. Idempotent; each queue wakes once.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let queues: Vec<Arc<SplitQueue<S>>> = {
            let mut nodes = self.nodes.lock().expect("task attempt split queues lock");
            nodes.drain().map(|(_, queue)| queue).collect()
        };
        for queue in queues {
            queue.close();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl TaskAttemptSplitQueues<ScheduledSplit> {
    /// Route one validated wire assignment to its plan-node queue.
    pub fn offer(
        &self,
        assignment: &SplitAssignment,
    ) -> Result<SplitOfferOutcome, SplitQueueError> {
        self.queue(assignment.plan_node_id()).offer(assignment)
    }
}

/// The owner of every live task attempt's split queues in one process role.
///
/// This is a plain owned object, deliberately not a process global: it is
/// created by whatever already owns query-scoped runtime state and dies with
/// it.
pub struct SplitQueueRegistry<S = ScheduledSplit> {
    attempts: Mutex<HashMap<TaskAttemptKey, Arc<TaskAttemptSplitQueues<S>>>>,
}

impl<S: ScheduledSplitFacts> SplitQueueRegistry<S> {
    pub fn new() -> Self {
        Self {
            attempts: Mutex::new(HashMap::new()),
        }
    }

    /// The queues of one task attempt, created on first use.
    ///
    /// Idempotent for one key: the same attempt always sees the same queues.
    /// A different attempt is a different key and therefore a different, empty
    /// set of queues; no state from an earlier attempt can reach it.
    pub fn open_attempt(
        &self,
        key: TaskAttemptKey,
        config: SplitQueueConfig,
    ) -> Arc<TaskAttemptSplitQueues<S>> {
        let mut attempts = self.attempts.lock().expect("split queue registry lock");
        Arc::clone(
            attempts
                .entry(key)
                .or_insert_with(|| TaskAttemptSplitQueues::new(key, config)),
        )
    }

    pub fn attempt(&self, key: TaskAttemptKey) -> Option<Arc<TaskAttemptSplitQueues<S>>> {
        let attempts = self.attempts.lock().expect("split queue registry lock");
        attempts.get(&key).map(Arc::clone)
    }

    /// Drop one attempt's queues and close them. Idempotent; returns whether
    /// this call was the one that removed the entry.
    pub fn close_attempt(&self, key: TaskAttemptKey) -> bool {
        let attempt = {
            let mut attempts = self.attempts.lock().expect("split queue registry lock");
            attempts.remove(&key)
        };
        match attempt {
            Some(attempt) => {
                attempt.close();
                true
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        let attempts = self.attempts.lock().expect("split queue registry lock");
        attempts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<S: ScheduledSplitFacts> Default for SplitQueueRegistry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use novarocks_types::{AttemptId, QueryId};

    use super::*;

    /// A neutral stand-in for one scheduled split.
    ///
    /// The wire type requires a present provider variant, so building it here
    /// would put a provider name into a file that must stay provider-free. The
    /// queue only ever sees [`ScheduledSplitFacts`], so exercising it through
    /// this view tests exactly the production code path.
    #[derive(Clone, Debug)]
    struct TestSplit {
        sequence_id: u64,
        plan_node_id: i32,
        canonical: Vec<u8>,
        retained: u64,
    }

    impl TestSplit {
        fn new(sequence_id: u64, plan_node_id: i32, canonical: &[u8]) -> Self {
            Self {
                sequence_id,
                plan_node_id,
                canonical: canonical.to_vec(),
                retained: canonical.len() as u64,
            }
        }

        fn with_retained(mut self, retained: u64) -> Self {
            self.retained = retained;
            self
        }
    }

    impl ScheduledSplitFacts for TestSplit {
        fn sequence_id(&self) -> u64 {
            self.sequence_id
        }

        fn plan_node_id(&self) -> i32 {
            self.plan_node_id
        }

        fn canonical_bytes(&self) -> &[u8] {
            &self.canonical
        }

        fn retained_size_in_bytes(&self) -> u64 {
            self.retained
        }
    }

    const NODE: i32 = 7;

    fn queue() -> Arc<SplitQueue<TestSplit>> {
        SplitQueue::new(NODE, SplitQueueConfig::default())
    }

    fn attempt_key(attempt: u64) -> TaskAttemptKey {
        TaskAttemptKey::new(
            QueryExecutionId::new(
                QueryId::new(1, 2),
                AttemptId::new(attempt).expect("attempt"),
            )
            .expect("execution id"),
            UniqueId::new(3, 4),
        )
    }

    fn counting_observer(queue: &SplitQueue<TestSplit>) -> Arc<AtomicUsize> {
        let counter = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&counter);
        queue.observable().add_observer(Arc::new(move || {
            observed.fetch_add(1, Ordering::AcqRel);
        }));
        counter
    }

    #[test]
    fn zero_splits_then_no_more_splits_is_a_clean_end_of_stream() {
        let queue = queue();
        assert!(matches!(queue.poll(), SplitPoll::Blocked));

        let outcome = queue
            .offer_splits(NODE, Vec::new(), true)
            .expect("terminal marker only");
        assert!(outcome.enqueued.is_empty());
        assert!(outcome.replayed.is_empty());
        assert_eq!(outcome.max_accepted_sequence, None);
        assert!(outcome.no_more_splits);

        // No synthetic empty split is manufactured to carry the marker.
        assert_eq!(queue.stats().queued_splits, 0);
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
        assert!(queue.is_exhausted());
    }

    #[test]
    fn an_empty_queue_is_blocked_and_a_later_offer_wakes_it() {
        let queue = queue();
        let woken = counting_observer(&queue);
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
        assert_eq!(woken.load(Ordering::Acquire), 0);

        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("first split");
        assert_eq!(woken.load(Ordering::Acquire), 1);

        match queue.poll() {
            SplitPoll::Ready(split) => assert_eq!(split.sequence_id(), 1),
            SplitPoll::Blocked | SplitPoll::Exhausted => panic!("the offered split must be ready"),
        }
        // Still not end of stream: no terminal marker has arrived.
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
    }

    #[test]
    fn a_pure_replay_offer_does_not_wake_waiters() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("first split");
        let woken = counting_observer(&queue);
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("exact replay");
        assert_eq!(woken.load(Ordering::Acquire), 0);
    }

    #[test]
    fn exact_replay_is_idempotent() {
        let queue = queue();
        let first = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(1, NODE, b"a"), TestSplit::new(2, NODE, b"b")],
                false,
            )
            .expect("first offer");
        assert_eq!(first.enqueued, vec![1, 2]);

        let replay = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(1, NODE, b"a"), TestSplit::new(2, NODE, b"b")],
                false,
            )
            .expect("exact replay");
        assert!(replay.enqueued.is_empty());
        assert_eq!(replay.replayed, vec![1, 2]);
        assert_eq!(replay.max_accepted_sequence, Some(2));
        assert_eq!(queue.stats().queued_splits, 2);
    }

    #[test]
    fn a_replay_that_extends_the_batch_enqueues_only_the_new_tail() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("first offer");
        let outcome = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(1, NODE, b"a"), TestSplit::new(2, NODE, b"b")],
                false,
            )
            .expect("retransmission with a new tail");
        assert_eq!(outcome.replayed, vec![1]);
        assert_eq!(outcome.enqueued, vec![2]);
    }

    #[test]
    fn the_same_sequence_with_different_bytes_is_a_conflict() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("first offer");
        let error = queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"different")], false)
            .expect_err("rewriting a sequence must be rejected");
        assert_eq!(error.kind(), SplitQueueErrorKind::SequenceConflict);
        assert_eq!(error.sequence_id(), Some(1));
        // The recorded split is untouched: no silent overwrite.
        assert_eq!(queue.stats().queued_splits, 1);
    }

    #[test]
    fn an_out_of_order_sequence_is_a_conflict() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(5, NODE, b"a")], false)
            .expect("first offer");
        let error = queue
            .offer_splits(NODE, vec![TestSplit::new(3, NODE, b"b")], false)
            .expect_err("a never-seen sequence below the high-water mark is a conflict");
        assert_eq!(error.kind(), SplitQueueErrorKind::SequenceConflict);
        assert_eq!(error.sequence_id(), Some(3));
    }

    #[test]
    fn a_batch_whose_sequences_do_not_increase_is_a_conflict() {
        let queue = queue();
        let error = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(2, NODE, b"a"), TestSplit::new(2, NODE, b"b")],
                false,
            )
            .expect_err("a repeated sequence inside one batch is a conflict");
        assert_eq!(error.kind(), SplitQueueErrorKind::SequenceConflict);
        assert_eq!(queue.stats().accepted_splits, 0);
    }

    #[test]
    fn a_new_split_after_the_terminal_marker_is_rejected() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("terminal batch");
        let error = queue
            .offer_splits(NODE, vec![TestSplit::new(2, NODE, b"b")], false)
            .expect_err("a split after the terminal marker must be rejected");
        assert_eq!(error.kind(), SplitQueueErrorKind::AfterNoMoreSplits);
        assert_eq!(error.sequence_id(), Some(2));
        assert_eq!(queue.stats().queued_splits, 1);
    }

    #[test]
    fn no_more_splits_is_idempotent_and_replayable() {
        let queue = queue();
        let first = queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("terminal batch");
        assert!(first.no_more_splits);

        let woken = counting_observer(&queue);
        // Retransmitting the terminal batch verbatim stays legal and changes
        // nothing, which is what a sender that missed the response does.
        let again = queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("terminal batch replay");
        assert!(again.no_more_splits);
        assert!(again.enqueued.is_empty());
        assert_eq!(again.replayed, vec![1]);
        assert_eq!(woken.load(Ordering::Acquire), 0);

        // A bare repeated marker is equally idempotent.
        let bare = queue
            .offer_splits(NODE, Vec::new(), true)
            .expect("bare terminal marker replay");
        assert!(bare.no_more_splits);
        assert_eq!(woken.load(Ordering::Acquire), 0);
    }

    #[test]
    fn draining_the_last_split_of_a_terminated_node_is_exhaustion() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("terminal batch");
        let woken = counting_observer(&queue);
        assert!(matches!(queue.poll(), SplitPoll::Ready(_)));
        // Siblings parked on "blocked" must learn that the queue is now done.
        assert_eq!(woken.load(Ordering::Acquire), 1);
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
    }

    #[test]
    fn close_is_idempotent_and_wakes_every_waiter_exactly_once() {
        let queue = queue();
        let first = counting_observer(&queue);
        let second = counting_observer(&queue);
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("one queued split");
        assert_eq!(first.load(Ordering::Acquire), 1);
        assert_eq!(second.load(Ordering::Acquire), 1);

        queue.close();
        queue.close();
        queue.close();
        assert_eq!(first.load(Ordering::Acquire), 2);
        assert_eq!(second.load(Ordering::Acquire), 2);

        assert!(queue.is_closed());
        assert!(queue.is_exhausted());
        assert_eq!(queue.stats().queued_bytes, 0);
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));

        let error = queue
            .offer_splits(NODE, vec![TestSplit::new(2, NODE, b"b")], false)
            .expect_err("a closed queue accepts nothing");
        assert_eq!(error.kind(), SplitQueueErrorKind::Closed);
    }

    #[test]
    fn an_assignment_for_another_plan_node_is_rejected() {
        let queue = queue();
        let error = queue
            .offer_splits(NODE + 1, Vec::new(), false)
            .expect_err("plan node mismatch");
        assert_eq!(error.kind(), SplitQueueErrorKind::PlanNodeMismatch);
    }

    #[test]
    fn the_retained_bytes_ceiling_rejects_an_oversized_offer() {
        let queue: Arc<SplitQueue<TestSplit>> = SplitQueue::new(
            NODE,
            SplitQueueConfig {
                max_queued_bytes: 64,
                max_replay_record_bytes: MAX_ASSIGNMENT_RETAINED_BYTES,
            },
        );
        queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(1, NODE, b"a").with_retained(48)],
                false,
            )
            .expect("within the ceiling");
        assert_eq!(queue.stats().queued_bytes, 48);

        let error = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(2, NODE, b"b").with_retained(32)],
                false,
            )
            .expect_err("the ceiling must reject the offer");
        assert_eq!(error.kind(), SplitQueueErrorKind::ResourceExhausted);
        // Rejected in full: neither the queue nor the replay record moved.
        assert_eq!(queue.stats().queued_splits, 1);
        assert_eq!(queue.stats().accepted_splits, 1);

        // Draining frees the budget again, which is what makes this a
        // backpressure signal rather than a permanent cap.
        assert!(matches!(queue.poll(), SplitPoll::Ready(_)));
        assert_eq!(queue.stats().queued_bytes, 0);
        queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(2, NODE, b"b").with_retained(32)],
                false,
            )
            .expect("accepted once the queue drained");
    }

    #[test]
    fn the_replay_record_ceiling_rejects_an_oversized_offer() {
        let queue: Arc<SplitQueue<TestSplit>> = SplitQueue::new(
            NODE,
            SplitQueueConfig {
                max_queued_bytes: MAX_ASSIGNMENT_RETAINED_BYTES,
                max_replay_record_bytes: 1,
            },
        );
        let error = queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect_err("the replay record ceiling must reject the offer");
        assert_eq!(error.kind(), SplitQueueErrorKind::ResourceExhausted);
        assert_eq!(queue.stats().accepted_splits, 0);
    }

    #[test]
    fn two_attempts_are_isolated() {
        let registry: SplitQueueRegistry<TestSplit> = SplitQueueRegistry::new();
        let first = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        let second = registry.open_attempt(attempt_key(2), SplitQueueConfig::default());
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(registry.len(), 2);

        first
            .queue(NODE)
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("first attempt");
        // The same sequence with different bytes is a conflict inside one
        // attempt, and a plain first offer in another: the sequence space and
        // the replay record are per attempt.
        second
            .queue(NODE)
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"different")], false)
            .expect("second attempt has a fresh sequence space");
        assert!(first.queue(NODE).no_more_splits());
        assert!(!second.queue(NODE).no_more_splits());

        // Closing one attempt leaves the other untouched, and the closed handle
        // can never be revived.
        assert!(registry.close_attempt(attempt_key(1)));
        assert!(!registry.close_attempt(attempt_key(1)));
        assert!(first.is_closed());
        assert!(first.queue(NODE).is_closed());
        assert!(!second.is_closed());
        assert!(!second.queue(NODE).is_closed());
        assert_eq!(registry.len(), 1);
        assert!(registry.attempt(attempt_key(1)).is_none());

        // Reopening the key allocates a fresh, empty attempt rather than
        // resurrecting the closed one.
        let reopened = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        assert!(!Arc::ptr_eq(&reopened, &first));
        assert!(!reopened.is_closed());
        assert_eq!(reopened.queue(NODE).stats(), SplitQueueStats::default());
    }

    #[test]
    fn one_attempt_keeps_one_queue_per_plan_node() {
        let registry: SplitQueueRegistry<TestSplit> = SplitQueueRegistry::new();
        let attempt = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        assert!(Arc::ptr_eq(&attempt.queue(NODE), &attempt.queue(NODE)));
        assert!(attempt.existing_queue(NODE + 1).is_none());
        let other = attempt.queue(NODE + 1);
        assert_eq!(other.plan_node_id(), NODE + 1);
        assert_eq!(attempt.plan_node_ids(), vec![NODE, NODE + 1]);

        // Reopening a live attempt is idempotent: the same key keeps the same
        // queues, so a second registration never resets a running scan.
        let again = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        assert!(Arc::ptr_eq(&attempt, &again));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn closing_an_attempt_closes_every_plan_node_queue_once() {
        let registry: SplitQueueRegistry<TestSplit> = SplitQueueRegistry::new();
        let attempt = registry.open_attempt(attempt_key(9), SplitQueueConfig::default());
        let first = attempt.queue(1);
        let second = attempt.queue(2);
        let first_woken = counting_observer(&first);
        let second_woken = counting_observer(&second);

        attempt.close();
        attempt.close();
        assert_eq!(first_woken.load(Ordering::Acquire), 1);
        assert_eq!(second_woken.load(Ordering::Acquire), 1);
        assert!(first.is_closed());
        assert!(second.is_closed());

        // A queue requested after the close is born closed rather than parked.
        let late = attempt.queue(3);
        assert!(late.is_closed());
        assert!(matches!(late.poll(), SplitPoll::Exhausted));
    }
}
