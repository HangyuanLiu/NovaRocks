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

#[cfg(test)]
use crate::cancellation::Cancellation;
use crate::control_storage::{CancellationStorage, SlotId};
use crate::{
    CancellationReason, CancellationView, LocalResourceAuthority, ResourceClass, ResourceConfig,
    WorkError, WorkloadObservationHandle,
    admission::{PendingAdmission, Stage},
    cancellation::{CancellationRequestOutcome, CancellationSuccessSealOutcome},
    observation::{
        ControlIntents, ObligationKey, ObligationRecord, OwnerState, RootLifecycleSnapshot,
    },
    queue::FairQueue,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkId(pub(crate) u64);

impl WorkId {
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkClass {
    Query,
    MaterializedView,
    Statistics,
    TableMaintenance,
    Management,
}

impl WorkClass {
    /// Work that can initiate warehouse computation shares the one query
    /// concurrency queue. Management requests retain their own protocol
    /// semantics and never consume this slot.
    pub const fn uses_warehouse_concurrency(self) -> bool {
        matches!(
            self,
            Self::Query | Self::MaterializedView | Self::Statistics | Self::TableMaintenance
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingState {
    Initializing,
    Ready,
    Closed,
}

#[derive(Clone, Debug)]
pub struct WorkRequest {
    pub class: WorkClass,
    pub deadline: Option<Instant>,
}

impl WorkRequest {
    pub fn new(class: WorkClass) -> Self {
        Self {
            class,
            deadline: None,
        }
    }
}

/// Bounds on one process's governance metadata and policy admission.
/// Byte limits here bound retained queue declarations, not physical memory.
#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub root_limit: usize,
    pub business_limit: usize,
    /// Concurrent logical queries admitted by the warehouse. This is separate
    /// from the legacy protocol-business limit while production callers move
    /// to `begin_query_root`.
    pub query_concurrency_limit: usize,
    pub preparation_limit: usize,
    pub execution_limit: usize,
    pub executions_per_root: usize,
    pub waiting_limit: usize,
    pub waiting_bytes: u64,
    /// Bound one capacity wait without changing the logical work's deadline.
    pub capacity_wait_timeout: Duration,
    pub restarts_per_work: usize,
    pub old_attempts_per_work: usize,
    pub old_attempts_limit: usize,
    pub unknown_creates_limit: usize,
    /// Includes parent, child and unresolved orphan records.
    pub scope_records_limit: usize,
    /// Includes known work and external completion obligations.
    pub obligation_records_limit: usize,
    pub control_inflight_limit: usize,
    pub control_ready_limit: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            root_limit: 256,
            business_limit: 256,
            query_concurrency_limit: 256,
            preparation_limit: 16,
            execution_limit: 64,
            executions_per_root: 4,
            waiting_limit: 1024,
            waiting_bytes: 64 * 1024 * 1024,
            capacity_wait_timeout: Duration::from_secs(30),
            restarts_per_work: 2,
            old_attempts_per_work: 2,
            old_attempts_limit: 128,
            unknown_creates_limit: 4096,
            scope_records_limit: 8192,
            obligation_records_limit: 8192,
            control_inflight_limit: 16,
            control_ready_limit: 256,
        }
    }
}

impl WorkloadConfig {
    pub fn validate(&self) -> Result<(), WorkError> {
        for (name, value) in [
            ("root_limit", self.root_limit),
            ("business_limit", self.business_limit),
            ("query_concurrency_limit", self.query_concurrency_limit),
            ("preparation_limit", self.preparation_limit),
            ("execution_limit", self.execution_limit),
            ("executions_per_root", self.executions_per_root),
            ("waiting_limit", self.waiting_limit),
            ("restarts_per_work", self.restarts_per_work),
            ("old_attempts_per_work", self.old_attempts_per_work),
            ("old_attempts_limit", self.old_attempts_limit),
            ("unknown_creates_limit", self.unknown_creates_limit),
            ("scope_records_limit", self.scope_records_limit),
            ("obligation_records_limit", self.obligation_records_limit),
            ("control_inflight_limit", self.control_inflight_limit),
            ("control_ready_limit", self.control_ready_limit),
        ] {
            if value == 0 || value > 1_000_000 {
                return Err(WorkError::InvalidConfig(name));
            }
        }
        if self.waiting_bytes == 0 || self.waiting_bytes > isize::MAX as u64 {
            return Err(WorkError::InvalidConfig("waiting_bytes"));
        }
        if self.capacity_wait_timeout.is_zero()
            || self.capacity_wait_timeout > Duration::from_secs(30)
        {
            return Err(WorkError::InvalidConfig("capacity_wait_timeout"));
        }
        if self.business_limit > self.root_limit || self.root_limit > self.scope_records_limit {
            return Err(WorkError::InvalidConfig("business/root/scope limits"));
        }
        if self.old_attempts_per_work > self.old_attempts_limit
            || self.old_attempts_limit > self.obligation_records_limit
            || self.unknown_creates_limit > self.obligation_records_limit
        {
            return Err(WorkError::InvalidConfig("residual/obligation limits"));
        }
        Ok(())
    }
}

pub(crate) struct Node {
    pub parent: Option<WorkId>,
    pub root: WorkId,
    pub class: WorkClass,
    pub cancellation: SlotId,
    pub children: usize,
    pub completed: bool,
    pub owner: OwnerState,
    pub handoffs: u64,
    pub business: bool,
    pub query_admitted: bool,
    pub stages: BTreeSet<Stage>,
    pub pending_admissions: usize,
    pub root_executions: usize,
    pub obligations: BTreeMap<ObligationKey, ObligationRecord>,
    pub restarts: usize,
    pub resource_holders: usize,
    pub resource_waiters: usize,
    pub reserved_bytes: u64,
    pub used_bytes: u64,
    pub result_windows: crate::ResultCapacitySnapshot,
    /// The original root has one queued or not-yet-received query admission,
    /// including admissions that request no result window.
    pub pending_query: bool,
    /// The result window class a queued query admission must take together
    /// with its computation permit; `None` when it is not queued or takes none.
    pub pending_query_window: Option<crate::ResultWindowClass>,
    pub control_pending: ControlIntents,
    pub cancellation_signalled: bool,
    pub terminal_cancel_settled: bool,
    pub control_queued: bool,
    pub control_inflight: bool,
}

impl Node {
    fn new(parent: Option<WorkId>, root: WorkId, class: WorkClass, cancellation: SlotId) -> Self {
        Self {
            parent,
            root,
            class,
            cancellation,
            children: 0,
            completed: false,
            owner: OwnerState::Active,
            handoffs: 0,
            business: false,
            query_admitted: false,
            stages: BTreeSet::new(),
            pending_admissions: 0,
            root_executions: 0,
            obligations: BTreeMap::new(),
            restarts: 0,
            resource_holders: 0,
            resource_waiters: 0,
            reserved_bytes: 0,
            used_bytes: 0,
            result_windows: crate::ResultCapacitySnapshot::default(),
            pending_query: false,
            pending_query_window: None,
            control_pending: ControlIntents::empty(),
            cancellation_signalled: false,
            terminal_cancel_settled: false,
            control_queued: false,
            control_inflight: false,
        }
    }

    pub(crate) fn check(&self, storage: &CancellationStorage) -> Result<(), WorkError> {
        if self.completed {
            return Err(WorkError::Released);
        }
        if let Some(reason) = storage.cell(self.cancellation).check_reason() {
            return Err(WorkError::Cancelled(reason));
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) joint: Option<crate::joint_admission::ledger::JointLedger>,
    pub(crate) cancellations: CancellationStorage,
    pub closed: bool,
    pub ready: bool,
    pub next_id: u64,
    pub nodes: BTreeMap<WorkId, Node>,
    pub roots: usize,
    pub businesses: usize,
    pub admitted_queries: usize,
    pub preparation: usize,
    pub execution: usize,
    pub requests: BTreeMap<u64, PendingAdmission>,
    pub resource_waiters: BTreeSet<(WorkId, ResourceClass)>,
    pub preparation_queue: FairQueue,
    pub execution_queue: FairQueue,
    pub query_queue: FairQueue,
    pub waiting_bytes: u64,
    pub old_attempts: usize,
    pub unknown_creates: usize,
    pub obligations: usize,
    pub obligation_endings: crate::ObligationEndSnapshot,
    pub control_ready: VecDeque<WorkId>,
    pub control_waiting: BTreeSet<WorkId>,
    pub control_inflight: usize,
    pub root_lifecycle: RootLifecycleSnapshot,
    pub control_cursor: Option<WorkId>,
    pub data_reserved: u64,
    pub data_used: u64,
    pub control_reserved: u64,
    pub control_used: u64,
    pub peak_held_bytes: u64,
    pub result_capacity: Option<crate::ResultCapacityConfig>,
    pub result_windows: crate::ResultCapacitySnapshot,
    pub peak_waiting: usize,
    pub peak_waiting_records: usize,
    pub peak_waiting_bytes: u64,
    pub progress_revision: u64,
}

impl State {
    fn take_root_cancellation(
        &mut self,
        class: WorkClass,
        id: WorkId,
        deadline: Option<Instant>,
    ) -> Result<SlotId, WorkError> {
        let slot = self.cancellations.take_root(class, id, deadline)?;
        if let Some(claim) = self.cancellations.slots[slot.0].claim {
            if let Err(error) = self
                .joint
                .as_mut()
                .unwrap()
                .activate_control_slot(slot, claim)
            {
                self.cancellations.retire(slot);
                return Err(error);
            }
        }
        Ok(slot)
    }

    fn take_child_cancellation(
        &mut self,
        parent: SlotId,
        class: WorkClass,
        id: WorkId,
        deadline: Option<Instant>,
    ) -> Result<SlotId, WorkError> {
        if self.cancellations.fixed {
            let original = &self.cancellations.slots[parent.0];
            if original.class != class {
                return Err(WorkError::Conflict);
            }
            if !self
                .joint
                .as_ref()
                .unwrap()
                .control_is_empty(original.claim.unwrap())
            {
                // Bound parents use request_child and its original installed receipt.
                return Err(WorkError::NotReady);
            }
        }
        let slot = self.cancellations.take_child(parent, class, id, deadline)?;
        if let Some(claim) = self.cancellations.slots[slot.0].claim {
            if let Err(error) = self
                .joint
                .as_mut()
                .unwrap()
                .activate_control_slot(slot, claim)
            {
                self.cancellations.retire(slot);
                return Err(error);
            }
        }
        Ok(slot)
    }

    /// Publish only the exact slot already committed by the original proof.
    /// Every fallible preflight precedes registration of the cancellation edge.
    pub(crate) fn install_bound_child(
        &mut self,
        inner: &Arc<Inner>,
        parent: WorkId,
        slot: SlotId,
        deadline: Option<Instant>,
    ) -> Result<WorkOwner, WorkError> {
        let node = self.nodes.get(&parent).ok_or(WorkError::Released)?;
        node.check(&self.cancellations)?;
        let root = node.root;
        let class = node.class;
        let parent_slot = node.cancellation;
        let children = node
            .children
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        if self.nodes.len() >= inner.config.scope_records_limit {
            return Err(WorkError::Capacity("scope records"));
        }
        let id = WorkId(self.next_id()?);
        self.cancellations
            .install_granted_child(slot, parent_slot, id, deadline)?;
        self.nodes
            .insert(id, Node::new(Some(parent), root, class, slot));
        self.nodes.get_mut(&parent).unwrap().children = children;
        Ok(WorkOwner {
            scope: Some(WorkScope {
                inner: Arc::clone(inner),
                id,
            }),
        })
    }

    pub(crate) fn waiting_records(&self) -> usize {
        self.requests.len() + self.resource_waiters.len()
    }

    pub(crate) fn record_waiting_peak(&mut self) {
        self.peak_waiting_records = self.peak_waiting_records.max(self.waiting_records());
    }

    pub(crate) fn advance_progress_revision(&mut self) {
        self.progress_revision = self.progress_revision.wrapping_add(1);
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.closed
            && self
                .joint
                .as_ref()
                .is_none_or(|ledger| ledger.is_drained(&self.cancellations))
            && self.cancellations.drained()
            && self.nodes.is_empty()
            && self.roots == 0
            && self.businesses == 0
            && self.admitted_queries == 0
            && self.preparation == 0
            && self.execution == 0
            && self.requests.is_empty()
            && self.resource_waiters.is_empty()
            && self.waiting_bytes == 0
            && self.old_attempts == 0
            && self.unknown_creates == 0
            && self.obligations == 0
            && self.control_ready.is_empty()
            && self.control_waiting.is_empty()
            && self.control_inflight == 0
            && self.data_reserved == 0
            && self.data_used == 0
            && self.control_reserved == 0
            && self.control_used == 0
    }

    pub(crate) fn next_id(&mut self) -> Result<u64, WorkError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(self.next_id)
    }

    pub(crate) fn collect(&mut self, mut id: WorkId) {
        loop {
            let Some(node) = self.nodes.get(&id) else {
                return;
            };
            if !node.completed
                || node.children != 0
                || node.business
                || node.query_admitted
                || !node.stages.is_empty()
                || node.pending_admissions != 0
                || !node.obligations.is_empty()
                || node.resource_holders != 0
                || node.resource_waiters != 0
                || !node.control_pending.is_empty()
                || node.control_queued
                || node.control_inflight
            {
                return;
            }
            let node = self.nodes.remove(&id).unwrap();
            self.cancellations.retire(node.cancellation);
            if let Some(parent) = node.parent {
                self.nodes.get_mut(&parent).unwrap().children -= 1;
                id = parent;
            } else {
                self.roots -= 1;
                return;
            }
        }
    }
}

pub(crate) struct Inner {
    pub config: WorkloadConfig,
    pub resource_config: Option<Arc<ResourceConfig>>,
    pub state: Mutex<State>,
    pub changed: Notify,
    reclaim_generation: AtomicU64,
}

/// A raw cancellation alias may outlive the State transaction that cloned it.
/// Declare this before every escaping alias so unwind drops those aliases first.
/// This weak observer cannot keep a Work node or the process authority alive.
pub(crate) struct ReclaimWakeOnExit {
    inner: Weak<Inner>,
}

impl ReclaimWakeOnExit {
    pub(crate) fn observe(inner: &Weak<Inner>) -> Self {
        Self {
            inner: inner.clone(),
        }
    }

    fn new(inner: &Arc<Inner>) -> Self {
        Self {
            inner: Arc::downgrade(inner),
        }
    }
}

impl Drop for ReclaimWakeOnExit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.notify_reclaim_progress();
        }
    }
}

impl Inner {
    /// Capture both event sources while the caller holds the original State lock.
    /// Equality only: neither wrapping counter expresses elapsed work or order.
    pub(crate) fn capture_progress(&self, state: &State) -> WorkloadProgressRevision {
        WorkloadProgressRevision {
            state_revision: state.progress_revision,
            reclaim_generation: self.reclaim_generation.load(Ordering::Acquire),
        }
    }

    /// Called only after the notifying cancellation Arc has actually exited.
    /// Notify only outside State; this does not release resources or author terminal facts.
    pub(crate) fn notify_reclaim_progress(&self) {
        self.reclaim_generation.fetch_add(1, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// Allocation and observation facts do not drive policy queues. Keep their
    /// hot path independent of the number of scopes and admission requests.
    pub(crate) fn update_facts<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let result = {
            let mut state = self.state.lock().unwrap();
            let result = f(&mut state);
            state.advance_progress_revision();
            result
        };
        self.changed.notify_waiters();
        result
    }

    /// Mutate reservation and allocation facts without waking capacity waiters.
    /// Capacity release separately notifies waiters after the actual holder exits.
    pub(crate) fn update_facts_silent<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.state.lock().unwrap();
        let result = f(&mut state);
        state.advance_progress_revision();
        result
    }

    pub(crate) fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let (result, wakers) = {
            let mut state = self.state.lock().unwrap();
            let result = f(&mut state);
            let wakers = crate::admission::dispatch(&mut state, &self.config);
            crate::observation::fill_control(&mut state, &self.config);
            state.advance_progress_revision();
            (result, wakers)
        };
        // Never invoke arbitrary wake implementations while holding the owner lock.
        for waker in wakers {
            waker.wake();
        }
        self.changed.notify_waiters();
        result
    }
}

/// Role-owned policy and control-progression authority.
///
/// This type is deliberately not cloneable. Role composition keeps it in the
/// process owner and injects only the narrower handles returned by
/// [`Self::try_new_split`] into long-lived services.
// Design: ADR-0147 (docs/adr/ADR-0147-process-local-work-governance-separates-responsibility-and-resources.md)
pub struct WorkloadControl {
    pub(crate) inner: Arc<Inner>,
}

/// Cloneable capability that can only begin a root business responsibility.
///
/// Holding this handle grants no readiness, drain, control-progression,
/// observation, recovery, or local-resource authority.
#[derive(Clone)]
pub struct RootAdmissionHandle {
    inner: Arc<Inner>,
}

/// Cloneable capability used only by role supervision to deliver elapsed
/// deadline cancellation. It cannot admit, release, or otherwise mutate
/// business responsibility.
#[derive(Clone)]
pub struct DeadlineExpiryHandle {
    inner: Arc<Inner>,
}

/// Complete process-composition result for one workload authority.
///
/// The owner is unique. Each handle is intentionally a separate capability so
/// a business service cannot obtain process lifecycle authority by cloning the
/// object it was injected.
pub struct WorkloadControlParts {
    pub owner: WorkloadControl,
    pub root_admission: RootAdmissionHandle,
    pub observation: WorkloadObservationHandle,
    pub resources: LocalResourceAuthority,
}

/// Process-composition capabilities for a role with no allocation budget.
pub struct CountedWorkloadControlParts {
    pub owner: WorkloadControl,
    pub root_admission: RootAdmissionHandle,
    pub observation: WorkloadObservationHandle,
}

/// Proof that the unique workload owner closed admission and observed a fully
/// drained local authority before relinquishing process ownership.
#[derive(Debug)]
pub struct WorkloadShutdown {
    _private: (),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadShutdownError {
    AdmissionOpen,
    NotDrained,
}

impl std::fmt::Display for WorkloadShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdmissionOpen => f.write_str("New work admission is still open"),
            Self::NotDrained => f.write_str("Workload authority has not fully drained"),
        }
    }
}

impl std::error::Error for WorkloadShutdownError {}

/// A failed shutdown returns the unique owner so role composition can continue
/// driving control and convergence.
pub struct WorkloadShutdownFailure {
    error: WorkloadShutdownError,
    owner: WorkloadControl,
}

/// Opaque process-local token used to await a later governance event.
///
/// Equality is the only supported interpretation. Both counters may wrap;
/// callers must never derive elapsed work or ordering distances from them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkloadProgressRevision {
    state_revision: u64,
    reclaim_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadProgress {
    ControlReady(WorkloadProgressRevision),
    StateChanged(WorkloadProgressRevision),
    Drained(WorkloadProgressRevision),
}

impl WorkloadProgress {
    pub fn revision(self) -> WorkloadProgressRevision {
        match self {
            Self::ControlReady(revision)
            | Self::StateChanged(revision)
            | Self::Drained(revision) => revision,
        }
    }
}

impl WorkloadShutdownFailure {
    pub fn error(&self) -> WorkloadShutdownError {
        self.error
    }

    pub fn into_parts(self) -> (WorkloadShutdownError, WorkloadControl) {
        (self.error, self.owner)
    }
}

fn try_begin_root(inner: &Arc<Inner>, request: WorkRequest) -> Result<RootWork, WorkError> {
    try_begin_root_window(inner, request, None).map(|(root, _)| root)
}

fn try_begin_root_window(
    inner: &Arc<Inner>,
    request: WorkRequest,
    window: Option<crate::ResultWindowClass>,
) -> Result<(RootWork, Option<crate::ResultWindowGrant>), WorkError> {
    inner.update(|state| {
        let class = request.class;
        let result = (|| {
            if window == Some(crate::ResultWindowClass::Closing)
                || (window.is_some() && class.uses_warehouse_concurrency())
            {
                return Err(WorkError::Conflict);
            }
            if state.closed {
                return Err(WorkError::Closed);
            }
            if !state.ready {
                return Err(WorkError::NotReady);
            }
            if state.roots >= inner.config.root_limit {
                return Err(WorkError::Capacity("root responsibilities"));
            }
            if state.businesses >= inner.config.business_limit {
                return Err(WorkError::Capacity("business admission"));
            }
            if state.nodes.len() >= inner.config.scope_records_limit {
                return Err(WorkError::Capacity("scope records"));
            }
            let id = WorkId(state.next_id()?);
            let cancellation = state.take_root_cancellation(class, id, request.deadline)?;
            let mut node = Node::new(None, id, class, cancellation);
            node.business = true;
            state.nodes.insert(id, node);
            state.roots += 1;
            state.businesses += 1;
            let scope = WorkScope {
                inner: Arc::clone(inner),
                id,
            };
            let grant = match window {
                None => None,
                Some(class) => match crate::result_window::reserve_window(state, &scope, class) {
                    Ok(grant) => Some(grant),
                    Err(error) => {
                        // No owner or permit has escaped this transaction.
                        let node = state.nodes.remove(&id).unwrap();
                        state.cancellations.retire(node.cancellation);
                        state.roots -= 1;
                        state.businesses -= 1;
                        return Err(error);
                    }
                },
            };
            Ok((
                RootWork {
                    owner: WorkOwner {
                        scope: Some(scope.clone()),
                    },
                    business: BusinessPermit { scope: Some(scope) },
                },
                grant,
            ))
        })();
        if result.is_err() {
            state.root_lifecycle.rejected_admissions.increment(class);
        }
        result
    })
}

fn begin_warehouse_root(
    inner: &Arc<Inner>,
    request: WorkRequest,
) -> Result<PendingQueryRoot, WorkError> {
    inner.update(|state| {
        let class = request.class;
        let result = (|| {
            if !class.uses_warehouse_concurrency() {
                return Err(WorkError::Conflict);
            }
            if state.closed {
                return Err(WorkError::Closed);
            }
            if !state.ready {
                return Err(WorkError::NotReady);
            }
            // A warehouse query enters the one fair query queue below. Root
            // and scope-record accounting must not become a second Frontend
            // admission decision before that queue can observe the request.
            let id = WorkId(state.next_id()?);
            let cancellation = state.take_root_cancellation(class, id, request.deadline)?;
            state
                .nodes
                .insert(id, Node::new(None, id, class, cancellation));
            state.roots += 1;
            Ok(PendingQueryRoot {
                owner: WorkOwner {
                    scope: Some(WorkScope {
                        inner: Arc::clone(inner),
                        id,
                    }),
                },
            })
        })();
        if result.is_err() {
            state.root_lifecycle.rejected_admissions.increment(class);
        }
        result
    })
}

impl RootAdmissionHandle {
    pub fn try_begin_root(&self, request: WorkRequest) -> Result<RootWork, WorkError> {
        try_begin_root(&self.inner, request)
    }

    /// Admit nonqueued work and its complete result position in one authority
    /// transaction. Refusal publishes neither a root nor a business permit.
    pub fn try_begin_root_with_result(
        &self,
        request: WorkRequest,
        class: crate::ResultWindowClass,
    ) -> Result<(RootWork, crate::ResultWindowGrant), WorkError> {
        try_begin_root_window(&self.inner, request, Some(class)).map(|(root, window)| {
            (
                root,
                window.expect("requested result position is returned atomically"),
            )
        })
    }

    /// Register a logical query before it receives warehouse concurrency.
    /// The caller installs protocol cancellation from the returned owner, then
    /// awaits `WorkScope::admit_query` without creating a second root.
    pub fn begin_query_root(&self, request: WorkRequest) -> Result<PendingQueryRoot, WorkError> {
        if request.class != WorkClass::Query {
            return Err(WorkError::Conflict);
        }
        begin_warehouse_root(&self.inner, request)
    }

    /// Register one compute-producing warehouse root before it receives the
    /// shared query concurrency permit. Statistics, MV refresh and table
    /// maintenance use the same queue as foreground queries; management work
    /// remains outside this admission class.
    pub fn begin_warehouse_root(
        &self,
        request: WorkRequest,
    ) -> Result<PendingQueryRoot, WorkError> {
        begin_warehouse_root(&self.inner, request)
    }
}

impl WorkloadControl {
    /// Construct a workload owner with an explicit allocation budget.
    /// Inject narrow capabilities into product-lived services.
    pub fn try_new(config: WorkloadConfig, resources: ResourceConfig) -> Result<Self, WorkError> {
        config.validate()?;
        resources.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                resource_config: Some(Arc::new(resources)),
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                reclaim_generation: AtomicU64::new(0),
            }),
        })
    }

    /// Construct one unique process owner and the complete set of narrow,
    /// cloneable service capabilities backed by that same local authority.
    pub fn try_new_split(
        config: WorkloadConfig,
        resources: ResourceConfig,
    ) -> Result<WorkloadControlParts, WorkError> {
        let owner = Self::try_new(config, resources)?;
        Ok(WorkloadControlParts {
            root_admission: owner.root_admission(),
            observation: owner.observation(),
            resources: owner.resources()?,
            owner,
        })
    }

    /// Construct responsibility, admission and lifecycle control without a
    /// generic allocation authority. Result windows are installed separately.
    pub fn try_new_counted(
        config: WorkloadConfig,
    ) -> Result<CountedWorkloadControlParts, WorkError> {
        config.validate()?;
        let owner = Self {
            inner: Arc::new(Inner {
                config,
                resource_config: None,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                reclaim_generation: AtomicU64::new(0),
            }),
        };
        Ok(CountedWorkloadControlParts {
            root_admission: owner.root_admission(),
            observation: owner.observation(),
            owner,
        })
    }

    pub fn root_admission(&self) -> RootAdmissionHandle {
        RootAdmissionHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn observation(&self) -> WorkloadObservationHandle {
        WorkloadObservationHandle::new(Arc::clone(&self.inner))
    }

    pub fn try_begin_root(&self, request: WorkRequest) -> Result<RootWork, WorkError> {
        try_begin_root(&self.inner, request)
    }

    pub fn begin_query_root(&self, request: WorkRequest) -> Result<PendingQueryRoot, WorkError> {
        if request.class != WorkClass::Query {
            return Err(WorkError::Conflict);
        }
        begin_warehouse_root(&self.inner, request)
    }

    pub fn begin_warehouse_root(
        &self,
        request: WorkRequest,
    ) -> Result<PendingQueryRoot, WorkError> {
        begin_warehouse_root(&self.inner, request)
    }

    /// Returns the narrow process-supervision capability for observing new
    /// deadlines and delivering their cancellation intent without handing a
    /// long-lived task the unique workload owner.
    pub fn deadline_expiry_handle(&self) -> DeadlineExpiryHandle {
        DeadlineExpiryHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Close new roots; existing work and control retain their authority.
    pub fn close_admission(&self) {
        self.inner.update(|state| state.closed = true);
    }

    /// Requests first-wins cancellation for every active root owned by this
    /// process authority. Closing admission and cancelling existing roots are
    /// deliberately separate lifecycle facts: callers use this only after a
    /// bounded drain has elapsed.
    pub fn cancel_active_roots(&self, reason: CancellationReason) -> usize {
        let _reclaim = ReclaimWakeOnExit::new(&self.inner);
        let roots = {
            let state = self.inner.state.lock().unwrap();
            state
                .nodes
                .iter()
                .filter(|(_, node)| node.parent.is_none() && !node.completed)
                .map(|(&id, node)| {
                    (
                        id,
                        node.class,
                        Arc::clone(state.cancellations.cell(node.cancellation)),
                    )
                })
                .collect::<Vec<_>>()
        };

        let mut requested = 0;
        for (id, class, cancellation) in roots {
            let outcome = cancellation.request(reason.clone());
            if outcome == CancellationRequestOutcome::Requested {
                requested += 1;
                if reason == CancellationReason::FrontendDrainDeadlineExceeded {
                    self.inner.update(|state| {
                        state
                            .root_lifecycle
                            .frontend_drain_deadline_cancelled
                            .increment(class);
                    });
                }
            }
            if outcome != CancellationRequestOutcome::SuccessSealed {
                self.inner
                    .update(|state| {
                        let Some(node) = state.nodes.get_mut(&id) else {
                            return Ok(());
                        };
                        if node.parent.is_some() || node.completed || node.cancellation_signalled {
                            return Ok(());
                        }
                        node.cancellation_signalled = true;
                        crate::observation::queue_control(state, id, crate::ControlIntent::Cancel)
                    })
                    .expect("active root cancellation updates a live local authority");
            }
        }
        requested
    }

    /// Role composition opens data admission only after its required services
    /// are ready. Repeating readiness is harmless; closed admission never reopens.
    pub fn mark_ready(&self) -> Result<(), WorkError> {
        self.inner.update(|state| {
            if state.closed {
                return Err(WorkError::Closed);
            }
            state.ready = true;
            Ok(())
        })
    }

    /// Obtain the explicitly installed allocation authority. Count-only role
    /// composition has no allocation budget and cannot mint this capability.
    pub fn resources(&self) -> Result<LocalResourceAuthority, WorkError> {
        let config = self
            .inner
            .resource_config
            .as_ref()
            .ok_or(WorkError::NotReady)?;
        Ok(LocalResourceAuthority {
            inner: Arc::clone(&self.inner),
            config: Arc::clone(config),
        })
    }

    /// Adopt an orphan without manufacturing a stop or release fact.
    pub fn adopt(&self, id: WorkId) -> Result<WorkOwner, WorkError> {
        self.inner.update(|state| {
            let node = state.nodes.get_mut(&id).ok_or(WorkError::Released)?;
            if node.owner != OwnerState::Orphaned {
                return Err(WorkError::OwnerStillPresent);
            }
            node.owner = OwnerState::Active;
            Ok(WorkOwner {
                scope: Some(WorkScope {
                    inner: Arc::clone(&self.inner),
                    id,
                }),
            })
        })
    }

    /// Role supervision drives this together with `next_deadline`; no work owns
    /// an OS waiting thread, and timeout only creates cancellation intent.
    pub fn expire_deadlines(&self) {
        self.deadline_expiry_handle().expire_deadlines();
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadline_expiry_handle().next_deadline()
    }

    /// Capture the current event revision before attempting a state-dependent
    /// owner operation such as shutdown.
    pub fn progress_revision(&self) -> WorkloadProgressRevision {
        self.inner
            .capture_progress(&self.inner.state.lock().unwrap())
    }

    /// Await a later authority event without dedicating a thread or polling.
    ///
    /// The subscription is armed before state inspection, so convergence
    /// between the caller's failed shutdown attempt and this future cannot be
    /// lost. A drained authority is returned immediately even when its revision
    /// equals `after`.
    pub async fn wait_progress(&self, after: WorkloadProgressRevision) -> WorkloadProgress {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let progress = {
                let state = self.inner.state.lock().unwrap();
                let revision = self.inner.capture_progress(&state);
                if state.is_drained() {
                    Some(WorkloadProgress::Drained(revision))
                } else if !state.control_ready.is_empty()
                    && state.control_inflight < self.inner.config.control_inflight_limit
                {
                    Some(WorkloadProgress::ControlReady(revision))
                } else if revision != after {
                    Some(WorkloadProgress::StateChanged(revision))
                } else {
                    None
                }
            };
            if let Some(progress) = progress {
                return progress;
            }
            changed.await;
        }
    }

    /// Consume the unique owner after admission has closed and every local
    /// responsibility, waiter, control intent, and allocation has converged.
    ///
    /// On failure the owner is returned so the caller can continue driving
    /// convergence. This method never fabricates cancellation, stop, or
    /// release facts.
    pub fn shutdown(self) -> Result<WorkloadShutdown, WorkloadShutdownFailure> {
        let result = {
            let state = self.inner.state.lock().unwrap();
            if !state.closed {
                Err(WorkloadShutdownError::AdmissionOpen)
            } else if !state.is_drained() {
                Err(WorkloadShutdownError::NotDrained)
            } else {
                Ok(WorkloadShutdown { _private: () })
            }
        };
        result.map_err(|error| WorkloadShutdownFailure { error, owner: self })
    }
}

impl DeadlineExpiryHandle {
    /// Role supervision drives this together with [`Self::next_deadline`]; no
    /// work owns an OS waiting thread, and timeout only creates cancellation
    /// intent.
    pub fn expire_deadlines(&self) {
        let _reclaim = ReclaimWakeOnExit::new(&self.inner);
        let notifications = self.inner.update(|state| {
            let cancelled = state
                .nodes
                .iter()
                .filter_map(|(&id, node)| {
                    (!node.completed
                        && !node.cancellation_signalled
                        && state
                            .cancellations
                            .cell(node.cancellation)
                            .check_reason()
                            .is_some())
                    .then_some(id)
                })
                .collect::<Vec<_>>();
            let mut notifications = Vec::new();
            for id in cancelled {
                let cancellation =
                    Arc::clone(state.cancellations.cell(state.nodes[&id].cancellation));
                let reason = cancellation.check_reason().unwrap();
                notifications.push((cancellation, reason));
                state.nodes.get_mut(&id).unwrap().cancellation_signalled = true;
                crate::observation::queue_control(state, id, crate::ControlIntent::Cancel).unwrap();
            }
            notifications
        });
        for (cancellation, reason) in notifications {
            cancellation.request(reason);
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        let state = self.inner.state.lock().unwrap();
        state
            .nodes
            .values()
            .filter(|node| !node.completed && !node.cancellation_signalled)
            .filter_map(|node| state.cancellations.cell(node.cancellation).deadline())
            .chain(
                state
                    .requests
                    .values()
                    .filter(|request| {
                        !matches!(request.state, crate::admission::AdmissionState::Rejected(_))
                    })
                    .map(|request| request.wait_deadline),
            )
            .min()
    }

    /// Capture the current event revision before waiting for a later deadline
    /// or state transition.
    pub fn progress_revision(&self) -> WorkloadProgressRevision {
        self.inner
            .capture_progress(&self.inner.state.lock().unwrap())
    }

    /// Wait for any state revision without treating an idle, open authority as
    /// shutdown-drained. Deadline supervision uses this to learn about a newly
    /// admitted earlier deadline without polling or spinning while idle.
    pub async fn wait_state_change(&self, after: WorkloadProgressRevision) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let advanced = {
                let state = self.inner.state.lock().unwrap();
                self.inner.capture_progress(&state) != after
            };
            if advanced {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for WorkloadControl {
    fn drop(&mut self) {
        // Losing the unique process owner must fail closed. Existing work and
        // cleanup capabilities remain valid and retain their real accounting.
        self.close_admission();
    }
}

pub struct RootWork {
    pub owner: WorkOwner,
    pub business: BusinessPermit,
}

/// A registered logical query that has a cancellation authority but has not
/// yet received warehouse concurrency. Dropping its owner follows the normal
/// orphan/cancellation path; it never consumes a query permit by itself.
pub struct PendingQueryRoot {
    pub owner: WorkOwner,
}

/// Cloneable, non-forgeable attribution. Clones do not create work or permits.
#[derive(Clone)]
pub struct WorkScope {
    pub(crate) inner: Arc<Inner>,
    pub(crate) id: WorkId,
}

impl WorkScope {
    pub fn id(&self) -> WorkId {
        self.id
    }

    pub fn check(&self) -> Result<(), WorkError> {
        let state = self.inner.state.lock().unwrap();
        state
            .nodes
            .get(&self.id)
            .ok_or(WorkError::Released)?
            .check(&state.cancellations)
    }

    pub fn cancellation(&self) -> Result<CancellationView, WorkError> {
        let state = self.inner.state.lock().unwrap();
        let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
        Ok(state
            .cancellations
            .cell(node.cancellation)
            .view(&self.inner))
    }

    /// Derivation preserves responsibility and the earliest parent deadline.
    /// It does not acquire another business or execution permit.
    pub fn child(&self, request: WorkRequest) -> Result<WorkOwner, WorkError> {
        self.inner.update(|state| {
            let root = {
                let parent = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
                parent.check(&state.cancellations)?;
                parent.root
            };
            if state.nodes.len() >= self.inner.config.scope_records_limit {
                return Err(WorkError::Capacity("scope records"));
            }
            // Mint the original identity before registering any cancellation
            // edge. State serializes this sequence with child publication.
            let id = WorkId(state.next_id()?);
            let parent_slot = state.nodes[&self.id].cancellation;
            let cancellation =
                state.take_child_cancellation(parent_slot, request.class, id, request.deadline)?;
            state.nodes.insert(
                id,
                Node::new(Some(self.id), root, request.class, cancellation),
            );
            state.nodes.get_mut(&self.id).unwrap().children += 1;
            Ok(WorkOwner {
                scope: Some(Self {
                    inner: Arc::clone(&self.inner),
                    id,
                }),
            })
        })
    }

    pub async fn wait_released(&self) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&self.id)
            {
                return;
            }
            changed.await;
        }
    }
}

/// Exactly one product owner, or an explicitly recorded orphan. Dropping an
/// owner requests termination; only `complete` supplies its completion fact.
pub struct WorkOwner {
    scope: Option<WorkScope>,
}

impl WorkOwner {
    pub fn scope(&self) -> WorkScope {
        self.scope.as_ref().unwrap().clone()
    }

    /// Derive a cloneable capability that may only request cancellation for
    /// this responsibility and its descendants. The requester does not retain
    /// ownership, keep the scope alive, or acquire completion/release rights.
    pub fn cancellation_requester(&self) -> WorkCancellationRequester {
        let scope = self.scope.as_ref().unwrap();
        WorkCancellationRequester {
            inner: Arc::clone(&scope.inner),
            id: scope.id,
        }
    }

    pub fn success_sealer(&self) -> WorkSuccessSealer {
        let scope = self.scope.as_ref().unwrap();
        WorkSuccessSealer {
            inner: Arc::clone(&scope.inner),
            id: scope.id,
        }
    }

    pub fn cancel(&self, reason: CancellationReason) {
        self.cancellation_requester()
            .request(reason)
            .expect("an active work owner must retain its responsibility");
    }

    /// Record a responsibility transfer before moving this owner to the
    /// receiving product/supervisor. No second work item is registered.
    pub fn handoff(self) -> Result<Self, WorkError> {
        let scope = self.scope.as_ref().unwrap();
        scope.inner.update(|state| {
            let node = state.nodes.get_mut(&scope.id).ok_or(WorkError::Released)?;
            node.handoffs = node
                .handoffs
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            Ok(())
        })?;
        Ok(self)
    }

    pub fn complete(self) {
        self.complete_with_terminal_cancel_settled(false);
    }

    /// Records responsibility completion after the matching cancellation
    /// delivery has reached its terminal boundary.
    ///
    /// This is narrower than [`Self::complete`]: ordinary completion retains
    /// a pending cancellation notification until its dispatcher settles it.
    /// A logical-execution owner may use this only after it has observed the
    /// real attempt/output/resource terminal condition that makes a pending
    /// Cancel notification obsolete.
    pub fn complete_after_terminal_cancel_settled(self) {
        self.complete_with_terminal_cancel_settled(true);
    }

    fn complete_with_terminal_cancel_settled(mut self, terminal_cancel_settled: bool) {
        let scope = self.scope.take().unwrap();
        scope.inner.update(|state| {
            let (is_root, class, remove_ready_control, remove_waiting_control) = {
                let node = state.nodes.get_mut(&scope.id).unwrap();
                node.completed = true;
                node.owner = OwnerState::Completed;
                if terminal_cancel_settled {
                    node.terminal_cancel_settled = true;
                    node.control_pending.remove(crate::ControlIntent::Cancel);
                }
                let remove_waiting_control =
                    terminal_cancel_settled && node.control_pending.is_empty();
                let remove_ready_control = remove_waiting_control && node.control_queued;
                if remove_ready_control {
                    node.control_queued = false;
                }
                (
                    node.parent.is_none(),
                    node.class,
                    remove_ready_control,
                    remove_waiting_control,
                )
            };
            if remove_waiting_control {
                state.control_waiting.remove(&scope.id);
            }
            if remove_ready_control {
                state.control_ready.retain(|id| *id != scope.id);
            }
            if is_root && state.closed {
                state
                    .root_lifecycle
                    .completed_after_admission_closed
                    .increment(class);
            }
            state.collect(scope.id);
        });
    }
}

/// Cloneable authority to request first-wins downward cancellation.
///
/// This capability cannot complete or hand off its owner responsibility and
/// cannot release business, stage, allocation, or obligation resources.
#[derive(Clone)]
pub struct WorkCancellationRequester {
    inner: Arc<Inner>,
    id: WorkId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkCancellationRequestOutcome {
    Requested,
    AlreadyRequested(CancellationReason),
    SuccessSealed,
}

#[derive(Clone)]
pub struct WorkSuccessSealer {
    inner: Arc<Inner>,
    id: WorkId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkSuccessSealOutcome {
    Sealed,
    AlreadySealed,
    Cancelled(CancellationReason),
}

impl WorkSuccessSealer {
    pub fn seal(&self) -> Result<WorkSuccessSealOutcome, WorkError> {
        let _reclaim = ReclaimWakeOnExit::new(&self.inner);
        let cancellation = {
            let state = self.inner.state.lock().unwrap();
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            if node.parent.is_some() {
                return Err(WorkError::Conflict);
            }
            Arc::clone(state.cancellations.cell(node.cancellation))
        };
        Ok(match cancellation.seal_success() {
            CancellationSuccessSealOutcome::Sealed => WorkSuccessSealOutcome::Sealed,
            CancellationSuccessSealOutcome::AlreadySealed => WorkSuccessSealOutcome::AlreadySealed,
            CancellationSuccessSealOutcome::Cancelled(reason) => {
                WorkSuccessSealOutcome::Cancelled(reason)
            }
        })
    }
}

impl WorkCancellationRequester {
    /// Request cancellation for this work and its descendants. Repeated or
    /// competing requests preserve the cancellation tree's first reason.
    /// Cancellation is intent only and never releases governed resources.
    pub fn request(&self, reason: CancellationReason) -> Result<(), WorkError> {
        self.request_with_outcome(reason).map(|_| ())
    }

    /// Request cancellation and report the exact first-wins decision made by
    /// this workload authority.
    pub fn request_with_outcome(
        &self,
        reason: CancellationReason,
    ) -> Result<WorkCancellationRequestOutcome, WorkError> {
        let _reclaim = ReclaimWakeOnExit::new(&self.inner);
        let cancellation = {
            let state = self.inner.state.lock().unwrap();
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            Arc::clone(state.cancellations.cell(node.cancellation))
        };
        let outcome = cancellation.request(reason);
        if outcome != CancellationRequestOutcome::SuccessSealed {
            self.inner.update(|state| {
                let node = state.nodes.get_mut(&self.id).ok_or(WorkError::Released)?;
                node.cancellation_signalled = true;
                crate::observation::queue_control(state, self.id, crate::ControlIntent::Cancel)?;
                Ok(())
            })?;
        }
        Ok(match outcome {
            CancellationRequestOutcome::Requested => WorkCancellationRequestOutcome::Requested,
            CancellationRequestOutcome::AlreadyRequested(reason) => {
                WorkCancellationRequestOutcome::AlreadyRequested(reason)
            }
            CancellationRequestOutcome::SuccessSealed => {
                WorkCancellationRequestOutcome::SuccessSealed
            }
        })
    }
}

impl Drop for WorkOwner {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            let _reclaim = ReclaimWakeOnExit::new(&scope.inner);
            let cancellation = {
                let state = scope.inner.state.lock().unwrap();
                Arc::clone(
                    state
                        .cancellations
                        .cell(state.nodes[&scope.id].cancellation),
                )
            };
            let cancellation_outcome = cancellation.request(CancellationReason::OwnerDropped);
            scope.inner.update(|state| {
                if let Some(node) = state.nodes.get_mut(&scope.id) {
                    node.owner = OwnerState::Orphaned;
                    if cancellation_outcome != CancellationRequestOutcome::SuccessSealed {
                        node.cancellation_signalled = true;
                        crate::observation::queue_control(
                            state,
                            scope.id,
                            crate::ControlIntent::Cancel,
                        )
                        .unwrap();
                    }
                }
            });
        }
    }
}

/// The product-defined business lifecycle is distinct from the responsibility
/// tree. Release this at that boundary, including during a longer cleanup tail.
pub struct BusinessPermit {
    scope: Option<WorkScope>,
}

/// One non-copyable warehouse query-concurrency grant. It is intentionally
/// independent of thread execution and remote cleanup; its owner releases it
/// when the logical query reaches a Frontend terminal outcome.
pub struct QueryConcurrencyPermit {
    pub(crate) scope: Option<WorkScope>,
}

impl QueryConcurrencyPermit {
    /// Verify both the host and exact admitted root before a runtime handoff.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.scope
            .as_ref()
            .is_some_and(|held| held.id == scope.id && Arc::ptr_eq(&held.inner, &scope.inner))
    }

    pub fn release(self) {
        drop(self);
    }
}

impl Drop for QueryConcurrencyPermit {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.inner.update(|state| {
                state.nodes.get_mut(&scope.id).unwrap().query_admitted = false;
                state.admitted_queries -= 1;
                state.collect(scope.id);
            });
        }
    }
}

impl BusinessPermit {
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for BusinessPermit {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.inner.update(|state| {
                state.nodes.get_mut(&scope.id).unwrap().business = false;
                state.businesses -= 1;
                state.collect(scope.id);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Wake, Waker},
    };

    struct InspectWake(Arc<Inner>);

    impl Wake for InspectWake {
        fn wake(self: Arc<Self>) {
            assert!(
                self.0.state.try_lock().is_ok(),
                "Wake callback ran under the work owner lock"
            );
        }
    }

    fn controller() -> WorkloadControl {
        let control = WorkloadControl::try_new_split(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 128,
                control_bytes: 16,
                per_scope_bytes: 112,
            },
        )
        .unwrap()
        .owner;
        control.mark_ready().unwrap();
        control
    }

    #[tokio::test]
    async fn cancellation_wake_may_reenter_the_work_owner() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cancellation = work.owner.scope().cancellation().unwrap();
        let mut cancelled = Box::pin(cancellation.cancelled());
        let waker = Waker::from(Arc::new(InspectWake(Arc::clone(&control.inner))));
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        work.owner.cancel(CancellationReason::Requested);
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[tokio::test]
    async fn observation_waits_for_the_last_root_responsibility() {
        let control = controller();
        let observation = control.observation();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit root work");
        let mut waiter = Box::pin(tokio::spawn(async move {
            observation.wait_until_no_root_responsibilities().await;
        }));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiter)
                .await
                .is_err(),
            "observation must not report drained while a root remains"
        );

        drop(work.business);
        work.owner.complete();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("observation wakes when the last root is released")
            .expect("wait task joins");
    }

    #[tokio::test]
    async fn completed_work_is_not_reenqueued_when_its_deadline_elapses() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(tokio::time::Instant::now() + Duration::from_millis(10)),
            })
            .expect("admit deadline-bound root");

        work.owner.complete();
        tokio::time::sleep(Duration::from_millis(20)).await;
        control.expire_deadlines();

        assert!(control.next_deadline().is_none());
        assert!(
            control.next_control().is_none(),
            "completed work has no owner left to deliver a later deadline control"
        );
        work.business.release();
    }

    #[test]
    fn late_request_cannot_requeue_terminally_settled_cancel() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit root work");
        let requester = work.owner.cancellation_requester();
        let scope = work.owner.scope();
        requester
            .request(CancellationReason::DeadlineExceeded)
            .expect("request cancellation");
        work.owner.complete_after_terminal_cancel_settled();
        assert_eq!(control.snapshot().root_responsibilities, 1);
        assert!(control.next_control().is_none());

        requester
            .request(CancellationReason::Requested)
            .expect("late request retains the first cancellation reason");
        scope
            .request_control(crate::ControlIntent::Cancel)
            .expect("late direct control request is harmless");
        assert!(control.next_control().is_none());
        work.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn dropped_inflight_cancel_cannot_requeue_after_terminal_settlement() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit root work");
        work.owner.cancel(CancellationReason::DeadlineExceeded);
        let permit = control.next_control().expect("dispatch cancel control");
        assert!(permit.intents().contains(crate::ControlIntent::Cancel));

        work.owner.complete_after_terminal_cancel_settled();
        drop(permit);
        assert!(control.next_control().is_none());
        work.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn process_owner_cancels_each_active_root_once() {
        let control = controller();
        let first = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit first root");
        let second = control
            .try_begin_root(WorkRequest::new(WorkClass::TableMaintenance))
            .expect("admit second root");
        let first_cancellation = first
            .owner
            .scope()
            .cancellation()
            .expect("first cancellation");
        let second_cancellation = second
            .owner
            .scope()
            .cancellation()
            .expect("second cancellation");

        assert_eq!(
            control.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded),
            2
        );
        assert_eq!(
            first_cancellation.reason(),
            Some(CancellationReason::FrontendDrainDeadlineExceeded)
        );
        assert_eq!(
            second_cancellation.reason(),
            Some(CancellationReason::FrontendDrainDeadlineExceeded)
        );
        assert_eq!(
            control.cancel_active_roots(CancellationReason::ServerShutdown),
            0,
            "a drain deadline must preserve the first cancellation reason"
        );

        drop(first.business);
        first.owner.complete();
        drop(second.business);
        second.owner.complete();
    }

    #[test]
    fn root_lifecycle_observation_records_rejection_drain_completion_and_deadline_cancel() {
        let control = controller();
        control.close_admission();
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Management)),
            Err(WorkError::Closed)
        ));
        assert_eq!(
            control
                .snapshot()
                .root_lifecycle
                .rejected_admissions
                .management,
            1
        );

        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit query root");
        control.close_admission();
        assert_eq!(
            control.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded),
            1
        );
        drop(work.business);
        work.owner.complete();

        let snapshot = control.snapshot().root_lifecycle;
        assert_eq!(snapshot.frontend_drain_deadline_cancelled.query, 1);
        assert_eq!(snapshot.completed_after_admission_closed.query, 1);
    }

    #[test]
    fn identity_exhaustion_is_rejected_without_reusing_an_id_or_charging_work() {
        let control = controller();
        control.inner.state.lock().unwrap().next_id = u64::MAX;
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::ArithmeticOverflow)
        ));
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(control.snapshot().businesses, 0);
    }

    #[test]
    fn child_identity_exhaustion_registers_no_cancellation_edge() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let cancellation = {
            let mut state = control.inner.state.lock().unwrap();
            state.next_id = u64::MAX;
            Arc::clone(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        assert!(matches!(
            scope.child(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::ArithmeticOverflow)
        ));
        assert_eq!(cancellation.linked_children_for_test(), 0);
        assert_eq!(
            control.inner.state.lock().unwrap().nodes[&scope.id].children,
            0
        );
        work.business.release();
        work.owner.complete();
    }

    #[test]
    fn unpublished_cancelled_child_detaches_its_registered_edge() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let cancellation = {
            let state = control.inner.state.lock().unwrap();
            Arc::clone(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        assert!(matches!(
            scope.child(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(Instant::now()),
            }),
            Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
        ));
        assert_eq!(cancellation.linked_children_for_test(), 0);
        assert_eq!(
            control.inner.state.lock().unwrap().nodes[&scope.id].children,
            0
        );
        assert_eq!(control.snapshot().root_responsibilities, 1);
        work.business.release();
        work.owner.complete();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    struct InspectReclaimWake {
        authority: Weak<Inner>,
        cancellation: Weak<Cancellation>,
        wakes: std::sync::atomic::AtomicUsize,
    }

    impl Wake for InspectReclaimWake {
        fn wake(self: Arc<Self>) {
            let inner = self.authority.upgrade().unwrap();
            assert!(inner.state.try_lock().is_ok(), "reclaim wake held State");
            assert_eq!(
                self.cancellation.strong_count(),
                0,
                "the final view must drop its actual Arc before notifying"
            );
            self.wakes.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn retained_view_exit_wakes_both_waiters_after_actual_arc_exit() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let first = scope.cancellation().unwrap();
        let before_clone_drop = control.progress_revision();
        let view = first.clone();
        let cell = {
            let state = control.inner.state.lock().unwrap();
            Arc::downgrade(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        assert_eq!(
            control.progress_revision(),
            before_clone_drop,
            "clones allocate no event"
        );
        drop(first);
        work.business.release();
        work.owner.complete();
        assert!(
            !control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id)
        );
        assert!(
            cell.upgrade().is_some(),
            "only the retained view keeps cancellation facts"
        );

        let after = control.progress_revision();
        let deadline_owner = control.deadline_expiry_handle();
        let wakes = Arc::new(InspectReclaimWake {
            authority: Arc::downgrade(&control.inner),
            cancellation: cell.clone(),
            wakes: std::sync::atomic::AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&wakes));
        let mut progress = Box::pin(control.wait_progress(after));
        let mut deadlines = Box::pin(deadline_owner.wait_state_change(after));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            deadlines
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(view);
        assert!(cell.upgrade().is_none());
        let now = control.progress_revision();
        assert_eq!(now.state_revision, after.state_revision);
        assert_ne!(now.reclaim_generation, after.reclaim_generation);
        assert!(wakes.wakes.load(Ordering::Relaxed) > 0);
        assert!(
            matches!(progress.as_mut().poll(&mut Context::from_waker(&waker)),
            std::task::Poll::Ready(WorkloadProgress::StateChanged(revision)) if revision == now)
        );
        assert!(
            deadlines
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[tokio::test]
    async fn view_exit_before_wait_registration_is_not_lost_and_does_not_pin_authority() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let view = work.owner.scope().cancellation().unwrap();
        let survivor = view.clone();
        work.business.release();
        work.owner.complete();
        let after = control.progress_revision();
        drop(view);
        let deadlines = control.deadline_expiry_handle();
        let mut changed = Box::pin(deadlines.wait_state_change(after));
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        let mut progress = Box::pin(control.wait_progress(after));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop(progress);
        drop(changed);
        drop(deadlines);
        let original_authority = Arc::downgrade(&control.inner);
        drop(control);
        assert!(
            original_authority.upgrade().is_none(),
            "a retained view is non-owning"
        );
        drop(survivor);
    }

    #[tokio::test(start_paused = true)]
    async fn each_original_raw_author_reports_exit_and_repeated_deadline_reads_are_silent() {
        for action in 0..5 {
            let control = controller();
            let deadline = Instant::now() + Duration::from_secs(1);
            let work = control
                .try_begin_root(WorkRequest {
                    class: WorkClass::Query,
                    deadline: Some(deadline),
                })
                .unwrap();
            let scope = work.owner.scope();
            let view = scope.cancellation().unwrap();
            let cell = {
                let state = control.inner.state.lock().unwrap();
                Arc::downgrade(
                    state
                        .cancellations
                        .cell(state.nodes[&scope.id].cancellation),
                )
            };
            let mut owner = Some(work.owner);
            let before = control.progress_revision();
            assert_eq!(control.next_deadline(), Some(deadline));
            assert_eq!(control.next_deadline(), Some(deadline));
            assert_eq!(
                control.progress_revision(),
                before,
                "deadline getters cannot create dropping views"
            );
            if action == 1 {
                tokio::time::advance(Duration::from_secs(2)).await;
            }
            match action {
                0 => {
                    control.cancel_active_roots(CancellationReason::Requested);
                }
                1 => control.expire_deadlines(),
                2 => {
                    assert_eq!(
                        owner.as_ref().unwrap().success_sealer().seal().unwrap(),
                        WorkSuccessSealOutcome::Sealed
                    );
                }
                3 => {
                    owner
                        .as_ref()
                        .unwrap()
                        .cancellation_requester()
                        .request(CancellationReason::Requested)
                        .unwrap();
                }
                4 => drop(owner.take()),
                _ => unreachable!(),
            }
            let after = control.progress_revision();
            assert_eq!(
                after.reclaim_generation,
                before.reclaim_generation.wrapping_add(1),
                "raw author {action}"
            );
            assert_eq!(
                cell.strong_count(),
                2,
                "only original node and view survive raw author {action}"
            );
            if action == 2 {
                assert_eq!(
                    after.state_revision, before.state_revision,
                    "seal's raw exit must be observable without a State mutation"
                );
            }
            let owner = owner.unwrap_or_else(|| control.adopt(scope.id).unwrap());
            work.business.release();
            owner.complete_after_terminal_cancel_settled();
            drop(view);
        }
    }

    struct PanicOnceCancellationWake(std::sync::atomic::AtomicBool);
    impl Wake for PanicOnceCancellationWake {
        fn wake(self: Arc<Self>) {
            if !self.0.swap(true, Ordering::Relaxed) {
                panic!("injected cancellation wake unwind");
            }
        }
    }

    #[tokio::test]
    async fn original_request_unwind_drops_raw_arc_before_publishing_reclaim_revision() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let view = scope.cancellation().unwrap();
        let cell = {
            let state = control.inner.state.lock().unwrap();
            Arc::downgrade(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        let requester = work.owner.cancellation_requester();
        let mut observer = Box::pin(view.cancelled());
        let waker = Waker::from(Arc::new(PanicOnceCancellationWake(
            std::sync::atomic::AtomicBool::new(false),
        )));
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let after = control.progress_revision();
        let mut progress = Box::pin(control.wait_progress(after));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            requester.request(CancellationReason::Requested)
        }));
        assert!(
            result.is_err(),
            "actual cancellation notification unwinds the original request"
        );
        assert_eq!(
            cell.strong_count(),
            2,
            "the escaping raw request Arc was actually dropped"
        );
        let now = control.progress_revision();
        assert_eq!(
            now.state_revision, after.state_revision,
            "unwind preceded the request's State update"
        );
        assert_ne!(now.reclaim_generation, after.reclaim_generation);
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop(progress);
        drop(observer);
        work.business.release();
        work.owner.complete_after_terminal_cancel_settled();
    }

    #[test]
    fn raw_author_error_return_also_advances_the_observable_pair() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let child = work
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let after = control.progress_revision();
        assert_eq!(child.success_sealer().seal(), Err(WorkError::Conflict));
        let now = control.progress_revision();
        assert_eq!(now.state_revision, after.state_revision);
        assert_ne!(now.reclaim_generation, after.reclaim_generation);
        child.complete();
        work.business.release();
        work.owner.complete();
    }
}

#[cfg(test)]
mod fixed_control_storage_tests {
    use super::*;
    use crate::control_storage::{ControlStorageConfig, SlotPhase};
    use crate::joint_admission::{
        JointCapacityConfig, JointProgress, JointProgressDriver, JointResource,
        JointResourceLimits, JointResources, RequiredJointProgress,
    };
    use std::{
        future::Future,
        num::NonZeroUsize,
        task::{Context, Poll, Waker},
    };

    fn prototype(count: usize) -> (WorkloadControl, JointProgressDriver) {
        prototype_with_fanout(count, 1)
    }

    fn prototype_with_fanout(
        count: usize,
        children_per_slot: usize,
    ) -> (WorkloadControl, JointProgressDriver) {
        let (control, _, driver) = prototype_full(count, children_per_slot);
        (control, driver)
    }

    fn prototype_full(
        count: usize,
        children_per_slot: usize,
    ) -> (
        WorkloadControl,
        crate::joint_admission::JointAdmissionHandle,
        JointProgressDriver,
    ) {
        prototype_profile(count, children_per_slot, 1, 1)
    }

    fn prototype_profile(
        count: usize,
        children_per_slot: usize,
        max_dependencies: usize,
        max_changes: usize,
    ) -> (
        WorkloadControl,
        crate::joint_admission::JointAdmissionHandle,
        JointProgressDriver,
    ) {
        let storage = ControlStorageConfig {
            slots_per_class: [count, 0, 0, 0, 0],
            children_per_slot,
        };
        let base = crate::joint_admission::ledger::control_storage_base(storage).unwrap();
        let metadata = base.get(JointResource::MetadataBytes);
        let global = JointResources::ZERO
            .with(JointResource::ScopeRecords, count as u64)
            .with(JointResource::ClaimRecords, (count * 2) as u64)
            .with(JointResource::MetadataBytes, metadata * count as u64 * 4);
        let domain = JointResources::ZERO
            .with(JointResource::ScopeRecords, (count - 1) as u64)
            .with(JointResource::ClaimRecords, count as u64)
            .with(JointResource::MetadataBytes, metadata * count as u64 * 2);
        let limit = JointResourceLimits {
            global,
            per_dependency: domain,
            protected_progress: base,
        };
        let mut classes = [JointResourceLimits::default(); 5];
        classes[0] = limit;
        let mut lanes = [JointResourceLimits::default(); 3];
        lanes[0] = limit;
        let mut class_lanes = [[JointResourceLimits::default(); 3]; 5];
        class_lanes[0][0] = limit;
        let config = JointCapacityConfig {
            global: limit,
            classes,
            lanes,
            class_lanes,
            max_dependencies_per_claim: max_dependencies,
            max_registered_dependencies: count,
            max_claims: count * 2,
            max_requests: count,
            max_changes_per_request: max_changes,
            max_snapshot_bytes: 16 * 1024 * 1024,
            max_scratch_bytes: 16 * 1024 * 1024,
            max_residual_arcs: 10000,
            max_total_proof_work: 10000000,
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        };
        // This minimum is only the storage substrate, never a complete role.
        let mut required = RequiredJointProgress {
            global: base,
            classes: [JointResources::ZERO; 5],
            lanes: [JointResources::ZERO; 3],
            class_lanes: [[JointResources::ZERO; 3]; 5],
        };
        required.classes[0] = base;
        required.lanes[0] = base;
        required.class_lanes[0][0] = base;
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (handle, driver) = control
            .configure_joint_capacity(config.validate(&required).unwrap())
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        control.mark_ready().unwrap();
        (control, handle, driver)
    }

    fn root(control: &WorkloadControl) -> RootWork {
        control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap()
    }
    fn drain(driver: &mut JointProgressDriver) -> WorkloadProgressRevision {
        for _ in 0..10000 {
            if let JointProgress::Idle(marker) = driver.step() {
                return marker;
            }
        }
        panic!("storage driver did not reach a finite idle pass");
    }
    fn slot(control: &WorkloadControl, id: WorkId) -> SlotId {
        control.inner.state.lock().unwrap().nodes[&id].cancellation
    }
    fn snapshot(control: &WorkloadControl) -> (usize, usize, JointResources, usize) {
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .control_snapshot_for_test()
    }

    fn selected_dependency(
        handle: &crate::joint_admission::JointAdmissionHandle,
        name: &str,
    ) -> crate::joint_admission::JointDependency {
        use novarocks_resource_contract::{NetworkServiceOrigin, SharedDependency};
        handle
            .register_dependency(SharedDependency::Network(
                NetworkServiceOrigin::from_selected_url(
                    &url::Url::parse(&format!("https://{name}.example")).unwrap(),
                )
                .unwrap(),
            ))
            .unwrap()
    }

    fn first_bind(
        handle: &crate::joint_admission::JointAdmissionHandle,
        owner: &WorkOwner,
        dependency: &crate::joint_admission::JointDependency,
    ) -> crate::joint_admission::JointAdmission {
        handle
            .request(
                &owner.scope(),
                vec![crate::joint_admission::JointClaimChange::FirstBind {
                    source: owner.control_claim_view().unwrap(),
                    dependencies: vec![dependency.clone()],
                }],
            )
            .unwrap()
    }

    fn binding_receipt(
        request: &mut crate::joint_admission::JointAdmission,
    ) -> crate::joint_admission::JointAdjustmentReceipt {
        let Poll::Ready(Ok(batch)) =
            std::pin::Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("the original proof must have granted the binding");
        };
        assert!(
            batch.new_claims.is_empty(),
            "binding cannot mint a physical guard"
        );
        batch.adjustment.unwrap()
    }

    #[test]
    fn first_bind_preserves_base_and_requires_the_exact_original_owner_at_install() {
        let (control, handle, mut driver) = prototype_full(4, 1);
        let dependency = selected_dependency(&handle, "a");
        let work = root(&control);
        let other = root(&control);
        let original_slot = slot(&control, work.owner.scope().id);
        let initial = snapshot(&control);
        let original_claim =
            control.inner.state.lock().unwrap().cancellations.slots[original_slot.0].claim;
        let mut request = first_bind(&handle, &work.owner, &dependency);
        drain(&mut driver);
        let charged = snapshot(&control);
        assert_eq!(
            (charged.0, charged.1, charged.2),
            (initial.0, initial.1, initial.2)
        );
        assert_eq!(charged.3, 1);
        let receipt = binding_receipt(&mut request);
        assert!(matches!(
            receipt.install_control(&other.owner, &[]),
            Err(WorkError::Conflict)
        ));
        assert_eq!(
            snapshot(&control),
            initial,
            "a refused install withdraws only this binding"
        );

        let mut request = first_bind(&handle, &work.owner, &dependency);
        drain(&mut driver);
        assert!(matches!(
            binding_receipt(&mut request).install(&[]),
            Err(WorkError::Conflict)
        ));
        assert_eq!(
            snapshot(&control),
            initial,
            "ordinary physical install cannot forge control authority"
        );

        let mut request = first_bind(&handle, &work.owner, &dependency);
        drain(&mut driver);
        binding_receipt(&mut request)
            .install_control(&work.owner, &[])
            .unwrap();
        let bound = snapshot(&control);
        assert_eq!(bound, charged);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.slots[original_slot.0].claim,
            original_claim
        );
        let view = work.owner.scope().cancellation().unwrap();
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
        assert_eq!(
            snapshot(&control),
            bound,
            "retained backing keeps the installed domain fee"
        );
        drop(view);
        other.business.release();
        other.owner.complete();
        drain(&mut driver);
        assert_eq!(snapshot(&control), initial);
    }

    #[test]
    fn first_bind_is_finite_under_one_bad_domain_and_preserves_healthy_progress() {
        let (control, handle, mut driver) = prototype_full(3, 1);
        let bad = selected_dependency(&handle, "bad");
        let healthy = selected_dependency(&handle, "healthy");
        let a = root(&control);
        let b = root(&control);
        let c = root(&control);
        for owner in [&a.owner, &b.owner] {
            let mut request = first_bind(&handle, owner, &bad);
            drain(&mut driver);
            binding_receipt(&mut request)
                .install_control(owner, &[])
                .unwrap();
        }
        let before = snapshot(&control);
        let mut rejected = first_bind(&handle, &c.owner, &bad);
        drain(&mut driver);
        let Poll::Ready(Err(failure)) =
            std::pin::Pin::new(&mut rejected).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("a self-held first binding must fail rather than park");
        };
        assert!(matches!(failure.error, WorkError::Capacity(_)));
        assert_eq!(snapshot(&control), before);
        let mut request = first_bind(&handle, &c.owner, &healthy);
        drain(&mut driver);
        binding_receipt(&mut request)
            .install_control(&c.owner, &[])
            .unwrap();
        for work in [a, b, c] {
            work.business.release();
            work.owner.complete();
        }
        drain(&mut driver);
        assert_eq!(snapshot(&control).3, 0);
    }

    #[test]
    fn first_bind_and_physical_growth_install_atomically_with_both_original_owners() {
        use crate::joint_admission::{JointClaimChange, JointLane};
        let (control, handle, mut driver) = prototype_profile(4, 1, 2, 2);
        let a = selected_dependency(&handle, "a");
        let b = selected_dependency(&handle, "b");
        let work = root(&control);
        let scope = work.owner.scope();
        let mut request = handle
            .request(
                &scope,
                vec![JointClaimChange::New {
                    dependencies: vec![a.clone()],
                    lane: JointLane::Custody,
                    amounts: JointResources::ZERO
                        .with(JointResource::ClaimRecords, 1)
                        .with(JointResource::MetadataBytes, 1),
                }],
            )
            .unwrap();
        drain(&mut driver);
        let Poll::Ready(Ok(mut batch)) =
            std::pin::Pin::new(&mut request).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("physical base grant");
        };
        let physical = batch.new_claims.pop().unwrap();
        let before = snapshot(&control);
        // Both source kinds share exact original claim deduplication.
        let view = work.owner.control_claim_view().unwrap();
        assert!(
            handle
                .request(
                    &scope,
                    vec![
                        JointClaimChange::FirstBind {
                            source: view.clone(),
                            dependencies: vec![a.clone()]
                        },
                        JointClaimChange::FirstBind {
                            source: view,
                            dependencies: vec![b.clone()]
                        },
                    ]
                )
                .is_err()
        );
        for omit_physical in [true, false] {
            let mut request = handle
                .request(
                    &scope,
                    vec![
                        JointClaimChange::FirstBind {
                            source: work.owner.control_claim_view().unwrap(),
                            dependencies: vec![a.clone(), b.clone()],
                        },
                        JointClaimChange::grow_by(
                            physical.view().unwrap(),
                            JointResources::ZERO.with(JointResource::MetadataBytes, 1),
                            vec![b.clone()],
                        ),
                    ],
                )
                .unwrap();
            drain(&mut driver);
            let receipt = binding_receipt(&mut request);
            assert_eq!(
                physical
                    .amounts()
                    .unwrap()
                    .get(JointResource::MetadataBytes),
                1,
                "charged delta is not installed permission"
            );
            if omit_physical {
                assert!(matches!(
                    receipt.install_control(&work.owner, &[]),
                    Err(WorkError::Conflict)
                ));
                assert_eq!(
                    snapshot(&control),
                    before,
                    "mixed withdrawal keeps the physical original"
                );
            } else {
                receipt.install_control(&work.owner, &[&physical]).unwrap();
                assert_eq!(
                    physical
                        .amounts()
                        .unwrap()
                        .get(JointResource::MetadataBytes),
                    2
                );
                assert_eq!(snapshot(&control).3, 1);
            }
        }
        assert!(matches!(
            handle.request(
                &scope,
                vec![JointClaimChange::FirstBind {
                    source: physical.view().unwrap(),
                    dependencies: vec![a],
                }]
            ),
            Err(crate::joint_admission::JointAdmissionFailure {
                error: WorkError::Conflict
            })
        ));
        drop(physical);
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
        assert_eq!(snapshot(&control).3, 0);
    }

    #[test]
    fn control_source_cannot_escape_through_grow_empty_binding_or_reused_slot() {
        use crate::joint_admission::JointClaimChange;
        let (control, handle, mut driver) = prototype_full(3, 1);
        let dependency = selected_dependency(&handle, "a");
        let work = root(&control);
        let old_scope = work.owner.scope();
        let old_slot = slot(&control, old_scope.id);
        let old = work.owner.control_claim_view().unwrap();
        for change in [
            JointClaimChange::FirstBind {
                source: old.clone(),
                dependencies: vec![],
            },
            JointClaimChange::grow_by(old.clone(), JointResources::ZERO, vec![dependency.clone()]),
            JointClaimChange::grow_by(
                old.clone(),
                JointResources::ZERO.with(JointResource::MetadataBytes, 1),
                vec![dependency.clone()],
            ),
        ] {
            assert!(matches!(
                handle.request(&old_scope, vec![change]),
                Err(crate::joint_admission::JointAdmissionFailure {
                    error: WorkError::Conflict
                })
            ));
        }
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
        let replacement = root(&control);
        let replacement_scope = replacement.owner.scope();
        assert_eq!(slot(&control, replacement_scope.id), old_slot);
        assert!(matches!(
            handle.request(
                &replacement_scope,
                vec![JointClaimChange::FirstBind {
                    source: old,
                    dependencies: vec![dependency]
                }]
            ),
            Err(crate::joint_admission::JointAdmissionFailure {
                error: WorkError::Conflict
            })
        ));
        replacement.business.release();
        replacement.owner.complete();
        drain(&mut driver);
    }

    #[test]
    fn waiting_pool_proposals_do_not_reserve_a_slot_or_block_a_new_root_binding() {
        let (control, handle, mut driver) = prototype_full(4, 1);
        let a = selected_dependency(&handle, "a");
        let b = selected_dependency(&handle, "b");
        let parent = root(&control);
        let mut binding = first_bind(&handle, &parent.owner, &a);
        drain(&mut driver);
        binding_receipt(&mut binding)
            .install_control(&parent.owner, &[])
            .unwrap();
        let mut first = handle
            .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
            .unwrap();
        let mut second = handle
            .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
            .unwrap();
        let newcomer = root(&control);
        let mut binding = first_bind(&handle, &newcomer.owner, &b);
        drain(&mut driver);
        for request in [&mut first, &mut second] {
            assert!(matches!(
                std::pin::Pin::new(request).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(crate::joint_admission::JointAdmissionFailure {
                    error: WorkError::Conflict
                }))
            ));
        }
        binding_receipt(&mut binding)
            .install_control(&newcomer.owner, &[])
            .unwrap();
        assert_eq!(snapshot(&control).3, 2);
        for work in [parent, newcomer] {
            work.business.release();
            work.owner.complete();
        }
        drain(&mut driver);
        assert_eq!(snapshot(&control).3, 0);
    }

    #[test]
    fn pool_grant_publishes_one_child_with_the_full_parent_domain_and_same_claim() {
        use crate::joint_admission::JointClaimChange;
        let (control, handle, mut driver) = prototype_profile(4, 1, 2, 2);
        let a = selected_dependency(&handle, "a");
        let b = selected_dependency(&handle, "b");
        let expected = [a.fact().clone(), b.fact().clone()];
        let parent = root(&control);
        let parent_scope = parent.owner.scope();
        let mut binding = handle
            .request(
                &parent_scope,
                vec![JointClaimChange::FirstBind {
                    source: parent.owner.control_claim_view().unwrap(),
                    dependencies: vec![a, b],
                }],
            )
            .unwrap();
        drain(&mut driver);
        binding_receipt(&mut binding)
            .install_control(&parent.owner, &[])
            .unwrap();
        let before = snapshot(&control);
        let candidate = control.inner.state.lock().unwrap().cancellations.free_heads[0].unwrap();
        let claim = control.inner.state.lock().unwrap().cancellations.slots[candidate.0].claim;
        let mut first = handle
            .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
            .unwrap();
        let mut second = handle
            .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
            .unwrap();
        drain(&mut driver);
        let receipt = binding_receipt(&mut first);
        assert!(matches!(
            std::pin::Pin::new(&mut second).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(crate::joint_admission::JointAdmissionFailure {
                error: WorkError::Conflict
            }))
        ));
        let granted = snapshot(&control);
        assert_eq!(
            (granted.0, granted.1, granted.2),
            (before.0, before.1, before.2)
        );
        assert_eq!(granted.3, before.3 + 1);
        {
            let state = control.inner.state.lock().unwrap();
            assert!(matches!(
                state.cancellations.slots[candidate.0].phase,
                SlotPhase::ChildGranted { .. }
            ));
            assert_eq!(
                state.nodes[&parent_scope.id].children, 0,
                "grant is not a fake Work"
            );
            assert_eq!(state.nodes[&parent_scope.id].resource_waiters, 1);
            assert!(
                state
                    .joint
                    .as_ref()
                    .unwrap()
                    .control_has_domains_for_test(claim.unwrap(), &expected)
            );
        }
        let child = receipt.install_child(&parent.owner).unwrap();
        let child_scope = child.scope();
        assert_eq!(slot(&control, child_scope.id), candidate);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.slots[candidate.0].claim,
            claim
        );
        assert_eq!(snapshot(&control), granted);
        let retained = child_scope.cancellation().unwrap();
        parent.business.release();
        parent.owner.complete();
        assert!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&parent_scope.id)
        );
        child.complete();
        assert!(
            !control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&parent_scope.id)
        );
        drain(&mut driver);
        assert_eq!(
            snapshot(&control),
            granted,
            "real child backing retains both domain liabilities"
        );
        drop(retained);
        drain(&mut driver);
        assert_eq!(snapshot(&control).3, 0);
    }

    #[test]
    fn granted_and_delivered_child_replies_keep_domain_fees_until_their_actual_exit() {
        for delivered in [false, true] {
            let (control, handle, mut driver) = prototype_full(3, 1);
            let a = selected_dependency(&handle, "a");
            let parent = root(&control);
            let scope = parent.owner.scope();
            let mut binding = first_bind(&handle, &parent.owner, &a);
            drain(&mut driver);
            binding_receipt(&mut binding)
                .install_control(&parent.owner, &[])
                .unwrap();
            let mut request = handle
                .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
                .unwrap();
            drain(&mut driver);
            let receipt = delivered.then(|| binding_receipt(&mut request));
            let charged = snapshot(&control);
            assert_eq!(charged.3, 2);
            parent.owner.cancel(CancellationReason::Requested);
            parent.business.release();
            parent.owner.complete_after_terminal_cancel_settled();
            drain(&mut driver);
            assert!(
                control
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .nodes
                    .contains_key(&scope.id)
            );
            assert_eq!(
                snapshot(&control),
                charged,
                "cancellation is not reply exit"
            );
            drop(receipt);
            drop(request);
            assert!(
                !control
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .nodes
                    .contains_key(&scope.id)
            );
            assert_eq!(
                snapshot(&control),
                charged,
                "reply exit retires without washing the domain fee"
            );
            drain(&mut driver);
            let reset = snapshot(&control);
            assert_eq!(
                (reset.0, reset.1, reset.2),
                (charged.0, charged.1, charged.2)
            );
            assert_eq!(reset.3, 0);
        }
    }

    #[test]
    fn refused_pool_proof_preserves_the_real_free_head_for_a_healthy_root() {
        let (control, handle, mut driver) = prototype_full(4, 2);
        let a = selected_dependency(&handle, "bad");
        let b = selected_dependency(&handle, "healthy");
        let parent = root(&control);
        let mut binding = first_bind(&handle, &parent.owner, &a);
        drain(&mut driver);
        binding_receipt(&mut binding)
            .install_control(&parent.owner, &[])
            .unwrap();
        let mut children = Vec::new();
        for _ in 0..2 {
            let mut request = handle
                .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
                .unwrap();
            drain(&mut driver);
            children.push(
                binding_receipt(&mut request)
                    .install_child(&parent.owner)
                    .unwrap(),
            );
        }
        let free = control.inner.state.lock().unwrap().cancellations.free_heads[0];
        let mut rejected = handle
            .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
            .unwrap();
        drain(&mut driver);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.free_heads[0],
            free
        );
        let healthy = root(&control);
        let mut binding = first_bind(&handle, &healthy.owner, &b);
        drain(&mut driver);
        binding_receipt(&mut binding)
            .install_control(&healthy.owner, &[])
            .unwrap();
        assert!(matches!(
            std::pin::Pin::new(&mut rejected).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(crate::joint_admission::JointAdmissionFailure {
                error: WorkError::Capacity(_)
            }))
        ));
        for child in children {
            child.complete();
        }
        for work in [parent, healthy] {
            work.business.release();
            work.owner.complete();
        }
        drain(&mut driver);
        assert_eq!(snapshot(&control).3, 0);
    }

    #[test]
    fn child_registration_failure_detaches_and_retires_the_exact_committed_reply() {
        let (control, handle, mut driver) = prototype_full(3, 1);
        let a = selected_dependency(&handle, "a");
        let parent = root(&control);
        let scope = parent.owner.scope();
        let mut binding = first_bind(&handle, &parent.owner, &a);
        drain(&mut driver);
        binding_receipt(&mut binding)
            .install_control(&parent.owner, &[])
            .unwrap();
        let mut request = handle
            .request_child(
                &parent.owner,
                WorkRequest {
                    class: WorkClass::Query,
                    deadline: Some(Instant::now()),
                },
            )
            .unwrap();
        drain(&mut driver);
        let charged = snapshot(&control);
        assert!(matches!(
            binding_receipt(&mut request).install_child(&parent.owner),
            Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
        ));
        {
            let state = control.inner.state.lock().unwrap();
            let node = &state.nodes[&scope.id];
            assert_eq!(node.children, 0);
            assert_eq!(node.resource_waiters, 0);
            assert_eq!(state.nodes.len(), 1);
            assert_eq!(
                state
                    .cancellations
                    .cell(node.cancellation)
                    .linked_children_for_test(),
                0
            );
        }
        assert_eq!(snapshot(&control), charged);
        drain(&mut driver);
        assert_eq!(snapshot(&control).3, 1);
        parent.business.release();
        parent.owner.complete();
        drain(&mut driver);
        assert_eq!(snapshot(&control).3, 0);
    }

    #[test]
    fn retained_view_collects_work_but_preserves_original_bound_claim_until_reset() {
        let (control, mut driver) = prototype(4);
        let prepaid = snapshot(&control);
        let work = root(&control);
        let scope = work.owner.scope();
        let original = slot(&control, scope.id);
        assert_eq!(
            snapshot(&control),
            prepaid,
            "same-key take preserves members and all coordinates"
        );
        let view = scope.cancellation().unwrap();
        let claim = {
            let mut state = control.inner.state.lock().unwrap();
            let claim = state.cancellations.slots[original.0].claim.unwrap();
            let ordinal = state.next_id().unwrap();
            state
                .joint
                .as_mut()
                .unwrap()
                .bind_control_fixture(original, claim, ordinal);
            claim
        };
        let bound = snapshot(&control);
        assert_eq!(bound.3, 1);
        work.business.release();
        work.owner.complete();
        assert!(matches!(scope.check(), Err(WorkError::Released)));
        drain(&mut driver);
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(
                state.cancellations.slots[original.0].phase,
                SlotPhase::Retired
            );
            assert_eq!(state.cancellations.slots[original.0].claim, Some(claim));
        }
        assert_eq!(
            snapshot(&control),
            bound,
            "logical collect never washes the original D"
        );
        drop(view);
        drain(&mut driver);
        let freed = snapshot(&control);
        assert_eq!(
            (freed.0, freed.1, freed.2),
            (prepaid.0, prepaid.1, prepaid.2)
        );
        assert_eq!(freed.3, 0);
    }

    #[tokio::test]
    async fn last_view_exit_wakes_the_original_idle_driver_without_a_business_event() {
        let (control, mut driver) = prototype(2);
        let work = root(&control);
        let scope = work.owner.scope();
        let original = slot(&control, scope.id);
        let view = scope.cancellation().unwrap();
        work.business.release();
        work.owner.complete();
        let marker = drain(&mut driver);
        let mut waiter = Box::pin(control.wait_progress(marker));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(view);
        assert!(matches!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(WorkloadProgress::StateChanged(_))
        ));
        drain(&mut driver);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
            SlotPhase::Free
        );
    }

    #[test]
    fn an_actual_raw_alias_prevents_free_until_its_exit_guard_notifies() {
        let (control, mut driver) = prototype(2);
        let work = root(&control);
        let scope = work.owner.scope();
        let original = slot(&control, scope.id);
        let wake_on_exit = ReclaimWakeOnExit::new(&control.inner);
        let raw = {
            let state = control.inner.state.lock().unwrap();
            Arc::clone(state.cancellations.cell(original))
        };
        work.business.release();
        work.owner.complete();
        let marker = drain(&mut driver);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
            SlotPhase::Retired
        );
        drop(raw);
        drop(wake_on_exit);
        assert_ne!(control.progress_revision(), marker);
        drain(&mut driver);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
            SlotPhase::Free
        );
    }

    #[test]
    fn observing_unset_cached_and_sealed_reasons_does_not_wake_the_reaper() {
        let (control, mut driver) = prototype(3);
        let work = root(&control);
        let scope = work.owner.scope();
        let root_view = scope.cancellation().unwrap();
        let child = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
        let child_view = child.scope().cancellation().unwrap();
        let sealed = root(&control);
        let sealed_view = sealed.owner.scope().cancellation().unwrap();
        let before = drain(&mut driver);
        assert_eq!(root_view.reason(), None);
        assert_eq!(child_view.reason(), None);
        assert_eq!(
            control.progress_revision(),
            before,
            "a negative read has no alias exit to publish"
        );
        assert_eq!(
            sealed.owner.success_sealer().seal().unwrap(),
            WorkSuccessSealOutcome::Sealed
        );
        work.owner.cancel(CancellationReason::Requested);
        let after = drain(&mut driver);
        for _ in 0..3 {
            assert_eq!(root_view.reason(), Some(CancellationReason::Requested));
            assert_eq!(child_view.reason(), Some(CancellationReason::Requested));
            assert_eq!(
                sealed_view.reason(),
                None,
                "sealed success remains immutable"
            );
        }
        assert_eq!(
            control.progress_revision(),
            after,
            "cached and sealed reads cannot manufacture progress"
        );
        // The real cancellation intent is still ready for its dispatcher.
        // Observe the revision-only waiter, not that independent ready queue.
        let deadlines = control.deadline_expiry_handle();
        let mut progress = Box::pin(deadlines.wait_state_change(after));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(progress);
        child.complete_after_terminal_cancel_settled();
        work.business.release();
        work.owner.complete_after_terminal_cancel_settled();
        sealed.business.release();
        sealed.owner.complete();
        drop(sealed_view);
        drop(child_view);
        drop(root_view);
        drain(&mut driver);
    }

    struct PausedCancellationWake {
        armed: std::sync::atomic::AtomicBool,
        entered: std::sync::mpsc::SyncSender<()>,
        resume: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl std::task::Wake for PausedCancellationWake {
        fn wake(self: Arc<Self>) {
            if !self.armed.swap(false, Ordering::AcqRel) {
                return;
            }
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .expect("the test must release the actual cancellation wake");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retained_root_reason_reports_descendant_alias_exit_to_idle_reaper() {
        let (control, mut driver) = prototype(3);
        let work = control
            .try_begin_root(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(Instant::now() + Duration::from_secs(1)),
            })
            .unwrap();
        let scope = work.owner.scope();
        let root_slot = slot(&control, scope.id);
        let root_view = scope.cancellation().unwrap();
        let child = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
        let child_scope = child.scope();
        let child_slot = slot(&control, child_scope.id);
        let original_claim =
            control.inner.state.lock().unwrap().cancellations.slots[child_slot.0].claim;
        let child_view = child_scope.cancellation().unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
        let paused = Arc::new(PausedCancellationWake {
            armed: std::sync::atomic::AtomicBool::new(false),
            entered: entered_tx,
            resume: Mutex::new(resume_rx),
        });
        let waker = Waker::from(Arc::clone(&paused));
        let mut observer = Box::pin(async move { child_view.cancelled().await });
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        // Advancing the timer is not the DFS notification being inspected.
        paused.armed.store(true, Ordering::Release);
        let runtime = tokio::runtime::Handle::current();
        let (before, after, waiting, phase, aliases, claim, reason) =
            std::thread::scope(|threads| {
                let request = threads.spawn(|| {
                    let _runtime = runtime.enter();
                    root_view.reason()
                });
                entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                // Tokio has released its waiter lock before invoking this Waker.
                // The real observer can exit while the DFS still holds its child Arc.
                drop(observer);
                child.complete_after_terminal_cancel_settled();
                let before = drain(&mut driver);
                let (phase, aliases, claim) = {
                    let state = control.inner.state.lock().unwrap();
                    let slot = &state.cancellations.slots[child_slot.0];
                    (
                        slot.phase,
                        Arc::strong_count(state.cancellations.cell(child_slot)),
                        slot.claim,
                    )
                };
                let mut waiter = Box::pin(control.wait_progress(before));
                let waiting = waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                resume_tx.send(()).unwrap();
                let reason = request.join().unwrap();
                let after = control.progress_revision();
                let notified = waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                (
                    before,
                    after,
                    (waiting, notified),
                    phase,
                    aliases,
                    claim,
                    reason,
                )
            });
        assert_eq!(reason, Some(CancellationReason::DeadlineExceeded));
        assert_eq!(phase, SlotPhase::Retired);
        assert_eq!(aliases, 2, "only the pool and actual DFS retain the child");
        assert_eq!(claim, original_claim);
        assert!(waiting.0.is_pending());
        assert_ne!(
            after, before,
            "temporary descendant exit must publish its own wake"
        );
        assert!(waiting.1.is_ready());
        drain(&mut driver);
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(
                state.cancellations.slots[child_slot.0].phase,
                SlotPhase::Free
            );
            assert_eq!(
                state.cancellations.slots[child_slot.0].claim,
                original_claim
            );
            assert_ne!(
                state.cancellations.slots[root_slot.0].phase,
                SlotPhase::Free
            );
        }
        // Keep this original view alive through every assertion above: its Drop
        // must not stand in for the already exited descendant traversal alias.
        drop(root_view);
        work.business.release();
        work.owner.complete_after_terminal_cancel_settled();
        drain(&mut driver);
    }

    #[test]
    fn weak_observation_is_not_mistaken_for_exclusive_reuse() {
        let (control, mut driver) = prototype(2);
        let work = root(&control);
        let scope = work.owner.scope();
        let original = slot(&control, scope.id);
        let weak = {
            let state = control.inner.state.lock().unwrap();
            Arc::downgrade(state.cancellations.cell(original))
        };
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
        assert_eq!(
            weak.strong_count(),
            1,
            "the pool anchor alone does not imply get_mut succeeds"
        );
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
            SlotPhase::Retired
        );
        drop(weak);
        // There is no public production Weak escape; publish the fixture's
        // private-edge drop just as the real detach/reset author does.
        control.inner.notify_reclaim_progress();
        drain(&mut driver);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
            SlotPhase::Free
        );
    }

    #[test]
    fn leaf_parent_hint_reclaims_a_retired_chain_in_linear_cell_checks() {
        let count = 32;
        let (control, mut driver) = prototype(count);
        let work = root(&control);
        let root_scope = work.owner.scope();
        let mut parent = root_scope.clone();
        let mut owners = vec![work.owner];
        for _ in 1..count {
            let child = parent.child(WorkRequest::new(WorkClass::Query)).unwrap();
            parent = child.scope();
            owners.push(child);
        }
        let view = parent.cancellation().unwrap();
        let (root_slot, capacity) = {
            let state = control.inner.state.lock().unwrap();
            let root_slot = state.nodes[&root_scope.id].cancellation;
            (
                root_slot,
                state
                    .cancellations
                    .cell(root_slot)
                    .child_capacity_for_test(),
            )
        };
        work.business.release();
        for owner in owners {
            owner.complete();
        }
        assert_eq!(control.snapshot().root_responsibilities, 0);
        drain(&mut driver);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.retired,
            count
        );
        let before = driver.reclaim_checks;
        drop(view);
        drain(&mut driver);
        assert!(driver.reclaim_checks - before <= count * 4);
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.cancellations.retired, 0);
        assert!(
            state
                .cancellations
                .slots
                .iter()
                .all(|slot| slot.phase == SlotPhase::Free)
        );
        assert_eq!(
            state
                .cancellations
                .cell(root_slot)
                .child_capacity_for_test(),
            capacity
        );
    }

    #[test]
    fn unpublished_child_failure_detaches_and_retires_its_real_fixed_slot() {
        let (control, mut driver) = prototype(2);
        let work = root(&control);
        let scope = work.owner.scope();
        let original = slot(&control, scope.id);
        assert!(matches!(
            scope.child(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(Instant::now())
            }),
            Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
        ));
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(state.nodes[&scope.id].children, 0);
            assert_eq!(
                state
                    .cancellations
                    .cell(original)
                    .linked_children_for_test(),
                0
            );
            assert_eq!(state.cancellations.retired, 1);
        }
        drain(&mut driver);
        assert_eq!(control.inner.state.lock().unwrap().cancellations.retired, 0);
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
    }

    #[test]
    fn same_cell_reuse_changes_original_work_and_claim_generation_without_refunding_base() {
        let (control, mut driver) = prototype(2);
        let prepaid = snapshot(&control);
        let first = root(&control);
        let scope = first.owner.scope();
        let requester = first.owner.cancellation_requester();
        let original = slot(&control, scope.id);
        let (address, generation, revision, claim) = {
            let state = control.inner.state.lock().unwrap();
            let cell = &state.cancellations.slots[original.0];
            (
                Arc::as_ptr(cell.cell.as_ref().unwrap()),
                cell.generation,
                state
                    .joint
                    .as_ref()
                    .unwrap()
                    .control_revision_for_test(cell.claim.unwrap()),
                cell.claim.unwrap(),
            )
        };
        first.business.release();
        first.owner.complete();
        drain(&mut driver);
        let second = root(&control);
        let second_scope = second.owner.scope();
        assert_ne!(scope.id, second_scope.id);
        assert_eq!(slot(&control, second_scope.id), original);
        assert!(matches!(
            requester.request(CancellationReason::Requested),
            Err(WorkError::Released)
        ));
        assert!(second_scope.cancellation().unwrap().reason().is_none());
        {
            let state = control.inner.state.lock().unwrap();
            let cell = &state.cancellations.slots[original.0];
            assert_eq!(Arc::as_ptr(cell.cell.as_ref().unwrap()), address);
            assert!(cell.generation > generation);
            assert_eq!(cell.claim, Some(claim));
            assert!(
                state
                    .joint
                    .as_ref()
                    .unwrap()
                    .control_revision_for_test(claim)
                    > revision
            );
        }
        assert_eq!(snapshot(&control), prepaid);
        second.business.release();
        second.owner.complete();
        drain(&mut driver);
    }

    #[test]
    fn shutdown_requires_exact_free_inventory_but_keeps_its_prepaid_profiles() {
        let (mut control, mut driver) = prototype(2);
        let prepaid = snapshot(&control);
        let work = root(&control);
        let view = work.owner.scope().cancellation().unwrap();
        work.business.release();
        work.owner.complete();
        control.close_admission();
        drain(&mut driver);
        let failure = control
            .shutdown()
            .err()
            .expect("retained backing is not drained");
        assert_eq!(failure.error(), WorkloadShutdownError::NotDrained);
        control = failure.into_parts().1;
        drop(view);
        drain(&mut driver);
        assert_eq!(snapshot(&control), prepaid);
        assert!(
            control.shutdown().is_ok(),
            "Free process claims remain charged through real teardown"
        );
    }

    #[test]
    fn bounded_fanout_failure_preserves_the_free_head_and_later_reuses_returned_storage() {
        let (control, mut driver) = prototype_with_fanout(4, 1);
        let work = root(&control);
        let scope = work.owner.scope();
        let first = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
        let first_slot = slot(&control, first.scope().id);
        let (heads, prepaid) = {
            let state = control.inner.state.lock().unwrap();
            assert!(state.cancellations.free_inventory_valid());
            (
                state.cancellations.free_heads,
                state.joint.as_ref().unwrap().control_snapshot_for_test(),
            )
        };
        assert!(matches!(
            scope.child(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::Capacity("prepaid parent child backing"))
        ));
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(state.cancellations.free_heads, heads);
            assert!(state.cancellations.free_inventory_valid());
            assert_eq!(state.nodes[&scope.id].children, 1);
        }
        assert_eq!(snapshot(&control), prepaid);
        first.complete();
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(
                state.cancellations.slots[first_slot.0].phase,
                SlotPhase::Retired
            );
            assert_eq!(
                state.cancellations.free_heads, heads,
                "Retired is never free inventory"
            );
            assert!(state.cancellations.free_inventory_valid());
        }
        drain(&mut driver);
        assert_eq!(
            control.inner.state.lock().unwrap().cancellations.free_heads[0],
            Some(first_slot)
        );
        let second = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
        assert_eq!(slot(&control, second.scope().id), first_slot);
        second.complete();
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
        assert!(control.inner.state.lock().unwrap().cancellations.drained());
    }

    #[test]
    fn rejected_fixed_root_deadline_leaves_its_head_and_original_claim_untouched() {
        let (control, mut driver) = prototype(2);
        let heads = control.inner.state.lock().unwrap().cancellations.free_heads;
        let prepaid = snapshot(&control);
        assert!(matches!(
            control.try_begin_root(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(Instant::now())
            }),
            Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
        ));
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(state.cancellations.free_heads, heads);
            assert!(state.cancellations.drained());
            assert!(state.nodes.is_empty());
        }
        assert_eq!(snapshot(&control), prepaid);
        let work = root(&control);
        assert_eq!(slot(&control, work.owner.scope().id), heads[0].unwrap());
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
    }

    #[test]
    fn drained_requires_complete_acyclic_class_correct_free_chains() {
        let (control, _driver) = prototype(4);
        control.close_admission();
        {
            let mut state = control.inner.state.lock().unwrap();
            assert!(state.is_drained());
            let heads = state.cancellations.free_heads;
            let head = heads[0].unwrap();
            let next = state.cancellations.slots[head.0].next_free;
            state.cancellations.free_heads[0] = None;
            assert!(
                !state.is_drained(),
                "unreachable Free entries cannot certify drain"
            );
            state.cancellations.free_heads = heads;
            state.cancellations.slots[head.0].next_free = Some(head);
            assert!(
                !state.is_drained(),
                "a cycle must be rejected within the visit budget"
            );
            state.cancellations.slots[head.0].next_free = None;
            assert!(
                !state.is_drained(),
                "a disconnected suffix cannot certify drain"
            );
            state.cancellations.slots[head.0].next_free = next;
            state.cancellations.free_heads[1] = Some(head);
            assert!(
                !state.is_drained(),
                "one slot cannot appear in a foreign class chain"
            );
            state.cancellations.free_heads = heads;
            state.cancellations.free_heads[0] = Some(SlotId(state.cancellations.slots.len()));
            assert!(
                !state.is_drained(),
                "out-of-inventory links are not followed"
            );
            state.cancellations.free_heads = heads;
            state.cancellations.slots[head.0].phase = SlotPhase::Retired;
            assert!(!state.is_drained(), "a linked Retired entry is never Free");
            state.cancellations.slots[head.0].phase = SlotPhase::Free;
            assert!(state.is_drained());
        }
        assert!(control.shutdown().is_ok());
    }

    fn legacy_control() -> WorkloadControl {
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        control.mark_ready().unwrap();
        control
    }

    #[test]
    fn legacy_vacant_head_reuses_the_slot_without_reusing_retained_observer_backing() {
        let control = legacy_control();
        let first = root(&control);
        let first_scope = first.owner.scope();
        let first_slot = slot(&control, first_scope.id);
        let view = first_scope.cancellation().unwrap();
        let first_address = {
            let state = control.inner.state.lock().unwrap();
            Arc::as_ptr(state.cancellations.cell(first_slot))
        };
        first.business.release();
        first.owner.complete();
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(state.cancellations.legacy_vacant, Some(first_slot));
            assert!(state.cancellations.drained());
        }
        let second = root(&control);
        let second_scope = second.owner.scope();
        assert_eq!(slot(&control, second_scope.id), first_slot);
        assert_ne!(first_scope.id, second_scope.id);
        {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(state.cancellations.slots.len(), 1);
            assert_eq!(state.cancellations.legacy_vacant, None);
            assert!(state.cancellations.free_inventory_valid());
            assert_ne!(
                Arc::as_ptr(state.cancellations.cell(first_slot)),
                first_address
            );
            assert_eq!(
                state.cancellations.slots[first_slot.0].phase,
                SlotPhase::Node(second_scope.id)
            );
        }
        assert!(
            view.reason().is_none(),
            "the old observer still sees its original cell"
        );
        second.business.release();
        second.owner.complete();
        {
            let mut state = control.inner.state.lock().unwrap();
            assert!(state.cancellations.drained());
            let next = state.cancellations.slots[first_slot.0].next_free;
            state.cancellations.slots[first_slot.0].next_free = Some(first_slot);
            assert!(
                !state.cancellations.drained(),
                "legacy cycles also have a finite visit budget"
            );
            state.cancellations.slots[first_slot.0].next_free = next;
            assert!(state.cancellations.drained());
        }
        drop(view);
    }

    #[test]
    fn legacy_generation_exhaustion_does_not_pop_or_lose_the_vacant_head() {
        let control = legacy_control();
        let work = root(&control);
        let original = slot(&control, work.owner.scope().id);
        work.business.release();
        work.owner.complete();
        {
            let mut state = control.inner.state.lock().unwrap();
            state.cancellations.slots[original.0].generation = u64::MAX;
        }
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::ArithmeticOverflow)
        ));
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.cancellations.legacy_vacant, Some(original));
        assert_eq!(state.cancellations.slots.len(), 1);
        assert!(state.cancellations.drained());
        assert!(state.nodes.is_empty());
    }

    #[test]
    fn storage_fanout_is_finite_configurable_and_charged_from_actual_layout() {
        let narrow = ControlStorageConfig {
            slots_per_class: [4, 0, 0, 0, 0],
            children_per_slot: 1,
        };
        let wider = ControlStorageConfig {
            children_per_slot: 2,
            ..narrow
        };
        let impossible = ControlStorageConfig {
            children_per_slot: 4,
            ..narrow
        };
        let narrow = crate::joint_admission::ledger::control_storage_base(narrow).unwrap();
        let wider = crate::joint_admission::ledger::control_storage_base(wider).unwrap();
        assert!(wider.get(JointResource::MetadataBytes) > narrow.get(JointResource::MetadataBytes));
        assert!(matches!(
            crate::joint_admission::ledger::control_storage_base(impossible),
            Err(WorkError::InvalidConfig("fixed control storage geometry"))
        ));
    }
}
