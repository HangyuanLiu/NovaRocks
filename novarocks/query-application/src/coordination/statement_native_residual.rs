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

//! Original statement Native ownership inside the existing process supervisor.
//! No actor, Task/SQL conclusion, dispatch authority, or WorkOwner is created here.

use crate::api::{
    NativeAttemptConvergence, QueryExecutionError, QueryExecutionErrorKind,
    StatementNativeAttemptOwner, StatementNativeClosure, StatementNativeConvergenceRequest,
    StatementNativeHealthFailure, StatementNativeLocalClosureKind, StatementNativeLocalHolder,
    StatementNativeRemoteClosure,
};
use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
use novarocks_execution_contract::{
    QueryContextRef, SafeDetail,
    failure::{FailureFact, FailureProjection},
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{
    Obligation, ObligationKey, ObligationKind, WorkError, WorkId, WorkScope,
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::Poll,
    time::{Duration, Instant},
};
use tokio::sync::{Notify, watch};

type NativeTaskFuture = Pin<Box<dyn Future<Output = Result<(), QueryExecutionError>> + Send>>;

/// Typed storage owned only by LogicalExecutionRuntimeRegistry. Clones are
/// submission/observation capabilities, never a new process owner or service.
#[derive(Clone)]
pub(crate) struct StatementNativeResidualRegistry(Arc<RegistryInner>);
struct RegistryInner {
    id: u64,
    state: Mutex<RegistryState>,
    changed: Notify,
}
struct RegistryState {
    accepting: bool,
    next_generation: u64,
    entries: BTreeMap<QueryExecutionId, Entry>,
    // Original panic payload or its complete poll-failure custody packet is
    // retained independently of task/entry retirement. This private Any carrier
    // keeps the original fields together; intake never projects their custody.
    health: Option<OriginalHealth>,
}
/// The custody transaction is finished. Deliver only after every caller lock.
pub(crate) struct StatementNativeHealthNotification(Arc<RegistryInner>);
impl StatementNativeHealthNotification {
    pub(crate) fn notify(self) {
        self.0.changed.notify_one();
    }
}
struct OriginalHealth {
    message: &'static str,
    _panic: Option<Box<dyn Any + Send>>,
    _cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
struct Entry {
    identity: StatementNativeRegistration,
    contexts: BTreeSet<QueryContextRef>,
    local: BTreeSet<StatementNativeLocalHolder>,
    obligation: Obligation,
    slot: Arc<dyn ErasedSlot>,
    remote_deadline: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatementNativeRegistration {
    registry: u64,
    generation: u64,
    execution: QueryExecutionId,
    scope: WorkId,
}
impl StatementNativeRegistration {
    pub const fn execution(self) -> QueryExecutionId {
        self.execution
    }
    pub const fn scope(self) -> WorkId {
        self.scope
    }
}
#[derive(Clone)]
pub struct StatementNativeResidualPort {
    registry: StatementNativeResidualRegistry,
    remote_cleanup_budget: Duration,
}
impl std::fmt::Debug for StatementNativeResidualPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementNativeResidualPort")
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatementNativeRegistrationError {
    Closed,
    Duplicate,
    WrongExecution,
    InvalidContexts,
    DuplicateLocalHolder,
    Governance(WorkError),
    GenerationExhausted,
}
impl FailureProjection for StatementNativeRegistrationError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            // This owner neither reclassifies workload facts nor turns an
            // observation of an existing stop into accepted client cancel.
            Self::Governance(error) => error.failure_fact(),
            Self::Closed => QueryFailureReason::LogicalFailureUnspecified.failure_fact(),
            Self::Duplicate
            | Self::WrongExecution
            | Self::InvalidContexts
            | Self::DuplicateLocalHolder
            | Self::GenerationExhausted => QueryFailureReason::PreparationProtocol.failure_fact(),
        }
    }
}
impl StatementNativeRegistrationError {
    /// Fixed details authored by this registration owner. Foreign error
    /// Display text never becomes client-safe detail.
    pub fn safe_detail(&self) -> SafeDetail {
        SafeDetail::truncating(match self {
            Self::Closed => "statement Native responsibility intake is closed",
            Self::Duplicate => "statement Native responsibility is already registered",
            Self::WrongExecution => "statement Native owner has a different execution identity",
            Self::InvalidContexts => "statement Native owner has invalid exact execution contexts",
            Self::DuplicateLocalHolder => "statement Native owner repeats a local holder role",
            Self::Governance(_) => {
                "statement Native responsibility registration was refused by workload control"
            }
            Self::GenerationExhausted => "statement Native registration generation is exhausted",
        })
    }
    pub fn into_execution_failure_input(self) -> ExecutionFailureInput {
        ExecutionFailureInput::new(
            self.failure_fact(),
            self.safe_detail(),
            ExecutionFailureSite::Coordinator,
        )
    }
}
impl std::fmt::Display for StatementNativeRegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.safe_detail().as_str())
    }
}
impl std::error::Error for StatementNativeRegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Governance(error) => Some(error),
            _ => None,
        }
    }
}
pub struct StatementNativeRegistrationRejected<T: ?Sized> {
    pub error: StatementNativeRegistrationError,
    pub owner: Box<T>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementNativeCompletionError {
    WrongRegistration,
    WrongExecution,
    WrongScope,
    InvalidContexts,
    InvalidLocalHolders,
    RemoteBudgetNotElapsed,
    ResponsibilitySettlementFailed,
}
impl std::fmt::Display for StatementNativeCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::WrongRegistration => "statement Native completion names another registration",
            Self::WrongExecution => "statement Native completion names another execution",
            Self::WrongScope => "statement Native completion names another work scope",
            Self::InvalidContexts => "statement Native completion has invalid exact contexts",
            Self::InvalidLocalHolders => "statement Native completion lacks original local closure",
            Self::RemoteBudgetNotElapsed => {
                "statement Native remote observation budget has not elapsed"
            }
            Self::ResponsibilitySettlementFailed => {
                "statement Native responsibility settlement failed"
            }
        })
    }
}
impl std::error::Error for StatementNativeCompletionError {}
/// A rejected dispatched completion retains the original lease, never a bare T.
pub struct StatementNativeCompletionRejected<T: StatementNativeAttemptOwner + ?Sized> {
    pub error: StatementNativeCompletionError,
    pub lease: StatementNativeForegroundLease<T>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatementNativeHandoffReceipt {
    registration: StatementNativeRegistration,
}
impl StatementNativeHandoffReceipt {
    pub const fn registration(self) -> StatementNativeRegistration {
        self.registration
    }
}

struct SlotState<T: ?Sized> {
    owner: Option<Box<T>>,
    foreground: bool,
    running: bool,
    retired: bool,
    health_blocked: bool,
    shutdown_delivered: bool,
}
struct Slot<T: ?Sized> {
    identity: StatementNativeRegistration,
    scope: WorkScope,
    registry: Weak<RegistryInner>,
    state: Mutex<SlotState<T>>,
    shutdown: watch::Sender<bool>,
    remote_cleanup_budget: Duration,
}
trait ErasedSlot: Send + Sync {
    fn request_shutdown(&self);
    fn start(self: Arc<Self>) -> Option<NativeTaskFuture>;
}

/// An exclusive move-only borrow of the original Box. No take/into_inner API
/// exists after dispatch. Foreground code must keep every inner owner in T.
pub struct StatementNativeForegroundLease<T: StatementNativeAttemptOwner + ?Sized> {
    owner: Option<Box<T>>,
    slot: Arc<Slot<T>>,
}
impl<T: StatementNativeAttemptOwner + ?Sized> StatementNativeForegroundLease<T> {
    /// Freeze the role cleanup budget at the FIRST original terminal cut.
    /// Repeated calls and a later handoff return this same absolute deadline.
    pub fn begin_terminal_cut(
        &self,
        cut: Instant,
    ) -> Result<StatementNativeConvergenceRequest, StatementNativeCompletionError> {
        let registry = StatementNativeResidualRegistry(
            self.slot
                .registry
                .upgrade()
                .expect("original process registry retains slot"),
        );
        registry.begin_terminal_cut(
            self.slot.identity,
            &self.slot.scope,
            cut,
            self.slot.remote_cleanup_budget,
        )
    }
    /// Contains a synchronous foreground native phase while preserving the
    /// original panic payload in process-owned health storage. The owner stays
    /// in this lease through catch_unwind and can be handed back afterwards.
    pub fn catch_foreground<R>(
        &mut self,
        operation: impl FnOnce(&mut T) -> R,
    ) -> Result<R, QueryExecutionError> {
        match catch_unwind(AssertUnwindSafe(|| operation(self.borrow_mut()))) {
            Ok(result) => Ok(result),
            Err(payload) => {
                let registry = StatementNativeResidualRegistry(
                    self.slot
                        .registry
                        .upgrade()
                        .expect("registered foreground retains process owner"),
                );
                registry.record_health(
                    "statement Native foreground execution panicked",
                    Some(payload),
                );
                let _ = self.begin_terminal_cut(Instant::now());
                Err(registry
                    .health()
                    .expect("original foreground panic is retained"))
            }
        }
    }
    /// Contains foreground polling of the original convergence owner. Unlike
    /// an execution-phase panic, an unexpected convergence panic cannot safely
    /// re-poll an unknown already-panicked family. Preserve this same owner,
    /// its original obligation and raw payload until explicit process exit.
    /// Ordinary typed close errors remain the owner's return value.
    pub fn catch_convergence<R>(
        &mut self,
        operation: impl FnOnce(&mut T) -> R,
    ) -> Result<R, QueryExecutionError> {
        match catch_unwind(AssertUnwindSafe(|| operation(self.borrow_mut()))) {
            Ok(result) => Ok(result),
            Err(payload) => {
                let registry = StatementNativeResidualRegistry(
                    self.slot
                        .registry
                        .upgrade()
                        .expect("registered convergence retains process owner"),
                );
                self.slot
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .health_blocked = true;
                registry.record_health(
                    "statement Native foreground convergence panicked",
                    Some(payload),
                );
                // Keep the original cut when already set by foreground. A
                // missing cut is frozen here, never re-borrowed on handoff.
                let _ = self.begin_terminal_cut(Instant::now());
                Err(registry
                    .health()
                    .expect("original convergence panic is retained"))
            }
        }
    }
    #[expect(
        clippy::should_implement_trait,
        reason = "Changing this public foreground-owner accessor to BorrowMut requires cross-crate API review."
    )]
    pub fn borrow_mut(&mut self) -> &mut T {
        self.owner.as_deref_mut().expect("live original owner")
    }
    pub fn registration(&self) -> StatementNativeRegistration {
        self.slot.identity
    }
    pub fn shutdown_requested(&self) -> bool {
        *self.slot.shutdown.borrow()
    }
    pub fn handoff(self) -> StatementNativeHandoffReceipt {
        let receipt = StatementNativeHandoffReceipt {
            registration: self.slot.identity,
        };
        // Drop restores the SAME original Box into its already registered slot.
        drop(self);
        receipt
    }
    pub fn complete(
        mut self,
        closure: StatementNativeClosure,
    ) -> Result<(), StatementNativeCompletionRejected<T>> {
        let registry = self
            .slot
            .registry
            .upgrade()
            .expect("process registry outlives issued lease");
        let registry = StatementNativeResidualRegistry(registry);
        if let Err(error) = registry.validate(self.slot.identity, &closure) {
            registry.record_health(
                "statement Native foreground closure did not match its registration",
                None,
            );
            return Err(StatementNativeCompletionRejected { error, lease: self });
        }
        if !registry.retire(
            self.slot.identity,
            matches!(
                closure.remote,
                StatementNativeRemoteClosure::TrackingEnded { .. }
            ),
        ) {
            return Err(StatementNativeCompletionRejected {
                error: StatementNativeCompletionError::ResponsibilitySettlementFailed,
                lease: self,
            });
        }
        // Actual work and retained worker payloads have already exited by the
        // original owner's proof. Remaining admission/window aliases still
        // account independently until the closed T is destroyed here.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(self.owner.take()))) {
            registry.record_health(
                "closed statement Native owner panicked during retirement",
                Some(payload),
            );
        }
        self.slot
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retired = true;
        Ok(())
    }
}
impl<T: StatementNativeAttemptOwner + ?Sized> Drop for StatementNativeForegroundLease<T> {
    fn drop(&mut self) {
        let Some(owner) = self.owner.take() else {
            return;
        };
        let mut slot = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        // Restore before publishing: notification loss cannot discard ownership.
        slot.owner = Some(owner);
        slot.foreground = false;
        drop(slot);
        if let Some(registry) = self.slot.registry.upgrade() {
            if std::thread::panicking() {
                // The enclosing foreground catch owns the original unwind
                // payload. Drop never consumes it or changes the query cause.
                StatementNativeResidualRegistry(Arc::clone(&registry)).record_health(
                    "statement Native foreground owner unwound before handoff",
                    None,
                );
            }
            registry.changed.notify_one();
        }
    }
}

impl StatementNativeResidualPort {
    /// Register once BEFORE any Native dispatch. Error returns the unstarted
    /// original Box. Success registers only RunningWork under the original scope.
    pub fn register<T: StatementNativeAttemptOwner + ?Sized>(
        &self,
        execution: QueryExecutionId,
        scope: &WorkScope,
        owner: Box<T>,
    ) -> Result<StatementNativeForegroundLease<T>, StatementNativeRegistrationRejected<T>> {
        let mut state = self
            .registry
            .0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let checked = (|| {
            if !state.accepting {
                return Err(StatementNativeRegistrationError::Closed);
            }
            if state.entries.contains_key(&execution) {
                return Err(StatementNativeRegistrationError::Duplicate);
            }
            if owner.execution() != execution {
                return Err(StatementNativeRegistrationError::WrongExecution);
            }
            let contexts = owner.contexts().iter().copied().collect::<BTreeSet<_>>();
            if contexts.is_empty()
                || contexts.len() != owner.contexts().len()
                || contexts.iter().any(|c| c.query_execution_id() != execution)
            {
                return Err(StatementNativeRegistrationError::InvalidContexts);
            }
            let local = owner
                .local_holders()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            if local.len() != owner.local_holders().len() {
                return Err(StatementNativeRegistrationError::DuplicateLocalHolder);
            }
            let generation = state
                .next_generation
                .checked_add(1)
                .ok_or(StatementNativeRegistrationError::GenerationExhausted)?;
            let obligation = scope
                .register_obligation(obligation_key(execution), ObligationKind::RunningWork)
                .map_err(StatementNativeRegistrationError::Governance)?;
            Ok((contexts, local, generation, obligation))
        })();
        let (contexts, local, generation, obligation) = match checked {
            Ok(values) => values,
            Err(error) => return Err(StatementNativeRegistrationRejected { error, owner }),
        };
        state.next_generation = generation;
        let identity = StatementNativeRegistration {
            registry: self.registry.0.id,
            generation,
            execution,
            scope: scope.id(),
        };
        let (shutdown, _) = watch::channel(false);
        let slot = Arc::new(Slot {
            identity,
            scope: scope.clone(),
            registry: Arc::downgrade(&self.registry.0),
            state: Mutex::new(SlotState {
                owner: None,
                foreground: true,
                running: false,
                retired: false,
                health_blocked: false,
                shutdown_delivered: false,
            }),
            shutdown,
            remote_cleanup_budget: self.remote_cleanup_budget,
        });
        state.entries.insert(
            execution,
            Entry {
                identity,
                contexts,
                local,
                obligation,
                slot: slot.clone(),
                remote_deadline: None,
            },
        );
        drop(state);
        self.registry.0.changed.notify_one();
        Ok(StatementNativeForegroundLease {
            owner: Some(owner),
            slot,
        })
    }
}

fn obligation_key(execution: QueryExecutionId) -> ObligationKey {
    let mut bytes = [0; 32];
    // Zero is reserved for this direct statement attempt: actor keys start
    // with their nonzero LogicalExecutionActorId. Exact query/attempt follow.
    bytes[8..16].copy_from_slice(&execution.query_id().high().to_le_bytes());
    bytes[16..24].copy_from_slice(&execution.query_id().low().to_le_bytes());
    bytes[24..32].copy_from_slice(&execution.attempt_id().get().to_le_bytes());
    ObligationKey(bytes)
}

impl StatementNativeResidualRegistry {
    pub(crate) fn new(id: u64) -> Self {
        Self(Arc::new(RegistryInner {
            id,
            state: Mutex::new(RegistryState {
                accepting: true,
                next_generation: 0,
                entries: BTreeMap::new(),
                health: None,
            }),
            changed: Notify::new(),
        }))
    }
    pub(crate) fn port(&self, remote_cleanup_budget: Duration) -> StatementNativeResidualPort {
        StatementNativeResidualPort {
            registry: self.clone(),
            remote_cleanup_budget,
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .len()
    }
    pub(crate) async fn changed(&self) {
        self.0.changed.notified().await;
    }
    pub(crate) fn close(&self) {
        let slots = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            state.accepting = false;
            let now = Instant::now();
            for entry in state.entries.values_mut() {
                if let Some(deadline) = &mut entry.remote_deadline {
                    *deadline = (*deadline).min(now);
                }
            }
            state
                .entries
                .values()
                .map(|e| Arc::clone(&e.slot))
                .collect::<Vec<_>>()
        };
        for slot in slots {
            slot.request_shutdown();
        }
        self.0.changed.notify_waiters();
    }
    pub(crate) fn ready_tasks(&self) -> Vec<NativeTaskFuture> {
        let slots = self
            .0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .values()
            .map(|e| Arc::clone(&e.slot))
            .collect::<Vec<_>>();
        slots.into_iter().filter_map(|slot| slot.start()).collect()
    }
    pub(crate) fn health(&self) -> Option<QueryExecutionError> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .health
            .as_ref()
            .map(|h| QueryExecutionError::new(QueryExecutionErrorKind::Failed, h.message))
    }
    fn record_health(&self, message: &'static str, panic: Option<Box<dyn Any + Send>>) {
        let rejected = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.health.is_none() {
                state.health = Some(OriginalHealth {
                    message,
                    _panic: panic,
                    _cause: None,
                });
                None
            } else if let Some(first) = state.health.as_mut() {
                if first._panic.is_none() {
                    first._panic = panic;
                    None
                } else {
                    panic
                }
            } else {
                unreachable!("health presence checked")
            }
        };
        drop(rejected);
        self.0.changed.notify_one();
    }
    /// Accept the original boxed panic payload or whole poll-failure packet.
    /// The health transaction takes custody before any notification callback.
    /// Rejected later custody is destroyed only after the health lock exits.
    pub(crate) fn record_supervision_panic(&self, original: Box<dyn Any + Send>) {
        self.record_health(
            "logical execution supervision task panicked",
            Some(original),
        );
    }

    /// Silent borrowed intake for the original finite role source. Later
    /// failures keep their exact source; first-health policy is unchanged.
    pub(crate) fn record_supervision_panic_in(
        &self,
        original: &mut Option<Box<dyn Any + Send>>,
    ) -> Option<StatementNativeHealthNotification> {
        if original.is_none() {
            return None;
        }
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .health
            .as_ref()
            .is_some_and(|first| first._panic.is_some())
        {
            return None;
        }
        if let Some(first) = state.health.as_mut() {
            first._panic = original.take();
        } else {
            state.health = Some(OriginalHealth {
                message: "logical execution supervision task panicked",
                _panic: original.take(),
                _cause: None,
            });
        }
        drop(state);
        Some(StatementNativeHealthNotification(Arc::clone(&self.0)))
    }

    pub(crate) fn record_supervision_join_failure(&self, error: tokio::task::JoinError) {
        if error.is_panic() {
            self.record_supervision_panic(error.into_panic());
        } else {
            self.record_unproven_exit(StatementNativeHealthFailure::actual_exit_unproven(
                "logical execution supervision task exited before actual local closure",
                Arc::new(error),
            ));
        }
    }
    pub(crate) fn record_root_ack_join_failure(&self, error: tokio::task::JoinError) {
        if error.is_panic() {
            self.record_health(
                "optional root ACK task panicked after actual local exit",
                Some(error.into_panic()),
            );
        } else {
            self.record_health_cause(
                "optional root ACK task was cancelled after actual local exit",
                Arc::new(error),
            );
        }
    }
    pub(crate) fn record_unproven_exit(&self, failure: StatementNativeHealthFailure) {
        let (message, cause) = failure.into_parts();
        self.record_health_cause(message, cause);
    }
    fn record_health_cause(
        &self,
        message: &'static str,
        cause: Arc<dyn std::error::Error + Send + Sync>,
    ) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.health.is_none() {
            state.health = Some(OriginalHealth {
                message,
                _panic: None,
                _cause: Some(cause),
            });
        } else if let Some(first) = state.health.as_mut()
            && first._cause.is_none()
        {
            first._cause = Some(cause);
        }
        drop(state);
        self.0.changed.notify_one();
    }
    fn begin_terminal_cut(
        &self,
        identity: StatementNativeRegistration,
        scope: &WorkScope,
        cut: Instant,
        budget: Duration,
    ) -> Result<StatementNativeConvergenceRequest, StatementNativeCompletionError> {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&identity.execution)
            .filter(|e| e.identity == identity)
            .ok_or(StatementNativeCompletionError::WrongRegistration)?;
        let deadline = match entry.remote_deadline {
            Some(deadline) => deadline,
            None => {
                let deadline = cut
                    .checked_add(budget)
                    .ok_or(StatementNativeCompletionError::RemoteBudgetNotElapsed)?;
                entry.remote_deadline = Some(deadline);
                deadline
            }
        };
        let cancellation = scope
            .cancellation()
            .map_err(|_| StatementNativeCompletionError::WrongScope)?;
        Ok(StatementNativeConvergenceRequest::new(
            identity.execution,
            identity.scope,
            cancellation,
            deadline,
        ))
    }
    fn convergence_request(
        &self,
        identity: StatementNativeRegistration,
        scope: &WorkScope,
    ) -> Option<StatementNativeConvergenceRequest> {
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = state
            .entries
            .get(&identity.execution)
            .filter(|e| e.identity == identity)?
            .remote_deadline?;
        Some(StatementNativeConvergenceRequest::new(
            identity.execution,
            identity.scope,
            scope.cancellation().ok()?,
            deadline,
        ))
    }
    fn validate(
        &self,
        identity: StatementNativeRegistration,
        closure: &StatementNativeClosure,
    ) -> Result<(), StatementNativeCompletionError> {
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get(&identity.execution)
            .filter(|e| e.identity == identity)
            .ok_or(StatementNativeCompletionError::WrongRegistration)?;
        if closure.execution != identity.execution {
            return Err(StatementNativeCompletionError::WrongExecution);
        }
        if closure.scope != identity.scope {
            return Err(StatementNativeCompletionError::WrongScope);
        }
        let (contexts, count) = match &closure.remote {
            StatementNativeRemoteClosure::Evidence(
                NativeAttemptConvergence::AllWorkersStoppedAndContextsFenced,
            ) => (entry.contexts.clone(), entry.contexts.len()),
            StatementNativeRemoteClosure::Evidence(NativeAttemptConvergence::Contexts(facts)) => (
                facts.iter().map(|c| c.context()).collect::<BTreeSet<_>>(),
                facts.len(),
            ),
            StatementNativeRemoteClosure::TrackingEnded { exact_contexts } => {
                if entry
                    .remote_deadline
                    .is_none_or(|deadline| Instant::now() < deadline)
                {
                    return Err(StatementNativeCompletionError::RemoteBudgetNotElapsed);
                }
                (
                    exact_contexts.iter().copied().collect::<BTreeSet<_>>(),
                    exact_contexts.len(),
                )
            }
        };
        if contexts != entry.contexts || contexts.len() != count {
            return Err(StatementNativeCompletionError::InvalidContexts);
        }
        if closure.local.iter().any(|fact| {
            fact.kind() == StatementNativeLocalClosureKind::OriginalProcessOwnerReturn
                && !matches!(
                    fact.holder(),
                    StatementNativeLocalHolder::NativeOperations
                        | StatementNativeLocalHolder::StatusSubscriptions
                )
        }) {
            return Err(StatementNativeCompletionError::InvalidLocalHolders);
        }
        let local = closure
            .local
            .iter()
            .map(|c| c.holder())
            .collect::<BTreeSet<_>>();
        if local != entry.local || local.len() != closure.local.len() {
            return Err(StatementNativeCompletionError::InvalidLocalHolders);
        }
        Ok(())
    }
    fn retire(&self, identity: StatementNativeRegistration, remote_unknown: bool) -> bool {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = state
            .entries
            .get(&identity.execution)
            .filter(|e| e.identity == identity)
        else {
            drop(state);
            self.record_health(
                "statement Native retirement lost its original registration",
                None,
            );
            return false;
        };
        let settled = if remote_unknown {
            entry.obligation.end_remote_tracking_unknown()
        } else {
            entry.obligation.resolve()
        };
        if !settled {
            drop(state);
            self.record_health(
                "statement Native original responsibility could not be settled",
                None,
            );
            return false;
        }
        state.entries.remove(&identity.execution);
        drop(state);
        self.0.changed.notify_one();
        true
    }
    /// Only irrevocable process exit may release a nonconverged slot or panic
    /// payload. Normal shutdown preserves both and remains retryable.
    pub(crate) fn abandon_for_process_exit(&self) {
        self.close();
        let (entries, health) = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            (std::mem::take(&mut state.entries), state.health.take())
        };
        drop(entries);
        drop(health);
        self.0.changed.notify_waiters();
    }
    pub(crate) async fn drain(&self) {
        // Recovery borrows the SAME slots after supervisor failure; it spawns
        // nothing and retains every borrowed close future across new handoffs.
        let mut pending: Vec<NativeTaskFuture> = Vec::new();
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            pending.extend(self.ready_tasks());
            if self.len() == 0 {
                return;
            }
            tokio::select! {
                _ = &mut changed => {},
                _ = std::future::poll_fn(|cx| {
                    let mut moved = false;
                    let mut i = 0;
                    while i < pending.len() {
                        if pending[i].as_mut().poll(cx).is_ready() {
                            drop(pending.swap_remove(i)); moved = true;
                        } else { i += 1; }
                    }
                    if moved { Poll::Ready(()) } else { Poll::Pending }
                }), if !pending.is_empty() => {},
            }
        }
    }
}

struct BackgroundLease<T: StatementNativeAttemptOwner + ?Sized> {
    owner: Option<Box<T>>,
    slot: Arc<Slot<T>>,
}
impl<T: StatementNativeAttemptOwner + ?Sized> Drop for BackgroundLease<T> {
    fn drop(&mut self) {
        let mut slot = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(owner) = self.owner.take() {
            slot.owner = Some(owner);
        }
        slot.running = false;
        drop(slot);
        if let Some(registry) = self.slot.registry.upgrade() {
            registry.changed.notify_one();
        }
    }
}
impl<T: StatementNativeAttemptOwner + ?Sized> ErasedSlot for Slot<T> {
    fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }
    fn start(self: Arc<Self>) -> Option<NativeTaskFuture> {
        let owner = {
            let mut slot = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if slot.foreground || slot.running || slot.retired || slot.health_blocked {
                return None;
            }
            let owner = slot.owner.take()?;
            slot.running = true;
            owner
        };
        // The lease exists BEFORE the future is built/polled. Dropping a queued
        // JoinSet future or a failed supervisor restores the exact original Box.
        let lease = BackgroundLease {
            owner: Some(owner),
            slot: self,
        };
        Some(Box::pin(run_registered(lease)))
    }
}
async fn run_registered<T: StatementNativeAttemptOwner + ?Sized>(
    mut lease: BackgroundLease<T>,
) -> Result<(), QueryExecutionError> {
    let registry = StatementNativeResidualRegistry(
        lease
            .slot
            .registry
            .upgrade()
            .expect("original process registry retains slot"),
    );
    match catch_original_panic(run_original(&mut lease)).await {
        Ok(result) => result,
        Err(payload) => {
            registry.record_health("statement Native supervision task panicked", Some(payload));
            lease
                .slot
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .health_blocked = true;
            // lease Drop restores the same owner. Registry can resume it even
            // after its original supervisor JoinSet has failed.
            Err(registry
                .health()
                .expect("original supervision panic is retained"))
        }
    }
}

async fn run_original<T: StatementNativeAttemptOwner + ?Sized>(
    lease: &mut BackgroundLease<T>,
) -> Result<(), QueryExecutionError> {
    let registry = StatementNativeResidualRegistry(
        lease
            .slot
            .registry
            .upgrade()
            .expect("original registry retains slot"),
    );
    let mut shutdown = lease.slot.shutdown.subscribe();
    let mut shutdown_delivered = lease
        .slot
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .shutdown_delivered;
    loop {
        if *shutdown.borrow() && !shutdown_delivered {
            shutdown_delivered = true;
            lease
                .slot
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .shutdown_delivered = true;
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                lease
                    .owner
                    .as_deref_mut()
                    .unwrap()
                    .request_shutdown_cleanup()
            })) {
                registry.record_health("statement Native shutdown cleanup panicked", Some(payload));
                break;
            }
        }
        let request = match registry.convergence_request(lease.slot.identity, &lease.slot.scope) {
            Some(request) => request,
            None => {
                registry.record_health(
                    "statement Native residual has no original terminal cut or scope",
                    None,
                );
                break;
            }
        };
        enum Turn {
            Closed(StatementNativeClosure),
            Unproven(StatementNativeHealthFailure),
            Panicked(Box<dyn Any + Send>),
            Shutdown,
        }
        let turn = {
            let convergence = catch_original_panic(async {
                lease.owner.as_deref_mut().unwrap().converge(request).await
            });
            tokio::pin!(convergence);
            tokio::select! {
                result = &mut convergence => match result {
                    Ok(Ok(proof)) => Turn::Closed(proof),
                    Ok(Err(failure)) => Turn::Unproven(failure),
                    Err(payload) => Turn::Panicked(payload),
                },
                _ = shutdown.changed(), if !shutdown_delivered => Turn::Shutdown,
            }
        };
        match turn {
            Turn::Closed(closure) => match registry.validate(lease.slot.identity, &closure) {
                Ok(()) => {
                    if !registry.retire(
                        lease.slot.identity,
                        matches!(
                            closure.remote,
                            StatementNativeRemoteClosure::TrackingEnded { .. }
                        ),
                    ) {
                        break;
                    }
                    if let Err(payload) =
                        catch_unwind(AssertUnwindSafe(|| drop(lease.owner.take())))
                    {
                        registry.record_health(
                            "closed statement Native owner panicked during retirement",
                            Some(payload),
                        );
                    }
                    lease
                        .slot
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .retired = true;
                    return registry.health().map_or(Ok(()), Err);
                }
                Err(_) => registry.record_health(
                    "statement Native closure did not match its original registration",
                    None,
                ),
            },
            Turn::Unproven(failure) => registry.record_unproven_exit(failure),
            Turn::Panicked(payload) => {
                registry.record_health("statement Native convergence panicked", Some(payload))
            }
            Turn::Shutdown => continue,
        }
        break;
    }
    // Exit cannot be proved. Retain the same owner and original obligation in
    // the registered slot; do not invent a successful close or re-poll forever.
    lease
        .slot
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .health_blocked = true;
    Err(registry
        .health()
        .expect("unproven original exit retains process health failure"))
}
async fn catch_original_panic<F: Future>(future: F) -> Result<F::Output, Box<dyn Any + Send>> {
    let mut future = Box::pin(future);
    std::future::poll_fn(move |cx| {
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

impl<T: ?Sized> std::fmt::Debug for StatementNativeRegistrationRejected<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementNativeRegistrationRejected")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl<T: StatementNativeAttemptOwner + ?Sized> std::fmt::Debug
    for StatementNativeCompletionRejected<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementNativeCompletionRejected")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "statement_native_residual_tests.rs"]
mod tests;

impl<T: StatementNativeAttemptOwner + ?Sized> std::fmt::Debug
    for StatementNativeForegroundLease<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementNativeForegroundLease")
            .field("registration", &self.slot.identity)
            .finish_non_exhaustive()
    }
}
