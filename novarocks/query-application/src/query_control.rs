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

//! Query-application-owned session and statement cancellation state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::cancellation::{QueryCancellationReason, QueryCancellationSource};
use crate::client_connection::ClientConnectionToken;
use crate::session_control::{
    ConnectionKillAuthorization, GovernedStatementCancellation, GovernedStatementFinishOutcome,
    GovernedStatementRegistration, GovernedStatementVisibilitySealOutcome, QueryCancelOutcome,
    QueryControlError, QueryControlPort, QueryControlService, SessionIdentity, SessionProcess,
    SessionToken, StatementFinishOutcome, StatementRegistration, StatementToken,
};
use novarocks_workload_control::{
    CancellationReason, WorkCancellationRequestOutcome, WorkError, WorkSuccessSealOutcome,
};

// Design: ADR-0102 (docs/adr/ADR-0102-mysql-kill-connection-lifecycle-ownership.md)
#[derive(Default)]
pub struct QueryApplicationControl {
    state: Mutex<QueryControlState>,
}

#[derive(Default)]
struct QueryControlState {
    next_session_epoch: u64,
    statement_progress: std::sync::Weak<crate::session_control::StatementProgressSlot>,
    statement_actions_open: bool,
    statement_progress_installed: bool,
    sessions: BTreeMap<u32, SessionEntry>,
}

struct SessionEntry {
    session_epoch: u64,
    connection: ClientConnectionToken,
    principal: Arc<str>,
    next_statement_generation: u64,
    active: Option<ActiveStatement>,
    unregister_pending: bool,
    /// When this session registered, which is what an idle session's reported
    /// time is measured from.
    registered_at: Instant,
}

struct ActiveStatement {
    generation: u64,
    action: Option<Arc<crate::session_control::StatementActionSource>>,
    cancellation: ActiveStatementCancellation,
    success_visibility_sealed: bool,
    /// When this statement began, and the text it began with. Both exist only
    /// to be reported; nothing in admission or cancellation reads them.
    began_at: Instant,
    text: Option<Arc<str>>,
}

enum ActiveStatementCancellation {
    Legacy(QueryCancellationSource),
    Governed(GovernedStatementCancellation),
}

impl ActiveStatementCancellation {
    fn request(&self, reason: QueryCancellationReason) -> QueryCancelOutcome {
        match self {
            Self::Legacy(cancellation) => match cancellation.request(reason) {
                crate::cancellation::QueryCancellationRequestResult::Requested => {
                    QueryCancelOutcome::Requested
                }
                crate::cancellation::QueryCancellationRequestResult::AlreadyRequested(reason) => {
                    QueryCancelOutcome::AlreadyRequested(reason)
                }
            },
            Self::Governed(cancellation) => {
                let workload_reason = workload_cancellation_reason(&reason);
                match cancellation
                    .requester()
                    .request_with_outcome(workload_reason)
                {
                    Ok(WorkCancellationRequestOutcome::Requested) => {
                        cancellation.remember_query_reason(reason);
                        QueryCancelOutcome::Requested
                    }
                    Ok(WorkCancellationRequestOutcome::AlreadyRequested(existing)) => {
                        let first = cancellation.first_query_reason();
                        QueryCancelOutcome::AlreadyRequested(query_cancellation_reason(
                            existing,
                            first.as_ref().unwrap_or(&reason),
                        ))
                    }
                    Ok(WorkCancellationRequestOutcome::SuccessSealed) => {
                        QueryCancelOutcome::NoActiveStatement
                    }
                    Err(error) => QueryCancelOutcome::Failed(error),
                }
            }
        }
    }
}

fn workload_cancellation_reason(reason: &QueryCancellationReason) -> CancellationReason {
    match reason {
        QueryCancellationReason::ExecutionCancellationRequested => CancellationReason::Requested,
        QueryCancellationReason::ExecutionOwnerDropped => CancellationReason::OwnerDropped,
        QueryCancellationReason::ExplicitKill {
            requester_connection_id,
        } => CancellationReason::ExplicitKill {
            requester_connection_id: u64::from(*requester_connection_id),
        },
        QueryCancellationReason::ExplicitKillConnection {
            requester_connection_id,
        } => CancellationReason::ExplicitKillConnection {
            requester_connection_id: u64::from(*requester_connection_id),
        },
        QueryCancellationReason::ClientDisconnected => CancellationReason::ClientDisconnected,
        QueryCancellationReason::DeadlineExceeded { .. } => CancellationReason::DeadlineExceeded,
        QueryCancellationReason::FrontendDrainDeadlineExceeded { .. } => {
            CancellationReason::FrontendDrainDeadlineExceeded
        }
        QueryCancellationReason::ServerShutdown => CancellationReason::ServerShutdown,
    }
}

fn query_cancellation_reason(
    reason: CancellationReason,
    requested: &QueryCancellationReason,
) -> QueryCancellationReason {
    match reason {
        CancellationReason::ExplicitKill {
            requester_connection_id,
        } => QueryCancellationReason::ExplicitKill {
            requester_connection_id: u32::try_from(requester_connection_id).unwrap_or(u32::MAX),
        },
        CancellationReason::ExplicitKillConnection {
            requester_connection_id,
        } => QueryCancellationReason::ExplicitKillConnection {
            requester_connection_id: u32::try_from(requester_connection_id).unwrap_or(u32::MAX),
        },
        CancellationReason::ClientDisconnected => QueryCancellationReason::ClientDisconnected,
        CancellationReason::DeadlineExceeded => match requested {
            QueryCancellationReason::DeadlineExceeded { timeout_ms } => {
                QueryCancellationReason::DeadlineExceeded {
                    timeout_ms: *timeout_ms,
                }
            }
            _ => QueryCancellationReason::DeadlineExceeded { timeout_ms: 0 },
        },
        CancellationReason::FrontendDrainDeadlineExceeded => match requested {
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => {
                QueryCancellationReason::FrontendDrainDeadlineExceeded {
                    timeout_ms: *timeout_ms,
                }
            }
            _ => QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 0 },
        },
        CancellationReason::ServerShutdown => QueryCancellationReason::ServerShutdown,
        CancellationReason::Requested => QueryCancellationReason::ExecutionCancellationRequested,
        CancellationReason::OwnerDropped => QueryCancellationReason::ExecutionOwnerDropped,
    }
}

/// Actual existing session-registry values, including its inline action slot.
/// The caller supplies the real client/session bound and BTree allocator fee.
#[derive(Clone, Copy, Debug)]
pub struct QueryControlSessionGeometry {
    pub owner: std::alloc::Layout,
    pub record: std::alloc::Layout,
}

impl QueryApplicationControl {
    pub fn session_geometry() -> QueryControlSessionGeometry {
        QueryControlSessionGeometry {
            owner: std::alloc::Layout::new::<Self>(),
            record: std::alloc::Layout::new::<(u32, SessionEntry)>(),
        }
    }
    pub fn service() -> QueryControlService {
        QueryControlService::new(Arc::new(Self::default()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueryControlState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(novarocks_dev_controls)]
impl QueryApplicationControl {
    /// Reads this owner's current session; it neither registers nor keeps it alive.
    pub fn current_session_for_test_controls(
        &self,
        connection_id: u32,
    ) -> Result<SessionToken, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "statement input query-control lock poisoned".to_owned())?;
        let entry = state
            .sessions
            .get(&connection_id)
            .filter(|entry| !entry.unregister_pending)
            .ok_or_else(|| {
                "statement input requires an active registered client session".to_owned()
            })?;
        Ok(SessionToken::new(connection_id, entry.session_epoch))
    }
}

impl QueryControlPort for QueryApplicationControl {
    fn attach_statement_progress(
        &self,
        slot: &Arc<crate::session_control::StatementProgressSlot>,
    ) -> Result<(), QueryControlError> {
        let mut state = self.lock();
        if state.statement_progress_installed
            || state.sessions.values().any(|entry| {
                entry
                    .active
                    .as_ref()
                    .is_some_and(|active| active.action.is_some())
            })
        {
            return Err(QueryControlError::StatementBusy);
        }
        state.statement_progress = Arc::downgrade(slot);
        state.statement_actions_open = true;
        state.statement_progress_installed = true;
        Ok(())
    }
    fn statement_progress_slot(
        &self,
    ) -> Option<Arc<crate::session_control::StatementProgressSlot>> {
        self.lock().statement_progress.upgrade()
    }
    fn publish_statement_action_in(
        &self,
        owner: &mut Option<crate::session_control::GovernedQueryStatementOwner>,
        action: &mut Option<Box<dyn crate::session_control::GovernedStatementAction>>,
        source: &Arc<crate::session_control::StatementActionSource>,
    ) -> Result<(), QueryControlError> {
        let token = owner
            .as_ref()
            .ok_or(QueryControlError::StatementBusy)?
            .token();
        if action.is_none() {
            return Err(QueryControlError::StatementBusy);
        }
        let mut state = self.lock();
        if !state.statement_actions_open {
            return Err(QueryControlError::StatementBusy);
        }
        let entry = state
            .sessions
            .get_mut(&token.session().connection_id())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.session_epoch != token.session().session_epoch() {
            return Err(QueryControlError::StaleSession);
        }
        let active = entry
            .active
            .as_mut()
            .filter(|active| active.generation == token.generation())
            .ok_or(QueryControlError::StatementBusy)?;
        if active.action.is_some() || active.success_visibility_sealed {
            return Err(QueryControlError::StatementBusy);
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return Err(QueryControlError::StatementBusy);
        };
        source.install(owner, action, cancellation);
        active.action = Some(Arc::clone(source));
        Ok(())
    }
    fn next_statement_action(
        &self,
        after: Option<u32>,
    ) -> Option<(
        u32,
        Option<Arc<crate::session_control::StatementActionSource>>,
    )> {
        use std::ops::Bound::{Excluded, Unbounded};
        let state = self.lock();
        let (id, entry) = state
            .sessions
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .next()?;
        Some((
            *id,
            entry
                .active
                .as_ref()
                .and_then(|active| active.action.as_ref())
                .map(Arc::clone),
        ))
    }
    fn detach_statement_action(
        &self,
        token: StatementToken,
        source: &Arc<crate::session_control::StatementActionSource>,
    ) -> bool {
        let removed = {
            let mut state = self.lock();
            let Some(entry) = state
                .sessions
                .get_mut(&token.session().connection_id())
                .filter(|entry| entry.session_epoch == token.session().session_epoch())
            else {
                return false;
            };
            let Some(active) = entry
                .active
                .as_mut()
                .filter(|active| active.generation == token.generation())
            else {
                return false;
            };
            if !active
                .action
                .as_ref()
                .is_some_and(|original| Arc::ptr_eq(original, source))
            {
                return false;
            }
            active.action.take()
        };
        drop(removed);
        true
    }
    fn settle_statement_action_silent(
        &self,
        token: StatementToken,
        source: &Arc<crate::session_control::StatementActionSource>,
    ) -> bool {
        let retired = {
            let mut state = self.lock();
            let Some(entry) = state
                .sessions
                .get_mut(&token.session().connection_id())
                .filter(|entry| entry.session_epoch == token.session().session_epoch())
            else {
                return false;
            };
            let Some(active) = entry
                .active
                .as_ref()
                .filter(|active| active.generation == token.generation())
            else {
                return false;
            };
            if !active
                .action
                .as_ref()
                .is_some_and(|original| Arc::ptr_eq(original, source))
                || !source.is_settled()
            {
                return false;
            }
            let unregister_pending = entry.unregister_pending;
            let retired = entry.active.take();
            if unregister_pending {
                state.sessions.remove(&token.session().connection_id());
            }
            retired
        };
        // Source/cancellation remain held by the original caller. The metadata
        // record's actual Drop is also outside the QueryControl lock.
        drop(retired);
        true
    }
    fn close_statement_actions(&self) {
        let mut state = self.lock();
        state.statement_actions_open = false;
        for entry in state.sessions.values() {
            if let Some(source) = entry
                .active
                .as_ref()
                .and_then(|active| active.action.as_ref())
            {
                source.stop();
            }
        }
    }
    fn statement_action_failure(
        &self,
    ) -> Option<Arc<crate::session_control::StatementActionFailure>> {
        self.lock()
            .sessions
            .values()
            .filter_map(|entry| {
                entry
                    .active
                    .as_ref()
                    .and_then(|active| active.action.as_ref())
            })
            .find_map(|source| source.failure())
    }
    fn has_statement_actions(&self) -> bool {
        self.lock().sessions.values().any(|entry| {
            entry
                .active
                .as_ref()
                .is_some_and(|active| active.action.is_some())
        })
    }

    fn register_session(
        &self,
        identity: SessionIdentity,
    ) -> Result<SessionToken, QueryControlError> {
        let mut state = self.lock();
        if state.sessions.contains_key(&identity.connection_id()) {
            return Err(QueryControlError::ConnectionIdInUse);
        }
        state.next_session_epoch = state.next_session_epoch.wrapping_add(1);
        if state.next_session_epoch == 0 {
            state.next_session_epoch = 1;
        }
        let token = SessionToken::new(identity.connection_id(), state.next_session_epoch);
        state.sessions.insert(
            token.connection_id(),
            SessionEntry {
                session_epoch: token.session_epoch(),
                connection: identity.connection_token(),
                principal: Arc::from(identity.principal()),
                next_statement_generation: 0,
                active: None,
                unregister_pending: false,
                registered_at: Instant::now(),
            },
        );
        Ok(token)
    }

    fn unregister_session(&self, token: SessionToken) {
        let mut state = self.lock();
        if state
            .sessions
            .get(&token.connection_id())
            .is_some_and(|entry| entry.session_epoch == token.session_epoch())
        {
            let entry = state.sessions.get_mut(&token.connection_id()).unwrap();
            if let Some(active) = entry.active.as_ref() {
                entry.unregister_pending = true;
                if !active.success_visibility_sealed
                    && let QueryCancelOutcome::Failed(error) = active
                        .cancellation
                        .request(QueryCancellationReason::ClientDisconnected)
                {
                    tracing::error!(
                        connection_id = token.connection_id(),
                        session_epoch = token.session_epoch(),
                        error = %error,
                        "retain query-control session after workload cancellation failed"
                    );
                }
                return;
            }
            state.sessions.remove(&token.connection_id());
        }
    }

    fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError> {
        self.begin_statement_with_cancellation(session, QueryCancellationSource::new())
    }

    fn begin_statement_with_cancellation(
        &self,
        session: SessionToken,
        cancellation: QueryCancellationSource,
    ) -> Result<StatementRegistration, QueryControlError> {
        let mut state = self.lock();
        let entry = state
            .sessions
            .get_mut(&session.connection_id())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.session_epoch != session.session_epoch() {
            return Err(QueryControlError::StaleSession);
        }
        if entry.unregister_pending {
            return Err(QueryControlError::UnknownSession);
        }
        if entry.active.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        entry.next_statement_generation = entry.next_statement_generation.wrapping_add(1);
        if entry.next_statement_generation == 0 {
            entry.next_statement_generation = 1;
        }
        let registration = StatementRegistration::new(
            StatementToken::new(session, entry.next_statement_generation),
            cancellation.view(),
        );
        entry.active = Some(ActiveStatement {
            action: None,
            generation: entry.next_statement_generation,
            began_at: Instant::now(),
            text: None,
            cancellation: ActiveStatementCancellation::Legacy(cancellation),
            success_visibility_sealed: false,
        });
        Ok(registration)
    }

    fn begin_statement_with_governed_cancellation(
        &self,
        session: SessionToken,
        cancellation: GovernedStatementCancellation,
        statement_text: Option<Arc<str>>,
    ) -> Result<GovernedStatementRegistration, QueryControlError> {
        let mut state = self.lock();
        let entry = state
            .sessions
            .get_mut(&session.connection_id())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.session_epoch != session.session_epoch() {
            return Err(QueryControlError::StaleSession);
        }
        if entry.unregister_pending {
            return Err(QueryControlError::UnknownSession);
        }
        if entry.active.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        entry.next_statement_generation = entry.next_statement_generation.wrapping_add(1);
        if entry.next_statement_generation == 0 {
            entry.next_statement_generation = 1;
        }
        let registration = GovernedStatementRegistration::new(
            StatementToken::new(session, entry.next_statement_generation),
            cancellation.view().clone(),
        );
        entry.active = Some(ActiveStatement {
            action: None,
            generation: entry.next_statement_generation,
            began_at: Instant::now(),
            text: statement_text,
            cancellation: ActiveStatementCancellation::Governed(cancellation),
            success_visibility_sealed: false,
        });
        Ok(registration)
    }

    fn list_processes(&self) -> Vec<SessionProcess> {
        let now = Instant::now();
        let state = self.lock();
        // `sessions` is keyed by connection id, so iteration order is already
        // the order a reader expects.
        state
            .sessions
            .values()
            .map(|entry| {
                let (since, statement) = entry
                    .active
                    .as_ref()
                    .map_or((entry.registered_at, None), |active| {
                        (active.began_at, active.text.clone())
                    });
                SessionProcess {
                    connection_id: entry.connection.connection_id(),
                    principal: Arc::clone(&entry.principal),
                    elapsed: now.saturating_duration_since(since),
                    statement,
                }
            })
            .collect()
    }

    fn finish_statement(&self, statement: StatementToken) -> StatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return StatementFinishOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return StatementFinishOutcome::Stale;
        }
        let Some(active) = entry.active.as_ref() else {
            return StatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return StatementFinishOutcome::Stale;
        }
        let ActiveStatementCancellation::Legacy(cancellation) = &active.cancellation else {
            return StatementFinishOutcome::Stale;
        };
        let reason = if active.success_visibility_sealed {
            None
        } else {
            cancellation.view().reason()
        };
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        let outcome = match reason {
            Some(reason) => StatementFinishOutcome::Cancelled(reason),
            None => StatementFinishOutcome::Completed,
        };
        if unregister_pending {
            state.sessions.remove(&statement.session().connection_id());
        }
        outcome
    }

    fn seal_governed_statement_visibility(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementVisibilitySealOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        let Some(active) = entry.active.as_mut() else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        if active.action.is_some() {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if active.success_visibility_sealed {
            return GovernedStatementVisibilitySealOutcome::Sealed;
        }
        match cancellation.success_sealer().seal() {
            Ok(WorkSuccessSealOutcome::Sealed | WorkSuccessSealOutcome::AlreadySealed) => {
                active.success_visibility_sealed = true;
                GovernedStatementVisibilitySealOutcome::Sealed
            }
            Ok(WorkSuccessSealOutcome::Cancelled(reason)) => {
                GovernedStatementVisibilitySealOutcome::Cancelled(reason)
            }
            Err(_) => GovernedStatementVisibilitySealOutcome::Stale,
        }
    }

    fn finish_governed_statement(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let Some(active) = entry.active.as_ref() else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return GovernedStatementFinishOutcome::Stale;
        }
        if active.action.is_some() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return GovernedStatementFinishOutcome::Stale;
        };
        let reason = if active.success_visibility_sealed {
            None
        } else {
            cancellation.view().reason()
        };
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        let outcome = match reason {
            Some(reason) => GovernedStatementFinishOutcome::Cancelled(reason),
            None => GovernedStatementFinishOutcome::Completed,
        };
        if unregister_pending {
            state.sessions.remove(&statement.session().connection_id());
        }
        outcome
    }

    fn fail_governed_statement(&self, statement: StatementToken) -> GovernedStatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let Some(active) = entry.active.as_ref() else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation()
            || !matches!(
                active.cancellation,
                ActiveStatementCancellation::Governed(_)
            )
        {
            return GovernedStatementFinishOutcome::Stale;
        }
        if active.action.is_some() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        if unregister_pending {
            state.sessions.remove(&statement.session().connection_id());
        }
        GovernedStatementFinishOutcome::ProtocolFailed
    }

    fn cancel_governed_statement(
        &self,
        statement: StatementToken,
        reason: CancellationReason,
    ) -> Result<Option<WorkCancellationRequestOutcome>, WorkError> {
        let state = self.lock();
        let Some(entry) = state.sessions.get(&statement.session().connection_id()) else {
            return Ok(None);
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return Ok(None);
        }
        let Some(active) = entry.active.as_ref() else {
            return Ok(None);
        };
        if active.generation != statement.generation() {
            return Ok(None);
        }
        if active.success_visibility_sealed {
            return Ok(None);
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return Ok(None);
        };
        cancellation
            .requester()
            .request_with_outcome(reason)
            .map(Some)
    }

    fn cancel_session_statement(
        &self,
        session: SessionToken,
        reason: QueryCancellationReason,
    ) -> QueryCancelOutcome {
        let state = self.lock();
        let Some(entry) = state.sessions.get(&session.connection_id()) else {
            return QueryCancelOutcome::UnknownSession;
        };
        if entry.session_epoch != session.session_epoch() {
            return QueryCancelOutcome::UnknownSession;
        }
        let Some(active) = entry.active.as_ref() else {
            return QueryCancelOutcome::NoActiveStatement;
        };
        if active.success_visibility_sealed {
            return QueryCancelOutcome::NoActiveStatement;
        }
        active.cancellation.request(reason)
    }

    fn kill_query(&self, requester: SessionToken, target_connection_id: u32) -> QueryCancelOutcome {
        let state = self.lock();
        let Some(requester_entry) = state.sessions.get(&requester.connection_id()) else {
            return QueryCancelOutcome::UnknownSession;
        };
        if requester_entry.session_epoch != requester.session_epoch() {
            return QueryCancelOutcome::UnknownSession;
        }
        let requester_principal = Arc::clone(&requester_entry.principal);
        let Some(target_entry) = state.sessions.get(&target_connection_id) else {
            return QueryCancelOutcome::UnknownSession;
        };
        if target_entry.principal != requester_principal {
            return QueryCancelOutcome::PermissionDenied;
        }
        let Some(active) = target_entry.active.as_ref() else {
            return QueryCancelOutcome::NoActiveStatement;
        };
        if active.success_visibility_sealed {
            return QueryCancelOutcome::NoActiveStatement;
        }
        active
            .cancellation
            .request(QueryCancellationReason::ExplicitKill {
                requester_connection_id: requester.connection_id(),
            })
    }

    fn authorize_connection_kill(
        &self,
        requester: SessionToken,
        target_connection_id: u32,
    ) -> ConnectionKillAuthorization {
        let state = self.lock();
        let Some(requester_entry) = state.sessions.get(&requester.connection_id()) else {
            return ConnectionKillAuthorization::UnknownSession;
        };
        if requester_entry.session_epoch != requester.session_epoch() {
            return ConnectionKillAuthorization::UnknownSession;
        }
        let Some(target_entry) = state.sessions.get(&target_connection_id) else {
            return ConnectionKillAuthorization::UnknownSession;
        };
        if target_entry.principal != requester_entry.principal {
            return ConnectionKillAuthorization::PermissionDenied;
        }
        ConnectionKillAuthorization::Authorized(target_entry.connection)
    }

    fn cancel_all(&self, reason: QueryCancellationReason) {
        let state = self.lock();
        for entry in state.sessions.values() {
            if let Some(active) = entry.active.as_ref() {
                if active.success_visibility_sealed {
                    continue;
                }
                if let QueryCancelOutcome::Failed(error) =
                    active.cancellation.request(reason.clone())
                {
                    tracing::error!(
                        error = %error,
                        "failed to request governed statement cancellation during cancel-all"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_control::{GovernedQueryStatementBeginError, QueryControlPort};
    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    fn register(
        control: &QueryApplicationControl,
        id: u32,
        generation: u64,
        principal: &str,
    ) -> SessionToken {
        control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(id, generation).expect("valid connection token"),
                principal,
            ))
            .expect("register session")
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn session_observation_tracks_the_service_owner_without_retaining_sessions() {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        assert!(control.current_session_for_test_controls(7).is_err());

        let first = service
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(7, 1).expect("valid connection token"),
                "root",
            ))
            .expect("register first session");
        let first_token = first.token();
        assert_eq!(
            control.current_session_for_test_controls(7).unwrap(),
            first_token
        );
        drop(first);
        assert!(control.current_session_for_test_controls(7).is_err());

        let second = service
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(7, 2).expect("valid connection token"),
                "root",
            ))
            .expect("register replacement session");
        assert_ne!(second.token(), first_token);
        assert_eq!(
            control.current_session_for_test_controls(7).unwrap(),
            second.token()
        );
    }

    #[test]
    fn stale_session_and_statement_cannot_remove_successor() {
        let control = QueryApplicationControl::default();
        let first = register(&control, 7, 1, "root");
        let old = control.begin_statement(first).expect("begin old");
        control.unregister_session(first);
        assert!(matches!(
            control.register_session(SessionIdentity::new(
                ClientConnectionToken::new(7, 2).expect("valid connection token"),
                "root",
            )),
            Err(QueryControlError::ConnectionIdInUse)
        ));
        assert_eq!(
            control.finish_statement(old.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ClientDisconnected)
        );
        let second = register(&control, 7, 2, "root");
        let current = control.begin_statement(second).expect("begin current");
        assert_eq!(
            control.finish_statement(old.token()),
            StatementFinishOutcome::Stale
        );
        assert_eq!(
            control.cancel_session_statement(second, QueryCancellationReason::ClientDisconnected),
            QueryCancelOutcome::Requested
        );
        assert!(current.cancellation().is_cancelled());
    }

    #[test]
    fn repeated_kill_preserves_first_reason() {
        let control = QueryApplicationControl::default();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let active = control.begin_statement(target).expect("begin target");
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8,
            })
        );
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8,
            })
        );
    }

    #[test]
    fn permission_and_idle_do_not_change_target() {
        let control = QueryApplicationControl::default();
        let target = register(&control, 7, 1, "root");
        let foreign = register(&control, 8, 1, "other");
        let own = register(&control, 9, 1, "root");
        assert_eq!(
            control.kill_query(own, 99),
            QueryCancelOutcome::UnknownSession
        );
        assert_eq!(
            control.kill_query(own, 7),
            QueryCancelOutcome::NoActiveStatement
        );
        let active = control.begin_statement(target).expect("begin target");
        assert_eq!(
            control.kill_query(foreign, 7),
            QueryCancelOutcome::PermissionDenied
        );
        assert!(!active.cancellation().is_cancelled());
    }

    #[test]
    fn finish_and_cancel_are_linearized_and_busy_session_rejects_successor() {
        let control = QueryApplicationControl::default();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let active = control.begin_statement(target).expect("begin target");

        assert!(matches!(
            control.begin_statement(target),
            Err(QueryControlError::StatementBusy)
        ));
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8,
            })
        );
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::NoActiveStatement
        );
        assert!(
            control.begin_statement(target).is_ok(),
            "only the matching active generation is released"
        );
    }

    #[test]
    fn caller_owned_cancellation_source_controls_the_registered_statement() {
        let control = QueryApplicationControl::default();
        let session = register(&control, 7, 1, "root");
        let source = QueryCancellationSource::new();
        let active = control
            .begin_statement_with_cancellation(session, source.clone())
            .expect("begin statement with lifecycle source");

        assert_eq!(
            source.request(QueryCancellationReason::FrontendDrainDeadlineExceeded {
                timeout_ms: 300_000,
            }),
            crate::cancellation::QueryCancellationRequestResult::Requested
        );
        assert!(active.cancellation().is_cancelled());
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(
                QueryCancellationReason::FrontendDrainDeadlineExceeded {
                    timeout_ms: 300_000,
                }
            )
        );
    }

    #[test]
    fn connection_kill_authorization_returns_only_the_exact_target_token() {
        let control = QueryApplicationControl::default();
        let target = register(&control, 7, 3, "root");
        let requester = register(&control, 8, 4, "root");
        let foreign = register(&control, 9, 5, "other");

        assert_eq!(
            control.authorize_connection_kill(requester, 7),
            ConnectionKillAuthorization::Authorized(
                ClientConnectionToken::new(7, 3).expect("valid token")
            )
        );
        assert_eq!(
            control.authorize_connection_kill(requester, 99),
            ConnectionKillAuthorization::UnknownSession
        );
        assert_eq!(
            control.authorize_connection_kill(foreign, 7),
            ConnectionKillAuthorization::PermissionDenied
        );

        control.unregister_session(target);
        let successor = register(&control, 7, 6, "root");
        assert_eq!(
            control.authorize_connection_kill(requester, 7),
            ConnectionKillAuthorization::Authorized(
                ClientConnectionToken::new(7, 6).expect("valid token")
            )
        );
        control.unregister_session(successor);
    }

    fn governed_control() -> (
        Arc<QueryApplicationControl>,
        QueryControlService,
        WorkloadControl,
    ) {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload authority");
        workload.mark_ready().expect("workload authority ready");
        (control, service, workload)
    }

    #[test]
    fn nonqueued_result_admission_rolls_back_registration_failure_and_retains_aliases() {
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass, WorkClass};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1, 2, 1, 1],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let session = register(&control, 7, 1, "root");
        let other = register(&control, 8, 2, "root");
        let begin = |session| {
            service.begin_governed_statement_with_result(
                session,
                &workload.root_admission(),
                WorkClass::Management,
                None,
                None,
                None,
                ResultWindowClass::Local,
            )
        };
        let statement = begin(session).unwrap();
        assert!(matches!(
            begin(session),
            Err(GovernedQueryStatementBeginError::QueryControl(_))
        ));
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let second = begin(other).unwrap();
        let third = register(&control, 9, 3, "root");
        assert!(matches!(
            begin(third),
            Err(GovernedQueryStatementBeginError::Admission(_))
        ));
        assert_eq!(workload.snapshot().root_responsibilities, 2);
        assert_eq!(workload.snapshot().businesses, 2);
        assert_eq!(workload.snapshot().admitted_queries, 0);
        let alias = statement.result_window_alias().unwrap();
        drop(statement);
        drop(second);
        assert_eq!(workload.snapshot().businesses, 0);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        drop(alias);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        // The failed capacity admission did not start a protocol generation.
        drop(begin(third).unwrap());
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn result_statement_takes_its_window_with_the_permit_and_keeps_it_for_aliases() {
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1; 4],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_queued_governed_query_statement_with_result(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Client,
            )
            .await
            .unwrap();
        assert_eq!(workload.snapshot().admitted_queries, 1);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        let alias = statement
            .result_window_alias()
            .expect("a result statement owns its window");
        assert_eq!(alias.class(), ResultWindowClass::Client);
        // An accepted cancel cut returns only the computation permit.
        assert_eq!(
            control.kill_query(session, 7),
            QueryCancelOutcome::Requested
        );
        assert!(statement.accept_cancel_delivery_cut().unwrap());
        assert_eq!(workload.snapshot().admitted_queries, 0);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        // The owner's exit does not free the position while an alias lives.
        drop(statement);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        drop(alias);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        // A plain statement takes no window.
        let plain = service
            .begin_queued_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(plain.result_window_alias().is_none());
    }

    #[tokio::test]
    async fn streaming_closing_transfer_releases_ordinary_window_after_last_alias_exit() {
        use crate::api::{
            ExecutionHandle, ExecutionOutput, QueryResultStream, ResultField, ResultRowCarrier,
            ResultSchema,
        };
        use crate::protocol_delivery::StreamingStatementResult;
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1; 4],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_queued_governed_query_statement_with_result(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Client,
            )
            .await
            .unwrap();
        let token = statement.token();
        let native_alias = statement.result_window_alias().unwrap();
        let owner = statement.take_execution_owner().unwrap();
        let (_transport, _receipt, _failure, stream) = QueryResultStream::try_channel(
            novarocks_types::identity::QueryId::new(71, 1),
            ResultSchema::new(vec![ResultField::new(
                "value",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int32,
                    false,
                ),
            )]),
            ResultRowCarrier::relayed(
                novarocks_result_contract::RootOutputKind::ClientRows,
                Some(
                    novarocks_result_contract::ClientRowProfile::try_new(
                        novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
                        novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
            1,
        )
        .unwrap();
        let execution = ExecutionHandle::new(
            owner.cancellation_requester(),
            ExecutionOutput::Rows(stream),
        );
        let mut result =
            StreamingStatementResult::try_from_execution(execution, statement).unwrap();
        assert_eq!(result.statement_token(), Some(token));
        assert!(matches!(
            service.begin_statement(session),
            Err(QueryControlError::StatementBusy)
        ));
        control.kill_query(session, 7);
        let grant = result.try_closing_capacity(true).unwrap();
        assert_eq!(result.statement_token(), Some(token));
        assert_eq!(workload.snapshot().admitted_queries, 0);
        let closing = result
            .into_closing_delivery((), grant, 1024)
            .unwrap_or_else(|_| panic!("valid transfer"));
        assert_eq!(
            capacity.snapshot().held_positions[0],
            1,
            "Native short tail retains the original window"
        );
        drop(native_alias);
        assert_eq!(
            capacity.snapshot().held_positions[0],
            0,
            "closing owns no ordinary window after actual alias exit"
        );
        assert_eq!(capacity.snapshot().held_positions[3], 1);
        assert!(matches!(
            service.begin_statement(session),
            Err(QueryControlError::StatementBusy)
        ));
        owner.complete();
        assert!(matches!(
            closing.settle_after_writer_exit().await,
            GovernedStatementFinishOutcome::Cancelled(_)
        ));
        assert_eq!(capacity.snapshot().held_positions[3], 0);
        assert!(service.begin_statement(session).is_ok());
    }

    #[tokio::test]
    async fn delivery_cut_returns_compute_only_and_closing_retains_generation_and_aliases() {
        use crate::protocol_delivery::{ClosingDelivery, GovernedProtocolOwner};
        use novarocks_workload_control::{ResultCapacityConfig, ResultClosingCut};
        for abandon_wait in [false, true] {
            let control = Arc::new(QueryApplicationControl::default());
            let service = QueryControlService::new(control.clone());
            let workload = WorkloadControl::try_new(
                WorkloadConfig {
                    query_concurrency_limit: 1,
                    ..WorkloadConfig::default()
                },
                ResourceConfig {
                    total_bytes: 1024,
                    control_bytes: 128,
                    per_scope_bytes: 896,
                },
            )
            .unwrap();
            let capacity = workload
                .configure_result_capacity(ResultCapacityConfig {
                    positions: [1; 4],
                    client_compute_positions: 1,
                    client_short_tail_positions: 0,
                    supported_cancel_burst: 0,
                    sustained_cancels_per_second: 0,
                    ..ResultCapacityConfig::V1
                })
                .unwrap();
            workload.mark_ready().unwrap();
            let session = register(&control, 7, 1, "root");
            let mut statement = service
                .begin_queued_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let token = statement.token();
            let scope = statement.scope().clone();
            assert_eq!(workload.snapshot().admitted_queries, 1);
            assert!(statement.accept_cancel_delivery_cut().is_err());
            assert_eq!(
                control.kill_query(session, 7),
                QueryCancelOutcome::Requested
            );
            assert!(statement.accept_cancel_delivery_cut().unwrap());
            assert!(!statement.accept_cancel_delivery_cut().unwrap());
            assert!(!statement.accept_failed_delivery_cut().unwrap());
            assert_eq!(workload.snapshot().admitted_queries, 0);
            assert_eq!(statement.token(), token);
            assert!(matches!(
                service.begin_statement(session),
                Err(QueryControlError::StatementBusy)
            ));
            let grant = capacity
                .try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation)
                .unwrap();
            let protocol = GovernedProtocolOwner::new(statement);
            let closing = ClosingDelivery::try_new((), protocol, grant, 1024)
                .unwrap_or_else(|_| panic!("accepted closing cut"));
            let alias = closing.retained_guard();
            let mut settlement = Box::pin(closing.settle_after_writer_exit());
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    std::future::Future::poll(settlement.as_mut(), cx).is_pending()
                ))
                .await
            );
            assert!(
                matches!(
                    service.begin_statement(session),
                    Err(QueryControlError::StatementBusy)
                ),
                "a still-live writer alias must retain the original statement generation"
            );
            assert_eq!(capacity.snapshot().held_positions[3], 1);
            if abandon_wait {
                drop(settlement);
                assert!(
                    matches!(
                        service.begin_statement(session),
                        Err(QueryControlError::StatementBusy)
                    ),
                    "dropping the closing waiter cannot settle a still-live writer alias"
                );
                drop(alias);
            } else {
                drop(alias);
                assert!(matches!(
                    settlement.await,
                    GovernedStatementFinishOutcome::Cancelled(_)
                ));
            }
            assert_eq!(
                capacity.snapshot().held_positions[3],
                0,
                "settlement follows the last physical alias exit"
            );
            let successor = service.begin_statement(session).unwrap();
            assert_ne!(successor.token(), token);
        }
    }

    #[test]
    fn failed_delivery_cut_retains_business_and_does_not_fabricate_cancel() {
        for previously_sealed in [false, true] {
            for finish_path in 0..3 {
                let (control, service, workload) = governed_control();
                let session = register(&control, 7, 1, "root");
                let mut statement = service
                    .begin_governed_query_statement(
                        session,
                        &workload.root_admission(),
                        None,
                        None,
                        None,
                    )
                    .unwrap();
                if previously_sealed {
                    assert_eq!(
                        statement.seal_success_visibility(),
                        GovernedStatementVisibilitySealOutcome::Sealed
                    );
                }
                assert_eq!(workload.snapshot().businesses, 1);
                // This entry owns a business permit rather than a compute
                // permit. The failure cut retains that business owner.
                assert!(!statement.accept_failed_delivery_cut().unwrap());
                assert_eq!(workload.snapshot().businesses, 1);
                assert!(statement.cancellation().reason().is_none());
                assert_eq!(
                    statement.seal_success_visibility(),
                    GovernedStatementVisibilitySealOutcome::Failed
                );
                assert!(matches!(
                    service.begin_statement(session),
                    Err(QueryControlError::StatementBusy)
                ));
                let outcome = match finish_path {
                    0 => statement.finish(),
                    1 => statement.finish_unstarted_read_after_cancellation(),
                    _ => statement.protocol_fail(),
                };
                assert_eq!(outcome, GovernedStatementFinishOutcome::ProtocolFailed);
                assert_eq!(workload.snapshot().businesses, 0);
            }
        }
    }

    #[tokio::test]
    async fn queued_governed_query_registers_kill_before_its_concurrency_grant() {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let config = WorkloadConfig {
            query_concurrency_limit: 1,
            ..WorkloadConfig::default()
        };
        let workload = WorkloadControl::try_new(
            config,
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload authority");
        workload.mark_ready().expect("workload authority ready");
        let first_session = register(&control, 7, 1, "root");
        let second_session = register(&control, 8, 1, "root");

        let mut first = service
            .begin_queued_governed_query_statement(
                first_session,
                &workload.root_admission(),
                None,
                None,
                None,
            )
            .await
            .expect("first query grant");
        let query_admission = workload.root_admission();
        let mut second = Box::pin(service.begin_queued_governed_query_statement(
            second_session,
            &query_admission,
            None,
            None,
            None,
        ));
        assert!(matches!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert_eq!(workload.snapshot().admitted_queries, 1);
        assert_eq!(workload.snapshot().admission_records, 1);

        assert_eq!(
            control.cancel_session_statement(
                second_session,
                QueryCancellationReason::ExplicitKill {
                    requester_connection_id: 7,
                },
            ),
            QueryCancelOutcome::Requested
        );
        assert!(matches!(
            second.await,
            Err(GovernedQueryStatementBeginError::Admission(
                WorkError::Cancelled(CancellationReason::ExplicitKill { .. })
            ))
        ));
        assert_eq!(workload.snapshot().admitted_queries, 1);
        assert_eq!(workload.snapshot().admission_records, 0);

        first
            .take_execution_owner()
            .expect("first execution owner")
            .complete();
        assert_eq!(first.finish(), GovernedStatementFinishOutcome::Completed);
        assert_eq!(workload.snapshot().admitted_queries, 0);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn governed_kill_requests_the_registered_work_scope() {
        let (control, service, workload) = governed_control();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let owner = statement
            .take_execution_owner()
            .expect("root owner transfers once");

        assert_eq!(
            control.kill_query(requester, target.connection_id()),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::ExplicitKill {
                requester_connection_id: u64::from(requester.connection_id()),
            })
        );
        owner.complete();
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::ExplicitKill {
                requester_connection_id: u64::from(requester.connection_id()),
            })
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_late_kill_reports_the_existing_owner_drop_reason() {
        let (control, service, workload) = governed_control();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None, None)
            .expect("governed query statement");

        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ExecutionOwnerDropped,),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.kill_query(requester, target.connection_id()),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::ExecutionOwnerDropped,)
        );
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::OwnerDropped)
        );
    }

    #[test]
    fn governed_disconnect_and_timeout_use_the_same_workload_authority() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let owner = statement
            .take_execution_owner()
            .expect("root owner transfers once");

        assert_eq!(
            control.cancel_session_statement(
                session,
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
            ),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.cancel_session_statement(session, QueryCancellationReason::ClientDisconnected,),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::DeadlineExceeded {
                timeout_ms: 50,
            })
        );
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::DeadlineExceeded)
        );
        owner.complete();
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );

        let session_lease = service
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(9, 1).expect("valid connection token"),
                "root",
            ))
            .expect("register second session");
        let session = session_lease.token();
        let statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("second governed query statement");
        drop(session_lease);
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::ClientDisconnected)
        );
    }

    #[test]
    fn cancelled_unstarted_read_settles_its_known_control() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");

        assert_eq!(
            control.cancel_session_statement(
                session,
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
            ),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            statement.finish_unstarted_read_after_cancellation(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert!(
            workload.next_control().is_none(),
            "an unstarted read has no logical-execution owner left to acknowledge cancellation"
        );
    }

    #[test]
    fn cancelled_local_query_terminals_do_not_leave_ready_control() {
        for terminal in 0..3 {
            let (control, service, workload) = governed_control();
            let session = register(&control, 7, 1, "root");
            let statement = service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .expect("governed query statement");
            assert_eq!(
                control.cancel_session_statement(
                    session,
                    QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
                ),
                QueryCancelOutcome::Requested
            );

            match terminal {
                0 => {
                    assert_eq!(
                        statement.finish(),
                        GovernedStatementFinishOutcome::Cancelled(
                            CancellationReason::DeadlineExceeded
                        )
                    );
                }
                1 => {
                    assert_eq!(
                        statement.protocol_fail(),
                        GovernedStatementFinishOutcome::ProtocolFailed
                    );
                }
                _ => drop(statement),
            }
            assert_eq!(workload.snapshot().root_responsibilities, 0);
            assert!(workload.next_control().is_none());
        }
    }

    #[test]
    fn cancelled_synchronous_execution_settles_control_before_protocol_finish() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        assert_eq!(
            control.cancel_session_statement(
                session,
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
            ),
            QueryCancelOutcome::Requested
        );

        statement.complete_execution();
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert!(workload.next_control().is_none());
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn governed_generation_and_business_permit_span_execution_start_to_protocol_finish() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut first = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("first governed query statement");
        let first_generation = first.token().generation();
        let owner = first
            .take_execution_owner()
            .expect("execution start consumes the root owner");
        owner.complete();

        assert_eq!(workload.snapshot().businesses, 1);
        assert!(matches!(
            service.begin_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None
            ),
            Err(GovernedQueryStatementBeginError::QueryControl(
                QueryControlError::StatementBusy
            ))
        ));
        assert_eq!(
            workload.snapshot().businesses,
            1,
            "rejected successor admission rolls back its root and business permit"
        );

        assert_eq!(first.finish(), GovernedStatementFinishOutcome::Completed);
        assert_eq!(workload.snapshot().businesses, 0);
        let second = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("protocol completion releases the next generation");
        assert!(second.token().generation() > first_generation);
    }

    #[test]
    fn governed_success_seal_wins_against_late_kill_until_protocol_finish() {
        let (control, service, workload) = governed_control();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(
            control.kill_query(requester, target.connection_id()),
            QueryCancelOutcome::NoActiveStatement
        );
        assert_eq!(statement.cancellation().reason(), None);
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn process_list_separates_an_idle_session_from_a_running_statement() {
        let (control, service, workload) = governed_control();
        let idle = register(&control, 7, 1, "root");
        let busy = register(&control, 9, 1, "reader");
        let _ = idle;
        let mut statement = service
            .begin_governed_query_statement(
                busy,
                &workload.root_admission(),
                None,
                None,
                Some(Arc::from("SELECT 1")),
            )
            .expect("governed query statement");

        let processes = control.list_processes();
        assert_eq!(
            processes
                .iter()
                .map(|process| process.connection_id)
                .collect::<Vec<_>>(),
            vec![7, 9],
            "sessions are reported in connection-id order"
        );
        assert_eq!(processes[0].command(), "Sleep");
        assert_eq!(processes[0].statement, None);
        assert_eq!(processes[0].principal.as_ref(), "root");
        assert_eq!(processes[1].command(), "Query");
        assert_eq!(
            processes[1].statement.as_deref(),
            Some("SELECT 1"),
            "a running statement reports the text it began with"
        );

        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Completed
        );

        let processes = control.list_processes();
        assert_eq!(processes[1].command(), "Sleep");
        assert_eq!(
            processes[1].statement, None,
            "a finished statement stops being reported as running"
        );
    }

    #[test]
    fn governed_eof_failure_after_success_seal_is_protocol_failed() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_internal_protocol_failure_does_not_fabricate_cancellation() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let cancellation = statement.cancellation().clone();
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(cancellation.reason(), None);
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn unregister_is_deferred_until_the_active_generation_settles() {
        let (control, service, workload) = governed_control();
        let connection = ClientConnectionToken::new(17, 1).expect("connection token");
        let session = service
            .register_session(SessionIdentity::new(connection, "root"))
            .expect("register session");
        let token = session.token();
        let statement = service
            .begin_governed_query_statement(token, &workload.root_admission(), None, None, None)
            .expect("begin governed statement");

        drop(session);
        assert!(
            matches!(
                service.register_session(SessionIdentity::new(connection, "root")),
                Err(QueryControlError::ConnectionIdInUse)
            ),
            "the registry retains the deferred unregister owner"
        );
        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        let successor = service
            .register_session(SessionIdentity::new(connection, "root"))
            .expect("settlement completes deferred unregister");
        assert_ne!(successor.token().session_epoch(), token.session_epoch());
        drop(successor);
        assert!(
            !control
                .lock()
                .sessions
                .contains_key(&connection.connection_id())
        );
    }

    #[tokio::test]
    async fn governed_root_deadline_is_observed_without_a_legacy_cancellation_source() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(10);
        let statement = service
            .begin_governed_query_statement(
                session,
                &workload.root_admission(),
                Some(deadline),
                Some(1),
                None,
            )
            .expect("deadline-bound governed query statement");

        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                statement.cancellation().cancelled(),
            )
            .await
            .expect("deadline cancellation arrives"),
            CancellationReason::DeadlineExceeded
        );
    }

    #[test]
    fn dropping_protocol_owner_after_execution_handoff_cancels_the_same_root() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let scope = statement.scope().clone();
        let owner = statement
            .take_execution_owner()
            .expect("execution owner transfers once");

        drop(statement);

        assert_eq!(
            scope.cancellation().expect("root remains owned").reason(),
            Some(CancellationReason::OwnerDropped)
        );
        assert_eq!(workload.snapshot().businesses, 0);
        owner.complete();
    }
    struct OriginalStatementGate {
        gate: futures::channel::oneshot::Receiver<()>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl crate::session_control::GovernedStatementAction for OriginalStatementGate {
        fn poll_action(
            &mut self,
            _owner: &mut crate::session_control::GovernedQueryStatementOwner,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<
            Result<
                crate::session_control::StatementActionValue,
                crate::session_error::QueryServiceError,
            >,
        > {
            use std::future::Future;
            std::pin::Pin::new(&mut self.gate).poll(cx).map(|ready| {
                ready.expect("original release sender");
                Ok(crate::session_control::StatementActionValue::Ok)
            })
        }
    }
    impl Drop for OriginalStatementGate {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    fn drive_original_statement_actions(slot: &crate::session_control::StatementProgressSlot) {
        let mut cursor = crate::session_control::StatementProgressCursor::default();
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        // The actual finite scanner is used; no mirror action state machine.
        assert!(
            slot.poll_turn(
                &mut cursor,
                std::num::NonZeroUsize::new(7).unwrap(),
                &mut cx
            )
            .is_ready()
        );
    }
    #[tokio::test]
    async fn original_statement_source_keeps_owner_until_actual_action_drop_and_protocol_take() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone()).unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let token = owner.as_ref().unwrap().token();
        let (release, gate) = futures::channel::oneshot::channel();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalStatementGate {
                gate,
                drops: Arc::clone(&drops),
            }));
        let mut receiver = None;
        let publication = service
            .publish_statement_action_in(&mut owner, &mut action, &mut receiver)
            .unwrap();
        assert!(owner.is_none() && action.is_none());
        publication.finish().unwrap();
        assert_eq!(
            control.finish_governed_statement(token),
            GovernedStatementFinishOutcome::Stale,
            "the old numeric finish route cannot destroy the source"
        );
        let mut wait = Box::pin(receiver.take().unwrap());
        use futures::FutureExt;
        assert!(wait.as_mut().now_or_never().is_none());
        drive_original_statement_actions(&slot);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        assert_eq!(
            drops.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "actual action Drop precedes the protocol handoff"
        );
        let receipt = wait.await.unwrap();
        let mut stable_ready = None;
        receipt
            .take_ready_in(&mut stable_ready)
            .unwrap_or_else(|_| panic!("same original stable receiver"))
            .finish()
            .unwrap();
        let ready = stable_ready.take().unwrap();
        assert!(ready.result.is_ok());
        assert_eq!(ready.owner.token(), token);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        ready.owner.finish();
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }
    #[test]
    fn abandoning_the_waiter_never_drops_the_original_inflight_statement() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone()).unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let (release, gate) = futures::channel::oneshot::channel();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalStatementGate {
                gate,
                drops: Arc::clone(&drops),
            }));
        let mut receiver = None;
        service
            .publish_statement_action_in(&mut owner, &mut action, &mut receiver)
            .unwrap()
            .finish()
            .unwrap();
        drop(receiver.take());
        drive_original_statement_actions(&slot);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert!(matches!(
            service.begin_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None
            ),
            Err(GovernedQueryStatementBeginError::QueryControl(
                QueryControlError::StatementBusy
            ))
        ));
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert!(!slot.has_sources());
    }
    #[test]
    fn original_statement_poll_panic_retains_same_cause_and_owner_without_repoll() {
        struct Cause(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Cause {
            fn drop(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        struct PanicAction {
            polls: Arc<std::sync::atomic::AtomicUsize>,
            cause: Option<Cause>,
        }
        impl crate::session_control::GovernedStatementAction for PanicAction {
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedQueryStatementOwner,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<
                    crate::session_control::StatementActionValue,
                    crate::session_error::QueryServiceError,
                >,
            > {
                self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::panic::panic_any(self.cause.take().unwrap())
            }
        }
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone()).unwrap();
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(PanicAction {
                polls: Arc::clone(&polls),
                cause: Some(Cause(Arc::clone(&drops))),
            }));
        let mut receiver = None;
        service
            .publish_statement_action_in(&mut owner, &mut action, &mut receiver)
            .unwrap()
            .finish()
            .unwrap();
        drive_original_statement_actions(&slot);
        let failure = slot.failure().expect("same original cause");
        assert!(failure.payload().is::<Cause>());
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        drop(receiver.take());
        drive_original_statement_actions(&slot);
        assert_eq!(
            polls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a poisoned source is never repolled"
        );
        let source = control.next_statement_action(None).unwrap().1.unwrap();
        drop(failure);
        source.dispose_poisoned_for_test(&service);
        drop(source);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn stopped_statement_publication_returns_the_same_two_original_sources() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone()).unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let token = owner.as_ref().unwrap().token();
        let (_release, gate) = futures::channel::oneshot::channel();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalStatementGate {
                gate,
                drops: Arc::clone(&drops),
            }));
        let mut receiver = None;
        slot.close();
        let refusal = service
            .publish_statement_action_in(&mut owner, &mut action, &mut receiver)
            .unwrap();
        assert_eq!(refusal.result(), &Err(QueryControlError::StatementBusy));
        assert_eq!(owner.as_ref().unwrap().token(), token);
        assert!(action.is_some() && receiver.is_none());
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(refusal.finish(), Err(QueryControlError::StatementBusy));
        drop(action);
        owner.take().unwrap().protocol_fail();
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }
    #[test]
    fn abandoned_statement_cleanup_wake_panic_keeps_the_same_source_and_owner() {
        use std::future::Future;
        use std::sync::atomic::{AtomicUsize, Ordering};
        type OriginalWait = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;
        struct CleanupCause(Arc<AtomicUsize>);
        impl Drop for CleanupCause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct PanicOnce {
            cause: Mutex<Option<CleanupCause>>,
        }
        impl std::task::Wake for PanicOnce {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                let cause = self.cause.lock().unwrap().take();
                if let Some(cause) = cause {
                    std::panic::panic_any(cause);
                }
            }
        }
        struct ArmActualCleanupWake {
            workload: Arc<WorkloadControl>,
            wait: Arc<Mutex<Option<OriginalWait>>>,
            wake: Arc<PanicOnce>,
        }
        impl crate::session_control::GovernedStatementAction for ArmActualCleanupWake {
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedQueryStatementOwner,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<
                    crate::session_control::StatementActionValue,
                    crate::session_error::QueryServiceError,
                >,
            > {
                std::task::Poll::Ready(Ok(crate::session_control::StatementActionValue::Ok))
            }
        }
        impl Drop for ArmActualCleanupWake {
            fn drop(&mut self) {
                // Arm the REAL authority waiter after action exit and before
                // the original root/permit cleanup. No injected cleanup hook.
                let workload = Arc::clone(&self.workload);
                let marker = workload.progress_revision();
                let mut waiting: OriginalWait = Box::pin(async move {
                    workload.wait_progress(marker).await;
                });
                let waker = std::task::Waker::from(Arc::clone(&self.wake));
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut std::task::Context::from_waker(&waker))
                        .is_pending()
                );
                *self.wait.lock().unwrap() = Some(waiting);
            }
        }
        let (control, service, workload) = governed_control();
        let workload = Arc::new(workload);
        let session = register(&control, 7, 1, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let wake = Arc::new(PanicOnce {
            cause: Mutex::new(Some(CleanupCause(Arc::clone(&drops)))),
        });
        let wait = Arc::new(Mutex::new(None));
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let token = owner.as_ref().unwrap().token();
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(ArmActualCleanupWake {
                workload: Arc::clone(&workload),
                wait: Arc::clone(&wait),
                wake,
            }));
        let mut receiver = None;
        service
            .publish_statement_action_in(&mut owner, &mut action, &mut receiver)
            .unwrap()
            .finish()
            .unwrap();
        drop(receiver.take());
        // Settle the original cancellation delivery so the waiter below arms
        // for the true completion callback rather than an older ready intent.
        while let Some(permit) = workload.next_control() {
            permit.acknowledge();
        }
        drive_original_statement_actions(&slot);
        let failure = slot.failure().expect("actual cleanup wake preserved");
        assert!(failure.payload().is::<CleanupCause>());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(
            slot.has_sources(),
            "callback panic cannot detach the sole stable receiver"
        );
        assert_eq!(
            control.finish_governed_statement(token),
            GovernedStatementFinishOutcome::Stale
        );
        let source = control.next_statement_action(None).unwrap().1.unwrap();
        assert!(
            !source.is_settled(),
            "partial cleanup is not a terminal receipt"
        );
        assert!(matches!(
            service.begin_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None
            ),
            Err(GovernedQueryStatementBeginError::QueryControl(
                QueryControlError::StatementBusy
            ))
        ));
        let waiting = wait.lock().unwrap().take();
        drop(waiting);
        drop(failure);
        source.dispose_poisoned_for_test(&service);
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert_eq!(workload.snapshot().businesses, 0);
    }
    struct OriginalLocalExplainGate {
        gate: futures::channel::oneshot::Receiver<()>,
        dropped: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl crate::session_control::GovernedStatementAction for OriginalLocalExplainGate {
        fn poll_action(
            &mut self,
            owner: &mut crate::session_control::GovernedQueryStatementOwner,
            cx: &mut Context<'_>,
        ) -> Poll<
            Result<
                crate::session_control::StatementActionValue,
                crate::session_error::QueryServiceError,
            >,
        > {
            if std::pin::Pin::new(&mut self.gate).poll(cx).is_pending() {
                return Poll::Pending;
            }
            let window = owner.result_window_alias().expect("same Local window");
            let value = crate::api::LocalResultProducer::try_new(owner.scope(), window)
                .and_then(|producer| {
                    producer.produce(|| {
                        crate::api::build_string_query_result(
                            "Explain String",
                            vec!["ready".into()],
                        )
                    })
                })
                .map(crate::session_control::StatementActionValue::Local)
                .map_err(|error| {
                    crate::session_error::QueryServiceError::new(
                        crate::session_error::QueryServiceErrorKind::Internal,
                        error,
                    )
                });
            Poll::Ready(value)
        }
    }
    impl Drop for OriginalLocalExplainGate {
        fn drop(&mut self) {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    async fn original_local_statement(
        control: &Arc<QueryApplicationControl>,
        service: &QueryControlService,
        workload: &WorkloadControl,
        id: u32,
    ) -> crate::session_control::GovernedQueryStatementOwner {
        let session = register(control, id, 1, "root");
        service
            .begin_queued_governed_query_statement_with_result(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
                novarocks_workload_control::ResultWindowClass::Local,
            )
            .await
            .unwrap()
    }
    fn local_workload() -> (
        WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        (workload, capacity)
    }
    #[tokio::test]
    async fn abandoned_explain_dispatch_retains_local_window_until_actual_action_exit() {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let (workload, capacity) = local_workload();
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone()).unwrap();
        let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
        let (release, gate) = futures::channel::oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalLocalExplainGate {
                gate,
                dropped: Arc::clone(&dropped),
            }));
        let mut wait = None;
        service
            .publish_statement_action_in(&mut owner, &mut action, &mut wait)
            .unwrap()
            .finish()
            .unwrap();
        drive_original_statement_actions(&slot);
        drop(wait.take());
        drive_original_statement_actions(&slot);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }
    #[tokio::test]
    async fn local_ready_is_stable_before_tail_panic_and_protocol_transfer_is_pure() {
        use crate::session_control::{StatementActionReady, StatementActionValue};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let (workload, capacity) = local_workload();
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone()).unwrap();
        let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
        let token = owner.as_ref().unwrap().token();
        let (release, gate) = futures::channel::oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalLocalExplainGate { gate, dropped }));
        let mut wait = None;
        service
            .publish_statement_action_in(&mut owner, &mut action, &mut wait)
            .unwrap()
            .finish()
            .unwrap();
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        let receipt = wait.take().unwrap().await.unwrap();
        let receiver = Arc::new(Mutex::new(None::<StatementActionReady>));
        struct PanicReceiverWake(Arc<Mutex<Option<StatementActionReady>>>);
        impl std::task::Wake for PanicReceiverWake {
            fn wake(self: Arc<Self>) {
                assert!(
                    self.0.try_lock().is_ok(),
                    "receiver notification is outside its lock"
                );
                std::panic::panic_any(17u32);
            }
        }
        let waker = std::task::Waker::from(Arc::new(PanicReceiverWake(Arc::clone(&receiver))));
        let mut cx = Context::from_waker(&waker);
        let mut cursor = crate::session_control::StatementProgressCursor::default();
        let _ = slot.poll_turn(
            &mut cursor,
            std::num::NonZeroUsize::new(7).unwrap(),
            &mut cx,
        );
        let tail = {
            let mut target = receiver.lock().unwrap();
            receipt
                .take_ready_in(&mut target)
                .unwrap_or_else(|_| panic!("same empty receiver"))
        };
        let cause = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tail.finish()));
        assert!(matches!(cause,Err(ref payload) if payload.downcast_ref::<u32>()==Some(&17)));
        assert_eq!(
            receiver.lock().unwrap().as_ref().unwrap().owner.token(),
            token
        );
        assert!(matches!(
            receiver.lock().unwrap().as_ref().unwrap().result,
            Ok(StatementActionValue::Local(_))
        ));
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let mut output = None;
        {
            let mut source = receiver.lock().unwrap();
            StatementActionReady::transfer_protocol_in(&mut source, &mut output).unwrap();
        }
        let crate::protocol_delivery::QuerySessionOutput::GovernedQuery(result) = output.unwrap()
        else {
            panic!("same Local graph")
        };
        let (local, mut protocol) = result.into_parts();
        assert_eq!(local.row_count(), 1);
        drop(local);
        assert_eq!(
            protocol.complete(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    #[tokio::test]
    async fn local_protocol_refusal_preserves_foreign_graph_and_original_owner() {
        use crate::session_control::{StatementActionReady, StatementActionValue};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let (workload, capacity) = local_workload();
        let owner = original_local_statement(&control, &service, &workload, 7).await;
        let foreign = original_local_statement(&control, &service, &workload, 8).await;
        let local = crate::api::LocalResultProducer::try_new(
            foreign.scope(),
            foreign.result_window_alias().unwrap(),
        )
        .unwrap()
        .produce(|| crate::api::build_string_query_result("Explain String", vec!["foreign".into()]))
        .unwrap();
        let token = owner.token();
        let mut source = Some(StatementActionReady {
            result: Ok(StatementActionValue::Local(local)),
            owner,
        });
        let mut target = None;
        assert!(StatementActionReady::transfer_protocol_in(&mut source, &mut target).is_err());
        assert!(target.is_none());
        assert_eq!(capacity.snapshot().held_positions, [0, 2, 0, 0]);
        assert_eq!(source.as_ref().unwrap().owner.token(), token);
        let ready = source.take().unwrap();
        drop(ready.result);
        ready.owner.finish();
        foreign.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
