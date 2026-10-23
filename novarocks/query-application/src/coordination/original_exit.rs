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

//! The original role's fixed receipt, not an execution or settlement authority.
use crate::api::{QueryExecutionError, QueryExecutionErrorKind};
use novarocks_types::identity::QueryExecutionId;
use novarocks_workload_control::PollCompletion;
use std::{
    alloc::Layout,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};
use tokio::task::JoinError;

type RoleResult = PollCompletion<Result<(), QueryExecutionError>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalActorExitPhase {
    Unreserved,
    Reserved,
    Installed,
    Joined,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalExecutionDisposition {
    ActualSourcesExited,
    OriginalPaidOwnerReturn,
}
/// Attempt closures are not a whole-statement remote receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteExecutionDisposition {
    RemoteNotReported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OriginalAssociation {
    pub registry: u64,
    pub initial: QueryExecutionId,
    pub generation: u64,
}
struct State {
    phase: LogicalActorExitPhase,
    association: Option<OriginalAssociation>,
    actor: Option<Result<(), Arc<JoinError>>>,
    pending_native_returns: u64,
    native_pre_actor_started: bool,
    native_closure_seen: bool,
    native_return_disposition: Option<LocalExecutionDisposition>,
    role: Option<RoleResult>,
    role_ended: bool,
    role_failed: bool,
    health_published: bool,
    health_disposed: bool,
}
pub(super) struct OriginalLogicalExit {
    state: Mutex<State>,
    changed: futures::task::AtomicWaker,
}
impl fmt::Debug for OriginalLogicalExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OriginalLogicalExit")
    }
}
impl fmt::Display for OriginalLogicalExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original logical execution failed; original cause retained")
    }
}
impl std::error::Error for OriginalLogicalExit {}

impl OriginalLogicalExit {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                phase: LogicalActorExitPhase::Unreserved,
                association: None,
                actor: None,
                pending_native_returns: 0,
                native_pre_actor_started: false,
                native_closure_seen: false,
                native_return_disposition: None,
                role: None,
                role_ended: false,
                role_failed: false,
                health_published: false,
                health_disposed: false,
            }),
            changed: futures::task::AtomicWaker::new(),
        })
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(super) fn reserve_silent(&self, association: OriginalAssociation) {
        let mut s = self.lock();
        assert!(s.phase == LogicalActorExitPhase::Unreserved && s.association.is_none());
        s.association = Some(association);
        s.phase = LogicalActorExitPhase::Reserved;
    }
    pub(super) fn unreserve_silent(&self, association: OriginalAssociation) {
        let mut s = self.lock();
        assert_eq!(s.association, Some(association));
        assert_eq!(s.phase, LogicalActorExitPhase::Reserved);
        s.phase = LogicalActorExitPhase::Unreserved;
        s.association = None;
    }
    pub(super) fn installed_silent(&self, association: OriginalAssociation) {
        let mut s = self.lock();
        assert_eq!(s.association, Some(association));
        assert_eq!(s.phase, LogicalActorExitPhase::Reserved);
        s.phase = LogicalActorExitPhase::Installed;
    }
    pub(super) fn native_pre_actor_started_silent(&self) {
        self.lock().native_pre_actor_started = true;
    }
    pub(super) fn native_reserved_silent(&self) {
        let mut s = self.lock();
        s.pending_native_returns = s
            .pending_native_returns
            .checked_add(1)
            .expect("finite original Native reservations cannot overflow");
    }
    pub(super) fn native_closure_silent(&self) {
        // Diagnostic only. A replayable closure observation is NOT the once
        // actual-return author of the original ActiveSlot.
        self.lock().native_closure_seen = true;
    }
    /// Called solely by the exact original ActiveSlot's successful remove.
    /// A replayed remote closure cannot invoke this once-return author.
    pub(super) fn native_return_silent(
        &self,
        key: super::NativeActiveCustodyKey,
        local: LocalExecutionDisposition,
    ) {
        let mut s = self.lock();
        let association = s
            .association
            .expect("original Native slot retains its source association");
        assert_eq!(association.initial, key.initial);
        assert_eq!(association.generation, key.generation);
        s.pending_native_returns = s
            .pending_native_returns
            .checked_sub(1)
            .expect("same original Native slot returns once");
        s.native_return_disposition = Some(match (s.native_return_disposition, local) {
            (Some(LocalExecutionDisposition::OriginalPaidOwnerReturn), _)
            | (_, LocalExecutionDisposition::OriginalPaidOwnerReturn) => {
                LocalExecutionDisposition::OriginalPaidOwnerReturn
            }
            _ => LocalExecutionDisposition::ActualSourcesExited,
        });
    }
    /// Only the original join author calls this after its exact native barrier.
    pub(super) fn joined_silent(
        &self,
        association: OriginalAssociation,
        result: Result<(), Arc<JoinError>>,
    ) {
        let mut s = self.lock();
        assert_eq!(s.association, Some(association));
        assert_eq!(s.phase, LogicalActorExitPhase::Installed);
        assert!(s.actor.is_none());
        s.actor = Some(result);
        s.phase = LogicalActorExitPhase::Joined;
    }
    pub(super) fn notify(&self) {
        self.changed.wake();
    }
    pub(super) fn role_ended_silent(&self, failed: bool) {
        let mut s = self.lock();
        s.role_ended = true;
        s.role_failed |= failed;
    }
    pub(super) fn health_published(&self) -> bool {
        self.lock().health_published
    }
    pub(super) fn mark_health_published_silent(&self) {
        self.lock().health_published = true;
    }
    pub(super) fn has_unhandled_failure(&self) -> bool {
        let s = self.lock();
        !s.health_disposed && (s.role_failed || matches!(&s.actor, Some(Err(_))))
    }
    pub(super) fn receive_role_silent(&self, packet: &mut Option<RoleResult>) {
        let mut s = self.lock();
        assert!(
            s.role.is_none() && packet.is_some(),
            "one original role completion"
        );
        s.role_ended = true;
        s.role_failed |= matches!(packet, Some(PollCompletion::Panicked(_)));
        s.role = packet.take();
    }
    pub(super) fn has_failure(&self) -> bool {
        let s = self.lock();
        s.role_failed || matches!(&s.actor, Some(Err(_)))
    }
    pub(super) fn pending_actor(&self) -> bool {
        let s = self.lock();
        s.role_ended
            && matches!(
                s.phase,
                LogicalActorExitPhase::Reserved | LogicalActorExitPhase::Installed
            )
    }
    fn disposition(&self) -> Option<LocalExecutionDisposition> {
        let s = self.lock();
        let cleared = match &s.role {
            Some(PollCompletion::Ready(_)) => true,
            Some(PollCompletion::Panicked(p)) => p.value().cleanup().is_none(),
            None => false,
        };
        if !cleared {
            return None;
        }
        match s.phase {
            LogicalActorExitPhase::Unreserved
                if !s.native_pre_actor_started && s.pending_native_returns == 0 =>
            {
                Some(LocalExecutionDisposition::ActualSourcesExited)
            }
            LogicalActorExitPhase::Unreserved => None,
            LogicalActorExitPhase::Joined
                if !s.native_pre_actor_started && s.pending_native_returns == 0 =>
            {
                Some(
                    s.native_return_disposition
                        .unwrap_or(LocalExecutionDisposition::ActualSourcesExited),
                )
            }
            LogicalActorExitPhase::Joined => None,
            _ => None,
        }
    }
}
/// Installed only in the original health slot. Dropping a rejected wrapper
/// does not dispose its source; the caller disarms it before returning custody
/// to the same finite bank Position.
pub(super) struct LogicalExitHealthCustody {
    source: Arc<OriginalLogicalExit>,
    armed: bool,
}
impl LogicalExitHealthCustody {
    pub(super) fn new(source: Arc<OriginalLogicalExit>) -> Self {
        Self {
            source,
            armed: true,
        }
    }
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for LogicalExitHealthCustody {
    fn drop(&mut self) {
        if self.armed {
            self.source.lock().health_disposed = true;
            // OriginalHealth is taken out of its lock before actual Drop.
            self.source.notify();
        }
    }
}

/// Unique observation edge. Cancelling/dropping a wait never cancels its source.
pub struct LogicalExecutionExitObserver {
    source: Option<Arc<OriginalLogicalExit>>,
}
/// Retains the original completion/footer until this exact receipt exits.
pub struct LogicalExecutionExitReceipt {
    source: Arc<OriginalLogicalExit>,
    local: LocalExecutionDisposition,
}
/// Borrow-only access; no raw packet, JoinError or footer can be taken.
pub struct LogicalExecutionExitView<'a> {
    state: MutexGuard<'a, State>,
}
impl LogicalExecutionExitView<'_> {
    pub fn actor_phase(&self) -> LogicalActorExitPhase {
        self.state.phase
    }
    pub fn actor_result(&self) -> Option<Result<(), &JoinError>> {
        self.state
            .actor
            .as_ref()
            .map(|r| r.as_ref().map(|_| ()).map_err(Arc::as_ref))
    }
    pub fn role_ended(&self) -> bool {
        self.state.role_ended
    }
    pub fn role_failed(&self) -> bool {
        self.state.role_failed
    }
    pub fn role_result(&self) -> Option<&RoleResult> {
        self.state.role.as_ref()
    }
    /// Diagnostic facts only; none substitutes for an actual local return.
    pub fn native_action_started(&self) -> bool {
        self.state.native_pre_actor_started
    }
    pub fn pending_native_returns(&self) -> u64 {
        self.state.pending_native_returns
    }
    pub fn native_closure_observed(&self) -> bool {
        self.state.native_closure_seen
    }
    pub fn native_return_disposition(&self) -> Option<LocalExecutionDisposition> {
        self.state.native_return_disposition
    }
}
impl LogicalExecutionExitObserver {
    pub(super) fn new(source: Arc<OriginalLogicalExit>) -> Self {
        Self {
            source: Some(source),
        }
    }
    pub fn view(&self) -> Option<LogicalExecutionExitView<'_>> {
        self.source.as_ref().map(|source| LogicalExecutionExitView {
            state: source.lock(),
        })
    }
    pub fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<LogicalExecutionExitReceipt, QueryExecutionError>> {
        let Some(source) = self.source.as_ref() else {
            return Poll::Ready(Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original exit observation was already received",
            )));
        };
        source.changed.register(cx.waker());
        if let Some(local) = source.disposition() {
            let source = self.source.take().unwrap();
            drop(source.changed.take());
            return Poll::Ready(Ok(LogicalExecutionExitReceipt { source, local }));
        }
        Poll::Pending
    }
}
impl Drop for LogicalExecutionExitObserver {
    fn drop(&mut self) {
        if let Some(source) = &self.source {
            drop(source.changed.take());
        }
    }
}
impl LogicalExecutionExitReceipt {
    pub const fn local_disposition(&self) -> LocalExecutionDisposition {
        self.local
    }
    pub const fn remote_disposition(&self) -> RemoteExecutionDisposition {
        RemoteExecutionDisposition::RemoteNotReported
    }
    pub fn view(&self) -> LogicalExecutionExitView<'_> {
        LogicalExecutionExitView {
            state: self.source.lock(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionExitGeometry {
    pub source: Layout,
    pub observer: Layout,
    pub receipt: Layout,
    pub input_header: Layout,
    pub publication_tail: Layout,
    pub health_wrapper: Layout,
}
impl LogicalExecutionExitObserver {
    /// Source header includes the whole original packet inline. Task/opaque
    /// pointees, platform Mutex and waker internals remain original-owner facts.
    pub fn metadata_geometry() -> Result<
        LogicalExecutionExitGeometry,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        Ok(LogicalExecutionExitGeometry {
            source: novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                OriginalLogicalExit,
            >())?,
            observer: Layout::new::<Self>(),
            receipt: Layout::new::<LogicalExecutionExitReceipt>(),
            input_header: Layout::new::<super::supervisor::OriginalLogicalStartInput>(),
            publication_tail: Layout::new::<super::supervisor::LogicalExecutionStartAttempt>(),
            health_wrapper: Layout::new::<LogicalExitHealthCustody>(),
        })
    }
}
