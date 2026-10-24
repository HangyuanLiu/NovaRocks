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

//! Stable action custody in the original registered statement, never a Work role.

use super::*;
/// An action cannot return a second governed owner through its value slot.
pub enum StatementActionValue {
    Local(crate::api::OwnedLocalResult),
    Ok,
}
use crate::session_error::QueryServiceError;
use futures::task::AtomicWaker;
use novarocks_spi::connector::LakePublicationDisposition;
use std::any::Any;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll, Wake};

/// FA implements the business state machine. Polling may only borrow its same
/// stable inputs. Ready means all local action futures/jobs were actually
/// observed and their remaining custody was installed in an original receiver.
/// This interface grants neither execution nor a second root owner.
pub trait GovernedStatementAction: Send + 'static {
    /// Read only facts already retained by the original action. This must not
    /// poll a future, perform I/O, dispatch an operation, or grant local exit.
    fn publication_disposition(&self) -> Option<LakePublicationDisposition> {
        None
    }

    /// A stable local source could not prove actual exit. This is independent
    /// of a query-domain error and retains this same action without repolling.
    fn local_health_failure(&self) -> Option<crate::api::StatementNativeHealthFailure> {
        None
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedQueryStatementOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>>;
}

enum StatementActionFailureCause {
    Panic(Mutex<Box<dyn Any + Send>>),
    LocalHealth(crate::api::StatementNativeHealthFailure),
}

pub struct StatementActionFailure {
    primary: StatementActionFailureCause,
    publication: AtomicU8,
}
impl StatementActionFailure {
    pub fn publication_disposition(&self) -> Option<LakePublicationDisposition> {
        match self.publication.load(Ordering::Acquire) {
            0 => None,
            1 => Some(LakePublicationDisposition::KnownUncommitted),
            2 => Some(LakePublicationDisposition::CommitUnknown),
            3 => Some(LakePublicationDisposition::KnownCommitted),
            _ => unreachable!("closed publication disposition"),
        }
    }

    pub fn panic_payload(&self) -> Option<std::sync::MutexGuard<'_, Box<dyn Any + Send>>> {
        match &self.primary {
            StatementActionFailureCause::Panic(payload) => {
                Some(payload.lock().unwrap_or_else(|p| p.into_inner()))
            }
            StatementActionFailureCause::LocalHealth(_) => None,
        }
    }
    pub fn health(&self) -> Option<&crate::api::StatementNativeHealthFailure> {
        match &self.primary {
            StatementActionFailureCause::Panic(_) => None,
            StatementActionFailureCause::LocalHealth(health) => Some(health),
        }
    }
}
impl std::fmt::Debug for StatementActionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StatementActionFailure(..)")
    }
}

/// An observer never owns the only statement/action. Its loss requests cancel;
/// the original supervisor continues polling the same source.
pub struct StatementActionWaiter {
    source: Arc<StatementActionSource>,
    service: QueryControlService,
    completed: bool,
}
#[derive(Debug)]
pub enum StatementActionWaitError {
    Failed(Arc<StatementActionFailure>),
    /// The original stopped source actually disposed its local action/result
    /// and settled its original protocol owner. This is not an absence test.
    Stopped {
        publication: Option<LakePublicationDisposition>,
    },
}
pub struct StatementActionReady {
    pub result: Result<StatementActionValue, QueryServiceError>,
    pub owner: GovernedQueryStatementOwner,
}
impl StatementActionReady {
    /// Validate before either move. Refusal leaves both stable slots unchanged.
    /// Success performs only moves and closed constructors, with no callback.
    pub fn transfer_protocol_in(
        source: &mut Option<Self>,
        target: &mut Option<crate::protocol_delivery::QuerySessionOutput>,
    ) -> Result<(), &'static str> {
        use crate::protocol_delivery::*;
        if target.is_some() {
            return Err("protocol destination is occupied");
        }
        let ready = source.as_ref().ok_or("statement receiver is empty")?;
        if let Ok(StatementActionValue::Local(result)) = &ready.result {
            if !result.is_for_scope(ready.owner.scope()) {
                return Err("Local result belongs to another statement");
            }
        }
        let ready = source.take().expect("checked original receiver");
        *target = Some(match ready.result {
            Ok(StatementActionValue::Local(result)) => QuerySessionOutput::GovernedQuery(
                GovernedImmediateStatementResult::from_verified_owned_local(result, ready.owner),
            ),
            Ok(StatementActionValue::Ok) => QuerySessionOutput::GovernedCompletion(
                GovernedCompletionStatementResult::new(ready.owner),
            ),
            Err(error) => QuerySessionOutput::GovernedError(GovernedErrorStatementResult::new(
                error,
                ready.owner,
            )),
        });
        Ok(())
    }
}
/// Move-only observation of Ready. The original source STILL owns both the
/// result and owner until silent installation in a real stable destination.
pub struct StatementActionReceipt {
    source: Arc<StatementActionSource>,
    service: QueryControlService,
    transferred: bool,
}
impl StatementActionReceipt {
    pub fn take_ready_in(
        mut self,
        destination: &mut Option<StatementActionReady>,
    ) -> Result<StatementActionAttempt, Self> {
        if destination.is_some() {
            return Err(self);
        }
        let root = self
            .source
            .wake
            .root
            .upgrade()
            .expect("original supervisor retains every source");
        {
            let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.closed
                || state.failure.is_some()
                || state.settled
                || state.owner.is_none()
                || state.result.is_none()
            {
                drop(state);
                return Err(self);
            }
            let mut owner = state.owner.take().expect("checked original owner");
            owner.restore_control_link(&self.service);
            let result = state.result.take().expect("checked original result");
            *destination = Some(StatementActionReady { result, owner });
        }
        assert!(
            self.service.port.detach_statement_action(
                destination.as_ref().expect("stable receiver").owner.token(),
                &self.source
            ),
            "same original statement source"
        );
        self.transferred = true;
        Ok(StatementActionAttempt {
            result: Ok(()),
            root,
            pending: true,
        })
    }
}
impl Drop for StatementActionReceipt {
    fn drop(&mut self) {
        if !self.transferred {
            self.source.abandon();
        }
    }
}
impl std::future::Future for StatementActionWaiter {
    type Output = Result<StatementActionReceipt, StatementActionWaitError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "a statement observer is consumed once");
        let source = Arc::clone(&self.source);
        source.waiter.register(cx.waker());
        {
            let state = source.state.lock().unwrap_or_else(|p| p.into_inner());
            // The original poll loan must restore its retained facts before
            // an observer can expose a failure caused by that same poll.
            if state.polling {
                return Poll::Pending;
            }
            if let Some(failure) = &state.failure {
                return Poll::Ready(Err(StatementActionWaitError::Failed(Arc::clone(failure))));
            }
            if state.settled {
                return Poll::Ready(Err(StatementActionWaitError::Stopped {
                    publication: state.publication,
                }));
            }
            if !state.closed {
                return Poll::Pending;
            }
        }
        self.completed = true;
        Poll::Ready(Ok(StatementActionReceipt {
            source,
            service: self.service.clone(),
            transferred: false,
        }))
    }
}

impl Drop for StatementActionWaiter {
    fn drop(&mut self) {
        if !self.completed {
            self.source.abandon();
        }
    }
}

struct StatementActionState {
    owner: Option<GovernedQueryStatementOwner>,
    action: Option<Box<dyn GovernedStatementAction>>,
    cancellation: Option<GovernedStatementCancellation>,
    result: Option<Result<StatementActionValue, QueryServiceError>>,
    failure: Option<Arc<StatementActionFailure>>,
    secondary: Option<Arc<StatementActionFailure>>,
    publication: Option<LakePublicationDisposition>,
    settled: bool,
    polling: bool,
    closed: bool,
}
#[doc(hidden)]
pub struct StatementActionSource {
    state: Mutex<StatementActionState>,
    waiter: AtomicWaker,
    abandoned: AtomicBool,
    cancel_requested: AtomicBool,
    wake: Arc<StatementActionWake>,
}
struct StatementActionWake {
    root: Weak<StatementProgressSlot>,
}
impl Wake for StatementActionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(root) = self.root.upgrade() {
            root.notify();
        }
    }
}
impl StatementActionSource {
    fn empty(root: &Arc<StatementProgressSlot>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(StatementActionState {
                owner: None,
                action: None,
                cancellation: None,
                result: None,
                failure: None,
                secondary: None,
                publication: None,
                settled: false,
                polling: false,
                closed: false,
            }),
            waiter: AtomicWaker::new(),
            abandoned: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            wake: Arc::new(StatementActionWake {
                root: Arc::downgrade(root),
            }),
        })
    }
    pub(crate) fn install(
        &self,
        owner: &mut Option<GovernedQueryStatementOwner>,
        action: &mut Option<Box<dyn GovernedStatementAction>>,
        cancellation: &GovernedStatementCancellation,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.owner.is_none() && state.action.is_none());
        // The caller still borrows the strong service. Demotion is a pure
        // authority-preserving link change, never an owner reconstruction.
        owner
            .as_mut()
            .expect("checked original owner")
            .park_control_link();
        state.owner = owner.take();
        state.action = action.take();
        state.cancellation = Some(cancellation.clone());
    }
    fn abandon(&self) {
        self.abandoned.store(true, Ordering::Release);
        if let Err(primary) = catch_unwind(AssertUnwindSafe(|| {
            self.request_cancel(CancellationReason::OwnerDropped);
            self.notify_root();
        })) {
            self.record_failure(primary);
        }
    }
    pub(crate) fn failure(&self) -> Option<Arc<StatementActionFailure>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .failure
            .clone()
    }
    #[cfg(test)]
    pub(crate) fn dispose_poisoned_for_test(self: &Arc<Self>, service: &QueryControlService) {
        let (action, mut owner, primary, secondary) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            (
                state.action.take(),
                state.owner.take().expect("same source owner"),
                state.failure.take(),
                state.secondary.take(),
            )
        };
        drop(action);
        owner.restore_control_link(service);
        assert!(service.port.detach_statement_action(owner.token(), self));
        owner.protocol_fail();
        drop((primary, secondary));
    }
    fn record_failure(&self, primary: Box<dyn Any + Send>) {
        self.record_cause(StatementActionFailureCause::Panic(Mutex::new(primary)));
    }
    fn record_cause(&self, primary: StatementActionFailureCause) {
        let packet = Arc::new(StatementActionFailure {
            primary,
            publication: AtomicU8::new(0),
        });
        // The actual packet's raw cause is already retained. Initialize its
        // one quoted platform backing before sharing the packet with observers.
        drop(packet.panic_payload());
        let overflow = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            packet
                .publication
                .store(publication_code(state.publication), Ordering::Release);
            if state.failure.is_none() {
                state.failure = Some(packet);
                None
            } else if state.secondary.is_none() {
                state.secondary = Some(packet);
                None
            } else {
                Some(packet)
            }
        };
        // A failure beyond the fixed source-local slots goes to the SAME
        // original root panic packet,
        // never ordinary opaque-cause destruction under this metadata lock.
        if let Some(packet) = overflow {
            std::panic::panic_any(packet);
        }
    }
    fn retain_local_health(&self, action: &dyn GovernedStatementAction) -> bool {
        // Borrow foreign metadata outside State. The original action remains
        // in its restoration loan on panic and on an actual-exit refusal.
        match catch_unwind(AssertUnwindSafe(|| action.local_health_failure())) {
            Ok(None) => true,
            Ok(Some(failure)) => {
                self.record_cause(StatementActionFailureCause::LocalHealth(failure));
                false
            }
            Err(primary) => {
                self.record_failure(primary);
                false
            }
        }
    }
    fn retain_publication(&self, action: &dyn GovernedStatementAction) -> bool {
        // Foreign code runs outside the metadata lock, while the original
        // action/owner loan and its first raw failure remain retained.
        let fact = match catch_unwind(AssertUnwindSafe(|| action.publication_disposition())) {
            Ok(fact) => fact,
            Err(primary) => {
                self.record_failure(primary);
                return false;
            }
        };
        if let Some(fact) = fact {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.publication = Some(fact);
            for packet in [&state.failure, &state.secondary].into_iter().flatten() {
                packet
                    .publication
                    .store(publication_code(Some(fact)), Ordering::Release);
            }
        }
        true
    }
    fn notify_root(&self) {
        self.wake.wake_by_ref();
    }
    fn request_cancel(&self, reason: CancellationReason) {
        // A source exit cannot repeatedly manufacture cancellation progress.
        // Publish this one-shot cut before any real WC callback can unwind.
        if self.cancel_requested.swap(true, Ordering::AcqRel) {
            return;
        }

        let cancellation = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancellation
            .clone();
        if let Some(cancellation) = cancellation {
            let _ = cancellation.requester().request(reason);
        }
    }
    pub(crate) fn stop(&self) {
        self.abandoned.store(true, Ordering::Release);
    }
    fn poll(self: &Arc<Self>) {
        if self.failure().is_some() {
            return;
        }
        if self.abandoned.load(Ordering::Acquire) {
            self.request_cancel(CancellationReason::OwnerDropped);
        }
        let Some(mut loan) = StatementPollLoan::take(Arc::clone(self)) else {
            self.finish_abandoned();
            return;
        };
        let waker = std::task::Waker::from(Arc::clone(&self.wake));
        let mut cx = Context::from_waker(&waker);
        let result = catch_unwind(AssertUnwindSafe(|| {
            loan.action
                .as_mut()
                .unwrap()
                .poll_action(loan.owner.as_mut().unwrap(), &mut cx)
        }));
        match result {
            Ok(Poll::Pending) => {
                let healthy =
                    self.retain_local_health(loan.action.as_deref().expect("same original action"));
                if !healthy {
                    self.retain_publication(loan.action.as_deref().expect("same original action"));
                }
                drop(loan);
                if !healthy {
                    self.waiter.wake();
                }
            }
            Err(primary) => {
                // The same original cause becomes stable BEFORE any action/F
                // cleanup. The poisoned action is retained and never repolled.
                self.record_failure(primary);
                self.retain_publication(loan.action.as_deref().expect("same original action"));
                drop(loan);
                self.waiter.wake();
            }
            Ok(Poll::Ready(result)) => {
                {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    state.result = Some(result);
                }
                // A close failure preserves the whole action/owner even when
                // its safe business result is Ready. It is not a fake panic.
                if !self.retain_local_health(loan.action.as_deref().expect("same original action"))
                {
                    self.retain_publication(loan.action.as_deref().expect("same original action"));
                    drop(loan);
                    self.waiter.wake();
                    return;
                }
                // The value is stable before reading the original effect fact,
                // which must survive a subsequent action destructor panic.
                if !self.retain_publication(loan.action.as_deref().expect("same original action")) {
                    drop(loan);
                    self.waiter.wake();
                    return;
                }
                let action = loan.action.take();
                // Restore owner to its stable source before arbitrary Drop.
                drop(loan);
                let cleanup = catch_unwind(AssertUnwindSafe(|| drop(action)));
                match cleanup {
                    Ok(()) => self.state.lock().unwrap_or_else(|p| p.into_inner()).closed = true,
                    Err(primary) => self.record_failure(primary),
                }
                self.waiter.wake();
                self.finish_abandoned();
            }
        }
    }
    fn finish_abandoned(self: &Arc<Self>) {
        if !self.abandoned.load(Ordering::Acquire) {
            return;
        }
        let result = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.closed || state.failure.is_some() || state.polling || state.owner.is_none() {
                return;
            }
            state.result.take()
        };
        // The original owner remains in Source while result Drop runs. A
        // partial/destructive result exit does not become a successful close.
        if let Err(primary) = catch_unwind(AssertUnwindSafe(|| drop(result))) {
            self.record_failure(primary);
            return;
        }
        let Some(mut loan) = StatementCleanupLoan::take(Arc::clone(self)) else {
            return;
        };
        let token = loan.owner.as_ref().expect("same original owner").token();
        let port = loan
            .owner
            .as_ref()
            .expect("same original owner")
            .service
            .port();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            loan.owner
                .as_mut()
                .expect("same original owner")
                .cleanup_abandoned_action_in_place()
        }));
        if let Err(primary) = outcome {
            // Stable first cause BEFORE returning the same in-place owner.
            self.record_failure(primary);
            drop(loan);
            self.waiter.wake();
            return;
        }
        drop(loan);
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.failure.is_some() {
                return;
            }
            state.settled = true;
        }
        // No callback/foreign Drop remains in this exact-source transaction.
        // A Source Arc is still held here while the original entry is removed.
        assert!(
            port.settle_statement_action_silent(token, self),
            "same actually settled original source"
        );
        self.waiter.wake();
    }
    pub(crate) fn is_settled(&self) -> bool {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).settled
    }
}
fn publication_code(fact: Option<LakePublicationDisposition>) -> u8 {
    match fact {
        None => 0,
        Some(LakePublicationDisposition::KnownUncommitted) => 1,
        Some(LakePublicationDisposition::CommitUnknown) => 2,
        Some(LakePublicationDisposition::KnownCommitted) => 3,
    }
}

// Same-source synchronous loan, not a new owner or an async guard. It restores
// the original owner on every unwind BEFORE the root can retain its packet.
struct StatementCleanupLoan {
    source: Arc<StatementActionSource>,
    owner: Option<GovernedQueryStatementOwner>,
}
impl StatementCleanupLoan {
    fn take(source: Arc<StatementActionSource>) -> Option<Self> {
        let owner = {
            let mut state = source.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.polling || state.failure.is_some() || state.owner.is_none() {
                return None;
            }
            state.polling = true;
            state.owner.take()
        };
        Some(Self { source, owner })
    }
}
impl Drop for StatementCleanupLoan {
    fn drop(&mut self) {
        let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.owner.is_none(), "one original cleanup loan");
        state.owner = self.owner.take();
        state.polling = false;
    }
}
struct StatementPollLoan {
    source: Arc<StatementActionSource>,
    action: Option<Box<dyn GovernedStatementAction>>,
    owner: Option<GovernedQueryStatementOwner>,
}
impl StatementPollLoan {
    fn take(source: Arc<StatementActionSource>) -> Option<Self> {
        let (action, owner) = {
            let mut state = source.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.polling || state.closed || state.failure.is_some() || state.action.is_none() {
                return None;
            }
            state.polling = true;
            (state.action.take(), state.owner.take())
        };
        Some(Self {
            source,
            action,
            owner,
        })
    }
}
impl Drop for StatementPollLoan {
    fn drop(&mut self) {
        let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            state.owner.is_none() && state.action.is_none(),
            "same action loan returned once"
        );
        state.owner = self.owner.take();
        state.action = self.action.take();
        state.polling = false;
    }
}

/// One process composition field, not a work registry or an execution role.
/// Its strong control alias is OUTSIDE Control/Registry. Control only stores a
/// Weak back-link; action futures may therefore use the original Registry.
#[doc(hidden)]
pub struct StatementProgressSlot {
    control: Mutex<Option<QueryControlService>>,
    accepting: AtomicBool,
    ready: AtomicBool,
    wake: AtomicWaker,
}
#[derive(Default)]
pub(crate) struct StatementProgressCursor {
    after: Option<u32>,
    active: bool,
}
impl StatementProgressSlot {
    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self {
            control: Mutex::new(None),
            accepting: AtomicBool::new(true),
            ready: AtomicBool::new(false),
            wake: AtomicWaker::new(),
        })
    }
    pub(crate) fn install(
        self: &Arc<Self>,
        control: QueryControlService,
    ) -> Result<(), QueryControlError> {
        // Install is a serial pre-Ready composition step, not a hot replacement.
        if !self.accepting.load(Ordering::Acquire) {
            return Err(QueryControlError::StatementBusy);
        }
        let mut slot = self.control.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        control.port.attach_statement_progress(self)?;
        *slot = Some(control);
        drop(slot);
        self.notify();
        Ok(())
    }
    fn control(&self) -> Option<QueryControlService> {
        self.control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    fn notify(&self) {
        self.ready.store(true, Ordering::Release);
        self.wake.wake();
    }
    pub(crate) fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        if let Some(control) = self.control() {
            control.port.close_statement_actions();
        }
        self.notify();
    }
    pub(crate) fn failure(&self) -> Option<Arc<StatementActionFailure>> {
        self.control()
            .and_then(|control| control.port.statement_action_failure())
    }
    pub(crate) fn has_sources(&self) -> bool {
        self.control()
            .is_some_and(|control| control.port.has_statement_actions())
    }
    pub(crate) fn poll_turn(
        &self,
        cursor: &mut StatementProgressCursor,
        budget: NonZeroUsize,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        self.wake.register(cx.waker());
        if !cursor.active {
            if !self.ready.swap(false, Ordering::AcqRel) {
                return Poll::Pending;
            }
            cursor.active = true;
            cursor.after = None;
        }
        let Some(control) = self.control() else {
            cursor.active = false;
            return Poll::Pending;
        };
        for _ in 0..budget.get() {
            let Some((key, source)) = control.port.next_statement_action(cursor.after) else {
                cursor.active = false;
                cursor.after = None;
                return Poll::Ready(());
            };
            cursor.after = Some(key);
            if let Some(source) = source {
                source.poll();
            }
        }
        // Preserve the finite scan frontier. No event/empty prefix is treated
        // as evidence that later original sessions have no runnable action.
        cx.waker().wake_by_ref();
        Poll::Ready(())
    }
}

#[must_use = "finish or drop only outside the publisher's locks"]
pub struct StatementActionAttempt {
    result: Result<(), QueryControlError>,
    root: Arc<StatementProgressSlot>,
    pending: bool,
}
impl StatementActionAttempt {
    pub fn result(&self) -> &Result<(), QueryControlError> {
        &self.result
    }
    pub fn finish(mut self) -> Result<(), QueryControlError> {
        self.notify();
        self.result.clone()
    }
    fn notify(&mut self) {
        if self.pending {
            self.pending = false;
            self.root.notify();
        }
    }
}
impl Drop for StatementActionAttempt {
    fn drop(&mut self) {
        self.notify();
    }
}
impl QueryControlService {
    pub fn publish_statement_action_in(
        &self,
        owner: &mut Option<GovernedQueryStatementOwner>,
        action: &mut Option<Box<dyn GovernedStatementAction>>,
        output: &mut Option<StatementActionWaiter>,
    ) -> Result<StatementActionAttempt, QueryControlError> {
        if owner.is_none() || action.is_none() || output.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        if !Arc::ptr_eq(
            &owner.as_ref().expect("checked owner").service.port(),
            &self.port,
        ) {
            return Err(QueryControlError::StaleSession);
        }
        let root = self
            .port
            .statement_progress_slot()
            .ok_or(QueryControlError::UnknownSession)?;
        let source = StatementActionSource::empty(&root);
        let result = self
            .port
            .publish_statement_action_in(owner, action, &source);
        if result.is_ok() {
            *output = Some(StatementActionWaiter {
                source: Arc::clone(&source),
                service: self.clone(),
                completed: false,
            });
        }
        let pending = result.is_ok();
        Ok(StatementActionAttempt {
            result,
            root,
            pending,
        })
    }
    pub fn statement_action_geometry() -> StatementProgressGeometry {
        StatementProgressGeometry {
            source: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementActionSource>(),
            )
            .expect("fixed source layout"),
            wake: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementActionWake>(),
            )
            .expect("fixed wake layout"),
            driver: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementProgressSlot>(),
            )
            .expect("fixed statement driver layout"),
            observer: std::alloc::Layout::new::<StatementActionWaiter>(),
            receipt: std::alloc::Layout::new::<StatementActionReceipt>(),
            failure: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementActionFailure>(),
            )
            .expect("fixed source failure layout"),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct StatementProgressGeometry {
    pub source: std::alloc::Layout,
    pub wake: std::alloc::Layout,
    pub driver: std::alloc::Layout,
    pub observer: std::alloc::Layout,
    pub receipt: std::alloc::Layout,
    pub failure: std::alloc::Layout,
}
