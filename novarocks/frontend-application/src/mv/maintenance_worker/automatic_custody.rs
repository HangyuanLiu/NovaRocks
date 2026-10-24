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

//! Same-worker automatic action custody. Borrowed waits own no unique input.

use super::*;
use novarocks_mv_application::maintenance::MaintenanceAttempt;
use novarocks_mv_application::management::EffectDisposition;
use novarocks_table_maintenance::runtime::TerminalError;
use novarocks_table_maintenance::{AutomaticMaintenanceOutcome, AutomaticOptimizeOutcome};
use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

type PanicCause = Box<dyn Any + Send + 'static>;
type ProductFuture = Pin<Box<dyn Future<Output = AutomaticProductResult> + Send>>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct AutomaticMaintenanceCustodyGeometry {
    /// Inline field in the original worker, including the original Work and
    /// action/result/cause Options, actual mutex/padding and fixed flags.
    pub(crate) slot: std::alloc::Layout,
    /// One Box pointee; the Box pointer is already inside slot.
    pub(crate) product: std::alloc::Layout,
}
pub(super) fn geometry() -> AutomaticMaintenanceCustodyGeometry {
    fn actual_future<I, F: Future>(_: impl Fn(I) -> F) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    AutomaticMaintenanceCustodyGeometry {
        slot: std::alloc::Layout::new::<Mutex<AutomaticPassSlot>>(),
        product: actual_future(make_automatic_product),
    }
}

#[derive(Default)]
pub(super) struct AutomaticPassSlot {
    borrowed: bool,
    body: Option<AutomaticPassBody>,
}
#[derive(Default)]
pub(super) struct AutomaticPassBody {
    // Action/F/result drop before their original Work owner and permits.
    action: Option<AutomaticAction>,
    pub(super) attempt: Option<MaintenanceAttempt>,
    pub(super) work: Option<AdmittedAutomaticQuery>,
}
/// The mutex protects only movement. Every foreign operation runs on this
/// loan outside the lock, and unwind restores the whole original record.
pub(super) struct AutomaticPassLoan<'a> {
    source: &'a Mutex<AutomaticPassSlot>,
    body: Option<AutomaticPassBody>,
}
impl<'a> AutomaticPassLoan<'a> {
    pub(super) fn take(source: &'a Mutex<AutomaticPassSlot>) -> Self {
        let mut slot = source.lock().unwrap_or_else(|p| p.into_inner());
        assert!(!slot.borrowed, "one original automatic pass poller");
        slot.borrowed = true;
        let body = slot.body.take().unwrap_or_default();
        Self {
            source,
            body: Some(body),
        }
    }
    pub(super) fn body(&self) -> &AutomaticPassBody {
        self.body.as_ref().expect("original pass loan")
    }
    pub(super) fn body_mut(&mut self) -> &mut AutomaticPassBody {
        self.body.as_mut().expect("original pass loan")
    }
}
impl Drop for AutomaticPassLoan<'_> {
    fn drop(&mut self) {
        let mut slot = self.source.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            slot.borrowed && slot.body.is_none(),
            "same original pass restored once"
        );
        slot.body = self.body.take();
        slot.borrowed = false;
    }
}

/// Closed production inputs, never an arbitrary callback supplied by a caller.
pub(super) enum AutomaticProductInput {
    Managed {
        service: Arc<dyn TableMaintenanceService>,
        engine: Arc<dyn TableMaintenanceEngine>,
        metadata: crate::query_execution::maintenance::MaintenanceActionState,
        request: MaintenanceActionRequest,
        effect_id: novarocks_table_maintenance::MaintenanceEffectId,
        context: AutomaticMaintenanceContext,
    },
    Optimize {
        service: Arc<dyn TableMaintenanceService>,
        engine: Arc<dyn TableMaintenanceEngine>,
        target: MaintenanceTarget,
        effect_id: novarocks_table_maintenance::MaintenanceEffectId,
        context: AutomaticMaintenanceContext,
    },
}
pub(super) enum AutomaticProductSuccess {
    Managed(AutomaticMaintenanceOutcome),
    Optimize(AutomaticOptimizeOutcome),
}
enum AutomaticProductResult {
    Managed(Result<AutomaticMaintenanceOutcome, TerminalError>),
    Optimize(Result<AutomaticOptimizeOutcome, TerminalError>),
}
impl AutomaticProductResult {
    fn disposition(&self) -> EffectDisposition {
        match self {
            Self::Managed(Ok(AutomaticMaintenanceOutcome::KnownCommitted(_))) => {
                EffectDisposition::KnownCommitted
            }
            Self::Managed(Ok(AutomaticMaintenanceOutcome::NoOpWithoutCommit(_))) => {
                EffectDisposition::KnownUncommitted
            }
            Self::Optimize(Ok(AutomaticOptimizeOutcome::Finished {
                commit_occurred: true,
                ..
            })) => EffectDisposition::KnownCommitted,
            Self::Optimize(Ok(
                AutomaticOptimizeOutcome::Finished {
                    commit_occurred: false,
                    ..
                }
                | AutomaticOptimizeOutcome::AlreadyActive,
            )) => EffectDisposition::KnownUncommitted,
            Self::Managed(Err(error)) | Self::Optimize(Err(error)) => {
                automatic_terminal_disposition(error.state)
            }
        }
    }
    fn finish(
        self,
        terminal: Result<(), MvBackgroundEngineError>,
    ) -> Result<AutomaticProductSuccess, MvBackgroundEngineError> {
        match self {
            Self::Managed(result) => {
                finish_automatic_action(result, terminal).map(AutomaticProductSuccess::Managed)
            }
            Self::Optimize(result) => {
                finish_automatic_action(result, terminal).map(AutomaticProductSuccess::Optimize)
            }
        }
    }
}

// This same function is the actual F layout source. It never captures the
// original pass slot, PreparedAction or WorkOwner; there is no back-reference.
async fn make_automatic_product(input: AutomaticProductInput) -> AutomaticProductResult {
    match input {
        AutomaticProductInput::Managed {
            service,
            engine,
            metadata,
            request,
            effect_id,
            context,
        } => AutomaticProductResult::Managed(
            service
                .execute_automatic_action_with_effect_id(
                    engine.as_ref(),
                    &metadata,
                    request,
                    effect_id,
                    &context,
                )
                .await,
        ),
        AutomaticProductInput::Optimize {
            service,
            engine,
            target,
            effect_id,
            context,
        } => AutomaticProductResult::Optimize(service.execute_automatic_optimize_with_effect_id(
            engine.as_ref(),
            target,
            effect_id,
            &context,
        )),
    }
}

struct AutomaticAction {
    // On explicit owner disposition, real F/output exit before the original
    // management lease. This field order is also part of the layout quote.
    future: Option<ProductFuture>,
    result: Option<AutomaticProductResult>,
    // Saved BEFORE mark_dispatched/terminal callbacks. A panic never replays them.
    dispatch_attempted: bool,
    terminal_attempted: bool,
    disposition: Option<EffectDisposition>,
    terminal: Option<Result<(), MvBackgroundEngineError>>,
    phase: ActionPhase,
    cause: Option<PanicCause>,
    prepared: Option<PreparedAutomaticAction>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActionPhase {
    Prepared,
    Running,
    ResultSaved,
    FutureDropped,
    TerminalSaved,
    Poisoned,
}
#[derive(Debug)]
struct OriginalAutomaticActionFailed;

pub(super) fn install_automatic_action(
    original: &Mutex<AutomaticPassSlot>,
    prepared: PreparedAutomaticAction,
    input: AutomaticProductInput,
) {
    let mut loan = AutomaticPassLoan::take(original);
    let body = loan.body_mut();
    assert!(
        body.work.is_some(),
        "Product requires its original admitted Work"
    );
    assert!(
        body.action.is_none(),
        "previous original action actually retired"
    );
    // Stable PreparedAction precedes even Product F construction.
    body.action = Some(AutomaticAction {
        prepared: Some(prepared),
        future: None,
        result: None,
        dispatch_attempted: false,
        terminal_attempted: false,
        disposition: None,
        terminal: None,
        phase: ActionPhase::Prepared,
        cause: None,
    });
    body.action.as_mut().unwrap().future = Some(Box::pin(make_automatic_product(input)));
}

fn poison(action: &mut AutomaticAction, cause: PanicCause) {
    assert!(
        action.cause.is_none(),
        "poisoned original action is never repolled"
    );
    action.cause = Some(cause);
    action.phase = ActionPhase::Poisoned;
}
fn publish_health_after_restore(loan: AutomaticPassLoan<'_>) -> ! {
    // The real opaque cause stays in the restored same-worker row. Only this
    // neutral marker reaches the original callback/JH panic slot.
    drop(loan);
    panic_any(OriginalAutomaticActionFailed)
}

// Advances only the real already-installed F. No readiness/absence fact is
// synthesized, and Pending preserves the same future in the same record.
fn poll_product(
    action: &mut AutomaticAction,
    cx: &mut Context<'_>,
) -> Poll<Result<(), PanicCause>> {
    match catch_unwind(AssertUnwindSafe(|| {
        action.future.as_mut().unwrap().as_mut().poll(cx)
    })) {
        Err(cause) => Poll::Ready(Err(cause)),
        Ok(Poll::Pending) => Poll::Pending,
        Ok(Poll::Ready(result)) => {
            action.disposition = Some(result.disposition());
            action.result = Some(result);
            action.phase = ActionPhase::ResultSaved;
            Poll::Ready(Ok(()))
        }
    }
}
fn drop_product(action: &mut AutomaticAction) -> Result<(), PanicCause> {
    if let Some(future) = action.future.take() {
        catch_unwind(AssertUnwindSafe(|| drop(future)))?;
    }
    Ok(())
}

pub(super) fn poll_automatic_action(
    original: &Mutex<AutomaticPassSlot>,
    readiness: &MvReadinessPort,
    connector_control: &dyn ConnectorControlRegistry,
    cx: &mut Context<'_>,
) -> Poll<Result<AutomaticProductSuccess, MvBackgroundEngineError>> {
    let mut loan = AutomaticPassLoan::take(original);
    let action = loan
        .body_mut()
        .action
        .as_mut()
        .expect("installed original action");
    if action.phase == ActionPhase::Poisoned {
        publish_health_after_restore(loan);
    }
    if !action.dispatch_attempted {
        action.dispatch_attempted = true;
        let dispatched = catch_unwind(AssertUnwindSafe(|| {
            action.prepared.as_mut().unwrap().mark_dispatched()
        }));
        match dispatched {
            Err(cause) => {
                poison(action, cause);
                publish_health_after_restore(loan);
            }
            Ok(Err(error)) => {
                // No Product poll occurred. Preserve the original business
                // error while actually dropping the unpolled F and lease.
                action.terminal = Some(Err(error));
                action.phase = ActionPhase::TerminalSaved;
            }
            Ok(Ok(())) => action.phase = ActionPhase::Running,
        }
    }
    if action.phase == ActionPhase::Running {
        match poll_product(action, cx) {
            Poll::Ready(Err(cause)) => {
                poison(action, cause);
                publish_health_after_restore(loan);
            }
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(())) => {}
        }
    }
    // Ready result, disposition and PreparedAction are stable BEFORE Drop.
    if let Err(cause) = drop_product(action) {
        poison(action, cause);
        publish_health_after_restore(loan);
    }
    if action.phase == ActionPhase::ResultSaved {
        action.phase = ActionPhase::FutureDropped;
    }
    if action.phase == ActionPhase::FutureDropped && !action.terminal_attempted {
        action.terminal_attempted = true;
        let disposition = action
            .disposition
            .expect("saved actual Product disposition");
        match catch_unwind(AssertUnwindSafe(|| {
            action.prepared.as_mut().unwrap().record_terminal(
                disposition,
                readiness,
                connector_control,
            )
        })) {
            Err(cause) => {
                poison(action, cause);
                publish_health_after_restore(loan);
            }
            Ok(terminal) => {
                action.terminal = Some(terminal);
                action.phase = ActionPhase::TerminalSaved;
            }
        }
    }
    assert_eq!(action.phase, ActionPhase::TerminalSaved);
    let prepared = action.prepared.take();
    if let Err(cause) = catch_unwind(AssertUnwindSafe(|| drop(prepared))) {
        poison(action, cause);
        publish_health_after_restore(loan);
    }
    let terminal = action
        .terminal
        .take()
        .expect("saved original terminal result");
    let result = action.result.take();
    // All original Product and lease objects actually exited. A normal domain
    // Err is still a terminal business outcome, not a health poison.
    let retired = loan.body_mut().action.take();
    drop(retired);
    Poll::Ready(match result {
        Some(result) => result.finish(terminal),
        None => Err(terminal.err().expect("pre-dispatch original error")),
    })
}

pub(super) fn finish_automatic_pass(original: &Mutex<AutomaticPassSlot>) {
    let work = {
        let mut loan = AutomaticPassLoan::take(original);
        assert!(
            loan.body().action.is_none(),
            "unproven Product must retain original Work"
        );
        loan.body_mut()
            .work
            .take()
            .expect("same original Work released once")
    };
    // Same original normal-release author, after actual Product/lease exit.
    finish_automatic_root(work.owner, work.query_concurrency);
    drop(work.result_capacity);
}

#[cfg(test)]
#[path = "automatic_custody_tests.rs"]
mod tests;
