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
//! Bounded, original-owner custody for the three named completion roles.
//!
//! This is not a destructor safety boundary: panic=abort and a destructor's
//! hidden double panic cannot be recovered by catch_unwind. An opaque payload
//! is never inspected or destroyed. Its original role is quarantined instead.

use std::any::Any;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};

pub(crate) type Payload = ManuallyDrop<Box<dyn Any + Send>>;
pub(crate) type Callback<F> = Box<dyn FnMut(&F) -> bool + Send + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Role {
    PipelineFinisher,
    WallTime,
    TerminalPublisher,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Phase {
    Call,
    CaptureDrop,
    ResourceHook,
    ResourceHolderDrop,
    CleanupNotification,
}

#[derive(Debug)]
pub(crate) struct CompletionFailure {
    pub(crate) role: Role,
    pub(crate) phase: Phase,
}
impl std::fmt::Display for CompletionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} {:?} panicked; its original exit scope is unresolved",
            self.role, self.phase
        )
    }
}
impl std::error::Error for CompletionFailure {}
impl FailureProjection for CompletionFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match (self.role, self.phase) {
            (_, Phase::CleanupNotification) => ExecutionReason::PipelineFinisherCallPanicked,
            (_, Phase::ResourceHook) => ExecutionReason::FragmentResourceHookPanicked,
            (_, Phase::ResourceHolderDrop) => ExecutionReason::FragmentResourceHolderDropPanicked,
            (Role::PipelineFinisher, Phase::Call) => ExecutionReason::PipelineFinisherCallPanicked,
            (Role::PipelineFinisher, Phase::CaptureDrop) => {
                ExecutionReason::PipelineFinisherCaptureDropPanicked
            }
            (Role::WallTime, Phase::Call) => ExecutionReason::WallTimeCallPanicked,
            (Role::WallTime, Phase::CaptureDrop) => ExecutionReason::WallTimeCaptureDropPanicked,
            (Role::TerminalPublisher, Phase::Call) => {
                ExecutionReason::TerminalPublisherCallPanicked
            }
            (Role::TerminalPublisher, Phase::CaptureDrop) => {
                ExecutionReason::TerminalPublisherCaptureDropPanicked
            }
        };
        FailureFact::new(FailureClass::Unexpected, reason.reason())
    }
}

#[derive(Debug)]
struct BindingConflict(Role, bool);
impl std::fmt::Display for BindingConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} completion role {}",
            self.0,
            if self.1 {
                "requires its original callback"
            } else {
                "is already bound"
            }
        )
    }
}
impl std::error::Error for BindingConflict {}
impl FailureProjection for BindingConflict {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::CompletionRoleBindingConflict.reason(),
        )
    }
}

struct State<F> {
    callback: Option<ManuallyDrop<Callback<F>>>,
    bound: bool,
    call_started: bool,
    disposed: bool,
    call_returned: bool,
    dispose_requested: bool,
    call_payload: Option<Payload>,
    dispose_payload: Option<Payload>,
    // Reporting is metadata too. A broken reporter must not recursively report
    // itself or destroy a second opaque payload during unwind.
    report_payloads: [[Option<Payload>; 2]; 2],
}

pub(crate) struct OnceRole<F> {
    state: Mutex<State<F>>,
    role: Role,
    quarantine: Mutex<Option<Arc<Self>>>,
}

impl<F: Send + 'static> OnceRole<F> {
    pub(crate) fn new(role: Role) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                callback: None,
                bound: false,
                call_started: false,
                disposed: false,
                call_returned: false,
                dispose_requested: false,
                call_payload: None,
                dispose_payload: None,
                report_payloads: [[None, None], [None, None]],
            }),
            role,
            quarantine: Mutex::new(None),
        })
    }

    /// A failed binding leaves the original callback in the caller's slot.
    /// No rejected foreign capture is destroyed by this API.
    pub(crate) fn bind(self: &Arc<Self>, slot: &mut Option<Callback<F>>) -> ExecutionResult<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.bound {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, false,
            )));
        }
        if slot.is_none() {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, true,
            )));
        }
        state.callback = slot.take().map(ManuallyDrop::new);
        state.bound = true;
        self.retain();
        drop(state);
        Ok(())
    }

    pub(crate) fn bind_notification(
        self: &Arc<Self>,
        slot: &mut Option<Box<dyn FnMut(&F) + Send + 'static>>,
    ) -> ExecutionResult<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.bound {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, false,
            )));
        }
        if slot.is_none() {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, true,
            )));
        }
        state.callback = slot.take().map(|mut callback| {
            ManuallyDrop::new(Box::new(move |fact: &F| {
                callback(fact);
                true
            }) as Callback<F>)
        });
        state.bound = true;
        self.retain();
        drop(state);
        Ok(())
    }

    /// A concurrent resource interruption can finish after the one logical
    /// stopped call. Its original owner supplies that independent exit proof.
    pub(crate) fn complete_deferred(self: &Arc<Self>, control: &RuntimeErrorState) {
        let returned = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.dispose_requested = true;
            state.call_returned
        };
        if returned {
            self.dispose(control);
        }
    }

    pub(crate) fn invoke(self: &Arc<Self>, fact: &F, control: &RuntimeErrorState) {
        let mut callback = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.call_started || !state.bound {
                return;
            }
            state.call_started = true;
            state.callback.take()
        };
        let Some(mut callback_value) = callback.take() else {
            return;
        };
        let result = catch_unwind(AssertUnwindSafe(|| callback_value(fact)));
        // Restore the original capture before diagnostics can invoke any host.
        let call_completed = matches!(result, Ok(true));
        let call_panicked = result.is_err();
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.callback = Some(callback_value);
            state.call_returned = true;
            if let Err(payload) = result {
                state.call_payload = Some(ManuallyDrop::new(payload));
            }
        }
        if !call_completed {
            self.retain();
        }
        if call_panicked {
            self.report(control, Phase::Call);
        }
        // An unresolved resource finisher still owns its real holders. A pure
        // metadata role can retire its capture independently of notification.
        let dispose_requested = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .dispose_requested;
        if call_completed
            || dispose_requested
            || (call_panicked && self.role != Role::PipelineFinisher)
        {
            self.dispose(control);
        }
    }

    fn dispose(self: &Arc<Self>, control: &RuntimeErrorState) {
        let callback = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.disposed {
                return;
            }
            state.disposed = true;
            state.callback.take()
        };
        if let Some(callback) = callback {
            let result = catch_unwind(AssertUnwindSafe(|| {
                drop(ManuallyDrop::into_inner(callback))
            }));
            if let Err(payload) = result {
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .dispose_payload = Some(ManuallyDrop::new(payload));
                self.retain();
                self.report(control, Phase::CaptureDrop);
            } else {
                let no_payload = {
                    let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.call_payload.is_none()
                        && state
                            .report_payloads
                            .iter()
                            .all(|phase| phase.iter().all(Option::is_none))
                };
                if no_payload {
                    let pin = self
                        .quarantine
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take();
                    drop(pin);
                }
            }
        }
    }

    fn retain(self: &Arc<Self>) {
        let mut pin = self.quarantine.lock().unwrap_or_else(|e| e.into_inner());
        if pin.is_none() {
            *pin = Some(Arc::clone(self));
        }
    }

    fn report(self: &Arc<Self>, control: &RuntimeErrorState, phase: Phase) {
        let cause = ExecutionFailure::from_domain(CompletionFailure {
            role: self.role,
            phase,
        });
        let report_phase = match phase {
            Phase::Call => 0,
            Phase::CaptureDrop => 1,
            _ => unreachable!("named role reporting phase"),
        };
        for (ordinal, report) in [
            RuntimeErrorState::report_cleanup as fn(&RuntimeErrorState, &ExecutionFailure),
            RuntimeErrorState::report_unresolved_exit,
        ]
        .into_iter()
        .enumerate()
        {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| report(control, &cause))) {
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .report_payloads[report_phase][ordinal] = Some(ManuallyDrop::new(payload));
                self.retain();
            }
        }
    }
}
