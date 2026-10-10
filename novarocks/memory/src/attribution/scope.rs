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

//! Synchronous attribution steps; bindings never escape through a public guard.
use super::binding;
use crate::lane::{LaneHandle, RecordRef};
use std::{marker::PhantomData, rc::Rc};

/// Exact entry branch of this synchronous observation attempt. This fact
/// neither authorizes memory nor rejects the work submitted to `run`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AmbientEntryObservation {
    #[default]
    NotAttempted,
    Bound,
    LaneEntryRefused,
    TlsUnavailable,
}

/// Exact restoration branch, after the body returned or unwound. A failed
/// entry retains its outer binding and does not attempt restoration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AmbientExitObservation {
    #[default]
    NotExited,
    EntryWasUnbound,
    Restored,
    RestoreTlsUnavailable,
}

/// Caller-owned, fixed storage for one synchronous observation attempt.
/// Reusing it starts a new observation; copy the completed facts first if
/// they must be retained. Normal return and unwind write the same storage.
/// This is coverage evidence, not a funding balance or an execution error.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AmbientStepObservation {
    entry: AmbientEntryObservation,
    exit: AmbientExitObservation,
}
impl AmbientStepObservation {
    pub const fn entry(&self) -> AmbientEntryObservation {
        self.entry
    }
    pub const fn exit(&self) -> AmbientExitObservation {
        self.exit
    }
}

// Private and thread-bound: every public entry point completes this guard on
// normal return or unwind before returning control to a caller or executor.
pub(crate) struct AmbientStep<'lane, 'observation> {
    lane: &'lane LaneHandle,
    previous: RecordRef,
    observation: Option<&'observation mut AmbientStepObservation>,
    _thread: PhantomData<Rc<()>>,
}
impl<'lane, 'observation> AmbientStep<'lane, 'observation> {
    fn enter(
        lane: &'lane LaneHandle,
        mut observation: Option<&'observation mut AmbientStepObservation>,
    ) -> Option<Self> {
        if let Some(facts) = observation.as_deref_mut() {
            *facts = AmbientStepObservation::default();
        }
        if !lane.enter() {
            lane.store().faults.binding_failed();
            if let Some(facts) = observation.as_deref_mut() {
                facts.entry = AmbientEntryObservation::LaneEntryRefused;
                facts.exit = AmbientExitObservation::EntryWasUnbound;
            }
            return None;
        }
        // SAFETY: lane and every outer guard retain their process-store owners;
        // this private guard restores on the same synchronous stack.
        let Some(previous) = (unsafe { binding::try_install_ambient(lane.reference()) }) else {
            lane.leave();
            lane.store().faults.binding_failed();
            if let Some(facts) = observation.as_deref_mut() {
                facts.entry = AmbientEntryObservation::TlsUnavailable;
                facts.exit = AmbientExitObservation::EntryWasUnbound;
            }
            return None;
        };
        if let Some(facts) = observation.as_deref_mut() {
            facts.entry = AmbientEntryObservation::Bound;
        }
        Some(Self {
            lane,
            previous,
            observation,
            _thread: PhantomData,
        })
    }
}
impl Drop for AmbientStep<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: same thread, exact saved binding, owners still retained.
        let restored = unsafe { binding::try_restore_ambient(self.previous) };
        self.lane.leave();
        if let Some(facts) = self.observation.as_deref_mut() {
            facts.exit = if restored {
                AmbientExitObservation::Restored
            } else {
                AmbientExitObservation::RestoreTlsUnavailable
            };
        }
    }
}
impl LaneHandle {
    /// Attributes one synchronous step. Do not put an await inside the step:
    /// constructing/spawning a future does not propagate this binding to polls.
    /// Sealed lanes or unavailable TLS record a coverage failure and still run
    /// the closure with its existing outer binding; this is not admission.
    pub fn run<T>(&self, step: impl FnOnce() -> T) -> T {
        self.run_inner(None, step)
    }

    /// Runs with the same policy as `run`, retaining the exact coverage
    /// branches in caller-owned storage. No callback, allocation, refusal,
    /// formatting, or process-counter differencing produces these facts.
    /// The storage remains available to an outer panic owner after unwind.
    pub fn run_observed<T>(
        &self,
        observation: &mut AmbientStepObservation,
        step: impl FnOnce() -> T,
    ) -> T {
        self.run_inner(Some(observation), step)
    }

    fn run_inner<T>(
        &self,
        observation: Option<&mut AmbientStepObservation>,
        step: impl FnOnce() -> T,
    ) -> T {
        let _guard = AmbientStep::enter(self, observation);
        step()
    }
}
