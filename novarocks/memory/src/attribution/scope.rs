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

// Private and thread-bound: every public entry point completes this guard on
// normal return or unwind before returning control to a caller or executor.
pub(crate) struct AmbientStep<'a> {
    lane: &'a LaneHandle,
    previous: RecordRef,
    _thread: PhantomData<Rc<()>>,
}
impl<'a> AmbientStep<'a> {
    fn enter(lane: &'a LaneHandle) -> Option<Self> {
        if !lane.enter() {
            lane.store().faults.binding_failed();
            return None;
        }
        // SAFETY: lane and every outer guard retain their process-store owners;
        // this private guard restores on the same synchronous stack.
        let Some(previous) = (unsafe { binding::try_install_ambient(lane.reference()) }) else {
            lane.leave();
            lane.store().faults.binding_failed();
            return None;
        };
        Some(Self {
            lane,
            previous,
            _thread: PhantomData,
        })
    }
}
impl Drop for AmbientStep<'_> {
    fn drop(&mut self) {
        // SAFETY: same thread, exact saved binding, owners still retained.
        unsafe { binding::restore_ambient(self.previous) };
        self.lane.leave();
    }
}
impl LaneHandle {
    /// Attributes one synchronous step. Do not put an await inside the step:
    /// constructing/spawning a future does not propagate this binding to polls.
    /// Sealed lanes or unavailable TLS record a coverage failure and still run
    /// the closure with its existing outer binding; this is not admission.
    pub fn run<T>(&self, step: impl FnOnce() -> T) -> T {
        let _guard = AmbientStep::enter(self);
        step()
    }
}
