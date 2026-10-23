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

//! Stable custody for the original HTTP process state across one synchronous poll.
//! The role root retains the cell and the sole driver join. This is not a root,
//! executor, registry, or recovery policy. The driver must not re-poll a future
//! whose poll unwound; a returned box only preserves its original responsibility.

use std::sync::{Mutex, MutexGuard, PoisonError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct HttpProcessCellBusy;

/// Private sharing, if needed, is `Arc<HttpProcessCell<T>>` held by the original
/// root and its one driver. The cell deliberately implements neither Clone nor
/// an asynchronous access method.
pub(super) struct HttpProcessCell<T> {
    state: Mutex<Option<Box<T>>>,
}

impl<T> HttpProcessCell<T> {
    pub(super) fn new(state: Box<T>) -> Self {
        Self {
            state: Mutex::new(Some(state)),
        }
    }

    /// Borrow the original state for one synchronous operation, normally exactly
    /// one poll. The lease never crosses await. All foreign poll, wake and Drop
    /// code executes without the custody mutex held. Busy does not queue a waiter
    /// or install replacement state; the single driver owns its continuation.
    pub(super) fn with_state<R>(
        &self,
        operation: impl FnOnce(&mut T) -> R,
    ) -> Result<R, HttpProcessCellBusy> {
        let state = {
            let mut slot = self.slot();
            slot.take().ok_or(HttpProcessCellBusy)?
        };
        let mut lease = PollLease {
            cell: self,
            state: Some(state),
        };
        let result = operation(
            lease
                .state
                .as_deref_mut()
                .expect("HTTP poll lease has its state"),
        );
        drop(lease);
        Ok(result)
    }

    /// Root-only final access. The caller must first observe the original driver
    /// join, end all producer/view access and recover unique ownership (for an
    /// Arc-backed cell, via Arc::try_unwrap). A successful Arc unwrap is not a
    /// substitute for observing that join. No shutdown or failure is synthesized.
    pub(super) fn into_state(self) -> Box<T> {
        self.state
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .expect("uniquely owned HTTP process cell has its state")
    }

    fn slot(&self) -> MutexGuard<'_, Option<Box<T>>> {
        // This mutex protects only Option moves. Foreign operations cannot poison
        // it; recovering custody metadata never retries a foreign operation.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct PollLease<'a, T> {
    cell: &'a HttpProcessCell<T>,
    state: Option<Box<T>>,
}

impl<T> Drop for PollLease<'_, T> {
    fn drop(&mut self) {
        let state = self
            .state
            .take()
            .expect("HTTP poll lease returns its original state once");
        let previous = {
            let mut slot = self.cell.slot();
            slot.replace(state)
        };
        // The only taker is with_state and only its lease can return the box.
        // Even an internal invariant failure drops no foreign state under lock.
        assert!(
            previous.is_none(),
            "HTTP process cell already contains state during lease return"
        );
    }
}

#[cfg(test)]
#[path = "process_cell_tests.rs"]
mod tests;
