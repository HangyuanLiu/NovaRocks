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

//! Query-owned staging primitives.
//!
//! QLC-3 deliberately separates preparing dormant fragment handles from
//! starting them.  A single [`StartGate`] is shared by every worker in one
//! staged participant bundle; no worker may start until the registry has
//! committed `Staged -> Running` and released this gate.

use std::sync::{Condvar, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartGateState {
    Pending,
    Released,
    Aborted,
}

/// A one-way, query-owned start decision.
///
/// The gate has no reset operation.  This makes duplicate Start and Abort
/// races harmless: exactly one terminal decision wins, and waiters always see
/// the same result.
pub(crate) struct StartGate {
    state: Mutex<StartGateState>,
    changed: Condvar,
}

impl Default for StartGate {
    fn default() -> Self {
        Self::new()
    }
}

impl StartGate {
    pub(crate) const fn new() -> Self {
        Self {
            state: Mutex::new(StartGateState::Pending),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn state(&self) -> StartGateState {
        *self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Releases prepared workers. Returns true only for the first release.
    pub(crate) fn release(&self) -> bool {
        self.transition(StartGateState::Released)
    }

    /// Wakes prepared workers without starting them. Returns true only for
    /// the first terminal decision.
    pub(crate) fn abort(&self) -> bool {
        self.transition(StartGateState::Aborted)
    }

    pub(crate) fn wait(&self) -> StartGateState {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while *state == StartGateState::Pending {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        *state
    }

    fn transition(&self, next: StartGateState) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if *state != StartGateState::Pending {
            return false;
        }
        *state = next;
        self.changed.notify_all();
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::{StartGate, StartGateState};

    #[test]
    fn release_is_one_way_and_idempotent() {
        let gate = StartGate::new();

        assert_eq!(gate.state(), StartGateState::Pending);
        assert!(gate.release());
        assert_eq!(gate.wait(), StartGateState::Released);
        assert!(!gate.release());
        assert!(!gate.abort());
        assert_eq!(gate.state(), StartGateState::Released);
    }

    #[test]
    fn abort_wakes_waiters_without_releasing_them() {
        let gate = Arc::new(StartGate::new());
        let waiter = Arc::clone(&gate);
        let (sender, receiver) = mpsc::channel();
        let join = std::thread::spawn(move || sender.send(waiter.wait()).expect("send result"));

        assert!(gate.abort());
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("waiter wakes"),
            StartGateState::Aborted
        );
        join.join().expect("waiter joins");
        assert!(!gate.release());
    }
}
