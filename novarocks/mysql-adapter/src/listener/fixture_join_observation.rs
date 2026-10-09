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

//! Opt-in observations from the listener's original session JoinSet.
//! Keeps one actual JoinError; counters grant no ownership or task-exit authority.

mod watcher_join_owner;
pub(crate) use watcher_join_owner::{
    WatcherAbortGuard, WatcherExitKind, WatcherFacts, WatcherPermit,
};

use std::sync::{Arc, Mutex};
use tokio::task::JoinError;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MysqlFixtureJoinFacts {
    pub joined: u64,
    pub succeeded: u64,
    pub panicked: u64,
    pub unexpected_cancelled: u64,
    pub aborted: u64,
    pub counter_overflow: bool,
}
#[derive(Default)]
struct State {
    facts: MysqlFixtureJoinFacts,
    first_failure: Option<JoinError>,
}
#[derive(Default)]
pub(crate) struct MysqlFixtureSessionJoins {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
    watchers: Arc<watcher_join_owner::OriginalWatcherOwner>,
}
fn increment(value: &mut u64) -> bool {
    match value.checked_add(1) {
        Some(next) => {
            *value = next;
            false
        }
        None => true,
    }
}
impl MysqlFixtureSessionJoins {
    pub(crate) fn reserve_watcher(
        self: &Arc<Self>,
        owner: Arc<crate::connection_registry::RegisteredConnectionLifetime>,
    ) -> std::io::Result<WatcherPermit> {
        self.watchers.reserve(owner)
    }
    pub(crate) fn poll_next_watcher(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<WatcherExitKind>> {
        self.watchers.poll_next(cx)
    }
    pub(crate) async fn next_watcher(&self) -> Option<WatcherExitKind> {
        self.watchers.next().await
    }
    pub(crate) fn abort_remaining_watchers(&self) {
        self.watchers.abort_remaining();
    }
    pub(crate) fn watchers_empty(&self) -> bool {
        self.watchers.is_empty()
    }
    pub(crate) fn watcher_snapshot(&self) -> WatcherFacts {
        self.watchers.snapshot()
    }
    pub(crate) fn take_watcher_failure_after_join(&self) -> Option<JoinError> {
        self.watchers.take_failure_after_join()
    }
    pub(crate) fn observe(&self, result: Result<(), JoinError>, aborting: bool) {
        let mut state = self
            .state
            .lock()
            .expect("MySQL fixture join observation lock");
        let mut overflow = increment(&mut state.facts.joined);
        match result {
            Ok(()) => overflow |= increment(&mut state.facts.succeeded),
            Err(error) => {
                if error.is_cancelled() && aborting {
                    overflow |= increment(&mut state.facts.aborted);
                } else {
                    if error.is_panic() {
                        overflow |= increment(&mut state.facts.panicked);
                    } else {
                        overflow |= increment(&mut state.facts.unexpected_cancelled);
                    }
                    // Move the original joined failure; never reconstruct it from a log/marker.
                    if state.first_failure.is_none() {
                        state.first_failure = Some(error);
                    }
                }
            }
        }
        state.facts.counter_overflow |= overflow;
        drop(state);
        self.changed.notify_waiters();
    }
    pub(crate) fn snapshot(&self) -> MysqlFixtureJoinFacts {
        self.state
            .lock()
            .expect("MySQL fixture join observation lock")
            .facts
    }
    /// The service owner takes this only after awaiting the original listener and owner drain.
    pub(crate) fn take_failure_after_join(&self) -> Option<JoinError> {
        self.state
            .lock()
            .expect("MySQL fixture join observation lock")
            .first_failure
            .take()
    }
    pub(crate) async fn wait_for_failure(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let facts = self.snapshot();
            if facts.panicked != 0 || facts.unexpected_cancelled != 0 || facts.counter_overflow {
                return;
            }
            tokio::select! {
                _ = changed => {}
                _ = self.watchers.wait_for_failure() => return,
            }
        }
    }
}
