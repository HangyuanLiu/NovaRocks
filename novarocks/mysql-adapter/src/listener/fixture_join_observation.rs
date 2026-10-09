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

use crate::connection_registry::MysqlConnectionClass;
use novarocks_query_application::client_connection::ClientConnectionToken;
use std::sync::{Arc, Mutex};
use tokio::task::JoinError;

pub(crate) struct MysqlFixtureProtocolFailure {
    pub connection: ClientConnectionToken,
    pub class: MysqlConnectionClass,
    pub cause: std::io::Error,
}
impl std::fmt::Display for MysqlFixtureProtocolFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original MySQL protocol IO failure: connection_id={} generation={} class={:?} io_kind={:?} raw_os={:?}",
            self.connection.connection_id(),
            self.connection.generation(),
            self.class,
            self.cause.kind(),
            self.cause.raw_os_error()
        )
    }
}
impl std::fmt::Debug for MysqlFixtureProtocolFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for MysqlFixtureProtocolFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MysqlFixtureJoinFacts {
    pub joined: u64,
    pub succeeded: u64,
    pub panicked: u64,
    pub unexpected_cancelled: u64,
    pub aborted: u64,
    pub counter_overflow: bool,
    pub protocol_io_failures: u64,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub prescribed_protocol_eofs: u64,
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub pressure_capacity_eofs: u64,
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub listener_io_failures: u64,
}
#[derive(Default)]
struct State {
    facts: MysqlFixtureJoinFacts,
    first_failure: Option<JoinError>,
    first_protocol_failure: Option<MysqlFixtureProtocolFailure>,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    first_prescribed_eof: Option<MysqlFixtureProtocolFailure>,
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pressure_enabled: bool,
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    first_pressure_eof: Option<MysqlFixtureProtocolFailure>,
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    first_listener_failure: Option<std::io::Error>,
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
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn for_closing_pressure() -> Self {
        Self {
            state: Mutex::new(State {
                pressure_enabled: true,
                ..State::default()
            }),
            ..Self::default()
        }
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn is_closing_pressure(&self) -> bool {
        self.state
            .lock()
            .expect("MySQL fixture join observation lock")
            .pressure_enabled
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn observe_listener_failure(&self, cause: std::io::Error) {
        let mut state = self
            .state
            .lock()
            .expect("MySQL fixture join observation lock");
        let overflow = increment(&mut state.facts.listener_io_failures);
        state.facts.counter_overflow |= overflow;
        if state.first_listener_failure.is_none() {
            state.first_listener_failure = Some(cause);
        }
        drop(state);
        self.changed.notify_waiters();
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn take_listener_failure_after_join(&self) -> Option<std::io::Error> {
        self.state
            .lock()
            .expect("MySQL fixture join observation lock")
            .first_listener_failure
            .take()
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn take_pressure_eof_after_join(&self) -> Option<MysqlFixtureProtocolFailure> {
        self.state
            .lock()
            .expect("MySQL fixture join observation lock")
            .first_pressure_eof
            .take()
    }
    pub(crate) fn observe_protocol_failure(
        &self,
        connection: ClientConnectionToken,
        class: MysqlConnectionClass,
        cause: std::io::Error,
    ) {
        let mut state = self
            .state
            .lock()
            .expect("MySQL fixture join observation lock");
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        if state.pressure_enabled
            && class == MysqlConnectionClass::Ordinary
            && state.facts.pressure_capacity_eofs == 0
            && crate::closing_pressure_gate::relay::PressureCapacityEof::from_error(&cause)
                .is_some_and(|eof| eof.matches(connection))
        {
            let overflow = increment(&mut state.facts.pressure_capacity_eofs);
            state.facts.counter_overflow |= overflow;
            state.first_pressure_eof = Some(MysqlFixtureProtocolFailure {
                connection,
                class,
                cause,
            });
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if {
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            {
                !state.pressure_enabled
            }
            #[cfg(not(feature = "mem-1-m07-closing-pressure"))]
            {
                true
            }
        } && class == MysqlConnectionClass::Ordinary
            && crate::mysql_write_gate::late_binding::PrescribedRelayEof::from_error(&cause)
                .is_some_and(|eof| eof.matches(connection, None))
            && state.facts.prescribed_protocol_eofs == 0
        {
            let overflow = increment(&mut state.facts.prescribed_protocol_eofs);
            state.facts.counter_overflow |= overflow;
            state.first_prescribed_eof = Some(MysqlFixtureProtocolFailure {
                connection,
                class,
                cause,
            });
            drop(state);
            self.changed.notify_waiters();
            return;
        }
        let overflow = increment(&mut state.facts.protocol_io_failures);
        state.facts.counter_overflow |= overflow;
        if state.first_protocol_failure.is_none() {
            state.first_protocol_failure = Some(MysqlFixtureProtocolFailure {
                connection,
                class,
                cause,
            });
        }
        drop(state);
        self.changed.notify_waiters();
    }
    pub(crate) fn take_protocol_failure_after_join(&self) -> Option<MysqlFixtureProtocolFailure> {
        self.state
            .lock()
            .expect("MySQL fixture join observation lock")
            .first_protocol_failure
            .take()
    }
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn take_prescribed_eof_after_join(&self) -> Option<MysqlFixtureProtocolFailure> {
        self.state
            .lock()
            .expect("MySQL fixture join observation lock")
            .first_prescribed_eof
            .take()
    }
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
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            if facts.listener_io_failures != 0 {
                return;
            }
            if facts.panicked != 0
                || facts.unexpected_cancelled != 0
                || facts.counter_overflow
                || facts.protocol_io_failures != 0
            {
                return;
            }
            tokio::select! {
                _ = changed => {}
                _ = self.watchers.wait_for_failure() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[derive(Debug)]
    struct ActualCause(Arc<()>);
    impl std::fmt::Display for ActualCause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private protocol cause must not enter fixture summaries")
        }
    }
    impl Error for ActualCause {}

    #[tokio::test]
    async fn protocol_failure_moves_actual_first_cause_and_wakes_original_ledger() {
        let observation = MysqlFixtureSessionJoins::default();
        let identity = Arc::new(());
        let token = ClientConnectionToken::new(37, 91).unwrap();
        let pending = observation.wait_for_failure();
        tokio::pin!(pending);
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())
            })
            .await
        );
        observation.observe_protocol_failure(
            token,
            MysqlConnectionClass::Control,
            std::io::Error::other(ActualCause(identity.clone())),
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), pending)
            .await
            .unwrap();
        observation.observe_protocol_failure(
            ClientConnectionToken::new(38, 92).unwrap(),
            MysqlConnectionClass::Ordinary,
            std::io::Error::from_raw_os_error(5),
        );
        let actual = observation.take_protocol_failure_after_join().unwrap();
        assert_eq!(actual.connection, token);
        assert_eq!(actual.class, MysqlConnectionClass::Control);
        assert!(Arc::ptr_eq(
            &actual
                .cause
                .get_ref()
                .unwrap()
                .downcast_ref::<ActualCause>()
                .unwrap()
                .0,
            &identity
        ));
        assert!(
            actual
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
        let summary = format!("{actual:?}");
        assert!(summary.contains("connection_id=37 generation=91 class=Control"));
        assert!(!summary.contains("private protocol cause"));
        assert!(observation.take_protocol_failure_after_join().is_none());
        assert_eq!(observation.snapshot().protocol_io_failures, 2);
        assert_eq!(observation.snapshot().joined, 0);
    }

    #[test]
    fn protocol_counter_overflow_is_sticky_without_replacing_original_error() {
        let observation = MysqlFixtureSessionJoins::default();
        observation.state.lock().unwrap().facts.protocol_io_failures = u64::MAX;
        observation.observe_protocol_failure(
            ClientConnectionToken::new(9, 3).unwrap(),
            MysqlConnectionClass::Ordinary,
            std::io::Error::from_raw_os_error(5),
        );
        assert!(observation.snapshot().counter_overflow);
        assert_eq!(observation.snapshot().protocol_io_failures, u64::MAX);
        assert_eq!(
            observation
                .take_protocol_failure_after_join()
                .unwrap()
                .cause
                .raw_os_error(),
            Some(5)
        );
    }
}
