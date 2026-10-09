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

//! Explicit, default-off fixture composition capability. No environment or task spawning.

use crate::listener::MysqlFixtureSessionJoins;
use crate::mysql_write_gate::GateFailure;
use crate::mysql_write_gate::late_binding::MysqlWriteGateHub;
use crate::mysql_write_gate::unix_control::{
    ControlExitError, ControlFailure, UnixMysqlWriteControl,
};
use novarocks_types::FrontendProcessId;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinError;

/// Moved once into the original listener; grants no controller or token-minting authority.
pub struct MysqlWriteFixtureListenerBinding {
    pub(crate) hub: Arc<MysqlWriteGateHub>,
    pub(crate) joins: Arc<MysqlFixtureSessionJoins>,
}
/// Read-only wakeup for the original task ledger; grants no join or cancel authority.
pub struct MysqlWriteFixtureFailureObservation(Arc<MysqlFixtureSessionJoins>);
impl MysqlWriteFixtureFailureObservation {
    pub async fn wait_for_failure(&self) {
        self.0.wait_for_failure().await;
    }
}

/// The original FE service owner keeps this outside the borrowed control future.
pub struct MysqlWriteFixture {
    control: UnixMysqlWriteControl,
    joins: Arc<MysqlFixtureSessionJoins>,
    binding: Option<MysqlWriteFixtureListenerBinding>,
}

/// Retains actual failures until the service owner combines its final verdict.
/// Display contains fixed classes and counters, never original panic/input text.
#[derive(Default)]
pub struct MysqlWriteFixtureError {
    control: Option<ControlExitError>,
    close: Option<ControlFailure>,
    join: Option<JoinError>,
    watcher_join: Option<JoinError>,
    protocol: Option<crate::listener::MysqlFixtureProtocolFailure>,
    prescribed_eof: Option<crate::listener::MysqlFixtureProtocolFailure>,
    gate: Option<io::Error>,
    invalid_facts: bool,
    aborted_sessions: u64,
    counter_overflow: bool,
    first_gate_failure: Option<GateFailure>,
    watcher_facts: Option<crate::listener::WatcherFacts>,
}
impl fmt::Debug for MysqlWriteFixtureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for MysqlWriteFixtureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exact MySQL fixture failure")?;
        if let Some(error) = &self.control {
            write!(f, "; control: {error}")?;
        }
        if let Some(error) = &self.close {
            write!(f, "; close: {error}")?;
        }
        if let Some(error) = &self.join {
            write!(
                f,
                "; original_join panic={} cancelled={}",
                error.is_panic(),
                error.is_cancelled()
            )?;
        }
        if let Some(error) = &self.watcher_join {
            write!(
                f,
                "; original_watcher_join panic={} cancelled={}",
                error.is_panic(),
                error.is_cancelled()
            )?;
        }
        if let Some(error) = &self.protocol {
            write!(f, "; {error}")?;
        }
        if let Some(error) = &self.prescribed_eof {
            write!(f, "; retained prescribed exit: {error}")?;
        }
        if let Some(facts) = &self.watcher_facts {
            write!(f, "; original_watchers={facts:?}")?;
        }
        if let Some(error) = &self.gate {
            write!(
                f,
                "; gate io_kind={:?} raw_os={:?}",
                error.kind(),
                error.raw_os_error()
            )?;
        }
        write!(
            f,
            "; invalid_facts={} aborted_sessions={} counter_overflow={} first_gate_failure={:?}",
            self.invalid_facts,
            self.aborted_sessions,
            self.counter_overflow,
            self.first_gate_failure
        )
    }
}
impl std::error::Error for MysqlWriteFixtureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(error) = &self.control {
            return Some(error);
        }
        if let Some(error) = &self.close {
            return Some(error);
        }
        if let Some(error) = &self.join {
            return Some(error);
        }
        if let Some(error) = &self.watcher_join {
            return Some(error);
        }
        if let Some(error) = &self.protocol {
            return Some(error);
        }
        if let Some(error) = &self.gate {
            return Some(error);
        }
        self.prescribed_eof.as_ref().map(|error| error as _)
    }
}
impl MysqlWriteFixture {
    pub fn failure_observation(&self) -> MysqlWriteFixtureFailureObservation {
        MysqlWriteFixtureFailureObservation(Arc::clone(&self.joins))
    }
    pub fn bind(
        path: PathBuf,
        actual_frontend: FrontendProcessId,
        nonce: [u8; 16],
        original_absolute_deadline: Instant,
    ) -> Result<Self, MysqlWriteFixtureError> {
        let (control, hub) = UnixMysqlWriteControl::bind_v2(
            path,
            actual_frontend,
            nonce,
            original_absolute_deadline,
        )
        .map_err(|control| MysqlWriteFixtureError {
            control: Some(control),
            ..Default::default()
        })?;
        let joins = Arc::new(MysqlFixtureSessionJoins::default());
        Ok(Self {
            control,
            binding: Some(MysqlWriteFixtureListenerBinding {
                hub,
                joins: Arc::clone(&joins),
            }),
            joins,
        })
    }
    pub fn listener_binding(
        &mut self,
    ) -> Result<MysqlWriteFixtureListenerBinding, MysqlWriteFixtureError> {
        self.binding.take().ok_or(MysqlWriteFixtureError {
            invalid_facts: true,
            ..Default::default()
        })
    }
    /// A successful Stop completes only control protocol; keep supervising original MySQL.
    pub async fn run_control(&mut self) -> Result<(), MysqlWriteFixtureError> {
        self.control
            .run()
            .await
            .map(|_| ())
            .map_err(|control| MysqlWriteFixtureError {
                control: Some(control),
                ..Default::default()
            })
    }
    /// On a selected sibling failure, drop the run borrow before calling this.
    pub fn fail_and_stop(&mut self) {
        self.control.fail(GateFailure::Transition);
        self.control.stop();
    }
    pub fn close_control(&mut self) -> Result<(), MysqlWriteFixtureError> {
        self.control
            .close()
            .map_err(|close| MysqlWriteFixtureError {
                close: Some(close),
                ..Default::default()
            })
    }
    /// Observe the original registry's owner drain after the original listener completes.
    pub async fn verify_original_connection_drain(
        &self,
        connections: &crate::MysqlClientConnectionRegistry,
    ) -> Result<(), MysqlWriteFixtureError> {
        // The adapter already used the original bounded drain. Never restart its
        // clock or turn a timeout into an unbounded second wait.
        let drained = connections.wait_drained();
        tokio::pin!(drained);
        let complete = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(drained.as_mut(), cx).is_ready())
        })
        .await;
        if complete {
            Ok(())
        } else {
            Err(MysqlWriteFixtureError {
                invalid_facts: true,
                ..Default::default()
            })
        }
    }
    /// Caller must have awaited the original listener and connection registry drain.
    /// Writer Drop and join counters alone are never task-exit evidence.
    pub fn finish_after_original_listener_join(&mut self) -> Result<(), MysqlWriteFixtureError> {
        let joins = self.joins.snapshot();
        let watchers = self.joins.watcher_snapshot();
        let hub = self.control.hub_snapshot();
        let facts = self.control.facts();
        let invalid_facts = watchers.reserved != 0
            || watchers.failed()
            || self.binding.is_some()
            || !facts.explicit_stop
            || !hub.used_arm
            || hub.failure.is_some()
            || !hub.original_writer_exited
            || !hub.gate.is_some_and(|gate| {
                gate.failure.is_none() && gate.cancel_receipt.is_some() && gate.writer_exited
            });
        let join = self.joins.take_failure_after_join();
        let protocol = self.joins.take_protocol_failure_after_join();
        let prescribed_eof = self.joins.take_prescribed_eof_after_join();
        let invalid_prescribed = match (&prescribed_eof, joins.prescribed_protocol_eofs) {
            (None, 0) => false,
            (Some(exit), 1) => !hub.gate.is_some_and(|gate| {
                crate::mysql_write_gate::late_binding::PrescribedRelayEof::from_error(&exit.cause)
                    .is_some_and(|eof| eof.matches_gate(&gate))
            }),
            _ => true,
        };
        let watcher_join = self.joins.take_watcher_failure_after_join();
        let gate = self.control.finish_after_protocol_join().err();
        if invalid_facts
            || invalid_prescribed
            || join.is_some()
            || watcher_join.is_some()
            || protocol.is_some()
            || joins.protocol_io_failures != 0
            || gate.is_some()
            || joins.aborted != 0
            || joins.counter_overflow
        {
            return Err(MysqlWriteFixtureError {
                invalid_facts: invalid_facts || invalid_prescribed,
                join,
                watcher_join,
                protocol,
                prescribed_eof,
                gate,
                aborted_sessions: joins.aborted,
                counter_overflow: joins.counter_overflow,
                first_gate_failure: hub
                    .failure
                    .or_else(|| hub.gate.and_then(|gate| gate.failure)),
                watcher_facts: Some(watchers),
                ..Default::default()
            });
        }
        Ok(())
    }
}
