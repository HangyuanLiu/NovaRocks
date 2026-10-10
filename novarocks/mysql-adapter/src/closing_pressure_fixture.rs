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

//! Explicit default-off Closing pressure composition. No environment or task spawning.
//! The external owner retains this fixture through the original listener's actual join.

use crate::closing_pressure_gate::{PressureController, PressureOwner};
use crate::listener::{MysqlFixtureProtocolFailure, MysqlFixtureSessionJoins, WatcherFacts};
use novarocks_types::FrontendProcessId;
use novarocks_workload_control::{ResultCapacitySnapshot, WorkloadObservationHandle};
use std::{fmt, io, sync::Arc, time::Instant};
use tokio::task::JoinError;

pub use crate::closing_pressure_gate::{
    ArmInput as ClosingPressureTarget, Failure as ClosingPressureFailure,
    Phase as ClosingPressurePhase, SlotSnapshot as ClosingPressureSnapshot,
};
pub const CLOSING_PRESSURE_TARGETS: usize = 65;

/// Moved once into the original listener. No control or query authority escapes.
pub struct ClosingPressureListenerBinding {
    pub(crate) owner: Arc<PressureOwner>,
    pub(crate) joins: Arc<MysqlFixtureSessionJoins>,
}

/// A stable held bracket around one snapshot of the original capacity lock.
/// This says nothing about allocator, backing, or final alias destruction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClosingPressureJointSnapshot {
    pub capacity: ResultCapacitySnapshot,
    pub closing_targets: usize,
    pub minimum_original_closing_polls: u64,
    pub original_capacity_refusal: bool,
}

pub struct ClosingPressureFixture {
    controller: PressureController,
    joins: Arc<MysqlFixtureSessionJoins>,
    binding: Option<ClosingPressureListenerBinding>,
    control: Option<crate::closing_pressure_control::UnixPressureControl>,
}

/// Read-only observation of the original session, watcher, protocol and listener failures.
#[derive(Clone)]
pub struct ClosingPressureFailureObservation(Arc<MysqlFixtureSessionJoins>);
impl ClosingPressureFailureObservation {
    pub async fn wait_for_failure(&self) {
        self.0.wait_for_failure().await;
    }
}

/// Original finite failure slots; presentation never expands panic or protocol input.
#[derive(Default)]
pub struct ClosingPressureFixtureError {
    gate: Option<io::Error>,
    control: Option<crate::closing_pressure_control::ControlError>,
    control_cleanup: Option<crate::closing_pressure_control::ControlError>,
    listener: Option<io::Error>,
    session_join: Option<JoinError>,
    watcher_join: Option<JoinError>,
    protocol: Option<MysqlFixtureProtocolFailure>,
    capacity_eof: Option<MysqlFixtureProtocolFailure>,
    invalid_facts: bool,
    aborted_sessions: u64,
    counter_overflow: bool,
    watchers: Option<WatcherFacts>,
}
impl fmt::Debug for ClosingPressureFixtureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for ClosingPressureFixtureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Closing pressure fixture failed; control={} control_cleanup={} gate={} listener={} session_join={} watcher_join={} protocol={} original_capacity_eof={} invalid_facts={} aborted_sessions={} counter_overflow={} watchers={:?}",
            self.control.is_some(),
            self.control_cleanup.is_some(),
            self.gate.is_some(),
            self.listener.is_some(),
            self.session_join.is_some(),
            self.watcher_join.is_some(),
            self.protocol.is_some(),
            self.capacity_eof.is_some(),
            self.invalid_facts,
            self.aborted_sessions,
            self.counter_overflow,
            self.watchers
        )
    }
}
impl std::error::Error for ClosingPressureFixtureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(source) = &self.control {
            return Some(source);
        }
        if let Some(source) = &self.listener {
            return Some(source);
        }
        if let Some(source) = &self.protocol {
            return Some(source);
        }
        if let Some(source) = &self.session_join {
            return Some(source);
        }
        if let Some(source) = &self.watcher_join {
            return Some(source);
        }
        if let Some(source) = &self.gate {
            return Some(source);
        }
        if let Some(source) = &self.control_cleanup {
            return Some(source);
        }
        self.capacity_eof.as_ref().map(|source| source as _)
    }
}

impl ClosingPressureFixture {
    pub fn failure_observation(&self) -> ClosingPressureFailureObservation {
        ClosingPressureFailureObservation(Arc::clone(&self.joins))
    }
    pub fn new(
        actual_frontend: FrontendProcessId,
        original_absolute_deadline: Instant,
        original_workload: WorkloadObservationHandle,
    ) -> io::Result<Self> {
        let (owner, controller) = PressureOwner::with_workload(
            actual_frontend,
            original_absolute_deadline,
            Some(original_workload),
        )?;
        let joins = Arc::new(MysqlFixtureSessionJoins::for_closing_pressure());
        Ok(Self {
            controller,
            binding: Some(ClosingPressureListenerBinding {
                owner: Arc::new(owner),
                joins: Arc::clone(&joins),
            }),
            joins,
            control: None,
        })
    }
    /// Explicit startup inputs, original FE identity and original workload authority.
    /// This owns the Unix IO; the polled run future only borrows it.
    pub fn bind(
        path: std::path::PathBuf,
        actual_frontend: FrontendProcessId,
        nonce: [u8; 16],
        original_absolute_deadline: Instant,
        original_workload: WorkloadObservationHandle,
    ) -> Result<Self, ClosingPressureFixtureError> {
        let mut fixture = Self::new(
            actual_frontend,
            original_absolute_deadline,
            original_workload,
        )
        .map_err(|gate| ClosingPressureFixtureError {
            gate: Some(gate),
            ..Default::default()
        })?;
        match crate::closing_pressure_control::UnixPressureControl::bind(
            path,
            actual_frontend,
            nonce,
            original_absolute_deadline,
        ) {
            Ok(control) => {
                fixture.control = Some(control);
                Ok(fixture)
            }
            Err((control, control_cleanup)) => {
                fixture.fail_and_stop();
                Err(ClosingPressureFixtureError {
                    control: Some(control),
                    control_cleanup,
                    ..Default::default()
                })
            }
        }
    }
    pub async fn run_control(&mut self) -> Result<(), ClosingPressureFixtureError> {
        let Some(control) = &mut self.control else {
            self.fail_and_stop();
            return Err(ClosingPressureFixtureError {
                gate: Some(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "pressure control was not bound",
                )),
                ..Default::default()
            });
        };
        control
            .run(&mut self.controller)
            .await
            .map_err(|control| ClosingPressureFixtureError {
                control: Some(control),
                ..Default::default()
            })
    }
    pub fn close_control(&mut self) -> Result<(), ClosingPressureFixtureError> {
        match &mut self.control {
            Some(control) => control
                .close(&mut self.controller)
                .map_err(|control_cleanup| ClosingPressureFixtureError {
                    control_cleanup: Some(control_cleanup),
                    ..Default::default()
                }),
            None => Ok(()),
        }
    }
    pub fn fail_and_stop(&mut self) {
        self.controller.fail(ClosingPressureFailure::Transition);
        self.controller.stop();
    }
    pub fn fail_startup_projection(
        &mut self,
        original_cause: io::Error,
    ) -> ClosingPressureFixtureError {
        self.fail_and_stop();
        let mut failure = self.close_control().err().unwrap_or_default();
        failure.gate = Some(original_cause);
        failure
    }
    pub fn listener_binding(&mut self) -> io::Result<ClosingPressureListenerBinding> {
        self.binding.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Closing pressure listener binding was already moved",
            )
        })
    }
    pub fn arm_targets(
        &mut self,
        actual_frontend: FrontendProcessId,
        targets: [ClosingPressureTarget; CLOSING_PRESSURE_TARGETS],
    ) -> io::Result<()> {
        self.controller.arm_targets(actual_frontend, targets)
    }
    pub fn snapshot(&self, slot: usize) -> io::Result<ClosingPressureSnapshot> {
        self.controller.snapshot(slot)
    }
    pub fn joint_closing_snapshot(
        &self,
        require_original_refusal: bool,
    ) -> io::Result<ClosingPressureJointSnapshot> {
        self.controller
            .joint_closing_snapshot(require_original_refusal)
    }
    pub fn release(&mut self, slot: usize) -> io::Result<()> {
        self.controller.release(slot)
    }
    pub fn stop(&mut self) {
        self.controller.stop();
    }
    /// One poll after the original listener's bounded drain; no second wait or clock.
    pub async fn verify_original_connection_drain(
        &self,
        connections: &crate::MysqlClientConnectionRegistry,
    ) -> io::Result<()> {
        let drained = connections.wait_drained();
        tokio::pin!(drained);
        let complete = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(drained.as_mut(), cx).is_ready())
        })
        .await;
        if !complete {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Closing pressure original connection owners remain live",
            ));
        }
        Ok(())
    }
    /// This verifies facts only. The caller must await the original listener first.
    pub fn finish_after_original_listener_join(
        &mut self,
    ) -> Result<(), ClosingPressureFixtureError> {
        let facts = self.joins.snapshot();
        let watchers = self.joins.watcher_snapshot();
        let listener = self.joins.take_listener_failure_after_join();
        let protocol = self.joins.take_protocol_failure_after_join();
        let capacity_eof = self.joins.take_pressure_eof_after_join();
        let invalid_eof = facts.pressure_capacity_eofs != 1
            || !capacity_eof.as_ref().is_some_and(|exit| {
                crate::closing_pressure_gate::relay::PressureCapacityEof::from_error(&exit.cause)
                    .is_some_and(|eof| eof.matches_controller(&self.controller))
            });
        let gate = self.controller.inspect_after_original_join().err();
        let session_join = self.joins.take_failure_after_join();
        let watcher_join = self.joins.take_watcher_failure_after_join();
        let invalid_facts = self
            .control
            .as_ref()
            .is_some_and(|control| !control.completed_and_closed())
            || self.binding.is_some()
            || !self.joins.watchers_empty()
            || watchers.reserved != 0
            || watchers.failed()
            || invalid_eof;
        if invalid_facts
            || gate.is_some()
            || listener.is_some()
            || protocol.is_some()
            || session_join.is_some()
            || watcher_join.is_some()
            || facts.aborted != 0
            || facts.counter_overflow
            || facts.protocol_io_failures != 0
            || facts.listener_io_failures != 0
        {
            return Err(ClosingPressureFixtureError {
                gate,
                listener,
                session_join,
                watcher_join,
                protocol,
                capacity_eof,
                invalid_facts,
                aborted_sessions: facts.aborted,
                counter_overflow: facts.counter_overflow,
                watchers: Some(watchers),
                ..Default::default()
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod startup_source_tests {
    use super::*;
    use std::{error::Error, os::unix::fs::PermissionsExt, time::Duration};
    #[derive(Debug)]
    struct OriginalOutput(Arc<()>);
    impl fmt::Display for OriginalOutput {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("original startup source must not be formatted");
        }
    }
    impl Error for OriginalOutput {}
    #[tokio::test]
    async fn startup_primary_survives_actual_replaced_socket_cleanup_error() {
        let directory = std::path::PathBuf::from(format!(
            "/tmp/nr-cp-src-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("pressure.sock");
        let workload = novarocks_workload_control::WorkloadControl::try_new_counted(
            novarocks_workload_control::WorkloadConfig::default(),
        )
        .unwrap()
        .owner;
        let mut fixture = ClosingPressureFixture::bind(
            path.clone(),
            FrontendProcessId::new_v7(),
            [1; 16],
            Instant::now() + Duration::from_secs(3),
            workload.observation(),
        )
        .unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement must remain").unwrap();
        let identity = Arc::new(());
        let failure =
            fixture.fail_startup_projection(io::Error::other(OriginalOutput(identity.clone())));
        assert!(failure.control_cleanup.is_some());
        let original = failure
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalOutput>()
            .unwrap();
        assert!(Arc::ptr_eq(&original.0, &identity));
        let _finite = format!("{failure:?} {failure}");
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement must remain");
        drop(fixture);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
}
