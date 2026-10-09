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

use anyhow::{Result, ensure};
use novarocks_cluster_harness::CrossProcessChildEnvironment;
use std::time::{Duration, Instant, SystemTime};
const SOCKET: &str = "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_SOCKET";
const NONCE: &str = "NOVAROCKS_MEM_1_M07_MYSQL_WRITE_NONCE_HEX";
#[derive(Clone, Copy)]
pub(crate) struct ExactMysqlPrelaunchClock {
    deadline: Instant,
    started_at: SystemTime,
}
impl ExactMysqlPrelaunchClock {
    /// Called exactly once by run_one before the original cluster launch.
    pub(crate) fn capture(environment: &CrossProcessChildEnvironment) -> Result<Option<Self>> {
        Self::capture_at(environment, Instant::now())
    }
    fn capture_at(
        environment: &CrossProcessChildEnvironment,
        origin: Instant,
    ) -> Result<Option<Self>> {
        ensure!(
            environment.be.keys().all(|k| k != SOCKET && k != NONCE)
                && environment
                    .be_by_index
                    .values()
                    .all(|e| !e.contains_key(SOCKET) && !e.contains_key(NONCE)),
            "exact MySQL control input must be FE-child-only"
        );
        match (
            environment.fe.contains_key(SOCKET),
            environment.fe.contains_key(NONCE),
        ) {
            (false, false) => Ok(None),
            (true, true) => Ok(Some(Self {
                deadline: origin + Duration::from_secs(20),
                started_at: SystemTime::now(),
            })),
            _ => Err(anyhow::anyhow!(
                "exact MySQL control launch requires both private FE inputs"
            )),
        }
    }
    pub(crate) fn validate_launch(
        profile: novarocks_cluster_harness::LaunchProfile,
        backends: usize,
    ) -> Result<()> {
        ensure!(
            profile == novarocks_cluster_harness::LaunchProfile::FaultScenario && backends == 3,
            "exact MySQL fixture requires fault-scenario 1FE+3BE before role launch"
        );
        Ok(())
    }
    pub(crate) fn started_at(self) -> SystemTime {
        self.started_at
    }
    pub(crate) fn deadline(self) -> Instant {
        self.deadline
    }
    pub(crate) fn remaining(self, operation: &'static str) -> Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "original exact MySQL prelaunch clock expired before {operation}"
        );
        Ok(remaining)
    }
    /// Always settle the original owners, including on an expired clock.
    /// Cleanup success cannot admit a late four-role settlement.
    pub(crate) fn settle_original_roles(self, cleanup: impl FnOnce() -> Result<()>) -> Result<()> {
        let cleanup = cleanup();
        let deadline = self
            .remaining("all original role cleanup settlement")
            .map(|_| ());
        match (cleanup, deadline) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(cleanup), Err(deadline)) => {
                Err(OriginalRolesSettlementFailure { cleanup, deadline }.into())
            }
        }
    }
}

struct OriginalRolesSettlementFailure {
    cleanup: anyhow::Error,
    deadline: anyhow::Error,
}
impl std::fmt::Debug for OriginalRolesSettlementFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for OriginalRolesSettlementFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.deadline;
        f.write_str("original role cleanup and original deadline failed; both sources retained")
    }
}
impl std::error::Error for OriginalRolesSettlementFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cleanup.as_ref())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn environment() -> CrossProcessChildEnvironment {
        let mut env = CrossProcessChildEnvironment::default();
        env.fe.insert(SOCKET.into(), "private".into());
        env.fe.insert(NONCE.into(), "private".into());
        env
    }
    #[test]
    fn expired_role_settlement_still_runs_cleanup_and_retains_original_source() {
        let clock = ExactMysqlPrelaunchClock::capture_at(
            &environment(),
            Instant::now() - Duration::from_secs(25),
        )
        .unwrap()
        .unwrap();
        let calls = std::cell::Cell::new(0);
        let error = clock
            .settle_original_roles(|| {
                calls.set(calls.get() + 1);
                Err(std::io::Error::from_raw_os_error(5).into())
            })
            .unwrap_err();
        assert_eq!(calls.get(), 1);
        let failure = error
            .downcast_ref::<OriginalRolesSettlementFailure>()
            .unwrap();
        assert_eq!(
            failure
                .cleanup
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(5)
        );
        assert!(!format!("{failure}").contains("Input/output"));
    }
    #[test]
    fn role_cleanup_crossing_original_clock_never_admits_late_success() {
        let clock = ExactMysqlPrelaunchClock {
            deadline: Instant::now(),
            started_at: SystemTime::now(),
        };
        let calls = std::cell::Cell::new(0);
        assert!(
            clock
                .settle_original_roles(|| {
                    calls.set(1);
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(calls.get(), 1);
    }
    #[test]
    fn original_origin_is_preserved_without_new_scene_clock() {
        let origin = Instant::now() - Duration::from_secs(25);
        let clock = ExactMysqlPrelaunchClock::capture_at(&environment(), origin)
            .unwrap()
            .unwrap();
        assert_eq!(clock.deadline(), origin + Duration::from_secs(20));
        assert!(clock.remaining("client connect").is_err());
        assert_eq!(clock.deadline(), clock.deadline());
    }
    #[test]
    fn only_original_fe_explicit_pair_activates_clock() {
        assert!(
            ExactMysqlPrelaunchClock::capture(&CrossProcessChildEnvironment::default())
                .unwrap()
                .is_none()
        );
        let mut env = environment();
        env.fe.remove(NONCE);
        assert!(ExactMysqlPrelaunchClock::capture(&env).is_err());
        let mut env = environment();
        env.be.insert(SOCKET.into(), "private".into());
        assert!(ExactMysqlPrelaunchClock::capture(&env).is_err());
    }
    #[test]
    fn invalid_topology_or_performance_refuses_before_launch() {
        use novarocks_cluster_harness::LaunchProfile;
        ExactMysqlPrelaunchClock::validate_launch(LaunchProfile::FaultScenario, 3).unwrap();
        for count in [0, 1, 2, 4] {
            assert!(
                ExactMysqlPrelaunchClock::validate_launch(LaunchProfile::FaultScenario, count)
                    .is_err()
            );
        }
        assert!(ExactMysqlPrelaunchClock::validate_launch(LaunchProfile::Performance, 3).is_err());
    }
}
