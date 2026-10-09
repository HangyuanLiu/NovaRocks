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
