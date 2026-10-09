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

//! Original direct-child graceful-success gate for the explicit exact MySQL fixture.
//! The caller still settles this same owner on every result; no role is spawned here.
use anyhow::{Context, Result};
use novarocks_test_support::ManagedProcess;
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeadlineStage {
    BeforeTermination,
    AfterSuccessfulExit,
}
#[derive(Debug)]
struct OriginalFeDeadlineExpired {
    stage: DeadlineStage,
}
impl std::fmt::Display for OriginalFeDeadlineExpired {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stage = match self.stage {
            DeadlineStage::BeforeTermination => "before original FE termination",
            DeadlineStage::AfterSuccessfulExit => "after original FE successful exit",
        };
        write!(
            formatter,
            "original exact MySQL prelaunch deadline expired {stage}"
        )
    }
}
impl std::error::Error for OriginalFeDeadlineExpired {}

fn check_original_deadline(deadline: Instant, stage: DeadlineStage) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(OriginalFeDeadlineExpired { stage }.into());
    }
    Ok(())
}

/// Uses one caller-owned absolute clock for admission and actual successful exit.
///
/// Even an already exited child cannot satisfy an expired original clock. The
/// postcheck covers ManagedProcess's output joining and status observation. This
/// function never substitutes ordinary `stop` for the successful-exit verdict.
/// Its caller must always run ordinary `stop` on this same owner after returning,
/// retaining both returned errors in its existing fixed cleanup source positions.
pub(crate) fn require_original_fe_success(
    process: &mut ManagedProcess,
    original_deadline: Instant,
) -> Result<()> {
    check_original_deadline(original_deadline, DeadlineStage::BeforeTermination)?;
    process
        .request_termination()
        .context("request original FE child termination")?;
    process
        .wait_for_successful_exit_until(original_deadline)
        .context("wait for original FE child successful exit")?;
    check_original_deadline(original_deadline, DeadlineStage::AfterSuccessfulExit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_test_support::ReadyMarker;
    use std::process::Command;
    use std::time::Duration;

    fn spawn_original_child(
        exit_code: u8,
        original_deadline: Instant,
    ) -> (tempfile::TempDir, ManagedProcess) {
        let directory = tempfile::tempdir().expect("create original-child component directory");
        let mut command = Command::new("/bin/sh");
        // All commands here are shell builtins: no extra child, runtime or service.
        // Install the trap before READY so the gate's signal cannot win that race.
        command.arg("-c").arg(format!(
            "trap 'exit {exit_code}' TERM; printf 'ORIGINAL_CHILD_READY\\n'; while :; do :; done"
        ));
        let process = ManagedProcess::spawn(
            "original FE exit host component".into(),
            command,
            ReadyMarker::StdoutContains("ORIGINAL_CHILD_READY".into()),
            original_deadline.saturating_duration_since(Instant::now()),
            directory.path().join("original-child.log"),
        )
        .expect("spawn original owned child before original clock");
        (directory, process)
    }

    fn settle_same_owner_and_assert_actual_pid_absent(process: &mut ManagedProcess) {
        let original_pid = process.pid();
        // Cleanup is always attempted BEFORE verdict assertions, including gate failure.
        // This is original ManagedProcess cleanup policy, not a renewed success clock.
        let settlement = process.stop();
        let running = process.is_running();
        let result = unsafe { libc::kill(i32::try_from(original_pid).unwrap(), 0) };
        let observed = std::io::Error::last_os_error();
        assert!(
            settlement.is_ok(),
            "actual original stop error: {settlement:?}"
        );
        assert!(
            matches!(running, Ok(false)),
            "same original owner running: {running:?}"
        );
        assert_eq!(result, -1, "original direct-child PID is still present");
        assert_eq!(
            observed.raw_os_error(),
            Some(libc::ESRCH),
            "PID absence requires ESRCH; permission/unknown/reused PID cannot pass"
        );
    }

    fn wait_for_original_clock_expiry(original_deadline: Instant) {
        // Consume only the caller's original remaining budget; never create a new deadline.
        std::thread::sleep(original_deadline.saturating_duration_since(Instant::now()));
    }

    #[test]
    fn actual_original_exit_zero_passes_then_same_owner_is_reaped_and_pid_absent() {
        let original_deadline = Instant::now() + Duration::from_secs(2);
        let (_directory, mut process) = spawn_original_child(0, original_deadline);
        let gate = require_original_fe_success(&mut process, original_deadline);
        settle_same_owner_and_assert_actual_pid_absent(&mut process);
        assert!(gate.is_ok(), "original success gate: {gate:?}");
    }

    #[test]
    fn actual_original_exit_one_fails_despite_same_owner_cleanup_and_pid_absence() {
        let original_deadline = Instant::now() + Duration::from_secs(2);
        let (_directory, mut process) = spawn_original_child(1, original_deadline);
        let gate = require_original_fe_success(&mut process, original_deadline);
        settle_same_owner_and_assert_actual_pid_absent(&mut process);
        let error = gate.expect_err("ordinary cleanup must not waive actual exit1");
        assert!(
            error.downcast_ref::<OriginalFeDeadlineExpired>().is_none(),
            "test must observe nonzero original exit rather than a clock failure"
        );
        assert!(
            error.chain().any(|source| source
                .to_string()
                .contains("did not exit successfully after SIGTERM")),
            "actual ManagedProcess nonzero status source must remain owned: {error:#}"
        );
    }

    #[test]
    fn expired_original_clock_rejects_even_already_successfully_exited_original_child() {
        let original_deadline = Instant::now() + Duration::from_secs(1);
        let (_directory, mut process) = spawn_original_child(0, original_deadline);
        process
            .request_termination()
            .expect("signal same original child for test setup");
        let prior_exit = process.wait_for_successful_exit_until(original_deadline);
        wait_for_original_clock_expiry(original_deadline);
        let gate = require_original_fe_success(&mut process, original_deadline);
        settle_same_owner_and_assert_actual_pid_absent(&mut process);
        assert!(
            prior_exit.is_ok(),
            "test setup must actually observe successful child exit"
        );
        let error = gate.expect_err("expired clock cannot admit already-exited child");
        assert_eq!(
            error
                .downcast_ref::<OriginalFeDeadlineExpired>()
                .unwrap()
                .stage,
            DeadlineStage::BeforeTermination
        );
    }

    #[test]
    fn expired_original_clock_rejects_live_original_child_then_same_owner_stop_reaps_it() {
        let original_deadline = Instant::now() + Duration::from_secs(1);
        let (_directory, mut process) = spawn_original_child(0, original_deadline);
        wait_for_original_clock_expiry(original_deadline);
        let before = process.is_running();
        let gate = require_original_fe_success(&mut process, original_deadline);
        let after = process.is_running();
        settle_same_owner_and_assert_actual_pid_absent(&mut process);
        assert!(
            matches!(before, Ok(true)) && matches!(after, Ok(true)),
            "expired admission must not signal the still-live original child"
        );
        let error = gate.expect_err("expired clock cannot admit live child");
        assert_eq!(
            error
                .downcast_ref::<OriginalFeDeadlineExpired>()
                .unwrap()
                .stage,
            DeadlineStage::BeforeTermination
        );
    }
}
