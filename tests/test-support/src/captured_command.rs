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

//! Complete, caller-owned file capture for one bounded command and its original process group.
use super::{
    EPERM, ESRCH, ObservedExitStatus, P_PID, POLL_INTERVAL, ProcessGroupOwnership, SIGKILL,
    WEXITED, WNOHANG, WNOWAIT, WaitSiginfo, send_signal, waitid,
};
use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::Instant;

pub struct CapturedCommandOutcome {
    pub output: Output,
    pub timed_out: bool,
    pub failures: Vec<String>,
}
struct CommandOwner {
    child: Child,
    group: ProcessGroupOwnership,
    reaped: bool,
    failures: Vec<String>,
}
impl CommandOwner {
    fn observe(&self) -> Result<Option<ObservedExitStatus>> {
        let mut info = WaitSiginfo::zeroed();
        // SAFETY: the parent module supplies the verified target-specific ABI.
        // This handle still owns its unreaped direct Child, preventing PID reuse.
        let result = unsafe {
            waitid(
                P_PID,
                self.child.id(),
                &mut info,
                WEXITED | WNOHANG | WNOWAIT,
            )
        };
        if result == 0 {
            info.observed_exit_status()
        } else {
            Err(std::io::Error::last_os_error())
                .context("observe captured original child without reaping")
        }
    }
    fn finish(&mut self) -> Result<ExitStatus> {
        if let Ok(group) = self.group.group_id_for_signal() {
            let group = i32::try_from(group).context("captured process group id exceeds i32")?;
            // SAFETY: process_group(0) created this exact group for the still
            // unreaped direct child. No group signal is issued after final signal.
            if unsafe { send_signal(-group, SIGKILL) } != 0 {
                let error = std::io::Error::last_os_error();
                let zombie_only = if error.raw_os_error() == Some(EPERM) {
                    match self.observe() {
                        Ok(status) => status.is_some(),
                        Err(observation) => {
                            self.failures
                                .push(format!("observe group signal refusal: {observation:#}"));
                            false
                        }
                    }
                } else {
                    false
                };
                if error.raw_os_error() != Some(ESRCH) && !zombie_only {
                    self.failures.push(format!(
                        "captured process group refused final signal: {error}"
                    ));
                    if let Err(error) = self.child.kill() {
                        self.failures.push(format!(
                            "kill captured original child after group refusal: {error}"
                        ));
                    }
                }
            }
            self.group.record_final_group_signal()?;
        }
        self.group.permit_reap()?;
        let status = self
            .child
            .wait()
            .context("actually reap captured original child")?;
        self.group.record_reaped()?;
        self.reaped = true;
        Ok(status)
    }
}
impl Drop for CommandOwner {
    fn drop(&mut self) {
        if !self.reaped {
            if let Err(error) = self.finish() {
                eprintln!("captured command original owner cleanup failed: {error:#}");
            }
        }
    }
}
/// Files remain caller-owned by descriptor aliases. Capture never reopens a
/// pathname, starts a detached reader, or signals a guessed/reaped process id.
/// A background script whose leader exits early is terminated with this group;
/// it is not certified as equivalent to unbounded pipe-EOF waiting.
pub fn capture_command_until(
    mut command: Command,
    mut stdout: File,
    mut stderr: File,
    deadline: Instant,
) -> Result<CapturedCommandOutcome> {
    ensure!(
        deadline > Instant::now(),
        "captured command original deadline expired before spawn"
    );
    for file in [&stdout, &stderr] {
        ensure!(
            file.metadata()?.is_file(),
            "captured command output must be an owned regular file"
        );
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?))
        .process_group(0);
    let child = command.spawn().context("spawn bounded captured command")?;
    let mut owner = CommandOwner {
        group: ProcessGroupOwnership::new(child.id()),
        child,
        reaped: false,
        failures: vec![],
    };
    let mut timed_out = false;
    loop {
        match owner.observe() {
            Ok(Some(_)) => {
                timed_out = Instant::now() >= deadline;
                break;
            }
            Ok(None) => {}
            Err(error) => {
                owner
                    .failures
                    .push(format!("captured command observation failed: {error:#}"));
                break;
            }
        }
        if Instant::now() >= deadline {
            timed_out = true;
            break;
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(POLL_INTERVAL),
        );
    }
    // Final group signal precedes reap even after a natural leader exit, so a
    // child cannot silently outlive a successful foreground command.
    let status = owner.finish()?;
    let mut output = Output {
        status,
        stdout: vec![],
        stderr: vec![],
    };
    for (name, file, bytes) in [
        ("stdout", &mut stdout, &mut output.stdout),
        ("stderr", &mut stderr, &mut output.stderr),
    ] {
        if let Err(error) = (|| -> std::io::Result<()> {
            file.seek(SeekFrom::Start(0))?;
            let mut buffer = [0u8; 65536];
            loop {
                if Instant::now() >= deadline {
                    timed_out = true;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "original deadline expired reading captured output; original file retained by caller",
                    ));
                }
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&buffer[..read]);
            }
            Ok(())
        })() {
            owner
                .failures
                .push(format!("read original captured {name}: {error}"));
        }
    }
    Ok(CapturedCommandOutcome {
        output,
        timed_out,
        failures: std::mem::take(&mut owner.failures),
    })
}
