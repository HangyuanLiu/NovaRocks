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

use anyhow::{Context, Result, bail};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

const LOG_TAIL_BYTES: usize = 8 * 1024;
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(unix)]
const ESRCH: i32 = 3;
#[cfg(unix)]
const EPERM: i32 = 1;

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn send_signal(pid: i32, signal: i32) -> i32;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadyMarker {
    StdoutContains(String),
    FileContains { path: PathBuf, needle: String },
}

pub(crate) struct ManagedProcess {
    label: String,
    child: Child,
    #[cfg(unix)]
    process_group_id: u32,
    log_path: PathBuf,
    log_file: Arc<Mutex<File>>,
    stdout_buffer: Arc<Mutex<String>>,
    stderr_buffer: Arc<Mutex<String>>,
    stdout_thread: Option<thread::JoinHandle<()>>,
    stderr_thread: Option<thread::JoinHandle<()>>,
    stopped: bool,
}

impl std::fmt::Debug for ManagedProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagedProcess")
            .field("label", &self.label)
            .field("pid", &self.child.id())
            .field("log_path", &self.log_path)
            .finish_non_exhaustive()
    }
}

impl ManagedProcess {
    pub(crate) fn spawn(
        label: String,
        mut command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
    ) -> Result<Self> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        let log_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&log_path)
            .with_context(|| format!("open durable process log {}", log_path.display()))?;
        let log_file = Arc::new(Mutex::new(log_file));

        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);

        let mut child = command
            .spawn()
            .with_context(|| format!("spawn {label}; log={}", log_path.display()))?;
        let stdout = child
            .stdout
            .take()
            .with_context(|| format!("capture {label} stdout"))?;
        let stderr = child
            .stderr
            .take()
            .with_context(|| format!("capture {label} stderr"))?;

        let stdout_buffer = Arc::new(Mutex::new(String::new()));
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let (ready_tx, ready_rx) = mpsc::sync_channel::<()>(1);
        let stdout_marker = match &marker {
            ReadyMarker::StdoutContains(needle) => Some(needle.clone()),
            ReadyMarker::FileContains { .. } => None,
        };
        let stdout_thread = spawn_reader(
            stdout,
            Arc::clone(&stdout_buffer),
            Arc::clone(&log_file),
            stdout_marker,
            Some(ready_tx),
        );
        let stderr_thread = spawn_reader(
            stderr,
            Arc::clone(&stderr_buffer),
            Arc::clone(&log_file),
            None,
            None,
        );

        let mut process = Self {
            label,
            #[cfg(unix)]
            process_group_id: child.id(),
            child,
            log_path,
            log_file,
            stdout_buffer,
            stderr_buffer,
            stdout_thread: Some(stdout_thread),
            stderr_thread: Some(stderr_thread),
            stopped: false,
        };
        if let Err(error) = process.wait_for_ready(&marker, &ready_rx, deadline, timeout) {
            let cleanup = process.kill_now();
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(error.context(format!(
                    "also failed to clean up {}: {cleanup_error:#}",
                    process.label
                ))),
            };
        }
        Ok(process)
    }

    pub(crate) fn pid(&self) -> u32 {
        self.child.id()
    }

    pub(crate) fn stdout_tail(&self) -> String {
        read_tail(&self.stdout_buffer, "<stdout lock poisoned>")
    }

    pub(crate) fn stderr_tail(&self) -> String {
        read_tail(&self.stderr_buffer, "<stderr lock poisoned>")
    }

    pub(crate) fn assert_log_contains(&self, needle: &str) -> Result<()> {
        if let Ok(mut log_file) = self.log_file.lock() {
            log_file.flush().with_context(|| {
                format!("flush durable process log {}", self.log_path.display())
            })?;
        }
        let log = fs::read_to_string(&self.log_path)
            .with_context(|| format!("read durable process log {}", self.log_path.display()))?;
        if log.contains(needle) {
            return Ok(());
        }
        bail!(
            "{} log {} does not contain {needle:?}; stdout_tail={:?}; stderr_tail={:?}",
            self.label,
            self.log_path.display(),
            self.stdout_tail(),
            self.stderr_tail()
        )
    }

    pub(crate) fn restart(
        &mut self,
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
    ) -> Result<()> {
        self.kill_now()
            .with_context(|| format!("kill {} before restart", self.label))?;
        let replacement = Self::spawn(self.label.clone(), command, marker, timeout, log_path)?;
        *self = replacement;
        Ok(())
    }

    pub(crate) fn stop(&mut self) -> Result<()> {
        if self.stopped {
            self.join_output_threads();
            return Ok(());
        }
        #[cfg(unix)]
        {
            if self.child.try_wait()?.is_some() && !self.process_group_exists()? {
                self.stopped = true;
                self.join_output_threads();
                return Ok(());
            }
            self.signal_group(SIGTERM)?;
            let deadline = Instant::now() + STOP_TIMEOUT;
            loop {
                let child_exited = self.child.try_wait()?.is_some();
                if child_exited && !self.process_group_exists()? {
                    self.stopped = true;
                    self.join_output_threads();
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    break;
                }
                thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(POLL_INTERVAL),
                );
            }
            self.signal_group(SIGKILL)?;
            let _status = self
                .child
                .wait()
                .with_context(|| format!("wait for {} after SIGKILL", self.label))?;
            self.wait_for_group_exit(Duration::from_secs(1))?;
            self.stopped = true;
            self.join_output_threads();
            return Ok(());
        }

        #[cfg(not(unix))]
        {
            if self.child.try_wait()?.is_none() {
                self.child.kill()?;
                let _ = self.child.wait()?;
            }
            self.stopped = true;
            self.join_output_threads();
            Ok(())
        }
    }

    pub(crate) fn kill_now(&mut self) -> Result<()> {
        if self.stopped {
            self.join_output_threads();
            return Ok(());
        }
        #[cfg(unix)]
        {
            if self.child.try_wait()?.is_some() && !self.process_group_exists()? {
                self.stopped = true;
                self.join_output_threads();
                return Ok(());
            }
            self.signal_group(SIGKILL)?;
        }
        #[cfg(not(unix))]
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }

        let _status = self
            .child
            .wait()
            .with_context(|| format!("wait for {} after immediate kill", self.label))?;
        #[cfg(unix)]
        self.wait_for_group_exit(Duration::from_secs(1))?;
        self.stopped = true;
        self.join_output_threads();
        Ok(())
    }

    pub(crate) fn runtime_diagnostic(
        &mut self,
        label: &str,
        endpoint: &str,
        config_path: &std::path::Path,
    ) -> Result<String> {
        let pid = self.pid();
        let status = self.child.try_wait().with_context(|| {
            format!("inspect {label} pid={pid} endpoint={endpoint} process status")
        })?;
        let stdout_tail = self.stdout_tail();
        let stderr_tail = self.stderr_tail();
        match status {
            Some(status) => bail!(
                "{label} exited status={status} pid={pid} endpoint={endpoint} config={} stdout_tail={stdout_tail:?} stderr_tail={stderr_tail:?}",
                config_path.display()
            ),
            None => Ok(format!(
                "{label}=running pid={pid} endpoint={endpoint} config={} stdout_tail={stdout_tail:?} stderr_tail={stderr_tail:?}",
                config_path.display()
            )),
        }
    }

    fn wait_for_ready(
        &mut self,
        marker: &ReadyMarker,
        ready_rx: &mpsc::Receiver<()>,
        deadline: Instant,
        timeout: Duration,
    ) -> Result<()> {
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.join_output_threads();
                bail!(
                    "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }

            match marker {
                ReadyMarker::StdoutContains(_) => match ready_rx.try_recv() {
                    Ok(()) => return Ok(()),
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) => {
                        if let Some(status) = self.child.try_wait()? {
                            self.join_output_threads();
                            bail!(
                                "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                                self.label,
                                self.stdout_tail(),
                                self.stderr_tail(),
                                self.log_path.display()
                            );
                        }
                        bail!(
                            "{} stdout closed before readiness marker while child was still running; stdout_tail={:?}; stderr_tail={}; log={}",
                            self.label,
                            self.stdout_tail(),
                            self.stderr_tail(),
                            self.log_path.display()
                        );
                    }
                },
                ReadyMarker::FileContains { path, needle } => {
                    if fs::read(path)
                        .ok()
                        .is_some_and(|bytes| String::from_utf8_lossy(&bytes).contains(needle))
                    {
                        return Ok(());
                    }
                }
            }

            let now = Instant::now();
            if now >= deadline {
                bail!(
                    "{} timed out waiting for readiness marker after {timeout:?}; stdout_tail={:?}; stderr_tail={}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }
            thread::sleep(deadline.saturating_duration_since(now).min(POLL_INTERVAL));
        }
    }

    fn join_output_threads(&mut self) {
        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
        if let Some(stderr_thread) = self.stderr_thread.take() {
            let _ = stderr_thread.join();
        }
    }

    #[cfg(unix)]
    fn signal_group(&self, signal: i32) -> Result<()> {
        let process_group_id =
            i32::try_from(self.process_group_id).context("managed process group id exceeds i32")?;
        // SAFETY: POSIX kill accepts a negative process-group id. This id was
        // assigned to the child by process_group(0) immediately before spawn.
        if unsafe { send_signal(-process_group_id, signal) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(ESRCH) {
            return Ok(());
        }
        Err(error).with_context(|| {
            format!(
                "send signal {signal} to {} process group {}",
                self.label, self.process_group_id
            )
        })
    }

    #[cfg(unix)]
    fn process_group_exists(&self) -> Result<bool> {
        let process_group_id =
            i32::try_from(self.process_group_id).context("managed process group id exceeds i32")?;
        // SAFETY: signal 0 performs existence/permission checking only.
        if unsafe { send_signal(-process_group_id, 0) } == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(ESRCH) {
            return Ok(false);
        }
        if error.raw_os_error() == Some(EPERM) {
            return Ok(true);
        }
        Err(error).with_context(|| {
            format!(
                "inspect {} process group {}",
                self.label, self.process_group_id
            )
        })
    }

    #[cfg(unix)]
    fn wait_for_group_exit(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        while self.process_group_exists()? {
            if Instant::now() >= deadline {
                bail!(
                    "{} process group {} remained after SIGKILL",
                    self.label,
                    self.process_group_id
                );
            }
            thread::sleep(POLL_INTERVAL);
        }
        Ok(())
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn spawn_reader<R: Read + Send + 'static>(
    mut reader: R,
    tail: Arc<Mutex<String>>,
    log_file: Arc<Mutex<File>>,
    ready_marker: Option<String>,
    ready_tx: Option<mpsc::SyncSender<()>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        let mut marker_scan = String::new();
        let mut ready_sent = false;
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(count) => count,
            };
            let chunk = &buffer[..count];
            if let Ok(mut output) = tail.lock() {
                push_bounded_log_chunk(&mut output, chunk, LOG_TAIL_BYTES);
            }
            if let Ok(mut log) = log_file.lock() {
                let _ = log.write_all(chunk);
                let _ = log.flush();
            }
            if !ready_sent && let Some(marker) = ready_marker.as_deref() {
                marker_scan.push_str(&String::from_utf8_lossy(chunk));
                if marker_scan.contains(marker) {
                    if let Some(ready_tx) = ready_tx.as_ref() {
                        let _ = ready_tx.try_send(());
                    }
                    ready_sent = true;
                    marker_scan.clear();
                } else {
                    let scan_capacity = LOG_TAIL_BYTES.max(marker.len().saturating_mul(2));
                    truncate_front(&mut marker_scan, scan_capacity);
                }
            }
        }
    })
}

fn push_bounded_log_chunk(buffer: &mut String, chunk: &[u8], capacity: usize) {
    buffer.push_str(&String::from_utf8_lossy(chunk));
    truncate_front(buffer, capacity);
}

fn truncate_front(buffer: &mut String, capacity: usize) {
    if capacity == 0 {
        buffer.clear();
        return;
    }
    if buffer.len() <= capacity {
        return;
    }
    let mut start = buffer.len() - capacity;
    while start < buffer.len() && !buffer.is_char_boundary(start) {
        start += 1;
    }
    buffer.drain(..start);
}

fn read_tail(buffer: &Arc<Mutex<String>>, poisoned: &str) -> String {
    buffer
        .lock()
        .map(|buffer| buffer.clone())
        .unwrap_or_else(|_| poisoned.to_string())
}

#[cfg(all(test, unix))]
mod tests {
    use super::{ManagedProcess, ReadyMarker};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "novarocks-managed-process-{label}-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create managed process test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(script);
        command
    }

    fn shell_with_arg(script: &str, arg: &Path) -> Command {
        let mut command = shell(script);
        command.arg("managed-process-fixture").arg(arg);
        command
    }

    fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn pid_exists(pid: u32) -> bool {
        Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    #[test]
    fn managed_process_captures_bounded_tails_and_durable_log() {
        let temp = TempDir::new("tails");
        let log_path = temp.path().join("fixture.log");
        let command = shell(
            "i=0; while [ $i -lt 1200 ]; do printf 'stdout-%04d-abcdefghij\\n' \"$i\"; printf 'stderr-%04d-abcdefghij\\n' \"$i\" >&2; i=$((i + 1)); done; printf 'STDOUT_TAIL_READY\\n'; printf 'STDERR_TAIL_DONE\\n' >&2; sleep 30",
        );

        let mut process = ManagedProcess::spawn(
            "tail fixture".to_string(),
            command,
            ReadyMarker::StdoutContains("STDOUT_TAIL_READY".to_string()),
            Duration::from_secs(5),
            log_path.clone(),
        )
        .expect("spawn tail fixture");

        assert!(process.stdout_tail().len() <= 8 * 1024);
        assert!(process.stdout_tail().contains("STDOUT_TAIL_READY"));
        assert!(wait_until(Duration::from_secs(1), || process
            .stderr_tail()
            .contains("STDERR_TAIL_DONE")));
        assert!(process.stderr_tail().len() <= 8 * 1024);
        process
            .assert_log_contains("STDOUT_TAIL_READY")
            .expect("durable log contains stdout");
        process
            .assert_log_contains("STDERR_TAIL_DONE")
            .expect("durable log contains stderr");
        let log = fs::read_to_string(log_path).expect("read durable log");
        assert!(log.contains("STDOUT_TAIL_READY"));
        assert!(log.contains("STDERR_TAIL_DONE"));
        process.kill_now().expect("kill tail fixture");
    }

    #[test]
    fn managed_process_reports_early_exit_status() {
        let temp = TempDir::new("early-exit");
        let error = ManagedProcess::spawn(
            "early exit fixture".to_string(),
            shell("printf 'fatal fixture error\\n' >&2; exit 23"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect_err("fixture must exit before readiness");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited before readiness marker"),
            "{message}"
        );
        assert!(message.contains("23"), "{message}");
        assert!(message.contains("fatal fixture error"), "{message}");
    }

    #[test]
    fn managed_process_readiness_timeout_is_bounded() {
        let temp = TempDir::new("timeout");
        let timeout = Duration::from_millis(150);
        let started = Instant::now();
        let error = ManagedProcess::spawn(
            "timeout fixture".to_string(),
            shell("sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            timeout,
            temp.path().join("fixture.log"),
        )
        .expect_err("fixture must time out");
        let elapsed = started.elapsed();
        assert!(
            elapsed <= timeout + Duration::from_millis(250),
            "timeout {timeout:?} took {elapsed:?}"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for readiness marker"),
            "{error:#}"
        );
    }

    #[test]
    fn managed_process_supports_file_readiness_markers() {
        let temp = TempDir::new("file-ready");
        let ready_path = temp.path().join("ready.txt");
        let command = shell_with_arg(
            "printf 'prefix FILE_READY suffix\\n' > \"$1\"; sleep 30",
            &ready_path,
        );
        let mut process = ManagedProcess::spawn(
            "file marker fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("file marker becomes ready");
        process.kill_now().expect("kill file marker fixture");
    }

    #[test]
    fn managed_process_stop_delivers_sigterm() {
        let temp = TempDir::new("sigterm");
        let term_path = temp.path().join("term.txt");
        let command = shell_with_arg(
            "trap 'printf TERM > \"$1\"; exit 0' TERM; printf 'READY\\n'; while :; do sleep 1; done",
            &term_path,
        );
        let mut process = ManagedProcess::spawn(
            "SIGTERM fixture".to_string(),
            command,
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn SIGTERM fixture");

        process.stop().expect("stop fixture gracefully");
        assert_eq!(
            fs::read_to_string(term_path).expect("read TERM marker"),
            "TERM"
        );
    }

    #[test]
    fn managed_process_drop_kills_descendants_and_reaps_child() {
        let temp = TempDir::new("descendant");
        let descendant_path = temp.path().join("descendant.pid");
        let child_pid;
        let descendant_pid;
        {
            let command = shell_with_arg(
                "sleep 30 & descendant=$!; printf '%s' \"$descendant\" > \"$1\"; printf 'READY\\n'; wait",
                &descendant_path,
            );
            let process = ManagedProcess::spawn(
                "descendant fixture".to_string(),
                command,
                ReadyMarker::StdoutContains("READY".to_string()),
                Duration::from_secs(2),
                temp.path().join("fixture.log"),
            )
            .expect("spawn descendant fixture");
            child_pid = process.pid();
            descendant_pid = fs::read_to_string(&descendant_path)
                .expect("read descendant pid")
                .parse::<u32>()
                .expect("parse descendant pid");
            assert!(pid_exists(child_pid));
            assert!(pid_exists(descendant_pid));
        }

        assert!(wait_until(Duration::from_secs(1), || !pid_exists(
            child_pid
        )));
        assert!(wait_until(Duration::from_secs(1), || !pid_exists(
            descendant_pid
        )));
    }

    #[test]
    fn managed_process_can_restart_with_a_new_command() {
        let temp = TempDir::new("restart");
        let mut process = ManagedProcess::spawn(
            "restart fixture".to_string(),
            shell("printf 'FIRST_READY\\n'; sleep 30"),
            ReadyMarker::StdoutContains("FIRST_READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("first.log"),
        )
        .expect("spawn first process");
        let first_pid = process.pid();

        process
            .restart(
                shell("printf 'SECOND_READY\\n'; sleep 30"),
                ReadyMarker::StdoutContains("SECOND_READY".to_string()),
                Duration::from_secs(2),
                temp.path().join("second.log"),
            )
            .expect("restart process");
        assert_ne!(process.pid(), first_pid);
        assert!(process.stdout_tail().contains("SECOND_READY"));
        process.kill_now().expect("kill restarted process");
    }
}
