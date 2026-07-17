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
#[cfg(unix)]
use std::ffi::c_void;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

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
const P_PID: i32 = 1;
#[cfg(unix)]
const WNOHANG: i32 = 0x00000001;
#[cfg(unix)]
const WEXITED: i32 = 0x00000004;
#[cfg(any(target_os = "linux", target_os = "android"))]
const WNOWAIT: i32 = 0x01000000;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const WNOWAIT: i32 = 0x00000020;

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
compile_error!(
    "ManagedProcess requires verified waitid WNOWAIT ABI constants for this Unix target"
);

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn send_signal(pid: i32, signal: i32) -> i32;
    fn waitid(idtype: i32, id: u32, info: *mut c_void, options: i32) -> i32;
}

#[cfg(unix)]
#[repr(C, align(16))]
struct OpaqueSiginfo {
    signal: i32,
    remaining: [u8; 252],
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessGroupPhase {
    SignalsAllowed,
    FinalSignalSent,
    LeaderReaped,
}

#[cfg(unix)]
#[derive(Debug)]
struct ProcessGroupOwnership {
    id: u32,
    phase: ProcessGroupPhase,
}

#[cfg(unix)]
impl ProcessGroupOwnership {
    fn new(id: u32) -> Self {
        Self {
            id,
            phase: ProcessGroupPhase::SignalsAllowed,
        }
    }

    fn group_id_for_signal(&self) -> Result<u32> {
        if self.phase != ProcessGroupPhase::SignalsAllowed {
            bail!(
                "process group {} no longer permits group-directed signals after the final signal",
                self.id
            );
        }
        Ok(self.id)
    }

    fn record_final_group_signal(&mut self) -> Result<()> {
        if self.phase != ProcessGroupPhase::SignalsAllowed {
            bail!(
                "process group {} final signal was already recorded",
                self.id
            );
        }
        self.phase = ProcessGroupPhase::FinalSignalSent;
        Ok(())
    }

    fn permit_reap(&self) -> Result<()> {
        if self.phase != ProcessGroupPhase::FinalSignalSent {
            bail!(
                "process group {} leader cannot be reaped before the final group signal",
                self.id
            );
        }
        Ok(())
    }

    fn record_reaped(&mut self) -> Result<()> {
        self.permit_reap()?;
        self.phase = ProcessGroupPhase::LeaderReaped;
        Ok(())
    }

    fn awaiting_reap(&self) -> bool {
        self.phase == ProcessGroupPhase::FinalSignalSent
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadyMarker {
    StdoutContains(String),
    FileContains { path: PathBuf, needle: String },
}

#[derive(Debug)]
enum ReadinessBaseline {
    Stdout,
    File {
        snapshot: Option<FileReadinessSnapshot>,
    },
}

#[derive(Debug)]
struct FileReadinessSnapshot {
    bytes: Vec<u8>,
    modified: Option<SystemTime>,
}

impl FileReadinessSnapshot {
    fn read(path: &std::path::Path) -> std::io::Result<Option<Self>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let modified = fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .ok();
        Ok(Some(Self { bytes, modified }))
    }
}

type SharedLogWriter = Arc<Mutex<Box<dyn Write + Send>>>;
type SharedOutputIoError = Arc<Mutex<Option<String>>>;

impl ReadinessBaseline {
    fn capture(marker: &ReadyMarker) -> Result<Self> {
        let ReadyMarker::FileContains { path, .. } = marker else {
            return Ok(Self::Stdout);
        };
        let snapshot = FileReadinessSnapshot::read(path)
            .with_context(|| format!("capture readiness file baseline {}", path.display()))?;
        Ok(Self::File { snapshot })
    }

    fn file_contains_fresh_marker(&self, current: &FileReadinessSnapshot, needle: &str) -> bool {
        let Self::File { snapshot: baseline } = self else {
            return false;
        };
        let fresh_bytes = match baseline {
            None => current.bytes.as_slice(),
            Some(baseline)
                if current.bytes == baseline.bytes && current.modified == baseline.modified =>
            {
                return false;
            }
            Some(baseline) if current.bytes == baseline.bytes => current.bytes.as_slice(),
            Some(baseline) if current.bytes.starts_with(&baseline.bytes) => {
                &current.bytes[baseline.bytes.len()..]
            }
            Some(_) => current.bytes.as_slice(),
        };
        String::from_utf8_lossy(fresh_bytes).contains(needle)
    }
}

pub(crate) struct ManagedProcess {
    label: String,
    child: Child,
    #[cfg(unix)]
    process_group: ProcessGroupOwnership,
    log_path: PathBuf,
    log_file: SharedLogWriter,
    output_io_error: SharedOutputIoError,
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
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
    ) -> Result<Self> {
        Self::spawn_impl(label, command, marker, timeout, log_path, None)
    }

    #[cfg(test)]
    fn spawn_with_log_writer(
        label: String,
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
        log_writer: Box<dyn Write + Send>,
    ) -> Result<Self> {
        Self::spawn_impl(label, command, marker, timeout, log_path, Some(log_writer))
    }

    fn spawn_impl(
        label: String,
        mut command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
        log_writer: Option<Box<dyn Write + Send>>,
    ) -> Result<Self> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        let readiness_baseline = ReadinessBaseline::capture(&marker)?;
        let log_file: Box<dyn Write + Send> = match log_writer {
            Some(log_writer) => log_writer,
            None => Box::new(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&log_path)
                    .with_context(|| format!("open durable process log {}", log_path.display()))?,
            ),
        };
        let log_file = Arc::new(Mutex::new(log_file));
        let output_io_error = Arc::new(Mutex::new(None));

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
            "stdout",
            stdout,
            Arc::clone(&stdout_buffer),
            Arc::clone(&log_file),
            Arc::clone(&output_io_error),
            stdout_marker,
            Some(ready_tx),
        );
        let stderr_thread = spawn_reader(
            "stderr",
            stderr,
            Arc::clone(&stderr_buffer),
            Arc::clone(&log_file),
            Arc::clone(&output_io_error),
            None,
            None,
        );

        let mut process = Self {
            label,
            #[cfg(unix)]
            process_group: ProcessGroupOwnership::new(child.id()),
            child,
            log_path,
            log_file,
            output_io_error,
            stdout_buffer,
            stderr_buffer,
            stdout_thread: Some(stdout_thread),
            stderr_thread: Some(stderr_thread),
            stopped: false,
        };
        if let Err(error) =
            process.wait_for_ready(&marker, &readiness_baseline, &ready_rx, deadline, timeout)
        {
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
        self.ensure_output_io_ok("assert durable log contents")?;
        match self.log_file.lock() {
            Ok(mut log_file) => {
                if let Err(error) = log_file.flush() {
                    record_output_io_error(
                        &self.output_io_error,
                        format!(
                            "flush durable process log {}: {error}",
                            self.log_path.display()
                        ),
                    );
                }
            }
            Err(_) => record_output_io_error(
                &self.output_io_error,
                format!("lock durable process log {}", self.log_path.display()),
            ),
        }
        self.ensure_output_io_ok("assert durable log contents")?;
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
            return self.ensure_output_io_ok("stop process");
        }
        #[cfg(unix)]
        {
            if self.process_group.awaiting_reap() {
                self.reap_after_final_group_signal("retry wait after final group signal")?;
                return self.ensure_output_io_ok("stop process");
            }
            self.signal_group(SIGTERM)?;
            let deadline = Instant::now() + STOP_TIMEOUT;
            loop {
                if self.leader_exit_observed()? {
                    self.finish_group_with_signal(
                        SIGKILL,
                        "wait for process-group cleanup after SIGTERM",
                    )?;
                    return self.ensure_output_io_ok("stop process");
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
            self.finish_group_with_signal(SIGKILL, "wait after SIGKILL timeout")?;
            return self.ensure_output_io_ok("stop process");
        }

        #[cfg(not(unix))]
        {
            if self.child.try_wait()?.is_none() {
                self.child.kill()?;
                let _ = self.child.wait()?;
            }
            self.stopped = true;
            self.join_output_threads();
            self.ensure_output_io_ok("stop process")
        }
    }

    pub(crate) fn kill_now(&mut self) -> Result<()> {
        if self.stopped {
            self.join_output_threads();
            return self.ensure_output_io_ok("kill process");
        }
        #[cfg(unix)]
        {
            if self.process_group.awaiting_reap() {
                self.reap_after_final_group_signal("retry wait after final group signal")?;
                return self.ensure_output_io_ok("kill process");
            }
            self.finish_group_with_signal(SIGKILL, "wait after immediate SIGKILL")?;
            return self.ensure_output_io_ok("kill process");
        }
        #[cfg(not(unix))]
        {
            if self.child.try_wait()?.is_none() {
                self.child.kill()?;
            }

            let _ = self
                .child
                .wait()
                .with_context(|| format!("wait for {} after immediate kill", self.label))?;
            self.stopped = true;
            self.join_output_threads();
            self.ensure_output_io_ok("kill process")
        }
    }

    pub(crate) fn runtime_diagnostic(
        &mut self,
        label: &str,
        endpoint: &str,
        config_path: &std::path::Path,
    ) -> Result<String> {
        self.ensure_output_io_ok("collect runtime diagnostics")?;
        let pid = self.pid();
        #[cfg(unix)]
        let exited = self.leader_exit_observed().with_context(|| {
            format!("inspect {label} pid={pid} endpoint={endpoint} process status")
        })?;
        #[cfg(not(unix))]
        let exited = self
            .child
            .try_wait()
            .with_context(|| {
                format!("inspect {label} pid={pid} endpoint={endpoint} process status")
            })?
            .is_some();
        let stdout_tail = self.stdout_tail();
        let stderr_tail = self.stderr_tail();
        if exited {
            bail!(
                "{label} exited pid={pid} endpoint={endpoint} config={} stdout_tail={stdout_tail:?} stderr_tail={stderr_tail:?}",
                config_path.display()
            );
        }
        Ok(format!(
            "{label}=running pid={pid} endpoint={endpoint} config={} stdout_tail={stdout_tail:?} stderr_tail={stderr_tail:?}",
            config_path.display()
        ))
    }

    fn wait_for_ready(
        &mut self,
        marker: &ReadyMarker,
        readiness_baseline: &ReadinessBaseline,
        ready_rx: &mpsc::Receiver<()>,
        deadline: Instant,
        timeout: Duration,
    ) -> Result<()> {
        loop {
            #[cfg(unix)]
            if self.leader_exit_observed()? {
                let status = self
                    .finish_group_with_signal(SIGKILL, "wait after exit before readiness marker")?;
                bail!(
                    "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }
            #[cfg(not(unix))]
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
                    Ok(()) => {
                        self.ensure_output_io_ok("confirm readiness")?;
                        return Ok(());
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) => {
                        #[cfg(unix)]
                        if self.leader_exit_observed()? {
                            let status = self.finish_group_with_signal(
                                SIGKILL,
                                "wait after stdout closed before readiness marker",
                            )?;
                            bail!(
                                "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                                self.label,
                                self.stdout_tail(),
                                self.stderr_tail(),
                                self.log_path.display()
                            );
                        }
                        #[cfg(not(unix))]
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
                    if FileReadinessSnapshot::read(path)
                        .ok()
                        .flatten()
                        .is_some_and(|snapshot| {
                            readiness_baseline.file_contains_fresh_marker(&snapshot, needle)
                        })
                    {
                        self.ensure_output_io_ok("confirm readiness")?;
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

    fn ensure_output_io_ok(&self, operation: &str) -> Result<()> {
        let error = self
            .output_io_error
            .lock()
            .map_err(|_| anyhow::anyhow!("process output I/O error state lock poisoned"))?
            .clone();
        if let Some(error) = error {
            bail!(
                "{} cannot {operation}: {error}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                self.label,
                self.stdout_tail(),
                self.stderr_tail(),
                self.log_path.display()
            );
        }
        Ok(())
    }

    #[cfg(unix)]
    fn signal_group(&self, signal: i32) -> Result<()> {
        let process_group_id = i32::try_from(self.process_group.group_id_for_signal()?)
            .context("managed process group id exceeds i32")?;
        // SAFETY: POSIX kill accepts a negative process-group id. This id was
        // assigned to the child by process_group(0) immediately before spawn.
        if unsafe { send_signal(-process_group_id, signal) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(ESRCH) {
            return Ok(());
        }
        if error.raw_os_error() == Some(EPERM) && self.leader_exit_observed()? {
            // Darwin reports EPERM when a process group contains only the
            // unreaped leader zombie. The WNOWAIT observation proves that the
            // leader PID is still owned and cannot have been reused.
            return Ok(());
        }
        Err(error).with_context(|| {
            format!(
                "send signal {signal} to {} process group {}",
                self.label, process_group_id
            )
        })
    }

    #[cfg(unix)]
    fn leader_exit_observed(&self) -> Result<bool> {
        if self.stopped {
            return Ok(true);
        }

        let mut info = OpaqueSiginfo {
            signal: 0,
            remaining: [0; 252],
        };
        // SAFETY: `info` is deliberately over-sized and over-aligned for the
        // supported Unix siginfo_t ABIs. WNOWAIT observes the direct child
        // without reaping its process-group leader PID.
        let result = unsafe {
            waitid(
                P_PID,
                self.child.id(),
                (&mut info as *mut OpaqueSiginfo).cast::<c_void>(),
                WEXITED | WNOHANG | WNOWAIT,
            )
        };
        if result == 0 {
            return Ok(info.signal != 0);
        }
        let error = std::io::Error::last_os_error();
        Err(error).with_context(|| {
            format!(
                "observe {} process leader {} without reaping",
                self.label,
                self.child.id()
            )
        })
    }

    #[cfg(unix)]
    fn finish_group_with_signal(&mut self, signal: i32, wait_context: &str) -> Result<ExitStatus> {
        self.signal_group(signal)?;
        self.process_group.record_final_group_signal()?;
        self.reap_after_final_group_signal(wait_context)
    }

    #[cfg(unix)]
    fn reap_after_final_group_signal(&mut self, wait_context: &str) -> Result<ExitStatus> {
        self.process_group.permit_reap()?;
        let status = self
            .child
            .wait()
            .with_context(|| format!("{wait_context} for {}", self.label))?;
        self.process_group.record_reaped()?;
        self.stopped = true;
        self.join_output_threads();
        Ok(status)
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn spawn_reader<R: Read + Send + 'static>(
    stream_name: &'static str,
    mut reader: R,
    tail: Arc<Mutex<String>>,
    log_file: SharedLogWriter,
    output_io_error: SharedOutputIoError,
    ready_marker: Option<String>,
    ready_tx: Option<mpsc::SyncSender<()>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        let mut marker_scan = String::new();
        let mut ready_sent = false;
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => count,
                Err(error) => {
                    record_output_io_error(
                        &output_io_error,
                        format!("read managed process {stream_name}: {error}"),
                    );
                    break;
                }
            };
            let chunk = &buffer[..count];
            if let Ok(mut output) = tail.lock() {
                push_bounded_log_chunk(&mut output, chunk, LOG_TAIL_BYTES);
            }
            match log_file.lock() {
                Ok(mut log) => {
                    if let Err(error) = log.write_all(chunk) {
                        record_output_io_error(
                            &output_io_error,
                            format!("write durable process log from {stream_name}: {error}"),
                        );
                    } else if let Err(error) = log.flush() {
                        record_output_io_error(
                            &output_io_error,
                            format!("flush durable process log from {stream_name}: {error}"),
                        );
                    }
                }
                Err(_) => record_output_io_error(
                    &output_io_error,
                    format!("lock durable process log for {stream_name}"),
                ),
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

fn record_output_io_error(errors: &SharedOutputIoError, error: String) {
    if let Ok(mut first_error) = errors.lock()
        && first_error.is_none()
    {
        *first_error = Some(error);
    }
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
    use super::{ManagedProcess, ProcessGroupOwnership, ReadyMarker};
    use std::fs;
    use std::io::{self, Write};
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

    struct FailingLogWriter;

    impl Write for FailingLogWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("injected durable log write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("injected durable log flush failure"))
        }
    }

    struct FailAfterFirstWrite {
        writes: usize,
    }

    impl Write for FailAfterFirstWrite {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes == 1 {
                Ok(buffer.len())
            } else {
                Err(io::Error::other("injected late durable log failure"))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn process_group_ownership_requires_final_signal_before_reap() {
        let mut ownership = ProcessGroupOwnership::new(42);

        assert!(ownership.permit_reap().is_err());
        assert_eq!(ownership.group_id_for_signal().expect("group owned"), 42);

        ownership
            .record_final_group_signal()
            .expect("record final group signal");
        ownership
            .permit_reap()
            .expect("final group signal permits reap");
        assert!(ownership.group_id_for_signal().is_err());

        ownership.record_reaped().expect("record leader reap");
        assert!(ownership.group_id_for_signal().is_err());
    }

    #[test]
    fn runtime_diagnostics_preserves_process_group_ownership_until_cleanup() {
        let temp = TempDir::new("diagnostic-unreaped");
        let config_path = temp.path().join("fixture.toml");
        fs::write(&config_path, "fixture = true\n").expect("write fixture config");
        let mut process = ManagedProcess::spawn(
            "diagnostic unreaped fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.05; sleep 30 & exit 23"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn diagnostic fixture");

        assert!(wait_until(Duration::from_secs(1), || process
            .runtime_diagnostic("fixture", "local", &config_path)
            .is_err()));
        assert_eq!(
            process
                .process_group
                .group_id_for_signal()
                .expect("runtime diagnostics must retain process-group ownership"),
            process.pid()
        );
        process
            .kill_now()
            .expect("cleanup after non-reaping runtime diagnostics");
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
    fn managed_process_surfaces_log_failure_after_draining_stdout() {
        let temp = TempDir::new("log-write-failure");
        let error = ManagedProcess::spawn_with_log_writer(
            "log write failure fixture".to_string(),
            shell(
                "printf 'FIRST_CHUNK\n'; i=0; while [ $i -lt 600 ]; do printf 'fill-%04d-abcdefghij\n' \"$i\"; i=$((i + 1)); done; printf 'DRAINED_AFTER_LOG_FAILURE\nREADY\n'; sleep 30",
            ),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
            Box::new(FailingLogWriter),
        )
        .expect_err("durable log failure must fail readiness");
        let message = format!("{error:#}");

        assert!(
            message.contains("injected durable log write failure"),
            "{message}"
        );
        assert!(message.contains("DRAINED_AFTER_LOG_FAILURE"), "{message}");
    }

    #[test]
    fn managed_process_stop_surfaces_log_failure_after_readiness() {
        let temp = TempDir::new("late-log-write-failure");
        let mut process = ManagedProcess::spawn_with_log_writer(
            "late log write failure fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.05; printf 'LATE_OUTPUT\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
            Box::new(FailAfterFirstWrite { writes: 0 }),
        )
        .expect("first durable log write permits readiness");
        assert!(wait_until(Duration::from_secs(1), || process
            .stdout_tail()
            .contains("LATE_OUTPUT")));

        let error = process
            .kill_now()
            .expect_err("late durable log failure must surface during stop");
        let message = format!("{error:#}");
        assert!(
            message.contains("injected late durable log failure"),
            "{message}"
        );
        assert!(message.contains("LATE_OUTPUT"), "{message}");
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
    fn managed_process_early_exit_with_inherited_pipes_is_bounded() {
        let temp = TempDir::new("early-exit-inherited-pipes");
        let timeout = Duration::from_millis(100);
        let started = Instant::now();
        let error = ManagedProcess::spawn(
            "early exit inherited pipes fixture".to_string(),
            shell("sleep 2 & printf 'parent failed before ready\n' >&2; exit 23"),
            ReadyMarker::StdoutContains("READY".to_string()),
            timeout,
            temp.path().join("fixture.log"),
        )
        .expect_err("parent must exit before readiness");
        let elapsed = started.elapsed();
        let message = format!("{error:#}");

        assert!(
            elapsed <= timeout + Duration::from_millis(350),
            "early exit cleanup exceeded bound: timeout={timeout:?} elapsed={elapsed:?}; {message}"
        );
        assert!(message.contains("23"), "{message}");
        assert!(message.contains("parent failed before ready"), "{message}");
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
    fn managed_process_rejects_stale_file_readiness_marker() {
        let temp = TempDir::new("stale-file-ready");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "STALE FILE_READY\n").expect("write stale readiness marker");
        let timeout = Duration::from_millis(120);
        let started = Instant::now();

        let error = ManagedProcess::spawn(
            "stale file marker fixture".to_string(),
            shell("sleep 30"),
            ReadyMarker::FileContains {
                path: ready_path.clone(),
                needle: "FILE_READY".to_string(),
            },
            timeout,
            temp.path().join("fixture.log"),
        )
        .expect_err("pre-existing marker must not satisfy readiness");

        assert!(
            started.elapsed() >= timeout,
            "stale marker returned before startup timeout"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for readiness marker"),
            "{error:#}"
        );
        assert_eq!(
            fs::read_to_string(ready_path).expect("read preserved stale marker"),
            "STALE FILE_READY\n",
            "ManagedProcess must not delete caller-owned readiness files"
        );
    }

    #[test]
    fn managed_process_accepts_file_marker_appended_after_spawn() {
        let temp = TempDir::new("appended-file-ready");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "existing prefix\n").expect("write file baseline");
        let command = shell_with_arg(
            "sleep 0.05; printf 'FILE_READY\n' >> \"$1\"; sleep 30",
            &ready_path,
        );

        let mut process = ManagedProcess::spawn(
            "appended file marker fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("marker appended after spawn becomes ready");
        process.kill_now().expect("kill appended marker fixture");
    }

    #[test]
    fn managed_process_accepts_file_marker_after_truncate() {
        let temp = TempDir::new("truncated-file-ready");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "old generation\n").expect("write file baseline");
        let command = shell_with_arg(
            "sleep 0.05; printf 'FILE_READY\n' > \"$1\"; sleep 30",
            &ready_path,
        );

        let mut process = ManagedProcess::spawn(
            "truncated file marker fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("marker written after truncate becomes ready");
        process.kill_now().expect("kill truncated marker fixture");
    }

    #[test]
    fn managed_process_accepts_same_marker_rewritten_after_truncate() {
        let temp = TempDir::new("same-marker-rewritten");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "FILE_READY\n").expect("write stale marker generation");
        let command = shell_with_arg(
            "sleep 0.05; printf 'FILE_READY\n' > \"$1\"; sleep 30",
            &ready_path,
        );

        let mut process = ManagedProcess::spawn(
            "same marker rewritten fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_millis(300),
            temp.path().join("fixture.log"),
        )
        .expect("same marker rewritten after truncate becomes fresh");
        process
            .kill_now()
            .expect("kill same marker rewritten fixture");
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
