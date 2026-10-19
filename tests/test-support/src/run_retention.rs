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

//! Run ownership and the directory/pin handoff, shared with the std-only guard.
#![allow(dead_code)]
use crate::unix;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static RUNS: OnceLock<Mutex<HashMap<PathBuf, Arc<RunContext>>>> = OnceLock::new();
static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn unique() -> String {
    format!(
        "{:032x}-{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        std::process::id(),
        SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

pub fn open_lock(path: &Path) -> io::Result<File> {
    #[cfg(target_os = "macos")]
    const NOFOLLOW: i32 = 0x100;
    #[cfg(target_os = "linux")]
    const NOFOLLOW: i32 = 0x20000;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(unix::error("test run lock must be an owned regular file"));
    }
    Ok(file)
}

pub fn lock_until(file: &File, shared: bool, deadline: Instant) -> io::Result<()> {
    loop {
        if Instant::now() >= deadline {
            return Err(unix::error("test run lock deadline expired"));
        }
        let result = if shared {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };
        match result {
            Ok(()) => {
                if Instant::now() >= deadline {
                    return Err(unix::error(
                        "test run lock acquired after original deadline",
                    ));
                }
                return Ok(());
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
            Err(std::fs::TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return Err(unix::error("test run lock deadline expired"));
                }
                std::thread::sleep(
                    unix::POLL.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
    }
}

fn write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)
}

pub fn quote(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub struct RunContext {
    pub root: PathBuf,
    pub formal: bool,
    pub pin: File,
    pub directory: File,
}

impl RunContext {
    pub fn verify_root(&self) -> io::Result<()> {
        let path = fs::symlink_metadata(&self.root)?;
        let original = self.directory.metadata()?;
        if !path.is_dir() || path.dev() != original.dev() || path.ino() != original.ino() {
            return Err(unix::error(
                "original test run directory was replaced or moved",
            ));
        }
        Ok(())
    }
    pub fn verify_file(&self, file: &File) -> io::Result<()> {
        self.verify_root()?;
        if file.metadata()?.dev() != self.directory.metadata()?.dev() {
            return Err(unix::error(
                "test run control/output must remain on the approved root filesystem; submounts are unsupported",
            ));
        }
        Ok(())
    }
    pub fn gate(&self, deadline: Instant) -> io::Result<File> {
        let gate = open_lock(&self.root.join(".gate"))?;
        self.verify_file(&gate)?;
        lock_until(&gate, true, deadline)?;
        if self.root.join("closing").exists() || self.root.join("Closed").exists() {
            // Rejection is an enduring fact, even if no launch was registered.
            write_new(
                &self.root.join(format!("late-registration-{}", unique())),
                b"registration after closing\n",
            )?;
            return Err(unix::error("test run registration is closed"));
        }
        Ok(gate)
    }
}

fn ordinary_base() -> io::Result<PathBuf> {
    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .ok_or_else(|| unix::error("test run state requires XDG_STATE_HOME or HOME"))?;
    if !state.is_absolute() {
        return Err(unix::error("test run state directory must be absolute"));
    }
    Ok(state.join("novarocks/test-runs"))
}

fn identity() -> io::Result<Option<String>> {
    let Some(run) = std::env::var_os("NEXTEST_RUN_ID") else {
        if std::env::var_os("NEXTEST").is_some() {
            return Err(unix::error("nextest execution is missing NEXTEST_RUN_ID"));
        }
        return Ok(None);
    };
    let run = run
        .into_string()
        .map_err(|_| unix::error("nextest run ID is not UTF-8"))?;
    if run.len() != 36
        || !run.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
    {
        return Err(unix::error("invalid native nextest run ID"));
    }
    for name in ["NEXTEST_BINARY_ID", "NEXTEST_TEST_NAME"] {
        if std::env::var_os(name).is_none_or(|v| v.is_empty()) {
            return Err(unix::error(format!(
                "nextest managed operation requires {name}"
            )));
        }
    }
    let version = std::env::var("NEXTEST_VERSION").map_err(unix::error)?;
    let parts = version
        .split(['.', '-'])
        .take(3)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(unix::error)?;
    if parts.len() != 3 || (parts[0], parts[1], parts[2]) < (0, 9, 148) {
        return Err(unix::error(
            "managed nextest execution requires version 0.9.148 or newer",
        ));
    }
    Ok(Some(run))
}

pub fn ensure_run_retention(deadline: Instant) -> io::Result<Arc<RunContext>> {
    let native = identity()?;
    if let Some(formal) = std::env::var_os("NOVAROCKS_TEST_RUN_ROOT") {
        let root = PathBuf::from(formal);
        if !root.is_absolute() || !root.join(".formal").is_file() {
            return Err(unix::error("invalid formal test run root"));
        }
        let directory = File::open(&root)?;
        unix::validate_local_fs(&directory)?;
        let pin = open_lock(&root.join(".pin"))?;
        lock_until(&pin, true, deadline)?;
        let context = Arc::new(RunContext {
            root,
            formal: true,
            pin,
            directory,
        });
        context.verify_file(&context.pin)?;
        drop(context.gate(deadline)?);
        return Ok(context);
    }
    let base = ordinary_base()?;
    let root = if let Some(run) = &native {
        base.join("nextest").join(run)
    } else {
        base.join("process").join(process_id())
    };
    let runs = RUNS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut runs = loop {
        match runs.try_lock() {
            Ok(guard) => break guard,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(unix::error("ordinary run initialization lock poisoned"));
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return Err(unix::error(
                        "original deadline expired waiting for local run initialization",
                    ));
                }
                std::thread::sleep(
                    unix::POLL.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
    };
    if let Some(context) = runs.get(&root) {
        if native.is_some() {
            verify_ready(context)?;
        }
        drop(context.gate(deadline)?);
        return Ok(Arc::clone(context));
    }
    fs::create_dir_all(&base)?;
    let base_directory = File::open(&base)?;
    unix::validate_local_fs(&base_directory)?;
    let parent = root.parent().unwrap();
    fs::create_dir_all(parent)?;
    let namespace = open_lock(&base.join(".namespace"))?;
    let (pin, initialize, created, directory) = loop {
        lock_until(&namespace, false, deadline)?;
        let result = (|| -> io::Result<_> {
            let created = if root.exists() {
                false
            } else {
                if native.is_some() {
                    unix::verify_nextest_parent(unix::parent_id())?;
                }
                fs::create_dir(&root)?;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
                true
            };
            let directory = File::open(&root)?;
            unix::validate_local_fs(&directory)?;
            if directory.metadata()?.dev() != base_directory.metadata()?.dev()
                || !fs::symlink_metadata(&root)?.is_dir()
            {
                return Err(unix::error(
                    "ordinary run directory cannot cross the approved state filesystem",
                ));
            }
            let pin = open_lock(&root.join(".pin"))?;
            if pin.metadata()?.dev() != directory.metadata()?.dev() {
                return Err(unix::error("run pin crosses the approved filesystem"));
            }
            // No blocking pin/init acquisition while holding the directory gate.
            pin.try_lock_shared().map_err(|e| match e {
                std::fs::TryLockError::Error(e) => e,
                _ => io::ErrorKind::WouldBlock.into(),
            })?;
            let initialize = open_lock(&root.join(".initialize"))?;
            if initialize.metadata()?.dev() != directory.metadata()?.dev() {
                return Err(unix::error(
                    "run initialization lock crosses the approved filesystem",
                ));
            }
            if created {
                initialize.try_lock().map_err(|e| match e {
                    std::fs::TryLockError::Error(e) => e,
                    _ => io::ErrorKind::WouldBlock.into(),
                })?;
                write_new(&root.join("Initializing"), b"schema=1\n")?;
                let count = std::env::var("NOVAROCKS_TEST_RUN_RETENTION_COUNT")
                    .unwrap_or_else(|_| "3".into())
                    .parse::<usize>()
                    .map_err(unix::error)?;
                if count == 0 {
                    return Err(unix::error("test run retention count must be positive"));
                }
                write_new(
                    &root.join(".ordinary"),
                    format!(
                        "{}\n{count}\n",
                        if native.is_some() {
                            "nextest"
                        } else {
                            "process"
                        }
                    )
                    .as_bytes(),
                )?;
                write_new(&root.join(".created"), unique().as_bytes())?;
                open_lock(&root.join(".gate"))?;
            }
            Ok((pin, initialize, created, directory))
        })();
        // This independently opened lock has no inherited aliases.
        namespace.unlock()?;
        match result {
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(unix::POLL)
            }
            result => break result?,
        }
    };
    let context = Arc::new(RunContext {
        root: root.clone(),
        formal: false,
        pin,
        directory,
    });
    if !created {
        lock_until(&initialize, false, deadline)?;
    }
    context.verify_root()?;
    if native.is_some() {
        if created {
            bootstrap_guard(&context, deadline)?;
        } else {
            verify_ready(&context)?;
        }
    }
    drop(initialize); // Never unlock an inherited open-file description.
    drop(context.gate(deadline)?);
    runs.insert(root, Arc::clone(&context));
    // Collection is bounded and opportunistic; it never blocks registration.
    if let Ok(count) = fs::read_to_string(context.root.join(".ordinary")).and_then(|s| {
        s.lines()
            .nth(1)
            .unwrap_or("")
            .parse::<usize>()
            .map_err(unix::error)
    }) {
        let base = base.clone();
        let _ = std::thread::Builder::new()
            .name("test-material-gc".into())
            .spawn(move || {
                if let Err(e) = collect(&base, count) {
                    eprintln!("test material collection failed: {e}");
                }
            });
    }
    Ok(context)
}

fn process_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(unique)
}

fn verify_ready(context: &RunContext) -> io::Result<()> {
    context.verify_root()?;
    if !context.root.join("Ready").is_file()
        || context.root.join("closing").exists()
        || context.root.join("Closed").exists()
    {
        return Err(unix::error(
            "nextest retention lease is not Ready/Open; outer tests must ensure retention before spawning nested runners; failed roots are never adopted",
        ));
    }
    let lease = open_lock(&context.root.join(".lease"))?;
    context.verify_file(&lease)?;
    match lease.try_lock() {
        Err(std::fs::TryLockError::WouldBlock) => Ok(()),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
        Ok(()) => Err(unix::error(
            "nextest retention guard lost its lease; diagnostics retained",
        )),
    }
}

fn bootstrap_guard(context: &RunContext, deadline: Instant) -> io::Result<()> {
    let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
    let parent = unix::parent_id();
    unix::verify_nextest_parent(parent)?;
    let lease = open_lock(&context.root.join(".lease"))?;
    context.verify_file(&lease)?;
    lock_until(&lease, false, deadline)?;
    let (read, mut write) = unix::fifo_pair(&context.root.join(".guard-control"))?;
    let (mut ack_read, ack_write) = unix::fifo_pair(&context.root.join(".guard-ack"))?;
    for file in [&read, &write, &ack_read, &ack_write] {
        context.verify_file(file)?;
    }
    let copies = [&read, &ack_write, &context.pin, &lease]
        .into_iter()
        .map(unix::high_copy)
        .collect::<io::Result<Vec<_>>>()?;
    let numbers: Vec<_> = copies.iter().map(AsRawFd::as_raw_fd).collect();
    // Prepare the fallible thread before there can be any Child wait obligation.
    let (send, receive) = std::sync::mpsc::channel::<Child>();
    let waiter = std::thread::Builder::new()
        .name("test-retention-wait".into())
        .spawn(move || {
            if let Ok(mut child) = receive.recv() {
                if let Err(error) = child.wait() {
                    eprintln!("wait test retention guard: {error}");
                }
            }
        })?;
    let absolute = unix::absolute_deadline(deadline)?;
    let mut command =
        Command::new(option_env!("NOVAROCKS_TEST_MONITOR").expect("library monitor build path"));
    command
        .arg("retention-guard")
        .arg(&context.root)
        .arg(parent.to_string())
        .arg(absolute.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(move || {
            for (i, fd) in numbers.iter().enumerate() {
                unix::map_fd(*fd, i as i32 + 3)?;
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    drop(copies);
    drop(read);
    drop(ack_write);
    let limit = deadline;
    let result = (|| {
        read_guard_byte(&mut ack_read, b'R', limit, &child, &context.root)?;
        unix::verify_nextest_parent(parent)?;
        if context.root.join("closing").exists() {
            return Err(unix::error("retention root closed during bootstrap"));
        }
        write.write_all(&unix::control_frame(b'A', absolute))?;
        read_guard_byte(&mut ack_read, b'Y', limit, &child, &context.root)
    })();
    if let Err(error) = &result {
        let _ = write_new(&context.root.join("closing"), b"bootstrap failed\n");
        let _ = write_new(
            &context.root.join(".guard-error"),
            error.to_string().as_bytes(),
        );
        // Actual Child is still unreaped and has not been handed to a waiter.
        let _ = child.kill();
    }
    // The receiver thread was successfully created before spawn and is alive.
    if let Err(failed) = send.send(child) {
        let mut child = failed.0;
        let _ = child.kill();
        let _ = child.wait();
    }
    drop(waiter);
    drop(lease);
    result
}

fn read_guard_byte(
    reader: &mut File,
    expected: u8,
    deadline: Instant,
    child: &Child,
    root: &Path,
) -> io::Result<()> {
    loop {
        if Instant::now() >= deadline {
            return Err(unix::error("monitor acknowledgement deadline expired"));
        }
        let mut byte = [0u8];
        match reader.read(&mut byte) {
            Ok(1) if byte[0] == expected => {
                if Instant::now() >= deadline {
                    return Err(unix::error(
                        "monitor acknowledgement arrived after original deadline",
                    ));
                }
                return Ok(());
            }
            Ok(1) => return Err(unix::error("invalid monitor acknowledgement")),
            Ok(_) => {} // Writer may not have exec'd yet; the original budget still applies.
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        if let Some((code, status)) = unix::observe(child)? {
            let cause = fs::read_to_string(root.join(".guard-error"))
                .unwrap_or_else(|error| format!("guard diagnostic unavailable: {error}"));
            return Err(unix::error(format!(
                "retention guard exited before acknowledgement (code={code}, status={status}): {cause}"
            )));
        }
        std::thread::sleep(unix::POLL);
    }
}

pub fn close_registration(root: &Path, deadline: Instant) -> io::Result<()> {
    let directory = File::open(root)?;
    unix::validate_local_fs(&directory)?;
    match write_new(&root.join("closing"), b"closing\n") {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let gate = open_lock(&root.join(".gate"))?;
    if !same_directory(root, &directory)? || gate.metadata()?.dev() != directory.metadata()?.dev() {
        return Err(unix::error(
            "registration gate crossed the approved root filesystem",
        ));
    }
    lock_until(&gate, false, deadline)?;
    write_new(&root.join("Closed"), b"schema=1\n")
}

/// None means an unresolved obligation; Some(0) is a safely empty run.
fn completed_records(root: &Path, limit: Instant) -> io::Result<Option<usize>> {
    let mut directories = vec![root.join("monitor")];
    let mut count = 0;
    while let Some(directory) = directories.pop() {
        if Instant::now() >= limit {
            return Ok(None);
        }
        let entries = match fs::read_dir(directory) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Ok(None);
            }
            if kind.is_dir() {
                directories.push(entry.path());
            }
            if entry.file_name() == "record.jsonl" {
                let file = open_lock(&entry.path())?;
                if file.try_lock().is_err() || file.metadata()?.len() > 64 * 1024 * 1024 {
                    return Ok(None);
                }
                let text = fs::read_to_string(entry.path())?;
                if !text.lines().last().is_some_and(|line| {
                    line.starts_with("{\"schema\":1,\"event\":\"Final\",")
                        && line.ends_with("\"cleanup_complete\":true}")
                }) || !text.ends_with('\n')
                {
                    return Ok(None);
                }
                count += 1;
            }
        }
    }
    Ok(Some(count))
}

fn same_directory(path: &Path, original: &File) -> io::Result<bool> {
    let path = fs::symlink_metadata(path)?;
    let original = original.metadata()?;
    Ok(path.is_dir() && path.dev() == original.dev() && path.ino() == original.ino())
}

pub fn collect(base: &Path, retain: usize) -> io::Result<()> {
    let lock = open_lock(&base.join(".gc"))?;
    if lock.try_lock().is_err() {
        return Ok(());
    }
    let deadline = Instant::now() + Duration::from_millis(100);
    // Interrupted removals remain private owned trees and are retried. Never
    // descend through an external symlink or a formal evidence root.
    for entry in fs::read_dir(base)?.take(128) {
        if Instant::now() >= deadline {
            return Ok(());
        }
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with(".trash-")
            || !entry.file_type()?.is_dir()
        {
            continue;
        }
        let path = entry.path();
        if !path.join(".ordinary").is_file() || path.join(".formal").exists() {
            continue;
        }
        let pin = open_lock(&path.join(".pin"))?;
        if pin.try_lock().is_err() {
            continue;
        }
        if let Err(error) = fs::remove_dir_all(&path) {
            eprintln!(
                "retry retained test material deletion {}: {error}",
                path.display()
            );
        }
    }
    let mut candidates = vec![];
    for kind in ["nextest", "process"] {
        let Ok(entries) = fs::read_dir(base.join(kind)) else {
            continue;
        };
        for entry in entries.take(128) {
            if Instant::now() >= deadline {
                break;
            }
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let root = entry.path();
            if !root.join(".ordinary").is_file() || root.join(".formal").exists() {
                continue;
            }
            if kind == "nextest" && !root.join("Closed").is_file() {
                continue;
            }
            let directory = File::open(&root)?;
            unix::validate_local_fs(&directory)?;
            let pin = open_lock(&root.join(".pin"))?;
            if pin.metadata()?.dev() != directory.metadata()?.dev() || pin.try_lock().is_err() {
                continue;
            }
            if !same_directory(&root, &directory)? {
                continue;
            }
            if let Some(count) = completed_records(&root, deadline)? {
                let created = match fs::read_to_string(root.join(".created")) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                if created.len() < 32 || !created.as_bytes()[..32].iter().all(u8::is_ascii_hexdigit)
                {
                    continue;
                }
                candidates.push((created, entry.file_name(), root, pin, directory, count));
            }
        }
    }
    candidates.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    let mut retained = 0;
    for (_, _, root, _pin, directory, count) in candidates {
        if Instant::now() >= deadline {
            break;
        }
        if count != 0 && retained < retain {
            retained += 1;
            continue;
        }
        let namespace = open_lock(&base.join(".namespace"))?;
        if namespace.try_lock().is_err() {
            continue;
        }
        // Joining only try-locks a pin under the namespace gate. Collection
        // already owns its exclusive pin and rechecks the exact directory.
        if !same_directory(&root, &directory)? {
            continue;
        }
        let trash = base.join(format!(".trash-{}", unique()));
        let result = fs::rename(&root, &trash);
        drop(namespace);
        result?;
        if let Err(error) = fs::remove_dir_all(&trash) {
            eprintln!(
                "retain failed test material deletion {}: {error}",
                trash.display()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_deadline_cannot_acquire_an_available_registration_lock() {
        let path = std::env::temp_dir().join(format!("nr-expired-lock-{}", unique()));
        let file = open_lock(&path).unwrap();
        let error = lock_until(&file, false, Instant::now() - Duration::from_secs(1))
            .expect_err("expired registration must not acquire even an idle lock");
        assert!(error.to_string().contains("deadline expired"));
        let independent = open_lock(&path).unwrap();
        independent.try_lock().unwrap();
        drop(independent);
        drop(file);
        fs::remove_file(path).unwrap();
    }
}
