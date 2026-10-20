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

//! Durable cleanup facts for resources that outlive the creating process.
//!
//! Registration precedes creation. Dropping the handle makes no cleanup claim;
//! only an explicit settlement records the owner's actual cleanup result.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const DIRECTORY: &str = "resource-registrations";

/// A snapshot of registration paths before an invocation starts.
pub type RegistrationSnapshot = BTreeSet<PathBuf>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Record {
    schema: u32,
    owner: String,
    id: String,
    label: String,
    pid: u32,
    state: State,
    error: Option<String>,
    /// UTC milliseconds since the Unix epoch.
    registered_at: u64,
    settled_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Registered,
    Released,
    CleanupFailed,
}

/// The persistent registration owned by a single resource creator.
#[derive(Debug)]
pub struct Registration {
    path: PathBuf,
    record: Record,
}

/// Cleanup facts available to an invocation's caller.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ObservedResource {
    /// Unavailable identity fields remain null when the record cannot be read.
    pub owner: Option<String>,
    pub id: Option<String>,
    pub label: Option<String>,
    pub registration: PathBuf,
    pub state: ObservedResourceState,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedResourceState {
    Released,
    CleanupFailed,
    Unsettled,
}

/// Atomically records an intent before its first external resource side effect.
/// Reusing an identity fails without overwriting the earlier registration.
pub fn register(root: &Path, owner: &str, id: &str, label: &str) -> Result<Registration> {
    for (name, value) in [("owner", owner), ("id", id)] {
        ensure!(
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                && value != "."
                && value != "..",
            "resource registration {name} must be a nonempty file-name component"
        );
    }
    ensure!(!label.is_empty(), "resource registration label is empty");
    let directory = root.join(DIRECTORY);
    fs::create_dir_all(&directory).with_context(|| {
        format!(
            "create resource registration directory {}",
            directory.display()
        )
    })?;
    let path = directory.join(format!("{owner}-{id}.json"));
    let record = Record {
        schema: 1,
        owner: owner.to_owned(),
        id: id.to_owned(),
        label: label.to_owned(),
        pid: std::process::id(),
        state: State::Registered,
        error: None,
        registered_at: now_millis()?,
        settled_at: None,
    };
    write_record(&path, &record, true)?;
    Ok(Registration { path, record })
}

impl Registration {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Records one real cleanup attempt. A failed write leaves the prior fact
    /// intact and returns an error; callers must not claim successful reporting.
    pub fn settle(&mut self, result: &Result<()>) -> Result<()> {
        let mut next = self.record.clone();
        next.state = if result.is_ok() {
            State::Released
        } else {
            State::CleanupFailed
        };
        next.error = result.as_ref().err().map(|error| format!("{error:#}"));
        next.settled_at = Some(now_millis()?);
        write_record(&self.path, &next, false)?;
        self.record = next;
        Ok(())
    }
}

fn now_millis() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("resource registration clock precedes the Unix epoch")?
        .as_millis()
        .try_into()
        .context("resource registration timestamp exceeds u64 milliseconds")
}

fn write_record(path: &Path, record: &Record, create: bool) -> Result<()> {
    let directory = path
        .parent()
        .context("resource registration has no parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)
        .context("create resource registration temporary file")?;
    serde_json::to_writer(&mut temporary, record).context("encode resource registration")?;
    temporary
        .write_all(b"\n")
        .context("finish resource registration")?;
    temporary
        .as_file()
        .sync_all()
        .context("sync resource registration")?;
    if create {
        temporary.persist_noclobber(path)
    } else {
        temporary.persist(path)
    }
    .map_err(|error| error.error)
    .with_context(|| format!("publish resource registration {}", path.display()))?;
    File::open(directory)?
        .sync_all()
        .context("sync resource registration directory")?;
    Ok(())
}

/// Recursively lists published registrations. Temporary files and symlinks are
/// excluded, so a snapshot cannot read half a write or escape the supplied root.
pub fn list(root: &Path) -> Result<RegistrationSnapshot> {
    let mut paths = BTreeSet::new();
    list_directory(root, false, &mut paths)?;
    Ok(paths)
}

fn list_directory(
    root: &Path,
    in_registration_directory: bool,
    paths: &mut RegistrationSnapshot,
) -> Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("list resource registrations in {}", root.display()));
        }
    };
    for entry in entries {
        let entry = entry.context("read resource registration directory entry")?;
        let kind = entry
            .file_type()
            .context("inspect resource registration entry")?;
        let path = entry.path();
        if kind.is_dir() {
            list_directory(&path, entry.file_name() == DIRECTORY, paths)?;
        } else if kind.is_file()
            && in_registration_directory
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
        {
            paths.insert(path);
        }
    }
    Ok(())
}

/// Collects newly created registrations for one invocation and process.
/// A new corrupt record whose pid cannot be established is retained as
/// unsettled: an unreadable cleanup fact cannot prove resource release.
pub fn collect(
    root: &Path,
    before: &RegistrationSnapshot,
    pid: u32,
) -> Result<Vec<ObservedResource>> {
    let mut observed = Vec::new();
    for path in list(root)?.difference(before) {
        let value = fs::read(path)
            .with_context(|| format!("read resource registration {}", path.display()))
            .and_then(|bytes| {
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .context("decode resource registration JSON")
            });
        if let Ok(value) = &value
            && let Some(record_pid) = value.get("pid").and_then(serde_json::Value::as_u64)
            && record_pid != u64::from(pid)
        {
            continue;
        }
        let mut resource = ObservedResource {
            owner: None,
            id: None,
            label: None,
            registration: path.clone(),
            state: ObservedResourceState::Unsettled,
            error: None,
        };
        let record = value.and_then(|value| {
            resource.owner = value
                .get("owner")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            resource.id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            resource.label = value
                .get("label")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            ensure!(
                value.get("schema").and_then(serde_json::Value::as_u64) == Some(1),
                "unsupported resource registration schema"
            );
            let record: Record =
                serde_json::from_value(value).context("decode resource registration fields")?;
            ensure!(
                record.pid == pid,
                "resource registration process identity is missing or invalid"
            );
            ensure!(
                !record.owner.is_empty() && !record.id.is_empty() && !record.label.is_empty(),
                "resource registration identity is empty"
            );
            match record.state {
                State::Registered => ensure!(
                    record.settled_at.is_none() && record.error.is_none(),
                    "registered resource contains settlement fields"
                ),
                State::Released => ensure!(
                    record.settled_at.is_some() && record.error.is_none(),
                    "released resource lacks a valid settlement"
                ),
                State::CleanupFailed => ensure!(
                    record.settled_at.is_some()
                        && record.error.as_ref().is_some_and(|error| !error.is_empty()),
                    "failed resource lacks a valid settlement"
                ),
            }
            Ok(record)
        });
        match record {
            Ok(record) => {
                resource.state = match record.state {
                    State::Registered => ObservedResourceState::Unsettled,
                    State::Released => ObservedResourceState::Released,
                    State::CleanupFailed => ObservedResourceState::CleanupFailed,
                };
                resource.error = record.error;
            }
            Err(error) => resource.error = Some(format!("{error:#}")),
        }
        observed.push(resource);
    }
    Ok(observed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_settles_and_preserves_cleanup_error() {
        let root = tempfile::tempdir().unwrap();
        let before = list(root.path()).unwrap();
        let mut registration = register(root.path(), "fixture", "one", "project-one").unwrap();
        assert_eq!(
            collect(root.path(), &before, std::process::id()).unwrap()[0].state,
            ObservedResourceState::Unsettled
        );
        registration.settle(&Ok(())).unwrap();
        assert_eq!(
            collect(root.path(), &before, std::process::id()).unwrap()[0].state,
            ObservedResourceState::Released
        );
        let failure = Err(anyhow::anyhow!("actual reclaim failure").context("remove project"));
        registration.settle(&failure).unwrap();
        let observed = collect(root.path(), &before, std::process::id()).unwrap();
        assert_eq!(observed[0].state, ObservedResourceState::CleanupFailed);
        assert_eq!(
            observed[0].error.as_deref(),
            Some("remove project: actual reclaim failure")
        );
        let record: Record =
            serde_json::from_slice(&fs::read(registration.path()).unwrap()).unwrap();
        assert!(record.settled_at.unwrap() >= record.registered_at);
    }

    #[test]
    fn dropped_registration_stays_unsettled_and_cannot_be_overwritten() {
        let root = tempfile::tempdir().unwrap();
        let before = list(root.path()).unwrap();
        drop(register(root.path(), "fixture", "one", "project-one").unwrap());
        assert!(register(root.path(), "fixture", "one", "different-project").is_err());
        let observed = collect(root.path(), &before, std::process::id()).unwrap();
        assert_eq!(observed[0].state, ObservedResourceState::Unsettled);
        assert_eq!(observed[0].label.as_deref(), Some("project-one"));
    }

    #[test]
    fn collection_filters_old_and_foreign_resources_and_recurses() {
        let root = tempfile::tempdir().unwrap();
        let _old = register(root.path(), "fixture", "old", "old-project").unwrap();
        let before = list(root.path()).unwrap();
        let mut foreign = register(root.path(), "fixture", "foreign", "other-project").unwrap();
        foreign.record.pid = std::process::id().wrapping_add(1);
        foreign.settle(&Ok(())).unwrap();
        let child = root.path().join("nested/fixture-root");
        let mut new = register(&child, "fixture", "new", "new-project").unwrap();
        new.settle(&Ok(())).unwrap();
        let observed = collect(root.path(), &before, std::process::id()).unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].id.as_deref(), Some("new"));
        assert_eq!(observed[0].state, ObservedResourceState::Released);
    }

    #[test]
    fn corrupt_and_unknown_schemas_are_unsettled() {
        let root = tempfile::tempdir().unwrap();
        let before = list(root.path()).unwrap();
        let registration = register(root.path(), "fixture", "unknown", "unknown-project").unwrap();
        let mut value = serde_json::to_value(&registration.record).unwrap();
        value["schema"] = 99.into();
        fs::write(registration.path(), serde_json::to_vec(&value).unwrap()).unwrap();
        fs::write(root.path().join(DIRECTORY).join("corrupt.json"), "{").unwrap();
        let observed = collect(root.path(), &before, std::process::id()).unwrap();
        assert_eq!(observed.len(), 2);
        assert!(observed.iter().all(
            |resource| resource.state == ObservedResourceState::Unsettled
                && resource.error.is_some()
        ));
    }

    #[test]
    fn atomic_settlement_never_exposes_partial_json() {
        let root = tempfile::tempdir().unwrap();
        let mut registration =
            register(root.path(), "fixture", "atomic", "atomic-project").unwrap();
        let path = registration.path().to_owned();
        let started = std::sync::Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let barrier = started.clone();
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..20 {
                    registration
                        .settle(&Err(anyhow::anyhow!("x".repeat(8192))))
                        .unwrap();
                    registration.settle(&Ok(())).unwrap();
                }
            });
            started.wait();
            for _ in 0..1000 {
                let record: Record = serde_json::from_slice(&fs::read(&path).unwrap())
                    .expect("atomic old or new record");
                match record.state {
                    State::Registered | State::Released => assert!(record.error.is_none()),
                    State::CleanupFailed => assert_eq!(record.error.unwrap(), "x".repeat(8192)),
                }
            }
        });
    }
}
