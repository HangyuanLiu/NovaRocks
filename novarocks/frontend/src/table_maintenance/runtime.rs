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

//! Bounded, process-local OPTIMIZE observations.
//!
//! A restart constructs a new runtime. This module deliberately has no
//! StateStore, serde, fence, or recovery representation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::maintenance::MaintenanceTarget;
use crate::query_execution::maintenance::OptimizeJobState;

use super::activity::MaintenanceActivityPermit;
use super::model::{OptimizeJob, OptimizeJobCreate, OptimizeJobOutcome};

pub const MAX_ACTIVE_OR_QUEUED_OPTIMIZE_JOBS: usize = 1024;
pub const MAX_RECENT_TERMINAL_OPTIMIZE_JOBS: usize = 4096;
pub const RECENT_TERMINAL_OPTIMIZE_JOB_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizeRuntimeErrorKind {
    AlreadyActive,
    Capacity,
    IdExhausted,
    InvalidTransition,
    NotFound,
    Poisoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeRuntimeError {
    kind: OptimizeRuntimeErrorKind,
    message: String,
}

impl OptimizeRuntimeError {
    pub const fn kind(&self) -> OptimizeRuntimeErrorKind {
        self.kind
    }

    fn new(kind: OptimizeRuntimeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for OptimizeRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for OptimizeRuntimeError {}

#[derive(Default)]
struct RuntimeState {
    active: HashMap<i64, OptimizeJob>,
    active_targets: HashMap<MaintenanceTarget, i64>,
    active_permits: HashMap<i64, MaintenanceActivityPermit>,
    cancellation_requested: HashSet<i64>,
    terminal: VecDeque<OptimizeJob>,
}

/// The only owner of current-process optimize job facts.
#[derive(Clone)]
pub struct OptimizeProcessRuntime {
    state: Arc<Mutex<RuntimeState>>,
    next_id: Arc<AtomicI64>,
    id_base: i64,
    accepting: Arc<AtomicBool>,
    changed: Arc<tokio::sync::Notify>,
}

impl fmt::Debug for OptimizeProcessRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OptimizeProcessRuntime")
            .field("id_base", &self.id_base)
            .finish_non_exhaustive()
    }
}

impl Default for OptimizeProcessRuntime {
    fn default() -> Self {
        // Keep ids positive and reserve the low 16 bits for a checked local
        // sequence. UUIDv7 supplies both current process entropy and ordering
        // without turning this identifier into durable cross-restart state.
        let entropy = Uuid::now_v7().as_u128() as u64;
        let id_base = ((entropy & 0x3fff_ffff_ffff_0000) as i64).max(1);
        Self {
            state: Arc::new(Mutex::new(RuntimeState::default())),
            next_id: Arc::new(AtomicI64::new(1)),
            id_base,
            accepting: Arc::new(AtomicBool::new(true)),
            changed: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl OptimizeProcessRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn submit(
        &self,
        request: OptimizeJobCreate,
        permit: MaintenanceActivityPermit,
    ) -> Result<OptimizeJob, OptimizeRuntimeError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::InvalidTransition,
                "the frontend optimize runtime is shutting down",
            ));
        }
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, request.created_at_ms);
        if state.active_targets.contains_key(&request.target) {
            return Err(OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::AlreadyActive,
                format!(
                    "an optimize job is already active for {}.{}.{}",
                    request.target.catalog, request.target.namespace, request.target.table
                ),
            ));
        }
        if state.active.len() >= MAX_ACTIVE_OR_QUEUED_OPTIMIZE_JOBS {
            return Err(OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::Capacity,
                format!(
                    "frontend optimize runtime has reached its {MAX_ACTIVE_OR_QUEUED_OPTIMIZE_JOBS} active or queued job limit"
                ),
            ));
        }
        let job_id = self.allocate_id()?;
        let job = OptimizeJob {
            job_id,
            target: request.target,
            object_id: request.object_id.as_bytes().to_vec(),
            base_snapshot_id: request.base_snapshot_id,
            state: OptimizeJobState::Pending,
            outcome: None,
            error_message: None,
            created_at_ms: request.created_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
        };
        state.active_targets.insert(job.target.clone(), job_id);
        state.active_permits.insert(job_id, permit);
        state.active.insert(job_id, job.clone());
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
    }

    pub async fn get(&self, job_id: i64) -> Result<Option<OptimizeJob>, OptimizeRuntimeError> {
        let state = self.lock()?;
        Ok(state.active.get(&job_id).cloned().or_else(|| {
            state
                .terminal
                .iter()
                .find(|job| job.job_id == job_id)
                .cloned()
        }))
    }

    pub async fn list(&self) -> Result<Vec<OptimizeJob>, OptimizeRuntimeError> {
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, now_ms());
        let mut jobs: Vec<_> = state.active.values().cloned().collect();
        jobs.extend(state.terminal.iter().cloned());
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }

    pub async fn claim_next(
        &self,
        at_ms: i64,
    ) -> Result<Option<OptimizeJob>, OptimizeRuntimeError> {
        let mut state = self.lock()?;
        let Some(job_id) = state
            .active
            .values()
            .filter(|job| job.state == OptimizeJobState::Pending)
            .min_by_key(|job| job.job_id)
            .map(|job| job.job_id)
        else {
            return Ok(None);
        };
        let job = state
            .active
            .get_mut(&job_id)
            .expect("selected optimize job exists");
        job.state = OptimizeJobState::Running;
        job.started_at_ms = Some(at_ms);
        let result = job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(Some(result))
    }

    pub async fn finish(
        &self,
        job_id: i64,
        outcome: Result<OptimizeJobOutcome, OptimizeTerminalError>,
        at_ms: i64,
    ) -> Result<OptimizeJob, OptimizeRuntimeError> {
        let mut state = self.lock()?;
        let job = state.active.remove(&job_id).ok_or_else(|| {
            OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::NotFound,
                format!("optimize job {job_id} is not in this frontend process"),
            )
        })?;
        if job.state != OptimizeJobState::Running {
            state.active.insert(job_id, job.clone());
            return Err(OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::InvalidTransition,
                format!("optimize job {job_id} is not running"),
            ));
        }
        let mut terminal = job;
        match outcome {
            Ok(outcome) => {
                terminal.state = OptimizeJobState::Finished;
                terminal.outcome = Some(outcome);
            }
            Err(error) if error.target_replaced => {
                terminal.state = OptimizeJobState::TargetReplaced;
                terminal.error_message = Some(error.message);
            }
            Err(error) => {
                terminal.state = OptimizeJobState::Failed;
                terminal.error_message = Some(error.message);
            }
        }
        terminal.finished_at_ms = Some(at_ms);
        state.active_targets.remove(&terminal.target);
        state.active_permits.remove(&job_id);
        state.cancellation_requested.remove(&job_id);
        state.terminal.push_back(terminal.clone());
        Self::prune_locked(&mut state, at_ms);
        drop(state);
        self.changed.notify_waiters();
        Ok(terminal)
    }

    pub async fn cancel_pending_or_request_running(
        &self,
        job_id: i64,
        at_ms: i64,
    ) -> Result<OptimizeJob, OptimizeRuntimeError> {
        let mut state = self.lock()?;
        let job = state.active.remove(&job_id).ok_or_else(|| {
            OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::NotFound,
                format!("optimize job {job_id} is not in this frontend process"),
            )
        })?;
        if job.state == OptimizeJobState::Pending {
            let mut terminal = job;
            terminal.state = OptimizeJobState::Failed;
            terminal.error_message = Some("optimize job cancelled before dispatch".into());
            terminal.finished_at_ms = Some(at_ms);
            state.active_targets.remove(&terminal.target);
            state.active_permits.remove(&job_id);
            state.cancellation_requested.remove(&job_id);
            state.terminal.push_back(terminal.clone());
            Self::prune_locked(&mut state, at_ms);
            drop(state);
            self.changed.notify_waiters();
            return Ok(terminal);
        }
        state.cancellation_requested.insert(job_id);
        let result = job.clone();
        state.active.insert(job_id, job);
        Ok(result)
    }

    pub fn stop_admission(&self) {
        self.accepting.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub async fn cancellation_requested(&self, job_id: i64) -> Result<bool, OptimizeRuntimeError> {
        let state = self.lock()?;
        Ok(state.cancellation_requested.contains(&job_id))
    }

    pub async fn request_shutdown_cancellation(&self) -> Result<(), OptimizeRuntimeError> {
        let mut state = self.lock()?;
        let active: Vec<_> = state.active.keys().copied().collect();
        state.cancellation_requested.extend(active);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn wait_for_change(&self) {
        self.changed.notified().await;
    }

    fn allocate_id(&self) -> Result<i64, OptimizeRuntimeError> {
        let offset = self.next_id.fetch_add(1, Ordering::Relaxed);
        if !(1..=0xffff).contains(&offset) {
            return Err(OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::IdExhausted,
                "frontend optimize runtime exhausted its checked process-local job-id range",
            ));
        }
        self.id_base.checked_add(offset).ok_or_else(|| {
            OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::IdExhausted,
                "frontend optimize runtime job-id overflow",
            )
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RuntimeState>, OptimizeRuntimeError> {
        self.state.lock().map_err(|_| {
            OptimizeRuntimeError::new(
                OptimizeRuntimeErrorKind::Poisoned,
                "frontend optimize runtime lock is poisoned",
            )
        })
    }

    fn prune_locked(state: &mut RuntimeState, at_ms: i64) {
        while state.terminal.front().is_some_and(|job| {
            job.finished_at_ms.is_some_and(|finished| {
                at_ms.saturating_sub(finished) >= RECENT_TERMINAL_OPTIMIZE_JOB_RETENTION_MS
            })
        }) {
            state.terminal.pop_front();
        }
        while state.terminal.len() > MAX_RECENT_TERMINAL_OPTIMIZE_JOBS {
            state.terminal.pop_front();
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeTerminalError {
    pub message: String,
    pub target_replaced: bool,
}

impl OptimizeTerminalError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            target_replaced: false,
        }
    }

    pub fn target_replaced(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            target_replaced: true,
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::ConnectorTableObjectId;

    use super::{OptimizeProcessRuntime, OptimizeRuntimeErrorKind};
    use crate::maintenance::MaintenanceTarget;
    use crate::table_maintenance::activity::{MaintenanceActivityFamily, TableMaintenanceActivity};
    use crate::table_maintenance::model::OptimizeJobCreate;

    fn request(table: &str, at_ms: i64) -> OptimizeJobCreate {
        OptimizeJobCreate {
            target: MaintenanceTarget {
                catalog: "ice".into(),
                namespace: "db".into(),
                table: table.into(),
            },
            object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"runtime-test-object"))
                .unwrap(),
            base_snapshot_id: 7,
            created_at_ms: at_ms,
        }
    }

    #[tokio::test]
    async fn restart_is_empty_and_a_target_cannot_be_queued_twice() {
        let activity = TableMaintenanceActivity::default();
        let runtime = OptimizeProcessRuntime::new();
        let target = request("orders", 1).target;
        let permit = activity
            .acquire(&target, MaintenanceActivityFamily::Optimize)
            .unwrap();
        let first = runtime.submit(request("orders", 1), permit).await.unwrap();
        assert!(first.job_id > 0);
        let second_permit = activity.acquire(&target, MaintenanceActivityFamily::Optimize);
        assert!(second_permit.is_err());
        assert!(
            OptimizeProcessRuntime::new()
                .list()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn active_capacity_never_evicts_and_terminal_history_is_bounded() {
        let runtime = OptimizeProcessRuntime::new();
        let activity = TableMaintenanceActivity::default();
        let at_ms = super::now_ms();
        let target = request("orders", at_ms).target;
        let permit = activity
            .acquire(&target, MaintenanceActivityFamily::Optimize)
            .unwrap();
        let job = runtime
            .submit(request("orders", at_ms), permit)
            .await
            .unwrap();
        assert_eq!(
            runtime.claim_next(at_ms + 1).await.unwrap().unwrap().job_id,
            job.job_id
        );
        runtime
            .finish(
                job.job_id,
                Err(super::OptimizeTerminalError::failed("test")),
                at_ms + 2,
            )
            .await
            .unwrap();
        assert_eq!(runtime.list().await.unwrap().len(), 1);
        assert_eq!(
            runtime.get(job.job_id).await.unwrap().unwrap().job_id,
            job.job_id
        );
        assert_eq!(runtime.get(job.job_id + 1).await.unwrap(), None);
        let _ = OptimizeRuntimeErrorKind::Capacity;
    }
}
