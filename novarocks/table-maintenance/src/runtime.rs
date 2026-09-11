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

//! Bounded current-process maintenance job ownership.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

pub const MAX_ACTIVE_OR_QUEUED_JOBS: usize = 1024;
pub const MAX_RECENT_TERMINAL_JOBS: usize = 4096;
pub const RECENT_TERMINAL_JOB_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MaintenanceJobState {
    Pending,
    Running,
    Finished,
    KnownUncommitted,
    CommitUnknown,
    KnownCommittedFinalizationFailed,
    Failed,
    TargetReplaced,
    CancelledBeforeDispatch,
}

impl MaintenanceJobState {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
            Self::KnownUncommitted => "KNOWN_UNCOMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::KnownCommittedFinalizationFailed => "KNOWN_COMMITTED_FINALIZATION_FAILED",
            Self::Failed => "FAILED",
            Self::TargetReplaced => "TARGET_REPLACED",
            Self::CancelledBeforeDispatch => "CANCELLED_BEFORE_DISPATCH",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobCreate<T> {
    pub target: T,
    pub object_id: Vec<u8>,
    pub base_snapshot_id: i64,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecord<T, O> {
    pub job_id: i64,
    pub target: T,
    pub object_id: Vec<u8>,
    pub base_snapshot_id: i64,
    pub state: MaintenanceJobState,
    pub outcome: Option<O>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

/// Names one exact current-process maintenance job.
///
/// A handle never means "the job currently active for this target": callers
/// may only wait on the immutable identifier returned at submission time.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JobHandle {
    job_id: i64,
}

impl JobHandle {
    pub const fn new(job_id: i64) -> Self {
        Self { job_id }
    }

    pub const fn job_id(self) -> i64 {
        self.job_id
    }
}

impl<T, O> JobRecord<T, O> {
    pub const fn handle(&self) -> JobHandle {
        JobHandle::new(self.job_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeErrorKind {
    AlreadyActive,
    Capacity,
    IdExhausted,
    InvalidTransition,
    NotFound,
    Poisoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeError {
    kind: RuntimeErrorKind,
    message: String,
}

impl RuntimeError {
    pub const fn kind(&self) -> RuntimeErrorKind {
        self.kind
    }
    pub fn new(kind: RuntimeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for RuntimeError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalError {
    pub state: MaintenanceJobState,
    pub message: String,
}

impl TerminalError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::Failed,
            message: message.into(),
        }
    }
    pub fn target_replaced(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::TargetReplaced,
            message: message.into(),
        }
    }
    pub fn cancelled_before_dispatch(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::CancelledBeforeDispatch,
            message: message.into(),
        }
    }
    pub fn known_uncommitted(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::KnownUncommitted,
            message: message.into(),
        }
    }
    pub fn commit_unknown(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::CommitUnknown,
            message: message.into(),
        }
    }
    pub fn known_committed_finalization_failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::KnownCommittedFinalizationFailed,
            message: message.into(),
        }
    }
}

struct RuntimeState<T, O, P> {
    active: HashMap<i64, JobRecord<T, O>>,
    active_targets: HashMap<T, i64>,
    permits: HashMap<i64, P>,
    cancellation_requested: HashSet<i64>,
    terminal: VecDeque<JobRecord<T, O>>,
}

impl<T, O, P> Default for RuntimeState<T, O, P> {
    fn default() -> Self {
        Self {
            active: HashMap::new(),
            active_targets: HashMap::new(),
            permits: HashMap::new(),
            cancellation_requested: HashSet::new(),
            terminal: VecDeque::new(),
        }
    }
}

#[derive(Clone)]
pub struct ProcessRuntime<T, O, P> {
    state: Arc<Mutex<RuntimeState<T, O, P>>>,
    next_id: Arc<AtomicI64>,
    id_base: i64,
    accepting: Arc<AtomicBool>,
    changed: Arc<tokio::sync::Notify>,
}

impl<T, O, P> Default for ProcessRuntime<T, O, P> {
    fn default() -> Self {
        let entropy = Uuid::now_v7().as_u128() as u64;
        Self {
            state: Arc::new(Mutex::new(RuntimeState::default())),
            next_id: Arc::new(AtomicI64::new(1)),
            id_base: ((entropy & 0x3fff_ffff_ffff_0000) as i64).max(1),
            accepting: Arc::new(AtomicBool::new(true)),
            changed: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl<T, O, P> ProcessRuntime<T, O, P>
where
    T: Clone + Eq + Hash,
    O: Clone,
{
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn submit(
        &self,
        request: JobCreate<T>,
        permit: P,
    ) -> Result<JobRecord<T, O>, RuntimeError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "the table-maintenance runtime is shutting down",
            ));
        }
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, now_ms());
        if state.active_targets.contains_key(&request.target) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::AlreadyActive,
                "a maintenance job is already active for this target",
            ));
        }
        if state.active.len() >= MAX_ACTIVE_OR_QUEUED_JOBS {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Capacity,
                "table-maintenance runtime capacity is exhausted",
            ));
        }
        let job_id = self.allocate_id()?;
        let job = JobRecord {
            job_id,
            target: request.target,
            object_id: request.object_id,
            base_snapshot_id: request.base_snapshot_id,
            state: MaintenanceJobState::Pending,
            outcome: None,
            error_message: None,
            created_at_ms: request.created_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
        };
        state.active_targets.insert(job.target.clone(), job_id);
        state.permits.insert(job_id, permit);
        state.active.insert(job_id, job.clone());
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
    }
    pub async fn get(&self, job_id: i64) -> Result<Option<JobRecord<T, O>>, RuntimeError> {
        let state = self.lock()?;
        Ok(state.active.get(&job_id).cloned().or_else(|| {
            state
                .terminal
                .iter()
                .find(|job| job.job_id == job_id)
                .cloned()
        }))
    }
    pub async fn list(&self) -> Result<Vec<JobRecord<T, O>>, RuntimeError> {
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, now_ms());
        let mut jobs: Vec<_> = state.active.values().cloned().collect();
        jobs.extend(state.terminal.iter().cloned());
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }
    pub async fn claim_next(&self, at_ms: i64) -> Result<Option<JobRecord<T, O>>, RuntimeError> {
        let mut state = self.lock()?;
        let Some(id) = state
            .active
            .values()
            .filter(|job| job.state == MaintenanceJobState::Pending)
            .min_by_key(|job| job.job_id)
            .map(|job| job.job_id)
        else {
            return Ok(None);
        };
        let job = state.active.get_mut(&id).expect("selected job exists");
        job.state = MaintenanceJobState::Running;
        job.started_at_ms = Some(at_ms);
        let result = job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(Some(result))
    }
    pub async fn finish(
        &self,
        job_id: i64,
        outcome: Result<O, TerminalError>,
        at_ms: i64,
    ) -> Result<JobRecord<T, O>, RuntimeError> {
        let mut state = self.lock()?;
        let job = state
            .active
            .remove(&job_id)
            .ok_or_else(|| Self::not_found(job_id))?;
        if job.state != MaintenanceJobState::Running {
            state.active.insert(job_id, job);
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "maintenance job is not running",
            ));
        }
        let mut terminal = job;
        match outcome {
            Ok(outcome) => {
                terminal.state = MaintenanceJobState::Finished;
                terminal.outcome = Some(outcome);
            }
            Err(error) => {
                terminal.state = error.state;
                terminal.error_message = Some(error.message);
            }
        }
        terminal.finished_at_ms = Some(at_ms);
        state.active_targets.remove(&terminal.target);
        state.permits.remove(&job_id);
        state.cancellation_requested.remove(&job_id);
        state.terminal.push_back(terminal.clone());
        Self::prune_locked(&mut state, at_ms);
        drop(state);
        self.changed.notify_waiters();
        Ok(terminal)
    }
    pub async fn wait_for_completion(&self, job_id: i64) -> Result<JobRecord<T, O>, RuntimeError> {
        loop {
            let notified = self.changed.notified();
            let job = self
                .get(job_id)
                .await?
                .ok_or_else(|| Self::not_found(job_id))?;
            if job.state.is_terminal() {
                return Ok(job);
            }
            notified.await;
        }
    }
    pub async fn cancellation_requested(&self, job_id: i64) -> Result<bool, RuntimeError> {
        Ok(self.lock()?.cancellation_requested.contains(&job_id))
    }
    pub async fn request_shutdown_cancellation(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let at_ms = now_ms();
        let pending: Vec<_> = state
            .active
            .iter()
            .filter_map(|(&job_id, job)| {
                (job.state == MaintenanceJobState::Pending).then_some(job_id)
            })
            .collect();
        for job_id in pending {
            let mut terminal = state
                .active
                .remove(&job_id)
                .expect("pending maintenance job exists");
            terminal.state = MaintenanceJobState::CancelledBeforeDispatch;
            terminal.error_message =
                Some("maintenance job cancelled before provider dispatch".into());
            terminal.finished_at_ms = Some(at_ms);
            state.active_targets.remove(&terminal.target);
            state.permits.remove(&job_id);
            state.cancellation_requested.remove(&job_id);
            state.terminal.push_back(terminal);
        }
        let running: Vec<_> = state.active.keys().copied().collect();
        state.cancellation_requested.extend(running);
        Self::prune_locked(&mut state, at_ms);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    pub async fn wait_for_change(&self) {
        self.changed.notified().await;
    }
    pub fn stop_admission(&self) {
        self.accepting.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RuntimeState<T, O, P>>, RuntimeError> {
        self.state.lock().map_err(|_| {
            RuntimeError::new(
                RuntimeErrorKind::Poisoned,
                "table-maintenance runtime lock is poisoned",
            )
        })
    }
    fn not_found(job_id: i64) -> RuntimeError {
        RuntimeError::new(
            RuntimeErrorKind::NotFound,
            format!("maintenance job {job_id} is not in this process"),
        )
    }
    fn allocate_id(&self) -> Result<i64, RuntimeError> {
        let offset = self.next_id.fetch_add(1, Ordering::Relaxed);
        if !(1..=0xffff).contains(&offset) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::IdExhausted,
                "table-maintenance job-id range exhausted",
            ));
        }
        self.id_base.checked_add(offset).ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorKind::IdExhausted,
                "table-maintenance job-id overflow",
            )
        })
    }
    fn prune_locked(state: &mut RuntimeState<T, O, P>, at_ms: i64) {
        while state.terminal.len() > MAX_RECENT_TERMINAL_JOBS
            || state.terminal.front().is_some_and(|job| {
                job.finished_at_ms.is_some_and(|finished| {
                    at_ms.saturating_sub(finished) > RECENT_TERMINAL_JOB_RETENTION_MS
                })
            })
        {
            state.terminal.pop_front();
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
    use super::{JobCreate, MaintenanceJobState, ProcessRuntime, RuntimeErrorKind, TerminalError};

    fn create(target: &str, at_ms: i64) -> JobCreate<String> {
        JobCreate {
            target: target.to_owned(),
            object_id: b"object".to_vec(),
            base_snapshot_id: 7,
            created_at_ms: at_ms,
        }
    }

    #[tokio::test]
    async fn target_is_exclusive_and_completed_before_wait_is_observable() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime
            .submit(create("orders", 1), ())
            .await
            .expect("submit");
        assert_eq!(
            runtime
                .submit(create("orders", 2), ())
                .await
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::AlreadyActive
        );
        runtime.claim_next(2).await.expect("claim").expect("job");
        runtime
            .finish(job.job_id, Err(TerminalError::failed("failed")), 3)
            .await
            .expect("finish");
        let completed = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            runtime.wait_for_completion(job.job_id),
        )
        .await
        .expect("wait must not miss completed-before-subscribe")
        .expect("job");
        assert_eq!(completed.state, MaintenanceJobState::Failed);
        runtime
            .submit(create("orders", 4), ())
            .await
            .expect("released target");
    }

    #[tokio::test]
    async fn terminal_classification_is_retained() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime
            .submit(create("orders", 1), ())
            .await
            .expect("submit");
        runtime.claim_next(2).await.expect("claim");
        runtime
            .finish(
                job.job_id,
                Err(TerminalError::cancelled_before_dispatch("cancelled")),
                3,
            )
            .await
            .expect("finish");
        assert_eq!(
            runtime.get(job.job_id).await.expect("get").unwrap().state,
            MaintenanceJobState::CancelledBeforeDispatch
        );
    }

    #[tokio::test]
    async fn shutdown_cancellation_completes_a_pending_job_before_dispatch() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime
            .submit(create("orders", 1), ())
            .await
            .expect("submit");
        runtime
            .request_shutdown_cancellation()
            .await
            .expect("shutdown cancellation");
        let terminal = runtime
            .wait_for_completion(job.job_id)
            .await
            .expect("terminal job");
        assert_eq!(terminal.state, MaintenanceJobState::CancelledBeforeDispatch);
        assert!(
            !runtime
                .cancellation_requested(job.job_id)
                .await
                .expect("cancellation state")
        );
    }
}
