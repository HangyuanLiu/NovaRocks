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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Bounded in-memory repository for the current frontend process.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use super::model::{StatisticsJob, StatisticsJobCreate, StatisticsJobError, StatisticsJobState};

pub const MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS: usize = 1024;
pub const MAX_RECENT_TERMINAL_STATISTICS_JOBS: usize = 4096;
pub const RECENT_TERMINAL_STATISTICS_JOB_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsJobRepositoryErrorKind {
    NotFound,
    Conflict,
    Capacity,
    InvalidTransition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobRepositoryError {
    kind: StatisticsJobRepositoryErrorKind,
    message: String,
}

impl StatisticsJobRepositoryError {
    pub const fn kind(&self) -> StatisticsJobRepositoryErrorKind {
        self.kind
    }

    fn new(kind: StatisticsJobRepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for StatisticsJobRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsJobRepositoryError {}

#[derive(Default)]
struct RuntimeState {
    active: HashMap<Uuid, StatisticsJob>,
    terminal: VecDeque<StatisticsJob>,
}

#[derive(Clone, Default)]
pub struct StatisticsJobRepository {
    state: Arc<Mutex<RuntimeState>>,
    changed: Arc<tokio::sync::Notify>,
}

impl fmt::Debug for StatisticsJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatisticsJobRepository")
            .finish_non_exhaustive()
    }
}

impl StatisticsJobRepository {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create(
        &self,
        request: StatisticsJobCreate,
    ) -> Result<StatisticsJob, StatisticsJobRepositoryError> {
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, request.submitted_at_ms);
        if state.active.len() >= MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS {
            return Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::Capacity,
                format!(
                    "statistics runtime has reached its {MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS} active or queued job limit"
                ),
            ));
        }
        let job = StatisticsJob::new(
            Uuid::now_v7(),
            novarocks_spi::connector::LakePublicationId::new_v7(),
            request,
        );
        state.active.insert(job.job_id, job.clone());
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
    }

    pub async fn get(
        &self,
        job_id: Uuid,
    ) -> Result<Option<StatisticsJob>, StatisticsJobRepositoryError> {
        let state = self.lock()?;
        Ok(state.active.get(&job_id).cloned().or_else(|| {
            state
                .terminal
                .iter()
                .find(|job| job.job_id == job_id)
                .cloned()
        }))
    }

    pub async fn list(&self) -> Result<Vec<StatisticsJob>, StatisticsJobRepositoryError> {
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, now_ms());
        let mut jobs: Vec<_> = state.active.values().cloned().collect();
        jobs.extend(state.terminal.iter().cloned());
        jobs.sort_by_key(|job| (job.submitted_at_ms, job.job_id));
        Ok(jobs)
    }

    pub async fn request_cancel(
        &self,
        job_id: Uuid,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsJobRepositoryError> {
        let mut state = self.lock()?;
        let Some(job) = state.active.get_mut(&job_id) else {
            return Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::NotFound,
                format!("statistics job {job_id} is not in this frontend process"),
            ));
        };
        job.cancel_requested = true;
        job.updated_at_ms = at_ms;
        if job.state == StatisticsJobState::Submitted {
            let mut terminal = state.active.remove(&job_id).expect("submitted job exists");
            terminal.state = StatisticsJobState::Cancelled;
            terminal.completed_at_ms = Some(at_ms);
            terminal.error = Some(StatisticsJobError {
                kind: super::model::StatisticsJobErrorKind::Cancelled,
                message: "statistics job cancelled before dispatch".into(),
            });
            state.terminal.push_back(terminal.clone());
            Self::prune_locked(&mut state, at_ms);
            drop(state);
            self.changed.notify_waiters();
            return Ok(terminal);
        }
        let result = job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(result)
    }

    pub async fn claim_next(
        &self,
        at_ms: i64,
    ) -> Result<Option<StatisticsJob>, StatisticsJobRepositoryError> {
        let mut state = self.lock()?;
        let job_id = state
            .active
            .values()
            .filter(|job| job.state == StatisticsJobState::Submitted && !job.cancel_requested)
            .min_by_key(|job| (job.submitted_at_ms, job.job_id))
            .map(|job| job.job_id);
        let Some(job_id) = job_id else {
            return Ok(None);
        };
        let job = state.active.get_mut(&job_id).expect("selected job exists");
        job.state = StatisticsJobState::Preparing;
        job.updated_at_ms = at_ms;
        let result = job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(Some(result))
    }

    pub async fn transition(
        &self,
        job_id: Uuid,
        expected: StatisticsJobState,
        next: StatisticsJobState,
        at_ms: i64,
        error: Option<StatisticsJobError>,
    ) -> Result<StatisticsJob, StatisticsJobRepositoryError> {
        let mut state = self.lock()?;
        let job = state.active.get(&job_id).cloned().ok_or_else(|| {
            StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::NotFound,
                format!("statistics job {job_id} is not in this frontend process"),
            )
        })?;
        if job.state != expected {
            return Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::Conflict,
                format!(
                    "statistics job {job_id} is {:?}, not expected {:?}",
                    job.state, expected
                ),
            ));
        }
        if !expected.can_transition_to(next) {
            return Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::InvalidTransition,
                format!(
                    "statistics job cannot transition from {:?} to {:?}",
                    expected, next
                ),
            ));
        }
        let mut updated = job;
        updated.state = next;
        updated.updated_at_ms = at_ms;
        updated.error = error;
        if next.is_terminal() {
            updated.completed_at_ms = Some(at_ms);
            state.active.remove(&job_id);
            state.terminal.push_back(updated.clone());
            Self::prune_locked(&mut state, at_ms);
        } else {
            state.active.insert(job_id, updated.clone());
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(updated)
    }

    pub async fn cancellation_requested(
        &self,
        job_id: Uuid,
    ) -> Result<bool, StatisticsJobRepositoryError> {
        let state = self.lock()?;
        Ok(state
            .active
            .get(&job_id)
            .is_some_and(|job| job.cancel_requested))
    }

    pub async fn cancel_active_for_shutdown(
        &self,
        at_ms: i64,
    ) -> Result<(), StatisticsJobRepositoryError> {
        let mut state = self.lock()?;
        for job in state.active.values_mut() {
            job.cancel_requested = true;
            job.updated_at_ms = at_ms;
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn wait_for_change(&self) {
        self.changed.notified().await;
    }

    pub(crate) fn notify_worker(&self) {
        self.changed.notify_waiters();
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, RuntimeState>, StatisticsJobRepositoryError> {
        self.state.lock().map_err(|_| {
            StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::Conflict,
                "statistics runtime lock poisoned",
            )
        })
    }

    fn prune_locked(state: &mut RuntimeState, at_ms: i64) {
        while state.terminal.front().is_some_and(|job| {
            job.completed_at_ms.is_some_and(|completed| {
                at_ms.saturating_sub(completed) >= RECENT_TERMINAL_STATISTICS_JOB_RETENTION_MS
            })
        }) {
            state.terminal.pop_front();
        }
        while state.terminal.len() > MAX_RECENT_TERMINAL_STATISTICS_JOBS {
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
