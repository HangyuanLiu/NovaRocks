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

//! Process-owned, one-shot ANALYZE worker.
//!
//! There is no lease, takeover, retry queue, startup scan, or reconciliation
//! pass. Every submission is one fresh attempt owned by this frontend process.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use novarocks_spi::connector::ExternalMutationEvidence;
use uuid::Uuid;

use super::application::StatisticsPublicationTerminal;
use super::model::{StatisticsJob, StatisticsJobError, StatisticsJobErrorKind, StatisticsJobState};
use super::repository::StatisticsJobRepository;
use crate::workload_lifecycle::{FrontendServingLifecycle, FrontendWorkloadKind};

pub const STATISTICS_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// A failed attempt is terminal. In particular, no error is retryable and a
/// `CommitUnknown` must not invoke any mutation after the failed publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptError {
    pub kind: StatisticsJobErrorKind,
    pub message: String,
    pub publication: Option<StatisticsPublicationTerminal>,
}

impl StatisticsAttemptError {
    pub fn permanent(kind: StatisticsJobErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            publication: None,
        }
    }

    pub fn publication(
        terminal: StatisticsPublicationTerminal,
        message: impl Into<String>,
    ) -> Self {
        let kind = match terminal {
            StatisticsPublicationTerminal::KnownUncommitted => StatisticsJobErrorKind::Publish,
            StatisticsPublicationTerminal::KnownCommittedFinalization => {
                StatisticsJobErrorKind::KnownCommittedFinalization
            }
            StatisticsPublicationTerminal::CommitUnknown => StatisticsJobErrorKind::CommitUnknown,
        };
        Self {
            kind,
            message: message.into(),
            publication: Some(terminal),
        }
    }
}

/// Attempt-local collection material. It never crosses a process boundary.
pub trait StatisticsCollectedAttempt: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn basis_data_version(&self) -> &[u8];
}

impl StatisticsCollectedAttempt for () {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn basis_data_version(&self) -> &[u8] {
        b"unit-test-basis"
    }
}

/// Connector-neutral execution owned by the frontend process worker.
pub trait StatisticsAttemptExecutor: Send + Sync {
    fn collect(
        &self,
        job: &StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError>;

    /// Side-effect-free preparation for the one publication attempt.
    fn prepare_publish(
        &self,
        job: &StatisticsJob,
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError>;

    fn publish(
        &self,
        job: &StatisticsJob,
        collected: &dyn StatisticsCollectedAttempt,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError>;
}

/// Lifecycle owner for the current process worker task.
pub struct StatisticsAnalyzeWorker {
    repository: StatisticsJobRepository,
    stop: Arc<AtomicBool>,
    join: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl StatisticsAnalyzeWorker {
    pub async fn start(
        runtime: &tokio::runtime::Handle,
        repository: StatisticsJobRepository,
        executor: Arc<dyn StatisticsAttemptExecutor>,
        workload_lifecycle: FrontendServingLifecycle,
    ) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let join = runtime.spawn(run_worker(
            repository.clone(),
            Arc::downgrade(&executor),
            Arc::clone(&stop),
            workload_lifecycle,
        ));
        Ok(Self {
            repository,
            stop,
            join: Some(join),
        })
    }

    pub fn wakeup(&self) {
        // Repository mutations wake the worker. Keep this method for the
        // application port's explicit post-submit signal.
        self.repository.notify_worker();
    }

    pub fn shutdown(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        let now = now_ms();
        let repository = self.repository.clone();
        let cancel = async move { repository.cancel_active_for_shutdown(now).await };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            if runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                return Err("statistics worker cannot synchronously join from a current-thread Tokio runtime".into());
            }
            tokio::task::block_in_place(|| runtime.block_on(cancel))
                .map_err(|error| error.to_string())?;
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("build statistics worker join runtime failed: {error}"))?
                .block_on(cancel)
                .map_err(|error| error.to_string())?;
        }
        self.wakeup();
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let joined = if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            if runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                return Err("statistics worker cannot synchronously join from a current-thread Tokio runtime".into());
            }
            tokio::task::block_in_place(|| runtime.block_on(join))
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("build statistics worker join runtime failed: {error}"))?
                .block_on(join)
        };
        joined.map_err(|error| format!("statistics worker join failed: {error}"))?
    }
}

async fn run_worker(
    repository: StatisticsJobRepository,
    executor: Weak<dyn StatisticsAttemptExecutor>,
    stop: Arc<AtomicBool>,
    workload_lifecycle: FrontendServingLifecycle,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let workload_lease = match workload_lifecycle.try_admit(FrontendWorkloadKind::Background) {
            Ok(lease) => lease,
            Err(_) => return Ok(()),
        };
        let Some(job) = repository
            .claim_next(now_ms())
            .await
            .map_err(|error| error.to_string())?
        else {
            drop(workload_lease);
            tokio::select! {
                _ = repository.wait_for_change() => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            continue;
        };
        let Some(executor) = executor.upgrade() else {
            return Ok(());
        };
        run_attempt(
            &repository,
            executor.as_ref(),
            job,
            STATISTICS_ATTEMPT_TIMEOUT,
            workload_lease.cancellation_source().view(),
        )
        .await?;
    }
}

async fn run_attempt(
    repository: &StatisticsJobRepository,
    executor: &dyn StatisticsAttemptExecutor,
    job: StatisticsJob,
    timeout: Duration,
    cancellation: crate::common::query_cancellation::QueryCancellationView,
) -> Result<(), String> {
    let started = Instant::now();
    if must_stop(repository, job.job_id, started, timeout, &cancellation).await? {
        return cancel(
            repository,
            job.job_id,
            StatisticsJobState::Preparing,
            "statistics job cancelled before collection",
        )
        .await;
    }
    let running = repository
        .transition(
            job.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            now_ms(),
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
    let collected = match executor.collect(&running) {
        Ok(collected) => collected,
        Err(error) => {
            return finish_error(
                repository,
                running.job_id,
                StatisticsJobState::Running,
                error,
            )
            .await;
        }
    };
    if must_stop(repository, running.job_id, started, timeout, &cancellation).await? {
        return cancel(
            repository,
            running.job_id,
            StatisticsJobState::Running,
            "statistics job cancelled before publication preparation",
        )
        .await;
    }
    let evidence = match executor.prepare_publish(&running, collected.as_ref()) {
        Ok(evidence) => evidence,
        Err(error) => {
            return finish_error(
                repository,
                running.job_id,
                StatisticsJobState::Running,
                error,
            )
            .await;
        }
    };
    if must_stop(repository, running.job_id, started, timeout, &cancellation).await? {
        return cancel(
            repository,
            running.job_id,
            StatisticsJobState::Running,
            "statistics job cancelled before publication dispatch",
        )
        .await;
    }
    let publishing = repository
        .transition(
            running.job_id,
            StatisticsJobState::Running,
            StatisticsJobState::Publishing,
            now_ms(),
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
    match executor.publish(&publishing, collected.as_ref(), &evidence) {
        Ok(()) => {
            repository
                .transition(
                    publishing.job_id,
                    StatisticsJobState::Publishing,
                    StatisticsJobState::Succeeded,
                    now_ms(),
                    None,
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok(())
        }
        Err(error) => {
            finish_error(
                repository,
                publishing.job_id,
                StatisticsJobState::Publishing,
                error,
            )
            .await
        }
    }
}

async fn must_stop(
    repository: &StatisticsJobRepository,
    job_id: Uuid,
    started: Instant,
    timeout: Duration,
    cancellation: &crate::common::query_cancellation::QueryCancellationView,
) -> Result<bool, String> {
    if cancellation.is_cancelled() || started.elapsed() >= timeout {
        return Ok(true);
    }
    repository
        .cancellation_requested(job_id)
        .await
        .map_err(|error| error.to_string())
}

async fn cancel(
    repository: &StatisticsJobRepository,
    job_id: Uuid,
    expected: StatisticsJobState,
    message: &str,
) -> Result<(), String> {
    repository
        .transition(
            job_id,
            expected,
            StatisticsJobState::Cancelled,
            now_ms(),
            Some(StatisticsJobError {
                kind: StatisticsJobErrorKind::Cancelled,
                message: message.into(),
            }),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn finish_error(
    repository: &StatisticsJobRepository,
    job_id: Uuid,
    expected: StatisticsJobState,
    error: StatisticsAttemptError,
) -> Result<(), String> {
    let (next, kind) = match error.publication {
        Some(StatisticsPublicationTerminal::CommitUnknown) => (
            StatisticsJobState::CommitUnknown,
            StatisticsJobErrorKind::CommitUnknown,
        ),
        Some(StatisticsPublicationTerminal::KnownCommittedFinalization) => (
            StatisticsJobState::Succeeded,
            StatisticsJobErrorKind::KnownCommittedFinalization,
        ),
        Some(StatisticsPublicationTerminal::KnownUncommitted) | None => {
            (StatisticsJobState::Failed, error.kind)
        }
    };
    repository
        .transition(
            job_id,
            expected,
            next,
            now_ms(),
            Some(StatisticsJobError {
                kind,
                message: error.message,
            }),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{
        StatisticsAttemptError, StatisticsAttemptExecutor, StatisticsCollectedAttempt, run_worker,
    };
    use crate::statistics_jobs::model::StatisticsJob;
    use crate::statistics_jobs::repository::StatisticsJobRepository;
    use crate::workload_lifecycle::FrontendServingLifecycle;

    struct NeverRunExecutor;

    impl StatisticsAttemptExecutor for NeverRunExecutor {
        fn collect(
            &self,
            _job: &StatisticsJob,
        ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
            unreachable!("draining must reject before a statistics attempt starts")
        }

        fn prepare_publish(
            &self,
            _job: &StatisticsJob,
            _collected: &dyn StatisticsCollectedAttempt,
        ) -> Result<novarocks_spi::connector::ExternalMutationEvidence, StatisticsAttemptError>
        {
            unreachable!("draining must reject before a statistics attempt starts")
        }

        fn publish(
            &self,
            _job: &StatisticsJob,
            _collected: &dyn StatisticsCollectedAttempt,
            _evidence: &novarocks_spi::connector::ExternalMutationEvidence,
        ) -> Result<(), StatisticsAttemptError> {
            unreachable!("draining must reject before a statistics attempt starts")
        }
    }

    #[tokio::test]
    async fn draining_rejects_the_worker_before_it_can_claim_an_attempt() {
        let lifecycle = FrontendServingLifecycle::new();
        lifecycle.mark_ready().expect("mark lifecycle ready");
        lifecycle.begin_drain(Duration::from_secs(1));
        let executor: Arc<dyn StatisticsAttemptExecutor> = Arc::new(NeverRunExecutor);

        run_worker(
            StatisticsJobRepository::new(),
            Arc::downgrade(&executor),
            Arc::new(AtomicBool::new(false)),
            lifecycle,
        )
        .await
        .expect("draining worker exits without claiming a job");
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
