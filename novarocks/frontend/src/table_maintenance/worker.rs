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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;

use crate::query_execution::maintenance::{
    MaintenanceActionOutcome, MaintenanceTargetRebind, TableMaintenanceEngine,
};
use tokio::runtime::{Builder, Handle};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::FrontendTableMaintenanceService;
use super::coordination::{
    MaintenanceAcquireOutcome, MaintenanceCoordination, MaintenanceFenceValidator,
    MaintenanceLeaseAttempt,
};
use super::model::{MaintenanceAuthorityV1, OptimizeJob, OptimizeJobOutcome};
use super::now_unix_millis;
use super::repository::{DistributedRewriteOperationRepository, OptimizeJobRepository};

const OPTIMIZE_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Runner-owned test root for the STAT-2F cross-process maintenance race.
///
/// When set in a debug build, a claimed optimize job writes three files below
/// this existing directory:
/// - `stat2f-maintenance-optimize-<job-id>.before-rebind.ready`
/// - `stat2f-maintenance-optimize-<job-id>.before-rebind.resume`
/// - `stat2f-maintenance-optimize-<job-id>.dispatch-count`
///
/// The worker writes `0` to the counter and the ready marker after durable
/// claim/authority, then waits for the runner to create the resume trigger.
/// It increments the counter immediately before entering the executor. The
/// hook is intentionally unavailable in release builds and a missing variable
/// is a no-op, so it cannot alter normal maintenance behavior.
const STAT2F_TEST_ROOT_ENV: &str = "NOVAROCKS_STAT2F_MAINTENANCE_TEST_DIR";
const STAT2F_TEST_BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

pub struct OptimizeWorker {
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    join: Option<JoinHandle<Result<(), String>>>,
}

/// Executes a claimed OPTIMIZE job after the worker has established durable
/// ownership. Keeping this port separate lets scheduler tests exercise claim,
/// ordering, and shutdown behavior without fabricating a connector write
/// session; production uses the distributed-rewrite implementation below.
pub trait OptimizeJobExecutor: Send + Sync {
    fn execute(
        &self,
        runtime: &Handle,
        engine: &dyn TableMaintenanceEngine,
        job: &OptimizeJob,
        attempt: &MaintenanceLeaseAttempt,
    ) -> Result<MaintenanceActionOutcome, String>;
}

struct DistributedRewriteOptimizeJobExecutor {
    repository: Arc<DistributedRewriteOperationRepository>,
}

impl OptimizeJobExecutor for DistributedRewriteOptimizeJobExecutor {
    fn execute(
        &self,
        runtime: &Handle,
        engine: &dyn TableMaintenanceEngine,
        job: &OptimizeJob,
        attempt: &MaintenanceLeaseAttempt,
    ) -> Result<MaintenanceActionOutcome, String> {
        FrontendTableMaintenanceService::execute_optimize_distributed_rewrite(
            runtime,
            Arc::clone(&self.repository),
            engine,
            job.target.clone(),
            job.job_id,
            attempt.clone(),
        )
    }
}

impl OptimizeWorker {
    pub fn start(
        runtime: &Handle,
        repository: Arc<OptimizeJobRepository>,
        distributed_rewrite_repository: Arc<DistributedRewriteOperationRepository>,
        engine: Weak<dyn TableMaintenanceEngine>,
        coordination: MaintenanceCoordination,
    ) -> Result<Self, String> {
        Self::start_with_executor(
            runtime,
            repository,
            engine,
            Arc::new(DistributedRewriteOptimizeJobExecutor {
                repository: distributed_rewrite_repository,
            }),
            coordination,
        )
    }

    pub fn start_with_executor(
        runtime: &Handle,
        repository: Arc<OptimizeJobRepository>,
        engine: Weak<dyn TableMaintenanceEngine>,
        executor: Arc<dyn OptimizeJobExecutor>,
        coordination: MaintenanceCoordination,
    ) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());
        let worker_stop = Arc::clone(&stop);
        let worker_wakeup = Arc::clone(&wakeup);
        let worker_runtime = runtime.clone();
        let join = runtime.spawn(async move {
            run_worker(
                worker_runtime,
                repository,
                engine,
                executor,
                coordination,
                worker_stop,
                worker_wakeup,
            )
            .await
        });
        Ok(Self {
            stop,
            wakeup,
            join: Some(join),
        })
    }

    pub fn wakeup(&self) {
        self.wakeup.notify_one();
    }

    pub fn shutdown(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        self.wakeup();
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let joined = if let Ok(runtime) = Handle::try_current() {
            tokio::task::block_in_place(|| runtime.block_on(join))
        } else {
            Builder::new_current_thread()
                .build()
                .map_err(|error| {
                    format!("build table maintenance worker join runtime failed: {error}")
                })?
                .block_on(join)
        };
        joined.map_err(|error| format!("table maintenance worker join failed: {error}"))?
    }
}

async fn run_worker(
    runtime: Handle,
    repository: Arc<OptimizeJobRepository>,
    engine: Weak<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    coordination: MaintenanceCoordination,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if engine.upgrade().is_none() {
            return Ok(());
        }

        // Recovery runs on every poll, not once at startup: a target whose
        // previous holder is still inside its takeover observation window is
        // skipped now and converged by a later round.
        recover_claimed_jobs(repository.as_ref(), &coordination).await?;

        let mut pending = repository
            .list_pending()
            .await
            .map_err(|error| format!("list pending optimize jobs failed: {error}"))?;
        pending.sort_by_key(|job| job.job_id);
        for job in pending {
            if stop.load(Ordering::Acquire) {
                return Ok(());
            }
            let Some(engine) = engine.upgrade() else {
                return Ok(());
            };
            // Per-table authority first. A contended target belongs to another
            // frontend attempt right now, so this worker must not touch its
            // durable record at all.
            let attempt = match coordination.acquire(&job.target).await {
                Ok(MaintenanceAcquireOutcome::Acquired(attempt)) => attempt,
                Ok(
                    MaintenanceAcquireOutcome::Contended(_)
                    | MaintenanceAcquireOutcome::AwaitingTakeover(_),
                ) => continue,
                Err(error) => {
                    return Err(format!(
                        "acquire optimize authority for job {} failed: {error}",
                        job.job_id
                    ));
                }
            };
            let (authority, validator) = match attempt.durable_authority().await {
                Ok(authority) => (authority, attempt.fence_validator()),
                Err(error) => {
                    return Err(format!(
                        "read optimize authority for job {} failed: {error}",
                        job.job_id
                    ));
                }
            };
            let Some(claimed) = repository
                .claim_fenced(
                    job.job_id,
                    now_unix_millis(),
                    authority.clone(),
                    Arc::clone(&validator),
                )
                .await
                .map_err(|error| format!("claim optimize job {} failed: {error}", job.job_id))?
            else {
                continue;
            };
            let executed = execute_claimed_job(
                &runtime,
                repository.as_ref(),
                engine,
                Arc::clone(&executor),
                claimed,
                attempt.clone(),
                authority,
                validator,
            )
            .await;
            release_attempt(&attempt).await;
            executed?;
        }
        if engine.upgrade().is_none() {
            return Ok(());
        }

        tokio::select! {
            _ = wakeup.notified() => {}
            _ = sleep(OPTIMIZE_POLL_INTERVAL) => {}
        }
    }
}

// Design: ADR-0065 (docs/adr/ADR-0065-per-table-maintenance-lease-attempt-authority.md)
/// Converge jobs a previous attempt left RUNNING.
///
/// This replaces the single-frontend restart policy that failed every running
/// job outright. Each job is decided under a freshly acquired attempt, and only
/// on evidence the durable record itself carries:
///
/// * a recorded outcome means the external work is known to have finished, so
///   the job is finalized;
/// * a dispatched child means an external rewrite may have run, so the job
///   fails closed and points at the child that owns the real reconciliation;
/// * neither means nothing was dispatched, so the job returns to PENDING and
///   any frontend may execute it under a new attempt.
///
/// A contended target is skipped: its current holder is still working on it.
async fn recover_claimed_jobs(
    repository: &OptimizeJobRepository,
    coordination: &MaintenanceCoordination,
) -> Result<(), String> {
    let running = repository
        .list_running()
        .await
        .map_err(|error| format!("list running optimize jobs failed: {error}"))?;
    for job in running {
        let attempt = match coordination.acquire(&job.target).await {
            Ok(MaintenanceAcquireOutcome::Acquired(attempt)) => attempt,
            Ok(
                MaintenanceAcquireOutcome::Contended(_)
                | MaintenanceAcquireOutcome::AwaitingTakeover(_),
            ) => continue,
            Err(error) => {
                return Err(format!(
                    "acquire optimize recovery authority for job {} failed: {error}",
                    job.job_id
                ));
            }
        };
        let authority = attempt.durable_authority().await.map_err(|error| {
            format!(
                "read optimize recovery authority for job {} failed: {error}",
                job.job_id
            )
        })?;
        let validator = attempt.fence_validator();
        let job_id = job.job_id;
        if job.outcome.is_some() {
            repository
                .finish_recovered_fenced(job_id, now_unix_millis(), authority, validator)
                .await
                .map_err(|error| {
                    format!("finish recovered optimize job {job_id} failed: {error}")
                })?;
            release_attempt(&attempt).await;
            continue;
        }
        match job.dispatched_child {
            Some(child) => {
                repository
                    .fail_recovered_fenced(
                        job_id,
                        now_unix_millis(),
                        format!(
                            "optimize job dispatched distributed rewrite {child}; its outcome \
                             requires the original exact connector generation"
                        ),
                        authority,
                        validator,
                    )
                    .await
                    .map_err(|error| {
                        format!("fail recovered optimize job {job_id} failed: {error}")
                    })?;
            }
            None => {
                repository
                    .release_undispatched_fenced(job_id, authority, validator)
                    .await
                    .map_err(|error| {
                        format!("release undispatched optimize job {job_id} failed: {error}")
                    })?;
            }
        }
        // The recovery decision is durable. Hand the target back so this same
        // round, or any other frontend, can execute a released job instead of
        // waiting out this attempt's lease.
        release_attempt(&attempt).await;
    }
    Ok(())
}

/// Best-effort lease release. A failed release is not a business failure: the
/// lease expires on its own and CP-1 takeover rules still arbitrate the next
/// acquire.
async fn release_attempt(attempt: &MaintenanceLeaseAttempt) {
    if let Err(error) = attempt.release().await {
        tracing::debug!(%error, "release table maintenance attempt failed");
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_claimed_job(
    runtime: &Handle,
    repository: &OptimizeJobRepository,
    engine: Arc<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    job: OptimizeJob,
    attempt: MaintenanceLeaseAttempt,
    authority: MaintenanceAuthorityV1,
    validator: MaintenanceFenceValidator,
) -> Result<(), String> {
    let job_id = job.job_id;
    stat2f_before_rebind_barrier(job_id)?;
    let expected_object_id = ConnectorTableObjectId::try_new(Bytes::from(job.object_id.clone()))
        .map_err(|error| {
            format!("restore optimize job {job_id} durable target object ID failed: {error}")
        })?;
    match engine.rebind_target_object(&job.target, &expected_object_id) {
        Ok(MaintenanceTargetRebind::Bound) => {}
        Ok(MaintenanceTargetRebind::Replaced) => {
            repository
                .mark_target_replaced_fenced(job_id, now_unix_millis(), authority, validator)
                .await
                .map_err(|error| {
                    format!("mark optimize job {job_id} target replaced failed: {error}")
                })?;
            return Ok(());
        }
        Ok(MaintenanceTargetRebind::Missing) => {
            repository
                .fail_fenced(
                    job_id,
                    now_unix_millis(),
                    "optimize target is missing before provider dispatch".to_string(),
                    authority,
                    validator,
                )
                .await
                .map_err(|error| {
                    format!("fail missing optimize target {job_id} before dispatch failed: {error}")
                })?;
            return Ok(());
        }
        Err(error) => {
            repository
                .fail_fenced(
                    job_id,
                    now_unix_millis(),
                    format!("optimize target rebind failed before provider dispatch: {error}"),
                    authority,
                    validator,
                )
                .await
                .map_err(|store| {
                    format!(
                        "persist optimize pre-dispatch rebind failure for {job_id} failed: {store}"
                    )
                })?;
            return Ok(());
        }
    }
    stat2f_record_provider_dispatch(job_id)?;
    let runtime = runtime.clone();
    let job_attempt = attempt.clone();
    let execution = tokio::task::spawn_blocking(move || {
        executor.execute(&runtime, engine.as_ref(), &job, &job_attempt)
    })
    .await
    .map_err(|error| format!("optimize job {job_id} engine task failed: {error}"))
    .and_then(|result| result)
    .and_then(optimize_outcome);

    let outcome = match execution {
        Ok(outcome) => outcome,
        Err(message) => {
            repository
                .fail_fenced(
                    job_id,
                    now_unix_millis(),
                    message,
                    authority,
                    Arc::clone(&validator),
                )
                .await
                .map_err(|error| format!("fail optimize job {job_id} failed: {error}"))?;
            return Ok(());
        }
    };
    repository
        .record_outcome_fenced(job_id, outcome, authority.clone(), Arc::clone(&validator))
        .await
        .map_err(|error| format!("record outcome for optimize job {job_id} failed: {error}"))?;
    repository
        .finish_fenced(job_id, now_unix_millis(), authority, validator)
        .await
        .map_err(|error| format!("finish optimize job {job_id} failed: {error}"))
}

#[cfg(debug_assertions)]
fn stat2f_before_rebind_barrier(job_id: i64) -> Result<(), String> {
    use std::time::Instant;

    let Some(root) = std::env::var_os(STAT2F_TEST_ROOT_ENV) else {
        return Ok(());
    };
    let root = std::path::PathBuf::from(root);
    let paths = stat2f_test_paths(&root, job_id);
    if paths.resume.exists() {
        return Err(format!(
            "STAT-2F maintenance test resume trigger already exists: {}",
            paths.resume.display()
        ));
    }
    std::fs::write(&paths.dispatch_count, "0\n").map_err(|error| {
        format!(
            "write STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })?;
    std::fs::write(
        &paths.ready,
        format!("job_id={job_id}\nphase=after-claim-before-rebind\n"),
    )
    .map_err(|error| {
        format!(
            "write STAT-2F maintenance ready marker {}: {error}",
            paths.ready.display()
        )
    })?;

    let deadline = Instant::now() + STAT2F_TEST_BARRIER_TIMEOUT;
    while !paths.resume.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if paths.resume.exists() {
        Ok(())
    } else {
        Err(format!(
            "timed out waiting for STAT-2F maintenance resume trigger {}",
            paths.resume.display()
        ))
    }
}

#[cfg(not(debug_assertions))]
fn stat2f_before_rebind_barrier(_job_id: i64) -> Result<(), String> {
    Ok(())
}

#[cfg(debug_assertions)]
fn stat2f_record_provider_dispatch(job_id: i64) -> Result<(), String> {
    let Some(root) = std::env::var_os(STAT2F_TEST_ROOT_ENV) else {
        return Ok(());
    };
    let paths = stat2f_test_paths(&std::path::PathBuf::from(root), job_id);
    let previous = std::fs::read_to_string(&paths.dispatch_count).map_err(|error| {
        format!(
            "read STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })?;
    let previous = previous.trim().parse::<u64>().map_err(|error| {
        format!(
            "parse STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })?;
    let next = previous.checked_add(1).ok_or_else(|| {
        format!(
            "STAT-2F maintenance dispatch counter overflow: {}",
            paths.dispatch_count.display()
        )
    })?;
    std::fs::write(&paths.dispatch_count, format!("{next}\n")).map_err(|error| {
        format!(
            "write STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })
}

#[cfg(not(debug_assertions))]
fn stat2f_record_provider_dispatch(_job_id: i64) -> Result<(), String> {
    Ok(())
}

#[cfg(debug_assertions)]
struct Stat2fTestPaths {
    ready: std::path::PathBuf,
    resume: std::path::PathBuf,
    dispatch_count: std::path::PathBuf,
}

#[cfg(debug_assertions)]
fn stat2f_test_paths(root: &std::path::Path, job_id: i64) -> Stat2fTestPaths {
    let stem = format!("stat2f-maintenance-optimize-{job_id}");
    Stat2fTestPaths {
        ready: root.join(format!("{stem}.before-rebind.ready")),
        resume: root.join(format!("{stem}.before-rebind.resume")),
        dispatch_count: root.join(format!("{stem}.dispatch-count")),
    }
}

#[cfg(all(test, debug_assertions))]
mod stat2f_test_hook_tests {
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use super::{
        STAT2F_TEST_ROOT_ENV, stat2f_before_rebind_barrier, stat2f_record_provider_dispatch,
        stat2f_test_paths,
    };

    static TEST_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct ScopedTestEnv {
        prior: Option<OsString>,
    }

    impl ScopedTestEnv {
        fn set(root: &std::path::Path) -> Self {
            let prior = std::env::var_os(STAT2F_TEST_ROOT_ENV);
            // The process-global environment is serialized by TEST_ENV_LOCK.
            unsafe { std::env::set_var(STAT2F_TEST_ROOT_ENV, root) };
            Self { prior }
        }
    }

    impl Drop for ScopedTestEnv {
        fn drop(&mut self) {
            // The process-global environment is serialized by TEST_ENV_LOCK.
            unsafe {
                if let Some(prior) = self.prior.take() {
                    std::env::set_var(STAT2F_TEST_ROOT_ENV, prior);
                } else {
                    std::env::remove_var(STAT2F_TEST_ROOT_ENV);
                }
            }
        }
    }

    #[test]
    fn barrier_reports_zero_then_one_dispatch_and_resumes_only_on_trigger() {
        let _environment = TEST_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock STAT-2F test environment");
        let temporary = TempDir::new().expect("create test root");
        let root = temporary.path().join("stat2f-hook");
        std::fs::create_dir(&root).expect("create hook directory");
        let _hook_environment = ScopedTestEnv::set(&root);
        let job_id = 42;
        let paths = stat2f_test_paths(&root, job_id);

        let barrier = std::thread::spawn(move || stat2f_before_rebind_barrier(job_id));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !paths.ready.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(paths.ready.exists(), "barrier never reported readiness");
        assert_eq!(
            std::fs::read_to_string(&paths.dispatch_count).expect("read zero counter"),
            "0\n"
        );

        std::fs::write(&paths.resume, "resume\n").expect("create resume trigger");
        barrier
            .join()
            .expect("join barrier thread")
            .expect("resume barrier");
        stat2f_record_provider_dispatch(job_id).expect("record provider dispatch");
        assert_eq!(
            std::fs::read_to_string(&paths.dispatch_count).expect("read dispatch counter"),
            "1\n"
        );
    }
}

pub(crate) fn optimize_outcome(
    outcome: MaintenanceActionOutcome,
) -> Result<OptimizeJobOutcome, String> {
    let MaintenanceActionOutcome::RewriteDataFiles {
        target_snapshot_id,
        rewritten_data_files_count,
        added_data_files_count,
        removed_delete_files_count,
        output_record_count,
        ..
    } = outcome
    else {
        return Err("optimize worker expected a RewriteDataFiles outcome".to_string());
    };
    Ok(OptimizeJobOutcome {
        target_snapshot_id,
        rewritten_data_files: i64::from(rewritten_data_files_count),
        deleted_data_files: i64::from(removed_delete_files_count),
        added_data_files: i64::from(added_data_files_count),
        output_record_count,
    })
}
