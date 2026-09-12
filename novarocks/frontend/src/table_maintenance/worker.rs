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

//! Current-process OPTIMIZE worker.
//!
//! It only consumes work submitted to this process runtime. It never scans a
//! previous process, retries an attempt, or reconciles a historical mutation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_workload_control::{
    RootAdmissionHandle, RootWork, WorkClass, WorkError, WorkRequest,
};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::query_execution::maintenance::TableMaintenanceEngine;

use super::now_unix_millis;
use novarocks_table_maintenance::runtime::TerminalError as OptimizeTerminalError;
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceTargetRebind, OptimizeJob, OptimizeProcessRuntime,
    optimize_job_outcome_from_action,
};

/// Runner-owned test root for the STAT-2F cross-process maintenance race.
///
/// When configured in a debug build, a claimed optimize job reports its durable
/// in-process claim before target rebind and waits for the system runner to
/// replace the table incarnation. This is not compiled into release builds.
#[cfg(debug_assertions)]
const STAT2F_TEST_ROOT_ENV: &str = "NOVAROCKS_STAT2F_MAINTENANCE_TEST_DIR";
#[cfg(debug_assertions)]
const STAT2F_TEST_BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub struct OptimizeWorker {
    runtime: Arc<OptimizeProcessRuntime>,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    join: Option<JoinHandle<Result<(), String>>>,
}

/// Worker-local execution adapter. It receives one fresh current-process job;
/// a provider terminal is returned exactly once and never becomes retry input.
pub trait OptimizeJobExecutor: Send + Sync {
    fn execute(
        &self,
        runtime: &Handle,
        engine: &dyn TableMaintenanceEngine,
        job: &OptimizeJob,
    ) -> Result<MaintenanceActionOutcome, OptimizeTerminalError>;
}

impl OptimizeWorker {
    pub fn start_with_executor(
        runtime: &Handle,
        jobs: Arc<OptimizeProcessRuntime>,
        engine: Weak<dyn TableMaintenanceEngine>,
        executor: Arc<dyn OptimizeJobExecutor>,
        root_admission: RootAdmissionHandle,
    ) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());
        let worker_runtime = runtime.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_wakeup = Arc::clone(&wakeup);
        let worker_jobs = Arc::clone(&jobs);
        let join = runtime.spawn(async move {
            run_worker(
                worker_runtime,
                worker_jobs,
                engine,
                executor,
                worker_stop,
                worker_wakeup,
                root_admission,
            )
            .await
        });
        Ok(Self {
            runtime: jobs,
            stop,
            wakeup,
            join: Some(join),
        })
    }

    pub fn wakeup(&self) {
        self.wakeup.notify_one();
    }

    pub fn request_stop(&self) {
        self.runtime.stop_admission();
        self.stop.store(true, Ordering::Release);
        self.wakeup();
    }

    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.request_stop();
        let Some(join) = self.join.as_mut() else {
            return Ok(());
        };
        let joined = tokio::time::timeout_at(deadline.into(), join)
            .await
            .map_err(|_| {
                "table maintenance worker did not stop before the shared shutdown deadline"
                    .to_string()
            })?;
        self.join.take();
        joined.map_err(|error| format!("table maintenance worker join failed: {error}"))?
    }

    pub(crate) fn has_join_owner(&self) -> bool {
        self.join.is_some()
    }
}

async fn run_worker(
    runtime: Handle,
    jobs: Arc<OptimizeProcessRuntime>,
    engine: Weak<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    root_admission: RootAdmissionHandle,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) {
            jobs.request_shutdown_cancellation()
                .await
                .map_err(|error| {
                    format!("request optimize shutdown cancellation failed: {error}")
                })?;
            return Ok(());
        }
        let Some(engine) = engine.upgrade() else {
            jobs.request_shutdown_cancellation()
                .await
                .map_err(|error| {
                    format!("cancel optimize jobs after engine drop failed: {error}")
                })?;
            return Ok(());
        };
        let work =
            match root_admission.try_begin_root(WorkRequest::new(WorkClass::TableMaintenance)) {
                Ok(work) => work,
                Err(WorkError::NotReady)
                | Err(WorkError::Capacity(_))
                | Err(WorkError::CapacityWaitTimeout) => {
                    tokio::select! {
                        _ = wakeup.notified() => {}
                        _ = jobs.wait_for_change() => {}
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                    }
                    continue;
                }
                Err(WorkError::Closed) => {
                    jobs.request_shutdown_cancellation()
                        .await
                        .map_err(|error| {
                            format!("cancel optimize jobs after governed admission closed: {error}")
                        })?;
                    return Ok(());
                }
                Err(error) => return Err(format!("admit governed optimize root failed: {error}")),
            };
        let RootWork { owner, business } = work;
        let cancellation = match owner.scope().cancellation() {
            Ok(view) => novarocks_query_application::cancellation::QueryCancellationView::governed(
                view, None,
            ),
            Err(error) => {
                drop(business);
                owner.complete();
                return Err(format!(
                    "observe governed optimize cancellation failed: {error}"
                ));
            }
        };
        let claimed = match jobs.claim_next(now_unix_millis()).await {
            Ok(claimed) => claimed,
            Err(error) => {
                drop(business);
                owner.complete();
                return Err(format!("claim current optimize job failed: {error}"));
            }
        };
        let Some(job) = claimed else {
            drop(business);
            owner.complete();
            tokio::select! {
                _ = wakeup.notified() => {}
                _ = jobs.wait_for_change() => {}
            }
            continue;
        };
        let result = execute_claimed_job(
            &runtime,
            jobs.as_ref(),
            engine,
            Arc::clone(&executor),
            job,
            cancellation,
        )
        .await;
        drop(business);
        owner.complete();
        result?;
    }
}

async fn execute_claimed_job(
    runtime: &Handle,
    jobs: &OptimizeProcessRuntime,
    engine: Arc<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    job: OptimizeJob,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
) -> Result<(), String> {
    let job_id = job.job_id;
    if cancellation.is_cancelled() {
        jobs.finish(
            job_id,
            Err(OptimizeTerminalError::cancelled_before_dispatch(
                "optimize job cancelled before target rebind",
            )),
            now_unix_millis(),
        )
        .await
        .map_err(|error| format!("record cancelled optimize job failed: {error}"))?;
        return Ok(());
    }
    stat2f_before_rebind_barrier(job_id)?;
    if cancellation.is_cancelled() {
        jobs.finish(
            job_id,
            Err(OptimizeTerminalError::cancelled_before_dispatch(
                "optimize job cancelled before target rebind",
            )),
            now_unix_millis(),
        )
        .await
        .map_err(|error| format!("record cancelled optimize job failed: {error}"))?;
        return Ok(());
    }
    let expected_object_id = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(
        &job.object_id,
    ))
    .map_err(|error| format!("restore optimize job {job_id} target object ID failed: {error}"))?;
    let terminal = match engine.rebind_target_object(&job.target, &expected_object_id) {
        Ok(MaintenanceTargetRebind::Bound) => None,
        Ok(MaintenanceTargetRebind::Replaced) => Some(Err(OptimizeTerminalError::target_replaced(
            "optimize target was replaced before provider dispatch",
        ))),
        Ok(MaintenanceTargetRebind::Missing) => Some(Err(OptimizeTerminalError::failed(
            "optimize target is missing before provider dispatch",
        ))),
        Err(error) => Some(Err(OptimizeTerminalError::failed(format!(
            "optimize target rebind failed before provider dispatch: {error}"
        )))),
    };
    if let Some(terminal) = terminal {
        jobs.finish(job_id, terminal, now_unix_millis())
            .await
            .map_err(|error| format!("record optimize pre-dispatch terminal failed: {error}"))?;
        return Ok(());
    }
    if cancellation.is_cancelled()
        || jobs
            .cancellation_requested(job_id)
            .await
            .map_err(|error| format!("read optimize cancellation failed: {error}"))?
    {
        jobs.finish(
            job_id,
            Err(OptimizeTerminalError::cancelled_before_dispatch(
                "optimize job cancelled before provider dispatch",
            )),
            now_unix_millis(),
        )
        .await
        .map_err(|error| format!("record cancelled optimize job failed: {error}"))?;
        return Ok(());
    }

    stat2f_record_provider_dispatch(job_id)?;
    let worker_runtime = runtime.clone();
    let execution = match tokio::task::spawn_blocking(move || {
        let _diagnostic_scope = crate::preparation_diagnostics::enter_product_work(
            format!("maintenance-job:{job_id}"),
            format!("maintenance-job:{job_id}"),
        );
        executor.execute(&worker_runtime, engine.as_ref(), &job)
    })
    .await
    {
        Ok(Ok(outcome)) => {
            optimize_job_outcome_from_action(outcome).map_err(OptimizeTerminalError::failed)
        }
        Ok(Err(terminal)) => Err(terminal),
        Err(error) => Err(OptimizeTerminalError::failed(format!(
            "optimize job {job_id} engine task failed: {error}"
        ))),
    };
    jobs.finish(job_id, execution, now_unix_millis())
        .await
        .map_err(|error| format!("record optimize terminal failed: {error}"))?;
    Ok(())
}

#[cfg(debug_assertions)]
fn stat2f_before_rebind_barrier(job_id: i64) -> Result<(), String> {
    use std::time::Instant;

    let Some(root) = std::env::var_os(STAT2F_TEST_ROOT_ENV) else {
        return Ok(());
    };
    let paths = stat2f_test_paths(std::path::Path::new(&root), job_id);
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
        std::thread::sleep(std::time::Duration::from_millis(10));
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
    let paths = stat2f_test_paths(std::path::Path::new(&root), job_id);
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
    std::fs::write(&paths.dispatch_count, format!("{}\n", previous + 1)).map_err(|error| {
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

#[cfg(test)]
mod lifecycle_tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::ConnectorTableObjectId;

    use super::{OptimizeJobExecutor, OptimizeWorker, run_worker};
    use crate::query_execution::maintenance::{MaintenanceRequestContext, TableMaintenanceEngine};
    use novarocks_table_maintenance::runtime::TerminalError as OptimizeTerminalError;
    use novarocks_table_maintenance::{
        MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
        MaintenanceTargetRebind, OptimizeJob, OptimizeProcessRuntime,
    };
    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};

    struct NeverRunEngine;

    impl TableMaintenanceEngine for NeverRunEngine {
        fn resolve_target(
            &self,
            _name_parts: &[String],
            _context: MaintenanceRequestContext<'_>,
        ) -> Result<MaintenanceTarget, String> {
            Err("never run".to_string())
        }

        fn capture_target_object_id(
            &self,
            _target: &MaintenanceTarget,
        ) -> Result<ConnectorTableObjectId, String> {
            Err("never run".to_string())
        }

        fn rebind_target_object(
            &self,
            _target: &MaintenanceTarget,
            _expected_object_id: &ConnectorTableObjectId,
        ) -> Result<MaintenanceTargetRebind, String> {
            Err("never run".to_string())
        }

        fn reject_user_action_on_mv(&self, _target: &MaintenanceTarget) -> Result<(), String> {
            Err("never run".to_string())
        }

        fn current_snapshot_id(&self, _target: &MaintenanceTarget) -> Result<i64, String> {
            Err("never run".to_string())
        }

        fn execute_action(
            &self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, String> {
            Err("never run".to_string())
        }
    }

    struct NeverRunExecutor;

    impl OptimizeJobExecutor for NeverRunExecutor {
        fn execute(
            &self,
            _runtime: &tokio::runtime::Handle,
            _engine: &dyn TableMaintenanceEngine,
            _job: &OptimizeJob,
        ) -> Result<MaintenanceActionOutcome, OptimizeTerminalError> {
            Err(OptimizeTerminalError::failed("never run"))
        }
    }

    #[tokio::test]
    async fn starting_worker_waits_for_governed_admission() {
        let parts = WorkloadControl::try_new_split(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 128,
                control_bytes: 16,
                per_scope_bytes: 112,
            },
        )
        .expect("workload control opens");
        let control = parts.owner;
        let jobs = Arc::new(OptimizeProcessRuntime::new());
        let engine: Arc<dyn TableMaintenanceEngine> = Arc::new(NeverRunEngine);
        let executor: Arc<dyn OptimizeJobExecutor> = Arc::new(NeverRunExecutor);
        let worker = tokio::spawn(run_worker(
            tokio::runtime::Handle::current(),
            Arc::clone(&jobs),
            Arc::downgrade(&engine),
            executor,
            Arc::new(AtomicBool::new(false)),
            Arc::new(tokio::sync::Notify::new()),
            parts.root_admission,
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !worker.is_finished(),
            "the optimize worker must not exit while the frontend is starting"
        );

        control.mark_ready().expect("mark governed admission ready");
        control.close_admission();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker observes the terminal serving state")
            .expect("worker task joins")
            .expect("worker exits without claiming a job");
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_optimize_join_for_retry() {
        let release = Arc::new(tokio::sync::Notify::new());
        let wait = Arc::clone(&release);
        let join = tokio::spawn(async move {
            wait.notified().await;
            Ok(())
        });
        let mut worker = OptimizeWorker {
            runtime: Arc::new(OptimizeProcessRuntime::new()),
            stop: Arc::new(AtomicBool::new(false)),
            wakeup: Arc::new(tokio::sync::Notify::new()),
            join: Some(join),
        };

        let error = worker
            .shutdown_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked optimize worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        assert!(worker.has_join_owner());

        release.notify_one();
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained optimize join remains retryable");
        assert!(!worker.has_join_owner());
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
