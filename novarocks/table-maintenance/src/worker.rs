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

//! Product-owned current-process OPTIMIZE job execution.
//!
//! The product owns job claim, terminal interpretation, cancellation checks,
//! and shutdown cancellation. Role composition supplies only a governed root
//! scope and one exact provider/native dispatch adapter.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::runtime::TerminalError;
use crate::{
    AutomaticMaintenanceOutcome, MaintenanceEffectId, MaintenanceTargetRebind, OptimizeJob,
    OptimizeProcessRuntime, optimize_job_outcome_from_action,
};

/// The host-owned scope that accounts for one current-process OPTIMIZE job.
///
/// Dropping this value completes the host's admission bookkeeping. The product
/// reads cancellation but never manufactures a host scope or a second
/// admission authority.
pub trait OptimizeJobScope: Send + Sync {
    fn result_capacity(
        &self,
    ) -> Result<
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        String,
    >;
    fn is_cancelled(&self) -> Result<bool, String>;
}

/// Result of asking the host for one Table Maintenance root scope.
pub enum OptimizeJobAdmission {
    Acquired(Box<dyn OptimizeJobScope>),
    RetryLater,
    Closed,
}

/// Host adapter for role-local admission and resource attribution.
#[async_trait::async_trait]
pub trait OptimizeJobAdmissionPort: Send + Sync {
    async fn begin(&self) -> Result<OptimizeJobAdmission, String>;
}

/// Exact role-local provider/native execution capability for one OPTIMIZE job.
///
/// The product decides when target rebind and provider dispatch may occur. The
/// host validates the frozen physical object identity and consumes the native
/// execution capability; it cannot replace the product's job state machine.
pub trait OptimizeJobExecutionPort: Send + Sync {
    /// Whether the role composition that owns the provider/native capability
    /// still exists. Loss of that owner cancels current-process work instead
    /// of translating host teardown into a provider failure.
    fn is_available(&self) -> bool;

    /// Acquires one exact provider/native execution lease for a claimed job.
    /// The returned value keeps the host capability alive through both target
    /// rebind and dispatch, so host teardown cannot race between them.
    fn acquire(
        &self,
        capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Option<Box<dyn OptimizeJobExecution>>, String>;
}

/// One exact provider/native execution lease for a claimed OPTIMIZE job.
pub trait OptimizeJobExecution: Send {
    fn rebind_target(&self, job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String>;

    fn execute(
        &self,
        job: &OptimizeJob,
        capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<crate::MaintenanceActionOutcome, TerminalError>;

    /// Executes an MV-owned OPTIMIZE using the identity frozen before job
    /// submission. An adapter without this capability fails before dispatch.
    fn execute_automatic(
        &self,
        _job: &OptimizeJob,
        _effect_id: MaintenanceEffectId,
        _capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
        Err(TerminalError::pre_dispatch_failed(
            "automatic optimize effect identity is unsupported",
        ))
    }
}

/// The sole current-process OPTIMIZE worker owner.
pub struct OptimizeWorker {
    runtime: Arc<OptimizeProcessRuntime>,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    join: Option<JoinHandle<Result<(), String>>>,
}

impl OptimizeWorker {
    pub fn start(
        runtime: &Handle,
        jobs: Arc<OptimizeProcessRuntime>,
        admission: Arc<dyn OptimizeJobAdmissionPort>,
        execution: Arc<dyn OptimizeJobExecutionPort>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());
        let join = runtime.spawn(run_worker(
            Arc::clone(&jobs),
            admission,
            execution,
            Arc::clone(&stop),
            Arc::clone(&wakeup),
        ));
        Self {
            runtime: jobs,
            stop,
            wakeup,
            join: Some(join),
        }
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

    pub fn has_join_owner(&self) -> bool {
        self.join.is_some()
    }
}

async fn run_worker(
    jobs: Arc<OptimizeProcessRuntime>,
    admission: Arc<dyn OptimizeJobAdmissionPort>,
    execution: Arc<dyn OptimizeJobExecutionPort>,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) {
            cancel_for_shutdown(jobs.as_ref()).await?;
            return Ok(());
        }
        if !execution.is_available() {
            cancel_for_shutdown(jobs.as_ref()).await?;
            return Ok(());
        }
        let scope = match admission.begin().await? {
            OptimizeJobAdmission::Acquired(scope) => scope,
            OptimizeJobAdmission::RetryLater => {
                tokio::select! {
                    _ = wakeup.notified() => {}
                    _ = jobs.wait_for_change() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
                continue;
            }
            OptimizeJobAdmission::Closed => {
                cancel_for_shutdown(jobs.as_ref()).await?;
                return Ok(());
            }
        };
        let claimed = jobs
            .claim_next(now_unix_millis())
            .await
            .map_err(|error| format!("claim current optimize job failed: {error}"))?;
        let Some(job) = claimed else {
            drop(scope);
            tokio::select! {
                _ = wakeup.notified() => {}
                _ = jobs.wait_for_change() => {}
            }
            continue;
        };
        let job_id = job.job_id;
        let acquisition = match scope.is_cancelled() {
            Ok(true) => {
                // Publish only after the original root/window scope is gone.
                drop(scope);
                finish(
                    jobs.as_ref(),
                    job_id,
                    Err(TerminalError::cancelled_before_dispatch(
                        "optimize job cancelled before execution binding",
                    )),
                )
                .await?;
                continue;
            }
            Ok(false) => scope.result_capacity().and_then(|capacity| {
                execution
                    .acquire(&capacity)
                    .map(|execution| (execution, capacity))
            }),
            Err(error) => Err(error),
        };
        let (execution, capacity) = match acquisition {
            Ok((Some(execution), capacity)) => (execution, capacity),
            Ok((None, capacity)) => {
                drop(capacity);
                drop(scope);
                finish(
                    jobs.as_ref(),
                    job_id,
                    Err(TerminalError::pre_dispatch_failed(
                        "optimize execution owner disappeared before provider dispatch",
                    )),
                )
                .await?;
                cancel_for_shutdown(jobs.as_ref()).await?;
                return Ok(());
            }
            Err(error) => {
                drop(scope);
                finish(
                    jobs.as_ref(),
                    job_id,
                    Err(TerminalError::pre_dispatch_failed(format!(
                        "bind optimize execution before target rebind failed: {error}"
                    ))),
                )
                .await?;
                continue;
            }
        };
        let terminal =
            execute_claimed_job(jobs.as_ref(), execution, scope.as_ref(), capacity, job).await;
        // Design: ADR-0169 (docs/adr/ADR-0169-read-only-hms-and-single-writer-admission.md)
        // The blocking task and its original capacity aliases have actually
        // returned before the root/window scope is dropped and terminal is published.
        drop(scope);
        finish(jobs.as_ref(), job_id, terminal).await?;
    }
}

async fn cancel_for_shutdown(jobs: &OptimizeProcessRuntime) -> Result<(), String> {
    jobs.request_shutdown_cancellation()
        .await
        .map_err(|error| format!("request optimize shutdown cancellation failed: {error}"))
}

async fn execute_claimed_job(
    jobs: &OptimizeProcessRuntime,
    execution: Box<dyn OptimizeJobExecution>,
    scope: &dyn OptimizeJobScope,
    capacity: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    job: OptimizeJob,
) -> Result<crate::OptimizeJobOutcome, TerminalError> {
    let job_id = job.job_id;
    let initially_cancelled = match scope.is_cancelled() {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return Err(TerminalError::pre_dispatch_failed(format!(
                "read optimize cancellation before target rebind failed: {error}"
            )));
        }
    };
    if initially_cancelled {
        return Err(TerminalError::cancelled_before_dispatch(
            "optimize job cancelled before target rebind",
        ));
    }
    let terminal = match execution.rebind_target(&job) {
        Ok(MaintenanceTargetRebind::Bound) => None,
        Ok(MaintenanceTargetRebind::Replaced) => Some(Err(TerminalError::target_replaced(
            "optimize target was replaced before provider dispatch",
        ))),
        Ok(MaintenanceTargetRebind::Missing) => Some(Err(TerminalError::pre_dispatch_failed(
            "optimize target is missing before provider dispatch",
        ))),
        Err(error) => Some(Err(TerminalError::pre_dispatch_failed(format!(
            "optimize target rebind failed before provider dispatch: {error}"
        )))),
    };
    if let Some(terminal) = terminal {
        return terminal;
    }
    let scope_cancelled = match scope.is_cancelled() {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return Err(TerminalError::pre_dispatch_failed(format!(
                "read optimize cancellation before provider dispatch failed: {error}"
            )));
        }
    };
    let job_cancelled = match jobs.cancellation_requested(job_id).await {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return Err(TerminalError::pre_dispatch_failed(format!(
                "read optimize job cancellation before provider dispatch failed: {error}"
            )));
        }
    };
    if scope_cancelled || job_cancelled {
        return Err(TerminalError::cancelled_before_dispatch(
            "optimize job cancelled before provider dispatch",
        ));
    }
    let automatic = job.effect_id.is_some();
    let execution = tokio::task::spawn_blocking(move || match job.effect_id {
        Some(effect_id) => execution
            .execute_automatic(&job, effect_id, &capacity)
            .map(|outcome| {
                let (action, committed) = match outcome {
                    AutomaticMaintenanceOutcome::KnownCommitted(action) => (action, true),
                    AutomaticMaintenanceOutcome::NoOpWithoutCommit(action) => (action, false),
                };
                (action, Some(committed))
            }),
        None => execution
            .execute(&job, &capacity)
            .map(|action| (action, None)),
    })
    .await;
    match execution {
        Ok(Ok((outcome, committed))) => optimize_job_outcome_from_action(outcome)
            .map(|mut outcome| {
                outcome.commit_occurred = committed;
                outcome
            })
            .map_err(|error| {
                if committed == Some(true) {
                    TerminalError::known_committed_finalization_failed(error)
                } else {
                    TerminalError::failed(error)
                }
            }),
        Ok(Err(terminal)) => Err(terminal),
        Err(error) => {
            let message = format!("optimize job {job_id} engine task failed: {error}");
            Err(if automatic {
                TerminalError::commit_unknown(message)
            } else {
                TerminalError::failed(message)
            })
        }
    }
}

async fn finish(
    jobs: &OptimizeProcessRuntime,
    job_id: i64,
    terminal: Result<crate::OptimizeJobOutcome, TerminalError>,
) -> Result<(), String> {
    jobs.finish(job_id, terminal, now_unix_millis())
        .await
        .map(|_| ())
        .map_err(|error| format!("record optimize terminal failed: {error}"))
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::activity::{MaintenanceActivityFamily, TableMaintenanceActivity};
    use crate::runtime::JobCreate;
    use crate::{MaintenanceActionOutcome, MaintenanceTarget};

    struct TestScope {
        cancelled: bool,
        root: Option<novarocks_workload_control::WorkOwner>,
        permit: Option<novarocks_workload_control::QueryConcurrencyPermit>,
        capacity: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    }
    impl Drop for TestScope {
        fn drop(&mut self) {
            drop(self.permit.take());
            if let Some(root) = self.root.take() {
                root.complete();
            }
        }
    }

    impl OptimizeJobScope for TestScope {
        fn result_capacity(
            &self,
        ) -> Result<
            novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
            String,
        > {
            Ok(self.capacity.clone())
        }
        fn is_cancelled(&self) -> Result<bool, String> {
            Ok(self.cancelled)
        }
    }

    pub(crate) async fn admitted_scope_fixture() -> Box<dyn OptimizeJobScope> {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
            WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .root_admission()
            .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
            .unwrap();
        let (permit, window) = root
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Internal)
            .unwrap()
            .await
            .unwrap();
        let capacity = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
        Box::new(TestScope {
            cancelled: false,
            root: Some(root.owner),
            permit: Some(permit),
            capacity,
        })
    }

    struct TestAdmission {
        ready: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl OptimizeJobAdmissionPort for TestAdmission {
        async fn begin(&self) -> Result<OptimizeJobAdmission, String> {
            if self.closed.load(Ordering::Acquire) {
                return Ok(OptimizeJobAdmission::Closed);
            }
            if !self.ready.load(Ordering::Acquire) {
                return Ok(OptimizeJobAdmission::RetryLater);
            }
            Ok(OptimizeJobAdmission::Acquired(
                admitted_scope_fixture().await,
            ))
        }
    }

    #[derive(Clone)]
    struct RecordingExecution {
        calls: Arc<Mutex<Vec<&'static str>>>,
        rebind: MaintenanceTargetRebind,
    }

    impl OptimizeJobExecutionPort for RecordingExecution {
        fn is_available(&self) -> bool {
            true
        }

        fn acquire(
            &self,
            capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        ) -> Result<Option<Box<dyn OptimizeJobExecution>>, String> {
            capacity
                .scope()
                .check()
                .map_err(|error| error.to_string())?;
            Ok(Some(Box::new(self.clone())))
        }
    }

    impl OptimizeJobExecution for RecordingExecution {
        fn rebind_target(&self, _job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String> {
            self.calls.lock().expect("calls lock").push("rebind");
            Ok(self.rebind)
        }

        fn execute(
            &self,
            _job: &OptimizeJob,
            capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        ) -> Result<MaintenanceActionOutcome, TerminalError> {
            assert_eq!(
                capacity.class(),
                novarocks_workload_control::ResultWindowClass::Internal
            );
            capacity.scope().check().unwrap();
            self.calls.lock().expect("calls lock").push("execute");
            Ok(MaintenanceActionOutcome::RewriteDataFiles {
                target_snapshot_id: Some(1),
                rewritten_data_files_count: 1,
                added_data_files_count: None,
                added_delete_files_count: None,
                rewritten_bytes_count: 0,
                failed_data_files_count: 0,
                removed_delete_files_count: 0,
                output_record_count: None,
            })
        }
    }

    fn submit_job(jobs: &OptimizeProcessRuntime) -> impl std::future::Future<Output = i64> + '_ {
        async move {
            let target = MaintenanceTarget {
                catalog: "catalog".to_string(),
                namespace: "namespace".to_string(),
                table: "table".to_string(),
            };
            let permit = TableMaintenanceActivity::default()
                .acquire(&target, MaintenanceActivityFamily::Optimize)
                .expect("permit");
            jobs.submit(
                JobCreate {
                    target,
                    object_id: vec![1],
                    base_snapshot_id: 1,
                    created_at_ms: 1,
                    effect_id: None,
                },
                permit,
            )
            .await
            .expect("submit")
            .job_id
        }
    }

    #[tokio::test]
    async fn worker_waits_for_admission_then_owns_the_complete_job_lifecycle() {
        let jobs = Arc::new(OptimizeProcessRuntime::new());
        let ready = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let job_id = submit_job(jobs.as_ref()).await;
        let mut worker = OptimizeWorker::start(
            &Handle::current(),
            Arc::clone(&jobs),
            Arc::new(TestAdmission {
                ready: Arc::clone(&ready),
                closed: Arc::clone(&closed),
            }),
            Arc::new(RecordingExecution {
                calls: Arc::clone(&calls),
                rebind: MaintenanceTargetRebind::Bound,
            }),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            jobs.get(job_id)
                .await
                .expect("job lookup")
                .is_some_and(|job| job.state == crate::runtime::MaintenanceJobState::Pending),
            "the product worker must not claim work before host admission"
        );
        ready.store(true, Ordering::Release);
        worker.wakeup();
        let terminal = jobs
            .wait_for_completion(job_id)
            .await
            .expect("completed job");
        assert_eq!(
            terminal.state,
            crate::runtime::MaintenanceJobState::Finished
        );
        assert_eq!(*calls.lock().expect("calls lock"), ["rebind", "execute"]);
        closed.store(true, Ordering::Release);
        worker.wakeup();
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("worker shutdown");
    }

    #[tokio::test]
    async fn target_replacement_finishes_without_provider_dispatch() {
        let jobs = Arc::new(OptimizeProcessRuntime::new());
        let ready = Arc::new(AtomicBool::new(true));
        let closed = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let job_id = submit_job(jobs.as_ref()).await;
        let mut worker = OptimizeWorker::start(
            &Handle::current(),
            Arc::clone(&jobs),
            Arc::new(TestAdmission { ready, closed }),
            Arc::new(RecordingExecution {
                calls: Arc::clone(&calls),
                rebind: MaintenanceTargetRebind::Replaced,
            }),
        );
        let terminal = jobs
            .wait_for_completion(job_id)
            .await
            .expect("completed job");
        assert_eq!(
            terminal.state,
            crate::runtime::MaintenanceJobState::TargetReplaced
        );
        assert_eq!(*calls.lock().expect("calls lock"), ["rebind"]);
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("worker shutdown");
    }

    #[tokio::test]
    async fn capacity_binding_failure_finishes_claim_without_target_rebind() {
        struct RefusedExecution;
        impl OptimizeJobExecutionPort for RefusedExecution {
            fn is_available(&self) -> bool {
                true
            }
            fn acquire(
                &self,
                capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
            ) -> Result<Option<Box<dyn OptimizeJobExecution>>, String> {
                assert_eq!(
                    capacity.class(),
                    novarocks_workload_control::ResultWindowClass::Internal
                );
                Err("frozen attempt capacity mismatch".to_string())
            }
        }
        let jobs = Arc::new(OptimizeProcessRuntime::new());
        let job_id = submit_job(jobs.as_ref()).await;
        let mut worker = OptimizeWorker::start(
            &Handle::current(),
            Arc::clone(&jobs),
            Arc::new(TestAdmission {
                ready: Arc::new(AtomicBool::new(true)),
                closed: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(RefusedExecution),
        );
        let terminal = jobs
            .wait_for_completion(job_id)
            .await
            .expect("binding refusal is terminal");
        assert_eq!(
            terminal.state,
            crate::runtime::MaintenanceJobState::PreDispatchFailed
        );
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("worker remains stoppable");
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_optimize_join_for_retry() {
        let release = Arc::new(Notify::new());
        let wait = Arc::clone(&release);
        let join = tokio::spawn(async move {
            wait.notified().await;
            Ok(())
        });
        let mut worker = OptimizeWorker {
            runtime: Arc::new(OptimizeProcessRuntime::new()),
            stop: Arc::new(AtomicBool::new(false)),
            wakeup: Arc::new(Notify::new()),
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

    #[tokio::test]
    async fn terminal_publication_follows_actual_scope_release_on_all_execution_paths() {
        struct ReleaseScope {
            jobs: Arc<OptimizeProcessRuntime>,
            job_id: i64,
            released: Arc<AtomicBool>,
            cancelled: bool,
            admitted: Option<Box<dyn OptimizeJobScope>>,
        }
        impl OptimizeJobScope for ReleaseScope {
            fn result_capacity(
                &self,
            ) -> Result<
                novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
                String,
            > {
                self.admitted
                    .as_ref()
                    .ok_or_else(|| "release fixture scope already dropped".to_string())?
                    .result_capacity()
            }
            fn is_cancelled(&self) -> Result<bool, String> {
                Ok(self.cancelled)
            }
        }
        impl Drop for ReleaseScope {
            fn drop(&mut self) {
                // Drop the actual admitted root/permit/window fixture first.
                drop(self.admitted.take());
                // The repository lookup is synchronous under its mutex. Check
                // the ordering inside release, not after a scheduling delay.
                let lookup = self.jobs.get(self.job_id);
                tokio::pin!(lookup);
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                let std::task::Poll::Ready(Ok(Some(job))) = lookup.as_mut().poll(&mut cx) else {
                    panic!("job lookup must be immediate");
                };
                assert_eq!(
                    job.state,
                    crate::runtime::MaintenanceJobState::Running,
                    "terminal was published while scope resources were still live"
                );
                self.released.store(true, Ordering::Release);
            }
        }
        struct ReleaseAdmission {
            scope: Mutex<Option<ReleaseScope>>,
        }
        #[async_trait::async_trait]
        impl OptimizeJobAdmissionPort for ReleaseAdmission {
            async fn begin(&self) -> Result<OptimizeJobAdmission, String> {
                Ok(match self.scope.lock().unwrap().take() {
                    Some(scope) => OptimizeJobAdmission::Acquired(Box::new(scope)),
                    None => OptimizeJobAdmission::Closed,
                })
            }
        }
        for (rebind, cancelled, expected) in [
            (
                MaintenanceTargetRebind::Bound,
                false,
                crate::runtime::MaintenanceJobState::Finished,
            ),
            (
                MaintenanceTargetRebind::Replaced,
                false,
                crate::runtime::MaintenanceJobState::TargetReplaced,
            ),
            (
                MaintenanceTargetRebind::Bound,
                true,
                crate::runtime::MaintenanceJobState::CancelledBeforeDispatch,
            ),
        ] {
            let jobs = Arc::new(OptimizeProcessRuntime::new());
            let job_id = submit_job(&jobs).await;
            let released = Arc::new(AtomicBool::new(false));
            let mut worker = OptimizeWorker::start(
                &Handle::current(),
                Arc::clone(&jobs),
                Arc::new(ReleaseAdmission {
                    scope: Mutex::new(Some(ReleaseScope {
                        jobs: Arc::clone(&jobs),
                        job_id,
                        released: Arc::clone(&released),
                        cancelled,
                        admitted: Some(admitted_scope_fixture().await),
                    })),
                }),
                Arc::new(RecordingExecution {
                    calls: Arc::new(Mutex::new(Vec::new())),
                    rebind,
                }),
            );
            let terminal =
                tokio::time::timeout(Duration::from_secs(1), jobs.wait_for_completion(job_id))
                    .await
                    .expect("terminal release barrier must complete")
                    .unwrap();
            assert_eq!(terminal.state, expected);
            assert!(released.load(Ordering::Acquire));
            worker
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        }
    }
}
