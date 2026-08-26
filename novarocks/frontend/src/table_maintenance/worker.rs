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

use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;
use tokio::runtime::{Builder, Handle};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::query_execution::maintenance::{
    MaintenanceActionOutcome, MaintenanceTargetRebind, TableMaintenanceEngine,
};

use super::model::OptimizeJob;
use super::now_unix_millis;
use super::runtime::{OptimizeProcessRuntime, OptimizeTerminalError};

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
    ) -> Result<MaintenanceActionOutcome, String>;
}

impl OptimizeWorker {
    pub fn start_with_executor(
        runtime: &Handle,
        jobs: Arc<OptimizeProcessRuntime>,
        engine: Weak<dyn TableMaintenanceEngine>,
        executor: Arc<dyn OptimizeJobExecutor>,
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

    pub fn shutdown(&mut self) -> Result<(), String> {
        self.runtime.stop_admission();
        self.stop.store(true, Ordering::Release);
        self.wakeup();
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let joined = if let Ok(runtime) = Handle::try_current() {
            tokio::task::block_in_place(|| runtime.block_on(join))
        } else {
            Builder::new_current_thread()
                .enable_all()
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
    jobs: Arc<OptimizeProcessRuntime>,
    engine: Weak<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
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
        let Some(job) = jobs
            .claim_next(now_unix_millis())
            .await
            .map_err(|error| format!("claim current optimize job failed: {error}"))?
        else {
            tokio::select! {
                _ = wakeup.notified() => {}
                _ = jobs.wait_for_change() => {}
            }
            continue;
        };
        execute_claimed_job(&runtime, jobs.as_ref(), engine, Arc::clone(&executor), job).await?;
    }
}

async fn execute_claimed_job(
    runtime: &Handle,
    jobs: &OptimizeProcessRuntime,
    engine: Arc<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    job: OptimizeJob,
) -> Result<(), String> {
    let job_id = job.job_id;
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
    if jobs
        .cancellation_requested(job_id)
        .await
        .map_err(|error| format!("read optimize cancellation failed: {error}"))?
    {
        jobs.finish(
            job_id,
            Err(OptimizeTerminalError::failed(
                "optimize job cancelled before provider dispatch",
            )),
            now_unix_millis(),
        )
        .await
        .map_err(|error| format!("record cancelled optimize job failed: {error}"))?;
        return Ok(());
    }

    let worker_runtime = runtime.clone();
    let execution = tokio::task::spawn_blocking(move || {
        executor.execute(&worker_runtime, engine.as_ref(), &job)
    })
    .await
    .map_err(|error| format!("optimize job {job_id} engine task failed: {error}"))
    .and_then(|result| result)
    .and_then(optimize_outcome)
    .map_err(OptimizeTerminalError::failed);
    jobs.finish(job_id, execution, now_unix_millis())
        .await
        .map_err(|error| format!("record optimize terminal failed: {error}"))?;
    Ok(())
}

pub(crate) fn optimize_outcome(
    outcome: MaintenanceActionOutcome,
) -> Result<super::model::OptimizeJobOutcome, String> {
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
    Ok(super::model::OptimizeJobOutcome {
        target_snapshot_id,
        rewritten_data_files: i64::from(rewritten_data_files_count),
        deleted_data_files: i64::from(removed_delete_files_count),
        added_data_files: i64::from(added_data_files_count),
        output_record_count,
    })
}
