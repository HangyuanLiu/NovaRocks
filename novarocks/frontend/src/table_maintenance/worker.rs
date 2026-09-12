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

//! Frontend adapters for the product-owned OPTIMIZE worker.

use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_table_maintenance::runtime::TerminalError as OptimizeTerminalError;
use novarocks_table_maintenance::worker::{
    OptimizeJobAdmission, OptimizeJobAdmissionPort, OptimizeJobExecution, OptimizeJobExecutionPort,
    OptimizeJobScope,
};
use novarocks_table_maintenance::{MaintenanceActionOutcome, MaintenanceTargetRebind, OptimizeJob};
use novarocks_workload_control::{
    RootAdmissionHandle, RootWork, WorkClass, WorkError, WorkRequest,
};

use crate::query_execution::maintenance::TableMaintenanceEngine;

use super::DistributedRewriteIntent;

pub(crate) struct FrontendOptimizeJobAdmissionPort {
    root_admission: RootAdmissionHandle,
}

impl FrontendOptimizeJobAdmissionPort {
    pub(crate) fn new(root_admission: RootAdmissionHandle) -> Self {
        Self { root_admission }
    }
}

impl OptimizeJobAdmissionPort for FrontendOptimizeJobAdmissionPort {
    fn try_begin(&self) -> Result<OptimizeJobAdmission, String> {
        match self
            .root_admission
            .try_begin_root(WorkRequest::new(WorkClass::TableMaintenance))
        {
            Ok(work) => Ok(OptimizeJobAdmission::Acquired(Box::new(
                FrontendOptimizeJobScope::new(work),
            ))),
            Err(WorkError::NotReady)
            | Err(WorkError::Capacity(_))
            | Err(WorkError::CapacityWaitTimeout) => Ok(OptimizeJobAdmission::RetryLater),
            Err(WorkError::Closed) => Ok(OptimizeJobAdmission::Closed),
            Err(error) => Err(format!("admit governed optimize root failed: {error}")),
        }
    }
}

struct FrontendOptimizeJobScope {
    work: Mutex<Option<RootWork>>,
}

impl FrontendOptimizeJobScope {
    fn new(work: RootWork) -> Self {
        Self {
            work: Mutex::new(Some(work)),
        }
    }
}

impl OptimizeJobScope for FrontendOptimizeJobScope {
    fn is_cancelled(&self) -> Result<bool, String> {
        let work = self
            .work
            .lock()
            .map_err(|error| format!("lock governed optimize root scope: {error}"))?;
        let work = work
            .as_ref()
            .ok_or_else(|| "governed optimize root scope was released".to_string())?;
        let cancellation =
            work.owner.scope().cancellation().map_err(|error| {
                format!("observe governed optimize cancellation failed: {error}")
            })?;
        Ok(cancellation.reason().is_some())
    }
}

impl Drop for FrontendOptimizeJobScope {
    fn drop(&mut self) {
        let Some(RootWork { owner, business }) = self
            .work
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        else {
            return;
        };
        drop(business);
        owner.complete();
    }
}

pub(crate) struct FrontendOptimizeJobExecutionPort {
    engine: Weak<dyn TableMaintenanceEngine>,
}

impl FrontendOptimizeJobExecutionPort {
    pub(crate) fn new(engine: Weak<dyn TableMaintenanceEngine>) -> Self {
        Self { engine }
    }
}

impl OptimizeJobExecutionPort for FrontendOptimizeJobExecutionPort {
    fn is_available(&self) -> bool {
        self.engine.strong_count() != 0
    }

    fn acquire(&self) -> Option<Box<dyn OptimizeJobExecution>> {
        self.engine.upgrade().map(|engine| {
            Box::new(FrontendOptimizeJobExecution { engine }) as Box<dyn OptimizeJobExecution>
        })
    }
}

struct FrontendOptimizeJobExecution {
    engine: Arc<dyn TableMaintenanceEngine>,
}

impl OptimizeJobExecution for FrontendOptimizeJobExecution {
    fn rebind_target(&self, job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String> {
        stat2f_before_rebind_barrier(job.job_id)?;
        let expected_object_id = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(
            &job.object_id,
        ))
        .map_err(|error| {
            format!(
                "restore optimize job {} target object ID failed: {error}",
                job.job_id
            )
        })?;
        self.engine
            .rebind_target_object(&job.target, &expected_object_id)
    }

    fn execute(
        &self,
        job: &OptimizeJob,
    ) -> Result<MaintenanceActionOutcome, OptimizeTerminalError> {
        stat2f_record_provider_dispatch(job.job_id).map_err(OptimizeTerminalError::failed)?;
        let _diagnostic_scope = crate::preparation_diagnostics::enter_product_work(
            format!("maintenance-job:{}", job.job_id),
            format!("maintenance-job:{}", job.job_id),
        );
        super::execute_distributed_rewrite_terminal(
            self.engine.as_ref(),
            &job.target,
            DistributedRewriteIntent::DataFiles { rewrite_all: true },
        )
    }
}

/// Runner-owned test root for the STAT-2F cross-process maintenance race.
#[cfg(debug_assertions)]
const STAT2F_TEST_ROOT_ENV: &str = "NOVAROCKS_STAT2F_MAINTENANCE_TEST_DIR";
#[cfg(debug_assertions)]
const STAT2F_TEST_BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
        let _scope = ScopedTestEnv::set(&root);
        let job_id = 19;
        let paths = stat2f_test_paths(&root, job_id);

        let waiter = std::thread::spawn(move || stat2f_before_rebind_barrier(job_id));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !paths.ready.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(paths.ready.exists(), "the barrier exposes its ready marker");
        assert_eq!(
            std::fs::read_to_string(&paths.dispatch_count).unwrap(),
            "0\n"
        );
        std::fs::write(&paths.resume, "resume\n").expect("release barrier");
        waiter
            .join()
            .expect("barrier joins")
            .expect("barrier resumes");
        stat2f_record_provider_dispatch(job_id).expect("record first provider dispatch");
        assert_eq!(
            std::fs::read_to_string(&paths.dispatch_count).unwrap(),
            "1\n"
        );
    }
}
