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

//! Product-owned current-process OPTIMIZE submission and observation.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::activity::{
    MaintenanceActivityBusy, MaintenanceActivityFamily, MaintenanceActivityPermit,
    TableMaintenanceActivity,
};
use crate::runtime::{JobCreate, JobHandle, MaintenanceJobState, RuntimeErrorKind};
use crate::{MaintenanceTarget, OptimizeJob, OptimizeProcessRuntime, OptimizeSubmission};

/// Exact provider facts captured only after the product has the target gate.
///
/// The product persists these opaque values without importing a provider
/// object, a connector handle, or any Native transport type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedOptimizeTarget {
    pub object_id: Vec<u8>,
    pub base_snapshot_id: i64,
}

/// Role-local adapter that captures the current provider binding for an
/// already-gated target.
pub trait OptimizeTargetCapturePort: Send + Sync {
    fn capture(&self, target: &MaintenanceTarget) -> Result<CapturedOptimizeTarget, String>;
}

/// The process-local business owner for maintenance conflict rights and
/// OPTIMIZE job submission, listing, and completion observation.
#[derive(Clone, Default)]
pub struct OptimizeJobService {
    activity: TableMaintenanceActivity,
    runtime: Arc<OptimizeProcessRuntime>,
}

impl OptimizeJobService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn runtime(&self) -> Arc<OptimizeProcessRuntime> {
        Arc::clone(&self.runtime)
    }

    pub fn acquire_activity(
        &self,
        target: &MaintenanceTarget,
        family: MaintenanceActivityFamily,
    ) -> Result<MaintenanceActivityPermit, MaintenanceActivityBusy> {
        self.activity.acquire(target, family)
    }

    /// Acquires the shared target gate before requesting provider facts, which
    /// prevents a same-name replacement from crossing a lock-before-binding
    /// window.
    pub async fn submit_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
    ) -> Result<OptimizeSubmission, String> {
        let permit = self
            .acquire_activity(&target, MaintenanceActivityFamily::Optimize)
            .map_err(|_| "an optimize job is already active for this table".to_string())?;
        let captured = capture.capture(&target)?;
        match self
            .runtime
            .submit(
                JobCreate {
                    target,
                    object_id: captured.object_id,
                    base_snapshot_id: captured.base_snapshot_id,
                    created_at_ms: now_unix_millis(),
                },
                permit,
            )
            .await
        {
            Ok(job) => Ok(OptimizeSubmission::Submitted { job_id: job.job_id }),
            Err(error) if error.kind() == RuntimeErrorKind::AlreadyActive => {
                Ok(OptimizeSubmission::AlreadyActive)
            }
            Err(error) => Err(format!("create optimize job failed: {error}")),
        }
    }

    pub async fn list(&self) -> Result<Vec<OptimizeJob>, String> {
        self.runtime
            .list()
            .await
            .map_err(|error| format!("list optimize jobs failed: {error}"))
    }

    pub async fn wait_for_completion(
        &self,
        handle: JobHandle,
    ) -> Result<MaintenanceJobState, String> {
        self.runtime
            .wait_for_completion(handle.job_id())
            .await
            .map(|job| job.state)
            .map_err(|error| format!("wait for optimize job failed: {error}"))
    }

    pub fn stop_admission(&self) {
        self.runtime.stop_admission();
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedCapture;

    impl OptimizeTargetCapturePort for FixedCapture {
        fn capture(&self, _target: &MaintenanceTarget) -> Result<CapturedOptimizeTarget, String> {
            Ok(CapturedOptimizeTarget {
                object_id: vec![7],
                base_snapshot_id: 11,
            })
        }
    }

    fn target() -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "catalog".to_string(),
            namespace: "namespace".to_string(),
            table: "table".to_string(),
        }
    }

    #[tokio::test]
    async fn submission_keeps_the_exact_handle_and_shared_target_gate() {
        let service = OptimizeJobService::new();
        let first = service
            .submit_optimize(target(), &FixedCapture)
            .await
            .expect("submit first optimize");
        let handle = first.handle().expect("first submission has a handle");
        assert_eq!(handle.job_id(), first.handle().unwrap().job_id());
        assert!(
            service
                .submit_optimize(target(), &FixedCapture)
                .await
                .expect_err("another caller cannot join the first job")
                .contains("already active")
        );
    }

    #[tokio::test]
    async fn capture_happens_only_after_the_shared_target_gate() {
        struct PanicCapture;
        impl OptimizeTargetCapturePort for PanicCapture {
            fn capture(
                &self,
                _target: &MaintenanceTarget,
            ) -> Result<CapturedOptimizeTarget, String> {
                panic!("capture must not run while target is busy")
            }
        }

        let service = OptimizeJobService::new();
        let _permit = service
            .acquire_activity(&target(), MaintenanceActivityFamily::Cleanup)
            .expect("take target gate");
        assert!(
            service
                .submit_optimize(target(), &PanicCapture)
                .await
                .expect_err("busy is not capture failure")
                .contains("already active")
        );
    }
}
