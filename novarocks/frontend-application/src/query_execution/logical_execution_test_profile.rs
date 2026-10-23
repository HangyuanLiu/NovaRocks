// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Explicit fixed unit inventory for identity-only FE component tests.
//! No Work or Native action is created; this is not a production profile.

use novarocks_query_application::{
    api::{LogicalExecutionNativePort, QueryExecutionClient},
    coordination::{
        LogicalExecutionSupervisor, LogicalExecutionSupervisorConfig,
        LogicalExecutionSupervisorShutdownError,
    },
};
use novarocks_types::identity::{FrontendProcessId, QueryProcessNamespace};
use novarocks_workload_control::joint_admission::{
    JointCapacityConfig, JointProgress, JointProgressDriver, JointResource, JointResourceLimits,
    JointResources, RequiredJointProgress,
};
use novarocks_workload_control::{ControlStorageConfig, WorkloadConfig, WorkloadControl};
use std::{num::NonZeroUsize, sync::Arc, time::Instant};

pub(crate) struct IdentityTestOwner {
    supervisor: Option<LogicalExecutionSupervisor>,
    control: Option<WorkloadControl>,
    driver: Option<JointProgressDriver>,
    complete: bool,
}
pub(crate) fn process(
    runtime: tokio::runtime::Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
) -> (IdentityTestOwner, QueryExecutionClient) {
    let roles = [LogicalExecutionSupervisor::logical_role_poll_geometry().unwrap()];
    let count = 4;
    let proposals_per_slot = 2;
    let storage =
        ControlStorageConfig::new([count, 0, 0, 0, 0], count - 1, proposals_per_slot, &roles);
    let mut joint = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: 1,
        max_registered_dependencies: 1,
        max_claims: count * 2,
        max_requests: count * 2,
        max_changes_per_request: 1,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    joint.max_registered_dependencies = 4 * joint.max_claims + 2 * count * proposals_per_slot + 3;
    let base = storage.base_charge(&joint).unwrap();
    let metadata = base.get(JointResource::MetadataBytes);
    let limits = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ScopeRecords, count as u64)
            .with(JointResource::ClaimRecords, (count * 2) as u64)
            .with(JointResource::MetadataBytes, metadata * count as u64 * 4),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, (count - 1) as u64)
            .with(JointResource::ClaimRecords, count as u64)
            .with(JointResource::MetadataBytes, metadata * count as u64 * 2),
        protected_progress: base,
    };
    joint.global = limits;
    joint.classes[0] = limits;
    joint.lanes[0] = limits;
    joint.class_lanes[0][0] = limits;
    let mut required = RequiredJointProgress {
        global: base,
        classes: [JointResources::ZERO; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.classes[0] = base;
    required.lanes[0] = base;
    required.class_lanes[0][0] = base;
    // Counted is only the existing initializing constructor. The actual same
    // owner immediately installs joint+fixed+bank, with no counted fallback.
    let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, driver) = control
        .configure_joint_capacity(joint.validate(&required).unwrap())
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    let (supervisor, client) = LogicalExecutionSupervisor::try_new(
        &mut control,
        0,
        NonZeroUsize::new(7).unwrap(),
        runtime,
        native,
        namespace,
        frontend,
        config,
    )
    .unwrap();
    control.mark_ready().unwrap();
    (
        IdentityTestOwner {
            supervisor: Some(supervisor),
            control: Some(control),
            driver: Some(driver),
            complete: false,
        },
        client,
    )
}
impl IdentityTestOwner {
    pub(crate) async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionSupervisorShutdownError> {
        if self.complete {
            return Ok(());
        }
        self.supervisor
            .as_mut()
            .unwrap()
            .shutdown_until(deadline)
            .await?;
        self.control.as_ref().unwrap().close_admission();
        // These exact tests reserve IDs only: no Work/request/source exists.
        // Reclaim the actual bank after its SAME supervisor root really joined.
        loop {
            assert!(
                Instant::now() < deadline,
                "identity fixture reaper deadline exceeded"
            );
            match self.driver.as_mut().unwrap().step() {
                JointProgress::Idle(_) => break,
                JointProgress::More => tokio::task::yield_now().await,
            }
        }
        drop(self.driver.take());
        let owner = self.control.take().unwrap();
        if let Err(failure) = owner.shutdown() {
            let (error, owner) = failure.into_parts();
            self.control = Some(owner);
            panic!("identity fixture kept actual unresolved responsibility: {error}");
        }
        self.complete = true;
        Ok(())
    }
}
impl Drop for IdentityTestOwner {
    fn drop(&mut self) {
        if !self.complete {
            // Failed assertion/cancelled shutdown retains the same owner graph;
            // dropping a test wrapper never fabricates a successful root join.
            std::mem::forget(self.supervisor.take());
            std::mem::forget(self.driver.take());
            std::mem::forget(self.control.take());
        }
    }
}
