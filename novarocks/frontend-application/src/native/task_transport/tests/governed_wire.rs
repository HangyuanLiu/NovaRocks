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

//! Original fixed-WC source for actual Native wire fixtures. This fixture
//! proves request/physical/retained lifetime, not complete Native protection.
use super::*;
use crate::task_execution::intent::TaskOperationQueuePreparation;
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub(super) struct GovernedWireRole {
    control: WorkloadControl,
    driver: Mutex<JointProgressDriver>,
    _root: novarocks_workload_control::RootWork,
    window: novarocks_workload_control::ResultWindowGrant,
}
impl GovernedWireRole {
    pub(super) fn new(peer: &Loopback) -> Self {
        let count = 8;
        let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], 4, 2, &[]);
        let mut config = JointCapacityConfig {
            global: JointResourceLimits::default(),
            classes: [JointResourceLimits::default(); 5],
            lanes: [JointResourceLimits::default(); 3],
            class_lanes: [[JointResourceLimits::default(); 3]; 5],
            max_dependencies_per_claim: 2,
            max_registered_dependencies: 1,
            max_claims: 512,
            max_requests: 32,
            max_changes_per_request: 2,
            max_snapshot_bytes: 64 * 1024 * 1024,
            max_scratch_bytes: 64 * 1024 * 1024,
            max_residual_arcs: 100000,
            max_total_proof_work: 10000000,
            work_per_turn: NonZeroUsize::new(31).unwrap(),
        };
        config.max_registered_dependencies = storage.minimum_dependency_records(&config).unwrap();
        let base = storage.base_charge(&config).unwrap();
        let metadata = base
            .get(JointResource::MetadataBytes)
            .checked_mul(64)
            .unwrap();
        let mut custody = JointResources::ZERO
            .with(JointResource::ScopeRecords, count as u64)
            .with(JointResource::ClaimRecords, 256)
            .with(JointResource::ReplyRecords, 512)
            .with(JointResource::QueueRecords, 128)
            .with(JointResource::MetadataBytes, metadata)
            .with(JointResource::PayloadBytes, 16 * 1024 * 1024 * 1024);
        let execution = JointResources::ZERO
            .with(JointResource::ClaimRecords, 128)
            .with(JointResource::MetadataBytes, metadata)
            .with(JointResource::PayloadBytes, 16 * 1024 * 1024 * 1024)
            .with(JointResource::AsyncIo, 64)
            .with(JointResource::PhysicalQueueRecords, 64)
            .with(JointResource::PhysicalQueueBytes, 1024 * 1024 * 1024);
        let custody_limits = JointResourceLimits {
            global: custody,
            per_dependency: custody,
            protected_progress: base,
        };
        let execution_limits = JointResourceLimits {
            global: execution,
            per_dependency: execution,
            protected_progress: JointResources::ZERO,
        };
        config.lanes = [custody_limits, execution_limits, execution_limits];
        config.class_lanes[0] = config.lanes;
        for r in [
            JointResource::ScopeRecords,
            JointResource::ClaimRecords,
            JointResource::ReplyRecords,
            JointResource::QueueRecords,
            JointResource::MetadataBytes,
            JointResource::PayloadBytes,
            JointResource::UnreceivedProductions,
            JointResource::Publications,
            JointResource::Residuals,
            JointResource::Computations,
            JointResource::AsyncIo,
            JointResource::BlockingJobs,
            JointResource::InternalCpuJobs,
            JointResource::PhysicalQueueRecords,
            JointResource::PhysicalQueueBytes,
        ] {
            custody = custody.with(
                r,
                custody
                    .get(r)
                    .checked_add(execution.get(r).checked_mul(2).unwrap())
                    .unwrap(),
            );
        }
        config.global = JointResourceLimits {
            global: custody,
            per_dependency: custody,
            protected_progress: base,
        };
        config.classes[0] = config.global;
        let mut required = RequiredJointProgress {
            global: base,
            classes: [JointResources::ZERO; 5],
            lanes: [JointResources::ZERO; 3],
            class_lanes: [[JointResources::ZERO; 3]; 5],
        };
        required.classes[0] = base;
        required.lanes[0] = base;
        required.class_lanes[0][0] = base;
        // Actual new action vectors are nonzero. Only the fixed substrate's
        // progress is asserted by this lifetime/wire fixture; it does not
        // claim the complete production fault-isolation profile.
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, mut driver) = control
            .configure_joint_capacity(config.validate(&required).unwrap())
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let mut proposal = scope.begin_control_binding().unwrap();
        for endpoint in [&peer.endpoint, &peer.control_endpoint] {
            let origin = novarocks_native_adapter::FrontendNativeTransport::plaintext()
                .selected_network_origin(endpoint.native_endpoint())
                .unwrap()
                .into_owned();
            proposal
                .push_dependency(&novarocks_resource_contract::SharedDependency::Network(
                    origin,
                ))
                .unwrap();
        }
        proposal.end_change().unwrap();
        let mut admission = proposal.submit().unwrap();
        drive_original(&mut driver);
        let mut ready = match Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("original control binding must grant"),
        };
        ready
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope)
            .unwrap()
            .notify();
        drop(ready);
        drive_original(&mut driver);
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        Self {
            control,
            driver: Mutex::new(driver),
            _root: root,
            window,
        }
    }
    pub(super) fn authority(&self) -> NativeAsyncActionAuthority {
        self.window
            .retain_alias()
            .native_action_authority()
            .unwrap()
    }
    pub(super) fn drive(&self) {
        drive_original(&mut self.driver.lock().unwrap())
    }
    pub(super) fn reserve(
        &self,
        sink: &NativeTaskOperationSink,
        intent: &OperationIntent,
    ) -> Box<dyn crate::task_execution::intent::TaskOperationQueuePermit> {
        let mut preparation = TaskOperationQueuePreparation::new();
        match sink.try_reserve_queue(intent.queue_request(), &mut preparation) {
            TaskOperationQueueAdmission::Admitted(permit) => return permit,
            TaskOperationQueueAdmission::Rejected(error) => {
                panic!("original paid input refused: {error}")
            }
            _ => {}
        }
        self.drive();
        match sink.try_reserve_queue(intent.queue_request(), &mut preparation) {
            TaskOperationQueueAdmission::Admitted(permit) => permit,
            result => panic!("original paid wire fixture input remains unavailable: {result:?}"),
        }
    }
}
fn drive_original(driver: &mut JointProgressDriver) {
    for _ in 0..100000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("finite original wire fixture proof driver failed to reach idle")
}
