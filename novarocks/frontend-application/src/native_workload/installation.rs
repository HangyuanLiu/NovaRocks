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

//! Mandatory installation recipe from the frontend's complete action authors.
//! The actual WC/source validation happens after binding the same CPU factory.

use super::*;
use novarocks_workload_control::joint_admission::{
    JointAdmissionHandle, JointCapacityConfig, JointLane, JointProgressDriver, JointResource,
    JointResources, RequiredJointProgress,
};
use novarocks_workload_control::{ControlStorageConfig, PollRoleGeometry, WorkloadControl};

/// Opaque production recipe. Only the frontend's concrete action composer can
/// construct one; neither geometry nor caller-supplied Required is a public
/// Native qualification API. Installation revalidates the original sources.
#[derive(Clone, Debug)]
pub struct FrontendWorkloadCapacityProfile {
    inputs: FrontendNativeCapacityInputs,
    workload: WorkloadConfig,
    capacity: JointCapacityConfig,
    required: RequiredJointProgress,
    children_per_slot: usize,
    proposals_per_slot: NonZeroUsize,
    poll_roles: [PollRoleGeometry; 1],
}
impl FrontendWorkloadCapacityProfile {
    /// Private to this original frontend composer. Call only after deriving
    /// complete supported action vectors, inputs and retained-source envelopes.
    /// This is not the still-missing production action recipe itself.
    pub(super) fn from_complete_actions(
        inputs: FrontendNativeCapacityInputs,
        workload: &WorkloadConfig,
        capacity: JointCapacityConfig,
        required: RequiredJointProgress,
        children_per_slot: usize,
        proposals_per_slot: NonZeroUsize,
    ) -> Result<Self, WorkError> {
        workload.validate()?;
        let policy = inputs.policy();
        if workload.scope_records_limit != policy.scope_limit()
            || workload.query_concurrency_limit > policy.slots_per_class()[0]
        {
            return Err(WorkError::InvalidConfig("frontend scope inventory"));
        }
        if capacity.max_changes_per_request < production_batch_records(&capacity)? {
            return Err(WorkError::InvalidConfig(
                "frontend production growth records",
            ));
        }
        let poll_roles = [novarocks_query_application::coordination::LogicalExecutionSupervisor::logical_role_poll_geometry()?];
        let storage = ControlStorageConfig::new(
            policy.slots_per_class(),
            children_per_slot,
            proposals_per_slot.get(),
            &poll_roles,
        );
        // Keep the formula with the original WC metadata author. In particular,
        // changing C for all canonical productions changes the actual Q/M fee.
        if capacity.max_registered_dependencies < storage.minimum_dependency_records(&capacity)? {
            return Err(WorkError::InvalidConfig(
                "frontend fixed dependency inventory",
            ));
        }
        let base = storage.base_charge(&capacity)?;
        let custody = JointLane::Custody as usize;
        let count = policy.scope_limit();
        let n = u64::try_from(count).map_err(|_| WorkError::ArithmeticOverflow)?;
        if capacity.max_claims < count
            || capacity.max_requests
                < count
                    .checked_mul(proposals_per_slot.get())
                    .ok_or(WorkError::ArithmeticOverflow)?
            || capacity.global.global.get(JointResource::ScopeRecords) != n
            || capacity.lanes[custody]
                .global
                .get(JointResource::ScopeRecords)
                != n
            || !covers(capacity.global.global, scale(base, count)?)
            || !covers(capacity.lanes[custody].global, scale(base, count)?)
            || !covers(required.global, base)
            || !covers(required.lanes[custody], base)
        {
            return Err(WorkError::InvalidConfig("frontend fixed global inventory"));
        }
        for (class, count) in policy.slots_per_class().into_iter().enumerate() {
            let n = u64::try_from(count).map_err(|_| WorkError::ArithmeticOverflow)?;
            if capacity.classes[class]
                .global
                .get(JointResource::ScopeRecords)
                != n
                || capacity.class_lanes[class][custody]
                    .global
                    .get(JointResource::ScopeRecords)
                    != n
                || !covers(capacity.classes[class].global, scale(base, count)?)
                || !covers(
                    capacity.class_lanes[class][custody].global,
                    scale(base, count)?,
                )
                || !covers(required.classes[class], base)
                || !covers(required.class_lanes[class][custody], base)
            {
                return Err(WorkError::InvalidConfig("frontend fixed class inventory"));
            }
        }
        for (resource, maximum) in [
            (
                JointResource::UnreceivedProductions,
                policy.dependency_custody().unreceived_productions,
            ),
            (
                JointResource::Publications,
                policy.dependency_custody().publications,
            ),
            (
                JointResource::Residuals,
                policy.dependency_custody().residuals,
            ),
            (
                JointResource::PayloadBytes,
                policy.dependency_custody().retained_payload_bytes,
            ),
        ] {
            if capacity.global.per_dependency.get(resource) > maximum {
                return Err(WorkError::InvalidConfig(
                    "frontend dependency custody policy",
                ));
            }
        }
        Ok(Self {
            inputs,
            workload: workload.clone(),
            capacity,
            required,
            children_per_slot,
            proposals_per_slot,
            poll_roles,
        })
    }

    pub const fn inputs(&self) -> &FrontendNativeCapacityInputs {
        &self.inputs
    }
    pub const fn policy(&self) -> &FrontendWorkloadCapacityConfig {
        self.inputs.policy()
    }
    pub(crate) fn with_native_sources(
        mut self,
        native: &FrontendDataRuntime,
    ) -> Result<Self, WorkError> {
        self.inputs = self.inputs.with_native_sources(native)?;
        Ok(self)
    }
    pub(crate) fn validate_pool_declarations(
        &self,
        workload: &WorkloadConfig,
        blocking: QueryBlockingExecutorConfig,
    ) -> Result<(), WorkError> {
        if !self.matches_workload(workload) {
            return Err(WorkError::Conflict);
        }
        self.inputs.validate_pool_declarations(workload, blocking)
    }
    pub(crate) fn capture_sources(
        &self,
        workload: &WorkloadConfig,
        result: FrontendResultCapacityProfile,
        blocking: &QueryBlockingExecutor,
        cpu: &InternalResultCpu,
    ) -> Result<FrontendNativeCapacitySources, WorkError> {
        self.inputs.capture_sources(workload, result, blocking, cpu)
    }

    /// Same original WC, same registered closed CPU source, actual QA factory.
    /// Storage installation is still not Native serving readiness or I/O grant.
    pub(crate) fn install(
        &self,
        workload: &mut WorkloadControl,
        sources: &FrontendNativeCapacitySources,
    ) -> Result<(JointAdmissionHandle, JointProgressDriver), WorkError> {
        if sources.logical_role != self.poll_roles[0] {
            return Err(WorkError::InvalidConfig("frontend actual logical factory"));
        }
        #[cfg(not(test))]
        if sources.native.is_none() {
            return Err(WorkError::NotReady);
        }
        if self
            .capacity
            .global
            .global
            .get(JointResource::InternalCpuJobs)
            > u64::try_from(sources.internal_cpu.worker_threads().get())
                .map_err(|_| WorkError::ArithmeticOverflow)?
            || self.capacity.global.global.get(JointResource::BlockingJobs)
                > u64::try_from(sources.query_blocking.worker_threads().get())
                    .map_err(|_| WorkError::ArithmeticOverflow)?
        {
            return Err(WorkError::InvalidConfig("frontend actual worker capacity"));
        }
        let capacity = self.inputs.validate_joint_sources(
            workload,
            sources,
            self.capacity.clone(),
            &self.required,
        )?;
        let pair = workload.configure_joint_capacity(capacity)?;
        workload.configure_control_storage(ControlStorageConfig::new(
            self.policy().slots_per_class(),
            self.children_per_slot,
            self.proposals_per_slot.get(),
            &self.poll_roles,
        ))?;
        Ok(pair)
    }

    fn matches_workload(&self, other: &WorkloadConfig) -> bool {
        self.workload.root_limit == other.root_limit
            && self.workload.business_limit == other.business_limit
            && self.workload.query_concurrency_limit == other.query_concurrency_limit
            && self.workload.preparation_limit == other.preparation_limit
            && self.workload.execution_limit == other.execution_limit
            && self.workload.executions_per_root == other.executions_per_root
            && self.workload.waiting_limit == other.waiting_limit
            && self.workload.waiting_bytes == other.waiting_bytes
            && self.workload.capacity_wait_timeout == other.capacity_wait_timeout
            && self.workload.restarts_per_work == other.restarts_per_work
            && self.workload.old_attempts_per_work == other.old_attempts_per_work
            && self.workload.old_attempts_limit == other.old_attempts_limit
            && self.workload.unknown_creates_limit == other.unknown_creates_limit
            && self.workload.scope_records_limit == other.scope_records_limit
            && self.workload.obligation_records_limit == other.obligation_records_limit
            && self.workload.control_inflight_limit == other.control_inflight_limit
            && self.workload.control_ready_limit == other.control_ready_limit
    }
}

/// Each canonical production Claim retains Publications=1 through Unknown and
/// Received. receive_in only retires UnreceivedProductions; settle removes the
/// row. Thus two distinct Root/stage works together cannot exceed this one
/// global bound. Occurrences within a cohort do not consume additional changes.
fn production_batch_records(capacity: &JointCapacityConfig) -> Result<usize, WorkError> {
    usize::try_from(capacity.global.global.get(JointResource::Publications))
        .map_err(|_| WorkError::ArithmeticOverflow)?
        .checked_add(
            novarocks_workload_control::joint_admission::RootStageSource::minimum_change_records(),
        )
        .ok_or(WorkError::ArithmeticOverflow)
}
const RESOURCES: [JointResource; 15] = [
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
];
fn covers(have: JointResources, need: JointResources) -> bool {
    RESOURCES.into_iter().all(|r| have.get(r) >= need.get(r))
}
fn scale(value: JointResources, count: usize) -> Result<JointResources, WorkError> {
    let count = u64::try_from(count).map_err(|_| WorkError::ArithmeticOverflow)?;
    RESOURCES
        .into_iter()
        .try_fold(JointResources::ZERO, |total, r| {
            let amount = value
                .get(r)
                .checked_mul(count)
                .ok_or(WorkError::ArithmeticOverflow)?;
            Ok(total.with(r, amount))
        })
}

#[cfg(test)]
#[path = "installation_tests.rs"]
mod tests;

/// This small lifecycle-only fixture never leaves a cfg(test) build. It does
/// not claim to describe Native action or result/physical-source support.
#[cfg(test)]
pub(crate) fn original_owner_test_workload() -> WorkloadConfig {
    WorkloadConfig {
        root_limit: 20,
        business_limit: 20,
        query_concurrency_limit: 1,
        preparation_limit: 20,
        execution_limit: 20,
        executions_per_root: 4,
        scope_records_limit: 20,
        ..WorkloadConfig::default()
    }
}
#[cfg(test)]
pub(crate) fn original_owner_test_profile(
    blocking: QueryBlockingExecutorConfig,
) -> FrontendWorkloadCapacityProfile {
    use novarocks_workload_control::joint_admission::JointResourceLimits;
    let workload = original_owner_test_workload();
    let policy = FrontendWorkloadCapacityConfig::try_new(
        [4; 5],
        FrontendDependencyCustodyLimits {
            unreceived_productions: 256,
            publications: 256,
            residuals: 256,
            retained_payload_bytes: 4 << 30,
        },
        NonZeroUsize::new(7).unwrap(),
        Duration::from_millis(25),
    )
    .unwrap();
    let inputs = FrontendNativeCapacityInputs::try_new(policy, blocking).unwrap();
    let count = workload.scope_records_limit;
    let proposals = NonZeroUsize::new(2).unwrap();
    let mut capacity = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: 1,
        max_registered_dependencies: 1,
        max_claims: count * 8,
        max_requests: count * proposals.get(),
        max_changes_per_request: 4,
        max_snapshot_bytes: 16 << 20,
        max_scratch_bytes: 16 << 20,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    let roles = [novarocks_query_application::coordination::LogicalExecutionSupervisor::logical_role_poll_geometry().unwrap()];
    let storage = ControlStorageConfig::new([4; 5], 4, proposals.get(), &roles);
    capacity.max_registered_dependencies = storage.minimum_dependency_records(&capacity).unwrap();
    let base = storage.base_charge(&capacity).unwrap();
    let limits = |count| JointResourceLimits {
        global: scale(base, count).unwrap(),
        per_dependency: base,
        protected_progress: base,
    };
    capacity.global = limits(count);
    capacity.lanes[0] = limits(count);
    let mut required = RequiredJointProgress {
        global: base,
        classes: [base; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.lanes[0] = base;
    for class in 0..5 {
        capacity.classes[class] = limits(4);
        capacity.class_lanes[class][0] = limits(4);
        required.class_lanes[class][0] = base;
    }
    FrontendWorkloadCapacityProfile::from_complete_actions(
        inputs, &workload, capacity, required, 4, proposals,
    )
    .unwrap()
}
