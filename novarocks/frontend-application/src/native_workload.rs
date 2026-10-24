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

//! Unvalidated Native capacity inputs and facts from original runtime owners.
//!
//! Neither configuration nor these source facts establish Native readiness. The
//! runtime owner supplies complete progress facts and retains independent
//! resident/payload/physical owners before installing its actual services.

use std::num::NonZeroUsize;
use std::time::Duration;

use novarocks_query_application::cpu::{QueryBlockingExecutor, QueryBlockingExecutorConfig};
use novarocks_workload_control::{WorkError, WorkloadConfig};

use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::transport_supervisor::NativeTaskOperationCapacityFacts;
use crate::query_execution::internal_result_cpu::{
    FrontendResultCapacityProfile, InternalCpuActionQuote, InternalCpuMetadataGeometry,
    InternalResultCpu,
};

/// Deployment policy for one dependency, not a backing quote or grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendDependencyCustodyLimits {
    pub unreceived_productions: u64,
    pub publications: u64,
    pub residuals: u64,
    pub retained_payload_bytes: u64,
}

/// Uninstalled policy. Class order is Query, MaterializedView, Statistics,
/// TableMaintenance, Management.
#[derive(Clone, Debug)]
pub struct FrontendWorkloadCapacityConfig {
    slots_per_class: [usize; 5],
    scope_limit: usize,
    dependency: FrontendDependencyCustodyLimits,
    turn_budget: NonZeroUsize,
    recheck_interval: Duration,
}
impl FrontendWorkloadCapacityConfig {
    pub fn try_new(
        slots_per_class: [usize; 5],
        dependency: FrontendDependencyCustodyLimits,
        turn_budget: NonZeroUsize,
        recheck_interval: Duration,
    ) -> Result<Self, WorkError> {
        let scope_limit = slots_per_class.iter().try_fold(0usize, |sum, n| {
            sum.checked_add(*n).ok_or(WorkError::ArithmeticOverflow)
        })?;
        if slots_per_class.contains(&0)
            || dependency.unreceived_productions == 0
            || dependency.publications == 0
            || dependency.residuals == 0
            || dependency.retained_payload_bytes == 0
            || recheck_interval.is_zero()
        {
            return Err(WorkError::InvalidConfig("frontend finite workload policy"));
        }
        Ok(Self {
            slots_per_class,
            scope_limit,
            dependency,
            turn_budget,
            recheck_interval,
        })
    }
    pub const fn scope_limit(&self) -> usize {
        self.scope_limit
    }
    pub const fn slots_per_class(&self) -> [usize; 5] {
        self.slots_per_class
    }
    pub const fn dependency_custody(&self) -> FrontendDependencyCustodyLimits {
        self.dependency
    }
    pub const fn turn_budget(&self) -> NonZeroUsize {
        self.turn_budget
    }
    pub const fn recheck_interval(&self) -> Duration {
        self.recheck_interval
    }
}

/// Unvalidated deployment inputs, not a Joint profile, installation, or grant.
/// The original RuntimeOwner must combine these with actual source geometry
/// and complete supported progress before configuring Joint/fixed storage.
#[derive(Clone, Debug)]
pub struct FrontendNativeCapacityInputs {
    policy: FrontendWorkloadCapacityConfig,
    query_blocking: QueryBlockingExecutorConfig,
    native: Option<NativeProcessCapacitySources>,
}
impl FrontendNativeCapacityInputs {
    pub fn try_new(
        policy: FrontendWorkloadCapacityConfig,
        query_blocking: QueryBlockingExecutorConfig,
    ) -> Result<Self, WorkError> {
        let geometry = QueryBlockingExecutor::queue_geometry_for_composition()
            .map_err(|_| WorkError::ArithmeticOverflow)?;
        // Check the original declared fixed arrays without constructing a pool.
        // This is not a fee for arbitrary input/result pointees or worker tasks.
        geometry
            .queue_record_inline
            .size()
            .checked_mul(query_blocking.queue_capacity().get())
            .and_then(|queue| {
                geometry
                    .worker_entry_inline
                    .size()
                    .checked_mul(query_blocking.worker_threads().get())
                    .and_then(|workers| queue.checked_add(workers))
            })
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(Self {
            policy,
            query_blocking,
            native: None,
        })
    }
    pub const fn policy(&self) -> &FrontendWorkloadCapacityConfig {
        &self.policy
    }
    pub const fn query_blocking_capacity(&self) -> QueryBlockingExecutorConfig {
        self.query_blocking
    }

    /// Read the same Native owners before any logical-role service is started.
    pub(crate) fn with_native_sources(
        mut self,
        native: &FrontendDataRuntime,
    ) -> Result<Self, WorkError> {
        let source = native.capacity_sources()?;
        if source.subscription_records != self.policy.scope_limit() {
            return Err(WorkError::InvalidConfig(
                "frontend actual subscription inventory",
            ));
        }
        self.native = Some(source);
        Ok(self)
    }

    pub(crate) fn validate_pool_declarations(
        &self,
        workload: &WorkloadConfig,
        query_blocking: QueryBlockingExecutorConfig,
    ) -> Result<(), WorkError> {
        if workload.scope_records_limit != self.policy.scope_limit()
            || query_blocking != self.query_blocking
        {
            return Err(WorkError::InvalidConfig("frontend actual source geometry"));
        }
        Ok(())
    }

    pub(crate) fn capture_sources(
        &self,
        workload: &WorkloadConfig,
        result: FrontendResultCapacityProfile,
        query_blocking: &QueryBlockingExecutor,
        internal_cpu: &InternalResultCpu,
    ) -> Result<FrontendNativeCapacitySources, WorkError> {
        self.validate_pool_declarations(workload, query_blocking.configured_capacity())?;
        if internal_cpu.configured_capacity() != result.internal_cpu_capacity() {
            return Err(WorkError::InvalidConfig("frontend actual source geometry"));
        }
        let internal_action = internal_cpu.operation_action_quote()?;
        if internal_action != result.internal_cpu_action_quote()? {
            return Err(WorkError::InvalidConfig(
                "frontend actual closed CPU factory quote",
            ));
        }
        Ok(FrontendNativeCapacitySources {
            native: self.native,
            logical_role: novarocks_query_application::coordination::LogicalExecutionSupervisor::logical_role_poll_geometry()?,
            logical_root_custody: novarocks_query_application::coordination::LogicalExecutionSupervisor::logical_root_custody_geometry(),
            query_blocking: query_blocking.configured_capacity(),
            query_blocking_queue: query_blocking
                .queue_geometry()
                .map_err(|_| WorkError::ArithmeticOverflow)?,
            internal_cpu: internal_cpu.configured_capacity(),
            internal_action,
            internal_metadata: result
                .internal_cpu_metadata_geometry()
                .map_err(|_| WorkError::ArithmeticOverflow)?,
        })
    }
}

/// Observations from the same original process owners. These are not a
/// RequiredJointProgress vector or evidence that independent pointees are paid.
pub(crate) struct FrontendNativeCapacitySources {
    pub(crate) logical_role: novarocks_workload_control::PollRoleGeometry,
    pub(crate) logical_root_custody:
        novarocks_query_application::coordination::LogicalExecutionRootCustodyGeometry,
    // None is retained only by the existing explicit in-process convenience
    // constructor. Production attaches the original Native owners first.
    pub(crate) native: Option<NativeProcessCapacitySources>,
    pub(crate) query_blocking: QueryBlockingExecutorConfig,
    pub(crate) query_blocking_queue: novarocks_query_application::cpu::QueryBlockingQueueGeometry,
    pub(crate) internal_cpu: QueryBlockingExecutorConfig,
    pub(crate) internal_action: InternalCpuActionQuote,
    pub(crate) internal_metadata: InternalCpuMetadataGeometry,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeProcessCapacitySources {
    pub(crate) apply_records: usize,
    pub(crate) apply_resident_bytes: usize,
    pub(crate) subscription_records: usize,
    pub(crate) subscription_resident_bytes: usize,
    pub(crate) transport: NativeTaskOperationCapacityFacts,
    pub(crate) physical: novarocks_native_adapter::native_transport_admission::AdmissionDimensions,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy(slots: [usize; 5]) -> Result<FrontendWorkloadCapacityConfig, WorkError> {
        FrontendWorkloadCapacityConfig::try_new(
            slots,
            FrontendDependencyCustodyLimits {
                unreceived_productions: 1,
                publications: 2,
                residuals: 3,
                retained_payload_bytes: 4,
            },
            NonZeroUsize::new(7).unwrap(),
            Duration::from_millis(25),
        )
    }
    #[test]
    fn five_class_inventory_has_one_checked_total() {
        assert_eq!(policy([4, 1, 1, 1, 1]).unwrap().scope_limit(), 8);
        assert!(policy([4, 0, 1, 1, 1]).is_err());
        assert!(matches!(
            policy([usize::MAX, 1, 1, 1, 1]),
            Err(WorkError::ArithmeticOverflow)
        ));
    }
    #[test]
    fn input_preserves_original_independent_pool_dimensions_and_checks_overflow() {
        let config = QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let inputs =
            FrontendNativeCapacityInputs::try_new(policy([4, 1, 1, 1, 1]).unwrap(), config)
                .unwrap();
        assert_eq!(inputs.query_blocking_capacity(), config);
        let overflow = QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(usize::MAX).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        assert!(matches!(
            FrontendNativeCapacityInputs::try_new(policy([4, 1, 1, 1, 1]).unwrap(), overflow),
            Err(WorkError::ArithmeticOverflow)
        ));
    }
}
