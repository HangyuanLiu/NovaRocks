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

    /// Source-derived floor used for every projection in the private composer.
    /// The full original action minimum remains the caller author's duty.
    pub(crate) fn progress_floor(
        &self,
        workload: &mut novarocks_workload_control::WorkloadControl,
        minimum: novarocks_workload_control::joint_admission::JointResources,
        fanout: NonZeroUsize,
    ) -> Result<novarocks_workload_control::joint_admission::JointResourceLimits, WorkError> {
        workload.joint_capacity_floor_for_registered_sources(minimum, fanout)
    }

    /// The original RuntimeOwner's private composer supplies complete action
    /// minima. This step validates only original WC/source capacity; it does
    /// not establish Native readiness or replace payload/physical admission.
    pub(crate) fn validate_joint_sources(
        &self,
        workload: &mut novarocks_workload_control::WorkloadControl,
        sources: &FrontendNativeCapacitySources,
        declared: novarocks_workload_control::joint_admission::JointCapacityConfig,
        required: &novarocks_workload_control::joint_admission::RequiredJointProgress,
    ) -> Result<novarocks_workload_control::joint_admission::ValidatedJointCapacityConfig, WorkError>
    {
        if sources.query_blocking != self.query_blocking {
            return Err(WorkError::InvalidConfig(
                "frontend actual blocking source geometry",
            ));
        }
        workload.validate_joint_capacity_for_registered_sources(declared, required)
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

    // This isolates the real closed factory's capacity coordinates. It is not
    // a complete Native profile or a proof that other source backing is paid.
    fn cpu_floor_probe(
        action: InternalCpuActionQuote,
        fanout: usize,
        physical_positions: usize,
    ) -> (
        novarocks_workload_control::joint_admission::JointCapacityConfig,
        novarocks_workload_control::joint_admission::RequiredJointProgress,
    ) {
        use novarocks_workload_control::joint_admission::{
            JointCapacityConfig, JointResource as R, JointResourceLimits as L, JointResources as V,
            RequiredJointProgress,
        };
        let minimum = V::ZERO
            .with(R::ClaimRecords, 1)
            .with(R::MetadataBytes, action.metadata_bytes().get())
            .with(R::InternalCpuJobs, 1)
            .with(R::PhysicalQueueRecords, 1)
            .with(R::PhysicalQueueBytes, action.physical_queue_bytes().get());
        let global = V::ZERO
            .with(R::ClaimRecords, (fanout + 1) as u64)
            .with(
                R::MetadataBytes,
                (fanout + 1) as u64 * action.metadata_bytes().get(),
            )
            .with(R::InternalCpuJobs, physical_positions as u64)
            .with(R::PhysicalQueueRecords, physical_positions as u64)
            .with(
                R::PhysicalQueueBytes,
                physical_positions as u64 * action.physical_queue_bytes().get(),
            );
        let dependency = global;
        let limits = L {
            global,
            per_dependency: dependency,
            protected_progress: minimum,
        };
        let lanes = [L::default(), limits, L::default()];
        (
            JointCapacityConfig {
                global: limits,
                classes: [limits; 5],
                lanes,
                class_lanes: [lanes; 5],
                max_dependencies_per_claim: fanout,
                max_registered_dependencies: 256,
                max_claims: 32,
                max_requests: 16,
                max_changes_per_request: 4,
                max_snapshot_bytes: 4 * 1024 * 1024,
                max_scratch_bytes: 4 * 1024 * 1024,
                max_residual_arcs: 10000,
                max_total_proof_work: 10_000_000,
                work_per_turn: NonZeroUsize::new(17).unwrap(),
            },
            RequiredJointProgress {
                global: minimum,
                classes: [minimum; 5],
                lanes: [V::ZERO, minimum, V::ZERO],
                class_lanes: [[V::ZERO, minimum, V::ZERO]; 5],
            },
        )
    }

    #[tokio::test]
    async fn original_three_position_closed_factory_uses_its_source_floor_not_network_fanout() {
        use crate::query_execution::internal_result_cpu::InternalResultCpuOwner;
        use novarocks_native_trust::{NativeEndpointConnector, NativeTransportMode};
        use novarocks_types::NativeEndpoint;
        use novarocks_workload_control::joint_admission::JointResource as R;
        use novarocks_workload_control::{ResultCapacityConfig, WorkloadControl};
        let mut domains = std::collections::BTreeSet::new();
        for host in ["be-one.example", "be-two.example", "be-three.example"] {
            for port in [19040, 19041] {
                let endpoint = NativeEndpoint::from_host_port(host, port).unwrap();
                domains.insert(
                    NativeEndpointConnector::selected_origin_for(
                        &endpoint,
                        NativeTransportMode::Disabled,
                    )
                    .unwrap()
                    .into_owned(),
                );
            }
        }
        let mut result = ResultCapacityConfig::V1;
        result.positions[2] = 3;
        let profile = FrontendResultCapacityProfile::try_new(result).unwrap();
        let mut control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let mut cpu = InternalResultCpuOwner::try_new(profile, &mut control).unwrap();
        let runtime = cpu.runtime();
        let actual = runtime.configured_capacity();
        let quote = runtime.operation_action_quote().unwrap();
        let (config, required) =
            cpu_floor_probe(quote, domains.len(), actual.worker_threads().get());
        let old_refuses = matches!(
            config.clone().validate(&required),
            Err(WorkError::InvalidConfig("joint dependency protection"))
        );
        let floor = control.joint_capacity_floor_for_registered_sources(
            required.global,
            NonZeroUsize::new(domains.len()).unwrap(),
        );
        let qualified =
            control.validate_joint_capacity_for_registered_sources(config.clone(), &required);
        let mut foreign = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let missing_refuses = matches!(
            foreign.validate_joint_capacity_for_registered_sources(config.clone(), &required),
            Err(WorkError::NotReady)
        );
        let source_bound = qualified.as_ref().ok().map(|validated| {
            matches!(
                foreign.configure_joint_capacity(validated.clone()),
                Err(WorkError::ForeignAuthority)
            )
        });
        // Keep one unqualified queue increment beside the exact CPU increment.
        // It must retain the K/K+1 floor although both use these same columns.
        let mut mixed = config;
        let mixed_minimum = required.global.with(R::PhysicalQueueRecords, 2).with(
            R::PhysicalQueueBytes,
            2 * quote.physical_queue_bytes().get(),
        );
        let mut mixed_required = required;
        mixed_required.global = mixed_minimum;
        mixed_required.classes = [mixed_minimum; 5];
        mixed_required.lanes[1] = mixed_minimum;
        for class in 0..5 {
            mixed_required.class_lanes[class][1] = mixed_minimum;
        }
        mixed.global.protected_progress = mixed_minimum;
        mixed.classes = [mixed.global; 5];
        mixed.lanes[1] = mixed.global;
        mixed.class_lanes = [mixed.lanes; 5];
        let mixed_refuses = matches!(
            control.validate_joint_capacity_for_registered_sources(mixed, &mixed_required),
            Err(WorkError::InvalidConfig("joint dependency protection"))
        );
        let installed = qualified.and_then(|validated| control.configure_joint_capacity(validated));
        let installation_ok = installed.is_ok();
        drop(installed);
        drop(runtime);
        let shutdown = cpu
            .shutdown_until(std::time::Instant::now() + Duration::from_secs(5))
            .await;
        // All real physical worker handles have been joined before assertions.
        assert_eq!(domains.len(), 6);
        assert_eq!(actual.worker_threads().get(), 3);
        assert_eq!(actual.queue_capacity().get(), 3);
        let floor = floor.unwrap();
        assert_eq!(floor.global.get(R::InternalCpuJobs), 1);
        assert_eq!(floor.per_dependency.get(R::InternalCpuJobs), 1);
        assert_eq!(floor.global.get(R::PhysicalQueueRecords), 1);
        assert_eq!(
            floor.global.get(R::PhysicalQueueBytes),
            quote.physical_queue_bytes().get()
        );
        assert_eq!(floor.global.get(R::ClaimRecords), 7);
        assert!(old_refuses);
        assert!(missing_refuses);
        assert_eq!(source_bound, Some(true));
        assert!(mixed_refuses);
        assert!(installation_ok);
        assert!(
            shutdown.is_ok(),
            "original CPU owner did not exit: {shutdown:?}"
        );
        // No charged dependencies were narrowed by capacity validation.
        assert_eq!(mixed_required.global.get(R::InternalCpuJobs), 1);
    }
}
