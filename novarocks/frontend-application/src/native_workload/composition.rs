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

//! Concrete frontend action composition; no public raw Required constructor.
//!
//! Native queue/reply amounts come from the original supported batch factory.
//! The registered WC validates its real closed CPU source at installation.

use super::*;
use crate::native::task_transport::NativeTaskOperationSink;
use crate::query_execution::maintenance::command::MaintenanceCommandExecutor;
use novarocks_native_adapter::FrontendTaskTransportBudget;
use novarocks_query_application::coordination::LogicalExecutionSupervisor;
use novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry;
use novarocks_workload_control::ControlStorageConfig;
use novarocks_workload_control::joint_admission::{
    JointCapacityConfig, JointLane, JointResource as R, JointResourceLimits as Limits,
    JointResources as V, RequiredJointProgress, RootStageSource,
};

/// Original Server/MySQL facts. These are bounds, not a new listener quota or
/// a substitute for the original ingress author enforcing its same limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendStatementIngress {
    pub ordinary_connections: NonZeroUsize,
    pub control_connections: NonZeroUsize,
    pub ordinary_command_bytes: NonZeroUsize,
    pub control_command_bytes: NonZeroUsize,
}
impl FrontendStatementIngress {
    pub fn sessions(self) -> Result<usize, WorkError> {
        plus(
            self.ordinary_connections.get(),
            self.control_connections.get(),
        )
    }
}

/// A composition fact retained with the profile, not proof that independent
/// original process owners have actually installed or paid these envelopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendCompositionFacts {
    pub maximum_dependencies: usize,
    pub maximum_writer_occurrences: usize,
    pub maximum_protected_payload_increment: u64,
    pub maximum_native_owned_increment: u64,
    pub metadata_source_bytes: u64,
    pub proposals_per_slot: usize,
    pub changes_per_proposal: usize,
    pub session_records: usize,
    pub session_record_inline_bytes: usize,
    pub statement_source_bytes: usize,
    pub explain_peak_inline_bytes: usize,
}

/// Finite per-Node proposal construction inventory policy. It bounds
/// simultaneous undelivered paid bodies, not all live installed grants or I/O.
/// A temporarily full inventory keeps the SAME source input for a later turn.
const PROPOSALS_PER_SLOT: usize = 6;

impl FrontendWorkloadCapacityProfile {
    pub(crate) fn statement_io_geometry(&self) -> Option<ConnectorMetadataMaintenanceIoGeometry> {
        // Lifecycle-only profiles have no statement factories. Their callers
        // still receive NotReady from the Read owner, never an inferred quote.
        self.recipe.map(|recipe| recipe.metadata)
    }

    /// Build the actual recipe from the same original configuration and pure
    /// factory facts. Neither this constructor nor its geometry grants I/O.
    pub fn try_for_native(
        inputs: FrontendNativeCapacityInputs,
        workload: &WorkloadConfig,
        result: FrontendResultCapacityProfile,
        transport: FrontendTaskTransportBudget,
        ingress: FrontendStatementIngress,
        metadata: ConnectorMetadataMaintenanceIoGeometry,
        staged_create_source_bytes: u64,
    ) -> Result<Self, WorkError> {
        inputs.validate_pool_declarations(workload, inputs.query_blocking_capacity())?;
        let cpu = result.internal_cpu_action_quote()?;
        let root_payload = result.internal_initial_payload_bytes();
        let least_owned =
            NativeTaskOperationSink::minimum_owned_support_for_composition(transport.into_codec())?;
        let native = NativeTaskOperationSink::minimum_progress_for_composition(
            transport.into_codec(),
            least_owned,
        )?;
        let (metadata_lanes, _) = metadata_progress(&metadata, 1)?;
        let lanes = action_lanes(
            [native.custody(), native.foreground(), native.settlement()],
            cpu,
            metadata_lanes,
            root_payload,
        )?;
        let physical = PhysicalCaps::from_original(&inputs, result, transport, &metadata)?;
        let ceiling = supported_fanout(&lanes, cpu, physical, inputs.policy())?;
        let mut last_error = WorkError::InvalidConfig("frontend complete supported profile");
        for fanout in (1..=ceiling).rev() {
            match Self::compose_for_fanout(
                &inputs,
                workload,
                transport,
                ingress,
                cpu,
                root_payload,
                physical,
                fanout,
                &metadata,
                staged_create_source_bytes,
            ) {
                Ok(profile) => return Ok(profile),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The original composer combines independently owned source facts without another authority schema."
    )]
    fn compose_for_fanout(
        inputs: &FrontendNativeCapacityInputs,
        workload: &WorkloadConfig,
        transport: FrontendTaskTransportBudget,
        ingress: FrontendStatementIngress,
        cpu: InternalCpuActionQuote,
        root_payload: u64,
        physical: PhysicalCaps,
        fanout: usize,
        metadata: &ConnectorMetadataMaintenanceIoGeometry,
        staged_create_source_bytes: u64,
    ) -> Result<Self, WorkError> {
        let k = as_u64(fanout)?;
        let (metadata_lanes, metadata_source_bytes) = metadata_progress(metadata, fanout)?;
        let owned = supported_owned_increment(
            transport,
            inputs.policy().dependency_custody().retained_payload_bytes,
            fanout,
            metadata_lanes[0].get(R::PayloadBytes),
            root_payload,
        )?;
        let native = NativeTaskOperationSink::minimum_progress_for_composition(
            transport.into_codec(),
            owned,
        )?;
        let mut lanes = action_lanes(
            [native.custody(), native.foreground(), native.settlement()],
            cpu,
            metadata_lanes,
            root_payload,
        )?;
        // This is the actual Custody projection which the original RootStage
        // author reads back as its complete next retained-increment ceiling.
        let protected_payload = lanes[0].get(R::PayloadBytes);
        // First derive the supported non-Production K. Then protect a complete
        // indivisible cohort under the same per-D cumulative policy. This is
        // not an assertion that one attempt must support the cumulative cap.
        let occurrences =
            usize::try_from(inputs.policy().dependency_custody().unreceived_productions / k)
                .map_err(|_| WorkError::ArithmeticOverflow)?;
        let occurrences = NonZeroUsize::new(occurrences)
            .ok_or(WorkError::InvalidConfig("frontend protected writer cohort"))?;
        let production =
            crate::query_execution::write_session::WriteProductionQuote::for_composition(
                occurrences,
            )?;
        lanes[0] = add(lanes[0], production.required_progress())?;
        let proposals = NonZeroUsize::new(PROPOSALS_PER_SLOT).unwrap();
        let count = inputs.policy().scope_limit();
        // Each fixed cell can be a parent of any supported child. This bound
        // is a policy maximum, not an allocation of N children per cell: the
        // original shared child inventory still contains only N-1 edges.
        let children = count.checked_sub(1).ok_or(WorkError::ArithmeticOverflow)?;
        let mut capacity = skeleton(&inputs, proposals, &lanes, fanout)?;
        let role = [LogicalExecutionSupervisor::logical_role_poll_geometry()?];
        let process_sources = process_source_backing(
            ingress,
            transport,
            inputs,
            fanout,
            metadata,
            staged_create_source_bytes,
        )?;
        let storage = ControlStorageConfig::new(
            inputs.policy().slots_per_class(),
            children,
            proposals.get(),
            &role,
        )
        .with_process_source_backing(process_sources);
        capacity.max_registered_dependencies = storage.minimum_dependency_records(&capacity)?;
        if capacity.max_registered_dependencies > u32::MAX as usize {
            return Err(WorkError::InvalidConfig("joint admission finite geometry"));
        }
        capacity.fill_original_proof_geometry()?;
        // This is a finite cumulative work policy, not a capacity or a
        // completeness certificate. Cap the product at the original counter's
        // representable maximum: a large valid inventory must not fail startup
        // merely because its exhaustive-work product exceeds that counter.
        // Actual exhaustion remains a typed refusal under the original driver.
        capacity.max_total_proof_work = [
            capacity.max_requests,
            capacity.max_changes_per_request,
            capacity.max_claims,
            capacity.max_registered_dependencies,
        ]
        .into_iter()
        .try_fold(1u64, |n, count| {
            Ok::<_, WorkError>(n.saturating_mul(as_u64(count)?))
        })?;
        let base = storage.base_charge(&capacity)?;
        // Root and stage remain live across attempts. Every allowed attempt
        // can still own its scan and covered-status children until actual exit;
        // retiring an attempt's obligations does not retire these Sources.
        // The same concrete Native author counts one scan and the complete
        // supported context set. Larger actual cohorts undergo one atomic
        // proof before any child or remote effect, without waiting on a prefix.
        let native_sources =
            crate::query_execution::native_execution_adapter::native_source_count(1, fanout)?;
        let attempts = workload
            .restarts_per_work
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let control_records = plus(
            RootStageSource::minimum_control_records(),
            native_sources
                .get()
                .checked_mul(attempts)
                .ok_or(WorkError::ArithmeticOverflow)?,
        )?;
        // Include the whole original base (Scope, Claim and metadata), not just
        // its visible Scope coordinate. The process Free inventory below is
        // still paid exactly once by N, independently of per-domain headroom.
        lanes[0] = add(lanes[0], scale(base, control_records)?)?;
        let total = sum(lanes)?;
        let required = RequiredJointProgress {
            global: total,
            classes: [total; 5],
            lanes,
            class_lanes: [lanes; 5],
        };
        // The original process Free inventory is charged exactly once. The
        // progress floor is additional headroom, never a replacement for base.
        for lane in 0..3 {
            capacity.lanes[lane] = floor(lanes[lane], fanout, cpu)?;
            if lane == JointLane::Custody as usize {
                capacity.lanes[lane].global = add(
                    without_scope(capacity.lanes[lane].global),
                    scale(base, count)?,
                )?;
            }
        }
        capacity.global = sum_limits(capacity.lanes)?;
        for (class, n) in inputs.policy().slots_per_class().into_iter().enumerate() {
            for lane in 0..3 {
                let mut limits = floor(required.class_lanes[class][lane], fanout, cpu)?;
                if lane == JointLane::Custody as usize {
                    limits.global = add(without_scope(limits.global), scale(base, n)?)?;
                }
                capacity.class_lanes[class][lane] = limits;
            }
            capacity.classes[class] = sum_limits(capacity.class_lanes[class])?;
        }
        // Only actual physical coordinates are tightened. Other dimensions
        // retain the checked finite action/base envelope above. Native queue
        // contributions remain under the original protection formula.
        preserve_internal_capacity(&mut capacity, physical, cpu)?;
        enforce_physical(&mut capacity, physical)?;
        apply_custody_policy(&capacity, inputs.policy())?;
        // Fixed class Sources and physical tightening are now included. The
        // initial ceiling is only a search bound; each complete candidate must
        // satisfy the original WC geometry before it can be selected. This
        // pure check does not qualify the unregistered CPU source for install.
        capacity.check_composition_geometry(&required, cpu.physical_queue_bytes())?;
        let facts = facts(
            ingress,
            protected_payload,
            owned.get(),
            metadata_source_bytes,
            fanout,
            occurrences.get(),
            proposals.get(),
            capacity.max_changes_per_request,
        )?;
        let mut profile = Self::from_complete_actions_with_sources(
            inputs.clone(),
            workload,
            capacity,
            required,
            children,
            proposals,
            process_sources,
        )?;
        profile.composition_facts = Some(facts);
        profile.transport_budget = Some(transport);
        profile.statement_ingress = Some(ingress);
        profile.recipe = Some(NativeCompositionRecipe {
            result_cpu: physical,
            cpu,
            root_payload,
            metadata: *metadata,
            staged_create_source_bytes,
        });
        Ok(profile)
    }

    pub fn statement_ingress(&self) -> Option<FrontendStatementIngress> {
        self.statement_ingress
    }

    pub fn composition_facts(&self) -> Option<FrontendCompositionFacts> {
        self.composition_facts
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NativeCompositionRecipe {
    result_cpu: PhysicalCaps,
    cpu: InternalCpuActionQuote,
    root_payload: u64,
    metadata: ConnectorMetadataMaintenanceIoGeometry,
    staged_create_source_bytes: u64,
}
impl FrontendWorkloadCapacityProfile {
    pub(super) fn rebind_process_sources(self) -> Result<Self, WorkError> {
        let Some(recipe) = self.recipe else {
            return Ok(self);
        };
        let transport = self.transport_budget.ok_or(WorkError::NotReady)?;
        let ingress = self.statement_ingress.ok_or(WorkError::NotReady)?;
        let fanout = self.capacity.max_dependencies_per_claim;
        Self::compose_for_fanout(
            &self.inputs,
            &self.workload,
            transport,
            ingress,
            recipe.cpu,
            recipe.root_payload,
            recipe.result_cpu,
            fanout,
            &recipe.metadata,
            recipe.staged_create_source_bytes,
        )
    }
}

/// The actual closed Internal factory's Root increment is reserved first.
/// Three remaining Native increments can coexist: foreground clone, settlement
/// clone, and queued input. Fixed wire/reply costs come from the SAME Native
/// factory. Metadata initial backing remains a separate actual factory fact.
fn supported_owned_increment(
    transport: FrontendTaskTransportBudget,
    per_dependency_payload: u64,
    fanout: usize,
    metadata_initial: u64,
    root_payload: u64,
) -> Result<std::num::NonZeroU64, WorkError> {
    let least =
        NativeTaskOperationSink::minimum_owned_support_for_composition(transport.into_codec())?;
    let native =
        NativeTaskOperationSink::minimum_progress_for_composition(transport.into_codec(), least)?;
    let total =
        sum([native.custody(), native.foreground(), native.settlement()])?.get(R::PayloadBytes);
    let fixed = total
        .checked_sub(mul(least.get(), 3)?)
        .ok_or(WorkError::Conflict)?;
    let quantum = RootStageSource::protected_payload_increment_for_policy(
        per_dependency_payload,
        NonZeroUsize::new(fanout).ok_or(WorkError::Conflict)?,
    )?;
    let available = quantum
        .checked_sub(fixed)
        .and_then(|n| n.checked_sub(metadata_initial))
        .and_then(|n| n.checked_sub(root_payload))
        .ok_or(WorkError::Capacity(
            "frontend complete owned source increments",
        ))?;
    // Root has its own actual factory invoice, rather than one Native unit.
    let owned = std::num::NonZeroU64::new(available / 3)
        .filter(|n| *n >= least)
        .ok_or(WorkError::Capacity(
            "frontend complete owned source increments",
        ))?;
    Ok(owned)
}
fn metadata_progress(
    geometry: &ConnectorMetadataMaintenanceIoGeometry,
    fanout: usize,
) -> Result<([V; 3], u64), WorkError> {
    let actual =
        MaintenanceCommandExecutor::metadata_io_requirements_for_geometry(geometry, fanout)?;
    let retained = V::ZERO
        .with(R::ClaimRecords, 1)
        .with(R::MetadataBytes, geometry.metadata_bytes().get())
        .with(R::PayloadBytes, actual.initial_payload_bytes);
    Ok((
        [retained, actual.foreground, actual.settlement],
        actual.source_metadata_bytes,
    ))
}
fn action_lanes(
    native: [V; 3],
    cpu: InternalCpuActionQuote,
    metadata: [V; 3],
    root_increment: u64,
) -> Result<[V; 3], WorkError> {
    let cpu = V::ZERO
        .with(R::ClaimRecords, 1)
        .with(R::MetadataBytes, cpu.metadata_bytes().get())
        .with(R::InternalCpuJobs, 1)
        .with(R::PhysicalQueueRecords, 1)
        .with(R::PhysicalQueueBytes, cpu.physical_queue_bytes().get());
    let root = V::ZERO
        .with(R::ClaimRecords, 1)
        .with(R::PayloadBytes, root_increment);
    Ok([
        add(add(native[0], metadata[0])?, root)?,
        add(
            add(add(native[1], metadata[1])?, cpu)?,
            RootStageSource::computation_progress(),
        )?,
        add(native[2], metadata[2])?,
    ])
}

#[derive(Clone, Copy, Debug)]
struct PhysicalCaps {
    internal: u64,
    blocking: u64,
    native: u64,
    native_queue_positions: u64,
    native_queue_bytes: u64,
    cpu_queue_bytes: u64,
    blocking_queue_bytes: u64,
    ordinary_items: u64,
    additional_queue_positions: u64,
}
impl PhysicalCaps {
    fn from_original(
        inputs: &FrontendNativeCapacityInputs,
        result: FrontendResultCapacityProfile,
        transport: FrontendTaskTransportBudget,
        metadata: &ConnectorMetadataMaintenanceIoGeometry,
    ) -> Result<Self, WorkError> {
        let cpu = result.internal_cpu_action_quote()?;
        let internal = as_u64(result.internal_cpu_capacity().worker_threads().get())?;
        Ok(Self {
            internal,
            blocking: as_u64(inputs.query_blocking_capacity().worker_threads().get())?,
            // Metadata uses its original finite cells and no local physical
            // queue. Native still takes its selected original process offer.
            native: checked_add(
                as_u64(transport.max_backend_queued_operations())?,
                mul(
                    as_u64(inputs.policy().scope_limit())?,
                    as_u64(metadata.max_concurrent_io().get())?,
                )?,
            )?,
            native_queue_positions: as_u64(transport.max_backend_queued_operations())?,
            native_queue_bytes: as_u64(transport.max_backend_queued_bytes())?,
            cpu_queue_bytes: mul(internal, cpu.physical_queue_bytes().get())?,
            // No enabled production call reaches prepare_blocking_*: the
            // historical spawn/JobOutcome cleanup path is cfg(test). An
            // enabled genuine source must add its actual author, not CPU proof.
            blocking_queue_bytes: 0,
            ordinary_items: as_u64(transport.max_backend_queued_operations())?,
            additional_queue_positions: 0,
        })
    }
}

/// Largest full-D support bound fitting every real non-Production resource.
/// CPU qualifications remove only the exact registered factory's three
/// coordinates; all charged D remain unchanged and mixed queues retain E.
fn supported_fanout(
    lanes: &[V; 3],
    cpu: InternalCpuActionQuote,
    physical: PhysicalCaps,
    policy: &FrontendWorkloadCapacityConfig,
) -> Result<usize, WorkError> {
    let mut ceiling = policy
        .slots_per_class()
        .into_iter()
        .min()
        .unwrap()
        .checked_sub(1)
        .ok_or(WorkError::InvalidConfig("frontend class progress headroom"))?;
    let total = sum(*lanes)?;
    for (resource, cap) in [
        (R::BlockingJobs, physical.blocking),
        (R::AsyncIo, physical.native),
        (
            R::PhysicalQueueRecords,
            physical
                .internal
                .checked_add(physical.native_queue_positions)
                .and_then(|n| n.checked_add(physical.blocking))
                .and_then(|n| n.checked_add(physical.additional_queue_positions))
                .ok_or(WorkError::ArithmeticOverflow)?,
        ),
        (
            R::PhysicalQueueBytes,
            physical
                .cpu_queue_bytes
                .checked_add(physical.native_queue_bytes)
                .and_then(|n| n.checked_add(physical.blocking_queue_bytes))
                .ok_or(WorkError::ArithmeticOverflow)?,
        ),
        (R::QueueRecords, physical.ordinary_items),
    ] {
        let f = finite(total, resource, cpu)?;
        if cap < f {
            return Err(WorkError::InvalidConfig("frontend actual physical support"));
        }
        let causal = total
            .get(resource)
            .checked_sub(f)
            .ok_or(WorkError::Conflict)?;
        if causal != 0 {
            let fit = cap.checked_sub(f).unwrap() / causal;
            let supported = fit.checked_sub(1).ok_or(WorkError::InvalidConfig(
                "frontend physical protected progress",
            ))?;
            ceiling =
                ceiling.min(usize::try_from(supported).map_err(|_| WorkError::ArithmeticOverflow)?);
        }
    }
    for (resource, cap) in [
        (
            R::UnreceivedProductions,
            policy.dependency_custody().unreceived_productions,
        ),
        (R::Publications, policy.dependency_custody().publications),
        (R::Residuals, policy.dependency_custody().residuals),
        (
            R::PayloadBytes,
            policy.dependency_custody().retained_payload_bytes,
        ),
    ] {
        let amount = total.get(resource);
        if amount != 0 {
            ceiling = ceiling
                .min(usize::try_from(cap / amount).map_err(|_| WorkError::ArithmeticOverflow)?);
        }
    }
    if ceiling == 0 {
        Err(WorkError::InvalidConfig(
            "frontend complete supported dependency progress",
        ))
    } else {
        Ok(ceiling)
    }
}

fn finite(minimum: V, resource: R, cpu: InternalCpuActionQuote) -> Result<u64, WorkError> {
    let jobs = minimum.get(R::InternalCpuJobs);
    match resource {
        R::InternalCpuJobs | R::PhysicalQueueRecords => Ok(jobs),
        R::PhysicalQueueBytes => mul(jobs, cpu.physical_queue_bytes().get()),
        _ => Ok(0),
    }
}
fn floor(minimum: V, k: usize, cpu: InternalCpuActionQuote) -> Result<Limits, WorkError> {
    let mut global = V::ZERO;
    let mut per_dependency = V::ZERO;
    let k = as_u64(k)?;
    for r in RESOURCES {
        let f = finite(minimum, r, cpu)?;
        let causal = minimum.get(r).checked_sub(f).ok_or(WorkError::Conflict)?;
        global = global.with(
            r,
            checked_add(
                f,
                mul(
                    causal,
                    k.checked_add(1).ok_or(WorkError::ArithmeticOverflow)?,
                )?,
            )?,
        );
        per_dependency = per_dependency.with(r, checked_add(f, mul(causal, k)?)?);
    }
    Ok(Limits {
        global,
        per_dependency,
        protected_progress: minimum,
    })
}
const RESOURCES: [R; 15] = [
    R::ScopeRecords,
    R::ClaimRecords,
    R::ReplyRecords,
    R::QueueRecords,
    R::MetadataBytes,
    R::PayloadBytes,
    R::UnreceivedProductions,
    R::Publications,
    R::Residuals,
    R::Computations,
    R::AsyncIo,
    R::BlockingJobs,
    R::InternalCpuJobs,
    R::PhysicalQueueRecords,
    R::PhysicalQueueBytes,
];
fn add(a: V, b: V) -> Result<V, WorkError> {
    RESOURCES
        .into_iter()
        .try_fold(a, |v, r| Ok(v.with(r, checked_add(a.get(r), b.get(r))?)))
}
fn sum(v: [V; 3]) -> Result<V, WorkError> {
    v.into_iter().try_fold(V::ZERO, add)
}
fn scale(v: V, n: usize) -> Result<V, WorkError> {
    let n = as_u64(n)?;
    RESOURCES
        .into_iter()
        .try_fold(V::ZERO, |o, r| Ok(o.with(r, mul(v.get(r), n)?)))
}
fn plus(a: usize, b: usize) -> Result<usize, WorkError> {
    a.checked_add(b).ok_or(WorkError::ArithmeticOverflow)
}
fn checked_add(a: u64, b: u64) -> Result<u64, WorkError> {
    a.checked_add(b).ok_or(WorkError::ArithmeticOverflow)
}
fn mul(a: u64, b: u64) -> Result<u64, WorkError> {
    a.checked_mul(b).ok_or(WorkError::ArithmeticOverflow)
}
fn as_u64(n: usize) -> Result<u64, WorkError> {
    u64::try_from(n).map_err(|_| WorkError::ArithmeticOverflow)
}
fn without_scope(v: V) -> V {
    v.with(R::ScopeRecords, 0)
}
fn sum_limits(lanes: [Limits; 3]) -> Result<Limits, WorkError> {
    Ok(Limits {
        global: sum(lanes.map(|l| l.global))?,
        per_dependency: sum(lanes.map(|l| l.per_dependency))?,
        protected_progress: sum(lanes.map(|l| l.protected_progress))?,
    })
}

fn skeleton(
    inputs: &FrontendNativeCapacityInputs,
    q: NonZeroUsize,
    lanes: &[V; 3],
    k: usize,
) -> Result<JointCapacityConfig, WorkError> {
    let requests = inputs
        .policy()
        .scope_limit()
        .checked_mul(q.get())
        .ok_or(WorkError::ArithmeticOverflow)?;
    let claims = plus(requests, inputs.policy().scope_limit())?;
    let publications = mul(
        sum(*lanes)?.get(R::Publications),
        as_u64(k.checked_add(1).ok_or(WorkError::ArithmeticOverflow)?)?,
    )?;
    let changes = plus(
        RootStageSource::minimum_change_records(),
        usize::try_from(publications).map_err(|_| WorkError::ArithmeticOverflow)?,
    )?;
    Ok(JointCapacityConfig {
        global: Limits::default(),
        classes: [Limits::default(); 5],
        lanes: [Limits::default(); 3],
        class_lanes: [[Limits::default(); 3]; 5],
        max_dependencies_per_claim: k,
        max_registered_dependencies: 1,
        max_claims: claims.max(changes),
        max_requests: requests,
        max_changes_per_request: changes,
        max_snapshot_bytes: 1,
        max_scratch_bytes: 1,
        max_residual_arcs: 1,
        max_total_proof_work: u64::MAX,
        work_per_turn: inputs.policy().turn_budget(),
    })
}
fn preserve_internal_capacity(
    c: &mut JointCapacityConfig,
    p: PhysicalCaps,
    cpu: InternalCpuActionQuote,
) -> Result<(), WorkError> {
    let lane = JointLane::Foreground as usize;
    let original = c.lanes[lane].global.get(R::InternalCpuJobs);
    let extra = p
        .internal
        .checked_sub(original)
        .ok_or(WorkError::InvalidConfig(
            "frontend Internal CPU physical capacity",
        ))?;
    let delta = V::ZERO
        .with(R::InternalCpuJobs, extra)
        .with(R::PhysicalQueueRecords, extra)
        .with(
            R::PhysicalQueueBytes,
            mul(extra, cpu.physical_queue_bytes().get())?,
        );
    c.lanes[lane].global = add(c.lanes[lane].global, delta)?;
    c.lanes[lane].per_dependency = add(c.lanes[lane].per_dependency, delta)?;
    c.global = sum_limits(c.lanes)?;
    for class in 0..5 {
        c.class_lanes[class][lane].global = add(c.class_lanes[class][lane].global, delta)?;
        c.class_lanes[class][lane].per_dependency =
            add(c.class_lanes[class][lane].per_dependency, delta)?;
        c.classes[class] = sum_limits(c.class_lanes[class])?;
    }
    Ok(())
}
fn enforce_physical(c: &mut JointCapacityConfig, p: PhysicalCaps) -> Result<(), WorkError> {
    for (r, cap) in [
        (R::InternalCpuJobs, p.internal),
        (R::BlockingJobs, p.blocking),
        (R::AsyncIo, p.native),
        (R::QueueRecords, p.ordinary_items),
        (
            R::PhysicalQueueRecords,
            checked_add(
                checked_add(
                    checked_add(p.internal, p.native_queue_positions)?,
                    p.blocking,
                )?,
                p.additional_queue_positions,
            )?,
        ),
        (
            R::PhysicalQueueBytes,
            checked_add(
                checked_add(p.cpu_queue_bytes, p.native_queue_bytes)?,
                p.blocking_queue_bytes,
            )?,
        ),
    ] {
        if c.global.global.get(r) > cap {
            return Err(WorkError::InvalidConfig(
                "frontend actual physical capacity",
            ));
        }
    }
    Ok(())
}
fn apply_custody_policy(
    c: &JointCapacityConfig,
    p: &FrontendWorkloadCapacityConfig,
) -> Result<(), WorkError> {
    for (r, cap) in [
        (
            R::UnreceivedProductions,
            p.dependency_custody().unreceived_productions,
        ),
        (R::Publications, p.dependency_custody().publications),
        (R::Residuals, p.dependency_custody().residuals),
        (
            R::PayloadBytes,
            p.dependency_custody().retained_payload_bytes,
        ),
    ] {
        if c.global.per_dependency.get(r) > cap {
            return Err(WorkError::InvalidConfig(
                "frontend dependency custody policy",
            ));
        }
    }
    Ok(())
}
// Independent original Statement roots are not LogicalExecution Source C.
// Their retained cancellation view pins the same fixed cell until the source
// exits. The shared driver and bounded session headers live to process exit.
// Variable action payload/plan/Provider backing remains separately admitted.
fn process_source_backing(
    ingress: FrontendStatementIngress,
    transport: FrontendTaskTransportBudget,
    inputs: &FrontendNativeCapacityInputs,
    complete_dependency_limit: usize,
    metadata: &ConnectorMetadataMaintenanceIoGeometry,
    staged_create_source_bytes: u64,
) -> Result<novarocks_workload_control::PollRoleSourceBacking, WorkError> {
    use std::alloc::Layout;
    let q = novarocks_query_application::session_control::QueryControlService::statement_action_geometry();
    let progress = crate::application::workload_progress::progress_geometry()
        .map_err(|_| WorkError::ArithmeticOverflow)?;
    let sessions =
        novarocks_query_application::query_control::QueryApplicationControl::session_geometry();
    let e = crate::query::explain_statement_geometry();
    let session_bytes = sessions
        .record
        .size()
        .checked_mul(ingress.sessions()?)
        .ok_or(WorkError::ArithmeticOverflow)?;
    let session_records = Layout::from_size_align(session_bytes, sessions.record.align())
        .map_err(|_| WorkError::ArithmeticOverflow)?;
    let future = if e.preparation.size() >= e.diagnostics.size() {
        e.preparation
    } else {
        e.diagnostics
    };
    // The same original root cannot hold two statement-family actions.
    // Each family quote already covers its own action/residual/F overlap.
    let explain_bytes = [e.action, e.receiver, future]
        .into_iter()
        .chain(e.provider_custody)
        .try_fold(0usize, |bytes, layout| plus(bytes, layout.size()))?;
    let insert_bytes = usize::try_from(crate::query::insert_action::fixed_source_bytes(
        metadata,
        complete_dependency_limit,
    )?)
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let delete_bytes = usize::try_from(crate::query::delete_action::fixed_source_bytes(
        metadata,
        complete_dependency_limit,
    )?)
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let read_bytes = usize::try_from(crate::query::read_action::fixed_source_bytes(
        metadata,
        complete_dependency_limit,
    )?)
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let mutation_bytes = usize::try_from(crate::query::mutation_action::fixed_source_bytes(
        metadata,
        complete_dependency_limit,
    )?)
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let ctas_bytes = usize::try_from(crate::query::ctas_action::fixed_source_bytes(
        metadata,
        complete_dependency_limit,
        staged_create_source_bytes,
    )?)
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let maintenance_bytes = usize::try_from(crate::query::metadata_statement_fixed_source_bytes(
        metadata,
        complete_dependency_limit,
    )?)
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let statistics_job_bytes = usize::try_from(
        crate::statistics_jobs::attempt_executor::fixed_source_bytes(
            metadata,
            complete_dependency_limit,
        )?,
    )
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let statistics_submission_bytes = usize::try_from(
        crate::statistics_jobs::service::submission_fixed_source_bytes(
            metadata,
            complete_dependency_limit,
        )?,
    )
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let statement_family = Layout::array::<u8>(
        explain_bytes
            .max(insert_bytes)
            .max(delete_bytes)
            .max(ctas_bytes)
            .max(read_bytes)
            .max(mutation_bytes)
            .max(maintenance_bytes)
            .max(statistics_job_bytes)
            .max(statistics_submission_bytes)
            // Each original statement Root can independently observe the
            // repository. Its list/get/cancel copies and safe String delta
            // are NOT included in the one shared cache quote below.
            .max(
                novarocks_statistics_application::publication_failure_geometry()?
                    .observation_backing
                    .size(),
            ),
    )
    .map_err(|_| WorkError::ArithmeticOverflow)?;
    let apply = NativeTaskOperationSink::source_geometry_for_composition(transport.into_codec())?;
    let apply_bytes = apply.resident_bytes()?;
    if let Some(actual) = inputs.native {
        if actual.apply_records != apply.records || actual.apply_resident_bytes != apply_bytes {
            return Err(WorkError::InvalidConfig(
                "frontend actual Apply source geometry",
            ));
        }
    }
    let waits = NativeTaskOperationSink::proposal_wait_source_geometry(transport.into_codec())?;
    if inputs
        .native
        .is_some_and(|actual| actual.transport.waiter_records != waits.records)
    {
        return Err(WorkError::InvalidConfig(
            "frontend actual Native source waiter geometry",
        ));
    }
    let native_shared = apply_bytes
        .checked_add(waits.resident_bytes()?)
        .and_then(|bytes| {
            bytes.checked_add(inputs.native.map_or(0, |s| s.subscription_resident_bytes))
        })
        .ok_or(WorkError::ArithmeticOverflow)?;
    let native_layout =
        Layout::array::<u8>(native_shared).map_err(|_| WorkError::ArithmeticOverflow)?;
    let mut source_layouts = vec![
        q.registry_record,
        q.source,
        q.wake,
        q.observer,
        q.receipt,
        q.failure,
        q.failure,
        statement_family,
    ];
    // Consume every actual inline, retained Arc and Q-wait layout exposed by
    // the original binding and cohort authors; no duplicated process bank.
    source_layouts.extend_from_slice(
        &crate::query_execution::native_execution_adapter::native_control_binding_layouts(),
    );
    novarocks_workload_control::PollRoleSourceBacking::from_layouts(
        &source_layouts,
        &[
            q.driver,
            sessions.owner,
            session_records,
            native_layout,
            progress.supervisor_inline,
            progress.custody_arc,
            progress.worker_frame,
            progress.owned_sleep,
            // These fixed facts outlive their original Root and belong to
            // the SAME process repository; never charge them per Root.
            novarocks_statistics_application::publication_failure_geometry()?.backing,
            // The moved JointProgressDriver itself is already included once
            // by its original metadata_backing_bytes author.
        ],
    )
}

fn facts(
    ingress: FrontendStatementIngress,
    protected_payload: u64,
    native_owned: u64,
    metadata_source_bytes: u64,
    k: usize,
    writers: usize,
    q: usize,
    c: usize,
) -> Result<FrontendCompositionFacts, WorkError> {
    let qa =
        novarocks_query_application::query_control::QueryApplicationControl::session_geometry();
    let statement=novarocks_query_application::session_control::QueryControlService::statement_action_geometry();
    let explain = crate::query::explain_statement_geometry();
    Ok(FrontendCompositionFacts {
        maximum_dependencies: k,
        maximum_writer_occurrences: writers,
        maximum_protected_payload_increment: protected_payload,
        maximum_native_owned_increment: native_owned,
        metadata_source_bytes,
        proposals_per_slot: q,
        changes_per_proposal: c,
        session_records: ingress.sessions()?,
        session_record_inline_bytes: qa.record.size(),
        statement_source_bytes: [
            statement.registry_record.size(),
            statement.source.size(),
            statement.wake.size(),
            statement.observer.size(),
            statement.receipt.size(),
            statement.failure.size(),
            statement.failure.size(),
        ]
        .into_iter()
        .try_fold(0, plus)?,
        explain_peak_inline_bytes: plus(
            plus(explain.action.size(), explain.receiver.size())?,
            explain.preparation.size().max(explain.diagnostics.size()),
        )?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy() -> FrontendWorkloadCapacityConfig {
        FrontendWorkloadCapacityConfig::try_new(
            [16; 5],
            FrontendDependencyCustodyLimits {
                unreceived_productions: 256,
                publications: 256,
                residuals: 256,
                retained_payload_bytes: 4 << 30,
            },
            NonZeroUsize::new(7).unwrap(),
            Duration::from_millis(25),
        )
        .unwrap()
    }
    fn actual_cpu() -> InternalCpuActionQuote {
        FrontendResultCapacityProfile::try_new(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap()
            .internal_cpu_action_quote()
            .unwrap()
    }
    fn physical(cpu: InternalCpuActionQuote) -> PhysicalCaps {
        PhysicalCaps {
            internal: 3,
            blocking: 1,
            native: 128,
            native_queue_positions: 128,
            native_queue_bytes: 1 << 30,
            cpu_queue_bytes: 3 * cpu.physical_queue_bytes().get(),
            blocking_queue_bytes: 0,
            ordinary_items: 4096,
            additional_queue_positions: 0,
        }
    }
    #[tokio::test]
    async fn complete_small_inventory_selects_a_profile_its_original_sources_can_install() {
        use crate::query_execution::internal_result_cpu::InternalResultCpuOwner;
        use novarocks_query_application::cpu::QueryBlockingExecutorOwner;
        use novarocks_workload_control::WorkloadControl;

        // The same finite inventory as the empty-topology real-entry host:
        // Root + stage + three allowed attempts, each with one scan/context.
        let mut workload = original_owner_test_workload();
        let controls = RootStageSource::minimum_control_records()
            + crate::query_execution::native_execution_adapter::native_source_count(1, 1)
                .unwrap()
                .get()
                * (workload.restarts_per_work + 1);
        let policy = policy();
        assert_eq!(policy.slots_per_class(), [2 * controls; 5]);
        workload.scope_records_limit = policy.scope_limit();
        let blocking_config = QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(32).unwrap(),
        );
        let inputs = FrontendNativeCapacityInputs::try_new(policy, blocking_config).unwrap();
        let result = FrontendResultCapacityProfile::for_test();
        let transport = FrontendTaskTransportBudget::DEFAULT;
        let metadata =
            crate::query::read_action::empty_provider_preparation_geometry_for_test().unwrap();
        let one = NonZeroUsize::new(1).unwrap();
        let ingress = FrontendStatementIngress {
            ordinary_connections: one,
            control_connections: one,
            ordinary_command_bytes: one,
            control_command_bytes: one,
        };
        let cpu_quote = result.internal_cpu_action_quote().unwrap();
        let root_payload = result.internal_initial_payload_bytes();
        let physical = PhysicalCaps::from_original(&inputs, result, transport, &metadata).unwrap();
        // K=2 has a complete physical recipe, but its fixed class inventory
        // cannot protect the real Sources. It must be rejected during selection,
        // before a host allocates or registers any process source.
        let unsupported = FrontendWorkloadCapacityProfile::compose_for_fanout(
            &inputs,
            &workload,
            transport,
            ingress,
            cpu_quote,
            root_payload,
            physical,
            2,
            &metadata,
            0,
        );
        let profile = FrontendWorkloadCapacityProfile::try_for_native(
            inputs, &workload, result, transport, ingress, metadata, 0,
        )
        .unwrap();
        assert_eq!(profile.composition_facts().unwrap().maximum_dependencies, 1);
        assert_eq!(profile.policy().slots_per_class(), [16; 5]);

        let mut control = WorkloadControl::try_new_counted(workload.clone()).unwrap();
        let mut blocking = QueryBlockingExecutorOwner::try_new(blocking_config).unwrap();
        let mut cpu = InternalResultCpuOwner::try_new(result, &mut control.owner).unwrap();
        let installed = profile
            .capture_sources(&workload, result, &blocking.executor(), &cpu.runtime())
            .and_then(|sources| profile.install(&mut control.owner, &sources));
        let installation_ok = installed.is_ok();
        drop(installed);
        control.owner.close_admission();
        let control_exit = control.owner.shutdown();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let cpu_exit = cpu.shutdown_until(deadline).await;
        let blocking_exit = blocking.shutdown_until(deadline).await;
        // Observe every original local exit before test assertions can unwind.
        assert!(matches!(
            unsupported,
            Err(WorkError::InvalidConfig(
                "joint protected progress geometry"
            ))
        ));
        assert!(installation_ok);
        assert!(control_exit.is_ok());
        assert!(
            cpu_exit.is_ok(),
            "original CPU owner did not exit: {cpu_exit:?}"
        );
        assert!(
            blocking_exit.is_ok(),
            "original blocking owner did not exit: {blocking_exit:?}"
        );
    }

    #[test]
    fn actual_closed_cpu_three_positions_do_not_derive_k_from_one_blocking_worker() {
        let cpu = actual_cpu();
        let minimum = V::ZERO
            .with(R::InternalCpuJobs, 1)
            .with(R::PhysicalQueueRecords, 1)
            .with(R::PhysicalQueueBytes, cpu.physical_queue_bytes().get());
        let k =
            supported_fanout(&[V::ZERO, minimum, V::ZERO], cpu, physical(cpu), &policy()).unwrap();
        assert_eq!(k, 15);
        let floor = floor(minimum, k, cpu).unwrap();
        assert_eq!(floor.global.get(R::InternalCpuJobs), 1);
        assert_eq!(floor.global.get(R::PhysicalQueueRecords), 1);
        assert_eq!(physical(cpu).blocking, 1);
    }
    #[test]
    fn a_genuine_blocking_source_cannot_borrow_the_registered_cpu_qualification() {
        let cpu = actual_cpu();
        let genuine = V::ZERO.with(R::BlockingJobs, 1);
        assert!(matches!(
            supported_fanout(&[V::ZERO, genuine, V::ZERO], cpu, physical(cpu), &policy()),
            Err(WorkError::InvalidConfig(
                "frontend complete supported dependency progress"
            ))
        ));
    }
    #[test]
    fn mixed_queue_preserves_its_native_causal_increment() {
        let cpu = actual_cpu();
        let mixed = V::ZERO
            .with(R::InternalCpuJobs, 1)
            .with(R::PhysicalQueueRecords, 2)
            .with(R::PhysicalQueueBytes, cpu.physical_queue_bytes().get() + 11);
        let floor = floor(mixed, 6, cpu).unwrap();
        assert_eq!(floor.global.get(R::PhysicalQueueRecords), 8);
        assert_eq!(floor.per_dependency.get(R::PhysicalQueueRecords), 7);
        assert_eq!(
            floor.global.get(R::PhysicalQueueBytes),
            cpu.physical_queue_bytes().get() + 77
        );
    }
    #[test]
    fn cohort_support_comes_from_k_and_original_fixed_serialization() {
        let k = 7u64;
        let count =
            NonZeroUsize::new((policy().dependency_custody().unreceived_productions / k) as usize)
                .unwrap();
        let quote =
            crate::query_execution::write_session::WriteProductionQuote::for_composition(count)
                .unwrap();
        assert_eq!(count.get(), 36);
        assert_eq!(quote.metadata_bytes(), 76 * 36);
        assert_eq!(quote.occurrences(), 36);
        assert!(quote.required_progress().get(R::UnreceivedProductions) * k <= 256);
        assert!((quote.occurrences() + 1) * k > 256);
    }
    #[test]
    fn native_and_root_owned_support_share_the_same_payload_policy() {
        let transport = FrontendTaskTransportBudget::DEFAULT;
        let per_dependency = 4u64 << 30;
        let k = 3;
        let metadata_initial = 160 * 1024;
        let root_payload =
            FrontendResultCapacityProfile::for_test().internal_initial_payload_bytes();
        let owned =
            supported_owned_increment(transport, per_dependency, k, metadata_initial, root_payload)
                .unwrap();
        let native = NativeTaskOperationSink::minimum_progress_for_composition(
            transport.into_codec(),
            owned,
        )
        .unwrap();
        let supported = add(
            sum([native.custody(), native.foreground(), native.settlement()]).unwrap(),
            V::ZERO.with(R::PayloadBytes, root_payload + metadata_initial),
        )
        .unwrap();
        assert!(
            supported
                .get(R::PayloadBytes)
                .checked_mul(k as u64)
                .unwrap()
                <= per_dependency
        );
        assert!(
            owned
                >= NativeTaskOperationSink::minimum_owned_support_for_composition(
                    transport.into_codec()
                )
                .unwrap()
        );
        let encoder = u64::try_from(transport.max_batch_encoded_bytes())
            .unwrap()
            .checked_mul(2)
            .unwrap();
        assert_eq!(
            native.foreground().get(R::PayloadBytes) - encoder,
            owned.get()
        );
        assert_eq!(
            native.settlement().get(R::PayloadBytes) - encoder,
            owned.get()
        );
        assert!(
            supported_owned_increment(transport, 1, k, metadata_initial, root_payload).is_err()
        );
    }

    #[test]
    fn actual_internal_factory_invoice_is_required_before_selecting_full_d_support() {
        let result = FrontendResultCapacityProfile::for_test();
        let root_payload = result.internal_initial_payload_bytes();
        // This is the same unconditional construction guard used by the domain
        // owner, not a sample result or the configured 1024 MiB window maximum.
        assert_eq!(root_payload, 1008 * 1024 * 1024);
        assert!(root_payload < result.declared().all_objects_bytes[2]);
        let transport = FrontendTaskTransportBudget::DEFAULT;
        let least =
            NativeTaskOperationSink::minimum_owned_support_for_composition(transport.into_codec())
                .unwrap();
        let native = NativeTaskOperationSink::minimum_progress_for_composition(
            transport.into_codec(),
            least,
        )
        .unwrap();
        let initial = 160 * 1024;
        let metadata = ConnectorMetadataMaintenanceIoGeometry::new(
            NonZeroUsize::new(8).unwrap(),
            std::num::NonZeroU64::new(128).unwrap(),
            std::num::NonZeroU64::new(initial).unwrap(),
        );
        let (metadata_lanes, _) = metadata_progress(&metadata, 1).unwrap();
        let cpu = result.internal_cpu_action_quote().unwrap();
        let lanes = action_lanes(
            [native.custody(), native.foreground(), native.settlement()],
            cpu,
            metadata_lanes,
            root_payload,
        )
        .unwrap();
        assert_eq!(
            lanes[1].get(R::PayloadBytes),
            native.foreground().get(R::PayloadBytes)
        );
        assert_eq!(
            lanes[0].get(R::PayloadBytes),
            native.custody().get(R::PayloadBytes) + initial + root_payload
        );
        let mut actual_physical = physical(cpu);
        actual_physical.native_queue_bytes = 512 * 1024 * 1024;
        let k = supported_fanout(&lanes, cpu, actual_physical, &policy()).unwrap();
        assert_eq!(k, 3);
        assert!(supported_owned_increment(transport, 4 << 30, 3, initial, root_payload).is_ok());
        assert!(matches!(
            supported_owned_increment(transport, 4 << 30, 4, initial, root_payload),
            Err(WorkError::Capacity(
                "frontend complete owned source increments"
            ))
        ));
    }

    #[test]
    fn original_per_root_backing_prepays_the_actual_statement_family_before_grants() {
        let transport = FrontendTaskTransportBudget::DEFAULT;
        let inputs = FrontendNativeCapacityInputs::try_new(
            policy(),
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(2).unwrap(),
            ),
        )
        .unwrap();
        let geometry = ConnectorMetadataMaintenanceIoGeometry::new(
            NonZeroUsize::new(8).unwrap(),
            std::num::NonZeroU64::new(128).unwrap(),
            std::num::NonZeroU64::new(8 * 1024 * 1024 + 2 * 64 * 1024).unwrap(),
        );
        let one = NonZeroUsize::new(1).unwrap();
        let ingress = FrontendStatementIngress {
            ordinary_connections: one,
            control_connections: one,
            ordinary_command_bytes: one,
            control_command_bytes: one,
        };
        let source = process_source_backing(ingress, transport, &inputs, 3, &geometry, 0).unwrap();
        let role = LogicalExecutionSupervisor::logical_role_poll_geometry().unwrap();
        let paid = role.with_source_backing(source).unwrap();
        // Shared process allocations cancel. Adding ONE original root must
        // have already paid the full actual INSERT and metadata family peak.
        let per_root = (paid.bytes(2).unwrap() - paid.bytes(1).unwrap())
            - (role.bytes(2).unwrap() - role.bytes(1).unwrap());
        let history = novarocks_statistics_application::publication_failure_geometry().unwrap();
        assert!(per_root >= history.observation_backing.size());
        let shared = paid.bytes(1).unwrap() - role.bytes(1).unwrap() - per_root;
        assert!(shared >= history.backing.size());
        assert!(
            per_root as u64
                >= crate::query::insert_action::fixed_source_bytes(&geometry, 3).unwrap()
        );
        assert!(
            per_root as u64
                >= crate::query::metadata_statement_fixed_source_bytes(&geometry, 3).unwrap()
        );
        assert!(
            per_root as u64
                >= crate::statistics_jobs::attempt_executor::fixed_source_bytes(&geometry, 3)
                    .unwrap()
        );
        assert!(
            per_root as u64
                >= crate::statistics_jobs::service::submission_fixed_source_bytes(&geometry, 3)
                    .unwrap()
        );
    }

    #[test]
    fn metadata_factory_quote_pays_source_and_both_actual_io_lanes() {
        let geometry = ConnectorMetadataMaintenanceIoGeometry::new(
            NonZeroUsize::new(8).unwrap(),
            std::num::NonZeroU64::new(128).unwrap(),
            std::num::NonZeroU64::new(160 * 1024).unwrap(),
        );
        let (lanes, source) = metadata_progress(&geometry, 7).unwrap();
        assert!(source >= geometry.metadata_bytes().get());
        assert_eq!(lanes[0].get(R::ClaimRecords), 1);
        assert_eq!(
            lanes[0].get(R::PayloadBytes),
            geometry.initial_owned_bytes().get()
        );
        assert_eq!(
            lanes[0].get(R::MetadataBytes),
            geometry.metadata_bytes().get()
        );
        assert!(source > lanes[0].get(R::MetadataBytes));
        assert_eq!(
            source - lanes[0].get(R::MetadataBytes),
            MaintenanceCommandExecutor::frontend_fixed_source_bytes(&geometry, 7).unwrap()
        );
        for lane in [1, 2] {
            assert_eq!(lanes[lane].get(R::AsyncIo), 1);
            assert_eq!(lanes[lane].get(R::ClaimRecords), 1);
            assert!(lanes[lane].get(R::MetadataBytes) > 0);
            assert_eq!(lanes[lane].get(R::PhysicalQueueRecords), 0);
        }
    }
}
