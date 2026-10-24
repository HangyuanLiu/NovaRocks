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

//! Joint resource admission under the original workload authority.
//!
//! Limits and their projections are declarations, not new grants. A resource
//! quantity has one global charge; class, lane and dependency coordinates are
//! constraints on that same quantity. Physical holders retain their original
//! capability until actual exit, independently of a logical terminal result.

mod driver;
pub(crate) mod ledger;
mod protection;

pub use driver::{JointProgress, JointProgressDriver};
pub use ledger::{
    AsyncActionAdmission, AsyncActionGrant, AsyncActionSource, BlockingActionAdmission,
    BlockingActionSource, ControlBindingProposal, InternalComputationAlias,
    InternalComputationGrant, InternalCpuActionAdmission, InternalCpuActionSource,
    InternalCpuFactoryBinding, InternalManifestBinding, InternalResultClosedFactoryDescriptor,
    JointAdjustmentReceipt, JointAdmission, JointAdmissionFailure, JointAdmissionHandle,
    JointClaim, JointClaimView, JointGrantedBatch, JointGrantedClaims, JointLocalGrant,
    JointLocalOffer, JointProposal, JointProposalSlotSignal, JointProposalSlotWait, JointTryBegin,
    NativeAsyncActionAdmission, NativeAsyncActionAuthority, NativeAsyncActionGrant,
    NativeAsyncActionQuote, NativeAsyncActionSource, NativeDispatchFrozen, NativeDispatchSource,
    NativeQueuedInputAdmission, NativeQueuedInputGrant, NativeQueuedInputQuote,
    NativeQueuedInputReady, NativeReplyGrant, RetainedPayloadAlias, RetainedPayloadGrant,
    RootDomainBinding, RootDomainInitialAdmission, RootDomainInitialProposal,
    RootDomainInitialSource, RootPayloadAdmission, RootPayloadInitialAdmission,
    RootPayloadInitialSource, RootPayloadProposal, RootPayloadReady, RootPayloadSource,
    RootStageAdmission, RootStageProposal, RootStageReady, RootStageSource, SourceCohort,
    UnreceivedProductionAdmission, UnreceivedProductionGrant, UnreceivedProductionOwner,
    UnreceivedProductionQuote, UnreceivedProductionReady,
};
#[cfg(test)]
pub use ledger::{JointClaimChange, JointDependency};

use crate::{WorkClass, WorkError};
use std::num::{NonZeroU64, NonZeroUsize};

/// Independently constrained resource coordinates. Byte values describe owned
/// backing or an explicit reserved envelope, never allocator metadata or RSS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum JointResource {
    ScopeRecords,
    ClaimRecords,
    ReplyRecords,
    QueueRecords,
    MetadataBytes,
    PayloadBytes,
    UnreceivedProductions,
    Publications,
    Residuals,
    Computations,
    AsyncIo,
    BlockingJobs,
    InternalCpuJobs,
    PhysicalQueueRecords,
    PhysicalQueueBytes,
}
const RESOURCE_COUNT: usize = 15;

// A closed source may exclude only these actual per-resource lifetimes from
// nonempty causal intersections. Charged dependencies and amounts never change.
// This private mask is not source certification or a public request parameter.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct CausalMask(u16);
impl CausalMask {
    const NONE: Self = Self(0);
    const INTERNAL_CPU_ACTION: Self = Self(
        (1u16 << JointResource::InternalCpuJobs as usize)
            | (1u16 << JointResource::PhysicalQueueRecords as usize)
            | (1u16 << JointResource::PhysicalQueueBytes as usize),
    );
    fn is_empty(self) -> bool {
        self.0 == 0
    }
    fn finite(self, coordinate: usize) -> bool {
        self.0 & (1u16 << (coordinate % RESOURCE_COUNT)) != 0
    }
    fn valid(self) -> bool {
        self.0 & !((1u16 << RESOURCE_COUNT) - 1) == 0
    }
}
const CLASS_COUNT: usize = 5;
const LANE_COUNT: usize = 3;
const PROJECTION_COUNT: usize = 1 + CLASS_COUNT + LANE_COUNT + CLASS_COUNT * LANE_COUNT;
const COORDINATE_COUNT: usize = RESOURCE_COUNT * PROJECTION_COUNT;

/// A request or a checked profile. Constructing a vector grants no capacity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JointResources([u64; RESOURCE_COUNT]);
impl JointResources {
    pub const ZERO: Self = Self([0; RESOURCE_COUNT]);

    pub const fn with(mut self, resource: JointResource, amount: u64) -> Self {
        self.0[resource as usize] = amount;
        self
    }

    pub const fn get(&self, resource: JointResource) -> u64 {
        self.0[resource as usize]
    }

    fn checked_add(self, other: Self) -> Result<Self, WorkError> {
        let mut result = Self::ZERO;
        for index in 0..RESOURCE_COUNT {
            result.0[index] = self.0[index]
                .checked_add(other.0[index])
                .ok_or(WorkError::ArithmeticOverflow)?;
        }
        Ok(result)
    }

    fn component_max(self, other: Self) -> Self {
        Self(std::array::from_fn(|index| {
            self.0[index].max(other.0[index])
        }))
    }

    fn checked_sub(self, other: Self) -> Result<Self, WorkError> {
        let mut result = Self::ZERO;
        for index in 0..RESOURCE_COUNT {
            result.0[index] = self.0[index]
                .checked_sub(other.0[index])
                .ok_or(WorkError::Conflict)?;
        }
        Ok(result)
    }

    fn covers(self, required: Self) -> bool {
        self.0
            .iter()
            .zip(required.0)
            .all(|(have, need)| *have >= need)
    }
}

/// Custody remains separately bounded while foreground and settlement actions
/// obtain their stage resources. A lane never changes an originating WorkClass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum JointLane {
    Custody,
    Foreground,
    Settlement,
}

/// Projection of the original charged quantity; this is diagnostic data only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JointCapacityProjection {
    Global,
    Class(WorkClass),
    Lane(JointLane),
    ClassLane { class: WorkClass, lane: JointLane },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JointCapacityConstraint {
    Global,
    Dependency,
}

/// Original infeasible coordinate. No payload, endpoint, identity, allocation,
/// or new proof traversal is needed to retain this bounded failure cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JointCapacityFailure {
    pub resource: JointResource,
    pub projection: JointCapacityProjection,
    pub constraint: JointCapacityConstraint,
}
impl JointCapacityFailure {
    fn from_coordinate(coordinate: usize, constraint: protection::ConstraintKind) -> Self {
        const RESOURCES: [JointResource; RESOURCE_COUNT] = [
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
        const CLASSES: [WorkClass; CLASS_COUNT] = [
            WorkClass::Query,
            WorkClass::MaterializedView,
            WorkClass::Statistics,
            WorkClass::TableMaintenance,
            WorkClass::Management,
        ];
        const LANES: [JointLane; LANE_COUNT] = [
            JointLane::Custody,
            JointLane::Foreground,
            JointLane::Settlement,
        ];
        assert!(coordinate < COORDINATE_COUNT, "validated proof coordinate");
        let projection = coordinate / RESOURCE_COUNT;
        let projection = if projection == 0 {
            JointCapacityProjection::Global
        } else if projection <= CLASS_COUNT {
            JointCapacityProjection::Class(CLASSES[projection - 1])
        } else if projection <= CLASS_COUNT + LANE_COUNT {
            JointCapacityProjection::Lane(LANES[projection - 1 - CLASS_COUNT])
        } else {
            let index = projection - 1 - CLASS_COUNT - LANE_COUNT;
            JointCapacityProjection::ClassLane {
                class: CLASSES[index / LANE_COUNT],
                lane: LANES[index % LANE_COUNT],
            }
        };
        Self {
            resource: RESOURCES[coordinate % RESOURCE_COUNT],
            projection,
            constraint: match constraint {
                protection::ConstraintKind::Global => JointCapacityConstraint::Global,
                protection::ConstraintKind::Domain => JointCapacityConstraint::Dependency,
            },
        }
    }
}

fn class_index(class: WorkClass) -> usize {
    match class {
        WorkClass::Query => 0,
        WorkClass::MaterializedView => 1,
        WorkClass::Statistics => 2,
        WorkClass::TableMaintenance => 3,
        WorkClass::Management => 4,
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JointResourceLimits {
    pub global: JointResources,
    pub per_dependency: JointResources,
    /// One complete protected progress increment, including necessary custody
    /// and reply storage. It is not one iteration or an arbitrary byte.
    pub protected_progress: JointResources,
}

/// Actual owners supply their complete supported progress geometries before
/// composition validates the deployment profile. There is no implicit geometry.
#[derive(Clone, Debug)]
pub struct RequiredJointProgress {
    pub global: JointResources,
    pub classes: [JointResources; CLASS_COUNT],
    pub lanes: [JointResources; LANE_COUNT],
    pub class_lanes: [[JointResources; LANE_COUNT]; CLASS_COUNT],
}

#[derive(Clone, Debug)]
pub struct JointCapacityConfig {
    pub global: JointResourceLimits,
    pub classes: [JointResourceLimits; CLASS_COUNT],
    pub lanes: [JointResourceLimits; LANE_COUNT],
    /// Class quotas share the original lane pool. They are simultaneous upper
    /// bounds, not independently reserved copies of that physical pool.
    pub class_lanes: [[JointResourceLimits; LANE_COUNT]; CLASS_COUNT],
    pub max_dependencies_per_claim: usize,
    pub max_registered_dependencies: usize,
    pub max_claims: usize,
    pub max_requests: usize,
    pub max_changes_per_request: usize,
    pub max_snapshot_bytes: usize,
    pub max_scratch_bytes: usize,
    pub max_residual_arcs: usize,
    pub max_total_proof_work: u64,
    pub work_per_turn: NonZeroUsize,
}

/// Validated once at composition and passed unchanged to the original ledger
/// and real execution owners. This is still a profile, never an admission.
#[derive(Debug)]
pub struct ValidatedJointCapacityConfig {
    config: JointCapacityConfig,
    internal_cpu_factory: Option<InternalResultClosedFactoryDescriptor>,
    source_authority: Option<std::sync::Weak<crate::scope::Inner>>,
    // Exact required jobs from the registered source, once per projection.
    finite_internal_cpu_jobs: [u64; PROJECTION_COUNT],
}

impl JointCapacityConfig {
    pub fn validate(
        self,
        required: &RequiredJointProgress,
    ) -> Result<ValidatedJointCapacityConfig, WorkError> {
        self.validate_inner(required, None, None)
    }

    fn validate_registered_internal_cpu(
        self,
        required: &RequiredJointProgress,
        factory: InternalResultClosedFactoryDescriptor,
        authority: std::sync::Weak<crate::scope::Inner>,
    ) -> Result<ValidatedJointCapacityConfig, WorkError> {
        self.validate_inner(required, Some(factory), Some(authority))
    }

    /// Check a proposed deployment's geometry using the original pure closed
    /// CPU factory's queue quote. This hypothetical contribution is not source
    /// authentication: no registered source, authority, readiness or installable
    /// validated capacity is returned. Installation must still validate against
    /// the same authority's actual registered CPU factory.
    pub fn check_composition_geometry(
        &self,
        required: &RequiredJointProgress,
        closed_cpu_queue_bytes: NonZeroU64,
    ) -> Result<(), WorkError> {
        self.check_geometry(required, Some(closed_cpu_queue_bytes))
    }

    fn validate_inner(
        self,
        required: &RequiredJointProgress,
        internal_cpu_factory: Option<InternalResultClosedFactoryDescriptor>,
        source_authority: Option<std::sync::Weak<crate::scope::Inner>>,
    ) -> Result<ValidatedJointCapacityConfig, WorkError> {
        self.check_geometry(
            required,
            internal_cpu_factory
                .as_ref()
                .map(|source| source.queue_bytes()),
        )?;
        let finite_internal_cpu_jobs = if internal_cpu_factory.is_some() {
            std::array::from_fn(|projection| {
                let minimum = if projection == 0 {
                    required.global
                } else if projection <= CLASS_COUNT {
                    required.classes[projection - 1]
                } else if projection <= CLASS_COUNT + LANE_COUNT {
                    required.lanes[projection - 1 - CLASS_COUNT]
                } else {
                    let slot = projection - 1 - CLASS_COUNT - LANE_COUNT;
                    required.class_lanes[slot / LANE_COUNT][slot % LANE_COUNT]
                };
                minimum.get(JointResource::InternalCpuJobs)
            })
        } else {
            [0; PROJECTION_COUNT]
        };
        Ok(ValidatedJointCapacityConfig {
            config: self,
            internal_cpu_factory,
            source_authority,
            finite_internal_cpu_jobs,
        })
    }

    fn check_geometry(
        &self,
        required: &RequiredJointProgress,
        finite_queue_bytes: Option<NonZeroU64>,
    ) -> Result<(), WorkError> {
        if self.max_dependencies_per_claim == 0
            || self.max_dependencies_per_claim > self.max_registered_dependencies
            || self.max_registered_dependencies > u32::MAX as usize
            || self.max_claims == 0
            || self.max_requests == 0
            || self.max_requests > self.max_claims
            || self.max_changes_per_request == 0
            || self.max_changes_per_request > self.max_claims
            || self.max_snapshot_bytes == 0
            || self.max_scratch_bytes == 0
            || self.max_residual_arcs == 0
            || self.max_total_proof_work == 0
            || self
                .max_claims
                .checked_mul(self.max_dependencies_per_claim)
                .is_none()
        {
            return Err(WorkError::InvalidConfig("joint admission finite geometry"));
        }
        self.validate_projection(self.global, required.global, finite_queue_bytes)?;
        for (limits, minimum) in self.classes.iter().zip(required.classes) {
            self.validate_projection(*limits, minimum, finite_queue_bytes)?;
            if !self.global.global.covers(limits.global)
                || !self
                    .global
                    .protected_progress
                    .covers(limits.protected_progress)
            {
                return Err(WorkError::InvalidConfig("joint class projection"));
            }
        }
        let mut simultaneous_lane_progress = JointResources::ZERO;
        let mut lane_capacities = JointResources::ZERO;
        let mut lane_dependencies = JointResources::ZERO;
        for (limits, minimum) in self.lanes.iter().zip(required.lanes) {
            self.validate_projection(*limits, minimum, finite_queue_bytes)?;
            if !self.global.global.covers(limits.global) {
                return Err(WorkError::InvalidConfig("joint lane projection"));
            }
            lane_capacities = lane_capacities.checked_add(limits.global)?;
            lane_dependencies = lane_dependencies.checked_add(limits.per_dependency)?;
            simultaneous_lane_progress =
                simultaneous_lane_progress.checked_add(limits.protected_progress)?;
        }
        if !self.global.global.covers(lane_capacities)
            || !self.global.per_dependency.covers(lane_dependencies)
        {
            return Err(WorkError::InvalidConfig("joint physical lane reservation"));
        }
        if self.global.protected_progress != simultaneous_lane_progress {
            return Err(WorkError::InvalidConfig("joint simultaneous lane progress"));
        }
        for class in 0..CLASS_COUNT {
            let mut global = JointResources::ZERO;
            let mut dependency = JointResources::ZERO;
            let mut progress = JointResources::ZERO;
            for lane in 0..LANE_COUNT {
                let limits = self.class_lanes[class][lane];
                self.validate_projection(
                    limits,
                    required.class_lanes[class][lane],
                    finite_queue_bytes,
                )?;
                global = global.checked_add(limits.global)?;
                dependency = dependency.checked_add(limits.per_dependency)?;
                progress = progress.checked_add(limits.protected_progress)?;
            }
            if self.classes[class].protected_progress != progress
                || !self.classes[class].global.covers(global)
                || !self.classes[class].per_dependency.covers(dependency)
            {
                return Err(WorkError::InvalidConfig("joint class lane reservation"));
            }
        }
        for lane in 0..LANE_COUNT {
            let mut global = JointResources::ZERO;
            let mut dependency = JointResources::ZERO;
            let mut progress = JointResources::ZERO;
            for class in 0..CLASS_COUNT {
                // Alternative classes share this actual lane. Reserve a
                // complete supported action from any class; a single action's
                // own concurrent jobs must already be in its full vector.
                global = global.component_max(self.class_lanes[class][lane].global);
                progress = progress.component_max(self.class_lanes[class][lane].protected_progress);
                dependency = dependency.component_max(self.class_lanes[class][lane].per_dependency);
            }
            if !self.lanes[lane].protected_progress.covers(progress)
                || !self.lanes[lane].global.covers(global)
                || !self.lanes[lane].per_dependency.covers(dependency)
            {
                return Err(WorkError::InvalidConfig("joint lane class reservation"));
            }
        }
        Ok(())
    }

    fn validate_projection(
        &self,
        limits: JointResourceLimits,
        minimum: JointResources,
        finite_queue_bytes: Option<NonZeroU64>,
    ) -> Result<(), WorkError> {
        if !limits.protected_progress.covers(minimum)
            || !limits.global.covers(limits.per_dependency)
        {
            return Err(WorkError::InvalidConfig(
                "joint protected progress geometry",
            ));
        }
        let floor = source_progress_floor(
            self.max_dependencies_per_claim,
            minimum,
            limits.protected_progress,
            finite_queue_bytes,
        )?;
        if !limits.global.covers(floor.global)
            || !limits.per_dependency.covers(floor.per_dependency)
        {
            return Err(WorkError::InvalidConfig("joint dependency protection"));
        }
        Ok(())
    }
}

// This is a floor fact only, never a capacity grant or a Native qualification.
fn source_progress_floor(
    fanout: usize,
    minimum: JointResources,
    protected: JointResources,
    finite_queue_bytes: Option<NonZeroU64>,
) -> Result<JointResourceLimits, WorkError> {
    if fanout == 0 || !protected.covers(minimum) {
        return Err(WorkError::InvalidConfig(
            "joint protected progress geometry",
        ));
    }
    let finite = source_finite_progress(
        minimum.get(JointResource::InternalCpuJobs),
        finite_queue_bytes,
    )?;
    if !minimum.covers(finite) {
        return Err(WorkError::InvalidConfig(
            "joint closed CPU progress source shape",
        ));
    }
    let fanout = fanout as u128;
    let mut global = JointResources::ZERO;
    let mut per_dependency = JointResources::ZERO;
    for index in 0..RESOURCE_COUNT {
        let finite = u128::from(finite.0[index]);
        let external = u128::from(protected.0[index]) - finite;
        let amount = |multiplier: u128| {
            external
                .checked_mul(multiplier)
                .and_then(|value| value.checked_add(finite))
                .and_then(|value| u64::try_from(value).ok())
                .ok_or(WorkError::InvalidConfig("joint dependency protection"))
        };
        global.0[index] = amount(fanout + 1)?;
        per_dependency.0[index] = amount(fanout)?;
    }
    Ok(JointResourceLimits {
        global,
        per_dependency,
        protected_progress: protected,
    })
}

// Keep composition floors and runtime protection on the same source facts.
fn source_finite_progress(
    jobs: u64,
    finite_queue_bytes: Option<NonZeroU64>,
) -> Result<JointResources, WorkError> {
    // Only the exact registered source's complete CPU contribution is finite.
    // Shared queue bytes belonging to other sources keep the original floor.
    let finite = if let Some(queue_bytes) = finite_queue_bytes {
        JointResources::ZERO
            .with(JointResource::InternalCpuJobs, jobs)
            .with(JointResource::PhysicalQueueRecords, jobs)
            .with(
                JointResource::PhysicalQueueBytes,
                jobs.checked_mul(queue_bytes.get())
                    .ok_or(WorkError::ArithmeticOverflow)?,
            )
    } else {
        JointResources::ZERO
    };
    Ok(finite)
}

impl Clone for ValidatedJointCapacityConfig {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            internal_cpu_factory: self
                .internal_cpu_factory
                .as_ref()
                .map(|source| source.duplicate()),
            source_authority: self.source_authority.clone(),
            finite_internal_cpu_jobs: self.finite_internal_cpu_jobs,
        }
    }
}

impl ValidatedJointCapacityConfig {
    pub fn declared(&self) -> &JointCapacityConfig {
        &self.config
    }

    fn finite_progress(&self, projection: usize, resource: usize) -> Result<u64, WorkError> {
        let Some(factory) = &self.internal_cpu_factory else {
            return Ok(0);
        };
        Ok(source_finite_progress(
            self.finite_internal_cpu_jobs[projection],
            Some(factory.queue_bytes()),
        )?
        .0[resource])
    }

    fn solver_bounds(&self) -> protection::SolverBounds {
        let config = &self.config;
        protection::SolverBounds {
            max_domains: config.max_registered_dependencies,
            max_profiles: config.max_claims,
            max_coordinates: COORDINATE_COUNT,
            max_profile_memberships: config
                .max_claims
                .checked_mul(config.max_dependencies_per_claim)
                .expect("validated membership geometry"),
            max_residual_arcs: config.max_residual_arcs,
            max_snapshot_bytes: config.max_snapshot_bytes,
            max_scratch_bytes: config.max_scratch_bytes,
            max_total_work: config.max_total_proof_work,
        }
    }
}

#[cfg(test)]
mod tests;
