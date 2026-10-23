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
    JointAdjustmentReceipt, JointAdmission, JointAdmissionFailure, JointAdmissionHandle,
    JointClaim, JointClaimChange, JointClaimView, JointDependency, JointGrantedBatch,
    JointGrantedClaims,
};

use crate::{WorkClass, WorkError};
use std::num::NonZeroUsize;

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
    /// Static physical reservations also constrain each class/lane intersection.
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
#[derive(Clone, Debug)]
pub struct ValidatedJointCapacityConfig {
    config: JointCapacityConfig,
}

impl JointCapacityConfig {
    pub fn validate(
        self,
        required: &RequiredJointProgress,
    ) -> Result<ValidatedJointCapacityConfig, WorkError> {
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
        self.validate_projection(self.global, required.global)?;
        for (limits, minimum) in self.classes.iter().zip(required.classes) {
            self.validate_projection(*limits, minimum)?;
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
            self.validate_projection(*limits, minimum)?;
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
                self.validate_projection(limits, required.class_lanes[class][lane])?;
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
                global = global.checked_add(self.class_lanes[class][lane].global)?;
                progress =
                    progress.checked_add(self.class_lanes[class][lane].protected_progress)?;
                dependency =
                    dependency.checked_add(self.class_lanes[class][lane].per_dependency)?;
            }
            if self.lanes[lane].protected_progress != progress
                || !self.lanes[lane].global.covers(global)
                || !self.lanes[lane].per_dependency.covers(dependency)
            {
                return Err(WorkError::InvalidConfig("joint lane class reservation"));
            }
        }
        Ok(ValidatedJointCapacityConfig { config: self })
    }

    fn validate_projection(
        &self,
        limits: JointResourceLimits,
        minimum: JointResources,
    ) -> Result<(), WorkError> {
        if !limits.protected_progress.covers(minimum)
            || !limits.global.covers(limits.per_dependency)
        {
            return Err(WorkError::InvalidConfig(
                "joint protected progress geometry",
            ));
        }
        let fanout = self.max_dependencies_per_claim as u128;
        for index in 0..RESOURCE_COUNT {
            let progress = u128::from(limits.protected_progress.0[index]);
            // Admit one complete supported increment even at the maximum
            // declared fanout, while retaining the protected intersections.
            let global = (fanout + 1)
                .checked_mul(progress)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let domain = fanout
                .checked_mul(progress)
                .ok_or(WorkError::ArithmeticOverflow)?;
            if global > u128::from(limits.global.0[index])
                || domain > u128::from(limits.per_dependency.0[index])
            {
                return Err(WorkError::InvalidConfig("joint dependency protection"));
            }
        }
        Ok(())
    }
}

impl ValidatedJointCapacityConfig {
    pub fn declared(&self) -> &JointCapacityConfig {
        &self.config
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
