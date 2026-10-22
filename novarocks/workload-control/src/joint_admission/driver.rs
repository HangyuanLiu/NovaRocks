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

//! A unique, caller-independent, finite-turn proof driver. The role owner
//! schedules turns and yields between them; this module spawns no task.

use super::{
    ledger::{ChangeRecord, ClaimRecord, release_claim},
    protection::{
        CheckError, CoordinateLimit, ExactProfile, Feasibility, ProofInput, ProtectionProof,
    },
    *,
};
use crate::{WorkId, WorkloadProgressRevision, scope::Inner};
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
    task::Poll,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JointProgress {
    /// The original revision observed under the same lock as the idle verdict.
    Idle(WorkloadProgressRevision),
    More,
}

/// Unique progression capability. Proof state lives here, outside the WC mutex;
/// only its checked identity/fence and the original records live in WC State.
pub struct JointProgressDriver {
    inner: Arc<Inner>,
    config: Arc<ValidatedJointCapacityConfig>,
    active: Option<Turn>,
    cursor: Option<u64>,
    scan_left: usize,
    observed_release: u64,
}
#[derive(Clone, Copy)]
enum Preparation {
    Allocate,
    Snapshot,
    Domains(usize, usize),
    NumberDomains(Option<u64>, usize),
    Limits(usize),
    LimitDomains(usize),
    Profiles(usize),
    Ready,
}
struct Turn {
    preparation: Preparation,
    id: u64,
    owner: WorkId,
    changes: Arc<[ChangeRecord]>,
    release_epoch: u64,
    cursor: Option<u64>,
    records: Vec<ClaimRecord>,
    snapshot_capacity: usize,
    pending_domain_limits: Vec<u128>,
    domains: BTreeMap<u64, usize>,
    input: Option<ProofInput>,
    proof: Option<ProtectionProof>,
    next_change: usize,
    work: u64,
}
impl JointProgressDriver {
    pub(super) fn new(inner: Arc<Inner>, config: Arc<ValidatedJointCapacityConfig>) -> Self {
        Self {
            inner,
            config,
            active: None,
            cursor: None,
            scan_left: 0,
            observed_release: 0,
        }
    }

    /// At most one snapshot record or one configured solver budget is processed
    /// per call. Never call repeatedly without yielding the role's executor.
    pub fn step(&mut self) -> JointProgress {
        if self.active.is_none() {
            let (start, idle_revision) = {
                let mut guard = self.inner.state.lock().unwrap();
                let state = &mut *guard;
                let idle_revision = state.progress_marker();
                let start = (|| {
                    let ledger = state.joint.as_mut().expect("installed joint driver");
                    if ledger.driver_closed {
                        return None;
                    }
                    if self.observed_release != ledger.release_epoch || self.scan_left == 0 {
                        self.observed_release = ledger.release_epoch;
                        self.scan_left = ledger.requests.len();
                    }
                    if self.scan_left == 0 {
                        return None;
                    }
                    let next = ledger
                        .requests
                        .range((self.cursor.map_or(Unbounded, Excluded), Unbounded))
                        .next()
                        .or_else(|| ledger.requests.first_key_value());
                    let (&id, request) = next?;
                    self.cursor = Some(id);
                    self.scan_left -= 1;
                    let cancelled = request
                        .changes
                        .iter()
                        .any(|change| change.claim.lane != JointLane::Settlement)
                        && state
                            .nodes
                            .get(&request.owner)
                            .is_none_or(|node| node.check().is_err());
                    if request.result.is_some()
                        || (request.blocked_at == Some(ledger.release_epoch) && !cancelled)
                    {
                        return Some(Err(()));
                    }
                    ledger.active = Some(id);
                    Some(Ok(Turn {
                        preparation: Preparation::Allocate,
                        id,
                        owner: request.owner,
                        changes: Arc::clone(&request.changes),
                        release_epoch: ledger.release_epoch,
                        cursor: None,
                        records: Vec::new(),
                        snapshot_capacity: ledger.claims.len(),
                        pending_domain_limits: Vec::new(),
                        domains: BTreeMap::new(),
                        input: None,
                        proof: None,
                        next_change: 0,
                        work: 0,
                    }))
                })();
                (start, idle_revision)
            };
            match start {
                Some(Ok(turn)) => {
                    self.active = Some(turn);
                    return JointProgress::More;
                }
                Some(Err(())) if self.scan_left != 0 => return JointProgress::More,
                _ => return JointProgress::Idle(idle_revision),
            }
        }
        let mut turn = self.active.take().unwrap();
        let valid = {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().unwrap();
            (|| {
                if ledger.driver_closed {
                    return Err(WorkError::Closed);
                }
                if !ledger.requests.contains_key(&turn.id) {
                    return Err(WorkError::Released);
                }
                if turn
                    .changes
                    .iter()
                    .any(|change| change.claim.lane != JointLane::Settlement)
                {
                    state
                        .nodes
                        .get(&turn.owner)
                        .ok_or(WorkError::Released)?
                        .check()?;
                }
                if turn.changes.iter().any(|change| {
                    change
                        .old
                        .is_some_and(|id| !ledger.claims.contains_key(&id))
                }) {
                    return Err(WorkError::Released);
                }
                Ok(())
            })()
        };
        if let Err(error) = valid {
            self.finish(turn, Err(error), false);
            return JointProgress::More;
        }
        if turn.proof.is_none() {
            match turn.work.checked_add(1) {
                Some(work) if work < self.config.config.max_total_proof_work => turn.work = work,
                _ => {
                    self.finish(turn, Err(WorkError::Capacity("joint builder work")), false);
                    return JointProgress::More;
                }
            }
        }
        if matches!(turn.preparation, Preparation::Snapshot) {
            // Copy one fixed-size claim header and at most declared K Arc refs.
            // Positive claims are fenced; concurrent releases can only make this
            // retained observation a conservative upper bound.
            let next = {
                let state = self.inner.state.lock().unwrap();
                let ledger = state.joint.as_ref().unwrap();
                ledger
                    .claims
                    .range((turn.cursor.map_or(Unbounded, Excluded), Unbounded))
                    .next()
                    .map(|(&id, record)| (id, record.clone()))
            };
            if let Some((id, record)) = next {
                turn.cursor = Some(id);
                if !turn.changes.iter().any(|change| change.old == Some(id)) {
                    if let Err(error) = turn.push_record(record, &self.config.config) {
                        self.finish(turn, Err(error), false);
                        return JointProgress::More;
                    }
                }
                self.active = Some(turn);
                return JointProgress::More;
            }
            turn.preparation = Preparation::Domains(0, 0);
            self.active = Some(turn);
            return JointProgress::More;
        }
        if !matches!(turn.preparation, Preparation::Ready) {
            if let Err(error) = turn.prepare_one(&self.config.config) {
                self.finish(turn, Err(error), false);
                return JointProgress::More;
            }
            self.active = Some(turn);
            return JointProgress::More;
        }
        if let Some(proof) = &mut turn.proof {
            match proof.poll_steps(self.config.config.work_per_turn).result {
                Poll::Pending => {
                    self.active = Some(turn);
                    return JointProgress::More;
                }
                Poll::Ready(result) => {
                    let proof = turn.proof.take().unwrap();
                    turn.work = match turn.work.checked_add(proof.work_used()) {
                        Some(work) => work,
                        None => {
                            self.finish(turn, Err(WorkError::ArithmeticOverflow), false);
                            return JointProgress::More;
                        }
                    };
                    match result {
                        Ok(Feasibility::Feasible) => turn.input = Some(proof.into_input()),
                        Ok(Feasibility::Infeasible { .. }) => {
                            self.finish(
                                turn,
                                Err(WorkError::Capacity("joint protected intersections")),
                                true,
                            );
                            return JointProgress::More;
                        }
                        Err(error) => {
                            self.finish(turn, Err(proof_error(error)), false);
                            return JointProgress::More;
                        }
                    }
                }
            }
        }
        if turn.next_change < turn.changes.len() {
            if turn.work >= self.config.config.max_total_proof_work {
                self.finish(
                    turn,
                    Err(WorkError::Capacity("joint total proof work")),
                    false,
                );
                return JointProgress::More;
            }
            let claim = &turn.changes[turn.next_change].claim;
            let profile = turn.profile(claim);
            let input = turn.input.as_mut().unwrap();
            input.affected_dependencies = profile.dependencies.clone();
            input.checked_coordinates = profile
                .amounts
                .iter()
                .enumerate()
                .filter_map(|(index, amount)| (*amount != 0).then_some(index))
                .collect();
            input.profiles.push(profile);
            turn.next_change += 1;
            let mut bounds = self.config.solver_bounds();
            bounds.max_total_work -= turn.work;
            turn.proof = Some(ProtectionProof::new(turn.input.take().unwrap(), bounds));
            self.active = Some(turn);
            return JointProgress::More;
        }
        self.finish(turn, Ok(()), false);
        JointProgress::More
    }

    fn finish(&mut self, turn: Turn, verdict: Result<(), WorkError>, infeasible: bool) {
        let waker = self.inner.update(|state| {
            let ledger = state.joint.as_mut().unwrap();
            if ledger.active == Some(turn.id) {
                ledger.active = None;
            }
            let Some(request) = ledger.requests.get(&turn.id) else {
                return None;
            };
            if request.result.is_some() {
                return None;
            }
            let stopped = if turn
                .changes
                .iter()
                .any(|change| change.claim.lane != JointLane::Settlement)
            {
                state
                    .nodes
                    .get(&turn.owner)
                    .ok_or(WorkError::Released)
                    .and_then(|node| node.check())
            } else {
                Ok(())
            };
            let verdict = stopped.clone().and(verdict);
            if infeasible && stopped.is_ok() {
                // A release during observation invalidates a negative answer,
                // never a positive certificate. Retry fairly on the new epoch.
                let epoch = ledger.release_epoch;
                let request = ledger.requests.get_mut(&turn.id).unwrap();
                request.blocked_at = (turn.release_epoch == epoch).then_some(epoch);
                return None;
            }
            let verdict = verdict.and_then(|()| {
                let node = state.nodes.get(&turn.owner).ok_or(WorkError::Released)?;
                if turn
                    .changes
                    .iter()
                    .any(|change| change.claim.lane != JointLane::Settlement)
                {
                    node.check()?;
                }
                let ledger = state.joint.as_ref().unwrap();
                if ledger.driver_closed {
                    return Err(WorkError::Closed);
                }
                let old_count = turn
                    .changes
                    .iter()
                    .filter(|change| change.old.is_some())
                    .count();
                let final_count = ledger
                    .claims
                    .len()
                    .checked_sub(old_count)
                    .and_then(|count| count.checked_add(turn.changes.len()))
                    .ok_or(WorkError::ArithmeticOverflow)?;
                if final_count > self.config.config.max_claims {
                    return Err(WorkError::Capacity("joint claim records"));
                }
                for change in turn.changes.iter() {
                    if let Some(id) = change.old {
                        let old = ledger.claims.get(&id).ok_or(WorkError::Released)?;
                        // Amounts may only decrease during a proof. Replacing a
                        // partially released old claim must not subtract its old
                        // snapshot a second time from the real live ledger.
                        if Some(old.amounts) != change.original_amounts {
                            return Err(WorkError::Conflict);
                        }
                    }
                }
                state.nodes[&turn.owner]
                    .resource_holders
                    .checked_add(turn.changes.len())
                    .ok_or(WorkError::ArithmeticOverflow)?;
                state
                    .next_id
                    .checked_add(turn.changes.len() as u64)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                Ok(())
            });
            let result = verdict.map(|()| {
                // All checks precede the single affected-entry transaction. Do
                // not install the shadow: that would resurrect concurrent frees.
                for change in turn.changes.iter() {
                    if let Some(id) = change.old {
                        release_claim(state, id);
                    }
                }
                let mut ids = Vec::with_capacity(turn.changes.len());
                for change in turn.changes.iter() {
                    let id = state.next_id().expect("prechecked claim ids");
                    state
                        .joint
                        .as_mut()
                        .unwrap()
                        .claims
                        .insert(id, change.claim.clone());
                    state.nodes.get_mut(&turn.owner).unwrap().resource_holders += 1;
                    ids.push(id);
                }
                ids
            });
            let request = state
                .joint
                .as_mut()
                .unwrap()
                .requests
                .get_mut(&turn.id)
                .unwrap();
            request.result = Some(result);
            request.waker.take()
        });
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Turn {
    fn push_record(
        &mut self,
        record: ClaimRecord,
        config: &JointCapacityConfig,
    ) -> Result<(), WorkError> {
        if self.records.len() >= config.max_claims {
            return Err(WorkError::Capacity("joint snapshot records"));
        }
        // Include both the retained ledger headers and their numeric profiles.
        let count = self
            .records
            .len()
            .checked_add(self.changes.len())
            .and_then(|n| n.checked_add(1))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let bytes = count
            .checked_mul(
                std::mem::size_of::<ClaimRecord>()
                    + COORDINATE_COUNT * 16
                    + config.max_dependencies_per_claim * std::mem::size_of::<usize>(),
            )
            .ok_or(WorkError::ArithmeticOverflow)?;
        if bytes > config.max_snapshot_bytes {
            return Err(WorkError::Capacity("joint snapshot bytes"));
        }
        if self.records.len() == self.records.capacity() {
            return Err(WorkError::Capacity("joint snapshot arena"));
        }
        self.records.push(record);
        Ok(())
    }
    fn prepare_one(&mut self, config: &JointCapacityConfig) -> Result<(), WorkError> {
        match self.preparation {
            Preparation::Allocate => {
                let per_record = std::mem::size_of::<ClaimRecord>();
                if self.snapshot_capacity > config.max_claims
                    || self
                        .snapshot_capacity
                        .checked_mul(per_record)
                        .is_none_or(|bytes| bytes > config.max_snapshot_bytes)
                {
                    return Err(WorkError::Capacity("joint snapshot arena"));
                }
                self.records
                    .try_reserve_exact(self.snapshot_capacity)
                    .map_err(|_| WorkError::Capacity("joint snapshot allocation"))?;
                self.preparation = Preparation::Snapshot;
            }
            Preparation::Domains(index, dependency) => {
                let record = if index < self.records.len() {
                    Some(&self.records[index])
                } else {
                    self.changes
                        .get(index - self.records.len())
                        .map(|change| &change.claim)
                };
                if let Some(record) = record {
                    if let Some(domain) = record.dependencies.get(dependency) {
                        self.domains.insert(domain.ordinal, 0);
                        if self.domains.len() > config.max_registered_dependencies {
                            return Err(WorkError::Capacity("joint snapshot dependencies"));
                        }
                        self.preparation = Preparation::Domains(index, dependency + 1);
                    } else {
                        self.preparation = Preparation::Domains(index + 1, 0);
                    }
                } else {
                    self.preparation = Preparation::NumberDomains(None, 0);
                }
            }
            Preparation::NumberDomains(after, index) => {
                if let Some((&ordinal, _)) = self
                    .domains
                    .range((after.map_or(Unbounded, Excluded), Unbounded))
                    .next()
                {
                    *self.domains.get_mut(&ordinal).unwrap() = index;
                    self.preparation = Preparation::NumberDomains(Some(ordinal), index + 1);
                } else {
                    let retained = self
                        .records
                        .len()
                        .checked_add(self.changes.len())
                        .ok_or(WorkError::ArithmeticOverflow)?;
                    if retained > config.max_claims {
                        return Err(WorkError::Capacity("joint post-state claims"));
                    }
                    // One explicit bound covers headers, numeric profiles,
                    // domain arcs/maps, coordinate limits and temporary buffers.
                    let per_profile = COORDINATE_COUNT
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(std::mem::size_of::<ClaimRecord>() * 2))
                        .and_then(|n| {
                            n.checked_add(config.max_dependencies_per_claim.checked_mul(32)?)
                        })
                        .ok_or(WorkError::ArithmeticOverflow)?;
                    let bytes = retained
                        .checked_mul(per_profile)
                        .and_then(|n| {
                            n.checked_add(
                                COORDINATE_COUNT
                                    .checked_mul(self.domains.len().checked_mul(16)?)?,
                            )
                        })
                        .and_then(|n| n.checked_add(self.domains.len().checked_mul(128)?))
                        .and_then(|n| n.checked_add(COORDINATE_COUNT * 128))
                        .ok_or(WorkError::ArithmeticOverflow)?;
                    if bytes > config.max_snapshot_bytes {
                        return Err(WorkError::Capacity("joint snapshot bytes"));
                    }
                    let mut profiles = Vec::new();
                    profiles
                        .try_reserve_exact(retained)
                        .map_err(|_| WorkError::Capacity("joint profile allocation"))?;
                    let mut limits = Vec::new();
                    limits
                        .try_reserve_exact(COORDINATE_COUNT)
                        .map_err(|_| WorkError::Capacity("joint limit allocation"))?;
                    self.input = Some(ProofInput {
                        profiles,
                        affected_dependencies: Vec::new(),
                        checked_coordinates: Vec::new(),
                        limits,
                    });
                    self.preparation = Preparation::Limits(0);
                }
            }
            Preparation::Limits(coordinate) => {
                if coordinate == COORDINATE_COUNT {
                    self.preparation = Preparation::Profiles(0);
                } else {
                    self.pending_domain_limits
                        .try_reserve_exact(self.domains.len())
                        .map_err(|_| WorkError::Capacity("joint domain limit allocation"))?;
                    self.preparation = Preparation::LimitDomains(coordinate);
                }
            }
            Preparation::LimitDomains(coordinate) => {
                let projection = coordinate / RESOURCE_COUNT;
                let index = coordinate % RESOURCE_COUNT;
                let limits = if projection == 0 {
                    &config.global
                } else if projection <= CLASS_COUNT {
                    &config.classes[projection - 1]
                } else if projection <= CLASS_COUNT + LANE_COUNT {
                    &config.lanes[projection - 1 - CLASS_COUNT]
                } else {
                    let slot = projection - 1 - CLASS_COUNT - LANE_COUNT;
                    &config.class_lanes[slot / LANE_COUNT][slot % LANE_COUNT]
                };
                if self.pending_domain_limits.len() < self.domains.len() {
                    self.pending_domain_limits
                        .push(u128::from(limits.per_dependency.0[index]));
                } else {
                    self.input.as_mut().unwrap().limits.push(CoordinateLimit {
                        global: u128::from(limits.global.0[index]),
                        protected_increment: u128::from(limits.protected_progress.0[index]),
                        domains: std::mem::take(&mut self.pending_domain_limits),
                    });
                    self.preparation = Preparation::Limits(coordinate + 1);
                }
            }
            Preparation::Profiles(index) => {
                if let Some(record) = self.records.get(index) {
                    let profile = self.profile(record);
                    self.input.as_mut().unwrap().profiles.push(profile);
                    self.preparation = Preparation::Profiles(index + 1);
                } else {
                    self.preparation = Preparation::Ready;
                }
            }
            Preparation::Ready | Preparation::Snapshot => unreachable!("separate driver phase"),
        }
        Ok(())
    }
    fn profile(&self, record: &ClaimRecord) -> ExactProfile {
        let mut amounts = vec![0; COORDINATE_COUNT];
        for projection in [
            0,
            1 + class_index(record.class),
            1 + CLASS_COUNT + record.lane as usize,
            1 + CLASS_COUNT
                + LANE_COUNT
                + class_index(record.class) * LANE_COUNT
                + record.lane as usize,
        ] {
            for index in 0..RESOURCE_COUNT {
                amounts[projection * RESOURCE_COUNT + index] = u128::from(record.amounts.0[index]);
            }
        }
        ExactProfile {
            dependencies: record
                .dependencies
                .iter()
                .map(|domain| self.domains[&domain.ordinal])
                .collect(),
            amounts,
        }
    }
}
fn proof_error(error: CheckError) -> WorkError {
    match error {
        CheckError::ArithmeticOverflow => WorkError::ArithmeticOverflow,
        CheckError::MalformedSnapshot => WorkError::Conflict,
        CheckError::Cancelled => WorkError::Released,
        CheckError::ResourceLimit(_) => WorkError::Capacity("joint proof resource bound"),
        CheckError::ScratchAllocationFailed => {
            WorkError::Capacity("joint proof scratch allocation")
        }
    }
}
impl Drop for JointProgressDriver {
    fn drop(&mut self) {
        if let Some(turn) = &mut self.active {
            if let Some(proof) = &mut turn.proof {
                proof.cancel();
            }
        }
        let wakers = self.inner.update(|state| {
            let ledger = state.joint.as_mut().unwrap();
            ledger.driver_closed = true;
            ledger.active = None;
            let mut wakers = Vec::new();
            for request in ledger.requests.values_mut() {
                if request.result.is_none() {
                    request.result = Some(Err(WorkError::Closed));
                    if let Some(waker) = request.waker.take() {
                        wakers.push(waker);
                    }
                }
            }
            wakers
        });
        for waker in wakers {
            waker.wake();
        }
    }
}
