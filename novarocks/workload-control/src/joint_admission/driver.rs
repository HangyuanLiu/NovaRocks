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
    ledger::{
        ProfileFacts, ProfileKey, ProfileRecord, RequestPhase, fail_request, grant_request,
        validate_waiting,
    },
    protection::{
        CheckError, CoordinateLimit, ExactProfile, Feasibility, ProofInput, ProtectionProof,
    },
    *,
};
use crate::{
    WorkId, WorkloadProgressRevision,
    control_storage::{ReapPreparation, SlotId, SlotPhase},
    scope::Inner,
};
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Included, Unbounded},
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
    scan_end: Option<u64>,
    scan_revision: Option<WorkloadProgressRevision>,
    local_cursor: Option<u64>,
    local_left: usize,
    local_end: Option<u64>,
    proposal_cursor: usize,
    proposal_left: usize,
    proposal_revision: Option<WorkloadProgressRevision>,
    proposal_idle: Option<WorkloadProgressRevision>,
    reclaim_cursor: usize,
    reclaim_left: usize,
    reclaim_revision: Option<WorkloadProgressRevision>,
    reclaim_idle: Option<WorkloadProgressRevision>,
    reclaim_parent: Option<SlotId>,
    last_reclaim_was_hint: bool,
    #[cfg(test)]
    pub(crate) reclaim_checks: usize,
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
    changes: super::ledger::proposal_storage::ChangeBody,
    release_epoch: u64,
    layout_revision: u64,
    cursor: Option<Arc<ProfileKey>>,
    records: Vec<ProfileRecord>,
    snapshot_capacity: usize,
    pending_domain_limits: Vec<u128>,
    domains: BTreeMap<u64, usize>,
    input: Option<ProofInput>,
    proof: Option<ProtectionProof>,
    next_change: usize,
    work: u64,
    work_limit: u64,
    _exit: crate::scope::ReclaimWakeOnExit,
}
// The return type ties this count to the actual closed projection author.
const PROFILE_PROJECTION_COUNT: usize = 4;
fn profile_projections(facts: &ProfileFacts) -> [usize; PROFILE_PROJECTION_COUNT] {
    [
        0,
        1 + class_index(facts.class),
        1 + CLASS_COUNT + facts.lane as usize,
        1 + CLASS_COUNT + LANE_COUNT + class_index(facts.class) * LANE_COUNT + facts.lane as usize,
    ]
}

pub(super) fn profile_checked_coordinates(amounts: &[u128]) -> Result<Vec<usize>, WorkError> {
    if amounts.len() != COORDINATE_COUNT {
        return Err(WorkError::Conflict);
    }
    let count = amounts.iter().filter(|amount| **amount != 0).count();
    if count > PROFILE_PROJECTION_COUNT * RESOURCE_COUNT {
        return Err(WorkError::Conflict);
    }
    let mut checked = Vec::new();
    checked
        .try_reserve_exact(count)
        .map_err(|_| WorkError::Capacity("joint checked coordinates"))?;
    for (index, amount) in amounts.iter().enumerate() {
        if *amount != 0 {
            checked.push(index);
        }
    }
    Ok(checked)
}

/// The one actual process driver pays old snapshots independently of role
/// recycling. Published facts and registry records are quoted by the ledger.
pub(super) fn metadata_backing_bytes(
    config: &JointCapacityConfig,
    requests: usize,
) -> Result<usize, WorkError> {
    use crate::control_storage::{array_bytes, btree_peak_bytes, sum_bytes};
    let n = config.max_claims;
    let k = config.max_dependencies_per_claim;
    let c = config.max_changes_per_request;
    let d = n
        .checked_mul(k)
        .ok_or(WorkError::ArithmeticOverflow)?
        .min(config.max_registered_dependencies);
    let build_d = n
        .checked_add(c)
        .and_then(|n| n.checked_mul(k))
        .ok_or(WorkError::ArithmeticOverflow)?
        .min(
            config
                .max_registered_dependencies
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?,
        );
    let per_profile = sum_bytes([
        array_bytes::<usize>(k)?,
        array_bytes::<u128>(COORDINATE_COUNT)?,
    ])?;
    let numeric = sum_bytes([
        array_bytes::<ExactProfile>(n)?,
        n.checked_mul(per_profile)
            .ok_or(WorkError::ArithmeticOverflow)?,
        array_bytes::<CoordinateLimit>(COORDINATE_COUNT)?,
        COORDINATE_COUNT
            .checked_mul(array_bytes::<u128>(d)?)
            .ok_or(WorkError::ArithmeticOverflow)?,
        // Replacing affected and checked-coordinate vectors retains old+new.
        array_bytes::<usize>(k)?
            .checked_mul(2)
            .ok_or(WorkError::ArithmeticOverflow)?,
        // Exact positive-count reserve; original and replacement coexist.
        array_bytes::<usize>(PROFILE_PROJECTION_COUNT * RESOURCE_COUNT)?
            .checked_mul(2)
            .ok_or(WorkError::ArithmeticOverflow)?,
    ])?;
    let graph = protection::graph_backing_bytes(n, k, config.max_residual_arcs)?;
    // Drop keeps a cancelled active graph while collecting original tickets
    // and outgoing wakers. Both authors reserve exact request count first.
    let shutdown = sum_bytes([
        array_bytes::<u64>(requests)?,
        array_bytes::<std::task::Waker>(requests)?,
    ])?;
    // Granted new_ids stay in their proposal's existing reply envelope.
    let commit = btree_peak_bytes::<Arc<ProfileKey>, ProfileRecord>(
        c.checked_mul(2).ok_or(WorkError::ArithmeticOverflow)?,
    )?;
    sum_bytes([
        // active: Option<Turn> already includes all Turn/ProofInput/graph headers.
        std::mem::size_of::<JointProgressDriver>(),
        array_bytes::<ProfileRecord>(n)?,
        btree_peak_bytes::<u64, usize>(build_d)?,
        numeric,
        graph
            .checked_add(shutdown)
            .ok_or(WorkError::ArithmeticOverflow)?
            .max(commit),
    ])
}

impl JointCapacityConfig {
    /// Fill the original snapshot/graph arenas from their actual private
    /// layout authors. This quotes storage, not feasibility or admission.
    /// The total-work ceiling remains an independent cumulative finite policy.
    pub fn fill_original_proof_geometry(&mut self) -> Result<(), WorkError> {
        let n = self.max_claims;
        let k = self.max_dependencies_per_claim;
        let d = n
            .checked_mul(k)
            .ok_or(WorkError::ArithmeticOverflow)?
            .min(self.max_registered_dependencies);
        let arcs = n
            .checked_mul(k.checked_add(1).ok_or(WorkError::ArithmeticOverflow)?)
            .and_then(|n| n.checked_add(k))
            .and_then(|n| n.checked_mul(2))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let per_profile = COORDINATE_COUNT
            .checked_mul(16)
            .and_then(|n| n.checked_add(std::mem::size_of::<ProfileRecord>().checked_mul(2)?))
            .and_then(|n| n.checked_add(k.checked_mul(32)?))
            .ok_or(WorkError::ArithmeticOverflow)?;
        self.max_snapshot_bytes = n
            .checked_mul(per_profile)
            .and_then(|n| n.checked_add(COORDINATE_COUNT.checked_mul(d.checked_mul(16)?)?))
            .and_then(|n| n.checked_add(d.checked_mul(128)?))
            .and_then(|n| n.checked_add(COORDINATE_COUNT.checked_mul(128)?))
            .ok_or(WorkError::ArithmeticOverflow)?;
        use crate::control_storage::{array_bytes, sum_bytes};
        let exact_snapshot = sum_bytes([
            std::mem::size_of::<ProofInput>(),
            array_bytes::<ExactProfile>(n)?,
            array_bytes::<CoordinateLimit>(COORDINATE_COUNT)?,
            array_bytes::<usize>(k)?,
            array_bytes::<usize>(PROFILE_PROJECTION_COUNT * RESOURCE_COUNT)?,
            n.checked_mul(sum_bytes([
                array_bytes::<usize>(k)?,
                array_bytes::<u128>(COORDINATE_COUNT)?,
            ])?)
            .ok_or(WorkError::ArithmeticOverflow)?,
            COORDINATE_COUNT
                .checked_mul(array_bytes::<u128>(d)?)
                .ok_or(WorkError::ArithmeticOverflow)?,
        ])?;
        self.max_snapshot_bytes = self.max_snapshot_bytes.max(exact_snapshot);
        self.max_residual_arcs = arcs;
        self.max_scratch_bytes = std::mem::size_of::<ProtectionProof>()
            .checked_add(protection::graph_backing_bytes(n, k, arcs)?)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(())
    }
}

impl JointProgressDriver {
    pub(super) fn new(inner: Arc<Inner>, config: Arc<ValidatedJointCapacityConfig>) -> Self {
        Self {
            inner,
            config,
            active: None,
            cursor: None,
            scan_left: 0,
            scan_end: None,
            scan_revision: None,
            local_cursor: None,
            local_left: 0,
            local_end: None,
            proposal_cursor: 0,
            proposal_left: 0,
            proposal_revision: None,
            proposal_idle: None,
            reclaim_cursor: 0,
            reclaim_left: 0,
            reclaim_revision: None,
            reclaim_idle: None,
            reclaim_parent: None,
            last_reclaim_was_hint: false,
            #[cfg(test)]
            reclaim_checks: 0,
        }
    }

    /// At most one snapshot record or one configured solver budget is processed
    /// per call. Never call repeatedly without yielding the role's executor.
    pub fn step(&mut self) -> JointProgress {
        let local_more = self.reobserve_local_one();
        let proposal_more = self.reclaim_proposal_one();
        let reclaim_more = self.reclaim_one();
        // One canonical zero row per fair turn. Final graph exit is published
        // by the original builder/Turn/reaper/State exit guards, never here.
        let dependency_more = {
            let mut state = self.inner.state.lock().unwrap();
            let reclaimed = state
                .joint
                .as_mut()
                .is_some_and(|ledger| ledger.reap_dependency());
            if reclaimed {
                state.advance_progress_revision();
            }
            reclaimed
        };
        self.step_after_reclaim(proposal_more, reclaim_more, dependency_more || local_more)
    }

    fn step_after_reclaim(
        &mut self,
        proposal_more: bool,
        reclaim_more: bool,
        dependency_more: bool,
    ) -> JointProgress {
        let proof = self.step_proof();
        if let JointProgress::Idle(marker) = proof {
            let state = self.inner.state.lock().unwrap();
            // A last alias may exit between the reaper's check and proof Idle.
            // Do not swallow that generation by returning the later marker.
            if proposal_more
                || reclaim_more
                || dependency_more
                || state
                    .joint
                    .as_ref()
                    .is_some_and(|ledger| ledger.has_reclaimable_dependencies())
                || (state.cancellations.retired_proposals != 0
                    && self.proposal_idle != Some(marker))
                || (state.cancellations.fixed
                    && (state.cancellations.retired != 0 || state.cancellations.retired_polls != 0)
                    && self.reclaim_idle != Some(marker))
            {
                return JointProgress::More;
            }
        }
        proof
    }

    /// One nested original backing cell per turn, with a finite unchanged pass.
    fn reclaim_proposal_one(&mut self) -> bool {
        use super::ledger::proposal_storage::{finish_reap_proposal, prepare_reap_proposal};
        let prepared = {
            let mut state = self.inner.state.lock().unwrap();
            let marker = self.inner.capture_progress(&state);
            if !state.cancellations.fixed || state.cancellations.retired_proposals == 0 {
                self.proposal_left = 0;
                self.proposal_idle = Some(marker);
                return false;
            }
            let per_slot = state.cancellations.slots[0].proposals.len();
            let count = state.cancellations.slots.len() * per_slot;
            if self.proposal_left == 0 {
                if self.proposal_idle == Some(marker) {
                    return false;
                }
                self.proposal_left = count;
                self.proposal_revision = Some(marker);
                self.proposal_idle = None;
            }
            let flat = self.proposal_cursor % count;
            self.proposal_cursor = (flat + 1) % count;
            self.proposal_left -= 1;
            let slot = SlotId(flat / per_slot);
            let index = flat % per_slot;
            prepare_reap_proposal(&mut state.cancellations, slot, index)
                .map(|(old, generation)| (slot, index, old, generation))
        };
        if let Some((slot, index, old, generation)) = prepared {
            // Destruction precedes Free publication, outside the original mutex.
            drop(old);
            {
                let mut state = self.inner.state.lock().unwrap();
                finish_reap_proposal(&mut state.cancellations, slot, index, generation);
                state.advance_progress_revision();
            }
            self.proposal_idle = None;
            self.inner.notify_reclaim_progress();
            return true;
        }
        let state = self.inner.state.lock().unwrap();
        let marker = self.inner.capture_progress(&state);
        if self.proposal_left != 0 || self.proposal_revision != Some(marker) {
            return true;
        }
        self.proposal_idle = Some(marker);
        false
    }

    /// The role service's prepaid timer requests a new finite pass after silent
    /// Arc/Waker exits and local physical refusals. This only arms finite
    /// original-inventory passes; it never resets a request's proof budget.
    /// The production timer/source profile is composed separately.
    pub fn recheck_retired_backing(&mut self) {
        self.reclaim_idle = None;
        self.reclaim_left = 0;
        if self.local_left == 0 {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().unwrap();
            self.local_left = if ledger.local_waiters == 0 {
                0
            } else {
                ledger.requests.len()
            };
            self.local_end = ledger.requests.last_key_value().map(|(&id, _)| id);
            self.local_cursor = None;
        }
    }

    /// One original request per timer-armed finite pass. New arrivals cannot
    /// extend it. No physical source, callback or new request is created here.
    fn reobserve_local_one(&mut self) -> bool {
        if self.local_left == 0 {
            return false;
        }
        let mut state = self.inner.state.lock().unwrap();
        let ledger = state.joint.as_mut().unwrap();
        let end = self.local_end.expect("nonempty original local pass");
        let next = ledger
            .requests
            .range((self.local_cursor.map_or(Unbounded, Excluded), Included(end)))
            .next()
            .map(|(&id, _)| id);
        let Some(id) = next else {
            self.local_left = 0;
            return false;
        };
        self.local_cursor = Some(id);
        self.local_left -= 1;
        let request = ledger.requests.get_mut(&id).unwrap();
        let unblocked = request.local_blocked && matches!(request.phase, RequestPhase::Waiting);
        if unblocked {
            request.local_blocked = false;
            ledger.local_waiters -= 1;
            state.advance_progress_revision();
        }
        unblocked || self.local_left != 0
    }

    /// One cell at most, alongside the original proof step. A successful leaf
    /// gives one parent hint, alternating with the finite fair storage scan.
    fn reclaim_one(&mut self) -> bool {
        let prepared = {
            let mut state = self.inner.state.lock().unwrap();
            let marker = self.inner.capture_progress(&state);
            if !state.cancellations.fixed
                || (state.cancellations.retired == 0 && state.cancellations.retired_polls == 0)
            {
                self.reclaim_left = 0;
                self.reclaim_parent = None;
                self.reclaim_idle = Some(marker);
                return false;
            }
            if self.reclaim_left == 0 {
                if self.reclaim_idle == Some(marker) {
                    return false;
                }
                self.reclaim_left = state.cancellations.slots.len();
                self.reclaim_revision = Some(marker);
                self.reclaim_idle = None;
            }
            let hint = if self.last_reclaim_was_hint {
                None
            } else {
                self.reclaim_parent.take()
            };
            let slot = match hint {
                Some(slot) => {
                    self.last_reclaim_was_hint = true;
                    slot
                }
                None => {
                    let count = state.cancellations.slots.len();
                    let slot = SlotId(self.reclaim_cursor % count);
                    self.reclaim_cursor = (self.reclaim_cursor + 1) % count;
                    self.reclaim_left -= 1;
                    self.last_reclaim_was_hint = false;
                    slot
                }
            };
            #[cfg(test)]
            {
                self.reclaim_checks += 1;
            }
            state.cancellations.prepare_reap(slot).map(|prepared| {
                if matches!(prepared, ReapPreparation::PollRole) {
                    state.advance_progress_revision();
                }
                (slot, prepared)
            })
        };
        if matches!(prepared, Some((_, ReapPreparation::PollRole))) {
            self.reclaim_idle = None;
            self.inner.notify_reclaim_progress();
            return true;
        }
        if let Some((
            id,
            ReapPreparation::Cancellation {
                reset,
                parent,
                generation,
            },
        )) = prepared
        {
            // Parent strong references and obsolete values actually leave here,
            // outside State. Only then may the exact original claim be rekeyed.
            drop(reset);
            {
                let mut state = self.inner.state.lock().unwrap();
                let slot = &state.cancellations.slots[id.0];
                assert_eq!(slot.phase, SlotPhase::Retired);
                assert_eq!(slot.generation, generation);
                let claim = slot.claim.unwrap();
                let base = slot.base;
                state
                    .joint
                    .as_mut()
                    .unwrap()
                    .reset_control_slot(id, claim, base);
                if !state.cancellations.finish_reaped(id) {
                    state.joint.as_mut().unwrap().driver_closed = true;
                }
                state.advance_progress_revision();
            }
            self.reclaim_parent = parent;
            self.reclaim_idle = None;
            self.inner.notify_reclaim_progress();
            return true;
        }
        let state = self.inner.state.lock().unwrap();
        let marker = self.inner.capture_progress(&state);
        if self.reclaim_left != 0 || self.reclaim_parent.is_some() {
            return true;
        }
        if self.reclaim_revision != Some(marker) {
            return true;
        }
        self.reclaim_idle = Some(marker);
        false
    }

    fn step_proof(&mut self) -> JointProgress {
        if self.active.is_none() {
            let mut local_waker = None;
            let (start, idle_revision) = {
                let mut guard = self.inner.state.lock().unwrap();
                let state = &mut *guard;
                let idle_revision = self.inner.capture_progress(state);
                let start = (|| {
                    let ledger = state.joint.as_mut().expect("installed joint driver");
                    if self.scan_left == 0 {
                        // New requests join the next finite pass. Releases can
                        // invalidate a negative certificate, but cannot extend
                        // this pass or let new IDs perpetually prevent wrap.
                        self.scan_left = ledger.requests.len();
                        self.scan_end = ledger.requests.last_key_value().map(|(&id, _)| id);
                        self.scan_revision = Some(idle_revision);
                    }
                    if self.scan_left == 0 {
                        return None;
                    }
                    let end = self.scan_end.expect("nonempty finite request pass");
                    let start = self.cursor.filter(|cursor| *cursor <= end);
                    let next = ledger
                        .requests
                        .range((start.map_or(Unbounded, Excluded), Included(end)))
                        .next()
                        .or_else(|| ledger.requests.range(..=end).next());
                    let Some((&id, request)) = next else {
                        // Every member of this pass may have exited. A newer
                        // ticket already present requires another turn, not an
                        // Idle verdict whose revision would swallow its wake.
                        self.scan_left = 0;
                        if ledger.requests.is_empty() {
                            return None;
                        }
                        return Some(Err(true));
                    };
                    self.cursor = Some(id);
                    self.scan_left -= 1;
                    let cancelled = ledger.driver_closed
                        || request
                            .changes
                            .iter()
                            .any(|change| change.claim.facts.lane != JointLane::Settlement)
                            && state
                                .nodes
                                .get(&request.owner)
                                .is_none_or(|node| node.check(&state.cancellations).is_err());
                    if matches!(request.phase, RequestPhase::LocalOffered { .. }) {
                        if cancelled {
                            let error = if ledger.driver_closed {
                                WorkError::Closed
                            } else {
                                state
                                    .nodes
                                    .get(&request.owner)
                                    .ok_or(WorkError::Released)
                                    .and_then(|node| node.check(&state.cancellations))
                                    .unwrap_err()
                            };
                            local_waker = fail_request(state, id, error);
                        }
                        return Some(Err(false));
                    }
                    if matches!(request.phase, RequestPhase::Failed(_))
                        || (!matches!(request.phase, RequestPhase::Waiting) && !cancelled)
                        || ((request.local_blocked
                            || request.blocked_at == Some(ledger.release_epoch))
                            && !cancelled)
                    {
                        return Some(Err(false));
                    }
                    ledger.active = Some(id);
                    Some(Ok(Turn {
                        preparation: Preparation::Allocate,
                        id,
                        owner: request.owner,
                        changes: request.changes.clone(),
                        release_epoch: ledger.release_epoch,
                        layout_revision: ledger.profile_layout_revision,
                        cursor: None,
                        records: Vec::new(),
                        snapshot_capacity: ledger.profiles.len(),
                        pending_domain_limits: Vec::new(),
                        domains: BTreeMap::new(),
                        input: None,
                        proof: None,
                        next_change: 0,
                        work: u64::from(request.budget.is_finite()),
                        _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(
                            &self.inner,
                        )),
                        work_limit: request
                            .budget
                            .limit(self.config.config.max_total_proof_work),
                    }))
                })();
                (start, idle_revision)
            };
            if let Some(waker) = local_waker {
                waker.wake();
            }
            match start {
                Some(Ok(turn)) => {
                    self.active = Some(turn);
                    return JointProgress::More;
                }
                Some(Err(new_pass))
                    if new_pass
                        || self.scan_left != 0
                        || self.scan_revision != Some(idle_revision) =>
                {
                    return JointProgress::More;
                }
                // Idle certifies one unchanged finite pass. A newer request,
                // release or cancellation may invalidate an earlier skip even
                // when the last examined ticket itself remains blocked.
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
                    .any(|change| change.claim.facts.lane != JointLane::Settlement)
                {
                    state
                        .nodes
                        .get(&turn.owner)
                        .ok_or(WorkError::Released)?
                        .check(&state.cancellations)?;
                }
                validate_waiting(&state, turn.id, &turn.changes)?;
                Ok(ledger.profile_layout_revision == turn.layout_revision)
            })()
        };
        match valid {
            Err(error) => {
                self.finish(turn, Err(error), false);
                return JointProgress::More;
            }
            Ok(false) => {
                self.reobserve(turn);
                return JointProgress::More;
            }
            Ok(true) => {}
        }
        if turn.proof.is_none() {
            match turn.work.checked_add(1) {
                Some(work) if work < turn.work_limit => turn.work = work,
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
                    .profiles
                    .range::<Arc<ProfileKey>, _>((
                        turn.cursor.as_ref().map_or(Unbounded, Excluded),
                        Unbounded,
                    ))
                    .next()
                    .map(|(key, record)| (Arc::clone(key), record.clone()))
            };
            if let Some((key, mut record)) = next {
                turn.cursor = Some(Arc::clone(&key));
                // Authenticate and subtract every old charge from its exact
                // aggregate once. Other owners' concurrent releases remain a
                // conservative overestimate, never a copied grant to restore.
                for change in turn.changes.iter() {
                    if change
                        .original_facts
                        .as_ref()
                        .is_some_and(|facts| facts.key == key)
                    {
                        record.amounts = match record
                            .amounts
                            .checked_sub(change.original_amounts.expect("original charge"))
                        {
                            Ok(amounts) => amounts,
                            Err(error) => {
                                self.finish(turn, Err(error), false);
                                return JointProgress::More;
                            }
                        };
                        record.claims -= 1;
                    }
                }
                if record.claims != 0 {
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
            if turn.work >= turn.work_limit {
                self.finish(
                    turn,
                    Err(WorkError::Capacity("joint total proof work")),
                    false,
                );
                return JointProgress::More;
            }
            let claim = &turn.changes[turn.next_change].claim;
            let profile = turn.profile(&claim.facts, claim.amounts);
            let checked = match profile_checked_coordinates(&profile.amounts) {
                Ok(checked) => checked,
                Err(error) => {
                    self.finish(turn, Err(error), false);
                    return JointProgress::More;
                }
            };
            let input = turn.input.as_mut().unwrap();
            input.affected_dependencies = profile.dependencies.clone();
            input.affected_causal = profile.causal;
            input.checked_coordinates = checked;
            input.profiles.push(profile);
            turn.next_change += 1;
            let mut bounds = self.config.solver_bounds();
            bounds.max_total_work = turn.work_limit - turn.work;
            turn.proof = Some(ProtectionProof::new(turn.input.take().unwrap(), bounds));
            self.active = Some(turn);
            return JointProgress::More;
        }
        self.finish(turn, Ok(()), false);
        JointProgress::More
    }

    #[cfg(test)]
    pub(super) fn reobserve_active_for_test(&mut self) {
        let turn = self.active.take().expect("selected original request");
        self.reobserve(turn);
    }

    fn reobserve(&mut self, turn: Turn) {
        let work = turn.actual_work();
        let waker = self.inner.update(|state| {
            let ledger = state.joint.as_mut().unwrap();
            if ledger.active == Some(turn.id) {
                ledger.active = None;
            }
            let Some(request) = ledger.requests.get(&turn.id) else {
                return None;
            };
            let waiting = matches!(request.phase, RequestPhase::Waiting);
            if let Err(error) = validate_turn_stop(state, &turn, waiting) {
                return fail_request(state, turn.id, error);
            }
            if !waiting {
                return None;
            }
            let request = state
                .joint
                .as_mut()
                .unwrap()
                .requests
                .get_mut(&turn.id)
                .unwrap();
            if let Err(error) = work.and_then(|work| request.budget.charge(work)) {
                return fail_request(state, turn.id, error);
            }
            if request.budget.exhausted() {
                return fail_request(
                    state,
                    turn.id,
                    WorkError::Capacity("joint finite proof work"),
                );
            }
            request.blocked_at = None;
            None
        });
        if let Some(waker) = waker {
            waker.wake();
        }
        // Keep the original cursor so other queued work gets its next fair turn.
    }

    fn finish(&mut self, turn: Turn, verdict: Result<(), WorkError>, infeasible: bool) {
        let work = turn.actual_work();
        let waker = self.inner.update(|state| {
            let ledger = state.joint.as_mut().unwrap();
            if ledger.active == Some(turn.id) {
                ledger.active = None;
            }
            let Some(request) = ledger.requests.get(&turn.id) else {
                return None;
            };
            let waiting = matches!(request.phase, RequestPhase::Waiting);
            let stopped = validate_turn_stop(state, &turn, waiting);
            if let Err(error) = stopped {
                return fail_request(state, turn.id, error);
            }
            if !waiting {
                return None;
            }
            let ledger = state.joint.as_mut().unwrap();
            let request = ledger.requests.get_mut(&turn.id).unwrap();
            if let Err(error) = work.and_then(|work| request.budget.charge(work)) {
                return fail_request(state, turn.id, error);
            }
            let finite = request.budget.is_finite();
            let exhausted = request.budget.exhausted();
            // Amount-only frees make a positive proof conservative. Rekeying a
            // live charge can move it behind the snapshot cursor, so layout
            // changes invalidate every verdict and rotate this request fairly.
            if ledger.profile_layout_revision != turn.layout_revision {
                if exhausted {
                    return fail_request(
                        state,
                        turn.id,
                        WorkError::Capacity("joint finite proof work"),
                    );
                }
                ledger.requests.get_mut(&turn.id).unwrap().blocked_at = None;
                return None;
            }
            if infeasible {
                if finite {
                    return fail_request(
                        state,
                        turn.id,
                        WorkError::Capacity("joint protected intersections"),
                    );
                }
                let epoch = ledger.release_epoch;
                ledger.requests.get_mut(&turn.id).unwrap().blocked_at =
                    (turn.release_epoch == epoch).then_some(epoch);
                return None;
            }
            match verdict.and_then(|()| grant_request(state, turn.id)) {
                Ok(()) => state
                    .joint
                    .as_mut()
                    .unwrap()
                    .requests
                    .get_mut(&turn.id)
                    .unwrap()
                    .waker
                    .take(),
                Err(error) => fail_request(state, turn.id, error),
            }
        });
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Recheck at the transaction that records a verdict. The earlier unlocked
/// validity observation does not author cancellation, closure or source facts.
fn validate_turn_stop(
    state: &crate::scope::State,
    turn: &Turn,
    waiting: bool,
) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().unwrap();
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    if turn
        .changes
        .iter()
        .any(|change| change.claim.facts.lane != JointLane::Settlement)
    {
        state
            .nodes
            .get(&turn.owner)
            .ok_or(WorkError::Released)?
            .check(&state.cancellations)?;
    }
    if waiting {
        validate_waiting(state, turn.id, &turn.changes)?;
    }
    Ok(())
}

impl Turn {
    fn actual_work(&self) -> Result<u64, WorkError> {
        self.work
            .checked_add(self.proof.as_ref().map_or(0, ProtectionProof::work_used))
            .ok_or(WorkError::ArithmeticOverflow)
    }

    fn push_record(
        &mut self,
        record: ProfileRecord,
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
                std::mem::size_of::<ProfileRecord>()
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
                let per_record = std::mem::size_of::<ProfileRecord>();
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
                    Some(&self.records[index].facts)
                } else {
                    self.changes
                        .get(index - self.records.len())
                        .map(|change| &change.claim.facts)
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
                        .and_then(|n| n.checked_add(std::mem::size_of::<ProfileRecord>() * 2))
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
                        affected_causal: super::CausalMask::NONE,
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
                    let profile = self.profile(&record.facts, record.amounts);
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
    fn profile(&self, facts: &ProfileFacts, values: JointResources) -> ExactProfile {
        let mut amounts = vec![0; COORDINATE_COUNT];
        for projection in profile_projections(facts) {
            for index in 0..RESOURCE_COUNT {
                amounts[projection * RESOURCE_COUNT + index] = u128::from(values.0[index]);
            }
        }
        ExactProfile {
            causal: facts.key.causal,
            dependencies: facts
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
            let count = ledger.requests.len();
            let mut ids = Vec::with_capacity(count);
            ids.extend(ledger.requests.keys().copied());
            let mut wakers = Vec::with_capacity(count);
            for id in ids {
                if let Some(waker) = fail_request(state, id, WorkError::Closed) {
                    wakers.push(waker);
                }
            }
            wakers
        });
        for waker in wakers {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod dependency_exit_tests {
    use super::*;
    use crate::joint_admission::ledger::JointAdmissionHandle;
    use novarocks_resource_contract::SharedDependency;
    use std::{
        future::Future,
        task::{Context, Waker},
    };

    fn fixture() -> (
        crate::WorkloadControl,
        JointAdmissionHandle,
        JointProgressDriver,
    ) {
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1)
    }

    #[test]
    fn last_external_dependency_exit_wakes_the_original_idle_driver() {
        let (control, handle, mut driver) = fixture();
        let reference = handle
            .register_dependency(SharedDependency::LocalHost)
            .unwrap();
        let JointProgress::Idle(marker) = driver.step() else {
            panic!("idle live identity")
        };
        let mut waiter = Box::pin(control.wait_progress(marker));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(reference);
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready(),
            "actual final graph exit must wake the same original progress wait"
        );
        assert!(matches!(driver.step(), JointProgress::More));
    }

    #[test]
    fn zero_published_between_reclaim_and_proof_idle_is_not_swallowed() {
        let (control, handle, mut driver) = fixture();
        let reference = handle
            .register_dependency(SharedDependency::LocalHost)
            .unwrap();
        assert!(matches!(driver.step(), JointProgress::Idle(_)));
        // The first half saw no zero. Last exit now happens before the proof
        // half captures its newer idle revision, exactly the lost-wake window.
        drop(reference);
        control.inner.notify_reclaim_progress();
        assert!(
            matches!(
                driver.step_after_reclaim(false, false, false),
                JointProgress::More
            ),
            "Idle must not capture a later wake while a zero row is queued"
        );
    }
}
