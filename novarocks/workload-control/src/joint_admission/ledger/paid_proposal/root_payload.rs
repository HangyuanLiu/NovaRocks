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

//! Monotone retained Root backing and its in-flight async rows, one paid batch.
use super::super::typed_custody::{payload_growth_view, same_payload_grant};
use super::*;
use std::num::NonZeroU64;

#[derive(Clone, Copy)]
pub(in crate::joint_admission::ledger) struct RootPayloadFence {
    payload: u64,
    epoch: u64,
    physical: usize,
    lane: JointLane,
}
#[must_use = "Capture is not capacity; construct outside receiver locks"]
pub struct RootPayloadSource<'a> {
    inner: Weak<Inner>,
    root: WorkId,
    fence: RootPayloadFence,
    payload: JointClaimView,
    control: JointClaimView,
    physical: &'a [&'a AsyncActionGrant],
}
fn node(state: &State, root: WorkId, lane: JointLane) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let node = state.nodes.get(&root).ok_or(WorkError::Released)?;
    if !state.cancellations.fixed
        || node.parent.is_some()
        || node.owner != crate::OwnerState::Active
    {
        return Err(WorkError::Conflict);
    }
    if lane == JointLane::Foreground {
        node.check(&state.cancellations)?;
        if state.closed {
            return Err(WorkError::Closed);
        }
    } else if lane != JointLane::Settlement {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
fn control(state: &State, scope: &WorkScope, lane: JointLane) -> Result<JointClaimView, WorkError> {
    node(state, scope.id, lane)?;
    let work = &state.nodes[&scope.id];
    let slot = &state.cancellations.slots[work.cancellation.0];
    let id = slot.claim.ok_or(WorkError::NotReady)?;
    let row = &state.joint.as_ref().unwrap().claims[&id];
    if slot.phase != SlotPhase::Node(scope.id)
        || row.owner != ClaimOwner::ControlSlot(work.cancellation)
        || !row.custody.is_raw()
        || row.pending.is_some()
        || row.amounts != slot.base
        || row.active_amounts != slot.base
        || row.facts.key != row.active_facts.key
        || row.active_facts.dependencies.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    Ok(JointClaimView {
        inner: Arc::downgrade(&scope.inner),
        id,
        source: ClaimSource::NodeControl {
            work: scope.id,
            slot: work.cancellation,
            generation: slot.generation,
        },
        structure_revision: row.structure_revision,
        release_revision: row.release_revision,
    })
}
impl RootPayloadFence {
    pub(in crate::joint_admission::ledger) fn requires_business_active(self) -> bool {
        self.lane != JointLane::Settlement
    }
}
impl RetainedPayloadGrant {
    /// Borrow all original in-flight async grants. Their complete old D and
    /// amounts are rekeyed in this same proof, never parked as a partial vector.
    pub fn root_payload_source<'a>(
        &self,
        root: &WorkScope,
        lane: JointLane,
        active: &'a [&'a AsyncActionGrant],
    ) -> Result<RootPayloadSource<'a>, WorkError> {
        let state = root.inner.state.lock().unwrap();
        let payload = payload_growth_view(self, root, &state)?;
        let ClaimSource::Payload { epoch } = payload.source else {
            return Err(WorkError::Conflict);
        };
        let control = control(&state, root, lane)?;
        let ledger = state.joint.as_ref().unwrap();
        if active
            .len()
            .checked_add(2)
            .ok_or(WorkError::ArithmeticOverflow)?
            > ledger.config.config.max_changes_per_request
        {
            return Err(WorkError::Capacity("Root payload paid batch backing"));
        }
        for grant in active {
            if grant.claims.len() != 1 {
                return Err(WorkError::Conflict);
            }
            let original = &grant.claims[0];
            if !Arc::ptr_eq(&original.inner, &root.inner) {
                return Err(WorkError::ForeignAuthority);
            }
            let id = original.id.ok_or(WorkError::Released)?;
            let row = ledger.claims.get(&id).ok_or(WorkError::Released)?;
            if row.owner != ClaimOwner::PhysicalWork(root.id) || !row.custody.is_raw() {
                return Err(WorkError::Conflict);
            }
            if row.pending.is_some()
                || row.amounts != row.active_amounts
                || row.facts.key != row.active_facts.key
                || row.amounts.get(JointResource::AsyncIo) == 0
                || !row.active_facts.key.causal.is_empty()
            {
                return Err(WorkError::Conflict);
            }
        }
        Ok(RootPayloadSource {
            inner: Arc::downgrade(&root.inner),
            root: root.id,
            fence: RootPayloadFence {
                payload: payload.id,
                epoch,
                physical: active.len(),
                lane,
            },
            payload,
            control,
            physical: active,
        })
    }
}
impl RootPayloadSource<'_> {
    pub fn begin(self, delta_bytes: u64) -> Result<RootPayloadProposal, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let scope = WorkScope {
            inner: Arc::clone(&inner),
            id: self.root,
        };
        {
            let state = inner.state.lock().unwrap();
            node(&state, self.root, self.fence.lane)?;
            if delta_bytes > super::root_stage::maximum_payload_increment(&state, self.root)? {
                return Err(WorkError::Capacity("Root protected payload increment"));
            }
        }
        let proposal = JointAdmissionHandle { inner }.begin_paid_proposal_for_scope(scope)?;
        self.finish_begin(proposal, delta_bytes)
    }

    /// Full owns no paid constructor. The caller keeps the same stable Root,
    /// payload grant and frozen active-grant set while awaiting reobservation.
    pub fn try_begin(
        self,
        delta_bytes: u64,
    ) -> Result<JointTryBegin<RootPayloadProposal>, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let scope = WorkScope {
            inner: Arc::clone(&inner),
            id: self.root,
        };
        {
            let state = inner.state.lock().unwrap();
            node(&state, self.root, self.fence.lane)?;
            if delta_bytes > super::root_stage::maximum_payload_increment(&state, self.root)? {
                return Err(WorkError::Capacity("Root protected payload increment"));
            }
        }
        match (JointAdmissionHandle { inner }).try_begin_paid_proposal_for_scope(scope)? {
            JointTryBegin::Full(wait) => Ok(JointTryBegin::Full(wait)),
            JointTryBegin::Started(proposal) => self
                .finish_begin(proposal, delta_bytes)
                .map(JointTryBegin::Started),
        }
    }

    fn finish_begin(
        self,
        mut proposal: JointProposal,
        delta_bytes: u64,
    ) -> Result<RootPayloadProposal, WorkError> {
        proposal.root_payload = Some(self.fence);
        let mut physical = Vec::new();
        physical
            .try_reserve_exact(self.physical.len())
            .map_err(|_| WorkError::Capacity("Root async source backing"))?;
        for grant in self.physical {
            let view = grant.claims[0].view()?;
            if physical
                .iter()
                .any(|old: &JointClaimView| old.id == view.id)
            {
                return Err(WorkError::Conflict);
            }
            physical.push(view);
        }
        proposal.begin_grow(
            self.payload,
            JointResources::ZERO.with(JointResource::PayloadBytes, delta_bytes),
        )?;
        Ok(RootPayloadProposal {
            control: self.control,
            physical,
            phase: 0,
            proposal,
        })
    }
}
#[must_use = "Finish the original Root union, or drop the same paid constructor"]
pub struct RootPayloadProposal {
    control: JointClaimView,
    physical: Vec<JointClaimView>,
    phase: u8,
    proposal: JointProposal,
}
impl RootPayloadProposal {
    pub fn push_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        if self.phase != 0 {
            self.proposal.failed = Some(WorkError::Conflict);
            return Err(WorkError::Conflict);
        }
        self.proposal.push_dependency(fact)
    }
    pub fn end_change(&mut self) -> Result<(), WorkError> {
        self.proposal.healthy()?;
        if self.phase != 0 {
            self.proposal.failed = Some(WorkError::Conflict);
            return Err(WorkError::Conflict);
        }
        self.proposal.arm()?;
        let result = (|| {
            self.proposal.end()?;
            self.proposal.begin(
                Some(self.control.clone()),
                JointLane::Custody,
                JointResources::ZERO,
                false,
            )?;
            let payload = Arc::clone(&self.proposal.records[0].claim.facts);
            super::root_stage::merge(&mut self.proposal, &payload.dependencies)?;
            // All active request rows and the Root control use one full union.
            for source in &self.physical {
                let facts = {
                    let state = self.proposal.scope.inner.state.lock().unwrap();
                    Arc::clone(
                        &state
                            .joint
                            .as_ref()
                            .unwrap()
                            .claims
                            .get(&source.id)
                            .ok_or(WorkError::Released)?
                            .active_facts,
                    )
                };
                super::root_stage::merge(&mut self.proposal, &facts.dependencies)?;
            }
            self.proposal.end()?;
            let full = Arc::clone(&self.proposal.records[1].claim.facts);
            for source in &self.physical {
                self.proposal.begin(
                    Some(source.clone()),
                    JointLane::Foreground,
                    JointResources::ZERO,
                    false,
                )?;
                super::root_stage::merge(&mut self.proposal, &full.dependencies)?;
                self.proposal.end()?;
            }
            self.phase = 1;
            Ok(())
        })();
        self.proposal.remember(result)
    }
    pub fn submit(self) -> Result<RootPayloadAdmission, JointAdmissionFailure> {
        if self.phase != 1 {
            return Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            });
        }
        self.proposal
            .submit()
            .map(|admission| RootPayloadAdmission { admission })
    }
    pub fn submit_finite(
        self,
        limit: NonZeroU64,
    ) -> Result<RootPayloadAdmission, JointAdmissionFailure> {
        if self.phase != 1 {
            return Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            });
        }
        self.proposal
            .submit_finite(limit)
            .map(|admission| RootPayloadAdmission { admission })
    }
}
fn includes(full: &[DependencyRef], old: &[DependencyRef]) -> bool {
    old.iter()
        .all(|r| full.binary_search_by_key(&r.ordinal, |r| r.ordinal).is_ok())
}
pub(in crate::joint_admission::ledger) fn validate_root_payload(
    state: &State,
    root: WorkId,
    fence: RootPayloadFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    node(state, root, fence.lane)?;
    let count = fence
        .physical
        .checked_add(2)
        .ok_or(WorkError::ArithmeticOverflow)?;
    if changes.len() < count
        || changes.iter().skip(count).any(|c| {
            c.source
                .as_ref()
                .is_none_or(|s| !matches!(s.source, ClaimSource::Production { .. }))
        })
    {
        return Err(WorkError::Conflict);
    }
    validate_control_productions(state, changes)?;
    let ledger = state.joint.as_ref().unwrap();
    let payload = &changes[0];
    let source = payload.source.as_ref().ok_or(WorkError::Conflict)?;
    let row = ledger
        .claims
        .get(&fence.payload)
        .ok_or(WorkError::Released)?;
    if source.id != fence.payload
        || !matches!(source.source,ClaimSource::Payload{epoch} if epoch==fence.epoch)
        || row.owner != ClaimOwner::PhysicalWork(root)
        || !matches!(row.custody,ClaimCustody::Payload{epoch,pins} if epoch==fence.epoch && pins!=0)
        || payload.old != Some(source.id)
        || payload.claim.facts.lane != JointLane::Custody
    {
        return Err(WorkError::Conflict);
    }
    let delta = payload
        .claim
        .amounts
        .checked_sub(payload.original_amounts.ok_or(WorkError::Conflict)?)?;
    if delta
        != JointResources::ZERO.with(
            JointResource::PayloadBytes,
            delta.get(JointResource::PayloadBytes),
        )
        || delta.get(JointResource::PayloadBytes)
            > super::root_stage::maximum_payload_increment(state, root)?
    {
        return Err(WorkError::Capacity("Root protected payload increment"));
    }
    let full = &changes[1].claim.facts.dependencies;
    if full.is_empty() || !includes(full, &payload.claim.facts.dependencies) {
        return Err(WorkError::Conflict);
    }
    for (index, change) in changes.iter().take(count).enumerate() {
        let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
        let old = change.original_facts.as_ref().ok_or(WorkError::Conflict)?;
        if change.old != Some(source.id)
            || change.claim.facts.class != state.nodes[&root].class
            || !includes(&change.claim.facts.dependencies, &old.dependencies)
        {
            return Err(WorkError::Conflict);
        }
        if index == 0 {
            continue;
        }
        if change.claim.amounts != change.original_amounts.ok_or(WorkError::Conflict)?
            || change.claim.facts.key.dependencies != changes[1].claim.facts.key.dependencies
        {
            return Err(WorkError::Conflict);
        }
        if index == 1 {
            let ClaimSource::NodeControl {
                work,
                slot,
                generation,
            } = source.source
            else {
                return Err(WorkError::Conflict);
            };
            let cell = &state.cancellations.slots[slot.0];
            if work != root
                || state.nodes[&root].cancellation != slot
                || cell.phase != SlotPhase::Node(root)
                || cell.generation != generation
                || cell.claim != Some(source.id)
                || change.claim.amounts != cell.base
            {
                return Err(WorkError::Conflict);
            }
        } else {
            let row = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            if !matches!(source.source, ClaimSource::Physical)
                || row.owner != ClaimOwner::PhysicalWork(root)
                || !row.custody.is_raw()
                || change.claim.amounts.get(JointResource::AsyncIo) == 0
                || !change.claim.facts.key.causal.is_empty()
            {
                return Err(WorkError::Conflict);
            }
        }
    }
    Ok(())
}
#[must_use = "Retain the same finite paid intent through installation"]
pub struct RootPayloadAdmission {
    admission: JointAdmission,
}
impl Future for RootPayloadAdmission {
    type Output = Result<RootPayloadReady, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.admission)
            .poll(cx)
            .map(|r| r.map(|batch| RootPayloadReady { batch }))
    }
}
#[must_use = "Install into the same original payload and active request rows"]
pub struct RootPayloadReady {
    batch: JointGrantedBatch,
}
impl RootPayloadReady {
    pub fn install_in(
        &mut self,
        root: &WorkScope,
        payload: &RetainedPayloadGrant,
        active: &[&AsyncActionGrant],
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if !self.batch.new_claims.is_empty() {
            return Err(WorkError::Conflict);
        }
        let receipt = self.batch.adjustment.as_mut().ok_or(WorkError::Released)?;
        let id = receipt.id.ok_or(WorkError::Released)?;
        let inner = Arc::clone(&receipt.inner);
        if !Arc::ptr_eq(&inner, &root.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        inner.update_facts_silent(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            if let RequestPhase::Failed(error) = &request.phase {
                return Err(error.clone());
            }
            if !matches!(request.phase, RequestPhase::Delivered)
                || request.owner != root.id
                || request.waker.is_some()
            {
                return Err(WorkError::Conflict);
            }
            let fence = request.root_payload.ok_or(WorkError::Conflict)?;
            if !same_payload_grant(payload, &inner, fence.payload, fence.epoch)
                || active.len() != fence.physical
            {
                return Err(WorkError::Conflict);
            }
            let changes = request.changes.clone();
            validate_root_payload(state, root.id, fence, &changes)?;
            for (grant, change) in active
                .iter()
                .zip(changes.iter().skip(2).take(fence.physical))
            {
                if grant.claims.len() != 1
                    || !Arc::ptr_eq(&grant.claims[0].inner, &inner)
                    || grant.claims[0].id != change.old
                {
                    return Err(WorkError::Conflict);
                }
            }
            for change in changes.iter() {
                let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
                let row = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                let mut committed = source.clone();
                committed.structure_revision = committed
                    .structure_revision
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                authenticate_scope(state, row, &committed, root.id)?;
                if row.pending != Some(id)
                    || row.amounts != change.claim.amounts
                    || row.facts.key != change.claim.facts.key
                    || row.active_amounts != change.original_amounts.ok_or(WorkError::Conflict)?
                    || row.active_facts.key
                        != change
                            .original_facts
                            .as_ref()
                            .ok_or(WorkError::Conflict)?
                            .key
                {
                    return Err(WorkError::Conflict);
                }
                row.structure_revision
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            for change in changes.iter() {
                let row = state
                    .joint
                    .as_mut()
                    .unwrap()
                    .claims
                    .get_mut(&change.old.unwrap())
                    .unwrap();
                row.active_facts = Arc::clone(&row.facts);
                row.active_amounts = row.amounts;
                row.pending = None;
                row.structure_revision += 1;
            }
            remove_request(state, id);
            Ok(())
        })?;
        receipt.id = None;
        Ok(crate::WorkloadProgressNotification::new(inner))
    }
}
