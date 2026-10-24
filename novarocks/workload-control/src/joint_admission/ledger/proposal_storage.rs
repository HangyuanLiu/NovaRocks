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

//! Original control-slot backing for request construction and late reply aliases.

use super::*;
use crate::scope::ReclaimWakeOnExit;
use std::ops::Deref;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProposalPhase {
    Free,
    Building,
    Queued(u64),
    Retired,
    Resetting,
}

pub(crate) struct ProposalCell {
    pub(crate) body: Arc<ProposalBody>,
    pub(crate) phase: ProposalPhase,
    pub(crate) generation: u64,
}

#[derive(Default)]
pub(crate) struct ProposalBody {
    changes: Vec<ChangeRecord>,
}

impl ProposalCell {
    pub(crate) fn new() -> Self {
        Self {
            body: Arc::new(ProposalBody::default()),
            phase: ProposalPhase::Free,
            generation: 0,
        }
    }
}

/// Internal raw aliases never run callbacks on Drop, including under State.
#[derive(Clone)]
pub(in crate::joint_admission) enum ChangeBody {
    #[cfg(test)]
    Migration(Arc<Vec<ChangeRecord>>),
    Fixed(Arc<ProposalBody>),
}
impl Deref for ChangeBody {
    type Target = [ChangeRecord];
    fn deref(&self) -> &Self::Target {
        match self {
            #[cfg(test)]
            Self::Migration(records) => records,
            Self::Fixed(body) => &body.changes,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProposalKey {
    slot: SlotId,
    index: usize,
    generation: u64,
}

/// This local constructor pin is never stored in State. Its body field in
/// PreparedRequest is declared first and exits before this release author.
pub(crate) struct ProposalConstruction {
    inner: Weak<Inner>,
    owner: WorkId,
    pub(super) key: Option<ProposalKey>,
}
impl Drop for ProposalConstruction {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else { return };
        if let Some(inner) = self.inner.upgrade() {
            inner.update(|state| {
                retire_proposal(state, key, None);
                state.nodes.get_mut(&self.owner).unwrap().resource_waiters -= 1;
                state.collect(self.owner);
            });
        }
    }
}

/// A temporary unacquired constructor slot is distinct from every capacity
/// proof/configuration/backing error. Full owns no request ticket or body.
pub enum JointTryBegin<T> {
    Started(T),
    Full(JointProposalSlotWait),
}

#[derive(Clone, Copy)]
pub(crate) struct ProposalSlotWaitTarget {
    owner: WorkId,
    slot: SlotId,
    generation: u64,
    marker: crate::WorkloadProgressRevision,
}
pub(crate) enum ProposalReservation {
    Reserved(Option<ProposalConstruction>),
    Full(ProposalSlotWaitTarget),
}

/// An event subscription in the original producer's existing fixed field.
/// Ready asks that SAME source to reobserve; it never reserves a Q cell.
pub struct JointProposalSlotWait {
    future: Pin<Box<dyn Future<Output = Result<(), WorkError>> + Send>>,
}
impl JointProposalSlotWait {
    pub(super) fn new(inner: Weak<Inner>, target: ProposalSlotWaitTarget) -> Self {
        Self {
            future: Box::pin(wait_proposal_slot(inner, target)),
        }
    }
    /// Inline layout is already included by the containing original row.
    /// Charge the second, actual named F pointee once per fixed wait position.
    pub fn metadata_layouts() -> (std::alloc::Layout, std::alloc::Layout) {
        fn layout<F: Future<Output = Result<(), WorkError>>>(
            _: fn(Weak<Inner>, ProposalSlotWaitTarget) -> F,
        ) -> std::alloc::Layout {
            std::alloc::Layout::new::<F>()
        }
        (
            std::alloc::Layout::new::<Self>(),
            layout(wait_proposal_slot),
        )
    }
}
impl Future for JointProposalSlotWait {
    type Output = Result<(), WorkError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
async fn wait_proposal_slot(
    inner: Weak<Inner>,
    target: ProposalSlotWaitTarget,
) -> Result<(), WorkError> {
    let inner = inner.upgrade().ok_or(WorkError::Released)?;
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let reobserve = {
            let state = inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            let node = state.nodes.get(&target.owner).ok_or(WorkError::Released)?;
            let slot = state
                .cancellations
                .slots
                .get(target.slot.0)
                .ok_or(WorkError::Released)?;
            if node.owner != crate::OwnerState::Active
                || node.cancellation != target.slot
                || slot.phase != SlotPhase::Node(target.owner)
                || slot.generation != target.generation
            {
                return Err(WorkError::Released);
            }
            slot.proposals
                .iter()
                .any(|cell| cell.phase == ProposalPhase::Free)
                || inner.capture_progress(&state) != target.marker
        };
        if reobserve {
            return Ok(());
        }
        changed.await;
    }
}

impl JointAdmissionHandle {
    pub(crate) fn reserve_proposal(
        &self,
        scope: &WorkScope,
    ) -> Result<Option<ProposalConstruction>, WorkError> {
        match self.try_reserve_proposal(scope)? {
            ProposalReservation::Reserved(custody) => Ok(custody),
            ProposalReservation::Full(_) => {
                Err(WorkError::Capacity("fixed proposal and reply cells"))
            }
        }
    }

    pub(crate) fn try_reserve_proposal(
        &self,
        scope: &WorkScope,
    ) -> Result<ProposalReservation, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let (result, changed) = {
            let mut state = self.inner.state.lock().unwrap();
            let result = (|| {
                let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
                if ledger.driver_closed {
                    return Err(WorkError::Closed);
                }
                let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
                if !state.cancellations.fixed {
                    return Ok(ProposalReservation::Reserved(None));
                }
                let slot = node.cancellation;
                if state.cancellations.slots[slot.0].proposals.is_empty() {
                    return Err(WorkError::Capacity("fixed proposal cell geometry"));
                }
                let Some(index) = state.cancellations.slots[slot.0]
                    .proposals
                    .iter()
                    .position(|cell| cell.phase == ProposalPhase::Free)
                else {
                    // A failed reservation is read-only. Advancing/notifying
                    // here would wake this constructor on its own Full result.
                    return Ok(ProposalReservation::Full(ProposalSlotWaitTarget {
                        owner: scope.id,
                        slot,
                        generation: state.cancellations.slots[slot.0].generation,
                        marker: self.inner.capture_progress(&state),
                    }));
                };
                let waiters = node
                    .resource_waiters
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                let cell = &mut state.cancellations.slots[slot.0].proposals[index];
                assert!(Arc::get_mut(&mut cell.body).is_some());
                let generation = cell
                    .generation
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                cell.generation = generation;
                cell.phase = ProposalPhase::Building;
                state.nodes.get_mut(&scope.id).unwrap().resource_waiters = waiters;
                Ok(ProposalReservation::Reserved(Some(ProposalConstruction {
                    inner: Arc::downgrade(&self.inner),
                    owner: scope.id,
                    key: Some(ProposalKey {
                        slot,
                        index,
                        generation,
                    }),
                })))
            })();
            let changed = matches!(&result, Ok(ProposalReservation::Reserved(Some(_))));
            if changed {
                crate::admission::dispatch(&mut state, &self.inner.config);
                crate::observation::fill_control(&mut state, &self.inner.config);
                state.advance_progress_revision();
            }
            (result, changed)
        };
        if changed {
            crate::admission::drain_wakes(&self.inner);
            self.inner.changed.notify_waiters();
        }
        result
    }

    pub(super) fn store_proposal(
        &self,
        custody: &Option<ProposalConstruction>,
        records: Vec<ChangeRecord>,
    ) -> Result<ChangeBody, WorkError> {
        let Some(key) = custody.as_ref().and_then(|c| c.key) else {
            #[cfg(test)]
            return Ok(ChangeBody::Migration(Arc::new(records)));
            #[cfg(not(test))]
            return Err(WorkError::NotReady);
        };
        // A failed construction keeps its original pin until all local values
        // exit. The original anchor remains unavailable during that exit.
        self.inner.update(|state| {
            let cell = &mut state.cancellations.slots[key.slot.0].proposals[key.index];
            if cell.generation != key.generation || cell.phase != ProposalPhase::Building {
                return Err(WorkError::Conflict);
            }
            let body = Arc::get_mut(&mut cell.body).ok_or(WorkError::Conflict)?;
            assert!(body.changes.is_empty());
            body.changes = records;
            Ok(ChangeBody::Fixed(Arc::clone(&cell.body)))
        })
    }
}

pub(super) fn queue_proposal(
    state: &mut State,
    custody: &Option<ProposalConstruction>,
    id: u64,
) -> Result<Option<ProposalKey>, WorkError> {
    let Some(key) = custody.as_ref().and_then(|c| c.key) else {
        return Ok(None);
    };
    let cell = &mut state.cancellations.slots[key.slot.0].proposals[key.index];
    if cell.generation != key.generation || cell.phase != ProposalPhase::Building {
        return Err(WorkError::Conflict);
    }
    cell.phase = ProposalPhase::Queued(id);
    Ok(Some(key))
}

pub(super) fn retire_proposal(state: &mut State, key: ProposalKey, ticket: Option<u64>) {
    let cell = &mut state.cancellations.slots[key.slot.0].proposals[key.index];
    assert_eq!(cell.generation, key.generation);
    assert_eq!(
        cell.phase,
        ticket.map_or(ProposalPhase::Building, ProposalPhase::Queued)
    );
    cell.phase = ProposalPhase::Retired;
    state.cancellations.retired_proposals += 1;
}

pub(in crate::joint_admission) fn prepare_reap_proposal(
    storage: &mut CancellationStorage,
    slot: SlotId,
    index: usize,
) -> Option<(Vec<ChangeRecord>, u64)> {
    let cell = storage.slots.get_mut(slot.0)?.proposals.get_mut(index)?;
    if cell.phase != ProposalPhase::Retired {
        return None;
    }
    let body = Arc::get_mut(&mut cell.body)?;
    let old = std::mem::take(&mut body.changes);
    cell.phase = ProposalPhase::Resetting;
    Some((old, cell.generation))
}

pub(crate) fn finish_reap_proposal(
    storage: &mut CancellationStorage,
    slot: SlotId,
    index: usize,
    generation: u64,
) {
    let cell = &mut storage.slots[slot.0].proposals[index];
    assert_eq!(cell.phase, ProposalPhase::Resetting);
    assert_eq!(cell.generation, generation);
    assert!(Arc::get_mut(&mut cell.body).is_some());
    cell.phase = ProposalPhase::Free;
    storage.retired_proposals -= 1;
}

/// The container's allocation cannot escape its original proposal custody.
/// Popped physical guards retain their own original claim/Work custody.
pub struct JointGrantedClaims {
    claims: Vec<JointClaim>,
    _body: ChangeBody,
    _exit: ReclaimWakeOnExit,
}
impl std::fmt::Debug for JointGrantedClaims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.claims.fmt(f)
    }
}
impl Deref for JointGrantedClaims {
    type Target = [JointClaim];
    fn deref(&self) -> &Self::Target {
        &self.claims
    }
}
impl JointGrantedClaims {
    pub fn pop(&mut self) -> Option<JointClaim> {
        self.claims.pop()
    }
    pub fn remove(&mut self, index: usize) -> JointClaim {
        self.claims.remove(index)
    }
    pub(super) fn new(claims: Vec<JointClaim>, body: ChangeBody, inner: &Arc<Inner>) -> Self {
        Self {
            claims,
            _body: body,
            _exit: ReclaimWakeOnExit::observe(&Arc::downgrade(inner)),
        }
    }
}

impl std::ops::DerefMut for JointGrantedClaims {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.claims
    }
}

pub struct JointGrantedClaimsIter {
    claims: std::vec::IntoIter<JointClaim>,
    _body: ChangeBody,
    _exit: ReclaimWakeOnExit,
}
impl IntoIterator for JointGrantedClaims {
    type Item = JointClaim;
    type IntoIter = JointGrantedClaimsIter;
    fn into_iter(self) -> Self::IntoIter {
        JointGrantedClaimsIter {
            claims: self.claims.into_iter(),
            _body: self._body,
            _exit: self._exit,
        }
    }
}
impl Iterator for JointGrantedClaimsIter {
    type Item = JointClaim;
    fn next(&mut self) -> Option<Self::Item> {
        self.claims.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.claims.size_hint()
    }
}
impl DoubleEndedIterator for JointGrantedClaimsIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.claims.next_back()
    }
}
impl ExactSizeIterator for JointGrantedClaimsIter {}
impl std::iter::FusedIterator for JointGrantedClaimsIter {}
