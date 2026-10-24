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

//! Paid, bounded request construction. No caller-owned vector or freely
//! cloned registry handle is a production input to joint admission.

//! One external-stallable action from the original admitted control source.
use super::*;
use std::num::NonZeroU64;

#[derive(Clone, Copy)]
pub(in crate::joint_admission::ledger) struct BlockingActionFence {
    claim: u64,
    slot: SlotId,
    generation: u64,
    structure_revision: u64,
    release_revision: u64,
    lane: JointLane,
}

/// Permission to seek one E action, not a physical grant or finite CPU source.
/// Only the original active owner can capture its complete admitted D.
#[must_use = "Construct the paid action in the original stable job"]
pub struct BlockingActionSource {
    inner: Weak<Inner>,
    owner: WorkId,
    fence: BlockingActionFence,
}
#[must_use = "Retain the same finite ticket through physical publication"]
pub struct BlockingActionAdmission {
    admission: JointAdmission,
}
impl BlockingActionAdmission {
    pub fn poll_local_offer(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<JointLocalOffer<'_>, JointAdmissionFailure>> {
        self.admission.poll_local_offer(cx)
    }
}

fn source_facts(
    state: &State,
    owner: WorkId,
    fence: BlockingActionFence,
) -> Result<&Arc<ProfileFacts>, WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let node = state.nodes.get(&owner).ok_or(WorkError::Released)?;
    if node.owner != crate::OwnerState::Active {
        return Err(WorkError::Released);
    }
    let slot = state
        .cancellations
        .slots
        .get(fence.slot.0)
        .ok_or(WorkError::Released)?;
    let claim = ledger.claims.get(&fence.claim).ok_or(WorkError::Released)?;
    if !state.cancellations.fixed
        || node.cancellation != fence.slot
        || slot.phase != SlotPhase::Node(owner)
        || slot.generation != fence.generation
        || slot.claim != Some(fence.claim)
        || claim.owner != ClaimOwner::ControlSlot(fence.slot)
        || claim.structure_revision != fence.structure_revision
        || claim.release_revision != fence.release_revision
        || claim.pending.is_some()
        || claim.amounts != slot.base
        || claim.active_amounts != slot.base
        || claim.facts.key != claim.active_facts.key
        || claim.active_facts.class != node.class
        || claim.active_facts.dependencies.is_empty()
        || !claim.active_facts.key.causal.is_empty()
        || !matches!(fence.lane, JointLane::Foreground | JointLane::Settlement)
    {
        return Err(WorkError::Conflict);
    }
    // This existing responsibility may settle after business cancellation.
    // No ordinary new business source can acquire the settlement policy.
    if fence.lane == JointLane::Foreground {
        node.check(&state.cancellations)?;
        if state.closed {
            return Err(WorkError::Closed);
        }
    }
    Ok(&claim.active_facts)
}
impl WorkOwner {
    /// First-party lifecycle delegation. Settlement is only for this owner's
    /// already registered source/output/failure obligation, never new business.
    /// The original owner must remain Active through queue/offer/accept.
    pub fn blocking_action_source(
        &self,
        lane: JointLane,
    ) -> Result<BlockingActionSource, WorkError> {
        let scope = self.scope();
        let (view, _) = control_claim_view_for_scope(&scope, false)?;
        let ClaimSource::NodeControl {
            slot, generation, ..
        } = view.source
        else {
            return Err(WorkError::Conflict);
        };
        let fence = BlockingActionFence {
            claim: view.id,
            slot,
            generation,
            structure_revision: view.structure_revision,
            release_revision: view.release_revision,
            lane,
        };
        source_facts(&scope.inner.state.lock().unwrap(), scope.id, fence)?;
        Ok(BlockingActionSource {
            inner: Arc::downgrade(&scope.inner),
            owner: scope.id,
            fence,
        })
    }
}

pub(in crate::joint_admission::ledger) fn validate_blocking_action(
    state: &State,
    owner: WorkId,
    fence: BlockingActionFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    let facts = source_facts(state, owner, fence)?;
    let [change] = changes else {
        return Err(WorkError::Conflict);
    };
    let amounts = change.claim.amounts;
    let expected = JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::BlockingJobs, 1)
        .with(JointResource::PhysicalQueueRecords, 1)
        .with(
            JointResource::MetadataBytes,
            amounts.get(JointResource::MetadataBytes),
        )
        .with(
            JointResource::PhysicalQueueBytes,
            amounts.get(JointResource::PhysicalQueueBytes),
        );
    if change.old.is_some()
        || change.source.is_some()
        || amounts != expected
        || amounts.get(JointResource::MetadataBytes) == 0
        || amounts.get(JointResource::PhysicalQueueBytes) == 0
        || change.claim.facts.key.class != facts.key.class
        || change.claim.facts.lane != fence.lane
        || change.claim.facts.key.dependencies != facts.key.dependencies
        || !change.claim.facts.key.causal.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
impl BlockingActionSource {
    pub fn begin(
        self,
        metadata_bytes: NonZeroU64,
        physical_queue_bytes: NonZeroU64,
    ) -> Result<BlockingActionAdmission, JointAdmissionFailure> {
        self.begin_inner(metadata_bytes, physical_queue_bytes)
            .map_err(|error| JointAdmissionFailure { error })
    }
    fn begin_inner(
        self,
        metadata_bytes: NonZeroU64,
        physical_queue_bytes: NonZeroU64,
    ) -> Result<BlockingActionAdmission, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        {
            source_facts(&inner.state.lock().unwrap(), self.owner, self.fence)?;
        }
        let mut proposal = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        }
        .begin_paid_proposal_for_scope(WorkScope {
            inner: Arc::clone(&inner),
            id: self.owner,
        })?;
        // Recheck after original Q reservation; no caller-built D or mask enters.
        let facts = {
            Arc::clone(source_facts(
                &inner.state.lock().unwrap(),
                self.owner,
                self.fence,
            )?)
        };
        proposal.blocking_action = Some(self.fence);
        proposal.begin_new(
            self.fence.lane,
            JointResources::ZERO
                .with(JointResource::ClaimRecords, 1)
                .with(JointResource::BlockingJobs, 1)
                .with(JointResource::PhysicalQueueRecords, 1)
                .with(JointResource::MetadataBytes, metadata_bytes.get())
                .with(
                    JointResource::PhysicalQueueBytes,
                    physical_queue_bytes.get(),
                ),
        )?;
        let current = proposal
            .current
            .as_mut()
            .expect("one original blocking action");
        if facts.dependencies.len() > current.selected.capacity() {
            return Err(WorkError::Capacity(
                "blocking action full dependency backing",
            ));
        }
        current.selected.extend(facts.dependencies.iter().cloned());
        proposal.end_change()?;
        Ok(BlockingActionAdmission {
            admission: proposal.submit().map_err(|failure| failure.error)?,
        })
    }
}

#[cfg(test)]
mod tests;
