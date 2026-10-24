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

//! Original Root Native action and independent canonical-reply custody.
use super::*;
use crate::result_window::NativeCapacityFence;
use std::num::NonZeroU64;

/// First-party composition input from the original Native request/reply
/// builders. This describes requested backing and grants no capacity. It is
/// not a finite-source certificate and never changes causal protection.
#[derive(Clone, Copy, Debug)]
pub struct NativeAsyncActionQuote {
    items: NonZeroU64,
    action_metadata: NonZeroU64,
    input_bytes: NonZeroU64,
    queue_bytes: NonZeroU64,
    reply_metadata: NonZeroU64,
    reply_bytes: NonZeroU64,
}
impl NativeAsyncActionQuote {
    pub fn for_original_native_transport(
        items: NonZeroU64,
        action_metadata: NonZeroU64,
        input_bytes: NonZeroU64,
        queue_bytes: NonZeroU64,
        reply_metadata: NonZeroU64,
        reply_bytes: NonZeroU64,
    ) -> Self {
        Self {
            items,
            action_metadata,
            input_bytes,
            queue_bytes,
            reply_metadata,
            reply_bytes,
        }
    }
    pub fn action(self, lane: JointLane) -> Result<JointResources, WorkError> {
        if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
            return Err(WorkError::Conflict);
        }
        Ok(JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::AsyncIo, 1)
            .with(JointResource::PhysicalQueueRecords, 1)
            .with(JointResource::MetadataBytes, self.action_metadata.get())
            .with(JointResource::PayloadBytes, self.input_bytes.get())
            .with(JointResource::PhysicalQueueBytes, self.queue_bytes.get()))
    }
    pub fn custody(self) -> JointResources {
        JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::ReplyRecords, self.items.get())
            .with(JointResource::MetadataBytes, self.reply_metadata.get())
            .with(JointResource::PayloadBytes, self.reply_bytes.get())
    }
}
/// Actual original input builder's requested new backing. No physical or
/// execution position is acquired in this prequeue custody phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeQueuedInputQuote {
    metadata: NonZeroU64,
    payload: u64,
}
impl NativeQueuedInputQuote {
    pub fn for_original_native_input(metadata: NonZeroU64, payload: u64) -> Self {
        Self { metadata, payload }
    }
    pub fn custody(self) -> JointResources {
        JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::QueueRecords, 1)
            .with(JointResource::MetadataBytes, self.metadata.get())
            .with(JointResource::PayloadBytes, self.payload)
    }
}
#[derive(Clone, Copy)]
enum NativeSourceQuote {
    Action(NativeAsyncActionQuote),
    Queued(NativeQueuedInputQuote),
}
#[derive(Clone)]
pub(in crate::joint_admission::ledger) struct NativeNodeFence {
    work: WorkId,
    claim: u64,
    slot: SlotId,
    generation: u64,
    structure_revision: u64,
    release_revision: u64,
}
#[derive(Clone)]
pub(in crate::joint_admission::ledger) struct NativeActionFence {
    root: NativeNodeFence,
    execution: NativeNodeFence,
    lane: JointLane,
    capacity: NativeCapacityFence,
    quote: NativeSourceQuote,
}
/// Reusable weak delegation from the actual original Root result owner.
/// Each new proposal captures current control revisions; an existing ticket
/// is never reminted or refreshed when the Root dependency union changes.
#[derive(Clone)]
pub struct NativeAsyncActionAuthority {
    inner: Weak<Inner>,
    owner: WorkId,
    execution: WorkId,
    capacity: NativeCapacityFence,
}
impl std::fmt::Debug for NativeAsyncActionAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalNativeActionAuthority")
    }
}
impl NativeAsyncActionAuthority {
    pub fn action_source(
        &self,
        lane: JointLane,
        quote: NativeAsyncActionQuote,
    ) -> Result<NativeAsyncActionSource, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        capture_native_action(
            &WorkScope {
                inner,
                id: self.owner,
            },
            &WorkScope {
                inner: Arc::clone(&self.inner.upgrade().ok_or(WorkError::Released)?),
                id: self.execution,
            },
            self.capacity.clone(),
            lane,
            quote,
        )
    }
}
pub(crate) fn delegate_native_actions(
    scope: &WorkScope,
    execution: &WorkScope,
    capacity: NativeCapacityFence,
) -> Result<NativeAsyncActionAuthority, WorkError> {
    if !Arc::ptr_eq(&scope.inner, &execution.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    {
        let state = scope.inner.state.lock().unwrap();
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        if node.owner != crate::OwnerState::Active || node.parent.is_some() || node.root != scope.id
        {
            return Err(WorkError::Conflict);
        }
        validate_execution(&state, scope.id, execution.id)?;
        capacity.validate(&state, scope.id)?;
    }
    Ok(NativeAsyncActionAuthority {
        inner: Arc::downgrade(&scope.inner),
        owner: scope.id,
        execution: execution.id,
        capacity,
    })
}
pub struct NativeAsyncActionSource {
    inner: Weak<Inner>,
    owner: WorkId,
    fence: NativeActionFence,
}
#[must_use = "Retain this same original paid ticket in the pending batch"]
pub struct NativeAsyncActionAdmission {
    admission: JointAdmission,
    scope: WorkScope,
    fence: NativeActionFence,
}
impl NativeAsyncActionAdmission {
    pub fn adopt_action_in(
        &self,
        source: &mut Option<JointClaim>,
        target: &mut Option<NativeAsyncActionGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        typed_custody::adopt_native_action_in(
            &WorkScope {
                inner: Arc::clone(&self.scope.inner),
                id: self.fence.execution.work,
            },
            &self.fence,
            source,
            target,
        )
    }
    pub fn adopt_reply_in(
        &self,
        source: &mut Option<JointClaim>,
        target: &mut Option<NativeReplyGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        typed_custody::adopt_native_reply_in(&self.scope, &self.fence, source, target)
    }
    pub fn poll_local_offer(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<JointLocalOffer<'_>, JointAdmissionFailure>> {
        self.admission.poll_local_offer(cx)
    }
}
fn validate_execution(state: &State, root: WorkId, execution: WorkId) -> Result<(), WorkError> {
    let original = state.nodes.get(&root).ok_or(WorkError::Released)?;
    if original.owner != crate::OwnerState::Active
        || original.parent.is_some()
        || original.root != root
    {
        return Err(WorkError::Released);
    }
    let mut cursor = execution;
    for _ in 0..=state.nodes.len() {
        let node = state.nodes.get(&cursor).ok_or(WorkError::Released)?;
        if node.owner != crate::OwnerState::Active
            || node.root != root
            || node.class != original.class
        {
            return Err(WorkError::Conflict);
        }
        if cursor == root {
            return Ok(());
        }
        cursor = node.parent.ok_or(WorkError::Conflict)?;
    }
    Err(WorkError::Conflict)
}
fn capture_node(scope: &WorkScope) -> Result<NativeNodeFence, WorkError> {
    let (view, _) = control_claim_view_for_scope(scope, false)?;
    let ClaimSource::NodeControl {
        slot, generation, ..
    } = view.source
    else {
        return Err(WorkError::Conflict);
    };
    Ok(NativeNodeFence {
        work: scope.id,
        claim: view.id,
        slot,
        generation,
        structure_revision: view.structure_revision,
        release_revision: view.release_revision,
    })
}
fn node_facts<'a>(
    state: &'a State,
    node_fence: &NativeNodeFence,
    foreground: bool,
) -> Result<&'a Arc<ProfileFacts>, WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let owner = node_fence.work;
    let node = state.nodes.get(&owner).ok_or(WorkError::Released)?;
    let slot = state
        .cancellations
        .slots
        .get(node_fence.slot.0)
        .ok_or(WorkError::Released)?;
    let claim = ledger
        .claims
        .get(&node_fence.claim)
        .ok_or(WorkError::Released)?;
    if node.owner != crate::OwnerState::Active
        || !state.cancellations.fixed
        || node.cancellation != node_fence.slot
        || slot.phase != SlotPhase::Node(owner)
        || slot.generation != node_fence.generation
        || slot.claim != Some(node_fence.claim)
        || claim.owner != ClaimOwner::ControlSlot(node_fence.slot)
        || claim.structure_revision != node_fence.structure_revision
        || claim.release_revision != node_fence.release_revision
        || claim.pending.is_some()
        || claim.amounts != slot.base
        || claim.active_amounts != slot.base
        || claim.facts.key != claim.active_facts.key
        || claim.active_facts.class != node.class
        || claim.active_facts.dependencies.is_empty()
        || !claim.active_facts.key.causal.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    if foreground {
        node.check(&state.cancellations)?;
        if state.closed {
            return Err(WorkError::Closed);
        }
    }
    Ok(&claim.active_facts)
}
fn source_facts<'a>(
    state: &'a State,
    owner: WorkId,
    fence: &NativeActionFence,
) -> Result<(&'a Arc<ProfileFacts>, &'a Arc<ProfileFacts>), WorkError> {
    if owner != fence.root.work {
        return Err(WorkError::Conflict);
    }
    validate_execution(state, owner, fence.execution.work)?;
    fence.capacity.validate(state, owner)?;
    Ok((
        node_facts(state, &fence.root, fence.lane == JointLane::Foreground)?,
        node_facts(state, &fence.execution, fence.lane == JointLane::Foreground)?,
    ))
}
fn exact_union(facts: &ProfileFacts, root: &ProfileFacts, execution: &ProfileFacts) -> bool {
    let actual = &facts.key.dependencies;
    actual.iter().all(|key| {
        root.key.dependencies.binary_search(key).is_ok()
            || execution.key.dependencies.binary_search(key).is_ok()
    }) && root
        .key
        .dependencies
        .iter()
        .chain(execution.key.dependencies.iter())
        .all(|key| actual.binary_search(key).is_ok())
}
pub(in crate::joint_admission::ledger) fn validate_native_action(
    state: &State,
    owner: WorkId,
    fence: &NativeActionFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    let (facts, execution) = source_facts(state, owner, fence)?;
    let expected = match fence.quote {
        NativeSourceQuote::Action(quote) => [
            Some((quote.action(fence.lane)?, fence.lane)),
            Some((quote.custody(), JointLane::Custody)),
        ],
        NativeSourceQuote::Queued(quote) => [Some((quote.custody(), JointLane::Custody)), None],
    };
    if changes.len() != expected.iter().flatten().count() {
        return Err(WorkError::Conflict);
    }
    for (change, (amounts, lane)) in changes.iter().zip(expected.into_iter().flatten()) {
        if change.old.is_some()
            || change.source.is_some()
            || change.claim.amounts != amounts
            || change.claim.facts.class != facts.class
            || change.claim.facts.lane != lane
            || !exact_union(&change.claim.facts, facts, execution)
            || !change.claim.facts.key.causal.is_empty()
        {
            return Err(WorkError::Conflict);
        }
    }
    Ok(())
}
pub(crate) fn capture_native_action(
    scope: &WorkScope,
    execution: &WorkScope,
    capacity: NativeCapacityFence,
    lane: JointLane,
    quote: NativeAsyncActionQuote,
) -> Result<NativeAsyncActionSource, WorkError> {
    quote.action(lane)?;
    capture_native_source(
        scope,
        execution,
        capacity,
        lane,
        NativeSourceQuote::Action(quote),
    )
}
fn capture_native_source(
    scope: &WorkScope,
    execution: &WorkScope,
    capacity: NativeCapacityFence,
    lane: JointLane,
    quote: NativeSourceQuote,
) -> Result<NativeAsyncActionSource, WorkError> {
    if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
        return Err(WorkError::Conflict);
    }
    if !Arc::ptr_eq(&scope.inner, &execution.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let fence = NativeActionFence {
        root: capture_node(scope)?,
        execution: capture_node(execution)?,
        lane,
        capacity,
        quote,
    };
    source_facts(&scope.inner.state.lock().unwrap(), scope.id, &fence)?;
    Ok(NativeAsyncActionSource {
        inner: Arc::downgrade(&scope.inner),
        owner: scope.id,
        fence,
    })
}
impl NativeAsyncActionSource {
    pub fn begin(self) -> Result<NativeAsyncActionAdmission, JointAdmissionFailure> {
        self.begin_inner()
            .map_err(|error| JointAdmissionFailure { error })
    }
    fn begin_inner(self) -> Result<NativeAsyncActionAdmission, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        source_facts(&inner.state.lock().unwrap(), self.owner, &self.fence)?;
        let scope = WorkScope {
            inner: Arc::clone(&inner),
            id: self.owner,
        };
        let mut proposal = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        }
        .begin_paid_proposal_for_scope(scope.clone())?;
        let (facts, execution) = {
            let state = inner.state.lock().unwrap();
            let (root, stage) = source_facts(&state, self.owner, &self.fence)?;
            (Arc::clone(root), Arc::clone(stage))
        };
        let changes = match self.fence.quote {
            NativeSourceQuote::Action(quote) => [
                Some((self.fence.lane, quote.action(self.fence.lane)?)),
                Some((JointLane::Custody, quote.custody())),
            ],
            NativeSourceQuote::Queued(quote) => [Some((JointLane::Custody, quote.custody())), None],
        };
        proposal.native_action = Some(self.fence.clone());
        for (lane, amounts) in changes.into_iter().flatten() {
            proposal.begin_new(lane, amounts)?;
            let current = proposal.current.as_mut().expect("original Native change");
            for record in facts
                .dependencies
                .iter()
                .chain(execution.dependencies.iter())
            {
                if let Err(position) = current
                    .selected
                    .binary_search_by_key(&record.ordinal, |record| record.ordinal)
                {
                    if current.selected.len() == current.selected.capacity() {
                        return Err(WorkError::Capacity("Native action full dependency backing"));
                    }
                    current.selected.insert(position, record.clone());
                }
            }
            proposal.end_change()?;
        }
        Ok(NativeAsyncActionAdmission {
            admission: proposal.submit().map_err(|e| e.error)?,
            scope,
            fence: self.fence,
        })
    }
}

/// Pure original-state validation used inside the same typed adoption lock.
pub(in crate::joint_admission::ledger) fn validate_native_fence(
    state: &State,
    scope: &WorkScope,
    fence: &NativeActionFence,
) -> Result<(), WorkError> {
    source_facts(state, fence.root.work, fence).and_then(|_| {
        if scope.id == fence.root.work {
            Ok(())
        } else {
            Err(WorkError::Conflict)
        }
    })
}
impl NativeAsyncActionAuthority {
    pub fn begin_queued_input(
        &self,
        lane: JointLane,
        quote: NativeQueuedInputQuote,
    ) -> Result<NativeQueuedInputAdmission, JointAdmissionFailure> {
        let result = (|| {
            let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
            let scope = WorkScope {
                inner,
                id: self.owner,
            };
            let source = capture_native_source(
                &scope,
                &WorkScope {
                    inner: Arc::clone(&scope.inner),
                    id: self.execution,
                },
                self.capacity.clone(),
                lane,
                NativeSourceQuote::Queued(quote),
            )?;
            let fence = source.fence.clone();
            let admission = source.begin_inner()?.admission;
            Ok(NativeQueuedInputAdmission {
                admission,
                scope,
                fence,
            })
        })();
        result.map_err(|error| JointAdmissionFailure { error })
    }
}
#[must_use]
pub struct NativeQueuedInputAdmission {
    admission: JointAdmission,
    scope: WorkScope,
    fence: NativeActionFence,
}
impl Future for NativeQueuedInputAdmission {
    type Output = Result<NativeQueuedInputReady, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.admission).poll(cx).map(|result| {
            result.map(|batch| NativeQueuedInputReady {
                original: None,
                batch,
                scope: self.scope.clone(),
                fence: self.fence.clone(),
            })
        })
    }
}
#[must_use]
pub struct NativeQueuedInputReady {
    original: Option<JointClaim>,
    batch: JointGrantedBatch,
    scope: WorkScope,
    fence: NativeActionFence,
}
impl NativeQueuedInputReady {
    pub fn install_in(
        &mut self,
        destination: &mut Option<NativeQueuedInputGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if self.batch.adjustment.is_some() {
            return Err(WorkError::Conflict);
        }
        if self.original.is_none() {
            if self.batch.new_claims.len() != 1 {
                return Err(WorkError::Conflict);
            }
            self.original = self.batch.new_claims.pop();
        }
        typed_custody::adopt_native_input_in(
            &self.scope,
            &self.fence,
            &mut self.original,
            destination,
        )
    }
}

pub(in crate::joint_admission::ledger) fn validate_native_action_claim(
    state: &State,
    scope: &WorkScope,
    fence: &NativeActionFence,
    claim: &JointClaim,
    reply: bool,
) -> Result<(), WorkError> {
    let (root, execution) = source_facts(state, fence.root.work, fence)?;
    let NativeSourceQuote::Action(quote) = fence.quote else {
        return Err(WorkError::Conflict);
    };
    let row = state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .claims
        .get(&claim.id.ok_or(WorkError::Released)?)
        .ok_or(WorkError::Released)?;
    let expected_owner = if reply {
        fence.root.work
    } else {
        fence.execution.work
    };
    let expected_amounts = if reply {
        quote.custody()
    } else {
        quote.action(fence.lane)?
    };
    if scope.id != expected_owner
        || row.owner != ClaimOwner::PhysicalWork(expected_owner)
        || row.active_amounts != expected_amounts
        || !exact_union(&row.active_facts, root, execution)
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
impl NativeActionFence {
    pub(in crate::joint_admission::ledger) fn permits_original_settlement(&self) -> bool {
        self.lane == JointLane::Settlement
    }
    pub(in crate::joint_admission::ledger) fn physical_owner(&self, index: usize) -> WorkId {
        match (self.quote, index) {
            (NativeSourceQuote::Action(_), 0) => self.execution.work,
            _ => self.root.work,
        }
    }
}
