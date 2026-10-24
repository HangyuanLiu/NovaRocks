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

//! One original paid request for retained Root payload and one direct stage.
use super::super::typed_custody::{
    install_computation_pin, payload_growth_view, same_payload_grant,
};
use super::*;

/// Pure capture from the positive original payload grant. No typed pin or Node
/// lifetime is retained, and no raw claim/view is exposed. Installation still
/// requires that same positive grant and the actual direct-child WorkOwner.
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::RootStageSource;
/// fn no_raw(source: RootStageSource) { source.view(); }
/// ```
#[must_use = "Capture is not a grant; construct outside receiver locks"]
pub struct RootStageSource {
    inner: Weak<Inner>,
    root: WorkId,
    fence: RootStageFence,
    payload: JointClaimView,
    root_control: JointClaimView,
    stage_control: JointClaimView,
}
fn node_view(state: &State, scope: &WorkScope) -> Result<JointClaimView, WorkError> {
    validate_active_control_node(state, scope)?;
    let node = &state.nodes[&scope.id];
    let cell = &state.cancellations.slots[node.cancellation.0];
    let id = cell.claim.ok_or(WorkError::NotReady)?;
    let row = &state.joint.as_ref().unwrap().claims[&id];
    if cell.phase != SlotPhase::Node(scope.id)
        || row.pending.is_some()
        || !row.custody.is_raw()
        || row.owner != ClaimOwner::ControlSlot(node.cancellation)
        || row.amounts != cell.base
        || row.active_amounts != cell.base
        || row.facts.key != row.active_facts.key
    {
        return Err(WorkError::Conflict);
    }
    Ok(JointClaimView {
        inner: Arc::downgrade(&scope.inner),
        id,
        source: ClaimSource::NodeControl {
            work: scope.id,
            slot: node.cancellation,
            generation: cell.generation,
        },
        structure_revision: row.structure_revision,
        release_revision: row.release_revision,
    })
}
fn validate_nodes(state: &State, root: WorkId, stage: WorkId) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if state.closed || ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    if !state.cancellations.fixed {
        return Err(WorkError::NotReady);
    }
    let parent = state.nodes.get(&root).ok_or(WorkError::Released)?;
    let child = state.nodes.get(&stage).ok_or(WorkError::Released)?;
    parent.check(&state.cancellations)?;
    child.check(&state.cancellations)?;
    if parent.owner != crate::OwnerState::Active
        || child.owner != crate::OwnerState::Active
        || parent.parent.is_some()
        || child.parent != Some(root)
        || child.root != root
        || parent.class != child.class
        || root == stage
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
impl RetainedPayloadGrant {
    /// Capture may run while the original finite receiver holds its own lock:
    /// only original State reads occur, with no constructors, wakes or pins.
    pub fn root_stage_source(
        &self,
        root: &WorkScope,
        stage: &WorkOwner,
    ) -> Result<RootStageSource, WorkError> {
        let child = stage.scope();
        if !Arc::ptr_eq(&root.inner, &child.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let state = root.inner.state.lock().unwrap();
        validate_nodes(&state, root.id, child.id)?;
        let payload = payload_growth_view(self, root, &state)?;
        let ClaimSource::Payload { epoch } = payload.source else {
            unreachable!()
        };
        let root_control = node_view(&state, root)?;
        let stage_control = node_view(&state, &child)?;
        if state.joint.as_ref().unwrap().claims[&root_control.id]
            .active_facts
            .dependencies
            .is_empty()
        {
            return Err(WorkError::Conflict);
        }
        Ok(RootStageSource {
            inner: Arc::downgrade(&root.inner),
            root: root.id,
            fence: RootStageFence {
                stage: child.id,
                payload: payload.id,
                epoch,
            },
            payload,
            root_control,
            stage_control,
        })
    }
}
const ROOT_STAGE_BASE_CHANGES: usize = 4;
impl RootStageSource {
    /// Actual closed batch shape before appending canonical Production rows.
    /// This is a pure backing fact; it neither captures a source nor grants.
    pub const fn minimum_change_records() -> usize {
        ROOT_STAGE_BASE_CHANGES
    }

    /// Exact original New record, shared by composition and end_stage. No
    /// CPU, physical job or queue resource is implied by this computation pin.
    pub fn computation_progress() -> JointResources {
        computation_amounts()
    }

    /// Source support derived from finite per-D policy, not a declaration of
    /// the Root's cumulative retained union. A complete profile includes the
    /// returned quantum in Required; this pure fact itself grants nothing.
    pub fn protected_payload_increment_for_policy(
        per_dependency_bytes: u64,
        fanout: std::num::NonZeroUsize,
    ) -> Result<u64, WorkError> {
        let k = u64::try_from(fanout.get()).map_err(|_| WorkError::ArithmeticOverflow)?;
        per_dependency_bytes
            .checked_div(k)
            .filter(|n| *n != 0)
            .ok_or(WorkError::Capacity(
                "Root/stage protected payload increment",
            ))
    }

    /// Read this same source's configured maximum before preparing any input
    /// whose complete new backing would exceed the supported increment.
    pub fn maximum_protected_payload_increment(&self) -> Result<u64, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        validate_nodes(&state, self.root, self.fence.stage)?;
        maximum_payload_increment(&state, self.root)
    }

    /// Actual current originals in this closed action, including every
    /// production row of both exact works. This is a fact, not maxL policy.
    pub fn required_change_records(&self) -> Result<usize, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        validate_nodes(&state, self.root, self.fence.stage)?;
        production_change_count(
            &state,
            &[self.root, self.fence.stage],
            ROOT_STAGE_BASE_CHANGES,
        )
    }
    /// Reserve the Root's original proposal cell before body allocation. Only
    /// PayloadBytes can grow here; no CPU/queue/physical geometry is guessed.
    pub fn begin(
        self,
        payload_delta_bytes: u64,
        lane: JointLane,
    ) -> Result<RootStageProposal, WorkError> {
        if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
            return Err(WorkError::Conflict);
        }
        if payload_delta_bytes > self.maximum_protected_payload_increment()? {
            return Err(WorkError::Capacity(
                "Root/stage protected payload increment",
            ));
        }
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let root = WorkScope {
            inner: Arc::clone(&inner),
            id: self.root,
        };
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        };
        let mut proposal = handle.begin_paid_proposal_for_scope(root)?;
        if proposal.config.config.max_changes_per_request < self.required_change_records()? {
            return Err(WorkError::Capacity("Root/stage paid batch backing"));
        }
        let stage_facts = {
            let state = inner.state.lock().unwrap();
            validate_nodes(&state, self.root, self.fence.stage)?;
            let ledger = state.joint.as_ref().unwrap();
            for (source, work) in [
                (&self.payload, self.root),
                (&self.root_control, self.root),
                (&self.stage_control, self.fence.stage),
            ] {
                let row = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                authenticate_scope(&state, row, source, work)?;
                if row.pending.is_some() {
                    return Err(WorkError::Conflict);
                }
                row.structure_revision
                    .checked_add(2)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            Arc::clone(&ledger.claims[&self.stage_control.id].active_facts)
        };
        proposal.root_stage = Some(self.fence);
        proposal.begin_grow(
            self.payload,
            JointResources::ZERO.with(JointResource::PayloadBytes, payload_delta_bytes),
        )?;
        Ok(RootStageProposal {
            proposal,
            root_control: self.root_control,
            stage_control: self.stage_control,
            stage_facts,
            lane,
            phase: 0,
        })
    }
}

/// Closed phases in one original paid body. The first group selects payload
/// dependencies; the second selects additional stage-only dependencies.
#[must_use = "Finish both closed phases and submit, or drop the same proposal"]
pub struct RootStageProposal {
    root_control: JointClaimView,
    stage_control: JointClaimView,
    stage_facts: Arc<ProfileFacts>,
    lane: JointLane,
    phase: u8,
    // Its original final exit observer must follow all outer fact aliases.
    proposal: JointProposal,
}
impl RootStageProposal {
    fn require(&mut self, phase: u8) -> Result<(), WorkError> {
        self.proposal.healthy()?;
        if self.phase != phase {
            self.proposal.failed = Some(WorkError::Conflict);
            return Err(WorkError::Conflict);
        }
        Ok(())
    }
    pub fn push_payload_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        self.require(0)?;
        self.proposal.push_dependency(fact)
    }
    pub fn push_payload_selected_network(
        &mut self,
        fact: SelectedNetworkOrigin<'_>,
    ) -> Result<(), WorkError> {
        self.require(0)?;
        self.proposal.push_selected_network(fact)
    }
    pub fn end_payload(&mut self) -> Result<(), WorkError> {
        self.require(0)?;
        self.proposal.arm()?;
        let result = (|| {
            self.proposal.end()?;
            self.proposal.begin(
                Some(self.root_control.clone()),
                JointLane::Custody,
                JointResources::ZERO,
                false,
            )?;
            let payload_facts = Arc::clone(&self.proposal.records[0].claim.facts);
            merge(&mut self.proposal, &payload_facts.dependencies)?;
            merge(&mut self.proposal, &self.stage_facts.dependencies)?;
            self.phase = 1;
            Ok(())
        })();
        self.proposal.remember(result)
    }
    pub fn push_stage_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        self.require(1)?;
        self.proposal.push_dependency(fact)
    }
    pub fn push_stage_selected_network(
        &mut self,
        fact: SelectedNetworkOrigin<'_>,
    ) -> Result<(), WorkError> {
        self.require(1)?;
        self.proposal.push_selected_network(fact)
    }
    pub fn end_stage(&mut self) -> Result<(), WorkError> {
        self.require(1)?;
        self.proposal.arm()?;
        let result = (|| {
            self.proposal.end()?;
            let full = Arc::clone(&self.proposal.records[1].claim.facts);
            let first = self.stage_facts.dependencies.is_empty();
            self.proposal.begin(
                Some(self.stage_control.clone()),
                JointLane::Custody,
                JointResources::ZERO,
                first,
            )?;
            merge(&mut self.proposal, &full.dependencies)?;
            self.proposal.end()?;
            self.proposal
                .begin(None, self.lane, computation_amounts(), false)?;
            merge(&mut self.proposal, &full.dependencies)?;
            self.proposal.end()?;
            self.phase = 2;
            Ok(())
        })();
        self.proposal.remember(result)
    }
    pub fn submit(self) -> Result<RootStageAdmission, JointAdmissionFailure> {
        if self.phase != 2 {
            return Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            });
        }
        let Self {
            proposal,
            root_control,
            stage_control,
            stage_facts,
            ..
        } = self;
        drop(root_control);
        drop(stage_control);
        drop(stage_facts);
        proposal
            .submit()
            .map(|admission| RootStageAdmission { admission })
    }
    pub fn submit_finite(
        self,
        limit: std::num::NonZeroU64,
    ) -> Result<RootStageAdmission, JointAdmissionFailure> {
        if self.phase != 2 {
            return Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            });
        }
        let Self {
            proposal,
            root_control,
            stage_control,
            stage_facts,
            ..
        } = self;
        drop(root_control);
        drop(stage_control);
        drop(stage_facts);
        proposal
            .submit_finite(limit)
            .map(|admission| RootStageAdmission { admission })
    }
}
pub(super) fn merge(
    proposal: &mut JointProposal,
    references: &[DependencyRef],
) -> Result<(), WorkError> {
    let current = proposal.current.as_mut().ok_or(WorkError::Conflict)?;
    let k = proposal.config.config.max_dependencies_per_claim;
    for reference in references {
        if let Err(position) = current
            .selected
            .binary_search_by_key(&reference.ordinal, |r| r.ordinal)
        {
            if current.selected.len() == k {
                return Err(WorkError::Capacity("joint dependency union"));
            }
            current.selected.insert(position, reference.clone());
        }
    }
    Ok(())
}
pub(super) fn maximum_payload_increment(state: &State, root: WorkId) -> Result<u64, WorkError> {
    let node = state.nodes.get(&root).ok_or(WorkError::Released)?;
    let config = &state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .config
        .config;
    let class = crate::joint_admission::class_index(node.class);
    let custody = JointLane::Custody as usize;
    let cap = [
        config.global,
        config.classes[class],
        config.lanes[custody],
        config.class_lanes[class][custody],
    ]
    .into_iter()
    .map(|l| l.per_dependency.get(JointResource::PayloadBytes))
    .min()
    .ok_or(WorkError::Conflict)?;
    RootStageSource::protected_payload_increment_for_policy(
        cap,
        std::num::NonZeroUsize::new(config.max_dependencies_per_claim)
            .ok_or(WorkError::Conflict)?,
    )
}

fn computation_amounts() -> JointResources {
    JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::Computations, 1)
}
fn includes(full: &[DependencyRef], narrow: &[DependencyRef]) -> bool {
    narrow.iter().all(|fact| {
        full.binary_search_by_key(&fact.ordinal, |f| f.ordinal)
            .is_ok()
    })
}

// Shape is closed in the original request. Revisions/pending fences are checked
// by the original queue/proof/commit authors, including the actual child owner.
pub(in crate::joint_admission::ledger) fn validate_root_stage(
    state: &State,
    root: WorkId,
    fence: RootStageFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    validate_nodes(state, root, fence.stage)?;
    if changes.len() < ROOT_STAGE_BASE_CHANGES
        || changes.iter().skip(ROOT_STAGE_BASE_CHANGES).any(|change| {
            change
                .source
                .as_ref()
                .is_none_or(|source| !matches!(source.source, ClaimSource::Production { .. }))
        })
    {
        return Err(WorkError::Conflict);
    }
    validate_control_productions(state, changes)?;
    let ledger = state.joint.as_ref().unwrap();
    let class = state.nodes[&root].class;
    let payload = &changes[0];
    let source = payload.source.as_ref().ok_or(WorkError::Conflict)?;
    let row = ledger
        .claims
        .get(&fence.payload)
        .ok_or(WorkError::Released)?;
    if source.id != fence.payload
        || !matches!(source.source, ClaimSource::Payload { epoch } if epoch == fence.epoch)
        || payload.old != Some(source.id)
        || row.owner != ClaimOwner::PhysicalWork(root)
        || !matches!(row.custody, ClaimCustody::Payload { epoch, pins } if epoch == fence.epoch && pins != 0)
        || payload.claim.facts.lane != JointLane::Custody
    {
        return Err(WorkError::Conflict);
    }
    let old = payload.original_amounts.ok_or(WorkError::Conflict)?;
    let delta = payload.claim.amounts.checked_sub(old)?;
    if delta
        != JointResources::ZERO.with(
            JointResource::PayloadBytes,
            delta.get(JointResource::PayloadBytes),
        )
        || payload
            .original_facts
            .as_ref()
            .is_none_or(|f| !includes(&payload.claim.facts.dependencies, &f.dependencies))
    {
        return Err(WorkError::Conflict);
    }
    if delta.get(JointResource::PayloadBytes) > maximum_payload_increment(state, root)? {
        return Err(WorkError::Capacity(
            "Root/stage protected payload increment",
        ));
    }
    let full = &changes[1].claim.facts.dependencies;
    if full.is_empty() || !includes(full, &payload.claim.facts.dependencies) {
        return Err(WorkError::Conflict);
    }
    for (change, work) in [(&changes[1], root), (&changes[2], fence.stage)] {
        let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
        let ClaimSource::NodeControl {
            work: original,
            slot,
            generation,
        } = source.source
        else {
            return Err(WorkError::Conflict);
        };
        let node = &state.nodes[&work];
        let cell = &state.cancellations.slots[node.cancellation.0];
        if original != work
            || slot != node.cancellation
            || generation != cell.generation
            || cell.phase != SlotPhase::Node(work)
            || cell.claim != Some(source.id)
            || change.old != Some(source.id)
            || change.original_amounts != Some(cell.base)
            || change.claim.amounts != cell.base
            || change.claim.facts.lane != JointLane::Custody
            || change.claim.facts.key.dependencies != changes[1].claim.facts.key.dependencies
            || change
                .original_facts
                .as_ref()
                .is_none_or(|f| !includes(full, &f.dependencies))
        {
            return Err(WorkError::Conflict);
        }
    }
    if changes[3].old.is_some()
        || changes[3].source.is_some()
        || changes[3].claim.amounts != computation_amounts()
        || !matches!(
            changes[3].claim.facts.lane,
            JointLane::Foreground | JointLane::Settlement
        )
        || changes[3].claim.facts.key.dependencies != changes[1].claim.facts.key.dependencies
        || changes.iter().any(|c| c.claim.facts.class != class)
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

/// A sealed future: the ordinary raw New claim/adjustment never escape.
#[must_use = "Keep the same original admission through its verdict"]
pub struct RootStageAdmission {
    admission: JointAdmission,
}
impl Future for RootStageAdmission {
    type Output = Result<RootStageReady, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.admission)
            .poll(cx)
            .map(|result| result.map(|batch| RootStageReady { batch }))
    }
}
/// Uninstalled capacity cannot be used or extracted. Drop withdraws its exact
/// provisional New and old deltas through the original RAII receipt authors.
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::RootStageReady;
/// fn no_extract(ready: RootStageReady) { let _ = ready.new_claims; }
/// ```
#[must_use = "Install atomically into the original receivers before use"]
pub struct RootStageReady {
    batch: JointGrantedBatch,
}
impl RootStageReady {
    pub fn install_in(
        &mut self,
        root: &WorkScope,
        payload: &RetainedPayloadGrant,
        stage: &WorkOwner,
        destination: &mut Option<InternalComputationGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if destination.is_some() {
            return Err(WorkError::Conflict);
        }
        let child = stage.scope();
        let receipt = self.batch.adjustment.as_mut().ok_or(WorkError::Released)?;
        let id = receipt.id.ok_or(WorkError::Released)?;
        let inner = Arc::clone(&receipt.inner);
        if !Arc::ptr_eq(&inner, &root.inner) || !Arc::ptr_eq(&inner, &child.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if self.batch.new_claims.len() != 1 {
            return Err(WorkError::Conflict);
        }
        let original = &mut self.batch.new_claims[0];
        let compute = original.id.ok_or(WorkError::Released)?;
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
            let fence = request.root_stage.ok_or(WorkError::Conflict)?;
            if fence.stage != child.id
                || !same_payload_grant(payload, &inner, fence.payload, fence.epoch)
            {
                return Err(WorkError::Conflict);
            }
            let changes = request.changes.clone();
            validate_root_stage(state, root.id, fence, &changes)?;
            for change in changes.iter().filter(|change| change.old.is_some()) {
                let source = change.source.as_ref().unwrap();
                let row = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                let mut committed = source.clone();
                committed.structure_revision = committed
                    .structure_revision
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                authenticate_scope(
                    state,
                    row,
                    &committed,
                    source_work(root.id, Some(fence), source),
                )?;
                if row.pending != Some(id)
                    || row.amounts != change.claim.amounts
                    || row.facts.key != change.claim.facts.key
                    || row.active_amounts != change.original_amounts.unwrap()
                    || row.active_facts.key != change.original_facts.as_ref().unwrap().key
                {
                    return Err(WorkError::Conflict);
                }
                row.structure_revision
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            let row = ledger.claims.get(&compute).ok_or(WorkError::Released)?;
            let epoch = row
                .structure_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            if !Arc::ptr_eq(&original.inner, &inner)
                || !row.custody.is_raw()
                || row.owner != ClaimOwner::PhysicalWork(child.id)
                || row.pending.is_some()
                || row.amounts != computation_amounts()
                || row.active_amounts != computation_amounts()
                || row.facts.key != changes[3].claim.facts.key
                || row.active_facts.key != row.facts.key
            {
                return Err(WorkError::Conflict);
            }
            // All fallible checks precede the first move. No callbacks, raw
            // aliases or old pin changes occur during this original commit.
            for change in changes.iter().filter(|change| change.old.is_some()) {
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
            let grant = install_computation_pin(&inner, state, compute, epoch);
            *destination = Some(grant);
            original.id = None;
            remove_request(state, id);
            Ok(())
        })?;
        receipt.id = None;
        Ok(crate::WorkloadProgressNotification::new(inner))
    }
}

#[cfg(test)]
mod tests;
