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

//! Capabilities and records in the original WC state. No proof runs here.

use super::*;
use crate::{
    WorkId, WorkOwner, WorkRequest, WorkScope, WorkloadControl,
    control_storage::{CancellationStorage, SlotId, SlotPhase},
    scope::{Inner, State},
};
use novarocks_resource_contract::SharedDependency;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, Waker},
};

mod control_storage;
mod dependency_storage;
mod metadata_geometry;
mod paid_proposal;
pub(crate) mod retained_claim;
pub use paid_proposal::JointProposal;
pub(crate) mod proposal_storage;
#[cfg(test)]
pub(crate) use control_storage::control_storage_base;
use dependency_storage::{DependencyRef, DependencyRegistry};
pub use proposal_storage::JointGrantedClaims;
use proposal_storage::{
    ChangeBody, ProposalConstruction, ProposalKey, queue_proposal, retire_proposal,
};

pub(super) struct DependencyRecord {
    pub(super) ordinal: u64,
    pub(super) fact: SharedDependency,
}

/// A registry identity for an actual selected dependency, not an I/O permit.
#[cfg(test)]
pub struct JointDependency {
    inner: Arc<Inner>,
    pub(super) record: DependencyRef,
    _exit: crate::scope::ReclaimWakeOnExit,
}
#[cfg(test)]
impl Clone for JointDependency {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            record: self.record.clone(),
            _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
        }
    }
}
#[cfg(test)]
impl JointDependency {
    pub fn fact(&self) -> &SharedDependency {
        &self.record.fact
    }
}

#[derive(Clone)]
pub struct JointAdmissionHandle {
    pub(super) inner: Arc<Inner>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ProfileKey {
    class: usize,
    lane: usize,
    dependencies: Vec<u64>,
}
pub(super) struct ProfileFacts {
    pub(super) key: Arc<ProfileKey>,
    pub(super) class: WorkClass,
    pub(super) lane: JointLane,
    pub(super) dependencies: Arc<[DependencyRef]>,
}
#[derive(Clone)]
pub(super) struct ProfileRecord {
    pub(super) facts: Arc<ProfileFacts>,
    pub(super) amounts: JointResources,
    pub(super) claims: usize,
}

#[derive(Clone)]
pub(super) struct ProposedClaim {
    pub(super) facts: Arc<ProfileFacts>,
    pub(super) amounts: JointResources,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClaimOwner {
    PhysicalWork(WorkId),
    ControlSlot(SlotId),
    OwnerRetained(WorkId),
}

pub(super) struct ClaimRecord {
    pub(super) owner: ClaimOwner,
    /// Charged proposal, including any not-yet-installed delta and DI union.
    pub(super) facts: Arc<ProfileFacts>,
    pub(super) amounts: JointResources,
    active_facts: Arc<ProfileFacts>,
    active_amounts: JointResources,
    pending: Option<u64>,
    structure_revision: u64,
    release_revision: u64,
}
/// Sealed by the original holder; a numeric claim ID grants no authority.
#[derive(Clone, Copy)]
enum ClaimSource {
    Physical,
    NodeControl {
        work: WorkId,
        slot: SlotId,
        generation: u64,
    },
    PoolFree {
        slot: SlotId,
        generation: u64,
        parent: ParentControlFence,
        deadline: Option<tokio::time::Instant>,
    },
}
#[derive(Clone, Copy)]
struct ParentControlFence {
    work: WorkId,
    slot: SlotId,
    generation: u64,
    claim: u64,
    structure_revision: u64,
    release_revision: u64,
}
#[derive(Clone)]
pub struct JointClaimView {
    inner: Weak<Inner>,
    id: u64,
    source: ClaimSource,
    structure_revision: u64,
    release_revision: u64,
}
#[derive(Clone)]
pub(super) struct ChangeRecord {
    pub(super) old: Option<u64>,
    pub(super) original_amounts: Option<JointResources>,
    pub(super) original_facts: Option<Arc<ProfileFacts>>,
    source: Option<JointClaimView>,
    pub(super) claim: ProposedClaim,
}
pub(super) enum RequestPhase {
    Waiting,
    Granted {
        new_ids: Vec<u64>,
    },
    /// New claims have crossed to their physical recipients. Withdrawal must
    /// never release them. Only the batch adjustment receipt remains here.
    Delivered,
    Failed(WorkError),
}
#[derive(Clone, Copy)]
pub(super) enum RequestBudget {
    WaitForCapacity,
    Finite { remaining: u64 },
}
impl RequestBudget {
    pub(super) fn is_finite(self) -> bool {
        matches!(self, Self::Finite { .. })
    }
    pub(super) fn limit(self, configured: u64) -> u64 {
        match self {
            Self::WaitForCapacity => configured,
            Self::Finite { remaining } => remaining.min(configured),
        }
    }
    pub(super) fn charge(&mut self, work: u64) -> Result<(), WorkError> {
        if let Self::Finite { remaining } = self {
            *remaining = remaining
                .checked_sub(work)
                .ok_or(WorkError::Capacity("joint finite proof work"))?;
        }
        Ok(())
    }
    pub(super) fn exhausted(self) -> bool {
        matches!(self, Self::Finite { remaining: 0 })
    }
}

pub(super) struct RequestRecord {
    pub(super) budget: RequestBudget,
    pub(super) owner: WorkId,
    pub(super) changes: ChangeBody,
    proposal: Option<ProposalKey>,
    pub(super) phase: RequestPhase,
    pub(super) blocked_at: Option<u64>,
    pub(super) waker: Option<Waker>,
}
pub(crate) struct JointLedger {
    pub(super) config: Arc<ValidatedJointCapacityConfig>,
    dependencies: Option<DependencyRegistry>,
    pub(super) claims: BTreeMap<u64, ClaimRecord>,
    pub(super) profiles: BTreeMap<Arc<ProfileKey>, ProfileRecord>,
    pub(super) requests: BTreeMap<u64, RequestRecord>,
    pub(super) release_epoch: u64,
    /// A rollback can rekey a live charge behind a snapshot's ordered cursor.
    /// Such a change invalidates both positive and negative certificates.
    pub(super) profile_layout_revision: u64,
    pub(super) active: Option<u64>,
    pub(super) driver_closed: bool,
    control_empty_facts: [Option<Arc<ProfileFacts>>; 5],
}
impl JointLedger {
    pub(super) fn has_reclaimable_dependencies(&self) -> bool {
        self.dependencies
            .as_ref()
            .is_some_and(|rows| rows.has_zero())
    }
    pub(super) fn reap_dependency(&mut self) -> bool {
        self.dependencies
            .as_mut()
            .is_some_and(|rows| rows.reap_one())
    }
    pub(crate) fn is_drained(&self, storage: &CancellationStorage) -> bool {
        if !self.requests.is_empty()
            || self.active.is_some()
            || self
                .dependencies
                .as_ref()
                .is_some_and(|rows| !rows.is_empty())
        {
            return false;
        }
        if !storage.fixed {
            return self.claims.is_empty() && self.profiles.is_empty();
        }
        if !storage.drained() || self.claims.len() != storage.slots.len() {
            return false;
        }
        for (index, slot) in storage.slots.iter().enumerate() {
            if slot.phase != SlotPhase::Free {
                return false;
            }
            let Some(record) = slot.claim.and_then(|id| self.claims.get(&id)) else {
                return false;
            };
            let Some(empty) = &self.control_empty_facts[class_index(slot.class)] else {
                return false;
            };
            if record.owner != ClaimOwner::ControlSlot(SlotId(index))
                || record.pending.is_some()
                || record.amounts != slot.base
                || record.active_amounts != slot.base
                || record.facts.key != empty.key
                || record.active_facts.key != empty.key
            {
                return false;
            }
        }
        let mut expected_profiles = 0;
        for (class, facts) in self.control_empty_facts.iter().enumerate() {
            let Some(facts) = facts else {
                continue;
            };
            expected_profiles += 1;
            let Some(profile) = self.profiles.get(&facts.key) else {
                return false;
            };
            let mut amount = JointResources::ZERO;
            let mut members = 0;
            for slot in &storage.slots {
                if class_index(slot.class) == class {
                    let Ok(sum) = amount.checked_add(slot.base) else {
                        return false;
                    };
                    amount = sum;
                    members += 1;
                }
            }
            if profile.amounts != amount || profile.claims != members {
                return false;
            }
        }
        self.profiles.len() == expected_profiles
    }
    pub(super) fn released(&mut self) {
        match self.release_epoch.checked_add(1) {
            Some(epoch) => self.release_epoch = epoch,
            None => self.driver_closed = true,
        }
    }
    fn layout_changed(&mut self) {
        match self.profile_layout_revision.checked_add(1) {
            Some(revision) => self.profile_layout_revision = revision,
            None => self.driver_closed = true,
        }
    }
}
impl WorkloadControl {
    pub fn configure_joint_capacity(
        &self,
        config: ValidatedJointCapacityConfig,
    ) -> Result<(JointAdmissionHandle, JointProgressDriver), WorkError> {
        let config = Arc::new(config);
        self.inner.update(|state| {
            if state.ready || state.closed || !state.nodes.is_empty() || state.joint.is_some() {
                return Err(WorkError::Conflict);
            }
            state.joint = Some(JointLedger {
                config: Arc::clone(&config),
                dependencies: None,
                claims: BTreeMap::new(),
                profiles: BTreeMap::new(),
                requests: BTreeMap::new(),
                release_epoch: 0,
                profile_layout_revision: 0,
                active: None,
                driver_closed: false,
                control_empty_facts: std::array::from_fn(|_| None),
            });
            Ok((
                JointAdmissionHandle {
                    inner: Arc::clone(&self.inner),
                },
                JointProgressDriver::new(Arc::clone(&self.inner), config),
            ))
        })
    }
}
impl JointAdmissionHandle {
    #[cfg(test)]
    pub fn register_dependency(
        &self,
        fact: SharedDependency,
    ) -> Result<JointDependency, WorkError> {
        self.inner.update(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            // Only the legacy kernel fixture can initialize an unpaid service.
            // Production composition creates this inventory after fee validation.
            #[cfg(test)]
            if state.joint.as_ref().unwrap().dependencies.is_none() {
                let count = state
                    .joint
                    .as_ref()
                    .unwrap()
                    .config
                    .config
                    .max_registered_dependencies;
                state.joint.as_mut().unwrap().dependencies = Some(DependencyRegistry::new(count)?);
            }
            let ledger = state.joint.as_mut().unwrap();
            let registry = ledger.dependencies.as_mut().ok_or(WorkError::NotReady)?;
            if let Some(record) = registry.lookup(&fact) {
                return Ok(JointDependency {
                    inner: Arc::clone(&self.inner),
                    record,
                    _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
                });
            }
            if ledger.active.is_some() {
                return Err(WorkError::Capacity("joint proof fence"));
            }
            let ordinal = state.next_id()?;
            let record = state
                .joint
                .as_mut()
                .unwrap()
                .dependencies
                .as_mut()
                .unwrap()
                .insert(fact, ordinal)?;
            Ok(JointDependency {
                inner: Arc::clone(&self.inner),
                record,
                _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
            })
        })
    }

    /// Propose a same-class child using an observed free position. Waiting
    /// does not reserve that position or block another root from taking it.
    pub fn request_child(
        &self,
        owner: &WorkOwner,
        request: WorkRequest,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        self.prepare_child(owner, request)
            .and_then(|prepared| self.queue_request(prepared))
            .map_err(|error| JointAdmissionFailure { error })
    }

    fn prepare_child(
        &self,
        owner: &WorkOwner,
        request: WorkRequest,
    ) -> Result<PreparedRequest, WorkError> {
        let scope = owner.scope();
        let custody = self.reserve_proposal(&scope)?;
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let (source, from, base, to, work_limit) = {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if !state.cancellations.fixed {
                return Err(WorkError::NotReady);
            }
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            if node.class != request.class {
                return Err(WorkError::Conflict);
            }
            let parent_slot = &state.cancellations.slots[node.cancellation.0];
            let parent_claim = parent_slot.claim.ok_or(WorkError::NotReady)?;
            let parent = &ledger.claims[&parent_claim];
            if parent.pending.is_some()
                || parent.active_facts.dependencies.is_empty()
                || parent.amounts != parent.active_amounts
                || parent.facts.key != parent.active_facts.key
            {
                return Err(WorkError::Conflict);
            }
            let slot = state.cancellations.free_heads[class_index(node.class)]
                .ok_or(WorkError::Capacity("fixed control slots"))?;
            let cell = &state.cancellations.slots[slot.0];
            let claim = cell.claim.ok_or(WorkError::NotReady)?;
            let old = &ledger.claims[&claim];
            cell.generation
                .checked_add(2)
                .ok_or(WorkError::ArithmeticOverflow)?;
            old.structure_revision
                .checked_add(2)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let source = JointClaimView {
                inner: Arc::downgrade(&self.inner),
                id: claim,
                source: ClaimSource::PoolFree {
                    slot,
                    generation: cell.generation,
                    parent: ParentControlFence {
                        work: scope.id,
                        slot: node.cancellation,
                        generation: parent_slot.generation,
                        claim: parent_claim,
                        structure_revision: parent.structure_revision,
                        release_revision: parent.release_revision,
                    },
                    deadline: request.deadline,
                },
                structure_revision: old.structure_revision,
                release_revision: old.release_revision,
            };
            authenticate_scope(&state, old, &source, scope.id)?;
            (
                source,
                Arc::clone(&old.active_facts),
                cell.base,
                Arc::clone(&parent.active_facts),
                ledger.config.config.max_total_proof_work,
            )
        };
        // No cell Arc escapes: only the existing immutable profile facts do.
        let change = ChangeRecord {
            old: Some(source.id),
            original_amounts: Some(base),
            original_facts: Some(from),
            source: Some(source),
            claim: ProposedClaim {
                facts: to,
                amounts: base,
            },
        };
        Ok(PreparedRequest {
            budget: RequestBudget::Finite {
                remaining: work_limit,
            },
            scope,
            changes: self.store_proposal(&custody, vec![change])?,
            custody,
            _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
        })
    }

    /// Queue New claims and/or growth of exact stable holders. No physical
    /// holder moves into this future. Growth authority starts only at install.
    #[cfg(test)]
    pub fn request(
        &self,
        scope: &WorkScope,
        changes: Vec<JointClaimChange>,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        self.prepare_request(scope, &changes)
            .and_then(|prepared| self.queue_request(prepared))
            .map_err(|error| JointAdmissionFailure { error })
    }

    /// Prove within one cumulative work budget, including layout reobservations.
    /// Infeasible capacity is returned rather than parked for another holder.
    /// This still awaits the original driver's bounded progression; it grants
    /// no capacity or metadata funding merely by constructing the request.
    #[cfg(test)]
    pub fn request_finite(
        &self,
        scope: &WorkScope,
        changes: Vec<JointClaimChange>,
        proof_work_limit: std::num::NonZeroU64,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        self.prepare_request(scope, &changes)
            .and_then(|mut prepared| {
                prepared.budget = RequestBudget::Finite {
                    remaining: proof_work_limit.get(),
                };
                self.queue_request(prepared)
            })
            .map_err(|error| JointAdmissionFailure { error })
    }

    /// Capture only immutable headers under State. Dependency union, sorting
    /// and record allocation occur outside the original authority's mutex.
    #[cfg(test)]
    pub(super) fn prepare_request(
        &self,
        scope: &WorkScope,
        changes: &[JointClaimChange],
    ) -> Result<PreparedRequest, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let custody = self.reserve_proposal(scope)?;
        let (config, class) = {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if changes.is_empty() || changes.len() > ledger.config.config.max_changes_per_request {
                return Err(WorkError::Capacity("joint batch records"));
            }
            if ledger.requests.len() >= ledger.config.config.max_requests {
                return Err(WorkError::Capacity("joint request and reply records"));
            }
            (Arc::clone(&ledger.config), node.class)
        };
        let config = &config.config;
        let mut records: Vec<ChangeRecord> = Vec::with_capacity(changes.len());
        for change in changes {
            let adjustment = match change {
                JointClaimChange::Grow {
                    source,
                    delta,
                    dependencies,
                } => Some((source, *delta, dependencies, false)),
                JointClaimChange::FirstBind {
                    source,
                    dependencies,
                } => Some((source, JointResources::ZERO, dependencies, true)),
                JointClaimChange::New { .. } => None,
            };
            let (source, original, lane, amounts, mut selected, additional) =
                if let Some((source, delta, dependencies, first_bind)) = adjustment {
                    if first_bind && dependencies.is_empty() {
                        return Err(WorkError::Conflict);
                    }
                    let (facts, old_amounts) =
                        self.capture_adjustment(scope, source, delta, first_bind)?;
                    if records.iter().any(|record| record.old == Some(source.id)) {
                        return Err(WorkError::Conflict);
                    }
                    let selected = facts.dependencies.to_vec();
                    (
                        Some(source.clone()),
                        Some((Arc::clone(&facts), old_amounts)),
                        facts.lane,
                        old_amounts.checked_add(delta)?,
                        selected,
                        dependencies,
                    )
                } else {
                    let JointClaimChange::New {
                        dependencies,
                        lane,
                        amounts,
                    } = change
                    else {
                        unreachable!("adjustment handled above")
                    };
                    (None, None, *lane, *amounts, Vec::new(), dependencies)
                };
            if additional.len() > config.max_dependencies_per_claim {
                return Err(WorkError::Capacity("joint dependency fanout"));
            }
            for dependency in additional {
                if !Arc::ptr_eq(&self.inner, &dependency.inner) {
                    return Err(WorkError::ForeignAuthority);
                }
                selected.push(dependency.record.clone());
            }
            selected.sort_unstable_by_key(|record| record.ordinal);
            if source.is_none() && selected.windows(2).any(|p| p[0].ordinal == p[1].ordinal) {
                return Err(WorkError::Conflict);
            }
            selected.dedup_by_key(|record| record.ordinal);
            if selected.len() > config.max_dependencies_per_claim {
                return Err(WorkError::Capacity("joint dependency union"));
            }
            let facts = Arc::new(ProfileFacts {
                key: Arc::new(ProfileKey {
                    class: class_index(class),
                    lane: lane as usize,
                    dependencies: selected.iter().map(|d| d.ordinal).collect(),
                }),
                class,
                lane,
                dependencies: selected.into(),
            });
            records.push(ChangeRecord {
                old: source.as_ref().map(|source| source.id),
                original_amounts: original.as_ref().map(|(_, amounts)| *amounts),
                original_facts: original.map(|(facts, _)| facts),
                source,
                claim: ProposedClaim { facts, amounts },
            });
        }
        Ok(PreparedRequest {
            // A resident control rekey uses one cumulative finite proof,
            // including later full-domain extensions. It cannot self-park.
            budget: if records.iter().any(|record| {
                record
                    .source
                    .as_ref()
                    .is_some_and(|source| matches!(source.source, ClaimSource::NodeControl { .. }))
            }) {
                RequestBudget::Finite {
                    remaining: config.max_total_proof_work,
                }
            } else {
                RequestBudget::WaitForCapacity
            },
            scope: scope.clone(),
            changes: self.store_proposal(&custody, records)?,
            custody,
            _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
        })
    }

    /// Reauthenticate every source after the lock-free construction phase.
    /// Only this transaction sets pending and publishes the original reply.
    pub(super) fn queue_request(
        &self,
        mut prepared: PreparedRequest,
    ) -> Result<JointAdmission, WorkError> {
        if !Arc::ptr_eq(&self.inner, &prepared.scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let scope = &prepared.scope;
        let records = &prepared.changes;
        let mut admission = JointAdmission {
            inner: Arc::clone(&self.inner),
            id: None,
        };
        self.inner.update(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if ledger.requests.len() >= ledger.config.config.max_requests {
                return Err(WorkError::Capacity("joint request and reply records"));
            }
            for change in records.iter() {
                if change.claim.facts.class != node.class {
                    return Err(WorkError::Conflict);
                }
                if let Some(source) = &change.source {
                    let old = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                    authenticate_scope(state, old, source, scope.id)?;
                    if old.pending.is_some() {
                        return Err(WorkError::Conflict);
                    }
                }
            }
            if records
                .iter()
                .any(|record| record.claim.facts.lane != JointLane::Settlement)
            {
                node.check(&state.cancellations)?;
                if state.closed {
                    return Err(WorkError::Closed);
                }
            }
            let waiters = node
                .resource_waiters
                .checked_add(usize::from(prepared.custody.is_none()))
                .ok_or(WorkError::ArithmeticOverflow)?;
            let id = state.next_id()?;
            let proposal = queue_proposal(state, &prepared.custody, id)?;
            state.nodes.get_mut(&scope.id).unwrap().resource_waiters = waiters;
            let ledger = state.joint.as_mut().unwrap();
            for record in records.iter() {
                if let Some(old) = record.old {
                    if !record
                        .source
                        .as_ref()
                        .is_some_and(|source| matches!(source.source, ClaimSource::PoolFree { .. }))
                    {
                        ledger.claims.get_mut(&old).unwrap().pending = Some(id);
                    }
                }
            }
            ledger.requests.insert(
                id,
                RequestRecord {
                    budget: match prepared.budget {
                        RequestBudget::Finite { remaining } => RequestBudget::Finite {
                            remaining: remaining.min(ledger.config.config.max_total_proof_work),
                        },
                        RequestBudget::WaitForCapacity => RequestBudget::WaitForCapacity,
                    },
                    owner: scope.id,
                    changes: records.clone(),
                    proposal,
                    phase: RequestPhase::Waiting,
                    blocked_at: None,
                    waker: None,
                },
            );
            admission.id = Some(id);
            if let Some(custody) = &mut prepared.custody {
                custody.key = None;
            }
            Ok(())
        })?;
        Ok(admission)
    }
}

pub(super) struct PreparedRequest {
    budget: RequestBudget,
    scope: WorkScope,
    changes: ChangeBody,
    custody: Option<ProposalConstruction>,
    _exit: crate::scope::ReclaimWakeOnExit,
}

#[cfg(test)]
pub enum JointClaimChange {
    /// Bind the complete resident control base to its first nonempty domain set.
    /// The source comes only from the original WorkOwner; no new guard is minted.
    FirstBind {
        source: JointClaimView,
        dependencies: Vec<JointDependency>,
    },
    New {
        dependencies: Vec<JointDependency>,
        lane: JointLane,
        amounts: JointResources,
    },
    Grow {
        source: JointClaimView,
        delta: JointResources,
        dependencies: Vec<JointDependency>,
    },
}
#[cfg(test)]
impl JointClaimChange {
    pub fn grow_by(
        source: JointClaimView,
        delta: JointResources,
        dependencies: Vec<JointDependency>,
    ) -> Self {
        Self::Grow {
            source,
            delta,
            dependencies,
        }
    }
}
fn authenticate(record: &ClaimRecord, source: &JointClaimView) -> Result<(), WorkError> {
    if record.structure_revision != source.structure_revision
        || record.release_revision != source.release_revision
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
fn authenticate_parent(
    state: &State,
    fence: ParentControlFence,
    work: WorkId,
) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    let node = state.nodes.get(&work).ok_or(WorkError::Released)?;
    node.check(&state.cancellations)?;
    let slot = state
        .cancellations
        .slots
        .get(fence.slot.0)
        .ok_or(WorkError::Released)?;
    let claim = ledger.claims.get(&fence.claim).ok_or(WorkError::Released)?;
    if work != fence.work
        || node.cancellation != fence.slot
        || slot.phase != SlotPhase::Node(work)
        || slot.generation != fence.generation
        || slot.claim != Some(fence.claim)
        || claim.owner != ClaimOwner::ControlSlot(fence.slot)
        || claim.structure_revision != fence.structure_revision
        || claim.release_revision != fence.release_revision
        || claim.pending.is_some()
        || claim.active_facts.dependencies.is_empty()
        || claim.active_amounts != claim.amounts
        || claim.active_facts.key != claim.facts.key
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

fn authenticate_scope(
    state: &State,
    record: &ClaimRecord,
    source: &JointClaimView,
    work: WorkId,
) -> Result<(), WorkError> {
    authenticate(record, source)?;
    match source.source {
        ClaimSource::Physical if record.owner == ClaimOwner::PhysicalWork(work) => Ok(()),
        ClaimSource::NodeControl {
            work: original,
            slot,
            generation,
        } => {
            let node = state.nodes.get(&work).ok_or(WorkError::Released)?;
            let cell = state
                .cancellations
                .slots
                .get(slot.0)
                .ok_or(WorkError::Released)?;
            if original != work
                || !state.cancellations.fixed
                || node.cancellation != slot
                || cell.phase != SlotPhase::Node(work)
                || cell.generation != generation
                || cell.claim != Some(source.id)
                || record.owner != ClaimOwner::ControlSlot(slot)
            {
                return Err(WorkError::Conflict);
            }
            Ok(())
        }
        ClaimSource::PoolFree {
            slot,
            generation,
            parent,
            ..
        } => {
            authenticate_parent(state, parent, work)?;
            let ledger = state.joint.as_ref().unwrap();
            let cell = state
                .cancellations
                .slots
                .get(slot.0)
                .ok_or(WorkError::Released)?;
            if !state.cancellations.fixed
                || cell.phase != SlotPhase::Free
                || cell.generation != generation
                || cell.claim != Some(source.id)
                || cell.parent.is_some()
                || record.owner != ClaimOwner::ControlSlot(slot)
                || record.pending.is_some()
                || record.amounts != cell.base
                || record.active_amounts != cell.base
                || !record.facts.dependencies.is_empty()
                || !record.active_facts.dependencies.is_empty()
                || state.cancellations.free_heads[class_index(cell.class)] != Some(slot)
                || ledger.claims[&parent.claim].active_facts.class != cell.class
            {
                return Err(WorkError::Conflict);
            }
            Ok(())
        }
        _ => Err(WorkError::Conflict),
    }
}

pub(super) fn validate_waiting(
    state: &State,
    id: u64,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    let work = ledger.requests.get(&id).ok_or(WorkError::Released)?.owner;
    for change in changes {
        if let Some(source) = &change.source {
            let record = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            authenticate_scope(state, record, source, work)?;
            let expected = if matches!(source.source, ClaimSource::PoolFree { .. }) {
                None
            } else {
                Some(id)
            };
            if record.pending != expected {
                return Err(WorkError::Conflict);
            }
        }
    }
    Ok(())
}

impl WorkOwner {
    /// Observe this exact Node's prepaid control claim. This view cannot keep
    /// the Node alive, complete it, or release its resident storage.
    pub fn control_claim_view(&self) -> Result<JointClaimView, WorkError> {
        let scope = self.scope();
        let state = scope.inner.state.lock().unwrap();
        if !state.cancellations.fixed {
            return Err(WorkError::NotReady);
        }
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        let slot = &state.cancellations.slots[node.cancellation.0];
        if slot.phase != SlotPhase::Node(scope.id) {
            return Err(WorkError::Conflict);
        }
        let id = slot.claim.ok_or(WorkError::NotReady)?;
        let record = state
            .joint
            .as_ref()
            .and_then(|ledger| ledger.claims.get(&id))
            .ok_or(WorkError::Released)?;
        Ok(JointClaimView {
            inner: Arc::downgrade(&scope.inner),
            id,
            source: ClaimSource::NodeControl {
                work: scope.id,
                slot: node.cancellation,
                generation: slot.generation,
            },
            structure_revision: record.structure_revision,
            release_revision: record.release_revision,
        })
    }
}

/// Physical custody remains here throughout any queued/granted adjustment.
/// Logical completion and adjustment cancellation must not drop this holder.
pub struct JointClaim {
    inner: Arc<Inner>,
    id: Option<u64>,
}
impl std::fmt::Debug for JointClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JointClaim")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
impl JointClaim {
    pub fn view(&self) -> Result<JointClaimView, WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        let state = self.inner.state.lock().unwrap();
        let record = state
            .joint
            .as_ref()
            .and_then(|l| l.claims.get(&id))
            .ok_or(WorkError::Released)?;
        if !matches!(record.owner, ClaimOwner::PhysicalWork(_)) {
            return Err(WorkError::Conflict);
        }
        Ok(JointClaimView {
            inner: Arc::downgrade(&self.inner),
            id,
            source: ClaimSource::Physical,
            structure_revision: record.structure_revision,
            release_revision: record.release_revision,
        })
    }
    /// Only the installed amount is permission to start physical work.
    pub fn amounts(&self) -> Result<JointResources, WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        let state = self.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .and_then(|l| l.claims.get(&id))
            .ok_or(WorkError::Released)
            .and_then(|record| {
                if matches!(record.owner, ClaimOwner::PhysicalWork(_)) {
                    Ok(record.active_amounts)
                } else {
                    Err(WorkError::Conflict)
                }
            })
    }
    /// Report an actual partial release. Granted delta remains separate; a
    /// queued/proving source becomes stale and receives typed Conflict.
    pub fn release_part(&mut self, released: JointResources) -> Result<(), WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        self.inner.update(|state| {
            let ledger = state.joint.as_mut().ok_or(WorkError::NotReady)?;
            let record = ledger.claims.get_mut(&id).ok_or(WorkError::Released)?;
            if !matches!(record.owner, ClaimOwner::PhysicalWork(_)) {
                return Err(WorkError::Conflict);
            }
            let active = record.active_amounts.checked_sub(released)?;
            let charged = record.amounts.checked_sub(released)?;
            let profile = ledger
                .profiles
                .get_mut(&record.facts.key)
                .expect("live claim profile");
            profile.amounts = profile.amounts.checked_sub(released)?;
            record.active_amounts = active;
            record.amounts = charged;
            match record.release_revision.checked_add(1) {
                Some(revision) => record.release_revision = revision,
                None => ledger.driver_closed = true,
            }
            ledger.released();
            Ok(())
        })
    }
}
impl Drop for JointClaim {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let waker = self.inner.update(|state| release_current(state, id));
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
fn subtract_profile(ledger: &mut JointLedger, facts: &Arc<ProfileFacts>, amounts: JointResources) {
    let profile = ledger
        .profiles
        .get_mut(&facts.key)
        .expect("live charge profile");
    profile.amounts = profile
        .amounts
        .checked_sub(amounts)
        .expect("profile contains charge");
    profile.claims -= 1;
    if profile.claims == 0 {
        ledger.profiles.remove(&facts.key);
    }
}
fn add_profile(ledger: &mut JointLedger, facts: Arc<ProfileFacts>, amounts: JointResources) {
    let profile = ledger
        .profiles
        .entry(Arc::clone(&facts.key))
        .or_insert(ProfileRecord {
            facts,
            amounts: JointResources::ZERO,
            claims: 0,
        });
    // Rollback is a monotone release of charged delta and dependency membership;
    // the already-certified global u64 capacity bounds the remaining sum.
    profile.amounts = profile
        .amounts
        .checked_add(amounts)
        .expect("certified live amount");
    profile.claims += 1;
}
fn remove_claim(state: &mut State, id: u64) {
    let Some(record) = state.joint.as_mut().unwrap().claims.remove(&id) else {
        return;
    };
    let ledger = state.joint.as_mut().unwrap();
    subtract_profile(ledger, &record.facts, record.amounts);
    ledger.released();
    let owner = match record.owner {
        ClaimOwner::PhysicalWork(owner) | ClaimOwner::OwnerRetained(owner) => owner,
        ClaimOwner::ControlSlot(_) => {
            unreachable!("fixed control claims are released only with their process inventory")
        }
    };
    state.nodes.get_mut(&owner).unwrap().resource_holders -= 1;
    state.collect(owner);
}
/// Only actual holder Drop or withdrawal of a never-delivered New invokes this.
fn release_current(state: &mut State, id: u64) -> Option<Waker> {
    if !state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&id))
        .is_some_and(|record| matches!(record.owner, ClaimOwner::PhysicalWork(_)))
    {
        return None;
    }
    let pending = state
        .joint
        .as_ref()
        .and_then(|l| l.claims.get(&id))
        .and_then(|r| r.pending);
    let waker = pending.and_then(|ticket| fail_request(state, ticket, WorkError::Released));
    remove_claim(state, id);
    waker
}
fn remove_request(state: &mut State, id: u64) {
    if let Some(request) = state.joint.as_mut().unwrap().requests.remove(&id) {
        for change in request.changes.iter() {
            if let Some(source) = &change.source {
                if let ClaimSource::PoolFree { slot, .. } = source.source {
                    if matches!(state.cancellations.slots[slot.0].phase,
                        SlotPhase::ChildGranted { ticket, .. } if ticket == id)
                    {
                        let record = state
                            .joint
                            .as_mut()
                            .unwrap()
                            .claims
                            .get_mut(&source.id)
                            .unwrap();
                        assert_eq!(record.pending, Some(id));
                        record.pending = None;
                        state.cancellations.retire(slot);
                    }
                }
            }
        }
        if let Some(key) = request.proposal {
            retire_proposal(state, key, Some(id));
        }
        state
            .nodes
            .get_mut(&request.owner)
            .unwrap()
            .resource_waiters -= 1;
        state.collect(request.owner);
    }
}
/// Shared cancellation path. The request retains its typed failure until its
/// future/receipt is consumed. It never restores a snapshot of an old amount.
pub(super) fn fail_request(state: &mut State, id: u64, error: WorkError) -> Option<Waker> {
    let ledger = state.joint.as_mut().unwrap();
    let request = ledger.requests.get_mut(&id)?;
    if matches!(request.phase, RequestPhase::Failed(_)) {
        return request.waker.take();
    }
    let phase = std::mem::replace(&mut request.phase, RequestPhase::Failed(error));
    let changes = request.changes.clone();
    let waker = request.waker.take();
    let charged = matches!(
        phase,
        RequestPhase::Granted { .. } | RequestPhase::Delivered
    );
    for change in changes.iter() {
        if change
            .source
            .as_ref()
            .is_some_and(|source| matches!(source.source, ClaimSource::PoolFree { .. }))
        {
            // ChildGranted is an actual committed responsibility. Keep its
            // active domain fee and pending ticket until this reply exits.
            continue;
        }
        let Some(old) = change.old else {
            continue;
        };
        let Some(record) = ledger.claims.get_mut(&old) else {
            continue;
        };
        if record.pending != Some(id) {
            continue;
        }
        record.pending = None;
        if charged {
            let from = Arc::clone(&record.facts);
            let amount = record.amounts;
            let to = Arc::clone(&record.active_facts);
            let active = record.active_amounts;
            record.facts = Arc::clone(&to);
            record.amounts = active;
            match record.structure_revision.checked_add(1) {
                Some(revision) => record.structure_revision = revision,
                None => ledger.driver_closed = true,
            }
            subtract_profile(ledger, &from, amount);
            add_profile(ledger, Arc::clone(&to), active);
            if from.key != to.key {
                ledger.layout_changed();
            }
            ledger.released();
        }
    }
    if let RequestPhase::Granted { new_ids } = phase {
        for id in new_ids {
            remove_claim(state, id);
        }
    }
    waker
}
fn withdraw_request(state: &mut State, id: u64) -> Option<Waker> {
    let waker = fail_request(state, id, WorkError::Released);
    remove_request(state, id);
    waker
}

/// Called only by the unique driver, after proof and exact source validation.
/// Preflight reads live affected groups; it never swaps a copied ledger.
pub(super) fn grant_request(state: &mut State, id: u64) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().unwrap();
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
    if !matches!(request.phase, RequestPhase::Waiting) {
        return Err(WorkError::Conflict);
    }
    validate_waiting(state, id, &request.changes)?;
    let changes = request.changes.clone();
    let owner = request.owner;
    let new_count = changes.iter().filter(|c| c.old.is_none()).count();
    if ledger
        .claims
        .len()
        .checked_add(new_count)
        .ok_or(WorkError::ArithmeticOverflow)?
        > ledger.config.config.max_claims
    {
        return Err(WorkError::Capacity("joint claim records"));
    }
    state.nodes[&owner]
        .resource_holders
        .checked_add(new_count)
        .ok_or(WorkError::ArithmeticOverflow)?;
    state
        .next_id
        .checked_add(new_count as u64)
        .ok_or(WorkError::ArithmeticOverflow)?;
    ledger
        .profile_layout_revision
        .checked_add(changes.len() as u64)
        .ok_or(WorkError::ArithmeticOverflow)?;
    for change in changes.iter() {
        if let Some(source) = &change.source {
            if let ClaimSource::PoolFree { slot, .. } = source.source {
                let cell = &mut state.cancellations.slots[slot.0];
                cell.generation
                    .checked_add(2)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                Arc::get_mut(cell.cell.as_mut().unwrap()).ok_or(WorkError::Conflict)?;
            }
        }
    }
    let mut profiles = BTreeMap::new();
    for change in changes.iter() {
        for facts in change
            .original_facts
            .iter()
            .chain(std::iter::once(&change.claim.facts))
        {
            profiles.entry(Arc::clone(&facts.key)).or_insert_with(|| {
                ledger
                    .profiles
                    .get(&facts.key)
                    .cloned()
                    .unwrap_or(ProfileRecord {
                        facts: Arc::clone(facts),
                        amounts: JointResources::ZERO,
                        claims: 0,
                    })
            });
        }
    }
    // All originals are removed before any candidate is added, including when
    // original and union dependency keys differ or two sources share a group.
    for change in changes.iter() {
        if let Some(facts) = &change.original_facts {
            let profile = profiles.get_mut(&facts.key).unwrap();
            profile.amounts = profile
                .amounts
                .checked_sub(change.original_amounts.unwrap())?;
            profile.claims = profile.claims.checked_sub(1).ok_or(WorkError::Conflict)?;
        }
    }
    for change in changes.iter() {
        let profile = profiles.get_mut(&change.claim.facts.key).unwrap();
        profile.amounts = profile.amounts.checked_add(change.claim.amounts)?;
        profile.claims = profile
            .claims
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    let mut new_ids = Vec::with_capacity(new_count);
    for change in changes.iter() {
        if let Some(old) = change.old {
            let ledger = state.joint.as_mut().unwrap();
            let record = ledger.claims.get_mut(&old).unwrap();
            let rekey = record.facts.key != change.claim.facts.key;
            record.facts = Arc::clone(&change.claim.facts);
            record.amounts = change.claim.amounts;
            record.structure_revision += 1; // Reserved by request preflight.
            if let Some(source) = &change.source {
                if let ClaimSource::PoolFree { slot, parent, .. } = source.source {
                    record.active_facts = Arc::clone(&record.facts);
                    record.active_amounts = record.amounts;
                    record.pending = Some(id);
                    let cell = &mut state.cancellations.slots[slot.0];
                    let class = class_index(cell.class);
                    assert_eq!(state.cancellations.free_heads[class], Some(slot));
                    state.cancellations.free_heads[class] = cell.next_free.take();
                    cell.generation += 1;
                    cell.phase = SlotPhase::ChildGranted {
                        parent: parent.work,
                        ticket: id,
                    };
                }
            }
            if rekey {
                ledger.layout_changed();
            }
        } else {
            let new_id = state.next_id().expect("prechecked claim ids");
            state.joint.as_mut().unwrap().claims.insert(
                new_id,
                ClaimRecord {
                    owner: ClaimOwner::PhysicalWork(owner),
                    facts: Arc::clone(&change.claim.facts),
                    amounts: change.claim.amounts,
                    active_facts: Arc::clone(&change.claim.facts),
                    active_amounts: change.claim.amounts,
                    pending: None,
                    structure_revision: 0,
                    release_revision: 0,
                },
            );
            state.nodes.get_mut(&owner).unwrap().resource_holders += 1;
            new_ids.push(new_id);
        }
    }
    let ledger = state.joint.as_mut().unwrap();
    for (key, profile) in profiles {
        if profile.claims == 0 {
            ledger.profiles.remove(&key);
        } else {
            ledger.profiles.insert(key, profile);
        }
    }
    for change in changes.iter() {
        for dependency in change.claim.facts.dependencies.iter() {
            ledger
                .dependencies
                .as_mut()
                .unwrap()
                .mark_published(dependency);
        }
    }
    ledger.requests.get_mut(&id).unwrap().phase = RequestPhase::Granted { new_ids };
    Ok(())
}

#[derive(Debug)]
pub struct JointAdmissionFailure {
    pub error: WorkError,
}
#[derive(Debug)]
pub struct JointGrantedBatch {
    pub new_claims: JointGrantedClaims,
    pub adjustment: Option<JointAdjustmentReceipt>,
}
#[must_use = "Admission must be awaited or explicitly dropped"]
pub struct JointAdmission {
    inner: Arc<Inner>,
    id: Option<u64>,
}
impl Future for JointAdmission {
    type Output = Result<JointGrantedBatch, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id.expect("completed joint admission polled again");
        let inner = Arc::clone(&self.inner);
        // Declared outside the transaction: every committed but unreceived ID
        // already has custody before any arbitrary wake callback can run.
        let mut delivery = ReplyDelivery {
            inner: Arc::clone(&inner),
            new_ids: Vec::new(),
            ticket: None,
            body: None,
            _exit: None,
        };
        let result = inner.update(|state| {
            let closed = state.joint.as_ref().unwrap().driver_closed;
            let request = state
                .joint
                .as_mut()
                .unwrap()
                .requests
                .get_mut(&id)
                .expect("live reply");
            match &request.phase {
                RequestPhase::Waiting => {
                    if request
                        .waker
                        .as_ref()
                        .is_none_or(|w| !w.will_wake(cx.waker()))
                    {
                        request.waker = Some(cx.waker().clone());
                    }
                    return None;
                }
                RequestPhase::Failed(error) => {
                    let error = error.clone();
                    self.id = None;
                    remove_request(state, id);
                    return Some(Err(error));
                }
                RequestPhase::Delivered => unreachable!("one reply consumer"),
                RequestPhase::Granted { .. } => {}
            }
            if closed {
                fail_request(state, id, WorkError::Closed);
                self.id = None;
                remove_request(state, id);
                return Some(Err(WorkError::Closed));
            }
            let changes = request.changes.clone();
            let owner = request.owner;
            if changes
                .iter()
                .any(|c| c.claim.facts.lane != JointLane::Settlement)
            {
                if let Err(error) = state
                    .nodes
                    .get(&owner)
                    .ok_or(WorkError::Released)
                    .and_then(|n| n.check(&state.cancellations))
                {
                    fail_request(state, id, error.clone());
                    self.id = None;
                    remove_request(state, id);
                    return Some(Err(error));
                }
            }
            let request = state.joint.as_mut().unwrap().requests.get_mut(&id).unwrap();
            let RequestPhase::Granted { new_ids } =
                std::mem::replace(&mut request.phase, RequestPhase::Delivered)
            else {
                unreachable!()
            };
            let has_adjustment = changes.iter().any(|change| change.old.is_some());
            // IDs are checked, monotonic and never reused in this exact
            // authority. Only this unique reply constructs physical guards.
            delivery.new_ids = new_ids;
            delivery.ticket = has_adjustment.then_some(id);
            delivery.body = Some(changes);
            delivery._exit = Some(crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(
                &inner,
            )));
            self.id = None;
            if !has_adjustment {
                remove_request(state, id);
            }
            Some(Ok(()))
        });
        let Some(result) = result else {
            return Poll::Pending;
        };
        if let Err(error) = result {
            return Poll::Ready(Err(JointAdmissionFailure { error }));
        }
        // A partial construction unwinds this later-declared Vec first, then
        // ReplyDelivery releases only IDs not yet held by real JointClaims.
        let mut claims = Vec::with_capacity(delivery.new_ids.len());
        while let Some(&id) = delivery.new_ids.last() {
            let claim = JointClaim {
                inner: Arc::clone(&inner),
                id: Some(id),
            };
            delivery.new_ids.pop();
            claims.push(claim);
        }
        claims.reverse();
        let adjustment = delivery.ticket.map(|id| JointAdjustmentReceipt {
            inner: Arc::clone(&inner),
            id: Some(id),
        });
        delivery.ticket = None;
        let new_claims = JointGrantedClaims::new(
            claims,
            delivery.body.take().expect("committed reply body"),
            &inner,
        );
        Poll::Ready(Ok(JointGrantedBatch {
            new_claims,
            adjustment,
        }))
    }
}

/// Stack-owned handoff custody, never a State member or another request table.
/// Its body and final exit observer outlive the rollback's original transaction.
struct ReplyDelivery {
    inner: Arc<Inner>,
    new_ids: Vec<u64>,
    ticket: Option<u64>,
    body: Option<ChangeBody>,
    _exit: Option<crate::scope::ReclaimWakeOnExit>,
}
impl Drop for ReplyDelivery {
    fn drop(&mut self) {
        if self.new_ids.is_empty() && self.ticket.is_none() {
            return;
        }
        let ticket = self.ticket.take();
        let waker = self.inner.update(|state| {
            while let Some(id) = self.new_ids.pop() {
                remove_claim(state, id);
            }
            ticket.and_then(|id| withdraw_request(state, id))
        });
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Drop for JointAdmission {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let waker = self.inner.update(|state| withdraw_request(state, id));
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
#[must_use = "Install against the exact stable holders or drop to withdraw the delta"]
pub struct JointAdjustmentReceipt {
    inner: Arc<Inner>,
    id: Option<u64>,
}
impl std::fmt::Debug for JointAdjustmentReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JointAdjustmentReceipt")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
impl JointAdjustmentReceipt {
    /// Synchronously adopt the whole batch into its original owners. No action
    /// may consume newly charged capacity before this operation succeeds.
    pub fn install(self, originals: &[&JointClaim]) -> Result<(), WorkError> {
        self.install_originals(originals, None)
    }

    /// Install the control binding only while borrowing its original owner.
    /// Physical claims in the same batch still require their original guards.
    pub fn install_control(
        self,
        owner: &WorkOwner,
        originals: &[&JointClaim],
    ) -> Result<(), WorkError> {
        self.install_originals(originals, Some(owner))
    }

    /// Publish the one child already committed by this exact reply. No second
    /// admission or physical-holder guard is created between grant and Node.
    pub fn install_child(mut self, owner: &WorkOwner) -> Result<WorkOwner, WorkError> {
        let parent_scope = owner.scope();
        if !Arc::ptr_eq(&parent_scope.inner, &self.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let id = self.id.expect("live child receipt");
        let result = self.inner.update(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            match &request.phase {
                RequestPhase::Failed(error) => return Err(error.clone()),
                RequestPhase::Delivered => {}
                _ => return Err(WorkError::Conflict),
            }
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if request.changes.len() != 1 || request.owner != parent_scope.id {
                return Err(WorkError::Conflict);
            }
            let change = &request.changes[0];
            let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
            let ClaimSource::PoolFree {
                slot,
                generation,
                parent,
                deadline,
            } = source.source
            else {
                return Err(WorkError::Conflict);
            };
            authenticate_parent(state, parent, parent_scope.id)?;
            let cell = &state.cancellations.slots[slot.0];
            let claim = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            if cell.phase
                != (SlotPhase::ChildGranted {
                    parent: parent.work,
                    ticket: id,
                })
                || cell.generation
                    != generation
                        .checked_add(1)
                        .ok_or(WorkError::ArithmeticOverflow)?
                || cell.claim != Some(source.id)
                || claim.owner != ClaimOwner::ControlSlot(slot)
                || claim.pending != Some(id)
                || claim.structure_revision
                    != source
                        .structure_revision
                        .checked_add(1)
                        .ok_or(WorkError::ArithmeticOverflow)?
                || claim.release_revision != source.release_revision
                || claim.amounts != change.claim.amounts
                || claim.active_amounts != change.claim.amounts
                || claim.facts.key != change.claim.facts.key
                || claim.active_facts.key != change.claim.facts.key
            {
                return Err(WorkError::Conflict);
            }
            claim
                .structure_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let claim_id = source.id;
            let child = state.install_bound_child(&self.inner, parent.work, slot, deadline)?;
            let claim = state
                .joint
                .as_mut()
                .unwrap()
                .claims
                .get_mut(&claim_id)
                .unwrap();
            claim.structure_revision += 1;
            claim.pending = None;
            remove_request(state, id);
            Ok(child)
        });
        if result.is_ok() {
            self.id = None;
        }
        result
    }

    fn install_originals(
        mut self,
        originals: &[&JointClaim],
        owner: Option<&WorkOwner>,
    ) -> Result<(), WorkError> {
        let control_scope = owner.map(WorkOwner::scope);
        let id = self.id.expect("live adjustment receipt");
        let result = self.inner.update(|state| {
            let ledger = state.joint.as_ref().unwrap();
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            match &request.phase {
                RequestPhase::Failed(error) => return Err(error.clone()),
                RequestPhase::Delivered => {}
                _ => return Err(WorkError::Conflict),
            }
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            let changes = request.changes.clone();
            if changes
                .iter()
                .any(|c| c.claim.facts.lane != JointLane::Settlement)
            {
                state
                    .nodes
                    .get(&request.owner)
                    .ok_or(WorkError::Released)?
                    .check(&state.cancellations)?;
            }
            let sources = changes.iter().filter_map(|c| c.source.as_ref());
            if originals.len()
                != sources
                    .clone()
                    .filter(|source| matches!(source.source, ClaimSource::Physical))
                    .count()
                || control_scope.is_some()
                    != sources
                        .clone()
                        .any(|source| matches!(source.source, ClaimSource::NodeControl { .. }))
            {
                return Err(WorkError::Conflict);
            }
            for source in sources {
                match source.source {
                    ClaimSource::Physical => {
                        let original = originals
                            .iter()
                            .find(|original| original.id == Some(source.id))
                            .ok_or(WorkError::Conflict)?;
                        if !Arc::ptr_eq(&original.inner, &self.inner) {
                            return Err(WorkError::ForeignAuthority);
                        }
                    }
                    ClaimSource::NodeControl { work, .. } => {
                        let original = control_scope.as_ref().ok_or(WorkError::Conflict)?;
                        if !Arc::ptr_eq(&original.inner, &self.inner) {
                            return Err(WorkError::ForeignAuthority);
                        }
                        if original.id != work || original.id != request.owner {
                            return Err(WorkError::Conflict);
                        }
                    }
                    ClaimSource::PoolFree { .. } => return Err(WorkError::Conflict),
                }
                let record = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                if record.pending != Some(id) {
                    return Err(WorkError::Conflict);
                }
                if matches!(source.source, ClaimSource::NodeControl { .. }) {
                    // The exact pending ticket excludes another control grant,
                    // and control partial release is not exposed. Its sole
                    // intervening structural change is this original grant.
                    let mut committed = source.clone();
                    committed.structure_revision = committed
                        .structure_revision
                        .checked_add(1)
                        .ok_or(WorkError::ArithmeticOverflow)?;
                    authenticate_scope(state, record, &committed, request.owner)?;
                } else if record.owner != ClaimOwner::PhysicalWork(request.owner) {
                    return Err(WorkError::Conflict);
                }
                // Physical holders may report an actual partial release after
                // grant. Adopt their remaining live amount, never the snapshot.
                record
                    .structure_revision
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            let ledger = state.joint.as_mut().unwrap();
            for change in changes.iter() {
                if let Some(old) = change.old {
                    let record = ledger.claims.get_mut(&old).unwrap();
                    record.active_facts = Arc::clone(&record.facts);
                    record.active_amounts = record.amounts;
                    record.pending = None;
                    record.structure_revision += 1;
                }
            }
            remove_request(state, id);
            Ok(())
        });
        if result.is_ok() {
            self.id = None;
        }
        // Error leaves the RAII ticket armed; Drop rolls back only live delta.
        result
    }
}
impl Drop for JointAdjustmentReceipt {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let waker = self.inner.update(|state| withdraw_request(state, id));
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
