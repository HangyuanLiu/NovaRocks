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
    WorkId, WorkScope, WorkloadControl,
    control_storage::{CancellationStorage, ControlSlot, ControlStorageConfig, SlotId, SlotPhase},
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

pub(super) struct DependencyRecord {
    pub(super) ordinal: u64,
    pub(super) fact: SharedDependency,
}

/// A registry identity for an actual selected dependency, not an I/O permit.
#[derive(Clone)]
pub struct JointDependency {
    inner: Arc<Inner>,
    pub(super) record: Arc<DependencyRecord>,
}
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
    pub(super) dependencies: Arc<[Arc<DependencyRecord>]>,
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
#[derive(Clone)]
pub struct JointClaimView {
    inner: Weak<Inner>,
    id: u64,
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
    pub(super) changes: Arc<[ChangeRecord]>,
    pub(super) phase: RequestPhase,
    pub(super) blocked_at: Option<u64>,
    pub(super) waker: Option<Waker>,
}
pub(crate) struct JointLedger {
    pub(super) config: Arc<ValidatedJointCapacityConfig>,
    dependencies: BTreeMap<SharedDependency, Weak<DependencyRecord>>,
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
    pub(crate) fn is_drained(&self, storage: &CancellationStorage) -> bool {
        if !self.requests.is_empty() || self.active.is_some() {
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
                dependencies: BTreeMap::new(),
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
    pub fn register_dependency(
        &self,
        fact: SharedDependency,
    ) -> Result<JointDependency, WorkError> {
        self.inner.update(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if let Some(record) = ledger.dependencies.get(&fact).and_then(Weak::upgrade) {
                return Ok(JointDependency {
                    inner: Arc::clone(&self.inner),
                    record,
                });
            }
            // New positive metadata cannot invalidate an in-flight snapshot.
            if ledger.active.is_some() {
                return Err(WorkError::Capacity("joint proof fence"));
            }
            let ledger = state.joint.as_mut().unwrap();
            ledger
                .dependencies
                .retain(|_, record| record.strong_count() != 0);
            if ledger.dependencies.len() >= ledger.config.config.max_registered_dependencies {
                return Err(WorkError::Capacity("joint dependency registry"));
            }
            let ordinal = state.next_id()?;
            let record = Arc::new(DependencyRecord {
                ordinal,
                fact: fact.clone(),
            });
            state
                .joint
                .as_mut()
                .unwrap()
                .dependencies
                .insert(fact, Arc::downgrade(&record));
            Ok(JointDependency {
                inner: Arc::clone(&self.inner),
                record,
            })
        })
    }

    /// Queue New claims and/or growth of exact stable holders. No physical
    /// holder moves into this future. Growth authority starts only at install.
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
    pub(super) fn prepare_request(
        &self,
        scope: &WorkScope,
        changes: &[JointClaimChange],
    ) -> Result<PreparedRequest, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
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
            let (source, original, lane, amounts, mut selected, additional) = match change {
                JointClaimChange::New {
                    dependencies,
                    lane,
                    amounts,
                } => (None, None, *lane, *amounts, Vec::new(), dependencies),
                JointClaimChange::Grow {
                    source,
                    delta,
                    dependencies,
                } => {
                    if !Weak::ptr_eq(&source.inner, &Arc::downgrade(&self.inner)) {
                        return Err(WorkError::ForeignAuthority);
                    }
                    let (facts, old_amounts) = {
                        let state = self.inner.state.lock().unwrap();
                        let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
                        let old = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                        authenticate(old, source)?;
                        if old.owner != ClaimOwner::PhysicalWork(scope.id) || old.pending.is_some()
                        {
                            return Err(WorkError::Conflict);
                        }
                        old.structure_revision
                            .checked_add(2)
                            .ok_or(WorkError::ArithmeticOverflow)?;
                        (Arc::clone(&old.active_facts), old.active_amounts)
                    };
                    if records.iter().any(|record| record.old == Some(source.id)) {
                        return Err(WorkError::Conflict);
                    }
                    let selected = facts.dependencies.to_vec();
                    (
                        Some(source.clone()),
                        Some((Arc::clone(&facts), old_amounts)),
                        facts.lane,
                        old_amounts.checked_add(*delta)?,
                        selected,
                        dependencies,
                    )
                }
            };
            if additional.len() > config.max_dependencies_per_claim {
                return Err(WorkError::Capacity("joint dependency fanout"));
            }
            for dependency in additional {
                if !Arc::ptr_eq(&self.inner, &dependency.inner) {
                    return Err(WorkError::ForeignAuthority);
                }
                selected.push(Arc::clone(&dependency.record));
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
            budget: RequestBudget::WaitForCapacity,
            scope: scope.clone(),
            changes: records.into(),
        })
    }

    /// Reauthenticate every source after the lock-free construction phase.
    /// Only this transaction sets pending and publishes the original reply.
    pub(super) fn queue_request(
        &self,
        prepared: PreparedRequest,
    ) -> Result<JointAdmission, WorkError> {
        if !Arc::ptr_eq(&self.inner, &prepared.scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let scope = &prepared.scope;
        let records = &prepared.changes;
        let id = self.inner.update(|state| {
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
                    authenticate(old, source)?;
                    if old.owner != ClaimOwner::PhysicalWork(scope.id) || old.pending.is_some() {
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
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let id = state.next_id()?;
            state.nodes.get_mut(&scope.id).unwrap().resource_waiters = waiters;
            let ledger = state.joint.as_mut().unwrap();
            for record in records.iter() {
                if let Some(old) = record.old {
                    ledger.claims.get_mut(&old).unwrap().pending = Some(id);
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
                    changes: Arc::clone(records),
                    phase: RequestPhase::Waiting,
                    blocked_at: None,
                    waker: None,
                },
            );
            Ok(id)
        })?;
        Ok(JointAdmission {
            inner: Arc::clone(&self.inner),
            id: Some(id),
        })
    }
}

pub(super) struct PreparedRequest {
    budget: RequestBudget,
    scope: WorkScope,
    changes: Arc<[ChangeRecord]>,
}

pub enum JointClaimChange {
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
pub(super) fn validate_waiting(
    ledger: &JointLedger,
    id: u64,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    for change in changes {
        if let Some(source) = &change.source {
            let record = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            authenticate(record, source)?;
            if record.pending != Some(id) {
                return Err(WorkError::Conflict);
            }
        }
    }
    Ok(())
}

/// Physical custody remains here throughout any queued/granted adjustment.
/// Logical completion and adjustment cancellation must not drop this holder.
pub struct JointClaim {
    inner: Arc<Inner>,
    id: u64,
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
        let state = self.inner.state.lock().unwrap();
        let record = state
            .joint
            .as_ref()
            .and_then(|l| l.claims.get(&self.id))
            .ok_or(WorkError::Released)?;
        Ok(JointClaimView {
            inner: Arc::downgrade(&self.inner),
            id: self.id,
            structure_revision: record.structure_revision,
            release_revision: record.release_revision,
        })
    }
    /// Only the installed amount is permission to start physical work.
    pub fn amounts(&self) -> Result<JointResources, WorkError> {
        let state = self.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .and_then(|l| l.claims.get(&self.id))
            .map(|record| record.active_amounts)
            .ok_or(WorkError::Released)
    }
    /// Report an actual partial release. Granted delta remains separate; a
    /// queued/proving source becomes stale and receives typed Conflict.
    pub fn release_part(&mut self, released: JointResources) -> Result<(), WorkError> {
        self.inner.update(|state| {
            let ledger = state.joint.as_mut().ok_or(WorkError::NotReady)?;
            let record = ledger.claims.get_mut(&self.id).ok_or(WorkError::Released)?;
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
        let waker = self.inner.update(|state| release_current(state, self.id));
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
    let ClaimOwner::PhysicalWork(owner) = record.owner else {
        unreachable!("fixed control claims are released only with their process inventory");
    };
    state.nodes.get_mut(&owner).unwrap().resource_holders -= 1;
    state.collect(owner);
}
/// Only actual holder Drop or withdrawal of a never-delivered New invokes this.
fn release_current(state: &mut State, id: u64) -> Option<Waker> {
    if state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&id))
        .is_some_and(|record| matches!(record.owner, ClaimOwner::ControlSlot(_)))
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
    let changes = Arc::clone(&request.changes);
    let waker = request.waker.take();
    let charged = matches!(
        phase,
        RequestPhase::Granted { .. } | RequestPhase::Delivered
    );
    for change in changes.iter() {
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
    validate_waiting(ledger, id, &request.changes)?;
    let changes = Arc::clone(&request.changes);
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
    ledger.requests.get_mut(&id).unwrap().phase = RequestPhase::Granted { new_ids };
    Ok(())
}

#[derive(Debug)]
pub struct JointAdmissionFailure {
    pub error: WorkError,
}
#[derive(Debug)]
pub struct JointGrantedBatch {
    pub new_claims: Vec<JointClaim>,
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
        let result = self.inner.update(|state| {
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
                    remove_request(state, id);
                    return Some(Err(error));
                }
                RequestPhase::Delivered => unreachable!("one reply consumer"),
                RequestPhase::Granted { .. } => {}
            }
            if closed {
                fail_request(state, id, WorkError::Closed);
                remove_request(state, id);
                return Some(Err(WorkError::Closed));
            }
            let changes = Arc::clone(&request.changes);
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
            if !has_adjustment {
                remove_request(state, id);
            }
            Some(Ok((new_ids, has_adjustment)))
        });
        let Some(result) = result else {
            return Poll::Pending;
        };
        self.id = None;
        Poll::Ready(
            result
                .map(|(new_ids, has_adjustment)| JointGrantedBatch {
                    new_claims: new_ids
                        .into_iter()
                        .map(|id| JointClaim {
                            inner: Arc::clone(&self.inner),
                            id,
                        })
                        .collect(),
                    adjustment: has_adjustment.then(|| JointAdjustmentReceipt {
                        inner: Arc::clone(&self.inner),
                        id: Some(id),
                    }),
                })
                .map_err(|error| JointAdmissionFailure { error }),
        )
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
    pub fn install(mut self, originals: &[&JointClaim]) -> Result<(), WorkError> {
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
            let changes = Arc::clone(&request.changes);
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
            if originals.len() != sources.clone().count() {
                return Err(WorkError::Conflict);
            }
            for source in sources {
                let Some(original) = originals.iter().find(|original| original.id == source.id)
                else {
                    return Err(WorkError::Conflict);
                };
                if !Arc::ptr_eq(&original.inner, &self.inner) {
                    return Err(WorkError::ForeignAuthority);
                }
                let record = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                if record.pending != Some(id) {
                    return Err(WorkError::Conflict);
                }
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

/// The actual storage envelope used by configuration. This deliberately covers
/// this substrate only, not actor mailboxes, Provider parsing or complete roles.
pub(crate) fn control_storage_base(
    config: ControlStorageConfig,
) -> Result<JointResources, WorkError> {
    use crate::control_storage::{
        arc_bytes, btree_peak_bytes, resources_for_bytes, storage_cell_bytes,
    };
    let count = config.count()?;
    if count == 0 || config.children_per_slot > count.saturating_sub(1) {
        return Err(WorkError::InvalidConfig("fixed control storage geometry"));
    }
    if !cfg!(any(target_os = "macos", target_os = "linux")) {
        return Err(WorkError::InvalidConfig("fixed control storage platform"));
    }
    let cell = storage_cell_bytes(config.children_per_slot)?;
    let facts = arc_bytes::<ProfileFacts>()?
        .checked_add(arc_bytes::<ProfileKey>()?)
        .and_then(|n| n.checked_add(2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>()))
        .ok_or(WorkError::ArithmeticOverflow)?;
    let mut bytes = cell
        .checked_add(facts)
        .and_then(|n| n.checked_mul(count))
        .and_then(|n| n.checked_add(std::mem::size_of::<CancellationStorage>()))
        .ok_or(WorkError::ArithmeticOverflow)?;
    // Fixed inventory reserves the original maps' retained roots and one
    // insertion path peak. These are whole-map amounts, counted exactly once.
    for map in [
        btree_peak_bytes::<WorkId, crate::scope::Node>(count)?,
        btree_peak_bytes::<u64, ClaimRecord>(count)?,
        btree_peak_bytes::<Arc<ProfileKey>, ProfileRecord>(count)?,
    ] {
        bytes = bytes
            .checked_add(map)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    // Evenly reserve this one physical envelope across its original members.
    // Rounding reserves at most count-1 extra bytes; it is not an RSS multiplier.
    let per_slot = bytes
        .checked_add(count - 1)
        .ok_or(WorkError::ArithmeticOverflow)?
        / count;
    resources_for_bytes(per_slot)
}

impl WorkloadControl {
    /// Internal storage-only composition. There is deliberately no Server call
    /// or public safe-profile API until FirstBind/roles/Provider closure exists.
    pub(crate) fn configure_control_storage(
        &self,
        config: ControlStorageConfig,
    ) -> Result<(), WorkError> {
        let count = config.count()?;
        let base = control_storage_base(config)?;
        if count != self.inner.config.scope_records_limit {
            return Err(WorkError::InvalidConfig("fixed control scope inventory"));
        }
        self.inner.update(|state| {
            if state.ready
                || state.closed
                || !state.nodes.is_empty()
                || !state.cancellations.slots.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed
                || ledger.active.is_some()
                || !ledger.claims.is_empty()
                || !ledger.requests.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            let declared = &ledger.config.config;
            if declared.max_claims < count {
                return Err(WorkError::InvalidConfig("fixed control claim inventory"));
            }
            let total_scopes = u64::try_from(count).map_err(|_| WorkError::ArithmeticOverflow)?;
            let totals = |n: usize| -> Result<JointResources, WorkError> {
                let mut result = JointResources::ZERO;
                for index in 0..RESOURCE_COUNT {
                    result.0[index] = base.0[index]
                        .checked_mul(u64::try_from(n).map_err(|_| WorkError::ArithmeticOverflow)?)
                        .ok_or(WorkError::ArithmeticOverflow)?;
                }
                Ok(result)
            };
            if declared.global.global.get(JointResource::ScopeRecords) != total_scopes
                || declared.lanes[JointLane::Custody as usize]
                    .global
                    .get(JointResource::ScopeRecords)
                    != total_scopes
                || !declared.global.global.covers(totals(count)?)
                || !declared.lanes[JointLane::Custody as usize]
                    .global
                    .covers(totals(count)?)
            {
                return Err(WorkError::InvalidConfig("fixed global custody inventory"));
            }
            for (class, n) in config.slots_per_class.into_iter().enumerate() {
                let n64 = u64::try_from(n).map_err(|_| WorkError::ArithmeticOverflow)?;
                if declared.classes[class]
                    .global
                    .get(JointResource::ScopeRecords)
                    != n64
                    || declared.class_lanes[class][JointLane::Custody as usize]
                        .global
                        .get(JointResource::ScopeRecords)
                        != n64
                    || !declared.classes[class].global.covers(totals(n)?)
                    || !declared.class_lanes[class][JointLane::Custody as usize]
                        .global
                        .covers(totals(n)?)
                {
                    return Err(WorkError::InvalidConfig("fixed class custody inventory"));
                }
            }
            state
                .next_id
                .checked_add(total_scopes)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let mut slots = Vec::new();
            slots
                .try_reserve_exact(count)
                .map_err(|_| WorkError::Capacity("fixed control inventory"))?;
            let mut claims = BTreeMap::new();
            let mut profiles: BTreeMap<Arc<ProfileKey>, ProfileRecord> = BTreeMap::new();
            let mut empty_facts = std::array::from_fn(|_| None);
            let mut free_heads = [None; 5];
            let classes = [
                WorkClass::Query,
                WorkClass::MaterializedView,
                WorkClass::Statistics,
                WorkClass::TableMaintenance,
                WorkClass::Management,
            ];
            for (class, n) in config.slots_per_class.into_iter().enumerate() {
                if n == 0 {
                    continue;
                }
                let facts = Arc::new(ProfileFacts {
                    key: Arc::new(ProfileKey {
                        class,
                        lane: JointLane::Custody as usize,
                        dependencies: Vec::new(),
                    }),
                    class: classes[class],
                    lane: JointLane::Custody,
                    dependencies: Arc::from(Vec::<Arc<DependencyRecord>>::new()),
                });
                empty_facts[class] = Some(Arc::clone(&facts));
                free_heads[class] = Some(SlotId(slots.len()));
                for offset in 0..n {
                    let cell =
                        crate::cancellation::Cancellation::preallocated(config.children_per_slot)?;
                    let claim = state.next_id()?;
                    let id = SlotId(slots.len());
                    slots.push(ControlSlot {
                        cell: Some(cell),
                        class: classes[class],
                        phase: SlotPhase::Free,
                        generation: 0,
                        claim: Some(claim),
                        base,
                        parent: None,
                        next_free: (offset + 1 < n).then_some(SlotId(id.0 + 1)),
                    });
                    claims.insert(
                        claim,
                        ClaimRecord {
                            owner: ClaimOwner::ControlSlot(id),
                            facts: Arc::clone(&facts),
                            amounts: base,
                            active_facts: Arc::clone(&facts),
                            active_amounts: base,
                            pending: None,
                            structure_revision: 0,
                            release_revision: 0,
                        },
                    );
                    let profile = profiles
                        .entry(Arc::clone(&facts.key))
                        .or_insert(ProfileRecord {
                            facts: Arc::clone(&facts),
                            amounts: JointResources::ZERO,
                            claims: 0,
                        });
                    profile.amounts = profile.amounts.checked_add(base)?;
                    profile.claims += 1;
                }
            }
            let ledger = state.joint.as_mut().unwrap();
            ledger.claims = claims;
            ledger.profiles = profiles;
            ledger.control_empty_facts = empty_facts;
            state.cancellations = CancellationStorage {
                slots,
                fixed: true,
                retired: 0,
                free_heads,
                legacy_vacant: None,
            };
            Ok(())
        })
    }
}

impl JointLedger {
    pub(crate) fn control_is_empty(&self, claim: u64) -> bool {
        self.claims.get(&claim).is_some_and(|record| {
            record.facts.dependencies.is_empty()
                && record.active_facts.dependencies.is_empty()
                && record.pending.is_none()
        })
    }

    pub(crate) fn activate_control_slot(
        &mut self,
        slot: SlotId,
        claim: u64,
    ) -> Result<(), WorkError> {
        let record = self.claims.get_mut(&claim).ok_or(WorkError::Released)?;
        if self.driver_closed
            || record.owner != ClaimOwner::ControlSlot(slot)
            || record.pending.is_some()
        {
            return Err(WorkError::Conflict);
        }
        record.structure_revision = record
            .structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(())
    }

    pub(crate) fn reset_control_slot(&mut self, slot: SlotId, claim: u64, base: JointResources) {
        let record = self.claims.get_mut(&claim).expect("original fixed claim");
        assert_eq!(record.owner, ClaimOwner::ControlSlot(slot));
        assert!(
            record.pending.is_none(),
            "collected control slot has no pending reply"
        );
        let from = Arc::clone(&record.facts);
        let amount = record.amounts;
        let to = Arc::clone(
            self.control_empty_facts[class_index(record.facts.class)]
                .as_ref()
                .unwrap(),
        );
        record.facts = Arc::clone(&to);
        record.active_facts = Arc::clone(&to);
        record.amounts = base;
        record.active_amounts = base;
        match record.structure_revision.checked_add(1) {
            Some(revision) => record.structure_revision = revision,
            None => self.driver_closed = true,
        }
        subtract_profile(self, &from, amount);
        add_profile(self, Arc::clone(&to), base);
        if from.key != to.key {
            self.layout_changed();
        }
        self.released();
    }
}

#[cfg(test)]
impl JointLedger {
    pub(crate) fn control_snapshot_for_test(&self) -> (usize, usize, JointResources, usize) {
        let mut amount = JointResources::ZERO;
        let mut records = 0;
        let mut bound = 0;
        for record in self.claims.values() {
            if matches!(record.owner, ClaimOwner::ControlSlot(_)) {
                amount = amount.checked_add(record.amounts).unwrap();
                records += 1;
                bound += usize::from(!record.facts.dependencies.is_empty());
            }
        }
        (
            records,
            self.profiles.values().map(|profile| profile.claims).sum(),
            amount,
            bound,
        )
    }

    pub(crate) fn control_revision_for_test(&self, claim: u64) -> u64 {
        self.claims[&claim].structure_revision
    }

    /// Establish an existing domain-bound fixture without pretending that this
    /// test setup is the still-unimplemented production FirstBind protocol.
    pub(crate) fn bind_control_fixture(&mut self, slot: SlotId, claim: u64, ordinal: u64) {
        let record = &self.claims[&claim];
        assert_eq!(record.owner, ClaimOwner::ControlSlot(slot));
        assert!(record.facts.dependencies.is_empty());
        let from = Arc::clone(&record.facts);
        let amount = record.amounts;
        let dependency = Arc::new(DependencyRecord {
            ordinal,
            fact: SharedDependency::LocalHost,
        });
        let to = Arc::new(ProfileFacts {
            key: Arc::new(ProfileKey {
                class: class_index(from.class),
                lane: from.lane as usize,
                dependencies: vec![ordinal],
            }),
            class: from.class,
            lane: from.lane,
            dependencies: Arc::from(vec![dependency]),
        });
        subtract_profile(self, &from, amount);
        add_profile(self, Arc::clone(&to), amount);
        let record = self.claims.get_mut(&claim).unwrap();
        record.facts = Arc::clone(&to);
        record.active_facts = to;
        record.structure_revision += 1;
        self.layout_changed();
    }
}
