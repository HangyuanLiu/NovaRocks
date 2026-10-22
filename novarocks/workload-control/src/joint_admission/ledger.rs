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

#[derive(Clone)]
pub(super) struct ClaimRecord {
    pub(super) owner: WorkId,
    pub(super) class: WorkClass,
    pub(super) lane: JointLane,
    pub(super) dependencies: Arc<[Arc<DependencyRecord>]>,
    pub(super) amounts: JointResources,
}

#[derive(Clone)]
pub(super) struct ChangeRecord {
    pub(super) old: Option<u64>,
    pub(super) original_amounts: Option<JointResources>,
    pub(super) claim: ClaimRecord,
}

pub(super) struct RequestRecord {
    pub(super) owner: WorkId,
    pub(super) changes: Arc<[ChangeRecord]>,
    pub(super) result: Option<Result<Vec<u64>, WorkError>>,
    pub(super) blocked_at: Option<u64>,
    pub(super) waker: Option<Waker>,
}

pub(crate) struct JointLedger {
    pub(super) config: Arc<ValidatedJointCapacityConfig>,
    dependencies: BTreeMap<SharedDependency, Weak<DependencyRecord>>,
    pub(super) claims: BTreeMap<u64, ClaimRecord>,
    pub(super) requests: BTreeMap<u64, RequestRecord>,
    /// A checked, nonwrapping release epoch; never a proof of actual exit.
    pub(super) release_epoch: u64,
    pub(super) active: Option<u64>,
    pub(super) driver_closed: bool,
}

impl JointLedger {
    pub(crate) fn is_drained(&self) -> bool {
        self.claims.is_empty() && self.requests.is_empty() && self.active.is_none()
    }
    pub(super) fn released(&mut self) {
        match self.release_epoch.checked_add(1) {
            Some(epoch) => self.release_epoch = epoch,
            // Refuse all subsequent positive growth rather than reuse an epoch.
            None => self.driver_closed = true,
        }
    }
}

impl WorkloadControl {
    /// Install once before readiness. The unique driver belongs to the role's
    /// supervised progression owner; waiting callers do not drive admission.
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
                requests: BTreeMap::new(),
                release_epoch: 0,
                active: None,
                driver_closed: false,
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

    /// Queue a complete batch in the original authority. Old capabilities move
    /// into the reply owner until one atomic verdict; their immutable class,
    /// lane and dependency provenance cannot be supplied again by the caller.
    pub fn request(
        &self,
        scope: &WorkScope,
        changes: Vec<JointClaimChange>,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        let build = || -> Result<Arc<[ChangeRecord]>, WorkError> {
            if !Arc::ptr_eq(&self.inner, &scope.inner) {
                return Err(WorkError::ForeignAuthority);
            }
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            let config = &ledger.config.config;
            if changes.is_empty() || changes.len() > config.max_changes_per_request {
                return Err(WorkError::Capacity("joint batch records"));
            }
            let mut records = Vec::with_capacity(changes.len());
            for change in &changes {
                let (old, claim) = match change {
                    JointClaimChange::New {
                        dependencies,
                        lane,
                        amounts,
                    } => {
                        if dependencies.len() > config.max_dependencies_per_claim {
                            return Err(WorkError::Capacity("joint dependency fanout"));
                        }
                        let mut selected = Vec::with_capacity(dependencies.len());
                        for dependency in dependencies {
                            if !Arc::ptr_eq(&self.inner, &dependency.inner) {
                                return Err(WorkError::ForeignAuthority);
                            }
                            selected.push(Arc::clone(&dependency.record));
                        }
                        selected.sort_unstable_by_key(|record| record.ordinal);
                        if selected
                            .windows(2)
                            .any(|pair| pair[0].ordinal == pair[1].ordinal)
                        {
                            return Err(WorkError::Conflict);
                        }
                        (
                            None,
                            ClaimRecord {
                                owner: scope.id,
                                class: node.class,
                                lane: *lane,
                                dependencies: selected.into(),
                                amounts: *amounts,
                            },
                        )
                    }
                    JointClaimChange::Replace { original, amounts } => {
                        if !Arc::ptr_eq(&self.inner, &original.inner) {
                            return Err(WorkError::ForeignAuthority);
                        }
                        let id = original.id.ok_or(WorkError::Released)?;
                        let old = ledger.claims.get(&id).ok_or(WorkError::Released)?;
                        if old.owner != scope.id
                            || records.iter().any(|r: &ChangeRecord| r.old == Some(id))
                        {
                            return Err(WorkError::Conflict);
                        }
                        let mut claim = old.clone();
                        claim.amounts = *amounts;
                        (Some(id), claim)
                    }
                };
                let original_amounts = old.map(|id| ledger.claims[&id].amounts);
                records.push(ChangeRecord {
                    old,
                    original_amounts,
                    claim,
                });
            }
            Ok(records.into())
        };
        let records = match build() {
            Ok(records) => records,
            Err(error) => {
                return Err(JointAdmissionFailure {
                    error,
                    originals: originals(changes),
                });
            }
        };
        let queued = self.inner.update(|state| {
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            // Settlement remains possible after cancellation/admission closure.
            if records
                .iter()
                .any(|record| record.claim.lane != JointLane::Settlement)
            {
                node.check()?;
            }
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if state.closed
                && records
                    .iter()
                    .any(|record| record.claim.lane != JointLane::Settlement)
            {
                return Err(WorkError::Closed);
            }
            if ledger.requests.len() >= ledger.config.config.max_requests {
                return Err(WorkError::Capacity("joint request and reply records"));
            }
            // Every original is still live and exclusively held by `changes`.
            for record in records.iter() {
                if let Some(id) = record.old {
                    if !ledger.claims.contains_key(&id) {
                        return Err(WorkError::Released);
                    }
                }
            }
            let waiters = node
                .resource_waiters
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let id = state.next_id()?;
            state.nodes.get_mut(&scope.id).unwrap().resource_waiters = waiters;
            state.joint.as_mut().unwrap().requests.insert(
                id,
                RequestRecord {
                    owner: scope.id,
                    changes: records,
                    result: None,
                    blocked_at: None,
                    waker: None,
                },
            );
            Ok(id)
        });
        match queued {
            Ok(id) => Ok(JointAdmission {
                inner: Arc::clone(&self.inner),
                id: Some(id),
                originals: originals(changes),
            }),
            Err(error) => Err(JointAdmissionFailure {
                error,
                originals: originals(changes),
            }),
        }
    }
}

pub enum JointClaimChange {
    New {
        dependencies: Vec<JointDependency>,
        lane: JointLane,
        amounts: JointResources,
    },
    Replace {
        original: JointClaim,
        amounts: JointResources,
    },
}
fn originals(changes: Vec<JointClaimChange>) -> Vec<JointClaim> {
    changes
        .into_iter()
        .filter_map(|change| match change {
            JointClaimChange::Replace { original, .. } => Some(original),
            JointClaimChange::New { .. } => None,
        })
        .collect()
}

/// One noncloneable physical/custody reservation. Its actual resource owner
/// retains it through real completion. Merely publishing a logical result does
/// not drop this capability. Shared backing uses an Arc of its real owner.
pub struct JointClaim {
    pub(super) inner: Arc<Inner>,
    pub(super) id: Option<u64>,
}
impl std::fmt::Debug for JointClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JointClaim")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
impl JointClaim {
    pub fn amounts(&self) -> Result<JointResources, WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        let state = self.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .and_then(|ledger| ledger.claims.get(&id))
            .map(|record| record.amounts)
            .ok_or(WorkError::Released)
    }
    /// Only a real owner's observed release calls this operation. No positive
    /// growth, class change or dependency change can enter through it.
    pub fn release_part(&mut self, released: JointResources) -> Result<(), WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        self.inner.update(|state| {
            let ledger = state.joint.as_mut().ok_or(WorkError::NotReady)?;
            let record = ledger.claims.get_mut(&id).ok_or(WorkError::Released)?;
            record.amounts = record.amounts.checked_sub(released)?;
            ledger.released();
            Ok(())
        })
    }
}
impl Drop for JointClaim {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.inner.update(|state| release_claim(state, id));
        }
    }
}
pub(super) fn release_claim(state: &mut State, id: u64) {
    if let Some(record) = state
        .joint
        .as_mut()
        .and_then(|ledger| ledger.claims.remove(&id))
    {
        state.joint.as_mut().unwrap().released();
        state.nodes.get_mut(&record.owner).unwrap().resource_holders -= 1;
        state.collect(record.owner);
    }
}

/// Rejection returns all original capabilities; a capacity wait never implies
/// their release or cancellation of the originating work.
pub struct JointAdmissionFailure {
    pub error: WorkError,
    pub originals: Vec<JointClaim>,
}
impl std::fmt::Debug for JointAdmissionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JointAdmissionFailure")
            .field("error", &self.error)
            .field("originals", &self.originals.len())
            .finish()
    }
}

#[must_use = "Admission must be awaited or explicitly dropped"]
pub struct JointAdmission {
    inner: Arc<Inner>,
    id: Option<u64>,
    originals: Vec<JointClaim>,
}
impl Future for JointAdmission {
    type Output = Result<Vec<JointClaim>, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id.expect("completed joint admission polled again");
        let result = self.inner.update(|state| {
            let request = state
                .joint
                .as_mut()
                .unwrap()
                .requests
                .get_mut(&id)
                .expect("live reply record");
            if let Some(result) = request.result.take() {
                let request = state.joint.as_mut().unwrap().requests.remove(&id).unwrap();
                state
                    .nodes
                    .get_mut(&request.owner)
                    .unwrap()
                    .resource_waiters -= 1;
                state.collect(request.owner);
                Some(result)
            } else {
                if request
                    .waker
                    .as_ref()
                    .is_none_or(|old| !old.will_wake(cx.waker()))
                {
                    request.waker = Some(cx.waker().clone());
                }
                None
            }
        });
        match result {
            None => Poll::Pending,
            Some(result) => {
                self.id = None;
                match result {
                    Ok(ids) => {
                        for original in &mut self.originals {
                            original.id = None;
                        }
                        self.originals.clear();
                        Poll::Ready(Ok(ids
                            .into_iter()
                            .map(|id| JointClaim {
                                inner: Arc::clone(&self.inner),
                                id: Some(id),
                            })
                            .collect()))
                    }
                    Err(error) => Poll::Ready(Err(JointAdmissionFailure {
                        error,
                        originals: std::mem::take(&mut self.originals),
                    })),
                }
            }
        }
    }
}
impl Drop for JointAdmission {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.inner.update(|state| {
                if let Some(request) = state.joint.as_mut().unwrap().requests.remove(&id) {
                    if let Some(Ok(ids)) = request.result {
                        for claim in ids {
                            release_claim(state, claim);
                        }
                    }
                    state
                        .nodes
                        .get_mut(&request.owner)
                        .unwrap()
                        .resource_waiters -= 1;
                    state.collect(request.owner);
                }
            });
        }
        // Original holder drops happen after the authority lock is released.
    }
}
