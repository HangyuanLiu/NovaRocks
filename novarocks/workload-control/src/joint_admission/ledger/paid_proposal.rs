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

use super::*;
use crate::scope::ReclaimWakeOnExit;

use novarocks_resource_contract::SelectedNetworkOrigin;
mod async_action;
mod control_action;
pub use async_action::{
    AsyncActionAdmission, AsyncActionGrant, AsyncActionSource, RootPayloadInitialAdmission,
    RootPayloadInitialSource,
};
pub(super) mod native_action;
pub(super) use native_action::{NativeActionFence, validate_native_action};
pub use native_action::{
    NativeAsyncActionAdmission, NativeAsyncActionAuthority, NativeAsyncActionQuote,
    NativeAsyncActionSource, NativeQueuedInputAdmission, NativeQueuedInputQuote,
    NativeQueuedInputReady,
};
pub(crate) use native_action::{capture_native_action, delegate_native_actions};
mod blocking_action;
pub use blocking_action::{BlockingActionAdmission, BlockingActionSource};
pub(super) use control_action::{ControlActionFence, validate_control_action};
mod cpu_action;
pub(super) use cpu_action::{CpuActionFence, capture as capture_cpu_action, validate_cpu_action};
pub use cpu_action::{
    InternalCpuActionAdmission, InternalCpuActionSource, InternalCpuFactoryBinding,
    InternalResultClosedFactoryDescriptor,
};
mod production_growth;
pub(super) use production_growth::{production_change_count, validate_control_productions};
mod root_payload;
pub use root_payload::{
    RootPayloadAdmission, RootPayloadProposal, RootPayloadReady, RootPayloadSource,
};
pub(super) use root_payload::{RootPayloadFence, validate_root_payload};
mod root_stage;
pub(super) use root_stage::validate_root_stage;
pub use root_stage::{RootStageAdmission, RootStageProposal, RootStageReady, RootStageSource};

/// Closed actual authors: a Provider-owned fact or its borrowed selected route.
/// Neither variant can run an external constructor under the WC mutex.
enum DependencyInput<'a> {
    Fact(&'a SharedDependency),
    Selected(SelectedNetworkOrigin<'a>),
}
impl DependencyInput<'_> {
    fn matches(&self, fact: &SharedDependency) -> bool {
        match (self, fact) {
            (Self::Fact(selected), fact) => *selected == fact,
            (Self::Selected(selected), SharedDependency::Network(fact)) => {
                fact.matches_selected(*selected)
            }
            (Self::Selected(_), SharedDependency::LocalHost | SharedDependency::Hdfs(_)) => false,
        }
    }
    fn into_owned(self) -> SharedDependency {
        match self {
            Self::Fact(fact) => fact.clone(),
            Self::Selected(selected) => SharedDependency::Network(selected.into_owned()),
        }
    }
}

struct PendingChange {
    source: Option<JointClaimView>,
    original: Option<(Arc<ProfileFacts>, JointResources)>,
    selected: Vec<DependencyRef>,
    lane: JointLane,
    amounts: JointResources,
    additional: usize,
    first_bind: bool,
}

fn track_dependency_union(
    union: &mut Vec<u64>,
    dependencies: &[DependencyRef],
    maximum: usize,
) -> Result<(), WorkError> {
    for record in dependencies {
        if let Err(position) = union.binary_search(&record.ordinal) {
            if union.len() >= maximum {
                return Err(WorkError::Capacity("joint request dependency union"));
            }
            union.insert(position, record.ordinal);
        }
    }
    Ok(())
}

/// One original prepaid proposal position, including its construction phase.
/// Any error poisons the proposal. It cannot submit a silently partial batch.
/// Free registry handles and caller-vector request inputs are test harness only.
///
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::{JointClaimChange, JointDependency};
/// ```
///
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::JointAdmissionHandle;
/// use novarocks_resource_contract::SharedDependency;
/// fn bypass(handle: &JointAdmissionHandle) {
///     let _ = handle.register_dependency(SharedDependency::LocalHost);
/// }
/// ```
#[must_use = "A proposal must be submitted or dropped"]
pub struct JointProposal {
    root_stage: Option<RootStageFence>,
    root_payload: Option<RootPayloadFence>,
    cpu_action: Option<CpuActionFence>,
    control_action: Option<ControlActionFence>,
    native_action: Option<NativeActionFence>,
    records: Vec<ChangeRecord>,
    // The complete source union, shared by every change in this one paid
    // request. Ordinals grant no access and retain no canonical fact alias.
    dependency_union: Vec<u64>,
    current: Option<PendingChange>,
    custody: Option<ProposalConstruction>,
    scope: WorkScope,
    config: Arc<ValidatedJointCapacityConfig>,
    class: WorkClass,
    started: usize,
    failed: Option<WorkError>,
    // All reference graphs and custody exit before this lock-free observer.
    _exit: ReclaimWakeOnExit,
}

impl JointAdmissionHandle {
    /// Reserve an original fixed cell before allocating any request backing.
    /// This does not admit execution, I/O, jobs or a physical queue entry.
    pub fn begin_paid_proposal(&self, owner: &WorkOwner) -> Result<JointProposal, WorkError> {
        self.begin_paid_proposal_for_scope(owner.scope())
    }

    // Scope callers reach this through closed owner-delegated wrappers.
    pub(super) fn begin_paid_proposal_for_scope(
        &self,
        scope: WorkScope,
    ) -> Result<JointProposal, WorkError> {
        let custody = self.reserve_proposal(&scope)?;
        let Some(custody) = custody else {
            return Err(WorkError::NotReady);
        };
        self.paid_proposal_with_custody(scope, custody)
    }

    pub(super) fn try_begin_paid_proposal_for_scope(
        &self,
        scope: WorkScope,
    ) -> Result<JointTryBegin<JointProposal>, WorkError> {
        match self.try_reserve_proposal(&scope)? {
            ProposalReservation::Reserved(Some(custody)) => self
                .paid_proposal_with_custody(scope, custody)
                .map(JointTryBegin::Started),
            ProposalReservation::Reserved(None) => Err(WorkError::NotReady),
            ProposalReservation::Full(target) => Ok(JointTryBegin::Full(
                JointProposalSlotWait::new(Arc::downgrade(&self.inner), target),
            )),
        }
    }

    fn paid_proposal_with_custody(
        &self,
        scope: WorkScope,
        custody: ProposalConstruction,
    ) -> Result<JointProposal, WorkError> {
        let (config, class) = {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if !state.cancellations.fixed || ledger.dependencies.is_none() {
                return Err(WorkError::NotReady);
            }
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            (Arc::clone(&ledger.config), node.class)
        };
        let mut proposal = JointProposal {
            root_stage: None,
            root_payload: None,
            cpu_action: None,
            control_action: None,
            native_action: None,
            records: Vec::new(),
            dependency_union: Vec::new(),
            current: None,
            custody: Some(custody),
            scope,
            config,
            class,
            started: 0,
            failed: None,
            _exit: ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
        };
        proposal
            .records
            .try_reserve_exact(proposal.config.config.max_changes_per_request)
            .map_err(|_| WorkError::Capacity("paid joint change backing"))?;
        proposal
            .dependency_union
            .try_reserve_exact(proposal.config.config.max_dependencies_per_claim)
            .map_err(|_| WorkError::Capacity("paid joint request union backing"))?;
        Ok(proposal)
    }

    pub(super) fn capture_adjustment(
        &self,
        scope: &WorkScope,
        source: &JointClaimView,
        delta: JointResources,
        first_bind: bool,
    ) -> Result<(Arc<ProfileFacts>, JointResources), WorkError> {
        if !Weak::ptr_eq(&source.inner, &Arc::downgrade(&self.inner)) {
            return Err(WorkError::ForeignAuthority);
        }
        let state = self.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
        let old = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
        authenticate_scope(&state, old, source, scope.id)?;
        if old.pending.is_some() || !old.active_facts.key.causal.is_empty() {
            return Err(WorkError::Conflict);
        }
        match (source.source, first_bind) {
            (ClaimSource::Physical, false) => {}
            (ClaimSource::Payload { .. }, false)
                if delta
                    == JointResources::ZERO.with(
                        JointResource::PayloadBytes,
                        delta.get(JointResource::PayloadBytes),
                    ) => {}
            (ClaimSource::NodeControl { slot, .. }, true)
                if old.active_facts.dependencies.is_empty()
                    && old.active_amounts == state.cancellations.slots[slot.0].base => {}
            (ClaimSource::NodeControl { slot, .. }, false)
                if delta == JointResources::ZERO
                    && !old.active_facts.dependencies.is_empty()
                    && old.active_amounts == state.cancellations.slots[slot.0].base => {}
            _ => return Err(WorkError::Conflict),
        }
        old.structure_revision
            .checked_add(2)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok((Arc::clone(&old.active_facts), old.active_amounts))
    }
}

impl JointProposal {
    fn healthy(&self) -> Result<(), WorkError> {
        self.failed.as_ref().map_or(Ok(()), |e| Err(e.clone()))
    }
    fn remember(&mut self, result: Result<(), WorkError>) -> Result<(), WorkError> {
        self.failed = result.as_ref().err().cloned();
        result
    }
    fn arm(&mut self) -> Result<(), WorkError> {
        self.healthy()?;
        // A caller may catch a panic from an arbitrary post-State wake. Until
        // that wake returns, submitting this potentially partial call is barred.
        self.failed = Some(WorkError::Conflict);
        Ok(())
    }
    fn begin(
        &mut self,
        source: Option<JointClaimView>,
        lane: JointLane,
        amounts: JointResources,
        first_bind: bool,
    ) -> Result<(), WorkError> {
        if self.current.is_some() {
            return Err(WorkError::Conflict);
        }
        if self.started >= self.config.config.max_changes_per_request {
            return Err(WorkError::Capacity("paid joint batch records"));
        }
        self.started += 1;
        let original = if let Some(source) = &source {
            if self
                .records
                .iter()
                .any(|record| record.old == Some(source.id))
            {
                return Err(WorkError::Conflict);
            }
            Some(
                JointAdmissionHandle {
                    inner: Arc::clone(&self.scope.inner),
                }
                .capture_adjustment(
                    &WorkScope {
                        inner: Arc::clone(&self.scope.inner),
                        id: source_work(self.scope.id, self.root_stage, source),
                    },
                    source,
                    amounts,
                    first_bind,
                )?,
            )
        } else {
            None
        };
        if let Some((facts, _)) = &original {
            // Adjustment retains its complete old union. Check it before
            // allocating another selected set or creating any new facts.
            track_dependency_union(
                &mut self.dependency_union,
                &facts.dependencies,
                self.config.config.max_dependencies_per_claim,
            )?;
        }
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(self.config.config.max_dependencies_per_claim)
            .map_err(|_| WorkError::Capacity("paid joint dependency backing"))?;
        let (lane, amounts) = if let Some((facts, old_amounts)) = &original {
            // This is the complete old set, not only newly selected domains.
            assert!(facts.dependencies.len() <= self.config.config.max_dependencies_per_claim);
            selected.extend(facts.dependencies.iter().cloned());
            (facts.lane, old_amounts.checked_add(amounts)?)
        } else {
            (lane, amounts)
        };
        if amounts.get(JointResource::InternalCpuJobs) != 0 && self.cpu_action.is_none() {
            return Err(WorkError::Conflict);
        }
        self.current = Some(PendingChange {
            source,
            original,
            selected,
            lane,
            amounts,
            additional: 0,
            first_bind,
        });
        Ok(())
    }
    pub fn begin_new(&mut self, lane: JointLane, amounts: JointResources) -> Result<(), WorkError> {
        self.healthy()?;
        let _exit = ReclaimWakeOnExit::observe(&Arc::downgrade(&self.scope.inner));
        let result = self
            .arm()
            .and_then(|_| self.begin(None, lane, amounts, false));
        self.remember(result)
    }
    pub fn begin_grow(
        &mut self,
        source: JointClaimView,
        delta: JointResources,
    ) -> Result<(), WorkError> {
        self.healthy()?;
        let _exit = ReclaimWakeOnExit::observe(&Arc::downgrade(&self.scope.inner));
        let result = self
            .arm()
            .and_then(|_| self.begin(Some(source), JointLane::Custody, delta, false));
        self.remember(result)
    }
    pub fn begin_first_bind(&mut self, source: JointClaimView) -> Result<(), WorkError> {
        self.healthy()?;
        let _exit = ReclaimWakeOnExit::observe(&Arc::downgrade(&self.scope.inner));
        let result = self
            .arm()
            .and_then(|_| self.begin(Some(source), JointLane::Custody, JointResources::ZERO, true));
        self.remember(result)
    }

    /// Borrow an actual selected fact. The original Provider/caller owns its
    /// input parsing/backing; this service owns the bounded canonical output.
    pub fn push_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        // A callback panic can drop a newly returned reference before it enters
        // selected while the caller catches unwind and retains this builder.
        // This per-call observer is after that actual exit, outside both locks.
        self.healthy()?;
        let _exit = ReclaimWakeOnExit::observe(&Arc::downgrade(&self.scope.inner));
        let result = self
            .arm()
            .and_then(|_| self.push(DependencyInput::Fact(fact)));
        self.remember(result)
    }
    /// Construct a canonical network fact within this paid body. The Provider
    /// supplies an allocation-free view of its already parsed, selected route.
    /// Duplicate and full-union checks precede any origin output allocation.
    pub fn push_selected_network(
        &mut self,
        selected: SelectedNetworkOrigin<'_>,
    ) -> Result<(), WorkError> {
        self.healthy()?;
        let _exit = ReclaimWakeOnExit::observe(&Arc::downgrade(&self.scope.inner));
        let result = self
            .arm()
            .and_then(|_| self.push(DependencyInput::Selected(selected)));
        self.remember(result)
    }
    fn push(&mut self, input: DependencyInput<'_>) -> Result<(), WorkError> {
        let change = self.current.as_mut().ok_or(WorkError::Conflict)?;
        let k = self.config.config.max_dependencies_per_claim;
        if change.additional >= k {
            return Err(WorkError::Capacity("paid joint dependency calls"));
        }
        change.additional += 1;
        if change
            .selected
            .iter()
            .any(|record| input.matches(&record.fact))
        {
            return if change.source.is_some() {
                Ok(())
            } else {
                Err(WorkError::Conflict)
            };
        }
        if change.selected.len() >= k {
            return Err(WorkError::Capacity("joint dependency union"));
        }
        if self.dependency_union.len() >= k
            && !self.records.iter().any(|record| {
                record
                    .claim
                    .facts
                    .dependencies
                    .iter()
                    .any(|record| input.matches(&record.fact))
            })
        {
            // All earlier members remain in records/current. A new domain
            // cannot consume canonical inventory merely by starting another
            // change of the same logical source. Never truncate its full D.
            return Err(WorkError::Capacity("joint request dependency union"));
        }
        let fact = input.into_owned();
        let record = self.scope.inner.update(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            let node = state.nodes.get(&self.scope.id).ok_or(WorkError::Released)?;
            if change.lane != JointLane::Settlement {
                node.check(&state.cancellations)?;
                if state.closed {
                    return Err(WorkError::Closed);
                }
            }
            let registry = state
                .joint
                .as_mut()
                .unwrap()
                .dependencies
                .as_mut()
                .ok_or(WorkError::NotReady)?;
            if let Some(record) = registry.lookup(&fact) {
                return Ok(record);
            }
            // A provisional identity does not change numeric profiles, layout,
            // charges, pending claims or an in-flight proof's input.
            let ordinal = state.next_id()?;
            state
                .joint
                .as_mut()
                .unwrap()
                .dependencies
                .as_mut()
                .unwrap()
                .insert(fact.clone(), ordinal)
        })?;
        if let Err(position) = self.dependency_union.binary_search(&record.ordinal) {
            assert!(self.dependency_union.len() < self.dependency_union.capacity());
            self.dependency_union.insert(position, record.ordinal);
        }
        let position = change
            .selected
            .binary_search_by_key(&record.ordinal, |record| record.ordinal)
            .expect_err("distinct canonical facts have distinct ordinals");
        assert!(change.selected.len() < change.selected.capacity());
        change.selected.insert(position, record);
        Ok(())
    }
    pub fn end_change(&mut self) -> Result<(), WorkError> {
        self.healthy()?;
        let _exit = ReclaimWakeOnExit::observe(&Arc::downgrade(&self.scope.inner));
        let result = self.arm().and_then(|_| self.end());
        self.remember(result)
    }
    fn end(&mut self) -> Result<(), WorkError> {
        // Closed action authors can copy an already-paid original set without
        // minting origins through push. They obey this SAME request bound
        // before a new immutable graph is formed or any body can be queued.
        track_dependency_union(
            &mut self.dependency_union,
            &self.current.as_ref().ok_or(WorkError::Conflict)?.selected,
            self.config.config.max_dependencies_per_claim,
        )?;
        let change = self.current.take().ok_or(WorkError::Conflict)?;
        if change.first_bind && change.selected.is_empty() {
            return Err(WorkError::Conflict);
        }
        let facts = Arc::new(ProfileFacts {
            key: Arc::new(ProfileKey {
                class: class_index(self.class),
                lane: change.lane as usize,
                dependencies: change.selected.iter().map(|d| d.ordinal).collect(),
                causal: if self
                    .cpu_action
                    .is_some_and(CpuActionFence::has_closed_factory)
                {
                    CausalMask::INTERNAL_CPU_ACTION
                } else {
                    CausalMask::NONE
                },
            }),
            class: self.class,
            lane: change.lane,
            // Pinned Arc::from(Vec) copies into the exact Arc slice before
            // releasing this capacity-K Vec. No Box/shrink author intervenes.
            dependencies: change.selected.into(),
        });
        assert!(self.records.len() < self.records.capacity());
        self.records.push(ChangeRecord {
            old: change.source.as_ref().map(|source| source.id),
            original_amounts: change.original.as_ref().map(|(_, amounts)| *amounts),
            original_facts: change.original.map(|(facts, _)| facts),
            source: change.source,
            claim: ProposedClaim {
                facts,
                amounts: change.amounts,
            },
        });
        Ok(())
    }

    /// One cumulative finite proof, including all layout reobservations. A
    /// starting/self-held owner never anonymously parks on its own resources.
    pub fn submit(self) -> Result<JointAdmission, JointAdmissionFailure> {
        let limit = self.config.config.max_total_proof_work;
        self.submit_with_budget(limit)
    }
    pub fn submit_finite(
        self,
        limit: std::num::NonZeroU64,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        self.submit_with_budget(limit.get())
    }
    fn submit_with_budget(self, limit: u64) -> Result<JointAdmission, JointAdmissionFailure> {
        self.submit_with_budget_mode(limit, false)
    }
    pub(super) fn submit_control_binding(
        self,
        limit: Option<std::num::NonZeroU64>,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        let limit = limit.map_or(self.config.config.max_total_proof_work, |n| n.get());
        self.submit_with_budget_mode(limit, true)
    }
    fn submit_with_budget_mode(
        mut self,
        limit: u64,
        control_only: bool,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        let limit = limit.min(self.config.config.max_total_proof_work);
        let result = (|| {
            self.healthy()?;
            if self.current.is_some() || self.records.is_empty() {
                return Err(WorkError::Conflict);
            }
            self.append_control_productions()?;
            let handle = JointAdmissionHandle {
                inner: Arc::clone(&self.scope.inner),
            };
            let changes =
                handle.store_proposal(&self.custody, std::mem::take(&mut self.records))?;
            // PreparedRequest owns body before constructor custody can exit.
            // Its queue author installs the original admission before any wake.
            handle.queue_request_mode(
                PreparedRequest {
                    root_stage: self.root_stage,
                    root_payload: self.root_payload,
                    cpu_action: self.cpu_action,
                    control_action: self.control_action,
                    native_action: self.native_action.take(),
                    budget: RequestBudget::Finite { remaining: limit },
                    changes,
                    custody: self.custody.take(),
                    scope: self.scope.clone(),
                    _exit: ReclaimWakeOnExit::observe(&Arc::downgrade(&self.scope.inner)),
                },
                control_only,
            )
        })();
        result.map_err(|error| JointAdmissionFailure { error })
    }
}

#[cfg(test)]
#[path = "paid_proposal_tests.rs"]
mod tests;
