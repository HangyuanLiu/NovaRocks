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

//! Closed action construction from the original positive computation grant.
//! This is permission to seek one joint action, never a physical reservation.
use super::*;
use std::{any::TypeId, num::NonZeroU64};

/// Identity of the existing physical pool allocation and its one closed call
/// factory. Only trusted first-party composition may describe this kind.
/// A descriptor is not a source-finiteness proof or a workload grant.
#[derive(Debug)]
pub struct InternalResultClosedFactoryDescriptor {
    pool: Weak<dyn Send + Sync>,
    call: TypeId,
    physical_queue_bytes: NonZeroU64,
}
impl InternalResultClosedFactoryDescriptor {
    pub(in crate::joint_admission) fn duplicate(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            call: self.call,
            physical_queue_bytes: self.physical_queue_bytes,
        }
    }
    /// Composition-only seam used by the original QA physical owner after it
    /// installs the exact first-party factory in that same pool. Arbitrary
    /// generic blocking work is not authorized by choosing this descriptor.
    pub fn for_composition(
        pool: Weak<dyn Send + Sync>,
        call: TypeId,
        physical_queue_bytes: NonZeroU64,
    ) -> Self {
        Self {
            pool,
            call,
            physical_queue_bytes,
        }
    }
    pub fn matches(&self, other: &Self) -> bool {
        self.call == other.call
            && self.physical_queue_bytes == other.physical_queue_bytes
            && Weak::ptr_eq(&self.pool, &other.pool)
    }
    pub(in crate::joint_admission) fn queue_bytes(&self) -> NonZeroU64 {
        self.physical_queue_bytes
    }
    pub(in crate::joint_admission) fn is_live(&self) -> bool {
        self.pool.strong_count() != 0
    }
    fn stamp(&self) -> CpuFactoryStamp {
        CpuFactoryStamp {
            // The original State keeps this Weak allocation identity alive.
            pool: self.pool.as_ptr() as *const () as usize,
            call: self.call,
            physical_queue_bytes: self.physical_queue_bytes,
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct CpuFactoryStamp {
    pool: usize,
    call: TypeId,
    physical_queue_bytes: NonZeroU64,
}
/// Narrow view of the original WC owner's before-Ready composition decision.
/// Weak identities retain neither a Work nor a physical pool or new budget.
pub struct InternalCpuFactoryBinding {
    inner: Weak<Inner>,
    descriptor: InternalResultClosedFactoryDescriptor,
}
impl Clone for InternalCpuFactoryBinding {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            descriptor: self.descriptor.duplicate(),
        }
    }
}
impl InternalCpuFactoryBinding {
    pub fn matches_descriptor(&self, descriptor: &InternalResultClosedFactoryDescriptor) -> bool {
        self.descriptor.matches(descriptor)
    }
    pub fn bind_source(
        &self,
        mut source: InternalCpuActionSource,
    ) -> Result<InternalCpuActionSource, WorkError> {
        if !Weak::ptr_eq(&self.inner, &source.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        let registered = state
            .internal_cpu_factory
            .as_ref()
            .ok_or(WorkError::NotReady)?;
        if !registered.matches(&self.descriptor) || registered.pool.strong_count() == 0 {
            return Err(WorkError::Released);
        }
        source.fence.factory = Some(registered.stamp());
        computation_facts(&state, source.owner, source.fence)?;
        Ok(source)
    }
}
impl WorkloadControl {
    /// Minimum global/domain limits for complete progress on this original
    /// registered source. Other sources' shared coordinates retain their
    /// original causal floor. Reading these facts grants no capacity.
    pub fn joint_capacity_floor_for_registered_sources(
        &mut self,
        minimum: JointResources,
        max_dependencies_per_claim: std::num::NonZeroUsize,
    ) -> Result<JointResourceLimits, WorkError> {
        let state = self.inner.state.lock().unwrap();
        if state.ready || state.closed || !state.nodes.is_empty() || state.joint.is_some() {
            return Err(WorkError::Conflict);
        }
        let descriptor = state
            .internal_cpu_factory
            .as_ref()
            .ok_or(WorkError::NotReady)?;
        if !descriptor.is_live() {
            return Err(WorkError::Released);
        }
        source_progress_floor(
            max_dependencies_per_claim.get(),
            minimum,
            minimum,
            Some(descriptor.queue_bytes()),
        )
    }

    /// Validate source-aware floors from this original authority's registered
    /// closed factory. Full charged dependencies and every hard cap remain.
    /// No caller-selected causal mask or arbitrary finite contribution enters.
    pub fn validate_joint_capacity_for_registered_sources(
        &mut self,
        config: JointCapacityConfig,
        required: &RequiredJointProgress,
    ) -> Result<ValidatedJointCapacityConfig, WorkError> {
        let state = self.inner.state.lock().unwrap();
        if state.ready || state.closed || !state.nodes.is_empty() || state.joint.is_some() {
            return Err(WorkError::Conflict);
        }
        let descriptor = state
            .internal_cpu_factory
            .as_ref()
            .ok_or(WorkError::NotReady)?;
        if descriptor.pool.strong_count() == 0 {
            return Err(WorkError::Released);
        }
        config.validate_registered_internal_cpu(
            required,
            descriptor.duplicate(),
            Arc::downgrade(&self.inner),
        )
    }
    /// Installs the one InternalResultClosedFactory on this original authority.
    /// Only first-party role composition owns both this unique owner and the
    /// physical CPU owner. The reviewed closed implementation, not its TypeId,
    /// establishes finite source semantics. No ordinary builder has this API.
    pub fn bind_internal_result_closed_factory_for_composition(
        &mut self,
        descriptor: InternalResultClosedFactoryDescriptor,
    ) -> Result<InternalCpuFactoryBinding, WorkError> {
        let mut state = self.inner.state.lock().unwrap();
        if state.ready || state.closed || state.internal_cpu_factory.is_some() {
            return Err(WorkError::Conflict);
        }
        if descriptor.pool.strong_count() == 0 {
            return Err(WorkError::Released);
        }
        state.internal_cpu_factory = Some(descriptor.duplicate());
        Ok(InternalCpuFactoryBinding {
            inner: Arc::downgrade(&self.inner),
            descriptor,
        })
    }
}

#[derive(Clone, Copy)]
pub(in crate::joint_admission::ledger) struct CpuActionFence {
    computation: u64,
    epoch: u64,
    structure_revision: u64,
    factory: Option<CpuFactoryStamp>,
}

/// The same finite request and paid proposal cell. Its only receiving path
/// requires a real local reservation and the original stable physical owner.
/// Aliases and caller-created domain vectors cannot construct this source.
#[must_use = "Keep the original admission in the stable CPU job owner"]
pub struct InternalCpuActionAdmission {
    admission: JointAdmission,
}
impl CpuActionFence {
    pub(super) fn has_closed_factory(self) -> bool {
        self.factory.is_some()
    }
    pub(in crate::joint_admission::ledger) fn descriptor(
        self,
        state: &State,
    ) -> Option<InternalResultClosedFactoryDescriptor> {
        self.factory.and_then(|stamp| {
            state
                .internal_cpu_factory
                .as_ref()
                .filter(|descriptor| descriptor.stamp() == stamp)
                .map(InternalResultClosedFactoryDescriptor::duplicate)
        })
    }
}
impl InternalCpuActionAdmission {
    pub fn poll_local_offer(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<JointLocalOffer<'_>, JointAdmissionFailure>> {
        self.admission.poll_local_offer(cx)
    }
}

fn computation_facts(
    state: &State,
    owner: WorkId,
    fence: CpuActionFence,
) -> Result<&Arc<ProfileFacts>, WorkError> {
    if let Some(factory) = fence.factory {
        let registered = state
            .internal_cpu_factory
            .as_ref()
            .ok_or(WorkError::NotReady)?;
        if registered.stamp() != factory || registered.pool.strong_count() == 0 {
            return Err(WorkError::Released);
        }
    }
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let node = state.nodes.get(&owner).ok_or(WorkError::Released)?;
    if node.owner != crate::OwnerState::Active {
        return Err(WorkError::Released);
    }
    let row = ledger
        .claims
        .get(&fence.computation)
        .ok_or(WorkError::Released)?;
    if row.owner != ClaimOwner::PhysicalWork(owner)
        || !matches!(row.custody, ClaimCustody::Computation { epoch, pins } if epoch == fence.epoch && pins != 0)
        || row.structure_revision != fence.structure_revision
        || row.pending.is_some()
        || row.amounts != row.active_amounts
        || row.facts.key != row.active_facts.key
        || row.active_facts.class != node.class
        || row.active_facts.dependencies.is_empty()
        || row.active_amounts.get(JointResource::Computations) != 1
        || !matches!(
            row.active_facts.lane,
            JointLane::Foreground | JointLane::Settlement
        )
    {
        return Err(WorkError::Conflict);
    }
    // Existing settlement work retains its separately admitted lane after new
    // business cancellation/admission close. Driver/source loss still refuses.
    if row.active_facts.lane != JointLane::Settlement {
        node.check(&state.cancellations)?;
        if state.closed {
            return Err(WorkError::Closed);
        }
    }
    Ok(&row.active_facts)
}

// Capture alone does not authorize execution. Once the unique WC owner has
// installed its closed factory, every CPU request must carry that same stamp.
// An unbound WC remains available for neutral E-source harnesses.
fn require_registered_factory(state: &State, fence: CpuActionFence) -> Result<(), WorkError> {
    if state.internal_cpu_factory.is_some() && !fence.has_closed_factory() {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

fn validate_queue_quote(
    state: &State,
    fence: CpuActionFence,
    physical_queue_bytes: u64,
) -> Result<(), WorkError> {
    if fence.has_closed_factory()
        && physical_queue_bytes
            < state
                .internal_cpu_factory
                .as_ref()
                .ok_or(WorkError::Conflict)?
                .queue_bytes()
                .get()
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

pub(in crate::joint_admission::ledger) fn validate_cpu_action(
    state: &State,
    owner: WorkId,
    fence: CpuActionFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    require_registered_factory(state, fence)?;
    let original = computation_facts(state, owner, fence)?;
    let [change] = changes else {
        return Err(WorkError::Conflict);
    };
    let amounts = change.claim.amounts;
    validate_queue_quote(state, fence, amounts.get(JointResource::PhysicalQueueBytes))?;
    let expected = JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(
            JointResource::MetadataBytes,
            amounts.get(JointResource::MetadataBytes),
        )
        .with(JointResource::InternalCpuJobs, 1)
        .with(JointResource::PhysicalQueueRecords, 1)
        .with(
            JointResource::PhysicalQueueBytes,
            amounts.get(JointResource::PhysicalQueueBytes),
        );
    if change.old.is_some()
        || change.source.is_some()
        || amounts != expected
        || amounts.get(JointResource::MetadataBytes) == 0
        || amounts.get(JointResource::PhysicalQueueBytes) == 0
        || change.claim.facts.key.class != original.key.class
        || change.claim.facts.key.lane != original.key.lane
        || change.claim.facts.key.dependencies != original.key.dependencies
        || change.claim.facts.key.causal
            != if fence.factory.is_some() {
                CausalMask::INTERNAL_CPU_ACTION
            } else {
                CausalMask::NONE
            }
        || !original.key.causal.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

/// A move-only original source capture. It retains no position or payload and
/// exposes no raw claim/identity; a released source cannot become a new grant.
#[must_use = "Construct the action outside original receiver locks"]
pub struct InternalCpuActionSource {
    inner: Weak<Inner>,
    owner: WorkId,
    fence: CpuActionFence,
}

pub(in crate::joint_admission::ledger) fn capture(
    inner: &Arc<Inner>,
    key: Option<[u64; 2]>,
    scope: &WorkScope,
) -> Result<InternalCpuActionSource, WorkError> {
    if !Arc::ptr_eq(inner, &scope.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let [id, epoch] = key.ok_or(WorkError::Released)?;
    let state = inner.state.lock().unwrap();
    let row = state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .claims
        .get(&id)
        .ok_or(WorkError::Released)?;
    let fence = CpuActionFence {
        computation: id,
        epoch,
        structure_revision: row.structure_revision,
        factory: None,
    };
    computation_facts(&state, scope.id, fence)?;
    Ok(InternalCpuActionSource {
        inner: Arc::downgrade(inner),
        owner: scope.id,
        fence,
    })
}
impl InternalCpuActionSource {
    /// The bytes are quotes of the actual closed job creator, not a profile
    /// certificate. This one request covers CPU and the original physical
    /// queue simultaneously, using the original cumulative finite proof.
    pub fn begin(
        self,
        metadata_bytes: NonZeroU64,
        physical_queue_bytes: NonZeroU64,
    ) -> Result<InternalCpuActionAdmission, JointAdmissionFailure> {
        self.begin_inner(metadata_bytes, physical_queue_bytes)
            .map_err(|error| JointAdmissionFailure { error })
    }
    fn begin_inner(
        self,
        metadata_bytes: NonZeroU64,
        physical_queue_bytes: NonZeroU64,
    ) -> Result<InternalCpuActionAdmission, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        {
            let state = inner.state.lock().unwrap();
            // Refuse an unauthorized request before reserving the original Q
            // cell or retaining any additional profile graph backing.
            require_registered_factory(&state, self.fence)?;
            computation_facts(&state, self.owner, self.fence)?;
            validate_queue_quote(&state, self.fence, physical_queue_bytes.get())?;
        }
        let scope = WorkScope {
            inner: Arc::clone(&inner),
            id: self.owner,
        };
        // Reserve the existing bounded proposal cell before constructing a body.
        // Full dependency references enter the same exact-K paid backing.
        let mut proposal = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        }
        .begin_paid_proposal_for_scope(scope)?;
        let facts = {
            let state = inner.state.lock().unwrap();
            require_registered_factory(&state, self.fence)?;
            Arc::clone(computation_facts(&state, self.owner, self.fence)?)
        };
        let amounts = JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::MetadataBytes, metadata_bytes.get())
            .with(JointResource::InternalCpuJobs, 1)
            .with(JointResource::PhysicalQueueRecords, 1)
            .with(
                JointResource::PhysicalQueueBytes,
                physical_queue_bytes.get(),
            );
        // Arm the same private source before its one body is constructed.
        // No generic builder can choose this per-resource causal shape.
        proposal.cpu_action = Some(self.fence);
        proposal.begin_new(facts.lane, amounts)?;
        let current = proposal.current.as_mut().expect("same closed CPU proposal");
        if facts.dependencies.len() > current.selected.capacity() {
            return Err(WorkError::Capacity("CPU action full dependency backing"));
        }
        current.selected.extend(facts.dependencies.iter().cloned());
        proposal.end_change()?;
        let admission = proposal.submit().map_err(|failure| failure.error)?;
        Ok(InternalCpuActionAdmission { admission })
    }
}
