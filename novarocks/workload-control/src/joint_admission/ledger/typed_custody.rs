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

//! Typed pins in the original prepaid ClaimRecord; no new holder allocation.
use super::*;
use crate::WorkloadProgressNotification;
use novarocks_resource_contract::{ExistingOwnerRetentionLease, RetentionOwner, RetentionPinError};

#[derive(Clone, Copy)]
pub(super) enum ClaimCustody {
    Raw,
    Production { epoch: u64 },
    Payload { epoch: u64, pins: usize },
    Computation { epoch: u64, pins: usize },
}
impl ClaimCustody {
    pub(super) fn is_raw(self) -> bool {
        matches!(self, Self::Raw)
    }
    pub(super) fn can_release_current(self) -> bool {
        matches!(
            self,
            Self::Raw | Self::Payload { pins: 0, .. } | Self::Computation { pins: 0, .. }
        )
    }
}
#[derive(Clone, Copy)]
enum Kind {
    Payload,
    Computation,
}
impl Kind {
    fn resource(self) -> JointResource {
        match self {
            Self::Payload => JointResource::PayloadBytes,
            Self::Computation => JointResource::Computations,
        }
    }
    fn new_custody(self, epoch: u64) -> ClaimCustody {
        match self {
            Self::Payload => ClaimCustody::Payload { epoch, pins: 1 },
            Self::Computation => ClaimCustody::Computation { epoch, pins: 1 },
        }
    }
    fn pins(self, custody: &mut ClaimCustody, epoch: u64) -> Result<&mut usize, RetentionPinError> {
        match custody {
            ClaimCustody::Payload {
                epoch: original,
                pins,
            } => {
                if matches!(self, Self::Payload) && *original == epoch {
                    Ok(pins)
                } else {
                    Err(RetentionPinError::Conflict)
                }
            }
            ClaimCustody::Computation {
                epoch: original,
                pins,
            } => {
                if matches!(self, Self::Computation) && *original == epoch {
                    Ok(pins)
                } else {
                    Err(RetentionPinError::Conflict)
                }
            }
            _ => Err(RetentionPinError::Conflict),
        }
    }
}
fn try_pin(inner: &Inner, key: [u64; 2], kind: Kind) -> Result<(), RetentionPinError> {
    let mut state = inner.state.lock().unwrap();
    let row = state
        .joint
        .as_mut()
        .and_then(|ledger| ledger.claims.get_mut(&key[0]))
        .ok_or(RetentionPinError::Released)?;
    let pins = kind.pins(&mut row.custody, key[1])?;
    if *pins == 0 {
        return Err(RetentionPinError::Released);
    }
    let next = pins.checked_add(1).ok_or(RetentionPinError::Overflow)?;
    *pins = next;
    Ok(())
}
fn drop_pin(inner: &Inner, key: [u64; 2], kind: Kind) {
    let last = {
        let mut state = inner.state.lock().unwrap();
        let Some(row) = state
            .joint
            .as_mut()
            .and_then(|ledger| ledger.claims.get_mut(&key[0]))
        else {
            return;
        };
        let Ok(pins) = kind.pins(&mut row.custody, key[1]) else {
            return;
        };
        if *pins == 0 {
            return;
        }
        *pins -= 1;
        *pins == 0
    };
    if last {
        // No foreign code between zero publication and this original release.
        // A paused Drop conservatively keeps the same record/charge alive.
        let waker = inner.update(|state| release_current(state, key[0]));
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
fn payload_capacity(inner: &Inner, key: [u64; 2]) -> Result<u64, RetentionPinError> {
    let state = inner.state.lock().unwrap();
    let row = state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&key[0]))
        .ok_or(RetentionPinError::Released)?;
    match row.custody {
        ClaimCustody::Payload { epoch, pins } if epoch == key[1] && pins != 0 => {
            Ok(row.active_amounts.get(JointResource::PayloadBytes))
        }
        _ => Err(RetentionPinError::Conflict),
    }
}
impl RetentionOwner for Inner {
    fn try_retain(&self, key: [u64; 2]) -> Result<(), RetentionPinError> {
        try_pin(self, key, Kind::Payload)
    }
    fn release_pin(&self, key: [u64; 2]) {
        drop_pin(self, key, Kind::Payload);
    }
    fn capacity_bytes(&self, key: [u64; 2]) -> Result<u64, RetentionPinError> {
        payload_capacity(self, key)
    }
}

struct OriginalPin {
    inner: Arc<Inner>,
    key: Option<[u64; 2]>,
    kind: Kind,
}
impl OriginalPin {
    fn try_clone(&self) -> Result<Self, RetentionPinError> {
        let key = self.key.ok_or(RetentionPinError::Released)?;
        let inner = Arc::clone(&self.inner);
        try_pin(&inner, key, self.kind)?;
        Ok(Self {
            inner,
            key: Some(key),
            kind: self.kind,
        })
    }
    fn same(&self, other: &Self) -> bool {
        self.key.is_some() && self.key == other.key && Arc::ptr_eq(&self.inner, &other.inner)
    }
    fn is_for_scope(&self, scope: &WorkScope) -> bool {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return false;
        }
        let Some(key) = self.key else {
            return false;
        };
        let state = self.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .and_then(|ledger| ledger.claims.get(&key[0]))
            .is_some_and(|row| {
                row.owner == ClaimOwner::PhysicalWork(scope.id)
                    && match (self.kind, row.custody) {
                        (Kind::Payload, ClaimCustody::Payload { epoch, pins })
                        | (Kind::Computation, ClaimCustody::Computation { epoch, pins }) => {
                            epoch == key[1] && pins != 0
                        }
                        _ => false,
                    }
            })
    }
}
impl Drop for OriginalPin {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            drop_pin(&self.inner, key, self.kind);
        }
    }
}
enum Target<'a> {
    Payload(&'a mut Option<RetainedPayloadGrant>),
    Computation(&'a mut Option<InternalComputationGrant>),
}
impl Target<'_> {
    fn kind(&self) -> Kind {
        match self {
            Self::Payload(_) => Kind::Payload,
            Self::Computation(_) => Kind::Computation,
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            Self::Payload(slot) => slot.is_none(),
            Self::Computation(slot) => slot.is_none(),
        }
    }
}
fn validate_adoption(
    state: &State,
    scope: &WorkScope,
    original: &JointClaim,
    kind: Kind,
) -> Result<(u64, WorkClass, JointLane), WorkError> {
    validate_active_control_node(state, scope)?;
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
    let cell = &state.cancellations.slots[node.cancellation.0];
    let control = ledger
        .claims
        .get(&cell.claim.ok_or(WorkError::NotReady)?)
        .ok_or(WorkError::Released)?;
    if !control.custody.is_raw()
        || cell.phase != SlotPhase::Node(scope.id)
        || cell.generation == 0
        || control.owner != ClaimOwner::ControlSlot(node.cancellation)
        || control.pending.is_some()
        || control.amounts != cell.base
        || control.active_amounts != cell.base
        || control.facts.key != control.active_facts.key
        || control.active_facts.dependencies.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    let record = ledger
        .claims
        .get(&original.id.ok_or(WorkError::Released)?)
        .ok_or(WorkError::Released)?;
    if !record.custody.is_raw()
        || record.owner != ClaimOwner::PhysicalWork(scope.id)
        || record.pending.is_some()
        || record.amounts != record.active_amounts
        || record.facts.key != record.active_facts.key
        || record.active_facts.class != node.class
        || record.active_facts.key.dependencies != control.active_facts.key.dependencies
    {
        return Err(WorkError::Conflict);
    }
    if !record.active_facts.key.causal.is_empty() {
        return Err(WorkError::Conflict);
    }
    let capacity = record.active_amounts.get(kind.resource());
    let valid = match kind {
        Kind::Payload => capacity != 0 && record.active_facts.lane == JointLane::Custody,
        Kind::Computation => {
            capacity == 1
                && matches!(
                    record.active_facts.lane,
                    JointLane::Foreground | JointLane::Settlement
                )
        }
    };
    let metadata = record.active_amounts.get(JointResource::MetadataBytes);
    // The enlarged ClaimRecord is prepaid by the original fixed map quote.
    // Any existing metadata amount stays on this same original claim; there
    // is no newly allocated self-funded carrier. Other coordinates are closed.
    let expected = JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::MetadataBytes, metadata)
        .with(kind.resource(), capacity);
    if !valid || record.active_amounts != expected {
        return Err(WorkError::Conflict);
    }
    record
        .structure_revision
        .checked_add(1)
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok((
        capacity,
        record.active_facts.class,
        record.active_facts.lane,
    ))
}
fn adopt_in(
    scope: &WorkScope,
    source: &mut Option<JointClaim>,
    mut target: Target<'_>,
) -> Result<WorkloadProgressNotification, WorkError> {
    if !target.is_empty() {
        return Err(WorkError::Conflict);
    }
    let original = source.as_ref().ok_or(WorkError::Released)?;
    if !Arc::ptr_eq(&scope.inner, &original.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let inner = Arc::clone(&original.inner);
    let kind = target.kind();
    let retired = {
        let mut state = inner.state.lock().unwrap();
        let original = source.as_mut().ok_or(WorkError::Released)?;
        validate_adoption(&state, scope, original, kind)?;
        let id = original.id.ok_or(WorkError::Released)?;
        let row = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
        let epoch = row
            .structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        row.structure_revision = epoch;
        row.custody = kind.new_custody(epoch);
        original.id = None;
        let pin = OriginalPin {
            inner: Arc::clone(&inner),
            key: Some([id, epoch]),
            kind,
        };
        match &mut target {
            Target::Payload(slot) => **slot = Some(RetainedPayloadGrant { pin }),
            Target::Computation(slot) => **slot = Some(InternalComputationGrant { pin }),
        }
        state.advance_progress_revision();
        source.take()
    };
    drop(retired);
    Ok(WorkloadProgressNotification::new(inner))
}

/// A payload-only original paid record. Its inline pin allocates no holder.
#[must_use = "Keep original payload custody through actual backing exit"]
pub struct RetainedPayloadGrant {
    pin: OriginalPin,
}
impl RetainedPayloadGrant {
    /// Inspect the original paid record without retaining or notifying.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn adopt_in(
        scope: &WorkScope,
        source: &mut Option<JointClaim>,
        target: &mut Option<Self>,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        adopt_in(scope, source, Target::Payload(target))
    }
    pub fn retain_alias(&self) -> Result<RetainedPayloadAlias, RetentionPinError> {
        Ok(RetainedPayloadAlias {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn into_alias(self) -> RetainedPayloadAlias {
        RetainedPayloadAlias { pin: self.pin }
    }
}
/// Retention only, never a new claim source.
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::RetainedPayloadAlias;
/// fn no_source(value: &RetainedPayloadAlias) { value.view(); }
/// ```
pub struct RetainedPayloadAlias {
    pin: OriginalPin,
}
impl RetainedPayloadAlias {
    pub fn try_clone(&self) -> Result<Self, RetentionPinError> {
        Ok(Self {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn shares_payload_with(&self, other: &Self) -> bool {
        self.pin.same(&other.pin)
    }
    pub fn capacity_bytes(&self) -> Result<u64, RetentionPinError> {
        payload_capacity(
            &self.pin.inner,
            self.pin.key.ok_or(RetentionPinError::Released)?,
        )
    }
    pub fn check_backing_total(&self, total: u64) -> Result<(), WorkError> {
        if total
            > self.capacity_bytes().map_err(|error| match error {
                RetentionPinError::Released => WorkError::Released,
                RetentionPinError::Conflict => WorkError::Conflict,
                RetentionPinError::Overflow => WorkError::ArithmeticOverflow,
            })?
        {
            Err(WorkError::Capacity("paid retained payload envelope"))
        } else {
            Ok(())
        }
    }
    /// Move this exact already-counted pin, without another Arc allocation or
    /// count increment. Neither the original owner nor its key is exposed.
    pub fn into_retention(mut self) -> ExistingOwnerRetentionLease {
        let key = self.pin.key.take().expect("live original payload pin");
        ExistingOwnerRetentionLease::from_existing_pin(
            Arc::clone(&self.pin.inner) as Arc<dyn RetentionOwner>,
            key,
        )
    }
}
#[must_use = "Only actual compute producers may retain this position"]
pub struct InternalComputationGrant {
    pin: OriginalPin,
}
impl InternalComputationGrant {
    /// Observe the actual original stage; no pin or callback.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn adopt_in(
        scope: &WorkScope,
        source: &mut Option<JointClaim>,
        target: &mut Option<Self>,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        adopt_in(scope, source, Target::Computation(target))
    }
    pub fn retain_alias(&self) -> Result<InternalComputationAlias, RetentionPinError> {
        Ok(InternalComputationAlias {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn into_alias(self) -> InternalComputationAlias {
        InternalComputationAlias { pin: self.pin }
    }
    /// Pure capture from this positive stage grant. Capture can run under the
    /// original receiver lock; it allocates no backing, pins and notifies none.
    /// Construct the paid action outside every receiver/physical-owner lock.
    pub fn cpu_action_source(
        &self,
        scope: &WorkScope,
    ) -> Result<InternalCpuActionSource, WorkError> {
        super::paid_proposal::capture_cpu_action(&self.pin.inner, self.pin.key, scope)
    }
}
/// No alias can release a position still used by another actual producer.
/// Nor can retention-only pins author a new physical action.
/// ```compile_fail
/// use novarocks_workload_control::{WorkScope, joint_admission::InternalComputationAlias};
/// fn no_action(alias: &InternalComputationAlias, scope: &WorkScope) {
///     let _ = alias.cpu_action_source(scope);
/// }
/// ```
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::InternalComputationAlias;
/// fn no_release(value: &mut InternalComputationAlias) { value.release_position(); }
/// ```
pub struct InternalComputationAlias {
    pin: OriginalPin,
}
impl InternalComputationAlias {
    pub fn try_clone(&self) -> Result<Self, RetentionPinError> {
        Ok(Self {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn shares_computation_with(&self, other: &Self) -> bool {
        self.pin.same(&other.pin)
    }
}

// Closed Root/stage construction borrows the original positive grant; aliases
// never gain this view. No pin, backing, owner or epoch is minted by capture.
pub(super) fn payload_growth_view(
    grant: &RetainedPayloadGrant,
    root: &WorkScope,
    state: &State,
) -> Result<JointClaimView, WorkError> {
    if !Arc::ptr_eq(&grant.pin.inner, &root.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let [id, epoch] = grant.pin.key.ok_or(WorkError::Released)?;
    let row = state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .claims
        .get(&id)
        .ok_or(WorkError::Released)?;
    if row.owner != ClaimOwner::PhysicalWork(root.id)
        || !matches!(row.custody, ClaimCustody::Payload { epoch: original, pins } if original == epoch && pins != 0)
        || row.pending.is_some()
        || row.amounts != row.active_amounts
        || row.facts.key != row.active_facts.key
    {
        return Err(WorkError::Conflict);
    }
    Ok(JointClaimView {
        inner: Arc::downgrade(&root.inner),
        id,
        source: ClaimSource::Payload { epoch },
        structure_revision: row.structure_revision,
        release_revision: row.release_revision,
    })
}
pub(super) fn same_payload_grant(
    grant: &RetainedPayloadGrant,
    inner: &Arc<Inner>,
    id: u64,
    epoch: u64,
) -> bool {
    Arc::ptr_eq(&grant.pin.inner, inner) && grant.pin.key == Some([id, epoch])
}
pub(super) fn install_computation_pin(
    inner: &Arc<Inner>,
    state: &mut State,
    id: u64,
    epoch: u64,
) -> InternalComputationGrant {
    let record = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
    record.structure_revision = epoch;
    record.custody = ClaimCustody::Computation { epoch, pins: 1 };
    InternalComputationGrant {
        pin: OriginalPin {
            inner: Arc::clone(inner),
            key: Some([id, epoch]),
            kind: Kind::Computation,
        },
    }
}

#[cfg(test)]
mod tests;

pub(super) mod unreceived;
pub use unreceived::{
    UnreceivedProductionAdmission, UnreceivedProductionGrant, UnreceivedProductionOwner,
    UnreceivedProductionQuote, UnreceivedProductionReady,
};
