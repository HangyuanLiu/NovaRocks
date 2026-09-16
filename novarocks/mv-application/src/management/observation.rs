// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_spi::connector::{ConnectorCommittedVersion, ConnectorDocumentManagementObservation};

use super::{
    CreateIntent, DeploymentOwner, EffectIdentity, ManagedMvTarget, ManagementOwnershipError,
    ProcessIncarnation, ReadmissionPermit, UnsettledEffect,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ManagementObservationRequestId([u8; 16]);

impl ManagementObservationRequestId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

#[derive(Clone)]
pub(super) struct ManagementObservationAuthorization(Arc<()>);

impl ManagementObservationAuthorization {
    pub(super) fn new() -> Self {
        Self(Arc::new(()))
    }

    pub(super) fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Debug)]
pub(super) struct ManagementObservationLiveness(Arc<AtomicBool>);

impl ManagementObservationLiveness {
    pub(super) fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    pub(super) fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub(super) fn close(&self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagementContinuation {
    SameOwner {
        previous_incarnation: ProcessIncarnation,
    },
    OwnerHandover {
        previous_owner: DeploymentOwner,
        previous_incarnation: ProcessIncarnation,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationRequirement {
    Incarnation,
    OwnerAndIncarnation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementObservationPhase {
    AwaitingEffectClosure,
    AwaitingFreshObservation,
    RegistrationRequired(RegistrationRequirement),
    AwaitingRegisteredObservation,
    Ready,
    ClosedForeignOwner,
    ClosedIncarnationMismatch,
    ClosedObjectReplacement,
}

/// Single-use wrapper minted only by completing a controller-issued current
/// observation request. Historical frozen observations have no such ticket.
#[derive(Debug, Eq, PartialEq)]
pub struct FreshManagementObservation {
    request_id: ManagementObservationRequestId,
    target: ManagedMvTarget,
    owner: DeploymentOwner,
    incarnation: ProcessIncarnation,
    metadata_version: ConnectorCommittedVersion,
}

impl FreshManagementObservation {
    fn try_from_connector(
        request_id: ManagementObservationRequestId,
        expected_target: &ManagedMvTarget,
        observation: &ConnectorDocumentManagementObservation,
    ) -> Result<Self, ManagementObservationError> {
        observation
            .validate_sealed()
            .map_err(|_| ManagementObservationError::UnsealedObservation)?;
        expected_target.validate_observation(observation)?;
        Ok(Self {
            request_id,
            target: expected_target.clone(),
            owner: DeploymentOwner::parse(observation.marker().owner())?,
            incarnation: ProcessIncarnation::parse(observation.marker().incarnation())?,
            metadata_version: observation.metadata_version().clone(),
        })
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn owner(&self) -> &DeploymentOwner {
        &self.owner
    }

    pub const fn incarnation(&self) -> &ProcessIncarnation {
        &self.incarnation
    }

    pub const fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }

    #[cfg(test)]
    pub(super) fn for_test(
        request_id: ManagementObservationRequestId,
        target: ManagedMvTarget,
        owner: DeploymentOwner,
        incarnation: ProcessIncarnation,
        metadata_version: ConnectorCommittedVersion,
    ) -> Self {
        Self {
            request_id,
            target,
            owner,
            incarnation,
            metadata_version,
        }
    }
}

pub struct PendingManagementObservation {
    request_id: ManagementObservationRequestId,
    target: ManagedMvTarget,
}

/// A one-shot observation capability issued only for a CREATE responsibility
/// whose stage request was dispatched before a physical object identity was
/// available. It does not accept a raw target: completion must consume a
/// sealed provider management observation and verify the original logical
/// create intent.
pub struct PendingCreateIntentObservation {
    intent: CreateIntent,
}

impl PendingCreateIntentObservation {
    pub(super) const fn for_intent(intent: CreateIntent) -> Self {
        Self { intent }
    }

    pub fn complete(
        self,
        observation: &ConnectorDocumentManagementObservation,
    ) -> Result<FreshCreateIntentObservation, ManagementObservationError> {
        observation
            .validate_sealed()
            .map_err(|_| ManagementObservationError::UnsealedObservation)?;
        let target = ManagedMvTarget::from_observation(observation)?;
        let target = self.intent.bind_target(target)?;
        Ok(FreshCreateIntentObservation {
            intent: self.intent,
            target,
        })
    }
}

/// Exact provider observation that may narrow one previously unbound CREATE
/// intent. It is intentionally consumable and cannot be constructed from a
/// caller-supplied object id.
pub struct FreshCreateIntentObservation {
    intent: CreateIntent,
    target: ManagedMvTarget,
}

impl FreshCreateIntentObservation {
    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub(crate) fn into_parts(self) -> (CreateIntent, ManagedMvTarget) {
        (self.intent, self.target)
    }

    #[cfg(test)]
    pub(super) const fn for_test(intent: CreateIntent, target: ManagedMvTarget) -> Self {
        Self { intent, target }
    }
}

impl PendingManagementObservation {
    pub fn complete(
        self,
        observation: &ConnectorDocumentManagementObservation,
    ) -> Result<FreshManagementObservation, ManagementObservationError> {
        FreshManagementObservation::try_from_connector(self.request_id, &self.target, observation)
    }
}

/// Target-local readmission state. It never records an alternative outcome for
/// an unknown effect and cannot execute provider operations.
pub struct ManagementObservationState {
    target: ManagedMvTarget,
    local_owner: DeploymentOwner,
    local_incarnation: ProcessIncarnation,
    continuation: ManagementContinuation,
    phase: ManagementObservationPhase,
    unsettled: HashSet<EffectIdentity>,
    resolved_effects: HashSet<EffectIdentity>,
    active_request: Option<ManagementObservationRequestId>,
    consumed_requests: HashSet<ManagementObservationRequestId>,
    required_committed_effect: Option<EffectIdentity>,
    authorization: Option<ManagementObservationAuthorization>,
    liveness: ManagementObservationLiveness,
    latest_metadata_version: Option<ConnectorCommittedVersion>,
}

impl ManagementObservationState {
    pub(super) fn try_new(
        target: ManagedMvTarget,
        local_owner: DeploymentOwner,
        local_incarnation: ProcessIncarnation,
        continuation: ManagementContinuation,
        unsettled: Vec<UnsettledEffect>,
        required_committed_effect: Option<EffectIdentity>,
        authorization: Option<ManagementObservationAuthorization>,
    ) -> Result<Self, ManagementObservationError> {
        if !unsettled.is_empty() && required_committed_effect.is_some() {
            return Err(ManagementObservationError::InvalidProvenance);
        }
        let previous_incarnation = match &continuation {
            ManagementContinuation::SameOwner {
                previous_incarnation,
            }
            | ManagementContinuation::OwnerHandover {
                previous_incarnation,
                ..
            } => previous_incarnation,
        };
        let mut identities = HashSet::with_capacity(unsettled.len());
        for effect in &unsettled {
            if effect.responsibility().target() != &target {
                return Err(ManagementObservationError::EffectTargetMismatch);
            }
            if effect.responsibility().dispatching_incarnation() != previous_incarnation {
                return Err(ManagementObservationError::InvalidRecoveryBarrier);
            }
            if !identities.insert(effect.responsibility().identity()) {
                return Err(ManagementObservationError::DuplicateEffect);
            }
        }
        let phase = if identities.is_empty() {
            ManagementObservationPhase::AwaitingFreshObservation
        } else {
            ManagementObservationPhase::AwaitingEffectClosure
        };
        Ok(Self {
            target,
            local_owner,
            local_incarnation,
            continuation,
            phase,
            unsettled: identities,
            resolved_effects: HashSet::new(),
            active_request: None,
            consumed_requests: HashSet::new(),
            required_committed_effect,
            authorization,
            liveness: ManagementObservationLiveness::new(),
            latest_metadata_version: None,
        })
    }

    pub const fn phase(&self) -> ManagementObservationPhase {
        self.phase
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn local_owner(&self) -> &DeploymentOwner {
        &self.local_owner
    }

    pub const fn local_incarnation(&self) -> &ProcessIncarnation {
        &self.local_incarnation
    }

    pub(crate) fn resolved_effects(&self) -> &HashSet<EffectIdentity> {
        &self.resolved_effects
    }

    pub(crate) const fn required_committed_effect(&self) -> Option<EffectIdentity> {
        self.required_committed_effect
    }

    pub(super) const fn continuation(&self) -> &ManagementContinuation {
        &self.continuation
    }

    pub(super) const fn authorization(&self) -> Option<&ManagementObservationAuthorization> {
        self.authorization.as_ref()
    }

    pub(super) const fn liveness(&self) -> &ManagementObservationLiveness {
        &self.liveness
    }

    pub fn accept_readmission_permit(
        &mut self,
        permit: ReadmissionPermit,
    ) -> Result<(), ManagementObservationError> {
        if permit.target() != &self.target {
            return Err(ManagementObservationError::EffectTargetMismatch);
        }
        if !permit.preserves_unknown_disposition() || !self.unsettled.remove(&permit.effect()) {
            return Err(ManagementObservationError::UnknownEffect);
        }
        self.resolved_effects.insert(permit.effect());
        if self.unsettled.is_empty() {
            self.phase = ManagementObservationPhase::AwaitingFreshObservation;
        }
        Ok(())
    }

    pub fn begin_current_observation(
        &mut self,
        request_id: ManagementObservationRequestId,
    ) -> Result<PendingManagementObservation, ManagementObservationError> {
        if !matches!(
            self.phase,
            ManagementObservationPhase::AwaitingFreshObservation
                | ManagementObservationPhase::AwaitingRegisteredObservation
                | ManagementObservationPhase::Ready
        ) {
            return Err(ManagementObservationError::ObservationNotAllowed);
        }
        if self.consumed_requests.contains(&request_id) {
            return Err(ManagementObservationError::ReusedObservationRequest);
        }
        if self.active_request.is_some() {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        self.active_request = Some(request_id);
        Ok(PendingManagementObservation {
            request_id,
            target: self.target.clone(),
        })
    }

    pub fn accept_current_observation(
        &mut self,
        observation: FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        self.consume_current_request(observation.request_id)?;
        self.apply_current_observation(observation)
    }

    fn apply_current_observation(
        &mut self,
        observation: FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        if observation.target() != &self.target {
            self.phase = ManagementObservationPhase::ClosedObjectReplacement;
            return Err(ManagementObservationError::Ownership(
                ManagementOwnershipError::TargetReplaced,
            ));
        }
        self.latest_metadata_version = Some(observation.metadata_version().clone());

        let result = match self.phase {
            ManagementObservationPhase::AwaitingFreshObservation => {
                self.accept_pre_registration_observation(&observation)
            }
            ManagementObservationPhase::AwaitingRegisteredObservation
            | ManagementObservationPhase::Ready => {
                self.accept_registered_or_live_observation(&observation)
            }
            _ => Err(ManagementObservationError::ObservationNotAllowed),
        };
        if result.is_err() {
            self.close_for_observation_error(&result);
        }
        result
    }

    /// Complete one exact request and apply it atomically to the state. This
    /// path can close admission on object replacement; callers must not turn a
    /// failed exact observation into a candidate miss.
    pub fn complete_current_observation(
        &mut self,
        pending: PendingManagementObservation,
        observation: &ConnectorDocumentManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        self.consume_current_request(pending.request_id)?;
        let fresh = match pending.complete(observation) {
            Ok(fresh) => fresh,
            Err(error) => {
                if matches!(
                    error,
                    ManagementObservationError::Ownership(ManagementOwnershipError::TargetReplaced)
                ) {
                    self.phase = ManagementObservationPhase::ClosedObjectReplacement;
                    self.liveness.close();
                }
                return Err(error);
            }
        };
        self.apply_current_observation(fresh)
    }

    pub fn record_registration_terminal(
        &mut self,
        terminal: super::EffectTerminalFact,
    ) -> Result<(), ManagementObservationError> {
        if !matches!(
            self.phase,
            ManagementObservationPhase::RegistrationRequired(_)
        ) {
            return Err(ManagementObservationError::RegistrationNotAllowed);
        }
        let responsibility = match &terminal {
            super::EffectTerminalFact::KnownCommitted(responsibility)
            | super::EffectTerminalFact::KnownUncommitted(responsibility) => responsibility,
            super::EffectTerminalFact::CommitUnknown(effect) => effect.responsibility(),
        };
        if responsibility.target() != &self.target
            || !responsibility
                .scope()
                .contains(super::EffectPath::CatalogCommit)
        {
            return Err(ManagementObservationError::EffectTargetMismatch);
        }
        match terminal {
            super::EffectTerminalFact::KnownCommitted(_) => {
                self.phase = ManagementObservationPhase::AwaitingRegisteredObservation;
            }
            super::EffectTerminalFact::KnownUncommitted(_) => {}
            super::EffectTerminalFact::CommitUnknown(effect) => {
                let identity = effect.responsibility().identity();
                if !self.unsettled.insert(identity) {
                    return Err(ManagementObservationError::DuplicateEffect);
                }
                self.phase = ManagementObservationPhase::AwaitingEffectClosure;
            }
        }
        Ok(())
    }

    pub const fn latest_metadata_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.latest_metadata_version.as_ref()
    }

    fn accept_pre_registration_observation(
        &mut self,
        observation: &FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        let requirement = match &self.continuation {
            ManagementContinuation::SameOwner {
                previous_incarnation,
            } => {
                if observation.owner() != &self.local_owner {
                    return Err(ManagementObservationError::ForeignOwner);
                }
                if observation.incarnation() == &self.local_incarnation {
                    self.phase = ManagementObservationPhase::Ready;
                    return Ok(self.phase);
                }
                if observation.incarnation() != previous_incarnation {
                    return Err(ManagementObservationError::IncarnationMismatch);
                }
                RegistrationRequirement::Incarnation
            }
            ManagementContinuation::OwnerHandover {
                previous_owner,
                previous_incarnation,
            } => {
                if observation.owner() == &self.local_owner
                    && observation.incarnation() == &self.local_incarnation
                {
                    self.phase = ManagementObservationPhase::Ready;
                    return Ok(self.phase);
                }
                if observation.owner() != previous_owner {
                    return Err(ManagementObservationError::ForeignOwner);
                }
                if observation.incarnation() != previous_incarnation {
                    return Err(ManagementObservationError::IncarnationMismatch);
                }
                RegistrationRequirement::OwnerAndIncarnation
            }
        };
        self.phase = ManagementObservationPhase::RegistrationRequired(requirement);
        Ok(self.phase)
    }

    fn accept_registered_or_live_observation(
        &mut self,
        observation: &FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        if observation.owner() != &self.local_owner {
            return Err(ManagementObservationError::ForeignOwner);
        }
        if observation.incarnation() != &self.local_incarnation {
            return Err(ManagementObservationError::IncarnationMismatch);
        }
        self.phase = ManagementObservationPhase::Ready;
        Ok(self.phase)
    }

    fn close_for_observation_error(
        &mut self,
        result: &Result<ManagementObservationPhase, ManagementObservationError>,
    ) {
        self.phase = match result {
            Err(ManagementObservationError::ForeignOwner) => {
                ManagementObservationPhase::ClosedForeignOwner
            }
            Err(ManagementObservationError::IncarnationMismatch) => {
                ManagementObservationPhase::ClosedIncarnationMismatch
            }
            Err(ManagementObservationError::Ownership(
                ManagementOwnershipError::TargetReplaced,
            )) => ManagementObservationPhase::ClosedObjectReplacement,
            _ => self.phase,
        };
        if matches!(
            self.phase,
            ManagementObservationPhase::ClosedForeignOwner
                | ManagementObservationPhase::ClosedIncarnationMismatch
                | ManagementObservationPhase::ClosedObjectReplacement
        ) {
            self.liveness.close();
        }
    }

    fn consume_current_request(
        &mut self,
        request_id: ManagementObservationRequestId,
    ) -> Result<(), ManagementObservationError> {
        if self.consumed_requests.contains(&request_id) {
            return Err(ManagementObservationError::ReusedObservationRequest);
        }
        if self.active_request != Some(request_id) {
            return Err(ManagementObservationError::StaleObservation);
        }
        self.active_request = None;
        self.consumed_requests.insert(request_id);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagementObservationError {
    Ownership(ManagementOwnershipError),
    EffectTargetMismatch,
    DuplicateEffect,
    UnknownEffect,
    ObservationNotAllowed,
    ObservationAlreadyPending,
    ReusedObservationRequest,
    StaleObservation,
    InvalidProvenance,
    InvalidRecoveryBarrier,
    RegistrationNotAllowed,
    ForeignOwner,
    IncarnationMismatch,
    UnsealedObservation,
}

impl fmt::Display for ManagementObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ownership(error) => error.fmt(formatter),
            Self::EffectTargetMismatch => formatter.write_str("effect targets another MV"),
            Self::DuplicateEffect => formatter.write_str("duplicate unsettled effect identity"),
            Self::UnknownEffect => formatter.write_str("readmission permit is stale or unknown"),
            Self::ObservationNotAllowed => {
                formatter.write_str("fresh observation is not allowed in the current phase")
            }
            Self::ObservationAlreadyPending => {
                formatter.write_str("a fresh management observation is already pending")
            }
            Self::ReusedObservationRequest => {
                formatter.write_str("management observation request was already consumed")
            }
            Self::StaleObservation => {
                formatter.write_str("management observation request is stale or mismatched")
            }
            Self::InvalidProvenance => {
                formatter.write_str("management observation provenance is invalid")
            }
            Self::InvalidRecoveryBarrier => formatter
                .write_str("recovery barrier does not cover the isolated previous incarnation"),
            Self::RegistrationNotAllowed => {
                formatter.write_str("owner or incarnation registration is not currently allowed")
            }
            Self::ForeignOwner => formatter.write_str("materialized view has another owner"),
            Self::IncarnationMismatch => {
                formatter.write_str("fresh observation found another management incarnation")
            }
            Self::UnsealedObservation => formatter
                .write_str("management observation was not sealed by its exact storage lease"),
        }
    }
}

impl std::error::Error for ManagementObservationError {}

impl From<ManagementOwnershipError> for ManagementObservationError {
    fn from(value: ManagementOwnershipError) -> Self {
        Self::Ownership(value)
    }
}
