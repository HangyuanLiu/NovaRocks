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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use novarocks_spi::connector::{
    CatalogHandle, ConnectorDocumentManagementOperation, ConnectorTableIdentity,
    ConnectorTableObjectId,
};

use crate::activity::{
    CanonicalMvTarget, MvActivityAdmissionError, MvActivityGate, MvActivityGateError,
    MvActivityLease, MvActivityOwner, MvActivityTicket,
};

use super::observation::{ManagementObservationAuthorization, ManagementObservationLiveness};
use super::{
    DeploymentOwner, EffectDisposition, EffectResponsibility, EffectScope, ManagedMvTarget,
    ManagementContinuation, ManagementObservationError, ManagementObservationPhase,
    ManagementObservationState, ProcessIncarnation, UnsettledEffect,
};

/// Exact MV-domain dependencies frozen before a long computation. Reacquiring
/// the entrance with an older set fails rather than publishing old output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagementDependencySet {
    definition_revision: [u8; 32],
    interpretation_revision: [u8; 32],
    publication_base: Option<[u8; 32]>,
    runtime_epoch: u64,
}

impl ManagementDependencySet {
    pub const fn new(
        definition_revision: [u8; 32],
        interpretation_revision: [u8; 32],
        publication_base: Option<[u8; 32]>,
        runtime_epoch: u64,
    ) -> Self {
        Self {
            definition_revision,
            interpretation_revision,
            publication_base,
            runtime_epoch,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ManagementRequest {
    catalog: CatalogHandle,
    table: ConnectorTableIdentity,
    expected_object_id: Option<ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    expected_dependencies: Option<ManagementDependencySet>,
    effect_scope: EffectScope,
}

impl ManagementRequest {
    pub fn try_new(
        catalog: CatalogHandle,
        table: ConnectorTableIdentity,
        expected_object_id: Option<ConnectorTableObjectId>,
        operation: ConnectorDocumentManagementOperation,
        expected_dependencies: Option<ManagementDependencySet>,
        effect_scope: EffectScope,
    ) -> Result<Self, ManagementAdmissionError> {
        if catalog.catalog_name() != &table.instance_id
            || (operation == ConnectorDocumentManagementOperation::Create)
                == expected_object_id.is_some()
            || (operation == ConnectorDocumentManagementOperation::Create)
                == expected_dependencies.is_some()
        {
            return Err(ManagementAdmissionError::InvalidRequest);
        }
        Ok(Self {
            catalog,
            table,
            expected_object_id,
            operation,
            expected_dependencies,
            effect_scope,
        })
    }

    pub const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub const fn operation(&self) -> ConnectorDocumentManagementOperation {
        self.operation
    }
}

#[derive(Clone)]
pub struct ManagementEntrance {
    inner: Arc<EntranceInner>,
}

struct EntranceInner {
    owner: DeploymentOwner,
    incarnation: ProcessIncarnation,
    activity: MvActivityGate,
    state: Mutex<HashMap<ConnectorTableIdentity, TargetAdmissionState>>,
}

struct TargetAdmissionState {
    target: ManagedMvTarget,
    dependencies: ManagementDependencySet,
    unsettled: HashMap<super::EffectIdentity, UnsettledEffect>,
    pending_committed_effect: Option<super::EffectIdentity>,
    pending_continuation: Option<ManagementContinuation>,
    pending_observation: Option<ManagementObservationAuthorization>,
    installed_observation: Option<ManagementObservationLiveness>,
    ready: bool,
}

impl ManagementEntrance {
    pub fn new(owner: DeploymentOwner, incarnation: ProcessIncarnation) -> Self {
        Self {
            inner: Arc::new(EntranceInner {
                owner,
                incarnation,
                activity: MvActivityGate::new(),
                state: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn owner(&self) -> &DeploymentOwner {
        &self.inner.owner
    }

    pub fn incarnation(&self) -> &ProcessIncarnation {
        &self.inner.incarnation
    }

    /// Installs only a state that completed effect closure, exact
    /// re-observation and owner/incarnation registration.
    pub fn install_observed_target(
        &self,
        observation: &ManagementObservationState,
        dependencies: ManagementDependencySet,
    ) -> Result<(), ManagementAdmissionError> {
        if observation.phase() != ManagementObservationPhase::Ready {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        if observation.local_owner() != &self.inner.owner
            || observation.local_incarnation() != &self.inner.incarnation
        {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        let mut state = lock(&self.inner.state);
        if let Some(current) = state.get(observation.target().table()) {
            let current_effects = current
                .unsettled
                .keys()
                .copied()
                .collect::<std::collections::HashSet<_>>();
            if current.target != *observation.target()
                || current_effects != *observation.resolved_effects()
                || current.pending_committed_effect != observation.required_committed_effect()
                || current.pending_continuation.as_ref() != Some(observation.continuation())
                || !matching_authorization(
                    current.pending_observation.as_ref(),
                    observation.authorization(),
                )
            {
                return Err(ManagementAdmissionError::ReadmissionIncomplete);
            }
        } else if !observation.resolved_effects().is_empty()
            || observation.required_committed_effect().is_some()
            || observation.authorization().is_some()
        {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        state.insert(
            observation.target().table().clone(),
            TargetAdmissionState {
                target: observation.target().clone(),
                dependencies,
                unsettled: HashMap::new(),
                pending_committed_effect: None,
                pending_continuation: None,
                pending_observation: None,
                installed_observation: Some(observation.liveness().clone()),
                ready: true,
            },
        );
        Ok(())
    }

    /// Close normal writes before acquiring new current evidence. The returned
    /// state is bound to the exact unresolved effect set retained by this
    /// entrance; creating an empty state cannot bypass Unknown.
    pub fn begin_readmission(
        &self,
        table: &ConnectorTableIdentity,
        continuation: ManagementContinuation,
    ) -> Result<ManagementObservationState, ManagementObservationError> {
        let mut state = lock(&self.inner.state);
        let current = state
            .get_mut(table)
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        if current.unsettled.is_empty() || current.pending_committed_effect.is_some() {
            return Err(ManagementObservationError::ObservationNotAllowed);
        }
        if current.pending_continuation.as_ref() != Some(&continuation) {
            return Err(ManagementObservationError::InvalidRecoveryBarrier);
        }
        if current.pending_observation.is_some() {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        current.ready = false;
        current.installed_observation = None;
        let authorization = ManagementObservationAuthorization::new();
        current.pending_observation = Some(authorization.clone());
        ManagementObservationState::try_new(
            current.target.clone(),
            self.inner.owner.clone(),
            self.inner.incarnation.clone(),
            continuation,
            current.unsettled.values().cloned().collect(),
            None,
            Some(authorization),
        )
    }

    /// Starts the mandatory exact re-observation after a successful external
    /// commit. The committed effect identity is retained as an unforgeable
    /// entrance-side convergence requirement until installation succeeds.
    pub fn begin_committed_convergence(
        &self,
        table: &ConnectorTableIdentity,
        continuation: ManagementContinuation,
    ) -> Result<ManagementObservationState, ManagementObservationError> {
        let mut state = lock(&self.inner.state);
        let current = state
            .get_mut(table)
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        let committed_effect = current
            .pending_committed_effect
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        if current.ready || !current.unsettled.is_empty() {
            return Err(ManagementObservationError::ObservationNotAllowed);
        }
        if current.pending_continuation.as_ref() != Some(&continuation) {
            return Err(ManagementObservationError::InvalidRecoveryBarrier);
        }
        if current.pending_observation.is_some() {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        let authorization = ManagementObservationAuthorization::new();
        current.installed_observation = None;
        current.pending_observation = Some(authorization.clone());
        ManagementObservationState::try_new(
            current.target.clone(),
            self.inner.owner.clone(),
            self.inner.incarnation.clone(),
            continuation,
            vec![],
            Some(committed_effect),
            Some(authorization),
        )
    }

    /// Establish the conservative barrier used after process reconstruction.
    /// No durable attempt ledger is loaded: the caller supplies one barrier
    /// covering every operation path that the isolated old incarnation could
    /// have dispatched, and normal management remains closed until it is
    /// resolved and followed by exact re-observation.
    pub fn begin_recovered_target(
        &self,
        target: ManagedMvTarget,
        continuation: ManagementContinuation,
        recovery_barrier: UnsettledEffect,
    ) -> Result<ManagementObservationState, ManagementObservationError> {
        if recovery_barrier.responsibility().target() != &target {
            return Err(ManagementObservationError::EffectTargetMismatch);
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
        if previous_incarnation == &self.inner.incarnation
            || recovery_barrier.responsibility().dispatching_incarnation() != previous_incarnation
            || recovery_barrier.responsibility().scope() != EffectScope::CATALOG_AND_OBJECT_DELETION
        {
            return Err(ManagementObservationError::InvalidRecoveryBarrier);
        }
        let mut state = lock(&self.inner.state);
        if state.contains_key(target.table()) {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        let authorization = ManagementObservationAuthorization::new();
        state.insert(
            target.table().clone(),
            TargetAdmissionState {
                target: target.clone(),
                dependencies: ManagementDependencySet::new([0; 32], [0; 32], None, 0),
                unsettled: HashMap::from([(
                    recovery_barrier.responsibility().identity(),
                    recovery_barrier.clone(),
                )]),
                pending_committed_effect: None,
                pending_continuation: Some(continuation.clone()),
                pending_observation: Some(authorization.clone()),
                installed_observation: None,
                ready: false,
            },
        );
        ManagementObservationState::try_new(
            target,
            self.inner.owner.clone(),
            self.inner.incarnation.clone(),
            continuation,
            vec![recovery_barrier],
            None,
            Some(authorization),
        )
    }

    /// Abandons one entrance-issued observation token without reopening the
    /// target. The unresolved or committed convergence requirement remains in
    /// place, so the caller may explicitly issue a replacement observation.
    pub fn abandon_observation(
        &self,
        observation: ManagementObservationState,
    ) -> Result<(), ManagementObservationError> {
        if observation.local_owner() != &self.inner.owner
            || observation.local_incarnation() != &self.inner.incarnation
        {
            return Err(ManagementObservationError::InvalidProvenance);
        }
        let authorization = observation
            .authorization()
            .ok_or(ManagementObservationError::InvalidProvenance)?;
        let mut state = lock(&self.inner.state);
        let current = state
            .get_mut(observation.target().table())
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        if current.target != *observation.target()
            || !matching_authorization(current.pending_observation.as_ref(), Some(authorization))
        {
            return Err(ManagementObservationError::StaleObservation);
        }
        current.pending_observation = None;
        observation.liveness().close();
        Ok(())
    }

    /// Queue one normal mutation on the only management entrance.
    ///
    /// The caller supplies its real activity owner so foreground, scheduler,
    /// and maintenance work share one FIFO without being mislabeled as a
    /// manual refresh. Domain dependencies are checked only after the ticket
    /// reaches the head and owns the target activity lease.
    pub fn request(
        &self,
        request: ManagementRequest,
        owner: MvActivityOwner,
    ) -> Result<ManagementEntranceTicket, ManagementAdmissionError> {
        let activity_target = CanonicalMvTarget::from_parts(
            Some(request.table.instance_id.as_str()),
            &request.table.namespace,
            &request.table.table,
        );
        let activity = self
            .inner
            .activity
            .request(activity_target, owner)
            .map_err(ManagementAdmissionError::from)?;
        Ok(ManagementEntranceTicket {
            entrance: Arc::downgrade(&self.inner),
            request: Some(request),
            activity,
        })
    }

    /// Blocking convenience for foreground callers. Worker adapters should
    /// retain the ticket and use `try_acquire` so their event loop never
    /// blocks behind another owner.
    pub fn acquire(
        &self,
        request: ManagementRequest,
        cancelled: impl Fn() -> bool,
    ) -> Result<ManagementEntranceLease, ManagementAdmissionError> {
        let owner = activity_owner(request.operation);
        self.request(request, owner)?
            .acquire_waiting(cancelled)?
            .ok_or(ManagementAdmissionError::Cancelled)
    }

    pub fn begin_stopping(&self) {
        self.inner.activity.begin_stopping();
    }

    /// Snapshot unresolved responsibilities without removing their admission
    /// block. Only a completed readmission observation may replace the state.
    pub fn unsettled_effects(&self, table: &ConnectorTableIdentity) -> Vec<UnsettledEffect> {
        lock(&self.inner.state)
            .get(table)
            .map(|state| state.unsettled.values().cloned().collect())
            .unwrap_or_default()
    }
}

/// A queued management request. Dropping it before acquisition removes the
/// request from the shared target FIFO through the embedded activity ticket.
pub struct ManagementEntranceTicket {
    entrance: Weak<EntranceInner>,
    request: Option<ManagementRequest>,
    activity: MvActivityTicket,
}

impl ManagementEntranceTicket {
    /// Acquire only when this request is at the head of the target FIFO.
    pub fn try_acquire(
        &mut self,
    ) -> Result<Option<ManagementEntranceLease>, ManagementAdmissionError> {
        let Some(activity) = self
            .activity
            .try_acquire()
            .map_err(ManagementAdmissionError::from)?
        else {
            return Ok(None);
        };
        self.finish_acquire(activity).map(Some)
    }

    /// Wait for the target FIFO while honoring the caller-owned cancellation
    /// probe. Worker callers normally use `try_acquire` instead.
    pub fn acquire_waiting(
        &mut self,
        cancelled: impl Fn() -> bool,
    ) -> Result<Option<ManagementEntranceLease>, ManagementAdmissionError> {
        let Some(activity) = self
            .activity
            .acquire_waiting(cancelled)
            .map_err(ManagementAdmissionError::from)?
        else {
            return Ok(None);
        };
        self.finish_acquire(activity).map(Some)
    }

    fn finish_acquire(
        &mut self,
        activity: MvActivityLease,
    ) -> Result<ManagementEntranceLease, ManagementAdmissionError> {
        let entrance = self
            .entrance
            .upgrade()
            .ok_or(ManagementAdmissionError::EntranceDropped)?;
        let request = self
            .request
            .take()
            .ok_or(ManagementAdmissionError::InvalidRequest)?;
        validate_request_against_state(&entrance, &request)?;
        Ok(ManagementEntranceLease {
            entrance: Arc::downgrade(&entrance),
            request,
            activity: Some(activity),
            dispatched: None,
        })
    }
}

pub struct ManagementEntranceLease {
    entrance: Weak<EntranceInner>,
    request: ManagementRequest,
    activity: Option<MvActivityLease>,
    dispatched: Option<EffectResponsibility>,
}

impl ManagementEntranceLease {
    /// Worker-owned admissions receive a shutdown cancellation view from the
    /// shared activity gate. Foreground admissions deliberately return none;
    /// their statement WorkScope remains the cancellation owner.
    pub fn worker_cancellation(
        &self,
    ) -> Option<novarocks_query_application::cancellation::QueryCancellationView> {
        self.activity
            .as_ref()
            .and_then(MvActivityLease::cancellation)
    }

    /// Must be called immediately before crossing the provider side-effect
    /// boundary. Dropping after this point conservatively records Unknown.
    pub fn mark_dispatched(
        &mut self,
        responsibility: EffectResponsibility,
    ) -> Result<(), ManagementAdmissionError> {
        let entrance = self
            .entrance
            .upgrade()
            .ok_or(ManagementAdmissionError::EntranceDropped)?;
        if self.dispatched.is_some()
            || self.request.effect_scope != responsibility.scope()
            || responsibility.dispatching_incarnation() != &entrance.incarnation
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        if responsibility.target().catalog() != &self.request.catalog
            || responsibility.target().table() != &self.request.table
            || self
                .request
                .expected_object_id
                .as_ref()
                .is_some_and(|object_id| responsibility.target().object_id() != object_id)
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        let mut state = lock(&entrance.state);
        match self.request.operation {
            ConnectorDocumentManagementOperation::Create => {
                if state.contains_key(&self.request.table) {
                    return Err(ManagementAdmissionError::ReadmissionIncomplete);
                }
            }
            ConnectorDocumentManagementOperation::SingleTargetUpdate
            | ConnectorDocumentManagementOperation::Publication => {
                let current = state
                    .get_mut(&self.request.table)
                    .ok_or(ManagementAdmissionError::ReadmissionIncomplete)?;
                if current
                    .installed_observation
                    .as_ref()
                    .is_some_and(|observation| !observation.is_open())
                {
                    current.ready = false;
                    current.installed_observation = None;
                }
                if !current.ready
                    || !current.unsettled.is_empty()
                    || current.pending_committed_effect.is_some()
                    || current.pending_observation.is_some()
                {
                    return Err(ManagementAdmissionError::ReadmissionIncomplete);
                }
                if current.target.catalog() != &self.request.catalog
                    || self.request.expected_object_id.as_ref() != Some(current.target.object_id())
                {
                    return Err(ManagementAdmissionError::TargetReplaced);
                }
                if self.request.expected_dependencies.as_ref() != Some(&current.dependencies) {
                    return Err(ManagementAdmissionError::DependencyChanged);
                }
            }
        }
        self.dispatched = Some(responsibility);
        Ok(())
    }

    pub fn record_terminal(
        mut self,
        disposition: EffectDisposition,
    ) -> Result<(), ManagementAdmissionError> {
        let responsibility = self
            .dispatched
            .take()
            .ok_or(ManagementAdmissionError::EffectNotDispatched)?;
        match disposition {
            EffectDisposition::KnownCommitted => {
                record_committed(&self.entrance, responsibility)?;
            }
            EffectDisposition::KnownUncommitted => {}
            EffectDisposition::CommitUnknown => {
                record_unsettled(&self.entrance, responsibility)?;
            }
        }
        self.activity.take();
        Ok(())
    }
}

fn record_committed(
    entrance: &Weak<EntranceInner>,
    responsibility: EffectResponsibility,
) -> Result<(), ManagementAdmissionError> {
    let entrance = entrance
        .upgrade()
        .ok_or(ManagementAdmissionError::EntranceDropped)?;
    let table = responsibility.target().table().clone();
    let mut state = lock(&entrance.state);
    let target = state.entry(table).or_insert_with(|| TargetAdmissionState {
        target: responsibility.target().clone(),
        dependencies: ManagementDependencySet::new([0; 32], [0; 32], None, 0),
        unsettled: HashMap::new(),
        pending_committed_effect: None,
        pending_continuation: None,
        pending_observation: None,
        installed_observation: None,
        ready: false,
    });
    if target.target != *responsibility.target()
        || !target.unsettled.is_empty()
        || target.pending_committed_effect.is_some()
        || target.pending_observation.is_some()
    {
        return Err(ManagementAdmissionError::InvalidEffect);
    }
    target.ready = false;
    target.pending_committed_effect = Some(responsibility.identity());
    target.pending_continuation = Some(ManagementContinuation::SameOwner {
        previous_incarnation: responsibility.dispatching_incarnation().clone(),
    });
    target.pending_observation = None;
    target.installed_observation = None;
    Ok(())
}

impl Drop for ManagementEntranceLease {
    fn drop(&mut self) {
        if let Some(responsibility) = self.dispatched.take() {
            let _ = record_unsettled(&self.entrance, responsibility);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementAdmissionError {
    InvalidRequest,
    Stopping,
    Cancelled,
    TargetAlreadyExists,
    TargetReplaced,
    DependencyChanged,
    EffectUnsettled,
    ReadmissionIncomplete,
    InvalidEffect,
    EffectNotDispatched,
    EntranceDropped,
}

impl fmt::Display for ManagementAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidRequest => "invalid MV management request",
            Self::Stopping => "MV management admission is stopping",
            Self::Cancelled => "MV management admission wait was cancelled",
            Self::TargetAlreadyExists => "MV target already has an observed object",
            Self::TargetReplaced => "MV target generation changed",
            Self::DependencyChanged => "MV dependencies changed during preparation",
            Self::EffectUnsettled => "a prior external effect may still change this MV",
            Self::ReadmissionIncomplete => "MV management readmission is incomplete",
            Self::InvalidEffect => "external effect does not match its management admission",
            Self::EffectNotDispatched => "external effect was not marked dispatched",
            Self::EntranceDropped => "MV management entrance no longer exists",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ManagementAdmissionError {}

impl From<MvActivityAdmissionError> for ManagementAdmissionError {
    fn from(value: MvActivityAdmissionError) -> Self {
        match value {
            MvActivityAdmissionError::Stopping => Self::Stopping,
            MvActivityAdmissionError::Cancelled => Self::Cancelled,
        }
    }
}

impl From<MvActivityGateError> for ManagementAdmissionError {
    fn from(value: MvActivityGateError) -> Self {
        match value {
            MvActivityGateError::Stopping => Self::Stopping,
        }
    }
}

fn validate_request_against_state(
    entrance: &EntranceInner,
    request: &ManagementRequest,
) -> Result<(), ManagementAdmissionError> {
    let mut state = lock(&entrance.state);
    match request.operation {
        ConnectorDocumentManagementOperation::Create => {
            if let Some(current) = state.get(&request.table) {
                return Err(if current.unsettled.is_empty() {
                    ManagementAdmissionError::TargetAlreadyExists
                } else {
                    ManagementAdmissionError::EffectUnsettled
                });
            }
        }
        ConnectorDocumentManagementOperation::SingleTargetUpdate
        | ConnectorDocumentManagementOperation::Publication => {
            let current = state
                .get_mut(&request.table)
                .ok_or(ManagementAdmissionError::ReadmissionIncomplete)?;
            if current
                .installed_observation
                .as_ref()
                .is_some_and(|observation| !observation.is_open())
            {
                current.ready = false;
                current.installed_observation = None;
            }
            if !current.ready {
                return Err(if current.unsettled.is_empty() {
                    ManagementAdmissionError::ReadmissionIncomplete
                } else {
                    ManagementAdmissionError::EffectUnsettled
                });
            }
            if current.target.catalog() != &request.catalog
                || request.expected_object_id.as_ref() != Some(current.target.object_id())
            {
                return Err(ManagementAdmissionError::TargetReplaced);
            }
            if request.expected_dependencies.as_ref() != Some(&current.dependencies) {
                return Err(ManagementAdmissionError::DependencyChanged);
            }
            if !current.unsettled.is_empty() {
                return Err(ManagementAdmissionError::EffectUnsettled);
            }
        }
    }
    Ok(())
}

fn activity_owner(operation: ConnectorDocumentManagementOperation) -> MvActivityOwner {
    match operation {
        ConnectorDocumentManagementOperation::Create => MvActivityOwner::Create,
        ConnectorDocumentManagementOperation::SingleTargetUpdate => MvActivityOwner::Alter,
        ConnectorDocumentManagementOperation::Publication => MvActivityOwner::ManualRefresh,
    }
}

fn record_unsettled(
    entrance: &Weak<EntranceInner>,
    responsibility: EffectResponsibility,
) -> Result<(), ManagementAdmissionError> {
    let entrance = entrance
        .upgrade()
        .ok_or(ManagementAdmissionError::EntranceDropped)?;
    let table = responsibility.target().table().clone();
    let unknown = match responsibility
        .clone()
        .record_terminal(EffectDisposition::CommitUnknown)
    {
        super::EffectTerminalFact::CommitUnknown(unknown) => unknown,
        _ => unreachable!("explicit CommitUnknown always yields an unsettled effect"),
    };
    let mut state = lock(&entrance.state);
    let target = state.entry(table).or_insert_with(|| TargetAdmissionState {
        target: responsibility.target().clone(),
        dependencies: ManagementDependencySet::new([0; 32], [0; 32], None, 0),
        unsettled: HashMap::new(),
        pending_committed_effect: None,
        pending_continuation: None,
        pending_observation: None,
        installed_observation: None,
        ready: false,
    });
    if target.target != *responsibility.target()
        || target.pending_committed_effect.is_some()
        || target.pending_observation.is_some()
    {
        return Err(ManagementAdmissionError::InvalidEffect);
    }
    target.ready = false;
    target.pending_committed_effect = None;
    target.pending_continuation = Some(ManagementContinuation::SameOwner {
        previous_incarnation: responsibility.dispatching_incarnation().clone(),
    });
    target.pending_observation = None;
    target.installed_observation = None;
    target
        .unsettled
        .entry(responsibility.identity())
        .or_insert(unknown);
    Ok(())
}

fn matching_authorization(
    expected: Option<&ManagementObservationAuthorization>,
    actual: Option<&ManagementObservationAuthorization>,
) -> bool {
    match (expected, actual) {
        (Some(expected), Some(actual)) => expected.matches(actual),
        (None, None) => true,
        _ => false,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
