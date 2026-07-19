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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::{
    OperationId, Precondition, StateRecord, StateStore, StoreIdentity, TransactionId, VersionToken,
};

use super::clock::{ClockHealth, LeaseClock, LeaseSettings};
use super::codec::{
    ControlRecord, LeaseRecord, LeaseState, control_storage_key, decode_control, decode_lease,
    encode_lease, lease_storage_key,
};
use super::gate::validate_write_limits_with_read_keys;
use super::metrics::{
    CoordinationMetrics, CoordinationMetricsSnapshot, CoordinationOperation, CoordinationOutcome,
    error_outcome,
};
use super::operation::{
    ReadBackCertainty, candidate_mismatch, classify_commit, recover_commit, transaction_id,
};
use super::{
    AttemptId, CoordinationError, CoordinationErrorKind, FencingToken, HolderId,
    LeaseCancellationReason, LeaseFence, LeaseObservation, ResourceEpoch, ResourceKey,
};

const ACQUIRE_PURPOSE: &str = "acquire fenced resource lease";
const RENEW_PURPOSE: &str = "renew fenced resource lease";
const RELEASE_PURPOSE: &str = "release fenced resource lease";

#[derive(Debug)]
pub enum AcquireOutcome {
    Acquired(LeaseGuard),
    Contended(LeaseObservation),
    AwaitingTakeover(LeaseObservation),
}

#[derive(Clone)]
pub struct LeaseManager {
    inner: Arc<LeaseManagerInner>,
}

struct LeaseManagerInner {
    store: Arc<dyn StateStore>,
    holder: HolderId,
    clock: Arc<dyn LeaseClock>,
    settings: LeaseSettings,
    metrics: Arc<CoordinationMetrics>,
    expiry_observations: Mutex<HashMap<[u8; 32], ExpiryObservation>>,
}

pub struct LeaseGuard {
    fence: Box<LeaseFence>,
    manager: LeaseManager,
    deadline_ms: u64,
    renewed_ms: u64,
    recovery: Option<LeaseMutationRecovery>,
    active: bool,
    acquired_by_takeover: bool,
    cancellation_tx: watch::Sender<Option<LeaseCancellationReason>>,
}

struct ExpiryObservation {
    version: VersionToken,
    first_seen_monotonic_ms: u64,
}

struct LeaseMutationSuccess {
    record: LeaseRecord,
    version: VersionToken,
}

#[derive(Clone)]
struct LeaseMutationRecoveryEvidence {
    operation_id: OperationId,
    state: LeaseState,
    deadline_ms: u64,
    renewed_ms: u64,
}

struct LeaseMutationRecovery {
    evidence: LeaseMutationRecoveryEvidence,
    pending: bool,
}

impl fmt::Debug for LeaseGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseGuard")
            .field("token", &self.fence.token)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl LeaseGuard {
    pub const fn token(&self) -> &FencingToken {
        &self.fence.token
    }

    pub fn fence(&self) -> LeaseFence {
        self.fence.as_ref().clone()
    }

    pub fn renew_after(&self) -> Duration {
        self.manager.inner.settings.renew_interval()
    }

    pub fn cancellation(&self) -> watch::Receiver<Option<LeaseCancellationReason>> {
        self.cancellation_tx.subscribe()
    }

    pub async fn renew(&mut self, operation_id: OperationId) -> Result<(), CoordinationError> {
        if !self.active {
            return Err(CoordinationError::fence_lost());
        }
        self.ensure_no_pending_mutation()?;
        let candidate = match self.manager.renew_candidate(
            &self.fence,
            self.deadline_ms,
            self.renewed_ms,
            operation_id,
        ) {
            Ok(candidate) => candidate,
            Err(error) => {
                self.cancel_for_error(&error);
                return Err(error);
            }
        };
        self.begin_mutation_recovery(&candidate)?;
        let result = self.manager.renew_exact(&self.fence, candidate).await;
        self.apply_renew_result(result)
    }

    pub async fn recover_renew(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), CoordinationError> {
        if !self.active {
            return Err(CoordinationError::fence_lost());
        }
        let evidence = self.recovery_evidence(operation_id, LeaseState::Held)?;
        let result = self
            .manager
            .recover_renew_exact(&self.fence, &evidence)
            .await;
        self.apply_renew_result(result)
    }

    pub async fn release(&mut self, operation_id: OperationId) -> Result<(), CoordinationError> {
        if !self.active {
            return Err(CoordinationError::fence_lost());
        }
        self.ensure_no_pending_mutation()?;
        let candidate = lease_candidate_from_fence(
            &self.fence,
            LeaseState::Released,
            self.deadline_ms,
            self.renewed_ms,
            operation_id,
        );
        self.begin_mutation_recovery(&candidate)?;
        let result = self.manager.release_exact(&self.fence, candidate).await;
        self.apply_release_result(result)
    }

    pub async fn recover_release(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), CoordinationError> {
        let evidence = self.recovery_evidence(operation_id, LeaseState::Released)?;
        let result = self
            .manager
            .recover_release_exact(&self.fence, &evidence)
            .await;
        self.apply_release_result(result)
    }

    fn apply_renew_result(
        &mut self,
        result: Result<LeaseMutationSuccess, CoordinationError>,
    ) -> Result<(), CoordinationError> {
        match result {
            Ok(success) => {
                self.apply_success(success);
                self.resolve_mutation_recovery();
                Ok(())
            }
            Err(error) => {
                self.clear_definite_mutation_recovery(&error);
                self.cancel_for_error(&error);
                Err(error)
            }
        }
    }

    fn apply_release_result(
        &mut self,
        result: Result<LeaseMutationSuccess, CoordinationError>,
    ) -> Result<(), CoordinationError> {
        match result {
            Ok(success) => {
                self.apply_success(success);
                self.resolve_mutation_recovery();
                self.active = false;
                self.cancellation_tx
                    .send_replace(Some(LeaseCancellationReason::Released));
                Ok(())
            }
            Err(error) => {
                self.clear_definite_mutation_recovery(&error);
                self.cancel_for_error(&error);
                Err(error)
            }
        }
    }

    fn apply_success(&mut self, success: LeaseMutationSuccess) {
        self.fence.record_version = success.version;
        self.deadline_ms = success.record.deadline_ms;
        self.renewed_ms = success.record.renewed_ms;
    }

    fn ensure_no_pending_mutation(&self) -> Result<(), CoordinationError> {
        if self
            .recovery
            .as_ref()
            .is_some_and(|recovery| recovery.pending)
        {
            return Err(CoordinationError::invalid_request(
                "pending lease mutation must be recovered before starting another mutation",
            ));
        }
        Ok(())
    }

    fn begin_mutation_recovery(
        &mut self,
        candidate: &LeaseRecord,
    ) -> Result<(), CoordinationError> {
        self.ensure_no_pending_mutation()?;
        self.recovery = Some(LeaseMutationRecovery {
            evidence: LeaseMutationRecoveryEvidence {
                operation_id: candidate.last_operation_id,
                state: candidate.state,
                deadline_ms: candidate.deadline_ms,
                renewed_ms: candidate.renewed_ms,
            },
            pending: true,
        });
        Ok(())
    }

    fn recovery_evidence(
        &self,
        operation_id: OperationId,
        state: LeaseState,
    ) -> Result<LeaseMutationRecoveryEvidence, CoordinationError> {
        self.recovery
            .as_ref()
            .filter(|recovery| {
                recovery.evidence.operation_id == operation_id && recovery.evidence.state == state
            })
            .map(|recovery| recovery.evidence.clone())
            .ok_or_else(|| {
                CoordinationError::invalid_request(
                    "lease recovery must match the guard's exact mutation evidence",
                )
            })
    }

    fn resolve_mutation_recovery(&mut self) {
        if let Some(recovery) = &mut self.recovery {
            recovery.pending = false;
        }
    }

    fn clear_definite_mutation_recovery(&mut self, error: &CoordinationError) {
        if !matches!(
            error.kind(),
            CoordinationErrorKind::CommitUncertain | CoordinationErrorKind::StoreUnavailable
        ) {
            self.recovery = None;
        }
    }

    fn cancel_for_error(&mut self, error: &CoordinationError) {
        let reason = match error.kind() {
            CoordinationErrorKind::FenceLost => {
                self.active = false;
                Some(LeaseCancellationReason::FenceLost)
            }
            CoordinationErrorKind::CommitUncertain => Some(LeaseCancellationReason::FenceLost),
            CoordinationErrorKind::IncarnationChanged => {
                self.active = false;
                Some(LeaseCancellationReason::IncarnationChanged)
            }
            CoordinationErrorKind::ClockUnsafe => Some(LeaseCancellationReason::ClockUnsafe),
            _ => None,
        };
        if let Some(reason) = reason {
            self.cancellation_tx.send_replace(Some(reason));
        }
    }
}

impl LeaseFence {
    pub async fn validate_in(
        &self,
        transaction: &mut dyn crate::WriteTransaction,
    ) -> Result<(), CoordinationError> {
        let result = async {
            let control_key = control_storage_key()?;
            let lease_key = lease_storage_key(&self.resource)?;
            let control_state = transaction
                .get(&control_key)
                .await
                .map_err(CoordinationError::from_state_store)?
                .ok_or_else(CoordinationError::not_bootstrapped)?;
            let control = decode_control(&control_state.value)?;
            if control.store_id != self.store_id || control.cluster_id != self.token.cluster_id() {
                return Err(CoordinationError::corruption());
            }
            if control.incarnation != self.token.control_plane_incarnation() {
                return Err(CoordinationError::incarnation_changed());
            }
            let lease_state = transaction
                .get(&lease_key)
                .await
                .map_err(CoordinationError::from_state_store)?
                .ok_or_else(CoordinationError::fence_lost)?;
            let lease = decode_lease(&lease_key, &lease_state.value)?;
            if !held_lease_matches_fence(&lease, &lease_state.version, self) {
                return Err(CoordinationError::fence_lost());
            }
            Ok(())
        }
        .await;
        let outcome = match &result {
            Ok(()) => Some(CoordinationOutcome::Success),
            Err(error) => error_outcome(error),
        };
        if let Some(outcome) = outcome {
            self.metrics
                .record(CoordinationOperation::ValidateFence, outcome);
        }
        result
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if !self.active
            || self
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.pending)
        {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let manager = self.manager.clone();
        let fence = self.fence.clone();
        let deadline_ms = self.deadline_ms;
        let renewed_ms = self.renewed_ms;
        handle.spawn(async move {
            let operation_id = OperationId::new_v7();
            let candidate = lease_candidate_from_fence(
                &fence,
                LeaseState::Released,
                deadline_ms,
                renewed_ms,
                operation_id,
            );
            let _ = manager.release_exact(&fence, candidate).await;
        });
    }
}

impl LeaseManager {
    pub fn new(
        store: Arc<dyn StateStore>,
        holder: HolderId,
        clock: Arc<dyn LeaseClock>,
        settings: LeaseSettings,
    ) -> Result<Self, CoordinationError> {
        Self::with_metrics(
            store,
            holder,
            clock,
            settings,
            Arc::new(CoordinationMetrics::new()),
        )
    }

    pub fn with_metrics(
        store: Arc<dyn StateStore>,
        holder: HolderId,
        clock: Arc<dyn LeaseClock>,
        settings: LeaseSettings,
        metrics: Arc<CoordinationMetrics>,
    ) -> Result<Self, CoordinationError> {
        let control_key = control_storage_key()?;
        let representative_resource = ResourceKey::try_from(Bytes::from_static(b"lease-key"))?;
        let lease_key = lease_storage_key(&representative_resource)?;
        if control_key.as_bytes().len() > store.limits().max_key_bytes
            || lease_key.as_bytes().len() > store.limits().max_key_bytes
        {
            return Err(CoordinationError::limit_exceeded(
                "coordination storage key exceeds state store limits",
            ));
        }
        Ok(Self {
            inner: Arc::new(LeaseManagerInner {
                store,
                holder,
                clock,
                settings,
                metrics,
                expiry_observations: Mutex::new(HashMap::new()),
            }),
        })
    }

    pub fn metrics_snapshot(&self) -> CoordinationMetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    pub async fn acquire(
        &self,
        resource: ResourceKey,
        attempt: AttemptId,
        operation_id: OperationId,
    ) -> Result<AcquireOutcome, CoordinationError> {
        let result = self.acquire_inner(resource, attempt, operation_id).await;
        self.record_result(&result);
        result
    }

    pub async fn recover_acquire(
        &self,
        resource: ResourceKey,
        attempt: AttemptId,
        operation_id: OperationId,
    ) -> Result<AcquireOutcome, CoordinationError> {
        let transaction_id = transaction_id(operation_id);
        let result = async {
            let certainty = recover_commit(self.inner.store.as_ref(), transaction_id).await?;
            let current = self.read_coordination(&resource).await?;
            let Some(lease) = current.lease else {
                return Err(read_back_mismatch(
                    certainty,
                    transaction_id,
                    current.control.record.incarnation,
                    current.control.record.incarnation,
                ));
            };
            if recovered_candidate_matches(
                &lease.record,
                &resource,
                &self.inner.holder,
                attempt,
                operation_id,
                self.inner.settings.lease_duration_ms,
            ) {
                if current.control.record.incarnation != lease.record.incarnation {
                    return Err(read_back_mismatch(
                        certainty,
                        transaction_id,
                        current.control.record.incarnation,
                        lease.record.incarnation,
                    ));
                }
                return Ok(AcquireOutcome::Acquired(self.guard(
                    &current.control.record,
                    &lease.record,
                    lease.state.version.clone(),
                    false,
                )?));
            }
            Err(read_back_mismatch(
                certainty,
                transaction_id,
                current.control.record.incarnation,
                current.control.record.incarnation,
            ))
        }
        .await;
        self.record_result(&result);
        result
    }

    async fn acquire_inner(
        &self,
        resource: ResourceKey,
        attempt: AttemptId,
        operation_id: OperationId,
    ) -> Result<AcquireOutcome, CoordinationError> {
        let wall_now = self.healthy_wall_time()?;
        let loaded = self.read_coordination(&resource).await?;
        match self.decision(&loaded, &resource, attempt, operation_id, wall_now)? {
            AcquireDecision::Immediate(outcome) => Ok(outcome),
            AcquireDecision::Write {
                candidate,
                takeover,
            } => {
                self.write_candidate(loaded, candidate, takeover, wall_now)
                    .await
            }
        }
    }

    fn healthy_wall_time(&self) -> Result<u64, CoordinationError> {
        if self.inner.clock.health() != ClockHealth::Healthy {
            return Err(CoordinationError::clock_unsafe());
        }
        self.inner
            .clock
            .wall_time_millis()
            .map_err(|_| CoordinationError::clock_unsafe())
    }

    fn decision(
        &self,
        loaded: &LoadedCoordination,
        resource: &ResourceKey,
        attempt: AttemptId,
        operation_id: OperationId,
        wall_now: u64,
    ) -> Result<AcquireDecision, CoordinationError> {
        let control = &loaded.control.record;
        let Some(lease) = &loaded.lease else {
            self.clear_expiry_observation(resource);
            return Ok(AcquireDecision::Write {
                candidate: self.candidate(
                    resource.clone(),
                    attempt,
                    operation_id,
                    control.incarnation,
                    ResourceEpoch::new(1)?,
                    wall_now,
                )?,
                takeover: false,
            });
        };
        if lease.record.incarnation > control.incarnation {
            self.clear_expiry_observation(resource);
            return Err(CoordinationError::corruption());
        }
        if lease.record.incarnation < control.incarnation {
            self.clear_expiry_observation(resource);
            return Ok(AcquireDecision::Write {
                candidate: self.candidate(
                    resource.clone(),
                    attempt,
                    operation_id,
                    control.incarnation,
                    lease.record.epoch.checked_next()?,
                    wall_now,
                )?,
                takeover: true,
            });
        }

        match lease.record.state {
            LeaseState::Held
                if lease.record.holder == self.inner.holder && lease.record.attempt == attempt =>
            {
                self.clear_expiry_observation(resource);
                Ok(AcquireDecision::Immediate(AcquireOutcome::Acquired(
                    self.guard(control, &lease.record, lease.state.version.clone(), false)?,
                )))
            }
            LeaseState::Held => {
                let current_token = token(control, lease.record.epoch)?;
                match lease
                    .record
                    .deadline_ms
                    .checked_add(self.inner.settings.max_clock_skew_ms)
                {
                    Some(expiry_boundary) if wall_now > expiry_boundary => self
                        .observe_expired_lease(
                            resource,
                            lease,
                            attempt,
                            operation_id,
                            control,
                            wall_now,
                        ),
                    Some(expiry_boundary) => {
                        self.clear_expiry_observation(resource);
                        let retry_ms = expiry_boundary.saturating_sub(wall_now).max(1);
                        Ok(AcquireDecision::Immediate(AcquireOutcome::Contended(
                            LeaseObservation::new(current_token, Duration::from_millis(retry_ms)),
                        )))
                    }
                    None => {
                        self.clear_expiry_observation(resource);
                        Err(CoordinationError::clock_unsafe())
                    }
                }
            }
            LeaseState::Released
                if lease.record.holder == self.inner.holder && lease.record.attempt == attempt =>
            {
                self.clear_expiry_observation(resource);
                Err(CoordinationError::invalid_request(
                    "released lease attempt cannot be reacquired",
                ))
            }
            LeaseState::Released => {
                self.clear_expiry_observation(resource);
                Ok(AcquireDecision::Write {
                    candidate: self.candidate(
                        resource.clone(),
                        attempt,
                        operation_id,
                        control.incarnation,
                        lease.record.epoch.checked_next()?,
                        wall_now,
                    )?,
                    takeover: false,
                })
            }
        }
    }

    fn observe_expired_lease(
        &self,
        resource: &ResourceKey,
        lease: &LoadedLease,
        attempt: AttemptId,
        operation_id: OperationId,
        control: &ControlRecord,
        wall_now: u64,
    ) -> Result<AcquireDecision, CoordinationError> {
        let token = token(control, lease.record.epoch)?;
        let monotonic_now = self.inner.clock.monotonic_time_millis();
        let digest = resource_digest(resource);
        let mut observations = self
            .inner
            .expiry_observations
            .lock()
            .expect("lease expiry observation lock poisoned");
        let elapsed = match observations.get(&digest) {
            Some(observation) if observation.version == lease.state.version => {
                monotonic_now.checked_sub(observation.first_seen_monotonic_ms)
            }
            _ => None,
        };
        let Some(elapsed) = elapsed else {
            observations.insert(
                digest,
                ExpiryObservation {
                    version: lease.state.version.clone(),
                    first_seen_monotonic_ms: monotonic_now,
                },
            );
            return Ok(AcquireDecision::Immediate(
                AcquireOutcome::AwaitingTakeover(LeaseObservation::new(
                    token,
                    self.inner.settings.observation_window(),
                )),
            ));
        };
        if elapsed < self.inner.settings.observation_window_ms {
            return Ok(AcquireDecision::Immediate(
                AcquireOutcome::AwaitingTakeover(LeaseObservation::new(
                    token,
                    Duration::from_millis(self.inner.settings.observation_window_ms - elapsed),
                )),
            ));
        }
        observations.remove(&digest);
        drop(observations);
        Ok(AcquireDecision::Write {
            candidate: self.candidate(
                resource.clone(),
                attempt,
                operation_id,
                control.incarnation,
                lease.record.epoch.checked_next()?,
                wall_now,
            )?,
            takeover: true,
        })
    }

    fn clear_expiry_observation(&self, resource: &ResourceKey) {
        self.inner
            .expiry_observations
            .lock()
            .expect("lease expiry observation lock poisoned")
            .remove(&resource_digest(resource));
    }

    fn candidate(
        &self,
        resource: ResourceKey,
        attempt: AttemptId,
        operation_id: OperationId,
        incarnation: super::ControlPlaneIncarnation,
        epoch: ResourceEpoch,
        wall_now: u64,
    ) -> Result<LeaseRecord, CoordinationError> {
        let deadline_ms = wall_now
            .checked_add(self.inner.settings.lease_duration_ms)
            .ok_or_else(CoordinationError::clock_unsafe)?;
        Ok(LeaseRecord {
            resource,
            state: LeaseState::Held,
            holder: self.inner.holder.clone(),
            attempt,
            incarnation,
            epoch,
            deadline_ms,
            renewed_ms: wall_now,
            last_operation_id: operation_id,
        })
    }

    async fn write_candidate(
        &self,
        loaded: LoadedCoordination,
        candidate: LeaseRecord,
        takeover: bool,
        wall_now: u64,
    ) -> Result<AcquireOutcome, CoordinationError> {
        let resource = candidate.resource.clone();
        let attempt = candidate.attempt;
        let control_key = control_storage_key()?;
        let lease_key = lease_storage_key(&resource)?;
        let value = encode_lease(&candidate)?;
        let expected_version = loaded.lease.as_ref().map(|lease| &lease.state.version);
        validate_write_limits_with_read_keys(
            self.inner.store.limits(),
            &lease_key,
            &value,
            expected_version,
            &[control_key.as_bytes().len(), lease_key.as_bytes().len()],
        )?;
        let transaction_id = transaction_id(candidate.last_operation_id);
        let mut transaction = self
            .inner
            .store
            .begin_write(transaction_id, ACQUIRE_PURPOSE)
            .await
            .map_err(CoordinationError::from_state_store)?;

        let current_control_state = transaction
            .get(&control_key)
            .await
            .map_err(CoordinationError::from_state_store)?
            .ok_or_else(CoordinationError::not_bootstrapped)?;
        let current_control = decode_control(&current_control_state.value)?;
        if let Err(error) =
            validate_exact_control(&loaded.control, &current_control_state, &current_control)
        {
            transaction
                .abort()
                .await
                .map_err(CoordinationError::from_state_store)?;
            return Err(error);
        }

        let current_lease_state = transaction
            .get(&lease_key)
            .await
            .map_err(CoordinationError::from_state_store)?;
        let current_lease = current_lease_state
            .map(|state| {
                let record = decode_lease(&lease_key, &state.value)?;
                Ok::<_, CoordinationError>(LoadedLease { state, record })
            })
            .transpose()?;
        if !same_loaded_lease(loaded.lease.as_ref(), current_lease.as_ref()) {
            transaction
                .abort()
                .await
                .map_err(CoordinationError::from_state_store)?;
            let raced = LoadedCoordination {
                control: LoadedControl {
                    state: current_control_state,
                    record: current_control,
                },
                lease: current_lease,
            };
            return match self.decision(
                &raced,
                &resource,
                attempt,
                candidate.last_operation_id,
                wall_now,
            )? {
                AcquireDecision::Immediate(outcome) => Ok(outcome),
                AcquireDecision::Write { .. } => Err(CoordinationError::fence_lost()),
            };
        }

        let precondition = expected_version
            .cloned()
            .map(Precondition::Version)
            .unwrap_or(Precondition::Absent);
        transaction
            .put(lease_key, value, precondition)
            .await
            .map_err(CoordinationError::from_state_store)?;
        let certainty = classify_commit(
            self.inner.store.as_ref(),
            transaction_id,
            transaction.commit().await,
        )
        .await?;
        self.read_back_candidate(&loaded.control, candidate, takeover, certainty, wall_now)
            .await
    }

    async fn read_back_candidate(
        &self,
        expected_control: &LoadedControl,
        candidate: LeaseRecord,
        takeover: bool,
        certainty: ReadBackCertainty,
        wall_now: u64,
    ) -> Result<AcquireOutcome, CoordinationError> {
        let resource = candidate.resource.clone();
        let attempt = candidate.attempt;
        let transaction_id = transaction_id(candidate.last_operation_id);
        let current = self.read_coordination(&resource).await?;
        if current.control.record != expected_control.record
            || current.control.state.version != expected_control.state.version
        {
            return Err(read_back_mismatch(
                certainty,
                transaction_id,
                current.control.record.incarnation,
                candidate.incarnation,
            ));
        }
        if let Some(lease) = &current.lease
            && lease.record == candidate
        {
            return Ok(AcquireOutcome::Acquired(self.guard(
                &current.control.record,
                &candidate,
                lease.state.version.clone(),
                takeover,
            )?));
        }
        if certainty == ReadBackCertainty::Conflict {
            return match self.decision(
                &current,
                &resource,
                attempt,
                candidate.last_operation_id,
                wall_now,
            )? {
                AcquireDecision::Immediate(outcome) => Ok(outcome),
                AcquireDecision::Write { .. } => {
                    Err(CoordinationError::operation_not_committed(transaction_id))
                }
            };
        }
        Err(read_back_mismatch(
            certainty,
            transaction_id,
            current.control.record.incarnation,
            candidate.incarnation,
        ))
    }

    async fn read_coordination(
        &self,
        resource: &ResourceKey,
    ) -> Result<LoadedCoordination, CoordinationError> {
        let identity = self
            .inner
            .store
            .identity()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let control_key = control_storage_key()?;
        let lease_key = lease_storage_key(resource)?;
        let mut transaction = self
            .inner
            .store
            .begin_read()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let control_state = transaction
            .get(&control_key)
            .await
            .map_err(CoordinationError::from_state_store)?
            .ok_or_else(CoordinationError::not_bootstrapped)?;
        let lease_state = transaction
            .get(&lease_key)
            .await
            .map_err(CoordinationError::from_state_store)?;
        transaction
            .abort()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let control_record = decode_control(&control_state.value)?;
        validate_identity(&control_record, &identity)?;
        let lease = lease_state
            .map(|state| {
                let record = decode_lease(&lease_key, &state.value)?;
                Ok::<_, CoordinationError>(LoadedLease { state, record })
            })
            .transpose()?;
        Ok(LoadedCoordination {
            control: LoadedControl {
                state: control_state,
                record: control_record,
            },
            lease,
        })
    }

    fn renew_candidate(
        &self,
        fence: &LeaseFence,
        current_deadline_ms: u64,
        current_renewed_ms: u64,
        operation_id: OperationId,
    ) -> Result<LeaseRecord, CoordinationError> {
        let result = (|| {
            let wall_now = self.healthy_wall_time()?;
            let deadline_ms = wall_now
                .checked_add(self.inner.settings.lease_duration_ms)
                .ok_or_else(CoordinationError::clock_unsafe)?;
            if wall_now < current_renewed_ms || deadline_ms < current_deadline_ms {
                return Err(CoordinationError::clock_unsafe());
            }
            Ok(lease_candidate_from_fence(
                fence,
                LeaseState::Held,
                deadline_ms,
                wall_now,
                operation_id,
            ))
        })();
        if let Err(error) = &result
            && let Some(outcome) = error_outcome(error)
        {
            self.inner
                .metrics
                .record(CoordinationOperation::Renew, outcome);
        }
        result
    }

    async fn renew_exact(
        &self,
        fence: &LeaseFence,
        candidate: LeaseRecord,
    ) -> Result<LeaseMutationSuccess, CoordinationError> {
        let result = self
            .write_exact_candidate(fence, candidate, RENEW_PURPOSE)
            .await;
        self.record_mutation_result(CoordinationOperation::Renew, &result);
        result
    }

    async fn recover_renew_exact(
        &self,
        fence: &LeaseFence,
        evidence: &LeaseMutationRecoveryEvidence,
    ) -> Result<LeaseMutationSuccess, CoordinationError> {
        let result = async {
            let transaction_id = transaction_id(evidence.operation_id);
            let certainty = recover_commit(self.inner.store.as_ref(), transaction_id).await?;
            self.read_back_recovered_mutation(fence, evidence, certainty, transaction_id)
                .await
        }
        .await;
        self.record_mutation_result(CoordinationOperation::Renew, &result);
        result
    }

    async fn release_exact(
        &self,
        fence: &LeaseFence,
        candidate: LeaseRecord,
    ) -> Result<LeaseMutationSuccess, CoordinationError> {
        let result = self
            .write_exact_candidate(fence, candidate, RELEASE_PURPOSE)
            .await;
        self.record_mutation_result(CoordinationOperation::Release, &result);
        result
    }

    async fn recover_release_exact(
        &self,
        fence: &LeaseFence,
        evidence: &LeaseMutationRecoveryEvidence,
    ) -> Result<LeaseMutationSuccess, CoordinationError> {
        let result = async {
            let transaction_id = transaction_id(evidence.operation_id);
            let certainty = recover_commit(self.inner.store.as_ref(), transaction_id).await?;
            self.read_back_recovered_mutation(fence, evidence, certainty, transaction_id)
                .await
        }
        .await;
        self.record_mutation_result(CoordinationOperation::Release, &result);
        result
    }

    async fn write_exact_candidate(
        &self,
        fence: &LeaseFence,
        candidate: LeaseRecord,
        purpose: &'static str,
    ) -> Result<LeaseMutationSuccess, CoordinationError> {
        let identity = self
            .inner
            .store
            .identity()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let control_key = control_storage_key()?;
        let lease_key = lease_storage_key(&fence.resource)?;
        let value = encode_lease(&candidate)?;
        validate_write_limits_with_read_keys(
            self.inner.store.limits(),
            &lease_key,
            &value,
            Some(&fence.record_version),
            &[control_key.as_bytes().len(), lease_key.as_bytes().len()],
        )?;
        let transaction_id = transaction_id(candidate.last_operation_id);
        let mut transaction = self
            .inner
            .store
            .begin_write(transaction_id, purpose)
            .await
            .map_err(CoordinationError::from_state_store)?;
        let control_state = transaction
            .get(&control_key)
            .await
            .map_err(CoordinationError::from_state_store)?
            .ok_or_else(CoordinationError::not_bootstrapped)?;
        let control = decode_control(&control_state.value)?;
        if let Err(error) = validate_control_fence(&control, &identity, fence) {
            transaction
                .abort()
                .await
                .map_err(CoordinationError::from_state_store)?;
            return Err(error);
        }
        let Some(lease_state) = transaction
            .get(&lease_key)
            .await
            .map_err(CoordinationError::from_state_store)?
        else {
            transaction
                .abort()
                .await
                .map_err(CoordinationError::from_state_store)?;
            return Err(CoordinationError::fence_lost());
        };
        let lease = decode_lease(&lease_key, &lease_state.value)?;
        if !held_lease_matches_fence(&lease, &lease_state.version, fence) {
            transaction
                .abort()
                .await
                .map_err(CoordinationError::from_state_store)?;
            return Err(CoordinationError::fence_lost());
        }
        transaction
            .put(
                lease_key,
                value,
                Precondition::Version(fence.record_version.clone()),
            )
            .await
            .map_err(CoordinationError::from_state_store)?;
        let certainty = classify_commit(
            self.inner.store.as_ref(),
            transaction_id,
            transaction.commit().await,
        )
        .await?;
        self.read_back_mutation_candidate(fence, candidate, certainty)
            .await
    }

    async fn read_back_mutation_candidate(
        &self,
        fence: &LeaseFence,
        candidate: LeaseRecord,
        certainty: ReadBackCertainty,
    ) -> Result<LeaseMutationSuccess, CoordinationError> {
        let transaction_id = transaction_id(candidate.last_operation_id);
        let current = self.read_coordination(&fence.resource).await?;
        if current.control.record.cluster_id != fence.token.cluster_id()
            || current.control.record.incarnation != fence.token.control_plane_incarnation()
        {
            return Err(read_back_mismatch(
                certainty,
                transaction_id,
                current.control.record.incarnation,
                fence.token.control_plane_incarnation(),
            ));
        }
        if let Some(lease) = &current.lease
            && lease.record == candidate
        {
            return Ok(LeaseMutationSuccess {
                record: lease.record.clone(),
                version: lease.state.version.clone(),
            });
        }
        if certainty == ReadBackCertainty::Conflict
            && current.lease.as_ref().is_some_and(|lease| {
                held_lease_matches_fence(&lease.record, &lease.state.version, fence)
            })
        {
            return Err(CoordinationError::operation_not_committed(transaction_id));
        }
        Err(read_back_mismatch(
            certainty,
            transaction_id,
            current.control.record.incarnation,
            fence.token.control_plane_incarnation(),
        ))
    }

    async fn read_back_recovered_mutation(
        &self,
        fence: &LeaseFence,
        evidence: &LeaseMutationRecoveryEvidence,
        certainty: ReadBackCertainty,
        transaction_id: TransactionId,
    ) -> Result<LeaseMutationSuccess, CoordinationError> {
        let current = self.read_coordination(&fence.resource).await?;
        if current.control.record.cluster_id != fence.token.cluster_id()
            || current.control.record.incarnation != fence.token.control_plane_incarnation()
        {
            return Err(read_back_mismatch(
                certainty,
                transaction_id,
                current.control.record.incarnation,
                fence.token.control_plane_incarnation(),
            ));
        }
        if let Some(lease) = &current.lease
            && recovered_mutation_matches(&lease.record, fence, evidence)
        {
            return Ok(LeaseMutationSuccess {
                record: lease.record.clone(),
                version: lease.state.version.clone(),
            });
        }
        Err(read_back_mismatch(
            certainty,
            transaction_id,
            current.control.record.incarnation,
            fence.token.control_plane_incarnation(),
        ))
    }

    fn guard(
        &self,
        control: &ControlRecord,
        record: &LeaseRecord,
        record_version: VersionToken,
        acquired_by_takeover: bool,
    ) -> Result<LeaseGuard, CoordinationError> {
        let (cancellation_tx, _) = watch::channel(None);
        Ok(LeaseGuard {
            fence: Box::new(LeaseFence {
                store_id: control.store_id,
                resource: record.resource.clone(),
                holder: record.holder.clone(),
                attempt: record.attempt,
                token: token(control, record.epoch)?,
                record_version,
                metrics: Arc::clone(&self.inner.metrics),
            }),
            manager: self.clone(),
            deadline_ms: record.deadline_ms,
            renewed_ms: record.renewed_ms,
            recovery: None,
            active: record.state == LeaseState::Held,
            acquired_by_takeover,
            cancellation_tx,
        })
    }

    fn record_result(&self, result: &Result<AcquireOutcome, CoordinationError>) {
        let outcome = match result {
            Ok(AcquireOutcome::Acquired(guard)) if guard.acquired_by_takeover => {
                Some(CoordinationOutcome::Takeover)
            }
            Ok(AcquireOutcome::Acquired(_)) => Some(CoordinationOutcome::Success),
            Ok(AcquireOutcome::Contended(_)) => Some(CoordinationOutcome::Contended),
            Ok(AcquireOutcome::AwaitingTakeover(_)) => Some(CoordinationOutcome::AwaitingTakeover),
            Err(error) => match error.kind() {
                CoordinationErrorKind::ClockUnsafe => Some(CoordinationOutcome::ClockUnsafe),
                CoordinationErrorKind::FenceLost => Some(CoordinationOutcome::FenceLost),
                CoordinationErrorKind::IncarnationChanged => {
                    Some(CoordinationOutcome::IncarnationChanged)
                }
                CoordinationErrorKind::WriteClosed => Some(CoordinationOutcome::WriteClosed),
                CoordinationErrorKind::OperationNotCommitted => {
                    Some(CoordinationOutcome::OperationNotCommitted)
                }
                CoordinationErrorKind::CommitUncertain => {
                    Some(CoordinationOutcome::CommitUncertain)
                }
                CoordinationErrorKind::Corruption => Some(CoordinationOutcome::Corruption),
                CoordinationErrorKind::StoreUnavailable => {
                    Some(CoordinationOutcome::StoreUnavailable)
                }
                CoordinationErrorKind::InvalidRequest
                | CoordinationErrorKind::LimitExceeded
                | CoordinationErrorKind::NotBootstrapped
                | CoordinationErrorKind::EpochExhausted
                | CoordinationErrorKind::IncarnationExhausted => None,
            },
        };
        if let Some(outcome) = outcome {
            self.inner
                .metrics
                .record(CoordinationOperation::Acquire, outcome);
        }
    }

    fn record_mutation_result(
        &self,
        operation: CoordinationOperation,
        result: &Result<LeaseMutationSuccess, CoordinationError>,
    ) {
        let outcome = match result {
            Ok(_) => Some(CoordinationOutcome::Success),
            Err(error) => error_outcome(error),
        };
        if let Some(outcome) = outcome {
            self.inner.metrics.record(operation, outcome);
        }
    }
}

enum AcquireDecision {
    Immediate(AcquireOutcome),
    Write {
        candidate: LeaseRecord,
        takeover: bool,
    },
}

struct LoadedCoordination {
    control: LoadedControl,
    lease: Option<LoadedLease>,
}

struct LoadedControl {
    state: StateRecord,
    record: ControlRecord,
}

struct LoadedLease {
    state: StateRecord,
    record: LeaseRecord,
}

fn resource_digest(resource: &ResourceKey) -> [u8; 32] {
    Sha256::digest(resource.as_bytes()).into()
}

fn lease_candidate_from_fence(
    fence: &LeaseFence,
    state: LeaseState,
    deadline_ms: u64,
    renewed_ms: u64,
    operation_id: OperationId,
) -> LeaseRecord {
    LeaseRecord {
        resource: fence.resource.clone(),
        state,
        holder: fence.holder.clone(),
        attempt: fence.attempt,
        incarnation: fence.token.control_plane_incarnation(),
        epoch: fence.token.resource_epoch(),
        deadline_ms,
        renewed_ms,
        last_operation_id: operation_id,
    }
}

fn validate_control_fence(
    control: &ControlRecord,
    identity: &StoreIdentity,
    fence: &LeaseFence,
) -> Result<(), CoordinationError> {
    validate_identity(control, identity)?;
    if control.cluster_id != fence.token.cluster_id() {
        return Err(CoordinationError::corruption());
    }
    if control.incarnation != fence.token.control_plane_incarnation() {
        return Err(CoordinationError::incarnation_changed());
    }
    Ok(())
}

fn held_lease_matches_fence(
    record: &LeaseRecord,
    version: &VersionToken,
    fence: &LeaseFence,
) -> bool {
    record.state == LeaseState::Held
        && record.resource == fence.resource
        && record.holder == fence.holder
        && record.attempt == fence.attempt
        && record.incarnation == fence.token.control_plane_incarnation()
        && record.epoch == fence.token.resource_epoch()
        && version == &fence.record_version
}

fn recovered_mutation_matches(
    record: &LeaseRecord,
    fence: &LeaseFence,
    evidence: &LeaseMutationRecoveryEvidence,
) -> bool {
    record.state == evidence.state
        && record.resource == fence.resource
        && record.holder == fence.holder
        && record.attempt == fence.attempt
        && record.incarnation == fence.token.control_plane_incarnation()
        && record.epoch == fence.token.resource_epoch()
        && record.last_operation_id == evidence.operation_id
        && record.deadline_ms == evidence.deadline_ms
        && record.renewed_ms == evidence.renewed_ms
}

fn validate_identity(
    control: &ControlRecord,
    identity: &StoreIdentity,
) -> Result<(), CoordinationError> {
    if control.store_id != identity.store_id || control.cluster_id != identity.cluster_id {
        return Err(CoordinationError::corruption());
    }
    Ok(())
}

fn validate_exact_control(
    expected: &LoadedControl,
    current_state: &StateRecord,
    current: &ControlRecord,
) -> Result<(), CoordinationError> {
    if current.store_id != expected.record.store_id
        || current.cluster_id != expected.record.cluster_id
    {
        return Err(CoordinationError::corruption());
    }
    if current.incarnation != expected.record.incarnation {
        return Err(CoordinationError::incarnation_changed());
    }
    if current != &expected.record || current_state.version != expected.state.version {
        return Err(CoordinationError::fence_lost());
    }
    Ok(())
}

fn same_loaded_lease(expected: Option<&LoadedLease>, current: Option<&LoadedLease>) -> bool {
    match (expected, current) {
        (None, None) => true,
        (Some(expected), Some(current)) => {
            expected.record == current.record && expected.state.version == current.state.version
        }
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn token(control: &ControlRecord, epoch: ResourceEpoch) -> Result<FencingToken, CoordinationError> {
    FencingToken::new(control.cluster_id.clone(), control.incarnation, epoch)
}

fn recovered_candidate_matches(
    record: &LeaseRecord,
    resource: &ResourceKey,
    holder: &HolderId,
    attempt: AttemptId,
    operation_id: OperationId,
    lease_duration_ms: u64,
) -> bool {
    record.state == LeaseState::Held
        && &record.resource == resource
        && &record.holder == holder
        && record.attempt == attempt
        && record.last_operation_id == operation_id
        && record
            .renewed_ms
            .checked_add(lease_duration_ms)
            .is_some_and(|deadline| deadline == record.deadline_ms)
}

fn read_back_mismatch(
    certainty: ReadBackCertainty,
    transaction_id: TransactionId,
    current_incarnation: super::ControlPlaneIncarnation,
    candidate_incarnation: super::ControlPlaneIncarnation,
) -> CoordinationError {
    candidate_mismatch(
        certainty,
        transaction_id,
        Some(current_incarnation),
        candidate_incarnation,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use uuid::Uuid;

    use super::{
        LeaseManager, LeaseMutationRecoveryEvidence, LeaseSettings, recovered_mutation_matches,
    };
    use crate::coordination::codec::{
        ControlRecord, LeaseRecord, LeaseState, control_storage_key, encode_control,
    };
    use crate::coordination::{
        AttemptId, ClockHealth, ControlPlaneIncarnation, ControlPlaneMode, CoordinationError,
        CoordinationErrorKind, FencingToken, HolderId, LeaseClock, LeaseFence, ResourceEpoch,
        ResourceKey,
    };
    use crate::{
        ChangePage, ChangePollRequest, CommitResolution, Key, OperationId, RangePage, RangeRequest,
        ReadTransaction, StateRecord, StateStore, StateStoreError, StateStoreErrorKind,
        StateStoreLimits, StateStoreMetrics, StateStoreMetricsSnapshot, StoreIdentity,
        TransactionId, VersionToken, WriteTransaction,
    };

    struct FixedClock {
        wall_readable: bool,
    }

    impl FixedClock {
        const fn healthy() -> Self {
            Self {
                wall_readable: true,
            }
        }

        const fn unreadable() -> Self {
            Self {
                wall_readable: false,
            }
        }
    }

    impl LeaseClock for FixedClock {
        fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
            if self.wall_readable {
                Ok(10_000)
            } else {
                Err(CoordinationError::clock_unsafe())
            }
        }

        fn monotonic_time_millis(&self) -> u64 {
            20_000
        }

        fn health(&self) -> ClockHealth {
            ClockHealth::Healthy
        }
    }

    struct ScriptedRead {
        control: StateRecord,
    }

    #[async_trait]
    impl ReadTransaction for ScriptedRead {
        async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
            if key == &self.control.key {
                Ok(Some(self.control.clone()))
            } else {
                Ok(None)
            }
        }

        async fn range(&mut self, _request: &RangeRequest) -> Result<RangePage, StateStoreError> {
            Err(unexpected_provider_call())
        }

        async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
            Ok(())
        }
    }

    struct CountingStore {
        limits: StateStoreLimits,
        identity: StoreIdentity,
        control: StateRecord,
        begin_writes: AtomicUsize,
        metrics: StateStoreMetrics,
    }

    impl CountingStore {
        fn new(mut limits: StateStoreLimits) -> Arc<Self> {
            let identity = StoreIdentity {
                store_id: Uuid::now_v7(),
                cluster_id: "lease-test-cluster".to_owned(),
                initial_incarnation: 1,
            };
            let control = ControlRecord::from_identity(
                &identity,
                ControlPlaneIncarnation::new(1).expect("incarnation"),
                ControlPlaneMode::WriteOpen,
                OperationId::new_v7(),
            );
            let control = StateRecord {
                key: control_storage_key().expect("control key"),
                value: encode_control(&control).expect("control value"),
                version: VersionToken::try_from(Bytes::from_static(b"control-version"))
                    .expect("version"),
            };
            limits.max_transaction_bytes = limits.max_transaction_bytes.max(4 * 1_024);
            Arc::new(Self {
                limits,
                identity,
                control,
                begin_writes: AtomicUsize::new(0),
                metrics: StateStoreMetrics::new("coordination-test"),
            })
        }
    }

    #[async_trait]
    impl StateStore for CountingStore {
        fn provider_name(&self) -> &'static str {
            "coordination-test"
        }

        fn limits(&self) -> &StateStoreLimits {
            &self.limits
        }

        fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
            self.metrics.snapshot()
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            Ok(Box::new(ScriptedRead {
                control: self.control.clone(),
            }))
        }

        async fn begin_write(
            &self,
            _transaction_id: TransactionId,
            _purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            self.begin_writes.fetch_add(1, Ordering::SeqCst);
            Err(unexpected_provider_call())
        }

        async fn poll_changes(
            &self,
            _request: &ChangePollRequest,
        ) -> Result<ChangePage, StateStoreError> {
            Err(unexpected_provider_call())
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            Ok(self.identity.clone())
        }

        async fn resolve_commit(
            &self,
            _transaction_id: &TransactionId,
        ) -> Result<CommitResolution, StateStoreError> {
            Ok(CommitResolution::NotCommitted)
        }
    }

    fn unexpected_provider_call() -> StateStoreError {
        StateStoreError::new(
            StateStoreErrorKind::Internal,
            "unexpected provider call in coordination unit test",
        )
    }

    fn settings() -> LeaseSettings {
        LeaseSettings::new(
            Duration::from_secs(10),
            Duration::from_secs(3),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect("settings")
    }

    #[tokio::test]
    async fn encoded_candidate_limit_is_rejected_before_begin_write() {
        let limits = StateStoreLimits {
            max_value_bytes: 80,
            ..StateStoreLimits::default()
        };
        let concrete = CountingStore::new(limits);
        let store: Arc<dyn StateStore> = concrete.clone();
        let manager = LeaseManager::new(
            store,
            HolderId::try_from(Bytes::from_static(b"holder-a")).expect("holder"),
            Arc::new(FixedClock::healthy()),
            settings(),
        )
        .expect("manager");

        let error = manager
            .acquire(
                ResourceKey::try_from(Bytes::from_static(b"candidate-too-large"))
                    .expect("resource"),
                AttemptId::try_from(Uuid::now_v7()).expect("attempt"),
                OperationId::new_v7(),
            )
            .await
            .expect_err("candidate must exceed tightened value limit");

        assert_eq!(error.kind(), CoordinationErrorKind::LimitExceeded);
        assert_eq!(concrete.begin_writes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn manager_rejects_fixed_storage_key_limit_without_provider_io() {
        let limits = StateStoreLimits {
            max_key_bytes: 54,
            ..StateStoreLimits::default()
        };
        let concrete = CountingStore::new(limits);
        let store: Arc<dyn StateStore> = concrete.clone();

        let error = LeaseManager::new(
            store,
            HolderId::try_from(Bytes::from_static(b"holder-a")).expect("holder"),
            Arc::new(FixedClock::healthy()),
            settings(),
        )
        .err()
        .expect("digest lease key is 55 bytes");

        assert_eq!(error.kind(), CoordinationErrorKind::LimitExceeded);
        assert_eq!(concrete.begin_writes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unreadable_healthy_clock_is_clock_unsafe() {
        let concrete = CountingStore::new(StateStoreLimits::default());
        let store: Arc<dyn StateStore> = concrete.clone();
        let manager = LeaseManager::new(
            store,
            HolderId::try_from(Bytes::from_static(b"holder-a")).expect("holder"),
            Arc::new(FixedClock::unreadable()),
            settings(),
        )
        .expect("manager");

        let error = manager
            .acquire(
                ResourceKey::try_from(Bytes::from_static(b"clock-unreadable")).expect("resource"),
                AttemptId::try_from(Uuid::now_v7()).expect("attempt"),
                OperationId::new_v7(),
            )
            .await
            .expect_err("unreadable wall time must fail closed");

        assert_eq!(error.kind(), CoordinationErrorKind::ClockUnsafe);
        assert_eq!(concrete.begin_writes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn recovered_mutation_requires_the_exact_pending_evidence() {
        let operation_id = OperationId::new_v7();
        let resource =
            ResourceKey::try_from(Bytes::from_static(b"recovery-resource")).expect("resource");
        let holder = HolderId::try_from(Bytes::from_static(b"recovery-holder")).expect("holder");
        let attempt = AttemptId::try_from(Uuid::now_v7()).expect("attempt");
        let incarnation = ControlPlaneIncarnation::new(1).expect("incarnation");
        let epoch = ResourceEpoch::new(1).expect("epoch");
        let fence = LeaseFence {
            store_id: Uuid::now_v7(),
            resource: resource.clone(),
            holder: holder.clone(),
            attempt,
            token: FencingToken::new("lease-test-cluster", incarnation, epoch).expect("token"),
            record_version: VersionToken::try_from(Bytes::from_static(b"lease-version"))
                .expect("version"),
            metrics: Arc::new(crate::coordination::CoordinationMetrics::new()),
        };
        let different_valid_candidate = LeaseRecord {
            resource,
            state: LeaseState::Held,
            holder,
            attempt,
            incarnation,
            epoch,
            renewed_ms: 12_000,
            deadline_ms: 22_000,
            last_operation_id: operation_id,
        };

        let exact_evidence = LeaseMutationRecoveryEvidence {
            operation_id,
            state: LeaseState::Held,
            renewed_ms: 11_000,
            deadline_ms: 21_000,
        };

        assert!(!recovered_mutation_matches(
            &different_valid_candidate,
            &fence,
            &exact_evidence,
        ));

        let mut different_release = different_valid_candidate;
        different_release.state = LeaseState::Released;
        let released_evidence = LeaseMutationRecoveryEvidence {
            state: LeaseState::Released,
            ..exact_evidence
        };
        assert!(!recovered_mutation_matches(
            &different_release,
            &fence,
            &released_evidence,
        ));
    }
}
