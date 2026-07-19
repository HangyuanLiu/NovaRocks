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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

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
};
use super::operation::{
    ReadBackCertainty, candidate_mismatch, classify_commit, recover_commit, transaction_id,
};
use super::{
    AttemptId, ControlPlaneMode, CoordinationError, CoordinationErrorKind, FencingToken, HolderId,
    LeaseObservation, ResourceEpoch, ResourceKey,
};

const ACQUIRE_PURPOSE: &str = "acquire fenced resource lease";

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
    metrics: CoordinationMetrics,
}

pub struct LeaseGuard {
    token: FencingToken,
    _manager: LeaseManager,
    _resource: ResourceKey,
    _attempt: AttemptId,
    _record_version: VersionToken,
}

impl fmt::Debug for LeaseGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseGuard")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl LeaseGuard {
    pub const fn token(&self) -> &FencingToken {
        &self.token
    }
}

impl LeaseManager {
    pub fn new(
        store: Arc<dyn StateStore>,
        holder: HolderId,
        clock: Arc<dyn LeaseClock>,
        settings: LeaseSettings,
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
                metrics: CoordinationMetrics::new(),
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
                if current.control.record.mode != ControlPlaneMode::WriteOpen {
                    return Err(read_back_mismatch(
                        certainty,
                        transaction_id,
                        current.control.record.incarnation,
                        lease.record.incarnation,
                    ));
                }
                return Ok(AcquireOutcome::Acquired(self.guard(
                    &current.control.record,
                    lease.record.epoch,
                    resource,
                    attempt,
                    lease.state.version,
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
        require_write_open(&loaded.control.record)?;
        match self.decision(&loaded, &resource, attempt, operation_id, wall_now)? {
            AcquireDecision::Immediate(outcome) => Ok(outcome),
            AcquireDecision::Write(candidate) => {
                self.write_candidate(loaded, candidate, wall_now).await
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
            return Ok(AcquireDecision::Write(self.candidate(
                resource.clone(),
                attempt,
                operation_id,
                control.incarnation,
                ResourceEpoch::new(1)?,
                wall_now,
            )?));
        };
        if lease.record.incarnation > control.incarnation {
            return Err(CoordinationError::corruption());
        }
        if lease.record.incarnation < control.incarnation {
            return Ok(AcquireDecision::Write(self.candidate(
                resource.clone(),
                attempt,
                operation_id,
                control.incarnation,
                lease.record.epoch.checked_next()?,
                wall_now,
            )?));
        }

        match lease.record.state {
            LeaseState::Held
                if lease.record.holder == self.inner.holder && lease.record.attempt == attempt =>
            {
                Ok(AcquireDecision::Immediate(AcquireOutcome::Acquired(
                    self.guard(
                        control,
                        lease.record.epoch,
                        resource.clone(),
                        attempt,
                        lease.state.version.clone(),
                    )?,
                )))
            }
            LeaseState::Held => {
                let token = token(control, lease.record.epoch)?;
                match lease
                    .record
                    .deadline_ms
                    .checked_add(self.inner.settings.max_clock_skew_ms)
                {
                    Some(expiry_boundary) if wall_now > expiry_boundary => Ok(
                        AcquireDecision::Immediate(AcquireOutcome::AwaitingTakeover(
                            LeaseObservation::new(token, self.inner.settings.observation_window()),
                        )),
                    ),
                    expiry_boundary => {
                        let retry_ms = expiry_boundary
                            .map(|boundary| boundary.saturating_sub(wall_now).max(1))
                            .unwrap_or(self.inner.settings.lease_duration_ms);
                        Ok(AcquireDecision::Immediate(AcquireOutcome::Contended(
                            LeaseObservation::new(token, Duration::from_millis(retry_ms)),
                        )))
                    }
                }
            }
            LeaseState::Released
                if lease.record.holder == self.inner.holder && lease.record.attempt == attempt =>
            {
                Err(CoordinationError::invalid_request(
                    "released lease attempt cannot be reacquired",
                ))
            }
            LeaseState::Released => Ok(AcquireDecision::Write(self.candidate(
                resource.clone(),
                attempt,
                operation_id,
                control.incarnation,
                lease.record.epoch.checked_next()?,
                wall_now,
            )?)),
        }
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
                AcquireDecision::Write(_) => Err(CoordinationError::fence_lost()),
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
        self.read_back_candidate(&loaded.control, candidate, certainty, wall_now)
            .await
    }

    async fn read_back_candidate(
        &self,
        expected_control: &LoadedControl,
        candidate: LeaseRecord,
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
                candidate.epoch,
                resource,
                attempt,
                lease.state.version.clone(),
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
                AcquireDecision::Write(_) => {
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

    fn guard(
        &self,
        control: &ControlRecord,
        epoch: ResourceEpoch,
        resource: ResourceKey,
        attempt: AttemptId,
        record_version: VersionToken,
    ) -> Result<LeaseGuard, CoordinationError> {
        Ok(LeaseGuard {
            token: token(control, epoch)?,
            _manager: self.clone(),
            _resource: resource,
            _attempt: attempt,
            _record_version: record_version,
        })
    }

    fn record_result(&self, result: &Result<AcquireOutcome, CoordinationError>) {
        let outcome = match result {
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
}

enum AcquireDecision {
    Immediate(AcquireOutcome),
    Write(LeaseRecord),
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

fn validate_identity(
    control: &ControlRecord,
    identity: &StoreIdentity,
) -> Result<(), CoordinationError> {
    if control.store_id != identity.store_id || control.cluster_id != identity.cluster_id {
        return Err(CoordinationError::corruption());
    }
    Ok(())
}

fn require_write_open(control: &ControlRecord) -> Result<(), CoordinationError> {
    if control.mode != ControlPlaneMode::WriteOpen {
        return Err(CoordinationError::write_closed());
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
    if current.mode != ControlPlaneMode::WriteOpen {
        return Err(CoordinationError::write_closed());
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

    use super::{LeaseManager, LeaseSettings};
    use crate::coordination::codec::{ControlRecord, control_storage_key, encode_control};
    use crate::coordination::{
        AttemptId, ClockHealth, ControlPlaneIncarnation, ControlPlaneMode, CoordinationError,
        CoordinationErrorKind, HolderId, LeaseClock, ResourceKey,
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
}
