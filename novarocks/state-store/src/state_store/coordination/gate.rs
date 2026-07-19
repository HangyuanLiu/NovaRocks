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

use std::sync::Arc;

use crate::{
    Key, OperationId, Precondition, StateRecord, StateStore, StateStoreLimits, StoreIdentity,
    TransactionId, Value, WriteTransaction,
};

use super::codec::{ControlRecord, control_storage_key, decode_control, encode_control};
use super::metrics::{
    CoordinationMetrics, CoordinationMetricsSnapshot, CoordinationOperation, CoordinationOutcome,
    error_outcome,
};
use super::operation::{
    ReadBackCertainty, candidate_mismatch, classify_commit, recover_commit, transaction_id,
};
use super::{ControlPlaneIncarnation, ControlPlaneMode, ControlPlaneSnapshot, CoordinationError};

const BOOTSTRAP_PURPOSE: &str = "bootstrap control plane incarnation";
const RESTORE_PURPOSE: &str = "begin control plane restore";
const REOPEN_PURPOSE: &str = "open control plane writes";
const TRANSACTION_ID_BYTES: usize = 16;
const MUTATION_KIND_BYTES: usize = 1;
const PRECONDITION_KIND_BYTES: usize = 1;
const V1_NAMESPACE_BYTES: usize = 5 + 16;
const V1_RECORD_TAG_BYTES: usize = 1;
const V1_CHANGE_TAG_BYTES: usize = 1;
const V1_COMMIT_TAG_BYTES: usize = 1;
const V1_HIGH_WATERMARK_KEY_BYTES: usize = 2;
const V1_RECORD_FORMAT_BYTES: usize = 1;
const V1_PENDING_TAG_BYTES: usize = 1;
const V1_COMMITTED_TAG_BYTES: usize = 1;
const V1_RESERVATION_TOKEN_BYTES: usize = 16;
const V1_REVISION_BYTES: usize = 10;
const V1_SEQUENCE_BYTES: usize = 4;
const V1_VERSIONSTAMP_TRAILER_BYTES: usize = 4;
const V1_PROVISIONAL_VERSION_TAG_BYTES: usize = 22;
const V1_PROVISIONAL_OPERATION_BYTES: usize = 8;
const V1_PERSISTED_VERSION_BYTES: usize = 16;

pub struct IncarnationGate {
    store: Arc<dyn StateStore>,
    metrics: Arc<CoordinationMetrics>,
}

impl IncarnationGate {
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self::with_metrics(store, Arc::new(CoordinationMetrics::new()))
    }

    pub fn with_metrics(store: Arc<dyn StateStore>, metrics: Arc<CoordinationMetrics>) -> Self {
        Self { store, metrics }
    }

    pub fn metrics_snapshot(&self) -> CoordinationMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub async fn bootstrap(
        &self,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let result = async {
            let identity = self
                .store
                .identity()
                .await
                .map_err(CoordinationError::from_state_store)?;
            let candidate = bootstrap_candidate(&identity, operation_id)?;
            let key = control_storage_key()?;
            let value = encode_control(&candidate)?;
            validate_write_limits(self.store.limits(), &key, &value, None)?;
            let transaction_id = transaction_id(operation_id);
            let mut transaction = self
                .store
                .begin_write(transaction_id, BOOTSTRAP_PURPOSE)
                .await
                .map_err(CoordinationError::from_state_store)?;
            transaction
                .put(key, value, Precondition::Absent)
                .await
                .map_err(CoordinationError::from_state_store)?;
            let certainty = classify_commit(
                self.store.as_ref(),
                transaction_id,
                transaction.commit().await,
            )
            .await?;
            self.read_back_bootstrap(candidate, &identity, transaction_id, certainty)
                .await
        }
        .await;
        self.record_result(CoordinationOperation::Bootstrap, &result);
        result
    }

    pub async fn load(&self) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let result = async {
            let identity = self
                .store
                .identity()
                .await
                .map_err(CoordinationError::from_state_store)?;
            Ok(self.load_for_identity(&identity).await?.snapshot)
        }
        .await;
        self.record_result(CoordinationOperation::Load, &result);
        result
    }

    pub async fn begin_restore(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let result = async {
            if expected.mode() != ControlPlaneMode::WriteOpen {
                return Err(self.classify_invalid_expected(expected).await?);
            }
            let candidate = ControlRecord {
                store_id: expected.store_id(),
                cluster_id: expected.cluster_id().to_owned(),
                incarnation: expected.incarnation().checked_next()?,
                mode: ControlPlaneMode::Reconciling,
                last_operation_id: operation_id,
            };
            self.apply_exact(expected, candidate, operation_id, RESTORE_PURPOSE)
                .await
        }
        .await;
        self.record_result(CoordinationOperation::BeginRestore, &result);
        result
    }

    pub async fn open_writes(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let result = async {
            if expected.mode() != ControlPlaneMode::Reconciling {
                return Err(self.classify_invalid_expected(expected).await?);
            }
            let candidate = ControlRecord {
                store_id: expected.store_id(),
                cluster_id: expected.cluster_id().to_owned(),
                incarnation: expected.incarnation(),
                mode: ControlPlaneMode::WriteOpen,
                last_operation_id: operation_id,
            };
            self.apply_exact(expected, candidate, operation_id, REOPEN_PURPOSE)
                .await
        }
        .await;
        self.record_result(CoordinationOperation::OpenWrites, &result);
        result
    }

    pub async fn recover_bootstrap(
        &self,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let result = async {
            let transaction_id = transaction_id(operation_id);
            let certainty = recover_commit(self.store.as_ref(), transaction_id).await?;
            let identity = self
                .store
                .identity()
                .await
                .map_err(CoordinationError::from_state_store)?;
            let candidate = bootstrap_candidate(&identity, operation_id)?;
            self.read_back_candidate(candidate, transaction_id, certainty)
                .await
        }
        .await;
        self.record_result(CoordinationOperation::Bootstrap, &result);
        result
    }

    pub async fn recover_begin_restore(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let result = async {
            if expected.mode() != ControlPlaneMode::WriteOpen {
                return Err(CoordinationError::fence_lost());
            }
            let candidate = ControlRecord {
                store_id: expected.store_id(),
                cluster_id: expected.cluster_id().to_owned(),
                incarnation: expected.incarnation().checked_next()?,
                mode: ControlPlaneMode::Reconciling,
                last_operation_id: operation_id,
            };
            self.recover_candidate(candidate, operation_id).await
        }
        .await;
        self.record_result(CoordinationOperation::BeginRestore, &result);
        result
    }

    pub async fn recover_open_writes(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let result = async {
            if expected.mode() != ControlPlaneMode::Reconciling {
                return Err(CoordinationError::fence_lost());
            }
            let candidate = ControlRecord {
                store_id: expected.store_id(),
                cluster_id: expected.cluster_id().to_owned(),
                incarnation: expected.incarnation(),
                mode: ControlPlaneMode::WriteOpen,
                last_operation_id: operation_id,
            };
            self.recover_candidate(candidate, operation_id).await
        }
        .await;
        self.record_result(CoordinationOperation::OpenWrites, &result);
        result
    }

    pub async fn admit_writes(&self) -> Result<WriteAdmission, CoordinationError> {
        let result = async {
            let snapshot = self.load().await?;
            if snapshot.mode() != ControlPlaneMode::WriteOpen {
                return Err(CoordinationError::write_closed());
            }
            Ok(WriteAdmission {
                snapshot,
                metrics: Arc::clone(&self.metrics),
            })
        }
        .await;
        self.record_result(CoordinationOperation::AdmitWrites, &result);
        result
    }

    fn record_result<T>(
        &self,
        operation: CoordinationOperation,
        result: &Result<T, CoordinationError>,
    ) {
        let outcome = match result {
            Ok(_) => Some(CoordinationOutcome::Success),
            Err(error) => error_outcome(error),
        };
        if let Some(outcome) = outcome {
            self.metrics.record(operation, outcome);
        }
    }

    async fn apply_exact(
        &self,
        expected: &ControlPlaneSnapshot,
        candidate: ControlRecord,
        operation_id: OperationId,
        purpose: &'static str,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let key = control_storage_key()?;
        let value = encode_control(&candidate)?;
        validate_write_limits(self.store.limits(), &key, &value, Some(expected.version()))?;
        let transaction_id = transaction_id(operation_id);
        let mut transaction = self
            .store
            .begin_write(transaction_id, purpose)
            .await
            .map_err(CoordinationError::from_state_store)?;
        let current = transaction
            .get(&key)
            .await
            .map_err(CoordinationError::from_state_store)?
            .ok_or_else(CoordinationError::not_bootstrapped)?;
        let current_record = decode_control(&current.value)?;
        if !snapshot_matches(expected, &current, &current_record) {
            return Err(snapshot_mismatch(expected, &current_record));
        }
        transaction
            .put(
                key,
                value,
                Precondition::Version(expected.version().clone()),
            )
            .await
            .map_err(CoordinationError::from_state_store)?;
        let certainty = classify_commit(
            self.store.as_ref(),
            transaction_id,
            transaction.commit().await,
        )
        .await?;
        self.read_back_candidate(candidate, transaction_id, certainty)
            .await
    }

    async fn recover_candidate(
        &self,
        candidate: ControlRecord,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let transaction_id = transaction_id(operation_id);
        let certainty = recover_commit(self.store.as_ref(), transaction_id).await?;
        self.read_back_candidate(candidate, transaction_id, certainty)
            .await
    }

    async fn read_back_candidate(
        &self,
        candidate: ControlRecord,
        transaction_id: TransactionId,
        certainty: ReadBackCertainty,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let current = self.try_load_for_identity(&identity).await?;
        let Some(current) = current else {
            return Err(candidate_mismatch(
                certainty,
                transaction_id,
                None,
                candidate.incarnation,
            ));
        };
        if current.record == candidate {
            return Ok(current.snapshot);
        }
        Err(candidate_mismatch(
            certainty,
            transaction_id,
            Some(current.record.incarnation),
            candidate.incarnation,
        ))
    }

    async fn read_back_bootstrap(
        &self,
        candidate: ControlRecord,
        identity: &StoreIdentity,
        transaction_id: TransactionId,
        certainty: ReadBackCertainty,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let current = self.try_load_for_identity(identity).await?;
        let Some(current) = current else {
            return Err(candidate_mismatch(
                certainty,
                transaction_id,
                None,
                candidate.incarnation,
            ));
        };
        if current.record == candidate
            || (certainty == ReadBackCertainty::Conflict
                && bootstrap_compatible(&current.record, identity)?)
        {
            return Ok(current.snapshot);
        }
        Err(candidate_mismatch(
            certainty,
            transaction_id,
            Some(current.record.incarnation),
            candidate.incarnation,
        ))
    }

    async fn load_for_identity(
        &self,
        identity: &StoreIdentity,
    ) -> Result<LoadedControl, CoordinationError> {
        self.try_load_for_identity(identity)
            .await?
            .ok_or_else(CoordinationError::not_bootstrapped)
    }

    async fn try_load_for_identity(
        &self,
        identity: &StoreIdentity,
    ) -> Result<Option<LoadedControl>, CoordinationError> {
        let key = control_storage_key()?;
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let record = transaction
            .get(&key)
            .await
            .map_err(CoordinationError::from_state_store)?;
        transaction
            .abort()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let Some(record) = record else {
            return Ok(None);
        };
        let control = decode_control(&record.value)?;
        validate_identity(&control, identity)?;
        let snapshot = ControlPlaneSnapshot::new(
            control.store_id,
            control.cluster_id.clone(),
            control.incarnation,
            control.mode,
            control.last_operation_id,
            record.version,
        );
        Ok(Some(LoadedControl {
            record: control,
            snapshot,
        }))
    }

    async fn classify_invalid_expected(
        &self,
        expected: &ControlPlaneSnapshot,
    ) -> Result<CoordinationError, CoordinationError> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(CoordinationError::from_state_store)?;
        let current = self.load_for_identity(&identity).await?;
        Ok(snapshot_mismatch(expected, &current.record))
    }
}

#[derive(Clone, Debug)]
pub struct WriteAdmission {
    snapshot: ControlPlaneSnapshot,
    metrics: Arc<CoordinationMetrics>,
}

impl PartialEq for WriteAdmission {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot == other.snapshot
    }
}

impl Eq for WriteAdmission {}

impl WriteAdmission {
    pub async fn validate_in(
        &self,
        transaction: &mut dyn WriteTransaction,
    ) -> Result<(), CoordinationError> {
        let result = async {
            let key = control_storage_key()?;
            let current = transaction
                .get(&key)
                .await
                .map_err(CoordinationError::from_state_store)?
                .ok_or_else(CoordinationError::not_bootstrapped)?;
            let record = decode_control(&current.value)?;
            if record.store_id != self.snapshot.store_id()
                || record.cluster_id != self.snapshot.cluster_id()
            {
                return Err(CoordinationError::corruption());
            }
            if snapshot_matches(&self.snapshot, &current, &record) {
                return Ok(());
            }
            Err(snapshot_mismatch(&self.snapshot, &record))
        }
        .await;
        let outcome = match &result {
            Ok(()) => Some(CoordinationOutcome::Success),
            Err(error) => error_outcome(error),
        };
        if let Some(outcome) = outcome {
            self.metrics
                .record(CoordinationOperation::AdmitWrites, outcome);
        }
        result
    }
}

struct LoadedControl {
    record: ControlRecord,
    snapshot: ControlPlaneSnapshot,
}

fn bootstrap_candidate(
    identity: &StoreIdentity,
    operation_id: OperationId,
) -> Result<ControlRecord, CoordinationError> {
    let incarnation = ControlPlaneIncarnation::new(identity.initial_incarnation)
        .map_err(|_| CoordinationError::corruption())?;
    Ok(ControlRecord::from_identity(
        identity,
        incarnation,
        ControlPlaneMode::WriteOpen,
        operation_id,
    ))
}

fn bootstrap_compatible(
    record: &ControlRecord,
    identity: &StoreIdentity,
) -> Result<bool, CoordinationError> {
    let initial = ControlPlaneIncarnation::new(identity.initial_incarnation)
        .map_err(|_| CoordinationError::corruption())?;
    Ok(record.store_id == identity.store_id
        && record.cluster_id == identity.cluster_id
        && record.incarnation == initial
        && record.mode == ControlPlaneMode::WriteOpen)
}

fn validate_identity(
    record: &ControlRecord,
    identity: &StoreIdentity,
) -> Result<(), CoordinationError> {
    if record.store_id != identity.store_id || record.cluster_id != identity.cluster_id {
        return Err(CoordinationError::corruption());
    }
    Ok(())
}

fn snapshot_matches(
    snapshot: &ControlPlaneSnapshot,
    state: &StateRecord,
    record: &ControlRecord,
) -> bool {
    record.store_id == snapshot.store_id()
        && record.cluster_id == snapshot.cluster_id()
        && record.incarnation == snapshot.incarnation()
        && record.mode == snapshot.mode()
        && record.last_operation_id == snapshot.last_operation_id()
        && state.version == *snapshot.version()
}

fn snapshot_mismatch(
    expected: &ControlPlaneSnapshot,
    current: &ControlRecord,
) -> CoordinationError {
    if current.store_id != expected.store_id() || current.cluster_id != expected.cluster_id() {
        return CoordinationError::corruption();
    }
    if current.incarnation != expected.incarnation() {
        return CoordinationError::incarnation_changed();
    }
    CoordinationError::fence_lost()
}

fn validate_write_limits(
    limits: &StateStoreLimits,
    key: &Key,
    value: &Value,
    expected_version: Option<&crate::VersionToken>,
) -> Result<(), CoordinationError> {
    if expected_version.is_some() {
        validate_write_limits_with_read_keys(
            limits,
            key,
            value,
            expected_version,
            &[key.as_bytes().len()],
        )
    } else {
        validate_write_limits_with_read_keys(limits, key, value, expected_version, &[])
    }
}

pub(crate) fn validate_write_limits_with_read_keys(
    limits: &StateStoreLimits,
    key: &Key,
    value: &Value,
    expected_version: Option<&crate::VersionToken>,
    additional_read_key_bytes: &[usize],
) -> Result<(), CoordinationError> {
    if key.as_bytes().len() > limits.max_key_bytes
        || value.as_bytes().len() > limits.max_value_bytes
        || limits.max_transaction_operations < 1
    {
        return Err(CoordinationError::limit_exceeded(
            "coordination mutation exceeds state store limits",
        ));
    }
    let transaction_bytes = coordination_transaction_upper_bound_with_read_keys(
        key.as_bytes().len(),
        value.as_bytes().len(),
        expected_version.map_or(0, |version| version.as_bytes().len()),
        additional_read_key_bytes,
    )?;
    if transaction_bytes > limits.max_transaction_bytes {
        return Err(CoordinationError::limit_exceeded(
            "coordination mutation exceeds state store limits",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn coordination_transaction_upper_bound(
    key_bytes: usize,
    value_bytes: usize,
    version_bytes: usize,
    includes_control_read: bool,
) -> Result<usize, CoordinationError> {
    if includes_control_read {
        coordination_transaction_upper_bound_with_read_keys(
            key_bytes,
            value_bytes,
            version_bytes,
            &[key_bytes],
        )
    } else {
        coordination_transaction_upper_bound_with_read_keys(
            key_bytes,
            value_bytes,
            version_bytes,
            &[],
        )
    }
}

fn coordination_transaction_upper_bound_with_read_keys(
    key_bytes: usize,
    value_bytes: usize,
    version_bytes: usize,
    additional_read_key_bytes: &[usize],
) -> Result<usize, CoordinationError> {
    // Use one neutral v1 durability ceiling for every provider. The categories below
    // include the largest current namespace/envelope representation without selecting
    // an implementation at runtime.
    let record_key_bytes = checked_budget_add(V1_NAMESPACE_BYTES, V1_RECORD_TAG_BYTES)?;
    let record_key_bytes = checked_budget_add(record_key_bytes, key_bytes)?;
    let record_conflict_bytes = exact_conflict_bytes(record_key_bytes)?;

    let commit_key_bytes = checked_budget_add(V1_NAMESPACE_BYTES, V1_COMMIT_TAG_BYTES)?;
    let commit_key_bytes = checked_budget_add(commit_key_bytes, TRANSACTION_ID_BYTES)?;
    let commit_conflict_bytes = exact_conflict_bytes(commit_key_bytes)?;
    let high_watermark_key_bytes =
        checked_budget_add(V1_NAMESPACE_BYTES, V1_HIGH_WATERMARK_KEY_BYTES)?;
    let high_watermark_conflict_bytes = exact_conflict_bytes(high_watermark_key_bytes)?;

    let mut fixed_envelope_bytes = commit_key_bytes;
    fixed_envelope_bytes = checked_budget_add(
        fixed_envelope_bytes,
        checked_budget_add(V1_PENDING_TAG_BYTES, V1_RESERVATION_TOKEN_BYTES)?,
    )?;
    fixed_envelope_bytes = checked_budget_add(
        fixed_envelope_bytes,
        checked_budget_mul(commit_conflict_bytes, 2)?,
    )?;
    fixed_envelope_bytes = checked_budget_add(fixed_envelope_bytes, commit_key_bytes)?;
    fixed_envelope_bytes = checked_budget_add(
        fixed_envelope_bytes,
        checked_budget_add(
            checked_budget_add(V1_COMMITTED_TAG_BYTES, V1_REVISION_BYTES)?,
            V1_VERSIONSTAMP_TRAILER_BYTES,
        )?,
    )?;
    fixed_envelope_bytes = checked_budget_add(fixed_envelope_bytes, commit_conflict_bytes)?;
    fixed_envelope_bytes = checked_budget_add(fixed_envelope_bytes, high_watermark_key_bytes)?;
    fixed_envelope_bytes = checked_budget_add(
        fixed_envelope_bytes,
        checked_budget_add(V1_REVISION_BYTES, V1_VERSIONSTAMP_TRAILER_BYTES)?,
    )?;
    fixed_envelope_bytes = checked_budget_add(fixed_envelope_bytes, high_watermark_conflict_bytes)?;

    let mut bytes = fixed_envelope_bytes;
    for read_key_bytes in additional_read_key_bytes {
        let read_record_key_bytes = checked_budget_add(V1_NAMESPACE_BYTES, V1_RECORD_TAG_BYTES)?;
        let read_record_key_bytes = checked_budget_add(read_record_key_bytes, *read_key_bytes)?;
        bytes = checked_budget_add(bytes, exact_conflict_bytes(read_record_key_bytes)?)?;
    }

    bytes = checked_budget_add(bytes, MUTATION_KIND_BYTES)?;
    bytes = checked_budget_add(bytes, key_bytes)?;
    bytes = checked_budget_add(bytes, value_bytes)?;
    bytes = checked_budget_add(bytes, PRECONDITION_KIND_BYTES)?;
    bytes = checked_budget_add(bytes, version_bytes)?;

    bytes = checked_budget_add(bytes, record_key_bytes)?;
    bytes = checked_budget_add(bytes, V1_RECORD_FORMAT_BYTES)?;
    bytes = checked_budget_add(bytes, V1_PERSISTED_VERSION_BYTES)?;
    bytes = checked_budget_add(bytes, value_bytes)?;

    bytes = checked_budget_add(bytes, V1_NAMESPACE_BYTES)?;
    bytes = checked_budget_add(bytes, V1_CHANGE_TAG_BYTES)?;
    bytes = checked_budget_add(bytes, V1_REVISION_BYTES)?;
    bytes = checked_budget_add(bytes, V1_SEQUENCE_BYTES)?;
    bytes = checked_budget_add(bytes, V1_VERSIONSTAMP_TRAILER_BYTES)?;
    bytes = checked_budget_add(bytes, key_bytes)?;

    bytes = checked_budget_add(bytes, checked_budget_mul(record_conflict_bytes, 2)?)?;
    bytes = checked_budget_add(bytes, V1_PROVISIONAL_VERSION_TAG_BYTES)?;
    bytes = checked_budget_add(bytes, TRANSACTION_ID_BYTES)?;
    checked_budget_add(bytes, V1_PROVISIONAL_OPERATION_BYTES)
}

fn exact_conflict_bytes(key_bytes: usize) -> Result<usize, CoordinationError> {
    checked_budget_add(checked_budget_mul(key_bytes, 2)?, 1)
}

fn checked_budget_add(total: usize, increment: usize) -> Result<usize, CoordinationError> {
    total.checked_add(increment).ok_or_else(write_limit_error)
}

fn checked_budget_mul(value: usize, multiplier: usize) -> Result<usize, CoordinationError> {
    value.checked_mul(multiplier).ok_or_else(write_limit_error)
}

fn write_limit_error() -> CoordinationError {
    CoordinationError::limit_exceeded("coordination mutation exceeds state store limits")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use uuid::Uuid;

    use super::{IncarnationGate, coordination_transaction_upper_bound, validate_write_limits};
    use crate::coordination::CoordinationErrorKind;
    use crate::{
        ChangePage, ChangePollRequest, CommitOutcome, CommitResolution, Key, OperationId,
        Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
        StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
        StoreIdentity, TransactionId, Value, VersionToken, WriteTransaction,
    };

    fn complete_v1_write_budget(
        key_bytes: usize,
        value_bytes: usize,
        version_bytes: usize,
        includes_control_read: bool,
    ) -> usize {
        let namespace_bytes = 5 + 16;
        let record_key_bytes = namespace_bytes + 1 + key_bytes;
        let exact_record_conflict_bytes = 2 * record_key_bytes + 1;
        let commit_key_bytes = namespace_bytes + 1 + 16;
        let exact_commit_conflict_bytes = 2 * commit_key_bytes + 1;
        let high_watermark_key_bytes = namespace_bytes + 2;
        let exact_high_watermark_conflict_bytes = 2 * high_watermark_key_bytes + 1;
        let fixed_envelope_bytes = commit_key_bytes
            + (1 + 16)
            + 2 * exact_commit_conflict_bytes
            + commit_key_bytes
            + (1 + 10 + 4)
            + exact_commit_conflict_bytes
            + high_watermark_key_bytes
            + (10 + 4)
            + exact_high_watermark_conflict_bytes;
        let control_read_bytes = if includes_control_read {
            exact_record_conflict_bytes
        } else {
            0
        };
        let logical_mutation_bytes = 1 + key_bytes + value_bytes + 1 + version_bytes;
        let durable_record_bytes = record_key_bytes + 1 + 16 + value_bytes;
        let change_entry_bytes = namespace_bytes + 1 + 10 + 4 + 4 + key_bytes;
        let precondition_and_write_conflicts = 2 * exact_record_conflict_bytes;
        let provisional_version_bytes = 22 + 16 + 8;

        fixed_envelope_bytes
            + control_read_bytes
            + logical_mutation_bytes
            + durable_record_bytes
            + change_entry_bytes
            + precondition_and_write_conflicts
            + provisional_version_bytes
    }

    #[test]
    fn encoded_control_mutation_uses_complete_conservative_boundary() {
        let key = Key::try_from(Bytes::from_static(b"control-key")).expect("key");
        let value = Value::try_from(Bytes::from_static(b"control-value")).expect("value");
        let version = VersionToken::try_from(Bytes::from_static(b"version")).expect("version");
        let exact_bytes = complete_v1_write_budget(
            key.as_bytes().len(),
            value.as_bytes().len(),
            version.as_bytes().len(),
            true,
        );
        let mut limits = StateStoreLimits {
            max_transaction_bytes: exact_bytes,
            ..StateStoreLimits::default()
        };
        validate_write_limits(&limits, &key, &value, Some(&version)).expect("exact limit");

        limits.max_transaction_bytes -= 1;
        assert_eq!(
            validate_write_limits(&limits, &key, &value, Some(&version))
                .unwrap_err()
                .kind(),
            CoordinationErrorKind::LimitExceeded
        );
    }

    #[test]
    fn conservative_budget_overflow_is_limit_exceeded() {
        assert_eq!(
            coordination_transaction_upper_bound(usize::MAX, 1, 1, true)
                .unwrap_err()
                .kind(),
            CoordinationErrorKind::LimitExceeded
        );
    }

    struct LimitedStore {
        limits: StateStoreLimits,
        begin_writes: AtomicUsize,
        identity: StoreIdentity,
    }

    #[async_trait]
    impl StateStore for LimitedStore {
        fn provider_name(&self) -> &'static str {
            "limited-test"
        }

        fn limits(&self) -> &StateStoreLimits {
            &self.limits
        }

        fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
            panic!("metrics must not be read")
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            panic!("read transaction must not begin")
        }

        async fn begin_write(
            &self,
            _transaction_id: TransactionId,
            _purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            self.begin_writes.fetch_add(1, Ordering::SeqCst);
            panic!("write transaction must not begin")
        }

        async fn poll_changes(
            &self,
            _request: &ChangePollRequest,
        ) -> Result<ChangePage, StateStoreError> {
            panic!("changes must not be polled")
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            Ok(self.identity.clone())
        }

        async fn resolve_commit(
            &self,
            _transaction_id: &TransactionId,
        ) -> Result<CommitResolution, StateStoreError> {
            panic!("commit must not be resolved")
        }
    }

    #[tokio::test]
    async fn gate_rejects_encoded_control_before_begin_write() {
        let store = Arc::new(LimitedStore {
            limits: StateStoreLimits {
                max_key_bytes: 1,
                ..StateStoreLimits::default()
            },
            begin_writes: AtomicUsize::new(0),
            identity: StoreIdentity {
                store_id: Uuid::now_v7(),
                cluster_id: "limited-cluster".to_owned(),
                initial_incarnation: 1,
            },
        });
        let gate_store: Arc<dyn StateStore> = store.clone();
        let gate = IncarnationGate::new(gate_store);

        assert_eq!(
            gate.bootstrap(OperationId::new_v7())
                .await
                .unwrap_err()
                .kind(),
            CoordinationErrorKind::LimitExceeded
        );
        assert_eq!(store.begin_writes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn coordination_budget_rejects_before_begin_write() {
        let key_bytes = b"\0novarocks/cp/v1/control".len();
        let value_bytes = 1 + 16 + 4 + b"limited-cluster".len() + 8 + 1 + 16;
        let previous_estimate = 16 + 1 + 1 + key_bytes * 2 + value_bytes;
        let complete_budget = complete_v1_write_budget(key_bytes, value_bytes, 0, false);
        let tightened_budget = previous_estimate + 1;
        assert!(tightened_budget < complete_budget);
        let store = Arc::new(LimitedStore {
            limits: StateStoreLimits {
                max_transaction_bytes: tightened_budget,
                ..StateStoreLimits::default()
            },
            begin_writes: AtomicUsize::new(0),
            identity: StoreIdentity {
                store_id: Uuid::now_v7(),
                cluster_id: "limited-cluster".to_owned(),
                initial_incarnation: 1,
            },
        });
        let gate_store: Arc<dyn StateStore> = store.clone();
        let gate = IncarnationGate::new(gate_store);

        assert_eq!(
            gate.bootstrap(OperationId::new_v7())
                .await
                .unwrap_err()
                .kind(),
            CoordinationErrorKind::LimitExceeded
        );
        assert_eq!(store.begin_writes.load(Ordering::SeqCst), 0);
    }

    struct AbsentReadTransaction;

    #[async_trait]
    impl ReadTransaction for AbsentReadTransaction {
        async fn get(&mut self, _key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
            Ok(None)
        }

        async fn range(&mut self, _request: &RangeRequest) -> Result<RangePage, StateStoreError> {
            panic!("direct bootstrap regression does not scan")
        }

        async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
            Ok(())
        }
    }

    struct UnknownBootstrapTransaction {
        transaction_id: TransactionId,
    }

    #[async_trait]
    impl ReadTransaction for UnknownBootstrapTransaction {
        async fn get(&mut self, _key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
            panic!("bootstrap does not read in its write transaction")
        }

        async fn range(&mut self, _request: &RangeRequest) -> Result<RangePage, StateStoreError> {
            panic!("direct bootstrap regression does not scan")
        }

        async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
            Ok(())
        }
    }

    #[async_trait]
    impl WriteTransaction for UnknownBootstrapTransaction {
        fn transaction_id(&self) -> &TransactionId {
            &self.transaction_id
        }

        async fn put(
            &mut self,
            _key: Key,
            _value: Value,
            _precondition: Precondition,
        ) -> Result<(), StateStoreError> {
            Ok(())
        }

        async fn delete(
            &mut self,
            _key: Key,
            _precondition: Precondition,
        ) -> Result<(), StateStoreError> {
            panic!("bootstrap does not delete")
        }

        async fn commit(self: Box<Self>) -> CommitOutcome {
            CommitOutcome::CommitUnknown(StateStoreError::new(
                StateStoreErrorKind::Transient,
                "scripted unresolved bootstrap",
            ))
        }
    }

    struct UnresolvedAbsentBootstrapStore {
        limits: StateStoreLimits,
        identity: StoreIdentity,
    }

    #[async_trait]
    impl StateStore for UnresolvedAbsentBootstrapStore {
        fn provider_name(&self) -> &'static str {
            "unresolved-absent-bootstrap"
        }

        fn limits(&self) -> &StateStoreLimits {
            &self.limits
        }

        fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
            panic!("direct bootstrap regression does not inspect metrics")
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            Ok(Box::new(AbsentReadTransaction))
        }

        async fn begin_write(
            &self,
            transaction_id: TransactionId,
            _purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            Ok(Box::new(UnknownBootstrapTransaction { transaction_id }))
        }

        async fn poll_changes(
            &self,
            _request: &ChangePollRequest,
        ) -> Result<ChangePage, StateStoreError> {
            panic!("direct bootstrap regression does not poll changes")
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            Ok(self.identity.clone())
        }

        async fn resolve_commit(
            &self,
            _transaction_id: &TransactionId,
        ) -> Result<CommitResolution, StateStoreError> {
            Ok(CommitResolution::Unresolved)
        }
    }

    #[tokio::test]
    async fn direct_bootstrap_unresolved_without_visible_record_is_commit_uncertain() {
        let store: Arc<dyn StateStore> = Arc::new(UnresolvedAbsentBootstrapStore {
            limits: StateStoreLimits::default(),
            identity: StoreIdentity {
                store_id: Uuid::now_v7(),
                cluster_id: "unresolved-absent-bootstrap".to_owned(),
                initial_incarnation: 1,
            },
        });
        let gate = IncarnationGate::new(store);
        let operation_id = OperationId::new_v7();
        let expected_transaction_id = crate::derive_transaction_id(operation_id, 1);

        let error = gate.bootstrap(operation_id).await.unwrap_err();

        assert_eq!(error.kind(), CoordinationErrorKind::CommitUncertain);
        assert_eq!(error.transaction_id(), Some(expected_transaction_id));
    }
}
