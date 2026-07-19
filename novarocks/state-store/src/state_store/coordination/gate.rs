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

pub struct IncarnationGate {
    store: Arc<dyn StateStore>,
}

impl IncarnationGate {
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self { store }
    }

    pub async fn bootstrap(
        &self,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
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
        let current = self.load_for_identity(&identity).await?;

        if current.record == candidate {
            return Ok(current.snapshot);
        }
        if certainty == ReadBackCertainty::Conflict
            && bootstrap_compatible(&current.record, &identity)?
        {
            return Ok(current.snapshot);
        }
        Err(candidate_mismatch(
            certainty,
            transaction_id,
            current.record.incarnation,
            candidate.incarnation,
        ))
    }

    pub async fn load(&self) -> Result<ControlPlaneSnapshot, CoordinationError> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(CoordinationError::from_state_store)?;
        Ok(self.load_for_identity(&identity).await?.snapshot)
    }

    pub async fn begin_restore(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
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

    pub async fn open_writes(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
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

    pub async fn recover_bootstrap(
        &self,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
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

    pub async fn recover_begin_restore(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
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

    pub async fn recover_open_writes(
        &self,
        expected: &ControlPlaneSnapshot,
        operation_id: OperationId,
    ) -> Result<ControlPlaneSnapshot, CoordinationError> {
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

    pub async fn admit_writes(&self) -> Result<WriteAdmission, CoordinationError> {
        let snapshot = self.load().await?;
        if snapshot.mode() != ControlPlaneMode::WriteOpen {
            return Err(CoordinationError::write_closed());
        }
        Ok(WriteAdmission { snapshot })
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
        let current = self.load_for_identity(&identity).await?;
        if current.record == candidate {
            return Ok(current.snapshot);
        }
        Err(candidate_mismatch(
            certainty,
            transaction_id,
            current.record.incarnation,
            candidate.incarnation,
        ))
    }

    async fn load_for_identity(
        &self,
        identity: &StoreIdentity,
    ) -> Result<LoadedControl, CoordinationError> {
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
        let record = record.ok_or_else(CoordinationError::not_bootstrapped)?;
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
        Ok(LoadedControl {
            record: control,
            snapshot,
        })
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAdmission {
    snapshot: ControlPlaneSnapshot,
}

impl WriteAdmission {
    pub async fn validate_in(
        &self,
        transaction: &mut dyn WriteTransaction,
    ) -> Result<(), CoordinationError> {
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
    if key.as_bytes().len() > limits.max_key_bytes
        || value.as_bytes().len() > limits.max_value_bytes
        || limits.max_transaction_operations < 1
    {
        return Err(CoordinationError::limit_exceeded(
            "coordination mutation exceeds state store limits",
        ));
    }
    let transaction_bytes = TRANSACTION_ID_BYTES
        .checked_add(MUTATION_KIND_BYTES)
        .and_then(|bytes| bytes.checked_add(PRECONDITION_KIND_BYTES))
        .and_then(|bytes| bytes.checked_add(key.as_bytes().len()))
        .and_then(|bytes| bytes.checked_add(key.as_bytes().len()))
        .and_then(|bytes| bytes.checked_add(value.as_bytes().len()))
        .and_then(|bytes| {
            bytes.checked_add(expected_version.map_or(0, |version| version.as_bytes().len()))
        })
        .ok_or_else(|| {
            CoordinationError::limit_exceeded("coordination mutation exceeds state store limits")
        })?;
    if transaction_bytes > limits.max_transaction_bytes {
        return Err(CoordinationError::limit_exceeded(
            "coordination mutation exceeds state store limits",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use uuid::Uuid;

    use super::{IncarnationGate, validate_write_limits};
    use crate::coordination::CoordinationErrorKind;
    use crate::{
        ChangePage, ChangePollRequest, CommitResolution, Key, OperationId, ReadTransaction,
        StateStore, StateStoreError, StateStoreLimits, StateStoreMetricsSnapshot, StoreIdentity,
        TransactionId, Value, VersionToken, WriteTransaction,
    };

    #[test]
    fn encoded_control_mutation_is_validated_before_provider_write() {
        let key = Key::try_from(Bytes::from_static(b"control-key")).expect("key");
        let value = Value::try_from(Bytes::from_static(b"control-value")).expect("value");
        let version = VersionToken::try_from(Bytes::from_static(b"version")).expect("version");
        let exact_bytes = 16
            + 1
            + 1
            + key.as_bytes().len() * 2
            + value.as_bytes().len()
            + version.as_bytes().len();
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

    struct LimitedStore {
        limits: StateStoreLimits,
        begin_writes: AtomicUsize,
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
            Ok(StoreIdentity {
                store_id: Uuid::now_v7(),
                cluster_id: "limited-cluster".to_owned(),
                initial_incarnation: 1,
            })
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
}
