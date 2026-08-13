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

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_spi::state_store::WriteTransaction;
use novarocks_state_store::coordination::ResourceKey;
use uuid::{Uuid, Version};

use crate::dml::error::DmlError;
use crate::dml::model::{
    AddFilesArtifact, AddFilesMutationRequest, CreatePreparingRequest,
    CreateStatementOperationRequest, DML_COORDINATION_RESOURCE_CODEC_VERSION,
    DML_RECOVERY_SHARD_COUNT, DmlCoordinationClaimRequest, DmlCtasRecoveryMutationRequest,
    DmlCtasRecoveryRecord, DmlDirectMutationFenceMutationRequest,
    DmlDirectMutationFenceReceiptRecord, DmlExternalFenceMutationRequest,
    DmlExternalFenceReceiptRecord, DmlHistoricalDataMutationRecoveryMutationRequest,
    DmlHistoricalDataMutationRecoveryRecord, DmlHistoricalWriteRecoveryMutationRequest,
    DmlHistoricalWriteRecoveryRecord, DmlOperationId, DmlRecoveryCandidate,
    DmlRecoveryDueRescheduleRequest, OperationFact, OperationMutationRequest, OperationState,
    StoredOperation,
};

#[async_trait]
pub trait DmlIntentAdmissionValidator: Send + Sync {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError>;
}

#[async_trait]
pub trait DmlMutationAuthorityValidator: Send + Sync {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError>;
}

/// Dynamic authority for one operation attempt. The validator must project
/// the latest live fence on every invocation; callers must not capture the
/// fence returned by the original acquire across renewals.
#[derive(Clone)]
pub struct DmlMutationAuthority {
    coordination_attempt_id: Uuid,
    validator: Arc<dyn DmlMutationAuthorityValidator>,
}

impl DmlMutationAuthority {
    pub fn try_new(
        coordination_attempt_id: Uuid,
        validator: Arc<dyn DmlMutationAuthorityValidator>,
    ) -> Result<Self, DmlError> {
        if coordination_attempt_id.get_version() != Some(Version::SortRand)
            || coordination_attempt_id.get_variant() != uuid::Variant::RFC4122
        {
            return Err(DmlError::journal_corruption(
                "DML coordination attempt id must be UUIDv7",
            ));
        }
        Ok(Self {
            coordination_attempt_id,
            validator,
        })
    }

    pub const fn coordination_attempt_id(&self) -> Uuid {
        self.coordination_attempt_id
    }

    pub fn validator(&self) -> &Arc<dyn DmlMutationAuthorityValidator> {
        &self.validator
    }
}

pub fn dml_operation_resource_key(operation_id: DmlOperationId) -> Result<ResourceKey, DmlError> {
    debug_assert_eq!(DML_COORDINATION_RESOURCE_CODEC_VERSION, 1);
    ResourceKey::try_from(Bytes::from(format!(
        "novarocks/frontend/dml/operation/v1/{operation_id}"
    )))
    .map_err(DmlError::journal_corruption)
}

pub trait OperationJournal: Send + Sync {
    fn create_preparing_admitted(
        &self,
        _request: CreatePreparingRequest,
        _admission: Arc<dyn DmlIntentAdmissionValidator>,
    ) -> Result<DmlOperationId, DmlError> {
        Err(DmlError::journal_unavailable(
            "transaction-scoped DML intent admission is not supported by this journal",
        ))
    }

    fn create_statement_operation_admitted(
        &self,
        _request: CreateStatementOperationRequest,
        _admission: Arc<dyn DmlIntentAdmissionValidator>,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "transaction-scoped statement intent admission is not supported by this journal",
        ))
    }

    fn claim_operation(
        &self,
        _request: DmlCoordinationClaimRequest,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "DML operation coordination claim is not supported by this journal",
        ))
    }

    fn claim_operation_admitted(
        &self,
        _request: DmlCoordinationClaimRequest,
        _admission: Arc<dyn DmlIntentAdmissionValidator>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "transaction-scoped foreground DML claim admission is not supported by this journal",
        ))
    }

    fn transition_authorized(
        &self,
        _operation_id: DmlOperationId,
        _expected_revision: u64,
        _mutation_id: Uuid,
        _to: OperationState,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized DML transition is not supported by this journal",
        ))
    }

    fn record_fact_authorized(
        &self,
        _operation_id: DmlOperationId,
        _expected_revision: u64,
        _mutation_id: Uuid,
        _fact: OperationFact,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized DML fact mutation is not supported by this journal",
        ))
    }

    fn mutate_statement_operation_authorized(
        &self,
        _request: OperationMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized statement DML mutation is not supported by this journal",
        ))
    }

    fn apply_add_files_mutation_authorized(
        &self,
        _request: AddFilesMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized ADD FILES mutation is not supported by this journal",
        ))
    }

    /// Persist the external operation fence one attempt confirmed before any
    /// writer or commit dispatch could produce an irreversible external effect.
    ///
    /// The mutation validates the live lease fence and the expected operation
    /// revision inside the same StateStore transaction that writes the receipt,
    /// so a stale holder can never install a fence receipt.
    fn record_external_fence_authorized(
        &self,
        _request: DmlExternalFenceMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized DML external fence receipt mutation is not supported by this journal",
        ))
    }

    /// Persist a historical write recovery request, the fence the current
    /// generation raised above the old authority, or the typed result of one
    /// provider inspection.
    ///
    /// The mutation is fenced exactly like every other authorized mutation and
    /// refuses to drop a retained cleanup obligation.
    fn record_historical_write_recovery_authorized(
        &self,
        _request: DmlHistoricalWriteRecoveryMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized DML historical write recovery mutation is not supported by this journal",
        ))
    }

    /// Persist the external operation fence one TRUNCATE or ADD FILES attempt
    /// confirmed before its irreversible direct mutation could be dispatched.
    ///
    /// Direct mutation reuses the CP-3B fence value rather than a second fence
    /// type; the record adds only the mutation family and, for ADD FILES, the
    /// immutable source scope the fence was minted for. The mutation validates
    /// the live lease fence and the expected operation revision inside the same
    /// StateStore transaction that writes the receipt.
    fn record_direct_mutation_fence_authorized(
        &self,
        _request: DmlDirectMutationFenceMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized DML direct mutation fence receipt mutation is not supported by this journal",
        ))
    }

    /// Persist a historical data-mutation recovery request, the fence the
    /// current generation raised above the old authority, or the typed result of
    /// one provider inspection.
    ///
    /// The mutation is fenced exactly like every other authorized mutation. It
    /// refuses to drop a retained cleanup obligation, and it refuses a result
    /// bound to any source scope other than the sealed request's.
    fn record_historical_data_mutation_recovery_authorized(
        &self,
        _request: DmlHistoricalDataMutationRecoveryMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized DML historical data mutation recovery mutation is not supported by this journal",
        ))
    }

    /// Persist provider-neutral CTAS catalog-fence, dispatch, historical,
    /// supersession, and cleanup-retention facts beside the top-level saga.
    /// The live lease authority, operation revision, and encoded bound are
    /// validated in the same transaction that advances the operation.
    fn record_ctas_recovery_authorized(
        &self,
        _request: DmlCtasRecoveryMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "authorized CTAS recovery mutation is not supported by this journal",
        ))
    }

    fn load_external_fence(
        &self,
        _operation_id: DmlOperationId,
    ) -> Result<Option<DmlExternalFenceReceiptRecord>, DmlError> {
        Err(DmlError::journal_unavailable(
            "DML external fence receipt loading is not supported by this journal",
        ))
    }

    fn load_historical_write_recovery(
        &self,
        _operation_id: DmlOperationId,
    ) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
        Err(DmlError::journal_unavailable(
            "DML historical write recovery loading is not supported by this journal",
        ))
    }

    /// Validate that a confirmed fence receipt can be durably encoded before
    /// the caller asks a provider to establish it. Failing closed keeps the
    /// fence-before-dispatch ordering honest: a receipt that could never be
    /// recorded must not be created.
    fn preflight_external_fence(
        &self,
        _request: &DmlExternalFenceMutationRequest,
    ) -> Result<(), DmlError> {
        Err(DmlError::journal_unavailable(
            "DML external fence receipt preflight is not supported by this journal",
        ))
    }

    /// Validate that a historical write recovery record can be durably encoded
    /// before the caller raises a fence or inspects the old operation.
    fn preflight_historical_write_recovery(
        &self,
        _request: &DmlHistoricalWriteRecoveryMutationRequest,
    ) -> Result<(), DmlError> {
        Err(DmlError::journal_unavailable(
            "DML historical write recovery preflight is not supported by this journal",
        ))
    }

    fn load_direct_mutation_fence(
        &self,
        _operation_id: DmlOperationId,
    ) -> Result<Option<DmlDirectMutationFenceReceiptRecord>, DmlError> {
        Err(DmlError::journal_unavailable(
            "DML direct mutation fence receipt loading is not supported by this journal",
        ))
    }

    fn load_ctas_recovery(
        &self,
        _operation_id: DmlOperationId,
    ) -> Result<Option<DmlCtasRecoveryRecord>, DmlError> {
        Err(DmlError::journal_unavailable(
            "CTAS recovery loading is not supported by this journal",
        ))
    }

    fn load_historical_data_mutation_recovery(
        &self,
        _operation_id: DmlOperationId,
    ) -> Result<Option<DmlHistoricalDataMutationRecoveryRecord>, DmlError> {
        Err(DmlError::journal_unavailable(
            "DML historical data mutation recovery loading is not supported by this journal",
        ))
    }

    /// Validate that a confirmed direct-mutation fence receipt can be durably
    /// encoded before the caller asks a provider to establish it. Failing closed
    /// keeps the fence-before-dispatch ordering honest for a destructive
    /// TRUNCATE just as it does for a distributed write.
    fn preflight_direct_mutation_fence(
        &self,
        _request: &DmlDirectMutationFenceMutationRequest,
    ) -> Result<(), DmlError> {
        Err(DmlError::journal_unavailable(
            "DML direct mutation fence receipt preflight is not supported by this journal",
        ))
    }

    /// Validate that a historical data-mutation recovery record can be durably
    /// encoded before the caller raises a fence or inspects the old operation.
    fn preflight_historical_data_mutation_recovery(
        &self,
        _request: &DmlHistoricalDataMutationRecoveryMutationRequest,
    ) -> Result<(), DmlError> {
        Err(DmlError::journal_unavailable(
            "DML historical data mutation recovery preflight is not supported by this journal",
        ))
    }

    fn preflight_ctas_recovery(
        &self,
        _request: &DmlCtasRecoveryMutationRequest,
    ) -> Result<(), DmlError> {
        Err(DmlError::journal_unavailable(
            "CTAS recovery preflight is not supported by this journal",
        ))
    }

    fn recovery_candidates(
        &self,
        _shard: u8,
        _due_at_or_before_ms: i64,
    ) -> Result<Vec<DmlRecoveryCandidate>, DmlError> {
        Err(DmlError::journal_unavailable(
            "bounded DML recovery candidate scan is not supported by this journal",
        ))
    }

    fn reschedule_recovery_due(
        &self,
        _request: DmlRecoveryDueRescheduleRequest,
        _authority: DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "fenced DML recovery reschedule is not supported by this journal",
        ))
    }

    fn recovery_shard_count(&self) -> u8 {
        DML_RECOVERY_SHARD_COUNT
    }

    // Focused-test compatibility surface for pre-coordination fake journals.
    // FrontendApplicationHost never composes the unfenced DmlService mode;
    // production routes use only the admitted/authorized methods above.
    fn create_preparing(&self, request: CreatePreparingRequest)
    -> Result<DmlOperationId, DmlError>;
    fn transition(&self, operation_id: DmlOperationId, to: OperationState) -> Result<(), DmlError>;
    fn record_fact(
        &self,
        operation_id: DmlOperationId,
        fact: OperationFact,
    ) -> Result<(), DmlError>;
    fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError>;
    fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError>;
    fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError>;

    fn create_statement_operation(
        &self,
        _request: CreateStatementOperationRequest,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "statement-specific DML operation payloads are not supported by this journal",
        ))
    }

    fn mutate_statement_operation(
        &self,
        _request: OperationMutationRequest,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "statement-specific DML operation mutation is not supported by this journal",
        ))
    }

    /// Validate that a complete post-dispatch statement envelope can be
    /// durably encoded by this journal before the external side effect starts.
    ///
    /// Journals must opt in with their real storage limits. Failing closed is
    /// intentional: executing without this guarantee could make the external
    /// truth impossible to record.
    fn preflight_statement_operation(&self, _operation: &StoredOperation) -> Result<(), DmlError> {
        Err(DmlError::journal_unavailable(
            "statement-specific DML operation preflight is not supported by this journal",
        ))
    }

    /// ADD FILES requires a single atomic publication of its operation change,
    /// bounded raw artifacts, and source-scope ownership transition. Generic
    /// journals must reject it rather than silently degrade to a second store.
    fn apply_add_files_mutation(
        &self,
        _request: AddFilesMutationRequest,
    ) -> Result<StoredOperation, DmlError> {
        Err(DmlError::journal_unavailable(
            "ADD FILES atomic mutation is not supported by this journal",
        ))
    }

    fn load_add_files_artifact(
        &self,
        _operation_id: DmlOperationId,
        _artifact: &crate::dml::model::AddFilesArtifactDescriptor,
    ) -> Result<AddFilesArtifact, DmlError> {
        Err(DmlError::journal_unavailable(
            "ADD FILES artifact loading is not supported by this journal",
        ))
    }

    fn preflight_add_files_mutation(
        &self,
        _request: &AddFilesMutationRequest,
    ) -> Result<(), DmlError> {
        Err(DmlError::journal_unavailable(
            "ADD FILES atomic mutation preflight is not supported by this journal",
        ))
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use uuid::Uuid;

    use super::*;
    use crate::dml::model::{
        ConnectorWriteLifecycleRecord, DML_OPERATION_SCHEMA_VERSION, OperationPayload,
        validate_operation_transition, validate_statement_operation_transition,
    };
    use crate::dml::now_unix_millis;

    #[derive(Default)]
    pub(crate) struct InMemoryOperationJournal {
        inner: Mutex<BTreeMap<Uuid, StoredOperation>>,
    }

    impl InMemoryOperationJournal {
        pub(crate) fn only_operation(&self) -> StoredOperation {
            let guard = self.inner.lock().unwrap();
            assert_eq!(guard.len(), 1);
            guard.values().next().unwrap().clone()
        }
    }

    impl OperationJournal for InMemoryOperationJournal {
        fn create_preparing(
            &self,
            request: CreatePreparingRequest,
        ) -> Result<DmlOperationId, DmlError> {
            let operation_id = DmlOperationId::new_v7();
            let mutation_id = Uuid::now_v7();
            let stored = StoredOperation {
                schema_version: DML_OPERATION_SCHEMA_VERSION,
                operation_id,
                revision: 1,
                last_mutation_id: mutation_id,
                operation_kind: request.operation_kind,
                operation_subkind: request.operation_subkind,
                target: request.target,
                state: OperationState::Preparing,
                attempt_id: request.attempt_id,
                base_snapshot_id: request.base_snapshot_id,
                base_snapshot_map: request.base_snapshot_map,
                staged_artifacts: request.staged_artifacts,
                payload: OperationPayload::ConnectorWriteLifecycle(
                    ConnectorWriteLifecycleRecord::Pending,
                ),
                coordination_provenance: None,
                recovery_due_at_ms: Some(request.created_at_ms),
                created_at_ms: request.created_at_ms,
                updated_at_ms: request.created_at_ms,
                finished_at_ms: None,
            };
            self.inner
                .lock()
                .unwrap()
                .insert(*operation_id.as_uuid(), stored);
            Ok(operation_id)
        }

        fn transition(
            &self,
            operation_id: DmlOperationId,
            to: OperationState,
        ) -> Result<(), DmlError> {
            let mut guard = self.inner.lock().unwrap();
            let operation = guard
                .get_mut(operation_id.as_uuid())
                .ok_or_else(|| DmlError::journal_unavailable("DML operation not found"))?;
            validate_operation_transition(operation.state, to)
                .map_err(DmlError::journal_unavailable)?;
            operation.state = to;
            operation.revision += 1;
            operation.last_mutation_id = Uuid::now_v7();
            operation.updated_at_ms = now_unix_millis();
            if to.is_finished() {
                operation.finished_at_ms = Some(operation.updated_at_ms);
                operation.recovery_due_at_ms = None;
            }
            Ok(())
        }

        fn record_fact(
            &self,
            operation_id: DmlOperationId,
            fact: OperationFact,
        ) -> Result<(), DmlError> {
            let mut guard = self.inner.lock().unwrap();
            let operation = guard
                .get_mut(operation_id.as_uuid())
                .ok_or_else(|| DmlError::journal_unavailable("DML operation not found"))?;
            validate_operation_transition(operation.state, fact.state)
                .map_err(DmlError::journal_unavailable)?;
            if operation.state == fact.state {
                let identical = operation.payload
                    == OperationPayload::ConnectorWriteLifecycle(fact.lifecycle.clone());
                if !identical {
                    return Err(DmlError::journal_unavailable(format!(
                        "conflicting DML operation fact replay for operation {operation_id}"
                    )));
                }
            }
            operation.state = fact.state;
            operation.payload = OperationPayload::ConnectorWriteLifecycle(fact.lifecycle);
            operation.revision += 1;
            operation.last_mutation_id = Uuid::now_v7();
            operation.updated_at_ms = now_unix_millis();
            if fact.state.is_finished() {
                operation.finished_at_ms = Some(operation.updated_at_ms);
                operation.recovery_due_at_ms = None;
            }
            Ok(())
        }

        fn preflight_statement_operation(
            &self,
            operation: &StoredOperation,
        ) -> Result<(), DmlError> {
            serde_json::to_vec(operation)
                .map(|_| ())
                .map_err(DmlError::journal_corruption)
        }

        fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .get(operation_id.as_uuid())
                .cloned())
        }

        fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(self.inner.lock().unwrap().values().cloned().collect())
        }

        fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .values()
                .filter(|operation| !operation.state.is_finished())
                .cloned()
                .collect())
        }

        fn create_statement_operation(
            &self,
            request: CreateStatementOperationRequest,
        ) -> Result<StoredOperation, DmlError> {
            let stored = StoredOperation {
                schema_version: DML_OPERATION_SCHEMA_VERSION,
                operation_id: request.operation_id,
                revision: 1,
                last_mutation_id: request.mutation_id,
                operation_kind: request.operation_kind,
                operation_subkind: None,
                target: request.target,
                state: OperationState::Preparing,
                attempt_id: request.attempt_id,
                base_snapshot_id: None,
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: Vec::new(),
                payload: request.payload,
                coordination_provenance: None,
                recovery_due_at_ms: Some(request.created_at_ms),
                created_at_ms: request.created_at_ms,
                updated_at_ms: request.created_at_ms,
                finished_at_ms: None,
            };
            let mut guard = self.inner.lock().unwrap();
            match guard.get(stored.operation_id.as_uuid()) {
                Some(existing) if existing == &stored => Ok(existing.clone()),
                Some(_) => Err(DmlError::journal_unresolved(format!(
                    "conflicting DML statement create replay for operation {}",
                    stored.operation_id
                ))),
                None => {
                    guard.insert(*stored.operation_id.as_uuid(), stored.clone());
                    Ok(stored)
                }
            }
        }

        fn mutate_statement_operation(
            &self,
            request: OperationMutationRequest,
        ) -> Result<StoredOperation, DmlError> {
            let mut guard = self.inner.lock().unwrap();
            let operation = guard
                .get_mut(request.operation_id.as_uuid())
                .ok_or_else(|| DmlError::journal_unavailable("DML operation not found"))?;
            if operation.last_mutation_id == request.mutation_id {
                let applied_revision =
                    request.expected_revision.checked_add(1).ok_or_else(|| {
                        DmlError::journal_unavailable("DML operation revision overflow")
                    })?;
                if operation.revision == applied_revision
                    && operation.state == request.state
                    && operation.payload == request.payload
                {
                    return Ok(operation.clone());
                }
                return Err(DmlError::journal_unresolved(format!(
                    "conflicting DML statement mutation replay for operation {}",
                    request.operation_id
                )));
            }
            if operation.revision != request.expected_revision {
                return Err(DmlError::journal_unresolved(format!(
                    "DML operation {} revision changed from expected {} to {}",
                    request.operation_id, request.expected_revision, operation.revision
                )));
            }
            validate_statement_operation_transition(
                operation.operation_kind,
                operation.state,
                request.state,
            )
            .map_err(DmlError::journal_unavailable)?;
            operation.schema_version = DML_OPERATION_SCHEMA_VERSION;
            operation.revision = operation
                .revision
                .checked_add(1)
                .ok_or_else(|| DmlError::journal_unavailable("DML operation revision overflow"))?;
            operation.last_mutation_id = request.mutation_id;
            operation.state = request.state;
            operation.payload = request.payload;
            operation.updated_at_ms = now_unix_millis();
            if operation.state.is_finished() {
                operation.finished_at_ms = Some(operation.updated_at_ms);
            }
            Ok(operation.clone())
        }
    }
}
