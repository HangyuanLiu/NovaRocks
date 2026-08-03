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

//! FE-only atomic staged-table publication contract.
//!
//! A provider prepares an invisible table, distributed writers stage data
//! against the returned opaque handle, and one provider commit publishes the
//! table initialization and the sealed writer aggregate. The handle payload
//! remains provider-private and is always fenced by the exact connector
//! incarnation that issued it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorColumnDefinition, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorInstanceDescriptor, ConnectorInstanceIncarnation, ConnectorMutationFailure,
    ConnectorMutationOperationId, ConnectorPartitionTransform, ConnectorRequestContext,
    ConnectorTableHandle, ConnectorTableIdentity, ConnectorWriteIntent,
    ConnectorWriteOperationCompletion, ConnectorWriteOperationId, CreatePolicy,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
};

pub const CONNECTOR_STAGED_CREATE_CONTRACT_VERSION: u32 = 1;
pub type ConnectorStagedCreateOperationId = ConnectorMutationOperationId;

const HANDLE_DOMAIN: &[u8] = b"novarocks.connector-staged-table-handle.v1\0";

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorStagedTableHandle {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorStagedCreateOperationId,
    provider_payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorStagedTableHandle {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if provider_payload.is_empty()
            || provider_payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "staged table handle payload must fit the connector handle limit",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(HANDLE_DOMAIN);
        hasher.update(owner.instance_id.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(owner.incarnation.to_bytes());
        hasher.update(operation_id.to_bytes());
        hasher.update(provider_payload.as_ref());
        let digest = hasher.finalize().into();
        Ok(Self {
            owner,
            operation_id,
            provider_payload,
            digest,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.operation_id
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorStagedWritePlanningRequest {
    pub handle: ConnectorStagedTableHandle,
    pub operation_id: ConnectorWriteOperationId,
    pub intent: ConnectorWriteIntent,
    pub input_schema: SchemaRef,
    pub context: ConnectorRequestContext,
}

/// Provider-neutral writer facts derived from one exact invisible target.
/// The table and provider payload remain opaque to generic CTAS orchestration;
/// they are consumed only by the existing connector writer planner.
#[derive(Clone)]
pub struct ConnectorStagedWritePlanningBinding {
    owner: ConnectorExecutionBindingKey,
    target_operation_id: ConnectorStagedCreateOperationId,
    target_handle_digest: [u8; 32],
    operation_id: ConnectorWriteOperationId,
    intent: ConnectorWriteIntent,
    input_schema: SchemaRef,
    table: ConnectorTableHandle,
    provider_payload: Bytes,
    context: ConnectorRequestContext,
}

impl ConnectorStagedWritePlanningBinding {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        handle: &ConnectorStagedTableHandle,
        operation_id: ConnectorWriteOperationId,
        intent: ConnectorWriteIntent,
        input_schema: SchemaRef,
        table: ConnectorTableHandle,
        provider_payload: Bytes,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if provider_payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
            return Err(invalid(
                "staged writer provider payload exceeds the handle limit",
            ));
        }
        if table.owner() != &handle.owner().instance_id {
            return Err(invalid(
                "staged writer table handle does not match the exact target owner",
            ));
        }
        Ok(Self {
            owner: handle.owner().clone(),
            target_operation_id: handle.operation_id(),
            target_handle_digest: handle.digest(),
            operation_id,
            intent,
            input_schema,
            table,
            provider_payload,
            context,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub const fn target_operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.target_operation_id
    }

    pub const fn target_handle_digest(&self) -> [u8; 32] {
        self.target_handle_digest
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn intent(&self) -> ConnectorWriteIntent {
        self.intent
    }

    pub fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }

    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }
}

impl std::fmt::Debug for ConnectorStagedTableHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorStagedTableHandle")
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorStagedCreatePrepareRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub operation_id: ConnectorStagedCreateOperationId,
    pub table: ConnectorTableIdentity,
    pub columns: Vec<ConnectorColumnDefinition>,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
    pub policy: CreatePolicy,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreateReceiptPhase {
    Prepared,
    Published,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedCreateReceipt {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorStagedCreateOperationId,
    phase: ConnectorStagedCreateReceiptPhase,
    effect: ExternalMutationEffect,
    provider_payload: Bytes,
}

impl ConnectorStagedCreateReceipt {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
        effect: ExternalMutationEffect,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if provider_payload.len() > super::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "staged-create receipt exceeds the evidence limit",
            ));
        }
        Ok(Self {
            owner,
            operation_id,
            phase,
            effect,
            provider_payload,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.operation_id
    }
    pub const fn phase(&self) -> ConnectorStagedCreateReceiptPhase {
        self.phase
    }
    pub const fn effect(&self) -> ExternalMutationEffect {
        self.effect
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreatePrepareOutcome {
    Prepared {
        handle: ConnectorStagedTableHandle,
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    Conflict {
        failure: ConnectorMutationFailure,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

#[derive(Clone)]
pub struct ConnectorStagedCreatePublishRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub handle: ConnectorStagedTableHandle,
    pub completion: ConnectorWriteOperationCompletion,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreatePublishOutcome {
    Applied {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    NoOp {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    Conflict {
        failure: ConnectorMutationFailure,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

#[derive(Clone)]
pub struct ConnectorStagedCreateAbortRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub handle: ConnectorStagedTableHandle,
    pub completion: Option<ConnectorWriteOperationCompletion>,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreateAbortOutcome {
    Aborted {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreateReconcilePhase {
    Prepare,
    Publish,
    Abort,
}

#[derive(Clone)]
pub struct ConnectorStagedCreateReconcileRequest {
    pub target_operation_id: ConnectorStagedCreateOperationId,
    pub phase: ConnectorStagedCreateReconcilePhase,
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreateReconcileOutcome {
    Prepared {
        handle: ConnectorStagedTableHandle,
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    Published {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    Aborted {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

pub trait ConnectorStagedCreate: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ConnectorInstanceIncarnation;

    fn prepare(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>;

    fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError>;

    /// Bind one sealed writer aggregate to the exact opaque staged target.
    /// This is provider-local and must complete before the application records
    /// the write as ready for publication.
    fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorError>;

    fn publish(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError>;

    fn abort(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError>;

    fn reconcile(
        &self,
        request: ConnectorStagedCreateReconcileRequest,
    ) -> Result<ConnectorStagedCreateReconcileOutcome, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorStagedCreateLease {
    owner: ConnectorExecutionBindingKey,
    capability: Arc<dyn ConnectorStagedCreate>,
    operations: Arc<Mutex<HashMap<ConnectorStagedCreateOperationId, LeaseOperationState>>>,
    _release: Arc<StagedCreateLeaseRelease>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaseOperationState {
    Preparing,
    Unpublished {
        handle_digest: [u8; 32],
        bound_write: Option<BoundWriteProof>,
    },
    Published,
    Aborted,
    Unknown {
        phase: Option<ConnectorStagedCreateReconcilePhase>,
        evidence_digest: Option<[u8; 32]>,
        handle_digest: Option<[u8; 32]>,
        bound_write: Option<BoundWriteProof>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BoundWriteProof {
    operation_id: super::ConnectorWriteOperationId,
    aggregate_digest: [u8; 32],
}

struct StagedCreateLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorStagedCreateLease {
    pub fn new(
        owner: ConnectorExecutionBindingKey,
        capability: Arc<dyn ConnectorStagedCreate>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if capability.descriptor().instance_id != owner.instance_id
            || capability.incarnation() != owner.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "staged-create capability does not match its lease generation",
            ));
        }
        Ok(Self {
            owner,
            capability,
            operations: Arc::new(Mutex::new(HashMap::new())),
            _release: Arc::new(StagedCreateLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn prepare(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
        if request.owner != self.owner || request.table.instance_id != self.owner.instance_id {
            return Err(invalid("staged-create prepare request has a foreign owner"));
        }
        let operation_id = request.operation_id;
        {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
            if operations.contains_key(&operation_id) {
                return Err(invalid(
                    "staged-create operation ID is already reserved by this lease",
                ));
            }
            operations.insert(operation_id, LeaseOperationState::Preparing);
        }
        let outcome = match self.capability.prepare(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.record_after_dispatch(
                    operation_id,
                    Some(LeaseOperationState::Unknown {
                        phase: None,
                        evidence_digest: None,
                        handle_digest: None,
                        bound_write: None,
                    }),
                );
                return Err(error);
            }
        };
        if let Err(error) = self.validate_prepare_outcome(operation_id, &outcome) {
            self.record_after_dispatch(
                operation_id,
                Some(LeaseOperationState::Unknown {
                    phase: None,
                    evidence_digest: None,
                    handle_digest: None,
                    bound_write: None,
                }),
            );
            return Err(error);
        }
        let state = match &outcome {
            ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } => {
                Some(LeaseOperationState::Unpublished {
                    handle_digest: handle.digest(),
                    bound_write: None,
                })
            }
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { evidence, .. } => {
                Some(LeaseOperationState::Unknown {
                    phase: Some(ConnectorStagedCreateReconcilePhase::Prepare),
                    evidence_digest: Some(evidence.digest()),
                    handle_digest: None,
                    bound_write: None,
                })
            }
            ConnectorStagedCreatePrepareOutcome::Conflict { .. }
            | ConnectorStagedCreatePrepareOutcome::KnownUncommitted { .. } => None,
        };
        self.record_after_dispatch(operation_id, state);
        Ok(outcome)
    }

    pub fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorError> {
        self.require_unpublished(&handle)?;
        if completion.owner() != &self.owner {
            return Err(invalid(
                "staged-create write completion has a foreign owner",
            ));
        }
        let target_operation_id = handle.operation_id();
        let proof = BoundWriteProof {
            operation_id: completion.sealed().operation_id(),
            aggregate_digest: completion.aggregate_digest(),
        };
        if let Err(error) = self.capability.bind_write(handle.clone(), completion) {
            self.record_after_dispatch(
                target_operation_id,
                Some(LeaseOperationState::Unknown {
                    phase: None,
                    evidence_digest: None,
                    handle_digest: Some(handle.digest()),
                    bound_write: Some(proof),
                }),
            );
            return Err(error);
        }
        self.record_after_dispatch(
            target_operation_id,
            Some(LeaseOperationState::Unpublished {
                handle_digest: handle.digest(),
                bound_write: Some(proof),
            }),
        );
        Ok(())
    }

    pub fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
        self.require_unpublished(&request.handle)?;
        if request.handle.owner() != &self.owner {
            return Err(invalid("staged writer planning has a foreign owner"));
        }
        let target_operation_id = request.handle.operation_id();
        let target_handle_digest = request.handle.digest();
        let prior_bound_write = self.bound_write_for_abort(&request.handle, None)?;
        let write_operation_id = request.operation_id;
        let intent = request.intent;
        let input_schema = Arc::clone(&request.input_schema);
        let binding = match self.capability.plan_write(request) {
            Ok(binding) => binding,
            Err(error) => {
                self.record_after_dispatch(
                    target_operation_id,
                    Some(LeaseOperationState::Unpublished {
                        handle_digest: target_handle_digest,
                        bound_write: prior_bound_write,
                    }),
                );
                return Err(error);
            }
        };
        if binding.owner() != &self.owner
            || binding.target_operation_id() != target_operation_id
            || binding.target_handle_digest() != target_handle_digest
            || binding.operation_id() != write_operation_id
            || binding.intent() != intent
            || binding.input_schema().as_ref() != input_schema.as_ref()
        {
            self.set_unknown_without_evidence(
                target_operation_id,
                Some(target_handle_digest),
                prior_bound_write,
            );
            return Err(invalid(
                "staged writer planning binding drifted from its exact target request",
            ));
        }
        self.record_after_dispatch(
            target_operation_id,
            Some(LeaseOperationState::Unpublished {
                handle_digest: target_handle_digest,
                bound_write: prior_bound_write,
            }),
        );
        Ok(binding)
    }

    pub fn publish(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
        let bound_write = self.require_bound_write(&request.handle, &request.completion)?;
        if request.completion.owner() != &self.owner {
            return Err(invalid(
                "staged-create publish completion has a foreign owner",
            ));
        }
        let operation_id = request.handle.operation_id();
        let handle_digest = request.handle.digest();
        let dispatch_operation_id = request.operation_id;
        let outcome = match self.capability.publish(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.set_unknown_without_evidence(
                    operation_id,
                    Some(handle_digest),
                    Some(bound_write),
                );
                return Err(error);
            }
        };
        if let Err(error) = self.validate_publish_outcome(dispatch_operation_id, &outcome) {
            self.set_unknown_without_evidence(operation_id, Some(handle_digest), Some(bound_write));
            return Err(error);
        }
        let state = match &outcome {
            ConnectorStagedCreatePublishOutcome::Applied { .. } => LeaseOperationState::Published,
            ConnectorStagedCreatePublishOutcome::NoOp { .. }
            | ConnectorStagedCreatePublishOutcome::Conflict { .. }
            | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. } => {
                LeaseOperationState::Unpublished {
                    handle_digest,
                    bound_write: Some(bound_write),
                }
            }
            ConnectorStagedCreatePublishOutcome::CommitUnknown { evidence, .. } => {
                LeaseOperationState::Unknown {
                    phase: Some(ConnectorStagedCreateReconcilePhase::Publish),
                    evidence_digest: Some(evidence.digest()),
                    handle_digest: Some(handle_digest),
                    bound_write: Some(bound_write),
                }
            }
        };
        self.record_after_dispatch(operation_id, Some(state));
        Ok(outcome)
    }

    pub fn mark_write_unknown(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.owner {
            return Err(invalid("staged table handle has a foreign owner"));
        }
        let mut operations = self
            .operations
            .lock()
            .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
        let bound_write = match operations.get(&handle.operation_id()) {
            Some(LeaseOperationState::Unpublished {
                handle_digest,
                bound_write,
            }) if handle_digest == &handle.digest() => *bound_write,
            Some(LeaseOperationState::Unknown { .. }) => {
                return Err(invalid(
                    "staged table operation is unresolved; publish and abort are forbidden",
                ));
            }
            _ => {
                return Err(invalid(
                    "staged table handle was not issued by this retained lease",
                ));
            }
        };
        operations.insert(
            handle.operation_id(),
            LeaseOperationState::Unknown {
                phase: None,
                evidence_digest: None,
                handle_digest: Some(handle.digest()),
                bound_write,
            },
        );
        Ok(())
    }

    /// Restore an unpublished staged target after the generic writer session
    /// proves its exact sealed aggregate complete. This is an explicit
    /// recovery path for write-staging uncertainty; it never dispatches a
    /// catalog mutation and cannot substitute a different writer operation.
    pub fn reconcile_write_completion(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.owner || completion.owner() != &self.owner {
            return Err(invalid("staged-create writer recovery has a foreign owner"));
        }
        let target_operation_id = handle.operation_id();
        let proof = BoundWriteProof {
            operation_id: completion.sealed().operation_id(),
            aggregate_digest: completion.aggregate_digest(),
        };
        {
            let operations = self
                .operations
                .lock()
                .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
            match operations.get(&target_operation_id) {
                Some(LeaseOperationState::Unknown {
                    phase: None,
                    evidence_digest: None,
                    handle_digest: Some(handle_digest),
                    bound_write,
                }) if *handle_digest == handle.digest()
                    && bound_write.is_none_or(|bound| bound == proof) => {}
                _ => {
                    return Err(invalid(
                        "staged-create writer recovery requires the exact unresolved write",
                    ));
                }
            }
        }
        if let Err(error) = self.capability.bind_write(handle.clone(), completion) {
            self.record_after_dispatch(
                target_operation_id,
                Some(LeaseOperationState::Unknown {
                    phase: None,
                    evidence_digest: None,
                    handle_digest: Some(handle.digest()),
                    bound_write: Some(proof),
                }),
            );
            return Err(error);
        }
        self.record_after_dispatch(
            target_operation_id,
            Some(LeaseOperationState::Unpublished {
                handle_digest: handle.digest(),
                bound_write: Some(proof),
            }),
        );
        Ok(())
    }

    pub fn abort(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
        let bound_write =
            self.bound_write_for_abort(&request.handle, request.completion.as_ref())?;
        if request
            .completion
            .as_ref()
            .is_some_and(|completion| completion.owner() != &self.owner)
        {
            return Err(invalid(
                "staged-create abort completion has a foreign owner",
            ));
        }
        let operation_id = request.handle.operation_id();
        let handle_digest = request.handle.digest();
        let dispatch_operation_id = request.operation_id;
        let outcome = match self.capability.abort(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.set_unknown_without_evidence(operation_id, Some(handle_digest), bound_write);
                return Err(error);
            }
        };
        if let Err(error) = self.validate_abort_outcome(dispatch_operation_id, &outcome) {
            self.set_unknown_without_evidence(operation_id, Some(handle_digest), bound_write);
            return Err(error);
        }
        let state = match &outcome {
            ConnectorStagedCreateAbortOutcome::Aborted { .. } => LeaseOperationState::Aborted,
            ConnectorStagedCreateAbortOutcome::KnownUncommitted { .. } => {
                LeaseOperationState::Unpublished {
                    handle_digest,
                    bound_write,
                }
            }
            ConnectorStagedCreateAbortOutcome::CommitUnknown { evidence, .. } => {
                LeaseOperationState::Unknown {
                    phase: Some(ConnectorStagedCreateReconcilePhase::Abort),
                    evidence_digest: Some(evidence.digest()),
                    handle_digest: Some(handle_digest),
                    bound_write,
                }
            }
        };
        self.record_after_dispatch(operation_id, Some(state));
        Ok(outcome)
    }

    pub fn reconcile(
        &self,
        request: ConnectorStagedCreateReconcileRequest,
    ) -> Result<ConnectorStagedCreateReconcileOutcome, ConnectorError> {
        let operation_id = request.target_operation_id;
        let dispatch_operation_id = request.evidence.operation_id();
        let unresolved_evidence_digest = request.evidence.digest();
        self.validate_evidence_for(
            &request.evidence,
            dispatch_operation_id,
            operation_kind(request.phase),
        )?;
        let (unresolved_handle_digest, unresolved_bound_write) = {
            let operations = self
                .operations
                .lock()
                .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
            match operations.get(&operation_id) {
                Some(LeaseOperationState::Unknown {
                    phase: Some(phase),
                    evidence_digest: Some(digest),
                    handle_digest,
                    bound_write,
                }) if *phase == request.phase && *digest == request.evidence.digest() => {
                    (*handle_digest, *bound_write)
                }
                _ => {
                    return Err(invalid(
                        "staged-create reconcile requires the exact unresolved operation evidence",
                    ));
                }
            }
        };
        let phase = request.phase;
        let unresolved_state = LeaseOperationState::Unknown {
            phase: Some(phase),
            evidence_digest: Some(unresolved_evidence_digest),
            handle_digest: unresolved_handle_digest,
            bound_write: unresolved_bound_write,
        };
        let outcome = match self.capability.reconcile(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.record_after_dispatch(operation_id, Some(unresolved_state));
                return Err(error);
            }
        };
        let state = (|| -> Result<Option<LeaseOperationState>, ConnectorError> {
            let state = match &outcome {
                ConnectorStagedCreateReconcileOutcome::Prepared {
                    handle, receipt, ..
                } => {
                    if phase != ConnectorStagedCreateReconcilePhase::Prepare {
                        return Err(invalid(
                            "staged-create reconcile returned Prepared for the wrong phase",
                        ));
                    }
                    self.validate_handle(handle, operation_id)?;
                    self.validate_receipt(
                        receipt,
                        dispatch_operation_id,
                        ConnectorStagedCreateReceiptPhase::Prepared,
                    )?;
                    Some(LeaseOperationState::Unpublished {
                        handle_digest: handle.digest(),
                        bound_write: unresolved_bound_write,
                    })
                }
                ConnectorStagedCreateReconcileOutcome::Published { receipt, .. } => {
                    if phase != ConnectorStagedCreateReconcilePhase::Publish {
                        return Err(invalid(
                            "staged-create reconcile returned Published for the wrong phase",
                        ));
                    }
                    self.validate_receipt(
                        receipt,
                        dispatch_operation_id,
                        ConnectorStagedCreateReceiptPhase::Published,
                    )?;
                    Some(LeaseOperationState::Published)
                }
                ConnectorStagedCreateReconcileOutcome::Aborted { receipt, .. } => {
                    if phase != ConnectorStagedCreateReconcilePhase::Abort {
                        return Err(invalid(
                            "staged-create reconcile returned Aborted for the wrong phase",
                        ));
                    }
                    self.validate_receipt(
                        receipt,
                        dispatch_operation_id,
                        ConnectorStagedCreateReceiptPhase::Aborted,
                    )?;
                    Some(LeaseOperationState::Aborted)
                }
                ConnectorStagedCreateReconcileOutcome::KnownUncommitted { .. } => match phase {
                    ConnectorStagedCreateReconcilePhase::Prepare => None,
                    ConnectorStagedCreateReconcilePhase::Publish => {
                        let handle_digest = unresolved_handle_digest.ok_or_else(|| {
                        invalid(
                            "staged-create reconcile cannot restore an unpublished operation without its exact handle",
                        )
                    })?;
                        Some(LeaseOperationState::Unpublished {
                            handle_digest,
                            bound_write: unresolved_bound_write,
                        })
                    }
                    ConnectorStagedCreateReconcilePhase::Abort => {
                        // An abort reconcile attempt that did not itself dispatch
                        // cannot disprove possible progress from the original
                        // abort. Preserve the exact unknown lease and evidence;
                        // only a provider-confirmed Aborted receipt may reopen it.
                        Some(LeaseOperationState::Unknown {
                            phase: Some(phase),
                            evidence_digest: Some(unresolved_evidence_digest),
                            handle_digest: unresolved_handle_digest,
                            bound_write: unresolved_bound_write,
                        })
                    }
                },
                ConnectorStagedCreateReconcileOutcome::CommitUnknown { evidence, .. } => {
                    self.validate_evidence_for(
                        evidence,
                        dispatch_operation_id,
                        operation_kind(phase),
                    )?;
                    Some(LeaseOperationState::Unknown {
                        phase: Some(phase),
                        evidence_digest: Some(evidence.digest()),
                        handle_digest: unresolved_handle_digest,
                        bound_write: unresolved_bound_write,
                    })
                }
            };
            Ok(state)
        })();
        let state = match state {
            Ok(state) => state,
            Err(error) => {
                self.record_after_dispatch(operation_id, Some(unresolved_state));
                return Err(error);
            }
        };
        self.record_after_dispatch(operation_id, state);
        Ok(outcome)
    }

    fn require_unpublished(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.owner {
            return Err(invalid("staged table handle has a foreign owner"));
        }
        let operations = self
            .operations
            .lock()
            .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
        match operations.get(&handle.operation_id()) {
            Some(LeaseOperationState::Unpublished { handle_digest, .. })
                if handle_digest == &handle.digest() =>
            {
                Ok(())
            }
            Some(LeaseOperationState::Unknown { .. }) => Err(invalid(
                "staged table operation is unresolved; publish and abort are forbidden",
            )),
            Some(LeaseOperationState::Published) => Err(invalid(
                "published staged table cannot be aborted or republished",
            )),
            Some(LeaseOperationState::Aborted) => {
                Err(invalid("aborted staged table cannot be reused"))
            }
            Some(LeaseOperationState::Preparing) => {
                Err(invalid("staged table prepare is still in progress"))
            }
            _ => Err(invalid(
                "staged table handle was not issued by this retained lease",
            )),
        }
    }

    fn require_bound_write(
        &self,
        handle: &ConnectorStagedTableHandle,
        completion: &ConnectorWriteOperationCompletion,
    ) -> Result<BoundWriteProof, ConnectorError> {
        let bound = self.bound_write_for_abort(handle, Some(completion))?;
        bound.ok_or_else(|| {
            invalid("staged-create publish requires an exact bound writer aggregate")
        })
    }

    fn bound_write_for_abort(
        &self,
        handle: &ConnectorStagedTableHandle,
        completion: Option<&ConnectorWriteOperationCompletion>,
    ) -> Result<Option<BoundWriteProof>, ConnectorError> {
        if handle.owner() != &self.owner {
            return Err(invalid("staged table handle has a foreign owner"));
        }
        let operations = self
            .operations
            .lock()
            .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
        let Some(LeaseOperationState::Unpublished {
            handle_digest,
            bound_write,
        }) = operations.get(&handle.operation_id())
        else {
            drop(operations);
            self.require_unpublished(handle)?;
            unreachable!("require_unpublished accepted a non-unpublished state")
        };
        if handle_digest != &handle.digest() {
            return Err(invalid("staged table handle digest mismatch"));
        }
        if let Some(completion) = completion {
            let observed = BoundWriteProof {
                operation_id: completion.sealed().operation_id(),
                aggregate_digest: completion.aggregate_digest(),
            };
            if bound_write.as_ref() != Some(&observed) {
                return Err(invalid(
                    "staged-create completion is not bound to this exact opaque target",
                ));
            }
        }
        Ok(*bound_write)
    }

    fn set_unknown_without_evidence(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        handle_digest: Option<[u8; 32]>,
        bound_write: Option<BoundWriteProof>,
    ) {
        self.record_after_dispatch(
            operation_id,
            Some(LeaseOperationState::Unknown {
                phase: None,
                evidence_digest: None,
                handle_digest,
                bound_write,
            }),
        );
    }

    fn record_after_dispatch(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        state: Option<LeaseOperationState>,
    ) {
        // Once provider dispatch has happened, a poisoned process-local lease
        // mutex must not downgrade an authoritative typed outcome or discard
        // exact unknown evidence. Recover the retained map and record the
        // result before returning it to the caller.
        let mut operations = match self.operations.lock() {
            Ok(operations) => operations,
            Err(poisoned) => {
                let operations = poisoned.into_inner();
                self.operations.clear_poison();
                operations
            }
        };
        if let Some(state) = state {
            operations.insert(operation_id, state);
        } else {
            operations.remove(&operation_id);
        }
    }

    fn validate_prepare_outcome(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        outcome: &ConnectorStagedCreatePrepareOutcome,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ConnectorStagedCreatePrepareOutcome::Prepared {
                handle, receipt, ..
            } => {
                self.validate_handle(handle, operation_id)?;
                self.validate_receipt(
                    receipt,
                    operation_id,
                    ConnectorStagedCreateReceiptPhase::Prepared,
                )
            }
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { evidence, .. } => self
                .validate_evidence_for(
                    evidence,
                    operation_id,
                    operation_kind(ConnectorStagedCreateReconcilePhase::Prepare),
                ),
            ConnectorStagedCreatePrepareOutcome::Conflict { .. }
            | ConnectorStagedCreatePrepareOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    fn validate_publish_outcome(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        outcome: &ConnectorStagedCreatePublishOutcome,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ConnectorStagedCreatePublishOutcome::Applied { receipt, .. }
            | ConnectorStagedCreatePublishOutcome::NoOp { receipt, .. } => self.validate_receipt(
                receipt,
                operation_id,
                ConnectorStagedCreateReceiptPhase::Published,
            ),
            ConnectorStagedCreatePublishOutcome::CommitUnknown { evidence, .. } => self
                .validate_evidence_for(
                    evidence,
                    operation_id,
                    operation_kind(ConnectorStagedCreateReconcilePhase::Publish),
                ),
            ConnectorStagedCreatePublishOutcome::Conflict { .. }
            | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    fn validate_abort_outcome(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        outcome: &ConnectorStagedCreateAbortOutcome,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ConnectorStagedCreateAbortOutcome::Aborted { receipt, .. } => self.validate_receipt(
                receipt,
                operation_id,
                ConnectorStagedCreateReceiptPhase::Aborted,
            ),
            ConnectorStagedCreateAbortOutcome::CommitUnknown { evidence, .. } => self
                .validate_evidence_for(
                    evidence,
                    operation_id,
                    operation_kind(ConnectorStagedCreateReconcilePhase::Abort),
                ),
            ConnectorStagedCreateAbortOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    fn validate_handle(
        &self,
        handle: &ConnectorStagedTableHandle,
        operation_id: ConnectorStagedCreateOperationId,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.owner || handle.operation_id() != operation_id {
            return Err(invalid(
                "staged table handle does not match its prepare request",
            ));
        }
        Ok(())
    }

    fn validate_receipt(
        &self,
        receipt: &ConnectorStagedCreateReceipt,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
    ) -> Result<(), ConnectorError> {
        if receipt.owner() != &self.owner
            || receipt.operation_id() != operation_id
            || receipt.phase() != phase
        {
            return Err(invalid("staged-create receipt does not match its request"));
        }
        Ok(())
    }

    fn validate_evidence_for(
        &self,
        evidence: &ExternalMutationEvidence,
        operation_id: ConnectorStagedCreateOperationId,
        operation_kind: &str,
    ) -> Result<(), ConnectorError> {
        if evidence.descriptor().instance_id != self.owner.instance_id
            || evidence.incarnation() != self.owner.incarnation
            || evidence.operation_id() != operation_id
            || evidence.operation_kind() != operation_kind
        {
            return Err(invalid(
                "staged-create evidence does not match its lease generation",
            ));
        }
        Ok(())
    }
}

fn operation_kind(phase: ConnectorStagedCreateReconcilePhase) -> &'static str {
    match phase {
        ConnectorStagedCreateReconcilePhase::Prepare => "staged-create-prepare",
        ConnectorStagedCreateReconcilePhase::Publish => "staged-create-publish",
        ConnectorStagedCreateReconcilePhase::Abort => "staged-create-abort",
    }
}

impl Drop for StagedCreateLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::connector::{
        CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorCancellation, ConnectorInstanceId,
        ConnectorProviderId, ConnectorSealedWriteCohortSet, ConnectorStagedReport,
        ConnectorStagedReportSummary, ConnectorWriteAttemptCompletion,
        ConnectorWriteCohortCompletion, ConnectorWriteCohortDescriptor, ConnectorWriteCohortId,
        ConnectorWriteExecutionId, ConnectorWriteIntent, ConnectorWriteOperationId,
        ConnectorWriterIdentity, ConnectorWriterTerminalState,
    };

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct FakeCapability {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        aborts: AtomicUsize,
        prepares: AtomicUsize,
        unknown_prepare: bool,
        fail_prepare: bool,
        malformed_prepare: bool,
        noop_publish: bool,
        unknown_abort: bool,
        known_uncommitted_reconcile: bool,
        binds: AtomicUsize,
        publishes: AtomicUsize,
    }

    impl FakeCapability {
        fn receipt(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
            phase: ConnectorStagedCreateReceiptPhase,
        ) -> ConnectorStagedCreateReceipt {
            ConnectorStagedCreateReceipt::try_new(
                ConnectorExecutionBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.incarnation,
                },
                operation_id,
                phase,
                ExternalMutationEffect::Applied,
                Bytes::new(),
            )
            .unwrap()
        }

        fn evidence(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
        ) -> ExternalMutationEvidence {
            self.evidence_for(operation_id, "staged-create-prepare")
        }

        fn evidence_for(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
            operation_kind: &str,
        ) -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                self.descriptor.clone(),
                self.incarnation,
                operation_id,
                operation_kind,
                Bytes::from_static(b"unknown"),
            )
            .unwrap()
        }
    }

    impl ConnectorStagedCreate for FakeCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.incarnation
        }
        fn prepare(
            &self,
            request: ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
            self.prepares.fetch_add(1, Ordering::SeqCst);
            if self.fail_prepare {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "prepare dispatch failed",
                ));
            }
            if self.unknown_prepare {
                return Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        super::super::ConnectorMutationFailureKind::Unavailable,
                        "unknown",
                    ),
                    evidence: self.evidence(request.operation_id),
                });
            }
            let handle = ConnectorStagedTableHandle::try_new(
                request.owner,
                if self.malformed_prepare {
                    ConnectorMutationOperationId::new()
                } else {
                    request.operation_id
                },
                Bytes::from_static(b"provider-handle"),
            )?;
            Ok(ConnectorStagedCreatePrepareOutcome::Prepared {
                handle,
                receipt: self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Prepared,
                ),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn publish(
            &self,
            request: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
            self.publishes.fetch_add(1, Ordering::SeqCst);
            if self.noop_publish {
                return Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                    receipt: self.receipt(
                        request.operation_id,
                        ConnectorStagedCreateReceiptPhase::Published,
                    ),
                    finalization: ExternalMutationFinalization::Complete,
                });
            }
            unreachable!("not used by these conformance tests")
        }
        fn plan_write(
            &self,
            request: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
            let table = ConnectorTableHandle::try_new(
                request.handle.owner().instance_id.clone(),
                Bytes::new(),
            )?;
            ConnectorStagedWritePlanningBinding::try_new(
                &request.handle,
                request.operation_id,
                request.intent,
                request.input_schema,
                table,
                Bytes::new(),
                request.context,
            )
        }
        fn bind_write(
            &self,
            _: ConnectorStagedTableHandle,
            _: ConnectorWriteOperationCompletion,
        ) -> Result<(), ConnectorError> {
            self.binds.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn abort(
            &self,
            request: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            if self.unknown_abort {
                return Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        super::super::ConnectorMutationFailureKind::Unavailable,
                        "unknown abort",
                    ),
                    evidence: self.evidence_for(request.operation_id, "staged-create-abort"),
                });
            }
            Ok(ConnectorStagedCreateAbortOutcome::Aborted {
                receipt: self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Aborted,
                ),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn reconcile(
            &self,
            _: ConnectorStagedCreateReconcileRequest,
        ) -> Result<ConnectorStagedCreateReconcileOutcome, ConnectorError> {
            if self.known_uncommitted_reconcile {
                return Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        super::super::ConnectorMutationFailureKind::Internal,
                        "current abort reconcile was not dispatched",
                    ),
                });
            }
            unreachable!("not used by these conformance tests")
        }
    }

    struct PoisoningCapability {
        inner: FakeCapability,
        poison_on_plan_write: Mutex<
            Option<Arc<Mutex<HashMap<ConnectorStagedCreateOperationId, LeaseOperationState>>>>,
        >,
        poison_on_abort: Mutex<
            Option<Arc<Mutex<HashMap<ConnectorStagedCreateOperationId, LeaseOperationState>>>>,
        >,
    }

    impl ConnectorStagedCreate for PoisoningCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            self.inner.descriptor()
        }

        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.inner.incarnation()
        }

        fn prepare(
            &self,
            request: ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
            self.inner.prepare(request)
        }

        fn publish(
            &self,
            request: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
            self.inner.publish(request)
        }

        fn plan_write(
            &self,
            request: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
            if let Some(operations) = self.poison_on_plan_write.lock().unwrap().take() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _guard = operations.lock().unwrap();
                    panic!("poison staged-create lease during provider planning");
                }));
            }
            self.inner.plan_write(request)
        }

        fn bind_write(
            &self,
            handle: ConnectorStagedTableHandle,
            completion: ConnectorWriteOperationCompletion,
        ) -> Result<(), ConnectorError> {
            self.inner.bind_write(handle, completion)
        }

        fn abort(
            &self,
            request: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
            if let Some(operations) = self.poison_on_abort.lock().unwrap().take() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _guard = operations.lock().unwrap();
                    panic!("poison staged-create lease during provider abort");
                }));
            }
            self.inner.abort(request)
        }

        fn reconcile(
            &self,
            request: ConnectorStagedCreateReconcileRequest,
        ) -> Result<ConnectorStagedCreateReconcileOutcome, ConnectorError> {
            self.inner.reconcile(request)
        }
    }

    fn owner() -> (ConnectorInstanceDescriptor, ConnectorInstanceIncarnation) {
        (
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("rest").unwrap(),
            },
            ConnectorInstanceIncarnation::new(),
        )
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            Arc::new(NeverCancelled),
            super::super::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            super::super::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    fn prepare_request(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
    ) -> ConnectorStagedCreatePrepareRequest {
        ConnectorStagedCreatePrepareRequest {
            table: ConnectorTableIdentity {
                instance_id: owner.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            owner,
            operation_id,
            columns: Vec::new(),
            partitioning: Vec::new(),
            properties: BTreeMap::new(),
            policy: CreatePolicy::FailIfExists,
            context: context(),
        }
    }

    fn completion(owner: ConnectorExecutionBindingKey) -> ConnectorWriteOperationCompletion {
        completion_for(owner, ConnectorWriteOperationId::new())
    }

    fn completion_for(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> ConnectorWriteOperationCompletion {
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([11; 16], 1);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            [12; 16],
            1,
            0,
            0,
            owner.clone(),
        );
        let report = ConnectorStagedReport::try_new(
            writer,
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"report"),
        )
        .unwrap();
        let accepted = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            operation_id,
            cohort_id,
            execution_id,
            [13; 32],
            vec![report],
            Bytes::new(),
        )
        .unwrap();
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Append,
                [14; 32],
            )],
        )
        .unwrap();
        ConnectorWriteOperationCompletion::try_new(
            owner,
            sealed,
            vec![
                ConnectorWriteCohortCompletion::try_new(cohort_id, Some(accepted), vec![]).unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn exact_staged_writer_plan_can_recover_unknown_staging_without_catalog_dispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        let write_operation_id = ConnectorWriteOperationId::new();
        let binding = lease
            .plan_write(ConnectorStagedWritePlanningRequest {
                handle: handle.clone(),
                operation_id: write_operation_id,
                intent: ConnectorWriteIntent::Append,
                input_schema: Arc::new(arrow::datatypes::Schema::empty()),
                context: context(),
            })
            .unwrap();
        assert_eq!(binding.operation_id(), write_operation_id);
        assert_eq!(binding.target_handle_digest(), handle.digest());
        let completion = completion_for(lease.owner().clone(), write_operation_id);
        lease.mark_write_unknown(&handle).unwrap();
        lease
            .reconcile_write_completion(handle.clone(), completion.clone())
            .unwrap();
        lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle,
                completion: Some(completion),
                context: context(),
            })
            .unwrap();
        assert_eq!(capability.binds.load(Ordering::SeqCst), 1);
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 0);
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn handle_is_exact_instance_bound_and_abort_is_unpublished_only() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected prepared")
        };
        let mut foreign_owner = lease.owner().clone();
        foreign_owner.incarnation = ConnectorInstanceIncarnation::new();
        let foreign = ConnectorStagedTableHandle::try_new(
            foreign_owner,
            operation_id,
            handle.provider_payload().clone(),
        )
        .unwrap();
        assert!(
            lease
                .abort(ConnectorStagedCreateAbortRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle: foreign,
                    completion: None,
                    context: context(),
                })
                .is_err()
        );
        lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle: handle.clone(),
                completion: None,
                context: context(),
            })
            .unwrap();
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
        assert!(
            lease
                .abort(ConnectorStagedCreateAbortRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle,
                    completion: None,
                    context: context(),
                })
                .is_err()
        );
    }

    #[test]
    fn unknown_prepare_forbids_abort_without_provider_dispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: true,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let outcome = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. }
        ));
        let forged = ConnectorStagedTableHandle::try_new(
            lease.owner().clone(),
            operation_id,
            Bytes::from_static(b"provider-handle"),
        )
        .unwrap();
        assert!(
            lease
                .abort(ConnectorStagedCreateAbortRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle: forged,
                    completion: None,
                    context: context(),
                })
                .is_err()
        );
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn duplicate_operation_id_is_rejected_before_provider_dispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap();
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert_eq!(capability.prepares.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn prepare_dispatch_error_locks_reservation_without_redispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: true,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert_eq!(capability.prepares.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn malformed_prepare_outcome_locks_reservation_without_redispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: true,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert_eq!(capability.prepares.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn completion_bound_to_one_target_cannot_publish_another_target() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle: first, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected first prepared target")
        };
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle: second, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected second prepared target")
        };
        let completion = completion(lease.owner().clone());
        lease.bind_write(first, completion.clone()).unwrap();
        assert!(
            lease
                .publish(ConnectorStagedCreatePublishRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle: second,
                    completion,
                    context: context(),
                })
                .is_err()
        );
        assert_eq!(capability.binds.load(Ordering::SeqCst), 1);
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn publish_noop_retains_exact_target_for_explicit_abort() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: true,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        let completion = completion(lease.owner().clone());
        lease
            .bind_write(handle.clone(), completion.clone())
            .unwrap();
        let outcome = lease
            .publish(ConnectorStagedCreatePublishRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle: handle.clone(),
                completion: completion.clone(),
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreatePublishOutcome::NoOp { .. }
        ));
        lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle,
                completion: Some(completion),
                context: context(),
            })
            .unwrap();
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 1);
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn abort_reconcile_known_uncommitted_preserves_prior_unknown() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: true,
            known_uncommitted_reconcile: true,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        let ConnectorStagedCreateAbortOutcome::CommitUnknown { evidence, .. } = lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle: handle.clone(),
                completion: None,
                context: context(),
            })
            .unwrap()
        else {
            panic!("expected unknown abort")
        };
        let outcome = lease
            .reconcile(ConnectorStagedCreateReconcileRequest {
                target_operation_id: operation_id,
                phase: ConnectorStagedCreateReconcilePhase::Abort,
                evidence,
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreateReconcileOutcome::KnownUncommitted { .. }
        ));
        assert!(
            lease
                .abort(ConnectorStagedCreateAbortRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle,
                    completion: None,
                    context: context(),
                })
                .is_err()
        );
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn post_dispatch_recording_recovers_poisoned_lease_state() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            capability,
            || {},
        )
        .unwrap();
        let operations = Arc::clone(&lease.operations);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = operations.lock().unwrap();
            panic!("poison staged-create lease state");
        }));
        let operation_id = ConnectorMutationOperationId::new();
        lease.record_after_dispatch(operation_id, Some(LeaseOperationState::Published));
        let operations = lease
            .operations
            .lock()
            .expect("post-dispatch recording clears poison");
        assert!(matches!(
            operations.get(&operation_id),
            Some(LeaseOperationState::Published)
        ));
    }

    #[test]
    fn abort_dispatch_records_authoritative_outcome_and_clears_poison() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(PoisoningCapability {
            inner: FakeCapability {
                descriptor: descriptor.clone(),
                incarnation,
                aborts: AtomicUsize::new(0),
                prepares: AtomicUsize::new(0),
                unknown_prepare: false,
                fail_prepare: false,
                malformed_prepare: false,
                noop_publish: false,
                unknown_abort: false,
                known_uncommitted_reconcile: false,
                binds: AtomicUsize::new(0),
                publishes: AtomicUsize::new(0),
            },
            poison_on_plan_write: Mutex::new(None),
            poison_on_abort: Mutex::new(None),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        *capability.poison_on_abort.lock().unwrap() = Some(Arc::clone(&lease.operations));
        let outcome = lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle,
                completion: None,
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreateAbortOutcome::Aborted { .. }
        ));
        assert_eq!(capability.inner.aborts.load(Ordering::SeqCst), 1);
        let operations = lease
            .operations
            .lock()
            .expect("authoritative abort clears lease poison");
        assert!(matches!(
            operations.get(&operation_id),
            Some(LeaseOperationState::Aborted)
        ));
    }

    #[test]
    fn plan_write_dispatch_clears_poison_before_bind() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(PoisoningCapability {
            inner: FakeCapability {
                descriptor: descriptor.clone(),
                incarnation,
                aborts: AtomicUsize::new(0),
                prepares: AtomicUsize::new(0),
                unknown_prepare: false,
                fail_prepare: false,
                malformed_prepare: false,
                noop_publish: false,
                unknown_abort: false,
                known_uncommitted_reconcile: false,
                binds: AtomicUsize::new(0),
                publishes: AtomicUsize::new(0),
            },
            poison_on_plan_write: Mutex::new(None),
            poison_on_abort: Mutex::new(None),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        *capability.poison_on_plan_write.lock().unwrap() = Some(Arc::clone(&lease.operations));
        let write_operation_id = ConnectorWriteOperationId::new();
        lease
            .plan_write(ConnectorStagedWritePlanningRequest {
                handle: handle.clone(),
                operation_id: write_operation_id,
                intent: ConnectorWriteIntent::Append,
                input_schema: Arc::new(arrow::datatypes::Schema::empty()),
                context: context(),
            })
            .unwrap();
        let completion = completion_for(lease.owner().clone(), write_operation_id);
        lease.bind_write(handle, completion).unwrap();
        assert_eq!(capability.inner.binds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reconcile_rejects_foreign_operation_kind_and_digest() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: true,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            unknown_abort: false,
            known_uncommitted_reconcile: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorExecutionBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability,
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::CommitUnknown { evidence, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected unknown")
        };
        let wrong_kind = ExternalMutationEvidence::try_new(
            1,
            descriptor.clone(),
            incarnation,
            operation_id,
            "staged-create-publish",
            evidence.provider_payload().clone(),
        )
        .unwrap();
        assert!(
            lease
                .reconcile(ConnectorStagedCreateReconcileRequest {
                    target_operation_id: operation_id,
                    phase: ConnectorStagedCreateReconcilePhase::Prepare,
                    evidence: wrong_kind,
                    context: context(),
                })
                .is_err()
        );
        let wrong_payload = ExternalMutationEvidence::try_new(
            1,
            descriptor,
            incarnation,
            operation_id,
            "staged-create-prepare",
            Bytes::from_static(b"different"),
        )
        .unwrap();
        assert!(
            lease
                .reconcile(ConnectorStagedCreateReconcileRequest {
                    target_operation_id: operation_id,
                    phase: ConnectorStagedCreateReconcilePhase::Prepare,
                    evidence: wrong_payload,
                    context: context(),
                })
                .is_err()
        );
    }
}
