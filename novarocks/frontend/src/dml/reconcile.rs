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

//! Projection of the generic Connector terminal contract into the durable DML
//! journal. The frontend records only SPI-owned wire envelopes and never
//! inspects provider payloads.

use novarocks_spi::connector::{
    ConnectorEstablishedWriteFence, ConnectorExternalFenceFailure, ConnectorExternalFenceReceipt,
    ConnectorExternalOperationFence, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorTableIdentity, ConnectorWriteAbortOutcome, ConnectorWriteReceipt,
    ConnectorWriteTargetRef, ExternalMutationEffect, ExternalMutationFinalization,
    ExternalMutationOutcome,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::model::{
    ConnectorWriteFailureKind, ConnectorWriteFailureRecord, ConnectorWriteFinalizationRecord,
    ConnectorWriteLifecycleRecord, ConnectorWriteReceiptWire,
    DML_DIRECT_MUTATION_FENCE_CODEC_VERSION, DML_EXTERNAL_FENCE_CODEC_VERSION,
    DML_OPAQUE_PAYLOAD_LIMIT, DmlDirectMutationFenceReceiptRecord, DmlDirectMutationKind,
    DmlExternalFenceGeneration, DmlExternalFenceIdentity, DmlExternalFenceReceiptRecord,
    DmlOpaquePayload, ExternalMutationEvidenceWire, OperationFact, OperationState,
    validate_direct_mutation_fence_receipt,
};
use crate::dml::now_unix_millis;

/// Domain separator for the fenced resource digest. The frontend hashes only
/// SPI-owned identity strings; it never reads a provider payload to build it.
const DML_EXTERNAL_FENCE_RESOURCE_DOMAIN: &[u8] = b"novarocks.dml.external-fence-resource.v1\0";

pub fn operation_fact_from_outcome(
    outcome: &ExternalMutationOutcome<ConnectorWriteReceipt>,
) -> Result<OperationFact, String> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            ..
        } => Ok(OperationFact {
            state: OperationState::Committed,
            lifecycle: ConnectorWriteLifecycleRecord::KnownEmpty,
        }),
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization,
        } => known_committed_fact(receipt, finalization),
        ExternalMutationOutcome::KnownUncommitted { failure } => Ok(OperationFact {
            state: OperationState::FailedKnownUncommitted,
            lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                failure: failure.into(),
            },
        }),
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => Ok(OperationFact {
            state: OperationState::CommitUnknown,
            lifecycle: ConnectorWriteLifecycleRecord::CommitUnknown {
                evidence_wire: ExternalMutationEvidenceWire::try_from_evidence(evidence)?,
                failure: failure.into(),
            },
        }),
    }
}

pub fn operation_fact_from_finalize_failure(
    receipt: &ConnectorWriteReceipt,
    failure: &ConnectorMutationFailure,
) -> Result<OperationFact, String> {
    known_committed_fact(
        receipt,
        &ExternalMutationFinalization::Failed(failure.clone()),
    )
}

pub fn operation_fact_from_abort_outcome(
    outcome: &ConnectorWriteAbortOutcome,
) -> Result<OperationFact, String> {
    match outcome {
        ConnectorWriteAbortOutcome::KnownUncommitted { .. } => Ok(OperationFact {
            state: OperationState::FailedKnownUncommitted,
            lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                failure: failure_record("writer abort completed"),
            },
        }),
        ConnectorWriteAbortOutcome::KnownCommitted {
            receipt,
            finalization,
        } => known_committed_fact(receipt, finalization),
        ConnectorWriteAbortOutcome::CommitUnknown { failure, evidence } => Ok(OperationFact {
            state: OperationState::CommitUnknown,
            lifecycle: ConnectorWriteLifecycleRecord::CommitUnknown {
                evidence_wire: ExternalMutationEvidenceWire::try_from_evidence(evidence)?,
                failure: failure.into(),
            },
        }),
    }
}

fn known_committed_fact(
    receipt: &ConnectorWriteReceipt,
    finalization: &ExternalMutationFinalization,
) -> Result<OperationFact, String> {
    let finalization = match finalization {
        ExternalMutationFinalization::Complete => ConnectorWriteFinalizationRecord::Complete,
        ExternalMutationFinalization::Failed(failure) => {
            ConnectorWriteFinalizationRecord::Failed(failure.into())
        }
    };
    let state = match finalization {
        ConnectorWriteFinalizationRecord::Complete => OperationState::Committed,
        ConnectorWriteFinalizationRecord::Failed(_) => OperationState::FinalizeFailedKnownCommitted,
    };
    Ok(OperationFact {
        state,
        lifecycle: ConnectorWriteLifecycleRecord::KnownCommitted {
            receipt_wire: ConnectorWriteReceiptWire::try_from_receipt(receipt)?,
            finalization,
        },
    })
}

fn failure_record(message: &str) -> ConnectorWriteFailureRecord {
    (&ConnectorMutationFailure::new(ConnectorMutationFailureKind::Cancelled, message)).into()
}

/// Project one confirmed external operation fence into its durable journal
/// record (CP-3B fence semantics, invariant 4).
///
/// Only identity, generation scalars, digests, and the bounded opaque provider
/// receipt cross this boundary: the frontend never decodes the receipt payload
/// and never learns what the provider used as its linearization marker.
pub fn external_fence_receipt_record(
    established: &ConnectorEstablishedWriteFence,
) -> Result<DmlExternalFenceReceiptRecord, String> {
    external_fence_receipt_record_parts(established.fence(), established.receipt())
}

/// Project a fence value and the provider receipt that acknowledged it.
///
/// Direct mutation establishes its fence through the data-mutation lease, which
/// hands back the bare receipt rather than a write-authority pair, so the
/// projection is shared at this level instead of being duplicated per family.
pub fn external_fence_receipt_record_parts(
    fence: &ConnectorExternalOperationFence,
    receipt: &ConnectorExternalFenceReceipt,
) -> Result<DmlExternalFenceReceiptRecord, String> {
    fence.validate().map_err(|error| error.to_string())?;
    receipt.validate().map_err(|error| error.to_string())?;
    if !receipt.matches(fence) {
        return Err(
            "connector external fence receipt acknowledges another fence value".to_string(),
        );
    }
    let generation = fence.generation();
    Ok(DmlExternalFenceReceiptRecord {
        codec_version: DML_EXTERNAL_FENCE_CODEC_VERSION,
        identity: DmlExternalFenceIdentity {
            cluster_identity_digest: hex::encode(fence.cluster().digest()),
            resource_digest: fenced_resource_digest(fence.table(), fence.target_ref()),
            write_operation_id: Uuid::from_bytes(fence.operation_id().to_bytes()),
            coordination_attempt_id: Uuid::from_bytes(fence.coordination_attempt_id()),
            generation: DmlExternalFenceGeneration {
                control_plane_incarnation: generation.control_plane_incarnation(),
                resource_epoch: generation.resource_epoch(),
                fence_generation: generation.coordination_attempt(),
            },
        },
        fence_digest: hex::encode(fence.digest()),
        receipt_digest: hex::encode(receipt.digest()),
        receipt_payload: DmlOpaquePayload::try_new(receipt.payload().to_vec())?,
        established_at_ms: now_unix_millis(),
    })
}

/// The largest fence receipt record this coordination attempt could produce.
///
/// The runner preflights this probe *before* it asks a provider to establish
/// anything. Only the bounded opaque provider payload can vary in size, so a
/// maximum-size probe bounds every receipt the attempt can return: if the
/// journal cannot hold the probe, no external marker may be created at all.
///
/// The identity fields the connector owns are not known yet, so the probe
/// carries same-shape stand-ins. This is a size and shape check, never a
/// durable record.
pub fn external_fence_preflight_probe(
    stand_in_write_operation_id: Uuid,
    coordination_attempt_id: Uuid,
    generation: DmlExternalFenceGeneration,
) -> Result<DmlExternalFenceReceiptRecord, String> {
    let digest = hex::encode([0xFFu8; 32]);
    Ok(DmlExternalFenceReceiptRecord {
        codec_version: DML_EXTERNAL_FENCE_CODEC_VERSION,
        identity: DmlExternalFenceIdentity {
            cluster_identity_digest: digest.clone(),
            resource_digest: digest.clone(),
            write_operation_id: stand_in_write_operation_id,
            coordination_attempt_id,
            generation,
        },
        fence_digest: digest.clone(),
        receipt_digest: digest,
        receipt_payload: DmlOpaquePayload::try_new(vec![0xFF; DML_OPAQUE_PAYLOAD_LIMIT])?,
        established_at_ms: now_unix_millis(),
    })
}

/// Project one confirmed direct-mutation fence into its durable journal record.
///
/// TRUNCATE and ADD FILES share the CP-3B fence carrier, so the record adds only
/// the mutation family and — for ADD FILES alone — the immutable source scope
/// the fence was minted for. Everything else is the same identity, generation
/// scalars, digests, and bounded opaque provider receipt.
pub fn direct_mutation_fence_receipt_record(
    operation_kind: DmlDirectMutationKind,
    fence: &ConnectorExternalOperationFence,
    receipt: &ConnectorExternalFenceReceipt,
    source_scope_digest: Option<String>,
) -> Result<DmlDirectMutationFenceReceiptRecord, String> {
    let record = DmlDirectMutationFenceReceiptRecord {
        codec_version: DML_DIRECT_MUTATION_FENCE_CODEC_VERSION,
        operation_kind,
        fence: external_fence_receipt_record_parts(fence, receipt)?,
        source_scope_digest,
    };
    validate_direct_mutation_fence_receipt(&record)?;
    Ok(record)
}

/// The largest direct-mutation fence record this attempt could produce.
///
/// The caller preflights this probe *before* it asks the provider to publish a
/// marker: only the bounded opaque provider payload can vary in size, so a
/// maximum-size probe bounds every receipt the attempt can return. The ADD FILES
/// source scope is already immutable at this point and is carried verbatim, so
/// the probe has the exact shape the real record will have.
pub fn direct_mutation_fence_preflight_probe(
    operation_kind: DmlDirectMutationKind,
    stand_in_write_operation_id: Uuid,
    coordination_attempt_id: Uuid,
    generation: DmlExternalFenceGeneration,
    source_scope_digest: Option<String>,
) -> Result<DmlDirectMutationFenceReceiptRecord, String> {
    Ok(DmlDirectMutationFenceReceiptRecord {
        codec_version: DML_DIRECT_MUTATION_FENCE_CODEC_VERSION,
        operation_kind,
        fence: external_fence_preflight_probe(
            stand_in_write_operation_id,
            coordination_attempt_id,
            generation,
        )?,
        source_scope_digest,
    })
}

/// Bounded digest of the fenced resource identity: the connector instance, the
/// table, and the write target ref the fence was minted for.
fn fenced_resource_digest(
    table: &ConnectorTableIdentity,
    target_ref: &ConnectorWriteTargetRef,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DML_EXTERNAL_FENCE_RESOURCE_DOMAIN);
    for component in [
        table.instance_id.as_str(),
        table.namespace.as_ref(),
        table.table.as_ref(),
        target_ref.as_str(),
    ] {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Project a typed external fence failure into the durable failure record.
///
/// A fence conflict keeps its typed classification here. It is never widened
/// into a commit-unknown outcome and never softened into an unsupported one:
/// the caller proved a superseded authority, not an ambiguous external effect.
pub fn external_fence_failure_record(
    failure: ConnectorExternalFenceFailure,
    message: &str,
) -> ConnectorWriteFailureRecord {
    let kind = match failure {
        ConnectorExternalFenceFailure::Stale
        | ConnectorExternalFenceFailure::Superseded
        | ConnectorExternalFenceFailure::ForeignOperation => ConnectorWriteFailureKind::Conflict,
        ConnectorExternalFenceFailure::NotEstablished => ConnectorWriteFailureKind::InvalidRequest,
    };
    ConnectorWriteFailureRecord {
        kind,
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationFinalization,
    };

    use super::*;

    fn receipt() -> ConnectorWriteReceipt {
        ConnectorWriteReceipt::try_new(Bytes::from_static(b"opaque-provider-receipt"))
            .expect("receipt")
    }

    #[test]
    fn applied_commit_persists_only_a_receipt_wire() {
        let outcome = ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt(),
            finalization: ExternalMutationFinalization::Complete,
        };
        let fact = operation_fact_from_outcome(&outcome).expect("fact");
        assert_eq!(fact.state, OperationState::Committed);
        let ConnectorWriteLifecycleRecord::KnownCommitted { receipt_wire, .. } = fact.lifecycle
        else {
            panic!("expected known committed lifecycle");
        };
        assert_eq!(receipt_wire.try_decode().expect("wire"), receipt());
    }

    #[test]
    fn no_op_commit_is_known_empty_without_a_provider_projection() {
        let outcome = ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt: receipt(),
            finalization: ExternalMutationFinalization::Complete,
        };
        let fact = operation_fact_from_outcome(&outcome).expect("fact");
        assert_eq!(fact.state, OperationState::Committed);
        assert_eq!(fact.lifecycle, ConnectorWriteLifecycleRecord::KnownEmpty);
    }

    #[test]
    fn external_fence_failures_keep_a_typed_conflict_classification() {
        for failure in [
            ConnectorExternalFenceFailure::Stale,
            ConnectorExternalFenceFailure::Superseded,
            ConnectorExternalFenceFailure::ForeignOperation,
        ] {
            let record = external_fence_failure_record(failure, "superseded authority");
            assert_eq!(record.kind, ConnectorWriteFailureKind::Conflict);
        }
        assert_eq!(
            external_fence_failure_record(
                ConnectorExternalFenceFailure::NotEstablished,
                "no fence"
            )
            .kind,
            ConnectorWriteFailureKind::InvalidRequest
        );
    }

    #[test]
    fn finalization_failure_keeps_the_known_committed_receipt() {
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Internal,
            "cache invalidation failed",
        );
        let fact = operation_fact_from_finalize_failure(&receipt(), &failure).expect("fact");
        assert_eq!(fact.state, OperationState::FinalizeFailedKnownCommitted);
        assert!(matches!(
            fact.lifecycle,
            ConnectorWriteLifecycleRecord::KnownCommitted {
                finalization: ConnectorWriteFinalizationRecord::Failed(_),
                ..
            }
        ));
    }
}
