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
    ConnectorExternalOperationFence, ConnectorHistoricalWriteDisposition,
    ConnectorHistoricalWriteObservation, ConnectorMutationFailure, ConnectorMutationFailureKind,
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
    DmlHistoricalCleanupState, DmlHistoricalWriteDisposition, DmlHistoricalWriteResultRecord,
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

/// What one typed historical write observation means for the durable DML
/// operation.
///
/// The frontend classifies nothing itself. Each variant is a direct
/// consequence of the provider's own disposition, so a later owner cannot turn
/// an unresolved observation into a terminal statement result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoricalWriteProjection {
    /// The statement result is settled; publish this operation fact.
    Terminal(OperationFact),
    /// The provider proved the old operation never dispatched and signed a
    /// continuation. The operation stays recoverable and no terminal fact is
    /// published: the continuation is resumed through the ordinary
    /// current-generation path instead.
    Continuation,
    /// Writer output exists but never committed. The operation stays
    /// recoverable until the proof-bound guarded cleanup completes; the fact
    /// records that the external mutation is known uncommitted.
    CleanupRequired(OperationFact),
    /// Evidence is insufficient. Publish no operation fact and keep the
    /// recovery record so a later cycle can inspect the same immutable request.
    Unresolved,
}

/// Project one typed provider historical write observation into the durable
/// result record (CP-3B spec D3).
///
/// Every payload stays opaque: proof, continuation and cleanup evidence are
/// copied as bounded bytes with their digests, never decoded.
pub fn historical_write_result_record(
    observation: &ConnectorHistoricalWriteObservation,
) -> Result<DmlHistoricalWriteResultRecord, String> {
    let disposition = historical_disposition_record(observation.disposition);
    let cleanup = if observation.cleanup_required {
        DmlHistoricalCleanupState::Pending
    } else {
        DmlHistoricalCleanupState::NotRequired
    };
    let continuation_payload = observation
        .continuation
        .as_ref()
        .map(|continuation| DmlOpaquePayload::try_new(continuation.payload().to_vec()))
        .transpose()?;
    Ok(DmlHistoricalWriteResultRecord {
        disposition,
        observation_digest: hex::encode(observation.digest()),
        // An applied observation keeps its neutral receipt in the operation
        // fact this projection also produces; duplicating it here would store
        // the same opaque bytes twice under two different bounds.
        evidence_payload: None,
        proof_payload: Some(DmlOpaquePayload::try_new(
            observation.proof.payload().to_vec(),
        )?),
        continuation_payload,
        cleanup,
        failure: historical_failure_record(observation.disposition),
        observed_at_ms: now_unix_millis(),
    })
}

/// Project one typed provider historical write observation into the operation
/// fact a later owner may publish.
pub fn historical_write_projection(
    observation: &ConnectorHistoricalWriteObservation,
) -> Result<HistoricalWriteProjection, String> {
    match observation.disposition {
        ConnectorHistoricalWriteDisposition::Applied => {
            let application = observation.application.as_ref().ok_or_else(|| {
                "applied historical write observation carries no neutral receipt".to_string()
            })?;
            Ok(HistoricalWriteProjection::Terminal(known_committed_fact(
                &application.receipt,
                &application.finalization,
            )?))
        }
        ConnectorHistoricalWriteDisposition::NotApplied => Ok(HistoricalWriteProjection::Terminal(
            historical_uncommitted_fact(observation.disposition),
        )),
        ConnectorHistoricalWriteDisposition::NotDispatched => {
            if observation.continuation.is_some() {
                Ok(HistoricalWriteProjection::Continuation)
            } else {
                Ok(HistoricalWriteProjection::Terminal(
                    historical_uncommitted_fact(observation.disposition),
                ))
            }
        }
        ConnectorHistoricalWriteDisposition::Staged => {
            Ok(HistoricalWriteProjection::CleanupRequired(
                historical_uncommitted_fact(observation.disposition),
            ))
        }
        ConnectorHistoricalWriteDisposition::Conflict => Ok(HistoricalWriteProjection::Terminal(
            historical_uncommitted_fact(observation.disposition),
        )),
        ConnectorHistoricalWriteDisposition::Ambiguous
        | ConnectorHistoricalWriteDisposition::Unsupported => {
            Ok(HistoricalWriteProjection::Unresolved)
        }
    }
}

const fn historical_disposition_record(
    disposition: ConnectorHistoricalWriteDisposition,
) -> DmlHistoricalWriteDisposition {
    match disposition {
        ConnectorHistoricalWriteDisposition::Applied => DmlHistoricalWriteDisposition::Applied,
        ConnectorHistoricalWriteDisposition::NotApplied => {
            DmlHistoricalWriteDisposition::NotApplied
        }
        ConnectorHistoricalWriteDisposition::NotDispatched => {
            DmlHistoricalWriteDisposition::NotDispatched
        }
        ConnectorHistoricalWriteDisposition::Staged => DmlHistoricalWriteDisposition::Staged,
        ConnectorHistoricalWriteDisposition::Conflict => DmlHistoricalWriteDisposition::Conflict,
        ConnectorHistoricalWriteDisposition::Ambiguous => DmlHistoricalWriteDisposition::Ambiguous,
        ConnectorHistoricalWriteDisposition::Unsupported => {
            DmlHistoricalWriteDisposition::Unsupported
        }
    }
}

fn historical_failure_record(
    disposition: ConnectorHistoricalWriteDisposition,
) -> Option<ConnectorWriteFailureRecord> {
    let (kind, message) = match disposition {
        ConnectorHistoricalWriteDisposition::Applied
        | ConnectorHistoricalWriteDisposition::NotApplied
        | ConnectorHistoricalWriteDisposition::NotDispatched
        | ConnectorHistoricalWriteDisposition::Staged => return None,
        ConnectorHistoricalWriteDisposition::Conflict => (
            ConnectorWriteFailureKind::Conflict,
            "historical write recovery observed a superseded external base or fence",
        ),
        ConnectorHistoricalWriteDisposition::Ambiguous => (
            ConnectorWriteFailureKind::Internal,
            "historical write recovery could not prove an external disposition",
        ),
        ConnectorHistoricalWriteDisposition::Unsupported => (
            ConnectorWriteFailureKind::Unsupported,
            "connector generation cannot classify a historical write operation",
        ),
    };
    Some(ConnectorWriteFailureRecord {
        kind,
        message: message.to_string(),
    })
}

fn historical_uncommitted_fact(disposition: ConnectorHistoricalWriteDisposition) -> OperationFact {
    let (kind, message) = match disposition {
        ConnectorHistoricalWriteDisposition::NotApplied => (
            ConnectorWriteFailureKind::Cancelled,
            "historical write recovery proved the operation never committed",
        ),
        ConnectorHistoricalWriteDisposition::NotDispatched => (
            ConnectorWriteFailureKind::Cancelled,
            "historical write recovery proved no writer or commit was dispatched",
        ),
        ConnectorHistoricalWriteDisposition::Staged => (
            ConnectorWriteFailureKind::Cancelled,
            "historical write recovery found staged writer output that never committed",
        ),
        _ => (
            ConnectorWriteFailureKind::Conflict,
            "historical write recovery observed a superseded external base or fence",
        ),
    };
    OperationFact {
        state: OperationState::FailedKnownUncommitted,
        lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
            failure: ConnectorWriteFailureRecord {
                kind,
                message: message.to_string(),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorClusterIdentity, ConnectorCommittedVersion, ConnectorExecutionBindingKey,
        ConnectorExternalFenceGeneration, ConnectorExternalFenceReceipt,
        ConnectorExternalOperationFence, ConnectorHistoricalWriteApplication,
        ConnectorHistoricalWriteCheckpoint, ConnectorHistoricalWriteContinuation,
        ConnectorHistoricalWriteDescriptor, ConnectorHistoricalWriteDispatchState,
        ConnectorHistoricalWriteFence, ConnectorHistoricalWriteFenceFacts,
        ConnectorHistoricalWriteIdentity, ConnectorHistoricalWriteOutcomeFacts,
        ConnectorHistoricalWritePhase, ConnectorHistoricalWriteProof, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorMutationFailure, ConnectorMutationFailureKind,
        ConnectorWriteIntent, ConnectorWriteOperationId, ExternalMutationFinalization,
    };

    use super::*;

    fn receipt() -> ConnectorWriteReceipt {
        ConnectorWriteReceipt::try_new(Bytes::from_static(b"opaque-provider-receipt"))
            .expect("receipt")
    }

    fn historical_table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice.reconcile").expect("instance id"),
            namespace: Arc::from("db"),
            table: Arc::from("target"),
        }
    }

    fn historical_operation_id() -> ConnectorWriteOperationId {
        ConnectorWriteOperationId::from_bytes([4; 16])
    }

    fn raised_fence() -> ConnectorExternalOperationFence {
        ConnectorExternalOperationFence::try_new(
            ConnectorClusterIdentity::derive("reconcile-cluster").expect("cluster identity"),
            ConnectorExternalFenceGeneration::try_new(1, 9, 3).expect("generation"),
            historical_operation_id(),
            [6; 16],
            historical_table(),
            ConnectorWriteTargetRef::main(),
        )
        .expect("raised fence")
    }

    fn descriptor(dispatched: bool) -> ConnectorHistoricalWriteDescriptor {
        let raised = raised_fence();
        let raised_receipt =
            ConnectorExternalFenceReceipt::try_new(&raised, Bytes::from_static(b"raised-marker"))
                .expect("raised receipt");
        let checkpoint = ConnectorHistoricalWriteCheckpoint {
            phase: if dispatched {
                ConnectorHistoricalWritePhase::WritersDispatched
            } else {
                ConnectorHistoricalWritePhase::Activated
            },
            state: if dispatched {
                ConnectorHistoricalWriteDispatchState::Dispatched
            } else {
                ConnectorHistoricalWriteDispatchState::NotDispatched
            },
            evidence_digest: None,
        };
        ConnectorHistoricalWriteDescriptor::try_new(
            ConnectorHistoricalWriteIdentity {
                historical_binding: ConnectorExecutionBindingKey {
                    instance_id: historical_table().instance_id,
                    incarnation: ConnectorInstanceIncarnation::from_bytes([2; 16]),
                },
                table: historical_table(),
                target_ref: ConnectorWriteTargetRef::main(),
                operation_id: historical_operation_id(),
                intent: ConnectorWriteIntent::Append,
                cohort_set_digest: [7; 32],
                aggregate_digest: Some([8; 32]),
            },
            ConnectorHistoricalWriteFenceFacts {
                historical_fence: ConnectorHistoricalWriteFence::NotEstablished,
                raised_fence: raised,
                raised_fence_receipt_digest: raised_receipt.digest(),
            },
            vec![checkpoint],
            None,
        )
        .expect("historical write descriptor")
    }

    fn observation(
        disposition: ConnectorHistoricalWriteDisposition,
        outcome: ConnectorHistoricalWriteOutcomeFacts,
        dispatched: bool,
    ) -> ConnectorHistoricalWriteObservation {
        ConnectorHistoricalWriteObservation::try_new(
            &descriptor(dispatched),
            disposition,
            outcome,
            ConnectorHistoricalWriteProof::try_new(Bytes::from_static(b"opaque-provider-proof"))
                .expect("proof"),
        )
        .expect("historical write observation")
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
    fn an_applied_historical_write_projects_the_neutral_receipt_only() {
        let application = ConnectorHistoricalWriteApplication {
            committed_version: ConnectorCommittedVersion::try_new(Bytes::from_static(b"v7"), None)
                .expect("committed version"),
            receipt: receipt(),
            finalization: ExternalMutationFinalization::Complete,
        };
        let observed = observation(
            ConnectorHistoricalWriteDisposition::Applied,
            ConnectorHistoricalWriteOutcomeFacts {
                application: Some(application),
                continuation: None,
                cleanup_required: false,
            },
            true,
        );

        let HistoricalWriteProjection::Terminal(fact) =
            historical_write_projection(&observed).expect("projection")
        else {
            panic!("an applied historical write settles the statement");
        };
        assert_eq!(fact.state, OperationState::Committed);
        let ConnectorWriteLifecycleRecord::KnownCommitted { receipt_wire, .. } = fact.lifecycle
        else {
            panic!("expected a known committed lifecycle");
        };
        assert_eq!(receipt_wire.try_decode().expect("wire"), receipt());

        let record = historical_write_result_record(&observed).expect("result record");
        assert_eq!(record.disposition, DmlHistoricalWriteDisposition::Applied);
        assert_eq!(record.cleanup, DmlHistoricalCleanupState::NotRequired);
        assert!(record.failure.is_none());
        assert!(record.continuation_payload.is_none());
        assert_eq!(record.observation_digest, hex::encode(observed.digest()));
    }

    #[test]
    fn a_not_dispatched_continuation_publishes_no_terminal_fact() {
        let continuation = ConnectorHistoricalWriteContinuation::try_new(
            &raised_fence(),
            Bytes::from_static(b"opaque-continuation"),
        )
        .expect("continuation");
        let observed = observation(
            ConnectorHistoricalWriteDisposition::NotDispatched,
            ConnectorHistoricalWriteOutcomeFacts {
                application: None,
                continuation: Some(continuation),
                cleanup_required: false,
            },
            false,
        );

        assert_eq!(
            historical_write_projection(&observed).expect("projection"),
            HistoricalWriteProjection::Continuation
        );
        let record = historical_write_result_record(&observed).expect("result record");
        assert_eq!(
            record.disposition,
            DmlHistoricalWriteDisposition::NotDispatched
        );
        assert!(
            record.continuation_payload.is_some(),
            "the bounded opaque continuation is retained without being decoded"
        );
    }

    #[test]
    fn staged_output_stays_recoverable_until_its_cleanup_completes() {
        let observed = observation(
            ConnectorHistoricalWriteDisposition::Staged,
            ConnectorHistoricalWriteOutcomeFacts {
                application: None,
                continuation: None,
                cleanup_required: true,
            },
            true,
        );

        let HistoricalWriteProjection::CleanupRequired(fact) =
            historical_write_projection(&observed).expect("projection")
        else {
            panic!("staged writer output requires a proof-bound cleanup");
        };
        assert_eq!(fact.state, OperationState::FailedKnownUncommitted);
        let record = historical_write_result_record(&observed).expect("result record");
        assert_eq!(record.cleanup, DmlHistoricalCleanupState::Pending);
    }

    #[test]
    fn unprovable_dispositions_publish_no_operation_fact() {
        for disposition in [
            ConnectorHistoricalWriteDisposition::Ambiguous,
            ConnectorHistoricalWriteDisposition::Unsupported,
        ] {
            let observed = observation(
                disposition,
                ConnectorHistoricalWriteOutcomeFacts::default(),
                true,
            );
            assert_eq!(
                historical_write_projection(&observed).expect("projection"),
                HistoricalWriteProjection::Unresolved,
                "{disposition:?} must never settle a statement"
            );
            let record = historical_write_result_record(&observed).expect("result record");
            assert!(record.failure.is_some());
            assert_eq!(record.cleanup, DmlHistoricalCleanupState::NotRequired);
        }
    }

    #[test]
    fn a_conflict_disposition_stays_typed_and_never_becomes_unknown() {
        let observed = observation(
            ConnectorHistoricalWriteDisposition::Conflict,
            ConnectorHistoricalWriteOutcomeFacts::default(),
            true,
        );
        let HistoricalWriteProjection::Terminal(fact) =
            historical_write_projection(&observed).expect("projection")
        else {
            panic!("a proven conflict settles the statement");
        };
        assert_eq!(fact.state, OperationState::FailedKnownUncommitted);
        assert!(
            !matches!(
                fact.lifecycle,
                ConnectorWriteLifecycleRecord::CommitUnknown { .. }
            ),
            "a fence or base conflict must never be widened into an unknown commit"
        );
        let record = historical_write_result_record(&observed).expect("result record");
        assert_eq!(
            record.failure.expect("typed failure").kind,
            ConnectorWriteFailureKind::Conflict
        );
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
