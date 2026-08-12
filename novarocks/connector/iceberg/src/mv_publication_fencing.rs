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

//! Iceberg implementation of the provider-neutral MV publication fencing
//! capability.
//!
//! This adapter is the only place that translates between the SPI vocabulary
//! (stable resource, fence generation, permit, opaque evidence) and the Iceberg
//! commit kernels in [`crate::commit::mv_publication_fence`] and
//! [`crate::commit::mv_refresh_ref`]. It is installed as an optional FE-only
//! facet on the Iceberg control binding and is deliberately absent from BE
//! execution bindings: establishing a fence and publishing under it are
//! control-plane external mutations.
//!
//! The unknown-outcome contract is the part worth reading carefully. A lost
//! reply is never retried under a fresh operation ID. Instead the adapter
//! returns bounded provider evidence naming the same operation, and
//! [`ConnectorMvPublicationFencing::inspect`] later resolves it from lake truth
//! — reporting `Unresolved` whenever the lake cannot prove an answer, rather
//! than guessing from timestamps.

use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorInstanceIncarnation, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMvPublicationDisposition, ConnectorMvPublicationFenceGeneration,
    ConnectorMvPublicationFenceReceipt, ConnectorMvPublicationFenceRequest,
    ConnectorMvPublicationFencing, ConnectorMvPublicationInspectRequest,
    ConnectorMvPublicationInspection, ConnectorMvPublicationOperation,
    ConnectorMvPublicationTargetObservation, ConnectorMvPublicationTargetRequest,
    ConnectorMvRefreshPublicationReceipt, ConnectorMvRefreshPublicationRequest,
    ConnectorMvRefreshResourceIdentity, ESTABLISH_MV_PUBLICATION_FENCE_KIND,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, PUBLISH_MV_REFRESH_KIND,
};
use serde::{Deserialize, Serialize};

use crate::commit::{
    MvProvenanceV2, MvPublicationError, MvPublicationFencePlan, MvPublicationOperationStatus,
    MvRefreshPublishV2Plan, classify_fence_operation, establish_publication_fence, observe_fence,
    publish_staging_branch_to_main_v2,
};
use crate::control_provider::{IcebergControlProvider, staged_publication_target_ancestors};

pub const ICEBERG_MV_PUBLICATION_EVIDENCE_VERSION: u16 = 1;

/// Bounded, provider-private reconciliation evidence.
///
/// It records exactly what `inspect` needs to re-answer the question from lake
/// truth: which table, which stable resource, which generation, which operation,
/// and — for a publication — which staged snapshot under which permit. It holds
/// no credential and no raw CP-1 token.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IcebergMvPublicationEvidenceV1 {
    pub version: u16,
    pub namespace: String,
    pub table: String,
    pub table_uuid: String,
    pub operation: IcebergMvPublicationOperationV1,
    pub resource_digest: [u8; 32],
    pub cluster_digest: [u8; 32],
    pub control_plane_incarnation: u64,
    pub resource_epoch: u64,
    pub token_digest: [u8; 32],
    pub operation_id: [u8; 16],
    #[serde(default)]
    pub permit_digest: Option<[u8; 32]>,
    #[serde(default)]
    pub staged_snapshot_id: Option<i64>,
    #[serde(default)]
    pub staging_branch: Option<String>,
    #[serde(default)]
    pub expected_main_snapshot_id: Option<i64>,
    #[serde(default)]
    pub expected_fence_snapshot_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IcebergMvPublicationOperationV1 {
    EstablishFence,
    Publish,
}

impl IcebergMvPublicationOperationV1 {
    const fn spi(self) -> ConnectorMvPublicationOperation {
        match self {
            Self::EstablishFence => ConnectorMvPublicationOperation::EstablishFence,
            Self::Publish => ConnectorMvPublicationOperation::Publish,
        }
    }

    const fn evidence_kind(self) -> &'static str {
        match self {
            Self::EstablishFence => ESTABLISH_MV_PUBLICATION_FENCE_KIND,
            Self::Publish => PUBLISH_MV_REFRESH_KIND,
        }
    }
}

pub fn encode_mv_publication_evidence(
    value: &IcebergMvPublicationEvidenceV1,
) -> Result<Bytes, String> {
    if value.version != ICEBERG_MV_PUBLICATION_EVIDENCE_VERSION {
        return Err(format!(
            "unsupported Iceberg MV publication evidence version: {}",
            value.version
        ));
    }
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| format!("encode Iceberg MV publication evidence: {error}"))
}

pub fn decode_mv_publication_evidence(
    payload: &[u8],
) -> Result<IcebergMvPublicationEvidenceV1, String> {
    let value: IcebergMvPublicationEvidenceV1 = serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg MV publication evidence: {error}"))?;
    if value.version != ICEBERG_MV_PUBLICATION_EVIDENCE_VERSION {
        return Err(format!(
            "unsupported Iceberg MV publication evidence version: {}",
            value.version
        ));
    }
    Ok(value)
}

/// Opaque committed-version payload for an MV fence or publication pointer.
fn committed_version(kind: &str, snapshot_id: i64) -> Result<ConnectorCommittedVersion, String> {
    ConnectorCommittedVersion::try_new(
        Bytes::from(format!("iceberg/mv-{kind}/v1/{snapshot_id}")),
        Some(snapshot_id),
    )
    .map_err(|error| format!("build Iceberg MV {kind} version: {error}"))
}

pub struct IcebergMvPublicationFencing {
    provider: Arc<IcebergControlProvider>,
}

impl IcebergMvPublicationFencing {
    pub fn new(provider: Arc<IcebergControlProvider>) -> Self {
        Self { provider }
    }

    fn table_location(
        &self,
        table: &novarocks_spi::connector::ConnectorTableHandle,
    ) -> Result<(String, String), ConnectorError> {
        let payload = self.provider.table_payload(table)?;
        Ok((payload.namespace, payload.table))
    }

    /// Loads the table with its cache invalidated.
    ///
    /// Fencing decisions must never be made against a stale cached metadata
    /// pointer: a takeover by another frontend is precisely the change a cache
    /// would hide.
    fn load_fresh(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<crate::loaded_table::IcebergPhysicalTable, ConnectorError> {
        self.provider
            .runtime()
            .control_state()
            .invalidate_table_cache(namespace, table);
        self.provider
            .runtime()
            .load_table(namespace, table)
            .map_err(unavailable)
    }

    fn resource_for(
        &self,
        metadata: &crate::iceberg::spec::TableMetadata,
    ) -> Result<ConnectorMvRefreshResourceIdentity, ConnectorError> {
        ConnectorMvRefreshResourceIdentity::try_new(
            self.provider.descriptor().provider_id.clone(),
            metadata.uuid(),
        )
    }

    fn evidence(
        &self,
        payload: IcebergMvPublicationEvidenceV1,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let kind = payload.operation.evidence_kind();
        let operation_id = novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
            payload.operation_id,
        );
        let encoded = encode_mv_publication_evidence(&payload).map_err(corrupt)?;
        ExternalMutationEvidence::try_new(
            ICEBERG_MV_PUBLICATION_EVIDENCE_VERSION,
            self.provider.descriptor().clone(),
            self.provider.incarnation(),
            operation_id,
            kind,
            encoded,
        )
    }

    fn run<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, MvPublicationError>> + Send + 'static,
    ) -> Result<Result<T, MvPublicationError>, ConnectorError>
    where
        T: Send + 'static,
    {
        self.provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(future)
            .map_err(unavailable)
    }
}

impl ConnectorMvPublicationFencing for IcebergMvPublicationFencing {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.provider.descriptor()
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.provider.incarnation()
    }

    fn observe_target(
        &self,
        request: ConnectorMvPublicationTargetRequest,
    ) -> Result<ConnectorMvPublicationTargetObservation, ConnectorError> {
        self.provider.validate_context(&request.context)?;
        let (namespace, table) = self.table_location(&request.table)?;
        let loaded = self.load_fresh(&namespace, &table)?;
        let metadata = loaded.table.metadata();

        let resource = self.resource_for(metadata)?;
        let current_visible_version = metadata
            .current_snapshot()
            .map(|snapshot| committed_version("target", snapshot.snapshot_id()))
            .transpose()
            .map_err(corrupt)?;
        let observed = observe_fence(metadata).map_err(corrupt)?;
        let (established_generation, established_fence_version) = match observed {
            Some(fence) => {
                let generation = fence.marker.generation().map_err(corrupt)?;
                let version = committed_version("fence", fence.snapshot_id).map_err(corrupt)?;
                (Some(generation), Some(version))
            }
            None => (None, None),
        };

        ConnectorMvPublicationTargetObservation::try_new(
            resource,
            current_visible_version,
            established_generation,
            established_fence_version,
        )
    }

    fn establish_fence(
        &self,
        request: ConnectorMvPublicationFenceRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMvPublicationFenceReceipt>, ConnectorError> {
        self.provider.validate_context(&request.context)?;
        request.validate()?;
        let (namespace, table) = self.table_location(&request.table)?;
        let operation_id = request.operation_id.to_bytes();
        let plan = MvPublicationFencePlan {
            namespace: namespace.clone(),
            table: table.clone(),
            resource: request.resource.clone(),
            generation: request.generation.clone(),
            operation_id,
            observed_main_snapshot_id: request
                .observed_target_version
                .as_ref()
                .and_then(ConnectorCommittedVersion::snapshot_id),
            expected_fence_snapshot_id: request
                .expected_fence_version
                .as_ref()
                .and_then(ConnectorCommittedVersion::snapshot_id),
        };

        // Fencing must observe committed lake state, not a cached pointer.
        self.provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&namespace, &table);
        let catalog = Arc::clone(self.provider.runtime().catalog());
        let plan_for_task = plan.clone();
        let outcome = self.run(async move {
            establish_publication_fence(catalog.as_ref(), &plan_for_task).await
        })?;

        match outcome {
            Ok(fence) => {
                let version =
                    committed_version("fence", fence.fence_snapshot_id).map_err(corrupt)?;
                let receipt = ConnectorMvPublicationFenceReceipt::try_new(
                    request.resource,
                    request.generation,
                    version,
                )?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: if fence.established {
                        ExternalMutationEffect::Applied
                    } else {
                        ExternalMutationEffect::NoOp
                    },
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Err(MvPublicationError::Precondition(message)) => {
                Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        message,
                    ),
                })
            }
            Err(MvPublicationError::CommitUnknown(message)) => {
                let evidence = self.evidence(IcebergMvPublicationEvidenceV1 {
                    version: ICEBERG_MV_PUBLICATION_EVIDENCE_VERSION,
                    namespace,
                    table,
                    table_uuid: plan.resource.target_table_uuid().to_string(),
                    operation: IcebergMvPublicationOperationV1::EstablishFence,
                    resource_digest: plan.resource.digest(),
                    cluster_digest: plan.generation.cluster_digest(),
                    control_plane_incarnation: plan.generation.control_plane_incarnation(),
                    resource_epoch: plan.generation.resource_epoch(),
                    token_digest: plan.generation.token_digest(),
                    operation_id,
                    permit_digest: None,
                    staged_snapshot_id: None,
                    staging_branch: None,
                    expected_main_snapshot_id: plan.observed_main_snapshot_id,
                    expected_fence_snapshot_id: plan.expected_fence_snapshot_id,
                })?;
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                    evidence,
                })
            }
        }
    }

    fn publish(
        &self,
        request: ConnectorMvRefreshPublicationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMvRefreshPublicationReceipt>, ConnectorError> {
        self.provider.validate_context(&request.context)?;
        request.validate()?;
        let (namespace, table) = self.table_location(&request.table)?;
        let operation_id = request.operation_id.to_bytes();
        let staged_snapshot_id = request.staged_version.snapshot_id().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg MV publication requires a staged version naming a snapshot",
            )
        })?;
        let expected_fence_snapshot_id = request
            .permit
            .fence()
            .fence_version()
            .snapshot_id()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg MV publication requires a fence version naming a snapshot",
                )
            })?;
        let staging_branch = staging_branch_for(&request);
        let plan = MvRefreshPublishV2Plan {
            namespace: namespace.clone(),
            table: table.clone(),
            permit: request.permit.clone(),
            staging_branch: staging_branch.clone(),
            staging_snapshot_id: staged_snapshot_id,
            expected_main_snapshot_id: request
                .expected_target_version
                .as_ref()
                .and_then(ConnectorCommittedVersion::snapshot_id),
            expected_fence_snapshot_id,
        };

        self.provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&namespace, &table);
        let catalog = Arc::clone(self.provider.runtime().catalog());
        let plan_for_task = plan.clone();
        let outcome = self.run(async move {
            publish_staging_branch_to_main_v2(catalog.as_ref(), &plan_for_task).await
        })?;

        match outcome {
            Ok(published) => {
                let version = committed_version("published", published.published_snapshot_id)
                    .map_err(corrupt)?;
                let receipt =
                    ConnectorMvRefreshPublicationReceipt::try_new(&request.permit, version)?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Err(MvPublicationError::Precondition(message)) => {
                Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        message,
                    ),
                })
            }
            Err(MvPublicationError::CommitUnknown(message)) => {
                let generation = request.permit.generation();
                let evidence = self.evidence(IcebergMvPublicationEvidenceV1 {
                    version: ICEBERG_MV_PUBLICATION_EVIDENCE_VERSION,
                    namespace,
                    table,
                    table_uuid: request.permit.resource().target_table_uuid().to_string(),
                    operation: IcebergMvPublicationOperationV1::Publish,
                    resource_digest: request.permit.resource().digest(),
                    cluster_digest: generation.cluster_digest(),
                    control_plane_incarnation: generation.control_plane_incarnation(),
                    resource_epoch: generation.resource_epoch(),
                    token_digest: generation.token_digest(),
                    operation_id,
                    permit_digest: Some(request.permit.digest()),
                    staged_snapshot_id: Some(staged_snapshot_id),
                    staging_branch: Some(staging_branch),
                    expected_main_snapshot_id: plan.expected_main_snapshot_id,
                    expected_fence_snapshot_id: Some(expected_fence_snapshot_id),
                })?;
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                    evidence,
                })
            }
        }
    }

    fn inspect(
        &self,
        request: ConnectorMvPublicationInspectRequest,
    ) -> Result<ConnectorMvPublicationInspection, ConnectorError> {
        self.provider.validate_context(&request.context)?;
        let payload =
            decode_mv_publication_evidence(request.evidence.provider_payload()).map_err(corrupt)?;
        if payload.resource_digest != request.resource.digest() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg MV publication evidence names a different stable resource",
            ));
        }
        let generation = ConnectorMvPublicationFenceGeneration::try_from_digests(
            payload.cluster_digest,
            payload.control_plane_incarnation,
            payload.resource_epoch,
            payload.token_digest,
        )?;

        let loaded = self.load_fresh(&payload.namespace, &payload.table)?;
        let metadata = loaded.table.metadata();
        // An external DROP/recreate produces a new table UUID. The old
        // operation's outcome is then unknowable, not "uncommitted".
        if metadata.uuid() != request.resource.target_table_uuid() {
            return build_inspection(
                &request,
                payload.operation.spi(),
                ConnectorMvPublicationDisposition::Unresolved,
                generation,
                None,
            );
        }

        let status = match payload.operation {
            IcebergMvPublicationOperationV1::EstablishFence => {
                let observed = observe_fence(metadata).map_err(corrupt)?;
                classify_fence_operation(
                    observed.as_ref(),
                    &request.resource,
                    &generation,
                    payload.operation_id,
                )
                .map_err(corrupt)?
            }
            IcebergMvPublicationOperationV1::Publish => {
                classify_publication(metadata, &payload, &generation, &request.resource)
                    .map_err(corrupt)?
            }
        };

        let (disposition, version) = match status {
            MvPublicationOperationStatus::KnownCommitted { snapshot_id } => (
                ConnectorMvPublicationDisposition::KnownCommitted,
                Some(
                    committed_version(
                        match payload.operation {
                            IcebergMvPublicationOperationV1::EstablishFence => "fence",
                            IcebergMvPublicationOperationV1::Publish => "published",
                        },
                        snapshot_id,
                    )
                    .map_err(corrupt)?,
                ),
            ),
            MvPublicationOperationStatus::KnownUncommitted => {
                (ConnectorMvPublicationDisposition::KnownUncommitted, None)
            }
            MvPublicationOperationStatus::Unresolved => {
                (ConnectorMvPublicationDisposition::Unresolved, None)
            }
        };
        build_inspection(
            &request,
            payload.operation.spi(),
            disposition,
            generation,
            version,
        )
    }
}

fn build_inspection(
    request: &ConnectorMvPublicationInspectRequest,
    operation: ConnectorMvPublicationOperation,
    disposition: ConnectorMvPublicationDisposition,
    generation: ConnectorMvPublicationFenceGeneration,
    committed_version: Option<ConnectorCommittedVersion>,
) -> Result<ConnectorMvPublicationInspection, ConnectorError> {
    ConnectorMvPublicationInspection::try_new(
        request.evidence.operation_id(),
        operation,
        disposition,
        request.resource.clone(),
        generation,
        committed_version,
    )
}

/// Resolves whether a lost publication reply actually advanced the target.
///
/// The only affirmative witnesses are lake facts: `main` *is* the staged
/// snapshot, or the staged snapshot is an ancestor of `main` (it committed and a
/// later refresh built on it). The only negative witness is a target still
/// untouched with the staged branch and our fence both intact, which means the
/// same operation may simply be retried. Everything else — including a fence
/// taken over by a newer generation while `main` sits on an unrelated snapshot —
/// is reported as unresolved, because a full refresh can legitimately publish a
/// snapshot that is not descended from ours.
fn classify_publication(
    metadata: &crate::iceberg::spec::TableMetadata,
    payload: &IcebergMvPublicationEvidenceV1,
    generation: &ConnectorMvPublicationFenceGeneration,
    resource: &ConnectorMvRefreshResourceIdentity,
) -> Result<MvPublicationOperationStatus, String> {
    let staged_snapshot_id = payload
        .staged_snapshot_id
        .ok_or_else(|| "Iceberg MV publication evidence has no staged snapshot".to_string())?;
    let permit_digest = payload
        .permit_digest
        .ok_or_else(|| "Iceberg MV publication evidence has no permit digest".to_string())?;
    let main = metadata.current_snapshot_id();

    let staged_carries_our_permit = metadata
        .snapshot_by_id(staged_snapshot_id)
        .map(|snapshot| {
            Ok::<bool, String>(match MvProvenanceV2::from_snapshot_summary(snapshot)? {
                Some(provenance) => {
                    provenance.permit_digest
                        == crate::commit::mv_provenance::hex_encode(&permit_digest)
                }
                None => false,
            })
        })
        .transpose()?
        .unwrap_or(false);

    if main == Some(staged_snapshot_id) {
        return Ok(if staged_carries_our_permit {
            MvPublicationOperationStatus::KnownCommitted {
                snapshot_id: staged_snapshot_id,
            }
        } else {
            MvPublicationOperationStatus::Unresolved
        });
    }
    if staged_carries_our_permit
        && staged_publication_target_ancestors(metadata, main).contains(&staged_snapshot_id)
    {
        return Ok(MvPublicationOperationStatus::KnownCommitted {
            snapshot_id: staged_snapshot_id,
        });
    }

    let staging_intact = payload.staging_branch.as_ref().is_some_and(|branch| {
        metadata
            .refs()
            .get(branch)
            .is_some_and(|reference| reference.snapshot_id == staged_snapshot_id)
    });
    let fence_intact = observe_fence(metadata)?.is_some_and(|fence| {
        fence.marker.matches_resource(resource)
            && fence
                .marker
                .generation()
                .is_ok_and(|observed| &observed == generation)
    });
    if staging_intact && fence_intact && main == payload.expected_main_snapshot_id {
        return Ok(MvPublicationOperationStatus::KnownUncommitted);
    }
    Ok(MvPublicationOperationStatus::Unresolved)
}

/// The staging branch a V2 publication reads from.
///
/// V2 keeps the per-attempt staging-branch convention, but names it after the
/// attempt ID rather than the numeric refresh ID, so the branch name no longer
/// depends on state a StateStore rebuild would reassign.
fn staging_branch_for(request: &ConnectorMvRefreshPublicationRequest) -> String {
    format!(
        "mv_refresh_{}",
        crate::commit::mv_provenance::hex_encode(&request.permit.attempt().to_bytes())
    )
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence() -> IcebergMvPublicationEvidenceV1 {
        IcebergMvPublicationEvidenceV1 {
            version: ICEBERG_MV_PUBLICATION_EVIDENCE_VERSION,
            namespace: "db".to_string(),
            table: "mv".to_string(),
            table_uuid: "00000000-0000-0000-0000-000000001234".to_string(),
            operation: IcebergMvPublicationOperationV1::Publish,
            resource_digest: [1; 32],
            cluster_digest: [2; 32],
            control_plane_incarnation: 3,
            resource_epoch: 4,
            token_digest: [5; 32],
            operation_id: [6; 16],
            permit_digest: Some([7; 32]),
            staged_snapshot_id: Some(300),
            staging_branch: Some("mv_refresh_abc".to_string()),
            expected_main_snapshot_id: Some(100),
            expected_fence_snapshot_id: Some(500),
        }
    }

    #[test]
    fn evidence_round_trips_and_stays_bounded() {
        let payload = encode_mv_publication_evidence(&evidence()).unwrap();
        let decoded = decode_mv_publication_evidence(&payload).unwrap();

        assert_eq!(decoded.operation_id, [6; 16]);
        assert_eq!(decoded.staged_snapshot_id, Some(300));
        assert_eq!(decoded.permit_digest, Some([7; 32]));
        assert!(
            payload.len() <= novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
            "evidence must stay within the SPI bound"
        );
    }

    #[test]
    fn evidence_rejects_unknown_version() {
        let mut value = evidence();
        value.version = 9;
        assert!(encode_mv_publication_evidence(&value).is_err());

        let raw = serde_json::to_vec(&value).unwrap();
        let err = decode_mv_publication_evidence(&raw).unwrap_err();
        assert!(err.contains("unsupported"), "{err}");
    }

    #[test]
    fn evidence_operation_kinds_map_to_the_spi_contract() {
        assert_eq!(
            IcebergMvPublicationOperationV1::EstablishFence.evidence_kind(),
            ESTABLISH_MV_PUBLICATION_FENCE_KIND
        );
        assert_eq!(
            IcebergMvPublicationOperationV1::Publish.evidence_kind(),
            PUBLISH_MV_REFRESH_KIND
        );
        assert_eq!(
            IcebergMvPublicationOperationV1::Publish.spi(),
            ConnectorMvPublicationOperation::Publish
        );
    }

    #[test]
    fn committed_version_payloads_are_distinct_per_pointer_kind() {
        let fence = committed_version("fence", 500).unwrap();
        let published = committed_version("published", 500).unwrap();

        assert_eq!(fence.snapshot_id(), Some(500));
        assert_ne!(
            fence.digest(),
            published.digest(),
            "a fence pointer and a published pointer must never compare equal"
        );
    }
}
