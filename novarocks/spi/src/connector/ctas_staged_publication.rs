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

//! Catalog-native fencing and historical recovery for CTAS publication.
//!
//! A CTAS destination does not exist before publication, so the table-local
//! external write fence cannot protect it. This contract instead names a
//! stable catalog resource: cluster, top-level CTAS operation, and destination
//! identity. The catalog must atomically compare its latest generation for
//! that resource on every stage, publish, and abort action.
//!
//! The ordinary and historical facets are intentionally separate. Ordinary
//! actions retain one exact Connector generation. Historical recovery uses the
//! current generation to inspect catalog truth and may clean up only the
//! unpublished staged identity named by a proof-bound observation. Neither
//! facet exposes provider metadata or a frontend coordination lease.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorClusterIdentity, ConnectorColumnAggregation, ConnectorColumnDefinition,
    ConnectorDataType, ConnectorDefaultValue, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExternalFenceFailure, ConnectorExternalFenceGeneration,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorPartitionTransform,
    ConnectorRequestContext, ConnectorStagedTableHandle, ConnectorStagedWritePlanningBinding,
    ConnectorStagedWritePlanningRequest, ConnectorStructField, ConnectorTableIdentity,
    ConnectorWriteOperationCompletion, ConnectorWriteOperationId, CreatePolicy,
    MAX_CONNECTOR_EXTERNAL_FENCE_IDENTITY_BYTES,
};

pub const CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION: u32 = 1;
pub const MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES: usize = 4 * 1024;
/// Every CTAS opaque envelope that can cross the durable frontend boundary
/// uses the same 4 KiB limit as `DmlOpaquePayload`.
pub const MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES: usize =
    MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES;
pub const MAX_CONNECTOR_CTAS_PUBLICATION_CHECKPOINTS: usize = 64;

const FENCE_DOMAIN: &[u8] = b"novarocks.connector-ctas-publication-fence.v1\0";
const FENCE_RECEIPT_DOMAIN: &[u8] = b"novarocks.connector-ctas-publication-fence-receipt.v1\0";
const LOCATOR_DOMAIN: &[u8] = b"novarocks.connector-ctas-staged-locator.v1\0";
const PROOF_DOMAIN: &[u8] = b"novarocks.connector-ctas-publication-proof.v1\0";
const RECEIPT_DOMAIN: &[u8] = b"novarocks.connector-ctas-publication-receipt.v1\0";
const STAGE_RESULT_DOMAIN: &[u8] = b"novarocks.connector-ctas-stage-result.v1\0";
const PUBLISH_RESULT_DOMAIN: &[u8] = b"novarocks.connector-ctas-publish-result.v1\0";
const ABORT_RESULT_DOMAIN: &[u8] = b"novarocks.connector-ctas-abort-result.v1\0";
const ADVANCE_REQUEST_DOMAIN: &[u8] = b"novarocks.connector-ctas-advance-request.v1\0";
const STAGE_REQUEST_DOMAIN: &[u8] = b"novarocks.connector-ctas-stage-request.v1\0";
const PUBLISH_REQUEST_DOMAIN: &[u8] = b"novarocks.connector-ctas-publish-request.v1\0";
const ABORT_REQUEST_DOMAIN: &[u8] = b"novarocks.connector-ctas-abort-request.v1\0";
const OBSERVATION_DOMAIN: &[u8] = b"novarocks.connector-historical-ctas-observation.v1\0";

/// Stable identity of one top-level frontend-owned CTAS saga.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCtasOperationId([u8; 16]);

impl ConnectorCtasOperationId {
    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, ConnectorError> {
        if Uuid::from_bytes(bytes).get_version_num() != 7 {
            return Err(invalid("connector CTAS operation ID must be UUIDv7"));
        }
        Ok(Self(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Stable identity of one stage, publish, or abort child action.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCtasActionId([u8; 16]);

impl ConnectorCtasActionId {
    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, ConnectorError> {
        if Uuid::from_bytes(bytes).get_version_num() != 7 {
            return Err(invalid("connector CTAS action ID must be UUIDv7"));
        }
        Ok(Self(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Dispatch-aware failure returned by the CTAS catalog protocol.
///
/// This classification is intentionally outside `ConnectorError`: callers
/// must never infer whether a catalog mutation may have happened from a
/// transport error string or retryability bit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorCtasFailure {
    KnownNotDispatched(ConnectorMutationFailure),
    PossiblyDispatched(ConnectorMutationFailure),
    CommittedResponseInvalid(ConnectorMutationFailure),
    Ambiguous(ConnectorMutationFailure),
    Conflict {
        kind: ConnectorCtasConflictKind,
        failure: ConnectorMutationFailure,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorCtasConflictKind {
    StaleFence,
    IdentityConflict,
    DigestConflict,
    AlreadyPublished,
    AlreadyAborted,
    CreatePolicyConflict,
}

impl ConnectorCtasConflictKind {
    pub const fn external_fence_failure(self) -> Option<ConnectorExternalFenceFailure> {
        match self {
            Self::StaleFence => Some(ConnectorExternalFenceFailure::Stale),
            Self::IdentityConflict => Some(ConnectorExternalFenceFailure::ForeignOperation),
            Self::DigestConflict => Some(ConnectorExternalFenceFailure::Superseded),
            Self::AlreadyPublished | Self::AlreadyAborted | Self::CreatePolicyConflict => None,
        }
    }
}

impl ConnectorCtasFailure {
    pub const fn failure(&self) -> &ConnectorMutationFailure {
        match self {
            Self::KnownNotDispatched(failure)
            | Self::PossiblyDispatched(failure)
            | Self::CommittedResponseInvalid(failure)
            | Self::Ambiguous(failure) => failure,
            Self::Conflict { failure, .. } => failure,
        }
    }

    pub const fn conflict_kind(&self) -> Option<ConnectorCtasConflictKind> {
        match self {
            Self::Conflict { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    fn known_not_dispatched(error: ConnectorError) -> Self {
        Self::KnownNotDispatched(mutation_failure(error))
    }

    fn committed_response_invalid(error: ConnectorError) -> Self {
        Self::CommittedResponseInvalid(mutation_failure(error))
    }
}

/// Versioned semantic capability advertised by an external catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorCtasStagedPublicationCapability {
    protocol_version: u32,
}

impl ConnectorCtasStagedPublicationCapability {
    pub fn try_new(protocol_version: u32) -> Result<Self, ConnectorError> {
        if protocol_version != CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "unsupported connector CTAS staged-publication protocol version: {protocol_version}"
                ),
            ));
        }
        Ok(Self { protocol_version })
    }

    pub const fn protocol_version(self) -> u32 {
        self.protocol_version
    }
}

/// Digest-sealed catalog fence for one absent CTAS destination.
///
/// This deliberately reuses only the shared cluster identity and ordered
/// generation primitives. It is not a table/ref `ConnectorExternalOperationFence`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCtasPublicationFence {
    cluster: ConnectorClusterIdentity,
    generation: ConnectorExternalFenceGeneration,
    operation_id: ConnectorCtasOperationId,
    target: ConnectorTableIdentity,
    digest: [u8; 32],
}

impl ConnectorCtasPublicationFence {
    pub fn try_new(
        cluster: ConnectorClusterIdentity,
        generation: ConnectorExternalFenceGeneration,
        operation_id: ConnectorCtasOperationId,
        target: ConnectorTableIdentity,
    ) -> Result<Self, ConnectorError> {
        validate_target(&target)?;
        let digest = fence_digest(cluster, generation, operation_id, &target);
        Ok(Self {
            cluster,
            generation,
            operation_id,
            target,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.cluster,
            self.generation,
            self.operation_id,
            self.target.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(corrupt(
                "connector CTAS publication fence digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn cluster(&self) -> ConnectorClusterIdentity {
        self.cluster
    }

    pub const fn generation(&self) -> ConnectorExternalFenceGeneration {
        self.generation
    }

    pub const fn operation_id(&self) -> ConnectorCtasOperationId {
        self.operation_id
    }

    pub fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn is_same_authority(&self, other: &Self) -> bool {
        self.cluster == other.cluster
            && self.operation_id == other.operation_id
            && self.target == other.target
    }

    pub fn compare_generation(&self, other: &Self) -> Result<Ordering, ConnectorError> {
        if !self.is_same_authority(other) {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::ForeignOperation,
                "connector CTAS publication fences describe different authorities",
            ));
        }
        Ok(self.generation.cmp(&other.generation))
    }

    /// Accept only an identical replay or a strictly higher generation.
    pub fn validate_monotonic_successor_of(
        &self,
        established: &Self,
    ) -> Result<(), ConnectorError> {
        self.validate()?;
        established.validate()?;
        match self.compare_generation(established)? {
            Ordering::Greater => Ok(()),
            Ordering::Equal if self.digest == established.digest => Ok(()),
            Ordering::Equal => Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Superseded,
                "connector CTAS publication fence reuses a generation with different contents",
            )),
            Ordering::Less => Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Stale,
                "connector CTAS publication fence generation is stale",
            )),
        }
    }
}

/// Catalog acknowledgement that the exact fence is durable.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCtasPublicationFenceReceipt {
    fence_digest: [u8; 32],
    action_id: ConnectorCtasActionId,
    input_digest: [u8; 32],
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorCtasPublicationFenceReceipt {
    pub fn try_new(
        request: &ConnectorCtasAdvanceFenceRequest,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_payload("CTAS publication fence receipt", &payload)?;
        let digest = opaque_digest(
            FENCE_RECEIPT_DOMAIN,
            &[
                &request.fence.digest(),
                &request.action_id.to_bytes(),
                &request.input_digest,
            ],
            &payload,
        );
        Ok(Self {
            fence_digest: request.fence.digest(),
            action_id: request.action_id,
            input_digest: request.input_digest,
            payload,
            digest,
        })
    }

    pub fn validate_for(
        &self,
        request: &ConnectorCtasAdvanceFenceRequest,
    ) -> Result<(), ConnectorError> {
        request.validate()?;
        validate_payload("CTAS publication fence receipt", &self.payload)?;
        if self.fence_digest != request.fence.digest()
            || self.action_id != request.action_id
            || self.input_digest != request.input_digest
        {
            return Err(foreign(
                "CTAS publication fence receipt answers another advance action",
            ));
        }
        let expected = opaque_digest(
            FENCE_RECEIPT_DOMAIN,
            &[
                &self.fence_digest,
                &self.action_id.to_bytes(),
                &self.input_digest,
            ],
            &self.payload,
        );
        if expected != self.digest {
            return Err(corrupt(
                "connector CTAS publication fence receipt digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn fence_digest(&self) -> [u8; 32] {
        self.fence_digest
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorCtasPublicationFenceReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCtasPublicationFenceReceipt")
            .field("fence_digest", &self.fence_digest)
            .field("action_id", &self.action_id)
            .field("input_digest", &self.input_digest)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// Opaque durable identity of one unpublished staged target.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCtasStagedLocator {
    issuance_owner: ConnectorExecutionBindingKey,
    issuance_fence: ConnectorCtasPublicationFence,
    stage_action_id: ConnectorCtasActionId,
    target_digest: [u8; 32],
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorCtasStagedLocator {
    pub fn try_new(
        issuance_owner: ConnectorExecutionBindingKey,
        fence: &ConnectorCtasPublicationFence,
        stage_action_id: ConnectorCtasActionId,
        target_digest: [u8; 32],
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        ensure_fence_owner(&issuance_owner, fence)?;
        require_digest("CTAS staged target", target_digest)?;
        validate_payload("CTAS staged locator", &payload)?;
        let digest = opaque_digest(
            LOCATOR_DOMAIN,
            &[
                issuance_owner.instance_id.as_str().as_bytes(),
                &issuance_owner.incarnation.to_bytes(),
                &fence.digest(),
                &fence.operation_id.to_bytes(),
                &stage_action_id.to_bytes(),
                &target_digest,
            ],
            &payload,
        );
        Ok(Self {
            issuance_owner,
            issuance_fence: fence.clone(),
            stage_action_id,
            target_digest,
            payload,
            digest,
        })
    }

    pub fn validate_for_foreground(
        &self,
        owner: &ConnectorExecutionBindingKey,
        fence: &ConnectorCtasPublicationFence,
    ) -> Result<(), ConnectorError> {
        self.validate_seal()?;
        fence.validate()?;
        if &self.issuance_owner != owner || self.issuance_fence.digest() != fence.digest() {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Stale,
                "CTAS staged locator was not issued by this exact binding and fence generation",
            ));
        }
        Ok(())
    }

    /// Historical inspection may carry an old locator only after a current,
    /// same-authority fence has superseded (or exactly replayed) its issuance
    /// generation. This never authorizes ordinary publish or abort.
    pub fn validate_for_historical(
        &self,
        current_fence: &ConnectorCtasPublicationFence,
    ) -> Result<(), ConnectorError> {
        self.validate_seal()?;
        current_fence.validate()?;
        match current_fence.compare_generation(&self.issuance_fence)? {
            Ordering::Greater | Ordering::Equal => Ok(()),
            Ordering::Less => Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Stale,
                "historical CTAS locator was issued by a newer fence generation",
            )),
        }
    }

    fn validate_seal(&self) -> Result<(), ConnectorError> {
        self.issuance_fence.validate()?;
        ensure_fence_owner(&self.issuance_owner, &self.issuance_fence)?;
        require_digest("CTAS staged target", self.target_digest)?;
        validate_payload("CTAS staged locator", &self.payload)?;
        let expected = opaque_digest(
            LOCATOR_DOMAIN,
            &[
                self.issuance_owner.instance_id.as_str().as_bytes(),
                &self.issuance_owner.incarnation.to_bytes(),
                &self.issuance_fence.digest(),
                &self.issuance_fence.operation_id.to_bytes(),
                &self.stage_action_id.to_bytes(),
                &self.target_digest,
            ],
            &self.payload,
        );
        if expected != self.digest {
            return Err(corrupt(
                "connector CTAS staged locator digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub fn issuance_owner(&self) -> &ConnectorExecutionBindingKey {
        &self.issuance_owner
    }

    pub fn issuance_fence(&self) -> &ConnectorCtasPublicationFence {
        &self.issuance_fence
    }

    pub const fn operation_id(&self) -> ConnectorCtasOperationId {
        self.issuance_fence.operation_id
    }

    pub const fn stage_action_id(&self) -> ConnectorCtasActionId {
        self.stage_action_id
    }

    pub const fn target_digest(&self) -> [u8; 32] {
        self.target_digest
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Versioned, bounded durable envelope retained by the frontend journal.
    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        self.validate_seal()?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(b"CTL1");
        encoded
            .extend_from_slice(&CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION.to_be_bytes());
        encode_binding(&mut encoded, &self.issuance_owner)?;
        encode_fence(&mut encoded, &self.issuance_fence)?;
        encoded.extend_from_slice(&self.stage_action_id.to_bytes());
        encoded.extend_from_slice(&self.target_digest);
        write_wire_bytes(&mut encoded, &self.payload)?;
        encoded.extend_from_slice(&self.digest);
        ensure_durable_wire_bound("CTAS staged locator", &encoded)?;
        Ok(Bytes::from(encoded))
    }

    pub fn try_from_wire_v1(bytes: &[u8]) -> Result<Self, ConnectorError> {
        ensure_durable_wire_bound("CTAS staged locator", bytes)?;
        let mut reader = CtasWireReader::new(bytes);
        reader.expect(b"CTL1")?;
        if reader.read_u32()? != CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION {
            return Err(corrupt("unsupported CTAS staged locator wire version"));
        }
        let issuance_owner = decode_binding(&mut reader)?;
        let issuance_fence = decode_fence(&mut reader)?;
        let stage_action_id = ConnectorCtasActionId::try_from_bytes(reader.read_array()?)?;
        let target_digest = reader.read_array()?;
        let payload = Bytes::copy_from_slice(reader.read_bytes()?);
        let wire_digest = reader.read_array()?;
        reader.finish()?;
        let locator = Self::try_new(
            issuance_owner,
            &issuance_fence,
            stage_action_id,
            target_digest,
            payload,
        )?;
        if locator.digest != wire_digest {
            return Err(corrupt("CTAS staged locator wire digest drifted"));
        }
        Ok(locator)
    }
}

impl fmt::Debug for ConnectorCtasStagedLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCtasStagedLocator")
            .field("issuance_owner", &self.issuance_owner)
            .field("issuance_fence_digest", &self.issuance_fence.digest())
            .field("operation_id", &self.issuance_fence.operation_id)
            .field("stage_action_id", &self.stage_action_id)
            .field("target_digest", &self.target_digest)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// The semantic statement sealed by one provider proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorCtasProofPurpose {
    Stage,
    PublishPublished,
    PublishNoOp,
    PublishConflict,
    AbortAborted,
    AbortAlreadyPublished,
    AbortConflict,
    HistoricalNotCreated,
    HistoricalStaged,
    HistoricalPublished,
    HistoricalNoOp,
    HistoricalAborted,
    HistoricalConflict,
    HistoricalAmbiguous,
    HistoricalUnsupported,
    HistoricalCleanup,
}

impl ConnectorCtasProofPurpose {
    fn for_publish(disposition: ConnectorCtasPublishDisposition) -> Self {
        match disposition {
            ConnectorCtasPublishDisposition::Published => Self::PublishPublished,
            ConnectorCtasPublishDisposition::NoOp => Self::PublishNoOp,
        }
    }

    fn for_abort(disposition: ConnectorCtasAbortDisposition) -> Self {
        match disposition {
            ConnectorCtasAbortDisposition::Aborted => Self::AbortAborted,
        }
    }

    fn for_historical(disposition: ConnectorHistoricalCtasDisposition) -> Self {
        match disposition {
            ConnectorHistoricalCtasDisposition::NotCreated => Self::HistoricalNotCreated,
            ConnectorHistoricalCtasDisposition::Staged => Self::HistoricalStaged,
            ConnectorHistoricalCtasDisposition::Published => Self::HistoricalPublished,
            ConnectorHistoricalCtasDisposition::NoOp => Self::HistoricalNoOp,
            ConnectorHistoricalCtasDisposition::Aborted => Self::HistoricalAborted,
            ConnectorHistoricalCtasDisposition::Conflict => Self::HistoricalConflict,
            ConnectorHistoricalCtasDisposition::Ambiguous => Self::HistoricalAmbiguous,
            ConnectorHistoricalCtasDisposition::Unsupported => Self::HistoricalUnsupported,
        }
    }
}

fn proof_purpose_matches_checkpoint(
    purpose: ConnectorCtasProofPurpose,
    action: ConnectorHistoricalCtasAction,
) -> bool {
    match action {
        ConnectorHistoricalCtasAction::AdvanceFence => false,
        ConnectorHistoricalCtasAction::Stage => purpose == ConnectorCtasProofPurpose::Stage,
        ConnectorHistoricalCtasAction::Publish => matches!(
            purpose,
            ConnectorCtasProofPurpose::PublishPublished
                | ConnectorCtasProofPurpose::PublishNoOp
                | ConnectorCtasProofPurpose::PublishConflict
        ),
        ConnectorHistoricalCtasAction::Abort => matches!(
            purpose,
            ConnectorCtasProofPurpose::AbortAborted
                | ConnectorCtasProofPurpose::AbortAlreadyPublished
                | ConnectorCtasProofPurpose::AbortConflict
        ),
    }
}

/// Bounded provider proof. The application owner persists but never decodes it.
/// The seal binds the provider generation and the complete neutral statement;
/// payload integrity alone is never cleanup authority.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCtasPublicationProof {
    issuer: ConnectorExecutionBindingKey,
    issuance_fence: ConnectorCtasPublicationFence,
    operation_id: ConnectorCtasOperationId,
    fence_digest: [u8; 32],
    purpose: ConnectorCtasProofPurpose,
    action_id: Option<ConnectorCtasActionId>,
    input_digest: [u8; 32],
    locator_digest: Option<[u8; 32]>,
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorCtasPublicationProof {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        issuer: ConnectorExecutionBindingKey,
        fence: &ConnectorCtasPublicationFence,
        purpose: ConnectorCtasProofPurpose,
        action_id: Option<ConnectorCtasActionId>,
        input_digest: [u8; 32],
        locator: Option<&ConnectorCtasStagedLocator>,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        ensure_fence_owner(&issuer, fence)?;
        require_digest("CTAS proof input", input_digest)?;
        if let Some(locator) = locator {
            locator.validate_for_historical(fence)?;
        }
        validate_payload("CTAS publication proof", &payload)?;
        let locator_digest = locator.map(ConnectorCtasStagedLocator::digest);
        let digest = proof_digest(
            &issuer,
            fence,
            purpose,
            action_id,
            input_digest,
            locator_digest,
            &payload,
        );
        Ok(Self {
            issuer,
            issuance_fence: fence.clone(),
            operation_id: fence.operation_id,
            fence_digest: fence.digest(),
            purpose,
            action_id,
            input_digest,
            locator_digest,
            payload,
            digest,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn validate_for(
        &self,
        issuer: &ConnectorExecutionBindingKey,
        fence: &ConnectorCtasPublicationFence,
        purpose: ConnectorCtasProofPurpose,
        action_id: Option<ConnectorCtasActionId>,
        input_digest: [u8; 32],
        locator: Option<&ConnectorCtasStagedLocator>,
    ) -> Result<(), ConnectorError> {
        fence.validate()?;
        ensure_fence_owner(issuer, fence)?;
        require_digest("CTAS proof input", input_digest)?;
        if let Some(locator) = locator {
            locator.validate_for_historical(fence)?;
        }
        validate_payload("CTAS publication proof", &self.payload)?;
        let locator_digest = locator.map(ConnectorCtasStagedLocator::digest);
        if &self.issuer != issuer
            || self.issuance_fence != *fence
            || self.operation_id != fence.operation_id
            || self.fence_digest != fence.digest()
            || self.purpose != purpose
            || self.action_id != action_id
            || self.input_digest != input_digest
            || self.locator_digest != locator_digest
        {
            return Err(foreign("CTAS publication proof answers another statement"));
        }
        if proof_digest(
            &self.issuer,
            fence,
            self.purpose,
            self.action_id,
            self.input_digest,
            self.locator_digest,
            &self.payload,
        ) != self.digest
        {
            return Err(corrupt(
                "connector CTAS publication proof digest does not match its statement",
            ));
        }
        Ok(())
    }

    fn validate_seal(&self) -> Result<(), ConnectorError> {
        self.issuance_fence.validate()?;
        ensure_fence_owner(&self.issuer, &self.issuance_fence)?;
        if self.operation_id != self.issuance_fence.operation_id
            || self.fence_digest != self.issuance_fence.digest()
        {
            return Err(corrupt(
                "CTAS proof issuance fence does not match its identity",
            ));
        }
        require_digest("CTAS proof fence", self.fence_digest)?;
        require_digest("CTAS proof input", self.input_digest)?;
        validate_payload("CTAS publication proof", &self.payload)?;
        if proof_digest_from_parts(
            &self.issuer,
            self.operation_id,
            self.fence_digest,
            self.purpose,
            self.action_id,
            self.input_digest,
            self.locator_digest,
            &self.payload,
        ) != self.digest
        {
            return Err(corrupt(
                "connector CTAS publication proof digest does not match its statement",
            ));
        }
        Ok(())
    }

    fn validate_as_foreground_abort_authority(
        &self,
        owner: &ConnectorExecutionBindingKey,
        fence: &ConnectorCtasPublicationFence,
        locator: &ConnectorCtasStagedLocator,
    ) -> Result<(), ConnectorError> {
        self.validate_seal()?;
        locator.validate_for_foreground(owner, fence)?;
        if &self.issuer != owner
            || self.operation_id != fence.operation_id
            || self.fence_digest != fence.digest()
            || self.purpose != ConnectorCtasProofPurpose::Stage
            || self.action_id != Some(locator.stage_action_id)
            || self.locator_digest != Some(locator.digest())
        {
            return Err(foreign(
                "CTAS abort authority is not the exact staged proof for this locator",
            ));
        }
        Ok(())
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn issuer(&self) -> &ConnectorExecutionBindingKey {
        &self.issuer
    }

    pub fn issuance_fence(&self) -> &ConnectorCtasPublicationFence {
        &self.issuance_fence
    }

    pub const fn purpose(&self) -> ConnectorCtasProofPurpose {
        self.purpose
    }

    pub const fn action_id(&self) -> Option<ConnectorCtasActionId> {
        self.action_id
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        self.validate_seal()?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(b"CTP1");
        encoded
            .extend_from_slice(&CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION.to_be_bytes());
        encode_binding(&mut encoded, &self.issuer)?;
        encode_fence(&mut encoded, &self.issuance_fence)?;
        encoded.push(proof_purpose_tag(self.purpose));
        encode_optional_array(
            &mut encoded,
            self.action_id.map(ConnectorCtasActionId::to_bytes),
        );
        encoded.extend_from_slice(&self.input_digest);
        encode_optional_array(&mut encoded, self.locator_digest);
        write_wire_bytes(&mut encoded, &self.payload)?;
        encoded.extend_from_slice(&self.digest);
        ensure_durable_wire_bound("CTAS publication proof", &encoded)?;
        Ok(Bytes::from(encoded))
    }

    pub fn try_from_wire_v1(bytes: &[u8]) -> Result<Self, ConnectorError> {
        ensure_durable_wire_bound("CTAS publication proof", bytes)?;
        let mut reader = CtasWireReader::new(bytes);
        reader.expect(b"CTP1")?;
        if reader.read_u32()? != CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION {
            return Err(corrupt("unsupported CTAS publication proof wire version"));
        }
        let issuer = decode_binding(&mut reader)?;
        let issuance_fence = decode_fence(&mut reader)?;
        let purpose = proof_purpose_from_wire(reader.read_u8()?)?;
        let action_id = reader
            .read_optional_array()?
            .map(ConnectorCtasActionId::try_from_bytes)
            .transpose()?;
        let input_digest = reader.read_array()?;
        let locator_digest = reader.read_optional_array()?;
        let payload = Bytes::copy_from_slice(reader.read_bytes()?);
        let digest = reader.read_array()?;
        reader.finish()?;
        let proof = Self {
            issuer,
            operation_id: issuance_fence.operation_id,
            fence_digest: issuance_fence.digest(),
            issuance_fence,
            purpose,
            action_id,
            input_digest,
            locator_digest,
            payload,
            digest,
        };
        proof.validate_seal()?;
        Ok(proof)
    }
}

impl fmt::Debug for ConnectorCtasPublicationProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCtasPublicationProof")
            .field("issuer", &self.issuer)
            .field("operation_id", &self.operation_id)
            .field("fence_digest", &self.fence_digest)
            .field("purpose", &self.purpose)
            .field("action_id", &self.action_id)
            .field("input_digest", &self.input_digest)
            .field("locator_digest", &self.locator_digest)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// Digest-sealed provider receipt for an ordinary CTAS action.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCtasPublicationReceipt {
    fence_digest: [u8; 32],
    action_id: ConnectorCtasActionId,
    input_digest: [u8; 32],
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorCtasPublicationReceipt {
    pub fn try_new(
        fence: &ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
        input_digest: [u8; 32],
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        require_digest("CTAS action input", input_digest)?;
        validate_payload("CTAS publication receipt", &payload)?;
        let digest = opaque_digest(
            RECEIPT_DOMAIN,
            &[&fence.digest(), &action_id.to_bytes(), &input_digest],
            &payload,
        );
        Ok(Self {
            fence_digest: fence.digest(),
            action_id,
            input_digest,
            payload,
            digest,
        })
    }

    pub fn validate_for(
        &self,
        fence: &ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
        input_digest: [u8; 32],
    ) -> Result<(), ConnectorError> {
        fence.validate()?;
        if self.fence_digest != fence.digest() || self.action_id != action_id {
            return Err(foreign("CTAS publication receipt answers another action"));
        }
        if self.input_digest != input_digest {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Superseded,
                "CTAS publication receipt input digest drifted within one action identity",
            ));
        }
        require_digest("CTAS action input", self.input_digest)?;
        validate_payload("CTAS publication receipt", &self.payload)?;
        let expected = opaque_digest(
            RECEIPT_DOMAIN,
            &[
                &self.fence_digest,
                &self.action_id.to_bytes(),
                &self.input_digest,
            ],
            &self.payload,
        );
        if expected != self.digest {
            return Err(corrupt(
                "connector CTAS publication receipt digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorCtasPublicationReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCtasPublicationReceipt")
            .field("fence_digest", &self.fence_digest)
            .field("action_id", &self.action_id)
            .field("input_digest", &self.input_digest)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorCtasAdvanceFenceRequest {
    pub fence: ConnectorCtasPublicationFence,
    pub action_id: ConnectorCtasActionId,
    pub input_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorCtasAdvanceFenceRequest {
    pub fn try_new(
        fence: ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        let input_digest = connector_ctas_advance_fence_request_digest(&fence, action_id);
        Ok(Self {
            fence,
            action_id,
            input_digest,
            context,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.fence.validate()?;
        let expected = connector_ctas_advance_fence_request_digest(&self.fence, self.action_id);
        if self.input_digest != expected {
            return Err(corrupt("CTAS advance-fence request digest drifted"));
        }
        Ok(())
    }
}

/// Provider-neutral definition of the invisible table created by `stage`.
/// Providers translate these facts to their native create request; Core never
/// constructs a provider-specific REST or metadata payload.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorCtasStagedTableDefinition {
    pub table: ConnectorTableIdentity,
    pub columns: Vec<ConnectorColumnDefinition>,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
    digest: [u8; 32],
}

impl ConnectorCtasStagedTableDefinition {
    pub fn try_new(
        table: ConnectorTableIdentity,
        columns: Vec<ConnectorColumnDefinition>,
        partitioning: Vec<ConnectorPartitionTransform>,
        properties: BTreeMap<Arc<str>, Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        validate_target(&table)?;
        if columns.is_empty() {
            return Err(invalid("CTAS staged table definition requires columns"));
        }
        let mut names = BTreeSet::new();
        for column in &columns {
            if column.name.is_empty() || !names.insert(column.name.clone()) {
                return Err(invalid(
                    "CTAS staged table columns must have unique non-empty names",
                ));
            }
        }
        if properties
            .iter()
            .any(|(key, value)| key.is_empty() || value.chars().any(char::is_control))
        {
            return Err(invalid("CTAS staged table properties are malformed"));
        }
        let digest = connector_ctas_staged_table_definition_digest(
            &table,
            &columns,
            &partitioning,
            &properties,
        );
        Ok(Self {
            table,
            columns,
            partitioning,
            properties,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.table.clone(),
            self.columns.clone(),
            self.partitioning.clone(),
            self.properties.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(corrupt("CTAS staged table definition digest drifted"));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorCtasStageRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub fence: ConnectorCtasPublicationFence,
    pub action_id: ConnectorCtasActionId,
    pub input_digest: [u8; 32],
    pub definition: ConnectorCtasStagedTableDefinition,
    pub target_digest: [u8; 32],
    pub initialization_digest: [u8; 32],
    pub create_policy: CreatePolicy,
    pub provider_payload: Bytes,
    pub context: ConnectorRequestContext,
}

impl ConnectorCtasStageRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        fence: ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
        definition: ConnectorCtasStagedTableDefinition,
        create_policy: CreatePolicy,
        provider_payload: Bytes,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        ensure_fence_owner(&owner, &fence)?;
        fence.validate()?;
        definition.validate()?;
        if definition.table != fence.target {
            return Err(foreign("CTAS staged definition names another fence target"));
        }
        let target_digest = fence.digest();
        let initialization_digest = definition.digest();
        validate_payload_allow_empty("CTAS stage provider payload", &provider_payload)?;
        let input_digest = connector_ctas_stage_request_digest(
            &owner,
            &fence,
            action_id,
            target_digest,
            initialization_digest,
            create_policy,
            &provider_payload,
        );
        Ok(Self {
            owner,
            fence,
            action_id,
            definition,
            input_digest,
            target_digest,
            initialization_digest,
            create_policy,
            provider_payload,
            context,
        })
    }

    pub fn validate_for(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if &self.owner != owner || self.fence.target.instance_id != owner.instance_id {
            return Err(foreign(
                "CTAS stage request owner does not match its capability",
            ));
        }
        self.fence.validate()?;
        self.definition.validate()?;
        if self.definition.table != self.fence.target
            || self.target_digest != self.fence.digest()
            || self.initialization_digest != self.definition.digest()
        {
            return Err(corrupt(
                "CTAS stage request definition does not match its target digests",
            ));
        }
        require_digest("CTAS staged target", self.target_digest)?;
        require_digest("CTAS initialization", self.initialization_digest)?;
        validate_payload_allow_empty("CTAS stage provider payload", &self.provider_payload)?;
        let expected = connector_ctas_stage_request_digest(
            &self.owner,
            &self.fence,
            self.action_id,
            self.target_digest,
            self.initialization_digest,
            self.create_policy,
            &self.provider_payload,
        );
        if expected != self.input_digest {
            return Err(corrupt("CTAS stage request digest drifted"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCtasStageResult {
    pub locator: ConnectorCtasStagedLocator,
    /// Process-local exact-generation writer target. This is never persisted;
    /// restart reconstruction uses `locator` through the historical facet.
    pub handle: ConnectorStagedTableHandle,
    pub receipt: ConnectorCtasPublicationReceipt,
    pub proof: ConnectorCtasPublicationProof,
    digest: [u8; 32],
}

impl ConnectorCtasStageResult {
    pub fn try_new(
        request: &ConnectorCtasStageRequest,
        locator: ConnectorCtasStagedLocator,
        handle: ConnectorStagedTableHandle,
        receipt: ConnectorCtasPublicationReceipt,
        proof: ConnectorCtasPublicationProof,
    ) -> Result<Self, ConnectorError> {
        request.validate_for(&request.owner)?;
        locator.validate_for_foreground(&request.owner, &request.fence)?;
        if locator.stage_action_id != request.action_id
            || locator.target_digest != request.target_digest
        {
            return Err(foreign("CTAS stage result answers another staged target"));
        }
        if handle.owner() != &request.owner {
            return Err(foreign(
                "CTAS staged writer handle was not issued by this exact binding generation",
            ));
        }
        if handle.operation_id().to_bytes() != request.action_id.to_bytes() {
            return Err(foreign(
                "CTAS staged writer handle answers another stage action",
            ));
        }
        receipt.validate_for(&request.fence, request.action_id, request.input_digest)?;
        proof.validate_for(
            &request.owner,
            &request.fence,
            ConnectorCtasProofPurpose::Stage,
            Some(request.action_id),
            request.input_digest,
            Some(&locator),
        )?;
        // A successful stage is journalable by construction. Providers must
        // not return an ordinary success which the frontend cannot durably
        // checkpoint within the shared 4 KiB opaque-value limit.
        locator.try_to_wire_v1()?;
        proof.try_to_wire_v1()?;
        let digest = aggregate_result_digest(
            STAGE_RESULT_DOMAIN,
            &[
                &request.fence.digest(),
                &request.action_id.to_bytes(),
                &request.input_digest,
                &request.target_digest,
                &locator.digest(),
                &handle.digest(),
                &receipt.digest(),
                &proof.digest(),
            ],
        );
        Ok(Self {
            locator,
            handle,
            receipt,
            proof,
            digest,
        })
    }

    pub fn validate_for(&self, request: &ConnectorCtasStageRequest) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            request,
            self.locator.clone(),
            self.handle.clone(),
            self.receipt.clone(),
            self.proof.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(corrupt(
                "connector CTAS stage result digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorCtasPublishRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub fence: ConnectorCtasPublicationFence,
    pub action_id: ConnectorCtasActionId,
    pub input_digest: [u8; 32],
    pub locator: ConnectorCtasStagedLocator,
    pub write_completion_digest: [u8; 32],
    pub create_policy: CreatePolicy,
    pub context: ConnectorRequestContext,
}

impl ConnectorCtasPublishRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        fence: ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
        locator: ConnectorCtasStagedLocator,
        write_completion_digest: [u8; 32],
        create_policy: CreatePolicy,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        ensure_fence_owner(&owner, &fence)?;
        locator.validate_for_foreground(&owner, &fence)?;
        require_digest("CTAS write completion", write_completion_digest)?;
        let input_digest = connector_ctas_publish_request_digest(
            &owner,
            &fence,
            action_id,
            &locator,
            write_completion_digest,
            create_policy,
        );
        Ok(Self {
            owner,
            fence,
            action_id,
            input_digest,
            locator,
            write_completion_digest,
            create_policy,
            context,
        })
    }

    pub fn validate_for(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if &self.owner != owner || self.fence.target.instance_id != owner.instance_id {
            return Err(foreign(
                "CTAS publish request owner does not match its capability",
            ));
        }
        self.fence.validate()?;
        self.locator.validate_for_foreground(owner, &self.fence)?;
        require_digest("CTAS write completion", self.write_completion_digest)?;
        let expected = connector_ctas_publish_request_digest(
            &self.owner,
            &self.fence,
            self.action_id,
            &self.locator,
            self.write_completion_digest,
            self.create_policy,
        );
        if expected != self.input_digest {
            return Err(corrupt("CTAS publish request digest drifted"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorCtasPublishDisposition {
    Published,
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCtasPublishResult {
    pub disposition: ConnectorCtasPublishDisposition,
    pub receipt: ConnectorCtasPublicationReceipt,
    pub proof: ConnectorCtasPublicationProof,
    digest: [u8; 32],
}

impl ConnectorCtasPublishResult {
    pub fn try_new(
        request: &ConnectorCtasPublishRequest,
        disposition: ConnectorCtasPublishDisposition,
        receipt: ConnectorCtasPublicationReceipt,
        proof: ConnectorCtasPublicationProof,
    ) -> Result<Self, ConnectorError> {
        request.validate_for(&request.owner)?;
        receipt.validate_for(&request.fence, request.action_id, request.input_digest)?;
        proof.validate_for(
            &request.owner,
            &request.fence,
            ConnectorCtasProofPurpose::for_publish(disposition),
            Some(request.action_id),
            request.input_digest,
            Some(&request.locator),
        )?;
        proof.try_to_wire_v1()?;
        let digest = aggregate_result_digest(
            PUBLISH_RESULT_DOMAIN,
            &[
                &request.fence.digest(),
                &request.action_id.to_bytes(),
                &request.input_digest,
                &[publish_disposition_tag(disposition)],
                &receipt.digest(),
                &proof.digest(),
            ],
        );
        Ok(Self {
            disposition,
            receipt,
            proof,
            digest,
        })
    }

    pub fn validate_for(
        &self,
        request: &ConnectorCtasPublishRequest,
    ) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            request,
            self.disposition,
            self.receipt.clone(),
            self.proof.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(corrupt(
                "connector CTAS publish result digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorCtasAbortRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub fence: ConnectorCtasPublicationFence,
    pub action_id: ConnectorCtasActionId,
    pub input_digest: [u8; 32],
    pub locator: ConnectorCtasStagedLocator,
    pub proof: ConnectorCtasPublicationProof,
    pub context: ConnectorRequestContext,
}

impl ConnectorCtasAbortRequest {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        fence: ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
        locator: ConnectorCtasStagedLocator,
        proof: ConnectorCtasPublicationProof,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        ensure_fence_owner(&owner, &fence)?;
        locator.validate_for_foreground(&owner, &fence)?;
        proof.validate_as_foreground_abort_authority(&owner, &fence, &locator)?;
        let input_digest =
            connector_ctas_abort_request_digest(&owner, &fence, action_id, &locator, &proof);
        Ok(Self {
            owner,
            fence,
            action_id,
            input_digest,
            locator,
            proof,
            context,
        })
    }

    pub fn validate_for(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if &self.owner != owner || self.fence.target.instance_id != owner.instance_id {
            return Err(foreign(
                "CTAS abort request owner does not match its capability",
            ));
        }
        self.fence.validate()?;
        self.locator.validate_for_foreground(owner, &self.fence)?;
        self.proof
            .validate_as_foreground_abort_authority(owner, &self.fence, &self.locator)?;
        let expected = connector_ctas_abort_request_digest(
            &self.owner,
            &self.fence,
            self.action_id,
            &self.locator,
            &self.proof,
        );
        if expected != self.input_digest {
            return Err(corrupt("CTAS abort request digest drifted"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorCtasAbortDisposition {
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCtasAbortResult {
    pub disposition: ConnectorCtasAbortDisposition,
    pub receipt: ConnectorCtasPublicationReceipt,
    pub proof: ConnectorCtasPublicationProof,
    digest: [u8; 32],
}

impl ConnectorCtasAbortResult {
    pub fn try_new(
        request: &ConnectorCtasAbortRequest,
        disposition: ConnectorCtasAbortDisposition,
        receipt: ConnectorCtasPublicationReceipt,
        proof: ConnectorCtasPublicationProof,
    ) -> Result<Self, ConnectorError> {
        request.validate_for(&request.owner)?;
        receipt.validate_for(&request.fence, request.action_id, request.input_digest)?;
        proof.validate_for(
            &request.owner,
            &request.fence,
            ConnectorCtasProofPurpose::for_abort(disposition),
            Some(request.action_id),
            request.input_digest,
            Some(&request.locator),
        )?;
        proof.try_to_wire_v1()?;
        let digest = aggregate_result_digest(
            ABORT_RESULT_DOMAIN,
            &[
                &request.fence.digest(),
                &request.action_id.to_bytes(),
                &request.input_digest,
                &[abort_disposition_tag(disposition)],
                &receipt.digest(),
                &proof.digest(),
            ],
        );
        Ok(Self {
            disposition,
            receipt,
            proof,
            digest,
        })
    }

    pub fn validate_for(&self, request: &ConnectorCtasAbortRequest) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            request,
            self.disposition,
            self.receipt.clone(),
            self.proof.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(corrupt(
                "connector CTAS abort result digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Frontend journal checkpoint passed as a value fact to historical recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalCtasCheckpoint {
    pub action_id: ConnectorCtasActionId,
    pub action: ConnectorHistoricalCtasAction,
    pub dispatch: ConnectorHistoricalCtasDispatchState,
    pub input_digest: [u8; 32],
    pub evidence_digest: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalCtasAction {
    AdvanceFence,
    Stage,
    Publish,
    Abort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalCtasDispatchState {
    NotDispatched,
    Dispatched,
    Completed,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalCtasDescriptor {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub fence: ConnectorCtasPublicationFence,
    pub fence_receipt_digest: [u8; 32],
    pub target_digest: [u8; 32],
    pub create_policy: CreatePolicy,
    pub locator: Option<ConnectorCtasStagedLocator>,
    pub checkpoints: Vec<ConnectorHistoricalCtasCheckpoint>,
    pub evidence: Option<ConnectorCtasPublicationProof>,
    digest: [u8; 32],
}

impl ConnectorHistoricalCtasDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        historical_binding: ConnectorExecutionBindingKey,
        fence: ConnectorCtasPublicationFence,
        fence_receipt_digest: [u8; 32],
        target_digest: [u8; 32],
        create_policy: CreatePolicy,
        locator: Option<ConnectorCtasStagedLocator>,
        checkpoints: Vec<ConnectorHistoricalCtasCheckpoint>,
        evidence: Option<ConnectorCtasPublicationProof>,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        require_digest("historical CTAS fence receipt", fence_receipt_digest)?;
        require_digest("historical CTAS target", target_digest)?;
        if checkpoints.is_empty() || checkpoints.len() > MAX_CONNECTOR_CTAS_PUBLICATION_CHECKPOINTS
        {
            return Err(invalid(
                "historical CTAS descriptor must carry 1..=64 checkpoints",
            ));
        }
        for checkpoint in &checkpoints {
            require_digest("historical CTAS checkpoint input", checkpoint.input_digest)?;
        }
        if let Some(locator) = &locator {
            locator.validate_for_historical(&fence)?;
            if locator.issuance_owner != historical_binding {
                return Err(foreign(
                    "historical CTAS locator was not issued by the historical binding",
                ));
            }
            if locator.target_digest != target_digest {
                return Err(foreign(
                    "historical CTAS locator names another staged target",
                ));
            }
        }
        if let Some(evidence) = &evidence {
            evidence.validate_seal()?;
            if evidence.issuer != historical_binding
                || evidence.operation_id != fence.operation_id
                || fence.compare_generation(&evidence.issuance_fence)? == Ordering::Less
                || evidence.locator_digest
                    != locator.as_ref().map(ConnectorCtasStagedLocator::digest)
            {
                return Err(foreign(
                    "historical CTAS evidence does not belong to this operation and locator",
                ));
            }
            let Some(action_id) = evidence.action_id else {
                return Err(foreign(
                    "historical CTAS descriptor evidence must name one checkpoint action",
                ));
            };
            let checkpoint_matches = checkpoints.iter().any(|checkpoint| {
                checkpoint.action_id == action_id
                    && checkpoint.input_digest == evidence.input_digest
                    && checkpoint.evidence_digest == Some(evidence.digest)
                    && proof_purpose_matches_checkpoint(evidence.purpose, checkpoint.action)
            });
            if !checkpoint_matches {
                return Err(foreign(
                    "historical CTAS evidence is not bound to an exact durable checkpoint",
                ));
            }
        }
        let digest = connector_historical_ctas_descriptor_digest(
            &historical_binding,
            &fence,
            fence_receipt_digest,
            target_digest,
            create_policy,
            locator.as_ref(),
            &checkpoints,
            evidence.as_ref(),
        );
        Ok(Self {
            historical_binding,
            fence,
            fence_receipt_digest,
            target_digest,
            create_policy,
            locator,
            checkpoints,
            evidence,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.historical_binding.clone(),
            self.fence.clone(),
            self.fence_receipt_digest,
            self.target_digest,
            self.create_policy,
            self.locator.clone(),
            self.checkpoints.clone(),
            self.evidence.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(corrupt(
                "historical CTAS descriptor digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalCtasDisposition {
    NotCreated,
    Staged,
    Published,
    NoOp,
    Aborted,
    Conflict,
    Ambiguous,
    Unsupported,
}

impl ConnectorHistoricalCtasDisposition {
    pub const fn is_resolved(self) -> bool {
        !matches!(self, Self::Ambiguous | Self::Unsupported)
    }

    pub const fn may_cleanup(self) -> bool {
        matches!(self, Self::Staged | Self::NoOp)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalCtasObservation {
    pub inspection_binding: ConnectorExecutionBindingKey,
    pub disposition: ConnectorHistoricalCtasDisposition,
    pub operation_id: ConnectorCtasOperationId,
    pub descriptor_digest: [u8; 32],
    pub fence_digest: [u8; 32],
    pub locator: Option<ConnectorCtasStagedLocator>,
    pub proof: Option<ConnectorCtasPublicationProof>,
    pub conflict_kind: Option<ConnectorCtasConflictKind>,
    pub failure: Option<ConnectorMutationFailure>,
    digest: [u8; 32],
}

impl ConnectorHistoricalCtasObservation {
    pub fn try_new(
        inspection_binding: ConnectorExecutionBindingKey,
        descriptor: &ConnectorHistoricalCtasDescriptor,
        disposition: ConnectorHistoricalCtasDisposition,
        locator: Option<ConnectorCtasStagedLocator>,
        proof: Option<ConnectorCtasPublicationProof>,
        conflict_kind: Option<ConnectorCtasConflictKind>,
        failure: Option<ConnectorMutationFailure>,
    ) -> Result<Self, ConnectorError> {
        descriptor.validate()?;
        ensure_fence_owner(&inspection_binding, &descriptor.fence)?;
        if matches!(disposition, ConnectorHistoricalCtasDisposition::Staged) && locator.is_none() {
            return Err(invalid(
                "a staged CTAS observation must carry its durable locator",
            ));
        }
        if matches!(
            disposition,
            ConnectorHistoricalCtasDisposition::NotCreated
                | ConnectorHistoricalCtasDisposition::Aborted
                | ConnectorHistoricalCtasDisposition::Published
        ) && locator.is_some()
        {
            return Err(invalid(
                "an absent or aborted CTAS observation cannot carry a staged locator",
            ));
        }
        if let Some(locator) = &locator {
            locator.validate_for_historical(&descriptor.fence)?;
            if locator.target_digest != descriptor.target_digest {
                return Err(foreign("historical CTAS observation names another target"));
            }
        }
        match disposition {
            ConnectorHistoricalCtasDisposition::Conflict => {
                if proof.is_none() || conflict_kind.is_none() || failure.is_none() {
                    return Err(invalid(
                        "a conflicting CTAS observation requires proof, typed conflict, and failure facts",
                    ));
                }
            }
            ConnectorHistoricalCtasDisposition::Ambiguous => {
                if failure.is_none() {
                    return Err(invalid(
                        "an ambiguous CTAS observation requires a typed failure",
                    ));
                }
            }
            ConnectorHistoricalCtasDisposition::Unsupported => {
                if proof.is_some() || failure.is_none() {
                    return Err(invalid(
                        "an unsupported CTAS observation requires a typed failure and no proof",
                    ));
                }
            }
            _ if proof.is_none() => {
                return Err(invalid(
                    "a conclusive CTAS observation requires provider proof",
                ));
            }
            _ => {}
        }
        if disposition != ConnectorHistoricalCtasDisposition::Conflict && conflict_kind.is_some() {
            return Err(invalid(
                "only a conflicting CTAS observation may carry a conflict kind",
            ));
        }
        if let Some(proof) = &proof {
            proof.validate_for(
                &inspection_binding,
                &descriptor.fence,
                ConnectorCtasProofPurpose::for_historical(disposition),
                None,
                descriptor.digest,
                locator.as_ref(),
            )?;
            proof.try_to_wire_v1()?;
        }
        if let Some(locator) = &locator {
            locator.try_to_wire_v1()?;
        }
        let digest = connector_historical_ctas_observation_digest(
            &inspection_binding,
            descriptor.digest,
            descriptor.fence.digest,
            descriptor.fence.operation_id,
            disposition,
            locator.as_ref(),
            proof.as_ref().map(ConnectorCtasPublicationProof::digest),
            conflict_kind,
            failure.as_ref(),
        );
        Ok(Self {
            inspection_binding,
            disposition,
            operation_id: descriptor.fence.operation_id,
            descriptor_digest: descriptor.digest,
            fence_digest: descriptor.fence.digest,
            locator,
            proof,
            conflict_kind,
            failure,
            digest,
        })
    }

    pub fn validate_for(
        &self,
        descriptor: &ConnectorHistoricalCtasDescriptor,
    ) -> Result<(), ConnectorError> {
        if self.operation_id != descriptor.fence.operation_id
            || self.descriptor_digest != descriptor.digest
            || self.fence_digest != descriptor.fence.digest
        {
            return Err(foreign(
                "historical CTAS observation answers another descriptor",
            ));
        }
        let expected = Self::try_new(
            self.inspection_binding.clone(),
            descriptor,
            self.disposition,
            self.locator.clone(),
            self.proof.clone(),
            self.conflict_kind,
            self.failure.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(corrupt(
                "historical CTAS observation digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorHistoricalCtasCleanupRequest {
    pub descriptor: ConnectorHistoricalCtasDescriptor,
    pub observation: ConnectorHistoricalCtasObservation,
    pub context: ConnectorRequestContext,
}

impl ConnectorHistoricalCtasCleanupRequest {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.descriptor.validate()?;
        self.observation.validate_for(&self.descriptor)?;
        if !self.observation.disposition.may_cleanup()
            || self.observation.locator.is_none()
            || self.observation.proof.is_none()
        {
            return Err(invalid(
                "historical CTAS cleanup requires proof-bound unpublished staging",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalCtasCleanupReceipt {
    pub descriptor_digest: [u8; 32],
    pub observation_digest: [u8; 32],
    pub locator_digest: [u8; 32],
    pub proof: ConnectorCtasPublicationProof,
    digest: [u8; 32],
}

impl ConnectorHistoricalCtasCleanupReceipt {
    pub fn try_new(
        request: &ConnectorHistoricalCtasCleanupRequest,
        proof: ConnectorCtasPublicationProof,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        proof.validate_for(
            &request.observation.inspection_binding,
            &request.descriptor.fence,
            ConnectorCtasProofPurpose::HistoricalCleanup,
            None,
            request.observation.digest,
            request.observation.locator.as_ref(),
        )?;
        proof.try_to_wire_v1()?;
        let locator_digest = request
            .observation
            .locator
            .as_ref()
            .expect("validated cleanup locator")
            .digest();
        let digest = aggregate_result_digest(
            b"novarocks.connector-historical-ctas-cleanup-receipt.v1\0",
            &[
                &request.descriptor.digest,
                &request.observation.digest,
                &locator_digest,
                &proof.digest(),
            ],
        );
        Ok(Self {
            descriptor_digest: request.descriptor.digest,
            observation_digest: request.observation.digest,
            locator_digest,
            proof,
            digest,
        })
    }

    pub fn validate_for(
        &self,
        request: &ConnectorHistoricalCtasCleanupRequest,
    ) -> Result<(), ConnectorError> {
        let expected = Self::try_new(request, self.proof.clone())?;
        if self.descriptor_digest != expected.descriptor_digest
            || self.observation_digest != expected.observation_digest
            || self.locator_digest != expected.locator_digest
            || self.digest != expected.digest
        {
            return Err(foreign(
                "historical CTAS cleanup receipt answers another cleanup authority",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Exact-generation foreground catalog capability.
pub trait ConnectorCtasStagedPublication: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ConnectorInstanceIncarnation;
    fn capability(&self) -> ConnectorCtasStagedPublicationCapability;

    fn advance_fence(
        &self,
        request: ConnectorCtasAdvanceFenceRequest,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure>;

    fn stage(
        &self,
        request: ConnectorCtasStageRequest,
    ) -> Result<ConnectorCtasStageResult, ConnectorCtasFailure>;

    fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorCtasFailure>;

    fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorCtasFailure>;

    fn publish(
        &self,
        request: ConnectorCtasPublishRequest,
    ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure>;

    fn abort(
        &self,
        request: ConnectorCtasAbortRequest,
    ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure>;
}

/// Current-generation historical facet. It never revives an ordinary handle.
pub trait ConnectorHistoricalCtasStagedPublicationRecovery: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;
    fn capability(&self) -> ConnectorCtasStagedPublicationCapability;

    fn advance_fence(
        &self,
        request: ConnectorCtasAdvanceFenceRequest,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure>;

    fn inspect(
        &self,
        descriptor: ConnectorHistoricalCtasDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure>;

    fn cleanup(
        &self,
        request: ConnectorHistoricalCtasCleanupRequest,
    ) -> Result<ConnectorHistoricalCtasCleanupReceipt, ConnectorCtasFailure>;
}

pub fn validate_ctas_staged_publication_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    capability: &dyn ConnectorCtasStagedPublication,
) -> Result<(), ConnectorError> {
    if capability.descriptor() != descriptor || capability.incarnation() != incarnation {
        return Err(invalid(
            "CTAS staged-publication capability does not match its control binding generation",
        ));
    }
    capability.capability().protocol_version();
    Ok(())
}

pub fn validate_historical_ctas_staged_publication_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    recovery: &dyn ConnectorHistoricalCtasStagedPublicationRecovery,
) -> Result<(), ConnectorError> {
    let key = recovery.binding_key();
    if key.instance_id != descriptor.instance_id || key.incarnation != incarnation {
        return Err(invalid(
            "historical CTAS staged-publication capability does not match its control binding generation",
        ));
    }
    recovery.capability().protocol_version();
    Ok(())
}

/// Lease retaining the exact control generation through foreground actions.
#[derive(Clone)]
pub struct ConnectorCtasStagedPublicationLease {
    owner: ConnectorExecutionBindingKey,
    capability: Arc<dyn ConnectorCtasStagedPublication>,
    planned_writes: Arc<Mutex<BTreeMap<[u8; 32], ConnectorWriteOperationId>>>,
    _release: Arc<CtasStagedPublicationLeaseRelease>,
}

struct CtasStagedPublicationLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorCtasStagedPublicationLease {
    pub fn new(
        owner: ConnectorExecutionBindingKey,
        capability: Arc<dyn ConnectorCtasStagedPublication>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        validate_ctas_staged_publication_owner(
            capability.descriptor(),
            owner.incarnation,
            capability.as_ref(),
        )?;
        if capability.descriptor().instance_id != owner.instance_id {
            return Err(invalid(
                "CTAS staged-publication lease owner does not match its capability",
            ));
        }
        Ok(Self {
            owner,
            capability,
            planned_writes: Arc::new(Mutex::new(BTreeMap::new())),
            _release: Arc::new(CtasStagedPublicationLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn capability(&self) -> ConnectorCtasStagedPublicationCapability {
        self.capability.capability()
    }

    pub fn advance_fence(
        &self,
        request: ConnectorCtasAdvanceFenceRequest,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
        request
            .validate()
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        ensure_fence_owner(&self.owner, &request.fence)
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        let receipt = self.capability.advance_fence(request.clone())?;
        receipt
            .validate_for(&request)
            .map_err(ConnectorCtasFailure::committed_response_invalid)?;
        Ok(receipt)
    }

    pub fn stage(
        &self,
        request: ConnectorCtasStageRequest,
    ) -> Result<ConnectorCtasStageResult, ConnectorCtasFailure> {
        request
            .validate_for(&self.owner)
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        let result = self.capability.stage(request.clone())?;
        result
            .validate_for(&request)
            .map_err(ConnectorCtasFailure::committed_response_invalid)?;
        Ok(result)
    }

    pub fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorCtasFailure> {
        validate_staged_table_handle(&request.handle)
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        if request.handle.owner() != &self.owner {
            return Err(ConnectorCtasFailure::known_not_dispatched(foreign(
                "CTAS staged writer planning handle belongs to another generation",
            )));
        }
        let binding = self.capability.plan_write(request.clone())?;
        if binding.owner() != &self.owner
            || binding.target_operation_id() != request.handle.operation_id()
            || binding.target_handle_digest() != request.handle.digest()
            || binding.operation_id() != request.operation_id
            || binding.intent() != request.intent
            || binding.input_schema().as_ref() != request.input_schema.as_ref()
        {
            return Err(ConnectorCtasFailure::committed_response_invalid(foreign(
                "CTAS staged writer planning binding does not exactly answer its request",
            )));
        }
        self.planned_writes
            .lock()
            .map_err(|_| {
                ConnectorCtasFailure::committed_response_invalid(internal(
                    "CTAS staged writer planning registry is poisoned",
                ))
            })?
            .insert(request.handle.digest(), request.operation_id);
        Ok(binding)
    }

    pub fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorCtasFailure> {
        validate_staged_table_handle(&handle)
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        validate_write_completion(&completion)
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        if handle.owner() != &self.owner || completion.owner() != &self.owner {
            return Err(ConnectorCtasFailure::known_not_dispatched(foreign(
                "CTAS staged writer handle or completion belongs to another generation",
            )));
        }
        let expected_operation = self
            .planned_writes
            .lock()
            .map_err(|_| {
                ConnectorCtasFailure::known_not_dispatched(internal(
                    "CTAS staged writer planning registry is poisoned",
                ))
            })?
            .get(&handle.digest())
            .copied();
        if expected_operation != Some(completion.sealed().operation_id()) {
            return Err(ConnectorCtasFailure::known_not_dispatched(foreign(
                "CTAS staged write completion does not answer the planned write operation",
            )));
        }
        self.capability.bind_write(handle.clone(), completion)?;
        self.planned_writes
            .lock()
            .map_err(|_| {
                ConnectorCtasFailure::committed_response_invalid(internal(
                    "CTAS staged writer planning registry is poisoned",
                ))
            })?
            .remove(&handle.digest());
        Ok(())
    }

    pub fn publish(
        &self,
        request: ConnectorCtasPublishRequest,
    ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
        request
            .validate_for(&self.owner)
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        let result = self.capability.publish(request.clone())?;
        result
            .validate_for(&request)
            .map_err(ConnectorCtasFailure::committed_response_invalid)?;
        Ok(result)
    }

    pub fn abort(
        &self,
        request: ConnectorCtasAbortRequest,
    ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
        request
            .validate_for(&self.owner)
            .map_err(ConnectorCtasFailure::known_not_dispatched)?;
        let result = self.capability.abort(request.clone())?;
        result
            .validate_for(&request)
            .map_err(ConnectorCtasFailure::committed_response_invalid)?;
        Ok(result)
    }
}

fn validate_staged_table_handle(handle: &ConnectorStagedTableHandle) -> Result<(), ConnectorError> {
    let expected = ConnectorStagedTableHandle::try_new(
        handle.owner().clone(),
        handle.operation_id(),
        handle.provider_payload().clone(),
    )?;
    if expected.digest() != handle.digest() {
        return Err(corrupt("CTAS staged table handle digest drifted"));
    }
    Ok(())
}

fn validate_write_completion(
    completion: &ConnectorWriteOperationCompletion,
) -> Result<(), ConnectorError> {
    let expected = ConnectorWriteOperationCompletion::try_new(
        completion.owner().clone(),
        completion.sealed().clone(),
        completion.cohorts().to_vec(),
    )?;
    if expected.aggregate_digest() != completion.aggregate_digest()
        || completion.sealed().operation_id() != expected.sealed().operation_id()
    {
        return Err(corrupt("CTAS staged write completion digest drifted"));
    }
    Ok(())
}

impl Drop for CtasStagedPublicationLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

fn ensure_fence_owner(
    owner: &ConnectorExecutionBindingKey,
    fence: &ConnectorCtasPublicationFence,
) -> Result<(), ConnectorError> {
    if fence.target.instance_id != owner.instance_id {
        return Err(foreign(
            "CTAS publication fence owner does not match its capability",
        ));
    }
    Ok(())
}

fn validate_target(target: &ConnectorTableIdentity) -> Result<(), ConnectorError> {
    if target.namespace.is_empty()
        || target.table.is_empty()
        || target.namespace.len() > MAX_CONNECTOR_EXTERNAL_FENCE_IDENTITY_BYTES
        || target.table.len() > MAX_CONNECTOR_EXTERNAL_FENCE_IDENTITY_BYTES
        || target.namespace.chars().any(char::is_control)
        || target.table.chars().any(char::is_control)
    {
        return Err(invalid(
            "connector CTAS target namespace and table must be non-empty non-control text",
        ));
    }
    Ok(())
}

fn validate_payload(name: &str, payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.is_empty() || payload.len() > MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES {
        return Err(invalid(format!(
            "{name} must contain 1..={MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_payload_allow_empty(name: &str, payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES {
        return Err(invalid(format!(
            "{name} exceeds {MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

fn require_digest(name: &str, digest: [u8; 32]) -> Result<(), ConnectorError> {
    if digest == [0; 32] {
        return Err(invalid(format!("{name} digest must be set")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

fn foreign(message: impl Into<String>) -> ConnectorError {
    ConnectorError::external_fence(ConnectorExternalFenceFailure::ForeignOperation, message)
}

fn mutation_failure(error: ConnectorError) -> ConnectorMutationFailure {
    let kind = if error.external_fence_failure().is_some() {
        ConnectorMutationFailureKind::Conflict
    } else {
        match error.kind() {
            ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
            ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
            ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
            ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
            ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
            ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
            ConnectorErrorKind::ResourceExhausted => {
                ConnectorMutationFailureKind::ResourceExhausted
            }
            ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
            ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
            ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
        }
    };
    ConnectorMutationFailure::new(kind, error.message())
}

fn fence_digest(
    cluster: ConnectorClusterIdentity,
    generation: ConnectorExternalFenceGeneration,
    operation_id: ConnectorCtasOperationId,
    target: &ConnectorTableIdentity,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(FENCE_DOMAIN);
    hasher.update(cluster.digest());
    hasher.update(generation.to_bytes());
    hasher.update(operation_id.to_bytes());
    hash_bytes(&mut hasher, target.instance_id.as_str().as_bytes());
    hash_bytes(&mut hasher, target.namespace.as_bytes());
    hash_bytes(&mut hasher, target.table.as_bytes());
    hasher.finalize().into()
}

fn hash_optional_array(hasher: &mut Sha256, value: Option<[u8; 32]>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value);
        }
        None => hasher.update([0]),
    }
}

const fn create_policy_tag(value: CreatePolicy) -> u8 {
    match value {
        CreatePolicy::FailIfExists => 0,
        CreatePolicy::NoOpIfExists => 1,
    }
}

const fn column_aggregation_tag(value: ConnectorColumnAggregation) -> u8 {
    match value {
        ConnectorColumnAggregation::Sum => 0,
        ConnectorColumnAggregation::Min => 1,
        ConnectorColumnAggregation::Max => 2,
        ConnectorColumnAggregation::Replace => 3,
        ConnectorColumnAggregation::ReplaceIfNotNull => 4,
        ConnectorColumnAggregation::BitmapUnion => 5,
        ConnectorColumnAggregation::HllUnion => 6,
    }
}

const fn proof_purpose_tag(value: ConnectorCtasProofPurpose) -> u8 {
    match value {
        ConnectorCtasProofPurpose::Stage => 0,
        ConnectorCtasProofPurpose::PublishPublished => 1,
        ConnectorCtasProofPurpose::PublishNoOp => 2,
        ConnectorCtasProofPurpose::PublishConflict => 3,
        ConnectorCtasProofPurpose::AbortAborted => 4,
        ConnectorCtasProofPurpose::AbortAlreadyPublished => 5,
        ConnectorCtasProofPurpose::AbortConflict => 6,
        ConnectorCtasProofPurpose::HistoricalNotCreated => 7,
        ConnectorCtasProofPurpose::HistoricalStaged => 8,
        ConnectorCtasProofPurpose::HistoricalPublished => 9,
        ConnectorCtasProofPurpose::HistoricalNoOp => 10,
        ConnectorCtasProofPurpose::HistoricalAborted => 11,
        ConnectorCtasProofPurpose::HistoricalConflict => 12,
        ConnectorCtasProofPurpose::HistoricalAmbiguous => 13,
        ConnectorCtasProofPurpose::HistoricalUnsupported => 14,
        ConnectorCtasProofPurpose::HistoricalCleanup => 15,
    }
}

const fn publish_disposition_tag(value: ConnectorCtasPublishDisposition) -> u8 {
    match value {
        ConnectorCtasPublishDisposition::Published => 0,
        ConnectorCtasPublishDisposition::NoOp => 1,
    }
}

const fn abort_disposition_tag(value: ConnectorCtasAbortDisposition) -> u8 {
    match value {
        ConnectorCtasAbortDisposition::Aborted => 0,
    }
}

const fn historical_action_tag(value: ConnectorHistoricalCtasAction) -> u8 {
    match value {
        ConnectorHistoricalCtasAction::AdvanceFence => 0,
        ConnectorHistoricalCtasAction::Stage => 1,
        ConnectorHistoricalCtasAction::Publish => 2,
        ConnectorHistoricalCtasAction::Abort => 3,
    }
}

const fn historical_dispatch_tag(value: ConnectorHistoricalCtasDispatchState) -> u8 {
    match value {
        ConnectorHistoricalCtasDispatchState::NotDispatched => 0,
        ConnectorHistoricalCtasDispatchState::Dispatched => 1,
        ConnectorHistoricalCtasDispatchState::Completed => 2,
        ConnectorHistoricalCtasDispatchState::Unknown => 3,
    }
}

const fn historical_disposition_tag(value: ConnectorHistoricalCtasDisposition) -> u8 {
    match value {
        ConnectorHistoricalCtasDisposition::NotCreated => 0,
        ConnectorHistoricalCtasDisposition::Staged => 1,
        ConnectorHistoricalCtasDisposition::Published => 2,
        ConnectorHistoricalCtasDisposition::NoOp => 3,
        ConnectorHistoricalCtasDisposition::Aborted => 4,
        ConnectorHistoricalCtasDisposition::Conflict => 5,
        ConnectorHistoricalCtasDisposition::Ambiguous => 6,
        ConnectorHistoricalCtasDisposition::Unsupported => 7,
    }
}

const fn ctas_conflict_kind_tag(value: ConnectorCtasConflictKind) -> u8 {
    match value {
        ConnectorCtasConflictKind::StaleFence => 0,
        ConnectorCtasConflictKind::IdentityConflict => 1,
        ConnectorCtasConflictKind::DigestConflict => 2,
        ConnectorCtasConflictKind::AlreadyPublished => 3,
        ConnectorCtasConflictKind::AlreadyAborted => 4,
        ConnectorCtasConflictKind::CreatePolicyConflict => 5,
    }
}

const fn mutation_failure_kind_tag(value: ConnectorMutationFailureKind) -> u8 {
    match value {
        ConnectorMutationFailureKind::InvalidRequest => 0,
        ConnectorMutationFailureKind::NotFound => 1,
        ConnectorMutationFailureKind::AlreadyExists => 2,
        ConnectorMutationFailureKind::Conflict => 3,
        ConnectorMutationFailureKind::Unauthenticated => 4,
        ConnectorMutationFailureKind::PermissionDenied => 5,
        ConnectorMutationFailureKind::Unsupported => 6,
        ConnectorMutationFailureKind::Cancelled => 7,
        ConnectorMutationFailureKind::DeadlineExceeded => 8,
        ConnectorMutationFailureKind::ResourceExhausted => 9,
        ConnectorMutationFailureKind::Unavailable => 10,
        ConnectorMutationFailureKind::CorruptData => 11,
        ConnectorMutationFailureKind::Internal => 12,
    }
}

fn opaque_digest(domain: &[u8], fields: &[&[u8]], payload: &Bytes) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    hasher.update((payload.len() as u64).to_be_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

fn aggregate_result_digest(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    hasher.finalize().into()
}

pub fn connector_ctas_advance_fence_request_digest(
    fence: &ConnectorCtasPublicationFence,
    action_id: ConnectorCtasActionId,
) -> [u8; 32] {
    aggregate_result_digest(
        ADVANCE_REQUEST_DOMAIN,
        &[&fence.digest(), &action_id.to_bytes()],
    )
}

pub fn connector_ctas_staged_table_definition_digest(
    table: &ConnectorTableIdentity,
    columns: &[ConnectorColumnDefinition],
    partitioning: &[ConnectorPartitionTransform],
    properties: &BTreeMap<Arc<str>, Arc<str>>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-ctas-staged-table-definition.v1\0");
    hash_bytes(&mut hasher, table.instance_id.as_str().as_bytes());
    hash_bytes(&mut hasher, table.namespace.as_bytes());
    hash_bytes(&mut hasher, table.table.as_bytes());
    hasher.update((columns.len() as u64).to_be_bytes());
    for column in columns {
        hash_column(&mut hasher, column);
    }
    hasher.update((partitioning.len() as u64).to_be_bytes());
    for transform in partitioning {
        hash_partition_transform(&mut hasher, transform);
    }
    hasher.update((properties.len() as u64).to_be_bytes());
    for (key, value) in properties {
        hash_bytes(&mut hasher, key.as_bytes());
        hash_bytes(&mut hasher, value.as_bytes());
    }
    hasher.finalize().into()
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn hash_column(hasher: &mut Sha256, column: &ConnectorColumnDefinition) {
    hash_bytes(hasher, column.name.as_bytes());
    hash_data_type(hasher, &column.data_type);
    hasher.update([u8::from(column.nullable)]);
    hasher.update([column
        .aggregation
        .map_or(0, |value| column_aggregation_tag(value) + 1)]);
    match &column.default {
        None => hasher.update([0]),
        Some(value) => {
            hasher.update([1]);
            hash_default(hasher, value);
        }
    }
}

fn hash_struct_field(hasher: &mut Sha256, field: &ConnectorStructField) {
    hash_bytes(hasher, field.name.as_bytes());
    hash_data_type(hasher, &field.data_type);
    hasher.update([u8::from(field.nullable)]);
}

fn hash_data_type(hasher: &mut Sha256, data_type: &ConnectorDataType) {
    let tag = match data_type {
        ConnectorDataType::Boolean => 0,
        ConnectorDataType::TinyInt => 1,
        ConnectorDataType::SmallInt => 2,
        ConnectorDataType::Int => 3,
        ConnectorDataType::BigInt => 4,
        ConnectorDataType::LargeInt => 5,
        ConnectorDataType::Float => 6,
        ConnectorDataType::Double => 7,
        ConnectorDataType::Decimal { .. } => 8,
        ConnectorDataType::String => 9,
        ConnectorDataType::Binary => 10,
        ConnectorDataType::Json => 11,
        ConnectorDataType::Bitmap => 12,
        ConnectorDataType::Hll => 13,
        ConnectorDataType::Date => 14,
        ConnectorDataType::DateTime => 15,
        ConnectorDataType::DateTimeNs => 16,
        ConnectorDataType::Time => 17,
        ConnectorDataType::Array(_) => 18,
        ConnectorDataType::Map(_, _) => 19,
        ConnectorDataType::Struct(_) => 20,
        ConnectorDataType::Variant => 21,
    };
    hasher.update([tag]);
    match data_type {
        ConnectorDataType::Decimal { precision, scale } => {
            hasher.update([*precision, *scale as u8]);
        }
        ConnectorDataType::Array(element) => hash_data_type(hasher, element),
        ConnectorDataType::Map(key, value) => {
            hash_data_type(hasher, key);
            hash_data_type(hasher, value);
        }
        ConnectorDataType::Struct(fields) => {
            hasher.update((fields.len() as u64).to_be_bytes());
            for field in fields {
                hash_struct_field(hasher, field);
            }
        }
        _ => {}
    }
}

fn hash_default(hasher: &mut Sha256, value: &ConnectorDefaultValue) {
    match value {
        ConnectorDefaultValue::Null => hasher.update([0]),
        ConnectorDefaultValue::Bool(value) => hasher.update([1, u8::from(*value)]),
        ConnectorDefaultValue::Int(value) => {
            hasher.update([2]);
            hasher.update(value.to_be_bytes());
        }
        ConnectorDefaultValue::Float(value) => {
            hasher.update([3]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        ConnectorDefaultValue::Decimal { unscaled, scale } => {
            hasher.update([4]);
            hasher.update(unscaled.to_be_bytes());
            hasher.update([*scale as u8]);
        }
        ConnectorDefaultValue::String(value) => {
            hasher.update([5]);
            hash_bytes(hasher, value.as_bytes());
        }
        ConnectorDefaultValue::Date(value) => {
            hasher.update([6]);
            hasher.update(value.to_be_bytes());
        }
        ConnectorDefaultValue::DateTime(value) => {
            hasher.update([7]);
            hasher.update(value.to_be_bytes());
        }
        ConnectorDefaultValue::Binary(value) => {
            hasher.update([8]);
            hash_bytes(hasher, value);
        }
    }
}

fn hash_partition_transform(hasher: &mut Sha256, value: &ConnectorPartitionTransform) {
    let (tag, column, parameter) = match value {
        ConnectorPartitionTransform::Identity { column } => (0, column, None),
        ConnectorPartitionTransform::Year { column } => (1, column, None),
        ConnectorPartitionTransform::Month { column } => (2, column, None),
        ConnectorPartitionTransform::Day { column } => (3, column, None),
        ConnectorPartitionTransform::Hour { column } => (4, column, None),
        ConnectorPartitionTransform::Bucket {
            column,
            num_buckets,
        } => (5, column, Some(*num_buckets)),
        ConnectorPartitionTransform::Truncate { column, width } => (6, column, Some(*width)),
        ConnectorPartitionTransform::Void { column } => (7, column, None),
    };
    hasher.update([tag]);
    hash_bytes(hasher, column.as_bytes());
    hasher.update(parameter.unwrap_or_default().to_be_bytes());
}

pub fn connector_ctas_stage_request_digest(
    owner: &ConnectorExecutionBindingKey,
    fence: &ConnectorCtasPublicationFence,
    action_id: ConnectorCtasActionId,
    target_digest: [u8; 32],
    initialization_digest: [u8; 32],
    create_policy: CreatePolicy,
    provider_payload: &Bytes,
) -> [u8; 32] {
    aggregate_result_digest(
        STAGE_REQUEST_DOMAIN,
        &[
            owner.instance_id.as_str().as_bytes(),
            &owner.incarnation.to_bytes(),
            &fence.digest(),
            &action_id.to_bytes(),
            &target_digest,
            &initialization_digest,
            &[create_policy_tag(create_policy)],
            &Sha256::digest(provider_payload),
        ],
    )
}

pub fn connector_ctas_publish_request_digest(
    owner: &ConnectorExecutionBindingKey,
    fence: &ConnectorCtasPublicationFence,
    action_id: ConnectorCtasActionId,
    locator: &ConnectorCtasStagedLocator,
    write_completion_digest: [u8; 32],
    create_policy: CreatePolicy,
) -> [u8; 32] {
    aggregate_result_digest(
        PUBLISH_REQUEST_DOMAIN,
        &[
            owner.instance_id.as_str().as_bytes(),
            &owner.incarnation.to_bytes(),
            &fence.digest(),
            &action_id.to_bytes(),
            &locator.digest(),
            &write_completion_digest,
            &[create_policy_tag(create_policy)],
        ],
    )
}

pub fn connector_ctas_abort_request_digest(
    owner: &ConnectorExecutionBindingKey,
    fence: &ConnectorCtasPublicationFence,
    action_id: ConnectorCtasActionId,
    locator: &ConnectorCtasStagedLocator,
    proof: &ConnectorCtasPublicationProof,
) -> [u8; 32] {
    aggregate_result_digest(
        ABORT_REQUEST_DOMAIN,
        &[
            owner.instance_id.as_str().as_bytes(),
            &owner.incarnation.to_bytes(),
            &fence.digest(),
            &action_id.to_bytes(),
            &locator.digest(),
            &proof.digest(),
        ],
    )
}

#[allow(clippy::too_many_arguments)]
fn proof_digest(
    issuer: &ConnectorExecutionBindingKey,
    fence: &ConnectorCtasPublicationFence,
    purpose: ConnectorCtasProofPurpose,
    action_id: Option<ConnectorCtasActionId>,
    input_digest: [u8; 32],
    locator_digest: Option<[u8; 32]>,
    payload: &Bytes,
) -> [u8; 32] {
    proof_digest_from_parts(
        issuer,
        fence.operation_id,
        fence.digest(),
        purpose,
        action_id,
        input_digest,
        locator_digest,
        payload,
    )
}

#[allow(clippy::too_many_arguments)]
fn proof_digest_from_parts(
    issuer: &ConnectorExecutionBindingKey,
    operation_id: ConnectorCtasOperationId,
    fence_digest: [u8; 32],
    purpose: ConnectorCtasProofPurpose,
    action_id: Option<ConnectorCtasActionId>,
    input_digest: [u8; 32],
    locator_digest: Option<[u8; 32]>,
    payload: &Bytes,
) -> [u8; 32] {
    opaque_digest(
        PROOF_DOMAIN,
        &[
            issuer.instance_id.as_str().as_bytes(),
            &issuer.incarnation.to_bytes(),
            &operation_id.to_bytes(),
            &fence_digest,
            &[proof_purpose_tag(purpose)],
            &action_id
                .map(ConnectorCtasActionId::to_bytes)
                .unwrap_or_default(),
            &input_digest,
            &locator_digest.unwrap_or_default(),
        ],
        payload,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn connector_historical_ctas_descriptor_digest(
    historical_binding: &ConnectorExecutionBindingKey,
    fence: &ConnectorCtasPublicationFence,
    fence_receipt_digest: [u8; 32],
    target_digest: [u8; 32],
    create_policy: CreatePolicy,
    locator: Option<&ConnectorCtasStagedLocator>,
    checkpoints: &[ConnectorHistoricalCtasCheckpoint],
    evidence: Option<&ConnectorCtasPublicationProof>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-historical-ctas-descriptor.v1\0");
    hash_bytes(
        &mut hasher,
        historical_binding.instance_id.as_str().as_bytes(),
    );
    hasher.update(historical_binding.incarnation.to_bytes());
    hasher.update(fence.digest);
    hasher.update(fence_receipt_digest);
    hasher.update(target_digest);
    hasher.update([create_policy_tag(create_policy)]);
    hash_optional_array(&mut hasher, locator.map(|value| value.digest));
    hasher.update((checkpoints.len() as u64).to_be_bytes());
    for checkpoint in checkpoints {
        hasher.update(checkpoint.action_id.to_bytes());
        hasher.update([
            historical_action_tag(checkpoint.action),
            historical_dispatch_tag(checkpoint.dispatch),
        ]);
        hasher.update(checkpoint.input_digest);
        hash_optional_array(&mut hasher, checkpoint.evidence_digest);
    }
    hash_optional_array(&mut hasher, evidence.map(|value| value.digest));
    hasher.finalize().into()
}

pub fn connector_historical_ctas_observation_digest(
    inspection_binding: &ConnectorExecutionBindingKey,
    descriptor_digest: [u8; 32],
    fence_digest: [u8; 32],
    operation_id: ConnectorCtasOperationId,
    disposition: ConnectorHistoricalCtasDisposition,
    locator: Option<&ConnectorCtasStagedLocator>,
    proof_digest: Option<[u8; 32]>,
    conflict_kind: Option<ConnectorCtasConflictKind>,
    failure: Option<&ConnectorMutationFailure>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(OBSERVATION_DOMAIN);
    hash_bytes(
        &mut hasher,
        inspection_binding.instance_id.as_str().as_bytes(),
    );
    hasher.update(inspection_binding.incarnation.to_bytes());
    hasher.update(descriptor_digest);
    hasher.update(fence_digest);
    hasher.update(operation_id.to_bytes());
    hasher.update([historical_disposition_tag(disposition)]);
    hash_optional_array(&mut hasher, locator.map(|value| value.digest));
    hash_optional_array(&mut hasher, proof_digest);
    match conflict_kind {
        Some(kind) => hasher.update([1, ctas_conflict_kind_tag(kind)]),
        None => hasher.update([0]),
    }
    if let Some(failure) = failure {
        hasher.update([1, mutation_failure_kind_tag(failure.kind())]);
        hash_bytes(&mut hasher, failure.message().as_bytes());
    } else {
        hasher.update([0]);
    }
    hasher.finalize().into()
}

fn ensure_durable_wire_bound(name: &str, bytes: &[u8]) -> Result<(), ConnectorError> {
    if bytes.is_empty() || bytes.len() > MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!(
                "{name} durable wire must contain 1..={MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES} bytes"
            ),
        ));
    }
    Ok(())
}

fn write_wire_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ConnectorError> {
    let len = u16::try_from(bytes.len()).map_err(|_| invalid("CTAS durable field is too large"))?;
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn encode_binding(
    encoded: &mut Vec<u8>,
    binding: &ConnectorExecutionBindingKey,
) -> Result<(), ConnectorError> {
    write_wire_bytes(encoded, binding.instance_id.as_str().as_bytes())?;
    encoded.extend_from_slice(&binding.incarnation.to_bytes());
    Ok(())
}

fn encode_fence(
    encoded: &mut Vec<u8>,
    fence: &ConnectorCtasPublicationFence,
) -> Result<(), ConnectorError> {
    fence.validate()?;
    encoded.extend_from_slice(&fence.cluster.digest());
    encoded.extend_from_slice(&fence.generation.to_bytes());
    encoded.extend_from_slice(&fence.operation_id.to_bytes());
    write_wire_bytes(encoded, fence.target.instance_id.as_str().as_bytes())?;
    write_wire_bytes(encoded, fence.target.namespace.as_bytes())?;
    write_wire_bytes(encoded, fence.target.table.as_bytes())?;
    encoded.extend_from_slice(&fence.digest);
    Ok(())
}

fn encode_optional_array<const N: usize>(encoded: &mut Vec<u8>, value: Option<[u8; N]>) {
    match value {
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(&value);
        }
        None => encoded.push(0),
    }
}

fn decode_binding(
    reader: &mut CtasWireReader<'_>,
) -> Result<ConnectorExecutionBindingKey, ConnectorError> {
    Ok(ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse(reader.read_utf8()?)?,
        incarnation: ConnectorInstanceIncarnation::from_bytes(reader.read_array()?),
    })
}

fn decode_fence(
    reader: &mut CtasWireReader<'_>,
) -> Result<ConnectorCtasPublicationFence, ConnectorError> {
    let cluster = ConnectorClusterIdentity::try_from_digest(reader.read_array()?)?;
    let generation_bytes: [u8; 24] = reader.read_array()?;
    let generation = ConnectorExternalFenceGeneration::try_new(
        u64::from_be_bytes(generation_bytes[..8].try_into().expect("fixed slice")),
        u64::from_be_bytes(generation_bytes[8..16].try_into().expect("fixed slice")),
        u64::from_be_bytes(generation_bytes[16..].try_into().expect("fixed slice")),
    )?;
    let operation_id = ConnectorCtasOperationId::try_from_bytes(reader.read_array()?)?;
    let target = ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse(reader.read_utf8()?)?,
        namespace: Arc::from(reader.read_utf8()?),
        table: Arc::from(reader.read_utf8()?),
    };
    let wire_digest = reader.read_array()?;
    let fence = ConnectorCtasPublicationFence::try_new(cluster, generation, operation_id, target)?;
    if fence.digest != wire_digest {
        return Err(corrupt("CTAS publication fence wire digest drifted"));
    }
    Ok(fence)
}

fn proof_purpose_from_wire(tag: u8) -> Result<ConnectorCtasProofPurpose, ConnectorError> {
    const PURPOSES: [ConnectorCtasProofPurpose; 16] = [
        ConnectorCtasProofPurpose::Stage,
        ConnectorCtasProofPurpose::PublishPublished,
        ConnectorCtasProofPurpose::PublishNoOp,
        ConnectorCtasProofPurpose::PublishConflict,
        ConnectorCtasProofPurpose::AbortAborted,
        ConnectorCtasProofPurpose::AbortAlreadyPublished,
        ConnectorCtasProofPurpose::AbortConflict,
        ConnectorCtasProofPurpose::HistoricalNotCreated,
        ConnectorCtasProofPurpose::HistoricalStaged,
        ConnectorCtasProofPurpose::HistoricalPublished,
        ConnectorCtasProofPurpose::HistoricalNoOp,
        ConnectorCtasProofPurpose::HistoricalAborted,
        ConnectorCtasProofPurpose::HistoricalConflict,
        ConnectorCtasProofPurpose::HistoricalAmbiguous,
        ConnectorCtasProofPurpose::HistoricalUnsupported,
        ConnectorCtasProofPurpose::HistoricalCleanup,
    ];
    PURPOSES
        .get(usize::from(tag))
        .copied()
        .ok_or_else(|| corrupt("invalid CTAS proof purpose wire tag"))
}

struct CtasWireReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CtasWireReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ConnectorError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| corrupt("CTAS wire offset overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| corrupt("truncated CTAS durable wire"))?;
        self.offset = end;
        Ok(value)
    }

    fn expect(&mut self, expected: &[u8]) -> Result<(), ConnectorError> {
        if self.take(expected.len())? != expected {
            return Err(corrupt("invalid CTAS durable wire magic"));
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, ConnectorError> {
        Ok(self.take(1)?[0])
    }
    fn read_u16(&mut self) -> Result<u16, ConnectorError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }
    fn read_u32(&mut self) -> Result<u32, ConnectorError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ConnectorError> {
        self.take(N)?
            .try_into()
            .map_err(|_| corrupt("invalid CTAS wire array"))
    }
    fn read_bytes(&mut self) -> Result<&'a [u8], ConnectorError> {
        let len = usize::from(self.read_u16()?);
        self.take(len)
    }
    fn read_utf8(&mut self) -> Result<&'a str, ConnectorError> {
        std::str::from_utf8(self.read_bytes()?).map_err(|_| corrupt("CTAS wire text is not UTF-8"))
    }
    fn read_optional_array<const N: usize>(&mut self) -> Result<Option<[u8; N]>, ConnectorError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_array().map(Some),
            _ => Err(corrupt("invalid CTAS wire option tag")),
        }
    }
    fn finish(self) -> Result<(), ConnectorError> {
        if self.offset != self.bytes.len() {
            return Err(corrupt("trailing CTAS durable wire bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;

    use super::*;
    use crate::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorTableHandle, ConnectorWriteIntent, ConnectorWriteOperationId,
    };

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .unwrap()
    }

    fn instance() -> ConnectorInstanceId {
        ConnectorInstanceId::parse("iceberg-rest").unwrap()
    }

    fn target(table: &str) -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: instance(),
            namespace: "analytics".into(),
            table: table.into(),
        }
    }

    fn generation(attempt: u64) -> ConnectorExternalFenceGeneration {
        ConnectorExternalFenceGeneration::try_new(7, 3, attempt).unwrap()
    }

    fn uuid_v7_bytes(value: u8) -> [u8; 16] {
        let mut bytes = [value; 16];
        bytes[6] = 0x70 | (value & 0x0f);
        bytes[8] = 0x80 | (value & 0x3f);
        bytes
    }

    fn fence(attempt: u64) -> ConnectorCtasPublicationFence {
        ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            generation(attempt),
            ConnectorCtasOperationId::try_from_bytes(uuid_v7_bytes(9)).unwrap(),
            target("orders"),
        )
        .unwrap()
    }

    fn action(value: u8) -> ConnectorCtasActionId {
        ConnectorCtasActionId::try_from_bytes(uuid_v7_bytes(value)).unwrap()
    }

    fn binding(incarnation: u64) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: instance(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation as u8; 16]),
        }
    }

    fn descriptor() -> ConnectorHistoricalCtasDescriptor {
        let current_fence = fence(2);
        let locator = ConnectorCtasStagedLocator::try_new(
            binding(1),
            &fence(1),
            action(2),
            [3; 32],
            Bytes::from_static(b"locator"),
        )
        .unwrap();
        ConnectorHistoricalCtasDescriptor::try_new(
            binding(1),
            current_fence,
            [4; 32],
            [3; 32],
            CreatePolicy::FailIfExists,
            Some(locator),
            vec![ConnectorHistoricalCtasCheckpoint {
                action_id: action(2),
                action: ConnectorHistoricalCtasAction::Stage,
                dispatch: ConnectorHistoricalCtasDispatchState::Unknown,
                input_digest: [5; 32],
                evidence_digest: None,
            }],
            None,
        )
        .unwrap()
    }

    fn proof(
        issuer: ConnectorExecutionBindingKey,
        fence: &ConnectorCtasPublicationFence,
        purpose: ConnectorCtasProofPurpose,
        action_id: Option<ConnectorCtasActionId>,
        input_digest: [u8; 32],
        locator: Option<&ConnectorCtasStagedLocator>,
        payload: &'static [u8],
    ) -> ConnectorCtasPublicationProof {
        ConnectorCtasPublicationProof::try_new(
            issuer,
            fence,
            purpose,
            action_id,
            input_digest,
            locator,
            Bytes::from_static(payload),
        )
        .unwrap()
    }

    #[test]
    fn fence_orders_only_one_catalog_authority() {
        let established = fence(1);
        established
            .validate_monotonic_successor_of(&established)
            .unwrap();
        fence(2)
            .validate_monotonic_successor_of(&established)
            .unwrap();

        let stale = fence(1)
            .validate_monotonic_successor_of(&fence(2))
            .unwrap_err();
        assert_eq!(
            stale.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );

        let foreign = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            generation(2),
            ConnectorCtasOperationId::try_from_bytes(uuid_v7_bytes(8)).unwrap(),
            target("orders"),
        )
        .unwrap()
        .validate_monotonic_successor_of(&established)
        .unwrap_err();
        assert_eq!(
            foreign.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::ForeignOperation)
        );
    }

    #[test]
    fn opaque_values_are_bounded_and_digest_sealed() {
        let fence = fence(1);
        assert!(
            ConnectorCtasPublicationProof::try_new(
                binding(1),
                &fence,
                ConnectorCtasProofPurpose::HistoricalNotCreated,
                None,
                [1; 32],
                None,
                Bytes::new(),
            )
            .is_err()
        );
        assert!(
            ConnectorCtasPublicationProof::try_new(
                binding(1),
                &fence,
                ConnectorCtasProofPurpose::HistoricalNotCreated,
                None,
                [1; 32],
                None,
                Bytes::from(vec![0; MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES + 1]),
            )
            .is_err()
        );

        let advance =
            ConnectorCtasAdvanceFenceRequest::try_new(fence.clone(), action(1), context()).unwrap();
        let receipt =
            ConnectorCtasPublicationFenceReceipt::try_new(&advance, Bytes::from_static(b"receipt"))
                .unwrap();
        receipt.validate_for(&advance).unwrap();
        assert_eq!(receipt.payload().as_ref(), b"receipt");
        assert!(!format!("{receipt:?}").contains("receipt\""));
        let oversized = Bytes::from(vec![0; MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES + 1]);
        assert!(
            ConnectorCtasPublicationFenceReceipt::try_new(&advance, oversized.clone()).is_err()
        );
        assert!(
            ConnectorCtasPublicationReceipt::try_new(&fence, action(2), [2; 32], oversized,)
                .is_err()
        );

        let mut corrupt = proof(
            binding(1),
            &fence,
            ConnectorCtasProofPurpose::HistoricalNotCreated,
            None,
            [1; 32],
            None,
            b"proof",
        );
        corrupt.digest = [7; 32];
        assert_eq!(
            corrupt.validate_seal().unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );

        assert!(ConnectorCtasStagedPublicationCapability::try_new(0).is_err());
        assert!(ConnectorCtasStagedPublicationCapability::try_new(2).is_err());
    }

    #[test]
    fn ids_are_uuid_v7_and_advance_replay_keeps_one_action_digest() {
        assert!(ConnectorCtasOperationId::try_from_bytes([9; 16]).is_err());
        assert!(ConnectorCtasActionId::try_from_bytes([2; 16]).is_err());
        let fence = fence(1);
        let action_id = action(2);
        let first =
            ConnectorCtasAdvanceFenceRequest::try_new(fence.clone(), action_id, context()).unwrap();
        let reply_loss_replay =
            ConnectorCtasAdvanceFenceRequest::try_new(fence.clone(), action_id, context()).unwrap();
        assert_eq!(first.input_digest, reply_loss_replay.input_digest);
        let receipt = ConnectorCtasPublicationFenceReceipt::try_new(
            &first,
            Bytes::from_static(b"advance-receipt"),
        )
        .unwrap();
        receipt.validate_for(&reply_loss_replay).unwrap();
        let foreign_action =
            ConnectorCtasAdvanceFenceRequest::try_new(fence.clone(), action(3), context()).unwrap();
        assert!(receipt.validate_for(&foreign_action).is_err());
        assert_ne!(
            first.input_digest,
            ConnectorCtasAdvanceFenceRequest::try_new(fence, action(3), context())
                .unwrap()
                .input_digest
        );
        let mut drifted = first;
        drifted.action_id = action(4);
        assert_eq!(
            drifted.validate().unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn successful_stage_must_fit_the_durable_locator_and_proof_budget() {
        let fence = fence(1);
        let owner = binding(1);
        let action_id = action(2);
        let request = ConnectorCtasStageRequest::try_new(
            owner.clone(),
            fence.clone(),
            action_id,
            ConnectorCtasStagedTableDefinition::try_new(
                target("orders"),
                vec![ConnectorColumnDefinition {
                    name: Arc::from("id"),
                    data_type: ConnectorDataType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                }],
                Vec::new(),
                BTreeMap::new(),
            )
            .unwrap(),
            CreatePolicy::FailIfExists,
            Bytes::new(),
            context(),
        )
        .unwrap();
        let locator = ConnectorCtasStagedLocator::try_new(
            owner.clone(),
            &fence,
            action_id,
            request.target_digest,
            Bytes::from(vec![7; MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES]),
        )
        .unwrap();
        let proof = proof(
            owner.clone(),
            &fence,
            ConnectorCtasProofPurpose::Stage,
            Some(action_id),
            request.input_digest,
            Some(&locator),
            b"stage-proof",
        );
        assert!(
            ConnectorCtasStageResult::try_new(
                &request,
                locator,
                ConnectorStagedTableHandle::try_new(
                    owner,
                    ConnectorMutationOperationId::from_bytes(action_id.to_bytes()),
                    Bytes::from_static(b"writer-handle"),
                )
                .unwrap(),
                ConnectorCtasPublicationReceipt::try_new(
                    &fence,
                    action_id,
                    request.input_digest,
                    Bytes::from_static(b"stage-receipt"),
                )
                .unwrap(),
                proof,
            )
            .is_err()
        );
    }

    #[test]
    fn descriptor_evidence_must_bind_an_exact_checkpoint_and_locator() {
        let old_fence = fence(1);
        let current_fence = fence(2);
        let owner = binding(1);
        let locator = ConnectorCtasStagedLocator::try_new(
            owner.clone(),
            &old_fence,
            action(2),
            [3; 32],
            Bytes::from_static(b"locator"),
        )
        .unwrap();
        let evidence = proof(
            owner.clone(),
            &old_fence,
            ConnectorCtasProofPurpose::Stage,
            Some(action(2)),
            [5; 32],
            Some(&locator),
            b"stage-evidence",
        );
        let checkpoint = ConnectorHistoricalCtasCheckpoint {
            action_id: action(2),
            action: ConnectorHistoricalCtasAction::Stage,
            dispatch: ConnectorHistoricalCtasDispatchState::Completed,
            input_digest: [5; 32],
            evidence_digest: Some(evidence.digest()),
        };
        ConnectorHistoricalCtasDescriptor::try_new(
            owner.clone(),
            current_fence.clone(),
            [4; 32],
            [3; 32],
            CreatePolicy::FailIfExists,
            Some(locator.clone()),
            vec![checkpoint],
            Some(evidence),
        )
        .unwrap();
        let unrelated = proof(
            owner.clone(),
            &old_fence,
            ConnectorCtasProofPurpose::Stage,
            Some(action(3)),
            [5; 32],
            Some(&locator),
            b"unrelated-valid-proof",
        );
        assert!(
            ConnectorHistoricalCtasDescriptor::try_new(
                owner,
                current_fence,
                [4; 32],
                [3; 32],
                CreatePolicy::FailIfExists,
                Some(locator),
                vec![checkpoint],
                Some(unrelated),
            )
            .is_err()
        );
    }

    #[test]
    fn canonical_enum_tags_are_explicit_and_stable() {
        assert_eq!(create_policy_tag(CreatePolicy::FailIfExists), 0);
        assert_eq!(create_policy_tag(CreatePolicy::NoOpIfExists), 1);
        assert_eq!(
            proof_purpose_tag(ConnectorCtasProofPurpose::HistoricalCleanup),
            15
        );
        assert_eq!(
            historical_disposition_tag(ConnectorHistoricalCtasDisposition::Conflict),
            5
        );
        assert_eq!(
            ctas_conflict_kind_tag(ConnectorCtasConflictKind::CreatePolicyConflict),
            5
        );
    }

    #[test]
    fn locator_and_proof_wire_round_trip_complete_neutral_authority() {
        let fence = fence(1);
        let owner = binding(1);
        let locator = ConnectorCtasStagedLocator::try_new(
            owner.clone(),
            &fence,
            action(2),
            [3; 32],
            Bytes::from_static(b"locator-wire"),
        )
        .unwrap();
        let locator_wire = locator.try_to_wire_v1().unwrap();
        assert!(locator_wire.len() <= MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES);
        assert_eq!(
            ConnectorCtasStagedLocator::try_from_wire_v1(&locator_wire).unwrap(),
            locator
        );

        let proof = proof(
            owner,
            &fence,
            ConnectorCtasProofPurpose::HistoricalStaged,
            None,
            [4; 32],
            Some(&locator),
            b"proof-wire",
        );
        let proof_wire = proof.try_to_wire_v1().unwrap();
        assert!(proof_wire.len() <= MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES);
        assert_eq!(
            ConnectorCtasPublicationProof::try_from_wire_v1(&proof_wire).unwrap(),
            proof
        );
        let mut corrupt_wire = proof_wire.to_vec();
        *corrupt_wire.last_mut().unwrap() ^= 1;
        assert!(ConnectorCtasPublicationProof::try_from_wire_v1(&corrupt_wire).is_err());
    }

    #[test]
    fn typed_ctas_failures_preserve_dispatch_and_conflict_classification() {
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Conflict,
            "catalog generation conflict",
        );
        for classified in [
            ConnectorCtasFailure::KnownNotDispatched(failure.clone()),
            ConnectorCtasFailure::PossiblyDispatched(failure.clone()),
            ConnectorCtasFailure::CommittedResponseInvalid(failure.clone()),
            ConnectorCtasFailure::Ambiguous(failure.clone()),
            ConnectorCtasFailure::Conflict {
                kind: ConnectorCtasConflictKind::DigestConflict,
                failure: failure.clone(),
            },
        ] {
            assert_eq!(
                classified.failure().kind(),
                ConnectorMutationFailureKind::Conflict
            );
        }
        for (kind, expected) in [
            (
                ConnectorCtasConflictKind::StaleFence,
                Some(ConnectorExternalFenceFailure::Stale),
            ),
            (
                ConnectorCtasConflictKind::IdentityConflict,
                Some(ConnectorExternalFenceFailure::ForeignOperation),
            ),
            (
                ConnectorCtasConflictKind::DigestConflict,
                Some(ConnectorExternalFenceFailure::Superseded),
            ),
            (ConnectorCtasConflictKind::AlreadyPublished, None),
            (ConnectorCtasConflictKind::AlreadyAborted, None),
            (ConnectorCtasConflictKind::CreatePolicyConflict, None),
        ] {
            assert_eq!(kind.external_fence_failure(), expected);
        }
    }

    #[test]
    fn locator_and_receipt_reject_foreign_or_drifted_actions() {
        let current_fence = fence(1);
        let locator = ConnectorCtasStagedLocator::try_new(
            binding(1),
            &current_fence,
            action(2),
            [3; 32],
            Bytes::from_static(b"locator"),
        )
        .unwrap();
        locator
            .validate_for_foreground(&binding(1), &current_fence)
            .unwrap();
        assert!(
            locator
                .validate_for_foreground(&binding(2), &fence(2))
                .is_err()
        );
        locator.validate_for_historical(&fence(2)).unwrap();

        let receipt = ConnectorCtasPublicationReceipt::try_new(
            &current_fence,
            action(2),
            [4; 32],
            Bytes::from_static(b"receipt"),
        )
        .unwrap();
        receipt
            .validate_for(&current_fence, action(2), [4; 32])
            .unwrap();
        let drift = receipt
            .validate_for(&current_fence, action(2), [5; 32])
            .unwrap_err();
        assert_eq!(
            drift.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Superseded)
        );
    }

    #[test]
    fn result_aggregates_seal_dispositions_locators_and_proofs() {
        let fence = fence(1);
        let owner = binding(1);
        let action_id = action(2);
        let stage_request = ConnectorCtasStageRequest::try_new(
            owner.clone(),
            fence.clone(),
            action_id,
            ConnectorCtasStagedTableDefinition::try_new(
                target("orders"),
                vec![ConnectorColumnDefinition {
                    name: Arc::from("id"),
                    data_type: ConnectorDataType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                }],
                Vec::new(),
                BTreeMap::new(),
            )
            .unwrap(),
            CreatePolicy::FailIfExists,
            Bytes::from_static(b"initialization"),
            context(),
        )
        .unwrap();
        let target_digest = stage_request.target_digest;
        let locator = ConnectorCtasStagedLocator::try_new(
            owner.clone(),
            &fence,
            action_id,
            target_digest,
            Bytes::from_static(b"locator"),
        )
        .unwrap();
        let receipt = ConnectorCtasPublicationReceipt::try_new(
            &fence,
            action_id,
            stage_request.input_digest,
            Bytes::from_static(b"receipt"),
        )
        .unwrap();
        let stage_proof = proof(
            owner.clone(),
            &fence,
            ConnectorCtasProofPurpose::Stage,
            Some(action_id),
            stage_request.input_digest,
            Some(&locator),
            b"proof-a",
        );

        let mut drifted_stage_request = stage_request.clone();
        drifted_stage_request.initialization_digest = [8; 32];
        assert_eq!(
            drifted_stage_request
                .validate_for(&owner)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut stage = ConnectorCtasStageResult::try_new(
            &stage_request,
            locator.clone(),
            ConnectorStagedTableHandle::try_new(
                owner.clone(),
                ConnectorMutationOperationId::from_bytes(action_id.to_bytes()),
                Bytes::from_static(b"writer-handle"),
            )
            .unwrap(),
            receipt.clone(),
            stage_proof.clone(),
        )
        .unwrap();
        stage.validate_for(&stage_request).unwrap();
        stage.proof = proof(
            owner.clone(),
            &fence,
            ConnectorCtasProofPurpose::Stage,
            Some(action_id),
            stage_request.input_digest,
            Some(&locator),
            b"proof-b",
        );
        assert_eq!(
            stage.validate_for(&stage_request).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );

        let publish_request = ConnectorCtasPublishRequest::try_new(
            owner.clone(),
            fence.clone(),
            action(3),
            locator.clone(),
            [7; 32],
            CreatePolicy::FailIfExists,
            context(),
        )
        .unwrap();
        let mut drifted_publish_request = publish_request.clone();
        drifted_publish_request.write_completion_digest = [8; 32];
        assert_eq!(
            drifted_publish_request
                .validate_for(&owner)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        let publish_receipt = ConnectorCtasPublicationReceipt::try_new(
            &fence,
            publish_request.action_id,
            publish_request.input_digest,
            Bytes::from_static(b"publish-receipt"),
        )
        .unwrap();
        let mut publish = ConnectorCtasPublishResult::try_new(
            &publish_request,
            ConnectorCtasPublishDisposition::Published,
            publish_receipt,
            proof(
                owner.clone(),
                &fence,
                ConnectorCtasProofPurpose::PublishPublished,
                Some(publish_request.action_id),
                publish_request.input_digest,
                Some(&locator),
                b"published",
            ),
        )
        .unwrap();
        publish.disposition = ConnectorCtasPublishDisposition::NoOp;
        assert_eq!(
            publish
                .validate_for(&publish_request)
                .unwrap_err()
                .external_fence_failure(),
            Some(ConnectorExternalFenceFailure::ForeignOperation)
        );

        let abort_request = ConnectorCtasAbortRequest::try_new(
            owner.clone(),
            fence.clone(),
            action(4),
            locator.clone(),
            stage_proof,
            context(),
        )
        .unwrap();
        let mut drifted_abort_request = abort_request.clone();
        drifted_abort_request.proof = proof(
            owner.clone(),
            &fence,
            ConnectorCtasProofPurpose::Stage,
            Some(action_id),
            stage_request.input_digest,
            Some(&locator),
            b"another-stage-proof",
        );
        assert_eq!(
            drifted_abort_request
                .validate_for(&owner)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        let abort_receipt = ConnectorCtasPublicationReceipt::try_new(
            &fence,
            abort_request.action_id,
            abort_request.input_digest,
            Bytes::from_static(b"abort-receipt"),
        )
        .unwrap();
        let mut abort = ConnectorCtasAbortResult::try_new(
            &abort_request,
            ConnectorCtasAbortDisposition::Aborted,
            abort_receipt,
            proof(
                owner,
                &fence,
                ConnectorCtasProofPurpose::AbortAborted,
                Some(abort_request.action_id),
                abort_request.input_digest,
                Some(&locator),
                b"aborted",
            ),
        )
        .unwrap();
        abort.proof = proof(
            binding(1),
            &fence,
            ConnectorCtasProofPurpose::AbortAborted,
            Some(abort_request.action_id),
            abort_request.input_digest,
            Some(&locator),
            b"different-abort-proof",
        );
        assert_eq!(
            abort.validate_for(&abort_request).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn publish_and_abort_results_reject_proofs_that_exceed_the_durable_wire_budget() {
        let fence = fence(1);
        let owner = binding(1);
        let locator = ConnectorCtasStagedLocator::try_new(
            owner.clone(),
            &fence,
            action(2),
            [3; 32],
            Bytes::from_static(b"locator"),
        )
        .unwrap();

        let publish_request = ConnectorCtasPublishRequest::try_new(
            owner.clone(),
            fence.clone(),
            action(3),
            locator.clone(),
            [7; 32],
            CreatePolicy::FailIfExists,
            context(),
        )
        .unwrap();
        let publish_receipt = ConnectorCtasPublicationReceipt::try_new(
            &fence,
            publish_request.action_id,
            publish_request.input_digest,
            Bytes::from_static(b"publish-receipt"),
        )
        .unwrap();
        let oversized_publish_proof = ConnectorCtasPublicationProof::try_new(
            owner.clone(),
            &fence,
            ConnectorCtasProofPurpose::PublishPublished,
            Some(publish_request.action_id),
            publish_request.input_digest,
            Some(&locator),
            Bytes::from(vec![0; MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES]),
        )
        .unwrap();
        assert!(
            ConnectorCtasPublishResult::try_new(
                &publish_request,
                ConnectorCtasPublishDisposition::Published,
                publish_receipt,
                oversized_publish_proof,
            )
            .is_err()
        );

        let stage_proof = proof(
            owner.clone(),
            &fence,
            ConnectorCtasProofPurpose::Stage,
            Some(action(2)),
            [4; 32],
            Some(&locator),
            b"stage-proof",
        );
        let abort_request = ConnectorCtasAbortRequest::try_new(
            owner.clone(),
            fence.clone(),
            action(4),
            locator.clone(),
            stage_proof,
            context(),
        )
        .unwrap();
        let abort_receipt = ConnectorCtasPublicationReceipt::try_new(
            &fence,
            abort_request.action_id,
            abort_request.input_digest,
            Bytes::from_static(b"abort-receipt"),
        )
        .unwrap();
        let oversized_abort_proof = ConnectorCtasPublicationProof::try_new(
            owner,
            &fence,
            ConnectorCtasProofPurpose::AbortAborted,
            Some(abort_request.action_id),
            abort_request.input_digest,
            Some(&locator),
            Bytes::from(vec![0; MAX_CONNECTOR_CTAS_PUBLICATION_PAYLOAD_BYTES]),
        )
        .unwrap();
        assert!(
            ConnectorCtasAbortResult::try_new(
                &abort_request,
                ConnectorCtasAbortDisposition::Aborted,
                abort_receipt,
                oversized_abort_proof,
            )
            .is_err()
        );
    }

    #[test]
    fn all_historical_dispositions_are_typed_and_fail_closed() {
        let descriptor = descriptor();
        let staged_locator = descriptor.locator.clone();
        for disposition in [
            ConnectorHistoricalCtasDisposition::NotCreated,
            ConnectorHistoricalCtasDisposition::Staged,
            ConnectorHistoricalCtasDisposition::Published,
            ConnectorHistoricalCtasDisposition::NoOp,
            ConnectorHistoricalCtasDisposition::Aborted,
            ConnectorHistoricalCtasDisposition::Conflict,
            ConnectorHistoricalCtasDisposition::Ambiguous,
            ConnectorHistoricalCtasDisposition::Unsupported,
        ] {
            let locator = if matches!(
                disposition,
                ConnectorHistoricalCtasDisposition::Staged
                    | ConnectorHistoricalCtasDisposition::NoOp
            ) {
                staged_locator.clone()
            } else {
                None
            };
            let inspection_binding = binding(2);
            let observation_proof =
                (disposition != ConnectorHistoricalCtasDisposition::Unsupported).then(|| {
                    proof(
                        inspection_binding.clone(),
                        &descriptor.fence,
                        ConnectorCtasProofPurpose::for_historical(disposition),
                        None,
                        descriptor.digest(),
                        locator.as_ref(),
                        b"inspection-proof",
                    )
                });
            let failure = matches!(
                disposition,
                ConnectorHistoricalCtasDisposition::Conflict
                    | ConnectorHistoricalCtasDisposition::Ambiguous
                    | ConnectorHistoricalCtasDisposition::Unsupported
            )
            .then(|| {
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "historical inspection is inconclusive",
                )
            });
            let observation = ConnectorHistoricalCtasObservation::try_new(
                inspection_binding,
                &descriptor,
                disposition,
                locator,
                observation_proof,
                (disposition == ConnectorHistoricalCtasDisposition::Conflict)
                    .then_some(ConnectorCtasConflictKind::DigestConflict),
                failure,
            )
            .unwrap();
            observation.validate_for(&descriptor).unwrap();
            assert_eq!(
                observation.disposition.is_resolved(),
                !matches!(
                    disposition,
                    ConnectorHistoricalCtasDisposition::Ambiguous
                        | ConnectorHistoricalCtasDisposition::Unsupported
                )
            );
        }

        assert!(
            ConnectorHistoricalCtasObservation::try_new(
                binding(2),
                &descriptor,
                ConnectorHistoricalCtasDisposition::Conflict,
                None,
                None,
                Some(ConnectorCtasConflictKind::DigestConflict),
                Some(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "conflict without proof",
                )),
            )
            .is_err()
        );

        let oversized_locator = ConnectorCtasStagedLocator::try_new(
            binding(1),
            &fence(1),
            action(2),
            descriptor.target_digest,
            Bytes::from(vec![0; MAX_CONNECTOR_CTAS_DURABLE_WIRE_BYTES]),
        )
        .unwrap();
        let oversized_proof = proof(
            binding(2),
            &descriptor.fence,
            ConnectorCtasProofPurpose::HistoricalStaged,
            None,
            descriptor.digest(),
            Some(&oversized_locator),
            b"inspection-proof",
        );
        assert!(
            ConnectorHistoricalCtasObservation::try_new(
                binding(2),
                &descriptor,
                ConnectorHistoricalCtasDisposition::Staged,
                Some(oversized_locator),
                Some(oversized_proof),
                None,
                None,
            )
            .is_err()
        );

        let staged_locator = descriptor.locator.clone().unwrap();
        let forged_published_proof = proof(
            binding(2),
            &descriptor.fence,
            ConnectorCtasProofPurpose::HistoricalPublished,
            None,
            descriptor.digest(),
            Some(&staged_locator),
            b"published-proof",
        );
        assert!(
            ConnectorHistoricalCtasObservation::try_new(
                binding(2),
                &descriptor,
                ConnectorHistoricalCtasDisposition::Staged,
                Some(staged_locator),
                Some(forged_published_proof),
                None,
                None,
            )
            .is_err()
        );

        let ambiguous_without_proof = ConnectorHistoricalCtasObservation::try_new(
            binding(2),
            &descriptor,
            ConnectorHistoricalCtasDisposition::Ambiguous,
            None,
            None,
            None,
            Some(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "inspection reply was lost",
            )),
        )
        .unwrap();
        assert!(
            ConnectorHistoricalCtasCleanupRequest {
                descriptor: descriptor.clone(),
                observation: ambiguous_without_proof,
                context: context(),
            }
            .validate()
            .is_err()
        );
        assert!(
            ConnectorHistoricalCtasObservation::try_new(
                binding(2),
                &descriptor,
                ConnectorHistoricalCtasDisposition::Unsupported,
                None,
                Some(proof(
                    binding(2),
                    &descriptor.fence,
                    ConnectorCtasProofPurpose::HistoricalUnsupported,
                    None,
                    descriptor.digest(),
                    None,
                    b"unsupported-must-not-prove",
                )),
                None,
                Some(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unsupported,
                    "unsupported",
                )),
            )
            .is_err()
        );

        let locator = descriptor.locator.clone().unwrap();
        let staged_observation = ConnectorHistoricalCtasObservation::try_new(
            binding(2),
            &descriptor,
            ConnectorHistoricalCtasDisposition::Staged,
            Some(locator.clone()),
            Some(proof(
                binding(2),
                &descriptor.fence,
                ConnectorCtasProofPurpose::HistoricalStaged,
                None,
                descriptor.digest(),
                Some(&locator),
                b"staged-inspection",
            )),
            None,
            None,
        )
        .unwrap();
        let cleanup = ConnectorHistoricalCtasCleanupRequest {
            descriptor: descriptor.clone(),
            observation: staged_observation.clone(),
            context: context(),
        };
        cleanup.validate().unwrap();
        ConnectorHistoricalCtasCleanupReceipt::try_new(
            &cleanup,
            proof(
                binding(2),
                &descriptor.fence,
                ConnectorCtasProofPurpose::HistoricalCleanup,
                None,
                staged_observation.digest(),
                Some(&locator),
                b"cleanup-proof",
            ),
        )
        .unwrap();

        let noop_observation = ConnectorHistoricalCtasObservation::try_new(
            binding(2),
            &descriptor,
            ConnectorHistoricalCtasDisposition::NoOp,
            Some(locator.clone()),
            Some(proof(
                binding(2),
                &descriptor.fence,
                ConnectorCtasProofPurpose::HistoricalNoOp,
                None,
                descriptor.digest(),
                Some(&locator),
                b"noop-inspection",
            )),
            None,
            None,
        )
        .unwrap();
        ConnectorHistoricalCtasCleanupRequest {
            descriptor: descriptor.clone(),
            observation: noop_observation,
            context: context(),
        }
        .validate()
        .unwrap();

        let published_observation = ConnectorHistoricalCtasObservation::try_new(
            binding(2),
            &descriptor,
            ConnectorHistoricalCtasDisposition::Published,
            None,
            Some(proof(
                binding(2),
                &descriptor.fence,
                ConnectorCtasProofPurpose::HistoricalPublished,
                None,
                descriptor.digest(),
                None,
                b"published-inspection",
            )),
            None,
            None,
        )
        .unwrap();
        assert!(
            ConnectorHistoricalCtasCleanupRequest {
                descriptor,
                observation: published_observation,
                context: context(),
            }
            .validate()
            .is_err()
        );
    }

    struct WrongOwnerCapability {
        descriptor: ConnectorInstanceDescriptor,
    }

    struct BadPlanningCapability {
        descriptor: ConnectorInstanceDescriptor,
    }

    impl ConnectorCtasStagedPublication for BadPlanningCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            ConnectorInstanceIncarnation::from_bytes([1; 16])
        }

        fn capability(&self) -> ConnectorCtasStagedPublicationCapability {
            ConnectorCtasStagedPublicationCapability::try_new(1).unwrap()
        }

        fn advance_fence(
            &self,
            _request: ConnectorCtasAdvanceFenceRequest,
        ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
            unreachable!()
        }

        fn stage(
            &self,
            _request: ConnectorCtasStageRequest,
        ) -> Result<ConnectorCtasStageResult, ConnectorCtasFailure> {
            unreachable!()
        }

        fn plan_write(
            &self,
            request: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorCtasFailure> {
            ConnectorStagedWritePlanningBinding::try_new(
                &request.handle,
                ConnectorWriteOperationId::new(),
                request.intent,
                request.input_schema,
                ConnectorTableHandle::try_new(instance(), Bytes::from_static(b"table")).unwrap(),
                Bytes::new(),
                request.context,
            )
            .map_err(ConnectorCtasFailure::known_not_dispatched)
        }

        fn bind_write(
            &self,
            _handle: ConnectorStagedTableHandle,
            _completion: ConnectorWriteOperationCompletion,
        ) -> Result<(), ConnectorCtasFailure> {
            unreachable!()
        }

        fn publish(
            &self,
            _request: ConnectorCtasPublishRequest,
        ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
            unreachable!()
        }

        fn abort(
            &self,
            _request: ConnectorCtasAbortRequest,
        ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
            unreachable!()
        }
    }

    impl ConnectorCtasStagedPublication for WrongOwnerCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            ConnectorInstanceIncarnation::from_bytes([2; 16])
        }

        fn capability(&self) -> ConnectorCtasStagedPublicationCapability {
            ConnectorCtasStagedPublicationCapability::try_new(1).unwrap()
        }

        fn advance_fence(
            &self,
            _request: ConnectorCtasAdvanceFenceRequest,
        ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
            unreachable!()
        }

        fn stage(
            &self,
            _request: ConnectorCtasStageRequest,
        ) -> Result<ConnectorCtasStageResult, ConnectorCtasFailure> {
            unreachable!()
        }

        fn plan_write(
            &self,
            _request: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorCtasFailure> {
            unreachable!()
        }

        fn bind_write(
            &self,
            _handle: ConnectorStagedTableHandle,
            _completion: ConnectorWriteOperationCompletion,
        ) -> Result<(), ConnectorCtasFailure> {
            unreachable!()
        }

        fn publish(
            &self,
            _request: ConnectorCtasPublishRequest,
        ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
            unreachable!()
        }

        fn abort(
            &self,
            _request: ConnectorCtasAbortRequest,
        ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
            unreachable!()
        }
    }

    #[test]
    fn owner_validation_rejects_another_generation() {
        let capability = WrongOwnerCapability {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: instance(),
            },
        };
        assert!(
            validate_ctas_staged_publication_owner(
                &capability.descriptor,
                ConnectorInstanceIncarnation::from_bytes([1; 16]),
                &capability,
            )
            .is_err()
        );
    }

    #[test]
    fn lease_rejects_a_writer_binding_that_does_not_exactly_answer_the_request() {
        let owner = binding(1);
        let lease = ConnectorCtasStagedPublicationLease::new(
            owner.clone(),
            Arc::new(BadPlanningCapability {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: instance(),
                },
            }),
            || {},
        )
        .unwrap();
        let request_operation = ConnectorWriteOperationId::new();
        let result = lease.plan_write(ConnectorStagedWritePlanningRequest {
            handle: ConnectorStagedTableHandle::try_new(
                owner,
                ConnectorMutationOperationId::from_bytes(action(2).to_bytes()),
                Bytes::from_static(b"staged-table"),
            )
            .unwrap(),
            operation_id: request_operation,
            intent: ConnectorWriteIntent::Append,
            input_schema: Arc::new(Schema::empty()),
            context: context(),
        });
        let Err(error) = result else {
            panic!("mismatched writer planning binding must be rejected");
        };
        assert!(matches!(
            error,
            ConnectorCtasFailure::CommittedResponseInvalid(_)
        ));
    }
}
