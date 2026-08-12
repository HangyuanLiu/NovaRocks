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

//! Provider-neutral external fencing contract for materialized-view
//! publication.
//!
//! The pre-existing [`super::ConnectorRefreshPublicationGuard`] proves that a
//! staged snapshot belongs to one refresh attempt, but it carries no stable
//! target identity and no control-plane fencing generation. A frontend that has
//! already lost refresh ownership can therefore still publish as long as the
//! target's main branch has not moved.
//!
//! This module freezes the three facts an external commit needs in order to
//! reject a superseded owner *at the external linearization point*:
//!
//! 1. [`ConnectorMvRefreshResourceIdentity`] — the stable fence domain, made of
//!    the provider ID and the provider-observed immutable target table UUID. A
//!    numeric MV ID, a catalog display name, and a CP-2 attachment lifecycle ID
//!    are all deliberately unrepresentable here, so a StateStore rebuild that
//!    reassigns `mv_id` cannot move a target into a different fence domain.
//! 2. [`ConnectorMvPublicationFenceGeneration`] — a monotonic ownership
//!    generation derived from a CP-1 fencing token. Only its canonical digest
//!    crosses the boundary; the raw token never does.
//! 3. [`ConnectorMvPublicationPermit`] — proof that this exact generation
//!    established the lake fence, binding the attempt to the exact fence
//!    version the provider must still observe when it advances the target.
//!
//! The contract intentionally exposes no provider ref, snapshot, or catalog
//! type. Establishing a fence and publishing under it are both external
//! mutations, so they reuse the existing [`ExternalMutationOutcome`] /
//! [`ExternalMutationEvidence`] reconciliation vocabulary rather than
//! introducing a second evidence codec: a reply loss is resolved by
//! [`ConnectorMvPublicationFencing::inspect`] against the same operation ID,
//! never by replaying under a fresh one.

use std::fmt;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::context::ConnectorRequestContext;
use super::handle::ConnectorTableHandle;
use super::identity::{ConnectorInstanceDescriptor, ConnectorProviderId};
use super::mutation::{
    ConnectorCommittedVersion, ConnectorMutationOperationId, ExternalMutationEvidence,
    ExternalMutationOutcome,
};
use super::{ConnectorError, ConnectorErrorKind, ConnectorInstanceIncarnation};

pub const CONNECTOR_MV_PUBLICATION_FENCING_CONTRACT_VERSION: u16 = 1;

/// Evidence `operation_kind` for a fence establishment.
pub const ESTABLISH_MV_PUBLICATION_FENCE_KIND: &str = "establish_mv_publication_fence";
/// Evidence `operation_kind` for a fenced MV publication.
pub const PUBLISH_MV_REFRESH_KIND: &str = "publish_mv_refresh";

fn invalid(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

/// Stable fence domain of one MV target.
///
/// Composed only of the provider ID and the provider-observed immutable target
/// table UUID. The UUID is typed rather than an opaque string precisely so a
/// display name or a numeric `mv_id` cannot be smuggled in as a fence key.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ConnectorMvRefreshResourceIdentity {
    provider_id: ConnectorProviderId,
    target_table_uuid: Uuid,
    digest: [u8; 32],
}

impl ConnectorMvRefreshResourceIdentity {
    pub fn try_new(
        provider_id: ConnectorProviderId,
        target_table_uuid: Uuid,
    ) -> Result<Self, ConnectorError> {
        if target_table_uuid.is_nil() {
            return Err(invalid(
                "MV refresh resource identity target table UUID must not be nil",
            ));
        }
        let digest = resource_digest(&provider_id, target_table_uuid);
        Ok(Self {
            provider_id,
            target_table_uuid,
            digest,
        })
    }

    pub fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub const fn target_table_uuid(&self) -> Uuid {
        self.target_table_uuid
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Stable V1 canonical encoding. Providers persist this, never a
    /// frontend-local rendering of the same facts.
    pub fn canonical_encoding(&self) -> Vec<u8> {
        let provider_id = self.provider_id.as_str().as_bytes();
        let mut encoded = Vec::with_capacity(provider_id.len() + 17);
        encoded.extend_from_slice(provider_id);
        encoded.push(0);
        encoded.extend_from_slice(self.target_table_uuid.as_bytes());
        encoded
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.target_table_uuid.is_nil() {
            return Err(invalid(
                "MV refresh resource identity target table UUID must not be nil",
            ));
        }
        if self.digest != resource_digest(&self.provider_id, self.target_table_uuid) {
            return Err(corrupt(
                "MV refresh resource identity digest does not match its contents",
            ));
        }
        Ok(())
    }
}

fn resource_digest(provider_id: &ConnectorProviderId, target_table_uuid: Uuid) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector.mv-refresh-resource.v1\0");
    hasher.update(provider_id.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(target_table_uuid.as_bytes());
    hasher.finalize().into()
}

impl fmt::Debug for ConnectorMvRefreshResourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorMvRefreshResourceIdentity")
            .field("provider_id", &self.provider_id)
            .field("target_table_uuid", &self.target_table_uuid)
            .field("digest", &self.digest)
            .finish()
    }
}

/// How two fence generations relate inside the same cluster.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorMvPublicationFenceOrder {
    /// Byte-identical generation: a repeated establish is idempotent.
    Same,
    /// `self` is strictly newer and may take over.
    Supersedes,
    /// `self` is strictly older and must not publish.
    Superseded,
}

/// Monotonic MV publication ownership generation, derived from a CP-1 fencing
/// token.
///
/// The raw CP-1 token never crosses this boundary — only its canonical digest,
/// so a provider can prove "same generation" without being able to forge one.
/// Ordering is defined only inside one cluster: comparing generations from
/// different clusters fails closed rather than guessing.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ConnectorMvPublicationFenceGeneration {
    cluster_digest: [u8; 32],
    control_plane_incarnation: u64,
    resource_epoch: u64,
    token_digest: [u8; 32],
    digest: [u8; 32],
}

impl ConnectorMvPublicationFenceGeneration {
    pub fn try_new(
        cluster_id: &str,
        control_plane_incarnation: u64,
        resource_epoch: u64,
        token_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        if cluster_id.is_empty() {
            return Err(invalid(
                "MV publication fence generation cluster ID must not be empty",
            ));
        }
        if control_plane_incarnation == 0 {
            return Err(invalid(
                "MV publication fence generation control plane incarnation must be nonzero",
            ));
        }
        if resource_epoch == 0 {
            return Err(invalid(
                "MV publication fence generation resource epoch must be nonzero",
            ));
        }
        if token_digest == [0u8; 32] {
            return Err(invalid(
                "MV publication fence generation token digest must not be zero",
            ));
        }
        let mut cluster_hasher = Sha256::new();
        cluster_hasher.update(b"novarocks.connector.mv-fence-cluster.v1\0");
        cluster_hasher.update(cluster_id.as_bytes());
        let cluster_digest: [u8; 32] = cluster_hasher.finalize().into();
        let digest = generation_digest(
            cluster_digest,
            control_plane_incarnation,
            resource_epoch,
            token_digest,
        );
        Ok(Self {
            cluster_digest,
            control_plane_incarnation,
            resource_epoch,
            token_digest,
            digest,
        })
    }

    pub const fn cluster_digest(&self) -> [u8; 32] {
        self.cluster_digest
    }

    pub const fn control_plane_incarnation(&self) -> u64 {
        self.control_plane_incarnation
    }

    pub const fn resource_epoch(&self) -> u64 {
        self.resource_epoch
    }

    pub const fn token_digest(&self) -> [u8; 32] {
        self.token_digest
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Stable V1 canonical encoding for provider-side persistence.
    pub fn canonical_encoding(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(80);
        encoded.extend_from_slice(&self.cluster_digest);
        encoded.extend_from_slice(&self.control_plane_incarnation.to_be_bytes());
        encoded.extend_from_slice(&self.resource_epoch.to_be_bytes());
        encoded.extend_from_slice(&self.token_digest);
        encoded
    }

    /// Orders two generations, failing closed on anything ambiguous.
    ///
    /// Cross-cluster comparison is an error, not an ordering: two clusters have
    /// independent incarnation counters, so their numbers are incomparable.
    /// Equal `(incarnation, epoch)` with a different token digest is also an
    /// error — that is two distinct owners claiming one generation.
    pub fn try_order(
        &self,
        other: &Self,
    ) -> Result<ConnectorMvPublicationFenceOrder, ConnectorError> {
        if self.cluster_digest != other.cluster_digest {
            return Err(invalid(
                "MV publication fence generations belong to different clusters",
            ));
        }
        let mine = (self.control_plane_incarnation, self.resource_epoch);
        let theirs = (other.control_plane_incarnation, other.resource_epoch);
        if mine == theirs {
            if self.token_digest != other.token_digest {
                return Err(corrupt(
                    "two MV publication fence generations share one epoch with different tokens",
                ));
            }
            return Ok(ConnectorMvPublicationFenceOrder::Same);
        }
        if mine > theirs {
            Ok(ConnectorMvPublicationFenceOrder::Supersedes)
        } else {
            Ok(ConnectorMvPublicationFenceOrder::Superseded)
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.control_plane_incarnation == 0
            || self.resource_epoch == 0
            || self.token_digest == [0u8; 32]
            || self.cluster_digest == [0u8; 32]
        {
            return Err(invalid(
                "MV publication fence generation carries an unset required field",
            ));
        }
        if self.digest
            != generation_digest(
                self.cluster_digest,
                self.control_plane_incarnation,
                self.resource_epoch,
                self.token_digest,
            )
        {
            return Err(corrupt(
                "MV publication fence generation digest does not match its contents",
            ));
        }
        Ok(())
    }
}

fn generation_digest(
    cluster_digest: [u8; 32],
    control_plane_incarnation: u64,
    resource_epoch: u64,
    token_digest: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector.mv-fence-generation.v1\0");
    hasher.update(cluster_digest);
    hasher.update(control_plane_incarnation.to_be_bytes());
    hasher.update(resource_epoch.to_be_bytes());
    hasher.update(token_digest);
    hasher.finalize().into()
}

impl fmt::Debug for ConnectorMvPublicationFenceGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorMvPublicationFenceGeneration")
            .field("control_plane_incarnation", &self.control_plane_incarnation)
            .field("resource_epoch", &self.resource_epoch)
            .field("digest", &self.digest)
            .finish()
    }
}

/// One MV refresh attempt inside a fence generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorMvRefreshAttemptId(Uuid);

impl ConnectorMvRefreshAttemptId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn try_from_uuid(value: Uuid) -> Result<Self, ConnectorError> {
        if value.get_version_num() != 7 {
            return Err(invalid("MV refresh attempt ID must be UUIDv7"));
        }
        Ok(Self(value))
    }

    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, ConnectorError> {
        Self::try_from_uuid(Uuid::from_bytes(bytes))
    }

    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl Default for ConnectorMvRefreshAttemptId {
    fn default() -> Self {
        Self::new()
    }
}

/// Which fenced external operation an evidence or inspection refers to.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorMvPublicationOperation {
    EstablishFence,
    Publish,
}

impl ConnectorMvPublicationOperation {
    pub const fn evidence_kind(self) -> &'static str {
        match self {
            Self::EstablishFence => ESTABLISH_MV_PUBLICATION_FENCE_KIND,
            Self::Publish => PUBLISH_MV_REFRESH_KIND,
        }
    }

    pub fn from_evidence_kind(kind: &str) -> Result<Self, ConnectorError> {
        match kind {
            ESTABLISH_MV_PUBLICATION_FENCE_KIND => Ok(Self::EstablishFence),
            PUBLISH_MV_REFRESH_KIND => Ok(Self::Publish),
            _ => Err(invalid(
                "evidence operation kind is not an MV publication fencing operation",
            )),
        }
    }
}

/// Side-effect-free observation of an MV target's current external state.
///
/// This is how the stable resource identity reaches the frontend: the provider
/// signs the immutable target UUID, so no application owner has to derive it
/// from a display name or a local catalog handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvPublicationTargetObservation {
    resource: ConnectorMvRefreshResourceIdentity,
    current_visible_version: Option<ConnectorCommittedVersion>,
    established_generation: Option<ConnectorMvPublicationFenceGeneration>,
    established_fence_version: Option<ConnectorCommittedVersion>,
}

impl ConnectorMvPublicationTargetObservation {
    pub fn try_new(
        resource: ConnectorMvRefreshResourceIdentity,
        current_visible_version: Option<ConnectorCommittedVersion>,
        established_generation: Option<ConnectorMvPublicationFenceGeneration>,
        established_fence_version: Option<ConnectorCommittedVersion>,
    ) -> Result<Self, ConnectorError> {
        resource.validate()?;
        if let Some(version) = &current_visible_version {
            version.validate()?;
        }
        if let Some(generation) = &established_generation {
            generation.validate()?;
        }
        if let Some(version) = &established_fence_version {
            version.validate()?;
        }
        if established_generation.is_some() != established_fence_version.is_some() {
            return Err(invalid(
                "MV publication target observation must pair an established generation with its fence version",
            ));
        }
        Ok(Self {
            resource,
            current_visible_version,
            established_generation,
            established_fence_version,
        })
    }

    pub fn resource(&self) -> &ConnectorMvRefreshResourceIdentity {
        &self.resource
    }

    pub fn current_visible_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.current_visible_version.as_ref()
    }

    pub fn established_generation(&self) -> Option<&ConnectorMvPublicationFenceGeneration> {
        self.established_generation.as_ref()
    }

    pub fn established_fence_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.established_fence_version.as_ref()
    }
}

/// Provider-signed proof that one generation owns the target's external fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvPublicationFenceReceipt {
    resource: ConnectorMvRefreshResourceIdentity,
    generation: ConnectorMvPublicationFenceGeneration,
    fence_version: ConnectorCommittedVersion,
}

impl ConnectorMvPublicationFenceReceipt {
    pub fn try_new(
        resource: ConnectorMvRefreshResourceIdentity,
        generation: ConnectorMvPublicationFenceGeneration,
        fence_version: ConnectorCommittedVersion,
    ) -> Result<Self, ConnectorError> {
        resource.validate()?;
        generation.validate()?;
        fence_version.validate()?;
        Ok(Self {
            resource,
            generation,
            fence_version,
        })
    }

    pub fn resource(&self) -> &ConnectorMvRefreshResourceIdentity {
        &self.resource
    }

    pub fn generation(&self) -> &ConnectorMvPublicationFenceGeneration {
        &self.generation
    }

    /// Opaque exact fence pointer. The publication commit must still observe
    /// this exact value, so it is a CAS operand and not a diagnostic.
    pub fn fence_version(&self) -> &ConnectorCommittedVersion {
        &self.fence_version
    }

    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.connector.mv-fence-receipt.v1\0");
        hasher.update(self.resource.digest());
        hasher.update(self.generation.digest());
        hasher.update(self.fence_version.digest());
        hasher.finalize().into()
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.resource.validate()?;
        self.generation.validate()?;
        self.fence_version.validate()
    }
}

/// Publication-capability proof for one attempt.
///
/// A permit exists only after the attempt's generation established the lake
/// fence, and it names the exact fence version the provider must still see. It
/// is the single object a publication request needs in order to be checkable
/// against all of resource, generation, attempt, and fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvPublicationPermit {
    attempt: ConnectorMvRefreshAttemptId,
    fence: ConnectorMvPublicationFenceReceipt,
    digest: [u8; 32],
}

impl ConnectorMvPublicationPermit {
    pub fn try_new(
        attempt: ConnectorMvRefreshAttemptId,
        fence: ConnectorMvPublicationFenceReceipt,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        let digest = permit_digest(attempt, &fence);
        Ok(Self {
            attempt,
            fence,
            digest,
        })
    }

    pub const fn attempt(&self) -> ConnectorMvRefreshAttemptId {
        self.attempt
    }

    pub fn resource(&self) -> &ConnectorMvRefreshResourceIdentity {
        self.fence.resource()
    }

    pub fn generation(&self) -> &ConnectorMvPublicationFenceGeneration {
        self.fence.generation()
    }

    pub fn fence(&self) -> &ConnectorMvPublicationFenceReceipt {
        &self.fence
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.fence.validate()?;
        if self.digest != permit_digest(self.attempt, &self.fence) {
            return Err(corrupt(
                "MV publication permit digest does not match its contents",
            ));
        }
        Ok(())
    }
}

fn permit_digest(
    attempt: ConnectorMvRefreshAttemptId,
    fence: &ConnectorMvPublicationFenceReceipt,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector.mv-publication-permit.v1\0");
    hasher.update(attempt.to_bytes());
    hasher.update(fence.digest());
    hasher.finalize().into()
}

/// Receipt of a fenced publication that advanced the target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvRefreshPublicationReceipt {
    permit_digest: [u8; 32],
    resource: ConnectorMvRefreshResourceIdentity,
    generation: ConnectorMvPublicationFenceGeneration,
    attempt: ConnectorMvRefreshAttemptId,
    published_version: ConnectorCommittedVersion,
}

impl ConnectorMvRefreshPublicationReceipt {
    pub fn try_new(
        permit: &ConnectorMvPublicationPermit,
        published_version: ConnectorCommittedVersion,
    ) -> Result<Self, ConnectorError> {
        permit.validate()?;
        published_version.validate()?;
        Ok(Self {
            permit_digest: permit.digest(),
            resource: permit.resource().clone(),
            generation: permit.generation().clone(),
            attempt: permit.attempt(),
            published_version,
        })
    }

    pub const fn permit_digest(&self) -> [u8; 32] {
        self.permit_digest
    }

    pub fn resource(&self) -> &ConnectorMvRefreshResourceIdentity {
        &self.resource
    }

    pub fn generation(&self) -> &ConnectorMvPublicationFenceGeneration {
        &self.generation
    }

    pub const fn attempt(&self) -> ConnectorMvRefreshAttemptId {
        self.attempt
    }

    pub fn published_version(&self) -> &ConnectorCommittedVersion {
        &self.published_version
    }
}

/// Side-effect-free target observation request.
#[derive(Clone)]
pub struct ConnectorMvPublicationTargetRequest {
    pub table: ConnectorTableHandle,
    pub context: ConnectorRequestContext,
}

/// Request to establish (or idempotently re-establish) the lake fence.
///
/// `expected_generation` / `expected_fence_version` are the CAS preconditions
/// taken from a prior observation. `observed_target_version` is the target's
/// main version the caller froze, so the provider can refuse to build a fence
/// on top of state the caller never saw.
#[derive(Clone)]
pub struct ConnectorMvPublicationFenceRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub table: ConnectorTableHandle,
    pub resource: ConnectorMvRefreshResourceIdentity,
    pub generation: ConnectorMvPublicationFenceGeneration,
    pub expected_generation: Option<ConnectorMvPublicationFenceGeneration>,
    pub expected_fence_version: Option<ConnectorCommittedVersion>,
    pub observed_target_version: Option<ConnectorCommittedVersion>,
    pub context: ConnectorRequestContext,
}

impl ConnectorMvPublicationFenceRequest {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.resource.validate()?;
        self.generation.validate()?;
        if let Some(version) = &self.expected_fence_version {
            version.validate()?;
        }
        if let Some(version) = &self.observed_target_version {
            version.validate()?;
        }
        if self.expected_generation.is_some() != self.expected_fence_version.is_some() {
            return Err(invalid(
                "MV publication fence request must pair an expected generation with its fence version",
            ));
        }
        if let Some(expected) = &self.expected_generation
            && matches!(
                self.generation.try_order(expected)?,
                ConnectorMvPublicationFenceOrder::Superseded
            )
        {
            return Err(invalid(
                "MV publication fence request would move the fence backwards",
            ));
        }
        Ok(())
    }
}

/// Request to advance the target under an established fence.
#[derive(Clone)]
pub struct ConnectorMvRefreshPublicationRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub table: ConnectorTableHandle,
    pub permit: ConnectorMvPublicationPermit,
    /// Provider-produced version of the staged result being published.
    pub staged_version: ConnectorCommittedVersion,
    /// The target's frozen main version. `None` means "target still empty".
    pub expected_target_version: Option<ConnectorCommittedVersion>,
    pub context: ConnectorRequestContext,
}

impl ConnectorMvRefreshPublicationRequest {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.permit.validate()?;
        self.staged_version.validate()?;
        if let Some(version) = &self.expected_target_version {
            version.validate()?;
        }
        Ok(())
    }
}

/// Cross-incarnation inspection of one previously issued operation.
#[derive(Clone)]
pub struct ConnectorMvPublicationInspectRequest {
    pub table: ConnectorTableHandle,
    pub resource: ConnectorMvRefreshResourceIdentity,
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

/// Terminal classification of an inspected operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorMvPublicationDisposition {
    KnownCommitted,
    KnownUncommitted,
    /// The lake carries no decisive evidence. Callers keep the attempt and its
    /// artifacts and report unresolved; they must not guess a winner.
    Unresolved,
}

/// Result of inspecting an operation whose reply was lost.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvPublicationInspection {
    operation_id: ConnectorMutationOperationId,
    operation: ConnectorMvPublicationOperation,
    disposition: ConnectorMvPublicationDisposition,
    resource: ConnectorMvRefreshResourceIdentity,
    generation: ConnectorMvPublicationFenceGeneration,
    committed_version: Option<ConnectorCommittedVersion>,
}

impl ConnectorMvPublicationInspection {
    pub fn try_new(
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorMvPublicationOperation,
        disposition: ConnectorMvPublicationDisposition,
        resource: ConnectorMvRefreshResourceIdentity,
        generation: ConnectorMvPublicationFenceGeneration,
        committed_version: Option<ConnectorCommittedVersion>,
    ) -> Result<Self, ConnectorError> {
        resource.validate()?;
        generation.validate()?;
        if let Some(version) = &committed_version {
            version.validate()?;
        }
        match disposition {
            ConnectorMvPublicationDisposition::KnownCommitted if committed_version.is_none() => {
                return Err(invalid(
                    "committed MV publication inspection must carry its committed version",
                ));
            }
            ConnectorMvPublicationDisposition::KnownUncommitted
            | ConnectorMvPublicationDisposition::Unresolved
                if committed_version.is_some() =>
            {
                return Err(invalid(
                    "undecided MV publication inspection must not carry a committed version",
                ));
            }
            _ => {}
        }
        Ok(Self {
            operation_id,
            operation,
            disposition,
            resource,
            generation,
            committed_version,
        })
    }

    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }

    pub const fn operation(&self) -> ConnectorMvPublicationOperation {
        self.operation
    }

    pub const fn disposition(&self) -> ConnectorMvPublicationDisposition {
        self.disposition
    }

    pub fn resource(&self) -> &ConnectorMvRefreshResourceIdentity {
        &self.resource
    }

    pub fn generation(&self) -> &ConnectorMvPublicationFenceGeneration {
        &self.generation
    }

    /// Present exactly when the operation is known to have committed: the fence
    /// version for `EstablishFence`, the published version for `Publish`.
    pub fn committed_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.committed_version.as_ref()
    }
}

/// Optional FE-only external fencing capability for MV publication.
///
/// It is deliberately absent from BE execution bindings: establishing a fence
/// and publishing under it are control-plane external mutations.
pub trait ConnectorMvPublicationFencing: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ConnectorInstanceIncarnation;

    /// Reads the target's immutable UUID and current external state without
    /// writing anything.
    fn observe_target(
        &self,
        request: ConnectorMvPublicationTargetRequest,
    ) -> Result<ConnectorMvPublicationTargetObservation, ConnectorError>;

    /// Establishes this generation's fence with a provider-authoritative CAS.
    fn establish_fence(
        &self,
        request: ConnectorMvPublicationFenceRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMvPublicationFenceReceipt>, ConnectorError>;

    /// Advances the target in a single external commit that also requires the
    /// exact fence version named by the permit.
    fn publish(
        &self,
        request: ConnectorMvRefreshPublicationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMvRefreshPublicationReceipt>, ConnectorError>;

    /// Resolves a lost reply from lake evidence under the same operation ID.
    ///
    /// This is the only supported recovery for `CommitUnknown`: providers must
    /// never re-issue the operation under a fresh ID, and must return
    /// [`ConnectorMvPublicationDisposition::Unresolved`] rather than guess.
    fn inspect(
        &self,
        request: ConnectorMvPublicationInspectRequest,
    ) -> Result<ConnectorMvPublicationInspection, ConnectorError>;
}

pub(crate) fn validate_mv_publication_fencing_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    capability: &dyn ConnectorMvPublicationFencing,
) -> Result<(), ConnectorError> {
    if capability.descriptor() != descriptor || capability.incarnation() != incarnation {
        return Err(invalid(
            "MV publication fencing capability owner does not match its control binding generation",
        ));
    }
    Ok(())
}

/// Narrow consumer port. A consumer can hold one generation-fenced lease but
/// cannot inspect, register, or retire control generations.
pub trait ConnectorMvPublicationFencingResolver: Send + Sync {
    fn acquire_current_mv_publication_fencing(
        &self,
        instance_id: &super::identity::ConnectorInstanceId,
    ) -> Result<ConnectorMvPublicationFencingLease, ConnectorError>;
}

/// Exact-generation lease over the fencing capability.
///
/// `observe_target`, `establish_fence`, and `publish` are bound to this exact
/// FE-local Connector incarnation. `inspect` deliberately is not: recovering a
/// lost reply is a lake-truth question that outlives the incarnation that
/// issued the operation, so it validates provider, stable resource, and
/// operation ID instead of the incarnation.
#[derive(Clone)]
pub struct ConnectorMvPublicationFencingLease {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    fencing: Arc<dyn ConnectorMvPublicationFencing>,
    _release: Arc<MvPublicationFencingLeaseRelease>,
}

struct MvPublicationFencingLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl Drop for MvPublicationFencingLeaseRelease {
    fn drop(&mut self) {
        if let Some(release) = self.release.lock().ok().and_then(|mut slot| slot.take()) {
            release();
        }
    }
}

impl ConnectorMvPublicationFencingLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        fencing: Arc<dyn ConnectorMvPublicationFencing>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        validate_mv_publication_fencing_owner(&descriptor, incarnation, fencing.as_ref())?;
        Ok(Self {
            descriptor,
            incarnation,
            fencing,
            _release: Arc::new(MvPublicationFencingLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    pub fn observe_target(
        &self,
        request: ConnectorMvPublicationTargetRequest,
    ) -> Result<ConnectorMvPublicationTargetObservation, ConnectorError> {
        let observation = self.fencing.observe_target(request)?;
        self.validate_resource(observation.resource())?;
        Ok(observation)
    }

    pub fn establish_fence(
        &self,
        request: ConnectorMvPublicationFenceRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMvPublicationFenceReceipt>, ConnectorError> {
        request.validate()?;
        self.validate_resource(&request.resource)?;
        let operation_id = request.operation_id;
        let expected_resource = request.resource.clone();
        let expected_generation = request.generation.clone();
        let outcome = self.fencing.establish_fence(request)?;
        match &outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                receipt.validate()?;
                if receipt.resource() != &expected_resource
                    || receipt.generation() != &expected_generation
                {
                    return Err(corrupt(
                        "established MV publication fence receipt does not match its request",
                    ));
                }
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {}
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(
                    operation_id,
                    ConnectorMvPublicationOperation::EstablishFence,
                    evidence,
                )?;
            }
        }
        Ok(outcome)
    }

    pub fn publish(
        &self,
        request: ConnectorMvRefreshPublicationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMvRefreshPublicationReceipt>, ConnectorError> {
        request.validate()?;
        self.validate_resource(request.permit.resource())?;
        let operation_id = request.operation_id;
        let expected_permit_digest = request.permit.digest();
        let outcome = self.fencing.publish(request)?;
        match &outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                if receipt.permit_digest() != expected_permit_digest {
                    return Err(corrupt(
                        "MV publication receipt does not match the permit it was requested with",
                    ));
                }
                receipt.published_version().validate()?;
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {}
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(
                    operation_id,
                    ConnectorMvPublicationOperation::Publish,
                    evidence,
                )?;
            }
        }
        Ok(outcome)
    }

    /// Resolves an operation issued by any incarnation of this provider.
    pub fn inspect(
        &self,
        request: ConnectorMvPublicationInspectRequest,
    ) -> Result<ConnectorMvPublicationInspection, ConnectorError> {
        request.resource.validate()?;
        self.validate_resource(&request.resource)?;
        if request.evidence.descriptor() != &self.descriptor {
            return Err(invalid(
                "MV publication inspection evidence belongs to a different connector instance",
            ));
        }
        let operation =
            ConnectorMvPublicationOperation::from_evidence_kind(request.evidence.operation_kind())?;
        let operation_id = request.evidence.operation_id();
        let expected_resource = request.resource.clone();
        let inspection = self.fencing.inspect(request)?;
        if inspection.operation_id() != operation_id || inspection.operation() != operation {
            return Err(corrupt(
                "MV publication inspection does not answer the operation it was asked about",
            ));
        }
        if inspection.resource() != &expected_resource {
            return Err(corrupt(
                "MV publication inspection answered for a different stable resource",
            ));
        }
        Ok(inspection)
    }

    fn validate_resource(
        &self,
        resource: &ConnectorMvRefreshResourceIdentity,
    ) -> Result<(), ConnectorError> {
        resource.validate()?;
        if resource.provider_id() != &self.descriptor.provider_id {
            return Err(invalid(
                "MV refresh resource identity belongs to a different provider",
            ));
        }
        Ok(())
    }

    fn validate_evidence(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorMvPublicationOperation,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), ConnectorError> {
        if evidence.descriptor() != &self.descriptor || evidence.incarnation() != self.incarnation {
            return Err(corrupt(
                "MV publication evidence does not belong to this control generation",
            ));
        }
        if evidence.operation_id() != operation_id {
            return Err(corrupt(
                "MV publication evidence operation ID does not match its request",
            ));
        }
        if evidence.operation_kind() != operation.evidence_kind() {
            return Err(corrupt(
                "MV publication evidence operation kind does not match its request",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::{Duration, Instant};

    use super::super::identity::ConnectorInstanceId;

    struct NeverCancelled;

    impl super::super::context::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024,
            1024,
        )
        .expect("valid connector request context")
    }

    fn table() -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("ice").unwrap(),
            Bytes::from_static(b"table"),
        )
        .unwrap()
    }

    fn provider() -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").unwrap()
    }

    fn resource() -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(provider(), Uuid::from_u128(0x1234_5678))
            .unwrap()
    }

    fn generation(incarnation: u64, epoch: u64) -> ConnectorMvPublicationFenceGeneration {
        ConnectorMvPublicationFenceGeneration::try_new("cluster-a", incarnation, epoch, [7u8; 32])
            .unwrap()
    }

    fn version(snapshot_id: i64) -> ConnectorCommittedVersion {
        ConnectorCommittedVersion::try_new(
            Bytes::from_static(b"version-payload"),
            Some(snapshot_id),
        )
        .unwrap()
    }

    #[test]
    fn resource_identity_rejects_nil_uuid_and_is_digest_stable() {
        assert!(
            ConnectorMvRefreshResourceIdentity::try_new(provider(), Uuid::nil()).is_err(),
            "nil target UUID must be rejected"
        );

        let first = resource();
        let second = resource();
        assert_eq!(first.digest(), second.digest());
        assert_eq!(first.canonical_encoding(), second.canonical_encoding());

        // A different target table is a different fence domain, which is what
        // makes an external DROP/recreate safe.
        let other =
            ConnectorMvRefreshResourceIdentity::try_new(provider(), Uuid::from_u128(0x9999))
                .unwrap();
        assert_ne!(first.digest(), other.digest());
    }

    #[test]
    fn generation_orders_within_cluster_and_fails_closed_across_clusters() {
        let base = generation(1, 1);
        assert_eq!(
            base.try_order(&generation(1, 1)).unwrap(),
            ConnectorMvPublicationFenceOrder::Same
        );
        assert_eq!(
            generation(1, 2).try_order(&base).unwrap(),
            ConnectorMvPublicationFenceOrder::Supersedes
        );
        assert_eq!(
            generation(2, 1).try_order(&generation(1, 9)).unwrap(),
            ConnectorMvPublicationFenceOrder::Supersedes,
            "a newer control-plane incarnation outranks a higher epoch"
        );
        assert_eq!(
            base.try_order(&generation(1, 2)).unwrap(),
            ConnectorMvPublicationFenceOrder::Superseded
        );

        let other_cluster =
            ConnectorMvPublicationFenceGeneration::try_new("cluster-b", 1, 1, [7u8; 32]).unwrap();
        assert!(
            base.try_order(&other_cluster).is_err(),
            "cross-cluster comparison must fail closed"
        );

        let conflicting =
            ConnectorMvPublicationFenceGeneration::try_new("cluster-a", 1, 1, [8u8; 32]).unwrap();
        assert!(
            base.try_order(&conflicting).is_err(),
            "one epoch with two tokens must fail closed"
        );
    }

    #[test]
    fn generation_rejects_unset_required_fields() {
        assert!(
            ConnectorMvPublicationFenceGeneration::try_new("", 1, 1, [7u8; 32]).is_err(),
            "empty cluster ID"
        );
        assert!(
            ConnectorMvPublicationFenceGeneration::try_new("c", 0, 1, [7u8; 32]).is_err(),
            "zero incarnation"
        );
        assert!(
            ConnectorMvPublicationFenceGeneration::try_new("c", 1, 0, [7u8; 32]).is_err(),
            "zero epoch"
        );
        assert!(
            ConnectorMvPublicationFenceGeneration::try_new("c", 1, 1, [0u8; 32]).is_err(),
            "zero token digest"
        );
    }

    #[test]
    fn attempt_id_requires_uuid_v7() {
        assert!(ConnectorMvRefreshAttemptId::try_from_uuid(Uuid::now_v7()).is_ok());
        assert!(ConnectorMvRefreshAttemptId::try_from_uuid(Uuid::nil()).is_err());

        // A version-4 shaped UUID carries no ordering, so it is rejected even
        // though it is a well-formed UUID.
        let mut v4_shaped = [0xabu8; 16];
        v4_shaped[6] = (v4_shaped[6] & 0x0f) | 0x40;
        v4_shaped[8] = (v4_shaped[8] & 0x3f) | 0x80;
        assert!(ConnectorMvRefreshAttemptId::try_from_bytes(v4_shaped).is_err());
    }

    #[test]
    fn permit_binds_resource_generation_attempt_and_exact_fence() {
        let fence =
            ConnectorMvPublicationFenceReceipt::try_new(resource(), generation(1, 1), version(10))
                .unwrap();
        let attempt = ConnectorMvRefreshAttemptId::new();
        let permit = ConnectorMvPublicationPermit::try_new(attempt, fence.clone()).unwrap();

        permit.validate().unwrap();
        assert_eq!(permit.resource(), &resource());
        assert_eq!(permit.generation(), &generation(1, 1));
        assert_eq!(permit.attempt(), attempt);
        assert_eq!(permit.fence().fence_version(), &version(10));

        // A different fence version is a different permit: the publication CAS
        // operand is part of the permit identity.
        let moved_fence =
            ConnectorMvPublicationFenceReceipt::try_new(resource(), generation(1, 1), version(11))
                .unwrap();
        let moved = ConnectorMvPublicationPermit::try_new(attempt, moved_fence).unwrap();
        assert_ne!(permit.digest(), moved.digest());

        // So is a different attempt under the same fence.
        let other_attempt =
            ConnectorMvPublicationPermit::try_new(ConnectorMvRefreshAttemptId::new(), fence)
                .unwrap();
        assert_ne!(permit.digest(), other_attempt.digest());
    }

    #[test]
    fn fence_request_rejects_backwards_moves_and_unpaired_preconditions() {
        let base = ConnectorMvPublicationFenceRequest {
            operation_id: ConnectorMutationOperationId::new(),
            table: table(),
            resource: resource(),
            generation: generation(1, 1),
            expected_generation: Some(generation(1, 2)),
            expected_fence_version: Some(version(10)),
            observed_target_version: None,
            context: context(),
        };
        assert!(
            base.validate().is_err(),
            "a lower generation must not move the fence backwards"
        );

        let unpaired = ConnectorMvPublicationFenceRequest {
            expected_generation: Some(generation(1, 1)),
            expected_fence_version: None,
            ..base.clone()
        };
        assert!(
            unpaired.validate().is_err(),
            "an expected generation without its fence version is not a CAS precondition"
        );

        let takeover = ConnectorMvPublicationFenceRequest {
            generation: generation(2, 1),
            expected_generation: Some(generation(1, 2)),
            expected_fence_version: Some(version(10)),
            ..base
        };
        takeover.validate().unwrap();
    }

    #[test]
    fn inspection_pairs_disposition_with_committed_version() {
        ConnectorMvPublicationInspection::try_new(
            ConnectorMutationOperationId::new(),
            ConnectorMvPublicationOperation::Publish,
            ConnectorMvPublicationDisposition::KnownCommitted,
            resource(),
            generation(1, 1),
            Some(version(12)),
        )
        .unwrap();

        assert!(
            ConnectorMvPublicationInspection::try_new(
                ConnectorMutationOperationId::new(),
                ConnectorMvPublicationOperation::Publish,
                ConnectorMvPublicationDisposition::KnownCommitted,
                resource(),
                generation(1, 1),
                None,
            )
            .is_err(),
            "committed inspection must carry its version"
        );

        assert!(
            ConnectorMvPublicationInspection::try_new(
                ConnectorMutationOperationId::new(),
                ConnectorMvPublicationOperation::Publish,
                ConnectorMvPublicationDisposition::Unresolved,
                resource(),
                generation(1, 1),
                Some(version(12)),
            )
            .is_err(),
            "unresolved inspection must not carry a version"
        );
    }

    #[test]
    fn operation_kinds_round_trip_and_reject_foreign_evidence() {
        for operation in [
            ConnectorMvPublicationOperation::EstablishFence,
            ConnectorMvPublicationOperation::Publish,
        ] {
            assert_eq!(
                ConnectorMvPublicationOperation::from_evidence_kind(operation.evidence_kind())
                    .unwrap(),
                operation
            );
        }
        assert!(
            ConnectorMvPublicationOperation::from_evidence_kind("publish_statistics").is_err(),
            "a foreign operation kind must not be interpreted as MV fencing"
        );
    }

    #[test]
    fn target_observation_pairs_generation_with_fence_version() {
        ConnectorMvPublicationTargetObservation::try_new(resource(), Some(version(9)), None, None)
            .unwrap();
        ConnectorMvPublicationTargetObservation::try_new(
            resource(),
            Some(version(9)),
            Some(generation(1, 1)),
            Some(version(10)),
        )
        .unwrap();
        assert!(
            ConnectorMvPublicationTargetObservation::try_new(
                resource(),
                Some(version(9)),
                Some(generation(1, 1)),
                None,
            )
            .is_err(),
            "an established generation must name its fence version"
        );
    }
}
