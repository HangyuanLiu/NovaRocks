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

//! SPI-owned external operation fence for distributed writes.
//!
//! A control-plane owner cannot withdraw a Connector commit it has already
//! dispatched.  The fence is the linearization point that lets a provider
//! reject a late commit from an authority that has already been superseded.
//!
//! The value is deliberately provider-neutral and bounded.  It carries only
//! cluster identity as a digest, the control-plane generation scalars, the
//! stable write operation identity, the coordination attempt identity, and the
//! resource identity.  This SPI never depends on `novarocks-state-store`: a
//! provider must not hold a coordination lease fence or reach a state store.
//!
//! Four invariants are frozen here and enforced jointly with the provider:
//!
//! 1. a lower generation cannot commit once a higher fence is established;
//! 2. the same operation retried at the same generation is idempotent;
//! 3. a different operation can never reuse another operation's fence receipt;
//! 4. the fence is established before any writer or commit dispatch that can
//!    produce an irreversible external effect.

use std::cmp::Ordering;
use std::fmt;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExternalFenceFailure, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteOperationId, ConnectorWriteTargetRef,
};

pub const MAX_CONNECTOR_EXTERNAL_FENCE_RECEIPT_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_EXTERNAL_FENCE_CLUSTER_ID_BYTES: usize = 256;
pub const MAX_CONNECTOR_EXTERNAL_FENCE_IDENTITY_BYTES: usize = 4096;

const CONNECTOR_CLUSTER_IDENTITY_DOMAIN: &[u8] = b"novarocks.connector-cluster-identity.v1\0";
const CONNECTOR_EXTERNAL_OPERATION_FENCE_DOMAIN: &[u8] =
    b"novarocks.connector-external-operation-fence.v1\0";
const CONNECTOR_EXTERNAL_FENCE_RECEIPT_DOMAIN: &[u8] =
    b"novarocks.connector-external-fence-receipt.v1\0";

/// Cluster identity reduced to a bounded digest.
///
/// The provider must be able to bind a fence marker to one NovaRocks cluster
/// without ever reading the control-plane cluster name.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorClusterIdentity([u8; 32]);

impl ConnectorClusterIdentity {
    /// Derive the identity from the control-plane cluster id.
    pub fn derive(cluster_id: &str) -> Result<Self, ConnectorError> {
        if cluster_id.is_empty()
            || cluster_id.len() > MAX_CONNECTOR_EXTERNAL_FENCE_CLUSTER_ID_BYTES
            || cluster_id.chars().any(char::is_control)
        {
            return Err(invalid(
                "connector cluster identity must contain 1..=256 non-control bytes",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_CLUSTER_IDENTITY_DOMAIN);
        hasher.update(cluster_id.as_bytes());
        Ok(Self(hasher.finalize().into()))
    }

    /// Rebuild the identity from a durable digest recorded by a previous
    /// coordination attempt. An all-zero digest is rejected as unset.
    pub fn try_from_digest(digest: [u8; 32]) -> Result<Self, ConnectorError> {
        if digest == [0; 32] {
            return Err(invalid("connector cluster identity digest must be set"));
        }
        Ok(Self(digest))
    }

    pub const fn digest(self) -> [u8; 32] {
        self.0
    }
}

/// The totally ordered external fence generation.
///
/// Derived `Ord` is lexicographic in declaration order, which is exactly the
/// control-plane precedence: a new control-plane incarnation outranks any
/// resource epoch, and a new resource epoch outranks any coordination attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorExternalFenceGeneration {
    control_plane_incarnation: u64,
    resource_epoch: u64,
    coordination_attempt: u64,
}

impl ConnectorExternalFenceGeneration {
    pub fn try_new(
        control_plane_incarnation: u64,
        resource_epoch: u64,
        coordination_attempt: u64,
    ) -> Result<Self, ConnectorError> {
        if control_plane_incarnation == 0 || resource_epoch == 0 || coordination_attempt == 0 {
            return Err(invalid(
                "connector external fence generation components must be nonzero",
            ));
        }
        Ok(Self {
            control_plane_incarnation,
            resource_epoch,
            coordination_attempt,
        })
    }

    pub const fn control_plane_incarnation(self) -> u64 {
        self.control_plane_incarnation
    }

    pub const fn resource_epoch(self) -> u64 {
        self.resource_epoch
    }

    pub const fn coordination_attempt(self) -> u64 {
        self.coordination_attempt
    }

    pub fn to_bytes(self) -> [u8; 24] {
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&self.control_plane_incarnation.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.resource_epoch.to_be_bytes());
        bytes[16..].copy_from_slice(&self.coordination_attempt.to_be_bytes());
        bytes
    }
}

/// A complete, digest-sealed external operation fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorExternalOperationFence {
    cluster: ConnectorClusterIdentity,
    generation: ConnectorExternalFenceGeneration,
    operation_id: ConnectorWriteOperationId,
    coordination_attempt_id: [u8; 16],
    table: ConnectorTableIdentity,
    target_ref: ConnectorWriteTargetRef,
    digest: [u8; 32],
}

impl ConnectorExternalOperationFence {
    pub fn try_new(
        cluster: ConnectorClusterIdentity,
        generation: ConnectorExternalFenceGeneration,
        operation_id: ConnectorWriteOperationId,
        coordination_attempt_id: [u8; 16],
        table: ConnectorTableIdentity,
        target_ref: ConnectorWriteTargetRef,
    ) -> Result<Self, ConnectorError> {
        if coordination_attempt_id == [0; 16] {
            return Err(invalid(
                "connector external fence coordination attempt id must be set",
            ));
        }
        validate_identity_component("namespace", &table.namespace)?;
        validate_identity_component("table", &table.table)?;
        target_ref.validate()?;
        let digest = fence_digest(
            cluster,
            generation,
            operation_id,
            coordination_attempt_id,
            &table,
            &target_ref,
        );
        Ok(Self {
            cluster,
            generation,
            operation_id,
            coordination_attempt_id,
            table,
            target_ref,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.cluster,
            self.generation,
            self.operation_id,
            self.coordination_attempt_id,
            self.table.clone(),
            self.target_ref.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector external operation fence digest does not match its contents",
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

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn coordination_attempt_id(&self) -> [u8; 16] {
        self.coordination_attempt_id
    }

    pub fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub fn target_ref(&self) -> &ConnectorWriteTargetRef {
        &self.target_ref
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Whether both fences describe the same fenced authority: one cluster, one
    /// stable write operation, and one resource. Only such a pair may be
    /// compared or replayed; anything else is a foreign-operation conflict.
    pub fn is_same_authority(&self, other: &Self) -> bool {
        self.cluster == other.cluster
            && self.operation_id == other.operation_id
            && self.table == other.table
            && self.target_ref == other.target_ref
    }

    /// Compare two fences of the same authority by generation.
    ///
    /// A fence from another authority is never ordered against this one; that
    /// is a typed foreign-operation conflict rather than an ordering answer.
    pub fn compare_generation(&self, other: &Self) -> Result<Ordering, ConnectorError> {
        if !self.is_same_authority(other) {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::ForeignOperation,
                "connector external operation fences describe different fenced authorities",
            ));
        }
        Ok(self.generation.cmp(&other.generation))
    }

    /// Whether this fence strictly outranks `other` within one authority.
    pub fn supersedes(&self, other: &Self) -> Result<bool, ConnectorError> {
        Ok(self.compare_generation(other)? == Ordering::Greater)
    }

    /// Fail closed unless this fence is a legal successor of `established`: a
    /// strictly higher generation, or the identical fence replayed.
    pub fn validate_monotonic_successor_of(
        &self,
        established: &Self,
    ) -> Result<(), ConnectorError> {
        match self.compare_generation(established)? {
            Ordering::Greater => Ok(()),
            Ordering::Equal if self.digest == established.digest => Ok(()),
            Ordering::Equal => Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Superseded,
                "connector external operation fence reuses an established generation with different contents",
            )),
            Ordering::Less => Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Stale,
                "connector external operation fence generation is behind the established fence",
            )),
        }
    }

    pub fn validate_for_operation(
        &self,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<(), ConnectorError> {
        if self.operation_id != operation_id {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::ForeignOperation,
                "connector external operation fence names another write operation",
            ));
        }
        self.validate()
    }
}

/// The provider-facing request that establishes or raises an external fence.
#[derive(Clone)]
pub struct ConnectorExternalFenceRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub fence: ConnectorExternalOperationFence,
    pub context: ConnectorRequestContext,
}

impl ConnectorExternalFenceRequest {
    /// Validate that this request targets the exact control generation and
    /// carries a sealed fence value.
    pub fn validate(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if &self.owner != owner {
            return Err(invalid(
                "connector external fence request does not match the exact control owner",
            ));
        }
        self.fence.validate()
    }
}

/// A provider acknowledgement that one exact fence is established.
///
/// The payload is an opaque provider container: neither the frontend nor
/// another provider may decode it. Only the digests cross the boundary.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorExternalFenceReceipt {
    fence_digest: [u8; 32],
    generation: ConnectorExternalFenceGeneration,
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorExternalFenceReceipt {
    pub fn try_new(
        fence: &ConnectorExternalOperationFence,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        if payload.is_empty() || payload.len() > MAX_CONNECTOR_EXTERNAL_FENCE_RECEIPT_BYTES {
            return Err(invalid(
                "connector external fence receipt exceeds its bounded payload limit",
            ));
        }
        let digest = receipt_digest(fence.digest(), fence.generation(), &payload);
        Ok(Self {
            fence_digest: fence.digest(),
            generation: fence.generation(),
            payload,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.payload.is_empty()
            || self.payload.len() > MAX_CONNECTOR_EXTERNAL_FENCE_RECEIPT_BYTES
        {
            return Err(invalid(
                "connector external fence receipt exceeds its bounded payload limit",
            ));
        }
        if receipt_digest(self.fence_digest, self.generation, &self.payload) != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector external fence receipt digest does not match its contents",
            ));
        }
        Ok(())
    }

    /// Whether this receipt acknowledges exactly the supplied fence.
    pub fn matches(&self, fence: &ConnectorExternalOperationFence) -> bool {
        self.fence_digest == fence.digest() && self.generation == fence.generation()
    }

    pub const fn fence_digest(&self) -> [u8; 32] {
        self.fence_digest
    }

    pub const fn generation(&self) -> ConnectorExternalFenceGeneration {
        self.generation
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorExternalFenceReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorExternalFenceReceipt")
            .field("fence_digest", &self.fence_digest)
            .field("generation", &self.generation)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

fn validate_identity_component(label: &str, value: &str) -> Result<(), ConnectorError> {
    if value.is_empty()
        || value.len() > MAX_CONNECTOR_EXTERNAL_FENCE_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(invalid(format!(
            "connector external fence resource {label} must contain 1..=4096 non-control bytes"
        )));
    }
    Ok(())
}

fn fence_digest(
    cluster: ConnectorClusterIdentity,
    generation: ConnectorExternalFenceGeneration,
    operation_id: ConnectorWriteOperationId,
    coordination_attempt_id: [u8; 16],
    table: &ConnectorTableIdentity,
    target_ref: &ConnectorWriteTargetRef,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_EXTERNAL_OPERATION_FENCE_DOMAIN);
    hasher.update(cluster.digest());
    hasher.update(generation.to_bytes());
    hasher.update(operation_id.to_bytes());
    hasher.update(coordination_attempt_id);
    hasher.update(table.instance_id.as_str().as_bytes());
    hasher.update(table.namespace.as_bytes());
    hasher.update(table.table.as_bytes());
    hasher.update(target_ref.as_str().as_bytes());
    hasher.finalize().into()
}

fn receipt_digest(
    fence_digest: [u8; 32],
    generation: ConnectorExternalFenceGeneration,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_EXTERNAL_FENCE_RECEIPT_DOMAIN);
    hasher.update(fence_digest);
    hasher.update(generation.to_bytes());
    hasher.update(payload.as_ref());
    hasher.finalize().into()
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::connector::ConnectorInstanceId;

    fn table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("catalog.ice").expect("instance id"),
            namespace: Arc::from("db"),
            table: Arc::from("orders"),
        }
    }

    pub(crate) fn fence(
        operation_id: ConnectorWriteOperationId,
        incarnation: u64,
        epoch: u64,
        attempt: u64,
    ) -> ConnectorExternalOperationFence {
        ConnectorExternalOperationFence::try_new(
            ConnectorClusterIdentity::derive("nova-cluster").expect("cluster identity"),
            ConnectorExternalFenceGeneration::try_new(incarnation, epoch, attempt)
                .expect("generation"),
            operation_id,
            [3; 16],
            table(),
            ConnectorWriteTargetRef::main(),
        )
        .expect("fence")
    }

    #[test]
    fn cluster_identity_rejects_malformed_and_unset_inputs() {
        assert!(ConnectorClusterIdentity::derive("").is_err());
        assert!(ConnectorClusterIdentity::derive("bad\u{0}id").is_err());
        assert!(
            ConnectorClusterIdentity::derive(
                &"x".repeat(MAX_CONNECTOR_EXTERNAL_FENCE_CLUSTER_ID_BYTES + 1)
            )
            .is_err()
        );
        assert!(ConnectorClusterIdentity::try_from_digest([0; 32]).is_err());
        let derived = ConnectorClusterIdentity::derive("nova-cluster").expect("cluster identity");
        assert_eq!(
            ConnectorClusterIdentity::try_from_digest(derived.digest()).expect("rebuilt"),
            derived
        );
    }

    #[test]
    fn generation_components_must_be_nonzero_and_totally_ordered() {
        assert!(ConnectorExternalFenceGeneration::try_new(0, 1, 1).is_err());
        assert!(ConnectorExternalFenceGeneration::try_new(1, 0, 1).is_err());
        assert!(ConnectorExternalFenceGeneration::try_new(1, 1, 0).is_err());
        let base = ConnectorExternalFenceGeneration::try_new(1, 1, 1).expect("base");
        let attempt = ConnectorExternalFenceGeneration::try_new(1, 1, 2).expect("attempt");
        let epoch = ConnectorExternalFenceGeneration::try_new(1, 2, 1).expect("epoch");
        let incarnation = ConnectorExternalFenceGeneration::try_new(2, 1, 1).expect("incarnation");
        assert!(base < attempt);
        assert!(attempt < epoch);
        assert!(epoch < incarnation);
    }

    #[test]
    fn fence_rejects_malformed_identity_components() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let generation =
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).expect("fence generation");
        let cluster = ConnectorClusterIdentity::derive("nova-cluster").expect("cluster identity");
        assert!(
            ConnectorExternalOperationFence::try_new(
                cluster,
                generation,
                operation_id,
                [0; 16],
                table(),
                ConnectorWriteTargetRef::main(),
            )
            .is_err()
        );
        let mut empty_namespace = table();
        empty_namespace.namespace = Arc::from("");
        assert!(
            ConnectorExternalOperationFence::try_new(
                cluster,
                generation,
                operation_id,
                [3; 16],
                empty_namespace,
                ConnectorWriteTargetRef::main(),
            )
            .is_err()
        );
        let mut oversized_table = table();
        oversized_table.table = Arc::from(
            "t".repeat(MAX_CONNECTOR_EXTERNAL_FENCE_IDENTITY_BYTES + 1)
                .as_str(),
        );
        assert!(
            ConnectorExternalOperationFence::try_new(
                cluster,
                generation,
                operation_id,
                [3; 16],
                oversized_table,
                ConnectorWriteTargetRef::main(),
            )
            .is_err()
        );
    }

    #[test]
    fn fence_digest_seals_every_bound_component() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let sealed = fence(operation_id, 1, 1, 1);
        sealed.validate().expect("sealed fence validates");
        let mut corrupted = sealed.clone();
        corrupted.coordination_attempt_id = [4; 16];
        assert!(corrupted.validate().is_err());
        let mut retargeted = sealed.clone();
        retargeted.target_ref = ConnectorWriteTargetRef::parse("audit").expect("branch");
        assert!(retargeted.validate().is_err());
        assert_ne!(sealed.digest(), fence(operation_id, 1, 1, 2).digest());
    }

    #[test]
    fn fence_comparison_is_total_within_one_authority_and_typed_across_operations() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let low = fence(operation_id, 1, 1, 1);
        let high = fence(operation_id, 1, 2, 1);
        assert_eq!(
            high.compare_generation(&low).expect("comparable"),
            Ordering::Greater
        );
        assert!(high.supersedes(&low).expect("comparable"));
        assert!(!low.supersedes(&high).expect("comparable"));

        let foreign = fence(ConnectorWriteOperationId::from_bytes([2; 16]), 1, 9, 9);
        let error = foreign
            .compare_generation(&low)
            .expect_err("a foreign operation is not ordered");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::ForeignOperation)
        );
        assert!(!error.retryable_before_progress());
    }

    #[test]
    fn monotonic_successor_rejects_a_stale_generation_as_typed_stale() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let established = fence(operation_id, 1, 2, 1);
        let stale = fence(operation_id, 1, 1, 1);
        let error = stale
            .validate_monotonic_successor_of(&established)
            .expect_err("a lower generation cannot supersede an established fence");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );
        assert!(!error.retryable_before_progress());
        assert!(
            !matches!(error.kind(), ConnectorErrorKind::Unsupported),
            "a fence conflict must never be reported as unsupported"
        );
        established
            .validate_monotonic_successor_of(&established)
            .expect("the identical fence replay is idempotent");
        fence(operation_id, 1, 3, 1)
            .validate_monotonic_successor_of(&established)
            .expect("a higher generation supersedes");
    }

    #[test]
    fn receipt_binds_one_fence_rejects_unbounded_payloads_and_redacts_debug() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let sealed = fence(operation_id, 1, 1, 1);
        assert!(ConnectorExternalFenceReceipt::try_new(&sealed, Bytes::new()).is_err());
        assert!(
            ConnectorExternalFenceReceipt::try_new(
                &sealed,
                Bytes::from(vec![0; MAX_CONNECTOR_EXTERNAL_FENCE_RECEIPT_BYTES + 1]),
            )
            .is_err()
        );
        let receipt =
            ConnectorExternalFenceReceipt::try_new(&sealed, Bytes::from_static(b"fence-marker"))
                .expect("receipt");
        receipt.validate().expect("receipt validates");
        assert!(receipt.matches(&sealed));
        assert!(!receipt.matches(&fence(operation_id, 1, 2, 1)));
        let debug = format!("{receipt:?}");
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("fence-marker"));

        let mut corrupted = receipt;
        corrupted.payload = Bytes::from_static(b"other-marker");
        assert!(corrupted.validate().is_err());
    }
}
