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

//! FE-only provider-neutral connector orphan-cleanup contract.
//! Design: ADR-0035 (docs/adr/ADR-0035-connector-orphan-cleanup-reconcile-contract.md)

use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMetadata, ConnectorRequestContext,
    ConnectorTableHandle,
};

pub const CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION: u16 = 1;
pub const MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS: usize = 1024;
pub const MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_CLEANUP_BATCH_OBJECTS: u32 = 1024;
pub const MAX_CONNECTOR_CLEANUP_BATCHES: u32 = 256;

pub const REMOVE_UNREFERENCED_OBJECTS_KIND: &str = "remove-unreferenced-objects";

const REQUEST_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.request.v1\0";
const PLAN_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.plan.v1\0";
const PREPARED_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.prepared.v1\0";
const RECEIPT_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.receipt.v1\0";
const PREPARED_WIRE_MAGIC: &[u8; 8] = b"NRCLEAN1";

/// Durable identity selected by the frontend operation owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCleanupOperationId(Uuid);

impl ConnectorCleanupOperationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl Default for ConnectorCleanupOperationId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorCleanupOperation {
    RemoveUnreferencedObjects {
        table: ConnectorTableHandle,
        older_than_ms: i64,
    },
}

impl ConnectorCleanupOperation {
    pub fn remove_unreferenced_objects(
        table: ConnectorTableHandle,
        older_than_ms: i64,
    ) -> Result<Self, ConnectorError> {
        let operation = Self::RemoveUnreferencedObjects {
            table,
            older_than_ms,
        };
        operation.validate()?;
        Ok(operation)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RemoveUnreferencedObjects { .. } => REMOVE_UNREFERENCED_OBJECTS_KIND,
        }
    }

    pub const fn table(&self) -> &ConnectorTableHandle {
        match self {
            Self::RemoveUnreferencedObjects { table, .. } => table,
        }
    }

    pub const fn older_than_ms(&self) -> i64 {
        match self {
            Self::RemoveUnreferencedObjects { older_than_ms, .. } => *older_than_ms,
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.older_than_ms() <= 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "orphan cleanup older-than timestamp must be positive",
            ));
        }
        Ok(())
    }

    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.kind().as_bytes());
        digest_bytes(hash, self.table().owner().as_str().as_bytes());
        digest_bytes(hash, self.table().payload());
        hash.update(self.older_than_ms().to_be_bytes());
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupPlanningRequest {
    operation_id: ConnectorCleanupOperationId,
    owner: ConnectorExecutionBindingKey,
    operation: ConnectorCleanupOperation,
    request_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorCleanupPlanningRequest {
    pub fn try_new(
        operation_id: ConnectorCleanupOperationId,
        owner: ConnectorExecutionBindingKey,
        operation: ConnectorCleanupOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup table handle does not match exact owner",
            ));
        }
        let request_digest = request_digest(operation_id, &owner, &operation);
        Ok(Self {
            operation_id,
            owner,
            operation,
            request_digest,
            context,
        })
    }

    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn operation(&self) -> &ConnectorCleanupOperation {
        &self.operation
    }

    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.operation.validate()?;
        if self.operation.table().owner() != &self.owner.instance_id
            || request_digest(self.operation_id, &self.owner, &self.operation)
                != self.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup request owner or digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorCleanupPlanningRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCleanupPlanningRequest")
            .field("operation_id", &self.operation_id)
            .field("owner", &self.owner)
            .field("operation_kind", &self.operation.kind())
            .field("request_digest", &self.request_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorCleanupPlanSummary {
    candidate_count: u64,
    total_bytes: u64,
    manifest_parts: u32,
    batch_count: u32,
}

impl ConnectorCleanupPlanSummary {
    pub fn try_new(
        candidate_count: u64,
        total_bytes: u64,
        manifest_parts: u32,
        batch_count: u32,
    ) -> Result<Self, ConnectorError> {
        if batch_count > MAX_CONNECTOR_CLEANUP_BATCHES
            || manifest_parts > 64
            || (candidate_count == 0 && batch_count != 0)
            || (candidate_count > 0 && batch_count == 0)
            || candidate_count
                > u64::from(MAX_CONNECTOR_CLEANUP_BATCHES)
                    * u64::from(MAX_CONNECTOR_CLEANUP_BATCH_OBJECTS)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup plan exceeds its bounded manifest or batch budget",
            ));
        }
        Ok(Self {
            candidate_count,
            total_bytes,
            manifest_parts,
            batch_count,
        })
    }

    pub const fn candidate_count(self) -> u64 {
        self.candidate_count
    }
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }
    pub const fn manifest_parts(self) -> u32 {
        self.manifest_parts
    }
    pub const fn batch_count(self) -> u32 {
        self.batch_count
    }

    fn digest_into(self, hash: &mut Sha256) {
        hash.update(self.candidate_count.to_be_bytes());
        hash.update(self.total_bytes.to_be_bytes());
        hash.update(self.manifest_parts.to_be_bytes());
        hash.update(self.batch_count.to_be_bytes());
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCleanupPlan {
    schema_version: u16,
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorCleanupOperationId,
    request_digest: [u8; 32],
    base_state_digest: [u8; 32],
    manifest_digest: [u8; 32],
    summary: ConnectorCleanupPlanSummary,
    provider_payload: Bytes,
    plan_digest: [u8; 32],
}

impl ConnectorCleanupPlan {
    pub fn try_new(
        request: &ConnectorCleanupPlanningRequest,
        base_state_digest: [u8; 32],
        manifest_digest: [u8; 32],
        summary: ConnectorCleanupPlanSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_payload(&provider_payload, "plan")?;
        let plan_digest = plan_digest(
            request.request_digest,
            base_state_digest,
            manifest_digest,
            summary,
            &provider_payload,
        );
        Ok(Self {
            schema_version: CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION,
            owner: request.owner.clone(),
            operation_id: request.operation_id,
            request_digest: request.request_digest,
            base_state_digest,
            manifest_digest,
            summary,
            provider_payload,
            plan_digest,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_restore(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorCleanupOperationId,
        request_digest: [u8; 32],
        base_state_digest: [u8; 32],
        manifest_digest: [u8; 32],
        summary: ConnectorCleanupPlanSummary,
        provider_payload: Bytes,
        plan_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        let plan = Self {
            schema_version: CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION,
            owner,
            operation_id,
            request_digest,
            base_state_digest,
            manifest_digest,
            summary,
            provider_payload,
            plan_digest,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    pub const fn base_state_digest(&self) -> [u8; 32] {
        self.base_state_digest
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn summary(&self) -> ConnectorCleanupPlanSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        ConnectorCleanupPlanSummary::try_new(
            self.summary.candidate_count,
            self.summary.total_bytes,
            self.summary.manifest_parts,
            self.summary.batch_count,
        )?;
        validate_payload(&self.provider_payload, "plan")?;
        if self.schema_version != CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION
            || plan_digest(
                self.request_digest,
                self.base_state_digest,
                self.manifest_digest,
                self.summary,
                &self.provider_payload,
            ) != self.plan_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup plan digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorCleanupPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCleanupPlan")
            .field("schema_version", &self.schema_version)
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("request_digest", &self.request_digest)
            .field("base_state_digest", &self.base_state_digest)
            .field("manifest_digest", &self.manifest_digest)
            .field("summary", &self.summary)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("plan_digest", &self.plan_digest)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PreparedBatch {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorCleanupOperationId,
    plan_digest: [u8; 32],
    manifest_digest: [u8; 32],
    batch_ordinal: u32,
    batch_digest: [u8; 32],
    evidence_payload: Bytes,
    evidence_digest: [u8; 32],
}

impl PreparedBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorCleanupOperationId,
        plan_digest: [u8; 32],
        manifest_digest: [u8; 32],
        batch_ordinal: u32,
        batch_digest: [u8; 32],
        evidence_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if batch_ordinal >= MAX_CONNECTOR_CLEANUP_BATCHES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup batch ordinal exceeds the hard limit",
            ));
        }
        validate_payload(&evidence_payload, "prepared-batch evidence")?;
        let evidence_digest = prepared_digest(
            &owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            &evidence_payload,
        );
        Ok(Self {
            owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            evidence_payload,
            evidence_digest,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn batch_ordinal(&self) -> u32 {
        self.batch_ordinal
    }
    pub const fn batch_digest(&self) -> [u8; 32] {
        self.batch_digest
    }
    pub fn evidence_payload(&self) -> &Bytes {
        &self.evidence_payload
    }
    pub const fn evidence_digest(&self) -> [u8; 32] {
        self.evidence_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.batch_ordinal >= MAX_CONNECTOR_CLEANUP_BATCHES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup batch ordinal exceeds the hard limit",
            ));
        }
        validate_payload(&self.evidence_payload, "prepared-batch evidence")?;
        if prepared_digest(
            &self.owner,
            self.operation_id,
            self.plan_digest,
            self.manifest_digest,
            self.batch_ordinal,
            self.batch_digest,
            &self.evidence_payload,
        ) != self.evidence_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared-batch evidence digest is invalid",
            ));
        }
        Ok(())
    }

    /// Bounded opaque durable carrier for the frontend operation owner. It
    /// contains no candidate locations or object identities; only the exact
    /// binding, frozen digests, and provider-produced prepare evidence needed
    /// to reconcile the same batch after a restart.
    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        self.validate()?;
        let instance = self.owner.instance_id.as_str().as_bytes();
        let instance_len = u16::try_from(instance.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup owner exceeds durable wire limit",
            )
        })?;
        let evidence_len = u32::try_from(self.evidence_payload.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup evidence exceeds durable wire limit",
            )
        })?;
        let mut output = Vec::with_capacity(
            8 + 2 + instance.len() + 16 + 16 + 32 * 4 + 4 + self.evidence_payload.len(),
        );
        output.extend_from_slice(PREPARED_WIRE_MAGIC);
        output.extend_from_slice(&CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
        output.extend_from_slice(&instance_len.to_be_bytes());
        output.extend_from_slice(instance);
        output.extend_from_slice(&self.owner.incarnation.to_bytes());
        output.extend_from_slice(&self.operation_id.to_bytes());
        output.extend_from_slice(&self.plan_digest);
        output.extend_from_slice(&self.manifest_digest);
        output.extend_from_slice(&self.batch_ordinal.to_be_bytes());
        output.extend_from_slice(&self.batch_digest);
        output.extend_from_slice(&self.evidence_digest);
        output.extend_from_slice(&evidence_len.to_be_bytes());
        output.extend_from_slice(&self.evidence_payload);
        if output.len() > MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup prepared wire exceeds the hard limit",
            ));
        }
        Ok(Bytes::from(output))
    }

    pub fn try_from_wire_v1(value: Bytes) -> Result<Self, ConnectorError> {
        if value.len() > MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES
            || value.len() < 8 + 2 + 2 + 16 + 16 + 32 * 4 + 4 + 4
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire length is invalid",
            ));
        }
        let mut cursor = 0usize;
        let take = |count: usize, cursor: &mut usize| -> Result<&[u8], ConnectorError> {
            let end = cursor.checked_add(count).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "cleanup prepared wire overflows",
                )
            })?;
            let part = value.get(*cursor..end).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "cleanup prepared wire is truncated",
                )
            })?;
            *cursor = end;
            Ok(part)
        };
        if take(8, &mut cursor)? != PREPARED_WIRE_MAGIC
            || take(2, &mut cursor)? != CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire version is invalid",
            ));
        }
        let instance_len = u16::from_be_bytes(take(2, &mut cursor)?.try_into().unwrap()) as usize;
        let instance = std::str::from_utf8(take(instance_len, &mut cursor)?).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared owner is not UTF-8",
            )
        })?;
        let incarnation =
            ConnectorInstanceIncarnation::from_bytes(take(16, &mut cursor)?.try_into().unwrap());
        let operation_id =
            ConnectorCleanupOperationId::from_bytes(take(16, &mut cursor)?.try_into().unwrap());
        let plan_digest = take(32, &mut cursor)?.try_into().unwrap();
        let manifest_digest = take(32, &mut cursor)?.try_into().unwrap();
        let batch_ordinal = u32::from_be_bytes(take(4, &mut cursor)?.try_into().unwrap());
        let batch_digest = take(32, &mut cursor)?.try_into().unwrap();
        let evidence_digest: [u8; 32] = take(32, &mut cursor)?.try_into().unwrap();
        let evidence_len = u32::from_be_bytes(take(4, &mut cursor)?.try_into().unwrap()) as usize;
        let evidence_payload = Bytes::copy_from_slice(take(evidence_len, &mut cursor)?);
        if cursor != value.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire has trailing bytes",
            ));
        }
        let prepared = Self::try_new(
            ConnectorExecutionBindingKey {
                instance_id: ConnectorInstanceId::parse(instance)?,
                incarnation,
            },
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            evidence_payload,
        )?;
        if prepared.evidence_digest != evidence_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire evidence digest is invalid",
            ));
        }
        Ok(prepared)
    }
}

impl fmt::Debug for PreparedBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedBatch")
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("batch_ordinal", &self.batch_ordinal)
            .field("batch_digest", &self.batch_digest)
            .field("evidence_payload_len", &self.evidence_payload.len())
            .field("evidence_digest", &self.evidence_digest)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchReceiptSummary {
    deleted: u32,
    already_absent: u32,
    failed: u32,
    unknown: u32,
}

impl BatchReceiptSummary {
    pub const fn new(deleted: u32, already_absent: u32, failed: u32, unknown: u32) -> Self {
        Self {
            deleted,
            already_absent,
            failed,
            unknown,
        }
    }
    pub const fn deleted(self) -> u32 {
        self.deleted
    }
    pub const fn already_absent(self) -> u32 {
        self.already_absent
    }
    pub const fn failed(self) -> u32 {
        self.failed
    }
    pub const fn unknown(self) -> u32 {
        self.unknown
    }
    pub const fn total(self) -> u32 {
        self.deleted + self.already_absent + self.failed + self.unknown
    }

    fn digest_into(self, hash: &mut Sha256) {
        hash.update(self.deleted.to_be_bytes());
        hash.update(self.already_absent.to_be_bytes());
        hash.update(self.failed.to_be_bytes());
        hash.update(self.unknown.to_be_bytes());
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct BatchReceipt {
    descriptor: ConnectorInstanceDescriptor,
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorCleanupOperationId,
    plan_digest: [u8; 32],
    manifest_digest: [u8; 32],
    batch_ordinal: u32,
    batch_digest: [u8; 32],
    summary: BatchReceiptSummary,
    provider_payload: Bytes,
    receipt_digest: [u8; 32],
}

impl BatchReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorCleanupOperationId,
        plan_digest: [u8; 32],
        manifest_digest: [u8; 32],
        batch_ordinal: u32,
        batch_digest: [u8; 32],
        summary: BatchReceiptSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if descriptor.instance_id != owner.instance_id
            || batch_ordinal >= MAX_CONNECTOR_CLEANUP_BATCHES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup receipt owner or ordinal is invalid",
            ));
        }
        validate_payload(&provider_payload, "receipt")?;
        let receipt_digest = receipt_digest(
            &descriptor,
            &owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            summary,
            &provider_payload,
        );
        Ok(Self {
            descriptor,
            owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            summary,
            provider_payload,
            receipt_digest,
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn batch_ordinal(&self) -> u32 {
        self.batch_ordinal
    }
    pub const fn batch_digest(&self) -> [u8; 32] {
        self.batch_digest
    }
    pub const fn summary(&self) -> BatchReceiptSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn receipt_digest(&self) -> [u8; 32] {
        self.receipt_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.descriptor.clone(),
            self.owner.clone(),
            self.operation_id,
            self.plan_digest,
            self.manifest_digest,
            self.batch_ordinal,
            self.batch_digest,
            self.summary,
            self.provider_payload.clone(),
        )?;
        if expected.receipt_digest != self.receipt_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup receipt digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for BatchReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchReceipt")
            .field("descriptor", &self.descriptor)
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("batch_ordinal", &self.batch_ordinal)
            .field("summary", &self.summary)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("receipt_digest", &self.receipt_digest)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidatePage {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorCleanupOperationId,
    manifest_digest: [u8; 32],
    offset: u64,
    locations: Vec<Arc<str>>,
    complete: bool,
}

impl CandidatePage {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorCleanupOperationId,
        manifest_digest: [u8; 32],
        offset: u64,
        locations: Vec<Arc<str>>,
        complete: bool,
    ) -> Result<Self, ConnectorError> {
        let page = Self {
            owner,
            operation_id,
            manifest_digest,
            offset,
            locations,
            complete,
        };
        page.validate()?;
        Ok(page)
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn offset(&self) -> u64 {
        self.offset
    }
    pub fn locations(&self) -> &[Arc<str>] {
        &self.locations
    }
    pub const fn complete(&self) -> bool {
        self.complete
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.locations.len() > MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS
            || self.locations.iter().any(|location| location.is_empty())
            || self.locations.windows(2).any(|pair| pair[0] >= pair[1])
            || self
                .locations
                .iter()
                .map(|location| location.len())
                .sum::<usize>()
                > MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup candidate page is invalid or exceeds its hard limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupPrepareRequest {
    pub plan: ConnectorCleanupPlan,
    pub batch_ordinal: u32,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupPrepareRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        batch_ordinal: u32,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if batch_ordinal >= plan.summary.batch_count {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup batch ordinal is outside the frozen plan",
            ));
        }
        Ok(Self {
            plan,
            batch_ordinal,
            context,
        })
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupExecuteRequest {
    pub plan: ConnectorCleanupPlan,
    pub prepared: PreparedBatch,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupExecuteRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        prepared.validate()?;
        validate_prepared_for_plan(&plan, &prepared)?;
        Ok(Self {
            plan,
            prepared,
            context,
        })
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupReconcileRequest {
    pub plan: ConnectorCleanupPlan,
    pub prepared: PreparedBatch,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupReconcileRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        ConnectorCleanupExecuteRequest::try_new(plan.clone(), prepared.clone(), context.clone())?;
        Ok(Self {
            plan,
            prepared,
            context,
        })
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupCandidatePageRequest {
    pub plan: ConnectorCleanupPlan,
    pub offset: u64,
    pub limit: u32,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupCandidatePageRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        offset: u64,
        limit: u32,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if limit == 0 || limit as usize > MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup candidate page limit is invalid",
            ));
        }
        Ok(Self {
            plan,
            offset,
            limit,
            context,
        })
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupFinalizeRequest {
    pub plan: ConnectorCleanupPlan,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupFinalizeRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        Ok(Self { plan, context })
    }
}

/// A cleanup provider never receives an opportunity to re-list candidates after
/// planning. `execute_batch` is invoked once per prepared batch; any uncertain
/// response is repaired exclusively through `reconcile_batch`.
pub trait ConnectorCleanupMaintenance: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;
    fn plan_cleanup(
        &self,
        request: ConnectorCleanupPlanningRequest,
    ) -> Result<ConnectorCleanupPlan, ConnectorError>;
    fn prepare_batch(
        &self,
        request: ConnectorCleanupPrepareRequest,
    ) -> Result<PreparedBatch, ConnectorError>;
    fn execute_batch(
        &self,
        request: ConnectorCleanupExecuteRequest,
    ) -> Result<BatchReceipt, ConnectorError>;
    fn reconcile_batch(
        &self,
        request: ConnectorCleanupReconcileRequest,
    ) -> Result<BatchReceipt, ConnectorError>;
    fn read_candidate_page(
        &self,
        request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<CandidatePage, ConnectorError>;
    fn finalize_terminal(
        &self,
        request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError>;
}

pub trait ConnectorCleanupMaintenanceResolver: Send + Sync {
    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError>;
    fn acquire_exact_cleanup_maintenance(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorCleanupMaintenanceLease {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
    metadata: Arc<dyn ConnectorMetadata>,
    cleanup: Arc<dyn ConnectorCleanupMaintenance>,
    _release: Arc<CleanupRelease>,
}
struct CleanupRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorCleanupMaintenanceLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
        metadata: Arc<dyn ConnectorMetadata>,
        cleanup: Arc<dyn ConnectorCleanupMaintenance>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if descriptor.instance_id != key.instance_id
            || metadata.instance_id() != &descriptor.instance_id
            || cleanup.descriptor() != &descriptor
            || cleanup.binding_key() != &key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup capabilities do not match lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            key,
            metadata,
            cleanup,
            _release: Arc::new(CleanupRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }
    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }
    pub fn plan_cleanup(
        &self,
        request: ConnectorCleanupPlanningRequest,
    ) -> Result<ConnectorCleanupPlan, ConnectorError> {
        request.validate()?;
        if request.owner != self.key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup request does not match lease",
            ));
        }
        let plan = self.cleanup.plan_cleanup(request.clone())?;
        plan.validate()?;
        if plan.owner != self.key
            || plan.operation_id != request.operation_id
            || plan.request_digest != request.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup plan does not match request",
            ));
        }
        Ok(plan)
    }
    pub fn prepare_batch(
        &self,
        request: ConnectorCleanupPrepareRequest,
    ) -> Result<PreparedBatch, ConnectorError> {
        self.validate_plan(&request.plan)?;
        let prepared = self.cleanup.prepare_batch(request.clone())?;
        prepared.validate()?;
        validate_prepared_for_plan(&request.plan, &prepared)?;
        if prepared.batch_ordinal != request.batch_ordinal {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup prepared batch does not match request ordinal",
            ));
        }
        Ok(prepared)
    }
    pub fn execute_batch(
        &self,
        request: ConnectorCleanupExecuteRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        self.validate_plan(&request.plan)?;
        validate_prepared_for_plan(&request.plan, &request.prepared)?;
        let receipt = self.cleanup.execute_batch(request.clone())?;
        self.validate_receipt(&request.plan, &request.prepared, &receipt)?;
        Ok(receipt)
    }
    pub fn reconcile_batch(
        &self,
        request: ConnectorCleanupReconcileRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        self.validate_plan(&request.plan)?;
        validate_prepared_for_plan(&request.plan, &request.prepared)?;
        let receipt = self.cleanup.reconcile_batch(request.clone())?;
        self.validate_receipt(&request.plan, &request.prepared, &receipt)?;
        Ok(receipt)
    }
    pub fn read_candidate_page(
        &self,
        request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<CandidatePage, ConnectorError> {
        self.validate_plan(&request.plan)?;
        let page = self.cleanup.read_candidate_page(request.clone())?;
        page.validate()?;
        if page.owner != self.key
            || page.operation_id != request.plan.operation_id
            || page.manifest_digest != request.plan.manifest_digest
            || page.offset != request.offset
            || page.locations.len() > request.limit as usize
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup candidate page does not match frozen plan",
            ));
        }
        Ok(page)
    }
    pub fn finalize_terminal(
        &self,
        request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError> {
        self.validate_plan(&request.plan)?;
        self.cleanup.finalize_terminal(request)
    }
    fn validate_plan(&self, plan: &ConnectorCleanupPlan) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner != self.key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup plan does not match lease",
            ));
        }
        Ok(())
    }
    fn validate_receipt(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
        receipt: &BatchReceipt,
    ) -> Result<(), ConnectorError> {
        receipt.validate()?;
        if receipt.descriptor != self.descriptor
            || receipt.owner != self.key
            || receipt.operation_id != plan.operation_id
            || receipt.plan_digest != plan.plan_digest
            || receipt.manifest_digest != plan.manifest_digest
            || receipt.batch_ordinal != prepared.batch_ordinal
            || receipt.batch_digest != prepared.batch_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup receipt does not match prepared batch",
            ));
        }
        Ok(())
    }
}

impl Drop for CleanupRelease {
    fn drop(&mut self) {
        if let Ok(mut release) = self.release.lock()
            && let Some(release) = release.take()
        {
            release();
        }
    }
}

pub(crate) fn validate_cleanup_maintenance_owner(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorExecutionBindingKey,
    cleanup: &dyn ConnectorCleanupMaintenance,
) -> Result<(), ConnectorError> {
    if cleanup.descriptor() != descriptor || cleanup.binding_key() != key {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "cleanup capability owner does not match control binding",
        ));
    }
    Ok(())
}

fn validate_prepared_for_plan(
    plan: &ConnectorCleanupPlan,
    prepared: &PreparedBatch,
) -> Result<(), ConnectorError> {
    prepared.validate()?;
    if prepared.owner != plan.owner
        || prepared.operation_id != plan.operation_id
        || prepared.plan_digest != plan.plan_digest
        || prepared.manifest_digest != plan.manifest_digest
        || prepared.batch_ordinal >= plan.summary.batch_count
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "cleanup prepared batch does not match frozen plan",
        ));
    }
    Ok(())
}

fn request_digest(
    id: ConnectorCleanupOperationId,
    owner: &ConnectorExecutionBindingKey,
    operation: &ConnectorCleanupOperation,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(REQUEST_DOMAIN);
    hash.update(CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
    hash.update(id.to_bytes());
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    operation.digest_into(&mut hash);
    hash.finalize().into()
}
fn plan_digest(
    request: [u8; 32],
    base: [u8; 32],
    manifest: [u8; 32],
    summary: ConnectorCleanupPlanSummary,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PLAN_DOMAIN);
    hash.update(CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
    hash.update(request);
    hash.update(base);
    hash.update(manifest);
    summary.digest_into(&mut hash);
    digest_bytes(&mut hash, payload);
    hash.finalize().into()
}
fn prepared_digest(
    owner: &ConnectorExecutionBindingKey,
    id: ConnectorCleanupOperationId,
    plan: [u8; 32],
    manifest: [u8; 32],
    ordinal: u32,
    batch: [u8; 32],
    payload: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PREPARED_DOMAIN);
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    hash.update(id.to_bytes());
    hash.update(plan);
    hash.update(manifest);
    hash.update(ordinal.to_be_bytes());
    hash.update(batch);
    digest_bytes(&mut hash, payload);
    hash.finalize().into()
}
#[allow(clippy::too_many_arguments)]
fn receipt_digest(
    descriptor: &ConnectorInstanceDescriptor,
    owner: &ConnectorExecutionBindingKey,
    id: ConnectorCleanupOperationId,
    plan: [u8; 32],
    manifest: [u8; 32],
    ordinal: u32,
    batch: [u8; 32],
    summary: BatchReceiptSummary,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(RECEIPT_DOMAIN);
    digest_bytes(&mut hash, descriptor.provider_id.as_str().as_bytes());
    digest_bytes(&mut hash, descriptor.instance_id.as_str().as_bytes());
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    hash.update(id.to_bytes());
    hash.update(plan);
    hash.update(manifest);
    hash.update(ordinal.to_be_bytes());
    hash.update(batch);
    summary.digest_into(&mut hash);
    digest_bytes(&mut hash, payload);
    hash.finalize().into()
}
fn validate_payload(payload: &Bytes, kind: &str) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("cleanup {kind} payload exceeds the hard limit"),
        ));
    }
    Ok(())
}
fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::connector::{
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorProviderId,
    };

    fn owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.main").unwrap(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        }
    }

    #[test]
    fn rejects_corrupt_prepared_evidence() {
        let prepared = PreparedBatch::try_new(
            owner(),
            ConnectorCleanupOperationId::new(),
            [1; 32],
            [2; 32],
            0,
            [3; 32],
            Bytes::from_static(b"evidence"),
        )
        .unwrap();
        let mut corrupt = prepared.clone();
        corrupt.evidence_payload = Bytes::from_static(b"other");
        assert_eq!(
            corrupt.validate().unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn candidate_page_is_canonical_and_bounded() {
        let page = CandidatePage::try_new(
            owner(),
            ConnectorCleanupOperationId::new(),
            [0; 32],
            0,
            vec![Arc::from("s3://bucket/a"), Arc::from("s3://bucket/b")],
            true,
        );
        assert!(page.is_ok());
        let noncanonical = CandidatePage::try_new(
            owner(),
            ConnectorCleanupOperationId::new(),
            [0; 32],
            0,
            vec![Arc::from("b"), Arc::from("a")],
            true,
        );
        assert!(noncanonical.is_err());
    }

    #[test]
    fn receipt_keeps_unknown_explicit() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: owner().instance_id.clone(),
        };
        let receipt = BatchReceipt::try_new(
            descriptor,
            owner(),
            ConnectorCleanupOperationId::new(),
            [1; 32],
            [2; 32],
            0,
            [3; 32],
            BatchReceiptSummary::new(1, 2, 3, 4),
            Bytes::new(),
        )
        .unwrap();
        assert_eq!(receipt.summary().unknown(), 4);
        assert!(receipt.validate().is_ok());
    }
}
