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

//! FE-only provider-neutral metadata maintenance contract.
//! Design: ADR-0028 (docs/adr/ADR-0028-connector-metadata-maintenance-control-contract.md)

use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorMetadata, ConnectorMutationOperationId,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableHandle,
    ExternalMutationEvidence, ExternalMutationOutcome, ProviderBindingEpoch,
};

pub const CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION: u16 = 1;
pub const MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES: usize = 16 * 1024;
pub const MAX_CONNECTOR_METADATA_MAINTENANCE_PATH_BYTES: usize = 16 * 1024;

const REQUEST_DOMAIN: &[u8] = b"novarocks.connector-metadata-maintenance.request.v1\0";
const PLAN_DOMAIN: &[u8] = b"novarocks.connector-metadata-maintenance.plan.v1\0";
const RECEIPT_DOMAIN: &[u8] = b"novarocks.connector-metadata-maintenance.receipt.v1\0";

pub const REWRITE_METADATA_LAYOUT_KIND: &str = "rewrite-metadata-layout";
pub const EXPIRE_TABLE_VERSIONS_KIND: &str = "expire-table-versions";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorMetadataMaintenanceOperation {
    RewriteMetadataLayout {
        table: ConnectorTableHandle,
    },
    ExpireTableVersions {
        table: ConnectorTableHandle,
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
}

impl ConnectorMetadataMaintenanceOperation {
    pub fn rewrite_metadata_layout(table: ConnectorTableHandle) -> Result<Self, ConnectorError> {
        Ok(Self::RewriteMetadataLayout { table })
    }

    pub fn expire_table_versions(
        table: ConnectorTableHandle,
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    ) -> Result<Self, ConnectorError> {
        let value = Self::ExpireTableVersions {
            table,
            older_than_ms,
            retain_last,
        };
        value.validate()?;
        Ok(value)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RewriteMetadataLayout { .. } => REWRITE_METADATA_LAYOUT_KIND,
            Self::ExpireTableVersions { .. } => EXPIRE_TABLE_VERSIONS_KIND,
        }
    }

    pub const fn table(&self) -> &ConnectorTableHandle {
        match self {
            Self::RewriteMetadataLayout { table } | Self::ExpireTableVersions { table, .. } => {
                table
            }
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Self::ExpireTableVersions {
            older_than_ms,
            retain_last,
            ..
        } = self
        {
            if older_than_ms.is_none() && retain_last.is_none() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "expire table versions requires older_than or retain_last",
                ));
            }
            if retain_last.is_some_and(|value| value == 0) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "expire table versions retain_last must be positive",
                ));
            }
        }
        Ok(())
    }

    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.kind().as_bytes());
        digest_bytes(hash, self.table().owner().as_str().as_bytes());
        digest_bytes(hash, self.table().payload());
        if let Self::ExpireTableVersions {
            older_than_ms,
            retain_last,
            ..
        } = self
        {
            hash.update(older_than_ms.unwrap_or_default().to_be_bytes());
            hash.update([u8::from(older_than_ms.is_some())]);
            hash.update(retain_last.unwrap_or_default().to_be_bytes());
            hash.update([u8::from(retain_last.is_some())]);
        }
    }
}

#[derive(Clone)]
pub struct ConnectorMetadataMaintenancePlanningRequest {
    operation_id: ConnectorMutationOperationId,
    owner: ConnectorProviderBindingKey,
    operation: ConnectorMetadataMaintenanceOperation,
    request_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorMetadataMaintenancePlanningRequest {
    pub fn try_new(
        operation_id: ConnectorMutationOperationId,
        owner: ConnectorProviderBindingKey,
        operation: ConnectorMetadataMaintenanceOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance table handle does not match exact owner",
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
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub fn operation(&self) -> &ConnectorMetadataMaintenanceOperation {
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
                "metadata maintenance request owner or digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorMetadataMaintenancePlanningRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorMetadataMaintenancePlanningRequest")
            .field("operation_id", &self.operation_id)
            .field("owner", &self.owner)
            .field("operation_kind", &self.operation.kind())
            .field("request_digest", &self.request_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorMetadataMaintenancePlanSummary {
    source_items: u64,
    replacement_items: u64,
    candidate_versions: u64,
    cleanup_candidates: u64,
    total_bytes: u64,
}
impl ConnectorMetadataMaintenancePlanSummary {
    pub const fn new(
        source_items: u64,
        replacement_items: u64,
        candidate_versions: u64,
        cleanup_candidates: u64,
        total_bytes: u64,
    ) -> Self {
        Self {
            source_items,
            replacement_items,
            candidate_versions,
            cleanup_candidates,
            total_bytes,
        }
    }
    pub const fn source_items(self) -> u64 {
        self.source_items
    }
    pub const fn replacement_items(self) -> u64 {
        self.replacement_items
    }
    pub const fn candidate_versions(self) -> u64 {
        self.candidate_versions
    }
    pub const fn cleanup_candidates(self) -> u64 {
        self.cleanup_candidates
    }
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }
    fn digest_into(self, hash: &mut Sha256) {
        for value in [
            self.source_items,
            self.replacement_items,
            self.candidate_versions,
            self.cleanup_candidates,
            self.total_bytes,
        ] {
            hash.update(value.to_be_bytes());
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorMetadataMaintenancePlan {
    schema_version: u16,
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorMetadataMaintenancePlanSummary,
    provider_payload: Bytes,
    plan_digest: [u8; 32],
}
impl ConnectorMetadataMaintenancePlan {
    pub fn try_new(
        request: &ConnectorMetadataMaintenancePlanningRequest,
        state_digest: [u8; 32],
        summary: ConnectorMetadataMaintenancePlanSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_payload(&provider_payload, "plan")?;
        let plan_digest = plan_digest(
            request.request_digest,
            state_digest,
            summary,
            &provider_payload,
        );
        Ok(Self {
            schema_version: CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION,
            owner: request.owner.clone(),
            operation_id: request.operation_id,
            operation_kind: request.operation.kind().into(),
            request_digest: request.request_digest,
            state_digest,
            summary,
            provider_payload,
            plan_digest,
        })
    }

    /// Rebuilds a plan previously persisted by the frontend operation owner.
    ///
    /// The caller must already have recovered the exact owner from its durable
    /// record. This constructor validates every bounded carrier field and the
    /// semantic plan digest; it deliberately cannot recreate an operation from
    /// a current connector generation.
    #[allow(clippy::too_many_arguments)]
    pub fn try_restore(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        request_digest: [u8; 32],
        state_digest: [u8; 32],
        summary: ConnectorMetadataMaintenancePlanSummary,
        provider_payload: Bytes,
        plan_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        let operation_kind = operation_kind.into();
        validate_kind(&operation_kind)?;
        validate_payload(&provider_payload, "plan")?;
        let plan = Self {
            schema_version: CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION,
            owner,
            operation_id,
            operation_kind,
            request_digest,
            state_digest,
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
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub fn operation_kind(&self) -> &str {
        &self.operation_kind
    }
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }
    pub const fn summary(&self) -> ConnectorMetadataMaintenancePlanSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_kind(&self.operation_kind)?;
        validate_payload(&self.provider_payload, "plan")?;
        if self.schema_version != CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION
            || plan_digest(
                self.request_digest,
                self.state_digest,
                self.summary,
                &self.provider_payload,
            ) != self.plan_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance plan digest is invalid",
            ));
        }
        Ok(())
    }
}
impl fmt::Debug for ConnectorMetadataMaintenancePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorMetadataMaintenancePlan")
            .field("schema_version", &self.schema_version)
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("request_digest", &self.request_digest)
            .field("state_digest", &self.state_digest)
            .field("summary", &self.summary)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("plan_digest", &self.plan_digest)
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorMetadataMaintenanceExecuteRequest {
    pub plan: ConnectorMetadataMaintenancePlan,
    pub context: ConnectorRequestContext,
}
impl ConnectorMetadataMaintenanceExecuteRequest {
    pub fn try_new(
        plan: ConnectorMetadataMaintenancePlan,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        Ok(Self { plan, context })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorMetadataMaintenanceReceiptSummary {
    pub affected_versions: u64,
    pub rewritten_items: u64,
    pub added_items: u64,
    pub cleanup_succeeded: u64,
    pub cleanup_failed: u64,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorMetadataMaintenanceReceipt {
    schema_version: u16,
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    request_digest: [u8; 32],
    plan_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorMetadataMaintenanceReceiptSummary,
    provider_payload: Bytes,
    provider_payload_digest: [u8; 32],
}
impl ConnectorMetadataMaintenanceReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        request_digest: [u8; 32],
        plan_digest: [u8; 32],
        state_digest: [u8; 32],
        summary: ConnectorMetadataMaintenanceReceiptSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        let operation_kind = operation_kind.into();
        validate_kind(&operation_kind)?;
        validate_payload(&provider_payload, "receipt")?;
        let provider_payload_digest = digest_with_domain(RECEIPT_DOMAIN, &provider_payload);
        Ok(Self {
            schema_version: CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION,
            descriptor,
            incarnation,
            operation_id,
            operation_kind,
            request_digest,
            plan_digest,
            state_digest,
            summary,
            provider_payload,
            provider_payload_digest,
        })
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub fn operation_kind(&self) -> &str {
        &self.operation_kind
    }
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }
    pub const fn summary(&self) -> ConnectorMetadataMaintenanceReceiptSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn provider_payload_digest(&self) -> [u8; 32] {
        self.provider_payload_digest
    }
}
impl fmt::Debug for ConnectorMetadataMaintenanceReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorMetadataMaintenanceReceipt")
            .field("schema_version", &self.schema_version)
            .field("descriptor", &self.descriptor)
            .field("incarnation", &self.incarnation)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("plan_digest", &self.plan_digest)
            .field("provider_payload_len", &self.provider_payload.len())
            .finish()
    }
}

/// Read-only observation request for one base table.
///
/// This request deliberately carries no operation id: the observation is not a
/// mutation attempt, so it never joins the durable maintenance lifecycle and
/// never produces a plan, a receipt or a marker.
#[derive(Clone)]
pub struct ConnectorMaxCompactableDataFilesRequest {
    pub table: ConnectorTableHandle,
    pub context: ConnectorRequestContext,
}

impl ConnectorMaxCompactableDataFilesRequest {
    pub fn try_new(
        table: ConnectorTableHandle,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        Ok(Self { table, context })
    }
}

impl fmt::Debug for ConnectorMaxCompactableDataFilesRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorMaxCompactableDataFilesRequest")
            .field("owner", self.table.owner())
            .finish_non_exhaustive()
    }
}

/// Provider-neutral scalar answer. `None` means the provider exposes no such
/// observation for this table; it is not a zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorMaxCompactableDataFiles {
    value: Option<u64>,
}

impl ConnectorMaxCompactableDataFiles {
    pub const fn new(value: Option<u64>) -> Self {
        Self { value }
    }
    pub const fn value(self) -> Option<u64> {
        self.value
    }
}

pub trait ConnectorMetadataMaintenance: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorProviderBindingKey;
    fn plan_maintenance(
        &self,
        request: ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError>;
    fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError>;
    /// Reads the largest number of data files the provider would place into one
    /// of its own rewrite groups for this table. The grouping rule stays
    /// provider-private; only the resulting scalar crosses this boundary.
    ///
    /// This call is explicitly expensive: answering it requires the provider to
    /// enumerate the table's live manifests. It is reserved for background
    /// maintenance policy and must never be placed on a SQL planning path.
    fn read_max_compactable_data_files(
        &self,
        request: ConnectorMaxCompactableDataFilesRequest,
    ) -> Result<ConnectorMaxCompactableDataFiles, ConnectorError> {
        let _ = request;
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector does not observe compactable data file groups",
        ))
    }
}

pub trait ConnectorMetadataMaintenanceResolver: Send + Sync {
    fn acquire_current_metadata_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError>;
    fn acquire_exact_metadata_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorMetadataMaintenanceLease {
    descriptor: ConnectorInstanceDescriptor,
    control_runtime_id: ConnectorControlRuntimeId,
    provider_binding_key: ConnectorProviderBindingKey,
    metadata: Arc<dyn ConnectorMetadata>,
    maintenance: Arc<dyn ConnectorMetadataMaintenance>,
    _release: Arc<MaintenanceRelease>,
}
struct MaintenanceRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}
impl ConnectorMetadataMaintenanceLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        maintenance: Arc<dyn ConnectorMetadataMaintenance>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let provider_binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: provider_incarnation,
        };
        if metadata.instance_id() != &descriptor.instance_id
            || maintenance.descriptor() != &descriptor
            || maintenance.binding_key() != &provider_binding_key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance capabilities do not match lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            control_runtime_id,
            provider_binding_key,
            metadata,
            maintenance,
            _release: Arc::new(MaintenanceRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }
    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }
    /// Builds and validates a provider planning request behind the FE-owned
    /// runtime lease. The provider's legacy binding fence stays internal to
    /// this capability boundary.
    pub fn plan_operation(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorMetadataMaintenanceOperation,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
        let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
            operation_id,
            self.provider_binding_key.clone(),
            operation,
            context,
        )?;
        self.plan_maintenance(request)
    }
    pub fn plan_maintenance(
        &self,
        request: ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
        request.validate()?;
        if request.owner != self.provider_binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance request does not match lease",
            ));
        }
        let plan = self.maintenance.plan_maintenance(request.clone())?;
        plan.validate()?;
        if plan.owner != self.provider_binding_key
            || plan.operation_id != request.operation_id
            || plan.operation_kind.as_ref() != request.operation.kind()
            || plan.request_digest != request.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance plan does not match request",
            ));
        }
        Ok(plan)
    }
    pub fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        self.validate_plan(&request.plan)?;
        let plan = request.plan.clone();
        let outcome = self.maintenance.execute(request)?;
        self.validate_outcome(&plan, &outcome)?;
        Ok(outcome)
    }
    /// Read-only passthrough. It shares the maintenance lease because the
    /// observation needs the provider's own runtime and file access, but it
    /// deliberately validates nothing beyond exact handle ownership: there is
    /// no plan, no operation id and no outcome to reconcile.
    pub fn read_max_compactable_data_files(
        &self,
        request: ConnectorMaxCompactableDataFilesRequest,
    ) -> Result<ConnectorMaxCompactableDataFiles, ConnectorError> {
        if request.table.owner() != &self.provider_binding_key.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance observation table does not match lease",
            ));
        }
        self.maintenance.read_max_compactable_data_files(request)
    }
    fn validate_plan(&self, plan: &ConnectorMetadataMaintenancePlan) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner != self.provider_binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance plan does not match lease",
            ));
        }
        Ok(())
    }
    fn validate_outcome(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        outcome: &ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                if receipt.descriptor != self.descriptor
                    || receipt.schema_version != CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION
                    || receipt.incarnation != self.provider_binding_key.incarnation
                    || receipt.operation_id != plan.operation_id
                    || receipt.operation_kind.as_ref() != plan.operation_kind.as_ref()
                    || receipt.request_digest != plan.request_digest
                    || receipt.plan_digest != plan.plan_digest
                    || receipt.state_digest != plan.state_digest
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "metadata maintenance receipt does not match plan",
                    ));
                }
            }
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(plan.operation_id, plan.operation_kind(), evidence)?
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {}
        }
        Ok(())
    }
    fn validate_evidence(
        &self,
        operation_id: ConnectorMutationOperationId,
        kind: &str,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), ConnectorError> {
        if evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.provider_binding_key.incarnation
            || evidence.operation_id() != operation_id
            || evidence.operation_kind() != kind
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "metadata maintenance evidence does not match lease",
            ));
        }
        Ok(())
    }
}
impl Drop for MaintenanceRelease {
    fn drop(&mut self) {
        if let Ok(mut release) = self.release.lock()
            && let Some(release) = release.take()
        {
            release();
        }
    }
}

pub(crate) fn validate_metadata_maintenance_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    maintenance: &dyn ConnectorMetadataMaintenance,
) -> Result<(), ConnectorError> {
    if maintenance.descriptor() != descriptor
        || maintenance.binding_key().instance_id != descriptor.instance_id
        || maintenance.binding_key().incarnation != incarnation
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "metadata maintenance capability owner does not match control binding",
        ));
    }
    Ok(())
}

fn request_digest(
    operation_id: ConnectorMutationOperationId,
    owner: &ConnectorProviderBindingKey,
    operation: &ConnectorMetadataMaintenanceOperation,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(REQUEST_DOMAIN);
    hash.update(CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
    hash.update(operation_id.to_bytes());
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    operation.digest_into(&mut hash);
    hash.finalize().into()
}
fn plan_digest(
    request: [u8; 32],
    state: [u8; 32],
    summary: ConnectorMetadataMaintenancePlanSummary,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PLAN_DOMAIN);
    hash.update(CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
    hash.update(request);
    hash.update(state);
    summary.digest_into(&mut hash);
    digest_bytes(&mut hash, payload);
    hash.finalize().into()
}
fn validate_payload(payload: &Bytes, kind: &str) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("metadata maintenance {kind} payload exceeds hard limit"),
        ));
    }
    Ok(())
}
fn validate_kind(kind: &str) -> Result<(), ConnectorError> {
    if matches!(
        kind,
        REWRITE_METADATA_LAYOUT_KIND | EXPIRE_TABLE_VERSIONS_KIND
    ) {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "unknown metadata maintenance operation kind",
        ))
    }
}
fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}
fn digest_with_domain(domain: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    digest_bytes(&mut hash, value);
    hash.finalize().into()
}
