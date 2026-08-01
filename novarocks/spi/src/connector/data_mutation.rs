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

//! FE-only provider-neutral data mutation contract.
// Design: ADR-0024 (docs/adr/ADR-0024-connector-data-mutation-contract.md)

use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMetadata,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorTableHandle,
    ExternalMutationEvidence, ExternalMutationOutcome,
};

pub const CONNECTOR_DATA_MUTATION_CONTRACT_VERSION: u16 = 1;
pub const MAX_CONNECTOR_DATA_MUTATION_SOURCE_LOCATION_BYTES: usize = 8 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_TARGET_REF_BYTES: usize = 256;
pub const MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_FILES: u32 = 4096;
pub const MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES: usize = 16 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES: usize = 64 * 1024 * 1024;

const REQUEST_DIGEST_DOMAIN: &[u8] = b"novarocks.connector-data-mutation.request.v1\0";
const PLAN_DIGEST_DOMAIN: &[u8] = b"novarocks.connector-data-mutation.plan.v1\0";

pub const REGISTER_EXISTING_FILES_KIND: &str = "register-existing-files";
pub const TRUNCATE_KIND: &str = "truncate";
const RECEIPT_PAYLOAD_DIGEST_DOMAIN: &[u8] =
    b"novarocks.connector-data-mutation.receipt-payload.v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorDataMutationOperation {
    RegisterExistingFiles {
        table: ConnectorTableHandle,
        source_location: Arc<str>,
    },
    Truncate {
        table: ConnectorTableHandle,
        target_ref: Arc<str>,
    },
}

impl ConnectorDataMutationOperation {
    pub fn register_existing_files(
        table: ConnectorTableHandle,
        source_location: impl Into<Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        let operation = Self::RegisterExistingFiles {
            table,
            source_location: source_location.into(),
        };
        operation.validate()?;
        Ok(operation)
    }

    pub fn truncate(
        table: ConnectorTableHandle,
        target_ref: impl Into<Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        let operation = Self::Truncate {
            table,
            target_ref: target_ref.into(),
        };
        operation.validate()?;
        Ok(operation)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RegisterExistingFiles { .. } => REGISTER_EXISTING_FILES_KIND,
            Self::Truncate { .. } => TRUNCATE_KIND,
        }
    }

    pub const fn table(&self) -> &ConnectorTableHandle {
        match self {
            Self::RegisterExistingFiles { table, .. } | Self::Truncate { table, .. } => table,
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        match self {
            Self::RegisterExistingFiles {
                source_location, ..
            } => validate_bounded_text(
                source_location,
                MAX_CONNECTOR_DATA_MUTATION_SOURCE_LOCATION_BYTES,
                "connector data mutation source location",
            ),
            Self::Truncate { target_ref, .. } => validate_bounded_text(
                target_ref,
                MAX_CONNECTOR_DATA_MUTATION_TARGET_REF_BYTES,
                "connector data mutation target ref",
            ),
        }
    }

    fn digest_into(&self, hasher: &mut Sha256) {
        digest_bytes(hasher, self.kind().as_bytes());
        digest_bytes(hasher, self.table().owner().as_str().as_bytes());
        digest_bytes(hasher, self.table().payload());
        match self {
            Self::RegisterExistingFiles {
                source_location, ..
            } => digest_bytes(hasher, source_location.as_bytes()),
            Self::Truncate { target_ref, .. } => digest_bytes(hasher, target_ref.as_bytes()),
        }
    }
}

#[derive(Clone)]
pub struct ConnectorDataMutationPlanningRequest {
    operation_id: ConnectorMutationOperationId,
    owner: ConnectorExecutionBindingKey,
    operation: ConnectorDataMutationOperation,
    request_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorDataMutationPlanningRequest {
    pub fn try_new(
        operation_id: ConnectorMutationOperationId,
        owner: ConnectorExecutionBindingKey,
        operation: ConnectorDataMutationOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation table handle does not match the exact owner",
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

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn operation(&self) -> &ConnectorDataMutationOperation {
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
                "connector data mutation planning request digest or owner is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorDataMutationPlanningRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorDataMutationPlanningRequest")
            .field("operation_id", &self.operation_id)
            .field("owner", &self.owner)
            .field("operation_kind", &self.operation.kind())
            .field("table_payload_len", &self.operation.table().payload().len())
            .field("request_digest", &self.request_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorDataMutationPlanSummary {
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
}

impl ConnectorDataMutationPlanSummary {
    pub fn try_new(
        file_count: u32,
        row_count: u64,
        total_bytes: u64,
    ) -> Result<Self, ConnectorError> {
        if file_count > MAX_CONNECTOR_DATA_MUTATION_FILES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector data mutation file count exceeds the hard limit",
            ));
        }
        Ok(Self {
            file_count,
            row_count,
            total_bytes,
        })
    }

    pub const fn file_count(self) -> u32 {
        self.file_count
    }

    pub const fn row_count(self) -> u64 {
        self.row_count
    }

    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }

    fn digest_into(self, hasher: &mut Sha256) {
        hasher.update(self.file_count.to_be_bytes());
        hasher.update(self.row_count.to_be_bytes());
        hasher.update(self.total_bytes.to_be_bytes());
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDataMutationPlan {
    schema_version: u16,
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorDataMutationPlanSummary,
    provider_payload: Bytes,
    plan_digest: [u8; 32],
}

impl ConnectorDataMutationPlan {
    pub fn try_new(
        request: &ConnectorDataMutationPlanningRequest,
        state_digest: [u8; 32],
        summary: ConnectorDataMutationPlanSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_provider_payload(&provider_payload, "plan")?;
        let operation_kind: Arc<str> = request.operation.kind().into();
        let plan_digest = plan_digest(
            request.request_digest,
            state_digest,
            summary,
            &provider_payload,
        );
        Ok(Self {
            schema_version: CONNECTOR_DATA_MUTATION_CONTRACT_VERSION,
            owner: request.owner.clone(),
            operation_id: request.operation_id,
            operation_kind,
            request_digest: request.request_digest,
            state_digest,
            summary,
            provider_payload,
            plan_digest,
        })
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
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

    pub const fn summary(&self) -> ConnectorDataMutationPlanSummary {
        self.summary
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_operation_kind(&self.operation_kind)?;
        validate_provider_payload(&self.provider_payload, "plan")?;
        if self.schema_version != CONNECTOR_DATA_MUTATION_CONTRACT_VERSION
            || plan_digest(
                self.request_digest,
                self.state_digest,
                self.summary,
                &self.provider_payload,
            ) != self.plan_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation plan digest or version is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorDataMutationPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorDataMutationPlan")
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
pub struct ConnectorDataMutationExecuteRequest {
    pub plan: ConnectorDataMutationPlan,
    pub context: ConnectorRequestContext,
}

impl ConnectorDataMutationExecuteRequest {
    pub fn try_new(
        plan: ConnectorDataMutationPlan,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        Ok(Self { plan, context })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDataMutationReceipt {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    request_digest: [u8; 32],
    plan_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorDataMutationPlanSummary,
    provider_payload: Bytes,
    provider_payload_digest: [u8; 32],
}

impl ConnectorDataMutationReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        request_digest: [u8; 32],
        plan_digest: [u8; 32],
        state_digest: [u8; 32],
        summary: ConnectorDataMutationPlanSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        let operation_kind = operation_kind.into();
        validate_operation_kind(&operation_kind)?;
        validate_provider_payload(&provider_payload, "receipt")?;
        let provider_payload_digest =
            digest_with_domain(RECEIPT_PAYLOAD_DIGEST_DOMAIN, provider_payload.as_ref());
        Ok(Self {
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

    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
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

    pub const fn summary(&self) -> ConnectorDataMutationPlanSummary {
        self.summary
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub const fn provider_payload_digest(&self) -> [u8; 32] {
        self.provider_payload_digest
    }
}

impl fmt::Debug for ConnectorDataMutationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorDataMutationReceipt")
            .field("descriptor", &self.descriptor)
            .field("incarnation", &self.incarnation)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("request_digest", &self.request_digest)
            .field("plan_digest", &self.plan_digest)
            .field("state_digest", &self.state_digest)
            .field("summary", &self.summary)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("provider_payload_digest", &self.provider_payload_digest)
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorDataMutationReconcileRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub operation_id: ConnectorMutationOperationId,
    pub operation_kind: Arc<str>,
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

impl ConnectorDataMutationReconcileRequest {
    pub fn try_new(
        plan: &ConnectorDataMutationPlan,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if evidence.operation_id() != plan.operation_id
            || evidence.operation_kind() != plan.operation_kind.as_ref()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation evidence does not match its plan",
            ));
        }
        Ok(Self {
            owner: plan.owner.clone(),
            operation_id: plan.operation_id,
            operation_kind: plan.operation_kind.clone(),
            request_digest: plan.request_digest,
            plan_digest: plan.plan_digest,
            state_digest: plan.state_digest,
            evidence,
            context,
        })
    }
}

/// FE-only external data mutation capability. It is never installed in a BE
/// execution binding and intentionally has no public abort method.
pub trait ConnectorDataMutation: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError>;

    fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError>;

    fn reconcile(
        &self,
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError>;
}

pub trait ConnectorDataMutationResolver: Send + Sync {
    fn acquire_current_data_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError>;

    fn acquire_exact_data_mutation(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorDataMutationLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorDataMutationLease {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
    metadata: Arc<dyn ConnectorMetadata>,
    mutation: Arc<dyn ConnectorDataMutation>,
    _release: Arc<DataMutationLeaseRelease>,
}

struct DataMutationLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorDataMutationLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
        metadata: Arc<dyn ConnectorMetadata>,
        mutation: Arc<dyn ConnectorDataMutation>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if descriptor.instance_id != key.instance_id
            || metadata.instance_id() != &descriptor.instance_id
            || mutation.descriptor() != &descriptor
            || mutation.binding_key() != &key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation capabilities do not match their lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            key,
            metadata,
            mutation,
            _release: Arc::new(DataMutationLeaseRelease {
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

    pub fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        self.validate_planning_request(&request)?;
        let plan = self.mutation.plan_mutation(request.clone())?;
        plan.validate()?;
        if plan.owner != self.key
            || plan.operation_id != request.operation_id
            || plan.operation_kind.as_ref() != request.operation.kind()
            || plan.request_digest != request.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation plan does not match its request or lease generation",
            ));
        }
        Ok(plan)
    }

    pub fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.validate_plan(&request.plan)?;
        let plan = request.plan.clone();
        let outcome = self.mutation.execute(request)?;
        self.validate_outcome(&plan, &outcome)?;
        Ok(outcome)
    }

    pub fn reconcile(
        &self,
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.validate_reconcile_request(&request)?;
        let outcome = self.mutation.reconcile(request.clone())?;
        self.validate_reconcile_outcome(&request, &outcome)?;
        Ok(outcome)
    }

    fn validate_planning_request(
        &self,
        request: &ConnectorDataMutationPlanningRequest,
    ) -> Result<(), ConnectorError> {
        request.validate()?;
        if request.owner != self.key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation request does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_plan(&self, plan: &ConnectorDataMutationPlan) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner != self.key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation plan does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_reconcile_request(
        &self,
        request: &ConnectorDataMutationReconcileRequest,
    ) -> Result<(), ConnectorError> {
        validate_operation_kind(&request.operation_kind)?;
        if request.owner != self.key
            || request.evidence.descriptor() != &self.descriptor
            || request.evidence.incarnation() != self.key.incarnation
            || request.evidence.operation_id() != request.operation_id
            || request.evidence.operation_kind() != request.operation_kind.as_ref()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation reconcile evidence does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_outcome(
        &self,
        plan: &ConnectorDataMutationPlan,
        outcome: &ExternalMutationOutcome<ConnectorDataMutationReceipt>,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                self.validate_receipt(
                    plan.operation_id,
                    plan.operation_kind(),
                    plan.request_digest,
                    plan.plan_digest,
                    plan.state_digest,
                    receipt,
                )?;
            }
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(plan.operation_id, plan.operation_kind(), evidence)?;
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {}
        }
        Ok(())
    }

    fn validate_reconcile_outcome(
        &self,
        request: &ConnectorDataMutationReconcileRequest,
        outcome: &ExternalMutationOutcome<ConnectorDataMutationReceipt>,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => self.validate_receipt(
                request.operation_id,
                &request.operation_kind,
                request.request_digest,
                request.plan_digest,
                request.state_digest,
                receipt,
            ),
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(request.operation_id, &request.operation_kind, evidence)
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_receipt(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation_kind: &str,
        request_digest: [u8; 32],
        plan_digest: [u8; 32],
        state_digest: [u8; 32],
        receipt: &ConnectorDataMutationReceipt,
    ) -> Result<(), ConnectorError> {
        if receipt.descriptor != self.descriptor
            || receipt.incarnation != self.key.incarnation
            || receipt.operation_id != operation_id
            || receipt.operation_kind.as_ref() != operation_kind
            || receipt.request_digest != request_digest
            || receipt.plan_digest != plan_digest
            || receipt.state_digest != state_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation receipt does not match its request",
            ));
        }
        Ok(())
    }

    fn validate_evidence(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation_kind: &str,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), ConnectorError> {
        if evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.key.incarnation
            || evidence.operation_id() != operation_id
            || evidence.operation_kind() != operation_kind
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation evidence does not match its request",
            ));
        }
        Ok(())
    }
}

impl Drop for DataMutationLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

pub(crate) fn validate_data_mutation_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    mutation: &dyn ConnectorDataMutation,
) -> Result<(), ConnectorError> {
    if mutation.descriptor() != descriptor
        || mutation.binding_key().instance_id != descriptor.instance_id
        || mutation.binding_key().incarnation != incarnation
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector data mutation capability owner does not match its control binding generation",
        ));
    }
    Ok(())
}

fn request_digest(
    operation_id: ConnectorMutationOperationId,
    owner: &ConnectorExecutionBindingKey,
    operation: &ConnectorDataMutationOperation,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(REQUEST_DIGEST_DOMAIN);
    hasher.update(CONNECTOR_DATA_MUTATION_CONTRACT_VERSION.to_be_bytes());
    hasher.update(operation_id.to_bytes());
    digest_bytes(&mut hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    operation.digest_into(&mut hasher);
    hasher.finalize().into()
}

fn plan_digest(
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorDataMutationPlanSummary,
    provider_payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PLAN_DIGEST_DOMAIN);
    hasher.update(CONNECTOR_DATA_MUTATION_CONTRACT_VERSION.to_be_bytes());
    hasher.update(request_digest);
    hasher.update(state_digest);
    summary.digest_into(&mut hasher);
    digest_bytes(&mut hasher, provider_payload);
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn digest_with_domain(domain: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    digest_bytes(&mut hasher, value);
    hasher.finalize().into()
}

fn validate_bounded_text(value: &str, max: usize, field: &str) -> Result<(), ConnectorError> {
    if value.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("{field} must not be empty"),
        ));
    }
    if value.len() > max {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("{field} exceeds the hard limit"),
        ));
    }
    Ok(())
}

fn validate_provider_payload(payload: &Bytes, kind: &str) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("connector data mutation {kind} payload exceeds the hard limit"),
        ));
    }
    Ok(())
}

fn validate_operation_kind(kind: &str) -> Result<(), ConnectorError> {
    if matches!(kind, REGISTER_EXISTING_FILES_KIND | TRUNCATE_KIND) {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "unsupported connector data mutation operation kind",
        ))
    }
}
