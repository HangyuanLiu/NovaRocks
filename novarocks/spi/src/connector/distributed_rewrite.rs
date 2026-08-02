// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
//
// Design: ADR-0029 (docs/adr/ADR-0029-connector-distributed-rewrite-contract.md)

//! FE-only provider-neutral distributed rewrite planning contract.

use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorMetadata, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorWriteAttemptCompletion, ConnectorWriteCohortId, ConnectorWriteControl,
    ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWriteReceipt,
};

pub const CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION: u16 = 1;
pub const MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS: usize = 4096;

const REQUEST_DOMAIN: &[u8] = b"novarocks.connector-distributed-rewrite.request.v1\0";
const PLAN_DOMAIN: &[u8] = b"novarocks.connector-distributed-rewrite.plan.v1\0";
const CHECKPOINT_DOMAIN: &[u8] = b"novarocks.connector-distributed-rewrite.checkpoint.v1\0";

pub const REWRITE_DATA_FILES_KIND: &str = "rewrite-data-files";
pub const REWRITE_POSITION_DELETES_KIND: &str = "rewrite-position-deletes";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorDistributedRewriteOperation {
    RewriteDataFiles {
        table: ConnectorTableHandle,
        rewrite_all: bool,
    },
    RewritePositionDeletes {
        table: ConnectorTableHandle,
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}

impl ConnectorDistributedRewriteOperation {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RewriteDataFiles { .. } => REWRITE_DATA_FILES_KIND,
            Self::RewritePositionDeletes { .. } => REWRITE_POSITION_DELETES_KIND,
        }
    }

    pub const fn table(&self) -> &ConnectorTableHandle {
        match self {
            Self::RewriteDataFiles { table, .. } | Self::RewritePositionDeletes { table, .. } => {
                table
            }
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Self::RewritePositionDeletes {
            min_input_files: Some(value),
            ..
        } = self
            && *value == 0
        {
            return Err(invalid(
                "rewrite position deletes min_input_files must be positive",
            ));
        }
        Ok(())
    }

    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.kind().as_bytes());
        digest_bytes(hash, self.table().owner().as_str().as_bytes());
        digest_bytes(hash, self.table().payload());
        match self {
            Self::RewriteDataFiles { rewrite_all, .. } => hash.update([u8::from(*rewrite_all)]),
            Self::RewritePositionDeletes {
                rewrite_all,
                min_input_files,
                ..
            } => {
                hash.update([u8::from(*rewrite_all), u8::from(min_input_files.is_some())]);
                hash.update(min_input_files.unwrap_or_default().to_be_bytes());
            }
        }
    }
}

#[derive(Clone)]
pub struct ConnectorDistributedRewritePlanningRequest {
    operation_id: ConnectorWriteOperationId,
    owner: ConnectorExecutionBindingKey,
    operation: ConnectorDistributedRewriteOperation,
    request_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorDistributedRewritePlanningRequest {
    pub fn try_new(
        operation_id: ConnectorWriteOperationId,
        owner: ConnectorExecutionBindingKey,
        operation: ConnectorDistributedRewriteOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(invalid(
                "distributed rewrite table handle does not match exact owner",
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
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub fn operation(&self) -> &ConnectorDistributedRewriteOperation {
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
            return Err(invalid(
                "distributed rewrite request owner or digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorDistributedRewritePlanningRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorDistributedRewritePlanningRequest")
            .field("operation_id", &self.operation_id)
            .field("owner", &self.owner)
            .field("operation_kind", &self.operation.kind())
            .field("request_digest", &self.request_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorDistributedRewritePlanSummary {
    pub groups: u64,
    pub input_data_files: u64,
    pub input_delete_files: u64,
    pub input_bytes: u64,
    pub expected_output_files: u64,
}

impl ConnectorDistributedRewritePlanSummary {
    fn digest_into(self, hash: &mut Sha256) {
        for value in [
            self.groups,
            self.input_data_files,
            self.input_delete_files,
            self.input_bytes,
            self.expected_output_files,
        ] {
            hash.update(value.to_be_bytes());
        }
    }
}

#[derive(Clone)]
pub struct ConnectorDistributedRewriteCohortPlan {
    cohort_id: ConnectorWriteCohortId,
    source: ConnectorTableHandle,
    intent: ConnectorWriteIntent,
    input_schema: SchemaRef,
    provider_payload: Bytes,
    group_digest: [u8; 32],
}

impl ConnectorDistributedRewriteCohortPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        cohort_id: ConnectorWriteCohortId,
        source: ConnectorTableHandle,
        intent: ConnectorWriteIntent,
        input_schema: SchemaRef,
        provider_payload: Bytes,
        group_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        validate_payload(&provider_payload, "cohort")?;
        Ok(Self {
            cohort_id,
            source,
            intent,
            input_schema,
            provider_payload,
            group_digest,
        })
    }
    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
    pub fn source(&self) -> &ConnectorTableHandle {
        &self.source
    }
    pub const fn intent(&self) -> ConnectorWriteIntent {
        self.intent
    }
    pub fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn group_digest(&self) -> [u8; 32] {
        self.group_digest
    }
    fn digest_into(&self, hash: &mut Sha256) {
        hash.update(self.cohort_id.to_bytes());
        hash.update(self.group_digest);
        hash.update([self.intent as u8]);
        digest_bytes(hash, self.source.owner().as_str().as_bytes());
        digest_bytes(hash, self.source.payload());
        digest_bytes(hash, &self.provider_payload);
    }
}

impl fmt::Debug for ConnectorDistributedRewriteCohortPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorDistributedRewriteCohortPlan")
            .field("cohort_id", &self.cohort_id)
            .field("intent", &self.intent)
            .field("group_digest", &self.group_digest)
            .field("provider_payload_len", &self.provider_payload.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ConnectorDistributedRewritePlan {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    operation_kind: Arc<str>,
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    manifest_digest: [u8; 32],
    summary: ConnectorDistributedRewritePlanSummary,
    provider_payload: Bytes,
    cohorts: Vec<ConnectorDistributedRewriteCohortPlan>,
    plan_digest: [u8; 32],
}

impl ConnectorDistributedRewritePlan {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        request: &ConnectorDistributedRewritePlanningRequest,
        state_digest: [u8; 32],
        manifest_digest: [u8; 32],
        summary: ConnectorDistributedRewritePlanSummary,
        provider_payload: Bytes,
        mut cohorts: Vec<ConnectorDistributedRewriteCohortPlan>,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_payload(&provider_payload, "plan")?;
        if cohorts.len() > MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS {
            return Err(exhausted("distributed rewrite plan exceeds cohort limit"));
        }
        cohorts.sort_by_key(ConnectorDistributedRewriteCohortPlan::cohort_id);
        if cohorts
            .windows(2)
            .any(|pair| pair[0].cohort_id == pair[1].cohort_id)
            || cohorts
                .iter()
                .any(|cohort| cohort.source.owner() != &request.owner.instance_id)
        {
            return Err(invalid(
                "distributed rewrite cohorts are invalid or foreign",
            ));
        }
        let plan_digest = plan_digest(
            request.request_digest,
            state_digest,
            manifest_digest,
            summary,
            &provider_payload,
            &cohorts,
        );
        Ok(Self {
            owner: request.owner.clone(),
            operation_id: request.operation_id,
            operation_kind: request.operation.kind().into(),
            request_digest: request.request_digest,
            state_digest,
            manifest_digest,
            summary,
            provider_payload,
            cohorts,
            plan_digest,
        })
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
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
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn summary(&self) -> ConnectorDistributedRewritePlanSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub fn cohorts(&self) -> &[ConnectorDistributedRewriteCohortPlan] {
        &self.cohorts
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_payload(&self.provider_payload, "plan")?;
        if !matches!(
            self.operation_kind.as_ref(),
            REWRITE_DATA_FILES_KIND | REWRITE_POSITION_DELETES_KIND
        ) || self.cohorts.len() > MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS
            || self
                .cohorts
                .windows(2)
                .any(|pair| pair[0].cohort_id >= pair[1].cohort_id)
        {
            return Err(invalid("distributed rewrite plan is invalid"));
        }
        if self
            .cohorts
            .iter()
            .any(|cohort| cohort.source.owner() != &self.owner.instance_id)
        {
            return Err(invalid("distributed rewrite plan contains foreign cohort"));
        }
        if self.plan_digest
            != plan_digest(
                self.request_digest,
                self.state_digest,
                self.manifest_digest,
                self.summary,
                &self.provider_payload,
                &self.cohorts,
            )
        {
            return Err(invalid("distributed rewrite plan digest is invalid"));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorDistributedRewritePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorDistributedRewritePlan")
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("manifest_digest", &self.manifest_digest)
            .field("cohort_count", &self.cohorts.len())
            .field("provider_payload_len", &self.provider_payload.len())
            .field("plan_digest", &self.plan_digest)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDistributedRewriteAttemptDisposition {
    Accepted,
    Superseded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDistributedRewriteAttemptCheckpoint {
    pub cohort_id: ConnectorWriteCohortId,
    pub disposition: ConnectorDistributedRewriteAttemptDisposition,
    pub attempt_digest: [u8; 32],
    pub artifact_digest: [u8; 32],
    pub artifact_handle: Bytes,
    pub checkpoint_digest: [u8; 32],
}

impl ConnectorDistributedRewriteAttemptCheckpoint {
    pub fn try_new(
        cohort_id: ConnectorWriteCohortId,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        attempt_digest: [u8; 32],
        artifact_digest: [u8; 32],
        artifact_handle: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_payload(&artifact_handle, "attempt checkpoint")?;
        let checkpoint_digest = checkpoint_digest(
            cohort_id,
            disposition,
            attempt_digest,
            artifact_digest,
            &artifact_handle,
        );
        Ok(Self {
            cohort_id,
            disposition,
            attempt_digest,
            artifact_digest,
            artifact_handle,
            checkpoint_digest,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_payload(&self.artifact_handle, "attempt checkpoint")?;
        let expected = checkpoint_digest(
            self.cohort_id,
            self.disposition,
            self.attempt_digest,
            self.artifact_digest,
            &self.artifact_handle,
        );
        if self.checkpoint_digest != expected {
            return Err(invalid("distributed rewrite checkpoint digest is invalid"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorDistributedRewriteReceiptSummary {
    pub input_data_files: u64,
    pub input_delete_files: u64,
    pub output_data_files: u64,
    pub output_delete_files: u64,
    pub output_rows: u64,
    pub target_version: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDistributedRewriteReceipt {
    summary: ConnectorDistributedRewriteReceiptSummary,
    provider_payload: Bytes,
    provider_payload_digest: [u8; 32],
}

impl ConnectorDistributedRewriteReceipt {
    pub fn try_new(
        summary: ConnectorDistributedRewriteReceiptSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_payload(&provider_payload, "receipt")?;
        let provider_payload_digest = Sha256::digest(&provider_payload).into();
        Ok(Self {
            summary,
            provider_payload,
            provider_payload_digest,
        })
    }
    pub const fn summary(&self) -> ConnectorDistributedRewriteReceiptSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn provider_payload_digest(&self) -> [u8; 32] {
        self.provider_payload_digest
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_payload(&self.provider_payload, "receipt")?;
        let actual: [u8; 32] = Sha256::digest(&self.provider_payload).into();
        if self.provider_payload_digest != actual {
            return Err(invalid("distributed rewrite receipt digest is invalid"));
        }
        Ok(())
    }
}

pub trait ConnectorDistributedRewrite: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;
    fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError>;
    fn activate_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
    ) -> Result<(), ConnectorError>;
    fn checkpoint_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        completion: &ConnectorWriteAttemptCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError>;
    fn restore_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError>;
    fn finalize_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError>;
}

pub trait ConnectorDistributedRewriteResolver: Send + Sync {
    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError>;
    fn acquire_exact_distributed_rewrite(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorDistributedRewriteLease {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
    metadata: Arc<dyn ConnectorMetadata>,
    rewrite: Arc<dyn ConnectorDistributedRewrite>,
    write: Arc<dyn ConnectorWriteControl>,
    distribution: Arc<dyn ConnectorExecutionDistribution>,
    _release: Arc<RewriteLeaseRelease>,
}
struct RewriteLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorDistributedRewriteLease {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
        metadata: Arc<dyn ConnectorMetadata>,
        rewrite: Arc<dyn ConnectorDistributedRewrite>,
        write: Arc<dyn ConnectorWriteControl>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if descriptor.instance_id != key.instance_id
            || metadata.instance_id() != &key.instance_id
            || rewrite.descriptor() != &descriptor
            || rewrite.binding_key() != &key
            || write.binding_key() != &key
        {
            return Err(invalid(
                "distributed rewrite capabilities do not match lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            key,
            metadata,
            rewrite,
            write,
            distribution,
            _release: Arc::new(RewriteLeaseRelease {
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
    pub fn rewrite(&self) -> &Arc<dyn ConnectorDistributedRewrite> {
        &self.rewrite
    }
    pub fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
        request.validate()?;
        if request.owner != self.key {
            return Err(invalid("distributed rewrite request does not match lease"));
        }
        let plan = self.rewrite.plan_rewrite(request.clone())?;
        plan.validate()?;
        if plan.owner != self.key
            || plan.operation_id != request.operation_id
            || plan.request_digest != request.request_digest
        {
            return Err(invalid("distributed rewrite plan does not match request"));
        }
        Ok(plan)
    }
    pub fn activate_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
    ) -> Result<(), ConnectorError> {
        self.validate_plan(plan)?;
        self.rewrite.activate_rewrite(plan)
    }
    pub fn checkpoint_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        completion: &ConnectorWriteAttemptCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        self.validate_plan(plan)?;
        self.validate_attempt(plan, completion)?;
        let checkpoint = self
            .rewrite
            .checkpoint_attempt(plan, disposition, completion)?;
        checkpoint.validate()?;
        if checkpoint.cohort_id != completion.cohort_id()
            || checkpoint.attempt_digest != completion.digest()
        {
            return Err(invalid(
                "distributed rewrite checkpoint does not match attempt completion",
            ));
        }
        Ok(checkpoint)
    }
    pub fn restore_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        self.validate_plan(plan)?;
        checkpoint.validate()?;
        if !plan
            .cohorts
            .iter()
            .any(|cohort| cohort.cohort_id == checkpoint.cohort_id)
        {
            return Err(invalid(
                "distributed rewrite checkpoint names unknown cohort",
            ));
        }
        let completion = self.rewrite.restore_attempt(plan, checkpoint)?;
        self.validate_attempt(plan, &completion)?;
        if completion.digest() != checkpoint.attempt_digest {
            return Err(invalid(
                "distributed rewrite restored attempt digest does not match checkpoint",
            ));
        }
        Ok(completion)
    }
    pub fn finalize_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        self.validate_plan(plan)?;
        receipt.validate()?;
        let rewrite_receipt = self.rewrite.finalize_rewrite(plan, receipt)?;
        rewrite_receipt.validate()?;
        Ok(rewrite_receipt)
    }
    pub fn derive_write_lease(&self) -> Result<ConnectorWriteLease, ConnectorError> {
        let retained = self.clone();
        ConnectorWriteLease::new_with_execution_distribution(
            self.key.clone(),
            self.write.clone(),
            self.distribution.clone(),
            move || drop(retained),
        )
    }
    fn validate_plan(&self, plan: &ConnectorDistributedRewritePlan) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner != self.key {
            return Err(invalid("distributed rewrite plan does not match lease"));
        }
        Ok(())
    }
    fn validate_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        completion: &ConnectorWriteAttemptCompletion,
    ) -> Result<(), ConnectorError> {
        if completion.owner() != &self.key
            || completion.operation_id() != plan.operation_id
            || !plan
                .cohorts
                .iter()
                .any(|cohort| cohort.cohort_id == completion.cohort_id())
        {
            return Err(invalid(
                "distributed rewrite attempt does not belong to plan or exact lease",
            ));
        }
        Ok(())
    }
}

impl Drop for RewriteLeaseRelease {
    fn drop(&mut self) {
        if let Ok(mut release) = self.release.lock()
            && let Some(release) = release.take()
        {
            release();
        }
    }
}

pub(crate) fn validate_distributed_rewrite_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: super::ConnectorInstanceIncarnation,
    rewrite: &dyn ConnectorDistributedRewrite,
) -> Result<(), ConnectorError> {
    if rewrite.descriptor() != descriptor
        || rewrite.binding_key().instance_id != descriptor.instance_id
        || rewrite.binding_key().incarnation != incarnation
    {
        return Err(invalid(
            "distributed rewrite capability owner does not match control binding",
        ));
    }
    Ok(())
}

fn request_digest(
    operation_id: ConnectorWriteOperationId,
    owner: &ConnectorExecutionBindingKey,
    operation: &ConnectorDistributedRewriteOperation,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(REQUEST_DOMAIN);
    hash.update(CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION.to_be_bytes());
    hash.update(operation_id.to_bytes());
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    operation.digest_into(&mut hash);
    hash.finalize().into()
}
fn plan_digest(
    request: [u8; 32],
    state: [u8; 32],
    manifest: [u8; 32],
    summary: ConnectorDistributedRewritePlanSummary,
    payload: &Bytes,
    cohorts: &[ConnectorDistributedRewriteCohortPlan],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PLAN_DOMAIN);
    hash.update(CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION.to_be_bytes());
    hash.update(request);
    hash.update(state);
    hash.update(manifest);
    summary.digest_into(&mut hash);
    digest_bytes(&mut hash, payload);
    hash.update((cohorts.len() as u64).to_be_bytes());
    for cohort in cohorts {
        cohort.digest_into(&mut hash);
    }
    hash.finalize().into()
}
fn checkpoint_digest(
    cohort: ConnectorWriteCohortId,
    disposition: ConnectorDistributedRewriteAttemptDisposition,
    attempt: [u8; 32],
    _artifact: [u8; 32],
    handle: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(CHECKPOINT_DOMAIN);
    hash.update(cohort.to_bytes());
    hash.update([disposition as u8]);
    hash.update(attempt);
    digest_bytes(&mut hash, handle);
    hash.finalize().into()
}
fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}
fn validate_payload(value: &Bytes, kind: &str) -> Result<(), ConnectorError> {
    if value.len() > MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES {
        Err(exhausted(format!(
            "distributed rewrite {kind} payload exceeds hard limit"
        )))
    } else {
        Ok(())
    }
}
fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}
