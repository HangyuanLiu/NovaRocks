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

use novarocks::engine::table_maintenance::{MaintenanceTarget, OptimizeJobState};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const OPTIMIZE_JOB_SCHEMA_VERSION: u8 = 1;
pub const METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION: u8 = 2;
pub const METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobCreate {
    pub target: MaintenanceTarget,
    pub base_snapshot_id: i64,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobOutcome {
    pub target_snapshot_id: Option<i64>,
    pub rewritten_data_files: i64,
    pub deleted_data_files: i64,
    pub added_data_files: i64,
    pub output_record_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJob {
    pub job_id: i64,
    pub target: MaintenanceTarget,
    pub base_snapshot_id: i64,
    pub state: OptimizeJobState,
    pub outcome: Option<OptimizeJobOutcome>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredMaintenanceTargetV1 {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StoredOptimizeJobStateV1 {
    Pending,
    Running,
    Finished,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOptimizeOutcomeV1 {
    pub target_snapshot_id: Option<i64>,
    pub rewritten_data_files: i64,
    pub deleted_data_files: i64,
    pub added_data_files: i64,
    pub output_record_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOptimizeJobV1 {
    pub schema_version: u8,
    pub job_id: i64,
    pub target: StoredMaintenanceTargetV1,
    pub base_snapshot_id: i64,
    pub state: StoredOptimizeJobStateV1,
    pub outcome: Option<StoredOptimizeOutcomeV1>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub last_operation_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredOptimizeCounterV1 {
    pub schema_version: u8,
    pub last_job_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredOptimizeOperationActionV1 {
    Create,
    Claim,
    RecordOutcome,
    Finish,
    Fail,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredOptimizeOperationV1 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub action: StoredOptimizeOperationActionV1,
    pub job_id: i64,
    pub post_job: StoredOptimizeJobV1,
}

impl From<&MaintenanceTarget> for StoredMaintenanceTargetV1 {
    fn from(value: &MaintenanceTarget) -> Self {
        Self {
            catalog: value.catalog.clone(),
            namespace: value.namespace.clone(),
            table: value.table.clone(),
        }
    }
}

impl From<StoredMaintenanceTargetV1> for MaintenanceTarget {
    fn from(value: StoredMaintenanceTargetV1) -> Self {
        Self {
            catalog: value.catalog,
            namespace: value.namespace,
            table: value.table,
        }
    }
}

impl From<OptimizeJobState> for StoredOptimizeJobStateV1 {
    fn from(value: OptimizeJobState) -> Self {
        match value {
            OptimizeJobState::Pending => Self::Pending,
            OptimizeJobState::Running => Self::Running,
            OptimizeJobState::Finished => Self::Finished,
            OptimizeJobState::Failed => Self::Failed,
        }
    }
}

impl From<StoredOptimizeJobStateV1> for OptimizeJobState {
    fn from(value: StoredOptimizeJobStateV1) -> Self {
        match value {
            StoredOptimizeJobStateV1::Pending => Self::Pending,
            StoredOptimizeJobStateV1::Running => Self::Running,
            StoredOptimizeJobStateV1::Finished => Self::Finished,
            StoredOptimizeJobStateV1::Failed => Self::Failed,
        }
    }
}

impl From<&OptimizeJobOutcome> for StoredOptimizeOutcomeV1 {
    fn from(value: &OptimizeJobOutcome) -> Self {
        Self {
            target_snapshot_id: value.target_snapshot_id,
            rewritten_data_files: value.rewritten_data_files,
            deleted_data_files: value.deleted_data_files,
            added_data_files: value.added_data_files,
            output_record_count: value.output_record_count,
        }
    }
}

impl From<StoredOptimizeOutcomeV1> for OptimizeJobOutcome {
    fn from(value: StoredOptimizeOutcomeV1) -> Self {
        Self {
            target_snapshot_id: value.target_snapshot_id,
            rewritten_data_files: value.rewritten_data_files,
            deleted_data_files: value.deleted_data_files,
            added_data_files: value.added_data_files,
            output_record_count: value.output_record_count,
        }
    }
}

impl From<&StoredOptimizeJobV1> for OptimizeJob {
    fn from(value: &StoredOptimizeJobV1) -> Self {
        Self {
            job_id: value.job_id,
            target: value.target.clone().into(),
            base_snapshot_id: value.base_snapshot_id,
            state: value.state.into(),
            outcome: value.outcome.clone().map(Into::into),
            error_message: value.error_message.clone(),
            created_at_ms: value.created_at_ms,
            started_at_ms: value.started_at_ms,
            finished_at_ms: value.finished_at_ms,
        }
    }
}

/// Durable, frontend-owned state for a metadata-only connector maintenance
/// operation.  This is deliberately separate from the v1 OPTIMIZE job model:
/// E1 operations run synchronously but still need a recovery record before any
/// external catalog dispatch occurs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetadataMaintenanceOperationKind {
    RewriteMetadataLayout,
    ExpireTableVersions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetadataMaintenanceOperationState {
    Pending,
    Running,
    ReconcilePending,
    Finished,
    Failed,
    Unresolved,
}

impl MetadataMaintenanceOperationState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed)
    }

    pub const fn holds_active_fence(self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Running | Self::ReconcilePending | Self::Unresolved
        )
    }

    pub const fn as_key_component(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::ReconcilePending => "reconcile-pending",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Unresolved => "unresolved",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenanceOperationCreate {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: MetadataMaintenanceOperationKind,
    pub request_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub request_payload: Vec<u8>,
    pub created_at_ms: i64,
}

/// Persisted exact-generation identity.  It is stored as canonical primitive
/// fields rather than a process-local lease so recovery can reconstruct and
/// validate the exact `ConnectorExecutionBindingKey` before reconcile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataMaintenanceExactOwner {
    pub instance_id: String,
    pub incarnation_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenancePlanPayload {
    pub plan_digest: [u8; 32],
    pub payload_digest: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenanceOpaquePayload {
    pub digest: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenanceOperation {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: MetadataMaintenanceOperationKind,
    pub request_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub plan_digest: Option<[u8; 32]>,
    pub state: MetadataMaintenanceOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMetadataMaintenanceOperationV2 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub target: StoredMaintenanceTargetV1,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: MetadataMaintenanceOperationKind,
    pub request_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub plan_digest: Option<[u8; 32]>,
    pub state: MetadataMaintenanceOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredMetadataMaintenanceTransactionActionV2 {
    Create,
    Start,
    ReconcilePending,
    Finish,
    Fail,
    Unresolve,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMetadataMaintenanceTransactionV2 {
    pub schema_version: u8,
    pub transaction_operation_id: Uuid,
    pub action: StoredMetadataMaintenanceTransactionActionV2,
    pub operation_id: Uuid,
    pub post_operation: StoredMetadataMaintenanceOperationV2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredMetadataMaintenancePayloadKindV2 {
    Request,
    Plan,
    Receipt,
    Evidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMetadataMaintenancePayloadV2 {
    pub schema_version: u8,
    pub kind: StoredMetadataMaintenancePayloadKindV2,
    pub digest: [u8; 32],
    pub payload: Vec<u8>,
}

impl From<&StoredMetadataMaintenanceOperationV2> for MetadataMaintenanceOperation {
    fn from(value: &StoredMetadataMaintenanceOperationV2) -> Self {
        Self {
            operation_id: value.operation_id,
            target: value.target.clone().into(),
            owner: value.owner.clone(),
            kind: value.kind,
            request_digest: value.request_digest,
            request_payload_digest: value.request_payload_digest,
            base_state_digest: value.base_state_digest,
            plan_digest: value.plan_digest,
            state: value.state,
            error_message: value.error_message.clone(),
            created_at_ms: value.created_at_ms,
            started_at_ms: value.started_at_ms,
            finished_at_ms: value.finished_at_ms,
        }
    }
}
