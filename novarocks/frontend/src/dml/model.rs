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

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use novarocks::connector::iceberg::commit::{
    CleanupAttempt, CommitOpKind, CommitOutcome, CommitServiceError, RecoveryEvidence,
};

pub const DML_OPERATION_SCHEMA_VERSION: u8 = 2;
pub const DML_LEGACY_OPERATION_SCHEMA_VERSION: u8 = 1;
pub const DML_UNFINISHED_SCHEMA_VERSION: u8 = 1;
pub const DML_EXTERNAL_FACT_ENCODED_LIMIT: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DmlOperationId(Uuid);

impl DmlOperationId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for DmlOperationId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl fmt::Display for DmlOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationKind {
    InsertAppend,
    InsertOverwrite,
    RowDelta,
    MvRefresh,
    Maintenance,
    CreateTableAsSelect,
    Truncate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationState {
    Preparing,
    Writing,
    Collecting,
    Committing,
    Committed,
    CommitUnknown,
    Finalizing,
    Finalized,
    Aborting,
    Aborted,
    FailedKnownUncommitted,
    FinalizeFailedKnownCommitted,
}

impl OperationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "PREPARING",
            Self::Writing => "WRITING",
            Self::Collecting => "COLLECTING",
            Self::Committing => "COMMITTING",
            Self::Committed => "COMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::Finalizing => "FINALIZING",
            Self::Finalized => "FINALIZED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::FailedKnownUncommitted => "FAILED_KNOWN_UNCOMMITTED",
            Self::FinalizeFailedKnownCommitted => "FINALIZE_FAILED_KNOWN_COMMITTED",
        }
    }

    pub const fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Finalized | Self::Aborted | Self::FailedKnownUncommitted
        )
    }
}

pub fn validate_operation_transition(
    from: OperationState,
    to: OperationState,
) -> Result<(), String> {
    if from == to {
        return Ok(());
    }
    let allowed = matches!(
        (from, to),
        (OperationState::Preparing, OperationState::Writing)
            | (OperationState::Preparing, OperationState::Committing)
            | (OperationState::Preparing, OperationState::Aborting)
            | (
                OperationState::Preparing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Writing, OperationState::Collecting)
            | (OperationState::Writing, OperationState::Committing)
            | (OperationState::Writing, OperationState::Aborting)
            | (
                OperationState::Writing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Collecting, OperationState::Committing)
            | (OperationState::Collecting, OperationState::Aborting)
            | (
                OperationState::Collecting,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Committing, OperationState::Committed)
            | (OperationState::Committing, OperationState::CommitUnknown)
            | (
                OperationState::Committing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::CommitUnknown, OperationState::Committed)
            | (
                OperationState::CommitUnknown,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Committed, OperationState::Finalizing)
            | (OperationState::Committed, OperationState::Finalized)
            | (OperationState::Finalizing, OperationState::Finalized)
            | (
                OperationState::Finalizing,
                OperationState::FinalizeFailedKnownCommitted
            )
            | (OperationState::Finalizing, OperationState::CommitUnknown)
            | (
                OperationState::FinalizeFailedKnownCommitted,
                OperationState::Finalizing
            )
            | (OperationState::Aborting, OperationState::Aborted)
            | (
                OperationState::Aborting,
                OperationState::FailedKnownUncommitted
            )
    );
    if allowed {
        Ok(())
    } else {
        Err(format!(
            "invalid DML operation state transition from {} to {}",
            from.as_str(),
            to.as_str()
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    #[serde(default)]
    pub ref_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationFailureKind {
    KnownUncommitted,
    Unknown,
    FinalizeKnownCommitted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationNextAction {
    None,
    RetryAbort,
    RetryFinalize,
    ManualInspect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergOperationFailureRecord {
    pub kind: IcebergOperationFailureKind,
    pub message: String,
    pub next_action: IcebergOperationNextAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergCommitOutcomeRecord {
    pub snapshot_id: i64,
    pub written_manifest_paths: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergCleanupOutcomeRecord {
    pub attempted: bool,
    pub error_count: i64,
    pub error_paths: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergRecoveryEvidenceRecord {
    pub table_ident: String,
    pub commit_op_kind: String,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: Option<i64>,
    pub staging_dir: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationFact {
    pub state: OperationState,
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    pub failure: Option<IcebergOperationFailureRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExternalFactOutcome {
    KnownCommitted,
    KnownUncommitted,
    CommitUnknown,
    Unsupported,
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementNextAction {
    None,
    Reconcile,
    AbortStaging,
    RetryFinalize,
    ManualInspect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableExternalFact {
    pub outcome: ExternalFactOutcome,
    #[serde(default)]
    pub receipt: Option<String>,
    #[serde(default)]
    pub evidence: Option<String>,
    #[serde(default)]
    pub finalization_failure: Option<String>,
    #[serde(default)]
    pub failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableMutationSummary {
    pub file_count: u32,
    pub row_count: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CtasSagaPhase {
    PreparingSource,
    PreparingStagedTable,
    Staged,
    Writing,
    Publishing,
    PublishUnknown,
    AbortingStaging,
    Committed,
    Failed,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CtasSagaRecord {
    pub phase: CtasSagaPhase,
    pub prepare_operation_id: Uuid,
    pub write_operation_id: Uuid,
    pub publish_operation_id: Uuid,
    pub abort_staging_operation_id: Uuid,
    pub create_policy: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub connector_instance_id: Option<String>,
    #[serde(default)]
    pub connector_incarnation: Option<String>,
    #[serde(default)]
    pub source_plan_digest: Option<String>,
    #[serde(default)]
    pub staged_handle_digest: Option<String>,
    #[serde(default)]
    pub aggregate_write_digest: Option<String>,
    #[serde(default)]
    pub prepare_fact: Option<DurableExternalFact>,
    #[serde(default)]
    pub publish_fact: Option<DurableExternalFact>,
    #[serde(default)]
    pub abort_staging_fact: Option<DurableExternalFact>,
    pub next_action: StatementNextAction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TruncateLifecyclePhase {
    Preparing,
    Planned,
    Executing,
    CommitUnknown,
    Reconciling,
    Committed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TruncateLifecycleRecord {
    pub phase: TruncateLifecyclePhase,
    pub connector_operation_id: Uuid,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub connector_instance_id: Option<String>,
    #[serde(default)]
    pub connector_incarnation: Option<String>,
    pub target_ref: String,
    #[serde(default)]
    pub request_digest: Option<String>,
    #[serde(default)]
    pub plan_digest: Option<String>,
    #[serde(default)]
    pub state_digest: Option<String>,
    #[serde(default)]
    pub plan_summary: Option<DurableMutationSummary>,
    #[serde(default)]
    pub outcome: Option<DurableExternalFact>,
    pub next_action: StatementNextAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "details", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationPayload {
    WriteV1,
    CtasSaga(CtasSagaRecord),
    TruncateLifecycle(TruncateLifecycleRecord),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePreparingRequest {
    pub operation_kind: OperationKind,
    pub operation_subkind: Option<String>,
    pub target: OperationTarget,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOperation {
    pub schema_version: u8,
    pub operation_id: DmlOperationId,
    pub revision: u64,
    pub last_mutation_id: Uuid,
    pub operation_kind: OperationKind,
    #[serde(default)]
    pub operation_subkind: Option<String>,
    pub target: OperationTarget,
    pub state: OperationState,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    #[serde(default)]
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    #[serde(default)]
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    #[serde(default)]
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    #[serde(default)]
    pub failure: Option<IcebergOperationFailureRecord>,
    pub payload: OperationPayload,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateStatementOperationRequest {
    pub operation_id: DmlOperationId,
    pub mutation_id: Uuid,
    pub operation_kind: OperationKind,
    pub target: OperationTarget,
    pub attempt_id: String,
    pub payload: OperationPayload,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationMutationRequest {
    pub operation_id: DmlOperationId,
    pub expected_revision: u64,
    pub mutation_id: Uuid,
    pub state: OperationState,
    pub payload: OperationPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionSpec {
    pub target: OperationTarget,
    pub operation_kind: OperationKind,
    pub commit_op_kind: CommitOpKind,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionOutcome {
    pub operation_id: Option<DmlOperationId>,
    pub committed_snapshot_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_payload_json_round_trips() {
        let ctas = OperationPayload::CtasSaga(CtasSagaRecord {
            phase: CtasSagaPhase::PublishUnknown,
            prepare_operation_id: Uuid::now_v7(),
            write_operation_id: Uuid::now_v7(),
            publish_operation_id: Uuid::now_v7(),
            abort_staging_operation_id: Uuid::now_v7(),
            create_policy: "FAIL_IF_EXISTS".to_string(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some("03".repeat(16)),
            source_plan_digest: Some("source".to_string()),
            staged_handle_digest: Some("staged".to_string()),
            aggregate_write_digest: Some("write".to_string()),
            prepare_fact: None,
            publish_fact: Some(DurableExternalFact {
                outcome: ExternalFactOutcome::CommitUnknown,
                receipt: None,
                evidence: Some("evidence".to_string()),
                finalization_failure: None,
                failure: Some("unknown".to_string()),
            }),
            abort_staging_fact: None,
            next_action: StatementNextAction::Reconcile,
        });
        let encoded = serde_json::to_vec(&ctas).unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationPayload>(&encoded).unwrap(),
            ctas
        );

        let truncate = OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
            phase: TruncateLifecyclePhase::Executing,
            connector_operation_id: Uuid::now_v7(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some("04".repeat(16)),
            target_ref: "main".to_string(),
            request_digest: Some("request".to_string()),
            plan_digest: Some("plan".to_string()),
            state_digest: Some("state".to_string()),
            plan_summary: Some(DurableMutationSummary {
                file_count: 3,
                row_count: 4,
                total_bytes: 5,
            }),
            outcome: None,
            next_action: StatementNextAction::None,
        });
        let encoded = serde_json::to_vec(&truncate).unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationPayload>(&encoded).unwrap(),
            truncate
        );
    }

    #[test]
    fn unsafe_statement_state_shortcuts_are_rejected() {
        assert!(
            validate_operation_transition(OperationState::Preparing, OperationState::Finalized)
                .is_err()
        );
        assert!(
            validate_operation_transition(
                OperationState::CommitUnknown,
                OperationState::FailedKnownUncommitted,
            )
            .is_ok()
        );
        assert!(OperationState::Aborted.is_finished());
        assert!(!OperationState::CommitUnknown.is_finished());
    }
}
