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

use crate::dml::model::{
    CleanupAttempt, CommitOpKind, CommitOutcome, CommitServiceError, IcebergCleanupOutcomeRecord,
    IcebergCommitOutcomeRecord, IcebergOperationFailureKind, IcebergOperationFailureRecord,
    IcebergOperationNextAction, IcebergRecoveryEvidenceRecord, OperationFact, OperationState,
    RecoveryEvidence,
};

/// Map a typed commit result to a durable operation fact. Faithful re-authoring
/// of core `operation_lifecycle::operation_fact_from_commit_result`
/// (`novarocks/core/src/connector/iceberg/operation_lifecycle.rs:36`).
pub fn operation_fact_from_commit_result(
    result: Result<&CommitOutcome, &CommitServiceError>,
) -> OperationFact {
    match result {
        Ok(outcome) => OperationFact {
            state: OperationState::Committed,
            commit_outcome: Some(IcebergCommitOutcomeRecord {
                snapshot_id: outcome.new_snapshot_id,
                written_manifest_paths: outcome.written_manifest_paths.clone(),
            }),
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: None,
        },
        Err(CommitServiceError::KnownUncommitted { message, cleanup }) => OperationFact {
            state: OperationState::FailedKnownUncommitted,
            commit_outcome: None,
            cleanup_outcome: Some(cleanup_outcome_from_attempt(cleanup)),
            recovery_evidence: None,
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::KnownUncommitted,
                message: message.clone(),
                next_action: cleanup_next_action(cleanup),
            }),
        },
        Err(CommitServiceError::InvalidInput { message }) => OperationFact {
            state: OperationState::FailedKnownUncommitted,
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::KnownUncommitted,
                message: message.clone(),
                next_action: IcebergOperationNextAction::None,
            }),
        },
        Err(CommitServiceError::FinalizeFailedKnownCommitted {
            outcome,
            finalize_error,
            evidence,
        }) => OperationFact {
            state: OperationState::FinalizeFailedKnownCommitted,
            commit_outcome: outcome.as_ref().map(|outcome| IcebergCommitOutcomeRecord {
                snapshot_id: outcome.new_snapshot_id,
                written_manifest_paths: outcome.written_manifest_paths.clone(),
            }),
            cleanup_outcome: None,
            recovery_evidence: Some(recovery_evidence_record_from_evidence(evidence)),
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
                message: finalize_error.clone(),
                next_action: IcebergOperationNextAction::RetryFinalize,
            }),
        },
        Err(CommitServiceError::Unknown { message, evidence }) => OperationFact {
            state: OperationState::CommitUnknown,
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: Some(recovery_evidence_record_from_evidence(evidence)),
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::Unknown,
                message: message.clone(),
                next_action: IcebergOperationNextAction::ManualInspect,
            }),
        },
    }
}

/// Post-commit finalization failure: metadata is known-committed, do not retry
/// the write. Mirrors core `operation_fact_from_finalize_failure`
/// (`operation_lifecycle.rs:104`).
pub fn operation_fact_from_finalize_failure(message: String) -> OperationFact {
    OperationFact {
        state: OperationState::FinalizeFailedKnownCommitted,
        commit_outcome: None,
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: Some(IcebergOperationFailureRecord {
            kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
            message,
            next_action: IcebergOperationNextAction::RetryFinalize,
        }),
    }
}

/// Writer aborted before commit. Mirrors core
/// `operation_fact_update_from_write_abort`
/// (`novarocks/core/src/engine/write_operation_lifecycle.rs:70`): known-
/// uncommitted; when staged files exist, record a not-yet-attempted cleanup
/// outcome and request `RetryAbort` so recovery can distinguish "abort with
/// cleanup pending" from "nothing to clean up".
pub fn operation_fact_from_writer_abort(reason: String, has_staged: bool) -> OperationFact {
    let cleanup_outcome = has_staged.then_some(IcebergCleanupOutcomeRecord {
        attempted: false,
        error_count: 0,
        error_paths: Vec::new(),
    });
    OperationFact {
        state: OperationState::FailedKnownUncommitted,
        commit_outcome: None,
        cleanup_outcome,
        recovery_evidence: None,
        failure: Some(IcebergOperationFailureRecord {
            kind: IcebergOperationFailureKind::KnownUncommitted,
            message: reason,
            next_action: if has_staged {
                IcebergOperationNextAction::RetryAbort
            } else {
                IcebergOperationNextAction::None
            },
        }),
    }
}

fn cleanup_next_action(cleanup: &CleanupAttempt) -> IcebergOperationNextAction {
    if cleanup.attempted && cleanup.error_count == 0 {
        IcebergOperationNextAction::None
    } else {
        IcebergOperationNextAction::RetryAbort
    }
}

fn commit_op_kind_record_name(kind: CommitOpKind) -> &'static str {
    match kind {
        CommitOpKind::FastAppend => "fast_append",
        CommitOpKind::Overwrite => "overwrite",
        CommitOpKind::RowDelta => "row_delta",
        CommitOpKind::RowDeltaDv => "row_delta_dv",
        CommitOpKind::RowDeltaDvFromFiles => "row_delta_dv_from_files",
        CommitOpKind::RewriteDataFiles => "rewrite_data_files",
        CommitOpKind::CowUpdate => "cow_update",
        CommitOpKind::Truncate => "truncate",
        CommitOpKind::OverwritePartitions => "overwrite_partitions",
        CommitOpKind::RewriteManifests => "rewrite_manifests",
    }
}

fn cleanup_outcome_from_attempt(cleanup: &CleanupAttempt) -> IcebergCleanupOutcomeRecord {
    IcebergCleanupOutcomeRecord {
        attempted: cleanup.attempted,
        error_count: cleanup.error_count as i64,
        error_paths: cleanup.error_paths.clone(),
    }
}

fn recovery_evidence_record_from_evidence(
    evidence: &RecoveryEvidence,
) -> IcebergRecoveryEvidenceRecord {
    IcebergRecoveryEvidenceRecord {
        table_ident: evidence.table_ident.clone(),
        commit_op_kind: commit_op_kind_record_name(evidence.op_kind).to_string(),
        base_snapshot_id: evidence.base_snapshot_id,
        base_sequence_number: Some(evidence.base_sequence_number),
        staging_dir: evidence.staging_dir.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence() -> RecoveryEvidence {
        RecoveryEvidence {
            table_ident: "cat.ns.tbl".to_string(),
            op_kind: CommitOpKind::FastAppend,
            base_snapshot_id: Some(7),
            base_sequence_number: 3,
            staging_dir: "/w/staging".to_string(),
        }
    }

    #[test]
    fn ok_maps_to_committed_with_snapshot() {
        let outcome = CommitOutcome {
            new_snapshot_id: 42,
            written_manifest_paths: vec!["m1".to_string()],
        };
        let fact = operation_fact_from_commit_result(Ok(&outcome));
        assert_eq!(fact.state, OperationState::Committed);
        assert_eq!(fact.commit_outcome.as_ref().unwrap().snapshot_id, 42);
        assert!(fact.failure.is_none());
    }

    #[test]
    fn known_uncommitted_maps_to_failed_with_cleanup_next_action() {
        let err = CommitServiceError::known_uncommitted(
            "conflict".to_string(),
            CleanupAttempt::not_attempted(),
        );
        let fact = operation_fact_from_commit_result(Err(&err));
        assert_eq!(fact.state, OperationState::FailedKnownUncommitted);
        assert!(fact.cleanup_outcome.is_some());
        let failure = fact.failure.unwrap();
        assert_eq!(failure.kind, IcebergOperationFailureKind::KnownUncommitted);
        // not_attempted => not (attempted && error_count==0) => RetryAbort
        assert_eq!(failure.next_action, IcebergOperationNextAction::RetryAbort);
    }

    #[test]
    fn invalid_input_maps_to_failed_no_next_action() {
        let err = CommitServiceError::invalid_input("bad".to_string());
        let fact = operation_fact_from_commit_result(Err(&err));
        assert_eq!(fact.state, OperationState::FailedKnownUncommitted);
        assert!(fact.cleanup_outcome.is_none());
        assert_eq!(
            fact.failure.unwrap().next_action,
            IcebergOperationNextAction::None
        );
    }

    #[test]
    fn unknown_maps_to_commit_unknown_manual_inspect() {
        let err = CommitServiceError::unknown("lost reply".to_string(), evidence());
        let fact = operation_fact_from_commit_result(Err(&err));
        assert_eq!(fact.state, OperationState::CommitUnknown);
        let recovery = fact
            .recovery_evidence
            .as_ref()
            .expect("recovery evidence recorded");
        assert_eq!(recovery.table_ident, "cat.ns.tbl");
        assert_eq!(recovery.commit_op_kind, "fast_append");
        assert_eq!(recovery.base_snapshot_id, Some(7));
        assert_eq!(recovery.base_sequence_number, Some(3));
        assert_eq!(recovery.staging_dir, "/w/staging");
        assert_eq!(
            fact.failure.unwrap().next_action,
            IcebergOperationNextAction::ManualInspect
        );
    }

    #[test]
    fn finalize_failed_known_committed_maps_with_retry_finalize() {
        let err = CommitServiceError::finalize_failed_known_committed(
            Some(CommitOutcome {
                new_snapshot_id: 9,
                written_manifest_paths: vec![],
            }),
            "finalize boom".to_string(),
            evidence(),
        );
        let fact = operation_fact_from_commit_result(Err(&err));
        assert_eq!(fact.state, OperationState::FinalizeFailedKnownCommitted);
        assert_eq!(fact.commit_outcome.as_ref().unwrap().snapshot_id, 9);
        assert_eq!(
            fact.failure.unwrap().next_action,
            IcebergOperationNextAction::RetryFinalize
        );
    }

    #[test]
    fn finalize_failure_helper_maps_to_finalize_failed() {
        let fact = operation_fact_from_finalize_failure("boom".to_string());
        assert_eq!(fact.state, OperationState::FinalizeFailedKnownCommitted);
        assert_eq!(
            fact.failure.unwrap().next_action,
            IcebergOperationNextAction::RetryFinalize
        );
    }

    #[test]
    fn writer_abort_with_staged_files_requests_retry_abort() {
        let fact = operation_fact_from_writer_abort("timeout".to_string(), true);
        assert_eq!(fact.state, OperationState::FailedKnownUncommitted);
        let cleanup = fact
            .cleanup_outcome
            .as_ref()
            .expect("staged files record a pending cleanup outcome");
        assert!(!cleanup.attempted);
        assert_eq!(
            fact.failure.unwrap().next_action,
            IcebergOperationNextAction::RetryAbort
        );
    }

    #[test]
    fn writer_abort_without_staged_files_needs_no_action() {
        let fact = operation_fact_from_writer_abort("empty".to_string(), false);
        assert!(fact.cleanup_outcome.is_none());
        assert_eq!(
            fact.failure.unwrap().next_action,
            IcebergOperationNextAction::None
        );
    }

    // Pins the durable commit-op-kind strings that are persisted into the
    // operation journal and read back by recovery. The frontend mirror has no
    // compile-time parity link to core, so this guard is the only protection
    // against a typo drifting a persisted value (mirrors core's
    // `commit_op_kind_record_names_are_stable`).
    #[test]
    fn commit_op_kind_record_names_are_stable() {
        assert_eq!(commit_op_kind_record_name(CommitOpKind::FastAppend), "fast_append");
        assert_eq!(commit_op_kind_record_name(CommitOpKind::Overwrite), "overwrite");
        assert_eq!(commit_op_kind_record_name(CommitOpKind::RowDelta), "row_delta");
        assert_eq!(commit_op_kind_record_name(CommitOpKind::RowDeltaDv), "row_delta_dv");
        assert_eq!(
            commit_op_kind_record_name(CommitOpKind::RowDeltaDvFromFiles),
            "row_delta_dv_from_files"
        );
        assert_eq!(
            commit_op_kind_record_name(CommitOpKind::RewriteDataFiles),
            "rewrite_data_files"
        );
        assert_eq!(commit_op_kind_record_name(CommitOpKind::CowUpdate), "cow_update");
        assert_eq!(commit_op_kind_record_name(CommitOpKind::Truncate), "truncate");
        assert_eq!(
            commit_op_kind_record_name(CommitOpKind::OverwritePartitions),
            "overwrite_partitions"
        );
        assert_eq!(
            commit_op_kind_record_name(CommitOpKind::RewriteManifests),
            "rewrite_manifests"
        );
    }
}
