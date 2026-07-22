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

use crate::dml::error::DmlError;
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    CleanupAttempt, CommitOutcome, CommitServiceError, CreatePreparingRequest, OperationState,
    WriteTransactionOutcome, WriteTransactionSpec,
};
use crate::dml::reconcile;
use crate::dml::now_unix_millis;

/// The outcome shape of a coordinated write, as reported by the executor. `H` is
/// the executor's commit handle carried from the write step into the commit step
/// (unit `()` for the fake; the real payload for DML-2).
#[derive(Clone, Debug)]
pub enum CoordinatedWriteReport<H> {
    /// Writer aborted before commit; `has_staged` drives cleanup next-action.
    Aborted { reason: String, has_staged: bool },
    /// Nothing to commit (append no-op). Runner records Aborting -> Aborted.
    NoOp,
    /// Committable output; the handle is passed back to `commit`.
    Committable(H),
}

/// Side-effecting dependencies of a write transaction. DML-1 ships only a fake;
/// the real implementation (DML-2) wraps the execution coordinator + typed
/// commit service + cache finalization.
pub trait WriteExecutor {
    /// Opaque payload carried from `run_coordinated_write` to `commit`.
    type CommitHandle;

    /// Run the coordinated writer plan.
    fn run_coordinated_write(
        &self,
        spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle>, String>;

    /// Commit the collected writer output via the typed commit service.
    // The Err type is core's genuine `CommitServiceError` contract (also consumed
    // by-reference by the reconcile classifier); boxing it would diverge from core
    // and burden DML-2's real executor, and this is a cold I/O path — so allow the
    // large-err lint here. Placed on the trait method; impls do not need their own.
    #[allow(clippy::result_large_err)]
    fn commit(
        &self,
        spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
    ) -> Result<CommitOutcome, CommitServiceError>;

    /// Post-commit finalization (cache invalidation).
    fn finalize(&self, spec: &WriteTransactionSpec) -> Result<(), String>;
}

/// Fencing/admission gate embedded at the start of every write transaction.
/// CP-3 plugs `IncarnationGate::admit_writes` in here; DML-1 always admits.
/// `Send + Sync` so a `DmlService` can hold `Arc<dyn WriteAdmission>`.
pub trait WriteAdmission: Send + Sync {
    fn admit(&self) -> Result<(), DmlError>;
}

/// The single-FE no-op admission used until CP-3 wires real fencing.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysAdmit;

impl WriteAdmission for AlwaysAdmit {
    fn admit(&self) -> Result<(), DmlError> {
        Ok(())
    }
}

/// Drives one Iceberg write transaction through the operation state machine,
/// persisting facts via the journal and delegating side effects to the executor.
/// Re-authors core `IcebergWriteTransactionRunner::run`
/// (`novarocks/core/src/engine/write_transaction.rs:256`) with narrow ports.
pub struct WriteTransactionRunner<'a, E: WriteExecutor> {
    journal: &'a dyn OperationJournal,
    executor: &'a E,
    admission: &'a dyn WriteAdmission,
}

impl<'a, E: WriteExecutor> WriteTransactionRunner<'a, E> {
    pub fn new(
        journal: &'a dyn OperationJournal,
        executor: &'a E,
        admission: &'a dyn WriteAdmission,
    ) -> Self {
        Self {
            journal,
            executor,
            admission,
        }
    }

    pub fn run(
        &self,
        spec: WriteTransactionSpec,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        self.admission.admit()?;

        let request = CreatePreparingRequest {
            operation_kind: spec.operation_kind,
            operation_subkind: None,
            target: spec.target.clone(),
            attempt_id: spec.attempt_id.clone(),
            base_snapshot_id: spec.base_snapshot_id,
            base_snapshot_map: spec.base_snapshot_map.clone(),
            staged_artifacts: Vec::new(),
            created_at_ms: now_unix_millis(),
        };
        let operation_id = self.journal.create_preparing(request)?;

        let report = match self.executor.run_coordinated_write(&spec) {
            Ok(report) => report,
            Err(message) => {
                let error = CommitServiceError::known_uncommitted(
                    message.clone(),
                    CleanupAttempt::not_attempted(),
                );
                self.journal.record_fact(
                    operation_id,
                    reconcile::operation_fact_from_commit_result(Err(&error)),
                )?;
                return Err(DmlError::executor(message));
            }
        };

        let handle = match report {
            CoordinatedWriteReport::Aborted { reason, has_staged } => {
                self.journal.record_fact(
                    operation_id,
                    reconcile::operation_fact_from_writer_abort(reason.clone(), has_staged),
                )?;
                return Err(DmlError::executor(format!(
                    "iceberg write operation {operation_id} aborted before commit: {reason}"
                )));
            }
            CoordinatedWriteReport::NoOp => {
                self.journal.transition(operation_id, OperationState::Aborting)?;
                self.journal.transition(operation_id, OperationState::Aborted)?;
                return Ok(WriteTransactionOutcome {
                    operation_id: None,
                    committed_snapshot_id: None,
                });
            }
            CoordinatedWriteReport::Committable(handle) => handle,
        };

        self.journal
            .transition(operation_id, OperationState::Committing)?;
        match self.executor.commit(&spec, &handle) {
            Ok(outcome) => {
                let snapshot_id = outcome.new_snapshot_id;
                self.journal.record_fact(
                    operation_id,
                    reconcile::operation_fact_from_commit_result(Ok(&outcome)),
                )?;
                self.journal
                    .transition(operation_id, OperationState::Finalizing)?;
                match self.executor.finalize(&spec) {
                    Ok(()) => {
                        self.journal
                            .transition(operation_id, OperationState::Finalized)?;
                        Ok(WriteTransactionOutcome {
                            operation_id: Some(operation_id),
                            committed_snapshot_id: Some(snapshot_id),
                        })
                    }
                    Err(message) => {
                        self.journal.record_fact(
                            operation_id,
                            reconcile::operation_fact_from_finalize_failure(message.clone()),
                        )?;
                        Err(DmlError::finalize(format!(
                            "iceberg write operation {operation_id}: metadata commit succeeded \
                             (snapshot {snapshot_id}, known committed) but finalization failed; \
                             do not retry the write: {message}"
                        )))
                    }
                }
            }
            Err(commit_error) => {
                self.journal.record_fact(
                    operation_id,
                    reconcile::operation_fact_from_commit_result(Err(&commit_error)),
                )?;
                Err(DmlError::commit(commit_error.message().to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::dml::error::DmlErrorKind;
    use crate::dml::journal::testing::InMemoryOperationJournal;
    use crate::dml::model::{
        CommitOpKind, OperationKind, OperationTarget, RecoveryEvidence,
    };

    struct FakeExecutor {
        write: Result<CoordinatedWriteReport<()>, String>,
        commit: Result<CommitOutcome, CommitServiceError>,
        finalize: Result<(), String>,
    }

    impl Default for FakeExecutor {
        fn default() -> Self {
            Self {
                write: Ok(CoordinatedWriteReport::Committable(())),
                commit: Ok(CommitOutcome {
                    new_snapshot_id: 42,
                    written_manifest_paths: vec![],
                }),
                finalize: Ok(()),
            }
        }
    }

    impl WriteExecutor for FakeExecutor {
        type CommitHandle = ();

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<()>, String> {
            self.write.clone()
        }

        fn commit(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &(),
        ) -> Result<CommitOutcome, CommitServiceError> {
            self.commit.clone()
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            self.finalize.clone()
        }
    }

    struct DenyAdmission;
    impl WriteAdmission for DenyAdmission {
        fn admit(&self) -> Result<(), DmlError> {
            Err(DmlError::admission("fenced"))
        }
    }

    fn spec() -> WriteTransactionSpec {
        WriteTransactionSpec {
            target: OperationTarget {
                catalog: "cat".to_string(),
                namespace: "ns".to_string(),
                table: "tbl".to_string(),
                ref_name: None,
            },
            operation_kind: OperationKind::InsertAppend,
            attempt_id: "attempt-1".to_string(),
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
        }
    }

    fn evidence() -> RecoveryEvidence {
        RecoveryEvidence {
            table_ident: "cat.ns.tbl".to_string(),
            op_kind: CommitOpKind::FastAppend,
            base_snapshot_id: None,
            base_sequence_number: 0,
            staging_dir: "/w/s".to_string(),
        }
    }

    #[test]
    fn happy_path_reaches_finalized() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor::default();
        let admit = AlwaysAdmit;
        let runner = WriteTransactionRunner::new(&journal, &executor, &admit);

        let outcome = runner.run(spec()).unwrap();
        assert_eq!(outcome.operation_id, Some(1));
        assert_eq!(outcome.committed_snapshot_id, Some(42));
        assert_eq!(
            journal.load(1).unwrap().unwrap().state,
            OperationState::Finalized
        );
        assert_eq!(
            journal.load(1).unwrap().unwrap().commit_outcome.unwrap().snapshot_id,
            42
        );
    }

    #[test]
    fn empty_write_aborts_as_noop() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor {
            write: Ok(CoordinatedWriteReport::NoOp),
            ..FakeExecutor::default()
        };
        let admit = AlwaysAdmit;
        let runner = WriteTransactionRunner::new(&journal, &executor, &admit);

        let outcome = runner.run(spec()).unwrap();
        assert_eq!(outcome.operation_id, None);
        assert_eq!(
            journal.load(1).unwrap().unwrap().state,
            OperationState::Aborted
        );
    }

    #[test]
    fn writer_abort_records_failed_known_uncommitted() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor {
            write: Ok(CoordinatedWriteReport::Aborted {
                reason: "timeout".to_string(),
                has_staged: true,
            }),
            ..FakeExecutor::default()
        };
        let admit = AlwaysAdmit;
        let runner = WriteTransactionRunner::new(&journal, &executor, &admit);

        let err = runner.run(spec()).unwrap_err();
        assert_eq!(err.kind(), DmlErrorKind::Executor);
        let stored = journal.load(1).unwrap().unwrap();
        assert_eq!(stored.state, OperationState::FailedKnownUncommitted);
        assert_eq!(
            stored.failure.unwrap().next_action,
            crate::dml::model::IcebergOperationNextAction::RetryAbort
        );
    }

    #[test]
    fn coordinated_write_error_records_known_uncommitted() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor {
            write: Err("plan blew up".to_string()),
            ..FakeExecutor::default()
        };
        let admit = AlwaysAdmit;
        let runner = WriteTransactionRunner::new(&journal, &executor, &admit);

        let err = runner.run(spec()).unwrap_err();
        assert_eq!(err.kind(), DmlErrorKind::Executor);
        assert_eq!(
            journal.load(1).unwrap().unwrap().state,
            OperationState::FailedKnownUncommitted
        );
    }

    #[test]
    fn commit_unknown_does_not_replay() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor {
            commit: Err(CommitServiceError::unknown(
                "reply lost".to_string(),
                evidence(),
            )),
            ..FakeExecutor::default()
        };
        let admit = AlwaysAdmit;
        let runner = WriteTransactionRunner::new(&journal, &executor, &admit);

        let err = runner.run(spec()).unwrap_err();
        assert_eq!(err.kind(), DmlErrorKind::Commit);
        let stored = journal.load(1).unwrap().unwrap();
        assert_eq!(stored.state, OperationState::CommitUnknown);
        assert_eq!(
            stored.failure.unwrap().next_action,
            crate::dml::model::IcebergOperationNextAction::ManualInspect
        );
    }

    #[test]
    fn commit_known_uncommitted_is_terminal_failure() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor {
            commit: Err(CommitServiceError::known_uncommitted(
                "conflict".to_string(),
                CleanupAttempt::not_attempted(),
            )),
            ..FakeExecutor::default()
        };
        let admit = AlwaysAdmit;
        let runner = WriteTransactionRunner::new(&journal, &executor, &admit);

        let err = runner.run(spec()).unwrap_err();
        assert_eq!(err.kind(), DmlErrorKind::Commit);
        assert_eq!(
            journal.load(1).unwrap().unwrap().state,
            OperationState::FailedKnownUncommitted
        );
    }

    #[test]
    fn finalize_failure_is_known_committed_no_retry() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor {
            finalize: Err("cache invalidation failed".to_string()),
            ..FakeExecutor::default()
        };
        let admit = AlwaysAdmit;
        let runner = WriteTransactionRunner::new(&journal, &executor, &admit);

        let err = runner.run(spec()).unwrap_err();
        assert_eq!(err.kind(), DmlErrorKind::Finalize);
        let stored = journal.load(1).unwrap().unwrap();
        assert_eq!(stored.state, OperationState::FinalizeFailedKnownCommitted);
        assert_eq!(
            stored.failure.unwrap().next_action,
            crate::dml::model::IcebergOperationNextAction::RetryFinalize
        );
    }

    #[test]
    fn denied_admission_creates_no_operation() {
        let journal = InMemoryOperationJournal::default();
        let executor = FakeExecutor::default();
        let deny = DenyAdmission;
        let runner = WriteTransactionRunner::new(&journal, &executor, &deny);

        let error = runner.run(spec()).unwrap_err();
        assert_eq!(error.kind(), DmlErrorKind::Admission);
        assert!(journal.load(1).unwrap().is_none());
    }
}
