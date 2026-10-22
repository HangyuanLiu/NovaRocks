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

//! Statement-local orchestration for ordinary connector writes.

use std::convert::Infallible;

use novarocks_spi::connector::{
    ConnectorWriteAbortOutcome, ConnectorWriteReceipt, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, LakePublicationFamily,
    LakePublicationId, LakePublicationTarget,
};

use crate::dml::attempt::{
    DmlPublicationAdjudicationOutcome, DmlPublicationAttempt, DmlPublicationFinalization,
};
use crate::dml::error::DmlError;

/// Catalog target frozen by one admitted statement. It is never persisted by
/// the Frontend and cannot be reused by a later statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub reference: Option<String>,
}

/// The statement-local write input. Provider handles and evidence remain in
/// the family stack and are intentionally absent from this carrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionSpec {
    pub publication_id: LakePublicationId,
    pub target: WriteTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionOutcome {
    pub committed_receipt: Option<ConnectorWriteReceipt>,
}

#[derive(Debug)]
pub enum CoordinatedWriteReport<H, A = Infallible> {
    NoOp,
    CommitRequired(H),
    AbortRequired { reason: Box<DmlError>, handle: A },
}

pub trait WriteExecutor {
    type CommitHandle;
    type AbortHandle;

    fn run_coordinated_write(
        &self,
        spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, DmlError>;

    fn abort(
        &self,
        spec: &WriteTransactionSpec,
        handle: &Self::AbortHandle,
    ) -> Result<ConnectorWriteAbortOutcome, String>;

    fn commit(
        &self,
        spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String>;

    /// Read only exact same-session publication evidence once after a
    /// commit-unknown result. Implementations must not restart, abort, clean
    /// up, or open a replacement connector session.
    fn adjudicate_publication(
        &self,
        _spec: &WriteTransactionSpec,
        _handle: &Self::CommitHandle,
        _evidence: ExternalMutationEvidence,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        Err("write executor does not expose same-session publication adjudication".to_string())
    }

    fn finalize(&self, spec: &WriteTransactionSpec) -> Result<(), String>;
}

/// Statement-local ordinary-write orchestration. It deliberately retains no
/// journal, StateStore admission, coordination lease, or recovery handle.
pub(crate) struct StatementWriteTransactionRunner<'a, E: WriteExecutor> {
    executor: &'a E,
    family: LakePublicationFamily,
}

impl<'a, E: WriteExecutor> StatementWriteTransactionRunner<'a, E> {
    pub(crate) const fn new(executor: &'a E, family: LakePublicationFamily) -> Self {
        Self { executor, family }
    }

    pub(crate) fn run(
        &self,
        spec: WriteTransactionSpec,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let target = LakePublicationTarget::try_new(
            spec.target.catalog.clone(),
            spec.target.namespace.clone(),
            Some(spec.target.table.clone()),
            spec.target.reference.clone(),
        )
        .map_err(|error| DmlError::executor(error.to_string()))?;
        let mut attempt =
            DmlPublicationAttempt::new(spec.publication_id, self.family, target, None);
        let report = match self.executor.run_coordinated_write(&spec) {
            Ok(report) => report,
            Err(error) => {
                attempt
                    .terminal_pre_dispatch_uncommitted()
                    .map_err(attempt_error)?;
                return Err(with_terminal(error, &attempt));
            }
        };
        match report {
            CoordinatedWriteReport::NoOp => {
                attempt.mark_dispatch_possible().map_err(attempt_error)?;
                self.finish_committed(
                    &spec,
                    &mut attempt,
                    None,
                    ExternalMutationFinalization::Complete,
                )
            }
            CoordinatedWriteReport::CommitRequired(handle) => {
                attempt.mark_dispatch_possible().map_err(attempt_error)?;
                match self.executor.commit(&spec, &handle) {
                    Ok(outcome) => self.complete_commit(&spec, &mut attempt, &handle, outcome),
                    Err(message) => {
                        attempt
                            .terminal_after_outer_failure()
                            .map_err(attempt_error)?;
                        Err(with_terminal(DmlError::commit(message), &attempt))
                    }
                }
            }
            CoordinatedWriteReport::AbortRequired { reason, handle } => {
                match self.executor.abort(&spec, &handle) {
                    Ok(ConnectorWriteAbortOutcome::KnownUncommitted { cleanup, .. }) => {
                        attempt
                            .terminal_known_uncommitted()
                            .map_err(attempt_error)?;
                        Err(with_terminal(
                            (*reason).with_uncommitted_cleanup(&cleanup),
                            &attempt,
                        ))
                    }
                    Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                        receipt,
                        finalization,
                        ..
                    }) => {
                        attempt.mark_dispatch_possible().map_err(attempt_error)?;
                        let settled =
                            self.finish_committed(&spec, &mut attempt, Some(receipt), finalization);
                        if reason.execution_failure().is_some() {
                            if let Err(error) = &settled {
                                tracing::warn!(%error, "publication finalization failed after the selected execution failure");
                            }
                            // Actual publication is committed; the earlier Task
                            // failure remains the selected SQL failure.
                            Err(with_terminal(*reason, &attempt))
                        } else {
                            settled
                        }
                    }
                    Ok(ConnectorWriteAbortOutcome::CommitUnknown { failure, .. }) => {
                        attempt.mark_dispatch_possible().map_err(attempt_error)?;
                        attempt.terminal_commit_unknown().map_err(attempt_error)?;
                        Err(with_terminal(
                            (*reason)
                                .prefer_selected_execution(DmlError::commit(failure.message())),
                            &attempt,
                        ))
                    }
                    Err(message) => {
                        attempt.mark_dispatch_possible().map_err(attempt_error)?;
                        attempt
                            .terminal_after_outer_failure()
                            .map_err(attempt_error)?;
                        Err(with_terminal(
                            (*reason).prefer_selected_execution(DmlError::commit(message)),
                            &attempt,
                        ))
                    }
                }
            }
        }
    }

    fn complete_commit(
        &self,
        spec: &WriteTransactionSpec,
        attempt: &mut DmlPublicationAttempt,
        handle: &E::CommitHandle,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } => self.finish_committed(spec, attempt, Some(receipt), finalization),
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
                attempt
                    .terminal_known_uncommitted()
                    .map_err(attempt_error)?;
                Err(with_terminal(
                    DmlError::commit(crate::connector::mutation::known_uncommitted_message(
                        failure, &cleanup,
                    )),
                    attempt,
                ))
            }
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                let token = attempt.begin_adjudication().map_err(attempt_error)?;
                match self.executor.adjudicate_publication(spec, handle, evidence) {
                    Ok(ExternalMutationOutcome::KnownCommitted {
                        receipt,
                        finalization,
                        ..
                    }) => {
                        let finalize =
                            combine_finalization(finalization, self.executor.finalize(spec));
                        let finalization = if finalize.is_ok() {
                            DmlPublicationFinalization::Succeeded
                        } else {
                            DmlPublicationFinalization::Failed
                        };
                        let terminal = attempt
                            .finish_adjudication(
                                token,
                                DmlPublicationAdjudicationOutcome::KnownCommitted,
                                finalization,
                            )
                            .map_err(attempt_error)?
                            .clone();
                        if let Err(message) = finalize {
                            return Err(DmlError::known_committed_finalization_failed(
                                terminal, message,
                            ));
                        }
                        Ok(WriteTransactionOutcome {
                            committed_receipt: Some(receipt),
                        })
                    }
                    Ok(ExternalMutationOutcome::KnownUncommitted { failure, cleanup }) => {
                        attempt
                            .finish_adjudication(
                                token,
                                DmlPublicationAdjudicationOutcome::CommitUnknown,
                                DmlPublicationFinalization::NotApplicable,
                            )
                            .map_err(attempt_error)?;
                        Err(with_terminal(
                            DmlError::commit(
                                crate::connector::mutation::known_uncommitted_message(
                                    failure, &cleanup,
                                ),
                            ),
                            attempt,
                        ))
                    }
                    Ok(ExternalMutationOutcome::CommitUnknown { failure, .. }) => {
                        attempt
                            .finish_adjudication(
                                token,
                                DmlPublicationAdjudicationOutcome::CommitUnknown,
                                DmlPublicationFinalization::NotApplicable,
                            )
                            .map_err(attempt_error)?;
                        Err(with_terminal(DmlError::commit(failure.message()), attempt))
                    }
                    Err(message) => {
                        attempt
                            .finish_adjudication(
                                token,
                                DmlPublicationAdjudicationOutcome::CommitUnknown,
                                DmlPublicationFinalization::NotApplicable,
                            )
                            .map_err(attempt_error)?;
                        Err(with_terminal(DmlError::commit(message), attempt))
                    }
                }
            }
        }
    }

    fn finish_committed(
        &self,
        spec: &WriteTransactionSpec,
        attempt: &mut DmlPublicationAttempt,
        receipt: Option<ConnectorWriteReceipt>,
        provider_finalization: ExternalMutationFinalization,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let finalized = combine_finalization(provider_finalization, self.executor.finalize(spec));
        let finalization = if finalized.is_ok() {
            DmlPublicationFinalization::Succeeded
        } else {
            DmlPublicationFinalization::Failed
        };
        let terminal = attempt
            .terminal_known_committed(finalization)
            .map_err(attempt_error)?
            .clone();
        if let Err(message) = finalized {
            return Err(DmlError::known_committed_finalization_failed(
                terminal, message,
            ));
        }
        Ok(WriteTransactionOutcome {
            committed_receipt: receipt,
        })
    }
}

fn combine_finalization(
    provider: ExternalMutationFinalization,
    local: Result<(), String>,
) -> Result<(), String> {
    match (provider, local) {
        (ExternalMutationFinalization::Complete, local) => local,
        (ExternalMutationFinalization::Failed(error), Ok(())) => Err(error.to_string()),
        (ExternalMutationFinalization::Failed(error), Err(local)) => {
            Err(format!("{error}; {local}"))
        }
    }
}

fn attempt_error(error: crate::dml::attempt::DmlPublicationAttemptError) -> DmlError {
    DmlError::executor(error.to_string())
}

fn with_terminal(mut error: DmlError, attempt: &DmlPublicationAttempt) -> DmlError {
    if let Some(terminal) = attempt.terminal() {
        error = error.with_publication_terminal(terminal.clone());
    }
    error
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationFailure,
        ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorProviderId,
        ExternalMutationEffect, ExternalMutationFinalization, LakePublicationDisposition,
        ProviderBindingEpoch,
    };

    use super::*;

    struct AbortRequiredExecutor {
        fact: novarocks_failure_contract::FailureFact,
        outcome: ConnectorWriteAbortOutcome,
        aborts: AtomicUsize,
    }

    impl WriteExecutor for AbortRequiredExecutor {
        type CommitHandle = ();
        type AbortHandle = ();
        fn run_coordinated_write(
            &self,
            _: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<(), ()>, DmlError> {
            Ok(CoordinatedWriteReport::AbortRequired {
                reason: Box::new(DmlError::from_execution(novarocks_query_application::execution_error::ExecutionFailureInput::new(
                    self.fact, novarocks_execution_contract::SafeDetail::truncating("unsupported diagnostic"),
                    novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator))),
                handle: (),
            })
        }
        fn abort(
            &self,
            _: &WriteTransactionSpec,
            _: &(),
        ) -> Result<ConnectorWriteAbortOutcome, String> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(self.outcome.clone())
        }
        fn commit(
            &self,
            _: &WriteTransactionSpec,
            _: &(),
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
            panic!("failed staging cannot commit");
        }
        fn finalize(&self, _: &WriteTransactionSpec) -> Result<(), String> {
            panic!("uncommitted failure cannot finalize");
        }
    }

    #[test]
    fn staging_refusal_identity_is_restored_only_after_the_required_abort() {
        use novarocks_failure_contract::FailureProjection;
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "unsupported diagnostic".into(),
            ),
        ] {
            let fact = original.failure_fact();
            let executor = AbortRequiredExecutor {
                fact,
                outcome: ConnectorWriteAbortOutcome::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                },
                aborts: AtomicUsize::new(0),
            };
            let error =
                StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
                    .run(spec())
                    .unwrap_err();
            assert_eq!(executor.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(error.execution_failure().unwrap().fact(), fact);
            assert_eq!(
                error.publication_terminal().unwrap().disposition(),
                LakePublicationDisposition::KnownUncommitted
            );
        }
        let fact = novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
            "unsupported diagnostic".into(),
        )
        .failure_fact();
        let executor = AbortRequiredExecutor {
            fact,
            outcome: ConnectorWriteAbortOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "abort outcome unknown",
                ),
                evidence: UnknownCommitExecutor::evidence(),
            },
            aborts: AtomicUsize::new(0),
        };
        let error = StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
            .run(spec())
            .unwrap_err();
        assert_eq!(executor.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(
            error.publication_terminal().unwrap().disposition(),
            LakePublicationDisposition::CommitUnknown
        );
        assert_eq!(
            error.execution_failure().unwrap().fact(),
            fact,
            "publication uncertainty cannot replace the original selected execution failure"
        );
    }

    enum Adjudication {
        Committed,
        CommittedCleanupFailed,
        Unproven,
    }

    struct UnknownCommitExecutor {
        adjudication: Adjudication,
        commits: AtomicUsize,
        adjudications: AtomicUsize,
        finalizations: AtomicUsize,
    }

    impl UnknownCommitExecutor {
        fn evidence() -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg-test").expect("provider"),
                    instance_id: ConnectorInstanceId::parse("iceberg-test").expect("instance"),
                },
                ProviderBindingEpoch::default(),
                ConnectorMutationOperationId::from_bytes([9; 16]),
                "write",
                Bytes::from_static(b"exact-same-session-evidence"),
            )
            .expect("evidence")
        }

        fn receipt() -> ConnectorWriteReceipt {
            ConnectorWriteReceipt::try_new(Bytes::from_static(b"receipt")).expect("receipt")
        }
    }

    impl WriteExecutor for UnknownCommitExecutor {
        type CommitHandle = ();
        type AbortHandle = Infallible;

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<()>, DmlError> {
            Ok(CoordinatedWriteReport::CommitRequired(()))
        }

        fn abort(
            &self,
            _spec: &WriteTransactionSpec,
            handle: &Self::AbortHandle,
        ) -> Result<ConnectorWriteAbortOutcome, String> {
            match *handle {}
        }

        fn commit(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &Self::CommitHandle,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "commit response lost",
                ),
                evidence: Self::evidence(),
            })
        }

        fn adjudicate_publication(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &Self::CommitHandle,
            _evidence: ExternalMutationEvidence,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
            self.adjudications.fetch_add(1, Ordering::SeqCst);
            match self.adjudication {
                Adjudication::Committed | Adjudication::CommittedCleanupFailed => {
                    Ok(ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        receipt: Self::receipt(),
                        finalization: if matches!(
                            self.adjudication,
                            Adjudication::CommittedCleanupFailed
                        ) {
                            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                                ConnectorMutationFailureKind::Unavailable,
                                "owned artifact cleanup incomplete",
                            ))
                        } else {
                            ExternalMutationFinalization::Complete
                        },
                    })
                }
                Adjudication::Unproven => Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::NotFound,
                        "exact marker is absent",
                    ),
                    cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                }),
            }
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            self.finalizations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn spec() -> WriteTransactionSpec {
        WriteTransactionSpec {
            publication_id: LakePublicationId::new_v7(),
            target: WriteTarget {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                reference: None,
            },
        }
    }

    struct SelectedStageFailureExecutor {
        input: novarocks_query_application::execution_error::ExecutionFailureInput,
        outcome: Result<ConnectorWriteAbortOutcome, String>,
        legacy: bool,
        fail_finalize: bool,
        stages: AtomicUsize,
        aborts: AtomicUsize,
        commits: AtomicUsize,
        finalizations: AtomicUsize,
    }
    impl WriteExecutor for SelectedStageFailureExecutor {
        type CommitHandle = ();
        type AbortHandle = ();
        fn run_coordinated_write(
            &self,
            _: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<(), ()>, DmlError> {
            self.stages.fetch_add(1, Ordering::SeqCst);
            Ok(CoordinatedWriteReport::AbortRequired {
                reason: if self.legacy {
                    Box::new(DmlError::executor("legacy staging rejection"))
                } else {
                    Box::new(DmlError::from_execution(self.input.clone()))
                },
                handle: (),
            })
        }
        fn abort(
            &self,
            _: &WriteTransactionSpec,
            _: &(),
        ) -> Result<ConnectorWriteAbortOutcome, String> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            self.outcome.clone()
        }
        fn commit(
            &self,
            _: &WriteTransactionSpec,
            _: &(),
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            Err("commit must not follow AbortRequired".into())
        }
        fn finalize(&self, _: &WriteTransactionSpec) -> Result<(), String> {
            self.finalizations.fetch_add(1, Ordering::SeqCst);
            if self.fail_finalize {
                Err("local finalization failed".into())
            } else {
                Ok(())
            }
        }
    }
    fn selected_stage_input() -> novarocks_query_application::execution_error::ExecutionFailureInput
    {
        use novarocks_execution_contract::{SafeDetail, TaskFailurePhase, identity::TaskIdentity};
        use novarocks_failure_contract::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite,
        };
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let source = novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            "private stage source",
        );
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(81, 7), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(2).unwrap(),
            BackendProcessId::new_v7(),
        );
        ExecutionFailureInput::new(
            source.failure_fact(),
            SafeDetail::new("Unexpected failure (reason 4:10)").unwrap(),
            ExecutionFailureSite::Task {
                identity,
                phase: TaskFailurePhase::Execution,
            },
        )
    }
    fn selected_executor(
        outcome: Result<ConnectorWriteAbortOutcome, String>,
    ) -> SelectedStageFailureExecutor {
        SelectedStageFailureExecutor {
            input: selected_stage_input(),
            outcome,
            legacy: false,
            fail_finalize: false,
            stages: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
            finalizations: AtomicUsize::new(0),
        }
    }
    #[test]
    fn selected_task_failure_survives_abort_known_committed_and_finalization() {
        for fail_finalize in [false, true] {
            let mut executor = selected_executor(Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                receipt: UnknownCommitExecutor::receipt(),
                finalization: ExternalMutationFinalization::Complete,
            }));
            executor.fail_finalize = fail_finalize;
            let error =
                StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
                    .run(spec())
                    .unwrap_err();
            assert_eq!(error.execution_failure(), Some(&executor.input));
            let terminal = error
                .publication_terminal()
                .expect("actual committed publication");
            assert_eq!(
                terminal.disposition(),
                LakePublicationDisposition::KnownCommitted
            );
            assert!(terminal.do_not_retry());
            assert_eq!(executor.stages.load(Ordering::SeqCst), 1);
            assert_eq!(executor.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(executor.commits.load(Ordering::SeqCst), 0);
            assert_eq!(executor.finalizations.load(Ordering::SeqCst), 1);
        }
    }
    #[test]
    fn selected_task_failure_survives_unknown_abort_and_outer_failure() {
        for outcome in [
            Ok(ConnectorWriteAbortOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "abort publication unknown",
                ),
                evidence: UnknownCommitExecutor::evidence(),
            }),
            Err("abort response lost".to_owned()),
        ] {
            let executor = selected_executor(outcome);
            let error =
                StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
                    .run(spec())
                    .unwrap_err();
            assert_eq!(error.execution_failure(), Some(&executor.input));
            let terminal = error
                .publication_terminal()
                .expect("actual unknown publication");
            assert_eq!(
                terminal.disposition(),
                LakePublicationDisposition::CommitUnknown
            );
            assert!(terminal.do_not_retry());
            assert_eq!(executor.stages.load(Ordering::SeqCst), 1);
            assert_eq!(executor.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(executor.commits.load(Ordering::SeqCst), 0);
            assert_eq!(executor.finalizations.load(Ordering::SeqCst), 0);
        }
    }
    #[test]
    fn legacy_abort_known_committed_keeps_its_original_success_verdict() {
        let mut executor = selected_executor(Ok(ConnectorWriteAbortOutcome::KnownCommitted {
            receipt: UnknownCommitExecutor::receipt(),
            finalization: ExternalMutationFinalization::Complete,
        }));
        executor.legacy = true;
        let result = StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
            .run(spec())
            .unwrap();
        assert_eq!(
            result.committed_receipt,
            Some(UnknownCommitExecutor::receipt())
        );
        assert_eq!(executor.stages.load(Ordering::SeqCst), 1);
        assert_eq!(executor.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(executor.commits.load(Ordering::SeqCst), 0);
        assert_eq!(executor.finalizations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn exact_positive_adjudication_commits_once() {
        let executor = UnknownCommitExecutor {
            adjudication: Adjudication::Committed,
            commits: AtomicUsize::new(0),
            adjudications: AtomicUsize::new(0),
            finalizations: AtomicUsize::new(0),
        };

        let outcome = StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
            .run(spec())
            .expect("exact positive adjudication commits");
        assert!(outcome.committed_receipt.is_some());
        assert_eq!(executor.commits.load(Ordering::SeqCst), 1);
        assert_eq!(executor.adjudications.load(Ordering::SeqCst), 1);
        assert_eq!(executor.finalizations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn direct_known_commit_keeps_failed_cleanup_without_adjudication_or_retry() {
        let executor = UnknownCommitExecutor {
            adjudication: Adjudication::Unproven,
            commits: AtomicUsize::new(0),
            adjudications: AtomicUsize::new(0),
            finalizations: AtomicUsize::new(0),
        };
        let spec = spec();
        let target = LakePublicationTarget::try_new(
            spec.target.catalog.clone(),
            spec.target.namespace.clone(),
            Some(spec.target.table.clone()),
            spec.target.reference.clone(),
        )
        .unwrap();
        let mut attempt = DmlPublicationAttempt::new(
            spec.publication_id,
            LakePublicationFamily::Write,
            target,
            None,
        );
        attempt.mark_dispatch_possible().unwrap();
        let error = StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
            .complete_commit(
                &spec,
                &mut attempt,
                &(),
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: UnknownCommitExecutor::receipt(),
                    finalization: ExternalMutationFinalization::Failed(
                        ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Unavailable,
                            "prior attempt artifacts remain",
                        ),
                    ),
                },
            )
            .expect_err("cleanup failure is visible after proven publication");
        assert_eq!(
            error.publication_terminal().unwrap().disposition(),
            LakePublicationDisposition::KnownCommitted
        );
        assert!(error.publication_terminal().unwrap().do_not_retry());
        assert_eq!(
            attempt.finalization(),
            Some(DmlPublicationFinalization::Failed)
        );
        assert!(error.to_string().contains("prior attempt artifacts remain"));
        assert_eq!(executor.commits.load(Ordering::SeqCst), 0);
        assert_eq!(executor.adjudications.load(Ordering::SeqCst), 0);
        assert_eq!(executor.finalizations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn positive_adjudication_keeps_known_commit_when_provider_cleanup_failed() {
        let executor = UnknownCommitExecutor {
            adjudication: Adjudication::CommittedCleanupFailed,
            commits: AtomicUsize::new(0),
            adjudications: AtomicUsize::new(0),
            finalizations: AtomicUsize::new(0),
        };
        let error = StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
            .run(spec())
            .expect_err("known commit cleanup failure is visible");
        let terminal = error.publication_terminal().unwrap();
        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::KnownCommitted
        );
        assert!(terminal.do_not_retry());
        assert!(
            error
                .to_string()
                .contains("owned artifact cleanup incomplete")
        );
        assert_eq!(executor.commits.load(Ordering::SeqCst), 1);
        assert_eq!(executor.adjudications.load(Ordering::SeqCst), 1);
        assert_eq!(executor.finalizations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn negative_adjudication_stays_unknown_without_finalization_or_retry() {
        let executor = UnknownCommitExecutor {
            adjudication: Adjudication::Unproven,
            commits: AtomicUsize::new(0),
            adjudications: AtomicUsize::new(0),
            finalizations: AtomicUsize::new(0),
        };

        let error = StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
            .run(spec())
            .expect_err("negative adjudication remains unknown");
        assert_eq!(
            error
                .publication_terminal()
                .expect("terminal")
                .disposition(),
            LakePublicationDisposition::CommitUnknown
        );
        assert_eq!(executor.commits.load(Ordering::SeqCst), 1);
        assert_eq!(executor.adjudications.load(Ordering::SeqCst), 1);
        assert_eq!(executor.finalizations.load(Ordering::SeqCst), 0);
    }
}
