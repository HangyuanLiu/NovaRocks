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

//! Frontend-owned durable lifecycle for a connector write.
//!
//! Provider-specific commit representations end at the Core reverse port.
//! This module observes only the connector terminal outcome and persists its
//! SPI wire envelope without decoding the provider payload.

use std::convert::Infallible;

use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorWriteAbortOutcome,
    ConnectorWriteReceipt, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, LakePublicationFamily, LakePublicationTarget,
};

use crate::dml::attempt::{
    DmlPublicationAdjudicationOutcome, DmlPublicationAttempt, DmlPublicationFinalization,
};
use crate::dml::coordination::ActiveDmlOperation;
use crate::dml::error::DmlError;
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    CreatePreparingRequest, DmlOperationId, OperationState, WriteTransactionOutcome,
    WriteTransactionSpec,
};
use crate::dml::now_unix_millis;
use crate::dml::reconcile;

#[derive(Clone, Debug)]
pub enum CoordinatedWriteReport<H, A = Infallible> {
    Aborted { reason: String },
    NoOp,
    CommitRequired(H),
    AbortRequired { reason: String, handle: A },
}

pub trait WriteExecutor {
    type CommitHandle;
    type AbortHandle;

    #[allow(
        clippy::result_large_err,
        reason = "The public transaction runner retains DmlError so typed analysis errors reach the client boundary."
    )]
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

    /// Read only the exact same-session publication evidence once after a
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

/// Statement-local ordinary-write orchestration.  It deliberately retains no
/// journal, StateStore admission, coordination lease, or recovery handle.
pub(crate) struct StatementWriteTransactionRunner<'a, E: WriteExecutor> {
    executor: &'a E,
    family: LakePublicationFamily,
}

impl<'a, E: WriteExecutor> StatementWriteTransactionRunner<'a, E> {
    pub(crate) const fn new(executor: &'a E, family: LakePublicationFamily) -> Self {
        Self { executor, family }
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn run(
        &self,
        spec: WriteTransactionSpec,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let target = LakePublicationTarget::try_new(
            spec.target.catalog.clone(),
            spec.target.namespace.clone(),
            Some(spec.target.table.clone()),
            spec.target.ref_name.clone(),
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
            CoordinatedWriteReport::Aborted { reason } => {
                attempt
                    .terminal_pre_dispatch_uncommitted()
                    .map_err(attempt_error)?;
                Err(with_terminal(DmlError::executor(reason), &attempt))
            }
            CoordinatedWriteReport::NoOp => {
                attempt.mark_dispatch_possible().map_err(attempt_error)?;
                self.finish_committed(&spec, &mut attempt, None)
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
                    Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }) => {
                        attempt
                            .terminal_known_uncommitted()
                            .map_err(attempt_error)?;
                        Err(with_terminal(DmlError::executor(reason), &attempt))
                    }
                    Ok(ConnectorWriteAbortOutcome::KnownCommitted { receipt, .. }) => {
                        attempt.mark_dispatch_possible().map_err(attempt_error)?;
                        self.finish_committed(&spec, &mut attempt, Some(receipt))
                    }
                    Ok(ConnectorWriteAbortOutcome::CommitUnknown { failure, .. }) => {
                        attempt.mark_dispatch_possible().map_err(attempt_error)?;
                        attempt.terminal_commit_unknown().map_err(attempt_error)?;
                        Err(with_terminal(DmlError::commit(failure.message()), &attempt))
                    }
                    Err(message) => {
                        attempt.mark_dispatch_possible().map_err(attempt_error)?;
                        attempt
                            .terminal_after_outer_failure()
                            .map_err(attempt_error)?;
                        Err(with_terminal(DmlError::commit(message), &attempt))
                    }
                }
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn complete_commit(
        &self,
        spec: &WriteTransactionSpec,
        attempt: &mut DmlPublicationAttempt,
        handle: &E::CommitHandle,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                self.finish_committed(spec, attempt, Some(receipt))
            }
            ExternalMutationOutcome::KnownUncommitted { failure } => {
                attempt
                    .terminal_known_uncommitted()
                    .map_err(attempt_error)?;
                Err(with_terminal(DmlError::commit(failure.message()), attempt))
            }
            ExternalMutationOutcome::CommitUnknown {
                failure: _,
                evidence,
            } => {
                let token = attempt.begin_adjudication().map_err(attempt_error)?;
                match self
                    .executor
                    .adjudicate_publication(spec, &handle, evidence)
                {
                    Ok(ExternalMutationOutcome::KnownCommitted { receipt, .. }) => {
                        let finalize = self.executor.finalize(spec);
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
                            operation_id: None,
                            committed_receipt: Some(receipt),
                        })
                    }
                    Ok(ExternalMutationOutcome::KnownUncommitted { failure }) => {
                        attempt
                            .finish_adjudication(
                                token,
                                DmlPublicationAdjudicationOutcome::CommitUnknown,
                                DmlPublicationFinalization::NotApplicable,
                            )
                            .map_err(attempt_error)?;
                        Err(with_terminal(DmlError::commit(failure.message()), attempt))
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

    #[allow(clippy::result_large_err)]
    fn finish_committed(
        &self,
        spec: &WriteTransactionSpec,
        attempt: &mut DmlPublicationAttempt,
        receipt: Option<ConnectorWriteReceipt>,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let finalization = if self.executor.finalize(spec).is_ok() {
            DmlPublicationFinalization::Succeeded
        } else {
            DmlPublicationFinalization::Failed
        };
        let terminal = attempt
            .terminal_known_committed(finalization)
            .map_err(attempt_error)?
            .clone();
        if finalization == DmlPublicationFinalization::Failed {
            return Err(DmlError::known_committed_finalization_failed(
                terminal,
                "post-commit finalization failed",
            ));
        }
        Ok(WriteTransactionOutcome {
            operation_id: None,
            committed_receipt: receipt,
        })
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

pub(crate) trait WriteAdmission: Send + Sync {
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn admit(&self) -> Result<(), DmlError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AlwaysAdmit;

impl WriteAdmission for AlwaysAdmit {
    fn admit(&self) -> Result<(), DmlError> {
        Ok(())
    }
}

pub(crate) struct WriteTransactionRunner<'a, E: WriteExecutor> {
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

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub fn run(&self, spec: WriteTransactionSpec) -> Result<WriteTransactionOutcome, DmlError> {
        self.admission.admit()?;
        let operation_id = self.journal.create_preparing(preparing_request(&spec))?;

        let report = match self.executor.run_coordinated_write(&spec) {
            Ok(report) => report,
            Err(error) => return self.known_uncommitted_error(operation_id, error),
        };
        match report {
            CoordinatedWriteReport::Aborted { reason } => {
                self.known_uncommitted(operation_id, reason)
            }
            CoordinatedWriteReport::NoOp => {
                self.journal
                    .transition(operation_id, OperationState::Committing)?;
                self.record_outcome(
                    operation_id,
                    &ExternalMutationOutcome::KnownCommitted {
                        effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                        receipt: empty_receipt()?,
                        finalization: ExternalMutationFinalization::Complete,
                    },
                )?;
                self.journal
                    .transition(operation_id, OperationState::Finalized)?;
                Ok(WriteTransactionOutcome {
                    operation_id: Some(operation_id),
                    committed_receipt: None,
                })
            }
            CoordinatedWriteReport::CommitRequired(handle) => {
                self.journal
                    .transition(operation_id, OperationState::Committing)?;
                match self.executor.commit(&spec, &handle) {
                    Ok(outcome) => self.complete_terminal(operation_id, &spec, outcome),
                    Err(message) => self.known_uncommitted(operation_id, message),
                }
            }
            CoordinatedWriteReport::AbortRequired { reason, handle } => {
                self.journal
                    .transition(operation_id, OperationState::Aborting)?;
                match self.executor.abort(&spec, &handle) {
                    Ok(outcome) => self.complete_abort(operation_id, &spec, &reason, outcome),
                    Err(message) => self.known_uncommitted(operation_id, message),
                }
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn complete_abort(
        &self,
        operation_id: DmlOperationId,
        spec: &WriteTransactionSpec,
        stage_reason: &str,
        outcome: ConnectorWriteAbortOutcome,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let fact = reconcile::operation_fact_from_abort_outcome(&outcome)
            .map_err(DmlError::journal_corruption)?;
        self.journal.record_fact(operation_id, fact)?;
        match outcome {
            ConnectorWriteAbortOutcome::KnownCommitted { receipt, .. } => {
                self.finish_committed(operation_id, spec, receipt)
            }
            ConnectorWriteAbortOutcome::KnownUncommitted { .. } => {
                Err(DmlError::executor(stage_reason))
            }
            ConnectorWriteAbortOutcome::CommitUnknown { failure, .. } => {
                Err(DmlError::commit(failure.message()))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn complete_terminal(
        &self,
        operation_id: DmlOperationId,
        spec: &WriteTransactionSpec,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        self.record_outcome(operation_id, &outcome)?;
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                ..
            } => {
                self.journal
                    .transition(operation_id, OperationState::Finalized)?;
                Ok(WriteTransactionOutcome {
                    operation_id: Some(operation_id),
                    committed_receipt: None,
                })
            }
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                self.finish_committed(operation_id, spec, receipt)
            }
            ExternalMutationOutcome::KnownUncommitted { failure } => {
                Err(DmlError::commit(failure.message()))
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                Err(DmlError::commit(failure.message()))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn finish_committed(
        &self,
        operation_id: DmlOperationId,
        spec: &WriteTransactionSpec,
        receipt: ConnectorWriteReceipt,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        self.journal
            .transition(operation_id, OperationState::Finalizing)?;
        if let Err(message) = self.executor.finalize(spec) {
            let failure =
                ConnectorMutationFailure::new(ConnectorMutationFailureKind::Internal, message);
            let fact = reconcile::operation_fact_from_finalize_failure(&receipt, &failure)
                .map_err(DmlError::journal_corruption)?;
            self.journal.record_fact(operation_id, fact)?;
            return Err(DmlError::committed_but_unfinalized(
                operation_id,
                Some(receipt),
                "post-commit finalization failed",
            ));
        }
        self.journal
            .transition(operation_id, OperationState::Finalized)?;
        Ok(WriteTransactionOutcome {
            operation_id: Some(operation_id),
            committed_receipt: Some(receipt),
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn record_outcome(
        &self,
        operation_id: DmlOperationId,
        outcome: &ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Result<(), DmlError> {
        self.journal.record_fact(
            operation_id,
            reconcile::operation_fact_from_outcome(outcome)
                .map_err(DmlError::journal_corruption)?,
        )
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn known_uncommitted(
        &self,
        operation_id: DmlOperationId,
        message: String,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Internal, message.clone());
        self.record_outcome(
            operation_id,
            &ExternalMutationOutcome::KnownUncommitted { failure },
        )?;
        Err(DmlError::executor(message))
    }

    #[allow(
        clippy::result_large_err,
        reason = "The runner must preserve a typed client error after recording the failed write outcome."
    )]
    fn known_uncommitted_error(
        &self,
        operation_id: DmlOperationId,
        error: DmlError,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Internal,
            error.to_string(),
        );
        self.record_outcome(
            operation_id,
            &ExternalMutationOutcome::KnownUncommitted { failure },
        )?;
        Err(error.with_operation_id(operation_id))
    }
}

/// Build the durable intent that must be admitted and claimed before provider
/// code is allowed to observe the write transaction.
pub(crate) fn preparing_request(spec: &WriteTransactionSpec) -> CreatePreparingRequest {
    CreatePreparingRequest {
        publication_id: spec.publication_id,
        operation_kind: spec.operation_kind,
        operation_subkind: spec.operation_subkind.clone(),
        target: spec.target.clone(),
        attempt_id: spec.attempt_id.clone(),
        base_snapshot_id: spec.base_snapshot_id,
        base_snapshot_map: spec.base_snapshot_map.clone(),
        staged_artifacts: Vec::new(),
        created_at_ms: now_unix_millis(),
    }
}

/// Production write runner. The active operation owns the current expected
/// revision and records only the catalog publication outcome.
pub(crate) struct ActiveWriteTransactionRunner<'a, E: WriteExecutor> {
    operation: ActiveDmlOperation,
    executor: &'a E,
}

impl<'a, E: WriteExecutor> ActiveWriteTransactionRunner<'a, E> {
    pub(crate) fn new(operation: ActiveDmlOperation, executor: &'a E) -> Self {
        Self {
            operation,
            executor,
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub(crate) fn run(
        mut self,
        spec: WriteTransactionSpec,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let result = self.run_inner(&spec);
        // Releasing the lease is cleanup, not part of the SQL commit outcome.
        // In particular, a terminal durable fact must remain authoritative if
        // release itself cannot be confirmed.
        let _ = self.operation.release();
        result
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn run_inner(
        &mut self,
        spec: &WriteTransactionSpec,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        self.transition_recoverable(OperationState::Writing)?;
        self.operation.check_before_dispatch()?;
        let report = match self.executor.run_coordinated_write(spec) {
            Ok(report) => report,
            Err(error) => return self.known_uncommitted_error(error),
        };
        match report {
            CoordinatedWriteReport::Aborted { reason } => self.known_uncommitted(reason),
            CoordinatedWriteReport::NoOp => {
                self.transition_recoverable(OperationState::Committing)?;
                self.record_outcome(&ExternalMutationOutcome::KnownCommitted {
                    effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                    receipt: empty_receipt()?,
                    finalization: ExternalMutationFinalization::Complete,
                })?;
                self.operation.transition(OperationState::Finalized, None)?;
                Ok(WriteTransactionOutcome {
                    operation_id: Some(self.operation.operation_id()),
                    committed_receipt: None,
                })
            }
            CoordinatedWriteReport::CommitRequired(handle) => {
                self.transition_recoverable(OperationState::Committing)?;
                self.operation.check_before_dispatch()?;
                match self.executor.commit(spec, &handle) {
                    Ok(outcome) => self.complete_terminal(spec, outcome),
                    // The provider call has crossed the commit dispatch
                    // boundary. An adapter/envelope error cannot prove that
                    // the external mutation did not commit, so keep the
                    // durable operation in its recoverable Committing state.
                    Err(message) => Err(DmlError::ambiguous_outcome_not_durable(
                        self.operation.operation_id(),
                        message,
                    )),
                }
            }
            CoordinatedWriteReport::AbortRequired { reason, handle } => {
                self.transition_recoverable(OperationState::Aborting)?;
                self.operation.check_before_dispatch()?;
                match self.executor.abort(spec, &handle) {
                    Ok(outcome) => self.complete_abort(spec, &reason, outcome),
                    // Abort dispatch can race an external commit and its
                    // result adapter can fail after observing provider state.
                    // Preserve Aborting + recovery eligibility until a typed
                    // terminal outcome can be recorded.
                    Err(message) => Err(DmlError::ambiguous_outcome_not_durable(
                        self.operation.operation_id(),
                        message,
                    )),
                }
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn complete_abort(
        &mut self,
        spec: &WriteTransactionSpec,
        stage_reason: &str,
        outcome: ConnectorWriteAbortOutcome,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let fact = reconcile::operation_fact_from_abort_outcome(&outcome)
            .map_err(DmlError::journal_corruption)?;
        if let Err(error) = self.record_fact(fact) {
            return match &outcome {
                ConnectorWriteAbortOutcome::KnownCommitted { receipt, .. } => {
                    Err(DmlError::committed_outcome_not_durable(
                        self.operation.operation_id(),
                        receipt.clone(),
                        error,
                    ))
                }
                ConnectorWriteAbortOutcome::CommitUnknown { .. } => Err(
                    DmlError::ambiguous_outcome_not_durable(self.operation.operation_id(), error),
                ),
                ConnectorWriteAbortOutcome::KnownUncommitted { .. } => Err(error),
            };
        }
        match outcome {
            ConnectorWriteAbortOutcome::KnownCommitted { receipt, .. } => {
                self.finish_committed(spec, receipt)
            }
            ConnectorWriteAbortOutcome::KnownUncommitted { .. } => {
                Err(DmlError::executor(stage_reason))
            }
            ConnectorWriteAbortOutcome::CommitUnknown { failure, .. } => {
                Err(DmlError::commit(failure.message()))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn complete_terminal(
        &mut self,
        spec: &WriteTransactionSpec,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        if let Err(error) = self.record_outcome(&outcome) {
            return match &outcome {
                ExternalMutationOutcome::KnownCommitted {
                    effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
                    receipt,
                    ..
                } => Err(DmlError::committed_outcome_not_durable(
                    self.operation.operation_id(),
                    receipt.clone(),
                    error,
                )),
                ExternalMutationOutcome::CommitUnknown { .. } => Err(
                    DmlError::ambiguous_outcome_not_durable(self.operation.operation_id(), error),
                ),
                ExternalMutationOutcome::KnownCommitted {
                    effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                    ..
                }
                | ExternalMutationOutcome::KnownUncommitted { .. } => Err(error),
            };
        }
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                ..
            } => {
                self.operation.transition(OperationState::Finalized, None)?;
                Ok(WriteTransactionOutcome {
                    operation_id: Some(self.operation.operation_id()),
                    committed_receipt: None,
                })
            }
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                self.finish_committed(spec, receipt)
            }
            ExternalMutationOutcome::KnownUncommitted { failure } => {
                Err(DmlError::commit(failure.message()))
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                Err(DmlError::commit(failure.message()))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn finish_committed(
        &mut self,
        spec: &WriteTransactionSpec,
        receipt: ConnectorWriteReceipt,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        if let Err(error) = self.transition_recoverable(OperationState::Finalizing) {
            return Err(DmlError::committed_but_unfinalized(
                self.operation.operation_id(),
                Some(receipt),
                error,
            ));
        }
        if let Err(error) = self.operation.check_before_dispatch() {
            return Err(DmlError::committed_but_unfinalized(
                self.operation.operation_id(),
                Some(receipt),
                error,
            ));
        }
        if let Err(message) = self.executor.finalize(spec) {
            let failure =
                ConnectorMutationFailure::new(ConnectorMutationFailureKind::Internal, message);
            let fact = reconcile::operation_fact_from_finalize_failure(&receipt, &failure)
                .map_err(DmlError::journal_corruption)?;
            if let Err(error) = self.record_fact(fact) {
                return Err(DmlError::committed_but_unfinalized(
                    self.operation.operation_id(),
                    Some(receipt),
                    error,
                ));
            }
            return Err(DmlError::committed_but_unfinalized(
                self.operation.operation_id(),
                Some(receipt),
                "post-commit finalization failed",
            ));
        }
        if let Err(error) = self.operation.transition(OperationState::Finalized, None) {
            return Err(DmlError::committed_but_unfinalized(
                self.operation.operation_id(),
                Some(receipt),
                error,
            ));
        }
        Ok(WriteTransactionOutcome {
            operation_id: Some(self.operation.operation_id()),
            committed_receipt: Some(receipt),
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn record_outcome(
        &mut self,
        outcome: &ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Result<(), DmlError> {
        let fact = reconcile::operation_fact_from_outcome(outcome)
            .map_err(DmlError::journal_corruption)?;
        self.record_fact(fact)
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn known_uncommitted(&mut self, message: String) -> Result<WriteTransactionOutcome, DmlError> {
        let failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Internal, message.clone());
        self.record_outcome(&ExternalMutationOutcome::KnownUncommitted { failure })?;
        Err(DmlError::executor(message))
    }

    #[allow(
        clippy::result_large_err,
        reason = "The active runner must preserve a typed client error after recording the failed write outcome."
    )]
    fn known_uncommitted_error(
        &mut self,
        error: DmlError,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Internal,
            error.to_string(),
        );
        self.record_outcome(&ExternalMutationOutcome::KnownUncommitted { failure })?;
        Err(error.with_operation_id(self.operation.operation_id()))
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn record_fact(&mut self, fact: crate::dml::model::OperationFact) -> Result<(), DmlError> {
        let recovery_due_at_ms = if fact.state.is_finished() {
            None
        } else {
            self.recovery_due()
        };
        self.operation.record_fact(fact, recovery_due_at_ms)
    }

    fn recovery_due(&self) -> Option<i64> {
        self.operation.stored.recovery_due_at_ms
    }

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn transition_recoverable(&mut self, to: OperationState) -> Result<(), DmlError> {
        let recovery_due_at_ms = self.recovery_due();
        self.operation.transition(to, recovery_due_at_ms)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn empty_receipt() -> Result<ConnectorWriteReceipt, DmlError> {
    ConnectorWriteReceipt::try_new(bytes::Bytes::from_static(b"connector-write-noop"))
        .map_err(DmlError::journal_corruption)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::sync::Arc;

    use super::*;
    use crate::dml::journal::testing::InMemoryOperationJournal;

    struct KnownUncommittedAbortExecutor;

    struct CommitEnvelopeFailureExecutor;

    impl WriteExecutor for CommitEnvelopeFailureExecutor {
        type CommitHandle = ();
        type AbortHandle = Infallible;

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, DmlError>
        {
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
            Err("encode terminal commit receipt".to_string())
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            panic!("unresolved commit must not finalize")
        }
    }

    struct AbortEnvelopeFailureExecutor;

    impl WriteExecutor for AbortEnvelopeFailureExecutor {
        type CommitHandle = Infallible;
        type AbortHandle = ();

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, DmlError>
        {
            Ok(CoordinatedWriteReport::AbortRequired {
                reason: "stage requires abort".to_string(),
                handle: (),
            })
        }

        fn abort(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &Self::AbortHandle,
        ) -> Result<ConnectorWriteAbortOutcome, String> {
            Err("encode terminal abort evidence".to_string())
        }

        fn commit(
            &self,
            _spec: &WriteTransactionSpec,
            handle: &Self::CommitHandle,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
            match *handle {}
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            panic!("unresolved abort must not finalize")
        }
    }

    impl WriteExecutor for KnownUncommittedAbortExecutor {
        type CommitHandle = Infallible;
        type AbortHandle = ();

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, DmlError>
        {
            Ok(CoordinatedWriteReport::AbortRequired {
                reason: "MOR UPDATE matched target row: duplicate _row_id=7".to_string(),
                handle: (),
            })
        }

        fn abort(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &Self::AbortHandle,
        ) -> Result<ConnectorWriteAbortOutcome, String> {
            Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            })
        }

        fn commit(
            &self,
            _spec: &WriteTransactionSpec,
            handle: &Self::CommitHandle,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
            match *handle {}
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            panic!("known-uncommitted abort must not finalize")
        }
    }

    fn spec() -> WriteTransactionSpec {
        WriteTransactionSpec {
            publication_id: DmlOperationId::new_v7(),
            target: crate::dml::model::OperationTarget {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "target".to_string(),
                ref_name: None,
            },
            operation_kind: crate::dml::model::OperationKind::RowDelta,
            operation_subkind: Some("UPDATE".to_string()),
            attempt_id: "update-attempt".to_string(),
            base_snapshot_id: Some(7),
            base_snapshot_map: BTreeMap::new(),
        }
    }

    #[test]
    fn known_uncommitted_abort_surfaces_the_original_stage_reason() {
        let journal = InMemoryOperationJournal::default();
        let runner =
            WriteTransactionRunner::new(&journal, &KnownUncommittedAbortExecutor, &AlwaysAdmit);

        let error = runner
            .run(spec())
            .expect_err("duplicate source match must fail after terminal abort");
        assert_eq!(
            error.to_string(),
            "Executor: MOR UPDATE matched target row: duplicate _row_id=7"
        );
        assert_eq!(
            journal.only_operation().state,
            OperationState::FailedKnownUncommitted
        );
    }

    fn begin_active(
        journal: &Arc<InMemoryOperationJournal>,
    ) -> (DmlOperationId, ActiveDmlOperation) {
        let operation_id = journal
            .create_preparing(preparing_request(&spec()))
            .expect("intent");
        let stored = journal.load(operation_id).unwrap().unwrap();
        let operation_journal: Arc<dyn OperationJournal> = journal.clone();
        (
            operation_id,
            ActiveDmlOperation::legacy(operation_journal, stored),
        )
    }

    #[test]
    fn active_runner_persists_terminal_abort_through_active_operation() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let (_, operation) = begin_active(&journal);

        let error = ActiveWriteTransactionRunner::new(operation, &KnownUncommittedAbortExecutor)
            .run(spec())
            .expect_err("known-uncommitted abort must fail");

        assert_eq!(
            error.to_string(),
            "Executor: MOR UPDATE matched target row: duplicate _row_id=7"
        );
        assert_eq!(
            journal.only_operation().state,
            OperationState::FailedKnownUncommitted
        );
    }

    #[test]
    fn active_runner_keeps_commit_dispatch_adapter_failure_recoverable() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let (operation_id, operation) = begin_active(&journal);

        let error = ActiveWriteTransactionRunner::new(operation, &CommitEnvelopeFailureExecutor)
            .run(spec())
            .expect_err("terminal receipt encoding failure must remain unresolved");

        assert_eq!(
            error.kind(),
            crate::dml::error::DmlErrorKind::CoordinationUnresolved
        );
        assert_eq!(error.operation_id(), Some(operation_id));
        assert_eq!(
            error.next_action(),
            Some(crate::dml::model::StatementNextAction::ManualInspect)
        );
        let stored = journal.only_operation();
        assert_eq!(stored.state, OperationState::Committing);
        assert!(stored.recovery_due_at_ms.is_some());
    }

    #[test]
    fn active_runner_keeps_abort_dispatch_adapter_failure_recoverable() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let (operation_id, operation) = begin_active(&journal);

        let error = ActiveWriteTransactionRunner::new(operation, &AbortEnvelopeFailureExecutor)
            .run(spec())
            .expect_err("terminal abort evidence encoding failure must remain unresolved");

        assert_eq!(
            error.kind(),
            crate::dml::error::DmlErrorKind::CoordinationUnresolved
        );
        assert_eq!(error.operation_id(), Some(operation_id));
        assert_eq!(
            error.next_action(),
            Some(crate::dml::model::StatementNextAction::ManualInspect)
        );
        let stored = journal.only_operation();
        assert_eq!(stored.state, OperationState::Aborting);
        assert!(stored.recovery_due_at_ms.is_some());
    }
}
