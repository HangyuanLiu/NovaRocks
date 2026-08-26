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
    ExternalMutationOutcome, LakePublicationFamily, LakePublicationId, LakePublicationTarget,
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

    #[allow(clippy::result_large_err)]
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

    #[allow(clippy::result_large_err)]
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
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                let token = attempt.begin_adjudication().map_err(attempt_error)?;
                match self.executor.adjudicate_publication(spec, handle, evidence) {
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
