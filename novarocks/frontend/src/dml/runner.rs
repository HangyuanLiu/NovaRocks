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

#[cfg(test)]
use novarocks_spi::connector::{
    ConnectorError, ConnectorEstablishedWriteFence, ConnectorExternalFenceFailure,
};
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorWriteAbortOutcome,
    ConnectorWriteReceipt, ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::dml::coordination::ActiveDmlOperation;
#[cfg(test)]
use crate::dml::coordination::DmlExternalFenceProposal;
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

    fn finalize(&self, spec: &WriteTransactionSpec) -> Result<(), String>;
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
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorExternalFenceReceipt,
        ConnectorExternalFenceRequest, ConnectorExternalOperationFence, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorRequestContext, ConnectorTableIdentity,
        ConnectorWriteAbortRequest, ConnectorWriteCommitRequest, ConnectorWriteControl,
        ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWritePlan,
        ConnectorWritePlanningRequest, ConnectorWriteReconcileRequest, ConnectorWriteTargetRef,
    };
    use novarocks_spi::state_store::WriteTransaction;
    use uuid::Uuid;

    use super::*;
    use crate::dml::journal::testing::InMemoryOperationJournal;
    use crate::dml::journal::{DmlMutationAuthority, DmlMutationAuthorityValidator};
    use crate::dml::model::{
        AddFilesArtifact, CreateStatementOperationRequest, DmlExternalFenceGeneration,
        DmlExternalFenceMutationRequest, DmlExternalFenceReceiptRecord, OperationFact,
        StoredOperation, validate_external_fence_receipt,
    };

    // ---------------------------------------------------------------------
    // CP-3B external fence fixtures.
    //
    // The runner only ever sees SPI values, so the fixtures build a real
    // `ConnectorWriteLease` over a recording control instead of faking the
    // established fence type.
    // ---------------------------------------------------------------------

    const FENCE_MARKER: &[u8] = b"runner-test-fence-marker";

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn connector_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("connector request context")
    }

    fn binding_key() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("runner-test").expect("instance id"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([5; 16]),
        }
    }

    fn connector_table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: binding_key().instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("target"),
        }
    }

    fn connector_write_operation_id() -> ConnectorWriteOperationId {
        ConnectorWriteOperationId::from_bytes([9; 16])
    }

    struct FenceControl {
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorWriteControl for FenceControl {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn establish_external_fence(
            &self,
            request: ConnectorExternalFenceRequest,
        ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
            request.validate(&self.key)?;
            ConnectorExternalFenceReceipt::try_new(&request.fence, Bytes::from_static(FENCE_MARKER))
        }

        fn plan_write(
            &self,
            _request: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "runner fence fixture does not plan writes",
            ))
        }

        fn commit(
            &self,
            _request: ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "runner fence fixture does not commit",
            ))
        }

        fn abort(
            &self,
            _request: ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "runner fence fixture does not abort",
            ))
        }

        fn reconcile(
            &self,
            _request: ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "runner fence fixture does not reconcile",
            ))
        }
    }

    fn fence_lease() -> ConnectorWriteLease {
        let key = binding_key();
        ConnectorWriteLease::new(key.clone(), Arc::new(FenceControl { key }), || {})
            .expect("exact write lease")
    }

    /// Complete a proposal exactly the way a production route must: the
    /// connector half comes from the write authority, never from the frontend.
    fn establish_test_fence(
        proposal: &DmlExternalFenceProposal,
    ) -> Result<ConnectorEstablishedWriteFence, ConnectorError> {
        let fence = proposal.seal(
            connector_write_operation_id(),
            connector_table(),
            ConnectorWriteTargetRef::main(),
        )?;
        fence_lease().establish_external_fence(fence, connector_context())
    }

    fn test_generation() -> DmlExternalFenceGeneration {
        DmlExternalFenceGeneration {
            control_plane_incarnation: 3,
            resource_epoch: 7,
            fence_generation: 11,
        }
    }

    fn test_proposal(operation_id: DmlOperationId) -> DmlExternalFenceProposal {
        DmlExternalFenceProposal::testing(
            operation_id,
            "runner-test-cluster",
            Uuid::now_v7(),
            test_generation(),
        )
        .expect("external fence proposal")
    }

    struct AlwaysCurrentAuthority;

    #[async_trait::async_trait]
    impl DmlMutationAuthorityValidator for AlwaysCurrentAuthority {
        async fn validate_in(
            &self,
            _transaction: &mut dyn WriteTransaction,
        ) -> Result<(), DmlError> {
            Ok(())
        }
    }

    /// In-memory journal that also accepts the CP-3B fence receipt record and
    /// records the order in which it was written relative to dispatch.
    #[derive(Default)]
    struct FenceRecordingJournal {
        inner: InMemoryOperationJournal,
        fences: Mutex<Vec<DmlExternalFenceReceiptRecord>>,
        events: Arc<Mutex<Vec<&'static str>>>,
        refuse_preflight: bool,
    }

    impl FenceRecordingJournal {
        fn with_events(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                inner: InMemoryOperationJournal::default(),
                fences: Mutex::new(Vec::new()),
                events,
                refuse_preflight: false,
            }
        }

        fn refusing_preflight(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                refuse_preflight: true,
                ..Self::with_events(events)
            }
        }

        fn only_operation(&self) -> StoredOperation {
            self.inner.only_operation()
        }

        fn recorded_fences(&self) -> Vec<DmlExternalFenceReceiptRecord> {
            self.fences.lock().expect("recorded fences").clone()
        }
    }

    impl OperationJournal for FenceRecordingJournal {
        fn create_preparing(
            &self,
            request: CreatePreparingRequest,
        ) -> Result<DmlOperationId, DmlError> {
            self.inner.create_preparing(request)
        }

        fn transition(
            &self,
            operation_id: DmlOperationId,
            to: OperationState,
        ) -> Result<(), DmlError> {
            self.inner.transition(operation_id, to)
        }

        fn record_fact(
            &self,
            operation_id: DmlOperationId,
            fact: OperationFact,
        ) -> Result<(), DmlError> {
            self.inner.record_fact(operation_id, fact)
        }

        fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError> {
            self.inner.load(operation_id)
        }

        fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
            self.inner.list_operations()
        }

        fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
            self.inner.list_unfinished()
        }

        fn create_statement_operation(
            &self,
            request: CreateStatementOperationRequest,
        ) -> Result<StoredOperation, DmlError> {
            self.inner.create_statement_operation(request)
        }

        fn load_add_files_artifact(
            &self,
            operation_id: DmlOperationId,
            artifact: &crate::dml::model::AddFilesArtifactDescriptor,
        ) -> Result<AddFilesArtifact, DmlError> {
            self.inner.load_add_files_artifact(operation_id, artifact)
        }

        fn preflight_external_fence(
            &self,
            request: &DmlExternalFenceMutationRequest,
        ) -> Result<(), DmlError> {
            self.events
                .lock()
                .expect("journal events")
                .push("preflight-fence");
            if self.refuse_preflight {
                return Err(DmlError::journal_corruption(
                    "test journal cannot hold an external fence receipt",
                ));
            }
            validate_external_fence_receipt(&request.fence).map_err(DmlError::journal_corruption)
        }

        fn load_historical_write_recovery(
            &self,
            _operation_id: DmlOperationId,
        ) -> Result<Option<crate::dml::model::DmlHistoricalWriteRecoveryRecord>, DmlError> {
            Ok(None)
        }

        fn record_external_fence_authorized(
            &self,
            request: DmlExternalFenceMutationRequest,
            _recovery_due_at_ms: Option<i64>,
            _authority: DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            validate_external_fence_receipt(&request.fence)
                .map_err(DmlError::journal_corruption)?;
            self.events
                .lock()
                .expect("journal events")
                .push("record-fence");
            self.fences
                .lock()
                .expect("recorded fences")
                .push(request.fence);
            self.inner.load(request.operation_id)?.ok_or_else(|| {
                DmlError::journal_unresolved("fenced test operation cannot be read back")
            })
        }
    }

    fn fenced_operation(
        journal: Arc<FenceRecordingJournal>,
        stored: StoredOperation,
        proposal: DmlExternalFenceProposal,
    ) -> ActiveDmlOperation {
        ActiveDmlOperation::testing_fenced(
            journal,
            stored,
            proposal,
            Arc::new(AlwaysCurrentAuthority),
        )
    }

    /// Executor bound to one exact write lease, exactly like a production
    /// route: the fence is established on the lease, and the connector write
    /// operation only begins afterwards.
    struct LeaseBoundFenceExecutor {
        lease: ConnectorWriteLease,
        events: Arc<Mutex<Vec<&'static str>>>,
        establish_failure: Option<ConnectorExternalFenceFailure>,
        foreign_attempt_id: bool,
    }

    impl LeaseBoundFenceExecutor {
        fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                lease: fence_lease(),
                events,
                establish_failure: None,
                foreign_attempt_id: false,
            }
        }

        fn failing(
            events: Arc<Mutex<Vec<&'static str>>>,
            failure: ConnectorExternalFenceFailure,
        ) -> Self {
            Self {
                establish_failure: Some(failure),
                ..Self::new(events)
            }
        }

        fn foreign(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                foreign_attempt_id: true,
                ..Self::new(events)
            }
        }

        fn push(&self, event: &'static str) {
            self.events.lock().expect("executor events").push(event);
        }
    }

    impl WriteExecutor for LeaseBoundFenceExecutor {
        type CommitHandle = Infallible;
        type AbortHandle = Infallible;

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, DmlError>
        {
            self.push("begin-write-operation");
            Ok(CoordinatedWriteReport::NoOp)
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
            handle: &Self::CommitHandle,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
            match *handle {}
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            self.push("finalize");
            Ok(())
        }
    }

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

    fn begin_fenced(journal: &Arc<FenceRecordingJournal>) -> (DmlOperationId, ActiveDmlOperation) {
        let operation_id = journal
            .create_preparing(preparing_request(&spec()))
            .expect("intent");
        let stored = journal.load(operation_id).unwrap().unwrap();
        let proposal = test_proposal(operation_id);
        (
            operation_id,
            fenced_operation(Arc::clone(journal), stored, proposal),
        )
    }

    #[test]
    fn active_runner_persists_terminal_abort_through_active_operation() {
        let journal = Arc::new(FenceRecordingJournal::default());
        let (_, operation) = begin_fenced(&journal);

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
        let journal = Arc::new(FenceRecordingJournal::default());
        let (operation_id, operation) = begin_fenced(&journal);

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
        let journal = Arc::new(FenceRecordingJournal::default());
        let (operation_id, operation) = begin_fenced(&journal);

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

    #[test]
    fn ordinary_active_runner_dispatches_without_an_external_fence() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let journal = Arc::new(FenceRecordingJournal::with_events(Arc::clone(&events)));
        let (_, operation) = begin_fenced(&journal);
        let executor = LeaseBoundFenceExecutor::new(Arc::clone(&events));

        ActiveWriteTransactionRunner::new(operation, &executor)
            .run(spec())
            .expect("ordinary no-op write completes");

        assert_eq!(*events.lock().expect("events"), ["begin-write-operation"]);
        assert!(
            executor
                .lease
                .established_external_fence()
                .expect("fence slot")
                .is_none(),
            "ordinary writes must not create a connector external fence"
        );
        assert!(journal.recorded_fences().is_empty());
    }

    #[test]
    fn replaying_the_identical_fence_is_idempotent() {
        let proposal = test_proposal(DmlOperationId::new_v7());
        let lease = fence_lease();
        let fence = proposal
            .seal(
                connector_write_operation_id(),
                connector_table(),
                ConnectorWriteTargetRef::main(),
            )
            .expect("sealed fence");
        let first = lease
            .establish_external_fence(fence.clone(), connector_context())
            .expect("first establishment");
        let replay = lease
            .establish_external_fence(fence, connector_context())
            .expect("identical replay is idempotent");
        assert_eq!(first.fence().digest(), replay.fence().digest());
        assert_eq!(first.receipt().digest(), replay.receipt().digest());
        assert_eq!(
            reconcile::external_fence_receipt_record(&first)
                .expect("record")
                .fence_digest,
            reconcile::external_fence_receipt_record(&replay)
                .expect("record")
                .fence_digest
        );
    }
}
