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
    ConnectorWriteReceipt, ExternalMutationFinalization, ExternalMutationOutcome,
};

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

    fn run_coordinated_write(
        &self,
        spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, String>;

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

pub trait WriteAdmission: Send + Sync {
    fn admit(&self) -> Result<(), DmlError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysAdmit;

impl WriteAdmission for AlwaysAdmit {
    fn admit(&self) -> Result<(), DmlError> {
        Ok(())
    }
}

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

    pub fn run(&self, spec: WriteTransactionSpec) -> Result<WriteTransactionOutcome, DmlError> {
        self.admission.admit()?;
        let operation_id = self.journal.create_preparing(CreatePreparingRequest {
            operation_kind: spec.operation_kind,
            operation_subkind: spec.operation_subkind.clone(),
            target: spec.target.clone(),
            attempt_id: spec.attempt_id.clone(),
            base_snapshot_id: spec.base_snapshot_id,
            base_snapshot_map: spec.base_snapshot_map.clone(),
            staged_artifacts: Vec::new(),
            created_at_ms: now_unix_millis(),
        })?;

        let report = match self.executor.run_coordinated_write(&spec) {
            Ok(report) => report,
            Err(message) => return self.known_uncommitted(operation_id, message),
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
}

fn empty_receipt() -> Result<ConnectorWriteReceipt, DmlError> {
    ConnectorWriteReceipt::try_new(bytes::Bytes::from_static(b"connector-write-noop"))
        .map_err(DmlError::journal_corruption)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::convert::Infallible;

    use super::*;
    use crate::dml::journal::testing::InMemoryOperationJournal;

    struct KnownUncommittedAbortExecutor;

    impl WriteExecutor for KnownUncommittedAbortExecutor {
        type CommitHandle = Infallible;
        type AbortHandle = ();

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, String> {
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
}
