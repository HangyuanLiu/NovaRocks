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

use std::sync::Arc;

use novarocks::engine::statistics::{EmptyStatisticsService, StatisticsService};
use tokio::runtime::Handle;

use crate::coordination::FrontendCoordinationRuntime;
use crate::dml::coordination::{ActiveDmlOperation, DmlCoordinator};
use crate::dml::error::DmlError;
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    CreatePreparingRequest, CreateStatementOperationRequest, DmlOperationId, DmlRecoveryCandidate,
    StoredOperation, WriteTransactionOutcome, WriteTransactionSpec,
};
use crate::dml::runner::{
    ActiveWriteTransactionRunner, AlwaysAdmit, WriteAdmission, WriteExecutor,
    WriteTransactionRunner, preparing_request,
};

/// The frontend DML application owner. Composes the narrow ports (journal +
/// admission) and drives write transactions. Constructed from narrow handles —
/// never from the host or a service locator.
pub struct DmlService {
    journal: Option<Arc<dyn OperationJournal>>,
    statistics: Arc<dyn StatisticsService>,
    admission: Arc<dyn WriteAdmission>,
    coordinator: Option<DmlCoordinator>,
    allow_unfenced_focused_test_support: bool,
}

impl DmlService {
    /// Build a journal-backed service with no-op statistics.
    ///
    /// Production composition uses [`Self::compose`]; this constructor keeps
    /// the statement-agnostic DML-1 runner usable in focused tests.
    #[doc(hidden)]
    pub fn new(journal: Arc<dyn OperationJournal>) -> Self {
        Self::compose(Some(journal), Arc::new(EmptyStatisticsService))
    }

    /// Compose the production DML owner from optional StateStore capability
    /// and the host-owned statistics service.
    #[doc(hidden)]
    pub fn compose(
        journal: Option<Arc<dyn OperationJournal>>,
        statistics: Arc<dyn StatisticsService>,
    ) -> Self {
        Self {
            journal,
            statistics,
            admission: Arc::new(AlwaysAdmit),
            coordinator: None,
            allow_unfenced_focused_test_support: true,
        }
    }

    /// Compose with real coordination.
    ///
    /// Hidden from the public API but reachable from integration tests, so a
    /// route test can exercise the fenced dispatch path instead of the
    /// unfenced focused-test seam.
    #[doc(hidden)]
    pub fn compose_with_coordination(
        journal: Option<Arc<dyn OperationJournal>>,
        statistics: Arc<dyn StatisticsService>,
        frontend: Arc<FrontendCoordinationRuntime>,
        runtime: Handle,
    ) -> Self {
        Self {
            journal,
            statistics,
            admission: Arc::new(AlwaysAdmit),
            coordinator: Some(DmlCoordinator::new(frontend, runtime)),
            allow_unfenced_focused_test_support: false,
        }
    }

    /// Build a service with a custom admission gate (CP-3 fencing).
    pub(crate) fn with_admission(
        journal: Option<Arc<dyn OperationJournal>>,
        statistics: Arc<dyn StatisticsService>,
        admission: Arc<dyn WriteAdmission>,
    ) -> Self {
        Self {
            journal,
            statistics,
            admission,
            coordinator: None,
            allow_unfenced_focused_test_support: true,
        }
    }

    pub(crate) fn begin_write_operation(
        &self,
        request: CreatePreparingRequest,
    ) -> Result<ActiveDmlOperation, DmlError> {
        let journal = self.require_journal_arc()?;
        let Some(coordinator) = self.coordinator.as_ref() else {
            if self.allow_unfenced_focused_test_support {
                let operation_id = journal.create_preparing(request)?;
                let operation = journal.load(operation_id)?.ok_or_else(|| {
                    DmlError::journal_unresolved(format!(
                        "created DML operation {operation_id} cannot be read back"
                    ))
                })?;
                return Ok(ActiveDmlOperation::legacy(journal, operation));
            }
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            ));
        };
        let operation_id = journal.create_preparing_admitted(request, coordinator.admission()?)?;
        let operation = journal.load(operation_id)?.ok_or_else(|| {
            DmlError::journal_unresolved(format!(
                "created DML operation {operation_id} cannot be read back"
            ))
        })?;
        coordinator.claim_foreground(journal, operation)
    }

    pub(crate) fn begin_statement_operation(
        &self,
        request: CreateStatementOperationRequest,
    ) -> Result<ActiveDmlOperation, DmlError> {
        let journal = self.require_journal_arc()?;
        let Some(coordinator) = self.coordinator.as_ref() else {
            if self.allow_unfenced_focused_test_support {
                let operation = journal.create_statement_operation(request)?;
                return Ok(ActiveDmlOperation::legacy(journal, operation));
            }
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            ));
        };
        let operation =
            journal.create_statement_operation_admitted(request, coordinator.admission()?)?;
        coordinator.claim_foreground(journal, operation)
    }

    pub(crate) async fn shutdown_coordination(&self) -> Result<(), DmlError> {
        if let Some(coordinator) = &self.coordinator {
            coordinator.shutdown().await?;
        }
        Ok(())
    }

    pub(crate) fn recovery_candidates(
        &self,
        shard: u8,
        due_at_or_before_ms: i64,
    ) -> Result<Vec<DmlRecoveryCandidate>, DmlError> {
        self.require_journal()?
            .recovery_candidates(shard, due_at_or_before_ms)
    }

    pub(crate) fn defer_recovery_candidate(
        &self,
        candidate: DmlRecoveryCandidate,
        next_due_at_ms: i64,
    ) -> Result<(), DmlError> {
        let journal = self.require_journal_arc()?;
        let Some(operation) = journal.load(candidate.operation_id)? else {
            return Ok(());
        };
        if operation.revision != candidate.operation_revision
            || operation.last_mutation_id != candidate.last_mutation_id
            || operation.recovery_due_at_ms != Some(candidate.recovery_due_at_ms)
        {
            return Ok(());
        }
        let mut active = self
            .require_coordinator()?
            .claim_recovery(journal, operation)?;
        let result = active.reschedule_recovery_due(Some(next_due_at_ms));
        let release = active.release();
        result.and(release)
    }

    /// Run one Iceberg write transaction with the given executor.
    pub fn run_write<E: WriteExecutor>(
        &self,
        spec: WriteTransactionSpec,
        executor: &E,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        if self.coordinator.is_some() {
            let operation = self.begin_write_operation(preparing_request(&spec))?;
            return ActiveWriteTransactionRunner::new(operation, executor).run(spec);
        }
        if !self.allow_unfenced_focused_test_support {
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            ));
        }
        let journal = self.require_journal()?;
        let runner = WriteTransactionRunner::new(journal, executor, self.admission.as_ref());
        runner.run(spec)
    }

    pub(crate) fn require_journal(&self) -> Result<&dyn OperationJournal, DmlError> {
        self.journal.as_deref().ok_or_else(|| {
            DmlError::journal_unavailable(
                "state store is required for Iceberg DML; configure [state_store]",
            )
        })
    }

    fn require_journal_arc(&self) -> Result<Arc<dyn OperationJournal>, DmlError> {
        self.journal.clone().ok_or_else(|| {
            DmlError::journal_unavailable(
                "state store is required for Iceberg DML; configure [state_store]",
            )
        })
    }

    fn require_coordinator(&self) -> Result<&DmlCoordinator, DmlError> {
        self.coordinator.as_ref().ok_or_else(|| {
            DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            )
        })
    }

    pub(crate) fn statistics(&self) -> &dyn StatisticsService {
        self.statistics.as_ref()
    }

    /// Load a stored operation by id.
    pub fn load_operation(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<StoredOperation>, DmlError> {
        self.require_journal()?.load(operation_id)
    }

    /// List all durable operations for lifecycle inspection and recovery audits.
    pub fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        self.require_journal()?.list_operations()
    }

    /// List operations that have not reached a terminal state (recovery input).
    pub fn list_unfinished_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        self.require_journal()?.list_unfinished()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::DmlService;
    use crate::dml::journal::testing::InMemoryOperationJournal;
    use crate::dml::model::{OperationKind, OperationState, OperationTarget, WriteTransactionSpec};
    use crate::dml::runner::{CoordinatedWriteReport, WriteExecutor};

    struct OkExecutor;

    impl WriteExecutor for OkExecutor {
        type CommitHandle = ();
        type AbortHandle = std::convert::Infallible;

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<()>, String> {
            Ok(CoordinatedWriteReport::CommitRequired(()))
        }

        fn abort(
            &self,
            _spec: &WriteTransactionSpec,
            handle: &Self::AbortHandle,
        ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
            match *handle {}
        }

        fn commit(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &(),
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<
                novarocks_spi::connector::ConnectorWriteReceipt,
            >,
            String,
        > {
            Ok(
                novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                    effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
                    receipt: novarocks_spi::connector::ConnectorWriteReceipt::try_new(
                        bytes::Bytes::from_static(b"test-receipt"),
                    )
                    .expect("receipt"),
                    finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                },
            )
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            Ok(())
        }
    }

    fn spec() -> WriteTransactionSpec {
        WriteTransactionSpec {
            target: OperationTarget {
                catalog: "c".to_string(),
                namespace: "n".to_string(),
                table: "t".to_string(),
                ref_name: None,
            },
            operation_kind: OperationKind::InsertAppend,
            operation_subkind: None,
            attempt_id: "a".to_string(),
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
        }
    }

    #[test]
    fn service_runs_write_and_exposes_operation() {
        let service = DmlService::new(Arc::new(InMemoryOperationJournal::default()));
        let outcome = service.run_write(spec(), &OkExecutor).unwrap();
        let id = outcome.operation_id.unwrap();
        assert_eq!(
            service.load_operation(id).unwrap().unwrap().state,
            OperationState::Finalized
        );
        let operations = service.list_operations().unwrap();
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].operation_id, id);
        assert_eq!(operations[0].state, OperationState::Finalized);
        assert!(service.list_unfinished_operations().unwrap().is_empty());
    }
}
