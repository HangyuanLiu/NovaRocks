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

use novarocks::meta::MetaStoreProvider;
use novarocks::meta::repository::iceberg_operation::{
    IcebergOperationFactUpdate, IcebergOperationRepository,
};

use crate::dml::error::DmlError;
use crate::dml::model::{CreatePreparingRequest, OperationFact, OperationState, StoredOperation};
use crate::dml::now_unix_millis;

/// Durable journal for Iceberg write operations. Each method is its own unit of
/// work (one `begin_write`/`commit` per call), so the port fully hides the
/// underlying meta store transaction.
pub trait OperationJournal: Send + Sync {
    /// Create a new operation in `Preparing`, returning its id.
    fn create_preparing(&self, request: CreatePreparingRequest) -> Result<i64, DmlError>;
    /// Advance the operation to a new state (validated by the repository).
    fn transition(&self, operation_id: i64, to: OperationState) -> Result<(), DmlError>;
    /// Record a durable lifecycle fact (state + evidence), replay-safe.
    fn record_fact(&self, operation_id: i64, fact: OperationFact) -> Result<(), DmlError>;
    /// Load a stored operation by id.
    fn load(&self, operation_id: i64) -> Result<Option<StoredOperation>, DmlError>;
    /// List operations that are not in a terminal state.
    fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError>;
}

/// Real journal backed by the core meta store repository. Wraps the already-`pub`
/// `IcebergOperationRepository` + `MetaStoreProvider` — no core changes.
pub struct MetaStoreOperationJournal {
    provider: Arc<dyn MetaStoreProvider>,
    repo: IcebergOperationRepository,
}

impl MetaStoreOperationJournal {
    pub fn new(provider: Arc<dyn MetaStoreProvider>) -> Self {
        Self {
            provider,
            repo: IcebergOperationRepository,
        }
    }
}

impl OperationJournal for MetaStoreOperationJournal {
    fn create_preparing(&self, request: CreatePreparingRequest) -> Result<i64, DmlError> {
        let mut txn = self
            .provider
            .begin_write("dml: create iceberg write operation")
            .map_err(DmlError::journal)?;
        let stored = self
            .repo
            .create_operation(txn.as_mut(), request)
            .map_err(DmlError::journal)?;
        txn.commit().map_err(DmlError::journal)?;
        Ok(stored.operation_id)
    }

    fn transition(&self, operation_id: i64, to: OperationState) -> Result<(), DmlError> {
        let mut txn = self
            .provider
            .begin_write("dml: advance iceberg write operation")
            .map_err(DmlError::journal)?;
        self.repo
            .transition_operation(txn.as_mut(), operation_id, to, now_unix_millis())
            .map_err(DmlError::journal)?;
        txn.commit().map_err(DmlError::journal)?;
        Ok(())
    }

    fn record_fact(&self, operation_id: i64, fact: OperationFact) -> Result<(), DmlError> {
        let update = IcebergOperationFactUpdate {
            operation_id,
            state: fact.state,
            commit_outcome: fact.commit_outcome,
            cleanup_outcome: fact.cleanup_outcome,
            recovery_evidence: fact.recovery_evidence,
            failure: fact.failure,
            now_ms: now_unix_millis(),
        };
        let mut txn = self
            .provider
            .begin_write("dml: record iceberg write operation fact")
            .map_err(DmlError::journal)?;
        self.repo
            .record_operation_fact(txn.as_mut(), update)
            .map_err(DmlError::journal)?;
        txn.commit().map_err(DmlError::journal)?;
        Ok(())
    }

    fn load(&self, operation_id: i64) -> Result<Option<StoredOperation>, DmlError> {
        let txn = self.provider.begin_read().map_err(DmlError::journal)?;
        self.repo
            .load_operation(txn.as_ref(), operation_id)
            .map_err(DmlError::journal)
    }

    fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
        let txn = self.provider.begin_read().map_err(DmlError::journal)?;
        self.repo
            .list_unfinished_operations(txn.as_ref())
            .map_err(DmlError::journal)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use novarocks::meta::repository::iceberg_operation::validate_operation_transition;

    use super::*;

    /// In-memory `OperationJournal` for runner unit tests. Uses the real
    /// `validate_operation_transition` so it rejects illegal transitions the
    /// same way the persistent repository does. It does NOT model core's
    /// same-state fact refinement / conflicting-replay rejection; a test that
    /// needs that behavior should drive the real `MetaStoreOperationJournal`.
    #[derive(Default)]
    pub(crate) struct InMemoryOperationJournal {
        inner: Mutex<Inner>,
    }

    #[derive(Default)]
    struct Inner {
        next_id: i64,
        ops: BTreeMap<i64, StoredOperation>,
    }

    impl OperationJournal for InMemoryOperationJournal {
        fn create_preparing(&self, request: CreatePreparingRequest) -> Result<i64, DmlError> {
            let mut guard = self.inner.lock().unwrap();
            guard.next_id += 1;
            let operation_id = guard.next_id;
            let stored = StoredOperation {
                operation_id,
                operation_kind: request.operation_kind,
                operation_subkind: request.operation_subkind,
                target: request.target,
                state: OperationState::Preparing,
                attempt_id: request.attempt_id,
                base_snapshot_id: request.base_snapshot_id,
                base_snapshot_map: request.base_snapshot_map,
                staged_artifacts: request.staged_artifacts,
                commit_request: None,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                created_at_ms: request.created_at_ms,
                updated_at_ms: request.created_at_ms,
                finished_at_ms: None,
            };
            guard.ops.insert(operation_id, stored);
            Ok(operation_id)
        }

        fn transition(&self, operation_id: i64, to: OperationState) -> Result<(), DmlError> {
            let mut guard = self.inner.lock().unwrap();
            let op = guard
                .ops
                .get_mut(&operation_id)
                .ok_or_else(|| DmlError::journal(format!("operation {operation_id} not found")))?;
            validate_operation_transition(op.state, to).map_err(DmlError::journal)?;
            op.state = to;
            Ok(())
        }

        fn record_fact(&self, operation_id: i64, fact: OperationFact) -> Result<(), DmlError> {
            let mut guard = self.inner.lock().unwrap();
            let op = guard
                .ops
                .get_mut(&operation_id)
                .ok_or_else(|| DmlError::journal(format!("operation {operation_id} not found")))?;
            validate_operation_transition(op.state, fact.state).map_err(DmlError::journal)?;
            op.state = fact.state;
            if fact.commit_outcome.is_some() {
                op.commit_outcome = fact.commit_outcome;
            }
            if fact.cleanup_outcome.is_some() {
                op.cleanup_outcome = fact.cleanup_outcome;
            }
            if fact.recovery_evidence.is_some() {
                op.recovery_evidence = fact.recovery_evidence;
            }
            if fact.failure.is_some() {
                op.failure = fact.failure;
            }
            Ok(())
        }

        fn load(&self, operation_id: i64) -> Result<Option<StoredOperation>, DmlError> {
            Ok(self.inner.lock().unwrap().ops.get(&operation_id).cloned())
        }

        fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .ops
                .values()
                .filter(|op| !op.state.is_finished())
                .cloned()
                .collect())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::dml::model::{OperationKind, OperationTarget};

        fn request() -> CreatePreparingRequest {
            CreatePreparingRequest {
                operation_kind: OperationKind::InsertAppend,
                operation_subkind: None,
                target: OperationTarget {
                    catalog: "cat".to_string(),
                    namespace: "ns".to_string(),
                    table: "tbl".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: None,
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: Vec::new(),
                created_at_ms: 1,
            }
        }

        #[test]
        fn create_then_load_starts_in_preparing() {
            let journal = InMemoryOperationJournal::default();
            let id = journal.create_preparing(request()).unwrap();
            assert_eq!(
                journal.load(id).unwrap().unwrap().state,
                OperationState::Preparing
            );
        }

        #[test]
        fn illegal_transition_is_rejected() {
            let journal = InMemoryOperationJournal::default();
            let id = journal.create_preparing(request()).unwrap();
            // Preparing -> Finalized is not a legal edge.
            assert!(journal.transition(id, OperationState::Finalized).is_err());
        }

        #[test]
        fn unfinished_list_excludes_terminal_ops() {
            let journal = InMemoryOperationJournal::default();
            let id = journal.create_preparing(request()).unwrap();
            assert_eq!(journal.list_unfinished().unwrap().len(), 1);
            journal.transition(id, OperationState::Aborting).unwrap();
            journal.transition(id, OperationState::Aborted).unwrap();
            assert!(journal.list_unfinished().unwrap().is_empty());
        }
    }
}

#[cfg(test)]
mod meta_store_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use novarocks::meta::SqliteMetaStoreProvider;

    use super::*;
    use crate::dml::model::{CleanupAttempt, CommitServiceError, OperationKind, OperationTarget};

    fn request() -> CreatePreparingRequest {
        CreatePreparingRequest {
            operation_kind: OperationKind::InsertAppend,
            operation_subkind: None,
            target: OperationTarget {
                catalog: "cat".to_string(),
                namespace: "ns".to_string(),
                table: "tbl".to_string(),
                ref_name: None,
            },
            attempt_id: "attempt-1".to_string(),
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
            staged_artifacts: Vec::new(),
            created_at_ms: 1,
        }
    }

    #[test]
    fn round_trip_over_real_sqlite_provider() {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider: Arc<dyn MetaStoreProvider> = Arc::new(
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("provider"),
        );
        let journal = MetaStoreOperationJournal::new(provider);

        let id = journal.create_preparing(request()).unwrap();
        assert_eq!(
            journal.load(id).unwrap().unwrap().state,
            OperationState::Preparing
        );

        journal.transition(id, OperationState::Committing).unwrap();
        assert_eq!(journal.list_unfinished().unwrap().len(), 1);

        let fact = crate::dml::reconcile::operation_fact_from_commit_result(Ok(
            &crate::dml::model::CommitOutcome {
                new_snapshot_id: 100,
                written_manifest_paths: vec![],
            },
        ));
        journal.record_fact(id, fact).unwrap();
        let stored = journal.load(id).unwrap().unwrap();
        assert_eq!(stored.state, OperationState::Committed);
        assert_eq!(stored.commit_outcome.unwrap().snapshot_id, 100);
    }

    fn create_committing(journal: &MetaStoreOperationJournal) -> i64 {
        let id = journal.create_preparing(request()).unwrap();
        journal.transition(id, OperationState::Committing).unwrap();
        id
    }

    fn known_uncommitted_fact(
        message: &str,
        cleanup: CleanupAttempt,
    ) -> crate::dml::model::OperationFact {
        crate::dml::reconcile::operation_fact_from_commit_result(Err(
            &CommitServiceError::known_uncommitted(message.to_string(), cleanup),
        ))
    }

    #[test]
    fn identical_fact_replay_succeeds_over_real_sqlite_provider() {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider: Arc<dyn MetaStoreProvider> = Arc::new(
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("provider"),
        );
        let journal = MetaStoreOperationJournal::new(provider);
        let id = create_committing(&journal);
        let fact = known_uncommitted_fact("conflict", CleanupAttempt::not_attempted());

        journal.record_fact(id, fact.clone()).unwrap();
        journal.record_fact(id, fact).unwrap();

        let stored = journal.load(id).unwrap().unwrap();
        assert_eq!(stored.state, OperationState::FailedKnownUncommitted);
        assert!(!stored.cleanup_outcome.unwrap().attempted);
    }

    #[test]
    fn cleanup_fact_refinement_succeeds_over_real_sqlite_provider() {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider: Arc<dyn MetaStoreProvider> = Arc::new(
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("provider"),
        );
        let journal = MetaStoreOperationJournal::new(provider);
        let id = create_committing(&journal);

        journal
            .record_fact(
                id,
                known_uncommitted_fact("conflict", CleanupAttempt::not_attempted()),
            )
            .unwrap();
        journal
            .record_fact(
                id,
                known_uncommitted_fact("conflict", CleanupAttempt::completed(Vec::new())),
            )
            .unwrap();

        let stored = journal.load(id).unwrap().unwrap();
        let cleanup = stored.cleanup_outcome.unwrap();
        assert!(cleanup.attempted);
        assert_eq!(cleanup.error_count, 0);
        assert_eq!(
            stored.failure.unwrap().next_action,
            crate::dml::model::IcebergOperationNextAction::None
        );
    }

    #[test]
    fn conflicting_fact_replay_fails_over_real_sqlite_provider() {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider: Arc<dyn MetaStoreProvider> = Arc::new(
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("provider"),
        );
        let journal = MetaStoreOperationJournal::new(provider);
        let id = create_committing(&journal);

        journal
            .record_fact(
                id,
                known_uncommitted_fact("conflict", CleanupAttempt::not_attempted()),
            )
            .unwrap();
        let error = journal
            .record_fact(
                id,
                known_uncommitted_fact("different conflict", CleanupAttempt::not_attempted()),
            )
            .expect_err("conflicting replay must be rejected");

        assert_eq!(error.kind(), crate::dml::error::DmlErrorKind::Journal);
        assert!(
            error
                .to_string()
                .contains("conflicting Iceberg operation fact replay")
        );
    }
}
