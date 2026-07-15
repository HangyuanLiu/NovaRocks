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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use novarocks::state_store::{
    ChangeCursor, ChangePage, ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution,
    ContinuationToken, Direction, Key, KeyRange, OperationId, Precondition, RangePage,
    RangeRequest, ReadTransaction, RunFailure, StateStore, StateStoreError, StateStoreErrorKind,
    StateStoreLimits, StateStoreMetrics, StateStoreOperation, StateStoreOutcome, StoreIdentity,
    StoreRevision, TransactionId, Value, VersionToken, WriteTransaction, derive_transaction_id,
    run_side_effect_free,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn key(bytes: &'static [u8]) -> Key {
    Key::try_from(Bytes::from_static(bytes)).expect("valid key")
}

fn assert_object_safe(_: Arc<dyn StateStore>) {}
fn assert_read_object_safe(_: Box<dyn ReadTransaction>) {}
fn assert_write_object_safe(_: Box<dyn WriteTransaction>) {}

#[derive(Clone, Copy)]
enum ScriptedCommit {
    Committed,
    Conflict,
    TransientBeforeCommit,
    DefiniteFailure,
    CommitUnknown,
}

struct ScriptedStore {
    limits: StateStoreLimits,
    commits: Mutex<VecDeque<ScriptedCommit>>,
    transaction_ids: Mutex<Vec<TransactionId>>,
}

impl ScriptedStore {
    fn new(commits: impl IntoIterator<Item = ScriptedCommit>) -> Self {
        Self {
            limits: StateStoreLimits::default(),
            commits: Mutex::new(commits.into_iter().collect()),
            transaction_ids: Mutex::new(Vec::new()),
        }
    }

    fn with_limits(
        limits: StateStoreLimits,
        commits: impl IntoIterator<Item = ScriptedCommit>,
    ) -> Self {
        Self {
            limits,
            commits: Mutex::new(commits.into_iter().collect()),
            transaction_ids: Mutex::new(Vec::new()),
        }
    }

    fn transaction_ids(&self) -> Vec<TransactionId> {
        self.transaction_ids
            .lock()
            .expect("transaction ids")
            .clone()
    }
}

struct ScriptedWriteTransaction {
    transaction_id: TransactionId,
    commit: ScriptedCommit,
}

#[async_trait]
impl ReadTransaction for ScriptedWriteTransaction {
    async fn get(
        &mut self,
        _key: &Key,
    ) -> Result<Option<novarocks::state_store::StateRecord>, StateStoreError> {
        unreachable!("runner tests do not read")
    }

    async fn range(&mut self, _request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        unreachable!("runner tests do not scan")
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        Ok(())
    }
}

#[async_trait]
impl WriteTransaction for ScriptedWriteTransaction {
    fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    async fn put(
        &mut self,
        _key: Key,
        _value: Value,
        _precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        unreachable!("runner tests do not write")
    }

    async fn delete(
        &mut self,
        _key: Key,
        _precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        unreachable!("runner tests do not delete")
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        let error = || {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "scripted state store outcome",
            )
        };
        match self.commit {
            ScriptedCommit::Committed => CommitOutcome::Committed(CommitReceipt {
                transaction_id: self.transaction_id,
                revision: StoreRevision::try_from(Bytes::from_static(b"revision"))
                    .expect("revision"),
            }),
            ScriptedCommit::Conflict => CommitOutcome::Conflict(error()),
            ScriptedCommit::TransientBeforeCommit => CommitOutcome::TransientBeforeCommit(error()),
            ScriptedCommit::DefiniteFailure => CommitOutcome::DefiniteFailure(error()),
            ScriptedCommit::CommitUnknown => CommitOutcome::CommitUnknown(error()),
        }
    }
}

#[async_trait]
impl StateStore for ScriptedStore {
    fn provider_name(&self) -> &'static str {
        "scripted"
    }

    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        unreachable!("runner tests do not begin reads")
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.transaction_ids
            .lock()
            .expect("transaction ids")
            .push(transaction_id);
        let commit = self
            .commits
            .lock()
            .expect("commit script")
            .pop_front()
            .expect("scripted commit outcome");
        Ok(Box::new(ScriptedWriteTransaction {
            transaction_id,
            commit,
        }))
    }

    async fn poll_changes(
        &self,
        _request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        unreachable!("runner tests do not poll changes")
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        unreachable!("runner tests do not load identity")
    }

    async fn resolve_commit(
        &self,
        _transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        unreachable!("runner tests do not resolve commits")
    }
}

#[test]
fn contract_accepts_binary_payloads_and_rejects_invalid_ranges() {
    let binary = key(&[0, 255]);
    assert_eq!(binary.as_bytes(), &[0, 255]);
    assert_eq!(
        Value::try_from(Bytes::from_static(&[255, 0]))
            .expect("binary value")
            .as_bytes(),
        &[255, 0]
    );
    assert_eq!(
        VersionToken::try_from(Bytes::from_static(&[0, 255]))
            .expect("binary version")
            .as_bytes(),
        &[0, 255]
    );

    for (start, end) in [(key(&[1]), key(&[1])), (key(&[2]), key(&[1]))] {
        let error = KeyRange::new(start, end).expect_err("range must be increasing");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest);
    }
}

#[test]
fn contract_enforces_common_binary_and_page_bounds() {
    let limits = StateStoreLimits::default();
    assert_eq!(
        Key::try_from(Bytes::from(vec![0; limits.max_key_bytes + 1]))
            .expect_err("oversized key")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert_eq!(
        Value::try_from(Bytes::from(vec![0; limits.max_value_bytes + 1]))
            .expect_err("oversized value")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert_eq!(
        StoreRevision::try_from(Bytes::new())
            .expect_err("empty revision")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    for page_size in [0, limits.max_page_size + 1] {
        let request = RangeRequest {
            range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
            direction: Direction::Forward,
            page_size,
            continuation: None,
        };
        assert_eq!(
            request
                .validate(&limits)
                .expect_err("invalid page size")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }
}

#[test]
fn contract_prefix_range_requires_a_finite_successor() {
    let range = KeyRange::for_prefix(key(&[0, 255])).expect("finite prefix successor");
    assert_eq!(range.start.as_bytes(), &[0, 255]);
    assert_eq!(range.end.as_bytes(), &[1]);

    let error = KeyRange::for_prefix(key(&[255, 255])).expect_err("all-ff has no successor");
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest);
}

#[test]
fn contract_continuation_binds_range_and_direction() {
    let forward = RangeRequest {
        range: KeyRange::new(key(&[0]), key(&[2])).expect("range"),
        direction: Direction::Forward,
        page_size: 10,
        continuation: None,
    };
    let reverse = RangeRequest {
        direction: Direction::Reverse,
        ..forward.clone()
    };
    let other_range = RangeRequest {
        range: KeyRange::new(key(&[0]), key(&[3])).expect("range"),
        ..forward.clone()
    };

    let token = forward.continuation_after(&key(&[1])).expect("token");
    assert_eq!(
        token.resume_after(&forward).expect("matching request"),
        key(&[1])
    );
    assert_eq!(
        token
            .resume_after(&reverse)
            .expect_err("direction mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(
        token
            .resume_after(&other_range)
            .expect_err("range mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn contract_codecs_reject_malformed_and_mismatched_tokens() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[0, 255])).expect("token");
    let mut trailing = token.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        novarocks::state_store::ContinuationToken::try_from(Bytes::from(trailing))
            .expect("opaque token")
            .resume_after(&request)
            .expect_err("trailing bytes")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let store_id = Uuid::now_v7();
    let revision = StoreRevision::try_from(Bytes::from_static(&[255, 255])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision.clone(), 42).expect("cursor");
    let (decoded_revision, sequence) = cursor.decode(store_id).expect("matching store");
    assert_eq!(decoded_revision, revision);
    assert_eq!(sequence, 42);
    assert_eq!(
        cursor
            .decode(Uuid::now_v7())
            .expect_err("store mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let mut trailing = cursor.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        ChangeCursor::try_from(Bytes::from(trailing))
            .expect("opaque cursor")
            .decode(store_id)
            .expect_err("trailing bytes")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn contract_codecs_preserve_their_error_context() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[1])).expect("token");
    let mut trailing_token = token.as_bytes().to_vec();
    trailing_token.push(0);
    for malformed in [
        Bytes::copy_from_slice(&token.as_bytes()[..token.as_bytes().len() - 1]),
        Bytes::from(trailing_token),
    ] {
        let error = ContinuationToken::try_from(malformed)
            .expect("opaque token")
            .resume_after(&request)
            .expect_err("malformed token");
        assert_eq!(
            error.to_string(),
            "InvalidRequest: invalid continuation token"
        );
    }

    let store_id = Uuid::now_v7();
    let revision = StoreRevision::try_from(Bytes::from_static(&[1])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision, 1).expect("cursor");
    let mut trailing_cursor = cursor.as_bytes().to_vec();
    trailing_cursor.push(0);
    for malformed in [
        Bytes::copy_from_slice(&cursor.as_bytes()[..cursor.as_bytes().len() - 1]),
        Bytes::from(trailing_cursor),
    ] {
        let error = ChangeCursor::try_from(malformed)
            .expect("opaque cursor")
            .decode(store_id)
            .expect_err("malformed cursor");
        assert_eq!(error.to_string(), "InvalidRequest: invalid change cursor");
    }
}

#[test]
fn contract_continuation_codec_has_the_stable_v1_layout() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[0, 255]), key(&[2])).expect("range"),
        direction: Direction::Reverse,
        page_size: 7,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[1, 0])).expect("token");
    let encoded = token.as_bytes();

    let expected_fingerprint = Sha256::digest(
        [
            &[1, 1][..],
            &2_u32.to_be_bytes(),
            &[0, 255],
            &1_u32.to_be_bytes(),
            &[2],
        ]
        .concat(),
    );
    assert_eq!(&encoded[..2], &[1, 1]);
    assert_eq!(&encoded[2..34], expected_fingerprint.as_slice());
    assert_eq!(&encoded[34..38], &2_u32.to_be_bytes());
    assert_eq!(&encoded[38..], &[1, 0]);

    for malformed in [
        Bytes::new(),
        Bytes::from_static(&[2, 1]),
        Bytes::copy_from_slice(&encoded[..encoded.len() - 1]),
    ] {
        assert_eq!(
            ContinuationToken::try_from(malformed)
                .expect("opaque token")
                .resume_after(&request)
                .expect_err("malformed token")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }
}

#[test]
fn contract_change_cursor_has_the_stable_v1_layout() {
    let store_id = Uuid::from_bytes([7; 16]);
    let revision = StoreRevision::try_from(Bytes::from_static(&[0, 255])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision, 0x01020304).expect("cursor");
    let encoded = cursor.as_bytes();

    assert_eq!(encoded[0], 1);
    assert_eq!(&encoded[1..17], store_id.as_bytes());
    assert_eq!(&encoded[17..21], &2_u32.to_be_bytes());
    assert_eq!(&encoded[21..23], &[0, 255]);
    assert_eq!(&encoded[23..27], &0x01020304_u32.to_be_bytes());

    for malformed in [
        Bytes::new(),
        Bytes::from_static(&[2]),
        Bytes::copy_from_slice(&encoded[..encoded.len() - 1]),
    ] {
        assert_eq!(
            ChangeCursor::try_from(malformed)
                .expect("opaque cursor")
                .decode(store_id)
                .expect_err("malformed cursor")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }
}

#[test]
fn contract_error_surface_is_typed_and_provider_neutral() {
    let kinds = [
        StateStoreErrorKind::InvalidRequest,
        StateStoreErrorKind::InvalidConfiguration,
        StateStoreErrorKind::UnsupportedDeployment,
        StateStoreErrorKind::LimitExceeded,
        StateStoreErrorKind::DeadlineExceeded,
        StateStoreErrorKind::PreconditionFailed,
        StateStoreErrorKind::Conflict,
        StateStoreErrorKind::Transient,
        StateStoreErrorKind::Corruption,
        StateStoreErrorKind::ProviderUnavailable,
        StateStoreErrorKind::Cancelled,
        StateStoreErrorKind::Internal,
    ];
    for kind in kinds {
        let error = StateStoreError::new(kind, "state store operation failed");
        assert_eq!(error.kind(), kind);
        assert!(!error.to_string().contains("SELECT"));
        assert!(!error.to_string().contains("password"));
    }
}

#[test]
fn contract_traits_are_object_safe() {
    let _ = assert_object_safe as fn(Arc<dyn StateStore>);
    let _ = assert_read_object_safe as fn(Box<dyn ReadTransaction>);
    let _ = assert_write_object_safe as fn(Box<dyn WriteTransaction>);
}

#[tokio::test]
async fn runner_replays_the_whole_operation_for_retryable_commit_outcomes() {
    let store = ScriptedStore::new([
        ScriptedCommit::Conflict,
        ScriptedCommit::TransientBeforeCommit,
        ScriptedCommit::Committed,
    ]);
    let metrics = StateStoreMetrics::new(store.provider_name());
    let operation_id = OperationId::new_v7();
    let operation_runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let success = run_side_effect_free(&store, &metrics, operation_id, "runner retry test", {
        let operation_runs = Arc::clone(&operation_runs);
        move |_transaction| -> BoxFuture<'_, Result<usize, StateStoreError>> {
            let run = operation_runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Box::pin(async move { Ok(run) })
        }
    })
    .await
    .expect("third attempt commits");

    assert_eq!(success.value, 3);
    assert_eq!(operation_runs.load(std::sync::atomic::Ordering::SeqCst), 3);
    let expected_ids = (1..=3)
        .map(|attempt| derive_transaction_id(operation_id, attempt))
        .collect::<Vec<_>>();
    assert_eq!(store.transaction_ids(), expected_ids);
    assert_eq!(success.receipt.transaction_id, expected_ids[2]);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.begin_count, 3);
    assert_eq!(snapshot.commit_count, 3);
    assert_eq!(snapshot.retry_count, 2);
    assert_eq!(
        snapshot.operation_outcome_count(StateStoreOperation::Commit, StateStoreOutcome::Conflict),
        1
    );
    assert_eq!(
        snapshot.operation_outcome_count(
            StateStoreOperation::Commit,
            StateStoreOutcome::TransientBeforeCommit
        ),
        1
    );
}

#[tokio::test]
async fn runner_does_not_retry_operation_definite_or_unknown_failures() {
    let operation_error_store = ScriptedStore::new([ScriptedCommit::Committed]);
    let operation_error_runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failure = run_side_effect_free(
        &operation_error_store,
        &StateStoreMetrics::new(operation_error_store.provider_name()),
        OperationId::new_v7(),
        "operation error test",
        {
            let operation_error_runs = Arc::clone(&operation_error_runs);
            move |_transaction| -> BoxFuture<'_, Result<(), StateStoreError>> {
                operation_error_runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async {
                    Err(StateStoreError::new(
                        StateStoreErrorKind::InvalidRequest,
                        "scripted operation failure",
                    ))
                })
            }
        },
    )
    .await
    .expect_err("operation failure is terminal");
    assert!(matches!(failure, RunFailure::Operation(_)));
    assert_eq!(
        operation_error_runs.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(operation_error_store.transaction_ids().len(), 1);

    for (outcome, expect_unknown) in [
        (ScriptedCommit::DefiniteFailure, false),
        (ScriptedCommit::CommitUnknown, true),
    ] {
        let store = ScriptedStore::new([outcome]);
        let operation_id = OperationId::new_v7();
        let failure = run_side_effect_free(
            &store,
            &StateStoreMetrics::new(store.provider_name()),
            operation_id,
            "terminal commit outcome test",
            |_transaction| Box::pin(async { Ok(()) }),
        )
        .await
        .expect_err("commit outcome is terminal");
        if expect_unknown {
            match failure {
                RunFailure::CommitUnknown { transaction_id, .. } => {
                    assert_eq!(transaction_id, derive_transaction_id(operation_id, 1));
                }
                other => panic!("expected CommitUnknown, got {other:?}"),
            }
        } else {
            assert!(matches!(failure, RunFailure::DefiniteFailure(_)));
        }
        assert_eq!(store.transaction_ids().len(), 1);
    }
}

#[test]
fn runner_derives_stable_distinct_uuid_v7_attempt_ids() {
    let operation_id = OperationId::new_v7();
    let first = derive_transaction_id(operation_id, 1);
    let first_again = derive_transaction_id(operation_id, 1);
    let second = derive_transaction_id(operation_id, 2);

    assert_eq!(first, first_again);
    assert_ne!(first, second);
    assert_eq!(
        &first.as_uuid().as_bytes()[..6],
        &operation_id.as_uuid().as_bytes()[..6]
    );
    assert_eq!(first.as_uuid().get_version(), Some(uuid::Version::SortRand));
    assert_eq!(first.as_uuid().get_variant(), uuid::Variant::RFC4122);

    let digest = Sha256::digest(
        [
            operation_id.as_uuid().as_bytes().as_slice(),
            &1_u32.to_be_bytes(),
        ]
        .concat(),
    );
    let mut expected = [0_u8; 16];
    expected[..6].copy_from_slice(&operation_id.as_uuid().as_bytes()[..6]);
    expected[6..].copy_from_slice(&digest[..10]);
    expected[6] = (expected[6] & 0x0f) | 0x70;
    expected[8] = (expected[8] & 0x3f) | 0x80;
    assert_eq!(first.as_uuid().as_bytes(), &expected);

    assert!(std::panic::catch_unwind(|| derive_transaction_id(operation_id, 0)).is_err());
    assert!(std::panic::catch_unwind(|| derive_transaction_id(operation_id, 6)).is_err());
}

#[tokio::test]
async fn runner_stops_at_the_attempt_budget_without_a_sixth_id() {
    let store = ScriptedStore::new([ScriptedCommit::Conflict; 5]);
    let operation_id = OperationId::new_v7();
    let failure = run_side_effect_free(
        &store,
        &StateStoreMetrics::new(store.provider_name()),
        operation_id,
        "retry budget test",
        |_transaction| Box::pin(async { Ok(()) }),
    )
    .await
    .expect_err("five conflicts exhaust retry budget");

    assert!(matches!(failure, RunFailure::RetryExhausted(_)));
    assert_eq!(store.transaction_ids().len(), 5);
    assert_eq!(
        store.transaction_ids().last().copied(),
        Some(derive_transaction_id(operation_id, 5))
    );
}

#[tokio::test]
async fn runner_enforces_one_total_deadline_without_retrying_a_slow_operation() {
    let limits = StateStoreLimits {
        transaction_deadline: Duration::from_millis(20),
        ..StateStoreLimits::default()
    };
    let store = ScriptedStore::with_limits(limits, [ScriptedCommit::Committed]);
    let metrics = StateStoreMetrics::new(store.provider_name());
    let failure = run_side_effect_free(
        &store,
        &metrics,
        OperationId::new_v7(),
        "deadline test",
        |_transaction| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(())
            })
        },
    )
    .await
    .expect_err("operation exceeds total deadline");

    assert!(matches!(failure, RunFailure::DeadlineExceeded));
    assert_eq!(store.transaction_ids().len(), 1);
    assert_eq!(metrics.snapshot().deadline_count, 1);
}

#[test]
fn runner_metrics_have_only_fixed_low_cardinality_dimensions() {
    let metrics = StateStoreMetrics::new("scripted");
    metrics.record_operation(StateStoreOperation::Get, StateStoreOutcome::Success);
    metrics.record_operation(StateStoreOperation::Range, StateStoreOutcome::Error);
    metrics.record_operation(StateStoreOperation::Put, StateStoreOutcome::Success);
    metrics.record_operation(StateStoreOperation::Delete, StateStoreOutcome::Error);
    metrics.record_bytes_read(11);
    metrics.record_bytes_written(13);
    metrics.record_page_records(17);
    metrics.record_notification_lag(Duration::from_millis(19));
    metrics.record_blocking_failure();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.provider, "scripted");
    assert_eq!(snapshot.get_count, 1);
    assert_eq!(snapshot.range_count, 1);
    assert_eq!(snapshot.put_count, 1);
    assert_eq!(snapshot.delete_count, 1);
    assert_eq!(snapshot.bytes_read, 11);
    assert_eq!(snapshot.bytes_written, 13);
    assert_eq!(snapshot.page_records, 17);
    assert_eq!(snapshot.notification_lag_micros, 19_000);
    assert_eq!(snapshot.blocking_failure_count, 1);

    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("customer/secret-key"));
    assert!(!debug.contains("secret-value"));
    assert!(!debug.contains("password"));
}
