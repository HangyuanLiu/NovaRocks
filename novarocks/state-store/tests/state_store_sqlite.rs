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

mod common;

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_state_store::limits::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
use novarocks_state_store::{
    ChangeCursor, ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution, Direction,
    FeDeploymentView, Key, KeyRange, Precondition, RangeRequest, StateStore, StateStoreConfig,
    StateStoreErrorKind, StateStoreLimitOverrides, StateStoreOperation, StateStoreOutcome,
    StateStoreProviderConfig, StateStoreRuntime, StoreRevision, TransactionId, Value,
    open_state_store,
};
use rusqlite::{Connection, params};
use tempfile::TempDir;
use tokio::sync::Barrier;
use uuid::Uuid;

use common::state_store_conformance::{
    FaultGate, FaultInjectingStateStore, PostDispatchControl, PostDispatchController,
    PostDispatchScenario, StateStoreConformanceFixture, StateStoreFactory,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordination_gate_suite() {
    common::state_store_coordination_conformance::incarnation_gate_lifecycle(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::concurrent_bootstrap_converges(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::stale_snapshots_cannot_mutate(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::incarnation_overflow_fails_closed(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::identity_mismatch_is_corruption(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::recovery_is_operation_scoped(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::commit_unknown_uses_authoritative_read_back(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::cancelled_mutation_recovers_with_same_operation(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::unresolved_bootstrap_without_visible_record_is_uncertain(
        &coordination_factory(),
    )
    .await;
    common::state_store_coordination_conformance::admission_read_conflicts_with_restore(
        &coordination_factory(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordination_acquire_suite() {
    let factory = coordination_factory();
    common::state_store_coordination_conformance::basic_acquire_contention_and_high_watermark(
        &factory,
    )
    .await;
    common::state_store_coordination_conformance::concurrent_acquire_exactly_one_winner(&factory)
        .await;
    common::state_store_coordination_conformance::external_lease_clock_error_is_clock_unsafe(
        &factory,
    )
    .await;
}

fn key(bytes: impl Into<Vec<u8>>) -> Key {
    Key::try_from(Bytes::from(bytes.into())).expect("valid key")
}

fn value(bytes: &'static [u8]) -> Value {
    Value::try_from(Bytes::from_static(bytes)).expect("valid value")
}

fn transaction_id() -> TransactionId {
    Uuid::now_v7().into()
}

fn operation_total(
    snapshot: &novarocks_state_store::StateStoreMetricsSnapshot,
    operation: StateStoreOperation,
) -> u64 {
    snapshot.operation_outcomes[operation as usize].iter().sum()
}

fn assert_failed_operation_observed(
    before: &novarocks_state_store::StateStoreMetricsSnapshot,
    after: &novarocks_state_store::StateStoreMetricsSnapshot,
    operation: StateStoreOperation,
) {
    assert_eq!(
        operation_total(after, operation),
        operation_total(before, operation) + 1
    );
    assert_eq!(
        after.operation_outcome_count(operation, StateStoreOutcome::Error),
        before.operation_outcome_count(operation, StateStoreOutcome::Error) + 1
    );
    assert_eq!(
        after.operation_duration_observations(operation),
        before.operation_duration_observations(operation) + 1
    );
}

async fn open_store(temp: &TempDir, owner: &str) -> Arc<dyn StateStore> {
    open_store_with_limits(temp, owner, StateStoreLimitOverrides::default()).await
}

async fn open_store_with_limits(
    temp: &TempDir,
    owner: &str,
    limits: StateStoreLimitOverrides,
) -> Arc<dyn StateStore> {
    let runtime = StateStoreRuntime::local().expect("create local state store runtime");
    open_state_store(
        &runtime,
        StateStoreConfig {
            cluster_id: "cluster-a".to_owned(),
            limits,
            provider: StateStoreProviderConfig::Sqlite {
                path: temp.path().join("state-store.sqlite"),
                deployment_owner: owner.to_owned(),
            },
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).expect("one FE"),
            topology_revision: Bytes::from_static(b"topology-r1"),
        },
    )
    .await
    .expect("open public SQLite state store")
}

async fn read_state_record(
    store: &Arc<dyn StateStore>,
    item: &Key,
) -> Option<novarocks_state_store::StateRecord> {
    let mut reader = store.begin_read().await.expect("begin state record read");
    let record = reader.get(item).await.expect("read state record");
    reader.abort().await.expect("abort state record read");
    record
}

fn conformance_factory() -> StateStoreFactory {
    state_store_factory(1_024)
}

fn coordination_factory() -> StateStoreFactory {
    // The current control read+put upper bound is 1,039 bytes. Keep the ordinary
    // provider conformance limit at 1 KiB and widen only this coordination fixture.
    state_store_factory(2 * 1_024)
}

fn state_store_factory(max_transaction_bytes: usize) -> StateStoreFactory {
    let temp = Arc::new(TempDir::new().expect("conformance temp dir"));
    std::rc::Rc::new(move || {
        let temp = Arc::clone(&temp);
        Box::pin(async move {
            let path = temp.path().join("state-store.sqlite");
            let runtime = StateStoreRuntime::local()?;
            let store = open_state_store(
                &runtime,
                StateStoreConfig {
                    cluster_id: "conformance-cluster".to_owned(),
                    limits: StateStoreLimitOverrides {
                        max_key_bytes: Some(64),
                        max_value_bytes: Some(128),
                        max_page_size: Some(10),
                        max_transaction_operations: Some(8),
                        max_transaction_bytes: Some(max_transaction_bytes),
                        transaction_deadline_ms: Some(250),
                        runner_max_attempts: Some(3),
                    },
                    provider: StateStoreProviderConfig::Sqlite {
                        path: path.clone(),
                        deployment_owner: "conformance-fe".to_owned(),
                    },
                },
                FeDeploymentView {
                    active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                    topology_revision: Bytes::from_static(b"conformance-topology"),
                },
            )
            .await?;
            let fault = FaultInjectingStateStore::new(store);
            let controller: Arc<dyn PostDispatchController> =
                Arc::new(SqlitePostDispatchController {
                    fault: Arc::clone(&fault),
                    path,
                });
            let store: Arc<dyn StateStore> = fault;
            Ok(StateStoreConformanceFixture::new(store, controller))
        })
    })
}

struct SqlitePostDispatchController {
    fault: Arc<FaultInjectingStateStore>,
    path: PathBuf,
}

#[async_trait]
impl PostDispatchController for SqlitePostDispatchController {
    async fn arm(&self, scenario: PostDispatchScenario) -> Box<dyn PostDispatchControl> {
        let path = self.path.clone();
        let blocker = tokio::task::spawn_blocking(move || {
            let connection = Connection::open(path).expect("open post-dispatch blocker");
            connection
                .execute_batch("BEGIN IMMEDIATE")
                .expect("hold post-dispatch provider progress");
            connection
        })
        .await
        .expect("create post-dispatch blocker");
        let gate = FaultGate::new();
        match scenario {
            PostDispatchScenario::CancelWaiterBeforeApply => {
                self.fault.pause_next_post_dispatch(gate.clone());
            }
            PostDispatchScenario::LoseCommittedResponse => {
                self.fault.lose_next_post_dispatch_response(gate.clone());
            }
        }
        Box::new(SqlitePostDispatchControl {
            gate,
            blocker: Mutex::new(Some(blocker)),
        })
    }
}

struct SqlitePostDispatchControl {
    gate: FaultGate,
    blocker: Mutex<Option<Connection>>,
}

#[async_trait]
impl PostDispatchControl for SqlitePostDispatchControl {
    async fn wait_dispatched(&self) {
        self.gate.wait_reached().await;
        self.gate.wait_armed().await;
    }

    async fn wait_waiter_cancelled(&self) {
        self.gate.wait_cancelled().await;
    }

    async fn allow_provider_progress(&self) {
        let blocker = self.blocker.lock().expect("post-dispatch blocker").take();
        if let Some(blocker) = blocker {
            tokio::task::spawn_blocking(move || {
                blocker
                    .execute_batch("ROLLBACK")
                    .expect("release post-dispatch provider progress");
            })
            .await
            .expect("release post-dispatch blocker");
        }
    }

    async fn release_response(&self) {
        self.gate.release().await;
    }

    async fn wait_inner_dropped(&self) {
        self.gate.wait_inner_dropped().await;
    }
}

mod conformance {
    use super::*;
    use common::state_store_conformance;

    fn hold_sqlite_writer_lock(path: &std::path::Path) -> Connection {
        let connection = Connection::open(path).expect("open SQLite conformance blocker");
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .expect("hold SQLite conformance writer lock");
        connection
    }

    fn durable_counts(path: &std::path::Path) -> (i64, i64, i64) {
        let connection = Connection::open(path).expect("open SQLite durable-state observer");
        (
            connection
                .query_row("SELECT COUNT(*) FROM state_store_kv", [], |row| row.get(0))
                .expect("count durable KV rows"),
            connection
                .query_row("SELECT COUNT(*) FROM state_store_changes", [], |row| {
                    row.get(0)
                })
                .expect("count durable change rows"),
            connection
                .query_row("SELECT COUNT(*) FROM state_store_commits", [], |row| {
                    row.get(0)
                })
                .expect("count durable commit rows"),
        )
    }

    async fn wait_for_resolution(
        store: &Arc<dyn StateStore>,
        transaction_id: &TransactionId,
        expected: CommitResolution,
    ) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let resolution = store
                    .resolve_commit(transaction_id)
                    .await
                    .expect("resolve SQLite conformance commit");
                if resolution == expected {
                    break;
                }
                assert_eq!(resolution, CommitResolution::Unresolved);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("SQLite commit resolution must become terminal");
        for _ in 0..3 {
            assert_eq!(
                store
                    .resolve_commit(transaction_id)
                    .await
                    .expect("repeat terminal SQLite resolution"),
                expected,
                "terminal resolution must not regress"
            );
        }
    }

    macro_rules! conformance_test {
        ($name:ident) => {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $name() {
                let factory = conformance_factory();
                state_store_conformance::$name(&factory).await;
            }
        };
    }

    conformance_test!(snapshot_repeatable_read);
    conformance_test!(same_key_conflict);
    conformance_test!(write_skew_conflict);
    conformance_test!(range_phantom_conflict);
    conformance_test!(preconditions);
    conformance_test!(forward_reverse_pages);
    conformance_test!(same_revision_change_pages);
    conformance_test!(notification_delivery_faults);
    conformance_test!(atomic_commit);
    conformance_test!(post_dispatch_cancel_waiter_reconciles);
    conformance_test!(post_dispatch_response_loss_reconciles);
    conformance_test!(limits_before_io);
    conformance_test!(arbitrary_binary_payloads);

    async fn assert_cancelled_commit_roundtrip(
        store: &Arc<dyn StateStore>,
        path: &std::path::Path,
        iteration: u8,
    ) {
        let fault = FaultInjectingStateStore::new(Arc::clone(&store));
        let transaction_id = transaction_id();
        let keys = [
            key(vec![b'c', iteration, b'a']),
            key(vec![b'c', iteration, b'b']),
        ];
        let mut transaction = fault
            .begin_write(transaction_id, "cancelled real commit")
            .await
            .expect("begin cancelled real commit");
        for item in &keys {
            transaction
                .put(item.clone(), value(b"value"), Precondition::Any)
                .await
                .expect("stage cancelled real commit row");
        }

        let blocker = hold_sqlite_writer_lock(&path);
        let gate = FaultGate::new();
        fault.pause_next_post_dispatch(gate.clone());
        let waiter = tokio::spawn(async move { transaction.commit().await });
        gate.wait_reached().await;
        gate.wait_armed().await;
        waiter.abort();
        assert!(
            waiter
                .await
                .expect_err("cancel commit waiter")
                .is_cancelled()
        );
        gate.wait_cancelled().await;
        for _ in 0..3 {
            assert_eq!(
                store
                    .resolve_commit(&transaction_id)
                    .await
                    .expect("resolve cancelled in-flight commit"),
                CommitResolution::Unresolved,
                "a cancelled waiter must not publish NotCommitted while its worker is blocked"
            );
        }
        assert_eq!(durable_counts(&path), (0, 0, 0));

        gate.release().await;
        gate.wait_inner_dropped().await;
        wait_for_resolution(&store, &transaction_id, CommitResolution::NotCommitted).await;
        assert_eq!(durable_counts(&path), (0, 0, 0));
        for item in keys {
            assert!(read_state_record(&store, &item).await.is_none());
        }
        blocker
            .execute_batch("ROLLBACK")
            .expect("release cancelled commit writer lock");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit_resolution_after_cancel() {
        let temp = TempDir::new().expect("cancelled commit temp dir");
        let path = temp.path().join("state-store.sqlite");
        let store = open_store(&temp, "fe-a").await;
        for iteration in 0..64 {
            assert_cancelled_commit_roundtrip(&store, &path, iteration).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resolve_does_not_report_not_committed_while_inflight() {
        let temp = TempDir::new().expect("committed in-flight temp dir");
        let path = temp.path().join("state-store.sqlite");
        let store = open_store(&temp, "fe-a").await;
        let fault = FaultInjectingStateStore::new(Arc::clone(&store));
        let transaction_id = transaction_id();
        let keys = [key(b"committed-a".to_vec()), key(b"committed-b".to_vec())];
        let mut transaction = fault
            .begin_write(transaction_id, "blocked real commit")
            .await
            .expect("begin blocked real commit");
        for item in &keys {
            transaction
                .put(item.clone(), value(b"value"), Precondition::Any)
                .await
                .expect("stage blocked real commit row");
        }

        let blocker = hold_sqlite_writer_lock(&path);
        let gate = FaultGate::new();
        fault.pause_next_post_dispatch(gate.clone());
        let waiter = tokio::spawn(async move { transaction.commit().await });
        gate.wait_reached().await;
        gate.wait_armed().await;
        for _ in 0..3 {
            assert_eq!(
                store
                    .resolve_commit(&transaction_id)
                    .await
                    .expect("resolve blocked real commit"),
                CommitResolution::Unresolved
            );
        }
        assert_eq!(durable_counts(&path), (0, 0, 0));

        blocker
            .execute_batch("ROLLBACK")
            .expect("release committed writer lock");
        gate.release().await;
        let receipt = match waiter.await.expect("join committed waiter") {
            CommitOutcome::Committed(receipt) => receipt,
            other => panic!("expected committed real outcome, got {other:?}"),
        };
        wait_for_resolution(
            &store,
            &transaction_id,
            CommitResolution::Committed(receipt),
        )
        .await;
        assert_eq!(durable_counts(&path), (2, 2, 1));
        for item in keys {
            assert_eq!(
                read_state_record(&store, &item)
                    .await
                    .expect("committed real row")
                    .value,
                value(b"value")
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deadline_interrupts_blocking_sql() {
        let temp = TempDir::new().expect("deadline temp dir");
        let path = temp.path().join("state-store.sqlite");
        let store = open_store_with_limits(
            &temp,
            "fe-a",
            StateStoreLimitOverrides {
                transaction_deadline_ms: Some(50),
                ..StateStoreLimitOverrides::default()
            },
        )
        .await;
        let item = key(b"deadline-blocked".to_vec());
        let mut transaction = store
            .begin_write(transaction_id(), "deadline blocked SQL")
            .await
            .expect("begin deadline blocked transaction");
        transaction
            .put(item.clone(), value(b"must-not-commit"), Precondition::Any)
            .await
            .expect("stage deadline blocked row");
        let blocker = hold_sqlite_writer_lock(&path);
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), transaction.commit())
            .await
            .expect("deadline must interrupt blocking SQLite commit");
        blocker
            .execute_batch("ROLLBACK")
            .expect("release deadline writer lock");
        match outcome {
            CommitOutcome::DefiniteFailure(error) => {
                assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded)
            }
            other => panic!("expected definite deadline failure, got {other:?}"),
        }
        assert_eq!(durable_counts(&path), (0, 0, 0));
        assert!(read_state_record(&store, &item).await.is_none());

        let abort_temp = TempDir::new().expect("expired abort temp dir");
        let abort_path = abort_temp.path().join("state-store.sqlite");
        let abort_store = open_store_with_limits(
            &abort_temp,
            "fe-a",
            StateStoreLimitOverrides {
                transaction_deadline_ms: Some(20),
                ..StateStoreLimitOverrides::default()
            },
        )
        .await;
        let abort_id = transaction_id();
        let abort_key = key(b"expired-explicit-abort".to_vec());
        let mut expired = abort_store
            .begin_write(abort_id, "expired explicit abort")
            .await
            .expect("begin expired explicit abort");
        expired
            .put(
                abort_key.clone(),
                value(b"must-not-commit"),
                Precondition::Any,
            )
            .await
            .expect("stage expired explicit abort row");
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        let abort_error = tokio::time::timeout(std::time::Duration::from_secs(2), expired.abort())
            .await
            .expect("expired explicit abort must remain bounded")
            .expect_err("expired explicit abort must report its deadline");
        assert_eq!(abort_error.kind(), StateStoreErrorKind::DeadlineExceeded);
        assert_eq!(durable_counts(&abort_path), (0, 0, 0));
        assert_eq!(
            abort_store
                .resolve_commit(&abort_id)
                .await
                .expect("resolve expired explicit abort"),
            CommitResolution::NotCommitted
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_owner_rejected() {
        let temp = TempDir::new().expect("owner lifecycle temp dir");
        let first = open_store(&temp, "fe-a").await;
        let identity = first.identity().await.expect("first owner identity");
        let runtime = StateStoreRuntime::local().expect("create local state store runtime");
        let second = open_state_store(
            &runtime,
            StateStoreConfig {
                cluster_id: "cluster-a".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path: temp.path().join("state-store.sqlite"),
                    deployment_owner: "fe-b".to_owned(),
                },
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"topology-r1"),
            },
        )
        .await;
        assert!(second.is_err(), "second live SQLite owner must be rejected");
        drop(first);
        let restarted = open_store(&temp, "fe-a").await;
        assert_eq!(
            restarted
                .identity()
                .await
                .expect("restarted owner identity"),
            identity
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pagination_range_key_limits_precede_snapshot_io() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store_with_limits(
        &temp,
        "fe-a",
        StateStoreLimitOverrides {
            max_key_bytes: Some(3),
            ..StateStoreLimitOverrides::default()
        },
    )
    .await;
    let mut reader = store.begin_read().await.expect("begin limited read");

    let oversized_boundary = RangeRequest {
        range: KeyRange::new(key(b"four".to_vec()), key(b"zzzzz".to_vec()))
            .expect("oversized bounded range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    assert_eq!(
        reader
            .range(&oversized_boundary)
            .await
            .expect_err("oversized range boundary")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );

    let base = RangeRequest {
        range: KeyRange::new(key(b"a".to_vec()), key(b"z".to_vec())).expect("short range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let oversized_last = base
        .continuation_after(&key(b"long".to_vec()))
        .expect("public continuation with long last key");
    let oversized_continuation = RangeRequest {
        continuation: Some(oversized_last),
        ..base.clone()
    };
    assert_eq!(
        reader
            .range(&oversized_continuation)
            .await
            .expect_err("oversized continuation last key")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );

    commit_puts(&store, &[(key(b"b".to_vec()), value(b"new"))]).await;
    let page = reader
        .range(&base)
        .await
        .expect("valid range after rejected requests");
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].key.as_bytes(), b"b");
    reader.abort().await.expect("abort limited reader");
}

async fn commit_puts(store: &Arc<dyn StateStore>, rows: &[(Key, Value)]) -> CommitReceipt {
    let transaction_id = transaction_id();
    let mut transaction = store
        .begin_write(transaction_id, "test seed")
        .await
        .expect("begin seed write");
    assert_eq!(transaction.transaction_id(), &transaction_id);
    for (key, value) in rows {
        transaction
            .put(key.clone(), value.clone(), Precondition::Any)
            .await
            .expect("stage seed row");
    }
    match transaction.commit().await {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("expected committed seed, got {other:?}"),
    }
}

fn bounded_range(direction: Direction, page_size: usize) -> RangeRequest {
    RangeRequest {
        range: KeyRange::new(key(Vec::new()), key(vec![0xff, 0xff, 0xff]))
            .expect("bounded binary range"),
        direction,
        page_size,
        continuation: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pagination_public_api_reads_binary_keys_in_both_directions() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let binary_keys = [
        vec![0x00],
        vec![0x00, 0xff],
        vec![0x01],
        vec![0xff],
        vec![0xff, 0xff],
    ];
    let rows = binary_keys
        .iter()
        .cloned()
        .map(|bytes| (key(bytes), value(b"value")))
        .collect::<Vec<_>>();
    let seed_receipt = commit_puts(&store, &rows).await;

    assert_eq!(store.provider_name(), "sqlite");
    assert_eq!(store.limits().max_page_size, 1_000);
    let identity = store.identity().await.expect("public identity");
    assert_eq!(store.identity().await.expect("cloned identity"), identity);
    assert_eq!(
        store
            .resolve_commit(&seed_receipt.transaction_id)
            .await
            .expect("public commit resolution"),
        CommitResolution::Committed(seed_receipt)
    );

    let mut point_reader = store.begin_read().await.expect("begin public point read");
    assert_eq!(
        point_reader
            .get(&key(vec![0x00, 0xff]))
            .await
            .expect("public point get")
            .expect("binary point record")
            .value,
        value(b"value")
    );
    point_reader.abort().await.expect("abort point reader");

    let mut reader = store.begin_read().await.expect("begin forward read");
    let mut request = bounded_range(Direction::Forward, 2);
    let mut forward = Vec::new();
    let mut forward_page_sizes = Vec::new();
    let first_token = loop {
        let page = reader.range(&request).await.expect("forward range page");
        forward_page_sizes.push(page.records.len());
        forward.extend(
            page.records
                .iter()
                .map(|record| record.key.as_bytes().to_vec()),
        );
        let Some(token) = page.continuation else {
            break request
                .continuation
                .expect("first continuation was captured");
        };
        if request.continuation.is_none() {
            request.continuation = Some(token.clone());
        } else {
            request.continuation = Some(token);
        }
    };
    assert_eq!(forward_page_sizes, [2, 2, 1]);
    assert_eq!(forward, binary_keys);
    reader.abort().await.expect("abort forward reader");

    let mut mismatch_reader = store.begin_read().await.expect("begin mismatch reader");
    let wrong_direction = RangeRequest {
        direction: Direction::Reverse,
        continuation: Some(first_token.clone()),
        ..bounded_range(Direction::Forward, 2)
    };
    assert_eq!(
        mismatch_reader
            .range(&wrong_direction)
            .await
            .expect_err("continuation direction mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    let wrong_range = RangeRequest {
        range: KeyRange::new(key(vec![0x00]), key(vec![0xff, 0xff, 0xff]))
            .expect("different range"),
        direction: Direction::Forward,
        page_size: 2,
        continuation: Some(first_token),
    };
    assert_eq!(
        mismatch_reader
            .range(&wrong_range)
            .await
            .expect_err("continuation range mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    mismatch_reader
        .abort()
        .await
        .expect("abort mismatch reader");

    let mut reader = store.begin_read().await.expect("begin reverse read");
    let mut request = bounded_range(Direction::Reverse, 2);
    let mut reverse = Vec::new();
    let mut reverse_page_sizes = Vec::new();
    loop {
        let page = reader.range(&request).await.expect("reverse range page");
        reverse_page_sizes.push(page.records.len());
        reverse.extend(
            page.records
                .iter()
                .map(|record| record.key.as_bytes().to_vec()),
        );
        match page.continuation {
            Some(token) => request.continuation = Some(token),
            None => break,
        }
    }
    assert_eq!(reverse_page_sizes, [2, 2, 1]);
    assert_eq!(
        reverse,
        binary_keys.iter().rev().cloned().collect::<Vec<_>>()
    );
    reader.abort().await.expect("abort reverse reader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pagination_overlay_merge_refills_deleted_base_windows_and_freezes_writes() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let rows = [b'a', b'b', b'c', b'd', b'e']
        .into_iter()
        .map(|byte| (key(vec![byte]), value(b"base")))
        .collect::<Vec<_>>();
    commit_puts(&store, &rows).await;

    let mut forward = store
        .begin_write(transaction_id(), "forward overlay")
        .await
        .expect("begin forward overlay");
    for byte in [b'a', b'b'] {
        forward
            .delete(key(vec![byte]), Precondition::Any)
            .await
            .expect("stage forward delete");
    }
    forward
        .put(key(vec![b'f']), value(b"old"), Precondition::Any)
        .await
        .expect("stage first overlay put");
    forward
        .delete(key(vec![b'f']), Precondition::Any)
        .await
        .expect("stage overlay replacement delete");
    forward
        .put(key(vec![b'f']), value(b"final"), Precondition::Any)
        .await
        .expect("stage final overlay put");
    let mut request = RangeRequest {
        range: KeyRange::new(key(vec![b'a']), key(vec![b'z'])).expect("letter range"),
        direction: Direction::Forward,
        page_size: 2,
        continuation: None,
    };
    let page = forward.range(&request).await.expect("forward overlay page");
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.key.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        [vec![b'c'], vec![b'd']]
    );
    request.continuation = page.continuation.clone();
    assert!(request.continuation.is_some());
    assert_eq!(
        forward
            .put(key(vec![b'g']), value(b"late"), Precondition::Any)
            .await
            .expect_err("put after paginated range")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(
        forward
            .delete(key(vec![b'e']), Precondition::Any)
            .await
            .expect_err("delete after paginated range")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    let tail = forward.range(&request).await.expect("forward overlay tail");
    assert_eq!(
        tail.records
            .iter()
            .map(|record| record.key.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        [vec![b'e'], vec![b'f']]
    );
    assert_eq!(tail.records[1].value, value(b"final"));
    assert!(tail.continuation.is_none());
    forward.abort().await.expect("abort forward overlay");

    let mut reverse = store
        .begin_write(transaction_id(), "reverse overlay")
        .await
        .expect("begin reverse overlay");
    for byte in [b'e', b'd'] {
        reverse
            .delete(key(vec![byte]), Precondition::Any)
            .await
            .expect("stage reverse delete");
    }
    reverse
        .put(key(b"aa".to_vec()), value(b"old"), Precondition::Any)
        .await
        .expect("stage reverse overlay put");
    reverse
        .put(key(b"aa".to_vec()), value(b"final"), Precondition::Any)
        .await
        .expect("replace reverse overlay put");
    let mut request = RangeRequest {
        range: KeyRange::new(key(vec![b'a']), key(vec![b'z'])).expect("letter range"),
        direction: Direction::Reverse,
        page_size: 2,
        continuation: None,
    };
    let page = reverse.range(&request).await.expect("reverse overlay page");
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.key.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        [vec![b'c'], vec![b'b']]
    );
    assert!(page.continuation.is_some());
    request.continuation = page.continuation;
    let tail = reverse.range(&request).await.expect("reverse overlay tail");
    assert_eq!(
        tail.records
            .iter()
            .map(|record| record.key.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        [b"aa".to_vec(), vec![b'a']]
    );
    assert_eq!(tail.records[0].value, value(b"final"));
    assert!(tail.continuation.is_none());
    reverse.abort().await.expect("abort reverse overlay");

    let mut single_page = store
        .begin_write(transaction_id(), "single page range")
        .await
        .expect("begin single page write");
    let page = single_page
        .range(&RangeRequest {
            range: KeyRange::new(key(vec![b'a']), key(vec![b'z'])).expect("letter range"),
            direction: Direction::Forward,
            page_size: 100,
            continuation: None,
        })
        .await
        .expect("single range page");
    assert!(page.continuation.is_none());
    single_page
        .put(key(vec![b'f']), value(b"allowed"), Precondition::Any)
        .await
        .expect("single-page range must not freeze writes");
    single_page.abort().await.expect("abort single-page write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pagination_keeps_one_snapshot_across_pages() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    commit_puts(
        &store,
        &[
            (key(vec![b'a']), value(b"a")),
            (key(vec![b'c']), value(b"c")),
        ],
    )
    .await;

    let mut reader = store.begin_read().await.expect("begin paginated read");
    let mut request = RangeRequest {
        range: KeyRange::new(key(vec![b'a']), key(vec![b'z'])).expect("letter range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let first = reader.range(&request).await.expect("first page");
    assert_eq!(first.records[0].key.as_bytes(), b"a");
    request.continuation = first.continuation;
    commit_puts(&store, &[(key(vec![b'b']), value(b"new"))]).await;
    let second = reader.range(&request).await.expect("second snapshot page");
    assert_eq!(second.records[0].key.as_bytes(), b"c");
    assert!(second.continuation.is_none());
    reader.abort().await.expect("abort paginated reader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pagination_range_phantoms_have_exactly_one_winner() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let range = KeyRange::new(key(vec![b'a']), key(vec![b'z'])).expect("letter range");
    let barrier = Arc::new(Barrier::new(2));
    let mut insert_tasks = Vec::new();
    for byte in [b'b', b'c'] {
        let store = Arc::clone(&store);
        let range = range.clone();
        let barrier = Arc::clone(&barrier);
        insert_tasks.push(tokio::spawn(async move {
            let mut writer = store
                .begin_write(transaction_id(), "insert phantom")
                .await
                .expect("begin insert writer");
            assert!(
                writer
                    .range(&RangeRequest {
                        range,
                        direction: Direction::Forward,
                        page_size: 10,
                        continuation: None,
                    })
                    .await
                    .expect("establish insert snapshot")
                    .records
                    .is_empty()
            );
            barrier.wait().await;
            writer
                .put(key(vec![byte]), value(b"insert"), Precondition::Any)
                .await
                .expect("stage phantom insert");
            writer.commit().await
        }));
    }
    let insert_outcomes = futures::future::join_all(insert_tasks)
        .await
        .into_iter()
        .map(|result| result.expect("insert task"))
        .collect::<Vec<_>>();
    assert_one_committed_one_conflict(&insert_outcomes);

    commit_puts(
        &store,
        &[
            (key(vec![b'x']), value(b"x")),
            (key(vec![b'y']), value(b"y")),
        ],
    )
    .await;
    let barrier = Arc::new(Barrier::new(2));
    let mut delete_tasks = Vec::new();
    for byte in [b'x', b'y'] {
        let store = Arc::clone(&store);
        let range = range.clone();
        let barrier = Arc::clone(&barrier);
        delete_tasks.push(tokio::spawn(async move {
            let mut writer = store
                .begin_write(transaction_id(), "delete phantom")
                .await
                .expect("begin delete writer");
            assert!(
                writer
                    .range(&RangeRequest {
                        range,
                        direction: Direction::Forward,
                        page_size: 10,
                        continuation: None,
                    })
                    .await
                    .expect("establish delete snapshot")
                    .records
                    .len()
                    >= 2
            );
            barrier.wait().await;
            writer
                .delete(key(vec![byte]), Precondition::Any)
                .await
                .expect("stage phantom delete");
            writer.commit().await
        }));
    }
    let delete_outcomes = futures::future::join_all(delete_tasks)
        .await
        .into_iter()
        .map(|result| result.expect("delete task"))
        .collect::<Vec<_>>();
    assert_one_committed_one_conflict(&delete_outcomes);
}

fn assert_one_committed_one_conflict(outcomes: &[CommitOutcome]) {
    let committed = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, CommitOutcome::Committed(_)))
        .count();
    let conflicts = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, CommitOutcome::Conflict(_)))
        .count();
    assert_eq!((committed, conflicts), (1, 1), "outcomes: {outcomes:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pagination_change_cursor_spans_one_revision_without_gaps() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let identity = store.identity().await.expect("store identity");

    let baseline = store
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: 1_000,
        })
        .await
        .expect("empty baseline poll");
    assert!(baseline.hints.is_empty());
    assert!(!baseline.resync_required);
    let (baseline_revision, baseline_sequence) = baseline
        .next_cursor
        .decode(identity.store_id)
        .expect("decode baseline cursor");
    assert_eq!(baseline_revision, baseline.high_watermark);
    assert_eq!(baseline_sequence, u32::MAX);

    let mut writer = store
        .begin_write(transaction_id(), "large same-revision commit")
        .await
        .expect("begin large write");
    let mut expected_keys = HashSet::new();
    for number in 0_u32..2_005 {
        let bytes = number.to_be_bytes().to_vec();
        expected_keys.insert(bytes.clone());
        writer
            .put(key(bytes), value(b"v"), Precondition::Any)
            .await
            .expect("stage large write");
    }
    let receipt = match writer.commit().await {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("expected large commit, got {other:?}"),
    };

    let first = store
        .poll_changes(&ChangePollRequest {
            after: Some(baseline.next_cursor),
            page_size: 1_000,
        })
        .await
        .expect("first change page");
    let second = store
        .poll_changes(&ChangePollRequest {
            after: Some(first.next_cursor.clone()),
            page_size: 1_000,
        })
        .await
        .expect("second change page");
    let third = store
        .poll_changes(&ChangePollRequest {
            after: Some(second.next_cursor.clone()),
            page_size: 1_000,
        })
        .await
        .expect("third change page");
    assert_eq!(
        (first.hints.len(), second.hints.len(), third.hints.len()),
        (1_000, 1_000, 5)
    );
    assert_eq!(first.high_watermark, receipt.revision);
    assert_eq!(second.high_watermark, receipt.revision);
    assert_eq!(third.high_watermark, receipt.revision);
    assert!(!first.resync_required && !second.resync_required && !third.resync_required);

    let cursor_points = [&first.next_cursor, &second.next_cursor, &third.next_cursor]
        .into_iter()
        .map(|cursor| {
            cursor
                .decode(identity.store_id)
                .expect("decode page cursor")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        cursor_points
            .iter()
            .map(|(_, sequence)| *sequence)
            .collect::<Vec<_>>(),
        [999, 1_999, 2_004]
    );
    assert!(
        cursor_points
            .iter()
            .all(|(revision, _)| revision == &receipt.revision)
    );

    let actual_keys = first
        .hints
        .iter()
        .chain(&second.hints)
        .chain(&third.hints)
        .map(|hint| {
            assert_eq!(hint.revision, receipt.revision);
            hint.key.as_bytes().to_vec()
        })
        .collect::<HashSet<_>>();
    assert_eq!(actual_keys, expected_keys);

    let no_more = store
        .poll_changes(&ChangePollRequest {
            after: Some(third.next_cursor.clone()),
            page_size: 1_000,
        })
        .await
        .expect("empty tail poll");
    assert!(no_more.hints.is_empty());
    assert_eq!(no_more.next_cursor, third.next_cursor);

    for page_size in [0, 1_001] {
        assert_eq!(
            store
                .poll_changes(&ChangePollRequest {
                    after: None,
                    page_size,
                })
                .await
                .expect_err("invalid change page size")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }

    let other_temp = TempDir::new().expect("other temp dir");
    let other_store = open_store(&other_temp, "fe-b").await;
    let other_cursor = other_store
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: 1,
        })
        .await
        .expect("other store baseline")
        .next_cursor;
    assert_eq!(
        store
            .poll_changes(&ChangePollRequest {
                after: Some(other_cursor),
                page_size: 1,
            })
            .await
            .expect_err("change cursor from another store")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_change_poll_rejects_a_cursor_beyond_the_snapshot_high_watermark() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let identity = store.identity().await.expect("store identity");
    let baseline = store
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: 10,
        })
        .await
        .expect("baseline poll");
    let future_revision = StoreRevision::try_from(Bytes::copy_from_slice(&100_u64.to_be_bytes()))
        .expect("future revision token");
    let future_cursor = ChangeCursor::new(identity.store_id, future_revision, u32::MAX)
        .expect("future change cursor");

    assert_eq!(
        store
            .poll_changes(&ChangePollRequest {
                after: Some(future_cursor),
                page_size: 10,
            })
            .await
            .expect_err("future cursor must not swallow subsequent commits")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let item = key(b"future-cursor-visible".to_vec());
    let receipt = commit_puts(&store, &[(item.clone(), value(b"visible"))]).await;
    let changes = store
        .poll_changes(&ChangePollRequest {
            after: Some(baseline.next_cursor),
            page_size: 10,
        })
        .await
        .expect("poll from valid baseline");
    assert!(
        changes
            .hints
            .iter()
            .any(|hint| hint.key == item && hint.revision == receipt.revision)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_get_classifies_an_oversized_persisted_value_as_corruption() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let item = key(b"oversized-value".to_vec());
    let connection =
        Connection::open(temp.path().join("state-store.sqlite")).expect("open fixture database");
    connection
        .execute(
            "INSERT INTO state_store_kv(key, value, version) VALUES (?1, ?2, 1)",
            params![item.as_bytes(), vec![7_u8; MAX_VALUE_BYTES + 1]],
        )
        .expect("inject oversized persisted value");
    drop(connection);

    let mut reader = store.begin_read().await.expect("begin corrupt value read");
    assert_eq!(
        reader
            .get(&item)
            .await
            .expect_err("oversized persisted value")
            .kind(),
        StateStoreErrorKind::Corruption
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_get_classifies_a_malformed_persisted_version_as_corruption() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let item = key(b"malformed-version".to_vec());
    let connection =
        Connection::open(temp.path().join("state-store.sqlite")).expect("open fixture database");
    connection
        .execute(
            "INSERT INTO state_store_kv(key, value, version) VALUES (?1, ?2, ?3)",
            params![
                item.as_bytes(),
                b"value".as_slice(),
                b"not-an-integer".as_slice()
            ],
        )
        .expect("inject malformed persisted version");
    drop(connection);

    let mut reader = store
        .begin_read()
        .await
        .expect("begin corrupt version read");
    assert_eq!(
        reader
            .get(&item)
            .await
            .expect_err("malformed persisted version")
            .kind(),
        StateStoreErrorKind::Corruption
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_range_classifies_an_oversized_persisted_key_as_corruption() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let oversized_key = vec![b'm'; MAX_KEY_BYTES + 1];
    let connection =
        Connection::open(temp.path().join("state-store.sqlite")).expect("open fixture database");
    connection
        .execute(
            "INSERT INTO state_store_kv(key, value, version) VALUES (?1, ?2, 1)",
            params![oversized_key, b"value".as_slice()],
        )
        .expect("inject oversized persisted key");
    drop(connection);

    let mut reader = store.begin_read().await.expect("begin corrupt range read");
    let request = RangeRequest {
        range: KeyRange::new(key(b"m".to_vec()), key(b"n".to_vec())).expect("fixture range"),
        direction: Direction::Forward,
        page_size: 10,
        continuation: None,
    };
    assert_eq!(
        reader
            .range(&request)
            .await
            .expect_err("oversized persisted range key")
            .kind(),
        StateStoreErrorKind::Corruption
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_change_poll_classifies_an_oversized_persisted_key_as_corruption() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let oversized_key = vec![b'c'; MAX_KEY_BYTES + 1];
    let connection =
        Connection::open(temp.path().join("state-store.sqlite")).expect("open fixture database");
    let transaction_id = Uuid::now_v7();
    connection
        .execute(
            "INSERT INTO state_store_commits(transaction_id, revision, committed_at_ms) \
             VALUES (?1, 1, 0)",
            params![transaction_id.as_bytes()],
        )
        .expect("inject commit ledger row");
    connection
        .execute(
            "INSERT INTO state_store_changes(revision, sequence, key) VALUES (1, 0, ?1)",
            params![oversized_key],
        )
        .expect("inject oversized change key");
    connection
        .execute(
            "UPDATE state_store_meta SET value = ?1 WHERE key = ?2",
            params![
                1_u64.to_be_bytes().as_slice(),
                b"current_revision".as_slice()
            ],
        )
        .expect("advance fixture high watermark");
    drop(connection);

    assert_eq!(
        store
            .poll_changes(&ChangePollRequest {
                after: None,
                page_size: 10,
            })
            .await
            .expect_err("oversized persisted change key")
            .kind(),
        StateStoreErrorKind::Corruption
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pagination_change_cursor_reports_a_retention_gap_before_resuming() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let identity = store.identity().await.expect("store identity");
    let revision_one = commit_puts(
        &store,
        &(0_u8..5)
            .map(|number| (key(vec![b'a', number]), value(b"v1")))
            .collect::<Vec<_>>(),
    )
    .await
    .revision;

    let first = store
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: 5,
        })
        .await
        .expect("first fixture page");
    let (_, first_sequence) = first
        .next_cursor
        .decode(identity.store_id)
        .expect("decode first cursor");
    assert_eq!(first_sequence, 4);
    assert_eq!(first.high_watermark, revision_one);

    let revision_two = commit_puts(
        &store,
        &(0_u8..3)
            .map(|number| (key(vec![b'b', number]), value(b"v2")))
            .collect::<Vec<_>>(),
    )
    .await
    .revision;

    let mut connection = rusqlite::Connection::open(temp.path().join("state-store.sqlite"))
        .expect("open fixture database");
    let revision_i64 = i64::try_from(u64::from_be_bytes(
        revision_two
            .as_bytes()
            .try_into()
            .expect("SQLite revision encoding"),
    ))
    .expect("SQLite revision range");
    let mut floor = Vec::with_capacity(12);
    floor.extend_from_slice(
        &u64::try_from(revision_i64)
            .expect("positive revision")
            .to_be_bytes(),
    );
    floor.extend_from_slice(&1_u32.to_be_bytes());
    let transaction = connection.transaction().expect("begin retention fixture");
    assert_eq!(
        transaction
            .execute(
                "DELETE FROM state_store_changes \
                 WHERE revision = ?1 AND sequence <= ?2",
                params![revision_i64, 1_i64],
            )
            .expect("delete retained fixture rows"),
        2
    );
    assert_eq!(
        transaction
            .execute(
                "UPDATE state_store_meta SET value = ?1 WHERE key = ?2",
                params![floor, b"change_retention_floor".as_slice()],
            )
            .expect("advance retention floor"),
        1
    );
    transaction.commit().expect("commit retention fixture");
    drop(connection);

    let gap = store
        .poll_changes(&ChangePollRequest {
            after: Some(first.next_cursor),
            page_size: 5,
        })
        .await
        .expect("detect retention gap");
    assert!(gap.resync_required);
    assert!(gap.hints.is_empty());
    let (gap_revision, gap_sequence) = gap
        .next_cursor
        .decode(identity.store_id)
        .expect("decode gap floor");
    assert_eq!(gap_revision, revision_two);
    assert_eq!(gap_sequence, 1);
    assert_eq!(gap.high_watermark, revision_two);

    let resumed = store
        .poll_changes(&ChangePollRequest {
            after: Some(gap.next_cursor.clone()),
            page_size: 5,
        })
        .await
        .expect("resume after retention floor");
    assert!(!resumed.resync_required);
    assert_eq!(resumed.hints.len(), 1);
    assert_eq!(resumed.hints[0].key.as_bytes(), &[b'b', 2]);

    let mut connection = rusqlite::Connection::open(temp.path().join("state-store.sqlite"))
        .expect("reopen fixture database");
    let mut floor = Vec::with_capacity(12);
    floor.extend_from_slice(
        &u64::try_from(revision_i64)
            .expect("positive revision")
            .to_be_bytes(),
    );
    floor.extend_from_slice(&2_u32.to_be_bytes());
    let transaction = connection.transaction().expect("begin tail fixture");
    assert_eq!(
        transaction
            .execute(
                "DELETE FROM state_store_changes WHERE revision = ?1 AND sequence = ?2",
                params![revision_i64, 2_i64],
            )
            .expect("delete retained tail row"),
        1
    );
    assert_eq!(
        transaction
            .execute(
                "UPDATE state_store_meta SET value = ?1 WHERE key = ?2",
                params![floor, b"change_retention_floor".as_slice()],
            )
            .expect("advance tail retention floor"),
        1
    );
    transaction.commit().expect("commit tail retention fixture");
    drop(connection);

    let tail_gap = store
        .poll_changes(&ChangePollRequest {
            after: Some(gap.next_cursor),
            page_size: 5,
        })
        .await
        .expect("detect retained tail gap without survivors");
    assert!(tail_gap.resync_required);
    assert!(tail_gap.hints.is_empty());
    let (tail_revision, tail_sequence) = tail_gap
        .next_cursor
        .decode(identity.store_id)
        .expect("decode tail floor");
    assert_eq!((tail_revision, tail_sequence), (revision_two, 2));

    let empty_tail = store
        .poll_changes(&ChangePollRequest {
            after: Some(tail_gap.next_cursor.clone()),
            page_size: 5,
        })
        .await
        .expect("resume from empty retained tail");
    assert!(!empty_tail.resync_required);
    assert!(empty_tail.hints.is_empty());
    assert_eq!(empty_tail.next_cursor, tail_gap.next_cursor);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_metrics_are_driven_by_real_store_operations() {
    let temp = TempDir::new().expect("metrics temp dir");
    let store = open_store(&temp, "metrics-fe").await;
    let before = store.metrics_snapshot();

    let first = key(b"metrics/a".to_vec());
    let second = key(b"metrics/b".to_vec());
    let mut seed = store
        .begin_write(transaction_id(), "metrics seed")
        .await
        .expect("begin metrics seed");
    seed.put(first.clone(), value(b"one"), Precondition::Any)
        .await
        .expect("put first metrics row");
    seed.put(second.clone(), value(b"two"), Precondition::Any)
        .await
        .expect("put second metrics row");
    assert!(matches!(seed.commit().await, CommitOutcome::Committed(_)));

    let mut reader = store.begin_read().await.expect("begin metrics reader");
    assert!(reader.get(&first).await.expect("metrics get").is_some());
    let page = reader
        .range(&RangeRequest {
            range: KeyRange::new(key(b"metrics/".to_vec()), key(b"metrics0".to_vec()))
                .expect("metrics range"),
            direction: Direction::Forward,
            page_size: 10,
            continuation: None,
        })
        .await
        .expect("metrics range page");
    assert_eq!(page.records.len(), 2);
    reader.abort().await.expect("abort metrics reader");

    let mut update = store
        .begin_write(transaction_id(), "metrics update")
        .await
        .expect("begin metrics update");
    update
        .put(first.clone(), value(b"new"), Precondition::Any)
        .await
        .expect("update metrics row");
    update
        .delete(second, Precondition::Any)
        .await
        .expect("delete metrics row");
    assert!(matches!(update.commit().await, CommitOutcome::Committed(_)));

    let mut conflict_winner = store
        .begin_write(transaction_id(), "metrics conflict winner")
        .await
        .expect("begin metrics conflict winner");
    let mut conflict_loser = store
        .begin_write(transaction_id(), "metrics conflict loser")
        .await
        .expect("begin metrics conflict loser");
    conflict_winner
        .get(&first)
        .await
        .expect("establish winner snapshot");
    conflict_loser
        .get(&first)
        .await
        .expect("establish loser snapshot");
    conflict_winner
        .put(first.clone(), value(b"winner"), Precondition::Any)
        .await
        .expect("stage metrics conflict winner");
    conflict_loser
        .put(first.clone(), value(b"loser"), Precondition::Any)
        .await
        .expect("stage metrics conflict loser");
    assert!(matches!(
        conflict_winner.commit().await,
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(
        conflict_loser.commit().await,
        CommitOutcome::Conflict(_)
    ));

    let changes = store
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: 10,
        })
        .await
        .expect("poll metrics changes");
    assert_eq!(changes.hints.len(), 5);

    let snapshot = store.metrics_snapshot();
    assert!(snapshot.begin_count >= before.begin_count + 5);
    assert_eq!(snapshot.get_count, before.get_count + 3);
    assert_eq!(snapshot.range_count, before.range_count + 1);
    assert_eq!(snapshot.put_count, before.put_count + 5);
    assert_eq!(snapshot.delete_count, before.delete_count + 1);
    assert_eq!(snapshot.commit_count, before.commit_count + 4);
    for operation in [
        StateStoreOperation::Begin,
        StateStoreOperation::Get,
        StateStoreOperation::Range,
        StateStoreOperation::Put,
        StateStoreOperation::Delete,
        StateStoreOperation::Commit,
    ] {
        assert!(
            snapshot.operation_duration_observations(operation) > 0,
            "real {operation:?} operation must record duration"
        );
        assert!(
            snapshot.operation_outcome_count(operation, StateStoreOutcome::Success) > 0,
            "real {operation:?} operation must record its outcome"
        );
    }
    assert!(snapshot.bytes_read > before.bytes_read);
    assert!(snapshot.bytes_written > before.bytes_written);
    assert!(snapshot.page_records >= before.page_records + 7);
    assert!(snapshot.notification_lag_observations > before.notification_lag_observations);
    assert!(
        snapshot.operation_outcome_count(StateStoreOperation::Commit, StateStoreOutcome::Conflict,)
            > 0,
        "real provider conflicts must drive outcome metrics"
    );

    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("metrics/a"));
    assert!(!debug.contains("metrics/b"));
    assert!(!debug.contains("INSERT INTO"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_fail_fast_operations_are_observed_once_without_payload_metrics() {
    let temp = TempDir::new().expect("fail-fast metrics temp dir");
    let store = open_store_with_limits(
        &temp,
        "fail-fast-metrics-fe",
        StateStoreLimitOverrides {
            max_key_bytes: Some(4),
            max_transaction_bytes: Some(60),
            ..StateStoreLimitOverrides::default()
        },
    )
    .await;

    let mut reader = store.begin_read().await.expect("begin fail-fast reader");
    let before_get = store.metrics_snapshot();
    assert_eq!(
        reader
            .get(&key(b"oversized".to_vec()))
            .await
            .expect_err("oversized key must fail before provider I/O")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    let after_get = store.metrics_snapshot();
    assert_failed_operation_observed(&before_get, &after_get, StateStoreOperation::Get);
    assert_eq!(after_get.bytes_read, before_get.bytes_read);

    let forward = RangeRequest {
        range: KeyRange::new(key(b"a".to_vec()), key(b"z".to_vec())).expect("fail-fast range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let mismatched = RangeRequest {
        direction: Direction::Reverse,
        continuation: Some(
            forward
                .continuation_after(&key(b"m".to_vec()))
                .expect("forward continuation"),
        ),
        ..forward
    };
    let before_range = store.metrics_snapshot();
    assert_eq!(
        reader
            .range(&mismatched)
            .await
            .expect_err("mismatched continuation must fail before provider I/O")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    let after_range = store.metrics_snapshot();
    assert_failed_operation_observed(&before_range, &after_range, StateStoreOperation::Range);
    assert_eq!(after_range.bytes_read, before_range.bytes_read);
    assert_eq!(after_range.page_records, before_range.page_records);
    reader.abort().await.expect("abort fail-fast reader");

    let mut putter = store
        .begin_write(transaction_id(), "fail-fast put")
        .await
        .expect("begin fail-fast put");
    let before_put = store.metrics_snapshot();
    assert_eq!(
        putter
            .put(key(b"put1".to_vec()), value(b"x"), Precondition::Any)
            .await
            .expect_err("accounted put must exceed the transaction budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    let after_put = store.metrics_snapshot();
    assert_failed_operation_observed(&before_put, &after_put, StateStoreOperation::Put);
    assert_eq!(after_put.bytes_written, before_put.bytes_written);
    putter.abort().await.expect("abort fail-fast put");

    let mut deleter = store
        .begin_write(transaction_id(), "fail-fast delete")
        .await
        .expect("begin fail-fast delete");
    let before_delete = store.metrics_snapshot();
    assert_eq!(
        deleter
            .delete(key(b"del1".to_vec()), Precondition::Any)
            .await
            .expect_err("accounted delete must exceed the transaction budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    let after_delete = store.metrics_snapshot();
    assert_failed_operation_observed(&before_delete, &after_delete, StateStoreOperation::Delete);
    assert_eq!(after_delete.bytes_written, before_delete.bytes_written);
    deleter.abort().await.expect("abort fail-fast delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_accounting_counts_repeated_change_keys_before_io() {
    let temp = TempDir::new().expect("accounting temp dir");
    let store = open_store(&temp, "accounting-fe").await;
    let mut transaction = store
        .begin_write(transaction_id(), "exact accounting")
        .await
        .expect("begin exact accounting transaction");
    let mut rejected_at = None;
    for index in 0_u64..300 {
        let mut bytes = vec![b'k'; 8 * 1024];
        bytes[(8 * 1024 - 8)..].copy_from_slice(&index.to_be_bytes());
        let result = transaction
            .put(key(bytes), value(b""), Precondition::Any)
            .await;
        if let Err(error) = result {
            assert_eq!(error.kind(), StateStoreErrorKind::LimitExceeded);
            rejected_at = Some(index);
            break;
        }
    }
    assert!(
        rejected_at.is_some(),
        "full accounting must reject a transaction whose repeated 8 KiB keys fit the old mutation-only estimate but exceed 4 MiB once exact change rows are included"
    );

    let connection =
        Connection::open(temp.path().join("state-store.sqlite")).expect("open accounting observer");
    for table in [
        "state_store_kv",
        "state_store_changes",
        "state_store_commits",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count durable rows before abort");
        assert_eq!(count, 0, "limit rejection must happen before durable I/O");
    }
    drop(connection);
    transaction
        .abort()
        .await
        .expect("abort rejected accounting transaction");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_concurrent_first_open_has_exactly_one_schema_owner() {
    let temp = TempDir::new().expect("first-open temp dir");
    let path = temp.path().join("state-store.sqlite");
    let gate = Arc::new(Barrier::new(3));
    let mut opens = Vec::new();
    for _ in 0..2 {
        let path = path.clone();
        let gate = Arc::clone(&gate);
        opens.push(tokio::spawn(async move {
            gate.wait().await;
            let runtime = StateStoreRuntime::local().expect("create local state store runtime");
            open_state_store(
                &runtime,
                StateStoreConfig {
                    cluster_id: "race-cluster".to_owned(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path,
                        deployment_owner: "race-fe".to_owned(),
                    },
                },
                FeDeploymentView {
                    active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                    topology_revision: Bytes::from_static(b"race-topology"),
                },
            )
            .await
        }));
    }
    gate.wait().await;
    let first = opens.remove(0).await.expect("first open task");
    let second = opens.remove(0).await.expect("second open task");
    let (winner, loser) = match (first, second) {
        (Ok(store), Err(error)) | (Err(error), Ok(store)) => (store, error),
        (Ok(_), Ok(_)) => panic!("exactly one concurrent first open may own the path"),
        (Err(first), Err(second)) => panic!("one first open must succeed: {first}; {second}"),
    };
    assert_eq!(loser.kind(), StateStoreErrorKind::ProviderUnavailable);
    let identity = winner.identity().await.expect("winner identity");

    let connection = Connection::open(&path).expect("inspect raced database");
    let tables: HashSet<String> = connection
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'state_store_%'",
        )
        .expect("prepare table inventory")
        .query_map([], |row| row.get(0))
        .expect("query table inventory")
        .collect::<rusqlite::Result<HashSet<_>>>()
        .expect("collect table inventory");
    assert_eq!(
        tables,
        HashSet::from([
            "state_store_meta".to_owned(),
            "state_store_kv".to_owned(),
            "state_store_changes".to_owned(),
            "state_store_commits".to_owned(),
        ])
    );
    drop(connection);
    drop(winner);

    let runtime = StateStoreRuntime::local().expect("create local state store runtime");
    let reopened = open_state_store(
        &runtime,
        StateStoreConfig {
            cluster_id: "race-cluster".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Sqlite {
                path,
                deployment_owner: "race-fe".to_owned(),
            },
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).expect("one FE"),
            topology_revision: Bytes::from_static(b"race-topology-reopen"),
        },
    )
    .await
    .expect("winner schema must reopen cleanly");
    assert_eq!(
        reopened.identity().await.expect("reopened identity"),
        identity
    );
}
