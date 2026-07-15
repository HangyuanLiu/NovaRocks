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

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use novarocks::state_store::{
    ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution, Direction, FeDeploymentView,
    Key, KeyRange, Precondition, RangeRequest, StateStore, StateStoreConfig, StateStoreErrorKind,
    StateStoreLimitOverrides, StateStoreProviderConfig, TransactionId, Value, open_state_store,
};
use rusqlite::params;
use tempfile::TempDir;
use tokio::sync::Barrier;
use uuid::Uuid;

fn key(bytes: impl Into<Vec<u8>>) -> Key {
    Key::try_from(Bytes::from(bytes.into())).expect("valid key")
}

fn value(bytes: &'static [u8]) -> Value {
    Value::try_from(Bytes::from_static(bytes)).expect("valid value")
}

fn transaction_id() -> TransactionId {
    Uuid::now_v7().into()
}

async fn open_store(temp: &TempDir, owner: &str) -> Arc<dyn StateStore> {
    open_store_with_limits(temp, owner, StateStoreLimitOverrides::default()).await
}

async fn open_store_with_limits(
    temp: &TempDir,
    owner: &str,
    limits: StateStoreLimitOverrides,
) -> Arc<dyn StateStore> {
    open_state_store(
        StateStoreConfig {
            provider: StateStoreProviderConfig::Sqlite,
            path: temp.path().join("state-store.sqlite"),
            cluster_id: "cluster-a".to_owned(),
            deployment_owner: owner.to_owned(),
            limits,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).expect("one FE"),
            topology_revision: Bytes::from_static(b"topology-r1"),
        },
    )
    .await
    .expect("open public SQLite state store")
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
async fn sqlite_pagination_change_cursor_reports_a_retention_gap_before_resuming() {
    let temp = TempDir::new().expect("temp dir");
    let store = open_store(&temp, "fe-a").await;
    let identity = store.identity().await.expect("store identity");
    let mut writer = store
        .begin_write(transaction_id(), "retention gap fixture")
        .await
        .expect("begin fixture write");
    for number in 0_u32..12 {
        writer
            .put(
                key(number.to_be_bytes().to_vec()),
                value(b"v"),
                Precondition::Any,
            )
            .await
            .expect("stage fixture write");
    }
    let revision = match writer.commit().await {
        CommitOutcome::Committed(receipt) => receipt.revision,
        other => panic!("expected fixture commit, got {other:?}"),
    };

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

    let connection = rusqlite::Connection::open(temp.path().join("state-store.sqlite"))
        .expect("open fixture database");
    let revision_i64 = i64::try_from(u64::from_be_bytes(
        revision
            .as_bytes()
            .try_into()
            .expect("SQLite revision encoding"),
    ))
    .expect("SQLite revision range");
    assert_eq!(
        connection
            .execute(
                "DELETE FROM state_store_changes \
                 WHERE revision = ?1 AND sequence >= ?2 AND sequence <= ?3",
                params![revision_i64, 5_i64, 6_i64],
            )
            .expect("delete retained fixture rows"),
        2
    );
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
    assert_eq!(gap_revision, revision);
    assert_eq!(gap_sequence, 6);

    let resumed = store
        .poll_changes(&ChangePollRequest {
            after: Some(gap.next_cursor),
            page_size: 5,
        })
        .await
        .expect("resume after retention floor");
    assert!(!resumed.resync_required);
    assert_eq!(resumed.hints.len(), 5);
    assert_eq!(resumed.hints[0].key.as_bytes(), 7_u32.to_be_bytes());
}
