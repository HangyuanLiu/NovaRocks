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

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use novarocks::state_store::limits::MAX_KEY_BYTES;
use novarocks::state_store::{
    ChangePage, ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution, Direction, Key,
    KeyRange, Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
    StoreIdentity, TransactionId, Value, WriteTransaction,
};
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

pub type StoreFuture =
    Pin<Box<dyn Future<Output = Result<Arc<dyn StateStore>, StateStoreError>> + Send + 'static>>;
pub type StateStoreFactory = Arc<dyn Fn() -> StoreFuture + Send + Sync>;

pub async fn run_state_store_conformance(factory: StateStoreFactory) {
    snapshot_repeatable_read(&factory).await;
    same_key_conflict(&factory).await;
    write_skew_conflict(&factory).await;
    range_phantom_conflict(&factory).await;
    preconditions(&factory).await;
    forward_reverse_pages(&factory).await;
    same_revision_change_pages(&factory).await;
    notification_delivery_faults(&factory).await;
    atomic_commit(&factory).await;
    limits_before_io(&factory).await;
    arbitrary_binary_payloads(&factory).await;
}

fn key(bytes: impl Into<Vec<u8>>) -> Key {
    Key::try_from(Bytes::from(bytes.into())).expect("valid conformance key")
}

fn value(bytes: impl Into<Vec<u8>>) -> Value {
    Value::try_from(Bytes::from(bytes.into())).expect("valid conformance value")
}

fn transaction_id() -> TransactionId {
    Uuid::now_v7().into()
}

async fn open(factory: &StateStoreFactory) -> Arc<dyn StateStore> {
    factory().await.expect("open conformance state store")
}

async fn commit_puts(store: &Arc<dyn StateStore>, rows: &[(Key, Value)]) -> CommitReceipt {
    let id = transaction_id();
    let mut transaction = store
        .begin_write(id, "state store conformance seed")
        .await
        .expect("begin conformance seed");
    for (key, value) in rows {
        transaction
            .put(key.clone(), value.clone(), Precondition::Any)
            .await
            .expect("stage conformance seed");
    }
    committed(transaction.commit().await)
}

fn committed(outcome: CommitOutcome) -> CommitReceipt {
    match outcome {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("expected committed outcome, got {other:?}"),
    }
}

fn assert_conflict(outcome: CommitOutcome) {
    assert!(matches!(outcome, CommitOutcome::Conflict(_)), "{outcome:?}");
}

async fn read_record(store: &Arc<dyn StateStore>, item: &Key) -> Option<StateRecord> {
    let mut reader = store.begin_read().await.expect("begin conformance read");
    let record = reader.get(item).await.expect("read conformance record");
    reader.abort().await.expect("abort conformance read");
    record
}

pub async fn snapshot_repeatable_read(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"c01/snapshot".to_vec());
    commit_puts(&store, &[(item.clone(), value(b"before".to_vec()))]).await;
    let mut reader = store.begin_read().await.expect("begin snapshot read");
    let before = reader.get(&item).await.expect("first snapshot read");
    assert_eq!(
        before.as_ref().expect("initial snapshot value").value,
        value(b"before".to_vec())
    );
    commit_puts(&store, &[(item.clone(), value(b"after".to_vec()))]).await;
    assert_eq!(
        reader.get(&item).await.expect("repeat snapshot read"),
        before
    );
    reader.abort().await.expect("abort snapshot read");
}

pub async fn same_key_conflict(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"c02/same-key".to_vec());
    commit_puts(&store, &[(item.clone(), value(b"initial".to_vec()))]).await;
    let mut first = store
        .begin_write(transaction_id(), "same key first")
        .await
        .expect("begin first same-key write");
    let mut second = store
        .begin_write(transaction_id(), "same key second")
        .await
        .expect("begin second same-key write");
    first.get(&item).await.expect("establish first snapshot");
    second.get(&item).await.expect("establish second snapshot");
    first
        .put(item.clone(), value(b"first".to_vec()), Precondition::Any)
        .await
        .expect("stage first write");
    second
        .put(item, value(b"second".to_vec()), Precondition::Any)
        .await
        .expect("stage second write");
    committed(first.commit().await);
    assert_conflict(second.commit().await);
}

pub async fn write_skew_conflict(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let left = key(b"c03/left".to_vec());
    let right = key(b"c03/right".to_vec());
    commit_puts(
        &store,
        &[
            (left.clone(), value(b"on".to_vec())),
            (right.clone(), value(b"on".to_vec())),
        ],
    )
    .await;
    let mut first = store
        .begin_write(transaction_id(), "write skew first")
        .await
        .expect("begin first skew write");
    let mut second = store
        .begin_write(transaction_id(), "write skew second")
        .await
        .expect("begin second skew write");
    for item in [&left, &right] {
        first.get(item).await.expect("first skew read");
        second.get(item).await.expect("second skew read");
    }
    first
        .delete(left, Precondition::Any)
        .await
        .expect("stage first skew delete");
    second
        .delete(right, Precondition::Any)
        .await
        .expect("stage second skew delete");
    committed(first.commit().await);
    assert_conflict(second.commit().await);
}

fn conformance_range(prefix: u8, direction: Direction, page_size: usize) -> RangeRequest {
    RangeRequest {
        range: KeyRange::new(key(vec![prefix, 0]), key(vec![prefix, 0xff]))
            .expect("bounded conformance range"),
        direction,
        page_size,
        continuation: None,
    }
}

pub async fn range_phantom_conflict(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let request = conformance_range(4, Direction::Forward, 10);
    let mut first = store
        .begin_write(transaction_id(), "phantom first")
        .await
        .expect("begin first phantom write");
    let mut second = store
        .begin_write(transaction_id(), "phantom second")
        .await
        .expect("begin second phantom write");
    first.range(&request).await.expect("first phantom range");
    second.range(&request).await.expect("second phantom range");
    first
        .put(key(vec![4, 1]), value(b"first".to_vec()), Precondition::Any)
        .await
        .expect("stage first phantom");
    second
        .put(
            key(vec![4, 2]),
            value(b"second".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage second phantom");
    committed(first.commit().await);
    assert_conflict(second.commit().await);

    let deleted = key(vec![14, 1]);
    commit_puts(&store, &[(deleted.clone(), value(b"present".to_vec()))]).await;
    let delete_request = conformance_range(14, Direction::Forward, 10);
    let mut delete_first = store
        .begin_write(transaction_id(), "delete phantom first")
        .await
        .expect("begin first delete phantom write");
    let mut delete_second = store
        .begin_write(transaction_id(), "delete phantom second")
        .await
        .expect("begin second delete phantom write");
    delete_first
        .range(&delete_request)
        .await
        .expect("first delete phantom range");
    delete_second
        .range(&delete_request)
        .await
        .expect("second delete phantom range");
    delete_first
        .delete(deleted, Precondition::Any)
        .await
        .expect("stage phantom delete");
    delete_second
        .put(
            key(vec![14, 2]),
            value(b"insert".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage competing phantom insert");
    committed(delete_first.commit().await);
    assert_conflict(delete_second.commit().await);
}

pub async fn preconditions(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"c05/item".to_vec());
    let mut absent = store
        .begin_write(transaction_id(), "absent precondition")
        .await
        .expect("begin absent write");
    absent
        .put(item.clone(), value(b"v1".to_vec()), Precondition::Absent)
        .await
        .expect("stage absent write");
    committed(absent.commit().await);
    let original = read_record(&store, &item)
        .await
        .expect("precondition record");

    let mut present = store
        .begin_write(transaction_id(), "present precondition")
        .await
        .expect("begin present write");
    present
        .put(item.clone(), value(b"v2".to_vec()), Precondition::Present)
        .await
        .expect("stage present write");
    committed(present.commit().await);

    let mut versioned = store
        .begin_write(transaction_id(), "version precondition")
        .await
        .expect("begin version write");
    versioned
        .put(
            item.clone(),
            value(b"v3".to_vec()),
            Precondition::Version(
                read_record(&store, &item)
                    .await
                    .expect("current version")
                    .version,
            ),
        )
        .await
        .expect("stage version write");
    committed(versioned.commit().await);

    let mut stale = store
        .begin_write(transaction_id(), "stale precondition")
        .await
        .expect("begin stale write");
    stale
        .put(
            item.clone(),
            value(b"stale".to_vec()),
            Precondition::Version(original.version.clone()),
        )
        .await
        .expect("stage stale write");
    assert_conflict(stale.commit().await);

    let mut absent_failure = store
        .begin_write(transaction_id(), "absent precondition failure")
        .await
        .expect("begin absent failure");
    absent_failure
        .put(
            item.clone(),
            value(b"absent-failure".to_vec()),
            Precondition::Absent,
        )
        .await
        .expect("stage absent failure");
    assert_conflict(absent_failure.commit().await);

    let missing = key(b"c05/missing".to_vec());
    let mut present_failure = store
        .begin_write(transaction_id(), "present precondition failure")
        .await
        .expect("begin present failure");
    present_failure
        .put(
            missing.clone(),
            value(b"present-failure".to_vec()),
            Precondition::Present,
        )
        .await
        .expect("stage present failure");
    assert_conflict(present_failure.commit().await);

    let mut missing_version = store
        .begin_write(transaction_id(), "missing version failure")
        .await
        .expect("begin missing version failure");
    missing_version
        .delete(missing, Precondition::Version(original.version.clone()))
        .await
        .expect("stage missing version failure");
    assert_conflict(missing_version.commit().await);

    let mut any = store
        .begin_write(transaction_id(), "any precondition")
        .await
        .expect("begin any write");
    any.delete(item, Precondition::Any)
        .await
        .expect("stage any delete");
    committed(any.commit().await);
}

async fn collect_pages(store: &Arc<dyn StateStore>, mut request: RangeRequest) -> Vec<Vec<u8>> {
    let mut reader = store.begin_read().await.expect("begin paginated read");
    let mut keys = Vec::new();
    loop {
        let page = reader.range(&request).await.expect("read range page");
        keys.extend(
            page.records
                .iter()
                .map(|record| record.key.as_bytes().to_vec()),
        );
        let Some(continuation) = page.continuation else {
            break;
        };
        request.continuation = Some(continuation);
    }
    reader.abort().await.expect("abort paginated read");
    keys
}

pub async fn forward_reverse_pages(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let rows = (1_u8..=5)
        .map(|suffix| (key(vec![6, suffix]), value(vec![suffix])))
        .collect::<Vec<_>>();
    commit_puts(&store, &rows).await;
    let forward = collect_pages(&store, conformance_range(6, Direction::Forward, 2)).await;
    let reverse = collect_pages(&store, conformance_range(6, Direction::Reverse, 2)).await;
    let expected = rows
        .iter()
        .map(|(key, _)| key.as_bytes().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(forward, expected);
    assert_eq!(reverse, expected.into_iter().rev().collect::<Vec<_>>());

    let boundary_rows = [
        (key(vec![0]), value(vec![0])),
        (key(vec![0, 0xff]), value(vec![1])),
        (key(vec![0xff]), value(vec![2])),
        (key(vec![0xff, 0xff]), value(vec![3])),
    ];
    commit_puts(&store, &boundary_rows).await;
    let boundary_request = RangeRequest {
        range: KeyRange::new(key(Vec::new()), key(vec![0xff, 0xff, 0xff]))
            .expect("bounded binary edge range"),
        direction: Direction::Forward,
        page_size: 2,
        continuation: None,
    };
    let mut reader = store.begin_read().await.expect("begin token read");
    let first = reader
        .range(&boundary_request)
        .await
        .expect("first binary edge page");
    let token = first.continuation.expect("binary edge continuation");
    assert_eq!(first.records.len(), 2);
    let wrong_direction = RangeRequest {
        direction: Direction::Reverse,
        continuation: Some(token.clone()),
        ..boundary_request.clone()
    };
    assert_eq!(
        reader
            .range(&wrong_direction)
            .await
            .expect_err("token direction mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    let wrong_range = RangeRequest {
        range: KeyRange::new(key(vec![0]), key(vec![0xff, 0xff, 0xff]))
            .expect("different token range"),
        continuation: Some(token),
        ..boundary_request.clone()
    };
    assert_eq!(
        reader
            .range(&wrong_range)
            .await
            .expect_err("token range mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    reader.abort().await.expect("abort token read");

    let mut writer = store
        .begin_write(transaction_id(), "write range freeze")
        .await
        .expect("begin write range freeze");
    assert!(
        writer
            .range(&boundary_request)
            .await
            .expect("paginated write range")
            .continuation
            .is_some()
    );
    assert_eq!(
        writer
            .put(key(vec![1]), value(vec![1]), Precondition::Any)
            .await
            .expect_err("write must freeze after continuation")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    writer.abort().await.expect("abort frozen writer");
}

pub async fn same_revision_change_pages(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let mut baseline = None;
    loop {
        let page = store
            .poll_changes(&ChangePollRequest {
                after: baseline,
                page_size: store.limits().max_page_size,
            })
            .await
            .expect("drain change baseline");
        baseline = Some(page.next_cursor);
        if page.hints.len() < store.limits().max_page_size {
            break;
        }
    }
    let rows = (1_u8..=5)
        .map(|suffix| (key(vec![7, suffix]), value(vec![suffix])))
        .collect::<Vec<_>>();
    let receipt = commit_puts(&store, &rows).await;
    let first = store
        .poll_changes(&ChangePollRequest {
            after: baseline,
            page_size: 2,
        })
        .await
        .expect("poll first same-revision page");
    assert_eq!(first.hints.len(), 2);
    let second = store
        .poll_changes(&ChangePollRequest {
            after: Some(first.next_cursor),
            page_size: 2,
        })
        .await
        .expect("poll second same-revision page");
    assert_eq!(second.hints.len(), 2);
    let third = store
        .poll_changes(&ChangePollRequest {
            after: Some(second.next_cursor),
            page_size: 2,
        })
        .await
        .expect("poll final same-revision page");
    assert_eq!(third.hints.len(), 1);
    let tail_cursor = third.next_cursor.clone();
    let high_watermark = third.high_watermark.clone();
    let hints = first
        .hints
        .into_iter()
        .chain(second.hints)
        .chain(third.hints)
        .collect::<Vec<_>>();
    assert!(hints.iter().all(|hint| hint.revision == receipt.revision));
    assert_eq!(
        hints
            .into_iter()
            .map(|hint| hint.key.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        rows.iter()
            .map(|(key, _)| key.as_bytes().to_vec())
            .collect::<Vec<_>>()
    );

    let fault = FaultInjectingStateStore::new(Arc::clone(&store));
    fault.script_next_change_page(ChangePage {
        hints: Vec::new(),
        next_cursor: tail_cursor.clone(),
        high_watermark,
        resync_required: true,
    });
    let gap = fault
        .poll_changes(&ChangePollRequest {
            after: Some(tail_cursor),
            page_size: 2,
        })
        .await
        .expect("inject retention gap");
    assert!(gap.resync_required);
    assert!(gap.hints.is_empty());
    let authoritative = collect_pages(
        &(fault as Arc<dyn StateStore>),
        conformance_range(7, Direction::Forward, 2),
    )
    .await;
    assert_eq!(
        authoritative,
        rows.iter()
            .map(|(key, _)| key.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        "retention gaps require a bounded authoritative reload"
    );
}

#[derive(Default)]
struct AuthoritativeNotificationConsumer {
    records: BTreeMap<Vec<u8>, Vec<u8>>,
    authoritative_reads: usize,
    reload_pages: usize,
}

impl AuthoritativeNotificationConsumer {
    async fn consume(
        &mut self,
        store: &Arc<dyn StateStore>,
        page: ChangePage,
        mut reload_request: RangeRequest,
    ) {
        if page.resync_required {
            let mut reader = store.begin_read().await.expect("begin notification resync");
            let mut reloaded = BTreeMap::new();
            loop {
                let page = reader
                    .range(&reload_request)
                    .await
                    .expect("read bounded notification resync page");
                self.reload_pages += 1;
                for record in page.records {
                    reloaded.insert(
                        record.key.as_bytes().to_vec(),
                        record.value.as_bytes().to_vec(),
                    );
                }
                let Some(continuation) = page.continuation else {
                    break;
                };
                reload_request.continuation = Some(continuation);
            }
            reader.abort().await.expect("abort notification resync");
            self.records = reloaded;
            return;
        }

        let mut seen = HashSet::new();
        for hint in page.hints {
            let identity = (
                hint.revision.as_bytes().to_vec(),
                hint.key.as_bytes().to_vec(),
            );
            if !seen.insert(identity) {
                continue;
            }
            self.authoritative_reads += 1;
            match read_record(store, &hint.key).await {
                Some(record) => {
                    self.records.insert(
                        record.key.as_bytes().to_vec(),
                        record.value.as_bytes().to_vec(),
                    );
                }
                None => {
                    self.records.remove(hint.key.as_bytes());
                }
            }
        }
    }
}

pub async fn notification_delivery_faults(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let mut baseline = None;
    loop {
        let page = store
            .poll_changes(&ChangePollRequest {
                after: baseline,
                page_size: store.limits().max_page_size,
            })
            .await
            .expect("drain notification baseline");
        baseline = Some(page.next_cursor);
        if page.hints.len() < store.limits().max_page_size {
            break;
        }
    }

    let rows = (1_u8..=3)
        .map(|suffix| (key(vec![17, suffix]), value(vec![suffix])))
        .collect::<Vec<_>>();
    commit_puts(&store, &rows).await;
    let original = store
        .poll_changes(&ChangePollRequest {
            after: baseline.clone(),
            page_size: store.limits().max_page_size,
        })
        .await
        .expect("poll original notification page");
    let original_hint = original.hints[0].clone();

    let updated_value = value(b"latest-after-delay".to_vec());
    let delayed_receipt = commit_puts(
        &store,
        &[(original_hint.key.clone(), updated_value.clone())],
    )
    .await;
    assert_ne!(original_hint.revision, delayed_receipt.revision);

    let loss_key = key(vec![17, 4]);
    commit_puts(&store, &[(loss_key.clone(), value(vec![4]))]).await;
    let loss_page = store
        .poll_changes(&ChangePollRequest {
            after: Some(original.next_cursor.clone()),
            page_size: store.limits().max_page_size,
        })
        .await
        .expect("poll page to be replaced by loss signal");

    let duplicate_page = ChangePage {
        hints: vec![original_hint.clone(), original_hint.clone()],
        next_cursor: original.next_cursor.clone(),
        high_watermark: original.high_watermark.clone(),
        resync_required: false,
    };
    let delayed_page = ChangePage {
        hints: vec![original_hint],
        next_cursor: original.next_cursor,
        high_watermark: delayed_receipt.revision,
        resync_required: false,
    };
    let resync_page = ChangePage {
        hints: Vec::new(),
        next_cursor: loss_page.next_cursor,
        high_watermark: loss_page.high_watermark,
        resync_required: true,
    };

    let fault = FaultInjectingStateStore::new(Arc::clone(&store));
    fault.script_change_pages(vec![duplicate_page, delayed_page, resync_page]);
    let consumer_store: Arc<dyn StateStore> = fault.clone();
    let reload_request = conformance_range(17, Direction::Forward, 2);
    let mut consumer = AuthoritativeNotificationConsumer::default();

    let duplicate = fault
        .poll_changes(&ChangePollRequest {
            after: baseline,
            page_size: 2,
        })
        .await
        .expect("inject duplicate notifications");
    assert_eq!(duplicate.hints.len(), 2);
    consumer
        .consume(&consumer_store, duplicate, reload_request.clone())
        .await;
    assert_eq!(
        consumer.authoritative_reads, 1,
        "duplicate hints deduplicate"
    );

    let delayed = fault
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: 2,
        })
        .await
        .expect("inject delayed notification");
    consumer
        .consume(&consumer_store, delayed, reload_request.clone())
        .await;
    assert_eq!(
        consumer.records.get(original.hints[0].key.as_bytes()),
        Some(&updated_value.as_bytes().to_vec()),
        "delayed hints trigger an authoritative read instead of replaying stale payload"
    );

    let loss = fault
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: 2,
        })
        .await
        .expect("inject notification loss signal");
    assert!(loss.resync_required);
    consumer
        .consume(&consumer_store, loss, reload_request)
        .await;
    assert_eq!(consumer.reload_pages, 2, "resync reload stays paginated");
    assert_eq!(consumer.records.len(), 4);
    assert_eq!(consumer.records.get(loss_key.as_bytes()), Some(&vec![4]));
}

pub async fn atomic_commit(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let guard = key(b"c08/guard".to_vec());
    let partial = key(b"c08/partial".to_vec());
    commit_puts(&store, &[(guard.clone(), value(b"original".to_vec()))]).await;
    let stale = read_record(&store, &guard).await.expect("guard version");
    commit_puts(&store, &[(guard.clone(), value(b"new".to_vec()))]).await;
    let mut transaction = store
        .begin_write(transaction_id(), "atomic conflict")
        .await
        .expect("begin atomic conflict");
    transaction
        .put(
            partial.clone(),
            value(b"must-not-commit".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage partial row");
    transaction
        .put(
            guard,
            value(b"stale".to_vec()),
            Precondition::Version(stale.version),
        )
        .await
        .expect("stage conflicting row");
    assert_conflict(transaction.commit().await);
    assert_eq!(read_record(&store, &partial).await, None);

    let mut baseline_cursor = None;
    loop {
        let page = store
            .poll_changes(&ChangePollRequest {
                after: baseline_cursor,
                page_size: store.limits().max_page_size,
            })
            .await
            .expect("poll scripted commit baseline");
        let page_is_full = page.hints.len() == store.limits().max_page_size;
        baseline_cursor = Some(page.next_cursor);
        if !page_is_full {
            break;
        }
    }
    let fault = FaultInjectingStateStore::new(Arc::clone(&store));
    let scripted_id = transaction_id();
    let scripted_key = key(b"c08/scripted-committed".to_vec());
    let mut scripted = fault
        .begin_write(scripted_id, "scripted real commit")
        .await
        .expect("begin scripted committed transaction");
    scripted
        .put(
            scripted_key.clone(),
            value(b"durable".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage scripted committed row");
    fault.script_next_pre_commit(ScriptedCommitResult::Committed);
    let scripted_receipt = committed(scripted.commit().await);
    assert_eq!(
        fault
            .resolve_commit(&scripted_id)
            .await
            .expect("resolve scripted committed transaction"),
        CommitResolution::Committed(scripted_receipt.clone())
    );
    assert_eq!(
        read_record(&store, &scripted_key)
            .await
            .expect("scripted committed row must be durable")
            .value,
        value(b"durable".to_vec())
    );
    let change = store
        .poll_changes(&ChangePollRequest {
            after: baseline_cursor,
            page_size: store.limits().max_page_size,
        })
        .await
        .expect("poll scripted committed change");
    assert!(
        change
            .hints
            .iter()
            .any(|hint| { hint.key == scripted_key && hint.revision == scripted_receipt.revision })
    );

    let failure_cursor = Some(change.next_cursor);
    let mut failure_keys = Vec::new();
    for (suffix, result) in [
        ("conflict", ScriptedCommitResult::Conflict),
        (
            "transient-before-commit",
            ScriptedCommitResult::TransientBeforeCommit,
        ),
        ("definite-failure", ScriptedCommitResult::DefiniteFailure),
        ("commit-unknown", ScriptedCommitResult::CommitUnknown),
    ] {
        let transaction_id = transaction_id();
        let item = key(format!("c08/scripted-{suffix}").into_bytes());
        let mut transaction = fault
            .begin_write(transaction_id, "scripted failure must abort")
            .await
            .expect("begin scripted failure transaction");
        transaction
            .put(
                item.clone(),
                value(b"must-not-commit".to_vec()),
                Precondition::Any,
            )
            .await
            .expect("stage scripted failure row");
        fault.script_next_pre_commit(result);
        let outcome = transaction.commit().await;
        assert!(
            matches!(
                (result, outcome),
                (ScriptedCommitResult::Conflict, CommitOutcome::Conflict(_))
                    | (
                        ScriptedCommitResult::TransientBeforeCommit,
                        CommitOutcome::TransientBeforeCommit(_)
                    )
                    | (
                        ScriptedCommitResult::DefiniteFailure,
                        CommitOutcome::DefiniteFailure(_)
                    )
                    | (
                        ScriptedCommitResult::CommitUnknown,
                        CommitOutcome::CommitUnknown(_)
                    )
            ),
            "unexpected scripted failure outcome"
        );
        assert_eq!(
            fault
                .resolve_commit(&transaction_id)
                .await
                .expect("resolve scripted failure transaction"),
            CommitResolution::NotCommitted
        );
        assert_eq!(read_record(&store, &item).await, None);
        failure_keys.push(item);
    }
    let failure_changes = store
        .poll_changes(&ChangePollRequest {
            after: failure_cursor,
            page_size: store.limits().max_page_size,
        })
        .await
        .expect("poll scripted failure changes");
    assert!(
        failure_changes
            .hints
            .iter()
            .all(|hint| !failure_keys.contains(&hint.key)),
        "scripted failure outcomes must not publish change hints"
    );
}

#[derive(Clone, Copy, Debug)]
pub enum ScriptedCommitResult {
    Committed,
    Conflict,
    TransientBeforeCommit,
    DefiniteFailure,
    CommitUnknown,
}

#[derive(Clone)]
pub struct FaultGate {
    reached: watch::Sender<bool>,
    armed: watch::Sender<bool>,
    cancelled: watch::Sender<bool>,
    inner_dropped: watch::Sender<bool>,
    release: watch::Sender<bool>,
}

impl FaultGate {
    pub fn new() -> Self {
        let (reached, _) = watch::channel(false);
        let (armed, _) = watch::channel(false);
        let (cancelled, _) = watch::channel(false);
        let (inner_dropped, _) = watch::channel(false);
        let (release, _) = watch::channel(false);
        Self {
            reached,
            armed,
            cancelled,
            inner_dropped,
            release,
        }
    }

    async fn pause(&self) {
        self.reached.send_replace(true);
        let mut release = self.release.subscribe();
        self.armed.send_replace(true);
        release
            .wait_for(|released| *released)
            .await
            .expect("fault gate release sender");
    }

    pub async fn wait_reached(&self) {
        let mut reached = self.reached.subscribe();
        reached
            .wait_for(|reached| *reached)
            .await
            .expect("fault gate reached sender");
    }

    pub async fn wait_armed(&self) {
        let mut armed = self.armed.subscribe();
        armed
            .wait_for(|armed| *armed)
            .await
            .expect("fault gate armed sender");
    }

    pub async fn wait_cancelled(&self) {
        let mut cancelled = self.cancelled.subscribe();
        cancelled
            .wait_for(|cancelled| *cancelled)
            .await
            .expect("fault gate cancellation sender");
    }

    fn publish_cancelled(&self) {
        self.cancelled.send_replace(true);
    }

    fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    fn publish_inner_dropped(&self) {
        self.inner_dropped.send_replace(true);
    }

    pub async fn wait_inner_dropped(&self) {
        let mut inner_dropped = self.inner_dropped.subscribe();
        inner_dropped
            .wait_for(|inner_dropped| *inner_dropped)
            .await
            .expect("fault gate inner-drop sender");
    }

    pub async fn release(&self) {
        self.release.send_replace(true);
    }
}

impl Default for FaultGate {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct FaultScript {
    begin: Option<StateStoreError>,
    operation: Option<StateStoreError>,
    pre_commit: Option<ScriptedCommitResult>,
    post_dispatch: Option<FaultGate>,
    change_poll: Option<StateStoreError>,
    change_pages: VecDeque<ChangePage>,
}

pub struct FaultInjectingStateStore {
    inner: Arc<dyn StateStore>,
    script: Arc<Mutex<FaultScript>>,
}

impl FaultInjectingStateStore {
    pub fn new(inner: Arc<dyn StateStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            script: Arc::new(Mutex::new(FaultScript::default())),
        })
    }

    pub fn fail_next_begin(&self, error: StateStoreError) {
        self.script.lock().expect("fault script").begin = Some(error);
    }

    pub fn fail_next_operation(&self, error: StateStoreError) {
        self.script.lock().expect("fault script").operation = Some(error);
    }

    pub fn script_next_pre_commit(&self, result: ScriptedCommitResult) {
        self.script.lock().expect("fault script").pre_commit = Some(result);
    }

    pub fn pause_next_post_dispatch(&self, gate: FaultGate) {
        self.script.lock().expect("fault script").post_dispatch = Some(gate);
    }

    pub fn fail_next_change_poll(&self, error: StateStoreError) {
        self.script.lock().expect("fault script").change_poll = Some(error);
    }

    pub fn script_next_change_page(&self, page: ChangePage) {
        self.script
            .lock()
            .expect("fault script")
            .change_pages
            .push_back(page);
    }

    pub fn script_change_pages(&self, pages: impl IntoIterator<Item = ChangePage>) {
        self.script
            .lock()
            .expect("fault script")
            .change_pages
            .extend(pages);
    }

    fn take_begin_error(&self) -> Option<StateStoreError> {
        self.script.lock().expect("fault script").begin.take()
    }
}

struct FaultReadTransaction {
    inner: Box<dyn ReadTransaction>,
    script: Arc<Mutex<FaultScript>>,
}

struct FaultWriteTransaction {
    inner: Box<dyn WriteTransaction>,
    script: Arc<Mutex<FaultScript>>,
}

fn take_operation_error(script: &Mutex<FaultScript>) -> Option<StateStoreError> {
    script.lock().expect("fault script").operation.take()
}

#[async_trait]
impl ReadTransaction for FaultReadTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.abort().await
    }
}

#[async_trait]
impl ReadTransaction for FaultWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.abort().await
    }
}

fn scripted_failure(result: ScriptedCommitResult) -> CommitOutcome {
    let error =
        || StateStoreError::new(StateStoreErrorKind::Internal, "injected state store fault");
    match result {
        ScriptedCommitResult::Committed => unreachable!("committed faults use the real provider"),
        ScriptedCommitResult::Conflict => CommitOutcome::Conflict(error()),
        ScriptedCommitResult::TransientBeforeCommit => {
            CommitOutcome::TransientBeforeCommit(error())
        }
        ScriptedCommitResult::DefiniteFailure => CommitOutcome::DefiniteFailure(error()),
        ScriptedCommitResult::CommitUnknown => CommitOutcome::CommitUnknown(error()),
    }
}

struct FaultWaiterCancellation {
    gate: FaultGate,
    armed: bool,
}

impl FaultWaiterCancellation {
    fn new(gate: FaultGate) -> Self {
        Self { gate, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for FaultWaiterCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.gate.publish_cancelled();
        }
    }
}

#[async_trait]
impl WriteTransaction for FaultWriteTransaction {
    fn transaction_id(&self) -> &TransactionId {
        self.inner.transaction_id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.delete(key, precondition).await
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        let (pre_commit, post_dispatch) = {
            let mut script = self.script.lock().expect("fault script");
            (script.pre_commit.take(), script.post_dispatch.take())
        };
        if let Some(result) = pre_commit {
            if matches!(result, ScriptedCommitResult::Committed) {
                return self.inner.commit().await;
            }
            return match self.inner.abort().await {
                Ok(()) => scripted_failure(result),
                Err(error) => CommitOutcome::DefiniteFailure(error),
            };
        }
        let Some(gate) = post_dispatch else {
            return self.inner.commit().await;
        };
        let (outcome_tx, outcome_rx) = oneshot::channel();
        let supervisor_gate = gate.clone();
        tokio::spawn(async move {
            let mut commit = self.inner.commit();
            let mut ready = None;
            std::future::poll_fn(|context| {
                match commit.as_mut().poll(context) {
                    Poll::Ready(outcome) => ready = Some(outcome),
                    Poll::Pending => {}
                }
                Poll::Ready(())
            })
            .await;
            supervisor_gate.pause().await;
            if supervisor_gate.is_cancelled() {
                drop(commit);
                supervisor_gate.publish_inner_dropped();
                return;
            }
            let outcome = match ready {
                Some(outcome) => outcome,
                None => commit.await,
            };
            let _ = outcome_tx.send(outcome);
        });
        let mut cancellation = FaultWaiterCancellation::new(gate);
        let outcome = outcome_rx.await.expect("fault commit supervisor");
        cancellation.disarm();
        outcome
    }
}

#[async_trait]
impl StateStore for FaultInjectingStateStore {
    fn provider_name(&self) -> &'static str {
        self.inner.provider_name()
    }

    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        if let Some(error) = self.take_begin_error() {
            return Err(error);
        }
        Ok(Box::new(FaultReadTransaction {
            inner: self.inner.begin_read().await?,
            script: Arc::clone(&self.script),
        }))
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        if let Some(error) = self.take_begin_error() {
            return Err(error);
        }
        Ok(Box::new(FaultWriteTransaction {
            inner: self.inner.begin_write(transaction_id, purpose).await?,
            script: Arc::clone(&self.script),
        }))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        let (error, page) = {
            let mut script = self.script.lock().expect("fault script");
            (script.change_poll.take(), script.change_pages.pop_front())
        };
        if let Some(error) = error {
            return Err(error);
        }
        if let Some(page) = page {
            return Ok(page);
        }
        self.inner.poll_changes(request).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        self.inner.resolve_commit(transaction_id).await
    }
}

pub async fn limits_before_io(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let limits = store.limits().clone();
    assert!(limits.max_key_bytes < MAX_KEY_BYTES);
    let oversized = key(vec![11; limits.max_key_bytes + 1]);
    let visible = key(b"c11/visible".to_vec());
    let mut reader = store.begin_read().await.expect("begin limited read");
    assert_eq!(
        reader
            .get(&oversized)
            .await
            .expect_err("reject oversized get")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    commit_puts(&store, &[(visible.clone(), value(b"new".to_vec()))]).await;
    assert!(
        reader
            .get(&visible)
            .await
            .expect("valid get after limit rejection")
            .is_some()
    );
    reader.abort().await.expect("abort limited read");

    let mut page_reader = store.begin_read().await.expect("begin page-limit read");
    assert_eq!(
        page_reader
            .range(&RangeRequest {
                page_size: limits.max_page_size + 1,
                ..conformance_range(11, Direction::Forward, 1)
            })
            .await
            .expect_err("reject oversized page")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    let page_visible = key(b"c11/page-visible".to_vec());
    commit_puts(&store, &[(page_visible.clone(), value(b"new".to_vec()))]).await;
    assert!(
        page_reader
            .get(&page_visible)
            .await
            .expect("valid get after page rejection")
            .is_some()
    );
    page_reader.abort().await.expect("abort page-limit read");

    let mut value_writer = store
        .begin_write(transaction_id(), "value budget")
        .await
        .expect("begin value-limit write");
    assert_eq!(
        value_writer
            .put(
                key(b"c11/value".to_vec()),
                value(vec![0; limits.max_value_bytes + 1]),
                Precondition::Any,
            )
            .await
            .expect_err("reject oversized value")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    value_writer.abort().await.expect("abort value-limit write");

    let mut writer = store
        .begin_write(transaction_id(), "operation budget")
        .await
        .expect("begin budget write");
    for index in 0..limits.max_transaction_operations {
        writer
            .put(
                key(vec![11, (index >> 8) as u8, index as u8]),
                value(b"v".to_vec()),
                Precondition::Any,
            )
            .await
            .expect("stage operation within budget");
    }
    assert_eq!(
        writer
            .put(
                key(vec![11, 0xff, 0xff]),
                value(b"v".to_vec()),
                Precondition::Any
            )
            .await
            .expect_err("reject operation over budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    committed(writer.commit().await);

    let mut byte_writer = store
        .begin_write(transaction_id(), "byte budget")
        .await
        .expect("begin byte-budget write");
    for suffix in 1_u8..=4 {
        byte_writer
            .put(
                key(vec![11, 0xfe, suffix]),
                value(vec![suffix; limits.max_value_bytes]),
                Precondition::Any,
            )
            .await
            .expect("stage mutation within byte budget");
    }
    assert_eq!(
        byte_writer
            .put(
                key(vec![11, 0xfe, 5]),
                value(vec![5; limits.max_value_bytes]),
                Precondition::Any,
            )
            .await
            .expect_err("reject transaction over byte budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    committed(byte_writer.commit().await);
}

pub async fn arbitrary_binary_payloads(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(vec![13, 0, 0xff, 0, 0xfe]);
    let payload = value(vec![0xff, 0, 0xfe, 0, 0xfd]);
    commit_puts(&store, &[(item.clone(), payload.clone())]).await;
    let record = read_record(&store, &item)
        .await
        .expect("read arbitrary binary row");
    assert_eq!(record.key, item);
    assert_eq!(record.value, payload);
    assert!(!record.version.as_bytes().is_empty());
}
