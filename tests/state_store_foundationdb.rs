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

#![cfg(feature = "foundationdb-provider")]

use std::num::NonZeroUsize;
use std::path::PathBuf;

use bytes::Bytes;
use foundationdb::Database;
use foundationdb::options::TransactionOption;
use novarocks::state_store::{
    CommitOutcome, Direction, FeDeploymentView, FoundationDbClientConfig, Key, KeyRange,
    Precondition, RangeRequest, StateStore, StateStoreConfig, StateStoreErrorKind,
    StateStoreLimitOverrides, StateStoreProviderConfig, StateStoreRuntime, TransactionId, Value,
    open_state_store,
};
use uuid::Uuid;

fn client_config() -> FoundationDbClientConfig {
    FoundationDbClientConfig {
        disable_multi_version_client: true,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_verify_peers: None,
        tls_password_env: None,
    }
}

fn cluster_file() -> PathBuf {
    PathBuf::from(
        std::env::var("NOVAROCKS_FDB_CLUSTER_FILE").expect("FoundationDB fixture cluster file"),
    )
}

fn store_config(cluster_id: &str, keyspace_id: Uuid) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: cluster_id.to_owned(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Foundationdb {
            cluster_file: cluster_file(),
            keyspace_id,
        },
    }
}

fn transaction_store_config(cluster_id: &str, keyspace_id: Uuid) -> StateStoreConfig {
    let mut config = store_config(cluster_id, keyspace_id);
    config.limits.max_transaction_bytes = Some(16 * 1024);
    config
}

fn deployment() -> FeDeploymentView {
    FeDeploymentView {
        active_fe_count: NonZeroUsize::new(2).expect("non-zero FE count"),
        topology_revision: Bytes::from_static(b"foundationdb-suite-topology"),
    }
}

async fn write_partial_identity(keyspace_id: Uuid) {
    let path = cluster_file();
    let database = Database::from_path(path.to_str().expect("UTF-8 cluster file"))
        .expect("create direct FoundationDB test handle");
    let transaction = database
        .create_trx()
        .expect("create corruption transaction");
    transaction
        .set_option(TransactionOption::Timeout(4_000))
        .expect("set corruption transaction timeout");
    transaction
        .set_option(TransactionOption::RetryLimit(0))
        .expect("disable corruption transaction retries");
    let schema_key = [
        b"NRSS\x01".as_slice(),
        keyspace_id.as_bytes(),
        &[0x00, 0x00],
    ]
    .concat();
    transaction.set(&schema_key, &[1]);
    transaction
        .commit()
        .await
        .expect("persist partial identity corruption");
}

fn key(bytes: impl Into<Bytes>) -> Key {
    Key::try_from(bytes.into()).expect("valid test key")
}

fn value(bytes: impl Into<Bytes>) -> Value {
    Value::try_from(bytes.into()).expect("valid test value")
}

fn range(
    start: &'static [u8],
    end: &'static [u8],
    direction: Direction,
    page_size: usize,
) -> RangeRequest {
    RangeRequest {
        range: KeyRange::new(key(Bytes::from_static(start)), key(Bytes::from_static(end)))
            .expect("valid range"),
        direction,
        page_size,
        continuation: None,
    }
}

fn assert_committed(outcome: CommitOutcome) {
    assert!(
        matches!(outcome, CommitOutcome::Committed(_)),
        "{outcome:?}"
    );
}

async fn seed(store: &dyn StateStore, records: &[(&'static [u8], &'static [u8])]) {
    let mut transaction = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "seed")
        .await
        .expect("begin seed transaction");
    for (item, payload) in records {
        transaction
            .put(
                key(Bytes::from_static(item)),
                value(Bytes::from_static(payload)),
                Precondition::Any,
            )
            .await
            .expect("stage seed record");
    }
    assert_committed(transaction.commit().await);
}

async fn transaction_scenarios(runtime: &StateStoreRuntime) {
    let keyspace_id = Uuid::new_v4();
    let store = open_state_store(
        runtime,
        transaction_store_config("transaction-cluster", keyspace_id),
        deployment(),
    )
    .await
    .expect("open transaction keyspace");

    let binary_key = key(Bytes::from_static(&[0x00, 0xff, 0x10]));
    let binary_value = value(Bytes::from_static(&[0xff, 0x00, 0x20]));
    let mut ordered = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "ordered-overlay")
        .await
        .expect("begin ordered overlay");
    ordered
        .put(binary_key.clone(), binary_value, Precondition::Absent)
        .await
        .expect("put absent");
    let first = ordered
        .get(&binary_key)
        .await
        .expect("overlay get")
        .expect("overlay record");
    assert!(
        first
            .version
            .as_bytes()
            .starts_with(b"fdb-provisional-v1\0")
    );
    ordered
        .delete(binary_key.clone(), Precondition::Version(first.version))
        .await
        .expect("delete provisional version");
    ordered
        .put(
            binary_key.clone(),
            value(Bytes::from_static(b"final")),
            Precondition::Absent,
        )
        .await
        .expect("put after overlay delete");
    assert_committed(ordered.commit().await);
    let mut repeatable = store.begin_read().await.expect("begin repeatable read");
    let before = repeatable.get(&binary_key).await.expect("first read");
    seed(store.as_ref(), &[(&[0x00, 0xff, 0x10], b"changed")]).await;
    assert_eq!(
        repeatable.get(&binary_key).await.expect("second read"),
        before
    );
    repeatable.abort().await.expect("abort repeatable read");

    let conflict_key = key(Bytes::from_static(b"same-key"));
    seed(store.as_ref(), &[(b"same-key", b"base")]).await;
    let mut left = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "same-left")
        .await
        .expect("begin left");
    let mut right = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "same-right")
        .await
        .expect("begin right");
    left.get(&conflict_key).await.expect("left read");
    right.get(&conflict_key).await.expect("right read");
    left.put(
        conflict_key.clone(),
        value(Bytes::from_static(b"left")),
        Precondition::Present,
    )
    .await
    .expect("left put");
    right
        .put(
            conflict_key,
            value(Bytes::from_static(b"right")),
            Precondition::Present,
        )
        .await
        .expect("right put");
    assert_committed(left.commit().await);
    assert!(matches!(right.commit().await, CommitOutcome::Conflict(_)));

    seed(store.as_ref(), &[(b"skew-a", b"1"), (b"skew-b", b"1")]).await;
    let mut skew_left = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "skew-left")
        .await
        .expect("begin skew left");
    let mut skew_right = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "skew-right")
        .await
        .expect("begin skew right");
    skew_left
        .get(&key(Bytes::from_static(b"skew-a")))
        .await
        .expect("read skew a");
    skew_right
        .get(&key(Bytes::from_static(b"skew-b")))
        .await
        .expect("read skew b");
    skew_left
        .put(
            key(Bytes::from_static(b"skew-b")),
            value(Bytes::from_static(b"0")),
            Precondition::Any,
        )
        .await
        .expect("write skew b");
    skew_right
        .put(
            key(Bytes::from_static(b"skew-a")),
            value(Bytes::from_static(b"0")),
            Precondition::Any,
        )
        .await
        .expect("write skew a");
    assert_committed(skew_left.commit().await);
    assert!(matches!(
        skew_right.commit().await,
        CommitOutcome::Conflict(_)
    ));

    let mut phantom = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "phantom-reader")
        .await
        .expect("begin phantom reader");
    phantom
        .range(&range(b"phantom-", b"phantom.", Direction::Forward, 2))
        .await
        .expect("read empty phantom range");
    seed(store.as_ref(), &[(b"phantom-key", b"inserted")]).await;
    phantom
        .put(
            key(Bytes::from_static(b"phantom-outcome")),
            value(Bytes::from_static(b"value")),
            Precondition::Any,
        )
        .await
        .expect("stage phantom outcome");
    assert!(matches!(phantom.commit().await, CommitOutcome::Conflict(_)));

    seed(
        store.as_ref(),
        &[
            (b"page-0", b"0"),
            (b"page-1", b"1"),
            (b"page-2", b"2"),
            (b"page-3", b"3"),
            (b"page-4", b"4"),
            (b"page-5", b"5"),
        ],
    )
    .await;
    let mut overlay = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "overlay-refill")
        .await
        .expect("begin overlay refill");
    for item in [b"page-0", b"page-1", b"page-2"] {
        overlay
            .delete(key(Bytes::from_static(item)), Precondition::Any)
            .await
            .expect("overlay delete");
    }
    let page = overlay
        .range(&range(b"page-", b"page.", Direction::Forward, 2))
        .await
        .expect("forward refill");
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.key.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"page-3".as_slice(), b"page-4".as_slice()]
    );
    assert!(page.continuation.is_some());
    assert_eq!(
        overlay
            .put(
                key(Bytes::from_static(b"page-new")),
                value(Bytes::from_static(b"new")),
                Precondition::Any,
            )
            .await
            .expect_err("continuation freezes mutations")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    overlay.abort().await.expect("abort overlay refill");

    let mut reverse_overlay = store
        .begin_write(
            TransactionId::from(Uuid::new_v4()),
            "reverse-overlay-refill",
        )
        .await
        .expect("begin reverse overlay refill");
    for item in [b"page-5", b"page-4", b"page-3"] {
        reverse_overlay
            .delete(key(Bytes::from_static(item)), Precondition::Any)
            .await
            .expect("reverse overlay delete");
    }
    let reverse_refill = reverse_overlay
        .range(&range(b"page-", b"page.", Direction::Reverse, 2))
        .await
        .expect("reverse refill");
    assert_eq!(
        reverse_refill
            .records
            .iter()
            .map(|record| record.key.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"page-2".as_slice(), b"page-1".as_slice()]
    );
    reverse_overlay
        .abort()
        .await
        .expect("abort reverse overlay refill");

    let page_request = range(b"page-", b"page.", Direction::Forward, 2);
    let mut snapshot_scan = store.begin_read().await.expect("begin snapshot scan");
    let first_page = snapshot_scan
        .range(&page_request)
        .await
        .expect("snapshot first page");
    let continuation = first_page.continuation.expect("snapshot continuation");
    seed(store.as_ref(), &[(b"page-15", b"between")]).await;
    let mut continued_request = page_request.clone();
    continued_request.continuation = Some(continuation.clone());
    let same_snapshot = snapshot_scan
        .range(&continued_request)
        .await
        .expect("same transaction next page");
    assert_eq!(same_snapshot.records[0].key.as_bytes(), b"page-2");
    snapshot_scan.abort().await.expect("abort snapshot scan");
    let mut checkpoint_scan = store.begin_read().await.expect("begin checkpoint scan");
    let checkpoint = checkpoint_scan
        .range(&continued_request)
        .await
        .expect("new transaction checkpoint page");
    assert_eq!(checkpoint.records[0].key.as_bytes(), b"page-15");
    checkpoint_scan
        .abort()
        .await
        .expect("abort checkpoint scan");

    let mut reverse = store.begin_read().await.expect("begin reverse scan");
    let reverse_page = reverse
        .range(&range(b"page-", b"page.", Direction::Reverse, 2))
        .await
        .expect("reverse page");
    assert_eq!(
        reverse_page
            .records
            .iter()
            .map(|record| record.key.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"page-5".as_slice(), b"page-4".as_slice()]
    );
    reverse.abort().await.expect("abort reverse scan");

    let limited_keyspace_id = Uuid::new_v4();
    let limited_store = open_state_store(
        runtime,
        StateStoreConfig {
            cluster_id: "limited-cluster".to_owned(),
            limits: StateStoreLimitOverrides {
                max_transaction_bytes: Some(16 * 1024),
                ..Default::default()
            },
            provider: StateStoreProviderConfig::Foundationdb {
                cluster_file: cluster_file(),
                keyspace_id: limited_keyspace_id,
            },
        },
        deployment(),
    )
    .await
    .expect("open limited keyspace");
    let transaction_id = TransactionId::from(Uuid::new_v4());
    let mut limited = limited_store
        .begin_write(transaction_id, "pre-io-limit")
        .await
        .expect("begin limited transaction without provider I/O");
    assert_eq!(
        limited
            .put(
                key(Bytes::from_static(b"large")),
                value(Bytes::from(vec![0x55; 16 * 1024])),
                Precondition::Any,
            )
            .await
            .expect_err("physical envelope exceeds public transaction budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    limited.abort().await.expect("abort limited transaction");
    let database = Database::from_path(
        cluster_file()
            .to_str()
            .expect("UTF-8 FoundationDB cluster file"),
    )
    .expect("open raw FoundationDB controller handle");
    let controller = database.create_trx().expect("controller transaction");
    controller
        .set_option(TransactionOption::Timeout(4_000))
        .expect("controller timeout");
    controller
        .set_option(TransactionOption::RetryLimit(0))
        .expect("controller retry limit");
    let reservation_key = [
        b"NRSS\x01".as_slice(),
        limited_keyspace_id.as_bytes(),
        &[0x03],
        transaction_id.as_uuid().as_bytes(),
    ]
    .concat();
    assert!(
        controller
            .get(&reservation_key, false)
            .await
            .expect("read reservation key")
            .is_none(),
        "pre-I/O limit failure must not create a durable reservation"
    );
    drop(limited_store);
    drop(store);
}

#[tokio::test(flavor = "multi_thread")]
async fn foundationdb_suite() {
    let mut runtime = StateStoreRuntime::foundationdb(client_config())
        .expect("boot process-owned FoundationDB runtime");

    let keyspace_id = Uuid::new_v4();
    let config = store_config("identity-cluster", keyspace_id);
    let (left, right) = tokio::join!(
        open_state_store(&runtime, config.clone(), deployment()),
        open_state_store(&runtime, config, deployment())
    );
    let left = left.expect("initialize FoundationDB keyspace");
    let right = right.expect("concurrent open converges on keyspace identity");
    let left_identity = left.identity().await.expect("read left identity");
    let right_identity = right.identity().await.expect("read right identity");
    assert_eq!(left_identity, right_identity);
    assert_eq!(left_identity.cluster_id, "identity-cluster");
    assert_eq!(left_identity.initial_incarnation, 1);

    let mismatch = match open_state_store(
        &runtime,
        store_config("different-cluster", keyspace_id),
        deployment(),
    )
    .await
    {
        Ok(_) => panic!("existing keyspace must reject a cluster identity mismatch"),
        Err(error) => error,
    };
    assert_eq!(mismatch.kind(), StateStoreErrorKind::InvalidConfiguration);

    let corrupt_keyspace = Uuid::new_v4();
    write_partial_identity(corrupt_keyspace).await;
    let corruption = match open_state_store(
        &runtime,
        store_config("identity-cluster", corrupt_keyspace),
        deployment(),
    )
    .await
    {
        Ok(_) => panic!("partial identity must fail closed"),
        Err(error) => error,
    };
    assert_eq!(corruption.kind(), StateStoreErrorKind::Corruption);

    transaction_scenarios(&runtime).await;

    drop(right);
    drop(left);
    runtime
        .shutdown()
        .await
        .expect("shutdown FoundationDB runtime after all handles drain");
}
