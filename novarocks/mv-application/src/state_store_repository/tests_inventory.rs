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

use super::StateStoreMvRepository;
use super::tests_definition::{projection_request, repository, target};
use crate::persistence::validation::PersistenceDecodeBudget;
use crate::repository::{
    DeleteMvProjectionRequest, LoadedMvProjection, MvProjectionInventoryBound, MvRepository,
    MvRepositoryErrorKind, ReplaceMvProjectionRequest,
};
use novarocks_state_store_api::{
    AttemptSupervisor, Key, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreLimits, StoreIdentity, WriteAttempt, WriteTransaction,
};
use novarocks_state_store_runtime::StateStoreRunPolicy;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Default)]
struct ReadTrace {
    begins: usize,
    aborts: usize,
    ranges: Vec<RangeRequest>,
}

struct PagePause {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

struct RecordingStore {
    inner: Arc<dyn StateStore>,
    trace: Arc<Mutex<ReadTrace>>,
    pause: Arc<Mutex<Option<PagePause>>>,
}

impl RecordingStore {
    fn new(inner: Arc<dyn StateStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            trace: Arc::new(Mutex::new(ReadTrace::default())),
            pause: Arc::new(Mutex::new(None)),
        })
    }

    fn reset(&self) {
        *self.trace.lock().expect("read trace") = ReadTrace::default();
    }

    fn pause_next_page(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, observed) = oneshot::channel();
        let (resume, released) = oneshot::channel();
        *self.pause.lock().expect("page pause") = Some(PagePause {
            reached,
            resume: released,
        });
        (observed, resume)
    }

    fn assert_scan_closed(&self, ranges: usize) {
        let trace = self.trace.lock().expect("read trace");
        assert_eq!(trace.begins, 1, "inventory must use one read snapshot");
        assert_eq!(trace.aborts, 1, "inventory must close its read snapshot");
        assert_eq!(trace.ranges.len(), ranges, "no range after refusal or EOF");
        assert!(trace.ranges.iter().all(|request| request.page_size == 1));
        assert!(trace.ranges[0].continuation.is_none());
        assert!(
            trace
                .ranges
                .iter()
                .skip(1)
                .all(|request| request.continuation.is_some())
        );
    }
}

struct RecordingRead {
    inner: Box<dyn ReadTransaction>,
    trace: Arc<Mutex<ReadTrace>>,
    pause: Arc<Mutex<Option<PagePause>>>,
}

#[async_trait::async_trait]
impl ReadTransaction for RecordingRead {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        self.trace
            .lock()
            .expect("read trace")
            .ranges
            .push(request.clone());
        let page = self.inner.range(request).await?;
        // Pause after the real store has fixed and read the first snapshot.
        let pause = self.pause.lock().expect("page pause").take();
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            pause.resume.await.expect("release snapshot page");
        }
        Ok(page)
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.trace.lock().expect("read trace").aborts += 1;
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl StateStore for RecordingStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        let inner = self.inner.begin_read().await?;
        self.trace.lock().expect("read trace").begins += 1;
        Ok(Box::new(RecordingRead {
            inner,
            trace: Arc::clone(&self.trace),
            pause: Arc::clone(&self.pause),
        }))
    }

    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.inner.begin_write(attempt, purpose).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
}

fn bound() -> MvProjectionInventoryBound {
    MvProjectionInventoryBound {
        entries: 4,
        snapshot_bytes: 16 * 1024,
        raw_page_bytes: 128 * 1024,
        decode: PersistenceDecodeBudget::default(),
    }
}

async fn recorded_repository() -> (Arc<RecordingStore>, Arc<StateStoreMvRepository>) {
    let (store, _) = repository().await;
    let store = RecordingStore::new(store);
    let repository = StateStoreMvRepository::open(store.clone(), StateStoreRunPolicy::default())
        .await
        .expect("open recorded repository");
    (store, repository)
}

async fn seed(repository: &StateStoreMvRepository, name: &str) -> LoadedMvProjection {
    repository
        .create_projection(
            Uuid::now_v7(),
            projection_request(name, name.as_bytes(), 1, "base"),
        )
        .await
        .expect("seed canonical projection")
}

#[tokio::test]
async fn inventory_reads_one_snapshot_in_single_record_pages_and_returns_thin_identities() {
    let (store, repository) = recorded_repository().await;
    let first = seed(&repository, "first").await;
    let second = seed(&repository, "second").await;
    store.reset();
    let mut exact = bound();
    exact.entries = 2;
    let inventory = repository.list_projection_inventory(exact).await.unwrap();
    store.assert_scan_closed(2);
    assert_eq!(inventory.len(), 2);
    for (entry, loaded) in inventory.iter().zip([&first, &second]) {
        assert_eq!(entry.mv_id, loaded.projection.mv_id);
        assert_eq!(&entry.target, loaded.projection.facts.target());
        assert_eq!(
            &entry.object_id,
            &loaded.projection.facts.source_revision().target_object_id
        );
    }
    // Snapshot locators do not serve as a cached projection/version read.
    assert_eq!(
        repository
            .find_by_target_bounded(&target("first"), exact)
            .await
            .unwrap(),
        Some(first),
    );
}

#[tokio::test]
async fn inventory_entry_refusal_closes_the_snapshot_without_reading_the_third_page() {
    let (store, repository) = recorded_repository().await;
    for name in ["first", "second", "third"] {
        seed(&repository, name).await;
    }
    store.reset();
    let mut tiny = bound();
    tiny.entries = 1;
    let error = repository
        .list_projection_inventory(tiny)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::InvalidRequest);
    assert!(error.message().contains("entry bound"), "{error}");
    store.assert_scan_closed(2);
}

#[tokio::test]
async fn inventory_snapshot_byte_refusal_closes_before_reading_the_next_page() {
    let (store, repository) = recorded_repository().await;
    seed(&repository, "first").await;
    seed(&repository, "second").await;
    store.reset();
    let mut tiny = bound();
    tiny.snapshot_bytes = 1;
    let error = repository
        .list_projection_inventory(tiny)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::InvalidRequest);
    assert!(error.message().contains("snapshot byte bound"), "{error}");
    store.assert_scan_closed(1);
}

#[tokio::test]
async fn inventory_continuation_keeps_its_first_snapshot_across_concurrent_insert_and_delete() {
    let (store, repository) = recorded_repository().await;
    let first = seed(&repository, "first").await;
    let removed = seed(&repository, "removed").await;
    store.reset();
    let (reached, resume) = store.pause_next_page();
    let scanner = Arc::clone(&repository);
    let task = tokio::spawn(async move { scanner.list_projection_inventory(bound()).await });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .expect("first snapshot page must be reached")
        .expect("first page signal");
    // Concurrent writers use the same underlying store, outside the scanner's
    // recording wrapper: create returns a fresh read of its committed result.
    let mutator =
        StateStoreMvRepository::open(Arc::clone(&store.inner), StateStoreRunPolicy::default())
            .await
            .expect("concurrent mutation repository");
    seed(&mutator, "inserted").await;
    mutator
        .delete_projection(
            Uuid::now_v7(),
            DeleteMvProjectionRequest {
                mv_id: removed.projection.mv_id,
                expected_version: removed.version.clone(),
                expected_source_revision: removed.projection.facts.source_revision().clone(),
            },
        )
        .await
        .expect("delete after snapshot was fixed");
    resume.send(()).expect("release snapshot");
    let inventory = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("inventory must finish")
        .expect("scanner task")
        .expect("snapshot inventory");
    store.assert_scan_closed(2);
    assert_eq!(
        inventory
            .iter()
            .map(|entry| entry.mv_id)
            .collect::<Vec<_>>(),
        [first.projection.mv_id, removed.projection.mv_id],
    );
    assert_eq!(inventory[1].target, target("removed"));
    assert!(
        repository
            .find_by_target_bounded(&target("removed"), bound())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .find_by_target_bounded(&target("inserted"), bound())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn inventory_raw_and_decode_refusals_abort_without_following_the_continuation() {
    let (store, repository) = recorded_repository().await;
    seed(&repository, "first").await;
    seed(&repository, "second").await;
    let mut raw = bound();
    raw.raw_page_bytes = 1;
    let mut document = bound();
    document.decode.max_document_bytes = 1;
    let mut workspace = bound();
    workspace.decode.max_working_set_bytes = 1;
    for (tiny, kind, message) in [
        (
            raw,
            MvRepositoryErrorKind::InvalidRequest,
            "raw page byte bound",
        ),
        (
            document,
            MvRepositoryErrorKind::Corruption,
            "encoded document",
        ),
        (
            workspace,
            MvRepositoryErrorKind::Corruption,
            "working set bound",
        ),
    ] {
        store.reset();
        let error = repository
            .list_projection_inventory(tiny)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert!(error.message().contains(message), "{error}");
        store.assert_scan_closed(1);
    }
}

#[tokio::test]
async fn bounded_target_lookup_returns_the_current_exact_version_after_replacement() {
    let (_, repository) = recorded_repository().await;
    let created = seed(&repository, "first").await;
    let inventory = repository.list_projection_inventory(bound()).await.unwrap();
    let replacement = repository
        .replace_projection(
            Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: created.projection.mv_id,
                expected_version: created.version.clone(),
                projection: projection_request("first", b"first", 2, "changed-base"),
            },
        )
        .await
        .expect("replace the scanned target");
    assert_ne!(created.version, replacement.version);
    let fresh = repository
        .find_by_target_bounded(&inventory[0].target, bound())
        .await
        .unwrap()
        .expect("fresh target");
    assert_eq!(fresh, replacement);
    assert_eq!(
        repository
            .find_by_target(&inventory[0].target)
            .await
            .unwrap(),
        Some(fresh),
    );
}
