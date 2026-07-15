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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Barrier, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks::state_store::limits::MAX_KEY_BYTES;
use novarocks::state_store::{
    ChangePage, ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution, Direction, Key,
    KeyRange, Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StoreIdentity, StoreRevision,
    TransactionId, Value, WriteTransaction,
};
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
    atomic_commit(&factory).await;
    commit_resolution_after_cancel(&factory).await;
    resolve_does_not_report_not_committed_while_inflight(&factory).await;
    limits_before_io(&factory).await;
    deadline_interrupts_blocking_sql(&factory).await;
    arbitrary_binary_payloads(&factory).await;
    second_owner_rejected(&factory).await;
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
            Precondition::Version(original.version),
        )
        .await
        .expect("stage stale write");
    assert_conflict(stale.commit().await);

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
    reached: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl FaultGate {
    pub fn new() -> Self {
        Self {
            reached: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
        }
    }

    async fn pause(&self) {
        let reached = Arc::clone(&self.reached);
        tokio::task::spawn_blocking(move || reached.wait())
            .await
            .expect("fault gate reached worker");
        let release = Arc::clone(&self.release);
        tokio::task::spawn_blocking(move || release.wait())
            .await
            .expect("fault gate release worker");
    }

    pub async fn wait_reached(&self) {
        let reached = Arc::clone(&self.reached);
        tokio::task::spawn_blocking(move || reached.wait())
            .await
            .expect("wait for fault gate");
    }

    pub async fn release(&self) {
        let release = Arc::clone(&self.release);
        tokio::task::spawn_blocking(move || release.wait())
            .await
            .expect("release fault gate");
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
}

pub struct FaultInjectingStateStore {
    inner: Arc<dyn StateStore>,
    script: Arc<Mutex<FaultScript>>,
    resolutions: Arc<Mutex<HashMap<TransactionId, CommitResolution>>>,
}

impl FaultInjectingStateStore {
    pub fn new(inner: Arc<dyn StateStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            script: Arc::new(Mutex::new(FaultScript::default())),
            resolutions: Arc::new(Mutex::new(HashMap::new())),
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
    resolutions: Arc<Mutex<HashMap<TransactionId, CommitResolution>>>,
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

fn scripted_outcome(id: TransactionId, result: ScriptedCommitResult) -> CommitOutcome {
    let error =
        || StateStoreError::new(StateStoreErrorKind::Internal, "injected state store fault");
    match result {
        ScriptedCommitResult::Committed => CommitOutcome::Committed(CommitReceipt {
            transaction_id: id,
            revision: StoreRevision::try_from(Bytes::from_static(b"fault-revision"))
                .expect("fault revision"),
        }),
        ScriptedCommitResult::Conflict => CommitOutcome::Conflict(error()),
        ScriptedCommitResult::TransientBeforeCommit => {
            CommitOutcome::TransientBeforeCommit(error())
        }
        ScriptedCommitResult::DefiniteFailure => CommitOutcome::DefiniteFailure(error()),
        ScriptedCommitResult::CommitUnknown => CommitOutcome::CommitUnknown(error()),
    }
}

fn resolution_for(outcome: &CommitOutcome) -> Option<CommitResolution> {
    match outcome {
        CommitOutcome::Committed(receipt) => Some(CommitResolution::Committed(receipt.clone())),
        CommitOutcome::Conflict(_)
        | CommitOutcome::TransientBeforeCommit(_)
        | CommitOutcome::DefiniteFailure(_) => Some(CommitResolution::NotCommitted),
        CommitOutcome::CommitUnknown(_) => None,
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
        let id = *self.inner.transaction_id();
        let (pre_commit, post_dispatch) = {
            let mut script = self.script.lock().expect("fault script");
            (script.pre_commit.take(), script.post_dispatch.take())
        };
        if let Some(result) = pre_commit {
            let outcome = scripted_outcome(id, result);
            let resolution = resolution_for(&outcome).unwrap_or(CommitResolution::Unresolved);
            self.resolutions
                .lock()
                .expect("fault resolutions")
                .insert(id, resolution);
            return outcome;
        }
        let Some(gate) = post_dispatch else {
            return self.inner.commit().await;
        };

        self.resolutions
            .lock()
            .expect("fault resolutions")
            .insert(id, CommitResolution::Unresolved);
        let resolutions = Arc::clone(&self.resolutions);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let commit = tokio::spawn(async move { self.inner.commit().await });
            gate.pause().await;
            let outcome = commit.await.unwrap_or_else(|_| {
                CommitOutcome::CommitUnknown(StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "injected commit worker failed",
                ))
            });
            let mut resolutions = resolutions.lock().expect("fault resolutions");
            if let Some(resolution) = resolution_for(&outcome) {
                resolutions.insert(id, resolution);
            } else {
                resolutions.remove(&id);
            }
            drop(resolutions);
            let _ = sender.send(outcome);
        });
        receiver.await.unwrap_or_else(|_| {
            CommitOutcome::CommitUnknown(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "injected commit response was cancelled",
            ))
        })
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
            resolutions: Arc::clone(&self.resolutions),
        }))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        if let Some(error) = self.script.lock().expect("fault script").change_poll.take() {
            return Err(error);
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
        if let Some(resolution) = self
            .resolutions
            .lock()
            .expect("fault resolutions")
            .get(transaction_id)
            .cloned()
        {
            return Ok(resolution);
        }
        self.inner.resolve_commit(transaction_id).await
    }
}

async fn cancelled_commit_resolution(factory: &StateStoreFactory, prefix: u8) {
    let fault = FaultInjectingStateStore::new(open(factory).await);
    let id = transaction_id();
    let first = key(vec![prefix, 1]);
    let second = key(vec![prefix, 2]);
    let mut transaction = fault
        .begin_write(id, "cancelled conformance commit")
        .await
        .expect("begin cancelled commit");
    transaction
        .put(first.clone(), value(b"first".to_vec()), Precondition::Any)
        .await
        .expect("stage first cancelled row");
    transaction
        .put(second.clone(), value(b"second".to_vec()), Precondition::Any)
        .await
        .expect("stage second cancelled row");
    let gate = FaultGate::new();
    fault.pause_next_post_dispatch(gate.clone());
    let commit = tokio::spawn(async move { transaction.commit().await });
    gate.wait_reached().await;
    commit.abort();
    assert!(
        commit
            .await
            .expect_err("commit waiter cancelled")
            .is_cancelled()
    );
    assert_eq!(
        fault
            .resolve_commit(&id)
            .await
            .expect("resolve in-flight commit"),
        CommitResolution::Unresolved
    );
    gate.release().await;
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match fault
                .resolve_commit(&id)
                .await
                .expect("resolve terminal commit")
            {
                CommitResolution::Unresolved => tokio::task::yield_now().await,
                terminal => break terminal,
            }
        }
    })
    .await
    .expect("commit reconciliation must terminate");
    let first_row = read_record(&(fault.clone() as Arc<dyn StateStore>), &first).await;
    let second_row = read_record(&(fault as Arc<dyn StateStore>), &second).await;
    match terminal {
        CommitResolution::Committed(_) => {
            assert!(first_row.is_some());
            assert!(second_row.is_some());
        }
        CommitResolution::NotCommitted => {
            assert!(first_row.is_none());
            assert!(second_row.is_none());
        }
        CommitResolution::Unresolved => unreachable!(),
    }
}

pub async fn commit_resolution_after_cancel(factory: &StateStoreFactory) {
    cancelled_commit_resolution(factory, 9).await;
}

pub async fn resolve_does_not_report_not_committed_while_inflight(factory: &StateStoreFactory) {
    let fault = FaultInjectingStateStore::new(open(factory).await);
    let id = transaction_id();
    let item = key(vec![10, 1]);
    let mut transaction = fault
        .begin_write(id, "observable in-flight commit")
        .await
        .expect("begin observable in-flight commit");
    transaction
        .put(item.clone(), value(b"value".to_vec()), Precondition::Any)
        .await
        .expect("stage observable in-flight row");
    let gate = FaultGate::new();
    fault.pause_next_post_dispatch(gate.clone());
    let commit = tokio::spawn(async move { transaction.commit().await });
    gate.wait_reached().await;
    for _ in 0..3 {
        assert_eq!(
            fault
                .resolve_commit(&id)
                .await
                .expect("resolve in-flight commit"),
            CommitResolution::Unresolved
        );
    }
    gate.release().await;
    let receipt = committed(commit.await.expect("join observable commit"));
    assert_eq!(
        fault
            .resolve_commit(&id)
            .await
            .expect("resolve committed transaction"),
        CommitResolution::Committed(receipt)
    );
    assert_eq!(
        read_record(&(fault as Arc<dyn StateStore>), &item)
            .await
            .expect("committed in-flight row")
            .value,
        value(b"value".to_vec())
    );
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

pub async fn deadline_interrupts_blocking_sql(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let deadline = store.limits().transaction_deadline;
    let mut reader = store.begin_read().await.expect("begin deadline read");
    tokio::time::sleep(deadline + std::time::Duration::from_millis(10)).await;
    assert_eq!(
        reader
            .get(&key(b"c12/deadline".to_vec()))
            .await
            .expect_err("transaction deadline covers operations")
            .kind(),
        StateStoreErrorKind::DeadlineExceeded
    );
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

pub async fn second_owner_rejected(factory: &StateStoreFactory) {
    let first = open(factory).await;
    let error = match factory().await {
        Ok(_) => panic!("second owner must fail while first handle is live"),
        Err(error) => error,
    };
    assert!(matches!(
        error.kind(),
        StateStoreErrorKind::ProviderUnavailable
            | StateStoreErrorKind::InvalidRequest
            | StateStoreErrorKind::UnsupportedDeployment
    ));
    drop(first);
}
