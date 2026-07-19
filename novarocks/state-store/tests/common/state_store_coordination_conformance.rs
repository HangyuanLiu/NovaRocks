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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, ClockHealth, ControlPlaneMode, CoordinationError,
    CoordinationErrorKind, CoordinationOperation, CoordinationOutcome, HolderId, IncarnationGate,
    LeaseCancellationReason, LeaseClock, LeaseGuard, LeaseManager, LeaseSettings, ResourceKey,
};
use novarocks_state_store::{
    ChangePage, ChangePollRequest, CommitOutcome, CommitResolution, Key, OperationId, Precondition,
    RangePage, RangeRequest, ReadTransaction, STATE_STORE_OUTCOME_COUNT, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
    StateStoreOperation, StoreIdentity, TransactionId, Value, WriteTransaction,
    derive_transaction_id,
};
use sha2::{Digest, Sha256};
use tokio::sync::Barrier;
use uuid::Uuid;

use super::state_store_conformance::{
    PostDispatchScenario, StateStoreConformanceFixture, StateStoreFactory,
};

const CONTROL_KEY: &[u8] = b"\0novarocks/cp/v1/control";
const LEASE_KEY_PREFIX: &[u8] = b"\0novarocks/cp/v1/lease/";

pub struct ManualLeaseClock {
    wall_ms: AtomicU64,
    monotonic_ms: AtomicU64,
    health: AtomicU8,
    wall_readable: AtomicBool,
}

impl ManualLeaseClock {
    pub fn new(wall_ms: u64, monotonic_ms: u64) -> Self {
        Self {
            wall_ms: AtomicU64::new(wall_ms),
            monotonic_ms: AtomicU64::new(monotonic_ms),
            health: AtomicU8::new(0),
            wall_readable: AtomicBool::new(true),
        }
    }

    pub fn set_health(&self, health: ClockHealth) {
        self.health.store(
            match health {
                ClockHealth::Healthy => 0,
                ClockHealth::Unsafe => 1,
                ClockHealth::Unknown => 2,
            },
            Ordering::SeqCst,
        );
    }

    pub fn advance_wall(&self, millis: u64) {
        self.wall_ms
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(millis)
            })
            .expect("manual wall clock overflow");
    }

    pub fn set_wall(&self, millis: u64) {
        self.wall_ms.store(millis, Ordering::SeqCst);
    }

    pub fn advance_monotonic(&self, millis: u64) {
        self.monotonic_ms
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(millis)
            })
            .expect("manual monotonic clock overflow");
    }

    pub fn set_wall_readable(&self, readable: bool) {
        self.wall_readable.store(readable, Ordering::SeqCst);
    }
}

impl LeaseClock for ManualLeaseClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        if self.wall_readable.load(Ordering::SeqCst) {
            Ok(self.wall_ms.load(Ordering::SeqCst))
        } else {
            Err(CoordinationError::clock_unsafe())
        }
    }

    fn monotonic_time_millis(&self) -> u64 {
        self.monotonic_ms.load(Ordering::SeqCst)
    }

    fn health(&self) -> ClockHealth {
        match self.health.load(Ordering::SeqCst) {
            0 => ClockHealth::Healthy,
            1 => ClockHealth::Unsafe,
            _ => ClockHealth::Unknown,
        }
    }
}

struct UnreadableLeaseClock;

impl LeaseClock for UnreadableLeaseClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        Err(CoordinationError::clock_unsafe())
    }

    fn monotonic_time_millis(&self) -> u64 {
        0
    }

    fn health(&self) -> ClockHealth {
        ClockHealth::Healthy
    }
}

struct OneAcquireConflictStore {
    inner: Arc<dyn StateStore>,
    commit_barrier: Arc<Barrier>,
    commit_order: Arc<AtomicUsize>,
    remaining_wrapped_writes: AtomicUsize,
}

impl OneAcquireConflictStore {
    fn new(inner: Arc<dyn StateStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            commit_barrier: Arc::new(Barrier::new(2)),
            commit_order: Arc::new(AtomicUsize::new(0)),
            remaining_wrapped_writes: AtomicUsize::new(2),
        })
    }
}

struct OneAcquireConflictTransaction {
    inner: Box<dyn WriteTransaction>,
    commit_barrier: Arc<Barrier>,
    commit_order: Arc<AtomicUsize>,
}

#[async_trait]
impl ReadTransaction for OneAcquireConflictTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner.abort().await
    }
}

#[async_trait]
impl WriteTransaction for OneAcquireConflictTransaction {
    fn transaction_id(&self) -> &TransactionId {
        self.inner.transaction_id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.delete(key, precondition).await
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        self.commit_barrier.wait().await;
        if self.commit_order.fetch_add(1, Ordering::SeqCst) == 0 {
            self.inner.commit().await
        } else {
            match self.inner.abort().await {
                Ok(()) => CommitOutcome::Conflict(StateStoreError::new(
                    StateStoreErrorKind::Conflict,
                    "injected disjoint acquisition conflict",
                )),
                Err(error) => CommitOutcome::DefiniteFailure(error),
            }
        }
    }
}

#[async_trait]
impl StateStore for OneAcquireConflictStore {
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
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let inner = self.inner.begin_write(transaction_id, purpose).await?;
        let wrap_acquire = purpose == "acquire fenced resource lease"
            && self
                .remaining_wrapped_writes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
        if wrap_acquire {
            Ok(Box::new(OneAcquireConflictTransaction {
                inner,
                commit_barrier: Arc::clone(&self.commit_barrier),
                commit_order: Arc::clone(&self.commit_order),
            }))
        } else {
            Ok(inner)
        }
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
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

struct LeaseMutationRaceStore {
    inner: Arc<dyn StateStore>,
    first_purpose: &'static str,
    second_purpose: &'static str,
    commit_barrier: Arc<Barrier>,
    remaining_wrapped_writes: AtomicUsize,
}

impl LeaseMutationRaceStore {
    fn new(
        inner: Arc<dyn StateStore>,
        first_purpose: &'static str,
        second_purpose: &'static str,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            first_purpose,
            second_purpose,
            commit_barrier: Arc::new(Barrier::new(2)),
            remaining_wrapped_writes: AtomicUsize::new(0),
        })
    }

    fn arm(&self) {
        self.remaining_wrapped_writes.store(2, Ordering::SeqCst);
    }
}

struct LeaseMutationRaceTransaction {
    inner: Box<dyn WriteTransaction>,
    commit_barrier: Arc<Barrier>,
}

#[async_trait]
impl ReadTransaction for LeaseMutationRaceTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner.abort().await
    }
}

#[async_trait]
impl WriteTransaction for LeaseMutationRaceTransaction {
    fn transaction_id(&self) -> &TransactionId {
        self.inner.transaction_id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.delete(key, precondition).await
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        self.commit_barrier.wait().await;
        self.inner.commit().await
    }
}

#[async_trait]
impl StateStore for LeaseMutationRaceStore {
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
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let inner = self.inner.begin_write(transaction_id, purpose).await?;
        let wrap = (purpose == self.first_purpose || purpose == self.second_purpose)
            && self
                .remaining_wrapped_writes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
        if wrap {
            Ok(Box::new(LeaseMutationRaceTransaction {
                inner,
                commit_barrier: Arc::clone(&self.commit_barrier),
            }))
        } else {
            Ok(inner)
        }
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
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

struct OpaqueProviderStore {
    inner: Arc<dyn StateStore>,
}

#[async_trait]
impl StateStore for OpaqueProviderStore {
    fn provider_name(&self) -> &'static str {
        "opaque-test-provider"
    }

    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.inner.begin_write(transaction_id, purpose).await
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
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

async fn open_fixture(factory: &StateStoreFactory) -> StateStoreConformanceFixture {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match factory().await {
                Ok(fixture) => break fixture,
                Err(error) if error.kind() == StateStoreErrorKind::ProviderUnavailable => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("open coordination fixture: {error:?}"),
            }
        }
    })
    .await
    .expect("best-effort lease drop releases prior fixture ownership")
}

fn key(bytes: &'static [u8]) -> Key {
    Key::try_from(Bytes::from_static(bytes)).expect("valid coordination key")
}

fn value(bytes: Vec<u8>) -> Value {
    Value::try_from(Bytes::from(bytes)).expect("valid coordination value")
}

fn transaction_id() -> TransactionId {
    Uuid::now_v7().into()
}

fn resource(bytes: impl Into<Vec<u8>>) -> ResourceKey {
    ResourceKey::try_from(Bytes::from(bytes.into())).expect("valid resource key")
}

fn holder(bytes: &'static [u8]) -> HolderId {
    HolderId::try_from(Bytes::from_static(bytes)).expect("valid holder id")
}

fn attempt() -> AttemptId {
    AttemptId::try_from(Uuid::now_v7()).expect("UUIDv7 lease attempt")
}

fn lease_settings() -> LeaseSettings {
    LeaseSettings::new(
        Duration::from_secs(10),
        Duration::from_secs(3),
        Duration::from_secs(1),
        Duration::from_millis(500),
    )
    .expect("valid lease settings")
}

fn acquired(outcome: AcquireOutcome) -> LeaseGuard {
    match outcome {
        AcquireOutcome::Acquired(guard) => guard,
        AcquireOutcome::Contended(_) => panic!("expected acquired lease, found contention"),
        AcquireOutcome::AwaitingTakeover(_) => {
            panic!("expected acquired lease, found takeover observation")
        }
    }
}

fn assert_cancelled_as(guard: &LeaseGuard, expected: LeaseCancellationReason) {
    let cancellation = guard.cancellation();
    assert_eq!(*cancellation.borrow(), Some(expected));
}

fn lease_key(resource_bytes: &[u8]) -> Key {
    let digest = Sha256::digest(resource_bytes);
    let mut bytes = Vec::with_capacity(LEASE_KEY_PREFIX.len() + digest.len());
    bytes.extend_from_slice(LEASE_KEY_PREFIX);
    bytes.extend_from_slice(&digest);
    Key::try_from(Bytes::from(bytes)).expect("valid lease storage key")
}

fn encoded_released_lease(
    resource_bytes: &[u8],
    holder_bytes: &[u8],
    attempt: Uuid,
    epoch: u64,
) -> Value {
    let mut encoded = Vec::new();
    encoded.push(1);
    encoded.extend_from_slice(&(resource_bytes.len() as u32).to_be_bytes());
    encoded.extend_from_slice(resource_bytes);
    encoded.push(2);
    encoded.extend_from_slice(&(holder_bytes.len() as u32).to_be_bytes());
    encoded.extend_from_slice(holder_bytes);
    encoded.extend_from_slice(attempt.as_bytes());
    encoded.extend_from_slice(&1_u64.to_be_bytes());
    encoded.extend_from_slice(&epoch.to_be_bytes());
    encoded.extend_from_slice(&100_000_u64.to_be_bytes());
    encoded.extend_from_slice(&100_000_u64.to_be_bytes());
    encoded.extend_from_slice(OperationId::new_v7().as_uuid().as_bytes());
    value(encoded)
}

async fn seed_released_lease(
    store: &Arc<dyn StateStore>,
    resource_bytes: &[u8],
    holder_bytes: &[u8],
    attempt: Uuid,
    epoch: u64,
) -> StateRecord {
    let lease_key = lease_key(resource_bytes);
    let mut transaction = store
        .begin_write(transaction_id(), "seed released coordination lease")
        .await
        .expect("begin released lease seed");
    transaction
        .put(
            lease_key.clone(),
            encoded_released_lease(resource_bytes, holder_bytes, attempt, epoch),
            Precondition::Absent,
        )
        .await
        .expect("stage released lease seed");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    let mut reader = store.begin_read().await.expect("begin lease seed read");
    let record = reader
        .get(&lease_key)
        .await
        .expect("read lease seed")
        .expect("released lease seed exists");
    reader.abort().await.expect("abort lease seed read");
    record
}

async fn finish_disjoint_acquire(
    manager: &LeaseManager,
    resource: ResourceKey,
    attempt: AttemptId,
    first_operation: OperationId,
    first: Result<AcquireOutcome, CoordinationError>,
) -> (LeaseGuard, bool) {
    match first {
        Ok(outcome) => (acquired(outcome), false),
        Err(error) => {
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_eq!(
                error.transaction_id(),
                Some(derive_transaction_id(first_operation, 1))
            );
            let retry_operation = OperationId::new_v7();
            assert_ne!(retry_operation, first_operation);
            (
                acquired(
                    manager
                        .acquire(resource, attempt, retry_operation)
                        .await
                        .expect("retry definite disjoint acquisition conflict"),
                ),
                true,
            )
        }
    }
}

fn state_store_operation_total(store: &Arc<dyn StateStore>, operation: StateStoreOperation) -> u64 {
    let snapshot = store.metrics_snapshot();
    (0..STATE_STORE_OUTCOME_COUNT)
        .map(|outcome| snapshot.operation_outcomes[operation as usize][outcome])
        .sum()
}

fn encoded_control(
    store_id: Uuid,
    cluster_id: &str,
    incarnation: u64,
    mode: ControlPlaneMode,
    operation_id: OperationId,
) -> Value {
    let mut encoded = Vec::new();
    encoded.push(1);
    encoded.extend_from_slice(store_id.as_bytes());
    encoded.extend_from_slice(&(cluster_id.len() as u32).to_be_bytes());
    encoded.extend_from_slice(cluster_id.as_bytes());
    encoded.extend_from_slice(&incarnation.to_be_bytes());
    encoded.push(match mode {
        ControlPlaneMode::Reconciling => 1,
        ControlPlaneMode::WriteOpen => 2,
    });
    encoded.extend_from_slice(operation_id.as_uuid().as_bytes());
    value(encoded)
}

async fn seed_control(
    store: &Arc<dyn StateStore>,
    store_id: Uuid,
    cluster_id: &str,
    incarnation: u64,
    mode: ControlPlaneMode,
) {
    let mut transaction = store
        .begin_write(transaction_id(), "seed coordination control record")
        .await
        .expect("begin control seed");
    transaction
        .put(
            key(CONTROL_KEY),
            encoded_control(
                store_id,
                cluster_id,
                incarnation,
                mode,
                OperationId::new_v7(),
            ),
            Precondition::Absent,
        )
        .await
        .expect("stage control seed");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
}

pub async fn incarnation_gate_lifecycle(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    assert_eq!(
        gate.load().await.unwrap_err().kind(),
        CoordinationErrorKind::NotBootstrapped
    );
    let bootstrap_id = OperationId::new_v7();
    let open = gate.bootstrap(bootstrap_id).await.unwrap();
    assert_eq!(open.incarnation().get(), 1);
    assert_eq!(open.mode(), ControlPlaneMode::WriteOpen);
    let restore_id = OperationId::new_v7();
    let restoring = gate.begin_restore(&open, restore_id).await.unwrap();
    assert_eq!(restoring.incarnation().get(), 2);
    assert_eq!(restoring.mode(), ControlPlaneMode::Reconciling);
    assert_eq!(
        gate.admit_writes().await.unwrap_err().kind(),
        CoordinationErrorKind::WriteClosed
    );
    let reopen_id = OperationId::new_v7();
    let reopened = gate.open_writes(&restoring, reopen_id).await.unwrap();
    assert_eq!(reopened.incarnation(), restoring.incarnation());
    assert_eq!(reopened.mode(), ControlPlaneMode::WriteOpen);
}

pub async fn concurrent_bootstrap_converges(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let left = IncarnationGate::new(Arc::clone(&fixture.store));
    let right = IncarnationGate::new(Arc::clone(&fixture.store));

    let (left, right) = tokio::join!(
        left.bootstrap(OperationId::new_v7()),
        right.bootstrap(OperationId::new_v7())
    );
    let left = left.expect("left bootstrap converges");
    let right = right.expect("right bootstrap converges");
    assert_eq!(left, right);
    assert_eq!(left.incarnation().get(), 1);
    assert_eq!(left.mode(), ControlPlaneMode::WriteOpen);
}

pub async fn stale_snapshots_cannot_mutate(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let open = gate.bootstrap(OperationId::new_v7()).await.unwrap();
    let restoring = gate
        .begin_restore(&open, OperationId::new_v7())
        .await
        .unwrap();

    assert_eq!(
        gate.begin_restore(&open, OperationId::new_v7())
            .await
            .unwrap_err()
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    assert_eq!(
        gate.open_writes(&open, OperationId::new_v7())
            .await
            .unwrap_err()
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );

    gate.open_writes(&restoring, OperationId::new_v7())
        .await
        .unwrap();
    assert_eq!(
        gate.open_writes(&restoring, OperationId::new_v7())
            .await
            .unwrap_err()
            .kind(),
        CoordinationErrorKind::FenceLost
    );
}

pub async fn incarnation_overflow_fails_closed(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let identity = fixture.store.identity().await.expect("load store identity");
    seed_control(
        &fixture.store,
        identity.store_id,
        &identity.cluster_id,
        u64::MAX,
        ControlPlaneMode::WriteOpen,
    )
    .await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let open = gate.load().await.expect("load maximum incarnation");

    assert_eq!(
        gate.begin_restore(&open, OperationId::new_v7())
            .await
            .unwrap_err()
            .kind(),
        CoordinationErrorKind::IncarnationExhausted
    );
    assert_eq!(gate.load().await.unwrap(), open);
}

pub async fn identity_mismatch_is_corruption(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let identity = fixture.store.identity().await.expect("load store identity");
    seed_control(
        &fixture.store,
        Uuid::now_v7(),
        &identity.cluster_id,
        identity.initial_incarnation,
        ControlPlaneMode::WriteOpen,
    )
    .await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));

    assert_eq!(
        gate.load().await.unwrap_err().kind(),
        CoordinationErrorKind::Corruption
    );
}

pub async fn recovery_is_operation_scoped(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let bootstrap_id = OperationId::new_v7();
    let open = gate.bootstrap(bootstrap_id).await.unwrap();
    assert_eq!(gate.recover_bootstrap(bootstrap_id).await.unwrap(), open);

    let never_applied = OperationId::new_v7();
    let error = gate.recover_bootstrap(never_applied).await.unwrap_err();
    assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
    assert_eq!(
        error.transaction_id(),
        Some(derive_transaction_id(never_applied, 1))
    );

    let restore_id = OperationId::new_v7();
    let restoring = gate.begin_restore(&open, restore_id).await.unwrap();
    assert_eq!(
        gate.recover_begin_restore(&open, restore_id).await.unwrap(),
        restoring
    );
    let reopen_id = OperationId::new_v7();
    let reopened = gate.open_writes(&restoring, reopen_id).await.unwrap();
    assert_eq!(
        gate.recover_open_writes(&restoring, reopen_id)
            .await
            .unwrap(),
        reopened
    );
    assert_eq!(
        gate.recover_begin_restore(&open, restore_id)
            .await
            .unwrap_err()
            .kind(),
        CoordinationErrorKind::FenceLost
    );
    assert_eq!(
        gate.recover_bootstrap(bootstrap_id)
            .await
            .unwrap_err()
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
}

pub async fn commit_unknown_uses_authoritative_read_back(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::LoseCommittedResponse)
        .await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let operation_id = OperationId::new_v7();
    let waiter = tokio::spawn(async move { gate.bootstrap(operation_id).await });
    control.wait_dispatched().await;
    control.allow_provider_progress().await;
    control.release_response().await;

    let snapshot = waiter
        .await
        .expect("join unknown bootstrap")
        .expect("resolve committed bootstrap");
    control.wait_inner_dropped().await;
    assert_eq!(snapshot.incarnation().get(), 1);
    assert_eq!(snapshot.mode(), ControlPlaneMode::WriteOpen);
    let recovery_gate = IncarnationGate::new(Arc::clone(&fixture.store));
    assert_eq!(
        recovery_gate
            .recover_bootstrap(operation_id)
            .await
            .expect("recover exact response-loss bootstrap"),
        snapshot
    );
}

pub async fn cancelled_mutation_recovers_with_same_operation(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::CancelWaiterBeforeApply)
        .await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let operation_id = OperationId::new_v7();
    let transaction_id = derive_transaction_id(operation_id, 1);
    let waiter = tokio::spawn(async move { gate.bootstrap(operation_id).await });
    control.wait_dispatched().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    control.wait_waiter_cancelled().await;
    control.release_response().await;
    control.wait_inner_dropped().await;
    control.allow_provider_progress().await;

    let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let resolution = fixture
                .store
                .resolve_commit(&transaction_id)
                .await
                .expect("resolve cancelled coordination mutation");
            if resolution != CommitResolution::Unresolved {
                break resolution;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled coordination mutation reaches terminal resolution");
    let recovery_gate = IncarnationGate::new(Arc::clone(&fixture.store));
    match terminal {
        CommitResolution::Committed(_) => {
            let snapshot = recovery_gate
                .recover_bootstrap(operation_id)
                .await
                .expect("recover committed cancelled bootstrap");
            assert_eq!(snapshot.incarnation().get(), 1);
            assert_eq!(snapshot.mode(), ControlPlaneMode::WriteOpen);
        }
        CommitResolution::NotCommitted => {
            let error = recovery_gate
                .recover_bootstrap(operation_id)
                .await
                .unwrap_err();
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_eq!(error.transaction_id(), Some(transaction_id));
        }
        CommitResolution::Unresolved => unreachable!("terminal loop excludes unresolved"),
    }
}

pub async fn unresolved_bootstrap_without_visible_record_is_uncertain(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::CancelWaiterBeforeApply)
        .await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let operation_id = OperationId::new_v7();
    let transaction_id = derive_transaction_id(operation_id, 1);
    let waiter = tokio::spawn(async move { gate.bootstrap(operation_id).await });
    control.wait_dispatched().await;

    assert_eq!(
        fixture
            .store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve held bootstrap"),
        CommitResolution::Unresolved
    );
    let recovery_gate = IncarnationGate::new(Arc::clone(&fixture.store));
    assert_eq!(
        recovery_gate.load().await.unwrap_err().kind(),
        CoordinationErrorKind::NotBootstrapped
    );
    let error = recovery_gate
        .recover_bootstrap(operation_id)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), CoordinationErrorKind::CommitUncertain);
    assert_eq!(error.transaction_id(), Some(transaction_id));

    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    control.wait_waiter_cancelled().await;
    control.release_response().await;
    control.wait_inner_dropped().await;
    control.allow_provider_progress().await;
}

pub async fn admission_read_conflicts_with_restore(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let open = gate.bootstrap(OperationId::new_v7()).await.unwrap();
    let admission = gate.admit_writes().await.unwrap();
    let mut domain = fixture
        .store
        .begin_write(transaction_id(), "admitted domain write")
        .await
        .expect("begin admitted domain write");
    admission
        .validate_in(domain.as_mut())
        .await
        .expect("validate domain write admission");

    let restore = gate
        .begin_restore(&open, OperationId::new_v7())
        .await
        .expect("commit restore gate");
    domain
        .put(
            key(b"domain/admitted-write"),
            value(b"value".to_vec()),
            Precondition::Absent,
        )
        .await
        .expect("stage admitted domain write");
    assert!(matches!(domain.commit().await, CommitOutcome::Conflict(_)));
    assert_eq!(restore.mode(), ControlPlaneMode::Reconciling);
}

pub async fn basic_acquire_contention_and_high_watermark(factory: &StateStoreFactory) {
    for invalid in [
        LeaseSettings::new(
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
        ),
        LeaseSettings::new(
            Duration::from_millis(2),
            Duration::from_millis(2),
            Duration::from_millis(1),
            Duration::from_millis(1),
        ),
        LeaseSettings::new(
            Duration::from_millis(2),
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_millis(1),
        ),
        LeaseSettings::new(
            Duration::from_millis(2),
            Duration::from_millis(1),
            Duration::ZERO,
            Duration::from_millis(1),
        ),
        LeaseSettings::new(
            Duration::from_millis(2),
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::ZERO,
        ),
        LeaseSettings::new(
            Duration::from_millis(u64::MAX),
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
        ),
        LeaseSettings::new(
            Duration::from_secs(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_secs(u64::MAX),
        ),
    ] {
        assert_eq!(
            invalid.expect_err("invalid lease settings").kind(),
            CoordinationErrorKind::InvalidRequest
        );
    }

    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(100_000, 7_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");

    clock.set_health(ClockHealth::Unsafe);
    assert_eq!(
        manager_a
            .acquire(resource(b"clock-unsafe"), attempt(), OperationId::new_v7(),)
            .await
            .expect_err("unsafe clock must fail closed")
            .kind(),
        CoordinationErrorKind::ClockUnsafe
    );
    assert_eq!(
        manager_a.metrics_snapshot().operation_outcome_count(
            CoordinationOperation::Acquire,
            CoordinationOutcome::ClockUnsafe,
        ),
        1
    );
    clock.set_health(ClockHealth::Unknown);
    assert_eq!(
        manager_a
            .acquire(resource(b"clock-unknown"), attempt(), OperationId::new_v7(),)
            .await
            .expect_err("unknown clock health must fail closed")
            .kind(),
        CoordinationErrorKind::ClockUnsafe
    );
    assert_eq!(
        manager_a.metrics_snapshot().operation_outcome_count(
            CoordinationOperation::Acquire,
            CoordinationOutcome::ClockUnsafe,
        ),
        2
    );
    clock.set_health(ClockHealth::Healthy);

    let binary_resource = resource(vec![0, 0xff, b'/', 0, 0x80]);
    let attempt_a = attempt();
    let attempt_b = attempt();
    let first_operation = OperationId::new_v7();
    let first = acquired(
        manager_a
            .acquire(binary_resource.clone(), attempt_a, first_operation)
            .await
            .unwrap(),
    );
    assert_eq!(first.token().resource_epoch().get(), 1);
    let recovered = acquired(
        manager_a
            .recover_acquire(binary_resource.clone(), attempt_a, first_operation)
            .await
            .expect("recover exact committed acquisition"),
    );
    assert_eq!(recovered.token(), first.token());
    let never_applied = OperationId::new_v7();
    let recovery_error = manager_a
        .recover_acquire(binary_resource.clone(), attempt_a, never_applied)
        .await
        .expect_err("unknown operation must not be replayed");
    assert_eq!(
        recovery_error.kind(),
        CoordinationErrorKind::OperationNotCommitted
    );
    assert_eq!(
        recovery_error.transaction_id(),
        Some(derive_transaction_id(never_applied, 1))
    );
    let observation = match manager_b
        .acquire(binary_resource.clone(), attempt_b, OperationId::new_v7())
        .await
        .unwrap()
    {
        AcquireOutcome::Contended(observation) => observation,
        _ => panic!("second holder must observe contention"),
    };
    assert_eq!(observation.token(), first.token());
    assert!(observation.retry_after() > Duration::ZERO);
    let same = acquired(
        manager_a
            .acquire(binary_resource, attempt_a, OperationId::new_v7())
            .await
            .unwrap(),
    );
    assert_eq!(same.token(), first.token());

    clock.advance_wall(11_001);
    let expired_observation = match manager_b
        .acquire(
            resource(vec![0, 0xff, b'/', 0, 0x80]),
            attempt_b,
            OperationId::new_v7(),
        )
        .await
        .expect("expired lease observation")
    {
        AcquireOutcome::AwaitingTakeover(observation) => observation,
        _ => panic!("Task 3 must not mutate an expired current-incarnation lease"),
    };
    assert_eq!(expired_observation.token(), first.token());

    let conflict_store: Arc<dyn StateStore> =
        OneAcquireConflictStore::new(Arc::clone(&fixture.store));
    let parallel_a = LeaseManager::new(
        Arc::clone(&conflict_store),
        holder(b"parallel-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("parallel manager A");
    let parallel_b = LeaseManager::new(
        conflict_store,
        holder(b"parallel-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("parallel manager B");
    let left_resource = resource(b"parallel-left");
    let right_resource = resource(b"parallel-right");
    let left_attempt = attempt();
    let right_attempt = attempt();
    let left_operation = OperationId::new_v7();
    let right_operation = OperationId::new_v7();
    let (left, right) = tokio::join!(
        parallel_a.acquire(left_resource.clone(), left_attempt, left_operation),
        parallel_b.acquire(right_resource.clone(), right_attempt, right_operation)
    );
    let (left, left_retried) = finish_disjoint_acquire(
        &parallel_a,
        left_resource,
        left_attempt,
        left_operation,
        left,
    )
    .await;
    let (right, right_retried) = finish_disjoint_acquire(
        &parallel_b,
        right_resource,
        right_attempt,
        right_operation,
        right,
    )
    .await;
    assert_eq!(usize::from(left_retried) + usize::from(right_retried), 1);
    assert_eq!(left.token().resource_epoch().get(), 1);
    assert_eq!(right.token().resource_epoch().get(), 1);

    let writes_before = state_store_operation_total(&fixture.store, StateStoreOperation::Put);
    let oversized = manager_a
        .acquire(resource(vec![b'x'; 80]), attempt(), OperationId::new_v7())
        .await
        .expect_err("encoded candidate exceeds fixture value limit");
    assert_eq!(oversized.kind(), CoordinationErrorKind::LimitExceeded);
    assert_eq!(
        state_store_operation_total(&fixture.store, StateStoreOperation::Put),
        writes_before,
        "candidate limit must be rejected before staging provider mutation"
    );
}

pub async fn external_lease_clock_error_is_clock_unsafe(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"clock-error-holder"),
        Arc::new(UnreadableLeaseClock),
        lease_settings(),
    )
    .expect("manager");

    let error = manager
        .acquire(
            resource(b"clock-error-resource"),
            attempt(),
            OperationId::new_v7(),
        )
        .await
        .expect_err("external clock error must be propagated as clock unsafe");

    assert_eq!(error.kind(), CoordinationErrorKind::ClockUnsafe);
    assert_eq!(error.transaction_id(), None);
    assert_eq!(error.to_string(), "ClockUnsafe: lease clock is unsafe");
}

pub async fn concurrent_acquire_exactly_one_winner(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(200_000, 9_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"holder-b"),
        clock,
        lease_settings(),
    )
    .expect("manager B");
    let resource = resource(b"one-winner");

    let (left, right) = tokio::join!(
        manager_a.acquire(resource.clone(), attempt(), OperationId::new_v7()),
        manager_b.acquire(resource, attempt(), OperationId::new_v7())
    );
    let outcomes = [
        left.expect("left terminal outcome"),
        right.expect("right terminal outcome"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcquireOutcome::Acquired(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcquireOutcome::Contended(_)))
            .count(),
        1
    );
}

pub async fn lease_expiry_observation(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(100_000, 7_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"expiry-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"expiry-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");
    let resource = resource(b"expiry-observation");
    let first_attempt = attempt();
    let second_attempt = attempt();
    let first = acquired(
        manager_a
            .acquire(resource.clone(), first_attempt, OperationId::new_v7())
            .await
            .expect("first acquisition"),
    );

    clock.advance_wall(11_000);
    let early = match manager_b
        .acquire(resource.clone(), second_attempt, OperationId::new_v7())
        .await
        .expect("deadline plus skew boundary is still contended")
    {
        AcquireOutcome::Contended(observation) => observation,
        _ => panic!("takeover must not start at the exact expiry boundary"),
    };
    assert_eq!(early.retry_after(), Duration::from_millis(1));
    clock.advance_wall(1);
    assert!(matches!(
        manager_b
            .acquire(resource.clone(), second_attempt, OperationId::new_v7())
            .await
            .expect("start expiry observation"),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(499);
    let awaiting = match manager_b
        .acquire(resource.clone(), second_attempt, OperationId::new_v7())
        .await
        .expect("observation is not complete")
    {
        AcquireOutcome::AwaitingTakeover(observation) => observation,
        _ => panic!("takeover must wait the full monotonic observation window"),
    };
    assert_eq!(awaiting.retry_after(), Duration::from_millis(1));
    clock.advance_monotonic(1);
    let second = acquired(
        manager_b
            .acquire(resource, second_attempt, OperationId::new_v7())
            .await
            .expect("take over unchanged expired lease"),
    );
    assert_eq!(first.token().resource_epoch().get(), 1);
    assert_eq!(second.token().resource_epoch().get(), 2);
}

pub async fn renew_resets_observation(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(200_000, 8_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"renew-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"renew-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");
    let resource = resource(b"renew-reset-observation");
    let first_attempt = attempt();
    let second_attempt = attempt();
    let mut first = acquired(
        manager_a
            .acquire(resource.clone(), first_attempt, OperationId::new_v7())
            .await
            .expect("first acquisition"),
    );

    clock.advance_wall(11_001);
    assert!(matches!(
        manager_b
            .acquire(resource.clone(), second_attempt, OperationId::new_v7())
            .await
            .expect("start first expiry observation"),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(250);
    let old_fence = first.fence();
    first
        .renew(OperationId::new_v7())
        .await
        .expect("renew exact observed lease");
    assert_eq!(first.token().resource_epoch().get(), 1);
    assert_ne!(first.fence(), old_fence);

    clock.advance_wall(11_001);
    assert!(matches!(
        manager_b
            .acquire(resource.clone(), second_attempt, OperationId::new_v7())
            .await
            .expect("renewed version starts a new observation"),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(499);
    assert!(matches!(
        manager_b
            .acquire(resource.clone(), second_attempt, OperationId::new_v7())
            .await
            .expect("renewed version still needs a full window"),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(1);
    let second = acquired(
        manager_b
            .acquire(resource, second_attempt, OperationId::new_v7())
            .await
            .expect("take over unchanged renewed version"),
    );
    assert_eq!(second.token().resource_epoch().get(), 2);
}

pub async fn lease_lifecycle_and_cancellation(factory: &StateStoreFactory) {
    exact_renew_release_and_stale_guards(factory).await;
    cancelled_renew_recovers_with_exact_pending_candidate(factory).await;
    clock_failures_cancel_renewal(factory).await;
    release_preserves_maximum_epoch(factory).await;
    runtime_and_no_runtime_drop(factory).await;
    renew_acquire_race_has_one_mutation_winner(factory).await;
    release_acquire_race_has_one_mutation_winner(factory).await;
    incarnation_change_cancels_guard(factory).await;
}

async fn cancelled_renew_recovers_with_exact_pending_candidate(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(350_000, 9_500));
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"cancelled-renew-holder"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager");
    let mut guard = acquired(
        manager
            .acquire(
                resource(b"cancelled-renew-recovery"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("acquire lease before cancelled renew"),
    );
    let old_fence = guard.fence();
    let operation_id = OperationId::new_v7();
    let transaction_id = derive_transaction_id(operation_id, 1);
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::CancelWaiterBeforeApply)
        .await;
    clock.advance_wall(100);

    let mut renewal = Box::pin(guard.renew(operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut renewal => panic!("renewal returned before cancellation: {result:?}"),
    }
    drop(renewal);
    control.wait_waiter_cancelled().await;
    control.release_response().await;
    control.wait_inner_dropped().await;
    control.allow_provider_progress().await;

    let terminal = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let resolution = fixture
                .store
                .resolve_commit(&transaction_id)
                .await
                .expect("resolve cancelled renewal");
            if resolution != CommitResolution::Unresolved {
                break resolution;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled renewal reaches terminal resolution");

    match terminal {
        CommitResolution::Committed(_) => {
            guard
                .recover_renew(operation_id)
                .await
                .expect("recover exact committed cancelled renewal");
            assert_ne!(guard.fence(), old_fence);
        }
        CommitResolution::NotCommitted => {
            let error = guard
                .recover_renew(operation_id)
                .await
                .expect_err("recover exact noncommitted cancelled renewal");
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_eq!(error.transaction_id(), Some(transaction_id));
        }
        CommitResolution::Unresolved => unreachable!("terminal loop excludes unresolved"),
    }
}

async fn exact_renew_release_and_stale_guards(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(300_000, 9_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"lifecycle-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"lifecycle-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");
    let resource = resource(b"exact-renew-release");
    let first_attempt = attempt();
    let mut guard = acquired(
        manager_a
            .acquire(resource.clone(), first_attempt, OperationId::new_v7())
            .await
            .expect("first acquisition"),
    );
    let mut stale_renew = acquired(
        manager_a
            .acquire(resource.clone(), first_attempt, OperationId::new_v7())
            .await
            .expect("stale renew guard snapshot"),
    );
    let mut stale_release = acquired(
        manager_a
            .acquire(resource.clone(), first_attempt, OperationId::new_v7())
            .await
            .expect("stale release guard snapshot"),
    );
    assert_eq!(guard.renew_after(), Duration::from_secs(3));
    let old_token = guard.token().clone();
    let old_fence = guard.fence();
    let renew_operation = OperationId::new_v7();
    clock.advance_wall(100);
    guard
        .renew(renew_operation)
        .await
        .expect("renew exact current lease");
    assert_eq!(guard.token(), &old_token);
    assert_ne!(guard.fence(), old_fence);
    assert_eq!(*guard.cancellation().borrow(), None);
    let renewed_fence = guard.fence();
    guard
        .recover_renew(renew_operation)
        .await
        .expect("recover exact committed renewal");
    assert_eq!(guard.fence(), renewed_fence);

    assert_eq!(
        stale_renew
            .renew(OperationId::new_v7())
            .await
            .expect_err("stale renewal must lose its fence")
            .kind(),
        CoordinationErrorKind::FenceLost
    );
    assert_cancelled_as(&stale_renew, LeaseCancellationReason::FenceLost);
    assert_eq!(
        stale_release
            .release(OperationId::new_v7())
            .await
            .expect_err("stale release must lose its fence")
            .kind(),
        CoordinationErrorKind::FenceLost
    );
    assert_cancelled_as(&stale_release, LeaseCancellationReason::FenceLost);

    let release_operation = OperationId::new_v7();
    guard
        .release(release_operation)
        .await
        .expect("release exact current lease");
    assert_cancelled_as(&guard, LeaseCancellationReason::Released);
    guard
        .recover_release(release_operation)
        .await
        .expect("recover exact committed release");
    assert_eq!(
        manager_a
            .acquire(resource.clone(), first_attempt, OperationId::new_v7())
            .await
            .expect_err("released attempt cannot be reacquired")
            .kind(),
        CoordinationErrorKind::InvalidRequest
    );
    let second = acquired(
        manager_b
            .acquire(resource, attempt(), OperationId::new_v7())
            .await
            .expect("different attempt advances released high watermark"),
    );
    assert_eq!(second.token().resource_epoch().get(), 2);
    assert_eq!(
        manager_a
            .metrics_snapshot()
            .operation_outcome_count(CoordinationOperation::Renew, CoordinationOutcome::Success,),
        2
    );
    assert_eq!(
        manager_a
            .metrics_snapshot()
            .operation_outcome_count(CoordinationOperation::Release, CoordinationOutcome::Success,),
        2
    );
}

async fn clock_failures_cancel_renewal(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(400_000, 10_000));
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"clock-lifecycle-holder"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager");

    let mut unsafe_guard = acquired(
        manager
            .acquire(resource(b"unsafe-renew"), attempt(), OperationId::new_v7())
            .await
            .expect("unsafe test acquisition"),
    );
    clock.set_health(ClockHealth::Unsafe);
    assert_eq!(
        unsafe_guard
            .renew(OperationId::new_v7())
            .await
            .expect_err("unsafe clock cancels renewal")
            .kind(),
        CoordinationErrorKind::ClockUnsafe
    );
    assert_cancelled_as(&unsafe_guard, LeaseCancellationReason::ClockUnsafe);

    clock.set_health(ClockHealth::Healthy);
    unsafe_guard
        .renew(OperationId::new_v7())
        .await
        .expect("clock-unsafe result must not make the guard inactive");
    let mut unknown_guard = acquired(
        manager
            .acquire(resource(b"unknown-renew"), attempt(), OperationId::new_v7())
            .await
            .expect("unknown test acquisition"),
    );
    clock.set_health(ClockHealth::Unknown);
    assert_eq!(
        unknown_guard
            .renew(OperationId::new_v7())
            .await
            .expect_err("unknown clock cancels renewal")
            .kind(),
        CoordinationErrorKind::ClockUnsafe
    );
    assert_cancelled_as(&unknown_guard, LeaseCancellationReason::ClockUnsafe);

    clock.set_health(ClockHealth::Healthy);
    let mut unreadable_guard = acquired(
        manager
            .acquire(
                resource(b"unreadable-renew"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("unreadable test acquisition"),
    );
    clock.set_wall_readable(false);
    assert_eq!(
        unreadable_guard
            .renew(OperationId::new_v7())
            .await
            .expect_err("unreadable clock cancels renewal")
            .kind(),
        CoordinationErrorKind::ClockUnsafe
    );
    assert_cancelled_as(&unreadable_guard, LeaseCancellationReason::ClockUnsafe);

    clock.set_wall_readable(true);
    clock.set_wall(500_000);
    let mut rollback_guard = acquired(
        manager
            .acquire(
                resource(b"rollback-renew"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("rollback test acquisition"),
    );
    clock.set_wall(499_999);
    assert_eq!(
        rollback_guard
            .renew(OperationId::new_v7())
            .await
            .expect_err("wall rollback cancels renewal")
            .kind(),
        CoordinationErrorKind::ClockUnsafe
    );
    assert_cancelled_as(&rollback_guard, LeaseCancellationReason::ClockUnsafe);
}

async fn incarnation_change_cancels_guard(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let open = gate
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(600_000, 11_000));
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"incarnation-holder"),
        clock,
        lease_settings(),
    )
    .expect("manager");
    let mut guard = acquired(
        manager
            .acquire(
                resource(b"incarnation-renew"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("incarnation test acquisition"),
    );
    gate.begin_restore(&open, OperationId::new_v7())
        .await
        .expect("advance control incarnation");
    assert_eq!(
        guard
            .renew(OperationId::new_v7())
            .await
            .expect_err("old incarnation cannot renew")
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    assert_cancelled_as(&guard, LeaseCancellationReason::IncarnationChanged);
}

async fn release_preserves_maximum_epoch(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let resource_bytes = b"maximum-released-epoch";
    let resource = resource(resource_bytes);
    let before = seed_released_lease(
        &fixture.store,
        resource_bytes,
        b"maximum-holder",
        Uuid::now_v7(),
        u64::MAX,
    )
    .await;
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"next-holder"),
        Arc::new(ManualLeaseClock::new(700_000, 12_000)),
        lease_settings(),
    )
    .expect("manager");
    assert_eq!(
        manager
            .acquire(resource, attempt(), OperationId::new_v7())
            .await
            .expect_err("maximum epoch must fail closed")
            .kind(),
        CoordinationErrorKind::EpochExhausted
    );
    let mut reader = fixture
        .store
        .begin_read()
        .await
        .expect("begin max epoch read");
    let after = reader
        .get(&lease_key(resource_bytes))
        .await
        .expect("read max epoch record")
        .expect("max epoch record remains");
    reader.abort().await.expect("abort max epoch read");
    assert_eq!(after, before);
}

async fn runtime_and_no_runtime_drop(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let clock = Arc::new(ManualLeaseClock::new(800_000, 13_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"drop-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"drop-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");
    let runtime_resource = resource(b"runtime-drop");
    let runtime_guard = acquired(
        manager_a
            .acquire(runtime_resource.clone(), attempt(), OperationId::new_v7())
            .await
            .expect("runtime drop acquisition"),
    );
    drop(runtime_guard);
    let next_attempt = attempt();
    let next = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match manager_b
                .acquire(
                    runtime_resource.clone(),
                    next_attempt,
                    OperationId::new_v7(),
                )
                .await
                .expect("poll best-effort runtime release")
            {
                AcquireOutcome::Acquired(guard) => break guard,
                AcquireOutcome::Contended(_) | AcquireOutcome::AwaitingTakeover(_) => {
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("runtime drop eventually writes best-effort release");
    assert_eq!(next.token().resource_epoch().get(), 2);

    let no_runtime_resource = resource(b"no-runtime-drop");
    let no_runtime_guard = acquired(
        manager_a
            .acquire(
                no_runtime_resource.clone(),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("no-runtime drop acquisition"),
    );
    std::thread::spawn(move || drop(no_runtime_guard))
        .join()
        .expect("no-runtime drop must not panic");
    assert!(matches!(
        manager_b
            .acquire(no_runtime_resource, attempt(), OperationId::new_v7())
            .await
            .expect("no-runtime drop leaves deadline correctness path"),
        AcquireOutcome::Contended(_)
    ));
}

async fn renew_acquire_race_has_one_mutation_winner(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let concrete = LeaseMutationRaceStore::new(
        Arc::clone(&fixture.store),
        "renew fenced resource lease",
        "acquire fenced resource lease",
    );
    let store: Arc<dyn StateStore> = concrete.clone();
    let clock = Arc::new(ManualLeaseClock::new(900_000, 14_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&store),
        holder(b"renew-race-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        store,
        holder(b"renew-race-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");
    let resource = resource(b"renew-acquire-race");
    let mut guard = acquired(
        manager_a
            .acquire(resource.clone(), attempt(), OperationId::new_v7())
            .await
            .expect("race source acquisition"),
    );
    let contender_attempt = attempt();
    clock.advance_wall(11_001);
    assert!(matches!(
        manager_b
            .acquire(resource.clone(), contender_attempt, OperationId::new_v7(),)
            .await
            .expect("start race observation"),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(500);
    concrete.arm();
    let (renew, acquire) = tokio::join!(
        guard.renew(OperationId::new_v7()),
        manager_b.acquire(resource, contender_attempt, OperationId::new_v7())
    );
    match (renew, acquire) {
        (Ok(()), Ok(AcquireOutcome::Contended(_))) => {}
        (Err(error), Ok(AcquireOutcome::Acquired(_))) => {
            assert_eq!(error.kind(), CoordinationErrorKind::FenceLost);
            assert_cancelled_as(&guard, LeaseCancellationReason::FenceLost);
        }
        (renew, acquire) => panic!("unexpected renew/acquire race: {renew:?}, {acquire:?}"),
    }
}

async fn release_acquire_race_has_one_mutation_winner(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap coordination control");
    let concrete = LeaseMutationRaceStore::new(
        Arc::clone(&fixture.store),
        "release fenced resource lease",
        "acquire fenced resource lease",
    );
    let store: Arc<dyn StateStore> = concrete.clone();
    let clock = Arc::new(ManualLeaseClock::new(1_000_000, 15_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&store),
        holder(b"release-race-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        store,
        holder(b"release-race-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");
    let resource = resource(b"release-acquire-race");
    let mut guard = acquired(
        manager_a
            .acquire(resource.clone(), attempt(), OperationId::new_v7())
            .await
            .expect("race source acquisition"),
    );
    let contender_attempt = attempt();
    clock.advance_wall(11_001);
    assert!(matches!(
        manager_b
            .acquire(resource.clone(), contender_attempt, OperationId::new_v7(),)
            .await
            .expect("start race observation"),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(500);
    concrete.arm();
    let (release, acquire) = tokio::join!(
        guard.release(OperationId::new_v7()),
        manager_b.acquire(resource, contender_attempt, OperationId::new_v7())
    );
    match (release, acquire) {
        (Ok(()), Err(error)) => {
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_cancelled_as(&guard, LeaseCancellationReason::Released);
        }
        (Err(error), Ok(AcquireOutcome::Acquired(_))) => {
            assert_eq!(error.kind(), CoordinationErrorKind::FenceLost);
            assert_cancelled_as(&guard, LeaseCancellationReason::FenceLost);
        }
        (release, acquire) => panic!("unexpected release/acquire race: {release:?}, {acquire:?}"),
    }
}

async fn force_takeover(
    manager: &LeaseManager,
    resource: ResourceKey,
    attempt: AttemptId,
    clock: &ManualLeaseClock,
) -> LeaseGuard {
    clock.advance_wall(11_001);
    assert!(matches!(
        manager
            .acquire(resource.clone(), attempt, OperationId::new_v7())
            .await
            .expect("start complete takeover observation"),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(500);
    acquired(
        manager
            .acquire(resource, attempt, OperationId::new_v7())
            .await
            .expect("take over after complete monotonic observation window"),
    )
}

pub async fn stale_fence_finalize(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap stale-fence fixture");
    let clock = Arc::new(ManualLeaseClock::new(1_100_000, 20_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"fence-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"fence-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");

    let first_resource = resource(b"fence-before-validation");
    let first = acquired(
        manager_a
            .acquire(first_resource.clone(), attempt(), OperationId::new_v7())
            .await
            .expect("acquire old fence"),
    );
    let old_fence = first.fence();
    let current = force_takeover(&manager_b, first_resource, attempt(), &clock).await;
    let mut stale_transaction = fixture
        .store
        .begin_write(transaction_id(), "validate stale lease fence")
        .await
        .expect("begin stale fence validation");
    assert_eq!(
        old_fence
            .validate_in(stale_transaction.as_mut())
            .await
            .expect_err("taken-over fence must be stale")
            .kind(),
        CoordinationErrorKind::FenceLost
    );
    stale_transaction
        .abort()
        .await
        .expect("abort stale validation");
    let mut current_transaction = fixture
        .store
        .begin_write(transaction_id(), "validate current lease fence")
        .await
        .expect("begin current fence validation");
    current
        .fence()
        .validate_in(current_transaction.as_mut())
        .await
        .expect("current fence validates");
    current_transaction
        .abort()
        .await
        .expect("abort current validation");

    let deadline_resource = resource(b"deadline-is-not-fence");
    let deadline_guard = acquired(
        manager_a
            .acquire(deadline_resource, attempt(), OperationId::new_v7())
            .await
            .expect("acquire deadline-only fence"),
    );
    clock.advance_wall(11_001);
    let mut deadline_transaction = fixture
        .store
        .begin_write(transaction_id(), "validate deadline-only fence")
        .await
        .expect("begin deadline validation");
    deadline_guard
        .fence()
        .validate_in(deadline_transaction.as_mut())
        .await
        .expect("deadline passage alone does not stale a fence");
    deadline_transaction
        .abort()
        .await
        .expect("abort deadline validation");
    assert_eq!(
        manager_a.metrics_snapshot().operation_outcome_count(
            CoordinationOperation::ValidateFence,
            CoordinationOutcome::FenceLost,
        ),
        1
    );
    assert_eq!(
        manager_a.metrics_snapshot().operation_outcome_count(
            CoordinationOperation::ValidateFence,
            CoordinationOutcome::Success,
        ),
        1
    );
}

pub async fn early_takeover_is_fenced(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap early-takeover fixture");
    let clock = Arc::new(ManualLeaseClock::new(1_200_000, 21_000));
    let manager_a = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"early-holder-a"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager A");
    let manager_b = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"early-holder-b"),
        clock.clone(),
        lease_settings(),
    )
    .expect("manager B");
    let resource = resource(b"early-takeover-finalize");
    let guard = acquired(
        manager_a
            .acquire(resource.clone(), attempt(), OperationId::new_v7())
            .await
            .expect("acquire fence before early wall jump"),
    );
    let mut domain = fixture
        .store
        .begin_write(transaction_id(), "fenced domain finalize")
        .await
        .expect("begin fenced domain finalize");
    guard
        .fence()
        .validate_in(domain.as_mut())
        .await
        .expect("validate fence before takeover");
    domain
        .put(
            key(b"domain/fenced-finalize"),
            value(b"value".to_vec()),
            Precondition::Absent,
        )
        .await
        .expect("stage fenced domain write");

    let current = force_takeover(&manager_b, resource, attempt(), &clock).await;
    assert_eq!(current.token().resource_epoch().get(), 2);
    assert!(matches!(domain.commit().await, CommitOutcome::Conflict(_)));
}

pub async fn restore_invalidates_old_tokens(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let gate = IncarnationGate::new(Arc::clone(&fixture.store));
    let open = gate
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap restore-permanence fixture");
    let admission = gate.admit_writes().await.expect("old admission");
    let clock = Arc::new(ManualLeaseClock::new(1_300_000, 22_000));
    let manager_old = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"restore-holder-old"),
        clock.clone(),
        lease_settings(),
    )
    .expect("old manager");
    let manager_new = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"restore-holder-new"),
        clock,
        lease_settings(),
    )
    .expect("new manager");
    let resource = resource(b"restore-permanent-fence");
    let old_attempt = attempt();
    let mut old_renew = acquired(
        manager_old
            .acquire(resource.clone(), old_attempt, OperationId::new_v7())
            .await
            .expect("old incarnation acquisition"),
    );
    let mut old_release = acquired(
        manager_old
            .acquire(resource.clone(), old_attempt, OperationId::new_v7())
            .await
            .expect("old release guard"),
    );
    let old_fence = old_renew.fence();
    let restoring = gate
        .begin_restore(&open, OperationId::new_v7())
        .await
        .expect("begin restore");
    assert_eq!(restoring.incarnation().get(), 2);
    assert_eq!(restoring.mode(), ControlPlaneMode::Reconciling);
    assert_eq!(
        old_renew
            .renew(OperationId::new_v7())
            .await
            .expect_err("old lease cannot renew")
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    assert_eq!(
        old_release
            .release(OperationId::new_v7())
            .await
            .expect_err("old lease cannot release")
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    let mut old_transaction = fixture
        .store
        .begin_write(transaction_id(), "validate old restore tokens")
        .await
        .expect("begin old token validation");
    assert_eq!(
        admission
            .validate_in(old_transaction.as_mut())
            .await
            .expect_err("old admission fails after restore")
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    assert_eq!(
        old_fence
            .validate_in(old_transaction.as_mut())
            .await
            .expect_err("old fence fails after restore")
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    old_transaction.abort().await.expect("abort old validation");
    assert_eq!(
        gate.admit_writes()
            .await
            .expect_err("user admission is closed during restore")
            .kind(),
        CoordinationErrorKind::WriteClosed
    );

    let current = acquired(
        manager_new
            .acquire(resource, attempt(), OperationId::new_v7())
            .await
            .expect("new incarnation immediately supersedes old lease"),
    );
    assert_eq!(current.token().control_plane_incarnation().get(), 2);
    assert_eq!(current.token().resource_epoch().get(), 2);
    let reopened = gate
        .open_writes(&restoring, OperationId::new_v7())
        .await
        .expect("reopen exact restoring incarnation");
    assert_eq!(reopened.mode(), ControlPlaneMode::WriteOpen);
    let new_admission = gate.admit_writes().await.expect("new admission");
    let mut reopened_transaction = fixture
        .store
        .begin_write(transaction_id(), "validate reopened tokens")
        .await
        .expect("begin reopened validation");
    new_admission
        .validate_in(reopened_transaction.as_mut())
        .await
        .expect("new admission validates");
    current
        .fence()
        .validate_in(reopened_transaction.as_mut())
        .await
        .expect("new fence validates after reopen");
    assert_eq!(
        admission
            .validate_in(reopened_transaction.as_mut())
            .await
            .expect_err("incarnation-1 admission stays permanently stale")
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    assert_eq!(
        old_fence
            .validate_in(reopened_transaction.as_mut())
            .await
            .expect_err("incarnation-1 fence stays permanently stale")
            .kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    reopened_transaction
        .abort()
        .await
        .expect("abort reopened validation");
}

async fn await_coordination_terminal(
    store: &Arc<dyn StateStore>,
    transaction_id: TransactionId,
) -> CommitResolution {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let resolution = store
                .resolve_commit(&transaction_id)
                .await
                .expect("resolve coordination mutation");
            if resolution != CommitResolution::Unresolved {
                return resolution;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("coordination mutation reaches a terminal resolution")
}

async fn acquire_fault_semantics(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap acquire response-loss fixture");
    let clock = Arc::new(ManualLeaseClock::new(1_400_000, 23_000));
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"fault-acquire-holder"),
        clock.clone(),
        lease_settings(),
    )
    .expect("acquire manager");
    let contender = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"fault-acquire-contender"),
        clock.clone(),
        lease_settings(),
    )
    .expect("acquire contender");
    let response_resource = resource(b"fault-acquire-response-loss");
    let response_attempt = attempt();
    let operation_id = OperationId::new_v7();
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::LoseCommittedResponse)
        .await;
    let mut acquisition =
        Box::pin(manager.acquire(response_resource.clone(), response_attempt, operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut acquisition => panic!("acquisition returned before response loss: {result:?}"),
    }
    control.allow_provider_progress().await;
    control.release_response().await;
    let acquired_guard = acquired(
        acquisition
            .await
            .expect("response-loss acquisition resolves committed candidate"),
    );
    control.wait_inner_dropped().await;
    let recovered = acquired(
        manager
            .recover_acquire(response_resource.clone(), response_attempt, operation_id)
            .await
            .expect("recover exact response-loss acquisition"),
    );
    assert_eq!(recovered.fence(), acquired_guard.fence());
    let current = force_takeover(&contender, response_resource.clone(), attempt(), &clock).await;
    assert_eq!(current.token().resource_epoch().get(), 2);
    assert_eq!(
        manager
            .recover_acquire(response_resource, response_attempt, operation_id)
            .await
            .expect_err("committed acquisition superseded by takeover")
            .kind(),
        CoordinationErrorKind::FenceLost
    );

    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap cancelled acquire fixture");
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"cancel-acquire-holder"),
        Arc::new(ManualLeaseClock::new(1_500_000, 24_000)),
        lease_settings(),
    )
    .expect("cancel acquire manager");
    let resource = resource(b"fault-acquire-cancel");
    let attempt = attempt();
    let operation_id = OperationId::new_v7();
    let transaction_id = derive_transaction_id(operation_id, 1);
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::CancelWaiterBeforeApply)
        .await;
    let mut acquisition = Box::pin(manager.acquire(resource.clone(), attempt, operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut acquisition => panic!("acquisition returned before cancellation: {result:?}"),
    }
    let unresolved = manager
        .recover_acquire(resource.clone(), attempt, operation_id)
        .await
        .expect_err("unresolved absent acquisition is uncertain");
    assert_eq!(unresolved.kind(), CoordinationErrorKind::CommitUncertain);
    assert_eq!(unresolved.transaction_id(), Some(transaction_id));
    drop(acquisition);
    control.wait_waiter_cancelled().await;
    control.release_response().await;
    control.wait_inner_dropped().await;
    control.allow_provider_progress().await;
    let terminal = await_coordination_terminal(&fixture.store, transaction_id).await;
    match terminal {
        CommitResolution::Committed(_) => {
            let recovered = acquired(
                manager
                    .recover_acquire(resource, attempt, operation_id)
                    .await
                    .expect("recover committed cancelled acquisition"),
            );
            assert_eq!(recovered.token().resource_epoch().get(), 1);
        }
        CommitResolution::NotCommitted => {
            let error = manager
                .recover_acquire(resource, attempt, operation_id)
                .await
                .expect_err("recover noncommitted cancelled acquisition");
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_eq!(error.transaction_id(), Some(transaction_id));
        }
        CommitResolution::Unresolved => unreachable!("terminal helper excludes unresolved"),
    }
}

async fn renew_fault_semantics(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap renew response-loss fixture");
    let clock = Arc::new(ManualLeaseClock::new(1_600_000, 25_000));
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"fault-renew-holder"),
        clock.clone(),
        lease_settings(),
    )
    .expect("renew manager");
    let response_resource = resource(b"fault-renew-response-loss");
    let response_attempt = attempt();
    let mut guard = acquired(
        manager
            .acquire(
                response_resource.clone(),
                response_attempt,
                OperationId::new_v7(),
            )
            .await
            .expect("acquire renew response-loss lease"),
    );
    let operation_id = OperationId::new_v7();
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::LoseCommittedResponse)
        .await;
    clock.advance_wall(100);
    let mut renewal = Box::pin(guard.renew(operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut renewal => panic!("renewal returned before response loss: {result:?}"),
    }
    control.allow_provider_progress().await;
    control.release_response().await;
    renewal
        .await
        .expect("response-loss renewal resolves committed candidate");
    control.wait_inner_dropped().await;
    guard
        .recover_renew(operation_id)
        .await
        .expect("recover exact response-loss renewal");
    let mut superseding = acquired(
        manager
            .acquire(response_resource, response_attempt, OperationId::new_v7())
            .await
            .expect("load exact renewed guard"),
    );
    clock.advance_wall(100);
    superseding
        .renew(OperationId::new_v7())
        .await
        .expect("supersede committed renewal");
    assert_eq!(
        guard
            .recover_renew(operation_id)
            .await
            .expect_err("committed renewal was subsequently superseded")
            .kind(),
        CoordinationErrorKind::FenceLost
    );

    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap unresolved renew fixture");
    let clock = Arc::new(ManualLeaseClock::new(1_700_000, 26_000));
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"unresolved-renew-holder"),
        clock.clone(),
        lease_settings(),
    )
    .expect("unresolved renew manager");
    let mut guard = acquired(
        manager
            .acquire(
                resource(b"fault-renew-unresolved"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("acquire unresolved-renew lease"),
    );
    let operation_id = OperationId::new_v7();
    let transaction_id = derive_transaction_id(operation_id, 1);
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::CancelWaiterBeforeApply)
        .await;
    clock.advance_wall(100);
    let mut renewal = Box::pin(guard.renew(operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut renewal => panic!("renewal returned before unresolved probe: {result:?}"),
    }
    drop(renewal);
    control.wait_waiter_cancelled().await;
    let unresolved = guard
        .recover_renew(operation_id)
        .await
        .expect_err("unresolved mismatching renewal is uncertain");
    assert_eq!(unresolved.kind(), CoordinationErrorKind::CommitUncertain);
    assert_eq!(unresolved.transaction_id(), Some(transaction_id));
    assert_ne!(
        *guard.cancellation().borrow(),
        Some(LeaseCancellationReason::Released)
    );
    control.release_response().await;
    control.wait_inner_dropped().await;
    control.allow_provider_progress().await;
    let terminal = await_coordination_terminal(&fixture.store, transaction_id).await;
    match terminal {
        CommitResolution::Committed(_) => guard
            .recover_renew(operation_id)
            .await
            .expect("recover committed unresolved renewal"),
        CommitResolution::NotCommitted => {
            let error = guard
                .recover_renew(operation_id)
                .await
                .expect_err("recover noncommitted unresolved renewal");
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_eq!(error.transaction_id(), Some(transaction_id));
            clock.advance_wall(100);
            guard
                .renew(OperationId::new_v7())
                .await
                .expect("definite noncommit leaves renewal guard active");
        }
        CommitResolution::Unresolved => unreachable!("terminal helper excludes unresolved"),
    }
}

async fn release_fault_semantics(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap release response-loss fixture");
    let clock = Arc::new(ManualLeaseClock::new(1_800_000, 27_000));
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"fault-release-holder"),
        clock.clone(),
        lease_settings(),
    )
    .expect("release manager");
    let contender = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"fault-release-contender"),
        clock,
        lease_settings(),
    )
    .expect("release contender");
    let response_resource = resource(b"fault-release-response-loss");
    let mut guard = acquired(
        manager
            .acquire(response_resource.clone(), attempt(), OperationId::new_v7())
            .await
            .expect("acquire release response-loss lease"),
    );
    let operation_id = OperationId::new_v7();
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::LoseCommittedResponse)
        .await;
    let mut release = Box::pin(guard.release(operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut release => panic!("release returned before response loss: {result:?}"),
    }
    control.allow_provider_progress().await;
    control.release_response().await;
    release
        .await
        .expect("response-loss release resolves committed candidate");
    control.wait_inner_dropped().await;
    guard
        .recover_release(operation_id)
        .await
        .expect("recover exact response-loss release");
    let next = acquired(
        contender
            .acquire(response_resource, attempt(), OperationId::new_v7())
            .await
            .expect("supersede committed release"),
    );
    assert_eq!(next.token().resource_epoch().get(), 2);
    assert_eq!(
        guard
            .recover_release(operation_id)
            .await
            .expect_err("committed release was subsequently superseded")
            .kind(),
        CoordinationErrorKind::FenceLost
    );

    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap cancelled release fixture");
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"cancel-release-holder"),
        Arc::new(ManualLeaseClock::new(1_900_000, 28_000)),
        lease_settings(),
    )
    .expect("cancel release manager");
    let mut guard = acquired(
        manager
            .acquire(
                resource(b"fault-release-cancel"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("acquire cancelled-release lease"),
    );
    let operation_id = OperationId::new_v7();
    let transaction_id = derive_transaction_id(operation_id, 1);
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::CancelWaiterBeforeApply)
        .await;
    let mut release = Box::pin(guard.release(operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut release => panic!("release returned before cancellation: {result:?}"),
    }
    drop(release);
    control.wait_waiter_cancelled().await;
    let unresolved = guard
        .recover_release(operation_id)
        .await
        .expect_err("unresolved mismatching release is uncertain");
    assert_eq!(unresolved.kind(), CoordinationErrorKind::CommitUncertain);
    assert_eq!(unresolved.transaction_id(), Some(transaction_id));
    assert_ne!(
        *guard.cancellation().borrow(),
        Some(LeaseCancellationReason::Released)
    );
    control.release_response().await;
    control.wait_inner_dropped().await;
    control.allow_provider_progress().await;
    let terminal = await_coordination_terminal(&fixture.store, transaction_id).await;
    match terminal {
        CommitResolution::Committed(_) => guard
            .recover_release(operation_id)
            .await
            .expect("recover committed cancelled release"),
        CommitResolution::NotCommitted => {
            let error = guard
                .recover_release(operation_id)
                .await
                .expect_err("recover noncommitted cancelled release");
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_eq!(error.transaction_id(), Some(transaction_id));
            guard
                .release(OperationId::new_v7())
                .await
                .expect("definite noncommit leaves release guard active");
        }
        CommitResolution::Unresolved => unreachable!("terminal helper excludes unresolved"),
    }

    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap exact cancelled release fixture");
    let manager = LeaseManager::new(
        Arc::clone(&fixture.store),
        holder(b"exact-cancel-release-holder"),
        Arc::new(ManualLeaseClock::new(1_950_000, 28_500)),
        lease_settings(),
    )
    .expect("exact cancel release manager");
    let mut guard = acquired(
        manager
            .acquire(
                resource(b"fault-release-exact-cancel"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("acquire exact cancelled-release lease"),
    );
    let operation_id = OperationId::new_v7();
    let transaction_id = derive_transaction_id(operation_id, 1);
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::CancelWaiterBeforeApply)
        .await;
    let mut release = Box::pin(guard.release(operation_id));
    tokio::select! {
        () = control.wait_dispatched() => {}
        result = &mut release => panic!("release returned before exact cancellation: {result:?}"),
    }
    drop(release);
    control.wait_waiter_cancelled().await;
    control.release_response().await;
    control.wait_inner_dropped().await;
    control.allow_provider_progress().await;
    let terminal = await_coordination_terminal(&fixture.store, transaction_id).await;
    match terminal {
        CommitResolution::Committed(_) => guard
            .recover_release(operation_id)
            .await
            .expect("recover committed exact cancelled release"),
        CommitResolution::NotCommitted => {
            let error = guard
                .recover_release(operation_id)
                .await
                .expect_err("recover noncommitted exact cancelled release");
            assert_eq!(error.kind(), CoordinationErrorKind::OperationNotCommitted);
            assert_eq!(error.transaction_id(), Some(transaction_id));
        }
        CommitResolution::Unresolved => unreachable!("terminal helper excludes unresolved"),
    }
}

pub async fn stable_operation_fault_matrix(factory: &StateStoreFactory) {
    commit_unknown_uses_authoritative_read_back(factory).await;
    cancelled_mutation_recovers_with_same_operation(factory).await;
    unresolved_bootstrap_without_visible_record_is_uncertain(factory).await;
    recovery_is_operation_scoped(factory).await;
    acquire_fault_semantics(factory).await;
    cancelled_renew_recovers_with_exact_pending_candidate(factory).await;
    renew_fault_semantics(factory).await;
    release_fault_semantics(factory).await;
}

async fn contention_round(
    store: Arc<dyn StateStore>,
    clock: Arc<ManualLeaseClock>,
    resource: ResourceKey,
    round: usize,
) -> LeaseGuard {
    let mut contenders = tokio::task::JoinSet::new();
    for contender in 0..8 {
        let manager = LeaseManager::new(
            Arc::clone(&store),
            HolderId::try_from(Bytes::from(format!(
                "contention-{round}-holder-{contender}"
            )))
            .expect("dynamic contention holder"),
            clock.clone(),
            lease_settings(),
        )
        .expect("contention manager");
        let resource = resource.clone();
        contenders.spawn(async move {
            manager
                .acquire(resource, attempt(), OperationId::new_v7())
                .await
        });
    }
    let mut winner = None;
    let mut contended = 0;
    while let Some(result) = contenders.join_next().await {
        match result
            .expect("join contention contender")
            .expect("contention contender reaches a typed outcome")
        {
            AcquireOutcome::Acquired(guard) => {
                assert!(winner.replace(guard).is_none(), "only one lease winner");
            }
            AcquireOutcome::Contended(_) => contended += 1,
            AcquireOutcome::AwaitingTakeover(_) => {
                panic!("fresh/released resource must not require takeover observation")
            }
        }
    }
    assert_eq!(contended, 7);
    winner.expect("one high-contention winner")
}

pub async fn high_contention_is_monotonic(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    IncarnationGate::new(Arc::clone(&fixture.store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap high-contention fixture");
    let clock = Arc::new(ManualLeaseClock::new(2_000_000, 29_000));
    let resource = resource(b"high-contention-monotonic");
    let mut first = contention_round(
        Arc::clone(&fixture.store),
        clock.clone(),
        resource.clone(),
        1,
    )
    .await;
    assert_eq!(first.token().resource_epoch().get(), 1);
    first
        .release(OperationId::new_v7())
        .await
        .expect("release first contention winner");
    let second = contention_round(Arc::clone(&fixture.store), clock, resource, 2).await;
    assert_eq!(second.token().resource_epoch().get(), 2);
}

pub async fn opaque_provider_name_has_no_branch(factory: &StateStoreFactory) {
    let fixture = open_fixture(factory).await;
    let store: Arc<dyn StateStore> = Arc::new(OpaqueProviderStore {
        inner: Arc::clone(&fixture.store),
    });
    assert_eq!(store.provider_name(), "opaque-test-provider");
    IncarnationGate::new(Arc::clone(&store))
        .bootstrap(OperationId::new_v7())
        .await
        .expect("bootstrap through opaque provider name");
    let manager = LeaseManager::new(
        store,
        holder(b"opaque-provider-holder"),
        Arc::new(ManualLeaseClock::new(2_100_000, 30_000)),
        lease_settings(),
    )
    .expect("opaque provider manager");
    let mut guard = acquired(
        manager
            .acquire(
                resource(b"opaque-provider-resource"),
                attempt(),
                OperationId::new_v7(),
            )
            .await
            .expect("acquire through opaque provider name"),
    );
    guard
        .renew(OperationId::new_v7())
        .await
        .expect("renew through opaque provider name");
    guard
        .release(OperationId::new_v7())
        .await
        .expect("release through opaque provider name");
}

pub async fn overflow_and_corruption_fail_closed(factory: &StateStoreFactory) {
    incarnation_overflow_fails_closed(factory).await;
    identity_mismatch_is_corruption(factory).await;
}

pub async fn run_coordination_conformance(factory: StateStoreFactory) {
    incarnation_gate_lifecycle(&factory).await;
    concurrent_bootstrap_converges(&factory).await;
    basic_acquire_contention_and_high_watermark(&factory).await;
    concurrent_acquire_exactly_one_winner(&factory).await;
    lease_expiry_observation(&factory).await;
    renew_resets_observation(&factory).await;
    lease_lifecycle_and_cancellation(&factory).await;
    stale_fence_finalize(&factory).await;
    early_takeover_is_fenced(&factory).await;
    stable_operation_fault_matrix(&factory).await;
    external_lease_clock_error_is_clock_unsafe(&factory).await;
    restore_invalidates_old_tokens(&factory).await;
    overflow_and_corruption_fail_closed(&factory).await;
    high_contention_is_monotonic(&factory).await;
    opaque_provider_name_has_no_branch(&factory).await;
}
