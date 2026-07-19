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
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, ClockHealth, ControlPlaneMode, CoordinationError,
    CoordinationErrorKind, CoordinationOperation, CoordinationOutcome, HolderId, IncarnationGate,
    LeaseClock, LeaseGuard, LeaseManager, LeaseSettings, ResourceKey,
};
use novarocks_state_store::{
    ChangePage, ChangePollRequest, CommitOutcome, CommitResolution, Key, OperationId, Precondition,
    RangePage, RangeRequest, ReadTransaction, STATE_STORE_OUTCOME_COUNT, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
    StateStoreOperation, StoreIdentity, TransactionId, Value, WriteTransaction,
    derive_transaction_id,
};
use tokio::sync::Barrier;
use uuid::Uuid;

use super::state_store_conformance::{
    PostDispatchScenario, StateStoreConformanceFixture, StateStoreFactory,
};

const CONTROL_KEY: &[u8] = b"\0novarocks/cp/v1/control";

pub struct ManualLeaseClock {
    wall_ms: AtomicU64,
    monotonic_ms: AtomicU64,
    health: AtomicU8,
}

impl ManualLeaseClock {
    pub fn new(wall_ms: u64, monotonic_ms: u64) -> Self {
        Self {
            wall_ms: AtomicU64::new(wall_ms),
            monotonic_ms: AtomicU64::new(monotonic_ms),
            health: AtomicU8::new(0),
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
}

impl LeaseClock for ManualLeaseClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        Ok(self.wall_ms.load(Ordering::SeqCst))
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

async fn open_fixture(factory: &StateStoreFactory) -> StateStoreConformanceFixture {
    factory().await.expect("open coordination fixture")
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
