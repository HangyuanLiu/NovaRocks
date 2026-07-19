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

use bytes::Bytes;
use novarocks_state_store::coordination::{
    ControlPlaneMode, CoordinationErrorKind, IncarnationGate,
};
use novarocks_state_store::{
    CommitOutcome, CommitResolution, Key, OperationId, Precondition, StateStore, TransactionId,
    Value, derive_transaction_id,
};
use uuid::Uuid;

use super::state_store_conformance::{
    PostDispatchScenario, StateStoreConformanceFixture, StateStoreFactory,
};

const CONTROL_KEY: &[u8] = b"\0novarocks/cp/v1/control";

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
