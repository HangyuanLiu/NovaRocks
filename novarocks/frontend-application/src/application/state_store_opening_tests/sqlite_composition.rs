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

//! Actual SQLite composition through the production FA original receivers.
//! This exercises the GC observation accelerator, not Iceberg provenance/CAS
//! discovery or the TableMaintenanceProduct selection/session boundary.

use super::*;
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use novarocks_resource_contract::SharedDependency;
use novarocks_state_store_api::{StateStoreLimits, StateStoreProviderFactory};
use novarocks_state_store_runtime::{
    StateStoreHostInput, StateStoreProviderRegistration, StateStoreProviderRegistry,
};
use novarocks_state_store_sqlite::{
    SQLITE_STATE_STORE_PROVIDER_ID, SqliteStateStoreProviderFactory,
};
use novarocks_table_maintenance::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
};
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, RootWork, WorkClass, WorkOwner, WorkRequest,
    WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver, ResponsibilityConfig},
};
use std::{future::Future, path::Path, task::Poll};

fn prepare_sqlite(path: &Path) -> FrontendStateStoreOpening {
    // Register the actual selected provider. No persistent-memory substitute
    // and no legacy open() can satisfy this prepared/original receiver.
    let path = path.to_owned();
    let descriptor = SqliteStateStoreProviderFactory::new(path.clone())
        .descriptor()
        .to_owned();
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(descriptor, move |_| {
            Ok(Box::new(SqliteStateStoreProviderFactory::new(path.clone())))
        }))
        .unwrap();
    let prepared = StateStoreHost::prepare_original_open(
        &registry,
        StateStoreHostInput {
            cluster_id: "actual-sqlite-original-composition".into(),
            provider_id: SQLITE_STATE_STORE_PROVIDER_ID,
            limits: StateStoreLimits::default(),
            run_policy: StateStoreRunPolicy::default(),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(
        prepared.original_blocking_dependency_facts(),
        &[SharedDependency::LocalHost]
    );
    FrontendStateStoreOpening::new(prepared)
}

async fn open_sqlite(
    opening: &mut FrontendStateStoreOpening,
    executor: &QueryBlockingExecutor,
    ready: &mut Option<StateStoreHost>,
) {
    opening.start(executor).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| opening.poll_open(cx, ready)),
    )
    .await
    .expect("actual SQLite Open phase and QA footer")
    .unwrap();
    assert_eq!(
        ready.as_ref().unwrap().original_blocking_dependency_facts(),
        Some([SharedDependency::LocalHost].as_slice())
    );
}

fn original_root() -> (WorkloadControl, JointProgressDriver, RootWork) {
    let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 4,
        business_limit: 4,
        scope_records_limit: 4,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(1).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(4, &[]))
        .unwrap();
    control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    (control, driver, root)
}

fn drive_one(driver: &mut JointProgressDriver, cx: &Context<'_>) {
    if matches!(driver.step(), JointProgress::More) {
        cx.waker().wake_by_ref();
    }
}

// Borrow the real production inventory and driver; this helper implements no
// provider phase, receiving, retry, publication, or physical-exit algorithm.
async fn with_original_progress<F: Future>(
    original: &StateStoreOriginalOwner,
    io: &Arc<MetadataIoAdmission>,
    root: &WorkOwner,
    driver: &mut JointProgressDriver,
    future: F,
) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            original.poll_progress(cx).unwrap();
            io.poll_progress(root, cx).unwrap();
            let result = future.as_mut().poll(cx);
            drive_one(driver, cx);
            result
        }),
    )
    .await
    .expect("same Root/provider/ordinary queue made progress")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_sqlite_original_observation_and_pinned_host_close_share_production_receivers() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actual-original.sqlite");
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let executor = workers.executor();
    // Stable receiving precedes the first borrowed Open poll.
    let mut opening = prepare_sqlite(&path);
    let mut ready = None;
    open_sqlite(&mut opening, &executor, &mut ready).await;
    let host = ready.as_ref().unwrap();
    let (vocabulary, instance) = host.original_blocking_sources().unwrap();
    let dependencies = host.original_blocking_dependency_facts().unwrap().to_vec();
    let store = host.state_store().unwrap();
    let policy = host.run_policy();
    let (_control, mut driver, root) = original_root();
    let io = MetadataIoAdmission::for_original_source(&root.owner, dependencies).unwrap();
    let mut original = StateStoreOriginalOwner::for_root(
        &executor,
        vocabulary,
        instance,
        root.owner.scope(),
        policy,
    )
    .unwrap();
    let context = original.context(Instant::now() + Duration::from_secs(5));
    with_original_progress(
        &original,
        &io,
        &root.owner,
        &mut driver,
        std::future::poll_fn(|cx| {
            if io.poll_progress(&root.owner, cx).unwrap() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await;
    let observations = GcOwnedRefObservationAccelerator::open(store.clone(), policy)
        .await
        .unwrap();
    // These are accelerator input facts, not invented proof that Iceberg
    // discovered a candidate or successfully changed a catalog ref.
    let observation = GcOwnedRefObservation::try_new(
        uuid::Uuid::from_u128(0x73716c697465),
        "__novarocks_owned_ref_component".into(),
        101,
        2,
        [7; 32],
        1,
    )
    .unwrap();
    let first = with_original_progress(
        &original,
        &io,
        &root.owner,
        &mut driver,
        observations.observe_in(&context, observation.clone(), 1_000, 500),
    )
    .await
    .unwrap();
    assert_eq!(
        first,
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 1_000
        }
    );
    let second = with_original_progress(
        &original,
        &io,
        &root.owner,
        &mut driver,
        observations.observe_in(&context, observation, 1_501, 500),
    )
    .await
    .unwrap();
    assert_eq!(
        second,
        GcOwnedRefObservationDecision::Mature {
            first_observed_at_ms: 1_000
        }
    );
    // Business completion does not retire serial transaction/evidence phases.
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            io.poll_progress(&root.owner, cx).unwrap();
            let result = original.poll_closed(cx);
            drive_one(&mut driver, cx);
            result
        }),
    )
    .await
    .expect("both actual transaction/evidence tails exited")
    .unwrap();
    assert!(original.local_health_failure().is_none());
    drop(context);
    drop(original);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            let result = io.poll_quiescent(&root.owner, cx);
            drive_one(&mut driver, cx);
            result
        }),
    )
    .await
    .expect("same Root metadata source actually quiesced")
    .unwrap();
    drop(io);
    root.owner.complete();
    root.business.release();
    // This real instance-issued observation pins the real evidence connection
    // and file lifetime. It has never been dispatched, so cancellation is exact.
    let (attempt, pin) = store.attempts().reserve().unwrap();
    assert_eq!(attempt.id().scope(), instance);
    attempt.cancel_before_dispatch().unwrap();
    drop(attempt);
    drop(observations);
    drop(store);
    let deadline = Instant::now() + Duration::from_secs(5);
    let held = tokio::time::timeout(
        Duration::from_millis(100),
        std::future::poll_fn(|cx| opening.poll_close(&mut ready, &executor, deadline, cx)),
    )
    .await;
    assert!(
        held.is_err(),
        "an actual Observation still pins SQLite close"
    );
    assert!(
        ready.is_some(),
        "timeout cannot clear the actual Host receiver"
    );
    assert!(opening.instance_close.as_ref().unwrap().original.is_some());
    assert_eq!(
        ready.as_ref().unwrap().lifecycle(),
        novarocks_state_store_runtime::StateStoreHostLifecycle::Draining
    );
    // Release only AFTER the resumed borrower is already Pending. The
    // observer cannot poll Source or notify its waker; no hand-authored receipt
    // or manual repoll can substitute for the owner's policy timer.
    let (release_tx, release_rx) = mpsc::channel();
    let observer = std::thread::spawn(move || {
        let waiting = release_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        std::thread::sleep(Duration::from_millis(50));
        drop(pin);
        waiting
    });
    let mut release_tx = Some(release_tx);
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            let result = opening.poll_close(&mut ready, &executor, deadline, cx);
            if result.is_pending() {
                if let Some(notice) = release_tx.take() {
                    notice.send(()).unwrap();
                }
            }
            result
        }),
    )
    .await;
    let released_after_pending = observer.join().unwrap();
    assert!(
        released_after_pending,
        "pin release followed a real Pending poll"
    );
    closed
        .expect("original owner backoff completed Store/Evidence/File close")
        .unwrap();
    assert!(
        ready.is_none(),
        "receiver and QA footers precede Host removal"
    );
    assert!(opening.original_failure().is_none());
    // Same-path actual OriginalOpen proves the process-exclusive file lease
    // was released. A legacy open or a new in-memory store cannot prove this.
    let mut reopened = prepare_sqlite(&path);
    open_sqlite(&mut reopened, &executor, &mut ready).await;
    let (_, new_instance) = ready.as_ref().unwrap().original_blocking_sources().unwrap();
    assert_ne!(new_instance, instance);
    let reopened_deadline = Instant::now() + Duration::from_secs(5);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            reopened.poll_close(&mut ready, &executor, reopened_deadline, cx)
        }),
    )
    .await
    .expect("reopened actual instance closed")
    .unwrap();
    assert!(ready.is_none());
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .expect("same ordinary worker actually joined");
}
