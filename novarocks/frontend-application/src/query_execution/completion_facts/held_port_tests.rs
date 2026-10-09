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

use super::*;
use novarocks_catalog_application::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogCreateCommand,
    CatalogDropCommand, CatalogRuntimeObservation,
};
use novarocks_mv_application::persistence::dependency::StoredMvDependency;
use novarocks_mv_application::product::MvTarget;
use novarocks_mv_application::repository::*;
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorStopOwner};
use novarocks_workload_control::{CancellationReason, ResultWindowClass};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant};

struct ActualPortDependency {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    drop_started: mpsc::Sender<()>,
    drop_release: Mutex<mpsc::Receiver<()>>,
    calls: Arc<AtomicUsize>,
    other_calls: Arc<AtomicUsize>,
    exact: Arc<AtomicBool>,
    returned: Arc<AtomicBool>,
    destroyed: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}
impl ActualPortDependency {
    fn enter(&self) {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let _ = self.started.send(());
        let _ = self.release.lock().unwrap().recv();
        self.returned.store(true, Ordering::Release);
    }
    fn destroy(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        let _ = self.drop_started.send(());
        let _ = self.drop_release.get_mut().unwrap().recv();
        self.destroyed.store(true, Ordering::Release);
    }
}

struct HeldCatalogApplication(ActualPortDependency);
impl Drop for HeldCatalogApplication {
    fn drop(&mut self) {
        self.0.destroy();
    }
}
impl CatalogApplicationPort for HeldCatalogApplication {
    fn admit_catalog(&self, instance: &ConnectorInstanceId) -> CatalogAdmission {
        self.0
            .exact
            .store(instance.as_str() == "ice.main", Ordering::Release);
        self.0.enter();
        CatalogAdmission::Unavailable {
            reason: "actual held admission is unavailable".into(),
        }
    }
    fn create_catalog(
        &self,
        _: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        self.0.other_calls.fetch_add(1, Ordering::AcqRel);
        Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            "readonly fixture cannot create a catalog",
        ))
    }
    fn drop_catalog(&self, _: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
        self.0.other_calls.fetch_add(1, Ordering::AcqRel);
        Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            "readonly fixture cannot drop a catalog",
        ))
    }
}

// The existing public in-memory repository implements the unrelated required
// methods. Only the actual read port under test is overridden; no mutation runs.
struct HeldMvRepository {
    inner: novarocks_mv_application::test_repository::InMemoryMvRepository,
    held: ActualPortDependency,
}
impl Drop for HeldMvRepository {
    fn drop(&mut self) {
        self.held.destroy();
    }
}
#[async_trait]
impl MvRepository for HeldMvRepository {
    async fn list_projections(&self) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        self.held.exact.store(true, Ordering::Release);
        self.held.enter();
        Err(MvRepositoryError::new(
            MvRepositoryErrorKind::Unavailable,
            "actual held candidate inventory is unavailable",
        ))
    }
    async fn create_projection(
        &self,
        id: uuid::Uuid,
        value: MvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.create_projection(id, value).await
    }
    async fn replace_projection(
        &self,
        id: uuid::Uuid,
        value: ReplaceMvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.replace_projection(id, value).await
    }
    async fn load_by_id(&self, id: i64) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.load_by_id(id).await
    }
    async fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.find_by_target(target).await
    }
    async fn list_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.list_projection_inventory(bound).await
    }
    async fn find_by_target_bounded(
        &self,
        target: &MvTarget,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.find_by_target_bounded(target, bound).await
    }
    async fn delete_projection(
        &self,
        id: uuid::Uuid,
        value: DeleteMvProjectionRequest,
    ) -> Result<bool, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.delete_projection(id, value).await
    }
    async fn wipe_projection_by_target(
        &self,
        id: uuid::Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.wipe_projection_by_target(id, target).await
    }
    async fn wipe_accelerator(&self, id: uuid::Uuid) -> Result<(), MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.wipe_accelerator(id).await
    }
    async fn list_dependencies_by_downstream(
        &self,
        id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.list_dependencies_by_downstream(id).await
    }
    async fn list_dependencies_by_downstream_bounded(
        &self,
        id: i64,
        version: &MvProjectionVersion,
        bound: MvDependencyReadBound,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner
            .list_dependencies_by_downstream_bounded(id, version, bound)
            .await
    }
    async fn list_downstream_dependencies(
        &self,
        upstream: &novarocks_mv_application::dependency::MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.list_downstream_dependencies(upstream).await
    }
    async fn ensure_no_downstream_dependencies(
        &self,
        upstream: &novarocks_mv_application::dependency::MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        self.held.other_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.ensure_no_downstream_dependencies(upstream).await
    }
}

#[derive(Clone, Copy)]
enum ActualCompletionPort {
    Catalog,
    MaterializedView,
}
fn observe_actual_completion_port(kind: ActualCompletionPort, class: ResultWindowClass) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let blocking = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
    let (control, root, window) = crate::task_execution::blocking_io::tests::admitted_class(class);
    let capacity =
        QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let other_calls = Arc::new(AtomicUsize::new(0));
    let exact = Arc::new(AtomicBool::new(false));
    let returned = Arc::new(AtomicBool::new(false));
    let destroyed = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicBool::new(false));
    let (started, entered) = mpsc::channel();
    let (release_call, call_held) = mpsc::channel();
    let (drop_started, drop_entered) = mpsc::channel();
    let (release_drop, drop_held) = mpsc::channel();
    let (disarm, watch) = mpsc::channel();
    let rescue_call = release_call.clone();
    let rescue_drop = release_drop.clone();
    let watchdog = std::thread::spawn(move || {
        if watch.recv_timeout(Duration::from_secs(6)).is_err() {
            let _ = rescue_call.send(());
            let _ = rescue_drop.send(());
        }
    });
    let original = ActualPortDependency {
        started,
        release: Mutex::new(call_held),
        drop_started,
        drop_release: Mutex::new(drop_held),
        calls: calls.clone(),
        other_calls: other_calls.clone(),
        exact: exact.clone(),
        returned: returned.clone(),
        destroyed: destroyed.clone(),
        drops: drops.clone(),
    };
    // Move the only external strong dependency owner into the actual owners.
    // Neither the test probes nor watchdog retain a dependency/window alias.
    let (application, repository): (
        Option<Arc<dyn CatalogApplicationPort>>,
        Arc<dyn MvRepository>,
    ) = match kind {
        ActualCompletionPort::Catalog => (
            Some(Arc::new(HeldCatalogApplication(original))),
            Arc::new(novarocks_mv_application::test_repository::InMemoryMvRepository::default()),
        ),
        ActualCompletionPort::MaterializedView => (
            None,
            Arc::new(HeldMvRepository {
                inner: Default::default(),
                held: original,
            }),
        ),
    };
    let inventory = novarocks_mv_application::readiness::MvReadinessService::new(
        repository,
        Arc::new(novarocks_mv_application::process_runtime::ProcessRuntime::default()),
    );
    let owners = CompletionFactOwners::new(
        Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
        application,
        Arc::new(ConnectorControlHost::new()),
        Arc::new(UnifiedStatisticsResolver::default()),
        MvCandidateReader::new(inventory.candidate_reader(), runtime.handle().clone()),
        Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
        blocking.clone(),
    );
    drop(inventory); // The actual port/closure now owns the only candidate reader.
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(10),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let scope = StatementFactScope::new(
        bindings.clone(),
        context,
        Some("ice.main"),
        &root.owner.scope(),
        capacity.clone(),
    )
    .unwrap();
    let published = delivered.clone();
    let mut waiter = runtime.spawn(async move {
        match kind {
            ActualCompletionPort::Catalog => {
                let need = novarocks_sql::compiler::fixtures::catalog_relation_need(
                    1,
                    novarocks_types::naming::TableIdentity::new("ice.main", "db", "orders"),
                    CatalogLookupTarget::Table {
                        mode: novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
                    },
                )
                .unwrap();
                let answer = FrontendCatalogFacts::new(owners, scope)
                    .resolve_relations(&[need])
                    .await;
                drop(answer);
            }
            ActualCompletionPort::MaterializedView => {
                let need = novarocks_sql::compiler::fixtures::materialized_view_need(
                    1,
                    vec![novarocks_types::naming::TableIdentity::new(
                        "ice.main", "db", "orders",
                    )],
                )
                .unwrap();
                let answer = FrontendMaterializedViewFacts::new(owners, scope)
                    .resolve_materialized_views(&[need])
                    .await;
                drop(answer);
            }
        }
        published.store(true, Ordering::Release);
    });
    let call_entered = entered.recv_timeout(Duration::from_secs(2)).is_ok();
    stop.request_stop();
    root.owner.cancel(CancellationReason::Requested);
    control.expire_deadlines();
    waiter.abort();
    let joined =
        runtime.block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut waiter).await });
    let cancelled = matches!(joined, Ok(Err(ref e)) if e.is_cancelled());
    drop(joined);
    drop(waiter);
    drop(capacity);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    drop(window);
    let during_call = control.snapshot();
    let _ = release_call.send(());
    let dependency_drop_entered = drop_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let during_drop = control.snapshot();
    let dependency_still_held = !destroyed.load(Ordering::Acquire);
    let callback_returned = returned.load(Ordering::Acquire);
    let bindings_unchanged = bindings.captured_bindings().is_empty();
    // No assert may leave the actual callback/dependency/runtime held.
    let _ = release_drop.send(());
    let _ = disarm.send(());
    let watchdog_joined = watchdog.join().is_ok();
    let deadline = Instant::now() + Duration::from_secs(2);
    let after = loop {
        let facts = control.snapshot();
        if facts.scopes.is_empty() || Instant::now() >= deadline {
            break facts;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    control.close_admission();
    let shutdown = control.shutdown();
    if let Some(error) = blocking.take_original_failure() {
        blocking.retire_original_failure(error);
    }
    drop(bindings);
    drop(blocking);
    drop(runtime);
    assert!(
        call_entered
            && cancelled
            && dependency_drop_entered
            && dependency_still_held
            && callback_returned
            && watchdog_joined
    );
    let expected = if class == ResultWindowClass::Local {
        [0, 1, 0, 0]
    } else {
        [0, 0, 1, 0]
    };
    for facts in [&during_call, &during_drop] {
        assert_eq!(facts.result_windows.held_positions, expected);
        assert_eq!(facts.root_responsibilities, 1);
        assert!(
            facts
                .scopes
                .iter()
                .any(|s| s.parent.is_some() && !s.own_completed),
            "actual completion port omitted its original admitted child"
        );
    }
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(other_calls.load(Ordering::Acquire), 0);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(exact.load(Ordering::Acquire) && destroyed.load(Ordering::Acquire));
    assert!(bindings_unchanged && !delivered.load(Ordering::Acquire));
    assert!(
        after.scopes.is_empty()
            && after.result_windows.held_positions == [0; 4]
            && shutdown.is_ok()
    );
}

#[test]
fn actual_catalog_admit_keeps_local_window_through_waiter_cancel_and_dependency_drop() {
    observe_actual_completion_port(ActualCompletionPort::Catalog, ResultWindowClass::Local);
}
#[test]
fn actual_catalog_admit_keeps_internal_window_through_waiter_cancel_and_dependency_drop() {
    observe_actual_completion_port(ActualCompletionPort::Catalog, ResultWindowClass::Internal);
}
#[test]
fn actual_mv_list_projections_keeps_local_window_through_waiter_cancel_and_repository_drop() {
    observe_actual_completion_port(
        ActualCompletionPort::MaterializedView,
        ResultWindowClass::Local,
    );
}
#[test]
fn actual_mv_list_projections_keeps_internal_window_through_waiter_cancel_and_repository_drop() {
    observe_actual_completion_port(
        ActualCompletionPort::MaterializedView,
        ResultWindowClass::Internal,
    );
}
