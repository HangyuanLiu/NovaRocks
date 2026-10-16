// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Readiness-aware access to the rebuildable MV Accelerator.
//!
//! Consumers use this façade instead of reading StateStore directly.  An
//! unavailable target never falls back to a retained projection, so SHOW,
//! rewrite, and scheduling cannot accidentally consume stale lake facts.

use novarocks_mv_application::activity::CanonicalMvTarget;
use novarocks_mv_application::readiness::{
    MvCurrentProjectionRequest, MvKeyedCandidateReader as ProductCandidateReader,
    MvKeyedCurrentProjectionSource, MvKeyedDropReadiness, MvKeyedProjectionDeleteGuard,
    MvKeyedProjectionInstallOutcome, MvKeyedReadOnlyCurrentProjectionSource,
    MvKeyedReadinessService, MvProjectionError,
};
#[cfg(feature = "test-controls")]
use std::sync::Arc;
use uuid::Uuid;

use crate::mv::activity::canonical_mv_target;
use novarocks_mv_application::dependency::MvDependencyObjectIdentity;
use novarocks_mv_application::persistence::dependency::StoredMvDependency;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::repository::MvRepositoryError;
use novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

/// The synchronous face of an asynchronous MV projection store.
///
/// The repository contract is async because its work is durable I/O. Almost
/// every reader of MV readiness, though, sits inside the `spawn_blocking`
/// closure that runs SQL statements, or on one of the two dedicated maintenance
/// threads. Propagating `await` through them would not remove the bridge — it
/// would move it onto a thread that has to block anyway — and would further
/// require restructuring statement admission, which belongs to the
/// query-application and query-preparation work lines rather than here.
///
/// So the adaptation lives in this one type, whose job is adaptation, and in no
/// domain contract. It is a single place to delete once those work lines lift
/// the surrounding boundaries.
pub struct MvReadinessPort {
    service: MvKeyedReadinessService,
    management: std::sync::Arc<novarocks_mv_application::management::ManagementEntrance>,
    handle: tokio::runtime::Handle,
    #[cfg(feature = "test-controls")]
    controls: Option<Arc<crate::mv::test_controls::MvTestControls>>,
}

pub(crate) struct MvLocalProjectionInventory {
    inventory: novarocks_mv_application::readiness::MvBoundedKeyedProjectionInventory,
    handle: tokio::runtime::Handle,
}

impl MvLocalProjectionInventory {
    pub(crate) fn order_by_namespace_and_name(&mut self) {
        self.inventory.order_by_namespace_and_name();
    }

    fn block_on<T>(
        handle: &tokio::runtime::Handle,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(future)),
            Err(_) => handle.block_on(future),
        }
    }

    pub(crate) fn next_ready(
        &mut self,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        Self::block_on(&self.handle, self.inventory.next_ready())
    }

    pub(crate) fn next_listable(
        &mut self,
    ) -> Result<
        Option<novarocks_mv_application::readiness::ListedKeyedMvProjection>,
        MvRepositoryError,
    > {
        Self::block_on(&self.handle, self.inventory.next_listable())
    }
}

fn local_inventory_bound() -> novarocks_mv_application::repository::MvProjectionInventoryBound {
    use novarocks_mv_application::repository::MvProjectionInventoryBound;
    let bound = MvProjectionInventoryBound {
        entries: novarocks_query_application::api::LocalResultBound::V1.rows,
        snapshot_bytes: 16 * 1024 * 1024,
        raw_page_bytes: 1024 * 1024,
        single_name_bytes: 65_536,
        continuation_token_bytes: 4096,
        decode: novarocks_mv_application::persistence::validation::PersistenceDecodeBudget {
            max_document_bytes: 1024 * 1024,
            max_working_set_bytes: 4 * 1024 * 1024,
            ..Default::default()
        },
    };
    bound
}

/// Read-only inventory for query-local MV candidate discovery.
///
/// A query freezes and validates every returned lake publication before it can
/// substitute a scan. It therefore must not inherit the refresh executor's
/// process-local readiness state: that state controls effect-capable refresh
/// and DDL consumers, not whether a retained candidate can be independently
/// proven against its exact publication.
#[derive(Clone)]
pub(crate) struct MvCandidateReader {
    reader: ProductCandidateReader,
    service: MvKeyedReadinessService,
    management: std::sync::Arc<novarocks_mv_application::management::ManagementEntrance>,
    handle: tokio::runtime::Handle,
}

impl MvReadinessPort {
    /// Construct the role-local synchronous adapter from the already-owned
    /// product readiness service. This adapter cannot create an independent
    /// repository/runtime state pair.
    pub(crate) fn from_product(
        service: MvKeyedReadinessService,
        handle: tokio::runtime::Handle,
        management: std::sync::Arc<novarocks_mv_application::management::ManagementEntrance>,
    ) -> Self {
        Self {
            service,
            management,
            handle,
            #[cfg(feature = "test-controls")]
            controls: None,
        }
    }

    #[cfg(feature = "test-controls")]
    pub(crate) fn with_test_controls(
        mut self,
        controls: Arc<crate::mv::test_controls::MvTestControls>,
    ) -> Self {
        self.controls = Some(controls);
        self
    }

    #[cfg(feature = "test-controls")]
    pub(crate) fn test_controls(&self) -> Option<&crate::mv::test_controls::MvTestControls> {
        self.controls.as_deref()
    }

    #[cfg(feature = "test-controls")]
    pub(crate) fn test_controls_owner(
        &self,
    ) -> Option<Arc<crate::mv::test_controls::MvTestControls>> {
        self.controls.clone()
    }

    /// Construct the separate query inventory from the same product-owned
    /// repository. The reader intentionally does not inherit this port's
    /// process-local readiness or refresh authority.
    /// The runtime this port defers blocking work to.
    pub(crate) fn runtime(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    pub(crate) fn candidate_reader(&self) -> MvCandidateReader {
        MvCandidateReader {
            reader: self.service.candidate_reader(),
            service: self.service.clone(),
            management: self.management.clone(),
            handle: self.handle.clone(),
        }
    }

    /// Drives one durable MV operation from a synchronous caller. Confined to
    /// this type on purpose; see the type-level note.
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| self.handle.block_on(future)),
            Err(_) => self.handle.block_on(future),
        }
    }

    pub(crate) fn observe_current_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.block_on(
            self.service
                .observe_and_install(operation_id, request, source),
        )
    }

    pub(crate) fn observe_current_read_only_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedReadOnlyCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.block_on(self.service.observe_current_read_only_and_install(
            operation_id,
            request,
            source,
            "read-only lake observation requires management readmission".to_string(),
        ))
    }

    pub(crate) fn quarantine(
        &self,
        target: CanonicalMvTarget,
        reason: String,
    ) -> Result<(), MvProjectionError> {
        self.block_on(self.service.invalidate_current(target, reason))
    }

    /// Isolate every retained projection in one catalog after an incomplete
    /// enumeration.  This is intentionally process-local: a later complete
    /// lake observation is the only operation that may make a target ready.
    pub(crate) fn quarantine_catalog(
        &self,
        catalog: &str,
        reason: String,
    ) -> Result<(), MvProjectionError> {
        let projections =
            self.block_on(self.service.candidate_reader().list_candidate_definitions())?;
        for projection in projections {
            if projection.facts.target().catalog() == Some(catalog) {
                self.block_on(
                    self.service
                        .invalidate_current(projection.facts.target().clone(), reason.clone()),
                )?;
            }
        }
        Ok(())
    }

    /// Whether plain SQL may read one MV target's storage. See
    /// [`novarocks_mv_application::readiness::MvKeyedReadinessService::query_admission`].
    pub(crate) fn query_admission(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
    ) -> Result<novarocks_mv_application::readiness::MvQueryAdmission, MvRepositoryError> {
        let target = novarocks_mv_application::product::MvTarget::from_parts(
            target.catalog.as_deref(),
            &target.database,
            &target.name,
        );
        self.block_on(self.service.query_admission(&target))
    }

    pub(crate) fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.block_on(
            self.service
                .load_ready_settled(&canonical_mv_target(target)),
        )
    }

    /// Local-source profile: one finite locator snapshot and one decoded
    /// page. Planning/background callers keep their own inventory contract.
    pub(crate) fn local_projection_inventory(
        &self,
    ) -> Result<MvLocalProjectionInventory, MvRepositoryError> {
        let bound = local_inventory_bound();
        let inventory = self.block_on(self.service.bounded_projection_inventory(bound))?;
        Ok(MvLocalProjectionInventory {
            inventory,
            handle: self.handle.clone(),
        })
    }

    /// Enumerate only projections whose current-process readiness permits
    /// consumption.  A catalog/package observation failure therefore removes
    /// exactly that target from SHOW, rewrite, scheduler and maintenance
    /// candidates instead of reviving a retained StateStore projection.
    pub(crate) fn list_ready_projections(
        &self,
    ) -> Result<Vec<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.block_on(self.service.list_ready_projections())
    }

    /// Dependency reads are tied to a ready downstream projection.  Callers
    /// cannot accidentally pair a live dependency index with a quarantined
    /// lake package.
    pub(crate) fn list_ready_dependencies_by_downstream(
        &self,
        projection: &LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.block_on(
            self.service
                .list_ready_dependencies_by_downstream(projection),
        )
    }

    pub(crate) fn list_local_dependencies_by_downstream(
        &self,
        projection: &LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        use novarocks_mv_application::repository::MvDependencyReadBound;
        let bound = MvDependencyReadBound {
            inventory: local_inventory_bound(),
            entries: 4096,
            collection_bytes: 4 * 1024 * 1024,
        };
        self.block_on(
            self.service
                .list_ready_dependencies_by_downstream_bounded(projection, bound),
        )
    }

    /// Reject a mutation only when a currently consumable downstream MV
    /// depends on the upstream object. Quarantined projections are retained
    /// accelerator data, not active semantic dependencies.
    pub(crate) fn ensure_no_ready_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectIdentity,
    ) -> Result<(), MvRepositoryError> {
        self.block_on(
            self.service
                .ensure_no_ready_downstream_dependencies(upstream),
        )
    }

    /// Reserve the exact repository expectation before any provider delete.
    pub(crate) fn reserve_projection_delete(
        &self,
        target: &MvTarget,
    ) -> Result<MvKeyedProjectionDeleteGuard, MvProjectionError> {
        self.block_on(
            self.service
                .reserve_projection_delete(canonical_mv_target(target)),
        )
    }

    /// Synchronous bridge to the product-owned DROP durable preflight.
    pub(crate) fn prepare_drop(
        &self,
        target: &MvTarget,
        if_exists: bool,
    ) -> Result<MvKeyedDropReadiness, MvProjectionError> {
        self.block_on(
            self.service
                .prepare_drop(&canonical_mv_target(target), if_exists),
        )
    }

    /// Synchronous bridge to the product-owned post-provider-delete CAS.
    pub(crate) fn delete_after_provider_drop(
        &self,
        operation_id: Uuid,
        guard: MvKeyedProjectionDeleteGuard,
    ) -> Result<MvKeyedProjectionInstallOutcome, String> {
        self.block_on(self.service.delete_after_provider_drop(operation_id, guard))
            .map_err(|error| error.to_string())
    }

    /// Reject the test/harness-only whole-family Accelerator wipe while this
    /// FE owns any publication. A wipe has no distributed coordination role;
    /// the runner immediately restarts this FE after the wipe succeeds.
    #[cfg(feature = "test-controls")]
    pub(crate) fn ensure_no_active_publications(&self) -> Result<(), MvRepositoryError> {
        self.service.ensure_no_active_publications()
    }

    // Design: ADR-0174 (docs/adr/ADR-0174-test-capabilities-and-finite-role-authorization.md)
    /// Permanently stop MV admission and join the existing product workers
    /// before the authorized whole-family wipe. The caller must stop/reap this
    /// FE immediately; this operation cannot resume its old MV runtime.
    #[cfg(feature = "test-controls")]
    pub(crate) fn wipe_accelerator_for_test(
        &self,
        product: &novarocks_mv_application::service::MvKeyedProductService,
        operation_id: Uuid,
        claim: &mut novarocks_failpoint::scope::ActionClaim,
    ) -> Result<(), String> {
        if !claim.cancellation().is_active() {
            return Err("MV wipe authorization expired before stopping".into());
        }
        product.begin_stopping();
        self.block_on(product.shutdown_background_workers_until(claim.cancellation().deadline()))
            .map_err(|error| format!("stop MV background work before wipe: {error}"))?;
        self.ensure_no_active_publications()
            .map_err(|error| error.to_string())?;
        if !claim.cancellation().is_active() {
            return Err("MV wipe authorization expired before destructive action".into());
        }
        self.block_on(self.service.wipe_accelerator(operation_id))
            .map_err(|error| format!("wipe MV Accelerator family: {error}"))?;
        claim.record_hit().map_err(|error| error.to_string())
    }
}

impl MvCandidateReader {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let handle = &self.handle;
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(future)),
            Err(_) => handle.block_on(future),
        }
    }
    pub(crate) fn list_candidate_definitions(
        &self,
    ) -> Result<Vec<StoredKeyedMvProjection>, MvRepositoryError> {
        self.block_on(self.reader.list_candidate_definitions())
    }
    pub(crate) fn observe_current(
        &self,
        lease: &novarocks_spi::connector::document_storage::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::document_storage::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        String,
    > {
        let Self {
            service,
            management,
            ..
        } = self;
        let (mut observed,convergence)=novarocks_mv_application::persistence::keyed_documents::observe_current_keyed_documents_with_management(lease,request.clone(),Default::default(),management).map_err(|e|e.to_string())?;
        if observed.deployment_owner != *management.owner()
            || observed.process_incarnation != *management.incarnation()
        {
            management
                .close_on_keyed_current_management_mismatch(lease, request.clone())
                .map_err(|e| e.to_string())?;
            struct ReadOnly(novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments);
            #[async_trait::async_trait]
            impl MvKeyedReadOnlyCurrentProjectionSource for ReadOnly {
                async fn observe_current_keyed_read_only(&self,_:MvCurrentProjectionRequest)->Result<novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,MvProjectionError>{
                    Ok(self.0.clone())
                }
            }
            let target = CanonicalMvTarget::from_parts(
                Some(request.target.instance_id.as_str()),
                &request.target.namespace,
                &request.target.table,
            );
            let projection_request = MvCurrentProjectionRequest::try_new(
                request.catalog_handle,
                target,
                request.context,
                Default::default(),
            )
            .map_err(|e| e.to_string())?;
            self.block_on(service.observe_current_read_only_and_install(
                Uuid::now_v7(),
                projection_request,
                &ReadOnly(observed.clone()),
                "fresh MV management slot names another writer".into(),
            ))
            .map_err(|e| e.to_string())?;
            return Ok(observed);
        }
        if convergence.reinstall_current_projection {
            let (fresh, admission, _) = management
                .observe_keyed_admission(lease, request.clone(), Default::default())
                .map_err(|e| e.to_string())?;
            observed = fresh;
            struct Source{observed:novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,admission:std::sync::Mutex<Option<novarocks_mv_application::management::MvCurrentManagementAdmission>>}
            #[async_trait::async_trait]
            impl MvKeyedCurrentProjectionSource for Source {
                async fn observe_current_keyed(&self,_:MvCurrentProjectionRequest)->Result<(novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,novarocks_mv_application::management::MvCurrentManagementAdmission),MvProjectionError>{
                    Ok((self.observed.clone(), self.admission.lock().map_err(|_|MvProjectionError::new(novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,"slot projection admission lock poisoned"))?.take().ok_or_else(||MvProjectionError::new(novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,"slot projection admission already consumed"))?))
                }
            }
            let target = CanonicalMvTarget::from_parts(
                Some(request.target.instance_id.as_str()),
                &request.target.namespace,
                &request.target.table,
            );
            let projection_request = MvCurrentProjectionRequest::try_new(
                request.catalog_handle,
                target,
                request.context,
                Default::default(),
            )
            .map_err(|e| e.to_string())?;
            self.block_on(service.observe_and_install(
                Uuid::now_v7(),
                projection_request,
                &Source {
                    observed: observed.clone(),
                    admission: std::sync::Mutex::new(Some(admission)),
                },
            ))
            .map_err(|e| e.to_string())?;
        }
        Ok(observed)
    }
}

#[cfg(all(test, unix, feature = "test-controls"))]
mod controlled_wipe_tests {
    use super::*;
    use crate::mv::test_controls::tests::{TestSession, wipe_grant};
    use novarocks_failpoint::session::SessionRole;
    use novarocks_mv_application::{
        activity::{MvActivityGateError, MvActivityOwner},
        process_runtime::{MvBackgroundRuntime, ProcessRuntime},
        product::MvTarget,
        scheduler::MvSchedulerConfig,
        service::MvKeyedProductService,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    async fn product_and_port() -> (
        MvKeyedProductService,
        MvReadinessPort,
        Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository>,
    ) {
        let (product, repository) = crate::mv::domain::test_admission::keyed_product().await;
        let service = product.keyed_readiness_service().unwrap();
        service
            .seed_projection(
                Uuid::now_v7(),
                crate::mv::domain::test_admission::sample_keyed_projection(
                    MvTarget::from_parts(Some("ice"), "sales", "orders"),
                    Some(7),
                ),
            )
            .await
            .unwrap();
        let port = MvReadinessPort::from_product(
            service,
            tokio::runtime::Handle::current(),
            product.management_entrance().unwrap(),
        );
        (product, port, repository)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_actual_exit_precedes_wipe_and_old_admission_stays_stopped() {
        let session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let (product, port, repository) = product_and_port().await;
        let (refresh_stop_tx, refresh_stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, maintenance_stop_rx) = mpsc::channel();
        let (wakeup_tx, _wakeup_rx) = mpsc::sync_channel(1);
        let refresh_exited = Arc::new(AtomicBool::new(false));
        let maintenance_exited = Arc::new(AtomicBool::new(false));
        let refresh_fact = refresh_exited.clone();
        let maintenance_fact = maintenance_exited.clone();
        let runtime = MvBackgroundRuntime::new(
            refresh_stop_tx,
            std::thread::spawn(move || {
                refresh_stop_rx.recv().unwrap();
                refresh_fact.store(true, Ordering::Release);
            }),
            maintenance_stop_tx,
            wakeup_tx,
            std::thread::spawn(move || {
                maintenance_stop_rx.recv().unwrap();
                maintenance_fact.store(true, Ordering::Release);
            }),
        );
        product
            .begin_background_start()
            .unwrap()
            .install(runtime)
            .unwrap();
        port.wipe_accelerator_for_test(&product, Uuid::now_v7(), &mut claim)
            .unwrap();
        assert!(refresh_exited.load(Ordering::Acquire));
        assert!(maintenance_exited.load(Ordering::Acquire));
        assert!(
            repository
                .list_keyed_projections()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            product.request_activity(
                canonical_mv_target(&novarocks_sql::planning::mv::SqlMvTarget {
                    catalog: Some("ice".into()),
                    database: "sales".into(),
                    name: "orders".into()
                }),
                MvActivityOwner::ScheduledRefresh
            ),
            Err(MvActivityGateError::Stopping)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unjoined_background_worker_preserves_accelerator_and_joins_on_cleanup() {
        let session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 400);
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let (product, port, repository) = product_and_port().await;
        let before = repository.list_keyed_projections().await.unwrap();
        let (refresh_stop_tx, _refresh_stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, _maintenance_stop_rx) = mpsc::channel();
        let (wakeup_tx, _wakeup_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let runtime = MvBackgroundRuntime::new(
            refresh_stop_tx,
            std::thread::spawn(move || {
                release_rx.recv().unwrap();
            }),
            maintenance_stop_tx,
            wakeup_tx,
            std::thread::spawn(|| {}),
        );
        product
            .begin_background_start()
            .unwrap()
            .install(runtime)
            .unwrap();
        let result = port.wipe_accelerator_for_test(&product, Uuid::now_v7(), &mut claim);
        release_tx.send(()).unwrap();
        product
            .shutdown_background_workers_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(result.unwrap_err().contains("shared shutdown deadline"));
        assert_eq!(repository.list_keyed_projections().await.unwrap(), before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_join_failure_preserves_accelerator() {
        let session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let (product, port, repository) = product_and_port().await;
        let before = repository.list_keyed_projections().await.unwrap();
        let (refresh_stop_tx, _refresh_stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, _maintenance_stop_rx) = mpsc::channel();
        let (wakeup_tx, _wakeup_rx) = mpsc::sync_channel(1);
        let runtime = MvBackgroundRuntime::new(
            refresh_stop_tx,
            std::thread::spawn(|| {
                panic!("injected MV worker panic");
            }),
            maintenance_stop_tx,
            wakeup_tx,
            std::thread::spawn(|| {}),
        );
        product
            .begin_background_start()
            .unwrap()
            .install(runtime)
            .unwrap();
        let result = port.wipe_accelerator_for_test(&product, Uuid::now_v7(), &mut claim);
        product
            .shutdown_background_workers_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(result.unwrap_err().contains("panicked"));
        assert_eq!(repository.list_keyed_projections().await.unwrap(), before);
    }
}
