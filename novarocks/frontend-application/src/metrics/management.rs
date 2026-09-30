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
use std::sync::Arc;

use novarocks_memory::MemoryAuthority;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::{
    Json, Router,
    routing::{get, post},
};

use crate::query_execution::lifecycle_diagnostics::QueryLifecycleConvergenceReader;
use crate::topology::{BackendIslandSnapshot, BackendIslandSnapshotReader};
use crate::workload_lifecycle::{
    FrontendCatalogServingSnapshot, FrontendDrainServingSnapshot, FrontendServingLifecycle,
    FrontendServingSnapshot, FrontendServingSnapshotReader, FrontendWorkloadServingSnapshot,
};
use novarocks_query_application::serving_admission::FrontendServingState;

use super::{FrontendMetricsRegistry, render_metrics, render_metrics_json};

#[derive(Clone)]
struct FrontendManagementState {
    registry: Arc<FrontendMetricsRegistry>,
    serving_reader: Arc<dyn FrontendServingSnapshotReader>,
    island_reader: Arc<dyn BackendIslandSnapshotReader>,
    memory_authority: Arc<MemoryAuthority>,
}

/// Versioned management document composed from independent read-only owners.
/// The serving lifecycle remains FE-local; topology remains the membership
/// authority. This type only joins their already-sanitized observations.
#[derive(serde::Serialize)]
struct FrontendManagementSnapshot {
    schema_version: u8,
    serving_state: FrontendServingState,
    catalog: FrontendCatalogServingSnapshot,
    workload: FrontendWorkloadServingSnapshot,
    drain: FrontendDrainServingSnapshot,
    island: FrontendIslandManagementSnapshot,
}

#[derive(serde::Serialize)]
struct FrontendIslandManagementSnapshot {
    native_compatibility_id: String,
    topology_revision: u64,
    compatible_eligible_backend_count: usize,
    other_island_backend_count: usize,
    unknown_or_invalid_backend_count: usize,
    ready: bool,
}

impl FrontendManagementSnapshot {
    fn compose(serving: FrontendServingSnapshot, island: BackendIslandSnapshot) -> Self {
        let ready = serving.base_ready() && island.compatible_eligible_backend_count() >= 1;
        Self {
            schema_version: 2,
            serving_state: serving.serving_state,
            catalog: serving.catalog,
            workload: serving.workload,
            drain: serving.drain,
            island: FrontendIslandManagementSnapshot {
                native_compatibility_id: island.native_compatibility_id().to_string(),
                topology_revision: island.topology_revision(),
                compatible_eligible_backend_count: island.compatible_eligible_backend_count(),
                other_island_backend_count: island.other_island_backend_count(),
                unknown_or_invalid_backend_count: island.unknown_or_invalid_backend_count(),
                ready,
            },
        }
    }
}

/// Builds the complete Frontend management HTTP surface. Native report gRPC
/// must not compose any management routes.
/// The process memory authority's own facts, rendered for a reader.
///
/// The memory core carries no dependencies at all, so it cannot derive
/// `Serialize`; this projection is where its numbers meet a wire format, and
/// keeping it here is what lets the core stay neutral.
///
/// Managed responsibility and observation coverage are reported separately.
/// `capacity_bytes` is the current managed target; headroom covers declared
/// blind spots. Neither is measured RSS. Shrink preserves existing commitment
/// and the control floor, so excess is pressure rather than erased liability.
#[derive(serde::Serialize)]
struct MemoryAuthorityManagementSnapshot {
    schema_version: u8,
    /// `P`: what the whole process may use.
    process_bound_bytes: u64,
    /// Current managed target B, which can fall below existing commitment.
    capacity_bytes: u64,
    /// `H`: the part set aside for declared blind spots.
    headroom_budget_bytes: u64,
    /// `C` at the root, as the root itself maintains it.
    committed_bytes: u64,
    /// Last incorporated payload L plus prepaid metadata, not current RSS.
    live_bytes: u64,
    /// `F`: issued rights not yet fulfilled.
    granted_bytes: u64,
    /// `O`: authorised third-party upper bound, not measured usage.
    bounded_bytes: u64,
    /// What `B` can still issue.
    capacity_remaining_bytes: u64,
    /// Whether `C <= B` held in this reading.
    honours_capacity_bound: bool,
    ledger_revision: u64,
    capacity_revision: u64,
    control_floor_bytes: u64,
    elastic_capacity_bytes: u64,
    elastic_committed_bytes: u64,
    root_excess_bytes: u64,
    elastic_excess_bytes: u64,
    floor_target_gap_bytes: u64,
    settled_payload_live_bytes: u64,
    sampled_payload_live_bytes: u64,
    sampling_span_ns: u64,
    active_scopes: u64,
    settled_idle_authorization_bytes: u64,
    pending_drain_domains: u64,
    dirty_domains: u64,
    changing_live_samples: u64,
    settled_debt_bytes: u64,
    sampled_debt_bytes: u64,
    /// Complete classification shares the root ledger/capacity versions.
    classification_complete: bool,
    classified_committed_bytes: u64,
    unclassified_committed_bytes: u64,
    query_committed_bytes: u64,
    residual_committed_bytes: u64,
    residual_query_committed_bytes: u64,
    residual_metadata_bytes: u64,
    active_metadata_bytes: u64,
    storage_metadata_bytes: u64,
    account_slack_bytes: u64,
    /// Null when a bounded observation cannot classify all commitment.
    query_pressure_bytes: Option<u64>,
    live_accounts: u32,
    /// Whether a separate control partition is installed in this process.
    control_branch_installed: bool,
}

impl MemoryAuthorityManagementSnapshot {
    fn of(authority: &MemoryAuthority) -> Self {
        let (snapshot, pressure) = authority.accounting_snapshot();
        Self {
            schema_version: 2,
            process_bound_bytes: snapshot.process_bound_bytes,
            capacity_bytes: snapshot.capacity_bytes,
            headroom_budget_bytes: snapshot.headroom_budget_bytes,
            committed_bytes: snapshot.root.committed_bytes,
            live_bytes: snapshot.root.live_bytes,
            granted_bytes: snapshot.root.granted_bytes,
            bounded_bytes: snapshot.root.bounded_bytes,
            capacity_remaining_bytes: snapshot.capacity_remaining_bytes(),
            honours_capacity_bound: snapshot.honours_capacity_bound(),
            ledger_revision: pressure.root_revision,
            capacity_revision: pressure.capacity_revision,
            control_floor_bytes: pressure.control_floor,
            elastic_capacity_bytes: pressure.elastic_capacity,
            elastic_committed_bytes: pressure.elastic_committed,
            root_excess_bytes: snapshot.root.excess_bytes,
            elastic_excess_bytes: pressure.elastic_excess,
            floor_target_gap_bytes: pressure.floor_target_gap,
            settled_payload_live_bytes: pressure.settled_payload_live,
            sampled_payload_live_bytes: pressure.sampled_payload_live,
            sampling_span_ns: snapshot.root.sampling_span_ns,
            active_scopes: pressure.active_scopes,
            settled_idle_authorization_bytes: pressure.settled_idle_authorization,
            pending_drain_domains: pressure.pending_drain_domains,
            dirty_domains: pressure.dirty_domains,
            changing_live_samples: pressure.changing_live_samples,
            settled_debt_bytes: pressure.settled_debt,
            sampled_debt_bytes: pressure.sampled_debt,
            classification_complete: pressure.classification_complete,
            classified_committed_bytes: pressure.classified_committed,
            unclassified_committed_bytes: pressure.unclassified_committed,
            query_committed_bytes: pressure.query_committed,
            residual_committed_bytes: pressure.residual_committed,
            residual_query_committed_bytes: pressure.residual_query_committed,
            residual_metadata_bytes: pressure.residual_metadata,
            active_metadata_bytes: pressure.active_metadata,
            storage_metadata_bytes: pressure.storage_metadata,
            account_slack_bytes: pressure.account_slack,
            query_pressure_bytes: pressure.complete_query_pressure(),
            live_accounts: snapshot.live_accounts,
            control_branch_installed: authority.control_branch().is_some(),
        }
    }
}

async fn memory_authority_snapshot(
    State(state): State<FrontendManagementState>,
) -> axum::response::Response {
    axum::Json(MemoryAuthorityManagementSnapshot::of(
        &state.memory_authority,
    ))
    .into_response()
}

/// Builds the management surface from late-bindable, read-only capabilities.
/// No route in this router can mutate the serving lifecycle.
pub(crate) fn frontend_management_router_with_readers(
    registry: Arc<FrontendMetricsRegistry>,
    serving_reader: Arc<dyn FrontendServingSnapshotReader>,
    island_reader: Arc<dyn BackendIslandSnapshotReader>,
    convergence_reader: Option<Arc<dyn QueryLifecycleConvergenceReader>>,
    memory_authority: Arc<MemoryAuthority>,
    debug_enabled: bool,
) -> Router {
    let state = FrontendManagementState {
        registry,
        serving_reader,
        island_reader,
        memory_authority,
    };
    let router = Router::new()
        .route("/metrics", get(handle_management_metrics))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/island-readyz", get(island_readyz))
        .route("/v1/frontend/state", get(frontend_state))
        .route("/v1/memory/authority", get(memory_authority_snapshot));
    let router = if debug_enabled && convergence_reader.is_some() {
        let convergence_reader = convergence_reader.expect("checked above");
        router.route(
            crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH,
            get(move || latest_lifecycle_convergence_snapshot(Arc::clone(&convergence_reader))),
        )
    } else {
        router
    };
    let router = if crate::preparation_diagnostics::enabled() {
        router
            .route(
                "/v1/diagnostics/preparation/arm",
                post(arm_preparation_diagnostics),
            )
            .route(
                "/v1/diagnostics/preparation/drain",
                post(drain_preparation_diagnostics),
            )
            .route(
                "/v1/diagnostics/preparation/statement/arm",
                post(arm_statement_observation),
            )
            .route(
                "/v1/diagnostics/preparation/statement/peek",
                post(peek_statement_observation),
            )
            .route(
                "/v1/diagnostics/preparation/statement/drain",
                post(drain_statement_observation),
            )
    } else {
        router
    };
    router.with_state(state)
}

async fn arm_preparation_diagnostics(
    headers: HeaderMap,
    Json(request): Json<crate::preparation_diagnostics::ControlRequest>,
) -> axum::response::Response {
    if !preparation_diagnostic_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match crate::preparation_diagnostics::arm(request.run_token) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

async fn drain_preparation_diagnostics(
    headers: HeaderMap,
    Json(request): Json<crate::preparation_diagnostics::ControlRequest>,
) -> axum::response::Response {
    if !preparation_diagnostic_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match crate::preparation_diagnostics::drain(&request.run_token) {
        Ok(response) => Json(response).into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

async fn arm_statement_observation(
    headers: HeaderMap,
    Json(request): Json<crate::preparation_diagnostics::StatementObservationArmRequest>,
) -> axum::response::Response {
    if !preparation_diagnostic_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match crate::preparation_diagnostics::arm_statement_observation(request) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

async fn peek_statement_observation(
    headers: HeaderMap,
    Json(request): Json<crate::preparation_diagnostics::ControlRequest>,
) -> axum::response::Response {
    if !preparation_diagnostic_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match crate::preparation_diagnostics::peek_statement_observation(&request.run_token) {
        Ok(response) => Json(response).into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

async fn drain_statement_observation(
    headers: HeaderMap,
    Json(request): Json<crate::preparation_diagnostics::ControlRequest>,
) -> axum::response::Response {
    if !preparation_diagnostic_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match crate::preparation_diagnostics::drain_statement_observation(&request.run_token) {
        Ok(response) => Json(response).into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

fn preparation_diagnostic_authorized(headers: &HeaderMap) -> bool {
    crate::preparation_diagnostics::authorize(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
}

async fn handle_management_metrics(
    State(state): State<FrontendManagementState>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let snapshot = management_snapshot(&state);
    super::publish_frontend_island_metrics(
        snapshot.island.ready,
        snapshot.island.compatible_eligible_backend_count,
    );
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match render_metrics_json(state.registry.as_ref()) {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
        };
    }
    match render_metrics(state.registry.as_ref()) {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

async fn livez(State(state): State<FrontendManagementState>) -> axum::response::Response {
    if state
        .serving_reader
        .frontend_serving_snapshot()
        .serving_state
        == FrontendServingState::Stopping
    {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    } else {
        StatusCode::OK.into_response()
    }
}

async fn readyz(State(state): State<FrontendManagementState>) -> axum::response::Response {
    if state
        .serving_reader
        .frontend_serving_snapshot()
        .base_ready()
    {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

async fn frontend_state(
    State(state): State<FrontendManagementState>,
) -> Json<FrontendManagementSnapshot> {
    Json(management_snapshot(&state))
}

async fn island_readyz(State(state): State<FrontendManagementState>) -> axum::response::Response {
    let snapshot = management_snapshot(&state);
    super::publish_frontend_island_metrics(
        snapshot.island.ready,
        snapshot.island.compatible_eligible_backend_count,
    );
    if snapshot.island.ready {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

fn management_snapshot(state: &FrontendManagementState) -> FrontendManagementSnapshot {
    let serving = state.serving_reader.frontend_serving_snapshot();
    super::publish_frontend_serving_metrics(serving.clone());
    FrontendManagementSnapshot::compose(serving, state.island_reader.backend_island_snapshot())
}

async fn latest_lifecycle_convergence_snapshot(
    reader: Arc<dyn QueryLifecycleConvergenceReader>,
) -> axum::response::Response {
    let Some(snapshot) = reader.latest_convergence_snapshot() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    Json(crate::native::report_server::lifecycle_convergence_debug_json(snapshot)).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    use super::{
        FrontendMetricsRegistry, MemoryAuthority, frontend_management_router_with_readers,
    };
    use crate::query_execution::lifecycle_diagnostics::{
        QueryLifecycleConvergenceReader, QueryLifecycleConvergenceSnapshot,
    };
    use crate::topology::{BackendIslandSnapshot, BackendIslandSnapshotReader};
    use crate::workload_lifecycle::{
        FrontendCatalogCounts, FrontendCatalogSnapshotIdentity, FrontendCatalogSourceMode,
        FrontendServingLifecycle,
    };

    struct EmptyConvergenceReader;

    impl QueryLifecycleConvergenceReader for EmptyConvergenceReader {
        fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
            None
        }
    }

    struct TestIslandReader {
        snapshot: RwLock<BackendIslandSnapshot>,
    }

    impl TestIslandReader {
        fn new(snapshot: BackendIslandSnapshot) -> Self {
            Self {
                snapshot: RwLock::new(snapshot),
            }
        }

        fn set(&self, snapshot: BackendIslandSnapshot) {
            *self.snapshot.write().expect("test island reader lock") = snapshot;
        }
    }

    impl BackendIslandSnapshotReader for TestIslandReader {
        fn backend_island_snapshot(&self) -> BackendIslandSnapshot {
            *self.snapshot.read().expect("test island reader lock")
        }
    }

    fn island_snapshot(
        revision: u64,
        compatible_eligible: usize,
        other_island: usize,
        unknown_or_invalid: usize,
    ) -> BackendIslandSnapshot {
        BackendIslandSnapshot::new(
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            revision,
            compatible_eligible,
            other_island,
            unknown_or_invalid,
        )
    }

    /// A small but real authority, so the management surface is exercised
    /// against the same type production installs rather than a stub.
    fn test_memory_authority() -> Arc<MemoryAuthority> {
        const BOUND: u64 = 64 * 1024 * 1024;
        let authority = MemoryAuthority::new(novarocks_memory::AuthorityConfig::new(
            BOUND,
            BOUND - BOUND / 4,
            BOUND / 4,
        ))
        .expect("the test partition must be valid");
        authority
            .install_control_branch(1024 * 1024)
            .expect("the control branch must install");
        Arc::new(authority)
    }

    fn router(
        debug_enabled: bool,
        lifecycle: Arc<FrontendServingLifecycle>,
        island: Arc<TestIslandReader>,
    ) -> axum::Router {
        frontend_management_router_with_readers(
            FrontendMetricsRegistry::new().expect("create frontend metrics registry"),
            lifecycle,
            island,
            Some(Arc::new(EmptyConvergenceReader)),
            test_memory_authority(),
            debug_enabled,
        )
    }

    #[tokio::test]
    async fn the_management_surface_reports_both_memory_tiers_without_adding_them() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        let island = Arc::new(TestIslandReader::new(island_snapshot(0, 0, 0, 0)));
        let response = router(false, lifecycle, island)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/memory/authority")
                    .body(axum::body::Body::empty())
                    .expect("build the authority snapshot request"),
            )
            .await
            .expect("the authority snapshot route must answer");
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read the authority snapshot body");
        let document: serde_json::Value =
            serde_json::from_slice(&body).expect("the snapshot must be JSON");

        const BOUND: u64 = 64 * 1024 * 1024;
        assert_eq!(document["process_bound_bytes"], BOUND);
        assert_eq!(document["capacity_bytes"], BOUND - BOUND / 4);
        assert_eq!(document["headroom_budget_bytes"], BOUND / 4);
        // The two tiers are reported separately and are never summed for the
        // reader: B + H is the partition of P, not a total of anything used.
        assert_eq!(
            document["capacity_bytes"].as_u64().unwrap()
                + document["headroom_budget_bytes"].as_u64().unwrap(),
            BOUND
        );
        assert_eq!(document["honours_capacity_bound"], true);
        assert_eq!(document["control_branch_installed"], true);
        for decomposed in ["live_bytes", "granted_bytes", "bounded_bytes"] {
            assert!(
                document[decomposed].is_u64(),
                "the snapshot must report {decomposed} so L/F/O can be read apart"
            );
        }
    }

    #[test]
    fn management_separates_dirty_samples_and_preserves_residual_pressure_after_handoff() {
        use novarocks_memory::{AccountKind, ExternalRef, OWNER_METADATA_BYTES, TeardownEvidence};
        let authority = test_memory_authority();
        let query = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = query.create_domain(1_024).unwrap();
        let mut scope = domain.activate(1_024, 0).unwrap();
        let origin = scope.record_allocation(1_024);
        let sample =
            serde_json::to_value(super::MemoryAuthorityManagementSnapshot::of(&authority)).unwrap();
        assert_eq!(sample["schema_version"], 2);
        assert_eq!(sample["settled_payload_live_bytes"], 0);
        assert_eq!(sample["sampled_payload_live_bytes"], 1_024);
        assert_eq!(sample["active_scopes"], 1);
        assert_eq!(sample["dirty_domains"], 1);
        assert_eq!(sample["classification_complete"], true);
        assert_eq!(sample["query_pressure_bytes"], 1_024 + OWNER_METADATA_BYTES);
        scope.finish();
        let before = super::MemoryAuthorityManagementSnapshot::of(&authority);
        let evidence = TeardownEvidence {
            tasks_exited: true,
            operators_destroyed: true,
            io: &[],
            now_ns: 1,
        };
        query.retire(&evidence).unwrap();
        let after =
            serde_json::to_value(super::MemoryAuthorityManagementSnapshot::of(&authority)).unwrap();
        assert_eq!(after["committed_bytes"], before.committed_bytes);
        assert_eq!(
            after["query_pressure_bytes"],
            before.query_pressure_bytes.unwrap()
        );
        assert_eq!(after["query_committed_bytes"], 0);
        assert_eq!(
            after["residual_query_committed_bytes"],
            1_024 + OWNER_METADATA_BYTES
        );
        assert_eq!(
            after["residual_committed_bytes"],
            1_024 + OWNER_METADATA_BYTES
        );
        assert_eq!(after["residual_metadata_bytes"], OWNER_METADATA_BYTES);
        assert_eq!(after["dirty_domains"], 0);
        assert_eq!(after["active_scopes"], 0);
        assert_eq!(after["classification_complete"], true);
        // SAFETY: this is the one release paired with the outstanding origin.
        unsafe {
            origin.record_deallocation(1_024);
        }
    }

    #[test]
    fn management_reports_floor_gap_and_elastic_excess_after_target_shrink() {
        let authority = test_memory_authority();
        let mut writer = authority.take_capacity_writer().unwrap();
        let revision = writer.set_capacity(0).unwrap();
        let sample =
            serde_json::to_value(super::MemoryAuthorityManagementSnapshot::of(&authority)).unwrap();
        assert_eq!(sample["capacity_bytes"], 0);
        assert_eq!(sample["capacity_revision"], revision);
        assert_eq!(sample["control_floor_bytes"], 1_024 * 1_024);
        assert_eq!(sample["floor_target_gap_bytes"], 1_024 * 1_024);
        assert_eq!(sample["elastic_capacity_bytes"], 0);
        assert_eq!(
            sample["elastic_excess_bytes"],
            sample["elastic_committed_bytes"]
        );
        assert_eq!(sample["root_excess_bytes"], sample["committed_bytes"]);
        assert_eq!(sample["classification_complete"], true);
        assert_eq!(sample["honours_capacity_bound"], false);
        assert_eq!(
            sample["classified_committed_bytes"],
            sample["committed_bytes"]
        );
        assert_eq!(sample["unclassified_committed_bytes"], 0);
        assert!(sample["storage_metadata_bytes"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn management_router_serves_metrics_and_gates_lifecycle_debug_route() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        let island = Arc::new(TestIslandReader::new(island_snapshot(0, 0, 0, 0)));
        let metrics = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);

        let debug_off = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH)
                    .body(Body::empty())
                    .expect("debug-off request"),
            )
            .await
            .expect("debug-off response");
        assert_eq!(debug_off.status(), StatusCode::NOT_FOUND);

        let debug_on = router(true, lifecycle, island)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH)
                    .body(Body::empty())
                    .expect("debug-on request"),
            )
            .await
            .expect("debug-on response");
        assert_eq!(debug_on.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn management_routes_report_liveness_readiness_and_sanitized_state() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        let island = Arc::new(TestIslandReader::new(island_snapshot(3, 0, 1, 2)));
        lifecycle.publish_catalog_bootstrap(
            FrontendCatalogSourceMode::StaticFile,
            true,
            Some(
                FrontendCatalogSnapshotIdentity::try_new(2, "0123456789abcdef")
                    .expect("snapshot identity"),
            ),
            FrontendCatalogCounts {
                desired: 2,
                ready: 1,
                unavailable: 1,
            },
        );
        let live = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .expect("live request"),
            )
            .await
            .expect("live response");
        assert_eq!(live.status(), StatusCode::OK);
        let not_ready = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("ready request"),
            )
            .await
            .expect("ready response");
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);

        lifecycle.mark_ready().expect("mark ready");
        let state = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/v1/frontend/state")
                    .body(Body::empty())
                    .expect("state request"),
            )
            .await
            .expect("state response");
        assert_eq!(state.status(), StatusCode::OK);
        let body = axum::body::to_bytes(state.into_body(), usize::MAX)
            .await
            .expect("state body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("state json");
        assert_eq!(json["schema_version"], 2);
        assert_eq!(json["catalog"]["counts"]["desired"], 2);
        assert!(json.get("properties").is_none());
        assert!(json.to_string().contains("0123456789abcdef"));
        assert_eq!(json["island"]["native_compatibility_id"], "71".repeat(32));
        assert_eq!(json["island"]["topology_revision"], 3);
        assert_eq!(json["island"]["compatible_eligible_backend_count"], 0);
        assert_eq!(json["island"]["other_island_backend_count"], 1);
        assert_eq!(json["island"]["unknown_or_invalid_backend_count"], 2);
        assert_eq!(json["island"]["ready"], false);

        let ready = router(false, lifecycle, island)
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("ready request"),
            )
            .await
            .expect("ready response");
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn island_readyz_requires_base_readiness_and_a_compatible_eligible_backend() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        let island = Arc::new(TestIslandReader::new(island_snapshot(0, 0, 0, 0)));
        let base_false = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(base_false.status(), StatusCode::SERVICE_UNAVAILABLE);

        lifecycle.publish_catalog_bootstrap(
            FrontendCatalogSourceMode::StaticFile,
            true,
            None,
            FrontendCatalogCounts::default(),
        );
        lifecycle.mark_ready().expect("mark ready");
        let no_backend = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(no_backend.status(), StatusCode::SERVICE_UNAVAILABLE);

        island.set(island_snapshot(1, 1, 0, 0));
        let compatible_backend = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(compatible_backend.status(), StatusCode::OK);

        island.set(island_snapshot(2, 0, 1, 0));
        let other_island_only = router(false, lifecycle, island)
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(other_island_only.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
