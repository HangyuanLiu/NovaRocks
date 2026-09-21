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

#![allow(dead_code)] // The same module is compiled by the optional standalone fixture binary.

//! Test-only transparent Iceberg REST proxy for publication acceptance tests.
//!
//! The proxy owns no catalog state and exposes no catalog extension.  It only
//! recognizes standard REST publication and discovery requests in order to
//! consume one runner-owned fault token. The real REST Catalog remains the
//! only authority for every create, commit, ref, object, and package outcome.

use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const MAX_PROXY_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_PROXY_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_FAULT_TRACE_EVENTS: usize = 256;

#[derive(Clone, Debug)]
pub(crate) struct FixtureConfig {
    #[allow(dead_code)]
    pub(crate) listen: SocketAddr,
    pub(crate) downstream: String,
}

pub(crate) struct FixtureHandle {
    uri: String,
    next_fault: Arc<Mutex<NextFaultState>>,
    mutations: Arc<Mutex<BTreeMap<(String, String), MutationCounts>>>,
    traffic: Arc<Mutex<TrafficCounters>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub(crate) struct FixtureControl {
    uri: String,
    client: reqwest::blocking::Client,
    next_fault: Arc<Mutex<NextFaultState>>,
    mutations: Arc<Mutex<BTreeMap<(String, String), MutationCounts>>>,
    traffic: Arc<Mutex<TrafficCounters>>,
}

pub(crate) struct FixtureFaultGuard {
    control: FixtureControl,
    arm_id: String,
    fault: PublicationFault,
    cleared: bool,
}

pub(crate) struct FixtureFaultEvidence {
    events: Vec<String>,
}

impl FixtureFaultEvidence {
    pub(crate) fn summary(&self) -> String {
        self.events.join(" -> ")
    }
}

#[derive(Clone)]
struct AppState {
    downstream: String,
    client: reqwest::Client,
    next_fault: Arc<Mutex<NextFaultState>>,
    next_fault_sequence: Arc<AtomicU64>,
    mutations: Arc<Mutex<BTreeMap<(String, String), MutationCounts>>>,
    traffic: Arc<Mutex<TrafficCounters>>,
}

/// Cumulative traffic forwarded to the real REST Catalog. Fixture control
/// requests and requests refused before dispatch are excluded.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct TrafficCounters {
    pub(crate) requests: u64,
    pub(crate) request_body_bytes: u64,
    pub(crate) response_body_bytes: u64,
    pub(crate) table_commit_requests: u64,
    pub(crate) table_commit_roundtrip_nanos: u64,
    pub(crate) by_method: BTreeMap<String, u64>,
    pub(crate) by_status: BTreeMap<u16, u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MutationCounts {
    pub(crate) stage_create_forwarded: usize,
    pub(crate) stage_create_succeeded: usize,
    pub(crate) table_commit_forwarded: usize,
    pub(crate) table_commit_succeeded: usize,
    pub(crate) other_forwarded: usize,
    pub(crate) other_succeeded: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetMutationAction {
    StageCreate,
    TableCommit,
    Other,
}

#[derive(Default)]
struct NextFaultState {
    armed: Option<ArmedNextFault>,
    status: Option<ConsumedNextFault>,
    trace_sequence: u64,
    trace: VecDeque<FaultTraceEvent>,
}

#[derive(Debug, Clone)]
struct FaultTraceEvent {
    sequence: u64,
    arm_id: String,
    event: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PublicationAction {
    StageCreate,
    TableCommit,
    NamespaceList,
    TableLoad,
}

impl PublicationAction {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StageCreate => "stage-create",
            Self::TableCommit => "table-commit",
            Self::NamespaceList => "namespace-list",
            Self::TableLoad => "table-load",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PublicationFault {
    BeforeDispatch,
    BeforeDispatchHoldForConcurrentShell,
    BeforeRequirementCheckHoldForConcurrentShell,
    AfterCommitBeforeResponse,
    AfterCommitHoldForFrontendKill,
    IncompleteDiscovery,
    CorruptPackage,
}

impl PublicationFault {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeDispatch => "before-dispatch",
            Self::BeforeDispatchHoldForConcurrentShell => {
                "before-dispatch-hold-for-concurrent-shell"
            }
            Self::BeforeRequirementCheckHoldForConcurrentShell => {
                "before-requirement-check-hold-for-concurrent-shell"
            }
            Self::AfterCommitBeforeResponse => "after-commit-before-response",
            Self::AfterCommitHoldForFrontendKill => "after-commit-hold-for-frontend-kill",
            Self::IncompleteDiscovery => "incomplete-discovery",
            Self::CorruptPackage => "corrupt-package",
        }
    }
}

#[derive(Debug, Clone)]
struct ArmedNextFault {
    arm_id: String,
    action: PublicationAction,
    fault: PublicationFault,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Debug, Clone)]
struct ConsumedNextFault {
    arm_id: String,
    entered: bool,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Deserialize, Serialize)]
struct ArmNextFaultRequest {
    action: PublicationAction,
    fault: PublicationFault,
}

#[derive(Deserialize, Serialize)]
struct ClearNextFaultResponse {
    entered: bool,
}

#[derive(Deserialize, Serialize)]
struct ArmNextFaultResponse {
    arm_id: String,
}

impl FixtureHandle {
    pub(crate) fn start(downstream: String) -> Result<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .context("reserve publication catalog fixture listener")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let next_fault = Arc::new(Mutex::new(NextFaultState::default()));
        let mutations = Arc::new(Mutex::new(BTreeMap::new()));
        let traffic = Arc::new(Mutex::new(TrafficCounters::default()));
        let state = AppState {
            downstream: downstream.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder().no_proxy().build()?,
            next_fault: Arc::clone(&next_fault),
            next_fault_sequence: Arc::new(AtomicU64::new(1)),
            mutations: Arc::clone(&mutations),
            traffic: Arc::clone(&traffic),
        };
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("publication-catalog-fixture".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build publication catalog fixture runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("adopt publication catalog fixture listener");
                    ready_tx
                        .send(())
                        .expect("signal publication catalog fixture ready");
                    axum::serve(listener, router(state))
                        .with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .expect("serve publication catalog fixture");
                });
            })?;
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .context("wait for publication catalog fixture readiness")?;
        Ok(Self {
            uri: format!("http://{address}"),
            next_fault,
            mutations,
            traffic,
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    pub(crate) fn control(&self) -> Result<FixtureControl> {
        Ok(FixtureControl {
            uri: self.uri.clone(),
            client: reqwest::blocking::Client::builder().no_proxy().build()?,
            next_fault: Arc::clone(&self.next_fault),
            mutations: Arc::clone(&self.mutations),
            traffic: Arc::clone(&self.traffic),
        })
    }
}

impl FixtureControl {
    pub(crate) fn traffic_snapshot(&self) -> TrafficCounters {
        self.traffic.lock().expect("catalog traffic mutex").clone()
    }

    pub(crate) fn mutation_counts(&self, namespace: &str, table: &str) -> MutationCounts {
        self.mutations
            .lock()
            .expect("publication mutation mutex")
            .get(&(namespace.to_string(), table.to_string()))
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn arm_next(&self, action: &str, fault: &str) -> Result<FixtureFaultGuard> {
        let action = parse_action(action)?;
        let fault = parse_fault(fault)?;
        let response: ArmNextFaultResponse = self
            .client
            .post(format!("{}/_fixture/publication-faults/next", self.uri))
            .json(&ArmNextFaultRequest { action, fault })
            .send()
            .context("arm publication catalog next-action fault")?
            .error_for_status()
            .context("publication catalog next-action fault was rejected")?
            .json()
            .context("decode publication catalog next-action fault receipt")?;
        Ok(FixtureFaultGuard {
            control: self.clone(),
            arm_id: response.arm_id,
            fault,
            cleared: false,
        })
    }

    fn clear_arm(&self, arm_id: &str) -> Result<bool> {
        let response: ClearNextFaultResponse = self
            .client
            .delete(format!(
                "{}/_fixture/publication-faults/next/{arm_id}",
                self.uri
            ))
            .send()
            .context("clear publication catalog next-action fault")?
            .error_for_status()
            .context("publication catalog next-action fault cleanup was rejected")?
            .json()
            .context("decode publication catalog next-action fault cleanup")?;
        Ok(response.entered)
    }

    fn wait_until_entered(&self, arm_id: &str, deadline: Instant) -> Result<()> {
        loop {
            let entered = self
                .next_fault
                .lock()
                .expect("publication fault mutex")
                .status
                .as_ref()
                .filter(|status| status.arm_id == arm_id)
                .is_some_and(|status| status.entered);
            if entered {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for publication catalog fault {arm_id} to reach its hold"
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn wait_for_trace_event(&self, arm_id: &str, expected: &str, deadline: Instant) -> Result<()> {
        loop {
            if self
                .next_fault
                .lock()
                .expect("publication fault mutex")
                .trace
                .iter()
                .any(|event| event.arm_id == arm_id && event.event == expected)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for publication catalog fault {arm_id} trace event {expected}"
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn trace_events(&self, arm_id: &str) -> Vec<String> {
        let mut events = self
            .next_fault
            .lock()
            .expect("publication fault mutex")
            .trace
            .iter()
            .filter(|event| event.arm_id == arm_id)
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.sequence);
        events.into_iter().map(|event| event.event).collect()
    }
}

fn parse_action(value: &str) -> Result<PublicationAction> {
    match value {
        "stage-create" => Ok(PublicationAction::StageCreate),
        "table-commit" => Ok(PublicationAction::TableCommit),
        "namespace-list" => Ok(PublicationAction::NamespaceList),
        "table-load" => Ok(PublicationAction::TableLoad),
        other => anyhow::bail!("unknown publication catalog action `{other}`"),
    }
}

fn parse_fault(value: &str) -> Result<PublicationFault> {
    match value {
        "before-dispatch" => Ok(PublicationFault::BeforeDispatch),
        "before-dispatch-hold-for-concurrent-shell" => {
            Ok(PublicationFault::BeforeDispatchHoldForConcurrentShell)
        }
        "before-requirement-check-hold-for-concurrent-shell" => {
            Ok(PublicationFault::BeforeRequirementCheckHoldForConcurrentShell)
        }
        "after-commit-before-response" => Ok(PublicationFault::AfterCommitBeforeResponse),
        "after-commit-hold-for-frontend-kill" => {
            Ok(PublicationFault::AfterCommitHoldForFrontendKill)
        }
        "incomplete-discovery" => Ok(PublicationFault::IncompleteDiscovery),
        "corrupt-package" => Ok(PublicationFault::CorruptPackage),
        other => anyhow::bail!("unknown publication catalog fault `{other}`"),
    }
}

impl FixtureFaultGuard {
    pub(crate) fn wait_until_entered(&self, deadline: Instant) -> Result<()> {
        self.control.wait_until_entered(&self.arm_id, deadline)
    }

    pub(crate) fn wait_until_downstream_successful_hold(&self, deadline: Instant) -> Result<()> {
        self.control.wait_for_trace_event(
            &self.arm_id,
            "response-held-after-downstream-success",
            deadline,
        )
    }

    pub(crate) fn release(&mut self) -> Result<bool> {
        if self.cleared {
            return Ok(true);
        }
        let entered = self.control.clear_arm(&self.arm_id)?;
        self.cleared = true;
        Ok(entered)
    }

    pub(crate) fn finish(mut self) -> Result<FixtureFaultEvidence> {
        let entered = self.release()?;
        if !entered {
            anyhow::bail!(
                "publication catalog next-action fault {} was not consumed by its matching REST request",
                self.arm_id
            );
        }
        let terminal_event = match self.fault {
            PublicationFault::BeforeDispatch => "known-not-dispatched",
            PublicationFault::BeforeDispatchHoldForConcurrentShell
            | PublicationFault::BeforeRequirementCheckHoldForConcurrentShell => {
                "downstream-response"
            }
            PublicationFault::AfterCommitBeforeResponse => {
                "response-dropped-after-downstream-success"
            }
            // Killing the FE closes its client connection. The proxy handler
            // may be canceled before it can record a post-release response;
            // the pre-kill successful hold and control release are the proof.
            PublicationFault::AfterCommitHoldForFrontendKill => {
                "response-held-after-downstream-success"
            }
            PublicationFault::IncompleteDiscovery => "discovery-response-replaced",
            PublicationFault::CorruptPackage => "package-response-corrupted",
        };
        self.control.wait_for_trace_event(
            &self.arm_id,
            terminal_event,
            Instant::now() + Duration::from_secs(5),
        )?;
        Ok(FixtureFaultEvidence {
            events: self.control.trace_events(&self.arm_id),
        })
    }
}

impl Drop for FixtureFaultGuard {
    fn drop(&mut self) {
        if !self.cleared {
            let _ = self.release();
        }
    }
}

impl Drop for FixtureHandle {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) async fn serve(config: FixtureConfig) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .context("bind publication catalog fixture listener")?;
    let state = AppState {
        downstream: config.downstream.trim_end_matches('/').to_string(),
        client: reqwest::Client::builder().no_proxy().build()?,
        next_fault: Arc::new(Mutex::new(NextFaultState::default())),
        next_fault_sequence: Arc::new(AtomicU64::new(1)),
        mutations: Arc::new(Mutex::new(BTreeMap::new())),
        traffic: Arc::new(Mutex::new(TrafficCounters::default())),
    };
    axum::serve(listener, router(state))
        .await
        .context("serve publication catalog fixture")
}

fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/_fixture/catalog-traffic",
            axum::routing::get(catalog_traffic),
        )
        .route("/_fixture/publication-faults/next", post(arm_next_fault))
        .route(
            "/_fixture/publication-faults/next/{arm_id}",
            axum::routing::delete(clear_next_fault),
        )
        .fallback(any(dispatch))
        .with_state(state)
}

async fn catalog_traffic(State(state): State<AppState>) -> Json<TrafficCounters> {
    let snapshot = state.traffic.lock().expect("catalog traffic mutex").clone();
    Json(snapshot)
}

async fn arm_next_fault(
    State(state): State<AppState>,
    Json(request): Json<ArmNextFaultRequest>,
) -> Response {
    let arm_id = format!(
        "publication-fault-{}",
        state.next_fault_sequence.fetch_add(1, Ordering::Relaxed)
    );
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    if next.armed.is_some() || next.status.is_some() {
        return wire_error(
            StatusCode::CONFLICT,
            "fixture-busy",
            "a publication catalog fault token is already active",
        );
    }
    next.armed = Some(ArmedNextFault {
        arm_id: arm_id.clone(),
        action: request.action,
        fault: request.fault,
        release: Arc::new(tokio::sync::Notify::new()),
    });
    record_fault_event_locked(&mut next, &arm_id, "armed");
    json_response(StatusCode::OK, json!({"arm_id": arm_id}))
}

async fn clear_next_fault(
    State(state): State<AppState>,
    AxumPath(arm_id): AxumPath<String>,
) -> Response {
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    let entered = match next.status.as_ref() {
        Some(status) if status.arm_id == arm_id => status.entered,
        None => {
            return wire_error(
                StatusCode::NOT_FOUND,
                "fixture-token",
                "publication catalog fault token is unknown",
            );
        }
        Some(_) => {
            return wire_error(
                StatusCode::NOT_FOUND,
                "fixture-token",
                "publication catalog fault token does not match the active token",
            );
        }
    };
    let release = next
        .status
        .as_ref()
        .expect("checked publication fault status")
        .release
        .clone();
    record_fault_event_locked(&mut next, &arm_id, "control-release");
    next.status = None;
    if next
        .armed
        .as_ref()
        .is_some_and(|armed| armed.arm_id == arm_id)
    {
        next.armed = None;
    }
    drop(next);
    release.notify_waiters();
    json_response(StatusCode::OK, json!({"entered": entered}))
}

async fn dispatch(State(state): State<AppState>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_PROXY_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(error.to_string()),
    };
    let action = standard_catalog_action(&parts.method, parts.uri.path(), &bytes);
    let mutation_target = mutation_target(&parts.method, action, parts.uri.path(), &bytes);
    let fault = action.and_then(|action| take_matching_fault(&state, action));
    if let Some(armed) = fault.as_ref()
        && armed.fault == PublicationFault::BeforeDispatch
    {
        // The fixture proves it did not forward this request. Use a typed
        // non-5xx REST response so the standard client can truthfully classify
        // the operation as known-not-dispatched rather than a transport
        // ambiguity; no private catalog protocol participates.
        record_fault_event(&state, armed, "known-not-dispatched");
        return known_not_dispatched("publication REST request rejected before dispatch");
    }
    if let Some(armed) = fault.as_ref()
        && matches!(
            armed.fault,
            PublicationFault::BeforeDispatchHoldForConcurrentShell
                | PublicationFault::BeforeRequirementCheckHoldForConcurrentShell
        )
    {
        record_fault_event(&state, armed, "before-requirement-check-hold");
        armed.release.notified().await;
        record_fault_event(&state, armed, "before-requirement-check-release");
    }
    if let Some(armed) = fault.as_ref() {
        record_fault_event(&state, armed, "request-forwarded");
    }
    if let Some((action, namespace, table)) = mutation_target.as_ref() {
        record_mutation(&state, *action, namespace, table, false);
    }
    let method = parts.method.clone();
    let request_body_bytes = bytes.len() as u64;
    let forwarded_at = Instant::now();
    let (response, response_body_bytes) =
        proxy_request(&state, parts.method, parts.uri, parts.headers, bytes).await;
    let roundtrip_nanos = u64::try_from(forwarded_at.elapsed().as_nanos())
        .expect("catalog request duration exceeds u64 nanoseconds");
    record_traffic(
        &state,
        &method,
        response.status(),
        request_body_bytes,
        response_body_bytes,
        mutation_target
            .as_ref()
            .is_some_and(|(action, _, _)| *action == TargetMutationAction::TableCommit),
        roundtrip_nanos,
    );
    if response.status().is_success()
        && let Some((action, namespace, table)) = mutation_target.as_ref()
    {
        record_mutation(&state, *action, namespace, table, true);
    }
    if let Some(armed) = fault.as_ref() {
        record_fault_event(&state, armed, "downstream-response");
        if response.status().is_success() {
            record_fault_event(&state, armed, "downstream-success");
        }
    }
    if let Some(armed) = fault
        && response.status().is_success()
    {
        match armed.fault {
            PublicationFault::AfterCommitBeforeResponse => {
                record_fault_event(&state, &armed, "response-dropped-after-downstream-success");
                return temporary_failure(
                    "publication REST response was lost after downstream success",
                );
            }
            PublicationFault::AfterCommitHoldForFrontendKill => {
                record_fault_event(&state, &armed, "response-held-after-downstream-success");
                armed.release.notified().await;
                record_fault_event(&state, &armed, "response-dropped-after-downstream-success");
                return temporary_failure("publication REST response released after frontend kill");
            }
            PublicationFault::IncompleteDiscovery => {
                // A valid empty list means discovery completed with no
                // namespaces. The MV contract needs an actual standard REST
                // read failure so SPI classifies this catalog as Incomplete.
                record_fault_event(&state, &armed, "discovery-response-replaced");
                return temporary_failure("catalog discovery read failed");
            }
            PublicationFault::CorruptPackage => {
                record_fault_event(&state, &armed, "package-response-corrupted");
                return response_with_headers(
                    StatusCode::OK,
                    HeaderMap::new(),
                    Bytes::from_static(b"{corrupt-package"),
                );
            }
            PublicationFault::BeforeDispatch => unreachable!("returned before dispatch"),
            PublicationFault::BeforeDispatchHoldForConcurrentShell
            | PublicationFault::BeforeRequirementCheckHoldForConcurrentShell => {}
        }
    }
    response
}

fn standard_catalog_action(method: &Method, path: &str, body: &[u8]) -> Option<PublicationAction> {
    if !path.contains("/v1/") {
        return None;
    }
    if method == Method::GET && path.ends_with("/v1/namespaces") {
        return Some(PublicationAction::NamespaceList);
    }
    if method == Method::GET && path.contains("/tables/") {
        return Some(PublicationAction::TableLoad);
    }
    if method != Method::POST {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    if path.ends_with("/tables") && value.get("stage-create").and_then(Value::as_bool) == Some(true)
    {
        return Some(PublicationAction::StageCreate);
    }
    if path.contains("/tables/")
        && value.get("requirements").is_some()
        && value.get("updates").is_some()
    {
        return Some(PublicationAction::TableCommit);
    }
    None
}

fn mutation_target(
    method: &Method,
    action: Option<PublicationAction>,
    path: &str,
    body: &[u8],
) -> Option<(TargetMutationAction, String, String)> {
    if !matches!(
        method,
        &Method::POST | &Method::PUT | &Method::PATCH | &Method::DELETE
    ) {
        return None;
    }
    let remainder = path.split_once("/v1/namespaces/")?.1;
    let (namespace, table_path) = remainder.split_once("/tables")?;
    if namespace.is_empty() || namespace.contains('/') {
        return None;
    }
    let (kind, table) = if table_path.is_empty() && method == Method::POST {
        let value: Value = serde_json::from_slice(body).ok()?;
        let table = value.get("name")?.as_str()?.to_string();
        let kind = if action == Some(PublicationAction::StageCreate) {
            TargetMutationAction::StageCreate
        } else {
            TargetMutationAction::Other
        };
        (kind, table)
    } else {
        let table = table_path.strip_prefix('/')?.to_string();
        let kind = if method == Method::POST && action == Some(PublicationAction::TableCommit) {
            TargetMutationAction::TableCommit
        } else {
            TargetMutationAction::Other
        };
        (kind, table)
    };
    if table.is_empty() || table.contains('/') {
        return None;
    }
    Some((kind, namespace.to_string(), table))
}

fn record_mutation(
    state: &AppState,
    action: TargetMutationAction,
    namespace: &str,
    table: &str,
    succeeded: bool,
) {
    let mut mutations = state.mutations.lock().expect("publication mutation mutex");
    let counts = mutations
        .entry((namespace.to_string(), table.to_string()))
        .or_default();
    match (action, succeeded) {
        (TargetMutationAction::StageCreate, false) => counts.stage_create_forwarded += 1,
        (TargetMutationAction::StageCreate, true) => counts.stage_create_succeeded += 1,
        (TargetMutationAction::TableCommit, false) => counts.table_commit_forwarded += 1,
        (TargetMutationAction::TableCommit, true) => counts.table_commit_succeeded += 1,
        (TargetMutationAction::Other, false) => counts.other_forwarded += 1,
        (TargetMutationAction::Other, true) => counts.other_succeeded += 1,
    }
}

fn take_matching_fault(state: &AppState, action: PublicationAction) -> Option<ArmedNextFault> {
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    let armed = next.armed.as_ref()?;
    if armed.action != action {
        return None;
    }
    // Consume the arm before the request is dispatched. In particular, a
    // definite OCC conflict may cause the product to issue one fresh attempt;
    // that second standard request must not re-enter this one-shot hold.
    let armed = next.armed.take().expect("matching arm exists");
    next.status = Some(ConsumedNextFault {
        arm_id: armed.arm_id.clone(),
        entered: true,
        release: Arc::clone(&armed.release),
    });
    record_fault_event_locked(&mut next, &armed.arm_id, "matched");
    Some(armed)
}

fn record_fault_event(state: &AppState, armed: &ArmedNextFault, event: &str) {
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    record_fault_event_locked(&mut next, &armed.arm_id, event);
}

fn record_fault_event_locked(state: &mut NextFaultState, arm_id: &str, event: &str) {
    state.trace_sequence = state.trace_sequence.saturating_add(1);
    while state.trace.len() >= MAX_FAULT_TRACE_EVENTS {
        state.trace.pop_front();
    }
    state.trace.push_back(FaultTraceEvent {
        sequence: state.trace_sequence,
        arm_id: arm_id.to_string(),
        event: event.to_string(),
    });
}

fn record_traffic(
    state: &AppState,
    method: &Method,
    status: StatusCode,
    request_body_bytes: u64,
    response_body_bytes: u64,
    is_table_commit: bool,
    roundtrip_nanos: u64,
) {
    let mut traffic = state.traffic.lock().expect("catalog traffic mutex");
    traffic.requests += 1;
    traffic.request_body_bytes += request_body_bytes;
    traffic.response_body_bytes += response_body_bytes;
    if is_table_commit {
        traffic.table_commit_requests += 1;
        traffic.table_commit_roundtrip_nanos += roundtrip_nanos;
    }
    *traffic
        .by_method
        .entry(method.as_str().to_string())
        .or_default() += 1;
    *traffic.by_status.entry(status.as_u16()).or_default() += 1;
}

async fn proxy_request(
    state: &AppState,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> (Response, u64) {
    let url = format!(
        "{}{}",
        state.downstream,
        uri.path_and_query()
            .map(|value| value.as_str())
            .unwrap_or("/")
    );
    let mut outbound = state.client.request(method, url).body(bytes);
    for (name, value) in &headers {
        if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
            outbound = outbound.header(name, value);
        }
    }
    let response = match outbound.send().await {
        Ok(response) => response,
        Err(error) => {
            return (
                temporary_failure(format!("downstream REST request failed: {error}")),
                0,
            );
        }
    };
    let status = response.status();
    let headers = response.headers().clone();
    if headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_PROXY_RESPONSE_BYTES)
    {
        return (
            temporary_failure("downstream REST response exceeds publication proxy limit"),
            0,
        );
    }
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            return (
                temporary_failure(format!("read downstream REST response: {error}")),
                0,
            );
        }
    };
    if bytes.len() > MAX_PROXY_RESPONSE_BYTES {
        return (
            temporary_failure("downstream REST response exceeds publication proxy limit"),
            0,
        );
    }
    let response_body_bytes = bytes.len() as u64;
    (
        response_with_headers(status, headers, bytes),
        response_body_bytes,
    )
}

fn response_with_headers(status: StatusCode, headers: HeaderMap, bytes: Bytes) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    for (name, value) in &headers {
        if name != axum::http::header::CONTENT_LENGTH
            && name != axum::http::header::TRANSFER_ENCODING
        {
            response.headers_mut().insert(name, value.clone());
        }
    }
    response
}

fn json_response(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

fn wire_error(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    json_response(
        status,
        json!({"error": {"kind": kind, "message": message.into()}}),
    )
}

fn temporary_failure(message: impl Into<String>) -> Response {
    wire_error(StatusCode::SERVICE_UNAVAILABLE, "ambiguous", message)
}

fn known_not_dispatched(message: impl Into<String>) -> Response {
    wire_error(StatusCode::BAD_REQUEST, "known-not-dispatched", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_standard_stage_create_and_table_commit_requests() {
        assert_eq!(
            standard_catalog_action(
                &Method::POST,
                "/v1/namespaces/ns/tables",
                br#"{"stage-create":true}"#,
            ),
            Some(PublicationAction::StageCreate)
        );
        assert_eq!(
            standard_catalog_action(
                &Method::POST,
                "/v1/namespaces/ns/tables/t",
                br#"{"requirements":[],"updates":[]}"#,
            ),
            Some(PublicationAction::TableCommit)
        );
        assert_eq!(
            standard_catalog_action(
                &Method::POST,
                "/extensions/private-action/publish",
                br#"{}"#,
            ),
            None
        );
        assert_eq!(
            standard_catalog_action(&Method::GET, "/v1/namespaces", br#""#),
            Some(PublicationAction::NamespaceList)
        );
        assert_eq!(
            standard_catalog_action(&Method::GET, "/v1/namespaces/ns/tables/t", br#""#),
            Some(PublicationAction::TableLoad)
        );
        assert_eq!(
            mutation_target(
                &Method::POST,
                Some(PublicationAction::StageCreate),
                "/v1/namespaces/ns/tables",
                br#"{"name":"mv","stage-create":true}"#,
            ),
            Some((TargetMutationAction::StageCreate, "ns".into(), "mv".into()))
        );
        assert_eq!(
            mutation_target(
                &Method::POST,
                Some(PublicationAction::TableCommit),
                "/v1/namespaces/ns/tables/mv",
                br#"{"requirements":[],"updates":[]}"#,
            ),
            Some((TargetMutationAction::TableCommit, "ns".into(), "mv".into()))
        );
        assert_eq!(
            mutation_target(
                &Method::POST,
                None,
                "/v1/namespaces/ns/tables/mv",
                br#"{"unexpected":"mutation"}"#,
            ),
            Some((TargetMutationAction::Other, "ns".into(), "mv".into()))
        );
    }

    #[test]
    fn one_shot_fault_only_consumes_its_matching_standard_action() {
        let state = AppState {
            downstream: "http://example.invalid".to_string(),
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            next_fault: Arc::new(Mutex::new(NextFaultState {
                armed: Some(ArmedNextFault {
                    arm_id: "one".to_string(),
                    action: PublicationAction::TableCommit,
                    fault: PublicationFault::AfterCommitBeforeResponse,
                    release: Arc::new(tokio::sync::Notify::new()),
                }),
                status: None,
                ..NextFaultState::default()
            })),
            next_fault_sequence: Arc::new(AtomicU64::new(1)),
            mutations: Arc::new(Mutex::new(BTreeMap::new())),
            traffic: Arc::new(Mutex::new(TrafficCounters::default())),
        };
        assert!(take_matching_fault(&state, PublicationAction::StageCreate).is_none());
        let consumed = take_matching_fault(&state, PublicationAction::TableCommit)
            .expect("matching fault is consumed");
        assert_eq!(consumed.arm_id, "one");
        assert_eq!(consumed.fault, PublicationFault::AfterCommitBeforeResponse);
        assert!(take_matching_fault(&state, PublicationAction::TableCommit).is_none());
        let trace = &state.next_fault.lock().unwrap().trace;
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[0].sequence, 1);
        assert_eq!(trace[0].event, "matched");
    }

    #[test]
    fn parses_explicit_pre_requirement_hold() {
        assert_eq!(
            parse_fault("before-requirement-check-hold-for-concurrent-shell").unwrap(),
            PublicationFault::BeforeRequirementCheckHoldForConcurrentShell
        );
    }

    #[test]
    fn records_forward_success_and_response_drop_for_one_arm() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let downstream_address = listener.local_addr().unwrap();
        let downstream = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).unwrap();
            assert!(
                String::from_utf8_lossy(&request[..read])
                    .contains("POST /v1/namespaces/ns/tables/t")
            );
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .unwrap();
        });

        let fixture = FixtureHandle::start(format!("http://{downstream_address}")).unwrap();
        let control = fixture.control().unwrap();
        let guard = control
            .arm_next("table-commit", "after-commit-before-response")
            .unwrap();
        let response = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/v1/namespaces/ns/tables/t", fixture.uri()))
            .json(&json!({"requirements": [], "updates": []}))
            .send()
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let evidence = guard.finish().unwrap();
        let counts = control.mutation_counts("ns", "t");
        assert_eq!(counts.table_commit_forwarded, 1);
        assert_eq!(counts.table_commit_succeeded, 1);
        let traffic = control.traffic_snapshot();
        assert_eq!(traffic.requests, 1);
        assert_eq!(traffic.by_method.get("POST"), Some(&1));
        assert_eq!(traffic.by_status.get(&200), Some(&1));
        assert!(traffic.request_body_bytes > 0);
        assert_eq!(traffic.response_body_bytes, 2);
        assert_eq!(traffic.table_commit_requests, 1);
        assert!(traffic.table_commit_roundtrip_nanos > 0);
        let wire_traffic: TrafficCounters =
            reqwest::blocking::get(format!("{}/_fixture/catalog-traffic", fixture.uri()))
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(wire_traffic, traffic);
        assert_eq!(
            evidence.summary(),
            "armed -> matched -> request-forwarded -> downstream-response -> downstream-success -> response-dropped-after-downstream-success -> control-release"
        );
        downstream.join().unwrap();
    }
}
