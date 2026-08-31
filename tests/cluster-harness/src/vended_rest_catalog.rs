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

//! Bounded REST Catalog proxy for vended-S3 acceptance tests.
//!
//! The proxy forwards every standard catalog operation to a real downstream
//! catalog. It changes only successful vended table-load and staged-create
//! responses by attaching standard Iceberg `storage-credentials`, then serves
//! one local refresh endpoint with a rotated credential. Audit state records
//! counters and public key ids only; it never retains HTTP headers, bodies, or
//! secret values.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::any;
use novarocks_secret::SecretValue;
use serde_json::{Value, json};

const ACCESS_DELEGATION_HEADER: &str = "x-iceberg-access-delegation";
const VENDED_CREDENTIALS: &str = "vended-credentials";
const REFRESH_PATH: &str = "/_fixture/vended-credentials/refresh";
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_KEY_ID_COUNTERS: usize = 2;

/// Secret material for one response-local test credential.
///
/// It has no `Debug` implementation, so a fixture configuration cannot leak
/// its secret access key or session token through diagnostics.
#[derive(Clone)]
pub struct VendedS3Credential {
    access_key_id: String,
    access_key_secret: SecretValue,
    session_token: SecretValue,
    not_after_unix_ms: Option<u64>,
}

impl VendedS3Credential {
    pub fn new(
        access_key_id: impl Into<String>,
        access_key_secret: SecretValue,
        session_token: SecretValue,
    ) -> Result<Self> {
        let access_key_id = access_key_id.into();
        ensure!(
            !access_key_id.trim().is_empty(),
            "vended REST fixture access key id must not be empty"
        );
        ensure!(
            !access_key_secret.is_empty(),
            "vended REST fixture secret access key must not be empty"
        );
        ensure!(
            !session_token.is_empty(),
            "vended REST fixture session token must not be empty"
        );
        Ok(Self {
            access_key_id,
            access_key_secret,
            session_token,
            not_after_unix_ms: None,
        })
    }

    /// Pins the response's declared expiry to the authority that issued this
    /// temporary credential. Unit fixtures without a real STS issuer leave it
    /// unset and use their configured synthetic TTL instead.
    pub fn with_not_after_unix_ms(mut self, not_after_unix_ms: u64) -> Result<Self> {
        ensure!(
            not_after_unix_ms > unix_ms(),
            "vended REST fixture credential expiration must be in the future"
        );
        self.not_after_unix_ms = Some(not_after_unix_ms);
        Ok(self)
    }

    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }
}

impl fmt::Debug for VendedS3Credential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VendedS3Credential")
            .field("access_key_id", &self.access_key_id)
            .field("access_key_secret", &"REDACTED")
            .field("session_token", &"REDACTED")
            .finish()
    }
}

/// Fixed, bounded input for one vended REST proxy.
#[derive(Clone, Debug)]
pub struct VendedRestCatalogConfig {
    pub downstream: String,
    pub scope_prefix: String,
    pub initial: VendedS3Credential,
    pub rotated: VendedS3Credential,
    pub initial_ttl: Duration,
    pub refresh_ttl: Duration,
    /// Test-only behavior for the fixture-owned refresh endpoint. Production
    /// catalog responses are always forwarded unchanged except for the
    /// standard vended credential injection described by this fixture.
    pub refresh_behavior: VendedRefreshBehavior,
    /// Test-only result after a downstream table commit has succeeded. This
    /// models losing the response without issuing a replayed catalog mutation.
    pub table_commit_response_behavior: VendedTableCommitResponseBehavior,
    /// Holds exactly one successful existing-table commit response after the
    /// downstream catalog has applied it. This models a response-loss window
    /// without manufacturing or replaying a catalog side effect.
    pub hold_first_table_commit_response: bool,
}

/// Bounded test-only responses from the fixture's local credential refresh
/// endpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VendedRefreshBehavior {
    #[default]
    IssueRotatedCredential,
    FailUnavailable,
}

/// Bounded test-only result returned after a downstream table commit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VendedTableCommitResponseBehavior {
    #[default]
    Forward,
    FailUnavailableAfterSideEffect,
}

impl VendedRestCatalogConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.downstream.starts_with("http://") || self.downstream.starts_with("https://"),
            "vended REST fixture downstream must be an HTTP(S) URL"
        );
        ensure!(
            self.scope_prefix.starts_with("s3://"),
            "vended REST fixture credential scope must be an s3:// prefix"
        );
        ensure!(
            !self.scope_prefix.contains('?') && !self.scope_prefix.contains('#'),
            "vended REST fixture credential scope must not contain query or fragment"
        );
        ensure!(
            !self.initial_ttl.is_zero() && !self.refresh_ttl.is_zero(),
            "vended REST fixture credential TTLs must be positive"
        );
        ensure!(
            self.initial.access_key_id != self.rotated.access_key_id,
            "vended REST fixture initial and rotated key ids must differ"
        );
        Ok(())
    }
}

/// Non-secret, bounded fixture observations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VendedRestCatalogAudit {
    pub table_loads: u64,
    pub staged_creates: u64,
    pub table_commits: u64,
    pub refreshes: u64,
    pub refresh_failures: u64,
    pub issued_key_ids: BTreeMap<String, u64>,
}

pub struct VendedRestCatalogFixture {
    uri: String,
    audit: Arc<Mutex<VendedRestCatalogAudit>>,
    commit_response_hold: Option<Arc<CommitResponseHold>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl VendedRestCatalogFixture {
    pub fn start(config: VendedRestCatalogConfig) -> Result<Self> {
        config.validate()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .context("bind vended REST catalog fixture listener")?;
        listener
            .set_nonblocking(true)
            .context("set vended REST catalog fixture listener nonblocking")?;
        let address = listener
            .local_addr()
            .context("read vended REST catalog fixture address")?;
        ensure!(
            address.ip().is_loopback(),
            "vended REST catalog fixture bound a non-loopback address"
        );
        let uri = format!("http://{address}");
        let audit = Arc::new(Mutex::new(VendedRestCatalogAudit::default()));
        let state = AppState {
            downstream: config.downstream.trim_end_matches('/').to_string(),
            scope_prefix: config.scope_prefix,
            initial: config.initial,
            rotated: config.rotated,
            initial_ttl: config.initial_ttl,
            refresh_ttl: config.refresh_ttl,
            refresh_behavior: config.refresh_behavior,
            table_commit_response_behavior: config.table_commit_response_behavior,
            refresh_endpoint: format!("{uri}{REFRESH_PATH}"),
            client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .context("build vended REST catalog fixture client")?,
            audit: Arc::clone(&audit),
            commit_response_hold: config
                .hold_first_table_commit_response
                .then(|| Arc::new(CommitResponseHold::default())),
        };
        let commit_response_hold = state.commit_response_hold.clone();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("vended-rest-catalog-fixture".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build vended REST catalog fixture runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("adopt vended REST catalog fixture listener");
                    ready_tx
                        .send(())
                        .expect("signal vended REST catalog fixture ready");
                    axum::serve(listener, router(state))
                        .with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .expect("serve vended REST catalog fixture");
                });
            })
            .context("start vended REST catalog fixture thread")?;
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .context("wait for vended REST catalog fixture readiness")?;
        Ok(Self {
            uri,
            audit,
            commit_response_hold,
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn audit(&self) -> VendedRestCatalogAudit {
        self.audit
            .lock()
            .expect("vended REST catalog fixture audit lock poisoned")
            .clone()
    }

    /// Wait until the fixture has forwarded one table commit successfully and
    /// is holding only its response. The table mutation is already durable at
    /// the downstream authority when this returns.
    pub fn wait_for_held_table_commit(&self, timeout: Duration) -> Result<()> {
        let hold = self.commit_response_hold.as_ref().ok_or_else(|| {
            anyhow::anyhow!("vended REST fixture was not configured to hold a table commit")
        })?;
        let deadline = std::time::Instant::now() + timeout;
        while !hold.observed.load(Ordering::SeqCst) {
            ensure!(
                std::time::Instant::now() < deadline,
                "timed out waiting for a successful vended REST table commit to be held; audit={:?}",
                self.audit()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    /// Releases the one configured response hold. It is idempotent so failure
    /// cleanup can safely call it after an attempt terminalizes.
    pub fn release_held_table_commit_response(&self) {
        if let Some(hold) = &self.commit_response_hold {
            hold.release();
        }
    }
}

impl fmt::Debug for VendedRestCatalogFixture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VendedRestCatalogFixture")
            .field("uri", &self.uri)
            .field("audit", &self.audit())
            .finish()
    }
}

impl Drop for VendedRestCatalogFixture {
    fn drop(&mut self) {
        self.release_held_table_commit_response();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct AppState {
    downstream: String,
    scope_prefix: String,
    initial: VendedS3Credential,
    rotated: VendedS3Credential,
    initial_ttl: Duration,
    refresh_ttl: Duration,
    refresh_behavior: VendedRefreshBehavior,
    table_commit_response_behavior: VendedTableCommitResponseBehavior,
    refresh_endpoint: String,
    client: reqwest::Client,
    audit: Arc<Mutex<VendedRestCatalogAudit>>,
    commit_response_hold: Option<Arc<CommitResponseHold>>,
}

#[derive(Default)]
struct CommitResponseHold {
    observed: AtomicBool,
    released: AtomicBool,
    release: tokio::sync::Notify,
}

impl CommitResponseHold {
    async fn hold_after_side_effect(&self) {
        if self
            .observed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        while !self.released.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .fallback(any(dispatch))
        .with_state(Arc::new(state))
}

async fn dispatch(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    if parts.method == Method::GET && parts.uri.path() == REFRESH_PATH {
        return refresh_response(&state);
    }
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(format!("read catalog request body: {error}")),
    };
    let action = vended_action(&parts.method, &parts.uri, &parts.headers, &bytes);
    let is_table_commit = is_existing_table_commit(&parts.method, &parts.uri);
    let hold_table_commit = is_table_commit
        .then(|| state.commit_response_hold.clone())
        .flatten();
    let response = proxy_request(&state, parts.method, parts.uri, parts.headers, bytes).await;
    if response.status().is_success() {
        if let Some(hold) = hold_table_commit {
            record_issue(&state.audit, VendedAction::TableCommit, "");
            hold.hold_after_side_effect().await;
        }
        if is_table_commit
            && state.table_commit_response_behavior
                == VendedTableCommitResponseBehavior::FailUnavailableAfterSideEffect
        {
            record_issue(&state.audit, VendedAction::TableCommit, "");
            return temporary_failure(
                "configured response loss after vended table commit side effect",
            );
        }
    }
    match action {
        Some(action) if response.status().is_success() => {
            inject_credentials(&state, action, response).await
        }
        _ => response,
    }
}

#[derive(Clone, Copy)]
enum VendedAction {
    TableLoad,
    StagedCreate,
    TableCommit,
    Refresh,
}

fn is_existing_table_commit(method: &Method, uri: &Uri) -> bool {
    method == Method::POST && uri.path().contains("/tables/") && !uri.path().ends_with("/tables")
}

fn vended_action(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<VendedAction> {
    let vended = headers
        .get(ACCESS_DELEGATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case(VENDED_CREDENTIALS));
    if !vended || !uri.path().contains("/v1/") {
        return None;
    }
    if method == Method::GET && uri.path().contains("/tables/") {
        return Some(VendedAction::TableLoad);
    }
    if method != Method::POST || !uri.path().ends_with("/tables") {
        return None;
    }
    serde_json::from_slice::<Value>(body)
        .ok()
        .filter(|value| value.get("stage-create").and_then(Value::as_bool) == Some(true))
        .map(|_| VendedAction::StagedCreate)
}

async fn proxy_request(
    state: &AppState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    let url = format!(
        "{}{}",
        state.downstream,
        uri.path_and_query().map_or("/", |value| value.as_str())
    );
    let mut outbound = state.client.request(method, url).body(bytes);
    for (name, value) in &headers {
        if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
            outbound = outbound.header(name, value);
        }
    }
    let response = match outbound.send().await {
        Ok(response) => response,
        Err(error) => return temporary_failure(format!("forward catalog request: {error}")),
    };
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = match response.bytes().await {
        Ok(bytes) if bytes.len() <= MAX_BODY_BYTES => bytes,
        Ok(_) => return temporary_failure("downstream catalog response exceeds fixture limit"),
        Err(error) => {
            return temporary_failure(format!("read downstream catalog response: {error}"));
        }
    };
    response_with_headers(status, headers, bytes)
}

async fn inject_credentials(
    state: &AppState,
    action: VendedAction,
    response: Response,
) -> Response {
    let (parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(format!("read downstream response body: {error}")),
    };
    let mut value = match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => value,
        Err(error) => return temporary_failure(format!("decode downstream catalog JSON: {error}")),
    };
    let key_id = state.initial.access_key_id.clone();
    let credential = storage_credential(
        state,
        &state.initial,
        state.initial_ttl,
        Some(state.refresh_endpoint.as_str()),
    );
    let Some(object) = value.as_object_mut() else {
        return temporary_failure("downstream catalog response is not a JSON object");
    };
    object.insert(
        "storage-credentials".to_string(),
        Value::Array(vec![credential]),
    );
    let bytes = match serde_json::to_vec(&value) {
        Ok(bytes) => Bytes::from(bytes),
        Err(error) => return temporary_failure(format!("encode vended catalog response: {error}")),
    };
    record_issue(&state.audit, action, &key_id);
    response_with_headers(parts.status, parts.headers, bytes)
}

fn refresh_response(state: &AppState) -> Response {
    if state.refresh_behavior == VendedRefreshBehavior::FailUnavailable {
        record_refresh_failure(&state.audit);
        return temporary_failure("configured vended credential refresh failure");
    }
    let key_id = state.rotated.access_key_id.clone();
    let credential = storage_credential(state, &state.rotated, state.refresh_ttl, None);
    record_issue(&state.audit, VendedAction::Refresh, &key_id);
    json_response(json!({"storage-credentials": [credential]}))
}

fn record_issue(audit: &Mutex<VendedRestCatalogAudit>, action: VendedAction, key_id: &str) {
    let mut audit = audit
        .lock()
        .expect("vended REST catalog fixture audit lock poisoned");
    match action {
        VendedAction::TableLoad => audit.table_loads += 1,
        VendedAction::StagedCreate => audit.staged_creates += 1,
        VendedAction::TableCommit => audit.table_commits += 1,
        VendedAction::Refresh => audit.refreshes += 1,
    }
    if audit.issued_key_ids.len() < MAX_KEY_ID_COUNTERS || audit.issued_key_ids.contains_key(key_id)
    {
        *audit.issued_key_ids.entry(key_id.to_string()).or_default() += 1;
    }
}

fn record_refresh_failure(audit: &Mutex<VendedRestCatalogAudit>) {
    let mut audit = audit
        .lock()
        .expect("vended REST catalog fixture audit lock poisoned");
    audit.refreshes += 1;
    audit.refresh_failures += 1;
}

fn storage_credential(
    state: &AppState,
    credential: &VendedS3Credential,
    ttl: Duration,
    refresh_endpoint: Option<&str>,
) -> Value {
    let expiration = credential.not_after_unix_ms.unwrap_or_else(|| {
        unix_ms().saturating_add(ttl.as_millis().try_into().unwrap_or(u64::MAX))
    });
    let mut config = serde_json::Map::from_iter([
        (
            "s3.access-key-id".to_string(),
            Value::String(credential.access_key_id.clone()),
        ),
        (
            "s3.secret-access-key".to_string(),
            Value::String(credential.access_key_secret.expose_secret().to_string()),
        ),
        (
            "s3.session-token".to_string(),
            Value::String(credential.session_token.expose_secret().to_string()),
        ),
        (
            "s3.session-token-expires-at-ms".to_string(),
            Value::String(expiration.to_string()),
        ),
    ]);
    if let Some(refresh_endpoint) = refresh_endpoint {
        config.insert(
            "client.refresh-credentials-enabled".to_string(),
            Value::String("true".to_string()),
        );
        config.insert(
            "client.refresh-credentials-endpoint".to_string(),
            Value::String(refresh_endpoint.to_string()),
        );
    }
    json!({"prefix": state.scope_prefix, "config": config})
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
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

fn json_response(value: Value) -> Response {
    let mut response = Response::new(Body::from(value.to_string()));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}

fn temporary_failure(message: impl Into<String>) -> Response {
    let mut response = Response::new(Body::from(
        json!({"error": {"kind": "fixture", "message": message.into()}}).to_string(),
    ));
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::http::HeaderValue;

    use super::*;

    async fn downstream() -> (String, tokio::sync::oneshot::Sender<()>, Arc<AtomicU64>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind downstream");
        let address = listener.local_addr().expect("downstream address");
        let requests = Arc::new(AtomicU64::new(0));
        let state = Arc::clone(&requests);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let app = Router::new().fallback(any(move |request: Request| {
                let state = Arc::clone(&state);
                async move {
                    state.fetch_add(1, Ordering::Relaxed);
                    let vended = request
                        .headers()
                        .get(ACCESS_DELEGATION_HEADER)
                        .and_then(|value| value.to_str().ok());
                    assert_eq!(vended, Some(VENDED_CREDENTIALS));
                    json_response(json!({"metadata": {"table-uuid": "test"}}))
                }
            }));
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve downstream");
        });
        (format!("http://{address}"), shutdown, requests)
    }

    fn credential(id: &str) -> VendedS3Credential {
        VendedS3Credential::new(
            id,
            SecretValue::new("fixture-secret"),
            SecretValue::new("fixture-token"),
        )
        .expect("credential")
    }

    #[tokio::test]
    async fn vended_table_load_is_forwarded_and_receives_a_redacted_audit_only() {
        let (downstream, shutdown, requests) = downstream().await;
        let fixture = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream,
            scope_prefix: "s3://fixture/warehouse/".to_string(),
            initial: credential("initial-key"),
            rotated: credential("rotated-key"),
            initial_ttl: Duration::from_secs(30),
            refresh_ttl: Duration::from_secs(30),
            refresh_behavior: Default::default(),
            table_commit_response_behavior: Default::default(),
            hold_first_table_commit_response: false,
        })
        .expect("fixture");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response: Value = client
            .get(format!("{}/v1/namespaces/db/tables/t", fixture.uri()))
            .header(
                ACCESS_DELEGATION_HEADER,
                HeaderValue::from_static(VENDED_CREDENTIALS),
            )
            .send()
            .await
            .expect("table response")
            .error_for_status()
            .expect("table status")
            .json()
            .await
            .expect("table JSON");
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        assert_eq!(
            response.pointer("/storage-credentials/0/config/s3.access-key-id"),
            Some(&Value::String("initial-key".to_string()))
        );
        assert_eq!(
            response.pointer("/storage-credentials/0/config/client.refresh-credentials-enabled"),
            Some(&Value::String("true".to_string()))
        );
        let audit = fixture.audit();
        assert_eq!(audit.table_loads, 1);
        assert_eq!(audit.staged_creates, 0);
        assert_eq!(audit.refreshes, 0);
        assert_eq!(
            audit.issued_key_ids,
            BTreeMap::from([("initial-key".to_string(), 1)])
        );
        assert!(!format!("{fixture:?}").contains("fixture-secret"));
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn staged_create_and_refresh_return_the_rotated_key_without_forwarding_refresh() {
        let (downstream, shutdown, requests) = downstream().await;
        let fixture = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream,
            scope_prefix: "s3://fixture/warehouse/".to_string(),
            initial: credential("initial-key"),
            rotated: credential("rotated-key"),
            initial_ttl: Duration::from_secs(30),
            refresh_ttl: Duration::from_secs(30),
            refresh_behavior: Default::default(),
            table_commit_response_behavior: Default::default(),
            hold_first_table_commit_response: false,
        })
        .expect("fixture");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let staged: Value = client
            .post(format!("{}/v1/namespaces/db/tables", fixture.uri()))
            .header(ACCESS_DELEGATION_HEADER, VENDED_CREDENTIALS)
            .json(&json!({"stage-create": true}))
            .send()
            .await
            .expect("stage response")
            .error_for_status()
            .expect("stage status")
            .json()
            .await
            .expect("stage JSON");
        assert_eq!(
            staged.pointer("/storage-credentials/0/config/s3.access-key-id"),
            Some(&Value::String("initial-key".to_string()))
        );
        let refresh: Value = client
            .get(format!("{}{}", fixture.uri(), REFRESH_PATH))
            .send()
            .await
            .expect("refresh response")
            .error_for_status()
            .expect("refresh status")
            .json()
            .await
            .expect("refresh JSON");
        assert_eq!(
            requests.load(Ordering::Relaxed),
            1,
            "refresh must not reach downstream"
        );
        assert_eq!(
            refresh.pointer("/storage-credentials/0/config/s3.access-key-id"),
            Some(&Value::String("rotated-key".to_string()))
        );
        let audit = fixture.audit();
        assert_eq!(audit.staged_creates, 1);
        assert_eq!(audit.refreshes, 1);
        assert_eq!(
            audit.issued_key_ids,
            BTreeMap::from([
                ("initial-key".to_string(), 1),
                ("rotated-key".to_string(), 1)
            ])
        );
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn table_commit_response_loss_preserves_one_downstream_side_effect() {
        let (downstream, shutdown, requests) = downstream().await;
        let fixture = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream,
            scope_prefix: "s3://fixture/warehouse/".to_string(),
            initial: credential("initial-key"),
            rotated: credential("rotated-key"),
            initial_ttl: Duration::from_secs(30),
            refresh_ttl: Duration::from_secs(30),
            refresh_behavior: Default::default(),
            table_commit_response_behavior:
                VendedTableCommitResponseBehavior::FailUnavailableAfterSideEffect,
            hold_first_table_commit_response: false,
        })
        .expect("fixture");
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/v1/namespaces/db/tables/t", fixture.uri()))
            .header(ACCESS_DELEGATION_HEADER, VENDED_CREDENTIALS)
            .send()
            .await
            .expect("commit response");
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        let audit = fixture.audit();
        assert_eq!(audit.table_commits, 1);
        assert_eq!(audit.table_loads, 0);
        assert_eq!(audit.refreshes, 0);
        assert_eq!(audit.refresh_failures, 0);
        let _ = shutdown.send(());
    }
}
