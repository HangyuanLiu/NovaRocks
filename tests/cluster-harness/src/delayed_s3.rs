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

//! Loopback S3 read proxy for repeatable remote-latency measurements.
//!
//! The object store remains the source of truth. This proxy delays each real
//! GET or HEAD, forwards the signed request, and counts the actual requests.
//! It never records credentials, headers, object names, or response bodies.

use anyhow::{Context, Result, ensure};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::connect_info::Connected;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{Method, StatusCode, Version, header};
use axum::response::Response;
use axum::routing::any;
use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::task::{Context as TaskContext, Poll};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle as TokioJoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tower::{Layer, Service};

const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_EVENT_ENTRIES: usize = 32_768;

/// An opaque fixture label. The path is matched exactly but is never logged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelayedS3ReadMatch {
    pub method: Method,
    pub object_path: String,
    pub object_id: String,
    pub range: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DelayedS3HoldMode {
    BeforeForward,
    AfterBodyPrefix { bytes: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DelayedS3ReadClass {
    Footer,
    Index,
    Data,
    Metadata,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DelayedS3EventKind {
    Arrived,
    UpstreamStarted,
    UpstreamHeaders,
    BodyPrefixQueued,
    BodyComplete,
    BodyClosed,
    UpstreamError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DelayedS3ConnectionEventKind {
    Accepted,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DelayedS3ConnectionEvent {
    pub kind: DelayedS3ConnectionEventKind,
    pub connection_id: u64,
    /// Monotonic milliseconds since this proxy was started.
    pub elapsed_millis: u128,
}

/// No credential, raw path, query string, or response body is retained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelayedS3Event {
    pub kind: DelayedS3EventKind,
    pub request_id: u64,
    pub connection_id: u64,
    /// Monotonic milliseconds since this proxy was started.
    pub elapsed_millis: u128,
    pub protocol: String,
    pub method: Method,
    pub object_id: Option<String>,
    pub read_class: Option<DelayedS3ReadClass>,
    pub range: Option<String>,
    pub bytes: u64,
}

#[derive(Clone, Debug)]
pub struct DelayedS3Config {
    pub downstream: String,
    pub delay: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DelayedS3Snapshot {
    pub gets: u64,
    pub heads: u64,
    pub upstream_errors: u64,
    pub observed_be_connections: u64,
    /// Actual proxy-to-store connector attempts and established connections.
    pub upstream_connect_attempts: u64,
    pub upstream_connections_established: u64,
    pub upstream_http1_responses: u64,
    pub upstream_http2_responses: u64,
    pub upstream_other_protocol_responses: u64,
    pub event_overflow: u64,
    pub peak_inflight_reads: u64,
    pub peak_buffered_response_bytes: u64,
    /// Bytes actually consumed from the proxy's upstream HTTP response,
    /// including partial reads that later failed or were cancelled.
    pub upstream_bytes_read: u64,
    /// Bytes in responses whose whole body the proxy finished preparing.
    /// This does not prove that the BE consumed the body.
    pub completed_response_bytes: u64,
}

struct ProxyState {
    started_at: Instant,
    downstream: reqwest::Url,
    delay: Duration,
    client: reqwest::Client,
    gets: AtomicU64,
    heads: AtomicU64,
    upstream_errors: AtomicU64,
    next_request_id: AtomicU64,
    next_connection_id: AtomicU64,
    upstream_connect_attempts: Arc<AtomicU64>,
    upstream_connections_established: Arc<AtomicU64>,
    upstream_http1_responses: AtomicU64,
    upstream_http2_responses: AtomicU64,
    upstream_other_protocol_responses: AtomicU64,
    connection_events: Mutex<Vec<DelayedS3ConnectionEvent>>,
    events: Mutex<Vec<DelayedS3Event>>,
    event_overflow: AtomicU64,
    inflight_reads: AtomicU64,
    peak_inflight_reads: AtomicU64,
    peak_buffered_response_bytes: AtomicU64,
    upstream_bytes_read: AtomicU64,
    completed_response_bytes: AtomicU64,
    object_labels: Mutex<BTreeMap<String, String>>,
    read_classes: Mutex<BTreeMap<(String, String, Option<String>), DelayedS3ReadClass>>,
    holds: Mutex<Vec<std::sync::Weak<ReadHoldState>>>,
    body_tasks: Mutex<Vec<TokioJoinHandle<()>>>,
    next_hold: Mutex<Option<Arc<ReadHoldState>>>,
}

#[derive(Clone)]
struct UpstreamConnectCounterLayer {
    attempts: Arc<AtomicU64>,
    established: Arc<AtomicU64>,
}

#[derive(Clone)]
struct UpstreamConnectCounter<S> {
    inner: S,
    attempts: Arc<AtomicU64>,
    established: Arc<AtomicU64>,
}

impl<S> Layer<S> for UpstreamConnectCounterLayer {
    type Service = UpstreamConnectCounter<S>;

    fn layer(&self, inner: S) -> Self::Service {
        UpstreamConnectCounter {
            inner,
            attempts: Arc::clone(&self.attempts),
            established: Arc::clone(&self.established),
        }
    }
}

impl<S, Request> Service<Request> for UpstreamConnectCounter<S>
where
    S: Service<Request> + Clone + Send + Sync + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        let future = self.inner.call(request);
        let established = Arc::clone(&self.established);
        Box::pin(async move {
            let result = future.await;
            if result.is_ok() {
                established.fetch_add(1, Ordering::AcqRel);
            }
            result
        })
    }
}

struct InflightRead {
    state: Arc<ProxyState>,
}

impl InflightRead {
    fn start(state: &Arc<ProxyState>) -> Self {
        let count = state.inflight_reads.fetch_add(1, Ordering::AcqRel) + 1;
        state.peak_inflight_reads.fetch_max(count, Ordering::AcqRel);
        Self {
            state: Arc::clone(state),
        }
    }
}

impl Drop for InflightRead {
    fn drop(&mut self) {
        self.state.inflight_reads.fetch_sub(1, Ordering::AcqRel);
    }
}

// Axum creates ConnectInfo at accept time. Its Connected callback has no
// per-server state argument, so the loopback listener address resolves the
// matching fixture here. Entries are removed on proxy teardown.
static PROXIES_BY_LISTENER: OnceLock<Mutex<BTreeMap<SocketAddr, Weak<ProxyState>>>> =
    OnceLock::new();

#[derive(Clone)]
struct ObservedConnection {
    lifetime: Arc<ConnectionLifetime>,
}

struct ConnectionLifetime {
    id: u64,
    state: Weak<ProxyState>,
}

impl Drop for ConnectionLifetime {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.record_connection(DelayedS3ConnectionEventKind::Closed, self.id);
        }
    }
}

impl Connected<axum::serve::IncomingStream<'_>> for ObservedConnection {
    fn connect_info(stream: axum::serve::IncomingStream<'_>) -> Self {
        let listener = stream
            .local_addr()
            .expect("read delayed S3 listener address");
        let state = PROXIES_BY_LISTENER
            .get()
            .expect("delayed S3 listener registry")
            .lock()
            .expect("delayed S3 listener registry lock")
            .get(&listener)
            .and_then(Weak::upgrade)
            .expect("registered delayed S3 listener");
        let id = state.next_connection_id.fetch_add(1, Ordering::AcqRel);
        state.record_connection(DelayedS3ConnectionEventKind::Accepted, id);
        Self {
            lifetime: Arc::new(ConnectionLifetime {
                id,
                state: Arc::downgrade(&state),
            }),
        }
    }
}

struct ReadHoldState {
    matcher: HoldMatcher,
    mode: DelayedS3HoldMode,
    entered: Mutex<bool>,
    entered_changed: Condvar,
    forwarded: Mutex<bool>,
    forwarded_changed: Condvar,
    prefix_queued: Mutex<bool>,
    prefix_changed: Condvar,
    body_closed: Mutex<bool>,
    body_closed_changed: Condvar,
    release: watch::Sender<bool>,
}

enum HoldMatcher {
    Any,
    Suffix(String),
    Exact(DelayedS3ReadMatch),
}

/// One real GET/HEAD hold. The request remains owned by the proxy until the
/// test explicitly releases it, so a cancellation test cannot pass by sleep.
pub struct DelayedS3ReadHold {
    state: Arc<ReadHoldState>,
}

impl DelayedS3ReadHold {
    pub fn wait_until_entered(&self, timeout: Duration) -> Result<()> {
        let entered = self.state.entered.lock().expect("S3 hold entry lock");
        let (entered, _) = self
            .state
            .entered_changed
            .wait_timeout_while(entered, timeout, |entered| !*entered)
            .expect("S3 hold entry wait");
        ensure!(
            *entered,
            "timed out waiting for real S3 GET/HEAD to enter hold"
        );
        Ok(())
    }

    pub fn release(&self) {
        self.state.release.send_replace(true);
    }

    /// Wait for the held object response to be fully read from the real
    /// downstream store after release. Entry alone is not I/O completion.
    pub fn wait_until_forwarded(&self, timeout: Duration) -> Result<()> {
        let forwarded = self.state.forwarded.lock().expect("S3 hold forward lock");
        let (forwarded, _) = self
            .state
            .forwarded_changed
            .wait_timeout_while(forwarded, timeout, |forwarded| !*forwarded)
            .expect("S3 hold forward wait");
        ensure!(
            *forwarded,
            "timed out waiting for held S3 read to finish forwarding"
        );
        Ok(())
    }

    /// The first body bytes have been queued to the BE-facing response stream.
    /// The caller should read those bytes before using a cancellation assertion.
    pub fn wait_until_prefix_queued(&self, timeout: Duration) -> Result<()> {
        wait_for_flag(
            &self.state.prefix_queued,
            &self.state.prefix_changed,
            timeout,
            "timed out waiting for held S3 body prefix",
        )
    }

    /// The BE-facing response body was dropped while the fixture was holding
    /// the rest of the body. Forwarding alone never satisfies this observation.
    pub fn wait_until_body_closed(&self, timeout: Duration) -> Result<()> {
        wait_for_flag(
            &self.state.body_closed,
            &self.state.body_closed_changed,
            timeout,
            "timed out waiting for held S3 response body to close",
        )
    }
}

fn wait_for_flag(
    flag: &Mutex<bool>,
    changed: &Condvar,
    timeout: Duration,
    message: &'static str,
) -> Result<()> {
    let value = flag.lock().expect("S3 hold observation lock");
    let (value, _) = changed
        .wait_timeout_while(value, timeout, |value| !*value)
        .expect("S3 hold observation wait");
    ensure!(*value, "{message}");
    Ok(())
}

impl HoldMatcher {
    fn matches(&self, request: &Request) -> bool {
        match self {
            Self::Any => true,
            Self::Suffix(suffix) => request.uri().path().ends_with(suffix),
            Self::Exact(key) => {
                request.method() == key.method
                    && request.uri().path() == key.object_path
                    && request
                        .headers()
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        == key.range.as_deref()
            }
        }
    }
}

impl Drop for DelayedS3ReadHold {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct DelayedS3Proxy {
    endpoint: String,
    listener_addr: SocketAddr,
    state: Arc<ProxyState>,
    shutdown: Option<oneshot::Sender<()>>,
    server_thread: Option<JoinHandle<()>>,
}

impl DelayedS3Proxy {
    pub fn start(config: DelayedS3Config) -> Result<Self> {
        let downstream = reqwest::Url::parse(&config.downstream)
            .context("parse delayed S3 downstream endpoint")?;
        ensure!(
            downstream.scheme() == "http",
            "delayed S3 downstream must use HTTP"
        );
        ensure!(
            matches!(downstream.host_str(), Some("127.0.0.1" | "localhost")),
            "delayed S3 downstream must be loopback"
        );
        ensure!(
            downstream.path() == "/" && downstream.query().is_none(),
            "delayed S3 downstream must have no path or query"
        );
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .context("bind delayed S3 proxy listener")?;
        listener
            .set_nonblocking(true)
            .context("set delayed S3 proxy listener nonblocking")?;
        let listener_addr = listener.local_addr()?;
        let endpoint = format!("http://{listener_addr}");
        let upstream_connect_attempts = Arc::new(AtomicU64::new(0));
        let upstream_connections_established = Arc::new(AtomicU64::new(0));
        let state = Arc::new(ProxyState {
            started_at: Instant::now(),
            downstream,
            delay: config.delay,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(30))
                .connector_layer(UpstreamConnectCounterLayer {
                    attempts: Arc::clone(&upstream_connect_attempts),
                    established: Arc::clone(&upstream_connections_established),
                })
                .build()
                .context("create delayed S3 proxy client")?,
            gets: AtomicU64::new(0),
            heads: AtomicU64::new(0),
            upstream_errors: AtomicU64::new(0),
            next_request_id: AtomicU64::new(1),
            next_connection_id: AtomicU64::new(1),
            upstream_connect_attempts,
            upstream_connections_established,
            upstream_http1_responses: AtomicU64::new(0),
            upstream_http2_responses: AtomicU64::new(0),
            upstream_other_protocol_responses: AtomicU64::new(0),
            connection_events: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
            event_overflow: AtomicU64::new(0),
            inflight_reads: AtomicU64::new(0),
            peak_inflight_reads: AtomicU64::new(0),
            peak_buffered_response_bytes: AtomicU64::new(0),
            upstream_bytes_read: AtomicU64::new(0),
            completed_response_bytes: AtomicU64::new(0),
            object_labels: Mutex::new(BTreeMap::new()),
            read_classes: Mutex::new(BTreeMap::new()),
            holds: Mutex::new(Vec::new()),
            body_tasks: Mutex::new(Vec::new()),
            next_hold: Mutex::new(None),
        });
        PROXIES_BY_LISTENER
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .expect("delayed S3 listener registry lock")
            .insert(listener_addr, Arc::downgrade(&state));
        let (shutdown, receiver) = oneshot::channel();
        let server_state = Arc::clone(&state);
        let server_thread = thread::Builder::new()
            .name("delayed-s3-proxy".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("create delayed S3 proxy runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("install delayed S3 proxy listener");
                    let state_for_join = Arc::clone(&server_state);
                    let router = Router::new()
                        .fallback(any(forward_read))
                        .with_state(server_state);
                    axum::serve(
                        listener,
                        router.into_make_service_with_connect_info::<ObservedConnection>(),
                    )
                    .with_graceful_shutdown(async {
                        let _ = receiver.await;
                    })
                    .await
                    .expect("serve delayed S3 proxy");
                    // Graceful shutdown joins accepted HTTP connections. A body
                    // pump is separately owned and must also be joined.
                    let tasks = {
                        let mut tasks = state_for_join
                            .body_tasks
                            .lock()
                            .expect("S3 body tasks lock");
                        std::mem::take(&mut *tasks)
                    };
                    for task in tasks {
                        let _ = task.await;
                    }
                });
            })
            .context("spawn delayed S3 proxy")?;
        Ok(Self {
            endpoint,
            listener_addr,
            state,
            shutdown: Some(shutdown),
            server_thread: Some(server_thread),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The same monotonic clock used for raw request and connection events.
    pub fn elapsed_millis(&self) -> u128 {
        self.state.started_at.elapsed().as_millis()
    }

    pub fn snapshot(&self) -> DelayedS3Snapshot {
        DelayedS3Snapshot {
            gets: self.state.gets.load(Ordering::Acquire),
            heads: self.state.heads.load(Ordering::Acquire),
            upstream_errors: self.state.upstream_errors.load(Ordering::Acquire),
            observed_be_connections: self.state.next_connection_id.load(Ordering::Acquire) - 1,
            upstream_connect_attempts: self.state.upstream_connect_attempts.load(Ordering::Acquire),
            upstream_connections_established: self
                .state
                .upstream_connections_established
                .load(Ordering::Acquire),
            upstream_http1_responses: self.state.upstream_http1_responses.load(Ordering::Acquire),
            upstream_http2_responses: self.state.upstream_http2_responses.load(Ordering::Acquire),
            upstream_other_protocol_responses: self
                .state
                .upstream_other_protocol_responses
                .load(Ordering::Acquire),
            event_overflow: self.state.event_overflow.load(Ordering::Acquire),
            peak_inflight_reads: self.state.peak_inflight_reads.load(Ordering::Acquire),
            peak_buffered_response_bytes: self
                .state
                .peak_buffered_response_bytes
                .load(Ordering::Acquire),
            upstream_bytes_read: self.state.upstream_bytes_read.load(Ordering::Acquire),
            completed_response_bytes: self.state.completed_response_bytes.load(Ordering::Acquire),
        }
    }

    /// A snapshot of the bounded raw buffer. A saturated log invalidates the
    /// experiment, even when the exact aggregate counters are still intact.
    pub fn event_log(&self) -> Vec<DelayedS3Event> {
        let events = self.state.events.lock().expect("S3 event log lock").clone();
        assert_eq!(
            self.snapshot().event_overflow,
            0,
            "delayed S3 raw event log overflowed"
        );
        events
    }

    pub fn connection_log(&self) -> Vec<DelayedS3ConnectionEvent> {
        let events = self
            .state
            .connection_events
            .lock()
            .expect("S3 connection event lock")
            .clone();
        assert_eq!(
            self.snapshot().event_overflow,
            0,
            "delayed S3 raw connection log overflowed"
        );
        events
    }

    /// Move one bounded batch to a caller that persists it before the next
    /// drain. The cumulative overflow flag is sticky: a truncated interval
    /// cannot be made valid by draining or resetting the raw buffer.
    pub fn take_event_log(&self) -> Result<Vec<DelayedS3Event>> {
        let mut events = self.state.events.lock().expect("S3 event log lock");
        ensure!(
            self.state.event_overflow.load(Ordering::Acquire) == 0,
            "delayed S3 raw event log overflowed; experiment evidence is incomplete"
        );
        Ok(std::mem::take(&mut *events))
    }

    pub fn take_connection_log(&self) -> Result<Vec<DelayedS3ConnectionEvent>> {
        let mut events = self
            .state
            .connection_events
            .lock()
            .expect("S3 connection event lock");
        ensure!(
            self.state.event_overflow.load(Ordering::Acquire) == 0,
            "delayed S3 raw connection log overflowed; experiment evidence is incomplete"
        );
        Ok(std::mem::take(&mut *events))
    }

    /// Register a non-secret object label for all future request events.
    pub fn label_object(&self, object_path: &str, object_id: &str) -> Result<()> {
        ensure!(
            object_path.starts_with('/') && !object_path.contains('?'),
            "S3 object path must be absolute and query-free"
        );
        ensure!(
            !object_id.is_empty()
                && object_id.len() <= 64
                && object_id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_')),
            "S3 object id must be a short opaque label"
        );
        let mut labels = self
            .state
            .object_labels
            .lock()
            .expect("S3 object labels lock");
        if let Some(existing) = labels.get(object_path) {
            ensure!(
                existing == object_id,
                "S3 object label conflicts with existing label"
            );
        } else {
            labels.insert(object_path.to_string(), object_id.to_string());
        }
        Ok(())
    }

    /// Classify one exact physical request shape for footer/index/data audit.
    pub fn label_read_class(
        &self,
        key: &DelayedS3ReadMatch,
        class: DelayedS3ReadClass,
    ) -> Result<()> {
        self.label_object(&key.object_path, &key.object_id)?;
        let mut classes = self
            .state
            .read_classes
            .lock()
            .expect("S3 read classes lock");
        let identity = (
            key.method.as_str().to_string(),
            key.object_path.clone(),
            key.range.clone(),
        );
        if let Some(existing) = classes.get(&identity) {
            ensure!(
                *existing == class,
                "S3 read class conflicts with existing class"
            );
        } else {
            classes.insert(identity, class);
        }
        Ok(())
    }

    /// Arm a hold for exactly the next real GET or HEAD. A second hold cannot
    /// silently replace the first, and the caller must observe entry before
    /// using this as evidence of in-flight work.
    pub fn hold_next_read(&self) -> Result<DelayedS3ReadHold> {
        self.arm_hold(HoldMatcher::Any, DelayedS3HoldMode::BeforeForward)
    }

    /// Hold the next matching object read without consuming the hold on
    /// metadata or data objects from another part of the same query.
    pub fn hold_next_read_with_suffix(&self, suffix: &str) -> Result<DelayedS3ReadHold> {
        ensure!(
            suffix.starts_with('.') && suffix.len() > 1,
            "S3 hold suffix must name an object extension"
        );
        self.arm_hold(
            HoldMatcher::Suffix(suffix.to_owned()),
            DelayedS3HoldMode::BeforeForward,
        )
    }

    /// Match an exact object path tail, including extensionless metadata such
    /// as a snapshot's LATEST pointer. The proxy does not record object names.
    pub fn hold_next_read_with_path_suffix(&self, suffix: &str) -> Result<DelayedS3ReadHold> {
        ensure!(
            suffix.starts_with('/') && suffix.len() > 1 && !suffix.contains('?'),
            "S3 hold path suffix must name an object path tail"
        );
        self.arm_hold(
            HoldMatcher::Suffix(suffix.to_owned()),
            DelayedS3HoldMode::BeforeForward,
        )
    }

    /// Arm one exact method, object, and Range match. The opaque object id is
    /// the only object identifier retained in events.
    pub fn hold_next_read_matching(
        &self,
        key: DelayedS3ReadMatch,
        mode: DelayedS3HoldMode,
    ) -> Result<DelayedS3ReadHold> {
        ensure!(
            matches!(key.method, Method::GET | Method::HEAD),
            "S3 hold method must be GET or HEAD"
        );
        self.label_object(&key.object_path, &key.object_id)?;
        if let Some(range) = &key.range {
            ensure!(
                range.starts_with("bytes=")
                    && range.len() > 6
                    && range.len() <= 128
                    && range[6..]
                        .chars()
                        .all(|c| c.is_ascii_digit() || matches!(c, '-' | ',')),
                "S3 hold Range must be an explicit byte range"
            );
        }
        if let DelayedS3HoldMode::AfterBodyPrefix { bytes } = mode {
            ensure!(
                key.method == Method::GET && bytes > 0,
                "body-prefix hold requires GET and positive prefix bytes"
            );
        }
        self.arm_hold(HoldMatcher::Exact(key), mode)
    }

    fn arm_hold(&self, matcher: HoldMatcher, mode: DelayedS3HoldMode) -> Result<DelayedS3ReadHold> {
        let mut next = self.state.next_hold.lock().expect("S3 next hold lock");
        ensure!(next.is_none(), "S3 read hold is already armed");
        let (release, _) = watch::channel(false);
        let state = Arc::new(ReadHoldState {
            matcher,
            mode,
            entered: Mutex::new(false),
            entered_changed: Condvar::new(),
            forwarded: Mutex::new(false),
            forwarded_changed: Condvar::new(),
            prefix_queued: Mutex::new(false),
            prefix_changed: Condvar::new(),
            body_closed: Mutex::new(false),
            body_closed_changed: Condvar::new(),
            release,
        });
        let mut holds = self.state.holds.lock().expect("S3 holds lock");
        holds.retain(|hold| hold.strong_count() > 0);
        holds.push(Arc::downgrade(&state));
        *next = Some(Arc::clone(&state));
        Ok(DelayedS3ReadHold { state })
    }
}

impl Drop for DelayedS3Proxy {
    fn drop(&mut self) {
        for hold in self
            .state
            .holds
            .lock()
            .expect("S3 holds lock")
            .iter()
            .filter_map(std::sync::Weak::upgrade)
        {
            hold.release.send_replace(true);
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.server_thread.take() {
            let _ = thread.join();
        }
        PROXIES_BY_LISTENER
            .get()
            .expect("delayed S3 listener registry")
            .lock()
            .expect("delayed S3 listener registry lock")
            .remove(&self.listener_addr);
    }
}

impl ProxyState {
    fn record(&self, mut event: DelayedS3Event) {
        let mut events = self.events.lock().expect("S3 event log lock");
        event.elapsed_millis = self.started_at.elapsed().as_millis();
        if events.len() < MAX_EVENT_ENTRIES {
            events.push(event);
        } else {
            self.event_overflow.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn record_connection(&self, kind: DelayedS3ConnectionEventKind, connection_id: u64) {
        let mut events = self
            .connection_events
            .lock()
            .expect("S3 connection event lock");
        if events.len() < MAX_EVENT_ENTRIES {
            events.push(DelayedS3ConnectionEvent {
                kind,
                connection_id,
                elapsed_millis: self.started_at.elapsed().as_millis(),
            });
        } else {
            self.event_overflow.fetch_add(1, Ordering::AcqRel);
        }
    }
}

fn protocol(version: Version) -> String {
    match version {
        Version::HTTP_10 => "http/1.0",
        Version::HTTP_11 => "http/1.1",
        Version::HTTP_2 => "h2",
        Version::HTTP_3 => "h3",
        _ => "unknown",
    }
    .to_string()
}

fn observed_range(request: &Request) -> Option<String> {
    request
        .headers()
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .filter(|range| {
            range.starts_with("bytes=")
                && range.len() <= 128
                && range[6..]
                    .chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, '-' | ','))
        })
        .map(str::to_owned)
}

async fn forward_read(
    State(state): State<Arc<ProxyState>>,
    ConnectInfo(connection): ConnectInfo<ObservedConnection>,
    request: Request,
) -> Response {
    let method = request.method().clone();
    if method == Method::GET {
        state.gets.fetch_add(1, Ordering::AcqRel);
    } else if method == Method::HEAD {
        state.heads.fetch_add(1, Ordering::AcqRel);
    } else {
        return error_response(StatusCode::METHOD_NOT_ALLOWED);
    }

    let hold = {
        let mut next = state.next_hold.lock().expect("S3 next hold lock");
        if next
            .as_ref()
            .is_some_and(|hold| hold.matcher.matches(&request))
        {
            next.take()
        } else {
            None
        }
    };
    let request_id = state.next_request_id.fetch_add(1, Ordering::AcqRel);
    let range = observed_range(&request);
    let mut event = DelayedS3Event {
        kind: DelayedS3EventKind::Arrived,
        request_id,
        connection_id: connection.lifetime.id,
        elapsed_millis: 0,
        protocol: protocol(request.version()),
        method: method.clone(),
        object_id: state
            .object_labels
            .lock()
            .expect("S3 object labels lock")
            .get(request.uri().path())
            .cloned(),
        read_class: state
            .read_classes
            .lock()
            .expect("S3 read classes lock")
            .get(&(
                method.as_str().to_string(),
                request.uri().path().to_string(),
                range.clone(),
            ))
            .copied(),
        range,
        bytes: 0,
    };
    let _inflight = InflightRead::start(&state);
    state.record(event.clone());

    if let Some(hold) = &hold {
        let mut released = hold.release.subscribe();
        *hold.entered.lock().expect("S3 hold entry lock") = true;
        hold.entered_changed.notify_all();
        if hold.mode == DelayedS3HoldMode::BeforeForward {
            while !*released.borrow() {
                if released.changed().await.is_err() {
                    return error_response(StatusCode::BAD_GATEWAY);
                }
            }
        }
    }

    if !state.delay.is_zero() {
        tokio::time::sleep(state.delay).await;
    }
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let url = format!(
        "{}{}",
        state.downstream.as_str().trim_end_matches('/'),
        path_and_query
    );
    let mut headers = request.headers().clone();
    headers.remove(header::CONNECTION);
    headers.remove(header::TRANSFER_ENCODING);
    event.kind = DelayedS3EventKind::UpstreamStarted;
    state.record(event.clone());
    let response = match state
        .client
        .request(method, url)
        .headers(headers)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => {
            state.upstream_errors.fetch_add(1, Ordering::AcqRel);
            event.kind = DelayedS3EventKind::UpstreamError;
            state.record(event);
            return error_response(StatusCode::BAD_GATEWAY);
        }
    };
    match response.version() {
        Version::HTTP_10 | Version::HTTP_11 => {
            state
                .upstream_http1_responses
                .fetch_add(1, Ordering::AcqRel);
        }
        Version::HTTP_2 => {
            state
                .upstream_http2_responses
                .fetch_add(1, Ordering::AcqRel);
        }
        _ => {
            state
                .upstream_other_protocol_responses
                .fetch_add(1, Ordering::AcqRel);
        }
    }
    let mut builder = Response::builder().status(response.status());
    for (name, value) in response.headers() {
        if name != header::CONNECTION && name != header::TRANSFER_ENCODING {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    event.kind = DelayedS3EventKind::UpstreamHeaders;
    state.record(event.clone());

    if let Some(hold) = hold
        .clone()
        .filter(|hold| matches!(hold.mode, DelayedS3HoldMode::AfterBodyPrefix { .. }))
    {
        let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
        let pump_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            pump_body(pump_state, response, tx, hold, event, _inflight).await;
        });
        let mut tasks = state.body_tasks.lock().expect("S3 body tasks lock");
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
        return builder
            .body(Body::from_stream(ReceiverStream::new(rx)))
            .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY));
    }

    let mut response = response;
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                state
                    .upstream_bytes_read
                    .fetch_add(chunk.len() as u64, Ordering::AcqRel);
                if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                    state.upstream_errors.fetch_add(1, Ordering::AcqRel);
                    event.kind = DelayedS3EventKind::UpstreamError;
                    state.record(event);
                    return error_response(StatusCode::BAD_GATEWAY);
                }
                bytes.extend_from_slice(&chunk);
                state
                    .peak_buffered_response_bytes
                    .fetch_max(bytes.len() as u64, Ordering::AcqRel);
            }
            Ok(None) => break,
            Err(_) => {
                state.upstream_errors.fetch_add(1, Ordering::AcqRel);
                event.kind = DelayedS3EventKind::UpstreamError;
                state.record(event);
                return error_response(StatusCode::BAD_GATEWAY);
            }
        }
    }
    if let Some(hold) = &hold {
        *hold.forwarded.lock().expect("S3 hold forward lock") = true;
        hold.forwarded_changed.notify_all();
    }
    event.kind = DelayedS3EventKind::BodyComplete;
    event.bytes = bytes.len() as u64;
    state
        .completed_response_bytes
        .fetch_add(event.bytes, Ordering::AcqRel);
    state.record(event);
    builder
        .body(Body::from(bytes))
        .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY))
}

async fn pump_body(
    state: Arc<ProxyState>,
    mut response: reqwest::Response,
    tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
    hold: Arc<ReadHoldState>,
    mut event: DelayedS3Event,
    _inflight: InflightRead,
) {
    let DelayedS3HoldMode::AfterBodyPrefix { bytes: prefix_len } = hold.mode else {
        unreachable!("only body-prefix holds create a body pump")
    };
    let mut prefix = Vec::with_capacity(prefix_len);
    let mut remainder = Bytes::new();
    let mut total = 0_u64;
    while prefix.len() < prefix_len {
        let chunk = tokio::select! {
            _ = tx.closed() => {
                mark_body_closed(&state, &hold, event);
                return;
            }
            chunk = response.chunk() => chunk,
        };
        match chunk {
            Ok(Some(mut chunk)) => {
                state
                    .upstream_bytes_read
                    .fetch_add(chunk.len() as u64, Ordering::AcqRel);
                let take = (prefix_len - prefix.len()).min(chunk.len());
                prefix.extend_from_slice(&chunk.split_to(take));
                remainder = chunk;
            }
            Ok(None) | Err(_) => {
                state.upstream_errors.fetch_add(1, Ordering::AcqRel);
                event.kind = DelayedS3EventKind::UpstreamError;
                state.record(event);
                return;
            }
        }
        if !remainder.is_empty() {
            break;
        }
    }
    total += prefix.len() as u64;
    state
        .peak_buffered_response_bytes
        .fetch_max((prefix.len() + remainder.len()) as u64, Ordering::AcqRel);
    if tx.send(Ok(Bytes::from(prefix))).await.is_err() {
        mark_body_closed(&state, &hold, event);
        return;
    }
    event.kind = DelayedS3EventKind::BodyPrefixQueued;
    event.bytes = total;
    state.record(event.clone());
    *hold.prefix_queued.lock().expect("S3 hold prefix lock") = true;
    hold.prefix_changed.notify_all();

    let mut released = hold.release.subscribe();
    while !*released.borrow() {
        tokio::select! {
            _ = tx.closed() => { mark_body_closed(&state, &hold, event); return; }
            changed = released.changed() => {
                if changed.is_err() { mark_body_closed(&state, &hold, event); return; }
            }
        }
    }
    if !remainder.is_empty() {
        state
            .peak_buffered_response_bytes
            .fetch_max(remainder.len() as u64, Ordering::AcqRel);
        total += remainder.len() as u64;
        if tx.send(Ok(remainder)).await.is_err() {
            mark_body_closed(&state, &hold, event);
            return;
        }
    }
    loop {
        let chunk = tokio::select! {
            _ = tx.closed() => { mark_body_closed(&state, &hold, event); return; }
            chunk = response.chunk() => chunk,
        };
        match chunk {
            Ok(Some(chunk)) => {
                state
                    .upstream_bytes_read
                    .fetch_add(chunk.len() as u64, Ordering::AcqRel);
                state
                    .peak_buffered_response_bytes
                    .fetch_max(chunk.len() as u64, Ordering::AcqRel);
                total += chunk.len() as u64;
                if tx.send(Ok(chunk)).await.is_err() {
                    mark_body_closed(&state, &hold, event);
                    return;
                }
            }
            Ok(None) => break,
            Err(_) => {
                state.upstream_errors.fetch_add(1, Ordering::AcqRel);
                event.kind = DelayedS3EventKind::UpstreamError;
                event.bytes = total;
                state.record(event);
                return;
            }
        }
    }
    *hold.forwarded.lock().expect("S3 hold forward lock") = true;
    hold.forwarded_changed.notify_all();
    event.kind = DelayedS3EventKind::BodyComplete;
    event.bytes = total;
    state
        .completed_response_bytes
        .fetch_add(total, Ordering::AcqRel);
    state.record(event);
}

fn mark_body_closed(state: &ProxyState, hold: &ReadHoldState, mut event: DelayedS3Event) {
    *hold.body_closed.lock().expect("S3 hold body close lock") = true;
    hold.body_closed_changed.notify_all();
    event.kind = DelayedS3EventKind::BodyClosed;
    state.record(event);
}

fn error_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("fixed delayed S3 error response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loopback_s3::{LoopbackS3Config, LoopbackS3Fixture, LoopbackS3Object};
    use std::time::Instant;

    const SIGNED: &str = "AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=must-not-log";

    fn fixture_with_object(bytes: &[u8]) -> (LoopbackS3Fixture, DelayedS3Proxy) {
        let upstream = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key("test-key"))
            .expect("start upstream S3 fixture");
        upstream
            .replace_object_for_test(LoopbackS3Object {
                bucket: "bucket".to_string(),
                key: "data/file.parquet".to_string(),
                bytes: bytes.to_vec(),
            })
            .expect("install object");
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: upstream.endpoint().to_string(),
            delay: Duration::ZERO,
        })
        .expect("start delayed proxy");
        (upstream, proxy)
    }

    #[test]
    fn repeated_event_drains_preserve_a_full_pilot_window_without_growth() {
        let (_upstream, proxy) = fixture_with_object(b"x");
        let event = DelayedS3Event {
            kind: DelayedS3EventKind::Arrived,
            request_id: 0,
            connection_id: 1,
            elapsed_millis: 0,
            protocol: "http/1.1".to_string(),
            method: Method::GET,
            object_id: Some("data-1".to_string()),
            read_class: Some(DelayedS3ReadClass::Data),
            range: Some("bytes=0-1".to_string()),
            bytes: 0,
        };
        let mut drained = 0;
        // One 120 s window can have at least 240 files per each of 1,000
        // queries, with several event phases for every request.
        for index in 0..(240_000 * 3) {
            let mut event = event.clone();
            event.request_id = index + 1;
            proxy.state.record(event);
            if index % 4_096 == 4_095 {
                let batch = proxy.take_event_log().expect("complete bounded batch");
                assert_eq!(batch.len(), 4_096);
                drained += batch.len();
            }
        }
        drained += proxy.take_event_log().expect("final batch").len();
        assert_eq!(drained, 720_000);
        assert!(proxy.event_log().is_empty());
        assert_eq!(proxy.snapshot().event_overflow, 0);
    }

    #[test]
    fn raw_event_overflow_is_sticky_and_fails_closed() {
        let (_upstream, proxy) = fixture_with_object(b"x");
        let event = DelayedS3Event {
            kind: DelayedS3EventKind::Arrived,
            request_id: 1,
            connection_id: 1,
            elapsed_millis: 0,
            protocol: "http/1.1".to_string(),
            method: Method::GET,
            object_id: None,
            read_class: None,
            range: None,
            bytes: 0,
        };
        for _ in 0..=MAX_EVENT_ENTRIES {
            proxy.state.record(event.clone());
        }
        assert_eq!(proxy.snapshot().event_overflow, 1);
        assert!(proxy.take_event_log().is_err());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| proxy.event_log())).is_err()
        );
        assert_eq!(
            proxy.state.events.lock().expect("event log").len(),
            MAX_EVENT_ENTRIES
        );
    }

    fn exact_range(range: &str) -> DelayedS3ReadMatch {
        DelayedS3ReadMatch {
            method: Method::GET,
            object_path: "/bucket/data/file.parquet".to_string(),
            object_id: "data-1".to_string(),
            range: Some(range.to_string()),
        }
    }

    #[test]
    fn exact_hold_skips_other_method_range_and_object() {
        let (_upstream, proxy) = fixture_with_object(b"abcdefghij");
        proxy
            .label_read_class(&exact_range("bytes=4-6"), DelayedS3ReadClass::Data)
            .expect("label data range");
        let hold = proxy
            .hold_next_read_matching(exact_range("bytes=4-6"), DelayedS3HoldMode::BeforeForward)
            .expect("arm exact hold");
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        assert_eq!(
            client
                .head(&url)
                .header(header::AUTHORIZATION, SIGNED)
                .send()
                .expect("HEAD")
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .get(&url)
                .header(header::AUTHORIZATION, SIGNED)
                .header(header::RANGE, "bytes=4-5")
                .send()
                .expect("other range")
                .bytes()
                .expect("bytes"),
            "ef"
        );
        assert_eq!(
            client
                .get(format!("{}/bucket/other", proxy.endpoint()))
                .header(header::AUTHORIZATION, SIGNED)
                .header(header::RANGE, "bytes=4-6")
                .send()
                .expect("other object")
                .status(),
            StatusCode::NOT_FOUND
        );
        let worker = thread::spawn(move || {
            client
                .get(url)
                .header(header::AUTHORIZATION, SIGNED)
                .header(header::RANGE, "bytes=4-6")
                .send()
                .expect("held GET")
                .bytes()
                .expect("held bytes")
        });
        hold.wait_until_entered(Duration::from_secs(2))
            .expect("exact request entered");
        assert!(!worker.is_finished());
        hold.release();
        assert_eq!(worker.join().expect("worker"), "efg");
        let events = proxy.event_log();
        let held: Vec<_> = events
            .iter()
            .filter(|event| event.object_id.as_deref() == Some("data-1"))
            .collect();
        assert!(
            held.iter()
                .any(|event| event.kind == DelayedS3EventKind::Arrived
                    && event.range.as_deref() == Some("bytes=4-6"))
        );
        assert!(
            held.iter()
                .any(|event| event.kind == DelayedS3EventKind::BodyComplete && event.bytes == 3)
        );
        assert_eq!(proxy.snapshot().event_overflow, 0);
    }

    #[test]
    fn connection_ids_count_new_connections_not_gets() {
        let (_upstream, proxy) = fixture_with_object(b"abcdefghij");
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        for _ in 0..2 {
            assert_eq!(
                client
                    .get(&url)
                    .header(header::AUTHORIZATION, SIGNED)
                    .send()
                    .expect("GET")
                    .bytes()
                    .expect("body"),
                "abcdefghij"
            );
        }
        let events = proxy.event_log();
        let arrivals: Vec<_> = events
            .iter()
            .filter(|event| event.kind == DelayedS3EventKind::Arrived)
            .collect();
        assert_eq!(arrivals.len(), 2);
        assert_eq!(arrivals[0].connection_id, arrivals[1].connection_id);
        assert_eq!(arrivals[0].protocol, "http/1.1");
        assert_eq!(proxy.snapshot().gets, 2);
        assert_eq!(proxy.snapshot().observed_be_connections, 1);
        // The loopback fixture closes each response, so the proxy must open
        // one upstream connection per GET even though its client is reused.
        assert_eq!(proxy.snapshot().upstream_connect_attempts, 2);
        assert_eq!(proxy.snapshot().upstream_connections_established, 2);
        assert_eq!(proxy.snapshot().upstream_http1_responses, 2);
        assert_eq!(proxy.snapshot().upstream_http2_responses, 0);
        let accepted = proxy.connection_log()[0];
        assert_eq!(accepted.kind, DelayedS3ConnectionEventKind::Accepted);
        assert!(accepted.elapsed_millis <= arrivals[0].elapsed_millis);
        assert!(arrivals[0].elapsed_millis <= arrivals[1].elapsed_millis);
        assert!(arrivals[1].elapsed_millis <= proxy.elapsed_millis());
        for request_id in [arrivals[0].request_id, arrivals[1].request_id] {
            let phases: Vec<_> = events
                .iter()
                .filter(|event| event.request_id == request_id)
                .collect();
            assert_eq!(phases[0].kind, DelayedS3EventKind::Arrived);
            assert_eq!(phases[1].kind, DelayedS3EventKind::UpstreamStarted);
            assert_eq!(phases[2].kind, DelayedS3EventKind::UpstreamHeaders);
            assert_eq!(phases[3].kind, DelayedS3EventKind::BodyComplete);
            assert!(
                phases
                    .windows(2)
                    .all(|pair| { pair[0].elapsed_millis <= pair[1].elapsed_millis })
            );
        }
    }

    #[test]
    fn accepts_and_closes_idle_be_facing_connection_without_get() {
        let (_upstream, proxy) = fixture_with_object(b"x");
        let address = proxy.listener_addr;
        let socket = std::net::TcpStream::connect(address).expect("connect idle BE-facing socket");
        let deadline = Instant::now() + Duration::from_secs(2);
        while proxy.connection_log().is_empty() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        let accepted = proxy.connection_log()[0];
        assert_eq!(accepted.kind, DelayedS3ConnectionEventKind::Accepted);
        assert_eq!(accepted.connection_id, 1);
        assert!(accepted.elapsed_millis <= proxy.elapsed_millis());
        assert_eq!(proxy.snapshot().gets, 0);
        drop(socket);
        while proxy.connection_log().len() < 2 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        let closed = proxy.connection_log()[1];
        assert_eq!(closed.kind, DelayedS3ConnectionEventKind::Closed);
        assert_eq!(closed.connection_id, 1);
        assert!(accepted.elapsed_millis <= closed.elapsed_millis);
        assert!(closed.elapsed_millis <= proxy.elapsed_millis());
    }

    #[test]
    fn streaming_prefix_hold_observes_client_close_before_release() {
        use std::io::Read;
        let (_upstream, proxy) = fixture_with_object(b"abcdefghij");
        let hold = proxy
            .hold_next_read_matching(
                exact_range("bytes=0-9"),
                DelayedS3HoldMode::AfterBodyPrefix { bytes: 3 },
            )
            .expect("arm streaming hold");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        let worker = thread::spawn(move || {
            let mut response = reqwest::blocking::Client::builder()
                .no_proxy()
                .build()
                .expect("client")
                .get(url)
                .header(header::AUTHORIZATION, SIGNED)
                .header(header::RANGE, "bytes=0-9")
                .send()
                .expect("GET");
            let mut prefix = [0_u8; 3];
            response.read_exact(&mut prefix).expect("read prefix");
            assert_eq!(&prefix, b"abc");
            drop(response);
        });
        hold.wait_until_prefix_queued(Duration::from_secs(2))
            .expect("prefix queued");
        worker.join().expect("worker");
        hold.wait_until_body_closed(Duration::from_secs(2))
            .expect("body close observed");
        assert!(
            hold.wait_until_forwarded(Duration::from_millis(20))
                .is_err()
        );
        let events = proxy.event_log();
        assert!(events.iter().any(|event| event.kind == DelayedS3EventKind::BodyPrefixQueued && event.bytes == 3));
        assert!(
            events
                .iter()
                .any(|event| event.kind == DelayedS3EventKind::BodyClosed)
        );
        assert!(
            !events
                .iter()
                .any(|event| event.kind == DelayedS3EventKind::BodyComplete)
        );
    }

    #[test]
    fn streaming_prefix_release_completes_exact_body() {
        use std::io::Read;
        let (_upstream, proxy) = fixture_with_object(b"abcdefghij");
        let hold = proxy
            .hold_next_read_matching(
                exact_range("bytes=0-9"),
                DelayedS3HoldMode::AfterBodyPrefix { bytes: 3 },
            )
            .expect("arm streaming hold");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        let worker = thread::spawn(move || {
            let mut response = reqwest::blocking::Client::builder()
                .no_proxy()
                .build()
                .expect("client")
                .get(url)
                .header(header::AUTHORIZATION, SIGNED)
                .header(header::RANGE, "bytes=0-9")
                .send()
                .expect("GET");
            let mut prefix = [0_u8; 3];
            response.read_exact(&mut prefix).expect("read prefix");
            let mut tail = Vec::new();
            response.read_to_end(&mut tail).expect("read tail");
            (prefix, tail)
        });
        hold.wait_until_prefix_queued(Duration::from_secs(2))
            .expect("prefix queued");
        assert!(!worker.is_finished());
        hold.release();
        let (prefix, tail) = worker.join().expect("worker");
        assert_eq!(&prefix, b"abc");
        assert_eq!(tail, b"defghij");
        hold.wait_until_forwarded(Duration::from_secs(2))
            .expect("body completed");
        assert!(
            proxy
                .event_log()
                .iter()
                .any(|event| event.kind == DelayedS3EventKind::BodyComplete && event.bytes == 10)
        );
    }

    #[test]
    fn short_upstream_body_reports_error_and_does_not_leave_hold() {
        let (_upstream, proxy) = fixture_with_object(b"abcdefghij");
        let hold = proxy
            .hold_next_read_matching(
                exact_range("bytes=0-9"),
                DelayedS3HoldMode::AfterBodyPrefix { bytes: 11 },
            )
            .expect("arm oversized prefix");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        let response = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("client")
            .get(url)
            .header(header::AUTHORIZATION, SIGNED)
            .header(header::RANGE, "bytes=0-9")
            .send()
            .expect("GET");
        assert!(response.bytes().is_err());
        assert!(
            hold.wait_until_prefix_queued(Duration::from_millis(20))
                .is_err()
        );
        assert_eq!(proxy.snapshot().upstream_errors, 1);
        assert!(
            proxy
                .event_log()
                .iter()
                .any(|event| event.kind == DelayedS3EventKind::UpstreamError)
        );
    }

    #[test]
    fn teardown_releases_held_request_and_joins_connection() {
        let (_upstream, proxy) = fixture_with_object(b"abcdefghij");
        let hold = proxy
            .hold_next_read_matching(exact_range("bytes=0-2"), DelayedS3HoldMode::BeforeForward)
            .expect("arm hold");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        let worker = thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .no_proxy()
                .build()
                .expect("client")
                .get(url)
                .header(header::AUTHORIZATION, SIGNED)
                .header(header::RANGE, "bytes=0-2")
                .send()
                .expect("GET")
                .bytes()
                .expect("body")
        });
        hold.wait_until_entered(Duration::from_secs(2))
            .expect("entered hold");
        drop(proxy);
        assert_eq!(worker.join().expect("worker"), "abc");
        hold.wait_until_forwarded(Duration::from_secs(2))
            .expect("forwarded after teardown");
    }

    #[test]
    fn teardown_releases_streaming_body_hold_and_joins_pump() {
        use std::io::Read;
        let (_upstream, proxy) = fixture_with_object(b"abcdefghij");
        let hold = proxy
            .hold_next_read_matching(
                exact_range("bytes=0-9"),
                DelayedS3HoldMode::AfterBodyPrefix { bytes: 3 },
            )
            .expect("arm streaming hold");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        let worker = thread::spawn(move || {
            let mut response = reqwest::blocking::Client::builder()
                .no_proxy()
                .build()
                .expect("client")
                .get(url)
                .header(header::AUTHORIZATION, SIGNED)
                .header(header::RANGE, "bytes=0-9")
                .send()
                .expect("GET");
            let mut body = Vec::new();
            response.read_to_end(&mut body).expect("body");
            body
        });
        hold.wait_until_prefix_queued(Duration::from_secs(2))
            .expect("prefix queued");
        drop(proxy);
        assert_eq!(worker.join().expect("worker"), b"abcdefghij");
        hold.wait_until_forwarded(Duration::from_secs(2))
            .expect("body forwarded after teardown");
    }

    #[test]
    fn forwards_signed_get_and_head_with_a_real_delay() {
        let upstream = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key("test-key"))
            .expect("start upstream S3 fixture");
        upstream
            .replace_object_for_test(LoopbackS3Object {
                bucket: "bucket".to_string(),
                key: "data/file".to_string(),
                bytes: b"abcdef".to_vec(),
            })
            .expect("install object");
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: upstream.endpoint().to_string(),
            delay: Duration::from_millis(25),
        })
        .expect("start delayed proxy");
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("create client");
        let signed = "AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=must-not-log";
        let url = format!("{}/bucket/data/file", proxy.endpoint());
        let start = Instant::now();
        let get = client
            .get(&url)
            .header(header::AUTHORIZATION, signed)
            .header(header::RANGE, "bytes=2-4")
            .send()
            .expect("send signed range GET");
        assert_eq!(get.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(get.bytes().expect("read range"), "cde");
        assert!(start.elapsed() >= Duration::from_millis(25));
        let head = client
            .head(&url)
            .header(header::AUTHORIZATION, signed)
            .send()
            .expect("send signed HEAD");
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(proxy.snapshot().gets, 1);
        assert_eq!(proxy.snapshot().heads, 1);
        assert_eq!(proxy.snapshot().upstream_errors, 0);
        assert_eq!(proxy.snapshot().upstream_bytes_read, 3);
        assert_eq!(proxy.snapshot().completed_response_bytes, 3);
        assert_eq!(upstream.request_count(), 2);
    }

    #[test]
    fn holds_a_real_signed_read_until_explicit_release() {
        let upstream = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key("test-key"))
            .expect("start upstream S3 fixture");
        upstream
            .replace_object_for_test(LoopbackS3Object {
                bucket: "bucket".to_string(),
                key: "held".to_string(),
                bytes: b"real bytes".to_vec(),
            })
            .expect("install object");
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: upstream.endpoint().to_string(),
            delay: Duration::ZERO,
        })
        .expect("start proxy");
        let hold = proxy.hold_next_read().expect("arm real read hold");
        let url = format!("{}/bucket/held", proxy.endpoint());
        let request = thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .no_proxy()
                .build()
                .expect("create client")
                .get(url)
                .header(
                    header::AUTHORIZATION,
                    "AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=must-not-log",
                )
                .send()
                .expect("send signed held GET")
                .bytes()
                .expect("read held bytes")
        });
        hold.wait_until_entered(Duration::from_secs(2))
            .expect("real S3 read reached the hold");
        assert!(!request.is_finished());
        assert_eq!(upstream.request_count(), 0);
        hold.release();
        assert_eq!(request.join().expect("request thread"), "real bytes");
        assert_eq!(upstream.request_count(), 1);
    }

    #[test]
    fn suffix_hold_skips_other_reads_and_releases_on_drop() {
        let upstream = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key("test-key"))
            .expect("start upstream S3 fixture");
        for (key, bytes) in [
            ("metadata/list.avro", b"manifest".as_slice()),
            ("data/file.parquet", b"data".as_slice()),
        ] {
            upstream
                .replace_object_for_test(LoopbackS3Object {
                    bucket: "bucket".to_string(),
                    key: key.to_string(),
                    bytes: bytes.to_vec(),
                })
                .expect("install object");
        }
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: upstream.endpoint().to_string(),
            delay: Duration::ZERO,
        })
        .expect("start proxy");
        let hold = proxy
            .hold_next_read_with_suffix(".parquet")
            .expect("arm data read hold");
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("create client");
        let signed = "AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=must-not-log";
        let manifest = client
            .get(format!("{}/bucket/metadata/list.avro", proxy.endpoint()))
            .header(header::AUTHORIZATION, signed)
            .send()
            .expect("read nonmatching metadata");
        assert_eq!(manifest.status(), StatusCode::OK);
        assert_eq!(manifest.bytes().expect("metadata bytes"), "manifest");
        let url = format!("{}/bucket/data/file.parquet", proxy.endpoint());
        let request = thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .no_proxy()
                .build()
                .expect("create client")
                .get(url)
                .header(header::AUTHORIZATION, signed)
                .send()
                .expect("send held data GET")
                .bytes()
                .expect("read held data bytes")
        });
        hold.wait_until_entered(Duration::from_secs(2))
            .expect("data read reached hold");
        assert_eq!(upstream.request_count(), 1);
        drop(hold);
        assert_eq!(request.join().expect("data request thread"), "data");
        assert_eq!(upstream.request_count(), 2);
    }

    #[test]
    fn path_suffix_hold_targets_extensionless_snapshot_metadata() {
        let upstream = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key("test-key"))
            .expect("start upstream S3 fixture");
        for (key, bytes) in [
            ("fixture.db/table/schema/schema-0", b"schema".as_slice()),
            ("fixture.db/table/snapshot/LATEST", b"16".as_slice()),
        ] {
            upstream
                .replace_object_for_test(LoopbackS3Object {
                    bucket: "bucket".to_string(),
                    key: key.to_string(),
                    bytes: bytes.to_vec(),
                })
                .expect("install object");
        }
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: upstream.endpoint().to_string(),
            delay: Duration::ZERO,
        })
        .expect("start proxy");
        let hold = proxy
            .hold_next_read_with_path_suffix("/fixture.db/table/snapshot/LATEST")
            .expect("arm exact snapshot metadata hold");
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .expect("create client");
        let signed = "AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=must-not-log";
        let schema = client
            .get(format!(
                "{}/bucket/fixture.db/table/schema/schema-0",
                proxy.endpoint()
            ))
            .header(header::AUTHORIZATION, signed)
            .send()
            .expect("read nonmatching schema");
        assert_eq!(schema.status(), StatusCode::OK);
        assert_eq!(schema.bytes().expect("schema bytes"), "schema");
        let url = format!(
            "{}/bucket/fixture.db/table/snapshot/LATEST",
            proxy.endpoint()
        );
        let request = thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .no_proxy()
                .build()
                .expect("create client")
                .get(url)
                .header(header::AUTHORIZATION, signed)
                .send()
                .expect("send held snapshot GET")
                .bytes()
                .expect("read held snapshot bytes")
        });
        hold.wait_until_entered(Duration::from_secs(2))
            .expect("snapshot metadata reached hold");
        assert_eq!(upstream.request_count(), 1);
        hold.release();
        hold.wait_until_forwarded(Duration::from_secs(2))
            .expect("held snapshot forwarding completed");
        assert_eq!(request.join().expect("snapshot request thread"), "16");
        assert_eq!(upstream.request_count(), 2);
    }
}
