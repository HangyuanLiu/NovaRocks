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
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use axum::routing::any;
use std::net::{Ipv4Addr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::{oneshot, watch};

const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

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
}

struct ProxyState {
    downstream: reqwest::Url,
    delay: Duration,
    client: reqwest::Client,
    gets: AtomicU64,
    heads: AtomicU64,
    upstream_errors: AtomicU64,
    next_hold: Mutex<Option<Arc<ReadHoldState>>>,
}

struct ReadHoldState {
    path_suffix: Option<String>,
    entered: Mutex<bool>,
    entered_changed: Condvar,
    forwarded: Mutex<bool>,
    forwarded_changed: Condvar,
    release: watch::Sender<bool>,
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
}

impl Drop for DelayedS3ReadHold {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct DelayedS3Proxy {
    endpoint: String,
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
        let endpoint = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(ProxyState {
            downstream,
            delay: config.delay,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(30))
                .build()
                .context("create delayed S3 proxy client")?,
            gets: AtomicU64::new(0),
            heads: AtomicU64::new(0),
            upstream_errors: AtomicU64::new(0),
            next_hold: Mutex::new(None),
        });
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
                    let router = Router::new()
                        .fallback(any(forward_read))
                        .with_state(server_state);
                    axum::serve(listener, router)
                        .with_graceful_shutdown(async {
                            let _ = receiver.await;
                        })
                        .await
                        .expect("serve delayed S3 proxy");
                });
            })
            .context("spawn delayed S3 proxy")?;
        Ok(Self {
            endpoint,
            state,
            shutdown: Some(shutdown),
            server_thread: Some(server_thread),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn snapshot(&self) -> DelayedS3Snapshot {
        DelayedS3Snapshot {
            gets: self.state.gets.load(Ordering::Acquire),
            heads: self.state.heads.load(Ordering::Acquire),
            upstream_errors: self.state.upstream_errors.load(Ordering::Acquire),
        }
    }

    /// Arm a hold for exactly the next real GET or HEAD. A second hold cannot
    /// silently replace the first, and the caller must observe entry before
    /// using this as evidence of in-flight work.
    pub fn hold_next_read(&self) -> Result<DelayedS3ReadHold> {
        self.hold_next_read_matching_suffix(None)
    }

    /// Hold the next matching object read without consuming the hold on
    /// metadata or data objects from another part of the same query.
    pub fn hold_next_read_with_suffix(&self, suffix: &str) -> Result<DelayedS3ReadHold> {
        ensure!(
            suffix.starts_with('.') && suffix.len() > 1,
            "S3 hold suffix must name an object extension"
        );
        self.hold_next_read_matching_suffix(Some(suffix.to_owned()))
    }

    /// Match an exact object path tail, including extensionless metadata such
    /// as a snapshot's LATEST pointer. The proxy does not record object names.
    pub fn hold_next_read_with_path_suffix(&self, suffix: &str) -> Result<DelayedS3ReadHold> {
        ensure!(
            suffix.starts_with('/') && suffix.len() > 1 && !suffix.contains('?'),
            "S3 hold path suffix must name an object path tail"
        );
        self.hold_next_read_matching_suffix(Some(suffix.to_owned()))
    }

    fn hold_next_read_matching_suffix(
        &self,
        path_suffix: Option<String>,
    ) -> Result<DelayedS3ReadHold> {
        let mut next = self.state.next_hold.lock().expect("S3 next hold lock");
        ensure!(next.is_none(), "S3 read hold is already armed");
        let (release, _) = watch::channel(false);
        let state = Arc::new(ReadHoldState {
            path_suffix,
            entered: Mutex::new(false),
            entered_changed: Condvar::new(),
            forwarded: Mutex::new(false),
            forwarded_changed: Condvar::new(),
            release,
        });
        *next = Some(Arc::clone(&state));
        Ok(DelayedS3ReadHold { state })
    }
}

impl Drop for DelayedS3Proxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.server_thread.take() {
            let _ = thread.join();
        }
    }
}

async fn forward_read(State(state): State<Arc<ProxyState>>, request: Request) -> Response {
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
        if next.as_ref().is_some_and(|hold| {
            hold.path_suffix
                .as_deref()
                .is_none_or(|suffix| request.uri().path().ends_with(suffix))
        }) {
            next.take()
        } else {
            None
        }
    };
    if let Some(hold) = &hold {
        let mut released = hold.release.subscribe();
        *hold.entered.lock().expect("S3 hold entry lock") = true;
        hold.entered_changed.notify_all();
        while !*released.borrow() {
            if released.changed().await.is_err() {
                return error_response(StatusCode::BAD_GATEWAY);
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
    // Keep the signed Host and range headers. The client signed the proxy
    // endpoint, so rewriting Host would invalidate its SigV4 request.
    let mut headers = request.headers().clone();
    headers.remove(header::CONNECTION);
    headers.remove(header::TRANSFER_ENCODING);
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
            return error_response(StatusCode::BAD_GATEWAY);
        }
    };
    let status = response.status();
    let mut builder = Response::builder().status(status);
    for (name, value) in response.headers() {
        if name != header::CONNECTION && name != header::TRANSFER_ENCODING {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    let mut response = response;
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                    state.upstream_errors.fetch_add(1, Ordering::AcqRel);
                    return error_response(StatusCode::BAD_GATEWAY);
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(_) => {
                state.upstream_errors.fetch_add(1, Ordering::AcqRel);
                return error_response(StatusCode::BAD_GATEWAY);
            }
        }
    }
    if let Some(hold) = &hold {
        *hold.forwarded.lock().expect("S3 hold forward lock") = true;
        hold.forwarded_changed.notify_all();
    }
    builder
        .body(Body::from(bytes))
        .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY))
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
