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

//! The REST HTTP client (L3), one send of one request with its dispatch
//! stage (5.5), and chunked reading of a success response body (L8).
//!
//! Observation only: this module records where a send reached and never
//! interprets a status code. Header values, bodies of non-success responses
//! and transport error texts never leave it.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::HeaderMap;
use reqwest::{Method, Url};
use tokio::time::Instant;

use super::evidence::{DispatchStage, NoStatusCause, NotWrittenCause};
use crate::catalog_runtime::{REST_CONNECT_TIMEOUT, REST_READ_TIMEOUT};

/// The HTTP client every REST request of one role goes through.
///
/// The frontend session, catalog reads, mutations and token exchange share
/// one role-local connection pool. Clones retain the same pool.
#[derive(Clone)]
pub(crate) struct RestHttpClient {
    inner: reqwest::Client,
}

impl fmt::Debug for RestHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestHttpClient")
            .finish_non_exhaustive()
    }
}

impl RestHttpClient {
    /// Builds the production client: today's timeouts (connect 5 s, read
    /// 30 s, no total deadline) plus the three L3 settings.
    pub(crate) fn new() -> Result<Self, String> {
        Self::with_timeouts(REST_CONNECT_TIMEOUT, REST_READ_TIMEOUT)
    }

    /// Builds a client with explicit timeouts and otherwise the production
    /// settings. Production always uses [`Self::new`].
    ///
    /// reqwest counts the read timeout from the start of the request,
    /// connection setup included, while the connect timeout covers DNS, TCP,
    /// TLS and proxy tunnel. Only a connect timeout shorter than the read
    /// timeout guarantees that a stalled connection setup is reported as a
    /// connect error (not written) rather than a read timeout.
    pub(crate) fn with_timeouts(
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Self, String> {
        if connect_timeout >= read_timeout {
            return Err("REST connect timeout must be shorter than the read timeout".to_string());
        }
        reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            // Routing is decided by the session binding only; a redirect
            // would let the server reroute outside it and could resend a
            // client secret to a new address (L4).
            .redirect(reqwest::redirect::Policy::none())
            // Every request is written at most once. Hyper's own retry of a
            // pooled connection that died before a single byte was written
            // stays (L6).
            .retry(reqwest::retry::never())
            // The not-written judgement relies on verified HTTP/1.1 behavior.
            .http1_only()
            .build()
            .map(|inner| Self { inner })
            .map_err(|_| "build REST Iceberg HTTP client failed".to_string())
    }

    /// The underlying pool for independent dev-only SDK comparison.
    #[cfg(test)]
    pub(crate) fn reqwest_client(&self) -> &reqwest::Client {
        &self.inner
    }

    /// Sends `request` exactly once and reports where the send reached.
    ///
    /// `request` is the caller's prepared request, or the cause it could not
    /// be prepared (binding, URL, serialization). A response body is handed
    /// out only for a status in `success_statuses`; any other body is dropped
    /// unread.
    pub(crate) async fn send_once(
        &self,
        request: Result<RestRequest, NotWrittenCause>,
        success_statuses: &[u16],
        context: &RequestContext,
    ) -> (DispatchStage, Option<ResponseBody>) {
        let request = match request.and_then(|request| request.build(&self.inner)) {
            Ok(request) => request,
            Err(cause) => return (DispatchStage::NotWritten(cause), None),
        };
        let executed = match context.deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline, self.inner.execute(request)).await {
                    Ok(result) => result,
                    Err(_) => {
                        return (
                            DispatchStage::WrittenNoStatus(NoStatusCause::ReadTimeout),
                            None,
                        );
                    }
                }
            }
            None => self.inner.execute(request).await,
        };
        match executed {
            Ok(response) => {
                let code = response.status().as_u16();
                let header_names = response.headers().keys().cloned().collect();
                let body = success_statuses.contains(&code).then(|| ResponseBody {
                    response,
                    deadline: context.deadline,
                });
                (DispatchStage::Status { code, header_names }, body)
            }
            Err(error) => (classify_send_error(&error), None),
        }
    }
}

/// Maps a transport error of one send to its dispatch stage (L7).
///
/// Only connector errors (DNS, TCP, TLS, proxy tunnel, connect timeout) prove
/// that nothing was written; every other error happened after the request may
/// have reached the wire.
fn classify_send_error(error: &reqwest::Error) -> DispatchStage {
    if error.is_connect() {
        return DispatchStage::NotWritten(NotWrittenCause::Connect);
    }
    if error.is_builder() {
        return DispatchStage::NotWritten(NotWrittenCause::Build);
    }
    if error.is_timeout() {
        return DispatchStage::WrittenNoStatus(NoStatusCause::ReadTimeout);
    }
    if closed_by_peer(error) {
        return DispatchStage::WrittenNoStatus(NoStatusCause::Closed);
    }
    DispatchStage::WrittenNoStatus(NoStatusCause::Other)
}

/// Whether the error chain carries an I/O closure of the connection (reset,
/// abort, broken pipe, unexpected EOF). A clean close before the status line
/// surfaces as hyper's incomplete-message error, which carries no I/O source
/// and is not nameable without a direct hyper dependency; it is reported as
/// `Other`. The cause is diagnostic only: both are written without status.
fn closed_by_peer(error: &reqwest::Error) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(error) = source {
        if let Some(io) = error.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            )
        {
            return true;
        }
        source = error.source();
    }
    false
}

/// One prepared REST request: method, absolute URL, complete headers and an
/// optional buffered body.
pub(crate) struct RestRequest {
    method: Method,
    url: String,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
}

impl fmt::Debug for RestRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestRequest")
            .field("method", &self.method)
            .field("header_names", &self.headers.keys().collect::<Vec<_>>())
            .field("body_bytes", &self.body.as_ref().map(Vec::len))
            .finish_non_exhaustive()
    }
}

impl RestRequest {
    pub(crate) fn new(method: Method, url: String, headers: HeaderMap) -> Self {
        Self {
            method,
            url,
            headers,
            body: None,
        }
    }

    /// Attaches a JSON body; serialization failure means not written.
    pub(crate) fn with_json_body<T: serde::Serialize + ?Sized>(
        mut self,
        value: &T,
    ) -> Result<Self, NotWrittenCause> {
        self.body = Some(serde_json::to_vec(value).map_err(|_| NotWrittenCause::Serialize)?);
        Ok(self)
    }

    /// Attaches an `application/x-www-form-urlencoded` body encoded exactly as
    /// reqwest's `RequestBuilder::form` encodes it.
    pub(crate) fn with_form_body<'a>(
        mut self,
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (name, value) in pairs {
            serializer.append_pair(name, value);
        }
        self.body = Some(serializer.finish().into_bytes());
        self
    }

    pub(crate) fn method(&self) -> &Method {
        &self.method
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub(crate) fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    fn build(self, client: &reqwest::Client) -> Result<reqwest::Request, NotWrittenCause> {
        let url = Url::parse(&self.url).map_err(|_| NotWrittenCause::Url)?;
        let mut builder = client.request(self.method, url).headers(self.headers);
        if let Some(body) = self.body {
            builder = builder.body(body);
        }
        builder.build().map_err(|_| NotWrittenCause::Build)
    }
}

/// The caller's bounds on one request. Cancellation is dropping the future.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RequestContext {
    deadline: Option<Instant>,
}

impl RequestContext {
    /// Configuration, OAuth and catalog reads have one finite phase budget,
    /// shared by response headers and every body chunk.
    pub(crate) fn for_read() -> Self {
        Self::with_deadline(Instant::now() + REST_READ_TIMEOUT)
    }

    pub(crate) fn with_deadline(deadline: Instant) -> Self {
        Self {
            deadline: Some(deadline),
        }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// Why reading a success response body stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyReadError {
    /// The caller's deadline passed.
    DeadlineExceeded,
    /// The connection failed or closed before the body completed.
    Transport,
}

/// The body of a success response, read in chunks.
pub(crate) struct ResponseBody {
    response: reqwest::Response,
    deadline: Option<Instant>,
}

impl fmt::Debug for ResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseBody")
            .field("status", &self.response.status().as_u16())
            .finish_non_exhaustive()
    }
}

impl ResponseBody {
    pub(crate) fn header(
        &self,
        name: reqwest::header::HeaderName,
    ) -> Option<&reqwest::header::HeaderValue> {
        self.response.headers().get(name)
    }

    /// Reads the next chunk; `None` once the body is complete.
    ///
    /// Each chunk is followed by a Tokio cooperative-budget checkpoint and a
    /// deadline check, so a fast server cannot monopolize the worker.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>, BodyReadError> {
        self.check_deadline()?;
        let chunk = match self.deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, self.response.chunk())
                .await
                .map_err(|_| BodyReadError::DeadlineExceeded)?,
            None => self.response.chunk().await,
        }
        .map_err(|_| BodyReadError::Transport)?;
        tokio::task::consume_budget().await;
        self.check_deadline()?;
        Ok(chunk)
    }

    /// Reads the whole body for a complete-document read (L8): still chunked
    /// with checkpoints; the bytes live only until the caller decodes them.
    pub(crate) async fn read_to_end(mut self) -> Result<Vec<u8>, BodyReadError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = self.next_chunk().await? {
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    fn check_deadline(&self) -> Result<(), BodyReadError> {
        match self.deadline {
            Some(deadline) if Instant::now() >= deadline => Err(BodyReadError::DeadlineExceeded),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
fn test_client(read_timeout: Duration) -> RestHttpClient {
    RestHttpClient::with_timeouts(read_timeout / 2, read_timeout).unwrap()
}

#[cfg(test)]
fn get(url: String) -> Result<RestRequest, NotWrittenCause> {
    Ok(RestRequest::new(Method::GET, url, HeaderMap::new()))
}

#[cfg(test)]
mod not_written {
    use super::*;
    use crate::rest_protocol::evidence::BindingFailure;
    use crate::rest_protocol::test_server::{Script, TestServer};

    async fn stage_of(request: Result<RestRequest, NotWrittenCause>) -> DispatchStage {
        let (stage, body) = test_client(Duration::from_secs(10))
            .send_once(request, &[200], &RequestContext::default())
            .await;
        assert!(body.is_none());
        stage
    }

    #[tokio::test]
    async fn a_refused_connection_is_not_written() {
        let closed_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        assert_eq!(
            stage_of(get(format!("http://127.0.0.1:{closed_port}/v1/config"))).await,
            DispatchStage::NotWritten(NotWrittenCause::Connect)
        );
    }

    #[tokio::test]
    async fn a_dns_failure_is_not_written() {
        assert_eq!(
            stage_of(get(
                "http://novarocks-rest-protocol.invalid/v1/config".to_string()
            ))
            .await,
            DispatchStage::NotWritten(NotWrittenCause::Connect)
        );
    }

    #[tokio::test]
    async fn a_tls_handshake_failure_is_not_written() {
        let server = TestServer::start(Script::new());
        let url = format!("https://{}/v1/config", server.address());
        assert_eq!(
            stage_of(get(url)).await,
            DispatchStage::NotWritten(NotWrittenCause::Connect)
        );
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn a_stalled_tls_handshake_is_not_written() {
        // The kernel accepts the connection, but nothing ever answers the
        // ClientHello: the connect timeout fires before the read timeout.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("https://{}/v1/config", listener.local_addr().unwrap());
        let client =
            RestHttpClient::with_timeouts(Duration::from_millis(300), Duration::from_secs(5))
                .unwrap();
        let started = std::time::Instant::now();
        let (stage, body) = client
            .send_once(get(url), &[200], &RequestContext::default())
            .await;
        assert_eq!(stage, DispatchStage::NotWritten(NotWrittenCause::Connect));
        assert!(body.is_none());
        assert!(started.elapsed() < Duration::from_secs(3));
        drop(listener);
    }

    #[test]
    fn the_connect_timeout_must_be_shorter_than_the_read_timeout() {
        assert!(
            RestHttpClient::with_timeouts(Duration::from_secs(5), Duration::from_secs(5)).is_err()
        );
        assert!(RestHttpClient::new().is_ok());
    }

    #[tokio::test]
    async fn an_unparseable_url_is_not_written() {
        assert_eq!(
            stage_of(get("not a url".to_string())).await,
            DispatchStage::NotWritten(NotWrittenCause::Url)
        );
    }

    #[tokio::test]
    async fn a_preparation_failure_is_reported_as_given() {
        let cause = NotWrittenCause::Binding(BindingFailure::TokenRejected);
        assert_eq!(stage_of(Err(cause)).await, DispatchStage::NotWritten(cause));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest_protocol::test_server::{BodyPlan, Reply, Script, TestServer};

    #[tokio::test]
    async fn a_redirect_is_reported_and_never_followed() {
        let target = TestServer::start(Script::new().on("GET", "/moved", Reply::status(200)));
        let origin = TestServer::start(
            Script::new()
                .on(
                    "GET",
                    "/v1/config",
                    Reply::redirect(307, &format!("{}/moved", target.url())),
                )
                .on("GET", "/local", Reply::status(200)),
        );
        let (stage, body) = test_client(Duration::from_secs(2))
            .send_once(
                get(format!("{}/v1/config", origin.url())),
                &[200],
                &RequestContext::default(),
            )
            .await;
        assert!(
            matches!(stage, DispatchStage::Status { code: 307, ref header_names }
            if header_names.iter().any(|name| name == "location"))
        );
        assert!(body.is_none());
        assert!(target.requests().is_empty());
        assert_eq!(origin.requests().len(), 1);
    }

    #[tokio::test]
    async fn a_close_after_reading_the_request_is_written_without_status() {
        let server =
            TestServer::start(Script::new().on("POST", "/commit", Reply::CloseAfterRequest));
        let request = RestRequest::new(
            Method::POST,
            format!("{}/commit", server.url()),
            HeaderMap::new(),
        )
        .with_json_body(&serde_json::json!({"k": 1}));
        let (stage, body) = test_client(Duration::from_secs(2))
            .send_once(request, &[200], &RequestContext::default())
            .await;
        assert!(
            matches!(stage, DispatchStage::WrittenNoStatus(_)),
            "{stage:?}"
        );
        assert!(body.is_none());
        // The request reached the server exactly once.
        assert_eq!(server.count("POST", "/commit"), 1);
        assert_eq!(server.requests()[0].body, br#"{"k":1}"#);
    }

    #[tokio::test]
    async fn a_close_after_the_status_line_is_written_without_status() {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::CloseAfterStatusLine(200),
        ));
        let (stage, _) = test_client(Duration::from_secs(2))
            .send_once(
                get(format!("{}/v1/config", server.url())),
                &[200],
                &RequestContext::default(),
            )
            .await;
        assert!(
            matches!(stage, DispatchStage::WrittenNoStatus(_)),
            "{stage:?}"
        );
    }

    #[tokio::test]
    async fn a_read_timeout_is_written_without_status() {
        let server = TestServer::start(Script::new().on("GET", "/v1/config", Reply::NeverAnswer));
        let (stage, body) = test_client(Duration::from_millis(200))
            .send_once(
                get(format!("{}/v1/config", server.url())),
                &[200],
                &RequestContext::default(),
            )
            .await;
        assert_eq!(
            stage,
            DispatchStage::WrittenNoStatus(NoStatusCause::ReadTimeout)
        );
        assert!(body.is_none());
        assert_eq!(server.count("GET", "/v1/config"), 1);
    }

    #[tokio::test]
    async fn a_non_success_body_is_never_read() {
        // Far more than socket buffers and the client's read-ahead can hold:
        // the server can finish writing only if the client consumes it.
        const TOTAL: usize = 64 * 1024 * 1024;
        let server = TestServer::start(Script::new().on(
            "GET",
            "/table",
            Reply::status(503).with_body(BodyPlan::Streamed {
                total: TOTAL,
                piece: 64 * 1024,
            }),
        ));
        let (stage, body) = test_client(Duration::from_secs(2))
            .send_once(
                get(format!("{}/table", server.url())),
                &[200],
                &RequestContext::default(),
            )
            .await;
        assert!(matches!(stage, DispatchStage::Status { code: 503, .. }));
        assert!(body.is_none());
        let outcomes = server.shut_down();
        assert_eq!(outcomes.len(), 1);
        assert!(!outcomes[0].complete);
        assert!(outcomes[0].written < TOTAL);
    }

    #[tokio::test]
    async fn a_success_body_is_read_in_chunks_with_its_headers_named() {
        let server = TestServer::start(
            Script::new().on(
                "GET",
                "/table",
                Reply::status(200)
                    .with_header("x-secret-header", "header-value-canary")
                    .with_body(BodyPlan::Chunked(vec![
                        (Duration::ZERO, b"{\"a\":".to_vec()),
                        (Duration::from_millis(30), b"1}".to_vec()),
                    ])),
            ),
        );
        let (stage, body) = test_client(Duration::from_secs(2))
            .send_once(
                get(format!("{}/table", server.url())),
                &[200],
                &RequestContext::default(),
            )
            .await;
        let DispatchStage::Status { code, header_names } = &stage else {
            panic!("{stage:?}");
        };
        assert_eq!(*code, 200);
        assert!(header_names.iter().any(|name| name == "x-secret-header"));
        assert!(!format!("{stage:?}").contains("header-value-canary"));
        let mut body = body.expect("success body");
        let mut chunks = Vec::new();
        while let Some(chunk) = body.next_chunk().await.unwrap() {
            chunks.push(chunk);
        }
        assert!(chunks.len() >= 2, "{chunks:?}");
        assert_eq!(chunks.concat(), b"{\"a\":1}");
    }

    #[tokio::test]
    async fn a_body_read_stops_at_the_caller_deadline() {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/table",
            Reply::status(200).with_body(BodyPlan::Chunked(vec![
                (Duration::ZERO, b"{".to_vec()),
                (Duration::from_secs(5), b"}".to_vec()),
            ])),
        ));
        let context = RequestContext::with_deadline(Instant::now() + Duration::from_millis(300));
        let (_, body) = test_client(Duration::from_secs(10))
            .send_once(get(format!("{}/table", server.url())), &[200], &context)
            .await;
        let started = std::time::Instant::now();
        assert_eq!(
            body.expect("success body").read_to_end().await,
            Err(BodyReadError::DeadlineExceeded)
        );
        assert!(started.elapsed() < Duration::from_secs(3));
    }
    #[tokio::test]
    async fn the_caller_deadline_bounds_waiting_for_response_headers() {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/table",
            Reply::status(200).with_delay(Duration::from_secs(2)),
        ));
        let context = RequestContext::with_deadline(Instant::now() + Duration::from_millis(100));
        let started = std::time::Instant::now();
        let (stage, body) = test_client(Duration::from_secs(10))
            .send_once(get(format!("{}/table", server.url())), &[200], &context)
            .await;
        assert_eq!(
            stage,
            DispatchStage::WrittenNoStatus(NoStatusCause::ReadTimeout)
        );
        assert!(body.is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(server.count("GET", "/table"), 1);
    }

    #[tokio::test]
    async fn response_header_wait_and_slow_body_share_one_deadline() {
        let server = TestServer::start(
            Script::new().on(
                "GET",
                "/table",
                Reply::status(200)
                    .with_delay(Duration::from_millis(100))
                    .with_body(BodyPlan::Chunked(vec![
                        (Duration::ZERO, b"a".to_vec()),
                        (Duration::from_millis(100), b"b".to_vec()),
                        (Duration::from_millis(100), b"c".to_vec()),
                        (Duration::from_millis(100), b"d".to_vec()),
                    ])),
            ),
        );
        let context = RequestContext::with_deadline(Instant::now() + Duration::from_millis(250));
        let (stage, body) = test_client(Duration::from_secs(2))
            .send_once(get(format!("{}/table", server.url())), &[200], &context)
            .await;
        assert!(matches!(stage, DispatchStage::Status { code: 200, .. }));
        assert_eq!(
            body.unwrap().read_to_end().await,
            Err(BodyReadError::DeadlineExceeded)
        );
        assert_eq!(server.count("GET", "/table"), 1);
    }
}
