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
use hyper::body::Incoming;
use std::any::Any;
use std::convert::Infallible;
use std::future::poll_fn;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, panic_any, resume_unwind};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use http_body_util::{BodyExt, Empty};
use hyper::service::service_fn;
use hyper_util::client::proxy::matcher::Matcher;
use tokio::io::DuplexStream;
use tokio::sync::Notify;
use tokio::time::{Instant, timeout, timeout_at};

use super::super::route::HttpRouteConfig;
type PanicPayload = Box<dyn Any + Send + 'static>;
enum CleanupCause {
    Handshake { _error: HttpConnectionError },
    Response { _error: RawHttpError },
    Deadline { _error: tokio::time::error::Elapsed },
}
struct IoGuard(Arc<AtomicBool>);
impl Drop for IoGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn connected(
    io: DuplexStream,
    protocol: NegotiatedHttpProtocol,
    form: RequestTargetForm,
    dropped: Arc<AtomicBool>,
) -> ConnectedHttpIo {
    ConnectedHttpIo {
        io: OwnedIo::with_guard(Box::new(io), IoGuard(dropped)),
        negotiated_protocol: protocol,
        request_target_form: form,
    }
}

// All original request/future/driver custody outlives the timed behavior.
// One connection and two fixed request fields are polled inline; no child task exists.
struct Fixture {
    handshake: Option<HttpHandshakeFuture>,
    handshake_result: Option<Result<HttpConnection, HttpConnectionError>>,
    connection: Option<HttpConnection>,
    sends: [Option<HttpResponseFuture>; 2],
    uploads: [Option<H2Upload>; 2],
    outcomes: [Option<HttpResponseOutcome>; 2],
    poll_poisoned: [bool; 3],
    driver_result: Option<Result<(), RawHttpError>>,
    cleanup_causes: Vec<CleanupCause>,
    cleanup_deadline: Option<Instant>,
}
impl Fixture {
    fn new() -> Self {
        Self {
            handshake: None,
            handshake_result: None,
            connection: None,
            sends: [None, None],
            uploads: [None, None],
            outcomes: [None, None],
            poll_poisoned: [false; 3],
            driver_result: None,
            cleanup_causes: Vec::new(),
            cleanup_deadline: None,
        }
    }
    fn progress(&mut self, cx: &mut Context<'_>) {
        if let Some(connection) = self.connection.as_mut() {
            if !connection.driver_completed() {
                if let Poll::Ready(result) = connection.poll_connection(cx) {
                    self.driver_result = Some(result);
                }
            }
        }
        for upload in self.uploads.iter_mut().flatten() {
            if !upload.complete() {
                match upload.poll_progress(cx) {
                    Poll::Ready(Ok(UploadStep::Progress | UploadStep::Complete)) => {
                        cx.waker().wake_by_ref()
                    }
                    Poll::Ready(Err(error)) => panic_any(error),
                    Poll::Pending => {}
                }
            }
        }
    }
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.handshake_result.is_some() {
            return Poll::Ready(());
        }
        if self.poll_poisoned[0] {
            return Poll::Pending;
        }
        let Some(handshake) = self.handshake.as_mut() else {
            return Poll::Pending;
        };
        self.poll_poisoned[0] = true;
        let result = Pin::new(handshake).poll(cx);
        self.poll_poisoned[0] = false;
        if let Poll::Ready(result) = result {
            self.handshake_result = Some(result);
            // Real result is stable before a completion callback can unwind.
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
    async fn connect(&mut self, io: ConnectedHttpIo, route: &PreparedHttpRoute) {
        self.handshake = Some(HttpConnection::handshake(io, route));
        poll_fn(|cx| self.poll_handshake(cx)).await;
        match self.handshake_result.take().unwrap() {
            Ok(connection) => self.connection = Some(connection),
            Err(error) => panic_any(error),
        }
        drop(self.handshake.take());
    }
    async fn ready(&mut self) {
        poll_fn(|cx| {
            self.progress(cx);
            self.connection.as_mut().unwrap().poll_ready(cx)
        })
        .await
        .unwrap();
    }
    fn send(
        &mut self,
        index: usize,
        source: &mut Option<Request<Buffer>>,
        route: &PreparedHttpRoute,
    ) {
        // Publication, real futures and upload fields stay in this fixture.
        // Missing future is never interpreted as a headers or IO-exit result.
        self.sends[index] = Some(
            self.connection
                .as_mut()
                .unwrap()
                .start_send_in(source, route, &Arc::new(()), &mut self.uploads[index])
                .unwrap(),
        );
    }
    fn poll_response(&mut self, index: usize, cx: &mut Context<'_>) -> Poll<()> {
        if self.outcomes[index].is_some() {
            return Poll::Ready(());
        }
        if self.poll_poisoned[index + 1] {
            return Poll::Pending;
        }
        let Some(future) = self.sends[index].as_mut() else {
            return Poll::Pending;
        };
        self.poll_poisoned[index + 1] = true;
        let result = Pin::new(future).poll(cx);
        self.poll_poisoned[index + 1] = false;
        if let Poll::Ready(outcome) = result {
            self.outcomes[index] = Some(outcome);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
    async fn response(&mut self, index: usize) -> HttpResponseOutcome {
        poll_fn(|cx| {
            self.progress(cx);
            self.poll_response(index, cx)
        })
        .await;
        let outcome = self.outcomes[index].take().unwrap();
        drop(self.sends[index].take());
        outcome
    }
    async fn drive<F: Future>(&mut self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        poll_fn(|cx| {
            self.progress(cx);
            future.as_mut().poll(cx)
        })
        .await
    }
    async fn finish_wire(&mut self) {
        for upload in self.uploads.iter_mut() {
            assert!(upload.as_ref().is_none_or(H2Upload::complete));
            drop(upload.take());
        }
        self.connection.as_mut().unwrap().close_sender();
        poll_fn(|cx| {
            self.progress(cx);
            if self.connection.as_ref().unwrap().driver_completed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(self.connection.take());
    }
    async fn cleanup(&mut self) -> bool {
        let deadline = *self
            .cleanup_deadline
            .get_or_insert_with(|| Instant::now() + Duration::from_secs(5));
        if let Some(connection) = self.connection.as_mut() {
            connection.close_sender();
        }
        // Actual original driver/IO fields exit before the request fields are retired.
        drop(self.connection.take());
        for upload in self.uploads.iter_mut() {
            drop(upload.take());
        }
        let finish = poll_fn(|cx| {
            if self.handshake_result.is_none()
                && self
                    .handshake
                    .as_ref()
                    .is_some_and(|future| future.completed)
            {
                // The connect helper may already have moved its real Err into
                // the preserved primary. Retire only the completed backing.
                drop(self.handshake.take());
            }
            if self.handshake.is_some() && self.handshake_result.is_none() {
                let _ = self.poll_handshake(cx);
            }
            if self.handshake_result.is_some() {
                // A late connection really exits; retain the original error
                // cause if this handshake instead returned an error result.
                match self.handshake_result.take().unwrap() {
                    Ok(connection) => drop(connection),
                    Err(error) => self
                        .cleanup_causes
                        .push(CleanupCause::Handshake { _error: error }),
                }
                drop(self.handshake.take());
            }
            for index in 0..2 {
                if self.outcomes[index].is_none()
                    && self.sends[index]
                        .as_ref()
                        .is_some_and(|future| future.completed)
                {
                    drop(self.sends[index].take());
                }
                if self.sends[index].is_some() && self.outcomes[index].is_none() {
                    let _ = self.poll_response(index, cx);
                }
                if self.outcomes[index].is_some() {
                    match self.outcomes[index].take().unwrap().response {
                        Ok(response) => drop(response),
                        Err(error) => self
                            .cleanup_causes
                            .push(CleanupCause::Response { _error: error }),
                    }
                    drop(self.sends[index].take());
                }
            }
            if self.handshake.is_none() && self.sends.iter().all(Option::is_none) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        if let Err(error) = timeout_at(deadline, finish).await {
            self.cleanup_causes
                .push(CleanupCause::Deadline { _error: error });
            return false;
        }
        true
    }
    async fn conclude(mut self, primary: Option<PanicPayload>) {
        let cleanup = AssertUnwindSafe(self.cleanup()).catch_unwind().await;
        let (complete, cleanup_panic) = match cleanup {
            Ok(complete) => (complete, None),
            Err(payload) => (false, Some(payload)),
        };
        let driver_error = match self.driver_result.take() {
            Some(Err(error)) => Some(error),
            _ => None,
        };
        if !complete
            || !self.cleanup_causes.is_empty()
            || cleanup_panic.is_some()
            || driver_error.is_some()
        {
            let cleanup_causes = std::mem::take(&mut self.cleanup_causes);
            panic_any(FixtureFailure {
                _primary: primary,
                _driver_error: driver_error,
                _cleanup_causes: cleanup_causes,
                _cleanup_panic: cleanup_panic,
                _cleanup_complete: complete,
                _retained: ManuallyDrop::new(Box::new(self)),
            });
        }
        drop(self);
        if let Some(payload) = primary {
            resume_unwind(payload);
        }
    }
}

// The harness keeps the original primary, driver/response errors and cleanup panic.
// A failed deadline explicitly retains the sole finite root to process exit;
// dropping a JoinHandle or requesting abort never becomes cleanup success.
struct FixtureFailure {
    _primary: Option<PanicPayload>,
    _driver_error: Option<RawHttpError>,
    _cleanup_causes: Vec<CleanupCause>,
    _cleanup_panic: Option<PanicPayload>,
    _cleanup_complete: bool,
    _retained: ManuallyDrop<Box<Fixture>>,
}
fn behavior_result(
    result: Result<Result<(), tokio::time::error::Elapsed>, PanicPayload>,
) -> Option<PanicPayload> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(Box::new(error)),
        Err(payload) => Some(payload),
    }
}
fn accept_response(outcome: HttpResponseOutcome) -> Response<RawBody> {
    match outcome.response {
        Ok(response) => response,
        Err(error) => panic_any(error),
    }
}

#[tokio::test]
async fn h1_keeps_head_signed_headers_and_raw_query_while_stripping_proxy_auth() {
    let mut fixture = Fixture::new();
    let result = AssertUnwindSafe(timeout(Duration::from_secs(10), async {
        let (client, peer) = tokio::io::duplex(16384);
        let uri: Uri = "http://origin.example/a%2Fb?sig=a%2Bb&x=%25"
            .parse()
            .unwrap();
        let route = HttpRouteConfig::local().prepare(&uri).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(AtomicBool::new(false));
        let server_observed = Arc::clone(&observed);
        let server = async {
            let service = service_fn(move |request: Request<Incoming>| {
                let observed = Arc::clone(&server_observed);
                async move {
                    assert_eq!(request.method(), Method::HEAD);
                    assert_eq!(request.uri().to_string(), "/a%2Fb?sig=a%2Bb&x=%25");
                    assert_eq!(request.headers()[http::header::HOST], "signed.example");
                    assert_eq!(
                        request.headers()[http::header::AUTHORIZATION],
                        "Bearer original"
                    );
                    assert!(
                        !request
                            .headers()
                            .contains_key(http::header::PROXY_AUTHORIZATION)
                    );
                    assert!(
                        request
                            .into_body()
                            .collect()
                            .await
                            .unwrap()
                            .to_bytes()
                            .is_empty()
                    );
                    observed.store(true, Ordering::SeqCst);
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(202)
                            .header("content-length", "123")
                            .body(Empty::<Bytes>::new())
                            .unwrap(),
                    )
                }
            });
            hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(peer), service)
                .await
                .unwrap();
        };
        let client = async {
            fixture
                .connect(
                    connected(
                        client,
                        NegotiatedHttpProtocol::H1,
                        RequestTargetForm::Origin,
                        Arc::clone(&dropped),
                    ),
                    &route,
                )
                .await;
            fixture.ready().await;
            let mut source = Some(
                Request::builder()
                    .method(Method::HEAD)
                    .uri(uri.clone())
                    .header("host", "signed.example")
                    .header("authorization", "Bearer original")
                    .header("proxy-authorization", "never-origin")
                    .body(Buffer::new())
                    .unwrap(),
            );
            fixture.send(0, &mut source, &route);
            assert!(source.is_none());
            let outcome = fixture.response(0).await;
            // Fixture stored the actual result before dropping the completed future.
            assert_eq!(outcome.original_uri, uri);
            assert!(outcome.is_head);
            let response = accept_response(outcome);
            assert_eq!(response.status(), 202);
            assert_eq!(response.headers()["content-length"], "123");
            assert!(
                fixture
                    .drive(response.into_body().collect())
                    .await
                    .unwrap()
                    .to_bytes()
                    .is_empty()
            );
            assert!(observed.load(Ordering::SeqCst));
            assert!(!dropped.load(Ordering::SeqCst));
            fixture.finish_wire().await;
            assert!(dropped.load(Ordering::SeqCst));
        };
        tokio::join!(client, server);
    }))
    .catch_unwind()
    .await;
    fixture.conclude(behavior_result(result)).await;
}

#[tokio::test]
async fn h1_forward_proxy_uses_absolute_form_and_preserves_body_and_explicit_auth() {
    let mut fixture = Fixture::new();
    let result = AssertUnwindSafe(timeout(Duration::from_secs(10), async {
        let (client, peer) = tokio::io::duplex(16384);
        let config = HttpRouteConfig::with_matcher(
            Matcher::builder()
                .all("http://user:password@proxy.example:3128")
                .build(),
        );
        let first_uri: Uri = "http://origin.example/a%2Fb?sig=raw%2Bquery"
            .parse()
            .unwrap();
        let first_route = config.prepare(&first_uri).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let server_count = Arc::clone(&count);
        let dropped = Arc::new(AtomicBool::new(false));
        let server = async {
            let service = service_fn(move |request: Request<Incoming>| {
                let count = Arc::clone(&server_count);
                async move {
                    let index = count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.method(), Method::POST);
                    assert_eq!(
                        request.uri().to_string(),
                        "http://origin.example/a%2Fb?sig=raw%2Bquery"
                    );
                    assert_eq!(request.headers()[http::header::HOST], "origin.example");
                    assert_eq!(request.headers()["x-signed-value"], "keep-exact");
                    assert_eq!(
                        request.headers()[http::header::PROXY_AUTHORIZATION],
                        if index == 0 {
                            "Basic dXNlcjpwYXNzd29yZA=="
                        } else {
                            "Custom caller-value"
                        }
                    );
                    assert_eq!(
                        request
                            .into_body()
                            .collect()
                            .await
                            .unwrap()
                            .to_bytes()
                            .as_ref(),
                        b"original-body"
                    );
                    Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new()))
                }
            });
            hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(peer), service)
                .await
                .unwrap();
        };
        let client = async {
            fixture
                .connect(
                    connected(
                        client,
                        NegotiatedHttpProtocol::H1,
                        RequestTargetForm::Absolute,
                        Arc::clone(&dropped),
                    ),
                    &first_route,
                )
                .await;
            for explicit in [false, true] {
                let route = config.prepare(&first_uri).unwrap();
                fixture.ready().await;
                let mut request = Request::builder()
                    .method(Method::POST)
                    .uri(first_uri.clone())
                    .header("x-signed-value", "keep-exact")
                    .body(Buffer::from(Bytes::from_static(b"original-body")))
                    .unwrap();
                if explicit {
                    request.headers_mut().insert(
                        http::header::PROXY_AUTHORIZATION,
                        HeaderValue::from_static("Custom caller-value"),
                    );
                }
                let mut source = Some(request);
                fixture.send(0, &mut source, &route);
                let outcome = fixture.response(0).await;
                assert_eq!(outcome.original_uri, first_uri);
                assert!(!outcome.is_head);
                fixture
                    .drive(accept_response(outcome).into_body().collect())
                    .await
                    .unwrap();
            }
            fixture.finish_wire().await;
            assert!(dropped.load(Ordering::SeqCst));
        };
        tokio::join!(client, server);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }))
    .catch_unwind()
    .await;
    fixture.conclude(behavior_result(result)).await;
}

#[tokio::test]
async fn source_and_route_refusals_preserve_the_original_request_before_publication() {
    let mut fixture = Fixture::new();
    let result = AssertUnwindSafe(timeout(Duration::from_secs(5), async {
        let (client, _peer) = tokio::io::duplex(16384);
        let uri: Uri = "http://origin.example/original?sig=keep".parse().unwrap();
        let route = HttpRouteConfig::local().prepare(&uri).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        fixture
            .connect(
                connected(
                    client,
                    NegotiatedHttpProtocol::H1,
                    RequestTargetForm::Origin,
                    Arc::clone(&dropped),
                ),
                &route,
            )
            .await;
        let mut source = Some(
            Request::builder()
                .uri(uri.clone())
                .header("x-proof", "original")
                .body(Buffer::from(Bytes::from_static(b"kept")))
                .unwrap(),
        );
        let custody_exited = Arc::new(AtomicBool::new(false));
        let custody = Arc::new(IoGuard(Arc::clone(&custody_exited)));
        let mut upload = None;
        assert!(matches!(
            fixture.connection.as_mut().unwrap().start_send_in(
                &mut source,
                &route,
                &custody,
                &mut upload
            ),
            Err(HttpConnectionError::NotReady)
        ));
        assert!(upload.is_none());
        assert_eq!(source.as_ref().unwrap().uri(), &uri);
        assert_eq!(source.as_ref().unwrap().body().len(), 4);
        assert!(!custody_exited.load(Ordering::SeqCst));
        fixture.ready().await;
        let other = HttpRouteConfig::local()
            .prepare(&"http://other.example/original?sig=keep".parse().unwrap())
            .unwrap();
        assert!(matches!(
            fixture.connection.as_mut().unwrap().start_send_in(
                &mut source,
                &other,
                &custody,
                &mut upload
            ),
            Err(HttpConnectionError::Invalid(_))
        ));
        assert_eq!(source.as_ref().unwrap().uri(), &uri);
        assert_eq!(source.as_ref().unwrap().headers()["x-proof"], "original");
        assert_eq!(source.as_ref().unwrap().body().len(), 4);
        assert!(!custody_exited.load(Ordering::SeqCst));
        drop(source);
        drop(custody);
        assert!(custody_exited.load(Ordering::SeqCst));
        assert!(!dropped.load(Ordering::SeqCst));
        fixture.finish_wire().await;
        assert!(dropped.load(Ordering::SeqCst));
    }))
    .catch_unwind()
    .await;
    fixture.conclude(behavior_result(result)).await;
}

async fn server_until<F: Future>(
    connection: &mut h2::server::Connection<DuplexStream, Bytes>,
    future: F,
) -> F::Output {
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(output);
        }
        if let Poll::Ready(result) = connection.poll_closed(cx) {
            result.unwrap();
            panic!("server closed before the expected stream result");
        }
        Poll::Pending
    })
    .await
}

#[tokio::test]
async fn h2_second_response_progresses_while_first_is_withheld_using_the_original_connection() {
    let mut fixture = Fixture::new();
    let result = AssertUnwindSafe(timeout(Duration::from_secs(10), async {
        let (client, peer) = tokio::io::duplex(65536);
        let config = HttpRouteConfig::local();
        let uri1: Uri = "http://origin.example/first%2Fobject?sig=first%2Bvalue"
            .parse()
            .unwrap();
        let uri2: Uri = "http://origin.example/second?sig=second%25value"
            .parse()
            .unwrap();
        let route1 = config.prepare(&uri1).unwrap();
        let route2 = config.prepare(&uri2).unwrap();
        let second_observed = Arc::new(Notify::new());
        let first_observed = Arc::new(Notify::new());
        let peer_closed = Notify::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let server = async {
            // Public h2 server needs no executor; this test retains/polls it inline.
            let mut connection = h2::server::handshake(peer).await.unwrap();
            let (first, mut reply1) = connection.accept().await.unwrap().unwrap();
            let (second, mut reply2) = connection.accept().await.unwrap().unwrap();
            assert_eq!(first.method(), Method::POST);
            assert_eq!(first.uri(), &uri1);
            assert_eq!(second.method(), Method::HEAD);
            assert_eq!(second.uri(), &uri2);
            assert_eq!(first.headers()["x-signed"], "first-exact");
            assert!(
                !first
                    .headers()
                    .contains_key(http::header::PROXY_AUTHORIZATION)
            );
            let mut body = first.into_body();
            let payload = server_until(&mut connection, async {
                let mut payload = Vec::new();
                while let Some(data) = body.data().await {
                    let data = data.unwrap();
                    payload.extend_from_slice(&data);
                    body.flow_control().release_capacity(data.len()).unwrap();
                }
                payload
            })
            .await;
            assert_eq!(&payload, b"first-body");
            reply2
                .send_response(Response::builder().status(202).body(()).unwrap(), true)
                .unwrap();
            // The first headers do not exist until the second were really received.
            server_until(&mut connection, second_observed.notified()).await;
            reply1
                .send_response(Response::builder().status(201).body(()).unwrap(), true)
                .unwrap();
            server_until(&mut connection, first_observed.notified()).await;
            // These are the actual completed server-side stream aliases.
            // Retire them before starting the real graceful close handshake.
            drop((body, second, reply1, reply2));
            connection.graceful_shutdown();
            poll_fn(|cx| connection.poll_closed(cx)).await.unwrap();
            peer_closed.notify_one();
        };
        let client = async {
            fixture
                .connect(
                    connected(
                        client,
                        NegotiatedHttpProtocol::H2,
                        RequestTargetForm::Origin,
                        Arc::clone(&dropped),
                    ),
                    &route1,
                )
                .await;
            fixture.ready().await;
            let mut source1 = Some(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri1.clone())
                    .header("x-signed", "first-exact")
                    .header("proxy-authorization", "never-origin")
                    .body(Buffer::from(Bytes::from_static(b"first-body")))
                    .unwrap(),
            );
            fixture.send(0, &mut source1, &route1);
            fixture.ready().await;
            let mut source2 = Some(
                Request::builder()
                    .method(Method::HEAD)
                    .uri(uri2.clone())
                    .body(Buffer::new())
                    .unwrap(),
            );
            fixture.send(1, &mut source2, &route2);
            let outcome2 = fixture.response(1).await;
            assert_eq!(outcome2.original_uri, uri2);
            assert!(outcome2.is_head);
            let response = accept_response(outcome2);
            assert_eq!(response.status(), 202);
            fixture.drive(response.into_body().collect()).await.unwrap();
            second_observed.notify_one();
            let outcome1 = fixture.response(0).await;
            assert_eq!(outcome1.original_uri, uri1);
            assert!(!outcome1.is_head);
            let response = accept_response(outcome1);
            assert_eq!(response.status(), 201);
            fixture.drive(response.into_body().collect()).await.unwrap();
            first_observed.notify_one();
            // Keep driving the same client IO while the peer really flushes
            // GOAWAY/PING and reports Ok; never swallow a BrokenPipe here.
            fixture.drive(peer_closed.notified()).await;
            fixture.finish_wire().await;
            assert!(dropped.load(Ordering::SeqCst));
        };
        tokio::join!(client, server);
    }))
    .catch_unwind()
    .await;
    fixture.conclude(behavior_result(result)).await;
}
