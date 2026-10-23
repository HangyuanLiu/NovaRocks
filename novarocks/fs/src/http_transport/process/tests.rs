// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use std::convert::Infallible;
use std::future::poll_fn;
use std::panic::{panic_any, resume_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::super::connector::{NegotiatedHttpProtocol, RequestTargetForm};
use super::super::route::HttpRouteConfig;
use crate::TokioFileIoRuntime;
use bytes::Bytes;
use futures::FutureExt;
use http_body_util::Full;
use hyper_util::rt::TokioIo;
use novarocks_transport_io::OwnedIo;
use opendal::raw::oio::ReadStream;
use tokio::io::DuplexStream;
use tokio::sync::Notify;
use tokio::time::{Instant, timeout, timeout_at};

const DATA: &[u8] = b"real original process body";
struct CountDrop(Arc<AtomicUsize>);
impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Handoff {
    drops: Arc<AtomicUsize>,
}
impl PayloadHandoff for Handoff {
    type Guard = CountDrop;
    fn poll_handoff(&mut self, _: &mut Context<'_>, _: &Bytes) -> Poll<FileResult<CountDrop>> {
        Poll::Ready(Ok(CountDrop(Arc::clone(&self.drops))))
    }
}
type Root = HttpProcessRoot<CountDrop, Handoff>;
type Submit = HttpSubmission<CountDrop, Handoff>;
fn root(requests: usize) -> (Root, Submit) {
    let handle = Handle::current();
    let runtime: Arc<dyn FileIoRuntime> = Arc::new(TokioFileIoRuntime::new(handle.clone()));
    HttpProcessRoot::try_new(
        handle,
        runtime,
        HttpProcessLimits {
            connections: NonZeroUsize::new(1).unwrap(),
            requests: NonZeroUsize::new(requests).unwrap(),
        },
    )
    .unwrap()
}
fn input(io: DuplexStream, h2: bool, io_exit: Arc<AtomicUsize>) -> HttpConnectionInput {
    let uri = "http://root.fixture/object".parse().unwrap();
    HttpConnectionInput {
        io: ConnectedHttpIo {
            io: OwnedIo::with_guard(Box::new(io), CountDrop(io_exit)),
            negotiated_protocol: if h2 {
                NegotiatedHttpProtocol::H2
            } else {
                NegotiatedHttpProtocol::H1
            },
            request_target_form: RequestTargetForm::Origin,
        },
        route: HttpRouteConfig::local().prepare(&uri).unwrap(),
    }
}
fn request(
    token: HttpConnectionToken,
    guard_exit: Arc<AtomicUsize>,
    payload_exit: Arc<AtomicUsize>,
) -> HttpRequestInput<CountDrop, Handoff> {
    let uri: Uri = "http://root.fixture/object".parse().unwrap();
    HttpRequestInput {
        connection: token,
        request: Some(
            Request::builder()
                .uri(uri.clone())
                .body(Buffer::new())
                .unwrap(),
        ),
        route: HttpRouteConfig::local().prepare(&uri).unwrap(),
        handoff: Handoff {
            drops: payload_exit,
        },
        source_guard: Arc::new(CountDrop(guard_exit)),
    }
}

/// The fixture retains the original server join even across a bounded wait.
struct Server {
    task: Option<FileTask>,
    joined: bool,
}
impl Drop for Server {
    fn drop(&mut self) {
        if !self.joined {
            if let Some(task) = self.task.as_mut() {
                task.abort();
            }
            // Failure fallback does not turn Drop of a handle into actual exit.
            std::mem::forget(self.task.take());
        }
    }
}
async fn server_until<F: Future>(
    connection: &mut h2::server::Connection<DuplexStream, Bytes>,
    future: F,
) -> F::Output {
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| {
        if let Poll::Ready(value) = future.as_mut().poll(cx) {
            return Poll::Ready(value);
        }
        if let Poll::Ready(result) = connection.poll_closed(cx) {
            result.unwrap();
            panic!("server exited before the expected request witness");
        }
        Poll::Pending
    })
    .await
}
fn pending_header_server(mut peer: DuplexStream, seen: Arc<Notify>) -> Server {
    let task = Handle::current().spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).await.unwrap(), 1);
            head.push(byte[0]);
            assert!(head.len() <= 1024);
        }
        seen.notify_one();
        // The genuine client C must exit; no response or parse-error swallow.
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    });
    Server {
        task: Some(FileTask::new(task)),
        joined: false,
    }
}

fn normal_server(peer: DuplexStream, h2: bool) -> Server {
    let task = Handle::current().spawn(async move {
        if h2 {
            let mut connection = h2::server::handshake(peer).await.unwrap();
            let (_, mut reply) = connection.accept().await.unwrap().unwrap();
            let mut stream = reply.send_response(Response::new(()), false).unwrap();
            stream.send_data(Bytes::from_static(DATA), true).unwrap();
            poll_fn(|cx| connection.poll_closed(cx)).await.unwrap();
        } else {
            let service = hyper::service::service_fn(|_: Request<hyper::body::Incoming>| async {
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(DATA))))
            });
            hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(peer), service)
                .await
                .unwrap();
        }
    });
    Server {
        task: Some(FileTask::new(task)),
        joined: false,
    }
}
struct CaseFailure {
    primary: Option<PanicPayload>,
    root_timeout: bool,
    root_join: Option<JoinError>,
    server_timeout: bool,
    server_join: Option<JoinError>,
}
impl std::fmt::Debug for CaseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaseFailure")
            .field("primary", &self.primary.is_some())
            .field("root_timeout", &self.root_timeout)
            .field("root_join", &self.root_join.is_some())
            .field("server_timeout", &self.server_timeout)
            .field("server_join", &self.server_join.is_some())
            .finish()
    }
}
async fn finish(root: &mut Root, server: &mut Server, primary: Result<(), PanicPayload>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let root_outcome = timeout_at(deadline, poll_fn(|cx| root.poll_shutdown(cx))).await;
    server.task.as_mut().unwrap().abort();
    let server_outcome = timeout_at(
        deadline,
        poll_fn(|cx| server.task.as_mut().unwrap().poll_drain(cx)),
    )
    .await;
    server.joined = server_outcome.is_ok();
    let root_timeout = root_outcome.is_err();
    let root_join = root_outcome.ok().and_then(Result::err);
    let server_timeout = server_outcome.is_err();
    let server_join = server_outcome
        .ok()
        .and_then(Result::err)
        .filter(|error| !error.is_cancelled());
    if root_timeout || root_join.is_some() || server_timeout || server_join.is_some() {
        panic_any(CaseFailure {
            primary: primary.err(),
            root_timeout,
            root_join,
            server_timeout,
            server_join,
        });
    }
    if let Err(payload) = primary {
        resume_unwind(payload);
    }
}

#[tokio::test]
async fn real_h1_h2_eof_is_independent_of_idle_io_and_cancelled_shutdown_keeps_original_join() {
    for h2 in [false, true] {
        let (io, peer) = tokio::io::duplex(65536);
        let io_exit = Arc::new(AtomicUsize::new(0));
        let guard_exit = Arc::new(AtomicUsize::new(0));
        let payload_exit = Arc::new(AtomicUsize::new(0));
        let mut server = normal_server(peer, h2);
        let (mut root, submission) = root(1);
        let result = AssertUnwindSafe(async {
            let mut connection_source = Some(input(io, h2, Arc::clone(&io_exit)));
            let token = submission
                .connect_in(&mut connection_source)
                .finish()
                .unwrap();
            assert!(connection_source.is_none());
            let mut source = Some(request(
                token,
                Arc::clone(&guard_exit),
                Arc::clone(&payload_exit),
            ));
            let reply = submission.request_in(&mut source).finish().unwrap();
            let response = timeout(Duration::from_secs(5), reply)
                .await
                .unwrap()
                .unwrap();
            let mut body = response.response.into_body();
            let first = timeout(Duration::from_secs(5), body.read())
                .await
                .unwrap()
                .unwrap();
            assert!(!first.is_empty());
            let bytes = first.to_bytes();
            drop(first);
            let retained_payload = bytes.clone();
            let mut content = bytes.to_vec();
            drop(bytes);
            loop {
                let part = timeout(Duration::from_secs(5), body.read())
                    .await
                    .unwrap()
                    .unwrap();
                if part.is_empty() {
                    break;
                }
                content.extend_from_slice(&part.to_bytes());
                assert!(content.len() <= DATA.len());
            }
            assert_eq!(content, DATA);
            assert_eq!(guard_exit.load(Ordering::SeqCst), 1);
            assert_eq!(io_exit.load(Ordering::SeqCst), 0);
            let counts = poll_fn(|cx| root.inspect(cx, |state| state.retained_counts())).await;
            assert_eq!(counts, (1, 1)); // The EOS body still owns its metadata view.
            let original_cell = Arc::as_ptr(root.cell.as_ref().unwrap());
            let wait = timeout(Duration::ZERO, poll_fn(|cx| root.poll_shutdown(cx))).await;
            assert!(wait.is_err());
            assert_eq!(Arc::as_ptr(root.cell.as_ref().unwrap()), original_cell);
            assert!(root.driver.is_some() && !root.drained);
            drop(body);
            drop(retained_payload);
        })
        .catch_unwind()
        .await;
        finish(&mut root, &mut server, result).await;
        assert!(root.drained && root.joined);
        assert_eq!(io_exit.load(Ordering::SeqCst), 1);
        assert_eq!(guard_exit.load(Ordering::SeqCst), 1);
        assert!(payload_exit.load(Ordering::SeqCst) >= 1);
    }
}

#[tokio::test]
async fn h2_abandoned_source_exits_its_own_row_while_existing_peer_finishes_then_real_root_joins() {
    let (io, peer) = tokio::io::duplex(65536);
    let release_b = Arc::new(Notify::new());
    let release_server = Arc::clone(&release_b);
    let task = Handle::current().spawn(async move {
        let mut connection = h2::server::handshake(peer).await.unwrap();
        let (_, mut first) = connection.accept().await.unwrap().unwrap();
        let mut a = first.send_response(Response::new(()), false).unwrap();
        a.send_data(Bytes::from_static(b"abandoned prefix"), false)
            .unwrap();
        let (_, mut second) = connection.accept().await.unwrap().unwrap();
        let mut b = second.send_response(Response::new(()), false).unwrap();
        server_until(&mut connection, release_server.notified()).await;
        b.send_data(Bytes::from_static(DATA), true).unwrap();
        drop(a);
        poll_fn(|cx| connection.poll_closed(cx)).await.unwrap();
    });
    let mut server = Server {
        task: Some(FileTask::new(task)),
        joined: false,
    };
    let io_exit = Arc::new(AtomicUsize::new(0));
    let a_exit = Arc::new(AtomicUsize::new(0));
    let b_exit = Arc::new(AtomicUsize::new(0));
    let payload_exit = Arc::new(AtomicUsize::new(0));
    let (mut root, submission) = root(2);
    let result = AssertUnwindSafe(async {
        let mut connect_source = Some(input(io, true, Arc::clone(&io_exit)));
        let token = submission.connect_in(&mut connect_source).finish().unwrap();
        let mut a = Some(request(
            token,
            Arc::clone(&a_exit),
            Arc::clone(&payload_exit),
        ));
        let a_reply = submission.request_in(&mut a).finish().unwrap();
        let mut b = Some(request(
            token,
            Arc::clone(&b_exit),
            Arc::clone(&payload_exit),
        ));
        let b_reply = submission.request_in(&mut b).finish().unwrap();
        let a_response = timeout(Duration::from_secs(5), a_reply)
            .await
            .unwrap()
            .unwrap();
        let b_response = timeout(Duration::from_secs(5), b_reply)
            .await
            .unwrap()
            .unwrap();
        drop(a_response); // Only A's public view disappears; B has no DATA yet.
        timeout(
            Duration::from_secs(5),
            poll_fn(|cx| {
                match root.inspect(cx, |state| {
                    state
                        .requests
                        .iter()
                        .filter(|r| r.token == Some(token))
                        .count()
                        == 1
                        && a_exit.load(Ordering::SeqCst) == 1
                }) {
                    Poll::Ready(true) => Poll::Ready(()),
                    Poll::Ready(false) | Poll::Pending => Poll::Pending,
                }
            }),
        )
        .await
        .unwrap();
        // Public direct-h2 reset retires A's local protocol refs independently
        // of B and the shared IO. The old whole-C close assertion no longer
        // describes this per-request cancellation contract.
        assert_eq!(a_exit.load(Ordering::SeqCst), 1);
        assert_eq!(io_exit.load(Ordering::SeqCst), 0);
        assert!(
            poll_fn(
                |cx| root.inspect(cx, |state| state.ingress.rows.lock().unwrap().connections
                    [token.index()]
                .accepting)
            )
            .await
        );
        release_b.notify_one();
        let mut body = b_response.response.into_body();
        let mut received = Vec::new();
        loop {
            let part = timeout(Duration::from_secs(5), body.read())
                .await
                .unwrap()
                .unwrap();
            if part.is_empty() {
                break;
            }
            received.extend_from_slice(&part.to_bytes());
            assert!(received.len() <= DATA.len());
        }
        assert_eq!(received, DATA);
        drop(body);
    })
    .catch_unwind()
    .await;
    finish(&mut root, &mut server, result).await;
    assert_eq!(a_exit.load(Ordering::SeqCst), 1);
    assert_eq!(b_exit.load(Ordering::SeqCst), 1);
    assert_eq!(io_exit.load(Ordering::SeqCst), 1);
    assert!(root.drained && root.joined);
}

#[tokio::test]
async fn losing_the_only_root_closes_original_ingress_before_retaining_failed_custody() {
    let (root, submission) = root(1);
    drop(root); // Illegal loss deliberately retains its one original join/state.
    let (io, _peer) = tokio::io::duplex(1024);
    let io_exit = Arc::new(AtomicUsize::new(0));
    let mut source = Some(input(io, false, Arc::clone(&io_exit)));
    assert!(matches!(
        submission.connect_in(&mut source).finish(),
        Err(HttpSubmitError::Closed)
    ));
    assert!(source.is_some());
    assert_eq!(io_exit.load(Ordering::SeqCst), 0);
    drop(source);
    assert_eq!(io_exit.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn real_pending_header_source_and_original_io_exit_before_request_row_reuse() {
    let (io, peer) = tokio::io::duplex(65536);
    let seen = Arc::new(Notify::new());
    let mut server = pending_header_server(peer, Arc::clone(&seen));
    let io_exit = Arc::new(AtomicUsize::new(0));
    let source_exit = Arc::new(AtomicUsize::new(0));
    let (mut root, submission) = root(1);
    let reused_io_exit = Arc::new(AtomicUsize::new(0));
    let mut reused_peer = None;
    let result = AssertUnwindSafe(async {
        let mut connect_source = Some(input(io, false, Arc::clone(&io_exit)));
        let token = submission.connect_in(&mut connect_source).finish().unwrap();
        let mut source = Some(request(
            token,
            Arc::clone(&source_exit),
            Arc::new(AtomicUsize::new(0)),
        ));
        let ticket = submission.request_in(&mut source).finish().unwrap();
        timeout(Duration::from_secs(5), seen.notified())
            .await
            .unwrap();
        let active = poll_fn(|cx| {
            root.inspect(cx, |state| {
                state
                    .requests
                    .iter()
                    .any(|r| r.token == Some(token) && r.future.is_some() && r.input.is_some())
            })
        })
        .await;
        assert!(active);
        assert_eq!(source_exit.load(Ordering::SeqCst), 0);
        drop(ticket);
        timeout(
            Duration::from_secs(5),
            poll_fn(
                |cx| match root.inspect(cx, |state| state.retained_counts()) {
                    Poll::Ready((0, 0)) => Poll::Ready(()),
                    Poll::Ready(_) | Poll::Pending => Poll::Pending,
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(source_exit.load(Ordering::SeqCst), 1);
        assert_eq!(io_exit.load(Ordering::SeqCst), 1);
        // Reuse the actual original C slot only after its old IO/R exited.
        let (next_io, next_peer) = tokio::io::duplex(1024);
        reused_peer = Some(next_peer);
        let mut next = Some(input(next_io, false, Arc::clone(&reused_io_exit)));
        let replacement = submission.connect_in(&mut next).finish().unwrap();
        assert!(next.is_none());
        assert_eq!(replacement.index(), token.index());
        assert!(replacement.generation().get() > token.generation().get());
        let refused_exit = Arc::new(AtomicUsize::new(0));
        let mut stale = Some(request(
            token,
            Arc::clone(&refused_exit),
            Arc::new(AtomicUsize::new(0)),
        ));
        assert!(matches!(
            submission.request_in(&mut stale).finish(),
            Err(HttpSubmitError::StaleConnection)
        ));
        assert!(stale.is_some());
        assert_eq!(refused_exit.load(Ordering::SeqCst), 0);
        drop(stale);
        assert_eq!(refused_exit.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    finish(&mut root, &mut server, result).await;
    assert!(root.drained && root.joined);
    assert_eq!(reused_io_exit.load(Ordering::SeqCst), 1);
    drop(reused_peer);
}

#[tokio::test]
async fn h2_cancel_segmented_upload_retires_only_a_while_existing_b_receives_data() {
    let (io, peer) = tokio::io::duplex(65536);
    let reset_seen = Arc::new(Notify::new());
    let peer_reset_seen = Arc::clone(&reset_seen);
    let release_b = Arc::new(Notify::new());
    let peer_release_b = Arc::clone(&release_b);
    let task = Handle::current().spawn(async move {
        let mut connection = h2::server::handshake(peer).await.unwrap();
        let (a_request, mut a_reply) = connection.accept().await.unwrap().unwrap();
        assert_eq!(a_request.method(), http::Method::POST);
        assert_eq!(a_request.headers()[http::header::CONTENT_LENGTH], "131076");
        let mut a = a_reply.send_response(Response::new(()), false).unwrap();
        a.send_data(Bytes::from_static(b"A headers and raw prefix"), false)
            .unwrap();
        let (b_request, mut b_reply) = connection.accept().await.unwrap().unwrap();
        let mut b = b_reply.send_response(Response::new(()), false).unwrap();
        // Keep A's genuine receive half and never consume/release upload DATA.
        // Its 65535-byte receive window cannot accept this segmented 128KiB+tail.
        let reason = server_until(&mut connection, poll_fn(|cx| a.poll_reset(cx)))
            .await
            .unwrap();
        assert_eq!(reason, h2::Reason::CANCEL);
        peer_reset_seen.notify_one();
        server_until(&mut connection, peer_release_b.notified()).await;
        b.send_data(Bytes::from_static(DATA), true).unwrap();
        drop((a_request, b_request, a, b));
        poll_fn(|cx| connection.poll_closed(cx)).await.unwrap();
    });
    let mut server = Server {
        task: Some(FileTask::new(task)),
        joined: false,
    };
    let io_exit = Arc::new(AtomicUsize::new(0));
    let a_exit = Arc::new(AtomicUsize::new(0));
    let b_exit = Arc::new(AtomicUsize::new(0));
    let payload_exit = Arc::new(AtomicUsize::new(0));
    let (mut root, submission) = root(2);
    let result = AssertUnwindSafe(async {
        let mut connect = Some(input(io, true, Arc::clone(&io_exit)));
        let token = submission.connect_in(&mut connect).finish().unwrap();
        let mut a = request(token, Arc::clone(&a_exit), Arc::clone(&payload_exit));
        let uri = a.route.original_uri().clone();
        a.request = Some(
            Request::builder()
                .method(http::Method::POST)
                .uri(uri)
                .body(Buffer::from(vec![
                    Bytes::from(vec![b'a'; 128 * 1024]),
                    Bytes::from_static(b"tail"),
                ]))
                .unwrap(),
        );
        let mut a = Some(a);
        let a_reply = submission.request_in(&mut a).finish().unwrap();
        let mut b = Some(request(
            token,
            Arc::clone(&b_exit),
            Arc::clone(&payload_exit),
        ));
        let b_reply = submission.request_in(&mut b).finish().unwrap();
        let a_response = timeout(Duration::from_secs(5), a_reply)
            .await
            .unwrap()
            .unwrap();
        let b_response = timeout(Duration::from_secs(5), b_reply)
            .await
            .unwrap()
            .unwrap();
        // B is already admitted and its headers genuinely received before A is cancelled.
        drop(a_response);
        timeout(Duration::from_secs(5), reset_seen.notified())
            .await
            .unwrap();
        timeout(
            Duration::from_secs(5),
            poll_fn(|cx| {
                match root.inspect(cx, |state| {
                    state
                        .requests
                        .iter()
                        .filter(|r| r.token == Some(token))
                        .count()
                        == 1
                        && a_exit.load(Ordering::SeqCst) == 1
                }) {
                    Poll::Ready(true) => Poll::Ready(()),
                    _ => Poll::Pending,
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(b_exit.load(Ordering::SeqCst), 0);
        assert_eq!(io_exit.load(Ordering::SeqCst), 0);
        assert!(
            poll_fn(
                |cx| root.inspect(cx, |state| state.ingress.rows.lock().unwrap().connections
                    [token.index()]
                .accepting)
            )
            .await
        );
        release_b.notify_one();
        let mut body = b_response.response.into_body();
        let mut actual = Vec::new();
        loop {
            let bytes = timeout(Duration::from_secs(5), body.read())
                .await
                .unwrap()
                .unwrap();
            if bytes.is_empty() {
                break;
            }
            actual.extend_from_slice(&bytes.to_bytes());
            assert!(actual.len() <= DATA.len());
        }
        assert_eq!(actual, DATA);
        drop(body);
    })
    .catch_unwind()
    .await;
    finish(&mut root, &mut server, result).await;
    assert!(root.drained && root.joined && server.joined);
    assert_eq!(a_exit.load(Ordering::SeqCst), 1);
    assert_eq!(b_exit.load(Ordering::SeqCst), 1);
    assert_eq!(io_exit.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn real_header_error_stays_primary_when_original_reply_notification_panics() {
    use std::sync::atomic::AtomicBool;
    use std::task::{Wake, Waker};

    #[derive(Debug)]
    struct ReplyNotificationPanic;
    struct PanicOnce(AtomicBool);
    impl Wake for PanicOnce {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.0.swap(false, Ordering::SeqCst) {
                panic_any(ReplyNotificationPanic);
            }
        }
    }

    let (io, peer) = tokio::io::duplex(65536);
    let seen = Arc::new(Notify::new());
    let mut server = pending_header_server(peer, Arc::clone(&seen));
    let io_exit = Arc::new(AtomicUsize::new(0));
    let source_exit = Arc::new(AtomicUsize::new(0));
    let (mut root, submission) = root(1);
    let mut ticket = None;
    let result = AssertUnwindSafe(async {
        // A test poller may take over this SAME state only after the original
        // service's actual join. Preserve that real cancellation in Root.
        root.driver.as_mut().unwrap().abort();
        let joined = timeout(Duration::from_secs(5), poll_fn(|cx|
            root.driver.as_mut().unwrap().poll_drain(cx))).await.unwrap();
        let error = joined.expect_err("the explicitly aborted original service exits");
        assert!(error.is_cancelled());
        root.joined = true;
        root.driver_failure = Some(error);

        let mut connect = Some(input(io, false, Arc::clone(&io_exit)));
        let token = submission.connect_in(&mut connect).finish().unwrap();
        let mut source = Some(request(token, Arc::clone(&source_exit), Arc::new(AtomicUsize::new(0))));
        ticket = Some(submission.request_in(&mut source).finish().unwrap());
        let mut seen = Box::pin(seen.notified());
        timeout(Duration::from_secs(5), poll_fn(|cx| {
            root.cell.as_ref().unwrap().with_state(|state| { let _ = state.poll_turn(cx); }).unwrap();
            seen.as_mut().poll(cx)
        })).await.unwrap();

        let waker = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
        assert!(Pin::new(ticket.as_mut().unwrap()).poll(&mut Context::from_waker(&waker)).is_pending());
        root.cell.as_ref().unwrap().with_state(|state| {
            let row = state.requests.iter().find(|r| r.token == Some(token)).unwrap();
            assert!(row.future.is_some() && row.input.is_some());
            // Actual original driver/IO destruction produces the genuine Hyper
            // header receiver error; no fabricated error or fake source future.
            drop(state.connections[token.index()].connection.take());
        }).unwrap();
        assert_eq!(io_exit.load(Ordering::SeqCst), 1);
        timeout(Duration::from_secs(5), poll_fn(|cx| {
            root.cell.as_ref().unwrap().with_state(|state| {
                let _ = state.poll_turn(cx);
                let row = state.requests.iter().find(|r| r.token == Some(token)).unwrap();
                if row.notification_panic.is_none() { return Poll::Pending; }
                assert!(matches!(state.request_primary_failures().next(),
                    Some((t, HttpRequestFailureRef::Operation(HttpProcessFailure::Response(RawHttpError::Hyper(_)))))
                    if t == token));
                assert!(row.notification_panic.as_deref().is_some_and(|p| p.is::<ReplyNotificationPanic>()));
                assert!(row.cleanup_panic.is_none());
                assert!(row.reply_published && row.future.is_none() && row.outcome.is_none());
                assert!(matches!(row.reply.as_ref().unwrap().slot.lock().unwrap().value.as_ref(),
                    Some(Err(HttpRequestFailure::SourceFailed))));
                Poll::Ready(())
            }).unwrap()
        })).await.unwrap();
        // The actual failed reply remains deliverable after its one wake panic.
        assert!(matches!(poll_fn(|cx| Pin::new(ticket.as_mut().unwrap()).poll(cx)).await,
            Err(HttpRequestFailure::SourceFailed)));
        let accepted = poll_fn(|cx| root.poll_dispose_failures(cx, token, |state| {
            assert!(matches!(state.request_primary_failures().next(),
                Some((_, HttpRequestFailureRef::Operation(HttpProcessFailure::Response(_))))));
            assert!(state.cleanup_panics().any(|(_, cause)| cause.is::<ReplyNotificationPanic>()));
            true
        })).await;
        assert!(accepted);
    }).catch_unwind().await;
    drop(ticket.take());
    let deadline = Instant::now() + Duration::from_secs(5);
    let root_outcome = timeout_at(deadline, poll_fn(|cx| root.poll_shutdown(cx))).await;
    server.task.as_mut().unwrap().abort();
    let server_outcome = timeout_at(
        deadline,
        poll_fn(|cx| server.task.as_mut().unwrap().poll_drain(cx)),
    )
    .await;
    server.joined = server_outcome.is_ok();
    let root_timeout = root_outcome.is_err();
    // Cancellation is expected only for the exact original service explicitly
    // aborted above, and only after Root proved its retained state drained.
    let root_join = root_outcome
        .ok()
        .and_then(Result::err)
        .filter(|error| !(root.drained && error.is_cancelled()));
    let server_timeout = server_outcome.is_err();
    let server_join = server_outcome
        .ok()
        .and_then(Result::err)
        .filter(|error| !error.is_cancelled());
    if root_timeout || root_join.is_some() || server_timeout || server_join.is_some() {
        panic_any(CaseFailure {
            primary: result.err(),
            root_timeout,
            root_join,
            server_timeout,
            server_join,
        });
    }
    if let Err(payload) = result {
        resume_unwind(payload);
    }
    assert!(root.joined && root.drained);
    assert_eq!(source_exit.load(Ordering::SeqCst), 1);
    assert_eq!(io_exit.load(Ordering::SeqCst), 1);
}
