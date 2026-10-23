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

use std::any::Any;
use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, panic_any, resume_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::FileTask;
use futures::FutureExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use opendal::raw::oio::ReadStream;

const DATA: &[u8] = b"actual raw response";
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("response fixture deadline")
}
struct CountDrop(Arc<AtomicUsize>);
impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Handoff {
    grants: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl PayloadHandoff for Handoff {
    type Guard = CountDrop;
    fn poll_handoff(&mut self, _: &mut Context<'_>, _: &Bytes) -> Poll<FileResult<CountDrop>> {
        // This finite test envelope transfers one actual received frame.
        self.grants.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(CountDrop(Arc::clone(&self.drops))))
    }
}

// Only this one-request fixture uses a Vec. Every actual Hyper child is joined
// after both outer connection drivers exit; this is not a production executor.
#[derive(Clone)]
struct Joined(Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>);
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for Joined {
    fn execute(&self, future: F) {
        let task = tokio::spawn(future);
        self.0.lock().unwrap().push(task);
    }
}
enum Sender {
    H1(hyper::client::conn::http1::SendRequest<Full<Bytes>>),
    H2(h2::client::SendRequest<Bytes>),
}
struct Fixture {
    sender: Option<Sender>,
    client: Option<FileTask>,
    server: FileTask,
    client_drained: bool,
    server_drained: bool,
    pending_children: Vec<tokio::task::JoinHandle<()>>,
    cleanup_errors: Vec<tokio::task::JoinError>,
    children: Joined,
    client_exit: Arc<AtomicUsize>,
}
impl Fixture {
    async fn incoming(h2: bool) -> (Self, RawBody) {
        let (client_io, server_io) = tokio::io::duplex(65536);
        let children = Joined(Arc::new(Mutex::new(Vec::new())));
        let server_exec = children.clone();
        let server = FileTask::new(tokio::spawn(async move {
            let service = hyper::service::service_fn(|_: hyper::Request<Incoming>| async {
                Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::from_static(DATA))))
            });
            if h2 {
                let _ = hyper::server::conn::http2::Builder::new(server_exec)
                    .serve_connection(TokioIo::new(server_io), service)
                    .await;
            } else {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(server_io), service)
                    .await;
            }
        }));
        let client_exit = Arc::new(AtomicUsize::new(0));
        // Stable cleanup custody exists before either fallible handshake/send.
        let mut fixture = Self {
            sender: None,
            client: None,
            server,
            children,
            client_exit,
            client_drained: false,
            server_drained: false,
            pending_children: Vec::new(),
            cleanup_errors: Vec::new(),
        };
        let setup = AssertUnwindSafe(async {
            let exit = CountDrop(Arc::clone(&fixture.client_exit));
            let (mut sender, client) = if h2 {
                let (sender, connection) = bounded(h2::client::handshake(client_io)).await.unwrap();
                (
                    Sender::H2(sender),
                    FileTask::new(tokio::spawn(async move {
                        let _exit = exit;
                        let _ = connection.await;
                    })),
                )
            } else {
                let (sender, connection) = bounded(hyper::client::conn::http1::handshake::<
                    _,
                    Full<Bytes>,
                >(TokioIo::new(client_io)))
                .await
                .unwrap();
                (
                    Sender::H1(sender),
                    FileTask::new(tokio::spawn(async move {
                        let _exit = exit;
                        let _ = connection.await;
                    })),
                )
            };
            fixture.client = Some(client);
            let request = hyper::Request::builder()
                .uri("http://fixture.invalid/object")
                .header("host", "fixture.invalid")
                .body(Full::new(Bytes::new()))
                .unwrap();
            let response = match &mut sender {
                Sender::H1(sender) => bounded(sender.send_request(request))
                    .await
                    .unwrap()
                    .map(RawBody::from),
                Sender::H2(sender) => {
                    bounded(poll_fn(|cx| sender.poll_ready(cx))).await.unwrap();
                    let (response, stream) =
                        sender.send_request(request.map(|_| ()), true).unwrap();
                    let response = bounded(response).await.unwrap().map(RawBody::from);
                    drop(stream); // Empty upload actually ended on request HEADERS.
                    response
                }
            };
            fixture.sender = Some(sender);
            response.into_body()
        })
        .catch_unwind()
        .await;
        match setup {
            Ok(raw) => (fixture, raw),
            Err(payload) => {
                let cleanup = fixture.close().await;
                fixture.finish_case(Err(payload), cleanup);
                unreachable!("failed fixture setup must preserve its failure");
            }
        }
    }

    fn observe_join(&mut self, outcome: Result<(), tokio::task::JoinError>) {
        if let Err(error) = outcome {
            // This author explicitly aborts every remaining task. Only that
            // cancellation is expected; retain the original panic JoinError.
            if !error.is_cancelled() {
                self.cleanup_errors.push(error);
            }
        }
    }

    fn receive_children(&mut self) -> usize {
        let mut published = self.children.0.lock().unwrap();
        let count = published.len();
        self.pending_children.append(&mut published);
        count
    }

    fn poll_cleanup(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.receive_children();
        if let Some(client) = self.client.as_mut() {
            client.abort();
        }
        self.server.abort();
        for child in &self.pending_children {
            child.abort();
        }
        if !self.client_drained {
            let outcome = match self.client.as_mut() {
                Some(client) => client.poll_drain(cx),
                None => Poll::Ready(Ok(())),
            };
            if let Poll::Ready(outcome) = outcome {
                self.client_drained = true;
                self.observe_join(outcome);
            }
        }
        if !self.server_drained {
            if let Poll::Ready(outcome) = self.server.poll_drain(cx) {
                self.server_drained = true;
                self.observe_join(outcome);
            }
        }
        let mut index = 0;
        while index < self.pending_children.len() {
            match Pin::new(&mut self.pending_children[index]).poll(cx) {
                Poll::Pending => index += 1,
                Poll::Ready(outcome) => {
                    // Only an actually joined handle leaves original storage.
                    drop(self.pending_children.swap_remove(index));
                    self.observe_join(outcome);
                }
            }
        }
        // A task may publish its own child before its real join. The final
        // frontier is checked after every existing producer was polled.
        let received = self.receive_children();
        if self.client_drained && self.server_drained && self.pending_children.is_empty() {
            Poll::Ready(())
        } else {
            if received != 0 {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }

    async fn close(&mut self) -> CleanupStatus {
        drop(self.sender.take());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        // timeout only borrows this Fixture. Pending handles stay here on
        // expiry, rather than disappearing with a temporary await future.
        let complete = tokio::time::timeout_at(deadline, poll_fn(|cx| self.poll_cleanup(cx)))
            .await
            .is_ok();
        CleanupStatus {
            complete,
            deadline_elapsed: !complete,
        }
    }

    fn finish_case(mut self, primary: Result<(), Box<dyn Any + Send>>, status: CleanupStatus) {
        if status.complete && self.cleanup_errors.is_empty() {
            if let Err(payload) = primary {
                resume_unwind(payload);
            }
            return;
        }
        // Preserve primary and every original secondary payload together. No
        // formatting or success projection destroys an observed JoinError.
        let joins = std::mem::take(&mut self.cleanup_errors);
        let unfinished = (!status.complete).then(|| std::mem::ManuallyDrop::new(Box::new(self)));
        panic_any(CleanupFailure {
            primary: primary.err(),
            joins,
            _unfinished: unfinished,
            deadline_elapsed: status.deadline_elapsed,
        });
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(client) = self.client.as_mut() {
            client.abort();
        }
        self.server.abort();
        for child in &self.pending_children {
            child.abort();
        }
        let children = std::mem::take(&mut *self.children.0.lock().unwrap());
        for child in children {
            child.abort();
        }
        // Error/unwind fallback only requests stop. Only a complete cleanup
        // result proves all original joins were observed, including failures.
    }
}

#[derive(Clone, Copy)]
struct CleanupStatus {
    complete: bool,
    deadline_elapsed: bool,
}
struct CleanupFailure {
    primary: Option<Box<dyn Any + Send>>,
    joins: Vec<tokio::task::JoinError>,
    deadline_elapsed: bool,
    // A cleanup deadline is not a join receipt. Keep the original handles in
    // this finite failure packet until process exit instead of detaching them.
    _unfinished: Option<std::mem::ManuallyDrop<Box<Fixture>>>,
}
impl std::fmt::Debug for CleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CleanupFailure")
            .field("has_original_primary", &self.primary.is_some())
            .field("original_join_failures", &self.joins.len())
            .field("deadline_elapsed", &self.deadline_elapsed)
            .finish()
    }
}

async fn next(source: &mut ResponseSource<CountDrop, Handoff>, body: &mut HttpBody) -> Buffer {
    let read = body.read();
    tokio::pin!(read);
    bounded(poll_fn(|cx| {
        let action = source.poll_progress(cx);
        assert!(!matches!(
            action,
            Poll::Ready(ResponseAction::NeedsQuiesce(_))
        ));
        if matches!(action, Poll::Ready(ResponseAction::Progress)) {
            // The real request-root driver also reschedules finite progress.
            cx.waker().wake_by_ref();
        }
        read.as_mut().poll(cx)
    }))
    .await
    .unwrap()
}

#[tokio::test]
async fn real_h1_h2_ticket_drop_keeps_raw_source_and_unreceived_payload() {
    for h2 in [false, true] {
        let (mut fixture, raw) = Fixture::incoming(h2).await;
        let source_drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let grants = Arc::new(AtomicUsize::new(0));
        let (mut source, body) = ResponseSource::new(
            raw,
            CountDrop(Arc::clone(&source_drops)),
            Handoff {
                grants,
                drops: Arc::clone(&payload_drops),
            },
            None,
        );
        let result = AssertUnwindSafe(async {
            bounded(poll_fn(|cx| {
                let action = source.poll_progress(cx);
                assert!(!matches!(
                    action,
                    Poll::Ready(ResponseAction::NeedsQuiesce(_))
                ));
                if source.shared.slot().data.is_some() {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }))
            .await;
            drop(body);
            assert_eq!(source_drops.load(Ordering::SeqCst), 0);
            assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
            assert!(source.raw.is_some());
            assert!(source.shared.slot().data.is_some());
            assert_eq!(
                poll_fn(|cx| source.poll_progress(cx)).await,
                ResponseAction::NeedsQuiesce(StopReason::Abandoned)
            );
        })
        .catch_unwind()
        .await;
        let cleanup = fixture.close().await;
        if cleanup.complete {
            source.dispose_after_connection_exit();
        }
        fixture.finish_case(result, cleanup);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn real_h1_h2_eos_releases_raw_source_before_idle_connection_and_last_payload_alias() {
    for h2 in [false, true] {
        let (mut fixture, raw) = Fixture::incoming(h2).await;
        let source_drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let grants = Arc::new(AtomicUsize::new(0));
        let (mut source, mut body) = ResponseSource::new(
            raw,
            CountDrop(Arc::clone(&source_drops)),
            Handoff {
                grants: Arc::clone(&grants),
                drops: Arc::clone(&payload_drops),
            },
            None,
        );
        let result = AssertUnwindSafe(async {
            let first = next(&mut source, &mut body).await;
            assert!(!first.is_empty());
            let bytes = first.to_bytes();
            drop(first);
            let clone = bytes.clone();
            let slice = bytes.slice(..1);
            let mut content = bytes.to_vec(); // Only finite expected fixture bytes.
            drop(bytes);
            loop {
                let part = next(&mut source, &mut body).await;
                if part.is_empty() {
                    break;
                }
                content.extend_from_slice(&part.to_bytes());
                assert!(content.len() <= DATA.len());
            }
            assert_eq!(content, DATA);
            assert!(source.complete && source.raw.is_none());
            assert!(!source.view_exited());
            drop(body);
            assert!(source.view_exited());
            assert_eq!(source_drops.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.client_exit.load(Ordering::SeqCst), 0);
            assert!(!fixture.client.as_ref().unwrap().is_finished());
            let expected = grants.load(Ordering::SeqCst);
            assert_eq!(payload_drops.load(Ordering::SeqCst), expected - 1);
            drop(clone);
            assert_eq!(payload_drops.load(Ordering::SeqCst), expected - 1);
            drop(slice);
            assert_eq!(payload_drops.load(Ordering::SeqCst), expected);
        })
        .catch_unwind()
        .await;
        let cleanup = fixture.close().await;
        if cleanup.complete {
            source.dispose_after_connection_exit();
        }
        fixture.finish_case(result, cleanup);
    }
}
