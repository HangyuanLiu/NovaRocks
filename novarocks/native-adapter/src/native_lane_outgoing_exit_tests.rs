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
use crate::native_transport_admission::{AdmissionDimensions, FrontendOutgoingSnapshot};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot};
use tower::ServiceExt;

const FIXTURE_DEADLINE: Duration = Duration::from_secs(8);

type FixtureResult<T> = Result<T, String>;
type Frames = mpsc::Sender<Result<Bytes, std::io::Error>>;
type HeadReply = oneshot::Sender<hyper::http::Response<PeerBody>>;

fn small_frontend() -> NativeTransportAdmission {
    NativeTransportAdmission::with_parts(
        TransportRole::Frontend,
        AdmissionDimensions {
            data_positions: 2,
            control_positions: 1,
            data_handshakes: 1,
            control_handshakes: 1,
        },
        None,
    )
    .unwrap()
}

async fn bounded<F: Future>(future: F) -> FixtureResult<F::Output> {
    tokio::time::timeout(FIXTURE_DEADLINE, future)
        .await
        .map_err(|_| "outgoing fixture watchdog expired".to_owned())
}

fn request() -> hyper::http::Request<BoxBody> {
    hyper::http::Request::builder()
        .method("POST")
        .uri("/novarocks.NovaRocksGrpc/FetchTaskResult")
        .body(tonic::body::empty_body())
        .unwrap()
}

struct PeerBody(mpsc::Receiver<Result<Bytes, std::io::Error>>);

impl HttpBody for PeerBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|item| item.map(|item| item.map(Frame::data)))
    }
}

// Exactly one listener, one accepted TCP connection, and one original task.
// No detached per-connection task or unbounded JoinSet exists in this fixture.
struct OriginalH2Peer {
    address: SocketAddr,
    heads: mpsc::Receiver<HeadReply>,
    task: tokio::task::JoinHandle<FixtureResult<()>>,
}

impl OriginalH2Peer {
    async fn start() -> FixtureResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "peer bind failed".to_owned())?;
        let address = listener
            .local_addr()
            .map_err(|_| "peer address failed".to_owned())?;
        let (heads_tx, heads) = mpsc::channel(2);
        let task = tokio::spawn(async move {
            let (io, _) = listener
                .accept()
                .await
                .map_err(|_| "peer accept failed".to_owned())?;
            drop(listener);
            let service = hyper::service::service_fn(
                move |_request: hyper::http::Request<hyper::body::Incoming>| {
                    let heads_tx = heads_tx.clone();
                    async move {
                        let (reply, response) = oneshot::channel();
                        heads_tx.send(reply).await.map_err(|_| {
                            std::io::Error::from(std::io::ErrorKind::ConnectionAborted)
                        })?;
                        response.await.map_err(|_| {
                            std::io::Error::from(std::io::ErrorKind::ConnectionAborted)
                        })
                    }
                },
            );
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(hyper_util::rt::TokioIo::new(io), service)
                .await
                .map_err(|_| "peer connection failed".to_owned())
        });
        Ok(Self {
            address,
            heads,
            task,
        })
    }

    async fn channel(
        &self,
        admission: &NativeTransportAdmission,
    ) -> FixtureResult<NativeLaneChannel> {
        let native_endpoint = novarocks_types::NativeEndpoint::from_socket_addr(self.address);
        let connector = frontend_lane_connector(
            novarocks_native_trust::NativeEndpointConnector::plaintext(native_endpoint),
            admission.clone(),
            FrontendNativeLane::ResultData,
        );
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", self.address))
            .map_err(|_| "endpoint construction failed".to_owned())?;
        let channel =
            bounded(configure_native_endpoint(endpoint).connect_with_connector(connector))
                .await?
                .map_err(|_| "actual admitted H2 connect failed".to_owned())?;
        Ok(NativeLaneChannel::new(
            channel,
            NativeLane::ResultData,
            Some(admission),
        ))
    }

    async fn stop(self) -> FixtureResult<()> {
        let task = self.task;
        task.abort();
        // This fixture task performs only asynchronous H2 IO. Cleanup must
        // actually join it even after a scenario watchdog failure. The outer
        // test process watchdog is a failure boundary, never an exit receipt.
        match task.await {
            Ok(_) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(_) => Err("original peer task panicked".to_owned()),
        }
    }
}

async fn headers(
    lane: &mut NativeLaneChannel,
    peer: &mut OriginalH2Peer,
) -> FixtureResult<(hyper::http::Response<BoxBody>, Frames)> {
    bounded(lane.ready())
        .await?
        .map_err(|_| "lane ready failed".to_owned())?;
    let response = lane.call(request());
    let serve = async {
        let reply = peer
            .heads
            .recv()
            .await
            .ok_or_else(|| "peer head missing".to_owned())?;
        let (frames, receiver) = mpsc::channel(1);
        if reply
            .send(hyper::http::Response::new(PeerBody(receiver)))
            .is_err()
        {
            return Err("original response waiter exited".to_owned());
        }
        Ok(frames)
    };
    let (response, frames) = bounded(async { tokio::join!(response, serve) }).await?;
    Ok((
        response.map_err(|_| "original response failed".to_owned())?,
        frames?,
    ))
}

// Uses the same Core notification and original public IO positions. It never
// treats this IO-only predicate as a response-body exit or a library-task join.
async fn actual_io_exit(admission: &NativeTransportAdmission) -> FixtureResult<()> {
    let deadline = tokio::time::Instant::now() + FIXTURE_DEADLINE;
    loop {
        let changed = admission.frontend_outgoing_notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let snapshot = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        if snapshot.data_connections == 0
            && snapshot.control_connections == 0
            && snapshot.data_handshakes == 0
            && snapshot.control_handshakes == 0
        {
            return Ok(());
        }
        tokio::time::timeout_at(deadline, changed)
            .await
            .map_err(|_| "actual public IO exit not observed".to_owned())?;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outgoing_close_keeps_actual_io_until_drop_and_does_not_close_membership() {
    let admission = small_frontend();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let native_endpoint =
        novarocks_types::NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let connector = frontend_lane_connector(
        novarocks_native_trust::NativeEndpointConnector::plaintext(native_endpoint),
        admission.clone(),
        FrontendNativeLane::ResultData,
    );
    let uri: hyper::http::Uri = "http://original-outgoing.test".parse().unwrap();
    let io = bounded(connector.clone().oneshot(uri.clone()))
        .await
        .unwrap()
        .unwrap();
    let (peer, _) = bounded(listener.accept()).await.unwrap().unwrap();
    let membership = admission.try_accept(TransportClass::Membership).unwrap();
    admission.close_frontend_outgoing().unwrap();
    let held = admission.frontend_outgoing_snapshot().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(30);
    let timed_out = admission
        .wait_frontend_outgoing_until(deadline)
        .await
        .err()
        .map(|e| e.kind());
    let refused = bounded(connector.oneshot(uri))
        .await
        .unwrap()
        .err()
        .map(|e| e.kind());
    let no_new_socket = listener
        .poll_accept(&mut Context::from_waker(std::task::Waker::noop()))
        .is_pending();
    // Actual IO ownership is destroyed; there is no count write or test ack.
    drop(io);
    drop(peer);
    let late_same_deadline = admission
        .wait_frontend_outgoing_until(deadline)
        .await
        .err()
        .map(|e| e.kind());
    let retry = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    let still_held_membership = admission.available_positions(TransportClass::Membership)
        == admission.positions(TransportClass::Membership) - 1;
    let membership_after_close = admission.try_accept(TransportClass::Membership).is_ok();
    drop(membership);
    drop(listener);
    assert_eq!(held.data_connections, 1);
    assert_eq!(held.data_handshakes, 0);
    assert_eq!(held.requests, [0; 4]);
    assert_eq!(timed_out, Some(std::io::ErrorKind::TimedOut));
    assert_eq!(late_same_deadline, Some(std::io::ErrorKind::TimedOut));
    assert!(retry.is_ok());
    assert!(refused.is_some() && no_new_socket);
    assert!(still_held_membership && membership_after_close);
}

#[tokio::test(flavor = "current_thread")]
async fn an_already_expired_original_deadline_never_returns_empty_success() {
    let admission = small_frontend();
    admission.close_frontend_outgoing().unwrap();
    let original_deadline = tokio::time::Instant::now();
    let empty = admission.frontend_outgoing_snapshot().unwrap().is_drained();
    let result = admission
        .wait_frontend_outgoing_until(original_deadline)
        .await;
    assert!(empty);
    assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::TimedOut);
}

#[tokio::test(flavor = "current_thread")]
async fn closed_poll_ready_destroys_the_original_hidden_acquire_qualification() {
    // Unit test of the real PollSemaphore reservation state. No Native IO claim.
    let admission = small_frontend();
    let lazy = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let mut lane = NativeLaneChannel::new(lazy, NativeLane::ResultData, Some(&admission));
    let count = lane.stream_limit();
    let held = lane
        .gate
        .semaphore
        .clone()
        .acquire_many_owned(u32::try_from(count).unwrap())
        .await
        .unwrap();
    let was_pending = lane
        .poll_ready(&mut Context::from_waker(std::task::Waker::noop()))
        .is_pending();
    drop(held); // Assigns one qualification to the pending original acquire future.
    let hidden = lane.available_streams();
    admission.close_frontend_outgoing().unwrap();
    let rejected = matches!(
        lane.poll_ready(&mut Context::from_waker(std::task::Waker::noop())),
        Poll::Ready(Err(_))
    );
    let released = lane.available_streams();
    drop(lane);
    assert!(was_pending && rejected);
    assert_eq!(hidden, count - 1);
    assert_eq!(released, count);
}

#[tokio::test(flavor = "current_thread")]
async fn escaped_ready_channel_cannot_dispatch_after_the_original_role_close() {
    let admission = small_frontend();
    let mut peer = OriginalH2Peer::start().await.unwrap();
    let facts: FixtureResult<(bool, bool, FrontendOutgoingSnapshot)> = async {
        let mut lane = peer.channel(&admission).await?;
        bounded(lane.ready())
            .await?
            .map_err(|_| "original poll_ready failed".to_owned())?;
        admission
            .close_frontend_outgoing()
            .map_err(|_| "close failed".to_owned())?;
        let rejected = bounded(lane.call(request())).await?.is_err();
        let no_request = matches!(peer.heads.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        let snapshot = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        drop(lane);
        Ok((rejected, no_request, snapshot))
    }
    .await;
    let original_peer_join = peer.stop().await;
    let drained = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    assert!(original_peer_join.is_ok());
    assert!(drained.is_ok());
    let (rejected, no_request, snapshot) = facts.unwrap();
    assert!(rejected && no_request);
    assert_eq!(snapshot.requests, [0; 4]);
}

#[tokio::test(flavor = "current_thread")]
async fn real_headers_and_io_exit_do_not_retire_a_held_response_body() {
    let admission = small_frontend();
    let mut peer = Some(OriginalH2Peer::start().await.unwrap());
    let body_dropped = Arc::new(AtomicBool::new(false));
    let facts: FixtureResult<(
        FrontendOutgoingSnapshot,
        FrontendOutgoingSnapshot,
        FrontendOutgoingSnapshot,
    )> = async {
        let mut lane = peer.as_ref().unwrap().channel(&admission).await?;
        let (response, frames) = headers(&mut lane, peer.as_mut().unwrap()).await?;
        admission
            .close_frontend_outgoing()
            .map_err(|_| "close failed".to_owned())?;
        let at_headers = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        let body = response.into_body();
        drop(lane);
        // Keep the actual client body unpolled while the actual peer task and
        // its socket exit. Thus a queued body reset is not mistaken for EOF.
        let joined = peer.take().unwrap().stop().await;
        if joined.is_err() {
            return Err("original peer join failed".to_owned());
        }
        actual_io_exit(&admission).await?;
        let io_only = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        drop(frames);
        drop(body);
        body_dropped.store(true, Ordering::Release);
        let last = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        Ok((at_headers, io_only, last))
    }
    .await;
    // Preserve and actually join the original peer on setup/error exits too.
    let rescue_join = match peer.take() {
        Some(peer) => peer.stop().await,
        None => Ok(()),
    };
    // No assertion is made until the original body/IO ownership is gone.
    let drained = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    let (at_headers, io_only, last) = facts.unwrap();
    assert!(rescue_join.is_ok() && drained.is_ok() && body_dropped.load(Ordering::Acquire));
    assert_eq!(at_headers.requests, [1, 0, 0, 0]);
    assert_eq!(io_only.data_connections, 0);
    assert_eq!(io_only.requests, [1, 0, 0, 0]);
    assert!(last.is_drained());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_body_eof_and_reset_each_retire_the_original_call_before_io_exit() {
    for reset in [false, true] {
        let admission = small_frontend();
        let mut peer = OriginalH2Peer::start().await.unwrap();
        let facts: FixtureResult<(FrontendOutgoingSnapshot, FrontendOutgoingSnapshot)> = async {
            let mut lane = peer.channel(&admission).await?;
            let (response, frames) = headers(&mut lane, &mut peer).await?;
            admission
                .close_frontend_outgoing()
                .map_err(|_| "close failed".to_owned())?;
            let at_headers = admission
                .frontend_outgoing_snapshot()
                .map_err(|_| "snapshot failed".to_owned())?;
            let mut body = response.into_body();
            if reset {
                frames
                    .send(Err(std::io::ErrorKind::ConnectionReset.into()))
                    .await
                    .map_err(|_| "actual peer body was gone".to_owned())?;
            }
            drop(frames);
            let mut ended = false;
            for _ in 0..4 {
                match bounded(std::future::poll_fn(|cx| {
                    Pin::new(&mut body).poll_frame(cx)
                }))
                .await?
                {
                    None => {
                        ended = true;
                        break;
                    }
                    Some(Err(_)) if reset => {
                        ended = true;
                        break;
                    }
                    Some(Err(_)) => return Err("unexpected actual EOF error".to_owned()),
                    Some(Ok(frame)) if frame.data_ref().is_some_and(Bytes::is_empty) => {}
                    Some(Ok(_)) => return Err("unexpected actual peer frame".to_owned()),
                }
            }
            if !ended {
                return Err("actual body public exit missing".to_owned());
            }
            let at_body_exit = admission
                .frontend_outgoing_snapshot()
                .map_err(|_| "snapshot failed".to_owned())?;
            drop(body);
            drop(lane);
            Ok((at_headers, at_body_exit))
        }
        .await;
        let joined = peer.stop().await;
        let drained = admission
            .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
            .await;
        assert!(joined.is_ok() && drained.is_ok());
        let (at_headers, at_body_exit) = facts.unwrap();
        assert_eq!(at_headers.requests, [1, 0, 0, 0]);
        assert_eq!(at_body_exit.requests, [0; 4]);
        assert_eq!(at_body_exit.data_connections, 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_entered_original_response_future_survives_close_until_actual_waiter_drop() {
    let admission = small_frontend();
    let mut peer = OriginalH2Peer::start().await.unwrap();
    let mut original: Option<
        tokio::task::JoinHandle<Result<hyper::http::Response<BoxBody>, StdError>>,
    > = None;
    let mut reply: Option<HeadReply> = None;
    let facts: FixtureResult<(FrontendOutgoingSnapshot, FrontendOutgoingSnapshot)> = async {
        let mut lane = peer.channel(&admission).await?;
        bounded(lane.ready())
            .await?
            .map_err(|_| "original ready failed".to_owned())?;
        original = Some(tokio::spawn(lane.call(request())));
        reply = Some(
            bounded(peer.heads.recv())
                .await?
                .ok_or_else(|| "original request absent".to_owned())?,
        );
        admission
            .close_frontend_outgoing()
            .map_err(|_| "close failed".to_owned())?;
        let entered = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        original.as_ref().unwrap().abort();
        let joined = bounded(original.as_mut().unwrap()).await?;
        original.take();
        if !joined.is_err_and(|error| error.is_cancelled()) {
            return Err("original waiter did not cancel".to_owned());
        }
        let exited = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        drop(lane);
        Ok((entered, exited))
    }
    .await;
    // Rescue any still-owned original waiter before peer shutdown and assertions.
    let rescue_join: FixtureResult<bool> = if let Some(task) = original.take() {
        task.abort();
        let joined = task.await;
        Ok(joined.is_ok() || joined.is_err_and(|error| error.is_cancelled()))
    } else {
        Ok(true)
    };
    drop(reply.take());
    let joined = peer.stop().await;
    let drained = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    assert!(rescue_join == Ok(true) && joined.is_ok() && drained.is_ok());
    let (entered, exited) = facts.unwrap();
    assert_eq!(entered.requests, [1, 0, 0, 0]);
    assert_eq!(exited.requests, [0; 4]);
}
