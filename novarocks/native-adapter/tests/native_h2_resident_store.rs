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

//! Real H2 resident-store admission and original capability lifetime.
//! Only the installed StreamStoreBuffer arrays/Core and its Bytes carrier are
//! prepaid here. Peer buffers, HeaderMap/HPACK, shared Inner, tasks and IO are
//! independent fixture allocations; no whole-connection bound is claimed.

use bytes::Bytes;
use h2::StreamStoreBuffer;
use hyper::http::{Request, Response};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::task::JoinHandle;

const WATCHDOG: Duration = Duration::from_secs(5);
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

struct Funding {
    buffer: StreamStoreBuffer,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
}
fn funded() -> Funding {
    let total = StreamStoreBuffer::allocation_capacity_bound(2, 4).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("original store pregrant must precede construction");
    };
    let owner = Bytes::from_owner_with_exit_guard(Bytes::new(), credit);
    let buffer = StreamStoreBuffer::new(2, 4, owner).unwrap();
    assert_eq!(buffer.max_resident_streams(), 2);
    assert_eq!(buffer.max_waiters(), 4);
    Funding {
        buffer,
        budget,
        total,
    }
}
fn held(budget: &Arc<ResultRetainedBudget>, total: usize) {
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn returned(budget: &Arc<ResultRetainedBudget>, total: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap() else {
        panic!("all original store owners must have physically exited");
    };
    drop(credit);
}
#[derive(Default)]
struct Signal(AtomicUsize);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn poll_ready(
    sender: &mut h2::client::SendRequest<Bytes>,
    signal: &Arc<Signal>,
) -> Poll<Result<(), h2::Error>> {
    let waker = Waker::from(signal.clone());
    sender.poll_ready(&mut Context::from_waker(&waker))
}
fn request() -> Request<()> {
    Request::builder()
        .uri("http://example.test/")
        .body(())
        .unwrap()
}
fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.len() + 9);
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    bytes.extend_from_slice(&[kind, flags]);
    bytes.extend_from_slice(&stream.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}
async fn read_frame(peer: &mut DuplexStream) -> (u8, u8, u32, Vec<u8>) {
    tokio::time::timeout(WATCHDOG, async {
        let mut header = [0; 9];
        peer.read_exact(&mut header).await.unwrap();
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let mut payload = vec![0; len];
        peer.read_exact(&mut payload).await.unwrap();
        (
            header[3],
            header[4],
            u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
            payload,
        )
    })
    .await
    .unwrap()
}
async fn expect_frame(peer: &mut DuplexStream, kind: u8, flags: u8, stream: u32) {
    loop {
        let (found, actual_flags, actual_stream, _) = read_frame(peer).await;
        if found == kind && actual_flags & flags == flags && actual_stream == stream {
            return;
        }
    }
}
async fn stop(task: JoinHandle<Result<(), h2::Error>>) {
    task.abort();
    let result = tokio::time::timeout(WATCHDOG, task).await.unwrap();
    assert!(result.is_ok() || result.unwrap_err().is_cancelled());
}
async fn client(
    buffer: &StreamStoreBuffer,
    advertised: Option<u32>,
) -> (
    h2::client::SendRequest<Bytes>,
    DuplexStream,
    JoinHandle<Result<(), h2::Error>>,
) {
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut builder = h2::client::Builder::new();
    builder.stream_store_buffer(buffer.clone());
    let (sender, connection) = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let task = tokio::spawn(connection);
    let mut preface = [0; 24];
    peer.read_exact(&mut preface).await.unwrap();
    assert_eq!(&preface, PREFACE);
    expect_frame(&mut peer, 4, 0, 0).await;
    let payload = advertised
        .map(|limit| {
            let mut p = vec![0, 3];
            p.extend_from_slice(&limit.to_be_bytes());
            p
        })
        .unwrap_or_default();
    peer.write_all(&frame(4, 0, 0, &payload)).await.unwrap();
    // A real ACK proves that the peer setting has applied, not merely arrived.
    expect_frame(&mut peer, 4, 1, 0).await;
    (sender, peer, task)
}
async fn response(
    sender: &mut h2::client::SendRequest<Bytes>,
    peer: &mut DuplexStream,
) -> h2::RecvStream {
    let (future, send) = sender.send_request(request(), true).unwrap();
    let id = u32::from(send.stream_id());
    drop(send);
    // Do not fabricate a response until the real request HEADERS are on wire.
    expect_frame(peer, 1, 4, id).await;
    peer.write_all(&frame(1, 5, id, &[0x88])).await.unwrap();
    let response = tokio::time::timeout(WATCHDOG, future)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.body().is_end_stream());
    response.into_body()
}

#[tokio::test]
async fn hard_two_survives_large_or_omitted_peer_limit_and_closed_aliases_wake_reuse() {
    for advertised in [Some(1000), None] {
        let funding = funded();
        let (mut sender, mut peer, task) = client(&funding.buffer, advertised).await;
        let first = response(&mut sender, &mut peer).await;
        let second = response(&mut sender, &mut peer).await;
        let signal = Arc::new(Signal::default());
        assert!(poll_ready(&mut sender, &signal).is_pending());
        assert!(sender.send_request(request(), true).is_err());
        let before = signal.0.load(Ordering::SeqCst);
        drop(first);
        assert!(
            signal.0.load(Ordering::SeqCst) > before,
            "actual released slot must wake readiness"
        );
        assert!(matches!(
            poll_ready(&mut sender, &signal),
            Poll::Ready(Ok(()))
        ));
        let replacement = response(&mut sender, &mut peer).await;
        assert!(poll_ready(&mut sender, &signal).is_pending());
        drop(second);
        drop(replacement);
        drop(sender);
        stop(task).await;
        drop(peer);
        let Funding {
            buffer,
            budget,
            total,
        } = funding;
        drop(buffer);
        returned(&budget, total);
    }
}

#[tokio::test]
async fn peer_limit_zero_cloned_senders_cannot_queue_beyond_original_two_slots() {
    let funding = funded();
    let (sender, peer, task) = client(&funding.buffer, Some(0)).await;
    let mut first = sender.clone();
    let mut second = sender.clone();
    let mut third = sender.clone();
    let signal = Arc::new(Signal::default());
    assert!(matches!(
        poll_ready(&mut first, &signal),
        Poll::Ready(Ok(()))
    ));
    let pending_first = first.send_request(request(), true).unwrap();
    assert!(matches!(
        poll_ready(&mut second, &signal),
        Poll::Ready(Ok(()))
    ));
    let pending_second = second.send_request(request(), true).unwrap();
    assert!(poll_ready(&mut third, &signal).is_pending());
    assert!(third.send_request(request(), true).is_err());
    drop(first);
    drop(second);
    drop(third);
    drop(sender);
    stop(task).await;
    drop(peer);
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    held(&budget, total);
    drop(pending_first);
    held(&budget, total);
    drop(pending_second);
    returned(&budget, total);
}

#[tokio::test]
async fn escaped_closed_recv_stream_holds_original_store_after_connection_task_exit() {
    let funding = funded();
    let (mut sender, mut peer, task) = client(&funding.buffer, Some(1000)).await;
    let alias = response(&mut sender, &mut peer).await;
    drop(sender);
    stop(task).await;
    drop(peer);
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    held(&budget, total);
    drop(alias);
    returned(&budget, total);
}

#[tokio::test]
async fn remote_third_stream_is_fail_closed_instead_of_waiting_for_local_aliases() {
    let funding = funded();
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut builder = h2::server::Builder::new();
    builder
        .max_concurrent_streams(1000)
        .stream_store_buffer(funding.buffer.clone());
    let mut wire = PREFACE.to_vec();
    wire.extend(frame(4, 0, 0, &[]));
    // Indexed GET, http, /, then indexed :authority with a literal value.
    let headers = [0x82, 0x86, 0x84, 0x01, 1, b'x'];
    for id in [1, 3] {
        wire.extend(frame(1, 5, id, &headers));
    }
    peer.write_all(&wire).await.unwrap();
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let mut bodies = Vec::new();
    for _ in 0..2 {
        let (request, mut respond) = tokio::time::timeout(WATCHDOG, connection.accept())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        bodies.push(request.into_body());
        drop(respond.send_response(Response::new(()), true).unwrap());
    }
    // Submit the third frame only after the first two have actually dispatched.
    // A coalesced three-frame read could otherwise fail before either accept.
    peer.write_all(&frame(1, 5, 5, &headers)).await.unwrap();
    // Resident refusal uses the same bounded REFUSED_STREAM wire path as
    // peer wire-concurrency refusal. The connection stays available, while
    // no third request may dispatch or wait for an existing alias to exit.
    {
        let waiting = connection.accept();
        tokio::pin!(waiting);
        tokio::select! {
            reset = async {
                loop {
                    let (kind, flags, id, payload) = read_frame(&mut peer).await;
                    if kind == 3 && id == 5 { break (flags, payload); }
                }
            } => {
                assert_eq!(reset.0, 0);
                assert_eq!(reset.1, 7u32.to_be_bytes());
            },
            unexpected = &mut waiting => panic!("resident exhaustion dispatched or closed the connection: {unexpected:?}"),
        }
    }
    drop(connection);
    drop(peer);
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    held(&budget, total);
    drop(bodies);
    returned(&budget, total);
}

#[derive(Default)]
struct IoCounts {
    reads: AtomicUsize,
    writes: AtomicUsize,
}
struct ReadyIo(Arc<IoCounts>);
impl AsyncRead for ReadyIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.reads.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
impl AsyncWrite for ReadyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.writes.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn reused_store_is_refused_for_both_peers_before_first_io() {
    let funding = funded();
    let (mut sender, mut peer, task) = client(&funding.buffer, Some(1000)).await;
    let alias = response(&mut sender, &mut peer).await;
    drop(sender);
    stop(task).await;
    drop(peer);
    for server in [false, true] {
        let counts = Arc::new(IoCounts::default());
        let result = if server {
            let mut builder = h2::server::Builder::new();
            builder.stream_store_buffer(funding.buffer.clone());
            tokio::time::timeout(
                WATCHDOG,
                builder.handshake::<_, Bytes>(ReadyIo(counts.clone())),
            )
            .await
            .unwrap()
            .map(|_| ())
        } else {
            let mut builder = h2::client::Builder::new();
            builder.stream_store_buffer(funding.buffer.clone());
            tokio::time::timeout(
                WATCHDOG,
                builder.handshake::<_, Bytes>(ReadyIo(counts.clone())),
            )
            .await
            .unwrap()
            .map(|_| ())
        };
        assert!(result.is_err());
        assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
        assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
    }
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    held(&budget, total);
    drop(alias);
    returned(&budget, total);
}

#[tokio::test]
async fn canceled_handles_and_repolls_release_original_waiter_positions() {
    let funding = funded();
    let (mut sender, mut peer, task) = client(&funding.buffer, None).await;
    let first = response(&mut sender, &mut peer).await;
    let second = response(&mut sender, &mut peer).await;
    for _ in 0..16 {
        let mut canceled = sender.clone();
        let signal = Arc::new(Signal::default());
        assert!(poll_ready(&mut canceled, &signal).is_pending());
        drop(canceled);
        assert_eq!(
            Arc::strong_count(&signal),
            1,
            "cancel must retire its waker"
        );
    }
    for _ in 0..16 {
        let signal = Arc::new(Signal::default());
        assert!(poll_ready(&mut sender, &signal).is_pending());
    }
    drop(first);
    assert!(matches!(
        poll_ready(&mut sender, &Arc::new(Signal::default())),
        Poll::Ready(Ok(()))
    ));
    drop(second);
    drop(sender);
    stop(task).await;
    drop(peer);
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    returned(&budget, total);
}

struct ReentrantStreamWake(std::sync::Mutex<Option<h2::RecvStream>>);
impl Wake for ReentrantStreamWake {
    fn wake(self: Arc<Self>) {
        // The actual second StreamRef destructor locks the same Inner. This
        // callback must run only after the first destructor unlocks it.
        let actual_stream = self.0.lock().unwrap().take();
        let (done, completed) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            drop(actual_stream);
            let _ = done.send(());
        });
        // A separate thread makes a wrongly held lock fail finitely instead
        // of leaving a permanently deadlocked fixture on a regression.
        completed
            .recv_timeout(WATCHDOG)
            .expect("callback retained the connection lock");
        thread.join().unwrap();
    }
}

#[tokio::test]
async fn resident_notification_can_drop_another_actual_stream_without_the_connection_lock() {
    let funding = funded();
    let (mut sender, mut peer, task) = client(&funding.buffer, None).await;
    let first = response(&mut sender, &mut peer).await;
    let second = response(&mut sender, &mut peer).await;
    let waker = Waker::from(Arc::new(ReentrantStreamWake(std::sync::Mutex::new(Some(
        second,
    )))));
    assert!(
        sender
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    let (done, completed) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        drop(first);
        done.send(()).unwrap();
    });
    tokio::time::timeout(WATCHDOG, completed)
        .await
        .expect("resident callback reentered a held connection lock")
        .unwrap();
    thread.join().unwrap();
    assert!(matches!(
        poll_ready(&mut sender, &Arc::new(Signal::default())),
        Poll::Ready(Ok(()))
    ));
    drop(sender);
    stop(task).await;
    drop(peer);
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    returned(&budget, total);
}

#[tokio::test]
async fn original_waiter_limit_is_finite_and_connection_error_wakes_retained_alias_waiters() {
    let funding = funded();
    let (mut sender, mut peer, task) = client(&funding.buffer, None).await;
    let first = response(&mut sender, &mut peer).await;
    let second = response(&mut sender, &mut peer).await;
    let signal = Arc::new(Signal::default());
    let mut waiters = Vec::new();
    for _ in 0..funding.buffer.max_waiters() {
        let mut waiting = sender.clone();
        assert!(poll_ready(&mut waiting, &signal).is_pending());
        waiters.push(waiting);
    }
    assert!(matches!(
        poll_ready(&mut sender, &signal),
        Poll::Ready(Err(_))
    ));
    drop(peer);
    tokio::time::timeout(WATCHDOG, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(signal.0.load(Ordering::SeqCst) >= waiters.len());
    for waiting in &mut waiters {
        assert!(matches!(poll_ready(waiting, &signal), Poll::Ready(Err(_))));
    }
    // Terminal wire state wakes waiters but does not free the original arrays
    // while the actual closed response aliases are retained.
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    drop(sender);
    drop(waiters);
    held(&budget, total);
    drop(first);
    held(&budget, total);
    drop(second);
    returned(&budget, total);
}

#[tokio::test]
async fn reserved_local_push_claims_actual_resident_position_before_stream_construction() {
    let funding = funded();
    let (io, mut peer) = tokio::io::duplex(65536);
    let mut builder = h2::server::Builder::new();
    builder.stream_store_buffer(funding.buffer.clone());
    let mut wire = PREFACE.to_vec();
    wire.extend(frame(4, 0, 0, &[]));
    wire.extend(frame(1, 5, 1, &[0x82, 0x86, 0x84, 0x01, 1, b'x']));
    peer.write_all(&wire).await.unwrap();
    let mut connection = builder.handshake::<_, Bytes>(io).await.unwrap();
    drop(builder);
    let (received, mut response) = tokio::time::timeout(WATCHDOG, connection.accept())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let malformed = Request::builder()
        .method("POST")
        .uri("http://example.test/")
        .body(())
        .unwrap();
    assert!(response.push_request(malformed).is_err());
    let malformed = Request::builder()
        .uri("http://example.test/")
        .header("content-length", "invalid")
        .body(())
        .unwrap();
    assert!(response.push_request(malformed).is_err());
    let first_push = response.push_request(request()).unwrap();
    // Reserved local push does not consume wire-concurrency count; it must
    // nevertheless retain the second original resident slot.
    assert!(response.push_request(request()).is_err());
    drop(first_push);
    drop(response);
    drop(connection);
    drop(peer);
    let Funding {
        buffer,
        budget,
        total,
    } = funding;
    drop(buffer);
    held(&budget, total);
    drop(received);
    returned(&budget, total);
}

#[tokio::test]
async fn legacy_none_ready_without_pending_does_not_clone_the_user_waker() {
    use std::task::{RawWaker, RawWakerVTable};
    static CLONES: AtomicUsize = AtomicUsize::new(0);
    unsafe fn clone(_: *const ()) -> RawWaker {
        CLONES.fetch_add(1, Ordering::SeqCst);
        RawWaker::new(std::ptr::null(), &TABLE)
    }
    unsafe fn noop(_: *const ()) {}
    static TABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: the vtable uses no borrowed data or owned pointer; every callback
    // is thread-safe and the immutable table/static counter outlive all clones.
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &TABLE)) };
    let (io, _peer) = tokio::io::duplex(65536);
    let (mut sender, connection) = h2::client::Builder::new()
        .handshake::<_, Bytes>(io)
        .await
        .unwrap();
    CLONES.store(0, Ordering::SeqCst);
    assert!(matches!(
        sender.poll_ready(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(CLONES.load(Ordering::SeqCst), 0);
    drop(sender);
    drop(connection);
}
