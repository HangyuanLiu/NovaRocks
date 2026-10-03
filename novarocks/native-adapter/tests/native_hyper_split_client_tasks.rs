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

//! Actual split client futures and static type/layout facts, without funding claims.
//! A raw TCP peer exposes the real SETTINGS and HEADERS/DATA boundaries. Task
//! handles are joined; a timeout only diagnoses lack of fixture progress.

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use hyper::http::Request;
use hyper::rt::{Executor, SplitClientExecutor};
use hyper_util::rt::TokioIo;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, LocalSet};

const WATCHDOG: Duration = Duration::from_secs(5);
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
struct Probe;
fn allocation() {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        ALLOCATIONS.with(|v| v.set(v.get() + 1));
    }
}
// SAFETY: Forward unchanged to System. Thread-local counters contain no heap
// objects and exclude all protocol, runtime and fixture allocations.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        allocation();
        unsafe { System.realloc(p, layout, bytes) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
    }
}
fn no_allocation<T>(f: impl FnOnce() -> T) -> T {
    assert!(!TRACK.with(|v| v.replace(true)));
    ALLOCATIONS.with(|v| v.set(0));
    let tracking = Tracking;
    let value = f();
    drop(tracking);
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
    value
}
#[derive(Clone, Copy, Debug)]
struct Dispatch {
    name: &'static str,
    bound: usize,
}
#[derive(Clone, Default)]
struct Recording {
    calls: Arc<Mutex<Vec<Dispatch>>>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for Recording {
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
    }
    fn execute(&self, future: F) {
        let bound = tokio::runtime::Handle::task_allocation_capacity_bound::<F>().unwrap();
        self.calls.lock().unwrap().push(Dispatch {
            name: std::any::type_name::<F>(),
            bound,
        });
        // Spawn this F directly. This fixture neither erases it nor adds an
        // async adapter and does not install an original allocation owner.
        self.handles.lock().unwrap().push(tokio::spawn(future));
    }
}
impl Recording {
    fn calls(&self) -> Vec<Dispatch> {
        self.calls.lock().unwrap().clone()
    }
    async fn join(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.handles.lock().unwrap());
            if tasks.is_empty() {
                return;
            }
            for task in tasks {
                tokio::time::timeout(WATCHDOG, task)
                    .await
                    .expect("actual split task did not exit")
                    .unwrap();
            }
        }
    }
}
struct PendingBody {
    receiver: Option<oneshot::Receiver<Bytes>>,
}
impl Body for PendingBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(data) => {
                self.receiver = None;
                Poll::Ready(Some(Ok(Frame::data(data.expect("fixture body release")))))
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.receiver.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(if self.receiver.is_some() { 7 } else { 0 })
    }
}
fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut wire = Vec::with_capacity(9 + payload.len());
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
    wire
}
async fn read_frame(io: &mut TcpStream) -> (u8, u8, u32, Vec<u8>) {
    let mut header = [0; 9];
    io.read_exact(&mut header).await.unwrap();
    let bytes = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    assert!(bytes <= 32768, "bounded actual fixture frame");
    let mut payload = vec![0; bytes];
    io.read_exact(&mut payload).await.unwrap();
    (
        header[3],
        header[4],
        u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
        payload,
    )
}
fn setting(payload: &[u8], id: u16) -> Option<u32> {
    assert_eq!(payload.len() % 6, 0);
    payload
        .chunks_exact(6)
        .find(|p| u16::from_be_bytes([p[0], p[1]]) == id)
        .map(|p| u32::from_be_bytes(p[2..6].try_into().unwrap()))
}
async fn peer(
    expect_settings: bool,
) -> (std::net::SocketAddr, oneshot::Receiver<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (head, received) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut preface = [0; 24];
        io.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        let (kind, flags, stream, settings) = read_frame(&mut io).await;
        assert_eq!((kind, flags, stream), (4, 0, 0));
        if expect_settings {
            assert_eq!(setting(&settings, 5), Some(32768));
            assert_eq!(setting(&settings, 6), Some(4096));
            assert_eq!(setting(&settings, 1), Some(0));
        }
        io.write_all(&frame(4, 0, 0, &[])).await.unwrap();
        io.write_all(&frame(4, 1, 0, &[])).await.unwrap();
        let mut head = Some(head);
        let mut body = Vec::new();
        loop {
            let (kind, flags, stream, payload) = read_frame(&mut io).await;
            match (kind, stream) {
                (1, 1) => {
                    assert_ne!(flags & 4, 0, "small request completes its HEADERS block");
                    head.take().unwrap().send(()).unwrap();
                }
                (0, 1) => {
                    body.extend_from_slice(&payload);
                    if flags & 1 != 0 {
                        break;
                    }
                }
                (4, 0) if flags & 1 != 0 => (),
                (8, _) => assert_eq!(payload.len(), 4, "actual flow-control increment"),
                other => panic!("unexpected request-side actual frame: {other:?}"),
            }
        }
        assert_eq!(body, b"payload");
        // After ACKing the zero-table setting, the peer must emit the HPACK
        // dynamic-size update before the indexed static :status=200 field.
        let block: &[u8] = if expect_settings {
            &[0x20, 0x88]
        } else {
            &[0x88]
        };
        io.write_all(&frame(1, 5, 1, block)).await.unwrap();
        let mut trailing = Vec::new();
        io.read_to_end(&mut trailing).await.unwrap();
    });
    (address, received, task)
}
async fn join_peer(task: JoinHandle<()>) {
    tokio::time::timeout(WATCHDOG, task)
        .await
        .expect("actual peer did not observe IO exit")
        .unwrap();
}

#[test]
fn static_split_queries_and_adapter_construction_allocate_and_dispatch_nothing() {
    type Split = SplitClientExecutor<Recording, Recording>;
    let legacy = Recording::default();
    let typed = Recording::default();
    let bounds =
        no_allocation(Split::allocation_capacity_bounds::<PendingBody, TokioIo<TcpStream>>)
            .unwrap();
    assert!(bounds.connection > 0 && bounds.pipe > 0 && bounds.send > 0);
    let adapter = no_allocation(|| Split::new(legacy.clone(), typed.clone()));
    assert!(legacy.calls().is_empty() && typed.calls().is_empty());
    drop(adapter);
}

#[tokio::test]
async fn actual_pending_request_dispatches_three_concrete_futures_with_exact_static_bounds_and_preserved_settings()
 {
    type Split = SplitClientExecutor<Recording, Recording>;
    let legacy = Recording::default();
    let typed = Recording::default();
    let bounds = Split::allocation_capacity_bounds::<PendingBody, TokioIo<TcpStream>>().unwrap();
    let (address, head, peer) = peer(true).await;
    let io = TcpStream::connect(address).await.unwrap();
    let mut builder = hyper::client::conn::http2::Builder::new(legacy.clone());
    builder
        .max_frame_size(32768)
        .max_header_list_size(4096)
        .header_table_size(0);
    let builder = builder.with_executor(Split::new(legacy.clone(), typed.clone()));
    let (mut sender, connection) = builder
        .handshake::<_, PendingBody>(TokioIo::new(io))
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let (release, receiver) = oneshot::channel();
    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{address}/split"))
        .body(PendingBody {
            receiver: Some(receiver),
        })
        .unwrap();
    let response = sender.send_request(request);
    let mut response = Box::pin(response);
    tokio::time::timeout(WATCHDOG, async {
        tokio::select! {
            result = &mut response => panic!("pending actual request body cannot finish: {result:?}"),
            result = head => result.unwrap(),
        }
    }).await.unwrap();
    let calls = typed.calls();
    assert_eq!(
        calls.len(),
        3,
        "connection, pending body pipe and response send tasks"
    );
    for (name, expected) in [
        ("::ConnTask<", bounds.connection),
        ("::PipeMap<", bounds.pipe),
        ("::SendWhen<", bounds.send),
    ] {
        let matching: Vec<_> = calls
            .iter()
            .filter(|call| call.name.contains(name))
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "actual concrete dispatch {name}: {calls:?}"
        );
        assert_eq!(
            matching[0].bound, expected,
            "static query matches the actual F dispatched directly"
        );
    }
    assert!(
        calls
            .iter()
            .all(|call| !call.name.contains("H2ClientFuture<"))
    );
    assert!(
        legacy.calls().is_empty(),
        "no enum/ordinary dispatch fallback"
    );
    release.send(Bytes::from_static(b"payload")).unwrap();
    let response = tokio::time::timeout(WATCHDOG, response)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    drop(sender);
    tokio::time::timeout(WATCHDOG, connection)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    typed.join().await;
    legacy.join().await;
    join_peer(peer).await;
}

#[derive(Clone, Default)]
struct LocalExecutor(Rc<RefCell<Vec<JoinHandle<()>>>>);
impl<F: Future<Output = ()> + 'static> Executor<F> for LocalExecutor {
    fn execute(&self, future: F) {
        self.0.borrow_mut().push(tokio::task::spawn_local(future));
    }
}
impl LocalExecutor {
    async fn join(&self) {
        loop {
            let handles = std::mem::take(&mut *self.0.borrow_mut());
            if handles.is_empty() {
                break;
            }
            for handle in handles {
                tokio::time::timeout(WATCHDOG, handle)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
}
struct LocalBody {
    data: Option<Bytes>,
    state: Rc<Cell<usize>>,
}
impl Body for LocalBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.state.set(self.state.get() + 1);
        Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
    }
    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.data.as_ref().map_or(0, |data| data.len() as u64))
    }
}
#[tokio::test]
async fn ordinary_non_send_executor_and_rc_body_still_roundtrip_on_local_set() {
    LocalSet::new()
        .run_until(async {
            let (address, head, peer) = peer(false).await;
            let state = Rc::new(Cell::new(0));
            let executor = LocalExecutor::default();
            let io = TcpStream::connect(address).await.unwrap();
            // No Split adapter: both executor and concrete Body are genuinely !Send.
            let builder = hyper::client::conn::http2::Builder::new(executor.clone());
            let (mut sender, connection) = builder
                .handshake::<_, LocalBody>(TokioIo::new(io))
                .await
                .unwrap();
            let connection = tokio::task::spawn_local(connection);
            let request = Request::builder()
                .method("POST")
                .uri(format!("http://{address}/ordinary"))
                .body(LocalBody {
                    data: Some(Bytes::from_static(b"payload")),
                    state: state.clone(),
                })
                .unwrap();
            let response = tokio::time::timeout(WATCHDOG, sender.send_request(request))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), 200);
            tokio::time::timeout(WATCHDOG, head).await.unwrap().unwrap();
            assert!(state.get() > 0, "actual non-Send body was polled");
            drop(response);
            drop(sender);
            tokio::time::timeout(WATCHDOG, connection)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            executor.join().await;
            join_peer(peer).await;
        })
        .await;
}
