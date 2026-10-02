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

//! Actual System requests and original owner exits for fixed stream backing.
//! The measured scope is the two typed arrays, Core/Arc and original carrier.
//! Stream payloads, outer shared state, sockets and executor tasks are separate.

use bytes::Bytes;
use h2::StreamStoreBuffer;
use hyper::body::{Body, Frame};
use hyper::http::{Request, Response, Uri};
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::{Endpoint, Http2ConnectionConfig};

const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct Allocation {
    pointer: usize,
    family: u8,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    family: 0,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static FAMILY: Cell<u8> = const { Cell::new(0) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
    static RECORDS: RefCell<[Allocation; 128]> = const { RefCell::new([EMPTY; 128]) };
}
fn allocated(pointer: *mut u8, size: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        CALLS.with(|v| v.set(v.get() + 1));
        REQUESTED.with(|v| v.set(v.get() + size));
        let family = FAMILY.with(Cell::get);
        if family != 0 {
            RECORDS.with(|records| {
                let mut records = records.borrow_mut();
                let slot = records.iter_mut().find(|r| r.pointer == 0).unwrap();
                *slot = Allocation {
                    pointer: pointer as usize,
                    family,
                };
            });
        }
    }
}
fn freed(pointer: *mut u8) {
    let _ = RECORDS.try_with(|records| {
        if let Some(record) = records
            .borrow_mut()
            .iter_mut()
            .find(|r| r.pointer == pointer as usize)
        {
            *record = EMPTY;
        }
    });
}
struct Probe;
// SAFETY: Allocation operations delegate unchanged to System. Fixed TLS records
// neither allocate nor dereference the recorded pointers.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, size) };
        freed(pointer);
        allocated(next, size);
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        freed(pointer);
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
        FAMILY.with(|v| v.set(0));
    }
}
fn measure<T>(family: u8, operation: impl FnOnce() -> T) -> (T, usize, usize) {
    assert!(!TRACK.with(|v| v.replace(true)));
    FAMILY.with(|v| v.set(family));
    CALLS.with(|v| v.set(0));
    REQUESTED.with(|v| v.set(0));
    let tracking = Tracking;
    let result = operation();
    drop(tracking);
    (result, CALLS.with(Cell::get), REQUESTED.with(Cell::get))
}
struct PhysicalExit {
    family: u8,
    _credit: ResultWriteCredit,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        if self.family != 0 {
            assert!(
                RECORDS.with(|records| records
                    .borrow()
                    .iter()
                    .all(|r| r.pointer == 0 || r.family != self.family)),
                "original fixed pool/Core/carrier backing must be physically freed before credit"
            );
        }
    }
}
fn grant(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("complete original family pregrant");
    };
    credit
}
fn owner(budget: &Arc<ResultRetainedBudget>, bytes: usize, family: u8) -> Bytes {
    let credit = grant(budget, bytes);
    // The carrier has its own complete pregrant, in addition to the fixed pool.
    let (owner, _, requested) = measure(family, || {
        Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            PhysicalExit {
                family,
                _credit: credit,
            },
        )
    });
    assert!(requested <= carrier());
    owner
}
fn carrier() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, PhysicalExit>()
}
fn funded(family: u8) -> (StreamStoreBuffer, Arc<ResultRetainedBudget>, usize) {
    let bound = StreamStoreBuffer::allocation_capacity_bound(128, 128).unwrap();
    let total = bound + carrier();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let original = owner(&budget, total, family);
    let (buffer, calls, requested) = measure(family, || {
        StreamStoreBuffer::new(128, 128, original).unwrap()
    });
    assert_eq!(
        calls, 3,
        "exact Stream slots, Waker slots and Core Arc requests"
    );
    assert_eq!(
        requested, bound,
        "public bound must cover actual requested layouts exactly"
    );
    (buffer, budget, total)
}
fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
#[derive(Default)]
struct IoCounts {
    reads: AtomicUsize,
    writes: AtomicUsize,
    flushes: AtomicUsize,
    exits: AtomicUsize,
}
impl IoCounts {
    fn assert_no_io(&self) {
        assert_eq!(self.reads.load(Ordering::SeqCst), 0);
        assert_eq!(self.writes.load(Ordering::SeqCst), 0);
        assert_eq!(self.flushes.load(Ordering::SeqCst), 0);
    }
}
struct CountIo(Arc<IoCounts>);
impl AsyncRead for CountIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.reads.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
impl AsyncWrite for CountIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.writes.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.flushes.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl Drop for CountIo {
    fn drop(&mut self) {
        self.0.exits.fetch_add(1, Ordering::SeqCst);
    }
}
struct EmptyBody;
impl Body for EmptyBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(None)
    }
    fn is_end_stream(&self) -> bool {
        true
    }
}
fn bind_without_io(buffer: &StreamStoreBuffer) {
    let counts = Arc::new(IoCounts::default());
    let mut builder = h2::server::Builder::new();
    builder.stream_store_buffer(buffer.clone());
    // The actual H2 constructor binds storage before its future is polled.
    let handshake = builder.handshake::<_, Bytes>(CountIo(counts.clone()));
    counts.assert_no_io();
    drop(handshake);
    counts.assert_no_io();
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn exact_constructor_backing_exits_before_original_credit_after_last_future_and_alias() {
    let (buffer, budget, total) = funded(1);
    assert_eq!(buffer.max_resident_streams(), 128);
    assert_eq!(buffer.max_waiters(), 128);
    let alias = buffer.clone();
    let counts = Arc::new(IoCounts::default());
    let mut builder = h2::server::Builder::new();
    builder.stream_store_buffer(buffer.clone());
    let handshake = builder.handshake::<_, Bytes>(CountIo(counts.clone()));
    drop(builder);
    drop(buffer);
    held(&budget, total);
    drop(alias);
    // Only the actual unpolled handshake and its fixed storage still own Core.
    held(&budget, total);
    counts.assert_no_io();
    drop(handshake);
    counts.assert_no_io();
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    assert!(RECORDS.with(|records| records.borrow().iter().all(|r| r.family != 1)));
    drop(grant(&budget, total));
}

#[tokio::test]
async fn cloned_hyper_client_forwards_once_bound_stream_storage_before_preface() {
    let (buffer, budget, total) = funded(0);
    bind_without_io(&buffer);
    let counts = Arc::new(IoCounts::default());
    let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
    builder.stream_store_buffer(buffer.clone());
    let cloned = builder.clone();
    drop(builder);
    let result = tokio::time::timeout(
        WATCHDOG,
        cloned.handshake::<_, EmptyBody>(TokioIo::new(CountIo(counts.clone()))),
    )
    .await
    .expect("Hyper client failed to refuse reused stream storage");
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("Hyper client omitted stream buffer forwarding"),
    };
    assert!(format!("{error:?}").contains("already bound"));
    counts.assert_no_io();
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    held(&budget, total);
    drop(error);
    drop(cloned);
    drop(buffer);
    drop(grant(&budget, total));
}

#[tokio::test]
async fn cloned_hyper_server_forwards_once_bound_stream_storage_before_settings() {
    let (buffer, budget, total) = funded(0);
    bind_without_io(&buffer);
    let counts = Arc::new(IoCounts::default());
    let service_calls = Arc::new(AtomicUsize::new(0));
    let calls = service_calls.clone();
    let service = hyper::service::service_fn(move |_: Request<hyper::body::Incoming>| {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Ok::<_, Infallible>(Response::new(EmptyBody)) }
    });
    let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    builder.stream_store_buffer(buffer.clone());
    let cloned = builder.clone();
    drop(builder);
    let connection = cloned.serve_connection(TokioIo::new(CountIo(counts.clone())), service);
    let error = tokio::time::timeout(WATCHDOG, connection)
        .await
        .expect("Hyper server failed to refuse reused stream storage")
        .unwrap_err();
    assert!(format!("{error:?}").contains("already bound"));
    counts.assert_no_io();
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    assert_eq!(service_calls.load(Ordering::SeqCst), 0);
    held(&budget, total);
    drop(error);
    drop(cloned);
    drop(buffer);
    drop(grant(&budget, total));
}

#[tokio::test]
async fn actual_tonic_factory_forwards_once_bound_stream_storage_before_first_io() {
    let (buffer, budget, total) = funded(0);
    bind_without_io(&buffer);
    let shared = buffer.clone();
    let endpoint = Endpoint::from_static("http://localhost").http2_connection_factory(move || {
        Ok::<_, io::Error>(Http2ConnectionConfig {
            stream_store_buffer: Some(shared.clone()),
            ..Default::default()
        })
    });
    let counts = Arc::new(IoCounts::default());
    let connector_calls = Arc::new(AtomicUsize::new(0));
    let socket = counts.clone();
    let calls = connector_calls.clone();
    let connector = tower::service_fn(move |_: Uri| {
        calls.fetch_add(1, Ordering::SeqCst);
        let socket = socket.clone();
        async move { Ok::<_, io::Error>(TokioIo::new(CountIo(socket))) }
    });
    let error = tokio::time::timeout(WATCHDOG, endpoint.clone().connect_with_connector(connector))
        .await
        .expect("Tonic connection failed to refuse reused stream storage")
        .unwrap_err();
    assert!(format!("{error:?}").contains("already bound"));
    assert_eq!(
        connector_calls.load(Ordering::SeqCst),
        1,
        "an actual fresh dial supplies IO"
    );
    counts.assert_no_io();
    assert_eq!(counts.exits.load(Ordering::SeqCst), 1);
    held(&budget, total);
    drop(error);
    drop(endpoint);
    drop(buffer);
    drop(grant(&budget, total));
}
