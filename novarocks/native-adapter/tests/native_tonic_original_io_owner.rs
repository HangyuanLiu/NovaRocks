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

//! Concrete IO box exits on the actual Tonic factory path. The private-source
//! helper includes the unchanged production IO wrapper to test allocator and
//! destructor-unwind ordering without exposing a funded IO extraction API.
//! TLS buffers, socket registration, task/future allocations and the complete
//! connection envelope are outside these two concrete box allocation oracles.

use bytes::Bytes;
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_native_trust::BoxedNativeIo;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tonic::transport::{Endpoint, Http2ConnectionConfig};
use tower::Service;

mod transport {
    pub mod channel {
        pub mod service {
            pub mod io {
                include!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../vendor/tonic-0.12.3/src/transport/channel/service/io.rs"
                ));
            }
        }
    }
    pub fn wrap(
        io: super::BoxedNativeIo,
        owner: Option<super::Bytes>,
    ) -> impl hyper::rt::Read + hyper::rt::Write + Unpin {
        let io = channel::service::io::BoxedIo::new(super::TokioIo::new(io));
        channel::service::io::OwnedConnectionIo::new(io, owner)
    }
}

#[derive(Clone, Copy)]
struct Record {
    pointer: usize,
    bytes: usize,
    freed: bool,
}
const EMPTY: Record = Record {
    pointer: 0,
    bytes: 0,
    freed: false,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static OVERFLOW: Cell<bool> = const { Cell::new(false) };
    static NEXT_SIZE: Cell<usize> = const { Cell::new(0) };
    static RECORDS: RefCell<[Record; 2]> = const { RefCell::new([EMPTY; 2]) };
}
fn allocated(pointer: *mut u8, bytes: usize) {
    let tracking = TRACK.try_with(Cell::get).unwrap_or(false);
    let next = NEXT_SIZE.try_with(Cell::get).unwrap_or(0);
    if tracking || (next != 0 && next == bytes) {
        if next == bytes {
            NEXT_SIZE.with(|size| size.set(0));
        }
        RECORDS.with(|records| {
            let mut records = records.borrow_mut();
            if let Some(target) = records.iter_mut().find(|record| record.pointer == 0) {
                *target = Record {
                    pointer: pointer as usize,
                    bytes,
                    freed: false,
                };
            } else {
                OVERFLOW.with(|overflow| overflow.set(true));
            }
        });
    }
}
fn freed(pointer: *mut u8) {
    let _ = RECORDS.try_with(|records| {
        for record in records.borrow_mut().iter_mut() {
            if record.pointer == pointer as usize {
                record.freed = true;
            }
        }
    });
}
struct Probe;
// SAFETY: Every operation delegates unchanged to System. Fixed thread-local
// records neither allocate nor dereference the recorded addresses.
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
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        freed(pointer);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, bytes) };
        freed(pointer);
        allocated(next, bytes);
        next
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
fn measure<T>(operation: impl FnOnce() -> T) -> T {
    assert!(!TRACK.with(|v| v.replace(true)));
    let tracking = Tracking;
    let result = operation();
    drop(tracking);
    result
}
fn reset_records() {
    TRACK.with(|v| assert!(!v.get()));
    NEXT_SIZE.with(|v| v.set(0));
    OVERFLOW.with(|v| v.set(false));
    RECORDS.with(|v| *v.borrow_mut() = [EMPTY; 2]);
}
fn recorded() -> usize {
    RECORDS.with(|records| records.borrow().iter().filter(|r| r.pointer != 0).count())
}
fn all_freed() -> bool {
    RECORDS.with(|records| records.borrow().iter().all(|r| r.pointer == 0 || r.freed))
}
fn exact_two_boxes() {
    assert!(
        !OVERFLOW.with(Cell::get),
        "unexpected extra allocation in concrete IO boxing scope"
    );
    RECORDS.with(|records| {
        let records = records.borrow();
        assert!(records[0].pointer != 0 && records[1].pointer != 0);
        assert_eq!(records[0].bytes, Layout::new::<ExitIo>().size());
        assert_eq!(
            records[1].bytes,
            Layout::new::<TokioIo<BoxedNativeIo>>().size()
        );
    });
}
#[derive(Default)]
struct Ledger {
    owner_exited: AtomicBool,
    owner_before_box_free: AtomicBool,
    io_exited: AtomicBool,
    owner_before_io_drop: AtomicBool,
    calls: AtomicUsize,
}
struct OwnerExit {
    ledger: Arc<Ledger>,
    credit: Option<ResultWriteCredit>,
}
impl Drop for OwnerExit {
    fn drop(&mut self) {
        // Record failures rather than panic during another destructor's unwind.
        self.ledger
            .owner_before_box_free
            .store(!all_freed(), Ordering::SeqCst);
        self.ledger.owner_exited.store(true, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
fn backing_bytes() -> usize {
    Layout::new::<ExitIo>().size()
        + Layout::new::<TokioIo<BoxedNativeIo>>().size()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, OwnerExit>()
}
fn budget() -> Arc<ResultRetainedBudget> {
    ResultRetainedBudget::new(NonZeroUsize::new(backing_bytes()).unwrap())
}
fn original(budget: &Arc<ResultRetainedBudget>, ledger: &Arc<Ledger>) -> Bytes {
    let ResultWriteAdmission::Granted(credit) =
        budget.try_reserve_process(backing_bytes()).unwrap()
    else {
        panic!("fixture original IO capability exhausted")
    };
    Bytes::from_owner_with_exit_guard(
        Bytes::new(),
        OwnerExit {
            ledger: ledger.clone(),
            credit: Some(credit),
        },
    )
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(backing_bytes()).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn returned(budget: &Arc<ResultRetainedBudget>, ledger: &Arc<Ledger>) {
    assert!(ledger.owner_exited.load(Ordering::SeqCst));
    assert!(!ledger.owner_before_box_free.load(Ordering::SeqCst));
    assert!(!ledger.owner_before_io_drop.load(Ordering::SeqCst));
    assert!(all_freed());
    let ResultWriteAdmission::Granted(credit) =
        budget.try_reserve_process(backing_bytes()).unwrap()
    else {
        panic!("actual IO box exits must return original funding")
    };
    drop(credit);
}
struct ExitIo {
    inner: Option<DuplexStream>,
    ledger: Arc<Ledger>,
    panic_on_drop: bool,
}
impl AsyncRead for ExitIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for ExitIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_shutdown(cx)
    }
}
impl Drop for ExitIo {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.ledger.io_exited.store(true, Ordering::SeqCst);
        // Record the order for the explicit oracle outside Drop. If a live
        // credit assertion has already failed, cleanup must not double-panic.
        self.ledger.owner_before_io_drop.store(
            self.ledger.owner_exited.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        if self.panic_on_drop {
            panic!("actual concrete boxed IO destructor panic");
        }
    }
}
fn boxed_io(inner: DuplexStream, ledger: &Arc<Ledger>, panic_on_drop: bool) -> BoxedNativeIo {
    measure(|| {
        Box::new(ExitIo {
            inner: Some(inner),
            ledger: ledger.clone(),
            panic_on_drop,
        }) as BoxedNativeIo
    })
}
#[test]
fn actual_private_wrapper_frees_both_boxes_before_original_owner() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let owner = original(&budget, &ledger);
    let (inner, peer) = tokio::io::duplex(65536);
    let io = boxed_io(inner, &ledger, false);
    let io = measure(|| transport::wrap(io, Some(owner)));
    exact_two_boxes();
    held(&budget);
    drop(io);
    returned(&budget, &ledger);
    drop(peer);
}
#[test]
fn actual_private_wrapper_frees_both_boxes_during_io_destructor_unwind() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let owner = original(&budget, &ledger);
    let (inner, peer) = tokio::io::duplex(65536);
    let io = boxed_io(inner, &ledger, true);
    let io = measure(|| transport::wrap(io, Some(owner)));
    exact_two_boxes();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(io))).is_err());
    returned(&budget, &ledger);
    drop(peer);
}
#[test]
fn escaping_original_alias_keeps_same_credit_after_both_io_boxes_exit() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let owner = original(&budget, &ledger);
    let alias = owner.clone();
    let (inner, peer) = tokio::io::duplex(65536);
    let io = boxed_io(inner, &ledger, false);
    let io = measure(|| transport::wrap(io, Some(owner)));
    exact_two_boxes();
    drop(io);
    assert!(all_freed());
    held(&budget);
    assert!(!ledger.owner_exited.load(Ordering::SeqCst));
    drop(alias);
    returned(&budget, &ledger);
    drop(peer);
}

#[test]
fn actual_ordered_scope_cancel_before_first_poll_exits_captured_io_box() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let owner = original(&budget, &ledger);
    let (inner, peer) = tokio::io::duplex(65536);
    let io = boxed_io(inner, &ledger, false);
    let future = tonic::transport::ConnectionAcquisition::new(
        async move {
            let _io = io;
            std::future::pending::<()>().await;
        },
        Some(owner),
    );
    assert_eq!(recorded(), 1);
    held(&budget);
    drop(future);
    returned(&budget, &ledger);
    drop(peer);
}

#[test]
fn private_wrapper_none_keeps_only_the_original_two_io_box_allocations() {
    reset_records();
    let ledger = Arc::new(Ledger::default());
    let (inner, peer) = tokio::io::duplex(65536);
    let io = boxed_io(inner, &ledger, false);
    let io = measure(|| transport::wrap(io, None));
    exact_two_boxes();
    drop(io);
    assert!(all_freed());
    drop(peer);
}

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Default)]
struct ManualExecutor(Arc<Mutex<Vec<Task>>>);
impl<F: Future<Output = ()> + Send + 'static> Executor<F> for ManualExecutor {
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(Box::pin(future));
    }
}
impl ManualExecutor {
    fn poll_once(&self, cx: &mut Context<'_>) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        for mut task in tasks {
            if task.as_mut().poll(cx).is_pending() {
                self.0.lock().unwrap().push(task);
            }
        }
    }
    fn stop(&self) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        drop(tasks);
    }
}
async fn drive<F: Future>(executor: &ManualExecutor, future: F) -> F::Output {
    let mut future = Box::pin(future);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            executor.poll_once(cx);
            match future.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }),
    )
    .await
    .expect("actual Tonic IO fixture exceeded progress watchdog")
}
#[derive(Clone, Copy)]
enum Dial {
    Ready,
    Pending,
    Error,
}
struct DialFuture {
    io: Option<BoxedNativeIo>,
    mode: Dial,
}
impl Future for DialFuture {
    type Output = io::Result<TokioIo<BoxedNativeIo>>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        match self.mode {
            Dial::Pending => Poll::Pending,
            Dial::Error => {
                drop(self.io.take());
                Poll::Ready(Err(io::ErrorKind::ConnectionRefused.into()))
            }
            Dial::Ready => {
                // Actual Connector awaits this Ready and immediately boxes this
                // exact TokioIo type. No other await/allocation intervenes in
                // tonic service/connector.rs before BoxedIo::new.
                NEXT_SIZE.with(|size| size.set(Layout::new::<TokioIo<BoxedNativeIo>>().size()));
                Poll::Ready(Ok(TokioIo::new(self.io.take().unwrap())))
            }
        }
    }
}
#[derive(Clone)]
struct Connector {
    inner: Arc<Mutex<Option<DuplexStream>>>,
    ledger: Arc<Ledger>,
    mode: Dial,
}
impl Service<hyper::http::Uri> for Connector {
    type Response = TokioIo<BoxedNativeIo>;
    type Error = io::Error;
    type Future = DialFuture;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: hyper::http::Uri) -> Self::Future {
        self.ledger.calls.fetch_add(1, Ordering::SeqCst);
        let inner = self.inner.lock().unwrap().take().unwrap();
        DialFuture {
            io: Some(boxed_io(inner, &self.ledger, false)),
            mode: self.mode,
        }
    }
}
fn endpoint(
    budget: &Arc<ResultRetainedBudget>,
    ledger: &Arc<Ledger>,
    executor: &ManualExecutor,
    invalid: bool,
) -> Endpoint {
    let budget = budget.clone();
    let ledger = ledger.clone();
    Endpoint::from_static("http://example.test")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            Ok::<_, io::Error>(Http2ConnectionConfig {
                io_owner: Some(original(&budget, &ledger)),
                max_frame_size: invalid.then_some(1),
                ..Default::default()
            })
        })
}
fn connector(inner: DuplexStream, ledger: &Arc<Ledger>, mode: Dial) -> Connector {
    Connector {
        inner: Arc::new(Mutex::new(Some(inner))),
        ledger: ledger.clone(),
        mode,
    }
}
#[tokio::test]
async fn actual_channel_keeps_io_owner_after_success_until_connection_exit() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let executor = ManualExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor, false);
    let (inner, peer) = tokio::io::duplex(65536);
    let (close, done) = tokio::sync::oneshot::channel();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let connection = h2::server::handshake(peer).await.unwrap();
        ready_tx.send(()).unwrap();
        done.await.unwrap();
        drop(connection);
    });
    let channel = drive(
        &executor,
        endpoint.connect_with_connector(connector(inner, &ledger, Dial::Ready)),
    )
    .await
    .unwrap();
    // A Channel without the initial-settings option can become Ready before
    // its independent connection task flushes the preface. Drive that actual
    // task until the peer finishes its handshake before closing either IO.
    drive(&executor, ready_rx)
        .await
        .expect("real peer handshake completed");
    exact_two_boxes();
    held(&budget);
    assert!(!ledger.owner_exited.load(Ordering::SeqCst));
    drop(channel);
    executor.stop();
    returned(&budget, &ledger);
    drop(endpoint);
    close.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn actual_channel_dial_failure_exits_box_before_original_owner() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let executor = ManualExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor, false);
    let (inner, peer) = tokio::io::duplex(65536);
    assert!(
        drive(
            &executor,
            endpoint.connect_with_connector(connector(inner, &ledger, Dial::Error))
        )
        .await
        .is_err()
    );
    assert_eq!(recorded(), 1);
    returned(&budget, &ledger);
    executor.stop();
    drop(endpoint);
    drop(peer);
}
#[tokio::test]
async fn actual_channel_pending_dial_cancel_exits_box_before_original_owner() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let executor = ManualExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor, false);
    let (inner, peer) = tokio::io::duplex(65536);
    let mut connecting =
        Box::pin(endpoint.connect_with_connector(connector(inner, &ledger, Dial::Pending)));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(ledger.calls.load(Ordering::SeqCst), 1);
    assert_eq!(recorded(), 1);
    held(&budget);
    drop(connecting);
    returned(&budget, &ledger);
    executor.stop();
    drop(endpoint);
    drop(peer);
}
#[tokio::test]
async fn actual_channel_factory_validation_rejects_without_dial_and_returns_owner() {
    reset_records();
    let budget = budget();
    let ledger = Arc::new(Ledger::default());
    let executor = ManualExecutor::default();
    let endpoint = endpoint(&budget, &ledger, &executor, true);
    let (inner, peer) = tokio::io::duplex(65536);
    assert!(
        drive(
            &executor,
            endpoint.connect_with_connector(connector(inner, &ledger, Dial::Ready))
        )
        .await
        .is_err()
    );
    assert_eq!(ledger.calls.load(Ordering::SeqCst), 0);
    assert_eq!(recorded(), 0);
    returned(&budget, &ledger);
    executor.stop();
    drop(endpoint);
    drop(peer);
}
