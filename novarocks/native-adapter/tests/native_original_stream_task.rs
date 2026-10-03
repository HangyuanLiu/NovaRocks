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

//! Real Hyper H2Stream tasks executed by the original Native task executor.
//! The instrumentation executor only tags actual generic prepare/spawn calls;
//! its bound queries use Hyper's real private H2Stream, never a model future.
//! Connection tasks, peer IO, HPACK/header backing, scheduler queues and fixture
//! allocations are separate and are not claimed by this TaskCell receipt.

use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::http::{Method, Request, Response};
use hyper::rt::Executor;
use hyper::service::Service;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_native_adapter::native_task_executor::NativeTaskExecutor;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

const LIMIT: usize = 64;
const ORIGINAL: usize = 1;
const POOL: usize = 2;
const PREPARE: usize = 3;
const TASK: usize = 4;
const WATCHDOG: Duration = Duration::from_secs(5);
struct Record {
    pointer: AtomicUsize,
    size: AtomicUsize,
    align: AtomicUsize,
    phase: AtomicUsize,
    freed: AtomicBool,
}
impl Record {
    const fn new() -> Self {
        Self {
            pointer: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
            align: AtomicUsize::new(0),
            phase: AtomicUsize::new(0),
            freed: AtomicBool::new(false),
        }
    }
}
static RECORDS: [Record; LIMIT] = [const { Record::new() }; LIMIT];
static COUNT: AtomicUsize = AtomicUsize::new(0);
static OVERFLOW: AtomicBool = AtomicBool::new(false);
static REALLOCATED: AtomicBool = AtomicBool::new(false);
static SERIAL: Mutex<()> = Mutex::new(());
thread_local! { static TAG: Cell<usize> = const { Cell::new(0) }; }
fn allocated(pointer: *mut u8, layout: Layout) {
    let phase = TAG.try_with(Cell::get).unwrap_or(0);
    if phase == 0 || pointer.is_null() {
        return;
    }
    let index = COUNT.fetch_add(1, Ordering::SeqCst);
    if let Some(r) = RECORDS.get(index) {
        r.size.store(layout.size(), Ordering::SeqCst);
        r.align.store(layout.align(), Ordering::SeqCst);
        r.phase.store(phase, Ordering::SeqCst);
        r.freed.store(false, Ordering::SeqCst);
        r.pointer.store(pointer as usize, Ordering::SeqCst);
    } else {
        OVERFLOW.store(true, Ordering::SeqCst);
    }
}
fn freed(pointer: *mut u8) {
    for r in &RECORDS {
        if r.pointer.load(Ordering::SeqCst) == pointer as usize {
            r.freed.store(true, Ordering::SeqCst);
        }
    }
}
struct Probe;
// SAFETY: System receives the unchanged layout/pointer. Instrumentation only
// records atomic addresses; it never dereferences them or allocates/locks.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        allocated(p, layout);
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        allocated(p, layout);
        p
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if TAG.try_with(Cell::get).unwrap_or(0) != 0
            || RECORDS.iter().any(|r| {
                r.pointer.load(Ordering::SeqCst) == p as usize && !r.freed.load(Ordering::SeqCst)
            })
        {
            REALLOCATED.store(true, Ordering::SeqCst);
        }
        let next = unsafe { System.realloc(p, layout, size) };
        freed(p);
        allocated(next, Layout::from_size_align(size, layout.align()).unwrap());
        next
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) };
        freed(p);
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
fn measured<T>(phase: usize, f: impl FnOnce() -> T) -> T {
    assert_eq!(TAG.with(|tag| tag.replace(phase)), 0);
    struct End;
    impl Drop for End {
        fn drop(&mut self) {
            TAG.with(|tag| tag.set(0));
        }
    }
    let end = End;
    let result = f();
    drop(end);
    result
}
fn reset() {
    assert_eq!(TAG.with(Cell::get), 0);
    COUNT.store(0, Ordering::SeqCst);
    OVERFLOW.store(false, Ordering::SeqCst);
    REALLOCATED.store(false, Ordering::SeqCst);
    for r in &RECORDS {
        r.pointer.store(0, Ordering::SeqCst);
        r.size.store(0, Ordering::SeqCst);
        r.align.store(0, Ordering::SeqCst);
        r.phase.store(0, Ordering::SeqCst);
        r.freed.store(false, Ordering::SeqCst);
    }
}
fn phase_count(phase: usize) -> usize {
    RECORDS
        .iter()
        .filter(|r| r.phase.load(Ordering::SeqCst) == phase)
        .count()
}
fn all_freed() -> bool {
    RECORDS
        .iter()
        .all(|r| r.pointer.load(Ordering::SeqCst) == 0 || r.freed.load(Ordering::SeqCst))
}
fn record(pointer: usize) -> &'static Record {
    assert_ne!(pointer, 0);
    RECORDS
        .iter()
        .find(|r| r.pointer.load(Ordering::SeqCst) == pointer)
        .expect("true service task Waker must point at an actual tracked Cell")
}
#[derive(Default)]
struct State {
    calls: AtomicUsize,
    polls: AtomicUsize,
    cells: [AtomicUsize; 4],
    futures: [AtomicUsize; 4],
    wakers: Mutex<[Option<Waker>; 4]>,
    owner_exited: AtomicBool,
    early_exit: AtomicBool,
}
impl State {
    fn new() -> Arc<Self> {
        let state = Arc::new(Self::default());
        drop(state.wakers.lock().unwrap()); // Fixture PAL is outside the receipt.
        state
    }
    fn take_waker(&self, index: usize) -> Waker {
        self.wakers.lock().unwrap()[index].take().unwrap()
    }
}
struct Exit {
    state: Arc<State>,
    credit: Option<ResultWriteCredit>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        // Record once rather than panic during cleanup in a source negative.
        self.state.early_exit.store(!all_freed(), Ordering::SeqCst);
        self.state.owner_exited.store(true, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
struct Funding {
    budget: Arc<ResultRetainedBudget>,
    state: Arc<State>,
    total: usize,
    bound: usize,
}
impl Funding {
    fn new(state: &Arc<State>, bound: usize) -> (Self, NativeTaskExecutor) {
        reset();
        let total = NativeTaskExecutor::allocation_capacity_bound(1, bound)
            .unwrap()
            .checked_add(Bytes::owner_with_exit_guard_metadata_size::<Bytes, Exit>())
            .unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("original complete pool/task/carrier grant must precede allocation");
        };
        let owner = measured(ORIGINAL, || {
            Bytes::from_owner_with_exit_guard(
                Bytes::new(),
                Exit {
                    state: state.clone(),
                    credit: Some(credit),
                },
            )
        });
        let exec = measured(POOL, || NativeTaskExecutor::with_original(1, bound, owner)).unwrap();
        assert_eq!(phase_count(ORIGINAL), 1);
        assert_eq!(
            phase_count(POOL),
            2,
            "fixed position Vec and strong-only Core Arc"
        );
        (
            Self {
                budget,
                state: state.clone(),
                total,
                bound,
            },
            exec,
        )
    }
    fn held(&self) {
        assert!(!self.state.owner_exited.load(Ordering::SeqCst));
        assert!(matches!(
            self.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn returned(&self) {
        assert!(self.state.owner_exited.load(Ordering::SeqCst));
        assert!(
            !self.state.early_exit.load(Ordering::SeqCst),
            "Cell/autoBox/carrier/pool requested backing must physically exit before credit"
        );
        assert!(all_freed());
        assert!(!OVERFLOW.load(Ordering::SeqCst));
        assert!(!REALLOCATED.load(Ordering::SeqCst));
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("full original grant must return");
        };
        drop(credit);
    }
    fn shape(&self, boxed: bool, alignment: usize) {
        assert_eq!(phase_count(PREPARE), 1, "one original TaskExit carrier");
        assert_eq!(phase_count(TASK), 1 + usize::from(boxed));
        assert_eq!(
            RECORDS
                .iter()
                .filter(|r| r.phase.load(Ordering::SeqCst) == TASK)
                .map(|r| r.size.load(Ordering::SeqCst))
                .sum::<usize>(),
            self.bound
        );
        let cell = record(self.state.cells[0].load(Ordering::SeqCst));
        assert_eq!(cell.phase.load(Ordering::SeqCst), TASK);
        let f = self.state.futures[0].load(Ordering::SeqCst);
        let containing = RECORDS
            .iter()
            .find(|r| {
                let start = r.pointer.load(Ordering::SeqCst);
                r.phase.load(Ordering::SeqCst) == TASK
                    && f >= start
                    && f < start + r.size.load(Ordering::SeqCst)
            })
            .expect("actual service future is inline in real Cell or real automatic H2Stream Box");
        assert_eq!(!std::ptr::eq(cell, containing), boxed);
        assert!(containing.align.load(Ordering::SeqCst) >= alignment);
        assert_eq!(f % alignment, 0);
    }
}
#[derive(Clone)]
struct TaggedExecutor(NativeTaskExecutor);
impl<F> Executor<F> for TaggedExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        <NativeTaskExecutor as Executor<F>>::task_allocation_capacity_bound()
    }
    fn try_prepare_task(&self) -> io::Result<Option<Self>> {
        measured(PREPARE, || {
            <NativeTaskExecutor as Executor<F>>::try_prepare_task(&self.0)
        })
        .map(|prepared| prepared.map(Self))
    }
    fn execute(&self, future: F) {
        measured(TASK, || self.0.execute(future));
    }
}
struct Empty;
impl Body for Empty {
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
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(0)
    }
}
struct ServiceFuture<const N: usize> {
    state: Arc<State>,
    index: usize,
    padding: [u8; N],
}
impl<const N: usize> Future for ServiceFuture<N> {
    type Output = Result<Response<Empty>, Infallible>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.polls.fetch_add(1, Ordering::SeqCst);
        self.state.cells[self.index].store(cx.waker().data() as usize, Ordering::SeqCst);
        self.state.futures[self.index].store(
            self.as_ref().get_ref() as *const Self as usize,
            Ordering::SeqCst,
        );
        self.state.wakers.lock().unwrap()[self.index] = Some(cx.waker().clone());
        assert_eq!(self.padding.first().copied().unwrap_or(19), 19);
        Poll::Ready(Ok(Response::new(Empty)))
    }
}
#[repr(align(512))]
struct AlignedFuture(ServiceFuture<32768>);
impl Future for AlignedFuture {
    type Output = Result<Response<Empty>, Infallible>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
struct TestService<F> {
    state: Arc<State>,
    make: fn(Arc<State>, usize) -> F,
}
impl<F> Service<Request<Incoming>> for TestService<F>
where
    F: Future<Output = Result<Response<Empty>, Infallible>> + Send + 'static,
{
    type Response = Response<Empty>;
    type Error = Infallible;
    type Future = F;
    fn call(&self, _request: Request<Incoming>) -> F {
        let index = self.state.calls.fetch_add(1, Ordering::SeqCst);
        assert!(index < 4);
        (self.make)(self.state.clone(), index)
    }
}
fn plain<const N: usize>(state: Arc<State>, index: usize) -> ServiceFuture<N> {
    ServiceFuture {
        state,
        index,
        padding: [19; N],
    }
}
fn aligned(state: Arc<State>, index: usize) -> AlignedFuture {
    AlignedFuture(plain(state, index))
}
struct ObservedIo {
    inner: DuplexStream,
    exited: Arc<AtomicUsize>,
}
impl Drop for ObservedIo {
    fn drop(&mut self) {
        self.exited.fetch_add(1, Ordering::SeqCst);
    }
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
fn warmed_runtime() -> Runtime {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Ordinary TaskId warmup covers OwnedTasks shards and queue/PAL setup.
    // Those shared allocations remain open, independently of this receipt.
    for _ in 0..128 {
        runtime.block_on(runtime.spawn(async {})).unwrap();
    }
    runtime
}
async fn stop<T>(task: JoinHandle<T>) {
    task.abort();
    let result = tokio::time::timeout(WATCHDOG, task)
        .await
        .expect("actual outer task must join");
    if let Err(error) = result {
        assert!(error.is_cancelled(), "outer task must not panic: {error}");
    }
}
async fn request(
    client: &mut h2::client::SendRequest<Bytes>,
    method: Method,
) -> Result<Response<h2::RecvStream>, h2::Error> {
    std::future::poll_fn(|cx| client.poll_ready(cx)).await?;
    let uri = if method == Method::CONNECT {
        "example.test:443"
    } else {
        "http://example.test/"
    };
    let (response, _) = client.send_request(
        Request::builder().method(method).uri(uri).body(()).unwrap(),
        true,
    )?;
    tokio::time::timeout(WATCHDOG, response)
        .await
        .expect("actual response/reset must arrive")
}
fn protocol<F>(make: fn(Arc<State>, usize) -> F, boxed: bool, alignment: usize, saturation: bool)
where
    F: Future<Output = Result<Response<Empty>, Infallible>> + Send + 'static,
{
    let _serial = SERIAL.lock().unwrap();
    let runtime = warmed_runtime();
    let state = State::new();
    let bound = hyper::server::conn::http2::Builder::<TaggedExecutor>::stream_task_allocation_capacity_bound::<TestService<F>>().unwrap();
    let (funding, exec) = Funding::new(&state, bound);
    let escaped = runtime.block_on(async {
        let (io, peer) = tokio::io::duplex(131072);
        let exited = Arc::new(AtomicUsize::new(0));
        let mut builder = hyper::server::conn::http2::Builder::new(TaggedExecutor(exec));
        builder
            .auto_date_header(false)
            .reject_connect_for_preallocated_tasks(true);
        let server = tokio::spawn(builder.serve_connection(
            TokioIo::new(ObservedIo {
                inner: io,
                exited: exited.clone(),
            }),
            TestService {
                state: state.clone(),
                make,
            },
        ));
        let (mut client, connection) = h2::client::handshake(peer).await.unwrap();
        let peer_task = tokio::spawn(connection);
        let response = request(&mut client, Method::GET).await.unwrap();
        assert_eq!(response.status(), 200);
        drop(response);
        funding.shape(boxed, alignment);
        funding.held();
        let escaped = state.take_waker(0);
        if saturation {
            let before = COUNT.load(Ordering::SeqCst);
            let refused = request(&mut client, Method::GET).await.unwrap_err();
            assert_eq!(refused.reason(), Some(h2::Reason::REFUSED_STREAM));
            assert_eq!(state.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                COUNT.load(Ordering::SeqCst),
                before,
                "full preparation refuses without carrier/TaskCell allocation"
            );
            drop(escaped);
            let response = request(&mut client, Method::GET).await.unwrap();
            assert_eq!(response.status(), 200);
            drop(response);
            assert_eq!(state.calls.load(Ordering::SeqCst), 2);
            let escaped = state.take_waker(1);
            drop(client);
            stop(peer_task).await;
            stop(server).await;
            drop(builder);
            assert_eq!(exited.load(Ordering::SeqCst), 1);
            escaped
        } else {
            drop(client);
            stop(peer_task).await;
            stop(server).await;
            drop(builder);
            assert_eq!(exited.load(Ordering::SeqCst), 1);
            escaped
        }
    });
    drop(runtime); // Shared scheduler references actually exit before the final user alias.
    funding.held();
    drop(escaped);
    funding.returned();
}
#[test]
fn real_small_stream_cell_waker_holds_position_refuses_before_service_and_recovers() {
    protocol(
        plain::<0>,
        false,
        std::mem::align_of::<ServiceFuture<0>>(),
        true,
    );
}
#[test]
fn real_large_stream_automatic_future_box_exits_before_original_pool_credit() {
    protocol(
        plain::<32768>,
        true,
        std::mem::align_of::<ServiceFuture<32768>>(),
        false,
    );
}
#[test]
fn real_overaligned_stream_future_box_uses_actual_layout_and_last_waker_exit() {
    protocol(aligned, true, 512, false);
}

#[test]
fn connect_is_refused_before_preparation_and_service_while_ordinary_default_dispatches() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = warmed_runtime();
    let state = State::new();
    let bound = hyper::server::conn::http2::Builder::<TaggedExecutor>::stream_task_allocation_capacity_bound::<TestService<ServiceFuture<0>>>().unwrap();
    let (funding, exec) = Funding::new(&state, bound);
    runtime.block_on(async {
        let (io, peer) = tokio::io::duplex(131072);
        let mut builder = hyper::server::conn::http2::Builder::new(TaggedExecutor(exec));
        builder
            .auto_date_header(false)
            .reject_connect_for_preallocated_tasks(true);
        let server = tokio::spawn(builder.serve_connection(
            TokioIo::new(io),
            TestService {
                state: state.clone(),
                make: plain::<0>,
            },
        ));
        let (mut client, connection) = h2::client::handshake(peer).await.unwrap();
        let peer_task = tokio::spawn(connection);
        let before = COUNT.load(Ordering::SeqCst);
        let refused = request(&mut client, Method::CONNECT).await.unwrap_err();
        assert_eq!(refused.reason(), Some(h2::Reason::REFUSED_STREAM));
        assert_eq!(state.calls.load(Ordering::SeqCst), 0);
        assert_eq!(COUNT.load(Ordering::SeqCst), before);
        drop(client);
        stop(peer_task).await;
        stop(server).await;
        drop(builder);
    });
    drop(runtime);
    funding.returned();
    // Default ordinary executor keeps CONNECT's legacy service/upgrade path.
    // Its separate upgrade/task backing is deliberately outside the grant.
    let runtime = warmed_runtime();
    runtime.block_on(async {
        let (io, peer) = tokio::io::duplex(131072);
        let mut builder = hyper::server::conn::http2::Builder::new(NativeTaskExecutor::ordinary());
        builder.auto_date_header(false);
        let server = tokio::spawn(builder.serve_connection(
            TokioIo::new(io),
            TestService {
                state: state.clone(),
                make: plain::<0>,
            },
        ));
        let (mut client, connection) = h2::client::handshake(peer).await.unwrap();
        let peer_task = tokio::spawn(connection);
        let response = request(&mut client, Method::CONNECT).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);
        drop(response);
        drop(state.take_waker(0));
        drop(client);
        stop(peer_task).await;
        stop(server).await;
        drop(builder);
    });
}

struct HookFuture<const N: usize> {
    state: Arc<State>,
    padding: [u8; N],
}
impl<const N: usize> Future for HookFuture<N> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.state.polls.fetch_add(1, Ordering::SeqCst);
        self.state.cells[0].store(cx.waker().data() as usize, Ordering::SeqCst);
        self.state.wakers.lock().unwrap()[0] = Some(cx.waker().clone());
        assert_eq!(self.padding.first().copied().unwrap_or(19), 19);
        Poll::Ready(())
    }
}
#[test]
fn public_prepared_alias_cannot_replay_or_spawn_larger_future_and_cannot_reuse_early() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = warmed_runtime();
    let state = State::new();
    let bound =
        <NativeTaskExecutor as Executor<HookFuture<0>>>::task_allocation_capacity_bound().unwrap();
    let (funding, executor) = Funding::new(&state, bound);
    let entered = runtime.enter();
    let prepared = measured(PREPARE, || {
        <NativeTaskExecutor as Executor<HookFuture<0>>>::try_prepare_task(&executor)
    })
    .unwrap()
    .unwrap();
    let alias = prepared.clone();
    let before = COUNT.load(Ordering::SeqCst);
    measured(TASK, || {
        prepared.execute(HookFuture {
            state: state.clone(),
            padding: [19; 32768],
        })
    });
    assert_eq!(
        COUNT.load(Ordering::SeqCst),
        before,
        "oversized future must not allocate or consume the prepared position"
    );
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    measured(TASK, || {
        prepared.execute(HookFuture {
            state: state.clone(),
            padding: [19; 0],
        })
    });
    let after = COUNT.load(Ordering::SeqCst);
    measured(TASK, || {
        alias.execute(HookFuture {
            state: state.clone(),
            padding: [19; 0],
        })
    });
    assert_eq!(
        COUNT.load(Ordering::SeqCst),
        after,
        "same prepared lease cannot spawn a second Cell"
    );
    drop(entered);
    runtime.block_on(async {
        tokio::time::timeout(WATCHDOG, async {
            while state.polls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
    assert_eq!(state.polls.load(Ordering::SeqCst), 1);
    assert_eq!(phase_count(TASK), 1);
    assert_eq!(
        record(state.cells[0].load(Ordering::SeqCst))
            .phase
            .load(Ordering::SeqCst),
        TASK
    );
    let waker = state.take_waker(0);
    drop(waker);
    let before = COUNT.load(Ordering::SeqCst);
    let refused = measured(PREPARE, || {
        <NativeTaskExecutor as Executor<HookFuture<0>>>::try_prepare_task(&executor)
    });
    assert_eq!(refused.err().unwrap().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(
        COUNT.load(Ordering::SeqCst),
        before,
        "aliases retain the same original carrier/position after Cell exit"
    );
    drop(prepared);
    assert_eq!(
        <NativeTaskExecutor as Executor<HookFuture<0>>>::try_prepare_task(&executor)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(alias);
    let recovered = measured(PREPARE, || {
        <NativeTaskExecutor as Executor<HookFuture<0>>>::try_prepare_task(&executor)
    })
    .unwrap()
    .unwrap();
    drop(recovered);
    drop(executor);
    drop(runtime);
    funding.returned();
}
