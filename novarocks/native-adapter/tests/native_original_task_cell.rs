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

//! Actual Tokio TaskCell and existing automatic Future Box allocations only.
//! Input/output external backing, task/Waker fixtures, shared scheduler shards,
//! queues, runtime and instrumentation are independently prepared and excluded.
//! Global atomic address records observe cross-thread System deallocation;
//! root-thread TLS tags only the actual spawn construction allocation scope.

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

const RECORD_LIMIT: usize = 16;
struct Record {
    pointer: AtomicUsize,
    bytes: AtomicUsize,
    alignment: AtomicUsize,
    freed: AtomicBool,
}
impl Record {
    const fn new() -> Self {
        Self {
            pointer: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            alignment: AtomicUsize::new(0),
            freed: AtomicBool::new(false),
        }
    }
}
static RECORDS: [Record; RECORD_LIMIT] = [const { Record::new() }; RECORD_LIMIT];
static COUNT: AtomicUsize = AtomicUsize::new(0);
static OVERFLOW: AtomicBool = AtomicBool::new(false);
static REALLOCATED: AtomicBool = AtomicBool::new(false);
static CARRIER: AtomicUsize = AtomicUsize::new(0);
static CARRIER_FREED: AtomicUsize = AtomicUsize::new(0);
static SERIAL: Mutex<()> = Mutex::new(());
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CAPTURE_CARRIER: Cell<bool> = const { Cell::new(false) };
}
fn allocated(pointer: *mut u8, layout: Layout) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let index = COUNT.fetch_add(1, Ordering::SeqCst);
        if let Some(record) = RECORDS.get(index) {
            record.bytes.store(layout.size(), Ordering::SeqCst);
            record.alignment.store(layout.align(), Ordering::SeqCst);
            record.freed.store(false, Ordering::SeqCst);
            record.pointer.store(pointer as usize, Ordering::SeqCst);
        } else {
            OVERFLOW.store(true, Ordering::SeqCst);
        }
    }
    if CAPTURE_CARRIER.try_with(Cell::get).unwrap_or(false) {
        CARRIER.store(pointer as usize, Ordering::SeqCst);
    }
}
fn freed(pointer: *mut u8) {
    for record in &RECORDS {
        if record.pointer.load(Ordering::SeqCst) == pointer as usize {
            record.freed.store(true, Ordering::SeqCst);
        }
    }
}
struct Probe;
// SAFETY: Operations forward unchanged to System. Records use only atomics and
// address comparisons, never locks, allocations or dereferences of freed data.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout);
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout);
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if TRACK.try_with(Cell::get).unwrap_or(false)
            || RECORDS
                .iter()
                .any(|r| r.pointer.load(Ordering::SeqCst) == pointer as usize)
        {
            REALLOCATED.store(true, Ordering::SeqCst);
        }
        let next = unsafe { System.realloc(pointer, layout, size) };
        freed(pointer);
        allocated(next, Layout::from_size_align(size, layout.align()).unwrap());
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        freed(pointer);
        if CARRIER
            .compare_exchange(pointer as usize, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            CARRIER_FREED.store(layout.size(), Ordering::SeqCst);
        }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
fn reset() {
    assert!(!TRACK.with(Cell::get));
    COUNT.store(0, Ordering::SeqCst);
    OVERFLOW.store(false, Ordering::SeqCst);
    REALLOCATED.store(false, Ordering::SeqCst);
    CARRIER.store(0, Ordering::SeqCst);
    CARRIER_FREED.store(0, Ordering::SeqCst);
    for record in &RECORDS {
        record.pointer.store(0, Ordering::SeqCst);
        record.bytes.store(0, Ordering::SeqCst);
        record.alignment.store(0, Ordering::SeqCst);
        record.freed.store(false, Ordering::SeqCst);
    }
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
        .expect("actual public task/future pointer must match a System allocation")
}
#[derive(Default)]
struct State {
    cell: AtomicUsize,
    future: AtomicUsize,
    polls: AtomicUsize,
    future_dropped: AtomicUsize,
    owner_exited: AtomicBool,
    early_exit: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
impl State {
    fn new() -> Arc<Self> {
        let state = Arc::new(Self::default());
        // User-owned fixture mutex backing is outside the task construction tag.
        drop(state.waker.lock().unwrap());
        state
    }
    fn take_waker(&self) -> Waker {
        self.waker.lock().unwrap().take().unwrap()
    }
}
struct Exit {
    state: Arc<State>,
    credit: Option<ResultWriteCredit>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        // Record rather than panic inside Tokio's destruction path. Negative
        // source mutations must yield one test failure, not cleanup double panic.
        self.state.early_exit.store(
            !all_freed() || CARRIER_FREED.load(Ordering::SeqCst) != carrier_bytes(),
            Ordering::SeqCst,
        );
        self.state.owner_exited.store(true, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
fn carrier_bytes() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, Exit>()
}
struct Funding {
    budget: Arc<ResultRetainedBudget>,
    bound: usize,
    total: usize,
    state: Arc<State>,
}
impl Funding {
    fn new<F: Future + Send + 'static>(state: &Arc<State>) -> (Self, Bytes)
    where
        F::Output: Send + 'static,
    {
        reset();
        let bound = Handle::task_allocation_capacity_bound::<F>().unwrap();
        let total = bound.checked_add(carrier_bytes()).unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("pregrant actual TaskCell/auto-Future-Box and original carrier before spawn");
        };
        CAPTURE_CARRIER.with(|v| v.set(true));
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            Exit {
                state: state.clone(),
                credit: Some(credit),
            },
        );
        CAPTURE_CARRIER.with(|v| v.set(false));
        (
            Self {
                budget,
                bound,
                total,
                state: state.clone(),
            },
            owner,
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
            "actual Cell/FutureBox/carrier deallocation must precede credit release"
        );
        assert!(all_freed());
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("original complete grant must return after physical task backing exit");
        };
        drop(credit);
    }
    fn shape<F>(&self, boxed: bool, polled: bool) {
        assert!(!OVERFLOW.load(Ordering::SeqCst));
        assert!(!REALLOCATED.load(Ordering::SeqCst));
        assert_eq!(
            COUNT.load(Ordering::SeqCst),
            1 + usize::from(boxed),
            "warmed scope must contain exactly actual Cell and optional automatic FutureBox"
        );
        assert_eq!(
            RECORDS
                .iter()
                .map(|r| r.bytes.load(Ordering::SeqCst))
                .sum::<usize>(),
            self.bound
        );
        if polled {
            // The actual Tokio task Waker points to Header, which is the first
            // field of repr(C) Cell. This is not a size-matching model pointer.
            let cell = record(self.state.cell.load(Ordering::SeqCst));
            if boxed {
                let future = record(self.state.future.load(Ordering::SeqCst));
                assert!(!std::ptr::eq(cell, future));
                assert_eq!(
                    future.bytes.load(Ordering::SeqCst),
                    Layout::new::<F>().size()
                );
                assert_eq!(
                    future.alignment.load(Ordering::SeqCst),
                    Layout::new::<F>().align()
                );
            } else {
                let start = cell.pointer.load(Ordering::SeqCst);
                let future = self.state.future.load(Ordering::SeqCst);
                assert!(future >= start && future < start + cell.bytes.load(Ordering::SeqCst));
            }
        } else {
            // With a paused current-thread runtime there is no task Waker yet.
            // Exactly one allocation in the actual spawn scope is the Cell;
            // bound equality and its final System exit are checked separately.
            assert!(!boxed);
            assert_eq!(self.state.cell.load(Ordering::SeqCst), 0);
        }
    }
}
fn spawn<F>(runtime: &Runtime, future: F, owner: Bytes) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    assert!(!TRACK.with(|v| v.replace(true)));
    struct End;
    impl Drop for End {
        fn drop(&mut self) {
            TRACK.with(|v| v.set(false));
        }
    }
    let end = End;
    let result = runtime.handle().spawn_with_task_owner(future, owner);
    drop(end);
    result.unwrap()
}
fn runtime(multi: bool) -> Runtime {
    let runtime = if multi {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    };
    // TaskId-based OwnedTasks sharding: these fixed runtimes use 4/8 shards.
    // Bind sequential ordinary IDs across every shard and drive ready queues.
    // Their shared PAL/queue/runtime backing remains outside this receipt.
    for _ in 0..128 {
        let join = runtime.spawn(async {});
        runtime.block_on(join).unwrap();
    }
    runtime
}
#[derive(Debug)]
struct Output<const N: usize>([u8; N]);
struct ProbeFuture<const N: usize, const O: usize> {
    state: Arc<State>,
    payload: [u8; N],
    pending: bool,
    panic_on_drop: bool,
}
impl<const N: usize, const O: usize> ProbeFuture<N, O> {
    fn new(state: &Arc<State>, pending: bool, panic_on_drop: bool) -> Self {
        Self {
            state: state.clone(),
            payload: [19; N],
            pending,
            panic_on_drop,
        }
    }
}
impl<const N: usize, const O: usize> Future for ProbeFuture<N, O> {
    type Output = Output<O>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.state
            .cell
            .store(cx.waker().data() as usize, Ordering::SeqCst);
        let future_address = this as *mut Self as usize;
        this.state.future.store(future_address, Ordering::SeqCst);
        this.state.polls.fetch_add(1, Ordering::SeqCst);
        let mut waker = this.state.waker.lock().unwrap();
        if waker.is_none() {
            *waker = Some(cx.waker().clone());
        }
        if this.pending {
            Poll::Pending
        } else {
            assert!(this.payload.iter().all(|byte| *byte == 19));
            Poll::Ready(Output([29; O]))
        }
    }
}
impl<const N: usize, const O: usize> Drop for ProbeFuture<N, O> {
    fn drop(&mut self) {
        self.state.future_dropped.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_on_drop, "actual task future destructor panic");
    }
}
#[repr(align(512))]
struct AlignedFuture(ProbeFuture<1, 0>);
impl Future for AlignedFuture {
    type Output = Output<0>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}
fn wait_until_polled(runtime: &Runtime, state: &State) {
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.polls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("actual task did not reach its first poll");
    });
}

#[test]
fn current_thread_small_future_large_output_and_overaligned_cell_keep_original_credit() {
    let _serial = SERIAL.lock().unwrap();
    {
        let runtime = runtime(false);
        let state = State::new();
        let (funding, owner) = Funding::new::<ProbeFuture<0, 4096>>(&state);
        let output = runtime
            .block_on(spawn(
                &runtime,
                ProbeFuture::<0, 4096>::new(&state, false, false),
                owner,
            ))
            .unwrap();
        assert!(output.0.iter().all(|byte| *byte == 29));
        assert!(
            Layout::new::<Output<4096>>().size() > Layout::new::<ProbeFuture<0, 4096>>().size()
        );
        funding.shape::<ProbeFuture<0, 4096>>(false, true);
        assert_eq!(state.future_dropped.load(Ordering::SeqCst), 1);
        drop(runtime); // Remove scheduler/task owners, but not the escaped Waker.
        funding.held();
        drop(state.take_waker());
        funding.returned();
    }
    {
        let runtime = runtime(false);
        let state = State::new();
        let (funding, owner) = Funding::new::<AlignedFuture>(&state);
        runtime
            .block_on(spawn(
                &runtime,
                AlignedFuture(ProbeFuture::new(&state, false, false)),
                owner,
            ))
            .unwrap();
        funding.shape::<AlignedFuture>(false, true);
        assert_eq!(
            record(state.cell.load(Ordering::SeqCst))
                .alignment
                .load(Ordering::SeqCst),
            512
        );
        drop(runtime);
        funding.held();
        drop(state.take_waker());
        funding.returned();
    }
}

#[test]
fn multi_thread_automatic_future_box_and_cross_thread_last_waker_exit_precede_credit() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = runtime(true);
    let state = State::new();
    let (funding, owner) = Funding::new::<ProbeFuture<32768, 1>>(&state);
    let output = runtime
        .block_on(spawn(
            &runtime,
            ProbeFuture::<32768, 1>::new(&state, false, false),
            owner,
        ))
        .unwrap();
    assert_eq!(output.0, [29]);
    funding.shape::<ProbeFuture<32768, 1>>(true, true);
    assert!(
        record(state.future.load(Ordering::SeqCst))
            .freed
            .load(Ordering::SeqCst),
        "actual auto-FutureBox exits at future completion, independently of Cell"
    );
    assert!(
        !record(state.cell.load(Ordering::SeqCst))
            .freed
            .load(Ordering::SeqCst)
    );
    drop(runtime);
    funding.held();
    let waker = state.take_waker();
    std::thread::spawn(move || drop(waker)).join().unwrap();
    funding.returned();
}

#[test]
fn pending_abort_and_dropped_join_preserve_cell_through_actual_waker_alias() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = runtime(true);
    let state = State::new();
    let (funding, owner) = Funding::new::<ProbeFuture<1, 0>>(&state);
    let join = spawn(
        &runtime,
        ProbeFuture::<1, 0>::new(&state, true, false),
        owner,
    );
    wait_until_polled(&runtime, &state);
    funding.shape::<ProbeFuture<1, 0>>(false, true);
    join.abort();
    drop(join);
    funding.held();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.future_dropped.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("actual aborted future did not exit before runtime shutdown");
    });
    assert_eq!(state.future_dropped.load(Ordering::SeqCst), 1);
    drop(runtime);
    funding.held();
    drop(state.take_waker());
    funding.returned();
}

#[test]
fn unpolled_task_join_drop_and_runtime_shutdown_exit_actual_cell_before_credit() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = runtime(false);
    let state = State::new();
    let (funding, owner) = Funding::new::<ProbeFuture<1, 0>>(&state);
    let join = spawn(
        &runtime,
        ProbeFuture::<1, 0>::new(&state, true, false),
        owner,
    );
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    funding.shape::<ProbeFuture<1, 0>>(false, false);
    drop(join); // Detaching a JoinHandle does not reclaim an unpolled TaskCell.
    funding.held();
    drop(runtime);
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    assert_eq!(state.future_dropped.load(Ordering::SeqCst), 1);
    funding.returned();
}

#[test]
fn future_destructor_panic_keeps_join_error_and_original_cell_until_last_waker() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = runtime(false);
    let state = State::new();
    let (funding, owner) = Funding::new::<ProbeFuture<1, 0>>(&state);
    let join = spawn(
        &runtime,
        ProbeFuture::<1, 0>::new(&state, false, true),
        owner,
    );
    assert!(runtime.block_on(join).unwrap_err().is_panic());
    funding.shape::<ProbeFuture<1, 0>>(false, true);
    assert_eq!(state.future_dropped.load(Ordering::SeqCst), 1);
    drop(runtime);
    funding.held();
    drop(state.take_waker());
    funding.returned();
}
