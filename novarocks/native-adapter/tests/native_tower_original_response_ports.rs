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

//! Actual Tower Buffer response ports, sharing one original Worker budget.
//! Cell+exit-wrapper capacity is pregranted for all eight simultaneous ports.
//! The current original constructor also prepays common and fixed FIFO metadata.
//! External service futures and scheduler backing remain separate obligations.
//! One test separately funds the real Worker
//! TaskCell through Tokio's actual typed constructor query, without attributing
//! that TaskCell to Tower's response-cell query. No deadline proves reclamation.

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use tower::Service;
use tower_buffer_04::buffer::Buffer;

const PORTS: usize = 8;
static SERIAL: Mutex<()> = Mutex::new(());

struct Allocation {
    pointer: AtomicUsize,
    bytes: AtomicUsize,
    freed: AtomicBool,
}
impl Allocation {
    const fn new() -> Self {
        Self {
            pointer: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            freed: AtomicBool::new(false),
        }
    }
}
static RECORDS: [Allocation; 32] = [const { Allocation::new() }; 32];
static COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOCATOR_GATE: AtomicBool = AtomicBool::new(false);
thread_local! { static TRACK: Cell<bool> = const { Cell::new(false) }; }
struct AllocationGuard;
impl AllocationGuard {
    fn enter() -> Self {
        while ALLOCATOR_GATE
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Self
    }
}
impl Drop for AllocationGuard {
    fn drop(&mut self) {
        ALLOCATOR_GATE.store(false, Ordering::Release);
    }
}
fn record(pointer: *mut u8, bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let index = COUNT.fetch_add(1, Ordering::SeqCst);
        if let Some(slot) = RECORDS.get(index) {
            slot.bytes.store(bytes, Ordering::SeqCst);
            slot.pointer.store(pointer as usize, Ordering::SeqCst);
        }
    }
}
fn retired(pointer: *mut u8) {
    for slot in &RECORDS {
        if slot.pointer.load(Ordering::SeqCst) == pointer as usize {
            slot.freed.store(true, Ordering::SeqCst);
        }
    }
}
struct Probe;
#[global_allocator]
static ALLOCATOR: Probe = Probe;
// SAFETY: Forward unchanged requests to System. The test-only fixed atomic
// address ledger never dereferences an allocation; its gate serializes actual
// frees against publication of a reused address and cannot allocate/reenter.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _gate = AllocationGuard::enter();
        let pointer = unsafe { System.alloc(layout) };
        record(pointer, layout.size());
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let _gate = AllocationGuard::enter();
        let pointer = unsafe { System.alloc_zeroed(layout) };
        record(pointer, layout.size());
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let _gate = AllocationGuard::enter();
        unsafe { System.dealloc(pointer, layout) };
        retired(pointer);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let _gate = AllocationGuard::enter();
        let next = unsafe { System.realloc(pointer, layout, bytes) };
        if !next.is_null() {
            retired(pointer);
            record(next, bytes);
        }
        next
    }
}
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|track| track.set(false));
    }
}
fn measure<T>(operation: impl FnOnce() -> T) -> T {
    {
        let _gate = AllocationGuard::enter();
        COUNT.store(0, Ordering::SeqCst);
        for slot in &RECORDS {
            slot.pointer.store(0, Ordering::SeqCst);
            slot.bytes.store(0, Ordering::SeqCst);
            slot.freed.store(false, Ordering::SeqCst);
        }
    }
    assert!(!TRACK.with(|track| track.replace(true)));
    let guard = Tracking;
    let result = operation();
    drop(guard);
    result
}

#[derive(Default)]
struct State {
    calls: AtomicUsize,
    future_polls: AtomicUsize,
    future_drops: AtomicUsize,
    worker_polls: AtomicUsize,
    worker_waker: Mutex<Option<Waker>>,
    original_exited: AtomicBool,
}
struct ServiceFuture {
    state: Arc<State>,
    value: usize,
}
impl Future for ServiceFuture {
    type Output = Result<usize, io::Error>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.future_polls.fetch_add(1, Ordering::SeqCst);
        let _ = self.value;
        Poll::Pending
    }
}
impl Drop for ServiceFuture {
    fn drop(&mut self) {
        self.state.future_drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct ActualService(Arc<State>);
impl Service<usize> for ActualService {
    type Response = usize;
    type Error = io::Error;
    type Future = ServiceFuture;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.0.worker_polls.fetch_add(1, Ordering::SeqCst);
        *self.0.worker_waker.lock().unwrap() = Some(cx.waker().clone());
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, value: usize) -> ServiceFuture {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        ServiceFuture {
            state: self.0.clone(),
            value,
        }
    }
}
type ActualBuffer = Buffer<ActualService, usize>;
fn worker_task_bound<W>(_: fn(ActualService, usize) -> (ActualBuffer, W)) -> io::Result<usize>
where
    W: Future<Output = ()> + Send + 'static,
{
    tokio::runtime::Handle::task_allocation_capacity_bound::<W>()
}

struct Exit {
    credit: Option<ResultWriteCredit>,
    state: Arc<State>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        self.state.original_exited.store(true, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
struct Funding {
    budget: Arc<ResultRetainedBudget>,
    owner: Option<Bytes>,
    state: Arc<State>,
    total: usize,
}
impl Funding {
    fn new(task_bytes: usize) -> Self {
        let state = Arc::new(State::default());
        let total = PORTS
            .checked_mul(ActualBuffer::response_cell_total_capacity_bound().unwrap())
            .unwrap()
            .checked_add(ActualBuffer::common_metadata_capacity_bound().unwrap())
            .unwrap()
            .checked_add(ActualBuffer::queue_metadata_capacity_bound(PORTS).unwrap())
            .unwrap()
            .checked_add(Bytes::owner_with_exit_guard_metadata_size::<Bytes, Exit>())
            .unwrap()
            .checked_add(task_bytes)
            .unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("pregrant all original response cells before constructing the Buffer");
        };
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            Exit {
                credit: Some(credit),
                state: state.clone(),
            },
        );
        Self {
            budget,
            owner: Some(owner),
            state,
            total,
        }
    }
    fn pair(
        &self,
    ) -> (
        ActualBuffer,
        impl Future<Output = ()> + Send + 'static + use<>,
    ) {
        ActualBuffer::pair_with_original_response_cells(
            ActualService(self.state.clone()),
            PORTS,
            ActualBuffer::response_cell_allocation_capacity_bound().unwrap(),
            self.owner.as_ref().unwrap().clone(),
        )
        .unwrap()
    }
    fn detach(&mut self) {
        drop(self.owner.take());
    }
    fn held(&self) {
        assert!(!self.state.original_exited.load(Ordering::SeqCst));
        assert!(matches!(
            self.budget.try_reserve_process(self.total).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn released(&self) {
        assert!(self.state.original_exited.load(Ordering::SeqCst));
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("original credit remains after all physical holders exited");
        };
        drop(credit);
    }
}
#[derive(Default)]
struct Notified {
    wakes: AtomicUsize,
    watched_cell: AtomicUsize,
    early_wake: AtomicBool,
}
impl Notified {
    fn notify(&self) {
        let watched = self.watched_cell.load(Ordering::SeqCst);
        if watched != 0 && !RECORDS[watched - 1].freed.load(Ordering::SeqCst) {
            self.early_wake.store(true, Ordering::SeqCst);
        }
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
impl Wake for Notified {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}
fn ready(buffer: &mut ActualBuffer, waker: &Waker) {
    assert!(matches!(
        buffer.poll_ready(&mut Context::from_waker(waker)),
        Poll::Ready(Ok(()))
    ));
}
fn drive<W: Future<Output = ()>>(worker: &mut Pin<Box<W>>, waker: &Waker) {
    assert!(
        worker
            .as_mut()
            .poll(&mut Context::from_waker(waker))
            .is_pending()
    );
}

#[test]
fn all_eight_original_ports_survive_worker_transfer_across_buffer_clones() {
    let _serial = SERIAL.lock().unwrap();
    let mut funding = Funding::new(0);
    let (buffer, worker) = funding.pair();
    funding.detach();
    funding.held(); // Empty Buffer alone retains the original; Worker does not.

    let mut worker = Box::pin(worker); // Test pin backing is outside the receipt.
    let notified = Arc::new(Notified::default());
    let waker = Waker::from(notified.clone());
    let mut clones: Vec<_> = (0..PORTS).map(|_| buffer.clone()).collect();
    let mut responses = Vec::with_capacity(PORTS);
    for (index, clone) in clones.iter_mut().enumerate() {
        ready(clone, &waker);
        responses.push(clone.call(index));
    }
    drive(&mut worker, &waker);
    assert_eq!(funding.state.calls.load(Ordering::SeqCst), PORTS);
    assert_eq!(funding.state.future_polls.load(Ordering::SeqCst), 0);
    let mut ninth = buffer.clone();
    assert!(
        ninth
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending(),
        "message/queue exit cannot return original response-cell positions"
    );
    let wakes = notified.wakes.load(Ordering::SeqCst);
    drop(responses.pop().unwrap());
    assert_eq!(funding.state.future_drops.load(Ordering::SeqCst), 1);
    assert!(notified.wakes.load(Ordering::SeqCst) > wakes);
    ready(&mut ninth, &waker);
    drop(ninth); // Return its unused readiness reservation.
    drop(buffer);
    drop(clones);
    funding.detach();
    funding.held(); // Worker transferred every message; only old cells remain.
    drop(worker);
    funding.held();
    drop(responses);
    assert_eq!(funding.state.future_drops.load(Ordering::SeqCst), PORTS);
    funding.released();
}

#[test]
fn actual_response_cell_is_freed_before_position_wake_and_external_future_stays_pending() {
    let _serial = SERIAL.lock().unwrap();
    let mut funding = Funding::new(0);
    let (mut buffer, worker) = funding.pair();
    let mut worker = Box::pin(worker);
    let notified = Arc::new(Notified::default());
    let waker = Waker::from(notified.clone());
    let mut responses = Vec::with_capacity(PORTS);
    ready(&mut buffer, &waker);
    let first = measure(|| buffer.call(0));
    let cell_bytes = ActualBuffer::response_cell_allocation_capacity_bound().unwrap();
    let cells: Vec<_> = RECORDS
        .iter()
        .enumerate()
        .filter(|(_, slot)| {
            slot.pointer.load(Ordering::SeqCst) != 0
                && slot.bytes.load(Ordering::SeqCst) == cell_bytes
        })
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        cells.len(),
        1,
        "identify the actual typed oneshot allocation in this call"
    );
    assert!(COUNT.load(Ordering::SeqCst) <= RECORDS.len());
    responses.push(first);
    for index in 1..PORTS {
        ready(&mut buffer, &waker);
        responses.push(buffer.call(index));
    }
    drive(&mut worker, &waker);
    assert!(
        buffer
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(!RECORDS[cells[0]].freed.load(Ordering::SeqCst));
    notified.watched_cell.store(cells[0] + 1, Ordering::SeqCst);
    // Taking the service F out of the Rx physically retires the response cell,
    // but does not finish or fund that unrelated service future.
    assert!(
        Pin::new(&mut responses[0])
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(RECORDS[cells[0]].freed.load(Ordering::SeqCst));
    assert!(
        !notified.early_wake.load(Ordering::SeqCst),
        "no permit callback before actual cell free"
    );
    assert_eq!(funding.state.future_polls.load(Ordering::SeqCst), 1);
    assert_eq!(funding.state.future_drops.load(Ordering::SeqCst), 0);
    ready(&mut buffer, &waker);
    drop(buffer);
    funding.detach();
    drop(worker);
    funding.held();
    drop(responses);
    funding.released();
}

#[test]
fn short_response_cell_bound_refuses_before_buffer_construction_and_query_allocates_nothing() {
    let _serial = SERIAL.lock().unwrap();
    let mut funding = Funding::new(0);
    let bounds = measure(|| {
        (
            ActualBuffer::response_cell_allocation_capacity_bound(),
            ActualBuffer::response_cell_total_capacity_bound(),
        )
    });
    assert_eq!(COUNT.load(Ordering::SeqCst), 0);
    let cell = bounds.0.unwrap();
    assert!(bounds.1.unwrap() > cell);
    let service = ActualService(funding.state.clone());
    let owner = funding.owner.as_ref().unwrap().clone();
    let result = measure(|| {
        ActualBuffer::pair_with_original_response_cells(service, PORTS, cell - 1, owner)
    });
    assert_eq!(
        COUNT.load(Ordering::SeqCst),
        0,
        "refuse before queue/semaphore/Handle backing"
    );
    assert!(result.is_err());
    for positions in [0, tokio::sync::Semaphore::MAX_PERMITS + 1, usize::MAX] {
        let service = ActualService(funding.state.clone());
        let owner = funding.owner.as_ref().unwrap().clone();
        let result = measure(|| {
            ActualBuffer::pair_with_original_response_cells(service, positions, cell, owner)
        });
        assert_eq!(COUNT.load(Ordering::SeqCst), 0);
        assert_eq!(result.err().unwrap().kind(), io::ErrorKind::InvalidInput);
    }
    assert_eq!(funding.state.worker_polls.load(Ordering::SeqCst), 0);
    assert_eq!(funding.state.calls.load(Ordering::SeqCst), 0);
    funding.detach();
    funding.released();
}

#[test]
fn legacy_pair_returns_queue_permits_when_worker_transfers_future() {
    let _serial = SERIAL.lock().unwrap();
    let state = Arc::new(State::default());
    let (mut buffer, worker) = ActualBuffer::pair(ActualService(state.clone()), PORTS);
    let mut worker = Box::pin(worker);
    let waker = Waker::from(Arc::new(Notified::default()));
    let mut responses = Vec::with_capacity(PORTS);
    for index in 0..PORTS {
        ready(&mut buffer, &waker);
        responses.push(buffer.call(index));
    }
    drive(&mut worker, &waker);
    ready(&mut buffer, &waker);
    assert_eq!(state.calls.load(Ordering::SeqCst), PORTS);
    assert_eq!(state.future_polls.load(Ordering::SeqCst), 0);
    drop(buffer);
    drop(worker);
    drop(responses);
}

#[test]
fn detached_original_generation_survives_real_worker_join_and_last_task_waker() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let task_bound = worker_task_bound(ActualBuffer::pair).unwrap();
    let mut funding = Funding::new(task_bound);
    let (mut buffer, worker) = funding.pair();
    let task_owner = funding.owner.as_ref().unwrap().clone();
    let join = runtime
        .handle()
        .spawn_with_task_owner(worker, task_owner)
        .unwrap();
    let waker = Waker::from(Arc::new(Notified::default()));
    ready(&mut buffer, &waker);
    let response = buffer.call(9);
    // Poll the real Worker task until its actual Service readiness hook captures
    // that task's Waker. The caller Rx stays unpolled throughout.
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while funding.state.calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("watchdog: actual Worker never dispatched its request");
    });
    let worker_waker = funding.state.worker_waker.lock().unwrap().take().unwrap();
    drop(buffer);
    funding.detach();
    funding.held();
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), join)
            .await
            .expect("watchdog: actual Worker did not exit after last Buffer drop")
            .unwrap();
    }); // Consumption of Join is not physical exit.
    drop(response);
    funding.held(); // Only the real, completed Worker Cell's Waker remains.
    drop(runtime);
    funding.held();
    drop(worker_waker);
    funding.released();
}
