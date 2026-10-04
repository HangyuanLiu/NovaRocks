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
//! The external test pin and Waker targets are not Worker TaskCell claims.
//! No deadline proves reclamation.

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::{Future, Ready, ready};
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};
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
// Phase 1 records the exact constructor identities. Phase 2 counts runtime
// allocations without replacing those identities; every free is still observed.
thread_local! { static TRACK: Cell<u8> = const { Cell::new(0) }; }
static RUNTIME_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static RUNTIME_BYTES: AtomicUsize = AtomicUsize::new(0);
static RUNTIME_REALLOCS: AtomicUsize = AtomicUsize::new(0);
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
    let phase = TRACK.try_with(Cell::get).unwrap_or(0);
    if phase == 2 {
        RUNTIME_ALLOCS.fetch_add(1, Ordering::SeqCst);
        RUNTIME_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }
    if phase == 1 {
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
        if TRACK.try_with(Cell::get).unwrap_or(0) == 2 {
            RUNTIME_REALLOCS.fetch_add(1, Ordering::SeqCst);
        }
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
        TRACK.with(|track| track.set(0));
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
    assert_eq!(TRACK.with(|track| track.replace(1)), 0);
    let guard = Tracking;
    let result = operation();
    drop(guard);
    result
}

fn runtime_measure<T>(operation: impl FnOnce() -> T) -> (T, usize, usize, usize) {
    RUNTIME_ALLOCS.store(0, Ordering::SeqCst);
    RUNTIME_BYTES.store(0, Ordering::SeqCst);
    RUNTIME_REALLOCS.store(0, Ordering::SeqCst);
    assert_eq!(TRACK.with(|track| track.replace(2)), 0);
    let guard = Tracking;
    let result = operation();
    drop(guard);
    (
        result,
        RUNTIME_ALLOCS.load(Ordering::SeqCst),
        RUNTIME_BYTES.load(Ordering::SeqCst),
        RUNTIME_REALLOCS.load(Ordering::SeqCst),
    )
}
fn requested_bytes() -> usize {
    let count = COUNT.load(Ordering::SeqCst);
    assert!(count > 0 && count <= RECORDS.len());
    RECORDS[..count]
        .iter()
        .map(|record| record.bytes.load(Ordering::SeqCst))
        .sum()
}

#[derive(Default)]
struct ExitState {
    exited: AtomicBool,
    watch_constructor: AtomicBool,
}
struct OriginalExit {
    credit: Option<ResultWriteCredit>,
    state: Arc<ExitState>,
}
impl Drop for OriginalExit {
    fn drop(&mut self) {
        if self.state.watch_constructor.load(Ordering::SeqCst) {
            let count = COUNT.load(Ordering::SeqCst);
            assert!(count > 0 && count <= RECORDS.len());
            for record in &RECORDS[..count] {
                assert!(
                    record.freed.load(Ordering::SeqCst),
                    "original credit returned before constructor backing physically exited"
                );
            }
        }
        self.state.exited.store(true, Ordering::SeqCst);
        drop(self.credit.take());
    }
}
struct Funding {
    budget: Arc<ResultRetainedBudget>,
    owner: Option<Bytes>,
    exit: Arc<ExitState>,
    total: usize,
}
impl Funding {
    fn new<S, R>() -> Self
    where
        S: Service<R>,
        S::Error: Into<tower_buffer_04::BoxError>,
    {
        let total = Buffer::<S, R>::common_metadata_capacity_bound()
            .unwrap()
            .checked_add(Buffer::<S, R>::queue_metadata_capacity_bound(PORTS).unwrap())
            .unwrap()
            .checked_add(
                Buffer::<S, R>::response_cell_total_capacity_bound()
                    .unwrap()
                    .checked_mul(PORTS)
                    .unwrap(),
            )
            .unwrap()
            .checked_add(Bytes::owner_with_exit_guard_metadata_size::<
                Bytes,
                OriginalExit,
            >())
            .unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("pregrant the exact common, queue and eight-cell constructor graph");
        };
        let exit = Arc::new(ExitState::default());
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            OriginalExit {
                credit: Some(credit),
                state: exit.clone(),
            },
        );
        Self {
            budget,
            owner: Some(owner),
            exit,
            total,
        }
    }
    fn pair<S, R>(
        &self,
        service: S,
    ) -> (
        Buffer<S, R>,
        impl Future<Output = ()> + Send + 'static + use<S, R>,
    )
    where
        S: Service<R> + Send + 'static,
        S::Future: Send,
        S::Error: Into<tower_buffer_04::BoxError> + Send + Sync,
        R: Send + 'static,
    {
        Buffer::<S, R>::pair_with_original_response_cells(
            service,
            PORTS,
            Buffer::<S, R>::response_cell_allocation_capacity_bound().unwrap(),
            self.owner.as_ref().unwrap().clone(),
        )
        .unwrap()
    }
    fn detach(&mut self) {
        drop(self.owner.take());
    }
    fn held(&self) {
        assert!(!self.exit.exited.load(Ordering::SeqCst));
        assert!(matches!(
            self.budget.try_reserve_process(self.total).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn released(&self) {
        assert!(self.exit.exited.load(Ordering::SeqCst));
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("full original capacity remains after actual last holder exit");
        };
        drop(credit);
    }
}

struct ServiceState {
    ready: AtomicBool,
    failed: AtomicBool,
    calls: AtomicUsize,
    values: [AtomicUsize; 128],
}
impl ServiceState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(true),
            failed: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            values: [const { AtomicUsize::new(0) }; 128],
        })
    }
    fn record(&self, value: usize) {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        self.values[index].store(value, Ordering::SeqCst);
    }
}
struct ActualService(Arc<ServiceState>);
impl Service<usize> for ActualService {
    type Response = usize;
    type Error = io::Error;
    type Future = Ready<Result<usize, io::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if self.0.failed.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
        } else if self.0.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn call(&mut self, value: usize) -> Self::Future {
        self.0.record(value);
        ready(Ok(value))
    }
}
type ActualBuffer = Buffer<ActualService, usize>;
type ActualResponse =
    tower_buffer_04::buffer::future::ResponseFuture<Ready<Result<usize, io::Error>>>;
#[derive(Default)]
struct Notified(AtomicUsize);
impl Wake for Notified {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn readiness<S, R>(buffer: &mut Buffer<S, R>, waker: &Waker)
where
    S: Service<R>,
    S::Error: Into<tower_buffer_04::BoxError>,
{
    assert!(matches!(
        buffer.poll_ready(&mut Context::from_waker(waker)),
        Poll::Ready(Ok(()))
    ));
}
fn drive<W: Future<Output = ()>>(worker: &mut Pin<Box<W>>, waker: &Waker) -> Poll<()> {
    worker.as_mut().poll(&mut Context::from_waker(waker))
}
fn warm_outside_measurement() {
    let semaphore = tokio::sync::Semaphore::new(1);
    semaphore.prewarm_allocation_metadata().unwrap();
    let _ = tracing::Span::current();
}

#[test]
fn actual_fixed_constructor_matches_exact_common_plus_queue_queries() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    let state = ServiceState::new();
    let bounds = measure(|| {
        (
            ActualBuffer::common_metadata_capacity_bound().unwrap(),
            ActualBuffer::queue_metadata_capacity_bound(PORTS).unwrap(),
            ActualBuffer::response_cell_total_capacity_bound().unwrap(),
        )
    });
    assert_eq!(
        COUNT.load(Ordering::SeqCst),
        0,
        "actual typed queries allocate nothing"
    );
    let mut funding = Funding::new::<ActualService, usize>();
    let expected = bounds.0 + bounds.1;
    let (mut buffer, worker) = measure(|| funding.pair(ActualService(state)));
    assert_eq!(requested_bytes(), expected);
    let mut worker = Box::pin(worker); // External test pin, not a Worker TaskCell claim.
    funding.exit.watch_constructor.store(true, Ordering::SeqCst);
    funding.detach();
    funding.held();
    let waker = Waker::from(Arc::new(Notified::default()));
    let (_, allocations, _, reallocations) = runtime_measure(|| {
        readiness(&mut buffer, &waker);
        drop(buffer); // Return its reserved, unused permit and close the last sender.
        assert!(drive(&mut worker, &waker).is_ready());
    });
    assert_eq!(allocations, 0);
    assert_eq!(reallocations, 0);
    funding.held(); // Ready(()) is not actual field/backing destruction.
    drop(worker);
    funding.released();
}

#[test]
fn actual_worker_exits_first_and_last_buffer_alias_retains_complete_metadata() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    let state = ServiceState::new();
    let mut funding = Funding::new::<ActualService, usize>();
    let (mut buffer, worker) = measure(|| funding.pair(ActualService(state)));
    let alias = buffer.clone();
    let waker = Waker::from(Arc::new(Notified::default()));
    readiness(&mut buffer, &waker); // Keep a real acquired permit through closure.
    funding.exit.watch_constructor.store(true, Ordering::SeqCst);
    funding.detach();
    drop(worker); // Receiver and Worker Weak exit before either Buffer.
    funding.held();
    assert!(
        buffer
            .poll_ready(&mut Context::from_waker(&waker))
            .is_ready()
    );
    drop(buffer);
    funding.held(); // The final clone still owns queue Core, Semaphore and Handle.
    drop(alias);
    funding.released(); // Includes each actual Arc/PAL and fixed slots free.
}

#[test]
fn actual_ring_wrap_preserves_fifo_and_has_only_prepaid_cell_allocations() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    let state = ServiceState::new();
    let mut funding = Funding::new::<ActualService, usize>();
    let (mut buffer, worker) = measure(|| funding.pair(ActualService(state.clone())));
    let expected = ActualBuffer::common_metadata_capacity_bound().unwrap()
        + ActualBuffer::queue_metadata_capacity_bound(PORTS).unwrap();
    assert_eq!(requested_bytes(), expected);
    let mut worker = Box::pin(worker);
    let waker = Waker::from(Arc::new(Notified::default()));
    funding.exit.watch_constructor.store(true, Ordering::SeqCst);
    let cell = ActualBuffer::response_cell_total_capacity_bound().unwrap();
    let (_, allocations, bytes, reallocations) = runtime_measure(|| {
        for turn in 0..12 {
            let mut responses: [Option<ActualResponse>; PORTS] = std::array::from_fn(|_| None);
            for (index, response) in responses.iter_mut().enumerate() {
                readiness(&mut buffer, &waker);
                *response = Some(buffer.call(turn * PORTS + index));
            }
            assert!(drive(&mut worker, &waker).is_pending());
            for (index, response) in responses.iter_mut().enumerate() {
                assert!(matches!(
                    Pin::new(response.as_mut().unwrap()).poll(&mut Context::from_waker(&waker)),
                    Poll::Ready(Ok(value)) if value == turn * PORTS + index
                ));
                drop(response.take());
            }
        }
    });
    assert_eq!(
        allocations,
        12 * PORTS * 2,
        "only original cell plus exit wrapper allocate"
    );
    assert_eq!(bytes, 12 * PORTS * cell);
    assert_eq!(reallocations, 0);
    assert_eq!(state.calls.load(Ordering::SeqCst), 12 * PORTS);
    for index in 0..12 * PORTS {
        assert_eq!(state.values[index].load(Ordering::SeqCst), index);
    }
    drop(buffer);
    assert!(drive(&mut worker, &waker).is_ready());
    funding.detach();
    funding.held();
    drop(worker);
    funding.released();
}

#[test]
fn actual_cancelled_head_middle_and_pending_current_keep_same_positions() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    let state = ServiceState::new();
    state.ready.store(false, Ordering::SeqCst);
    let mut funding = Funding::new::<ActualService, usize>();
    let (mut buffer, worker) = measure(|| funding.pair(ActualService(state.clone())));
    let mut worker = Box::pin(worker);
    let waker = Waker::from(Arc::new(Notified::default()));
    let mut responses: [Option<ActualResponse>; PORTS] = std::array::from_fn(|_| None);
    for (index, response) in responses.iter_mut().enumerate() {
        readiness(&mut buffer, &waker);
        *response = Some(buffer.call(index));
    }
    drop(responses[0].take());
    drop(responses[3].take());
    assert!(
        buffer
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(drive(&mut worker, &waker).is_pending());
    assert_eq!(state.calls.load(Ordering::SeqCst), 0);
    // Only the actually popped cancelled head released its physical sender.
    readiness(&mut buffer, &waker);
    let mut replacement = buffer.call(PORTS);
    assert!(
        buffer
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(drive(&mut worker, &waker).is_pending());
    state.ready.store(true, Ordering::SeqCst);
    assert!(drive(&mut worker, &waker).is_pending());
    let expected = [1, 2, 4, 5, 6, 7, 8];
    assert_eq!(state.calls.load(Ordering::SeqCst), expected.len());
    for (index, value) in expected.into_iter().enumerate() {
        assert_eq!(state.values[index].load(Ordering::SeqCst), value);
    }
    for response in responses.iter_mut().flatten() {
        assert!(
            Pin::new(response)
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }
    assert!(
        Pin::new(&mut replacement)
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    drop(responses);
    drop(replacement);
    funding.exit.watch_constructor.store(true, Ordering::SeqCst);
    drop(buffer);
    assert!(drive(&mut worker, &waker).is_ready());
    funding.detach();
    funding.held();
    drop(worker);
    funding.released();
}

fn assert_original_service_error(error: &tower_buffer_04::BoxError) {
    let source = error
        .source()
        .expect("original ServiceError has its actual cause");
    assert_eq!(
        source.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::ConnectionReset
    );
}
#[test]
fn actual_service_error_closes_new_sends_and_drains_every_old_reply() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    let state = ServiceState::new();
    state.failed.store(true, Ordering::SeqCst);
    let mut funding = Funding::new::<ActualService, usize>();
    let (mut buffer, worker) = measure(|| funding.pair(ActualService(state.clone())));
    let mut worker = Box::pin(worker);
    let waker = Waker::from(Arc::new(Notified::default()));
    let mut responses: [Option<ActualResponse>; PORTS] = std::array::from_fn(|_| None);
    for (index, response) in responses.iter_mut().enumerate() {
        readiness(&mut buffer, &waker);
        *response = Some(buffer.call(index));
    }
    assert!(drive(&mut worker, &waker).is_ready());
    assert_eq!(state.calls.load(Ordering::SeqCst), 0);
    let Poll::Ready(Err(error)) = buffer.poll_ready(&mut Context::from_waker(&waker)) else {
        panic!("new sends must see the published original error after close");
    };
    assert_original_service_error(&error);
    drop(error);
    for response in responses.iter_mut().flatten() {
        let Poll::Ready(Err(error)) = Pin::new(response).poll(&mut Context::from_waker(&waker))
        else {
            panic!("each queued original sender must receive the same service failure");
        };
        assert_original_service_error(&error);
    }
    drop(responses);
    funding.exit.watch_constructor.store(true, Ordering::SeqCst);
    funding.detach();
    drop(buffer);
    funding.held();
    drop(worker);
    funding.released();
}

#[test]
fn actual_last_sender_wakes_pending_worker_then_eof_preserves_physical_exit() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    let mut funding = Funding::new::<ActualService, usize>();
    let state = ServiceState::new();
    let (buffer, worker) = measure(|| funding.pair(ActualService(state)));
    let last = buffer.clone();
    let mut worker = Box::pin(worker);
    let notified = Arc::new(Notified::default());
    let waker = Waker::from(notified.clone());
    assert!(drive(&mut worker, &waker).is_pending());
    drop(buffer);
    assert!(drive(&mut worker, &waker).is_pending());
    let wakes = notified.0.load(Ordering::SeqCst);
    drop(last);
    assert!(notified.0.load(Ordering::SeqCst) > wakes);
    assert!(drive(&mut worker, &waker).is_ready());
    funding.exit.watch_constructor.store(true, Ordering::SeqCst);
    funding.detach();
    funding.held();
    drop(worker);
    funding.released();
}

struct RequestDrops {
    count: AtomicUsize,
    panic_once: AtomicBool,
}
struct DropRequest {
    id: usize,
    state: Arc<RequestDrops>,
}
impl Drop for DropRequest {
    fn drop(&mut self) {
        self.state.count.fetch_add(1, Ordering::SeqCst);
        if self.id == 0 && self.state.panic_once.swap(false, Ordering::SeqCst) {
            panic!("actual queued request destructor panic");
        }
    }
}
struct RequestService;
impl Service<DropRequest> for RequestService {
    type Response = usize;
    type Error = io::Error;
    type Future = Ready<Result<usize, io::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DropRequest) -> Self::Future {
        ready(Ok(request.id))
    }
}
fn record_additional<T>(operation: impl FnOnce() -> T) -> T {
    assert_eq!(TRACK.with(|track| track.replace(1)), 0);
    let guard = Tracking;
    let value = operation();
    drop(guard);
    value
}

#[test]
fn actual_receiver_drop_request_panic_still_frees_slots_cells_pal_and_arc_before_credit() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    let mut funding = Funding::new::<RequestService, DropRequest>();
    let (mut buffer, worker) = measure(|| funding.pair(RequestService));
    let expected = Buffer::<RequestService, DropRequest>::common_metadata_capacity_bound().unwrap()
        + Buffer::<RequestService, DropRequest>::queue_metadata_capacity_bound(PORTS).unwrap();
    assert_eq!(requested_bytes(), expected);
    let requests = Arc::new(RequestDrops {
        count: AtomicUsize::new(0),
        panic_once: AtomicBool::new(true),
    });
    let waker = Waker::from(Arc::new(Notified::default()));
    for id in 0..4 {
        readiness(&mut buffer, &waker);
        let response = record_additional(|| {
            buffer.call(DropRequest {
                id,
                state: requests.clone(),
            })
        });
        // The actual Msg.tx still holds the original cell until Receiver Drop.
        drop(response);
    }
    funding.exit.watch_constructor.store(true, Ordering::SeqCst);
    funding.detach();
    drop(buffer);
    funding.held();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(worker)));
    assert!(outcome.is_err());
    assert_eq!(requests.count.load(Ordering::SeqCst), 4);
    assert!(!requests.panic_once.load(Ordering::SeqCst));
    funding.released();
}

// Callback state may temporarily own an application Buffer, but the original
// ExitState never does. Explicit take/drop below breaks each registered Waker
// cycle. These callback backings are outside queue metadata/funding assertions.
struct CallbackState {
    buffer: Mutex<Option<ActualBuffer>>,
    response: Mutex<Option<ActualResponse>>,
    sent: AtomicBool,
    clones: AtomicUsize,
    wakes: AtomicUsize,
    drops: AtomicUsize,
}
impl CallbackState {
    fn reenter(&self) {
        let buffer = self.buffer.lock().unwrap().take();
        if let Some(mut buffer) = buffer {
            let waker = Waker::noop();
            readiness(&mut buffer, waker); // Actual is_closed reenters queue lock.
            if !self.sent.swap(true, Ordering::SeqCst) {
                let response = buffer.call(99);
                *self.response.lock().unwrap() = Some(response);
            }
            *self.buffer.lock().unwrap() = Some(buffer);
        }
    }
}
// SAFETY: Each RawWaker data pointer is one Arc::into_raw ownership. clone and
// wake_by_ref borrow via ManuallyDrop; clone publishes a fresh Arc, while wake
// and drop consume exactly the matching one. No queue/Core pointers escape.
unsafe fn callback_clone(pointer: *const ()) -> RawWaker {
    let state =
        std::mem::ManuallyDrop::new(unsafe { Arc::<CallbackState>::from_raw(pointer.cast()) });
    state.clones.fetch_add(1, Ordering::SeqCst);
    state.reenter();
    callback_raw(Arc::clone(&state))
}
unsafe fn callback_wake(pointer: *const ()) {
    let state = unsafe { Arc::<CallbackState>::from_raw(pointer.cast()) };
    state.wakes.fetch_add(1, Ordering::SeqCst);
    state.reenter();
}
unsafe fn callback_wake_by_ref(pointer: *const ()) {
    let state =
        std::mem::ManuallyDrop::new(unsafe { Arc::<CallbackState>::from_raw(pointer.cast()) });
    state.wakes.fetch_add(1, Ordering::SeqCst);
    state.reenter();
}
unsafe fn callback_drop(pointer: *const ()) {
    let state = unsafe { Arc::<CallbackState>::from_raw(pointer.cast()) };
    state.drops.fetch_add(1, Ordering::SeqCst);
    state.reenter();
}
static CALLBACK_VTABLE: RawWakerVTable = RawWakerVTable::new(
    callback_clone,
    callback_wake,
    callback_wake_by_ref,
    callback_drop,
);
fn callback_raw(state: Arc<CallbackState>) -> RawWaker {
    RawWaker::new(Arc::into_raw(state).cast(), &CALLBACK_VTABLE)
}

#[test]
fn actual_waker_clone_send_wake_and_drop_reentry_run_outside_queue_lock() {
    let _serial = SERIAL.lock().unwrap();
    warm_outside_measurement();
    // Bound a regression which executes a callback under the queue mutex. A
    // timeout diagnoses that lock regression; normal reclamation is joined and
    // checked through actual allocator/free and the complete original budget.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let job = std::thread::spawn(move || {
        let mut funding = Funding::new::<ActualService, usize>();
        let service = ServiceState::new();
        let (mut buffer, worker) = measure(|| funding.pair(ActualService(service.clone())));
        let callbacks = Arc::new(CallbackState {
            buffer: Mutex::new(Some(buffer.clone())),
            response: Mutex::new(None),
            sent: AtomicBool::new(false),
            clones: AtomicUsize::new(0),
            wakes: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        });
        let weak = Arc::downgrade(&callbacks);
        // SAFETY: callback_raw supplies the Arc-backed exact vtable above.
        let waker = unsafe { Waker::from_raw(callback_raw(callbacks.clone())) };
        let mut worker = Box::pin(worker);
        // Waker clone sends while the empty queue registration is outside its
        // lock. The second actual queue check must see this same-poll message.
        assert!(drive(&mut worker, &waker).is_pending());
        assert!(callbacks.clones.load(Ordering::SeqCst) > 0);
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.values[0].load(Ordering::SeqCst), 99);
        let mut response = callbacks.response.lock().unwrap().take().unwrap();
        assert!(matches!(
            Pin::new(&mut response).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(99))
        ));
        drop(response);
        readiness(&mut buffer, Waker::noop());
        let mut response = buffer.call(100); // Takes and invokes stored queue Waker.
        assert!(callbacks.wakes.load(Ordering::SeqCst) > 0);
        assert!(drive(&mut worker, &waker).is_pending());
        assert!(matches!(
            Pin::new(&mut response).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(100))
        ));
        drop(response);
        // Another real registration replaces a stored custom Waker, whose
        // Drop reenters queue is_closed without running under its state lock.
        assert!(drive(&mut worker, Waker::noop()).is_pending());
        assert!(callbacks.drops.load(Ordering::SeqCst) > 0);
        let callback_buffer = callbacks.buffer.lock().unwrap().take();
        drop(callback_buffer);
        drop(buffer);
        assert!(drive(&mut worker, Waker::noop()).is_ready());
        funding.exit.watch_constructor.store(true, Ordering::SeqCst);
        funding.detach();
        funding.held();
        drop(worker);
        funding.released();
        drop(waker);
        drop(callbacks);
        assert!(
            weak.upgrade().is_none(),
            "temporary callback ownership cycle was explicitly cleared"
        );
        done_tx.send(()).unwrap();
    });
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .is_ok(),
        "actual queue callback reentry did not complete outside the lock"
    );
    job.join().unwrap();
}
