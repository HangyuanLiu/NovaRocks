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

//! Actual shared Semaphore/Handle metadata probes.
//! Queues, readiness Boxes, service/error payloads, external Wakers and Worker
//! task/pin backing remain outside this funding receipt. Pair allocations are
//! observed only to distinguish exact common metadata and real final exit.

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::{Future, Ready, ready};
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use tokio::sync::Semaphore;
use tower::Service;
use tower_buffer_04::buffer::{Buffer, error::ServiceError};

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

fn measure_additional<T>(operation: impl FnOnce() -> T) -> T {
    // Retain the original allocation identities while observing repeat calls.
    assert!(!TRACK.with(|track| track.replace(true)));
    let guard = Tracking;
    let result = operation();
    drop(guard);
    result
}

#[derive(Default)]
struct State {
    calls: AtomicUsize,
    exited: AtomicBool,
    watch_pair: AtomicBool,
}
struct ActualService(Arc<State>);
impl Service<usize> for ActualService {
    type Response = usize;
    type Error = io::Error;
    type Future = Ready<Result<usize, io::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, value: usize) -> Self::Future {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok(value))
    }
}
type ActualBuffer = Buffer<ActualService, usize>;
#[derive(Default)]
struct Notified;
impl Wake for Notified {
    fn wake(self: Arc<Self>) {}
}

struct Exit {
    credit: Option<ResultWriteCredit>,
    state: Arc<State>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        if self.state.watch_pair.load(Ordering::SeqCst) {
            let count = COUNT.load(Ordering::SeqCst);
            assert!(count > 0 && count <= RECORDS.len());
            for allocation in &RECORDS[..count] {
                assert!(
                    allocation.freed.load(Ordering::SeqCst),
                    "common original returned before actual pair allocation exit"
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
    state: Arc<State>,
    total: usize,
}
impl Funding {
    fn new() -> Self {
        let state = Arc::new(State::default());
        let total = ActualBuffer::common_metadata_capacity_bound()
            .unwrap()
            .checked_add(
                ActualBuffer::response_cell_total_capacity_bound()
                    .unwrap()
                    .checked_mul(PORTS)
                    .unwrap(),
            )
            .unwrap()
            .checked_add(Bytes::owner_with_exit_guard_metadata_size::<Bytes, Exit>())
            .unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(total).unwrap()
        else {
            panic!("pregrant actual common metadata and response-cell capacity");
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
        assert!(!self.state.exited.load(Ordering::SeqCst));
        assert!(matches!(
            self.budget.try_reserve_process(self.total).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn released(&self) {
        assert!(self.state.exited.load(Ordering::SeqCst));
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("common original did not return after final physical holder exit");
        };
        drop(credit);
    }
}
fn requested_bytes() -> usize {
    let count = COUNT.load(Ordering::SeqCst);
    assert!(count <= RECORDS.len());
    RECORDS[..count]
        .iter()
        .map(|allocation| allocation.bytes.load(Ordering::SeqCst))
        .sum()
}
fn actual_arc_layout<T>() -> usize {
    // Actual public payload types and pinned std Arc prefix, not a substitute
    // private algorithm or guessed container size. System measures these next.
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .unwrap()
        .0
        .pad_to_align()
        .size()
}
fn warm_shared_parking_outside_measurement() {
    // parking_lot's shared runtime resources are explicitly outside per-Mutex
    // backing. Initialize those before measuring another real Semaphore.
    let semaphore = Semaphore::new(1);
    semaphore.prewarm_allocation_metadata().unwrap();
}

#[test]
fn actual_common_queries_allocate_nothing_and_unknown_abi_refuses() {
    let _serial = SERIAL.lock().unwrap();
    let (semaphore, common) = measure(|| {
        (
            Semaphore::allocation_capacity_bound(),
            ActualBuffer::common_metadata_capacity_bound(),
        )
    });
    assert_eq!(COUNT.load(Ordering::SeqCst), 0);
    if cfg!(any(
        all(target_os = "macos", target_pointer_width = "64"),
        all(target_os = "linux", target_has_atomic = "32")
    )) {
        let semaphore = semaphore.unwrap();
        assert!(common.unwrap() > semaphore);
    } else {
        assert_eq!(common.err().unwrap().kind(), io::ErrorKind::Unsupported);
        // A supported inline parking_lot implementation can require no private
        // PAL, but Tower's actual std Handle must still refuse an unknown ABI.
        if let Err(error) = semaphore {
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        }
    }
}

#[test]
fn actual_semaphore_arc_and_prewarm_match_query_without_changing_state() {
    let _serial = SERIAL.lock().unwrap();
    if let Err(error) = ActualBuffer::common_metadata_capacity_bound() {
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        return;
    }
    warm_shared_parking_outside_measurement();
    let bound = Semaphore::allocation_capacity_bound().unwrap();
    let semaphore = measure(|| {
        let semaphore = Arc::new(Semaphore::new(PORTS));
        semaphore.prewarm_allocation_metadata().unwrap();
        semaphore
    });
    assert_eq!(
        requested_bytes(),
        bound,
        "actual Arc plus actual first-lock PAL matches static query"
    );
    let count = COUNT.load(Ordering::SeqCst);
    assert!(count > 0);
    assert_eq!(semaphore.available_permits(), PORTS);
    assert!(!semaphore.is_closed());
    let weak = Arc::downgrade(&semaphore);
    let permits = measure_additional(|| {
        semaphore.prewarm_allocation_metadata().unwrap();
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        assert_eq!(semaphore.available_permits(), PORTS - 1);
        drop(permit);
        assert!(!semaphore.is_closed());
        semaphore.available_permits()
    });
    assert_eq!(
        COUNT.load(Ordering::SeqCst),
        count,
        "repeat prewarm and no-waiter owned acquire/release allocate no private backing"
    );
    assert_eq!(permits, PORTS);
    drop(semaphore);
    assert!(weak.upgrade().is_none());
    assert!(
        RECORDS[..count]
            .iter()
            .any(|allocation| !allocation.freed.load(Ordering::SeqCst)),
        "final Weak still holds the actual Arc allocation after Semaphore data exits"
    );
    drop(weak);
    assert!(
        RECORDS[..count]
            .iter()
            .all(|allocation| allocation.freed.load(Ordering::SeqCst)),
        "all actual Arc/PAL backing exits only after last Weak"
    );
}

#[test]
fn actual_owned_pair_prewarm_delta_matches_both_real_mutex_backings() {
    let _serial = SERIAL.lock().unwrap();
    if let Err(error) = ActualBuffer::common_metadata_capacity_bound() {
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        return;
    }
    warm_shared_parking_outside_measurement();
    let mut funding = Funding::new();
    let state = funding.state.clone();
    let ordinary = measure(|| ActualBuffer::pair(ActualService(state), PORTS));
    let ordinary_bytes = requested_bytes();
    let ordinary_count = COUNT.load(Ordering::SeqCst);
    assert!(
        ordinary_count > 0,
        "actual ordinary constructor is a positive allocator control"
    );
    drop(ordinary);
    let owned = measure(|| funding.pair());
    let owned_bytes = requested_bytes();
    assert!(COUNT.load(Ordering::SeqCst) >= ordinary_count);
    let arc_bytes = actual_arc_layout::<Semaphore>()
        .checked_add(actual_arc_layout::<Mutex<Option<ServiceError>>>())
        .unwrap();
    let actual_private_mutex_bytes = owned_bytes.checked_sub(ordinary_bytes).unwrap();
    assert_eq!(
        arc_bytes.checked_add(actual_private_mutex_bytes).unwrap(),
        ActualBuffer::common_metadata_capacity_bound().unwrap(),
        "identical real queue/Arc constructors cancel only for this comparison; observed first locks supply PAL bytes"
    );
    // This lifetime check observes actual pair allocations, without charging or
    // claiming a finite MPSC block graph. No requests grow that graph here.
    funding.state.watch_pair.store(true, Ordering::SeqCst);
    funding.detach();
    funding.held();
    drop(owned);
    funding.released();
}

#[test]
fn actual_worker_retains_common_original_after_buffer_and_all_cells_exit() {
    let _serial = SERIAL.lock().unwrap();
    if let Err(error) = ActualBuffer::common_metadata_capacity_bound() {
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        return;
    }
    warm_shared_parking_outside_measurement();
    let mut funding = Funding::new();
    let (mut buffer, worker) = measure(|| funding.pair());
    funding.state.watch_pair.store(true, Ordering::SeqCst);
    // The test pin Box itself is caller-owned scratch, not common metadata or
    // a funded Worker TaskCell. Actual production task funding is independent.
    let mut worker = Box::pin(worker);
    let waker = Waker::from(Arc::new(Notified));
    assert!(matches!(
        buffer.poll_ready(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(()))
    ));
    let response = buffer.call(7);
    assert!(
        worker
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(funding.state.calls.load(Ordering::SeqCst), 1);
    drop(response); // The real sender has transferred F; the final Rx now exits.
    drop(buffer);
    funding.detach();
    funding.held(); // Only the real Worker and its shared Handle/Weak remain.
    assert!(
        worker
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    funding.held(); // Completion alone is not destruction of those real fields.
    drop(worker);
    funding.released();
}

#[test]
fn actual_none_pair_has_no_original_owner_and_worker_still_owns_shared_metadata() {
    let _serial = SERIAL.lock().unwrap();
    if let Err(error) = ActualBuffer::common_metadata_capacity_bound() {
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        return;
    }
    let mut funding = Funding::new();
    let state = funding.state.clone();
    let (buffer, worker) = measure(|| ActualBuffer::pair(ActualService(state), PORTS));
    let count = COUNT.load(Ordering::SeqCst);
    assert!(count > 0 && count <= RECORDS.len());
    // Ordinary pair receives no Bytes. Sharing the ordinary Service state does
    // not infer ownership of its unrelated, separately granted capability.
    funding.detach();
    funding.released();
    drop(buffer);
    assert!(
        RECORDS[..count]
            .iter()
            .any(|allocation| !allocation.freed.load(Ordering::SeqCst)),
        "ordinary Worker still owns actual queue/Handle/Weak metadata"
    );
    drop(worker);
    assert!(
        RECORDS[..count]
            .iter()
            .all(|allocation| allocation.freed.load(Ordering::SeqCst)),
        "ordinary metadata still follows actual Worker destruction"
    );
}
