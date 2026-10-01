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

//! Requested Rust allocator layouts under pinned std 1.92, isolated per test
//! thread. Callback closure/Arc owners are prepared outside every probe. These
//! receipts exclude libc RSS/TLS and separately owned caller/root metadata.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use novarocks_execution::runtime::observable::{
    Observable, ObservableCapacityError, Observer, ObserverSubscription,
};

#[derive(Clone, Copy, Default, Debug)]
struct Allocations {
    calls: usize,
    requested_bytes: usize,
    maximum_request: usize,
}
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static COUNTERS: Cell<Allocations> = const { Cell::new(Allocations { calls: 0, requested_bytes: 0, maximum_request: 0 }) };
}
struct Probe;
fn record(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = COUNTERS.try_with(|counter| {
            let mut value = counter.get();
            value.calls = value.calls.saturating_add(1);
            value.requested_bytes = value.requested_bytes.saturating_add(bytes);
            value.maximum_request = value.maximum_request.max(bytes);
            counter.set(value);
        });
    }
}
// SAFETY: Requests are forwarded unchanged to System. Observation touches only
// allocation-free thread-local numeric cells, never allocation contents.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct StopTracking;
impl Drop for StopTracking {
    fn drop(&mut self) {
        TRACK.with(|track| track.set(false));
    }
}
fn measure<T>(f: impl FnOnce() -> T) -> (T, Allocations) {
    COUNTERS.with(|counter| counter.set(Allocations::default()));
    TRACK.with(|track| track.set(true));
    let stop = StopTracking;
    let result = f();
    drop(stop);
    (result, COUNTERS.with(Cell::get))
}
fn no_allocation<T>(f: impl FnOnce() -> T) -> T {
    let (result, actual) = measure(f);
    assert_eq!(actual.calls, 0, "bounded operation allocated: {actual:?}");
    assert_eq!(actual.requested_bytes, 0);
    result
}
fn counted(counter: &Arc<AtomicUsize>) -> Observer {
    let counter = Arc::clone(counter);
    Arc::new(move || {
        counter.fetch_add(1, Ordering::Relaxed);
    })
}

#[test]
fn bounded_constructor_requested_heap_fits_pregrant_without_hidden_slot_growth() {
    for capacity in [0, 1, 2, 64, 129] {
        let bound = no_allocation(|| Observable::bounded_backing_bytes(capacity)).unwrap();
        let (observable, actual) = measure(|| Observable::try_bounded(capacity));
        let observable = observable.unwrap();
        // Self is a stack value here. No Arc<Observable> wrapper or closure is
        // constructed inside this measurement, so the heap-only bound suffices.
        assert!(
            actual.requested_bytes <= bound,
            "capacity={capacity}, bound={bound}, actual={actual:?}, inline Self={}",
            size_of::<Observable>()
        );
        assert!(actual.maximum_request <= bound);
        if capacity != 0 {
            assert!(
                actual.calls >= capacity,
                "fixed registration Arc slots must already exist"
            );
        }
        no_allocation(|| observable.notify_observers());
    }
}

#[test]
fn invalid_bounded_capacity_rejects_before_allocating() {
    for capacity in [130, usize::MAX] {
        assert_eq!(
            no_allocation(|| Observable::bounded_backing_bytes(capacity)),
            Err(ObservableCapacityError::InvalidCapacity)
        );
        assert!(matches!(
            no_allocation(|| Observable::try_bounded(capacity)),
            Err(ObservableCapacityError::InvalidCapacity)
        ));
    }
}

#[test]
fn sixty_four_sink_and_finish_callbacks_plus_one_scoped_share_exact_capacity() {
    let observable = Observable::try_bounded(129).unwrap();
    let sink_count = Arc::new(AtomicUsize::new(0));
    let finish_count = Arc::new(AtomicUsize::new(0));
    let scoped_count = Arc::new(AtomicUsize::new(0));
    let sink = counted(&sink_count);
    let finish = counted(&finish_count);
    let scoped = counted(&scoped_count);
    let overflow: Observer = Arc::new(|| {});
    for _ in 0..64 {
        no_allocation(|| observable.try_add_observer(Arc::clone(&sink))).unwrap();
        no_allocation(|| observable.try_add_observer(Arc::clone(&finish))).unwrap();
    }
    let subscription = no_allocation(|| observable.try_subscribe(Arc::clone(&scoped))).unwrap();
    assert_eq!(observable.num_observers(), 128);
    assert_eq!(
        no_allocation(|| observable.try_add_observer(Arc::clone(&overflow))),
        Err(ObservableCapacityError::CapacityExceeded)
    );
    assert!(matches!(
        no_allocation(|| observable.try_subscribe(Arc::clone(&overflow))),
        Err(ObservableCapacityError::CapacityExceeded)
    ));
    no_allocation(|| {
        for _ in 0..256 {
            observable.notify_observers();
        }
    });
    assert_eq!(sink_count.load(Ordering::Relaxed), 64 * 256);
    assert_eq!(finish_count.load(Ordering::Relaxed), 64 * 256);
    assert_eq!(scoped_count.load(Ordering::Relaxed), 256);
    assert_eq!(observable.generation(), 256);
    no_allocation(|| drop(subscription));
    let replacement = no_allocation(|| observable.try_subscribe(Arc::clone(&scoped))).unwrap();
    no_allocation(|| observable.notify_observers());
    assert_eq!(scoped_count.load(Ordering::Relaxed), 257);
    no_allocation(|| drop(replacement));
}

#[test]
fn repeated_scoped_subscribe_drop_reuses_preallocated_registration_arcs() {
    let observable = Observable::try_bounded(129).unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let callback = counted(&counter);
    for expected in 1..=1024 {
        let subscription =
            no_allocation(|| observable.try_subscribe(Arc::clone(&callback))).unwrap();
        no_allocation(|| observable.notify_observers());
        assert_eq!(counter.load(Ordering::Relaxed), expected);
        no_allocation(|| drop(subscription));
        no_allocation(|| observable.notify_observers());
        assert_eq!(counter.load(Ordering::Relaxed), expected);
    }
    assert_eq!(observable.num_observers(), 0);
}

#[test]
fn bounded_callback_reenters_subscribe_without_locks_or_allocation() {
    let observable = Arc::new(Observable::try_bounded(2).unwrap());
    let installed = Arc::new(Mutex::new(None::<ObserverSubscription>));
    // The caller's Mutex/Arc/callback layout is a separate pregrant. Initialize
    // Darwin's lazy mutex backing outside the registry operation measurement.
    drop(installed.lock().unwrap());
    let counter = Arc::new(AtomicUsize::new(0));
    let scoped = counted(&counter);
    let weak = Arc::downgrade(&observable);
    let callback_installed = Arc::clone(&installed);
    let callback: Observer = Arc::new(move || {
        let mut installed = callback_installed.lock().unwrap();
        if installed.is_none() {
            *installed = Some(
                weak.upgrade()
                    .unwrap()
                    .try_subscribe(Arc::clone(&scoped))
                    .unwrap(),
            );
        }
    });
    no_allocation(|| observable.try_add_observer(callback)).unwrap();
    no_allocation(|| observable.notify_observers());
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "new subscription was outside the original snapshot"
    );
    no_allocation(|| observable.notify_observers());
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    let subscription = installed.lock().unwrap().take().unwrap();
    no_allocation(|| drop(subscription));
}

struct ClosingCapture {
    observable: Weak<Observable>,
    replacement: Observer,
    refused: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
}
impl Drop for ClosingCapture {
    fn drop(&mut self) {
        let observable = self.observable.upgrade().unwrap();
        self.refused.store(
            matches!(
                observable.try_subscribe(Arc::clone(&self.replacement)),
                Err(ObservableCapacityError::CapacityExceeded)
            ),
            Ordering::SeqCst,
        );
        self.exited.store(true, Ordering::SeqCst);
    }
}

#[test]
fn captured_owner_physical_drop_keeps_slot_occupied_until_destructor_returns() {
    let observable = Arc::new(Observable::try_bounded(1).unwrap());
    let refused = Arc::new(AtomicBool::new(false));
    let exited = Arc::new(AtomicBool::new(false));
    let replacement: Observer = Arc::new(|| {});
    let capture = ClosingCapture {
        observable: Arc::downgrade(&observable),
        replacement: Arc::clone(&replacement),
        refused: Arc::clone(&refused),
        exited: Arc::clone(&exited),
    };
    let callback: Observer = Arc::new(move || {
        let _keep_actual_capture = &capture;
    });
    let subscription = no_allocation(|| observable.try_subscribe(callback)).unwrap();
    no_allocation(|| drop(subscription));
    assert!(exited.load(Ordering::SeqCst));
    assert!(
        refused.load(Ordering::SeqCst),
        "slot reuse must wait for physical captured-owner Drop, not logical removal"
    );
    let replacement_subscription =
        no_allocation(|| observable.try_subscribe(Arc::clone(&replacement))).unwrap();
    no_allocation(|| drop(replacement_subscription));
}
