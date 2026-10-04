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

//! Actual public constructor probes. The caller's scratch credit covers only
//! the queried oneshot Arc allocation; Bytes carrier and external value/Waker
//! allocations are constructed outside measurement and are separate obligations.

use bytes::Bytes;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Wake, Waker};
use tokio::sync::oneshot;

const NONE: usize = usize::MAX;
const RECORDS: usize = 64;
struct Record {
    used: AtomicBool,
    requested: AtomicUsize,
    pointer: AtomicUsize,
    allocations: AtomicUsize,
    reallocations: AtomicUsize,
    physical_free: AtomicBool,
    reserved: AtomicUsize,
    returns: AtomicUsize,
    value_drops: AtomicUsize,
    waker_drops: AtomicUsize,
}
impl Record {
    const fn new() -> Self {
        Self {
            used: AtomicBool::new(false),
            requested: AtomicUsize::new(0),
            pointer: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
            reallocations: AtomicUsize::new(0),
            physical_free: AtomicBool::new(false),
            reserved: AtomicUsize::new(0),
            returns: AtomicUsize::new(0),
            value_drops: AtomicUsize::new(0),
            waker_drops: AtomicUsize::new(0),
        }
    }
}
static LEDGER: [Record; RECORDS] = [const { Record::new() }; RECORDS];
// Serialize System allocation/free and pointer identity publication together.
// A completed free cannot then mark a concurrently reused address as exited.
// This test-only lock has no allocation or user callback, including on Darwin.
static ALLOCATOR_GATE: AtomicBool = AtomicBool::new(false);
struct AllocatorGuard;
impl AllocatorGuard {
    fn acquire() -> Self {
        while ALLOCATOR_GATE
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Self
    }
}
impl Drop for AllocatorGuard {
    fn drop(&mut self) {
        ALLOCATOR_GATE.store(false, Ordering::Release);
    }
}
thread_local! { static TRACK: Cell<usize> = const { Cell::new(NONE) }; }
struct ActualAllocator;
#[global_allocator]
static ALLOCATOR: ActualAllocator = ActualAllocator;
unsafe impl GlobalAlloc for ActualAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _guard = AllocatorGuard::acquire();
        // SAFETY: delegate the unchanged allocation request to System.
        let pointer = unsafe { System.alloc(layout) };
        let _ = TRACK.try_with(|track| {
            let index = track.get();
            if index != NONE {
                let record = &LEDGER[index];
                record.allocations.fetch_add(1, Ordering::SeqCst);
                if layout.size() == record.requested.load(Ordering::SeqCst) {
                    record.pointer.store(pointer as usize, Ordering::SeqCst);
                }
            }
        });
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let _guard = AllocatorGuard::acquire();
        // SAFETY: System uses the requested layout and returns initialized bytes.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        let _ = TRACK.try_with(|track| {
            let index = track.get();
            if index != NONE {
                LEDGER[index].allocations.fetch_add(1, Ordering::SeqCst);
                if layout.size() == LEDGER[index].requested.load(Ordering::SeqCst) {
                    LEDGER[index]
                        .pointer
                        .store(pointer as usize, Ordering::SeqCst);
                }
            }
        });
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _guard = AllocatorGuard::acquire();
        let _ = TRACK.try_with(|track| {
            let index = track.get();
            if index != NONE {
                LEDGER[index].reallocations.fetch_add(1, Ordering::SeqCst);
            }
        });
        // SAFETY: delegate the allocator's original pointer/layout and new size.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let _guard = AllocatorGuard::acquire();
        // Record completion after the actual allocator deallocation, rather
        // than marking an Inner::drop callback as allocation exit.
        unsafe { System.dealloc(pointer, layout) };
        for record in &LEDGER {
            if record.used.load(Ordering::SeqCst)
                && record.pointer.load(Ordering::SeqCst) == pointer as usize
            {
                record.physical_free.store(true, Ordering::SeqCst);
            }
        }
    }
}

struct Probe(usize);
impl Probe {
    fn new() -> Self {
        let available = {
            let _guard = AllocatorGuard::acquire();
            let index = LEDGER.iter().position(|entry| {
                entry
                    .used
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            });
            if let Some(index) = index {
                let record = &LEDGER[index];
                record.requested.store(0, Ordering::SeqCst);
                record.pointer.store(0, Ordering::SeqCst);
                record.allocations.store(0, Ordering::SeqCst);
                record.reallocations.store(0, Ordering::SeqCst);
                record.physical_free.store(false, Ordering::SeqCst);
                record.reserved.store(0, Ordering::SeqCst);
                record.returns.store(0, Ordering::SeqCst);
                record.value_drops.store(0, Ordering::SeqCst);
                record.waker_drops.store(0, Ordering::SeqCst);
            }
            index
        };
        // Panic formatting is deliberately outside the allocator gate.
        Self(available.expect("fixed allocator record available"))
    }
    fn record(&self) -> &Record {
        &LEDGER[self.0]
    }
    fn measure<R>(&self, requested: usize, operation: impl FnOnce() -> R) -> R {
        self.record().requested.store(requested, Ordering::SeqCst);
        TRACK.with(|track| assert_eq!(track.replace(self.0), NONE));
        struct Scope;
        impl Drop for Scope {
            fn drop(&mut self) {
                TRACK.with(|track| track.set(NONE));
            }
        }
        let scope = Scope;
        let result = operation();
        drop(scope);
        result
    }
    fn grant(&self, amount: usize, expect_allocation: bool) -> Bytes {
        // This is a caller-owned test capacity position, obtained before the
        // actual constructor. It is not a production wallet or inferred credit.
        self.record()
            .reserved
            .compare_exchange(0, amount, Ordering::SeqCst, Ordering::SeqCst)
            .expect("one scratch credit issued");
        Bytes::from_owner(Credit {
            index: self.0,
            amount,
            expect_allocation,
        })
    }
    fn channel<T>(&self) -> (oneshot::Sender<T>, oneshot::Receiver<T>) {
        let bound = oneshot::allocation_capacity_bound::<T>().unwrap();
        let original = self.grant(bound, true);
        let pair = self.measure(bound, || {
            oneshot::channel_with_original_owner(bound, original).unwrap()
        });
        assert_eq!(self.record().allocations.load(Ordering::SeqCst), 1);
        assert_eq!(self.record().reallocations.load(Ordering::SeqCst), 0);
        assert_ne!(self.record().pointer.load(Ordering::SeqCst), 0);
        pair
    }
    fn held(&self) {
        assert_ne!(self.record().reserved.load(Ordering::SeqCst), 0);
        assert_eq!(self.record().returns.load(Ordering::SeqCst), 0);
    }
    fn returned(&self) {
        assert_eq!(self.record().reserved.load(Ordering::SeqCst), 0);
        assert_eq!(self.record().returns.load(Ordering::SeqCst), 1);
        assert!(self.record().physical_free.load(Ordering::SeqCst));
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        // Keep failed-test records available to destructors during unwinding.
        if !std::thread::panicking() {
            assert_eq!(self.record().reserved.load(Ordering::SeqCst), 0);
            let _guard = AllocatorGuard::acquire();
            self.record().used.store(false, Ordering::SeqCst);
        }
    }
}
struct Credit {
    index: usize,
    amount: usize,
    expect_allocation: bool,
}
impl AsRef<[u8]> for Credit {
    fn as_ref(&self) -> &[u8] {
        &[]
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        let record = &LEDGER[self.index];
        if self.expect_allocation {
            assert!(
                record.physical_free.load(Ordering::SeqCst),
                "original credit returned before actual Arc allocation free"
            );
        } else {
            assert_eq!(record.allocations.load(Ordering::SeqCst), 0);
            assert_eq!(record.pointer.load(Ordering::SeqCst), 0);
        }
        assert_eq!(record.reserved.swap(0, Ordering::SeqCst), self.amount);
        assert_eq!(record.returns.fetch_add(1, Ordering::SeqCst), 0);
    }
}
struct Payload {
    index: usize,
    panic_on_drop: bool,
}
impl Drop for Payload {
    fn drop(&mut self) {
        let record = &LEDGER[self.index];
        assert_ne!(record.reserved.load(Ordering::SeqCst), 0);
        record.value_drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("original payload destructor panic");
        }
    }
}
struct Observer {
    index: usize,
    panic_on_drop: bool,
}
impl Wake for Observer {
    fn wake(self: Arc<Self>) {}
    fn wake_by_ref(self: &Arc<Self>) {}
}
impl Drop for Observer {
    fn drop(&mut self) {
        let record = &LEDGER[self.index];
        assert_ne!(record.reserved.load(Ordering::SeqCst), 0);
        record.waker_drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("original Waker destructor panic");
        }
    }
}

#[test]
fn original_bound_query_does_not_construct_or_allocate() {
    let probe = Probe::new();
    let bound = probe
        .measure(0, oneshot::allocation_capacity_bound::<[u8; 32768]>)
        .unwrap();
    assert!(bound >= 32768);
    assert_eq!(probe.record().allocations.load(Ordering::SeqCst), 0);
    assert_eq!(probe.record().reallocations.load(Ordering::SeqCst), 0);
}

#[test]
fn ordinary_none_keeps_one_actual_allocation_and_send_semantics() {
    let probe = Probe::new();
    let (tx, mut rx) = probe.measure(0, oneshot::channel::<usize>);
    assert_eq!(probe.record().allocations.load(Ordering::SeqCst), 1);
    assert_eq!(probe.record().reallocations.load(Ordering::SeqCst), 0);
    tx.send(7).unwrap();
    assert_eq!(rx.try_recv(), Ok(7));
    assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    drop(rx);
}

#[test]
fn short_declared_pregrant_refuses_before_actual_allocation() {
    let probe = Probe::new();
    let bound = oneshot::allocation_capacity_bound::<u64>().unwrap();
    let original = probe.grant(bound - 1, false);
    let error = probe
        .measure(bound, || {
            oneshot::channel_with_original_owner::<u64>(bound - 1, original)
        })
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(probe.record().returns.load(Ordering::SeqCst), 1);
}

#[test]
fn early_receiver_drop_does_not_release_sender_allocation_credit() {
    let probe = Probe::new();
    let (tx, rx) = probe.channel::<u64>();
    drop(rx);
    probe.held();
    assert!(!probe.record().physical_free.load(Ordering::SeqCst));
    assert_eq!(tx.send(9), Err(9));
    probe.returned();
}

#[test]
fn sent_value_and_closed_receiver_keep_credit_until_last_receiver_drop() {
    let probe = Probe::new();
    let (tx, mut rx) = probe.channel::<Payload>();
    assert!(
        tx.send(Payload {
            index: probe.0,
            panic_on_drop: false
        })
        .is_ok()
    );
    rx.close();
    probe.held();
    assert_eq!(probe.record().value_drops.load(Ordering::SeqCst), 0);
    drop(rx);
    assert_eq!(probe.record().value_drops.load(Ordering::SeqCst), 1);
    probe.returned();
}

#[test]
fn both_registered_wakers_exit_before_original_credit() {
    let probe = Probe::new();
    let (mut tx, mut rx) = probe.channel::<u8>();
    let tx_waker = Waker::from(Arc::new(Observer {
        index: probe.0,
        panic_on_drop: false,
    }));
    let rx_waker = Waker::from(Arc::new(Observer {
        index: probe.0,
        panic_on_drop: false,
    }));
    assert!(
        tx.poll_closed(&mut Context::from_waker(&tx_waker))
            .is_pending()
    );
    assert!(
        Pin::new(&mut rx)
            .poll(&mut Context::from_waker(&rx_waker))
            .is_pending()
    );
    drop(tx_waker);
    drop(rx_waker);
    drop(tx);
    probe.held();
    drop(rx);
    assert_eq!(probe.record().waker_drops.load(Ordering::SeqCst), 2);
    probe.returned();
}

#[test]
fn payload_unwind_cleans_value_and_waker_before_original_credit() {
    let probe = Probe::new();
    let (tx, mut rx) = probe.channel::<Payload>();
    let waker = Waker::from(Arc::new(Observer {
        index: probe.0,
        panic_on_drop: false,
    }));
    assert!(
        Pin::new(&mut rx)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    assert!(
        tx.send(Payload {
            index: probe.0,
            panic_on_drop: true
        })
        .is_ok()
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(rx))).is_err());
    assert_eq!(probe.record().value_drops.load(Ordering::SeqCst), 1);
    assert_eq!(probe.record().waker_drops.load(Ordering::SeqCst), 1);
    probe.returned();
}

#[test]
fn final_waker_unwind_preserves_other_waker_and_original_exit_order() {
    let probe = Probe::new();
    let (mut tx, mut rx) = probe.channel::<u8>();
    // Original cleanup drops tx_task first, so its panic must still clean rx_task.
    let tx_waker = Waker::from(Arc::new(Observer {
        index: probe.0,
        panic_on_drop: true,
    }));
    let rx_waker = Waker::from(Arc::new(Observer {
        index: probe.0,
        panic_on_drop: false,
    }));
    assert!(
        tx.poll_closed(&mut Context::from_waker(&tx_waker))
            .is_pending()
    );
    assert!(
        Pin::new(&mut rx)
            .poll(&mut Context::from_waker(&rx_waker))
            .is_pending()
    );
    drop(tx_waker);
    drop(rx_waker);
    drop(tx);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(rx))).is_err());
    assert_eq!(probe.record().waker_drops.load(Ordering::SeqCst), 2);
    probe.returned();
}

#[test]
fn concurrent_endpoint_last_drops_return_one_credit_after_actual_free() {
    let probe = Probe::new();
    let (tx, rx) = probe.channel::<u64>();
    let barrier = Arc::new(Barrier::new(3));
    let left_barrier = barrier.clone();
    let left = std::thread::spawn(move || {
        left_barrier.wait();
        drop(tx);
    });
    let right_barrier = barrier.clone();
    let right = std::thread::spawn(move || {
        right_barrier.wait();
        drop(rx);
    });
    barrier.wait();
    left.join().unwrap();
    right.join().unwrap();
    probe.returned();
}

#[test]
fn returned_value_is_external_after_cell_actual_exit() {
    let probe = Probe::new();
    let (tx, mut rx) = probe.channel::<u64>();
    tx.send(123).unwrap();
    let output = rx.try_recv().unwrap();
    assert_eq!(output, 123);
    probe.returned();
    drop(rx);
}

#[test]
fn sender_exit_keeps_original_until_receiver_observes_closed() {
    let probe = Probe::new();
    let (tx, mut rx) = probe.channel::<u64>();
    drop(tx);
    probe.held();
    assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    probe.returned();
}

#[test]
fn actual_overaligned_payload_layout_matches_original_query() {
    #[repr(align(4096))]
    struct Aligned([u8; 31]);
    let probe = Probe::new();
    let (tx, mut rx) = probe.channel::<Aligned>();
    let bound = oneshot::allocation_capacity_bound::<Aligned>().unwrap();
    assert_eq!(bound % 4096, 0);
    assert_eq!(probe.record().pointer.load(Ordering::SeqCst) % 4096, 0);
    assert!(tx.send(Aligned([42; 31])).is_ok());
    assert_eq!(rx.try_recv().unwrap().0, [42; 31]);
    probe.returned();
}

#[test]
fn original_channel_preserves_local_non_send_payload_semantics() {
    let probe = Probe::new();
    let (tx, mut rx) = probe.channel::<std::rc::Rc<u64>>();
    let payload = std::rc::Rc::new(55);
    assert!(tx.send(payload.clone()).is_ok());
    let returned = rx.try_recv().unwrap();
    assert!(std::rc::Rc::ptr_eq(&returned, &payload));
    probe.returned();
}

#[test]
fn send_notification_unwind_retains_original_value_until_receiver_exit() {
    struct PanicWake(usize);
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("original wake callback panic");
        }
        fn wake_by_ref(self: &Arc<Self>) {
            panic!("original wake callback panic");
        }
    }
    impl Drop for PanicWake {
        fn drop(&mut self) {
            let record = &LEDGER[self.0];
            assert_ne!(record.reserved.load(Ordering::SeqCst), 0);
            record.waker_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let probe = Probe::new();
    let (tx, mut rx) = probe.channel::<Payload>();
    let waker = Waker::from(Arc::new(PanicWake(probe.0)));
    assert!(
        Pin::new(&mut rx)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = tx.send(Payload {
                index: probe.0,
                panic_on_drop: false,
            });
        }))
        .is_err()
    );
    probe.held();
    assert_eq!(probe.record().value_drops.load(Ordering::SeqCst), 0);
    drop(rx);
    assert_eq!(probe.record().value_drops.load(Ordering::SeqCst), 1);
    assert_eq!(probe.record().waker_drops.load(Ordering::SeqCst), 1);
    probe.returned();
}
