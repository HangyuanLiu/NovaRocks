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

//! Actual patched Tokio socket-registration backing only. The current-thread
//! reactor and System allocator observe physical Arc/platform-mutex exit.
//! Runtime, sockets, DNS, tasks, future Boxes and user Wakers are separate.

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener as StdListener, TcpStream as StdStream};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::{Context, Waker};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

const SLOTS: usize = 3;
const RECORDS_PER_SLOT: usize = 8;
#[derive(Clone, Copy)]
struct Record {
    pointer: usize,
    bytes: usize,
    alignment: usize,
    freed: bool,
}
const EMPTY: Record = Record {
    pointer: 0,
    bytes: 0,
    alignment: 0,
    freed: false,
};
thread_local! {
    static ACTIVE: Cell<Option<usize>> = const { Cell::new(None) };
    static CARRIER_ACTIVE: Cell<Option<usize>> = const { Cell::new(None) };
    static RECORDS: RefCell<[[Record; RECORDS_PER_SLOT]; SLOTS]> = const { RefCell::new([[EMPTY; RECORDS_PER_SLOT]; SLOTS]) };
    static OVERFLOW: Cell<bool> = const { Cell::new(false) };
    static CARRIERS: Cell<[usize; SLOTS]> = const { Cell::new([0; SLOTS]) };
    static CARRIER_FREED: Cell<[usize; SLOTS]> = const { Cell::new([0; SLOTS]) };
    static EXITS: Cell<[bool; SLOTS]> = const { Cell::new([false; SLOTS]) };
    static EARLY_EXIT: Cell<[bool; SLOTS]> = const { Cell::new([false; SLOTS]) };
}
fn set<T: Copy>(key: &'static std::thread::LocalKey<Cell<[T; SLOTS]>>, slot: usize, value: T) {
    let _ = key.try_with(|key| {
        let mut values = key.get();
        values[slot] = value;
        key.set(values);
    });
}
fn allocated(pointer: *mut u8, layout: Layout) {
    if let Some(slot) = ACTIVE.try_with(Cell::get).ok().flatten() {
        let _ = RECORDS.try_with(|records| {
            let mut records = records.borrow_mut();
            if let Some(record) = records[slot].iter_mut().find(|r| r.pointer == 0) {
                *record = Record {
                    pointer: pointer as usize,
                    bytes: layout.size(),
                    alignment: layout.align(),
                    freed: false,
                };
            } else {
                let _ = OVERFLOW.try_with(|overflow| overflow.set(true));
            }
        });
    }
    if let Some(slot) = CARRIER_ACTIVE.try_with(Cell::get).ok().flatten() {
        set(&CARRIERS, slot, pointer as usize);
    }
}
struct Probe;
// SAFETY: The allocator delegates unchanged to System. Fixed TLS records only
// compare pointer identities and requested Layouts, without reading allocations.
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
        let next = unsafe { System.realloc(pointer, layout, size) };
        // No registration backing is allowed to grow. Record every requested
        // replacement; physical-exit checks will also reject an unretired old one.
        allocated(next, Layout::from_size_align(size, layout.align()).unwrap());
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        let _ = RECORDS.try_with(|records| {
            for record in records.borrow_mut().iter_mut().flatten() {
                if record.pointer == pointer as usize && !record.freed {
                    record.freed = true;
                }
            }
        });
        let carriers = CARRIERS.try_with(Cell::get).unwrap_or([0; SLOTS]);
        for (slot, carrier) in carriers.into_iter().enumerate() {
            if carrier == pointer as usize {
                set(&CARRIER_FREED, slot, layout.size());
                set(&CARRIERS, slot, 0);
            }
        }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Capture;
impl Drop for Capture {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.set(None));
    }
}
fn capture<T>(slot: usize, operation: impl FnOnce() -> T) -> T {
    assert!(ACTIVE.with(|active| active.replace(Some(slot))).is_none());
    let end = Capture;
    let result = operation();
    drop(end);
    result
}
fn observed(slot: usize) -> (usize, usize, bool) {
    RECORDS.with(|records| {
        let records = records.borrow();
        let records = &records[slot];
        (
            records.iter().filter(|r| r.pointer != 0).count(),
            records.iter().map(|r| r.bytes).sum(),
            records.iter().all(|r| r.pointer == 0 || r.freed),
        )
    })
}
struct Exit {
    slot: usize,
    credit: Option<ResultWriteCredit>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        // Record failures without panicking inside reactor/IO destruction. A
        // premature release must fail the outer oracle, never double-panic.
        let early =
            !observed(self.slot).2 || CARRIER_FREED.with(Cell::get)[self.slot] != carrier_bytes();
        set(&EARLY_EXIT, self.slot, early);
        set(&EXITS, self.slot, true);
        drop(self.credit.take());
    }
}
fn carrier_bytes() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, Exit>()
}
struct Funding {
    budget: Arc<ResultRetainedBudget>,
    per_registration: usize,
    total: usize,
}
impl Funding {
    fn new(slots: usize) -> Self {
        assert!(slots <= SLOTS);
        assert!(ACTIVE.with(Cell::get).is_none());
        assert!(CARRIER_ACTIVE.with(Cell::get).is_none());
        RECORDS.with(|r| *r.borrow_mut() = [[EMPTY; RECORDS_PER_SLOT]; SLOTS]);
        OVERFLOW.with(|v| v.set(false));
        CARRIERS.with(|v| v.set([0; SLOTS]));
        CARRIER_FREED.with(|v| v.set([0; SLOTS]));
        EXITS.with(|v| v.set([false; SLOTS]));
        EARLY_EXIT.with(|v| v.set([false; SLOTS]));
        let per_registration = TcpStream::registration_allocation_capacity_bound().unwrap();
        let total = per_registration
            .checked_add(carrier_bytes())
            .unwrap()
            .checked_mul(slots)
            .unwrap();
        Self {
            budget: ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap()),
            per_registration,
            total,
        }
    }
    fn owner(&self, slot: usize) -> Bytes {
        let ResultWriteAdmission::Granted(credit) = self
            .budget
            .try_reserve_process(self.per_registration + carrier_bytes())
            .unwrap()
        else {
            panic!("registration plus original carrier must be pregranted before allocation");
        };
        CARRIER_ACTIVE.with(|v| v.set(Some(slot)));
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            Exit {
                slot,
                credit: Some(credit),
            },
        );
        CARRIER_ACTIVE.with(|v| v.set(None));
        owner
    }
    fn registered(&self, slot: usize) {
        assert!(!OVERFLOW.with(Cell::get));
        let (count, requested, freed) = observed(slot);
        assert!(!freed, "actual registration backing must still be resident");
        assert_eq!(
            requested, self.per_registration,
            "all actual registration allocations must fit the original typed bound exactly"
        );
        // Tokio's actual feature selection determines whether this mutex owns
        // a platform Box. A macOS parking_lot build has no Darwin PAL backing.
        let pal = TcpStream::registration_platform_mutex_allocation_capacity_bound().unwrap();
        assert_eq!(count, 1 + usize::from(pal > 0));
        let arc_bytes = self.per_registration.checked_sub(pal).unwrap();
        RECORDS.with(|r| {
            let r = r.borrow();
            let arc = r[slot]
                .iter()
                .find(|record| record.pointer != 0 && record.bytes == arc_bytes)
                .expect("actual ScheduledIo Arc allocation must be observed");
            #[cfg(any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "powerpc64"
            ))]
            assert_eq!(
                arc.alignment, 128,
                "actual cache-line-padded ScheduledIo Arc"
            );
            #[cfg(not(any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "powerpc64"
            )))]
            assert!(arc.alignment.is_power_of_two());
            if pal > 0 {
                assert_eq!(
                    r[slot]
                        .iter()
                        .filter(|record| record.pointer != 0 && record.bytes == pal)
                        .count(),
                    1,
                    "actual feature-selected platform mutex backing must be observed"
                );
            }
        });
        assert!(!EXITS.with(Cell::get)[slot]);
    }
    fn deferred(&self, slot: usize) {
        assert!(
            !EXITS.with(Cell::get)[slot],
            "socket drop must not pretend the paused reactor has retired its event token"
        );
        assert!(!observed(slot).2);
    }
    fn returned(&self, slots: &[usize]) {
        for &slot in slots {
            assert!(EXITS.with(Cell::get)[slot]);
            assert!(
                !EARLY_EXIT.with(Cell::get)[slot],
                "actual Arc/PAL and carrier must exit before original credit release"
            );
            assert!(observed(slot).2);
        }
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.total).unwrap()
        else {
            panic!("all exact original registration grants must return after physical exit");
        };
        drop(credit);
    }
}
fn std_listener() -> StdListener {
    let listener = StdListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    listener
}
fn runtime() -> Runtime {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Warm runtime/driver-owned mutexes outside the registration measurement.
    // This ordinary registration has no tested original registration capability.
    {
        let _entered = runtime.enter();
        drop(TcpListener::from_std(std_listener()).unwrap());
    }
    runtime
}
fn retire(runtime: &Runtime, slots: &[usize]) {
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if slots.iter().all(|slot| EXITS.with(Cell::get)[*slot]) {
                    break;
                }
                // This drives actual reactor safety points. The timeout is only
                // a fixture watchdog: exact System deallocation is the proof.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("actual registration retirement exceeded fixture watchdog");
    });
}
fn funded_listener(runtime: &Runtime, funding: &Funding, slot: usize) -> (TcpListener, SocketAddr) {
    let original = funding.owner(slot);
    let listener = std_listener();
    let addr = listener.local_addr().unwrap();
    let _entered = runtime.enter();
    let listener = capture(slot, || {
        TcpListener::from_std_with_registration_owner(listener, original)
    })
    .unwrap();
    funding.registered(slot);
    (listener, addr)
}
fn accepted<T>(
    runtime: &Runtime,
    listener: &TcpListener,
    slot: usize,
    acquire: impl FnOnce() -> io::Result<(Bytes, T)>,
) -> io::Result<(TcpStream, SocketAddr, T)> {
    let mut future = std::pin::pin!(listener.accept_with_registration_owner(|| {
        ACTIVE.with(|v| {
            assert!(v.get().is_none());
            v.set(Some(slot));
        });
        acquire()
    }));
    runtime.block_on(std::future::poll_fn(|cx| {
        let end = Capture;
        let result = future.as_mut().poll(cx);
        drop(end);
        result
    }))
}

#[test]
fn listener_drop_waits_for_actual_reactor_retirement_before_original_credit() {
    let runtime = runtime();
    let funding = Funding::new(1);
    let (listener, _) = funded_listener(&runtime, &funding, 0);
    drop(listener);
    funding.deferred(0);
    assert!(matches!(
        funding.budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    retire(&runtime, &[0]);
    funding.returned(&[0]);
}

#[test]
fn actual_connect_accept_and_split_halves_keep_original_registration_until_last_exit() {
    let runtime = runtime();
    let funding = Funding::new(3);
    let (listener, addr) = funded_listener(&runtime, &funding, 0);
    let client_owner = funding.owner(1);
    let mut connecting = Box::pin(TcpStream::connect_with_registration_owner(
        addr,
        client_owner,
    ));
    {
        let _entered = runtime.enter();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(capture(1, || connecting.as_mut().poll(&mut cx)).is_pending());
    }
    funding.registered(1);
    let client = runtime.block_on(connecting).unwrap();
    let accepted_owner = funding.owner(2);
    let (server, remote, token) =
        accepted(&runtime, &listener, 2, || Ok((accepted_owner, 73_u32))).unwrap();
    assert_eq!(token, 73);
    assert_eq!(remote, client.local_addr().unwrap());
    funding.registered(2);
    let (read, write) = client.into_split();
    drop(read);
    // An actual surviving socket half is a real registration owner, independent
    // of wire FIN or a logical close. A reactor turn cannot release it yet.
    runtime.block_on(async {
        tokio::time::sleep(Duration::from_millis(1)).await;
    });
    funding.registered(1);
    drop(write);
    funding.deferred(1);
    drop(server);
    funding.deferred(2);
    drop(listener);
    funding.deferred(0);
    retire(&runtime, &[0, 1, 2]);
    funding.returned(&[0, 1, 2]);
}

#[test]
fn pending_actual_connect_cancellation_and_runtime_shutdown_retire_original_backing() {
    let runtime = runtime();
    let funding = Funding::new(1);
    let peer = std_listener();
    let owner = funding.owner(0);
    let mut connecting = Box::pin(TcpStream::connect_with_registration_owner(
        peer.local_addr().unwrap(),
        owner,
    ));
    {
        let _entered = runtime.enter();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(capture(0, || connecting.as_mut().poll(&mut cx)).is_pending());
    }
    funding.registered(0);
    drop(connecting);
    funding.deferred(0);
    drop(runtime); // Actual shutdown detaches and frees driver-held references.
    funding.returned(&[0]);
    drop(peer);
}

#[test]
fn actual_accept_refusal_allocates_no_registration_and_next_accept_recovers() {
    let runtime = runtime();
    let funding = Funding::new(2);
    let (listener, addr) = funded_listener(&runtime, &funding, 0);
    let mut rejected_peer = StdStream::connect(addr).unwrap();
    let mut calls = 0;
    let error = accepted::<()>(&runtime, &listener, 1, || {
        calls += 1;
        Err(io::ErrorKind::WouldBlock.into())
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(calls, 1);
    assert_eq!(
        observed(1),
        (0, 0, true),
        "actual OS accept refusal must precede ScheduledIo/platform allocation"
    );
    rejected_peer
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        rejected_peer.read(&mut [0]).unwrap(),
        0,
        "refused actual accepted FD must close"
    );
    drop(rejected_peer);
    let peer = StdStream::connect(addr).unwrap();
    let owner = funding.owner(1);
    let (stream, _, ()) = accepted(&runtime, &listener, 1, || Ok((owner, ()))).unwrap();
    funding.registered(1);
    drop(stream);
    funding.deferred(1);
    drop(listener);
    funding.deferred(0);
    retire(&runtime, &[0, 1]);
    funding.returned(&[0, 1]);
    drop(peer);
}
