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

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use super::*;

#[derive(Default)]
struct Counts {
    polls: AtomicUsize,
    future_drops: AtomicUsize,
    record_drops: AtomicUsize,
    wakes: AtomicUsize,
}

struct OriginalRecord(Arc<Counts>);

impl Drop for OriginalRecord {
    fn drop(&mut self) {
        self.0.record_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct OriginalHttpState {
    identity: u64,
    completed_polls: usize,
    records: Vec<OriginalRecord>,
    future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

struct OpaquePanic(Arc<()>);

struct OpaqueFuture {
    cell: Weak<HttpProcessCell<OriginalHttpState>>,
    counts: Arc<Counts>,
    panic_payload: Option<Arc<()>>,
    unwound: bool,
}

impl Future for OpaqueFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        assert!(!self.unwound, "test must never re-poll an unwound future");
        let cell = self.cell.upgrade().unwrap();
        let slot = cell
            .state
            .try_lock()
            .expect("foreign poll holds no custody lock");
        assert!(slot.is_none());
        drop(slot);
        self.counts.polls.fetch_add(1, Ordering::SeqCst);
        cx.waker().wake_by_ref();
        if let Some(payload) = self.panic_payload.take() {
            self.unwound = true;
            panic_any(OpaquePanic(payload));
        }
        Poll::Pending
    }
}

impl Drop for OpaqueFuture {
    fn drop(&mut self) {
        if let Some(cell) = self.cell.upgrade() {
            assert!(
                cell.state.try_lock().is_ok(),
                "foreign Drop holds no custody lock"
            );
        }
        self.counts.future_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct PollWake {
    cell: Weak<HttpProcessCell<OriginalHttpState>>,
    counts: Arc<Counts>,
}

impl Wake for PollWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let cell = self.cell.upgrade().unwrap();
        let slot = cell
            .state
            .try_lock()
            .expect("foreign wake holds no custody lock");
        assert!(slot.is_none());
        drop(slot);
        assert_eq!(
            cell.with_state::<()>(|_| panic!("nested borrow must not enter")),
            Err(HttpProcessCellBusy)
        );
        self.counts.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

fn original_state(counts: &Arc<Counts>) -> Box<OriginalHttpState> {
    Box::new(OriginalHttpState {
        identity: 0x7140,
        completed_polls: 0,
        records: vec![OriginalRecord(Arc::clone(counts))],
        future: None,
    })
}

fn install_future(
    cell: &Arc<HttpProcessCell<OriginalHttpState>>,
    counts: &Arc<Counts>,
    panic_payload: Option<Arc<()>>,
) -> Waker {
    cell.with_state(|state| {
        state.future = Some(Box::pin(OpaqueFuture {
            cell: Arc::downgrade(cell),
            counts: Arc::clone(counts),
            panic_payload,
            unwound: false,
        }));
    })
    .unwrap();
    Waker::from(Arc::new(PollWake {
        cell: Arc::downgrade(cell),
        counts: Arc::clone(counts),
    }))
}

#[test]
fn one_poll_keeps_original_box_records_and_opaque_future_without_locking_callbacks() {
    let counts = Arc::new(Counts::default());
    let state = original_state(&counts);
    let address = std::ptr::from_ref(&*state) as usize;
    let cell = Arc::new(HttpProcessCell::new(state));
    let waker = install_future(&cell, &counts, None);
    let mut cx = Context::from_waker(&waker);
    let result = cell
        .with_state(|state| {
            assert_eq!(std::ptr::from_ref(state) as usize, address);
            state.completed_polls += 1;
            state.future.as_mut().unwrap().as_mut().poll(&mut cx)
        })
        .unwrap();
    assert!(result.is_pending());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(counts.future_drops.load(Ordering::SeqCst), 0);
    let future = cell
        .with_state(|state| {
            assert_eq!(std::ptr::from_ref(state) as usize, address);
            assert_eq!(state.completed_polls, 1);
            assert_eq!(state.records.len(), 1);
            state.future.take().unwrap()
        })
        .unwrap();
    // The actual future exits after the lease returned the same state.
    drop(future);
    assert_eq!(counts.future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.record_drops.load(Ordering::SeqCst), 0);
    drop(waker);
    let state = Arc::try_unwrap(cell).ok().unwrap().into_state();
    assert_eq!(std::ptr::from_ref(&*state) as usize, address);
    drop(state);
    assert_eq!(counts.record_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn foreign_poll_unwind_restores_same_box_and_retains_original_payload_and_future() {
    let counts = Arc::new(Counts::default());
    let state = original_state(&counts);
    let address = std::ptr::from_ref(&*state) as usize;
    let cell = Arc::new(HttpProcessCell::new(state));
    let payload = Arc::new(());
    let waker = install_future(&cell, &counts, Some(Arc::clone(&payload)));
    let mut cx = Context::from_waker(&waker);
    let unwind = catch_unwind(AssertUnwindSafe(|| {
        cell.with_state(|state| {
            state.completed_polls += 1;
            let _ = state.future.as_mut().unwrap().as_mut().poll(&mut cx);
        })
        .unwrap();
    }))
    .unwrap_err();
    let original = unwind.downcast::<OpaquePanic>().ok().unwrap();
    assert!(Arc::ptr_eq(&original.0, &payload));
    assert_eq!(counts.future_drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.record_drops.load(Ordering::SeqCst), 0);
    // Inspection is not retry: never poll the unwound opaque future again.
    let future = cell
        .with_state(|state| {
            assert_eq!(std::ptr::from_ref(state) as usize, address);
            assert_eq!(state.identity, 0x7140);
            assert_eq!(state.completed_polls, 1);
            assert_eq!(state.records.len(), 1);
            state.future.take().unwrap()
        })
        .unwrap();
    drop(future);
    assert_eq!(counts.future_drops.load(Ordering::SeqCst), 1);
    drop(original);
    drop(waker);
    drop(Arc::try_unwrap(cell).ok().unwrap().into_state());
    assert_eq!(counts.record_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_busy_does_not_steal_state_and_root_access_waits_for_driver_join() {
    let counts = Arc::new(Counts::default());
    let state = original_state(&counts);
    let address = std::ptr::from_ref(&*state) as usize;
    let cell = Arc::new(HttpProcessCell::new(state));
    std::thread::scope(|scope| {
        let (entered_tx, entered_rx) = sync_channel(1);
        // This sender belongs to the scope closure. If a main-thread assertion
        // unwinds, dropping it releases the actual driver before scope joins.
        let (release_tx, release_rx) = sync_channel(1);
        let driver_cell = Arc::clone(&cell);
        let driver = scope.spawn(move || {
            driver_cell
                .with_state(|state| {
                    assert_eq!(std::ptr::from_ref(state) as usize, address);
                    state.completed_polls += 1;
                    entered_tx
                        .send(())
                        .expect("main still observes driver entry");
                    match release_rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(()) => true,
                        Err(RecvTimeoutError::Disconnected) => false,
                        Err(RecvTimeoutError::Timeout) => panic!("main did not release the driver"),
                    }
                })
                .unwrap()
        });
        // A driver panic before entry drops entered_tx, so this cannot wait
        // forever for a failed pointer oracle or failed driver acquisition.
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("driver entered its lease");
        let mut entered_busy_operation = false;
        assert_eq!(
            cell.with_state(|_| entered_busy_operation = true),
            Err(HttpProcessCellBusy)
        );
        assert!(!entered_busy_operation);
        assert!(cell.state.try_lock().unwrap().is_none());
        assert_eq!(counts.record_drops.load(Ordering::SeqCst), 0);
        release_tx.send(()).expect("driver still waits for release");
        assert!(driver.join().expect("driver exited without panic"));
    });
    let state = Arc::try_unwrap(cell).ok().unwrap().into_state();
    assert_eq!(std::ptr::from_ref(&*state) as usize, address);
    assert_eq!(state.completed_polls, 1);
    drop(state);
    assert_eq!(counts.record_drops.load(Ordering::SeqCst), 1);
}
