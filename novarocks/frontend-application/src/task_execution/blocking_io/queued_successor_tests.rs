// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original queued callback and successor panic payload custody.

use super::*;
use novarocks_workload_control::{OwnerState, ResultWindowClass};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum CustodyCase {
    QueuedCallbackDrop,
    ReturnedOutputDrop,
    OriginalPanicDrop,
}

struct HeldSuccessor {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    destroyed: Arc<AtomicBool>,
}
impl Drop for HeldSuccessor {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.recv();
        self.destroyed.store(true, Ordering::Release);
    }
}

struct ThrowsActualSuccessor(Option<HeldSuccessor>);
impl Drop for ThrowsActualSuccessor {
    fn drop(&mut self) {
        if let Some(successor) = self.0.take() {
            std::panic::panic_any(successor);
        }
    }
}

fn observe_original_custody(case: CustodyCase) {
    let queued = matches!(case, CustodyCase::QueuedCallbackDrop);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("original single-position blocking pool");
    let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
    let (control, root, window) = super::tests::admitted_class(ResultWindowClass::Local);
    let calls = Arc::new(AtomicUsize::new(0));
    let destroyed = Arc::new(AtomicBool::new(false));
    let (successor_started, successor_entered) = mpsc::channel();
    let (release_successor, successor_held) = mpsc::channel();
    let throws = ThrowsActualSuccessor(Some(HeldSuccessor {
        entered: successor_started,
        release: successor_held,
        destroyed: Arc::clone(&destroyed),
    }));
    let (blocker_started, blocker_entered) = mpsc::channel();
    let (release_blocker, blocker_held) = mpsc::channel();
    let (callback_started, callback_entered) = mpsc::channel();
    let (release_callback, callback_held) = mpsc::channel();
    let (disarm, watch) = mpsc::channel();
    let rescue_blocker = release_blocker.clone();
    let rescue_callback = release_callback.clone();
    let rescue_successor = release_successor.clone();
    let watchdog = std::thread::spawn(move || {
        if watch.recv_timeout(Duration::from_secs(8)).is_err() {
            let _ = rescue_callback.send(());
            let _ = rescue_blocker.send(());
            let _ = rescue_successor.send(());
        }
    });
    let mut blocker = if queued {
        Some(runtime.spawn_blocking(move || {
            let _ = blocker_started.send(());
            let _ = blocker_held.recv();
        }))
    } else {
        drop(blocker_held);
        None
    };
    let blocker_began = !queued || blocker_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let invoked = Arc::clone(&calls);
    let mut job = supervisor
        .spawn_admitted(&root.owner.scope(), &window.retain_alias(), move || {
            invoked.fetch_add(1, Ordering::AcqRel);
            let _ = callback_started.send(());
            let _ = callback_held.recv();
            if matches!(case, CustodyCase::OriginalPanicDrop) {
                std::panic::panic_any(throws);
            }
            throws
        })
        .expect("one actual admitted callback");
    let slot = Arc::clone(&job.original_blocking_abort);
    let slot_deadline = Instant::now() + Duration::from_secs(2);
    let original_abort = loop {
        let handle = slot.lock().unwrap().take();
        if handle.is_some() || Instant::now() >= slot_deadline {
            break handle;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let blocking_created = original_abort.is_some();
    let callback_began = queued
        || callback_entered
            .recv_timeout(Duration::from_secs(2))
            .is_ok();
    if !blocker_began || !blocking_created || !callback_began {
        let _ = release_callback.send(());
        let _ = release_blocker.send(());
        let _ = release_successor.send(());
    }

    job.publisher.abort();
    let publisher_observed = runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut job.publisher).await });
    let publisher_cancelled = matches!(publisher_observed,
        Ok(Err(ref error)) if error.is_cancelled());
    drop(publisher_observed);
    let outcome = Arc::downgrade(&job.outcome);
    drop(job);
    let waiter_gone = outcome.upgrade().is_none();
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    drop(window);

    // In the queued case this is the actual spawn_blocking AbortHandle.
    // It is a cancellation request, never an original join observation.
    if queued {
        if let Some(handle) = &original_abort {
            handle.abort();
        }
    }
    drop(original_abort);
    drop(slot);
    let _ = release_callback.send(());
    let _ = release_blocker.send(());
    let blocker_joined = if let Some(handle) = blocker.as_mut() {
        matches!(
            runtime.block_on(async { tokio::time::timeout(Duration::from_secs(2), handle).await }),
            Ok(Ok(()))
        )
    } else {
        true
    };
    drop(blocker);
    let successor_began = successor_entered
        .recv_timeout(Duration::from_secs(2))
        .is_ok();
    let held = control.snapshot();
    let held_before_exit = !destroyed.load(Ordering::Acquire);
    let unfinished = held
        .scopes
        .iter()
        .any(|scope| scope.parent.is_some() && !scope.own_completed);
    let actual_calls = calls.load(Ordering::Acquire);

    // Finish all barriers and actual jobs before checking the saved oracle.
    let _ = release_successor.send(());
    let _ = disarm.send(());
    let watchdog_joined = watchdog.join().is_ok();
    let final_deadline = Instant::now() + Duration::from_secs(2);
    let after = loop {
        let facts = control.snapshot();
        let orphaned = facts
            .scopes
            .iter()
            .any(|scope| scope.parent.is_some() && scope.owner == OwnerState::Orphaned);
        if (destroyed.load(Ordering::Acquire)
            && facts.result_windows.held_positions == [0; 4]
            && orphaned)
            || Instant::now() >= final_deadline
        {
            break facts;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    // Remove mere notification residue; orphaned Work still cannot drain.
    while let Some(permit) = control.next_control() {
        permit.acknowledge();
    }
    control.close_admission();
    let shutdown = control.shutdown();
    let shutdown_refused = shutdown.is_err();
    drop(shutdown);
    drop(supervisor);
    drop(runtime);

    assert!(blocker_began && blocking_created && callback_began);
    assert!(publisher_cancelled && waiter_gone);
    assert!(blocker_joined && watchdog_joined);
    assert!(successor_began && held_before_exit);
    assert_eq!(
        actual_calls,
        if queued { 0 } else { 1 },
        "queued cancellation polled the callback or running callback did not execute once"
    );
    assert!(unfinished);
    assert_eq!(held.root_responsibilities, 1);
    assert_eq!(
        held.result_windows.held_positions,
        [0, 1, 0, 0],
        "actual successor payload outlived its original admitted backing"
    );
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(after.result_windows.held_positions, [0; 4]);
    assert!(
        after
            .scopes
            .iter()
            .any(|scope| scope.parent.is_some() && scope.owner == OwnerState::Orphaned)
    );
    assert!(
        shutdown_refused,
        "unobserved original join manufactured drain success"
    );
}

#[test]
fn aborted_queued_callback_drop_panic_keeps_its_actual_successor_backing() {
    observe_original_custody(CustodyCase::QueuedCallbackDrop);
}

#[test]
fn aborted_returned_output_drop_panic_keeps_its_actual_successor_backing() {
    observe_original_custody(CustodyCase::ReturnedOutputDrop);
}

#[test]
fn aborted_original_panic_payload_drop_keeps_its_actual_successor_backing() {
    observe_original_custody(CustodyCase::OriginalPanicDrop);
}
