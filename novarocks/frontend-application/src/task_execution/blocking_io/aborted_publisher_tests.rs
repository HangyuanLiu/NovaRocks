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

//! Original payload custody after its observing publisher is cancelled.

use super::*;
use novarocks_workload_control::{OwnerState, ResultWindowClass};
use std::sync::mpsc;
use std::time::{Duration, Instant};

struct AbortedObserverPayload {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    destroyed: Arc<AtomicBool>,
}

impl Drop for AbortedObserverPayload {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.recv();
        self.destroyed.store(true, Ordering::Release);
    }
}

fn aborted_publisher_retains_actual_payload(panic_payload: bool) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .expect("original test runtime");
    let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
    let (control, root, window) = super::tests::admitted_class(ResultWindowClass::Local);
    let destroyed = Arc::new(AtomicBool::new(false));
    let (payload_started, payload_entered) = mpsc::channel();
    let (release_payload, payload_held) = mpsc::channel();
    let payload = AbortedObserverPayload {
        entered: payload_started,
        release: payload_held,
        destroyed: Arc::clone(&destroyed),
    };
    let (callback_started, callback_entered) = mpsc::channel();
    let (release_callback, callback_held) = mpsc::channel();
    let mut job = supervisor
        .spawn_admitted(&root.owner.scope(), &window.retain_alias(), move || {
            let _ = callback_started.send(());
            let _ = callback_held.recv();
            if panic_payload {
                std::panic::panic_any(payload);
            }
            payload
        })
        .expect("one original admitted callback");

    // Keep the rescue independent of the future being aborted. It releases
    // both original barriers even if a prerequisite fails, and is joined.
    let (disarm, watch) = mpsc::channel();
    let rescue_callback = release_callback.clone();
    let rescue_payload = release_payload.clone();
    let watchdog = std::thread::spawn(move || {
        if watch.recv_timeout(Duration::from_secs(4)).is_err() {
            let _ = rescue_callback.send(());
            let _ = rescue_payload.send(());
        }
    });
    let callback_began = callback_entered
        .recv_timeout(Duration::from_secs(2))
        .is_ok();
    if !callback_began {
        // Unblock everything before observing an unsuccessful prerequisite.
        let _ = release_callback.send(());
        let _ = release_payload.send(());
    }

    job.publisher.abort();
    let publisher_observed = runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut job.publisher).await });
    let publisher_cancelled =
        matches!(publisher_observed, Ok(Err(ref error)) if error.is_cancelled());
    drop(publisher_observed);
    let outcome = Arc::downgrade(&job.outcome);
    drop(job);
    let last_waiter_gone = outcome.upgrade().is_none();
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    drop(window);

    // No caller grant, outcome Arc, or observer pin is kept across this turn.
    // The callback/output/panic carrier must provide the physical holder.
    let callback_released = release_callback.send(()).is_ok();
    let payload_began = payload_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let held = control.snapshot();
    let held_before_drop_exit = !destroyed.load(Ordering::Acquire);
    let unfinished_child = held
        .scopes
        .iter()
        .any(|scope| scope.parent.is_some() && !scope.own_completed);

    // Capture facts first, then release and settle every actual object/task
    // before assertions. Work is intentionally orphaned, not force-completed.
    let _ = release_payload.send(());
    let _ = disarm.send(());
    let watchdog_joined = watchdog.join().is_ok();
    let deadline = Instant::now() + Duration::from_secs(2);
    let after = loop {
        let snapshot = control.snapshot();
        let orphaned = snapshot
            .scopes
            .iter()
            .any(|scope| scope.parent.is_some() && scope.owner == OwnerState::Orphaned);
        if (destroyed.load(Ordering::Acquire)
            && snapshot.result_windows.held_positions == [0; 4]
            && orphaned)
            || Instant::now() >= deadline
        {
            break snapshot;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    control.close_admission();
    let shutdown = control.shutdown();
    let shutdown_refused = shutdown.is_err();
    drop(shutdown);
    drop(supervisor);
    // This waits the runtime's real blocking pool; it is not a JoinHandle
    // observation and cannot change the orphaned Work verdict above.
    drop(runtime);

    assert!(
        callback_began && callback_released,
        "original callback did not enter/leave its gate"
    );
    assert!(
        publisher_cancelled && last_waiter_gone,
        "original publisher abort was not actually observed or the outcome was retained"
    );
    assert!(
        payload_began && held_before_drop_exit,
        "original payload Drop did not remain held"
    );
    assert!(watchdog_joined, "original rescue thread did not join");
    assert_eq!(held.root_responsibilities, 1);
    assert!(
        unfinished_child,
        "held raw payload manufactured completed child work"
    );
    assert_eq!(
        held.result_windows.held_positions,
        [0, 1, 0, 0],
        "aborted original publisher returned the window before actual payload Drop"
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
        "unobserved original blocking join fabricated a successful drain"
    );
}

#[test]
fn aborted_original_publisher_keeps_the_returned_payload_backing_until_drop() {
    aborted_publisher_retains_actual_payload(false);
}

#[test]
fn aborted_original_publisher_keeps_the_original_panic_payload_backing_until_drop() {
    aborted_publisher_retains_actual_payload(true);
}
