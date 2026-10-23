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

use super::*;
fn credit(
    inputs: usize,
    queue_positions: usize,
    holder_positions: usize,
) -> (OperatorByteCredit, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("operator credit fixture");
    let specs = vec![
        PortCreditSpec {
            queue_positions,
            holder_positions,
            minimum_frame_bytes: 1,
            waiters: 2
        };
        inputs
    ];
    (
        OperatorByteCredit::bind(64 * 1024, inputs, 512, &specs, tracker.clone(), None).unwrap(),
        tracker,
    )
}
fn queued(port: &PortCredit, bytes: usize) -> QueuePermit {
    let CreditAcquire::Granted(value) = port.try_queue(bytes).unwrap() else {
        panic!("available queue position")
    };
    value
}
fn held(queue: QueuePermit) -> HolderPermit {
    let HolderAcquire::Granted(value) = queue.try_into_holder() else {
        panic!("available holder position")
    };
    value
}
#[test]
fn queue_holder_transfer_has_distinct_windows_and_release_wakeup() {
    let (credit, tracker) = credit(1, 1, 1);
    let port = credit.port(0).unwrap();
    let bytes = port.max_frame_bytes();
    let first = queued(&port, bytes);
    let CreditAcquire::Pending(wait) = port.try_queue(0).unwrap() else {
        panic!("metadata slots are finite, including zero-byte frames")
    };
    let holder = held(first);
    assert!(wait.is_ready());
    assert_eq!(port.snapshot(), (0, bytes, 0, 1));
    let second = queued(&port, bytes);
    let HolderAcquire::Pending { queue, wait } = second.try_into_holder() else {
        panic!("holder remains owned downstream")
    };
    assert_eq!(port.snapshot(), (bytes, bytes, 1, 1));
    drop(holder);
    assert!(wait.is_ready());
    drop(held(queue));
    assert_eq!(port.snapshot(), (0, 0, 0, 0));
    assert!(
        tracker.current() > 0,
        "metadata stays owned until the final endpoint exits"
    );
    drop(port);
    drop(credit);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn another_input_keeps_progress_while_one_holder_is_retained() {
    let (credit, tracker) = credit(2, 1, 1);
    let first = credit.port(0).unwrap();
    let second = credit.port(1).unwrap();
    let retained = held(queued(&first, first.max_frame_bytes()));
    let pending = queued(&first, first.max_frame_bytes());
    assert!(matches!(
        pending.try_into_holder(),
        HolderAcquire::Pending { .. }
    ));
    drop(held(queued(&second, second.max_frame_bytes())));
    assert_eq!(second.snapshot(), (0, 0, 0, 0));
    assert_eq!(first.snapshot().1, first.max_frame_bytes());
    drop(retained);
    drop(first);
    drop(second);
    drop(credit);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn oversized_is_resource_while_temporary_full_is_pending() {
    let (credit, _) = credit(1, 1, 1);
    let port = credit.port(0).unwrap();
    let error = match port.try_queue(port.max_frame_bytes() + 1) {
        Err(error) => error,
        _ => panic!("oversized must fail"),
    };
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    let permit = queued(&port, 0);
    assert!(matches!(
        port.try_queue(0).unwrap(),
        CreditAcquire::Pending(_)
    ));
    drop(permit);
    assert!(matches!(
        port.try_queue(0).unwrap(),
        CreditAcquire::Granted(_)
    ));
}
#[test]
fn racing_last_release_and_close_are_observable_without_polling_or_sleep() {
    use std::sync::Barrier;
    let (credit, tracker) = credit(1, 1, 1);
    let port = credit.port(0).unwrap();
    let holder = held(queued(&port, 1));
    let queue = queued(&port, 1);
    let HolderAcquire::Pending { queue, wait } = queue.try_into_holder() else {
        panic!("held position")
    };
    let barrier = Arc::new(Barrier::new(2));
    let other = barrier.clone();
    let thread = std::thread::spawn(move || {
        other.wait();
        drop(holder);
    });
    barrier.wait();
    credit.close();
    thread.join().unwrap();
    assert!(wait.is_ready());
    assert!(matches!(queue.try_into_holder(), HolderAcquire::Closed(_)));
    assert!(matches!(port.try_queue(0).unwrap(), CreditAcquire::Closed));
    assert_eq!(port.snapshot(), (0, 0, 0, 0));
    drop(port);
    drop(credit);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn preparation_refuses_impossible_minimum_and_checked_envelope_overflow() {
    let parent = MemTracker::new_root("impossible credit");
    let spec = [PortCreditSpec {
        queue_positions: 1,
        holder_positions: 1,
        minimum_frame_bytes: usize::MAX,
        waiters: 1,
    }];
    for (bytes, contributors) in [(64 * 1024, 1), (usize::MAX, 2)] {
        let error =
            match OperatorByteCredit::bind(bytes, contributors, 0, &spec, parent.clone(), None) {
                Err(e) => e,
                Ok(_) => panic!("must refuse"),
            };
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(parent.current(), 0);
    }
}

#[test]
fn staging_has_one_full_byte_window_and_no_consumer_holder_path() {
    let tracker = MemTracker::new_root("BE staging fixture");
    let staging = StagingCredit::bind(64, 1, tracker.clone()).unwrap();
    let StagingAcquire::Granted(first) = staging.try_acquire(64).unwrap() else {
        panic!("full R fits");
    };
    assert_eq!(first.bytes(), 64);
    let StagingAcquire::Pending(wait) = staging.try_acquire(1).unwrap() else {
        panic!("R full");
    };
    drop(first);
    assert!(wait.is_ready());
    let StagingAcquire::Granted(last) = staging.try_acquire(64).unwrap() else {
        panic!("R returns on drop");
    };
    staging.close();
    assert!(matches!(
        staging.try_acquire(1).unwrap(),
        StagingAcquire::Closed
    ));
    drop(staging);
    assert!(tracker.current() > 0);
    drop(last);
    assert_eq!(tracker.current(), 0);
}
