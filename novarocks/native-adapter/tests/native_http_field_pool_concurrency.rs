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

//! Concurrent consumers of one original-funded field arena. These tests cover
//! the pool and escaped fields, not connection scheduling or a whole IO graph.

use bytes::Bytes;
use hyper::http::HeaderValue;
use hyper::http::header::{HeaderFieldAllocationPool, HeaderFieldFillError};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

const DEADLINE: Duration = Duration::from_secs(3);
const DATE: &[u8] = b"Sat, 03 Oct 2026 08:00:00 GMT";

struct OriginalExit {
    exited: Arc<AtomicUsize>,
    _credit: ResultWriteCredit,
}
impl Drop for OriginalExit {
    fn drop(&mut self) {
        self.exited.fetch_add(1, Ordering::SeqCst);
    }
}
struct Funded {
    pool: HeaderFieldAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    exited: Arc<AtomicUsize>,
}
fn grant(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    match budget.try_reserve_process(bytes).unwrap() {
        ResultWriteAdmission::Granted(credit) => credit,
        ResultWriteAdmission::Blocked => panic!("original capacity remains held"),
    }
}
impl Funded {
    fn new(capacity: usize, positions: usize, max: usize) -> Self {
        let total = HeaderFieldAllocationPool::allocation_capacity_bound(capacity, positions, max)
            .unwrap()
            + Bytes::owner_with_exit_guard_metadata_size::<Bytes, OriginalExit>();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let exited = Arc::new(AtomicUsize::new(0));
        let ownership = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            OriginalExit {
                exited: exited.clone(),
                _credit: grant(&budget, total),
            },
        );
        let pool = HeaderFieldAllocationPool::new(capacity, positions, max, ownership).unwrap();
        Self {
            pool,
            budget,
            total,
            exited,
        }
    }
}
fn filled(pool: &HeaderFieldAllocationPool, len: usize, byte: u8) -> Bytes {
    pool.try_fill(len, |output| {
        output.fill(byte);
        Ok::<_, ()>(())
    })
    .unwrap()
}
fn held(budget: &Arc<ResultRetainedBudget>, total: usize) {
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}

#[test]
fn pending_fill_does_not_refuse_original_date_and_generated_header_consumers() {
    let f = Funded::new(256, 4, 64);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first_pool = f.pool.clone();
    let first = std::thread::spawn(move || {
        first_pool.try_fill(64, |output| {
            output.fill(b'a');
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(DEADLINE).unwrap();
            Ok::<_, ()>(())
        })
    });
    entered_rx.recv_timeout(DEADLINE).unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let other_pool = f.pool.clone();
    let other = std::thread::spawn(move || {
        // This is the public shared HeaderValue conversion used by Hyper's
        // automatic Date path, with an independent fixed IMF-fixdate oracle.
        let date = other_pool
            .try_fill(DATE.len(), |output| {
                output.copy_from_slice(DATE);
                Ok::<_, ()>(())
            })
            .map(|bytes| HeaderValue::from_maybe_shared(bytes).unwrap());
        let number = HeaderValue::try_from_u64_with_pool(18446744073709551615, &other_pool);
        done_tx.send((date, number)).unwrap();
    });
    let completed = done_rx.recv_timeout(DEADLINE);
    // Release before checking failures so a callback-under-lock regression
    // fails the timeout oracle without leaving its first callback blocked.
    let _ = release_tx.send(());
    let first = first.join().unwrap().unwrap();
    other.join().unwrap();
    let (date, number) =
        completed.expect("consumers must complete while the first fill is pending");
    let date = date.expect("sufficient original extent and positions for Date");
    let number = number.expect("sufficient original extent and positions for decimal field");
    assert_eq!(first.as_ref(), &[b'a'; 64]);
    assert_eq!(date.as_bytes(), DATE);
    assert_eq!(number.as_bytes(), b"18446744073709551615");
    assert_eq!(f.pool.available_positions(), 1);
    drop((first, date, number));
    assert_eq!(f.pool.available_positions(), 4);
}

#[test]
fn reentrant_fill_has_disjoint_bytes_and_exact_alias_retirement() {
    let f = Funded::new(256, 4, 128);
    let mut inner = None;
    let outer = f
        .pool
        .try_fill(128, |output| {
            output.fill(b'a');
            inner = Some(filled(&f.pool, 64, b'b'));
            assert_eq!(f.pool.available_positions(), 2);
            Ok::<_, ()>(())
        })
        .unwrap();
    let inner = inner.unwrap();
    assert_eq!(outer.as_ref(), &[b'a'; 128]);
    assert_eq!(inner.as_ref(), &[b'b'; 64]);
    assert_eq!(inner.as_ptr(), outer.as_ptr().wrapping_add(128));
    let value = HeaderValue::from_maybe_shared(inner).unwrap();
    let alias = value.clone();
    let budget = f.budget.clone();
    let exited = f.exited.clone();
    let total = f.total;
    drop((f, outer, value));
    assert_eq!(exited.load(Ordering::SeqCst), 0);
    held(&budget, total);
    assert_eq!(alias.as_bytes(), &[b'b'; 64]);
    drop(alias);
    assert_eq!(exited.load(Ordering::SeqCst), 1);
    drop(grant(&budget, total));
}

#[test]
fn eight_overlapping_callbacks_fill_all_real_positions_without_transient_exhaustion() {
    let f = Funded::new(512, 8, 64);
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_tx, entered_rx) = mpsc::channel();
    let mut handles = Vec::new();
    for byte in b'a'..=b'h' {
        let pool = f.pool.clone();
        let release = release.clone();
        let entered = entered_tx.clone();
        handles.push(std::thread::spawn(move || {
            pool.try_fill(64, |output| {
                output.fill(byte);
                entered.send(byte).unwrap();
                let (lock, notify) = &*release;
                let deadline = Instant::now() + DEADLINE;
                let mut ready = lock.lock().unwrap();
                while !*ready {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    assert!(!remaining.is_zero(), "test release must arrive");
                    ready = notify.wait_timeout(ready, remaining).unwrap().0;
                }
                Ok::<_, ()>(())
            })
        }));
    }
    drop(entered_tx);
    let mut entered = 0;
    while entered < 8 {
        if entered_rx.recv_timeout(DEADLINE).is_err() {
            break;
        }
        entered += 1;
    }
    let extra = f.pool.try_fill(1, |_| Ok::<_, ()>(()));
    let (lock, notify) = &*release;
    *lock.lock().unwrap() = true;
    notify.notify_all();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        entered, 8,
        "every sufficient claim must reach its callback concurrently"
    );
    assert!(matches!(extra, Err(HeaderFieldFillError::Exhausted)));
    let outputs = results.into_iter().map(Result::unwrap).collect::<Vec<_>>();
    assert_eq!(f.pool.available_positions(), 0);
    for (index, output) in outputs.iter().enumerate() {
        assert_eq!(output.as_ref(), &[b'a' + index as u8; 64]);
        for other in &outputs[..index] {
            assert_ne!(output.as_ptr(), other.as_ptr());
        }
    }
    drop(outputs);
    assert_eq!(f.pool.available_positions(), 8);
}

#[test]
fn failed_and_panicking_reentrant_callbacks_retire_only_their_own_extent() {
    let f = Funded::new(256, 4, 128);
    let mut nested = None;
    let failure = f.pool.try_fill(128, |output| {
        output.fill(b'x');
        nested = Some(filled(&f.pool, 64, b'n'));
        Err(37u8)
    });
    assert!(matches!(failure, Err(HeaderFieldFillError::Fill(37))));
    let nested = nested.unwrap();
    assert_eq!(f.pool.available_positions(), 3);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        f.pool.try_fill(128, |output| -> Result<(), ()> {
            output.fill(b'p');
            panic!("original callback panic");
        })
    }))
    .unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"original callback panic")
    );
    assert_eq!(f.pool.available_positions(), 3);
    let reused = filled(&f.pool, 128, b'r');
    assert_eq!(nested.as_ref(), &[b'n'; 64]);
    assert_eq!(reused.as_ref(), &[b'r'; 128]);
    drop((nested, reused));
    assert_eq!(f.pool.available_positions(), 4);
}

#[test]
fn true_fragmentation_and_position_exhaustion_still_refuse_without_callback() {
    let f = Funded::new(256, 4, 128);
    let a = filled(&f.pool, 64, b'a');
    let b = filled(&f.pool, 64, b'b');
    let c = filled(&f.pool, 64, b'c');
    let d = filled(&f.pool, 64, b'd');
    assert!(matches!(
        f.pool
            .try_fill(1, |_| -> Result<(), ()> { panic!("no free position") }),
        Err(HeaderFieldFillError::Exhausted)
    ));
    drop((a, c));
    assert_eq!(f.pool.available_positions(), 2);
    assert!(matches!(
        f.pool.try_fill(128, |_| -> Result<(), ()> {
            panic!("no contiguous extent")
        }),
        Err(HeaderFieldFillError::Exhausted)
    ));
    drop(b);
    let merged = filled(&f.pool, 128, b'm');
    assert_eq!(merged.as_ref(), &[b'm'; 128]);
    assert_eq!(d.as_ref(), &[b'd'; 64]);
    drop((merged, d));
    assert_eq!(f.pool.available_positions(), 4);
}
