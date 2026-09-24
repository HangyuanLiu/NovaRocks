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

//! Standalone protocol cost probe. The two leaves perform the same successful
//! grow/shrink pairs over an already committed 1 MiB quantum. Parent walks
//! are measured separately from this fast-path comparison.

use std::alloc::System;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use novarocks_memory::Reservation;
use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::observe::CountingAllocator;

#[path = "../src/reservation_protocol.rs"]
mod reservation_protocol;

#[global_allocator]
static ALLOCATOR: CountingAllocator<System> = CountingAllocator::new(System);

const QUANTUM: u64 = 1024 * 1024;
const DELTA: u64 = 64;

#[derive(Default)]
struct PlainMutexLeaf {
    // (free, live, peak_live, closed); matches the leaf's fast-path facts.
    values: Mutex<(u64, u64, u64, bool)>,
}

impl PlainMutexLeaf {
    fn new() -> Self {
        Self {
            values: Mutex::new((QUANTUM, 0, 0, false)),
        }
    }

    fn grow(&self, bytes: u64) {
        let mut values = self.values.lock().unwrap();
        assert!(!values.3);
        assert!(values.0 >= bytes);
        values.0 -= bytes;
        values.1 += bytes;
        values.2 = values.2.max(values.1);
    }

    fn shrink(&self, bytes: u64) {
        let mut values = self.values.lock().unwrap();
        assert!(values.1 >= bytes);
        values.1 -= bytes;
        values.0 += bytes;
        assert!(values.0 <= 2 * QUANTUM);
    }
}

#[derive(Clone, Copy)]
enum Candidate {
    Reservation,
    Protocol,
    Mutex,
}

struct AtomicProtocolLeaf {
    free: AtomicU64,
    live: AtomicU64,
    peak_live: AtomicU64,
    closed: AtomicBool,
}

impl AtomicProtocolLeaf {
    fn new() -> Self {
        Self {
            free: AtomicU64::new(QUANTUM),
            live: AtomicU64::new(0),
            peak_live: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn grow(&self, bytes: u64) {
        assert!(!self.closed.load(Ordering::Acquire));
        assert!(reservation_protocol::take_free(&self.free, bytes));
        let live = reservation_protocol::add_live(&self.live, bytes);
        let mut observed = self.peak_live.load(Ordering::Relaxed);
        while live > observed {
            match self.peak_live.compare_exchange_weak(
                observed,
                live,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }
    }

    fn shrink(&self, bytes: u64) {
        let free = reservation_protocol::release_live(&self.live, &self.free, bytes);
        assert!(free <= 2 * QUANTUM);
    }
}

fn main() {
    let threads: usize = std::env::args()
        .nth(1)
        .as_deref()
        .unwrap_or("1")
        .parse()
        .expect("threads");
    let pairs: usize = std::env::args()
        .nth(2)
        .as_deref()
        .unwrap_or("100000")
        .parse()
        .expect("pairs per thread");
    let reverse = std::env::args().nth(3).as_deref() == Some("reservation-first");
    let order = if reverse {
        [
            Candidate::Reservation,
            Candidate::Protocol,
            Candidate::Mutex,
        ]
    } else {
        [
            Candidate::Mutex,
            Candidate::Protocol,
            Candidate::Reservation,
        ]
    };
    for candidate in order {
        run(candidate, threads, pairs);
    }
}

fn run(candidate: Candidate, threads: usize, pairs: usize) {
    let mut config = AuthorityConfig::new(8 * QUANTUM, 4 * QUANTUM, 4 * QUANTUM);
    config.top_up = TopUpPolicy::uniform(QUANTUM);
    let authority = MemoryAuthority::new(config).unwrap();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Arc::new(Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap());
    leaf.try_grow(QUANTUM).unwrap();
    leaf.shrink(QUANTUM);
    let plain = Arc::new(PlainMutexLeaf::new());
    let protocol = Arc::new(AtomicProtocolLeaf::new());
    let start = Arc::new(Barrier::new(threads + 1));
    let mut joins = Vec::with_capacity(threads);
    for _ in 0..threads {
        let leaf = Arc::clone(&leaf);
        let plain = Arc::clone(&plain);
        let protocol = Arc::clone(&protocol);
        let start = Arc::clone(&start);
        joins.push(std::thread::spawn(move || {
            let mut latencies = Vec::with_capacity(pairs);
            start.wait();
            for _ in 0..pairs {
                let begin = Instant::now();
                match candidate {
                    Candidate::Reservation => {
                        leaf.try_grow(black_box(DELTA)).unwrap();
                        leaf.shrink(black_box(DELTA));
                    }
                    Candidate::Mutex => {
                        plain.grow(black_box(DELTA));
                        plain.shrink(black_box(DELTA));
                    }
                    Candidate::Protocol => {
                        protocol.grow(black_box(DELTA));
                        protocol.shrink(black_box(DELTA));
                    }
                }
                latencies.push(begin.elapsed().as_nanos() as u64);
            }
            latencies
        }));
    }
    let alloc_before = ALLOCATOR.snapshot();
    let began = Instant::now();
    start.wait();
    let mut latencies = Vec::with_capacity(threads * pairs);
    for join in joins {
        latencies.extend(join.join().unwrap());
    }
    let elapsed = began.elapsed();
    let alloc_after = ALLOCATOR.snapshot();
    latencies.sort_unstable();
    let percentile =
        |fraction: f64| -> u64 { latencies[((latencies.len() - 1) as f64 * fraction) as usize] };
    let label = match candidate {
        Candidate::Reservation => "reservation",
        Candidate::Protocol => "protocol",
        Candidate::Mutex => "mutex",
    };
    println!(
        "kind={label} threads={threads} pairs={} elapsed_ns={} pairs_per_s={:.0} median_ns={} p90_ns={} p999_ns={} allocation_calls={} live_bytes={}",
        latencies.len(),
        elapsed.as_nanos(),
        latencies.len() as f64 / elapsed.as_secs_f64(),
        percentile(0.5),
        percentile(0.9),
        percentile(0.999),
        alloc_after
            .allocations
            .saturating_sub(alloc_before.allocations),
        leaf.snapshot().live_bytes,
    );
}
