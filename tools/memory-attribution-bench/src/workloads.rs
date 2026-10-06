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

//! All variants share allocation, ownership transfer, layout and touch operations.
use crate::{
    Variant,
    manifest::{Manifest, Parameters},
    report::{self, CpuSample, SpaceDelta, SpaceSample},
};
use novarocks_memory::{
    AuthorityConfig, LaneHandle, MemoryAuthority, attribution::explicit::ExplicitOwner,
};
use serde::Serialize;
use std::{
    alloc::{self, Layout},
    hint::black_box,
    ptr::NonNull,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::Instant,
};

#[derive(Clone, Debug)]
pub enum Operation {
    Block(usize, usize),
    Chunk(Vec<usize>),
    Mixed(Vec<usize>),
    Vector {
        initial: usize,
        grow: usize,
        shrink: usize,
    },
    R1(Vec<usize>),
    Small(Vec<usize>),
}
#[derive(Clone, Debug)]
pub struct Case {
    pub workload: &'static str,
    pub subcase: String,
    pub workers: usize,
    pub remote_every: Option<usize>,
    pub scoped: bool,
    pub operation: Operation,
}
impl Case {
    fn release_threads(&self) -> usize {
        usize::from(self.remote_every.is_some())
    }
    fn operation_description(&self) -> &'static str {
        match self.operation {
            Operation::Chunk(_) => {
                "allocate one complete 4096-row batch, transfer ownership, wait for actual remote release"
            }
            Operation::Vector { .. } => {
                "grow one Vec across threshold, truncate, shrink across threshold, release"
            }
            Operation::R1(_) => "one explicit owner allocation, complete resize chain, release",
            _ if self.remote_every.is_some() => {
                "one allocation plus actual local or acknowledged remote release"
            }
            _ => "one allocation plus local release",
        }
    }
    fn sizes(&self) -> Vec<(usize, usize, u64)> {
        match &self.operation {
            Operation::Block(n, a) => vec![(*n, *a, 1)],
            Operation::Chunk(v) => v.iter().map(|n| (*n, 8, 1)).collect(),
            Operation::Mixed(v) | Operation::Small(v) | Operation::R1(v) => {
                v.iter().map(|n| (*n, 8, 1)).collect()
            }
            Operation::Vector {
                initial,
                grow,
                shrink,
            } => {
                let mut v = vec![(*initial, 1, 1)];
                let mut n = *initial;
                while n < *grow {
                    n = (n * 2).max(8);
                    v.push((n, 1, 1));
                }
                v.push((*shrink, 1, 1));
                v
            }
        }
    }
}
pub fn cases(m: &Manifest) -> Vec<Case> {
    let mut cases = Vec::new();
    for &n in &m.workloads.w1.sizes_bytes {
        for &a in &m.alignment_bytes {
            cases.push(Case {
                workload: "W1",
                subcase: format!("size-{n}-align-{a}"),
                workers: 1,
                remote_every: None,
                scoped: true,
                operation: Operation::Block(n, a),
            });
        }
    }
    let mut chunk = m.workloads.w2.buffer_sizes_bytes.clone();
    for i in 0..m.workloads.w2.small_objects {
        chunk.push(m.workloads.w2.small_sizes_bytes[i % m.workloads.w2.small_sizes_bytes.len()]);
    }
    cases.push(Case {
        workload: "W2",
        subcase: format!("chunk-{}-rows", m.workloads.w2.rows),
        workers: 1,
        remote_every: Some(1),
        scoped: true,
        operation: Operation::Chunk(chunk),
    });
    assert_eq!(m.workloads.w3.release_threads, 1);
    for &workers in &m.workloads.w3.worker_counts {
        cases.push(Case {
            workload: "W3",
            subcase: format!("mixed-{workers}-workers"),
            workers,
            remote_every: Some(m.workloads.w3.remote_release_every),
            scoped: true,
            operation: Operation::Mixed(m.workloads.w3.sizes_bytes.clone()),
        });
    }
    let r = &m.workloads.w4;
    cases.push(Case {
        workload: "W4",
        subcase: "vec".into(),
        workers: 1,
        remote_every: None,
        scoped: true,
        operation: Operation::Vector {
            initial: r.vec_initial_capacity,
            grow: r.vec_grow_len,
            shrink: r.vec_shrink_len,
        },
    });
    cases.push(Case {
        workload: "W4",
        subcase: "explicit-r1".into(),
        workers: 1,
        remote_every: None,
        scoped: true,
        operation: Operation::R1(r.r1_resize_sizes_bytes.clone()),
    });
    for &workers in &m.workloads.w5.worker_counts {
        cases.push(Case {
            workload: "W5",
            subcase: format!("small-{workers}-workers"),
            workers,
            remote_every: None,
            scoped: false,
            operation: Operation::Small(m.workloads.w5.sizes_bytes.clone()),
        });
    }
    cases
}
pub struct Context {
    _authority: MemoryAuthority,
    lanes: Vec<LaneHandle>,
}
impl Context {
    pub fn new() -> Result<Self, String> {
        let mut c = AuthorityConfig::new(64 << 20, 32 << 20, 32 << 20);
        c.max_accounts = 32;
        c.max_active_owners = 32;
        c.metadata_budget_bytes = 64 << 10;
        let authority =
            MemoryAuthority::new(c).map_err(|e| format!("authority creation failed: {e:?}"))?;
        let lanes = (0..16)
            .map(|_| {
                authority
                    .root()
                    .create_lane()
                    .map_err(|e| format!("lane creation failed: {e:?}"))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            _authority: authority,
            lanes,
        })
    }
}
struct Block {
    pointer: NonNull<u8>,
    layout: Layout,
    owner: Option<ExplicitOwner>,
}
// SAFETY: Block uniquely owns a global allocation. Transfer moves ownership;
// allocator release can occur on any thread. No aliases access the block.
unsafe impl Send for Block {}
impl Block {
    fn new(size: usize, align: usize, owner: Option<&ExplicitOwner>) -> Self {
        let layout = Layout::from_size_align(size, align).unwrap();
        let underlying = || {
            // SAFETY: positive valid layout, result is uniquely owned if nonnull.
            NonNull::new(unsafe { alloc::alloc(layout) }).ok_or(())
        };
        let pointer = owner
            .map_or_else(underlying, |o| o.allocate_with(layout, underlying))
            .unwrap_or_else(|_| alloc::handle_alloc_error(layout));
        let mut block = Self {
            pointer,
            layout,
            owner: owner.cloned(),
        };
        block.touch(false);
        block
    }
    fn resize(&mut self, size: usize) {
        let new = Layout::from_size_align(size, self.layout.align()).unwrap();
        let old = self.layout;
        let pointer = self.pointer;
        let underlying = || {
            // SAFETY: original live pointer/layout; positive new size, same alignment.
            NonNull::new(unsafe { alloc::realloc(pointer.as_ptr(), old, size) }).ok_or(())
        };
        let result = if let Some(o) = &self.owner {
            // SAFETY: helper owns the exact old block; Err preserves it.
            unsafe { o.resize_with(old, new, underlying) }
        } else {
            underlying()
        };
        self.pointer = result.unwrap_or_else(|_| alloc::handle_alloc_error(new));
        self.layout = new;
        self.touch(false);
    }
    fn touch(&mut self, resident: bool) {
        // SAFETY: these offsets are within the positive uniquely owned block.
        unsafe {
            self.pointer.as_ptr().write_volatile(0x5b);
            self.pointer
                .as_ptr()
                .add(self.layout.size() - 1)
                .write_volatile(0x6c);
            if resident {
                let page = libc::sysconf(libc::_SC_PAGESIZE);
                let page = if page > 0 { page as usize } else { 4096 };
                for offset in (0..self.layout.size()).step_by(page) {
                    self.pointer.as_ptr().add(offset).write_volatile(0x5b);
                }
            }
        }
        black_box(self.pointer);
    }
    fn usable(&self, variant: Variant) -> Option<usize> {
        variant.has_jemalloc().then(|| {
            // SAFETY: this variant allocated the still-live block with jemalloc.
            unsafe { tikv_jemallocator::usable_size(self.pointer.as_ptr()) }
        })
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        let pointer = self.pointer;
        let layout = self.layout;
        let underlying = || {
            // SAFETY: unique ownership, original requested layout, exactly one free.
            unsafe { alloc::dealloc(pointer.as_ptr(), layout) };
        };
        if let Some(o) = &self.owner {
            // SAFETY: helper is retained for the exact block's lifetime.
            unsafe { o.deallocate_with(layout, underlying) };
        } else {
            underlying()
        }
    }
}
enum Payload {
    Block(Block),
    Batch(Vec<Block>),
    Vector(Vec<u8>),
}
impl Payload {
    fn touch(&mut self) {
        match self {
            Self::Block(b) => b.touch(true),
            Self::Batch(v) => v.iter_mut().for_each(|b| b.touch(true)),
            Self::Vector(v) => {
                for byte in v.iter_mut() {
                    *byte = black_box(0x5b);
                }
            }
        }
    }
}
fn produce(case: &Case, index: usize, lane: &LaneHandle, variant: Variant) -> Payload {
    let make = || match &case.operation {
        Operation::Block(n, a) => Payload::Block(Block::new(*n, *a, None)),
        Operation::Mixed(v) | Operation::Small(v) => {
            Payload::Block(Block::new(v[index % v.len()], 8, None))
        }
        Operation::Chunk(v) => Payload::Batch(v.iter().map(|n| Block::new(*n, 8, None)).collect()),
        Operation::Vector {
            initial,
            grow,
            shrink,
        } => {
            let mut v = Vec::with_capacity(*initial);
            for i in 0..*grow {
                v.push(black_box(i as u8));
            }
            v.truncate(*shrink);
            v.shrink_to_fit();
            Payload::Vector(v)
        }
        Operation::R1(v) => {
            let owner = variant
                .is_attributing()
                .then(|| ExplicitOwner::new(lane.clone()));
            let mut block = Block::new(v[0], 8, owner.as_ref());
            for &size in &v[1..] {
                block.resize(size);
            }
            Payload::Block(block)
        }
    };
    if case.scoped && variant.is_attributing() {
        lane.run(make)
    } else {
        make()
    }
}
#[derive(Debug, Serialize)]
pub struct LayoutBin {
    pub requested_bytes: usize,
    pub alignment_bytes: usize,
    pub underlying_requested_bytes: usize,
    pub jemalloc_usable_bytes: Option<usize>,
    pub usable_over_requested_ratio: Option<f64>,
    pub requests_in_size_cycle_or_operation: u64,
}
fn layout_probe(case: &Case, variant: Variant) -> Vec<LayoutBin> {
    // These real allocation probes occur outside all timing and space passes.
    case.sizes()
        .into_iter()
        .map(|(n, a, count)| {
            let block = Block::new(n, a, None);
            let usable = block.usable(variant);
            LayoutBin {
                requested_bytes: n,
                alignment_bytes: a,
                underlying_requested_bytes: n + if variant.is_attributing() && n >= 512 {
                    8
                } else {
                    0
                },
                jemalloc_usable_bytes: usable,
                usable_over_requested_ratio: usable.map(|v| v as f64 / n as f64),
                requests_in_size_cycle_or_operation: count,
            }
        })
        .collect()
}
#[derive(Debug, Serialize)]
pub struct Timing {
    pub elapsed_ns: u128,
    pub operations: u64,
    pub ops_per_second: f64,
    pub process_cpu: CpuSample,
    pub p50_ns_per_operation: Option<u64>,
    pub p99_ns_per_operation: Option<u64>,
    pub samples: usize,
    pub remote_operation_count: u64,
    pub remote_release_fraction: f64,
}
fn percentile(sorted: &[u64], percent: usize) -> Option<u64> {
    (!sorted.is_empty()).then(|| {
        sorted[(sorted.len() * percent)
            .div_ceil(100)
            .saturating_sub(1)
            .min(sorted.len() - 1)]
    })
}
fn timing(
    case: &Case,
    context: &Context,
    variant: Variant,
    iterations: usize,
    warmup_iterations: usize,
    latency: bool,
) -> Timing {
    let barrier = Arc::new(Barrier::new(case.workers + 1 + case.release_threads()));
    let warmed = Arc::new(Barrier::new(case.workers + 1 + case.release_threads()));
    let (release_tx, release_rx) = mpsc::sync_channel::<(usize, Payload)>(1024);
    let mut acknowledgements = Vec::with_capacity(case.workers);
    let mut receivers = Vec::with_capacity(case.workers);
    for _ in 0..case.workers {
        let (tx, rx) = mpsc::sync_channel::<()>(1);
        acknowledgements.push(tx);
        receivers.push(rx);
    }
    thread::scope(|scope| {
        let release_barrier = barrier.clone();
        let release_warmed = warmed.clone();
        let release_handle = case.remote_every.map(|every| {
            scope.spawn(move || {
                // Drain the exact fixed warmup stream before the measurement
                // start barrier; workers retain the same thread-local caches.
                for _ in 0..(warmup_iterations / every) * case.workers {
                    let (worker, payload) = release_rx.recv().unwrap();
                    drop(payload);
                    acknowledgements[worker].send(()).unwrap();
                }
                release_warmed.wait();
                release_barrier.wait();
                for (worker, payload) in release_rx {
                    drop(payload);
                    acknowledgements[worker].send(()).unwrap();
                }
            })
        });
        let mut handles = Vec::with_capacity(case.workers);
        for (worker, ack) in receivers.into_iter().enumerate() {
            let b = barrier.clone();
            let worker_warmed = warmed.clone();
            let tx = release_tx.clone();
            let lane = &context.lanes[worker];
            // Sample backing is allocated before synchronization and timing.
            let mut samples = Vec::with_capacity(if latency { iterations } else { 0 });
            handles.push(scope.spawn(move || {
                for i in 0..warmup_iterations {
                    let payload = produce(case, i, lane, variant);
                    if case.remote_every.is_some_and(|n| (i + 1) % n == 0) {
                        tx.send((worker, payload)).unwrap();
                        ack.recv().unwrap();
                    } else {
                        drop(payload);
                    }
                }
                worker_warmed.wait();
                b.wait();
                let mut remote_operation_count = 0_u64;
                for i in 0..iterations {
                    let started = latency.then(Instant::now);
                    let payload = produce(case, i, lane, variant);
                    if case.remote_every.is_some_and(|n| (i + 1) % n == 0) {
                        tx.send((worker, payload)).unwrap();
                        ack.recv().unwrap();
                        // Count only measured operations after the actual remote
                        // free acknowledgement; warmup is deliberately excluded.
                        remote_operation_count += 1;
                    } else {
                        drop(payload);
                    }
                    if let Some(started) = started {
                        samples.push(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
                    }
                }
                (samples, remote_operation_count)
            }));
        }
        drop(release_tx);
        warmed.wait();
        let cpu_before = report::cpu_sample();
        let start = Instant::now();
        barrier.wait();
        let worker_results: Vec<(Vec<u64>, u64)> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        if let Some(h) = release_handle {
            h.join().unwrap();
        }
        let elapsed = start.elapsed().as_nanos();
        let cpu = report::cpu_sample().delta(cpu_before);
        let remote_operation_count = worker_results.iter().map(|(_, count)| *count).sum::<u64>();
        let mut sorted: Vec<u64> = worker_results
            .into_iter()
            .flat_map(|(samples, _)| samples)
            .collect();
        sorted.sort_unstable();
        let operations = (iterations * case.workers) as u64;
        Timing {
            elapsed_ns: elapsed,
            operations,
            ops_per_second: operations as f64 * 1e9 / elapsed.max(1) as f64,
            process_cpu: cpu,
            p50_ns_per_operation: percentile(&sorted, 50),
            p99_ns_per_operation: percentile(&sorted, 99),
            samples: sorted.len(),
            remote_operation_count,
            remote_release_fraction: if operations == 0 {
                0.0
            } else {
                remote_operation_count as f64 / operations as f64
            },
        }
    })
}
#[derive(Debug, Serialize)]
pub struct Space {
    pub baseline: SpaceSample,
    pub held: SpaceSample,
    pub released: SpaceSample,
    pub held_minus_baseline: SpaceDelta,
    pub released_minus_baseline: SpaceDelta,
    pub retained_operations: usize,
    pub retained_space_release_thread_count: usize,
}
fn space(
    case: &Case,
    context: &Context,
    variant: Variant,
    iterations: usize,
) -> Result<Space, String> {
    // Backing is reserved before baseline. This pass intentionally has no timers.
    let mut held = Vec::with_capacity(iterations * case.workers);
    let baseline = report::space_sample(variant)?;
    for worker in 0..case.workers {
        for i in 0..iterations {
            let mut value = produce(case, i, &context.lanes[worker], variant);
            value.touch();
            held.push(value);
        }
    }
    let sample = report::space_sample(variant)?;
    let remote = case.release_threads();
    if remote == 1 {
        thread::scope(|scope| {
            scope
                .spawn(|| {
                    held.clear();
                })
                .join()
                .unwrap();
        });
    } else {
        held.clear();
    }
    let released = report::space_sample(variant)?;
    Ok(Space {
        baseline,
        held: sample,
        released,
        held_minus_baseline: sample.delta(baseline),
        released_minus_baseline: released.delta(baseline),
        retained_operations: iterations * case.workers,
        retained_space_release_thread_count: remote,
    })
}
#[derive(Debug, Serialize)]
pub struct ResultRow {
    pub workload: &'static str,
    pub subcase: String,
    pub worker_count: usize,
    pub release_thread_count: usize,
    pub nominal_remote_release_fraction: f64,
    pub ambient_scope_installed: bool,
    pub explicit_owner_helper_installed: bool,
    pub operation_definition: &'static str,
    pub throughput: Timing,
    pub latency: Timing,
    pub space: Space,
    pub size_histogram: Vec<LayoutBin>,
}
pub fn run_case(
    case: &Case,
    context: &Context,
    variant: Variant,
    p: Parameters,
) -> Result<ResultRow, String> {
    let throughput = timing(
        case,
        context,
        variant,
        p.throughput_iterations_per_worker,
        p.warmup_iterations,
        false,
    );
    let latency = timing(
        case,
        context,
        variant,
        p.latency_iterations_per_worker,
        p.warmup_iterations,
        true,
    );
    let space = space(case, context, variant, p.live_hold_iterations)?;
    let size_histogram = layout_probe(case, variant);
    Ok(ResultRow {
        workload: case.workload,
        subcase: case.subcase.clone(),
        worker_count: case.workers,
        release_thread_count: case.release_threads(),
        nominal_remote_release_fraction: case.remote_every.map_or(0.0, |n| 1.0 / n as f64),
        ambient_scope_installed: case.scoped && variant.is_attributing(),
        explicit_owner_helper_installed: matches!(case.operation, Operation::R1(_))
            && variant.is_attributing(),
        operation_definition: case.operation_description(),
        throughput,
        latency,
        space,
        size_histogram,
    })
}
