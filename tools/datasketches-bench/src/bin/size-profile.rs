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

use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::fs;
use std::mem::{size_of, size_of_val};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use datasketches::hll::{HllSketch, HllType, HllUnion};
use datasketches::theta::{
    CompactThetaSketch, ThetaSketch, ThetaSketchBuilder, ThetaUnion, ThetaUnionBuilder,
};
use novarocks_datasketches_bench::{
    deterministic_value, theta_partitions, workload_sha256, workloads,
};
use serde::Serialize;

struct TrackingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        DEALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, old, new_size) };
        if !new_pointer.is_null() {
            LIVE_BYTES.fetch_sub(old.size(), Ordering::Relaxed);
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
            DEALLOCATED_BYTES.fetch_add(old.size(), Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size, Ordering::Relaxed);
            DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        new_pointer
    }
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    live_bytes: usize,
    allocated_bytes: usize,
    deallocated_bytes: usize,
    alloc_calls: usize,
    dealloc_calls: usize,
}

impl AllocationSnapshot {
    fn capture() -> Self {
        Self {
            live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
            alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
            dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
        }
    }

    fn delta(self, before: Self) -> AllocationDelta {
        AllocationDelta {
            retained_heap_bytes: self.live_bytes.saturating_sub(before.live_bytes),
            allocated_bytes: self.allocated_bytes.saturating_sub(before.allocated_bytes),
            deallocated_bytes: self
                .deallocated_bytes
                .saturating_sub(before.deallocated_bytes),
            alloc_calls: self.alloc_calls.saturating_sub(before.alloc_calls),
            dealloc_calls: self.dealloc_calls.saturating_sub(before.dealloc_calls),
        }
    }
}

#[derive(Debug, Serialize)]
struct AllocationDelta {
    retained_heap_bytes: usize,
    allocated_bytes: usize,
    deallocated_bytes: usize,
    alloc_calls: usize,
    dealloc_calls: usize,
}

#[derive(Debug, Serialize)]
struct SizePoint {
    family: &'static str,
    phase: String,
    target: Option<&'static str>,
    lg_k: u8,
    input_count: usize,
    estimated_size_bytes: usize,
    inline_size_bytes: usize,
    public_api_heap_bytes: usize,
    allocation: AllocationDelta,
    serialized_bytes: Option<usize>,
    evidence: &'static str,
}

#[derive(Debug, Serialize)]
struct Environment {
    git_head: String,
    os: String,
    architecture: String,
    cpu: String,
    rustc: String,
    profile: String,
    features: [&'static str; 2],
    dependency: &'static str,
    workload_sha256: String,
    warmup_seconds: u64,
    sample_count: usize,
    command: String,
}

#[derive(Debug, Serialize)]
struct SizeReport {
    schema_version: u32,
    environment: Environment,
    points: Vec<SizePoint>,
    assertions: Vec<&'static str>,
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn parse_output_path() -> PathBuf {
    let mut arguments = env::args_os().skip(1);
    match (arguments.next(), arguments.next(), arguments.next()) {
        (Some(flag), Some(path), None) if flag == "--output" => PathBuf::from(path),
        _ => {
            eprintln!("usage: size-profile --output <path>");
            std::process::exit(2);
        }
    }
}

fn target_name(target: HllType) -> &'static str {
    match target {
        HllType::Hll4 => "HLL4",
        HllType::Hll6 => "HLL6",
        HllType::Hll8 => "HLL8",
    }
}

fn profile_hll<F>(
    phase: impl Into<String>,
    target: HllType,
    lg_k: u8,
    input_count: usize,
    build: F,
) -> SizePoint
where
    F: FnOnce() -> HllSketch,
{
    let before = AllocationSnapshot::capture();
    let sketch = build();
    let after = AllocationSnapshot::capture();
    let allocation = after.delta(before);
    let inline_size = size_of_val(&sketch);
    let estimated_size = sketch.estimated_size();
    let public_heap = estimated_size - inline_size;
    assert_eq!(sketch.target_type(), target);
    assert_eq!(
        public_heap, allocation.retained_heap_bytes,
        "HllSketch::estimated_size must equal current retained allocation"
    );
    let serialized_bytes = sketch.serialize().len();
    SizePoint {
        family: "hll_sketch",
        phase: phase.into(),
        target: Some(target_name(target)),
        lg_k,
        input_count,
        estimated_size_bytes: estimated_size,
        inline_size_bytes: inline_size,
        public_api_heap_bytes: public_heap,
        allocation,
        serialized_bytes: Some(serialized_bytes),
        evidence: "estimated_size minus inline size equals allocator-observed current heap",
    }
}

fn profile_hll_union<F>(
    phase: impl Into<String>,
    lg_k: u8,
    input_count: usize,
    build: F,
) -> SizePoint
where
    F: FnOnce() -> HllUnion,
{
    let before = AllocationSnapshot::capture();
    let union = build();
    let after = AllocationSnapshot::capture();
    let allocation = after.delta(before);
    let inline_size = size_of_val(&union);
    let estimated_size = union.estimated_size();
    let public_heap = estimated_size - inline_size;
    assert_eq!(
        public_heap, allocation.retained_heap_bytes,
        "HllUnion::estimated_size must equal current retained allocation"
    );
    let serialized_bytes = union.to_sketch(HllType::Hll8).serialize().len();
    SizePoint {
        family: "hll_union",
        phase: phase.into(),
        target: Some("internal HLL8"),
        lg_k,
        input_count,
        estimated_size_bytes: estimated_size,
        inline_size_bytes: inline_size,
        public_api_heap_bytes: public_heap,
        allocation,
        serialized_bytes: Some(serialized_bytes),
        evidence: "estimated_size minus inline size equals allocator-observed current union heap",
    }
}

fn profile_theta<F>(phase: impl Into<String>, lg_k: u8, input_count: usize, build: F) -> SizePoint
where
    F: FnOnce() -> ThetaSketch,
{
    let before = AllocationSnapshot::capture();
    let sketch = build();
    let after = AllocationSnapshot::capture();
    let allocation = after.delta(before);
    let inline_size = size_of_val(&sketch);
    let estimated_size = sketch.estimated_size();
    let public_heap = estimated_size - inline_size;
    assert_eq!(
        public_heap, allocation.retained_heap_bytes,
        "ThetaSketch::estimated_size must equal current retained allocation"
    );
    let serialized_bytes = sketch.compact(true).serialize().len();
    SizePoint {
        family: "theta_sketch",
        phase: phase.into(),
        target: None,
        lg_k,
        input_count,
        estimated_size_bytes: estimated_size,
        inline_size_bytes: inline_size,
        public_api_heap_bytes: public_heap,
        allocation,
        serialized_bytes: Some(serialized_bytes),
        evidence: "estimated_size minus inline size equals allocator-observed current heap",
    }
}

fn profile_theta_compact<F>(
    phase: impl Into<String>,
    lg_k: u8,
    input_count: usize,
    build: F,
) -> SizePoint
where
    F: FnOnce() -> CompactThetaSketch,
{
    let before = AllocationSnapshot::capture();
    let compact = build();
    let after = AllocationSnapshot::capture();
    let allocation = after.delta(before);
    let inline_size = size_of_val(&compact);
    let estimated_size = compact.estimated_size();
    let public_heap = estimated_size - inline_size;
    assert_eq!(
        public_heap, allocation.retained_heap_bytes,
        "CompactThetaSketch::estimated_size must equal current retained allocation"
    );
    let serialized_bytes = compact.serialize().len();
    SizePoint {
        family: "theta_compact",
        phase: phase.into(),
        target: None,
        lg_k,
        input_count,
        estimated_size_bytes: estimated_size,
        inline_size_bytes: inline_size,
        public_api_heap_bytes: public_heap,
        allocation,
        serialized_bytes: Some(serialized_bytes),
        evidence: "compact current capacity is measured independently of serialized length",
    }
}

fn profile_theta_union<F>(
    phase: impl Into<String>,
    lg_k: u8,
    input_count: usize,
    build: F,
) -> SizePoint
where
    F: FnOnce() -> ThetaUnion,
{
    let before = AllocationSnapshot::capture();
    let union = build();
    let after = AllocationSnapshot::capture();
    let allocation = after.delta(before);
    let inline_size = size_of_val(&union);
    let estimated_size = union.estimated_size();
    let public_heap = estimated_size - inline_size;
    assert_eq!(
        public_heap, allocation.retained_heap_bytes,
        "ThetaUnion::estimated_size must equal current retained allocation"
    );
    let serialized_bytes = union.to_sketch(true).serialize().len();
    SizePoint {
        family: "theta_union",
        phase: phase.into(),
        target: None,
        lg_k,
        input_count,
        estimated_size_bytes: estimated_size,
        inline_size_bytes: inline_size,
        public_api_heap_bytes: public_heap,
        allocation,
        serialized_bytes: Some(serialized_bytes),
        evidence: "union retained state uses public estimated_size, not result serialization",
    }
}

fn build_hll(target: HllType, lg_k: u8, count: usize, seed: u64) -> HllSketch {
    let mut sketch = HllSketch::new(lg_k, target).unwrap();
    for index in 0..count {
        sketch.update(deterministic_value(index, seed));
    }
    sketch
}

fn build_theta(lg_k: u8, count: usize, seed: u64) -> ThetaSketch {
    let mut sketch = ThetaSketchBuilder::default().lg_k(lg_k).build().unwrap();
    for index in 0..count {
        sketch.update(deterministic_value(index, seed));
    }
    sketch
}

fn find_hll4_aux(
    lg_k: u8,
    seed: u64,
    dense_start: usize,
    scan_limit: usize,
    dense_no_aux_heap: usize,
) -> (HllSketch, usize) {
    let mut sketch = build_hll(HllType::Hll4, lg_k, dense_start, seed);
    assert_eq!(
        sketch.estimated_size() - size_of::<HllSketch>(),
        dense_no_aux_heap,
        "the deterministic dense-start checkpoint must be Array4 without AuxMap"
    );
    for index in dense_start..scan_limit {
        sketch.update(deterministic_value(index, seed));
        if sketch.estimated_size() - size_of::<HllSketch>() > dense_no_aux_heap {
            return (sketch, index + 1);
        }
    }
    panic!("deterministic HLL4 workload did not reach AuxMap by {scan_limit} updates");
}

fn hll_points() -> Vec<SizePoint> {
    let workload = workloads();
    let lg_k = workload.hll.lg_k;
    let seed = workload.seed;
    let mut points = Vec::new();

    for (phase, count) in [
        ("list-empty", 0),
        ("list-last-slot", 7),
        ("set-first-capacity", 8),
        ("set-resized", 25),
    ] {
        points.push(profile_hll(phase, HllType::Hll8, lg_k, count, || {
            build_hll(HllType::Hll8, lg_k, count, seed)
        }));
    }

    for target in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        points.push(profile_hll(
            format!("dense-{}", target_name(target).to_ascii_lowercase()),
            target,
            lg_k,
            workload.hll.dense_updates,
            || build_hll(target, lg_k, workload.hll.dense_updates, seed),
        ));
    }

    let heap_for = |phase: &str| {
        points
            .iter()
            .find(|point| point.phase == phase)
            .unwrap()
            .public_api_heap_bytes
    };
    assert_eq!(heap_for("list-empty"), heap_for("list-last-slot"));
    assert!(heap_for("set-first-capacity") > heap_for("list-last-slot"));
    assert!(heap_for("set-resized") > heap_for("set-first-capacity"));
    assert!(heap_for("dense-hll4") < heap_for("dense-hll6"));
    assert!(heap_for("dense-hll6") < heap_for("dense-hll8"));
    let dense_hll4_heap = heap_for("dense-hll4");

    let before = AllocationSnapshot::capture();
    let (aux_sketch, aux_count) = find_hll4_aux(
        lg_k,
        seed,
        workload.hll.dense_updates,
        workload.hll.aux_scan_limit,
        dense_hll4_heap,
    );
    let after = AllocationSnapshot::capture();
    let allocation = after.delta(before);
    let inline_size = size_of_val(&aux_sketch);
    let estimated_size = aux_sketch.estimated_size();
    let public_heap = estimated_size - inline_size;
    assert_eq!(public_heap, allocation.retained_heap_bytes);
    assert!(public_heap > dense_hll4_heap);
    points.push(SizePoint {
        family: "hll_sketch",
        phase: "dense-hll4-with-aux-map".to_owned(),
        target: Some("HLL4"),
        lg_k,
        input_count: aux_count,
        estimated_size_bytes: estimated_size,
        inline_size_bytes: inline_size,
        public_api_heap_bytes: public_heap,
        allocation,
        serialized_bytes: Some(aux_sketch.serialize().len()),
        evidence: "heap exceeds packed Array4 bytes and exactly matches allocator-observed AuxMap capacity",
    });

    for (phase, count) in [
        ("union-list", 7),
        ("union-set", 8),
        ("union-array8", workload.hll.dense_updates),
    ] {
        points.push(profile_hll_union(phase, lg_k, count, || {
            let mut union = HllUnion::new(lg_k).unwrap();
            for index in 0..count {
                union.update_value(deterministic_value(index, seed));
            }
            union
        }));
    }

    points.push(profile_hll_union(
        "union-downsampled-array8",
        workload.hll.downsample_lg_k,
        workload.hll.dense_updates,
        || {
            let input = build_hll(
                HllType::Hll4,
                workload.hll.downsample_lg_k,
                workload.hll.dense_updates,
                seed,
            );
            let mut union = HllUnion::new(lg_k).unwrap();
            union.update(&input);
            union
        },
    ));

    points
}

fn theta_points() -> Vec<SizePoint> {
    let workload = workloads();
    let lg_k = workload.theta.lg_k;
    let seed = workload.seed;
    let mut points = Vec::new();
    for (phase, count) in [
        ("empty", 0),
        ("initial-table", 1),
        ("resize-64", 64),
        ("nominal-k", 1usize << lg_k),
        ("estimation", 1usize << (lg_k + 2)),
    ] {
        points.push(profile_theta(phase, lg_k, count, || {
            build_theta(lg_k, count, seed)
        }));
    }

    let mutable = build_theta(lg_k, 1usize << (lg_k + 2), seed);
    points.push(profile_theta_compact(
        "ordered-after-estimation",
        lg_k,
        1usize << (lg_k + 2),
        || mutable.compact(true),
    ));
    points.push(profile_theta_compact(
        "unordered-after-estimation",
        lg_k,
        1usize << (lg_k + 2),
        || mutable.compact(false),
    ));

    for overlap in &workload.theta.overlaps_percent {
        let inputs = theta_partitions(*overlap);
        let total_inputs = workload.theta.partitions * workload.theta.partition_size;
        points.push(profile_theta_union(
            format!("flat-overlap-{overlap}"),
            lg_k,
            total_inputs,
            || {
                let mut union = ThetaUnionBuilder::default().lg_k(lg_k).build().unwrap();
                for input in &inputs {
                    union.update(input).unwrap();
                }
                union
            },
        ));

        let mut level = inputs;
        let mut depth = 0;
        while level.len() > 1 {
            let current = level;
            let representative = current.iter().take(2).cloned().collect::<Vec<_>>();
            let represented_inputs = workload
                .theta
                .partition_size
                .saturating_mul(1usize << (depth + 1))
                .min(total_inputs);
            points.push(profile_theta_union(
                format!("tree-overlap-{overlap}-union-level-{}", depth + 1),
                lg_k,
                represented_inputs,
                || {
                    let mut union = ThetaUnionBuilder::default().lg_k(lg_k).build().unwrap();
                    for input in &representative {
                        union.update(input).unwrap();
                    }
                    union
                },
            ));
            let mut next = Vec::with_capacity(current.len().div_ceil(2));
            for pair in current.chunks(2) {
                let mut union = ThetaUnionBuilder::default().lg_k(lg_k).build().unwrap();
                for input in pair {
                    union.update(input).unwrap();
                }
                next.push(union.to_sketch(true));
            }
            depth += 1;
            let last = next.last().unwrap().clone();
            points.push(profile_theta_compact(
                format!("tree-overlap-{overlap}-level-{depth}"),
                lg_k,
                represented_inputs,
                || last.clone(),
            ));
            level = next;
        }
    }
    points
}

fn assert_o_k_trends(points: &mut Vec<SizePoint>) {
    let workload = workloads();
    for lg_k in [8u8, 10, 12, 14] {
        let count = 1usize << (lg_k + 2);
        points.push(profile_theta(
            format!("o-k-lg-k-{lg_k}"),
            lg_k,
            count,
            || build_theta(lg_k, count, workload.seed),
        ));
        for target in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
            points.push(profile_hll(
                format!("o-k-lg-k-{lg_k}"),
                target,
                lg_k,
                count,
                || build_hll(target, lg_k, count, workload.seed),
            ));
        }
    }

    for point in points
        .iter()
        .filter(|point| point.phase.starts_with("o-k-"))
    {
        let k = 1usize << point.lg_k;
        assert!(
            point.public_api_heap_bytes <= k * 64,
            "retained memory must remain O(k): {point:?}"
        );
    }
}

fn environment() -> Environment {
    let workload = workloads();
    Environment {
        git_head: command_output("git", &["rev-parse", "HEAD"]),
        os: command_output("sw_vers", &["-productVersion"]),
        architecture: command_output("uname", &["-m"]),
        cpu: command_output("sysctl", &["-n", "machdep.cpu.brand_string"]),
        rustc: command_output("rustc", &["--version"]),
        profile: workload.measurement.profile.clone(),
        features: ["theta", "hll"],
        dependency: "crates.io datasketches =0.5.0-rc.1",
        workload_sha256: workload_sha256(),
        warmup_seconds: workload.measurement.warmup_seconds,
        sample_count: workload.measurement.sample_count,
        command: env::args().collect::<Vec<_>>().join(" "),
    }
}

fn write_report(path: &Path, report: &SizeReport) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let json = serde_json::to_vec_pretty(report).unwrap();
    fs::write(path, json).unwrap();
}

fn main() {
    let output = parse_output_path();
    let environment = environment();
    let mut points = theta_points();
    points.extend(hll_points());
    assert_o_k_trends(&mut points);
    let report = SizeReport {
        schema_version: 1,
        environment,
        points,
        assertions: vec![
            "public estimated_size equals inline object plus allocator-observed retained heap",
            "HLL list, set, Array4, Array6, Array8, AuxMap, union, and mode transitions are covered by deterministic update phases",
            "Theta mutable, compact, flat union, tree union, resize, nominal-k, and estimation phases are covered",
            "serialized length is reported separately and is never used as retained memory",
            "lg_k is used only to select workloads and verify O(k) trend, never as a retained-size substitute",
        ],
    };
    write_report(&output, &report);
    println!(
        "wrote {} size points to {}",
        report.points.len(),
        output.display()
    );
}
