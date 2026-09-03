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
use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

use datasketches::hll::{HllSketch, HllType};
use novarocks_execution::exec::hll::{HllAllocationUpperBounds, HllHandle, HllTargetType};

struct TrackingAllocator;

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static RESERVATION_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

static LIVE_DELTA: AtomicIsize = AtomicIsize::new(0);
static PEAK_LIVE_DELTA: AtomicIsize = AtomicIsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATION_WITHOUT_RESERVATION: AtomicBool = AtomicBool::new(false);
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn tracking_reservation() -> Option<bool> {
    TRACKING.with(|tracking| tracking.get().then(|| RESERVATION_ACTIVE.with(Cell::get)))
}

fn record_live_delta(delta: isize) {
    let current = LIVE_DELTA.fetch_add(delta, Ordering::Relaxed) + delta;
    let mut peak = PEAK_LIVE_DELTA.load(Ordering::Relaxed);
    while current > peak {
        match PEAK_LIVE_DELTA.compare_exchange_weak(
            peak,
            current,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

fn record_allocation(size: usize, reservation_active: bool) {
    if !reservation_active {
        ALLOCATION_WITHOUT_RESERVATION.store(true, Ordering::Relaxed);
    }
    ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
    record_live_delta(size as isize);
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null()
            && let Some(active) = tracking_reservation()
        {
            record_allocation(layout.size(), active);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        if tracking_reservation().is_some() {
            record_live_delta(-(layout.size() as isize));
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null()
            && let Some(active) = tracking_reservation()
        {
            record_allocation(layout.size(), active);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, old, new_size) };
        if !new_pointer.is_null()
            && let Some(active) = tracking_reservation()
        {
            // Treat realloc as old and new storage coexisting to preserve a conservative peak.
            record_allocation(new_size, active);
            record_live_delta(-(old.size() as isize));
        }
        new_pointer
    }
}

struct TrackingScope;

impl Drop for TrackingScope {
    fn drop(&mut self) {
        TRACKING.with(|tracking| tracking.set(false));
        RESERVATION_ACTIVE.with(|active| active.set(false));
    }
}

struct ReservationGuard;

impl ReservationGuard {
    fn acquire() -> Self {
        RESERVATION_ACTIVE.with(|active| {
            assert!(!active.replace(true), "reservation is already active");
        });
        Self
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        RESERVATION_ACTIVE.with(|active| {
            assert!(active.replace(false), "reservation was not active");
        });
    }
}

#[derive(Debug)]
struct AllocationMeasurement {
    peak_live_delta: usize,
    allocated_bytes: usize,
    allocation_without_reservation: bool,
}

fn measure_allocations<T>(operation: impl FnOnce() -> T) -> (T, AllocationMeasurement) {
    LIVE_DELTA.store(0, Ordering::Relaxed);
    PEAK_LIVE_DELTA.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_WITHOUT_RESERVATION.store(false, Ordering::Relaxed);
    RESERVATION_ACTIVE.with(|active| active.set(false));
    TRACKING.with(|tracking| tracking.set(true));
    let scope = TrackingScope;
    let result = operation();
    drop(scope);
    (
        result,
        AllocationMeasurement {
            peak_live_delta: PEAK_LIVE_DELTA.load(Ordering::Relaxed).max(0) as usize,
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            allocation_without_reservation: ALLOCATION_WITHOUT_RESERVATION.load(Ordering::Relaxed),
        },
    )
}

fn with_reservation<T>(operation: impl FnOnce(&ReservationGuard) -> T) -> T {
    let guard = ReservationGuard::acquire();
    let result = operation(&guard);
    assert!(
        RESERVATION_ACTIVE.with(Cell::get),
        "operation must not release the caller-owned reservation"
    );
    // A real caller reconciles the retained charge here while the guard is still live.
    drop(guard);
    result
}

fn target_type(target: HllType) -> HllTargetType {
    match target {
        HllType::Hll4 => HllTargetType::Hll4,
        HllType::Hll6 => HllTargetType::Hll6,
        HllType::Hll8 => HllTargetType::Hll8,
    }
}

fn mixed_value(index: usize, seed: u64) -> u64 {
    let mut value = (index as u64)
        .wrapping_add(seed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn payload(target: HllType, lg_k: u8, count: usize, seed: u64) -> Vec<u8> {
    let mut sketch = HllSketch::new(lg_k, target).expect("create input sketch");
    for index in 0..count {
        sketch.update(mixed_value(index, seed));
    }
    sketch.serialize()
}

fn hll4_aux_payload() -> Vec<u8> {
    let mut sketch = HllSketch::new(12, HllType::Hll4).expect("create HLL4 sketch");
    let initial_size = sketch.estimated_size();
    for index in 0..2_000_000 {
        sketch.update(mixed_value(index, 7_640_891_576_956_012_809));
        if sketch.estimated_size() > initial_size + (1 << 11) {
            return sketch.serialize();
        }
    }
    panic!("deterministic HLL4 workload did not reach AuxMap");
}

fn populated_handle(target: HllType, lg_k: u8, count: usize, seed: u64) -> HllHandle {
    let mut handle = HllHandle::new_unreserved(lg_k, target_type(target)).expect("create handle");
    for index in 0..count {
        handle
            .update_hash_unreserved(mixed_value(index, seed))
            .expect("update handle");
    }
    handle
}

fn assert_peak_bound(
    label: &str,
    bounds: HllAllocationUpperBounds,
    measured: &AllocationMeasurement,
) {
    let absolute_peak = bounds.current_bytes + measured.peak_live_delta;
    assert!(
        absolute_peak <= bounds.operation_peak_bytes,
        "{label}: measured peak {absolute_peak} exceeds {bounds:?}; {measured:?}"
    );
    assert_eq!(
        bounds.additional_headroom_bytes(),
        bounds.operation_peak_bytes - bounds.current_bytes
    );
    assert!(
        !measured.allocation_without_reservation,
        "{label}: allocation happened without the caller guard: {measured:?}"
    );
}

fn assert_from_payload_bound(label: &str, input: &[u8]) {
    let (preflight, measured_preflight) =
        measure_allocations(|| HllHandle::from_payload_allocation_preflight(input));
    let preflight = preflight.expect("payload preflight");
    assert_eq!(measured_preflight.allocated_bytes, 0, "{label}: preflight");
    let bounds = preflight.bounds();
    assert_eq!(bounds.current_bytes, 0);
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| {
            HllHandle::from_payload_under_reservation(input, &preflight, guard)
        })
    });
    let (handle, outcome) = result.expect("initialize from payload");
    assert_eq!(
        outcome.current_bytes,
        handle.current_allocation_upper_bound()
    );
    assert_eq!(outcome.operation_peak_bytes, bounds.operation_peak_bytes);
    assert_peak_bound(label, bounds, &measured);
}

fn assert_merge_bound(label: &str, mut handle: HllHandle, input: &[u8]) {
    let (preflight, measured_preflight) =
        measure_allocations(|| handle.merge_payload_allocation_preflight(input));
    let preflight = preflight.expect("merge preflight");
    assert_eq!(measured_preflight.allocated_bytes, 0, "{label}: preflight");
    let bounds = preflight.bounds();
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| handle.merge_payload_under_reservation(input, &preflight, guard))
    });
    let outcome = result.expect("merge payload");
    assert_eq!(
        outcome.current_bytes,
        handle.current_allocation_upper_bound()
    );
    assert_eq!(outcome.operation_peak_bytes, bounds.operation_peak_bytes);
    assert_peak_bound(label, bounds, &measured);
}

fn reserved_update(handle: &mut HllHandle, hash: u64) -> HllAllocationUpperBounds {
    let (preflight, measured_preflight) =
        measure_allocations(|| handle.update_hash_allocation_preflight());
    assert_eq!(measured_preflight.allocated_bytes, 0, "update preflight");
    let bounds = preflight.bounds();
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| handle.update_hash_under_reservation(hash, &preflight, guard))
    });
    let outcome = result.expect("reserved update");
    assert_eq!(
        outcome.current_bytes,
        handle.current_allocation_upper_bound()
    );
    assert_peak_bound("update", bounds, &measured);
    outcome
}

#[test]
fn preflight_bounds_cover_modes_targets_aux_and_transitions() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let lg_k = 10;
    for target in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        let token =
            HllHandle::new_allocation_preflight(lg_k, target_type(target)).expect("new preflight");
        let bounds = token.bounds();
        let (result, measured) = measure_allocations(|| {
            with_reservation(|guard| HllHandle::new_under_reservation(&token, guard))
        });
        let (mut handle, outcome) = result.expect("create handle");
        assert_eq!(
            outcome.current_bytes,
            handle.current_allocation_upper_bound()
        );
        assert_peak_bound("new", bounds, &measured);

        let mut observed_sizes = vec![outcome.current_bytes];
        for index in 0..256 {
            let outcome = reserved_update(&mut handle, mixed_value(index, 0x7711_0000));
            if observed_sizes.last().copied() != Some(outcome.current_bytes) {
                observed_sizes.push(outcome.current_bytes);
            }
        }
        assert!(
            observed_sizes.len() >= 4,
            "{target:?}: expected list, set growth, and dense sizes: {observed_sizes:?}"
        );

        for (mode, count) in [("list", 1), ("set", 16), ("dense", 4_096)] {
            let input = payload(target, lg_k, count, 0x1234_0000 + count as u64);
            let label = format!("{target:?}-{mode}");
            assert_from_payload_bound(&label, &input);
            assert_merge_bound(
                &label,
                populated_handle(target, lg_k, count / 2, 0x9876_0000),
                &input,
            );
        }
    }

    let lower_precision = payload(HllType::Hll6, 8, 4_096, 0x2233_4455);
    assert_merge_bound(
        "dense-downsample",
        populated_handle(HllType::Hll8, lg_k, 4_096, 0x6677_8899),
        &lower_precision,
    );
    let aux = hll4_aux_payload();
    assert_from_payload_bound("HLL4-aux", &aux);
    assert_merge_bound(
        "HLL4-aux",
        populated_handle(HllType::Hll4, 12, 4_096, 0x55aa_aa55),
        &aux,
    );

    // lg_k below 8 skips SET and promotes LIST directly to the union's HLL8 array.
    for direct_lg_k in [4, 5, 7] {
        let mut handle = HllHandle::new_unreserved(direct_lg_k, HllTargetType::Hll8)
            .expect("create direct-promotion handle");
        for index in 0..64 {
            reserved_update(
                &mut handle,
                mixed_value(index, 0x4400_0000 + u64::from(direct_lg_k)),
            );
        }
    }
}

#[test]
fn update_preflight_is_live_configuration_sensitive() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let mut small = HllHandle::new_unreserved(10, HllTargetType::Hll8).expect("small handle");
    let small_initial = small.update_hash_allocation_preflight().bounds();
    assert_eq!(
        small_initial.current_bytes,
        small_initial.operation_peak_bytes
    );
    for index in 0..7 {
        small
            .update_hash_unreserved(mixed_value(index, 1))
            .expect("fill list");
    }
    let list_transition = small.update_hash_allocation_preflight().bounds();
    assert!(list_transition.additional_headroom_bytes() < 1_024);

    let huge = HllHandle::new_unreserved(21, HllTargetType::Hll8).expect("huge handle");
    let huge_initial = huge.update_hash_allocation_preflight().bounds();
    assert_eq!(
        huge_initial.current_bytes,
        huge_initial.operation_peak_bytes
    );
    assert_eq!(huge_initial.current_bytes, small_initial.current_bytes);

    let dense = populated_handle(HllType::Hll8, 10, 4_096, 2);
    let dense_update = dense.update_hash_allocation_preflight().bounds();
    assert_eq!(
        dense_update.current_bytes,
        dense_update.operation_peak_bytes
    );
    assert!(dense_update.current_bytes > small_initial.current_bytes);
}

#[test]
fn stale_and_mismatched_tokens_fail_before_datasketches_mutation() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let mut handle = HllHandle::new_unreserved(10, HllTargetType::Hll8).expect("handle");
    let stale = handle.update_hash_allocation_preflight();
    handle
        .update_hash_unreserved(1_u64)
        .expect("advance handle");
    let estimate = handle.estimate().expect("estimate");
    let (_, stale_error_baseline) = measure_allocations(|| {
        with_reservation(|_| {
            drop(String::from(
                "ds_hll: update allocation preflight is stale or mismatched",
            ))
        })
    });
    let (stale_result, stale_measurement) = measure_allocations(|| {
        with_reservation(|guard| handle.update_hash_under_reservation(2_u64, &stale, guard))
    });
    assert!(stale_result.is_err());
    assert_eq!(
        stale_measurement.allocated_bytes, stale_error_baseline.allocated_bytes,
        "stale rejection must allocate only its error string"
    );
    assert_eq!(handle.estimate().expect("estimate after stale"), estimate);

    let list_payload = payload(HllType::Hll8, 10, 1, 3);
    let dense_payload = payload(HllType::Hll8, 10, 4_096, 4);
    let mismatched = handle
        .merge_payload_allocation_preflight(&list_payload)
        .expect("list preflight");
    let estimate = handle.estimate().expect("estimate before mismatch");
    let (_, mismatch_error_baseline) = measure_allocations(|| {
        with_reservation(|_| {
            drop(String::from(
                "ds_hll: merge allocation preflight is stale or mismatched",
            ))
        })
    });
    let (mismatch_result, mismatch_measurement) = measure_allocations(|| {
        with_reservation(|guard| {
            handle.merge_payload_under_reservation(&dense_payload, &mismatched, guard)
        })
    });
    assert!(mismatch_result.is_err());
    assert_eq!(
        mismatch_measurement.allocated_bytes, mismatch_error_baseline.allocated_bytes,
        "mismatched payload rejection must allocate only its error string"
    );
    assert_eq!(
        handle.estimate().expect("estimate after mismatch"),
        estimate
    );

    // Reservation denial is represented by the caller not invoking the operation at all.
    let denied = handle.update_hash_allocation_preflight();
    let denied_estimate = handle.estimate().expect("estimate before denial");
    assert!(denied.bounds().additional_headroom_bytes() <= 1 << 10);
    let (_, denied_measurement) = measure_allocations(|| {});
    assert_eq!(denied_measurement.allocated_bytes, 0);
    assert_eq!(
        handle.estimate().expect("estimate after denial"),
        denied_estimate
    );
}

#[test]
fn malformed_allocation_headers_fail_before_standard_decode() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let valid = payload(HllType::Hll8, 10, 4_096, 5);
    for mutation in 0..6 {
        let mut malformed = valid.clone();
        match mutation {
            0 => malformed.truncate(7),
            1 => malformed[1] = 99,
            2 => malformed[2] = 99,
            3 => malformed[3] = 3,
            4 => malformed[7] = 15,
            5 => malformed.truncate(40),
            _ => unreachable!(),
        }
        assert!(
            HllHandle::from_payload_allocation_preflight(&malformed).is_err(),
            "mutation {mutation} must fail allocation admission"
        );
    }

    let mut overflowing_hll4 = payload(HllType::Hll4, 10, 4_096, 6);
    overflowing_hll4[4] = 63;
    overflowing_hll4[5] &= !8;
    overflowing_hll4[36..40].copy_from_slice(&1_u32.to_le_bytes());
    assert!(HllHandle::from_payload_allocation_preflight(&overflowing_hll4).is_err());
}
