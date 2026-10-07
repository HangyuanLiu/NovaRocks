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

//! Test-only runtime binding and the admission witness for governed pairs.
//!
//! The witness is this test binary's global allocator. Outside an explicit
//! per-thread scope it forwards to `System` unchanged. Inside a scope it
//! compares every byte the thread asks the global allocator for against the
//! bytes the scope's task tracker has admitted: the task allocator charges
//! its tracker before it calls the global allocator, so an admitted block is
//! always covered, and any other block is counted as non-admitted.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{Arc, OnceLock};

use crate::runtime::execution_runtime::{ExecutionRuntime, test_execution_runtime};
use crate::runtime::local_offset::LocalOffsetOwner;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::verification::TaskVerificationHolder;
use novarocks_execution_contract::TaskIdentity;

/// One process runtime shared by the pair tests, with the production
/// chrono-backed local offset owner.
pub(super) fn shared_runtime() -> Arc<ExecutionRuntime> {
    static RUNTIME: OnceLock<Arc<ExecutionRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(test_execution_runtime))
}

/// The shared runtime with its local offset owner replaced by `owner`.
pub(super) fn runtime_with_offset_owner(owner: Arc<LocalOffsetOwner>) -> Arc<ExecutionRuntime> {
    Arc::new(ExecutionRuntime::clone(&shared_runtime()).with_local_offset_owner(owner))
}

pub(super) fn task_state(
    identity: TaskIdentity,
    tracker: Arc<MemTracker>,
    runtime: Option<Arc<ExecutionRuntime>>,
) -> RuntimeState {
    RuntimeState::new(None, None, None, None, None, Some(tracker), runtime)
        .with_verification(Arc::new(TaskVerificationHolder::new(identity)))
}

/// Variant metadata with no keys.
pub(crate) const EMPTY_METADATA: [u8; 3] = [0x01, 0x00, 0x00];
/// Variant metadata with the single key "t".
pub(crate) const KEY_T_METADATA: [u8; 5] = [0x01, 0x01, 0x00, 0x01, b't'];

/// A serialized Variant whose every byte is ASCII, so it travels in the
/// original UTF8 carrier.
pub(crate) fn ascii_variant(metadata: &[u8], value: &[u8]) -> String {
    let size = u32::try_from(metadata.len() + value.len()).unwrap();
    let mut raw = size.to_le_bytes().to_vec();
    raw.extend_from_slice(metadata);
    raw.extend_from_slice(value);
    assert!(raw.iter().all(u8::is_ascii), "raw={raw:?}");
    String::from_utf8(raw).expect("ASCII is UTF8")
}

/// A Variant array value with one-byte offsets over `elements`.
pub(crate) fn variant_array(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut value = vec![0x03, u8::try_from(elements.len()).unwrap(), 0];
    let mut offset = 0usize;
    for element in elements {
        offset += element.len();
        value.push(u8::try_from(offset).expect("one-byte array offset"));
    }
    for element in elements {
        value.extend_from_slice(element);
    }
    value
}

/// One primitive Variant value: its header byte and payload.
pub(crate) fn variant_primitive(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut value = vec![kind << 2];
    value.extend_from_slice(payload);
    value
}

/// A serialized Variant `[{"t": ts0}, ts1, ...]` of TimestampTz values whose
/// every byte is ASCII.
///
/// Each micros value must be non-negative with every little-endian byte below
/// 0x80; the element count stays small enough for one-byte offsets.
pub(crate) fn ascii_timestamp_variant(micros: &[i64]) -> String {
    const TIMESTAMP_TZ: u8 = 12;
    assert!(!micros.is_empty() && micros.len() < 8);
    // Object {"t": ts0}: header, count, field id, two offsets, value.
    let mut object = vec![0x02, 0x01, 0x00, 0x00, 0x09];
    object.extend(variant_primitive(TIMESTAMP_TZ, &micros[0].to_le_bytes()));
    let mut elements = vec![object];
    elements.extend(
        micros[1..]
            .iter()
            .map(|value| variant_primitive(TIMESTAMP_TZ, &value.to_le_bytes())),
    );
    ascii_variant(&KEY_T_METADATA, &variant_array(&elements))
}

/// What the global allocator saw on this thread inside one witness scope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct AdmissionReport {
    pub admitted_bytes: u64,
    pub non_admitted_allocations: u64,
    pub non_admitted_bytes: u64,
}

#[derive(Clone, Copy)]
struct Scope {
    tracker: *const MemTracker,
    admitted_base: i64,
    report: AdmissionReport,
}

thread_local! {
    static SCOPE: Cell<Option<Scope>> = const { Cell::new(None) };
}

/// Runs `body` with every global allocation on this thread checked against
/// the bytes `tracker` admitted during the same scope.
pub(super) fn with_admission_witness<R>(
    tracker: &Arc<MemTracker>,
    body: impl FnOnce() -> R,
) -> (R, AdmissionReport) {
    let scope = Scope {
        tracker: Arc::as_ptr(tracker),
        admitted_base: tracker.allocated(),
        report: AdmissionReport::default(),
    };
    assert!(
        SCOPE.with(|cell| cell.replace(Some(scope))).is_none(),
        "admission witness scopes do not nest"
    );
    let result = body();
    let scope = SCOPE
        .with(Cell::take)
        .expect("the admission witness scope is still installed");
    (result, scope.report)
}

fn observe(size: usize) {
    let _ = SCOPE.try_with(|cell| {
        let Some(mut scope) = cell.get() else {
            return;
        };
        // SAFETY: the scope owner holds an `Arc` to the tracker for the whole
        // scope, and reading its counter does not allocate.
        let admitted = unsafe { (*scope.tracker).allocated() } - scope.admitted_base;
        let observed = scope.report.admitted_bytes + size as u64;
        if u64::try_from(admitted).is_ok_and(|admitted| observed <= admitted) {
            scope.report.admitted_bytes = observed;
        } else {
            scope.report.non_admitted_allocations += 1;
            scope.report.non_admitted_bytes += size as u64;
        }
        cell.set(Some(scope));
    });
}

struct AdmissionWitness;

#[global_allocator]
static WITNESS: AdmissionWitness = AdmissionWitness;

// SAFETY: every method forwards to `System` with the caller's arguments; the
// witness only reads thread-local state and an atomic tracker counter.
unsafe impl GlobalAlloc for AdmissionWitness {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        observe(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        observe(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        observe(new_size);
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_in_pair_admission_witness_separates_admitted_and_other_allocations() {
        let tracker = MemTracker::new_root("witness-task");
        let allocator = crate::exec::expr::agg::AggregateAllocator::new(Arc::clone(&tracker));
        let ((), report) = with_admission_witness(&tracker, || {
            let mut admitted = crate::exec::expr::agg::AggregateVec::<u64>::new_in(allocator);
            admitted.try_reserve_exact(4).unwrap();
            admitted.try_reserve_exact(64).unwrap();
            drop(admitted);
        });
        assert_eq!(report.non_admitted_allocations, 0);
        assert_eq!(report.admitted_bytes, 4 * 8 + 64 * 8);

        let (text, report) = with_admission_witness(&tracker, || String::from("not admitted"));
        assert_eq!(report.non_admitted_allocations, 1);
        assert_eq!(report.non_admitted_bytes, text.len() as u64);
        assert_eq!(report.admitted_bytes, 0);
    }
}
