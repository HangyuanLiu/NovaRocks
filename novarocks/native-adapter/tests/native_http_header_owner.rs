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

//! Actual HTTP field ownership only. HeaderMap scaffolds, budget/observer
//! fixtures, the reservation callback, and whole Native connection capacity
//! are separate from the fixed payload and Bytes carrier measured here.
//! The System probe records requested Rust layouts, not allocator RSS or TLS.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use bytes::Bytes;
use hyper::http::{HeaderMap, HeaderName, HeaderValue};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
    static CAPTURE_CARRIER: Cell<bool> = const { Cell::new(false) };
    static PAYLOAD_PTR: Cell<usize> = const { Cell::new(0) };
    static CARRIER_PTR: Cell<usize> = const { Cell::new(0) };
    static PAYLOAD_FREED: Cell<usize> = const { Cell::new(0) };
    static CARRIER_FREED: Cell<usize> = const { Cell::new(0) };
    static EXITS: Cell<usize> = const { Cell::new(0) };
    static PAYLOAD_REALLOCATED: Cell<bool> = const { Cell::new(false) };
}

struct AllocationProbe;
fn allocated(pointer: *mut u8, size: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = CALLS.try_with(|n| n.set(n.get() + 1));
        let _ = REQUESTED.try_with(|n| n.set(n.get() + size));
    }
    if CAPTURE_CARRIER.try_with(Cell::get).unwrap_or(false) {
        let _ = CARRIER_PTR.try_with(|p| p.set(pointer as usize));
    }
}
// SAFETY: Every operation delegates unchanged to System. Observation uses
// allocation-free TLS cells and never dereferences the recorded addresses.
unsafe impl GlobalAlloc for AllocationProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, size) };
        if PAYLOAD_PTR.try_with(Cell::get).unwrap_or(0) == pointer as usize {
            let _ = PAYLOAD_REALLOCATED.try_with(|p| p.set(true));
            let _ = PAYLOAD_PTR.try_with(|p| p.set(next as usize));
        }
        allocated(next, size);
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        if PAYLOAD_PTR.try_with(Cell::get).unwrap_or(0) == pointer as usize {
            let _ = PAYLOAD_FREED.try_with(|n| n.set(layout.size()));
            let _ = PAYLOAD_PTR.try_with(|p| p.set(0));
        }
        if CARRIER_PTR.try_with(Cell::get).unwrap_or(0) == pointer as usize {
            let _ = CARRIER_FREED.try_with(|n| n.set(layout.size()));
            let _ = CARRIER_PTR.try_with(|p| p.set(0));
        }
    }
}
#[global_allocator]
static ALLOCATOR: AllocationProbe = AllocationProbe;

struct Tracking;
impl Tracking {
    fn begin() -> Self {
        CALLS.with(|n| n.set(0));
        REQUESTED.with(|n| n.set(0));
        assert!(!TRACK.with(|t| t.replace(true)), "nested allocation probe");
        Self
    }
}
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|t| t.set(false));
        CAPTURE_CARRIER.with(|t| t.set(false));
    }
}
fn measure<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    let tracking = Tracking::begin();
    let value = f();
    drop(tracking);
    (value, CALLS.with(Cell::get), REQUESTED.with(Cell::get))
}
fn no_allocation<T>(f: impl FnOnce() -> T) -> T {
    let (value, calls, requested) = measure(f);
    assert_eq!(calls, 0, "HTTP field operation allocated");
    assert_eq!(requested, 0);
    value
}

struct Payload(Vec<u8>);
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
struct PhysicalExit {
    _credit: ResultWriteCredit,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        // The real budget release runs only after this check and after both
        // allocations have actually passed through System::dealloc.
        assert_ne!(PAYLOAD_FREED.with(Cell::get), 0, "payload still allocated");
        assert_ne!(CARRIER_FREED.with(Cell::get), 0, "carrier still allocated");
        assert!(!PAYLOAD_REALLOCATED.with(Cell::get));
        EXITS.with(|n| n.set(n.get() + 1));
    }
}
struct Funded {
    bytes: Bytes,
    budget: Arc<ResultRetainedBudget>,
    grant: usize,
    capacity: usize,
    carrier: usize,
}
fn funded(input: &[u8]) -> Funded {
    // Even an empty invalid input owns a real one-byte allocation, so its
    // physical exit has the same non-vacuous oracle as nonempty inputs.
    let capacity = input.len().max(1);
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Payload, PhysicalExit>();
    let grant = capacity.checked_add(carrier).unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(grant).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(grant).unwrap() else {
        panic!("original fixed payload/carrier pregrant");
    };
    PAYLOAD_FREED.with(|n| n.set(0));
    CARRIER_FREED.with(|n| n.set(0));
    EXITS.with(|n| n.set(0));
    PAYLOAD_REALLOCATED.with(|n| n.set(false));
    let (bytes, calls, requested) = measure(|| {
        let mut payload = Vec::with_capacity(capacity);
        assert_eq!(payload.capacity(), capacity);
        payload.extend_from_slice(input);
        PAYLOAD_PTR.with(|p| p.set(payload.as_ptr() as usize));
        CAPTURE_CARRIER.with(|t| t.set(true));
        let bytes =
            Bytes::from_owner_with_exit_guard(Payload(payload), PhysicalExit { _credit: credit });
        CAPTURE_CARRIER.with(|t| t.set(false));
        bytes
    });
    assert_eq!(calls, 2, "one fixed Vec and one Bytes owner carrier");
    assert_eq!(
        requested, grant,
        "actual requested layouts must fit pregrant"
    );
    Funded {
        bytes,
        budget,
        grant,
        capacity,
        carrier,
    }
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    assert_eq!(EXITS.with(Cell::get), 0);
}
fn released(budget: &Arc<ResultRetainedBudget>, grant: usize, capacity: usize, carrier: usize) {
    assert_eq!(EXITS.with(Cell::get), 1);
    assert_eq!(PAYLOAD_FREED.with(Cell::get), capacity);
    assert_eq!(CARRIER_FREED.with(Cell::get), carrier);
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(grant).unwrap() else {
        panic!("original physical exit must make the entire grant available");
    };
    drop(credit);
}

#[test]
fn custom_name_and_value_move_exact_backing_and_clone_without_allocation() {
    let Funded {
        bytes,
        budget,
        grant,
        capacity,
        carrier,
    } = funded(b"x-owned-nameowned-value");
    let pointer = bytes.as_ptr();
    let name = no_allocation(|| HeaderName::from_lowercase_bytes(bytes.slice(..12)).unwrap());
    let value = no_allocation(|| HeaderValue::from_maybe_shared(bytes.slice(12..)).unwrap());
    assert_eq!(name.as_str(), "x-owned-name");
    assert_eq!(value.as_bytes(), b"owned-value");
    assert_eq!(name.as_str().as_ptr(), pointer);
    assert_eq!(value.as_bytes().as_ptr(), pointer.wrapping_add(12));
    let alias = no_allocation(|| name.clone());
    let value_alias = no_allocation(|| value.clone());
    drop(bytes);
    drop(name);
    drop(value);
    held(&budget);
    assert_eq!(alias.as_str().as_ptr(), pointer);
    assert_eq!(value_alias.as_bytes().as_ptr(), pointer.wrapping_add(12));
    drop(alias);
    held(&budget);
    drop(value_alias);
    released(&budget, grant, capacity, carrier);
}

#[test]
fn default_lowercase_constructor_copies_and_does_not_retain_original_owner() {
    let Funded {
        bytes,
        budget,
        grant,
        capacity,
        carrier,
    } = funded(b"x-owned-default-copy");
    let pointer = bytes.as_ptr();
    let (name, calls, requested) = measure(|| HeaderName::from_lowercase(&bytes).unwrap());
    assert!(calls > 0, "default custom-name constructor must copy");
    assert!(requested >= bytes.len());
    assert_ne!(name.as_str().as_ptr(), pointer);
    drop(bytes);
    released(&budget, grant, capacity, carrier);
    assert_eq!(name.as_str(), "x-owned-default-copy");
}

#[test]
fn map_clone_remove_and_early_iterator_drop_preserve_only_field_owners() {
    let Funded {
        bytes,
        budget,
        grant,
        capacity,
        carrier,
    } = funded(b"x-owned-nameowned-value");
    let pointer = bytes.as_ptr();
    let name = HeaderName::from_lowercase_bytes(bytes.slice(..12)).unwrap();
    let value = HeaderValue::from_maybe_shared(bytes.slice(12..)).unwrap();
    let duplicate_name = name.clone();
    let duplicate_value = value.clone();
    let mut map = HeaderMap::new();
    map.insert(name, value);
    map.append(duplicate_name, duplicate_value);
    map.insert("content-type", HeaderValue::from_static("text/plain"));
    drop(bytes);
    // These actual HeaderMap allocations are deliberately NOT charged to the
    // field grant. Cloning the fields must still preserve their owner/pointer.
    let (copy, calls, _) = measure(|| map.clone());
    assert!(
        calls > 0,
        "HeaderMap scaffolds remain independent allocations"
    );
    assert_eq!(
        copy.iter()
            .find(|(name, _)| name.as_str() == "x-owned-name")
            .unwrap()
            .0
            .as_str()
            .as_ptr(),
        pointer
    );
    let removed = map.remove("x-owned-name").unwrap();
    drop(map);
    held(&budget);
    let mut iterator = copy.into_iter();
    let (name, value) = iterator.next().unwrap();
    let name = name.unwrap();
    assert_eq!(name.as_str().as_ptr(), pointer);
    assert_eq!(value.as_bytes().as_ptr(), pointer.wrapping_add(12));
    drop(iterator); // Drops the duplicate and unrelated entries early.
    held(&budget);
    drop(removed);
    drop(name);
    held(&budget);
    drop(value);
    released(&budget, grant, capacity, carrier);
}

#[test]
fn every_single_byte_has_identical_lowercase_validation() {
    for byte in 0_u8..=255 {
        let expected = HeaderName::from_lowercase(&[byte]);
        let Funded {
            bytes,
            budget,
            grant,
            capacity,
            carrier,
        } = funded(&[byte]);
        let actual = HeaderName::from_lowercase_bytes(bytes);
        assert_eq!(actual.is_ok(), expected.is_ok(), "byte {byte}");
        if let (Ok(actual), Ok(expected)) = (&actual, &expected) {
            assert_eq!(actual, expected, "byte {byte}");
            held(&budget);
        }
        drop(actual);
        released(&budget, grant, capacity, carrier);
    }
}

#[test]
fn exact_maximum_name_preserves_backing_and_one_more_byte_rejects() {
    // http 1.4.0 header/mod.rs MAX_HEADER_NAME_LEN = (1 << 16) - 1.
    // Cover both sides of the legacy parser's 64-byte scratch boundary and
    // the maximum. Acceptance is checked against the actual legacy API.
    for length in [64, 65, 65534, 65535] {
        let input = vec![b'x'; length];
        let expected = HeaderName::from_lowercase(&input).unwrap();
        let Funded {
            bytes,
            budget,
            grant,
            capacity,
            carrier,
        } = funded(&input);
        let pointer = bytes.as_ptr();
        let name = no_allocation(|| HeaderName::from_lowercase_bytes(bytes).unwrap());
        assert_eq!(name, expected, "length {length}");
        assert_eq!(name.as_str().len(), length);
        assert_eq!(name.as_str().as_ptr(), pointer);
        held(&budget);
        drop(name);
        released(&budget, grant, capacity, carrier);
    }

    let input = vec![b'x'; 65536];
    assert!(HeaderName::from_lowercase(&input).is_err());
    let Funded {
        bytes,
        budget,
        grant,
        capacity,
        carrier,
    } = funded(&input);
    assert!(HeaderName::from_lowercase_bytes(bytes).is_err());
    released(&budget, grant, capacity, carrier);
}

#[test]
fn standard_empty_and_invalid_names_physically_release_input() {
    for input in [
        b"content-length".as_slice(),
        b"",
        b"Content-Length",
        b"x bad",
        b"x\0bad",
    ] {
        let Funded {
            bytes,
            budget,
            grant,
            capacity,
            carrier,
        } = funded(input);
        let pointer = bytes.as_ptr();
        let name = HeaderName::from_lowercase_bytes(bytes);
        if input == b"content-length" {
            let name = name.unwrap();
            assert_eq!(name, hyper::http::header::CONTENT_LENGTH);
            assert_ne!(name.as_str().as_ptr(), pointer);
            released(&budget, grant, capacity, carrier);
            assert_eq!(name.as_str(), "content-length");
        } else {
            assert!(name.is_err());
            released(&budget, grant, capacity, carrier);
        }
    }
}

#[test]
fn unwinding_one_clone_does_not_release_the_surviving_name() {
    let Funded {
        bytes,
        budget,
        grant,
        capacity,
        carrier,
    } = funded(b"x-owned-unwind");
    let name = HeaderName::from_lowercase_bytes(bytes).unwrap();
    let alias = no_allocation(|| name.clone());
    let outcome = catch_unwind(AssertUnwindSafe(move || {
        let _unwinding_alias = alias;
        panic!("intentional field-alias unwind");
    }));
    assert!(outcome.is_err());
    held(&budget);
    assert_eq!(name.as_str(), "x-owned-unwind");
    drop(name);
    released(&budget, grant, capacity, carrier);
}
