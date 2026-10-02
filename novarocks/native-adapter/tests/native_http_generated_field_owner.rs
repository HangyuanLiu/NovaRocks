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

//! Generated decimal header fields in original-funded arena storage.
//! The allocator oracle covers actual synchronous arena/bitmap/Core/carrier and
//! Bytes wrapper allocations, not a complete connection or allocator cache.

use bytes::Bytes;
use hyper::http::HeaderValue;
use hyper::http::header::{HeaderFieldAllocationPool, HeaderFieldFillError};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Clone, Copy)]
struct Allocation {
    pointer: usize,
    family: u8,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    family: 0,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static FAMILY: Cell<u8> = const { Cell::new(0) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
    static RECORDS: RefCell<[Allocation; 128]> = const { RefCell::new([EMPTY; 128]) };
}
fn allocated(pointer: *mut u8, size: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        CALLS.with(|v| v.set(v.get() + 1));
        REQUESTED.with(|v| v.set(v.get() + size));
        let family = FAMILY.with(Cell::get);
        if family != 0 {
            RECORDS.with(|records| {
                let mut records = records.borrow_mut();
                let slot = records.iter_mut().find(|r| r.pointer == 0).unwrap();
                *slot = Allocation {
                    pointer: pointer as usize,
                    family,
                };
            });
        }
    }
}
fn freed(pointer: *mut u8) {
    let _ = RECORDS.try_with(|records| {
        if let Some(record) = records
            .borrow_mut()
            .iter_mut()
            .find(|r| r.pointer == pointer as usize)
        {
            *record = EMPTY;
        }
    });
}
struct Probe;
// SAFETY: Allocation operations delegate unchanged to System. Fixed TLS records
// neither allocate nor dereference the recorded pointers.
unsafe impl GlobalAlloc for Probe {
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
        freed(pointer);
        allocated(next, size);
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        freed(pointer);
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
        FAMILY.with(|v| v.set(0));
    }
}
fn measure<T>(family: u8, operation: impl FnOnce() -> T) -> (T, usize, usize) {
    assert!(!TRACK.with(|v| v.replace(true)));
    FAMILY.with(|v| v.set(family));
    CALLS.with(|v| v.set(0));
    REQUESTED.with(|v| v.set(0));
    let tracking = Tracking;
    let result = operation();
    drop(tracking);
    (result, CALLS.with(Cell::get), REQUESTED.with(Cell::get))
}
fn no_allocation<T>(operation: impl FnOnce() -> T) -> T {
    let (result, calls, bytes) = measure(0, operation);
    assert_eq!((calls, bytes), (0, 0));
    result
}
struct PhysicalExit {
    family: u8,
    _credit: ResultWriteCredit,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        if self.family != 0 {
            assert!(
                RECORDS.with(|records| records
                    .borrow()
                    .iter()
                    .all(|r| r.pointer == 0 || r.family != self.family)),
                "original fixed pool/Core/carrier backing must be physically freed before credit"
            );
        }
    }
}
fn grant(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("complete original family pregrant");
    };
    credit
}
fn owner(budget: &Arc<ResultRetainedBudget>, bytes: usize, family: u8) -> Bytes {
    let credit = grant(budget, bytes);
    // The carrier has its own complete pregrant, in addition to the fixed pool.
    let (owner, _, requested) = measure(family, || {
        Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            PhysicalExit {
                family,
                _credit: credit,
            },
        )
    });
    assert!(requested <= carrier());
    owner
}
fn carrier() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, PhysicalExit>()
}
struct Funded {
    fields: HeaderFieldAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    constructor_requested: usize,
}
impl Funded {
    fn new(capacity: usize, positions: usize, max: usize) -> Self {
        let bound =
            HeaderFieldAllocationPool::allocation_capacity_bound(capacity, positions, max).unwrap();
        let total = bound + carrier();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let original = owner(&budget, total, 1);
        let (fields, calls, requested) = measure(1, || {
            HeaderFieldAllocationPool::new(capacity, positions, max, original).unwrap()
        });
        assert_eq!(
            calls, 3,
            "exact arena, bitmap, Core Arc constructor allocations"
        );
        assert!(requested <= bound);
        Self {
            fields,
            budget,
            total,
            constructor_requested: requested + carrier(),
        }
    }
    fn input(&self, input: &[u8]) -> Bytes {
        let (bytes, calls, _) = measure(1, || {
            self.fields
                .try_fill(input.len(), |out| {
                    out.copy_from_slice(input);
                    Ok::<_, ()>(())
                })
                .unwrap()
        });
        assert_eq!(calls, usize::from(!input.is_empty()));
        bytes
    }
    fn finish(self) {
        let budget = self.budget.clone();
        let total = self.total;
        drop(self);
        drop(grant(&budget, total));
    }
}
fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
#[test]
fn generated_decimal_goldens_fill_all_positions_with_only_prepaid_owner_wrappers() {
    let f = Funded::new(384, 6, 20);
    let numbers = [0, 9, 10, 99, 100, u64::MAX];
    let expected: [&[u8]; 6] = [b"0", b"9", b"10", b"99", b"100", b"18446744073709551615"];
    let (values, calls, requested) = measure(1, || {
        std::array::from_fn::<_, 6, _>(|index| {
            HeaderValue::try_from_u64_with_pool(numbers[index], &f.fields).unwrap()
        })
    });
    assert_eq!(
        calls, 6,
        "one prepaid Bytes owner wrapper; no decimal payload allocation"
    );
    assert!(f.constructor_requested + requested <= f.total);
    assert_eq!(f.fields.available_positions(), 0);
    let base = values[0].as_bytes().as_ptr();
    for (index, value) in values.iter().enumerate() {
        assert_eq!(value.as_bytes(), expected[index]);
        assert_eq!(value.as_bytes().as_ptr(), base.wrapping_add(index * 64));
        assert!(!value.is_sensitive());
    }
    let aliases = no_allocation(|| values.clone());
    let budget = f.budget.clone();
    let total = f.total;
    drop(values);
    drop(f);
    held(&budget, total);
    drop(aliases);
    // PhysicalExit checks that System freed arena, bitmap, Core, every field
    // wrapper and the original carrier before returning this actual grant.
    drop(grant(&budget, total));
}

#[test]
fn exact_digit_limits_refuse_before_allocation_and_preserve_existing_positions() {
    for (max, accepted, expected, refused) in [
        (1, 9, b"9".as_slice(), 10),
        (2, 10, b"10".as_slice(), 100),
        (
            19,
            9_999_999_999_999_999_999,
            b"9999999999999999999".as_slice(),
            u64::MAX,
        ),
    ] {
        let f = Funded::new(64, 1, max);
        let (value, calls, _) = measure(1, || {
            HeaderValue::try_from_u64_with_pool(accepted, &f.fields)
        });
        // Keep panic-report allocations outside the original family probe.
        // A source regression must fail the oracle without a cleanup abort.
        let value = value.unwrap();
        assert_eq!(calls, 1);
        assert_eq!(value.as_bytes(), expected);
        assert_eq!(f.fields.available_positions(), 0);
        let failure = no_allocation(|| HeaderValue::try_from_u64_with_pool(refused, &f.fields));
        assert!(
            matches!(failure, Err(HeaderFieldFillError::TooLarge)),
            "length refusal precedes a currently full position window"
        );
        assert_eq!(f.fields.available_positions(), 0);
        drop(value);
        assert_eq!(f.fields.available_positions(), 1);
        f.finish();
    }
    let f = Funded::new(64, 1, 20);
    let (value, calls, _) = measure(1, || {
        HeaderValue::try_from_u64_with_pool(u64::MAX, &f.fields).unwrap()
    });
    assert_eq!(calls, 1);
    assert_eq!(value.as_bytes(), b"18446744073709551615");
    drop(value);
    f.finish();
}

#[test]
fn last_independent_header_value_alias_holds_position_and_original_worker_credit() {
    let f = Funded::new(128, 1, 20);
    let (value, calls, _) = measure(1, || {
        HeaderValue::try_from_u64_with_pool(10, &f.fields).unwrap()
    });
    assert_eq!(calls, 1);
    let mut alias = no_allocation(|| value.clone());
    assert_eq!(alias.as_bytes().as_ptr(), value.as_bytes().as_ptr());
    no_allocation(|| alias.set_sensitive(true));
    assert!(alias.is_sensitive());
    assert!(!value.is_sensitive());
    let failure = no_allocation(|| HeaderValue::try_from_u64_with_pool(0, &f.fields));
    assert!(matches!(failure, Err(HeaderFieldFillError::Exhausted)));
    drop(value);
    assert_eq!(f.fields.available_positions(), 0);
    let config = no_allocation(|| f.fields.clone());
    let budget = f.budget.clone();
    let total = f.total;
    drop(f);
    drop(config);
    held(&budget, total);
    assert_eq!(alias.as_bytes(), b"10");
    drop(alias);
    drop(grant(&budget, total));
}

#[test]
fn free_positions_do_not_allow_heap_fallback_when_no_contiguous_extent_is_available() {
    let f = Funded::new(256, 4, 128);
    let first = f.input(&[b'a'; 128]);
    let second = f.input(&[b'b'; 128]);
    let first_pointer = first.as_ptr();
    assert_eq!(f.fields.available_positions(), 2);
    let failure = no_allocation(|| HeaderValue::try_from_u64_with_pool(u64::MAX, &f.fields));
    assert!(matches!(failure, Err(HeaderFieldFillError::Exhausted)));
    assert_eq!(f.fields.available_positions(), 2);
    assert_eq!(first.as_ref(), &[b'a'; 128]);
    assert_eq!(second.as_ref(), &[b'b'; 128]);
    drop(first);
    let (value, calls, _) = measure(1, || {
        HeaderValue::try_from_u64_with_pool(u64::MAX, &f.fields).unwrap()
    });
    assert_eq!(calls, 1);
    assert_eq!(value.as_bytes(), b"18446744073709551615");
    assert_eq!(value.as_bytes().as_ptr(), first_pointer);
    assert_eq!(second.as_ref(), &[b'b'; 128]);
    drop((value, second));
    assert_eq!(f.fields.available_positions(), 4);
    f.finish();
}

#[test]
fn ordinary_u64_conversion_keeps_legacy_payload_allocation_and_bytes_clone_promotion() {
    let f = Funded::new(64, 1, 20);
    let (ordinary, calls, requested) = measure(0, || HeaderValue::from(u64::MAX));
    assert_eq!(
        calls, 1,
        "ordinary full-width value retains the original 20-byte allocation"
    );
    assert_eq!(requested, 20);
    let (ordinary_alias, calls, requested) = measure(0, || ordinary.clone());
    assert_eq!(
        calls, 1,
        "ordinary full Vec promotes metadata on its first clone"
    );
    assert!(requested > 0);
    let next_alias = no_allocation(|| ordinary.clone());
    assert_eq!(
        ordinary.as_bytes().as_ptr(),
        ordinary_alias.as_bytes().as_ptr()
    );
    assert_eq!(ordinary.as_bytes().as_ptr(), next_alias.as_bytes().as_ptr());
    let (funded, calls, _) = measure(1, || {
        HeaderValue::try_from_u64_with_pool(u64::MAX, &f.fields).unwrap()
    });
    assert_eq!(
        calls, 1,
        "funded path only allocates the prepaid owner wrapper"
    );
    assert_eq!(funded.as_bytes(), b"18446744073709551615");
    assert_eq!(funded, ordinary);
    assert_ne!(funded.as_bytes().as_ptr(), ordinary.as_bytes().as_ptr());
    let funded_alias = no_allocation(|| funded.clone());
    assert_eq!(funded_alias.as_bytes().as_ptr(), funded.as_bytes().as_ptr());
    drop((funded, funded_alias));
    f.finish();
}
