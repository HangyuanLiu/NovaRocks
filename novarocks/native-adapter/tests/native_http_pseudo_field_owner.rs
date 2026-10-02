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

//! Original-funded public HTTP Method/Scheme storage and physical alias exit.
//! Only synchronous Rust-requested arena/Core/bitmap/carrier/wrapper backing is
//! measured. Runtime, connection, unrelated URI components and allocator caches
//! are outside this oracle; the original Worker grant remains the actual wallet.

use bytes::Bytes;
use hyper::http::header::HeaderFieldAllocationPool;
use hyper::http::{Method, Request, Uri, uri};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::hash::{Hash, Hasher};
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
fn hash(value: &impl Hash) -> u64 {
    let mut state = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut state);
    state.finish()
}

#[test]
fn all_field_positions_fit_the_declared_original_bound_and_physically_exit_before_credit() {
    let f = Funded::new(512, 8, 64);
    let (payloads, calls, requested) = measure(1, || {
        std::array::from_fn::<_, 8, _>(|index| {
            f.fields
                .try_fill(64, |out| {
                    out.fill(b'a' + index as u8);
                    Ok::<_, ()>(())
                })
                .unwrap()
        })
    });
    assert_eq!(calls, 8, "one actual Bytes owner wrapper per live position");
    assert!(f.constructor_requested + requested <= f.total);
    assert_eq!(f.fields.available_positions(), 0);
    for (index, payload) in payloads.iter().enumerate() {
        assert_eq!(payload.as_ref(), &[b'a' + index as u8; 64]);
    }
    let aliases = no_allocation(|| payloads.clone());
    let budget = f.budget.clone();
    let total = f.total;
    drop(payloads);
    drop(f);
    held(&budget, total);
    drop(aliases);
    // PhysicalExit checks System deallocation of every tracked family allocation
    // before its original ResultWriteCredit is dropped.
    drop(grant(&budget, total));
}

#[test]
fn shared_pseudo_fields_and_request_clones_allocate_nothing_and_hold_the_actual_wallet() {
    let f = Funded::new(256, 4, 128);
    let method_bytes = f.input(b"X-ORIGINAL-LONG-METHOD");
    let scheme_bytes = f.input(b"x+original.9");
    let method_pointer = method_bytes.as_ptr();
    let scheme_pointer = scheme_bytes.as_ptr();
    // All cloned URI components carry owner-backed arena Bytes, avoiding the
    // first-clone promotion required by an ordinary Vec-backed parsed fixture.
    let authority = uri::Authority::from_maybe_shared(f.input(b"example.com")).unwrap();
    let path = uri::PathAndQuery::from_maybe_shared(f.input(b"/owned")).unwrap();
    let (method_alias, scheme_alias, request, request_alias, uri_alias) = no_allocation(|| {
        let method = Method::from_owned_bytes(method_bytes).unwrap();
        let scheme = uri::Scheme::from_owned_bytes(scheme_bytes).unwrap();
        let method_alias = method.clone();
        let scheme_alias = scheme.clone();
        let mut parts = uri::Parts::default();
        parts.scheme = Some(scheme);
        parts.authority = Some(authority);
        parts.path_and_query = Some(path);
        let uri = Uri::from_parts(parts).unwrap();
        let uri_alias = uri.clone();
        let request = Request::builder().method(method).uri(uri).body(()).unwrap();
        let request_alias = request.clone();
        (
            method_alias,
            scheme_alias,
            request,
            request_alias,
            uri_alias,
        )
    });
    assert_eq!(method_alias.as_str().as_ptr(), method_pointer);
    assert_eq!(scheme_alias.as_str().as_ptr(), scheme_pointer);
    assert_eq!(request.method().as_str().as_ptr(), method_pointer);
    assert_eq!(request_alias.method().as_str().as_ptr(), method_pointer);
    assert_eq!(
        request_alias.uri().scheme().unwrap().as_str().as_ptr(),
        scheme_pointer
    );
    assert_eq!(
        uri_alias.scheme().unwrap().as_str().as_ptr(),
        scheme_pointer
    );
    assert_eq!(f.fields.available_positions(), 0);
    let config_alias = no_allocation(|| f.fields.clone());
    let budget = f.budget.clone();
    let total = f.total;
    drop((request, request_alias, uri_alias));
    assert_eq!(f.fields.available_positions(), 2);
    drop(f);
    drop(config_alias);
    held(&budget, total);
    assert_eq!(method_alias.as_str(), "X-ORIGINAL-LONG-METHOD");
    assert_eq!(scheme_alias.as_str(), "x+original.9");
    drop(method_alias);
    held(&budget, total);
    drop(scheme_alias);
    drop(grant(&budget, total));
}

#[test]
fn default_eager_clone_is_preserved_while_shared_identity_and_hash_allocate_nothing() {
    let f = Funded::new(256, 4, 128);
    let (ordinary_method, calls, requested) =
        measure(0, || Method::from_bytes(b"X-ORIGINAL-LONG-METHOD").unwrap());
    assert_eq!(calls, 1);
    assert_eq!(requested, b"X-ORIGINAL-LONG-METHOD".len());
    let (method_copy, calls, requested) = measure(0, || ordinary_method.clone());
    assert_eq!(calls, 1, "ordinary Method Box clone remains eager");
    assert_eq!(requested, b"X-ORIGINAL-LONG-METHOD".len());
    assert_ne!(
        ordinary_method.as_str().as_ptr(),
        method_copy.as_str().as_ptr()
    );
    let ordinary_scheme: uri::Scheme = "X+ORIGINAL.9".parse().unwrap();
    let (scheme_copy, calls, requested) = measure(0, || ordinary_scheme.clone());
    assert_eq!(
        calls, 2,
        "ordinary Scheme first clone boxes metadata and promotes Bytes"
    );
    assert!(requested > 0);
    let (next_scheme_copy, calls, requested) = measure(0, || ordinary_scheme.clone());
    assert_eq!(
        calls, 1,
        "after promotion only the ByteStr Box is allocated"
    );
    assert!(requested > 0);
    // The ordinary Scheme payload already shares immutable Bytes, while its Box
    // metadata remains an independent allocation, unlike the new shared variant.
    assert_eq!(
        ordinary_scheme.as_str().as_ptr(),
        scheme_copy.as_str().as_ptr()
    );
    assert_eq!(
        ordinary_scheme.as_str().as_ptr(),
        next_scheme_copy.as_str().as_ptr()
    );
    let method_bytes = f.input(b"X-ORIGINAL-LONG-METHOD");
    let scheme_bytes = f.input(b"x+original.9");
    let (method, scheme, method_copy_shared, scheme_copy_shared) = no_allocation(|| {
        let method = Method::from_owned_bytes(method_bytes).unwrap();
        let scheme = uri::Scheme::from_owned_bytes(scheme_bytes).unwrap();
        assert_eq!(method, ordinary_method);
        assert_eq!(hash(&method), hash(&ordinary_method));
        assert_eq!(scheme, ordinary_scheme);
        assert_eq!(hash(&scheme), hash(&ordinary_scheme));
        let method_copy = method.clone();
        let scheme_copy = scheme.clone();
        (method, scheme, method_copy, scheme_copy)
    });
    assert_eq!(
        method.as_str().as_ptr(),
        method_copy_shared.as_str().as_ptr()
    );
    assert_eq!(
        scheme.as_str().as_ptr(),
        scheme_copy_shared.as_str().as_ptr()
    );
    drop((method, scheme, method_copy_shared, scheme_copy_shared));
    f.finish();
}

#[test]
fn owned_validation_standard_and_inline_paths_have_zero_extra_allocations_and_return_positions() {
    let f = Funded::new(256, 4, 128);
    for text in [
        b"GET".as_slice(),
        b"get".as_slice(),
        b"123456789012345".as_slice(),
    ] {
        let input = f.input(text);
        let method = no_allocation(|| Method::from_owned_bytes(input).unwrap());
        assert_eq!(method.as_str().as_bytes(), text);
        assert_eq!(f.fields.available_positions(), 4);
    }
    for text in [b"http".as_slice(), b"https".as_slice()] {
        let input = f.input(text);
        let scheme = no_allocation(|| uri::Scheme::from_owned_bytes(input).unwrap());
        assert_eq!(scheme.as_str().as_bytes(), text);
        assert_eq!(f.fields.available_positions(), 4);
    }
    for text in [
        b"".as_slice(),
        b"X-ORIGINAL-LONG METHOD".as_slice(),
        b"X-ORIGINAL-LONG\xffMETHOD".as_slice(),
    ] {
        let input = f.input(text);
        assert!(no_allocation(|| Method::from_owned_bytes(input)).is_err());
        assert_eq!(f.fields.available_positions(), 4);
    }
    let oversized_scheme = [b'a'; 65];
    for text in [
        b"x:scheme".as_slice(),
        b"x\xffscheme".as_slice(),
        &oversized_scheme,
    ] {
        let input = f.input(text);
        assert!(no_allocation(|| uri::Scheme::from_owned_bytes(input)).is_err());
        assert_eq!(f.fields.available_positions(), 4);
    }
    for text in [
        b"".as_slice(),
        b"9numeric".as_slice(),
        b"~custom".as_slice(),
    ] {
        let input = f.input(text);
        let scheme = no_allocation(|| uri::Scheme::from_owned_bytes(input).unwrap());
        assert_eq!(scheme.as_str().as_bytes(), text);
        assert_eq!(
            f.fields.available_positions(),
            4 - usize::from(!text.is_empty())
        );
        drop(scheme);
        assert_eq!(f.fields.available_positions(), 4);
    }
    f.finish();
}
