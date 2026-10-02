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

//! Actual fixed HeaderMap storage and its original grant only. Field payloads,
//! task/socket/HPACK state and the complete Native connection are separate.
//! The allocator observes requested Rust layouts, not allocator RSS or TLS.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use bytes::Bytes;
use hyper::http::header::{Entry, HeaderMapAllocationPool};
use hyper::http::{HeaderMap, HeaderName, HeaderValue};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;

#[derive(Clone, Copy)]
struct Allocation {
    pointer: usize,
    bytes: usize,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    bytes: 0,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
    static LIVE: Cell<usize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
    static RECORDS: RefCell<[Allocation; 128]> = const { RefCell::new([EMPTY; 128]) };
    static CAPTURE_CARRIER: Cell<bool> = const { Cell::new(false) };
    static CARRIER: Cell<usize> = const { Cell::new(0) };
    static CARRIER_FREED: Cell<bool> = const { Cell::new(false) };
    static EXITS: Cell<usize> = const { Cell::new(0) };
}

fn allocated(pointer: *mut u8, size: usize) {
    if CAPTURE_CARRIER.try_with(Cell::get).unwrap_or(false) {
        CARRIER.with(|p| p.set(pointer as usize));
    }
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        CALLS.with(|n| n.set(n.get() + 1));
        REQUESTED.with(|n| n.set(n.get() + size));
        RECORDS.with(|records| {
            let mut records = records.borrow_mut();
            let record = records.iter_mut().find(|r| r.pointer == 0).unwrap();
            *record = Allocation {
                pointer: pointer as usize,
                bytes: size,
            };
        });
        LIVE.with(|n| {
            n.set(n.get() + size);
            PEAK.with(|p| p.set(p.get().max(n.get())));
        });
    }
}
fn freed(pointer: *mut u8) {
    let _ = RECORDS.try_with(|records| {
        let mut records = records.borrow_mut();
        if let Some(record) = records.iter_mut().find(|r| r.pointer == pointer as usize) {
            LIVE.with(|n| n.set(n.get() - record.bytes));
            *record = EMPTY;
        }
    });
    if CARRIER.try_with(Cell::get).unwrap_or(0) == pointer as usize {
        let _ = CARRIER_FREED.try_with(|n| n.set(true));
        let _ = CARRIER.try_with(|n| n.set(0));
    }
}
struct Probe;
// SAFETY: Operations delegate unchanged to System; fixed TLS bookkeeping
// never dereferences pointers and does not allocate.
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
        TRACK.with(|n| n.set(false));
    }
}
fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize, usize) {
    assert!(!TRACK.with(|n| n.replace(true)));
    CALLS.with(|n| n.set(0));
    REQUESTED.with(|n| n.set(0));
    let tracking = Tracking;
    let value = operation();
    drop(tracking);
    (value, CALLS.with(Cell::get), REQUESTED.with(Cell::get))
}
fn no_allocation<T>(operation: impl FnOnce() -> T) -> T {
    let (value, calls, requested) = measure(operation);
    assert_eq!((calls, requested), (0, 0));
    value
}
struct PoolOwner;
impl AsRef<[u8]> for PoolOwner {
    fn as_ref(&self) -> &[u8] {
        b"original-map-grant"
    }
}
struct PhysicalExit {
    _credit: ResultWriteCredit,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        assert_eq!(
            LIVE.with(Cell::get),
            0,
            "map/Core storage remains allocated"
        );
        assert!(
            CARRIER_FREED.with(Cell::get),
            "original carrier remains allocated"
        );
        EXITS.with(|n| n.set(n.get() + 1));
    }
}
struct Funded {
    pool: HeaderMapAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    bound: usize,
    grant: usize,
    total: usize,
}
impl Funded {
    fn new(maps: usize, keys: usize, extra: usize) -> Self {
        Self::with_extra_grant(maps, keys, extra, 0)
    }
    fn with_extra_grant(maps: usize, keys: usize, extra: usize, other: usize) -> Self {
        assert_eq!(LIVE.with(Cell::get), 0);
        PEAK.with(|n| n.set(0));
        EXITS.with(|n| n.set(0));
        CARRIER_FREED.with(|n| n.set(false));
        let bound = HeaderMapAllocationPool::allocation_capacity_bound(maps, keys, extra).unwrap();
        let carrier = Bytes::owner_with_exit_guard_metadata_size::<PoolOwner, PhysicalExit>();
        let grant = bound.checked_add(carrier).unwrap();
        let total = grant.checked_add(other).unwrap();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(grant).unwrap()
        else {
            panic!("complete map family pregrant");
        };
        CAPTURE_CARRIER.with(|n| n.set(true));
        let ownership =
            Bytes::from_owner_with_exit_guard(PoolOwner, PhysicalExit { _credit: credit });
        CAPTURE_CARRIER.with(|n| n.set(false));
        let (pool, calls, bytes) =
            measure(|| HeaderMapAllocationPool::new(maps, keys, extra, ownership).unwrap());
        assert_eq!(calls, 1, "constructor allocates only the pool Core Arc");
        assert!(bytes <= bound);
        Self {
            pool,
            budget,
            bound,
            grant,
            total,
        }
    }
    fn map(&self) -> HeaderMap {
        let (map, calls, _) = measure(|| HeaderMap::try_from_allocation_pool(&self.pool).unwrap());
        assert_eq!(calls, 3, "indices, entries and duplicate storage");
        assert!(PEAK.with(Cell::get) <= self.bound);
        map
    }
    fn finish(self) {
        let Self {
            pool,
            budget,
            total,
            ..
        } = self;
        drop(pool);
        assert_eq!(EXITS.with(Cell::get), 1);
        reserve_and_drop(&budget, total);
    }
}
fn reserve_and_drop(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("physical owner exit must restore the original grant");
    };
    drop(credit);
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn value(text: &'static str) -> HeaderValue {
    HeaderValue::from_static(text)
}

#[test]
fn original_pregrant_covers_every_coexisting_fixed_backing_and_rejects_bad_geometry() {
    for (maps, keys, extra) in [
        (0, 4, 3),
        (1, 0, 3),
        (1, usize::MAX, 3),
        (1, 4, 32769),
        (usize::MAX, 4, 3),
    ] {
        assert!(
            no_allocation(|| HeaderMapAllocationPool::allocation_capacity_bound(maps, keys, extra))
                .is_err()
        );
    }
    let funded = Funded::new(3, 4, 3);
    let first = funded.map();
    let second = funded.map();
    let third = funded.map();
    assert_eq!(funded.pool.available_maps(), 0);
    assert!(no_allocation(|| HeaderMap::try_from_allocation_pool(&funded.pool)).is_err());
    held(&funded.budget);
    drop(second);
    assert_eq!(funded.pool.available_maps(), 1);
    let replacement = funded.map();
    drop((first, third, replacement));
    assert_eq!(funded.pool.available_maps(), 3);
    funded.finish();
}

#[test]
fn full_keys_allow_existing_key_updates_and_exact_duplicate_capacity_without_growth() {
    let funded = Funded::new(1, 4, 3);
    let mut map = funded.map();
    // Prepare all field names outside the scaffold allocation measurement.
    let names = ["x-a", "x-b", "x-c", "x-d", "x-e", "x-f"].map(HeaderName::from_static);
    assert_eq!(map.capacity(), names.len());
    no_allocation(|| {
        for name in &names {
            map.try_insert(name.clone(), value("head")).unwrap();
        }
        assert!(
            map.try_insert(HeaderName::from_static("x-over"), value("reject"))
                .is_err()
        );
        assert_eq!(
            map.try_insert(HeaderName::from_static("x-a"), value("replace"))
                .unwrap(),
            Some(value("head"))
        );
        map.try_append(HeaderName::from_static("x-a"), value("second"))
            .unwrap();
        if let Entry::Occupied(mut entry) = map.try_entry(HeaderName::from_static("x-a")).unwrap() {
            entry.try_append(value("third")).unwrap();
            entry.try_append(value("fourth")).unwrap();
            assert!(entry.try_append(value("fifth")).is_err());
        } else {
            panic!("existing entry");
        }
        assert!(
            map.try_append(HeaderName::from_static("x-b"), value("reject"))
                .is_err()
        );
        assert!(map.try_reserve(usize::MAX).is_err());
    });
    assert_eq!(
        map.get_all("x-a")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect::<Vec<_>>(),
        [b"replace".as_slice(), b"second", b"third", b"fourth"]
    );
    no_allocation(|| {
        map.remove("x-a");
        map.try_append(HeaderName::from_static("x-b"), value("reused"))
            .unwrap();
        map.clear();
    });
    assert_eq!(map.capacity(), names.len());
    no_allocation(|| {
        map.try_insert(HeaderName::from_static("x-reuse"), value("ok"))
            .unwrap()
    });
    drop(map);
    funded.finish();
}

#[test]
fn eager_fallible_clone_escapes_pool_and_failure_allocates_nothing() {
    let funded = Funded::new(2, 4, 3);
    let mut map = funded.map();
    map.try_append(HeaderName::from_static("x-a"), value("first"))
        .unwrap();
    map.try_append(HeaderName::from_static("x-a"), value("second"))
        .unwrap();
    let (mut copy, calls, _) = measure(|| map.try_clone().unwrap());
    assert_eq!(calls, 3);
    assert_ne!(
        map.get("x-a").unwrap() as *const _,
        copy.get("x-a").unwrap() as *const _
    );
    assert!(no_allocation(|| map.try_clone()).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| drop(map.clone()))).is_err());
    assert_eq!(funded.pool.available_maps(), 0);
    copy.get_mut("x-a").unwrap().set_sensitive(true);
    assert!(!map["x-a"].is_sensitive());
    assert!(copy["x-a"].is_sensitive());
    let Funded {
        pool,
        budget,
        total,
        bound,
        ..
    } = funded;
    assert!(PEAK.with(Cell::get) <= bound);
    drop(pool);
    drop(map);
    held(&budget);
    assert_eq!(copy.get_all("x-a").iter().count(), 2);
    drop(copy);
    assert_eq!(EXITS.with(Cell::get), 1);
    reserve_and_drop(&budget, total);
}

#[test]
fn partial_into_iter_retains_original_claim_until_its_buffers_physically_exit() {
    let funded = Funded::new(1, 4, 3);
    let mut map = funded.map();
    map.try_append(HeaderName::from_static("x-a"), value("first"))
        .unwrap();
    map.try_append(HeaderName::from_static("x-a"), value("second"))
        .unwrap();
    map.try_insert(HeaderName::from_static("x-b"), value("other"))
        .unwrap();
    let mut iterator = no_allocation(|| map.into_iter());
    assert_eq!(
        iterator.next().unwrap(),
        (Some(HeaderName::from_static("x-a")), value("first"))
    );
    assert_eq!(funded.pool.available_maps(), 0);
    assert!(no_allocation(|| HeaderMap::try_from_allocation_pool(&funded.pool)).is_err());
    let Funded {
        pool,
        budget,
        total,
        ..
    } = funded;
    drop(pool);
    held(&budget);
    drop(iterator);
    assert_eq!(EXITS.with(Cell::get), 1);
    reserve_and_drop(&budget, total);
}

#[test]
fn value_drain_claims_before_mutation_and_releases_after_partial_iterator_drop() {
    let funded = Funded::new(2, 4, 3);
    let mut map = funded.map();
    map.try_append(HeaderName::from_static("x-a"), value("head"))
        .unwrap();
    map.try_append(HeaderName::from_static("x-a"), value("extra1"))
        .unwrap();
    map.try_append(HeaderName::from_static("x-a"), value("extra2"))
        .unwrap();
    let blocker = funded.map();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            if let Entry::Occupied(mut entry) = map.entry(HeaderName::from_static("x-a")) {
                drop(entry.insert_mult(value("replacement")));
            }
        }))
        .is_err()
    );
    assert_eq!(map["x-a"], "head");
    assert_eq!(map.get_all("x-a").iter().count(), 3);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            if let Entry::Occupied(entry) = map.entry(HeaderName::from_static("x-a")) {
                drop(entry.remove_entry_mult());
            }
        }))
        .is_err()
    );
    assert_eq!(map.get_all("x-a").iter().count(), 3);
    drop(blocker);
    let Entry::Occupied(mut entry) = map.entry(HeaderName::from_static("x-a")) else {
        panic!("occupied");
    };
    let borrowed_entry = &mut entry;
    let (mut drain, calls, _) = measure(move || borrowed_entry.insert_mult(value("replacement")));
    assert_eq!(calls, 1, "ValueDrain allocates one bounded value Vec");
    assert_eq!(funded.pool.available_maps(), 0);
    assert_eq!(drain.next(), Some(value("head")));
    drop(drain);
    assert_eq!(funded.pool.available_maps(), 1);
    assert_eq!(map["x-a"], "replacement");
    map.try_append(HeaderName::from_static("x-a"), value("next"))
        .unwrap();
    let Entry::Occupied(entry) = map.entry(HeaderName::from_static("x-a")) else {
        panic!("occupied");
    };
    let ((name, mut drain), calls, _) = measure(|| entry.remove_entry_mult());
    assert_eq!(calls, 1);
    assert_eq!(name, "x-a");
    assert_eq!(drain.next(), Some(value("replacement")));
    drop(drain);
    assert!(map.is_empty());
    assert_eq!(funded.pool.available_maps(), 1);
    assert!(PEAK.with(Cell::get) <= funded.bound);
    drop(map);
    funded.finish();
}

#[test]
fn mutable_views_and_sensitive_flags_are_independent_and_generic_cell_clone_stays_eager() {
    let funded = Funded::new(2, 4, 3);
    let mut map = funded.map();
    map.try_append(HeaderName::from_static("x-a"), value("head"))
        .unwrap();
    map.try_append(HeaderName::from_static("x-a"), value("extra"))
        .unwrap();
    let mut copy = measure(|| map.try_clone().unwrap()).0;
    no_allocation(|| {
        for (_, value) in copy.iter_mut() {
            value.set_sensitive(true);
        }
        assert!(map.values().all(|value| !value.is_sensitive()));
        for value in copy.values_mut() {
            value.set_sensitive(false);
        }
        if let Entry::Occupied(mut entry) = copy.entry(HeaderName::from_static("x-a")) {
            for value in entry.iter_mut() {
                value.set_sensitive(true);
            }
        }
    });
    assert!(copy.values().all(HeaderValue::is_sensitive));
    assert!(map.values().all(|value| !value.is_sensitive()));
    drop((map, copy));
    funded.finish();
    let mut ordinary = HeaderMap::<Cell<usize>>::default();
    ordinary.append(HeaderName::from_static("x-a"), Cell::new(1));
    ordinary.append(HeaderName::from_static("x-a"), Cell::new(2));
    let (copy, calls, _) = measure(|| ordinary.try_clone().unwrap());
    assert!(calls >= 3, "ordinary generic Clone remains eager");
    copy["x-a"].set(9);
    assert_eq!(ordinary["x-a"].get(), 1);
    assert_eq!(
        copy.get_all("x-a")
            .iter()
            .map(Cell::get)
            .collect::<Vec<_>>(),
        [9, 2]
    );
    drop((ordinary, copy));
    assert_eq!(LIVE.with(Cell::get), 0);
}

struct FieldOwner;
impl AsRef<[u8]> for FieldOwner {
    fn as_ref(&self) -> &[u8] {
        b"x-aliasvalue"
    }
}
#[test]
fn escaped_field_payload_alias_has_a_separate_original_grant_after_map_storage_exit() {
    let field_grant = Bytes::owner_with_exit_guard_metadata_size::<FieldOwner, ResultWriteCredit>();
    let funded = Funded::with_extra_grant(1, 4, 3, field_grant);
    let ResultWriteAdmission::Granted(credit) =
        funded.budget.try_reserve_process(field_grant).unwrap()
    else {
        panic!("field pregrant");
    };
    let bytes = Bytes::from_owner_with_exit_guard(FieldOwner, credit);
    let pointer = bytes.as_ptr();
    let name = HeaderName::from_lowercase_bytes(bytes.slice(..7)).unwrap();
    let field = HeaderValue::from_maybe_shared(bytes.slice(7..)).unwrap();
    let mut map = funded.map();
    no_allocation(|| map.try_insert(name, field).unwrap());
    drop(bytes);
    let mut iterator = no_allocation(|| map.into_iter());
    let alias = iterator.next().unwrap();
    drop(iterator);
    let Funded {
        pool,
        budget,
        grant,
        total,
        ..
    } = funded;
    drop(pool);
    assert_eq!(EXITS.with(Cell::get), 1);
    reserve_and_drop(&budget, grant);
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    assert_eq!(alias.0.as_ref().unwrap().as_str().as_ptr(), pointer);
    assert_eq!(alias.1.as_bytes(), b"value");
    drop(alias);
    reserve_and_drop(&budget, total);
}

#[test]
fn infallible_capacity_panics_leave_fixed_map_and_pool_reusable() {
    let funded = Funded::new(1, 4, 1);
    let mut map = funded.map();
    map.append(HeaderName::from_static("x-a"), value("head"));
    map.append(HeaderName::from_static("x-a"), value("extra"));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            map.append(HeaderName::from_static("x-a"), value("reject"));
        }))
        .is_err()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| map.reserve(100))).is_err());
    assert_eq!(map.get_all("x-a").iter().count(), 2);
    assert_eq!(funded.pool.available_maps(), 0);
    no_allocation(|| {
        drop(map.drain());
        map.try_insert(HeaderName::from_static("x-b"), value("reused"))
            .unwrap();
    });
    drop(map);
    assert_eq!(funded.pool.available_maps(), 1);
    funded.finish();
}

// Used only to select adversarial input names for the current fast hash.
// Correctness is checked through the actual public maps, not this hasher.
struct InputHash(u64);
impl Hasher for InputHash {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
    }
}
#[test]
fn colliding_keys_duplicate_links_and_removal_match_the_ordinary_map() {
    let mut names = Vec::new();
    for index in 0..100_000 {
        let name = HeaderName::from_bytes(format!("x-collision-{index}").as_bytes()).unwrap();
        let mut hash = InputHash(0xcbf29ce484222325);
        name.hash(&mut hash);
        if hash.finish() & 63 == 0 {
            names.push(name);
        }
        if names.len() == 32 {
            break;
        }
    }
    assert_eq!(names.len(), 32);
    // Custom names may promote their Bytes payload to shared metadata on
    // first Clone. Prepare that independent payload owner before measuring
    // the fixed HeaderMap scaffolds and their original grant.
    for name in &names {
        drop(name.clone());
    }
    let funded = Funded::new(1, 48, 3);
    let mut bounded = funded.map();
    assert_eq!(bounded.capacity(), 48);
    let mut ordinary = HeaderMap::new();
    for name in &names {
        no_allocation(|| bounded.try_insert(name.clone(), value("head")).unwrap());
        ordinary.insert(name.clone(), value("head"));
    }
    for name in &names[..3] {
        no_allocation(|| bounded.try_append(name.clone(), value("extra")).unwrap());
        ordinary.append(name.clone(), value("extra"));
    }
    for name in names.iter().step_by(3) {
        let removed = no_allocation(|| bounded.remove(name));
        assert_eq!(removed, ordinary.remove(name));
    }
    for name in &names {
        assert!(
            bounded
                .get_all(name)
                .iter()
                .eq(ordinary.get_all(name).iter())
        );
    }
    assert_eq!(bounded.len(), ordinary.len());
    drop(bounded);
    funded.finish();
}
