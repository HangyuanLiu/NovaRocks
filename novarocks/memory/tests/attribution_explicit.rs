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

#![cfg(not(loom))]
mod common;
use allocator_api2::alloc::{AllocError, Allocator, Global};
use common::*;
use novarocks_memory::attribution::{AttributingAllocator, binding, explicit::ExplicitOwner};
use novarocks_memory::lane::{LaneHandle, RecordRef, ResponsibilityClass, global_store};
use std::{
    alloc::{Layout, System},
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
};
#[global_allocator]
static WRAPPED: AttributingAllocator<System> = AttributingAllocator::new(System);
fn layout(n: usize) -> Layout {
    Layout::from_size_align(n, 8).unwrap()
}
fn physical(l: Layout) -> i64 {
    if l.size() == 0 {
        0
    } else {
        (l.size() + if l.size() >= 512 { 8 } else { 0 }) as i64
    }
}
#[derive(Clone)]
struct R1 {
    owner: ExplicitOwner,
    live: Arc<AtomicI64>,
    fail: Arc<AtomicBool>,
}
impl R1 {
    fn new(lane: LaneHandle) -> Self {
        Self {
            owner: ExplicitOwner::new(lane),
            live: Arc::new(AtomicI64::new(0)),
            fail: Arc::new(AtomicBool::new(false)),
        }
    }
    fn bytes(&self) -> i64 {
        self.live.load(Ordering::Relaxed)
    }
    fn check(&self) {
        binding::flush_current();
        let s = global_store()
            .snapshot_ref(self.owner.lane().reference())
            .unwrap();
        assert_eq!(
            i128::from(s.tagged_bytes) + i128::from(s.r1_small_bytes),
            i128::from(self.bytes())
        );
    }
    unsafe fn resize(
        &self,
        p: NonNull<u8>,
        old: Layout,
        new: Layout,
        kind: u8,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: this forwarding fixture has the same exact allocator contract.
        let result = unsafe {
            self.owner.resize_with(old, new, || {
                if self.fail.load(Ordering::Relaxed) {
                    return Err(AllocError);
                }
                match kind {
                    0 => Global.grow(p, old, new),
                    1 => Global.grow_zeroed(p, old, new),
                    _ => Global.shrink(p, old, new),
                }
            })
        };
        if result.is_ok() {
            self.live
                .fetch_add(physical(new) - physical(old), Ordering::Relaxed);
        }
        result
    }
}
// SAFETY: every request routes once through Global and the helper, preserves
// exact layouts and real block ownership, and success/Err follow Allocator.
unsafe impl Allocator for R1 {
    fn allocate(&self, l: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let result = self.owner.allocate_with(l, || {
            if self.fail.load(Ordering::Relaxed) {
                Err(AllocError)
            } else {
                Global.allocate(l)
            }
        });
        if result.is_ok() {
            self.live.fetch_add(physical(l), Ordering::Relaxed);
        }
        result
    }
    fn allocate_zeroed(&self, l: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let result = self.owner.allocate_with(l, || {
            if self.fail.load(Ordering::Relaxed) {
                Err(AllocError)
            } else {
                Global.allocate_zeroed(l)
            }
        });
        if result.is_ok() {
            self.live.fetch_add(physical(l), Ordering::Relaxed);
        }
        result
    }
    unsafe fn deallocate(&self, p: NonNull<u8>, l: Layout) {
        unsafe { self.owner.deallocate_with(l, || Global.deallocate(p, l)) };
        self.live.fetch_sub(physical(l), Ordering::Relaxed);
    }
    unsafe fn grow(
        &self,
        p: NonNull<u8>,
        old: Layout,
        new: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        unsafe { self.resize(p, old, new, 0) }
    }
    unsafe fn grow_zeroed(
        &self,
        p: NonNull<u8>,
        old: Layout,
        new: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        unsafe { self.resize(p, old, new, 1) }
    }
    unsafe fn shrink(
        &self,
        p: NonNull<u8>,
        old: Layout,
        new: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        unsafe { self.resize(p, old, new, 2) }
    }
}
fn data(p: NonNull<[u8]>) -> NonNull<u8> {
    p.cast()
}
#[test]
fn explicit_source_wins_over_ambient_and_failed_operations_preserve_facts() {
    let a = authority(65_536);
    let q = work(&a);
    let lane = q.create_lane().unwrap();
    let other = q.create_lane().unwrap();
    let r = R1::new(lane.clone());
    let mut p = other.run(|| r.allocate(layout(512)).unwrap());
    r.check();
    assert_eq!(
        unsafe { RecordRef::read(data(p).as_ptr().add(512)) },
        lane.reference()
    );
    assert_eq!(
        global_store()
            .snapshot_ref(other.reference())
            .unwrap()
            .tagged_bytes,
        0
    );
    for (old, new, kind) in [(512, 1024, 0), (512, 64, 2)] {
        let before = global_store().snapshot_ref(lane.reference()).unwrap();
        r.fail.store(true, Ordering::Relaxed);
        assert!(unsafe { r.resize(data(p), layout(old), layout(new), kind) }.is_err());
        r.check();
        assert_eq!(
            global_store().snapshot_ref(lane.reference()).unwrap(),
            before
        );
    }
    assert!(r.allocate(layout(64)).is_err());
    r.check();
    r.fail.store(false, Ordering::Relaxed);
    p = unsafe { r.shrink(data(p), layout(512), layout(128)).unwrap() };
    r.check();
    let s = global_store().snapshot_ref(lane.reference()).unwrap();
    assert_eq!(
        (s.tagged_bytes, s.r1_small_bytes, s.outstanding),
        (0, 128, 1)
    );
    unsafe { r.deallocate(data(p), layout(128)) };
    r.check();
}
#[test]
fn resize_routes_zero_requests_and_alignment_changes_have_exact_physical_facts() {
    let a = authority(65_536);
    let q = work(&a);
    let lane = q.create_lane().unwrap();
    let r = R1::new(lane.clone());
    for sizes in [vec![0, 64, 128, 512, 1024, 64, 0], vec![0, 512, 0]] {
        let mut old = layout(sizes[0]);
        let mut p = r.allocate(old).unwrap();
        r.check();
        for &n in &sizes[1..] {
            let new = layout(n);
            p = unsafe {
                if n >= old.size() {
                    r.grow_zeroed(data(p), old, new)
                } else {
                    r.shrink(data(p), old, new)
                }
            }
            .unwrap();
            r.check();
            old = new;
            let s = global_store().snapshot_ref(lane.reference()).unwrap();
            assert_eq!(s.outstanding, i64::from(n != 0));
        }
        unsafe { r.deallocate(data(p), old) };
        r.check();
    }
    let old = Layout::from_size_align(100, 8).unwrap();
    let new = Layout::from_size_align(700, 64).unwrap();
    let p = r.allocate(old).unwrap();
    unsafe { std::ptr::write_bytes(data(p).as_ptr(), 0x6b, 100) };
    let p = unsafe { r.grow(data(p), old, new).unwrap() };
    r.check();
    assert_eq!(data(p).as_ptr().addr() % 64, 0);
    assert!(
        unsafe { std::slice::from_raw_parts(data(p).as_ptr(), 100) }
            .iter()
            .all(|b| *b == 0x6b)
    );
    unsafe { r.deallocate(data(p), new) };
    r.check();
}
#[test]
fn hashbrown_insert_grow_clear_and_shrink_match_live_requested_bytes() {
    let a = authority(1 << 20);
    let q = work(&a);
    let r = R1::new(q.create_lane().unwrap());
    let mut map = hashbrown::HashMap::with_hasher_in(
        std::collections::hash_map::RandomState::new(),
        r.clone(),
    );
    for i in 0..256 {
        map.insert(i, i * 2);
        if matches!(i, 0 | 3 | 31 | 127 | 255) {
            r.check();
        }
    }
    assert!(r.bytes() > 512);
    map.clear();
    r.check();
    assert!(r.bytes() > 0);
    map.shrink_to_fit();
    r.check();
    assert_eq!(r.bytes(), 0);
    map.insert(1, 2);
    r.check();
    drop(map);
    r.check();
    assert_eq!(r.bytes(), 0);
}
#[test]
fn funded_small_facts_are_counted_once_and_survive_teardown_then_growth() {
    let a = authority(65_536);
    let q = work(&a);
    let domain = q.create_domain(128).unwrap();
    let r = R1::new(domain.lane().clone());
    let before_sample = novarocks_memory::attribution::readout::AttributionSnapshot::sample(
        global_store(),
        WRAPPED.band_snapshot(),
    );
    let p = r.allocate(layout(64)).unwrap();
    let after_sample = novarocks_memory::attribution::readout::AttributionSnapshot::sample(
        global_store(),
        WRAPPED.band_snapshot(),
    );
    assert_eq!(
        after_sample.ledger_blind_spot_bytes,
        before_sample.ledger_blind_spot_bytes
    );
    assert_eq!(
        after_sample.classified[0].r1_small_bytes - before_sample.classified[0].r1_small_bytes,
        64
    );
    r.check();
    let receipt = domain.settle();
    assert_eq!(receipt.accepted_live, 64);
    let before = a.pressure_projection();
    assert_eq!(domain.snapshot().committed, 128);
    q.retire(&exited()).unwrap();
    assert_eq!(
        domain.lane().responsibility_class(),
        ResponsibilityClass::Residual
    );
    assert_eq!(domain.snapshot().committed, 64);
    assert_eq!(a.pressure_projection().query_pressure(), 0);
    assert_eq!(
        before.root_committed - a.pressure_projection().root_committed,
        64
    );
    let before_fault = global_store().faults.snapshot().residual_growth_events;
    let p = unsafe { r.grow(data(p), layout(64), layout(96)).unwrap() };
    r.check();
    assert_eq!(
        global_store().faults.snapshot().residual_growth_events,
        before_fault + 1
    );
    assert_eq!(domain.settle().accepted_live, 96);
    unsafe { r.deallocate(data(p), layout(96)) };
    r.check();
    a.maintain(32);
}
#[test]
fn residual_tagged_to_small_shrink_is_a_transfer_without_growth_fault() {
    let a = authority(65_536);
    let q = work(&a);
    let lane = q.create_lane().unwrap();
    let r = R1::new(lane.clone());
    let p = r.allocate(layout(600)).unwrap();
    q.retire(&exited()).unwrap();
    let before = global_store().faults.snapshot().residual_growth_events;
    let p = unsafe { r.shrink(data(p), layout(600), layout(128)).unwrap() };
    r.check();
    assert_eq!(
        global_store().faults.snapshot().residual_growth_events,
        before
    );
    let p = unsafe { r.grow(data(p), layout(128), layout(512)).unwrap() };
    r.check();
    assert_eq!(
        global_store().faults.snapshot().residual_growth_events,
        before + 1
    );
    unsafe { r.deallocate(data(p), layout(512)) };
    r.check();
}
#[test]
fn nested_explicit_helpers_without_ambient_flush_on_each_exit_and_unwind() {
    let a = authority(65_536);
    let q = work(&a);
    let x = R1::new(q.create_lane().unwrap());
    let y = R1::new(q.create_lane().unwrap());
    let p = x
        .owner
        .allocate_with(layout(600), || {
            let inner = y.allocate(layout(600)).unwrap();
            assert_eq!(binding::pending_bytes(), 0);
            unsafe { y.deallocate(data(inner), layout(600)) };
            Global.allocate(layout(600))
        })
        .unwrap();
    assert_eq!(
        unsafe { RecordRef::read(data(p).as_ptr().add(600)) },
        x.owner.lane().reference()
    );
    assert_eq!(binding::pending_bytes(), 0);
    unsafe {
        x.owner
            .deallocate_with(layout(600), || Global.deallocate(data(p), layout(600)))
    };
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| x
            .owner
            .allocate_with::<(), AllocError>(layout(600), || panic!(
                "intentional helper unwind"
            ))))
        .is_err()
    );
    assert_eq!(binding::pending_bytes(), 0);
}
