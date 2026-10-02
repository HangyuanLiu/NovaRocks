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
use novarocks_memory::attribution::{AttributingAllocator, binding};
use novarocks_memory::lane::{ResponsibilityClass, StoreHandle, global_store};
use std::alloc::{GlobalAlloc, Layout, System};
fn layout(size: usize) -> Layout {
    Layout::from_size_align(size, 8).unwrap()
}
fn owner() -> novarocks_memory::lane::RecordOwner {
    StoreHandle::global()
        .acquire(7, ResponsibilityClass::Query)
        .unwrap()
}
fn bytes(r: novarocks_memory::lane::RecordRef) -> i64 {
    global_store().snapshot_ref(r).unwrap().tagged_bytes
}

use std::sync::atomic::{AtomicBool, Ordering};
struct Failable {
    fail: AtomicBool,
}
unsafe impl GlobalAlloc for Failable {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if self.fail.load(Ordering::Relaxed) {
            std::ptr::null_mut()
        } else {
            unsafe { System.alloc(l) }
        }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if self.fail.load(Ordering::Relaxed) {
            std::ptr::null_mut()
        } else {
            unsafe { System.realloc(p, l, n) }
        }
    }
}
#[test]
fn all_resize_routes_reconcile_bands_and_preserve_original_token_owner() {
    let a = AttributingAllocator::new(System);
    let x = owner();
    let y = owner();
    let rx = x.reference();
    let ry = y.reference();
    let mut l = layout(100);
    let mut p = unsafe { a.alloc(l) };
    assert!(!p.is_null());
    let old = unsafe { binding::install_ambient(rx) };
    p = unsafe { a.realloc(p, l, 200) };
    assert!(!p.is_null());
    l = layout(200);
    p = unsafe { a.realloc(p, l, 512) };
    assert!(!p.is_null());
    l = layout(512);
    unsafe { binding::restore_ambient(old) };
    assert_eq!(bytes(rx), 520);
    let old = unsafe { binding::install_ambient(ry) };
    p = unsafe { a.realloc(p, l, 1025) };
    assert!(!p.is_null());
    l = layout(1025);
    unsafe { binding::restore_ambient(old) };
    assert_eq!(bytes(rx), 1033);
    assert_eq!(bytes(ry), 0);
    assert_eq!(global_store().snapshot_ref(rx).unwrap().outstanding, 1);
    p = unsafe { a.realloc(p, l, 513) };
    assert!(!p.is_null());
    l = layout(513);
    assert_eq!(bytes(rx), 521);
    p = unsafe { a.realloc(p, l, 128) };
    assert!(!p.is_null());
    l = layout(128);
    assert_eq!(bytes(rx), 0);
    let s = a.band_snapshot();
    assert_eq!(s.small.live_bytes, 128);
    assert_eq!(s.tagged.live_bytes, 0);
    assert_eq!(s.total.live_bytes, 128);
    assert_eq!(s.total.allocations, 1);
    assert_eq!(s.total.reallocations, 5);
    assert_eq!(s.small_deallocations, 0);
    assert_eq!(s.tagged_deallocations, 0);
    assert_eq!(s.total_deallocations, 0);
    assert_eq!(s.total.allocated_total_bytes, 1033);
    assert_eq!(s.total.deallocated_total_bytes, 905);
    unsafe { a.dealloc(p, l) };
    assert_eq!(a.band_snapshot().small_deallocations, 1);
    assert_eq!(a.band_snapshot().tagged_deallocations, 0);
    assert_eq!(a.band_snapshot().total_deallocations, 1);
    assert_eq!(
        a.snapshot().allocated_total_bytes,
        a.snapshot().deallocated_total_bytes
    );
}
#[test]
fn failure_preserves_pointer_contents_token_and_all_facts_in_every_route() {
    for (old_size, new_size) in [(64, 128), (64, 512), (512, 1024), (512, 64)] {
        let a = AttributingAllocator::new(Failable {
            fail: AtomicBool::new(false),
        });
        let o = owner();
        let r = o.reference();
        let previous = unsafe { binding::install_ambient(r) };
        let p = unsafe { a.alloc(layout(old_size)) };
        assert!(!p.is_null());
        unsafe {
            std::ptr::write_bytes(p, 0x7b, old_size);
            binding::restore_ambient(previous)
        };
        let before = global_store().snapshot_ref(r).unwrap();
        let counters = a.band_snapshot();
        a.inner().fail.store(true, Ordering::Relaxed);
        assert!(unsafe { a.realloc(p, layout(old_size), new_size) }.is_null());
        assert_eq!(global_store().snapshot_ref(r).unwrap(), before);
        assert_eq!(a.snapshot().live_bytes, counters.total.live_bytes);
        assert_eq!(
            a.snapshot().allocated_total_bytes,
            counters.total.allocated_total_bytes
        );
        assert_eq!(
            a.snapshot().deallocated_total_bytes,
            counters.total.deallocated_total_bytes
        );
        assert_eq!(a.snapshot().reallocations, 0);
        assert_eq!(a.snapshot().failures, 1);
        assert_eq!(a.band_snapshot().total_deallocations, 0);
        assert!(
            unsafe { std::slice::from_raw_parts(p, old_size) }
                .iter()
                .all(|b| *b == 0x7b)
        );
        if old_size >= 512 {
            assert_eq!(
                unsafe { novarocks_memory::lane::RecordRef::read(p.add(old_size)) },
                r
            );
        }
        unsafe { a.dealloc(p, layout(old_size)) };
    }
}
#[test]
fn checked_resize_overflow_keeps_the_old_allocation_live() {
    let a = AttributingAllocator::new(System);
    let p = unsafe { a.alloc(layout(512)) };
    assert!(!p.is_null());
    let before = a.snapshot();
    assert!(unsafe { a.realloc(p, layout(512), isize::MAX as usize - 7) }.is_null());
    assert_eq!(a.snapshot().live_bytes, before.live_bytes);
    assert_eq!(a.snapshot().failures, 1);
    unsafe { a.dealloc(p, layout(512)) };
}

#[test]
fn failed_alloc_and_zeroed_alloc_publish_no_lane_facts() {
    let a = AttributingAllocator::new(Failable {
        fail: AtomicBool::new(true),
    });
    let o = owner();
    let r = o.reference();
    let before = global_store().snapshot_ref(r).unwrap();
    let previous = unsafe { binding::install_ambient(r) };
    for size in [64, 512] {
        assert!(unsafe { a.alloc(layout(size)) }.is_null());
        assert!(unsafe { a.alloc_zeroed(layout(size)) }.is_null());
    }
    unsafe { binding::restore_ambient(previous) };
    assert_eq!(global_store().snapshot_ref(r).unwrap(), before);
    assert_eq!(a.snapshot().failures, 4);
    assert_eq!(a.band_snapshot().total_deallocations, 0);
    assert_eq!(a.snapshot().allocations, 0);
    assert_eq!(a.snapshot().live_bytes, 0);
}
