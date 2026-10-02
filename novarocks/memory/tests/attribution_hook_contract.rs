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
use novarocks_memory::lane::{ResponsibilityClass, StoreHandle};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
static ARMED: AtomicBool = AtomicBool::new(false);
static UNEXPECTED: AtomicU64 = AtomicU64::new(0);
struct MetadataProbe;
unsafe impl GlobalAlloc for MetadataProbe {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            UNEXPECTED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            UNEXPECTED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static GLOBAL: MetadataProbe = MetadataProbe;
#[test]
fn hooks_make_no_nested_metadata_allocations_including_first_tls_access() {
    let a = AttributingAllocator::new(System);
    let o = StoreHandle::global()
        .acquire(7, ResponsibilityClass::Query)
        .unwrap();
    let l = Layout::from_size_align(512, 8).unwrap();
    let before = UNEXPECTED.load(Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let previous = unsafe { binding::install_ambient(o.reference()) };
    let p = unsafe { a.alloc_zeroed(l) };
    let q = unsafe { a.realloc(p, l, 2048) };
    unsafe {
        a.dealloc(q, Layout::from_size_align(2048, 8).unwrap());
        binding::restore_ambient(previous)
    };
    ARMED.store(false, Ordering::Relaxed);
    assert!(!p.is_null());
    assert!(!q.is_null());
    assert_eq!(UNEXPECTED.load(Ordering::Relaxed), before);
    assert_eq!(a.snapshot().live_bytes, 0);
}
