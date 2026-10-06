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
use novarocks_memory::attribution::AttributingAllocator;
use std::alloc::{GlobalAlloc, Layout, System};
#[global_allocator]
static GLOBAL: AttributingAllocator<System> = AttributingAllocator::new(System);

#[test]
fn tails_preserve_all_alignments_and_zeroed_user_bytes() {
    let a = AttributingAllocator::new(System);
    for align in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096] {
        for size in [1, 511, 512, 513, 4095, 4096, 4097] {
            let l = Layout::from_size_align(size, align).unwrap();
            unsafe {
                let p = a.alloc_zeroed(l);
                assert!(!p.is_null());
                assert_eq!(p.addr() % align, 0);
                assert!(std::slice::from_raw_parts(p, size).iter().all(|b| *b == 0));
                // Touch the complete user range: token must never occupy it.
                std::ptr::write_bytes(p, 0xa5, size);
                a.dealloc(p, l);
            }
        }
    }
    assert_eq!(a.snapshot().live_bytes, 0);
    assert_eq!(a.snapshot().allocations, 91);
    assert_eq!(a.band_snapshot().small_deallocations, 26);
    assert_eq!(a.band_snapshot().tagged_deallocations, 65);
    assert_eq!(a.band_snapshot().total_deallocations, 91);
}
#[test]
fn extended_layout_overflow_is_a_failure_without_backend_call() {
    let a = AttributingAllocator::new(System);
    let l = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
    assert!(unsafe { a.alloc(l) }.is_null());
    assert!(unsafe { a.alloc_zeroed(l) }.is_null());
    assert_eq!(a.snapshot().failures, 2);
    assert_eq!(a.snapshot().allocations, 0);
    assert_eq!(a.snapshot().live_bytes, 0);
}

#[test]
fn const_wrapper_is_available_without_runtime_initialization() {
    // libtest startup itself has already exercised the installed const wrapper.
    assert!(GLOBAL.snapshot().allocations > 0);
    let l = Layout::from_size_align(513, 1).unwrap();
    let p = unsafe { GLOBAL.alloc(l) };
    assert!(!p.is_null());
    unsafe { GLOBAL.dealloc(p, l) };
}
