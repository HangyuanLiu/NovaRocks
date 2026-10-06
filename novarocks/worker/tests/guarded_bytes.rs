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

//! Pins the wrapper sizes `guarded_bytes` charges against the allocations
//! the upstream `bytes` crate actually makes, and the exit order it relies on.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_worker::guarded_bytes::{
    BYTES_MUT_SHARED_HEADER_BYTES, bytes_with_exit_guard, owner_wrapper_bytes,
};

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATED.with(|bytes| bytes.set(bytes.get() + layout.size()));
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn counted<R>(work: impl FnOnce() -> R) -> (R, usize, usize) {
    ALLOCATED.with(|bytes| bytes.set(0));
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|on| on.set(true));
    let value = work();
    COUNTING.with(|on| on.set(false));
    (
        value,
        ALLOCATED.with(Cell::get),
        ALLOCATIONS.with(Cell::get),
    )
}

struct Flag(Arc<AtomicBool>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn wrapper_is_the_only_allocation_and_matches_its_constant() {
    let backing = vec![7_u8; 64];
    let released = Arc::new(AtomicBool::new(false));
    let guard = Flag(Arc::clone(&released));
    let (bytes, allocated, allocations) = counted(move || bytes_with_exit_guard(backing, guard));
    assert_eq!(allocations, 1);
    assert_eq!(allocated, owner_wrapper_bytes::<Vec<u8>, Flag>());
    assert_eq!(&bytes[..], &[7_u8; 64][..]);
    drop(bytes);
    assert!(released.load(Ordering::SeqCst));
}

#[test]
fn guard_waits_for_the_last_clone_and_slice() {
    let released = Arc::new(AtomicBool::new(false));
    let bytes = bytes_with_exit_guard(vec![1_u8; 32], Flag(Arc::clone(&released)));
    let clone = bytes.clone();
    let slice = bytes.slice(4..8);
    drop(bytes);
    drop(clone);
    assert!(!released.load(Ordering::SeqCst));
    assert_eq!(&slice[..], &[1_u8; 4][..]);
    drop(slice);
    assert!(released.load(Ordering::SeqCst));
}

#[test]
fn backing_drops_before_its_guard() {
    struct Backing(Arc<AtomicBool>, Vec<u8>);
    impl AsRef<[u8]> for Backing {
        fn as_ref(&self) -> &[u8] {
            &self.1
        }
    }
    impl Drop for Backing {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct OrderGuard(Arc<AtomicBool>);
    impl Drop for OrderGuard {
        fn drop(&mut self) {
            assert!(
                self.0.load(Ordering::SeqCst),
                "guard must not run while its backing is live"
            );
        }
    }
    let backing_dropped = Arc::new(AtomicBool::new(false));
    let bytes = bytes_with_exit_guard(
        Backing(Arc::clone(&backing_dropped), vec![0; 8]),
        OrderGuard(Arc::clone(&backing_dropped)),
    );
    drop(bytes);
    assert!(backing_dropped.load(Ordering::SeqCst));
}

#[test]
fn bytes_mut_shared_header_matches_its_constant() {
    let mut buffer = bytes::BytesMut::with_capacity(256);
    buffer.extend_from_slice(&[3_u8; 128]);
    let (frozen, allocated, allocations) = counted(move || {
        let mut buffer = buffer;
        let tail = buffer.split_off(64);
        (buffer.freeze(), tail.freeze())
    });
    assert_eq!(allocations, 1);
    assert_eq!(allocated, BYTES_MUT_SHARED_HEADER_BYTES);
    drop(frozen);
}
