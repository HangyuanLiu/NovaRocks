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
use common::*;
use novarocks_memory::attribution::{AttributingAllocator, binding, future::attributed};
use novarocks_memory::lane::{LaneHandle, RecordRef, global_store};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    future::Future,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll, Waker},
};
fn layout() -> Layout {
    Layout::from_size_align(600, 8).unwrap()
}
fn allocate(a: &AttributingAllocator<System>) -> NonNull<u8> {
    NonNull::new(unsafe { a.alloc(layout()) }).unwrap()
}
fn source(p: NonNull<u8>) -> RecordRef {
    // SAFETY: genuine tagged block has its initialized reserved tail.
    unsafe { RecordRef::read(p.as_ptr().add(600)) }
}
fn release(a: &AttributingAllocator<System>, p: NonNull<u8>) {
    // SAFETY: exact original layout, one release per returned pointer.
    unsafe { a.dealloc(p.as_ptr(), layout()) };
}
#[test]
fn nested_steps_restore_bindings_and_leave_no_slot_on_unwind() {
    let authority = authority(65_536);
    let q = work(&authority);
    let x = q.create_lane().unwrap();
    let y = q.create_lane().unwrap();
    let a = AttributingAllocator::new(System);
    x.run(|| {
        x.run(|| {
            let p = allocate(&a);
            assert_eq!(source(p), x.reference());
            release(&a, p);
        });
        y.run(|| {
            let p = allocate(&a);
            assert_eq!(source(p), y.reference());
            release(&a, p);
        });
        let p = allocate(&a);
        assert_eq!(source(p), x.reference());
        release(&a, p);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| y.run(|| {
                let p = allocate(&a);
                release(&a, p);
                panic!("intentional step unwind");
            })))
            .is_err()
        );
        let p = allocate(&a);
        assert_eq!(source(p), x.reference());
        release(&a, p);
    });
    assert_eq!(binding::pending_bytes(), 0);
    for lane in [x, y] {
        let s = global_store().snapshot_ref(lane.reference()).unwrap();
        assert_eq!((s.tagged_bytes, s.outstanding, s.slot_pins), (0, 0, 0));
        lane.stop_producing().unwrap();
    }
    let p = allocate(&a);
    assert_ne!(source(p), RecordRef::NONE);
    assert_eq!(
        global_store()
            .resolve(source(p))
            .unwrap()
            .responsibility_class(),
        novarocks_memory::lane::ResponsibilityClass::Unattributed
    );
    release(&a, p);
}
#[test]
fn funding_writer_and_observation_steps_share_facts_without_refusing_nesting() {
    let authority = authority(65_536);
    let q = work(&authority);
    let d = q.create_domain(1024).unwrap();
    let scope = d.activate(1024, 0).unwrap();
    let a = AttributingAllocator::new(System);
    let p = d.lane().run(|| allocate(&a));
    assert_eq!(source(p), d.lane().reference());
    let receipt = scope.finish();
    assert_eq!(receipt.accepted_live, 608);
    release(&a, p);
}
#[test]
fn sealed_lane_runs_with_outer_binding_and_records_coverage_failure() {
    let authority = authority(65_536);
    let q = work(&authority);
    let outer = q.create_lane().unwrap();
    let sealed = q.create_lane().unwrap();
    sealed.seal();
    let before = global_store().faults.snapshot();
    let a = AttributingAllocator::new(System);
    outer.run(|| {
        sealed.run(|| {
            let p = allocate(&a);
            assert_eq!(source(p), outer.reference());
            release(&a, p);
        })
    });
    let after = global_store().faults.snapshot();
    assert_eq!(after.binding_failures, before.binding_failures + 1);
    assert_eq!(after.scope_refusals, before.scope_refusals + 1);
    assert_eq!(binding::pending_bytes(), 0);
}
struct Block {
    pointer: NonNull<u8>,
    allocator: Arc<AttributingAllocator<System>>,
}
// SAFETY: unique block ownership crosses threads; no concurrent pointer access.
unsafe impl Send for Block {}
impl Drop for Block {
    fn drop(&mut self) {
        release(&self.allocator, self.pointer);
    }
}
struct Probe {
    allocator: Arc<AttributingAllocator<System>>,
    lane: RecordRef,
    blocks: Vec<Block>,
    panic: bool,
}
impl Future for Probe {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        let p = allocate(&self.allocator);
        assert_eq!(source(p), self.lane);
        let allocator = self.allocator.clone();
        self.blocks.push(Block {
            pointer: p,
            allocator,
        });
        if self.panic {
            panic!("intentional poll unwind")
        }
        if self.blocks.len() == 1 {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
fn probe(lane: &LaneHandle, panic: bool) -> Probe {
    Probe {
        allocator: Arc::new(AttributingAllocator::new(System)),
        lane: lane.reference(),
        blocks: Vec::new(),
        panic,
    }
}
#[test]
fn future_migration_pending_ready_and_drop_keep_one_lane_and_flush_each_poll() {
    let authority = authority(65_536);
    let q = work(&authority);
    let lane = q.create_lane().unwrap();
    let high_water = global_store().high_water();
    let future = Box::pin(attributed(lane.clone(), probe(&lane, false)));
    let future = std::thread::spawn(move || {
        let mut future = future;
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert_eq!(binding::pending_bytes(), 0);
        future
    })
    .join()
    .unwrap();
    let future = std::thread::spawn(move || {
        let mut future = future;
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_ready());
        assert_eq!(binding::pending_bytes(), 0);
        future
    })
    .join()
    .unwrap();
    assert_eq!(global_store().high_water(), high_water);
    let s = global_store().snapshot_ref(lane.reference()).unwrap();
    assert_eq!((s.tagged_bytes, s.outstanding, s.slot_pins), (1216, 2, 0));
    drop(future);
    let s = global_store().snapshot_ref(lane.reference()).unwrap();
    assert_eq!((s.tagged_bytes, s.outstanding, s.slot_pins), (0, 0, 0));
}
#[test]
fn future_poll_unwind_and_pending_drop_restore_the_outer_step() {
    let authority = authority(65_536);
    let q = work(&authority);
    let x = q.create_lane().unwrap();
    let y = q.create_lane().unwrap();
    let a = AttributingAllocator::new(System);
    for panic in [false, true] {
        let mut future = Box::pin(attributed(y.clone(), probe(&y, panic)));
        x.run(|| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            }));
            assert_eq!(result.is_err(), panic);
            let p = allocate(&a);
            assert_eq!(source(p), x.reference());
            release(&a, p);
            drop(future);
            let p = allocate(&a);
            assert_eq!(source(p), x.reference());
            release(&a, p);
        });
        assert_eq!(binding::pending_bytes(), 0);
        let s = global_store().snapshot_ref(y.reference()).unwrap();
        assert_eq!((s.tagged_bytes, s.outstanding, s.slot_pins), (0, 0, 0));
    }
}
