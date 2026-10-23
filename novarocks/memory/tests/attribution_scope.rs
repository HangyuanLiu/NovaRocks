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
use novarocks_memory::attribution::{AttributingAllocator, binding};
use novarocks_memory::lane::{RecordRef, global_store};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    ptr::NonNull,
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
    let x = q.create_lane_shards();
    let y = q.create_lane_shards();
    let a = AttributingAllocator::new(System);
    x.run(|| {
        x.run(|| {
            let p = allocate(&a);
            assert_eq!(source(p), reference(&x));
            release(&a, p);
        });
        y.run(|| {
            let p = allocate(&a);
            assert_eq!(source(p), reference(&y));
            release(&a, p);
        });
        let p = allocate(&a);
        assert_eq!(source(p), reference(&x));
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
        assert_eq!(source(p), reference(&x));
        release(&a, p);
    });
    assert_eq!(binding::pending_bytes(), 0);
    for lane in [x, y] {
        let s = lane.snapshot();
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
    let outer = q.create_lane_shards();
    let sealed = q.create_domain(1).unwrap();
    sealed.seal();
    let before = global_store().faults.snapshot();
    let a = AttributingAllocator::new(System);
    outer.run(|| {
        sealed.lane().run(|| {
            let p = allocate(&a);
            assert_eq!(source(p), reference(&outer));
            release(&a, p);
        })
    });
    let after = global_store().faults.snapshot();
    assert_eq!(after.binding_failures, before.binding_failures + 1);
    assert_eq!(after.scope_refusals, before.scope_refusals + 1);
    assert_eq!(binding::pending_bytes(), 0);
}
