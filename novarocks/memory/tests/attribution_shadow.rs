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
use novarocks_memory::{
    attribution::{
        AttributingAllocator, binding,
        future::AttributedFuture,
        scope::{AmbientExitObservation, OwnerEntryObservation, OwnerStepObservation},
    },
    lane::{RecordRef, ResponsibilityClass, global_store},
    *,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    future::Future,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll, Waker},
};
fn layout(n: usize) -> Layout {
    Layout::from_size_align(n, 8).unwrap()
}
fn source(pointer: NonNull<u8>, n: usize) -> RecordRef {
    unsafe { RecordRef::read(pointer.as_ptr().add(n)) }
}
fn tagged() -> RecordRef {
    unsafe {
        let allocator = AttributingAllocator::new(System);
        let pointer = NonNull::new(allocator.alloc(layout(600))).unwrap();
        let r = source(pointer, 600);
        allocator.dealloc(pointer.as_ptr(), layout(600));
        r
    }
}
fn unassigned(r: RecordRef) {
    assert!(r.index < lane::UNATTRIBUTED_RECORDS);
    assert_eq!(
        global_store().resolve(r).unwrap().responsibility_class(),
        ResponsibilityClass::Unattributed
    );
}
struct ShadowFuture {
    owner: LaneShards,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}
impl Future for ShadowFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        unassigned(tagged());
        Poll::Pending
    }
}
impl Drop for ShadowFuture {
    fn drop(&mut self) {
        unassigned(tagged());
        self.drops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(
            self.owner.coverage().is_err()
                || self
                    .owner
                    .snapshot()
                    .records
                    .iter()
                    .flatten()
                    .all(|s| s.production != lane::ProductionState::Producing)
        );
    }
}
#[test]
fn unavailable_and_sealed_owners_shadow_service_and_other_query_through_future_lifetime() {
    let a = authority(65_536);
    let q = work(&a);
    let service = a
        .create_account(AccountKind::Service, ExternalRef::NONE)
        .unwrap();
    let outers = [q.create_lane_shards(), service.create_lane_shards()];
    let closed = work(&a);
    closed.retire(&exited()).unwrap();
    let unavailable = closed.create_lane_shards();
    let sealed = q.create_lane_shards();
    sealed.stop_producing().unwrap();
    for (owner, entry) in [
        (unavailable, OwnerEntryObservation::UncoveredShadow),
        (sealed, OwnerEntryObservation::SealedShadow),
    ] {
        for outer in &outers {
            outer.run(|| {
                let mut observation = OwnerStepObservation::default();
                let before_fault = global_store().faults.snapshot().binding_failures;
                owner.run_observed(&mut observation, || {
                    unassigned(tagged());
                });
                if entry == OwnerEntryObservation::SealedShadow {
                    assert!(global_store().faults.snapshot().binding_failures > before_fault);
                }
                assert_eq!(observation.entry(), entry);
                assert_eq!(observation.exit(), AmbientExitObservation::Restored);
                assert_eq!(tagged(), reference(outer));
                let explicit = outer.explicit_owner();
                owner.run(|| {
                    let allocator = AttributingAllocator::new(System);
                    let pointer = explicit
                        .allocate_with(layout(600), || {
                            NonNull::new(unsafe { allocator.alloc(layout(600)) }).ok_or(())
                        })
                        .unwrap();
                    assert_eq!(source(pointer, 600), reference(outer));
                    unsafe {
                        explicit.deallocate_with(layout(600), || {
                            allocator.dealloc(pointer.as_ptr(), layout(600))
                        });
                    }
                });
                let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    owner.run(|| {
                        unassigned(tagged());
                        panic!("shadow unwind")
                    })
                }));
                assert!(unwind.is_err());
                assert_eq!(tagged(), reference(outer));
                let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let mut future = Box::pin(AttributedFuture::new(owner.clone(), || {
                    unassigned(tagged());
                    ShadowFuture {
                        owner: owner.clone(),
                        drops: drops.clone(),
                    }
                }));
                assert!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert_eq!(tagged(), reference(outer));
                drop(future);
                assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 1);
                assert_eq!(tagged(), reference(outer));
            });
        }
    }
    assert_eq!(binding::pending_bytes(), 0);
}
#[test]
fn uncovered_explicit_owner_never_borrows_ambient_during_cross_band_resize() {
    let a = authority(65_536);
    let q = work(&a);
    let ambient = q.create_lane_shards();
    let closed = work(&a);
    closed.retire(&exited()).unwrap();
    let owner = closed.create_lane_shards().explicit_owner();
    assert_eq!(owner.reference(), None);
    let allocator = AttributingAllocator::new(System);
    let mut old = layout(64);
    let mut pointer = owner
        .allocate_with(old, || {
            NonNull::new(unsafe { allocator.alloc(old) }).ok_or(())
        })
        .unwrap();
    ambient.run(|| {
        for bytes in [600, 64, 700] {
            let next = layout(bytes);
            pointer = unsafe {
                owner.resize_with(old, next, || {
                    NonNull::new(allocator.realloc(pointer.as_ptr(), old, next.size())).ok_or(())
                })
            }
            .unwrap();
            if bytes >= 512 {
                unassigned(source(pointer, bytes));
            }
            old = next;
        }
    });
    unsafe {
        owner.deallocate_with(old, || allocator.dealloc(pointer.as_ptr(), old));
    }
    assert_eq!(ambient.snapshot().tagged_bytes, 0);
    assert_eq!(ambient.snapshot().r1_small_bytes, 0);
    assert_eq!(binding::pending_bytes(), 0);
}
