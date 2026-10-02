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

//! Global allocator adapter. Every route preserves requested Layout and provenance.
use super::{
    band::{is_tagged, tagged_layout},
    counters::{BandCounters, BandSnapshot},
    tls,
};
use crate::lane::{RecordRef, global_store};
use crate::observe::allocator::AllocatorSnapshot;
use std::alloc::{GlobalAlloc, Layout};
use std::ptr;
/// Const construction permits installation before main. Hook paths allocate no
/// metadata, acquire no locks, invoke no application policy and never unwind.
pub struct AttributingAllocator<A: GlobalAlloc> {
    inner: A,
    counters: BandCounters,
}
impl<A: GlobalAlloc> AttributingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            counters: BandCounters::new(),
        }
    }
    pub const fn inner(&self) -> &A {
        &self.inner
    }
    pub fn snapshot(&self) -> AllocatorSnapshot {
        self.counters.snapshot().total
    }
    pub fn band_snapshot(&self) -> BandSnapshot {
        self.counters.snapshot()
    }
    #[inline]
    fn owner() -> RecordRef {
        let owner = tls::effective_owner();
        if owner.is_none() {
            global_store().unattributed_ref(crate::lane::faults::stack_shard())
        } else {
            owner
        }
    }
    #[inline]
    unsafe fn allocate(&self, layout: Layout, zeroed: bool) -> *mut u8 {
        if !is_tagged(layout.size()) {
            // SAFETY: original caller layout is unchanged, including zeroing semantics.
            let p = unsafe {
                if zeroed {
                    self.inner.alloc_zeroed(layout)
                } else {
                    self.inner.alloc(layout)
                }
            };
            self.counters.small.record_allocation(p, layout.size());
            return p;
        }
        let Some(extended) = tagged_layout(layout) else {
            self.counters.tagged.record_failure();
            return ptr::null_mut();
        };
        let owner = Self::owner();
        // SAFETY: checked extended layout retains original alignment.
        let p = unsafe {
            if zeroed {
                self.inner.alloc_zeroed(extended)
            } else {
                self.inner.alloc(extended)
            }
        };
        self.counters.tagged.record_allocation(p, extended.size());
        if !p.is_null() {
            // SAFETY: reserved eight initialized/writable tail bytes. The bound
            // owner or immortal unattributed record is held through publication.
            unsafe {
                owner.write(p.add(layout.size()));
                tls::add(owner, extended.size() as i64, 0, 1)
            };
        }
        p
    }
}
// SAFETY: the wrapper reserves exactly eight tail bytes for tagged layouts,
// retains the user's alignment/content, and translates every release/resize to
// the exact corresponding inner layout. Facts publish before pointer escape;
// a genuine allocation or slot pin protects the last record update on release.
unsafe impl<A: GlobalAlloc> GlobalAlloc for AttributingAllocator<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { self.allocate(layout, false) }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe { self.allocate(layout, true) }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if !is_tagged(layout.size()) {
            // SAFETY: original pointer and layout are forwarded unchanged.
            unsafe { self.inner.dealloc(p, layout) };
            self.counters.small.record_release(layout.size());
            self.counters.record_deallocation(false);
            return;
        }
        // A valid live tagged block implies this checked layout exists. Invalid
        // caller layouts are outside GlobalAlloc's contract; still never unwind.
        let Some(extended) = tagged_layout(layout) else {
            self.counters.tagged.record_failure();
            return;
        };
        // SAFETY: genuine live allocation's initialized tail before release.
        let owner = unsafe { RecordRef::read(p.add(layout.size())) };
        let valid = global_store().resolve_hook(owner).is_some();
        // SAFETY: exactly the layout used to allocate this pointer.
        unsafe { self.inner.dealloc(p, extended) };
        if valid {
            // SAFETY: published count or source slot pin keeps the record alive
            // until this final decrement. No record is read after the decrement.
            unsafe { tls::add(owner, -(extended.size() as i64), 0, -1) };
        }
        self.counters.tagged.record_release(extended.size());
        self.counters.record_deallocation(true);
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old_tagged = is_tagged(layout.size());
        let new_tagged = is_tagged(new_size);
        if !old_tagged && !new_tagged {
            // SAFETY: caller's original valid resize arguments, unchanged.
            let next = unsafe { self.inner.realloc(p, layout, new_size) };
            self.counters
                .small
                .record_reallocation(next, layout.size(), new_size);
            return next;
        }
        let new_layout = Layout::from_size_align(new_size, layout.align())
            .ok()
            .and_then(|l| {
                if new_tagged {
                    tagged_layout(l)
                } else {
                    Some(l)
                }
            });
        let Some(new_layout) = new_layout else {
            self.counters.band(new_tagged).record_failure();
            return ptr::null_mut();
        };
        let old_layout = if old_tagged {
            tagged_layout(layout)
        } else {
            Some(layout)
        };
        let Some(old_layout) = old_layout else {
            self.counters.band(new_tagged).record_failure();
            return ptr::null_mut();
        };
        let owner = if old_tagged {
            // SAFETY: read before inner realloc may invalidate the old pointer.
            unsafe { RecordRef::read(p.add(layout.size())) }
        } else {
            Self::owner()
        };
        let valid = if old_tagged {
            global_store().resolve_hook(owner).is_some()
        } else {
            true
        };
        // SAFETY: exact old inner layout and checked new size, original alignment.
        let next = unsafe { self.inner.realloc(p, old_layout, new_layout.size()) };
        if next.is_null() {
            self.counters.band(new_tagged).record_failure();
            return next;
        }
        if new_tagged {
            // SAFETY: new block includes requested eight-byte tail at new_size.
            unsafe { owner.write(next.add(new_size)) };
        }
        if valid {
            let (bytes, count) = match (old_tagged, new_tagged) {
                (true, true) => (new_layout.size() as i64 - old_layout.size() as i64, 0),
                (false, true) => (new_layout.size() as i64, 1),
                (true, false) => (-(old_layout.size() as i64), -1),
                (false, false) => (0, 0),
            };
            // SAFETY: original live allocation protects resize/removal; current
            // binding owner protects a newly established source. Static store.
            unsafe { tls::add(owner, bytes, 0, count) };
        }
        if old_tagged == new_tagged {
            self.counters.band(new_tagged).record_reallocation(
                next,
                old_layout.size(),
                new_layout.size(),
            );
        } else {
            self.counters
                .migrate(old_tagged, old_layout.size(), new_layout.size());
        }
        next
    }
}
