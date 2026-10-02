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

//! Constant, destructor-free TLS. Hooks only copy Cells and use lane atomics.
use crate::lane::{RecordRef, SlotCore, global_store};
use std::cell::Cell;
#[derive(Clone, Copy)]
struct ThreadState {
    ambient: RecordRef,
    explicit: RecordRef,
    slot: SlotCore,
}
impl ThreadState {
    const fn new() -> Self {
        Self {
            ambient: RecordRef::NONE,
            explicit: RecordRef::NONE,
            slot: SlotCore::new(),
        }
    }
    fn buffered(self) -> bool {
        !self.ambient.is_none() || !self.explicit.is_none()
    }
}
const _: () = assert!(!std::mem::needs_drop::<ThreadState>());
thread_local! { static STATE: Cell<ThreadState> = const { Cell::new(ThreadState::new()) }; }
#[cfg(test)]
thread_local! { static ACCESSES: Cell<u64> = const { Cell::new(0) }; }
#[inline]
fn access() {
    #[cfg(test)]
    ACCESSES.with(|n| n.set(n.get() + 1));
}
#[inline]
pub(crate) fn effective_owner() -> RecordRef {
    access();
    STATE
        .try_with(|cell| {
            let state = cell.get();
            if !state.explicit.is_none() {
                state.explicit
            } else {
                state.ambient
            }
        })
        .unwrap_or(RecordRef::NONE)
}
/// Caller holds a live owner, genuine allocation, or the slot pin; identity alone
/// is insufficient. The production global store is the only supported store.
pub(crate) unsafe fn add(reference: RecordRef, tagged: i64, small: i64, count: i64) {
    access();
    if STATE
        .try_with(|cell| {
            let mut state = cell.get();
            let buffered = state.buffered();
            // SAFETY: caller's lifetime capability protects the new reference; the
            // copied slot is used exactly once and immediately replaced in the Cell.
            unsafe {
                state
                    .slot
                    .add(global_store(), reference, tagged, small, count, buffered)
            };
            cell.set(state);
        })
        .is_err()
    {
        // SAFETY: same caller lifetime capability, no TLS slot involved.
        unsafe { SlotCore::direct(global_store(), reference, tagged, small, count) };
    }
}
pub(crate) fn flush() {
    access();
    let _ = STATE.try_with(|cell| {
        let mut state = cell.get();
        // SAFETY: this thread's sole slot copy owns its pin and static storage.
        unsafe { state.slot.flush(global_store()) };
        cell.set(state);
    });
}
pub(crate) fn replace(reference: RecordRef, explicit: bool, restoring: bool) -> RecordRef {
    access();
    STATE
        .try_with(|cell| {
            let mut state = cell.get();
            let old = if explicit {
                state.explicit
            } else {
                state.ambient
            };
            if explicit {
                state.explicit = reference;
            } else {
                state.ambient = reference;
            }
            if restoring && (!explicit || !state.buffered()) {
                // SAFETY: sole slot pin; flush completes before a binding owner drops.
                unsafe { state.slot.flush(global_store()) };
            }
            cell.set(state);
            old
        })
        .unwrap_or(RecordRef::NONE)
}
pub(crate) fn pending_bytes() -> u64 {
    STATE
        .try_with(|cell| cell.get().slot.pending_bytes())
        .unwrap_or(0)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::attribution::AttributingAllocator;
    use std::alloc::{GlobalAlloc, Layout, System};
    #[test]
    fn small_alloc_release_and_resize_never_read_tls() {
        let a = AttributingAllocator::new(System);
        let before = ACCESSES.with(Cell::get);
        let old = Layout::from_size_align(64, 8).unwrap();
        // SAFETY: exact live allocation and matching layouts throughout.
        unsafe {
            let p = a.alloc(old);
            assert!(!p.is_null());
            let p = a.realloc(p, old, 128);
            assert!(!p.is_null());
            a.dealloc(p, Layout::from_size_align(128, 8).unwrap());
            let p = a.alloc_zeroed(old);
            assert!(!p.is_null());
            a.dealloc(p, old);
        }
        assert_eq!(ACCESSES.with(Cell::get), before);
    }
}
