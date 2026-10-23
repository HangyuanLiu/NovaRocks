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
use std::sync::atomic::{AtomicUsize, Ordering};
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
const UNASSIGNED_SHARD: u8 = u8::MAX;
#[derive(Clone, Copy)]
struct ThreadState {
    ambient: RecordRef,
    explicit: RecordRef,
    slot: SlotCore,
    shard: u8,
    role: u8,
}
impl ThreadState {
    const fn new() -> Self {
        Self {
            ambient: RecordRef::NONE,
            explicit: RecordRef::NONE,
            slot: SlotCore::new(),
            shard: UNASSIGNED_SHARD,
            role: 0,
        }
    }
    fn buffered(self) -> bool {
        self.ambient.is_record() || self.explicit.is_record()
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
            let owner = if !state.explicit.is_none() {
                state.explicit
            } else {
                state.ambient
            };
            if owner.is_record() {
                owner
            } else {
                global_store()
                    .unattributed_ref(usize::from(state.role), crate::lane::faults::stack_shard())
            }
        })
        .unwrap_or_else(|_| global_store().unattributed_ref(0, crate::lane::faults::stack_shard()))
}
/// Stable execution-thread shard. It grants no owner or allocation access.
pub(crate) fn current_shard() -> usize {
    STATE
        .try_with(|cell| {
            let mut state = cell.get();
            if state.shard == UNASSIGNED_SHARD {
                state.shard =
                    (NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % crate::lane::LANE_SHARDS) as u8;
                cell.set(state);
            }
            usize::from(state.shard)
        })
        .unwrap_or_else(|_| crate::lane::faults::stack_shard() % crate::lane::LANE_SHARDS)
}
pub(crate) fn set_thread_role(role: u8) -> bool {
    STATE
        .try_with(|cell| {
            let mut state = cell.get();
            state.role = role;
            cell.set(state);
        })
        .is_ok()
}
/// Caller holds a live owner, genuine allocation, or the slot pin; identity alone
/// is insufficient. The production global store is the only supported store.
pub(crate) unsafe fn add(reference: RecordRef, tagged: i64, small: i64, count: i64) {
    unsafe { add_inner(reference, tagged, small, count, false) };
}
pub(crate) unsafe fn add_transfer(reference: RecordRef, tagged: i64, small: i64, count: i64) {
    unsafe { add_inner(reference, tagged, small, count, true) };
}
unsafe fn add_inner(reference: RecordRef, tagged: i64, small: i64, count: i64, transfer: bool) {
    access();
    if STATE
        .try_with(|cell| {
            let mut state = cell.get();
            let buffered = state.buffered();
            // SAFETY: caller's lifetime capability protects the new reference; the
            // copied slot is used exactly once and immediately replaced in the Cell.
            unsafe {
                if transfer {
                    state.slot.add_transfer(
                        global_store(),
                        reference,
                        tagged,
                        small,
                        count,
                        buffered,
                    );
                } else {
                    state
                        .slot
                        .add(global_store(), reference, tagged, small, count, buffered);
                }
            };
            cell.set(state);
        })
        .is_err()
    {
        // SAFETY: same caller lifetime capability, no TLS slot involved.
        unsafe {
            if transfer {
                SlotCore::direct_transfer(global_store(), reference, tagged, small, count);
            } else {
                SlotCore::direct(global_store(), reference, tagged, small, count);
            }
        };
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
    try_replace(reference, explicit, restoring).unwrap_or(RecordRef::NONE)
}
pub(crate) fn try_replace(
    reference: RecordRef,
    explicit: bool,
    restoring: bool,
) -> Option<RecordRef> {
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
            if restoring && (!explicit || !state.ambient.is_record()) {
                // SAFETY: sole slot pin; flush completes before a binding owner drops.
                unsafe { state.slot.flush(global_store()) };
            }
            cell.set(state);
            old
        })
        .ok()
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

#[cfg(test)]
mod shard_tests {
    use super::*;
    #[test]
    fn sequential_threads_receive_distinct_stable_shards() {
        let mut shards = Vec::new();
        for _ in 0..crate::lane::LANE_SHARDS {
            shards.push(
                std::thread::spawn(|| {
                    let first = current_shard();
                    assert_eq!(first, current_shard());
                    first
                })
                .join()
                .unwrap(),
            );
        }
        shards.sort_unstable();
        assert_eq!(shards, (0..crate::lane::LANE_SHARDS).collect::<Vec<_>>());
    }
}
