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

//! Actual allocator-container growth probes. This test-only heap host verifies
//! the Allocator contract; runtime admission is covered by Execution's real
//! expression host probes, not inferred from this unit fixture.
use super::*;
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelFailure};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
};
#[derive(Clone, Debug)]
enum Event {
    Granted(usize, Layout, usize),
    Released(usize, Layout, usize),
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    live: Vec<(usize, Layout)>,
    bytes: usize,
    peak: usize,
    events: Vec<Event>,
    refusal: Option<(usize, KernelFailure)>,
}
#[derive(Default)]
struct HeapHost(Mutex<Ledger>);
impl AggregateStateAllocator for HeapHost {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.0.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &ledger.refusal {
            if at == *stop {
                return Err(cause.clone());
            }
        }
        // SAFETY: the exact nonzero Layout is passed to the physical allocator.
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        let bytes = ledger.bytes;
        ledger.peak = ledger.peak.max(bytes);
        ledger.live.push((pointer.as_ptr().addr(), layout));
        ledger
            .events
            .push(Event::Granted(pointer.as_ptr().addr(), layout, bytes));
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.0.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, l)| *address == pointer.as_ptr().addr() && *l == layout)
            .expect("exact single release");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        let bytes = ledger.bytes;
        ledger
            .events
            .push(Event::Released(pointer.as_ptr().addr(), layout, bytes));
        // SAFETY: ledger verified the exact original live block/Layout once.
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("child grow invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("child grow internal")),
        KernelFailure::Operational(KernelDiagnostic::new("child grow operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn by_copy_child_scratch_vector_actual_growth_reserves_replacement_before_release() {
    let host = Arc::new(HeapHost::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let metadata = allocator.metadata_bytes();
    let values = (0..65).collect::<Vec<usize>>();
    let mut table = ChildScratchVec::new(Some(&allocator));
    for value in &values {
        table.try_push(value).unwrap();
    }
    assert_eq!(&*table, values.iter().collect::<Vec<_>>().as_slice());
    let ledger = host.0.lock().unwrap();
    let grants = ledger
        .events
        .iter()
        .filter_map(|e| {
            if let Event::Granted(address, layout, bytes) = e {
                Some((*address, *layout, *bytes))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert!(grants.len() > 3);
    let table_grants = &grants[1..];
    for pair in table_grants.windows(2) {
        assert_eq!(
            pair[1].2,
            metadata + pair[0].1.size() + pair[1].1.size(),
            "actual old/new backing overlaps until copy completes"
        );
        let grant_at = ledger
            .events
            .iter()
            .position(|e| matches!(e,Event::Granted(address,layout,_) if *address==pair[1].0 && *layout==pair[1].1))
            .unwrap();
        let release_at = ledger
            .events
            .iter()
            .position(|e| matches!(e,Event::Released(address,layout,_) if *address==pair[0].0 && *layout==pair[0].1))
            .unwrap();
        assert!(grant_at < release_at);
    }
    drop(ledger);
    drop(table);
    assert_eq!(host.0.lock().unwrap().bytes, metadata);
    drop(allocator);
    let ledger = host.0.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert!(ledger.live.is_empty());
    assert!(ledger.events.iter().any(
        |e| matches!(e,Event::Released(_,layout,bytes) if layout.size()==metadata && *bytes==0)
    ));
}
#[test]
fn by_copy_child_scratch_vector_every_growth_failure_latches_seven_original_causes() {
    for cause in causes() {
        let host = Arc::new(HeapHost::default());
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        let metadata = allocator.metadata_bytes();
        let mut table = ChildScratchVec::new(Some(&allocator));
        for value in 0..4_usize {
            table.try_push(value).unwrap();
        }
        let next = host.0.lock().unwrap().attempts;
        host.0.lock().unwrap().refusal = Some((next, cause.clone()));
        // Default HostVec capacity for usize is at least four. Fill its actual
        // capacity, without guessing the next allocation ordinal or growth size.
        while let ChildScratchVec::Hosted(values) = &table {
            if values.len() == values.capacity() {
                break;
            }
            let value = values.len();
            table.try_push(value).unwrap();
        }
        let before = table.len();
        assert!(matches!(table.try_push(before),Err(CopyError::Control(actual)) if actual==cause));
        assert_eq!(table.len(), before);
        assert_eq!(allocator.recorded_failure(), Some(cause.clone()));
        let attempts = host.0.lock().unwrap().attempts;
        assert!(matches!(table.try_push(before),Err(CopyError::Control(actual)) if actual==cause));
        assert_eq!(
            host.0.lock().unwrap().attempts,
            attempts,
            "failure journal prevents retry/second host attempt"
        );
        drop(table);
        assert_eq!(host.0.lock().unwrap().bytes, metadata);
        drop(allocator);
        assert_eq!(host.0.lock().unwrap().bytes, 0);
    }
}
#[test]
fn by_copy_child_scratch_vector_original_std_path_has_no_host_or_journal() {
    let mut actual = ChildScratchVec::new(None);
    let mut original = Vec::new();
    for n in 0..100 {
        actual.try_push(n).unwrap();
        original.push(n);
    }
    assert_eq!(&*actual, original.as_slice());
    assert!(matches!(actual, ChildScratchVec::Original(_)));
}
