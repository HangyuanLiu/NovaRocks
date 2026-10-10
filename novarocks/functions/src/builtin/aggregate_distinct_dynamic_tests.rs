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

//! Real host allocations and exact releases, with deterministic refusal.
use super::*;
use crate::{KernelDiagnostic, KernelEvaluationControl};
use allocator_api2::alloc::Allocator;
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Ledger {
    attempts: usize,
    live: Vec<(usize, Layout)>,
    peak: usize,
    bytes: usize,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
    limit: Mutex<Option<usize>>,
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((ordinal, error)) = &*self.refusal.lock().unwrap() {
            if *ordinal == at {
                return Err(error.clone());
            }
        }
        let next = ledger
            .bytes
            .checked_add(layout.size())
            .ok_or(KernelFailure::ResourceExhausted)?;
        if self.limit.lock().unwrap().is_some_and(|limit| next > limit) {
            return Err(KernelFailure::ResourceExhausted);
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes = next;
        ledger.peak = ledger.peak.max(next);
        ledger.live.push((pointer.as_ptr().addr(), layout));
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact live Layout released once");
        ledger.live.swap_remove(at);
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
        ledger.bytes -= layout.size();
    }
}
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("DISTINCT never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("host original")),
        KernelFailure::Internal(KernelDiagnostic::new("host original")),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}

fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next + offset, cause));
}
fn assert_released(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert!(ledger.live.is_empty());
}
#[test]
fn exact_host_allocator_keeps_zero_size_and_grow_refusal_facts() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let metadata = allocator.metadata_bytes();
    let constructor_attempts = host.ledger.lock().unwrap().attempts;
    let funded = host.ledger.lock().unwrap().bytes + 8;
    *host.limit.lock().unwrap() = Some(funded);
    let empty = Layout::from_size_align(0, 64).unwrap();
    let pointer = allocator.allocate(empty).unwrap();
    assert_eq!(pointer.as_ptr().cast::<u8>().addr() % 64, 0);
    unsafe { allocator.deallocate(pointer.cast(), empty) };
    assert_eq!(host.ledger.lock().unwrap().attempts, constructor_attempts);
    let old = Layout::from_size_align(8, 8).unwrap();
    let larger = Layout::from_size_align(16, 8).unwrap();
    let pointer = allocator.allocate(old).unwrap().cast::<u8>();
    unsafe { pointer.as_ptr().write(71) };
    assert!(unsafe { allocator.grow(pointer, old, larger) }.is_err());
    assert_eq!(allocator.take_failure(), KernelFailure::ResourceExhausted);
    assert_eq!(unsafe { pointer.as_ptr().read() }, 71);
    assert_eq!(host.ledger.lock().unwrap().bytes, metadata + 8);
    unsafe { allocator.deallocate(pointer, old) };
    assert_eq!(host.ledger.lock().unwrap().bytes, metadata);
    assert_eq!(host.ledger.lock().unwrap().live.len(), 1);
    drop(allocator);
    assert_released(&host);
}
#[test]
fn actual_container_allocator_restores_every_typed_host_failure() {
    for cause in causes() {
        let host = Arc::new(Host::default());
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        let metadata = allocator.metadata_bytes();
        let initial = host.ledger.lock().unwrap().attempts;
        arm_refusal(&host, 0, cause.clone());
        let mut bytes = HostVec::<u8, _>::new_in(allocator);
        assert!(bytes.try_reserve_exact(8).is_err());
        assert_eq!(bytes.allocator().take_failure(), cause);
        assert_eq!(host.ledger.lock().unwrap().bytes, metadata);
        assert_eq!(host.ledger.lock().unwrap().attempts, initial + 1);
        drop(bytes);
        assert_released(&host);
    }
}
#[test]
fn rejected_key_keeps_real_grown_table_until_drop_and_retained_matches_layouts() {
    for refusal in [0, 1] {
        let host = Arc::new(Host::default());
        let mut state = NumericDistinctState::new(host.clone()).unwrap();
        let metadata = state.allocator.metadata_bytes();
        arm_refusal(&host, refusal, KernelFailure::ResourceExhausted);
        let mut work = EvaluationCheckpoints::new(&Control);
        let error = state.insert(&11i64.to_le_bytes(), &mut work).unwrap_err();
        assert!(matches!(
            error,
            DistinctComputationError::Kernel(KernelFailure::ResourceExhausted)
        ));
        assert_eq!(state.len(), 0);
        assert_eq!(state.retained_bytes(), host.ledger.lock().unwrap().bytes);
        if refusal == 0 {
            assert_eq!(state.retained_bytes(), metadata);
        } else {
            assert!(state.retained_bytes() > metadata);
        }
        drop(state);
        assert_released(&host);
    }
}
#[test]
fn duplicates_do_not_allocate_and_codec_temp_is_released() {
    use super::super::aggregate_distinct_numeric as core;
    let host = Arc::new(Host::default());
    let mut state = NumericDistinctState::new(host.clone()).unwrap();
    let mut work = EvaluationCheckpoints::new(&Control);
    for key in [1i64, 2, 2, 3] {
        state.insert(&key.to_le_bytes(), &mut work).unwrap();
    }
    let attempts = host.ledger.lock().unwrap().attempts;
    state.insert(&2i64.to_le_bytes(), &mut work).unwrap();
    assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
    assert_eq!(state.retained_bytes(), host.ledger.lock().unwrap().bytes);
    let stable = state.retained_bytes();
    let mut output = state.buffer();
    core::serialize_set_into(&state, &mut output, &mut work).unwrap();
    assert_eq!(output.bytes.len(), 40);
    assert_eq!(
        host.ledger.lock().unwrap().bytes,
        stable + output.bytes.capacity()
    );
    let mut incoming = NumericDistinctState::new(host.clone()).unwrap();
    core::visit_serialized_keys_with_work(&output.bytes, 8, &mut work, |bytes, work| {
        incoming.insert(bytes, work)
    })
    .unwrap();
    assert_eq!(incoming.len(), 3);
    assert_eq!(
        incoming.retained_bytes() + stable + output.bytes.capacity(),
        host.ledger.lock().unwrap().bytes
    );
    drop(incoming);
    let mut invalid = output.bytes.clone();
    invalid[4..8].copy_from_slice(&7u32.to_le_bytes());
    let mut mutation_count = 0;
    assert!(
        core::visit_serialized_keys_with_work(&invalid, 8, &mut work, |_, _| {
            mutation_count += 1;
            Ok(())
        })
        .is_err()
    );
    assert_eq!(mutation_count, 0);
    drop(invalid);
    drop(output);
    assert_eq!(host.ledger.lock().unwrap().bytes, stable);
    drop(state);
    assert_released(&host);
}
#[test]
fn serialization_restores_typed_temporary_allocation_refusal() {
    use super::super::aggregate_distinct_numeric as core;
    for cause in causes() {
        let host = Arc::new(Host::default());
        let mut state = NumericDistinctState::new(host.clone()).unwrap();
        let mut work = EvaluationCheckpoints::new(&Control);
        state.insert(&11i64.to_le_bytes(), &mut work).unwrap();
        let stable = state.retained_bytes();
        arm_refusal(&host, 0, cause.clone());
        let mut buffer = state.buffer();
        let error = core::serialize_set_into(&state, &mut buffer, &mut work).unwrap_err();
        assert!(matches!(error,DistinctComputationError::Kernel(error) if error==cause));
        assert_eq!(host.ledger.lock().unwrap().bytes, stable);
        drop(buffer);
        drop(state);
        assert_released(&host);
    }
}
#[test]
fn successful_resize_charges_old_and_replacement_until_actual_release() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let metadata = allocator.metadata_bytes();
    let old = Layout::from_size_align(8, 8).unwrap();
    let larger = Layout::from_size_align(16, 8).unwrap();
    let small = Layout::from_size_align(4, 8).unwrap();
    let pointer = allocator.allocate(old).unwrap().cast::<u8>();
    unsafe { pointer.as_ptr().write(87) };
    let pointer = unsafe { allocator.grow(pointer, old, larger) }
        .unwrap()
        .cast::<u8>();
    assert_eq!(unsafe { pointer.as_ptr().read() }, 87);
    {
        let ledger = host.ledger.lock().unwrap();
        assert_eq!(ledger.bytes, metadata + 16);
        assert_eq!(ledger.peak, metadata + 24);
        assert_eq!(ledger.live.len(), 2);
    }
    let pointer = unsafe { allocator.shrink(pointer, larger, small) }
        .unwrap()
        .cast::<u8>();
    assert_eq!(unsafe { pointer.as_ptr().read() }, 87);
    assert_eq!(host.ledger.lock().unwrap().bytes, metadata + 4);
    unsafe { allocator.deallocate(pointer, small) };
    assert_eq!(host.ledger.lock().unwrap().bytes, metadata);
    drop(allocator);
    assert_released(&host);
}
#[test]
fn numeric_distinct_actual_metadata_constructor_refusal_is_typed_and_never_published() {
    for cause in causes() {
        let host = Arc::new(Host::default());
        arm_refusal(&host, 0, cause.clone());
        assert!(matches!(NumericDistinctState::new(host.clone()),Err(e) if e==cause));
        assert_eq!(host.ledger.lock().unwrap().attempts, 1);
        assert_released(&host);
    }
}
