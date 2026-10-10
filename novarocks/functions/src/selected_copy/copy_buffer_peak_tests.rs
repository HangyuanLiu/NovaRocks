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

use super::*;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueAllocationHost, OpaqueRetainedCharge};
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use arrow_array::{Array, ArrayRef, Int32Array, builder::Int32Builder};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

#[test]
fn array_cast_raw_vec_growth_bound_covers_actual_capacity_and_initial_reservation() {
    for initial in [0, 1, 4, 1024] {
        for maximum in [0, 1, 7, 1024, 1025, 2049] {
            let bound = CopyBufferPeak::vec::<u32>(initial, maximum).unwrap();
            let mut values = Vec::<u32>::with_capacity(initial);
            for value in 0..maximum {
                let previous = values.capacity() * size_of::<u32>();
                values.push(value as u32);
                let current = values.capacity() * size_of::<u32>();
                assert!(current <= bound.retained_upper());
                if current != previous {
                    assert!(previous + current <= bound.transient_upper());
                }
            }
            assert!(values.capacity() * size_of::<u32>() <= bound.retained_upper());
        }
    }
    assert_eq!(
        CopyBufferPeak::vec::<()>(usize::MAX, usize::MAX)
            .unwrap()
            .transient_upper(),
        0
    );
    assert!(matches!(
        CopyBufferPeak::vec::<u64>(0, usize::MAX),
        Err(CopyError::Extent)
    ));
}
#[test]
fn array_cast_string_growth_bound_keeps_live_replaced_buffer_and_full_text() {
    for initial in [0, 8, 1024] {
        let bytes = "\u{1f642}\0α".repeat(321);
        let bound = CopyBufferPeak::string(initial, bytes.len()).unwrap();
        let mut value = String::with_capacity(initial);
        for c in bytes.chars() {
            let previous = value.capacity();
            value.push(c);
            assert!(value.capacity() <= bound.retained_upper());
            if previous != value.capacity() {
                assert!(previous + value.capacity() <= bound.transient_upper());
            }
        }
        assert_eq!(value, bytes);
    }
}
#[derive(Default)]
struct Ledger {
    opaque: usize,
    requests: Vec<usize>,
    physical: Vec<(usize, Layout)>,
    refusal: Option<KernelFailure>,
    limit: usize,
}
struct Host(Mutex<Ledger>);
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        // This is a real physical block with exact layout, not an opaque grant.
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        self.0
            .lock()
            .unwrap()
            .physical
            .push((pointer.as_ptr().addr(), layout));
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.0.lock().unwrap();
        let at = ledger
            .physical
            .iter()
            .position(|(address, original)| {
                *address == pointer.as_ptr().addr() && *original == layout
            })
            .expect("same physical block released once");
        ledger.physical.swap_remove(at);
        drop(ledger);
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut ledger = self.0.lock().unwrap();
        ledger.requests.push(bytes);
        if let Some(cause) = &ledger.refusal {
            return Err(cause.clone());
        }
        let next = ledger
            .opaque
            .checked_add(bytes)
            .ok_or(KernelFailure::ResourceExhausted)?;
        if next > ledger.limit {
            return Err(KernelFailure::ResourceExhausted);
        }
        ledger.opaque = next;
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let mut ledger = self.0.lock().unwrap();
        ledger.opaque = ledger
            .opaque
            .checked_sub(bytes)
            .expect("opaque admission released once");
    }
}
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("builder probe does not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("CAST builder invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("CAST builder internal")),
        KernelFailure::Operational(KernelDiagnostic::new("CAST builder operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn primitive_peak(rows: usize) -> usize {
    let values = CopyBufferPeak::vec::<i32>(1024, rows).unwrap();
    let bitmap = CopyBufferPeak::mutable(1024usize.div_ceil(8), rows.div_ceil(8)).unwrap();
    values.transient_upper()
        + bitmap.transient_upper()
        + crate::arrow_result_custody::custody_type_metadata_upper_bound(1).unwrap()
}
#[test]
fn array_cast_builder_reservation_refusal_precedes_every_original_allocation() {
    let peak = primitive_peak(2049);
    for cause in causes() {
        let host = Arc::new(Host(Mutex::new(Ledger {
            limit: usize::MAX,
            refusal: Some(cause.clone()),
            ..Ledger::default()
        })));
        let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
        let mut entered = false;
        let result = (|| {
            let _reservation = charge.reserve_operation(peak)?;
            entered = true;
            let _builder = Int32Builder::new();
            Ok::<_, KernelFailure>(())
        })();
        assert_eq!(result, Err(cause));
        assert!(!entered);
        drop(charge);
        let ledger = host.0.lock().unwrap();
        assert_eq!(ledger.requests, [peak]);
        assert_eq!(ledger.opaque, 0);
        assert!(ledger.physical.is_empty());
    }
    let host = Arc::new(Host(Mutex::new(Ledger {
        limit: peak - 1,
        ..Ledger::default()
    })));
    let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
    assert!(matches!(
        charge.reserve_operation(peak),
        Err(KernelFailure::ResourceExhausted)
    ));
    drop(charge);
    assert_eq!(host.0.lock().unwrap().opaque, 0);
}
#[test]
fn array_cast_builder_actual_buffer_custody_releases_only_after_derived_buffer_last_drop() {
    let host = Arc::new(Host(Mutex::new(Ledger {
        limit: usize::MAX,
        ..Ledger::default()
    })));
    let host_trait: Arc<dyn AggregateStateAllocator> = host.clone();
    let charge = OpaqueRetainedCharge::try_new(host_trait.clone()).unwrap();
    let mut reservation = charge.reserve_operation(primitive_peak(2049)).unwrap();
    // The original pinned builder runs only AFTER its actual operation grant.
    let mut builder = Int32Builder::new();
    for row in 0..2049 {
        if row % 7 == 0 {
            builder.append_null();
        } else {
            builder.append_value(row);
        }
    }
    let original = Arc::new(builder.finish()) as ArrayRef;
    let allocator = HostAggregateAllocator::try_new(host_trait).unwrap();
    let control = Control;
    let mut work = EvaluationCheckpoints::new(&control);
    let retained = crate::arrow_result_custody::retain_result_backing(
        original,
        charge,
        &mut reservation,
        allocator,
        &mut work,
    )
    .unwrap();
    drop(reservation);
    let result = retained.values;
    let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(array.is_null(0));
    assert_eq!(array.value(1), 1);
    let borrowed = array.values().inner().clone();
    drop(result);
    assert!(host.0.lock().unwrap().opaque > 0);
    drop(borrowed);
    let ledger = host.0.lock().unwrap();
    assert_eq!(ledger.opaque, 0);
    assert!(ledger.physical.is_empty());
}
