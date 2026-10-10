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

//! Shared original HLL tests that do not depend on an installed pure owner.
use super::*;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::{KernelControlObservation, internal, invalid};
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after primary refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("NDV never waits")
    }
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            if *stop == at {
                return Err(cause.clone());
            }
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        let block = (pointer.as_ptr().addr(), layout);
        if ledger.metadata.is_none() {
            ledger.metadata = Some(block);
        }
        ledger.live.push(block);
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next + offset, cause));
}
fn assert_metadata_only(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    let metadata = ledger
        .metadata
        .expect("successful constructor allocated metadata");
    assert_eq!(ledger.live.as_slice(), &[metadata]);
    assert_eq!(ledger.bytes, metadata.1.size());
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host original"),
        internal("host original"),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}

fn state(host: Arc<Host>) -> HllRawState<HostAggregateAllocator> {
    HllRawState::new(HostAggregateAllocator::try_new(host).unwrap())
}
fn assert_released(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert!(ledger.live.is_empty());
}
fn observe<T>(
    control: &Control,
    f: impl FnOnce(&mut HllWork<'_, '_>) -> Result<T, HllError>,
) -> Result<T, HllError> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut checkpoints = EvaluationCheckpoints::new(&observation);
    let result = f(&mut HllWork::new(Some(&mut checkpoints)));
    if matches!(result, Err(HllError::Kernel(_))) {
        return result;
    }
    checkpoints.finish()?;
    observation.checkpoint(0)?;
    result
}
#[test]
fn original_hll_legacy_layout_float_canonicalization_and_sparse_partial_mutation() {
    #[derive(Default)]
    struct OriginalLayout {
        has_value: bool,
        registers: Option<Box<[u8; HLL_REGISTERS_COUNT]>>,
    }
    assert_eq!(
        std::mem::size_of::<OriginalLayout>(),
        std::mem::size_of::<HllRawState>()
    );
    assert_eq!(
        std::mem::align_of::<OriginalLayout>(),
        std::mem::align_of::<HllRawState>()
    );
    let _ = OriginalLayout::default().has_value;
    let _ = OriginalLayout::default().registers;
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        0.,
        -0.,
        f64::from_bits(0x7ff8000000000001),
        f64::from_bits(0xfff8000000000042),
    ]));
    let mut work = HllWork::new(None);
    assert_eq!(
        hash_array_value_for_hll_observed(&values, 0, &mut work).unwrap(),
        hash_array_value_for_hll_observed(&values, 1, &mut work).unwrap()
    );
    assert_eq!(
        hash_array_value_for_hll_observed(&values, 2, &mut work).unwrap(),
        hash_array_value_for_hll_observed(&values, 3, &mut work).unwrap()
    );
    let payload = [2, 2, 0, 0, 0, 1, 0, 7, 255, 255, 9];
    let mut actual = HllRawState {
        has_value: true,
        ..HllRawState::default()
    };
    merge_hll_bytes(&mut actual, &payload, &mut work).unwrap();
    let mut expected = HllRawState {
        has_value: true,
        ..HllRawState::default()
    };
    ensure_registers(&mut expected, &mut work).unwrap()[1] = 7;
    update_state_register_from_hash(
        &mut expected,
        hash_bytes_for_hll_observed(&payload, &mut work).unwrap(),
        &mut work,
    )
    .unwrap();
    assert_eq!(
        serialize_hll_state(&actual, &mut work).unwrap(),
        serialize_hll_state(&expected, &mut work).unwrap()
    );
}
#[test]
fn original_hll_generic_all_null_and_empty_states_keep_full_error_domain() {
    let mut work = HllWork::new(None);
    let unsigned: ArrayRef = Arc::new(UInt32Array::from(vec![None]));
    assert_eq!(
        hash_array_value_for_hll_observed(&unsigned, 0, &mut work).unwrap(),
        None
    );
    let fields = arrow_schema::Fields::from(
        (0..96)
            .map(|n| {
                arrow_schema::Field::new(
                    format!("original_field_{n}"),
                    arrow_schema::DataType::Utf8,
                    true,
                )
            })
            .collect::<Vec<_>>(),
    );
    let arrays = (0..96)
        .map(|_| Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef)
        .collect::<Vec<_>>();
    let nulls = Some(arrow_buffer::NullBuffer::new_null(1));
    let all_null: ArrayRef = Arc::new(StructArray::new(fields.clone(), arrays.clone(), nulls));
    assert_eq!(
        hash_array_value_for_hll_observed(&all_null, 0, &mut work).unwrap(),
        None
    );
    let actual: ArrayRef = Arc::new(StructArray::new(fields, arrays, None));
    let expected = format!(
        "hll_raw does not support input type {:?}",
        actual.data_type()
    );
    assert!(expected.len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    assert!(
        matches!(hash_array_value_for_hll_observed(&actual,0,&mut work),Err(HllError::Legacy(text)) if text==expected)
    );
    let empty = HllRawState::default();
    assert_eq!(estimate_cardinality(&empty, &mut work).unwrap(), 0);
    assert_eq!(serialize_hll_state(&empty, &mut work).unwrap(), None);
}
#[test]
fn original_hll_real_host_metadata_and_register_allocation_preserve_every_cause() {
    for cause in causes() {
        let host = Arc::new(Host::default());
        arm_refusal(&host, 0, cause.clone());
        assert!(matches!(HostAggregateAllocator::try_new(host.clone()),Err(error) if error==cause));
        assert_released(&host);
        let host = Arc::new(Host::default());
        let mut state = state(host.clone());
        assert_metadata_only(&host);
        arm_refusal(&host, 0, cause.clone());
        assert!(
            matches!(update_state_register_from_hash(&mut state,1,&mut HllWork::new(None)),Err(HllError::Kernel(error)) if error==cause)
        );
        assert!(state.registers.is_none());
        assert_metadata_only(&host);
        drop(state);
        assert_released(&host);
    }
}
#[test]
fn original_hll_shared_hash_codec_estimate_serialization_every_callback_has_no_tail() {
    for operation in 0..4 {
        let host = Arc::new(Host::default());
        let mut successful = state(host.clone());
        successful.has_value = true;
        if operation >= 2 {
            update_state_register_from_hash(&mut successful, 1, &mut HllWork::new(None)).unwrap();
        }
        let control = Control::default();
        let exercise = |state: &mut HllRawState<HostAggregateAllocator>,
                        work: &mut HllWork<'_, '_>|
         -> Result<(), HllError> {
            match operation {
                0 => {
                    let bytes = vec![255u8; 4097];
                    let hash = hash_bytes_for_hll_observed(&bytes, work)?;
                    update_state_register_from_hash(state, hash, work)?;
                }
                1 => {
                    let mut payload = vec![HLL_DATA_FULL];
                    payload.extend(vec![1; HLL_REGISTERS_COUNT]);
                    merge_hll_bytes(state, &payload, work)?;
                }
                2 => {
                    estimate_cardinality(state, work)?;
                }
                3 => {
                    serialize_hll_state(state, work)?;
                }
                _ => unreachable!(),
            }
            Ok(())
        };
        observe(&control, |work| exercise(&mut successful, work)).unwrap();
        let steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        drop(successful);
        assert_released(&host);
        for cause in causes() {
            for stop in 0..steps {
                let host = Arc::new(Host::default());
                let mut actual = state(host.clone());
                actual.has_value = true;
                if operation >= 2 {
                    update_state_register_from_hash(&mut actual, 1, &mut HllWork::new(None))
                        .unwrap();
                }
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(observe(&control,|work|exercise(&mut actual,work)),Err(HllError::Kernel(error)) if error==cause)
                );
                assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
                drop(actual);
                assert_released(&host);
            }
        }
    }
}
