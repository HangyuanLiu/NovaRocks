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
use crate::builtin::aggregate_mann_whitney_core::MannFailure;
use allocator_api2::alloc::{AllocError, Allocator, Global};
use arrow_array::{NullArray, UInt64Array};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Default)]
struct Storage(Arc<Mutex<Vec<usize>>>);
// SAFETY: test storage forwards exact blocks and layouts to Global.
unsafe impl Allocator for Storage {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.0.lock().unwrap().push(layout.size());
        Global.allocate(layout)
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(pointer, layout) };
    }
}
impl ScalarStateAllocator for Storage {
    fn scalar_allocation_error<E: crate::allocation_failure::AllocationRefusalSource>(
        &self,
        _: &str,
        _source: E,
    ) -> crate::KernelFailure {
        KernelFailure::ResourceExhausted
    }
}
#[derive(Debug, PartialEq)]
enum Event {
    Static(&'static str, bool),
    Int(DataType, bool),
    Alternative(String),
    Read(DataType, usize),
    Scratch(MannStateExtents, MannStateExtents),
}
#[derive(Default)]
struct Observe {
    events: Vec<Event>,
    refusal: Option<(usize, KernelFailure)>,
}
impl MannResources for Observe {
    fn before(
        &mut self,
        event: MannResourceEvent<'_>,
        _: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        if let Some((ordinal, _)) = &self.refusal {
            assert!(self.events.len() <= *ordinal, "event after refusal");
        }
        self.events.push(match event {
            MannResourceEvent::StaticDiagnostic {
                message,
                treatment_replacement,
            } => Event::Static(message, treatment_replacement),
            MannResourceEvent::IntSetupDiagnostic {
                data_type,
                treatment_replacement,
            } => Event::Int(data_type.clone(), treatment_replacement),
            MannResourceEvent::FirstAlternative { source } => Event::Alternative(source.to_owned()),
            MannResourceEvent::ScalarRead { array, row } => {
                Event::Read(array.data_type().clone(), row)
            }
            MannResourceEvent::MergeScratch { left, right } => Event::Scratch(left, right),
        });
        match &self.refusal {
            Some((ordinal, cause)) if self.events.len() == ordinal + 1 => Err(cause.clone()),
            _ => Ok(()),
        }
    }
}
fn domain_message<T>(result: Result<T, MannFailure>) -> String {
    match result {
        Err(MannFailure::Value(source)) => source.to_string(),
        Err(MannFailure::Kernel(source)) => {
            assert_eq!(
                novarocks_failure_contract::FailureProjection::failure_fact(&source).class(),
                novarocks_failure_contract::FailureClass::Invariant
            );
            source.to_string()
        }
        _ => panic!("expected actual domain refusal"),
    }
}
fn input<'a>(
    args: &'a [ArrayRef],
    events: &mut Observe,
) -> Result<MannWhitneyInput<'a>, MannFailure> {
    MannWhitneyInput::try_new_channels_with_resources(
        std::array::from_fn(|i| args.get(i)),
        args.len(),
        &mut ScalarWork::new(None),
        events,
    )
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("input refusal")),
        KernelFailure::Internal(crate::KernelDiagnostic::new("internal refusal")),
        KernelFailure::Operational(crate::KernelDiagnostic::new("operational refusal")),
        KernelFailure::InstanceFailed,
    ]
}
fn state(alternative: TestingAlternative, count: usize) -> MannWhitneyState<Storage> {
    let mut state = MannWhitneyState::new(Storage::default());
    state.init(alternative, 1);
    for i in 0..count {
        state.update((count - i) as f64, i % 2 == 1).unwrap();
    }
    state
}

#[test]
fn mann_whitney_resources_setup_events_precede_original_errors_even_without_rows() {
    let mut events = Observe::default();
    assert_eq!(
        domain_message(input(&[], &mut events)),
        "mann_whitney_u_test expects 2 to 4 arguments"
    );
    assert_eq!(
        events.events,
        vec![Event::Static(
            "mann_whitney_u_test expects 2 to 4 arguments",
            false
        )]
    );
    let args: [ArrayRef; 4] = [
        Arc::new(NullArray::new(0)),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(NullArray::new(0)),
        Arc::new(NullArray::new(0)),
    ];
    let mut events = Observe::default();
    assert_eq!(
        domain_message(input(&args, &mut events)),
        "mann_whitney_u_test expects boolean or integer treatment; got Utf8"
    );
    assert_eq!(events.events, vec![Event::Int(DataType::Utf8, true)]);
    for cause in causes() {
        let mut events = Observe {
            refusal: Some((0, cause.clone())),
            ..Observe::default()
        };
        assert!(
            matches!(input(&args, &mut events), Err(MannFailure::Kernel(actual)) if actual == cause)
        );
        assert_eq!(events.events.len(), 1);
    }
}

/// A lawful Arrow wrapper retains its real type/buffers but is not the
/// standard concrete carrier. This reaches the original downcast rejection.
#[derive(Debug)]
struct Wrapped(ArrayRef);
// SAFETY: every Arrow data, type, length, offset and validity operation
// delegates to the same valid underlying array; as_any exposes this wrapper.
unsafe impl Array for Wrapped {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_data(&self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn into_data(self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length)))
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn offset(&self) -> usize {
        self.0.offset()
    }
    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.0.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.0.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}
#[test]
fn mann_whitney_resources_static_integer_events_keep_treatment_context_and_typed_cause() {
    let carriers: [(ArrayRef, &str); 4] = [
        (
            Arc::new(Int64Array::from(vec![1])),
            "failed to downcast to Int64Array",
        ),
        (
            Arc::new(Int32Array::from(vec![1])),
            "failed to downcast to Int32Array",
        ),
        (
            Arc::new(Int16Array::from(vec![1])),
            "failed to downcast to Int16Array",
        ),
        (
            Arc::new(Int8Array::from(vec![1])),
            "failed to downcast to Int8Array",
        ),
    ];
    for (carrier, message) in carriers {
        let array: ArrayRef = Arc::new(Wrapped(carrier));
        for replacement in [false, true] {
            let mut events = Observe::default();
            let result = if replacement {
                TreatmentArrayView::new_with_resources(
                    &array,
                    &mut ScalarWork::new(None),
                    &mut events,
                )
                .map(|_| ())
            } else {
                IntArrayView::new_with_resources(
                    &array,
                    false,
                    &mut ScalarWork::new(None),
                    &mut events,
                )
                .map(|_| ())
            };
            assert_eq!(domain_message(result), message);
            assert_eq!(
                events.events,
                vec![Event::Static(
                    match array.data_type() {
                        DataType::Int64 => "failed to downcast to Int64Array",
                        DataType::Int32 => "failed to downcast to Int32Array",
                        DataType::Int16 => "failed to downcast to Int16Array",
                        _ => "failed to downcast to Int8Array",
                    },
                    replacement
                )]
            );
            for cause in causes() {
                let mut events = Observe {
                    refusal: Some((0, cause.clone())),
                    ..Observe::default()
                };
                let result = TreatmentArrayView::new_with_resources(
                    &array,
                    &mut ScalarWork::new(None),
                    &mut events,
                );
                assert!(matches!(result, Err(MannFailure::Kernel(actual)) if actual == cause));
            }
        }
    }
    let boolean: ArrayRef = Arc::new(Wrapped(Arc::new(BooleanArray::from(vec![true]))));
    let mut events = Observe::default();
    assert_eq!(
        domain_message(TreatmentArrayView::new_with_resources(
            &boolean,
            &mut ScalarWork::new(None),
            &mut events
        )),
        "failed to downcast to BooleanArray"
    );
    assert_eq!(
        events.events,
        vec![Event::Static("failed to downcast to BooleanArray", false)]
    );
}

#[test]
fn mann_whitney_resources_row_frontiers_preserve_first_parameters_and_null_skips() {
    for (alternative, continuity, treatment, expected, event_count, contract_failure) in [
        (
            "invalid",
            -1,
            None,
            Some("Logical Error: invalid alternative `invalid`."),
            1,
            false,
        ),
        (
            "LESS",
            -1,
            None,
            Some("Logical Error: continuity_correction must be non-negative."),
            2,
            false,
        ),
        ("LESS", 0, None, None, 1, false),
        (
            "LESS",
            0,
            Some(true),
            Some("unsupported scalar type: UInt64"),
            2,
            true,
        ),
    ] {
        let args: [ArrayRef; 4] = [
            Arc::new(UInt64Array::from(vec![None])),
            Arc::new(BooleanArray::from(vec![treatment])),
            Arc::new(StringArray::from(vec![alternative])),
            Arc::new(Int64Array::from(vec![continuity])),
        ];
        let mut events = Observe::default();
        let input = input(&args, &mut events).unwrap();
        let mut state = MannWhitneyState::new(Storage::default());
        let result = input.update_row_with_resources(
            &mut state,
            [0; 4],
            &mut ScalarWork::new(None),
            &mut events,
        );
        match expected {
            Some(message) if contract_failure => {
                let Err(MannFailure::Kernel(cause)) = result else {
                    panic!("reader contract cause")
                };
                assert_eq!(
                    novarocks_failure_contract::FailureProjection::failure_fact(&cause).class(),
                    novarocks_failure_contract::FailureClass::Invariant
                );
                assert_eq!(cause.to_string(), message);
            }
            Some(message) => assert_eq!(domain_message(result), message),
            None => result.unwrap(),
        }
        assert_eq!(events.events.len(), event_count);
        assert!(
            matches!(events.events[0], Event::Alternative(ref source) if source == alternative)
        );
        assert_eq!(
            state.extents().initialized,
            alternative == "LESS" && continuity >= 0
        );
        if treatment.is_none() {
            assert!(
                !events
                    .events
                    .iter()
                    .any(|event| matches!(event, Event::Read(..)))
            );
        }
        assert!(events.events.len() <= 3);
    }
    let args: [ArrayRef; 4] = [
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(BooleanArray::from(vec![false, true])),
        Arc::new(StringArray::from(vec!["less", "invalid"])),
        Arc::new(Int64Array::from(vec![0, -1])),
    ];
    let mut events = Observe::default();
    let input = input(&args, &mut events).unwrap();
    let mut state = MannWhitneyState::new(Storage::default());
    input
        .update_row_with_resources(&mut state, [0; 4], &mut ScalarWork::new(None), &mut events)
        .unwrap();
    events.events.clear();
    input
        .update_row_with_resources(&mut state, [1; 4], &mut ScalarWork::new(None), &mut events)
        .unwrap();
    assert_eq!(events.events, vec![Event::Read(DataType::Int64, 1)]);
}

#[test]
fn mann_whitney_resources_each_row_frontier_refusal_keeps_all_seven_causes() {
    let args: [ArrayRef; 3] = [
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(StringArray::from(vec!["LESS"])),
    ];
    for at in [0, 1] {
        for cause in causes() {
            let mut events = Observe {
                refusal: Some((at, cause.clone())),
                ..Observe::default()
            };
            let input = input(&args, &mut events).unwrap();
            let mut state = MannWhitneyState::new(Storage::default());
            assert!(
                matches!(input.update_row_with_resources(&mut state, [0;4], &mut ScalarWork::new(None), &mut events), Err(MannFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(events.events.len(), at + 1);
            assert!(!state.has_samples());
            assert_eq!(state.extents().initialized, at == 1);
        }
    }
}

#[test]
fn mann_whitney_resources_merge_frontier_follows_compatibility_and_precedes_sort() {
    let mut left = state(TestingAlternative::Less, 6);
    let right = state(TestingAlternative::Greater, 6);
    let before = left.extents();
    let mut events = Observe::default();
    assert_eq!(
        domain_message(left.merge_with_resources(right, &mut ScalarWork::new(None), &mut events)),
        "mann_whitney_u_test state parameters mismatch"
    );
    assert_eq!(
        events.events,
        vec![Event::Static(
            "mann_whitney_u_test state parameters mismatch",
            false
        )]
    );
    assert_eq!(left.extents(), before);
    for cause in causes() {
        let mut left = state(TestingAlternative::Less, 6);
        let right = state(TestingAlternative::Less, 6);
        let before = left.extents();
        let other = right.extents();
        let mut events = Observe {
            refusal: Some((0, cause.clone())),
            ..Observe::default()
        };
        assert!(
            matches!(left.merge_with_resources(right, &mut ScalarWork::new(None), &mut events), Err(MannFailure::Kernel(actual)) if actual == cause)
        );
        assert_eq!(events.events, vec![Event::Scratch(before, other)]);
        assert_eq!(left.extents(), before);
        assert!(!left.sorted);
    }
    let mut left = MannWhitneyState::new(Storage::default());
    let right = state(TestingAlternative::Less, 6);
    let mut events = Observe::default();
    left.merge_with_resources(right, &mut ScalarWork::new(None), &mut events)
        .unwrap();
    assert!(events.events.is_empty());
    assert!(left.has_samples());
}

#[test]
fn mann_whitney_resources_decode_validates_payload_before_sample_allocation() {
    let mut data = vec![1];
    data.extend_from_slice(&1i64.to_le_bytes());
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes());
    let allocator = Storage::default();
    let mut events = Observe::default();
    assert_eq!(
        domain_message(MannWhitneyState::deserialize_in_with_resources(
            &data,
            allocator.clone(),
            &mut ScalarWork::new(None),
            &mut events
        )),
        "mann_whitney_u_test deserialize: buffer too short"
    );
    assert!(allocator.0.lock().unwrap().is_empty());
    assert_eq!(
        events.events,
        vec![Event::Static(
            "mann_whitney_u_test deserialize: buffer too short",
            false
        )]
    );
    for cause in causes() {
        let allocator = Storage::default();
        let mut events = Observe {
            refusal: Some((0, cause.clone())),
            ..Observe::default()
        };
        assert!(
            matches!(MannWhitneyState::deserialize_in_with_resources(&data, allocator.clone(), &mut ScalarWork::new(None), &mut events), Err(MannFailure::Kernel(actual)) if actual == cause)
        );
        assert!(allocator.0.lock().unwrap().is_empty());
    }
    let allocator = Storage::default();
    let mut events = Observe::default();
    assert_eq!(
        domain_message(MannWhitneyState::deserialize_in_with_resources(
            &[],
            allocator.clone(),
            &mut ScalarWork::new(None),
            &mut events
        )),
        "mann_whitney_u_test deserialize: buffer too short"
    );
    assert!(allocator.0.lock().unwrap().is_empty());
}

#[test]
fn mann_whitney_codec_exact_storage_matches_actual_producer() {
    use super::super::aggregate_mann_whitney_resources::codec_facts;
    use crate::arrow_result_custody::RequestUpper;
    for count in [0, 1, 2, 3, 7, 29, 63, 127, 220, 513, 1024] {
        let mut state = state(TestingAlternative::TwoSided, count);
        let actual = state.serialize().unwrap();
        let (facts, output) = codec_facts(count).unwrap();
        assert_eq!(output, actual.len());
        assert_eq!(facts.retained, actual.capacity());
        assert_eq!(facts.peak, actual.capacity());
        assert_eq!(
            facts.requests,
            RequestUpper {
                bytes: actual.capacity(),
                requests: 1
            }
        );
    }
    assert_eq!(
        codec_facts(usize::MAX),
        Err(crate::scalar_resources::ScalarResourceError::Arithmetic)
    );
}

#[test]
fn mann_whitney_untrusted_wire_counts_never_allocate_before_validation() {
    for lengths in [[1u32, 0], [0, u32::MAX], [u32::MAX, u32::MAX]] {
        let mut data = vec![1];
        data.extend_from_slice(&1i64.to_le_bytes());
        for count in lengths {
            data.extend_from_slice(&count.to_le_bytes());
        }
        let allocator = Storage::default();
        let failure = MannWhitneyState::deserialize_in(&data, allocator.clone()).unwrap_err();
        assert!(matches!(
            failure,
            MannFailure::Value(MannValueFailure::TruncatedState)
        ));
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(
            &failure.into_kernel_failure(),
        );
        assert_eq!(fact.class(), novarocks_failure_contract::FailureClass::Data);
        assert_eq!(
            fact,
            novarocks_failure_contract::FailureProjection::failure_fact(
                &crate::DataFailureReason::InvalidEncoding
            )
        );
        assert!(allocator.0.lock().unwrap().is_empty());
    }
}

#[test]
fn mann_whitney_wire_count_boundary_keeps_exact_fact_and_original_source() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let ([left, right], bytes) = wire_geometry([u32::MAX as usize, 0]).unwrap();
    assert_eq!([left, right], [u32::MAX, 0]);
    assert_eq!(bytes as u128, 17 + 8 * u32::MAX as u128);
    let required = u32::MAX as usize + 1;
    let cause = wire_geometry([0, required]).unwrap_err();
    assert_eq!(cause.failure_fact().class(), FailureClass::Representation);
    let extent = cause.failure_fact().representation().unwrap();
    assert_eq!(extent.required, Some(required as u64));
    assert_eq!(extent.extent_limit, Some(u32::MAX as u64));
    assert_eq!(extent.representable_prefix, None);
    let mut source: &(dyn std::error::Error + 'static) = &cause;
    while !source.is::<std::num::TryFromIntError>() {
        source = source
            .source()
            .expect("original integer conversion failure retained");
    }
    let cause = storage_count::<(f64, usize)>(usize::MAX as u128).unwrap_err();
    assert_eq!(cause.failure_fact().class(), FailureClass::Representation);
    assert_eq!(
        cause.failure_fact().representation().unwrap().required,
        None
    );
    let mut source: &(dyn std::error::Error + 'static) = &cause;
    while !source.is::<std::alloc::LayoutError>() {
        source = source.source().expect("actual layout failure retained");
    }
}
