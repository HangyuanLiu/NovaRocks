// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow_array::types::{Int8Type, Int16Type};
use arrow_array::{
    Decimal128Array, DictionaryArray, Int8Array, Int16Array, Int64Array, StringArray, UInt64Array,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, UnionFields};
use arrow_select::take::take;
use std::sync::Arc;

fn copied(source: &dyn Array, indices: &[Option<u64>]) -> ArrayRef {
    preflight_take(source, indices, |_| Ok(())).unwrap();
    take(source, &UInt64Array::from(indices.to_vec()), None).unwrap()
}

#[test]
fn take_fixed_and_decimal_slices_preserve_nullable_indices_compact_order_and_repeats() {
    let indices = [Some(2), None, Some(0), Some(2)];
    let fixed = Int64Array::from(vec![Some(99), Some(9), None, Some(-7), Some(-99)]).slice(1, 3);
    let output = copied(&fixed, &indices);
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-7), None, Some(9), Some(-7)]
    );
    let decimal = Decimal128Array::from(vec![Some(999), Some(155), None, Some(-12), Some(-999)])
        .with_precision_and_scale(3, 2)
        .unwrap()
        .slice(1, 3);
    let output = copied(&decimal, &indices);
    assert_eq!(output.data_type(), &DataType::Decimal128(3, 2));
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-12), None, Some(155), Some(-12)]
    );
    preflight_broadcast(&decimal, 2, 4, |_| Ok(())).unwrap();
    let broadcast = copied(&decimal, &[Some(2); 4]);
    assert_eq!(
        broadcast
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-12); 4]
    );
    preflight_take(&decimal, &[], |_| Ok(())).unwrap();
    assert!(copied(&decimal, &[]).is_empty());
}

#[test]
fn take_null_indices_and_selected_null_bytes_do_not_copy_null_or_unselected_payload() {
    // Valid, nonempty payload exists beneath a NULL row. Unlike interleave's
    // Extend protocol, take skips this offset range and creates no NULL bytes.
    let hidden = vec![b'x'; 257 * 1024];
    let mut data = hidden;
    data.extend_from_slice(b"okunused");
    let n = 257 * 1024;
    let source = StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, n, n + 2, n + 8])),
        Buffer::from(data),
        Some(NullBuffer::from(vec![false, true, true])),
    );
    let output = copied(&source, &[Some(0), None, Some(1), Some(1)]);
    let values = output.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![None, None, Some("ok"), Some("ok")]
    );
    assert_eq!(output.to_data().buffers()[1].as_slice(), b"okok");
    // An all-NULL index plan reserves output validity but has no source row.
    let output = copied(&source, &[None; 3]);
    assert_eq!(output.null_count(), 3);
    assert!(output.to_data().buffers()[1].is_empty());
}

fn run_values(array: &ArrayRef) -> Vec<i64> {
    let runs = array
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    let values = runs.values().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..runs.len())
        .map(|row| values.value(runs.get_physical_index(row)))
        .collect()
}
fn union_values(array: &ArrayRef) -> Vec<i64> {
    let union = array.as_any().downcast_ref::<UnionArray>().unwrap();
    (0..union.len())
        .map(|row| {
            union
                .child(union.type_id(row))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(union.value_offset(row))
        })
        .collect()
}

#[test]
fn take_encoded_nonnull_addresses_preserve_dictionary_backing_union_offsets_and_sliced_runs() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "unused"]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), None, Some(0), Some(1)]),
        values.clone(),
    )
    .unwrap()
    .slice(1, 3);
    let output = copied(&dictionary, &[Some(2), None, Some(1)]);
    let actual = output
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(
        actual.keys().iter().collect::<Vec<_>>(),
        vec![Some(1), None, Some(0)]
    );
    assert!(Arc::ptr_eq(actual.values(), &values));
    assert_eq!(output.data_type(), dictionary.data_type());
    preflight_broadcast(&dictionary, 1, 3, |_| Ok(())).unwrap();
    let repeated = copied(&dictionary, &[Some(1); 3]);
    assert_eq!(
        repeated
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap()
            .keys()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0); 3]
    );

    let union: ArrayRef = Arc::new(
        UnionArray::try_new(
            UnionFields::try_new(
                [5, 99],
                [
                    Field::new("first", DataType::Int64, false),
                    Field::new("second", DataType::Int64, false),
                ],
            )
            .unwrap(),
            vec![5_i8, 99, 5, 99].into(),
            Some(vec![0_i32, 0, 1, 1].into()),
            vec![
                Arc::new(Int64Array::from(vec![10, 30])),
                Arc::new(Int64Array::from(vec![20, 40])),
            ],
        )
        .unwrap()
        .slice(1, 3),
    );
    assert_eq!(
        union_values(&copied(union.as_ref(), &[Some(2), Some(0), Some(1)])),
        vec![40, 20, 30]
    );
    preflight_broadcast(union.as_ref(), 1, 3, |_| Ok(())).unwrap();
    assert_eq!(
        union_values(&copied(union.as_ref(), &[Some(1); 3])),
        vec![30; 3]
    );

    let runs: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![2, 5]),
            &Int64Array::from(vec![10, 20]),
        )
        .unwrap()
        .slice(1, 3),
    );
    assert_eq!(
        run_values(&copied(runs.as_ref(), &[Some(2), Some(0), Some(2)])),
        vec![20, 10, 20]
    );
    preflight_broadcast(runs.as_ref(), 1, 3, |_| Ok(())).unwrap();
    assert_eq!(
        run_values(&copied(runs.as_ref(), &[Some(1); 3])),
        vec![20; 3]
    );
    // These are the unchanged format-author refusals, not a new encoded NULL
    // value policy or an assertion about vector's disputed Arrow semantics.
    for source in [union, runs] {
        assert!(matches!(
            preflight_take(source.as_ref(), &[None], |_| Ok(())),
            Err(CopyError::Invalid(_))
        ));
    }
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("original invalid program")),
        KernelFailure::Internal(crate::KernelDiagnostic::new("original internal failure")),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operation failure")),
        KernelFailure::InstanceFailed,
    ]
}

#[test]
fn take_original_bounded_work_callbacks_preserve_all_seven_failures_without_adding_a_tail() {
    let source = Int64Array::from(vec![7]);
    let indices = vec![Some(0); 320];
    let mut trace = Vec::new();
    preflight_take(&source, &indices, |boundary| {
        trace.push(boundary);
        Ok(())
    })
    .unwrap();
    assert!(trace.len() > 256);
    assert!(trace.iter().all(|boundary| !*boundary));
    for stop_at in 1..=trace.len() {
        for primary in causes() {
            let mut actual = Vec::new();
            let result = preflight_take(&source, &indices, |boundary| {
                actual.push(boundary);
                if actual.len() == stop_at {
                    Err(primary.clone())
                } else {
                    Ok(())
                }
            });
            assert!(matches!(result, Err(CopyError::Control(cause)) if cause == primary));
            assert_eq!(actual, trace[..stop_at]);
        }
    }
    let mut ordinary = Vec::new();
    assert!(matches!(
        preflight_take(&source, &[Some(0), Some(1)], |boundary| {
            ordinary.push(boundary);
            Ok(())
        }),
        Err(CopyError::Invalid(_))
    ));
    assert!(ordinary.iter().all(|boundary| !*boundary));
    // Existing take has no self-owned entry/finish. Its controller owns those
    // opaque boundaries; migration must preserve the original callback shape.
}
