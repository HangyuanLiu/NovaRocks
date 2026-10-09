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

//! Actual original concat versus hosted concat; these probes do not activate
//! compiled/public BY or manufacture a production Chunk/source pairing proof.
use super::*;
use novarocks_functions::selected_copy::concat_copy_in;
use arrow::array::ListViewArray;
use arrow::buffer::{BooleanBuffer, NullBuffer};

fn original(arrays: &[ArrayRef]) -> Result<ArrayRef, arrow::error::ArrowError> {
    arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())
}
fn dict(label: &str, len: usize) -> ArrayRef {
    Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None]),
            Arc::new(StringArray::from(
                (0..len).map(|n| format!("{label}-{n}")).collect::<Vec<_>>(),
            )),
        )
        .unwrap(),
    )
}
fn list_views() -> Vec<ArrayRef> {
    let mut field = Field::new("retained_list_values", DataType::Int32, true);
    field.set_metadata(std::collections::HashMap::from([(
        "PARQUET:field_id".into(),
        "7".into(),
    )]));
    let field = Arc::new(field);
    vec![
        Arc::new(
            ListViewArray::try_new(
                field.clone(),
                vec![2_i32].into(),
                vec![0_i32].into(),
                Arc::new(Int32Array::from(vec![11, 12, 13, 14])),
                None,
            )
            .unwrap(),
        ),
        Arc::new(
            ListViewArray::try_new(
                field,
                vec![1_i32].into(),
                vec![0_i32].into(),
                Arc::new(Int32Array::from(vec![21, 22, 23])),
                None,
            )
            .unwrap(),
        ),
    ]
}
fn runs() -> Vec<ArrayRef> {
    vec![
        Arc::new(
            RunArray::<Int16Type>::try_new(
                &Int16Array::from(vec![500, 1000]),
                &Int64Array::from(vec![10, 11]),
            )
            .unwrap(),
        ),
        Arc::new(
            RunArray::<Int16Type>::try_new(
                &Int16Array::from(vec![600, 1000]),
                &Int64Array::from(vec![20, 21]),
            )
            .unwrap(),
        ),
    ]
}
fn matching(arrays: Vec<ArrayRef>) {
    let expected = original(&arrays).unwrap();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr.clone(), None);
    let actual = concat_copy_in(arrays, (), host.clone(), &control).unwrap();
    assert_eq!(actual.values().to_data(), expected.to_data());
    assert!(actual.new_buffer_stock() <= actual.facts().retained_new_backing_upper());
    assert!(
        host.events()
            .iter()
            .any(|event| matches!(event, Event::OpaqueAttempt(bytes) if *bytes>0))
    );
    drop(actual);
    assert_released(&host);
}
#[test]
fn by_copy_original_concat_specialized_dictionary_listview_run_resource_paths() {
    // The old full-domain Int8 cardinality gate would reject 140 domains even
    // though original merge uses only two referenced values. No replacement
    // key decoder or blanket MutableArrayData gate is permitted here.
    matching(vec![dict("first", 70), dict("second", 70)]);
    matching(list_views());
    matching(runs());
}
#[test]
fn by_copy_original_concat_nested_empty_slice_nullable_alias_and_union_paths() {
    matching(vec![input().slice(1, 2), input().slice(0, 1)]);
    matching(vec![
        Arc::new(StructArray::new_empty_fields(2, None)),
        Arc::new(StructArray::new_empty_fields(1, None)),
    ]);
    matching(vec![
        Arc::new(NullArray::new(3)),
        Arc::new(NullArray::new(2)),
    ]);
    matching(vec![
        Arc::new(StringViewArray::from(vec![
            Some("long original aliased backing"),
            None,
        ])),
        Arc::new(StringViewArray::from(vec![
            Some("a second variadic source backing"),
            Some("s"),
        ])),
    ]);
    let values: ArrayRef = Arc::new(StringArray::from(vec!["first", "second"]));
    let first: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), None]), values.clone())
            .unwrap(),
    );
    let second: ArrayRef =
        Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![1]), values).unwrap());
    matching(vec![first, second]);
    let fields = UnionFields::new(
        vec![0, 127],
        vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ],
    );
    for dense in [false, true] {
        let first: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields.clone(),
                vec![0_i8, 127, 0].into(),
                dense.then(|| vec![0_i32, 0, 1].into()),
                if dense {
                    vec![
                        Arc::new(Int32Array::from(vec![1, 3])),
                        Arc::new(Int32Array::from(vec![2])),
                    ]
                } else {
                    vec![
                        Arc::new(Int32Array::from(vec![1, 2, 3])),
                        Arc::new(Int32Array::from(vec![4, 5, 6])),
                    ]
                },
            )
            .unwrap(),
        );
        matching(vec![first.clone(), first]);
    }
    let field = Arc::new(Field::new("fixed_child", DataType::Int64, true));
    let fixed: ArrayRef = Arc::new(
        arrow::array::FixedSizeListArray::try_new(
            field,
            2,
            Arc::new(Int64Array::from(vec![Some(7), None, Some(8), Some(9)])),
            Some(NullBuffer::new(BooleanBuffer::from(vec![false, true]))),
        )
        .unwrap(),
    );
    matching(vec![fixed.clone(), fixed]);
}
#[test]
fn by_copy_original_concat_every_actual_host_and_callback_refusal_preserves_first_cause() {
    let arrays = list_views();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    drop(concat_copy_in(arrays.clone(), (), host.clone(), &control).unwrap());
    let attempts = *host.allocations.lock().unwrap();
    let trace = control.trace();
    assert_released(&host);
    for cause in causes() {
        for stop in 0..attempts {
            let tr = tracker();
            let host = Host::new(tr.clone(), Some((stop, cause.clone())));
            let control = Control::new(tr, None);
            assert!(
                matches!(concat_copy_in(arrays.clone(),(),host.clone(),&control),
                Err(CopyOperationError::Control(actual)) if actual==cause)
            );
            assert_eq!(*host.allocations.lock().unwrap(), stop + 1);
            assert_released(&host);
        }
        for stop in 0..trace.len() {
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, Some((stop, cause.clone())));
            assert!(
                matches!(concat_copy_in(arrays.clone(),(),host.clone(),&control),
                Err(CopyOperationError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
            assert_released(&host);
        }
    }
}
#[test]
fn by_copy_original_concat_last_buffer_retains_all_actual_source_owners_and_charge() {
    let arrays = vec![
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![3])),
    ];
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr.clone(), None);
    let drops = Arc::new(AtomicUsize::new(0));
    let actual =
        concat_copy_in(arrays, SourceOwner(drops.clone()), host.clone(), &control).unwrap();
    let values = actual.into_values();
    let buffer = values.to_data().buffers()[0].clone();
    drop(values);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(tr.current() > 0);
    drop(buffer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_released(&host);
}
#[test]
fn by_copy_original_concat_dynamic_full_data_and_empty_error_keep_original_text_until_drop() {
    let mut field = Field::new("long_original_field_".repeat(100), DataType::Int32, true);
    field.set_metadata(std::collections::HashMap::from([(
        "large_original_metadata".into(),
        "original".repeat(200),
    )]));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(field),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            Arc::new(Int32Array::from(vec![7])),
            None,
        )
        .unwrap(),
    );
    for arrays in [vec![], vec![list, Arc::new(Int64Array::from(vec![1]))]] {
        let expected = original(&arrays).unwrap_err().to_string();
        let tr = tracker();
        let host = Host::new(tr.clone(), None);
        let control = Control::new(tr.clone(), None);
        let error = concat_copy_in(arrays, (), host.clone(), &control)
            .err()
            .unwrap();
        let CopyOperationError::OriginalData(data) = error else {
            panic!("original concat Data was reclassified: {error:?}");
        };
        assert_eq!(data.text(), expected);
        assert!(data.retained_bytes() >= expected.len());
        assert!(tr.current() > 0);
        drop(data);
        assert_released(&host);
    }
}
#[test]
fn by_copy_original_concat_one_source_preserves_slice_alias_and_bufferless_output() {
    matching(vec![input()]);
    matching(vec![Arc::new(StructArray::new_empty_fields(0, None))]);
    matching(vec![Arc::new(NullArray::new(0))]);
}

#[test]
fn by_copy_original_concat_data_path_every_actual_refusal_and_no_data_footer() {
    let arrays = vec![
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        Arc::new(Int64Array::from(vec![2])),
    ];
    let expected = original(&arrays).unwrap_err().to_string();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let error = concat_copy_in(arrays.clone(), (), host.clone(), &control)
        .err()
        .unwrap();
    let CopyOperationError::OriginalData(data) = error else {
        panic!("original Data was reclassified: {error:?}")
    };
    assert_eq!(data.text(), expected);
    let trace = control.trace();
    let attempts = *host.allocations.lock().unwrap();
    drop(data);
    assert_released(&host);
    for cause in causes() {
        for stop in 0..attempts {
            let tr = tracker();
            let host = Host::new(tr.clone(), Some((stop, cause.clone())));
            let control = Control::new(tr, None);
            assert!(
                matches!(concat_copy_in(arrays.clone(), (), host.clone(), &control), Err(CopyOperationError::Control(actual)) if actual == cause)
            );
            assert_eq!(*host.allocations.lock().unwrap(), stop + 1);
            assert_released(&host);
        }
        for stop in 0..trace.len() {
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, Some((stop, cause.clone())));
            assert!(
                matches!(concat_copy_in(arrays.clone(), (), host.clone(), &control), Err(CopyOperationError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
            assert_released(&host);
        }
    }
}
