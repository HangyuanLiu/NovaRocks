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
//! Original MAP_AGG raw state, ordering, NULL and full diagnostic baselines.
use super::*;
use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow_buffer::NullBuffer;
use std::collections::HashMap;

fn packed(keys: ArrayRef, values: ArrayRef, nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![
            Field::new("k", keys.data_type().clone(), true),
            Field::new("v", values.data_type().clone(), true),
        ]
        .into(),
        vec![keys, values],
        nulls,
    ))
}
fn target(k: DataType, v: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![Field::new("key", k, true), Field::new("value", v, true)].into()),
            false,
        )),
        false,
    )
}
fn spec(input: &ArrayRef, output: DataType) -> AggSpec {
    MapAggAgg
        .build_spec_from_type(
            &AggFunction {
                name: "map_agg".into(),
                types: Some(crate::exec::node::aggregate::AggTypeSignature {
                    intermediate_type: Some(output.clone()),
                    output_type: Some(output),
                    input_arg_type: None,
                }),
                ..Default::default()
            },
            Some(input.data_type()),
            false,
        )
        .unwrap()
}
fn update(state: &mut MapAggState, spec: &AggSpec, input: &ArrayRef) -> Result<(), String> {
    let ptr = state as *mut MapAggState as AggStatePtr;
    MapAggAgg.update_batch(spec, 0, &vec![ptr; input.len()], &AggInputView::Any(input))
}
fn emit(state: &MapAggState, spec: &AggSpec, partial: bool) -> Result<ArrayRef, String> {
    let ptr = state as *const MapAggState as AggStatePtr;
    MapAggAgg.build_array(spec, 0, &[ptr], partial)
}
#[test]
fn legacy_map_agg_baseline_empty_and_all_null_keys_emit_nonnull_empty_map() {
    let input = packed(
        Arc::new(Int64Array::from(vec![None, None])),
        Arc::new(Int64Array::from(vec![Some(9), None])),
        None,
    );
    let s = spec(&input, target(DataType::Int64, DataType::Int64));
    let tracker = MemTracker::new_root("map-empty-original");
    let mut state = MapAggState::new(tracker.clone());
    for partial in [false, true] {
        let out = emit(&state, &s, partial).unwrap();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!map.is_null(0));
        assert_eq!(map.value_offsets(), &[0, 0]);
    }
    update(&mut state, &s, &input).unwrap();
    assert!(state.entries.is_empty());
    assert!(state.seen_keys.is_empty());
    assert_eq!(tracker.current(), 0);
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_map_agg_baseline_first_wins_null_value_and_parent_struct_null_is_ignored() {
    let input = packed(
        Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(2), None])),
        Arc::new(Int64Array::from(vec![None, Some(10), Some(99), Some(88)])),
        Some(NullBuffer::from(vec![false, true, true, true])),
    );
    let s = spec(&input, target(DataType::Int64, DataType::Int64));
    let tracker = MemTracker::new_root("map-first-original");
    let mut state = MapAggState::new(tracker.clone());
    update(&mut state, &s, &input).unwrap();
    let out = emit(&state, &s, false).unwrap();
    let map = out.as_any().downcast_ref::<MapArray>().unwrap();
    let keys = map.keys().as_any().downcast_ref::<Int64Array>().unwrap();
    let values = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(keys.values().as_ref(), &[2, 1]);
    assert!(values.is_null(0));
    assert_eq!(values.value(1), 10);
    assert_eq!(
        MapAggAgg.retained_bytes(&s, &state as *const MapAggState as *const u8),
        0
    );
    assert_eq!(
        MapAggAgg.retained_memory_policy(&s),
        RetainedMemoryPolicy::AllocationTracked
    );
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_map_agg_baseline_key_null_skips_value_but_duplicate_value_is_read_before_lookup() {
    let input = packed(
        Arc::new(Int64Array::from(vec![None, Some(1), Some(1)])),
        Arc::new(UInt32Array::from(vec![Some(7), None, Some(99)])),
        None,
    );
    let s = spec(&input, target(DataType::Int64, DataType::UInt32));
    let tracker = MemTracker::new_root("map-prefix-original");
    let mut state = MapAggState::new(tracker.clone());
    assert_eq!(
        update(&mut state, &s, &input).unwrap_err(),
        "unsupported tracked scalar type: UInt32"
    );
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.seen_keys.len(), 1);
    assert!(state.entries[0].1.is_none());
    assert_eq!(
        emit(&state, &s, false).unwrap_err(),
        "unsupported scalar output type: UInt32"
    );
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_map_agg_baseline_partial_merge_order_and_null_map_slice() {
    let input = packed(
        Arc::new(Int64Array::from(vec![2, 1, 2, 3])),
        Arc::new(Int64Array::from(vec![20, 10, 99, 30])),
        None,
    );
    let s = spec(&input, target(DataType::Int64, DataType::Int64));
    let tracker = MemTracker::new_root("map-merge-original");
    let mut source = MapAggState::new(tracker.clone());
    update(&mut source, &s, &input).unwrap();
    let partial = emit(&source, &s, true).unwrap();
    assert_eq!(partial.data_type(), &s.intermediate_type);
    let map = partial.as_any().downcast_ref::<MapArray>().unwrap();
    let DataType::Map(map_field, _) = partial.data_type() else {
        panic!("actual map state");
    };
    let mut dest = MapAggState::new(tracker.clone());
    let ptr = &mut dest as *mut MapAggState as AggStatePtr;
    MapAggAgg
        .merge_batch(&s, 0, &[ptr], &AggInputView::Any(&partial))
        .unwrap();
    let out = emit(&dest, &s, false).unwrap();
    assert_eq!(out.to_data(), partial.to_data());
    let sliced: ArrayRef = Arc::new(
        MapArray::new(
            map_field.clone(),
            OffsetBuffer::new(vec![0i32, 0, 3].into()),
            map.entries().clone(),
            Some(NullBuffer::from(vec![false, true])),
            false,
        )
        .slice(1, 1),
    );
    MapAggAgg
        .merge_batch(&s, 0, &[ptr], &AggInputView::Any(&sliced))
        .unwrap();
    assert_eq!(emit(&dest, &s, false).unwrap().to_data(), out.to_data());
    drop((dest, source));
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_map_agg_baseline_float_fingerprint_nan_canonical_but_signed_zero_distinct() {
    let input = packed(
        Arc::new(Float64Array::from(vec![
            0.0,
            -0.0,
            f64::from_bits(0x7ff8000000000001),
            f64::from_bits(0xfff8000000001234),
        ])),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        None,
    );
    let s = spec(&input, target(DataType::Float64, DataType::Int64));
    let tracker = MemTracker::new_root("map-float-original");
    let mut state = MapAggState::new(tracker.clone());
    update(&mut state, &s, &input).unwrap();
    let out = emit(&state, &s, false).unwrap();
    let map = out.as_any().downcast_ref::<MapArray>().unwrap();
    let keys = map.keys().as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys.value(0).to_bits(), 0.0f64.to_bits());
    assert_eq!(keys.value(1).to_bits(), (-0.0f64).to_bits());
    assert_eq!(keys.value(2).to_bits(), 0x7ff8000000000001);
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_map_agg_baseline_target_full_fields_nested_metadata_and_sorted_flag() {
    let mut meta = HashMap::new();
    meta.insert("provider.fixture".into(), "same-source".into());
    let nested_type = DataType::Struct(
        vec![Field::new("payload", DataType::Utf8, true).with_metadata(meta.clone())].into(),
    );
    let vals = build_scalar_array(
        &nested_type,
        vec![Some(AggScalarValue::Struct(vec![Some(
            AggScalarValue::Utf8("original".into()),
        )]))],
    )
    .unwrap();
    let input = packed(Arc::new(Int64Array::from(vec![1])), vals, None);
    let output = DataType::Map(
        Arc::new(
            Field::new(
                "custom_entries",
                DataType::Struct(
                    vec![
                        Field::new("custom_key", DataType::Int64, false)
                            .with_metadata(meta.clone()),
                        Field::new("custom_value", nested_type, true).with_metadata(meta.clone()),
                    ]
                    .into(),
                ),
                false,
            )
            .with_metadata(meta),
        ),
        true,
    );
    let s = spec(&input, output.clone());
    let tracker = MemTracker::new_root("map-metadata-original");
    let mut state = MapAggState::new(tracker.clone());
    update(&mut state, &s, &input).unwrap();
    for partial in [false, true] {
        assert_eq!(emit(&state, &s, partial).unwrap().data_type(), &output);
    }
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_map_agg_baseline_original_view_and_spec_errors_are_exact() {
    let raw: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    assert_eq!(
        MapAggAgg
            .build_spec_from_type(
                &AggFunction {
                    name: "map_agg".into(),
                    ..Default::default()
                },
                None,
                false
            )
            .unwrap_err(),
        "map_agg input type missing"
    );
    assert_eq!(
        MapAggAgg
            .build_spec_from_type(
                &AggFunction {
                    name: "map_agg".into(),
                    ..Default::default()
                },
                Some(raw.data_type()),
                false
            )
            .unwrap_err(),
        "map_agg expects struct input, got Int64"
    );
    let input = packed(raw.clone(), raw.clone(), None);
    let mut s = spec(&input, target(DataType::Int64, DataType::Int64));
    assert_eq!(
        MapAggAgg.build_input_view(&s, &None).err().unwrap(),
        "map_agg input missing"
    );
    assert_eq!(
        MapAggAgg
            .build_merge_view(&s, &Some(raw.clone()))
            .err()
            .unwrap(),
        "map_agg merge input must be MapArray"
    );
    let tracker = MemTracker::new_root("map-error-original");
    let mut state = MapAggState::new(tracker);
    assert_eq!(
        update(&mut state, &s, &raw).unwrap_err(),
        "map_agg expects struct input"
    );
    s.output_type = DataType::Int64;
    assert_eq!(
        emit(&state, &s, false).unwrap_err(),
        "map_agg output type must be MAP, got Int64"
    );
}
#[test]
fn legacy_map_agg_baseline_real_allocator_refusal_rolls_back_and_tracker_is_required() {
    let input = packed(
        Arc::new(StringArray::from(vec!["key"])),
        Arc::new(StringArray::from(vec!["value"])),
        None,
    );
    let s = spec(&input, target(DataType::Utf8, DataType::Utf8));
    let mut slot = std::mem::MaybeUninit::<MapAggState>::uninit();
    assert_eq!(
        MapAggAgg
            .init_state_with_tracker(&s, slot.as_mut_ptr() as *mut u8, None)
            .unwrap_err(),
        "allocation-tracked map_agg requires a memory tracker"
    );
    let tracker = MemTracker::new_root("map-refusal-original");
    tracker.install_limit_once(1).unwrap();
    let mut state = MapAggState::new(tracker.clone());
    let error = update(&mut state, &s, &input).unwrap_err();
    assert!(error.starts_with("ResourceExhausted:"), "{error}");
    assert!(state.entries.is_empty());
    assert!(state.seen_keys.is_empty());
    drop(state);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn legacy_map_agg_baseline_lazy_full_diagnostic_uses_actual_carrier_debug() {
    let mut metadata = HashMap::new();
    metadata.insert("provider.original".into(), "x".repeat(1024));
    let field = Arc::new(
        Field::new("wide-actual-unsupported", DataType::UInt32, true).with_metadata(metadata),
    );
    let value: ArrayRef = Arc::new(arrow::array::FixedSizeListArray::new(
        field,
        1,
        Arc::new(UInt32Array::from(vec![7])),
        None,
    ));
    let actual_type = value.data_type().clone();
    let input = packed(Arc::new(Int64Array::from(vec![1])), value, None);
    let s = spec(&input, target(DataType::Int64, actual_type.clone()));
    let tracker = MemTracker::new_root("map-long-original");
    let mut state = MapAggState::new(tracker.clone());
    let error = update(&mut state, &s, &input).unwrap_err();
    assert_eq!(
        error,
        format!("unsupported tracked scalar type: {actual_type:?}")
    );
    assert!(error.len() > 512);
    assert!(state.entries.is_empty());
    drop(state);
    assert_eq!(tracker.current(), 0);
}
