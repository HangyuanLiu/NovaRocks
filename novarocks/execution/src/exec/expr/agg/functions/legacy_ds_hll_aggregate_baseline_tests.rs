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

//! Original DS HLL aggregate entry, state, tuning and error-order baselines.
use super::*;
use crate::exec::node::aggregate::AggTypeSignature;
use arrow::array::{Float64Array, Int64Array, UInt32Array};
use arrow::datatypes::Field;

fn spec(name: &str, input: &DataType, intermediate: bool, output: DataType) -> AggSpec {
    DsHllAgg
        .build_spec_from_type(
            &AggFunction {
                name: name.to_owned(),
                input_is_intermediate: intermediate,
                types: Some(AggTypeSignature {
                    intermediate_type: Some(DataType::Binary),
                    output_type: Some(output),
                    input_arg_type: Some(input.clone()),
                }),
                ..Default::default()
            },
            Some(input),
            intermediate,
        )
        .unwrap()
}
fn update(spec: &AggSpec, array: &ArrayRef, state: &mut DsHllState) -> Result<(), String> {
    let pointer = state as *mut DsHllState as AggStatePtr;
    DsHllAgg.update_batch(
        spec,
        0,
        &vec![pointer; array.len()],
        &AggInputView::Any(array),
    )
}
fn build(spec: &AggSpec, state: &DsHllState, intermediate: bool) -> ArrayRef {
    let pointer = state as *const DsHllState as AggStatePtr;
    DsHllAgg
        .build_array(spec, 0, &[pointer], intermediate)
        .unwrap()
}
fn packed(values: ArrayRef, log: ArrayRef, target: Option<ArrayRef>) -> ArrayRef {
    let mut columns = vec![values, log];
    if let Some(target) = target {
        columns.push(target);
    }
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(format!("f{i}"), a.data_type().clone(), true)))
        .collect::<Vec<_>>();
    Arc::new(StructArray::new(fields.into(), columns, None))
}
#[test]
fn legacy_ds_hll_baseline_real_spec_names_alias_and_phase_dispatch() {
    for name in ["ds_hll_count_distinct", "approx_count_distinct_hll_sketch"] {
        assert!(matches!(
            spec(name, &DataType::Int64, false, DataType::Int64).kind,
            AggKind::DsHllHash
        ));
        assert!(matches!(
            spec(name, &DataType::Binary, true, DataType::Int64).kind,
            AggKind::DsHllCount
        ));
    }
    assert!(matches!(
        spec(
            "ds_hll_count_distinct_union",
            &DataType::Binary,
            false,
            DataType::Binary
        )
        .kind,
        AggKind::DsHllMerge
    ));
    assert!(matches!(
        spec(
            "ds_hll_count_distinct_merge",
            &DataType::Binary,
            false,
            DataType::Binary
        )
        .kind,
        AggKind::DsHllMerge
    ));
    assert!(matches!(
        spec(
            "ds_hll_count_distinct_merge",
            &DataType::Binary,
            false,
            DataType::Int64
        )
        .kind,
        AggKind::DsHllCount
    ));
    assert!(matches!(
        spec(
            "ds_hll_count_distinct|frozen",
            &DataType::Int64,
            false,
            DataType::Int64
        )
        .kind,
        AggKind::DsHllHash
    ));
    assert_eq!(
        DsHllAgg
            .build_spec_from_type(&AggFunction::default(), None, false)
            .unwrap_err(),
        "ds_hll expects input"
    );
}
#[test]
fn legacy_ds_hll_baseline_empty_null_and_duplicate_final_and_state() {
    for values in [
        Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("a"),
            Some("b"),
        ])) as ArrayRef,
        Arc::new(StringArray::from(vec![None::<&str>; 3])) as ArrayRef,
        Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
    ] {
        let tracker = MemTracker::new_root("ds-hll-baseline");
        {
            let mut state = DsHllState::new(tracker.clone());
            let spec = spec(
                "ds_hll_count_distinct",
                values.data_type(),
                false,
                DataType::Int64,
            );
            update(&spec, &values, &mut state).unwrap();
            let expected = if values.len() == 4 { 2 } else { 0 };
            let out = build(&spec, &state, false);
            assert_eq!(
                out.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
                expected
            );
            assert_eq!(out.null_count(), 0);
            let payload = build(&spec, &state, true);
            let payload = payload.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert!(!payload.is_null(0));
            let decoded = HllHandle::from_payload_unreserved(payload.value(0)).unwrap();
            assert_eq!(decoded.estimate().unwrap(), expected);
        }
        assert_eq!(tracker.current(), 0);
    }
}
#[test]
fn legacy_ds_hll_baseline_parameters_null_order_wrap_and_first_handle_policy() {
    let tracker = MemTracker::new_root("ds-hll-tuning");
    let values = Arc::new(Int64Array::from(vec![None, Some(1), Some(2)])) as ArrayRef;
    let bad = packed(
        values.clone(),
        Arc::new(Float64Array::from(vec![10.; 3])),
        None,
    );
    let mut state = DsHllState::new(tracker.clone());
    let current = spec(
        "ds_hll_count_distinct",
        bad.data_type(),
        false,
        DataType::Int64,
    );
    assert_eq!(
        update(&current, &bad, &mut state).unwrap_err(),
        "ds_hll_count_distinct: ds_hll log_k expects integer input, got Float64"
    );
    assert!(state.handle.is_none());
    let wrapped = packed(
        values,
        Arc::new(Int64Array::from(vec![0, 266, 0])),
        Some(Arc::new(StringArray::from(vec!["HLL_8", "hll_4", "HLL_8"]))),
    );
    let current = spec(
        "ds_hll_count_distinct",
        wrapped.data_type(),
        false,
        DataType::Int64,
    );
    update(&current, &wrapped, &mut state).unwrap();
    let payload = build(&current, &state, true);
    let bytes = payload
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(bytes[3], 10, "266 wraps to u8 before the first handle");
    assert_eq!((bytes[7] >> 2) & 3, 0, "first non-NULL value chooses HLL_4");
    assert_eq!(state.handle.as_ref().unwrap().estimate().unwrap(), 2);
}
#[test]
fn legacy_ds_hll_baseline_struct_root_null_and_ignored_tail_follow_original_branch() {
    let columns = vec![
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![10, 10])) as ArrayRef,
        Arc::new(StringArray::from(vec!["unknown", "unknown"])) as ArrayRef,
        Arc::new(UInt32Array::from(vec![7, 8])) as ArrayRef,
    ];
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(format!("f{i}"), a.data_type().clone(), true)))
        .collect::<Vec<_>>();
    let values = Arc::new(StructArray::new(
        fields.into(),
        columns,
        Some(arrow::buffer::NullBuffer::from(vec![false, true])),
    )) as ArrayRef;
    let mut state = DsHllState::new(MemTracker::new_root("ds-hll-struct"));
    let current = spec(
        "ds_hll_count_distinct",
        values.data_type(),
        false,
        DataType::Int64,
    );
    update(&current, &values, &mut state).unwrap();
    assert_eq!(
        state.handle.as_ref().unwrap().estimate().unwrap(),
        2,
        "original ignores root bitmap and fourth field"
    );
    let payload = build(&current, &state, true);
    assert_eq!(
        (payload
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)[7]
            >> 2)
            & 3,
        1,
        "unknown target uses original HLL_6"
    );
}
#[test]
fn legacy_ds_hll_baseline_unsupported_all_null_and_empty_are_distinct() {
    for values in [
        Arc::new(UInt32Array::from(vec![Some(1)])) as ArrayRef,
        Arc::new(UInt32Array::from(vec![None])) as ArrayRef,
    ] {
        let mut state = DsHllState::new(MemTracker::new_root("ds-hll-unsupported"));
        let current = spec(
            "ds_hll_count_distinct",
            values.data_type(),
            false,
            DataType::Int64,
        );
        assert_eq!(
            update(&current, &values, &mut state).unwrap_err(),
            "ds_hll_count_distinct: unsupported sketch hash input type UInt32"
        );
        assert!(state.handle.is_none());
    }
    let values = Arc::new(UInt32Array::from(Vec::<u32>::new())) as ArrayRef;
    let mut state = DsHllState::new(MemTracker::new_root("ds-hll-empty"));
    update(
        &spec(
            "ds_hll_count_distinct",
            values.data_type(),
            false,
            DataType::Int64,
        ),
        &values,
        &mut state,
    )
    .unwrap();
}
#[test]
fn legacy_ds_hll_baseline_merge_latin1_bytes_null_and_exact_malformed_prefix() {
    let mut original = HllHandle::new_unreserved(10, HllTargetType::Hll8).unwrap();
    original.update_hash_unreserved(11).unwrap();
    let payload = original.serialize().unwrap();
    let text = payload.iter().map(|&b| char::from(b)).collect::<String>();
    for values in [
        Arc::new(BinaryArray::from(vec![None, Some(payload.as_slice())])) as ArrayRef,
        Arc::new(StringArray::from(vec![None, Some(text.as_str())])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![None, Some(text.as_str())])) as ArrayRef,
    ] {
        let mut state = DsHllState::new(MemTracker::new_root("ds-hll-merge"));
        let current = spec(
            "ds_hll_count_distinct_merge",
            values.data_type(),
            true,
            DataType::Int64,
        );
        update(&current, &values, &mut state).unwrap();
        assert_eq!(state.handle.as_ref().unwrap().estimate().unwrap(), 1);
        let mut builder = BinaryBuilder::new();
        builder.append_value(payload.as_slice());
        builder.append_value([]);
        let malformed = Arc::new(builder.finish()) as ArrayRef;
        let pointer = &mut state as *mut DsHllState as AggStatePtr;
        assert_eq!(
            DsHllAgg
                .merge_batch(
                    &current,
                    0,
                    &[pointer, pointer],
                    &AggInputView::Any(&malformed)
                )
                .unwrap_err(),
            "ds_hll merge preflight: HLL payload requires 8 bytes, got 0"
        );
        assert_eq!(state.handle.as_ref().unwrap().estimate().unwrap(), 1);
    }
    let mut state = DsHllState::new(MemTracker::new_root("ds-hll-first-payload"));
    assert_eq!(
        state.merge_payload(&[]).unwrap_err(),
        "ds_hll preflight: HLL payload requires 8 bytes, got 0"
    );
    assert!(state.handle.is_none());
}
#[test]
fn legacy_ds_hll_baseline_float_bits_are_not_ndv_canonicalization() {
    let values = Arc::new(Float64Array::from(vec![
        0.0,
        -0.0,
        f64::from_bits(0x7ff8000000000001),
        f64::from_bits(0xfff8000000001234),
    ])) as ArrayRef;
    assert_ne!(
        prehash_array_value(&values, 0, "raw").unwrap(),
        prehash_array_value(&values, 1, "raw").unwrap()
    );
    assert_ne!(
        prehash_array_value(&values, 2, "raw").unwrap(),
        prehash_array_value(&values, 3, "raw").unwrap()
    );
}

#[cfg(test)]
#[path = "ds_hll_local_phase_before_tests.rs"]
mod ds_hll_local_phase_before_tests;
