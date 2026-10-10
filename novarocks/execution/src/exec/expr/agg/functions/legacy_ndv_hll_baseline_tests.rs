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

//! Independent original HLL hash/state/codec baselines before shared extraction.
use super::*;
use arrow::array::{Float32Array, Float64Array, Int32Array, NullArray, UInt32Array};
use std::sync::Arc;

#[test]
fn legacy_ndv_hll_baseline_float_nan_and_signed_zero_are_canonical() {
    let f32s: ArrayRef = Arc::new(Float32Array::from(vec![
        0.0,
        -0.0,
        f32::from_bits(0x7fc00001),
        f32::from_bits(0xffc01234),
        f32::INFINITY,
    ]));
    let f64s: ArrayRef = Arc::new(Float64Array::from(vec![
        0.0,
        -0.0,
        f64::from_bits(0x7ff8000000000001),
        f64::from_bits(0xfff8000000001234),
        f64::INFINITY,
    ]));
    for array in [&f32s, &f64s] {
        assert_eq!(
            hash_array_value_for_hll(array, 0).unwrap(),
            hash_array_value_for_hll(array, 1).unwrap()
        );
        assert_eq!(
            hash_array_value_for_hll(array, 2).unwrap(),
            hash_array_value_for_hll(array, 3).unwrap()
        );
        assert_ne!(
            hash_array_value_for_hll(array, 0).unwrap(),
            hash_array_value_for_hll(array, 4).unwrap()
        );
    }
}
#[test]
fn legacy_ndv_hll_baseline_null_row_bounds_and_original_unsupported_errors() {
    let integers: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(1)]));
    assert_eq!(hash_array_value_for_hll(&integers, 0).unwrap(), None);
    assert_eq!(
        hash_array_value_for_hll(&integers, 2).unwrap_err(),
        "hll_raw row 2 out of bounds for len 2"
    );
    let unsigned: ArrayRef = Arc::new(UInt32Array::from(vec![1]));
    assert_eq!(
        hash_array_value_for_hll(&unsigned, 0).unwrap_err(),
        "hll_raw does not support input type UInt32"
    );
    let nulls: ArrayRef = Arc::new(NullArray::new(1));
    assert_eq!(
        hash_array_value_for_hll(&nulls, 0).unwrap_err(),
        "hll_raw does not support input type Null"
    );
}
#[test]
fn legacy_ndv_hll_baseline_zero_hash_and_sparse_state_bytes_are_exact() {
    let mut state = HllRawState {
        has_value: true,
        ..HllRawState::default()
    };
    update_state_register_from_hash(&mut state, 0);
    assert!(state.registers.is_none());
    assert_eq!(serialize_hll_state(&state), Some(vec![HLL_DATA_EMPTY]));
    update_state_register_from_hash(&mut state, 1);
    assert_eq!(
        serialize_hll_state(&state),
        Some(vec![2, 1, 0, 0, 0, 1, 0, 51])
    );
    assert_eq!(estimate_cardinality(&state), 1);
}
#[test]
fn legacy_ndv_hll_baseline_empty_and_short_codec_errors_remain_exact() {
    for (bytes, message) in [
        (&[][..], "hll_raw merge payload is empty"),
        (
            &[HLL_DATA_EXPLICIT][..],
            "hll_raw EXPLICIT payload is malformed",
        ),
        (
            &[HLL_DATA_SPARSE, 0][..],
            "hll_raw SPARSE payload is malformed",
        ),
    ] {
        let mut state = HllRawState::default();
        assert_eq!(merge_hll_bytes(&mut state, bytes).unwrap_err(), message);
    }
}
#[test]
fn legacy_ndv_hll_baseline_nonstandard_payloads_use_original_opaque_hash() {
    for bytes in [
        &[99, 1, 2][..],
        &[HLL_DATA_EXPLICIT, 1][..],
        &[HLL_DATA_FULL, 1][..],
        &[HLL_DATA_SPARSE, 1, 0, 0, 0][..],
    ] {
        let mut actual = HllRawState {
            has_value: true,
            ..HllRawState::default()
        };
        merge_hll_bytes(&mut actual, bytes).unwrap();
        let mut expected = HllRawState {
            has_value: true,
            ..HllRawState::default()
        };
        update_state_register_from_hash(&mut expected, hash_bytes_for_hll(bytes));
        assert_eq!(serialize_hll_state(&actual), serialize_hll_state(&expected));
    }
}
#[test]
fn legacy_ndv_hll_baseline_sparse_invalid_index_keeps_prior_update_before_opaque_hash() {
    let bytes = [2, 2, 0, 0, 0, 1, 0, 7, 255, 255, 9];
    let mut actual = HllRawState {
        has_value: true,
        ..HllRawState::default()
    };
    merge_hll_bytes(&mut actual, &bytes).unwrap();
    let registers = actual.registers.as_ref().unwrap();
    assert!(registers[1] >= 7);
    let mut expected = HllRawState {
        has_value: true,
        ..HllRawState::default()
    };
    ensure_registers(&mut expected)[1] = 7;
    update_state_register_from_hash(&mut expected, hash_bytes_for_hll(&bytes));
    assert_eq!(serialize_hll_state(&actual), serialize_hll_state(&expected));
}
#[test]
fn legacy_ndv_hll_baseline_full_dense_payload_roundtrips_without_new_decoder() {
    let mut payload = vec![HLL_DATA_FULL];
    payload.extend(std::iter::repeat_n(1, HLL_REGISTERS_COUNT));
    let mut state = HllRawState {
        has_value: true,
        ..HllRawState::default()
    };
    merge_hll_bytes(&mut state, &payload).unwrap();
    assert_eq!(serialize_hll_state(&state), Some(payload));
}
#[test]
fn legacy_ndv_hll_baseline_empty_group_final_zero_but_intermediate_null() {
    let agg = HllRawAgg;
    let spec = AggSpec {
        kind: AggKind::HllRawHash,
        output_type: DataType::Int64,
        intermediate_type: DataType::Binary,
        input_arg_type: None,
        count_all: false,
    };
    let mut slot = Box::new(0usize);
    let ptr = (&mut *slot) as *mut usize as *mut u8;
    agg.init_state(&spec, ptr);
    let final_values = agg
        .build_array(&spec, 0, &[ptr as AggStatePtr], false)
        .unwrap();
    assert_eq!(
        final_values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert!(!final_values.is_null(0));
    let intermediate = agg
        .build_array(&spec, 0, &[ptr as AggStatePtr], true)
        .unwrap();
    assert!(intermediate.is_null(0));
    agg.drop_state(&spec, ptr);
}
