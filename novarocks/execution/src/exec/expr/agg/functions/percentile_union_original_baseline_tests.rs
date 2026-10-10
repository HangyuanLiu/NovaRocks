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

//! Original UNION update/merge/finalize entry, before owner installation.
use super::*;
use arrow::array::{LargeBinaryArray, LargeStringArray, StringArray, UInt32Array};
fn union() -> Original {
    Original::new("percentile_union", DataType::Binary)
}
#[test]
fn legacy_percentile_union_baseline_binary_update_merge_and_empty_state() {
    let one = percentile::encode_single_value(3.);
    let mut state = union();
    state
        .update_packed(Arc::new(BinaryArray::from(vec![
            Some(one.as_slice()),
            None,
            Some(one.as_slice()),
        ])))
        .unwrap();
    assert_eq!(state.count(), 2.);
    let payload = state.bytes();
    let mut merged = union();
    merged.merge(bin(&payload)).unwrap();
    assert_eq!(merged.count(), 2.);
    let final_bytes = merged.output(false).unwrap();
    assert_eq!(final_bytes.data_type(), &DataType::Binary);
    assert_eq!(final_bytes.null_count(), 0);
    assert_eq!(
        final_bytes
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        merged.bytes()
    );
    let mut empty = union();
    empty
        .update_packed(Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())))
        .unwrap();
    assert_eq!(empty.bytes(), percentile::encode_empty_state());
}
#[test]
fn legacy_percentile_union_baseline_struct_root_null_is_ignored_and_rate_first() {
    let values = vec![
        floats(vec![Some(3.), Some(7.)]),
        floats(vec![Some(0.5), Some(0.5)]),
    ];
    let fields = values
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("original-{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let input = Arc::new(StructArray::new(
        Fields::from(fields),
        values,
        Some(NullBuffer::from(vec![false, false])),
    )) as ArrayRef;
    let mut state = union();
    state.update_packed(input).unwrap();
    assert_eq!(state.count(), 2.);
    let mut invalid = union();
    assert_eq!(
        invalid.update(vec![one(None), one(Some(2.))]).unwrap_err(),
        "percentile_approx: percentile parameter must be between 0 and 1, got 2"
    );
    assert_eq!(invalid.count(), 0.);
}
#[test]
fn legacy_percentile_union_baseline_empty_struct_shape_error_before_rows() {
    for length in [0, 1] {
        let empty = Arc::new(StructArray::new_empty_fields(length, None)) as ArrayRef;
        assert_eq!(
            union().update_packed(empty).unwrap_err(),
            "percentile_approx: percentile_approx expects STRUCT(value, quantile[, compression]) input"
        );
        let child = floats(vec![None; length]);
        let input = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("value", DataType::Float64, true)]),
            vec![child],
            None,
        )) as ArrayRef;
        assert_eq!(
            union().update_packed(input).unwrap_err(),
            "percentile_approx: percentile_approx expects STRUCT(value, quantile[, compression]) input"
        );
    }
}
#[test]
fn legacy_percentile_union_baseline_unsupported_null_carrier_and_prefix_mutation() {
    let bad = Arc::new(UInt32Array::from(vec![None::<u32>])) as ArrayRef;
    assert_eq!(
        union().update_packed(bad.clone()).unwrap_err(),
        "percentile_approx: unsupported percentile payload type UInt32"
    );
    assert_eq!(
        union().merge(bad).unwrap_err(),
        "percentile_approx_merge: unsupported percentile payload type UInt32"
    );
    let good = percentile::encode_single_value(3.);
    let mut state = union();
    let input = Arc::new(BinaryArray::from(vec![good.as_slice(), &[0xA2, 4][..]])) as ArrayRef;
    assert_eq!(
        state.update_packed(input).unwrap_err(),
        "percentile state payload too short"
    );
    assert_eq!(
        state.count(),
        1.,
        "original first valid row survives the following Data failure"
    );
}
#[test]
fn legacy_percentile_union_baseline_original_bounded_v4_and_no_magic_check() {
    let mut old = percentile::encode_empty_state();
    old[1] = 3;
    for bytes in [&[][..], old.as_slice(), b"bad".as_slice()] {
        assert_eq!(
            union().update_packed(bin(bytes)).unwrap_err(),
            "bounded percentile aggregate requires state version 4"
        );
    }
    // Preserve the original aggregate decoder quirk; scalar decoder differs.
    let mut bytes = percentile::encode_empty_state();
    bytes[0] = b'P';
    let text = String::from_utf8(bytes.clone()).unwrap();
    for input in [
        bin(&bytes),
        Arc::new(LargeBinaryArray::from(vec![bytes.as_slice()])) as ArrayRef,
        Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![text.as_str()])) as ArrayRef,
    ] {
        let mut state = union();
        state.update_packed(input).unwrap();
        assert_eq!(state.count(), 0.);
        assert_eq!(state.bytes(), percentile::encode_empty_state());
    }
}
