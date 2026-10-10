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

//! Independent raw v1 count DISTINCT contract before shared computation extraction.
//! Register as a cfg(test) child of agg::functions::count_distinct.
use super::*;
use arrow::array::{LargeStringArray, ListArray, NullArray, StructArray, UInt64Array};
use arrow_buffer::i256;
use arrow::datatypes::{Field, Fields, Int32Type};
use std::mem::MaybeUninit;

struct LegacyState {
    raw: MaybeUninit<DistinctSet>,
    spec: AggSpec,
}
impl LegacyState {
    fn new(ty: DataType, tracker: Arc<MemTracker>) -> Self {
        let spec = CountDistinctAgg
            .build_spec_from_type(
                &AggFunction {
                    name: "multi_distinct_count".into(),
                    ..Default::default()
                },
                Some(&ty),
                false,
            )
            .unwrap();
        let mut state = Self {
            raw: MaybeUninit::uninit(),
            spec,
        };
        CountDistinctAgg
            .init_state_with_tracker(&state.spec, state.raw.as_mut_ptr().cast(), Some(tracker))
            .unwrap();
        state
    }
    fn pointer(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn update(&mut self, input: &ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); input.len()];
        CountDistinctAgg.update_batch(&self.spec, 0, &pointers, &AggInputView::Any(input))
    }
    fn merge(&mut self, input: &BinaryArray) -> Result<(), String> {
        let pointers = vec![self.pointer(); input.len()];
        CountDistinctAgg.merge_batch(&self.spec, 0, &pointers, &AggInputView::Binary(input))
    }
    fn output(&mut self, intermediate: bool) -> ArrayRef {
        let pointer = self.pointer();
        CountDistinctAgg
            .build_array(&self.spec, 0, &[pointer], intermediate)
            .unwrap()
    }
    fn count(&mut self) -> i64 {
        self.output(false)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }
    fn keys(&mut self) -> Vec<Vec<u8>> {
        let output = self.output(true);
        let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(!output.is_null(0));
        let mut keys = deserialize_set(output.value(0)).unwrap();
        keys.sort();
        keys
    }
}
impl Drop for LegacyState {
    fn drop(&mut self) {
        CountDistinctAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
fn keys(input: ArrayRef) -> Vec<Vec<u8>> {
    let mut state = LegacyState::new(
        input.data_type().clone(),
        MemTracker::new_root("legacy-distinct-keys"),
    );
    state.update(&input).unwrap();
    state.keys()
}
fn payload(keys: &[&[u8]], tail: &[u8]) -> Vec<u8> {
    let mut out = (keys.len() as u32).to_le_bytes().to_vec();
    for key in keys {
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
    }
    out.extend_from_slice(tail);
    out
}
#[test]
fn legacy_count_distinct_float_signed_zero_and_nan_payloads_are_distinct() {
    let bits = [
        0,
        0x8000_0000_0000_0000,
        0x7ff8_0000_0000_0001,
        0x7ff8_0000_0000_0002,
    ];
    let values = bits
        .into_iter()
        .map(f64::from_bits)
        .chain([f64::from_bits(bits[2])])
        .collect::<Vec<_>>();
    let mut expected = bits
        .into_iter()
        .map(|v: u64| v.to_ne_bytes().to_vec())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(keys(Arc::new(Float64Array::from(values))), expected);
    let bits = [0, 0x8000_0000, 0x7fc0_0001, 0x7fc0_0002];
    let values = bits
        .into_iter()
        .map(f32::from_bits)
        .chain([f32::from_bits(bits[2])])
        .collect::<Vec<_>>();
    let mut expected = bits
        .into_iter()
        .map(|v: u32| v.to_ne_bytes().to_vec())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(keys(Arc::new(Float32Array::from(values))), expected);
}
#[test]
fn legacy_count_distinct_primitive_keys_are_native_endian_decimal_keys_are_little_endian() {
    let cases: Vec<(ArrayRef, Vec<u8>)> = vec![
        (
            Arc::new(Int8Array::from(vec![-7])),
            (-7_i8).to_ne_bytes().to_vec(),
        ),
        (
            Arc::new(Int16Array::from(vec![-1234])),
            (-1234_i16).to_ne_bytes().to_vec(),
        ),
        (
            Arc::new(Int32Array::from(vec![0x0102_0304])),
            0x0102_0304_i32.to_ne_bytes().to_vec(),
        ),
        (
            Arc::new(Int64Array::from(vec![0x0102_0304_0506_0708])),
            0x0102_0304_0506_0708_i64.to_ne_bytes().to_vec(),
        ),
        (
            Arc::new(Date32Array::from(vec![-42])),
            (-42_i32).to_ne_bytes().to_vec(),
        ),
        (Arc::new(BooleanArray::from(vec![true])), vec![1]),
        (
            Arc::new(
                Decimal128Array::from(vec![-123456_i128])
                    .with_precision_and_scale(20, 3)
                    .unwrap(),
            ),
            (-123456_i128).to_le_bytes().to_vec(),
        ),
        (
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(-123456)])
                    .with_precision_and_scale(60, 3)
                    .unwrap(),
            ),
            i256::from_i128(-123456).to_le_bytes().to_vec(),
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(keys(input), vec![expected]);
    }
}
#[test]
fn legacy_count_distinct_raw_bytes_empty_keys_and_timestamp_timezone_are_preserved() {
    for input in [
        Arc::new(StringArray::from(vec![Some(""), None, Some("a"), Some("")])) as ArrayRef,
        Arc::new(BinaryArray::from(vec![
            Some(&b""[..]),
            None,
            Some(&b"a"[..]),
            Some(&b""[..]),
        ])) as ArrayRef,
    ] {
        assert_eq!(keys(input), vec![vec![], vec![b'a']]);
    }
    for input in [
        Arc::new(TimestampSecondArray::from(vec![7]).with_timezone("Pacific/Apia")) as ArrayRef,
        Arc::new(TimestampMillisecondArray::from(vec![7]).with_timezone("UTC")) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![7])) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(vec![7])) as ArrayRef,
    ] {
        assert_eq!(keys(input), vec![7_i64.to_ne_bytes().to_vec()]);
    }
}
#[test]
fn legacy_count_distinct_list_null_element_counts_but_struct_direct_null_field_skips() {
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![None]),
        Some(vec![None]),
        None,
        Some(vec![]),
    ]));
    let mut state = LegacyState::new(
        list.data_type().clone(),
        MemTracker::new_root("legacy-distinct-list"),
    );
    state.update(&list).unwrap();
    assert_eq!(state.count(), 2);
    let fields = Fields::from(vec![Field::new("v", DataType::Int32, true)]);
    let input: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![Arc::new(Int32Array::from(vec![None, Some(1), Some(1)]))],
        None,
    ));
    let mut state = LegacyState::new(
        input.data_type().clone(),
        MemTracker::new_root("legacy-distinct-struct"),
    );
    state.update(&input).unwrap();
    assert_eq!(state.count(), 1);
    let mut expected = vec![1, 8];
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.extend_from_slice(&[1, 2]);
    expected.extend_from_slice(&1_i64.to_le_bytes());
    assert_eq!(state.keys(), vec![expected]);
}
#[test]
fn legacy_count_distinct_struct_null_skip_is_not_recursive_and_list_float32_widens() {
    let nested = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("inner", DataType::Int32, true)]),
        vec![Arc::new(Int32Array::from(vec![None]))],
        None,
    )) as ArrayRef;
    let input = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("outer", nested.data_type().clone(), true)]),
        vec![nested],
        None,
    )) as ArrayRef;
    let mut expected = vec![1, 8];
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.extend_from_slice(&[1, 8]);
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.push(0);
    assert_eq!(keys(input), vec![expected]);
    let f32value = f32::from_bits(0x7fc0_0012);
    let input = Arc::new(ListArray::from_iter_primitive::<
        arrow::datatypes::Float32Type,
        _,
        _,
    >(vec![Some(vec![Some(f32value)])])) as ArrayRef;
    let mut expected = vec![1, 10];
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.extend_from_slice(&[1, 3]);
    expected.extend_from_slice(&(f32value as f64).to_bits().to_le_bytes());
    assert_eq!(keys(input), vec![expected]);
}
#[test]
fn legacy_count_distinct_decoder_accepts_trailing_bytes_duplicate_and_empty_variable_keys() {
    let encoded = payload(&[b"", b"abc", b"abc", b"z"], b"ignored trailing bytes");
    assert_eq!(
        deserialize_set(&encoded).unwrap(),
        vec![vec![], b"abc".to_vec(), b"abc".to_vec(), b"z".to_vec()]
    );
    assert!(
        deserialize_set(&payload(&[], b"any tail"))
            .unwrap()
            .is_empty()
    );
    let mut state = LegacyState::new(
        DataType::Int64,
        MemTracker::new_root("legacy-distinct-tolerant"),
    );
    state
        .merge(&BinaryArray::from(vec![encoded.as_slice()]))
        .unwrap();
    assert_eq!(state.count(), 3);
}
#[test]
fn legacy_count_distinct_decoder_rejects_truncation_before_mutating_that_row_but_not_prior_rows() {
    for bytes in [
        vec![],
        vec![0, 0, 0],
        1_u32.to_le_bytes().to_vec(),
        payload(&[b"abc"], &[])[..10].to_vec(),
    ] {
        assert_eq!(
            deserialize_set(&bytes).unwrap_err(),
            "invalid distinct set encoding"
        );
    }
    let valid = payload(&[b"old"], &[]);
    let mut malformed = payload(&[b"prefix"], &[]);
    malformed[..4].copy_from_slice(&2_u32.to_le_bytes());
    let mut state = LegacyState::new(
        DataType::Utf8,
        MemTracker::new_root("legacy-distinct-merge-error"),
    );
    assert_eq!(
        state
            .merge(&BinaryArray::from(vec![
                valid.as_slice(),
                malformed.as_slice()
            ]))
            .unwrap_err(),
        "invalid distinct set encoding"
    );
    assert_eq!(state.keys(), vec![b"old".to_vec()]);
    state
        .merge(&BinaryArray::from(vec![
            payload(&[b"later"], &[]).as_slice(),
        ]))
        .unwrap();
    assert_eq!(state.count(), 2, "legacy state is not failure-latched");
}
#[test]
fn legacy_count_distinct_empty_and_null_final_zero_have_nonnull_binary_zero_count() {
    for input in [
        Arc::new(NullArray::new(4)) as ArrayRef,
        Arc::new(Int64Array::from(vec![None, None])) as ArrayRef,
    ] {
        let mut state = LegacyState::new(
            input.data_type().clone(),
            MemTracker::new_root("legacy-distinct-empty"),
        );
        state.update(&input).unwrap();
        assert_eq!(state.count(), 0);
        let out = state.output(true);
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(!out.is_null(0));
        assert_eq!(out.value(0), &0_u32.to_le_bytes());
    }
}
#[test]
fn legacy_count_distinct_unsupported_top_level_carrier_and_full_nested_error_remain_exact() {
    let mut state = LegacyState::new(
        DataType::UInt64,
        MemTracker::new_root("legacy-distinct-errors"),
    );
    assert_eq!(
        state
            .update(&(Arc::new(UInt64Array::from(vec![None])) as ArrayRef))
            .unwrap_err(),
        "unsupported count_distinct input type: UInt64"
    );
    assert_eq!(
        state
            .update(&(Arc::new(LargeStringArray::from(vec!["a"])) as ArrayRef))
            .unwrap_err(),
        "unsupported count_distinct input type: LargeUtf8"
    );
    let name = "x".repeat(800);
    let map_type = DataType::Map(
        Arc::new(Field::new(
            name.clone(),
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        )),
        false,
    );
    let input = arrow::array::new_empty_array(&map_type);
    let error = state.update(&input).unwrap_err();
    assert!(error.len() > 512);
    assert_eq!(
        error,
        format!("unsupported count_distinct input type: {:?}", map_type)
    );
    let child = Arc::new(StructArray::new(
        Fields::from(vec![Field::new(name, DataType::UInt64, true)]),
        vec![Arc::new(UInt64Array::from(vec![1]))],
        None,
    )) as ArrayRef;
    // Direct struct reader diagnoses the unsupported leaf, without a bounded diagnostic carrier.
    assert_eq!(
        state.update(&child).unwrap_err(),
        "unsupported scalar type: UInt64"
    );
    state
        .update(&(Arc::new(Int64Array::from(vec![1])) as ArrayRef))
        .unwrap();
    assert_eq!(state.count(), 1);
}
#[test]
fn legacy_count_distinct_allocator_duplicate_skips_reservation_and_drop_releases_all() {
    let tracker = MemTracker::new_root("legacy-distinct-memory");
    let mut set = DistinctSet::new(Arc::clone(&tracker));
    set.insert(b"abc".to_vec()).unwrap();
    let bytes = tracker.current();
    let allocations = tracker.allocated();
    let peak = tracker.peak();
    assert!(bytes > 3);
    assert_eq!(set.retained_bytes(), 0);
    set.insert(b"abc".to_vec()).unwrap();
    assert_eq!(tracker.current(), bytes);
    assert_eq!(tracker.allocated(), allocations);
    assert_eq!(tracker.peak(), peak);
    drop(set);
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.allocated(), tracker.deallocated());
}
#[test]
fn legacy_count_distinct_allocator_table_rejection_and_key_rejection_keep_exact_labels_and_order() {
    let tracker = MemTracker::new_root("legacy-distinct-table-refusal");
    tracker.install_limit_once(1).unwrap();
    let mut set = DistinctSet::new(Arc::clone(&tracker));
    assert_eq!(
        set.insert(vec![1]).unwrap_err(),
        "ResourceExhausted: reserve distinct hash set: aggregate allocation was rejected by memory tracker legacy-distinct-table-refusal or the system allocator"
    );
    assert_eq!(set.len(), 0);
    assert_eq!(tracker.current(), 0);
    assert!(tracker.peak() > 1);
    drop(set);
    let probe = MemTracker::new_root("legacy-distinct-table-probe");
    let mut set = DistinctSet::new(Arc::clone(&probe));
    set.insert(vec![]).unwrap();
    let table = probe.current();
    drop(set);
    let tracker = MemTracker::new_root("legacy-distinct-key-refusal");
    tracker.install_limit_once(table).unwrap();
    let mut set = DistinctSet::new(Arc::clone(&tracker));
    assert_eq!(
        set.insert(vec![1]).unwrap_err(),
        "ResourceExhausted: reserve aggregate byte value: aggregate allocation was rejected by memory tracker legacy-distinct-key-refusal or the system allocator"
    );
    assert_eq!(set.len(), 0);
    assert_eq!(tracker.current(), table);
    assert_eq!(tracker.peak(), table + 1);
    set.insert(vec![]).unwrap();
    assert_eq!(set.len(), 1, "legacy failure does not latch");
    drop(set);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_count_distinct_allocator_growth_charges_old_plus_replacement_peak() {
    let tracker = MemTracker::new_root("legacy-distinct-growth");
    let mut set = DistinctSet::new(Arc::clone(&tracker));
    for key in [b"a", b"b", b"c"] {
        set.insert(key.to_vec()).unwrap();
    }
    let before = tracker.current();
    let old_table = set.values.raw_table().allocation_info().1.size() as i64;
    set.insert(b"d".to_vec()).unwrap();
    let new_table = set.values.raw_table().allocation_info().1.size() as i64;
    assert!(new_table > old_table);
    assert!(tracker.peak() >= before + new_table);
    assert_eq!(tracker.current(), new_table + 4);
    drop(set);
    assert_eq!(tracker.current(), 0);
}
