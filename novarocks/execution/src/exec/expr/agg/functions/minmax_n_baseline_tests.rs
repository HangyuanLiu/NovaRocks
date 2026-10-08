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
//! Independent v1 N admission, stable sort, tolerant codec and allocation order.
//! Register as a cfg(test) child of minmax_n before production extraction.
use super::*;
use arrow::array::{
    BooleanArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
    new_null_array,
};
use arrow::datatypes::Fields;
use std::mem::MaybeUninit;
struct LegacyState {
    raw: MaybeUninit<MinMaxNState>,
    spec: AggSpec,
}
impl LegacyState {
    fn new(name: &str, value: DataType, tracker: Arc<MemTracker>) -> Self {
        let input = DataType::Struct(Fields::from(vec![
            Field::new("value", value, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let spec = MinMaxNAgg
            .build_spec_from_type(
                &AggFunction {
                    name: name.into(),
                    ..Default::default()
                },
                Some(&input),
                false,
            )
            .unwrap();
        let mut state = Self {
            raw: MaybeUninit::uninit(),
            spec,
        };
        MinMaxNAgg
            .init_state_with_tracker(&state.spec, state.raw.as_mut_ptr().cast(), Some(tracker))
            .unwrap();
        state
    }
    fn pointer(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn update(&mut self, values: ArrayRef, limits: ArrayRef) -> Result<(), String> {
        let fields = Fields::from(vec![
            Field::new("v", values.data_type().clone(), true),
            Field::new("n", limits.data_type().clone(), true),
        ]);
        let input: ArrayRef = Arc::new(StructArray::new(fields, vec![values, limits], None));
        let pointers = vec![self.pointer(); input.len()];
        MinMaxNAgg.update_batch(&self.spec, 0, &pointers, &AggInputView::Any(&input))
    }
    fn merge(&mut self, input: ArrayRef) -> Result<(), String> {
        let pointers = vec![self.pointer(); input.len()];
        MinMaxNAgg.merge_batch(&self.spec, 0, &pointers, &AggInputView::Any(&input))
    }
    fn output(&mut self, partial: bool) -> Result<ArrayRef, String> {
        let pointer = self.pointer();
        MinMaxNAgg.build_array(&self.spec, 0, &[pointer], partial)
    }
    fn initialized(&self) -> bool {
        unsafe { self.raw.assume_init_ref() }.initialized
    }
    fn limit(&self) -> usize {
        unsafe { self.raw.assume_init_ref() }.limit
    }
}
impl Drop for LegacyState {
    fn drop(&mut self) {
        MinMaxNAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
fn ints(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn limits(n: i64, rows: usize) -> ArrayRef {
    ints(vec![Some(n); rows])
}
fn int_output(state: &mut LegacyState) -> Vec<Option<i64>> {
    let out = state.output(false).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!list.is_null(0));
    list.value(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn wire(limit: u32, values: &[i64], tail: &[u8]) -> Vec<u8> {
    let mut bytes = limit.to_le_bytes().to_vec();
    bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        bytes.push(2);
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.extend_from_slice(tail);
    bytes
}
fn binary(bytes: &[u8]) -> ArrayRef {
    Arc::new(BinaryArray::from(vec![Some(bytes)]))
}
#[test]
fn legacy_n_empty_and_all_null_values_are_non_null_empty_lists_and_initialized_wire() {
    for name in ["min_n", "max_n"] {
        let mut state = LegacyState::new(
            name,
            DataType::Int64,
            MemTracker::new_root("legacy-n-empty"),
        );
        assert_eq!(int_output(&mut state), Vec::<Option<i64>>::new());
        assert!(!state.initialized());
        let out = state.output(true).unwrap();
        let bytes = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(!bytes.is_null(0));
        assert_eq!(bytes.value(0), [0; 8]);
        state
            .update(new_null_array(&DataType::Int64, 2), limits(2, 2))
            .unwrap();
        assert!(state.initialized());
        assert_eq!(state.limit(), 2);
        assert_eq!(int_output(&mut state), Vec::<Option<i64>>::new());
        let out = state.output(true).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<BinaryArray>().unwrap().value(0),
            wire(2, &[], &[])
        );
    }
}
#[test]
fn legacy_n_limit_admission_precedes_value_null_and_mismatch_keeps_original_message() {
    for name in ["min_n", "max_n"] {
        for (limit, error) in [
            (None, "min_n/max_n limit cannot be null"),
            (Some(0), "min_n/max_n limit must be positive, got 0"),
            (Some(-1), "min_n/max_n limit must be positive, got -1"),
        ] {
            let mut state = LegacyState::new(
                name,
                DataType::Int64,
                MemTracker::new_root("legacy-n-limit"),
            );
            assert_eq!(
                state
                    .update(new_null_array(&DataType::Int64, 1), ints(vec![limit]))
                    .unwrap_err(),
                error
            );
            assert!(!state.initialized());
        }
        let mut state = LegacyState::new(
            name,
            DataType::Int64,
            MemTracker::new_root("legacy-n-null-mismatch"),
        );
        state
            .update(new_null_array(&DataType::Int64, 1), limits(2, 1))
            .unwrap();
        assert_eq!(
            state
                .update(new_null_array(&DataType::Int64, 1), limits(3, 1))
                .unwrap_err(),
            "min_n/max_n limit mismatch while merging states: 2 vs 3"
        );
        assert_eq!(state.limit(), 2);
        assert_eq!(
            state
                .update(
                    new_null_array(&DataType::Int64, 1),
                    Arc::new(BooleanArray::from(vec![true]))
                )
                .unwrap_err(),
            "min_n/max_n limit must be integer"
        );
    }
}
#[test]
fn legacy_n_all_signed_limit_carriers_and_ascending_final_truncation_keep_duplicates() {
    for name in ["min_n", "max_n"] {
        for ns in [
            Arc::new(Int8Array::from(vec![3; 4])) as ArrayRef,
            Arc::new(Int16Array::from(vec![3; 4])),
            Arc::new(Int32Array::from(vec![3; 4])),
            limits(3, 4),
        ] {
            let mut state = LegacyState::new(
                name,
                DataType::Int64,
                MemTracker::new_root("legacy-n-carriers"),
            );
            state
                .update(ints(vec![Some(3), Some(1), Some(3), Some(2)]), ns)
                .unwrap();
            assert_eq!(
                int_output(&mut state),
                if name == "min_n" {
                    vec![Some(1), Some(2), Some(3)]
                } else {
                    vec![Some(2), Some(3), Some(3)]
                }
            );
        }
    }
}
#[test]
fn legacy_n_stable_equal_signed_zero_and_nan_equal_comparison_keep_exact_order() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0041);
    for name in ["min_n", "max_n"] {
        let mut zero = LegacyState::new(
            name,
            DataType::Float64,
            MemTracker::new_root("legacy-n-zero"),
        );
        zero.update(Arc::new(Float64Array::from(vec![-0.0, 0.0])), limits(2, 2))
            .unwrap();
        let out = zero.output(false).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap().value(0);
        let bits = out
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>();
        assert_eq!(
            bits,
            if name == "min_n" {
                vec![(-0.0f64).to_bits(), 0.0f64.to_bits()]
            } else {
                vec![0.0f64.to_bits(), (-0.0f64).to_bits()]
            }
        );
        let mut state = LegacyState::new(
            name,
            DataType::Float64,
            MemTracker::new_root("legacy-n-nan"),
        );
        state
            .update(
                Arc::new(Float64Array::from(vec![nan, 1.0, 2.0])),
                limits(2, 3),
            )
            .unwrap();
        let out = state.output(false).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap().value(0);
        let bits = out
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>();
        assert_eq!(
            bits,
            if name == "min_n" {
                vec![nan.to_bits(), 1.0f64.to_bits()]
            } else {
                vec![2.0f64.to_bits(), nan.to_bits()]
            }
        );
    }
}
#[test]
fn legacy_n_merge_accepts_trailing_bytes_four_carriers_and_zero_limit_after_full_decode() {
    for name in ["min_n", "max_n"] {
        let bytes = wire(2, &[3, 1, 2], b"tolerated tail");
        for input in [
            binary(&bytes),
            Arc::new(LargeBinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                std::str::from_utf8(&bytes).unwrap(),
            ])),
            Arc::new(LargeStringArray::from(vec![
                std::str::from_utf8(&bytes).unwrap(),
            ])),
        ] {
            let mut state =
                LegacyState::new(name, DataType::Int64, MemTracker::new_root("legacy-n-tail"));
            state.merge(input).unwrap();
            assert_eq!(
                int_output(&mut state),
                if name == "min_n" {
                    vec![Some(1), Some(2)]
                } else {
                    vec![Some(2), Some(3)]
                }
            );
        }
        let mut state = LegacyState::new(
            name,
            DataType::Int64,
            MemTracker::new_root("legacy-n-zero-merge"),
        );
        state.merge(binary(&wire(0, &[9], &[]))).unwrap();
        assert!(!state.initialized());
        assert_eq!(int_output(&mut state), Vec::<Option<i64>>::new());
        let mut malformed = 0u32.to_le_bytes().to_vec();
        malformed.extend_from_slice(&1u32.to_le_bytes());
        assert_eq!(
            state.merge(binary(&malformed)).unwrap_err(),
            "min_n/max_n decode tag overflow: pos=8 need=1 len=8"
        );
    }
}
#[test]
fn legacy_n_decode_full_errors_and_prior_values_survive_partial_graph_failure() {
    let tracker = MemTracker::new_root("legacy-n-decode");
    let mut state = LegacyState::new("min_n", DataType::Int64, tracker.clone());
    state.update(ints(vec![Some(7)]), limits(2, 1)).unwrap();
    let stable = tracker.current();
    for (bytes, error) in [
        (
            vec![],
            "min_n/max_n decode limit overflow: pos=0 need=4 len=0",
        ),
        (
            2u32.to_le_bytes().to_vec(),
            "min_n/max_n decode count overflow: pos=4 need=4 len=4",
        ),
        (
            {
                let mut out = wire(2, &[], &[]);
                out[4..8].copy_from_slice(&1u32.to_le_bytes());
                out.push(255);
                out
            },
            "min_n/max_n decode unknown tag 255",
        ),
    ] {
        assert_eq!(state.merge(binary(&bytes)).unwrap_err(), error);
        assert_eq!(int_output(&mut state), vec![Some(7)]);
        assert_eq!(tracker.current(), stable);
    }
    // UTF8 validation precedes byte allocation; decoded outer Vec is temporary.
    let mut bytes = wire(2, &[], &[]);
    bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
    bytes.push(4);
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.push(255);
    assert_eq!(
        state.merge(binary(&bytes)).unwrap_err(),
        "min_n/max_n utf8 decode failed: invalid utf-8 sequence of 1 bytes from index 0"
    );
    assert_eq!(tracker.current(), stable);
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_n_initializes_limit_before_retained_allocation_failure_and_can_retry_same_limit() {
    let tracker = MemTracker::new_root("legacy-n-value-oom");
    tracker.install_limit_once(1).unwrap();
    let mut state = LegacyState::new("min_n", DataType::Utf8, tracker.clone());
    assert_eq!(
        state
            .update(Arc::new(StringArray::from(vec!["ab"])), limits(2, 1))
            .unwrap_err(),
        "ResourceExhausted: reserve aggregate byte value: aggregate allocation was rejected by memory tracker legacy-n-value-oom or the system allocator"
    );
    assert!(state.initialized());
    assert_eq!(state.limit(), 2);
    assert_eq!(tracker.current(), 0);
    assert_eq!(
        state
            .update(new_null_array(&DataType::Utf8, 1), limits(3, 1))
            .unwrap_err(),
        "min_n/max_n limit mismatch while merging states: 2 vs 3"
    );
    state
        .update(new_null_array(&DataType::Utf8, 1), limits(2, 1))
        .unwrap();
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_n_nested_and_binary_single_output_exists_but_original_codec_refuses() {
    for (ty, value, marker) in [
        (
            DataType::Binary,
            AggScalarValue::Binary(vec![1, 2, 3]),
            "Binary",
        ),
        (
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            AggScalarValue::List(vec![Some(AggScalarValue::Int64(9)), None]),
            "List",
        ),
    ] {
        let tracker = MemTracker::new_root("legacy-n-codec-gap");
        let mut state = LegacyState::new("min_n", ty.clone(), tracker.clone());
        let values = build_scalar_array(&ty, vec![Some(value)]).unwrap();
        state.update(values, limits(2, 1)).unwrap();
        assert!(!state.output(false).unwrap().is_null(0));
        let error = state.output(true).unwrap_err();
        assert!(error.starts_with("min_n/max_n does not support serialized value "));
        assert!(error.contains(marker));
        drop(state);
        assert_eq!(tracker.current(), 0);
    }
}
