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
//! Independent raw original COUNT receipts; mount as a child of count.rs.
use super::*;
use arrow::array::{
    Array, BooleanArray, DictionaryArray, Int8Array, Int16Array, Int32Array, Int64Array, NullArray,
    StringArray,
};
use arrow::datatypes::Int8Type;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct Legacy {
    state: i64,
    spec: AggSpec,
}
impl Legacy {
    fn new(ty: Option<&DataType>) -> Self {
        let spec = CountAgg
            .build_spec_from_type(
                &AggFunction {
                    name: "count".into(),
                    ..Default::default()
                },
                ty,
                false,
            )
            .unwrap();
        let mut value = Self { state: -9, spec };
        CountAgg.init_state(&value.spec, (&mut value.state as *mut i64).cast());
        value
    }
    fn pointers(&mut self, n: usize) -> Vec<AggStatePtr> {
        vec![(&mut self.state as *mut i64) as AggStatePtr; n]
    }
    fn update(&mut self, input: &ArrayRef) {
        let p = self.pointers(input.len());
        let holder = Some(input.clone());
        let view = CountAgg.build_input_view(&self.spec, &holder).unwrap();
        CountAgg.update_batch(&self.spec, 0, &p, &view).unwrap();
    }
    fn output(&mut self, partial: bool) -> i64 {
        let p = self.pointers(1);
        let result = CountAgg.build_array(&self.spec, 0, &p, partial).unwrap();
        assert_eq!(result.data_type(), &DataType::Int64);
        assert_eq!(result.null_count(), 0);
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }
}
#[test]
fn legacy_count_original_star_empty_expr_null_and_ignored_payload_types() {
    let mut star = Legacy::new(None);
    let p = star.pointers(3);
    CountAgg
        .update_batch(&star.spec, 0, &p, &AggInputView::None)
        .unwrap();
    assert_eq!(star.output(false), 3);
    assert_eq!(star.output(true), 3);
    let empty = CountAgg.build_array(&star.spec, 0, &[], false).unwrap();
    assert_eq!(empty.len(), 0);
    for input in [
        Arc::new(Int64Array::from(vec![Some(2), None, Some(-9)])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("é\0"), None, Some("")])) as ArrayRef,
    ] {
        let mut state = Legacy::new(Some(input.data_type()));
        state.update(&input);
        assert_eq!(state.output(false), 2);
        assert_eq!(state.output(true), 2);
    }
    let null: ArrayRef = Arc::new(NullArray::new(3));
    assert_eq!(null.null_count(), 0);
    assert_eq!(null.logical_null_count(), 3);
    assert!(!null.is_null(0));
    let mut state = Legacy::new(Some(&DataType::Null));
    state.update(&null);
    assert_eq!(state.output(false), 3);
}
#[test]
fn legacy_count_original_dictionary_value_null_is_not_key_null() {
    let input: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![Some("ok"), None])),
        )
        .unwrap(),
    );
    assert_eq!(input.null_count(), 1);
    let mut state = Legacy::new(Some(input.data_type()));
    state.update(&input);
    assert_eq!(state.output(false), 4);
    let sliced = input.slice(1, 3);
    let mut state = Legacy::new(Some(sliced.data_type()));
    state.update(&sliced);
    assert_eq!(state.output(false), 2);
}
#[test]
fn legacy_count_original_merge_all_signed_carriers_and_nulls() {
    for input in [
        Arc::new(Int8Array::from(vec![Some(-2), None, Some(5)])) as ArrayRef,
        Arc::new(Int16Array::from(vec![Some(-2), None, Some(5)])) as ArrayRef,
        Arc::new(Int32Array::from(vec![Some(-2), None, Some(5)])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(-2), None, Some(5)])) as ArrayRef,
    ] {
        let mut state = Legacy::new(Some(&DataType::Int64));
        let p = state.pointers(input.len());
        let holder = Some(input);
        let view = CountAgg.build_merge_view(&state.spec, &holder).unwrap();
        CountAgg.merge_batch(&state.spec, 0, &p, &view).unwrap();
        assert_eq!(state.output(false), 3);
    }
}
#[test]
fn legacy_count_original_missing_mismatch_and_complete_unsupported_type_error() {
    let mut state = Legacy::new(Some(&DataType::Utf8));
    assert_eq!(
        CountAgg.build_input_view(&state.spec, &None).err().unwrap(),
        "count input missing"
    );
    assert_eq!(
        CountAgg.build_merge_view(&state.spec, &None).err().unwrap(),
        "count input missing"
    );
    let boolean = BooleanArray::from(vec![true]);
    let p = state.pointers(1);
    assert_eq!(
        CountAgg
            .update_batch(&state.spec, 0, &p, &AggInputView::Bool(&boolean))
            .unwrap_err(),
        "count batch input type mismatch"
    );
    assert_eq!(
        CountAgg
            .merge_batch(&state.spec, 0, &p, &AggInputView::None)
            .unwrap_err(),
        "count merge batch input type mismatch"
    );
    let input = Some(Arc::new(StringArray::from(vec!["x"])) as ArrayRef);
    assert_eq!(
        CountAgg
            .build_merge_view(&state.spec, &input)
            .err()
            .unwrap(),
        "unsupported int input type: Utf8"
    );
}
#[test]
fn legacy_count_original_offset_state_and_update_overflow_prefix() {
    let spec = count_fallback(true);
    let mut states = [[111_i64, 0, 222], [333, i64::MAX, 444]];
    let pointers = states
        .iter_mut()
        .map(|s| s.as_mut_ptr() as AggStatePtr)
        .collect::<Vec<_>>();
    let result = catch_unwind(AssertUnwindSafe(|| {
        CountAgg.update_batch(&spec, 8, &pointers, &AggInputView::None)
    }));
    assert_eq!(states[0], [111, 1, 222]);
    if cfg!(debug_assertions) {
        let panic = result.expect_err("original overflow");
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap();
        assert_eq!(message, "attempt to add with overflow");
        assert_eq!(states[1], [333, i64::MAX, 444]);
    } else {
        result.unwrap().unwrap();
        assert_eq!(states[1], [333, i64::MIN, 444]);
    }
}
#[test]
fn legacy_count_original_signed_merge_overflow_is_original_arithmetic() {
    let mut state = Legacy::new(Some(&DataType::Int64));
    state.state = i64::MIN;
    let input = Some(Arc::new(Int64Array::from(vec![-1])) as ArrayRef);
    let view = CountAgg.build_merge_view(&state.spec, &input).unwrap();
    let p = state.pointers(1);
    let result = catch_unwind(AssertUnwindSafe(|| {
        CountAgg.merge_batch(&state.spec, 0, &p, &view)
    }));
    if cfg!(debug_assertions) {
        let panic = result.expect_err("original merge overflow");
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap();
        assert_eq!(message, "attempt to add with overflow");
        assert_eq!(state.state, i64::MIN);
    } else {
        result.unwrap().unwrap();
        assert_eq!(state.state, i64::MAX);
    }
}

#[test]
fn legacy_count_original_long_unsupported_merge_type_preserves_full_text() {
    let zone = "original-zone-".repeat(80);
    let input: ArrayRef = Arc::new(
        arrow::array::TimestampMicrosecondArray::from(vec![1]).with_timezone(zone.clone()),
    );
    let state = Legacy::new(Some(&DataType::Int64));
    let holder = Some(input);
    let actual = CountAgg
        .build_merge_view(&state.spec, &holder)
        .err()
        .unwrap();
    let expected = format!(
        "unsupported int input type: {:?}",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(zone.into()))
    );
    assert_eq!(actual, expected);
    assert!(actual.len() > 512);
}
