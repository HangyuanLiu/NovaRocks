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

//! Independent v1 any_value semantics before shared scalar extraction.
//! Register as a cfg(test) child of agg::functions::any_value.
use super::*;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, StructArray, UInt64Array,
    new_empty_array, new_null_array,
};
use arrow::datatypes::{Field, Fields};
use std::mem::MaybeUninit;

struct LegacyState {
    raw: MaybeUninit<AnyValueState>,
    spec: AggSpec,
}
impl LegacyState {
    fn new(ty: DataType, tracker: Arc<MemTracker>) -> Self {
        let spec = AnyValueAgg
            .build_spec_from_type(
                &AggFunction {
                    name: "any_value".into(),
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
        AnyValueAgg
            .init_state_with_tracker(&state.spec, state.raw.as_mut_ptr().cast(), Some(tracker))
            .unwrap();
        state
    }
    fn pointer(&mut self) -> AggStatePtr {
        self.raw.as_mut_ptr() as AggStatePtr
    }
    fn update(&mut self, input: &ArrayRef, merge: bool) -> Result<(), String> {
        let pointers = vec![self.pointer(); input.len()];
        let view = AggInputView::Any(input);
        if merge {
            AnyValueAgg.merge_batch(&self.spec, 0, &pointers, &view)
        } else {
            AnyValueAgg.update_batch(&self.spec, 0, &pointers, &view)
        }
    }
    fn output(&mut self, intermediate: bool) -> ArrayRef {
        let pointer = self.pointer();
        AnyValueAgg
            .build_array(&self.spec, 0, &[pointer], intermediate)
            .unwrap()
    }
}
impl Drop for LegacyState {
    fn drop(&mut self) {
        AnyValueAgg.drop_state(&self.spec, self.raw.as_mut_ptr().cast());
    }
}
#[test]
fn legacy_any_value_first_non_null_then_skips_all_later_reads_and_allocations() {
    let tracker = MemTracker::new_root("legacy-any-value-first");
    let mut state = LegacyState::new(DataType::Utf8, Arc::clone(&tracker));
    let first: ArrayRef = Arc::new(StringArray::from(vec![None, Some("first"), Some("second")]));
    state.update(&first, false).unwrap();
    assert_eq!(tracker.current(), 5);
    let unsupported: ArrayRef = Arc::new(UInt64Array::from(vec![1, 2, 3]));
    state.update(&unsupported, false).unwrap();
    assert_eq!(
        tracker.current(),
        5,
        "populated state must not read or allocate a later row"
    );
    for partial in [false, true] {
        let output = state.output(partial);
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "first"
        );
    }
    drop(state);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_any_value_empty_and_all_null_keep_nullable_result_for_nonnullable_input() {
    for ty in [
        DataType::Boolean,
        DataType::Int64,
        DataType::Utf8,
        DataType::Decimal256(60, 2),
    ] {
        for input in [new_empty_array(&ty), new_null_array(&ty, 513)] {
            let mut state =
                LegacyState::new(ty.clone(), MemTracker::new_root("legacy-any-value-empty"));
            state.update(&input, false).unwrap();
            for partial in [false, true] {
                let output = state.output(partial);
                assert_eq!(output.data_type(), &ty);
                assert_eq!(output.len(), 1);
                assert!(output.is_null(0));
            }
        }
    }
    use novarocks_functions::{
        EngineFunctionCatalog, FunctionArgument, FunctionBindingRequest, FunctionKind,
        FunctionResultType,
    };
    let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let arguments = [FunctionArgument::Value {
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        constant: None,
    }];
    let binding = catalog
        .resolve_bound_user(
            "any_value",
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            &crate::exec::expr::pure_differential::HarnessControl,
        )
        .unwrap();
    let FunctionResultType::Scalar(output) = binding.selected.result_type else {
        panic!("scalar aggregate result")
    };
    assert!(
        output.nullable,
        "the resolver overrides result nullability after copying the input type"
    );
}
#[test]
fn legacy_any_value_partial_final_merge_keeps_first_non_null_in_merge_order() {
    let tracker = MemTracker::new_root("legacy-any-value-merge");
    for (left, right, expected) in [(10, 20, 10), (20, 10, 20)] {
        let mut state = LegacyState::new(DataType::Int64, Arc::clone(&tracker));
        for value in [None, Some(left), Some(right)] {
            let input: ArrayRef = Arc::new(Int64Array::from(vec![value]));
            state.update(&input, true).unwrap();
        }
        assert_eq!(
            state
                .output(false)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    assert_eq!(tracker.current(), 0);
}
#[test]
fn legacy_any_value_preserves_first_float_payload_signed_zero_and_parent_struct_null() {
    for value in [
        -0.0,
        0.0,
        f64::from_bits(0x7ff8_0000_0000_0123),
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        let mut state = LegacyState::new(
            DataType::Float64,
            MemTracker::new_root("legacy-any-value-float"),
        );
        let input: ArrayRef = Arc::new(Float64Array::from(vec![None, Some(value), Some(2.0)]));
        state.update(&input, false).unwrap();
        let output = state.output(false);
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            value.to_bits()
        );
    }
    let fields = Fields::from(vec![Field::new("child", DataType::Boolean, true)]);
    let input: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::new(BooleanArray::from(vec![None, Some(true)]))],
        None,
    ));
    let mut state = LegacyState::new(
        DataType::Struct(fields),
        MemTracker::new_root("legacy-any-value-struct"),
    );
    state.update(&input, false).unwrap();
    let output = state.output(false);
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(
        !output.is_null(0),
        "nonnull parent with all-null children is a value"
    );
    assert!(output.column(0).is_null(0));
}
#[test]
fn legacy_any_value_unsupported_empty_state_keeps_full_error_and_rollback() {
    let tracker = MemTracker::new_root("legacy-any-value-unsupported");
    let mut state = LegacyState::new(DataType::UInt64, Arc::clone(&tracker));
    let input: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
    assert_eq!(
        state.update(&input, false).unwrap_err(),
        "unsupported tracked scalar type: UInt64"
    );
    assert_eq!(tracker.current(), 0);
    // The legacy state is unchanged: a later legal carrier may still fill it.
    let input: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    state.update(&input, false).unwrap();
    let unsupported: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
    state.update(&unsupported, false).unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn legacy_any_value_nested_allocation_refusal_releases_partial_value_and_keeps_state_empty() {
    let fields = Fields::from(vec![
        Field::new("first", DataType::Utf8, true),
        Field::new("second", DataType::Utf8, true),
    ]);
    let root_bytes = 2 * std::mem::size_of::<Option<TrackedAggScalarValue>>();
    let tracker = MemTracker::new_root("legacy-any-value-nested-oom");
    tracker.install_limit_once((root_bytes + 1) as i64).unwrap();
    let mut state = LegacyState::new(DataType::Struct(fields.clone()), Arc::clone(&tracker));
    let rejected: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("a")])),
            Arc::new(StringArray::from(vec![Some("too-large")])),
        ],
        None,
    ));
    assert_eq!(
        state.update(&rejected, false).unwrap_err(),
        "ResourceExhausted: reserve aggregate byte value: aggregate allocation was rejected by memory tracker legacy-any-value-nested-oom or the system allocator"
    );
    assert_eq!(tracker.current(), 0);
    assert!(state.output(false).is_null(0));
    let accepted: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(StringArray::from(vec![Some("a")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
        None,
    ));
    state.update(&accepted, false).unwrap();
    assert_eq!(tracker.current(), (root_bytes + 1) as i64);
    let output = state.output(false);
    assert!(!output.is_null(0));
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    assert!(output.column(1).is_null(0));
    drop(state);
    assert_eq!(tracker.current(), 0);
}
