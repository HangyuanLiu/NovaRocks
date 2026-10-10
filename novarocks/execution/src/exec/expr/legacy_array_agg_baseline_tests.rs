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
//! Independent raw pre-extraction ARRAY aggregate oracles; mounted under original array_agg.
use super::*;
use arrow::array::{Decimal256Array, Float64Array, Int64Array, ListArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, i256};
use std::mem::MaybeUninit;
fn list_type(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item, true)))
}
fn function(
    name: &str,
    item: DataType,
    intermediate: DataType,
    output: DataType,
    ascending: Vec<bool>,
    nulls: Vec<bool>,
) -> AggFunction {
    AggFunction {
        name: name.to_owned(),
        types: Some(crate::exec::node::aggregate::AggTypeSignature {
            input_arg_type: Some(item),
            intermediate_type: Some(intermediate),
            output_type: Some(output),
        }),
        order: crate::exec::node::aggregate::AggOrderSpec {
            is_distinct: false,
            is_asc_order: ascending,
            nulls_first: nulls,
            ..Default::default()
        },
        ..Default::default()
    }
}
struct RawState {
    spec: AggSpec,
    state: Box<MaybeUninit<ArrayAggState>>,
    tracker: Arc<MemTracker>,
}
impl RawState {
    fn new(function: &AggFunction, input: &DataType, merge: bool) -> Self {
        let spec = ArrayAggAgg
            .build_spec_from_type(function, Some(input), merge)
            .unwrap();
        let mut state = Box::new(MaybeUninit::<ArrayAggState>::uninit());
        let tracker = MemTracker::new_root("array-original-baseline");
        ArrayAggAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(tracker.clone()))
            .unwrap();
        Self {
            spec,
            state,
            tracker,
        }
    }
    fn input(&mut self, array: &ArrayRef, merge: bool) -> Result<(), String> {
        let ptr = self.state.as_mut_ptr() as AggStatePtr;
        let ptrs = vec![ptr; array.len()];
        if merge {
            ArrayAggAgg.merge_batch(&self.spec, 0, &ptrs, &AggInputView::Any(array))
        } else {
            ArrayAggAgg.update_batch(&self.spec, 0, &ptrs, &AggInputView::Any(array))
        }
    }
    fn emit(&mut self, intermediate: bool) -> Result<ArrayRef, String> {
        ArrayAggAgg.build_array(
            &self.spec,
            0,
            &[self.state.as_mut_ptr() as AggStatePtr],
            intermediate,
        )
    }
}
impl Drop for RawState {
    fn drop(&mut self) {
        ArrayAggAgg.drop_state(&self.spec, self.state.as_mut_ptr().cast());
        assert_eq!(self.tracker.current(), 0);
    }
}
fn ints(array: &ArrayRef) -> Vec<Option<i64>> {
    let list = array.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.len(), 1);
    assert!(!list.is_null(0));
    list.value(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn ordinary(name: &str, ty: DataType) -> AggFunction {
    function(
        name,
        ty.clone(),
        list_type(ty.clone()),
        list_type(ty),
        vec![],
        vec![],
    )
}
#[test]
fn legacy_array_family_baseline_collect_null_empty_and_distinct_first_survivor() {
    for (name, expected) in [
        ("array_agg", vec![Some(2), None, Some(2), Some(1), None]),
        ("array_agg_distinct", vec![Some(2), None, Some(1)]),
    ] {
        let function = ordinary(name, DataType::Int64);
        let input = Arc::new(Int64Array::from(vec![
            Some(2),
            None,
            Some(2),
            Some(1),
            None,
        ])) as ArrayRef;
        let mut state = RawState::new(&function, input.data_type(), false);
        state.input(&input, false).unwrap();
        assert_eq!(ints(&state.emit(false).unwrap()), expected);
        let mut empty = RawState::new(&function, &DataType::Int64, false);
        assert!(ints(&empty.emit(false).unwrap()).is_empty());
    }
}
#[test]
fn legacy_array_family_baseline_distinct_nan_canonical_but_signed_zero_distinct() {
    let input = Arc::new(Float64Array::from(vec![
        Some(f64::from_bits(0x7ff8000000000001)),
        Some(f64::from_bits(0x7ff8000000000002)),
        Some(-0.0),
        Some(0.0),
        None,
        None,
    ])) as ArrayRef;
    let mut state = RawState::new(
        &ordinary("array_agg_distinct", DataType::Float64),
        input.data_type(),
        false,
    );
    state.input(&input, false).unwrap();
    let output = state.emit(false).unwrap();
    let list = output
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let values = list.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.len(), 4);
    assert_eq!(values.value(0).to_bits(), 0x7ff8000000000001);
    assert_eq!(values.value(1).to_bits(), (-0.0f64).to_bits());
    assert_eq!(values.value(2).to_bits(), 0.0f64.to_bits());
    assert!(values.is_null(3));
}
#[test]
fn legacy_array_family_baseline_unique_flattens_lists_and_skips_null_parent() {
    let input = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0, 3, 4, 6].into()),
        Arc::new(Int64Array::from(vec![
            Some(2),
            None,
            Some(2),
            Some(99),
            Some(1),
            Some(2),
        ])),
        Some(NullBuffer::from(vec![true, false, true])),
    )) as ArrayRef;
    let mut state = RawState::new(
        &ordinary("array_unique_agg", DataType::Int64),
        input.data_type(),
        false,
    );
    state.input(&input, false).unwrap();
    assert_eq!(
        ints(&state.emit(false).unwrap()),
        vec![Some(2), None, Some(1)]
    );
    let intermediate = state.emit(true).unwrap();
    let mut merged = RawState::new(
        &ordinary("array_unique_agg", DataType::Int64),
        intermediate.data_type(),
        true,
    );
    merged.input(&intermediate, true).unwrap();
    assert_eq!(
        ints(&merged.emit(false).unwrap()),
        vec![Some(2), None, Some(1)]
    );
}
#[test]
fn legacy_array_family_baseline_ordered_distinct_three_phase_first_sorted_survivor() {
    let value = Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(2), None])) as ArrayRef;
    let key = Arc::new(Int64Array::from(vec![Some(9), Some(3), Some(1), None])) as ArrayRef;
    let fields = Fields::from(vec![
        Field::new("v", DataType::Int64, true),
        Field::new("k", DataType::Int64, true),
    ]);
    let input = Arc::new(StructArray::new(fields, vec![value, key], None)) as ArrayRef;
    let intermediate = DataType::Struct(Fields::from(vec![
        Field::new("c0", list_type(DataType::Int64), true),
        Field::new("c1", list_type(DataType::Int64), true),
    ]));
    let function = function(
        "array_agg_distinct",
        DataType::Int64,
        intermediate,
        list_type(DataType::Int64),
        vec![true],
        vec![false],
    );
    let mut partial = RawState::new(&function, input.data_type(), false);
    partial.input(&input, false).unwrap();
    let payload = partial.emit(true).unwrap();
    let mut intermediate = RawState::new(&function, payload.data_type(), true);
    intermediate.input(&payload, true).unwrap();
    let payload = intermediate.emit(true).unwrap();
    let mut final_state = RawState::new(&function, payload.data_type(), true);
    final_state.input(&payload, true).unwrap();
    assert_eq!(
        ints(&final_state.emit(false).unwrap()),
        vec![Some(2), Some(1), None]
    );
}
#[test]
fn legacy_array_family_baseline_unordered_nan_order_error_text_remains_complete() {
    let values = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let keys = Arc::new(Float64Array::from(vec![f64::NAN, 1.0])) as ArrayRef;
    let input = Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("k", DataType::Float64, false),
        ]),
        vec![values, keys],
        None,
    )) as ArrayRef;
    let intermediate = DataType::Struct(Fields::from(vec![
        Field::new("c0", list_type(DataType::Int64), true),
        Field::new("c1", list_type(DataType::Float64), true),
    ]));
    let function = function(
        "array_agg",
        DataType::Int64,
        intermediate,
        list_type(DataType::Int64),
        vec![true],
        vec![true],
    );
    let mut state = RawState::new(&function, input.data_type(), false);
    state.input(&input, false).unwrap();
    assert_eq!(
        state.emit(false).unwrap_err(),
        "float comparison is not ordered"
    );
}
#[test]
fn legacy_array_family_baseline_decimal256_output_rejects_original_scalar_variant() {
    let input = Arc::new(
        Decimal256Array::from(vec![i256::from_i128(1)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    ) as ArrayRef;
    let mut state = RawState::new(
        &ordinary("array_agg", input.data_type().clone()),
        input.data_type(),
        false,
    );
    state.input(&input, false).unwrap();
    assert_eq!(
        state.emit(false).unwrap_err(),
        "array_agg tracked scalar type is not supported by its output ABI"
    );
}
struct Compile;
impl novarocks_type_contract::PureCompileControl for Compile {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
#[test]
fn legacy_array_family_baseline_unique_scalar_binding_drift_is_not_repaired() {
    use novarocks_functions::{
        FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionValueType,
    };
    let args = [FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Int64, true),
        constant: None,
    }];
    let bound = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog()
        .resolve_bound_user(
            "array_unique_agg",
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            &Compile,
        )
        .unwrap();
    let selected =
        novarocks_functions::builtin::catalogue::resolved_aggregate_signature_from_binding(bound)
            .unwrap();
    assert_eq!(selected.output_type, DataType::Int64);
    assert_eq!(selected.intermediate_type, DataType::Int64);
    let function = function(
        "array_unique_agg",
        DataType::Int64,
        selected.intermediate_type,
        selected.output_type,
        vec![],
        vec![],
    );
    let input = Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef;
    let mut state = RawState::new(&function, input.data_type(), false);
    state.input(&input, false).unwrap();
    assert_eq!(
        state.emit(true).unwrap_err(),
        "array_agg target type must be List or Struct(List...), got Int64"
    );
    assert_eq!(ints(&state.emit(false).unwrap()), vec![Some(1), Some(2)]);
}
