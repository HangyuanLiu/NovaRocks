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
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, StringBuilder, StructArray};
use arrow::datatypes::DataType;

use crate::exec::expr::agg::AggregateAllocator;
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use super::super::*;
use super::AggregateFunction;
use std::sync::Arc;

pub(super) struct MannWhitneyUTestAgg;

type MannWhitneyState =
    novarocks_functions::builtin::aggregate_mann_whitney_core::MannWhitneyState<AggregateAllocator>;
use novarocks_functions::aggregate_scalar::ScalarWork;
use novarocks_functions::builtin::aggregate_mann_whitney_core::MannFailure;
use novarocks_functions::builtin::aggregate_mann_whitney_core::MannWhitneyInput;

fn mann_failure(error: MannFailure) -> crate::runtime::fragment::ExecutionFailure {
    error.into_kernel_failure().into()
}

impl AggregateFunction for MannWhitneyUTestAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        let sig = super::super::agg_type_signature(func).ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "mann_whitney_u_test type signature missing",
            )
        })?;
        let output_type = sig.output_type.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "mann_whitney_u_test output type missing",
            )
        })?;
        let intermediate_type = sig.intermediate_type.as_ref().cloned().ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "mann_whitney_u_test intermediate type missing",
            )
        })?;

        if input_is_intermediate {
            return Ok(AggSpec {
                kind: AggKind::MannWhitneyUTest,
                output_type: output_type.clone(),
                intermediate_type,
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        let input_type = input_type.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "mann_whitney_u_test input type missing",
            )
        })?;
        let DataType::Struct(fields) = input_type else {
            return Err(
                crate::exec::expr::agg::functions::aggregate_binding_contract(
                    "mann_whitney_u_test expects struct input",
                ),
            );
        };
        if !(fields.len() == 2 || fields.len() == 3 || fields.len() == 4) {
            return Err(
                crate::exec::expr::agg::functions::aggregate_binding_contract(
                    "mann_whitney_u_test expects 2 to 4 arguments",
                ),
            );
        }

        Ok(AggSpec {
            kind: AggKind::MannWhitneyUTest,
            output_type: output_type.clone(),
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, _kind: &AggKind) -> (usize, usize) {
        (
            std::mem::size_of::<MannWhitneyState>(),
            std::mem::align_of::<MannWhitneyState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "mann_whitney_u_test input missing",
            )
        })?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "mann_whitney_u_test merge input missing",
            )
        })?;
        let merge_array = if let Some(struct_arr) = arr.as_any().downcast_ref::<StructArray>() {
            struct_arr.columns().first().ok_or_else(|| {
                crate::exec::expr::agg::views::aggregate_input_contract(
                    "mann_whitney_u_test merge struct input has no fields",
                )
            })?
        } else {
            arr
        };
        let bin = merge_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                crate::exec::expr::agg::views::aggregate_input_contract(
                    "failed to downcast to BinaryArray",
                )
            })?;
        Ok(AggInputView::Binary(bin))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut MannWhitneyState,
                MannWhitneyState::new(AggregateAllocator::new(process_mem_tracker())),
            );
        }
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let tracker = tracker.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "allocation-tracked mann_whitney_u_test requires a memory tracker",
            )
        })?;
        unsafe {
            ptr.cast::<MannWhitneyState>()
                .write(MannWhitneyState::new(AggregateAllocator::new(tracker)))
        };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MannWhitneyState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
        0
    }

    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::AllocationTracked
    }

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let AggInputView::Any(array) = input else {
            return Err(super::aggregate_binding_contract(
                "mann_whitney_u_test input type mismatch",
            ));
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                super::aggregate_binding_contract("mann_whitney_u_test expects struct input")
            })?;
        let arguments = MannWhitneyInput::try_new(struct_arr.columns()).map_err(mann_failure)?;
        let mut work = ScalarWork::new(None);
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
            arguments
                .update_row(state, [row; 4], &mut work)
                .map_err(mann_failure)?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let AggInputView::Binary(arr) = input else {
            return Err(super::aggregate_binding_contract(
                "mann_whitney_u_test merge input type mismatch",
            ));
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
            let bytes = arr.value(row);
            let other = MannWhitneyState::deserialize_in(bytes, state.allocator.clone())
                .map_err(mann_failure)?;
            if state.is_uninitialized() {
                *state = other;
            } else {
                state.merge(other).map_err(mann_failure)?;
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            let mut output_offset = 0;
            for &base in group_states {
                let state =
                    unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
                let bytes = state.serialize().map_err(mann_failure)?;
                output_offset =
                    novarocks_functions::aggregate_scalar::checked_binary_output_offset(
                        output_offset,
                        bytes.len(),
                    )?;
                builder.append_value(bytes);
            }
            return Ok(Arc::new(builder.finish()));
        }

        let mut builder = StringBuilder::new();
        let mut output_offset = 0;
        for &base in group_states {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
            if state.is_uninitialized() || !state.has_samples() {
                builder.append_null();
                continue;
            }
            let value = state.build_result().map_err(mann_failure)?;
            output_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                output_offset,
                value.len(),
            )?;
            builder.append_value(value);
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::mem::MaybeUninit;

    #[test]
    fn sample_growth_is_rejected_before_allocator_mutation() {
        let values = Arc::new(Float64Array::from(vec![1.0])) as ArrayRef;
        let treatment = Arc::new(BooleanArray::from(vec![false])) as ArrayRef;
        let fields = vec![
            Field::new("f0", DataType::Float64, true),
            Field::new("f1", DataType::Boolean, true),
        ];
        let input_type = DataType::Struct(Fields::from(fields.clone()));
        let input = Arc::new(StructArray::new(
            fields.into(),
            vec![values, treatment],
            None,
        )) as ArrayRef;
        let function = AggFunction {
            name: "mann_whitney_u_test".to_string(),
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Float64),
            }),
            ..Default::default()
        };
        let spec = MannWhitneyUTestAgg
            .build_spec_from_type(&function, Some(&input_type), false)
            .unwrap();
        let tracker = MemTracker::new_root("mann-whitney-limit");
        tracker.install_limit_once(1).unwrap();
        let mut state = MaybeUninit::<MannWhitneyState>::uninit();
        MannWhitneyUTestAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();

        let error = MannWhitneyUTestAgg
            .update_batch(
                &spec,
                0,
                &[state.as_mut_ptr() as AggStatePtr],
                &AggInputView::Any(&input),
            )
            .expect_err("sample vector allocation must fail closed");
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&error).class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        assert_eq!(tracker.current(), 0);

        MannWhitneyUTestAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_mann_whitney_simple() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
        let treatment = Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef;

        let fields = vec![
            Field::new("f0", DataType::Float64, true),
            Field::new("f1", DataType::Boolean, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values, treatment], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;

        let func = AggFunction {
            name: "mann_whitney_u_test".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Float64),
            }),
            ..Default::default()
        };

        let spec = MannWhitneyUTestAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<MannWhitneyState>::uninit();
        MannWhitneyUTestAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        let input = AggInputView::Any(&array_ref);

        MannWhitneyUTestAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = MannWhitneyUTestAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MannWhitneyUTestAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
        let json_val: serde_json::Value = serde_json::from_str(out_arr.value(0)).unwrap();
        let arr = json_val.as_array().unwrap();
        let u2 = arr[0].as_f64().unwrap();
        let p = arr[1].as_f64().unwrap();
        assert!((u2 - 0.0).abs() < 1e-9);
        assert!((p - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_mann_whitney_accepts_numeric_treatment() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
        let treatment = Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef;

        let fields = vec![
            Field::new("f0", DataType::Float64, true),
            Field::new("f1", DataType::Int64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values, treatment], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;

        let func = AggFunction {
            name: "mann_whitney_u_test".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Float64),
            }),
            ..Default::default()
        };

        let spec = MannWhitneyUTestAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<MannWhitneyState>::uninit();
        MannWhitneyUTestAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        let input = AggInputView::Any(&array_ref);

        MannWhitneyUTestAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .expect("numeric 0/1 treatment should be accepted");
        let out = MannWhitneyUTestAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MannWhitneyUTestAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
        let json_val: serde_json::Value = serde_json::from_str(out_arr.value(0)).unwrap();
        let arr = json_val.as_array().unwrap();
        let u2 = arr[0].as_f64().unwrap();
        let p = arr[1].as_f64().unwrap();
        assert!((u2 - 0.0).abs() < 1e-9);
        assert!((p - 1.0).abs() < 1e-9);
    }
}

#[cfg(test)]
#[path = "mann_whitney_contract_tests.rs"]
mod contract_tests;
