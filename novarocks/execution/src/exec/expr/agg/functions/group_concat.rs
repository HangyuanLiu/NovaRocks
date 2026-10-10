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
use std::sync::Arc;

use crate::exec::expr::agg::{AggregateAllocator, RetainedMemoryPolicy};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::DataType;
#[cfg(test)]
use arrow::datatypes::{Field, Fields};

use super::super::*;
use super::AggregateFunction;

pub(super) struct GroupConcatAgg;

use novarocks_functions::{aggregate_scalar::ScalarWork, builtin::aggregate_concat_core as core};
type GroupConcatState = core::GroupConcatState<AggregateAllocator>;
use core::{
    GroupConcatLayout, build_default_intermediate_type, extract_arg_types, extract_input_columns,
    validate_intermediate_type,
};
type GroupConcatKind<'a> = (bool, &'a [bool], &'a [bool], i64);

fn group_concat_kind(spec: &AggSpec) -> Result<GroupConcatKind<'_>, String> {
    match &spec.kind {
        AggKind::GroupConcat {
            is_distinct,
            is_asc_order,
            nulls_first,
            max_len,
        } => Ok((
            *is_distinct,
            is_asc_order.as_slice(),
            nulls_first.as_slice(),
            *max_len,
        )),
        _ => Err("group_concat spec kind mismatch".to_string()),
    }
}

impl AggregateFunction for GroupConcatAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "group_concat input type missing".to_string())?;
        let is_distinct = func.order.is_distinct;
        let is_asc_order = func.order.is_asc_order.clone();
        let nulls_first = func.order.nulls_first.clone();
        let max_len = func.order.effective_group_concat_max_len().unwrap_or(1024);
        let order_by_num = is_asc_order.len();
        let kind = AggKind::GroupConcat {
            is_distinct,
            is_asc_order,
            nulls_first,
            max_len,
        };

        if input_is_intermediate {
            let fields = validate_intermediate_type(input_type)?;
            let _ = GroupConcatLayout::infer(fields.len(), order_by_num)?;
            return Ok(AggSpec {
                kind,
                output_type: DataType::Utf8,
                intermediate_type: input_type.clone(),
                input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
                count_all: false,
            });
        }

        let arg_types = extract_arg_types(input_type);
        let _ = GroupConcatLayout::infer(arg_types.len(), order_by_num)?;

        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or_else(|| build_default_intermediate_type(&arg_types));
        let fields = validate_intermediate_type(&intermediate_type)?;
        if fields.len() != arg_types.len() {
            return Err(format!(
                "group_concat intermediate field count mismatch: expected {}, got {}",
                arg_types.len(),
                fields.len()
            ));
        }

        Ok(AggSpec {
            kind,
            output_type: DataType::Utf8,
            intermediate_type,
            input_arg_type: arg_types.first().cloned(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::GroupConcat { .. } => (
                std::mem::size_of::<GroupConcatState>(),
                std::mem::align_of::<GroupConcatState>(),
            ),
            other => unreachable!("unexpected kind for group_concat: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "group_concat input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "group_concat merge input missing".to_string())?;
        let _ = arr
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "group_concat merge input must be StructArray".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked group_concat requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked group_concat requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(
                ptr as *mut GroupConcatState,
                GroupConcatState::new(AggregateAllocator::new(tracker)),
            );
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut GroupConcatState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, ptr: *const u8) -> usize {
        let _ = ptr;
        0
    }

    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::AllocationTracked
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("group_concat batch input type mismatch".to_string());
        };
        let (_, is_asc_order, _, _) = group_concat_kind(spec)?;
        let columns = extract_input_columns(array)?;
        let layout = GroupConcatLayout::infer(columns.len(), is_asc_order.len())?;
        let arg_types = core::intermediate_arg_types(&spec.intermediate_type)?;
        if columns.len() != arg_types.len() {
            return Err(format!(
                "group_concat argument count mismatch: expected {}, got {}",
                arg_types.len(),
                columns.len()
            ));
        }
        for (index, (column, expected)) in columns.iter().zip(&arg_types).enumerate() {
            if column.data_type() != expected {
                return Err(format!(
                    "group_concat argument type mismatch at {index}: expected {expected:?}, got {:?}",
                    column.data_type()
                ));
            }
        }

        let mut work = ScalarWork::new(None);
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut GroupConcatState) };
            let addresses: Vec<_> = columns.iter().map(|column| (column, row)).collect();
            state
                .update_row(&addresses, layout, &mut work)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("group_concat merge input type mismatch".to_string());
        };
        let (_, is_asc_order, _, _) = group_concat_kind(spec)?;
        let mut work = ScalarWork::new(None);
        let merge = core::GroupConcatMerge::new(array, is_asc_order.len(), &mut work)
            .map_err(|error| error.to_string())?;
        for (row, &base) in state_ptrs.iter().enumerate() {
            if merge.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut GroupConcatState) };
            merge
                .merge_row(state, row, &spec.intermediate_type, &mut work)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let states = group_states
            .iter()
            .map(|&base| unsafe { &*((base as *mut u8).add(offset) as *const GroupConcatState) });
        let mut work = ScalarWork::new(None);
        if output_intermediate {
            return core::build_intermediate_array(&spec.intermediate_type, states, &mut work)
                .map_err(|error| error.to_string());
        }
        let (distinct, ascending, nulls_first, max_len) = group_concat_kind(spec)?;
        core::build_final_array(
            &spec.intermediate_type,
            states,
            distinct,
            ascending,
            nulls_first,
            max_len,
            &mut work,
        )
        .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::agg::{
        AggStateArena, build_kernel_set, test_builtin_execution_function_set,
    };
    use arrow::array::{ArrayRef, Int64Array, NullArray, StringArray};
    use std::mem::MaybeUninit;

    fn intermediate_type(arg_types: &[DataType]) -> DataType {
        build_default_intermediate_type(arg_types)
    }

    fn make_func(
        name: &str,
        input_is_intermediate: bool,
        intermediate_type: DataType,
        input_arg_type: DataType,
        order: crate::exec::node::aggregate::AggOrderSpec,
    ) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(intermediate_type),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(input_arg_type),
            }),
            order,
        }
    }

    fn order_spec(
        is_distinct: bool,
        is_asc_order: &[bool],
        nulls_first: &[bool],
    ) -> crate::exec::node::aggregate::AggOrderSpec {
        crate::exec::node::aggregate::AggOrderSpec {
            is_distinct,
            is_asc_order: is_asc_order.to_vec(),
            nulls_first: nulls_first.to_vec(),
            group_concat_max_len: None,
        }
    }

    fn run_update_then_finalize(spec: &AggSpec, input: ArrayRef) -> String {
        let view = AggInputView::Any(&input);
        let mut state = MaybeUninit::<GroupConcatState>::uninit();
        GroupConcatAgg
            .init_state_with_tracker(
                spec,
                state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("group-concat-test")),
            )
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let ptrs = vec![state_ptr; input.len()];
        GroupConcatAgg
            .update_batch(spec, 0, &ptrs, &view)
            .expect("update should succeed");
        let out = GroupConcatAgg
            .build_array(spec, 0, &[state_ptr], false)
            .expect("finalize should succeed");
        GroupConcatAgg.drop_state(spec, state.as_mut_ptr() as *mut u8);
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        out.value(0).to_string()
    }

    fn run_bound_group_concat(max_len: Option<i64>, values: &[&str]) -> String {
        let function_set = test_builtin_execution_function_set();
        let selected = function_set
            .catalog()
            .resolve_aggregate_trusted("group_concat", &[DataType::Utf8, DataType::Utf8])
            .expect("resolve group_concat");
        let input_fields = Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
        ]);
        let input_type = DataType::Struct(input_fields.clone());
        let mut order = order_spec(false, &[], &[]);
        order.group_concat_max_len = max_len;
        let func = make_func(
            "group_concat",
            false,
            selected.intermediate_type.clone(),
            DataType::Utf8,
            order,
        );
        // Exercise the generic option binder before the legacy implementation
        // adapter. Calling build_spec_from_type directly misses negative limits.
        let kernels = build_kernel_set(&function_set, &[func], &[Some(input_type)], &[selected])
            .expect("bind group_concat kernel");
        let kernel = &kernels.entries[0];
        let mut arena = AggStateArena::new(1024);
        let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
        kernel
            .init_state_with_tracker(base, MemTracker::new_root("bound-group-concat-test"))
            .expect("init group_concat state");
        let input = Arc::new(StructArray::new(
            input_fields,
            vec![
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(","); values.len()])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        kernel
            .update_batch(
                &vec![base; input.len()],
                novarocks_functions::AggregateInputBatch::try_new(Some(&input), input.len())
                    .expect("group_concat input batch"),
            )
            .expect("update group_concat");
        let output = kernel
            .build_array(&[base], false)
            .expect("finalize group_concat");
        kernel.drop_state(base);
        let output = output
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("group_concat UTF-8 output");
        assert!(!output.is_null(0));
        output.value(0).to_string()
    }

    #[test]
    fn bound_group_concat_applies_minimum_before_generic_option_validation() {
        for (limit, expected) in [
            (None, "é你,abcd"),
            (Some(-121), "é"),
            (Some(0), "é"),
            (Some(1), "é"),
            (Some(4), "é"),
            (Some(5), "é你"),
            (Some(8), "é你,ab"),
        ] {
            assert_eq!(
                run_bound_group_concat(limit, &["é你", "abcd"]),
                expected,
                "raw group_concat_max_len={limit:?}"
            );
        }
    }

    #[test]
    fn bound_group_concat_retains_default_limit_when_option_is_absent() {
        let value = "a".repeat(1100);
        assert_eq!(run_bound_group_concat(None, &[&value]), "a".repeat(1024));
    }

    #[test]
    fn test_group_concat_spec_with_metadata() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let arg_types = vec![DataType::Utf8, DataType::Utf8, DataType::Int64];
        let func = make_func(
            "group_concat",
            false,
            intermediate_type(&arg_types),
            input_type.clone(),
            order_spec(true, &[true], &[false]),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        match &spec.kind {
            AggKind::GroupConcat {
                is_distinct,
                is_asc_order,
                nulls_first,
                ..
            } => {
                assert!(*is_distinct);
                assert_eq!(is_asc_order, &vec![true]);
                assert_eq!(nulls_first, &vec![false]);
            }
            other => panic!("unexpected kind: {:?}", other),
        }
    }

    #[test]
    fn test_string_agg_alias_uses_group_concat_metadata() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let arg_types = vec![DataType::Utf8, DataType::Utf8, DataType::Int64];
        let func = make_func(
            "string_agg",
            false,
            intermediate_type(&arg_types),
            input_type.clone(),
            order_spec(true, &[true], &[false]),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        match &spec.kind {
            AggKind::GroupConcat {
                is_distinct,
                is_asc_order,
                nulls_first,
                ..
            } => {
                assert!(*is_distinct);
                assert_eq!(is_asc_order, &vec![true]);
                assert_eq!(nulls_first, &vec![false]);
            }
            other => panic!("unexpected kind: {:?}", other),
        }
    }

    #[test]
    fn test_group_concat_default_separator_single_column() {
        let input_type = DataType::Int64;
        let func = make_func(
            "group_concat",
            false,
            intermediate_type(&[DataType::Int64]),
            input_type.clone(),
            crate::exec::node::aggregate::AggOrderSpec::default(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "1,3");
    }

    #[test]
    fn test_group_concat_multi_output_columns() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
        ]));
        let func = make_func(
            "group_concat",
            false,
            intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            input_type.clone(),
            crate::exec::node::aggregate::AggOrderSpec::default(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("1"), Some("2")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(","), Some(",")])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "x1,y2");
    }

    #[test]
    fn test_group_concat_null_output_argument_skips_rows() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Null, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
        ]));
        let func = make_func(
            "group_concat",
            false,
            intermediate_type(&[DataType::Utf8, DataType::Null, DataType::Utf8]),
            input_type.clone(),
            order_spec(true, &[], &[]),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Null, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef,
                Arc::new(NullArray::new(2)) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(","), Some(",")])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let view = AggInputView::Any(&input);
        let mut state = MaybeUninit::<GroupConcatState>::uninit();
        GroupConcatAgg
            .init_state_with_tracker(
                &spec,
                state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("group-concat-test")),
            )
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        GroupConcatAgg
            .update_batch(&spec, 0, &vec![state_ptr; input.len()], &view)
            .expect("null output argument rows should be skipped");
        let out = GroupConcatAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .expect("finalize should succeed");
        GroupConcatAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(out.is_null(0));
    }

    #[test]
    fn test_group_concat_order_by() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let func = make_func(
            "group_concat",
            false,
            intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Int64]),
            input_type.clone(),
            order_spec(false, &[true], &[false]),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), Some("a"), Some("c")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("|"), Some("|"), Some("|")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(3)])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "a|b|c");
    }

    #[test]
    fn test_group_concat_distinct_keep_last_after_sort() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let func = make_func(
            "group_concat",
            false,
            intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Int64]),
            input_type.clone(),
            order_spec(true, &[true], &[false]),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("x"), Some("y")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("|"), Some("/"), Some("-")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "x/y");
    }

    #[test]
    fn test_group_concat_merge_roundtrip() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let intermediate_ty = intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Int64]);
        let func = make_func(
            "group_concat",
            false,
            intermediate_ty.clone(),
            input_type.clone(),
            order_spec(true, &[true], &[false]),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), Some("a"), Some("a")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(","), Some(","), Some("|")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(3)])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let view = AggInputView::Any(&input);

        let mut left_state = MaybeUninit::<GroupConcatState>::uninit();
        GroupConcatAgg
            .init_state_with_tracker(
                &spec,
                left_state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("group-concat-left-test")),
            )
            .unwrap();
        let left_ptr = left_state.as_mut_ptr() as AggStatePtr;
        GroupConcatAgg
            .update_batch(&spec, 0, &vec![left_ptr; input.len()], &view)
            .unwrap();
        let intermediate = GroupConcatAgg
            .build_array(&spec, 0, &[left_ptr], true)
            .unwrap();

        let merge_func = make_func(
            "group_concat",
            true,
            intermediate_ty.clone(),
            input_type,
            order_spec(true, &[true], &[false]),
        );
        let merge_spec = GroupConcatAgg
            .build_spec_from_type(&merge_func, Some(&intermediate_ty), true)
            .unwrap();
        let merge_view = AggInputView::Any(&intermediate);
        let mut right_state = MaybeUninit::<GroupConcatState>::uninit();
        GroupConcatAgg
            .init_state_with_tracker(
                &merge_spec,
                right_state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("group-concat-right-test")),
            )
            .unwrap();
        let right_ptr = right_state.as_mut_ptr() as AggStatePtr;
        GroupConcatAgg
            .merge_batch(&merge_spec, 0, &[right_ptr], &merge_view)
            .unwrap();

        let out = GroupConcatAgg
            .build_array(&merge_spec, 0, &[right_ptr], false)
            .unwrap();
        GroupConcatAgg.drop_state(&spec, left_state.as_mut_ptr() as *mut u8);
        GroupConcatAgg.drop_state(&merge_spec, right_state.as_mut_ptr() as *mut u8);
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "b,a");
    }
}
