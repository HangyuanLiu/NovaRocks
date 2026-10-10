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
use super::super::*;
use super::AggregateFunction;
use crate::exec::expr::agg::{AggregateAllocator, RetainedMemoryPolicy};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{Array, ArrayRef, ListArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use novarocks_functions::{
    aggregate_scalar::ScalarWork,
    builtin::aggregate_array_core::{self as core, ArrayAggConfig, ArrayAggKind},
};
use std::sync::Arc;
type ArrayAggState = core::ArrayAggState<AggregateAllocator>;
pub(super) struct ArrayAggAgg;
fn config(spec: &AggSpec) -> ArrayAggConfig<'_> {
    let kind = match &spec.kind {
        AggKind::ArrayAgg {
            is_distinct,
            is_asc_order,
            nulls_first,
        } => ArrayAggKind::Array {
            distinct: *is_distinct,
            ascending: is_asc_order,
            nulls_first,
        },
        AggKind::ArrayUniqueAgg => ArrayAggKind::Unique,
        other => unreachable!("unexpected kind for array_agg: {:?}", other),
    };
    ArrayAggConfig {
        kind,
        output_type: &spec.output_type,
        intermediate_type: &spec.intermediate_type,
        input_arg_type: spec.input_arg_type.as_ref(),
    }
}
fn merge_input_list_array(array: &ArrayRef) -> Result<(&ListArray, Option<&StructArray>), String> {
    core::merge_input_list_array(array).map_err(|e| e.to_string())
}
use super::common::tracked_optional_key_fingerprint;
#[cfg(test)]
use core::ArrayAggValue;
#[cfg(test)]
fn distinct_key(value: &Option<ArrayAggValue>) -> Vec<u8> {
    core::distinct_key(value, &mut ScalarWork::new(None))
        .expect("legacy fingerprint observer cannot fail")
}
#[cfg(test)]
fn to_common_scalar(value: &ArrayAggValue) -> super::common::AggScalarValue {
    core::to_common_scalar(value, &mut ScalarWork::new(None))
        .expect("legacy scalar observer cannot fail")
}
fn first_field_item_type(field: &Field) -> Result<DataType, String> {
    match field.data_type() {
        DataType::List(item) => Ok(item.data_type().clone()),
        other => Err(format!(
            "array_agg intermediate struct field must be ARRAY, got {:?}",
            other
        )),
    }
}

fn first_item_type_from_update_input(input_type: &DataType) -> Result<DataType, String> {
    match input_type {
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                "array_agg input struct must contain at least 1 field".to_string()
            })?;
            Ok(first.data_type().clone())
        }
        other => Ok(other.clone()),
    }
}

fn unique_item_type_from_update_input(input_type: &DataType) -> Result<DataType, String> {
    match input_type {
        DataType::List(item) => Ok(item.data_type().clone()),
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                "array_unique_agg input struct must contain at least 1 field".to_string()
            })?;
            if let DataType::List(item) = first.data_type() {
                Ok(item.data_type().clone())
            } else {
                Ok(first.data_type().clone())
            }
        }
        other => Ok(other.clone()),
    }
}

fn first_item_type_from_intermediate_input(input_type: &DataType) -> Result<DataType, String> {
    match input_type {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => {
                let first = fields.first().ok_or_else(|| {
                    "array_agg merge ARRAY<STRUCT> input must contain at least 1 struct field"
                        .to_string()
                })?;
                Ok(first.data_type().clone())
            }
            other => Ok(other.clone()),
        },
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                "array_agg merge struct input must contain at least 1 field".to_string()
            })?;
            first_field_item_type(first)
        }
        other => Err(format!(
            "array_agg merge input must be ARRAY or STRUCT, got {:?}",
            other
        )),
    }
}

fn extract_arg_types(input_type: &DataType) -> Vec<DataType> {
    match input_type {
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect(),
        other => vec![other.clone()],
    }
}

fn build_default_intermediate_type(arg_types: &[DataType]) -> DataType {
    if arg_types.len() <= 1 {
        let item_type = arg_types.first().cloned().unwrap_or(DataType::Null);
        return DataType::List(Arc::new(Field::new("item", item_type, true)));
    }

    let fields = arg_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| {
            Arc::new(Field::new(
                format!("c{idx}"),
                DataType::List(Arc::new(Field::new("item", data_type.clone(), true))),
                true,
            ))
        })
        .collect::<Vec<_>>();
    DataType::Struct(Fields::from(fields))
}
impl AggregateFunction for ArrayAggAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "array_agg input type missing".to_string())?;
        let base_name = func.name.as_str();
        let is_asc_order = func.order.is_asc_order.clone();
        let nulls_first = func.order.nulls_first.clone();
        let kind = match base_name {
            "array_agg" => AggKind::ArrayAgg {
                is_distinct: false,
                is_asc_order,
                nulls_first,
            },
            "array_agg_distinct" => AggKind::ArrayAgg {
                is_distinct: true,
                is_asc_order,
                nulls_first,
            },
            "array_unique_agg" => AggKind::ArrayUniqueAgg,
            other => return Err(format!("unsupported array agg function: {}", other)),
        };

        let arg_types = extract_arg_types(input_type);
        // A STRUCT can be the aggregate value itself, not just a legacy
        // value/ORDER-BY channel wrapper. The selected output's item type is
        // authoritative in both update and merge phases; inspecting field[0]
        // would silently discard the other fields of a ROW value.
        let item_type = if let Some(DataType::List(field)) = func
            .types
            .as_ref()
            .and_then(|types| types.output_type.as_ref())
        {
            field.data_type().clone()
        } else if input_is_intermediate {
            first_item_type_from_intermediate_input(input_type)?
        } else if matches!(kind, AggKind::ArrayUniqueAgg) {
            unique_item_type_from_update_input(input_type)?
        } else {
            first_item_type_from_update_input(input_type)?
        };

        let output_list_type = func
            .types
            .as_ref()
            .and_then(|t| t.output_type.as_ref())
            .and_then(|t| match t {
                DataType::List(field) if field.data_type() == &item_type => Some(t.clone()),
                _ => None,
            })
            .unwrap_or_else(|| {
                DataType::List(Arc::new(Field::new("item", item_type.clone(), true)))
            });
        let intermediate_type = if input_is_intermediate {
            input_type.clone()
        } else {
            func.types
                .as_ref()
                .and_then(|t| t.intermediate_type.as_ref())
                .cloned()
                .unwrap_or_else(|| build_default_intermediate_type(&arg_types))
        };

        Ok(AggSpec {
            kind,
            output_type: output_list_type,
            intermediate_type,
            input_arg_type: Some(item_type),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::ArrayAgg { .. } | AggKind::ArrayUniqueAgg => (
                std::mem::size_of::<ArrayAggState>(),
                std::mem::align_of::<ArrayAggState>(),
            ),
            other => unreachable!("unexpected kind for array_agg: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "array_agg input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "array_agg merge input missing".to_string())?;
        let _ = merge_input_list_array(arr)?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked array_agg requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked array_agg requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(
                ptr as *mut ArrayAggState,
                ArrayAggState::new(AggregateAllocator::new(tracker)),
            );
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut ArrayAggState);
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
            return Err("array_agg batch input type mismatch".to_string());
        };
        let config = config(spec);
        if matches!(spec.kind, AggKind::ArrayUniqueAgg) && array.as_any().is::<ListArray>() {
            for (row, &base) in state_ptrs.iter().enumerate() {
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ArrayAggState) };
                core::update_row(
                    state,
                    config.kind,
                    std::iter::once((*array, row)),
                    false,
                    false,
                    &mut ScalarWork::new(None),
                )
                .map_err(|e| e.to_string())?;
            }
            return Ok(());
        }
        let (value, wrapper) =
            core::unwrap_update_value_array(&config, array, &mut ScalarWork::new(None))
                .map_err(|e| e.to_string())?;
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ArrayAggState) };
            if let Some(wrapper) = wrapper {
                core::update_row(
                    state,
                    config.kind,
                    wrapper.columns().iter().map(|col| (col, row)),
                    true,
                    wrapper.is_null(row),
                    &mut ScalarWork::new(None),
                )
                .map_err(|e| e.to_string())?;
            } else {
                core::update_row(
                    state,
                    config.kind,
                    std::iter::once((value, row)),
                    false,
                    false,
                    &mut ScalarWork::new(None),
                )
                .map_err(|e| e.to_string())?;
            }
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
            return Err("array_agg merge input type mismatch".to_string());
        };
        // Preserve original validation before any group iteration, including an empty batch.
        let _ = core::merge_input_list_array(array).map_err(|e| e.to_string())?;
        let config = config(spec);
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ArrayAggState) };
            core::merge_row(state, config.kind, array, row, &mut ScalarWork::new(None))
                .map_err(|e| e.to_string())?;
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
        core::build_array(
            &config(spec),
            group_states
                .iter()
                .map(|&base| unsafe { &*((base as *mut u8).add(offset) as *const ArrayAggState) }),
            output_intermediate,
            &mut ScalarWork::new(None),
        )
        .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, MapArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use arrow_buffer::OffsetBuffer;
    use std::mem::MaybeUninit;

    fn list_i64_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
    }

    fn make_func(name: &str) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(list_i64_type()),
                output_type: Some(list_i64_type()),
                input_arg_type: Some(DataType::Int64),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn selected_struct_item_survives_update_and_merge() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let values: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(9)])),
                Arc::new(Int32Array::from(vec![None, Some(2)])),
            ],
            None,
        ));
        let item_type = DataType::Struct(fields);
        let list_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
        let mut function = make_func("array_agg");
        function.types = Some(crate::exec::node::aggregate::AggTypeSignature {
            intermediate_type: Some(list_type.clone()),
            output_type: Some(list_type.clone()),
            input_arg_type: Some(item_type.clone()),
        });
        let spec = ArrayAggAgg
            .build_spec_from_type(&function, Some(&item_type), false)
            .unwrap();
        assert_eq!(spec.output_type, list_type);
        assert_eq!(spec.input_arg_type, Some(item_type.clone()));
        let mut update_state = MaybeUninit::<ArrayAggState>::uninit();
        let update_ptr = update_state.as_mut_ptr() as AggStatePtr;
        ArrayAggAgg
            .init_state_with_tracker(
                &spec,
                update_ptr as *mut u8,
                Some(MemTracker::new_root("array-agg-struct-update")),
            )
            .unwrap();
        ArrayAggAgg
            .update_batch(
                &spec,
                0,
                &[update_ptr, update_ptr],
                &AggInputView::Any(&values),
            )
            .unwrap();
        let intermediate = ArrayAggAgg
            .build_array(&spec, 0, &[update_ptr], true)
            .unwrap();
        ArrayAggAgg.drop_state(&spec, update_ptr as *mut u8);

        let merge_spec = ArrayAggAgg
            .build_spec_from_type(&function, Some(&list_type), true)
            .unwrap();
        assert_eq!(merge_spec.output_type, list_type);
        assert_eq!(merge_spec.input_arg_type, Some(item_type));
        let mut merge_state = MaybeUninit::<ArrayAggState>::uninit();
        let merge_ptr = merge_state.as_mut_ptr() as AggStatePtr;
        ArrayAggAgg
            .init_state_with_tracker(
                &merge_spec,
                merge_ptr as *mut u8,
                Some(MemTracker::new_root("array-agg-struct-merge")),
            )
            .unwrap();
        ArrayAggAgg
            .merge_batch(
                &merge_spec,
                0,
                &[merge_ptr],
                &AggInputView::Any(&intermediate),
            )
            .unwrap();
        let result = ArrayAggAgg
            .build_array(&merge_spec, 0, &[merge_ptr], false)
            .unwrap();
        ArrayAggAgg.drop_state(&merge_spec, merge_ptr as *mut u8);
        let result = result
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(result.data_type(), values.data_type());
        assert_eq!(result.to_data(), values.to_data());
    }

    fn map_type_i32_utf8(key_nullable: bool) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int32, key_nullable)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ])),
                false,
            )),
            false,
        )
    }

    fn array_agg_map_order_types(
        key_nullable: bool,
    ) -> crate::exec::node::aggregate::AggTypeSignature {
        let map_type = map_type_i32_utf8(key_nullable);
        let output_type = DataType::List(Arc::new(Field::new("item", map_type.clone(), true)));
        let intermediate_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new(
                "c0",
                DataType::List(Arc::new(Field::new("item", map_type.clone(), true))),
                true,
            )),
            Arc::new(Field::new(
                "c1",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
        ]));
        crate::exec::node::aggregate::AggTypeSignature {
            intermediate_type: Some(intermediate_type),
            output_type: Some(output_type),
            input_arg_type: Some(map_type),
        }
    }

    fn build_nullable_key_map_array() -> ArrayRef {
        let keys = Arc::new(Int32Array::from(vec![Some(3), Some(4), None])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec![Some("3"), Some("4"), None])) as ArrayRef;
        let entries = StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int32, true)),
                Arc::new(Field::new("value", DataType::Utf8, true)),
            ]),
            vec![keys, values],
            None,
        );
        Arc::new(MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entries.fields().clone()),
                false,
            )),
            OffsetBuffer::new(vec![0, 2, 3, 3].into()),
            entries,
            Some(arrow_buffer::NullBuffer::from(vec![true, true, false])),
            false,
        )) as ArrayRef
    }

    #[test]
    fn array_agg_distinct_key_uses_common_fingerprint() {
        let value = Some(ArrayAggValue::Struct(vec![
            Some(ArrayAggValue::Int64(7)),
            None,
            Some(ArrayAggValue::Utf8("x".to_string())),
        ]));
        let mut expected = vec![1];
        expected.extend(super::super::common::key_fingerprint(&to_common_scalar(
            value.as_ref().unwrap(),
        )));

        assert_eq!(distinct_key(&value), expected);
        assert_eq!(distinct_key(&None), vec![0]);
    }

    #[test]
    fn test_array_agg_spec() {
        let func = make_func("array_agg");
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::ArrayAgg { .. }));
        assert_eq!(spec.output_type, list_i64_type());
    }

    #[test]
    fn test_array_agg_collects_values_and_nulls() {
        let func = make_func("array_agg");
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();

        let values = Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])) as ArrayRef;
        let input = AggInputView::Any(&values);

        let mut state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg
            .init_state_with_tracker(
                &spec,
                state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("array-agg-test")),
            )
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 3];
        ArrayAggAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = ArrayAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        ArrayAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), 2);
    }

    #[test]
    fn test_array_agg_distinct_deduplicates_values() {
        let func = make_func("array_agg_distinct");
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();

        let values = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(1),
            None,
            None,
            Some(2),
        ])) as ArrayRef;
        let input = AggInputView::Any(&values);

        let mut state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg
            .init_state_with_tracker(
                &spec,
                state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("array-agg-test")),
            )
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 5];
        ArrayAggAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = ArrayAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        ArrayAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), 2);
    }

    #[test]
    fn test_array_agg_collects_list_items() {
        let func = AggFunction {
            name: "array_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: None,
            ..Default::default()
        };
        let input_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let list_values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let input = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            list_values,
            None,
        )) as ArrayRef;
        let input = AggInputView::Any(&input);

        let mut state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg
            .init_state_with_tracker(
                &spec,
                state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("array-agg-test")),
            )
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        ArrayAggAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = ArrayAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        ArrayAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.len(), 1);
        let nested = out.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(nested.len(), 2);
        let flat = nested
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(flat.len(), 3);
        assert_eq!(flat.value(0), "a");
        assert_eq!(flat.value(1), "b");
        assert_eq!(flat.value(2), "c");
    }

    #[test]
    fn test_array_unique_agg_alias() {
        let func = make_func("array_unique_agg");
        let spec = super::build_spec_from_type(&func, Some(&DataType::Int64), false).unwrap();
        assert!(matches!(spec.kind, AggKind::ArrayUniqueAgg));
    }

    #[test]
    fn test_array_agg_distinct_order_by_accepts_runtime_map_key_widening() {
        let planned_input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("f0", map_type_i32_utf8(false), true)),
            Arc::new(Field::new("f1", DataType::Int32, true)),
        ]));
        let update_func = AggFunction {
            name: "array_agg_distinct".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(array_agg_map_order_types(false)),
            order: crate::exec::node::aggregate::AggOrderSpec {
                is_asc_order: vec![true],
                nulls_first: vec![false],
                is_distinct: false,
                group_concat_max_len: None,
            },
        };
        let update_spec = ArrayAggAgg
            .build_spec_from_type(&update_func, Some(&planned_input_type), false)
            .expect("update spec");

        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("f0", map_type_i32_utf8(true), true)),
                Arc::new(Field::new("f1", DataType::Int32, true)),
            ]),
            vec![
                build_nullable_key_map_array(),
                Arc::new(Int32Array::from(vec![None, Some(98), None])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let view = AggInputView::Any(&input);

        let mut update_state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg
            .init_state_with_tracker(
                &update_spec,
                update_state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("array-agg-update-test")),
            )
            .unwrap();
        let update_ptr = update_state.as_mut_ptr() as AggStatePtr;
        let update_ptrs = vec![update_ptr; 3];
        ArrayAggAgg
            .update_batch(&update_spec, 0, &update_ptrs, &view)
            .expect("update batch");
        let intermediate = ArrayAggAgg
            .build_array(&update_spec, 0, &[update_ptr], true)
            .expect("build intermediate");
        ArrayAggAgg.drop_state(&update_spec, update_state.as_mut_ptr() as *mut u8);

        let merge_func = AggFunction {
            name: "array_agg_distinct".to_string(),
            inputs: vec![],
            input_is_intermediate: true,
            types: Some(array_agg_map_order_types(false)),
            order: crate::exec::node::aggregate::AggOrderSpec {
                is_asc_order: vec![true],
                nulls_first: vec![false],
                is_distinct: false,
                group_concat_max_len: None,
            },
        };
        let merge_spec = ArrayAggAgg
            .build_spec_from_type(
                &merge_func,
                update_func
                    .types
                    .as_ref()
                    .and_then(|types| types.intermediate_type.as_ref()),
                true,
            )
            .expect("merge spec");

        let mut merge_state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg
            .init_state_with_tracker(
                &merge_spec,
                merge_state.as_mut_ptr() as *mut u8,
                Some(MemTracker::new_root("array-agg-merge-test")),
            )
            .unwrap();
        let merge_ptr = merge_state.as_mut_ptr() as AggStatePtr;
        let merge_ptrs = vec![merge_ptr];
        let merge_view = AggInputView::Any(&intermediate);
        ArrayAggAgg
            .merge_batch(&merge_spec, 0, &merge_ptrs, &merge_view)
            .expect("merge batch");
        let out = ArrayAggAgg
            .build_array(&merge_spec, 0, &[merge_ptr], false)
            .expect("build final");
        ArrayAggAgg.drop_state(&merge_spec, merge_state.as_mut_ptr() as *mut u8);

        let out = out
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("outer list");
        assert_eq!(out.len(), 1);
        let values = out
            .values()
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map values");
        assert_eq!(values.len(), 3);
        assert_eq!(values.value_length(0), 1);
        assert_eq!(values.value_length(1), 2);
        assert!(values.is_null(2));
    }
}

#[cfg(test)]
#[path = "../../legacy_array_agg_baseline_tests.rs"]
mod legacy_array_agg_baseline_tests;
