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
use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

use crate::exec::expr::agg::AggregateAllocator;
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};
use novarocks_functions::aggregate_scalar::ScalarWork;
use novarocks_functions::builtin::aggregate_map_core as map_core;

use super::super::*;
use super::AggregateFunction;
use super::common::TrackedAggScalarValue;

#[cfg(test)]
use super::common::{build_scalar_array, tracked_scalar_from_array};
#[cfg(test)]
use arrow_buffer::OffsetBuffer;

pub(super) struct MapAggAgg;

#[repr(transparent)]
#[derive(Debug)]
struct MapAggState(map_core::MapAggState<AggregateAllocator>);
impl std::ops::Deref for MapAggState {
    type Target = map_core::MapAggState<AggregateAllocator>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for MapAggState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl MapAggState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        Self(map_core::MapAggState::new(AggregateAllocator::new(tracker)))
    }
}
fn append_entry(
    state: &mut MapAggState,
    key: TrackedAggScalarValue,
    value: Option<TrackedAggScalarValue>,
) -> Result<(), String> {
    map_core::append_entry(&mut state.0, key, value, &mut ScalarWork::new(None))
        .map_err(|error| error.to_string())
}

impl AggregateFunction for MapAggAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "map_agg input type missing".to_string())?;
        let Some(kind) = kind_from_name(func.name.as_str()) else {
            return Err(format!("unsupported map agg function: {}", func.name));
        };

        if input_is_intermediate {
            let output_type = func
                .types
                .as_ref()
                .and_then(|t| t.output_type.clone())
                .unwrap_or_else(|| input_type.clone());
            return Ok(AggSpec {
                kind,
                output_type,
                intermediate_type: input_type.clone(),
                input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
                count_all: false,
            });
        }

        let DataType::Struct(fields) = input_type else {
            return Err(format!(
                "map_agg expects struct input, got {:?}",
                input_type
            ));
        };
        if fields.len() != 2 {
            return Err("map_agg expects 2 arguments".to_string());
        }

        let output_type = func
            .types
            .as_ref()
            .and_then(|t| t.output_type.clone())
            .unwrap_or_else(|| build_default_map_type(fields[0].clone(), fields[1].clone()));
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or_else(|| output_type.clone());

        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MapAgg => (
                std::mem::size_of::<MapAggState>(),
                std::mem::align_of::<MapAggState>(),
            ),
            other => unreachable!("unexpected kind for map_agg: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "map_agg input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "map_agg merge input missing".to_string())?;
        let _ = arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "map_agg merge input must be MapArray".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut MapAggState,
                MapAggState::new(process_mem_tracker()),
            );
        }
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker
            .ok_or_else(|| "allocation-tracked map_agg requires a memory tracker".to_string())?;
        unsafe { ptr.cast::<MapAggState>().write(MapAggState::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MapAggState);
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
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("map_agg batch input type mismatch".to_string());
        };
        let input = map_core::update_input(array).map_err(|error| error.to_string())?;
        let mut work = ScalarWork::new(None);
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MapAggState) };
            map_core::update_row(&mut state.0, &input, row, &mut work)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("map_agg merge input type mismatch".to_string());
        };
        let input = map_core::merge_input(array).map_err(|error| error.to_string())?;
        let mut work = ScalarWork::new(None);
        for (row, &base) in state_ptrs.iter().enumerate() {
            // Do not dereference a mapped state for an original NULL partial row.
            if array.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MapAggState) };
            map_core::merge_row(&mut state.0, &input, row, &mut work)
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
        let target_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };
        let states = group_states.iter().map(|&base| {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const MapAggState) };
            &state.0
        });
        map_core::build_array(target_type, states, &mut ScalarWork::new(None))
            .map_err(|error| error.to_string())
    }
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "map_agg" => Some(AggKind::MapAgg),
        _ => None,
    }
}

#[cfg(test)]
fn parse_map_type(ty: &DataType) -> Result<(Arc<Field>, arrow::datatypes::Fields, bool), String> {
    map_core::parse_map_type(ty).map_err(|error| error.to_string())
}

fn build_default_map_type(key_field: Arc<Field>, value_field: Arc<Field>) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new(
                    "key",
                    key_field.data_type().clone(),
                    key_field.is_nullable(),
                )),
                Arc::new(Field::new(
                    "value",
                    value_field.data_type().clone(),
                    value_field.is_nullable(),
                )),
            ])),
            false,
        )),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::array::{Int64Array, Int64Builder, MapBuilder, StringArray, StructArray};
    use std::mem::MaybeUninit;

    fn map_type_i64_i64() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn test_map_agg_spec() {
        let func = AggFunction {
            name: "map_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_type_i64_i64()),
                output_type: Some(map_type_i64_i64()),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("v", DataType::Int64, true)),
        ]));
        let spec = MapAggAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::MapAgg));
        assert_eq!(spec.output_type, map_type_i64_i64());
    }

    #[test]
    fn test_map_agg_update_dedups_keep_first() {
        let func = AggFunction {
            name: "map_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_type_i64_i64()),
                output_type: Some(map_type_i64_i64()),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("v", DataType::Int64, true)),
        ]));
        let spec = MapAggAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let keys = Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])) as ArrayRef;
        let vals = Arc::new(Int64Array::from(vec![
            Some(10),
            Some(99),
            Some(20),
            Some(30),
        ])) as ArrayRef;
        let input = Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("k", DataType::Int64, true)),
                Arc::new(Field::new("v", DataType::Int64, true)),
            ]),
            vec![keys, vals],
            None,
        )) as ArrayRef;

        let view = AggInputView::Any(&input);
        let mut state = MaybeUninit::<MapAggState>::uninit();
        MapAggAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 4];

        MapAggAgg
            .update_batch(&spec, 0, &state_ptrs, &view)
            .unwrap();
        let out = MapAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MapAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.value_length(0), 2);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(vals.value(0), 10);
        assert_eq!(keys.value(1), 2);
        assert_eq!(vals.value(1), 20);
    }

    #[test]
    fn test_map_agg_merge_dedups_keep_first() {
        let func = AggFunction {
            name: "map_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: true,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_type_i64_i64()),
                output_type: Some(map_type_i64_i64()),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = MapAggAgg
            .build_spec_from_type(&func, Some(&map_type_i64_i64()), true)
            .unwrap();

        let mut map_builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        map_builder.keys().append_value(1);
        map_builder.values().append_value(10);
        map_builder.keys().append_value(2);
        map_builder.values().append_value(20);
        map_builder.append(true).unwrap();
        map_builder.keys().append_value(2);
        map_builder.values().append_value(99);
        map_builder.keys().append_value(3);
        map_builder.values().append_value(30);
        map_builder.append(true).unwrap();
        let map_array = Arc::new(map_builder.finish()) as ArrayRef;
        let view = AggInputView::Any(&map_array);

        let mut state = MaybeUninit::<MapAggState>::uninit();
        MapAggAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        MapAggAgg.merge_batch(&spec, 0, &state_ptrs, &view).unwrap();
        let out = MapAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MapAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.value_length(0), 3);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(vals.value(0), 10);
        assert_eq!(keys.value(1), 2);
        assert_eq!(vals.value(1), 20);
        assert_eq!(keys.value(2), 3);
        assert_eq!(vals.value(2), 30);
    }

    #[test]
    fn tracked_allocations_change_only_for_new_keys_and_release_on_drop() {
        let tracker = MemTracker::new_root("map-agg-test");
        let mut state = MapAggState::new(Arc::clone(&tracker));
        let keys = Arc::new(StringArray::from(vec!["key", "key", "other"])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec!["first", "ignored", "second"])) as ArrayRef;

        let key = tracked_scalar_from_array(&keys, 0, &state.allocator)
            .unwrap()
            .unwrap();
        let value = tracked_scalar_from_array(&values, 0, &state.allocator).unwrap();
        append_entry(&mut state, key, value).unwrap();
        let first = tracker.current();
        assert!(first > 0);

        let key = tracked_scalar_from_array(&keys, 1, &state.allocator)
            .unwrap()
            .unwrap();
        let value = tracked_scalar_from_array(&values, 1, &state.allocator).unwrap();
        append_entry(&mut state, key, value).unwrap();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(tracker.current(), first);

        let key = tracked_scalar_from_array(&keys, 2, &state.allocator)
            .unwrap()
            .unwrap();
        let value = tracked_scalar_from_array(&values, 2, &state.allocator).unwrap();
        append_entry(&mut state, key, value).unwrap();
        assert_eq!(state.entries.len(), 2);
        assert!(tracker.current() > first);

        drop(state);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
#[path = "legacy_map_agg_baseline_tests.rs"]
mod legacy_map_agg_baseline_tests;
