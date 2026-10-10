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

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, LargeBinaryArray, LargeStringArray, StringArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field};

use crate::exec::expr::agg::AggregateAllocator;
#[cfg(test)]
use crate::exec::expr::agg::aggregate_bytes;
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use super::super::*;
use super::AggregateFunction;
#[cfg(test)]
use super::common::TrackedAggScalarValue;
use super::common::build_scalar_array;

pub(super) struct MinMaxNAgg;

#[derive(Debug)]
struct MinMaxNState(novarocks_functions::builtin::aggregate_n_core::NState<AggregateAllocator>);
impl MinMaxNState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        Self(novarocks_functions::builtin::aggregate_n_core::NState::new(
            AggregateAllocator::new(tracker),
        ))
    }
}
impl std::ops::Deref for MinMaxNState {
    type Target = novarocks_functions::builtin::aggregate_n_core::NState<AggregateAllocator>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for MinMaxNState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
struct LegacyNBuffer(Vec<u8>);
impl novarocks_functions::builtin::aggregate_by_core::ByEncodeBuffer for LegacyNBuffer {
    fn push(
        &mut self,
        byte: u8,
    ) -> Result<(), novarocks_functions::aggregate_scalar::ScalarStateError> {
        self.0.push(byte);
        Ok(())
    }
    fn append(
        &mut self,
        bytes: &[u8],
        _work: &mut novarocks_functions::aggregate_scalar::ScalarWork<'_, '_>,
    ) -> Result<(), novarocks_functions::aggregate_scalar::ScalarStateError> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}
impl AggregateFunction for MinMaxNAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "min_n/max_n input type missing".to_string())?;
        let kind = match canonical_agg_name(func.name.as_str()) {
            "min_n" => AggKind::MinN,
            "max_n" => AggKind::MaxN,
            other => return Err(format!("unsupported min/max_n function: {}", other)),
        };

        let output_type = func
            .types
            .as_ref()
            .and_then(|sig| sig.output_type.clone())
            .unwrap_or_else(|| default_output_type(input_type, input_is_intermediate));
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|sig| sig.intermediate_type.clone())
            .unwrap_or(DataType::Binary);

        if input_is_intermediate {
            if !matches!(
                input_type,
                DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8
            ) {
                return Err(format!(
                    "min_n/max_n merge input must be binary-like, got {:?}",
                    input_type
                ));
            }
        } else if !matches!(input_type, DataType::Struct(fields) if fields.len() >= 2) {
            return Err(format!(
                "min_n/max_n expects packed STRUCT(value, n), got {:?}",
                input_type
            ));
        }

        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: func
                .types
                .as_ref()
                .and_then(|sig| sig.input_arg_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MinN | AggKind::MaxN => (
                std::mem::size_of::<MinMaxNState>(),
                std::mem::align_of::<MinMaxNState>(),
            ),
            other => unreachable!("unexpected kind for min/max_n: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "min_n/max_n input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "min_n/max_n merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut MinMaxNState,
                MinMaxNState::new(process_mem_tracker()),
            );
        }
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked min_n/max_n requires a memory tracker".to_string()
        })?;
        unsafe { ptr.cast::<MinMaxNState>().write(MinMaxNState::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MinMaxNState);
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
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("min_n/max_n input type mismatch".to_string());
        };
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "min_n/max_n input must be StructArray".to_string())?;
        if struct_array.num_columns() < 2 {
            return Err(format!(
                "min_n/max_n input expects at least 2 fields, got {}",
                struct_array.num_columns()
            ));
        }
        let values = struct_array.column(0).clone();
        let limits = struct_array.column(1).clone();
        let keep_smallest = matches!(spec.kind, AggKind::MinN);

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MinMaxNState) };
            state
                .update_from_arrays(
                    &values,
                    row,
                    &limits,
                    row,
                    keep_smallest,
                    &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
                )
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
            return Err("min_n/max_n merge input type mismatch".to_string());
        };
        let keep_smallest = matches!(spec.kind, AggKind::MinN);
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MinMaxNState) };
            state
                .merge_from_array(
                    array,
                    row,
                    keep_smallest,
                    &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
                )
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

        match target_type {
            DataType::Binary => {
                let mut builder = BinaryBuilder::new();
                for &base in group_states {
                    let state = unsafe { &*((base as *mut u8).add(offset) as *const MinMaxNState) };
                    let mut bytes = LegacyNBuffer(Vec::new());
                    state
                        .serialize(
                            &mut bytes,
                            &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
                        )
                        .map_err(|error| error.to_string())?;
                    builder.append_value(bytes.0);
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::List(_) => {
                let mut values = Vec::with_capacity(group_states.len());
                for &base in group_states {
                    let state = unsafe { &*((base as *mut u8).add(offset) as *const MinMaxNState) };
                    values.push(Some(
                        state
                            .output(&mut novarocks_functions::aggregate_scalar::ScalarWork::new(
                                None,
                            ))
                            .map_err(|error| error.to_string())?,
                    ));
                }
                build_scalar_array(target_type, values)
            }
            other => Err(format!(
                "min_n/max_n output type must be Binary or List, got {:?}",
                other
            )),
        }
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

fn default_output_type(input_type: &DataType, input_is_intermediate: bool) -> DataType {
    if input_is_intermediate {
        return DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    }
    if let DataType::Struct(fields) = input_type
        && let Some(first) = fields.first()
    {
        return DataType::List(Arc::new(Field::new(
            "item",
            first.data_type().clone(),
            true,
        )));
    }
    DataType::List(Arc::new(Field::new("item", input_type.clone(), true)))
}

#[cfg(test)]
fn push_value(
    state: &mut MinMaxNState,
    value: TrackedAggScalarValue,
    keep_smallest: bool,
) -> Result<(), String> {
    state
        .push_value(
            value,
            keep_smallest,
            &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
        )
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod retained_bytes_tests {
    use super::*;

    #[test]
    fn truncation_subtracts_evicted_nested_payload() {
        let tracker = MemTracker::new_root("minmax-n-test");
        let mut state = MinMaxNState::new(Arc::clone(&tracker));
        state.initialized = true;
        state.limit = 2;
        for value in ["b", "a", "c"] {
            let value = aggregate_bytes(state.allocator.clone(), value.as_bytes()).unwrap();
            push_value(&mut state, TrackedAggScalarValue::Utf8(value), true).expect("push value");
        }

        assert_eq!(state.values.len(), 2);
        assert!(tracker.current() > 0);
        assert_eq!(
            state
                .values
                .iter()
                .map(|value| match value {
                    TrackedAggScalarValue::Utf8(value) => {
                        std::str::from_utf8(value).expect("utf8")
                    }
                    _ => unreachable!("test values are strings"),
                })
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        drop(state);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
#[path = "minmax_n_baseline_tests.rs"]
mod minmax_n_baseline_tests;
