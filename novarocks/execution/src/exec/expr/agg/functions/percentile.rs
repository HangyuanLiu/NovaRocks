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

use arrow::array::{Array, ListArray};
use arrow::array::{ArrayRef, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::exec::expr::agg::functions::common::build_scalar_array;
use crate::exec::node::aggregate::AggFunction;
use crate::exec::percentile;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_functions::approx_percentile_aggregate_core::{
    self as approx_core, ApproxPercentileDiagnostic,
};

use super::super::*;
use super::AggregateFunction;

pub(super) struct PercentileAgg;

type TrackedPercentileState = percentile::PercentileState<AggregateAllocator>;

unsafe fn state_mut<'a>(ptr: *mut u8) -> &'a mut TrackedPercentileState {
    unsafe { &mut *(ptr as *mut TrackedPercentileState) }
}

unsafe fn state_ref<'a>(ptr: *const u8) -> &'a TrackedPercentileState {
    unsafe { &*(ptr as *const TrackedPercentileState) }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

fn merge_unweighted_payload_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: ApproxPercentileDiagnostic,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let Some(payload) = approx_core::payload_for_merge(array, row, context)? else {
            continue;
        };
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { state_mut(ptr) };
        approx_core::merge_payload(state, payload)?;
    }
    Ok(())
}

fn merge_weighted_payload_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: ApproxPercentileDiagnostic,
) -> Result<(), String> {
    merge_unweighted_payload_array(array, offset, state_ptrs, context)
}

fn update_unweighted_struct(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: ApproxPercentileDiagnostic,
) -> Result<(), String> {
    let input = approx_core::UnweightedInput::try_new(array, context)?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { state_mut(ptr) };
        input.update_row(state, row, context)?;
    }
    Ok(())
}

fn update_weighted_struct(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: ApproxPercentileDiagnostic,
) -> Result<(), String> {
    let input = approx_core::WeightedInput::try_new(array, context)?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { state_mut(ptr) };
        input.update_row(state, row, context)?;
    }
    Ok(())
}

impl AggregateFunction for PercentileAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let sig = func
            .types
            .as_ref()
            .ok_or_else(|| "aggregate type signature is required".to_string())?;

        let kind = match canonical_agg_name(func.name.as_str()) {
            "percentile_union" => AggKind::PercentileUnion,
            "percentile_approx" => AggKind::PercentileApprox,
            "percentile_approx_weighted" => AggKind::PercentileApproxWeighted,
            other => {
                return Err(format!(
                    "unsupported percentile aggregate function: {}",
                    other
                ));
            }
        };

        Ok(AggSpec {
            kind,
            output_type: sig.output_type.clone().unwrap_or(DataType::Float64),
            intermediate_type: sig.intermediate_type.clone().unwrap_or(DataType::Binary),
            input_arg_type: input_type.cloned(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::PercentileUnion
            | AggKind::PercentileApprox
            | AggKind::PercentileApproxWeighted => (
                std::mem::size_of::<TrackedPercentileState>(),
                std::mem::align_of::<TrackedPercentileState>(),
            ),
            other => unreachable!("unexpected percentile agg kind: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile aggregate input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile aggregate merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, _ptr: *mut u8) {
        panic!("allocation-tracked percentile requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked percentile requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(
                ptr as *mut TrackedPercentileState,
                percentile::PercentileState::new_in(
                    percentile::DEFAULT_COMPRESSION_FACTOR,
                    AggregateAllocator::new(tracker),
                ),
            );
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut TrackedPercentileState);
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
            return Err("percentile aggregate input type mismatch".to_string());
        };
        match &spec.kind {
            AggKind::PercentileApproxWeighted => {
                if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
                    update_weighted_struct(
                        struct_array,
                        offset,
                        state_ptrs,
                        ApproxPercentileDiagnostic::WeightedUpdate,
                    )
                } else {
                    merge_weighted_payload_array(
                        array,
                        offset,
                        state_ptrs,
                        ApproxPercentileDiagnostic::WeightedUpdate,
                    )
                }
            }
            AggKind::PercentileUnion | AggKind::PercentileApprox => {
                if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
                    update_unweighted_struct(
                        struct_array,
                        offset,
                        state_ptrs,
                        ApproxPercentileDiagnostic::UnweightedUpdate,
                    )
                } else {
                    merge_unweighted_payload_array(
                        array,
                        offset,
                        state_ptrs,
                        ApproxPercentileDiagnostic::UnweightedUpdate,
                    )
                }
            }
            other => Err(format!("unexpected percentile aggregate kind: {:?}", other)),
        }
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("percentile aggregate merge input type mismatch".to_string());
        };
        match &spec.kind {
            AggKind::PercentileApproxWeighted => merge_weighted_payload_array(
                array,
                offset,
                state_ptrs,
                ApproxPercentileDiagnostic::WeightedMerge,
            ),
            _ => merge_unweighted_payload_array(
                array,
                offset,
                state_ptrs,
                ApproxPercentileDiagnostic::UnweightedMerge,
            ),
        }
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let output_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };
        match output_type {
            DataType::Binary => {
                let mut builder = BinaryBuilder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    builder.append_value(percentile::encode_state(unsafe { state_ref(ptr) }));
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 | DataType::List(_) => {
                let output = if matches!(output_type, DataType::Float64) {
                    approx_core::ScalarOutput::Float64
                } else {
                    approx_core::ScalarOutput::List
                };
                let mut values = Vec::with_capacity(group_states.len());
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    values.push(approx_core::scalar_output(
                        unsafe { state_ref(ptr) },
                        output,
                    )?);
                }
                build_scalar_array(output_type, values)
            }
            other => {
                if matches!(&spec.kind, AggKind::PercentileApproxWeighted) {
                    Err(format!(
                        "weighted percentile aggregate output type must be Binary/Float64/List, got {:?}",
                        other
                    ))
                } else {
                    Err(format!(
                        "percentile aggregate output type must be Binary/Float64/List, got {:?}",
                        other
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "weighted_prepared_tests.rs"]
mod weighted_prepared_tests;

#[cfg(test)]
#[path = "approx_percentile_original_baseline_tests.rs"]
mod approx_percentile_original_baseline_tests;
