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
use crate::exec::expr::agg::functions::common::build_scalar_array;
use crate::exec::expr::agg::{AggregateAllocator, RetainedMemoryPolicy};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{Array, ArrayRef, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use novarocks_functions::exact_percentile_core::{
    self as core, encode_state, finalize_cont, finalize_disc,
};
use std::sync::Arc;

#[cfg(test)]
use super::common::TrackedAggScalarValue;
#[cfg(test)]
use novarocks_functions::exact_percentile_core::{apply_rate, decode_state_into};
pub(super) struct PercentilePlaceholderAgg;
#[repr(transparent)]
struct ExactPercentileState(core::ExactPercentileState<AggregateAllocator>);
impl ExactPercentileState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        Self(core::ExactPercentileState::new(AggregateAllocator::new(
            tracker,
        )))
    }
    #[cfg(test)]
    fn push(&mut self, value: super::common::TrackedAggScalarValue) -> Result<(), String> {
        self.0.push(value)
    }
}
impl std::ops::Deref for ExactPercentileState {
    type Target = core::ExactPercentileState<AggregateAllocator>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for ExactPercentileState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}
fn merge_payload_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    diagnostic: core::ExactMergeDiagnostic,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { &mut *(ptr as *mut ExactPercentileState) };
        core::merge_from_array(&mut state.0, array, row, diagnostic)?;
    }
    Ok(())
}
fn update_from_struct(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
) -> Result<(), String> {
    let fields = array.columns();
    if fields.len() < 2 {
        return Err(
            "percentile_disc_cont_update: percentile_disc/cont expects STRUCT(value, rate) input"
                .to_string(),
        );
    }
    let values = fields[0].clone();
    let rates = fields[1].clone();
    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { &mut *(ptr as *mut ExactPercentileState) };
        core::update_from_arrays(&mut state.0, &values, row, &rates, row)?;
    }
    Ok(())
}
impl AggregateFunction for PercentilePlaceholderAgg {
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
            "percentile_cont" => AggKind::PercentileCont,
            "percentile_disc" => AggKind::PercentileDisc,
            "percentile_disc_lc" => AggKind::PercentileDiscLc,
            other => {
                return Err(format!(
                    "unsupported percentile aggregate function: {}",
                    other
                ));
            }
        };
        let output_type = sig
            .output_type
            .as_ref()
            .cloned()
            .or_else(|| input_type.cloned())
            .unwrap_or(DataType::Binary);
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or(DataType::Binary);
        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::PercentileCont | AggKind::PercentileDisc | AggKind::PercentileDiscLc => (
                std::mem::size_of::<ExactPercentileState>(),
                std::mem::align_of::<ExactPercentileState>(),
            ),
            other => unreachable!("unexpected kind for percentile placeholder: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile_disc/cont input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile_disc/cont merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked exact percentile requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked exact percentile requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(
                ptr as *mut ExactPercentileState,
                ExactPercentileState::new(tracker),
            );
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut ExactPercentileState);
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
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("percentile_disc/cont input type mismatch".to_string());
        };
        if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
            update_from_struct(struct_array, offset, state_ptrs)
        } else {
            merge_payload_array(
                array,
                offset,
                state_ptrs,
                core::ExactMergeDiagnostic::Update,
            )
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("percentile_disc/cont merge input type mismatch".to_string());
        };
        merge_payload_array(array, offset, state_ptrs, core::ExactMergeDiagnostic::Merge)
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

        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let state = unsafe { &*(ptr as *const ExactPercentileState) };
                builder.append_value(encode_state(state));
            }
            return Ok(Arc::new(builder.finish()));
        }

        let mut values = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let state = unsafe { &*(ptr as *const ExactPercentileState) };
            let value = match &spec.kind {
                AggKind::PercentileCont => finalize_cont(state, output_type),
                AggKind::PercentileDisc | AggKind::PercentileDiscLc => finalize_disc(state),
                other => Err(format!(
                    "unexpected percentile placeholder kind: {:?}",
                    other
                )),
            }?;
            values.push(value);
        }
        build_scalar_array(output_type, values)
    }
}

#[cfg(test)]
mod retained_bytes_tests {
    use super::*;

    #[test]
    fn json_round_trip_allocates_state_only_through_tracker() {
        let tracker = MemTracker::new_root("exact-percentile-test");
        let mut state = ExactPercentileState::new(tracker.clone());
        state.rate = Some(0.5);
        state
            .push(TrackedAggScalarValue::Utf8(
                crate::exec::expr::agg::aggregate_bytes(state.allocator.clone(), b"percentile")
                    .unwrap(),
            ))
            .unwrap();
        let encoded = encode_state(&state);

        let mut decoded = ExactPercentileState::new(tracker.clone());
        let (rate, values) = decode_state_into(&encoded, &decoded.allocator).unwrap();
        decoded.rate = rate;
        decoded.values = values;
        assert_eq!(decoded.rate, Some(0.5));
        assert!(tracker.current() > 0);

        drop(state);
        drop(decoded);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn failed_rate_merge_does_not_append_values_or_change_cache() {
        let mut state = ExactPercentileState::new(MemTracker::new_root("exact-percentile-rate"));
        apply_rate(&mut state, 0.5).expect("set rate");
        assert!(apply_rate(&mut state, 0.9).is_err());
        assert_eq!(state.rate, Some(0.5));
    }
}

#[cfg(test)]
#[path = "percentile_original_baseline_tests.rs"]
mod percentile_original_baseline_tests;
