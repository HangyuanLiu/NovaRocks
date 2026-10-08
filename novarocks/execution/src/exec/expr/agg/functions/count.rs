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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::builtin::aggregate_count_core as count_core;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct CountAgg;

fn count_fallback(count_all: bool) -> AggSpec {
    AggSpec {
        kind: AggKind::Count,
        output_type: DataType::Int64,
        intermediate_type: DataType::Int64,
        input_arg_type: None,
        count_all,
    }
}

impl AggregateFunction for CountAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        Ok(count_fallback(input_type.is_none()))
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::Count => (std::mem::size_of::<i64>(), std::mem::align_of::<i64>()),
            other => unreachable!("unexpected kind for count: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        if spec.count_all {
            Ok(AggInputView::None)
        } else {
            let arr = array
                .as_ref()
                .ok_or_else(|| "count input missing".to_string())?;
            Ok(AggInputView::Any(arr))
        }
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count input missing".to_string())?;
        Ok(AggInputView::Int(IntArrayView::new(arr)?))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut i64, count_core::initial_state());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
        0
    }
    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::FixedZero
    }
    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match input {
            AggInputView::None => {
                unsafe { count_core::update_legacy_batch(true, None, offset, state_ptrs) };
                Ok(())
            }
            AggInputView::Any(array) => {
                unsafe {
                    count_core::update_legacy_batch(
                        spec.count_all,
                        Some(array.as_ref()),
                        offset,
                        state_ptrs,
                    )
                };
                Ok(())
            }
            _ => Err("count batch input type mismatch".to_string()),
        }
    }
    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let source = match input {
            AggInputView::Int(IntArrayView::Int64(a)) => count_core::CountMergeArray::Int64(a),
            AggInputView::Int(IntArrayView::Int32(a)) => count_core::CountMergeArray::Int32(a),
            AggInputView::Int(IntArrayView::Int16(a)) => count_core::CountMergeArray::Int16(a),
            AggInputView::Int(IntArrayView::Int8(a)) => count_core::CountMergeArray::Int8(a),
            _ => return Err("count merge batch input type mismatch".to_string()),
        };
        unsafe { count_core::merge_legacy_batch(source, offset, state_ptrs) };
        Ok(())
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let values = group_states
            .iter()
            .map(|&base| Ok(unsafe { *((base as *mut u8).add(offset) as *const i64) }));
        let result = count_core::build_state_array_observed(values, &mut |_| {
            Ok::<_, std::convert::Infallible>(())
        });
        Ok(result.unwrap_or_else(|never| match never {}))
    }
}

#[cfg(test)]
#[path = "legacy_count_original_baseline_tests.rs"]
mod original_count_baseline_tests;
