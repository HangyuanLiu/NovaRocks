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

use super::super::registry::PreparedAggregateError;
use super::super::*;
use super::{AggregateFunction, sum::SumAgg};
use crate::exec::node::aggregate::AggFunction;
use arrow::datatypes::DataType;

pub(super) struct MvWeightSumAgg;
impl AggregateFunction for MvWeightSumAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        intermediate: bool,
    ) -> Result<AggSpec, String> {
        if !matches!(
            input_type,
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64)
        ) {
            return Err("mv_weight_sum requires a signed integer".into());
        }
        SumAgg.build_spec_from_type(func, input_type, intermediate)
    }
    fn state_layout_for(&self, kind: &super::AggKind) -> (usize, usize) {
        SumAgg.state_layout_for(kind)
    }
    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        SumAgg.build_input_view(spec, array)
    }
    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        SumAgg.build_merge_view(spec, array)
    }
    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        SumAgg.init_state(spec, ptr);
    }
    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8) {
        SumAgg.drop_state(spec, ptr);
    }
    fn retained_bytes(&self, spec: &AggSpec, ptr: *const u8) -> usize {
        SumAgg.retained_bytes(spec, ptr)
    }
    fn retained_memory_policy(&self, spec: &AggSpec) -> super::RetainedMemoryPolicy {
        SumAgg.retained_memory_policy(spec)
    }
    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        pointers: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        self.update_batch_typed(spec, offset, pointers, input)
            .map_err(|e| e.to_string())
    }
    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        pointers: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        self.merge_batch_typed(spec, offset, pointers, input)
            .map_err(|e| e.to_string())
    }
    fn update_batch_typed(
        &self,
        _: &AggSpec,
        offset: usize,
        pointers: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), PreparedAggregateError> {
        checked_update(offset, pointers, input)
    }
    fn merge_batch_typed(
        &self,
        _: &AggSpec,
        offset: usize,
        pointers: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), PreparedAggregateError> {
        checked_update(offset, pointers, input)
    }
    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        pointers: &[AggStatePtr],
        intermediate: bool,
    ) -> Result<ArrayRef, String> {
        SumAgg.build_array(spec, offset, pointers, intermediate)
    }
}
fn checked_update(
    offset: usize,
    pointers: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), PreparedAggregateError> {
    let AggInputView::Int(view) = input else {
        return Err(PreparedAggregateError::Input(
            "mv_weight_sum requires signed integer values".into(),
        ));
    };
    for (row, &base) in pointers.iter().enumerate() {
        if let Some(value) = view.value_at(row) {
            // SAFETY: the prepared kernel owns the exact initialized SumInt layout.
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumIntState) };
            state.sum = checked_weight_add(state.sum, value)
                .map_err(PreparedAggregateError::TaskFailure)?;
            state.has_value = true;
        }
    }
    Ok(())
}
fn weight_overflow() -> novarocks_execution_contract::TaskFailure {
    novarocks_execution_contract::TaskFailure::new(
        novarocks_execution_contract::TaskFailureCategory::Execution,
        novarocks_execution_contract::SafeDetail::truncating(
            "MV integer weight arithmetic overflow",
        ),
    )
}
pub fn checked_weight_add(
    a: i64,
    b: i64,
) -> Result<i64, novarocks_execution_contract::TaskFailure> {
    a.checked_add(b).ok_or_else(weight_overflow)
}
pub fn checked_weight_mul(
    a: i64,
    b: i64,
) -> Result<i64, novarocks_execution_contract::TaskFailure> {
    a.checked_mul(b).ok_or_else(weight_overflow)
}
pub fn checked_weight_neg(a: i64) -> Result<i64, novarocks_execution_contract::TaskFailure> {
    a.checked_neg().ok_or_else(weight_overflow)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checked_mv_weights_reject_overflow() {
        assert!(checked_weight_add(i64::MAX, 1).is_err());
        assert!(checked_weight_mul(i64::MAX, 2).is_err());
        assert!(checked_weight_neg(i64::MIN).is_err());
        assert_eq!(checked_weight_add(-7, 7).unwrap(), 0);
    }
    #[test]
    fn checked_mv_weight_updates_and_merges_keep_typed_failure() {
        let mut state = SumIntState {
            sum: i64::MAX,
            has_value: true,
        };
        let values = Some(std::sync::Arc::new(Int64Array::from(vec![1])) as ArrayRef);
        let input = AggInputView::Int(IntArrayView::new(values.as_ref().unwrap()).unwrap());
        assert!(matches!(
            checked_update(0, &[(&mut state as *mut SumIntState) as usize], &input),
            Err(PreparedAggregateError::TaskFailure(_))
        ));
        assert_eq!(state.sum, i64::MAX);
    }
}
