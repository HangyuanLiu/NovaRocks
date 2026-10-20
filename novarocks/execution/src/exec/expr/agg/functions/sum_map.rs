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
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MapArray, NullArray};
use arrow::datatypes::DataType;

use crate::exec::expr::agg::{
    AggregateAllocator, AggregateHashMap, AggregateVec, aggregate_hash_map,
};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use super::super::*;
use super::AggregateFunction;
use super::common::{
    AggScalarValue, TrackedAggScalarValue, build_scalar_array, tracked_optional_key_fingerprint,
    tracked_scalar_from_array, tracked_scalar_to_output,
};

pub(super) struct SumMapAgg;

#[derive(Debug)]
struct SumMapState {
    allocator: AggregateAllocator,
    saw_non_null_map: bool,
    indexes: AggregateHashMap<AggregateVec<u8>, usize>,
    entries: AggregateVec<(Option<TrackedAggScalarValue>, TrackedAggScalarValue)>,
}

impl SumMapState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            indexes: aggregate_hash_map(allocator.clone()),
            entries: AggregateVec::new_in(allocator.clone()),
            allocator,
            saw_non_null_map: false,
        }
    }
}

impl AggregateFunction for SumMapAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        let input_type = input_type.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "sum_map input type missing",
            )
        })?;
        let signature = func.types.as_ref().ok_or_else(|| {
            super::aggregate_binding_contract("sum_map requires its frozen type signature")
        })?;
        let output_type = signature.output_type.clone().ok_or_else(|| {
            super::aggregate_binding_contract("sum_map requires its frozen output type")
        })?;
        let intermediate_type = signature.intermediate_type.clone().ok_or_else(|| {
            super::aggregate_binding_contract("sum_map requires its frozen intermediate type")
        })?;

        let check_type = if input_is_intermediate {
            &intermediate_type
        } else {
            input_type
        };
        if !matches!(check_type, DataType::Map(_, _) | DataType::Null) {
            return Err(
                crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                    "sum_map expects MAP input, got {:?}",
                    check_type
                )),
            );
        }

        Ok(AggSpec {
            kind: AggKind::SumMap,
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
            AggKind::SumMap => (
                std::mem::size_of::<SumMapState>(),
                std::mem::align_of::<SumMapState>(),
            ),
            other => unreachable!("unexpected kind for sum_map: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract("sum_map input missing")
        })?;
        if matches!(arr.data_type(), DataType::Null)
            && arr.as_any().downcast_ref::<NullArray>().is_some()
        {
            return Ok(AggInputView::Any(arr));
        }
        let _ = arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "sum_map input must be MapArray",
            )
        })?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        self.build_input_view(spec, array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut SumMapState,
                SumMapState::new(process_mem_tracker()),
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
                "allocation-tracked sum_map requires a memory tracker",
            )
        })?;
        unsafe { ptr.cast::<SumMapState>().write(SumMapState::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut SumMapState);
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
            return Err(crate::exec::expr::agg::views::aggregate_input_contract(
                "sum_map input type mismatch",
            ));
        };
        if matches!(array.data_type(), DataType::Null)
            && array.as_any().downcast_ref::<NullArray>().is_some()
        {
            return Ok(());
        }
        let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "sum_map input must be MapArray",
            )
        })?;
        merge_map_array(offset, state_ptrs, map)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        self.update_batch(spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        let target_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };
        let mut out = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const SumMapState) };
            if !state.saw_non_null_map {
                out.push(None);
                continue;
            }
            let mut entries = state
                .entries
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.as_ref().map(tracked_scalar_to_output).transpose()?,
                        Some(tracked_scalar_to_output(value)?),
                    ))
                })
                .collect::<ExecutionResult<Vec<_>>>()?;
            sort_map_entries(&mut entries)?;
            out.push(Some(AggScalarValue::Map(entries)));
        }
        build_scalar_array(target_type, out)
    }
}

fn merge_map_array(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    map: &MapArray,
) -> ExecutionResult<()> {
    let keys = map.keys().clone();
    let values = map.values().clone();
    let offsets = map.value_offsets();

    for (row, &base) in state_ptrs.iter().enumerate() {
        if map.is_null(row) {
            continue;
        }
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumMapState) };
        state.saw_non_null_map = true;
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        for idx in start..end {
            let Some(value) = tracked_scalar_from_array(&values, idx, &state.allocator)? else {
                continue;
            };
            let key = tracked_scalar_from_array(&keys, idx, &state.allocator)?;
            let fp = tracked_optional_key_fingerprint(&key, &state.allocator)?;
            if let Some(existing_idx) = state.indexes.get(&fp).copied() {
                sum_scalar_in_place(&mut state.entries[existing_idx].1, &value)?;
            } else {
                let insert_idx = state.entries.len();
                state.indexes.try_reserve(1).map_err(|source| {
                    state
                        .allocator
                        .allocation_error("reserve sum_map key index", source)
                })?;
                state.entries.try_reserve(1).map_err(|source| {
                    state
                        .allocator
                        .allocation_error("reserve sum_map entries", source)
                })?;
                state.indexes.insert(fp, insert_idx);
                state.entries.push((key, value));
            }
        }
    }
    Ok(())
}

fn sort_map_entries(
    entries: &mut [(Option<AggScalarValue>, Option<AggScalarValue>)],
) -> ExecutionResult<()> {
    let mut failure = None;
    entries.sort_by(|left, right| {
        if failure.is_some() {
            return Ordering::Equal;
        }
        match compare_optional_scalars(&left.0, &right.0) {
            Ok(order) => order,
            Err(source) => {
                failure = Some(source);
                Ordering::Equal
            }
        }
    });
    match failure {
        Some(source) => Err(source),
        None => Ok(()),
    }
}

fn compare_optional_scalars(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
) -> ExecutionResult<Ordering> {
    match (left, right) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(left), Some(right)) => {
            use novarocks_functions::aggregate_scalar::{ScalarOrdering, ScalarWork};
            let order =
                novarocks_functions::aggregate_scalar_fingerprint::compare_scalar_values_partial(
                    left,
                    right,
                    &mut ScalarWork::new(None),
                )?;
            // Preserve this aggregate's existing NaN and mixed-kind ordering
            // policy while keeping real computation failures fallible.
            Ok(match order {
                ScalarOrdering::Ordered(order) => order,
                ScalarOrdering::UnorderedFloat | ScalarOrdering::MismatchedKinds => Ordering::Equal,
            })
        }
    }
}

fn sum_scalar_in_place(
    target: &mut TrackedAggScalarValue,
    incoming: &TrackedAggScalarValue,
) -> ExecutionResult<()> {
    match (target, incoming) {
        (TrackedAggScalarValue::Int64(left), TrackedAggScalarValue::Int64(right)) => {
            *left = left.checked_add(*right).ok_or_else(|| {
                ExecutionFailure::from_domain(SumMapOverflow::Int64 {
                    left: *left,
                    right: *right,
                })
            })?;
            Ok(())
        }
        (TrackedAggScalarValue::Float64(left), TrackedAggScalarValue::Float64(right)) => {
            *left += *right;
            Ok(())
        }
        (TrackedAggScalarValue::Decimal128(left), TrackedAggScalarValue::Decimal128(right)) => {
            *left = left.checked_add(*right).ok_or_else(|| {
                ExecutionFailure::from_domain(SumMapOverflow::Decimal128 {
                    left: *left,
                    right: *right,
                })
            })?;
            Ok(())
        }
        (TrackedAggScalarValue::Decimal256(left), TrackedAggScalarValue::Decimal256(right)) => {
            *left = left.checked_add(*right).ok_or_else(|| {
                ExecutionFailure::from_domain(SumMapOverflow::Decimal256 {
                    left: *left,
                    right: *right,
                })
            })?;
            Ok(())
        }
        _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            "sum_map value type mismatch",
        )),
    }
}

#[derive(Debug)]
enum SumMapOverflow {
    Int64 {
        left: i64,
        right: i64,
    },
    Decimal128 {
        left: i128,
        right: i128,
    },
    Decimal256 {
        left: arrow_buffer::i256,
        right: arrow_buffer::i256,
    },
}
impl std::fmt::Display for SumMapOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Int64 { .. } => "sum_map int64 overflow",
            Self::Decimal128 { .. } => "sum_map decimal128 overflow",
            Self::Decimal256 { .. } => "sum_map decimal256 overflow",
        })
    }
}
impl std::error::Error for SumMapOverflow {}
impl novarocks_failure_contract::FailureProjection for SumMapOverflow {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &novarocks_functions::DataFailureReason::NumericOutOfRange,
        )
    }
}

#[cfg(test)]
mod typed_sum_map_properties {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn actual_checked_addition_keeps_prior_state_and_exact_operands() {
        for (mut target, incoming) in [
            (
                TrackedAggScalarValue::Int64(i64::MAX),
                TrackedAggScalarValue::Int64(1),
            ),
            (
                TrackedAggScalarValue::Decimal128(i128::MAX),
                TrackedAggScalarValue::Decimal128(1),
            ),
            (
                TrackedAggScalarValue::Decimal256(arrow_buffer::i256::MAX),
                TrackedAggScalarValue::Decimal256(arrow_buffer::i256::ONE),
            ),
        ] {
            let failure = sum_scalar_in_place(&mut target, &incoming).unwrap_err();
            assert_eq!(
                failure.failure_fact(),
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            );
            let crate::runtime::fragment::ExecutionFailureCause::Domain(source) = failure.cause()
            else {
                panic!("sum owner must retain its cause")
            };
            match (source.downcast_ref::<SumMapOverflow>().unwrap(), target) {
                (SumMapOverflow::Int64 { left, right }, TrackedAggScalarValue::Int64(value)) => {
                    assert_eq!((*left, *right), (i64::MAX, 1));
                    assert_eq!(value, i64::MAX);
                }
                (
                    SumMapOverflow::Decimal128 { left, right },
                    TrackedAggScalarValue::Decimal128(value),
                ) => {
                    assert_eq!((*left, *right), (i128::MAX, 1));
                    assert_eq!(value, i128::MAX);
                }
                (
                    SumMapOverflow::Decimal256 { left, right },
                    TrackedAggScalarValue::Decimal256(value),
                ) => {
                    assert_eq!(
                        (*left, *right),
                        (arrow_buffer::i256::MAX, arrow_buffer::i256::ONE)
                    );
                    assert_eq!(value, arrow_buffer::i256::MAX);
                }
                _ => panic!("source kind must match the original arithmetic"),
            }
        }
    }
    #[test]
    fn nan_and_heterogeneous_keys_keep_their_original_equal_policy() {
        for (left, right) in [
            (
                AggScalarValue::Float64(f64::NAN),
                AggScalarValue::Float64(1.0),
            ),
            (AggScalarValue::Int64(1), AggScalarValue::Bool(true)),
        ] {
            assert_eq!(
                compare_optional_scalars(&Some(left), &Some(right)).unwrap(),
                Ordering::Equal
            );
        }
        let mut entries = vec![
            (Some(AggScalarValue::Int64(2)), None),
            (None, None),
            (Some(AggScalarValue::Int64(1)), None),
        ];
        sort_map_entries(&mut entries).unwrap();
        assert!(entries[0].0.is_none());
        assert!(matches!(entries[1].0, Some(AggScalarValue::Int64(1))));
        assert!(matches!(entries[2].0, Some(AggScalarValue::Int64(2))));
    }
}
