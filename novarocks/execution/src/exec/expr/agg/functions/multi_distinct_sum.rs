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
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder};
#[cfg(test)]
use arrow::array::{
    BooleanArray, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
#[cfg(test)]
use arrow_buffer::i256;

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use super::super::*;
use super::AggregateFunction;
use novarocks_functions::builtin::aggregate_distinct_numeric as numeric_core;
use novarocks_functions::{EvaluationCheckpoints, KernelEvaluationControl, KernelFailure};
struct NumericControl;
impl KernelEvaluationControl for NumericControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("numeric distinct requested a wait"),
        ))
    }
}
fn numeric_work<T>(
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, numeric_core::DistinctComputationError>,
) -> Result<T, String> {
    let mut w = EvaluationCheckpoints::new(&NumericControl);
    let result = f(&mut w);
    w.finish().map_err(|e| e.to_string())?;
    result.map_err(numeric_core::DistinctComputationError::into_legacy_message)
}
impl numeric_core::NumericDistinctSet for DistinctSet {
    fn len(&self) -> usize {
        self.len()
    }
    fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.iter().map(|v| v.as_slice())
    }
}
struct NumericBuffer<'a> {
    out: &'a mut AggregateVec<u8>,
    allocator: &'a AggregateAllocator,
}
impl numeric_core::NumericDistinctBuffer for NumericBuffer<'_> {
    fn reserve_exact(&mut self, size: usize) -> Result<(), String> {
        self.out.try_reserve_exact(size).map_err(|_| {
            self.allocator
                .allocation_error("reserve distinct serialization")
        })
    }
    fn append(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }
}

// Hash the borrowed key exactly as the allocator-owned byte vector.
struct DistinctLookup<'a>(&'a [u8]);
impl std::hash::Hash for DistinctLookup<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.0, state);
    }
}
impl hashbrown::Equivalent<AggregateVec<u8>> for DistinctLookup<'_> {
    fn equivalent(&self, key: &AggregateVec<u8>) -> bool {
        self.0 == key.as_slice()
    }
}

struct DistinctSet {
    allocator: AggregateAllocator,
    values: AggregateHashSet<AggregateVec<u8>>,
}

impl DistinctSet {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            values: aggregate_hash_set(allocator.clone()),
            allocator,
        }
    }

    fn insert(&mut self, value: &[u8]) -> Result<(), String> {
        if self.values.contains(&DistinctLookup(value)) {
            return Ok(());
        }
        self.values
            .try_reserve(1)
            .map_err(|_| self.allocator.allocation_error("reserve distinct hash set"))?;
        let value = aggregate_bytes(self.allocator.clone(), value)?;
        self.values.insert(value);
        Ok(())
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn iter(&self) -> impl Iterator<Item = &AggregateVec<u8>> {
        self.values.iter()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn retained_bytes(&self) -> usize {
        0
    }
}

pub(super) struct MultiDistinctNumericAgg;

unsafe fn get_set_mut<'a>(ptr: *mut u8) -> &'a mut DistinctSet {
    unsafe { &mut *(ptr.cast::<DistinctSet>()) }
}

unsafe fn get_set<'a>(ptr: *const u8) -> &'a DistinctSet {
    unsafe { &*(ptr.cast::<DistinctSet>()) }
}

fn serialize_set(set: &DistinctSet) -> Result<AggregateVec<u8>, String> {
    let mut out = AggregateVec::new_in(set.allocator.clone());
    numeric_work(|w| {
        numeric_core::serialize_set_into(
            set,
            &mut NumericBuffer {
                out: &mut out,
                allocator: &set.allocator,
            },
            w,
        )
    })?;
    Ok(out)
}
fn numeric_key_width(t: &DataType) -> Result<usize, String> {
    numeric_core::numeric_key_width(t)
        .map_err(numeric_core::DistinctComputationError::into_legacy_message)
}
fn visit_serialized_keys(
    bytes: &[u8],
    width: usize,
    mut visit: impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    numeric_work(|w| {
        numeric_core::visit_serialized_keys(bytes, width, w, |value| {
            visit(value).map_err(Into::into)
        })
    })
}
fn sum_from_set(
    set: &DistinctSet,
    input: &DataType,
    output: &DataType,
) -> Result<ArrayRef, String> {
    numeric_work(|w| numeric_core::sum_from_set(set, input, output, w))
}
fn avg_from_set(
    set: &DistinctSet,
    input: &DataType,
    output: &DataType,
) -> Result<ArrayRef, String> {
    numeric_work(|w| numeric_core::avg_from_set(set, input, output, w))
}

impl AggregateFunction for MultiDistinctNumericAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type =
            input_type.ok_or_else(|| "distinct numeric input type missing".to_string())?;
        let is_avg = func.name == "multi_distinct_avg";
        let kind = if is_avg {
            AggKind::MultiDistinctAvg
        } else {
            AggKind::MultiDistinctSum
        };

        if input_is_intermediate {
            let sig = super::super::agg_type_signature(func)
                .ok_or_else(|| "multi_distinct_sum type signature missing".to_string())?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| "multi_distinct_sum output type signature missing".to_string())?;
            return Ok(AggSpec {
                kind,
                output_type: output_type.clone(),
                intermediate_type: data_type.clone(),
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        let output_type = if is_avg {
            match data_type {
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64 => DataType::Float64,
                DataType::Decimal128(..) => {
                    novarocks_type_contract::canonical_agg_decimal_type("avg", data_type)
                        .ok_or_else(|| "distinct avg decimal output type missing".to_string())?
                }
                DataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
                other => return Err(format!("distinct avg unsupported input type: {other:?}")),
            }
        } else {
            match data_type {
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Boolean => DataType::Int64,
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                DataType::Decimal128(..) => novarocks_type_contract::canonical_agg_decimal_type(
                    "multi_distinct_sum",
                    data_type,
                )
                .ok_or_else(|| {
                    format!(
                        "multi_distinct_sum unsupported decimal input type: {:?}",
                        data_type
                    )
                })?,
                DataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
                other => {
                    return Err(format!(
                        "multi_distinct_sum unsupported input type: {:?}",
                        other
                    ));
                }
            }
        };
        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type: DataType::Binary,
            input_arg_type: Some(data_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MultiDistinctSum | AggKind::MultiDistinctAvg => (
                std::mem::size_of::<DistinctSet>(),
                std::mem::align_of::<DistinctSet>(),
            ),
            other => unreachable!("unexpected kind for multi_distinct_sum: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "multi_distinct_sum input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "multi_distinct_sum merge input missing".to_string())?;
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(bin))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            ptr.cast::<DistinctSet>()
                .write(DistinctSet::new(process_mem_tracker()))
        };
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked numeric distinct state requires a memory tracker".to_string()
        })?;
        unsafe { ptr.cast::<DistinctSet>().write(DistinctSet::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe { ptr.cast::<DistinctSet>().drop_in_place() };
    }

    fn retained_bytes(&self, _spec: &AggSpec, ptr: *const u8) -> usize {
        unsafe { get_set(ptr).retained_bytes() }
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
            return Err("numeric distinct batch input type mismatch".to_string());
        };
        numeric_work(|work| {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(encoded) = numeric_core::encode_numeric_row(array.as_ref(), row, work)?
                {
                    let set = unsafe { get_set_mut((base as *mut u8).add(offset)) };
                    set.insert(encoded.as_bytes())?;
                }
            }
            Ok(())
        })
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Binary(arr) = input else {
            return Err("numeric distinct merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let input_type = spec
                .input_arg_type
                .as_ref()
                .ok_or_else(|| "distinct numeric logical input signature missing".to_string())?;
            let width = numeric_key_width(input_type)?;
            let set = unsafe { get_set_mut((base as *mut u8).add(offset)) };
            visit_serialized_keys(arr.value(row), width, |value| set.insert(value))?;
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
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            let mut payload_bytes = 0usize;
            for &base in group_states {
                let set = unsafe { get_set((base as *const u8).add(offset)) };
                if set.is_empty() {
                    builder.append_null();
                } else {
                    let encoded = serialize_set(set)?;
                    payload_bytes = payload_bytes
                        .checked_add(encoded.len())
                        .filter(|bytes| *bytes <= i32::MAX as usize)
                        .ok_or_else(|| {
                            "distinct state batch exceeds the Binary offset domain".to_string()
                        })?;
                    builder.append_value(encoded);
                }
            }
            return Ok(std::sync::Arc::new(builder.finish()));
        }

        let input_type = spec
            .input_arg_type
            .as_ref()
            .ok_or_else(|| "multi_distinct_sum input_arg_type missing".to_string())?;

        let mut arrays = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let set = unsafe { get_set((base as *const u8).add(offset)) };
            let array = if matches!(spec.kind, AggKind::MultiDistinctAvg) {
                avg_from_set(set, input_type, &spec.output_type)?
            } else {
                sum_from_set(set, input_type, &spec.output_type)?
            };
            arrays.push(array);
        }

        // Each array is a single-value array. Concatenate into a batch.
        let values: Vec<&dyn arrow::array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
        // Use concat to combine single-value arrays.
        arrow::compute::kernels::concat::concat(&values).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use std::mem::MaybeUninit;

    // Exercise sealed selected-binding kernels, local serialization, tree merge,
    // and replay. Expected values below are independent of the set codec.
    fn prepared_distinct_avg_tree(input_type: DataType, partitions: Vec<ArrayRef>) -> ArrayRef {
        use crate::exec::expr::agg::{
            AggStateArena, build_kernel_set, test_builtin_execution_function_set,
        };
        use crate::exec::node::aggregate::AggTypeSignature;
        use novarocks_functions::AggregateInputBatch;
        let functions = test_builtin_execution_function_set();
        let selected = functions
            .catalog()
            .resolve_aggregate_trusted("multi_distinct_avg", &[input_type.clone()])
            .unwrap();
        assert_eq!(selected.intermediate_type, DataType::Binary);
        assert_eq!(
            selected.state_format.as_str(),
            "novarocks/multi_distinct_avg/state-v1"
        );
        let function = |merge| AggFunction {
            name: "multi_distinct_avg".to_string(),
            input_is_intermediate: merge,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(selected.output_type.clone()),
                input_arg_type: Some(input_type.clone()),
            }),
            ..Default::default()
        };
        let update = build_kernel_set(
            &functions,
            &[function(false)],
            &[Some(input_type.clone())],
            &[selected.clone()],
        )
        .unwrap();
        let merge = build_kernel_set(
            &functions,
            &[function(true)],
            &[Some(DataType::Binary)],
            &[selected],
        )
        .unwrap();
        let tracker = MemTracker::new_root("prepared-distinct-avg-tree");
        let mut arena = AggStateArena::new(4096);
        arena.try_set_mem_tracker(tracker.clone()).unwrap();
        let update = &update.entries[0];
        let merge = &merge.entries[0];
        let mut partials = Vec::new();
        for partition in partitions {
            let pointer = arena.alloc(update.state.size, update.state_align());
            update
                .init_state_with_tracker(pointer, tracker.clone())
                .unwrap();
            update
                .update_batch(
                    &vec![pointer; partition.len()],
                    AggregateInputBatch::try_new(Some(&partition), partition.len()).unwrap(),
                )
                .unwrap();
            partials.push(update.build_array(&[pointer], true).unwrap());
            update.drop_state(pointer);
        }
        let tree = arena.alloc(merge.state.size, merge.state_align());
        merge
            .init_state_with_tracker(tree, tracker.clone())
            .unwrap();
        for partial in &partials {
            merge
                .merge_batch(
                    &[tree],
                    AggregateInputBatch::try_new(Some(partial), 1).unwrap(),
                )
                .unwrap();
        }
        let intermediate = merge.build_array(&[tree], true).unwrap();
        merge.drop_state(tree);
        let root = arena.alloc(merge.state.size, merge.state_align());
        merge
            .init_state_with_tracker(root, tracker.clone())
            .unwrap();
        // Replaying an earlier partial must not count its values again.
        for partial in std::iter::once(&intermediate).chain(partials.first()) {
            merge
                .merge_batch(
                    &[root],
                    AggregateInputBatch::try_new(Some(partial), 1).unwrap(),
                )
                .unwrap();
        }
        let result = merge.build_array(&[root], false).unwrap();
        merge.drop_state(root);
        drop(arena);
        assert_eq!(
            tracker.current(),
            0,
            "all retained and serialization allocations must be released"
        );
        result
    }

    #[test]
    fn distinct_avg_prepared_tree_unions_overlapping_partitions_and_replayed_state() {
        let result = prepared_distinct_avg_tree(
            DataType::Int64,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(3)])),
                Arc::new(Int64Array::from(vec![Some(3), Some(8), None])),
                Arc::new(Int64Array::from(vec![None])),
            ],
        );
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![Some(4.0)]);
    }

    #[test]
    fn distinct_avg_prepared_supports_each_existing_integer_and_float_input_width() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int16Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Float32Array::from(vec![1.0, 1.0, 2.0, 3.0])),
            Arc::new(Float64Array::from(vec![1.0, 1.0, 2.0, 3.0])),
        ];
        for values in arrays {
            let result = prepared_distinct_avg_tree(values.data_type().clone(), vec![values]);
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                2.0
            );
        }
    }

    #[test]
    fn distinct_avg_prepared_numeric_empty_and_signed_zero_controls() {
        let result = prepared_distinct_avg_tree(
            DataType::Int64,
            vec![Arc::new(Int64Array::from(vec![None, None]))],
        );
        assert!(result.is_null(0));
        let result = prepared_distinct_avg_tree(
            DataType::Float64,
            vec![
                Arc::new(Float64Array::from(vec![0.0, -0.0, 4.0])),
                Arc::new(Float64Array::from(vec![4.0])),
            ],
        );
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
        let result = prepared_distinct_avg_tree(
            DataType::Int64,
            vec![Arc::new(Int64Array::from(vec![i64::MAX, i64::MAX - 1]))],
        );
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            i64::MAX as f64
        );
    }

    #[test]
    fn distinct_avg_prepared_decimal_retains_canonical_scale_and_signed_rounding() {
        for (input_type, coefficients, expected_type, expected_coefficient) in [
            (
                DataType::Decimal128(10, 2),
                vec![101, 101, 202, 202],
                DataType::Decimal128(38, 8),
                151_500_000,
            ),
            (
                DataType::Decimal128(20, 10),
                vec![10_000_000_000, 30_000_000_000],
                DataType::Decimal128(38, 12),
                2_000_000_000_000,
            ),
            (
                DataType::Decimal128(38, 13),
                vec![1, 2, 2],
                DataType::Decimal128(38, 13),
                2,
            ),
            (
                DataType::Decimal128(38, 13),
                vec![-1, -2, -2],
                DataType::Decimal128(38, 13),
                -2,
            ),
        ] {
            let DataType::Decimal128(precision, scale) = input_type else {
                unreachable!()
            };
            let array = Decimal128Array::from(coefficients)
                .with_precision_and_scale(precision, scale)
                .unwrap();
            let left = array.slice(0, 1);
            let right = array.slice(1, array.len() - 1);
            let result =
                prepared_distinct_avg_tree(input_type, vec![Arc::new(left), Arc::new(right)]);
            assert_eq!(result.data_type(), &expected_type);
            let result = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert!(!result.is_null(0));
            assert_eq!(result.value(0), expected_coefficient);
        }
    }

    #[test]
    fn distinct_avg_prepared_decimal256_unions_wide_coefficients_without_narrowing() {
        let unit = crate::exec::expr::decimal::pow10_i256(50).unwrap();
        assert!(unit.to_i128().is_none());
        let array = |values| -> ArrayRef {
            Arc::new(
                Decimal256Array::from(values)
                    .with_precision_and_scale(60, 2)
                    .unwrap(),
            )
        };
        let result = prepared_distinct_avg_tree(
            DataType::Decimal256(60, 2),
            vec![
                array(vec![
                    Some(unit),
                    Some(unit),
                    Some(unit * i256::from_i128(3)),
                ]),
                array(vec![
                    Some(unit * i256::from_i128(3)),
                    Some(unit * i256::from_i128(5)),
                    None,
                ]),
            ],
        );
        assert_eq!(result.data_type(), &DataType::Decimal256(60, 2));
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value(0),
            unit * i256::from_i128(3)
        );
        let null =
            prepared_distinct_avg_tree(DataType::Decimal256(60, 2), vec![array(vec![None, None])]);
        assert!(null.is_null(0));
    }

    #[test]
    fn distinct_avg_prepared_decimal256_mean_fits_when_sum_exceeds_i256() {
        let ten76 = crate::exec::expr::decimal::pow10_i256(76).unwrap();
        let values: Vec<_> = (1..=6)
            .map(|offset| ten76 - i256::from_i128(offset))
            .collect();
        assert!(
            values
                .iter()
                .try_fold(i256::ZERO, |sum, value| sum.checked_add(*value))
                .is_none()
        );
        for negative in [false, true] {
            let array = Decimal256Array::from(
                values
                    .iter()
                    .map(|value| if negative { -*value } else { *value })
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(76, 0)
            .unwrap();
            let result = prepared_distinct_avg_tree(
                DataType::Decimal256(76, 0),
                vec![Arc::new(array.slice(0, 4)), Arc::new(array.slice(2, 4))],
            );
            let expected = ten76 - i256::from_i128(3);
            assert_eq!(result.data_type(), &DataType::Decimal256(76, 0));
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap()
                    .value(0),
                if negative { -expected } else { expected }
            );
        }
    }

    #[test]
    fn distinct_avg_prepared_decimal256_rounds_half_up_with_opposite_signs() {
        for (coefficients, expected) in [
            (vec![1, 2, 2], 2),
            (vec![-1, -2, -2], -2),
            (vec![-1, 2, 2], 1),
            (vec![-2, 1, 1], -1),
        ] {
            let array = Decimal256Array::from(
                coefficients
                    .into_iter()
                    .map(i256::from_i128)
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(50, 17)
            .unwrap();
            let result =
                prepared_distinct_avg_tree(DataType::Decimal256(50, 17), vec![Arc::new(array)]);
            assert_eq!(result.data_type(), &DataType::Decimal256(50, 17));
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap()
                    .value(0),
                i256::from_i128(expected)
            );
        }
    }

    #[test]
    fn distinct_numeric_decimal256_frame_preserves_all_32_bytes_and_validates_last_key() {
        let unit = crate::exec::expr::decimal::pow10_i256(50).unwrap();
        let expected = [unit, unit * i256::from_i128(3)];
        let mut frame = 2_u32.to_le_bytes().to_vec();
        for value in expected {
            frame.extend_from_slice(&32_u32.to_le_bytes());
            frame.extend_from_slice(&value.to_le_bytes());
        }
        let mut decoded = Vec::new();
        visit_serialized_keys(&frame, 32, |bytes| {
            decoded.push(i256::from_le_bytes(bytes.try_into().unwrap()));
            Ok(())
        })
        .unwrap();
        assert_eq!(decoded, expected);
        // Keep the overall frame length correct but corrupt its last key width.
        frame[40..44].copy_from_slice(&31_u32.to_le_bytes());
        let mut visited = 0;
        assert!(
            visit_serialized_keys(&frame, 32, |_| {
                visited += 1;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(visited, 0);
    }

    #[test]
    fn distinct_numeric_merge_rejects_malformed_state_before_visiting_any_key() {
        for bytes in [
            vec![],
            vec![255, 255, 255, 255],
            vec![0, 0, 0, 0, 1],
            [vec![1, 0, 0, 0, 7, 0, 0, 0], vec![0; 8]].concat(),
            // A valid first key followed by a malformed last key must also
            // reject before the visitor can mutate any destination state.
            [
                vec![2, 0, 0, 0, 8, 0, 0, 0],
                vec![0; 8],
                vec![7, 0, 0, 0],
                vec![0; 8],
            ]
            .concat(),
        ] {
            let mut visited = 0;
            assert!(
                visit_serialized_keys(&bytes, 8, |_| {
                    visited += 1;
                    Ok(())
                })
                .is_err()
            );
            assert_eq!(visited, 0);
        }
    }

    #[test]
    fn distinct_numeric_serialization_reserves_tracked_scratch_before_allocation() {
        let tracker = MemTracker::new_root("distinct-serialization-limit");
        let mut set = DistinctSet::new(tracker.clone());
        set.insert(&7_i64.to_le_bytes()).unwrap();
        let retained = tracker.current();
        // One encoded i64 needs count + key length + key bytes = 16 bytes.
        tracker.install_limit_once(retained + 15).unwrap();
        assert!(
            serialize_set(&set)
                .unwrap_err()
                .contains("ResourceExhausted")
        );
        assert_eq!(tracker.current(), retained);
        assert_eq!(set.len(), 1);
        drop(set);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn distinct_avg_tracked_state_rejects_growth_before_mutation() {
        let function = AggFunction {
            name: "multi_distinct_avg".to_string(),
            ..Default::default()
        };
        let spec = MultiDistinctNumericAgg
            .build_spec_from_type(&function, Some(&DataType::Int64), false)
            .unwrap();
        let tracker = MemTracker::new_root("distinct-avg-limit");
        tracker.install_limit_once(1).unwrap();
        let mut state = MaybeUninit::<DistinctSet>::uninit();
        MultiDistinctNumericAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(tracker.clone()))
            .unwrap();
        let values = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        let result = MultiDistinctNumericAgg.update_batch(
            &spec,
            0,
            &[state.as_mut_ptr() as AggStatePtr],
            &AggInputView::Any(&values),
        );
        assert!(result.unwrap_err().contains("ResourceExhausted"));
        assert_eq!(unsafe { state.assume_init_ref() }.len(), 0);
        assert_eq!(tracker.current(), 0);
        MultiDistinctNumericAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_multi_distinct_sum_spec() {
        let func = AggFunction {
            name: "multi_distinct_sum".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Int64),
            }),
            ..Default::default()
        };
        let spec = MultiDistinctNumericAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::MultiDistinctSum));
    }

    #[test]
    fn test_multi_distinct_sum_decimal128_spec_uses_canonical_sum_type() {
        let func = AggFunction {
            name: "multi_distinct_sum".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Decimal128(38, 2)),
                input_arg_type: Some(DataType::Decimal128(32, 2)),
            }),
            ..Default::default()
        };
        let spec = MultiDistinctNumericAgg
            .build_spec_from_type(&func, Some(&DataType::Decimal128(32, 2)), false)
            .unwrap();
        assert_eq!(spec.output_type, DataType::Decimal128(38, 2));
    }

    #[test]
    fn test_multi_distinct_sum_updates() {
        let func = AggFunction {
            name: "multi_distinct_sum".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Int64),
            }),
            ..Default::default()
        };
        let spec = MultiDistinctNumericAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();

        let values = Arc::new(Int64Array::from(vec![1, 2, 2, 3])) as ArrayRef;
        let input = AggInputView::Any(&values);

        let mut state = MaybeUninit::<DistinctSet>::uninit();
        MultiDistinctNumericAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 4];
        MultiDistinctNumericAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = MultiDistinctNumericAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MultiDistinctNumericAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out_arr.value(0), 6);
    }

    #[test]
    fn allocation_tracker_fails_before_hash_growth_and_releases_on_drop() {
        let spec = MultiDistinctNumericAgg
            .build_spec_from_type(
                &AggFunction {
                    name: "multi_distinct_sum".to_string(),
                    types: Some(crate::exec::node::aggregate::AggTypeSignature {
                        intermediate_type: Some(DataType::Binary),
                        output_type: Some(DataType::Int64),
                        input_arg_type: Some(DataType::Int64),
                    }),
                    ..Default::default()
                },
                Some(&DataType::Int64),
                false,
            )
            .unwrap();
        let tracker = MemTracker::new_root("multi-distinct-sum");
        tracker.install_limit_once(1).unwrap();
        let mut state = MaybeUninit::<DistinctSet>::uninit();
        MultiDistinctNumericAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let values = Arc::new(Int64Array::from(vec![7])) as ArrayRef;
        let input = AggInputView::Any(&values);

        let error = MultiDistinctNumericAgg
            .update_batch(&spec, 0, &[state.as_mut_ptr() as AggStatePtr], &input)
            .expect_err("hash allocation must be rejected before mutation");
        assert!(error.contains("ResourceExhausted"), "{error}");
        assert_eq!(unsafe { state.assume_init_ref() }.retained_bytes(), 0);
        assert_eq!(tracker.current(), 0);

        MultiDistinctNumericAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }
}
