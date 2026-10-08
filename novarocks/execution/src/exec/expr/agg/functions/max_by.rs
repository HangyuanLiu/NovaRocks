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
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::super::*;
use super::AggregateFunction;
use super::common::build_scalar_array;

pub(super) struct MaxMinByAgg;

type MaxMinByState = novarocks_functions::builtin::aggregate_by_core::ByState<AggregateAllocator>;
fn direction(kind: &AggKind) -> novarocks_functions::builtin::aggregate_by_core::ByDirection {
    if is_max_kind(kind) {
        novarocks_functions::builtin::aggregate_by_core::ByDirection::Maximum
    } else {
        novarocks_functions::builtin::aggregate_by_core::ByDirection::Minimum
    }
}
struct LegacyByBuffer(Vec<u8>);
impl novarocks_functions::builtin::aggregate_by_core::ByEncodeBuffer for LegacyByBuffer {
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
fn is_max_kind(kind: &AggKind) -> bool {
    matches!(kind, AggKind::MaxBy | AggKind::MaxByV2)
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "max_by" => Some(AggKind::MaxBy),
        "max_by_v2" => Some(AggKind::MaxByV2),
        "min_by" => Some(AggKind::MinBy),
        "min_by_v2" => Some(AggKind::MinByV2),
        _ => None,
    }
}

impl AggregateFunction for MaxMinByAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let kind = kind_from_name(func.name.as_str())
            .ok_or_else(|| format!("unsupported max_by/min_by: {}", func.name))?;
        let data_type = input_type.ok_or_else(|| "max_by/min_by input type missing".to_string())?;

        if input_is_intermediate {
            let sig = super::super::agg_type_signature(func)
                .ok_or_else(|| "max_by/min_by type signature missing".to_string())?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| "max_by/min_by output type signature missing".to_string())?;
            return Ok(AggSpec {
                kind,
                output_type: output_type.clone(),
                intermediate_type: data_type.clone(),
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        match data_type {
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err("max_by/min_by expects 2 arguments".to_string());
                }
                let value_type = fields[0].data_type().clone();
                Ok(AggSpec {
                    kind,
                    output_type: value_type,
                    intermediate_type: DataType::Binary,
                    input_arg_type: None,
                    count_all: false,
                })
            }
            other => Err(format!(
                "max_by/min_by expects struct input, got {:?}",
                other
            )),
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MaxBy | AggKind::MinBy | AggKind::MaxByV2 | AggKind::MinByV2 => (
                std::mem::size_of::<MaxMinByState>(),
                std::mem::align_of::<MaxMinByState>(),
            ),
            other => unreachable!("unexpected kind for max_by/min_by: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "max_by/min_by input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "max_by/min_by merge input missing".to_string())?;
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(bin))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked max_by/min_by state requires tracker-aware init");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked max_by/min_by state requires a memory tracker".to_string()
        })?;
        unsafe {
            ptr.cast::<MaxMinByState>()
                .write(MaxMinByState::new(AggregateAllocator::new(tracker)))
        };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MaxMinByState);
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
            return Err("max_by/min_by batch input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "max_by/min_by expects struct input".to_string())?;
        if struct_arr.num_columns() != 2 {
            return Err("max_by/min_by expects 2 arguments".to_string());
        }
        let value_arr = struct_arr.column(0);
        let key_arr = struct_arr.column(1);

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MaxMinByState) };
            state
                .update_from_arrays(
                    direction(&spec.kind),
                    value_arr,
                    row,
                    key_arr,
                    row,
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
        let AggInputView::Binary(arr) = input else {
            return Err("max_by/min_by merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MaxMinByState) };
            state
                .merge_bytes(
                    direction(&spec.kind),
                    bytes,
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
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let state = unsafe { &*((base as *mut u8).add(offset) as *const MaxMinByState) };
                let mut buf = LegacyByBuffer(Vec::new());
                if state
                    .serialize(
                        &mut buf,
                        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
                    )
                    .map_err(|error| error.to_string())?
                {
                    builder.append_value(&buf.0);
                } else {
                    builder.append_null();
                }
            }
            return Ok(std::sync::Arc::new(builder.finish()));
        }

        let mut values = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const MaxMinByState) };
            values.push(
                state
                    .output(&mut novarocks_functions::aggregate_scalar::ScalarWork::new(
                        None,
                    ))
                    .map_err(|error| error.to_string())?,
            );
        }
        build_scalar_array(&spec.output_type, values)
    }
}

#[cfg(test)]
mod tests {
    use super::super::common::AggScalarValue;
    use super::*;
    use crate::exec::expr::ExprId;
    use crate::exec::expr::agg::{
        AggKernelSet, AggStateArena, build_kernel_set, test_builtin_execution_function_set,
    };
    use crate::exec::node::aggregate::AggTypeSignature;
    use arrow::array::{
        Array, Decimal128Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Fields};
    use novarocks_functions::AggregateInputBatch;
    use std::mem::MaybeUninit;

    fn bound_max_min_by(
        name: &str,
        value_type: DataType,
        key_type: DataType,
        merge: bool,
    ) -> AggKernelSet {
        let function_set = test_builtin_execution_function_set();
        let selected = function_set
            .catalog()
            .resolve_aggregate_trusted(name, &[value_type.clone(), key_type.clone()])
            .expect("resolve public max_by/min_by");
        let evaluated_type = if merge {
            DataType::Binary
        } else {
            DataType::Struct(
                vec![
                    Field::new("value", value_type.clone(), true),
                    Field::new("key", key_type, true),
                ]
                .into(),
            )
        };
        let func = AggFunction {
            name: name.to_string(),
            // The resolved signature has two logical arguments; execution packs them into one struct.
            inputs: vec![ExprId(0)],
            input_is_intermediate: merge,
            types: Some(AggTypeSignature {
                intermediate_type: Some(selected.intermediate_type.clone()),
                output_type: Some(selected.output_type.clone()),
                input_arg_type: Some(value_type),
            }),
            ..Default::default()
        };
        build_kernel_set(&function_set, &[func], &[Some(evaluated_type)], &[selected])
            .expect("bind public max_by/min_by kernel")
    }

    fn bound_max_min_states(
        kernels: &AggKernelSet,
        count: usize,
    ) -> (AggStateArena, Vec<AggStatePtr>) {
        let tracker = MemTracker::new_root("max-min-by-null-value-test");
        let mut arena = AggStateArena::new(1024);
        arena.set_mem_tracker(Arc::clone(&tracker));
        let kernel = &kernels.entries[0];
        let states = (0..count)
            .map(|_| {
                let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
                kernel
                    .init_state_with_tracker(base, Arc::clone(&tracker))
                    .expect("init tracked state");
                base
            })
            .collect();
        (arena, states)
    }

    fn packed_value_key(values: ArrayRef, keys: ArrayRef) -> ArrayRef {
        let fields = Fields::from(vec![
            Field::new("value", values.data_type().clone(), true),
            Field::new("key", keys.data_type().clone(), true),
        ]);
        Arc::new(StructArray::new(fields, vec![values, keys], None))
    }

    #[test]
    fn bound_max_min_by_keeps_null_value_winner_and_ignores_null_key() {
        for (name, null_key, worse_key) in [("min_by", 6, 7), ("max_by", 10000, 9500)] {
            let kernels = bound_max_min_by(name, DataType::Int64, DataType::Int64, false);
            let kernel = &kernels.entries[0];
            let (_arena, states) = bound_max_min_states(&kernels, 4);
            // Group zero has a NULL winner followed by a worse non-NULL row.
            // Group one is untouched, group two has only NULL keys, group three is a control.
            let input = packed_value_key(
                Arc::new(Int64Array::from(vec![
                    Some(4),
                    Some(9),
                    None,
                    Some(999),
                    Some(5),
                    Some(7),
                    None,
                    Some(8),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(4008),
                    Some(9006),
                    Some(null_key),
                    None,
                    Some(worse_key),
                    None,
                    None,
                    Some(1),
                ])),
            );
            let destinations = [
                states[0], states[0], states[0], states[0], states[0], states[2], states[2],
                states[3],
            ];
            kernel
                .update_batch(
                    &destinations,
                    AggregateInputBatch::try_new(Some(&input), input.len()).unwrap(),
                )
                .unwrap();
            let output = kernel.build_array(&states, false).unwrap();
            let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(output.len(), 4);
            assert!(output.is_null(0));
            assert!(output.is_null(1));
            assert!(output.is_null(2));
            assert!(!output.is_null(3));
            assert_eq!(output.value(3), 8);
            let partial = kernel.build_array(&states, true).unwrap();
            // A selected NULL value must retain a non-NULL partial carrying its valid key.
            assert!(!partial.is_null(0));
            assert!(partial.is_null(1));
            assert!(partial.is_null(2));
            assert!(!partial.is_null(3));
            for base in states {
                kernel.drop_state(base);
            }
        }
    }

    #[test]
    fn bound_max_min_by_merges_null_value_winner_in_both_orders() {
        for (name, winner_key, worse_key) in [("min_by", 6, 4008), ("max_by", 10000, 9006)] {
            let local = bound_max_min_by(name, DataType::Int64, DataType::Int64, false);
            let local_kernel = &local.entries[0];
            let (_local_arena, local_states) = bound_max_min_states(&local, 3);
            let input = packed_value_key(
                Arc::new(Int64Array::from(vec![None, Some(4), Some(999)])),
                Arc::new(Int64Array::from(vec![
                    Some(winner_key),
                    Some(worse_key),
                    None,
                ])),
            );
            local_kernel
                .update_batch(
                    &local_states,
                    AggregateInputBatch::try_new(Some(&input), input.len()).unwrap(),
                )
                .unwrap();
            let partial = local_kernel.build_array(&local_states, true).unwrap();
            let partial = partial.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert!(!partial.is_null(0));
            assert!(!partial.is_null(1));
            assert!(partial.is_null(2));
            let input: ArrayRef = Arc::new(BinaryArray::from(vec![
                Some(partial.value(0)),
                Some(partial.value(1)),
                None,
                Some(partial.value(1)),
                Some(partial.value(0)),
                None,
            ]));
            let merged = bound_max_min_by(name, DataType::Int64, DataType::Int64, true);
            let merged_kernel = &merged.entries[0];
            let (_merged_arena, merged_states) = bound_max_min_states(&merged, 2);
            let destinations = [
                merged_states[0],
                merged_states[0],
                merged_states[0],
                merged_states[1],
                merged_states[1],
                merged_states[1],
            ];
            merged_kernel
                .merge_batch(
                    &destinations,
                    AggregateInputBatch::try_new(Some(&input), input.len()).unwrap(),
                )
                .unwrap();
            let output = merged_kernel.build_array(&merged_states, false).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.null_count(), 2);
            let partial = merged_kernel.build_array(&merged_states, true).unwrap();
            assert_eq!(partial.null_count(), 0);
            for base in local_states {
                local_kernel.drop_state(base);
            }
            for base in merged_states {
                merged_kernel.drop_state(base);
            }
        }
    }

    #[test]
    fn bound_max_min_by_preserves_decimal_type_and_exact_nullable_key_selection() {
        let decimal = DataType::Decimal128(18, 9);
        let coefficient = 111111111111111111_i128;
        let low = 123456789123456788_i128;
        let high = low + 1;
        for (name, first_key, null_key) in [("min_by", high, low), ("max_by", low, high)] {
            let kernels = bound_max_min_by(name, decimal.clone(), decimal.clone(), false);
            let kernel = &kernels.entries[0];
            let (_arena, states) = bound_max_min_states(&kernels, 2);
            // Adjacent Decimal(18,9) keys differ by 1e-9, below binary64 resolution here.
            let values: ArrayRef = Arc::new(
                Decimal128Array::from(vec![
                    Some(coefficient),
                    None,
                    Some(coefficient),
                    Some(coefficient),
                ])
                .with_precision_and_scale(18, 9)
                .unwrap(),
            );
            let keys: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(first_key), Some(null_key), None, Some(first_key)])
                    .with_precision_and_scale(18, 9)
                    .unwrap(),
            );
            let input = packed_value_key(values, keys);
            kernel
                .update_batch(
                    &[states[0], states[0], states[0], states[1]],
                    AggregateInputBatch::try_new(Some(&input), input.len()).unwrap(),
                )
                .unwrap();
            let output = kernel.build_array(&states, false).unwrap();
            assert_eq!(output.data_type(), &decimal);
            let output = output.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert!(output.is_null(0));
            assert!(!output.is_null(1));
            assert_eq!(output.value(1), coefficient);
            for base in states {
                kernel.drop_state(base);
            }
        }
    }

    fn utf8_max_by_spec() -> (AggSpec, DataType) {
        let struct_type = DataType::Struct(
            vec![
                Field::new("v", DataType::Utf8, true),
                Field::new("k", DataType::Utf8, true),
            ]
            .into(),
        );
        let func = AggFunction {
            name: "max_by".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Utf8),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = MaxMinByAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();
        (spec, struct_type)
    }

    fn utf8_struct_batch(values: Vec<&str>, keys: Vec<&str>) -> ArrayRef {
        let fields = vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("k", DataType::Utf8, true),
        ];
        Arc::new(StructArray::new(
            fields.into(),
            vec![
                Arc::new(StringArray::from(values)) as ArrayRef,
                Arc::new(StringArray::from(keys)) as ArrayRef,
            ],
            None,
        ))
    }

    #[test]
    fn test_max_by_spec() {
        let func = AggFunction {
            name: "max_by".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let struct_type = DataType::Struct(
            vec![
                Field::new("v", DataType::Int64, true),
                Field::new("k", DataType::Int64, true),
            ]
            .into(),
        );
        let spec = MaxMinByAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::MaxBy));
    }

    #[test]
    fn test_max_min_by_variants() {
        let values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let keys = Arc::new(Int64Array::from(vec![1, 3, 2])) as ArrayRef;
        let fields = vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("k", DataType::Int64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values, keys], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;
        let input = AggInputView::Any(&array_ref);

        for (name, expected) in [
            ("max_by", "b"),
            ("min_by", "a"),
            ("max_by_v2", "b"),
            ("min_by_v2", "a"),
        ] {
            let func = AggFunction {
                name: name.to_string(),
                inputs: vec![],
                input_is_intermediate: false,
                types: Some(crate::exec::node::aggregate::AggTypeSignature {
                    intermediate_type: Some(DataType::Binary),
                    output_type: Some(DataType::Utf8),
                    input_arg_type: None,
                }),
                ..Default::default()
            };
            let spec = MaxMinByAgg
                .build_spec_from_type(&func, Some(&struct_type), false)
                .unwrap();

            let tracker = MemTracker::new_root(format!("{name}-test"));
            let mut state = MaybeUninit::<MaxMinByState>::uninit();
            MaxMinByAgg
                .init_state_with_tracker(
                    &spec,
                    state.as_mut_ptr().cast(),
                    Some(Arc::clone(&tracker)),
                )
                .unwrap();
            let state_ptr = state.as_mut_ptr() as AggStatePtr;
            let state_ptrs = vec![state_ptr; 3];
            MaxMinByAgg
                .update_batch(&spec, 0, &state_ptrs, &input)
                .unwrap();
            let out = MaxMinByAgg
                .build_array(&spec, 0, &[state_ptr], false)
                .unwrap();
            MaxMinByAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

            let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(out_arr.value(0), expected);
            assert_eq!(tracker.current(), 0);
        }
    }

    #[test]
    fn max_by_tracks_update_replacement_and_drop_exactly() {
        let (spec, _) = utf8_max_by_spec();
        let tracker = MemTracker::new_root("max-by-update-test");
        let mut state = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = utf8_struct_batch(vec!["old"], vec!["a"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&initial))
            .unwrap();
        assert_eq!(tracker.current(), 4);

        let replacement = utf8_struct_batch(vec!["replacement"], vec!["z"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&replacement))
            .unwrap();
        assert_eq!(tracker.current(), 12);

        let output = MaxMinByAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "replacement"
        );

        MaxMinByAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn max_by_merge_uses_destination_allocator_and_releases_both_states() {
        let (spec, _) = utf8_max_by_spec();
        let source_tracker = MemTracker::new_root("max-by-merge-source-test");
        let destination_tracker = MemTracker::new_root("max-by-merge-destination-test");
        let mut source = MaybeUninit::<MaxMinByState>::uninit();
        let mut destination = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                source.as_mut_ptr().cast(),
                Some(Arc::clone(&source_tracker)),
            )
            .unwrap();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                destination.as_mut_ptr().cast(),
                Some(Arc::clone(&destination_tracker)),
            )
            .unwrap();
        let source_ptr = source.as_mut_ptr() as AggStatePtr;
        let destination_ptr = destination.as_mut_ptr() as AggStatePtr;

        let source_input = utf8_struct_batch(vec!["merged"], vec!["z"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[source_ptr], &AggInputView::Any(&source_input))
            .unwrap();
        let destination_input = utf8_struct_batch(vec!["old"], vec!["a"]);
        MaxMinByAgg
            .update_batch(
                &spec,
                0,
                &[destination_ptr],
                &AggInputView::Any(&destination_input),
            )
            .unwrap();

        let intermediate = MaxMinByAgg
            .build_array(&spec, 0, &[source_ptr], true)
            .unwrap();
        let intermediate = Some(intermediate);
        let merge_view = MaxMinByAgg.build_merge_view(&spec, &intermediate).unwrap();
        MaxMinByAgg
            .merge_batch(&spec, 0, &[destination_ptr], &merge_view)
            .unwrap();
        assert_eq!(destination_tracker.current(), 7);
        let output = MaxMinByAgg
            .build_array(&spec, 0, &[destination_ptr], false)
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "merged"
        );

        MaxMinByAgg.drop_state(&spec, source.as_mut_ptr().cast());
        MaxMinByAgg.drop_state(&spec, destination.as_mut_ptr().cast());
        assert_eq!(source_tracker.current(), 0);
        assert_eq!(destination_tracker.current(), 0);
    }

    #[test]
    fn max_by_oom_preserves_prior_state_and_exact_charge() {
        let (spec, _) = utf8_max_by_spec();
        let tracker = MemTracker::new_root("max-by-oom-test");
        tracker.install_limit_once(8).unwrap();
        let mut state = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = utf8_struct_batch(vec!["aa"], vec!["aa"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&initial))
            .unwrap();
        assert_eq!(tracker.current(), 4);

        // The candidate would retain only 7 bytes, but replacing the 4-byte
        // state requires admitting the complete 11-byte old + new peak.
        let rejected = utf8_struct_batch(vec!["bbbb"], vec!["zzz"]);
        let error = MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&rejected))
            .unwrap_err();
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), 4);

        let output = MaxMinByAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "aa"
        );

        MaxMinByAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn max_by_nested_value_update_and_merge_use_exact_allocators() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let map_entry_type = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Binary, true),
        ]));
        let map_type = DataType::Map(
            Arc::new(Field::new("entries", map_entry_type, false)),
            false,
        );
        let value_type = DataType::Struct(Fields::from(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("items", list_type, true),
            Field::new("attributes", map_type, true),
        ]));
        let packed_fields = Fields::from(vec![
            Field::new("v", value_type.clone(), true),
            Field::new("k", DataType::Utf8, true),
        ]);
        let packed_type = DataType::Struct(packed_fields.clone());
        let func = AggFunction {
            name: "max_by".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(value_type.clone()),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = MaxMinByAgg
            .build_spec_from_type(&func, Some(&packed_type), false)
            .unwrap();
        let nested_value = AggScalarValue::Struct(vec![
            Some(AggScalarValue::Utf8("root".to_string())),
            Some(AggScalarValue::Binary(vec![1, 2, 3])),
            Some(AggScalarValue::List(vec![
                Some(AggScalarValue::Utf8("first".to_string())),
                None,
            ])),
            Some(AggScalarValue::Map(vec![(
                Some(AggScalarValue::Utf8("key".to_string())),
                Some(AggScalarValue::Binary(vec![4, 5])),
            )])),
        ]);
        let value_array = build_scalar_array(&value_type, vec![Some(nested_value)]).unwrap();
        let key_array = Arc::new(StringArray::from(vec!["z"])) as ArrayRef;
        let packed = Arc::new(StructArray::new(
            packed_fields,
            vec![value_array, key_array],
            None,
        )) as ArrayRef;

        let source_tracker = MemTracker::new_root("nested-max-by-source-test");
        let destination_tracker = MemTracker::new_root("nested-max-by-destination-test");
        let mut source = MaybeUninit::<MaxMinByState>::uninit();
        let mut destination = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                source.as_mut_ptr().cast(),
                Some(Arc::clone(&source_tracker)),
            )
            .unwrap();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                destination.as_mut_ptr().cast(),
                Some(Arc::clone(&destination_tracker)),
            )
            .unwrap();
        let source_ptr = source.as_mut_ptr() as AggStatePtr;
        let destination_ptr = destination.as_mut_ptr() as AggStatePtr;
        MaxMinByAgg
            .update_batch(&spec, 0, &[source_ptr], &AggInputView::Any(&packed))
            .unwrap();
        assert!(source_tracker.current() > 0);

        let intermediate = MaxMinByAgg
            .build_array(&spec, 0, &[source_ptr], true)
            .unwrap();
        let intermediate = Some(intermediate);
        let merge_view = MaxMinByAgg.build_merge_view(&spec, &intermediate).unwrap();
        MaxMinByAgg
            .merge_batch(&spec, 0, &[destination_ptr], &merge_view)
            .unwrap();
        assert!(destination_tracker.current() > 0);

        let output = MaxMinByAgg
            .build_array(&spec, 0, &[destination_ptr], false)
            .unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "root"
        );
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &[1, 2, 3]
        );
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            2
        );
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap()
                .value_length(0),
            1
        );

        MaxMinByAgg.drop_state(&spec, source.as_mut_ptr().cast());
        MaxMinByAgg.drop_state(&spec, destination.as_mut_ptr().cast());
        assert_eq!(source_tracker.current(), 0);
        assert_eq!(destination_tracker.current(), 0);
    }
}

#[cfg(test)]
#[path = "max_by_baseline_tests.rs"]
mod max_by_baseline_tests;
