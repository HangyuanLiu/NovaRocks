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
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};
use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Builder};
#[cfg(test)]
use arrow::array::{
    Date32Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::DataType;
use novarocks_functions::aggregate_scalar::ScalarWork;
use novarocks_functions::builtin::aggregate_count_distinct_core::{
    self as core, CountDistinctState, LegacyCountReader,
};
type DistinctSet = CountDistinctState<AggregateAllocator>;
impl From<Arc<MemTracker>> for AggregateAllocator {
    fn from(tracker: Arc<MemTracker>) -> Self {
        Self::new(tracker)
    }
}
pub(super) struct CountDistinctAgg;
unsafe fn get_or_init_set<'a>(ptr: *mut u8) -> &'a mut DistinctSet {
    unsafe { &mut *ptr.cast::<DistinctSet>() }
}
fn serialize_set(set: &DistinctSet) -> Vec<u8> {
    core::serialize_set(set, Vec::new(), &mut ScalarWork::new(None))
        .expect("legacy serialization uses infallible scratch")
}
fn deserialize_set(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    core::deserialize_set(bytes, Vec::new(), &mut ScalarWork::new(None))
        .map_err(|error| error.to_string())
}
impl AggregateFunction for CountDistinctAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        if input_type.is_none() {
            return Err("count_distinct requires 1 argument".to_string());
        }
        // multi_distinct_count is the FE-internal name for all COUNT(DISTINCT) operations.
        // Count all non-null distinct values regardless of sign.
        let kind = AggKind::CountDistinct;
        Ok(AggSpec {
            kind,
            output_type: DataType::Int64,
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::CountDistinct => (
                std::mem::size_of::<DistinctSet>(),
                std::mem::align_of::<DistinctSet>(),
            ),
            other => unreachable!("unexpected kind for count_distinct: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_distinct input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_distinct intermediate input missing".to_string())?;
        let binary = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(binary))
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
            "allocation-tracked count_distinct state requires a memory tracker".to_string()
        })?;
        unsafe { ptr.cast::<DistinctSet>().write(DistinctSet::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe { ptr.cast::<DistinctSet>().drop_in_place() };
    }

    fn retained_bytes(&self, _spec: &AggSpec, ptr: *const u8) -> usize {
        unsafe { (&*ptr.cast::<DistinctSet>()).retained_bytes() }
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
            return Err("count_distinct batch input type mismatch".to_string());
        };
        core::validate_legacy_array(array)?;
        for (row, &base) in state_ptrs.iter().enumerate() {
            let key = core::encode_row(
                array,
                row,
                &LegacyCountReader,
                Vec::new(),
                &mut ScalarWork::new(None),
            )
            .map_err(|error| error.to_string())?;
            if let Some(key) = key {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let set = unsafe { get_or_init_set(ptr) };
                set.insert(key)?;
            }
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
        let AggInputView::Binary(arr) = input else {
            return Err("count_distinct merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let values = deserialize_set(arr.value(row))?;
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let set = unsafe { get_or_init_set(ptr) };
            core::merge_decoded(set, values, &mut ScalarWork::new(None))
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let set = unsafe { &*ptr.cast::<DistinctSet>() };
                let bytes = serialize_set(set);
                builder.append_value(bytes);
            }
            return Ok(std::sync::Arc::new(builder.finish()));
        }

        let mut builder = Int64Builder::new();
        for &base in group_states {
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let count = unsafe { (&*ptr.cast::<DistinctSet>()).len() };
            builder.append_value(count as i64);
        }
        Ok(std::sync::Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    #[cfg(feature = "core-pipeline-integration")]
    use std::collections::HashMap;
    #[cfg(feature = "core-pipeline-integration")]
    use std::time::Duration;

    #[cfg(feature = "core-pipeline-integration")]
    use arrow::array::Int32Array;
    use arrow::array::{ArrayRef, Int64Array, ListArray, NullArray};
    use arrow::datatypes::{DataType, Int32Type};
    #[cfg(feature = "core-pipeline-integration")]
    use arrow::datatypes::{Field, Schema};
    #[cfg(feature = "core-pipeline-integration")]
    use arrow::record_batch::RecordBatch;

    use super::{AggregateFunction, CountDistinctAgg, DistinctSet, get_or_init_set};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::agg::{AggInputView, AggStatePtr};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::expr::{ExprArena, ExprNode};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::node::aggregate::AggregateNode;
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::node::values::ValuesNode;
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::pipeline::executor::execute_native_plan_with_pipeline;
    use crate::runtime::mem_tracker::MemTracker;
    #[cfg(feature = "core-pipeline-integration")]
    use crate::runtime::runtime_state::RuntimeState;
    #[cfg(feature = "core-pipeline-integration")]
    use novarocks_types::SlotId;

    #[cfg(feature = "core-pipeline-integration")]
    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    #[test]
    fn count_distinct_supports_int_list_values() {
        let values = vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(2), Some(1)]),
            None,
        ];
        let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(values)) as ArrayRef;
        let func = AggFunction {
            name: "multi_distinct_count".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = CountDistinctAgg
            .build_spec_from_type(&func, Some(array.data_type()), false)
            .expect("count distinct spec");

        let mut state = MaybeUninit::<DistinctSet>::uninit();
        CountDistinctAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; array.len()];
        CountDistinctAgg
            .update_batch(&spec, 0, &state_ptrs, &AggInputView::Any(&array))
            .expect("update list values");
        let out = CountDistinctAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .expect("output");
        CountDistinctAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 2);
    }

    #[test]
    fn count_distinct_ignores_null_typed_input() {
        let array = Arc::new(NullArray::new(3)) as ArrayRef;
        let func = AggFunction {
            name: "multi_distinct_count".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = CountDistinctAgg
            .build_spec_from_type(&func, Some(array.data_type()), false)
            .expect("count distinct spec");

        let mut state = MaybeUninit::<DistinctSet>::uninit();
        CountDistinctAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; array.len()];
        CountDistinctAgg
            .update_batch(&spec, 0, &state_ptrs, &AggInputView::Any(&array))
            .expect("update null values");
        let out = CountDistinctAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .expect("output");
        CountDistinctAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 0);
    }

    #[cfg(feature = "core-pipeline-integration")]
    #[test]
    fn group_by_multi_distinct_count_is_correct_with_dop_2() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3, 3])) as arrow::array::ArrayRef;
        let vals =
            Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8, 9, 9])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };
        let output_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("cnt", DataType::Int64, true),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![k],
                    functions: vec![AggFunction {
                        name: "multi_distinct_count".to_string(),
                        inputs: vec![v],
                        input_is_intermediate: false,
                        types: Some(AggTypeSignature {
                            intermediate_type: Some(DataType::Binary),
                            output_type: Some(DataType::Int64),
                            input_arg_type: None,
                        }),
                        ..Default::default()
                    }],
                    resolved_aggregates: vec![
                        crate::exec::expr::agg::test_builtin_execution_function_set()
                            .catalog()
                            .resolve_aggregate_trusted("multi_distinct_count", &[DataType::Int32])
                            .expect("resolved builtin aggregate"),
                    ],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_chunk_schema,
                    runtime_filter_spec: crate::exec::node::aggregate::AggregateRuntimeFilterSpec {
                        topn_producers: Vec::new(),
                    },
                    streaming_preaggregation_mode: None,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            2,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut out: HashMap<i32, i64> = HashMap::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            assert_eq!(chunk.columns().len(), 2);
            let k_arr = chunk
                .columns()
                .first()
                .expect("k column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("k Int32");
            let v_arr = chunk
                .columns()
                .get(1)
                .expect("distinct count column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("distinct count Int64");
            for i in 0..chunk.len() {
                out.insert(k_arr.value(i), v_arr.value(i));
            }
        }

        assert_eq!(out.get(&1).copied(), Some(2));
        assert_eq!(out.get(&2).copied(), Some(1));
        assert_eq!(out.get(&3).copied(), Some(3));
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn retained_bytes_track_unique_key_capacity_and_drop_clears_slot() {
        let mut slot = MaybeUninit::<DistinctSet>::uninit();
        let ptr = slot.as_mut_ptr().cast::<u8>();
        let tracker = MemTracker::new_root("count-distinct-test");
        unsafe {
            ptr.cast::<DistinctSet>()
                .write(DistinctSet::new(Arc::clone(&tracker)))
        };
        let state = unsafe { get_or_init_set(ptr) };
        state.insert(Vec::with_capacity(32)).unwrap();
        let first = tracker.current();
        assert!(first > 0);

        state.insert(Vec::with_capacity(32)).unwrap();
        assert_eq!(state.values.len(), 1);
        assert_eq!(tracker.current(), first);

        let func = CountDistinctAgg;
        let spec = func
            .build_spec_from_type(&AggFunction::default(), Some(&DataType::Int64), false)
            .expect("build spec");
        assert_eq!(func.retained_bytes(&spec, ptr), 0);
        func.drop_state(&spec, ptr);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
#[path = "count_distinct_baseline_tests.rs"]
mod count_distinct_baseline_tests;
