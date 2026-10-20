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

//! Actual preparation capability installation for direct join-kernel oracles.
use crate::{
    exec::pipeline::operator::{InputConstructionFactory, Operator},
    runtime::{
        mem_tracker::MemTracker,
        runtime_state::{RuntimeErrorState, RuntimeState},
    },
};
use std::sync::Arc;
pub(super) fn factory(
    error: Arc<RuntimeErrorState>,
) -> (Arc<MemTracker>, Arc<dyn InputConstructionFactory>) {
    let tracker = MemTracker::new_root("join probe fixture construction");
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker.clone(), error,
    ).unwrap();
    (tracker, factory)
}
pub(super) fn install(operator: &mut dyn Operator, runtime: &RuntimeState) -> Arc<MemTracker> {
    let (tracker, factory) = factory(runtime.error_state());
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(factory);
    operator.bind_runtime_state(runtime).unwrap();
    operator.prepare().unwrap();
    tracker
}

/// Closed join fixtures author Int32 scalar channels explicitly. This is input
/// construction for those oracles, not runtime inference from Arrow metadata.
pub(super) fn int32_schema(
    schema: &arrow::datatypes::Schema,
    ids: &[novarocks_types::SlotId],
) -> crate::exec::chunk::ChunkSchemaRef {
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let storage = ValueTypeStorageLifetime::pure();
    let slots = schema
        .fields()
        .iter()
        .zip(ids)
        .map(|(field, id)| {
            let ty = arrow::datatypes::DataType::Int32;
            let binding = TypedValueBinding::try_new(
                ValueTypeRef::try_new(
                    ValueKind::Scalar(ty.clone()),
                    field.is_nullable(),
                    storage.clone(),
                )
                .unwrap(),
                ValueLayout::try_new(&ty, storage.clone()).unwrap(),
            )
            .unwrap();
            ChunkSlotSchema::try_new_typed(*id, field.clone(), binding, None, storage.clone())
                .unwrap()
        })
        .collect();
    Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap())
}
pub(super) fn int32_source(source: crate::exec::chunk::Chunk) -> crate::exec::chunk::Chunk {
    let ids = source
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| slot.slot_id())
        .collect::<Vec<_>>();
    let schema = int32_schema(source.record_batch().schema_ref(), &ids);
    crate::exec::chunk::Chunk::try_new_typed_with_columns_observed::<
        crate::runtime::fragment::ExecutionFailure,
        _,
    >(schema, source.columns().to_vec(), source.len(), |_| Ok(()))
    .unwrap()
}

#[test]
fn empty_probe_eos_waits_for_build_then_emits_unmatched_rows_once_in_both_distributions() {
    use crate::exec::{
        chunk::Chunk,
        expr::{ExprArena, ExprNode},
        node::join::{JoinDistributionMode, JoinType},
        operators::hashjoin::{
            HashJoinBuildSinkFactory,
            broadcast_join_probe_processor::BroadcastJoinProbeProcessorFactory,
            broadcast_join_shared::BroadcastJoinSharedState, build_state::JoinBuildSinkState,
            partitioned_join_probe_processor::PartitionedJoinProbeProcessorFactory,
            partitioned_join_shared::PartitionedJoinSharedState,
        },
        pipeline::{
            dependency::DependencyManager,
            input::RawInput,
            operator_factory::OperatorFactory,
            step::{InputResult, NextStep},
            step_context::{StepContext, TurnWork},
        },
    };
    use arrow::{
        array::{Array, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use novarocks_types::SlotId;

    for partitioned in [false, true] {
        let runtime = RuntimeState::default();
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let arena = Arc::new(arena);
        let left = int32_schema(
            &Schema::new(vec![Field::new("probe", DataType::Int32, false)]),
            &[SlotId::new(1)],
        );
        let right = int32_schema(
            &Schema::new(vec![Field::new("build", DataType::Int32, false)]),
            &[SlotId::new(2)],
        );
        let scope = int32_schema(
            &Schema::new(vec![
                Field::new("probe", DataType::Int32, true),
                Field::new("build", DataType::Int32, true),
            ]),
            &[SlotId::new(1), SlotId::new(2)],
        );
        let (probe_factory, shared): (Box<dyn OperatorFactory>, Arc<dyn JoinBuildSinkState>) =
            if partitioned {
                let state = Arc::new(PartitionedJoinSharedState::new(
                    51,
                    1,
                    DependencyManager::new(),
                    false,
                ));
                (
                    Box::new(PartitionedJoinProbeProcessorFactory::new_native(
                        arena.clone(),
                        JoinType::FullOuter,
                        vec![probe_key],
                        None,
                        true,
                        true,
                        left,
                        right.clone(),
                        scope,
                        state.clone(),
                    )),
                    state,
                )
            } else {
                let state = Arc::new(BroadcastJoinSharedState::new(
                    51,
                    DependencyManager::new(),
                    1,
                ));
                (
                    Box::new(BroadcastJoinProbeProcessorFactory::new_native(
                        arena.clone(),
                        JoinType::FullOuter,
                        vec![probe_key],
                        None,
                        true,
                        true,
                        left,
                        right.clone(),
                        scope,
                        state.clone(),
                    )),
                    state,
                )
            };
        let build_factory = HashJoinBuildSinkFactory::new_native(
            arena,
            JoinType::FullOuter,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            if partitioned {
                JoinDistributionMode::Partitioned
            } else {
                JoinDistributionMode::Broadcast
            },
            shared,
        );
        let mut probe = probe_factory.create(1, 0);
        let tracker = install(probe.as_mut(), &runtime);
        let control = runtime.error_state();
        let mut probe_turn = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut probe_turn);
        let probe_step = probe.as_mut();
        probe_step.finish(&mut ctx).unwrap();
        let first = probe_step.drive(&mut ctx).unwrap();
        assert!(first.output.is_none());
        let NextStep::Wait(wait) = first.next else {
            panic!("EOS must wait for the actual unpublished build");
        };
        let covered = tracker.current();
        for _ in 0..3 {
            let step = probe_step.drive(&mut ctx).unwrap();
            assert!(step.output.is_none());
            assert!(matches!(step.next, NextStep::Wait(_)));
            assert_eq!(
                tracker.current(),
                covered,
                "same-generation wait performs no finalization allocation"
            );
        }
        let mut build = build_factory.create(1, 0);
        install(build.as_mut(), &runtime);
        let batch = RecordBatch::try_new(
            right.arrow_schema_ref(),
            vec![Arc::new(Int32Array::from(vec![7]))],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, right);
        let mut build_turn = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut build_ctx = StepContext::new(&control, &mut build_turn);
        let build_step = build.as_mut();
        assert!(matches!(
            build_step.try_push(&mut build_ctx, RawInput::for_test(chunk)),
            InputResult::Accepted(Ok(_))
        ));
        for _ in 0..8 {
            let step = build_step.drive(&mut build_ctx).unwrap();
            assert!(step.output.is_none());
            if matches!(step.next, NextStep::NeedInput) {
                break;
            }
        }
        build_step.finish(&mut build_ctx).unwrap();
        let mut finished = false;
        for _ in 0..8 {
            let step = build_step.drive(&mut build_ctx).unwrap();
            assert!(step.output.is_none());
            if matches!(step.next, NextStep::Finished) {
                finished = true;
                break;
            }
        }
        assert!(
            finished,
            "the real build publishes its artifact before probe continuation"
        );
        assert_ne!(wait.observable.generation(), wait.observed_generation);
        let completed = probe_step.drive(&mut ctx).unwrap();
        assert!(matches!(completed.next, NextStep::Finished));
        let output = completed
            .output
            .expect("full outer join emits the one unmatched build row");
        assert_eq!(output.len(), 1);
        assert!(output.columns()[0].is_null(0));
        assert_eq!(
            output.columns()[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
        probe_step.finish(&mut ctx).unwrap();
        let after = probe_step.drive(&mut ctx).unwrap();
        assert!(after.output.is_none());
        assert!(matches!(after.next, NextStep::Finished));
        assert!(runtime.error().is_none());
    }
}
