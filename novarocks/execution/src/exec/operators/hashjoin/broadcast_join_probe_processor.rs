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
//! Broadcast hash-join probe processor.
//!
//! Responsibilities:
//! - Probes a fully replicated build-side hash table with probe-side chunks.
//! - Implements join-type specific match/miss output shaping for broadcast joins.
//!
//! Key exported interfaces:
//! - Types: `BroadcastJoinProbeProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use std::sync::Arc;

use super::broadcast_join_shared::BroadcastJoinSharedState;
use super::hash_join_probe_core::{HashJoinProbeCore, join_type_str};
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinType;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{InputConstructionFactory, InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    shape::{Transform, TransformKernel},
    step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::runtime_state::RuntimeState;
use tracing::debug;

fn parse_join_node_id_from_dep_key(dep_key: &str) -> i32 {
    // Expected format: "broadcast_join_build:<node_id>"
    dep_key
        .strip_prefix("broadcast_join_build:")
        .unwrap_or(dep_key)
        .parse::<i32>()
        .unwrap_or(-1)
}

/// Factory for broadcast hash-join probe operators that read a replicated build artifact.
pub struct BroadcastJoinProbeProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    join_type: JoinType,
    probe_keys: Vec<ExprId>,
    residual_predicate: Option<ExprId>,
    probe_is_left: bool,
    has_equi_keys: bool,
    left_chunk_schema: ChunkSchemaRef,
    right_chunk_schema: ChunkSchemaRef,
    join_scope_chunk_schema: ChunkSchemaRef,
    state: Arc<BroadcastJoinSharedState>,
}

impl BroadcastJoinProbeProcessorFactory {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_native(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        has_equi_keys: bool,
        left_chunk_schema: ChunkSchemaRef,
        right_chunk_schema: ChunkSchemaRef,
        join_scope_chunk_schema: ChunkSchemaRef,
        state: Arc<BroadcastJoinSharedState>,
    ) -> Self {
        Self::new_in_mode(
            arena,
            join_type,
            probe_keys,
            residual_predicate,
            probe_is_left,
            has_equi_keys,
            left_chunk_schema,
            right_chunk_schema,
            join_scope_chunk_schema,
            state,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_in_mode(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        has_equi_keys: bool,
        left_chunk_schema: ChunkSchemaRef,
        right_chunk_schema: ChunkSchemaRef,
        join_scope_chunk_schema: ChunkSchemaRef,
        state: Arc<BroadcastJoinSharedState>,
    ) -> Self {
        let node_id = parse_join_node_id_from_dep_key(state.dep_name());
        Self {
            name: format!("HASH_JOIN (id={})", node_id),
            arena,
            join_type,
            probe_keys,
            residual_predicate,
            probe_is_left,
            has_equi_keys,
            left_chunk_schema,
            right_chunk_schema,
            join_scope_chunk_schema,
            state,
        }
    }
}

impl OperatorFactory for BroadcastJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(BroadcastJoinProbeProcessorKernel {
            name: self.name.clone(),
            state: Arc::clone(&self.state),
            driver_id,
            dep: self.state.dep(),
            core: HashJoinProbeCore::new_native(
                Arc::clone(&self.arena),
                self.join_type,
                self.probe_keys.clone(),
                self.residual_predicate,
                self.probe_is_left,
                self.has_equi_keys,
                Arc::clone(&self.left_chunk_schema),
                Arc::clone(&self.right_chunk_schema),
                Arc::clone(&self.join_scope_chunk_schema),
            ),
            input_rows: 0,
            input_chunks: 0,
            buffered_rows: 0,
            profile_initialized: false,
            finalizing: false,
            profiles: None,
        }))
    }
}

struct BroadcastJoinProbeProcessorKernel {
    name: String,
    state: Arc<BroadcastJoinSharedState>,
    driver_id: i32,
    dep: DependencyHandle,
    core: HashJoinProbeCore,
    input_rows: usize,
    input_chunks: usize,
    buffered_rows: usize,
    profile_initialized: bool,
    finalizing: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
}

impl OperatorLifecycle for BroadcastJoinProbeProcessorKernel {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.core.set_construction_factory(factory);
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.core.release_owned();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.core.release_owned();
    }
    fn on_driver_failure(&mut self) {
        self.core.release_owned();
    }
}
impl InputEncodingPolicy for BroadcastJoinProbeProcessorKernel {}
impl TransformKernel for BroadcastJoinProbeProcessorKernel {
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        Ok(self.build_wait().map(AdmissionNext::Wait))
    }
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        self.init_profile_if_needed();
        let result = (|| {
            self.load_build_side()?;
            if !chunk.is_empty() {
                self.input_rows = self.input_rows.saturating_add(chunk.len());
                self.input_chunks = self.input_chunks.saturating_add(1);
            }
            self.core.accept_probe(ctx, chunk)
        })();
        self.complete_step(ctx, result)
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if self.finalizing {
            return self.advance_finalization(ctx);
        }
        let result = self.core.drive_probe(ctx);
        if result.is_err() {
            self.core.release_owned();
        }
        result
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        self.finalizing = true;
        self.advance_finalization(ctx)
    }
}

impl BroadcastJoinProbeProcessorKernel {
    // EOS is signalled once by the shape. Build readiness can arrive later;
    // that wake must resume this same finalization instead of requesting input.
    fn advance_finalization(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let Some(wait) = self.build_wait() {
            return Ok(StepResult::new(None, NextStep::Wait(wait), 0));
        }
        self.init_profile_if_needed();
        let result = self
            .finish_one()
            .map(|output| StepResult::new(output, NextStep::Finished, 0))
            .map_err(Into::into);
        let step = self.complete_step(ctx, result)?;
        self.core.release_owned();
        Ok(step)
    }

    fn build_wait(&self) -> Option<WaitFact> {
        let observable = self.dep.observable();
        let observed_generation = observable.generation();
        if self.core.is_build_loaded() || self.state.has_build() {
            return None;
        }
        Some(WaitFact {
            reason: WaitReason::Dependency,
            observable,
            observed_generation,
            deadline: None,
        })
    }
    fn complete_step(
        &mut self,
        ctx: &StepContext<'_>,
        result: ExecutionResult<StepResult>,
    ) -> ExecutionResult<StepResult> {
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
            self.core.release_owned();
        }
        result
    }
    fn init_profile_if_needed(&mut self) {
        if self.profile_initialized {
            return;
        }
        self.profile_initialized = true;
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .add_info_string("JoinType", join_type_str(self.core.join_type()));
            profile
                .common
                .add_info_string("DistributionMode", "BROADCAST");
            let search = profile.common.add_timer("SearchHashTableTime");
            let out_build = profile.common.add_timer("OutputBuildColumnTime");
            let out_probe = profile.common.add_timer("OutputProbeColumnTime");
            self.core.set_phase_timers(search, out_build, out_probe);
        }
    }

    fn finish_one(&mut self) -> Result<Option<Chunk>, crate::runtime::fragment::ExecutionFailure> {
        self.load_build_side()?;
        let join_type = self.core.join_type();
        let emits_build = matches!(join_type, JoinType::FullOuter | JoinType::RightOuter)
            || (self.core.probe_is_left()
                && matches!(join_type, JoinType::RightAnti | JoinType::RightSemi));
        let output = if emits_build {
            let flags = self.core.take_build_matched().unwrap_or_default();
            match self.state.merge_build_matched(flags)? {
                Some(merged) => self
                    .core
                    .finish_from_probe_output(None, Some(merged), false)?,
                None => None,
            }
        } else {
            self.core.finish_from_probe_output(None, None, true)?
        };
        self.log_stats();
        Ok(output)
    }

    fn log_stats(&self) {
        debug!(
            "BroadcastJoinProbe finished: dep_key={} driver_id={} input_rows={} input_chunks={} buffered_rows={} output_rows={} build_store_rows={} build_table={} probe_keys={}",
            self.dep.name(),
            self.driver_id,
            self.input_rows,
            self.input_chunks,
            self.buffered_rows,
            self.core.output_rows(),
            self.core.build_store_rows(),
            self.core.build_table_present(),
            self.core.probe_keys_len()
        );
    }

    fn load_build_side(&mut self) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
        if self.core.is_build_loaded() {
            return Ok(());
        }
        let Some(artifact) = self.state.get_build() else {
            return Err(super::build_state::build_contract(
                "broadcast join build not ready",
            ));
        };
        let global_rows = artifact.build_row_count;
        let global_has_null = artifact.build_has_null_key;
        self.core
            .set_build_artifact(artifact, global_rows, global_has_null)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::pipeline::test_driver::TestDriver;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::join::JoinDistributionMode;
    use crate::exec::node::join::JoinType;
    use crate::exec::operators::hashjoin::HashJoinBuildSinkFactory;
    use crate::exec::operators::hashjoin::broadcast_join_shared::BroadcastJoinSharedState;
    use crate::exec::operators::hashjoin::build_state::JoinBuildSinkState;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::exec::pipeline::{
        input::{PreparedInput, RawInput},
        shape::{Transform, TransformKernel},
        step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason},
        step_context::StepContext,
    };
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    use super::BroadcastJoinProbeProcessorFactory;

    fn chunk_of(slot_id: SlotId, values: &[i32]) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[slot_id],
            )
            .expect("chunk schema");
            super::super::probe_step_test_support::int32_source(Chunk::new_with_chunk_schema(
                batch,
                chunk_schema,
            ))
        }
    }

    fn chunk_of_two(
        k_slot_id: SlotId,
        v_slot_id: SlotId,
        k: &[i32],
        v: &[i32],
        v_name: &str,
    ) -> Chunk {
        assert_eq!(k.len(), v.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new(v_name, DataType::Int32, false),
        ]));
        let k_arr = Arc::new(Int32Array::from(k.to_vec())) as arrow::array::ArrayRef;
        let v_arr = Arc::new(Int32Array::from(v.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![k_arr, v_arr]).expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[k_slot_id, v_slot_id],
            )
            .expect("chunk schema");
            super::super::probe_step_test_support::int32_source(Chunk::new_with_chunk_schema(
                batch,
                chunk_schema,
            ))
        }
    }

    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        let types = schema
            .fields()
            .iter()
            .map(|field| {
                assert_eq!(
                    field.data_type(),
                    &DataType::Int32,
                    "fixed original join fixture declaration"
                );
                novarocks_type_contract::FunctionValueType::new(
                    DataType::Int32,
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();
        let layout = crate::exec::chunk::typed_fixture::layout(schema.clone(), slot_ids, &types);
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("join fixture schema construction"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap()
        .create_compiled_chunk_schema(&layout)
        .expect("actual declared join fixture schema")
    }

    fn append_pairs(chunk: Chunk, pairs: &mut Vec<(i32, i32)>) {
        let left_arr = chunk
            .columns()
            .first()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_arr = chunk
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            pairs.push((left_arr.value(i), right_arr.value(i)));
        }
    }

    fn append_pair_columns(
        chunk: Chunk,
        left_index: usize,
        right_index: usize,
        pairs: &mut Vec<(i32, i32)>,
    ) {
        let left_arr = chunk
            .columns()
            .get(left_index)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_arr = chunk
            .columns()
            .get(right_index)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            pairs.push((left_arr.value(i), right_arr.value(i)));
        }
    }

    #[test]
    fn broadcast_inner_join_shares_build_across_probers() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let arena = Arc::new(arena);

        let left_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("k", DataType::Int32, false),
        ]));

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(BroadcastJoinSharedState::new(1, dep_manager, 1));

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Broadcast,
            build_state,
        );
        let probe_factory = BroadcastJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            None,
            true,
            true,
            super::super::probe_step_test_support::int32_schema(&left_schema, &[SlotId::new(1)]),
            super::super::probe_step_test_support::int32_schema(&right_schema, &[SlotId::new(2)]),
            super::super::probe_step_test_support::int32_schema(
                &join_scope_schema,
                &[SlotId::new(1), SlotId::new(2)],
            ),
            Arc::clone(&join_state),
        );

        let build = chunk_of(SlotId::new(2), &[1, 2, 4]);
        let probe_left = chunk_of(SlotId::new(1), &[2, 3]);
        let probe_right = chunk_of(SlotId::new(1), &[4]);

        let mut probe_op_left = probe_factory.create(2, 0);
        super::super::probe_step_test_support::install(probe_op_left.as_mut(), &rt);
        use crate::exec::pipeline::{input::RawInput, step::InputResult, step_context::TurnWork};
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut work);
        let original = probe_left.columns()[0].clone();
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(wait),
        } = probe_op_left
            .as_mut()
            .try_push(&mut ctx, RawInput::for_test(probe_left))
        else {
            panic!("build must defer raw probe input");
        };
        assert!(Arc::ptr_eq(
            &input.original_for_test().columns()[0],
            &original
        ));

        let mut build_op = build_factory.create(1, 0);
        super::super::probe_step_test_support::install(build_op.as_mut(), &rt);
        let mut sink = TestDriver::new(build_op.as_mut(), &rt);
        sink.input_without_output(build).expect("build push");
        sink.signal_eos().expect("build EOS");
        assert!(sink.collect_until_finished().unwrap().is_empty());
        assert_ne!(wait.observable.generation(), wait.observed_generation);
        let step = probe_op_left.as_mut();
        assert!(matches!(
            step.try_push(&mut ctx, input),
            InputResult::Accepted(Ok(_))
        ));
        assert!(step.drive(&mut ctx).unwrap().output.is_none());
        let mut proc_left = TestDriver::new(probe_op_left.as_mut(), &rt);
        let mut pairs = Vec::new();
        while matches!(proc_left.next(), NextStep::Runnable | NextStep::Yield) {
            if let Some(out) = proc_left
                .advance_to_boundary()
                .map(|step| step.output)
                .expect("probe pull")
            {
                append_pairs(out, &mut pairs);
            }
        }
        proc_left.signal_eos().expect("probe finish");
        while matches!(proc_left.next(), NextStep::Runnable | NextStep::Yield) {
            if let Some(out) = proc_left
                .advance_to_boundary()
                .map(|step| step.output)
                .expect("probe pull")
            {
                append_pairs(out, &mut pairs);
            }
        }

        let mut probe_op_right = probe_factory.create(2, 1);
        super::super::probe_step_test_support::install(probe_op_right.as_mut(), &rt);
        let mut proc_right = TestDriver::new(probe_op_right.as_mut(), &rt);
        proc_right
            .accept_without_output(probe_right)
            .expect("probe process");
        while matches!(proc_right.next(), NextStep::Runnable | NextStep::Yield) {
            if let Some(out) = proc_right
                .advance_to_boundary()
                .map(|step| step.output)
                .expect("probe pull")
            {
                append_pairs(out, &mut pairs);
            }
        }
        proc_right.signal_eos().expect("probe finish");
        while matches!(proc_right.next(), NextStep::Runnable | NextStep::Yield) {
            if let Some(out) = proc_right
                .advance_to_boundary()
                .map(|step| step.output)
                .expect("probe pull")
            {
                append_pairs(out, &mut pairs);
            }
        }

        pairs.sort();
        assert_eq!(pairs, vec![(2, 2), (4, 4)]);
    }

    #[test]
    fn broadcast_left_semi_join_outputs_probe_side() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let arena = Arc::new(arena);

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("k", DataType::Int32, false),
        ]));

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(BroadcastJoinSharedState::new(1, dep_manager, 1));

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Broadcast,
            build_state,
        );
        let probe_factory = BroadcastJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[SlotId::new(1), SlotId::new(2)]),
            chunk_schema_of(&right_schema, &[SlotId::new(3)]),
            chunk_schema_of(
                &join_scope_schema,
                &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
            ),
            Arc::clone(&join_state),
        );

        // Left semi: probe is left, build is right.
        let build = chunk_of(SlotId::new(3), &[1, 3]);
        let probe = chunk_of_two(
            SlotId::new(1),
            SlotId::new(2),
            &[1, 2, 3],
            &[100, 200, 300],
            "v",
        );

        let mut probe_op = probe_factory.create(1, 0);
        super::super::probe_step_test_support::install(probe_op.as_mut(), &rt);
        use crate::exec::pipeline::{input::RawInput, step::InputResult, step_context::TurnWork};
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut work);
        let step = probe_op.as_mut();
        let original = probe.columns()[0].clone();
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(wait),
        } = step.try_push(&mut ctx, RawInput::for_test(probe))
        else {
            panic!("build dependency leaves the raw probe on its edge");
        };
        assert!(Arc::ptr_eq(
            &input.original_for_test().columns()[0],
            &original
        ));
        let mut build_op = build_factory.create(1, 0);
        super::super::probe_step_test_support::install(build_op.as_mut(), &rt);
        let mut sink = TestDriver::new(build_op.as_mut(), &rt);
        sink.input_without_output(build).unwrap();
        sink.signal_eos().unwrap();
        assert!(sink.collect_until_finished().unwrap().is_empty());
        assert_ne!(wait.observable.generation(), wait.observed_generation);
        assert!(matches!(
            step.try_push(&mut ctx, input),
            InputResult::Accepted(Ok(_))
        ));
        let computed = step.drive(&mut ctx).unwrap();
        let mut rows = Vec::new();
        if let Some(out) = computed.output {
            append_pairs(out, &mut rows);
        }
        step.finish(&mut ctx).unwrap();
        loop {
            let next = step.drive(&mut ctx).unwrap();
            if let Some(out) = next.output {
                append_pairs(out, &mut rows);
            }
            if matches!(next.next, NextStep::Finished) {
                break;
            }
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 100), (3, 300)]);
    }

    #[test]
    fn broadcast_right_semi_join_with_probe_left_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));
        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(BroadcastJoinSharedState::new(1, dep_manager, 1));

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            true,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Broadcast,
            build_state,
        );
        let probe_factory = BroadcastJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[SlotId::new(1), SlotId::new(2)]),
            chunk_schema_of(&right_schema, &[SlotId::new(3), SlotId::new(4)]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    SlotId::new(1),
                    SlotId::new(2),
                    SlotId::new(3),
                    SlotId::new(4),
                ],
            ),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(SlotId::new(3), SlotId::new(4), &[1, 1, 2], &[5, 20, 7], "w");
        let probe = chunk_of_two(
            SlotId::new(1),
            SlotId::new(2),
            &[1, 1, 2],
            &[10, 15, 100],
            "v",
        );

        let mut probe_op = probe_factory.create(1, 0);
        super::super::probe_step_test_support::install(probe_op.as_mut(), &rt);
        use crate::exec::pipeline::{input::RawInput, step::InputResult, step_context::TurnWork};
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut work);
        let step = probe_op.as_mut();
        let original = probe.columns()[0].clone();
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(wait),
        } = step.try_push(&mut ctx, RawInput::for_test(probe))
        else {
            panic!("build dependency leaves the raw probe on its edge");
        };
        assert!(Arc::ptr_eq(
            &input.original_for_test().columns()[0],
            &original
        ));
        let mut build_op = build_factory.create(1, 0);
        super::super::probe_step_test_support::install(build_op.as_mut(), &rt);
        let mut sink = TestDriver::new(build_op.as_mut(), &rt);
        sink.input_without_output(build).unwrap();
        sink.signal_eos().unwrap();
        assert!(sink.collect_until_finished().unwrap().is_empty());
        assert_ne!(wait.observable.generation(), wait.observed_generation);
        assert!(matches!(
            step.try_push(&mut ctx, input),
            InputResult::Accepted(Ok(_))
        ));
        let computed = step.drive(&mut ctx).unwrap();
        let mut rows = Vec::new();
        if let Some(out) = computed.output {
            append_pair_columns(out, 2, 3, &mut rows);
        }
        step.finish(&mut ctx).unwrap();
        loop {
            let next = step.drive(&mut ctx).unwrap();
            if let Some(out) = next.output {
                append_pair_columns(out, 2, 3, &mut rows);
            }
            if matches!(next.next, NextStep::Finished) {
                break;
            }
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 20)]);
    }
}
