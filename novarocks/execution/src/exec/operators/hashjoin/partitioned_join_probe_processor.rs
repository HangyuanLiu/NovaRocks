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
//! Partitioned hash-join probe processor.
//!
//! Responsibilities:
//! - Probes partition-scoped build artifacts and joins probe rows with matching partitions.
//! - Coordinates partition readiness and emits join-type specific output batches.
//!
//! Key exported interfaces:
//! - Types: `PartitionedJoinProbeProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use std::sync::Arc;

use super::hash_join_probe_core::{HashJoinProbeCore, join_type_str};
use super::partitioned_join_shared::PartitionedJoinSharedState;
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

/// Factory for partitioned hash-join probe operators over per-partition build artifacts.
pub struct PartitionedJoinProbeProcessorFactory {
    name: String,
    node_id: i32,
    arena: Arc<ExprArena>,
    join_type: JoinType,
    probe_keys: Vec<ExprId>,
    residual_predicate: Option<ExprId>,
    probe_is_left: bool,
    has_equi_keys: bool,
    left_chunk_schema: ChunkSchemaRef,
    right_chunk_schema: ChunkSchemaRef,
    join_scope_chunk_schema: ChunkSchemaRef,
    state: Arc<PartitionedJoinSharedState>,
}

impl PartitionedJoinProbeProcessorFactory {
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
        state: Arc<PartitionedJoinSharedState>,
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
        state: Arc<PartitionedJoinSharedState>,
    ) -> Self {
        let node_id = parse_join_node_id_from_dep_key(state.dep_name(0));
        Self {
            name: format!("HASH_JOIN (id={})", node_id),
            node_id,
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

impl OperatorFactory for PartitionedJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let partition = driver_id.max(0) as usize;
        debug!(
            "PartitionedJoinProbe create: node_id={} driver_id={} partition={} join_type={} probe_keys={}",
            self.node_id,
            driver_id,
            partition,
            join_type_str(self.join_type),
            self.probe_keys.len()
        );
        Box::new(Transform::hydrating(PartitionedJoinProbeProcessorKernel {
            name: self.name.clone(),
            node_id: self.node_id,
            driver_id,
            state: Arc::clone(&self.state),
            partition,
            dep: self.state.dep_handle(partition),
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
            buffered_rows: 0,
            input_rows: 0,
            input_chunks: 0,
            profile_initialized: false,
            finalizing: false,
            profiles: None,
        }))
    }
}

struct PartitionedJoinProbeProcessorKernel {
    name: String,
    node_id: i32,
    driver_id: i32,
    state: Arc<PartitionedJoinSharedState>,
    partition: usize,
    dep: DependencyHandle,
    core: HashJoinProbeCore,
    buffered_rows: u64,
    input_rows: u64,
    input_chunks: u64,
    profile_initialized: bool,
    finalizing: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
}

impl OperatorLifecycle for PartitionedJoinProbeProcessorKernel {
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
impl InputEncodingPolicy for PartitionedJoinProbeProcessorKernel {}
impl TransformKernel for PartitionedJoinProbeProcessorKernel {
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
                self.input_rows = self.input_rows.saturating_add(
                    u64::try_from(chunk.len()).expect("supported row count fits u64"),
                );
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

impl PartitionedJoinProbeProcessorKernel {
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
        if self.core.is_build_loaded() || self.state.is_partition_ready(self.partition) {
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
                .add_info_string("DistributionMode", "PARTITIONED");
            let search = profile.common.add_timer("SearchHashTableTime");
            let out_build = profile.common.add_timer("OutputBuildColumnTime");
            let out_probe = profile.common.add_timer("OutputProbeColumnTime");
            self.core.set_phase_timers(search, out_build, out_probe);
        }
    }
}

fn parse_join_node_id_from_dep_key(dep_key: &str) -> i32 {
    let after_prefix = dep_key.strip_prefix("join_build:").unwrap_or(dep_key);
    let node_id_str = after_prefix.split(':').next().unwrap_or("");
    node_id_str.parse::<i32>().unwrap_or(-1)
}

impl PartitionedJoinProbeProcessorKernel {
    fn finish_one(&mut self) -> Result<Option<Chunk>, crate::runtime::fragment::ExecutionFailure> {
        self.load_build_side()?;
        let output = self.core.finish_from_probe_output(None, None, true)?;
        self.log_stats();
        Ok(output)
    }

    fn load_build_side(&mut self) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
        if self.core.is_build_loaded() {
            return Ok(());
        }
        if self.partition >= self.state.partition_count() {
            return Err(super::build_state::build_contract(
                "join probe partition out of bounds",
            ));
        }

        let Some(artifact) = self.state.take_build_partition(self.partition)? else {
            return Err(super::build_state::build_contract(
                "partitioned join build not ready",
            ));
        };

        let global_rows = self.state.global_build_row_count();
        let global_has_null = self.state.global_build_has_null_key();
        self.core
            .set_build_artifact(artifact, global_rows, global_has_null)?;
        Ok(())
    }

    fn log_stats(&self) {
        debug!(
            "PartitionedJoinProbe finished: dep_key={} driver_id={} partition={} node_id={} join_type={} input_rows={} input_chunks={} buffered_rows={} output_rows={} build_partition_rows={} build_partition_has_null_key={} build_store_rows={} build_table={} probe_keys={} residual_predicate={} lookup_hit_rows={} lookup_miss_rows={} residual_rows_checked={} residual_eval_batches={} residual_eval_pairs={} residual_matched_rows={} residual_group_rows_total={}",
            self.dep.name(),
            self.driver_id,
            self.partition,
            self.node_id,
            join_type_str(self.core.join_type()),
            self.input_rows,
            self.input_chunks,
            self.buffered_rows,
            self.core.output_rows(),
            self.core.build_partition_row_count(),
            self.core.build_partition_has_null_key(),
            self.core.build_store_rows(),
            self.core.build_table_present(),
            self.core.probe_keys_len(),
            self.core.has_residual_predicate(),
            self.core.lookup_hit_rows(),
            self.core.lookup_miss_rows(),
            self.core.residual_rows_checked(),
            self.core.residual_eval_batches(),
            self.core.residual_eval_pairs(),
            self.core.residual_matched_rows(),
            self.core.residual_group_rows_total()
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::pipeline::operator::Operator;
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::join::JoinDistributionMode;
    use crate::exec::node::join::JoinType;
    use crate::exec::operators::hashjoin::HashJoinBuildSinkFactory;
    use crate::exec::operators::hashjoin::build_state::JoinBuildSinkState;
    use crate::exec::operators::hashjoin::partitioned_join_shared::PartitionedJoinSharedState;
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

    use super::PartitionedJoinProbeProcessorFactory;

    const LEFT_K_SLOT_ID: SlotId = SlotId::new(1);
    const LEFT_V_SLOT_ID: SlotId = SlotId::new(2);
    const RIGHT_K_SLOT_ID: SlotId = SlotId::new(3);
    const RIGHT_W_SLOT_ID: SlotId = SlotId::new(4);
    const LEFT_K_SLOT_IDS: [SlotId; 1] = [LEFT_K_SLOT_ID];
    const RIGHT_K_SLOT_IDS: [SlotId; 1] = [RIGHT_K_SLOT_ID];
    const LEFT_KV_SLOT_IDS: [SlotId; 2] = [LEFT_K_SLOT_ID, LEFT_V_SLOT_ID];
    const RIGHT_KV_SLOT_IDS: [SlotId; 2] = [RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID];
    const JOIN_K_SLOT_IDS: [SlotId; 2] = [LEFT_K_SLOT_ID, RIGHT_K_SLOT_ID];
    const JOIN_KV_SLOT_IDS: [SlotId; 4] = [
        LEFT_K_SLOT_ID,
        LEFT_V_SLOT_ID,
        RIGHT_K_SLOT_ID,
        RIGHT_W_SLOT_ID,
    ];

    fn schema_k(_slot_id: SlotId, nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::Int32,
            nullable,
        )]))
    }

    fn schema_kv(
        _k_slot_id: SlotId,
        _v_slot_id: SlotId,
        v_name: &str,
        nullable: bool,
    ) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, nullable),
            Field::new(v_name, DataType::Int32, nullable),
        ]))
    }

    fn join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
        let mut fields = left.fields().to_vec();
        fields.extend(right.fields().to_vec());
        Arc::new(Schema::new(fields))
    }

    fn chunk_schema_of(schema: &SchemaRef, slot_ids: &[SlotId]) -> ChunkSchemaRef {
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

    fn accept_probe_input(op: &mut dyn Operator, rt: &RuntimeState, chunk: Chunk) {
        match op.try_push_for_test(rt, crate::exec::pipeline::input::RawInput::for_test(chunk)) {
            crate::exec::pipeline::step::InputResult::Accepted(result) => {
                assert!(result.expect("accepted input").output.is_none())
            }
            _ => panic!("fixture probe input is admissible"),
        }
    }
    fn drain_probe_work(
        op: &mut dyn Operator,
        rt: &RuntimeState,
        mut visit: impl FnMut(Chunk),
    ) -> NextStep {
        let control = rt.error_state();
        let mut turn =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        for _ in 0..100_000 {
            let step = op
                .drive(&mut StepContext::new(&control, &mut turn))
                .expect("finite join fixture step");
            if let Some(output) = step.output {
                visit(output);
            }
            match step.next {
                next @ (NextStep::NeedInput | NextStep::Finished) => return next,
                NextStep::Yield => turn.begin_turn(64).unwrap(),
                NextStep::Runnable => {}
                NextStep::Wait(wait) => {
                    panic!("the fixture built every partition before probing: {wait:?}")
                }
            }
        }
        panic!("finite join fixture exceeded its step bound");
    }
    fn push_build_input(op: &mut dyn Operator, rt: &RuntimeState, chunk: Chunk) {
        accept_probe_input(op, rt, chunk);
        assert!(matches!(
            drain_probe_work(op, rt, |_| panic!("build has no output edge")),
            NextStep::NeedInput
        ));
    }
    fn finish_build(op: &mut dyn Operator, rt: &RuntimeState) {
        op.finish_for_test(rt).expect("build EOS");
        assert!(matches!(
            drain_probe_work(op, rt, |_| panic!("build has no output edge")),
            NextStep::Finished
        ));
    }

    fn chunk_of(values: &[i32], k_slot_id: SlotId) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[k_slot_id],
            )
            .expect("chunk schema");
            super::super::probe_step_test_support::int32_source(Chunk::new_with_chunk_schema(
                batch,
                chunk_schema,
            ))
        }
    }

    fn chunk_of_two(
        k: &[i32],
        k_slot_id: SlotId,
        v: &[i32],
        v_slot_id: SlotId,
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

    fn chunk_of_two_nullable(
        k: &[Option<i32>],
        k_slot_id: SlotId,
        v: &[Option<i32>],
        v_slot_id: SlotId,
        v_name: &str,
    ) -> Chunk {
        assert_eq!(k.len(), v.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new(v_name, DataType::Int32, true),
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

    fn append_quads(chunk: Chunk, out: &mut Vec<(i32, i32, i32, i32)>) {
        let c0 = chunk
            .columns()
            .first()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c1 = chunk
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c2 = chunk
            .columns()
            .get(2)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c3 = chunk
            .columns()
            .get(3)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            out.push((c0.value(i), c1.value(i), c2.value(i), c3.value(i)));
        }
    }

    fn append_pairs(chunk: Chunk, out: &mut Vec<(i32, i32)>) {
        // SEMI/ANTI joins now extend probe-only output with null-filled
        // build-side columns (via extend_with_null_build_columns) so the
        // chunk may have more than 2 columns; we only care about the
        // first two (probe-side) columns.
        assert!(chunk.columns().len() >= 2);
        let c0 = chunk
            .columns()
            .first()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c1 = chunk
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            out.push((c0.value(i), c1.value(i)));
        }
    }

    fn append_pair_slots(
        chunk: Chunk,
        first_slot: SlotId,
        second_slot: SlotId,
        out: &mut Vec<(i32, i32)>,
    ) {
        let first = int32_options_by_slot(&chunk, first_slot);
        let second = int32_options_by_slot(&chunk, second_slot);
        for row in 0..chunk.len() {
            out.push((
                first[row].expect("first slot must be non-null"),
                second[row].expect("second slot must be non-null"),
            ));
        }
    }

    fn int32_options_by_slot(chunk: &Chunk, slot_id: SlotId) -> Vec<Option<i32>> {
        let array = chunk.column_by_slot_id(slot_id).expect("slot column");
        let values = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        (0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row))
                }
            })
            .collect()
    }

    type SlotQuad = (Option<i32>, Option<i32>, Option<i32>, Option<i32>);

    fn append_slot_quad_rows(chunk: Chunk, out: &mut Vec<SlotQuad>) {
        let left_k = int32_options_by_slot(&chunk, LEFT_K_SLOT_ID);
        let left_v = int32_options_by_slot(&chunk, LEFT_V_SLOT_ID);
        let right_k = int32_options_by_slot(&chunk, RIGHT_K_SLOT_ID);
        let right_w = int32_options_by_slot(&chunk, RIGHT_W_SLOT_ID);
        for row in 0..chunk.len() {
            out.push((left_k[row], left_v[row], right_k[row], right_w[row]));
        }
    }

    fn append_pair_columns(
        chunk: Chunk,
        left_index: usize,
        right_index: usize,
        out: &mut Vec<(i32, i32)>,
    ) {
        let c0 = chunk
            .columns()
            .get(left_index)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c1 = chunk
            .columns()
            .get(right_index)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            out.push((c0.value(i), c1.value(i)));
        }
    }

    #[test]
    fn partitioned_join_defers_original_probe_until_build_ready() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            1,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_k(LEFT_K_SLOT_ID, false);
        let right_schema = schema_k(RIGHT_K_SLOT_ID, false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            None,
            true,
            true,
            super::super::probe_step_test_support::int32_schema(&left_schema, &LEFT_K_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(&right_schema, &RIGHT_K_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(
                &join_scope_schema,
                &JOIN_K_SLOT_IDS,
            ),
            Arc::clone(&join_state),
        );

        let mut build = build_factory.create(1, 0);
        super::super::probe_step_test_support::install(build.as_mut(), &rt);
        let mut probe = probe_factory.create(1, 0);
        let tracker = super::super::probe_step_test_support::install(probe.as_mut(), &rt);
        use crate::exec::pipeline::{input::RawInput, step::InputResult, step_context::TurnWork};
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut work);
        let step = probe.as_mut();
        let first = chunk_of(&[1, 2], LEFT_K_SLOT_ID);
        let original = first.columns()[0].clone();
        let before = tracker.allocated();
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(wait),
        } = step.try_push(&mut ctx, RawInput::for_test(first))
        else {
            panic!("unpublished partition must retain raw input at its edge");
        };
        assert!(Arc::ptr_eq(
            &input.original_for_test().columns()[0],
            &original
        ));
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(same),
        } = step.try_push(&mut ctx, input)
        else {
            panic!("same generation must keep waiting");
        };
        assert_eq!(same.observed_generation, wait.observed_generation);
        assert!(Arc::ptr_eq(&same.observable, &wait.observable));
        assert_eq!(tracker.allocated(), before, "no preparation while waiting");
        push_build_input(
            build.as_mut(),
            &rt,
            chunk_of(&[1, 2, 3, 4], RIGHT_K_SLOT_ID),
        );
        finish_build(build.as_mut(), &rt);
        assert_ne!(wait.observable.generation(), wait.observed_generation);
        assert!(matches!(
            step.try_push(&mut ctx, input),
            InputResult::Accepted(Ok(_))
        ));
        assert!(step.drive(&mut ctx).unwrap().output.is_none());
        let first = step.drive(&mut ctx).unwrap();
        assert!(matches!(first.next, NextStep::NeedInput));
        let mut keys = first.output.unwrap().columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert!(matches!(
            step.try_push(
                &mut ctx,
                RawInput::for_test(chunk_of(&[3, 4], LEFT_K_SLOT_ID))
            ),
            InputResult::Accepted(Ok(_))
        ));
        assert!(step.drive(&mut ctx).unwrap().output.is_none());
        step.finish(&mut ctx).unwrap();
        let last = step.drive(&mut ctx).unwrap();
        assert!(matches!(last.next, NextStep::Runnable));
        keys.extend_from_slice(
            last.output.unwrap().columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
        );
        assert!(matches!(
            step.drive(&mut ctx).unwrap().next,
            NextStep::Finished
        ));
        assert!(step.drive(&mut ctx).unwrap().output.is_none());
        assert_eq!(keys, vec![1, 2, 3, 4]);
    }

    #[test]
    fn partitioned_join_drains_finish_pending_outputs_before_finished() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            1,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_k(LEFT_K_SLOT_ID, false);
        let right_schema = schema_k(RIGHT_K_SLOT_ID, false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            None,
            true,
            true,
            super::super::probe_step_test_support::int32_schema(&left_schema, &LEFT_K_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(&right_schema, &RIGHT_K_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(
                &join_scope_schema,
                &JOIN_K_SLOT_IDS,
            ),
            Arc::clone(&join_state),
        );

        let row_count = 20_000usize;
        let values = (0..row_count as i32).collect::<Vec<_>>();
        let mut build = build_factory.create(1, 0);
        super::super::probe_step_test_support::install(build.as_mut(), &rt);
        let mut probe = probe_factory.create(1, 0);
        let tracker = super::super::probe_step_test_support::install(probe.as_mut(), &rt);
        push_build_input(build.as_mut(), &rt, chunk_of(&values, RIGHT_K_SLOT_ID));
        finish_build(build.as_mut(), &rt);
        use crate::exec::pipeline::{input::RawInput, step::InputResult, step_context::TurnWork};
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 0);
        let mut ctx = StepContext::new(&control, &mut work);
        let step = probe.as_mut();
        assert!(matches!(
            step.try_push(
                &mut ctx,
                RawInput::for_test(chunk_of(&values, LEFT_K_SLOT_ID))
            ),
            InputResult::Accepted(Ok(_))
        ));
        assert!(step.drive(&mut ctx).unwrap().output.is_none());
        step.finish(&mut ctx).unwrap();
        let before = tracker.allocated();
        assert!(matches!(
            step.drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        assert_eq!(tracker.allocated(), before);
        drop(ctx);
        work.begin_turn(1).unwrap();
        let mut ctx = StepContext::new(&control, &mut work);
        let first = step.drive(&mut ctx).unwrap();
        assert!(matches!(first.next, NextStep::Runnable));
        let first = first.output.unwrap();
        assert!(first.len() < row_count);
        let before = tracker.allocated();
        let yielded = step.drive(&mut ctx).unwrap();
        assert!(yielded.output.is_none());
        assert!(matches!(yielded.next, NextStep::Yield));
        assert_eq!(tracker.allocated(), before);
        drop(ctx);
        work.begin_turn(1).unwrap();
        let mut ctx = StepContext::new(&control, &mut work);
        let last = step.drive(&mut ctx).unwrap();
        assert!(matches!(last.next, NextStep::Runnable));
        let last = last.output.unwrap();
        assert_eq!(first.len() + last.len(), row_count);
        let keys = [&first, &last]
            .into_iter()
            .flat_map(|chunk| {
                chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, values);
        assert!(matches!(
            step.drive(&mut ctx).unwrap().next,
            NextStep::Finished
        ));
        assert!(step.drive(&mut ctx).unwrap().output.is_none());
    }

    #[test]
    fn partitioned_inner_join_matches_keys_with_dop_2() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_k(LEFT_K_SLOT_ID, false);
        let right_schema = schema_k(RIGHT_K_SLOT_ID, false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            None,
            true,
            true,
            super::super::probe_step_test_support::int32_schema(&left_schema, &LEFT_K_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(&right_schema, &RIGHT_K_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(
                &join_scope_schema,
                &JOIN_K_SLOT_IDS,
            ),
            Arc::clone(&join_state),
        );

        let build = chunk_of(&[1, 2, 4], RIGHT_K_SLOT_ID);
        let probe = chunk_of(&[2, 3, 4], LEFT_K_SLOT_ID);

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        // Build per partition.
        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            {
                let sink = op.as_mut();
                let c = build_part.clone();
                if !c.is_empty() {
                    push_build_input(sink, &rt, c);
                }
                finish_build(sink, &rt);
            }
        }

        // Probe per partition.
        let mut pairs = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();

            let proc = op.as_mut();
            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                let left_arr = out
                    .columns()
                    .first()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let right_arr = out
                    .columns()
                    .get(1)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..out.len() {
                    pairs.push((left_arr.value(i), right_arr.value(i)));
                }
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                let left_arr = out
                    .columns()
                    .first()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let right_arr = out
                    .columns()
                    .get(1)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..out.len() {
                    pairs.push((left_arr.value(i), right_arr.value(i)));
                }
            });
        }

        pairs.sort();
        assert_eq!(pairs, vec![(2, 2), (4, 4)]);
    }

    #[test]
    fn partitioned_inner_join_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            true,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            Some(residual),
            true,
            true,
            super::super::probe_step_test_support::int32_schema(&left_schema, &LEFT_KV_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(&right_schema, &RIGHT_KV_SLOT_IDS),
            super::super::probe_step_test_support::int32_schema(
                &join_scope_schema,
                &JOIN_KV_SLOT_IDS,
            ),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(
            &[1, 1, 2],
            RIGHT_K_SLOT_ID,
            &[5, 1, 10],
            RIGHT_W_SLOT_ID,
            "w",
        );
        let probe = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[3, 9], LEFT_V_SLOT_ID, "v");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_quads(out, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_quads(out, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 3, 1, 5), (2, 9, 2, 10)]);
    }

    #[test]
    fn partitioned_left_semi_join_outputs_probe_side() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Left semi: probe is left, build is right.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(
            &[1, 1, 3],
            RIGHT_K_SLOT_ID,
            &[10, 11, 30],
            RIGHT_W_SLOT_ID,
            "w",
        );
        let probe = chunk_of_two(
            &[1, 2, 3],
            LEFT_K_SLOT_ID,
            &[100, 200, 300],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 100), (3, 300)]);
    }

    #[test]
    fn partitioned_left_anti_join_outputs_nonmatching_probe_rows() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Left anti: probe is left, build is right.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 3], RIGHT_K_SLOT_ID, &[10, 30], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two(
            &[1, 2, 3],
            LEFT_K_SLOT_ID,
            &[100, 200, 300],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(2, 200)]);
    }

    #[test]
    fn partitioned_right_semi_join_outputs_probe_side() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "l", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "r", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Right semi: probe is right, build is left.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            false,
            false,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            None,
            false,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[10, 20], LEFT_V_SLOT_ID, "l");
        let probe = chunk_of_two(&[2, 3], RIGHT_K_SLOT_ID, &[200, 300], RIGHT_W_SLOT_ID, "r");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pair_columns(out, 2, 3, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pair_columns(out, 2, 3, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(2, 200)]);
    }

    #[test]
    fn partitioned_right_semi_join_outputs_join_scope_order_with_right_probe() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "l", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "r", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            false,
            false,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            None,
            false,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[10, 20], LEFT_V_SLOT_ID, "l");
        let probe = chunk_of_two(&[2, 3], RIGHT_K_SLOT_ID, &[200, 300], RIGHT_W_SLOT_ID, "r");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, probe_part.clone());
            drain_probe_work(proc, &rt, |out| {
                append_slot_quad_rows(out, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_slot_quad_rows(out, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(None, None, Some(2), Some(200))]);
    }

    #[test]
    fn partitioned_right_semi_join_with_probe_left_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Keep probe on left to match real pipeline topology.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            true,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(
            &[1, 1, 2],
            RIGHT_K_SLOT_ID,
            &[5, 20, 7],
            RIGHT_W_SLOT_ID,
            "w",
        );
        let probe = chunk_of_two(
            &[1, 1, 2],
            LEFT_K_SLOT_ID,
            &[10, 15, 100],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pair_columns(out, 2, 3, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pair_columns(out, 2, 3, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 20)]);
    }

    #[test]
    fn partitioned_left_semi_join_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            true,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 1], RIGHT_K_SLOT_ID, &[5, 20], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[10, 7], LEFT_V_SLOT_ID, "v");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pair_slots(out, LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pair_slots(out, LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 10)]);
    }

    #[test]
    fn partitioned_left_anti_join_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            true,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 1], RIGHT_K_SLOT_ID, &[5, 20], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two(
            &[1, 1, 2],
            LEFT_K_SLOT_ID,
            &[30, 7, 999],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pair_slots(out, LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pair_slots(out, LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 30), (2, 999)]);
    }

    #[test]
    fn partitioned_right_anti_join_outputs_nonmatching_probe_rows() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "l", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "r", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Right anti: probe is right, build is left.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightAnti,
            false,
            false,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::RightAnti,
            vec![probe_key],
            None,
            false,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[10, 20], LEFT_V_SLOT_ID, "l");
        let probe = chunk_of_two(&[2, 3], RIGHT_K_SLOT_ID, &[200, 300], RIGHT_W_SLOT_ID, "r");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pair_slots(out, RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pair_slots(out, RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, &mut rows);
            });
        }

        rows.sort();
        assert_eq!(rows, vec![(3, 300)]);
    }

    #[test]
    fn partitioned_left_semi_join_skips_null_keys() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", true);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            false,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1], RIGHT_K_SLOT_ID, &[10], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two_nullable(
            &[Some(1), None, Some(2)],
            LEFT_K_SLOT_ID,
            &[Some(100), Some(999), Some(200)],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
        }

        // Only k=1 matches; NULL key should never match.
        rows.sort();
        assert_eq!(rows, vec![(1, 100)]);
    }

    #[test]
    fn partitioned_left_semi_join_treats_null_residual_as_false() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", true);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", true);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            true,
            true,
            true,
            vec![build_key],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &LEFT_KV_SLOT_IDS),
            chunk_schema_of(&right_schema, &RIGHT_KV_SLOT_IDS),
            chunk_schema_of(&join_scope_schema, &JOIN_KV_SLOT_IDS),
            Arc::clone(&join_state),
        );

        let build = chunk_of_two_nullable(
            &[Some(1)],
            RIGHT_K_SLOT_ID,
            &[Some(5)],
            RIGHT_W_SLOT_ID,
            "w",
        );
        // v=NULL makes residual evaluate to NULL => treated as false.
        let probe = chunk_of_two_nullable(
            &[Some(1), Some(1)],
            LEFT_K_SLOT_ID,
            &[None, Some(3)],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for (part, build_part) in build_parts.iter().enumerate() {
            let mut op = build_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let sink = op.as_mut();
            let c = build_part.clone();
            if !c.is_empty() {
                push_build_input(sink, &rt, c);
            }
            finish_build(sink, &rt);
        }

        let mut rows = Vec::new();
        for (part, probe_part) in probe_parts.iter().enumerate() {
            let mut op = probe_factory.create(2, part as i32);
            super::super::probe_step_test_support::install(op.as_mut(), &rt);
            let c = probe_part.clone();
            let proc = op.as_mut();

            assert!(matches!(proc.initial_step(), NextStep::NeedInput));
            accept_probe_input(proc, &rt, c);
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
            proc.finish_for_test(&rt).expect("probe finish");
            drain_probe_work(proc, &rt, |out| {
                append_pairs(out, &mut rows);
            });
        }

        // Only the v=3 row satisfies v<w (3<5). NULL residual row is filtered out.
        rows.sort();
        assert_eq!(rows, vec![(1, 3)]);
    }
}
