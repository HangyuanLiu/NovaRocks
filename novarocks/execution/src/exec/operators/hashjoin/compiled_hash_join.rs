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

//! Compiled hash join: the build sink and probe processor of one compiled
//! LocalProgram Join node.
//!
//! Keys and the residual are evaluated only through per-driver compiled root
//! instances. A key root reads its own side's batch as the operator receives
//! it; the residual reads the gathered candidate pairs in the join-scope
//! layout. The hash map, build store, gather and match-decision kernels work
//! on the evaluated arrays; nothing reads a legacy expression arena.
//!
//! The local orientation is fixed by the compiler: the probe is the program's
//! left input and the build its right input. One build driver publishes one
//! artifact that every probe driver of the instance shares: every physical
//! distribution has already delivered this instance its whole share of both
//! sides. This milestone executes Inner, LeftOuter, LeftSemi, LeftAnti and
//! NullAwareLeftAnti; a kind that must publish unmatched build rows is
//! refused when the pipeline is built.
//!
//! Output batches are exactly the node's frozen canonical layout: a
//! NULL-extended build column is already nullable there, so no schema is
//! patched at runtime. A row data error of a key or of the residual over a
//! candidate pair is a required error.
//!
//! The build driver also feeds the join's membership runtime-filter
//! producers: each evaluated build-key array is submitted once, the producers
//! close after the artifact is published, and any build failure or cancel
//! fails them. The instance's one build driver is their one local partition.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelDiagnostic, KernelFailure};
use novarocks_local_program::{
    JoinDistributionMode, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};

use super::broadcast_join_shared::BroadcastJoinSharedState;
use super::build_artifact::{BuildView, JoinBuildArtifact};
use super::build_requirements::{
    BuildComponentRequirements, NullKeyRequirement, required_build_components,
};
use super::join_hash_map::build_store::BuildStoreBuilder;
use super::join_hash_map::finalize::{finalize_probe_rows, is_all_match_one};
use super::join_hash_map::gather::{
    MAX_JOIN_OUTPUT_ROWS_PER_BATCH, gather_left_with_null_right, gather_probe_build_batches,
};
use super::join_hash_map::method::{BuildKeyBatch, JoinHashMap, JoinHashMapBuildOptions};
use super::join_hash_map::search::{JoinSelection, append_cross_selection};
use super::native_runtime_filter::{NativeRuntimeFilterProducerFactory, NativeRuntimeFilterProducerSet};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::node::join::JoinType;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime_filter::RuntimeFilterProducerFailure;

fn failure(context: &str, error: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from(format!("{context}: {error}"))
}

/// The frozen facts of one compiled Join node that both sides share.
pub(crate) struct CompiledHashJoinPlan {
    node: ProgramNodeId,
    join_type: JoinType,
    probe_sites: Vec<ProgramExpressionRootSite>,
    build_sites: Vec<ProgramExpressionRootSite>,
    residual: Option<ProgramExpressionRootSite>,
    eq_null_safe: Vec<bool>,
    build: ChunkSchemaRef,
    scope: ChunkSchemaRef,
    output: ChunkSchemaRef,
    requirements: BuildComponentRequirements,
}

impl CompiledHashJoinPlan {
    pub(crate) fn try_new(program: &LocalProgram, node: ProgramNodeId) -> Result<Self, String> {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or_else(|| format!("compiled join node {} is absent", node.index()))?;
        let ProgramNodeKind::Join {
            join_type,
            distribution_mode,
            left_layout,
            right_layout,
            join_scope_layout,
            probe_keys,
            build_keys,
            eq_null_safe,
            residual_predicate,
            ..
        } = graph_node.kind()
        else {
            return Err(format!("compiled node {} is not a hash join", node.index()));
        };
        validate_join_scope(node, left_layout, right_layout, join_scope_layout)?;
        let join_type = match join_type {
            novarocks_local_program::JoinType::Inner => JoinType::Inner,
            novarocks_local_program::JoinType::LeftOuter => JoinType::LeftOuter,
            novarocks_local_program::JoinType::LeftSemi => JoinType::LeftSemi,
            novarocks_local_program::JoinType::LeftAnti => JoinType::LeftAnti,
            novarocks_local_program::JoinType::NullAwareLeftAnti => JoinType::NullAwareLeftAnti,
            other => {
                return Err(format!(
                    "compiled {other:?} hash join at local node {} publishes build rows and is not executable yet",
                    node.index()
                ));
            }
        };
        if *distribution_mode != JoinDistributionMode::Broadcast {
            return Err(format!(
                "compiled hash join at local node {} with a local partitioned build is not executable yet",
                node.index()
            ));
        }
        if probe_keys.is_empty()
            || probe_keys.len() != build_keys.len()
            || probe_keys.len() != eq_null_safe.len()
        {
            return Err(format!(
                "compiled hash join at local node {} has no exact key pairs",
                node.index()
            ));
        }
        let site = |role| ProgramExpressionRootSite::Node { node, role };
        let ordinal = |key: usize| u32::try_from(key).map_err(|_| "join key count exceeds u32");
        let mut probe_sites = Vec::with_capacity(probe_keys.len());
        let mut build_sites = Vec::with_capacity(build_keys.len());
        for key in 0..probe_keys.len() {
            let key = ordinal(key)?;
            probe_sites.push(site(ProgramNodeExpressionRole::JoinProbeKey { key }));
            build_sites.push(site(ProgramNodeExpressionRole::JoinBuildKey { key }));
        }
        let residual = residual_predicate.map(|_| site(ProgramNodeExpressionRole::JoinResidual));
        let requirements = required_build_components(join_type, residual.is_some(), true, true);
        Ok(Self {
            node,
            join_type,
            probe_sites,
            build_sites,
            residual,
            eq_null_safe: eq_null_safe.clone(),
            build: ChunkSchema::from_compiled_layout(right_layout)?,
            scope: ChunkSchema::from_compiled_layout(join_scope_layout)?,
            output: ChunkSchema::from_compiled_layout(graph_node.output_layout())?,
            requirements,
        })
    }

    /// The null-safe equality flag of every key pair, in key order.
    pub(crate) fn eq_null_safe(&self) -> &[bool] {
        &self.eq_null_safe
    }

    /// The build-key root of key `ordinal`.
    pub(crate) fn build_site(&self, ordinal: usize) -> Option<ProgramExpressionRootSite> {
        self.build_sites.get(ordinal).copied()
    }
}

/// The single build driver: evaluates every build key root, collects the
/// rows the kind needs, publishes the instance's one build artifact and
/// feeds the join's runtime-filter producers.
pub(crate) struct CompiledHashJoinBuildSinkFactory {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    error: Arc<RuntimeErrorState>,
    producers: Option<Arc<NativeRuntimeFilterProducerFactory>>,
}

impl CompiledHashJoinBuildSinkFactory {
    pub(crate) fn new(
        program: Arc<LocalProgram>,
        plan: Arc<CompiledHashJoinPlan>,
        state: Arc<BroadcastJoinSharedState>,
        error: Arc<RuntimeErrorState>,
        producers: Option<Arc<NativeRuntimeFilterProducerFactory>>,
    ) -> Self {
        Self {
            name: format!("COMPILED_HASH_JOIN_BUILD (node={})", plan.node.index()),
            program,
            plan,
            state,
            error,
            producers,
        }
    }
}

/// One build driver's runtime-filter producers: the producer streams of its
/// local partition, or why they could not be created, reported at bind.
struct BuildRuntimeFilterProducers {
    producers: Option<NativeRuntimeFilterProducerSet>,
    create_error: Option<String>,
    local_partition_count: u32,
}

impl OperatorFactory for CompiledHashJoinBuildSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let null_key_rows =
            (self.plan.requirements.null_keys == NullKeyRequirement::NullKeyRows).then(Vec::new);
        let runtime_filters = self.producers.as_ref().map(|factory| {
            let (producers, create_error) = match factory.create_for_driver(dop, driver_id) {
                Ok(producers) => (Some(producers), None),
                Err(error) => (None, Some(error)),
            };
            BuildRuntimeFilterProducers {
                producers,
                create_error,
                local_partition_count: factory.local_partition_count(),
            }
        });
        Box::new(CompiledHashJoinBuildSink {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            plan: Arc::clone(&self.plan),
            state: Arc::clone(&self.state),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            store: BuildStoreBuilder::new(),
            input_chunks: Vec::new(),
            key_batches: Vec::new(),
            row_count: 0,
            has_null_key: false,
            null_key_rows,
            chunks_tracker: None,
            table_tracker: None,
            finished: false,
            runtime_filters,
        })
    }
    fn is_sink(&self) -> bool {
        true
    }
}

struct CompiledHashJoinBuildSink {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    store: BuildStoreBuilder,
    /// Retained only to charge the build rows to this operator's tracker
    /// until the artifact owns them.
    input_chunks: Vec<Chunk>,
    key_batches: Vec<BuildKeyBatch>,
    row_count: usize,
    has_null_key: bool,
    null_key_rows: Option<Vec<u32>>,
    chunks_tracker: Option<Arc<MemTracker>>,
    table_tracker: Option<Arc<MemTracker>>,
    finished: bool,
    runtime_filters: Option<BuildRuntimeFilterProducers>,
}

impl Operator for CompiledHashJoinBuildSink {
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        let Some(filters) = self.runtime_filters.as_mut() else {
            return Ok(());
        };
        if let Some(error) = filters.create_error.take() {
            return Err(error.into());
        }
        match filters.producers.as_mut() {
            Some(producers) => Ok(producers.bind(filters.local_partition_count)?),
            None => Ok(()),
        }
    }
    fn cancel(&mut self) {
        let _ = self.fail_runtime_filters(RuntimeFilterProducerFailure::Cancelled);
    }
    fn close(&mut self) -> ExecutionResult<()> {
        Ok(self.fail_runtime_filters(RuntimeFilterProducerFailure::ExecutionFailed)?)
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        let chunks = MemTracker::new_child("BuildInputChunks", &tracker);
        for chunk in self.input_chunks.iter_mut() {
            chunk.transfer_to(&chunks);
        }
        self.chunks_tracker = Some(chunks);
        self.table_tracker = Some(MemTracker::new_child("BuildHashTable", &tracker));
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for CompiledHashJoinBuildSink {
    fn need_input(&self) -> bool {
        !self.finished
    }
    fn has_output(&self) -> bool {
        false
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finished || chunk.is_empty() {
            return Ok(());
        }
        let result = self.push_build_chunk(chunk);
        if result.is_err() {
            let _ = self.fail_runtime_filters(RuntimeFilterProducerFailure::ExecutionFailed);
        }
        result
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        Ok(None)
    }
    fn set_finishing(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        let result = self.publish_build(state);
        if result.is_err() {
            let _ = self.fail_runtime_filters(RuntimeFilterProducerFailure::ExecutionFailed);
        }
        result
    }
}

impl CompiledHashJoinBuildSink {
    fn push_build_chunk(&mut self, mut chunk: Chunk) -> ExecutionResult<()> {
        instances(
            &mut self.instances,
            &self.program,
            &self.plan.build_sites,
            &self.control,
        )?;
        let roots = self.instances.as_mut().expect("instances were created");
        let mut keys = Vec::with_capacity(roots.len());
        for (instance, site) in roots.iter_mut().zip(&self.plan.build_sites) {
            keys.push(evaluate_all(instance, *site, &chunk.batch, &self.control)?);
        }
        let base = self.row_count;
        let rows = chunk.len();
        self.row_count = base
            .checked_add(rows)
            .ok_or("hash join build row count overflow")?;
        self.has_null_key |= keys.iter().any(|key| key.null_count() > 0);
        if let Some(null_key_rows) = self.null_key_rows.as_mut() {
            // A row whose non-null-safe key is NULL can never be an equal
            // match; a null-aware residual still sees it.
            for row in 0..rows {
                let forbidden = keys
                    .iter()
                    .zip(&self.plan.eq_null_safe)
                    .any(|(key, null_safe)| !null_safe && key.is_null(row));
                if forbidden {
                    let flat = base
                        .checked_add(row)
                        .and_then(|row| u32::try_from(row).ok())
                        .ok_or("hash join build null-key row id overflow")?;
                    null_key_rows.push(flat);
                }
            }
        }
        // The producers observe the same evaluated key arrays the table is
        // built from, after the build accepted the chunk.
        let submitted = self.runtime_filters.is_some().then(|| keys.clone());
        self.key_batches
            .push(BuildKeyBatch::new(keys, rows).map_err(ExecutionFailure::from)?);
        if self.plan.requirements.requires_row_payload() {
            self.store
                .push_chunk(&chunk)
                .map_err(ExecutionFailure::from)?;
            if let Some(tracker) = self.chunks_tracker.as_ref() {
                chunk.transfer_to(tracker);
            }
            self.input_chunks.push(chunk);
        }
        if let Some(keys) = submitted {
            self.submit_runtime_filters(&keys)?;
        }
        Ok(())
    }

    /// Builds and publishes the instance's one artifact, then closes the
    /// runtime-filter producers: the published build is what they describe.
    fn publish_build(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        let requirements = self.plan.requirements;
        let mut table = match self.key_batches.first() {
            Some(first) => {
                let key_types = first
                    .arrays()
                    .iter()
                    .map(|array| array.data_type().clone())
                    .collect::<Vec<_>>();
                Some(
                    JoinHashMap::build_from_key_batches_with_tracker(
                        key_types,
                        self.plan.eq_null_safe.clone(),
                        &self.key_batches,
                        JoinHashMapBuildOptions {
                            purpose: requirements.join_hash_map_purpose().ok_or(
                                "compiled hash join lookup purpose missing for keyed build",
                            )?,
                            ..JoinHashMapBuildOptions::default()
                        },
                        self.table_tracker.as_ref().map(Arc::clone),
                    )
                    .map_err(ExecutionFailure::from)?,
                )
            }
            None => None,
        };
        self.key_batches.clear();
        let mut store = if requirements.requires_row_payload() {
            std::mem::replace(&mut self.store, BuildStoreBuilder::new())
                .finish()
                .map_err(ExecutionFailure::from)?
        } else {
            None
        };
        self.input_chunks.clear();
        if let Some(root) = state.mem_tracker() {
            let artifact = MemTracker::new_child(
                format!("JoinBuildArtifact: {}", self.state.dep_name()),
                &root,
            );
            if let Some(table) = table.as_mut() {
                table.set_mem_tracker(MemTracker::new_child("BuildHashTable", &artifact));
            }
            if let Some(store) = store.as_mut() {
                store.transfer_to(&MemTracker::new_child("BuildStore", &artifact));
            }
        }
        let artifact = Arc::new(JoinBuildArtifact::new_native(
            requirements,
            store,
            table,
            self.row_count,
            self.has_null_key,
            self.null_key_rows.take().map(Arc::new),
        ));
        artifact
            .validate_components(requirements)
            .map_err(ExecutionFailure::from)?;
        self.state
            .set_build(artifact)
            .map_err(ExecutionFailure::from)?;
        self.finish_runtime_filters()?;
        Ok(())
    }

    fn submit_runtime_filters(&mut self, keys: &[ArrayRef]) -> Result<(), String> {
        match self
            .runtime_filters
            .as_mut()
            .and_then(|filters| filters.producers.as_mut())
        {
            Some(producers) => producers.submit(keys),
            None => Ok(()),
        }
    }

    fn finish_runtime_filters(&mut self) -> Result<(), String> {
        match self
            .runtime_filters
            .as_mut()
            .and_then(|filters| filters.producers.as_mut())
        {
            Some(producers) => producers.finish(),
            None => Ok(()),
        }
    }

    fn fail_runtime_filters(&mut self, reason: RuntimeFilterProducerFailure) -> Result<(), String> {
        match self
            .runtime_filters
            .as_mut()
            .and_then(|filters| filters.producers.as_mut())
        {
            Some(producers) => producers.fail(reason),
            None => Ok(()),
        }
    }
}

/// One probe driver: evaluates every probe key root per chunk and decides
/// matches against the shared build artifact.
pub(crate) struct CompiledHashJoinProbeProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    error: Arc<RuntimeErrorState>,
}

impl CompiledHashJoinProbeProcessorFactory {
    pub(crate) fn new(
        program: Arc<LocalProgram>,
        plan: Arc<CompiledHashJoinPlan>,
        state: Arc<BroadcastJoinSharedState>,
        error: Arc<RuntimeErrorState>,
    ) -> Self {
        Self {
            name: format!("COMPILED_HASH_JOIN_PROBE (node={})", plan.node.index()),
            program,
            plan,
            state,
            error,
        }
    }
}

impl OperatorFactory for CompiledHashJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        let mut sites = self.plan.probe_sites.clone();
        sites.extend(self.plan.residual);
        Box::new(CompiledHashJoinProbe {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            plan: Arc::clone(&self.plan),
            dep: self.state.dep(),
            state: Arc::clone(&self.state),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            sites,
            instances: None,
            build: None,
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

/// The loaded build artifact one probe driver reads.
struct LoadedBuild {
    table: Option<Arc<JoinHashMap>>,
    chunk: Option<Arc<Chunk>>,
    null_key_rows: Option<Arc<Vec<u32>>>,
    row_count: usize,
    has_null_key: bool,
}

struct CompiledHashJoinProbe {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    dep: DependencyHandle,
    control: RuntimeKernelControl,
    /// Probe key roots in key order, then the residual root.
    sites: Vec<ProgramExpressionRootSite>,
    instances: Option<Vec<CompiledExpressionInstance>>,
    build: Option<LoadedBuild>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for CompiledHashJoinProbe {
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for CompiledHashJoinProbe {
    fn need_input(&self) -> bool {
        !self.finishing
            && !self.finished
            && self.pending.is_empty()
            && (self.build.is_some() || self.state.has_build())
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled hash join probe received input after finishing".into());
        }
        if !self.pending.is_empty() {
            return Err("compiled hash join probe received input while output is pending".into());
        }
        self.load_build()?;
        if chunk.is_empty() {
            return Ok(());
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let keys = self.plan.probe_sites.len();
        let mut arrays = Vec::with_capacity(keys);
        {
            let roots = self.instances.as_mut().expect("instances were created");
            for (instance, site) in roots.iter_mut().zip(&self.sites).take(keys) {
                arrays.push(evaluate_all(instance, *site, &chunk.batch, &self.control)?);
            }
        }
        let batches = match self.plan.join_type {
            JoinType::Inner => self.join_inner(&chunk, &arrays)?,
            JoinType::LeftOuter => self.join_left_outer(&chunk, &arrays)?,
            JoinType::LeftSemi | JoinType::LeftAnti => self.join_semi_anti(&chunk, &arrays)?,
            JoinType::NullAwareLeftAnti => self.join_null_aware_anti(&chunk, &arrays)?,
            other => {
                return Err(failure(
                    "compiled hash join",
                    format!("{other:?} is not executable yet"),
                ));
            }
        };
        for batch in batches {
            if batch.num_rows() > 0 {
                self.pending.push_back(Chunk::try_new_with_chunk_schema(
                    batch,
                    Arc::clone(&self.plan.output),
                )?);
            }
        }
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.pop_front();
        if self.finishing && self.pending.is_empty() {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.pending.is_empty() {
            self.finished = true;
        }
        Ok(())
    }
    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        if self.build.is_some() || self.state.has_build() {
            None
        } else {
            Some(self.dep.clone())
        }
    }
}

impl CompiledHashJoinProbe {
    fn load_build(&mut self) -> ExecutionResult<()> {
        if self.build.is_some() {
            return Ok(());
        }
        let artifact = self
            .state
            .get_build()
            .ok_or("compiled hash join build is not ready")?;
        let row_count = artifact.build_row_count;
        let has_null_key = artifact.build_has_null_key;
        let view =
            BuildView::new(artifact, self.plan.requirements).map_err(ExecutionFailure::from)?;
        self.build = Some(LoadedBuild {
            table: view.build_table(),
            chunk: view.optional_build_chunk(),
            null_key_rows: view.build_null_key_rows(),
            row_count,
            has_null_key,
        });
        Ok(())
    }

    fn loaded(&self) -> ExecutionResult<&LoadedBuild> {
        self.build
            .as_ref()
            .ok_or_else(|| "compiled hash join build is not loaded".into())
    }

    /// The build rows the decision needs, present exactly when the build has
    /// rows and its kind retains them.
    fn build_rows(&self) -> ExecutionResult<Option<Arc<Chunk>>> {
        let build = self.loaded()?;
        if build.row_count == 0 {
            return Ok(None);
        }
        build
            .chunk
            .clone()
            .map(Some)
            .ok_or_else(|| "compiled hash join build rows are missing".into())
    }

    /// The residual root over every candidate pair of `selection`, in
    /// bounded scope batches. Returns the kept pairs and their scope rows.
    fn residual(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        selection: &JoinSelection,
    ) -> ExecutionResult<(JoinSelection, Vec<RecordBatch>)> {
        let site = self
            .plan
            .residual
            .ok_or("compiled hash join has no residual root")?;
        let mut kept = JoinSelection::new();
        let mut batches = Vec::new();
        let mut offset = 0usize;
        while offset < selection.len() {
            let end = offset
                .saturating_add(MAX_JOIN_OUTPUT_ROWS_PER_BATCH)
                .min(selection.len());
            let gathered = gather_probe_build_batches(
                probe,
                build,
                &selection.probe[offset..end],
                &selection.build[offset..end],
                &self.plan.scope.arrow_schema_ref(),
                true,
                false,
            )
            .map_err(ExecutionFailure::from)?;
            let [candidates] = <[RecordBatch; 1]>::try_from(gathered.batches)
                .map_err(|_| "compiled hash join residual candidates are not one batch")?;
            let instance = self
                .instances
                .as_mut()
                .and_then(|roots| roots.last_mut())
                .ok_or("compiled hash join residual instance is missing")?;
            let truth = evaluate_all(instance, site, &candidates, &self.control)?;
            let mask = true_mask(&truth)?;
            for (ordinal, keep) in mask.iter().enumerate() {
                if keep == Some(true) {
                    kept.push(
                        selection.probe[offset + ordinal],
                        selection.build[offset + ordinal],
                    );
                }
            }
            let filtered = filter_record_batch(&candidates, &mask)
                .map_err(|error| failure("compiled hash join residual filter", error))?;
            if filtered.num_rows() > 0 {
                batches.push(filtered);
            }
            offset = end;
        }
        Ok((kept, batches))
    }

    /// The scope rows of every pair of `selection`.
    fn gather_scope(
        &self,
        probe: &Chunk,
        build: &Chunk,
        selection: &JoinSelection,
    ) -> ExecutionResult<Vec<RecordBatch>> {
        Ok(gather_probe_build_batches(
            probe,
            build,
            &selection.probe,
            &selection.build,
            &self.plan.scope.arrow_schema_ref(),
            true,
            is_all_match_one(selection, probe.len()),
        )
        .map_err(ExecutionFailure::from)?
        .batches)
    }

    /// Scope rows republished in the canonical output layout: the same
    /// arrays under the output's own fields.
    fn output_of(&self, batch: RecordBatch) -> ExecutionResult<RecordBatch> {
        RecordBatch::try_new(
            self.plan.output.arrow_schema_ref(),
            batch.columns().to_vec(),
        )
        .map_err(|error| failure("compiled hash join output", error))
    }

    fn matched_pairs(
        &mut self,
        probe: &Chunk,
        keys: &[ArrayRef],
    ) -> ExecutionResult<Option<(JoinSelection, Vec<RecordBatch>)>> {
        let table = match self.loaded()?.table.clone() {
            Some(table) if !table.is_empty() => table,
            _ => return Ok(None),
        };
        let Some(build) = self.build_rows()? else {
            return Ok(None);
        };
        let (selection, _) = table
            .search_pairs_arrays(keys, probe.len())
            .map_err(ExecutionFailure::from)?;
        if selection.is_empty() {
            return Ok(Some((selection, Vec::new())));
        }
        if self.plan.residual.is_some() {
            return self.residual(probe, &build, &selection).map(Some);
        }
        let batches = self.gather_scope(probe, &build, &selection)?;
        Ok(Some((selection, batches)))
    }

    fn join_inner(
        &mut self,
        probe: &Chunk,
        keys: &[ArrayRef],
    ) -> ExecutionResult<Vec<RecordBatch>> {
        let Some((_, batches)) = self.matched_pairs(probe, keys)? else {
            return Ok(Vec::new());
        };
        batches
            .into_iter()
            .map(|batch| self.output_of(batch))
            .collect()
    }

    fn join_left_outer(
        &mut self,
        probe: &Chunk,
        keys: &[ArrayRef],
    ) -> ExecutionResult<Vec<RecordBatch>> {
        let (selection, matched) = self
            .matched_pairs(probe, keys)?
            .unwrap_or_else(|| (JoinSelection::new(), Vec::new()));
        let mut output = matched
            .into_iter()
            .map(|batch| self.output_of(batch))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let unmatched =
            finalize_probe_rows(probe.len(), &selection, false, "compiled left outer join")
                .map_err(ExecutionFailure::from)?
                .selected;
        if let Some(batch) = gather_left_with_null_right(
            probe,
            &unmatched,
            &self.plan.build.arrow_schema_ref(),
            &self.plan.output.arrow_schema_ref(),
        )
        .map_err(ExecutionFailure::from)?
        {
            output.push(batch);
        }
        Ok(output)
    }

    fn join_semi_anti(
        &mut self,
        probe: &Chunk,
        keys: &[ArrayRef],
    ) -> ExecutionResult<Vec<RecordBatch>> {
        let semi = self.plan.join_type == JoinType::LeftSemi;
        let table = match self.loaded()?.table.clone() {
            Some(table) if !table.is_empty() => table,
            // No build key can match: semi keeps nothing, anti everything.
            _ => return self.keep_probe(probe, &vec![!semi; probe.len()]),
        };
        let matched = if self.plan.residual.is_some() {
            let (selection, _) = self
                .matched_pairs(probe, keys)?
                .unwrap_or_else(|| (JoinSelection::new(), Vec::new()));
            finalize_probe_rows(probe.len(), &selection, true, "compiled semi/anti join")
                .map_err(ExecutionFailure::from)?
                .matched
        } else {
            table
                .search_membership_arrays(keys, probe.len())
                .map_err(ExecutionFailure::from)?
                .0
                .into_vec()
        };
        let keep = matched
            .into_iter()
            .map(|matched| matched == semi)
            .collect::<Vec<_>>();
        self.keep_probe(probe, &keep)
    }

    /// NOT IN semantics: a probe row is kept only when no build row can be
    /// equal to it. A NULL probe key may equal any build row, and a build row
    /// with a NULL key may equal any probe row; with a residual, only pairs
    /// whose residual is TRUE count.
    fn join_null_aware_anti(
        &mut self,
        probe: &Chunk,
        keys: &[ArrayRef],
    ) -> ExecutionResult<Vec<RecordBatch>> {
        let rows = probe.len();
        let (row_count, has_null_key, null_key_rows, table) = {
            let build = self.loaded()?;
            (
                build.row_count,
                build.has_null_key,
                build.null_key_rows.clone(),
                build.table.clone(),
            )
        };
        if row_count == 0 {
            return self.keep_probe(probe, &vec![true; rows]);
        }
        let probe_null = (0..rows)
            .map(|row| keys.iter().any(|key| key.is_null(row)))
            .collect::<Vec<_>>();
        let group_ids = match table.as_ref() {
            Some(table) => table
                .lookup_group_ids_arrays(keys, rows)
                .map_err(ExecutionFailure::from)?,
            None => vec![None; rows],
        };
        if self.plan.residual.is_none() {
            if has_null_key {
                return Ok(Vec::new());
            }
            let keep = group_ids
                .iter()
                .zip(&probe_null)
                .map(|(group, null)| !null && group.is_none())
                .collect::<Vec<_>>();
            return self.keep_probe(probe, &keep);
        }
        let null_key_rows = null_key_rows
            .ok_or("compiled null-aware anti join with a residual requires build null-key rows")?;
        let build = self
            .build_rows()?
            .ok_or("compiled null-aware anti join with a residual requires build rows")?;
        let mut matched_equal = vec![false; rows];
        if let Some(table) = table.as_ref() {
            let (selection, _) = table
                .search_pairs_arrays(keys, rows)
                .map_err(ExecutionFailure::from)?;
            if !selection.is_empty() {
                let (kept, _) = self.residual(probe, &build, &selection)?;
                mark(&mut matched_equal, &kept);
            }
        }
        // Every probe row against every build row whose key is NULL.
        let mut matched_null_key = vec![false; rows];
        if !null_key_rows.is_empty() {
            let probe_rows = (0..rows)
                .map(|row| u32::try_from(row).map_err(|_| "join probe row id overflow"))
                .collect::<Result<Vec<_>, _>>()?;
            self.cross_residual(
                probe,
                &build,
                &probe_rows,
                &null_key_rows,
                &mut matched_null_key,
            )?;
        }
        // Every probe row whose key is NULL against every build row.
        let mut matched_any = vec![false; rows];
        let null_probe_rows = probe_null
            .iter()
            .enumerate()
            .filter(|(_, null)| **null)
            .map(|(row, _)| u32::try_from(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "join probe row id overflow")?;
        if !null_probe_rows.is_empty() {
            let build_rows = (0..build.len())
                .map(|row| u32::try_from(row).map_err(|_| "join build row id overflow"))
                .collect::<Result<Vec<_>, _>>()?;
            self.cross_residual(
                probe,
                &build,
                &null_probe_rows,
                &build_rows,
                &mut matched_any,
            )?;
        }
        let keep = (0..rows)
            .map(|row| {
                if probe_null[row] {
                    !matched_any[row]
                } else {
                    !(matched_equal[row] || matched_null_key[row])
                }
            })
            .collect::<Vec<_>>();
        self.keep_probe(probe, &keep)
    }

    /// The residual over every pair of `probe_rows` x `build_rows`, in
    /// bounded blocks; marks the probe rows that have a TRUE pair.
    fn cross_residual(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        probe_rows: &[u32],
        build_rows: &[u32],
        matched: &mut [bool],
    ) -> ExecutionResult<()> {
        if build_rows.is_empty() {
            return Ok(());
        }
        let mut probe_position = 0usize;
        let mut build_position = 0usize;
        while probe_position < probe_rows.len() {
            let mut selection = JoinSelection::new();
            while probe_position < probe_rows.len()
                && selection.len() < MAX_JOIN_OUTPUT_ROWS_PER_BATCH
            {
                let before = selection.len();
                let stopped = append_cross_selection(
                    &mut selection,
                    &probe_rows[probe_position..=probe_position],
                    &build_rows[build_position..],
                    MAX_JOIN_OUTPUT_ROWS_PER_BATCH,
                );
                build_position += selection.len() - before;
                if build_position >= build_rows.len() {
                    probe_position += 1;
                    build_position = 0;
                }
                if stopped {
                    break;
                }
            }
            if !selection.is_empty() {
                let (kept, _) = self.residual(probe, build, &selection)?;
                mark(matched, &kept);
            }
        }
        Ok(())
    }

    /// The probe rows `keep` selects, republished in the canonical output.
    fn keep_probe(&self, probe: &Chunk, keep: &[bool]) -> ExecutionResult<Vec<RecordBatch>> {
        if !keep.iter().any(|keep| *keep) {
            return Ok(Vec::new());
        }
        let filtered = filter_record_batch(&probe.batch, &BooleanArray::from(keep.to_vec()))
            .map_err(|error| failure("compiled hash join probe filter", error))?;
        Ok(vec![self.output_of(filtered)?])
    }
}

fn mark(matched: &mut [bool], selection: &JoinSelection) {
    for &row in &selection.probe {
        if let Some(slot) = matched.get_mut(row as usize) {
            *slot = true;
        }
    }
}

/// The join scope is read positionally as the probe columns followed by the
/// build columns, so its frozen layout must be exactly the left layout then
/// the right layout: the same slots and the same complete fields.
pub(crate) fn validate_join_scope(
    node: ProgramNodeId,
    left: &novarocks_local_program::StaticLayout,
    right: &novarocks_local_program::StaticLayout,
    scope: &novarocks_local_program::StaticLayout,
) -> Result<(), String> {
    let slots = scope
        .slots()
        .iter()
        .eq(left.slots().iter().chain(right.slots()));
    let fields = scope.schema().fields().iter().eq(left
        .schema()
        .fields()
        .iter()
        .chain(right.schema().fields()));
    if slots && fields {
        Ok(())
    } else {
        Err(format!(
            "compiled join at local node {} has a scope other than its left then right layout",
            node.index()
        ))
    }
}

/// TRUE rows of a Boolean root: FALSE and NULL are both excluded.
pub(crate) fn true_mask(truth: &ArrayRef) -> ExecutionResult<BooleanArray> {
    let truth = truth
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ExecutionFailure::from(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled join predicate is not Boolean",
            )))
        })?;
    Ok(if truth.null_count() == 0 {
        truth.clone()
    } else {
        truth
            .iter()
            .map(|value| Some(value == Some(true)))
            .collect()
    })
}
