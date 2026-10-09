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

//! Compiled nested-loop join probe.
//!
//! The build side is the existing nested-loop build sink, which evaluates
//! nothing; every probe driver of the instance reads its one artifact. Each
//! probe chunk is paired with every build row in bounded blocks, probe-major
//! within each build batch. The predicate root, when present, is evaluated by
//! the driver's own compiled instance over the block's join-scope rows, so
//! every candidate pair is evaluated and its row data error is required.
//!
//! The local orientation is fixed by the compiler: the probe is the program's
//! left input. This milestone executes Cross, Inner, LeftOuter, LeftSemi,
//! LeftAnti and a predicate-free NullAwareLeftAnti, which keeps every probe
//! row exactly when the build is empty. Output batches are the node's frozen
//! canonical layout.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{
    LocalProgram, NestedLoopJoinType, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::operators::NlJoinSharedState;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use crate::exec::operators::hashjoin::compiled_hash_join::{true_mask, validate_join_scope};
use crate::exec::operators::hashjoin::join_probe_utils::{
    gather_join_batch, gather_left_with_null_right,
};
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

fn failure(context: &str, error: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from(format!("{context}: {error}"))
}

/// The frozen facts of one compiled NestedLoopJoin node.
pub(crate) struct CompiledNlJoinPlan {
    node: ProgramNodeId,
    join_type: NestedLoopJoinType,
    predicate: Option<ProgramExpressionRootSite>,
    build: ChunkSchemaRef,
    scope: ChunkSchemaRef,
    output: ChunkSchemaRef,
}

impl CompiledNlJoinPlan {
    pub(crate) fn try_new(program: &LocalProgram, node: ProgramNodeId) -> Result<Self, String> {
        let graph_node =
            program.graph().nodes().get(node.index()).ok_or_else(|| {
                format!("compiled nested-loop join node {} is absent", node.index())
            })?;
        let ProgramNodeKind::NestedLoopJoin {
            join_type,
            join_conjunct,
            left_layout,
            right_layout,
            join_scope_layout,
            ..
        } = graph_node.kind()
        else {
            return Err(format!(
                "compiled node {} is not a nested-loop join",
                node.index()
            ));
        };
        validate_join_scope(node, left_layout, right_layout, join_scope_layout)?;
        match join_type {
            NestedLoopJoinType::Cross
            | NestedLoopJoinType::Inner
            | NestedLoopJoinType::LeftOuter
            | NestedLoopJoinType::LeftSemi
            | NestedLoopJoinType::LeftAnti => {}
            NestedLoopJoinType::NullAwareLeftAnti if join_conjunct.is_none() => {}
            other => {
                return Err(format!(
                    "compiled {other:?} nested-loop join at local node {} is not executable yet",
                    node.index()
                ));
            }
        }
        Ok(Self {
            node,
            join_type: *join_type,
            predicate: join_conjunct.map(|_| ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::NestedLoopPredicate,
            }),
            build: ChunkSchema::from_compiled_layout(right_layout)?,
            scope: ChunkSchema::from_compiled_layout(join_scope_layout)?,
            output: ChunkSchema::from_compiled_layout(graph_node.output_layout())?,
        })
    }

    /// The output publishes matched pairs, not only probe rows.
    const fn emits_pairs(&self) -> bool {
        matches!(
            self.join_type,
            NestedLoopJoinType::Cross | NestedLoopJoinType::Inner | NestedLoopJoinType::LeftOuter
        )
    }
}

pub(crate) struct CompiledNlJoinProbeProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledNlJoinPlan>,
    state: Arc<NlJoinSharedState>,
    error: Arc<RuntimeErrorState>,
}

impl CompiledNlJoinProbeProcessorFactory {
    pub(crate) fn new(
        program: Arc<LocalProgram>,
        plan: Arc<CompiledNlJoinPlan>,
        state: Arc<NlJoinSharedState>,
        error: Arc<RuntimeErrorState>,
    ) -> Self {
        Self {
            name: format!("COMPILED_NL_JOIN_PROBE (node={})", plan.node.index()),
            program,
            plan,
            state,
            error,
        }
    }
}

impl OperatorFactory for CompiledNlJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledNlJoinProbe {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            plan: Arc::clone(&self.plan),
            dep: self.state.build_dep(),
            state: Arc::clone(&self.state),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instance: None,
            build: None,
            cursor: None,
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

/// One probe chunk's progress over the build batches.
struct ProbeCursor {
    chunk: Chunk,
    matched: Vec<bool>,
    batch: usize,
    probe_row: usize,
    build_row: usize,
}

struct CompiledNlJoinProbe {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledNlJoinPlan>,
    state: Arc<NlJoinSharedState>,
    dep: DependencyHandle,
    control: RuntimeKernelControl,
    instance: Option<Vec<CompiledExpressionInstance>>,
    build: Option<Arc<Vec<Chunk>>>,
    cursor: Option<ProbeCursor>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for CompiledNlJoinProbe {
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

impl ProcessorOperator for CompiledNlJoinProbe {
    fn need_input(&self) -> bool {
        !self.finishing
            && !self.finished
            && self.cursor.is_none()
            && self.pending.is_empty()
            && (self.build.is_some() || self.state.has_build())
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty() || self.cursor.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled nested-loop join probe received input after finishing".into());
        }
        if self.cursor.is_some() || !self.pending.is_empty() {
            return Err(
                "compiled nested-loop join probe received input while output is pending".into(),
            );
        }
        self.load_build()?;
        if chunk.is_empty() {
            return Ok(());
        }
        if let Some(site) = self.plan.predicate {
            instances(
                &mut self.instance,
                &self.program,
                std::slice::from_ref(&site),
                &self.control,
            )?;
        }
        self.cursor = Some(ProbeCursor {
            matched: vec![false; chunk.len()],
            chunk,
            batch: 0,
            probe_row: 0,
            build_row: 0,
        });
        Ok(())
    }
    fn pull_chunk(&mut self, state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        while self.pending.is_empty() && self.cursor.is_some() {
            if let Some(batch) = self.advance(state.chunk_size().max(1))?
                && batch.num_rows() > 0
            {
                self.pending.push_back(Chunk::try_new_with_chunk_schema(
                    batch,
                    Arc::clone(&self.plan.output),
                )?);
            }
        }
        let output = self.pending.pop_front();
        if self.finishing && self.cursor.is_none() && self.pending.is_empty() {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.cursor.is_none() && self.pending.is_empty() {
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

impl CompiledNlJoinProbe {
    fn load_build(&mut self) -> ExecutionResult<()> {
        if self.build.is_none() {
            let artifact = self
                .state
                .get_build()
                .ok_or("compiled nested-loop join build is not ready")?;
            self.build = Some(Arc::clone(&artifact.build_batches));
        }
        Ok(())
    }

    /// One step of the current probe chunk: one block of pairs, or the
    /// chunk's tail once every build batch is consumed.
    fn advance(&mut self, block: usize) -> ExecutionResult<Option<RecordBatch>> {
        let build = Arc::clone(
            self.build
                .as_ref()
                .ok_or("compiled nested-loop join build is not loaded")?,
        );
        let plan = Arc::clone(&self.plan);
        let cursor = self
            .cursor
            .as_mut()
            .ok_or("compiled nested-loop join has no probe chunk")?;
        // Without a predicate a semi or anti decision only needs whether
        // the build has a row.
        if plan.predicate.is_none() && !plan.emits_pairs() {
            let any = build.iter().any(|chunk| !chunk.is_empty());
            cursor.matched.iter_mut().for_each(|matched| *matched = any);
            cursor.batch = build.len();
        }
        while cursor.batch < build.len() && build[cursor.batch].is_empty() {
            cursor.batch += 1;
        }
        if cursor.batch >= build.len() {
            let cursor = self.cursor.take().expect("current probe chunk");
            return self.tail(cursor);
        }
        let right = &build[cursor.batch];
        let probe_rows = cursor.chunk.len();
        let mut probe_indices = Vec::with_capacity(block);
        let mut build_indices = Vec::with_capacity(block);
        while probe_indices.len() < block && cursor.probe_row < probe_rows {
            while probe_indices.len() < block && cursor.build_row < right.len() {
                probe_indices.push(
                    u32::try_from(cursor.probe_row).map_err(|_| "join probe row id overflow")?,
                );
                build_indices.push(
                    u32::try_from(cursor.build_row).map_err(|_| "join build row id overflow")?,
                );
                cursor.build_row += 1;
            }
            if cursor.build_row >= right.len() {
                cursor.build_row = 0;
                cursor.probe_row += 1;
            }
        }
        if cursor.probe_row >= probe_rows {
            cursor.probe_row = 0;
            cursor.build_row = 0;
            cursor.batch += 1;
        }
        let Some(pairs) = gather_join_batch(
            &cursor.chunk,
            right,
            &probe_indices,
            &build_indices,
            &plan.scope.arrow_schema_ref(),
        )
        .map_err(ExecutionFailure::from)?
        else {
            return Ok(None);
        };
        let kept = match plan.predicate {
            Some(site) => {
                let instance = self
                    .instance
                    .as_mut()
                    .and_then(|roots| roots.first_mut())
                    .ok_or("compiled nested-loop join predicate instance is missing")?;
                let mask = true_mask(&evaluate_all(instance, site, &pairs, &self.control)?)?;
                for (ordinal, keep) in mask.iter().enumerate() {
                    if keep == Some(true) {
                        cursor.matched[probe_indices[ordinal] as usize] = true;
                    }
                }
                filter_record_batch(&pairs, &mask)
                    .map_err(|error| failure("compiled nested-loop join predicate filter", error))?
            }
            None => {
                for &row in &probe_indices {
                    cursor.matched[row as usize] = true;
                }
                pairs
            }
        };
        if !plan.emits_pairs() || kept.num_rows() == 0 {
            return Ok(None);
        }
        RecordBatch::try_new(plan.output.arrow_schema_ref(), kept.columns().to_vec())
            .map(Some)
            .map_err(|error| failure("compiled nested-loop join output", error))
    }

    /// What a finished probe chunk publishes beyond its matched pairs.
    fn tail(&self, cursor: ProbeCursor) -> ExecutionResult<Option<RecordBatch>> {
        let keep = match self.plan.join_type {
            NestedLoopJoinType::Cross | NestedLoopJoinType::Inner => return Ok(None),
            NestedLoopJoinType::LeftOuter => {
                let unmatched = cursor
                    .matched
                    .iter()
                    .enumerate()
                    .filter(|(_, matched)| !**matched)
                    .map(|(row, _)| u32::try_from(row).map_err(|_| "join probe row id overflow"))
                    .collect::<Result<Vec<_>, _>>()?;
                return gather_left_with_null_right(
                    &cursor.chunk,
                    &unmatched,
                    &self.plan.build.arrow_schema_ref(),
                    &self.plan.output.arrow_schema_ref(),
                )
                .map_err(ExecutionFailure::from);
            }
            NestedLoopJoinType::LeftSemi => cursor.matched,
            NestedLoopJoinType::LeftAnti | NestedLoopJoinType::NullAwareLeftAnti => {
                cursor.matched.into_iter().map(|matched| !matched).collect()
            }
            other => {
                return Err(failure(
                    "compiled nested-loop join",
                    format!("{other:?} is not executable yet"),
                ));
            }
        };
        if !keep.iter().any(|keep| *keep) {
            return Ok(None);
        }
        let filtered = filter_record_batch(&cursor.chunk.batch, &BooleanArray::from(keep))
            .map_err(|error| failure("compiled nested-loop join probe filter", error))?;
        RecordBatch::try_new(
            self.plan.output.arrow_schema_ref(),
            filtered.columns().to_vec(),
        )
        .map(Some)
        .map_err(|error| failure("compiled nested-loop join output", error))
    }
}
