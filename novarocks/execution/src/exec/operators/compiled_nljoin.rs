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

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::BooleanArray;
use novarocks_local_program::{
    LocalProgram, NestedLoopJoinType, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::operators::NlJoinSharedState;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use crate::exec::operators::hashjoin::compiled_hash_join::{
    PreparedJoinSchemas, prepare_join_schemas, true_mask, validate_join_scope,
};
use crate::exec::operators::hashjoin::{
    JoinGatherError, TypedJoinBatch, filter_probe_typed, gather_join_batch_typed,
    gather_left_with_null_right_typed,
};
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    shape::{Transform, TransformKernel},
    step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// The frozen facts of one compiled NestedLoopJoin node.
pub(crate) struct CompiledNlJoinPlan {
    node: ProgramNodeId,
    join_type: NestedLoopJoinType,
    predicate: Option<ProgramExpressionRootSite>,
    build: novarocks_local_program::StaticLayout,
    scope: novarocks_local_program::StaticLayout,
    output: novarocks_local_program::StaticLayout,
}

impl CompiledNlJoinPlan {
    pub(crate) fn try_new(program: &LocalProgram, node: ProgramNodeId) -> ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled nested-loop join node {} is absent", node.index()),
            )
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
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled node {} is not a nested-loop join", node.index()),
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
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled {other:?} nested-loop join at local node {} is not executable yet",
                        node.index()
                    ),
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
            build: right_layout.clone(),
            scope: join_scope_layout.clone(),
            output: graph_node.output_layout().clone(),
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
        Box::new(Transform::hydrating(CompiledNlJoinProbe {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            plan: Arc::clone(&self.plan),
            dep: self.state.build_dep(),
            state: Arc::clone(&self.state),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            construction: None,
            schemas: None,
            prepare_failure: None,
            instance: None,
            build: None,
            cursor: None,
            batch_size: None,
        }))
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
    construction: Option<Arc<dyn InputConstructionFactory>>,
    schemas: Option<PreparedJoinSchemas>,
    prepare_failure: Option<ExecutionFailure>,
    instance: Option<Vec<CompiledExpressionInstance>>,
    build: Option<Arc<Vec<Chunk>>>,
    cursor: Option<ProbeCursor>,
    batch_size: Option<NonZeroUsize>,
}

impl OperatorLifecycle for CompiledNlJoinProbe {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        if self.schemas.is_some() {
            return Ok(());
        }
        let result = (|| {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            prepare_join_schemas(
                factory.as_ref(),
                &self.plan.build,
                &self.plan.scope,
                &self.plan.output,
            )
        })();
        match result {
            Ok(schemas) => {
                self.schemas = Some(schemas);
                Ok(())
            }
            Err(first) => {
                // Only complete schema preparation is published. Earlier
                // temporary schemas have already dropped on this refusal.
                self.prepare_failure = Some(first.clone());
                Err(first)
            }
        }
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        self.batch_size = Some(NonZeroUsize::new(state.chunk_size()).ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "compiled nested-loop join requires a positive output batch size",
                ),
            )
        })?);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.release_owned();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.release_owned();
    }
    fn on_driver_failure(&mut self) {
        self.release_owned();
    }
}

impl InputEncodingPolicy for CompiledNlJoinProbe {}
impl TransformKernel for CompiledNlJoinProbe {
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let observable = self.dep.observable();
        let observed_generation = observable.generation();
        if self.build.is_some() || self.state.has_build() {
            return Ok(None);
        }
        Ok(Some(AdmissionNext::Wait(WaitFact {
            reason: WaitReason::Dependency,
            observable,
            observed_generation,
            deadline: None,
        })))
    }
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let result = (|| {
            self.prepared_schemas()?;
            self.load_build()?;
            if chunk.is_empty() {
                return Ok(StepResult::new(None, NextStep::NeedInput, rows));
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
            Ok(StepResult::new(None, NextStep::Runnable, rows))
        })();
        self.complete_step(ctx, result)
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if self.cursor.is_none() {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let result = (|| {
            let block = self
                .batch_size
                .ok_or(ConstructionFailure::MissingOwner)?
                .get();
            let output = match self.advance(block)? {
                Some(batch) if batch.num_rows() > 0 => {
                    let owner = self
                        .control
                        .construction_owner(&self.prepared_schemas()?.output)?;
                    Some(
                        batch
                            .into_chunk(owner.as_ref())
                            .map_err(JoinGatherError::into_execution)?,
                    )
                }
                _ => None,
            };
            let next = if self.cursor.is_some() {
                NextStep::Runnable
            } else {
                NextStep::NeedInput
            };
            Ok(StepResult::new(output, next, 0))
        })();
        self.complete_step(ctx, result)
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let result = self
            .prepared_schemas()
            .map(|_| StepResult::new(None, NextStep::Finished, 0));
        let step = self.complete_step(ctx, result)?;
        self.release_owned();
        Ok(step)
    }
}

impl CompiledNlJoinProbe {
    fn release_owned(&mut self) {
        self.cursor = None;
        self.build = None;
        self.instance = None;
    }
    fn complete_step(
        &mut self,
        ctx: &StepContext<'_>,
        result: ExecutionResult<StepResult>,
    ) -> ExecutionResult<StepResult> {
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
            self.release_owned();
        }
        result
    }

    fn prepared_schemas(&self) -> ExecutionResult<&PreparedJoinSchemas> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        self.schemas
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }

    fn load_build(&mut self) -> ExecutionResult<()> {
        if self.build.is_none() {
            let artifact = self
                .state
                .get_build()
                .ok_or(NestedLoopFailure::BuildNotReady)?;
            self.build = Some(Arc::clone(&artifact.build_batches));
        }
        Ok(())
    }

    /// One step of the current probe chunk: one block of pairs, or the
    /// chunk's tail once every build batch is consumed.
    fn advance(&mut self, block: usize) -> ExecutionResult<Option<TypedJoinBatch>> {
        let build = Arc::clone(
            self.build
                .as_ref()
                .ok_or(NestedLoopFailure::BuildNotLoaded)?,
        );
        let plan = Arc::clone(&self.plan);
        let scope = Arc::clone(&self.prepared_schemas()?.scope);
        let output = Arc::clone(&self.prepared_schemas()?.output);
        let cursor = self
            .cursor
            .as_mut()
            .ok_or(NestedLoopFailure::MissingProbe)?;
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
                probe_indices.push(input_row_id(cursor.probe_row, JoinInputSide::Probe)?);
                build_indices.push(input_row_id(cursor.build_row, JoinInputSide::Build)?);
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
        let owner = self.control.construction_owner(&scope)?;
        let scratch = self.control.construction_owner(&scope)?;
        let Some(pairs) = gather_join_batch_typed(
            &cursor.chunk,
            right,
            &probe_indices,
            &build_indices,
            &scope,
            owner.as_ref(),
            scratch.as_ref(),
            &self.control,
        )
        .map_err(JoinGatherError::into_execution)?
        else {
            return Ok(None);
        };
        let kept = match plan.predicate {
            Some(site) => {
                let instance = self
                    .instance
                    .as_mut()
                    .and_then(|roots| roots.first_mut())
                    .ok_or(NestedLoopFailure::MissingPredicate)?;
                let mask = true_mask(&evaluate_all(
                    instance,
                    site,
                    pairs.record_batch(),
                    &self.control,
                )?)?;
                for (ordinal, keep) in mask.iter().enumerate() {
                    if keep == Some(true) {
                        cursor.matched[probe_indices[ordinal] as usize] = true;
                    }
                }
                pairs
                    .filter(&mask, owner.as_ref())
                    .map_err(JoinGatherError::into_execution)?
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
        let output_owner = self.control.construction_owner(&output)?;
        kept.republish(output, output_owner.as_ref())
            .map(Some)
            .map_err(JoinGatherError::into_execution)
    }

    /// What a finished probe chunk publishes beyond its matched pairs.
    fn tail(&self, cursor: ProbeCursor) -> ExecutionResult<Option<TypedJoinBatch>> {
        let keep = match self.plan.join_type {
            NestedLoopJoinType::Cross | NestedLoopJoinType::Inner => return Ok(None),
            NestedLoopJoinType::LeftOuter => {
                let unmatched = cursor
                    .matched
                    .iter()
                    .enumerate()
                    .filter(|(_, matched)| !**matched)
                    .map(|(row, _)| input_row_id(row, JoinInputSide::Probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let schemas = self.prepared_schemas()?;
                let owner = self.control.construction_owner(&schemas.output)?;
                let scratch = self.control.construction_owner(&schemas.output)?;
                return gather_left_with_null_right_typed(
                    &cursor.chunk,
                    &unmatched,
                    &schemas.build,
                    &schemas.output,
                    owner.as_ref(),
                    scratch.as_ref(),
                    &self.control,
                )
                .map_err(JoinGatherError::into_execution);
            }
            NestedLoopJoinType::LeftSemi => cursor.matched,
            NestedLoopJoinType::LeftAnti | NestedLoopJoinType::NullAwareLeftAnti => {
                cursor.matched.into_iter().map(|matched| !matched).collect()
            }
            other => {
                return Err(NestedLoopFailure::UnsupportedJoin(other).into());
            }
        };
        if !keep.iter().any(|keep| *keep) {
            return Ok(None);
        }
        let schema = self.prepared_schemas()?.output.clone();
        let owner = self.control.construction_owner(&schema)?;
        filter_probe_typed(
            &cursor.chunk,
            &BooleanArray::from(keep),
            schema,
            owner.as_ref(),
        )
        .map(Some)
        .map_err(JoinGatherError::into_execution)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinInputSide {
    Probe,
    Build,
}
#[derive(Debug)]
enum NestedLoopFailure {
    BuildNotReady,
    BuildNotLoaded,
    MissingProbe,
    MissingPredicate,
    UnsupportedJoin(NestedLoopJoinType),
    RowIdExtent { side: JoinInputSide, row: usize },
}
fn input_row_id(row: usize, side: JoinInputSide) -> Result<u32, NestedLoopFailure> {
    u32::try_from(row).map_err(|_| NestedLoopFailure::RowIdExtent { side, row })
}
impl std::fmt::Display for NestedLoopFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BuildNotReady => f.write_str("compiled nested-loop join build is not ready"),
            Self::BuildNotLoaded => f.write_str("compiled nested-loop join build is not loaded"),
            Self::MissingProbe => f.write_str("compiled nested-loop join has no probe chunk"),
            Self::MissingPredicate => {
                f.write_str("compiled nested-loop join predicate instance is missing")
            }
            Self::UnsupportedJoin(kind) => write!(
                f,
                "compiled nested-loop join {kind:?} is not executable yet"
            ),
            Self::RowIdExtent { side, row } => write!(
                f,
                "join {side:?} row {row} exceeds its UInt32 row-id representation"
            ),
        }
    }
}
impl std::error::Error for NestedLoopFailure {}
impl novarocks_failure_contract::FailureProjection for NestedLoopFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let reason = match self {
            Self::BuildNotReady => ExecutionReason::NestedLoopBuildNotReady,
            Self::BuildNotLoaded => ExecutionReason::NestedLoopBuildNotLoaded,
            Self::MissingProbe => ExecutionReason::NestedLoopMissingProbe,
            Self::MissingPredicate => ExecutionReason::NestedLoopMissingPredicate,
            Self::UnsupportedJoin(_) => ExecutionReason::NestedLoopUnsupportedJoin,
            Self::RowIdExtent { row, .. } => {
                return FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::NestedLoopRowIdExtent.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::MinimumUnit,
                    // Execution layout 2: a source row ordinal in a UInt32 join selection.
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: std::num::NonZeroU16::new(2).unwrap(),
                    },
                    required: u64::try_from(*row).ok(),
                    extent_limit: None,
                    representable_prefix: None,
                });
            }
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<NestedLoopFailure> for ExecutionFailure {
    fn from(cause: NestedLoopFailure) -> Self {
        Self::from_domain(cause)
    }
}
#[cfg(test)]
mod row_id_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn row_id_boundary_refuses_one_unaddressable_pair_without_numeric_data_overflow() {
        let maximum = u32::MAX as usize;
        for side in [JoinInputSide::Probe, JoinInputSide::Build] {
            assert_eq!(input_row_id(maximum, side).unwrap(), u32::MAX);
            if let Some(unrepresentable) = maximum.checked_add(1) {
                let failure: ExecutionFailure =
                    input_row_id(unrepresentable, side).unwrap_err().into();
                let fact = failure.failure_fact();
                assert_eq!(fact.class(), FailureClass::Representation);
                let representation = fact.representation().unwrap();
                assert_eq!(representation.boundary, RepresentationBoundary::MinimumUnit);
                assert_eq!(representation.required, u64::try_from(unrepresentable).ok());
                assert_eq!(representation.extent_limit, None);
            }
        }
    }
}
