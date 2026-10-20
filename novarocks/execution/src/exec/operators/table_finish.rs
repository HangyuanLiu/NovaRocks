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

//! The `TableFinish` operator: the single-owner write completion data plane.
//!
//! One driver on one Root BE consumes every writer row that arrives through the
//! gather Exchange. It validates each row's shape against the frozen relation,
//! checks the target ordinal against the sealed set, hands the canonical bytes
//! to the validator port for a structural check, and charges both the frozen
//! per-fragment and prepared-write-set budgets. Exceeding a budget is a typed
//! rejection, never a truncation.
//!
//! Nothing is emitted before every sender reaches EOS: a prepared write set is
//! complete or it does not exist. On abort the buffered fragments are released
//! immediately rather than held until the driver is dropped.
//!
//! Design: ADR-0136 (docs/adr/ADR-0136-ordinary-aggregate-statistics-dataflow.md)

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

use super::table_finish_failure::{
    FinishArrowOperation, FinishContractSite, FinishOutputBound, finish_arrow, finish_contract,
};
fn finish_connector(source: novarocks_spi::connector::ConnectorError) -> ExecutionFailure {
    crate::runtime::fragment::connector_failure::ConnectorCallFailure::new(
        crate::runtime::fragment::connector_failure::ConnectorOperation::ValidateWriteRelation,
        source,
    )
    .into()
}

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, Int8Array, Int32Array, Int64Array, new_null_array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES;
use novarocks_spi::connector::write_stack::{
    PreparedWriteSetLedger, RootRowKind, RootWriteResultRowShape, WriteRowCountAccumulator,
    WriteTargetOrdinal, WriterRowKind, row_count_from_wire, row_count_to_wire,
    target_ordinal_from_wire, target_ordinal_to_wire, validate_artifact_draft_nested_values,
    validate_root_write_result_row, validate_writer_multiplex_row,
};

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
#[cfg(test)]
use crate::exec::node::table_finish::FinishFixture;
use crate::exec::node::table_finish::TableFinishRuntimeBinding;
use crate::exec::node::table_write_aggregate::{
    WriterFinalAggregatePlan, WriterGroupedUnpivotMapping, WriterGroupedUnpivotPlan,
};
#[cfg(test)]
use crate::exec::node::table_write_relation::WriterMultiplexRelationSchema;
use crate::exec::node::table_write_relation::{
    ConnectorCommitFragmentCarrierValidator, RootWriteResultRelationSchema,
    RootWriteResultRelationTemplate, WriterMultiplexRelationTemplate, WriterRelationPreparation,
};
#[cfg(novarocks_dev_controls)]
use crate::exec::node::table_write_relation::{
    TableWriteAggregateBoundary, TableWriteAggregateGuard,
};
use crate::exec::node::unpivot::{UnpivotPassthroughColumn, UnpivotValueMapping};
use crate::exec::operators::aggregate::AggregateProcessorFactory;
use crate::exec::operators::blocked_duration::BlockedDuration;
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use crate::exec::operators::table_writer::TableWriteRelationColumns;
#[cfg(test)]
use crate::exec::operators::unpivot_processor::UnpivotFixtureFactory;
use crate::exec::pipeline::input::{PreparedInput, RawInput};
use crate::exec::pipeline::operator::{
    FinishWatch, InputConstructionFactory, InputEncodingPolicy, Operator, forward_observable,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::shape::{ControlStep, HydrateInput, Transform, TransformKernel};
use crate::exec::pipeline::shape::{Source, SourceKernel, child::ChildStep};
use crate::exec::pipeline::step::{AdmissionNext, WaitFact, WaitReason};
use crate::exec::pipeline::step::{NextStep, StepResult};
use crate::exec::pipeline::step_context::StepContext;
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::profile::{OperatorProfiles, ProfileUnit};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

#[derive(Clone, Debug)]
enum FinishCoverageCalls {
    Original(Vec<(novarocks_types::SlotId, novarocks_types::SlotId)>),
    Prepared(novarocks_local_program::PreparedWriterAggregateCalls),
}
impl Default for FinishCoverageCalls {
    fn default() -> Self {
        Self::Original(Vec::new())
    }
}
enum FinishCoverageIter<'a> {
    Original(
        std::iter::Copied<std::slice::Iter<'a, (novarocks_types::SlotId, novarocks_types::SlotId)>>,
    ),
    Prepared(std::slice::Iter<'a, novarocks_local_program::PreparedWriterAggregateCall>),
}
impl Iterator for FinishCoverageIter<'_> {
    type Item = (novarocks_types::SlotId, novarocks_types::SlotId);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Original(calls) => calls.next(),
            Self::Prepared(calls) => calls.next().map(|call| (call.input(), call.output())),
        }
    }
}
impl FinishCoverageCalls {
    fn len(&self) -> usize {
        match self {
            Self::Original(calls) => calls.len(),
            Self::Prepared(calls) => calls.calls().len(),
        }
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn iter(&self) -> FinishCoverageIter<'_> {
        match self {
            Self::Original(calls) => FinishCoverageIter::Original(calls.iter().copied()),
            Self::Prepared(calls) => FinishCoverageIter::Prepared(calls.calls().iter()),
        }
    }
}

/// The slot facts a finish's statistics are checked against: each final
/// call's intermediate input and final output channel, in call order, and
/// each grouped Unpivot mapping's target and the final output it reads.
/// Coverage, completeness and channel mapping are judged from these alone,
/// identically for every statistics owner.
#[derive(Clone, Debug, Default)]
pub(crate) struct FinishStatisticsCoverage {
    calls: FinishCoverageCalls,
    mappings: Option<Vec<(u32, novarocks_types::SlotId)>>,
}

impl FinishStatisticsCoverage {
    pub(crate) fn new(
        calls: Vec<(novarocks_types::SlotId, novarocks_types::SlotId)>,
        mappings: Option<Vec<(u32, novarocks_types::SlotId)>>,
    ) -> Self {
        Self {
            calls: FinishCoverageCalls::Original(calls),
            mappings,
        }
    }

    #[cfg(test)]
    fn from_plan(plan: &WriterFinalAggregatePlan) -> Self {
        Self {
            calls: match &plan.prepared {
                Some(calls) => FinishCoverageCalls::Prepared(calls.clone()),
                None => FinishCoverageCalls::Original(
                    plan.calls
                        .iter()
                        .map(|call| (call.intermediate_input_slot_id, call.final_output_slot_id))
                        .collect(),
                ),
            },
            mappings: plan.unpivot.as_ref().map(|unpivot| {
                unpivot
                    .mappings
                    .iter()
                    .map(|mapping| (mapping.grouping_key, mapping.input_value_slot_id))
                    .collect()
            }),
        }
    }
}

/// An owner of a finish's statistics other than the plan-tree build from the
/// process function set and the decoder's expression arena: it supplies the
/// final aggregate over every `AGGREGATE_PARTIAL` row and the grouped Unpivot
/// of each batch that aggregate emits.
pub(crate) trait FinishStatisticsFactory: Send + Sync {
    /// The final aggregate. It takes the finish's filtered `AGGREGATE_PARTIAL`
    /// rows and emits each target group's final values once.
    fn final_aggregate(
        &self,
        state: &RuntimeState,
        factory: &dyn InputConstructionFactory,
    ) -> ExecutionResult<Box<dyn Operator>>;

    /// The grouped Unpivot of one final aggregate batch, emitting Root
    /// artifact rows.
    fn grouped_unpivot(
        &self,
        control: &RuntimeKernelControl,
        final_chunk: Chunk,
        root_schema: crate::exec::chunk::ChunkSchemaRef,
        tracker: Option<Arc<MemTracker>>,
        factory: Arc<dyn InputConstructionFactory>,
    ) -> ExecutionResult<Box<dyn GroupedUnpivotSource>>;
}

/// The Root artifact rows of one final aggregate batch, one target at a time.
pub(crate) trait GroupedUnpivotSource: Operator {
    fn visit_targets(
        &self,
        visit: &mut dyn FnMut(u32) -> ExecutionResult<()>,
    ) -> ExecutionResult<()>;
}
pub(crate) trait GroupedUnpivotKernel: SourceKernel + OperatorLifecycle {
    fn visit_targets(
        &self,
        visit: &mut dyn FnMut(u32) -> ExecutionResult<()>,
    ) -> ExecutionResult<()>;
}
impl<K: GroupedUnpivotKernel> GroupedUnpivotSource for Source<K> {
    fn visit_targets(
        &self,
        visit: &mut dyn FnMut(u32) -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        self.kernel().visit_targets(visit)
    }
}
struct GroupedOutput {
    child: ChildStep,
    next: NextStep,
    retiring: bool,
}
impl GroupedOutput {
    fn new(
        operator: Box<dyn GroupedUnpivotSource>,
        observable: Arc<Observable>,
        failures: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let mut child = ChildStep::new(operator, observable);
        child.bind_failure_port(failures)?;
        child.prepare()?;
        Ok(Self {
            child,
            next: NextStep::Runnable,
            retiring: false,
        })
    }
    fn runnable(&self) -> bool {
        match &self.next {
            NextStep::Wait(wait) => wait.is_ready(),
            NextStep::NeedInput => false,
            _ => true,
        }
    }
    fn step(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let NextStep::Wait(wait) = &self.next
            && !wait.is_ready()
        {
            return Ok(StepResult::new(None, self.next.clone(), 0));
        }
        if self.retiring {
            self.next = self.child.retire(ctx)?;
            return Ok(StepResult::new(None, self.next.clone(), 0));
        }
        let output = self.child.advance(ctx)?;
        if self.child.is_complete() {
            self.retiring = true;
            self.next = NextStep::Runnable;
        } else {
            self.next = self.child.work_next();
            if matches!(self.next, NextStep::NeedInput) {
                return Err(crate::exec::pipeline::shape::source_contract_failure(
                    crate::exec::pipeline::shape::SourceContractFailure::RequestedInput,
                ));
            }
        }
        Ok(StepResult::new(output, self.next.clone(), 0))
    }
}

/// Construction state only; type meaning remains in the frozen template.
#[derive(Clone)]
enum RootSchemaPreparation {
    #[cfg(test)]
    Fixture(RootWriteResultRelationSchema),
    Compiled {
        template: RootWriteResultRelationTemplate,
        prepared: Option<RootWriteResultRelationSchema>,
    },
}

impl RootSchemaPreparation {
    fn is_compiled(&self) -> bool {
        matches!(self, Self::Compiled { .. })
    }

    fn prepared(&self) -> Option<&RootWriteResultRelationSchema> {
        match self {
            #[cfg(test)]
            Self::Fixture(schema) => Some(schema),
            Self::Compiled { prepared, .. } => prepared.as_ref(),
        }
    }

    fn prepare(
        &mut self,
        factory: Option<&Arc<dyn InputConstructionFactory>>,
    ) -> ExecutionResult<()> {
        if let Self::Compiled { template, prepared } = self {
            if prepared.is_none() {
                let factory = factory
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
                let schema = factory.create_compiled_chunk_schema(template.layout())?;
                // Publish only after the complete template/slot check succeeds.
                *prepared = Some(template.bind_created_schema(schema)?);
            }
        }
        Ok(())
    }
}

/// Factory for the single-driver table finish operator.
pub struct TableFinishOperatorFactory {
    name: String,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    writer_schema: WriterRelationPreparation,
    root_schema: RootSchemaPreparation,
    /// The plan-tree statistics, built from the process function set and
    /// `arena` when `statistics` is absent.
    #[cfg(test)]
    final_plan: WriterFinalAggregatePlan,
    coverage: FinishStatisticsCoverage,
    statistics: Option<Arc<dyn FinishStatisticsFactory>>,
    #[cfg(novarocks_dev_controls)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
    #[cfg(test)]
    arena: Arc<ExprArena>,
}

impl TableFinishOperatorFactory {
    #[expect(
        clippy::too_many_arguments,
        reason = "A local finish joins frozen relation facts and exact Task validation capability"
    )]
    #[cfg(test)]
    pub(crate) fn new_local(
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        writer_schema: WriterMultiplexRelationTemplate,
        root_schema: RootWriteResultRelationTemplate,
        #[cfg(test)] final_plan: WriterFinalAggregatePlan,
        binding: &TableFinishRuntimeBinding,
        #[cfg(test)] arena: Arc<ExprArena>,
    ) -> ExecutionResult<Self> {
        Self::new_core(
            node_id,
            expected_targets,
            WriterRelationPreparation::compiled(writer_schema),
            RootSchemaPreparation::Compiled {
                template: root_schema,
                prepared: None,
            },
            #[cfg(test)]
            final_plan,
            binding,
            #[cfg(test)]
            arena,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The shared constructor keeps explicit legacy and compiled preparation facts"
    )]
    fn new_core(
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        writer_schema: WriterRelationPreparation,
        root_schema: RootSchemaPreparation,
        #[cfg(test)] final_plan: WriterFinalAggregatePlan,
        binding: &TableFinishRuntimeBinding,
        #[cfg(test)] arena: Arc<ExprArena>,
    ) -> ExecutionResult<Self> {
        novarocks_spi::connector::write_stack::validate_query_target_ordinals(&expected_targets)
            .map_err(finish_connector)?;
        let name = if node_id >= 0 {
            format!("TABLE_FINISH (id={node_id})")
        } else {
            "TABLE_FINISH".to_string()
        };
        #[cfg(test)]
        let coverage = FinishStatisticsCoverage::from_plan(&final_plan);
        #[cfg(not(test))]
        let coverage = FinishStatisticsCoverage::default();
        Ok(Self {
            name,
            expected_targets: Arc::new(expected_targets),
            fragment_validator: Arc::clone(&binding.fragment_validator),
            writer_schema,
            root_schema,
            coverage,
            #[cfg(test)]
            final_plan,
            statistics: None,
            #[cfg(novarocks_dev_controls)]
            aggregate_guard: Arc::clone(&binding.aggregate_guard),
            #[cfg(test)]
            arena,
        })
    }

    /// A compiled finish over the program's positional relations. Its
    /// statistics, if any, are the compiled owner's, checked against its
    /// coverage facts exactly as the plan-tree statistics are; no function
    /// set is read and the empty arena is never consulted.
    pub(crate) fn new_compiled(
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        writer_schema: WriterMultiplexRelationTemplate,
        root_template: RootWriteResultRelationTemplate,
        statistics: Option<(FinishStatisticsCoverage, Arc<dyn FinishStatisticsFactory>)>,
        binding: &TableFinishRuntimeBinding,
    ) -> ExecutionResult<Self> {
        let mut factory = Self::new_core(
            node_id,
            expected_targets,
            WriterRelationPreparation::compiled(writer_schema),
            RootSchemaPreparation::Compiled {
                template: root_template,
                prepared: None,
            },
            #[cfg(test)]
            WriterFinalAggregatePlan::default(),
            binding,
            #[cfg(test)]
            Arc::new(ExprArena::default()),
        )?;
        if let Some((coverage, statistics)) = statistics {
            factory.coverage = coverage;
            factory.statistics = Some(statistics);
        }
        Ok(factory)
    }

    /// Construct the NCP-8 composite with the same immutable expression arena
    /// that decoded the generic Unpivot constants.
    #[cfg(test)]
    fn for_test_fixture(node: &FinishFixture, arena: Arc<ExprArena>) -> Self {
        let name = if node.node_id >= 0 {
            format!("TABLE_FINISH (id={})", node.node_id)
        } else {
            "TABLE_FINISH".to_string()
        };
        Self {
            name,
            expected_targets: Arc::clone(node.expected_targets()),
            fragment_validator: Arc::clone(node.fragment_validator()),
            writer_schema: WriterRelationPreparation::Existing(
                node.writer_multiplex_schema().clone(),
            ),
            root_schema: RootSchemaPreparation::Fixture(node.root_result_schema().clone()),
            final_plan: node.final_aggregate_plan().clone(),
            coverage: FinishStatisticsCoverage::from_plan(node.final_aggregate_plan()),
            statistics: None,
            #[cfg(novarocks_dev_controls)]
            aggregate_guard: Arc::clone(node.aggregate_guard()),
            #[cfg(test)]
            arena,
        }
    }

    fn create_operator(&self, dop: i32, driver_id: i32) -> TableFinishStep {
        // The builder creates this factory's pipeline at DOP 1, converging every
        // writer input into it first, so any other degree of parallelism means
        // the plan and the pipeline disagree about who owns the complete
        // prepared write set. A second driver would each see a partial set and
        // each believe it was complete, so fail closed at `prepare` rather than
        // silently aggregating a partial set per driver.
        let parallelism_error = (dop != 1 || driver_id != 0).then(|| {
            finish_contract(FinishContractSite::Binding, format_args!(
                "table finish must run at DOP 1 on a single driver, but was created with dop={dop} driver_id={driver_id}"
            ))
        });
        Transform::hydrating(TableFinishOperator {
            name: self.name.clone(),
            expected_targets: Arc::clone(&self.expected_targets),
            fragment_validator: Arc::clone(&self.fragment_validator),
            writer_schema: self.writer_schema.clone(),
            root_schema: self.root_schema.clone(),
            #[cfg(test)]
            final_plan: self.final_plan.clone(),
            coverage: self.coverage.clone(),
            statistics: self.statistics.clone(),
            #[cfg(novarocks_dev_controls)]
            aggregate_guard: Arc::clone(&self.aggregate_guard),
            channel_to_call: self
                .writer_schema
                .slots()
                .iter()
                .skip(novarocks_spi::connector::write_stack::WRITE_RELATION_COLUMN_COUNT)
                .map(|slot| {
                    self.coverage
                        .calls
                        .iter()
                        .position(|(input, _)| input == *slot)
                })
                .collect(),
            #[cfg(test)]
            arena: Arc::clone(&self.arena),
            parallelism_error,
            rows: WriteRowCountAccumulator::new(),
            ledger: PreparedWriteSetLedger::new(),
            fragments: Vec::new(),
            aggregate: None,
            construction: None,
            aggregate_coverage: HashMap::new(),
            final_groups_seen: HashSet::new(),
            grouped_unpivot: None,
            prefix_output: None,
            phase: FinishPhase::Consuming,
            mem_tracker: None,
            fragment_tracker: None,
            output_tracker: None,
            profiles: None,
            runtime_error: None,
            grouped_control: None,
            final_aggregate_blocked_time: BlockedDuration::default(),
            source_observable: Arc::new(Observable::new()),
            input: None,
            pending_rows: 0,
        })
    }
}

impl OperatorFactory for TableFinishOperatorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(self.create_operator(dop, driver_id))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FinishPhase {
    Consuming,
    Finalizing,
    Producing,
    Failed,
    Finished,
}

struct BufferedFragment {
    target: WriteTargetOrdinal,
    bytes: Vec<u8>,
    _accounting: Option<TrackedBytes>,
}

/// Streams the non-statistics Root prefix without materializing a second copy
/// of the complete prepared write set. The summary is its own first batch;
/// prepared fragments then move through bounded, byte-aware batches.
struct PrefixOutputDriver {
    summary_rows: Option<i64>,
    fragments: VecDeque<BufferedFragment>,
    root_schema: crate::exec::chunk::ChunkSchemaRef,
    tracker: Option<Arc<MemTracker>>,
}

impl PrefixOutputDriver {
    fn try_new(
        rows: u64,
        fragments: Vec<BufferedFragment>,
        root_schema: crate::exec::chunk::ChunkSchemaRef,
        tracker: Option<Arc<MemTracker>>,
    ) -> ExecutionResult<Self> {
        let summary_rows = row_count_to_wire(rows).map_err(|error| finish_connector(error))?;
        validate_root_write_result_row(
            RootRowKind::Summary,
            RootWriteResultRowShape {
                row_count: Some(summary_rows),
                ..RootWriteResultRowShape::default()
            },
        )
        .map_err(|error| finish_connector(error))?;
        Ok(Self {
            summary_rows: Some(summary_rows),
            fragments: fragments.into(),
            root_schema,
            tracker,
        })
    }

    fn has_output(&self) -> bool {
        self.summary_rows.is_some() || !self.fragments.is_empty()
    }

    fn is_finished(&self) -> bool {
        !self.has_output()
    }

    fn pull(&mut self) -> ExecutionResult<Option<Chunk>> {
        if let Some(summary_rows) = self.summary_rows.take() {
            let root_arrow = self.root_schema.arrow_schema_ref();
            let batch = RecordBatch::try_new(
                root_arrow,
                vec![
                    Arc::new(Int8Array::from(vec![RootRowKind::Summary.to_wire()])) as ArrayRef,
                    new_null_array(self.root_schema.arrow_schema_ref().field(1).data_type(), 1),
                    Arc::new(Int64Array::from(vec![Some(summary_rows)])),
                    new_null_array(self.root_schema.arrow_schema_ref().field(3).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(4).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(5).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(6).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(7).data_type(), 1),
                ],
            )
            .map_err(|error| finish_arrow(FinishArrowOperation::Summary, error))?;
            return self.track_batch(batch).map(Some);
        }
        if self.fragments.is_empty() {
            return Ok(None);
        }

        // Select a bounded candidate before copying any Binary values. The
        // conservative fixed allowance covers validity/offset buffers and IPC
        // framing; an exact retained/logical/IPC check below is authoritative.
        const FIXED_BATCH_ALLOWANCE: usize = 16 * 1024;
        const PER_ROW_ALLOWANCE: usize = 256;
        let mut selected = 0usize;
        let mut selected_payload = 0usize;
        for fragment in &self.fragments {
            let next_payload = selected_payload
                .checked_add(fragment.bytes.len())
                .ok_or_else(|| {
                    crate::exec::pipeline::operator::ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)
                })?;
            let next_rows = selected + 1;
            let estimate = FIXED_BATCH_ALLOWANCE
                .saturating_add(next_payload)
                .saturating_add(next_rows.saturating_mul(PER_ROW_ALLOWANCE));
            if estimate > MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES {
                break;
            }
            selected = next_rows;
            selected_payload = next_payload;
        }
        if selected == 0 {
            return Err(ExecutionFailure::from_domain(FinishOutputBound {
                required: None,
                limit: MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
            }));
        }

        // The conservative estimate should normally fit on the first attempt.
        // Retrying with a smaller prefix keeps even unusual Arrow/IPC overhead
        // bounded to one candidate batch instead of copying the complete ledger.
        loop {
            let batch = self.build_fragment_batch(selected)?;
            let size = root_output_size(&batch, self.tracker.as_ref())?;
            if size <= MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES {
                let output = self.track_batch(batch)?;
                for _ in 0..selected {
                    let _released = self
                        .fragments
                        .pop_front()
                        .expect("selected fragments remain buffered until output admission");
                }
                return Ok(Some(output));
            }
            if selected == 1 {
                return Err(ExecutionFailure::from_domain(FinishOutputBound {
                    required: Some(size),
                    limit: MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
                }));
            }
            selected = selected.div_ceil(2);
        }
    }

    fn build_fragment_batch(&self, rows: usize) -> ExecutionResult<RecordBatch> {
        let mut kinds = Vec::with_capacity(rows);
        let mut ordinals = Vec::with_capacity(rows);
        let mut payloads = BinaryBuilder::new();
        for fragment in self.fragments.iter().take(rows) {
            let target = target_ordinal_to_wire(fragment.target);
            validate_root_write_result_row(
                RootRowKind::PreparedFragment,
                RootWriteResultRowShape {
                    target: Some(target),
                    fragment_len: Some(fragment.bytes.len()),
                    ..RootWriteResultRowShape::default()
                },
            )
            .map_err(|error| finish_connector(error))?;
            kinds.push(RootRowKind::PreparedFragment.to_wire());
            ordinals.push(Some(target));
            payloads.append_value(&fragment.bytes);
        }
        let root_arrow = self.root_schema.arrow_schema_ref();
        RecordBatch::try_new(
            root_arrow,
            vec![
                Arc::new(Int8Array::from(kinds)) as ArrayRef,
                Arc::new(Int32Array::from(ordinals)),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(2).data_type(),
                    rows,
                ),
                Arc::new(payloads.finish()),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(4).data_type(),
                    rows,
                ),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(5).data_type(),
                    rows,
                ),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(6).data_type(),
                    rows,
                ),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(7).data_type(),
                    rows,
                ),
            ],
        )
        .map_err(|error| finish_arrow(FinishArrowOperation::Fragment, error))
    }

    fn track_batch(&self, batch: RecordBatch) -> ExecutionResult<Chunk> {
        let mut output = Chunk::try_new_with_chunk_schema(batch, Arc::clone(&self.root_schema))?;
        if let Some(tracker) = self.tracker.as_ref() {
            output
                .try_transfer_to(tracker)
                .map_err(ExecutionFailure::from_domain)?;
        }
        Ok(output)
    }
}

fn root_output_size(
    batch: &RecordBatch,
    tracker: Option<&Arc<MemTracker>>,
) -> ExecutionResult<usize> {
    let retained = batch.get_array_memory_size();
    let logical = crate::exec::chunk::record_batch_bytes(batch);
    let mut encoded = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut encoded, &batch.schema())
            .map_err(|error| finish_arrow(FinishArrowOperation::Encoding, error))?;
        writer
            .write(batch)
            .map_err(|error| finish_arrow(FinishArrowOperation::Encoding, error))?;
        writer
            .finish()
            .map_err(|error| finish_arrow(FinishArrowOperation::Encoding, error))?;
    }
    let _encoded_accounting = tracker
        .map(|tracker| {
            TrackedBytes::try_new(encoded.capacity(), Arc::clone(tracker))
                .map_err(ExecutionFailure::from_domain)
        })
        .transpose()?;
    Ok(retained.max(logical).max(encoded.len()))
}

type TableFinishStep = Transform<TableFinishOperator, HydrateInput>;

struct TableFinishOperator {
    name: String,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    writer_schema: WriterRelationPreparation,
    root_schema: RootSchemaPreparation,
    #[cfg(test)]
    final_plan: WriterFinalAggregatePlan,
    coverage: FinishStatisticsCoverage,
    statistics: Option<Arc<dyn FinishStatisticsFactory>>,
    #[cfg(novarocks_dev_controls)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
    channel_to_call: Vec<Option<usize>>,
    #[cfg(test)]
    arena: Arc<ExprArena>,
    parallelism_error: Option<ExecutionFailure>,
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<BufferedFragment>,
    aggregate: Option<ChildStep>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    aggregate_coverage: HashMap<u32, Vec<bool>>,
    final_groups_seen: HashSet<u32>,
    grouped_unpivot: Option<GroupedOutput>,
    prefix_output: Option<PrefixOutputDriver>,
    phase: FinishPhase,
    mem_tracker: Option<Arc<MemTracker>>,
    fragment_tracker: Option<Arc<MemTracker>>,
    output_tracker: Option<Arc<MemTracker>>,
    profiles: Option<OperatorProfiles>,
    runtime_error: Option<Arc<RuntimeErrorState>>,
    grouped_control: Option<RuntimeKernelControl>,
    final_aggregate_blocked_time: BlockedDuration,
    source_observable: Arc<Observable>,
    input: Option<Chunk>,
    pending_rows: u64,
}

impl TableFinishOperator {
    fn prepared_root_schema(&self) -> ExecutionResult<&RootWriteResultRelationSchema> {
        self.root_schema.prepared().ok_or_else(|| {
            crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                "compiled TableFinish root schema was not prepared".to_string(),
            )
            .into()
        })
    }

    fn install_aggregate(&mut self, aggregate: Box<dyn Operator>) -> ExecutionResult<()> {
        let mut child = ChildStep::new(aggregate, self.source_observable.clone());
        child.bind_failure_port(self.runtime_error.as_ref().cloned().ok_or_else(|| {
            finish_contract(
                FinishContractSite::Binding,
                "table finish final child has no original runtime failure binding",
            )
        })?)?;
        child.prepare()?;
        self.aggregate = Some(child);
        Ok(())
    }
    fn begin_aggregate_exit(&mut self) -> ExecutionResult<()> {
        if let Some(aggregate) = self.aggregate.as_mut() {
            aggregate.begin_exit()?;
        }
        Ok(())
    }
    fn close_aggregate(&mut self) -> ExecutionResult<()> {
        if let Some(aggregate) = self.aggregate.as_mut() {
            aggregate.close()?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn install_grouped_unpivot(
        &mut self,
        driver: impl GroupedUnpivotKernel + 'static,
        failures: Arc<RuntimeErrorState>,
    ) {
        self.grouped_unpivot = Some(
            GroupedOutput::new(
                Box::new(Source::new(driver)),
                self.source_observable.clone(),
                failures,
            )
            .unwrap(),
        );
    }

    fn install_grouped_unpivot_source(
        &mut self,
        driver: Box<dyn GroupedUnpivotSource>,
    ) -> ExecutionResult<()> {
        let failures = self.runtime_error.as_ref().cloned().ok_or_else(|| {
            finish_contract(
                FinishContractSite::Binding,
                "table finish grouped output has no original runtime failure binding",
            )
        })?;
        self.grouped_unpivot = Some(GroupedOutput::new(
            driver,
            self.source_observable.clone(),
            failures,
        )?);
        Ok(())
    }

    fn release_buffer(&mut self) {
        self.input = None;
        self.fragments.clear();
        self.prefix_output = None;
        if let Some(grouped) = self.grouped_unpivot.as_mut() {
            grouped.child.cancel();
        }
        if self.phase != FinishPhase::Failed {
            self.phase = FinishPhase::Failed;
            if let Some(aggregate) = self.aggregate.as_mut() {
                aggregate.cancel();
            }
        }
        // Preserve both original child owners until their actual exits.
        self.aggregate_coverage.clear();
        self.final_groups_seen.clear();
    }

    fn fail<T>(&mut self, error: impl Into<ExecutionFailure>) -> ExecutionResult<T> {
        let failure = error.into();
        if let Some(signal) = &self.runtime_error {
            signal.set_error(failure.clone());
        }
        self.release_buffer();
        self.phase = FinishPhase::Failed;
        self.finish_final_aggregate_blocked_interval();
        self.sync_metrics();
        Err(failure)
    }

    /// Read one target ordinal off the signed carrier. A negative value is
    /// corrupt data, never a very large unsigned ordinal.
    fn target_in_sealed_set(&self, raw: i32) -> ExecutionResult<WriteTargetOrdinal> {
        let target = target_ordinal_from_wire(raw).map_err(|error| finish_connector(error))?;
        // An exact set test, not a bound: the query's expected set need not be
        // dense from zero, so "at or below the highest ordinal" would admit a
        // target this query compiled no writer for.
        if !self.expected_targets.contains(&target) {
            return Err(finish_contract(
                FinishContractSite::Relation,
                format_args!(
                    "table finish received a write target ordinal {raw} outside the sealed set of {} targets",
                    self.expected_targets.len()
                ),
            ));
        }
        Ok(target)
    }

    fn accept_row(
        &mut self,
        chunk: &Chunk,
        columns: &TableWriteRelationColumns<'_>,
        row: usize,
    ) -> ExecutionResult<()> {
        if columns.kinds.is_null(row) {
            return Err(finish_contract(
                FinishContractSite::Relation,
                "table finish received a writer row with a null kind",
            ));
        }
        let kind = WriterRowKind::from_wire(columns.kinds.value(row))
            .map_err(|error| finish_connector(error))?;
        let row_count = (!columns.row_counts.is_null(row)).then(|| columns.row_counts.value(row));
        let fragment_len =
            (!columns.fragments.is_null(row)).then(|| columns.fragments.value(row).len());
        let auxiliary_non_null = self
            .writer_schema
            .schema()?
            .contract()
            .auxiliary_channels()
            .iter()
            .map(|channel| {
                let slot = novarocks_types::SlotId::new(channel.slot_id());
                let index = chunk
                    .chunk_schema()
                    .index_of(slot)
                    .expect("exact writer schema contains every auxiliary slot");
                !chunk.columns()[index].is_null(row)
            })
            .collect::<Vec<_>>();
        validate_writer_multiplex_row(
            self.writer_schema.schema()?.contract(),
            kind,
            row_count,
            fragment_len,
            &auxiliary_non_null,
        )
        .map_err(|error| finish_connector(error))?;

        if columns.ordinals.is_null(row) {
            return Err(finish_contract(
                FinishContractSite::Relation,
                "table finish received a writer row with a null write target ordinal",
            ));
        }
        let target = self.target_in_sealed_set(columns.ordinals.value(row))?;

        match kind {
            WriterRowKind::RowCount => {
                let rows = row_count.ok_or_else(|| {
                    finish_contract(
                        FinishContractSite::Relation,
                        "table finish ROW_COUNT row lost its validated row count",
                    )
                })?;
                // The SPI count owner rejects a negative trusted protocol count
                // before it can become the statement's affected row count.
                let rows = row_count_from_wire(rows).map_err(|error| finish_connector(error))?;
                self.rows.add(rows).map_err(|error| finish_connector(error))
            }
            WriterRowKind::CommitFragment => {
                let encoded = columns.fragments.value(row);
                self.fragment_validator
                    .validate(target, encoded)
                    .map_err(|error| finish_connector(error))?;
                self.ledger
                    .reserve_fragment(encoded.len())
                    .map_err(|error| finish_connector(error))?;
                let bytes = encoded.to_vec();
                let accounting = self
                    .fragment_tracker
                    .as_ref()
                    .map(|tracker| {
                        TrackedBytes::try_new(bytes.capacity(), Arc::clone(tracker))
                            .map_err(ExecutionFailure::from_domain)
                    })
                    .transpose()?;
                self.fragments.push(BufferedFragment {
                    target,
                    bytes,
                    _accounting: accounting,
                });
                Ok(())
            }
            WriterRowKind::AggregatePartial => {
                if self.coverage.calls.is_empty() {
                    return Err(finish_contract(
                        FinishContractSite::Coverage,
                        "table finish received aggregate partials without a final aggregate plan",
                    ));
                }
                let coverage = self
                    .aggregate_coverage
                    .entry(target.get())
                    .or_insert_with(|| vec![false; self.coverage.calls.len()]);
                for (channel, non_null) in auxiliary_non_null.into_iter().enumerate() {
                    if non_null {
                        let index = self.channel_to_call[channel].ok_or_else(|| {
                            finish_contract(
                                FinishContractSite::Coverage,
                                "table finish writer auxiliary channel has no final aggregate",
                            )
                        })?;
                        coverage[index] = true;
                    }
                }
                Ok(())
            }
        }
    }

    fn validate_coverage(&self) -> ExecutionResult<()> {
        let Some(mappings) = self.coverage.mappings.as_ref() else {
            return if self.coverage.calls.is_empty() {
                Ok(())
            } else {
                Err(finish_contract(
                    FinishContractSite::Coverage,
                    "table finish final aggregates are missing grouped Unpivot",
                ))
            };
        };
        let call_by_output = self
            .coverage
            .calls
            .iter()
            .enumerate()
            .map(|(index, (_, output))| (output, index))
            .collect::<HashMap<_, _>>();
        for (grouping_key, input_value_slot_id) in mappings {
            let call = call_by_output
                .get(input_value_slot_id)
                .copied()
                .ok_or_else(|| {
                    finish_contract(
                        FinishContractSite::Coverage,
                        "table finish grouped Unpivot references an unknown final output",
                    )
                })?;
            if !self
                .aggregate_coverage
                .get(grouping_key)
                .and_then(|coverage| coverage.get(call))
                .copied()
                .unwrap_or(false)
            {
                return Err(finish_contract(
                    FinishContractSite::Coverage,
                    format_args!(
                        "table finish target {} has no non-null aggregate partial for channel {}",
                        grouping_key, call
                    ),
                ));
            }
        }
        Ok(())
    }

    fn filter_aggregate_rows(
        &self,
        chunk: &Chunk,
        partial_rows: &[bool],
    ) -> ExecutionResult<Chunk> {
        let mask = BooleanArray::from(partial_rows.to_vec());
        let batch = filter_record_batch(&chunk.record_batch(), &mask)
            .map_err(|error| finish_arrow(FinishArrowOperation::Filter, error))?;
        Ok(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(self.writer_schema.schema()?.chunk_schema()),
        )?)
    }

    fn begin_finalize(&mut self) -> ExecutionResult<()> {
        self.validate_coverage()?;
        self.prefix_output = Some(PrefixOutputDriver::try_new(
            self.rows.get(),
            std::mem::take(&mut self.fragments),
            Arc::clone(self.prepared_root_schema()?.chunk_schema()),
            self.output_tracker.as_ref().map(Arc::clone),
        )?);
        let Some(aggregate) = self.aggregate.as_mut() else {
            self.phase = FinishPhase::Producing;
            return Ok(());
        };
        aggregate.finish_input();
        // The child may now be asynchronously finalizing while already
        // producing zero or more ordinary aggregate batches. TableFinish
        // streams those batches instead of imposing a scalar/single-batch ABI.
        self.phase = FinishPhase::Producing;
        Ok(())
    }

    fn start_grouped_unpivot(&mut self, final_chunk: Chunk) -> ExecutionResult<()> {
        let driver: Box<dyn GroupedUnpivotSource> = match self.statistics.as_ref() {
            Some(statistics) => {
                statistics.grouped_unpivot(
                    self.grouped_control.as_ref().ok_or(
                        crate::exec::pipeline::operator::ConstructionFailure::MissingOwner,
                    )?,
                    final_chunk,
                    Arc::clone(self.prepared_root_schema()?.chunk_schema()),
                    self.output_tracker.as_ref().map(Arc::clone),
                    Arc::clone(self.construction.as_ref().ok_or(
                        crate::exec::pipeline::operator::ConstructionFailure::MissingOwner,
                    )?),
                )?
            }
            #[cfg(test)]
            None if self.final_plan.prepared.is_some() => {
                let factory =
                    Arc::clone(self.construction.as_ref().ok_or(
                        crate::exec::pipeline::operator::ConstructionFailure::MissingOwner,
                    )?);
                let plan = self.final_plan.unpivot.as_ref().ok_or_else(|| {
                    crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                        "writer grouped Unpivot declaration is absent".into(),
                    )
                })?;
                super::compiled_writer_statistics::create_prepared_grouped_in(
                    &self.arena,
                    plan,
                    final_chunk,
                    Arc::clone(self.prepared_root_schema()?.chunk_schema()),
                    self.output_tracker.as_ref().map(Arc::clone),
                    factory,
                    self.grouped_control.as_ref().ok_or(
                        crate::exec::pipeline::operator::ConstructionFailure::MissingOwner,
                    )?,
                )?
            }
            #[cfg(test)]
            None => {
                let arena = Arc::clone(&self.arena);
                let plan = self.final_plan.unpivot.as_ref().cloned().ok_or_else(|| {
                    finish_contract(
                        FinishContractSite::FinalGroups,
                        "table finish final aggregate is missing grouped Unpivot",
                    )
                })?;
                Box::new(Source::new(GroupedUnpivotFixtureKernel::try_new(
                    arena,
                    plan,
                    Arc::clone(self.prepared_root_schema()?.chunk_schema()),
                    final_chunk,
                    self.output_tracker.as_ref().map(Arc::clone),
                    self.grouped_control
                        .as_ref()
                        .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?
                        .for_receiver(),
                    self.construction.clone(),
                )?))
            }
            #[cfg(not(test))]
            None => {
                return Err(
                    crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into(),
                );
            }
        };
        driver.visit_targets(&mut |target| {
            if !self.final_groups_seen.insert(target) {
                return Err(finish_contract(FinishContractSite::FinalGroups, format_args!("table finish final aggregate produced duplicate target {target} across output batches")));
            }
            Ok(())
        })?;
        self.install_grouped_unpivot_source(driver)
    }

    fn validate_final_groups_complete(&self) -> ExecutionResult<()> {
        let expected = self
            .coverage
            .mappings
            .iter()
            .flatten()
            .map(|(grouping_key, _)| *grouping_key)
            .collect::<HashSet<_>>();
        if expected == self.final_groups_seen {
            return Ok(());
        }
        let missing = expected
            .difference(&self.final_groups_seen)
            .copied()
            .collect::<Vec<_>>();
        let unexpected = self
            .final_groups_seen
            .difference(&expected)
            .copied()
            .collect::<Vec<_>>();
        Err(finish_contract(
            FinishContractSite::FinalGroups,
            format_args!(
                "table finish final aggregate target coverage mismatch: missing={missing:?}, unexpected={unexpected:?}"
            ),
        ))
    }

    fn runtime_error(&self) -> Option<ExecutionFailure> {
        self.runtime_error.as_ref().and_then(|state| state.error())
    }

    fn record_output(&self, output: &Chunk) {
        if let Some(profiles) = self.profiles.as_ref() {
            profiles
                .common
                .counter_add_unit("RootOutputRows", output.len() as i64);
            profiles
                .common
                .counter_add_bytes("RootOutputBytes", output.logical_bytes() as i64);
        }
    }

    fn finish_final_aggregate_blocked_interval(&self) {
        self.final_aggregate_blocked_time.observe(false);
    }

    fn sync_metrics(&self) {
        let Some(profiles) = self.profiles.as_ref() else {
            return;
        };
        profiles.common.counter_set_unit(
            "FinalAggregateBlockedCount",
            i64::try_from(self.final_aggregate_blocked_time.intervals()).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set(
            "FinalAggregateBlockedTime",
            ProfileUnit::TimeNs,
            crate::runtime::profile::clamp_u128_to_i64(
                self.final_aggregate_blocked_time.elapsed_ns(),
            ),
        );
    }
}

#[cfg(test)]
fn build_final_aggregate(
    plan: &WriterFinalAggregatePlan,
    arena: &Arc<ExprArena>,
    function_set: Arc<crate::exec::expr::agg::SealedExecutionFunctionSet>,
    node_id: i32,
) -> ExecutionResult<Box<dyn Operator>> {
    let unpivot = plan.unpivot.as_ref().ok_or_else(|| {
        finish_contract(
            FinishContractSite::Relation,
            "table finish final aggregates require grouped Unpivot",
        )
    })?;
    let mut aggregate_arena = arena.as_ref().clone();
    let grouping = aggregate_arena.push_typed(
        ExprNode::SlotId(unpivot.grouping_input_slot_id),
        DataType::Int32,
    );
    let mut functions = Vec::with_capacity(plan.calls.len());
    let mut resolved = Vec::with_capacity(plan.calls.len());
    let mut fields = vec![Field::new("write_target_ordinal", DataType::Int32, false)];
    let mut slot_ids = vec![unpivot.grouping_output_slot_id];
    for (index, call) in plan.calls.iter().enumerate() {
        let input = aggregate_arena.push_typed(
            ExprNode::SlotId(call.intermediate_input_slot_id),
            call.resolved.intermediate_type.clone(),
        );
        functions.push(AggFunction {
            frozen: call.frozen.clone(),
            name: call.function_name.to_string(),
            inputs: vec![input],
            input_is_intermediate: true,
            types: Some(AggTypeSignature {
                intermediate_type: Some(call.resolved.intermediate_type.clone()),
                output_type: Some(call.resolved.output_type.clone()),
                input_arg_type: call.resolved.argument_types.first().cloned(),
            }),
            order: Default::default(),
        });
        fields.push(Field::new(
            format!("final_aggregate_{index}"),
            call.resolved.output_type.clone(),
            true,
        ));
        slot_ids.push(call.final_output_slot_id);
        resolved.push(call.resolved.clone());
    }
    let output_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(&Schema::new(fields), &slot_ids)?;
    let factory = AggregateProcessorFactory::new_native(
        node_id,
        Arc::new(aggregate_arena),
        vec![grouping],
        functions,
        function_set,
        resolved,
        false,
        false,
        output_schema,
        Vec::new(),
        None,
        1,
        None,
    )?;
    Ok(factory.create(1, 0))
}

#[cfg(test)]
struct GroupedUnpivotFixtureKernel {
    #[cfg(test)]
    arena: Arc<ExprArena>,
    plan: WriterGroupedUnpivotPlan,
    root_schema: crate::exec::chunk::ChunkSchemaRef,
    final_chunk: Chunk,
    rows: Vec<(u32, usize)>,
    next_row: usize,
    mappings: BTreeMap<u32, Vec<WriterGroupedUnpivotMapping>>,
    active: Option<ChildStep>,
    active_exit_started: bool,
    tracker: Option<Arc<MemTracker>>,
    source_observable: Arc<Observable>,
    control: RuntimeKernelControl,
    construction: Option<Arc<dyn InputConstructionFactory>>,
}

#[cfg(test)]
impl GroupedUnpivotFixtureKernel {
    fn try_new(
        arena: Arc<ExprArena>,
        plan: WriterGroupedUnpivotPlan,
        root_schema: crate::exec::chunk::ChunkSchemaRef,
        mut final_chunk: Chunk,
        tracker: Option<Arc<MemTracker>>,
        control: RuntimeKernelControl,
        construction: Option<Arc<dyn InputConstructionFactory>>,
    ) -> ExecutionResult<Self> {
        if let Some(tracker) = tracker.as_ref() {
            final_chunk
                .try_transfer_to(tracker)
                .map_err(ExecutionFailure::from_domain)?;
        }
        let groups = final_chunk
            .column_by_slot_id(plan.grouping_output_slot_id)?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                finish_contract(
                    FinishContractSite::Relation,
                    "table finish final aggregate grouping output is not Int32",
                )
            })?
            .clone();
        let mut rows = Vec::with_capacity(groups.len());
        let mut seen = std::collections::HashSet::with_capacity(groups.len());
        for row in 0..groups.len() {
            if groups.is_null(row) {
                return Err(finish_contract(
                    FinishContractSite::Relation,
                    "table finish final aggregate produced a null grouping key",
                ));
            }
            let raw = groups.value(row);
            let target = target_ordinal_from_wire(raw).map_err(|error| finish_connector(error))?;
            if !seen.insert(target.get()) {
                return Err(finish_contract(
                    FinishContractSite::Relation,
                    format_args!(
                        "table finish final aggregate produced duplicate target {}",
                        target.get()
                    ),
                ));
            }
            rows.push((target.get(), row));
        }
        let mut mappings = BTreeMap::<u32, Vec<_>>::new();
        for mapping in &plan.mappings {
            mappings
                .entry(mapping.grouping_key)
                .or_default()
                .push(mapping.clone());
        }
        for (target, _) in &rows {
            if !mappings.contains_key(target) {
                return Err(finish_contract(
                    FinishContractSite::Relation,
                    format_args!(
                        "table finish final aggregate produced target {target} with no grouped Unpivot mapping"
                    ),
                ));
            }
        }
        Ok(Self {
            #[cfg(test)]
            arena,
            plan,
            root_schema,
            final_chunk,
            rows,
            next_row: 0,
            mappings,
            active: None,
            active_exit_started: false,
            tracker,
            source_observable: Arc::new(Observable::new()),
            control,
            construction,
        })
    }

    fn install_active(&mut self, active: Box<dyn Operator>) -> ExecutionResult<()> {
        let mut child = ChildStep::new(active, self.source_observable.clone());
        child.bind_failure_port(self.control.failure_port())?;
        child.prepare()?;
        self.active = Some(child);
        self.active_exit_started = false;
        Ok(())
    }

    fn output_schema(&self, target: u32) -> ExecutionResult<crate::exec::chunk::ChunkSchemaRef> {
        let slots = [
            self.plan.passthrough_output_slot_id,
            self.plan.literal_output_slot_ids[0],
            self.plan.literal_output_slot_ids[1],
            self.plan.value_output_slot_id,
            self.plan.literal_output_slot_ids[2],
        ];
        let mut fields = slots
            .iter()
            .map(|slot| {
                self.root_schema.slot(*slot).cloned().ok_or_else(|| {
                    finish_contract(
                        FinishContractSite::FinalGroups,
                        format_args!("table finish Root schema is missing slot {slot}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        fields[0] = fields[0].with_nullable(false);
        let mappings = self.mappings.get(&target).ok_or_else(|| {
            finish_contract(
                FinishContractSite::FinalGroups,
                format_args!("table finish target {target} has no grouped mappings"),
            )
        })?;
        let literal_positions = [1usize, 2, 4];
        for (literal_index, position) in literal_positions
            .iter()
            .copied()
            .enumerate()
            .take(self.plan.literal_output_slot_ids.len())
        {
            let nullable = mappings.iter().any(|mapping| {
                matches!(
                    mapping.constants[literal_index],
                    crate::exec::node::unpivot::UnpivotConstant::Scalar { nullable: true, .. }
                )
            });
            fields[position] = fields[position].with_nullable(nullable);
        }
        let value_nullable = mappings.iter().try_fold(false, |nullable, mapping| {
            self.final_chunk
                .chunk_schema()
                .slot(mapping.input_value_slot_id)
                .map(|slot| nullable || slot.nullable())
                .ok_or_else(|| {
                    finish_contract(
                        FinishContractSite::FinalGroups,
                        format_args!(
                            "table finish final aggregate is missing value slot {}",
                            mapping.input_value_slot_id
                        ),
                    )
                })
        })?;
        fields[3] = fields[3].with_nullable(value_nullable);
        ChunkSchema::try_new(fields)
            .map(Arc::new)
            .map_err(ExecutionFailure::from)
    }

    fn start_next(
        &mut self,
        ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<bool> {
        let Some((target, row)) = self.rows.get(self.next_row).copied() else {
            return Ok(false);
        };
        let mappings = self
            .mappings
            .get(&target)
            .ok_or_else(|| {
                finish_contract(
                    FinishContractSite::Relation,
                    format_args!("table finish target {target} lost its grouped mappings"),
                )
            })?
            .iter()
            .map(|mapping| UnpivotValueMapping {
                input_value_slot_id: mapping.input_value_slot_id,
                constants: mapping.constants.clone(),
            })
            .collect();
        let factory = UnpivotFixtureFactory::new(
            -1,
            Arc::clone(&self.arena),
            vec![UnpivotPassthroughColumn {
                input_slot_id: self.plan.grouping_output_slot_id,
                output_slot_id: self.plan.passthrough_output_slot_id,
            }],
            self.plan.value_output_slot_id,
            self.plan.literal_output_slot_ids.clone(),
            mappings,
            self.output_schema(target)?,
            self.plan.max_output_rows,
            self.plan.max_output_bytes,
        )?;
        let mut active = factory.create_bound(self.control.for_receiver());
        if let Some(factory) = self.construction.as_ref() {
            active.set_construction_factory(factory.clone());
        }
        if let Some(tracker) = self.tracker.as_ref() {
            active.set_mem_tracker(Arc::clone(tracker));
        }
        self.install_active(active)?;
        let input = if self.final_chunk.chunk_schema().is_typed() {
            let factory = self
                .construction
                .as_ref()
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            let owner = factory.begin_input_construction(&self.final_chunk)?;
            self.final_chunk
                .slice_typed_observed::<crate::exec::pipeline::operator::ConstructionFailure, _>(
                    row,
                    1,
                    owner.storage_lifetime(),
                    |step| owner.observe(0, &[], step),
                )?
        } else {
            self.final_chunk.slice(row, 1)
        };
        let child = self.active.as_mut().expect("installed grouped Unpivot");
        child.offer(input)?;
        child.finish_input();
        Ok(true)
    }

    fn step(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if self.active.as_ref().is_some_and(ChildStep::is_complete) {
            self.active_exit_started = true;
        }
        if self.active_exit_started {
            let next = self
                .active
                .as_mut()
                .expect("retiring grouped child")
                .retire(ctx)?;
            if !matches!(next, NextStep::Finished) {
                return Ok(StepResult::new(None, next, 0));
            }
            self.active = None;
            self.active_exit_started = false;
            self.next_row += 1;
            return Ok(StepResult::new(
                None,
                if self.next_row == self.rows.len() {
                    NextStep::Finished
                } else {
                    NextStep::Runnable
                },
                0,
            ));
        }
        if self.active.is_none() && self.next_row == self.rows.len() {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        if self.active.is_none() {
            self.start_next(ctx)?;
            return Ok(StepResult::new(None, NextStep::Runnable, 0));
        }
        let active = self.active.as_mut().expect("started grouped Unpivot");
        let output = active.advance(ctx)?;
        let next = if active.is_complete() {
            self.active_exit_started = true;
            NextStep::Runnable
        } else {
            active.work_next()
        };
        let output = match output {
            Some(chunk) => root_artifact_chunk(chunk, &self.root_schema, self.tracker.as_ref())?,
            None => None,
        };
        Ok(StepResult::new(output, next, 0))
    }
}
#[cfg(test)]
impl GroupedUnpivotKernel for GroupedUnpivotFixtureKernel {
    fn visit_targets(
        &self,
        visit: &mut dyn FnMut(u32) -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        for (target, _) in &self.rows {
            visit(*target)?;
        }
        Ok(())
    }
}
#[cfg(test)]
impl SourceKernel for GroupedUnpivotFixtureKernel {
    fn poll_output(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        self.step(ctx)
    }
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.source_observable.clone())
    }
}
#[cfg(test)]
impl OperatorLifecycle for GroupedUnpivotFixtureKernel {
    fn name(&self) -> &str {
        "GROUPED_WRITER_UNPIVOT"
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        if let Some(active) = self.active.as_mut() {
            active.begin_exit()?;
        }
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        if let Some(active) = self.active.as_mut() {
            active.close()?;
        }
        Ok(())
    }
    fn cancel(&mut self) {
        if let Some(active) = self.active.as_mut() {
            active.cancel();
        }
    }
    fn visit_exits(
        &mut self,
        visit: &mut dyn FnMut(crate::exec::pipeline::operator::ExitObservation),
    ) {
        if let Some(active) = self.active.as_mut() {
            active.visit_exits(visit);
        }
    }
}

/// Validate one grouped Unpivot artifact batch and lift it onto the Root
/// relation: every row is an `ARTIFACT_DRAFT` of its target, with the
/// relation's own carriers.
enum RootArtifactStep {
    Work,
    IdScratch(usize),
    PairScratch(usize),
    Columns,
    KindWrapper,
    ListWrapper,
    MapWrapper,
}

pub(crate) fn root_artifact_chunk(
    artifact: Chunk,
    root_schema: &crate::exec::chunk::ChunkSchemaRef,
    tracker: Option<&Arc<MemTracker>>,
) -> ExecutionResult<Option<Chunk>> {
    // The explicit legacy adapter shares the exact validation/column body.
    root_artifact_chunk_core(
        artifact,
        root_schema,
        |_| Ok(()),
        |ordinal, rows| {
            Ok(new_null_array(
                root_schema.arrow_schema_ref().field(ordinal).data_type(),
                rows,
            ))
        },
        |_, columns, rows| {
            let batch = RecordBatch::try_new(root_schema.arrow_schema_ref(), columns)
                .map_err(|error| finish_arrow(FinishArrowOperation::Artifact, error))?;
            let mut output = Chunk::try_new_with_chunk_schema(batch, Arc::clone(root_schema))?;
            if let Some(tracker) = tracker {
                output
                    .try_transfer_to(tracker)
                    .map_err(ExecutionFailure::from_domain)?;
            }
            assert_eq!(output.len(), rows);
            Ok(Some(output))
        },
    )
}

pub(crate) fn root_artifact_chunk_with_construction(
    artifact: Chunk,
    root_schema: &crate::exec::chunk::ChunkSchemaRef,
    tracker: Option<&Arc<MemTracker>>,
    factory: &dyn InputConstructionFactory,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> ExecutionResult<Option<Chunk>> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_functions::KernelFailure;
    use std::alloc::Layout;
    if artifact.is_empty() {
        return Ok(None);
    }
    if !artifact.chunk_schema().is_typed() || !root_schema.is_typed() {
        return Err(ConstructionFailure::MissingOwner.into());
    }
    // The protocol explicitly widens NULL promises on these existing value
    // roles. It does not infer a domain from the source Arrow carrier.
    for ordinal in [1, 4, 5, 6, 7] {
        let expected = root_schema.slots().get(ordinal).ok_or_else(|| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "Root artifact declaration is absent",
            )
        })?;
        let source = artifact
            .chunk_schema()
            .slot(expected.slot_id())
            .ok_or_else(|| {
                finish_contract(
                    FinishContractSite::RootArtifact,
                    "artifact source declaration is absent",
                )
            })?;
        let source = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
            source.typed_binding().clone(),
        )
        .map_err(ExecutionFailure::from_domain)?;
        let expected = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
            expected.typed_binding().clone(),
        )
        .map_err(ExecutionFailure::from_domain)?;
        if !source.fits_physical_binding_observed(&expected, || control.checkpoint(1))? {
            return Err(finish_contract(
                FinishContractSite::RootArtifact,
                "artifact source differs from its complete Root declaration",
            ));
        }
    }
    let owner = factory.begin_typed_construction(root_schema)?;
    let temporary = factory.begin_typed_construction(root_schema)?;
    root_artifact_chunk_core(
        artifact,
        root_schema,
        |step| {
            let arc = |payload| {
                novarocks_type_contract::owned_resources::layout::arc_layout(payload)
                    .map_err(ConstructionFailure::Resource)
            };
            match step {
                RootArtifactStep::Work => temporary.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                        units: 1,
                    },
                )?,
                RootArtifactStep::IdScratch(count) => temporary.request(
                    Layout::array::<Option<i32>>(count)
                        .map_err(|_| KernelFailure::ResourceExhausted)?,
                )?,
                RootArtifactStep::PairScratch(count) => temporary.request(
                    Layout::array::<(Option<&str>, Option<&str>)>(count)
                        .map_err(|_| KernelFailure::ResourceExhausted)?,
                )?,
                RootArtifactStep::Columns => owner.request(
                    Layout::array::<ArrayRef>(8).map_err(|_| KernelFailure::ResourceExhausted)?,
                )?,
                RootArtifactStep::KindWrapper => {
                    super::compiled_writer_statistics::grouped_primitive_adoption(
                        owner.as_ref(),
                        control,
                    )?;
                    owner.request(arc(Layout::new::<Int8Array>())?)?;
                }
                RootArtifactStep::ListWrapper => {
                    owner.request(arc(Layout::new::<arrow::array::ListArray>())?)?
                }
                RootArtifactStep::MapWrapper => {
                    owner.request(
                        Layout::array::<ArrayRef>(2)
                            .map_err(|_| KernelFailure::ResourceExhausted)?,
                    )?;
                    owner.request(arc(Layout::new::<arrow::array::MapArray>())?)?;
                }
            }
            Ok(())
        },
        |ordinal, rows| {
            super::compiled_writer_statistics::grouped_null_array(
                root_schema.slots()[ordinal].typed_binding().layout(),
                rows,
                owner.as_ref(),
                control,
            )
        },
        |artifact, columns, rows| {
            let storage = artifact.source_storage_lifetime_observed::<ConstructionFailure>(
                owner.storage_lifetime(),
                |layout| owner.request(layout),
            )?;
            let mut output =
                Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
                    Arc::clone(root_schema),
                    columns,
                    rows,
                    storage,
                    |step| owner.observe(0, &[], step),
                )?;
            if let Some(tracker) = tracker {
                output
                    .try_transfer_to(tracker)
                    .map_err(ConstructionFailure::Memory)?;
            }
            Ok(Some(output))
        },
    )
}

fn root_artifact_chunk_core(
    artifact: Chunk,
    root_schema: &crate::exec::chunk::ChunkSchemaRef,
    mut before: impl FnMut(RootArtifactStep) -> ExecutionResult<()>,
    mut null_array: impl FnMut(usize, usize) -> ExecutionResult<ArrayRef>,
    construct: impl FnOnce(Chunk, Vec<ArrayRef>, usize) -> ExecutionResult<Option<Chunk>>,
) -> ExecutionResult<Option<Chunk>> {
    let rows = artifact.len();
    if rows == 0 {
        return Ok(None);
    }
    let slot = |index: usize| root_schema.slot_ids()[index];
    let target = artifact.column_by_slot_id(slot(1))?;
    let input_fields = artifact.column_by_slot_id(slot(4))?;
    let blob_type = artifact.column_by_slot_id(slot(5))?;
    let body = artifact.column_by_slot_id(slot(6))?;
    let properties = artifact.column_by_slot_id(slot(7))?;
    let input_lists = input_fields
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact input_fields is not List<Int32>",
            )
        })?;
    let blob_types = blob_type
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact blob_type is not Utf8",
            )
        })?;
    let bodies = body
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .ok_or_else(|| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact body is not Binary",
            )
        })?;
    let maps = properties
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact properties is not Map<Utf8,Utf8>",
            )
        })?;
    let targets = target
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact target is not Int32",
            )
        })?;
    for row in 0..rows {
        before(RootArtifactStep::Work)?;
        let ids = input_lists
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                finish_contract(
                    FinishContractSite::RootArtifact,
                    "table finish artifact input_fields values are not Int32",
                )
            })?;
        let begin = usize::try_from(input_lists.value_offsets()[row]).map_err(|_| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact input_fields offset is negative",
            )
        })?;
        let end = usize::try_from(input_lists.value_offsets()[row + 1]).map_err(|_| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact input_fields offset is negative",
            )
        })?;
        before(RootArtifactStep::IdScratch(end - begin))?;
        let ids = (begin..end)
            .map(|index| (!ids.is_null(index)).then(|| ids.value(index)))
            .collect::<Vec<_>>();
        let entries = maps.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                finish_contract(
                    FinishContractSite::RootArtifact,
                    "table finish artifact property keys are not Utf8",
                )
            })?;
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                finish_contract(
                    FinishContractSite::RootArtifact,
                    "table finish artifact property values are not Utf8",
                )
            })?;
        let begin = usize::try_from(maps.value_offsets()[row]).map_err(|_| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact properties offset is negative",
            )
        })?;
        let end = usize::try_from(maps.value_offsets()[row + 1]).map_err(|_| {
            finish_contract(
                FinishContractSite::RootArtifact,
                "table finish artifact properties offset is negative",
            )
        })?;
        before(RootArtifactStep::PairScratch(end - begin))?;
        let pairs = (begin..end)
            .map(|index| {
                (
                    (!keys.is_null(index)).then(|| keys.value(index)),
                    (!values.is_null(index)).then(|| values.value(index)),
                )
            })
            .collect::<Vec<_>>();
        validate_artifact_draft_nested_values(&ids, &pairs)
            .map_err(|error| finish_connector(error))?;
        validate_root_write_result_row(
            RootRowKind::ArtifactDraft,
            RootWriteResultRowShape {
                target: (!targets.is_null(row)).then(|| targets.value(row)),
                input_fields_len: (!input_lists.is_null(row)).then_some(ids.len()),
                blob_type_len: (!blob_types.is_null(row)).then(|| blob_types.value(row).len()),
                body_len: (!bodies.is_null(row)).then(|| bodies.value(row).len()),
                properties_len: (!maps.is_null(row)).then_some(pairs.len()),
                ..RootWriteResultRowShape::default()
            },
        )
        .map_err(|error| finish_connector(error))?;
    }
    let root_arrow = root_schema.arrow_schema_ref();
    // The reused upstream nested arrays name the upstream layout's child
    // fields. Rebind them to the Root layout's own fields (buffers are shared,
    // nothing is copied) so the Root output carries only metadata owners its
    // own layout provides; a bounded root producer refuses any other owner.
    before(RootArtifactStep::ListWrapper)?;
    let input_fields = rebind_list(input_lists, root_arrow.field(4).data_type())?;
    before(RootArtifactStep::MapWrapper)?;
    let properties = rebind_map(maps, root_arrow.field(7).data_type())?;
    before(RootArtifactStep::Columns)?;
    before(RootArtifactStep::KindWrapper)?;
    let null_two = null_array(2, rows)?;
    let null_three = null_array(3, rows)?;
    let columns = vec![
        Arc::new(Int8Array::from(vec![
            RootRowKind::ArtifactDraft.to_wire();
            rows
        ])) as ArrayRef,
        target,
        null_two,
        null_three,
        input_fields,
        blob_type,
        body,
        properties,
    ];
    construct(artifact, columns, rows)
}

fn rebind_list(list: &arrow::array::ListArray, root: &DataType) -> ExecutionResult<ArrayRef> {
    let DataType::List(item) = root else {
        return Err(finish_contract(
            FinishContractSite::RootArtifact,
            "table finish Root input_fields is not a List",
        ));
    };
    arrow::array::ListArray::try_new(
        Arc::clone(item),
        list.offsets().clone(),
        Arc::clone(list.values()),
        list.nulls().cloned(),
    )
    .map(|list| Arc::new(list) as ArrayRef)
    .map_err(|error| finish_arrow(FinishArrowOperation::List, error))
}

fn rebind_map(map: &arrow::array::MapArray, root: &DataType) -> ExecutionResult<ArrayRef> {
    let DataType::Map(entries, sorted) = root else {
        return Err(finish_contract(
            FinishContractSite::RootArtifact,
            "table finish Root properties is not a Map",
        ));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(finish_contract(
            FinishContractSite::RootArtifact,
            "table finish Root property entries are not a Struct",
        ));
    };
    let rebound = arrow::array::StructArray::try_new(
        fields.clone(),
        map.entries().columns().to_vec(),
        map.entries().nulls().cloned(),
    )
    .map_err(|error| finish_arrow(FinishArrowOperation::Map, error))?;
    arrow::array::MapArray::try_new(
        Arc::clone(entries),
        map.offsets().clone(),
        rebound,
        map.nulls().cloned(),
        *sorted,
    )
    .map(|map| Arc::new(map) as ArrayRef)
    .map_err(|error| finish_arrow(FinishArrowOperation::Map, error))
}

impl OperatorLifecycle for TableFinishOperator {
    fn creates_typed_storage(&self) -> bool {
        self.writer_schema.is_compiled()
            || self.root_schema.is_compiled()
            || self.statistics.is_some()
    }

    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        if let Some(aggregate) = self.aggregate.as_mut() {
            aggregate.set_construction_factory(Arc::clone(&factory));
        }
        self.construction = Some(factory);
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(error) = self.parallelism_error.take() {
            self.phase = FinishPhase::Failed;
            return Err(error.into());
        }
        if self.coverage.calls.is_empty() != self.coverage.mappings.is_none() {
            self.phase = FinishPhase::Failed;
            return Err(finish_contract(
                FinishContractSite::Coverage,
                "table finish final aggregate and grouped Unpivot must be both empty or both present",
            ));
        }
        if self.channel_to_call.iter().any(Option::is_none)
            || self.channel_to_call.len() != self.coverage.calls.len()
        {
            self.phase = FinishPhase::Failed;
            return Err(finish_contract(
                FinishContractSite::Coverage,
                "table finish final aggregates do not exactly cover the frozen writer tail",
            ));
        }
        if let Err(error) = self.writer_schema.prepare(self.construction.as_ref()) {
            return self.fail(error);
        }
        if let Err(error) = self.root_schema.prepare(self.construction.as_ref()) {
            return self.fail(error);
        }
        Ok(())
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.mem_tracker = Some(Arc::clone(&tracker));
        self.fragment_tracker = Some(MemTracker::new_child("TableFinishFragments", &tracker));
        self.output_tracker = Some(MemTracker::new_child("TableFinishOutput", &tracker));
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.profiles = Some(profiles);
        self.sync_metrics();
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.runtime_error = Some(state.error_state());
        let mut control = RuntimeKernelControl::new(state.error_state());
        control.bind_runtime_memory(state);
        if let Some(factory) = self.construction.as_ref() {
            control.bind_construction_factory(factory.clone());
        }
        if let Some(tracker) = self.mem_tracker.as_ref() {
            control.bind_mem_tracker(tracker.clone());
        }
        self.grouped_control = Some(control);
        if self.coverage.calls.is_empty() {
            return Ok(());
        }
        let mut aggregate = match self.statistics.as_ref() {
            Some(statistics) => statistics.final_aggregate(
                state,
                self.construction
                    .as_ref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?
                    .as_ref(),
            )?,
            #[cfg(test)]
            None if self.final_plan.prepared.is_some() => {
                let factory = self
                    .construction
                    .as_ref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
                let input = Arc::clone(self.writer_schema.schema()?.chunk_schema());
                let owner = factory.begin_typed_construction(&input)?;
                let prepared = self
                    .final_plan
                    .prepared
                    .as_ref()
                    .expect("checked prepared source");
                if prepared
                    .calls()
                    .iter()
                    .map(|call| (call.input(), call.output()))
                    .ne(self.coverage.calls.iter())
                {
                    return Err(
                        crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                            "writer final prepared occurrences differ from the original coverage"
                                .into(),
                        )
                        .into(),
                    );
                }
                let unpivot = self.final_plan.unpivot.as_ref().ok_or_else(|| {
                    crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                        "writer final grouping declaration is absent".into(),
                    )
                })?;
                super::compiled_writer_statistics::create_prepared_final_in(
                    prepared.clone(),
                    unpivot.grouping_input_slot_id,
                    unpivot.grouping_output_slot_id,
                    input,
                    state.error_state(),
                    owner.as_ref(),
                )?
            }
            #[cfg(test)]
            None if self.writer_schema.is_compiled() => {
                return Err(
                    crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into(),
                );
            }
            #[cfg(test)]
            None => {
                // Explicit legacy expansion fixture route; production prepared
                // relations never resolve a function name here.
                let arena = Arc::clone(&self.arena);
                let function_set = state
                    .execution_runtime()
                    .map(|runtime| Arc::clone(runtime.function_set()))
                    .ok_or_else(|| {
                        finish_contract(
                            FinishContractSite::Binding,
                            "table finish aggregate plan requires the execution function set",
                        )
                    })?;
                build_final_aggregate(&self.final_plan, &arena, function_set, -1)?
            }
            #[cfg(not(test))]
            None => {
                return Err(
                    crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into(),
                );
            }
        };
        if let Some(tracker) = self.mem_tracker.as_ref() {
            aggregate.set_mem_tracker(MemTracker::new_child("TableFinishFinalAggregate", tracker));
        }
        if let Some(profiles) = self.profiles.as_ref() {
            aggregate.set_profiles(OperatorProfiles::new(
                profiles.operator.child("TableFinishFinalAggregate"),
            ));
        }
        if let Some(factory) = self.construction.as_ref() {
            // Expression input construction uses this same caller-owned scope
            // even when the child retains a legacy output declaration.
            aggregate.set_construction_factory(Arc::clone(factory));
        } else if aggregate.creates_typed_storage() {
            return Err(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into());
        }
        aggregate.bind_runtime_state(state)?;
        self.install_aggregate(aggregate)?;
        Ok(())
    }

    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if let Some(aggregate) = self.aggregate.as_mut() {
            aggregate.activate(state)?;
        }
        Ok(())
    }

    fn cancel(&mut self) {
        self.release_buffer();
        self.phase = FinishPhase::Failed;
        self.finish_final_aggregate_blocked_interval();
        self.sync_metrics();
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        let aggregate = self.begin_aggregate_exit();
        let grouped = self
            .grouped_unpivot
            .as_mut()
            .map(|grouped| grouped.child.begin_exit())
            .unwrap_or(Ok(()));
        // Ordinary child cleanup failures were consumed by each exact binding;
        // even a binding invariant cannot prevent the sibling hook attempt.
        aggregate.and(grouped)
    }
    fn visit_exits(
        &mut self,
        visit: &mut dyn FnMut(crate::exec::pipeline::operator::ExitObservation),
    ) {
        if let Some(aggregate) = self.aggregate.as_mut() {
            aggregate.visit_exits(visit);
        }
        if let Some(grouped) = self.grouped_unpivot.as_mut() {
            grouped.child.visit_exits(visit);
        }
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.finish_final_aggregate_blocked_interval();
        self.sync_metrics();
        let aggregate = self.close_aggregate();
        let grouped = self
            .grouped_unpivot
            .as_mut()
            .map(|grouped| grouped.child.close())
            .unwrap_or(Ok(()));
        aggregate.and(grouped)
    }

    fn on_driver_failure(&mut self) {
        self.cancel();
    }
}

#[derive(Debug)]
enum TableFinishStepFailure {
    FinalInputClosed,
    EarlyFinalOutput,
}
impl std::fmt::Display for TableFinishStepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::FinalInputClosed => "table finish final aggregate closed before writer input EOS",
            Self::EarlyFinalOutput => {
                "table finish final aggregate produced output before writer input EOS"
            }
        })
    }
}
impl std::error::Error for TableFinishStepFailure {}
impl novarocks_failure_contract::FailureProjection for TableFinishStepFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            (match self {
                Self::FinalInputClosed => ExecutionReason::TableFinishFinalInputClosed,
                Self::EarlyFinalOutput => ExecutionReason::TableFinishEarlyFinalOutput,
            })
            .reason(),
        )
    }
}
impl InputEncodingPolicy for TableFinishOperator {}
impl TransformKernel for TableFinishOperator {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.source_observable.clone())
    }
    fn poll_control(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        if let Some(error) = self.runtime_error() {
            return Err(error);
        }
        if let Some(aggregate) = self.aggregate.as_mut() {
            let control = aggregate.poll_control(ctx)?;
            if aggregate.is_complete() && self.phase == FinishPhase::Consuming {
                return Err(ExecutionFailure::from_domain(
                    TableFinishStepFailure::FinalInputClosed,
                ));
            }
            if (matches!(control, ControlStep::Runnable) || aggregate.runnable())
                && (self.phase == FinishPhase::Consuming
                    || self.prefix_output.is_none() && self.grouped_unpivot.is_none())
            {
                return Ok(ControlStep::Runnable);
            }
        }
        Ok(ControlStep::Active)
    }
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let generation = self.source_observable.generation();
        if let Some(error) = self.runtime_error() {
            return Err(error);
        }
        if self.aggregate.as_ref().is_some_and(ChildStep::is_complete) {
            return Err(ExecutionFailure::from_domain(
                TableFinishStepFailure::FinalInputClosed,
            ));
        }
        let ready = self.phase == FinishPhase::Consuming
            && self.input.is_none()
            && self.aggregate.as_ref().is_none_or(ChildStep::can_accept);
        self.final_aggregate_blocked_time.observe(!ready);
        Ok((!ready).then(|| {
            AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Output,
                observable: self.source_observable.clone(),
                observed_generation: generation,
                deadline: self
                    .aggregate
                    .as_ref()
                    .and_then(ChildStep::wait)
                    .and_then(|wait| wait.deadline),
            })
        }))
    }
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        self.input = Some(input.into_uncovered_chunk()?);
        self.drive(ctx)
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let generation = self.source_observable.generation();
        let result = self.advance(ctx);
        match result {
            Ok(mut step) => {
                if let NextStep::Wait(wait) = &step.next {
                    step.next =
                        if wait.is_ready() || self.source_observable.generation() != generation {
                            NextStep::Runnable
                        } else {
                            NextStep::Wait(WaitFact {
                                reason: wait.reason,
                                observable: self.source_observable.clone(),
                                observed_generation: generation,
                                deadline: wait.deadline,
                            })
                        };
                }
                self.final_aggregate_blocked_time
                    .observe(matches!(&step.next,NextStep::Wait(wait) if !wait.is_ready()));
                Ok(step)
            }
            Err(error) => {
                let error = ctx.locate(error);
                ctx.report_failure(error.clone());
                self.fail(error)
            }
        }
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        self.phase = FinishPhase::Finalizing;
        self.finish_final_aggregate_blocked_interval();
        self.drive(ctx)
    }
}
impl TableFinishOperator {
    fn consume_relation(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        if chunk.chunk_schema() != self.writer_schema.schema()?.chunk_schema().as_ref() {
            return Err(finish_contract(
                FinishContractSite::Relation,
                "table finish writer relation schema drifted from the frozen plan",
            ));
        }
        // The chunk is exactly the frozen relation (checked above), so its
        // prefix is read at the relation's positions, whatever its slots.
        let columns = match TableWriteRelationColumns::try_from_relation(&chunk) {
            Ok(columns) => columns,
            Err(error) => return Err(error.into()),
        };
        let mut partial_rows = vec![false; chunk.len()];
        for (row, is_partial) in partial_rows.iter_mut().enumerate() {
            let raw_kind = if columns.kinds.is_null(row) {
                None
            } else {
                Some(columns.kinds.value(row))
            };
            if let Err(error) = self.accept_row(&chunk, &columns, row) {
                return Err(error.into());
            }
            *is_partial = raw_kind == Some(WriterRowKind::AGGREGATE_PARTIAL);
        }
        if partial_rows.iter().any(|value| *value) {
            let filtered = self.filter_aggregate_rows(&chunk, &partial_rows)?;
            #[cfg(novarocks_dev_controls)]
            if let Err(error) = self
                .aggregate_guard
                .check(TableWriteAggregateBoundary::FinalMerge)
            {
                return Err(ExecutionFailure::from_domain(error));
            }
            let Some(aggregate) = self.aggregate.as_mut() else {
                return Err(finish_contract(
                    FinishContractSite::Binding,
                    "table finish aggregate partials arrived before aggregate binding",
                ));
            };
            aggregate.offer(filtered)?;
        }
        if let Some(profiles) = self.profiles.as_ref() {
            profiles
                .common
                .counter_add_unit("WriterMultiplexRows", chunk.len() as i64);
            profiles
                .common
                .counter_add_bytes("WriterMultiplexBytes", chunk.logical_bytes() as i64);
        }
        Ok(())
    }

    fn result(&mut self, output: Option<Chunk>, next: NextStep) -> StepResult {
        if let Some(output) = output.as_ref() {
            self.record_output(output);
        }
        let rows = if matches!(next, NextStep::NeedInput) {
            std::mem::take(&mut self.pending_rows)
        } else {
            0
        };
        StepResult::new(output, next, rows)
    }
    fn advance(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = self.runtime_error() {
            return Err(error);
        }
        if !ctx.work().try_consume(1) {
            return Ok(self.result(None, NextStep::Yield));
        }
        if self.phase == FinishPhase::Finalizing {
            ctx.perform(
                crate::runtime::fragment::PipelineOperation::Finishing,
                |_| {
                    #[cfg(novarocks_dev_controls)]
                    if self.aggregate.is_some() {
                        self.aggregate_guard
                            .check(TableWriteAggregateBoundary::FinalFinalize)
                            .map_err(ExecutionFailure::from_domain)?;
                    }
                    self.begin_finalize()
                },
            )?;
        }
        if let Some(chunk) = self.input.take() {
            let result = ctx.perform(crate::runtime::fragment::PipelineOperation::Push, |_| {
                self.consume_relation(&chunk)
            });
            if let Err(error) = result {
                ctx.report_failure(error.clone());
                self.input = Some(chunk);
                return Err(error);
            }
            self.pending_rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        }
        if self.phase == FinishPhase::Consuming {
            let next = if let Some(aggregate) = self.aggregate.as_mut() {
                if aggregate.runnable() {
                    let output = aggregate.advance(ctx)?;
                    if let Some(error) = self
                        .runtime_error
                        .as_ref()
                        .and_then(|control| control.error())
                    {
                        return Err(error);
                    }
                    if output.is_some() {
                        return Err(ExecutionFailure::from_domain(
                            TableFinishStepFailure::EarlyFinalOutput,
                        ));
                    }
                }
                if aggregate.can_accept() {
                    NextStep::NeedInput
                } else {
                    aggregate.work_next()
                }
            } else {
                NextStep::NeedInput
            };
            return Ok(self.result(None, next));
        }
        if self.phase != FinishPhase::Producing {
            return Ok(self.result(None, NextStep::Finished));
        }
        if let Some(prefix) = self.prefix_output.as_mut() {
            let output = prefix.pull()?;
            if prefix.is_finished() {
                self.prefix_output = None;
            }
            return Ok(self.result(output, NextStep::Runnable));
        }
        if let Some(grouped) = self.grouped_unpivot.as_mut() {
            let result = grouped.step(ctx)?;
            let next = if matches!(result.next, NextStep::Finished) {
                self.grouped_unpivot = None;
                NextStep::Runnable
            } else {
                result.next
            };
            return Ok(self.result(result.output, next));
        }
        if let Some(aggregate) = self.aggregate.as_mut() {
            if aggregate.runnable() {
                let started = Instant::now();
                let output = aggregate.advance(ctx)?;
                if let Some(error) = self
                    .runtime_error
                    .as_ref()
                    .and_then(|control| control.error())
                {
                    return Err(error);
                }
                if let Some(profiles) = self.profiles.as_ref() {
                    profiles.common.counter_add(
                        "FinalAggregateCpuTime",
                        ProfileUnit::TimeNs,
                        i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX),
                    );
                }
                if let Some(output) = output {
                    self.start_grouped_unpivot(output)?;
                    return Ok(self.result(None, NextStep::Runnable));
                }
            }
            if self.aggregate.as_ref().unwrap().is_complete() {
                self.validate_final_groups_complete()?;
                let next = self.aggregate.as_mut().unwrap().retire(ctx)?;
                if matches!(next, NextStep::Finished) {
                    self.aggregate = None;
                    return Ok(self.result(None, NextStep::Runnable));
                }
                return Ok(self.result(None, next));
            }
            let next = self.aggregate.as_ref().unwrap().work_next();
            self.final_aggregate_blocked_time
                .observe(matches!(next, NextStep::Wait(_)));
            return Ok(self.result(None, next));
        }
        self.aggregate_coverage.clear();
        self.final_groups_seen.clear();
        self.phase = FinishPhase::Finished;
        Ok(self.result(None, NextStep::Finished))
    }
}

#[cfg(test)]
pub(crate) use tests::complete_input as complete_table_finish_input;

#[cfg(test)]
mod tests {
    use crate::exec::pipeline::{shape::StepOperator, test_driver::TestDriver};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::array::{BinaryArray, ListArray, StringArray};
    use arrow::datatypes::DataType;
    use novarocks_spi::connector::write_stack::{
        MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES,
        MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES, ROOT_WRITE_RESULT_BLOB_TYPE_INDEX,
        ROOT_WRITE_RESULT_BODY_INDEX, ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
        ROOT_WRITE_RESULT_PROPERTIES_INDEX, ROOT_WRITE_RESULT_TARGET_INDEX, RootWriteResultSchema,
        WriterAuxiliaryChannel, WriterMultiplexSchema, root_write_result_column_id,
    };
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

    use super::*;
    use crate::exec::node::table_write_aggregate::{
        WriterFinalAggregateCall, WriterFinalAggregatePlan, WriterGroupedUnpivotMapping,
        WriterGroupedUnpivotPlan,
    };
    use crate::exec::operators::table_writer::tests::target;
    use crate::runtime::ExecutionRuntime;
    use crate::runtime::execution_runtime::ExecutionRuntimeConfig;

    fn compiled_root_owner_fixture() -> (
        TableFinishOperatorFactory,
        Arc<novarocks_local_program::LocalProgram>,
        RuntimeState,
    ) {
        let (program, binding, state) =
            crate::exec::pipeline::builder::compiled_statistics_finish_fixture();
        let factory = crate::exec::operators::compiled_writer::compiled_table_finish_factory(
            &program,
            program.graph().root(),
            0,
            &binding,
            &state.error_state(),
        )
        .expect("actual compiled TableFinish factory");
        assert!(factory.root_schema.is_compiled());
        assert!(
            factory.root_schema.prepared().is_none(),
            "factory retains only the frozen Root template"
        );
        (factory, program, state)
    }

    fn install_compiled_root_creator(
        operator: &mut TableFinishStep,
        state: &RuntimeState,
    ) -> Arc<MemTracker> {
        let tracker = MemTracker::new_root("compiled-outer-root-creator");
        operator.set_mem_tracker(Arc::clone(&tracker));
        assert!(operator.creates_typed_storage());
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            Arc::clone(&tracker), state.error_state(),
        ).unwrap();
        operator.set_construction_factory(factory);
        tracker
    }

    #[test]
    fn compiled_outer_root_prepare_retains_creator_until_last_schema_slot() {
        let (factory, program, state) = compiled_root_owner_fixture();
        let mut operator = factory.create_operator(1, 0);
        assert!(
            operator
                .kernel_for_test_mut()
                .root_schema
                .prepared()
                .is_none()
        );
        let tracker = install_compiled_root_creator(&mut operator, &state);
        let installed = tracker.current();
        operator
            .prepare()
            .expect("Root schema created by actual preparation owner");
        assert!(
            tracker.current() > installed,
            "real Root schema construction is observed"
        );
        let root = Arc::clone(
            operator
                .kernel_for_test_mut()
                .prepared_root_schema()
                .unwrap()
                .chunk_schema(),
        );
        assert!(root.is_typed());
        let slot = root.slots()[0].clone();
        let declared = program.graph().nodes()[program.graph().root().index()].output_layout();
        assert_eq!(root.slot_ids(), declared.slots());
        assert_eq!(slot.typed_binding(), declared.value_binding_at(0).unwrap());
        drop(operator);
        drop(factory);
        drop(program);
        drop(state);
        assert!(
            tracker.current() > 0,
            "Root schema holds only its finite creating responsibility"
        );
        drop(root);
        assert!(
            tracker.current() > 0,
            "independent Root slot retains the actual schema creator"
        );
        drop(slot);
        assert_eq!(tracker.current(), 0, "last Root schema holder exits");
    }

    #[test]
    fn compiled_outer_root_prepare_preserves_first_tracker_refusal_without_publishing() {
        use crate::exec::pipeline::operator::ConstructionFailure;
        use crate::runtime::fragment::ExecutionFailureCause;
        let (factory, program, state) = compiled_root_owner_fixture();
        let mut operator = factory.create_operator(1, 0);
        let tracker = install_compiled_root_creator(&mut operator, &state);
        let installed = tracker.current();
        assert!(installed > 0);
        tracker.install_limit_once(installed).unwrap();
        let failure = operator
            .prepare()
            .expect_err("first actual Root allocation is refused");
        assert!(
            matches!(
                failure.cause(),
                ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
            ),
            "original caller memory cause: {failure:?}"
        );
        assert!(
            operator
                .kernel_for_test_mut()
                .root_schema
                .prepared()
                .is_none(),
            "no partially prepared Root is published"
        );
        assert_eq!(operator.kernel_for_test_mut().phase, FinishPhase::Failed);
        assert_eq!(
            tracker.current(),
            installed,
            "refused scope rolls back its partial construction"
        );
        drop(operator);
        drop(factory);
        drop(program);
        drop(state);
        assert_eq!(
            tracker.current(),
            0,
            "refusal retains no uncreated Root schema"
        );
    }

    #[test]
    fn root_artifact_rebinds_reused_nested_arrays_to_root_layout_fields() {
        use arrow::array::{Int32Array, MapArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use novarocks_spi::connector::write_stack::root_write_result_schema;
        let root = RootWriteResultRelationSchema::fixed();
        let root_schema = Arc::clone(root.chunk_schema());
        // An upstream layout with equal types but independently owned fields.
        let upstream = root_write_result_schema();
        let DataType::List(item) = upstream.field(4).data_type() else {
            unreachable!()
        };
        let DataType::Map(entries, false) = upstream.field(7).data_type() else {
            unreachable!()
        };
        let DataType::Struct(entry_fields) = entries.data_type() else {
            unreachable!()
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![RootRowKind::ArtifactDraft.to_wire()])),
            Arc::new(Int32Array::from(vec![0])),
            new_null_array(&DataType::Int64, 1),
            new_null_array(&DataType::Binary, 1),
            Arc::new(ListArray::new(
                Arc::clone(item),
                OffsetBuffer::from_lengths([1]),
                Arc::new(Int32Array::from(vec![5])),
                None,
            )),
            Arc::new(StringArray::from(vec!["theta"])),
            Arc::new(BinaryArray::from(vec![b"body".as_slice()])),
            Arc::new(MapArray::new(
                Arc::clone(entries),
                OffsetBuffer::from_lengths([1]),
                StructArray::new(
                    entry_fields.clone(),
                    vec![
                        Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
                        Arc::new(StringArray::from(vec!["v"])) as ArrayRef,
                    ],
                    None,
                ),
                None,
                false,
            )),
        ];
        let upstream_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&upstream, root_schema.slot_ids())
                .unwrap();
        let batch = RecordBatch::try_new(upstream_schema.arrow_schema_ref(), columns).unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, upstream_schema).unwrap();
        let output = root_artifact_chunk(chunk, &root_schema, None)
            .unwrap()
            .expect("one artifact row");
        let root_arrow = root_schema.arrow_schema_ref();
        let (DataType::List(expected_item), DataType::List(actual_item)) = (
            root_arrow.field(4).data_type(),
            output.record_batch().column(4).data_type(),
        ) else {
            panic!("input_fields is a List");
        };
        assert!(Arc::ptr_eq(expected_item, &actual_item));
        assert!(!Arc::ptr_eq(item, &actual_item));
        let (DataType::Map(expected_entries, _), DataType::Map(actual_entries, _)) = (
            root_arrow.field(7).data_type(),
            output.record_batch().column(7).data_type(),
        ) else {
            panic!("properties is a Map");
        };
        assert!(Arc::ptr_eq(expected_entries, &actual_entries));
        let rebound = output
            .record_batch()
            .column(7)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        let (DataType::Struct(expected_children), DataType::Struct(actual_children)) =
            (expected_entries.data_type(), rebound.entries().data_type())
        else {
            panic!("entries are a Struct");
        };
        assert!(Arc::ptr_eq(&expected_children[0], &actual_children[0]));
        assert!(Arc::ptr_eq(&expected_children[1], &actual_children[1]));
    }

    /// A row shape the tests can express, including shapes a correct
    /// `TableWriter` would never produce.
    type RawRow = (i8, Option<i32>, Option<i64>, Option<Vec<u8>>);

    #[derive(Default)]
    struct AcceptEveryCarrier {
        calls: AtomicUsize,
    }

    impl ConnectorCommitFragmentCarrierValidator for AcceptEveryCarrier {
        fn validate(
            &self,
            _target: WriteTargetOrdinal,
            _encoded: &[u8],
        ) -> Result<(), ConnectorError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct RejectEveryCarrier;

    impl ConnectorCommitFragmentCarrierValidator for RejectEveryCarrier {
        fn validate(
            &self,
            _target: WriteTargetOrdinal,
            _encoded: &[u8],
        ) -> Result<(), ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "not a canonical carrier of the expected provider",
            ))
        }
    }

    #[cfg(novarocks_dev_controls)]
    struct RejectAggregateBoundary(TableWriteAggregateBoundary);

    #[cfg(novarocks_dev_controls)]
    impl TableWriteAggregateGuard for RejectAggregateBoundary {
        fn check(&self, boundary: TableWriteAggregateBoundary) -> Result<(), ConnectorError> {
            if boundary == self.0 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("rejected {boundary:?}"),
                ));
            }
            Ok(())
        }
    }

    fn with_step<T>(
        state: &RuntimeState,
        units: u32,
        action: impl FnOnce(&mut StepContext<'_>) -> T,
    ) -> T {
        let error = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, units);
        action(&mut StepContext::new(&error, &mut work))
    }
    pub(crate) fn complete_input(
        step: &mut dyn Operator,
        state: &RuntimeState,
        chunk: Chunk,
    ) -> ExecutionResult<()> {
        TestDriver::new(step, state).input_without_output(chunk)
    }
    fn finish_failure(
        operator: &mut dyn crate::exec::pipeline::shape::StepOperator,
        state: &RuntimeState,
    ) -> ExecutionFailure {
        with_step(state, 64, |ctx| {
            if let Err(error) = operator.finish(ctx) {
                return error;
            }
            for _ in 0..32 {
                if let Some(error) = state.error() {
                    return error;
                }
                match operator.drive(ctx) {
                    Err(error) => return error,
                    Ok(result) => assert!(
                        !matches!(result.next, NextStep::Finished),
                        "injected finalization cannot succeed"
                    ),
                }
            }
            panic!("fixture finalization failure must be reported");
        })
    }
    fn await_final_wait(
        operator: &mut dyn crate::exec::pipeline::shape::StepOperator,
        state: &RuntimeState,
    ) -> WaitFact {
        for _ in 0..16 {
            let result = with_step(state, 64, |ctx| operator.drive(ctx)).unwrap();
            if let NextStep::Wait(wait) = result.next {
                return wait;
            }
        }
        panic!("final aggregate must report the exact work wait");
    }
    struct ScriptedFinalAggregate {
        outputs: VecDeque<Chunk>,
        accepting: Arc<AtomicBool>,
        ready: Arc<AtomicBool>,
        observable: Arc<crate::exec::pipeline::schedule::observer::Observable>,
        finishing: bool,
        pushed_rows: usize,
    }

    impl ScriptedFinalAggregate {
        fn new(outputs: Vec<Chunk>, accepting: bool, ready: bool) -> Self {
            Self {
                outputs: outputs.into(),
                accepting: Arc::new(AtomicBool::new(accepting)),
                ready: Arc::new(AtomicBool::new(ready)),
                observable: Arc::new(Observable::try_bounded(1).unwrap()),
                finishing: false,
                pushed_rows: 0,
            }
        }
    }

    impl OperatorLifecycle for ScriptedFinalAggregate {
        fn name(&self) -> &str {
            "SCRIPTED_FINAL_AGGREGATE"
        }
    }

    impl InputEncodingPolicy for ScriptedFinalAggregate {}
    impl TransformKernel for ScriptedFinalAggregate {
        fn control_observable(&self) -> Option<Arc<Observable>> {
            Some(self.observable.clone())
        }
        fn admission(
            &self,
            _: &mut StepContext<'_>,
            _: &RawInput,
        ) -> ExecutionResult<Option<AdmissionNext>> {
            let generation = self.observable.generation();
            Ok((!self.accepting.load(Ordering::Acquire)).then(|| {
                AdmissionNext::Wait(WaitFact {
                    reason: WaitReason::Output,
                    observable: self.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                })
            }))
        }
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            self.pushed_rows += input.chunk().len();
            Ok(StepResult::new(None, NextStep::NeedInput, 0))
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            let generation = self.observable.generation();
            if !self.ready.load(Ordering::Acquire) {
                return Ok(StepResult::new(
                    None,
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Finish,
                        observable: self.observable.clone(),
                        observed_generation: generation,
                        deadline: None,
                    }),
                    0,
                ));
            }
            let output = self.outputs.pop_front();
            Ok(StepResult::new(
                output,
                if self.outputs.is_empty() {
                    NextStep::Finished
                } else {
                    NextStep::Runnable
                },
                0,
            ))
        }
        fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            self.finishing = true;
            self.drive(ctx)
        }
    }

    struct ErrorOnlyFinalAggregate;

    impl OperatorLifecycle for ErrorOnlyFinalAggregate {
        fn name(&self) -> &str {
            "ERROR_ONLY_FINAL_AGGREGATE"
        }
    }

    impl InputEncodingPolicy for ErrorOnlyFinalAggregate {}
    impl TransformKernel for ErrorOnlyFinalAggregate {
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(None, NextStep::NeedInput, 0))
        }
        fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            let failure: ExecutionFailure = novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new(
                    "scripted final aggregate failed asynchronously",
                ),
            )
            .into();
            ctx.report_failure(failure);
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
    }

    struct FinalizeErrorAggregate;

    impl OperatorLifecycle for FinalizeErrorAggregate {
        fn name(&self) -> &str {
            "FINALIZE_ERROR_AGGREGATE"
        }
    }

    impl InputEncodingPolicy for FinalizeErrorAggregate {}
    impl TransformKernel for FinalizeErrorAggregate {
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(None, NextStep::NeedInput, 0))
        }
        fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            let failure: ExecutionFailure = novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new(
                    "scripted final aggregate synchronous finalize failure",
                ),
            )
            .into();
            Err(failure)
        }
    }

    fn finish_fixture(
        targets: usize,
        validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    ) -> FinishFixture {
        let expected = (0..targets)
            .map(|value| target(u32::try_from(value).expect("bounded")))
            .collect();
        FinishFixture::try_new(3, expected, validator).expect("table finish node")
    }

    fn factory(targets: usize) -> TableFinishOperatorFactory {
        TableFinishOperatorFactory::for_test_fixture(
            &finish_fixture(targets, Arc::new(AcceptEveryCarrier::default())),
            #[cfg(test)]
            Arc::new(ExprArena::default()),
        )
    }

    fn writer_rows(rows: Vec<RawRow>) -> Chunk {
        let mut kinds = Vec::with_capacity(rows.len());
        let mut ordinals = Vec::with_capacity(rows.len());
        let mut row_counts = Vec::with_capacity(rows.len());
        let mut payloads = BinaryBuilder::new();
        for (kind, ordinal, row_count, payload) in rows {
            kinds.push(kind);
            ordinals.push(ordinal);
            row_counts.push(row_count);
            match payload {
                Some(bytes) => payloads.append_value(&bytes),
                None => payloads.append_null(),
            }
        }
        // Arrow permits constructing a null bitmap against a non-null field;
        // the operator must still validate the row rather than trusting schema
        // metadata as proof about an untrusted Exchange batch.
        let relation = WriterMultiplexRelationSchema::empty();
        let schema = relation.chunk_schema().arrow_schema_ref();
        let chunk_schema = Arc::clone(relation.chunk_schema());
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(payloads.finish()) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("writer relation batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("writer relation chunk")
    }

    type CompositeRow = (i8, i32, Option<i64>, Option<Vec<u8>>, Vec<Option<Vec<u8>>>);

    fn composite_rows(schema: &WriterMultiplexRelationSchema, rows: Vec<CompositeRow>) -> Chunk {
        let mut kinds = Vec::with_capacity(rows.len());
        let mut ordinals = Vec::with_capacity(rows.len());
        let mut row_counts = Vec::with_capacity(rows.len());
        let mut fragments = BinaryBuilder::new();
        let channels = schema.contract().auxiliary_channels().len();
        let mut auxiliary = (0..channels)
            .map(|_| BinaryBuilder::new())
            .collect::<Vec<_>>();
        for (kind, target, row_count, fragment, values) in rows {
            assert_eq!(values.len(), channels);
            kinds.push(kind);
            ordinals.push(target);
            row_counts.push(row_count);
            match fragment {
                Some(bytes) => fragments.append_value(bytes),
                None => fragments.append_null(),
            }
            for (builder, value) in auxiliary.iter_mut().zip(values) {
                match value {
                    Some(bytes) => builder.append_value(bytes),
                    None => builder.append_null(),
                }
            }
        }
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(fragments.finish()),
        ];
        columns.extend(
            auxiliary
                .into_iter()
                .map(|mut builder| Arc::new(builder.finish()) as ArrayRef),
        );
        let batch = RecordBatch::try_new(schema.chunk_schema().arrow_schema_ref(), columns)
            .expect("composite writer batch");
        Chunk::try_new_with_chunk_schema(batch, Arc::clone(schema.chunk_schema()))
            .expect("composite writer chunk")
    }

    fn composite_runtime_state(
        function_set: Arc<crate::exec::expr::agg::SealedExecutionFunctionSet>,
        tracker: Option<Arc<MemTracker>>,
    ) -> RuntimeState {
        let runtime = Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                function_set,
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .expect("composite test runtime"),
        );
        RuntimeState::for_local_execution(None, None, None, None, None, tracker, Some(runtime))
    }

    fn install_composite_construction(
        operator: &mut dyn Operator,
        state: &RuntimeState,
        tracker: Arc<MemTracker>,
    ) {
        operator.set_construction_factory(
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker, state.error_state(),
            ).unwrap(),
        );
    }

    fn composite_fixture(
        target_count: usize,
        channel_count: usize,
        max_output_rows: usize,
    ) -> (
        TableFinishOperatorFactory,
        RuntimeState,
        WriterMultiplexRelationSchema,
    ) {
        let function_set = crate::exec::expr::agg::test_builtin_execution_function_set();
        let resolved = function_set
            .catalog()
            .resolve_aggregate_trusted("any_value", &[DataType::Binary])
            .expect("any_value(Binary)");
        let writer_contract = WriterMultiplexSchema::try_new(
            (0..channel_count)
                .map(|index| {
                    WriterAuxiliaryChannel::try_new(
                        100 + u32::try_from(index).expect("channel slot"),
                        format!("partial_{index}"),
                        novarocks_type_contract::FunctionValueType::new(DataType::Binary, true),
                    )
                    .expect("auxiliary channel")
                })
                .collect(),
        )
        .expect("writer schema");
        let writer_schema = WriterMultiplexRelationSchema::try_new(writer_contract)
            .expect("execution writer schema");
        let root_schema = RootWriteResultRelationSchema::try_new(RootWriteResultSchema::new())
            .expect("root schema");
        let grouping_output_slot_id = novarocks_types::SlotId::new(10_000);
        let calls = (0..channel_count)
            .map(|index| WriterFinalAggregateCall {
                // Explicit legacy carrier fixture; this is not frozen-binding admission.
                frozen: None,
                function_name: Arc::from("any_value"),
                resolved: resolved.clone(),
                intermediate_input_slot_id: novarocks_types::SlotId::new(
                    100 + u32::try_from(index).expect("channel slot"),
                ),
                final_output_slot_id: novarocks_types::SlotId::new(
                    10_001 + u32::try_from(index).expect("final slot"),
                ),
            })
            .collect::<Vec<_>>();
        let mut arena = ExprArena::default();
        let blob_type = arena.push_typed(
            crate::exec::expr::ExprNode::Literal(crate::exec::expr::LiteralValue::Utf8(
                "test/blob-v1".to_string(),
            )),
            DataType::Utf8,
        );
        let mappings = (0..target_count)
            .flat_map(|target_index| {
                (0..channel_count).map(move |channel_index| (target_index, channel_index))
            })
            .map(
                |(target_index, channel_index)| WriterGroupedUnpivotMapping {
                    grouping_key: u32::try_from(target_index).expect("target"),
                    input_value_slot_id: calls[channel_index].final_output_slot_id,
                    constants: vec![
                        crate::exec::node::unpivot::UnpivotConstant::Int32List(vec![
                            i32::try_from(target_index * channel_count + channel_index + 1)
                                .expect("field id"),
                        ]),
                        crate::exec::node::unpivot::UnpivotConstant::Scalar {
                            expr_id: blob_type,
                            nullable: false,
                        },
                        crate::exec::node::unpivot::UnpivotConstant::Utf8Map(Vec::new()),
                    ],
                },
            )
            .collect();
        let final_plan = WriterFinalAggregatePlan {
            prepared: None,
            calls,
            unpivot: Some(WriterGroupedUnpivotPlan {
                grouping_input_slot_id:
                    crate::exec::node::table_write_relation::WRITE_RELATION_TARGET_SLOT,
                grouping_output_slot_id,
                passthrough_output_slot_id: novarocks_types::SlotId::new(
                    root_write_result_column_id(ROOT_WRITE_RESULT_TARGET_INDEX),
                ),
                value_output_slot_id: novarocks_types::SlotId::new(root_write_result_column_id(
                    ROOT_WRITE_RESULT_BODY_INDEX,
                )),
                literal_output_slot_ids: vec![
                    novarocks_types::SlotId::new(root_write_result_column_id(
                        ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
                    )),
                    novarocks_types::SlotId::new(root_write_result_column_id(
                        ROOT_WRITE_RESULT_BLOB_TYPE_INDEX,
                    )),
                    novarocks_types::SlotId::new(root_write_result_column_id(
                        ROOT_WRITE_RESULT_PROPERTIES_INDEX,
                    )),
                ],
                mappings,
                max_output_rows,
                max_output_bytes: 32 * 1024 * 1024,
            }),
        };
        let expected = (0..target_count)
            .map(|index| target(u32::try_from(index).expect("target")))
            .collect();
        let node = FinishFixture::try_new_with_relations(
            3,
            expected,
            Arc::new(AcceptEveryCarrier::default()),
            writer_schema.clone(),
            root_schema,
            #[cfg(test)]
            final_plan,
        )
        .expect("composite finish node");
        let state = composite_runtime_state(Arc::clone(&function_set), None);
        (
            TableFinishOperatorFactory::for_test_fixture(&node, Arc::new(arena)),
            state,
            writer_schema,
        )
    }

    fn install_composite_input_creator(
        operator: &mut dyn Operator,
        state: &RuntimeState,
        tracker: Arc<MemTracker>,
    ) {
        operator.set_mem_tracker(Arc::clone(&tracker));
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile()
                .expect("actual runtime construction profile"),
            tracker,
            state.error_state(),
        )
        .expect("actual composite input construction factory");
        operator.set_construction_factory(factory);
    }

    fn scripted_final_chunk(
        factory: &TableFinishOperatorFactory,
        rows: Vec<(i32, Vec<Option<Vec<u8>>>)>,
    ) -> Chunk {
        let unpivot = factory.final_plan.unpivot.as_ref().expect("unpivot plan");
        let mut groups = Vec::with_capacity(rows.len());
        let mut values = (0..factory.final_plan.calls.len())
            .map(|_| BinaryBuilder::new())
            .collect::<Vec<_>>();
        for (group, row_values) in rows {
            assert_eq!(row_values.len(), values.len());
            groups.push(group);
            for (builder, value) in values.iter_mut().zip(row_values) {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
        }
        let mut fields = vec![Field::new("write_target_ordinal", DataType::Int32, false)];
        let mut slots = vec![unpivot.grouping_output_slot_id];
        let mut columns = vec![Arc::new(Int32Array::from(groups)) as ArrayRef];
        for (index, (call, mut builder)) in factory.final_plan.calls.iter().zip(values).enumerate()
        {
            fields.push(Field::new(
                format!("final_aggregate_{index}"),
                call.resolved.output_type.clone(),
                true,
            ));
            slots.push(call.final_output_slot_id);
            columns.push(Arc::new(builder.finish()));
        }
        let schema = Schema::new(fields);
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &slots)
            .expect("scripted final schema");
        let batch = RecordBatch::try_new(Arc::new(schema), columns).expect("scripted final batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("scripted final chunk")
    }

    fn row_count_row(ordinal: i32, rows: i64) -> RawRow {
        (
            WriterRowKind::RowCount.to_wire(),
            Some(ordinal),
            Some(rows),
            None,
        )
    }

    fn fragment_row(ordinal: i32, bytes: Vec<u8>) -> RawRow {
        (
            WriterRowKind::CommitFragment.to_wire(),
            Some(ordinal),
            None,
            Some(bytes),
        )
    }

    fn finish_columns(chunk: &Chunk) -> (Int8Array, Int32Array, Int64Array, BinaryArray) {
        let kinds = chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("kind")
            .clone();
        let ordinals = chunk.columns()[1]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ordinal")
            .clone();
        let row_counts = chunk.columns()[2]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row count")
            .clone();
        let fragments = chunk.columns()[3]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("fragment")
            .clone();
        (kinds, ordinals, row_counts, fragments)
    }

    fn run(operator: &mut Box<dyn Operator>, chunks: Vec<Chunk>) -> ExecutionResult<Vec<Chunk>> {
        let state = RuntimeState::default();
        let mut calls = TestDriver::new(operator.as_mut(), &state);
        for chunk in chunks {
            calls.input_without_output(chunk)?;
        }
        let idle = calls.drive()?;
        assert!(
            idle.output.is_none(),
            "table finish must emit nothing before EOS"
        );
        assert!(matches!(idle.next, NextStep::NeedInput));
        calls.signal_eos()?;
        calls.collect_until_finished()
    }

    #[test]
    fn table_finish_runs_at_dop_one_and_rejects_any_other_parallelism() {
        let factory = factory(1);
        let mut single = factory.create(1, 0);
        assert!(single.prepare().is_ok());

        let mut wide = factory.create(4, 0);
        let error = wide.prepare().expect_err("DOP > 1 must fail closed");
        assert!(error.detail().contains("must run at DOP 1"));

        let mut second_driver = factory.create(1, 1);
        let error = second_driver
            .prepare()
            .expect_err("a second driver must fail closed");
        assert!(error.detail().contains("must run at DOP 1"));
    }

    #[test]
    fn table_finish_checked_sums_row_counts_from_many_senders_in_any_order() {
        let factory = factory(2);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let outputs = run(
            &mut operator,
            vec![
                writer_rows(vec![
                    fragment_row(1, b"second-target".to_vec()),
                    row_count_row(1, 7),
                ]),
                writer_rows(vec![row_count_row(0, 5)]),
                writer_rows(vec![
                    row_count_row(0, 0),
                    fragment_row(0, b"first-target".to_vec()),
                ]),
            ],
        )
        .expect("finish");
        let mut prepared = Vec::new();
        let mut summaries = 0;
        for output in &outputs {
            let (kinds, ordinals, row_counts, fragments) = finish_columns(output);
            for row in 0..output.len() {
                match RootRowKind::from_wire(kinds.value(row)).expect("root row kind") {
                    RootRowKind::Summary => {
                        summaries += 1;
                        assert!(ordinals.is_null(row));
                        assert_eq!(row_counts.value(row), 12);
                        assert!(fragments.is_null(row));
                    }
                    RootRowKind::PreparedFragment => {
                        assert!(row_counts.is_null(row));
                        prepared.push((ordinals.value(row), fragments.value(row).to_vec()));
                    }
                    RootRowKind::ArtifactDraft => panic!("fragment-only finish emitted artifact"),
                }
            }
        }
        assert_eq!(summaries, 1);
        prepared.sort_unstable();
        assert_eq!(
            prepared,
            vec![
                (0, b"first-target".to_vec()),
                (1, b"second-target".to_vec())
            ]
        );
    }

    #[test]
    fn table_finish_emits_only_a_summary_when_no_writer_staged_anything() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let outputs =
            run(&mut operator, vec![writer_rows(vec![row_count_row(0, 0)])]).expect("finish");
        assert_eq!(outputs.len(), 1);
        let output = &outputs[0];
        assert_eq!(output.len(), 1);
        let (kinds, _, row_counts, _) = finish_columns(output);
        assert_eq!(kinds.value(0), RootRowKind::Summary.to_wire());
        assert_eq!(row_counts.value(0), 0);
    }

    #[test]
    fn table_finish_rejects_inconsistent_null_and_kind_combinations() {
        let payload_mismatch = "payload does not match its row kind";
        let cases: Vec<(Vec<RawRow>, &str)> = vec![
            // ROW_COUNT with no row count
            (
                vec![(WriterRowKind::RowCount.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            // ROW_COUNT carrying a commit fragment
            (
                vec![(
                    WriterRowKind::RowCount.to_wire(),
                    Some(0),
                    Some(1),
                    Some(b"x".to_vec()),
                )],
                payload_mismatch,
            ),
            // COMMIT_FRAGMENT carrying a row count
            (
                vec![(
                    WriterRowKind::CommitFragment.to_wire(),
                    Some(0),
                    Some(1),
                    Some(b"x".to_vec()),
                )],
                payload_mismatch,
            ),
            // COMMIT_FRAGMENT with no fragment
            (
                vec![(WriterRowKind::CommitFragment.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            // both payloads null
            (
                vec![(WriterRowKind::CommitFragment.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            (
                vec![(0, Some(0), Some(1), None)],
                "unknown connector writer row kind",
            ),
            (vec![(3, Some(0), Some(1), None)], payload_mismatch),
        ];

        for (rows, expected) in cases {
            let factory = factory(1);
            let mut operator = factory.create(1, 0);
            operator.prepare().expect("prepare");
            let state = RuntimeState::default();
            let error = complete_input(operator.as_mut(), &state, writer_rows(rows))
                .expect_err("invalid row shape");
            assert!(
                error.detail().contains(expected),
                "expected {expected:?} in error {error:?}"
            );
        }
    }

    #[test]
    fn table_finish_rejects_a_target_ordinal_outside_the_sealed_set() {
        let factory = factory(2);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = complete_input(
            operator.as_mut(),
            &state,
            writer_rows(vec![row_count_row(2, 1)]),
        )
        .expect_err("ordinal outside the sealed set");
        assert!(
            error
                .detail()
                .contains("outside the sealed set of 2 targets")
        );
    }

    #[test]
    fn table_finish_rejects_a_non_canonical_carrier_before_it_enters_the_set() {
        let node = finish_fixture(1, Arc::new(RejectEveryCarrier));
        let factory =
            TableFinishOperatorFactory::for_test_fixture(&node, Arc::new(ExprArena::default()));
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = complete_input(
            operator.as_mut(),
            &state,
            writer_rows(vec![fragment_row(0, b"junk".to_vec())]),
        )
        .expect_err("foreign carrier");
        assert!(
            error
                .detail()
                .contains("not a canonical carrier of the expected provider")
        );
    }

    #[test]
    fn table_finish_row_count_overflow_is_an_error_not_a_saturating_counter() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        // The accumulator stays unsigned and checked, so two maximal signed
        // row counts fit and the third must fail rather than wrap.
        for _ in 0..2 {
            processor
                .input_without_output(writer_rows(vec![row_count_row(0, i64::MAX)]))
                .expect("within the unsigned accumulator");
        }
        let error = processor
            .input_without_output(writer_rows(vec![row_count_row(0, i64::MAX)]))
            .expect_err("row count overflow");
        assert!(error.detail().contains("row count overflowed"));
    }

    #[test]
    fn table_finish_rejects_a_negative_row_count_as_corrupt_data() {
        // The relation's carrier is signed, so a corrupt negative value is
        // newly expressible. It must be rejected, never reinterpreted as a
        // huge unsigned row count the client would see as truth.
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = complete_input(
            operator.as_mut(),
            &state,
            writer_rows(vec![row_count_row(0, -1)]),
        )
        .expect_err("a negative row count is corrupt data");
        assert!(
            error.detail().contains("payload does not match"),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn table_finish_rejects_a_negative_target_ordinal_as_corrupt_data() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = complete_input(
            operator.as_mut(),
            &state,
            writer_rows(vec![row_count_row(-1, 1)]),
        )
        .expect_err("a negative target ordinal is corrupt data");
        assert!(
            error.detail().contains("target ordinal is negative"),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn table_finish_accepts_the_exact_single_fragment_limit_and_rejects_one_more() {
        let state = RuntimeState::default();
        let at_limit = factory(1);
        let mut operator = at_limit.create(1, 0);
        operator.prepare().expect("prepare");
        complete_input(
            operator.as_mut(),
            &state,
            writer_rows(vec![fragment_row(
                0,
                vec![7; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
            )]),
        )
        .expect("the exact single-fragment budget is legal");

        let over_limit = factory(1);
        let mut operator = over_limit.create(1, 0);
        operator.prepare().expect("prepare");
        let error = complete_input(
            operator.as_mut(),
            &state,
            writer_rows(vec![fragment_row(
                0,
                vec![7; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1],
            )]),
        )
        .expect_err("over the single-fragment budget");
        assert!(
            error
                .detail()
                .contains("exceeds the frozen single-fragment budget")
        );
    }

    #[test]
    fn table_finish_rejects_the_prepared_write_set_byte_budget() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        let full = MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;
        for _ in 0..full {
            processor
                .input_without_output(writer_rows(vec![fragment_row(
                    0,
                    vec![1; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
                )]))
                .expect("within the set byte budget");
        }
        let error = processor
            .input_without_output(writer_rows(vec![fragment_row(0, vec![1])]))
            .expect_err("over the set byte budget");
        assert!(error.detail().contains("exceeds the frozen byte budget"));
    }

    #[test]
    fn table_finish_rejects_the_prepared_write_set_entry_budget() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        let rows = (0..MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES)
            .map(|_| fragment_row(0, vec![1]))
            .collect::<Vec<_>>();
        processor
            .input_without_output(writer_rows(rows))
            .expect("the exact entry budget is legal");
        let error = processor
            .input_without_output(writer_rows(vec![fragment_row(0, vec![1])]))
            .expect_err("over the entry budget");
        assert!(error.detail().contains("exceeds the frozen entry budget"));
    }

    #[test]
    fn table_finish_streams_a_large_prepared_set_in_byte_bounded_batches() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        let tracker = MemTracker::new_root("table-finish-large-prefix");
        operator.set_mem_tracker(Arc::clone(&tracker));
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        for index in 0..40u8 {
            complete_input(
                operator.as_mut(),
                &state,
                writer_rows(vec![fragment_row(
                    0,
                    vec![index; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
                )]),
            )
            .expect("bounded prepared fragment");
        }
        operator.finish_for_test(&state).expect("gather EOS");
        let mut outputs = Vec::new();
        for output in TestDriver::new(operator.as_mut(), &state)
            .collect_until_finished()
            .expect("stream prefix")
        {
            {
                assert!(
                    root_output_size(&output.record_batch(), None).expect("output size")
                        <= MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                );
                outputs.push(output);
            }
        }
        let fragment_batches = outputs
            .iter()
            .filter(|output| {
                output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("kind")
                    .value(0)
                    == RootRowKind::PreparedFragment.to_wire()
            })
            .count();
        assert!(fragment_batches >= 2, "large prefix must be multi-batch");
        let fragment_rows = outputs
            .iter()
            .flat_map(|output| {
                let kinds = output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("kind");
                (0..output.len()).map(move |row| kinds.value(row))
            })
            .filter(|kind| *kind == RootRowKind::PreparedFragment.to_wire())
            .count();
        assert_eq!(fragment_rows, 40);
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("kind"))
                .flat_map(|kinds| (0..kinds.len()).map(|row| kinds.value(row)))
                .filter(|kind| *kind == RootRowKind::Summary.to_wire())
                .count(),
            1
        );
        drop(outputs);
        assert_eq!(tracker.current(), 0, "all prefix/output leases released");
        assert!(tracker.peak() > 40 * 1024 * 1024);
    }

    #[test]
    fn table_finish_emits_nothing_before_eos_and_releases_its_buffer_on_abort() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        {
            let mut processor = TestDriver::new(operator.as_mut(), &state);
            processor
                .input_without_output(writer_rows(vec![
                    row_count_row(0, 11),
                    fragment_row(0, vec![3; 4096]),
                    fragment_row(0, vec![4; 4096]),
                ]))
                .expect("accumulate");
            assert!(matches!(
                processor.next(),
                NextStep::NeedInput | NextStep::Finished
            ));
            assert!(
                processor
                    .drive()
                    .map(|step| step.output)
                    .expect("pull before EOS")
                    .is_none()
            );
        }

        operator.cancel();
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        assert!(
            processor
                .drive()
                .map(|step| step.output)
                .expect("pull after abort")
                .is_none()
        );
        let ended = operator.drive_for_test(&state).unwrap();
        assert!(ended.output.is_none());
        assert!(matches!(ended.next, NextStep::Finished));
    }

    #[test]
    fn native_finish_retains_accepted_rows_across_zero_quota_and_signals_eos_once() {
        use crate::exec::pipeline::{
            shape::StepOperator, step::InputResult, step_context::TurnWork,
        };
        let tracker = MemTracker::new_root("native finish quantum custody");
        let factory = factory(1);
        let mut operator = factory.create_operator(1, 0);
        operator.set_mem_tracker(tracker.clone());
        operator.prepare().unwrap();
        let state = RuntimeState::default();
        operator.bind_runtime_state(&state).unwrap();
        let error = state.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 0);
        let mut ctx = StepContext::new(&error, &mut work);
        assert!(matches!(
            operator.try_push(
                &mut ctx,
                RawInput::for_test(writer_rows(vec![
                    row_count_row(0, 9),
                    fragment_row(0, vec![3; 2048])
                ]))
            ),
            InputResult::Accepted(Ok(_))
        ));
        operator.finish(&mut ctx).unwrap();
        let paused = operator.drive(&mut ctx).unwrap();
        assert!(paused.output.is_none());
        assert!(matches!(paused.next, NextStep::Yield));
        assert_eq!(operator.kernel_for_test().rows.get(), 0);
        assert!(operator.kernel_for_test().prefix_output.is_none());
        ctx.work().begin_turn(1).unwrap();
        let applied = operator.drive(&mut ctx).unwrap();
        assert!(applied.output.is_none());
        assert!(matches!(applied.next, NextStep::Runnable));
        assert_eq!(operator.kernel_for_test().rows.get(), 9);
        assert_eq!(operator.kernel_for_test().fragments.len(), 1);
        let finalizing = operator.drive(&mut ctx).unwrap();
        assert!(finalizing.output.is_none());
        assert!(matches!(finalizing.next, NextStep::Yield));
        assert_eq!(operator.kernel_for_test().phase, FinishPhase::Finalizing);
        assert!(
            operator.kernel_for_test().prefix_output.is_none(),
            "EOS alone never constructs the root prefix"
        );
        ctx.work().begin_turn(1).unwrap();
        let summary = operator.drive(&mut ctx).unwrap().output.unwrap();
        assert_eq!(
            summary.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            RootRowKind::Summary.to_wire()
        );
        assert_eq!(
            summary.columns()[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            9
        );
        operator.finish(&mut ctx).unwrap();
        let empty = operator.drive(&mut ctx).unwrap();
        assert!(empty.output.is_none());
        assert!(matches!(empty.next, NextStep::Yield));
        ctx.work().begin_turn(8).unwrap();
        let mut outputs = vec![summary];
        let mut finished = false;
        for _ in 0..8 {
            let result = operator.drive(&mut ctx).unwrap();
            if let Some(output) = result.output {
                outputs.push(output);
            }
            if matches!(result.next, NextStep::Finished) {
                finished = true;
                break;
            }
        }
        assert!(finished);
        assert_eq!(outputs.len(), 2);
        assert_eq!(
            outputs[1].columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            RootRowKind::PreparedFragment.to_wire()
        );
        assert_eq!(
            outputs[1].columns()[3]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            vec![3; 2048]
        );
        operator.begin_exit().unwrap();
        operator.close().unwrap();
        drop(outputs);
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn table_finish_drops_its_buffer_when_a_row_is_rejected() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor
            .input_without_output(writer_rows(vec![fragment_row(0, vec![1; 1024])]))
            .expect("first fragment");
        let error = processor
            .input_without_output(writer_rows(vec![(
                WriterRowKind::CommitFragment.to_wire(),
                Some(0),
                Some(1),
                Some(vec![2; 1024]),
            )]))
            .expect_err("invalid row");
        assert!(
            error
                .detail()
                .contains("payload does not match its row kind")
        );
        let finish_error = processor
            .signal_eos()
            .expect_err("a failed stream cannot be repaired into a summary");
        assert!(
            finish_error.same_occurrence(&error),
            "EOS preserves the original accepted failure"
        );
        assert!(processor.drive().unwrap_err().same_occurrence(&error));
    }

    #[test]
    fn table_finish_rejects_a_writer_row_that_arrives_after_eos() {
        use crate::exec::pipeline::step::InputResult;
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().unwrap();
        let state = RuntimeState::default();
        with_step(&state, 64, |ctx| {
            let step = operator.as_mut();
            step.finish(ctx).unwrap();
            let raw = RawInput::for_test(writer_rows(vec![row_count_row(0, 1)]));
            let original = raw.original_for_test().columns()[0].clone();
            match step.try_push(ctx, raw) {
                InputResult::Rejected { input, failure } => {
                    use novarocks_failure_contract::FailureProjection;
                    assert!(Arc::ptr_eq(
                        &original,
                        &input.original_for_test().columns()[0]
                    ));
                    assert_eq!(
                        failure.failure_fact().class(),
                        novarocks_failure_contract::FailureClass::Invariant
                    );
                }
                _ => panic!("input EOS cannot accept or defer another raw value"),
            }
        });
    }

    #[test]
    fn table_finish_requires_a_non_empty_duplicate_free_target_set() {
        assert!(
            FinishFixture::try_new(3, Vec::new(), Arc::new(AcceptEveryCarrier::default()),)
                .is_err()
        );
        assert!(
            FinishFixture::try_new(
                3,
                vec![target(1), target(1)],
                Arc::new(AcceptEveryCarrier::default()),
            )
            .is_err()
        );
    }

    /// A copy-on-write statement drives one query per rewritten file against a
    /// single write session, and each of those queries compiles exactly one
    /// writer -- the one at that group's own ordinal. Query `k` therefore
    /// expects `[k]`, which is correctly not dense from zero. Denseness belongs
    /// to the session's sealed target set, not to any one query.
    #[test]
    fn table_finish_accepts_a_single_writer_query_at_a_non_zero_ordinal() {
        let node =
            FinishFixture::try_new(3, vec![target(2)], Arc::new(AcceptEveryCarrier::default()))
                .expect("a single-writer query at ordinal 2");
        assert!(node.accepts_target(target(2)));
        // Membership stays exact: a bound check would have admitted every
        // ordinal below the one this query actually feeds.
        assert!(!node.accepts_target(target(0)));
        assert!(!node.accepts_target(target(1)));

        let factory =
            TableFinishOperatorFactory::for_test_fixture(&node, Arc::new(ExprArena::default()));
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor
            .input_without_output(writer_rows(vec![row_count_row(2, 5)]))
            .expect("its own writer's rows are accepted");
        let error = processor
            .input_without_output(writer_rows(vec![row_count_row(0, 1)]))
            .expect_err("a target this query never compiled a writer for");
        assert!(error.detail().contains("outside the sealed set"), "{error}");
    }

    #[test]
    fn composite_finish_demultiplexes_interleaved_rows_merges_sparse_partials_and_streams_unpivot()
    {
        let (factory, state, writer_schema) = composite_fixture(2, 2, 1);
        let mut operator = factory.create(1, 0);
        install_composite_construction(
            operator.as_mut(),
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind aggregate");
        complete_input(
            operator.as_mut(),
            &state,
            composite_rows(
                &writer_schema,
                vec![
                    (
                        WriterRowKind::CommitFragment.to_wire(),
                        1,
                        None,
                        Some(b"fragment-one".to_vec()),
                        vec![None, None],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"zero-a".to_vec()), None],
                    ),
                    (
                        WriterRowKind::RowCount.to_wire(),
                        1,
                        Some(7),
                        None,
                        vec![None, None],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        1,
                        None,
                        None,
                        vec![Some(b"one-a".to_vec()), None],
                    ),
                ],
            ),
        )
        .expect("first interleaved batch");
        complete_input(
            operator.as_mut(),
            &state,
            composite_rows(
                &writer_schema,
                vec![
                    (
                        WriterRowKind::RowCount.to_wire(),
                        0,
                        Some(5),
                        None,
                        vec![None, None],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![None, Some(b"zero-b".to_vec())],
                    ),
                    (
                        WriterRowKind::CommitFragment.to_wire(),
                        0,
                        None,
                        Some(b"fragment-zero".to_vec()),
                        vec![None, None],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        1,
                        None,
                        None,
                        vec![None, Some(b"one-b".to_vec())],
                    ),
                ],
            ),
        )
        .expect("second interleaved batch");
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        assert!(
            matches!(processor.next(), NextStep::NeedInput | NextStep::Finished),
            "Gather has not reached EOS"
        );
        assert!(
            processor
                .drive()
                .map(|step| step.output)
                .expect("early pull")
                .is_none()
        );
        processor
            .signal_eos()
            .expect("all gather inputs reached EOS");

        let mut outputs = Vec::new();
        for chunk in TestDriver::new(operator.as_mut(), &state)
            .collect_until_finished()
            .expect("root output")
        {
            {
                outputs.push(chunk);
            }
        }
        assert!(
            outputs.len() >= 6,
            "summary and fragments stream separately before four bounded artifacts"
        );
        let mut summaries = 0;
        let mut prepared = Vec::new();
        let mut artifacts = Vec::new();
        for output in &outputs {
            let kinds = output.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("kind");
            for row in 0..output.len() {
                match RootRowKind::from_wire(kinds.value(row)).expect("root kind") {
                    RootRowKind::Summary => {
                        summaries += 1;
                        let counts = output.columns()[2]
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("row counts");
                        assert_eq!(counts.value(row), 12);
                    }
                    RootRowKind::PreparedFragment => {
                        let targets = output.columns()[1]
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("targets");
                        let fragments = output.columns()[3]
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .expect("fragments");
                        prepared.push((targets.value(row), fragments.value(row).to_vec()));
                    }
                    RootRowKind::ArtifactDraft => {
                        let targets = output.columns()[1]
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("targets");
                        let bodies = output.columns()[6]
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .expect("bodies");
                        let types = output.columns()[5]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("blob types");
                        let fields = output.columns()[4]
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .expect("input fields");
                        assert_eq!(types.value(row), "test/blob-v1");
                        let ids = fields.value(row);
                        let ids = ids.as_any().downcast_ref::<Int32Array>().expect("ids");
                        artifacts.push((
                            targets.value(row),
                            ids.value(0),
                            bodies.value(row).to_vec(),
                        ));
                    }
                }
            }
        }
        prepared.sort();
        artifacts.sort();
        assert_eq!(summaries, 1);
        assert_eq!(
            prepared,
            vec![
                (0, b"fragment-zero".to_vec()),
                (1, b"fragment-one".to_vec())
            ]
        );
        assert_eq!(
            artifacts,
            vec![
                (0, 1, b"zero-a".to_vec()),
                (0, 2, b"zero-b".to_vec()),
                (1, 3, b"one-a".to_vec()),
                (1, 4, b"one-b".to_vec()),
            ]
        );
    }

    #[test]
    fn composite_finish_streams_multiple_final_aggregate_batches_exactly_once() {
        let (factory, state, writer_schema) = composite_fixture(2, 1, 8);
        let final_batches = vec![
            scripted_final_chunk(&factory, vec![(0, vec![Some(b"zero".to_vec())])]),
            scripted_final_chunk(&factory, vec![(1, vec![Some(b"one".to_vec())])]),
        ];
        let mut operator = factory.create_operator(1, 0);
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator
            .kernel_for_test_mut()
            .install_aggregate(Box::new(Transform::hydrating(ScriptedFinalAggregate::new(
                final_batches,
                true,
                true,
            ))))
            .unwrap();
        complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial-zero".to_vec())],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        1,
                        None,
                        None,
                        vec![Some(b"partial-one".to_vec())],
                    ),
                ],
            ),
        )
        .expect("partial input");
        operator.finish_for_test(&state).expect("gather EOS");

        let mut artifacts = BTreeMap::new();
        let mut summaries = 0;
        let mut calls = TestDriver::new(&mut operator, &state);
        loop {
            let step = calls.advance_to_boundary().expect("root output");
            let finished = matches!(step.next, NextStep::Finished);
            let Some(output) = step.output else {
                if finished {
                    break;
                }
                continue;
            };
            let kinds = output.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("kind");
            let targets = output.columns()[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("target");
            let bodies = output.columns()[ROOT_WRITE_RESULT_BODY_INDEX]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("body");
            for row in 0..output.len() {
                match RootRowKind::from_wire(kinds.value(row)).expect("kind") {
                    RootRowKind::Summary => summaries += 1,
                    RootRowKind::PreparedFragment => panic!("unexpected fragment"),
                    RootRowKind::ArtifactDraft => {
                        assert!(
                            artifacts
                                .insert(targets.value(row), bodies.value(row).to_vec())
                                .is_none(),
                            "each target must be emitted exactly once"
                        );
                    }
                }
            }
            if finished {
                break;
            }
        }
        assert_eq!(summaries, 1);
        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[&0], b"zero");
        assert_eq!(artifacts[&1], b"one");
    }

    #[test]
    fn composite_finish_forwards_child_readiness_before_and_after_gather_eos() {
        use crate::exec::pipeline::{shape::StepOperator, step::InputResult};
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let final_batch =
            scripted_final_chunk(&factory, vec![(0, vec![Some(b"complete".to_vec())])]);
        let scripted = ScriptedFinalAggregate::new(vec![final_batch], false, false);
        let accepting = scripted.accepting.clone();
        let ready = scripted.ready.clone();
        let observable = scripted.observable.clone();
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "final-aggregate-blocked-metrics",
        ));
        operator.set_profiles(profiles.clone());
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        operator
            .kernel_for_test_mut()
            .install_aggregate(Box::new(Transform::hydrating(scripted)))
            .unwrap();
        let source_identity = operator.control_observable().unwrap();
        assert!(Arc::ptr_eq(
            &source_identity,
            &operator.control_observable().unwrap()
        ));
        with_step(&state, 64, |ctx| {
            let input = composite_rows(
                &writer_schema,
                vec![
                    (
                        WriterRowKind::RowCount.to_wire(),
                        0,
                        Some(7),
                        None,
                        vec![None],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    ),
                ],
            );
            assert!(matches!(
                operator.try_push(ctx, RawInput::for_test(input)),
                InputResult::Accepted(Ok(_))
            ));
            let result = operator.drive(ctx).unwrap();
            let NextStep::Wait(wait) = result.next else {
                panic!("the accepted parent retains a blocked child raw");
            };
            assert!(!wait.is_ready());
            assert!(Arc::ptr_eq(&wait.observable, &source_identity));
            let coverage = operator.kernel_for_test().aggregate_coverage.clone();
            assert_eq!(operator.kernel_for_test().rows.get(), 7);
            std::thread::sleep(Duration::from_millis(1));
            accepting.store(true, Ordering::Release);
            observable.notify_observers();
            assert!(wait.is_ready());
            for _ in 0..8 {
                let result = operator.drive(ctx).unwrap();
                assert!(result.output.is_none());
                if matches!(result.next, NextStep::NeedInput) {
                    break;
                }
            }
            assert_eq!(
                operator.kernel_for_test().aggregate_coverage,
                coverage,
                "resume cannot accept writer rows again"
            );
            assert_eq!(
                operator.kernel_for_test().rows.get(),
                7,
                "a child admission retry never re-applies the parent row count"
            );
            operator.finish(ctx).unwrap();
        });
        let mut summaries = 0;
        let mut parked = None;
        for _ in 0..12 {
            let result = with_step(&state, 64, |ctx| operator.drive(ctx)).unwrap();
            if let Some(output) = result.output {
                summaries += output.len();
            }
            if let NextStep::Wait(wait) = result.next {
                parked = Some(wait);
                break;
            }
        }
        let wait =
            parked.expect("finalization reports its exact event instead of pending physical exit");
        assert!(!wait.is_ready());
        assert!(Arc::ptr_eq(&source_identity, &wait.observable));
        assert!(
            operator.pending_finish().is_none(),
            "waiting computation remains normal dataflow"
        );
        ready.store(true, Ordering::Release);
        observable.notify_observers();
        assert!(wait.is_ready());
        let mut artifacts = 0;
        for _ in 0..32 {
            let result = with_step(&state, 64, |ctx| operator.drive(ctx)).unwrap();
            if let Some(output) = result.output {
                artifacts += output.len();
            }
            if matches!(result.next, NextStep::Finished) {
                break;
            }
        }
        assert_eq!(summaries, 1);
        assert_eq!(artifacts, 1);
        operator.close().unwrap();
        assert!(
            profiles
                .common
                .counter_value("FinalAggregateBlockedCount")
                .is_some_and(|count| count >= 1)
        );
        assert!(
            profiles
                .common
                .counter_value("FinalAggregateBlockedTime")
                .is_some_and(|elapsed| elapsed > 0)
        );
    }

    #[test]
    fn composite_finish_forwards_grouped_unpivot_child_readiness() {
        let (factory, _state, _writer_schema) = composite_fixture(1, 1, 8);
        let final_chunk =
            scripted_final_chunk(&factory, vec![(0, vec![Some(b"complete".to_vec())])]);
        let mut grouped = GroupedUnpivotFixtureKernel::try_new(
            Arc::clone(&factory.arena),
            factory
                .final_plan
                .unpivot
                .as_ref()
                .cloned()
                .expect("grouped Unpivot plan"),
            Arc::clone(
                factory
                    .root_schema
                    .prepared()
                    .expect("legacy fixture root schema")
                    .chunk_schema(),
            ),
            final_chunk,
            None,
            RuntimeKernelControl::new(_state.error_state()),
            None,
        )
        .expect("grouped Unpivot driver");
        let child = crate::exec::pipeline::shape::fanout::test_support::Probe::new(true);
        let child_observable = child.event.clone();
        grouped
            .install_active(crate::exec::pipeline::shape::fanout::test_support::sink(
                child,
            ))
            .unwrap();

        let mut operator = factory.create_operator(1, 0);
        use crate::exec::pipeline::shape::StepOperator;
        let source_identity = operator
            .control_observable()
            .expect("stable source observable while consuming");
        operator
            .kernel_for_test_mut()
            .install_grouped_unpivot(grouped, _state.error_state());
        operator.kernel_for_test_mut().phase = FinishPhase::Producing;
        let generation = source_identity.generation();

        child_observable.notify_observers();

        assert!(Arc::ptr_eq(
            &source_identity,
            &operator
                .control_observable()
                .expect("same source observable while producing")
        ));
        assert_eq!(source_identity.generation(), generation + 1);
    }

    #[test]
    fn composite_finish_counts_final_aggregate_work_wait_as_one_interval() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let final_batch =
            scripted_final_chunk(&factory, vec![(0, vec![Some(b"complete".to_vec())])]);
        let scripted = ScriptedFinalAggregate::new(vec![final_batch], true, false);
        let ready = Arc::clone(&scripted.ready);
        let observable = scripted.observable.clone();
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "driver-pending-finalize-metrics",
        ));
        operator.set_profiles(profiles.clone());
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator
            .kernel_for_test_mut()
            .install_aggregate(Box::new(Transform::hydrating(scripted)))
            .unwrap();
        complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    vec![Some(b"partial".to_vec())],
                )],
            ),
        )
        .expect("partial input");

        with_step(&state, 64, |ctx| operator.finish(ctx)).unwrap();
        let wait = await_final_wait(&mut operator, &state);
        for _ in 0..3 {
            assert!(matches!(
                with_step(&state, 64, |ctx| operator.drive(ctx))
                    .unwrap()
                    .next,
                NextStep::Wait(_)
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
        ready.store(true, Ordering::Release);
        observable.notify_observers();
        assert!(wait.is_ready());
        assert!(
            TestDriver::new(&mut operator, &state)
                .collect_until_finished()
                .is_ok()
        );
        operator.close().expect("close");
        assert_eq!(
            profiles.common.counter_value("FinalAggregateBlockedCount"),
            Some(1),
            "repeated work-wait observations belong to one continuous wait"
        );
        assert!(
            profiles
                .common
                .counter_value("FinalAggregateBlockedTime")
                .is_some_and(|elapsed| elapsed > 0),
            "the finalization work wait contributes wall time"
        );
    }

    #[test]
    fn composite_finish_closes_final_aggregate_work_wait_on_failure() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let scripted = ScriptedFinalAggregate::new(Vec::new(), true, false);
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "failed-driver-pending-finalize-metrics",
        ));
        operator.set_profiles(profiles.clone());
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator
            .kernel_for_test_mut()
            .install_aggregate(Box::new(Transform::hydrating(scripted)))
            .unwrap();
        complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    vec![Some(b"partial".to_vec())],
                )],
            ),
        )
        .expect("partial input");
        with_step(&state, 64, |ctx| operator.finish(ctx)).unwrap();
        let wait = await_final_wait(&mut operator, &state);
        assert!(!wait.is_ready());
        std::thread::sleep(Duration::from_millis(1));

        operator.on_driver_failure();
        let ended = operator.drive_for_test(&state).unwrap();
        assert!(ended.output.is_none());
        assert!(matches!(ended.next, NextStep::Finished));
        assert!(operator.pending_finish().is_none());
        let elapsed_after_failure = profiles
            .common
            .counter_value("FinalAggregateBlockedTime")
            .expect("failure must publish the closed interval");
        std::thread::sleep(Duration::from_millis(1));
        operator.close().expect("close after failure");
        assert_eq!(
            profiles.common.counter_value("FinalAggregateBlockedCount"),
            Some(1)
        );
        assert_eq!(
            profiles.common.counter_value("FinalAggregateBlockedTime"),
            Some(elapsed_after_failure),
            "failure must stop the timer rather than leave a live interval"
        );
    }

    #[test]
    fn composite_finish_surfaces_error_only_finalization_without_waiting_for_output() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let mut operator = factory.create_operator(1, 0);
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator
            .kernel_for_test_mut()
            .install_aggregate(Box::new(Transform::hydrating(ErrorOnlyFinalAggregate)))
            .unwrap();
        complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    vec![Some(b"partial".to_vec())],
                )],
            ),
        )
        .expect("partial input");
        let error = finish_failure(&mut operator, &state);
        assert!(error.detail().contains("failed asynchronously"), "{error}");
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error)
        );
    }

    #[test]
    fn composite_finish_fails_closed_when_final_aggregate_rejects_finalize() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let tracker = MemTracker::new_root("table-finish-finalize-error");
        let mut operator = factory.create_operator(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        install_composite_construction(&mut operator, &state, tracker.clone());
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator
            .kernel_for_test_mut()
            .install_aggregate(Box::new(Transform::hydrating(FinalizeErrorAggregate)))
            .unwrap();
        complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![
                    (
                        WriterRowKind::CommitFragment.to_wire(),
                        0,
                        None,
                        Some(vec![7; 4096]),
                        vec![None],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    ),
                ],
            ),
        )
        .expect("writer input");
        let error = finish_failure(&mut operator, &state);
        assert!(
            error.detail().contains("synchronous finalize failure"),
            "{error}"
        );
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error)
        );
        operator.begin_exit().unwrap();
        operator.close().unwrap();
        drop(operator);
        assert_eq!(tracker.current(), 0, "failed finalize releases all buffers");
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn final_aggregate_guard_rejects_merge_without_root_output() {
        let (mut factory, state, writer_schema) = composite_fixture(1, 1, 8);
        factory.aggregate_guard = Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::FinalMerge,
        ));
        let mut operator = factory.create_operator(1, 0);
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");

        let error = complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    vec![Some(b"partial".to_vec())],
                )],
            ),
        )
        .expect_err("final merge fault");

        assert!(error.detail().contains("FinalMerge"), "{error}");
        let expected = ConnectorError::new(ConnectorErrorKind::Internal, "rejected FinalMerge");
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&error),
            novarocks_failure_contract::FailureProjection::failure_fact(&expected),
            "the final merge keeps the original Connector category and reason"
        );
        assert!(
            matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Domain(source)
                if source.downcast_ref::<ConnectorError>() == Some(&expected)),
            "the original typed Connector source survives the final merge boundary"
        );
        assert!(
            matches!(operator.initial_step(), NextStep::Finished),
            "the Root operator must fail closed"
        );
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error),
            "a failed Root cannot emit a successful result row"
        );
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn final_aggregate_guard_rejects_finalize_without_root_output() {
        let (mut factory, state, writer_schema) = composite_fixture(1, 1, 8);
        factory.aggregate_guard = Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::FinalFinalize,
        ));
        let mut operator = factory.create_operator(1, 0);
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    vec![Some(b"partial".to_vec())],
                )],
            ),
        )
        .expect("partial input before finalize fault");

        let error = finish_failure(&mut operator, &state);

        assert!(error.detail().contains("FinalFinalize"), "{error}");
        assert!(
            matches!(operator.initial_step(), NextStep::Finished),
            "the Root operator must fail closed"
        );
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error),
            "a failed Root cannot emit a successful result row"
        );
    }

    #[test]
    fn composite_finish_streams_more_than_one_thousand_channels_and_releases_memory() {
        let channels = 1_001usize;
        let (factory, state, writer_schema) = composite_fixture(1, channels, 64);
        let tracker = MemTracker::new_root("table-finish-wide-unpivot");
        let mut operator = factory.create_operator(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        install_composite_construction(&mut operator, &state, tracker.clone());
        let creator_bytes = tracker.current();
        assert!(
            creator_bytes > 0,
            "the original construction owner is tracked"
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        complete_input(
            &mut operator,
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    (0..channels)
                        .map(|index| Some(vec![u8::try_from(index % 251).expect("byte")]))
                        .collect(),
                )],
            ),
        )
        .expect("wide partial input");
        operator.finish_for_test(&state).expect("gather EOS");

        let mut outputs = Vec::new();
        for output in TestDriver::new(&mut operator, &state)
            .collect_until_finished()
            .expect("root output")
        {
            {
                outputs.push(output);
            }
        }
        let mut artifact_rows = 0usize;
        let mut artifact_batches = 0usize;
        for output in &outputs {
            let kinds = output.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("kind");
            let in_batch = (0..output.len())
                .filter(|row| kinds.value(*row) == RootRowKind::ArtifactDraft.to_wire())
                .count();
            artifact_rows += in_batch;
            artifact_batches += usize::from(in_batch > 0);
        }
        assert_eq!(artifact_rows, channels);
        assert!(
            artifact_batches > 1,
            "wide Unpivot must emit multiple batches"
        );
        drop(outputs);
        operator.begin_exit().unwrap();
        operator.close().unwrap();
        assert_eq!(
            tracker.current(),
            creator_bytes,
            "only the live operator's original creator metadata remains"
        );
        drop(operator);
        assert_eq!(
            tracker.current(),
            0,
            "all final/Unpivot/output memory released"
        );
        assert!(tracker.peak() > 0);
    }

    #[test]
    fn composite_finish_fails_closed_on_missing_channel_coverage_at_eos() {
        let (factory, state, writer_schema) = composite_fixture(1, 2, 16);
        let mut operator = factory.create(1, 0);
        install_composite_construction(
            operator.as_mut(),
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind aggregate");
        complete_input(
            operator.as_mut(),
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    vec![Some(b"present".to_vec()), None],
                )],
            ),
        )
        .expect("one sparse partial");
        let error = finish_failure(operator.as_mut(), &state);
        assert!(
            error
                .detail()
                .contains("no non-null aggregate partial for channel 1")
        );
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error)
        );
    }

    #[test]
    fn composite_finish_rejects_all_null_partial_and_schema_drift_at_nearest_ingress() {
        let (factory, state, writer_schema) = composite_fixture(1, 2, 16);
        let mut all_null = factory.create(1, 0);
        install_composite_construction(
            all_null.as_mut(),
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        all_null.prepare().expect("prepare");
        all_null.bind_runtime_state(&state).expect("bind aggregate");
        let error = complete_input(
            all_null.as_mut(),
            &state,
            composite_rows(
                &writer_schema,
                vec![(
                    WriterRowKind::AggregatePartial.to_wire(),
                    0,
                    None,
                    None,
                    vec![None, None],
                )],
            ),
        )
        .expect_err("an all-null partial carries no contribution");
        assert!(
            error
                .detail()
                .contains("payload does not match its row kind")
        );

        // An independent invalid invocation has its own Task failure scope.
        let (factory, state, _) = composite_fixture(1, 2, 16);
        let mut drifted = factory.create(1, 0);
        install_composite_construction(
            drifted.as_mut(),
            &state,
            MemTracker::new_root("composite finish construction"),
        );
        drifted.prepare().expect("prepare");
        drifted.bind_runtime_state(&state).expect("bind aggregate");
        let error = complete_input(
            drifted.as_mut(),
            &state,
            writer_rows(vec![row_count_row(0, 1)]),
        )
        .expect_err("the four-column legacy schema is not the frozen multiplex schema");
        assert!(
            error
                .detail()
                .contains("schema drifted from the frozen plan")
        );
    }

    #[test]
    fn composite_finish_cancel_releases_fragment_final_and_output_memory() {
        let (factory, base_state, writer_schema) = composite_fixture(1, 1, 1);
        let tracker = MemTracker::new_root("table-finish-test");
        let state = composite_runtime_state(
            Arc::clone(
                base_state
                    .execution_runtime()
                    .expect("runtime")
                    .function_set(),
            ),
            Some(Arc::clone(&tracker)),
        );
        let mut operator = factory.create(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        install_composite_construction(operator.as_mut(), &state, tracker.clone());
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind aggregate");
        complete_input(
            operator.as_mut(),
            &state,
            composite_rows(
                &writer_schema,
                vec![
                    (
                        WriterRowKind::CommitFragment.to_wire(),
                        0,
                        None,
                        Some(vec![7; 4096]),
                        vec![None],
                    ),
                    (
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(vec![8; 4096])],
                    ),
                ],
            ),
        )
        .expect("buffer input");
        assert!(tracker.current() > 0);
        operator.finish_for_test(&state).expect("finalize");
        operator.cancel();
        drop(operator);
        assert_eq!(tracker.current(), 0, "cancel releases every retained owner");
    }
    #[test]
    fn composite_cleanup_errors_keep_actual_grouped_exit_and_original_failure_binding() {
        use crate::exec::pipeline::operator::{
            ExitObservation, ExitProgress, FinishWatch, InputEncodingPolicy,
        };
        struct CleanupAggregate {
            failure: ExecutionFailure,
            begins: Arc<AtomicUsize>,
            closes: Arc<AtomicUsize>,
        }
        impl OperatorLifecycle for CleanupAggregate {
            fn name(&self) -> &str {
                "ACTUAL_TABLE_FINISH_CLEANUP"
            }
            fn begin_exit(&mut self) -> ExecutionResult<()> {
                self.begins.fetch_add(1, Ordering::SeqCst);
                Err(self.failure.clone())
            }
            fn close(&mut self) -> ExecutionResult<()> {
                self.closes.fetch_add(1, Ordering::SeqCst);
                Err(self.failure.clone())
            }
        }
        impl InputEncodingPolicy for CleanupAggregate {}
        impl crate::exec::pipeline::shape::TransformKernel for CleanupAggregate {
            fn apply(
                &mut self,
                _: &mut StepContext<'_>,
                _: crate::exec::pipeline::input::PreparedInput,
            ) -> ExecutionResult<StepResult> {
                unreachable!("lifecycle test has no aggregate data work")
            }
        }
        struct Resource {
            data: Vec<u64>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Resource {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Release(Arc<tokio::sync::Semaphore>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.add_permits(1);
            }
        }
        struct Wake(Arc<Observable>);
        impl futures::task::ArcWake for Wake {
            fn wake_by_ref(this: &Arc<Self>) {
                this.0.notify_observers();
            }
        }
        struct GatedGrouped {
            join: Option<tokio::task::JoinHandle<()>>,
            event: Arc<Observable>,
            joined: Arc<AtomicBool>,
            begins: Arc<AtomicUsize>,
            closes: Arc<AtomicUsize>,
        }
        impl OperatorLifecycle for GatedGrouped {
            fn name(&self) -> &str {
                "ACTUAL_GATED_GROUPED_EXIT"
            }
            fn begin_exit(&mut self) -> ExecutionResult<()> {
                self.begins.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn close(&mut self) -> ExecutionResult<()> {
                assert!(
                    self.joined.load(Ordering::Acquire),
                    "real grouped task must join before close"
                );
                self.closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn observe_exit(&mut self) -> ExitObservation {
                use std::{
                    future::Future,
                    task::{Context, Poll},
                };
                let Some(join) = self.join.as_mut() else {
                    return ExitObservation::exited();
                };
                let wake = Arc::new(Wake(self.event.clone()));
                let waker = futures::task::waker_ref(&wake);
                let mut cx = Context::from_waker(&waker);
                match std::pin::Pin::new(join).poll(&mut cx) {
                    Poll::Pending => {
                        ExitObservation::pending(FinishWatch::Notify(self.event.clone()))
                    }
                    Poll::Ready(result) => {
                        result.expect("actual grouped task exit");
                        self.join = None;
                        self.joined.store(true, Ordering::Release);
                        ExitObservation::exited()
                    }
                }
            }
        }
        impl SourceKernel for GatedGrouped {
            fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
                Ok(StepResult::new(None, NextStep::Finished, 0))
            }
        }
        impl GroupedUnpivotKernel for GatedGrouped {
            fn visit_targets(
                &self,
                _: &mut dyn FnMut(u32) -> ExecutionResult<()>,
            ) -> ExecutionResult<()> {
                Ok(())
            }
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let (factory, state, _) = composite_fixture(1, 1, 8);
        let failures = state.error_state();
        let mut operator = factory.create_operator(1, 0);
        install_composite_construction(
            &mut operator,
            &state,
            MemTracker::new_root("actual table finish exit construction"),
        );
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        let begin = Arc::new(AtomicUsize::new(0));
        let close = Arc::new(AtomicUsize::new(0));
        let cleanup = finish_contract(FinishContractSite::Binding, "actual child cleanup error");
        operator
            .kernel_for_test_mut()
            .install_aggregate(Box::new(Transform::hydrating(CleanupAggregate {
                failure: cleanup,
                begins: begin.clone(),
                closes: close.clone(),
            })))
            .unwrap();
        let gate = Release(Arc::new(tokio::sync::Semaphore::new(0)));
        let task_gate = gate.0.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(Resource {
            data: vec![5, 8, 13],
            drops: drops.clone(),
        });
        let weak = Arc::downgrade(&resource);
        let event = Arc::new(Observable::new());
        let joined = Arc::new(AtomicBool::new(false));
        let grouped_begins = Arc::new(AtomicUsize::new(0));
        let grouped_closes = Arc::new(AtomicUsize::new(0));
        let grouped = GatedGrouped {
            join: Some(runtime.spawn(async move {
                let original = resource;
                task_gate.acquire().await.unwrap().forget();
                assert_eq!(original.data, [5, 8, 13]);
            })),
            event: event.clone(),
            joined: joined.clone(),
            begins: grouped_begins.clone(),
            closes: grouped_closes.clone(),
        };
        operator
            .kernel_for_test_mut()
            .install_grouped_unpivot(grouped, failures.clone());
        operator.begin_exit().unwrap();
        assert_eq!(begin.load(Ordering::SeqCst), 1);
        assert_eq!(
            grouped_begins.load(Ordering::SeqCst),
            1,
            "aggregate cleanup error cannot skip grouped sibling"
        );
        let mut observations = Vec::new();
        operator.visit_exits(&mut |observation| observations.push(observation));
        assert!(
            observations
                .iter()
                .any(|observation| matches!(observation.progress, ExitProgress::Pending(_)))
        );
        assert!(
            observations
                .iter()
                .all(|observation| observation.failure.is_none())
        );
        assert!(failures.error().is_none());
        assert_eq!(weak.upgrade().unwrap().data, [5, 8, 13]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(!joined.load(Ordering::Acquire));
        gate.0.add_permits(1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut pending = false;
            operator.visit_exits(&mut |observation| {
                assert!(observation.failure.is_none());
                pending |= !matches!(observation.progress, ExitProgress::Exited);
            });
            if !pending {
                break;
            }
            assert!(Instant::now() < deadline, "actual grouped join must exit");
            std::thread::yield_now();
        }
        assert!(joined.load(Ordering::Acquire));
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        operator.close().unwrap();
        operator.close().unwrap();
        operator.begin_exit().unwrap();
        assert_eq!(begin.load(Ordering::SeqCst), 1);
        assert_eq!(close.load(Ordering::SeqCst), 1);
        assert_eq!(grouped_begins.load(Ordering::SeqCst), 1);
        assert_eq!(grouped_closes.load(Ordering::SeqCst), 1);
        assert!(
            failures.error().is_none(),
            "cleanup never enters original Runtime originating error"
        );
    }
}
