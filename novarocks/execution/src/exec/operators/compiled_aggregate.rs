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

//! Compiled hash aggregation.
//!
//! One driver owns one processor: an instance per group and argument root, one
//! group-key table and one owned state column per call. Every arriving row's
//! group keys and arguments are evaluated once by the driver's compiled roots;
//! a row data error of any root is a required error. Each call runs through
//! the exact prepared aggregate handle the compiler attached to its site, so
//! no implementation is selected by name and no legacy expression is read.
//!
//! Group-key equivalence has one owner, the existing `KeyTable`: NULL keys
//! form one group, every NaN of a float key is one group, and `-0.0` and
//! `+0.0` are one group. The table's first stored key represents its group.
//!
//! Grouping is full hash aggregation that emits every group once at finish,
//! in group creation order, into exactly the node's frozen output layout.
//! A group-less aggregate owns one group from its first input, or from
//! finishing on empty input, so each driver emits exactly one row. The
//! builder places a Complete aggregate on one driver; a Partial aggregate
//! runs per driver and leaves merging to its later phase.
//!
//! State backing comes from a host allocator charged to the operator's memory
//! tracker. Owner-reported retained heap inside a state is not reconciled yet;
//! the formal memory grant remains an open host obligation.

use std::alloc::Layout;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::DataType;
use novarocks_functions::{
    AggregateCallContract, AggregateStateAllocator, AggregateStateColumn, EvaluatedArgument,
    KernelDiagnostic, KernelFailure, PreparedAggregateHandle, SelectedAggregateMergeInput,
    SelectedAggregateUpdateInput, Selection,
};
use novarocks_local_program::{
    CompiledAggregateGrouping, LocalProgram, ProgramCallSite, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate,
};

use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::hash_table::key_builder::build_group_key_views;
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::exec::hash_table::key_table::KeyTable;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// Group keys use the table's specialized layouts where they apply.
const ENABLE_GROUP_KEY_OPTIMIZATIONS: bool = true;
/// Rows per emitted chunk; the whole result is materialized at finish.
const OUTPUT_CHUNK_ROWS: usize = 4096;
/// Target bytes of one state backing block for a grouped aggregate.
const STATE_BLOCK_BYTES: usize = 64 * 1024;
const MAX_STATES_PER_BLOCK: usize = 4096;

fn failure(context: &str, error: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from(format!("{context}: {error}"))
}

/// State backing charged to the operator's memory tracker. A block is charged
/// before it is allocated and released after it is returned, so the tracker
/// never undercounts live backing.
struct TrackedStateAllocator {
    tracker: Arc<MemTracker>,
}

impl AggregateStateAllocator for TrackedStateAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        if layout.size() == 0 {
            return Err(KernelFailure::Internal(KernelDiagnostic::new(
                "aggregate state block has no size",
            )));
        }
        let bytes = i64::try_from(layout.size()).map_err(|_| KernelFailure::ResourceExhausted)?;
        if self.tracker.consume_and_check_limit(bytes).is_err() {
            self.tracker.release(bytes);
            return Err(KernelFailure::ResourceExhausted);
        }
        // SAFETY: the layout has a nonzero size.
        match NonNull::new(unsafe { std::alloc::alloc(layout) }) {
            Some(block) => Ok(block),
            None => {
                self.tracker.release(bytes);
                Err(KernelFailure::ResourceExhausted)
            }
        }
    }

    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the column returns a block this allocator produced with
        // exactly this layout, after destroying every state inside it.
        unsafe { std::alloc::dealloc(block.as_ptr(), layout) };
        self.tracker
            .release(i64::try_from(layout.size()).unwrap_or(i64::MAX));
    }
}

/// One call's exact prepared handle and the roots that feed it: the logical
/// arguments of an update phase, or the one state input of a merge phase.
#[derive(Clone)]
struct CallPlan {
    handle: PreparedAggregateHandle,
    /// The handle's own contract allocation; a frame input borrows it while
    /// the call's state column is mutably borrowed.
    contract: Arc<AggregateCallContract>,
    merge: bool,
    /// Offset of this call's roots in the processor's root list.
    first_root: usize,
    roots: usize,
}

pub struct CompiledAggregateProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    groups: usize,
    calls: Vec<CallPlan>,
    key_types: Vec<DataType>,
    output: ChunkSchemaRef,
    grouping: CompiledAggregateGrouping,
    error: Arc<RuntimeErrorState>,
}

impl CompiledAggregateProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let at = node.index();
        let graph_node = program
            .graph()
            .nodes()
            .get(at)
            .ok_or_else(|| format!("compiled Aggregate node {at} is absent"))?;
        let ProgramNodeKind::Aggregate {
            group_by,
            functions,
            topn_filters,
            streaming_preaggregation_mode,
            ..
        } = graph_node.kind()
        else {
            return Err(format!("compiled local node {at} is not an Aggregate"));
        };
        if !topn_filters.is_empty() {
            return Err(format!(
                "compiled Aggregate at local node {at} with TopN runtime filters is not executable"
            ));
        }
        if streaming_preaggregation_mode.is_some() {
            return Err(format!(
                "compiled Aggregate at local node {at} with streaming pre-aggregation is not executable"
            ));
        }
        let grouping = program
            .aggregates()
            .get(&node)
            .ok_or_else(|| format!("compiled Aggregate at local node {at} has no grouping fact"))?
            .grouping;
        let mut sites = Vec::new();
        for group in 0..group_by.len() {
            let group = u32::try_from(group)
                .map_err(|_| format!("compiled Aggregate at local node {at} has too many keys"))?;
            sites.push(ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::AggregateGroup { group },
            });
        }
        let mut calls = Vec::with_capacity(functions.len());
        for (ordinal, function) in functions.iter().enumerate() {
            let call = u32::try_from(ordinal)
                .map_err(|_| format!("compiled Aggregate at local node {at} has too many calls"))?;
            let Some(ProgramStateTemplate::Aggregate { kernel, .. }) =
                program.state_template(ProgramCallSite::Aggregate { node, call })
            else {
                return Err(format!(
                    "compiled Aggregate call {call} at local node {at} has no prepared aggregate"
                ));
            };
            let handle = kernel.clone();
            let contract = Arc::clone(handle.contract());
            if !contract.order_keys().is_empty() {
                return Err(format!(
                    "compiled Aggregate call {call} at local node {at} with function ORDER BY is not executable"
                ));
            }
            let merge = !contract.phase().consumes_logical_arguments();
            let roots = if merge {
                1
            } else {
                contract.logical_argument_types().len()
            };
            if function.inputs.len() != roots {
                return Err(format!(
                    "compiled Aggregate call {call} at local node {at} has {} roots for {roots} channels",
                    function.inputs.len()
                ));
            }
            let first_root = sites.len();
            for argument in 0..roots {
                let argument = u32::try_from(argument).map_err(|_| {
                    format!("compiled Aggregate call {call} at local node {at} has too many inputs")
                })?;
                sites.push(ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::AggregateInput { call, argument },
                });
            }
            calls.push(CallPlan {
                handle,
                contract,
                merge,
                first_root,
                roots,
            });
        }
        let output = ChunkSchema::from_compiled_layout(graph_node.output_layout())?;
        let fields = graph_node.output_layout().schema().fields();
        if fields.len() != group_by.len() + calls.len() {
            return Err(format!(
                "compiled Aggregate at local node {at} output is not its keys followed by its calls"
            ));
        }
        let key_types = fields[..group_by.len()]
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let groups = group_by.len();
        Ok(Self {
            name: format!("COMPILED_AGGREGATE (node={at})"),
            program,
            sites,
            groups,
            calls,
            key_types,
            output,
            grouping,
            error,
        })
    }

    /// Whether every group of this aggregate must be owned by one driver.
    pub(crate) fn completes_groups(&self) -> bool {
        self.grouping == CompiledAggregateGrouping::Complete
    }
}

impl OperatorFactory for CompiledAggregateProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledAggregateProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            group_roots: self.groups,
            calls: self.calls.clone(),
            key_types: self.key_types.clone(),
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            instances: None,
            key_table: None,
            states: None,
            groups: 0,
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

struct CompiledAggregateProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    group_roots: usize,
    calls: Vec<CallPlan>,
    key_types: Vec<DataType>,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    instances: Option<Vec<CompiledExpressionInstance>>,
    /// Present exactly when the aggregate has group keys.
    key_table: Option<KeyTable>,
    /// One column per call, created on first use.
    states: Option<Vec<AggregateStateColumn>>,
    /// Groups created so far; every state column holds exactly this many.
    groups: usize,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

impl CompiledAggregateProcessor {
    fn tracker(&self) -> ExecutionResult<Arc<MemTracker>> {
        if let Some(tracker) = self.tracker.as_ref() {
            return Ok(Arc::clone(tracker));
        }
        #[cfg(test)]
        {
            Ok(MemTracker::new_child(
                "CompiledAggregateTest",
                &crate::runtime::mem_tracker::process_mem_tracker(),
            ))
        }
        #[cfg(not(test))]
        {
            Err(ExecutionFailure::from(
                "compiled aggregate memory tracker must be bound before its first state",
            ))
        }
    }

    /// Create the key table and the state columns on first use.
    fn ensure_state(&mut self) -> ExecutionResult<()> {
        if self.states.is_some() {
            return Ok(());
        }
        let tracker = self.tracker()?;
        if self.group_roots > 0 {
            self.key_table = Some(
                KeyTable::new_with_tracker(
                    self.key_types.clone(),
                    ENABLE_GROUP_KEY_OPTIMIZATIONS,
                    MemTracker::new_child("CompiledAggregateKeys", &tracker),
                )
                .map_err(|error| failure("compiled aggregate key table", error))?,
            );
        }
        let allocator: Arc<dyn AggregateStateAllocator> = Arc::new(TrackedStateAllocator {
            tracker: MemTracker::new_child("CompiledAggregateStates", &tracker),
        });
        let mut states = Vec::with_capacity(self.calls.len());
        for call in &self.calls {
            let stride = call.handle.state_layout().pad_to_align().size().max(1);
            // A group-less aggregate owns exactly one state per call.
            let per_block = if self.group_roots == 0 {
                1
            } else {
                (STATE_BLOCK_BYTES / stride).clamp(1, MAX_STATES_PER_BLOCK)
            };
            states.push(AggregateStateColumn::try_new(
                call.handle.clone(),
                Arc::clone(&allocator),
                NonZeroUsize::new(per_block).expect("clamped to at least one state"),
            )?);
        }
        self.states = Some(states);
        Ok(())
    }

    /// Initialize the state of every group created since the last batch.
    fn grow_states(&mut self) -> ExecutionResult<()> {
        let states = self.states.as_mut().expect("states were created");
        for column in states.iter_mut() {
            while column.len() < self.groups {
                column.push(&self.control)?;
            }
        }
        Ok(())
    }

    /// Assign every row its group, creating groups in first-arrival order.
    fn assign_groups(&mut self, keys: &[ArrayRef], rows: usize) -> ExecutionResult<Vec<usize>> {
        if self.group_roots == 0 {
            // The one group of a group-less aggregate exists from its first row.
            self.groups = 1;
            return Ok(vec![0; rows]);
        }
        for (ordinal, (key, expected)) in keys.iter().zip(&self.key_types).enumerate() {
            if !novarocks_type_contract::arrow_data_types_exact(key.data_type(), expected) {
                return Err(ExecutionFailure::from(format!(
                    "compiled aggregate group key {ordinal} is {:?}, frozen {expected:?}",
                    key.data_type()
                )));
            }
        }
        let table = self
            .key_table
            .as_mut()
            .expect("grouped aggregate has a key table");
        let views = build_group_key_views(keys)
            .map_err(|error| failure("compiled aggregate group keys", error))?;
        if table.key_strategy() == GroupKeyStrategy::CompressedFixed
            && table.compressed_ctx().is_none()
        {
            table
                .ensure_compressed_ctx(&views)
                .map_err(|error| failure("compiled aggregate compressed keys", error))?;
        }
        let mut group_ids = Vec::with_capacity(rows);
        let mut next = self.groups;
        let mut record = |lookup: crate::exec::hash_table::key_table::KeyLookup| {
            if lookup.is_new {
                if lookup.group_id != next {
                    return Err(ExecutionFailure::from(
                        "compiled aggregate key table created a group out of order",
                    ));
                }
                next += 1;
            }
            group_ids.push(lookup.group_id);
            Ok(())
        };
        let key_error = |error: String| failure("compiled aggregate group lookup", error);
        match table.key_strategy() {
            GroupKeyStrategy::Serialized => {
                let rows_bytes = table.build_rows_fallback(keys).map_err(key_error)?;
                let hashes = table.build_group_hashes(&views, rows).map_err(key_error)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(rows) {
                    let bytes = rows_bytes
                        .get(row)
                        .map(|bytes| bytes.as_slice())
                        .ok_or_else(|| ExecutionFailure::from("serialized group row missing"))?;
                    record(
                        table
                            .find_or_insert_from_row(&views, row, bytes, hash)
                            .map_err(key_error)?,
                    )?;
                }
            }
            GroupKeyStrategy::Scalar => {
                return Err(ExecutionFailure::from(
                    "group key strategy Scalar is invalid for group keys",
                ));
            }
            GroupKeyStrategy::OneNumber => {
                let view = views
                    .first()
                    .ok_or_else(|| ExecutionFailure::from("one number key view missing"))?;
                let hashes = table
                    .build_one_number_hashes(view, rows)
                    .map_err(key_error)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(rows) {
                    record(
                        table
                            .find_or_insert_one_number(view, row, hash)
                            .map_err(key_error)?,
                    )?;
                }
            }
            GroupKeyStrategy::OneString => {
                let view = views
                    .first()
                    .ok_or_else(|| ExecutionFailure::from("one string key view missing"))?;
                let hashes = table.build_group_hashes(&views, rows).map_err(key_error)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(rows) {
                    record(
                        table
                            .find_or_insert_one_string_like(view, row, hash)
                            .map_err(key_error)?,
                    )?;
                }
            }
            GroupKeyStrategy::FixedSize => {
                let hashes = table.build_group_hashes(&views, rows).map_err(key_error)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(rows) {
                    record(
                        table
                            .find_or_insert_fixed_size(&views, row, hash)
                            .map_err(key_error)?,
                    )?;
                }
            }
            GroupKeyStrategy::CompressedFixed => {
                let compressed = table
                    .build_compressed_flags(&views, rows)
                    .map_err(key_error)?;
                let hashes = table.build_group_hashes(&views, rows).map_err(key_error)?;
                let mut fallback = None;
                for (row, (compressed, hash)) in compressed
                    .iter()
                    .copied()
                    .zip(hashes.iter().copied())
                    .enumerate()
                    .take(rows)
                {
                    let lookup = if compressed {
                        table
                            .find_or_insert_compressed(&views, row, hash)
                            .map_err(key_error)?
                    } else {
                        if fallback.is_none() {
                            fallback = Some(table.build_rows_fallback(keys).map_err(key_error)?);
                        }
                        let bytes = fallback
                            .as_ref()
                            .and_then(|rows| rows.get(row))
                            .map(|bytes| bytes.as_slice())
                            .ok_or_else(|| {
                                ExecutionFailure::from("compressed fallback group row missing")
                            })?;
                        table
                            .find_or_insert_from_row(&views, row, bytes, hash)
                            .map_err(key_error)?
                    };
                    record(lookup)?;
                }
            }
        }
        if group_ids.len() != rows {
            return Err(ExecutionFailure::from(
                "compiled aggregate group id count differs from its rows",
            ));
        }
        self.groups = next;
        Ok(group_ids)
    }

    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        self.ensure_state()?;
        // Keys first, then each call's roots in call order; each root is
        // evaluated once over every arriving row.
        let instances = self.instances.as_mut().expect("instances were created");
        let mut values = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            values.push(evaluate_all(instance, *site, &chunk.batch, &self.control)?);
        }
        let rows = chunk.len();
        let mapping = self.assign_groups(&values[..self.group_roots], rows)?;
        self.grow_states()?;
        let selection = Selection::all(rows);
        let states = self.states.as_mut().expect("states were created");
        for (call, column) in self.calls.iter().zip(states.iter_mut()) {
            let arguments = values[call.first_root..call.first_root + call.roots]
                .iter()
                .map(EvaluatedArgument::Column)
                .collect::<Vec<_>>();
            let mut frame = if call.merge {
                let input = SelectedAggregateMergeInput::try_new(
                    call.contract.as_ref(),
                    selection,
                    arguments[0],
                    &self.control,
                )?;
                column.prepare_merge_batch(&mapping, input, &self.control)?
            } else {
                let input = SelectedAggregateUpdateInput::try_new(
                    call.contract.as_ref(),
                    selection,
                    &arguments,
                    &[],
                    &self.control,
                )?;
                column.prepare_update_batch(&mapping, input, &self.control)?
            };
            frame.run(&self.control)?;
        }
        Ok(())
    }

    /// Every group's keys and call emissions, in group creation order.
    fn emit(&mut self) -> ExecutionResult<()> {
        self.ensure_state()?;
        if self.group_roots == 0 && self.groups == 0 {
            // A group-less aggregate emits its initial state on empty input.
            self.groups = 1;
            self.grow_states()?;
        }
        let groups = self.groups;
        if groups == 0 {
            return Ok(());
        }
        let fields = self.output.arrow_schema_ref();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.fields().len());
        if let Some(table) = self.key_table.as_ref() {
            for (ordinal, key) in table.key_columns().iter().enumerate() {
                let array = key
                    .to_array()
                    .map_err(|error| failure("compiled aggregate key output", error))?;
                let expected = fields.field(ordinal).data_type();
                if !novarocks_type_contract::arrow_data_types_exact(array.data_type(), expected) {
                    return Err(ExecutionFailure::from(format!(
                        "compiled aggregate key output {ordinal} is {:?}, frozen {expected:?}",
                        array.data_type()
                    )));
                }
                if array.len() != groups {
                    return Err(ExecutionFailure::from(
                        "compiled aggregate key output differs from its group count",
                    ));
                }
                columns.push(array);
            }
        }
        let indices = (0..groups).collect::<Vec<_>>();
        let states = self.states.as_ref().expect("states were created");
        for column in states {
            columns.push(column.emit(&indices, groups, &self.control)?);
        }
        let batch = RecordBatch::try_new(fields, columns)
            .map_err(|error| failure("compiled aggregate output", error))?;
        let mut offset = 0;
        while offset < groups {
            let len = OUTPUT_CHUNK_ROWS.min(groups - offset);
            self.pending.push_back(Chunk::try_new_with_chunk_schema(
                batch.slice(offset, len),
                Arc::clone(&self.output),
            )?);
            offset += len;
        }
        Ok(())
    }
}

impl Operator for CompiledAggregateProcessor {
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.tracker = Some(tracker);
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

impl ProcessorOperator for CompiledAggregateProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled Aggregate received input after finishing".into());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        self.consume(&chunk)
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.pop_front();
        if self.finishing && self.pending.is_empty() {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.finishing = true;
        self.emit()?;
        // The states and keys are released once their output is built.
        self.states = None;
        self.key_table = None;
        self.instances = None;
        if self.pending.is_empty() {
            self.finished = true;
        }
        Ok(())
    }
}
