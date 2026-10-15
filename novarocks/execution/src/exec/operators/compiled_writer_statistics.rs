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

//! Compiled writer statistics (collect-on-write aggregates).
//!
//! * A writer's partial processor owns one state per prepared partial call
//!   for its one driver: it updates each state from the call's projected
//!   column of every page and emits exactly one intermediate row at finish,
//!   even on empty input. The writer packs that row into sparse
//!   `AGGREGATE_PARTIAL` rows with its own packer, guard and limits.
//! * A finish's final aggregate merges every non-null auxiliary channel of
//!   each `AGGREGATE_PARTIAL` row into one state per call of that row's
//!   target. It groups strictly by target, never by channel, and emits every
//!   target group once at finish in ascending target order.
//! * A finish's grouped Unpivot expands each target's final values through
//!   that target's own mappings with the shared compiled Unpivot core.
//!   Scalar constants are the program's `FinishUnpivotConstant` roots,
//!   evaluated once per final batch over one row of the Root port.
//!
//! Every call runs through the exact prepared aggregate handle its compiler
//! attached at its writer site: no implementation is selected by name, no
//! expression arena is read, and the process function set is never
//! consulted. Coverage, completeness and channel-mapping checks stay with the
//! finish operator, identical for every statistics owner.

use std::alloc::Layout;
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array, Int32Array, new_null_array};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    AggregateCallContract, AggregateKernelPhase, AggregateStateAllocator, AggregateStateColumn,
    EvaluatedArgument, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    PreparedAggregateHandle, SelectedAggregateMergeInput, SelectedAggregateUpdateInput, Selection,
};
use novarocks_local_program::{
    LocalProgram, ProgramCallSite, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind, ProgramStateTemplate, StaticLayout, UnpivotConstant,
    WriterGroupedUnpivotPlan,
};
use novarocks_spi::connector::write_stack::{
    RootRowKind, target_ordinal_from_wire, target_ordinal_to_wire,
};
use novarocks_types::SlotId;

use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use super::compiled_unpivot::{
    LiteralSource, Producer, UnpivotExpansion, list_item_field, map_entries_field,
};
use super::table_finish::{
    FinishStatisticsCoverage, FinishStatisticsFactory, GroupedUnpivotSource,
    root_artifact_chunk_with_construction,
};
use crate::exec::chunk::{
    ArrayNormalizationStep, Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema,
};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner, Operator,
    ProcessorOperator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use novarocks_type_contract::ValueTypeStorageLifetime;

/// Target bytes of one state backing block of the finish's target groups.
const STATE_BLOCK_BYTES: usize = 64 * 1024;
const MAX_STATES_PER_BLOCK: usize = 4096;

/// State backing charged to the operator's memory tracker before it is
/// allocated and released after it is returned.
struct TrackedStateAllocator {
    tracker: Arc<MemTracker>,
}

impl novarocks_functions::opaque_memory::OpaqueAllocationHost for TrackedStateAllocator {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = i64::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        if self.tracker.consume_and_check_limit(bytes).is_err() {
            self.tracker.release(bytes);
            return Err(KernelFailure::ResourceExhausted);
        }
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        if bytes != 0 {
            // Successful reservation was represented exactly as i64.
            self.tracker
                .release(i64::try_from(bytes).expect("successful opaque reservation size"));
        }
    }
}
impl AggregateStateAllocator for TrackedStateAllocator {
    fn opaque_allocation_host(
        &self,
    ) -> Option<&dyn novarocks_functions::opaque_memory::OpaqueAllocationHost> {
        Some(self)
    }

    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        if layout.size() == 0 {
            return Err(KernelFailure::Internal(KernelDiagnostic::new(
                "writer aggregate state block has no size",
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

/// The operator's bound memory tracker. A unit test may drive an operator
/// without a pipeline; it charges a child of the process tracker.
fn bound_tracker(
    tracker: Option<&Arc<MemTracker>>,
    label: &str,
) -> ExecutionResult<Arc<MemTracker>> {
    if let Some(tracker) = tracker {
        return Ok(Arc::clone(tracker));
    }
    #[cfg(test)]
    {
        Ok(MemTracker::new_child(
            label,
            &crate::runtime::mem_tracker::process_mem_tracker(),
        ))
    }
    #[cfg(not(test))]
    {
        Err(ExecutionFailure::from(format!(
            "{label} memory tracker must be bound before its first state"
        )))
    }
}

/// One prepared writer call: its exact handle and the slot it reads.
#[derive(Clone)]
struct WriterCall {
    handle: PreparedAggregateHandle,
    contract: Arc<AggregateCallContract>,
    input: SlotId,
}

/// The prepared handle of one writer call site, checked against its phase.
fn writer_call(
    program: &LocalProgram,
    site: ProgramCallSite,
    phase: AggregateKernelPhase,
    input: SlotId,
) -> Result<WriterCall, String> {
    let Some(ProgramStateTemplate::Aggregate { kernel, .. }) = program.state_template(site) else {
        return Err(format!(
            "compiled writer call {site:?} has no prepared aggregate"
        ));
    };
    let handle = kernel.clone();
    let contract = Arc::clone(handle.contract());
    if contract.phase() != phase
        || contract.logical_argument_types().len() != 1
        || contract.distinct()
        || !contract.order_keys().is_empty()
    {
        return Err(format!(
            "compiled writer call {site:?} is not a single-channel {phase:?} aggregate"
        ));
    }
    Ok(WriterCall {
        handle,
        contract,
        input,
    })
}

/// The ordinal of `slot` in `layout`.
fn ordinal(layout: &StaticLayout, slot: SlotId) -> Option<usize> {
    layout
        .slots()
        .iter()
        .position(|candidate| *candidate == slot)
}

fn statistics_work(
    owner: &dyn InputConstructionOwner,
    units: usize,
) -> Result<(), ConstructionFailure> {
    owner.observe(
        0,
        &[],
        ArrayNormalizationStep::InspectConstructionWork { units },
    )
}
fn writer_binding_matches(
    expected: &novarocks_type_contract::FunctionValueType,
    actual: &novarocks_type_contract::FunctionValueType,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<bool> {
    enum Failure {
        Source(novarocks_type_contract::ValueTypeError),
        Creator(ConstructionFailure),
    }
    impl From<novarocks_type_contract::ValueTypeError> for Failure {
        fn from(value: novarocks_type_contract::ValueTypeError) -> Self {
            Self::Source(value)
        }
    }
    expected
        .same_physical_binding_ignoring_root_nullability_observed(actual, || {
            statistics_work(owner, 1).map_err(Failure::Creator)
        })
        .map_err(|failure| match failure {
            Failure::Creator(cause) => ExecutionFailure::from(cause),
            Failure::Source(cause) => ConstructionFailure::InvalidSource(cause.to_string()).into(),
        })
}

fn project_statistics_schema(
    layout: &StaticLayout,
    positions: impl ExactSizeIterator<Item = usize>,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<ChunkSchemaRef> {
    if !layout.is_typed() {
        return Err(ConstructionFailure::InvalidSource(
            "writer statistics layout lacks complete typed declarations".into(),
        )
        .into());
    }
    let storage = owner
        .storage_lifetime()
        .retaining_observed(&layout.storage_lifetime(), |layout| owner.request(layout))?;
    owner.request(
        Layout::array::<ChunkSlotSchema>(positions.len()).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?,
    )?;
    let mut slots = Vec::new();
    slots.try_reserve_exact(positions.len()).map_err(|_| {
        ConstructionFailure::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
    })?;
    for index in positions {
        statistics_work(owner, 3)?;
        let field = layout.schema().fields().get(index).ok_or_else(|| {
            ConstructionFailure::InvalidSource("writer statistics occurrence is absent".into())
        })?;
        let binding = layout.value_binding_at(index).ok_or_else(|| {
            ConstructionFailure::InvalidSource("writer statistics binding is absent".into())
        })?;
        let (_, unique_id) = layout.slot_metadata_at(index).ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "writer statistics slot declaration is absent".into(),
            )
        })?;
        slots.push(ChunkSlotSchema::try_new_typed_observed::<
            ConstructionFailure,
            _,
        >(
            layout.slots()[index],
            field.clone(),
            binding.clone(),
            unique_id,
            storage.clone(),
            |step| owner.observe(index, &[], step),
        )?);
    }
    let schema = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
        slots,
        storage,
        |layout| owner.request(layout),
        |units| statistics_work(owner, units),
    )?;
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<ChunkSchema>())
            .map_err(ConstructionFailure::Resource)?,
    )?;
    Ok(Arc::new(schema))
}

fn statistics_copy_failure(
    failure: novarocks_functions::selected_copy::CopyConstructionError<ConstructionFailure>,
) -> ExecutionFailure {
    use novarocks_functions::selected_copy::{CopyConstructionError, CopyError};
    match failure {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(CopyError::Control(cause)) => cause.into(),
        CopyConstructionError::Copy(CopyError::Extent) => KernelFailure::ResourceExhausted.into(),
        CopyConstructionError::Copy(CopyError::Invalid(message)) => {
            ConstructionFailure::InvalidSource(message.to_owned()).into()
        }
        CopyConstructionError::Copy(CopyError::Unsupported(ty)) => {
            ConstructionFailure::InvalidSource(format!(
                "writer statistics carrier {ty:?} is unsupported"
            ))
            .into()
        }
    }
}

fn final_statistics_schema(
    mut calls: PartialCallIter<'_>,
    slots: impl ExactSizeIterator<Item = SlotId>,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<ChunkSchemaRef> {
    use novarocks_type_contract::{TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef};
    if slots.len() != calls.len() + 1 {
        return Err(ConstructionFailure::InvalidSource(
            "writer final statistics declaration arity differs".into(),
        )
        .into());
    }
    let storage = owner.storage_lifetime();
    owner.request(
        ValueTypeRef::storage_allocation_layout().map_err(ConstructionFailure::Resource)?,
    )?;
    let grouping =
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, storage.clone()).map_err(
            |error| {
                ConstructionFailure::InvalidSource(format!("writer target declaration: {error}"))
            },
        )?;
    owner.request(
        ValueLayout::storage_allocation_layout().map_err(ConstructionFailure::Resource)?,
    )?;
    let grouping_layout =
        ValueLayout::try_new(&DataType::Int32, storage.clone()).map_err(|error| {
            ConstructionFailure::InvalidSource(format!("writer target layout: {error}"))
        })?;
    let grouping = TypedValueBinding::try_new(grouping, grouping_layout).map_err(|error| {
        ConstructionFailure::InvalidSource(format!("writer target binding: {error}"))
    })?;
    owner.request(Layout::array::<ChunkSlotSchema>(slots.len()).map_err(|_| {
        ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?)?;
    let mut output = Vec::new();
    output.try_reserve_exact(slots.len()).map_err(|_| {
        ConstructionFailure::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
    })?;
    for (at, slot) in slots.enumerate() {
        statistics_work(owner, 1)?;
        let binding = if at == 0 {
            grouping.clone()
        } else {
            calls
                .next()
                .expect("checked final arity")
                .contract
                .final_type()
                .typed_binding()
                .clone()
        };
        let prefix = if at == 0 {
            "write_target_ordinal"
        } else {
            "final_aggregate_"
        };
        let mut digits = [0u8; 20];
        let mut start = digits.len();
        if at != 0 {
            let mut ordinal = at - 1;
            loop {
                start -= 1;
                digits[start] = b'0' + (ordinal % 10) as u8;
                ordinal /= 10;
                if ordinal == 0 {
                    break;
                }
            }
        }
        let len = prefix.len() + digits.len() - start;
        owner.request(Layout::array::<u8>(len).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
        let mut name = String::new();
        name.try_reserve_exact(len).map_err(|_| {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        name.push_str(prefix);
        for &digit in &digits[start..] {
            name.push(char::from(digit));
        }
        statistics_work(owner, len)?;
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Field>())
                .map_err(ConstructionFailure::Resource)?,
        )?;
        let field = Arc::new(Field::new(
            name,
            binding.data_type().clone(),
            binding.nullable(),
        ));
        output.push(ChunkSlotSchema::try_new_typed_observed::<
            ConstructionFailure,
            _,
        >(
            slot,
            field,
            binding,
            None,
            storage.clone(),
            |step| owner.observe(at, &[], step),
        )?);
    }
    let schema = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
        output,
        storage,
        |layout| owner.request(layout),
        |units| statistics_work(owner, units),
    )?;
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<ChunkSchema>())
            .map_err(ConstructionFailure::Resource)?,
    )?;
    Ok(Arc::new(schema))
}

/// Both hosts lend the same exact immutable prepared handles to one runtime.
#[derive(Clone)]
enum PartialCallSource {
    Compiled {
        calls: Arc<[WriterCall]>,
        positions: Arc<[usize]>,
    },
    Prepared(novarocks_local_program::PreparedWriterAggregateCalls),
}
struct PartialCallRef<'a> {
    handle: &'a PreparedAggregateHandle,
    contract: &'a Arc<AggregateCallContract>,
    input: SlotId,
}
enum PartialCallIter<'a> {
    Compiled(std::slice::Iter<'a, WriterCall>),
    Prepared(std::slice::Iter<'a, novarocks_local_program::PreparedWriterAggregateCall>),
}
impl<'a> Iterator for PartialCallIter<'a> {
    type Item = PartialCallRef<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Compiled(calls) => calls.next().map(|call| PartialCallRef {
                handle: &call.handle,
                contract: &call.contract,
                input: call.input,
            }),
            Self::Prepared(calls) => calls.next().map(|call| PartialCallRef {
                handle: call.handle(),
                contract: call.handle().contract(),
                input: call.input(),
            }),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}
impl ExactSizeIterator for PartialCallIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Compiled(calls) => calls.len(),
            Self::Prepared(calls) => calls.len(),
        }
    }
}
enum PartialPositionIter<'a> {
    Compiled(std::iter::Copied<std::slice::Iter<'a, usize>>),
    Prepared(std::slice::Iter<'a, novarocks_local_program::PreparedWriterAggregateCall>),
}
impl Iterator for PartialPositionIter<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        match self {
            Self::Compiled(values) => values.next(),
            Self::Prepared(values) => values.next().map(|call| call.output_position()),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}
impl ExactSizeIterator for PartialPositionIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Compiled(values) => values.len(),
            Self::Prepared(values) => values.len(),
        }
    }
}
impl PartialCallSource {
    fn len(&self) -> usize {
        match self {
            Self::Compiled { calls, .. } => calls.len(),
            Self::Prepared(calls) => calls.calls().len(),
        }
    }
    fn iter(&self) -> PartialCallIter<'_> {
        match self {
            Self::Compiled { calls, .. } => PartialCallIter::Compiled(calls.iter()),
            Self::Prepared(calls) => PartialCallIter::Prepared(calls.calls().iter()),
        }
    }
    fn positions(&self) -> PartialPositionIter<'_> {
        match self {
            Self::Compiled { positions, .. } => {
                PartialPositionIter::Compiled(positions.iter().copied())
            }
            Self::Prepared(calls) => PartialPositionIter::Prepared(calls.calls().iter()),
        }
    }
}

/// Each driver's partial statistics of one compiled writer.
pub(crate) struct CompiledWriterPartialFactory {
    name: String,
    calls: PartialCallSource,
    /// Immutable source declarations; runtime schema storage is created by prepare.
    output_layout: StaticLayout,
    error: Arc<RuntimeErrorState>,
}

impl CompiledWriterPartialFactory {
    /// The PlanTree/local host already prepared its independent frozen calls.
    /// Create only this runtime header under the actual installed preparation
    /// owner, retaining that finite responsibility through the last processor.
    pub(crate) fn create_prepared_in(
        calls: novarocks_local_program::PreparedWriterAggregateCalls,
        input_layout: StaticLayout,
        output_layout: StaticLayout,
        error: Arc<RuntimeErrorState>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Box<dyn Operator>> {
        if calls.phase() != AggregateKernelPhase::Partial
            || !input_layout.is_typed()
            || !output_layout.is_typed()
        {
            return Err(ConstructionFailure::InvalidSource(
                "writer prepared source changes its phase or complete layouts".into(),
            )
            .into());
        }
        const NAME: &str = "COMPILED_WRITER_PARTIAL_AGGREGATE";
        owner.request(Layout::array::<u8>(NAME.len()).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
        let name = NAME.to_owned();
        owner.request(Layout::new::<CompiledWriterPartialProcessor>())?;
        Ok(Box::new(CompiledWriterPartialProcessor {
            name,
            calls: PartialCallSource::Prepared(calls),
            output_layout,
            input_layout: Some(input_layout),
            _creation: Some(owner.storage_lifetime()),
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(error),
            tracker: None,
            states: None,
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        }))
    }

    /// The partial statistics of local writer `node`, or `None` when it
    /// collects none.
    pub(crate) fn try_new(
        program: &LocalProgram,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Option<Self>, String> {
        let at = node.index();
        let graph_node = program
            .graph()
            .nodes()
            .get(at)
            .ok_or_else(|| format!("missing compiled table writer node {at}"))?;
        let ProgramNodeKind::TableWriter {
            projection,
            writer_multiplex_layout,
            partial_aggregates,
            ..
        } = graph_node.kind()
        else {
            return Err(format!("compiled node {at} is not a TableWriter"));
        };
        if partial_aggregates.is_empty() {
            return Ok(None);
        }
        let mut calls = Vec::with_capacity(partial_aggregates.len());
        let mut output_positions = Vec::with_capacity(partial_aggregates.len());
        for (ordinal_in_node, call) in partial_aggregates.iter().enumerate() {
            let site = ProgramCallSite::WriterPartial {
                node,
                call: u32::try_from(ordinal_in_node)
                    .map_err(|_| format!("compiled writer {at} has too many partial calls"))?,
            };
            if ordinal(&projection.layout, call.input_slot_id).is_none() {
                return Err(format!(
                    "compiled writer partial call {site:?} reads slot {} outside its projection",
                    call.input_slot_id
                ));
            }
            let output = ordinal(writer_multiplex_layout, call.intermediate_slot_id)
                .filter(|ordinal| *ordinal > 3)
                .ok_or_else(|| {
                    format!(
                        "compiled writer partial call {site:?} writes slot {} outside its auxiliary channels",
                        call.intermediate_slot_id
                    )
                })?;
            let prepared = writer_call(
                program,
                site,
                AggregateKernelPhase::Partial,
                call.input_slot_id,
            )?;
            let binding = writer_multiplex_layout
                .value_binding_at(output)
                .ok_or_else(|| {
                    format!(
                        "compiled writer partial call {site:?} channel lacks a typed declaration"
                    )
                })?;
            let declared =
                novarocks_type_contract::FunctionValueType::try_from_typed_binding(binding.clone())
                    .map_err(|error| error.to_string())?;
            let intermediate = prepared.contract.intermediate_type();
            if !intermediate.same_physical_binding_ignoring_root_nullability_observed::<novarocks_type_contract::ValueTypeError>(
                &declared, || Ok(()),
            ).map_err(|error| error.to_string())?
                || (intermediate.nullable() && !declared.nullable())
            {
                return Err(format!(
                    "compiled writer partial call {site:?} state differs from its channel"
                ));
            }
            output_positions.push(output);
            calls.push(prepared);
        }
        Ok(Some(Self {
            name: format!("COMPILED_WRITER_PARTIAL_AGGREGATE (node={at})"),
            calls: PartialCallSource::Compiled {
                calls: Arc::from(calls),
                positions: Arc::from(output_positions),
            },
            output_layout: writer_multiplex_layout.clone(),
            error,
        }))
    }
}

impl OperatorFactory for CompiledWriterPartialFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledWriterPartialProcessor {
            name: self.name.clone(),
            calls: self.calls.clone(),
            output_layout: self.output_layout.clone(),
            input_layout: None,
            _creation: None,
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            states: None,
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

/// One driver's partial statistics: one state per call, all in one group.
struct CompiledWriterPartialProcessor {
    name: String,
    calls: PartialCallSource,
    output_layout: StaticLayout,
    input_layout: Option<StaticLayout>,
    _creation: Option<ValueTypeStorageLifetime>,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    /// One column per call, each holding exactly its driver's one state.
    states: Option<Vec<AggregateStateColumn>>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

impl CompiledWriterPartialProcessor {
    fn ensure_states(&mut self) -> ExecutionResult<()> {
        if self.output.is_none() {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        if self.states.is_some() {
            return Ok(());
        }
        let tracker = bound_tracker(self.tracker.as_ref(), "CompiledWriterPartialAggregate")?;
        let allocator: Arc<dyn AggregateStateAllocator> = Arc::new(TrackedStateAllocator {
            tracker: MemTracker::new_child("CompiledWriterPartialStates", &tracker),
        });
        let mut states = Vec::with_capacity(self.calls.len());
        for call in self.calls.iter() {
            let mut column = AggregateStateColumn::try_new(
                call.handle.clone(),
                Arc::clone(&allocator),
                NonZeroUsize::MIN,
            )?;
            column.push(&self.control)?;
            states.push(column);
        }
        self.states = Some(states);
        Ok(())
    }

    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        self.ensure_states()?;
        // Writer and statistics have distinct actual demands. Keep the writer's
        // original Chunk; only this processor adapts its own typed view.
        for (call, column) in self
            .calls
            .iter()
            .zip(self.states.as_ref().expect("states were created"))
        {
            if !call.handle.same_installed_owner(column.handle()) {
                return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                    "writer state and input have different installed owners",
                ))
                .into());
            }
            call.handle.argument_encoding(0)?;
        }
        let adapted = crate::exec::pipeline::operator::hydrate_for_downstream_with_factory(
            chunk,
            self,
            self.construction.as_deref(),
        )?;
        let chunk = &adapted;
        let rows = chunk.len();
        let mapping = vec![0usize; rows];
        let states = self.states.as_mut().expect("states were created");
        for (call, column) in self.calls.iter().zip(states.iter_mut()) {
            let values = chunk.column_by_slot_id(call.input)?;
            let arguments = [EvaluatedArgument::Column(&values)];
            let source = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                chunk
                    .chunk_schema()
                    .slot(call.input)
                    .ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "writer typed input slot is absent".into(),
                        )
                    })?
                    .typed_binding()
                    .clone(),
            )
            .map_err(|cause| ConstructionFailure::InvalidSource(cause.to_string()))?;
            let encoding = call.handle.argument_encoding(0)?;
            let input = SelectedAggregateUpdateInput::try_new_with_typed_sources(
                call.contract.as_ref(),
                Selection::all(rows),
                &arguments,
                &[],
                &[&source],
                &[],
                &[encoding],
                &self.control,
            )?;
            let mut frame =
                column.prepare_update_batch_evaluation(&mapping, input, &self.control)?;
            frame.run(&self.control)?;
        }
        Ok(())
    }

    /// The driver's one intermediate row; the empty input's is each call's
    /// initial state.
    fn emit(&mut self) -> ExecutionResult<()> {
        self.ensure_states()?;
        let states = self.states.as_ref().expect("states were created");
        let output = self
            .output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = self.control.construction_owner(output.as_ref())?;
        owner.request(Layout::array::<ArrayRef>(states.len()).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
        let mut columns = Vec::new();
        columns.try_reserve_exact(states.len()).map_err(|_| {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        for column in states {
            columns.push(column.emit_evaluation(&[0], 1, &self.control)?);
        }
        self.pending
            .push_back(Chunk::try_new_typed_with_columns_owned_observed::<
                ConstructionFailure,
                _,
            >(
                Arc::clone(output),
                columns,
                1,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )?);
        Ok(())
    }
}

impl Operator for CompiledWriterPartialProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_compiled_layout_construction(&self.output_layout)?;
        if let (PartialCallSource::Prepared(source), Some(input)) =
            (&self.calls, &self.input_layout)
        {
            for call in source.calls() {
                statistics_work(owner.as_ref(), 1)?;
                let actual_input = input.value_binding(call.input()).ok_or_else(|| {
                    ConstructionFailure::InvalidSource(
                        "writer prepared input slot is outside its projection".into(),
                    )
                })?;
                let expected_input = call
                    .handle()
                    .contract()
                    .logical_argument_types()
                    .next()
                    .ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "writer prepared logical input is absent".into(),
                        )
                    })?;
                let actual_input =
                    novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                        actual_input.clone(),
                    )
                    .map_err(|cause| ConstructionFailure::InvalidSource(cause.to_string()))?;
                if !writer_binding_matches(expected_input, &actual_input, owner.as_ref())?
                    || actual_input.nullable() && !expected_input.nullable()
                {
                    return Err(ConstructionFailure::InvalidSource(
                        "writer prepared input differs from its complete channel".into(),
                    )
                    .into());
                }
                let ordinal = call.output_position();
                if self.output_layout.slots().get(ordinal) != Some(&call.output()) {
                    return Err(ConstructionFailure::InvalidSource(
                        "writer prepared output occurrence differs from its relation".into(),
                    )
                    .into());
                }
                let output = self
                    .output_layout
                    .value_binding_at(ordinal)
                    .ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "writer prepared output lacks its complete channel".into(),
                        )
                    })?;
                let output = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                    output.clone(),
                )
                .map_err(|cause| ConstructionFailure::InvalidSource(cause.to_string()))?;
                let expected = call.handle().contract().intermediate_type();
                if !writer_binding_matches(expected, &output, owner.as_ref())?
                    || expected.nullable() && !output.nullable()
                {
                    return Err(ConstructionFailure::InvalidSource(
                        "writer prepared intermediate differs from its complete channel".into(),
                    )
                    .into());
                }
            }
        }
        self.output = Some(project_statistics_schema(
            &self.output_layout,
            self.calls.positions(),
            owner.as_ref(),
        )?);
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
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

impl ProcessorOperator for CompiledWriterPartialProcessor {
    fn input_construction_factory(&self) -> Option<&dyn InputConstructionFactory> {
        self.construction.as_deref()
    }
    fn accepts_encoded_path(
        &self,
        occurrence: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        let Some(states) = self.states.as_ref() else {
            return false;
        };
        self.calls
            .iter()
            .zip(states.iter())
            .filter(|(call, _)| call.input == occurrence.slot)
            .all(|(_, state)| {
                occurrence.path.is_empty()
                    && matches!(occurrence.carrier, DataType::Dictionary(key, values) if key.as_ref() == &DataType::Int32 && matches!(values.as_ref(), DataType::Utf8 | DataType::LargeUtf8))
                    && state
                        .argument_encoding(0)
                        .is_ok_and(|token| token.accepts_root_int32_string_dictionary())
            })
    }
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled writer partial aggregate received input after finishing".into());
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
        self.states = None;
        if self.pending.is_empty() {
            self.finished = true;
        }
        Ok(())
    }
}

/// One grouped Unpivot mapping: the final channel it reads and, per literal,
/// its frozen constant (a scalar's root index into the evaluated constants).
#[derive(Clone)]
struct GroupedMapping {
    target: u32,
    value: SlotId,
    literals: Vec<LiteralSource>,
}

/// The compiled statistics of one finish: its prepared final calls, its
/// grouped Unpivot plan and the roots of the plan's scalar constants.
pub(crate) struct CompiledFinishStatistics {
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    calls: Arc<[WriterCall]>,
    /// The finish's final aggregate output: the grouping channel followed
    /// by one final channel per call.
    final_layout: StaticLayout,
    final_slots: Arc<[SlotId]>,
    plan: WriterGroupedUnpivotPlan,
    mappings: Vec<GroupedMapping>,
    /// Every scalar constant root, in mapping-then-literal order.
    constant_sites: Vec<ProgramExpressionRootSite>,
    /// The Root port each scalar constant root reads.
    root_port: StaticLayout,
    error: Arc<RuntimeErrorState>,
}

impl CompiledFinishStatistics {
    /// The statistics of local finish `node` with their coverage facts, or
    /// `None` when it carries none.
    pub(crate) fn try_new(
        program: &Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Option<(FinishStatisticsCoverage, Arc<Self>)>, String> {
        let at = node.index();
        let graph_node = program
            .graph()
            .nodes()
            .get(at)
            .ok_or_else(|| format!("missing compiled table finish node {at}"))?;
        let ProgramNodeKind::TableFinish {
            writer_multiplex_layout,
            root_result_layout,
            final_aggregates,
            ..
        } = graph_node.kind()
        else {
            return Err(format!("compiled node {at} is not a TableFinish"));
        };
        let invalid = |what: String| format!("compiled table finish at local node {at} {what}");
        let unpivot = match (&final_aggregates.unpivot, final_aggregates.calls.is_empty()) {
            (None, true) => return Ok(None),
            (Some(unpivot), false) => unpivot,
            _ => {
                return Err(invalid(
                    "has final aggregates and a grouped Unpivot that are not both present"
                        .to_string(),
                ));
            }
        };
        let grouping = ordinal(writer_multiplex_layout, unpivot.grouping_input_slot_id)
            .ok_or_else(|| invalid("groups by a slot outside its writer relation".to_string()))?;
        if writer_multiplex_layout.schema().field(grouping).data_type() != &DataType::Int32 {
            return Err(invalid("groups by a non-Int32 channel".to_string()));
        }
        let mut calls = Vec::with_capacity(final_aggregates.calls.len());
        let mut slots = vec![unpivot.grouping_output_slot_id];
        for (call_ordinal, call) in final_aggregates.calls.iter().enumerate() {
            let site = ProgramCallSite::WriterFinal {
                node,
                call: u32::try_from(call_ordinal)
                    .map_err(|_| invalid("has too many final calls".to_string()))?,
            };
            let input = ordinal(writer_multiplex_layout, call.intermediate_input_slot_id)
                .filter(|ordinal| *ordinal > 3)
                .ok_or_else(|| {
                    invalid(format!(
                        "final call {call_ordinal} reads slot {} outside its auxiliary channels",
                        call.intermediate_input_slot_id
                    ))
                })?;
            let prepared = writer_call(
                program,
                site,
                AggregateKernelPhase::Final,
                call.intermediate_input_slot_id,
            )?;
            if writer_multiplex_layout.schema().field(input).data_type()
                != prepared.contract.intermediate_type().data_type()
            {
                return Err(invalid(format!(
                    "final call {call_ordinal} reads a channel of another state type"
                )));
            }
            slots.push(call.final_output_slot_id);
            calls.push(prepared);
        }

        let root_field = |slot: SlotId| {
            ordinal(root_result_layout, slot)
                .map(|ordinal| {
                    root_result_layout
                        .schema()
                        .field(ordinal)
                        .data_type()
                        .clone()
                })
                .ok_or_else(|| invalid(format!("expands into slot {slot} outside its Root")))
        };
        if root_field(unpivot.passthrough_output_slot_id)? != DataType::Int32 {
            return Err(invalid(
                "passes its target through a non-Int32 channel".to_string(),
            ));
        }
        let value_type = root_field(unpivot.value_output_slot_id)?;
        let literal_types = unpivot
            .literal_output_slot_ids
            .iter()
            .map(|slot| root_field(*slot))
            .collect::<Result<Vec<_>, _>>()?;
        let mut mappings = Vec::with_capacity(unpivot.mappings.len());
        let mut constant_sites = Vec::new();
        for (mapping_ordinal, mapping) in unpivot.mappings.iter().enumerate() {
            let value = final_aggregates
                .calls
                .iter()
                .position(|call| call.final_output_slot_id == mapping.input_value_slot_id)
                .ok_or_else(|| {
                    invalid(format!(
                        "mapping {mapping_ordinal} reads a slot outside its final aggregates"
                    ))
                })?;
            if *calls[value].contract.final_type().data_type() != value_type {
                return Err(invalid(format!(
                    "mapping {mapping_ordinal} final value differs from its Root value channel"
                )));
            }
            if mapping.constants.len() != literal_types.len() {
                return Err(invalid(format!(
                    "mapping {mapping_ordinal} has another literal width"
                )));
            }
            let mut literals = Vec::with_capacity(mapping.constants.len());
            for (constant_ordinal, (constant, literal_type)) in
                mapping.constants.iter().zip(&literal_types).enumerate()
            {
                literals.push(match constant {
                    UnpivotConstant::Scalar { .. } => {
                        constant_sites.push(ProgramExpressionRootSite::Node {
                            node,
                            role: ProgramNodeExpressionRole::FinishUnpivotConstant {
                                mapping: u32::try_from(mapping_ordinal)
                                    .map_err(|_| invalid("has too many mappings".to_string()))?,
                                constant: u32::try_from(constant_ordinal)
                                    .map_err(|_| invalid("has too many literals".to_string()))?,
                            },
                        });
                        LiteralSource::Root(constant_sites.len() - 1)
                    }
                    UnpivotConstant::Int32List(values) => {
                        if list_item_field(literal_type).is_none() {
                            return Err(invalid(format!(
                                "expands an Int32List constant into {literal_type:?}"
                            )));
                        }
                        LiteralSource::Int32List(Arc::from(values.as_slice()))
                    }
                    UnpivotConstant::Utf8Map(entries) => {
                        if map_entries_field(literal_type).is_none() {
                            return Err(invalid(format!(
                                "expands a Utf8Map constant into {literal_type:?}"
                            )));
                        }
                        LiteralSource::Utf8Map(Arc::from(entries.as_slice()))
                    }
                });
            }
            mappings.push(GroupedMapping {
                target: mapping.grouping_key,
                value: mapping.input_value_slot_id,
                literals,
            });
        }
        let coverage = FinishStatisticsCoverage::new(
            final_aggregates
                .calls
                .iter()
                .map(|call| (call.intermediate_input_slot_id, call.final_output_slot_id))
                .collect(),
            Some(
                unpivot
                    .mappings
                    .iter()
                    .map(|mapping| (mapping.grouping_key, mapping.input_value_slot_id))
                    .collect(),
            ),
        );
        Ok(Some((
            coverage,
            Arc::new(Self {
                program: Arc::clone(program),
                node,
                calls: Arc::from(calls),
                final_layout: writer_multiplex_layout.clone(),
                final_slots: Arc::from(slots),
                plan: unpivot.clone(),
                mappings,
                constant_sites,
                root_port: root_result_layout.clone(),
                error,
            }),
        )))
    }

    /// Evaluate every scalar constant root once over one row of the Root
    /// port: an `ARTIFACT_DRAFT` kind and NULL in every other field. A
    /// constant reads no channel, so the row only gives it its one row.
    fn evaluate_constants(
        &self,
        state: &RuntimeState,
        root_schema: &ChunkSchemaRef,
        factory: &Arc<dyn InputConstructionFactory>,
        tracker: Option<&Arc<MemTracker>>,
    ) -> ExecutionResult<GroupedConstants> {
        let owner = factory.begin_typed_construction(root_schema)?;
        if self.constant_sites.is_empty() {
            return Ok(GroupedConstants {
                arrays: Vec::new(),
                storage: owner.storage_lifetime(),
            });
        }
        // The compiler's complete declared Root port is retained as a layout,
        // and its actual already-prepared schema remains the Arrow author.
        for slot in root_schema.slots() {
            let original = self
                .root_port
                .value_binding(slot.slot_id())
                .ok_or_else(|| {
                    ExecutionFailure::from("compiled grouped Unpivot Root binding is absent")
                })?;
            if original != slot.typed_binding() {
                return Err(
                    "compiled grouped Unpivot Root binding differs from its declared layout".into(),
                );
            }
        }
        let temporary = factory.begin_typed_construction(root_schema)?;
        let mut columns = super::unpivot_processor::unpivot_vec::<ArrayRef>(
            root_schema.slots().len(),
            temporary.as_ref(),
        )?;
        let mut control = RuntimeKernelControl::new(Arc::clone(&self.error));
        control.bind_runtime_memory(state);
        if let Some(tracker) = tracker {
            control.bind_mem_tracker(Arc::clone(tracker));
        }
        control.bind_construction_factory(Arc::clone(factory));
        for (ordinal, slot) in root_schema.slots().iter().enumerate() {
            if ordinal == 0 {
                grouped_primitive_adoption(temporary.as_ref(), &control)?;
                grouped_arc::<Int8Array>(temporary.as_ref())?;
                columns.push(
                    Arc::new(Int8Array::from(vec![RootRowKind::ArtifactDraft.to_wire()]))
                        as ArrayRef,
                );
            } else if slot.field().is_nullable() {
                columns.push(grouped_null_array(
                    slot.typed_binding().layout(),
                    1,
                    temporary.as_ref(),
                    &control,
                )?);
            } else {
                return Err(format!(
                    "compiled grouped Unpivot at local node {} has a NOT NULL Root field {}",
                    self.node.index(),
                    slot.field().name()
                )
                .into());
            }
        }
        let row = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            Arc::clone(root_schema),
            columns,
            1,
            temporary.storage_lifetime(),
            |step| temporary.observe(0, &[], step),
        )?;
        if !self.constant_sites.is_empty() {
            owner.request(
                Layout::array::<CompiledExpressionInstance>(self.constant_sites.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?,
            )?;
        }
        let mut created = None;
        instances(&mut created, &self.program, &self.constant_sites, &control)?;
        let created = created.as_mut().expect("instances were created");
        let mut constants = super::unpivot_processor::unpivot_vec::<ArrayRef>(
            self.constant_sites.len(),
            owner.as_ref(),
        )?;
        for (instance, site) in created.iter_mut().zip(&self.constant_sites) {
            constants.push(evaluate_all(instance, *site, row.record_batch(), &control)?);
        }
        // This hold covers only this actual constant/reference scaffold.
        // Evaluated payload custody belongs to the original scalar kernel;
        // no source/program/query owner is fabricated to fill that proof.
        Ok(GroupedConstants {
            arrays: constants,
            storage: owner.storage_lifetime(),
        })
    }
}

impl FinishStatisticsFactory for CompiledFinishStatistics {
    fn final_aggregate(
        &self,
        _state: &RuntimeState,
        factory: &dyn InputConstructionFactory,
    ) -> ExecutionResult<Box<dyn Operator>> {
        let owner = factory.begin_compiled_layout_construction(&self.final_layout)?;
        let mut node = self.node.index();
        let mut digits = 1usize;
        while node >= 10 {
            digits += 1;
            node /= 10;
        }
        let bytes = "COMPILED_WRITER_FINAL_AGGREGATE (node=)".len() + digits;
        owner.request(Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?)?;
        let mut name = String::new();
        name.try_reserve_exact(bytes)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        use std::fmt::Write;
        write!(
            &mut name,
            "COMPILED_WRITER_FINAL_AGGREGATE (node={})",
            self.node.index()
        )
        .expect("String formatting cannot fail");
        owner.request(Layout::new::<CompiledWriterFinalAggregate>())?;
        Ok(Box::new(CompiledWriterFinalAggregate {
            name,
            calls: FinalCallSource::Compiled(Arc::clone(&self.calls)),
            grouping: self.plan.grouping_input_slot_id,
            output_layout: Some(self.final_layout.clone()),
            input_schema: None,
            _creation: Some(owner.storage_lifetime()),
            output_slots: FinalOutputSlots::Compiled(Arc::clone(&self.final_slots)),
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            states: None,
            groups: BTreeMap::new(),
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        }))
    }

    fn grouped_unpivot(
        &self,
        state: &RuntimeState,
        mut final_chunk: Chunk,
        root_schema: ChunkSchemaRef,
        tracker: Option<Arc<MemTracker>>,
        factory: Arc<dyn InputConstructionFactory>,
    ) -> ExecutionResult<Box<dyn GroupedUnpivotSource>> {
        if !root_schema.is_typed() || !final_chunk.chunk_schema().is_typed() {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        if let Some(tracker) = tracker.as_ref() {
            final_chunk
                .try_transfer_to_typed(tracker)
                .map_err(ConstructionFailure::Memory)?;
        }
        let owner = factory.begin_typed_construction(&root_schema)?;
        let temporary = factory.begin_typed_construction(&root_schema)?;
        let mut control = RuntimeKernelControl::new(Arc::clone(&self.error));
        control.bind_construction_factory(Arc::clone(&factory));
        // Group/row traversal borrows the actual final array; it constructs
        // neither an Int32Array clone nor a second source Chunk.
        let groups = final_chunk.column_by_slot_id(self.plan.grouping_output_slot_id)?;
        let groups = groups
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                ExecutionFailure::from("table finish final aggregate grouping output is not Int32")
            })?;
        let mut rows = super::unpivot_processor::unpivot_vec::<(u32, usize)>(
            groups.len(),
            temporary.as_ref(),
        )?;
        let table =
            novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<u32, ()>(
                groups.len(),
            )
            .map_err(grouped_hashmap_error)?;
        temporary.observe_opaque_upper_bound(table.request_bytes_upper_bound)?;
        control.checkpoint(0)?;
        let mut seen = std::collections::HashSet::with_capacity(groups.len());
        for row in 0..groups.len() {
            control.checkpoint(1)?;
            if groups.is_null(row) {
                return Err("table finish final aggregate produced a null grouping key".into());
            }
            let target = target_ordinal_from_wire(groups.value(row))
                .map_err(|error| format!("table finish final aggregate grouping key: {error}"))?;
            if !seen.insert(target.get()) {
                return Err(format!(
                    "table finish final aggregate produced duplicate target {}",
                    target.get()
                )
                .into());
            }
            if !self
                .mappings
                .iter()
                .any(|mapping| mapping.target == target.get())
            {
                return Err(format!("table finish final aggregate produced target {} with no grouped Unpivot mapping", target.get()).into());
            }
            rows.push((target.get(), row));
        }
        control.checkpoint(0)?;
        // The originating expression/type authors establish scalar meaning;
        // the Arrow arrays returned later can establish only representation.
        for mapping in &self.mappings {
            for (ordinal, literal) in mapping.literals.iter().enumerate() {
                let expected = root_schema
                    .slot(self.plan.literal_output_slot_ids[ordinal])
                    .ok_or_else(|| {
                        ExecutionFailure::from("grouped Unpivot literal declaration is absent")
                    })?;
                match literal {
                    LiteralSource::Root(root) => {
                        let source = super::compiled_expression::root_value_type(
                            &self.program,
                            self.constant_sites[*root],
                        )?;
                        let expected =
                            novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                                expected.typed_binding().clone(),
                            )
                            .map_err(|_| {
                                ExecutionFailure::from(
                                    "grouped Unpivot literal declaration is invalid",
                                )
                            })?;
                        if !source
                            .fits_physical_binding_observed(&expected, || control.checkpoint(1))?
                        {
                            return Err("grouped Unpivot scalar differs from the complete Root literal declaration".into());
                        }
                    }
                    LiteralSource::Int32List(_) => {
                        if !matches!(expected.typed_binding().value_type().kind(),
                            novarocks_type_contract::ValueKind::List(item)
                                if item.kind() == &novarocks_type_contract::ValueKind::Scalar(DataType::Int32))
                        {
                            return Err(
                                "grouped Unpivot Int32List differs from its Root value domain"
                                    .into(),
                            );
                        }
                    }
                    LiteralSource::Utf8Map(_) => {
                        if !matches!(expected.typed_binding().value_type().kind(),
                            novarocks_type_contract::ValueKind::Map {key,value}
                                if key.kind() == &novarocks_type_contract::ValueKind::String
                                    && value.kind() == &novarocks_type_contract::ValueKind::String)
                        {
                            return Err(
                                "grouped Unpivot Utf8Map differs from its Root value domains"
                                    .into(),
                            );
                        }
                    }
                }
            }
        }
        let constants = self.evaluate_constants(state, &root_schema, &factory, tracker.as_ref())?;
        let count = self
            .plan
            .literal_output_slot_ids
            .len()
            .checked_add(2)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let mut output_fields =
            super::unpivot_processor::unpivot_vec::<ChunkSlotSchema>(count, owner.as_ref())?;
        for slot in [
            self.plan.passthrough_output_slot_id,
            self.plan.value_output_slot_id,
        ]
        .into_iter()
        .chain(self.plan.literal_output_slot_ids.iter().copied())
        {
            output_fields.push(
                root_schema
                    .slot(slot)
                    .cloned()
                    .ok_or_else(|| format!("table finish Root schema is missing slot {slot}"))?,
            );
        }
        let output = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
            output_fields,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )?;
        grouped_arc::<ChunkSchema>(owner.as_ref())?;
        let output = Arc::new(output);
        let final_ordinal = |slot| {
            final_chunk
                .chunk_schema()
                .index_of(slot)
                .ok_or_else(|| format!("table finish final aggregate is missing value slot {slot}"))
        };
        let grouping_ordinal = final_ordinal(self.plan.grouping_output_slot_id)?;
        let expected_value = root_schema
            .slot(self.plan.value_output_slot_id)
            .ok_or_else(|| {
                ExecutionFailure::from("grouped Unpivot Root value declaration is absent")
            })?;
        for mapping in &self.mappings {
            let source = final_chunk
                .chunk_schema()
                .slot(mapping.value)
                .ok_or_else(|| {
                    ExecutionFailure::from("grouped Unpivot final value declaration is absent")
                })?;
            let source = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                source.typed_binding().clone(),
            )
            .map_err(|_| ExecutionFailure::from("grouped Unpivot final declaration is invalid"))?;
            let expected = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                expected_value.typed_binding().clone(),
            )
            .map_err(|_| ExecutionFailure::from("grouped Unpivot Root declaration is invalid"))?;
            if !source.fits_physical_binding_observed(&expected, || control.checkpoint(1))? {
                return Err(
                    "grouped Unpivot final value differs from the complete Root declaration".into(),
                );
            }
        }

        let mut targets =
            super::unpivot_processor::unpivot_vec::<GroupedTarget>(rows.len(), owner.as_ref())?;
        for (target, row) in rows {
            let mapping_count = self
                .mappings
                .iter()
                .filter(|mapping| mapping.target == target)
                .count();
            let mut producers =
                super::unpivot_processor::unpivot_vec::<Producer>(count, owner.as_ref())?;
            producers.push(Producer::Passthrough {
                input: grouping_ordinal,
            });
            let mut inputs =
                super::unpivot_processor::unpivot_vec::<usize>(mapping_count, owner.as_ref())?;
            for mapping in self
                .mappings
                .iter()
                .filter(|mapping| mapping.target == target)
            {
                inputs.push(final_ordinal(mapping.value)?);
            }
            producers.push(Producer::Value { inputs });
            for literal in 0..self.plan.literal_output_slot_ids.len() {
                let mut sources = super::unpivot_processor::unpivot_vec::<LiteralSource>(
                    mapping_count,
                    owner.as_ref(),
                )?;
                for mapping in self
                    .mappings
                    .iter()
                    .filter(|mapping| mapping.target == target)
                {
                    sources.push(mapping.literals[literal].clone());
                }
                producers.push(Producer::Literal { sources });
            }
            if producers.capacity() > producers.len() {
                owner.request(
                    Layout::array::<Producer>(producers.len())
                        .map_err(|_| KernelFailure::ResourceExhausted)?,
                )?;
            }
            owner.request(
                novarocks_type_contract::owned_resources::layout::arc_layout(
                    Layout::array::<Producer>(producers.len())
                        .map_err(|_| KernelFailure::ResourceExhausted)?,
                )
                .map_err(ConstructionFailure::Resource)?,
            )?;
            let producers = Arc::from(producers);
            targets.push(GroupedTarget {
                target,
                row,
                expansion: UnpivotExpansion::new(
                    producers,
                    Arc::clone(&output),
                    mapping_count,
                    self.plan.max_output_rows,
                    self.plan.max_output_bytes,
                ),
            });
        }
        grouped_arc::<Observable>(owner.as_ref())?;
        let source_observable = Arc::new(Observable::new());
        owner.request(Layout::new::<CompiledGroupedUnpivot>())?;
        Ok(Box::new(CompiledGroupedUnpivot {
            final_chunk,
            constants,
            root_schema,
            targets,
            next: 0,
            started: false,
            tracker,
            factory,
            control,
            source_observable,
            _storage: owner.storage_lifetime(),
        }))
    }
}

#[derive(Clone)]
enum FinalCallSource {
    Compiled(Arc<[WriterCall]>),
    Prepared(novarocks_local_program::PreparedWriterAggregateCalls),
}
impl FinalCallSource {
    fn len(&self) -> usize {
        match self {
            Self::Compiled(calls) => calls.len(),
            Self::Prepared(calls) => calls.calls().len(),
        }
    }
    fn iter(&self) -> PartialCallIter<'_> {
        match self {
            Self::Compiled(calls) => PartialCallIter::Compiled(calls.iter()),
            Self::Prepared(calls) => PartialCallIter::Prepared(calls.calls().iter()),
        }
    }
}
#[derive(Clone)]
enum FinalOutputSlots {
    Compiled(Arc<[SlotId]>),
    Prepared {
        grouping: SlotId,
        calls: novarocks_local_program::PreparedWriterAggregateCalls,
    },
}
enum FinalSlotIter<'a> {
    Compiled(std::iter::Copied<std::slice::Iter<'a, SlotId>>),
    Prepared {
        grouping: Option<SlotId>,
        calls: std::slice::Iter<'a, novarocks_local_program::PreparedWriterAggregateCall>,
    },
}
impl Iterator for FinalSlotIter<'_> {
    type Item = SlotId;
    fn next(&mut self) -> Option<SlotId> {
        match self {
            Self::Compiled(slots) => slots.next(),
            Self::Prepared { grouping, calls } => grouping
                .take()
                .or_else(|| calls.next().map(|call| call.output())),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}
impl ExactSizeIterator for FinalSlotIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Compiled(slots) => slots.len(),
            Self::Prepared { grouping, calls } => usize::from(grouping.is_some()) + calls.len(),
        }
    }
}
impl FinalOutputSlots {
    fn iter(&self) -> FinalSlotIter<'_> {
        match self {
            Self::Compiled(slots) => FinalSlotIter::Compiled(slots.iter().copied()),
            Self::Prepared { grouping, calls } => FinalSlotIter::Prepared {
                grouping: Some(*grouping),
                calls: calls.calls().iter(),
            },
        }
    }
}

/// The default host lends independently guarded prepared calls. It creates
/// no aggregate graph or programme, and runs the same final state algorithm.
pub(crate) fn create_prepared_final_in(
    calls: novarocks_local_program::PreparedWriterAggregateCalls,
    grouping_input: SlotId,
    grouping_output: SlotId,
    input_schema: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<Box<dyn Operator>> {
    prepared_final_in(
        calls,
        grouping_input,
        grouping_output,
        input_schema,
        error,
        owner,
    )
    .map(|operator| operator as Box<dyn Operator>)
}

fn prepared_final_in(
    calls: novarocks_local_program::PreparedWriterAggregateCalls,
    grouping_input: SlotId,
    grouping_output: SlotId,
    input_schema: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<Box<CompiledWriterFinalAggregate>> {
    if calls.phase() != AggregateKernelPhase::Final || !input_schema.is_typed() {
        return Err(ConstructionFailure::InvalidSource(
            "writer final preparation is not complete".into(),
        )
        .into());
    }
    const NAME: &str = "PREPARED_WRITER_FINAL_AGGREGATE";
    owner
        .request(Layout::array::<u8>(NAME.len()).map_err(|_| KernelFailure::ResourceExhausted)?)?;
    let mut name = String::new();
    name.try_reserve_exact(NAME.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    name.push_str(NAME);
    owner.request(Layout::new::<CompiledWriterFinalAggregate>())?;
    Ok(Box::new(CompiledWriterFinalAggregate {
        name,
        output_slots: FinalOutputSlots::Prepared {
            grouping: grouping_output,
            calls: calls.clone(),
        },
        calls: FinalCallSource::Prepared(calls),
        grouping: grouping_input,
        output_layout: None,
        input_schema: Some(input_schema),
        _creation: Some(owner.storage_lifetime()),
        output: None,
        construction: None,
        control: RuntimeKernelControl::new(error),
        tracker: None,
        states: None,
        groups: BTreeMap::new(),
        pending: VecDeque::new(),
        finishing: false,
        finished: false,
    }))
}

/// The finish's final aggregate: one state per call of each target group.
struct CompiledWriterFinalAggregate {
    name: String,
    calls: FinalCallSource,
    grouping: SlotId,
    output_layout: Option<StaticLayout>,
    input_schema: Option<ChunkSchemaRef>,
    _creation: Option<ValueTypeStorageLifetime>,
    output_slots: FinalOutputSlots,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    /// One column per call; every column holds one state per target group.
    states: Option<Vec<AggregateStateColumn>>,
    /// Each target's group index, in ascending target order.
    groups: BTreeMap<u32, usize>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

impl CompiledWriterFinalAggregate {
    fn ensure_states(&mut self) -> ExecutionResult<()> {
        if self.output.is_none() {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        if self.states.is_some() {
            return Ok(());
        }
        let tracker = bound_tracker(self.tracker.as_ref(), "CompiledWriterFinalAggregate")?;
        let allocator: Arc<dyn AggregateStateAllocator> = Arc::new(TrackedStateAllocator {
            tracker: MemTracker::new_child("CompiledWriterFinalStates", &tracker),
        });
        let mut states = Vec::with_capacity(self.calls.len());
        for call in self.calls.iter() {
            let stride = call.handle.state_layout().pad_to_align().size().max(1);
            let per_block = (STATE_BLOCK_BYTES / stride).clamp(1, MAX_STATES_PER_BLOCK);
            states.push(AggregateStateColumn::try_new(
                call.handle.clone(),
                Arc::clone(&allocator),
                NonZeroUsize::new(per_block).expect("clamped to at least one state"),
            )?);
        }
        self.states = Some(states);
        Ok(())
    }

    /// Each row's target group, creating a target's group with one initial
    /// state per call on its first row.
    fn assign_groups(&mut self, chunk: &Chunk) -> ExecutionResult<Vec<usize>> {
        let targets = chunk.column_by_slot_id(self.grouping)?;
        let targets = targets
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ExecutionFailure::from("table finish target ordinal is not Int32"))?;
        let mut mapping = Vec::with_capacity(targets.len());
        for row in 0..targets.len() {
            if targets.is_null(row) {
                return Err(ExecutionFailure::from(
                    "table finish received an aggregate partial with a null target ordinal",
                ));
            }
            let target = target_ordinal_from_wire(targets.value(row))
                .map_err(|error| format!("table finish aggregate partial target: {error}"))?
                .get();
            let next = self.groups.len();
            let group = *self.groups.entry(target).or_insert(next);
            if group == next {
                let states = self.states.as_mut().expect("states were created");
                for column in states.iter_mut() {
                    column.push(&self.control)?;
                }
            }
            mapping.push(group);
        }
        Ok(mapping)
    }

    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        if !chunk.chunk_schema().is_typed() {
            return Err(ConstructionFailure::InvalidSource(
                "writer final input lacks its complete declaration".into(),
            )
            .into());
        }
        let grouping = chunk.chunk_schema().slot(self.grouping).ok_or_else(|| {
            ConstructionFailure::InvalidSource("writer final grouping occurrence is absent".into())
        })?;
        if grouping.typed_binding().value_type().kind()
            != &novarocks_type_contract::ValueKind::Scalar(DataType::Int32)
        {
            return Err(ConstructionFailure::InvalidSource(
                "writer final grouping domain changed".into(),
            )
            .into());
        }
        for call in self.calls.iter() {
            let slot = chunk.chunk_schema().slot(call.input).ok_or_else(|| {
                ConstructionFailure::InvalidSource("writer final state occurrence is absent".into())
            })?;
            let actual = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                slot.typed_binding().clone(),
            )
            .map_err(|_| {
                ConstructionFailure::InvalidSource(
                    "writer final state declaration is invalid".into(),
                )
            })?;
            // Sparse multiplex rows explicitly permit absent channel values.
            // The merge selection removes only those root NULLs; every nested
            // domain, child NULL promise and exact value layout remains fixed.
            if !actual.same_physical_binding_ignoring_root_nullability_observed(
                call.contract.intermediate_type(),
                || self.control.checkpoint(1),
            )? {
                return Err(ConstructionFailure::InvalidSource(
                    "writer final state differs from its complete selected declaration".into(),
                )
                .into());
            }
        }
        self.ensure_states()?;
        let mapping = self.assign_groups(chunk)?;
        let rows = chunk.len();
        let states = self.states.as_mut().expect("states were created");
        for (call, column) in self.calls.iter().zip(states.iter_mut()) {
            if !call.handle.same_installed_owner(column.handle()) {
                return Err(ConstructionFailure::InvalidSource(
                    "writer final installed state owner changed".into(),
                )
                .into());
            }
            let values = chunk.column_by_slot_id(call.input)?;
            // A NULL channel carries no state of this call on that row.
            let selected = (0..rows)
                .filter(|row| !values.is_null(*row))
                .collect::<Vec<_>>();
            if selected.is_empty() {
                continue;
            }
            let groups = selected.iter().map(|row| mapping[*row]).collect::<Vec<_>>();
            let selection = Selection::try_sparse(rows, &selected).map_err(|_| {
                ExecutionFailure::from("table finish aggregate partial selection is not ordered")
            })?;
            let input = SelectedAggregateMergeInput::try_new(
                call.contract.as_ref(),
                selection,
                EvaluatedArgument::Column(&values),
                &self.control,
            )?;
            let mut frame = column.prepare_merge_batch_evaluation(&groups, input, &self.control)?;
            frame.run(&self.control)?;
        }
        Ok(())
    }

    /// Every target group's final values, in ascending target order.
    fn emit(&mut self) -> ExecutionResult<()> {
        let count = self.groups.len();
        if count == 0 {
            return Ok(());
        }
        let output = self
            .output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = self.control.construction_owner(output.as_ref())?;
        owner.request(Layout::array::<i32>(count).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
        let mut targets = Vec::new();
        targets.try_reserve_exact(count).map_err(|_| {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        // Group indices are temporary; their real allocator releases them after
        // emit, independently of the returned target array's late lifetime.
        let tracker = bound_tracker(self.tracker.as_ref(), "CompiledWriterFinalAggregate")?;
        let allocator = crate::exec::expr::agg::AggregateAllocator::new(tracker);
        let mut indices = crate::exec::expr::agg::AggregateVec::new_in(allocator);
        indices
            .try_reserve_exact(count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for (target, group) in &self.groups {
            let target =
                novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(*target)
                    .map_err(|error| format!("table finish final target: {error}"))?;
            targets.push(
                target_ordinal_to_wire(target)
                    .map_err(|error| format!("table finish final target: {error}"))?,
            );
            indices.push(*group);
            statistics_work(owner.as_ref(), 1)?;
        }
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
            owner.storage_lifetime(),
            &mut request,
            &mut opaque,
        );
        novarocks_functions::selected_copy::preflight_primitive_vec_adoption_construction(
            &mut creation,
            &self.control,
        )
        .map_err(statistics_copy_failure)?;
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                Layout::new::<Int32Array>(),
            )
            .map_err(ConstructionFailure::Resource)?,
        )?;
        owner.request(
            Layout::array::<ArrayRef>(self.calls.len() + 1).map_err(|_| {
                ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
            })?,
        )?;
        let mut columns = Vec::new();
        columns
            .try_reserve_exact(self.calls.len() + 1)
            .map_err(|_| {
                ConstructionFailure::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                )
            })?;
        columns.push(Arc::new(Int32Array::from(targets)) as ArrayRef);
        let states = self.states.as_ref().expect("a group created the states");
        for column in states {
            columns.push(column.emit_evaluation(&indices, count, &self.control)?);
        }
        self.pending
            .push_back(Chunk::try_new_typed_with_columns_owned_observed::<
                ConstructionFailure,
                _,
            >(
                Arc::clone(output),
                columns,
                count,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )?);
        Ok(())
    }
}

impl Operator for CompiledWriterFinalAggregate {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = match (&self.output_layout, &self.input_schema) {
            (Some(layout), None) => factory.begin_compiled_layout_construction(layout)?,
            (None, Some(schema)) => factory.begin_typed_construction(schema)?,
            _ => {
                return Err(ConstructionFailure::InvalidSource(
                    "writer final source owner is ambiguous".into(),
                )
                .into());
            }
        };
        if let Some(input) = &self.input_schema {
            let grouping = input.slot(self.grouping).ok_or_else(|| {
                ConstructionFailure::InvalidSource("writer final grouping source is absent".into())
            })?;
            if grouping.typed_binding().value_type().kind()
                != &novarocks_type_contract::ValueKind::Scalar(DataType::Int32)
            {
                return Err(ConstructionFailure::InvalidSource(
                    "writer final grouping source is not declared Int32".into(),
                )
                .into());
            }
            for (position, call) in self.calls.iter().enumerate() {
                statistics_work(owner.as_ref(), 1)?;
                if call.contract.phase() != AggregateKernelPhase::Final {
                    return Err(ConstructionFailure::InvalidSource(
                        "writer final phase changed".into(),
                    )
                    .into());
                }
                let source = input.slot(call.input).ok_or_else(|| {
                    ConstructionFailure::InvalidSource("writer final state input is absent".into())
                })?;
                let actual = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                    source.typed_binding().clone(),
                )
                .map_err(|_| {
                    ConstructionFailure::InvalidSource(
                        "writer final state binding is invalid".into(),
                    )
                })?;
                let expected = call.contract.intermediate_type();
                if !writer_binding_matches(expected, &actual, owner.as_ref())? || !actual.nullable()
                {
                    return Err(ConstructionFailure::InvalidSource(
                        "writer final state differs from its complete prepared declaration".into(),
                    )
                    .into());
                }
                if let FinalCallSource::Prepared(calls) = &self.calls {
                    if calls.calls()[position].output_position() != position + 1 {
                        return Err(ConstructionFailure::InvalidSource(
                            "writer final output occurrence changed".into(),
                        )
                        .into());
                    }
                }
            }
        }
        self.output = Some(final_statistics_schema(
            self.calls.iter(),
            self.output_slots.iter(),
            owner.as_ref(),
        )?);
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
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

impl ProcessorOperator for CompiledWriterFinalAggregate {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled writer final aggregate received input after finishing".into());
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
        self.states = None;
        self.groups.clear();
        if self.pending.is_empty() {
            self.finished = true;
        }
        Ok(())
    }
}

struct GroupedConstants {
    arrays: Vec<ArrayRef>,
    storage: ValueTypeStorageLifetime,
}
fn grouped_arc<T>(owner: &dyn InputConstructionOwner) -> ExecutionResult<()> {
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(ConstructionFailure::Resource)?,
    )?;
    Ok(())
}
fn grouped_hashmap_error(
    error: novarocks_type_contract::owned_resources::hashmap::HashMapResourceError,
) -> ExecutionFailure {
    use novarocks_type_contract::owned_resources::hashmap::HashMapResourceError;
    match error {
        HashMapResourceError::SourceModel(message) => {
            ConstructionFailure::InvalidSource(message.to_owned()).into()
        }
        HashMapResourceError::Arithmetic(_) => KernelFailure::ResourceExhausted.into(),
    }
}
pub(crate) fn grouped_primitive_adoption(
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut opaque,
    );
    novarocks_functions::selected_copy::preflight_primitive_vec_adoption_construction(
        &mut creation,
        control,
    )
    .map_err(super::unpivot_processor::unpivot_copy_failure)
}
pub(crate) fn grouped_null_array(
    layout: &novarocks_type_contract::ValueLayout,
    rows: usize,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut opaque,
    );
    novarocks_functions::selected_copy::preflight_null_structure_construction(
        layout,
        rows,
        &mut creation,
        control,
    )
    .map_err(super::unpivot_processor::unpivot_copy_failure)?;
    control.checkpoint(0)?;
    let output = new_null_array(layout.data_type(), rows);
    control.checkpoint(0)?;
    Ok(output)
}

/// One target's expansion over its one final row.
struct GroupedTarget {
    target: u32,
    row: usize,
    expansion: UnpivotExpansion,
}

/// The Root artifact rows of one final batch, target by target in the
/// batch's row order.
struct CompiledGroupedUnpivot {
    final_chunk: Chunk,
    constants: GroupedConstants,
    root_schema: ChunkSchemaRef,
    targets: Vec<GroupedTarget>,
    next: usize,
    started: bool,
    tracker: Option<Arc<MemTracker>>,
    source_observable: Arc<Observable>,
    factory: Arc<dyn InputConstructionFactory>,
    control: RuntimeKernelControl,
    _storage: ValueTypeStorageLifetime,
}

impl GroupedUnpivotSource for CompiledGroupedUnpivot {
    fn visit_targets(
        &self,
        visit: &mut dyn FnMut(u32) -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        for target in &self.targets {
            visit(target.target)?;
        }
        Ok(())
    }

    fn targets(&self) -> Vec<u32> {
        self.targets.iter().map(|target| target.target).collect()
    }

    fn pull(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        loop {
            let Some(target) = self.targets.get_mut(self.next) else {
                return Ok(None);
            };
            if !self.started {
                let owner = self.factory.begin_input_construction(&self.final_chunk)?;
                let row = self
                    .final_chunk
                    .slice_typed_observed::<ConstructionFailure, _>(
                        target.row,
                        1,
                        owner.storage_lifetime(),
                        |step| owner.observe(0, &[], step),
                    )?;
                let mut constants = super::unpivot_processor::unpivot_vec::<ArrayRef>(
                    self.constants.arrays.len(),
                    owner.as_ref(),
                )?;
                for constant in &self.constants.arrays {
                    owner.observe(
                        0,
                        &[],
                        ArrayNormalizationStep::SliceArray {
                            source: constant,
                            offset: 0,
                            length: 1,
                        },
                    )?;
                    constants.push(constant.slice(0, 1));
                    owner.observe(0, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
                }
                let storage = row
                    .source_storage_lifetime_observed::<ConstructionFailure>(
                        owner.storage_lifetime(),
                        |layout| owner.request(layout),
                    )?
                    .retaining_observed(&self.constants.storage, |layout| owner.request(layout))?;
                target
                    .expansion
                    .start_typed(row.into_batch_projection(), constants, storage)?;
                self.started = true;
            }
            if let Some(chunk) = target.expansion.next_typed(
                self.tracker.as_ref(),
                self.factory.as_ref(),
                &self.control,
            )? {
                if let Some(output) = root_artifact_chunk_with_construction(
                    chunk,
                    &self.root_schema,
                    self.tracker.as_ref(),
                    self.factory.as_ref(),
                    &self.control,
                )? {
                    return Ok(Some(output));
                }
                continue;
            }
            // The target's expansion is exhausted.
            self.next += 1;
            self.started = false;
        }
    }

    fn is_finished(&self) -> bool {
        self.next == self.targets.len()
    }

    fn source_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.source_observable)
    }
}

#[cfg(test)]
#[path = "opaque_memory_host_tests.rs"]
mod opaque_memory_host_tests;

#[cfg(test)]
#[path = "compiled_writer_statistics/prepared_source_tests.rs"]
mod prepared_source_tests;

#[path = "compiled_writer_statistics/prepared_grouped.rs"]
mod prepared_grouped;
pub(crate) use prepared_grouped::create_prepared_grouped_in;

#[cfg(test)]
#[path = "compiled_writer_statistics/prepared_final_tests.rs"]
mod prepared_final_tests;
