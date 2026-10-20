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

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts,
    ResourceScope,
};
#[derive(Debug)]
struct UnpivotContractFailure(BoundedDetail);
impl std::fmt::Display for UnpivotContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for UnpivotContractFailure {}
impl FailureProjection for UnpivotContractFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::UnpivotContract.reason(),
        )
    }
}
pub(super) fn unpivot_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(UnpivotContractFailure(BoundedDetail::truncating_display(
        detail,
    )))
}
#[derive(Debug)]
struct UnpivotLimit {
    dimension: ResourceDimension,
    requested: usize,
    limit: usize,
}
impl std::fmt::Display for UnpivotLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unpivot {:?} request {} exceeds limit {}",
            self.dimension, self.requested, self.limit
        )
    }
}
impl std::error::Error for UnpivotLimit {}
impl FailureProjection for UnpivotLimit {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::fragment::ExecutionReason::UnpivotLimit.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Request),
            constraint_scope: Some(ResourceScope::Request),
            dimension: self.dimension,
            requested: u64::try_from(self.requested).ok(),
            required_growth: None,
            limit: u64::try_from(self.limit).ok(),
            live: None,
            committed: None,
        })
    }
}
fn unpivot_limit(dimension: ResourceDimension, requested: usize, limit: usize) -> ExecutionFailure {
    ExecutionFailure::from_domain(UnpivotLimit {
        dimension,
        requested,
        limit,
    })
}
fn unpivot_arithmetic() -> ExecutionFailure {
    ExecutionFailure::from_domain(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
}
pub(super) fn unpivot_arrow(source: arrow::error::ArrowError) -> ExecutionFailure {
    size_probe::UnpivotProbeFailure::Encoding(source).into_execution()
}

#[path = "unpivot_processor/size_probe.rs"]
mod size_probe;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    make_array,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::ipc::writer::StreamWriter;
use arrow_data::transform::MutableArrayData;

use super::compiled_expression::RuntimeKernelControl;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::unpivot::{UnpivotConstant, UnpivotPassthroughColumn, UnpivotValueMapping};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner, InputEncodingPolicy,
    Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
    step_context::StepContext,
};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::runtime_state::RuntimeState;
use novarocks_functions::array_projection::ArrayNormalizationStep;
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
#[cfg(test)]
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_types::SlotId;

const MAX_UNPIVOT_MAPPINGS: usize = 4_096;
const MAX_UNPIVOT_CONSTANTS: usize = 16_384;
const MAX_UNPIVOT_NESTED_ELEMENTS: usize = 4_096;
const MAX_UNPIVOT_CONSTANT_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
/// F3 component oracle over explicit expression/array/layout facts.
/// This test-only fixture owns no construction tree and is not a product operator.
pub(crate) struct UnpivotFixtureFactory {
    name: String,
    arena: Arc<ExprArena>,
    passthrough_columns: Vec<UnpivotPassthroughColumn>,
    value_output_slot_id: SlotId,
    literal_output_slot_ids: Vec<SlotId>,
    value_mappings: Vec<UnpivotValueMapping>,
    output_chunk_schema: Option<ChunkSchemaRef>,
    output_layout: Option<novarocks_local_program::StaticLayout>,
    max_output_rows: usize,
    max_output_bytes: usize,
}

#[cfg(test)]
impl UnpivotFixtureFactory {
    #[expect(
        clippy::too_many_arguments,
        reason = "The typed unpivot operator keeps independently validated column roles explicit."
    )]
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        passthrough_columns: Vec<UnpivotPassthroughColumn>,
        value_output_slot_id: SlotId,
        literal_output_slot_ids: Vec<SlotId>,
        value_mappings: Vec<UnpivotValueMapping>,
        output_chunk_schema: ChunkSchemaRef,
        max_output_rows: usize,
        max_output_bytes: usize,
    ) -> ExecutionResult<Self> {
        validate_static_contract(
            &arena,
            &passthrough_columns,
            value_output_slot_id,
            &literal_output_slot_ids,
            &value_mappings,
            &output_chunk_schema,
            max_output_rows,
            max_output_bytes,
        )?;
        Ok(Self {
            name: if node_id >= 0 {
                format!("UNPIVOT (id={node_id})")
            } else {
                "UNPIVOT".to_string()
            },
            arena,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            output_chunk_schema: Some(output_chunk_schema),
            output_layout: None,
            max_output_rows,
            max_output_bytes,
        })
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "The original Unpivot roles and frozen declaration remain explicit."
    )]
    pub(crate) fn new_from_static_layout(
        node_id: i32,
        arena: Arc<ExprArena>,
        passthrough_columns: Vec<UnpivotPassthroughColumn>,
        value_output_slot_id: SlotId,
        literal_output_slot_ids: Vec<SlotId>,
        value_mappings: Vec<UnpivotValueMapping>,
        output_layout: novarocks_local_program::StaticLayout,
        max_output_rows: usize,
        max_output_bytes: usize,
    ) -> ExecutionResult<Self> {
        if !output_layout.is_typed() {
            return Err(unpivot_contract(
                "unpivot requires a complete typed output declaration",
            ));
        }
        Ok(Self {
            name: if node_id >= 0 {
                format!("UNPIVOT (id={node_id})")
            } else {
                "UNPIVOT".to_owned()
            },
            arena,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            output_chunk_schema: None,
            output_layout: Some(output_layout),
            max_output_rows,
            max_output_bytes,
        })
    }
}

#[cfg(test)]
impl OperatorFactory for UnpivotFixtureFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        self.create_receiver(None)
    }
}
#[cfg(test)]
impl UnpivotFixtureFactory {
    pub(crate) fn create_bound(&self, control: RuntimeKernelControl) -> Box<dyn Operator> {
        self.create_receiver(Some(control))
    }
    fn create_receiver(&self, control: Option<RuntimeKernelControl>) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(UnpivotFixtureKernel {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            passthrough_columns: self.passthrough_columns.clone(),
            value_output_slot_id: self.value_output_slot_id,
            literal_output_slot_ids: self.literal_output_slot_ids.clone(),
            value_mappings: self.value_mappings.clone(),
            output_chunk_schema: self.output_chunk_schema.as_ref().map(Arc::clone),
            output_layout: self.output_layout.clone(),
            construction: None,
            max_output_rows: self.max_output_rows,
            max_output_bytes: self.max_output_bytes,
            input: None,
            cursor: 0,
            output_rows_hint: None,
            control,
            mem_tracker: None,
        }))
    }
}

#[cfg(test)]
struct UnpivotFixtureKernel {
    name: String,
    arena: Arc<ExprArena>,
    passthrough_columns: Vec<UnpivotPassthroughColumn>,
    value_output_slot_id: SlotId,
    literal_output_slot_ids: Vec<SlotId>,
    value_mappings: Vec<UnpivotValueMapping>,
    output_chunk_schema: Option<ChunkSchemaRef>,
    output_layout: Option<novarocks_local_program::StaticLayout>,
    max_output_rows: usize,
    max_output_bytes: usize,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    input: Option<Chunk>,
    cursor: usize,
    output_rows_hint: Option<usize>,
    control: Option<RuntimeKernelControl>,
    mem_tracker: Option<Arc<MemTracker>>,
}

#[cfg(test)]
impl OperatorLifecycle for UnpivotFixtureKernel {
    fn creates_typed_storage(&self) -> bool {
        self.output_layout.is_some()
            || self
                .output_chunk_schema
                .as_ref()
                .is_some_and(|schema| schema.is_typed())
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if !self.creates_typed_storage() {
            return Ok(());
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        if let Some(layout) = &self.output_layout {
            let output = factory.create_compiled_chunk_schema(layout)?;
            validate_static_contract(
                &self.arena,
                &self.passthrough_columns,
                self.value_output_slot_id,
                &self.literal_output_slot_ids,
                &self.value_mappings,
                &output,
                self.max_output_rows,
                self.max_output_bytes,
            )?;
            self.output_chunk_schema = Some(output);
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.mem_tracker = Some(tracker);
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control = Some(RuntimeKernelControl::new(state.error_state()));
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.input = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.input = None;
        Ok(())
    }
    fn cancel(&mut self) {
        self.input = None;
    }
}

#[cfg(test)]
impl InputEncodingPolicy for UnpivotFixtureKernel {}
#[cfg(test)]
impl TransformKernel for UnpivotFixtureKernel {
    fn apply(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        validate_input_contract(
            &chunk,
            &self.passthrough_columns,
            self.value_output_slot_id,
            &self.value_mappings,
            self.output_chunk_schema
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?,
        )?;
        if self
            .output_chunk_schema
            .as_ref()
            .is_some_and(|schema| schema.is_typed())
        {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = factory.begin_input_construction(&chunk)?;
            validate_input_complete(
                &chunk,
                &self.passthrough_columns,
                self.value_output_slot_id,
                &self.value_mappings,
                self.output_chunk_schema.as_ref().unwrap(),
                owner.as_ref(),
            )?;
        }
        self.cursor = 0;
        let next = if chunk.is_empty() {
            NextStep::NeedInput
        } else {
            self.input = Some(chunk);
            NextStep::Runnable
        };
        Ok(StepResult::new(None, next, rows))
    }

    fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let Some(input) = self.input.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        let total_rows = input
            .len()
            .checked_mul(self.value_mappings.len())
            .ok_or_else(unpivot_arithmetic)?;
        let mut hint = self.output_rows_hint;
        let (output, candidate_rows) = if self
            .output_chunk_schema
            .as_ref()
            .is_some_and(|schema| schema.is_typed())
        {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let control = self
                .control
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            next_bounded_output_with_construction(
                total_rows - self.cursor,
                self.max_output_rows,
                self.max_output_bytes,
                &mut hint,
                self.mem_tracker.as_ref(),
                factory.as_ref(),
                control,
                |len| self.build_output_typed(input, self.cursor, len, factory.as_ref(), control),
            )?
        } else {
            next_bounded_output(
                total_rows - self.cursor,
                self.max_output_rows,
                self.max_output_bytes,
                &mut hint,
                self.mem_tracker.as_ref(),
                |len| self.build_output(input, self.cursor, len),
            )?
        };
        self.output_rows_hint = hint;
        self.cursor += candidate_rows;
        let next = if self.cursor == total_rows {
            self.input = None;
            self.cursor = 0;
            NextStep::NeedInput
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(output), next, 0))
    }
}

/// Choose the next expanded batch within the row and byte budgets. `build`
/// materializes the next `len` expanded rows; `remaining` counts the rows not
/// yet emitted. `hint` carries the previous batch's exact fit between calls.
/// Returns the batch and its row count. Shared by the legacy and compiled
/// Unpivot processors so both honor one budget law.
pub(crate) fn next_bounded_output(
    remaining: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    hint: &mut Option<usize>,
    tracker: Option<&Arc<MemTracker>>,
    build: impl FnMut(usize) -> ExecutionResult<Chunk>,
) -> ExecutionResult<(Chunk, usize)> {
    // Explicit legacy expansion adapter. The strong entry below supplies its
    // actual creator, original tracker refusal, and typed IPC probe.
    next_bounded_output_core(
        remaining,
        max_output_rows,
        max_output_bytes,
        hint,
        build,
        |chunk| track_candidate(chunk, tracker),
        |chunk| output_size(chunk, tracker),
        |actual_bytes, max_output_bytes| {
            unpivot_limit(ResourceDimension::Bytes, actual_bytes, max_output_bytes)
        },
    )
}

/// Strong caller entry to the SAME expansion budget search. The caller's
/// constructor must return a fully typed immutable Chunk and retain its own
/// actual creation/source ownership; this function adds no declaration author
/// or fallback. Each IPC/logical-size measurement has independent temp scopes.
#[expect(
    clippy::too_many_arguments,
    reason = "Independent expansion limits, tracker and actual construction/control remain explicit."
)]
pub(crate) fn next_bounded_output_with_construction(
    remaining: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    hint: &mut Option<usize>,
    tracker: Option<&Arc<MemTracker>>,
    factory: &dyn crate::exec::pipeline::operator::input_construction::InputConstructionFactory,
    control: &dyn novarocks_functions::KernelEvaluationControl,
    build: impl FnMut(usize) -> ExecutionResult<Chunk>,
) -> ExecutionResult<(Chunk, usize)> {
    use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
    next_bounded_output_core(
        remaining,
        max_output_rows,
        max_output_bytes,
        hint,
        build,
        |mut chunk| {
            if let Some(tracker) = tracker {
                chunk
                    .try_transfer_to(tracker)
                    .map_err(ConstructionFailure::Memory)?;
            }
            Ok(chunk)
        },
        |chunk| {
            size_probe::output_size_with_construction(chunk, factory, control)
                .map_err(size_probe::UnpivotProbeFailure::into_execution)
        },
        |actual_bytes, max_output_bytes| {
            unpivot_limit(ResourceDimension::Bytes, actual_bytes, max_output_bytes)
        },
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "One shared search separates construction, admission and measurement owners."
)]
fn next_bounded_output_core<E>(
    remaining: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    hint: &mut Option<usize>,
    mut build: impl FnMut(usize) -> Result<Chunk, E>,
    mut track: impl FnMut(Chunk) -> Result<Chunk, E>,
    mut measure: impl FnMut(&Chunk) -> Result<usize, E>,
    mut budget_refusal: impl FnMut(usize, usize) -> E,
) -> Result<(Chunk, usize), E> {
    let row_limit = remaining.min(max_output_rows);
    let had_size_hint = hint.is_some();
    let mut candidate_rows = if max_output_bytes == usize::MAX {
        row_limit
    } else {
        hint.unwrap_or(1).min(row_limit)
    };
    let mut output = track(build(candidate_rows)?)?;
    let mut upper_bound = row_limit;
    let candidate_bytes = if max_output_bytes == usize::MAX {
        None
    } else {
        Some(measure(&output)?)
    };
    if candidate_bytes.is_some_and(|bytes| bytes > max_output_bytes) {
        upper_bound = candidate_rows - 1;
        loop {
            if candidate_rows == 1 {
                let actual_bytes = measure(&output)?;
                return Err(budget_refusal(actual_bytes, max_output_bytes));
            }
            candidate_rows = (candidate_rows / 2).max(1);
            output = track(build(candidate_rows)?)?;
            if max_output_bytes == usize::MAX || measure(&output)? <= max_output_bytes {
                break;
            }
        }
    } else if had_size_hint || candidate_rows == row_limit {
        // Reuse the preceding batch's exact fit, but allow one bounded
        // doubling probe when it now occupies at most half the budget.
        // This recovers from a hint reduced by an earlier large value
        // without repeating a logarithmic search for every output batch.
        upper_bound = candidate_rows;
        if candidate_rows < row_limit
            && candidate_bytes.is_some_and(|bytes| bytes.saturating_mul(2) <= max_output_bytes)
        {
            let next_rows = candidate_rows.saturating_mul(2).min(row_limit);
            let next = track(build(next_rows)?)?;
            if max_output_bytes == usize::MAX || measure(&next)? <= max_output_bytes {
                candidate_rows = next_rows;
                output = next;
                upper_bound = candidate_rows;
            }
        }
    } else {
        // The first byte-bounded batch grows from one row so a small budget
        // never materializes the complete wide expansion merely to learn it
        // is too large.
        while candidate_rows < row_limit {
            let next_rows = candidate_rows.saturating_mul(2).min(row_limit);
            let next = track(build(next_rows)?)?;
            if max_output_bytes == usize::MAX || measure(&next)? <= max_output_bytes {
                candidate_rows = next_rows;
                output = next;
            } else {
                upper_bound = next_rows - 1;
                break;
            }
        }
    }
    while candidate_rows < upper_bound {
        let middle = candidate_rows + (upper_bound - candidate_rows).div_ceil(2);
        let candidate = track(build(middle)?)?;
        if max_output_bytes == usize::MAX || measure(&candidate)? <= max_output_bytes {
            candidate_rows = middle;
            output = candidate;
        } else {
            upper_bound = middle - 1;
        }
    }
    *hint = Some(candidate_rows);
    Ok((output, candidate_rows))
}

fn track_candidate(mut chunk: Chunk, tracker: Option<&Arc<MemTracker>>) -> ExecutionResult<Chunk> {
    if let Some(tracker) = tracker {
        chunk
            .try_transfer_to(tracker)
            .map_err(ExecutionFailure::from_domain)?;
    }
    Ok(chunk)
}

fn output_size(chunk: &Chunk, tracker: Option<&Arc<MemTracker>>) -> ExecutionResult<usize> {
    let retained_bytes = chunk.estimated_bytes();
    let logical_bytes = chunk.logical_bytes();
    let mut encoded = Vec::new();
    {
        let schema = chunk.record_batch().schema();
        let mut writer = StreamWriter::try_new(&mut encoded, &schema).map_err(unpivot_arrow)?;
        writer.write(chunk.record_batch()).map_err(unpivot_arrow)?;
        writer.finish().map_err(unpivot_arrow)?;
    }
    let _encoded_accounting = tracker
        .map(|tracker| {
            TrackedBytes::try_new(encoded.capacity(), Arc::clone(tracker))
                .map_err(ExecutionFailure::from)
        })
        .transpose()?;
    Ok(retained_bytes.max(logical_bytes).max(encoded.len()))
}

fn canonical_int32_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
}

fn canonical_utf8_map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        )),
        false,
    )
}

fn materialize_constant(
    arena: &ExprArena,
    constant: &UnpivotConstant,
    input: &Chunk,
    offset: usize,
    len: usize,
) -> ExecutionResult<ArrayRef> {
    match constant {
        UnpivotConstant::Scalar { expr_id, .. } => arena.eval(*expr_id, &input.slice(offset, len)),
        UnpivotConstant::Int32List(values) => {
            let mut builder = ListBuilder::new(Int32Builder::new())
                .with_field(Arc::new(Field::new("item", DataType::Int32, false)));
            for _ in 0..len {
                builder.values().append_slice(values);
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        UnpivotConstant::Utf8Map(entries) => {
            let mut builder = MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                StringBuilder::new(),
                StringBuilder::new(),
            )
            .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
            .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
            for _ in 0..len {
                for (key, value) in entries {
                    builder.keys().append_value(key);
                    builder.values().append_value(value);
                }
                builder.append(true).map_err(unpivot_arrow)?;
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Segment {
    pub(crate) mapping_index: usize,
    pub(crate) input_offset: usize,
    pub(crate) len: usize,
}

#[cfg(test)]
impl UnpivotFixtureKernel {
    fn build_output(&self, input: &Chunk, start: usize, len: usize) -> ExecutionResult<Chunk> {
        let output_chunk_schema = self
            .output_chunk_schema
            .as_ref()
            .ok_or_else(|| unpivot_contract("unpivot output schema was not prepared"))?;
        let segments = flattened_segments(start, len, input.len());
        let passthrough_by_output = self
            .passthrough_columns
            .iter()
            .map(|mapping| (mapping.output_slot_id, mapping.input_slot_id))
            .collect::<HashMap<_, _>>();
        let literal_index_by_output = self
            .literal_output_slot_ids
            .iter()
            .copied()
            .enumerate()
            .map(|(index, slot)| (slot, index))
            .collect::<HashMap<_, _>>();
        let mut columns = Vec::with_capacity(output_chunk_schema.slots().len());

        for output_slot in output_chunk_schema.slots() {
            let output_slot_id = output_slot.slot_id();
            let parts = if let Some(input_slot_id) = passthrough_by_output.get(&output_slot_id) {
                segment_input_parts(input, *input_slot_id, &segments)?
            } else if output_slot_id == self.value_output_slot_id {
                let mut parts = Vec::with_capacity(segments.len());
                for segment in &segments {
                    let mapping = &self.value_mappings[segment.mapping_index];
                    parts.push(
                        input
                            .column_by_slot_id(mapping.input_value_slot_id)?
                            .slice(segment.input_offset, segment.len),
                    );
                }
                parts
            } else if let Some(literal_index) = literal_index_by_output.get(&output_slot_id) {
                let mut parts = Vec::with_capacity(segments.len());
                for segment in &segments {
                    let mapping = &self.value_mappings[segment.mapping_index];
                    parts.push(materialize_constant(
                        &self.arena,
                        &mapping.constants[*literal_index],
                        input,
                        segment.input_offset,
                        segment.len,
                    )?);
                }
                parts
            } else {
                return Err(unpivot_contract(format_args!(
                    "unpivot output slot {output_slot_id} has no producer"
                )));
            };
            columns.push(concat_owned(parts, output_slot_id)?);
        }

        Chunk::try_new_with_columns(Arc::clone(output_chunk_schema), columns)
            .map_err(ExecutionFailure::from)
    }
}

#[cfg(test)]
impl UnpivotFixtureKernel {
    fn build_output_typed(
        &self,
        input: &Chunk,
        start: usize,
        len: usize,
        factory: &dyn InputConstructionFactory,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Chunk> {
        let output = self
            .output_chunk_schema
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(output)?;
        let temporary = factory.begin_typed_construction(output)?;
        let count = if len == 0 {
            0
        } else {
            (start % input.len())
                .checked_add(len - 1)
                .ok_or(KernelFailure::ResourceExhausted)?
                / input.len()
                + 1
        };
        let mut segments = unpivot_vec::<Segment>(count, temporary.as_ref())?;
        visit_flattened_segments(start, len, input.len(), |segment| segments.push(segment));
        let mut columns = unpivot_vec::<ArrayRef>(output.slots().len(), owner.as_ref())?;
        let mut storage = input
            .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        for slot in output.slots() {
            control.checkpoint(0)?;
            let mut parts = unpivot_vec::<ArrayRef>(segments.len(), temporary.as_ref())?;
            let passthrough = self
                .passthrough_columns
                .iter()
                .find(|mapping| mapping.output_slot_id == slot.slot_id());
            let literal = self
                .literal_output_slot_ids
                .iter()
                .position(|candidate| *candidate == slot.slot_id());
            for segment in &segments {
                control.checkpoint(1)?;
                let mapping = &self.value_mappings[segment.mapping_index];
                let value = if let Some(passthrough) = passthrough {
                    slice_part(
                        &input.column_by_slot_id(passthrough.input_slot_id)?,
                        segment.input_offset,
                        segment.len,
                        temporary.as_ref(),
                    )?
                } else if slot.slot_id() == self.value_output_slot_id {
                    slice_part(
                        &input.column_by_slot_id(mapping.input_value_slot_id)?,
                        segment.input_offset,
                        segment.len,
                        temporary.as_ref(),
                    )?
                } else if let Some(index) = literal {
                    match &mapping.constants[index] {
                        UnpivotConstant::Scalar { expr_id, .. } => {
                            let Some(ExprNode::Constant(value)) = self.arena.node(*expr_id) else {
                                return Err(ConstructionFailure::InvalidSource("typed unpivot scalar requires its original checked constant pool".to_owned()).into());
                            };
                            storage = storage
                                .retaining_observed(&value.pool().storage_lifetime(), |layout| {
                                    owner.request(layout)
                                })?;
                            crate::exec::expr::constant_eval::broadcast_with_construction(
                                value,
                                segment.len,
                                temporary.as_ref(),
                                temporary.as_ref(),
                                control,
                            )?
                        }
                        UnpivotConstant::Int32List(values) => {
                            let view =
                                super::compiled_unpivot::FixedLiteralView::<String>::Int32List(
                                    values,
                                );
                            super::compiled_unpivot::observe_fixed_literal(
                                view,
                                segment.len,
                                temporary.as_ref(),
                                control,
                            )?;
                            let result = super::compiled_unpivot::materialize_fixed_literal(
                                super::compiled_unpivot::FixedLiteralView::<String>::Int32List(
                                    values,
                                ),
                                slot.field(),
                                segment.len,
                            )?;
                            control.checkpoint(0)?;
                            result
                        }
                        UnpivotConstant::Utf8Map(entries) => {
                            super::compiled_unpivot::observe_fixed_literal(
                                super::compiled_unpivot::FixedLiteralView::Utf8Map(entries),
                                segment.len,
                                temporary.as_ref(),
                                control,
                            )?;
                            let result = super::compiled_unpivot::materialize_fixed_literal(
                                super::compiled_unpivot::FixedLiteralView::Utf8Map(entries),
                                slot.field(),
                                segment.len,
                            )?;
                            control.checkpoint(0)?;
                            result
                        }
                    }
                } else {
                    return Err(ConstructionFailure::InvalidSource(format!(
                        "unpivot output slot {} has no producer",
                        slot.slot_id()
                    ))
                    .into());
                };
                parts.push(value);
            }
            columns.push(concat_owned_with_construction(
                parts,
                slot.slot_id(),
                temporary.as_ref(),
                owner.as_ref(),
                control,
            )?);
        }
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(output.clone(), columns, len, storage, |step| {
            owner.observe(0, &[], step)
        })?)
    }
}
fn slice_part(
    source: &ArrayRef,
    offset: usize,
    length: usize,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<ArrayRef> {
    owner.observe(
        0,
        &[],
        ArrayNormalizationStep::SliceArray {
            source,
            offset,
            length,
        },
    )?;
    let result = source.slice(offset, length);
    owner.observe(0, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
    Ok(result)
}
fn validate_input_complete(
    input: &Chunk,
    passthrough: &[UnpivotPassthroughColumn],
    value_output: SlotId,
    mappings: &[UnpivotValueMapping],
    output: &ChunkSchemaRef,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<()> {
    if !input.chunk_schema().is_typed() {
        return Err(ConstructionFailure::InvalidSource(
            "typed unpivot requires a complete typed input".to_owned(),
        )
        .into());
    }
    let check = |source: SlotId, target: SlotId| -> ExecutionResult<()> {
        let source = input
            .chunk_schema()
            .slot(source)
            .ok_or_else(|| unpivot_contract("unpivot source slot is absent"))?
            .typed_binding()
            .borrowed();
        let target = output
            .slot(target)
            .ok_or_else(|| unpivot_contract("unpivot output slot is absent"))?
            .typed_binding()
            .borrowed();
        let same =
            source.same_physical_binding_ignoring_root_nullability_observed(target, || {
                owner.observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units: 1 },
                )
            })?;
        if !same {
            return Err(ConstructionFailure::InvalidSource(
                "unpivot complete value binding drift".to_owned(),
            )
            .into());
        }
        Ok(())
    };
    for mapping in passthrough {
        check(mapping.input_slot_id, mapping.output_slot_id)?;
    }
    for mapping in mappings {
        check(mapping.input_value_slot_id, value_output)?;
    }
    Ok(())
}

pub(crate) fn flattened_segments(start: usize, len: usize, input_rows: usize) -> Vec<Segment> {
    let mut segments = Vec::new();
    visit_flattened_segments(start, len, input_rows, |segment| segments.push(segment));
    segments
}

pub(crate) fn visit_flattened_segments(
    start: usize,
    len: usize,
    input_rows: usize,
    mut emit: impl FnMut(Segment),
) {
    // Mapping-major traversal is the original array algorithm; the relational
    // contract deliberately exposes no output ordering guarantee.
    let mut cursor = start;
    let end = start + len;
    while cursor < end {
        let mapping_index = cursor / input_rows;
        let input_offset = cursor % input_rows;
        let segment_len = (input_rows - input_offset).min(end - cursor);
        emit(Segment {
            mapping_index,
            input_offset,
            len: segment_len,
        });
        cursor += segment_len;
    }
}

fn segment_input_parts(
    input: &Chunk,
    input_slot_id: SlotId,
    segments: &[Segment],
) -> ExecutionResult<Vec<ArrayRef>> {
    let input_array = input.column_by_slot_id(input_slot_id)?;
    Ok(segments
        .iter()
        .map(|segment| input_array.slice(segment.input_offset, segment.len))
        .collect())
}

pub(crate) fn concat_owned(
    parts: Vec<ArrayRef>,
    output_slot_id: SlotId,
) -> ExecutionResult<ArrayRef> {
    let capacity = parts.iter().try_fold(0_usize, |total, part| {
        total
            .checked_add(part.len())
            .ok_or_else(|| unpivot_arithmetic())
    })?;
    let data = parts.iter().map(|part| part.to_data()).collect::<Vec<_>>();
    let refs = data.iter().collect::<Vec<_>>();
    let mut output = MutableArrayData::new(refs, true, capacity);
    for (index, part) in parts.iter().enumerate() {
        output.extend(index, 0, part.len());
    }
    let output = make_array(output.freeze());
    if output.len() != capacity {
        return Err(unpivot_contract(format_args!(
            "unpivot output slot {output_slot_id} materialization produced {} rows, expected {capacity}",
            output.len()
        )));
    }
    Ok(output)
}

/// Actual multi-source MutableArrayData operation. This preserves the original
/// new(refs, true, capacity)/ordered extend/freeze body, and uses the sole
/// functions selection/invoice author before its opaque allocations.
pub(crate) fn concat_owned_with_construction(
    parts: Vec<ArrayRef>,
    output_slot_id: SlotId,
    temporary: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    retained: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
    use novarocks_functions::selected_copy::{
        ExtendSegment, preflight_extend_multi_structure_construction,
    };
    use novarocks_type_contract::ValueSchemaCreation;
    use std::alloc::Layout;
    let capacity = parts
        .iter()
        .try_fold(0usize, |sum, part| sum.checked_add(part.len()))
        .ok_or(novarocks_functions::KernelFailure::ResourceExhausted)?;
    let mut source_refs = unpivot_vec::<&dyn Array>(parts.len(), temporary)?;
    let mut segments = unpivot_vec::<ExtendSegment>(parts.len(), temporary)?;
    for (source, part) in parts.iter().enumerate() {
        control.checkpoint(1)?;
        source_refs.push(part.as_ref());
        segments.push(ExtendSegment {
            source,
            start: 0,
            len: part.len(),
            repeats: 1,
            nulls: 0,
        });
    }
    let mut temporary_request = |layout| temporary.request(layout);
    let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
    let mut retained_request = |layout| retained.request(layout);
    let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
    let mut temporary_creation = ValueSchemaCreation::new(
        temporary.storage_lifetime(),
        &mut temporary_request,
        &mut temporary_opaque,
    );
    let mut retained_creation = ValueSchemaCreation::new(
        retained.storage_lifetime(),
        &mut retained_request,
        &mut retained_opaque,
    );
    let mut work = |boundary: bool| {
        temporary.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork {
                units: usize::from(!boundary),
            },
        )
    };
    preflight_extend_multi_structure_construction(
        &source_refs,
        &segments,
        capacity,
        &mut temporary_creation,
        &mut retained_creation,
        &mut work,
    )
    .map_err(unpivot_copy_failure)?;
    // to_data and its Vec belong to this actual operation, separately from the
    // observed preflight's own exact scratch; no new payload is hydrated.
    let mut data = unpivot_vec::<arrow_data::ArrayData>(parts.len(), temporary)?;
    for part in &parts {
        control.checkpoint(0)?;
        data.push(part.to_data());
        control.checkpoint(0)?;
    }
    temporary.request(
        Layout::array::<&arrow_data::ArrayData>(data.len())
            .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?,
    )?;
    let mut refs = Vec::new();
    refs.try_reserve_exact(data.len())
        .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?;
    refs.extend(data.iter());
    control.checkpoint(0)?;
    let mut output = MutableArrayData::new(refs, true, capacity);
    control.checkpoint(0)?;
    for (index, part) in parts.iter().enumerate() {
        control.checkpoint(0)?;
        output.extend(index, 0, part.len());
        control.checkpoint(0)?;
    }
    control.checkpoint(0)?;
    let output = make_array(output.freeze());
    control.checkpoint(0)?;
    if output.len() != capacity {
        return Err(ConstructionFailure::InvalidSource(format!(
            "unpivot output slot {output_slot_id} materialization produced {} rows, expected {capacity}", output.len()
        )).into());
    }
    Ok(output)
}

pub(crate) fn unpivot_vec<T>(
    count: usize,
    owner: &dyn crate::exec::pipeline::operator::input_construction::InputConstructionOwner,
) -> ExecutionResult<Vec<T>> {
    let layout = std::alloc::Layout::array::<T>(count)
        .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?;
    if layout.size() != 0 {
        owner.request(layout)?;
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?;
    Ok(values)
}

pub(crate) fn unpivot_copy_failure(
    failure: novarocks_functions::selected_copy::CopyConstructionError<
        crate::exec::pipeline::operator::input_construction::ConstructionFailure,
    >,
) -> crate::runtime::fragment::ExecutionFailure {
    use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
    use novarocks_functions::selected_copy::{CopyConstructionError, CopyError};
    match failure {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "The typed unpivot contract keeps independently validated column roles explicit."
)]
fn validate_static_contract(
    arena: &ExprArena,
    passthrough_columns: &[UnpivotPassthroughColumn],
    value_output_slot_id: SlotId,
    literal_output_slot_ids: &[SlotId],
    value_mappings: &[UnpivotValueMapping],
    output_chunk_schema: &ChunkSchemaRef,
    max_output_rows: usize,
    max_output_bytes: usize,
) -> ExecutionResult<()> {
    if max_output_rows == 0 || max_output_bytes == 0 {
        return Err(unpivot_contract(
            "unpivot output row and byte budgets must be greater than zero",
        ));
    }
    if value_mappings.is_empty() {
        return Err(unpivot_contract(
            "unpivot requires at least one value mapping",
        ));
    }
    if value_mappings.len() > MAX_UNPIVOT_MAPPINGS {
        return Err(unpivot_limit(
            ResourceDimension::Items,
            value_mappings.len(),
            MAX_UNPIVOT_MAPPINGS,
        ));
    }
    let mut constant_count = 0usize;
    let mut nested_element_count = 0usize;
    let mut constant_bytes = 0usize;
    let mut output_roles = HashSet::new();
    for mapping in passthrough_columns {
        if !output_roles.insert(mapping.output_slot_id) {
            return Err(unpivot_contract(format_args!(
                "unpivot output slot {} has multiple producers",
                mapping.output_slot_id
            )));
        }
    }
    if !output_roles.insert(value_output_slot_id) {
        return Err(unpivot_contract(format_args!(
            "unpivot value output slot {value_output_slot_id} has multiple producers"
        )));
    }
    for slot_id in literal_output_slot_ids {
        if !output_roles.insert(*slot_id) {
            return Err(unpivot_contract(format_args!(
                "unpivot literal output slot {slot_id} has multiple producers"
            )));
        }
    }
    let schema_outputs = output_chunk_schema
        .slot_ids()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if output_roles != schema_outputs {
        return Err(unpivot_contract(format_args!(
            "unpivot output roles {output_roles:?} do not match output schema {schema_outputs:?}"
        )));
    }
    for (mapping_index, mapping) in value_mappings.iter().enumerate() {
        constant_count = constant_count
            .checked_add(mapping.constants.len())
            .ok_or_else(|| unpivot_arithmetic())?;
        if constant_count > MAX_UNPIVOT_CONSTANTS {
            return Err(unpivot_limit(
                ResourceDimension::Items,
                constant_count,
                MAX_UNPIVOT_CONSTANTS,
            ));
        }
        if mapping.constants.len() != literal_output_slot_ids.len() {
            return Err(unpivot_contract(format_args!(
                "unpivot mapping {mapping_index} literal count mismatch: expected {}, got {}",
                literal_output_slot_ids.len(),
                mapping.constants.len()
            )));
        }
        for (literal_index, constant) in mapping.constants.iter().enumerate() {
            let output_slot = output_chunk_schema
                .slot(literal_output_slot_ids[literal_index])
                .expect("validated output slot membership");
            let constant_type = match constant {
                UnpivotConstant::Scalar { expr_id, nullable } => {
                    let retained = match arena.node(*expr_id) {
                        Some(ExprNode::Literal(value)) => execution_literal_retained_bytes(value),
                        Some(ExprNode::Constant(value)) => {
                            let same = value
                                .value_type()
                                .typed_binding()
                                .borrowed()
                                .same_physical_binding_ignoring_root_nullability_observed(
                                    output_slot.typed_binding().borrowed(),
                                    || Ok::<(), std::convert::Infallible>(()),
                                )
                                .unwrap_or_else(|never| match never {});
                            if !same || value.value_type().nullable() != *nullable {
                                return Err(unpivot_contract(
                                    "unpivot constant differs from its frozen complete type",
                                ));
                            }
                            let facts = value.pool().resource_facts();
                            let logical_carrier = value.value_type().data_type();
                            if matches!(
                                logical_carrier,
                                DataType::List(_)
                                    | DataType::LargeList(_)
                                    | DataType::FixedSizeList(_, _)
                                    | DataType::ListView(_)
                                    | DataType::LargeListView(_)
                                    | DataType::Struct(_)
                                    | DataType::Map(_, _)
                                    | DataType::Union(_, _)
                            ) {
                                // Conservatively include the retained pool's
                                // admitted logical extent, not only one row.
                                nested_element_count = nested_element_count
                                    .checked_add(
                                        usize::try_from(facts.logical_elements_upper_bound)
                                            .map_err(|_| unpivot_arithmetic())?,
                                    )
                                    .ok_or_else(|| unpivot_arithmetic())?;
                            }
                            facts
                                .retained_buffer_capacity_bytes
                                .checked_add(facts.metadata_bytes)
                                .and_then(|bytes| usize::try_from(bytes).ok())
                                .ok_or_else(|| unpivot_arithmetic())?
                        }
                        _ => {
                            return Err(unpivot_contract(format_args!(
                                "unpivot mapping {mapping_index} scalar constant {literal_index} is not a literal expression"
                            )));
                        }
                    };
                    constant_bytes = constant_bytes
                        .checked_add(retained)
                        .ok_or_else(|| unpivot_arithmetic())?;
                    arena.data_type(*expr_id).cloned().ok_or_else(|| {
                        unpivot_contract(format_args!(
                            "unpivot mapping {mapping_index} scalar constant {literal_index} has no declared type"
                        ))
                    })?
                }
                UnpivotConstant::Int32List(values) => {
                    nested_element_count = nested_element_count
                        .checked_add(values.len())
                        .ok_or_else(|| unpivot_arithmetic())?;
                    constant_bytes = constant_bytes
                        .checked_add(
                            values
                                .len()
                                .checked_mul(size_of::<i32>())
                                .ok_or_else(unpivot_arithmetic)?,
                        )
                        .ok_or_else(|| unpivot_arithmetic())?;
                    canonical_int32_list_type()
                }
                UnpivotConstant::Utf8Map(entries) => {
                    nested_element_count = nested_element_count
                        .checked_add(entries.len())
                        .ok_or_else(|| unpivot_arithmetic())?;
                    let mut previous = None;
                    for (entry_index, (key, value)) in entries.iter().enumerate() {
                        if key.is_empty() {
                            return Err(unpivot_contract(format_args!(
                                "unpivot mapping {mapping_index} map constant {literal_index} entry {entry_index} has an empty key"
                            )));
                        }
                        if previous.is_some_and(|previous: &str| previous >= key.as_str()) {
                            return Err(unpivot_contract(format_args!(
                                "unpivot mapping {mapping_index} map constant {literal_index} keys must be strictly increasing"
                            )));
                        }
                        constant_bytes = constant_bytes
                            .checked_add(key.len())
                            .and_then(|total| total.checked_add(value.len()))
                            .ok_or_else(|| unpivot_arithmetic())?;
                        previous = Some(key.as_str());
                    }
                    canonical_utf8_map_type()
                }
            };
            if &constant_type != output_slot.data_type() {
                return Err(unpivot_contract(format_args!(
                    "unpivot mapping {mapping_index} constant {literal_index} type mismatch: constant {:?}, output {:?}",
                    constant_type,
                    output_slot.data_type()
                )));
            }
        }
    }
    if nested_element_count > MAX_UNPIVOT_NESTED_ELEMENTS {
        return Err(unpivot_limit(
            ResourceDimension::Items,
            nested_element_count,
            MAX_UNPIVOT_NESTED_ELEMENTS,
        ));
    }
    if constant_bytes > MAX_UNPIVOT_CONSTANT_BYTES {
        return Err(unpivot_limit(
            ResourceDimension::Bytes,
            constant_bytes,
            MAX_UNPIVOT_CONSTANT_BYTES,
        ));
    }
    for (literal_index, slot_id) in literal_output_slot_ids.iter().enumerate() {
        let nullable = value_mappings.iter().any(|mapping| {
            matches!(
                mapping.constants[literal_index],
                UnpivotConstant::Scalar { nullable: true, .. }
            )
        });
        let output_slot = output_chunk_schema
            .slot(*slot_id)
            .expect("validated literal output slot membership");
        if nullable != output_slot.nullable() {
            return Err(unpivot_contract(format_args!(
                "unpivot literal output slot {slot_id} nullability drift: expected {nullable}, got {}",
                output_slot.nullable()
            )));
        }
    }
    Ok(())
}

fn validate_input_contract(
    input: &Chunk,
    passthrough_columns: &[UnpivotPassthroughColumn],
    value_output_slot_id: SlotId,
    value_mappings: &[UnpivotValueMapping],
    output_chunk_schema: &ChunkSchemaRef,
) -> ExecutionResult<()> {
    for mapping in passthrough_columns {
        let input_slot = input
            .chunk_schema()
            .slot(mapping.input_slot_id)
            .ok_or_else(|| {
                unpivot_contract(format_args!(
                    "unpivot passthrough input slot {} is missing",
                    mapping.input_slot_id
                ))
            })?;
        let output_slot = output_chunk_schema
            .slot(mapping.output_slot_id)
            .expect("validated output slot membership");
        if input_slot.data_type() != output_slot.data_type()
            || input_slot.nullable() != output_slot.nullable()
        {
            return Err(unpivot_contract(format_args!(
                "unpivot passthrough slot shape drift: input {} is {:?} nullable={}, output {} is {:?} nullable={}",
                mapping.input_slot_id,
                input_slot.data_type(),
                input_slot.nullable(),
                mapping.output_slot_id,
                output_slot.data_type(),
                output_slot.nullable()
            )));
        }
    }
    let value_output = output_chunk_schema
        .slot(value_output_slot_id)
        .expect("validated value output slot membership");
    let mut nullable = false;
    for (mapping_index, mapping) in value_mappings.iter().enumerate() {
        let input_slot = input
            .chunk_schema()
            .slot(mapping.input_value_slot_id)
            .ok_or_else(|| {
                unpivot_contract(format_args!(
                    "unpivot mapping {mapping_index} input value slot {} is missing",
                    mapping.input_value_slot_id
                ))
            })?;
        if input_slot.data_type() != value_output.data_type() {
            return Err(unpivot_contract(format_args!(
                "unpivot mapping {mapping_index} value type drift: input {:?}, output {:?}",
                input_slot.data_type(),
                value_output.data_type()
            )));
        }
        nullable |= input_slot.nullable();
    }
    if nullable != value_output.nullable() {
        return Err(unpivot_contract(format_args!(
            "unpivot value output nullability drift: expected {nullable}, got {}",
            value_output.nullable()
        )));
    }
    Ok(())
}

fn execution_literal_retained_bytes(value: &crate::exec::expr::LiteralValue) -> usize {
    match value {
        crate::exec::expr::LiteralValue::Utf8(value) => value.len(),
        crate::exec::expr::LiteralValue::Binary(value) => value.len(),
        _ => size_of::<crate::exec::expr::LiteralValue>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::exec::pipeline::{step::NextStep, test_driver::TestDriver};
    use arrow::array::{Array, BinaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn schema(slots: Vec<(u32, &str, DataType, bool)>) -> ChunkSchemaRef {
        let slots = slots
            .into_iter()
            .map(|(id, name, data_type, nullable)| {
                ChunkSlotSchema::from_field(
                    SlotId::new(id),
                    &Field::new(name, data_type, nullable),
                    None,
                )
                .unwrap()
            })
            .collect();
        Arc::new(ChunkSchema::try_new(slots).unwrap())
    }

    fn input_chunk() -> Chunk {
        let schema = schema(vec![
            (1, "group", DataType::Utf8, false),
            (2, "v1", DataType::Int32, true),
            (3, "v2", DataType::Int32, false),
        ]);
        Chunk::try_new_with_columns(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
                Arc::new(Int32Array::from(vec![20, 30])),
            ],
        )
        .unwrap()
    }

    fn byte_budget_input_chunk() -> Chunk {
        let groups = (0..128)
            .map(|index| format!("group-{index:03}-{}", "x".repeat(128)))
            .collect::<Vec<_>>();
        Chunk::try_new_with_columns(
            schema(vec![
                (1, "group", DataType::Utf8, false),
                (2, "v1", DataType::Int32, true),
                (3, "v2", DataType::Int32, false),
            ]),
            vec![
                Arc::new(StringArray::from(groups)),
                Arc::new(Int32Array::from((0..128).map(Some).collect::<Vec<_>>())),
                Arc::new(Int32Array::from_iter_values(128..256)),
            ],
        )
        .unwrap()
    }

    fn factory(max_rows: usize, max_bytes: usize) -> UnpivotFixtureFactory {
        let mut arena = ExprArena::default();
        let first = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("first".to_string())),
            DataType::Utf8,
        );
        let second = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("second".to_string())),
            DataType::Utf8,
        );
        UnpivotFixtureFactory::new(
            9,
            Arc::new(arena),
            vec![UnpivotPassthroughColumn {
                input_slot_id: SlotId::new(1),
                output_slot_id: SlotId::new(11),
            }],
            SlotId::new(13),
            vec![SlotId::new(12)],
            vec![
                UnpivotValueMapping {
                    input_value_slot_id: SlotId::new(2),
                    constants: vec![UnpivotConstant::Scalar {
                        expr_id: first,
                        nullable: false,
                    }],
                },
                UnpivotValueMapping {
                    input_value_slot_id: SlotId::new(3),
                    constants: vec![UnpivotConstant::Scalar {
                        expr_id: second,
                        nullable: false,
                    }],
                },
            ],
            schema(vec![
                (11, "group", DataType::Utf8, false),
                (12, "label", DataType::Utf8, false),
                (13, "value", DataType::Int32, true),
            ]),
            max_rows,
            max_bytes,
        )
        .unwrap()
    }

    #[test]
    fn unpivot_expands_typed_values_and_splits_by_row_budget() {
        let state = RuntimeState::default();
        let mut operator = factory(2, usize::MAX).create(1, 0);
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input_chunk()).unwrap();

        let first = processor.advance_to_boundary().unwrap().output.unwrap();
        let second = processor.advance_to_boundary().unwrap().output.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert!(matches!(processor.next(), NextStep::NeedInput));
        let mut rows = Vec::new();
        for chunk in [&first, &second] {
            let groups = chunk
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let labels = chunk
                .record_batch()
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = chunk
                .record_batch()
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..chunk.len() {
                rows.push((
                    groups.value(row).to_string(),
                    labels.value(row).to_string(),
                    (!values.is_null(row)).then(|| values.value(row)),
                ));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("a".to_string(), "first".to_string(), Some(10)),
                ("a".to_string(), "second".to_string(), Some(20)),
                ("b".to_string(), "first".to_string(), None),
                ("b".to_string(), "second".to_string(), Some(30)),
            ]
        );
    }

    #[test]
    fn empty_input_produces_no_batch() {
        let state = RuntimeState::default();
        let mut operator = factory(2, usize::MAX).create(1, 0);
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        let input = input_chunk();
        let empty = Chunk::try_new_with_chunk_schema(
            RecordBatch::new_empty(Arc::new(Schema::new(
                input
                    .chunk_schema()
                    .slots()
                    .iter()
                    .map(|slot| Arc::new(slot.field().clone()))
                    .collect::<Vec<_>>(),
            ))),
            input.chunk_schema_ref(),
        )
        .unwrap();
        processor.accept_without_output(empty).unwrap();
        let step = processor.advance_to_boundary().unwrap();
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::NeedInput));
    }

    #[test]
    fn direct_construction_rejects_noncanonical_map_constants() {
        let error = UnpivotFixtureFactory::new(
            1,
            Arc::new(ExprArena::default()),
            Vec::new(),
            SlotId::new(11),
            vec![SlotId::new(12)],
            vec![UnpivotValueMapping {
                input_value_slot_id: SlotId::new(1),
                constants: vec![UnpivotConstant::Utf8Map(vec![
                    ("b".to_string(), "1".to_string()),
                    ("a".to_string(), "2".to_string()),
                ])],
            }],
            schema(vec![
                (11, "value", DataType::Int64, false),
                (12, "properties", canonical_utf8_map_type(), false),
            ]),
            1,
            1024,
        )
        .err()
        .expect("unordered map keys");
        assert!(error.detail().contains("strictly increasing"), "{error}");
    }

    #[test]
    fn direct_construction_preserves_scalar_constant_nullability() {
        let mut arena = ExprArena::default();
        let null = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);
        let error = UnpivotFixtureFactory::new(
            1,
            Arc::new(arena),
            Vec::new(),
            SlotId::new(11),
            vec![SlotId::new(12)],
            vec![UnpivotValueMapping {
                input_value_slot_id: SlotId::new(1),
                constants: vec![UnpivotConstant::Scalar {
                    expr_id: null,
                    nullable: true,
                }],
            }],
            schema(vec![
                (11, "value", DataType::Int64, false),
                (12, "label", DataType::Utf8, false),
            ]),
            1,
            1024,
        )
        .err()
        .expect("nullable constant into non-null output");
        assert!(error.detail().contains("nullability drift"), "{error}");
    }

    #[test]
    fn single_value_over_byte_budget_is_resource_exhausted() {
        let state = RuntimeState::default();
        let mut operator = factory(100, 1).create(1, 0);
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input_chunk()).unwrap();
        let error = processor.advance_to_boundary().unwrap_err();
        let crate::runtime::fragment::ExecutionFailureCause::Domain(domain) = error.cause() else {
            panic!("the actual per-output budget author");
        };
        let original = domain.downcast_ref::<UnpivotLimit>().unwrap();
        assert_eq!(original.dimension, ResourceDimension::Bytes);
        assert_eq!(original.limit, 1);
        assert!(original.requested > original.limit);
        assert_eq!(error.failure_fact(), original.failure_fact());
        let resource = error.failure_fact().resource().unwrap();
        assert_eq!(resource.requested, Some(original.requested as u64));
        assert_eq!(resource.limit, Some(1));
        assert_eq!(resource.request_scope, Some(ResourceScope::Request));
        assert!(error.failure_fact().representation().is_none());
    }

    #[test]
    fn splits_by_actual_output_byte_budget() {
        let state = RuntimeState::default();
        let mut one_row_operator = factory(1, usize::MAX).create(1, 0);
        let mut one_row_processor = TestDriver::new(one_row_operator.as_mut(), &state);
        one_row_processor
            .accept_without_output(byte_budget_input_chunk())
            .unwrap();
        let one_row_size = output_size(
            &one_row_processor
                .advance_to_boundary()
                .unwrap()
                .output
                .expect("one output row"),
            None,
        )
        .unwrap();

        let mut full_operator = factory(256, usize::MAX).create(1, 0);
        let mut full_processor = TestDriver::new(full_operator.as_mut(), &state);
        full_processor
            .accept_without_output(byte_budget_input_chunk())
            .unwrap();
        let full_size = output_size(
            &full_processor
                .advance_to_boundary()
                .unwrap()
                .output
                .expect("complete expansion"),
            None,
        )
        .unwrap();
        assert!(one_row_size < full_size);
        let byte_budget = one_row_size + (full_size - one_row_size) / 2;

        let mut bounded_operator = factory(256, byte_budget).create(1, 0);
        let mut bounded_processor = TestDriver::new(bounded_operator.as_mut(), &state);
        bounded_processor
            .accept_without_output(byte_budget_input_chunk())
            .unwrap();
        let mut rows = 0;
        let mut batches = 0;
        while matches!(
            bounded_processor.next(),
            NextStep::Runnable | NextStep::Yield
        ) {
            let chunk = bounded_processor
                .advance_to_boundary()
                .unwrap()
                .output
                .unwrap();
            assert!(output_size(&chunk, None).unwrap() <= byte_budget);
            rows += chunk.len();
            batches += 1;
        }
        assert_eq!(rows, 256);
        assert!(batches > 1);
    }

    #[test]
    fn wide_mapping_set_streams_without_materializing_the_full_expansion() {
        let state = RuntimeState::default();
        let mut arena = ExprArena::default();
        let label = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("value".to_string())),
            DataType::Utf8,
        );
        let mappings = (0..1_001)
            .map(|_| UnpivotValueMapping {
                input_value_slot_id: SlotId::new(2),
                constants: vec![UnpivotConstant::Scalar {
                    expr_id: label,
                    nullable: false,
                }],
            })
            .collect::<Vec<_>>();
        let factory = UnpivotFixtureFactory::new(
            10,
            Arc::new(arena),
            vec![],
            SlotId::new(13),
            vec![SlotId::new(12)],
            mappings,
            schema(vec![
                (12, "label", DataType::Utf8, false),
                (13, "value", DataType::Int32, true),
            ]),
            17,
            usize::MAX,
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input_chunk()).unwrap();

        let mut output_rows = 0;
        let mut output_batches = 0;
        while matches!(processor.next(), NextStep::Runnable | NextStep::Yield) {
            let chunk = processor.advance_to_boundary().unwrap().output.unwrap();
            assert!(chunk.len() <= 17);
            output_rows += chunk.len();
            output_batches += 1;
        }
        assert_eq!(output_rows, 2_002);
        assert!(output_batches > 100);
    }

    #[test]
    fn supports_independent_passthrough_literal_and_value_types() {
        independent_type_case(ExprNode::Literal(LiteralValue::Int8(7)));
        let source = cv_constant(
            Arc::new(arrow::array::Int8Array::from(vec![99, 7])),
            novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
            1,
        );
        independent_type_case(ExprNode::Constant(source));
    }

    fn independent_type_case(label: ExprNode) {
        let state = RuntimeState::default();
        let mut arena = ExprArena::default();
        let label = arena.push_typed(label, DataType::Int8);
        let factory = UnpivotFixtureFactory::new(
            11,
            Arc::new(arena),
            vec![UnpivotPassthroughColumn {
                input_slot_id: SlotId::new(1),
                output_slot_id: SlotId::new(11),
            }],
            SlotId::new(13),
            vec![SlotId::new(12)],
            vec![UnpivotValueMapping {
                input_value_slot_id: SlotId::new(2),
                constants: vec![UnpivotConstant::Scalar {
                    expr_id: label,
                    nullable: false,
                }],
            }],
            schema(vec![
                (11, "key", DataType::Int64, false),
                (12, "label", DataType::Int8, false),
                (13, "value", DataType::Binary, true),
            ]),
            128,
            usize::MAX,
        )
        .unwrap();
        let input = Chunk::try_new_with_columns(
            schema(vec![
                (1, "key", DataType::Int64, false),
                (2, "value", DataType::Binary, true),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![41, 42])),
                Arc::new(BinaryArray::from(vec![Some(&b"a"[..]), None])),
            ],
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input).unwrap();
        let output = processor.advance_to_boundary().unwrap().output.unwrap();

        assert_eq!(
            output
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[41, 42]
        );
        assert_eq!(
            output
                .record_batch()
                .column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[7, 7]
        );
        assert!(output.record_batch().column(2).is_null(1));
    }

    #[test]
    fn preserves_all_null_value_columns() {
        let state = RuntimeState::default();
        let input = Chunk::try_new_with_columns(
            schema(vec![
                (1, "group", DataType::Utf8, false),
                (2, "v1", DataType::Int32, true),
                (3, "v2", DataType::Int32, true),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![None::<i32>, None])),
                Arc::new(Int32Array::from(vec![None::<i32>, None])),
            ],
        )
        .unwrap();
        let mut operator = factory(128, usize::MAX).create(1, 0);
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input).unwrap();
        let output = processor.advance_to_boundary().unwrap().output.unwrap();

        assert_eq!(output.len(), 4);
        assert_eq!(output.record_batch().column(2).null_count(), 4);
    }

    #[test]
    fn byte_hint_recovers_after_a_large_leading_value() {
        let state = RuntimeState::default();
        let output_schema = schema(vec![(11, "value", DataType::Binary, false)]);
        let make_factory = |max_rows, max_bytes| {
            UnpivotFixtureFactory::new(
                12,
                Arc::new(ExprArena::default()),
                vec![],
                SlotId::new(11),
                vec![],
                vec![UnpivotValueMapping {
                    input_value_slot_id: SlotId::new(1),
                    constants: vec![],
                }],
                Arc::clone(&output_schema),
                max_rows,
                max_bytes,
            )
            .unwrap()
        };
        let make_input = || {
            let mut values = Vec::with_capacity(128);
            values.push(vec![1_u8; 100 * 1024]);
            values.push(vec![2_u8; 60 * 1024]);
            values.extend((2..128).map(|_| vec![3_u8]));
            Chunk::try_new_with_columns(
                schema(vec![(1, "value", DataType::Binary, false)]),
                vec![Arc::new(BinaryArray::from_iter_values(
                    values.iter().map(Vec::as_slice),
                ))],
            )
            .unwrap()
        };

        let mut measuring_operator = make_factory(1, usize::MAX).create(1, 0);
        let mut measuring_processor = TestDriver::new(measuring_operator.as_mut(), &state);
        measuring_processor
            .accept_without_output(make_input())
            .unwrap();
        let first_row = measuring_processor
            .advance_to_boundary()
            .unwrap()
            .output
            .unwrap();
        let byte_budget = output_size(&first_row, None).unwrap();

        let mut bounded_operator = make_factory(128, byte_budget).create(1, 0);
        let mut bounded_processor = TestDriver::new(bounded_operator.as_mut(), &state);
        bounded_processor
            .accept_without_output(make_input())
            .unwrap();
        let mut batch_rows = Vec::new();
        while matches!(
            bounded_processor.next(),
            NextStep::Runnable | NextStep::Yield
        ) {
            batch_rows.push(
                bounded_processor
                    .advance_to_boundary()
                    .unwrap()
                    .output
                    .unwrap()
                    .len(),
            );
        }

        assert_eq!(batch_rows.iter().sum::<usize>(), 128);
        assert_eq!(batch_rows[0], 1);
        assert!(batch_rows[1..].iter().any(|rows| *rows > 1));
        assert!(batch_rows.len() < 16, "batch rows: {batch_rows:?}");
    }

    #[test]
    fn pending_output_is_admitted_with_retained_input_and_released_on_failure() {
        let state = RuntimeState::default();
        let tracker = MemTracker::new_root("unpivot-memory-limit");
        let mut input = input_chunk();
        let input_bytes = i64::try_from(input.logical_bytes()).expect("bounded test input");
        tracker
            .install_limit_once(input_bytes + 1)
            .expect("install query memory limit");
        input
            .try_transfer_to(&tracker)
            .expect("retained input fits query limit");
        let original_alias = input.batch_projection();

        let mut operator = factory(2, usize::MAX).create(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        TestDriver::new(operator.as_mut(), &state)
            .accept_without_output(input)
            .unwrap();

        let error = TestDriver::new(operator.as_mut(), &state)
            .advance_to_boundary()
            .expect_err("retained input plus pending output must exceed the query limit");
        assert!(error.detail().contains("ResourceExhausted"), "{error}");
        assert_eq!(
            tracker.current(),
            input_bytes,
            "only the original caller alias remains paid"
        );
        assert_eq!(original_alias.record_batch().num_rows(), 2);
        drop(original_alias);
        assert_eq!(
            tracker.current(),
            0,
            "failure cleanup released the receiver's input while the operator is still alive"
        );
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error)
        );

        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
    fn cv_constant(
        array: ArrayRef,
        ty: novarocks_type_contract::FunctionValueType,
        ordinal: u32,
    ) -> novarocks_functions::ConstantValue {
        struct Control;
        impl novarocks_type_contract::PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        novarocks_functions::ConstantPool::try_new(
            // IPC/source representation is independent of the normalized
            // execution binding, especially for Dictionary and Run arrays.
            Arc::new(Field::new(
                "source",
                array.data_type().clone(),
                ty.nullable(),
            )),
            ty,
            array.to_data(),
            novarocks_functions::ConstantPolicy {
                max_rows: 10000,
                max_array_nodes: 100,
                max_logical_elements: 10000,
                max_retained_buffer_bytes: 1024 * 1024,
                max_type_depth: 64,
                max_type_nodes: 4096,
                max_dictionary_depth: 64,
                max_metadata_bytes: 65536,
                max_library_validation_work: 1024 * 1024,
                max_library_validation_bytes: 1024 * 1024,
            },
            novarocks_type_contract::CompilePhase::Validate,
            &Control,
        )
        .unwrap()
        .value(ordinal)
        .unwrap()
    }

    #[test]
    fn shared_scalar_constants_cannot_retag_logical_identity_or_nullability() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        for source in [
            cv_constant(
                Arc::new(StringArray::from(vec!["{}"])),
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    false,
                    ValueLogicalType::Json,
                )
                .unwrap(),
                0,
            ),
            cv_constant(
                Arc::new(StringArray::from(vec![None::<&str>])),
                FunctionValueType::new(DataType::Utf8, true),
                0,
            ),
        ] {
            let mut arena = ExprArena::default();
            let id = arena.push_typed(ExprNode::Constant(source), DataType::Utf8);
            let error = UnpivotFixtureFactory::new(
                1,
                Arc::new(arena),
                vec![],
                SlotId::new(11),
                vec![SlotId::new(12)],
                vec![UnpivotValueMapping {
                    input_value_slot_id: SlotId::new(1),
                    constants: vec![UnpivotConstant::Scalar {
                        expr_id: id,
                        nullable: false,
                    }],
                }],
                schema(vec![
                    (11, "value", DataType::Int64, false),
                    (12, "label", DataType::Utf8, false),
                ]),
                128,
                1024,
            )
            .err()
            .expect("shared CV retagging must be refused");
            assert!(error.detail().contains("frozen complete type"), "{error}");
        }
    }

    #[test]
    fn encoded_nested_constants_cannot_bypass_the_element_limit() {
        use arrow::array::{DictionaryArray, Int8Array, Int16Array, ListArray, RunArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{Int8Type, Int16Type};
        use novarocks_type_contract::FunctionValueType;

        for elements in [4093_i32, 4094, 4095, 5000] {
            let values: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, false)),
                OffsetBuffer::new(vec![0_i32, elements].into()),
                Arc::new(StringArray::from(vec![""; elements as usize])),
                None,
            ));
            let encoded: [ArrayRef; 2] = [
                Arc::new(
                    DictionaryArray::<Int8Type>::try_new(
                        Int8Array::from(vec![0]),
                        Arc::clone(&values),
                    )
                    .unwrap(),
                ),
                Arc::new(
                    RunArray::<Int16Type>::try_new(&Int16Array::from(vec![1]), values.as_ref())
                        .unwrap(),
                ),
            ];
            for (encoding, array) in encoded.into_iter().enumerate() {
                let ty = array.data_type().clone();
                let source = cv_constant(array, FunctionValueType::new(ty.clone(), false), 0);
                // REE retains its run-end row as well as the encoded value.
                let logical_elements = elements as u64 + if encoding == 0 { 2 } else { 3 };
                assert_eq!(
                    source.pool().resource_facts().logical_elements_upper_bound,
                    logical_elements
                );
                let mut arena = ExprArena::default();
                let id = arena.push_typed(ExprNode::Constant(source), ty.clone());
                let result = UnpivotFixtureFactory::new(
                    1,
                    Arc::new(arena),
                    vec![],
                    SlotId::new(11),
                    vec![SlotId::new(12)],
                    vec![UnpivotValueMapping {
                        input_value_slot_id: SlotId::new(1),
                        constants: vec![UnpivotConstant::Scalar {
                            expr_id: id,
                            nullable: false,
                        }],
                    }],
                    schema(vec![
                        (11, "value", DataType::Int64, false),
                        (12, "label", ty, false),
                    ]),
                    128,
                    1024,
                );
                if logical_elements <= 4096 {
                    assert!(
                        result.is_ok(),
                        "encoded nested payload at the limit must be accepted"
                    );
                } else {
                    let error = result
                        .err()
                        .expect("encoded nested payload must count toward the same element limit");
                    let fact = error.failure_fact();
                    assert_eq!(fact.class(), FailureClass::Resource);
                    assert_eq!(
                        fact.reason(),
                        crate::runtime::fragment::ExecutionReason::UnpivotLimit.reason()
                    );
                    let resource = fact.resource().unwrap();
                    assert_eq!(resource.dimension, ResourceDimension::Items);
                    assert_eq!(resource.limit, Some(MAX_UNPIVOT_NESTED_ELEMENTS as u64));
                    assert_eq!(resource.requested, Some(logical_elements));
                }
            }
        }
    }
    struct PureDeclarationControl;
    impl novarocks_type_contract::PureCompileControl for PureDeclarationControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    fn typed_declaration(
        fields: &[(u32, &str, novarocks_type_contract::FunctionValueType)],
    ) -> ChunkSchemaRef {
        let pure = ValueTypeStorageLifetime::pure();
        Arc::new(
            crate::exec::chunk::ChunkSchema::try_new_typed(
                fields
                    .iter()
                    .map(|(id, name, ty)| {
                        crate::exec::chunk::ChunkSlotSchema::try_new_typed(
                            SlotId::new(*id),
                            Arc::new(Field::new(*name, ty.data_type().clone(), ty.nullable())),
                            ty.typed_binding().clone(),
                            None,
                            pure.clone(),
                        )
                        .unwrap()
                    })
                    .collect(),
                pure,
            )
            .unwrap(),
        )
    }
    fn actual_unpivot_creator(
        state: &RuntimeState,
        tracker: Arc<MemTracker>,
    ) -> Arc<dyn InputConstructionFactory> {
        use crate::exec::pipeline::operator::input_construction::{
            InputConstructionProfile, LockedArrowInputConstructionModel,
            ObservedInputConstructionFactory,
            runtime::{RuntimeInputConstructionControl, TrackedInputConstructionHost},
        };
        ObservedInputConstructionFactory::try_new(
            InputConstructionProfile {
                max_requests: 4096,
                max_request_bytes: 1 << 20,
                max_opaque_requests: 4096,
                max_opaque_bytes: 1 << 20,
                max_work: 1 << 20,
                max_depth: 64,
            },
            Arc::new(TrackedInputConstructionHost::new(tracker)),
            Arc::new(RuntimeInputConstructionControl::new(state.error_state())),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap()
    }
    fn typed_scalar_unpivot() -> UnpivotFixtureFactory {
        use novarocks_type_contract::FunctionValueType;
        let label_type = FunctionValueType::new(DataType::Int8, false);
        let label = cv_constant(
            Arc::new(arrow::array::Int8Array::from(vec![99, 7])),
            label_type.clone(),
            1,
        );
        let mut arena = ExprArena::default();
        let expr_id = arena.push_value_typed(ExprNode::Constant(label), label_type.clone());
        let output = typed_declaration(&[
            (11, "group", FunctionValueType::new(DataType::Utf8, false)),
            (12, "label", label_type),
            (13, "value", FunctionValueType::new(DataType::Int32, true)),
        ]);
        let layout = novarocks_local_program::StaticLayout::try_new_typed_for_compile(
            output.arrow_schema_ref(),
            Arc::from(output.slot_ids()),
            output
                .slots()
                .iter()
                .map(|slot| (slot.typed_binding().clone(), slot.unique_id()))
                .collect(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &PureDeclarationControl,
        )
        .unwrap();
        UnpivotFixtureFactory::new_from_static_layout(
            9,
            Arc::new(arena),
            vec![UnpivotPassthroughColumn {
                input_slot_id: SlotId::new(1),
                output_slot_id: SlotId::new(11),
            }],
            SlotId::new(13),
            vec![SlotId::new(12)],
            vec![UnpivotValueMapping {
                input_value_slot_id: SlotId::new(2),
                constants: vec![UnpivotConstant::Scalar {
                    expr_id,
                    nullable: false,
                }],
            }],
            layout,
            128,
            1 << 20,
        )
        .unwrap()
    }
    fn typed_scalar_input() -> Chunk {
        use novarocks_type_contract::FunctionValueType;
        let schema = typed_declaration(&[
            (1, "group", FunctionValueType::new(DataType::Utf8, false)),
            (2, "value", FunctionValueType::new(DataType::Int32, true)),
        ]);
        Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::fragment::ExecutionFailure,
            _,
        >(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
            ],
            2,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap()
    }
    #[test]
    fn default_unpivot_frozen_layout_broadcasts_checked_scalar_without_slicing_input() {
        let state = RuntimeState::default();
        let tracker = MemTracker::new_root("typed default unpivot");
        let weak_control = Arc::downgrade(&state.error_state());
        let factory = typed_scalar_unpivot();
        let mut operator = factory.create(1, 0);
        operator.set_construction_factory(actual_unpivot_creator(&state, tracker.clone()));
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        TestDriver::new(operator.as_mut(), &state)
            .accept_without_output(typed_scalar_input())
            .unwrap();
        let output = TestDriver::new(operator.as_mut(), &state)
            .advance_to_boundary()
            .unwrap()
            .output
            .unwrap();
        assert!(output.chunk_schema().is_typed());
        assert_eq!(output.len(), 2);
        let labels = output
            .record_batch()
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int8Array>()
            .unwrap();
        assert_eq!(labels.values().as_ref(), &[7, 7]);
        let values = output
            .record_batch()
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert!(values.is_null(1));
        assert!(
            output
                .record_batch()
                .schema()
                .fields()
                .iter()
                .all(|field| field.metadata().capacity() == 0)
        );
        let late = output.clone();
        drop(output);
        drop(operator);
        drop(factory);
        drop(state);
        assert!(weak_control.upgrade().is_none());
        assert!(tracker.current() > 0);
        assert_eq!(
            late.record_batch()
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .value(0),
            7
        );
        drop(late);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn default_unpivot_actual_creator_refusal_keeps_tracker_cause_and_rolls_back_temporary() {
        let state = RuntimeState::default();
        let tracker = MemTracker::new_root("typed default unpivot refusal");
        let factory = typed_scalar_unpivot();
        let mut operator = factory.create(1, 0);
        operator.set_construction_factory(actual_unpivot_creator(&state, tracker.clone()));
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        TestDriver::new(operator.as_mut(), &state)
            .accept_without_output(typed_scalar_input())
            .unwrap();
        let before = tracker.current();
        tracker.install_limit_once(before).unwrap();
        let failure = TestDriver::new(operator.as_mut(), &state)
            .advance_to_boundary()
            .unwrap_err();
        assert!(matches!(
            failure.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Construction(
                ConstructionFailure::Memory(_)
            )
        ));
        assert_eq!(tracker.current(), before);
        drop(operator);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn default_unpivot_rejects_same_carrier_plain_source_for_declared_json_value() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let state = RuntimeState::default();
        let tracker = MemTracker::new_root("unpivot complete domain gate");
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let output = typed_declaration(&[(13, "value", json)]);
        let layout = novarocks_local_program::StaticLayout::try_new_typed_for_compile(
            output.arrow_schema_ref(),
            Arc::from(output.slot_ids()),
            output
                .slots()
                .iter()
                .map(|slot| (slot.typed_binding().clone(), slot.unique_id()))
                .collect(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &PureDeclarationControl,
        )
        .unwrap();
        let factory = UnpivotFixtureFactory::new_from_static_layout(
            9,
            Arc::new(ExprArena::default()),
            vec![],
            SlotId::new(13),
            vec![],
            vec![UnpivotValueMapping {
                input_value_slot_id: SlotId::new(1),
                constants: vec![],
            }],
            layout,
            128,
            1 << 20,
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        operator.set_construction_factory(actual_unpivot_creator(&state, tracker.clone()));
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        let source =
            typed_declaration(&[(1, "value", FunctionValueType::new(DataType::Utf8, false))]);
        let input = Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::fragment::ExecutionFailure,
            _,
        >(
            source,
            vec![Arc::new(StringArray::from(vec!["{}"]))],
            1,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
        let error = TestDriver::new(operator.as_mut(), &state)
            .input_and_collect(input)
            .unwrap_err();
        assert!(
            matches!(error.cause(),crate::runtime::fragment::ExecutionFailureCause::Construction(
            ConstructionFailure::InvalidSource(message)) if message=="unpivot complete value binding drift")
        );
        drop(operator);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}
