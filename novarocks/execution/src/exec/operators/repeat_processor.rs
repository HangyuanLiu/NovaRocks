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
//! Repeat processor for grouping-set style row expansion.
//!
//! Responsibilities:
//! - Repeats input rows with per-repeat nulling masks to emulate grouping-set semantics.
//! - Maintains repeat-id aware output layout required by downstream aggregation.
//!
//! Key exported interfaces:
//! - Types: `RepeatProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::fragment::ExecutionResult;

use std::collections::HashSet;
use std::sync::Arc;

use crate::exec::chunk::{Chunk, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::{Int64Array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_types::SlotId;

/// Mirror the output contract used by the running Repeat operator when a
/// fragment is frozen, including grouping slots appended after its input.
pub(crate) fn repeat_output_chunk_schema(
    input: &ChunkSchemaRef,
    null_slot_ids: &[Vec<SlotId>],
    grouping_slot_ids: &[SlotId],
) -> Result<ChunkSchemaRef, String> {
    let nullable_slot_ids: HashSet<SlotId> = null_slot_ids
        .iter()
        .flat_map(|slots| slots.iter().copied())
        .collect();
    let mut slots = Vec::with_capacity(input.slots().len() + grouping_slot_ids.len());
    for slot in input.slots() {
        slots.push(
            slot.with_nullable(nullable_slot_ids.contains(&slot.slot_id()) || slot.nullable()),
        );
    }
    for (index, slot_id) in grouping_slot_ids.iter().enumerate() {
        slots.push(ChunkSlotSchema::try_new_with_field(
            *slot_id,
            Field::new(format!("repeat_grouping_{index}"), DataType::Int64, true),
            None,
            None,
        )?);
    }
    Ok(Arc::new(crate::exec::chunk::ChunkSchema::try_new(slots)?))
}

/// Author only Repeat's original root-NULL widening and fixed GROUPING fields.
/// The caller owns two actual ports: the null-membership table is temporary;
/// fields, immutable types and the returned schema retain their creating scope.
pub(crate) fn repeat_output_chunk_schema_observed<'a, E>(
    input_schema: &'a arrow::datatypes::Schema,
    input_slots: &[SlotId],
    binding_at: impl Fn(usize) -> Option<(&'a novarocks_type_contract::TypedValueBinding, Option<i32>)>,
    input_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    null_slot_ids: &[Vec<SlotId>],
    grouping_slot_ids: &[SlotId],
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
    mut temporary: impl FnMut(std::alloc::Layout) -> Result<(), E>,
    mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
    mut work: impl FnMut(usize) -> Result<(), E>,
) -> Result<ChunkSchemaRef, E>
where
    E: From<String> + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
{
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef,
        owned_resources::{
            hashmap,
            layout::{LayoutResourceError, arc_layout},
        },
    };
    use std::{alloc::Layout, fmt::Write};
    work(0)?;
    if input_schema.fields().len() != input_slots.len() {
        return Err(E::from(
            "Repeat source requires exact declared channels".to_string(),
        ));
    }
    let mut occurrences = 0usize;
    for ids in null_slot_ids {
        work(1)?;
        occurrences = occurrences
            .checked_add(ids.len())
            .ok_or(LayoutResourceError::Arithmetic)?;
    }
    // std HashSet delegates to hashbrown HashMap<T, ()>. The explicit one-shot
    // reserve uses the sole locked RawTable author, not a private layout mirror.
    let table =
        hashmap::fresh_table_layout::<SlotId, ()>(occurrences).map_err(|cause| match cause {
            hashmap::HashMapResourceError::Arithmetic(_) => LayoutResourceError::Arithmetic,
            hashmap::HashMapResourceError::SourceModel(_) => LayoutResourceError::SourceModel,
        })?;
    if let Some(layout) = table.layout {
        temporary(layout)?;
    }
    work(0)?;
    let mut nullable_slots = HashSet::with_capacity(occurrences);
    for ids in null_slot_ids {
        for slot in ids {
            work(1)?;
            nullable_slots.insert(*slot);
        }
    }
    let count = input_slots
        .len()
        .checked_add(grouping_slot_ids.len())
        .ok_or(LayoutResourceError::Arithmetic)?;
    request(Layout::array::<ChunkSlotSchema>(count).map_err(|_| LayoutResourceError::Arithmetic)?)?;
    let storage = storage.retaining_observed(&input_storage, &mut request)?;
    let mut slots = Vec::with_capacity(count);
    for (ordinal, slot_id) in input_slots.iter().enumerate() {
        work(1)?;
        let (original, unique_id) = binding_at(ordinal)
            .ok_or_else(|| E::from("Repeat source requires complete value bindings".to_string()))?;
        let field = &input_schema.fields()[ordinal];
        if field.is_nullable() != original.nullable() {
            return Err(E::from(
                "Repeat source NULL declaration differs from its binding".to_string(),
            ));
        }
        let nullable = original.nullable() || nullable_slots.contains(slot_id);
        let binding =
            original.with_nullable_observed(nullable, storage.clone(), &mut request, &mut work)?;
        let field = if nullable == original.nullable() {
            Arc::clone(field)
        } else {
            request(
                Layout::array::<u8>(field.name().len())
                    .map_err(|_| LayoutResourceError::Arithmetic)?,
            )?;
            request(arc_layout(Layout::new::<Field>())?)?;
            work(0)?;
            Arc::new(field.as_ref().clone().with_nullable(nullable))
        };
        slots.push(ChunkSlotSchema::try_new_typed_observed(
            *slot_id,
            field,
            binding,
            unique_id,
            storage.clone(),
            |step| match step {
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units } => {
                    work(units)
                }
                _ => work(1),
            },
        )?);
    }
    for (index, id) in grouping_slot_ids.iter().enumerate() {
        work(1)?;
        // The original producer declares nullable Int64; values remain the
        // frozen grouping_list entries, rather than an inferred array domain.
        request(ValueTypeRef::storage_allocation_layout()?)?;
        work(0)?;
        let value =
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int64), true, storage.clone())
                .map_err(|cause| E::from(cause.to_string()))?;
        request(ValueLayout::storage_allocation_layout()?)?;
        work(0)?;
        let layout = ValueLayout::try_new(&DataType::Int64, storage.clone())
            .map_err(|cause| E::from(cause.to_string()))?;
        let binding = TypedValueBinding::try_new(value, layout)
            .map_err(|cause| E::from(cause.to_string()))?;
        let digits = if index == 0 {
            1
        } else {
            index.ilog10() as usize + 1
        };
        let length = "repeat_grouping_"
            .len()
            .checked_add(digits)
            .ok_or(LayoutResourceError::Arithmetic)?;
        request(Layout::array::<u8>(length).map_err(|_| LayoutResourceError::Arithmetic)?)?;
        request(arc_layout(Layout::new::<Field>())?)?;
        work(0)?;
        let mut name = String::with_capacity(length);
        name.push_str("repeat_grouping_");
        write!(&mut name, "{index}")
            .expect("the pre-sized String accepts the original grouping name");
        slots.push(ChunkSlotSchema::try_new_typed_observed(
            *id,
            Arc::new(Field::new(name, DataType::Int64, true)),
            binding,
            None,
            storage.clone(),
            |step| match step {
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units } => {
                    work(units)
                }
                _ => work(1),
            },
        )?);
    }
    let schema = crate::exec::chunk::ChunkSchema::try_new_typed_observed(
        slots,
        storage,
        &mut request,
        &mut work,
    )?;
    request(arc_layout(Layout::new::<crate::exec::chunk::ChunkSchema>())?)?;
    work(0)?;
    Ok(Arc::new(schema))
}

/// Factory for repeat processors that expand rows for grouping-set style execution.
pub struct RepeatProcessorFactory {
    name: String,
    null_slot_ids: Vec<Vec<SlotId>>,
    grouping_slot_ids: Vec<SlotId>,
    grouping_list: Vec<Vec<i64>>,
    repeat_times: usize,
    input_layout: Option<novarocks_local_program::StaticLayout>,
    output_layout: Option<novarocks_local_program::StaticLayout>,
    declared_input: Option<ChunkSchemaRef>,
}

impl RepeatProcessorFactory {
    pub fn new(
        node_id: i32,
        null_slot_ids: Vec<Vec<SlotId>>,
        grouping_slot_ids: Vec<SlotId>,
        grouping_list: Vec<Vec<i64>>,
        repeat_times: usize,
    ) -> Self {
        let name = if node_id >= 0 {
            format!("Repeat (id={node_id})")
        } else {
            "Repeat".to_string()
        };
        Self {
            name,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
            input_layout: None,
            output_layout: None,
            declared_input: None,
        }
    }

    pub(crate) fn new_from_static_layouts(
        node_id: i32,
        null_slot_ids: Vec<Vec<SlotId>>,
        grouping_slot_ids: Vec<SlotId>,
        grouping_list: Vec<Vec<i64>>,
        repeat_times: usize,
        input: novarocks_local_program::StaticLayout,
        output: novarocks_local_program::StaticLayout,
    ) -> Result<Self, String> {
        if !input.is_typed() || !output.is_typed() {
            return Err("Repeat requires complete input and output declarations".into());
        }
        let mut factory = Self::new(
            node_id,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
        );
        factory.input_layout = Some(input);
        factory.output_layout = Some(output);
        Ok(factory)
    }

    pub(crate) fn new_from_declared_input(
        node_id: i32,
        null_slot_ids: Vec<Vec<SlotId>>,
        grouping_slot_ids: Vec<SlotId>,
        grouping_list: Vec<Vec<i64>>,
        repeat_times: usize,
        input: ChunkSchemaRef,
    ) -> Result<Self, String> {
        if !input.is_typed() {
            return Err("Repeat construction requires a complete input declaration".into());
        }
        let mut factory = Self::new(
            node_id,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
        );
        factory.declared_input = Some(input);
        Ok(factory)
    }
}

impl OperatorFactory for RepeatProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(RepeatProcessorOperator {
            name: self.name.clone(),
            null_slot_ids: self.null_slot_ids.clone(),
            grouping_slot_ids: self.grouping_slot_ids.clone(),
            grouping_list: self.grouping_list.clone(),
            repeat_times: self.repeat_times,

            input: None,
            repeat_idx: 0,
            finishing: false,
            finished: false,
            emit_empty_once: false,
            output_schema: None,
            output_chunk_schema: None,
            input_layout: self.input_layout.clone(),
            output_layout: self.output_layout.clone(),
            declared_input: self.declared_input.clone(),
            construction: None,
        })
    }
}

struct RepeatProcessorOperator {
    name: String,
    null_slot_ids: Vec<Vec<SlotId>>,
    grouping_slot_ids: Vec<SlotId>,
    grouping_list: Vec<Vec<i64>>,
    repeat_times: usize,

    input: Option<Chunk>,
    repeat_idx: usize,
    finishing: bool,
    finished: bool,
    emit_empty_once: bool,
    output_schema: Option<Arc<Schema>>,
    output_chunk_schema: Option<ChunkSchemaRef>,
    input_layout: Option<novarocks_local_program::StaticLayout>,
    output_layout: Option<novarocks_local_program::StaticLayout>,
    declared_input: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}

impl Operator for RepeatProcessorOperator {
    fn creates_typed_storage(&self) -> bool {
        self.input_layout.is_some() || self.declared_input.is_some()
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        if self.output_chunk_schema.is_some() {
            return Ok(());
        }
        if self.input_layout.is_none() && self.declared_input.is_none() {
            return Ok(());
        }
        self.validate_config()?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        factory.observe_input_work(0)?;
        let output = if let (Some(input), Some(output)) = (&self.input_layout, &self.output_layout)
        {
            let temporary = factory.begin_compiled_layout_construction(input)?;
            validate_repeat_layouts(
                input,
                output,
                &self.null_slot_ids,
                &self.grouping_slot_ids,
                temporary.as_ref(),
            )?;
            // This node's frozen output is authoritative. A runtime schema
            // creation may not replace its names or conservative NULL promises.
            factory.create_compiled_chunk_schema(output)?
        } else {
            let input = self.declared_input.as_ref().expect("validated declaration");
            let retained = factory.begin_typed_construction(input)?;
            let temporary = factory.begin_typed_construction(input)?;
            let arrow = input.arrow_schema_ref();
            repeat_output_chunk_schema_observed::<ConstructionFailure>(
                arrow.as_ref(),
                input.slot_ids(),
                |ordinal| {
                    input
                        .slots()
                        .get(ordinal)
                        .map(|slot| (slot.typed_binding(), slot.unique_id()))
                },
                input.storage_lifetime(),
                &self.null_slot_ids,
                &self.grouping_slot_ids,
                retained.storage_lifetime(),
                |layout| temporary.request(layout),
                |layout| retained.request(layout),
                |units| {
                    temporary.observe(
                        0,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                            units,
                        },
                    )
                },
            )?
        };
        self.output_schema = Some(output.arrow_schema_ref());
        self.output_chunk_schema = Some(output);
        Ok(())
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

impl ProcessorOperator for RepeatProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.input.is_none()
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        if self.emit_empty_once {
            return true;
        }
        self.input.is_some() && self.repeat_idx < self.repeat_times
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if !self.need_input() {
            return Err("repeat push_chunk called when operator does not need input"
                .to_string()
                .into());
        }
        self.validate_config()?;
        if self.input_layout.is_some() || self.declared_input.is_some() {
            use crate::exec::pipeline::operator::ConstructionFailure;
            self.output_chunk_schema
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            if let Some(input) = &self.input_layout {
                validate_repeat_input(
                    &chunk,
                    input.schema().as_ref(),
                    input.slots(),
                    |ordinal| input.value_binding_at(ordinal),
                    factory.as_ref(),
                )?;
            } else {
                let input = self.declared_input.as_ref().expect("validated declaration");
                let arrow = input.arrow_schema_ref();
                validate_repeat_input(
                    &chunk,
                    arrow.as_ref(),
                    input.slot_ids(),
                    |ordinal| input.slots().get(ordinal).map(|slot| slot.typed_binding()),
                    factory.as_ref(),
                )?;
            }
            self.emit_empty_once = chunk.is_empty();
            self.input = Some(chunk);
            self.repeat_idx = 0;
            return Ok(());
        }
        if chunk.chunk_schema().is_typed() {
            return Err(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into());
        }

        if chunk.is_empty() {
            self.input = Some(chunk);
            self.repeat_idx = 0;
            self.emit_empty_once = true;
            self.output_schema = None;
            self.output_chunk_schema = None;
            return Ok(());
        }

        let output = repeat_output_chunk_schema(
            &chunk.chunk_schema_ref(),
            &self.null_slot_ids,
            &self.grouping_slot_ids,
        )?;
        self.output_schema = Some(output.arrow_schema_ref());
        self.output_chunk_schema = Some(output);

        self.input = Some(chunk);
        self.repeat_idx = 0;
        self.emit_empty_once = false;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        if !self.has_output() {
            return Ok(None);
        }
        if self.input_layout.is_some() || self.declared_input.is_some() {
            let source = self.input.as_ref().ok_or_else(|| {
                crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                    "Repeat input disappeared".into(),
                )
            })?;
            let output = self
                .output_chunk_schema
                .as_ref()
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            let factory = self
                .construction
                .as_ref()
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            let set = self.repeat_idx;
            let nulls =
                self.null_slot_ids[set].iter().map(|slot| {
                    source.slot_id_to_index().get(slot).copied().ok_or_else(|| {
                        format!("repeat null slot id {} not found in input chunk", slot)
                    })
                });
            let groups = self
                .grouping_list
                .iter()
                .enumerate()
                .map(|(index, values)| {
                    values.get(set).copied().ok_or_else(|| {
                        format!(
                            "repeat grouping_list[{}] missing value at repeat_idx={}",
                            index, set
                        )
                    })
                });
            let produced = repeat_output_columns_owned(
                source,
                output,
                nulls,
                groups,
                factory.as_ref(),
                |value, rows| {
                    Arc::new(Int64Array::from_iter(std::iter::repeat_n(
                        Some(value),
                        rows,
                    )))
                },
            )?;
            self.repeat_idx += 1;
            if self.emit_empty_once || self.repeat_idx >= self.repeat_times {
                self.emit_empty_once = false;
                self.input = None;
                if self.finishing {
                    self.finished = true;
                }
            }
            return Ok(Some(produced));
        }
        if self.emit_empty_once {
            self.emit_empty_once = false;
            self.input = None;
            if self.finishing {
                self.finished = true;
            }
            return Ok(Some(Chunk::default()));
        }

        self.validate_config()?;
        let Some(chunk) = self.input.as_ref() else {
            return Ok(None);
        };
        if self.repeat_idx >= self.repeat_times {
            return Ok(None);
        }

        let _schema = self
            .output_schema
            .as_ref()
            .ok_or_else(|| "repeat missing output schema; push_chunk was not called".to_string())?;
        let output_chunk_schema = self.output_chunk_schema.as_ref().ok_or_else(|| {
            "repeat missing output chunk schema; push_chunk was not called".to_string()
        })?;
        let num_rows = chunk.len();
        let mut columns = chunk.record_batch().columns().to_vec();

        for slot_id in &self.null_slot_ids[self.repeat_idx] {
            let col_idx = chunk
                .slot_id_to_index()
                .get(slot_id)
                .copied()
                .ok_or_else(|| {
                    format!("repeat null slot id {} not found in input chunk", slot_id)
                })?;
            let dt = columns[col_idx].data_type().clone();
            columns[col_idx] = new_null_array(&dt, num_rows);
        }

        for (gidx, values) in self.grouping_list.iter().enumerate() {
            let v = values.get(self.repeat_idx).copied().ok_or_else(|| {
                format!(
                    "repeat grouping_list[{}] missing value at repeat_idx={}",
                    gidx, self.repeat_idx
                )
            })?;
            let arr = Int64Array::from_iter(std::iter::repeat_n(Some(v), num_rows));
            columns.push(Arc::new(arr) as _);
        }

        self.repeat_idx += 1;
        if self.repeat_idx >= self.repeat_times {
            self.input = None;
            if self.finishing {
                self.finished = true;
            }
        }
        Ok(Some(
            Chunk::try_new_with_columns(Arc::clone(output_chunk_schema), columns)
                .map_err(|e| format!("repeat build batch failed: {e}"))?,
        ))
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.input.is_none() && !self.emit_empty_once {
            self.finished = true;
        }
        Ok(())
    }
}

impl RepeatProcessorOperator {
    fn validate_config(&self) -> Result<(), String> {
        if self.repeat_times == 0 {
            return Err("repeat operator requires repeat_times > 0".to_string());
        }
        if self.null_slot_ids.len() != self.repeat_times {
            return Err(format!(
                "repeat null_slot_ids size mismatch: expected={} actual={}",
                self.repeat_times,
                self.null_slot_ids.len()
            ));
        }
        if self.grouping_slot_ids.len() != self.grouping_list.len() {
            return Err(format!(
                "repeat grouping slot ids mismatch: slot_ids={} grouping_list={}",
                self.grouping_slot_ids.len(),
                self.grouping_list.len()
            ));
        }
        for (idx, row) in self.grouping_list.iter().enumerate() {
            if row.len() != self.repeat_times {
                return Err(format!(
                    "repeat grouping_list[{}] size mismatch: expected={} actual={}",
                    idx,
                    self.repeat_times,
                    row.len()
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::runtime_state::RuntimeState;

    use arrow::array::Array;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;

    fn schema_for(slot_ids: &[SlotId]) -> SchemaRef {
        let mut fields = Vec::new();
        for (idx, _slot_id) in slot_ids.iter().enumerate() {
            let field = Field::new(format!("c{idx}"), DataType::Int32, true);
            fields.push(Arc::new(field));
        }
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn repeat_expands_and_nulls_slots() {
        let schema = schema_for(&[SlotId::new(1), SlotId::new(2)]);
        let a = Arc::new(Int32Array::from(vec![Some(10), Some(20)])) as _;
        let b = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as _;
        let batch = RecordBatch::try_new(schema, vec![a, b]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let mut op = RepeatProcessorOperator {
            name: "Repeat".to_string(),
            null_slot_ids: vec![vec![SlotId::new(2)], vec![SlotId::new(1)]],
            grouping_slot_ids: vec![SlotId::new(10)],
            grouping_list: vec![vec![0, 1]],
            repeat_times: 2,

            input: None,
            repeat_idx: 0,
            finishing: false,
            finished: false,
            emit_empty_once: false,
            output_schema: None,
            output_chunk_schema: None,
            input_layout: None,
            output_layout: None,
            declared_input: None,
            construction: None,
        };

        let state = RuntimeState::default();
        op.push_chunk(&state, chunk).expect("push_chunk");

        let out1 = op.pull_chunk(&state).expect("pull1").expect("chunk1");
        let out2 = op.pull_chunk(&state).expect("pull2").expect("chunk2");
        assert!(op.pull_chunk(&state).expect("pull3").is_none());

        assert_eq!(out1.len(), 2);
        assert_eq!(out2.len(), 2);

        let a1 = out1.column_by_slot_id(SlotId::new(1)).expect("slot 1");
        let b1 = out1.column_by_slot_id(SlotId::new(2)).expect("slot 2");
        let g1 = out1.column_by_slot_id(SlotId::new(10)).expect("slot 10");

        let a1 = a1.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let b1 = b1.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let g1 = g1.as_any().downcast_ref::<Int64Array>().expect("Int64");

        // First repeat: slot 2 is NULL, grouping=0
        assert_eq!(a1.value(0), 10);
        assert_eq!(a1.value(1), 20);
        assert!(b1.is_null(0));
        assert!(b1.is_null(1));
        assert_eq!(g1.value(0), 0);
        assert_eq!(g1.value(1), 0);

        let a2 = out2.column_by_slot_id(SlotId::new(1)).expect("slot 1");
        let b2 = out2.column_by_slot_id(SlotId::new(2)).expect("slot 2");
        let g2 = out2.column_by_slot_id(SlotId::new(10)).expect("slot 10");

        let a2 = a2.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let b2 = b2.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let g2 = g2.as_any().downcast_ref::<Int64Array>().expect("Int64");

        // Second repeat: slot 1 is NULL, grouping=1
        assert!(a2.is_null(0));
        assert!(a2.is_null(1));
        assert_eq!(b2.value(0), 1);
        assert_eq!(b2.value(1), 2);
        assert_eq!(g2.value(0), 1);
        assert_eq!(g2.value(1), 1);
    }
}

#[cfg(test)]
mod typed_owner_tests {
    use super::*;
    use crate::exec::chunk::typed_fixture;
    use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
    use crate::runtime::{
        fragment::{ExecutionFailure, ExecutionFailureCause},
        mem_tracker::MemTracker,
        runtime_state::RuntimeErrorState,
    };
    use arrow::array::{Array, ListArray, StringArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use novarocks_type_contract::{
        FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };

    fn list_type(kind: ValueKind, root_nullable: bool, child_nullable: bool) -> FunctionValueType {
        let storage = ValueTypeStorageLifetime::pure();
        let child = ValueTypeRef::try_new(kind, child_nullable, storage.clone()).unwrap();
        let value =
            ValueTypeRef::try_new(ValueKind::List(child), root_nullable, storage.clone()).unwrap();
        let carrier = DataType::List(Arc::new(Field::new("", DataType::Utf8, child_nullable)));
        FunctionValueType::try_from_value(value, ValueLayout::try_new(&carrier, storage).unwrap())
            .unwrap()
    }
    fn layouts(
        child: ValueKind,
        output_child_nullable: bool,
    ) -> (
        novarocks_local_program::StaticLayout,
        novarocks_local_program::StaticLayout,
    ) {
        let input = list_type(child.clone(), false, false);
        let output = list_type(child, true, output_child_nullable);
        let grouping = FunctionValueType::new(DataType::Int64, true);
        (
            typed_fixture::layout(
                Arc::new(Schema::new(vec![Field::new(
                    "document",
                    input.data_type().clone(),
                    false,
                )])),
                &[SlotId::new(1)],
                &[input],
            ),
            typed_fixture::layout(
                Arc::new(Schema::new(vec![
                    Field::new("document", output.data_type().clone(), true),
                    Field::new("group", DataType::Int64, true),
                ])),
                &[SlotId::new(1), SlotId::new(10)],
                &[output, grouping],
            ),
        )
    }
    fn source(kind: ValueKind) -> Chunk {
        let ty = list_type(kind, false, false);
        let DataType::List(field) = ty.data_type() else {
            unreachable!()
        };
        let array = ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
            Arc::new(StringArray::from(vec!["{\"n\":1}", "{\"n\":2}"])),
            None,
        )
        .unwrap();
        typed_fixture::columns(
            &["document"],
            &[SlotId::new(1)],
            &[ty],
            vec![Arc::new(array)],
            2,
        )
    }
    fn factory() -> (
        Arc<MemTracker>,
        Arc<RuntimeErrorState>,
        Arc<dyn InputConstructionFactory>,
    ) {
        let tracker = MemTracker::new_root("repeat actual structural owner");
        let errors = Arc::new(RuntimeErrorState::default());
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            errors.clone(),
        )
        .unwrap();
        (tracker, errors, factory)
    }
    fn operator(
        input: novarocks_local_program::StaticLayout,
        output: novarocks_local_program::StaticLayout,
    ) -> Box<dyn Operator> {
        RepeatProcessorFactory::new_from_static_layouts(
            71,
            vec![vec![SlotId::new(1)], vec![]],
            vec![SlotId::new(10)],
            vec![vec![4, 7]],
            2,
            input,
            output,
        )
        .unwrap()
        .create(1, 0)
    }

    #[test]
    fn actual_repeat_preserves_nested_domain_masks_and_last_independent_holder() {
        let json = ValueKind::Nominal(ValueLogicalType::Json);
        let (input, output) = layouts(json.clone(), false);
        let (tracker, errors, factory) = factory();
        let weak_factory = Arc::downgrade(&factory);
        let mut op = operator(input, output);
        assert!(op.creates_typed_storage());
        op.set_construction_factory(factory.clone());
        op.prepare().unwrap();
        let state = RuntimeState::default();
        op.as_processor_mut()
            .unwrap()
            .push_chunk(&state, source(json))
            .unwrap();
        let nulled = op
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        let kept = op
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        assert!(
            op.as_processor_mut()
                .unwrap()
                .pull_chunk(&state)
                .unwrap()
                .is_none()
        );
        assert_eq!((nulled.len(), kept.len()), (2, 2));
        assert_eq!(
            nulled
                .column_by_slot_id(SlotId::new(1))
                .unwrap()
                .null_count(),
            2
        );
        let actual_column = kept.column_by_slot_id(SlotId::new(1)).unwrap();
        let actual = actual_column.as_any().downcast_ref::<ListArray>().unwrap();
        let values = actual
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (values.value(0), values.value(1)),
            ("{\"n\":1}", "{\"n\":2}")
        );
        for (chunk, group) in [(&nulled, 4), (&kept, 7)] {
            let schema = chunk.chunk_schema();
            let slot = &schema.slots()[0];
            assert!(slot.typed_binding().nullable());
            let ValueKind::List(child) = slot.typed_binding().value_type().kind() else {
                panic!("complete List")
            };
            assert!(matches!(
                child.kind(),
                ValueKind::Nominal(ValueLogicalType::Json)
            ));
            assert!(!child.nullable());
            assert_eq!(schema.arrow_schema_ref().metadata().capacity(), 0);
            assert_eq!(slot.field().metadata().capacity(), 0);
            let DataType::List(field) = slot.field().data_type() else {
                panic!("List carrier")
            };
            assert_eq!(field.metadata().capacity(), 0);
            let grouping_column = chunk.column_by_slot_id(SlotId::new(10)).unwrap();
            let groups = grouping_column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!((groups.value(0), groups.value(1)), (group, group));
        }
        // The original default processor emits exactly one empty event. Its
        // typed sibling preserves the already frozen channels, not Chunk::default.
        let empty_source = source(ValueKind::Nominal(ValueLogicalType::Json));
        let owner = factory
            .begin_typed_construction(empty_source.chunk_schema())
            .unwrap();
        let empty = empty_source
            .slice_typed_observed::<ConstructionFailure, _>(
                0,
                0,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )
            .unwrap();
        drop(owner);
        drop(empty_source);
        op.as_processor_mut()
            .unwrap()
            .push_chunk(&state, empty)
            .unwrap();
        let empty = op
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.columns().len(), 2);
        assert_eq!(
            empty.chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(10)]
        );
        assert_eq!(empty.record_batch().schema(), kept.record_batch().schema());
        assert!(empty.chunk_schema().is_typed());
        assert!(
            op.as_processor_mut()
                .unwrap()
                .pull_chunk(&state)
                .unwrap()
                .is_none()
        );
        drop(empty);
        let late_batch = kept.batch_projection();
        let late_slot = kept.chunk_schema().slots()[0].clone();
        drop(kept);
        drop(nulled);
        drop(op);
        drop(factory);
        drop(errors);
        drop(state);
        assert!(
            weak_factory.upgrade().is_none(),
            "output does not retain its factory"
        );
        assert!(
            tracker.current() > 0,
            "the independently returned backing is still owned"
        );
        assert_eq!(late_batch.len(), 2);
        drop(late_batch);
        assert!(
            tracker.current() > 0,
            "the independent slot still owns its schema creation"
        );
        assert!(late_slot.is_typed());
        drop(late_slot);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn actual_repeat_missing_owner_and_first_memory_or_caller_have_no_request_tail() {
        let (input, output) = layouts(ValueKind::Nominal(ValueLogicalType::Json), false);
        let mut missing = operator(input.clone(), output);
        assert!(matches!(
            missing.prepare().unwrap_err().cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
        ));
        drop(missing);
        let (tracker, errors, factory) = factory();
        let declared = factory.create_compiled_chunk_schema(&input).unwrap();
        let retained = factory.begin_typed_construction(&declared).unwrap();
        let temporary = factory.begin_typed_construction(&declared).unwrap();
        let prior = tracker.current();
        tracker.install_limit_once(prior + 1).unwrap();
        let mut temporary_requests = 0;
        let mut retained_requests = 0;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let work_calls = AtomicUsize::new(0);
        let work_at_refusal = AtomicUsize::new(usize::MAX);
        let arrow = declared.arrow_schema_ref();
        let result = repeat_output_chunk_schema_observed::<ConstructionFailure>(
            arrow.as_ref(),
            declared.slot_ids(),
            |at| {
                declared
                    .slots()
                    .get(at)
                    .map(|slot| (slot.typed_binding(), slot.unique_id()))
            },
            declared.storage_lifetime(),
            &[vec![SlotId::new(1)]],
            &[SlotId::new(10)],
            retained.storage_lifetime(),
            |layout| {
                temporary_requests += 1;
                let result = temporary.request(layout);
                if result.is_err() {
                    work_at_refusal.store(work_calls.load(Ordering::Relaxed), Ordering::Relaxed);
                }
                result
            },
            |layout| {
                retained_requests += 1;
                retained.request(layout)
            },
            |units| {
                work_calls.fetch_add(1, Ordering::Relaxed);
                let result = temporary.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
                );
                if result.is_err() {
                    work_at_refusal.store(work_calls.load(Ordering::Relaxed), Ordering::Relaxed);
                }
                result
            },
        );
        assert!(matches!(
            result.err().unwrap(),
            ConstructionFailure::Memory(_)
        ));
        assert_eq!((temporary_requests, retained_requests), (1, 0));
        assert_eq!(
            work_calls.load(Ordering::Relaxed),
            work_at_refusal.load(Ordering::Relaxed),
            "no control callback after first Memory refusal"
        );
        assert_eq!(
            tracker.current(),
            prior,
            "rejected request adds no fee or tail allocation"
        );
        let caller = ExecutionFailure::from("repeat original caller");
        errors.set_error(caller.clone());
        temporary_requests = 0;
        retained_requests = 0;
        work_calls.store(0, Ordering::Relaxed);
        work_at_refusal.store(usize::MAX, Ordering::Relaxed);
        let result = repeat_output_chunk_schema_observed::<ConstructionFailure>(
            arrow.as_ref(),
            declared.slot_ids(),
            |at| {
                declared
                    .slots()
                    .get(at)
                    .map(|slot| (slot.typed_binding(), slot.unique_id()))
            },
            declared.storage_lifetime(),
            &[vec![SlotId::new(1)]],
            &[SlotId::new(10)],
            retained.storage_lifetime(),
            |layout| {
                temporary_requests += 1;
                let result = temporary.request(layout);
                if result.is_err() {
                    work_at_refusal.store(work_calls.load(Ordering::Relaxed), Ordering::Relaxed);
                }
                result
            },
            |layout| {
                retained_requests += 1;
                retained.request(layout)
            },
            |units| {
                work_calls.fetch_add(1, Ordering::Relaxed);
                let result = temporary.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
                );
                if result.is_err() {
                    work_at_refusal.store(work_calls.load(Ordering::Relaxed), Ordering::Relaxed);
                }
                result
            },
        );
        assert!(
            matches!(result.err().unwrap(), ConstructionFailure::Caller(cause) if *cause == caller)
        );
        assert_eq!((temporary_requests, retained_requests), (0, 0));
        assert_eq!(
            work_calls.load(Ordering::Relaxed),
            work_at_refusal.load(Ordering::Relaxed),
            "no callback after original Caller refusal"
        );
        drop(arrow);
        drop(temporary);
        drop(retained);
        drop(declared);
        drop(factory);
        drop(errors);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn actual_repeat_rejects_same_carrier_wrong_child_and_child_nullability() {
        let (input, output) = layouts(ValueKind::Nominal(ValueLogicalType::Json), false);
        let (tracker, errors, factory) = factory();
        let mut op = operator(input.clone(), output);
        op.set_construction_factory(factory.clone());
        op.prepare().unwrap();
        let error = op
            .as_processor_mut()
            .unwrap()
            .push_chunk(&RuntimeState::default(), source(ValueKind::String))
            .unwrap_err();
        assert!(matches!(error.cause(), ExecutionFailureCause::Construction(
            ConstructionFailure::InvalidSource(detail)) if detail.contains("complete declared type")));
        drop(op);
        let (_, changed_children) = layouts(ValueKind::Nominal(ValueLogicalType::Json), true);
        let mut op = operator(input, changed_children);
        op.set_construction_factory(factory.clone());
        let error = op.prepare().unwrap_err();
        assert!(matches!(error.cause(), ExecutionFailureCause::Construction(
            ConstructionFailure::InvalidSource(detail)) if detail.contains("complete children")));
        drop(op);
        drop(factory);
        drop(errors);
        assert_eq!(tracker.current(), 0);
    }
}

fn validate_repeat_layouts(
    input: &novarocks_local_program::StaticLayout,
    output: &novarocks_local_program::StaticLayout,
    null_slot_ids: &[Vec<SlotId>],
    grouping_slots: &[SlotId],
    temporary: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> ExecutionResult<()> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::{
        ValueKind,
        owned_resources::{hashmap, layout::LayoutResourceError},
    };
    temporary.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 0 },
    )?;
    let occurrences = null_slot_ids.iter().try_fold(0usize, |count, slots| {
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )?;
        count
            .checked_add(slots.len())
            .ok_or(ConstructionFailure::Resource(
                LayoutResourceError::Arithmetic,
            ))
    })?;
    let table = hashmap::fresh_table_layout::<SlotId, ()>(occurrences)
        .map_err(|cause| match cause {
            hashmap::HashMapResourceError::Arithmetic(_) => LayoutResourceError::Arithmetic,
            hashmap::HashMapResourceError::SourceModel(_) => LayoutResourceError::SourceModel,
        })
        .map_err(ConstructionFailure::Resource)?;
    if let Some(layout) = table.layout {
        temporary.request(layout)?;
    }
    let mut nullable = HashSet::with_capacity(occurrences);
    for slots in null_slot_ids {
        for slot in slots {
            temporary.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
            )?;
            nullable.insert(*slot);
        }
    }
    let width = input.slots().len();
    let count = width
        .checked_add(grouping_slots.len())
        .ok_or(ConstructionFailure::Resource(
            LayoutResourceError::Arithmetic,
        ))?;
    if output.slots().len() != count
        || output.slots()[..width] != *input.slots()
        || output.slots()[width..] != *grouping_slots
    {
        return Err(ConstructionFailure::InvalidSource(
            "Repeat frozen channels differ from input and GROUPING outputs".into(),
        )
        .into());
    }
    for (ordinal, slot) in input.slots().iter().enumerate() {
        let source = input.value_binding_at(ordinal).ok_or_else(|| {
            ConstructionFailure::InvalidSource("Repeat input has no complete binding".into())
        })?;
        let target = output.value_binding_at(ordinal).ok_or_else(|| {
            ConstructionFailure::InvalidSource("Repeat output has no complete binding".into())
        })?;
        if (source.nullable() || nullable.contains(slot)) && !target.nullable()
            || !source
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(
                    target.borrowed(),
                    || {
                        temporary.observe(
                            0,
                            &[],
                            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                                units: 1,
                            },
                        )
                    },
                )?
        {
            return Err(ConstructionFailure::InvalidSource(
                "Repeat changes complete children or narrows the required NULL extension".into(),
            )
            .into());
        }
    }
    for ordinal in width..count {
        let target = output.value_binding_at(ordinal).ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "Repeat GROUPING output has no complete binding".into(),
            )
        })?;
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )?;
        if !matches!(
            target.value_type().kind(),
            ValueKind::Scalar(DataType::Int64)
        ) {
            return Err(ConstructionFailure::InvalidSource(
                "Repeat GROUPING output is not the declared Int64 domain".into(),
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn validate_repeat_input<'a>(
    chunk: &Chunk,
    schema: &arrow::datatypes::Schema,
    slots: &[SlotId],
    binding_at: impl Fn(usize) -> Option<&'a novarocks_type_contract::TypedValueBinding>,
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
) -> ExecutionResult<()> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    factory.observe_input_work(0)?;
    if !chunk.chunk_schema().is_typed()
        || chunk.chunk_schema().slot_ids() != slots
        || chunk.record_batch().schema().as_ref() != schema
    {
        return Err(ConstructionFailure::InvalidSource(
            "Repeat input differs from its complete frozen port".into(),
        )
        .into());
    }
    for (ordinal, slot) in chunk.chunk_schema().slots().iter().enumerate() {
        let expected = binding_at(ordinal).ok_or_else(|| {
            ConstructionFailure::InvalidSource("Repeat input has no complete binding".into())
        })?;
        if !slot
            .typed_binding()
            .borrowed()
            .exactly_equals_observed(expected.borrowed(), || factory.observe_input_work(1))?
        {
            return Err(ConstructionFailure::InvalidSource(
                "Repeat input differs from its complete declared type".into(),
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn repeat_output_columns_owned(
    source: &Chunk,
    output: &ChunkSchemaRef,
    null_ordinals: impl IntoIterator<Item = Result<usize, String>>,
    grouping_values: impl IntoIterator<Item = Result<i64, String>>,
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    mut grouping_array: impl FnMut(i64, usize) -> arrow::array::ArrayRef,
) -> ExecutionResult<Chunk> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_functions::selected_copy::{self, CopyConstructionError, CopyError};
    use novarocks_type_contract::ValueSchemaCreation;
    use std::alloc::Layout;
    fn failure(
        cause: CopyConstructionError<ConstructionFailure>,
    ) -> crate::runtime::fragment::ExecutionFailure {
        match cause {
            CopyConstructionError::Creator(cause) => cause.into(),
            CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
            CopyConstructionError::Copy(CopyError::Control(cause)) => cause.into(),
            CopyConstructionError::Copy(CopyError::Extent) => {
                novarocks_functions::KernelFailure::ResourceExhausted.into()
            }
            CopyConstructionError::Copy(CopyError::Invalid(message)) => {
                ConstructionFailure::InvalidSource(message.into()).into()
            }
            CopyConstructionError::Copy(CopyError::Unsupported(ty)) => {
                ConstructionFailure::InvalidSource(format!(
                    "Repeat construction carrier {ty:?} is unsupported"
                ))
                .into()
            }
        }
    }
    factory.observe_input_work(0)?;
    let retained = factory.begin_typed_construction(output)?;
    let temporary = factory.begin_typed_construction(output)?;
    retained.request(
        Layout::array::<arrow::array::ArrayRef>(output.slots().len()).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?,
    )?;
    let mut columns = Vec::with_capacity(output.slots().len());
    for column in source.columns() {
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )?;
        columns.push(Arc::clone(column));
    }
    for ordinal in null_ordinals {
        let ordinal = ordinal?;
        let array = columns.get_mut(ordinal).ok_or_else(|| {
            ConstructionFailure::InvalidSource("Repeat NULL ordinal is outside its input".into())
        })?;
        let mut request = |layout| retained.request(layout);
        let mut opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(retained.storage_lifetime(), &mut request, &mut opaque);
        selected_copy::preflight_null_arrow_structure_observed(
            array.data_type(),
            source.len(),
            &mut creation,
            &mut |boundary| {
                temporary.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                        units: usize::from(!boundary),
                    },
                )
            },
        )
        .map_err(failure)?;
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
        *array = new_null_array(array.data_type(), source.len());
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
    }
    for value in grouping_values {
        let value = value?;
        let slot = output.slots().get(columns.len()).ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "Repeat GROUPING ordinal is outside its output".into(),
            )
        })?;
        let mut request = |layout| retained.request(layout);
        let mut opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(retained.storage_lifetime(), &mut request, &mut opaque);
        selected_copy::preflight_flat_array_structure_construction(
            slot.typed_binding().layout(),
            false,
            &mut creation,
            &mut |boundary| {
                temporary.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                        units: usize::from(!boundary),
                    },
                )
            },
        )
        .map_err(failure)?;
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
        columns.push(grouping_array(value, source.len()));
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
    }
    // Preserve original payload responsibility only when an original array is
    // actually returned. New NULL arrays still borrow their source DataType,
    // so retain that finite type backing even when no source buffer survives.
    let mut shares_source = false;
    for (original, produced) in source.columns().iter().zip(&columns) {
        temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )?;
        shares_source |= Arc::ptr_eq(original, produced);
    }
    let storage = if shares_source {
        source.source_storage_lifetime_observed(retained.storage_lifetime(), |layout| {
            retained.request(layout)
        })?
    } else {
        retained
            .storage_lifetime()
            .retaining_observed(&source.array_type_storage_lifetime(), |layout| {
                retained.request(layout)
            })?
    };
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        output.clone(),
        columns,
        source.len(),
        storage,
        |step| retained.observe(0, &[], step),
    )
    .map_err(Into::into)
}
