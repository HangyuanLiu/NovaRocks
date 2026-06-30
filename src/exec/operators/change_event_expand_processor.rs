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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, new_null_array};
use arrow::compute::{concat, filter};
use arrow::datatypes::DataType;

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, cast_array_to_target};
use crate::exec::node::change_event_expand::ChangeEventRuntimeSpec;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for runtime ChangeEventExpand processors.
pub struct ChangeEventExpandProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    events: Vec<ChangeEventRuntimeSpec>,
    output_chunk_schema: ChunkSchemaRef,
    output_slot_ids: Vec<SlotId>,
    change_op_slot_id: SlotId,
    data_route_slot_id: Option<SlotId>,
}

impl ChangeEventExpandProcessorFactory {
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        events: Vec<ChangeEventRuntimeSpec>,
        output_chunk_schema: ChunkSchemaRef,
        output_slot_ids: Vec<SlotId>,
        change_op_slot_id: SlotId,
        data_route_slot_id: Option<SlotId>,
    ) -> Result<Self, String> {
        validate_output_schema(
            &output_chunk_schema,
            &output_slot_ids,
            change_op_slot_id,
            data_route_slot_id,
        )?;
        let name = if node_id >= 0 {
            format!("CHANGE_EVENT_EXPAND (id={node_id})")
        } else {
            "CHANGE_EVENT_EXPAND".to_string()
        };
        Ok(Self {
            name,
            arena,
            events,
            output_chunk_schema,
            output_slot_ids,
            change_op_slot_id,
            data_route_slot_id,
        })
    }
}

impl OperatorFactory for ChangeEventExpandProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(ChangeEventExpandProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            events: self.events.clone(),
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            output_slot_ids: self.output_slot_ids.clone(),
            change_op_slot_id: self.change_op_slot_id,
            data_route_slot_id: self.data_route_slot_id,
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct ChangeEventExpandProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    events: Vec<ChangeEventRuntimeSpec>,
    output_chunk_schema: ChunkSchemaRef,
    output_slot_ids: Vec<SlotId>,
    change_op_slot_id: SlotId,
    data_route_slot_id: Option<SlotId>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for ChangeEventExpandProcessorOperator {
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

impl ProcessorOperator for ChangeEventExpandProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err(
                "change event expand received input while output buffer is full".to_string(),
            );
        }
        self.pending_output = Some(self.process_one(&chunk)?);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

impl ChangeEventExpandProcessorOperator {
    fn process_one(&self, chunk: &Chunk) -> Result<Chunk, String> {
        self.validate_output_schema()?;
        if chunk.is_empty() {
            return self.empty_output_chunk();
        }

        let masks = self
            .events
            .iter()
            .enumerate()
            .map(|(event_idx, event)| self.predicate_mask(event_idx, event, chunk))
            .collect::<Result<Vec<_>, _>>()?;

        let mut event_chunks = Vec::new();
        for (event_idx, (event, mask)) in self.events.iter().zip(masks.iter()).enumerate() {
            let selected_count = selected_row_count(mask);
            if selected_count == 0 {
                continue;
            }
            event_chunks.push(self.build_event_chunk(
                event_idx,
                event,
                mask,
                selected_count,
                chunk,
            )?);
        }

        self.concat_event_chunks(event_chunks)
    }

    fn validate_output_schema(&self) -> Result<(), String> {
        validate_output_schema(
            &self.output_chunk_schema,
            &self.output_slot_ids,
            self.change_op_slot_id,
            self.data_route_slot_id,
        )
    }

    fn predicate_mask(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        chunk: &Chunk,
    ) -> Result<BooleanArray, String> {
        let Some(predicate) = event.predicate else {
            return Ok(BooleanArray::from(vec![true; chunk.len()]));
        };
        let array = self
            .arena
            .eval(predicate, chunk)
            .map_err(|err| format!("change event expand predicate {event_idx} failed: {err}"))?;
        let mask = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                format!(
                    "change event expand predicate {event_idx} must return boolean, got {:?}",
                    array.data_type()
                )
            })?;
        if mask.len() != chunk.len() {
            return Err(format!(
                "change event expand predicate {event_idx} length mismatch: mask={} input={}",
                mask.len(),
                chunk.len()
            ));
        }
        Ok(mask.clone())
    }

    fn build_event_chunk(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        mask: &BooleanArray,
        selected_count: usize,
        chunk: &Chunk,
    ) -> Result<Chunk, String> {
        let assignments = self.assignment_arrays(event_idx, event, mask, selected_count, chunk)?;
        let route_key = event.branch_kind.route_key();
        let mut columns = Vec::with_capacity(self.output_chunk_schema.slots().len());

        for slot_schema in self.output_chunk_schema.slots() {
            let slot_id = slot_schema.slot_id();
            let array = if slot_id == self.change_op_slot_id {
                route_value_array(route_key.change_op, slot_schema.data_type(), selected_count)?
            } else if Some(slot_id) == self.data_route_slot_id {
                match route_key.data_route {
                    Some(route) => {
                        route_value_array(route, slot_schema.data_type(), selected_count)?
                    }
                    None => new_null_array(slot_schema.data_type(), selected_count),
                }
            } else if let Some(array) = assignments.get(&slot_id) {
                array.clone()
            } else {
                new_null_array(slot_schema.data_type(), selected_count)
            };
            columns.push(array);
        }

        Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), columns).map_err(|err| {
            format!("change event expand build event {event_idx} chunk failed: {err}")
        })
    }

    fn assignment_arrays(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        mask: &BooleanArray,
        selected_count: usize,
        chunk: &Chunk,
    ) -> Result<HashMap<SlotId, ArrayRef>, String> {
        let mut arrays = HashMap::with_capacity(event.assignments.len());
        for assignment in &event.assignments {
            let slot_id = assignment.output_slot_id;
            if slot_id == self.change_op_slot_id || Some(slot_id) == self.data_route_slot_id {
                return Err(format!(
                    "change event expand event {event_idx} assignment targets generated route slot {}",
                    slot_id
                ));
            }
            let Some(slot_schema) = self.output_chunk_schema.slot(slot_id) else {
                return Err(format!(
                    "change event expand event {event_idx} assignment output slot {} is not in output schema",
                    slot_id
                ));
            };
            let array = match assignment.expr {
                Some(expr) => {
                    let array = self.arena.eval(expr, chunk).map_err(|err| {
                        format!(
                            "change event expand event {event_idx} assignment for slot {} failed: {err}",
                            slot_id
                        )
                    })?;
                    if array.len() != chunk.len() {
                        return Err(format!(
                            "change event expand event {event_idx} assignment for slot {} length mismatch: array={} input={}",
                            slot_id,
                            array.len(),
                            chunk.len()
                        ));
                    }
                    let filtered = filter_selected_rows(array, mask, selected_count)?;
                    cast_array_to_target(&filtered, slot_schema.data_type()).map_err(|err| {
                        format!(
                            "change event expand event {event_idx} assignment for slot {} cast to {:?} failed: {err}",
                            slot_id,
                            slot_schema.data_type()
                        )
                    })?
                }
                None => new_null_array(slot_schema.data_type(), selected_count),
            };
            if arrays.insert(slot_id, array).is_some() {
                return Err(format!(
                    "change event expand event {event_idx} has duplicate assignment for slot {}",
                    slot_id
                ));
            }
        }
        Ok(arrays)
    }

    fn concat_event_chunks(&self, event_chunks: Vec<Chunk>) -> Result<Chunk, String> {
        if event_chunks.is_empty() {
            return self.empty_output_chunk();
        }
        if event_chunks.len() == 1 {
            return Ok(event_chunks.into_iter().next().expect("event chunk"));
        }

        let mut columns = Vec::with_capacity(self.output_chunk_schema.slots().len());
        for column_idx in 0..self.output_chunk_schema.slots().len() {
            let parts = event_chunks
                .iter()
                .map(|chunk| chunk.batch.column(column_idx).as_ref())
                .collect::<Vec<&dyn Array>>();
            columns.push(concat(&parts).map_err(|err| {
                format!("change event expand concat output column {column_idx} failed: {err}")
            })?);
        }

        Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), columns)
            .map_err(|err| format!("change event expand build concatenated chunk failed: {err}"))
    }

    fn empty_output_chunk(&self) -> Result<Chunk, String> {
        let columns = self
            .output_chunk_schema
            .slots()
            .iter()
            .map(|slot| new_null_array(slot.data_type(), 0))
            .collect();
        Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), columns)
            .map_err(|err| format!("change event expand build empty output chunk failed: {err}"))
    }
}

fn selected_row_count(mask: &BooleanArray) -> usize {
    mask.iter()
        .filter(|value| matches!(value, Some(true)))
        .count()
}

fn filter_selected_rows(
    array: ArrayRef,
    mask: &BooleanArray,
    selected_count: usize,
) -> Result<ArrayRef, String> {
    if selected_count == array.len() {
        return Ok(array);
    }
    filter(array.as_ref(), mask).map_err(|err| format!("filter selected rows failed: {err}"))
}

fn route_value_array(
    value: i32,
    target_type: &arrow::datatypes::DataType,
    len: usize,
) -> Result<ArrayRef, String> {
    let array = Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
        value, len,
    ))) as ArrayRef;
    cast_array_to_target(&array, target_type)
}

fn validate_output_schema(
    output_chunk_schema: &ChunkSchemaRef,
    output_slot_ids: &[SlotId],
    change_op_slot_id: SlotId,
    data_route_slot_id: Option<SlotId>,
) -> Result<(), String> {
    if output_chunk_schema.slot_ids() != output_slot_ids {
        return Err(format!(
            "change event expand output_slot_ids {:?} do not match output schema slot order {:?}",
            output_slot_ids,
            output_chunk_schema.slot_ids()
        ));
    }
    if data_route_slot_id == Some(change_op_slot_id) {
        return Err(format!(
            "change event expand change-op slot {} and data-route slot must be distinct",
            change_op_slot_id
        ));
    }
    let Some(change_op_slot) = output_chunk_schema.slot(change_op_slot_id) else {
        return Err(format!(
            "change event expand output schema is missing change-op slot {}",
            change_op_slot_id
        ));
    };
    if change_op_slot.data_type() != &DataType::Int8 {
        return Err(format!(
            "change event expand change-op slot {} must be Int8, got {:?}",
            change_op_slot_id,
            change_op_slot.data_type()
        ));
    }
    if let Some(data_route_slot_id) = data_route_slot_id {
        let data_route_slot = output_chunk_schema
            .slot(data_route_slot_id)
            .ok_or_else(|| {
                format!(
                    "change event expand output schema is missing data-route slot {}",
                    data_route_slot_id
                )
            })?;
        if !is_signed_integer_route_type(data_route_slot.data_type()) {
            return Err(format!(
                "change event expand data-route slot {} must be a signed integer route type, got {:?}",
                data_route_slot_id,
                data_route_slot.data_type()
            ));
        }
    }
    Ok(())
}

fn is_signed_integer_route_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::node::change_event_expand::{
        ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
    };
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::sql::common::ChangeStreamBranchKind;

    const INPUT_FILE_SLOT: SlotId = SlotId::new(10);
    const INPUT_POS_SLOT: SlotId = SlotId::new(11);
    const INPUT_ROW_ID_SLOT: SlotId = SlotId::new(12);
    const INPUT_VALUE_SLOT: SlotId = SlotId::new(13);
    const CHANGE_OP_SLOT: SlotId = SlotId::new(20);
    const DATA_ROUTE_SLOT: SlotId = SlotId::new(21);
    const OUTPUT_FILE_SLOT: SlotId = SlotId::new(22);
    const OUTPUT_POS_SLOT: SlotId = SlotId::new(23);
    const OUTPUT_VALUE_SLOT: SlotId = SlotId::new(24);

    fn output_slot_ids() -> Vec<SlotId> {
        vec![
            CHANGE_OP_SLOT,
            DATA_ROUTE_SLOT,
            OUTPUT_FILE_SLOT,
            OUTPUT_POS_SLOT,
            OUTPUT_VALUE_SLOT,
        ]
    }

    fn chunk_schema(fields: Vec<(SlotId, Field)>) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(
                fields
                    .into_iter()
                    .map(|(slot_id, field)| {
                        ChunkSlotSchema::new_with_field(slot_id, field, None, None)
                    })
                    .collect(),
            )
            .expect("chunk schema"),
        )
    }

    fn input_chunk() -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("f1.parquet")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(3)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(9)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
            ],
        )
        .expect("input batch");
        Chunk::new_with_chunk_schema(
            batch,
            chunk_schema(vec![
                (INPUT_FILE_SLOT, Field::new("_file", DataType::Utf8, false)),
                (INPUT_POS_SLOT, Field::new("_pos", DataType::Int64, false)),
                (
                    INPUT_ROW_ID_SLOT,
                    Field::new("_row_id", DataType::Int64, false),
                ),
                (
                    INPUT_VALUE_SLOT,
                    Field::new("value", DataType::Int32, false),
                ),
            ]),
        )
    }

    fn output_chunk_schema() -> ChunkSchemaRef {
        chunk_schema(vec![
            (
                CHANGE_OP_SLOT,
                Field::new("__change_op", DataType::Int8, false),
            ),
            (
                DATA_ROUTE_SLOT,
                Field::new("__data_route", DataType::Int32, true),
            ),
            (OUTPUT_FILE_SLOT, Field::new("_file", DataType::Utf8, true)),
            (OUTPUT_POS_SLOT, Field::new("_pos", DataType::Int64, true)),
            (
                OUTPUT_VALUE_SLOT,
                Field::new("value", DataType::Int32, true),
            ),
        ])
    }

    fn assert_empty_output_has_output_schema(out: &Chunk) {
        let expected_slot_ids = output_slot_ids();
        let expected_schema = output_chunk_schema();

        assert_eq!(out.len(), 0);
        assert_eq!(out.batch.num_columns(), expected_slot_ids.len());
        assert_eq!(out.chunk_schema().slot_ids(), expected_slot_ids.as_slice());

        for slot in expected_schema.slots() {
            let array = out
                .column_by_slot_id(slot.slot_id())
                .expect("output slot must exist");
            assert_eq!(array.len(), 0, "slot {} row count", slot.slot_id());
            assert_eq!(
                array.data_type(),
                slot.data_type(),
                "slot {} data type",
                slot.slot_id()
            );
        }
    }

    fn slot_expr(
        arena: &mut ExprArena,
        slot_id: SlotId,
        data_type: DataType,
    ) -> crate::exec::expr::ExprId {
        arena.push_typed(ExprNode::SlotId(slot_id), data_type)
    }

    #[test]
    fn change_event_expand_emits_delete_and_reuse_rows() {
        // Input has one row: file=f1.parquet, pos=3, row_id=9, value=42.
        // Expand has DeleteDv and ReuseData events.
        // Output has two rows:
        //   row 0: __change_op=-1, route=NULL, _file=f1.parquet, _pos=3, data value NULL
        //   row 1: __change_op=+1, route=1, _file NULL, _pos NULL, data value=42
        let mut arena = ExprArena::default();
        let file_expr = slot_expr(&mut arena, INPUT_FILE_SLOT, DataType::Utf8);
        let pos_expr = slot_expr(&mut arena, INPUT_POS_SLOT, DataType::Int64);
        let value_expr = slot_expr(&mut arena, INPUT_VALUE_SLOT, DataType::Int32);
        let factory = ChangeEventExpandProcessorFactory::new(
            7,
            Arc::new(arena),
            vec![
                ChangeEventRuntimeSpec {
                    predicate: None,
                    branch_kind: ChangeStreamBranchKind::DeleteDv,
                    assignments: vec![
                        ChangeEventRuntimeOutputExpr {
                            output_slot_id: OUTPUT_FILE_SLOT,
                            expr: Some(file_expr),
                        },
                        ChangeEventRuntimeOutputExpr {
                            output_slot_id: OUTPUT_POS_SLOT,
                            expr: Some(pos_expr),
                        },
                    ],
                },
                ChangeEventRuntimeSpec {
                    predicate: None,
                    branch_kind: ChangeStreamBranchKind::ReuseData,
                    assignments: vec![ChangeEventRuntimeOutputExpr {
                        output_slot_id: OUTPUT_VALUE_SLOT,
                        expr: Some(value_expr),
                    }],
                },
            ],
            output_chunk_schema(),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        )
        .expect("factory");
        let mut op = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = op.as_processor_mut().expect("processor");

        processor.push_chunk(&state, input_chunk()).expect("push");
        let out = processor
            .pull_chunk(&state)
            .expect("pull")
            .expect("output chunk");

        assert_eq!(out.len(), 2);
        let change_op_array = out.column_by_slot_id(CHANGE_OP_SLOT).expect("change op");
        let change_op = change_op_array
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("change op int8");
        let data_route_array = out.column_by_slot_id(DATA_ROUTE_SLOT).expect("data route");
        let data_route = data_route_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("data route int32");
        let file_array = out.column_by_slot_id(OUTPUT_FILE_SLOT).expect("file");
        let file = file_array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file string");
        let pos_array = out.column_by_slot_id(OUTPUT_POS_SLOT).expect("pos");
        let pos = pos_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("pos int64");
        let value_array = out.column_by_slot_id(OUTPUT_VALUE_SLOT).expect("value");
        let value = value_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value int32");

        assert_eq!(change_op.value(0), -1);
        assert_eq!(change_op.value(1), 1);
        assert!(data_route.is_null(0));
        assert_eq!(data_route.value(1), 1);
        assert_eq!(file.value(0), "f1.parquet");
        assert!(file.is_null(1));
        assert_eq!(pos.value(0), 3);
        assert!(pos.is_null(1));
        assert!(value.is_null(0));
        assert_eq!(value.value(1), 42);
    }

    #[test]
    fn change_event_expand_skips_false_predicate() {
        // One event predicate is false for all rows; processor emits no rows for it
        // and still reaches finish cleanly.
        let mut arena = ExprArena::default();
        let false_predicate = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(false)),
            DataType::Boolean,
        );
        let value_expr = slot_expr(&mut arena, INPUT_VALUE_SLOT, DataType::Int32);
        let factory = ChangeEventExpandProcessorFactory::new(
            8,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: Some(false_predicate),
                branch_kind: ChangeStreamBranchKind::ReuseData,
                assignments: vec![ChangeEventRuntimeOutputExpr {
                    output_slot_id: OUTPUT_VALUE_SLOT,
                    expr: Some(value_expr),
                }],
            }],
            output_chunk_schema(),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        )
        .expect("factory");
        let mut op = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = op.as_processor_mut().expect("processor");

        processor.push_chunk(&state, input_chunk()).expect("push");
        let out = processor
            .pull_chunk(&state)
            .expect("pull")
            .expect("empty output chunk");

        assert_empty_output_has_output_schema(&out);
        processor.set_finishing(&state).expect("finish");
        assert!(op.is_finished());
    }

    #[test]
    fn change_event_expand_skips_null_predicate() {
        let mut arena = ExprArena::default();
        let null_predicate =
            arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Boolean);
        let value_expr = slot_expr(&mut arena, INPUT_VALUE_SLOT, DataType::Int32);
        let factory = ChangeEventExpandProcessorFactory::new(
            12,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: Some(null_predicate),
                branch_kind: ChangeStreamBranchKind::ReuseData,
                assignments: vec![ChangeEventRuntimeOutputExpr {
                    output_slot_id: OUTPUT_VALUE_SLOT,
                    expr: Some(value_expr),
                }],
            }],
            output_chunk_schema(),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        )
        .expect("factory");
        let mut op = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = op.as_processor_mut().expect("processor");

        processor.push_chunk(&state, input_chunk()).expect("push");
        let out = processor
            .pull_chunk(&state)
            .expect("pull")
            .expect("empty output chunk");

        assert_empty_output_has_output_schema(&out);
    }

    #[test]
    fn change_event_expand_empty_input_preserves_output_schema() {
        let factory = ChangeEventExpandProcessorFactory::new(
            14,
            Arc::new(ExprArena::default()),
            vec![ChangeEventRuntimeSpec {
                predicate: None,
                branch_kind: ChangeStreamBranchKind::DeleteDv,
                assignments: vec![],
            }],
            output_chunk_schema(),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        )
        .expect("factory");
        let mut op = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = op.as_processor_mut().expect("processor");

        processor
            .push_chunk(&state, input_chunk().slice(0, 0))
            .expect("push");
        let out = processor
            .pull_chunk(&state)
            .expect("pull")
            .expect("empty output chunk");

        assert_empty_output_has_output_schema(&out);
    }

    #[test]
    fn change_event_expand_emits_fresh_data_route() {
        let mut arena = ExprArena::default();
        let value_expr = slot_expr(&mut arena, INPUT_VALUE_SLOT, DataType::Int32);
        let factory = ChangeEventExpandProcessorFactory::new(
            13,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: None,
                branch_kind: ChangeStreamBranchKind::FreshData,
                assignments: vec![ChangeEventRuntimeOutputExpr {
                    output_slot_id: OUTPUT_VALUE_SLOT,
                    expr: Some(value_expr),
                }],
            }],
            output_chunk_schema(),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        )
        .expect("factory");
        let mut op = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = op.as_processor_mut().expect("processor");

        processor.push_chunk(&state, input_chunk()).expect("push");
        let out = processor
            .pull_chunk(&state)
            .expect("pull")
            .expect("output chunk");

        assert_eq!(out.len(), 1);
        let change_op_array = out.column_by_slot_id(CHANGE_OP_SLOT).expect("change op");
        let change_op = change_op_array
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("change op int8");
        let data_route_array = out.column_by_slot_id(DATA_ROUTE_SLOT).expect("data route");
        let data_route = data_route_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("data route int32");

        assert_eq!(change_op.value(0), 1);
        assert_eq!(data_route.value(0), 2);
    }

    #[test]
    fn change_event_expand_rejects_equal_route_slots() {
        let mut arena = ExprArena::default();
        let value_expr = slot_expr(&mut arena, INPUT_VALUE_SLOT, DataType::Int32);
        let err = match ChangeEventExpandProcessorFactory::new(
            9,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: None,
                branch_kind: ChangeStreamBranchKind::ReuseData,
                assignments: vec![ChangeEventRuntimeOutputExpr {
                    output_slot_id: OUTPUT_VALUE_SLOT,
                    expr: Some(value_expr),
                }],
            }],
            output_chunk_schema(),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(CHANGE_OP_SLOT),
        ) {
            Ok(_) => panic!("equal route slots must fail"),
            Err(err) => err,
        };

        assert!(
            err.contains("change-op") && err.contains("data-route") && err.contains("distinct"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn change_event_expand_rejects_non_int8_change_op_schema() {
        let err = match ChangeEventExpandProcessorFactory::new(
            10,
            Arc::new(ExprArena::default()),
            vec![ChangeEventRuntimeSpec {
                predicate: None,
                branch_kind: ChangeStreamBranchKind::DeleteDv,
                assignments: vec![],
            }],
            chunk_schema(vec![
                (
                    CHANGE_OP_SLOT,
                    Field::new("__change_op", DataType::Int32, false),
                ),
                (
                    DATA_ROUTE_SLOT,
                    Field::new("__data_route", DataType::Int32, true),
                ),
                (OUTPUT_FILE_SLOT, Field::new("_file", DataType::Utf8, true)),
                (OUTPUT_POS_SLOT, Field::new("_pos", DataType::Int64, true)),
                (
                    OUTPUT_VALUE_SLOT,
                    Field::new("value", DataType::Int32, true),
                ),
            ]),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        ) {
            Ok(_) => panic!("non-Int8 change-op must fail"),
            Err(err) => err,
        };

        assert!(
            err.contains("change-op") && err.contains("Int8"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn change_event_expand_rejects_non_integral_data_route_schema() {
        let err = match ChangeEventExpandProcessorFactory::new(
            15,
            Arc::new(ExprArena::default()),
            vec![ChangeEventRuntimeSpec {
                predicate: None,
                branch_kind: ChangeStreamBranchKind::ReuseData,
                assignments: vec![],
            }],
            chunk_schema(vec![
                (
                    CHANGE_OP_SLOT,
                    Field::new("__change_op", DataType::Int8, false),
                ),
                (
                    DATA_ROUTE_SLOT,
                    Field::new("__data_route", DataType::Utf8, true),
                ),
                (OUTPUT_FILE_SLOT, Field::new("_file", DataType::Utf8, true)),
                (OUTPUT_POS_SLOT, Field::new("_pos", DataType::Int64, true)),
                (
                    OUTPUT_VALUE_SLOT,
                    Field::new("value", DataType::Int32, true),
                ),
            ]),
            output_slot_ids(),
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        ) {
            Ok(_) => panic!("non-integral data-route must fail"),
            Err(err) => err,
        };

        assert!(
            err.contains("data-route") && err.contains("integer"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn change_event_expand_rejects_output_slot_order_mismatch() {
        let err = match ChangeEventExpandProcessorFactory::new(
            11,
            Arc::new(ExprArena::default()),
            vec![ChangeEventRuntimeSpec {
                predicate: None,
                branch_kind: ChangeStreamBranchKind::DeleteDv,
                assignments: vec![],
            }],
            output_chunk_schema(),
            vec![
                DATA_ROUTE_SLOT,
                CHANGE_OP_SLOT,
                OUTPUT_FILE_SLOT,
                OUTPUT_POS_SLOT,
                OUTPUT_VALUE_SLOT,
            ],
            CHANGE_OP_SLOT,
            Some(DATA_ROUTE_SLOT),
        ) {
            Ok(_) => panic!("output slot order mismatch must fail"),
            Err(err) => err,
        };

        assert!(
            err.contains("output_slot_ids") && err.contains("output schema"),
            "unexpected error: {err}"
        );
    }
}
