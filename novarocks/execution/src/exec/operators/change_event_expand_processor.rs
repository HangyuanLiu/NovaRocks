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

use crate::exec::fragment::error::node_construction_failure as expression_contract;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, new_null_array};
use arrow::compute::filter;
use arrow::datatypes::DataType;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprEvaluationContext, cast_array_to_target};
use crate::exec::node::change_event_expand::ChangeEventRuntimeSpec;
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
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;

/// F3 array/expression oracle over explicit event facts; unavailable to product composition.
#[cfg(test)]
pub(crate) struct ChangeEventExpandFixtureFactory {
    name: String,
    arena: Arc<ExprArena>,
    events: Vec<ChangeEventRuntimeSpec>,
    output_chunk_schema: ChunkSchemaRef,
    output_slot_ids: Vec<SlotId>,
    effect_slot_id: SlotId,
}

#[cfg(test)]
impl ChangeEventExpandFixtureFactory {
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        events: Vec<ChangeEventRuntimeSpec>,
        output_chunk_schema: ChunkSchemaRef,
        output_slot_ids: Vec<SlotId>,
        effect_slot_id: SlotId,
    ) -> crate::runtime::fragment::ExecutionResult<Self> {
        validate_output_schema(&output_chunk_schema, &output_slot_ids, effect_slot_id)?;
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
            effect_slot_id,
        })
    }
}

#[cfg(test)]
impl OperatorFactory for ChangeEventExpandFixtureFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(ChangeEventExpandFixtureKernel {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            events: self.events.clone(),
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            output_slot_ids: self.output_slot_ids.clone(),
            effect_slot_id: self.effect_slot_id,
            construction: None,
            input: None,
            next_event: 0,
        }))
    }
}

#[cfg(test)]
struct ChangeEventExpandFixtureKernel {
    construction: Option<Arc<dyn InputConstructionFactory>>,
    name: String,
    arena: Arc<ExprArena>,
    events: Vec<ChangeEventRuntimeSpec>,
    output_chunk_schema: ChunkSchemaRef,
    output_slot_ids: Vec<SlotId>,
    effect_slot_id: SlotId,
    input: Option<Chunk>,
    next_event: usize,
}

#[cfg(test)]
impl OperatorLifecycle for ChangeEventExpandFixtureKernel {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        self.construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.input = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.input = None;
    }
}

#[cfg(test)]
impl InputEncodingPolicy for ChangeEventExpandFixtureKernel {}
#[cfg(test)]
impl TransformKernel for ChangeEventExpandFixtureKernel {
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        ctx.checkpoint()?;
        let chunk = input.into_uncovered_chunk()?;
        self.validate_output_schema()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if chunk.is_empty() || self.events.is_empty() {
            return Ok(StepResult::new(
                Some({
                    let factory = self
                        .construction
                        .as_deref()
                        .ok_or(ConstructionFailure::MissingOwner)?;
                    let owner = factory.begin_input_construction(&chunk)?;
                    self.empty_output_chunk(&chunk, owner.as_ref())?
                }),
                NextStep::NeedInput,
                rows,
            ));
        }
        self.input = Some(chunk);
        self.next_event = 0;
        Ok(StepResult::new(None, NextStep::Runnable, rows))
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let Some(input) = self.input.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        ctx.checkpoint()?;
        let output = {
            let factory = self
                .construction
                .as_deref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = factory.begin_input_construction(input)?;
            let evaluation = ExprEvaluationContext::new(&self.arena, owner.as_ref(), ctx);
            let event = &self.events[self.next_event];
            let mask = self.predicate_mask(self.next_event, event, input, &evaluation)?;
            let count = selected_row_count(&mask);
            if count == 0 {
                self.empty_output_chunk(input, owner.as_ref())?
            } else {
                self.build_event_chunk(
                    self.next_event,
                    event,
                    &mask,
                    count,
                    input,
                    &evaluation,
                    owner.as_ref(),
                )?
            }
        };
        ctx.checkpoint()?;
        self.next_event += 1;
        let next = if self.next_event == self.events.len() {
            self.input = None;
            NextStep::NeedInput
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(output), next, 0))
    }
}

#[cfg(test)]
impl ChangeEventExpandFixtureKernel {
    fn validate_output_schema(&self) -> crate::runtime::fragment::ExecutionResult<()> {
        validate_output_schema(
            &self.output_chunk_schema,
            &self.output_slot_ids,
            self.effect_slot_id,
        )
    }

    fn predicate_mask(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        chunk: &Chunk,
        evaluation: &ExprEvaluationContext<'_>,
    ) -> ExecutionResult<BooleanArray> {
        let Some(predicate) = event.predicate else {
            return Ok(BooleanArray::from(vec![true; chunk.len()]));
        };
        let array = evaluation
            .eval(predicate, chunk)
            .map_err(|source| evaluation_failure(event_idx, None, "predicate", source))?;
        let mask = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                expression_contract(format_args!(
                    "change event expand predicate {event_idx} must return boolean, got {:?}",
                    array.data_type()
                ))
            })?;
        if mask.len() != chunk.len() {
            return Err(expression_contract(format_args!(
                "change event expand predicate {event_idx} length mismatch: mask={} input={}",
                mask.len(),
                chunk.len()
            )));
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
        evaluation: &ExprEvaluationContext<'_>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Chunk> {
        let assignments =
            self.assignment_arrays(event_idx, event, mask, selected_count, chunk, evaluation)?;
        owner.request(std::alloc::Layout::array::<ArrayRef>(self.output_chunk_schema.slots().len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut columns = Vec::with_capacity(self.output_chunk_schema.slots().len());

        for slot_schema in self.output_chunk_schema.slots() {
            let slot_id = slot_schema.slot_id();
            let array = if slot_id == self.effect_slot_id {
                effect_value_array(event.effect as i8, slot_schema.data_type(), selected_count)?
            } else if let Some(array) = assignments.get(&slot_id) {
                array.clone()
            } else {
                new_null_array(slot_schema.data_type(), selected_count)
            };
            columns.push(array);
        }

        self.materialize_output(columns, selected_count, chunk, owner)
            .map_err(|source| evaluation_failure(event_idx, None, "output", source))
    }

    fn assignment_arrays(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        mask: &BooleanArray,
        selected_count: usize,
        chunk: &Chunk,
        evaluation: &ExprEvaluationContext<'_>,
    ) -> ExecutionResult<HashMap<SlotId, ArrayRef>> {
        let mut arrays = HashMap::with_capacity(event.assignments.len());
        for assignment in &event.assignments {
            let slot_id = assignment.output_slot_id;
            if slot_id == self.effect_slot_id {
                return Err(expression_contract(format_args!(
                    "change event expand event {event_idx} assignment targets generated route slot {}",
                    slot_id
                )));
            }
            let Some(slot_schema) = self.output_chunk_schema.slot(slot_id) else {
                return Err(expression_contract(format_args!(
                    "change event expand event {event_idx} assignment output slot {} is not in output schema",
                    slot_id
                )));
            };
            let array = match assignment.expr {
                Some(expr) => {
                    let array = evaluation.eval(expr, chunk).map_err(|source| {
                        evaluation_failure(event_idx, Some(slot_id), "assignment", source)
                    })?;
                    if array.len() != chunk.len() {
                        return Err(expression_contract(format_args!(
                            "change event expand event {event_idx} assignment for slot {} length mismatch: array={} input={}",
                            slot_id,
                            array.len(),
                            chunk.len()
                        )));
                    }
                    let filtered = filter_selected_rows(array, mask, selected_count)?;
                    cast_array_to_target(&filtered, slot_schema.data_type()).map_err(|source| {
                        evaluation_failure(event_idx, Some(slot_id), "cast", source)
                    })?
                }
                None => new_null_array(slot_schema.data_type(), selected_count),
            };
            if arrays.insert(slot_id, array).is_some() {
                return Err(expression_contract(format_args!(
                    "change event expand event {event_idx} has duplicate assignment for slot {}",
                    slot_id
                )));
            }
        }
        Ok(arrays)
    }

    fn materialize_output(
        &self,
        columns: Vec<ArrayRef>,
        rows: usize,
        source: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Chunk> {
        let storage = source
            .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ExecutionFailure,
            _,
        >(
            self.output_chunk_schema.clone(),
            columns,
            rows,
            storage,
            |step| Ok(owner.observe(0, &[], step)?),
        )?)
    }

    fn empty_output_chunk(
        &self,
        source: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Chunk, ExecutionFailure> {
        owner.request(std::alloc::Layout::array::<ArrayRef>(self.output_chunk_schema.slots().len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let columns = self
            .output_chunk_schema
            .slots()
            .iter()
            .map(|slot| new_null_array(slot.data_type(), 0))
            .collect();
        self.materialize_output(columns, 0, source, owner)
    }
}

#[cfg(test)]
fn selected_row_count(mask: &BooleanArray) -> usize {
    mask.iter()
        .filter(|value| matches!(value, Some(true)))
        .count()
}

#[cfg(test)]
fn filter_selected_rows(
    array: ArrayRef,
    mask: &BooleanArray,
    selected_count: usize,
) -> ExecutionResult<ArrayRef> {
    if selected_count == array.len() {
        return Ok(array);
    }
    filter(array.as_ref(), mask)
        .map_err(|source| ExecutionFailure::from_domain(ChangeEventFilterFailure(source)))
}

#[cfg(test)]
fn effect_value_array(
    value: i8,
    target_type: &arrow::datatypes::DataType,
    len: usize,
) -> ExecutionResult<ArrayRef> {
    let array = Arc::new(Int8Array::from_iter_values(std::iter::repeat_n(value, len))) as ArrayRef;
    cast_array_to_target(&array, target_type)
}

#[cfg(test)]
fn validate_output_schema(
    output_chunk_schema: &ChunkSchemaRef,
    output_slot_ids: &[SlotId],
    effect_slot_id: SlotId,
) -> crate::runtime::fragment::ExecutionResult<()> {
    if output_chunk_schema.slot_ids() != output_slot_ids {
        return Err(crate::exec::fragment::error::node_construction_failure(
            format_args!(
                "change event expand output_slot_ids {:?} do not match output schema slot order {:?}",
                output_slot_ids,
                output_chunk_schema.slot_ids()
            ),
        ));
    }
    let Some(effect_slot) = output_chunk_schema.slot(effect_slot_id) else {
        return Err(crate::exec::fragment::error::node_construction_failure(
            format_args!(
                "change event expand output schema is missing logical effect slot {}",
                effect_slot_id
            ),
        ));
    };
    if effect_slot.data_type() != &DataType::Int8 {
        return Err(crate::exec::fragment::error::node_construction_failure(
            format_args!(
                "change event expand logical effect slot {} must be Int8, got {:?}",
                effect_slot_id,
                effect_slot.data_type()
            ),
        ));
    }
    Ok(())
}

/// Location enrichment retains the domain cause and forwards its exact facts.
#[derive(Debug)]
struct ChangeEventEvaluationFailure {
    event: usize,
    slot: Option<SlotId>,
    stage: &'static str,
    source: ExecutionFailure,
}
impl std::fmt::Display for ChangeEventEvaluationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "change event {} {}", self.event, self.stage)?;
        if let Some(slot) = self.slot {
            write!(f, " for slot {slot}")?;
        }
        write!(f, ": {}", self.source)
    }
}
impl std::error::Error for ChangeEventEvaluationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for ChangeEventEvaluationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.source)
    }
}
fn evaluation_failure(
    event: usize,
    slot: Option<SlotId>,
    stage: &'static str,
    source: ExecutionFailure,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(ChangeEventEvaluationFailure {
        event,
        slot,
        stage,
        source,
    })
}
#[derive(Debug)]
struct ChangeEventFilterFailure(arrow::error::ArrowError);
impl std::fmt::Display for ChangeEventFilterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "change event filter failed: {}", self.0)
    }
}
impl std::error::Error for ChangeEventFilterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ChangeEventFilterFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, reason) = if matches!(self.0, arrow::error::ArrowError::MemoryError(_)) {
            (
                FailureClass::Resource,
                ExecutionReason::ChangeEventFilterAllocation,
            )
        } else {
            (
                FailureClass::Invariant,
                ExecutionReason::ChangeEventFilterContract,
            )
        };
        FailureFact::new(class, reason.reason())
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int8Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::ConnectorRowMutationEffect;

    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::node::change_event_expand::{
        ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
    };
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    const INPUT_SLOT: SlotId = SlotId::new(10);
    const EFFECT_SLOT: SlotId = SlotId::new(20);
    const VALUE_SLOT: SlotId = SlotId::new(21);

    fn install_creator(operator: &mut dyn Operator, state: &RuntimeState) {
        let factory =
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root(
                    "change event actual creator fixture",
                ),
                state.error_state(),
            )
            .unwrap();
        operator.set_construction_factory(factory);
        operator.prepare().unwrap();
    }

    fn schema(fields: Vec<(SlotId, Field)>) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new_typed(
                fields
                    .into_iter()
                    .map(|(slot_id, field)| {
                        let ty = novarocks_type_contract::FunctionValueType::new(
                            field.data_type().clone(),
                            field.is_nullable(),
                        );
                        ChunkSlotSchema::try_new_typed(
                            slot_id,
                            Arc::new(field),
                            ty.typed_binding().clone(),
                            None,
                            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                        )
                        .unwrap()
                    })
                    .collect(),
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            )
            .expect("schema"),
        )
    }

    fn input() -> Chunk {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![42])) as ArrayRef],
        )
        .expect("input");
        Chunk::new_with_chunk_schema(
            batch,
            schema(vec![(
                INPUT_SLOT,
                Field::new("value", DataType::Int32, false),
            )]),
        )
    }

    fn output_schema() -> ChunkSchemaRef {
        schema(vec![
            (
                EFFECT_SLOT,
                Field::new("__row_mutation_effect", DataType::Int8, false),
            ),
            (VALUE_SLOT, Field::new("value", DataType::Int32, true)),
        ])
    }

    fn slot_expr(arena: &mut ExprArena) -> crate::exec::expr::ExprId {
        arena.push_typed(ExprNode::SlotId(INPUT_SLOT), DataType::Int32)
    }

    #[test]
    fn change_event_expand_materializes_logical_effects() {
        let mut arena = ExprArena::default();
        let value = slot_expr(&mut arena);
        let factory = ChangeEventExpandFixtureFactory::new(
            7,
            Arc::new(arena),
            vec![
                ChangeEventRuntimeSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Delete,
                    assignments: Vec::new(),
                },
                ChangeEventRuntimeSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Replace,
                    assignments: vec![ChangeEventRuntimeOutputExpr {
                        output_slot_id: VALUE_SLOT,
                        expr: Some(value),
                    }],
                },
            ],
            output_schema(),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        )
        .expect("factory");
        let state = RuntimeState::default();
        let mut operator = factory.create(1, 0);
        install_creator(operator.as_mut(), &state);
        let mut processor =
            crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input()).expect("push");
        let mut effects = Vec::new();
        let mut values = Vec::new();
        for _ in 0..2 {
            let output = processor
                .advance_to_boundary()
                .map(|step| step.output)
                .expect("pull")
                .expect("one event");
            let column = output.column_by_slot_id(EFFECT_SLOT).expect("effect");
            let effect = column
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("int8 effect");
            effects.extend(effect.values().iter().copied());
            let column = output.column_by_slot_id(VALUE_SLOT).expect("value");
            let value = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 value");
            values.extend(
                (0..output.len()).map(|row| (!value.is_null(row)).then(|| value.value(row))),
            );
        }
        assert_eq!(effects, [1, 2]);
        assert_eq!(values, [None, Some(42)]);
        assert!(
            processor
                .advance_to_boundary()
                .map(|step| step.output)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn change_event_expand_skips_false_predicate_without_changing_schema() {
        let mut arena = ExprArena::default();
        let false_predicate = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(false)),
            DataType::Boolean,
        );
        let factory = ChangeEventExpandFixtureFactory::new(
            8,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: Some(false_predicate),
                effect: ConnectorRowMutationEffect::Insert,
                assignments: Vec::new(),
            }],
            output_schema(),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        )
        .expect("factory");
        let state = RuntimeState::default();
        let mut operator = factory.create(1, 0);
        install_creator(operator.as_mut(), &state);
        let mut processor =
            crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input()).expect("push");
        let output = processor
            .advance_to_boundary()
            .map(|step| step.output)
            .expect("pull")
            .expect("empty output");
        assert_eq!(output.len(), 0);
        assert_eq!(output.chunk_schema().slot_ids(), &[EFFECT_SLOT, VALUE_SLOT]);
    }

    #[test]
    fn change_event_expand_rejects_non_int8_effect_slot() {
        let err = match ChangeEventExpandFixtureFactory::new(
            9,
            Arc::new(ExprArena::default()),
            Vec::new(),
            schema(vec![
                (
                    EFFECT_SLOT,
                    Field::new("__row_mutation_effect", DataType::Int32, false),
                ),
                (VALUE_SLOT, Field::new("value", DataType::Int32, true)),
            ]),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        ) {
            Ok(_) => panic!("effect slot type must fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("logical effect") && err.to_string().contains("Int8"),
            "{err}"
        );
    }
    #[test]
    fn change_event_predicate_context_keeps_the_original_failure_fact_and_source() {
        use crate::exec::expr::function::FunctionKind;
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use std::error::Error;
        let mut arena = ExprArena::default();
        let message = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("actual predicate failure".into())),
            DataType::Utf8,
        );
        let predicate = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("raise_error"),
                args: vec![message],
            },
            DataType::Boolean,
        );
        let event = ChangeEventRuntimeSpec {
            predicate: Some(predicate),
            effect: ConnectorRowMutationEffect::Insert,
            assignments: Vec::new(),
        };
        let kernel = ChangeEventExpandFixtureKernel {
            name: "context property".into(),
            arena: Arc::new(arena),
            events: Vec::new(),
            output_chunk_schema: output_schema(),
            output_slot_ids: vec![EFFECT_SLOT, VALUE_SLOT],
            effect_slot_id: EFFECT_SLOT,
            construction: None,
            input: None,
            next_event: 0,
        };
        let runtime = RuntimeState::default();
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("predicate context construction"), runtime.error_state(),
        ).unwrap();
        let input = input();
        let owner = factory.begin_input_construction(&input).unwrap();
        let control = runtime.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let ctx = StepContext::new(&control, &mut work);
        let evaluation = ExprEvaluationContext::new(&kernel.arena, owner.as_ref(), &ctx);
        let error = kernel
            .predicate_mask(0, &event, &input, &evaluation)
            .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(error.failure_fact().reason().code(), 518);
        assert!(error.to_string().contains("change event 0 predicate"));
        let original = error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<ExecutionFailure>()
            .unwrap();
        assert_eq!(original.failure_fact(), error.failure_fact());
        assert!(original.to_string().contains("actual predicate failure"));
    }

    #[test]
    fn typed_change_event_predicate_and_assignment_use_the_installed_creator() {
        use crate::exec::expr::function::FunctionKind;
        use arrow::array::{Int64Builder, ListBuilder};
        let mut values = ListBuilder::new(Int64Builder::new());
        values.values().append_slice(&[1, 2]);
        values.append(true);
        values.values().append_value(3);
        values.append(true);
        let array = Arc::new(values.finish()) as ArrayRef;
        let input_schema = schema(vec![(
            INPUT_SLOT,
            Field::new("values", array.data_type().clone(), false),
        )]);
        let input = Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(input_schema.arrow_schema_ref(), vec![array.clone()]).unwrap(),
            input_schema,
        )
        .unwrap();
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(INPUT_SLOT), array.data_type().clone());
        let two = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let predicate = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_contains"),
                args: vec![source, two],
            },
            DataType::Boolean,
        );
        let position = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_position"),
                args: vec![source, two],
            },
            DataType::Int64,
        );
        let factory = ChangeEventExpandFixtureFactory::new(
            10,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: Some(predicate),
                effect: ConnectorRowMutationEffect::Insert,
                assignments: vec![ChangeEventRuntimeOutputExpr {
                    output_slot_id: VALUE_SLOT,
                    expr: Some(position),
                }],
            }],
            output_schema(),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        )
        .unwrap();
        let state = RuntimeState::default();
        let mut operator = factory.create(1, 0);
        install_creator(operator.as_mut(), &state);
        let mut processor =
            crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state);
        processor.accept_without_output(input).unwrap();
        let output = processor.advance_to_boundary().unwrap().output.unwrap();
        assert_eq!(output.len(), 1);
        let values = output.column_by_slot_id(VALUE_SLOT).unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert!(output.chunk_schema().is_typed());
        assert!(!output.construction_storage_lifetime().is_pure());
    }

    #[test]
    fn change_event_runtime_first_failure_preserves_payload_and_driver_context() {
        use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let mut causes: Vec<ExecutionFailure> = vec![
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InstanceFailed,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("exact invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("exact internal")),
            KernelFailure::Operational(KernelDiagnostic::new("exact operation")),
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        causes.push(
            ExecutionFailure::from(KernelFailure::Operational(KernelDiagnostic::new(
                "cancelled",
            )))
            .at_operator(43, PipelineOperation::Push),
        );
        for original in causes {
            let mut arena = ExprArena::default();
            let source = slot_expr(&mut arena);
            let factory = ChangeEventExpandFixtureFactory::new(
                11,
                Arc::new(arena),
                vec![ChangeEventRuntimeSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: vec![ChangeEventRuntimeOutputExpr {
                        output_slot_id: VALUE_SLOT,
                        expr: Some(source),
                    }],
                }],
                output_schema(),
                vec![EFFECT_SLOT, VALUE_SLOT],
                EFFECT_SLOT,
            )
            .unwrap();
            let state = RuntimeState::default();
            let mut operator = factory.create(1, 0);
            install_creator(operator.as_mut(), &state);
            state.error_state().set_error(original.clone());
            state
                .error_state()
                .set_error(KernelFailure::Operational(KernelDiagnostic::new(
                    "later cause cannot replace the first",
                )));
            let mut processor =
                crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state);
            assert_eq!(
                processor.accept_without_output(input()).unwrap_err(),
                original
            );
            assert!(processor.advance_to_boundary().is_err());
        }
    }
}
