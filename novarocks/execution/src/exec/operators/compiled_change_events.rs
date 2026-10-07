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

//! Compiled ChangeEventExpand: one input row becomes one output row per
//! change event whose predicate selects it.
//!
//! For each input batch the events run in frozen order. An event's
//! `ChangePredicate` root (TruthOnly) is evaluated over every input row and
//! selects exactly its TRUE rows; an event without a predicate selects every
//! row. Each `ChangeAssignment` root is then evaluated over that event's
//! selected rows only, so an unselected row can raise no assignment error.
//! The event's output rows are its selected rows in input order: assigned
//! channels carry their root's values, unassigned channels are NULL, and the
//! effect channel carries the event's frozen mutation effect code.
//!
//! An assignment value must already have its output channel's exact Arrow
//! type; there is no cast, unlike the legacy operator. Expressions are
//! evaluated only through compiled roots.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, new_null_array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind,
};

use super::compiled_expression::{
    RuntimeKernelControl, evaluate_all, evaluate_selected, instances,
};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// One event's frozen facts, resolved to root indices and output ordinals.
#[derive(Clone)]
struct EventPlan {
    /// Index into the root list of the TruthOnly predicate, if any.
    predicate: Option<usize>,
    /// The effect code written to the effect channel.
    effect: i8,
    /// Per output ordinal: `Some(root index)` for an assigned channel.
    assigned: Vec<Option<usize>>,
}

pub struct CompiledChangeEventProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    events: Arc<[EventPlan]>,
    effect_ordinal: usize,
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}

impl CompiledChangeEventProcessorFactory {
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
            .ok_or_else(|| format!("compiled change-event node {at} is absent"))?;
        let ProgramNodeKind::ChangeEventExpand {
            events,
            output_slot_ids,
            effect_slot_id,
            ..
        } = graph_node.kind()
        else {
            return Err(format!(
                "compiled local node {at} is not a ChangeEventExpand"
            ));
        };
        let invalid = |what: String| format!("compiled change-event node {at} {what}");
        let layout = graph_node.output_layout();
        if layout.slots() != output_slot_ids.as_slice() {
            return Err(invalid("output slots differ from its layout".to_string()));
        }
        let ordinal_of = |slot| {
            layout
                .slots()
                .iter()
                .position(|candidate| *candidate == slot)
        };
        let effect_ordinal = ordinal_of(*effect_slot_id)
            .ok_or_else(|| invalid(format!("has no effect channel {effect_slot_id}")))?;
        let effect_type = layout.schema().field(effect_ordinal).data_type();
        if effect_type != &DataType::Int8 {
            return Err(invalid(format!(
                "effect channel has type {effect_type:?}, not Int8"
            )));
        }
        let mut sites = Vec::new();
        let mut plans = Vec::with_capacity(events.len());
        for (event_ordinal, event) in events.iter().enumerate() {
            let event_index = u32::try_from(event_ordinal)
                .map_err(|_| invalid("has too many events".to_string()))?;
            let predicate = event.predicate.map(|_| {
                sites.push(ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::ChangePredicate { event: event_index },
                });
                sites.len() - 1
            });
            let mut assigned = vec![None; layout.slots().len()];
            let mut covered = vec![false; layout.slots().len()];
            for (assignment_ordinal, assignment) in event.assignments.iter().enumerate() {
                let ordinal = ordinal_of(assignment.output_slot_id).ok_or_else(|| {
                    invalid(format!(
                        "event {event_ordinal} assigns slot {} outside its output",
                        assignment.output_slot_id
                    ))
                })?;
                if ordinal == effect_ordinal || std::mem::replace(&mut covered[ordinal], true) {
                    return Err(invalid(format!(
                        "event {event_ordinal} assigns slot {} twice or assigns the effect",
                        assignment.output_slot_id
                    )));
                }
                if assignment.expr.is_some() {
                    let assignment_index = u32::try_from(assignment_ordinal)
                        .map_err(|_| invalid("has too many assignments".to_string()))?;
                    sites.push(ProgramExpressionRootSite::Node {
                        node,
                        role: ProgramNodeExpressionRole::ChangeAssignment {
                            event: event_index,
                            assignment: assignment_index,
                        },
                    });
                    assigned[ordinal] = Some(sites.len() - 1);
                }
            }
            plans.push(EventPlan {
                predicate,
                effect: event.effect as i8,
                assigned,
            });
        }
        let output = ChunkSchema::from_compiled_layout(layout)?;
        Ok(Self {
            name: format!("COMPILED_CHANGE_EVENT_EXPAND (node={at})"),
            program,
            sites,
            events: Arc::from(plans),
            effect_ordinal,
            output,
            error,
        })
    }
}

impl OperatorFactory for CompiledChangeEventProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledChangeEventProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            events: Arc::clone(&self.events),
            effect_ordinal: self.effect_ordinal,
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

struct CompiledChangeEventProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    events: Arc<[EventPlan]>,
    effect_ordinal: usize,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

fn failure(message: String) -> ExecutionFailure {
    ExecutionFailure::from(message)
}

impl CompiledChangeEventProcessor {
    fn expand(&mut self, input: &RecordBatch) -> ExecutionResult<()> {
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let schema = self.output.arrow_schema_ref();
        for (event_ordinal, event) in self.events.iter().enumerate() {
            let rows: Vec<usize> = match event.predicate {
                Some(root) => {
                    let truth =
                        evaluate_all(&mut instances[root], self.sites[root], input, &self.control)?;
                    let truth = truth
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            failure(format!(
                                "compiled change-event predicate {event_ordinal} is not Boolean"
                            ))
                        })?;
                    // TruthOnly: FALSE and NULL both leave the row unselected.
                    (0..truth.len())
                        .filter(|&row| truth.is_valid(row) && truth.value(row))
                        .collect()
                }
                None => (0..input.num_rows()).collect(),
            };
            if rows.is_empty() {
                continue;
            }
            let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
            for (ordinal, field) in schema.fields().iter().enumerate() {
                let column = if ordinal == self.effect_ordinal {
                    Arc::new(Int8Array::from_value(event.effect, rows.len())) as ArrayRef
                } else if let Some(root) = event.assigned[ordinal] {
                    let values = evaluate_selected(
                        &mut instances[root],
                        self.sites[root],
                        input,
                        &rows,
                        &self.control,
                    )?;
                    if values.data_type() != field.data_type() {
                        return Err(failure(format!(
                            "compiled change-event assignment for {} has type {:?}, not {:?}",
                            field.name(),
                            values.data_type(),
                            field.data_type()
                        )));
                    }
                    values
                } else {
                    new_null_array(field.data_type(), rows.len())
                };
                columns.push(column);
            }
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|error| {
                failure(format!(
                    "compiled change-event event {event_ordinal} output: {error}"
                ))
            })?;
            self.pending.push_back(Chunk::try_new_with_chunk_schema(
                batch,
                Arc::clone(&self.output),
            )?);
        }
        Ok(())
    }
}

impl Operator for CompiledChangeEventProcessor {
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

impl ProcessorOperator for CompiledChangeEventProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending.is_empty()
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if !self.pending.is_empty() {
            return Err(
                "compiled change-event expand received input while output is pending".into(),
            );
        }
        if chunk.is_empty() {
            return Ok(());
        }
        self.expand(&chunk.batch)
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
}
