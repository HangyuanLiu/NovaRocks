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

//! One local probe driver's resumable membership processor.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use novarocks_execution_contract::{SafeDetail, TaskFailure, TaskFailureCategory};
use novarocks_local_program::MembershipSpec;
use novarocks_types::SlotId;

use super::output::{ResultBits, assemble};
use super::shared::{CompleteRhs, MembershipShared, RhsUnavailable};
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::json_in_pair::{
    JsonPairContext, JsonPairCursor, JsonPairError, JsonPairInput, JsonPairInputId, JsonPairPoll,
    JsonPairTask, JsonPairTruth, JsonPairWork,
};
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Work units one probe driver may spend in one scheduling turn: input
/// bytes, tokens, tree nodes and comparison steps of the pair cursor, plus
/// one per decided row or skipped candidate. At tens of nanoseconds per unit
/// this stays well inside a driver time slice; it bounds a turn's work and
/// is not a value or memory limit.
pub(crate) const MEMBERSHIP_TURN_WORK_UNITS: usize = 64 * 1024;

/// Creates the probe processor of every local probe driver of one node.
pub(crate) struct MembershipProbeFactory {
    name: String,
    shared: Arc<MembershipShared>,
    probe_slot: SlotId,
    negated: bool,
    output_schema: ChunkSchemaRef,
    work_units: usize,
}

impl MembershipProbeFactory {
    /// Validates the frozen output: every probe column in order, then the
    /// one new nullable Boolean result.
    pub(crate) fn try_new(
        shared: Arc<MembershipShared>,
        spec: &MembershipSpec,
        probe_schema: &ChunkSchemaRef,
        output_schema: ChunkSchemaRef,
    ) -> Result<Self, String> {
        let node_id = shared.node_id();
        match probe_schema.slot(spec.probe).map(|slot| slot.data_type()) {
            Some(DataType::Utf8) => {}
            other => {
                return Err(format!(
                    "membership node {node_id} probe slot {} is not the Utf8 JSON carrier: {other:?}",
                    spec.probe
                ));
            }
        }
        let probe_slots = probe_schema.slots();
        let output_slots = output_schema.slots();
        let result = output_slots.last();
        if output_slots.len() != probe_slots.len() + 1
            || probe_slots.iter().zip(output_slots).any(|(probe, output)| {
                probe.slot_id() != output.slot_id() || probe.data_type() != output.data_type()
            })
            || !result.is_some_and(|result| {
                result.slot_id() == spec.result
                    && result.data_type() == &DataType::Boolean
                    && result.nullable()
            })
        {
            return Err(format!(
                "membership node {node_id} output is not its probe columns followed by one nullable Boolean"
            ));
        }
        Ok(Self {
            name: format!("MEMBERSHIP_PROBE (id={node_id})"),
            shared,
            probe_slot: spec.probe,
            negated: spec.negated,
            output_schema,
            work_units: MEMBERSHIP_TURN_WORK_UNITS,
        })
    }

    /// A smaller per-turn allowance, so tests can resume across many turns.
    #[cfg(test)]
    pub(crate) fn with_work_units(mut self, units: usize) -> Self {
        self.work_units = units;
        self
    }
}

impl OperatorFactory for MembershipProbeFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(MembershipProbe {
            name: self.name.clone(),
            shared: Arc::clone(&self.shared),
            probe_slot: self.probe_slot,
            negated: self.negated,
            output_schema: Arc::clone(&self.output_schema),
            work_units: self.work_units,
            task: None,
            rhs: None,
            cursor: None,
            pending: None,
            generation: 0,
            work: JsonPairWork::new(self.work_units),
            yield_requested: false,
            stopped: AtomicBool::new(false),
            finishing: false,
            released: false,
        })
    }
}

/// The one input chunk a probe holds and the exact position of its cursor.
struct Pending {
    chunk: Chunk,
    column: usize,
    position: Position,
}

/// Outer row, candidate batch and row, whether a pair is in flight, whether
/// an UNKNOWN pair was already seen for the current row, and the decided
/// rows so far.
struct Position {
    generation: u64,
    row: usize,
    batch: usize,
    build_row: usize,
    unknown: bool,
    pair_active: bool,
    bits: ResultBits,
}

impl Position {
    fn decide(&mut self, truth: JsonPairTruth, negated: bool) {
        self.bits.set(self.row, truth, negated);
        self.row += 1;
        self.batch = 0;
        self.build_row = 0;
        self.unknown = false;
        self.pair_active = false;
    }
}

enum Advance {
    Yield,
    Done,
}

struct MembershipProbe {
    name: String,
    shared: Arc<MembershipShared>,
    probe_slot: SlotId,
    negated: bool,
    output_schema: ChunkSchemaRef,
    work_units: usize,
    task: Option<JsonPairTask>,
    rhs: Option<Arc<CompleteRhs>>,
    cursor: Option<JsonPairCursor>,
    pending: Option<Pending>,
    generation: u64,
    work: JsonPairWork,
    yield_requested: bool,
    stopped: AtomicBool,
    finishing: bool,
    released: bool,
}

fn pair_failure(state: &RuntimeState, error: JsonPairError) -> String {
    match error {
        JsonPairError::Failed(failure) => state.fail_task(failure),
        JsonPairError::Stopped => state
            .error()
            .unwrap_or_else(|| "JSON membership probe stopped".to_string()),
        JsonPairError::Contract(message) => state.fail_task(TaskFailure::new(
            TaskFailureCategory::Internal,
            SafeDetail::truncating(message),
        )),
    }
}

impl MembershipProbe {
    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        self.pending = None;
        self.cursor = None;
        self.rhs = None;
        self.shared.close_probe();
    }

    fn bind_rhs(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.rhs.is_some() {
            return Ok(());
        }
        self.rhs = Some(match self.shared.rhs() {
            Ok(rhs) => rhs,
            Err(RhsUnavailable::Failed(Some(failure))) => return Err(state.fail_task(failure)),
            Err(RhsUnavailable::Failed(None)) => {
                return Err(state
                    .error()
                    .unwrap_or_else(|| "membership RHS build failed".to_string()));
            }
            Err(RhsUnavailable::Stopped) => {
                return Err(state
                    .error()
                    .unwrap_or_else(|| "membership RHS build was stopped".to_string()));
            }
            Err(RhsUnavailable::Contract(message)) => {
                return Err(state.fail_task(TaskFailure::new(
                    TaskFailureCategory::Internal,
                    SafeDetail::truncating(message),
                )));
            }
        });
        Ok(())
    }

    /// Resumes the pending chunk until every row is decided or this turn's
    /// allowance is spent. Nothing here allocates outside the Task.
    fn advance(&mut self, state: &RuntimeState) -> Result<Advance, String> {
        let Self {
            task,
            rhs,
            cursor,
            pending,
            work,
            stopped,
            negated,
            ..
        } = self;
        let task = task
            .as_ref()
            .ok_or("membership probe is not bound to its task")?;
        let rhs = rhs.as_ref().ok_or("membership probe has no RHS")?;
        let cursor = cursor
            .as_mut()
            .ok_or("membership probe has no pair cursor")?;
        let negated = *negated;
        let stopped: &AtomicBool = stopped;
        let Pending {
            chunk,
            column,
            position,
        } = pending.as_mut().ok_or("membership probe has no input")?;
        let probe = chunk
            .columns()
            .get(*column)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("membership probe lost its Utf8 JSON carrier")?;
        let rows = chunk.len();
        while position.row < rows {
            // Rows decided without a pair still spend one unit each.
            if rhs.is_empty() || probe.is_null(position.row) {
                if !work.spend_one() {
                    return Ok(Advance::Yield);
                }
                // An empty RHS decides FALSE even for a SQL NULL probe; a
                // NULL probe against any candidate is UNKNOWN for each pair.
                let truth = if rhs.is_empty() {
                    JsonPairTruth::False
                } else {
                    JsonPairTruth::Unknown
                };
                position.decide(truth, negated);
                continue;
            }
            let Some(batch) = rhs.batches().get(position.batch) else {
                if !work.spend_one() {
                    return Ok(Advance::Yield);
                }
                let truth = if position.unknown {
                    JsonPairTruth::Unknown
                } else {
                    JsonPairTruth::False
                };
                position.decide(truth, negated);
                continue;
            };
            let values = batch.values()?;
            if position.build_row >= values.len() {
                position.batch += 1;
                position.build_row = 0;
                continue;
            }
            if values.is_null(position.build_row) {
                if !work.spend_one() {
                    return Ok(Advance::Yield);
                }
                position.unknown = true;
                position.build_row += 1;
                continue;
            }
            let input = JsonPairInput {
                id: JsonPairInputId {
                    probe_generation: position.generation,
                    probe_row: position.row,
                    build_batch: position.batch,
                    build_row: position.build_row,
                },
                lhs: Some(probe.value(position.row)),
                rhs: Some(values.value(position.build_row)),
            };
            if !position.pair_active {
                cursor
                    .start(JsonPairInput { ..input })
                    .map_err(|error| pair_failure(state, error))?;
                position.pair_active = true;
            }
            let poll = cursor
                .poll(input, JsonPairContext { task, stopped }, work)
                .map_err(|error| pair_failure(state, error))?;
            match poll {
                JsonPairPoll::Yield => return Ok(Advance::Yield),
                JsonPairPoll::Ready(JsonPairTruth::True) => {
                    position.decide(JsonPairTruth::True, negated);
                }
                JsonPairPoll::Ready(truth) => {
                    position.unknown |= truth == JsonPairTruth::Unknown;
                    position.pair_active = false;
                    position.build_row += 1;
                }
            }
        }
        Ok(Advance::Done)
    }
}

impl Drop for MembershipProbe {
    fn drop(&mut self) {
        // A probe dropped without its driver's close still leaves the node.
        self.release();
    }
}

impl Operator for MembershipProbe {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        let task = self.shared.bind(state)?;
        self.cursor = Some(JsonPairCursor::new(&task));
        self.task = Some(task);
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.release();
        Ok(())
    }

    fn cancel(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.release();
    }

    fn on_driver_failure(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.release();
    }

    fn is_finished(&self) -> bool {
        self.released || (self.finishing && self.pending.is_none())
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for MembershipProbe {
    fn take_yield_request(&mut self) -> bool {
        std::mem::take(&mut self.yield_requested)
    }

    fn begin_turn(&mut self) {
        self.work = JsonPairWork::new(self.work_units);
    }

    fn need_input(&self) -> bool {
        !self.released
            && !self.finishing
            && self.pending.is_none()
            && self.shared.dependency().is_ready()
    }

    fn has_output(&self) -> bool {
        !self.released && self.pending.is_some()
    }

    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        (!self.released).then(|| Arc::clone(self.shared.dependency()))
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.released || self.finishing || self.pending.is_some() {
            return Err(format!(
                "membership probe {} received input while it cannot accept it",
                self.shared.node_id()
            ));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        let column = chunk
            .chunk_schema()
            .index_of(self.probe_slot)
            .filter(|column| {
                chunk.columns()[*column]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some()
            })
            .ok_or_else(|| {
                format!(
                    "membership probe {} input lacks its Utf8 JSON slot {}",
                    self.shared.node_id(),
                    self.probe_slot
                )
            })?;
        let task = self
            .task
            .as_ref()
            .ok_or("membership probe is not bound to its task")?;
        let bits = ResultBits::try_new(task, chunk.len())?;
        self.generation += 1;
        self.pending = Some(Pending {
            chunk,
            column,
            position: Position {
                generation: self.generation,
                row: 0,
                batch: 0,
                build_row: 0,
                unknown: false,
                pair_active: false,
                bits,
            },
        });
        Ok(())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.released || self.pending.is_none() {
            return Ok(None);
        }
        // Each turn reads its input anew from the exact task that bound it.
        self.task
            .as_ref()
            .ok_or("membership probe is not bound to its task")?
            .validate_task(state)
            .map_err(|error| pair_failure(state, error))?;
        self.bind_rhs(state)?;
        match self.advance(state)? {
            Advance::Yield => {
                self.yield_requested = true;
                Ok(None)
            }
            Advance::Done => {
                let pending = self.pending.take().expect("checked pending input");
                let task = self
                    .task
                    .as_ref()
                    .ok_or("membership probe is not bound to its task")?;
                assemble(
                    task,
                    &self.output_schema,
                    pending.chunk,
                    pending.position.bits,
                )
                .map(Some)
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        Ok(())
    }
}
