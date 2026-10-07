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

//! Compiled TableFunction: every input row is one parent of one exact table
//! call, and the output is the relation it produces joined to that parent.
//!
//! The kernel is the program's prepared table cursor at its `Table` call site,
//! never a function looked up by name. Each input batch is one invocation over
//! all its rows: its parameters are the frozen parameter channels of the
//! argument Project that feeds this node, which already evaluated every
//! argument root through compiled instances. The host drives the cursor with
//! an explicit full step grant -- a page of up to one chunk of rows, every
//! parent's completion and every parent's error -- so a step that still asks
//! for capacity can never make progress and is refused instead of retried.
//!
//! The host owns what the kernel does not: it rejects a row or a completion
//! for a parent that already completed, and an end before every parent
//! completed; a parent row error is a required error and fails the operator;
//! LEFT OUTER emits one NULL-extended row for each parent that completed
//! without a row. Output rows follow parent order, a parent's rows in the
//! order the kernel produced them, so the operator preserves its input's
//! order and every pass-through ordering. Rows stay on their driver.
//!
//! Open host obligation: the produced relation of one input batch is retained
//! until its output chunks are pulled. The step grant bounds each page, not
//! the expansion of a whole batch; memory admission for that expansion is not
//! yet loaned to this host.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array, new_empty_array};
use arrow::compute::{concat, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    EvaluatedArgument, PreparedTableKernel, SelectedTableInput, Selection, TableCursorStep,
    TableEvaluationCursor, TablePageCapacity, TableStepCapacity,
};
use novarocks_local_program::{
    LocalProgram, ProgramCallSite, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate,
    TableFunctionOutputSlot,
};
use novarocks_types::SlotId;

use super::compiled_expression::RuntimeKernelControl;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// The producer of one output channel, in output order.
#[derive(Clone, Copy, Debug)]
enum Producer {
    /// An outer pass-through channel at this input ordinal.
    Outer(usize),
    /// This produced relation column.
    Result(usize),
}

pub struct CompiledTableFunctionProcessorFactory {
    name: String,
    node: usize,
    kernel: Arc<dyn PreparedTableKernel>,
    /// The frozen input port; channels are read by position.
    input: SchemaRef,
    params: Arc<[usize]>,
    producers: Arc<[Producer]>,
    left_outer: bool,
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}

impl CompiledTableFunctionProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let at = node.index();
        let nodes = program.graph().nodes();
        let graph_node = nodes
            .get(at)
            .ok_or_else(|| format!("compiled TableFunction node {at} is absent"))?;
        let ProgramNodeKind::TableFunction {
            input,
            param_slots,
            outer_slots,
            fn_result_slots,
            fn_result_required,
            is_left_join,
            output_slot_sources,
            ..
        } = graph_node.kind()
        else {
            return Err(format!("compiled local node {at} is not a TableFunction"));
        };
        let invalid = |what: String| format!("compiled TableFunction at local node {at} {what}");
        if !*fn_result_required {
            return Err(invalid(
                "with optional function results is not executable yet".to_string(),
            ));
        }
        let Some(ProgramStateTemplate::TableCursor { kernel, .. }) =
            program.state_template(ProgramCallSite::Table { node })
        else {
            return Err(invalid("has no prepared table cursor".to_string()));
        };
        let kernel = Arc::clone(kernel);
        let contract = Arc::clone(kernel.contract());
        let argument_types = contract.argument_types().collect::<Vec<_>>();
        let result_types = contract.result_types();
        if param_slots.len() != argument_types.len() || fn_result_slots.len() != result_types.len()
        {
            return Err(invalid(format!(
                "has {} parameters and {} results for a call of {} arguments and {} results",
                param_slots.len(),
                fn_result_slots.len(),
                argument_types.len(),
                result_types.len()
            )));
        }
        let input_layout = nodes
            .get(input.index())
            .ok_or_else(|| invalid("has no input".to_string()))?
            .output_layout();
        let layout = graph_node.output_layout();
        let input_ordinal = |slot: SlotId| {
            input_layout
                .slots()
                .iter()
                .position(|candidate| *candidate == slot)
                .ok_or_else(|| invalid(format!("reads slot {slot} outside its input")))
        };
        let input_type = |ordinal: usize| input_layout.schema().field(ordinal).data_type();
        let mut params = Vec::with_capacity(param_slots.len());
        for (slot, expected) in param_slots.iter().zip(&argument_types) {
            let ordinal = input_ordinal(*slot)?;
            if input_type(ordinal) != &expected.data_type {
                return Err(invalid(format!(
                    "parameter slot {slot} has type {:?}, not {:?}",
                    input_type(ordinal),
                    expected.data_type
                )));
            }
            params.push(ordinal);
        }
        if output_slot_sources.len() != layout.slots().len() {
            return Err(invalid(
                "has an output source count different from its layout".to_string(),
            ));
        }
        let mut producers = Vec::with_capacity(output_slot_sources.len());
        for (ordinal, source) in output_slot_sources.iter().enumerate() {
            let output_type = layout.schema().field(ordinal).data_type();
            let (producer, source_type) = match source {
                TableFunctionOutputSlot::Outer { slot } => {
                    if !outer_slots.contains(slot) {
                        return Err(invalid(format!(
                            "passes through slot {slot} that is not an outer slot"
                        )));
                    }
                    let input = input_ordinal(*slot)?;
                    (Producer::Outer(input), input_type(input))
                }
                TableFunctionOutputSlot::Result { index } => {
                    let result = result_types.get(*index).ok_or_else(|| {
                        invalid(format!("publishes result {index} outside its relation"))
                    })?;
                    (Producer::Result(*index), &result.data_type)
                }
            };
            if source_type != output_type {
                return Err(invalid(format!(
                    "output channel {ordinal} has type {output_type:?}, not {source_type:?}"
                )));
            }
            producers.push(producer);
        }
        let output = ChunkSchema::from_compiled_layout(layout)?;
        Ok(Self {
            name: format!("COMPILED_TABLE_FUNCTION (node={at})"),
            node: at,
            kernel,
            input: input_layout.schema().clone(),
            params: Arc::from(params),
            producers: Arc::from(producers),
            left_outer: *is_left_join,
            output,
            error,
        })
    }
}

impl OperatorFactory for CompiledTableFunctionProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledTableFunctionProcessor {
            name: self.name.clone(),
            node: self.node,
            kernel: Arc::clone(&self.kernel),
            input_schema: Arc::clone(&self.input),
            params: Arc::clone(&self.params),
            producers: Arc::clone(&self.producers),
            left_outer: self.left_outer,
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

struct CompiledTableFunctionProcessor {
    name: String,
    node: usize,
    kernel: Arc<dyn PreparedTableKernel>,
    input_schema: SchemaRef,
    params: Arc<[usize]>,
    producers: Arc<[Producer]>,
    left_outer: bool,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    /// Output chunks of the current input batch, in output order.
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

/// The rows one invocation produced, in arrival order, with their parents.
struct Produced {
    /// Each produced row's parent ordinal.
    parents: Vec<usize>,
    /// Every produced relation column, concatenated in arrival order.
    columns: Vec<ArrayRef>,
    /// Rows produced for each parent.
    counts: Vec<usize>,
}

impl CompiledTableFunctionProcessor {
    fn failure(&self, what: impl std::fmt::Display) -> ExecutionFailure {
        ExecutionFailure::from(format!(
            "compiled TableFunction at local node {} {what}",
            self.node
        ))
    }

    /// Run one exact invocation over every row of `batch` to its end.
    fn invoke(&self, batch: &RecordBatch, page_rows: usize) -> ExecutionResult<Produced> {
        let parents = batch.num_rows();
        let contract = self.kernel.contract();
        let arguments = self
            .params
            .iter()
            .map(|ordinal| EvaluatedArgument::Column(batch.column(*ordinal)))
            .collect::<Vec<_>>();
        let selection = Selection::all(parents);
        let input =
            SelectedTableInput::try_new(contract.as_ref(), selection, &arguments, &self.control)?;
        let mut cursor =
            TableEvaluationCursor::begin(Arc::clone(&self.kernel), input, &self.control)?;
        // The full grant: one chunk of rows, and every parent's completion
        // and error. Each channel needs at most one fact to make progress.
        let capacity = TableStepCapacity {
            page: TablePageCapacity {
                rows: page_rows.max(1),
                completions: parents,
            },
            parent_errors: parents,
        };
        let mut completed = vec![false; parents];
        let mut counts = vec![0usize; parents];
        let mut produced = Vec::new();
        let mut pages = Vec::new();
        loop {
            let page = match cursor.next(capacity, &self.control)? {
                TableCursorStep::Page(page) => page,
                TableCursorStep::CapacityRequired(required) => {
                    return Err(self.failure(format!(
                        "table cursor requires {required:?} beyond its full step grant"
                    )));
                }
            };
            // A parent's row error is a required error of this operator.
            if let Some(error) = page.parent_errors.first() {
                let row = selection
                    .row(error.selected_ordinal())
                    .ok_or_else(|| self.failure("reported an error outside its invocation"))?;
                return Err(self.failure(format!("failed at batch row {row}: {}", error.message())));
            }
            for &parent in page.parent_ordinals.iter() {
                if completed[parent] {
                    return Err(self.failure("produced a row after its parent completed"));
                }
                counts[parent] += 1;
                produced.push(parent);
            }
            for &parent in page.completed_parents.iter() {
                if std::mem::replace(&mut completed[parent], true) {
                    return Err(self.failure("completed one parent twice"));
                }
            }
            if !page.parent_ordinals.is_empty() {
                pages.push(page.columns);
            }
            if page.eof {
                break;
            }
        }
        if completed.iter().any(|done| !done) {
            return Err(self.failure("ended before every parent completed"));
        }
        cursor.finish(&self.control)?;
        drop(cursor);
        let mut columns = Vec::with_capacity(contract.result_types().len());
        for (index, result) in contract.result_types().iter().enumerate() {
            let column = if pages.is_empty() {
                new_empty_array(&result.data_type)
            } else {
                let parts = pages
                    .iter()
                    .map(|page| page[index].as_ref())
                    .collect::<Vec<&dyn Array>>();
                concat(&parts).map_err(|error| self.failure(format!("result {index}: {error}")))?
            };
            columns.push(column);
        }
        Ok(Produced {
            parents: produced,
            columns,
            counts,
        })
    }

    /// Assemble `produced` with its parents into output chunks of at most
    /// `chunk_rows` rows, in parent order.
    fn assemble(
        &self,
        batch: &RecordBatch,
        produced: Produced,
        chunk_rows: usize,
    ) -> ExecutionResult<VecDeque<Chunk>> {
        let parents = batch.num_rows();
        // Rows of each parent: its produced rows, or one NULL-extended row for
        // a LEFT OUTER parent that completed without one.
        let mut starts = Vec::with_capacity(parents + 1);
        let mut total = 0usize;
        for &count in &produced.counts {
            starts.push(total);
            let rows = if count == 0 && self.left_outer {
                1
            } else {
                count
            };
            total = total
                .checked_add(rows)
                .ok_or_else(|| self.failure("output row count exceeds the host range"))?;
        }
        starts.push(total);
        let mut parent_rows = vec![0u64; total];
        let mut result_rows: Vec<Option<u64>> = vec![None; total];
        let mut next = starts.clone();
        for (row, &parent) in produced.parents.iter().enumerate() {
            let at = next[parent];
            next[parent] += 1;
            parent_rows[at] = parent as u64;
            result_rows[at] = Some(row as u64);
        }
        for parent in 0..parents {
            if next[parent] < starts[parent + 1] {
                // The NULL-extended row of an empty LEFT OUTER parent.
                parent_rows[next[parent]] = parent as u64;
                next[parent] += 1;
            }
        }
        let schema = self.output.arrow_schema_ref();
        let mut chunks = VecDeque::new();
        let mut start = 0usize;
        while start < total {
            let end = total.min(start + chunk_rows.max(1));
            let outer = UInt64Array::from(parent_rows[start..end].to_vec());
            let results = UInt64Array::from(result_rows[start..end].to_vec());
            let mut columns = Vec::with_capacity(self.producers.len());
            for producer in self.producers.iter() {
                let column = match producer {
                    Producer::Outer(input) => take(batch.column(*input).as_ref(), &outer, None),
                    Producer::Result(index) => {
                        take(produced.columns[*index].as_ref(), &results, None)
                    }
                }
                .map_err(|error| self.failure(format!("output gather: {error}")))?;
                columns.push(column);
            }
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
                .map_err(|error| self.failure(format!("output: {error}")))?;
            chunks.push_back(Chunk::try_new_with_chunk_schema(
                batch,
                Arc::clone(&self.output),
            )?);
            start = end;
        }
        Ok(chunks)
    }
}

impl Operator for CompiledTableFunctionProcessor {
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

impl ProcessorOperator for CompiledTableFunctionProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending.is_empty()
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if !self.pending.is_empty() {
            return Err(self.failure("received input while output is pending"));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        if chunk.batch.schema().as_ref() != self.input_schema.as_ref() {
            return Err(self.failure("input differs from its frozen input port"));
        }
        let chunk_rows = state.chunk_size();
        let produced = self.invoke(&chunk.batch, chunk_rows)?;
        self.pending = self.assemble(&chunk.batch, produced, chunk_rows)?;
        Ok(())
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
