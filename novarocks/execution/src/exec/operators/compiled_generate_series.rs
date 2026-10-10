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
//! Closed compiled integer-series processor over its exact bounds Values port.
//!
//! The preceding Values source owns parameter evaluation through the original
//! compiled Frame. This processor receives arrays only and calls the one
//! original Functions computation. Whole-call original Data remains the
//! existing pipeline String channel; controls keep their typed first cause.
//! Expansion/Arrow retained memory and opaque allocation are still OPEN.

use super::compiled_expression::RuntimeKernelControl;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::pipeline::operator::{FinishingWait, Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
    generate_series_core::{self, SeriesFailure, SeriesObservation},
};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use std::sync::Arc;

fn invalid(message: &str) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message)).into()
}

pub(crate) struct CompiledGenerateSeriesProcessorFactory {
    name: String,
    input: SchemaRef,
    parameters: Arc<[usize]>,
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}
impl CompiledGenerateSeriesProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or("compiled series node is absent")?;
        let ProgramNodeKind::GenerateSeries {
            input,
            parameter_slots,
        } = graph_node.kind()
        else {
            return Err("compiled node is not an integer-series source".to_owned());
        };
        let bounds = program
            .graph()
            .nodes()
            .get(input.index())
            .ok_or("compiled series bounds are absent")?;
        let ProgramNodeKind::Values { values } = bounds.kind() else {
            return Err("compiled series bounds are not Values".to_owned());
        };
        let layout = bounds.output_layout();
        if values.num_rows() != 1
            || !matches!(parameter_slots.len(), 2 | 3)
            || graph_node.output_layout().slots().len() != 1
        {
            return Err("compiled series has an invalid bounds or output shape".to_owned());
        }
        let parameters = parameter_slots
            .iter()
            .map(|slot| {
                layout
                    .slots()
                    .iter()
                    .position(|candidate| candidate == slot)
                    .ok_or_else(|| {
                        "compiled series parameter is outside its bounds port".to_owned()
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = ChunkSchema::from_compiled_layout(graph_node.output_layout())?;
        Ok(Self {
            name: format!("COMPILED_GENERATE_SERIES (node={})", node.index()),
            input: layout.schema().clone(),
            parameters: Arc::from(parameters),
            output,
            error,
        })
    }
    fn instance(
        &self,
        control: Arc<dyn KernelEvaluationControl>,
    ) -> CompiledGenerateSeriesProcessor {
        CompiledGenerateSeriesProcessor {
            name: self.name.clone(),
            input: Arc::clone(&self.input),
            parameters: Arc::clone(&self.parameters),
            output: Arc::clone(&self.output),
            control,
            pending: None,
            offset: 0,
            accepted: false,
            finishing: false,
            failed: false,
        }
    }
}
impl OperatorFactory for CompiledGenerateSeriesProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(self.instance(Arc::new(RuntimeKernelControl::new(Arc::clone(&self.error)))))
    }
}

struct Work<'a> {
    control: &'a dyn KernelEvaluationControl,
    pending: u32,
}
impl<'a> Work<'a> {
    fn begin(control: &'a dyn KernelEvaluationControl) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        Ok(Self {
            control,
            pending: 0,
        })
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        let units = std::mem::take(&mut self.pending);
        self.control.checkpoint(units)
    }
    fn observe(&mut self, event: SeriesObservation) -> Result<(), KernelFailure> {
        match event {
            SeriesObservation::Step => {
                self.pending += 1;
                if self.pending == MAX_UNOBSERVED_KERNEL_WORK {
                    self.flush()?;
                }
            }
            SeriesObservation::OpaqueBoundary => self.flush()?,
        };
        Ok(())
    }
}

struct CompiledGenerateSeriesProcessor {
    name: String,
    input: SchemaRef,
    parameters: Arc<[usize]>,
    output: ChunkSchemaRef,
    control: Arc<dyn KernelEvaluationControl>,
    pending: Option<Chunk>,
    offset: usize,
    accepted: bool,
    finishing: bool,
    failed: bool,
}
impl CompiledGenerateSeriesProcessor {
    fn invoke(&self, chunk: &Chunk) -> ExecutionResult<Option<Chunk>> {
        let mut work = Work::begin(self.control.as_ref())?;
        if chunk.len() != 1 || chunk.schema().as_ref() != self.input.as_ref() {
            return Err(invalid(
                "compiled series input differs from its frozen bounds port",
            ));
        }
        let batch = &chunk.batch;
        let start = batch.column(self.parameters[0]);
        let stop = batch.column(self.parameters[1]);
        let step = self.parameters.get(2).map(|ordinal| batch.column(*ordinal));
        let expanded =
            generate_series_core::expand_observed(start, stop, step, 1, false, &mut |event| {
                work.observe(event)
            });
        let expanded = match expanded {
            Ok(value) => value,
            Err(SeriesFailure::Control(cause)) => return Err(cause.into()),
            Err(SeriesFailure::Data(message)) => return Err(message.into()),
        };
        if expanded.total_rows == 0 {
            work.flush()?;
            return Ok(None);
        }
        let data_type = self.output.arrow_schema_ref().field(0).data_type().clone();
        let column = generate_series_core::result_column_observed(
            expanded.values,
            &[data_type],
            &mut |event| work.observe(event),
        );
        let column = match column {
            Ok(column) => column,
            Err(SeriesFailure::Control(cause)) => return Err(cause.into()),
            Err(SeriesFailure::Data(message)) => return Err(message.into()),
        };
        work.flush()?;
        let batch = RecordBatch::try_new(self.output.arrow_schema_ref(), vec![column])
            .map_err(|error| ExecutionFailure::from(error.to_string()))?;
        work.flush()?;
        let output = Chunk::try_new_with_chunk_schema(batch, Arc::clone(&self.output))
            .map_err(ExecutionFailure::from)?;
        work.flush()?;
        Ok(Some(output))
    }
    fn alive(&self) -> ExecutionResult<()> {
        if self.failed {
            Err(KernelFailure::InstanceFailed.into())
        } else {
            Ok(())
        }
    }
}
impl Operator for CompiledGenerateSeriesProcessor {
    fn name(&self) -> &str {
        &self.name
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
    fn is_finished(&self) -> bool {
        self.failed || (self.finishing && self.pending.is_none())
    }
    fn cancel(&mut self) {
        self.failed = true;
        self.pending = None;
    }
}
impl ProcessorOperator for CompiledGenerateSeriesProcessor {
    fn need_input(&self) -> bool {
        !self.failed && !self.accepted && !self.finishing
    }
    fn has_output(&self) -> bool {
        !self.failed && self.pending.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        self.alive()?;
        if self.accepted || self.finishing {
            self.failed = true;
            return Err(invalid("compiled series accepts its bounds port once"));
        }
        self.accepted = true;
        match self.invoke(&chunk) {
            Ok(output) => {
                self.pending = output;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                self.pending = None;
                Err(error)
            }
        }
    }
    fn pull_chunk(&mut self, state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        self.alive()?;
        if let Err(cause) = self.control.checkpoint(0) {
            self.failed = true;
            self.pending = None;
            return Err(cause.into());
        }
        let Some(chunk) = &self.pending else {
            return Ok(None);
        };
        let total = chunk.len();
        let rows = state.chunk_size().max(1).min(total - self.offset);
        let output = chunk.slice(self.offset, rows);
        if let Err(cause) = self.control.checkpoint(0) {
            self.failed = true;
            self.pending = None;
            return Err(cause.into());
        }
        self.offset += rows;
        if self.offset == total {
            self.pending = None;
        }
        Ok(Some(output))
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.alive()?;
        self.finishing = true;
        Ok(())
    }
    fn finishing_wait(&self) -> FinishingWait {
        if self.pending.is_some() {
            FinishingWait::OwedOutput
        } else {
            FinishingWait::Complete
        }
    }
}

#[cfg(test)]
#[path = "compiled_generate_series_control_tests.rs"]
mod control_tests;
