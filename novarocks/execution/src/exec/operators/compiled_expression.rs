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

//! Processors that evaluate compiled LocalProgram expression roots.
//!
//! Each driver owns one `CompiledExpressionInstance` per actual root. The
//! input batch is the child's frozen output port; the output batch is exactly
//! the node's frozen output layout. Row data errors of a required root and
//! kernel failures stay typed. There is no legacy ExprArena or name dispatch.
//!
//! The kernel control observes the fragment's runtime error state. A unified
//! absolute evaluation deadline and memory admission are not yet loaned to
//! this host; they remain open host obligations, not implied by this control.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult, RequiredExpressionRowError};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// Kernel control backed by the fragment's runtime error state: a recorded
/// failure or cancellation refuses the next checkpoint, and waits are
/// interruptible by the same state.
pub(crate) struct RuntimeKernelControl {
    error: Arc<RuntimeErrorState>,
}
impl RuntimeKernelControl {
    pub(crate) fn new(error: Arc<RuntimeErrorState>) -> Self {
        Self { error }
    }
}
impl KernelEvaluationControl for RuntimeKernelControl {
    fn checkpoint(&self, _work_units: u32) -> Result<(), KernelFailure> {
        if self.error.error().is_some() {
            return Err(KernelFailure::Cancelled);
        }
        Ok(())
    }
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        self.error
            .wait_interruptibly(duration)
            .map_err(|_| KernelFailure::Cancelled)
    }
}

fn root(node: ProgramNodeId, role: ProgramNodeExpressionRole) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node { node, role }
}

/// Evaluate one root over every row of `input` and return the full column.
/// A row data error of the root is a required error and is returned typed.
fn evaluate_all(
    instance: &mut CompiledExpressionInstance,
    site: ProgramExpressionRootSite,
    input: &RecordBatch,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    let selection = Selection::all(input.num_rows());
    let result = instance.evaluate(input, selection, control)?;
    let (selection, values, errors) = result.into_parts();
    if let Some(error) = errors.into_vec().into_iter().next() {
        return Err(RequiredExpressionRowError::try_new(site, selection, error)?.into());
    }
    Ok(values)
}

/// Lazily create one instance per root on the driver's first batch, so a
/// refused preparation is reported on the actual operator operation.
fn instances(
    slot: &mut Option<Vec<CompiledExpressionInstance>>,
    program: &Arc<LocalProgram>,
    sites: &[ProgramExpressionRootSite],
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    if slot.is_some() {
        return Ok(());
    }
    let mut created = Vec::with_capacity(sites.len());
    for site in sites {
        created.push(CompiledExpressionInstance::try_new(
            Arc::clone(program),
            *site,
            control,
        )?);
    }
    *slot = Some(created);
    Ok(())
}

/// Project one compiled node: each output column is one ProjectOutput root.
pub struct CompiledProjectProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}
impl CompiledProjectProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or("compiled Project node is absent")?;
        let novarocks_local_program::ProgramNodeKind::Project { exprs, .. } = graph_node.kind()
        else {
            return Err("compiled node is not a Project".to_string());
        };
        let mut sites = Vec::with_capacity(exprs.len());
        for ordinal in 0..exprs.len() {
            let expression = u32::try_from(ordinal).map_err(|_| "Project width exceeds u32")?;
            sites.push(root(
                node,
                ProgramNodeExpressionRole::ProjectOutput { expression },
            ));
        }
        let output = ChunkSchema::from_compiled_layout(graph_node.output_layout())?;
        Ok(Self {
            name: format!("COMPILED_PROJECT (node={})", node.index()),
            program,
            sites,
            output,
            error,
        })
    }
}
impl OperatorFactory for CompiledProjectProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledProjectProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            pending: None,
            finishing: false,
            finished: false,
        })
    }
}
struct CompiledProjectProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    pending: Option<Chunk>,
    finishing: bool,
    finished: bool,
}
impl Operator for CompiledProjectProcessor {
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
impl ProcessorOperator for CompiledProjectProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending.is_none()
    }
    fn has_output(&self) -> bool {
        self.pending.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.pending.is_some() {
            return Err("compiled Project received input while output is pending".into());
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut columns = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            columns.push(evaluate_all(instance, *site, &chunk.batch, &self.control)?);
        }
        let batch = RecordBatch::try_new(self.output.arrow_schema_ref(), columns)
            .map_err(|error| ExecutionFailure::from(format!("compiled Project output: {error}")))?;
        self.pending = Some(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.output),
        )?);
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.take();
        if self.finishing {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.pending.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

/// Filter one compiled node: keep exactly the rows whose TruthOnly
/// predicate root is TRUE; FALSE and NULL are both excluded.
pub struct CompiledFilterProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    site: ProgramExpressionRootSite,
    error: Arc<RuntimeErrorState>,
}
impl CompiledFilterProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or("compiled Filter node is absent")?;
        if !matches!(
            graph_node.kind(),
            novarocks_local_program::ProgramNodeKind::Filter { .. }
        ) {
            return Err("compiled node is not a Filter".to_string());
        }
        Ok(Self {
            name: format!("COMPILED_FILTER (node={})", node.index()),
            program,
            site: root(node, ProgramNodeExpressionRole::FilterPredicate),
            error,
        })
    }
}
impl OperatorFactory for CompiledFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledFilterProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            site: self.site,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instance: None,
            pending: None,
            finishing: false,
            finished: false,
        })
    }
}
struct CompiledFilterProcessor {
    name: String,
    program: Arc<LocalProgram>,
    site: ProgramExpressionRootSite,
    control: RuntimeKernelControl,
    instance: Option<Vec<CompiledExpressionInstance>>,
    pending: Option<Chunk>,
    finishing: bool,
    finished: bool,
}
impl Operator for CompiledFilterProcessor {
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
impl ProcessorOperator for CompiledFilterProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending.is_none()
    }
    fn has_output(&self) -> bool {
        self.pending.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.pending.is_some() {
            return Err("compiled Filter received input while output is pending".into());
        }
        instances(
            &mut self.instance,
            &self.program,
            std::slice::from_ref(&self.site),
            &self.control,
        )?;
        let instance = &mut self.instance.as_mut().expect("instance was created")[0];
        let truth = evaluate_all(instance, self.site, &chunk.batch, &self.control)?;
        let truth = truth
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ExecutionFailure::from(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                    "compiled Filter predicate is not Boolean",
                )))
            })?;
        // TruthOnly: NULL is not TRUE. Clear nulls before using the mask.
        let mask = if truth.null_count() == 0 {
            truth.clone()
        } else {
            BooleanArray::from_iter(truth.iter().map(|v| Some(v == Some(true))))
        };
        let batch = filter_record_batch(&chunk.batch, &mask)
            .map_err(|error| ExecutionFailure::from(format!("compiled Filter: {error}")))?;
        self.pending = Some(Chunk::try_new_with_chunk_schema(
            batch,
            chunk.chunk_schema_ref(),
        )?);
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.take();
        if self.finishing {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.pending.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}
