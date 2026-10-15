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

//! The compiled Values source for Values with dynamic cells.
//!
//! Each dynamic cell is a `ValuesCell { row, column }` root. On its opening
//! turn the source evaluates every dynamic cell exactly once, in row-major
//! order, through the one compiled evaluator over an explicit empty port of
//! one row: the same value, NULL and row-error effects as v1's per-cell
//! evaluation. A cell's row error is a typed required-expression error of its
//! own root, and the first failing cell in row-major order is the one
//! reported. Each column is then assembled exactly in its frozen layout type
//! from its constant cells and its evaluated cells; no cell is cast, coerced
//! or defaulted here. All rows are emitted from the one source driver.

use std::sync::Arc;

use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::interleave::interleave;
use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_functions::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
    selected_copy::{self, CopyConstructionError, CopyError},
};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, StaticValuesBacking,
};
use novarocks_type_contract::{ValueSchemaCreation, arrow_data_types_exact_observed};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

fn invalid(message: &str) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message)).into()
}

/// Source factory for one compiled Values node that has dynamic cells.
pub(crate) struct CompiledValuesSourceFactory {
    name: String,
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    error: Arc<RuntimeErrorState>,
}

impl CompiledValuesSourceFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or("compiled Values node is absent")?;
        let ProgramNodeKind::Values { values } = graph_node.kind() else {
            return Err("compiled node is not a Values source".to_string());
        };
        if !values.layout().is_typed() {
            return Err("compiled Values require their complete frozen layout".into());
        }
        Ok(Self {
            name: format!("COMPILED_VALUES (node={})", node.index()),
            program,
            node,
            error,
        })
    }
}

impl OperatorFactory for CompiledValuesSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledValuesSource {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            node: self.node,
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            driver_id,
            emitted: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct CompiledValuesSource {
    name: String,
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: RuntimeKernelControl,
    driver_id: i32,
    emitted: bool,
}

impl Operator for CompiledValuesSource {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let ProgramNodeKind::Values { values } =
            self.program.graph().nodes()[self.node.index()].kind()
        else {
            return Err(invalid("compiled Values node changed"));
        };
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_compiled_layout_construction(values.layout())?;
        let output = ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
            values.layout(),
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )?;
        self.output = Some(output);
        Ok(())
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }
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
        self.emitted
    }
}

impl ProcessorOperator for CompiledValuesSource {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        !self.emitted
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
        Err("compiled Values source does not accept input".into())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        // The source pipeline has one driver; every row comes from it, so the
        // cells are evaluated once and rows keep their frozen order.
        if self.driver_id != 0 {
            return Ok(None);
        }
        let output = self
            .output
            .as_ref()
            .ok_or_else(|| invalid("compiled Values source was not prepared"))?;
        materialize(&self.program, self.node, output, &self.control).map(Some)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }
}

fn reserve<T>(
    count: usize,
    owner: &dyn InputConstructionOwner,
    work: &mut Work<'_>,
) -> ExecutionResult<Vec<T>> {
    let layout = std::alloc::Layout::array::<T>(count).map_err(|_| {
        ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    work.flush()?;
    owner.request(layout)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}
fn copy_failure(error: CopyConstructionError<ConstructionFailure>) -> ExecutionFailure {
    match error {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(CopyError::Control(cause)) => cause.into(),
        CopyConstructionError::Copy(CopyError::Extent) => KernelFailure::ResourceExhausted.into(),
        CopyConstructionError::Copy(CopyError::Invalid(_) | CopyError::Unsupported(_)) => {
            invalid("Values column carrier lacks selected-copy preflight")
        }
    }
}

/// Evaluate every dynamic cell once and assemble the frozen layout.
fn materialize(
    program: &Arc<LocalProgram>,
    node: ProgramNodeId,
    output: &ChunkSchemaRef,
    control: &RuntimeKernelControl,
) -> ExecutionResult<Chunk> {
    let ProgramNodeKind::Values { values } = program.graph().nodes()[node.index()].kind() else {
        return Err(invalid("compiled Values node changed"));
    };
    let owner = control.construction_owner(output.as_ref())?;
    let storage = owner
        .storage_lifetime()
        .retaining_observed::<crate::exec::pipeline::operator::ConstructionFailure>(
            &values.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
    if let Some(projection) = values.batch_projection() {
        owner.request(std::alloc::Layout::array::<ArrayRef>(projection.record_batch().num_columns()).map_err(|_|crate::exec::pipeline::operator::ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let columns = projection.record_batch().columns().to_vec();
        return Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            crate::exec::pipeline::operator::ConstructionFailure,
            _,
        >(
            output.clone(),
            columns,
            projection.record_batch().num_rows(),
            storage,
            |step| owner.observe(0, &[], step),
        )?);
    }
    let StaticValuesBacking::Cells {
        rows,
        constants,
        dynamic,
    } = values.backing()
    else {
        return Err(invalid("compiled Values source has no dynamic cell"));
    };
    let schema = values.layout().schema();
    let mut work = Work::new(control);
    work.flush()?;
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            Schema,
        >())
        .map_err(ConstructionFailure::Resource)?,
    )?;
    let empty = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|error| ExecutionFailure::from(format!("compiled Values empty port: {error}")))?;
    // Row-major: the first failing cell is the one v1 would have reported.
    let mut evaluated = reserve(dynamic.len(), owner.as_ref(), &mut work)?;
    for cell in dynamic {
        let site = ProgramExpressionRootSite::Node {
            node,
            role: ProgramNodeExpressionRole::ValuesCell {
                row: cell.row,
                column: cell.column,
            },
        };
        let mut instance = CompiledExpressionInstance::try_new_with_allocator(
            Arc::clone(program),
            site,
            control,
            control.allocator(),
        )?;
        let value = evaluate_all(&mut instance, site, &empty, control)?;
        let field = schema
            .fields()
            .get(cell.column as usize)
            .ok_or_else(|| invalid("Values cell column is outside its layout"))?;
        work.flush()?;
        let exact = value.len() == 1
            && arrow_data_types_exact_observed(value.data_type(), field.data_type(), || {
                work.step()
            })?;
        if !exact {
            return Err(invalid(
                "Values cell result differs from its frozen column type",
            ));
        }
        evaluated.push(value);
        work.step()?;
    }
    let mut columns = reserve(constants.len(), owner.as_ref(), &mut work)?;
    for (ordinal, (constant, field)) in constants.iter().zip(schema.fields()).enumerate() {
        // `sources[0]` holds this column's constant cells in row order; each
        // evaluated cell of the column follows as its own one-row source.
        let mut count = 0usize;
        for cell in dynamic {
            if cell.column as usize == ordinal {
                count += 1;
            }
            work.step()?;
        }
        let mut sources: Vec<ArrayRef> = reserve(
            count
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?,
            owner.as_ref(),
            &mut work,
        )?;
        sources.push(Arc::clone(constant));
        let mut dynamic_rows = reserve(count, owner.as_ref(), &mut work)?;
        for (cell, value) in dynamic.iter().zip(&evaluated) {
            if cell.column as usize == ordinal {
                dynamic_rows.push(cell.row as usize);
                sources.push(Arc::clone(value));
            }
            work.step()?;
        }
        if dynamic_rows.is_empty() {
            columns.push(Arc::clone(constant));
            continue;
        }
        let mut choices = reserve(rows, owner.as_ref(), &mut work)?;
        let mut next_constant = 0usize;
        let mut next_dynamic = 0usize;
        for row in 0..rows {
            if dynamic_rows.get(next_dynamic) == Some(&row) {
                next_dynamic += 1;
                choices.push((next_dynamic, 0));
            } else {
                choices.push((0, next_constant));
                next_constant += 1;
            }
            work.step()?;
        }
        if next_constant != constant.len() || next_dynamic != dynamic_rows.len() {
            return Err(invalid("Values cell sources do not cover their column"));
        }
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation = ValueSchemaCreation::new(storage.clone(), &mut request, &mut opaque);
        work.flush()?;
        selected_copy::preflight_interleave_construction(
            field.data_type(),
            &sources,
            &choices,
            &mut creation,
            control,
        )
        .map_err(copy_failure)?;
        let mut borrowed: Vec<&dyn Array> = reserve(sources.len(), owner.as_ref(), &mut work)?;
        for source in &sources {
            borrowed.push(source.as_ref());
            work.step()?;
        }
        work.flush()?;
        let column = interleave(&borrowed, &choices)
            .map_err(|error| ExecutionFailure::from(format!("compiled Values column: {error}")))?;
        work.flush()?;
        columns.push(column);
    }
    work.flush()?;
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        crate::exec::pipeline::operator::ConstructionFailure,
        _,
    >(output.clone(), columns, rows, storage, |step| {
        owner.observe(0, &[], step)
    })?)
}

/// Bounded observation of this source's own assembly work.
struct Work<'a> {
    control: &'a dyn KernelEvaluationControl,
    pending: u32,
}

impl<'a> Work<'a> {
    fn new(control: &'a dyn KernelEvaluationControl) -> Self {
        Self {
            control,
            pending: 0,
        }
    }
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.pending += 1;
        if self.pending == MAX_UNOBSERVED_KERNEL_WORK {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        self.control.checkpoint(self.pending)?;
        self.pending = 0;
        Ok(())
    }
    fn observe(&mut self, boundary: bool) -> Result<(), KernelFailure> {
        if boundary { self.flush() } else { self.step() }
    }
}
