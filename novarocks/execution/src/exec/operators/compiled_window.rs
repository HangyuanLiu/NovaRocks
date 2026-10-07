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

//! Compiled Analytic (window) evaluation over pure window kernels.
//!
//! One driver owns one processor; the builder gathers the instance input to
//! that one driver. The input arrives sorted by the partition keys and then by
//! the order keys: the checked plan places an analytic or global Sort below
//! every window that partitions or orders, and the compiler admits only that
//! source. The processor relies on that order and never re-sorts.
//!
//! Evaluation boundary: every partition key, order key and call argument is a
//! compiled root (`WindowPartition`, `WindowOrder`, `WindowInput`), evaluated
//! by this driver's own instance exactly once per arriving row, in arrival
//! order; a row data error of any root is a required error. Each call runs
//! through the exact prepared window kernel the compiler attached at its
//! `ProgramCallSite::Window`. No implementation is selected by name and no
//! legacy expression is read.
//!
//! Equality: two rows are in one partition when their normalized partition
//! keys are equal, and peers when their normalized order keys are equal.
//! Equality is that of the sort's total order, through the Arrow row format of
//! the same normalized key arrays the compiled Sort orders by: NULL equals
//! NULL, a float equals only the same bits (one NaN equals itself, and `-0.0`
//! and `+0.0` differ, exactly as the sort separates them).
//!
//! Partitions: rows of the open partition are buffered with their evaluated
//! keys and arguments. A partition closes when a row with different partition
//! keys arrives or at finish; its peer groups and each call's frame table are
//! computed over the complete partition, every call's kernel evaluates it
//! once, and the partition's rows are emitted with their call values. A whole
//! partition is held in memory (no spill, ADR-0162).

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::{concat, concat_batches};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use novarocks_functions::{
    EvaluatedArgument, FullPartitionWindowInput, PreparedWindowKernel, Selection,
    WindowEvaluationPartition, WindowPartitionInput, WindowRowRange,
};
use novarocks_local_program::{
    AnalyticOutputColumn, LocalProgram, ProgramCallSite, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate, WindowFrame,
    WindowFunctionKind,
};

use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use super::compiled_window_geometry::{admit_frame, frame_table, peer_groups};
use super::sort::normalize_sort_key_array;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

fn failure(context: &str, error: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from(format!("{context}: {error}"))
}

/// One call's exact prepared kernel, its explicit frame and its roots.
#[derive(Clone)]
struct CallPlan {
    kernel: Arc<dyn PreparedWindowKernel>,
    frame: WindowFrame,
    /// Offset of this call's argument roots in the processor's root list.
    first_root: usize,
    roots: usize,
}

/// Where one output column comes from.
#[derive(Clone, Copy)]
enum OutputSource {
    Input(usize),
    Call(usize),
}

pub struct CompiledWindowProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    partition_keys: usize,
    order_keys: usize,
    calls: Vec<CallPlan>,
    columns: Vec<OutputSource>,
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}

impl CompiledWindowProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let at = node.index();
        let nodes = program.graph().nodes();
        let graph_node = nodes
            .get(at)
            .ok_or_else(|| format!("compiled Analytic node {at} is absent"))?;
        let ProgramNodeKind::Analytic {
            input,
            partition_exprs,
            order_by_exprs,
            functions,
            output_columns,
        } = graph_node.kind()
        else {
            return Err(format!("compiled local node {at} is not an Analytic"));
        };
        let key = |count: usize, role: fn(u32) -> ProgramNodeExpressionRole| {
            (0..count)
                .map(|key| {
                    u32::try_from(key)
                        .map(|key| ProgramExpressionRootSite::Node {
                            node,
                            role: role(key),
                        })
                        .map_err(|_| {
                            format!("compiled Analytic at local node {at} has too many keys")
                        })
                })
                .collect::<Result<Vec<_>, String>>()
        };
        let mut sites = key(partition_exprs.len(), |key| {
            ProgramNodeExpressionRole::WindowPartition { key }
        })?;
        sites.extend(key(order_by_exprs.len(), |key| {
            ProgramNodeExpressionRole::WindowOrder { key }
        })?);
        let mut calls = Vec::with_capacity(functions.len());
        for (ordinal, function) in functions.iter().enumerate() {
            let call = u32::try_from(ordinal)
                .map_err(|_| format!("compiled Analytic at local node {at} has too many calls"))?;
            if !matches!(function.kind, WindowFunctionKind::Prepared) {
                return Err(format!(
                    "compiled Analytic call {call} at local node {at} is not a prepared window call"
                ));
            }
            let frame = function.frame.ok_or_else(|| {
                format!("compiled Analytic call {call} at local node {at} has no explicit frame")
            })?;
            admit_frame(&frame).map_err(|error| {
                format!("compiled Analytic call {call} at local node {at}: {error}")
            })?;
            let Some(ProgramStateTemplate::WindowPartition { kernel, .. }) =
                program.state_template(ProgramCallSite::Window { node, call })
            else {
                return Err(format!(
                    "compiled Analytic call {call} at local node {at} has no prepared window kernel"
                ));
            };
            let kernel = Arc::clone(kernel);
            let contract = kernel.contract();
            if contract.order_argument_types().len() != 0 {
                return Err(format!(
                    "compiled Analytic call {call} at local node {at} with function ORDER BY is not executable"
                ));
            }
            let roots = contract.logical_argument_types().len();
            if function.args.len() != roots {
                return Err(format!(
                    "compiled Analytic call {call} at local node {at} has {} roots for {roots} channels",
                    function.args.len()
                ));
            }
            let first_root = sites.len();
            for argument in 0..roots {
                let argument = u32::try_from(argument).map_err(|_| {
                    format!("compiled Analytic call {call} at local node {at} has too many inputs")
                })?;
                sites.push(ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::WindowInput { call, argument },
                });
            }
            calls.push(CallPlan {
                kernel,
                frame,
                first_root,
                roots,
            });
        }
        let input_slots = nodes
            .get(input.index())
            .ok_or_else(|| format!("compiled Analytic at local node {at} has no input node"))?
            .output_layout()
            .slots();
        let mut columns = Vec::with_capacity(output_columns.len());
        for column in output_columns {
            columns.push(match column {
                AnalyticOutputColumn::InputSlotId(slot) => OutputSource::Input(
                    input_slots
                        .iter()
                        .position(|candidate| candidate == slot)
                        .ok_or_else(|| {
                            format!(
                                "compiled Analytic at local node {at} outputs slot {} its input lacks",
                                slot.as_u32()
                            )
                        })?,
                ),
                AnalyticOutputColumn::Window(call) if *call < calls.len() => {
                    OutputSource::Call(*call)
                }
                AnalyticOutputColumn::Window(call) => {
                    return Err(format!(
                        "compiled Analytic at local node {at} outputs absent call {call}"
                    ));
                }
            });
        }
        let output = ChunkSchema::from_compiled_layout(graph_node.output_layout())?;
        if output.arrow_schema_ref().fields().len() != columns.len() {
            return Err(format!(
                "compiled Analytic at local node {at} output width differs from its columns"
            ));
        }
        let (partition_keys, order_keys) = (partition_exprs.len(), order_by_exprs.len());
        Ok(Self {
            name: format!("COMPILED_ANALYTIC (node={at})"),
            program,
            sites,
            partition_keys,
            order_keys,
            calls,
            columns,
            output,
            error,
        })
    }
}

impl OperatorFactory for CompiledWindowProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledWindowProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            partition_keys: self.partition_keys,
            order_keys: self.order_keys,
            calls: self.calls.clone(),
            columns: self.columns.clone(),
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            partition_rows: None,
            order_rows: None,
            last_partition: None,
            open: Vec::new(),
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

/// Rows of the open partition with their evaluated order keys and arguments.
struct Piece {
    batch: RecordBatch,
    order: Vec<ArrayRef>,
    arguments: Vec<ArrayRef>,
}

struct CompiledWindowProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    partition_keys: usize,
    order_keys: usize,
    calls: Vec<CallPlan>,
    columns: Vec<OutputSource>,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    /// Row encoders of the normalized partition and order keys, created from
    /// the first evaluated keys; one encoder owns every comparison.
    partition_rows: Option<RowConverter>,
    order_rows: Option<RowConverter>,
    /// The partition keys of the last arrived row.
    last_partition: Option<OwnedRow>,
    open: Vec<Piece>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

/// Row-encode normalized keys with the processor's one encoder for them.
fn encode(converter: &mut Option<RowConverter>, keys: &[ArrayRef]) -> ExecutionResult<Rows> {
    if converter.is_none() {
        let fields = keys
            .iter()
            .map(|key| SortField::new(key.data_type().clone()))
            .collect::<Vec<_>>();
        *converter = Some(
            RowConverter::new(fields)
                .map_err(|error| failure("compiled window key row encoding", error))?,
        );
    }
    converter
        .as_mut()
        .expect("converter was created")
        .convert_columns(keys)
        .map_err(|error| failure("compiled window key row encoding", error))
}

impl CompiledWindowProcessor {
    /// Evaluate every root over the arriving rows, then split them at the
    /// partition boundaries; every partition the rows close is evaluated.
    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut values = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            values.push(evaluate_all(instance, *site, &chunk.batch, &self.control)?);
        }
        let keys = self.partition_keys + self.order_keys;
        for key in values.iter_mut().take(keys) {
            *key = normalize_sort_key_array(key).map_err(ExecutionFailure::from)?;
        }
        let rows = chunk.len();
        // Rows at which a new partition starts; without partition keys every
        // row belongs to the one partition.
        let mut starts = Vec::new();
        if self.partition_keys > 0 {
            let encoded = encode(&mut self.partition_rows, &values[..self.partition_keys])?;
            if self
                .last_partition
                .as_ref()
                .is_none_or(|last| last.row() != encoded.row(0))
            {
                starts.push(0);
            }
            for row in 1..rows {
                if encoded.row(row) != encoded.row(row - 1) {
                    starts.push(row);
                }
            }
            self.last_partition = Some(encoded.row(rows - 1).owned());
        }
        let mut closed = Vec::new();
        let mut cursor = 0;
        for &start in starts.iter().chain(std::iter::once(&rows)) {
            if start > cursor {
                self.open.push(Piece {
                    batch: chunk.batch.slice(cursor, start - cursor),
                    order: slice(&values[self.partition_keys..keys], cursor, start - cursor),
                    arguments: slice(&values[keys..], cursor, start - cursor),
                });
                cursor = start;
            }
            if start < rows && !self.open.is_empty() {
                closed.push(self.close()?);
            }
        }
        self.publish(closed)
    }

    /// Evaluate the complete open partition and return its output rows.
    fn close(&mut self) -> ExecutionResult<RecordBatch> {
        let pieces = std::mem::take(&mut self.open);
        let schema = pieces[0].batch.schema();
        let batch = if pieces.len() == 1 {
            pieces[0].batch.clone()
        } else {
            concat_batches(&schema, pieces.iter().map(|piece| &piece.batch))
                .map_err(|error| failure("compiled window partition concat", error))?
        };
        let rows = batch.num_rows();
        let join = |column: &dyn Fn(&Piece) -> &ArrayRef| -> ExecutionResult<ArrayRef> {
            if pieces.len() == 1 {
                return Ok(Arc::clone(column(&pieces[0])));
            }
            let parts = pieces
                .iter()
                .map(|piece| column(piece).as_ref())
                .collect::<Vec<&dyn Array>>();
            concat(&parts).map_err(|error| failure("compiled window partition concat", error))
        };
        let order = (0..self.order_keys)
            .map(|key| join(&|piece| &piece.order[key]))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let arguments = (0..self.sites.len() - self.partition_keys - self.order_keys)
            .map(|root| join(&|piece| &piece.arguments[root]))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let peers = if self.order_keys == 0 {
            peer_groups(rows, |_| true)
        } else {
            let encoded = encode(&mut self.order_rows, &order)?;
            peer_groups(rows, |row| encoded.row(row) == encoded.row(row - 1))
        };
        let first_argument = self.partition_keys + self.order_keys;
        let mut results = Vec::with_capacity(self.calls.len());
        for call in &self.calls {
            let frames = frame_table(&call.frame, &peers).map_err(ExecutionFailure::from)?;
            let start = call.first_root - first_argument;
            let logical = arguments[start..start + call.roots]
                .iter()
                .map(EvaluatedArgument::Column)
                .collect::<Vec<_>>();
            results.push(evaluate_call(
                &call.kernel,
                rows,
                &logical,
                &peers,
                &frames,
                &self.control,
            )?);
        }
        let columns = self
            .columns
            .iter()
            .map(|source| match source {
                OutputSource::Input(ordinal) => Arc::clone(batch.column(*ordinal)),
                OutputSource::Call(call) => Arc::clone(&results[*call]),
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(self.output.arrow_schema_ref(), columns)
            .map_err(|error| failure("compiled window output", error))
    }

    /// Emit the partitions one arrival closed as one chunk, in partition order.
    fn publish(&mut self, closed: Vec<RecordBatch>) -> ExecutionResult<()> {
        let batch = match closed.len() {
            0 => return Ok(()),
            1 => closed.into_iter().next().expect("one closed partition"),
            _ => concat_batches(&self.output.arrow_schema_ref(), &closed)
                .map_err(|error| failure("compiled window output concat", error))?,
        };
        self.pending.push_back(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.output),
        )?);
        Ok(())
    }
}

fn slice(values: &[ArrayRef], offset: usize, len: usize) -> Vec<ArrayRef> {
    values
        .iter()
        .map(|value| value.slice(offset, len))
        .collect()
}

/// Run one call's exact kernel once over one complete partition. Its whole
/// input is supplied even when no output were demanded; the partition latches
/// any failure, which is returned as a required error.
fn evaluate_call(
    kernel: &Arc<dyn PreparedWindowKernel>,
    rows: usize,
    logical: &[EvaluatedArgument<'_>],
    peers: &[WindowRowRange],
    frames: &[WindowRowRange],
    control: &RuntimeKernelControl,
) -> ExecutionResult<ArrayRef> {
    let contract = Arc::clone(kernel.contract());
    let full = FullPartitionWindowInput::try_new(contract.as_ref(), rows, logical, &[], control)?;
    let input = WindowPartitionInput::try_new(full, peers, frames, control)?;
    let mut partition = WindowEvaluationPartition::begin(Arc::clone(kernel), input, control)?;
    let output = partition.evaluate(Selection::all(rows), rows, control)?;
    partition.finish(control)?;
    let (_, values, errors) = output.into_parts();
    if !errors.is_empty() {
        return Err(ExecutionFailure::from(
            "compiled window kernel returned row errors on a channel without row masking",
        ));
    }
    if values.len() != rows {
        return Err(ExecutionFailure::from(
            "compiled window kernel output differs from its partition rows",
        ));
    }
    Ok(values)
}

impl Operator for CompiledWindowProcessor {
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

impl ProcessorOperator for CompiledWindowProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled Analytic received input after finishing".into());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        self.consume(&chunk)
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.pop_front();
        if self.finishing && self.pending.is_empty() {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.finishing = true;
        let closed = if self.open.is_empty() {
            Vec::new()
        } else {
            vec![self.close()?]
        };
        self.publish(closed)?;
        self.instances = None;
        if self.pending.is_empty() {
            self.finished = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};

    /// Peers of `keys` after ordering them as the compiled Sort does.
    fn sorted_peers(keys: ArrayRef) -> Vec<WindowRowRange> {
        let keys = normalize_sort_key_array(&keys).unwrap();
        let indices = lexsort_to_indices(
            &[SortColumn {
                values: Arc::clone(&keys),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            }],
            None,
        )
        .unwrap();
        let sorted = take(keys.as_ref(), &indices, None).unwrap();
        let encoded = encode(&mut None, &[sorted]).unwrap();
        peer_groups(encoded.num_rows(), |row| {
            encoded.row(row) == encoded.row(row - 1)
        })
    }

    fn ranges(values: &[(usize, usize)]) -> Vec<WindowRowRange> {
        values
            .iter()
            .map(|&(start, end)| WindowRowRange { start, end })
            .collect()
    }

    #[test]
    fn key_equality_is_the_sort_total_order() {
        // NULL equals NULL; one NaN equals itself; -0.0 and +0.0 are distinct
        // adjacent runs, exactly as the sort separates them.
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(f64::NAN),
            None,
            Some(-0.0),
            Some(1.5),
            None,
            Some(f64::NAN),
            Some(-0.0),
        ]));
        assert_eq!(
            sorted_peers(floats),
            ranges(&[(0, 2), (2, 4), (4, 5), (5, 6), (6, 8)])
        );
        let integers: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(2),
            None,
            Some(2),
            Some(-1),
            None,
        ]));
        assert_eq!(sorted_peers(integers), ranges(&[(0, 2), (2, 3), (3, 5)]));
    }
}
