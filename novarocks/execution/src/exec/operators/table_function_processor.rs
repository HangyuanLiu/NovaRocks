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
//! Table-function processor.
//!
//! Responsibilities:
//! - Expands each input row through table-function evaluation into zero or more output rows.
//! - Builds expanded chunks while preserving slot mapping contracts from plan lowering.
//!
//! Key exported interfaces:
//! - Types: `TableFunctionFixtureFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
#[path = "table_function_failure.rs"]
mod failure;
use failure::TableFunctionFailure;
fn binding_failure(kind: FragmentBindingErrorKind, detail: impl Into<String>) -> ExecutionFailure {
    FragmentBindingError::new(FragmentBindingTarget::Program, kind, detail).into()
}
fn layout_failure(_: std::alloc::LayoutError) -> ConstructionFailure {
    ConstructionFailure::Resource(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
}
fn output_rows(counts: &[usize]) -> ExecutionResult<usize> {
    let required = counts
        .iter()
        .try_fold(0_u128, |sum, count| sum.checked_add(*count as u128));
    required
        .filter(|rows| *rows <= u128::from(u32::MAX))
        .and_then(|rows| usize::try_from(rows).ok())
        .ok_or_else(|| TableFunctionFailure::OutputRowExtent { required }.into())
}

use novarocks_local_program::StaticLayout;
use std::alloc::Layout;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Array, Int64Builder, ListArray, NullArray,
    UInt32Array, make_array, new_empty_array,
};
#[cfg(test)]
use arrow::array::{Int8Array, Int16Array, Int32Array};
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow_data::transform::MutableArrayData;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::table_function::TableFunctionOutputSlot;
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
    step_context::StepContext,
};
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;
#[cfg(test)]
use novarocks_types::largeint;
use novarocks_types::value::bitmap;

#[cfg(test)]
const MAX_TABLE_FUNCTION_OUTPUT_ROWS: usize =
    novarocks_functions::generate_series_core::MAX_TABLE_FUNCTION_OUTPUT_ROWS;

#[derive(Clone)]
#[cfg(test)]
enum TableFunctionSchemaSource {
    Legacy(ChunkSchemaRef),
    Typed {
        input: StaticLayout,
        output: StaticLayout,
    },
}
#[cfg(test)]
struct GeneratedColumns {
    columns: Vec<ArrayRef>,
    rows: usize,
}

/// Actual constructor sites in the unchanged payload algorithm. The typed
/// caller assigns temporary scratch and retained wrapper responsibilities.
#[cfg(test)]
enum TableFunctionConstructionStep<'a> {
    Scratch(Layout),
    OuterMap(usize),
    PrimitiveIndex,
    EndOperation,
    Work(usize),
    Take {
        source: &'a dyn Array,
        indices: &'a UInt32Array,
    },
    Mutable {
        list: &'a ListArray,
        counts: &'a [usize],
        rows: usize,
    },
    Flat {
        result: usize,
        has_nulls: bool,
    },
    Null {
        result: usize,
        rows: usize,
    },
    Empty(usize),
    EmptyOuter(SlotId),
}
#[cfg(test)]
struct TableFunctionConstruction<'a> {
    observe: Option<&'a mut dyn FnMut(TableFunctionConstructionStep<'_>) -> ExecutionResult<()>>,
}
#[cfg(test)]
impl TableFunctionConstruction<'_> {
    fn step(&mut self, step: TableFunctionConstructionStep<'_>) -> ExecutionResult<()> {
        match self.observe.as_mut() {
            Some(observe) => observe(step),
            None => Ok(()),
        }
    }
}

/// F3 table-function array oracle; no plan node or product dispatch is accepted.
#[cfg(test)]
pub(crate) struct TableFunctionFixtureFactory {
    name: String,
    function_name: String,
    param_slots: Vec<SlotId>,
    outer_slots: Vec<SlotId>,
    fn_result_slots: Vec<SlotId>,
    fn_result_required: bool,
    is_left_join: bool,
    param_types: Vec<DataType>,
    ret_types: Vec<DataType>,
    schema_source: TableFunctionSchemaSource,
    output_slot_sources: Vec<TableFunctionOutputSlot>,
}

#[cfg(test)]
impl TableFunctionFixtureFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        function_name: String,
        param_slots: Vec<SlotId>,
        outer_slots: Vec<SlotId>,
        fn_result_slots: Vec<SlotId>,
        fn_result_required: bool,
        is_left_join: bool,
        param_types: Vec<DataType>,
        ret_types: Vec<DataType>,
        output_chunk_schema: ChunkSchemaRef,
        output_slot_sources: Vec<TableFunctionOutputSlot>,
    ) -> Self {
        let name = if node_id >= 0 {
            format!("TableFunction (id={node_id})")
        } else {
            "TableFunction".to_string()
        };
        Self {
            name,
            function_name,
            param_slots,
            outer_slots,
            fn_result_slots,
            fn_result_required,
            is_left_join,
            param_types,
            ret_types,
            schema_source: TableFunctionSchemaSource::Legacy(output_chunk_schema),
            output_slot_sources,
        }
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "The original table function facts and complete input/output templates stay distinct."
    )]
    pub(crate) fn new_typed(
        node_id: i32,
        function_name: String,
        param_slots: Vec<SlotId>,
        outer_slots: Vec<SlotId>,
        fn_result_slots: Vec<SlotId>,
        fn_result_required: bool,
        is_left_join: bool,
        param_types: Vec<DataType>,
        ret_types: Vec<DataType>,
        input: StaticLayout,
        output: StaticLayout,
        output_slot_sources: Vec<TableFunctionOutputSlot>,
    ) -> ExecutionResult<Self> {
        if !input.is_typed() || !output.is_typed() {
            return Err(binding_failure(
                FragmentBindingErrorKind::MissingAssignment,
                "native table function requires complete input/output declarations",
            ));
        }
        let name = if node_id >= 0 {
            format!("TableFunction (id={node_id})")
        } else {
            "TableFunction".into()
        };
        Ok(Self {
            name,
            function_name,
            param_slots,
            outer_slots,
            fn_result_slots,
            fn_result_required,
            is_left_join,
            param_types,
            ret_types,
            schema_source: TableFunctionSchemaSource::Typed { input, output },
            output_slot_sources,
        })
    }
}

#[cfg(test)]
impl OperatorFactory for TableFunctionFixtureFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(TableFunctionFixtureKernel {
            name: self.name.clone(),
            function_name: self.function_name.clone(),
            param_slots: self.param_slots.clone(),
            outer_slots: self.outer_slots.clone(),
            fn_result_slots: self.fn_result_slots.clone(),
            fn_result_required: self.fn_result_required,
            is_left_join: self.is_left_join,
            param_types: self.param_types.clone(),
            ret_types: self.ret_types.clone(),
            schema_source: self.schema_source.clone(),
            output_chunk_schema: match &self.schema_source {
                TableFunctionSchemaSource::Legacy(schema) => Some(schema.clone()),
                TableFunctionSchemaSource::Typed { .. } => None,
            },
            construction: None,
            output_slot_sources: self.output_slot_sources.clone(),
            output_chunk: None,
            output_offset: 0,
            emit_empty_once: false,
            batch_rows: None,
        }))
    }
}

#[cfg(test)]
struct TableFunctionFixtureKernel {
    name: String,
    function_name: String,
    param_slots: Vec<SlotId>,
    outer_slots: Vec<SlotId>,
    fn_result_slots: Vec<SlotId>,
    fn_result_required: bool,
    is_left_join: bool,
    param_types: Vec<DataType>,
    ret_types: Vec<DataType>,
    schema_source: TableFunctionSchemaSource,
    output_chunk_schema: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    output_slot_sources: Vec<TableFunctionOutputSlot>,
    output_chunk: Option<Chunk>,
    output_offset: usize,
    emit_empty_once: bool,
    batch_rows: Option<std::num::NonZeroUsize>,
}

#[cfg(test)]
impl OperatorLifecycle for TableFunctionFixtureKernel {
    fn creates_typed_storage(&self) -> bool {
        matches!(self.schema_source, TableFunctionSchemaSource::Typed { .. })
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let TableFunctionSchemaSource::Typed { output, .. } = &self.schema_source {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = factory.begin_compiled_layout_construction(output)?;
            self.validate_typed_declarations(owner.as_ref())?;
            self.output_chunk_schema = Some(factory.create_compiled_chunk_schema(output)?);
        }
        self.validate_config()?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.batch_rows = Some(std::num::NonZeroUsize::new(state.chunk_size()).ok_or_else(
            || {
                novarocks_functions::KernelFailure::InvalidProgram(
                    novarocks_functions::KernelDiagnostic::new("table function batch size is zero"),
                )
            },
        )?);
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.output_chunk = None;
        self.emit_empty_once = false;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.output_chunk = None;
        self.emit_empty_once = false;
    }
}

#[cfg(test)]
impl InputEncodingPolicy for TableFunctionFixtureKernel {}
#[cfg(test)]
impl TransformKernel for TableFunctionFixtureKernel {
    fn apply(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        self.batch_rows.ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "table function batch size was not bound",
                ),
            )
        })?;
        self.validate_config()?;
        if chunk.is_empty() {
            if self.creates_typed_storage() {
                let output = self
                    .output_chunk_schema
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?;
                let owner = self
                    .construction
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .begin_typed_construction(output)?;
                self.validate_typed_input(&chunk, owner.as_ref())?;
            }
            self.emit_empty_once = true;
            self.output_chunk = None;
            self.output_offset = 0;
            return Ok(StepResult::new(None, NextStep::Runnable, rows));
        }

        let output = if matches!(self.schema_source, TableFunctionSchemaSource::Typed { .. }) {
            self.build_output_chunk_typed(&chunk)?
        } else {
            self.build_output_chunk(&chunk)?
        };
        if output.is_empty() {
            self.emit_empty_once = true;
            self.output_chunk = None;
            self.output_offset = 0;
            return Ok(StepResult::new(None, NextStep::Runnable, rows));
        }
        self.output_chunk = Some(output);
        self.output_offset = 0;
        Ok(StepResult::new(None, NextStep::Runnable, rows))
    }

    fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if self.emit_empty_once {
            self.emit_empty_once = false;
            let output = if self.creates_typed_storage() {
                self.empty_output_chunk_typed()?
            } else {
                self.empty_output_chunk()?
            };
            return Ok(StepResult::new(Some(output), NextStep::NeedInput, 0));
        }

        let Some(output) = self.output_chunk.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        let total = output.len();
        if self.output_offset >= total {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }
        let take_len = self
            .batch_rows
            .ok_or_else(|| {
                novarocks_functions::KernelFailure::InvalidProgram(
                    novarocks_functions::KernelDiagnostic::new(
                        "table function batch size was not bound",
                    ),
                )
            })?
            .get()
            .min(total - self.output_offset);
        let slice = if matches!(self.schema_source, TableFunctionSchemaSource::Typed { .. }) {
            let owner = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?
                .begin_input_construction(output)?;
            output.slice_typed_observed::<ConstructionFailure, _>(
                self.output_offset,
                take_len,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )?
        } else {
            output.slice(self.output_offset, take_len)
        };
        self.output_offset += take_len;
        let next = if self.output_offset == total {
            self.output_chunk = None;
            self.output_offset = 0;
            NextStep::NeedInput
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(slice), next, 0))
    }
}

#[cfg(test)]
impl TableFunctionFixtureKernel {
    fn schema(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output_chunk_schema.as_ref().ok_or_else(|| {
            binding_failure(
                FragmentBindingErrorKind::MissingAssignment,
                "table function schema was not prepared",
            )
        })
    }
    fn build_output_chunk(&self, chunk: &Chunk) -> ExecutionResult<Chunk> {
        let generated = self.build_output_columns(
            chunk,
            &mut |count| Ok(Vec::with_capacity(count)),
            &mut TableFunctionConstruction { observe: None },
        )?;
        Chunk::try_new_with_columns(self.schema()?.clone(), generated.columns)
            .map_err(ExecutionFailure::from)
    }
    fn empty_output_chunk(&self) -> ExecutionResult<Chunk> {
        let generated = self.empty_output_columns(
            &mut |count| Ok(Vec::with_capacity(count)),
            &mut TableFunctionConstruction { observe: None },
        )?;
        Chunk::try_new_with_columns(self.schema()?.clone(), generated.columns)
            .map_err(ExecutionFailure::from)
    }

    fn validate_config(&self) -> ExecutionResult<()> {
        if self.param_slots.len() != self.param_types.len() {
            return Err(binding_failure(
                FragmentBindingErrorKind::SchemaMismatch,
                format!(
                    "table function param slot mismatch: slots={} types={}",
                    self.param_slots.len(),
                    self.param_types.len()
                ),
            ));
        }
        if self.fn_result_slots.len() != self.ret_types.len() {
            return Err(binding_failure(
                FragmentBindingErrorKind::SchemaMismatch,
                format!(
                    "table function result slot mismatch: slots={} types={}",
                    self.fn_result_slots.len(),
                    self.ret_types.len()
                ),
            ));
        }
        if self.output_slot_sources.len() != self.schema()?.slot_ids().len() {
            return Err(binding_failure(
                FragmentBindingErrorKind::SchemaMismatch,
                format!(
                    "table function output mapping mismatch: slots={} sources={}",
                    self.schema()?.slot_ids().len(),
                    self.output_slot_sources.len()
                ),
            ));
        }
        Ok(())
    }

    fn build_output_columns(
        &self,
        chunk: &Chunk,
        vector: &mut impl FnMut(usize) -> ExecutionResult<Vec<ArrayRef>>,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<GeneratedColumns> {
        // The original case-insensitive dispatch borrows the frozen label;
        // no temporary lowercase String is needed by the actual operator.
        if self.function_name.eq_ignore_ascii_case("unnest") {
            self.build_output_columns_unnest(chunk, vector, construction)
        } else if self.function_name.eq_ignore_ascii_case("unnest_bitmap") {
            self.build_output_columns_unnest_bitmap(chunk, vector, construction)
        } else if self.function_name.eq_ignore_ascii_case("subdivide_bitmap") {
            self.build_output_columns_subdivide_bitmap(chunk, vector, construction)
        } else if self.function_name.eq_ignore_ascii_case("generate_series") {
            self.build_output_columns_generate_series(chunk, vector, construction)
        } else {
            Err(binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!("unsupported table function: {}", self.function_name),
            ))
        }
    }

    fn build_output_columns_unnest(
        &self,
        chunk: &Chunk,
        vector: &mut impl FnMut(usize) -> ExecutionResult<Vec<ArrayRef>>,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<GeneratedColumns> {
        construction.step(TableFunctionConstructionStep::Scratch(
            Layout::array::<ArrayRef>(self.param_slots.len()).map_err(layout_failure)?,
        ))?;
        let mut list_cols = Vec::with_capacity(self.param_slots.len());
        for (idx, slot) in self.param_slots.iter().enumerate() {
            let col = chunk.column_by_slot_id(*slot)?;
            if col.as_any().downcast_ref::<ListArray>().is_none() {
                return Err(binding_failure(
                    FragmentBindingErrorKind::SchemaMismatch,
                    format!(
                        "table function unnest param {idx} expects ListArray, got {:?}",
                        col.data_type()
                    ),
                )
                .into());
            }
            list_cols.push(col);
        }
        construction.step(TableFunctionConstructionStep::Scratch(
            Layout::array::<&ListArray>(list_cols.len()).map_err(layout_failure)?,
        ))?;
        let list_args: Vec<&ListArray> = list_cols
            .iter()
            .map(|col| {
                col.as_any()
                    .downcast_ref::<ListArray>()
                    .expect("list array checked")
            })
            .collect();

        let row_counts = if list_args.len() == 1 {
            self.row_counts_single(list_args[0])?
        } else {
            self.row_counts_multi(&list_args)?
        };
        let total_output_rows = output_rows(&row_counts)?;
        if total_output_rows == 0 {
            return self.empty_output_columns(vector, construction);
        }

        let row_indices = build_row_indices(&row_counts)?;

        let outer_columns = self.build_outer_columns(chunk, &row_indices, construction)?;
        let result_columns = if self.fn_result_required {
            if list_args.len() == 1 {
                construction.step(TableFunctionConstructionStep::Scratch(
                    Layout::array::<ArrayRef>(1).map_err(layout_failure)?,
                ))?;
                vec![self.build_result_column_single(
                    list_args[0],
                    &row_counts,
                    total_output_rows,
                    construction,
                )?]
            } else {
                self.build_result_columns_multi(
                    &list_args,
                    &row_counts,
                    total_output_rows,
                    construction,
                )?
            }
        } else {
            Vec::new()
        };

        self.assemble_output_columns(
            outer_columns,
            result_columns,
            total_output_rows,
            vector,
            construction,
        )
    }

    fn build_output_columns_unnest_bitmap(
        &self,
        chunk: &Chunk,
        vector: &mut impl FnMut(usize) -> ExecutionResult<Vec<ArrayRef>>,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<GeneratedColumns> {
        if self.param_slots.len() != 1 {
            return Err(binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "table function unnest_bitmap expects 1 arg, got {}",
                    self.param_slots.len()
                ),
            )
            .into());
        }
        let bitmap_col = chunk.column_by_slot_id(self.param_slots[0])?;
        let bitmap_arr = bitmap_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                binding_failure(
                    FragmentBindingErrorKind::SchemaMismatch,
                    format!(
                        "table function unnest_bitmap param expects BinaryArray, got {:?}",
                        bitmap_col.data_type()
                    ),
                )
            })?;

        let num_rows = chunk.len();
        let mut row_counts = Vec::with_capacity(num_rows);
        let mut row_values: Vec<Option<Vec<i64>>> = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            if bitmap_arr.is_null(row) {
                if self.is_left_join {
                    row_counts.push(1);
                } else {
                    row_counts.push(0);
                }
                row_values.push(None);
                continue;
            }
            let decoded = bitmap::decode_bitmap(bitmap_arr.value(row))
                .map_err(ExecutionFailure::from_domain)?;
            let mut values = Vec::with_capacity(decoded.len());
            for value in decoded {
                let value = i64::try_from(value)
                    .map_err(|_| TableFunctionFailure::BitmapValueRange { value })?;
                values.push(value);
            }
            if values.is_empty() {
                if self.is_left_join {
                    row_counts.push(1);
                } else {
                    row_counts.push(0);
                }
            } else {
                row_counts.push(values.len());
            }
            row_values.push(Some(values));
        }

        let total_output_rows = output_rows(&row_counts)?;
        if total_output_rows == 0 {
            return self.empty_output_columns(vector, construction);
        }

        let row_indices = build_row_indices(&row_counts)?;
        let outer_columns = self.build_outer_columns(chunk, &row_indices, construction)?;
        let result_columns = if self.fn_result_required {
            if !matches!(self.ret_types.first(), Some(DataType::Int64)) {
                return Err(binding_failure(
                    FragmentBindingErrorKind::SchemaMismatch,
                    format!(
                        "table function unnest_bitmap return type expects BIGINT, got {:?}",
                        self.ret_types.first()
                    ),
                )
                .into());
            }
            construction.step(TableFunctionConstructionStep::Scratch(
                Layout::array::<ArrayRef>(1).map_err(layout_failure)?,
            ))?;
            construction.step(TableFunctionConstructionStep::Flat {
                result: 0,
                has_nulls: self.is_left_join,
            })?;
            let mut builder = Int64Builder::new();
            for (row, count) in row_counts.iter().enumerate() {
                if *count == 0 {
                    continue;
                }
                match row_values.get(row).and_then(|v| v.as_ref()) {
                    Some(values) if !values.is_empty() => {
                        for value in values {
                            builder.append_value(*value);
                        }
                    }
                    _ => {
                        for _ in 0..*count {
                            builder.append_null();
                        }
                    }
                }
            }
            vec![Arc::new(builder.finish()) as ArrayRef]
        } else {
            Vec::new()
        };
        self.assemble_output_columns(
            outer_columns,
            result_columns,
            total_output_rows,
            vector,
            construction,
        )
    }

    fn build_output_columns_subdivide_bitmap(
        &self,
        chunk: &Chunk,
        vector: &mut impl FnMut(usize) -> ExecutionResult<Vec<ArrayRef>>,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<GeneratedColumns> {
        if self.param_slots.len() != 2 {
            return Err(binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "table function subdivide_bitmap expects 2 args, got {}",
                    self.param_slots.len()
                ),
            )
            .into());
        }
        let bitmap_col = chunk.column_by_slot_id(self.param_slots[0])?;
        let size_col = chunk.column_by_slot_id(self.param_slots[1])?;
        let bitmap_arr = bitmap_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                binding_failure(
                    FragmentBindingErrorKind::SchemaMismatch,
                    format!(
                        "table function subdivide_bitmap param 0 expects BinaryArray, got {:?}",
                        bitmap_col.data_type()
                    ),
                )
            })?;

        let num_rows = chunk.len();
        let mut row_counts = Vec::with_capacity(num_rows);
        let mut row_values: Vec<Option<Vec<Vec<u64>>>> = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            let batch_size = self.int_like_arg_to_i128(&size_col, row, 1, novarocks_functions::generate_series_core::IntegerDiagnosticContext::SubdivideBitmap)?;
            if bitmap_arr.is_null(row) || batch_size.is_none() {
                if self.is_left_join {
                    row_counts.push(1);
                } else {
                    row_counts.push(0);
                }
                row_values.push(None);
                continue;
            }
            let batch_size = batch_size.expect("checked");
            if batch_size <= 0 {
                if self.is_left_join {
                    row_counts.push(1);
                } else {
                    row_counts.push(0);
                }
                row_values.push(None);
                continue;
            }
            let bitmap_values = bitmap::decode_bitmap(bitmap_arr.value(row))
                .map_err(ExecutionFailure::from_domain)?
                .into_iter()
                .collect::<Vec<_>>();
            let batch_size = match usize::try_from(batch_size) {
                Ok(v) => v,
                Err(_) => usize::MAX,
            };
            let splits = split_bitmap_values(&bitmap_values, batch_size);
            row_counts.push(splits.len());
            row_values.push(Some(splits));
        }

        let total_output_rows = output_rows(&row_counts)?;
        if total_output_rows == 0 {
            return self.empty_output_columns(vector, construction);
        }

        let row_indices = build_row_indices(&row_counts)?;
        let outer_columns = self.build_outer_columns(chunk, &row_indices, construction)?;
        let result_columns = if self.fn_result_required {
            if !matches!(self.ret_types.first(), Some(DataType::Binary)) {
                return Err(binding_failure(
                    FragmentBindingErrorKind::SchemaMismatch,
                    format!(
                        "table function subdivide_bitmap return type expects Binary, got {:?}",
                        self.ret_types.first()
                    ),
                )
                .into());
            }
            construction.step(TableFunctionConstructionStep::Scratch(
                Layout::array::<ArrayRef>(1).map_err(layout_failure)?,
            ))?;
            construction.step(TableFunctionConstructionStep::Flat {
                result: 0,
                has_nulls: self.is_left_join,
            })?;
            let mut builder = BinaryBuilder::new();
            for (row, count) in row_counts.iter().enumerate() {
                if *count == 0 {
                    continue;
                }
                match row_values.get(row).and_then(|v| v.as_ref()) {
                    Some(splits) => {
                        for split in splits {
                            builder.append_value(encode_bitmap_chunk(split)?);
                        }
                    }
                    None => {
                        for _ in 0..*count {
                            builder.append_null();
                        }
                    }
                }
            }
            vec![Arc::new(builder.finish()) as ArrayRef]
        } else {
            Vec::new()
        };
        self.assemble_output_columns(
            outer_columns,
            result_columns,
            total_output_rows,
            vector,
            construction,
        )
    }

    fn build_output_columns_generate_series(
        &self,
        chunk: &Chunk,
        vector: &mut impl FnMut(usize) -> ExecutionResult<Vec<ArrayRef>>,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<GeneratedColumns> {
        if !(self.param_slots.len() == 2 || self.param_slots.len() == 3) {
            return Err(binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "table function generate_series expects 2 or 3 args, got {}",
                    self.param_slots.len()
                ),
            )
            .into());
        }
        let start_col = chunk.column_by_slot_id(self.param_slots[0])?;
        let end_col = chunk.column_by_slot_id(self.param_slots[1])?;
        let step_col = if self.param_slots.len() == 3 {
            Some(chunk.column_by_slot_id(self.param_slots[2])?)
        } else {
            None
        };

        let expansion = novarocks_functions::generate_series_core::expand_observed(
            &start_col,
            &end_col,
            step_col.as_ref(),
            chunk.len(),
            self.is_left_join,
            &mut |event| {
                construction.step(TableFunctionConstructionStep::Work(match event {
                novarocks_functions::generate_series_core::SeriesObservation::Step => 1,
                novarocks_functions::generate_series_core::SeriesObservation::OpaqueBoundary => 0,
            }))
            },
        )
        .map_err(|failure| match failure {
            novarocks_functions::generate_series_core::SeriesFailure::Cause(cause) => {
                ExecutionFailure::from(cause)
            }
            novarocks_functions::generate_series_core::SeriesFailure::Control(cause) => cause,
        })?;
        if expansion.total_rows == 0 {
            return self.empty_output_columns(vector, construction);
        }
        let row_indices = build_row_indices(&expansion.row_counts)?;
        let outer_columns = self.build_outer_columns(chunk, &row_indices, construction)?;
        let result_columns = if self.fn_result_required {
            construction.step(TableFunctionConstructionStep::Scratch(
                Layout::array::<ArrayRef>(1).map_err(layout_failure)?,
            ))?;
            if self.ret_types.first().is_some() {
                construction.step(TableFunctionConstructionStep::Flat {
                    result: 0,
                    has_nulls: expansion.values.iter().any(Option::is_none),
                })?;
            }
            vec![novarocks_functions::generate_series_core::result_column_observed(
                expansion.values, &self.ret_types,
                &mut |event| construction.step(TableFunctionConstructionStep::Work(match event {
                    novarocks_functions::generate_series_core::SeriesObservation::Step => 1,
                    novarocks_functions::generate_series_core::SeriesObservation::OpaqueBoundary => 0,
                })),
            ).map_err(|failure| match failure {
                novarocks_functions::generate_series_core::SeriesFailure::Cause(cause) => ExecutionFailure::from(cause),
                novarocks_functions::generate_series_core::SeriesFailure::Control(cause) => cause,
            })?]
        } else {
            Vec::new()
        };
        self.assemble_output_columns(
            outer_columns,
            result_columns,
            expansion.total_rows,
            vector,
            construction,
        )
    }

    fn assemble_output_columns(
        &self,
        outer_columns: HashMap<SlotId, ArrayRef>,
        result_columns: Vec<ArrayRef>,
        rows: usize,
        vector: &mut impl FnMut(usize) -> ExecutionResult<Vec<ArrayRef>>,
        _construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<GeneratedColumns> {
        let mut output_columns = vector(self.schema()?.slot_ids().len())?;
        for source in &self.output_slot_sources {
            match source {
                TableFunctionOutputSlot::Outer { slot } => {
                    let col = outer_columns.get(slot).ok_or_else(|| {
                        binding_failure(
                            FragmentBindingErrorKind::MissingAssignment,
                            format!("table function missing outer slot {}", slot),
                        )
                    })?;
                    output_columns.push(col.clone());
                }
                TableFunctionOutputSlot::Result { index } => {
                    let col = result_columns.get(*index).ok_or_else(|| {
                        binding_failure(
                            FragmentBindingErrorKind::MissingAssignment,
                            format!("table function missing result column {}", index),
                        )
                    })?;
                    output_columns.push(col.clone());
                }
            }
        }

        Ok(GeneratedColumns {
            columns: output_columns,
            rows,
        })
    }

    fn empty_output_columns(
        &self,
        vector: &mut impl FnMut(usize) -> ExecutionResult<Vec<ArrayRef>>,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<GeneratedColumns> {
        let schema = self.schema()?;
        let mut columns = vector(schema.slots().len())?;
        for (ordinal, slot) in schema.slots().iter().enumerate() {
            construction.step(TableFunctionConstructionStep::Empty(ordinal))?;
            columns.push(new_empty_array(slot.data_type()));
        }
        Ok(GeneratedColumns { columns, rows: 0 })
    }

    fn int_like_arg_to_i128(
        &self,
        array: &ArrayRef,
        row: usize,
        arg_idx: usize,
        diagnostic: novarocks_functions::generate_series_core::IntegerDiagnosticContext,
    ) -> Result<Option<i128>, novarocks_functions::generate_series_core::SeriesError> {
        novarocks_functions::generate_series_core::integer_argument(array, row, arg_idx, diagnostic)
    }

    fn generate_series_result_column(
        &self,
        values: Vec<Option<i128>>,
    ) -> Result<ArrayRef, novarocks_functions::generate_series_core::SeriesError> {
        novarocks_functions::generate_series_core::result_column(values, &self.ret_types)
    }

    fn row_counts_single(&self, list: &ListArray) -> ExecutionResult<Vec<usize>> {
        let num_rows = list.len();
        let offsets = list.value_offsets();
        let mut counts = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            if list.is_null(row) {
                counts.push(if self.is_left_join { 1 } else { 0 });
                continue;
            }
            let start = offsets[row];
            let end = offsets[row + 1];
            if end < start {
                return Err(TableFunctionFailure::InvalidOffsets { row, start, end }.into());
            }
            let len = (end - start) as usize;
            if len == 0 && self.is_left_join {
                counts.push(1);
            } else {
                counts.push(len);
            }
        }
        Ok(counts)
    }

    fn row_counts_multi(&self, lists: &[&ListArray]) -> ExecutionResult<Vec<usize>> {
        if lists.is_empty() {
            return Err(TableFunctionFailure::MissingList.into());
        }
        let num_rows = lists[0].len();
        for (idx, list) in lists.iter().enumerate() {
            if list.len() != num_rows {
                return Err(TableFunctionFailure::InputLength {
                    argument: idx,
                    expected: num_rows,
                    actual: list.len(),
                }
                .into());
            }
        }
        let mut counts = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            let mut max_len = 0usize;
            for list in lists {
                if list.is_null(row) {
                    continue;
                }
                let offsets = list.value_offsets();
                let start = offsets[row];
                let end = offsets[row + 1];
                if end < start {
                    return Err(TableFunctionFailure::InvalidOffsets { row, start, end }.into());
                }
                let len = (end - start) as usize;
                if len > max_len {
                    max_len = len;
                }
            }
            if max_len == 0 && self.is_left_join {
                counts.push(1);
            } else {
                counts.push(max_len);
            }
        }
        Ok(counts)
    }

    fn build_outer_columns(
        &self,
        chunk: &Chunk,
        row_indices: &[u32],
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<HashMap<SlotId, ArrayRef>> {
        construction.step(TableFunctionConstructionStep::OuterMap(
            self.outer_slots.len(),
        ))?;
        let mut out = HashMap::with_capacity(self.outer_slots.len());
        let idx_array = if row_indices.is_empty() {
            None
        } else {
            construction.step(TableFunctionConstructionStep::PrimitiveIndex)?;
            Some(UInt32Array::from(row_indices.to_vec()))
        };
        for slot in &self.outer_slots {
            let col = chunk.column_by_slot_id(*slot)?;
            let out_col = match idx_array.as_ref() {
                Some(idx) => {
                    construction.step(TableFunctionConstructionStep::Take {
                        source: col.as_ref(),
                        indices: idx,
                    })?;
                    let output =
                        take(col.as_ref(), idx, None).map_err(TableFunctionFailure::Arrow)?;
                    construction.step(TableFunctionConstructionStep::EndOperation)?;
                    output
                }
                None => {
                    construction.step(TableFunctionConstructionStep::EmptyOuter(*slot))?;
                    new_empty_array(col.data_type())
                }
            };
            out.insert(*slot, out_col);
        }
        Ok(out)
    }

    fn build_result_column_single(
        &self,
        list: &ListArray,
        row_counts: &[usize],
        total_output_rows: usize,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<ArrayRef> {
        let values = list.values();
        let expected = self.ret_types.first().ok_or_else(|| {
            binding_failure(
                FragmentBindingErrorKind::MissingAssignment,
                "table function missing return type",
            )
        })?;
        if matches!(expected, DataType::Null) {
            construction.step(TableFunctionConstructionStep::Null {
                result: 0,
                rows: total_output_rows,
            })?;
            return Ok(Arc::new(NullArray::new(total_output_rows)) as ArrayRef);
        }
        if values.data_type() != expected {
            return Err(binding_failure(
                FragmentBindingErrorKind::SchemaMismatch,
                format!(
                    "table function unnest result type mismatch: expected {:?}, got {:?}",
                    expected,
                    values.data_type()
                ),
            )
            .into());
        }
        construction.step(TableFunctionConstructionStep::Mutable {
            list,
            counts: row_counts,
            rows: total_output_rows,
        })?;
        construction.step(TableFunctionConstructionStep::Scratch(
            Layout::array::<&arrow_data::ArrayData>(1).map_err(layout_failure)?,
        ))?;
        let data_storage = [values.to_data()];
        let data_refs: Vec<&arrow_data::ArrayData> = data_storage.iter().collect();
        let mut mutable = MutableArrayData::new(data_refs, true, total_output_rows);

        let offsets = list.value_offsets();
        for (row, count) in row_counts.iter().enumerate() {
            if *count == 0 {
                continue;
            }
            if list.is_null(row) {
                mutable.extend_nulls(*count);
                continue;
            }
            let start = offsets[row];
            let end = offsets[row + 1];
            if end < start {
                return Err(TableFunctionFailure::InvalidOffsets { row, start, end }.into());
            }
            let len = (end - start) as usize;
            if len == 0 && self.is_left_join {
                mutable.extend_nulls(1);
                continue;
            }
            if len > 0 {
                mutable.extend(0, start as usize, end as usize);
            }
            if len < *count {
                mutable.extend_nulls(*count - len);
            }
        }
        let output = make_array(mutable.freeze());
        construction.step(TableFunctionConstructionStep::EndOperation)?;
        Ok(output)
    }

    fn build_result_columns_multi(
        &self,
        lists: &[&ListArray],
        row_counts: &[usize],
        total_output_rows: usize,
        construction: &mut TableFunctionConstruction<'_>,
    ) -> ExecutionResult<Vec<ArrayRef>> {
        construction.step(TableFunctionConstructionStep::Scratch(
            Layout::array::<ArrayRef>(lists.len()).map_err(layout_failure)?,
        ))?;
        let mut out = Vec::with_capacity(lists.len());
        for (col_idx, list) in lists.iter().enumerate() {
            let values = list.values();
            let expected = self.ret_types.get(col_idx).ok_or_else(|| {
                binding_failure(
                    FragmentBindingErrorKind::MissingAssignment,
                    "table function missing return type",
                )
            })?;
            if matches!(expected, DataType::Null) {
                construction.step(TableFunctionConstructionStep::Null {
                    result: col_idx,
                    rows: total_output_rows,
                })?;
                out.push(Arc::new(NullArray::new(total_output_rows)) as ArrayRef);
                continue;
            }
            if values.data_type() != expected {
                return Err(binding_failure(
                    FragmentBindingErrorKind::SchemaMismatch,
                    format!(
                        "table function unnest result type mismatch: expected {:?}, got {:?}",
                        expected,
                        values.data_type()
                    ),
                )
                .into());
            }
            construction.step(TableFunctionConstructionStep::Mutable {
                list,
                counts: row_counts,
                rows: total_output_rows,
            })?;
            construction.step(TableFunctionConstructionStep::Scratch(
                Layout::array::<&arrow_data::ArrayData>(1).map_err(layout_failure)?,
            ))?;
            let data_storage = [values.to_data()];
            let data_refs: Vec<&arrow_data::ArrayData> = data_storage.iter().collect();
            let mut mutable = MutableArrayData::new(data_refs, true, total_output_rows);

            let offsets = list.value_offsets();
            for (row, count) in row_counts.iter().enumerate() {
                if *count == 0 {
                    continue;
                }
                if list.is_null(row) {
                    mutable.extend_nulls(*count);
                    continue;
                }
                let start = offsets[row];
                let end = offsets[row + 1];
                if end < start {
                    return Err(TableFunctionFailure::InvalidOffsets { row, start, end }.into());
                }
                let len = (end - start) as usize;
                if len == 0 && self.is_left_join && *count == 1 {
                    mutable.extend_nulls(1);
                    continue;
                }
                if len > 0 {
                    mutable.extend(0, start as usize, end as usize);
                }
                if len < *count {
                    mutable.extend_nulls(*count - len);
                }
            }
            out.push(make_array(mutable.freeze()));
            construction.step(TableFunctionConstructionStep::EndOperation)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
fn build_row_indices(counts: &[usize]) -> ExecutionResult<Vec<u32>> {
    let total = output_rows(counts)?;
    let mut indices = Vec::with_capacity(total);
    for (row, count) in counts.iter().enumerate() {
        if *count == 0 {
            continue;
        }
        let row_u32 =
            u32::try_from(row).map_err(|_| TableFunctionFailure::InputRowExtent { row })?;
        for _ in 0..*count {
            indices.push(row_u32);
        }
    }
    Ok(indices)
}

#[cfg(test)]
fn encode_bitmap_chunk(values: &[u64]) -> ExecutionResult<Vec<u8>> {
    let set: std::collections::BTreeSet<u64> = values.iter().copied().collect();
    bitmap::encode_internal_bitmap(&set).map_err(ExecutionFailure::from_domain)
}

#[cfg(test)]
fn split_bitmap_values(values: &[u64], batch_size: usize) -> Vec<Vec<u64>> {
    if batch_size == 0 {
        return Vec::new();
    }
    let cardinality = values.len();
    let split_num = cardinality / batch_size + usize::from(!cardinality.is_multiple_of(batch_size));
    if split_num <= 1 {
        return vec![values.to_vec()];
    }
    let mut out = Vec::with_capacity(split_num);
    for idx in 0..split_num {
        let start = idx * batch_size;
        let end = ((idx + 1) * batch_size).min(cardinality);
        out.push(values[start..end].to_vec());
    }
    out
}

#[cfg(test)]
fn generate_series_count(
    start: i128,
    end: i128,
    step: i128,
) -> Result<usize, novarocks_functions::generate_series_core::SeriesError> {
    novarocks_functions::generate_series_core::count(start, end, step)
}

#[cfg(test)]
fn checked_add_table_function_rows(
    total: &mut usize,
    rows: usize,
) -> Result<(), novarocks_functions::generate_series_core::SeriesError> {
    novarocks_functions::generate_series_core::checked_add_output_rows(total, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;

    #[test]
    fn generate_series_count_handles_descending_steps() {
        assert_eq!(generate_series_count(10, 2, -2).expect("count"), 5);
        assert_eq!(generate_series_count(2, 10, -2).expect("count"), 0);
    }

    #[test]
    fn table_function_output_row_guard_rejects_before_materialization() {
        let mut total = 0usize;
        checked_add_table_function_rows(&mut total, MAX_TABLE_FUNCTION_OUTPUT_ROWS)
            .expect("max rows accepted");
        let err = checked_add_table_function_rows(&mut total, 1).expect_err("too many rows");
        assert_eq!(
            err.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Representation
        );
    }
}

#[cfg(test)]
#[path = "generate_series_original_baseline_tests.rs"]
mod generate_series_original_baseline_tests;

#[cfg(test)]
#[path = "generate_series_shared_differential_tests.rs"]
mod generate_series_shared_differential_tests;

#[cfg(test)]
#[path = "typed_table_function.rs"]
mod typed_table_function;

#[cfg(test)]
#[path = "typed_table_function_tests.rs"]
mod typed_table_function_tests;

#[cfg(test)]
#[path = "typed_table_function_construction.rs"]
mod typed_table_function_construction;
