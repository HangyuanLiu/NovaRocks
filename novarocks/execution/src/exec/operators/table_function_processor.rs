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
//! - Types: `TableFunctionProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, Int8Array, Int16Array,
    Int32Array, Int64Array, Int64Builder, ListArray, NullArray, UInt32Array, make_array,
    new_empty_array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field};
use arrow_data::transform::MutableArrayData;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::table_function::TableFunctionOutputSlot;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;
use novarocks_types::largeint;
use novarocks_types::value::bitmap;

const MAX_TABLE_FUNCTION_OUTPUT_ROWS: usize = u32::MAX as usize;

/// Factory for table-function processors that expand each input row to variable output rows.
pub struct TableFunctionProcessorFactory {
    name: String,
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
}

impl TableFunctionProcessorFactory {
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
            output_chunk_schema,
            output_slot_sources,
        }
    }
}

impl OperatorFactory for TableFunctionProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(TableFunctionProcessorOperator {
            name: self.name.clone(),
            function_name: self.function_name.clone(),
            param_slots: self.param_slots.clone(),
            outer_slots: self.outer_slots.clone(),
            fn_result_slots: self.fn_result_slots.clone(),
            fn_result_required: self.fn_result_required,
            is_left_join: self.is_left_join,
            param_types: self.param_types.clone(),
            ret_types: self.ret_types.clone(),
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            output_slot_sources: self.output_slot_sources.clone(),
            series_cursor: None,
            output_chunk: None,
            output_offset: 0,
            emit_empty_once: false,
            finishing: false,
            finished: false,
        })
    }
}

struct TableFunctionProcessorOperator {
    name: String,
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
    series_cursor: Option<GenerateSeriesCursor>,
    output_chunk: Option<Chunk>,
    output_offset: usize,
    emit_empty_once: bool,
    finishing: bool,
    finished: bool,
}

impl Operator for TableFunctionProcessorOperator {
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

impl ProcessorOperator for TableFunctionProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing
            && !self.finished
            && self.output_chunk.is_none()
            && self.series_cursor.is_none()
            && !self.emit_empty_once
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        if self.emit_empty_once || self.series_cursor.is_some() {
            return true;
        }
        self.output_chunk
            .as_ref()
            .map(|c| self.output_offset < c.len())
            .unwrap_or(false)
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("table function received input while not ready".to_string());
        }
        self.validate_config()?;
        if chunk.is_empty() {
            self.emit_empty_once = true;
            self.output_chunk = None;
            self.output_offset = 0;
            return Ok(());
        }

        if self.function_name.eq_ignore_ascii_case("generate_series") {
            let cursor = self.prepare_series_cursor(chunk)?;
            if cursor.rows.iter().all(|row| row.remaining == 0) {
                self.emit_empty_once = true;
            } else {
                self.series_cursor = Some(cursor);
            }
            return Ok(());
        }
        let output = self.build_output_chunk(&chunk)?;
        if output.is_empty() {
            self.emit_empty_once = true;
            self.output_chunk = None;
            self.output_offset = 0;
            return Ok(());
        }
        self.output_chunk = Some(output);
        self.output_offset = 0;
        Ok(())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if !self.has_output() {
            return Ok(None);
        }
        if self.emit_empty_once {
            self.emit_empty_once = false;
            if self.finishing && self.output_chunk.is_none() {
                self.finished = true;
            }
            return Ok(Some(self.empty_output_chunk()?));
        }

        if let Some(mut cursor) = self.series_cursor.take() {
            let (values, indices) = cursor.take_batch(state.chunk_size())?;
            let outer = self.build_outer_columns(&cursor.input, &indices)?;
            let results = if self.fn_result_required {
                vec![self.generate_series_result_column(values)?]
            } else {
                Vec::new()
            };
            let output = self.assemble_output_chunk(outer, results)?;
            if cursor.has_remaining() {
                self.series_cursor = Some(cursor);
            } else if self.finishing {
                self.finished = true;
            }
            return Ok(Some(output));
        }

        let Some(output) = self.output_chunk.as_ref() else {
            return Ok(None);
        };
        let total = output.len();
        if self.output_offset >= total {
            return Ok(None);
        }
        let take_len = state.chunk_size().min(total - self.output_offset);
        let slice = output.slice(self.output_offset, take_len);
        self.output_offset += take_len;
        if self.output_offset >= total {
            self.output_chunk = None;
            self.output_offset = 0;
            if self.finishing {
                self.finished = true;
            }
        }
        Ok(Some(slice))
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        if self.output_chunk.is_none() && self.series_cursor.is_none() && !self.emit_empty_once {
            self.finished = true;
        }
        Ok(())
    }
}

impl TableFunctionProcessorOperator {
    fn validate_config(&self) -> Result<(), String> {
        if self.param_slots.len() != self.param_types.len() {
            return Err(format!(
                "table function param slot mismatch: slots={} types={}",
                self.param_slots.len(),
                self.param_types.len()
            ));
        }
        if self.fn_result_slots.len() != self.ret_types.len() {
            return Err(format!(
                "table function result slot mismatch: slots={} types={}",
                self.fn_result_slots.len(),
                self.ret_types.len()
            ));
        }
        if self.output_slot_sources.len() != self.output_chunk_schema.slot_ids().len() {
            return Err(format!(
                "table function output mapping mismatch: slots={} sources={}",
                self.output_chunk_schema.slot_ids().len(),
                self.output_slot_sources.len()
            ));
        }
        Ok(())
    }

    fn build_output_chunk(&self, chunk: &Chunk) -> Result<Chunk, String> {
        let func = self.function_name.to_ascii_lowercase();
        match func.as_str() {
            "unnest" => self.build_output_chunk_unnest(chunk),
            "unnest_bitmap" => self.build_output_chunk_unnest_bitmap(chunk),
            "subdivide_bitmap" => self.build_output_chunk_subdivide_bitmap(chunk),
            "generate_series" => Err("generate_series must use its pull cursor".into()),
            _ => Err(format!(
                "unsupported table function: {}",
                self.function_name
            )),
        }
    }

    fn build_output_chunk_unnest(&self, chunk: &Chunk) -> Result<Chunk, String> {
        let mut list_cols = Vec::with_capacity(self.param_slots.len());
        for (idx, slot) in self.param_slots.iter().enumerate() {
            let col = chunk.column_by_slot_id(*slot)?;
            if col.as_any().downcast_ref::<ListArray>().is_none() {
                return Err(format!(
                    "table function unnest param {idx} expects ListArray, got {:?}",
                    col.data_type()
                ));
            }
            list_cols.push(col);
        }
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
        let mut total_output_rows = 0;
        for count in &row_counts {
            checked_add_table_function_rows(&mut total_output_rows, *count)?;
        }
        if total_output_rows == 0 {
            return self.empty_output_chunk();
        }

        let row_indices = build_row_indices(&row_counts)?;

        let outer_columns = self.build_outer_columns(chunk, &row_indices)?;
        let result_columns = if self.fn_result_required {
            if list_args.len() == 1 {
                vec![self.build_result_column_single(
                    list_args[0],
                    &row_counts,
                    total_output_rows,
                )?]
            } else {
                self.build_result_columns_multi(&list_args, &row_counts, total_output_rows)?
            }
        } else {
            Vec::new()
        };

        self.assemble_output_chunk(outer_columns, result_columns)
    }

    fn build_output_chunk_unnest_bitmap(&self, chunk: &Chunk) -> Result<Chunk, String> {
        if self.param_slots.len() != 1 {
            return Err(format!(
                "table function unnest_bitmap expects 1 arg, got {}",
                self.param_slots.len()
            ));
        }
        let bitmap_col = chunk.column_by_slot_id(self.param_slots[0])?;
        let bitmap_arr = bitmap_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                format!(
                    "table function unnest_bitmap param expects BinaryArray, got {:?}",
                    bitmap_col.data_type()
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
                .map_err(|e| format!("table function unnest_bitmap decode failed: {e}"))?;
            let mut values = Vec::with_capacity(decoded.len());
            for value in decoded {
                let value = i64::try_from(value).map_err(|_| {
                    format!("table function unnest_bitmap value out of BIGINT range: {value}")
                })?;
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

        let mut total_output_rows = 0;
        for count in &row_counts {
            checked_add_table_function_rows(&mut total_output_rows, *count)?;
        }
        if total_output_rows == 0 {
            return self.empty_output_chunk();
        }

        let row_indices = build_row_indices(&row_counts)?;
        let outer_columns = self.build_outer_columns(chunk, &row_indices)?;
        let result_columns = if self.fn_result_required {
            if !matches!(self.ret_types.first(), Some(DataType::Int64)) {
                return Err(format!(
                    "table function unnest_bitmap return type expects BIGINT, got {:?}",
                    self.ret_types.first()
                ));
            }
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
        self.assemble_output_chunk(outer_columns, result_columns)
    }

    fn build_output_chunk_subdivide_bitmap(&self, chunk: &Chunk) -> Result<Chunk, String> {
        if self.param_slots.len() != 2 {
            return Err(format!(
                "table function subdivide_bitmap expects 2 args, got {}",
                self.param_slots.len()
            ));
        }
        let bitmap_col = chunk.column_by_slot_id(self.param_slots[0])?;
        let size_col = chunk.column_by_slot_id(self.param_slots[1])?;
        let bitmap_arr = bitmap_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                format!(
                    "table function subdivide_bitmap param 0 expects BinaryArray, got {:?}",
                    bitmap_col.data_type()
                )
            })?;

        let num_rows = chunk.len();
        let mut row_counts = Vec::with_capacity(num_rows);
        let mut row_values: Vec<Option<Vec<Vec<u64>>>> = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            let batch_size = self.int_like_arg_to_i128(&size_col, row, 1, "subdivide_bitmap")?;
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
                .map_err(|e| format!("table function subdivide_bitmap decode failed: {e}"))?
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

        let mut total_output_rows = 0;
        for count in &row_counts {
            checked_add_table_function_rows(&mut total_output_rows, *count)?;
        }
        if total_output_rows == 0 {
            return self.empty_output_chunk();
        }

        let row_indices = build_row_indices(&row_counts)?;
        let outer_columns = self.build_outer_columns(chunk, &row_indices)?;
        let result_columns = if self.fn_result_required {
            if !matches!(self.ret_types.first(), Some(DataType::Binary)) {
                return Err(format!(
                    "table function subdivide_bitmap return type expects Binary, got {:?}",
                    self.ret_types.first()
                ));
            }
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
        self.assemble_output_chunk(outer_columns, result_columns)
    }

    fn prepare_series_cursor(&self, chunk: Chunk) -> Result<GenerateSeriesCursor, String> {
        if !(self.param_slots.len() == 2 || self.param_slots.len() == 3) {
            return Err("table function generate_series expects 2 or 3 args".into());
        }
        let start_col = chunk.column_by_slot_id(self.param_slots[0])?;
        let end_col = chunk.column_by_slot_id(self.param_slots[1])?;
        let step_col = if self.param_slots.len() == 3 {
            Some(chunk.column_by_slot_id(self.param_slots[2])?)
        } else {
            None
        };
        let mut rows = Vec::with_capacity(chunk.len());
        for row in 0..chunk.len() {
            let start = self.int_like_arg_to_i128(&start_col, row, 0, "generate_series")?;
            let end = self.int_like_arg_to_i128(&end_col, row, 1, "generate_series")?;
            let step = match &step_col {
                Some(col) => self.int_like_arg_to_i128(col, row, 2, "generate_series")?,
                None => Some(1),
            };
            let (current, step, count) = match (start, end, step) {
                (Some(start), Some(end), Some(step)) => {
                    let count = generate_series_count(start, end, step)?;
                    // The original implementation validates the increment after the last value.
                    if count > 0 {
                        series_after_n(start, step, count)?;
                    }
                    (Some(start), step, count)
                }
                _ => (None, 1, 0),
            };
            let remaining = if count == 0 && self.is_left_join {
                1
            } else {
                count
            };
            // Only the input row index and each pulled batch need bounded sizes.
            // Logical multiplicity is kept per row without summing the expanded result.
            rows.push(SeriesRow {
                current: if count == 0 { None } else { current },
                step,
                remaining,
            });
        }
        if self.fn_result_required {
            self.generate_series_result_column(Vec::new())?;
        }
        Ok(GenerateSeriesCursor {
            input: chunk,
            rows,
            row: 0,
        })
    }

    fn assemble_output_chunk(
        &self,
        outer_columns: HashMap<SlotId, ArrayRef>,
        result_columns: Vec<ArrayRef>,
    ) -> Result<Chunk, String> {
        let mut output_columns: Vec<ArrayRef> =
            Vec::with_capacity(self.output_chunk_schema.slot_ids().len());
        for source in &self.output_slot_sources {
            match source {
                TableFunctionOutputSlot::Outer { slot } => {
                    let col = outer_columns
                        .get(slot)
                        .ok_or_else(|| format!("table function missing outer slot {}", slot))?;
                    output_columns.push(col.clone());
                }
                TableFunctionOutputSlot::Result { index } => {
                    let col = result_columns
                        .get(*index)
                        .ok_or_else(|| format!("table function missing result column {}", index))?;
                    output_columns.push(col.clone());
                }
            }
        }

        let mut fields = Vec::with_capacity(self.output_chunk_schema.slot_ids().len());
        for (idx, slot_id) in self.output_chunk_schema.slot_ids().iter().enumerate() {
            let base_field = self
                .output_chunk_schema
                .slot(*slot_id)
                .ok_or_else(|| format!("table function output slot {} missing", slot_id))?
                .field();
            let dt = output_columns
                .get(idx)
                .ok_or_else(|| "table function output column missing".to_string())?
                .data_type()
                .clone();
            fields.push(Field::new(base_field.name(), dt, base_field.is_nullable()));
        }
        Chunk::try_new_with_columns(
            Arc::new(self.output_chunk_schema.with_fields_in_order(fields)?),
            output_columns,
        )
        .map_err(|e| format!("table function build batch failed: {e}"))
    }

    fn empty_output_chunk(&self) -> Result<Chunk, String> {
        let mut fields = Vec::with_capacity(self.output_chunk_schema.slot_ids().len());
        let mut columns = Vec::with_capacity(self.output_chunk_schema.slot_ids().len());
        for slot in self.output_chunk_schema.slots() {
            let field = slot.field().clone();
            columns.push(new_empty_array(field.data_type()));
            fields.push(field);
        }
        Chunk::try_new_with_columns(
            Arc::new(self.output_chunk_schema.with_fields_in_order(fields)?),
            columns,
        )
        .map_err(|e| format!("table function build empty batch failed: {e}"))
    }

    fn int_like_arg_to_i128(
        &self,
        array: &ArrayRef,
        row: usize,
        arg_idx: usize,
        fn_name: &str,
    ) -> Result<Option<i128>, String> {
        match array.data_type() {
            DataType::Int8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| format!("table function {fn_name} downcast Int8Array failed"))?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                    format!("table function {fn_name} downcast Int16Array failed")
                })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    format!("table function {fn_name} downcast Int32Array failed")
                })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    format!("table function {fn_name} downcast Int64Array failed")
                })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::UInt8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt8Array>()
                    .ok_or_else(|| {
                        format!("table function {fn_name} downcast UInt8Array failed")
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::UInt16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt16Array>()
                    .ok_or_else(|| {
                        format!("table function {fn_name} downcast UInt16Array failed")
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::UInt32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt32Array>()
                    .ok_or_else(|| {
                        format!("table function {fn_name} downcast UInt32Array failed")
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::UInt64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .ok_or_else(|| {
                        format!("table function {fn_name} downcast UInt64Array failed")
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(i128::from(arr.value(row))))
                }
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        format!("table function {fn_name} downcast FixedSizeBinaryArray failed")
                    })?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    let value = largeint::i128_from_be_bytes(arr.value(row)).map_err(|e| {
                        format!("table function {fn_name} decode LARGEINT failed: {e}")
                    })?;
                    Ok(Some(value))
                }
            }
            other => Err(format!(
                "table function {fn_name} arg {arg_idx} expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {:?}",
                other
            )),
        }
    }

    fn generate_series_result_column(&self, values: Vec<Option<i128>>) -> Result<ArrayRef, String> {
        if self.ret_types.len() != 1 {
            return Err(format!(
                "table function generate_series expects 1 return type, got {}",
                self.ret_types.len()
            ));
        }
        match self
            .ret_types
            .first()
            .ok_or_else(|| "table function generate_series missing return type".to_string())?
        {
            DataType::Int8 => {
                let mut out_i8 = Vec::with_capacity(values.len());
                for value in values {
                    let v = match value {
                        Some(v) => Some(i8::try_from(v).map_err(|_| {
                            format!(
                                "table function generate_series value out of TINYINT range: {v}"
                            )
                        })?),
                        None => None,
                    };
                    out_i8.push(v);
                }
                Ok(Arc::new(Int8Array::from(out_i8)) as ArrayRef)
            }
            DataType::Int16 => {
                let mut out_i16 = Vec::with_capacity(values.len());
                for value in values {
                    let v = match value {
                        Some(v) => Some(i16::try_from(v).map_err(|_| {
                            format!(
                                "table function generate_series value out of SMALLINT range: {v}"
                            )
                        })?),
                        None => None,
                    };
                    out_i16.push(v);
                }
                Ok(Arc::new(Int16Array::from(out_i16)) as ArrayRef)
            }
            DataType::Int32 => {
                let mut out_i32 = Vec::with_capacity(values.len());
                for value in values {
                    let v = match value {
                        Some(v) => Some(i32::try_from(v).map_err(|_| {
                            format!("table function generate_series value out of INT range: {v}")
                        })?),
                        None => None,
                    };
                    out_i32.push(v);
                }
                Ok(Arc::new(Int32Array::from(out_i32)) as ArrayRef)
            }
            DataType::Int64 => {
                let mut out_i64 = Vec::with_capacity(values.len());
                for value in values {
                    let v = match value {
                        Some(v) => Some(i64::try_from(v).map_err(|_| {
                            format!("table function generate_series value out of BIGINT range: {v}")
                        })?),
                        None => None,
                    };
                    out_i64.push(v);
                }
                Ok(Arc::new(Int64Array::from(out_i64)) as ArrayRef)
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                largeint::array_from_i128(&values)
            }
            other => Err(format!(
                "table function generate_series return type expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {:?}",
                other
            )),
        }
    }

    fn row_counts_single(&self, list: &ListArray) -> Result<Vec<usize>, String> {
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
                return Err("table function unnest list offsets are invalid".to_string());
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

    fn row_counts_multi(&self, lists: &[&ListArray]) -> Result<Vec<usize>, String> {
        if lists.is_empty() {
            return Err("table function unnest requires at least one list param".to_string());
        }
        let num_rows = lists[0].len();
        for (idx, list) in lists.iter().enumerate() {
            if list.len() != num_rows {
                return Err(format!(
                    "table function unnest param {idx} length mismatch: expected {num_rows}, got {}",
                    list.len()
                ));
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
                    return Err("table function unnest list offsets are invalid".to_string());
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
    ) -> Result<HashMap<SlotId, ArrayRef>, String> {
        let mut out = HashMap::with_capacity(self.outer_slots.len());
        let idx_array = if row_indices.is_empty() {
            None
        } else {
            Some(UInt32Array::from(row_indices.to_vec()))
        };
        for slot in &self.outer_slots {
            let col = chunk.column_by_slot_id(*slot)?;
            let out_col = match idx_array.as_ref() {
                Some(idx) => take(col.as_ref(), idx, None).map_err(|e| e.to_string())?,
                None => new_empty_array(col.data_type()),
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
    ) -> Result<ArrayRef, String> {
        let values = list.values();
        let expected = self
            .ret_types
            .first()
            .ok_or_else(|| "table function missing return type".to_string())?;
        if matches!(expected, DataType::Null) {
            return Ok(Arc::new(NullArray::new(total_output_rows)) as ArrayRef);
        }
        if values.data_type() != expected {
            return Err(format!(
                "table function unnest result type mismatch: expected {:?}, got {:?}",
                expected,
                values.data_type()
            ));
        }
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
                return Err("table function unnest list offsets are invalid".to_string());
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
        Ok(make_array(mutable.freeze()))
    }

    fn build_result_columns_multi(
        &self,
        lists: &[&ListArray],
        row_counts: &[usize],
        total_output_rows: usize,
    ) -> Result<Vec<ArrayRef>, String> {
        let mut out = Vec::with_capacity(lists.len());
        for (col_idx, list) in lists.iter().enumerate() {
            let values = list.values();
            let expected = self
                .ret_types
                .get(col_idx)
                .ok_or_else(|| "table function missing return type".to_string())?;
            if matches!(expected, DataType::Null) {
                out.push(Arc::new(NullArray::new(total_output_rows)) as ArrayRef);
                continue;
            }
            if values.data_type() != expected {
                return Err(format!(
                    "table function unnest result type mismatch: expected {:?}, got {:?}",
                    expected,
                    values.data_type()
                ));
            }
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
                    return Err("table function unnest list offsets are invalid".to_string());
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
        }
        Ok(out)
    }
}

fn build_row_indices(counts: &[usize]) -> Result<Vec<u32>, String> {
    let total: usize = counts.iter().sum();
    let mut indices = Vec::with_capacity(total);
    for (row, count) in counts.iter().enumerate() {
        let row_u32 =
            u32::try_from(row).map_err(|_| "table function row index overflow".to_string())?;
        for _ in 0..*count {
            indices.push(row_u32);
        }
    }
    Ok(indices)
}

fn encode_bitmap_chunk(values: &[u64]) -> Result<Vec<u8>, String> {
    let set: std::collections::BTreeSet<u64> = values.iter().copied().collect();
    bitmap::encode_internal_bitmap(&set)
}

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

fn generate_series_count(start: i128, end: i128, step: i128) -> Result<u128, String> {
    if step == 0 {
        return Err("table function generate_series step size cannot equal zero".into());
    }
    if (step > 0 && start > end) || (step < 0 && start < end) {
        return Ok(0);
    }
    // Unsigned distances cover the full i128 domain without subtraction overflow.
    let distance = if step > 0 {
        end.abs_diff(start)
    } else {
        start.abs_diff(end)
    };
    let count = distance
        .checked_div(step.unsigned_abs())
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| "table function generate_series count overflow".to_string())?;
    Ok(count)
}

fn series_after_n(start: i128, step: i128, count: u128) -> Result<i128, String> {
    let distance = step
        .unsigned_abs()
        .checked_mul(count)
        .ok_or_else(|| "table function generate_series value overflow".to_string())?;
    let biased = (start as u128) ^ (1u128 << 127);
    let end = if step > 0 {
        biased.checked_add(distance)
    } else {
        biased.checked_sub(distance)
    }
    .ok_or_else(|| "table function generate_series value overflow".to_string())?;
    Ok((end ^ (1u128 << 127)) as i128)
}

struct SeriesRow {
    current: Option<i128>,
    step: i128,
    remaining: u128,
}
struct GenerateSeriesCursor {
    input: Chunk,
    rows: Vec<SeriesRow>,
    row: usize,
}
impl GenerateSeriesCursor {
    fn has_remaining(&self) -> bool {
        self.rows[self.row..].iter().any(|row| row.remaining > 0)
    }
    fn take_batch(&mut self, limit: usize) -> Result<(Vec<Option<i128>>, Vec<u32>), String> {
        if limit == 0 {
            return Err("table function output chunk size must be positive".into());
        }
        let mut values = Vec::with_capacity(limit);
        let mut indices = Vec::with_capacity(limit);
        while values.len() < limit && self.row < self.rows.len() {
            let row = &mut self.rows[self.row];
            if row.remaining == 0 {
                self.row += 1;
                continue;
            }
            values.push(row.current);
            indices.push(
                u32::try_from(self.row)
                    .map_err(|_| "table function input too large".to_string())?,
            );
            row.remaining -= 1;
            if let Some(current) = row.current {
                row.current =
                    Some(current.checked_add(row.step).ok_or_else(|| {
                        "table function generate_series value overflow".to_string()
                    })?);
            }
        }
        Ok((values, indices))
    }
}

fn checked_add_table_function_rows(total: &mut usize, rows: usize) -> Result<(), String> {
    *total = total
        .checked_add(rows)
        .ok_or_else(|| "table function output too large".to_string())?;
    if *total > MAX_TABLE_FUNCTION_OUTPUT_ROWS {
        return Err("table function output too large".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_series(
        start: Vec<Option<i64>>,
        end: Vec<Option<i64>>,
        step: Vec<Option<i64>>,
    ) -> Chunk {
        use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
        use arrow::datatypes::Schema;
        use arrow::record_batch::RecordBatch;
        let fields = (0..3)
            .map(|i| Field::new(format!("p{i}"), DataType::Int64, true))
            .collect::<Vec<_>>();
        let schema = Arc::new(
            ChunkSchema::try_new(
                fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        ChunkSlotSchema::from_field(SlotId::new(i as u32 + 1), f, None).unwrap()
                    })
                    .collect(),
            )
            .unwrap(),
        );
        let arrays = vec![
            Arc::new(Int64Array::from(start)) as ArrayRef,
            Arc::new(Int64Array::from(end)) as ArrayRef,
            Arc::new(Int64Array::from(step)) as ArrayRef,
        ];
        Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap(),
            schema,
        )
        .unwrap()
    }
    fn series_factory(left: bool) -> TableFunctionProcessorFactory {
        use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
        let field = Field::new("series", DataType::Int64, true);
        let schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(SlotId::new(4), &field, None).unwrap(),
            ])
            .unwrap(),
        );
        TableFunctionProcessorFactory::new(
            1,
            "generate_series".into(),
            vec![SlotId::new(1), SlotId::new(2), SlotId::new(3)],
            Vec::new(),
            vec![SlotId::new(4)],
            true,
            left,
            vec![DataType::Int64; 3],
            vec![DataType::Int64],
            schema,
            vec![TableFunctionOutputSlot::Result { index: 0 }],
        )
    }
    #[test]
    fn table_function_series_large_count_retains_one_input_and_bounded_pull() {
        let factory = series_factory(false);
        let mut operator = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().unwrap();
        processor
            .push_chunk(
                &state,
                input_series(
                    vec![Some(1)],
                    vec![Some(i64::from(u32::MAX) + 1)],
                    vec![Some(1)],
                ),
            )
            .unwrap();
        assert!(!processor.need_input());
        for batch in 0..3 {
            let output = processor.pull_chunk(&state).unwrap().unwrap();
            assert_eq!(output.len(), state.chunk_size());
            let values = output.column_by_slot_id(SlotId::new(4)).unwrap();
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.value(0), (batch * state.chunk_size() + 1) as i64);
        }
        // The logical result exceeds u32::MAX, but each output allocation is batch-sized.
        assert!(processor.has_output());
    }
    #[test]
    fn table_function_series_multiple_maximum_weights_have_no_total_row_bound() {
        let factory = series_factory(false);
        let mut operator = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().unwrap();
        // The input's combined multiplicity exceeds both u32 and u64.
        processor
            .push_chunk(
                &state,
                input_series(vec![Some(1); 3], vec![Some(i64::MAX); 3], vec![Some(1); 3]),
            )
            .unwrap();
        processor.set_finishing(&state).unwrap();
        for batch in 0..3 {
            let output = processor.pull_chunk(&state).unwrap().unwrap();
            assert_eq!(output.len(), state.chunk_size());
            let values = output.column_by_slot_id(SlotId::new(4)).unwrap();
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.value(0), (batch * state.chunk_size() + 1) as i64);
            assert_eq!(
                values.value(values.len() - 1),
                ((batch + 1) * state.chunk_size()) as i64
            );
        }
        assert!(processor.has_output());
        assert!(!processor.need_input());
        assert!(!operator.is_finished());
        assert_eq!(
            generate_series_count(1, i128::from(i64::MAX), 1).unwrap(),
            i64::MAX as u128
        );
        assert_eq!(
            series_after_n(1, 1, i64::MAX as u128).unwrap(),
            i128::from(i64::MAX) + 1
        );
    }

    #[test]
    fn table_function_series_i64_tail_preserves_outer_values_and_eos() {
        use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
        let mut factory = series_factory(false);
        let outer_slot = SlotId::new(2);
        let result_slot = SlotId::new(4);
        factory.outer_slots = vec![outer_slot];
        factory.output_slot_sources = vec![
            TableFunctionOutputSlot::Outer { slot: outer_slot },
            TableFunctionOutputSlot::Result { index: 0 },
        ];
        factory.output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(
                    outer_slot,
                    &Field::new("visible", DataType::Int64, false),
                    None,
                )
                .unwrap(),
                ChunkSlotSchema::from_field(
                    result_slot,
                    &Field::new("series", DataType::Int64, true),
                    None,
                )
                .unwrap(),
            ])
            .unwrap(),
        );
        let mut operator = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().unwrap();
        processor
            .push_chunk(
                &state,
                input_series(
                    vec![Some(i64::MAX - 2), Some(7)],
                    vec![Some(i64::MAX), Some(8)],
                    vec![Some(1); 2],
                ),
            )
            .unwrap();
        processor.set_finishing(&state).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();
        let values = output.column_by_slot_id(result_slot).unwrap();
        let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            values.values().as_ref(),
            &[i64::MAX - 2, i64::MAX - 1, i64::MAX, 7, 8]
        );
        let outer = output.column_by_slot_id(outer_slot).unwrap();
        let outer = outer.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            outer.values().as_ref(),
            &[i64::MAX, i64::MAX, i64::MAX, 8, 8]
        );
        assert!(!processor.has_output());
        assert!(processor.pull_chunk(&state).unwrap().is_none());
        assert!(operator.is_finished());
    }

    #[test]
    fn table_function_series_null_left_join_descending_and_finish() {
        let factory = series_factory(true);
        let mut operator = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().unwrap();
        processor
            .push_chunk(
                &state,
                input_series(
                    vec![Some(3), None, Some(9)],
                    vec![Some(1), Some(2), Some(1)],
                    vec![Some(-1), Some(1), Some(1)],
                ),
            )
            .unwrap();
        processor.set_finishing(&state).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();
        let column = output.column_by_slot_id(SlotId::new(4)).unwrap();
        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(3), Some(2), Some(1), None, None]
        );
        assert!(!processor.has_output());
        assert!(operator.is_finished());
        assert!(generate_series_count(0, 1, 0).is_err());
        assert!(generate_series_count(i128::MIN, i128::MAX, 1).is_err());
        assert!(series_after_n(i128::MAX, 1, 1).is_err());
        assert_eq!(
            series_after_n(i128::MIN, i128::MAX, 2).unwrap(),
            i128::MAX - 1
        );
    }

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
        assert!(err.contains("output too large"));
    }
}
