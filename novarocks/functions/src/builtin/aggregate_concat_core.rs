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
//! One original GROUP_CONCAT state, ordering, DISTINCT and Arrow state codec.
use crate::aggregate_format::scalar_to_string;
use crate::aggregate_scalar::{
    AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork, TrackedAggScalarValue,
    aggregate_vec_with_capacity, build_scalar_array, compare_tracked_scalar_values,
    tracked_scalar_from_array, tracked_scalar_heap_capacity, tracked_scalar_to_output,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{Array, ArrayRef, ListArray, StructArray, builder::StringBuilder};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Fields};
use std::{cmp::Ordering, sync::Arc};
const DEFAULT_SEPARATOR: &str = ",";
#[derive(Debug)]
pub struct GroupConcatState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub rows: ScalarVec<ScalarVec<Option<TrackedAggScalarValue<A>>, A>, A>,
    pub failed: bool,
    payload_bytes: usize,
}
impl<A: ScalarStateAllocator> GroupConcatState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            rows: ScalarVec::new_in(allocator.clone()),
            allocator,
            failed: false,
            payload_bytes: 0,
        }
    }
    pub fn retained_bytes(&self) -> usize {
        self.rows.capacity() * std::mem::size_of::<ScalarVec<Option<TrackedAggScalarValue<A>>, A>>()
            + self.payload_bytes
    }
    pub fn latch_failure(&mut self) {
        self.failed = true;
        self.payload_bytes = 0;
        self.rows = ScalarVec::new_in(self.allocator.clone());
    }
    pub fn push_row(
        &mut self,
        row: ScalarVec<Option<TrackedAggScalarValue<A>>, A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        work.flush()?;
        self.rows.try_reserve(1).map_err(|_| {
            self.allocator
                .scalar_allocation_error("reserve group_concat row")
        })?;
        work.flush()?;
        let mut retained = row.capacity() * std::mem::size_of::<Option<TrackedAggScalarValue<A>>>();
        for value in row.iter().flatten() {
            work.step()?;
            retained = retained
                .checked_add(tracked_scalar_heap_capacity(value, work)?)
                .ok_or(crate::KernelFailure::ResourceExhausted)?;
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(retained)
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        self.rows.push(row);
        Ok(())
    }
    pub fn update_row(
        &mut self,
        columns: &[(&ArrayRef, usize)],
        layout: GroupConcatLayout,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let mut values = aggregate_vec_with_capacity(
            &self.allocator,
            columns.len(),
            "reserve group_concat row values",
            work,
        )?;
        for (idx, (column, row)) in columns.iter().enumerate() {
            work.step()?;
            let value = tracked_scalar_from_array(column, *row, &self.allocator, work)?;
            if idx < layout.output_col_num && value.is_none() {
                return Ok(());
            }
            values.push(value);
        }
        self.push_row(values, work)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct GroupConcatLayout {
    pub output_col_num: usize,
    pub separator_idx: Option<usize>,
    pub order_by_start: usize,
    pub order_by_num: usize,
}

impl GroupConcatLayout {
    pub fn infer(arg_count: usize, order_by_num: usize) -> Result<Self, String> {
        if arg_count == 0 {
            return Err("group_concat argument count must be positive".to_string());
        }
        if arg_count < order_by_num + 1 {
            return Err(format!(
                "group_concat argument count {} is less than order-by columns {}",
                arg_count, order_by_num
            ));
        }

        // FE usually rewrites group_concat to include separator as one argument.
        // Preserve the original raw one-argument projection.
        let (output_col_num, separator_idx, order_by_start) = if arg_count == order_by_num + 1 {
            (arg_count - order_by_num, None, arg_count - order_by_num)
        } else {
            let output_col_num = arg_count - order_by_num - 1;
            (output_col_num, Some(output_col_num), output_col_num + 1)
        };
        if output_col_num == 0 {
            return Err("group_concat output column should not be empty".to_string());
        }
        if order_by_start + order_by_num != arg_count {
            return Err("group_concat argument layout is invalid".to_string());
        }
        Ok(Self {
            output_col_num,
            separator_idx,
            order_by_start,
            order_by_num,
        })
    }
}

fn truncate_utf8_by_bytes(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

fn append_with_limit(out: &mut String, piece: &str, max_len: usize) -> bool {
    if max_len == 0 || piece.is_empty() {
        return max_len == 0;
    }
    let before = out.len();
    out.push_str(piece);
    if out.len() > max_len {
        truncate_utf8_by_bytes(out, max_len);
        return true;
    }
    before + piece.len() >= max_len
}

pub fn extract_arg_types(input_type: &DataType) -> Vec<DataType> {
    match input_type {
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        other => vec![other.clone()],
    }
}

pub fn build_default_intermediate_type(arg_types: &[DataType]) -> DataType {
    let fields = arg_types
        .iter()
        .enumerate()
        .map(|(idx, dt)| {
            Arc::new(Field::new(
                format!("c{idx}"),
                DataType::List(Arc::new(Field::new("item", dt.clone(), true))),
                true,
            ))
        })
        .collect::<Vec<_>>();
    DataType::Struct(Fields::from(fields))
}

pub fn validate_intermediate_type(ty: &DataType) -> Result<&Fields, String> {
    let DataType::Struct(fields) = ty else {
        return Err(format!(
            "group_concat intermediate type must be STRUCT, got {:?}",
            ty
        ));
    };
    if fields.is_empty() {
        return Err("group_concat intermediate type must contain at least one field".to_string());
    }
    for (idx, field) in fields.iter().enumerate() {
        if !matches!(field.data_type(), DataType::List(_)) {
            return Err(format!(
                "group_concat intermediate field {} must be ARRAY type",
                idx
            ));
        }
    }
    Ok(fields)
}

pub fn intermediate_arg_types(intermediate_type: &DataType) -> Result<Vec<DataType>, String> {
    validate_intermediate_type(intermediate_type)?
        .iter()
        .map(|field| match field.data_type() {
            DataType::List(item) => Ok(item.data_type().clone()),
            other => Err(format!(
                "group_concat intermediate field must be ARRAY type, got {other:?}"
            )),
        })
        .collect()
}

pub fn extract_input_columns(array: &ArrayRef) -> Result<Vec<ArrayRef>, String> {
    match array.data_type() {
        DataType::Struct(_) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "group_concat input must be StructArray".to_string())?;
            Ok(struct_arr.columns().to_vec())
        }
        _ => Ok(vec![array.clone()]),
    }
}

fn compare_optional_scalar<A: ScalarStateAllocator>(
    left: &Option<TrackedAggScalarValue<A>>,
    right: &Option<TrackedAggScalarValue<A>>,
    asc: bool,
    nulls_first: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, ScalarStateError> {
    let ord = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let ord = compare_tracked_scalar_values(left, right, work)?;
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

fn sort_indices<A: ScalarStateAllocator>(
    rows: &[ScalarVec<Option<TrackedAggScalarValue<A>>, A>],
    layout: GroupConcatLayout,
    is_asc_order: &[bool],
    nulls_first: &[bool],
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<usize>, ScalarStateError> {
    for _ in rows {
        work.step()?;
    }
    work.flush()?;
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    work.flush()?;
    if layout.order_by_num == 0 {
        return Ok(indices);
    }

    work.flush()?;
    let mut error: Option<ScalarStateError> = None;
    indices.sort_by(|l, r| {
        if error.is_some() {
            return Ordering::Equal;
        }
        for key in 0..layout.order_by_num {
            let col = layout.order_by_start + key;
            match compare_optional_scalar(
                &rows[*l][col],
                &rows[*r][col],
                is_asc_order[key],
                nulls_first[key],
                work,
            ) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(e) => {
                    error = Some(e);
                    return Ordering::Equal;
                }
            }
        }
        Ordering::Equal
    });
    if let Some(err) = error {
        return Err(err);
    }
    work.flush()?;
    Ok(indices)
}

fn rows_equal_on_output<A: ScalarStateAllocator>(
    left: &[Option<TrackedAggScalarValue<A>>],
    right: &[Option<TrackedAggScalarValue<A>>],
    layout: GroupConcatLayout,
    work: &mut ScalarWork<'_, '_>,
) -> Result<bool, ScalarStateError> {
    for col in 0..layout.output_col_num {
        work.step()?;
        match (&left[col], &right[col]) {
            (None, None) => {}
            (Some(left), Some(right)) => {
                if compare_tracked_scalar_values(left, right, work)? != Ordering::Equal {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn mark_duplicated_rows<A: ScalarStateAllocator>(
    rows: &[ScalarVec<Option<TrackedAggScalarValue<A>>, A>],
    sorted_indices: &[usize],
    layout: GroupConcatLayout,
    is_distinct: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<bool>, ScalarStateError> {
    work.flush()?;
    let mut duplicated = vec![false; rows.len()];
    work.flush()?;
    if !is_distinct {
        return Ok(duplicated);
    }
    for (pos, &idx) in sorted_indices.iter().enumerate() {
        work.step()?;
        for &next_idx in sorted_indices.iter().skip(pos + 1) {
            work.step()?;
            if rows_equal_on_output(&rows[idx], &rows[next_idx], layout, work)? {
                duplicated[idx] = true;
                break;
            }
        }
    }
    Ok(duplicated)
}

fn separator_for_row<A: ScalarStateAllocator>(
    row: &[Option<TrackedAggScalarValue<A>>],
    arg_types: &[DataType],
    layout: GroupConcatLayout,
    work: &mut ScalarWork<'_, '_>,
) -> Result<String, ScalarStateError> {
    if let Some(sep_idx) = layout.separator_idx {
        if let Some(separator) = row
            .get(sep_idx)
            .ok_or_else(|| "group_concat separator index out of bounds".to_string())?
            .as_ref()
        {
            scalar_to_string(
                &tracked_scalar_to_output(separator, work)?,
                &arg_types[sep_idx],
                work,
            )
        } else {
            Ok(String::new())
        }
    } else {
        Ok(DEFAULT_SEPARATOR.to_string())
    }
}

pub struct GroupConcatMerge<'a> {
    array: &'a StructArray,
    layout: GroupConcatLayout,
    list_columns: Vec<&'a ListArray>,
    value_columns: Vec<ArrayRef>,
    offsets: Vec<Vec<i32>>,
    arg_types: Vec<DataType>,
}
impl<'a> GroupConcatMerge<'a> {
    pub fn new(
        array: &'a ArrayRef,
        order_by_num: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, ScalarStateError> {
        work.flush()?;
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "group_concat merge input must be StructArray".to_string())?;
        let field_num = struct_arr.num_columns();
        let layout = GroupConcatLayout::infer(field_num, order_by_num)?;
        let mut list_columns = Vec::with_capacity(field_num);
        let mut value_columns = Vec::with_capacity(field_num);
        let mut offsets = Vec::with_capacity(field_num);
        let mut arg_types = Vec::with_capacity(field_num);
        for (idx, col) in struct_arr.columns().iter().enumerate() {
            work.step()?;
            let list_col = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| format!("group_concat merge field {} must be ListArray", idx))?;
            let DataType::List(item_field) = list_col.data_type() else {
                return Err(format!("group_concat merge field {} must be ARRAY type", idx).into());
            };
            arg_types.push(item_field.data_type().clone());
            value_columns.push(list_col.values().clone());
            offsets.push(list_col.value_offsets().to_vec());
            list_columns.push(list_col);
        }

        work.flush()?;
        Ok(Self {
            array: struct_arr,
            layout,
            list_columns,
            value_columns,
            offsets,
            arg_types,
        })
    }
    pub fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }
    pub fn merge_row<A: ScalarStateAllocator>(
        &self,
        state: &mut GroupConcatState<A>,
        row: usize,
        intermediate_type: &DataType,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let field_num = self.list_columns.len();
        work.step()?;
        if self.array.is_null(row) {
            return Ok(());
        }
        let expected_types = intermediate_arg_types(intermediate_type)?;
        if self.arg_types != expected_types {
            return Err(format!(
                "group_concat merge argument types mismatch: expected {expected_types:?}, got {:?}",
                self.arg_types
            )
            .into());
        }

        let mut output_list_is_null = false;
        for list_col in self.list_columns.iter().take(self.layout.output_col_num) {
            work.step()?;
            if list_col.is_null(row) {
                output_list_is_null = true;
                break;
            }
        }
        if output_list_is_null {
            return Ok(());
        }

        if self.list_columns[0].is_null(row) {
            return Ok(());
        }
        let base_start = self.offsets[0][row] as usize;
        let base_end = self.offsets[0][row + 1] as usize;
        let row_count = base_end.saturating_sub(base_start);
        for col_idx in 0..field_num {
            work.step()?;
            if self.list_columns[col_idx].is_null(row) {
                return Err(format!(
                    "group_concat merge field {} is null while struct row is non-null",
                    col_idx
                )
                .into());
            }
            let start = self.offsets[col_idx][row] as usize;
            let end = self.offsets[col_idx][row + 1] as usize;
            if end.saturating_sub(start) != row_count {
                return Err(format!(
                    "group_concat merge field {} length mismatch: expected {}, got {}",
                    col_idx,
                    row_count,
                    end.saturating_sub(start)
                )
                .into());
            }
        }

        for idx in 0..row_count {
            work.step()?;
            let mut row_values = aggregate_vec_with_capacity(
                &state.allocator,
                field_num,
                "reserve group_concat merge row",
                work,
            )?;
            let mut has_null_output = false;
            for col_idx in 0..field_num {
                work.step()?;
                let start = self.offsets[col_idx][row] as usize;
                let value = tracked_scalar_from_array(
                    &self.value_columns[col_idx],
                    start + idx,
                    &state.allocator,
                    work,
                )?;
                if col_idx < self.layout.output_col_num && value.is_none() {
                    has_null_output = true;
                    break;
                }
                row_values.push(value);
            }
            if !has_null_output {
                state.push_row(row_values, work)?;
            }
        }

        Ok(())
    }
}
pub fn build_intermediate_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s GroupConcatState<A>>,
>(
    intermediate_type: &DataType,
    group_states: I,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError> {
    let fields = validate_intermediate_type(intermediate_type)?.clone();
    let field_num = fields.len();

    let mut offsets = Vec::with_capacity(group_states.len() + 1);
    offsets.push(0_i32);
    let mut current_len: i64 = 0;
    let mut struct_null_builder = NullBufferBuilder::new(group_states.len());
    let mut list_null_builder = NullBufferBuilder::new(group_states.len());
    let mut flat_values: Vec<Vec<Option<AggScalarValue>>> =
        (0..field_num).map(|_| Vec::new()).collect();

    for state in group_states {
        work.step()?;
        if state.rows.is_empty() {
            offsets.push(current_len as i32);
            struct_null_builder.append_null();
            list_null_builder.append_null();
            continue;
        }

        for row in &state.rows {
            work.step()?;
            if row.len() != field_num {
                return Err("group_concat state row width mismatch".to_string().into());
            }
            for idx in 0..field_num {
                work.step()?;
                flat_values[idx].push(
                    row[idx]
                        .as_ref()
                        .map(|value| tracked_scalar_to_output(value, work))
                        .transpose()?,
                );
            }
        }
        current_len += i64::try_from(state.rows.len())
            .map_err(|_| "group_concat intermediate row count overflow".to_string())?;
        if current_len > i32::MAX as i64 {
            return Err("group_concat intermediate offset overflow"
                .to_string()
                .into());
        }
        offsets.push(current_len as i32);
        struct_null_builder.append_non_null();
        list_null_builder.append_non_null();
    }

    let list_nulls = list_null_builder.finish();
    let mut columns = Vec::with_capacity(field_num);
    for (idx, field) in fields.iter().enumerate() {
        work.step()?;
        let DataType::List(item_field) = field.data_type() else {
            return Err(
                format!("group_concat intermediate field {} must be ARRAY type", idx).into(),
            );
        };
        let mut values =
            build_scalar_array(item_field.data_type(), flat_values[idx].clone(), work)?;
        if values.data_type() != item_field.data_type() {
            work.flush()?;
            values = cast(&values, item_field.data_type()).map_err(|e| {
                format!(
                    "group_concat failed to cast intermediate field {} values: {}",
                    idx, e
                )
            })?;
            work.flush()?;
        }
        let list = ListArray::new(
            item_field.clone(),
            OffsetBuffer::new(offsets.clone().into()),
            values,
            list_nulls.clone(),
        );
        columns.push(Arc::new(list) as ArrayRef);
    }

    let out = StructArray::new(fields, columns, struct_null_builder.finish());
    work.flush()?;
    Ok(Arc::new(out))
}

pub fn build_final_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s GroupConcatState<A>>,
>(
    intermediate_type: &DataType,
    group_states: I,
    is_distinct: bool,
    is_asc_order: &[bool],
    nulls_first: &[bool],
    max_len: i64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError> {
    let mut builder = StringBuilder::new();
    for state in group_states {
        work.step()?;
        if state.rows.is_empty() {
            builder.append_null();
            continue;
        }

        let arg_types = intermediate_arg_types(intermediate_type)?;
        let layout = GroupConcatLayout::infer(arg_types.len(), is_asc_order.len())?;

        let sorted_indices = sort_indices(&state.rows, layout, is_asc_order, nulls_first, work)?;
        let duplicated =
            mark_duplicated_rows(&state.rows, &sorted_indices, layout, is_distinct, work)?;
        let mut last_unique_pos = None;
        for (pos, idx) in sorted_indices.iter().enumerate().rev() {
            work.step()?;
            if !duplicated[*idx] {
                last_unique_pos = Some(pos);
                break;
            }
        }
        let Some(last_unique_pos) = last_unique_pos else {
            builder.append_null();
            continue;
        };

        let mut out = String::new();
        let max_len = usize::try_from(max_len).unwrap_or(usize::MAX);
        let mut reached_limit = false;
        for (pos, idx) in sorted_indices.iter().enumerate().take(last_unique_pos + 1) {
            work.step()?;
            if duplicated[*idx] {
                continue;
            }
            let row = &state.rows[*idx];
            for col in 0..layout.output_col_num {
                work.step()?;
                let value = row[col].as_ref().ok_or_else(|| {
                    "group_concat output column should not contain null".to_string()
                })?;
                if append_with_limit(
                    &mut out,
                    &scalar_to_string(
                        &tracked_scalar_to_output(value, work)?,
                        &arg_types[col],
                        work,
                    )?,
                    max_len,
                ) {
                    reached_limit = true;
                    break;
                }
            }
            if reached_limit {
                break;
            }
            if pos != last_unique_pos
                && append_with_limit(
                    &mut out,
                    &separator_for_row(row, &arg_types, layout, work)?,
                    max_len,
                )
            {
                break;
            }
        }
        builder.append_value(out);
    }
    Ok(Arc::new(builder.finish()))
}
