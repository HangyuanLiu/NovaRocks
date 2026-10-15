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
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Array,
    Int32Builder, Int64Builder, ListArray, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
#[cfg(test)]
use arrow::datatypes::{Schema, SchemaRef};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};

use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::agg::{AggKernelEntry, AggregateAllocator, AggregateVec, aggregate_bytes};
#[cfg(test)]
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};
use novarocks_types::largeint;

use super::key_builder::{
    GroupKeyArrayView, canonical_group_key_equals, decode_group_key_rows,
    encode_group_key_row_tracked,
};

fn float32_key_equal(left: f32, right: f32) -> bool {
    (left.is_nan() && right.is_nan()) || left == right
}

fn float64_key_equal(left: f64, right: f64) -> bool {
    (left.is_nan() && right.is_nan()) || left == right
}

/// A vector whose nested, exclusively-owned payload is cached at mutation time.
///
/// The vector backing allocation is derived from `capacity()` in O(1). Callers
/// supply the retained bytes below each inserted element, which keeps the whole
/// aggregate key-state query O(1) even for nested list and Arrow values.
#[derive(Clone, Debug)]
pub struct RetainedVec<T> {
    values: AggregateVec<T>,
    nested_retained_bytes: usize,
}

impl<T> RetainedVec<T> {
    fn new_in(allocator: AggregateAllocator) -> Self {
        Self {
            values: AggregateVec::new_in(allocator),
            nested_retained_bytes: 0,
        }
    }

    fn push(&mut self, value: T, nested_retained_bytes: usize) {
        self.values.push(value);
        self.nested_retained_bytes = self
            .nested_retained_bytes
            .saturating_add(nested_retained_bytes);
    }

    fn try_reserve_one(&mut self, operation: &str) -> Result<(), String> {
        reserve_one(&mut self.values, operation)
    }

    fn allocator(&self) -> AggregateAllocator {
        self.values.allocator().clone()
    }

    fn retained_bytes(&self) -> usize {
        self.values
            .capacity()
            .saturating_mul(std::mem::size_of::<T>())
            .saturating_add(self.nested_retained_bytes)
    }
}

impl<T> Deref for RetainedVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<'a, T> IntoIterator for &'a RetainedVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum KeyColumn {
    Int8 {
        values: AggregateVec<i8>,
        nulls: AggregateVec<u8>,
    },
    Int16 {
        values: AggregateVec<i16>,
        nulls: AggregateVec<u8>,
    },
    Int32 {
        values: AggregateVec<i32>,
        nulls: AggregateVec<u8>,
    },
    Int64 {
        values: AggregateVec<i64>,
        nulls: AggregateVec<u8>,
    },
    Float32 {
        values: AggregateVec<f32>,
        nulls: AggregateVec<u8>,
    },
    Float64 {
        values: AggregateVec<f64>,
        nulls: AggregateVec<u8>,
    },
    Boolean {
        values: AggregateVec<u8>,
        nulls: AggregateVec<u8>,
    },
    Utf8 {
        offsets: AggregateVec<usize>,
        data: AggregateVec<u8>,
        nulls: AggregateVec<u8>,
    },
    Date32 {
        values: AggregateVec<i32>,
        nulls: AggregateVec<u8>,
    },
    Timestamp {
        values: AggregateVec<i64>,
        nulls: AggregateVec<u8>,
        unit: TimeUnit,
        tz: Option<Arc<str>>,
    },
    Decimal128 {
        values: AggregateVec<i128>,
        nulls: AggregateVec<u8>,
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        values: AggregateVec<i256>,
        nulls: AggregateVec<u8>,
        precision: u8,
        scale: i8,
    },
    LargeIntBinary {
        values: AggregateVec<i128>,
        nulls: AggregateVec<u8>,
    },
    ListUtf8 {
        values: RetainedVec<Option<AggregateVec<Option<AggregateVec<u8>>>>>,
    },
    ListInt32 {
        values: RetainedVec<Option<AggregateVec<Option<i32>>>>,
    },
    Complex {
        data_type: DataType,
        keys: RetainedVec<Option<AggregateVec<u8>>>,
    },
}

impl KeyColumn {
    #[cfg(test)]
    pub(crate) fn int8_for_test(values: Vec<i8>, nulls: Vec<u8>) -> Self {
        let allocator = test_allocator();
        Self::Int8 {
            values: tracked_test_vec(values, allocator.clone()),
            nulls: tracked_test_vec(nulls, allocator),
        }
    }

    #[cfg(test)]
    pub(crate) fn int64_for_test(values: Vec<i64>, nulls: Vec<u8>) -> Self {
        let allocator = test_allocator();
        Self::Int64 {
            values: tracked_test_vec(values, allocator.clone()),
            nulls: tracked_test_vec(nulls, allocator),
        }
    }

    pub(crate) fn try_reserve_value_from_view(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
    ) -> Result<(), String> {
        match self {
            KeyColumn::Int8 { values, nulls } => reserve_value_and_null(values, nulls, "int8"),
            KeyColumn::Int16 { values, nulls } => reserve_value_and_null(values, nulls, "int16"),
            KeyColumn::Int32 { values, nulls } => reserve_value_and_null(values, nulls, "int32"),
            KeyColumn::Int64 { values, nulls } => reserve_value_and_null(values, nulls, "int64"),
            KeyColumn::Float32 { values, nulls } => {
                reserve_value_and_null(values, nulls, "float32")
            }
            KeyColumn::Float64 { values, nulls } => {
                reserve_value_and_null(values, nulls, "float64")
            }
            KeyColumn::Boolean { values, nulls } => {
                reserve_value_and_null(values, nulls, "boolean")
            }
            KeyColumn::Utf8 {
                offsets,
                data,
                nulls,
            } => {
                reserve_one(offsets, "reserve utf8 group-key offsets")?;
                reserve_one(nulls, "reserve utf8 group-key nulls")?;
                let value_len = match view {
                    GroupKeyArrayView::Utf8(array) if !array.is_null(row) => array.value(row).len(),
                    GroupKeyArrayView::Dictionary(dict) => dict
                        .code_at(row)?
                        .map(|code| dict.value_bytes_for_code(code).map(<[u8]>::len))
                        .transpose()?
                        .unwrap_or(0),
                    _ => 0,
                };
                reserve_additional(data, value_len, "reserve utf8 group-key bytes")
            }
            KeyColumn::Date32 { values, nulls } => reserve_value_and_null(values, nulls, "date32"),
            KeyColumn::Timestamp { values, nulls, .. } => {
                reserve_value_and_null(values, nulls, "timestamp")
            }
            KeyColumn::Decimal128 { values, nulls, .. } => {
                reserve_value_and_null(values, nulls, "decimal128")
            }
            KeyColumn::Decimal256 { values, nulls, .. } => {
                reserve_value_and_null(values, nulls, "decimal256")
            }
            KeyColumn::LargeIntBinary { values, nulls } => {
                reserve_value_and_null(values, nulls, "largeint")
            }
            KeyColumn::ListUtf8 { values } => {
                values.try_reserve_one("reserve list-utf8 group-key values")
            }
            KeyColumn::ListInt32 { values } => {
                values.try_reserve_one("reserve list-int32 group-key values")
            }
            KeyColumn::Complex { keys, .. } => {
                keys.try_reserve_one("reserve complex group-key encoded values")
            }
        }
    }

    fn push_int_value<T>(
        values: &mut AggregateVec<T>,
        nulls: &mut AggregateVec<u8>,
        value: Option<i64>,
        type_name: &str,
    ) -> Result<(), String>
    where
        T: TryFrom<i64>,
    {
        if let Some(value) = value {
            let casted = T::try_from(value)
                .map_err(|_| format!("group key {} overflow: {}", type_name, value))?;
            values.push(casted);
            nulls.push(1);
        } else {
            let zero =
                T::try_from(0i64).map_err(|_| format!("group key {} zero overflow", type_name))?;
            values.push(zero);
            nulls.push(0);
        }
        Ok(())
    }

    fn int_value_equals<T>(
        values: &[T],
        nulls: &[u8],
        group_id: usize,
        value: Option<i64>,
        type_name: &str,
    ) -> Result<bool, String>
    where
        T: TryFrom<i64> + PartialEq,
    {
        let stored = values
            .get(group_id)
            .ok_or_else(|| "group key index out of bounds".to_string())?;
        let valid = *nulls
            .get(group_id)
            .ok_or_else(|| "group key index out of bounds".to_string())?
            != 0;
        match value {
            Some(value) => {
                let casted = T::try_from(value)
                    .map_err(|_| format!("group key {} overflow: {}", type_name, value))?;
                Ok(valid && *stored == casted)
            }
            None => Ok(!valid),
        }
    }

    fn largeint_value_at(arr: &FixedSizeBinaryArray, row: usize) -> Result<Option<i128>, String> {
        if arr.is_null(row) {
            return Ok(None);
        }
        largeint::i128_from_be_bytes(arr.value(row))
            .map(Some)
            .map_err(|e| format!("group key LARGEINT decode failed at row {}: {}", row, e))
    }

    pub fn push_value_from_view(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
    ) -> Result<(), String> {
        match (self, view) {
            (KeyColumn::Int8 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int8")
            }
            (KeyColumn::Int16 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int16")
            }
            (KeyColumn::Int32 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int32")
            }
            (KeyColumn::Int64 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int64")
            }
            (KeyColumn::Float32 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                if let Some(value) = view.value_at(row) {
                    values.push(value as f32);
                    nulls.push(1);
                } else {
                    values.push(0.0);
                    nulls.push(0);
                }
                Ok(())
            }
            (KeyColumn::Float64 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                if let Some(value) = view.value_at(row) {
                    values.push(value);
                    nulls.push(1);
                } else {
                    values.push(0.0);
                    nulls.push(0);
                }
                Ok(())
            }
            (KeyColumn::Boolean { values, nulls }, GroupKeyArrayView::Boolean(arr)) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(if arr.value(row) { 1 } else { 0 });
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                GroupKeyArrayView::Utf8(arr),
            ) => {
                if arr.is_null(row) {
                    nulls.push(0);
                } else {
                    data.extend_from_slice(arr.value(row).as_bytes());
                    nulls.push(1);
                }
                offsets.push(data.len());
                Ok(())
            }
            (
                KeyColumn::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                GroupKeyArrayView::Dictionary(dict),
            ) => {
                let Some(code) = dict.code_at(row)? else {
                    nulls.push(0);
                    offsets.push(data.len());
                    return Ok(());
                };
                data.extend_from_slice(dict.value_bytes_for_code(code)?);
                nulls.push(1);
                offsets.push(data.len());
                Ok(())
            }
            (KeyColumn::Date32 { values, nulls }, GroupKeyArrayView::Date32(arr)) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampSecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMillisecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMicrosecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampNanosecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (KeyColumn::Decimal128 { values, nulls, .. }, GroupKeyArrayView::Decimal128(arr)) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (KeyColumn::Decimal256 { values, nulls, .. }, GroupKeyArrayView::Decimal256(arr)) => {
                if arr.is_null(row) {
                    values.push(i256::ZERO);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::LargeIntBinary { values, nulls },
                GroupKeyArrayView::LargeIntBinary(arr),
            ) => {
                if let Some(value) = Self::largeint_value_at(arr, row)? {
                    values.push(value);
                    nulls.push(1);
                } else {
                    values.push(0);
                    nulls.push(0);
                }
                Ok(())
            }
            (
                KeyColumn::ListUtf8 { values: stored },
                GroupKeyArrayView::ListUtf8 { list, values },
            ) => {
                if list.is_null(row) {
                    stored.push(None, 0);
                    return Ok(());
                }
                let offsets = list.value_offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let allocator = stored.allocator();
                let mut items = AggregateVec::new_in(allocator.clone());
                items
                    .try_reserve_exact(end.saturating_sub(start))
                    .map_err(|_| allocator.allocation_error("reserve list-utf8 group-key items"))?;
                for index in start..end {
                    if values.is_null(index) {
                        items.push(None);
                    } else {
                        items.push(Some(aggregate_bytes(
                            allocator.clone(),
                            values.value(index).as_bytes(),
                        )?));
                    }
                }
                let nested_retained_bytes = items
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Option<AggregateVec<u8>>>())
                    .saturating_add(
                        items
                            .iter()
                            .flatten()
                            .map(|bytes| bytes.capacity())
                            .sum::<usize>(),
                    );
                stored.push(Some(items), nested_retained_bytes);
                Ok(())
            }
            (
                KeyColumn::ListInt32 { values: stored },
                GroupKeyArrayView::ListInt32 { list, values },
            ) => {
                if list.is_null(row) {
                    stored.push(None, 0);
                    return Ok(());
                }
                let offsets = list.value_offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let allocator = stored.allocator();
                let mut items = AggregateVec::new_in(allocator.clone());
                items
                    .try_reserve_exact(end.saturating_sub(start))
                    .map_err(|_| {
                        allocator.allocation_error("reserve list-int32 group-key items")
                    })?;
                for index in start..end {
                    items.push((!values.is_null(index)).then(|| values.value(index)));
                }
                let nested_retained_bytes = items
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Option<i32>>());
                stored.push(Some(items), nested_retained_bytes);
                Ok(())
            }
            (KeyColumn::Complex { keys, .. }, GroupKeyArrayView::Complex(array)) => {
                let encoded = encode_group_key_row_tracked(array, row, keys.allocator())?;
                let retained_bytes = encoded.as_ref().map_or(0, |bytes| bytes.capacity());
                keys.push(encoded, retained_bytes);
                Ok(())
            }
            _ => Err("group by key type mismatch".to_string()),
        }
    }

    pub fn value_equals(
        &self,
        group_id: usize,
        view: &GroupKeyArrayView<'_>,
        row: usize,
    ) -> Result<bool, String> {
        match (self, view) {
            (KeyColumn::Int8 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int8")
            }
            (KeyColumn::Int16 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int16")
            }
            (KeyColumn::Int32 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int32")
            }
            (KeyColumn::Int64 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int64")
            }
            (KeyColumn::Float32 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                match view.value_at(row) {
                    Some(value) => Ok(valid && float32_key_equal(*stored, value as f32)),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Float64 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                match view.value_at(row) {
                    Some(value) => Ok(valid && float64_key_equal(*stored, value)),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Boolean { values, nulls }, GroupKeyArrayView::Boolean(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && (*stored != 0) == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                GroupKeyArrayView::Utf8(arr),
            ) => {
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => {
                        if !valid {
                            return Ok(false);
                        }
                        let start = *offsets
                            .get(group_id)
                            .ok_or_else(|| "group key index out of bounds".to_string())?;
                        let end = *offsets
                            .get(group_id + 1)
                            .ok_or_else(|| "group key index out of bounds".to_string())?;
                        Ok(data
                            .get(start..end)
                            .is_some_and(|bytes| bytes == value.as_bytes()))
                    }
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                GroupKeyArrayView::Dictionary(dict),
            ) => {
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let Some(code) = dict.code_at(row)? else {
                    return Ok(!valid);
                };
                if !valid {
                    return Ok(false);
                }
                let start = *offsets
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let end = *offsets
                    .get(group_id + 1)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let current = dict.value_bytes_for_code(code)?;
                Ok(data.get(start..end).is_some_and(|bytes| bytes == current))
            }
            (KeyColumn::Date32 { values, nulls }, GroupKeyArrayView::Date32(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampSecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMillisecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMicrosecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampNanosecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Decimal128 { values, nulls, .. }, GroupKeyArrayView::Decimal128(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Decimal256 { values, nulls, .. }, GroupKeyArrayView::Decimal256(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::LargeIntBinary { values, nulls },
                GroupKeyArrayView::LargeIntBinary(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                match Self::largeint_value_at(arr, row)? {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::ListUtf8 { values: stored },
                GroupKeyArrayView::ListUtf8 { list, values },
            ) => {
                let saved = stored
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                if list.is_null(row) {
                    return Ok(saved.is_none());
                }
                let Some(saved) = saved else {
                    return Ok(false);
                };
                let offsets = list.value_offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if saved.len() != end.saturating_sub(start) {
                    return Ok(false);
                }
                Ok(saved
                    .iter()
                    .zip(start..end)
                    .all(|(saved, index)| match saved {
                        None => values.is_null(index),
                        Some(saved) => {
                            !values.is_null(index)
                                && saved.as_slice() == values.value(index).as_bytes()
                        }
                    }))
            }
            (
                KeyColumn::ListInt32 { values: stored },
                GroupKeyArrayView::ListInt32 { list, values },
            ) => {
                let saved = stored
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                if list.is_null(row) {
                    return Ok(saved.is_none());
                }
                let Some(saved) = saved else {
                    return Ok(false);
                };
                let offsets = list.value_offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if saved.len() != end.saturating_sub(start) {
                    return Ok(false);
                }
                Ok(saved
                    .iter()
                    .zip(start..end)
                    .all(|(saved, index)| match saved {
                        None => values.is_null(index),
                        Some(saved) => !values.is_null(index) && *saved == values.value(index),
                    }))
            }
            (KeyColumn::Complex { keys, .. }, GroupKeyArrayView::Complex(array)) => {
                let saved = keys
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                match (saved, array.is_null(row)) {
                    (None, true) => Ok(true),
                    (None, false) | (Some(_), true) => Ok(false),
                    (Some(saved), false) => canonical_group_key_equals(array, row, saved),
                }
            }
            _ => Err("group by key type mismatch".to_string()),
        }
    }

    pub fn to_array(&self) -> Result<ArrayRef, String> {
        match self {
            KeyColumn::Int8 { values, nulls } => {
                let mut builder = Int8Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Int16 { values, nulls } => {
                let mut builder = Int16Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Int32 { values, nulls } => {
                let mut builder = Int32Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Int64 { values, nulls } => {
                let mut builder = Int64Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Float32 { values, nulls } => {
                let mut builder = Float32Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Float64 { values, nulls } => {
                let mut builder = Float64Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Boolean { values, nulls } => {
                let mut builder = BooleanBuilder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value != 0);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Utf8 {
                offsets,
                data,
                nulls,
            } => {
                let mut builder = StringBuilder::new();
                if offsets.len() < nulls.len() + 1 {
                    return Err("group key offsets out of bounds".to_string());
                }
                for idx in 0..nulls.len() {
                    if nulls[idx] == 0 {
                        builder.append_null();
                        continue;
                    }
                    let start = offsets[idx];
                    let end = offsets[idx + 1];
                    let bytes = data
                        .get(start..end)
                        .ok_or_else(|| "group key utf8 slice out of bounds".to_string())?;
                    let value = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
                    builder.append_value(value);
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Date32 { values, nulls } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                Ok(Arc::new(Date32Array::from(out)))
            }
            KeyColumn::Timestamp {
                values,
                nulls,
                unit,
                tz,
            } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                let array: ArrayRef = match unit {
                    TimeUnit::Second => {
                        let array = TimestampSecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                    TimeUnit::Millisecond => {
                        let array = TimestampMillisecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                    TimeUnit::Microsecond => {
                        let array = TimestampMicrosecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let array = TimestampNanosecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                };
                Ok(array)
            }
            KeyColumn::Decimal128 {
                values,
                nulls,
                precision,
                scale,
            } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                let array = Decimal128Array::from(out)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(Arc::new(array))
            }
            KeyColumn::Decimal256 {
                values,
                nulls,
                precision,
                scale,
            } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                let array = Decimal256Array::from(out)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(Arc::new(array))
            }
            KeyColumn::LargeIntBinary { values, nulls } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                largeint::array_from_i128(&out)
            }
            KeyColumn::ListUtf8 { values } => {
                let mut flat = Vec::<Option<String>>::new();
                let mut offsets = Vec::with_capacity(values.len() + 1);
                offsets.push(0_i32);
                let mut nulls = NullBufferBuilder::new(values.len());
                let mut current: i64 = 0;
                for value in values {
                    match value {
                        None => {
                            nulls.append_null();
                            offsets.push(current as i32);
                        }
                        Some(items) => {
                            current = current
                                .checked_add(items.len() as i64)
                                .ok_or_else(|| "group key list offset overflow".to_string())?;
                            if current > i32::MAX as i64 {
                                return Err("group key list offset overflow".to_string());
                            }
                            for item in items {
                                flat.push(
                                    item.as_ref()
                                        .map(|bytes| {
                                            String::from_utf8(bytes.to_vec()).map_err(|error| {
                                                format!(
                                                    "stored list-utf8 group key is invalid: {error}"
                                                )
                                            })
                                        })
                                        .transpose()?,
                                );
                            }
                            nulls.append_non_null();
                            offsets.push(current as i32);
                        }
                    }
                }
                let item_array = Arc::new(StringArray::from(flat)) as ArrayRef;
                let field = Arc::new(Field::new("item", DataType::Utf8, true));
                let array = ListArray::new(
                    field,
                    OffsetBuffer::new(offsets.into()),
                    item_array,
                    nulls.finish(),
                );
                Ok(Arc::new(array))
            }
            KeyColumn::ListInt32 { values } => {
                let mut flat = Vec::<Option<i32>>::new();
                let mut offsets = Vec::with_capacity(values.len() + 1);
                offsets.push(0_i32);
                let mut nulls = NullBufferBuilder::new(values.len());
                let mut current: i64 = 0;
                for value in values {
                    match value {
                        None => {
                            nulls.append_null();
                            offsets.push(current as i32);
                        }
                        Some(items) => {
                            current = current
                                .checked_add(items.len() as i64)
                                .ok_or_else(|| "group key list offset overflow".to_string())?;
                            if current > i32::MAX as i64 {
                                return Err("group key list offset overflow".to_string());
                            }
                            flat.extend(items.iter().copied());
                            nulls.append_non_null();
                            offsets.push(current as i32);
                        }
                    }
                }
                let item_array = Arc::new(Int32Array::from(flat)) as ArrayRef;
                let field = Arc::new(Field::new("item", DataType::Int32, true));
                let array = ListArray::new(
                    field,
                    OffsetBuffer::new(offsets.into()),
                    item_array,
                    nulls.finish(),
                );
                Ok(Arc::new(array))
            }
            KeyColumn::Complex { data_type, keys } => {
                let rows = keys
                    .iter()
                    .map(|row| row.as_ref().map(|bytes| bytes.as_slice()))
                    .collect::<Vec<_>>();
                decode_group_key_rows(data_type, &rows)
            }
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            KeyColumn::Int8 { .. } => DataType::Int8,
            KeyColumn::Int16 { .. } => DataType::Int16,
            KeyColumn::Int32 { .. } => DataType::Int32,
            KeyColumn::Int64 { .. } => DataType::Int64,
            KeyColumn::Float32 { .. } => DataType::Float32,
            KeyColumn::Float64 { .. } => DataType::Float64,
            KeyColumn::Boolean { .. } => DataType::Boolean,
            KeyColumn::Utf8 { .. } => DataType::Utf8,
            KeyColumn::Date32 { .. } => DataType::Date32,
            KeyColumn::Timestamp { unit, tz, .. } => {
                let tz_arc = tz.as_deref().map(Arc::<str>::from);
                DataType::Timestamp(*unit, tz_arc)
            }
            KeyColumn::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
            KeyColumn::Decimal256 {
                precision, scale, ..
            } => DataType::Decimal256(*precision, *scale),
            KeyColumn::LargeIntBinary { .. } => {
                DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH)
            }
            KeyColumn::ListUtf8 { .. } => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
            KeyColumn::ListInt32 { .. } => {
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
            }
            KeyColumn::Complex { data_type, .. } => data_type.clone(),
        }
    }

    pub fn has_nulls(&self) -> bool {
        match self {
            KeyColumn::Int8 { nulls, .. }
            | KeyColumn::Int16 { nulls, .. }
            | KeyColumn::Int32 { nulls, .. }
            | KeyColumn::Int64 { nulls, .. }
            | KeyColumn::Float32 { nulls, .. }
            | KeyColumn::Float64 { nulls, .. }
            | KeyColumn::Boolean { nulls, .. }
            | KeyColumn::Utf8 { nulls, .. }
            | KeyColumn::Date32 { nulls, .. }
            | KeyColumn::Timestamp { nulls, .. }
            | KeyColumn::Decimal128 { nulls, .. }
            | KeyColumn::Decimal256 { nulls, .. }
            | KeyColumn::LargeIntBinary { nulls, .. } => nulls.contains(&0),
            KeyColumn::ListUtf8 { values } => values.iter().any(|value| value.is_none()),
            KeyColumn::ListInt32 { values } => values.iter().any(|value| value.is_none()),
            KeyColumn::Complex { keys, .. } => keys.iter().any(|value| value.is_none()),
        }
    }

    /// Returns heap memory retained exclusively by this column in O(1).
    ///
    /// Inline enum storage is part of `KeyTable::key_columns` and is therefore
    /// not repeated here. Complex values retain only canonical encoded bytes.
    pub(crate) fn retained_bytes(&self) -> usize {
        fn vec_bytes<T>(values: &AggregateVec<T>) -> usize {
            values.capacity().saturating_mul(std::mem::size_of::<T>())
        }

        match self {
            KeyColumn::Int8 { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Int16 { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Int32 { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Int64 { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Float32 { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Float64 { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Boolean { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Utf8 {
                offsets,
                data,
                nulls,
            } => vec_bytes(offsets) + vec_bytes(data) + vec_bytes(nulls),
            KeyColumn::Date32 { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Timestamp { values, nulls, .. } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Decimal128 { values, nulls, .. } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::Decimal256 { values, nulls, .. } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::LargeIntBinary { values, nulls } => vec_bytes(values) + vec_bytes(nulls),
            KeyColumn::ListUtf8 { values } => values.retained_bytes(),
            KeyColumn::ListInt32 { values } => values.retained_bytes(),
            KeyColumn::Complex { keys, .. } => keys.retained_bytes(),
        }
    }
}

fn reserve_one<T>(values: &mut AggregateVec<T>, operation: &str) -> Result<(), String> {
    reserve_additional(values, 1, operation)
}

fn reserve_additional<T>(
    values: &mut AggregateVec<T>,
    additional: usize,
    operation: &str,
) -> Result<(), String> {
    let allocator = values.allocator().clone();
    values
        .try_reserve(additional)
        .map_err(|_| allocator.allocation_error(operation))
}

fn reserve_value_and_null<T>(
    values: &mut AggregateVec<T>,
    nulls: &mut AggregateVec<u8>,
    type_name: &str,
) -> Result<(), String> {
    reserve_one(values, &format!("reserve {type_name} group-key values"))?;
    reserve_one(nulls, &format!("reserve {type_name} group-key nulls"))
}

#[cfg(test)]
fn test_allocator() -> AggregateAllocator {
    AggregateAllocator::new(MemTracker::new_child(
        "KeyColumnTest",
        &process_mem_tracker(),
    ))
}

#[cfg(test)]
fn tracked_test_vec<T>(values: Vec<T>, allocator: AggregateAllocator) -> AggregateVec<T> {
    let mut tracked = AggregateVec::new_in(allocator);
    tracked
        .try_reserve_exact(values.len())
        .expect("reserve test key-column values");
    tracked.extend(values);
    tracked
}

#[cfg(test)]
pub(crate) fn key_column_from_type(data_type: &DataType) -> Result<KeyColumn, String> {
    key_column_from_type_in(
        data_type,
        AggregateAllocator::new(MemTracker::new_child(
            "KeyColumnUnbounded",
            &process_mem_tracker(),
        )),
    )
}

pub(crate) fn key_column_from_type_in(
    data_type: &DataType,
    allocator: AggregateAllocator,
) -> Result<KeyColumn, String> {
    match data_type {
        DataType::Int8 => Ok(KeyColumn::Int8 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Int16 => Ok(KeyColumn::Int16 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Int32 => Ok(KeyColumn::Int32 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Int64 => Ok(KeyColumn::Int64 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Float32 => Ok(KeyColumn::Float32 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Float64 => Ok(KeyColumn::Float64 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Boolean => Ok(KeyColumn::Boolean {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Utf8 => {
            let mut offsets = AggregateVec::new_in(allocator.clone());
            offsets
                .try_reserve_exact(1)
                .map_err(|_| allocator.allocation_error("initialize group-key offsets"))?;
            offsets.push(0);
            Ok(KeyColumn::Utf8 {
                offsets,
                data: AggregateVec::new_in(allocator.clone()),
                nulls: AggregateVec::new_in(allocator.clone()),
            })
        }
        DataType::Date32 => Ok(KeyColumn::Date32 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
        }),
        DataType::Timestamp(unit, tz) => Ok(KeyColumn::Timestamp {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
            unit: *unit,
            tz: tz.clone(),
        }),
        DataType::Decimal128(precision, scale) => Ok(KeyColumn::Decimal128 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
            precision: *precision,
            scale: *scale,
        }),
        DataType::Decimal256(precision, scale) => Ok(KeyColumn::Decimal256 {
            values: AggregateVec::new_in(allocator.clone()),
            nulls: AggregateVec::new_in(allocator.clone()),
            precision: *precision,
            scale: *scale,
        }),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            Ok(KeyColumn::LargeIntBinary {
                values: AggregateVec::new_in(allocator.clone()),
                nulls: AggregateVec::new_in(allocator.clone()),
            })
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Utf8) => {
            Ok(KeyColumn::ListUtf8 {
                values: RetainedVec::new_in(allocator.clone()),
            })
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Int32) => {
            Ok(KeyColumn::ListInt32 {
                values: RetainedVec::new_in(allocator.clone()),
            })
        }
        DataType::Null => Err("group by type is null".to_string()),
        other => Ok(KeyColumn::Complex {
            data_type: other.clone(),
            keys: RetainedVec::new_in(allocator),
        }),
    }
}

/// Validate the complete original output before any runtime schema construction.
/// Array representation normalization remains the existing typed Chunk author.
pub(crate) fn validate_typed_aggregate_output(
    schema: &ChunkSchema,
    arena: &crate::exec::expr::ExprArena,
    groups: &[crate::exec::expr::ExprId],
    kernels: &[AggKernelEntry],
    intermediate: bool,
    mut work: impl FnMut(usize) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure>,
) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    work(0)?;
    let count = groups
        .len()
        .checked_add(kernels.len())
        .ok_or(LayoutResourceError::Arithmetic)?;
    if !schema.is_typed() || schema.slots().len() < count {
        return Err(ConstructionFailure::InvalidSource(
            "aggregate output requires its complete declared channels".into(),
        ));
    }
    for (index, expression) in groups.iter().enumerate() {
        work(1)?;
        let actual = arena.value_type(*expression).ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "aggregate group output lacks its complete original type author".into(),
            )
        })?;
        let declared = schema.slots()[index].typed_binding();
        if (actual.nullable() && !declared.nullable())
            || !actual
                .typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(
                    declared.borrowed(),
                    || work(1),
                )?
        {
            return Err(ConstructionFailure::InvalidSource(format!(
                "aggregate group output binding differs at column {index}",
            )));
        }
    }
    for (index, kernel) in kernels.iter().enumerate() {
        work(1)?;
        let declared = schema.slots()[groups.len() + index].typed_binding();
        let actual = kernel
            .frozen_output_value_type(intermediate)
            .ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "aggregate kernel lacks its complete frozen output author".into(),
                )
            })?;
        if (actual.nullable() && !declared.nullable())
            || !actual
                .typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(
                    declared.borrowed(),
                    || work(1),
                )?
        {
            return Err(ConstructionFailure::InvalidSource(format!(
                "aggregate frozen output binding differs at column {}",
                groups.len() + index,
            )));
        }
    }
    Ok(())
}

/// The local two-phase rewrite creates a new Partial occurrence. Its state
/// fields come from the original frozen intermediate declarations, never from
/// the final result carrier. The genuine final port is validated first.
pub(crate) fn create_local_partial_aggregate_schema(
    final_schema: &ChunkSchemaRef,
    arena: &crate::exec::expr::ExprArena,
    groups: &[crate::exec::expr::ExprId],
    kernels: &[AggKernelEntry],
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
) -> Result<ChunkSchemaRef, crate::exec::pipeline::operator::ConstructionFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    use std::alloc::Layout;
    validate_typed_aggregate_output(final_schema, arena, groups, kernels, false, |units| {
        factory.observe_input_work(units)
    })?;
    let count = groups
        .len()
        .checked_add(kernels.len())
        .ok_or(LayoutResourceError::Arithmetic)?;
    let owner = factory.begin_typed_construction(final_schema)?;
    owner.request(
        Layout::array::<ChunkSlotSchema>(count).map_err(|_| LayoutResourceError::Arithmetic)?,
    )?;
    let mut slots = Vec::with_capacity(count);
    for slot in &final_schema.slots()[..groups.len()] {
        factory.observe_input_work(1)?;
        slots.push(slot.clone());
    }
    for (index, kernel) in kernels.iter().enumerate() {
        factory.observe_input_work(1)?;
        let original = &final_schema.slots()[groups.len() + index];
        let state = kernel.frozen_output_value_type(true).ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "local partial aggregate lacks its frozen state declaration".into(),
            )
        })?;
        owner.request(
            Layout::array::<u8>(original.field().name().len())
                .map_err(|_| LayoutResourceError::Arithmetic)?,
        )?;
        owner.request(arc_layout(Layout::new::<Field>())?)?;
        factory.observe_input_work(1)?;
        let field = Arc::new(Field::new(
            original.field().name(),
            state.data_type().clone(),
            state.nullable(),
        ));
        slots.push(ChunkSlotSchema::try_new_typed_observed(
            original.slot_id(),
            field,
            state
                .typed_binding()
                .clone_observed(|| factory.observe_input_work(1))?,
            original.unique_id(),
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?);
    }
    let schema = ChunkSchema::try_new_typed_observed(
        slots,
        owner.storage_lifetime(),
        |layout| owner.request(layout),
        |units| factory.observe_input_work(units),
    )?;
    owner.request(arc_layout(Layout::new::<ChunkSchema>())?)?;
    Ok(Arc::new(schema))
}

/// A flat loan set follows only Arrow backing actually reused by output arrays.
/// It never retains the arena, kernel set, factory, or task control.
struct AggregateArrayTypeLoans {
    _loans: Vec<novarocks_type_contract::ValueTypeStorageLifetime>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl novarocks_type_contract::ValueTypeStorageHold for AggregateArrayTypeLoans {}

pub(crate) struct AggregateOutputCreation {
    owner: Box<dyn crate::exec::pipeline::operator::InputConstructionOwner>,
    schema: ChunkSchemaRef,
    array_storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl AggregateOutputCreation {
    /// Begin before the original output-column Vec and array constructors.
    pub(crate) fn begin(
        source: &ChunkSchemaRef,
        arena: &crate::exec::expr::ExprArena,
        groups: &[crate::exec::expr::ExprId],
        kernels: &[AggKernelEntry],
        intermediate: bool,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    ) -> crate::runtime::fragment::ExecutionResult<Self> {
        use crate::exec::chunk::ArrayNormalizationStep;
        use crate::exec::pipeline::operator::ConstructionFailure;
        use novarocks_type_contract::{
            ValueTypeStorageLifetime,
            owned_resources::layout::{LayoutResourceError, arc_layout},
        };
        use std::alloc::Layout;
        validate_typed_aggregate_output(source, arena, groups, kernels, intermediate, |units| {
            factory.observe_input_work(units)
        })?;
        let count = groups
            .len()
            .checked_add(kernels.len())
            .ok_or(LayoutResourceError::Arithmetic)
            .map_err(ConstructionFailure::from)?;
        let schema = if count == source.slots().len() {
            source.clone()
        } else {
            let container = factory.begin_typed_construction(source)?;
            container.request(
                Layout::array::<ChunkSlotSchema>(count)
                    .map_err(|_| ConstructionFailure::from(LayoutResourceError::Arithmetic))?,
            )?;
            let mut slots = Vec::with_capacity(count);
            for slot in &source.slots()[..count] {
                factory.observe_input_work(1)?;
                slots.push(slot.clone());
            }
            let schema = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
                slots,
                container.storage_lifetime(),
                |layout| container.request(layout),
                |units| factory.observe_input_work(units),
            )?;
            container.request(
                arc_layout(Layout::new::<ChunkSchema>()).map_err(ConstructionFailure::from)?,
            )?;
            Arc::new(schema)
        };
        let owner = factory.begin_typed_construction(&schema)?;
        let mut loans = Vec::new();
        if count != 0 {
            owner.request(
                Layout::array::<ValueTypeStorageLifetime>(count)
                    .map_err(|_| ConstructionFailure::from(LayoutResourceError::Arithmetic))?,
            )?;
            loans = Vec::with_capacity(count);
        }
        for expression in groups {
            factory.observe_input_work(1)?;
            let value = arena.value_type(*expression).ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "aggregate group output lacks its complete original type author".into(),
                )
            })?;
            loans.push(
                value
                    .typed_binding()
                    .layout()
                    .arrow_backing_storage_lifetime(),
            );
        }
        for kernel in kernels {
            factory.observe_input_work(1)?;
            let value = kernel
                .frozen_output_value_type(intermediate)
                .ok_or_else(|| {
                    ConstructionFailure::InvalidSource(
                        "aggregate array output lacks its complete original type author".into(),
                    )
                })?;
            loans.push(
                value
                    .typed_binding()
                    .layout()
                    .arrow_backing_storage_lifetime(),
            );
        }
        let array_storage = if count == 0 {
            owner.storage_lifetime()
        } else {
            owner.request(
                arc_layout(Layout::new::<AggregateArrayTypeLoans>())
                    .map_err(ConstructionFailure::from)?,
            )?;
            ValueTypeStorageLifetime::held(Arc::new(AggregateArrayTypeLoans {
                _loans: loans,
                _storage: owner.storage_lifetime(),
            }))
        };
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::CreateChunkColumns { channels: count },
        )?;
        Ok(Self {
            owner,
            schema,
            array_storage,
        })
    }

    pub(crate) fn finish(
        self,
        arrays: Vec<arrow::array::ArrayRef>,
        rows: usize,
    ) -> crate::runtime::fragment::ExecutionResult<crate::exec::chunk::Chunk> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        Ok(
            crate::exec::chunk::Chunk::try_new_typed_with_columns_owned_observed::<
                ConstructionFailure,
                _,
            >(self.schema, arrays, rows, self.array_storage, |step| {
                self.owner.observe(0, &[], step)
            })?,
        )
    }
}

#[cfg(test)]
pub(crate) fn build_output_schema_from_kernels(
    key_columns: &[KeyColumn],
    kernels: &[AggKernelEntry],
    output_intermediate: bool,
    output_chunk_schema: &ChunkSchemaRef,
    group_key_nullable: Option<&[bool]>,
) -> Result<SchemaRef, String> {
    let output_slots = output_chunk_schema.slot_ids();
    let mut fields: Vec<Field> = Vec::with_capacity(key_columns.len() + kernels.len());
    let mut chunk_slots = Vec::with_capacity(key_columns.len() + kernels.len());
    for (idx, col) in key_columns.iter().enumerate() {
        let slot_id = output_slots.get(idx).copied().ok_or_else(|| {
            format!(
                "aggregate output slot count mismatch: missing group slot at index {}",
                idx
            )
        })?;
        let slot_schema = output_chunk_schema.slot(slot_id);
        let runtime_nullable = group_key_nullable
            .and_then(|nullable| nullable.get(idx).copied())
            .unwrap_or_else(|| col.has_nulls());
        let nullable = slot_schema
            .map(|schema| schema.nullable() || runtime_nullable)
            .unwrap_or(true);
        let field = slot_schema
            .map(|schema| Field::new(schema.name(), col.data_type(), nullable))
            .unwrap_or_else(|| Field::new(format!("group_{}", idx), col.data_type(), true));
        let chunk_slot = if let Some(schema) = slot_schema {
            schema.with_field_and_slot_id(slot_id, field.clone())?
        } else {
            ChunkSlotSchema::new_with_field(slot_id, field.clone(), None, None)
        };
        fields.push(field);
        chunk_slots.push(chunk_slot);
    }
    for (idx, kernel) in kernels.iter().enumerate() {
        let slot_idx = key_columns.len() + idx;
        let slot_id = output_slots.get(slot_idx).copied().ok_or_else(|| {
            format!(
                "aggregate output slot count mismatch: missing agg slot at index {}",
                slot_idx
            )
        })?;
        let slot_schema = output_chunk_schema.slot(slot_id);
        let field = slot_schema
            .map(|schema| {
                Field::new(
                    schema.name(),
                    kernel.output_type(output_intermediate),
                    schema.nullable(),
                )
            })
            .unwrap_or_else(|| {
                Field::new(
                    format!("agg_{}", idx),
                    kernel.output_type(output_intermediate),
                    true,
                )
            });
        let chunk_slot = if let Some(schema) = slot_schema {
            schema.with_field(field.clone())?
        } else {
            ChunkSlotSchema::new_with_field(slot_id, field.clone(), None, None)
        };
        fields.push(field);
        chunk_slots.push(chunk_slot);
    }
    // StarRocks FE may include extra materialized slots in the output tuple (e.g. intermediate /
    // passthrough slots) that are not part of the final operator output schema we construct here.
    // We only require that the provided output_slots cover the columns we actually output.
    if !output_slots.is_empty() && output_slots.len() < fields.len() {
        return Err(format!(
            "aggregate output slot count mismatch: slots={} columns={}",
            output_slots.len(),
            fields.len()
        ));
    }
    let _chunk_schema = ChunkSchema::try_new(chunk_slots)?;
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::{
        KeyColumn, RetainedVec, build_output_schema_from_kernels, key_column_from_type,
        test_allocator,
    };
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use novarocks_types::SlotId;
    use std::sync::Arc;

    #[test]
    fn build_output_schema_marks_nullable_group_keys_when_runtime_keys_contain_nulls() {
        let key_columns = vec![KeyColumn::int8_for_test(vec![0, 1], vec![0, 1])];
        let output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(13),
                Field::new("col_5_13", DataType::Int8, false),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let schema =
            build_output_schema_from_kernels(&key_columns, &[], false, &output_chunk_schema, None)
                .expect("output schema");

        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn utf8_key_column_pushes_dictionary_values_as_flat_utf8() {
        use crate::exec::hash_table::key_builder::{GroupKeyArrayView, build_group_key_views};

        let dict: ArrayRef = Arc::new(
            vec![Some("PAID"), None, Some("NEW"), Some("PAID")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let arrays = [dict];
        let views = build_group_key_views(&arrays).expect("views");
        let view = &views[0];
        let mut col = key_column_from_type(&DataType::Utf8).expect("utf8 key column");
        col.try_reserve_value_from_view(view, 0)
            .expect("reserve paid");
        col.push_value_from_view(view, 0).expect("push paid");
        col.try_reserve_value_from_view(view, 1)
            .expect("reserve null");
        col.push_value_from_view(view, 1).expect("push null");
        col.try_reserve_value_from_view(view, 2)
            .expect("reserve new");
        col.push_value_from_view(view, 2).expect("push new");

        let out = col.to_array().expect("array");
        assert_eq!(out.data_type(), &DataType::Utf8);
        let strings = out.as_any().downcast_ref::<StringArray>().expect("strings");
        assert_eq!(strings.value(0), "PAID");
        assert!(strings.is_null(1));
        assert_eq!(strings.value(2), "NEW");
        assert!(
            col.value_equals(0, view, 3)
                .expect("dictionary value equality")
        );
        assert!(
            col.value_equals(1, view, 1)
                .expect("dictionary null equality")
        );
        assert!(
            !col.value_equals(2, view, 0)
                .expect("dictionary value mismatch")
        );

        let GroupKeyArrayView::Dictionary(_) = view else {
            panic!("expected dictionary view");
        };
    }

    #[test]
    fn retained_vec_caches_nested_owned_payload_without_rescanning() {
        let mut retained = RetainedVec::new_in(test_allocator());
        let mut text = String::with_capacity(37);
        text.push_str("value");
        let mut row = Vec::with_capacity(5);
        row.push(Some(text));
        let nested = row
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<String>>())
            .saturating_add(row[0].as_ref().expect("text").capacity());
        retained.push(Some(row), nested);

        assert_eq!(
            retained.retained_bytes(),
            retained
                .values
                .capacity()
                .saturating_mul(std::mem::size_of::<Option<Vec<Option<String>>>>())
                .saturating_add(nested)
        );
    }
}

#[cfg(test)]
mod typed_output_owner_tests {
    use super::AggregateOutputCreation;
    use crate::exec::chunk::{ChunkSchemaRef, typed_fixture};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
    use crate::runtime::fragment::{ExecutionFailure, ExecutionFailureCause};
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use arrow::array::{Array, ArrayRef, ListArray, StringArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_type_contract::{
        FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn declared(kind: ValueKind) -> FunctionValueType {
        let storage = ValueTypeStorageLifetime::pure();
        let child = ValueTypeRef::try_new(kind, false, storage.clone()).unwrap();
        let ty = ValueTypeRef::try_new(ValueKind::List(child), false, storage.clone()).unwrap();
        let carrier = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
        FunctionValueType::try_from_value(ty, ValueLayout::try_new(&carrier, storage).unwrap())
            .unwrap()
    }
    fn fixture() -> (
        Arc<MemTracker>,
        Arc<RuntimeErrorState>,
        Arc<dyn InputConstructionFactory>,
        ChunkSchemaRef,
        ExprArena,
        crate::exec::expr::ExprId,
    ) {
        let tracker = MemTracker::new_root("aggregate typed output structure");
        let errors = Arc::new(RuntimeErrorState::default());
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            errors.clone(),
        )
        .unwrap();
        let ty = declared(ValueKind::Nominal(ValueLogicalType::Json));
        let layout = typed_fixture::layout(
            Arc::new(Schema::new(vec![Field::new(
                "documents",
                ty.data_type().clone(),
                false,
            )])),
            &[SlotId::new(1)],
            &[ty.clone()],
        );
        let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        let mut arena = ExprArena::default();
        let expression = arena.push_value_typed(ExprNode::SlotId(SlotId::new(1)), ty);
        (tracker, errors, factory, schema, arena, expression)
    }
    fn values() -> ArrayRef {
        // This is fixture payload. Its original noncanonical anonymous label
        // exercises the real output normalizer, never a semantic author.
        Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("source_item", DataType::Utf8, false)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
                Arc::new(StringArray::from(vec!["{}", "[]"])),
                None,
            )
            .unwrap(),
        )
    }
    #[test]
    fn aggregate_typed_output_normalizes_representation_and_keeps_complete_owner_until_last_exit() {
        let (tracker, errors, factory, schema, arena, expression) = fixture();
        let weak_factory = Arc::downgrade(&factory);
        let input = values();
        let original_data = input.to_data();
        let created = AggregateOutputCreation::begin(
            &schema,
            &arena,
            &[expression],
            &[],
            false,
            factory.as_ref(),
        )
        .unwrap();
        let output = created.finish(vec![input.clone()], 2).unwrap();
        assert!(output.chunk_schema().is_typed());
        assert_eq!(
            output.chunk_schema().slots()[0].typed_binding(),
            schema.slots()[0].typed_binding()
        );
        assert_eq!(
            output.columns()[0].data_type(),
            schema.slots()[0].data_type()
        );
        assert_eq!(input.to_data(), original_data);
        assert_eq!(output.columns()[0].null_count(), 0);
        let list = output.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let strings = list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((strings.value(0), strings.value(1)), ("{}", "[]"));
        let projection = output.into_batch_projection();
        drop(schema);
        drop(arena);
        drop(factory);
        drop(errors);
        assert!(weak_factory.upgrade().is_none());
        assert!(
            tracker.current() > 0,
            "independent batch projection retains only real finite structure"
        );
        assert_eq!(
            projection.record_batch().schema_ref().fields()[0]
                .metadata()
                .capacity(),
            0
        );
        drop(projection);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn aggregate_typed_output_refuses_same_carrier_wrong_domain_before_new_scope() {
        let (tracker, errors, factory, schema, _, _) = fixture();
        let mut arena = ExprArena::default();
        let expression = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(1)),
            declared(ValueKind::String),
        );
        let allocated = tracker.allocated();
        let failure = AggregateOutputCreation::begin(
            &schema,
            &arena,
            &[expression],
            &[],
            false,
            factory.as_ref(),
        )
        .err()
        .unwrap();
        assert!(
            matches!(failure.cause(), ExecutionFailureCause::Construction(ConstructionFailure::InvalidSource(detail)) if detail == "aggregate group output binding differs at column 0")
        );
        assert_eq!(
            tracker.allocated(),
            allocated,
            "wrong declaration precedes all constructor requests"
        );
        drop(schema);
        drop(arena);
        drop(factory);
        drop(errors);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn aggregate_typed_output_real_memory_and_original_control_stop_before_output_construction() {
        let (tracker, errors, factory, schema, arena, expression) = fixture();
        let prior = tracker.current();
        tracker.install_limit_once(prior + 1).unwrap();
        let input = values();
        let original = input.to_data();
        let failure = AggregateOutputCreation::begin(
            &schema,
            &arena,
            &[expression],
            &[],
            false,
            factory.as_ref(),
        )
        .err()
        .unwrap();
        assert!(matches!(
            failure.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
        ));
        assert_eq!(tracker.current(), prior, "refused scope is refunded");
        assert_eq!(input.to_data(), original);
        let stopped = ExecutionFailure::from(ConstructionFailure::from(
            novarocks_type_contract::CompileControlError::Cancelled,
        ));
        errors.set_error(stopped.clone());
        let allocated = tracker.allocated();
        let failure = AggregateOutputCreation::begin(
            &schema,
            &arena,
            &[expression],
            &[],
            false,
            factory.as_ref(),
        )
        .err()
        .unwrap();
        assert_eq!(
            failure, stopped,
            "the original caller cause is not converted to a schema String"
        );
        assert_eq!(
            tracker.allocated(),
            allocated,
            "control refusal has no following request"
        );
        drop(schema);
        drop(arena);
        drop(factory);
        drop(errors);
        assert_eq!(tracker.current(), 0);
    }
}
