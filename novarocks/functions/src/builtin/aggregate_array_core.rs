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
//! Original ARRAY aggregate computation, scalar output author and row-state codec.
use crate::{
    aggregate_scalar::{
        AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork, TrackedAggScalarValue,
        aggregate_vec_with_capacity, tracked_scalar_from_array, tracked_scalar_heap_capacity,
        tracked_scalar_to_output,
    },
    aggregate_scalar_fingerprint::{
        compare_scalar_values, key_fingerprint, tracked_optional_key_fingerprint,
    },
    largeint,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray, NullArray, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use hashbrown::{HashSet as ScalarSet, hash_map::DefaultHashBuilder};
use std::{cmp::Ordering, collections::HashSet, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub enum ArrayAggKind<'a> {
    Array {
        distinct: bool,
        ascending: &'a [bool],
        nulls_first: &'a [bool],
    },
    Unique,
}
#[derive(Clone, Copy, Debug)]
pub struct ArrayAggConfig<'a> {
    pub kind: ArrayAggKind<'a>,
    pub output_type: &'a DataType,
    pub intermediate_type: &'a DataType,
    pub input_arg_type: Option<&'a DataType>,
}
#[derive(Debug)]
pub struct ArrayAggState<A: ScalarStateAllocator> {
    pub allocator: A,
    rows: ScalarVec<ScalarVec<Option<TrackedAggScalarValue<A>>, A>, A>,
    distinct_seen: ScalarSet<ScalarVec<u8, A>, DefaultHashBuilder, A>,
    row_backing_bytes: usize,
    key_backing_bytes: usize,
}
impl<A: ScalarStateAllocator> ArrayAggState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            rows: ScalarVec::new_in(allocator.clone()),
            distinct_seen: ScalarSet::with_hasher_in(
                DefaultHashBuilder::default(),
                allocator.clone(),
            ),
            allocator,
            row_backing_bytes: 0,
            key_backing_bytes: 0,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.distinct_seen.is_empty()
    }
    pub fn retained_bytes(&self) -> usize {
        self.rows.capacity() * std::mem::size_of::<ScalarVec<Option<TrackedAggScalarValue<A>>, A>>()
            + self.row_backing_bytes
            + self.distinct_seen.raw_table().allocation_info().1.size()
            + self.key_backing_bytes
    }
    fn note_row_backing(
        &mut self,
        row: &ScalarVec<Option<TrackedAggScalarValue<A>>, A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let mut retained = row.capacity() * std::mem::size_of::<Option<TrackedAggScalarValue<A>>>();
        for value in row.iter().flatten() {
            work.step()?;
            retained = retained
                .checked_add(tracked_scalar_heap_capacity(value, work)?)
                .ok_or(crate::KernelFailure::ResourceExhausted)?;
        }
        self.row_backing_bytes = self
            .row_backing_bytes
            .checked_add(retained)
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        Ok(())
    }
    pub fn clear(&mut self) {
        self.row_backing_bytes = 0;
        self.key_backing_bytes = 0;
        self.rows = ScalarVec::new_in(self.allocator.clone());
        self.distinct_seen =
            ScalarSet::with_hasher_in(DefaultHashBuilder::default(), self.allocator.clone());
    }
    fn push_row(
        &mut self,
        row: ScalarVec<Option<TrackedAggScalarValue<A>>, A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        work.flush()?;
        self.rows.try_reserve(1).map_err(|_| {
            self.allocator
                .scalar_allocation_error("reserve array_agg row")
        })?;
        work.flush()?;
        self.note_row_backing(&row, work)?;
        self.rows.push(row);
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub enum ArrayAggValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
    Struct(Vec<Option<ArrayAggValue>>),
    List(Vec<Option<ArrayAggValue>>),
}

fn is_distinct_kind(kind: &ArrayAggKind<'_>) -> bool {
    match kind {
        ArrayAggKind::Array { distinct, .. } => *distinct,
        ArrayAggKind::Unique => true,
    }
}
fn order_by_kind<'a>(kind: &ArrayAggKind<'a>) -> (&'a [bool], &'a [bool]) {
    match kind {
        ArrayAggKind::Array {
            ascending,
            nulls_first,
            ..
        } => (*ascending, *nulls_first),
        ArrayAggKind::Unique => (&[], &[]),
    }
}
pub fn to_common_scalar(
    value: &ArrayAggValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<AggScalarValue, ScalarStateError> {
    work.step()?;
    Ok(match value {
        ArrayAggValue::Bool(v) => AggScalarValue::Bool(*v),
        ArrayAggValue::Int64(v) => AggScalarValue::Int64(*v),
        ArrayAggValue::Float64(v) => AggScalarValue::Float64(*v),
        ArrayAggValue::Utf8(v) => {
            work.flush()?;
            let value = v.clone();
            work.flush()?;
            AggScalarValue::Utf8(value)
        }
        ArrayAggValue::Date32(v) => AggScalarValue::Date32(*v),
        ArrayAggValue::Timestamp(v) => AggScalarValue::Timestamp(*v),
        ArrayAggValue::Decimal128(v) => AggScalarValue::Decimal128(*v),
        ArrayAggValue::Struct(items) => AggScalarValue::Struct(
            items
                .iter()
                .map(|item| {
                    work.step()?;
                    item.as_ref().map(|v| to_common_scalar(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ArrayAggValue::List(items) => AggScalarValue::List(
            items
                .iter()
                .map(|item| {
                    work.step()?;
                    item.as_ref().map(|v| to_common_scalar(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}
pub fn distinct_key(
    value: &Option<ArrayAggValue>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, ScalarStateError> {
    work.step()?;
    Ok(match value {
        None => vec![0],
        Some(v) => {
            let mut out = vec![1];
            out.extend(key_fingerprint(&to_common_scalar(v, work)?, work)?);
            out
        }
    })
}
fn compare_optional_values(
    left: &Option<ArrayAggValue>,
    right: &Option<ArrayAggValue>,
    asc: bool,
    nulls_first: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, ScalarStateError> {
    work.step()?;
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
            let left = to_common_scalar(left, work)?;
            let right = to_common_scalar(right, work)?;
            let ord = compare_scalar_values(&left, &right, work)?;
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

fn array_value_from_tracked<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayAggValue, ScalarStateError> {
    array_value_from_common(tracked_scalar_to_output(value, work)?, work)
}

fn array_value_from_common(
    value: AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayAggValue, ScalarStateError> {
    work.step()?;
    Ok(match value {
        AggScalarValue::Bool(value) => ArrayAggValue::Bool(value),
        AggScalarValue::Int64(value) => ArrayAggValue::Int64(value),
        AggScalarValue::Float64(value) => ArrayAggValue::Float64(value),
        AggScalarValue::Utf8(value) => ArrayAggValue::Utf8(value),
        AggScalarValue::Date32(value) => ArrayAggValue::Date32(value),
        AggScalarValue::Timestamp(value) => ArrayAggValue::Timestamp(value),
        AggScalarValue::Decimal128(value) => ArrayAggValue::Decimal128(value),
        AggScalarValue::Struct(values) => ArrayAggValue::Struct(
            values
                .into_iter()
                .map(|value| {
                    work.step()?;
                    value.map(|v| array_value_from_common(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AggScalarValue::List(values) => ArrayAggValue::List(
            values
                .into_iter()
                .map(|value| {
                    work.step()?;
                    value.map(|v| array_value_from_common(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AggScalarValue::Map(entries) => ArrayAggValue::List(
            entries
                .into_iter()
                .map(|(key, value)| {
                    work.step()?;
                    Ok(Some(ArrayAggValue::Struct(vec![
                        key.map(|v| array_value_from_common(v, work)).transpose()?,
                        value
                            .map(|v| array_value_from_common(v, work))
                            .transpose()?,
                    ])))
                })
                .collect::<Result<Vec<_>, ScalarStateError>>()?,
        ),
        AggScalarValue::Decimal256(_) | AggScalarValue::Binary(_) => {
            return Err(
                ("array_agg tracked scalar type is not supported by its output ABI".to_string())
                    .into(),
            );
        }
    })
}

fn sort_rows(
    rows: &mut [Vec<Option<ArrayAggValue>>],
    is_asc_order: &[bool],
    nulls_first: &[bool],
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    let order_by_num = is_asc_order.len();
    if order_by_num == 0 {
        return Ok(());
    }
    work.flush()?;
    let mut error: Option<ScalarStateError> = None;
    rows.sort_by(|left, right| {
        if error.is_some() {
            return Ordering::Equal;
        }
        for key in 0..order_by_num {
            let col = 1 + key;
            let l = left.get(col).cloned().unwrap_or(None);
            let r = right.get(col).cloned().unwrap_or(None);
            match compare_optional_values(&l, &r, is_asc_order[key], nulls_first[key], work) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(err) => {
                    error = Some(err);
                    return Ordering::Equal;
                }
            }
        }
        Ordering::Equal
    });
    work.flush()?;
    if let Some(err) = error {
        return Err(err);
    }
    Ok(())
}

fn extract_final_values<A: ScalarStateAllocator>(
    spec: &ArrayAggConfig<'_>,
    state: &ArrayAggState<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<Option<ArrayAggValue>>, ScalarStateError> {
    let mut rows = state
        .rows
        .iter()
        .map(|row| {
            work.step()?;
            row.iter()
                .map(|value| {
                    work.step()?;
                    value
                        .as_ref()
                        .map(|v| array_value_from_tracked(v, work))
                        .transpose()
                })
                .collect::<Result<Vec<_>, ScalarStateError>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (is_asc_order, nulls_first) = order_by_kind(&spec.kind);
    sort_rows(&mut rows, is_asc_order, nulls_first, work)?;

    let distinct = is_distinct_kind(&spec.kind);
    let mut out = Vec::with_capacity(rows.len());
    let mut seen = HashSet::new();
    for row in rows {
        work.step()?;
        let value = row.first().cloned().unwrap_or(None);
        if distinct {
            let key = distinct_key(&value, work)?;
            if seen.insert(key) {
                out.push(value);
            }
        } else {
            out.push(value);
        }
    }
    Ok(out)
}

fn reconcile_field_to_field(
    expected: &Arc<Field>,
    actual: &Arc<Field>,
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Arc<Field>, ScalarStateError> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        let data_type =
            reconcile_data_type(expected.data_type(), actual.data_type(), context, work)?;
        let nullable = if expected.is_nullable() == actual.is_nullable() {
            expected.is_nullable()
        } else {
            actual.is_nullable()
        };
        if &data_type == expected.data_type() && nullable == expected.is_nullable() {
            Ok(expected.clone())
        } else {
            Ok(Arc::new(Field::new(expected.name(), data_type, nullable)))
        }
    })();
    work.flush()?;
    result
}

fn reconcile_field_to_data_type(
    expected: &Arc<Field>,
    actual: &DataType,
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Arc<Field>, ScalarStateError> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        let data_type = reconcile_data_type(expected.data_type(), actual, context, work)?;
        if &data_type == expected.data_type() {
            Ok(expected.clone())
        } else {
            Ok(Arc::new(Field::new(
                expected.name(),
                data_type,
                expected.is_nullable(),
            )))
        }
    })();
    work.flush()?;
    result
}

fn reconcile_data_type(
    expected: &DataType,
    actual: &DataType,
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<DataType, ScalarStateError> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        if expected == actual {
            return Ok(expected.clone());
        }
        match (expected, actual) {
            (DataType::Map(_, expected_ordered), DataType::Map(_, actual_ordered))
                if expected_ordered == actual_ordered =>
            {
                Ok(actual.clone())
            }
            (DataType::List(expected_item), DataType::List(actual_item)) => Ok(DataType::List(
                reconcile_field_to_field(expected_item, actual_item, context, work)?,
            )),
            (DataType::Struct(expected_fields), DataType::Struct(actual_fields))
                if expected_fields.len() == actual_fields.len() =>
            {
                let fields = expected_fields
                    .iter()
                    .zip(actual_fields.iter())
                    .map(|(expected_field, actual_field)| {
                        work.step()?;
                        reconcile_field_to_field(expected_field, actual_field, context, work)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            _ => Err((format!(
                "{context} type mismatch: expected {:?}, got {:?}",
                expected, actual
            ))
            .into()),
        }
    })();
    work.flush()?;
    result
}

fn reconcile_fields_for_columns(
    expected_fields: &Fields,
    columns: &[ArrayRef],
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Fields, ScalarStateError> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        if expected_fields.len() != columns.len() {
            return Err((format!(
                "{context} field count mismatch: expected {}, got {}",
                expected_fields.len(),
                columns.len()
            ))
            .into());
        }
        let fields = expected_fields
            .iter()
            .zip(columns.iter())
            .map(|(expected_field, column)| {
                work.step()?;
                reconcile_field_to_data_type(expected_field, column.data_type(), context, work)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Fields::from(fields))
    })();
    work.flush()?;
    result
}

fn append_value<A: ScalarStateAllocator>(
    state: &mut ArrayAggState<A>,
    value: Option<TrackedAggScalarValue<A>>,
    distinct: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    if distinct {
        let key = tracked_optional_key_fingerprint(&value, &state.allocator, work)?;
        if state.distinct_seen.contains(&key) {
            return Ok(());
        }
        let mut row = aggregate_vec_with_capacity(
            &state.allocator,
            1,
            "reserve array_agg distinct row",
            work,
        )?;
        row.push(value);
        state.rows.try_reserve(1).map_err(|_| {
            state
                .allocator
                .scalar_allocation_error("reserve array_agg row")
        })?;
        state.distinct_seen.try_reserve(1).map_err(|_| {
            state
                .allocator
                .scalar_allocation_error("reserve array_agg distinct key")
        })?;
        state.note_row_backing(&row, work)?;
        state.key_backing_bytes = state
            .key_backing_bytes
            .checked_add(key.capacity())
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        state.distinct_seen.insert(key);
        state.rows.push(row);
    } else {
        let mut row =
            aggregate_vec_with_capacity(&state.allocator, 1, "reserve array_agg row", work)?;
        row.push(value);
        state.push_row(row, work)?;
    }
    Ok(())
}

pub fn unwrap_update_value_array<'a>(
    spec: &ArrayAggConfig<'_>,
    array: &'a ArrayRef,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(&'a ArrayRef, Option<&'a StructArray>), ScalarStateError> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        let Some(wrapper) = array.as_any().downcast_ref::<StructArray>() else {
            return Ok((array, None));
        };
        let Some(expected_item_type) = spec.input_arg_type else {
            return Ok((array, None));
        };
        let first = wrapper.columns().first().ok_or_else(|| {
            "array_agg struct input must contain at least 1 field for value extraction".to_string()
        })?;
        let first_matches_expected = first.data_type() == expected_item_type
            || reconcile_data_type(
                expected_item_type,
                first.data_type(),
                "array_agg update input",
                work,
            )
            .is_ok();
        if first_matches_expected {
            Ok((first, Some(wrapper)))
        } else {
            Ok((array, None))
        }
    })();
    work.flush()?;
    result
}

fn build_scalar_array(
    output_type: &DataType,
    values: Vec<Option<ArrayAggValue>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError> {
    match output_type {
        DataType::Null => {
            let mut any = false;
            for value in &values {
                work.step()?;
                any |= value.is_some();
            }
            if any {
                work.step()?;
                return Err(("scalar output type mismatch for Null".to_string()).into());
            }
            Ok(Arc::new(NullArray::new(values.len())) as ArrayRef)
        }
        DataType::Boolean => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Bool(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Boolean".to_string()).into()),
                }
            }
            Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
        }
        DataType::Int8 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v = i8::try_from(v).map_err(|_| "int8 overflow".to_string())?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Int8".to_string()).into()),
                }
            }
            Ok(Arc::new(Int8Array::from(out)) as ArrayRef)
        }
        DataType::Int16 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v = i16::try_from(v).map_err(|_| "int16 overflow".to_string())?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Int16".to_string()).into()),
                }
            }
            Ok(Arc::new(Int16Array::from(out)) as ArrayRef)
        }
        DataType::Int32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v = i32::try_from(v).map_err(|_| "int32 overflow".to_string())?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Int32".to_string()).into()),
                }
            }
            Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
        }
        DataType::Int64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Int64".to_string()).into()),
                }
            }
            Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
        }
        DataType::Float32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Float64(v)) => out.push(Some(v as f32)),
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Float32".to_string()).into()),
                }
            }
            Ok(Arc::new(Float32Array::from(out)) as ArrayRef)
        }
        DataType::Float64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Float64(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Float64".to_string()).into()),
                }
            }
            Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Utf8(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Utf8".to_string()).into()),
                }
            }
            Ok(Arc::new(StringArray::from(out)) as ArrayRef)
        }
        DataType::Date32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Date32(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(("scalar output type mismatch for Date32".to_string()).into()),
                }
            }
            Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
        }
        DataType::Timestamp(unit, tz) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Timestamp(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err(
                            ("scalar output type mismatch for Timestamp".to_string()).into()
                        );
                    }
                }
            }
            let tz = tz.as_deref().map(|s| s.to_string());
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    let arr = TimestampSecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Millisecond => {
                    let arr = TimestampMillisecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Microsecond => {
                    let arr = TimestampMicrosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Nanosecond => {
                    let arr = TimestampNanosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
            };
            Ok(array)
        }
        DataType::Decimal128(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err(
                            ("scalar output type mismatch for Decimal128".to_string()).into()
                        );
                    }
                }
            }
            let arr = Decimal128Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err(("scalar output type mismatch for LargeInt".to_string()).into());
                    }
                }
            }
            largeint::array_from_i128_observed(&out, &mut |event| match event {
                largeint::LargeIntObservation::Step => work.step(),
                largeint::LargeIntObservation::OpaqueBoundary => work.flush(),
            })?
            .map_err(Into::into)
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(
                    ("array_agg cannot build Struct output with no fields".to_string()).into(),
                );
            }
            let mut null_builder = NullBufferBuilder::new(values.len());
            let mut by_field: Vec<Vec<Option<ArrayAggValue>>> = (0..fields.len())
                .map(|_| Vec::with_capacity(values.len()))
                .collect();
            for value in values {
                work.step()?;
                match value {
                    None => {
                        null_builder.append_null();
                        for field_values in &mut by_field {
                            work.step()?;
                            field_values.push(None);
                        }
                    }
                    Some(ArrayAggValue::Struct(items)) => {
                        if items.len() != fields.len() {
                            return Err((format!(
                                "scalar output type mismatch for Struct: expected {} fields, got {}",
                                fields.len(),
                                items.len()
                            ))
                            .into());
                        }
                        null_builder.append_non_null();
                        for (idx, item) in items.into_iter().enumerate() {
                            work.step()?;
                            by_field[idx].push(item);
                        }
                    }
                    Some(other) => {
                        return Err((format!(
                            "scalar output type mismatch for Struct: got {:?}",
                            other
                        ))
                        .into());
                    }
                }
            }
            let mut columns = Vec::with_capacity(fields.len());
            for (idx, field) in fields.iter().enumerate() {
                work.step()?;
                columns.push(build_scalar_array(
                    field.data_type(),
                    std::mem::take(&mut by_field[idx]),
                    work,
                )?);
            }
            let output_fields =
                reconcile_fields_for_columns(fields, &columns, "array_agg struct output", work)?;
            work.flush()?;
            let out = StructArray::new(output_fields, columns, null_builder.finish());
            work.flush()?;
            Ok(Arc::new(out) as ArrayRef)
        }
        DataType::Map(entries_field, ordered) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err((format!(
                    "array_agg map entries field must be Struct, got {:?}",
                    entries_field.data_type()
                ))
                .into());
            };
            if entry_fields.len() != 2 {
                return Err((format!(
                    "array_agg map entries field must contain key/value, got {} fields",
                    entry_fields.len()
                ))
                .into());
            }

            let mut key_values = Vec::new();
            let mut value_values = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            let mut null_builder = NullBufferBuilder::new(values.len());

            for value in values {
                work.step()?;
                match value {
                    None => {
                        null_builder.append_null();
                        offsets.push(current as i32);
                    }
                    Some(ArrayAggValue::List(items)) => {
                        null_builder.append_non_null();
                        for item in items {
                            work.step()?;
                            let Some(ArrayAggValue::Struct(entry)) = item else {
                                return Err(("scalar output type mismatch for Map entry"
                                    .to_string())
                                .into());
                            };
                            if entry.len() != 2 {
                                return Err((format!(
                                    "scalar output type mismatch for Map entry field count: expected 2, got {}",
                                    entry.len()
                                )).into());
                            }
                            key_values.push(entry[0].clone());
                            value_values.push(entry[1].clone());
                            current += 1;
                            if current > i32::MAX as i64 {
                                return Err(("array_agg map offset overflow".to_string()).into());
                            }
                        }
                        offsets.push(current as i32);
                    }
                    Some(other) => {
                        return Err((format!(
                            "scalar output type mismatch for Map: got {:?}",
                            other
                        ))
                        .into());
                    }
                }
            }

            let key_array = build_scalar_array(entry_fields[0].data_type(), key_values, work)?;
            let value_array = build_scalar_array(entry_fields[1].data_type(), value_values, work)?;
            let entries_fields = if key_array.null_count() > 0 && !entry_fields[0].is_nullable() {
                let mut adjusted = entry_fields.iter().cloned().collect::<Vec<_>>();
                adjusted[0] = Arc::new(Field::new(
                    entry_fields[0].name(),
                    entry_fields[0].data_type().clone(),
                    true,
                ));
                Fields::from(adjusted)
            } else {
                entry_fields.clone()
            };
            let entries =
                StructArray::new(entries_fields.clone(), vec![key_array, value_array], None);
            let entries_field = Arc::new(Field::new(
                entries_field.name(),
                DataType::Struct(entries_fields),
                entries_field.is_nullable(),
            ));
            let out = MapArray::new(
                entries_field,
                OffsetBuffer::new(offsets.into()),
                entries,
                null_builder.finish(),
                *ordered,
            );
            Ok(Arc::new(out) as ArrayRef)
        }
        DataType::List(field) => {
            let mut flattened = Vec::<Option<ArrayAggValue>>::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            let mut null_builder = NullBufferBuilder::new(values.len());
            for value in values {
                work.step()?;
                match value {
                    None => {
                        null_builder.append_null();
                        offsets.push(current as i32);
                    }
                    Some(ArrayAggValue::List(items)) => {
                        current += items.len() as i64;
                        if current > i32::MAX as i64 {
                            return Err(("array_agg offset overflow".to_string()).into());
                        }
                        flattened.extend(items);
                        null_builder.append_non_null();
                        offsets.push(current as i32);
                    }
                    Some(other) => {
                        return Err((format!(
                            "scalar output type mismatch for List: got {:?}",
                            other
                        ))
                        .into());
                    }
                }
            }
            let child_values = build_scalar_array(field.data_type(), flattened, work)?;
            let list_field = reconcile_field_to_data_type(
                field,
                child_values.data_type(),
                "array_agg list item",
                work,
            )?;
            let out = ListArray::new(
                list_field,
                OffsetBuffer::new(offsets.into()),
                child_values,
                null_builder.finish(),
            );
            Ok(Arc::new(out) as ArrayRef)
        }
        other => Err((format!("unsupported scalar output type: {:?}", other)).into()),
    }
}

pub fn merge_input_list_array(
    array: &ArrayRef,
) -> Result<(&ListArray, Option<&StructArray>), ScalarStateError> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok((list, None));
    }
    if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
        if struct_arr.num_columns() < 1 {
            return Err((format!(
                "array_agg merge struct input expects at least 1 field, got {}",
                struct_arr.num_columns()
            ))
            .into());
        }
        let list = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "array_agg merge struct field[0] must be ListArray".to_string())?;
        return Ok((list, Some(struct_arr)));
    }
    Err(("array_agg merge input must be ListArray or StructArray".to_string()).into())
}
/// Exact arena-free row computation. Each address came from an evaluated carrier.
pub fn update_row<
    'a,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = (&'a ArrayRef, usize)>,
>(
    state: &mut ArrayAggState<A>,
    kind: ArrayAggKind<'_>,
    mut columns: I,
    wrapped: bool,
    parent_null: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    if !wrapped {
        let (array, row) = columns
            .next()
            .ok_or_else(|| ScalarStateError::from("array_agg input missing".to_string()))?;
        if matches!(kind, ArrayAggKind::Unique)
            && let Some(list) = array.as_any().downcast_ref::<ListArray>()
        {
            if list.is_null(row) {
                return Ok(());
            }
            let values = list.values();
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            for idx in start..end {
                work.step()?;
                let value = tracked_scalar_from_array(values, idx, &state.allocator, work)?;
                append_value(state, value, true, work)?;
            }
            return Ok(());
        }
        let value = tracked_scalar_from_array(array, row, &state.allocator, work)?;
        return append_value(state, value, false, work);
    }
    let mut row_values = aggregate_vec_with_capacity(
        &state.allocator,
        columns.len(),
        "reserve array_agg input row",
        work,
    )?;
    for (col, row) in columns {
        work.step()?;
        if parent_null {
            row_values.push(None)
        } else {
            row_values.push(tracked_scalar_from_array(col, row, &state.allocator, work)?)
        }
    }
    state.push_row(row_values, work)
}
pub fn merge_row<A: ScalarStateAllocator>(
    state: &mut ArrayAggState<A>,
    kind: ArrayAggKind<'_>,
    array: &ArrayRef,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    let (first_list, struct_arr) = merge_input_list_array(array)?;
    let first_values = first_list.values();
    let first_offsets = first_list.value_offsets();
    work.step()?;
    if struct_arr.map(|s| s.is_null(row)).unwrap_or(false) || first_list.is_null(row) {
        return Ok(());
    }
    let start = first_offsets[row] as usize;
    let end = first_offsets[row + 1] as usize;
    for idx in start..end {
        work.step()?;
        if matches!(kind, ArrayAggKind::Unique) {
            let value = tracked_scalar_from_array(first_values, idx, &state.allocator, work)?;
            append_value(state, value, true, work)?;
            continue;
        }
        if let Some(struct_arr) = struct_arr {
            let mut row_values = aggregate_vec_with_capacity(
                &state.allocator,
                struct_arr.num_columns(),
                "reserve array_agg merge row",
                work,
            )?;
            for (col_idx, col) in struct_arr.columns().iter().enumerate() {
                work.step()?;
                let list = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    format!(
                        "array_agg merge struct field[{}] must be ListArray",
                        col_idx
                    )
                })?;
                if list.is_null(row) {
                    row_values.push(None);
                    continue;
                }
                let offsets = list.value_offsets();
                if offsets[row] != first_offsets[row] || offsets[row + 1] != first_offsets[row + 1]
                {
                    return Err(format!(
                        "array_agg merge struct field[{}] offsets mismatch",
                        col_idx
                    )
                    .into());
                }
                row_values.push(tracked_scalar_from_array(
                    list.values(),
                    idx,
                    &state.allocator,
                    work,
                )?);
            }
            state.push_row(row_values, work)?;
        } else {
            let value = tracked_scalar_from_array(first_values, idx, &state.allocator, work)?;
            append_value(state, value, false, work)?;
        }
    }
    Ok(())
}

pub fn build_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s ArrayAggState<A>>,
>(
    spec: &ArrayAggConfig<'_>,
    states: I,
    output_intermediate: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError> {
    let target_type = if output_intermediate {
        &spec.intermediate_type
    } else {
        &spec.output_type
    };
    match target_type {
        DataType::List(list_field) => {
            let mut flattened = Vec::<Option<ArrayAggValue>>::new();
            let mut out_offsets = Vec::with_capacity(states.len() + 1);
            out_offsets.push(0_i32);
            let mut current: i64 = 0;
            for state in states {
                work.step()?;
                let values = if output_intermediate {
                    state
                        .rows
                        .iter()
                        .map(|row| {
                            work.step()?;
                            row.first()
                                .and_then(Option::as_ref)
                                .map(|v| array_value_from_tracked(v, work))
                                .transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    extract_final_values(spec, state, work)?
                };
                current += values.len() as i64;
                if current > i32::MAX as i64 {
                    return Err(("array_agg offset overflow".to_string()).into());
                }
                out_offsets.push(current as i32);
                flattened.extend(values);
            }
            let values = build_scalar_array(list_field.data_type(), flattened, work)?;
            let output_list_field = reconcile_field_to_data_type(
                list_field,
                values.data_type(),
                "array_agg output list item",
                work,
            )?;
            let list_out = ListArray::new(
                output_list_field,
                OffsetBuffer::new(out_offsets.into()),
                values,
                None,
            );
            Ok(Arc::new(list_out) as ArrayRef)
        }
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                "array_agg struct target type must contain at least 1 field".to_string()
            })?;
            let DataType::List(first_list_field) = first.data_type() else {
                return Err((format!(
                    "array_agg struct field[0] must be List, got {:?}",
                    first.data_type()
                ))
                .into());
            };
            let mut flattened_by_col: Vec<Vec<Option<ArrayAggValue>>> =
                (0..fields.len()).map(|_| Vec::new()).collect();
            let mut out_offsets = Vec::with_capacity(states.len() + 1);
            out_offsets.push(0_i32);
            let mut current: i64 = 0;

            for state in states {
                work.step()?;
                let rows = if output_intermediate {
                    state
                        .rows
                        .iter()
                        .map(|row| {
                            work.step()?;
                            row.iter()
                                .map(|value| {
                                    work.step()?;
                                    value
                                        .as_ref()
                                        .map(|v| array_value_from_tracked(v, work))
                                        .transpose()
                                })
                                .collect::<Result<Vec<_>, ScalarStateError>>()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    extract_final_values(spec, state, work)?
                        .into_iter()
                        .map(|value| vec![value])
                        .collect()
                };
                current += rows.len() as i64;
                if current > i32::MAX as i64 {
                    return Err(("array_agg offset overflow".to_string()).into());
                }
                out_offsets.push(current as i32);
                for row in rows {
                    work.step()?;
                    for (col_idx, values) in flattened_by_col.iter_mut().enumerate() {
                        work.step()?;
                        values.push(row.get(col_idx).cloned().unwrap_or(None));
                    }
                }
            }

            let mut columns = Vec::with_capacity(fields.len());
            for (idx, field) in fields.iter().enumerate() {
                work.step()?;
                let DataType::List(list_field) = field.data_type() else {
                    return Err((format!(
                        "array_agg struct field[{}] must be List, got {:?}",
                        idx,
                        field.data_type()
                    ))
                    .into());
                };
                let values = if idx == 0 {
                    build_scalar_array(
                        first_list_field.data_type(),
                        flattened_by_col[idx].clone(),
                        work,
                    )?
                } else {
                    build_scalar_array(list_field.data_type(), flattened_by_col[idx].clone(), work)?
                };
                let output_list_field = reconcile_field_to_data_type(
                    list_field,
                    values.data_type(),
                    "array_agg intermediate list item",
                    work,
                )?;
                let list = ListArray::new(
                    output_list_field,
                    OffsetBuffer::new(out_offsets.clone().into()),
                    values,
                    None,
                );
                columns.push(Arc::new(list) as ArrayRef);
            }

            let output_fields = reconcile_fields_for_columns(
                fields,
                &columns,
                "array_agg intermediate struct output",
                work,
            )?;
            Ok(Arc::new(StructArray::new(output_fields, columns, None)) as ArrayRef)
        }
        other => Err((format!(
            "array_agg target type must be List or Struct(List...), got {:?}",
            other
        ))
        .into()),
    }
}
