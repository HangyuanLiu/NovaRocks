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
//! Original map stable insertion sorting and key/value List construction author.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, MapArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};

use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use std::cmp::Ordering;
use std::sync::Arc;

use crate::largeint;

use super::array_literal_core::CollectionObservation;
use arrow_array::{ListArray, UInt32Array};
use arrow_buffer::NullBuffer;
use arrow_select::take::take;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MapPart {
    Keys,
    Values,
}
#[derive(Debug)]
pub enum ProjectionFailure<E> {
    Data(String),
    Take(String),
    Control(E),
}
fn observe<E>(
    o: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), ProjectionFailure<E>> {
    o(event).map_err(ProjectionFailure::Control)
}
pub fn part_array(map: &MapArray, part: MapPart) -> &ArrayRef {
    match part {
        MapPart::Keys => map.keys(),
        MapPart::Values => map.values(),
    }
}
pub fn sorted_map_offsets_and_indices(
    map: &MapArray,
) -> Result<(OffsetBuffer<i32>, Vec<u32>), String> {
    sort_observed(
        map,
        crate::Selection::all(map.len()),
        |_, row| Ok::<_, String>(row),
        None,
        |_, _, _| Ok(()),
        &mut |_| Ok(()),
    )
    .map_err(|e| match e {
        ProjectionFailure::Data(s) | ProjectionFailure::Take(s) | ProjectionFailure::Control(s) => {
            s
        }
    })
}
pub fn sort_observed<E>(
    map: &MapArray,
    selection: crate::Selection<'_>,
    mut row: impl FnMut(usize, usize) -> Result<usize, E>,
    mut row_error: Option<&mut dyn FnMut(usize, String) -> Result<(), E>>,
    mut outcome: impl FnMut(usize, usize, bool) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<(OffsetBuffer<i32>, Vec<u32>), ProjectionFailure<E>> {
    let offsets = map.value_offsets();
    let keys = map.keys();
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut sorted_indices: Vec<u32> = Vec::new();
    let mut sorted_offsets: Vec<i32> = Vec::with_capacity(selection.len() + 1);
    sorted_offsets.push(0);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let row = row(ordinal, batch_row).map_err(ProjectionFailure::Control)?;
        let current = *sorted_offsets.last().unwrap_or(&0);
        if map.is_null(row) {
            sorted_offsets.push(current);
            outcome(ordinal, row, true).map_err(ProjectionFailure::Control)?;
            continue;
        }
        let first = sorted_indices.len();
        let result = (|| {
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            let mut row_indices: Vec<usize> = (start..end).collect();
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            sort_indices_by_key_observed(keys, &mut row_indices, observer)?;
            for idx in row_indices {
                observe(observer, CollectionObservation::Step)?;
                let idx_u32 = u32::try_from(idx).map_err(|_| {
                    ProjectionFailure::Data(format!(
                        "map entry index overflow while sorting: {}",
                        idx
                    ))
                })?;
                sorted_indices.push(idx_u32);
            }
            let next = i32::try_from(sorted_indices.len()).map_err(|_| {
                ProjectionFailure::Data("map offset overflow while sorting entries".to_string())
            })?;
            Ok(next)
        })();
        match result {
            Ok(next) => {
                sorted_offsets.push(next);
                outcome(ordinal, row, false).map_err(ProjectionFailure::Control)?;
            }
            Err(ProjectionFailure::Data(text)) if row_error.is_some() => {
                sorted_indices.truncate(first);
                row_error.as_mut().unwrap()(ordinal, text).map_err(ProjectionFailure::Control)?;
                sorted_offsets.push(current);
                outcome(ordinal, row, true).map_err(ProjectionFailure::Control)?;
            }
            Err(other) => return Err(other),
        }
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let result = (OffsetBuffer::new(sorted_offsets.into()), sorted_indices);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(result)
}
fn sort_indices_by_key_observed<E>(
    keys: &ArrayRef,
    indices: &mut [usize],
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<(), ProjectionFailure<E>> {
    for i in 1..indices.len() {
        observe(observer, CollectionObservation::Step)?;
        let mut j = i;
        while j > 0 {
            observe(observer, CollectionObservation::Step)?;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            let ord = compare_keys_ordered(keys, indices[j - 1], indices[j])
                .map_err(ProjectionFailure::Data)?;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            if ord.is_gt() {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}
fn compare_keys_ordered(keys: &ArrayRef, left: usize, right: usize) -> Result<Ordering, String> {
    match (keys.is_null(left), keys.is_null(right)) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Less),
        (false, true) => return Ok(Ordering::Greater),
        (false, false) => {}
    }

    match keys.data_type() {
        DataType::Int8 => {
            let arr = keys.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int16 => {
            let arr = keys.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int32 => {
            let arr = keys.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int64 => {
            let arr = keys.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Float32 => {
            let arr = keys.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr
                .value(left)
                .partial_cmp(&arr.value(right))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Float64 => {
            let arr = keys.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr
                .value(left)
                .partial_cmp(&arr.value(right))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Boolean => {
            let arr = keys.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Utf8 => {
            let arr = keys.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(left).cmp(arr.value(right)))
        }
        DataType::Date32 => {
            let arr = keys.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Decimal128(_, _) => {
            let arr = keys.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Decimal256(_, _) => {
            let arr = keys.as_any().downcast_ref::<Decimal256Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = keys
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast map key to FixedSizeBinaryArray".to_string())?;
            let left_value = largeint::i128_from_be_bytes(arr.value(left))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            let right_value = largeint::i128_from_be_bytes(arr.value(right))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            Ok(left_value.cmp(&right_value))
        }
        other => Err(format!(
            "map key ordered compare unsupported type: {:?}",
            other
        )),
    }
}

/// Frozen field and NULL projection are real caller facts, not default templates.
/// The legacy shell passes map.nulls().clone; selected supplies compact row outcomes.
pub fn project_observed<E>(
    map: &MapArray,
    part: MapPart,
    field: Arc<Field>,
    selection: crate::Selection<'_>,
    row: impl FnMut(usize, usize) -> Result<usize, E>,
    row_error: Option<&mut dyn FnMut(usize, String) -> Result<(), E>>,
    outcome: impl FnMut(usize, usize, bool) -> Result<(), E>,
    nulls: impl FnOnce() -> Option<NullBuffer>,
    mut before_take: impl FnMut(&dyn Array, &[u32]) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, ProjectionFailure<E>> {
    let (sorted_offsets, sorted_indices) =
        sort_observed(map, selection, row, row_error, outcome, observer)?;
    let source = part_array(map, part);
    before_take(source.as_ref(), &sorted_indices).map_err(ProjectionFailure::Control)?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let sorted_indices = UInt32Array::from(sorted_indices);
    let sorted = take(source.as_ref(), &sorted_indices, None)
        .map_err(|e| ProjectionFailure::Take(e.to_string()))?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let list = ListArray::new(field, sorted_offsets, sorted, nulls());
    let out = Arc::new(list) as ArrayRef;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(out)
}
