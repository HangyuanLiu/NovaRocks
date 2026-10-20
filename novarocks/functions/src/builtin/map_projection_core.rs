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
    Domain(MapProjectionFailure),
    Control(E),
}

#[derive(Debug)]
pub enum MapProjectionFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    UnsupportedKey(novarocks_failure_contract::BoundedDetail),
    LargeIntWidth(std::array::TryFromSliceError),
    Index {
        index: usize,
        source: std::num::TryFromIntError,
    },
    Offsets {
        required: u128,
        minimum_unit: bool,
    },
    Backing {
        required: u128,
    },
    Arrow(arrow_schema::ArrowError),
}
impl MapProjectionFailure {
    fn contract(detail: impl std::fmt::Display) -> Self {
        Self::Contract(novarocks_failure_contract::BoundedDetail::truncating_display(detail))
    }
}
impl std::fmt::Display for MapProjectionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) | Self::UnsupportedKey(detail) => f.write_str(detail.as_str()),
            Self::LargeIntWidth(cause) => write!(f, "map key LARGEINT decode failed: {cause}"),
            Self::Index { index, .. } => {
                write!(f, "map entry index {index} exceeds u32 take addressing")
            }
            Self::Offsets {
                required,
                minimum_unit,
            } => write!(
                f,
                "map {} requires {required} child positions, i32 offset maximum {}",
                if *minimum_unit { "row" } else { "output" },
                i32::MAX
            ),
            Self::Backing { required } => write!(f, "map offsets require {required} backing bytes"),
            Self::Arrow(source) => write!(f, "map projection Arrow operation: {source}"),
        }
    }
}
impl std::error::Error for MapProjectionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LargeIntWidth(source) => Some(source),
            Self::Index { source, .. } => Some(source),
            Self::Arrow(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for MapProjectionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, ResourceDimension, ResourceFacts, ResourceScope, domain,
        };
        if matches!(self, Self::UnsupportedKey(_)) {
            return crate::DataFailureReason::InvalidValue.failure_fact();
        }
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 225),
            Self::UnsupportedKey(_) => unreachable!("data returned above"),
            Self::LargeIntWidth(_) => (FailureClass::Invariant, 226),
            Self::Index { .. }
            | Self::Offsets { .. }
            | Self::Arrow(ArrowError::OffsetOverflowError(_)) => {
                (FailureClass::Representation, 227)
            }
            Self::Backing { .. } => (FailureClass::Representation, 401),
            Self::Arrow(ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_)) => {
                (FailureClass::Resource, 228)
            }
            Self::Arrow(ArrowError::IoError(..)) => (FailureClass::Unexpected, 229),
            Self::Arrow(_) => (FailureClass::Invariant, 230),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        match self {
            Self::Offsets {
                required,
                minimum_unit,
            } => fact.with_representation(RepresentationFacts {
                boundary: if *minimum_unit {
                    RepresentationBoundary::MinimumUnit
                } else {
                    RepresentationBoundary::SingleOutput
                },
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(36).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(i32::MAX as u64),
                representable_prefix: None,
            }),
            Self::Index { index, .. } => fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(37).unwrap(),
                },
                required: u64::try_from(*index as u128 + 1).ok(),
                extent_limit: Some(u32::MAX as u64 + 1),
                representable_prefix: None,
            }),
            Self::Arrow(ArrowError::OffsetOverflowError(_)) => {
                fact.with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: std::num::NonZeroU16::new(36).unwrap(),
                    },
                    required: None,
                    extent_limit: None,
                    representable_prefix: None,
                })
            }
            Self::Backing { required } => {
                crate::allocation_failure::rust_array_backing_failure_fact(
                    fact.reason(),
                    *required,
                    RepresentationBoundary::SingleOutput,
                )
            }
            _ => fact,
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for MapProjectionFailure {}
fn checked_output_extent(current: usize, incoming: usize) -> Result<(), MapProjectionFailure> {
    let required = current as u128 + incoming as u128;
    if required > i32::MAX as u128 {
        return Err(MapProjectionFailure::Offsets {
            required: if incoming > i32::MAX as usize {
                incoming as u128
            } else {
                required
            },
            minimum_unit: incoming > i32::MAX as usize,
        });
    }
    Ok(())
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
) -> Result<(OffsetBuffer<i32>, Vec<u32>), MapProjectionFailure> {
    sort_observed(
        map,
        crate::Selection::all(map.len()),
        |_, row| Ok::<_, std::convert::Infallible>(row),
        None,
        |_, _, _| Ok(()),
        &mut |_| Ok(()),
    )
    .map_err(|e| match e {
        ProjectionFailure::Domain(cause) => cause,
        ProjectionFailure::Control(never) => match never {},
    })
}
pub fn sort_observed<E>(
    map: &MapArray,
    selection: crate::Selection<'_>,
    mut row: impl FnMut(usize, usize) -> Result<usize, E>,
    mut row_error: Option<&mut dyn FnMut(usize, MapProjectionFailure) -> Result<(), E>>,
    mut outcome: impl FnMut(usize, usize, bool) -> Result<(), E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<(OffsetBuffer<i32>, Vec<u32>), ProjectionFailure<E>> {
    let offsets = map.value_offsets();
    let keys = map.keys();
    let backing = (selection.len() as u128 + 1) * std::mem::size_of::<i32>() as u128;
    if backing > isize::MAX as u128 {
        return Err(ProjectionFailure::Domain(MapProjectionFailure::Backing {
            required: backing,
        }));
    }
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
            // Check the exact next output before allocating or appending its row.
            checked_output_extent(sorted_indices.len(), end - start)
                .map_err(ProjectionFailure::Domain)?;
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            let mut row_indices: Vec<usize> = (start..end).collect();
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            sort_indices_by_key_observed(keys, &mut row_indices, observer)?;
            for idx in row_indices {
                observe(observer, CollectionObservation::Step)?;
                let idx_u32 = u32::try_from(idx).map_err(|source| {
                    ProjectionFailure::Domain(MapProjectionFailure::Index { index: idx, source })
                })?;
                sorted_indices.push(idx_u32);
            }
            let next = sorted_indices.len() as i32; // checked before appending
            Ok(next)
        })();
        match result {
            Ok(next) => {
                sorted_offsets.push(next);
                outcome(ordinal, row, false).map_err(ProjectionFailure::Control)?;
            }
            Err(ProjectionFailure::Domain(cause @ MapProjectionFailure::UnsupportedKey(_)))
                if row_error.is_some() =>
            {
                sorted_indices.truncate(first);
                row_error.as_mut().unwrap()(ordinal, cause).map_err(ProjectionFailure::Control)?;
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
                .map_err(ProjectionFailure::Domain)?;
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
fn compare_keys_ordered(
    keys: &ArrayRef,
    left: usize,
    right: usize,
) -> Result<Ordering, MapProjectionFailure> {
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
                .ok_or_else(|| {
                    MapProjectionFailure::contract(
                        "failed to downcast map key to FixedSizeBinaryArray",
                    )
                })?;
            let left_value = i128::from_be_bytes(
                arr.value(left)
                    .try_into()
                    .map_err(MapProjectionFailure::LargeIntWidth)?,
            );
            let right_value = i128::from_be_bytes(
                arr.value(right)
                    .try_into()
                    .map_err(MapProjectionFailure::LargeIntWidth)?,
            );
            Ok(left_value.cmp(&right_value))
        }
        other => Err(MapProjectionFailure::UnsupportedKey(
            novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                "map key ordered compare unsupported type: {other:?}"
            )),
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
    row_error: Option<&mut dyn FnMut(usize, MapProjectionFailure) -> Result<(), E>>,
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
        .map_err(|cause| ProjectionFailure::Domain(MapProjectionFailure::Arrow(cause)))?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let list = ListArray::try_new(field, sorted_offsets, sorted, nulls())
        .map_err(|cause| ProjectionFailure::Domain(MapProjectionFailure::Arrow(cause)))?;
    let out = Arc::new(list) as ArrayRef;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn map_projection_extent_refuses_before_append_with_exact_boundary() {
        checked_output_extent(i32::MAX as usize - 1, 1).unwrap();
        let batch = checked_output_extent(i32::MAX as usize, 1)
            .unwrap_err()
            .failure_fact();
        assert_eq!(batch.class(), FailureClass::Representation);
        assert_eq!(
            batch.representation().unwrap().boundary,
            RepresentationBoundary::SingleOutput
        );
        assert_eq!(
            batch.representation().unwrap().required,
            Some(i32::MAX as u64 + 1)
        );
        let single = checked_output_extent(0, i32::MAX as usize + 1)
            .unwrap_err()
            .failure_fact();
        assert_eq!(
            single.representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
    }
    #[test]
    fn map_projection_preserves_actual_arrow_source_without_payload_owner() {
        use std::error::Error;
        let cause = MapProjectionFailure::Arrow(arrow_schema::ArrowError::MemoryError(
            "CAST numeric cancelled".into(),
        ));
        assert_eq!(cause.failure_fact().class(), FailureClass::Resource);
        assert!(cause.source().unwrap().is::<arrow_schema::ArrowError>());
        let values: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![None]));
        let weak = Arc::downgrade(&values);
        let error = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            OffsetBuffer::new(vec![0, 1].into()),
            values,
            None,
        )
        .unwrap_err();
        let retained = MapProjectionFailure::Arrow(error);
        assert!(weak.upgrade().is_none());
        assert_eq!(retained.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(cause.failure_fact().class(), FailureClass::Resource);
    }
}
