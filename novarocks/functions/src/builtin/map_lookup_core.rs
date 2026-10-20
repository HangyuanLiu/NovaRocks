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
//! Original map key equality, reverse search, take and NULL repair author.
use super::array_literal_core::CollectionObservation;
use super::collection_arrow::{ArrowSite, arrow_failure, cast_output, contract};
use crate::{KernelFailure, largeint};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, MapArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
};
use arrow_schema::DataType;
use arrow_select::take::take;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};

/// This existing frozen Map has an uninhabited key. Its value type remains
/// independent and complete, including nominal domains and nullability. It
/// cannot contain an entry without violating the ordinary non-null key rule.
/// This is a complete-type fact, independent of literal origin or row bytes.
pub fn has_uninhabited_map_key(value: &crate::FunctionValueType) -> bool {
    use novarocks_type_contract::{ValueKind, ValueLogicalType};
    if value.logical_type() != ValueLogicalType::Physical
        || !has_uninhabited_map_key_carrier(value.data_type())
    {
        return false;
    }
    let ValueKind::Map { key, .. } = value.value_type().kind() else {
        return false;
    };
    !key.nullable() && matches!(key.kind(), ValueKind::Scalar(DataType::Null))
}

/// Signature-carrier constraint only; executable selection also checks the
/// complete semantic declaration through `has_uninhabited_map_key`.
pub(super) fn has_uninhabited_map_key_carrier(value: &DataType) -> bool {
    let DataType::Map(entries, _) = value else {
        return false;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return false;
    };
    fields.len() == 2
        && !entries.is_nullable()
        && fields[0].data_type() == &DataType::Null
        && !fields[0].is_nullable()
}

pub fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}
#[derive(Debug)]
pub enum MapLookupFailure<E> {
    Operation(KernelFailure),
    Control(E),
}
pub fn compare_keys_at(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_idx: usize,
) -> Result<bool, KeyComparisonFailure> {
    if key_idx >= keys.len() || target_idx >= targets.len() {
        return Err(contract("map key comparison is outside its physical carriers").into());
    }
    if keys.is_null(key_idx) || targets.is_null(target_idx) {
        return Ok(false);
    }
    if keys.data_type() != targets.data_type() {
        return Err(contract(format_args!(
            "map key type mismatch: {:?} vs {:?}",
            keys.data_type(),
            targets.data_type()
        ))
        .into());
    }

    match keys.data_type() {
        DataType::Int8 => {
            let l = keys.as_any().downcast_ref::<Int8Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int16 => {
            let l = keys.as_any().downcast_ref::<Int16Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int32 => {
            let l = keys.as_any().downcast_ref::<Int32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int64 => {
            let l = keys.as_any().downcast_ref::<Int64Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Float32 => {
            let l = keys.as_any().downcast_ref::<Float32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Float64 => {
            let l = keys.as_any().downcast_ref::<Float64Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Boolean => {
            let l = keys.as_any().downcast_ref::<BooleanArray>().unwrap();
            let r = targets.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Utf8 => {
            let l = keys.as_any().downcast_ref::<StringArray>().unwrap();
            let r = targets.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Date32 => {
            let l = keys.as_any().downcast_ref::<Date32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Decimal128(_, _) => {
            let l = keys.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Decimal256(_, _) => {
            let l = keys.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Decimal256Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let l = keys
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| contract("failed to downcast key to FixedSizeBinaryArray"))?;
            let r = targets
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| contract("failed to downcast target to FixedSizeBinaryArray"))?;
            let lv = largeint::i128_from_be_bytes(l.value(key_idx))
                .map_err(|source| KernelFailure::from_domain(LookupLargeIntFailure(source)))?;
            let rv = largeint::i128_from_be_bytes(r.value(target_idx))
                .map_err(|source| KernelFailure::from_domain(LookupLargeIntFailure(source)))?;
            Ok(lv == rv)
        }
        other => Err(KeyComparisonFailure::Row(
            LookupRowFailure::UnsupportedKeyDomain(BoundedDetail::truncating_display(
                format_args!("map key compare unsupported type: {:?}", other),
            )),
        )),
    }
}

pub struct MapLookupInputs<'a> {
    pub map: &'a MapArray,
    key_arr: &'a ArrayRef,
    check_arr: Option<&'a BooleanArray>,
}
impl<'a> MapLookupInputs<'a> {
    /// Original carrier checks, in their original order after all child evals.
    pub fn new(
        map_arr: &'a ArrayRef,
        key_arr: &'a ArrayRef,
        check_arr: Option<&'a ArrayRef>,
    ) -> Result<Self, KernelFailure> {
        let map = map_arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            contract(format_args!(
                "element_at expects MapArray, got {:?}",
                map_arr.data_type()
            ))
        })?;
        let check_arr = check_arr
            .map(|a| {
                a.as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| contract("element_at check flag must be BOOLEAN"))
            })
            .transpose()?;
        Ok(Self {
            map,
            key_arr,
            check_arr,
        })
    }
    /// This is the v1 array-layout contract, not a guess about selected mappings.
    pub fn validate_direct_lengths(&self) -> Result<(), KernelFailure> {
        if self.key_arr.len() != 1 && self.key_arr.len() != self.map.len() {
            return Err(contract(format_args!(
                "element_at key length mismatch: map rows={}, key rows={}",
                self.map.len(),
                self.key_arr.len()
            )));
        }
        if let Some(flags) = self.check_arr
            && flags.len() != 1
            && flags.len() != self.map.len()
        {
            return Err(contract(format_args!(
                "element_at check flag length mismatch: map rows={}, check rows={}",
                self.map.len(),
                flags.len()
            )));
        }
        Ok(())
    }
    pub fn direct_rows(&self, row: usize) -> LookupRows {
        LookupRows {
            map: row,
            key: row_index(row, self.key_arr.len()),
            check: self.check_arr.map(|flags| row_index(row, flags.len())),
        }
    }
}
#[derive(Clone, Copy)]
pub struct LookupRows {
    pub map: usize,
    pub key: usize,
    pub check: Option<usize>,
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), MapLookupFailure<E>> {
    observer(event).map_err(MapLookupFailure::Control)
}
/// Each caller supplies its real argument mapping, never a type-derived default.
pub fn lookup_observed<E>(
    inputs: &MapLookupInputs<'_>,
    selection: crate::Selection<'_>,
    mut rows: impl FnMut(usize, usize) -> Result<LookupRows, E>,
    output_type: Option<&DataType>,
    mut before_take: impl FnMut(&dyn Array, &UInt32Array) -> Result<(), E>,
    mut row_error: impl FnMut(usize, LookupRowFailure) -> Result<(), KernelFailure>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MapLookupFailure<E>> {
    let map = inputs.map;
    let key_arr = inputs.key_arr;
    let check_arr = inputs.check_arr;
    let keys = map.keys();
    let values = map.values();
    let offsets = map.value_offsets();
    if values.is_empty() {
        for (ordinal, row) in selection.iter().enumerate() {
            observe(observer, CollectionObservation::Step)?;
            let row = rows(ordinal, row).map_err(MapLookupFailure::Control)?;
            validate_address(inputs, row).map_err(MapLookupFailure::Operation)?;
            if let Some(flags) = check_arr {
                let check_idx = row.check.expect("validated flag address");
                let strict = !flags.is_null(check_idx) && flags.value(check_idx);
                if strict && !map.is_null(row.map) {
                    row_error(ordinal, LookupRowFailure::MissingKey)
                        .map_err(MapLookupFailure::Operation)?;
                }
            }
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let out = arrow_array::new_null_array(values.data_type(), selection.len());
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let out =
            cast_output(out, output_type, "element_at").map_err(MapLookupFailure::Operation)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        return Ok(out);
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    super::string_failure::array_layout::<Option<u32>>(selection.len())
        .map_err(MapLookupFailure::Operation)?;
    let mut indices = Vec::new();
    indices
        .try_reserve_exact(selection.len())
        .map_err(|source| {
            MapLookupFailure::Operation(super::string_failure::reservation::<Option<u32>>(
                selection.len(),
                source,
            ))
        })?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    'rows: for (ordinal, row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let row = rows(ordinal, row).map_err(MapLookupFailure::Control)?;
        validate_address(inputs, row).map_err(MapLookupFailure::Operation)?;
        let key_idx = row.key;
        let check_idx = row.check;
        let check_out_of_bounds = check_idx
            .map(|idx| {
                let flags = check_arr.expect("check_arr exists when check_idx exists");
                !flags.is_null(idx) && flags.value(idx)
            })
            .unwrap_or(false);
        if map.is_null(row.map) {
            indices.push(None);
            continue;
        }
        let start = offsets[row.map] as usize;
        let end = offsets[row.map + 1] as usize;
        let mut found = None;
        if key_arr.is_null(key_idx) {
            for i in (start..end).rev() {
                observe(observer, CollectionObservation::Step)?;
                if keys.is_null(i) {
                    found = Some(i as u32);
                    break;
                }
            }
        } else {
            for i in (start..end).rev() {
                observe(observer, CollectionObservation::Step)?;
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                let comparison = match compare_keys_at(keys, i, key_arr, key_idx) {
                    Ok(value) => Ok(value),
                    Err(KeyComparisonFailure::Row(cause)) => Err(cause),
                    Err(KeyComparisonFailure::Operation(cause)) => {
                        return Err(MapLookupFailure::Operation(cause));
                    }
                };
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                match comparison {
                    Ok(true) => {
                        found = Some(i as u32);
                        break;
                    }
                    Ok(false) => {}
                    Err(cause) => {
                        row_error(ordinal, cause).map_err(MapLookupFailure::Operation)?;
                        indices.push(None);
                        continue 'rows;
                    }
                }
            }
        }
        if found.is_none() && check_out_of_bounds {
            row_error(ordinal, LookupRowFailure::MissingKey)
                .map_err(MapLookupFailure::Operation)?;
        }
        if let Some(v) = found {
            indices.push(Some(v));
        } else {
            indices.push(None);
        }
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let indices = UInt32Array::from(indices);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    before_take(values.as_ref(), &indices).map_err(MapLookupFailure::Control)?;
    let out = take(values.as_ref(), &indices, None).map_err(|source| {
        MapLookupFailure::Operation(arrow_failure("element_at", ArrowSite::Take, source))
    })?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = apply_indices_nulls(&out, &indices);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = cast_output(out, output_type, "element_at").map_err(MapLookupFailure::Operation)?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(out)
}
fn apply_indices_nulls(out: &ArrayRef, indices: &UInt32Array) -> ArrayRef {
    // NullArray already represents NULL at every position and cannot carry a
    // physical validity buffer. The lookup still ran before this storage repair.
    if out.data_type() == &DataType::Null {
        return out.clone();
    }
    let Some(idx_nulls) = indices.nulls() else {
        return out.clone();
    };
    if idx_nulls.null_count() == 0 {
        return out.clone();
    }
    let combined_nulls = match out.nulls() {
        Some(existing) => arrow_buffer::NullBuffer::union(Some(existing), Some(idx_nulls))
            .expect("combined null buffer is always Some"),
        None => idx_nulls.clone(),
    };
    let data = out.to_data().into_builder().nulls(Some(combined_nulls));
    arrow_array::make_array(unsafe { data.build_unchecked() })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LookupRowFailure {
    MissingKey,
    UnsupportedKeyDomain(BoundedDetail),
}
impl LookupRowFailure {
    pub fn message(&self) -> &str {
        match self {
            Self::MissingKey => "Key not present in map",
            Self::UnsupportedKeyDomain(detail) => detail.as_str(),
        }
    }
}
impl std::fmt::Display for LookupRowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for LookupRowFailure {}
#[derive(Debug)]
struct RequiredLookupRow {
    ordinal: Option<usize>,
    cause: LookupRowFailure,
}
impl std::fmt::Display for RequiredLookupRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for RequiredLookupRow {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl FailureProjection for LookupRowFailure {
    fn failure_fact(&self) -> FailureFact {
        lookup_fact(
            FailureClass::Data,
            match self {
                Self::MissingKey => 377,
                Self::UnsupportedKeyDomain(_) => 378,
            },
        )
    }
}
impl FailureProjection for RequiredLookupRow {
    fn failure_fact(&self) -> FailureFact {
        self.cause.failure_fact()
    }
}
impl novarocks_failure_contract::OuterFailureProjection for RequiredLookupRow {}
pub fn required_row_error(ordinal: usize, cause: LookupRowFailure) -> KernelFailure {
    KernelFailure::from_domain(RequiredLookupRow {
        ordinal: Some(ordinal),
        cause,
    })
}
#[derive(Debug)]
struct LookupLargeIntFailure(crate::largeint::LargeIntCarrierFailure);
impl std::fmt::Display for LookupLargeIntFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "map key LARGEINT decode failed: {}", self.0)
    }
}
impl std::error::Error for LookupLargeIntFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for LookupLargeIntFailure {
    fn failure_fact(&self) -> FailureFact {
        self.0.failure_fact()
    }
}
impl novarocks_failure_contract::OuterFailureProjection for LookupLargeIntFailure {}
fn lookup_fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
    )
}
fn validate_address(inputs: &MapLookupInputs<'_>, rows: LookupRows) -> Result<(), KernelFailure> {
    if rows.map >= inputs.map.len() || rows.key >= inputs.key_arr.len() {
        return Err(contract(
            "map lookup demanded row is outside its physical carriers",
        ));
    }
    match (inputs.check_arr, rows.check) {
        (None, None) => Ok(()),
        (Some(flags), Some(index)) if index < flags.len() => Ok(()),
        _ => Err(contract(
            "map lookup flag address differs from its prepared input",
        )),
    }
}

#[cfg(test)]
mod source_properties {
    use super::*;
    use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use std::{error::Error, sync::Arc};
    #[test]
    fn only_explicit_strict_missing_key_becomes_a_required_row_failure() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("present");
        builder.values().append_value(7);
        builder.append(true).unwrap();
        let map: ArrayRef = Arc::new(builder.finish());
        let key: ArrayRef = Arc::new(StringArray::from(vec!["missing"]));
        let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let weak_map = Arc::downgrade(&map);
        let weak_key = Arc::downgrade(&key);
        let inputs = MapLookupInputs::new(&map, &key, Some(&flags)).unwrap();
        let error = lookup_observed(
            &inputs,
            crate::Selection::all(1),
            |_, row| Ok::<_, KernelFailure>(inputs.direct_rows(row)),
            None,
            |_, _| Ok(()),
            |ordinal, cause| Err(required_row_error(ordinal, cause)),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        let MapLookupFailure::Operation(failure) = error else {
            panic!("originating missing-key failure");
        };
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 377);
        assert_eq!(failure.to_string(), "Key not present in map");
        let original = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<RequiredLookupRow>()
            .unwrap();
        assert_eq!(original.ordinal, Some(0));
        drop(inputs);
        drop(map);
        drop(key);
        drop(flags);
        assert!(weak_map.upgrade().is_none());
        assert!(weak_key.upgrade().is_none());
    }
    #[test]
    fn actual_empty_value_sdk_cast_refusal_is_not_a_row_error_and_has_no_later_observer() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.append(true).unwrap();
        let map: ArrayRef = Arc::new(builder.finish());
        let key: ArrayRef = Arc::new(StringArray::from(vec!["missing"]));
        let inputs = MapLookupInputs::new(&map, &key, None).unwrap();
        let target = DataType::Struct(
            vec![Arc::new(arrow_schema::Field::new(
                "ResourceExhausted",
                DataType::Int32,
                true,
            ))]
            .into(),
        );
        let mut calls = 0;
        let error = lookup_observed(
            &inputs,
            crate::Selection::all(1),
            |_, row| Ok::<_, KernelFailure>(inputs.direct_rows(row)),
            Some(&target),
            |_, _| Ok(()),
            |_, _| panic!("SDK/metadata failures cannot enter the row journal"),
            &mut |_| {
                calls += 1;
                assert!(calls <= 3, "callback after the actual SDK cast failure");
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(calls, 3);
        let MapLookupFailure::Operation(failure) = error else {
            panic!("originating SDK cast failure");
        };
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 375);
        assert!(failure.failure_fact().resource().is_none());
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<arrow_schema::ArrowError>()
        );
    }
}

#[derive(Debug)]
pub enum KeyComparisonFailure {
    Row(LookupRowFailure),
    Operation(KernelFailure),
}
impl From<KernelFailure> for KeyComparisonFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Operation(cause)
    }
}
/// A required comparison has no caller-selected row ordinal at this boundary.
/// Only the comparator's own explicit row-domain refusal may be promoted here.
pub fn compare_keys_required(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_idx: usize,
) -> Result<bool, KernelFailure> {
    compare_keys_at(keys, key_idx, targets, target_idx).map_err(|cause| match cause {
        KeyComparisonFailure::Operation(cause) => cause,
        KeyComparisonFailure::Row(cause) => KernelFailure::from_domain(RequiredLookupRow {
            ordinal: None,
            cause,
        }),
    })
}
