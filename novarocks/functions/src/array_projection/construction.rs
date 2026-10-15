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

//! Arrow 58.4 locked-operation logical observations. Private Bytes owners,
//! ArrayData reconstruction and validation scratch are explicitly upper bounds,
//! never exact Layout invoices or retained foreign backing estimates.
//! Source anchors: arrow-select/take.rs::{take_bytes,take_nulls};
//! arrow-buffer/buffer/immutable.rs::Buffer::from_vec;
//! arrow-array/array/{struct_array,map_array,union_array,run_array}.rs;
//! arrow-data/data.rs::{ArrayData,ArrayDataBuilder}. The shared private-buffer
//! owner bound is the existing root_array_storage contract, not a new mirror.

use super::ArrayNormalizationStep;

fn dictionary_values(source: &dyn Array) -> Option<&ArrayRef> {
    use arrow_array::types::{
        Int8Type, Int16Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    macro_rules! values {
        ($key:ty) => {
            source
                .as_any()
                .downcast_ref::<DictionaryArray<$key>>()
                .map(DictionaryArray::values)
        };
    }
    let DataType::Dictionary(key, _) = source.data_type() else {
        return None;
    };
    match key.as_ref() {
        DataType::Int8 => values!(Int8Type),
        DataType::Int16 => values!(Int16Type),
        DataType::Int32 => values!(Int32Type),
        DataType::Int64 => values!(Int64Type),
        DataType::UInt8 => values!(UInt8Type),
        DataType::UInt16 => values!(UInt16Type),
        DataType::UInt32 => values!(UInt32Type),
        DataType::UInt64 => values!(UInt64Type),
        _ => None,
    }
}
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, DictionaryArray, LargeStringArray, StringArray};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, FieldRef};
use std::alloc::Layout;

/// Original private immutable Buffer-owner bound, pinned by Execution's
/// counting-allocator root_capacity_introspection regression (pool disabled).
/// This is an opaque bound; it grants no capacity and is not an exact Layout.
pub const ARROW_BUFFER_OWNER_METADATA_BOUND: usize = 96;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArrayProjectionOpaqueBound {
    pub requests_upper: usize,
    pub bytes_upper: usize,
    pub work_upper: usize,
}
type Request<'a, E> = dyn FnMut(Layout) -> Result<(), E> + 'a;
type Opaque<'a, E> = dyn FnMut(ArrayProjectionOpaqueBound) -> Result<(), E> + 'a;
type Work<'a, E> = dyn FnMut(usize) -> Result<(), E> + 'a;

fn overflow() -> String {
    "Arrow construction observation overflow".to_owned()
}
fn add(a: usize, b: usize) -> Result<usize, String> {
    a.checked_add(b).ok_or_else(overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, String> {
    a.checked_mul(b).ok_or_else(overflow)
}
fn array<T, E: From<String>>(n: usize, request: &mut Request<'_, E>) -> Result<(), E> {
    if n != 0 {
        request(Layout::array::<T>(n).map_err(|_| overflow())?)?;
    }
    Ok(())
}
fn arc<T, E: From<String>>(request: &mut Request<'_, E>) -> Result<(), E> {
    request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(|error| E::from(format!("Arrow public wrapper request: {error:?}")))?,
    )
}
fn bitmap_bound(rows: usize) -> Result<usize, String> {
    // MutableBuffer's locked source rounds a nonzero bitmap payload to 64 B.
    let bytes = add(rows, 7)? / 8;
    Ok(mul(add(bytes, 63)? / 64, 64)?)
}
fn clone_parameters<E: From<String>>(
    ty: &DataType,
    request: &mut Request<'_, E>,
    work: &mut Work<'_, E>,
    depth: usize,
) -> Result<(), E> {
    work(1)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(overflow()));
    }
    if let DataType::Dictionary(key, value) = ty {
        request(Layout::new::<DataType>())?;
        request(Layout::new::<DataType>())?;
        clone_parameters(key, request, work, depth + 1)?;
        clone_parameters(value, request, work, depth + 1)?;
    }
    // Other container/scalar parameters share Arc-owned backing on DataType clone.
    Ok(())
}

pub fn observe_array_normalization_construction<E: From<String>>(
    step: ArrayNormalizationStep<'_>,
    request: &mut Request<'_, E>,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), E> {
    match step {
        ArrayNormalizationStep::OpaqueOperationFinished => work(0),
        ArrayNormalizationStep::InspectType(_) => work(1),
        ArrayNormalizationStep::InspectConstructionWork { units } => work(units),
        ArrayNormalizationStep::InspectField { source, name } => {
            work(add(add(source.name().len(), name.len())?, 1)?)
        }
        ArrayNormalizationStep::InspectDictionaryRows { row_count, .. } => work(row_count),
        ArrayNormalizationStep::RebuildChunkScaffolds { channels } => work(channels),
        ArrayNormalizationStep::CreateRecordBatchWrapper => {
            arc::<arrow_array::RecordBatch, E>(request)
        }
        ArrayNormalizationStep::CreateChunkColumns { channels } => {
            work(channels)?;
            array::<ArrayRef, E>(channels, request)
        }
        ArrayNormalizationStep::RebuildField { target, name, .. } => {
            work(name.len())?;
            array::<u8, E>(name.len(), request)?;
            clone_parameters(target, request, work, 1)?;
            arc::<Field, E>(request)
        }
        ArrayNormalizationStep::RebuildType { source } => type_bound(source, opaque, work),
        ArrayNormalizationStep::RebuildArray { source, target, .. } => {
            array_bound(source, target, request, opaque, work, false)
        }
        ArrayNormalizationStep::DecodeDictionary { source, target, .. } => {
            decode(source, target, request, opaque, work)
        }
        ArrayNormalizationStep::ValidateStringWrapper { source, .. } => {
            // arrow-array 58.4 GenericStringType::validate scans the complete
            // visible value buffer and checks each offset's UTF-8 boundary.
            // Buffer clones share their existing owner; only the public Arc
            // wrapper is a new exact request. Validation has no allocation on
            // this already-valid success path and is an opaque work bound.
            work(0)?;
            let (bytes, offsets) = match source.data_type() {
                DataType::Utf8 => {
                    let array = source
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            E::from(
                                "validated String wrapper has a different physical array"
                                    .to_owned(),
                            )
                        })?;
                    arc::<StringArray, E>(request)?;
                    (array.values().len(), array.offsets().len())
                }
                DataType::LargeUtf8 => {
                    let array = source
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            E::from(
                                "validated LargeString wrapper has a different physical array"
                                    .to_owned(),
                            )
                        })?;
                    arc::<LargeStringArray, E>(request)?;
                    (array.values().len(), array.offsets().len())
                }
                _ => {
                    return Err(E::from(
                        "validated String wrapper requires UTF-8 offsets".to_owned(),
                    ));
                }
            };
            opaque(ArrayProjectionOpaqueBound {
                requests_upper: 0,
                bytes_upper: 0,
                work_upper: add(bytes, offsets)?,
            })
        }
        ArrayNormalizationStep::SliceArray { source, .. } => {
            array_bound(source, source.data_type(), request, opaque, work, true)
        }
    }
}

fn type_bound<E: From<String>>(
    ty: &DataType,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), E> {
    work(1)?;
    let (elements, width, extra) = match ty {
        DataType::Struct(fields) => (fields.len(), std::mem::size_of::<FieldRef>(), 1),
        DataType::Union(fields, _) => (fields.len(), std::mem::size_of::<(i8, FieldRef)>(), 4),
        DataType::Dictionary(_, _) => (2, std::mem::size_of::<DataType>(), 0),
        _ => (0, 0, 0),
    };
    if elements == 0 {
        return Ok(());
    }
    // Result-shunt/UnionFields collections can grow from Vec::new. Their
    // doubling capacities and the final Arc conversion are covered, rather
    // than claiming a foreign private collection has an exact layout.
    let bytes = mul(add(mul(elements.max(4), 4)?, extra)?, width)?;
    opaque(ArrayProjectionOpaqueBound {
        requests_upper: add(elements, 4)?,
        bytes_upper: add(bytes, mul(extra, ARROW_BUFFER_OWNER_METADATA_BOUND)?)?,
        work_upper: elements,
    })
}

fn datatype_nodes<E: From<String>>(
    ty: &DataType,
    depth: usize,
    work: &mut Work<'_, E>,
) -> Result<usize, E> {
    work(1)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(overflow()));
    }
    let mut sum = 1usize;
    let mut child = |ty: &DataType| -> Result<(), E> {
        sum = add(sum, datatype_nodes(ty, depth + 1, work)?)?;
        if sum > novarocks_type_contract::MAX_VALUE_TYPE_NODES {
            return Err(E::from(overflow()));
        }
        Ok(())
    };
    match ty {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => child(f.data_type())?,
        DataType::Struct(fs) => {
            for f in fs {
                child(f.data_type())?;
            }
        }
        DataType::Union(fs, _) => {
            for (_, f) in fs.iter() {
                child(f.data_type())?;
            }
        }
        DataType::Dictionary(k, v) => {
            child(k)?;
            child(v)?;
        }
        DataType::RunEndEncoded(k, v) => {
            child(k.data_type())?;
            child(v.data_type())?;
        }
        _ => {}
    }
    Ok(sum)
}
fn array_bound<E: From<String>>(
    source: &ArrayRef,
    target: &DataType,
    request: &mut Request<'_, E>,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
    slice: bool,
) -> Result<(), E> {
    use arrow_array::{FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray};
    work(1)?;
    if !slice {
        if super::requires_default_validated_container(target) {
            return metadata_fallback(source, target, opaque, work, false);
        }
        let mut scratch_rows = 0usize;
        let mut scratch_masks = 0usize;
        let mut inspect_required = |field: &Field, array: &ArrayRef| -> Result<(), E> {
            work(1)?;
            if !field.is_nullable() {
                if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                    if dict.values().null_count() != 0 {
                        scratch_rows = add(scratch_rows, array.len())?;
                        scratch_masks = add(scratch_masks, 1)?;
                    }
                } else if matches!(
                    array.data_type(),
                    DataType::Union(_, _) | DataType::RunEndEncoded(_, _) | DataType::Null
                ) {
                    let mut nodes = 0;
                    let span = borrowed_array_span(array.as_ref(), 1, &mut nodes, work)?;
                    scratch_rows = add(scratch_rows, mul(nodes, span)?)?;
                    scratch_masks = add(scratch_masks, nodes)?;
                }
            }
            Ok(())
        };
        match target {
            DataType::Struct(fields) => {
                let actual = source
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        E::from("observed Struct wrapper is not an Arrow StructArray".to_owned())
                    })?;
                array::<ArrayRef, E>(fields.len(), request)?;
                arc::<StructArray, E>(request)?;
                for (field, child) in fields.iter().zip(actual.columns()) {
                    inspect_required(field, child)?;
                }
            }
            DataType::Map(entries, _) => {
                let actual = source.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    E::from("observed Map wrapper is not an Arrow MapArray".to_owned())
                })?;
                array::<ArrayRef, E>(2, request)?;
                arc::<StructArray, E>(request)?;
                arc::<MapArray, E>(request)?;
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(E::from(overflow()));
                };
                for (field, child) in fields.iter().zip(actual.entries().columns()) {
                    inspect_required(field, child)?;
                }
            }
            DataType::List(field) => {
                let actual = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    E::from("observed List wrapper is not an Arrow ListArray".to_owned())
                })?;
                arc::<ListArray, E>(request)?;
                inspect_required(field, actual.values())?;
            }
            DataType::LargeList(field) => {
                let actual = source
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        E::from(
                            "observed LargeList wrapper is not an Arrow LargeListArray".to_owned(),
                        )
                    })?;
                arc::<LargeListArray, E>(request)?;
                inspect_required(field, actual.values())?;
            }
            DataType::FixedSizeList(field, _) => {
                let actual = source
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        E::from(
                            "observed FixedSizeList wrapper is not an Arrow FixedSizeListArray"
                                .to_owned(),
                        )
                    })?;
                arc::<FixedSizeListArray, E>(request)?;
                inspect_required(field, actual.values())?;
            }
            _ => return metadata_fallback(source, target, opaque, work, false),
        }
        if scratch_masks != 0 {
            opaque(ArrayProjectionOpaqueBound {
                requests_upper: mul(scratch_masks, 4)?,
                bytes_upper: add(
                    mul(
                        add(bitmap_bound(scratch_rows)?, mul(scratch_masks, 64)?)?,
                        4,
                    )?,
                    mul(scratch_masks, ARROW_BUFFER_OWNER_METADATA_BOUND)?,
                )?,
                work_upper: mul(scratch_rows, 4)?,
            })?;
        }
        return Ok(());
    }
    metadata_fallback(source, target, opaque, work, true)
}
fn metadata_fallback<E: From<String>>(
    source: &ArrayRef,
    target: &DataType,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
    slice: bool,
) -> Result<(), E> {
    let nodes = datatype_nodes(source.data_type(), 1, work)?.max(datatype_nodes(target, 1, work)?);
    let span = borrowed_array_span(source.as_ref(), 1, &mut 0, work)?;
    // Slice visits its retained child wrappers; the ArrayData Union/REE branch
    // can repeatedly reconstruct child subtrees along their bounded depth.
    let factor = if slice {
        4
    } else {
        novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 2
    };
    let repeats = mul(nodes, factor)?;
    let node_bytes = add(
        mul(std::mem::size_of::<ArrayData>(), 8)?,
        mul(ARROW_BUFFER_OWNER_METADATA_BOUND, 8)?,
    )?;
    let null_scratch = mul(
        nodes,
        add(
            mul(bitmap_bound(span)?, 4)?,
            ARROW_BUFFER_OWNER_METADATA_BOUND,
        )?,
    )?;
    opaque(ArrayProjectionOpaqueBound {
        requests_upper: mul(repeats, 12)?,
        bytes_upper: add(mul(repeats, node_bytes)?, null_scratch)?,
        work_upper: add(repeats, mul(nodes, span)?)?,
    })
}

fn borrowed_array_span<E: From<String>>(
    source: &dyn Array,
    depth: usize,
    nodes: &mut usize,
    work: &mut Work<'_, E>,
) -> Result<usize, E> {
    use arrow_array::types::{Int16Type, Int64Type};
    use arrow_array::{
        FixedSizeListArray, LargeListArray, ListArray, MapArray, RunArray, StructArray, UnionArray,
    };
    work(1)?;
    *nodes = add(*nodes, 1)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
        || *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES
    {
        return Err(E::from(overflow()));
    }
    let mut span = source.len();
    let mut child = |array: &dyn Array| -> Result<(), E> {
        span = span.max(borrowed_array_span(array, depth + 1, nodes, work)?);
        Ok(())
    };
    if let Some(array) = source.as_any().downcast_ref::<StructArray>() {
        for array in array.columns() {
            child(array.as_ref())?;
        }
    } else if let Some(array) = source.as_any().downcast_ref::<MapArray>() {
        child(array.entries())?;
    } else if let Some(array) = source.as_any().downcast_ref::<ListArray>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<LargeListArray>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<FixedSizeListArray>() {
        child(array.values().as_ref())?;
    } else if let Some(values) = dictionary_values(source) {
        child(values.as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<UnionArray>() {
        let DataType::Union(fields, _) = source.data_type() else {
            return Err(E::from(overflow()));
        };
        for (id, _) in fields.iter() {
            child(array.child(id).as_ref())?;
        }
    } else if let Some(array) = source.as_any().downcast_ref::<RunArray<Int16Type>>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<RunArray<Int32Type>>() {
        child(array.values().as_ref())?;
    } else if let Some(array) = source.as_any().downcast_ref::<RunArray<Int64Type>>() {
        child(array.values().as_ref())?;
    }
    Ok(span)
}

fn decode<E: From<String>>(
    source: &ArrayRef,
    target: &DataType,
    request: &mut Request<'_, E>,
    opaque: &mut Opaque<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), E> {
    let dict = source
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| {
            E::from("decode observation requires admitted Int32 string dictionary".to_owned())
        })?;
    let mut bytes = 0usize;
    // Offsets remain borrowed. Do not collect an auxiliary length vector.
    let (width, wrapper) = match (dict.values().data_type(), target) {
        (DataType::Utf8, DataType::Utf8) => (4, 0),
        (DataType::LargeUtf8, DataType::LargeUtf8) => (8, 1),
        _ => {
            return Err(E::from(
                "decode observation does not authorize another encoding".to_owned(),
            ));
        }
    };
    // The direct loop duplicates the finite take_bytes sizing pass, preserving
    // NULL keys and referenced value NULLs, including sliced key coordinates.
    for at in 0..dict.len() {
        if at % 256 == 0 {
            work((dict.len() - at).min(256))?;
        }
        if dict.keys().is_null(at) {
            continue;
        }
        let key = usize::try_from(dict.keys().value(at))
            .map_err(|_| E::from("negative dictionary key during observation".to_owned()))?;
        if key >= dict.values().len() {
            return Err(E::from("dictionary key exceeds observed values".to_owned()));
        }
        if dict.values().is_null(key) {
            continue;
        }
        let length = if wrapper == 0 {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("observed Utf8 carrier");
            usize::try_from(values.value_offsets()[key + 1] - values.value_offsets()[key])
                .map_err(|_| overflow())?
        } else {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("observed LargeUtf8 carrier");
            usize::try_from(values.value_offsets()[key + 1] - values.value_offsets()[key])
                .map_err(|_| overflow())?
        };
        bytes = add(bytes, length)?;
    }
    if (width == 4 && bytes > i32::MAX as usize) || bytes > i64::MAX as usize {
        return Err(E::from(
            "decoded strings exceed their frozen offset representation".to_owned(),
        ));
    }
    if width == 4 {
        array::<i32, E>(add(dict.len(), 1)?, request)?;
    } else {
        array::<i64, E>(add(dict.len(), 1)?, request)?;
    }
    array::<u8, E>(bytes, request)?;
    if wrapper == 0 {
        arc::<StringArray, E>(request)?;
    } else {
        arc::<LargeStringArray, E>(request)?;
    }
    // take_nulls can create validity scratch; Buffer::from_vec creates private
    // Bytes owners. Cast may also rebuild the existing dictionary value wrapper.
    opaque(ArrayProjectionOpaqueBound {
        requests_upper: 12,
        bytes_upper: add(
            mul(ARROW_BUFFER_OWNER_METADATA_BOUND, 12)?,
            mul(bitmap_bound(dict.len())?, 4)?,
        )?,
        work_upper: add(mul(dict.len(), 4)?, bytes)?,
    })
}

#[cfg(test)]
mod validated_string_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn validation_model_covers_whole_buffer_and_offsets_but_only_new_wrapper_storage() {
        for large in [false, true] {
            let string: ArrayRef = Arc::new(StringArray::from(vec!["prefix", "é", "suffix"]));
            let string = if large {
                arrow_cast::cast(string.as_ref(), &DataType::LargeUtf8).unwrap()
            } else {
                string
            };
            let source = string.slice(1, 1);
            let mut requests = Vec::new();
            let mut bounds = Vec::new();
            let mut work_units = Vec::new();
            observe_array_normalization_construction::<String>(
                ArrayNormalizationStep::ValidateStringWrapper {
                    path: &[],
                    source: &source,
                },
                &mut |layout| {
                    requests.push(layout);
                    Ok(())
                },
                &mut |bound| {
                    bounds.push(bound);
                    Ok(())
                },
                &mut |units| {
                    work_units.push(units);
                    Ok(())
                },
            )
            .unwrap();
            let (bytes, offsets, layout) = if large {
                let a = source.as_any().downcast_ref::<LargeStringArray>().unwrap();
                (
                    a.values().len(),
                    a.offsets().len(),
                    Layout::new::<LargeStringArray>(),
                )
            } else {
                let a = source.as_any().downcast_ref::<StringArray>().unwrap();
                (
                    a.values().len(),
                    a.offsets().len(),
                    Layout::new::<StringArray>(),
                )
            };
            assert!(bytes > "é".len());
            assert_eq!(
                requests,
                vec![novarocks_type_contract::owned_resources::layout::arc_layout(layout).unwrap()]
            );
            assert_eq!(
                bounds,
                vec![ArrayProjectionOpaqueBound {
                    requests_upper: 0,
                    bytes_upper: 0,
                    work_upper: bytes + offsets
                }]
            );
            assert_eq!(work_units, vec![0]);
            let mut called = false;
            let error = observe_array_normalization_construction::<String>(
                ArrayNormalizationStep::ValidateStringWrapper {
                    path: &[],
                    source: &source,
                },
                &mut |_| Err("original wrapper refusal".to_owned()),
                &mut |_| {
                    called = true;
                    Ok(())
                },
                &mut |_| Ok(()),
            )
            .unwrap_err();
            assert_eq!(error, "original wrapper refusal");
            assert!(!called);
        }
    }
}
