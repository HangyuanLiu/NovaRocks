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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryBuilder, LargeListArray, LargeStringBuilder, ListArray, StringBuilder, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use crate::runtime::fragment::ExecutionFailure;
#[derive(Debug)]
enum DictDecodeFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    Code {
        value: i128,
        source: std::num::TryFromIntError,
    },
    Utf8 {
        code: Option<i32>,
        source: std::str::Utf8Error,
    },
    Offsets {
        unit: usize,
        required: u128,
        wide: bool,
    },
    Backing {
        required: u128,
    },
}
impl std::fmt::Display for DictDecodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::Code { value, source } => {
                write!(f, "dictionary code {value} exceeds i32: {source}")
            }
            Self::Utf8 { code, source } => write!(
                f,
                "global dict value for code {code:?} is not utf8: {source}"
            ),
            Self::Offsets {
                unit,
                required,
                wide,
            } => write!(
                f,
                "dictionary output offset exceeded: single={unit} bytes={required} large={wide}"
            ),
            Self::Backing { required } => write!(
                f,
                "dictionary decode backing requires {required} bytes, maximum {}",
                isize::MAX
            ),
        }
    }
}
impl std::error::Error for DictDecodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Code { source, .. } => Some(source),
            Self::Utf8 { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for DictDecodeFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, ResourceDimension, ResourceFacts, ResourceScope, domain,
        };
        let (class, reason) = match self {
            Self::Contract(_) => (FailureClass::Invariant, ExecutionReason::DictDecodeContract),
            Self::Code { .. } => (FailureClass::Invariant, ExecutionReason::DictDecodeCode),
            Self::Utf8 { .. } => (FailureClass::Invariant, ExecutionReason::DictDecodeUtf8),
            Self::Offsets { .. } => (
                FailureClass::Representation,
                ExecutionReason::DictDecodeOffsets,
            ),
            Self::Backing { .. } => (
                FailureClass::Representation,
                ExecutionReason::DictDecodeBacking,
            ),
        };
        let fact = FailureFact::new(class, reason.reason());
        match self {
            Self::Offsets {
                unit,
                required,
                wide,
            } => {
                let limit = if *wide {
                    i64::MAX as u128
                } else {
                    i32::MAX as u128
                };
                let single = *unit as u128 > limit;
                fact.with_representation(RepresentationFacts {
                    boundary: if single {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: std::num::NonZeroU16::new(if *wide { 30 } else { 29 }).unwrap(),
                    },
                    required: u64::try_from(if single { *unit as u128 } else { *required }).ok(),
                    extent_limit: Some(limit as u64),
                    representable_prefix: None,
                })
            }
            Self::Backing { required } => {
                novarocks_functions::allocation_failure::rust_array_backing_failure_fact(
                    fact.reason(),
                    *required,
                    RepresentationBoundary::SingleOutput,
                )
            }
            _ => fact,
        }
    }
}
fn dict_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(DictDecodeFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
fn backing_extent(required: u128) -> Result<(), ExecutionFailure> {
    if required > isize::MAX as u128 {
        Err(ExecutionFailure::from_domain(DictDecodeFailure::Backing {
            required,
        }))
    } else {
        Ok(())
    }
}
fn output_extent(
    rows: usize,
    unit: usize,
    required: u128,
    wide: bool,
) -> Result<(), ExecutionFailure> {
    let limit = if wide {
        i64::MAX as u128
    } else {
        i32::MAX as u128
    };
    if required > limit {
        return Err(ExecutionFailure::from_domain(DictDecodeFailure::Offsets {
            unit,
            required,
            wide,
        }));
    }
    backing_extent((rows as u128 + 1) * if wide { 8 } else { 4 })?;
    backing_extent(required)
}
fn decode_codes_to_output(
    codes: &[Option<i32>],
    dict: &HashMap<i32, Vec<u8>>,
    output_type: &DataType,
) -> Result<ArrayRef, ExecutionFailure> {
    let code_to_bytes = |code: Option<i32>| -> Result<Option<&Vec<u8>>, ExecutionFailure> {
        if let Some(code) = code {
            if let Some(bytes) = dict.get(&code) {
                return Ok(Some(bytes));
            }
            // Keep legacy semantics for NULL sentinel code 0 when no explicit mapping exists.
            if code == 0 {
                return Ok(None);
            }
            return Err(dict_contract(format_args!(
                "global dict missing code {}",
                code
            )));
        }
        // Null row: allow derived dicts to provide explicit null-output mapping on code 0.
        if let Some(bytes) = dict.get(&0) {
            return Ok(Some(bytes));
        }
        Ok(None)
    };

    let wide = matches!(output_type, DataType::LargeBinary | DataType::LargeUtf8);
    let mut required = 0u128;
    let mut unit = 0usize;
    for &code in codes {
        if let Some(bytes) = code_to_bytes(code)? {
            required += bytes.len() as u128;
            unit = unit.max(bytes.len());
        }
    }
    output_extent(codes.len(), unit, required, wide)?;
    match output_type {
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    builder.append_value(bytes);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            let mut builder = LargeBinaryBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    builder.append_value(bytes);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    let value = std::str::from_utf8(bytes).map_err(|source| {
                        ExecutionFailure::from_domain(DictDecodeFailure::Utf8 {
                            code: *code,
                            source,
                        })
                    })?;
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            let mut builder = StringBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    let value = std::str::from_utf8(bytes).map_err(|source| {
                        ExecutionFailure::from_domain(DictDecodeFailure::Utf8 {
                            code: *code,
                            source,
                        })
                    })?;
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn collect_codes_i8(values: &Int8Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    Ok(out)
}

fn collect_codes_i16(values: &Int16Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    Ok(out)
}

fn collect_codes_i32(values: &Int32Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(values.value(i)));
        }
    }
    Ok(out)
}

fn collect_codes_i64(values: &Int64Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            let value = values.value(i);
            out.push(Some(i32::try_from(value).map_err(|source| {
                ExecutionFailure::from_domain(DictDecodeFailure::Code {
                    value: value as i128,
                    source,
                })
            })?));
        }
    }
    Ok(out)
}

fn collect_codes_u8(values: &UInt8Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    Ok(out)
}

fn collect_codes_u16(values: &UInt16Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    Ok(out)
}

fn collect_codes_u32(values: &UInt32Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            let value = values.value(i);
            out.push(Some(i32::try_from(value).map_err(|source| {
                ExecutionFailure::from_domain(DictDecodeFailure::Code {
                    value: value as i128,
                    source,
                })
            })?));
        }
    }
    Ok(out)
}

fn collect_codes_u64(values: &UInt64Array) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    backing_extent(values.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            let value = values.value(i);
            out.push(Some(i32::try_from(value).map_err(|source| {
                ExecutionFailure::from_domain(DictDecodeFailure::Code {
                    value: value as i128,
                    source,
                })
            })?));
        }
    }
    Ok(out)
}

fn collect_codes_from_array(input: &ArrayRef) -> Result<Vec<Option<i32>>, ExecutionFailure> {
    match input.data_type() {
        DataType::Int8 => {
            let values = input
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast Int8 array"))?;
            collect_codes_i8(values)
        }
        DataType::Int16 => {
            let values = input
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast Int16 array"))?;
            collect_codes_i16(values)
        }
        DataType::Int32 => {
            let values = input
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast Int32 array"))?;
            collect_codes_i32(values)
        }
        DataType::Int64 => {
            let values = input
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast Int64 array"))?;
            collect_codes_i64(values)
        }
        DataType::UInt8 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast UInt8 array"))?;
            collect_codes_u8(values)
        }
        DataType::UInt16 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast UInt16 array"))?;
            collect_codes_u16(values)
        }
        DataType::UInt32 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast UInt32 array"))?;
            collect_codes_u32(values)
        }
        DataType::UInt64 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast UInt64 array"))?;
            collect_codes_u64(values)
        }
        DataType::Null => {
            backing_extent(input.len() as u128 * std::mem::size_of::<Option<i32>>() as u128)?;
            Ok(vec![None; input.len()])
        }
        other => Err(dict_contract(format_args!(
            "dict decode expects integer input, got {other:?}"
        ))),
    }
}

fn decode_array_with_dict(
    input: &ArrayRef,
    dict: &HashMap<i32, Vec<u8>>,
    output_type: &DataType,
) -> Result<ArrayRef, ExecutionFailure> {
    match (input.data_type(), output_type) {
        (DataType::List(_), DataType::List(output_item)) => {
            let list = input
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast List array"))?;
            let decoded_values =
                decode_array_with_dict(list.values(), dict, output_item.data_type())?;
            Ok(Arc::new(ListArray::new(
                output_item.clone(),
                list.offsets().clone(),
                decoded_values,
                list.nulls().cloned(),
            )))
        }
        (DataType::LargeList(_), DataType::LargeList(output_item)) => {
            let list = input
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| dict_contract("dict decode failed to downcast LargeList array"))?;
            let decoded_values =
                decode_array_with_dict(list.values(), dict, output_item.data_type())?;
            Ok(Arc::new(LargeListArray::new(
                output_item.clone(),
                list.offsets().clone(),
                decoded_values,
                list.nulls().cloned(),
            )))
        }
        (DataType::Null, _) => Ok(new_null_array(output_type, input.len())),
        _ => {
            let codes = collect_codes_from_array(input)?;
            decode_codes_to_output(&codes, dict, output_type)
        }
    }
}

fn default_dict_decode_output_type(input_type: &DataType) -> DataType {
    match input_type {
        DataType::List(item) => DataType::List(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(default_dict_decode_output_type(item.data_type())),
        )),
        DataType::LargeList(item) => DataType::LargeList(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(default_dict_decode_output_type(item.data_type())),
        )),
        _ => DataType::Utf8,
    }
}

fn normalize_dict_decode_output_type(
    input_type: &DataType,
    requested_output_type: &DataType,
) -> DataType {
    match input_type {
        DataType::List(input_item) => {
            let item_output = match requested_output_type {
                DataType::List(output_item) => normalize_dict_decode_output_type(
                    input_item.data_type(),
                    output_item.data_type(),
                ),
                _ => default_dict_decode_output_type(input_item.data_type()),
            };
            DataType::List(Arc::new(
                input_item.as_ref().clone().with_data_type(item_output),
            ))
        }
        DataType::LargeList(input_item) => {
            let item_output = match requested_output_type {
                DataType::LargeList(output_item) => normalize_dict_decode_output_type(
                    input_item.data_type(),
                    output_item.data_type(),
                ),
                _ => default_dict_decode_output_type(input_item.data_type()),
            };
            DataType::LargeList(Arc::new(
                input_item.as_ref().clone().with_data_type(item_output),
            ))
        }
        _ => requested_output_type.clone(),
    }
}

pub fn eval_dict_decode<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    id: ExprId,
    child: ExprId,
    dict: &Arc<HashMap<i32, Vec<u8>>>,
    chunk: &Chunk,
) -> Result<ArrayRef, ExecutionFailure> {
    let input = arena.eval(child, chunk)?;
    let output_type = arena.data_type(id).unwrap_or(&DataType::Utf8);
    let normalized_output_type = normalize_dict_decode_output_type(input.data_type(), output_type);
    Ok((decode_array_with_dict(&input, dict, &normalized_output_type))?)
}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn dictionary_refusals_keep_original_sources_without_retaining_payloads() {
        use std::error::Error;
        let dict = Arc::new(HashMap::from([(1, vec![255])]));
        let weak = Arc::downgrade(&dict);
        let error = decode_codes_to_output(&[Some(1)], &dict, &DataType::Utf8).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        let source = std::str::from_utf8(&[255]).unwrap_err();
        let cause = DictDecodeFailure::Utf8 {
            code: Some(1),
            source,
        };
        assert!(
            cause
                .source()
                .unwrap()
                .downcast_ref::<std::str::Utf8Error>()
                .is_some()
        );
        drop(dict);
        assert!(weak.upgrade().is_none());
        assert!(!error.detail().is_empty());
        let error = collect_codes_u64(&UInt64Array::from(vec![u64::MAX])).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
    #[test]
    fn dictionary_expansion_checks_geometry_and_keeps_null_sentinel_bytes() {
        let error = output_extent(2, 1, i32::MAX as u128 + 1, false).unwrap_err();
        assert_eq!(
            error.failure_fact().representation().unwrap().boundary,
            RepresentationBoundary::SingleOutput
        );
        assert_eq!(
            error.failure_fact().representation().unwrap().required,
            Some(i32::MAX as u64 + 1)
        );
        let error =
            output_extent(1, i32::MAX as usize + 1, i32::MAX as u128 + 1, false).unwrap_err();
        assert_eq!(
            error.failure_fact().representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
        let error = output_extent(usize::MAX, 0, 0, true).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Representation);
        assert!(error.failure_fact().resource().is_none());
        let extent = error.failure_fact().representation().unwrap();
        assert_eq!(extent.required, None);
        assert_eq!(extent.layout.code.get(), 80);
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        let dict = HashMap::from([(1, vec![0, 255, 3])]);
        let array =
            decode_codes_to_output(&[Some(1), Some(0), None], &dict, &DataType::Binary).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(array.value(0), &[0, 255, 3]);
        assert!(array.is_null(1));
        assert!(array.is_null(2));
    }
}
