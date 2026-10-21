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

//! Metadata-only signed COW layout normalization and exact dictionary expansion.
//! Values are converted by SQL before this boundary. This module never casts.
use arrow::{
    array::{
        Array, ArrayRef, DictionaryArray, LargeListArray, ListArray, StringArray, StringBuilder,
        StructArray, make_array,
    },
    datatypes::{DataType, Field, Int32Type, SchemaRef},
    record_batch::RecordBatch,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::sync::Arc;

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn allocation(source: std::collections::TryReserveError) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        novarocks_functions::allocation_failure::AllocationRefusal::new(
            "signed COW layout metadata",
            source,
        ),
    )
}
#[derive(Debug)]
struct ArrowAssemblyFailure(arrow::error::ArrowError);
impl std::fmt::Display for ArrowAssemblyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ArrowAssemblyFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ArrowAssemblyFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match &self.0 {
            ArrowError::MemoryError(_) => (FailureClass::Resource, 6001),
            ArrowError::OffsetOverflowError(_) => (FailureClass::Representation, 6002),
            ArrowError::IoError(..) => (FailureClass::Unexpected, 6003),
            _ => (FailureClass::Invariant, 6004),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
fn arrow_assembly(source: arrow::error::ArrowError) -> ConnectorError {
    ConnectorError::from_domain(ConnectorErrorKind::Internal, ArrowAssemblyFailure(source))
}
#[derive(Debug)]
struct DictionaryExtentFailure {
    required: Option<u64>,
}
impl std::fmt::Display for DictionaryExtentFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "COW dictionary expansion exceeds UTF8 offsets (required {:?})",
            self.required
        )
    }
}
impl std::error::Error for DictionaryExtentFailure {}
impl novarocks_failure_contract::FailureProjection for DictionaryExtentFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(6005).unwrap()),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::QUERY,
                code: std::num::NonZeroU16::new(1).unwrap(),
            },
            required: self.required,
            extent_limit: Some(i32::MAX as u64),
            representable_prefix: None,
        })
    }
}
fn dictionary_extent(required: Option<u64>) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        DictionaryExtentFailure { required },
    )
}

pub(super) fn normalize(
    schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch, ConnectorError> {
    if batch.num_columns() != schema.fields().len() {
        return Err(invalid(
            "COW match query output width differs from its signed contract",
        ));
    }
    let mut columns = Vec::new();
    columns
        .try_reserve_exact(schema.fields().len())
        .map_err(allocation)?;
    for (column, field) in batch.columns().iter().zip(schema.fields()) {
        columns.push(column_to_field(column, field, &|_| true)?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_assembly)
}

pub(super) fn column_to_field(
    array: &ArrayRef,
    field: &Field,
    visible: &dyn Fn(usize) -> bool,
) -> Result<ArrayRef, ConnectorError> {
    let array = if matches!(array.data_type(), DataType::Dictionary(..)) {
        if field.data_type() != &DataType::Utf8 {
            return Err(invalid(
                "COW dictionary carrier differs from its signed string type",
            ));
        }
        decode_dictionary(array)?
    } else {
        Arc::clone(array)
    };
    if !field.is_nullable() {
        for row in 0..array.len() {
            if array.is_null(row) && visible(row) {
                return Err(invalid(format!(
                    "signed COW required field contains NULL at row {row}"
                )));
            }
        }
    }
    let mut children = Vec::new();
    match (array.data_type(), field.data_type()) {
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("COW Struct carrier is not a StructArray"))?;
            children
                .try_reserve_exact(target.len())
                .map_err(allocation)?;
            let child_visible = |row| visible(row) && structure.is_valid(row);
            for (child, target) in structure.columns().iter().zip(target) {
                children.push(column_to_field(child, target, &child_visible)?.to_data());
            }
        }
        (DataType::List(_), DataType::List(target)) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| invalid("COW List carrier is not a ListArray"))?;
            let offsets = list.value_offsets();
            let child_visible = |row: usize| {
                let Some(first) = offsets.first().and_then(|v| usize::try_from(*v).ok()) else {
                    return false;
                };
                let Some(last) = offsets.last().and_then(|v| usize::try_from(*v).ok()) else {
                    return false;
                };
                if row < first || row >= last {
                    return false;
                }
                let parent = offsets
                    .partition_point(|offset| {
                        usize::try_from(*offset).is_ok_and(|offset| offset <= row)
                    })
                    .saturating_sub(1);
                parent < list.len() && visible(parent) && list.is_valid(parent)
            };
            children.push(column_to_field(list.values(), target, &child_visible)?.to_data());
        }
        (DataType::LargeList(_), DataType::LargeList(target)) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| invalid("COW LargeList carrier is not a LargeListArray"))?;
            let offsets = list.value_offsets();
            let child_visible = |row: usize| {
                let Some(first) = offsets.first().and_then(|v| usize::try_from(*v).ok()) else {
                    return false;
                };
                let Some(last) = offsets.last().and_then(|v| usize::try_from(*v).ok()) else {
                    return false;
                };
                if row < first || row >= last {
                    return false;
                }
                let parent = offsets
                    .partition_point(|offset| {
                        usize::try_from(*offset).is_ok_and(|offset| offset <= row)
                    })
                    .saturating_sub(1);
                parent < list.len() && visible(parent) && list.is_valid(parent)
            };
            children.push(column_to_field(list.values(), target, &child_visible)?.to_data());
        }
        (source, target) if source == target => {}
        _ => {
            return Err(invalid(
                "COW root carrier differs from its signed type beyond the admitted layout differences",
            ));
        }
    }
    let mut data = array
        .to_data()
        .into_builder()
        .data_type(field.data_type().clone());
    if !children.is_empty() {
        data = data.child_data(children);
    }
    if !field.is_nullable() {
        // All remaining NULLs were proved invisible beneath a NULL ancestor.
        // Removing their bitmap preserves the logical value and gives Arrow's
        // required List element representation its exact non-null contract.
        data = data.nulls(None);
    }
    let data = data.build().map_err(arrow_assembly)?;
    Ok(make_array(data))
}

pub(super) fn decoded_dictionary_bytes(array: &ArrayRef) -> Result<usize, ConnectorError> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| invalid("COW only admits Dictionary(Int32, Utf8)"))?;
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("COW only admits Dictionary(Int32, Utf8)"))?;
    let mut bytes = 0usize;
    for row in 0..dict.len() {
        if dict.keys().is_null(row) {
            continue;
        }
        let key = usize::try_from(dict.keys().value(row))
            .map_err(|_| invalid("COW dictionary has a negative key"))?;
        if key >= values.len() {
            return Err(invalid("COW dictionary key exceeds its values"));
        }
        if !values.is_null(key) {
            bytes = bytes
                .checked_add(values.value(key).len())
                .ok_or_else(|| dictionary_extent(None))?;
        }
    }
    if bytes > i32::MAX as usize {
        return Err(dictionary_extent(u64::try_from(bytes).ok()));
    }
    Ok(bytes)
}
fn decode_dictionary(array: &ArrayRef) -> Result<ArrayRef, ConnectorError> {
    let bytes = decoded_dictionary_bytes(array)?;
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| invalid("COW dictionary has a foreign carrier"))?;
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("COW dictionary has a foreign value carrier"))?;
    let mut output = StringBuilder::with_capacity(dict.len(), bytes);
    for row in 0..dict.len() {
        if dict.keys().is_null(row) {
            output.append_null();
            continue;
        }
        let key = usize::try_from(dict.keys().value(row))
            .map_err(|_| invalid("COW dictionary has a negative key"))?;
        if values.is_null(key) {
            output.append_null();
        } else {
            output.append_value(values.value(key));
        }
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests;
