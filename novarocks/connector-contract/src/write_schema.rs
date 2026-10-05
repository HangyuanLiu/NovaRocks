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

//! Pure bounded Arrow field contracts shared by writer recipes and write
//! result relations. Runtime Arrow arrays and provider capabilities stay out.
use crate::{ConnectorError, ConnectorErrorKind};
use arrow_schema::{DataType, Field};

pub const MAX_WRITE_RELATION_FIELD_NAME_BYTES: usize = 1_024;
pub const MAX_WRITE_RELATION_TYPE_DEPTH: usize = 32;
pub const MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD: usize = 64;
pub const MAX_WRITE_RELATION_METADATA_KEY_BYTES: usize = 1_024;
pub const MAX_WRITE_RELATION_METADATA_VALUE_BYTES: usize = 64 * 1_024;
pub const MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES: usize = 16 * 1_024 * 1_024;

pub const WRITE_FIELD_ALLOCATION_CHARGE: usize = 128;
const TYPE_ALLOCATION_CHARGE: usize = 64;

pub use novarocks_type_contract::{
    arrow_data_types_exact, arrow_fields_exact, arrow_schemas_exact,
};

fn resource_exhausted(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

pub fn charge_write_schema(decoded_bytes: &mut usize, amount: usize) -> Result<(), ConnectorError> {
    *decoded_bytes = decoded_bytes
        .checked_add(amount)
        .ok_or_else(|| resource_exhausted("write relation schema allocation charge overflowed"))?;
    if *decoded_bytes > MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES {
        return Err(resource_exhausted(
            "write relation schema exceeds the decoded allocation limit",
        ));
    }
    Ok(())
}

pub fn validate_write_field_name(name: &str) -> Result<(), ConnectorError> {
    if name.len() > MAX_WRITE_RELATION_FIELD_NAME_BYTES {
        return Err(resource_exhausted(
            "write relation field name exceeds the byte limit",
        ));
    }
    Ok(())
}

/// The writer owner's original per-field charge. This reads only borrowed
/// attributes, so the flat type graph can admit the same domain before building
/// an Arrow Field. It does not validate logical metadata or grant allocation.
pub fn charge_write_field_header(
    name: &str,
    metadata_entries: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    validate_write_field_name(name)?;
    if metadata_entries > MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD {
        return Err(resource_exhausted(
            "write relation field metadata exceeds the entry limit",
        ));
    }
    charge_write_schema(decoded_bytes, WRITE_FIELD_ALLOCATION_CHARGE + name.len())
}

/// The same entry bounds and charge for either an Arrow metadata entry or its
/// borrowed flat representation. Order and logical interpretation remain with
/// their existing owners.
pub fn charge_write_metadata_entry(
    key: &str,
    value: &str,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    if key.len() > MAX_WRITE_RELATION_METADATA_KEY_BYTES {
        return Err(resource_exhausted(
            "write relation field metadata key exceeds the byte limit",
        ));
    }
    if value.len() > MAX_WRITE_RELATION_METADATA_VALUE_BYTES {
        return Err(resource_exhausted(
            "write relation field metadata value exceeds the byte limit",
        ));
    }
    charge_write_schema(
        decoded_bytes,
        key.len() + value.len() + 2 * size_of::<String>(),
    )
}

/// The writer depth and original type-node charge, including a timestamp's
/// optional timezone bytes. This does not reinterpret the carrier, parse a
/// timezone or impose the Value owner's unrelated unfolded-node bound.
pub fn charge_write_type_header(
    depth: usize,
    timestamp_timezone: Option<&str>,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    if depth > MAX_WRITE_RELATION_TYPE_DEPTH {
        return Err(resource_exhausted(
            "write relation Arrow type exceeds the nesting depth limit",
        ));
    }
    charge_write_schema(decoded_bytes, TYPE_ALLOCATION_CHARGE)?;
    if let Some(timezone) = timestamp_timezone {
        charge_write_schema(decoded_bytes, timezone.len())?;
    }
    Ok(())
}

/// Events at the original writer-law traversal. Before events permit the
/// caller to admit opaque source lookup/iteration before it starts. Completed
/// events retain the existing observer trace and partial charge behavior.
#[derive(Clone, Copy, Debug)]
pub enum WriteSchemaVisit<'a> {
    BeforeField(&'a Field),
    BeforeType(&'a DataType),
    Completed,
}

/// Borrow an existing caller context; this function owns no entry, footer,
/// resource budget or new control scope. It uses the same validation grammar
/// as the plain and completed-only observed entry points below.
pub fn validate_write_field_schema_events<E: From<ConnectorError>>(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    validate_field_core(field, depth, decoded_bytes, &mut observe)
}

pub fn validate_write_field_schema(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    // The existing unobserved contract delegates the same traversal. It does
    // not manufacture a PureCompileControl or certify a preparation budget.
    validate_field_core(field, depth, decoded_bytes, &mut |_| Ok(()))
}

/// Validate the same writer domain on the caller's existing observer. The
/// caller owns entry, ordinary/success tails and resource admission; this
/// traversal neither creates a scope nor copies fields. HashMap iteration and
/// logical metadata lookup remain opaque library work, not a claimed internal
/// cooperative quantum or an allocator grant.
pub fn validate_write_field_schema_observed<E: From<ConnectorError>>(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    validate_field_core(field, depth, decoded_bytes, &mut |event| {
        if matches!(event, WriteSchemaVisit::Completed) {
            observe()?;
        }
        Ok(())
    })
}

fn validate_field_core<E: From<ConnectorError>>(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
    observe: &mut impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    // Preserve name, logical metadata, entry count and storage diagnostic order.
    observe(WriteSchemaVisit::BeforeField(field))?;
    validate_write_field_name(field.name())?;
    novarocks_type_contract::field_logical_type(field).map_err(|error| {
        ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
    })?;
    charge_write_field_header(field.name(), field.metadata().len(), decoded_bytes)?;
    observe(WriteSchemaVisit::Completed)?;
    for (key, value) in field.metadata() {
        charge_write_metadata_entry(key, value, decoded_bytes)?;
        observe(WriteSchemaVisit::Completed)?;
    }
    validate_type_core(field.data_type(), depth, decoded_bytes, observe)
}

pub fn validate_write_data_type(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    validate_type_core(data_type, depth, decoded_bytes, &mut |_| Ok(()))
}

/// Borrow the caller's observer for the original type traversal. All child
/// occurrences remain charged, including shared Arrow Field backing.
pub fn validate_write_data_type_observed<E: From<ConnectorError>>(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    validate_type_core(data_type, depth, decoded_bytes, &mut |event| {
        if matches!(event, WriteSchemaVisit::Completed) {
            observe()?;
        }
        Ok(())
    })
}

fn validate_type_core<E: From<ConnectorError>>(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
    observe: &mut impl FnMut(WriteSchemaVisit<'_>) -> Result<(), E>,
) -> Result<(), E> {
    observe(WriteSchemaVisit::BeforeType(data_type))?;
    let timezone = match data_type {
        DataType::Timestamp(_, timezone) => timezone.as_deref(),
        _ => None,
    };
    charge_write_type_header(depth, timezone, decoded_bytes)?;
    observe(WriteSchemaVisit::Completed)?;
    match data_type {
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => validate_field_core(field, depth + 1, decoded_bytes, observe)?,
        DataType::Struct(fields) => {
            for field in fields {
                validate_field_core(field, depth + 1, decoded_bytes, observe)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                validate_field_core(field, depth + 1, decoded_bytes, observe)?;
            }
        }
        DataType::Dictionary(key, value) => {
            validate_type_core(key, depth + 1, decoded_bytes, observe)?;
            validate_type_core(value, depth + 1, decoded_bytes, observe)?;
        }
        DataType::RunEndEncoded(run_ends, values) => {
            validate_field_core(run_ends, depth + 1, decoded_bytes, observe)?;
            validate_field_core(values, depth + 1, decoded_bytes, observe)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests;
