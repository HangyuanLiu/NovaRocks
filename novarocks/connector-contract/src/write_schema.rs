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

pub fn validate_write_field_schema(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    validate_write_field_name(field.name())?;
    novarocks_type_contract::field_logical_type(field).map_err(|error| {
        ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
    })?;
    if field.metadata().len() > MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD {
        return Err(resource_exhausted(
            "write relation field metadata exceeds the entry limit",
        ));
    }
    charge_write_schema(
        decoded_bytes,
        WRITE_FIELD_ALLOCATION_CHARGE + field.name().len(),
    )?;
    for (key, value) in field.metadata() {
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
        )?;
    }
    validate_write_data_type(field.data_type(), depth, decoded_bytes)
}

pub fn validate_write_data_type(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    if depth > MAX_WRITE_RELATION_TYPE_DEPTH {
        return Err(resource_exhausted(
            "write relation Arrow type exceeds the nesting depth limit",
        ));
    }
    charge_write_schema(decoded_bytes, TYPE_ALLOCATION_CHARGE)?;
    match data_type {
        DataType::Timestamp(_, Some(timezone)) => {
            charge_write_schema(decoded_bytes, timezone.len())?;
        }
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => validate_write_field_schema(field, depth + 1, decoded_bytes)?,
        DataType::Struct(fields) => {
            for field in fields {
                validate_write_field_schema(field, depth + 1, decoded_bytes)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                validate_write_field_schema(field, depth + 1, decoded_bytes)?;
            }
        }
        DataType::Dictionary(key, value) => {
            validate_write_data_type(key, depth + 1, decoded_bytes)?;
            validate_write_data_type(value, depth + 1, decoded_bytes)?;
        }
        DataType::RunEndEncoded(run_ends, values) => {
            validate_write_field_schema(run_ends, depth + 1, decoded_bytes)?;
            validate_write_field_schema(values, depth + 1, decoded_bytes)?;
        }
        _ => {}
    }
    Ok(())
}
