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
use arrow_schema::{DataType, Field, Schema};

pub const MAX_WRITE_RELATION_FIELD_NAME_BYTES: usize = 1_024;
pub const MAX_WRITE_RELATION_TYPE_DEPTH: usize = 32;
pub const MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD: usize = 64;
pub const MAX_WRITE_RELATION_METADATA_KEY_BYTES: usize = 1_024;
pub const MAX_WRITE_RELATION_METADATA_VALUE_BYTES: usize = 64 * 1_024;
pub const MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES: usize = 16 * 1_024 * 1_024;

pub const WRITE_FIELD_ALLOCATION_CHARGE: usize = 128;
const TYPE_ALLOCATION_CHARGE: usize = 64;

/// Compare every Arrow physical field attribute recursively. Arrow's built-in
/// `Field::eq` deliberately ignores dictionary ids and dictionary ordering,
/// which is appropriate for logical schema compatibility but not for a frozen
/// internal-relation contract.
pub fn arrow_schemas_exact(left: &Schema, right: &Schema) -> bool {
    left.metadata() == right.metadata()
        && left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| arrow_fields_exact(left, right))
}

pub fn arrow_fields_exact(left: &Field, right: &Field) -> bool {
    #[allow(deprecated)]
    let dictionary_ids_equal = left.dict_id() == right.dict_id();
    left.name() == right.name()
        && left.is_nullable() == right.is_nullable()
        && left.metadata() == right.metadata()
        && dictionary_ids_equal
        && left.dict_is_ordered() == right.dict_is_ordered()
        && arrow_data_types_exact(left.data_type(), right.data_type())
}

pub fn arrow_data_types_exact(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(left), DataType::List(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            arrow_fields_exact(left, right)
        }
        (
            DataType::FixedSizeList(left_field, left_size),
            DataType::FixedSizeList(right_field, right_size),
        ) => left_size == right_size && arrow_fields_exact(left_field, right_field),
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| arrow_fields_exact(left, right))
        }
        (DataType::Union(left_fields, left_mode), DataType::Union(right_fields, right_mode)) => {
            left_mode == right_mode
                && left_fields.len() == right_fields.len()
                && left_fields.iter().zip(right_fields.iter()).all(
                    |((left_id, left), (right_id, right))| {
                        left_id == right_id && arrow_fields_exact(left, right)
                    },
                )
        }
        (
            DataType::Dictionary(left_key, left_value),
            DataType::Dictionary(right_key, right_value),
        ) => {
            arrow_data_types_exact(left_key, right_key)
                && arrow_data_types_exact(left_value, right_value)
        }
        (
            DataType::Map(left_entries, left_ordered),
            DataType::Map(right_entries, right_ordered),
        ) => left_ordered == right_ordered && arrow_fields_exact(left_entries, right_entries),
        (
            DataType::RunEndEncoded(left_runs, left_values),
            DataType::RunEndEncoded(right_runs, right_values),
        ) => {
            arrow_fields_exact(left_runs, right_runs)
                && arrow_fields_exact(left_values, right_values)
        }
        _ => left == right,
    }
}

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

/// Rebuild bounded physical fields without retaining excess collection/string
/// capacity or unrelated backing through nested Arc fields. Callers validate
/// the aggregate schema before invoking this copy.
pub(crate) fn owned_write_field(field: &Field) -> Result<Field, ConnectorError> {
    // This constructor is required to preserve Arrow's physical dictionary ID
    // and ordering; ordinary Field equality/constructors omit those attributes.
    #[allow(deprecated)]
    let copy = Field::new_dict(
        field.name().to_owned(),
        owned_write_type(field.data_type())?,
        field.is_nullable(),
        field.dict_id().unwrap_or(0),
        field.dict_is_ordered().unwrap_or(false),
    );
    Ok(copy.with_metadata(
        field
            .metadata()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    ))
}

fn owned_write_type(data_type: &DataType) -> Result<DataType, ConnectorError> {
    use std::sync::Arc;
    let field = |field: &Field| owned_write_field(field).map(Arc::new);
    Ok(match data_type {
        DataType::Timestamp(unit, zone) => DataType::Timestamp(
            *unit,
            zone.as_ref().map(|zone| Arc::<str>::from(zone.as_ref())),
        ),
        DataType::List(item) => DataType::List(field(item)?),
        DataType::ListView(item) => DataType::ListView(field(item)?),
        DataType::LargeList(item) => DataType::LargeList(field(item)?),
        DataType::LargeListView(item) => DataType::LargeListView(field(item)?),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(field(item)?, *size),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|item| field(item))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::Union(fields, mode) => {
            let (ids, fields): (Vec<_>, Vec<_>) = fields
                .iter()
                .map(|(id, item)| field(item).map(|item| (id, item)))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            DataType::Union(
                arrow_schema::UnionFields::try_new(ids, fields).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "writer recipe has invalid union field identities",
                    )
                })?,
                *mode,
            )
        }
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(owned_write_type(key)?),
            Box::new(owned_write_type(value)?),
        ),
        DataType::Map(entries, sorted) => DataType::Map(field(entries)?, *sorted),
        DataType::RunEndEncoded(runs, values) => {
            DataType::RunEndEncoded(field(runs)?, field(values)?)
        }
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => data_type.clone(),
    })
}
