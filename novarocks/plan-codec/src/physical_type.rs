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

//! Exact Arrow-to-native-v1 type translation for final physical plans.

use arrow::datatypes::{DataType, TimeUnit};

/// Validate the legacy SQL shape paired with an exact writer Arrow schema
/// without allocating its protobuf representation.
pub(crate) fn validate_arrow_authoritative_compatibility_type(
    data_type: &DataType,
) -> Result<(), String> {
    arrow_authoritative_wire_depths(data_type).map(|_| ())
}

/// Maximum SQL `TypeDesc` and exact `ArrowPhysicalType` message depths.
///
/// The walk is iterative because it is the guard for both recursive encoders.
/// Map's Arrow path includes its synthetic entries struct and is intentionally
/// charged more deeply than its SQL compatibility descriptor.
pub(crate) fn arrow_authoritative_wire_depths(
    data_type: &DataType,
) -> Result<(usize, usize), String> {
    let mut pending = vec![(data_type, 1_usize, 1_usize, 1_usize)];
    let mut sql_depth = 0_usize;
    let mut arrow_depth = 0_usize;
    while let Some((data_type, sql_prefix, arrow_prefix, logical_depth)) = pending.pop() {
        if logical_depth > novarocks_spi::connector::write_stack::MAX_WRITE_RELATION_TYPE_DEPTH {
            return Err(format!(
                "native wire v1 writer type depth {logical_depth} exceeds the relation limit {}",
                novarocks_spi::connector::write_stack::MAX_WRITE_RELATION_TYPE_DEPTH
            ));
        }
        match data_type {
            DataType::List(field) => {
                sql_depth = sql_depth.max(sql_prefix.saturating_add(2));
                arrow_depth = arrow_depth.max(arrow_prefix.saturating_add(2));
                pending.push((
                    field.data_type(),
                    sql_prefix.saturating_add(2),
                    arrow_prefix.saturating_add(2),
                    logical_depth.saturating_add(1),
                ));
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err("native wire v1 writer map entries must be a struct".into());
                };
                if fields.len() != 2 {
                    return Err(
                        "native wire v1 writer map entries must contain key and value".into(),
                    );
                }
                sql_depth = sql_depth.max(sql_prefix.saturating_add(2));
                arrow_depth = arrow_depth.max(arrow_prefix.saturating_add(5));
                for field in fields {
                    pending.push((
                        field.data_type(),
                        sql_prefix.saturating_add(2),
                        arrow_prefix.saturating_add(5),
                        logical_depth.saturating_add(1),
                    ));
                }
            }
            DataType::Struct(fields) => {
                sql_depth = sql_depth.max(sql_prefix.saturating_add(2));
                arrow_depth = arrow_depth.max(arrow_prefix.saturating_add(2));
                for field in fields {
                    pending.push((
                        field.data_type(),
                        sql_prefix.saturating_add(3),
                        arrow_prefix.saturating_add(3),
                        logical_depth.saturating_add(1),
                    ));
                }
            }
            _ => {
                validate_physical_type(data_type)?;
                sql_depth = sql_depth.max(sql_prefix.saturating_add(1));
                arrow_depth = arrow_depth.max(arrow_prefix.saturating_add(1));
            }
        }
    }
    Ok((sql_depth, arrow_depth))
}

/// Validate exact v1 type expressibility without allocating a protobuf value.
/// How deep a nested type may be before native wire v1 refuses it.
const MAX_NESTED_TYPE_DEPTH: usize = 16;

/// Whether a nested field is named the way the reader rebuilds it.
///
/// The v1 `TypeDesc` carries a nested type's shape and a struct field's name
/// and nothing else, so the reader rebuilds a list's element as `item`, a
/// map's entries as a non-null `entries` struct of `key` and `value`, and
/// every field inside those as nullable. A field named otherwise, or carrying
/// metadata -- a Parquet field id -- would come back as a different type, so
/// it is refused rather than normalized.
///
/// What a nested field admits is not refused, because the reader only ever
/// widens it: a field the plan says is never null comes back saying it may be,
/// which is the same direction nullability travels everywhere else in a plan.
/// A map's entries are the exception -- the reader builds that one non-null,
/// so a plan that says otherwise would be narrowed.
fn require_canonical_nested_field(
    field: &arrow::datatypes::Field,
    name: &str,
    entries: bool,
    whole: &DataType,
) -> Result<(), String> {
    // The logical type is the one entry that says what the field is rather
    // than where it came from, so it travels; a provider's own bookkeeping
    // does not, because the reader gives it back without it.
    let only_logical_metadata = field
        .metadata()
        .keys()
        .all(|key| key == novarocks_types::logical::NR_LOGICAL_TYPE_KEY);
    if field.name() != name || !only_logical_metadata || (entries && field.is_nullable()) {
        return Err(format!(
            "native wire v1 TypeDesc cannot preserve nested Arrow field naming and metadata for {whole:?}"
        ));
    }
    Ok(())
}

pub(crate) fn validate_physical_type(data_type: &DataType) -> Result<(), String> {
    validate_physical_type_at(data_type, 1)
}

fn validate_physical_type_at(data_type: &DataType, depth: usize) -> Result<(), String> {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return Err(format!(
            "native wire v1 TypeDesc nesting exceeds depth {MAX_NESTED_TYPE_DEPTH}"
        ));
    }
    match data_type {
        DataType::List(element) => {
            require_canonical_nested_field(element, "item", false, data_type)?;
            return validate_physical_type_at(element.data_type(), depth + 1);
        }
        DataType::Struct(fields) => {
            for field in fields {
                let name = field.name().clone();
                require_canonical_nested_field(field, &name, false, data_type)?;
                validate_physical_type_at(field.data_type(), depth + 1)?;
            }
            return Ok(());
        }
        DataType::Map(entries, sorted) => {
            if *sorted {
                return Err(format!(
                    "native wire v1 TypeDesc cannot preserve a sorted map for {data_type:?}"
                ));
            }
            require_canonical_nested_field(entries, "entries", true, data_type)?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "native wire v1 map entries must be a struct for {data_type:?}"
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "native wire v1 map entries must contain key and value for {data_type:?}"
                ));
            }
            require_canonical_nested_field(&fields[0], "key", false, data_type)?;
            require_canonical_nested_field(&fields[1], "value", false, data_type)?;
            validate_physical_type_at(fields[0].data_type(), depth + 1)?;
            return validate_physical_type_at(fields[1].data_type(), depth + 1);
        }
        _ => {}
    }
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(16)
        | DataType::Time64(TimeUnit::Microsecond) => Ok(()),
        DataType::Decimal128(precision, scale) => validate_decimal(*precision, *scale, 38),
        DataType::Decimal256(precision, scale) => validate_decimal(*precision, *scale, 76),
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, zone) => {
            if zone.as_ref().is_some_and(|zone| zone.is_empty()) {
                return Err("native wire v1 cannot encode an empty timestamp time zone".into());
            }
            Ok(())
        }
        DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Err(format!(
            "native wire v1 TypeDesc cannot preserve Arrow list offset width for {data_type:?}"
        )),
        other => Err(format!(
            "native wire v1 TypeDesc cannot preserve Arrow data type {other:?}"
        )),
    }
}

fn validate_decimal(precision: u8, scale: i8, max_precision: u8) -> Result<(), String> {
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "native wire v1 decimal precision {precision} must be between 1 and {max_precision}"
        ));
    }
    if scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(format!(
            "native wire v1 decimal scale {scale} must be between 0 and precision {precision}"
        ));
    }
    Ok(())
}

/// Exact protobuf message depth of a normalized value layout, borrowing its
/// original tree. This is independent of logical depth and local kernel support.
pub(crate) fn value_layout_wire_depth_observed<
    E: From<novarocks_type_contract::CompileControlError>,
>(
    value: &arrow::datatypes::DataType,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<usize, E> {
    use arrow::datatypes::DataType as D;
    observe()?;
    let mut maximum = 1usize;
    let mut child = |value: &D, prefix: usize| -> Result<(), E> {
        let depth = value_layout_wire_depth_observed(value, observe)?;
        maximum = maximum.max(
            prefix
                .checked_add(depth)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?,
        );
        Ok(())
    };
    match value {
        D::List(field) | D::LargeList(field) | D::ListView(field) | D::LargeListView(field) => {
            child(field.data_type(), 2)?;
        }
        D::FixedSizeList(field, _) => child(field.data_type(), 3)?,
        D::Struct(fields) => {
            maximum = 2; // ArrowPhysicalType -> ArrowStructType, even when empty.
            for field in fields {
                let depth = value_layout_wire_depth_observed(field.data_type(), observe)?;
                maximum = maximum.max(
                    3usize
                        .checked_add(depth)
                        .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?,
                );
            }
        }
        D::Union(fields, _) => {
            maximum = 2;
            for (_, field) in fields.iter() {
                let depth = value_layout_wire_depth_observed(field.data_type(), observe)?;
                maximum = maximum.max(
                    4usize
                        .checked_add(depth)
                        .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?,
                );
            }
        }
        D::Map(entries, _) => child(entries.data_type(), 3)?,
        D::Timestamp(..)
        | D::Time32(..)
        | D::Time64(..)
        | D::Decimal32(..)
        | D::Decimal64(..)
        | D::Decimal128(..)
        | D::Decimal256(..) => maximum = 2,
        // Constructors have already rejected runtime encoding wrappers; those
        // types never arrive at this normalized layout boundary.
        _ => {}
    }
    Ok(maximum)
}
