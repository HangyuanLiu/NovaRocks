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
use novarocks_proto_models::common;

/// Encode an Arrow type only when native wire v1 preserves its exact identity.
///
/// The v1 `TypeDesc` is a SQL type carrier. It cannot preserve Arrow offset
/// width, nested field nullability, field metadata, dictionary identity, or
/// several physical primitives. Those types are rejected instead of being
/// normalized into a nearby SQL type.
pub(crate) fn encode_physical_type(data_type: &DataType) -> Result<common::TypeDesc, String> {
    validate_physical_type(data_type)?;
    use common::PrimitiveType;

    let (primitive, precision, scale, time_unit, time_zone) = match data_type {
        DataType::Null => (PrimitiveType::NullType, None, None, None, None),
        DataType::Boolean => (PrimitiveType::Boolean, None, None, None, None),
        DataType::Int8 => (PrimitiveType::Tinyint, None, None, None, None),
        DataType::Int16 => (PrimitiveType::Smallint, None, None, None, None),
        DataType::Int32 => (PrimitiveType::Int, None, None, None, None),
        DataType::Int64 => (PrimitiveType::Bigint, None, None, None, None),
        DataType::Float32 => (PrimitiveType::Float, None, None, None, None),
        DataType::Float64 => (PrimitiveType::Double, None, None, None, None),
        DataType::Decimal128(precision, scale) => (
            PrimitiveType::Decimal128,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
            None,
            None,
        ),
        DataType::Date32 => (PrimitiveType::Date, None, None, None, None),
        DataType::Timestamp(unit, zone) => {
            let time_unit = match unit {
                TimeUnit::Microsecond => None,
                TimeUnit::Nanosecond => Some(3),
                _ => unreachable!("validated timestamp unit became unsupported"),
            };
            (
                PrimitiveType::Datetime,
                None,
                None,
                time_unit,
                zone.as_ref().map(ToString::to_string),
            )
        }
        DataType::Time64(TimeUnit::Microsecond) => (PrimitiveType::Time, None, None, None, None),
        DataType::Utf8 => (PrimitiveType::Varchar, None, None, None, None),
        DataType::Binary => (PrimitiveType::Varbinary, None, None, None, None),
        DataType::FixedSizeBinary(16) => (PrimitiveType::Largeint, None, None, None, None),
        other => unreachable!("validated physical type became unsupported: {other:?}"),
    };

    Ok(common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: primitive as i32,
            len: None,
            precision,
            scale,
            time_unit,
            time_zone,
        })),
    })
}

/// Encode the SQL compatibility descriptor for a value whose complete Arrow
/// type is carried by a colocated, mandatory `ArrowPhysicalSchema`.
///
/// Native v1 cannot preserve nested Arrow field nullability and metadata in
/// `TypeDesc`. Writer relation nodes also carry one exact Arrow schema, which
/// is the execution authority; their legacy output-column descriptor is only
/// an indexable SQL shape. Keep this encoder private to that paired carrier.
pub(crate) fn encode_arrow_authoritative_compatibility_type(
    data_type: &DataType,
) -> Result<common::TypeDesc, String> {
    validate_arrow_authoritative_compatibility_type(data_type)?;
    encode_arrow_authoritative_compatibility_type_inner(data_type)
}

fn encode_arrow_authoritative_compatibility_type_inner(
    data_type: &DataType,
) -> Result<common::TypeDesc, String> {
    use common::type_desc::Kind;

    let kind = match data_type {
        DataType::List(field) => Kind::List(Box::new(common::ListType {
            element: Some(Box::new(
                encode_arrow_authoritative_compatibility_type_inner(field.data_type())?,
            )),
        })),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("native wire v1 writer map entries must be a struct".into());
            };
            if fields.len() != 2 {
                return Err("native wire v1 writer map entries must contain key and value".into());
            }
            Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(
                    encode_arrow_authoritative_compatibility_type_inner(fields[0].data_type())?,
                )),
                value: Some(Box::new(
                    encode_arrow_authoritative_compatibility_type_inner(fields[1].data_type())?,
                )),
            }))
        }
        DataType::Struct(fields) => Kind::Strct(common::StructType {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(common::StructField {
                        name: field.name().clone(),
                        r#type: Some(encode_arrow_authoritative_compatibility_type_inner(
                            field.data_type(),
                        )?),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        }),
        _ => return encode_physical_type(data_type),
    };
    Ok(common::TypeDesc { kind: Some(kind) })
}

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
pub(crate) fn validate_physical_type(data_type: &DataType) -> Result<(), String> {
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
        | DataType::FixedSizeBinary(16)
        | DataType::Time64(TimeUnit::Microsecond) => Ok(()),
        DataType::Decimal128(precision, scale) => validate_decimal(*precision, *scale, 38),
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, zone) => {
            if zone.as_ref().is_some_and(|zone| zone.is_empty()) {
                return Err("native wire v1 cannot encode an empty timestamp time zone".into());
            }
            Ok(())
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        | DataType::Struct(_) => Err(format!(
            "native wire v1 TypeDesc cannot preserve nested Arrow field nullability and metadata for {data_type:?}"
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
