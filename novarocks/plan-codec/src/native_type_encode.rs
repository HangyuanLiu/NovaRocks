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

//! Arrow-to-native type translation for the Native boundary.

use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_proto_models::common;
use novarocks_types::logical::{LogicalType, logical_type_of_field};

pub fn encode_type(dt: &DataType) -> Result<common::TypeDesc, String> {
    encode_type_inner(dt, None)
}

fn encode_type_inner(dt: &DataType, field: Option<&Field>) -> Result<common::TypeDesc, String> {
    if let Some(logical_type) = field.and_then(logical_type_of_field) {
        return Ok(scalar_desc(
            logical_primitive(logical_type),
            None,
            None,
            None,
        ));
    }
    use common::type_desc::Kind;
    let kind = match dt {
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            Kind::List(Box::new(common::ListType {
                element: Some(Box::new(encode_type_inner(item.data_type(), Some(item))?)),
            }))
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "MAP logical entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "MAP logical entries field must have exactly 2 children, got {}",
                    fields.len()
                ));
            }
            Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(encode_type_inner(
                    fields[0].data_type(),
                    Some(&fields[0]),
                )?)),
                value: Some(Box::new(encode_type_inner(
                    fields[1].data_type(),
                    Some(&fields[1]),
                )?)),
            }))
        }
        DataType::Struct(fields) => Kind::Strct(common::StructType {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(common::StructField {
                        name: field.name().to_string(),
                        r#type: Some(encode_type_inner(field.data_type(), Some(field))?),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        }),
        _ => return encode_scalar_type(dt),
    };
    Ok(common::TypeDesc { kind: Some(kind) })
}

fn encode_scalar_type(dt: &DataType) -> Result<common::TypeDesc, String> {
    use common::PrimitiveType;
    let mut time_zone = None;
    let (primitive, precision, scale, time_unit) = match dt {
        DataType::Null => (PrimitiveType::NullType, None, None, None),
        DataType::Boolean => (PrimitiveType::Boolean, None, None, None),
        DataType::Int8 => (PrimitiveType::Tinyint, None, None, None),
        DataType::Int16 => (PrimitiveType::Smallint, None, None, None),
        DataType::Int32 => (PrimitiveType::Int, None, None, None),
        DataType::Int64 => (PrimitiveType::Bigint, None, None, None),
        DataType::Float32 => (PrimitiveType::Float, None, None, None),
        DataType::Float64 => (PrimitiveType::Double, None, None, None),
        DataType::Decimal128(precision, scale) => {
            validate_decimal(*precision, *scale, 38, "Decimal128")?;
            (
                PrimitiveType::Decimal128,
                Some(i32::from(*precision)),
                Some(i32::from(*scale)),
                None,
            )
        }
        DataType::Decimal256(precision, scale) => {
            validate_decimal(*precision, *scale, 76, "Decimal256")?;
            (
                PrimitiveType::Decimal256,
                Some(i32::from(*precision)),
                Some(i32::from(*scale)),
                None,
            )
        }
        DataType::Date32 => (PrimitiveType::Date, None, None, None),
        DataType::Timestamp(unit, zone) => {
            let time_unit = match unit {
                TimeUnit::Microsecond => None,
                TimeUnit::Nanosecond => Some(3),
                other => {
                    return Err(format!(
                        "unsupported timestamp unit {other:?}; only Microsecond/Nanosecond supported"
                    ));
                }
            };
            // A zoned timestamp and an unzoned one are different types.
            // Dropping the zone here rewrote a column's type across the process
            // boundary, and the receiver then rejected the batch it was sent.
            time_zone = zone.as_ref().map(|zone| zone.to_string());
            (PrimitiveType::Datetime, None, None, time_unit)
        }
        DataType::Time64(TimeUnit::Microsecond) => (PrimitiveType::Time, None, None, None),
        DataType::Time64(unit) => {
            return Err(format!(
                "unsupported Time64 unit {unit:?}; only Microsecond supported"
            ));
        }
        DataType::Utf8 | DataType::LargeUtf8 => (PrimitiveType::Varchar, None, None, None),
        DataType::Binary => (PrimitiveType::Varbinary, None, None, None),
        DataType::LargeBinary => (PrimitiveType::Variant, None, None, None),
        DataType::FixedSizeBinary(16) => (PrimitiveType::Largeint, None, None, None),
        other => {
            return Err(format!(
                "Arrow-to-native TypeDesc conversion does not support data type {other:?}"
            ));
        }
    };
    Ok(scalar_desc_with_zone(
        primitive, precision, scale, time_unit, time_zone,
    ))
}

fn scalar_desc(
    primitive: common::PrimitiveType,
    precision: Option<i32>,
    scale: Option<i32>,
    time_unit: Option<i32>,
) -> common::TypeDesc {
    scalar_desc_with_zone(primitive, precision, scale, time_unit, None)
}

fn scalar_desc_with_zone(
    primitive: common::PrimitiveType,
    precision: Option<i32>,
    scale: Option<i32>,
    time_unit: Option<i32>,
    time_zone: Option<String>,
) -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: primitive as i32,
            len: None,
            precision,
            scale,
            time_unit,
            time_zone,
        })),
    }
}

fn validate_decimal(
    precision: u8,
    scale: i8,
    max_precision: u8,
    label: &str,
) -> Result<(), String> {
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "{label} precision {precision} must be between 1 and {max_precision}"
        ));
    }
    if scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(format!(
            "{label} scale {scale} must be between 0 and precision {precision}"
        ));
    }
    Ok(())
}

fn logical_primitive(logical_type: LogicalType) -> common::PrimitiveType {
    match logical_type {
        LogicalType::Json => common::PrimitiveType::Json,
        LogicalType::Hll => common::PrimitiveType::Hll,
        LogicalType::Bitmap => common::PrimitiveType::Bitmap,
        LogicalType::Object => common::PrimitiveType::Object,
        LogicalType::Percentile => common::PrimitiveType::Percentile,
    }
}
