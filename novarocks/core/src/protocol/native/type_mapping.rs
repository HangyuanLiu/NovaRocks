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

//! Native protobuf `TypeDesc` decoder used by BE-owned wire consumption.
//!
//! Encoding belongs to the Frontend native fragment encoder. A `TypeDesc` is
//! a SQL type contract, not a full Arrow schema round-trip: top-level Arrow
//! field names/nullability are outside this boundary, while logical SQL
//! markers are retained in `Field` metadata.

use std::sync::Arc;

use arrow::datatypes::Fields;
use arrow::datatypes::{DataType, Field, TimeUnit};

use novarocks_protocol::common;
use novarocks_types::logical::{LogicalType, field_with_logical_type};

const TIME_UNIT_MICROS: i32 = 2;
const TIME_UNIT_NANOS: i32 = 3;

pub(crate) fn decode_type(desc: &common::TypeDesc) -> Result<DataType, String> {
    decode_type_inner(desc)
}

pub(crate) fn decode_field_type(
    name: &str,
    nullable: bool,
    desc: &common::TypeDesc,
) -> Result<Field, String> {
    let data_type = decode_type_inner(desc)?;
    let field = Field::new(name, data_type, nullable);
    Ok(match logical_type_from_desc(desc) {
        Some(logical_type) => field_with_logical_type(field, logical_type),
        None => field,
    })
}

fn decode_type_inner(desc: &common::TypeDesc) -> Result<DataType, String> {
    use common::type_desc::Kind;

    match desc.kind.as_ref().ok_or("TypeDesc.kind missing")? {
        Kind::Scalar(scalar) => decode_scalar_type(scalar),
        Kind::List(list) => {
            let element = list.element.as_ref().ok_or("ListType.element missing")?;
            Ok(DataType::List(Arc::new(decode_field_type(
                "item", true, element,
            )?)))
        }
        Kind::Map(map) => {
            let key = map.key.as_ref().ok_or("MapType.key missing")?;
            let value = map.value.as_ref().ok_or("MapType.value missing")?;
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(decode_field_type("key", true, key)?),
                    Arc::new(decode_field_type("value", true, value)?),
                ])),
                false,
            );
            Ok(DataType::Map(Arc::new(entries), false))
        }
        Kind::Strct(strct) => {
            let fields = strct
                .fields
                .iter()
                .map(|field| {
                    let field_type = field.r#type.as_ref().ok_or("StructField.type missing")?;
                    Ok(Arc::new(decode_field_type(&field.name, true, field_type)?))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
    }
}

fn decode_scalar_type(scalar: &common::ScalarType) -> Result<DataType, String> {
    use common::PrimitiveType;

    let primitive = PrimitiveType::try_from(scalar.r#type)
        .map_err(|_| format!("unknown primitive type {}", scalar.r#type))?;
    match primitive {
        PrimitiveType::Unspecified => Err("primitive type is unspecified".to_string()),
        PrimitiveType::NullType => Ok(DataType::Null),
        PrimitiveType::Boolean => Ok(DataType::Boolean),
        PrimitiveType::Tinyint => Ok(DataType::Int8),
        PrimitiveType::Smallint => Ok(DataType::Int16),
        PrimitiveType::Int => Ok(DataType::Int32),
        PrimitiveType::Bigint => Ok(DataType::Int64),
        PrimitiveType::Largeint => Ok(DataType::FixedSizeBinary(16)),
        PrimitiveType::Float => Ok(DataType::Float32),
        PrimitiveType::Double => Ok(DataType::Float64),
        PrimitiveType::Decimal32
        | PrimitiveType::Decimal64
        | PrimitiveType::Decimal128
        | PrimitiveType::Decimal256 => decode_decimal_type(primitive, scalar),
        PrimitiveType::Date => Ok(DataType::Date32),
        PrimitiveType::Datetime => {
            let unit = match scalar.time_unit {
                None => TimeUnit::Microsecond,
                Some(TIME_UNIT_MICROS) => TimeUnit::Microsecond,
                Some(TIME_UNIT_NANOS) => TimeUnit::Nanosecond,
                Some(value) => {
                    return Err(format!(
                        "unsupported DATETIME time_unit {value}; only unset/{TIME_UNIT_MICROS}/{TIME_UNIT_NANOS} supported"
                    ));
                }
            };
            Ok(DataType::Timestamp(unit, None))
        }
        PrimitiveType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        PrimitiveType::Varchar | PrimitiveType::Char | PrimitiveType::Json => Ok(DataType::Utf8),
        PrimitiveType::Varbinary
        | PrimitiveType::Binary
        | PrimitiveType::Hll
        | PrimitiveType::Bitmap
        | PrimitiveType::Object
        | PrimitiveType::Percentile => Ok(DataType::Binary),
        PrimitiveType::Variant => Ok(DataType::LargeBinary),
    }
}

fn decode_decimal_type(
    primitive: common::PrimitiveType,
    scalar: &common::ScalarType,
) -> Result<DataType, String> {
    let precision = scalar
        .precision
        .ok_or_else(|| "decimal precision missing".to_string())
        .and_then(|v| u8::try_from(v).map_err(|_| format!("invalid decimal precision {v}")))?;
    let scale = scalar
        .scale
        .ok_or_else(|| "decimal scale missing".to_string())
        .and_then(|v| i8::try_from(v).map_err(|_| format!("invalid decimal scale {v}")))?;
    let (max_precision, label) = match primitive {
        common::PrimitiveType::Decimal32 => (9, "Decimal32"),
        common::PrimitiveType::Decimal64 => (18, "Decimal64"),
        common::PrimitiveType::Decimal128 => (38, "Decimal128"),
        common::PrimitiveType::Decimal256 => (76, "Decimal256"),
        _ => unreachable!(),
    };
    validate_decimal(precision, scale, max_precision, label)?;
    if primitive == common::PrimitiveType::Decimal256 || precision > 38 {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
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

fn logical_type_from_desc(desc: &common::TypeDesc) -> Option<LogicalType> {
    let common::type_desc::Kind::Scalar(scalar) = desc.kind.as_ref()? else {
        return None;
    };
    match common::PrimitiveType::try_from(scalar.r#type).ok()? {
        common::PrimitiveType::Json => Some(LogicalType::Json),
        common::PrimitiveType::Hll => Some(LogicalType::Hll),
        common::PrimitiveType::Bitmap => Some(LogicalType::Bitmap),
        common::PrimitiveType::Object => Some(LogicalType::Object),
        common::PrimitiveType::Percentile => Some(LogicalType::Percentile),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::logical::logical_type_of_field;

    fn scalar(
        primitive: common::PrimitiveType,
        precision: Option<i32>,
        scale: Option<i32>,
        time_unit: Option<i32>,
    ) -> common::TypeDesc {
        common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: primitive as i32,
                len: None,
                precision,
                scale,
                time_unit,
            })),
        }
    }

    #[test]
    fn decoder_preserves_nested_shapes_and_logical_field_metadata() {
        let decimal = scalar(common::PrimitiveType::Decimal128, Some(18), Some(2), None);
        let json = scalar(common::PrimitiveType::Json, None, None, None);
        let list = common::TypeDesc {
            kind: Some(common::type_desc::Kind::List(Box::new(common::ListType {
                element: Some(Box::new(decimal)),
            }))),
        };
        let map = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(scalar(
                    common::PrimitiveType::Varchar,
                    None,
                    None,
                    None,
                ))),
                value: Some(Box::new(list)),
            }))),
        };
        let struct_desc = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Strct(common::StructType {
                fields: vec![
                    common::StructField {
                        name: "metadata".to_string(),
                        r#type: Some(json),
                    },
                    common::StructField {
                        name: "values".to_string(),
                        r#type: Some(map),
                    },
                ],
            })),
        };

        let DataType::Struct(fields) = decode_type(&struct_desc).expect("decode struct") else {
            panic!("expected struct");
        };
        assert_eq!(fields[0].name(), "metadata");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(
            logical_type_of_field(fields[0].as_ref()),
            Some(LogicalType::Json)
        );
        assert!(matches!(fields[1].data_type(), DataType::Map(_, false)));
    }

    #[test]
    fn decoder_maps_datetime_decimal_and_reports_malformed_descriptors() {
        assert_eq!(
            decode_type(&scalar(
                common::PrimitiveType::Datetime,
                None,
                None,
                Some(TIME_UNIT_NANOS),
            ))
            .expect("decode datetime"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            decode_type(&scalar(
                common::PrimitiveType::Decimal256,
                Some(41),
                Some(3),
                None,
            ))
            .expect("decode decimal"),
            DataType::Decimal256(41, 3)
        );
        assert_eq!(
            decode_type(&common::TypeDesc { kind: None }).unwrap_err(),
            "TypeDesc.kind missing"
        );
        assert_eq!(
            decode_type(&scalar(
                common::PrimitiveType::Decimal64,
                Some(19),
                Some(0),
                None,
            ))
            .unwrap_err(),
            "Decimal64 precision 19 must be between 1 and 18"
        );
        assert_eq!(
            decode_type(&scalar(
                common::PrimitiveType::Datetime,
                None,
                None,
                Some(99),
            ))
            .unwrap_err(),
            "unsupported DATETIME time_unit 99; only unset/2/3 supported"
        );
    }
}
