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

//! Bidirectional NovaRocks native protobuf `TypeDesc` codec. `TypeDesc` is a SQL
//! type contract, not a full Arrow schema round-trip: top-level Arrow field
//! names/nullability are outside this boundary, and logical SQL markers require
//! `Field` metadata.

use std::sync::Arc;

use arrow::datatypes::Fields;
use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::proto::common;
use crate::types::logical::field_with_logical_type;
use crate::types::logical::{LogicalType, logical_type_of_field};

const TIME_UNIT_MICROS: i32 = 2;
const TIME_UNIT_NANOS: i32 = 3;

pub(crate) fn encode_type(dt: &DataType) -> Result<common::TypeDesc, String> {
    encode_type_inner(dt, None)
}

#[cfg(test)]
pub(crate) fn encode_field_type(field: &Field) -> Result<common::TypeDesc, String> {
    encode_type_inner(field.data_type(), Some(field))
}

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
                element: Some(Box::new(encode_type_inner(
                    item.data_type(),
                    Some(item.as_ref()),
                )?)),
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
                    Some(fields[0].as_ref()),
                )?)),
                value: Some(Box::new(encode_type_inner(
                    fields[1].data_type(),
                    Some(fields[1].as_ref()),
                )?)),
            }))
        }
        DataType::Struct(fields) => Kind::Strct(common::StructType {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(common::StructField {
                        name: field.name().to_string(),
                        r#type: Some(encode_type_inner(field.data_type(), Some(field.as_ref()))?),
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
        DataType::Timestamp(unit, _) => {
            let time_unit = match unit {
                TimeUnit::Microsecond => None,
                TimeUnit::Nanosecond => Some(TIME_UNIT_NANOS),
                other => {
                    return Err(format!(
                        "unsupported timestamp unit {other:?}; only Microsecond/Nanosecond supported"
                    ));
                }
            };
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

    Ok(scalar_desc(primitive, precision, scale, time_unit))
}

fn scalar_desc(
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

fn logical_primitive(logical_type: LogicalType) -> common::PrimitiveType {
    match logical_type {
        LogicalType::Json => common::PrimitiveType::Json,
        LogicalType::Hll => common::PrimitiveType::Hll,
        LogicalType::Bitmap => common::PrimitiveType::Bitmap,
        LogicalType::Object => common::PrimitiveType::Object,
        LogicalType::Percentile => common::PrimitiveType::Percentile,
    }
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
    use prost::Message;

    fn roundtrip_message<M>(value: &M) -> M
    where
        M: Message + Default,
    {
        M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
    }

    fn scalar_primitive(desc: &common::TypeDesc) -> common::PrimitiveType {
        let common::type_desc::Kind::Scalar(scalar) = desc.kind.as_ref().expect("type kind") else {
            panic!("expected scalar TypeDesc");
        };
        common::PrimitiveType::try_from(scalar.r#type).expect("known primitive")
    }

    #[test]
    fn recursive_arrow_type_round_trips_through_type_desc() {
        let data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("amount", DataType::Decimal128(18, 2), true)),
                Arc::new(Field::new(
                    "ids",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                )),
            ])),
            true,
        )));

        let encoded = encode_type(&data_type).expect("encode recursive type");
        let decoded_proto: common::TypeDesc = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded_proto);
        assert_eq!(
            decode_type(&decoded_proto).expect("decode recursive type"),
            data_type
        );
    }

    #[test]
    fn map_type_round_trips_through_type_desc() {
        let data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                    Arc::new(Field::new("value", DataType::Decimal128(12, 4), true)),
                ])),
                false,
            )),
            false,
        );

        let encoded = encode_type(&data_type).expect("encode map type");
        let decoded_proto: common::TypeDesc = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded_proto);
        assert_eq!(
            decode_type(&decoded_proto).expect("decode map type"),
            data_type
        );
    }

    #[test]
    fn metadata_logical_fields_encode_to_logical_primitives() {
        let cases = [
            (
                field_with_logical_type(
                    Field::new("json_payload", DataType::Utf8, true),
                    LogicalType::Json,
                ),
                common::PrimitiveType::Json,
                DataType::Utf8,
                Some(LogicalType::Json),
            ),
            (
                field_with_logical_type(
                    Field::new("hll_state", DataType::Binary, true),
                    LogicalType::Hll,
                ),
                common::PrimitiveType::Hll,
                DataType::Binary,
                Some(LogicalType::Hll),
            ),
            (
                field_with_logical_type(
                    Field::new("bitmap_state", DataType::Binary, true),
                    LogicalType::Bitmap,
                ),
                common::PrimitiveType::Bitmap,
                DataType::Binary,
                Some(LogicalType::Bitmap),
            ),
            (
                field_with_logical_type(
                    Field::new("object_state", DataType::Binary, true),
                    LogicalType::Object,
                ),
                common::PrimitiveType::Object,
                DataType::Binary,
                Some(LogicalType::Object),
            ),
            (
                field_with_logical_type(
                    Field::new("percentile_state", DataType::Binary, true),
                    LogicalType::Percentile,
                ),
                common::PrimitiveType::Percentile,
                DataType::Binary,
                Some(LogicalType::Percentile),
            ),
            (
                Field::new("variant_payload", DataType::LargeBinary, true),
                common::PrimitiveType::Variant,
                DataType::LargeBinary,
                None,
            ),
            (
                Field::new("large_int", DataType::FixedSizeBinary(16), true),
                common::PrimitiveType::Largeint,
                DataType::FixedSizeBinary(16),
                None,
            ),
        ];

        for (field, expected_primitive, expected_type, expected_logical) in cases {
            let encoded = encode_field_type(&field).expect("encode logical field");
            assert_eq!(scalar_primitive(&encoded), expected_primitive);

            let decoded = decode_field_type(field.name(), field.is_nullable(), &encoded)
                .expect("decode field");
            assert_eq!(decoded.data_type(), &expected_type);
            assert_eq!(logical_type_of_field(&decoded), expected_logical);
        }
    }

    #[test]
    fn nested_logical_field_metadata_survives_decode_type() {
        let data_type = DataType::Struct(Fields::from(vec![Arc::new(field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        ))]));

        let encoded = encode_type(&data_type).expect("encode struct with logical child");
        let decoded = decode_type(&roundtrip_message(&encoded)).expect("decode logical child");

        let DataType::Struct(fields) = decoded else {
            panic!("expected struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(
            logical_type_of_field(fields[0].as_ref()),
            Some(LogicalType::Json)
        );
    }

    #[test]
    fn invalid_decimal_type_widths_are_rejected() {
        let err = encode_type(&DataType::Decimal128(39, 0))
            .expect_err("Decimal128 precision above 38 must fail");
        assert!(err.contains("Decimal128"));
        assert!(err.contains("precision"));

        let err = encode_type(&DataType::Decimal256(77, 0))
            .expect_err("Decimal256 precision above 76 must fail");
        assert!(err.contains("Decimal256"));
        assert!(err.contains("precision"));
    }

    #[test]
    fn unsupported_timestamp_unit_reports_clear_error() {
        let err = encode_type(&DataType::Timestamp(TimeUnit::Second, None))
            .expect_err("second timestamp rejected");

        assert!(err.contains("unsupported timestamp unit"));
    }

    #[test]
    fn unsupported_time64_unit_reports_clear_error() {
        let err = encode_type(&DataType::Time64(TimeUnit::Nanosecond))
            .expect_err("nanosecond Time64 rejected");

        assert!(err.contains("unsupported Time64 unit"));
    }

    #[test]
    fn malformed_native_type_desc_reports_stable_first_errors() {
        assert_eq!(
            decode_type(&common::TypeDesc { kind: None }).unwrap_err(),
            "TypeDesc.kind missing"
        );

        let missing_element = common::TypeDesc {
            kind: Some(common::type_desc::Kind::List(Box::new(common::ListType {
                element: None,
            }))),
        };
        assert_eq!(
            decode_type(&missing_element).unwrap_err(),
            "ListType.element missing"
        );

        let unknown = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: i32::MAX,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            })),
        };
        assert_eq!(
            decode_type(&unknown).unwrap_err(),
            format!("unknown primitive type {}", i32::MAX)
        );

        let missing_precision = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: common::PrimitiveType::Decimal128 as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            })),
        };
        assert_eq!(
            decode_type(&missing_precision).unwrap_err(),
            "decimal precision missing"
        );
    }

    #[test]
    fn malformed_decimal_and_datetime_descs_report_exact_errors() {
        struct DecimalCase {
            primitive: common::PrimitiveType,
            precision: Option<i32>,
            scale: Option<i32>,
            expected: &'static str,
        }

        let decimal_cases = [
            DecimalCase {
                primitive: common::PrimitiveType::Decimal32,
                precision: Some(10),
                scale: Some(0),
                expected: "Decimal32 precision 10 must be between 1 and 9",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal64,
                precision: Some(19),
                scale: Some(0),
                expected: "Decimal64 precision 19 must be between 1 and 18",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal128,
                precision: Some(39),
                scale: Some(0),
                expected: "Decimal128 precision 39 must be between 1 and 38",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal256,
                precision: Some(77),
                scale: Some(0),
                expected: "Decimal256 precision 77 must be between 1 and 76",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal32,
                precision: Some(9),
                scale: None,
                expected: "decimal scale missing",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal64,
                precision: Some(18),
                scale: None,
                expected: "decimal scale missing",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal128,
                precision: Some(38),
                scale: None,
                expected: "decimal scale missing",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal256,
                precision: Some(76),
                scale: None,
                expected: "decimal scale missing",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal32,
                precision: Some(9),
                scale: Some(-1),
                expected: "Decimal32 scale -1 must be between 0 and precision 9",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal64,
                precision: Some(18),
                scale: Some(-1),
                expected: "Decimal64 scale -1 must be between 0 and precision 18",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal128,
                precision: Some(38),
                scale: Some(-1),
                expected: "Decimal128 scale -1 must be between 0 and precision 38",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal256,
                precision: Some(76),
                scale: Some(-1),
                expected: "Decimal256 scale -1 must be between 0 and precision 76",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal32,
                precision: Some(8),
                scale: Some(9),
                expected: "Decimal32 scale 9 must be between 0 and precision 8",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal64,
                precision: Some(17),
                scale: Some(18),
                expected: "Decimal64 scale 18 must be between 0 and precision 17",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal128,
                precision: Some(37),
                scale: Some(38),
                expected: "Decimal128 scale 38 must be between 0 and precision 37",
            },
            DecimalCase {
                primitive: common::PrimitiveType::Decimal256,
                precision: Some(75),
                scale: Some(76),
                expected: "Decimal256 scale 76 must be between 0 and precision 75",
            },
        ];

        for case in decimal_cases {
            let desc = scalar_desc(case.primitive, case.precision, case.scale, None);
            assert_eq!(decode_type(&desc).unwrap_err(), case.expected);
        }

        let datetime = scalar_desc(common::PrimitiveType::Datetime, None, None, Some(99));
        assert_eq!(
            decode_type(&datetime).unwrap_err(),
            "unsupported DATETIME time_unit 99; only unset/2/3 supported"
        );
    }

    #[test]
    fn malformed_map_and_struct_descs_report_stable_first_errors() {
        let valid_int = scalar_desc(common::PrimitiveType::Int, None, None, None);
        let malformed = common::TypeDesc { kind: None };

        let map_key_missing_before_malformed_value = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Map(Box::new(common::MapType {
                key: None,
                value: Some(Box::new(malformed.clone())),
            }))),
        };
        assert_eq!(
            decode_type(&map_key_missing_before_malformed_value).unwrap_err(),
            "MapType.key missing"
        );

        let map_value_missing_after_valid_key = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(valid_int.clone())),
                value: None,
            }))),
        };
        assert_eq!(
            decode_type(&map_value_missing_after_valid_key).unwrap_err(),
            "MapType.value missing"
        );

        let struct_first_type_missing_before_later_malformed = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Strct(common::StructType {
                fields: vec![
                    common::StructField {
                        name: "first".to_string(),
                        r#type: None,
                    },
                    common::StructField {
                        name: "later".to_string(),
                        r#type: Some(malformed),
                    },
                ],
            })),
        };
        assert_eq!(
            decode_type(&struct_first_type_missing_before_later_malformed).unwrap_err(),
            "StructField.type missing"
        );

        let struct_later_type_missing_after_valid_first = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Strct(common::StructType {
                fields: vec![
                    common::StructField {
                        name: "first".to_string(),
                        r#type: Some(valid_int),
                    },
                    common::StructField {
                        name: "later".to_string(),
                        r#type: None,
                    },
                ],
            })),
        };
        assert_eq!(
            decode_type(&struct_later_type_missing_after_valid_first).unwrap_err(),
            "StructField.type missing"
        );
    }
}
