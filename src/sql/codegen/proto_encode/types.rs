use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};

use crate::proto::common;
use crate::types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

const TIME_UNIT_MICROS: i32 = 2;
const TIME_UNIT_NANOS: i32 = 3;

pub(crate) fn encode_type(dt: &DataType) -> Result<common::TypeDesc, String> {
    encode_type_inner(dt, None)
}

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
            validate_decimal(*precision, *scale)?;
            (
                PrimitiveType::Decimal128,
                Some(i32::from(*precision)),
                Some(i32::from(*scale)),
                None,
            )
        }
        DataType::Decimal256(precision, scale) => {
            validate_decimal(*precision, *scale)?;
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
        DataType::Time64(_) => (PrimitiveType::Time, None, None, None),
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
    validate_decimal(precision, scale)?;
    if primitive == common::PrimitiveType::Decimal256 || precision > 38 {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

fn validate_decimal(precision: u8, scale: i8) -> Result<(), String> {
    if precision == 0 {
        return Err("decimal precision must be positive".to_string());
    }
    if scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(format!(
            "decimal scale {scale} must be between 0 and precision {precision}"
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
