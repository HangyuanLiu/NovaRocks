use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::common::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use crate::thrift::exprs;
use crate::thrift::types;
use crate::types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

const THRIFT_TIME_UNIT_MICROS: i32 = 2;
pub(crate) const THRIFT_TIME_UNIT_NANOS: i32 = 3;

pub(crate) fn thrift_time_unit_for_arrow(unit: TimeUnit) -> Result<Option<i32>, String> {
    match unit {
        TimeUnit::Microsecond => Ok(None),
        TimeUnit::Nanosecond => Ok(Some(THRIFT_TIME_UNIT_NANOS)),
        other => Err(format!(
            "unsupported timestamp unit {other:?} for thrift descriptor; only Microsecond/Nanosecond supported"
        )),
    }
}

pub(crate) fn logical_type_to_primitive(logical_type: LogicalType) -> types::TPrimitiveType {
    match logical_type {
        LogicalType::Json => types::TPrimitiveType::JSON,
        LogicalType::Hll => types::TPrimitiveType::HLL,
        LogicalType::Bitmap | LogicalType::Object => types::TPrimitiveType::OBJECT,
        LogicalType::Percentile => types::TPrimitiveType::PERCENTILE,
    }
}

pub(crate) fn field_logical_primitive(field: &Field) -> Option<types::TPrimitiveType> {
    logical_type_of_field(field).map(logical_type_to_primitive)
}

pub(crate) fn arrow_field_to_primitive(field: &Field) -> Option<types::TPrimitiveType> {
    field_logical_primitive(field).or_else(|| arrow_type_to_primitive(field.data_type()).ok())
}

pub(crate) fn arrow_type_to_primitive(
    data_type: &DataType,
) -> Result<types::TPrimitiveType, String> {
    match data_type {
        DataType::Boolean => Ok(types::TPrimitiveType::BOOLEAN),
        DataType::Int8 => Ok(types::TPrimitiveType::TINYINT),
        DataType::Int16 => Ok(types::TPrimitiveType::SMALLINT),
        DataType::Int32 => Ok(types::TPrimitiveType::INT),
        DataType::Int64 => Ok(types::TPrimitiveType::BIGINT),
        DataType::Float32 => Ok(types::TPrimitiveType::FLOAT),
        DataType::Float64 => Ok(types::TPrimitiveType::DOUBLE),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(types::TPrimitiveType::VARCHAR),
        DataType::Binary => Ok(types::TPrimitiveType::VARBINARY),
        // NovaRocks reserves arrow `LargeBinary` for the v3 variant payload
        // (see src/lower/type_lowering.rs:170). Plain BINARY uses `Binary`.
        DataType::LargeBinary => Ok(types::TPrimitiveType::VARIANT),
        DataType::Date32 => Ok(types::TPrimitiveType::DATE),
        DataType::Timestamp(_, _) => Ok(types::TPrimitiveType::DATETIME),
        DataType::Decimal128(_, _) => Ok(types::TPrimitiveType::DECIMAL128),
        DataType::Decimal256(_, _) => Ok(types::TPrimitiveType::DECIMAL256),
        DataType::FixedSizeBinary(16) => Ok(types::TPrimitiveType::LARGEINT),
        DataType::Time64(_) => Ok(types::TPrimitiveType::TIME),
        DataType::Null => Ok(types::TPrimitiveType::NULL_TYPE),
        other => Err(format!(
            "Arrow-to-thrift primitive conversion does not support data type {:?}",
            other
        )),
    }
}

pub(crate) fn thrift_node_to_primitive(node: &exprs::TExprNode) -> Option<types::TPrimitiveType> {
    thrift_desc_to_primitive(&node.type_)
}

pub(crate) fn thrift_desc_to_primitive(desc: &types::TTypeDesc) -> Option<types::TPrimitiveType> {
    let types = desc.types.as_ref()?;
    let first = types.first()?;
    if first.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = first.scalar_type.as_ref()?;
    Some(scalar.type_)
}

pub(crate) fn thrift_desc_to_arrow_type(desc: &types::TTypeDesc) -> Option<DataType> {
    let types = desc.types.as_ref()?;
    let mut cursor = 0usize;
    thrift_nodes_to_arrow_type(types, &mut cursor)
}

pub(crate) fn thrift_desc_to_arrow_field(
    name: &str,
    nullable: bool,
    desc: &types::TTypeDesc,
) -> Option<Field> {
    let types = desc.types.as_ref()?;
    let mut cursor = 0usize;
    thrift_nodes_to_arrow_field(types, &mut cursor, name, nullable)
}

fn thrift_nodes_to_arrow_type(types: &[types::TTypeNode], cursor: &mut usize) -> Option<DataType> {
    let node = types.get(*cursor)?;
    *cursor += 1;
    match node.type_ {
        t if t == types::TTypeNodeType::SCALAR => {
            let scalar = node.scalar_type.as_ref()?;
            let data_type = match scalar.type_ {
                t if t == types::TPrimitiveType::NULL_TYPE => DataType::Null,
                t if t == types::TPrimitiveType::BOOLEAN => DataType::Boolean,
                t if t == types::TPrimitiveType::TINYINT => DataType::Int8,
                t if t == types::TPrimitiveType::SMALLINT => DataType::Int16,
                t if t == types::TPrimitiveType::INT => DataType::Int32,
                t if t == types::TPrimitiveType::BIGINT => DataType::Int64,
                t if t == types::TPrimitiveType::LARGEINT => DataType::FixedSizeBinary(16),
                t if t == types::TPrimitiveType::FLOAT => DataType::Float32,
                t if t == types::TPrimitiveType::DOUBLE => DataType::Float64,
                t if t == types::TPrimitiveType::DATE => DataType::Date32,
                t if t == types::TPrimitiveType::DATETIME => {
                    let unit = match scalar.time_unit {
                        None => TimeUnit::Microsecond,
                        Some(c) if c == THRIFT_TIME_UNIT_MICROS => TimeUnit::Microsecond,
                        Some(c) if c == THRIFT_TIME_UNIT_NANOS => TimeUnit::Nanosecond,
                        Some(_) => return None,
                    };
                    DataType::Timestamp(unit, None)
                }
                t if t == types::TPrimitiveType::TIME => DataType::Time64(TimeUnit::Microsecond),
                t if t == types::TPrimitiveType::DECIMALV2 => {
                    DataType::Decimal128(LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE)
                }
                t if t == types::TPrimitiveType::DECIMAL32
                    || t == types::TPrimitiveType::DECIMAL64
                    || t == types::TPrimitiveType::DECIMAL128
                    || t == types::TPrimitiveType::DECIMAL256
                    || t == types::TPrimitiveType::DECIMAL =>
                {
                    let precision = scalar.precision.and_then(|v| u8::try_from(v).ok())?;
                    let scale = scalar.scale.and_then(|v| i8::try_from(v).ok())?;
                    if scalar.type_ == types::TPrimitiveType::DECIMAL256 || precision > 38 {
                        DataType::Decimal256(precision, scale)
                    } else {
                        DataType::Decimal128(precision, scale)
                    }
                }
                t if t == types::TPrimitiveType::BINARY
                    || t == types::TPrimitiveType::VARBINARY =>
                {
                    DataType::Binary
                }
                t if t == types::TPrimitiveType::HLL
                    || t == types::TPrimitiveType::OBJECT
                    || t == types::TPrimitiveType::PERCENTILE =>
                {
                    DataType::Binary
                }
                t if t == types::TPrimitiveType::CHAR
                    || t == types::TPrimitiveType::VARCHAR
                    || t == types::TPrimitiveType::JSON
                    || t == types::TPrimitiveType::FUNCTION =>
                {
                    DataType::Utf8
                }
                t if t == types::TPrimitiveType::VARIANT => DataType::LargeBinary,
                _ => return None,
            };
            Some(data_type)
        }
        t if t == types::TTypeNodeType::STRUCT => {
            let fields = node.struct_fields.as_ref()?;
            let mut out_fields = Vec::with_capacity(fields.len());
            for field in fields {
                let name = field.name.clone()?;
                out_fields.push(thrift_nodes_to_arrow_field(types, cursor, &name, true)?);
            }
            Some(DataType::Struct(out_fields.into()))
        }
        t if t == types::TTypeNodeType::ARRAY => {
            let item_field = Arc::new(thrift_nodes_to_arrow_field(types, cursor, "item", true)?);
            Some(DataType::List(item_field))
        }
        t if t == types::TTypeNodeType::MAP => {
            let key_field = thrift_nodes_to_arrow_field(types, cursor, "key", true)?;
            let value_field = thrift_nodes_to_arrow_field(types, cursor, "value", true)?;
            let entries = Arc::new(Field::new(
                "entries",
                DataType::Struct(vec![key_field, value_field].into()),
                false,
            ));
            Some(DataType::Map(entries, false))
        }
        _ => None,
    }
}

fn thrift_nodes_to_arrow_field(
    types: &[types::TTypeNode],
    cursor: &mut usize,
    name: &str,
    nullable: bool,
) -> Option<Field> {
    let node_start = *cursor;
    let data_type = thrift_nodes_to_arrow_type(types, cursor)?;
    let field = Field::new(name, data_type, nullable);
    Some(match logical_type_from_node(types.get(node_start)?) {
        Some(logical_type) => field_with_logical_type(field, logical_type),
        None => field,
    })
}

fn logical_type_from_node(node: &types::TTypeNode) -> Option<LogicalType> {
    if node.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    match node.scalar_type.as_ref()?.type_ {
        t if t == types::TPrimitiveType::JSON => Some(LogicalType::Json),
        t if t == types::TPrimitiveType::HLL => Some(LogicalType::Hll),
        t if t == types::TPrimitiveType::OBJECT => Some(LogicalType::Object),
        t if t == types::TPrimitiveType::PERCENTILE => Some(LogicalType::Percentile),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::types::logical::field_with_logical_type;

    fn logical_field(name: &str, data_type: DataType, logical_type: LogicalType) -> Field {
        field_with_logical_type(Field::new(name, data_type, true), logical_type)
    }

    #[test]
    fn arrow_field_to_primitive_honors_top_level_json_metadata() {
        let field = logical_field("payload", DataType::Utf8, LogicalType::Json);

        assert_eq!(
            arrow_field_to_primitive(&field),
            Some(types::TPrimitiveType::JSON)
        );
    }

    #[test]
    fn arrow_field_to_primitive_honors_object_family_metadata() {
        let cases = [
            (
                logical_field("hll", DataType::Binary, LogicalType::Hll),
                types::TPrimitiveType::HLL,
            ),
            (
                logical_field("bitmap", DataType::Binary, LogicalType::Bitmap),
                types::TPrimitiveType::OBJECT,
            ),
            (
                logical_field("object", DataType::LargeBinary, LogicalType::Object),
                types::TPrimitiveType::OBJECT,
            ),
            (
                logical_field("percentile", DataType::Binary, LogicalType::Percentile),
                types::TPrimitiveType::PERCENTILE,
            ),
        ];

        for (field, expected) in cases {
            assert_eq!(arrow_field_to_primitive(&field), Some(expected));
        }
    }

    #[test]
    fn arrow_field_to_primitive_falls_back_to_arrow_type_without_metadata() {
        let field = Field::new("plain", DataType::Utf8, true);

        assert_eq!(
            arrow_field_to_primitive(&field),
            Some(types::TPrimitiveType::VARCHAR)
        );
    }

    #[test]
    fn thrift_desc_to_arrow_field_tags_json_metadata() {
        let desc = scalar_desc(types::TPrimitiveType::JSON, None, None);

        let field =
            thrift_desc_to_arrow_field("payload", true, &desc).expect("json thrift desc lowers");

        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(logical_type_of_field(&field), Some(LogicalType::Json));
        assert_eq!(
            thrift_desc_to_primitive(&desc),
            Some(types::TPrimitiveType::JSON)
        );
    }

    #[test]
    fn thrift_desc_to_arrow_type_routes_wide_decimal_to_decimal256() {
        let desc = scalar_desc(types::TPrimitiveType::DECIMAL128, Some(40), Some(8));

        assert_eq!(
            thrift_desc_to_arrow_type(&desc),
            Some(DataType::Decimal256(40, 8))
        );
    }

    fn scalar_desc(
        primitive: types::TPrimitiveType,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> types::TTypeDesc {
        types::TTypeDesc::new(vec![types::TTypeNode::new(
            types::TTypeNodeType::SCALAR,
            types::TScalarType::new(primitive, None, precision, scale, None),
            None,
            None,
        )])
    }
}
