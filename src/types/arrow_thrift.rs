use arrow::datatypes::{DataType, Field};

use crate::thrift::types;
use crate::types::logical::{LogicalType, logical_type_of_field};

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
}
