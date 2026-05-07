use arrow::datatypes::DataType;

use crate::lower::thrift::type_lowering::scalar_type_desc;
use crate::types;

/// Convert Arrow DataType to Thrift TTypeDesc.
pub(crate) fn arrow_type_to_type_desc(data_type: &DataType) -> Result<types::TTypeDesc, String> {
    let mut nodes = Vec::new();
    append_arrow_type_nodes(data_type, &mut nodes)?;
    Ok(types::TTypeDesc::new(nodes))
}

fn append_arrow_type_nodes(
    data_type: &DataType,
    nodes: &mut Vec<types::TTypeNode>,
) -> Result<(), String> {
    match data_type {
        DataType::List(field) => {
            nodes.push(types::TTypeNode {
                type_: types::TTypeNodeType::ARRAY,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_arrow_type_nodes(field.data_type(), nodes)
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
            nodes.push(types::TTypeNode {
                type_: types::TTypeNodeType::MAP,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_arrow_type_nodes(fields[0].data_type(), nodes)?;
            append_arrow_type_nodes(fields[1].data_type(), nodes)
        }
        DataType::Struct(fields) => {
            nodes.push(types::TTypeNode {
                type_: types::TTypeNodeType::STRUCT,
                scalar_type: None,
                is_named: None,
                struct_fields: Some(
                    fields
                        .iter()
                        .map(|field| {
                            types::TStructField::new(
                                Some(field.name().to_string()),
                                None::<String>,
                                None::<i32>,
                                None::<String>,
                            )
                        })
                        .collect(),
                ),
            });
            for field in fields {
                append_arrow_type_nodes(field.data_type(), nodes)?;
            }
            Ok(())
        }
        DataType::Decimal128(p, s) => {
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DECIMAL128,
                None::<i32>,
                Some(i32::from(*p)),
                Some(i32::from(*s)),
            );
            nodes.push(types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            ));
            Ok(())
        }
        DataType::Decimal256(p, s) => {
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DECIMAL256,
                None::<i32>,
                Some(i32::from(*p)),
                Some(i32::from(*s)),
            );
            nodes.push(types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            ));
            Ok(())
        }
        _ => {
            let primitive = arrow_type_to_primitive(data_type)?;
            nodes.extend(scalar_type_desc(primitive).types.unwrap_or_default());
            Ok(())
        }
    }
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
            "ThriftPlanBuilder does not support data type {:?}",
            other
        )),
    }
}

pub(crate) use crate::sql::types::{arithmetic_result_type_with_op, wider_type};
