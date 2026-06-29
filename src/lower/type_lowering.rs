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
use crate::common::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use crate::thrift::types;
pub(crate) use crate::types::arrow_thrift::{THRIFT_TIME_UNIT_NANOS, thrift_time_unit_for_arrow};
use crate::types::arrow_thrift::{
    thrift_desc_to_arrow_field, thrift_desc_to_arrow_type, thrift_desc_to_primitive,
    thrift_node_to_primitive, thrift_type_desc_from_primitive,
};
use arrow::datatypes::{DataType, Field, TimeUnit};

/// Extract primitive type from TExprNode.
pub(crate) fn primitive_type_from_node(
    node: &crate::thrift::exprs::TExprNode,
) -> Option<types::TPrimitiveType> {
    thrift_node_to_primitive(node)
}

pub(crate) fn primitive_type_from_desc(desc: &types::TTypeDesc) -> Option<types::TPrimitiveType> {
    thrift_desc_to_primitive(desc)
}

pub(crate) fn scalar_type_desc(primitive: types::TPrimitiveType) -> types::TTypeDesc {
    thrift_type_desc_from_primitive(primitive)
}

/// Convert TPrimitiveType to Arrow DataType when precision/scale is not required.
///
/// This is mainly used by expression fields like `TExprNode.child_type` where FE already decides
/// a comparable type for both children, and BE executes comparison with that single logical type.
pub(crate) fn arrow_type_from_primitive(primitive: types::TPrimitiveType) -> Option<DataType> {
    let data_type = match primitive {
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
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        t if t == types::TPrimitiveType::TIME => DataType::Time64(TimeUnit::Microsecond),
        t if t == types::TPrimitiveType::BINARY || t == types::TPrimitiveType::VARBINARY => {
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
        t if t == types::TPrimitiveType::DECIMALV2 => {
            DataType::Decimal128(LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE)
        }
        // Decimal requires precision/scale from TTypeDesc; without that metadata we cannot build a
        // correct Arrow decimal type, except for legacy DECIMALV2 which has a fixed BE shape.
        _ => return None,
    };
    Some(data_type)
}

/// Convert TTypeDesc to Arrow DataType.
pub(crate) fn arrow_type_from_desc(desc: &types::TTypeDesc) -> Option<DataType> {
    thrift_desc_to_arrow_type(desc)
}

pub(crate) fn arrow_field_from_desc(
    name: &str,
    nullable: bool,
    desc: &types::TTypeDesc,
) -> Option<Field> {
    thrift_desc_to_arrow_field(name, nullable, desc)
}

// Keeping `decimal_params_from_desc` for potential future use when we need
// explicit decimal precision/scale, but suppress dead_code warning for now.
#[allow(dead_code)]
pub(crate) fn decimal_params_from_desc(desc: &types::TTypeDesc) -> Option<(u8, i8)> {
    let types = desc.types.as_ref()?;
    let first = types.first()?;
    if first.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = first.scalar_type.as_ref()?;
    let precision = scalar.precision.and_then(|v| u8::try_from(v).ok())?;
    let scale = scalar.scale.and_then(|v| i8::try_from(v).ok())?;
    Some((precision, scale))
}

#[cfg(test)]
mod tests {
    use super::{arrow_type_from_desc, arrow_type_from_primitive};
    use crate::thrift::types::TPrimitiveType;
    use crate::thrift::types::{TScalarType, TTypeDesc, TTypeNode, TTypeNodeType};
    use arrow::datatypes::DataType;

    #[test]
    fn object_family_primitives_lower_to_binary() {
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::HLL),
            Some(DataType::Binary)
        );
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::OBJECT),
            Some(DataType::Binary)
        );
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::PERCENTILE),
            Some(DataType::Binary)
        );
    }

    #[test]
    fn largeint_primitive_lowers_to_fixed_size_binary() {
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::LARGEINT),
            Some(DataType::FixedSizeBinary(16))
        );
    }

    #[test]
    fn decimalv2_primitive_lowers_to_legacy_decimal128() {
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::DECIMALV2),
            Some(DataType::Decimal128(27, 9))
        );
    }

    #[test]
    fn decimalv2_desc_ignores_fe_default_precision_scale() {
        let desc = TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: Some(TScalarType {
                    type_: TPrimitiveType::DECIMALV2,
                    len: None,
                    precision: Some(9),
                    scale: Some(0),
                    time_unit: None,
                }),
                is_named: None,
                struct_fields: None,
            }]),
        };

        assert_eq!(
            arrow_type_from_desc(&desc),
            Some(DataType::Decimal128(27, 9))
        );
    }

    #[test]
    fn datetime_desc_without_time_unit_defaults_to_microsecond() {
        use arrow::datatypes::TimeUnit;
        // An FE-style descriptor never sets time_unit; it must stay microsecond.
        let desc = TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: Some(TScalarType {
                    type_: TPrimitiveType::DATETIME,
                    len: None,
                    precision: None,
                    scale: None,
                    time_unit: None,
                }),
                is_named: None,
                struct_fields: None,
            }]),
        };
        assert_eq!(
            arrow_type_from_desc(&desc),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
    }
}
