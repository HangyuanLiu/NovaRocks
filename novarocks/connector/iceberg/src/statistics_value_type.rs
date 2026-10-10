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

//! Exact Iceberg-owned statistics input type projection.

use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::{FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType};

use crate::iceberg::spec::{NestedField, PrimitiveType, Type};

/// The storage field authors root identity. Arrow contributes the already
/// resolved carrier, complete nested field facts and nullability; its carrier
/// never establishes UUID, VARIANT or numeric LARGEINT identity.
pub(crate) fn statistics_input_value_type(
    storage: &NestedField,
    source: &Field,
    field: &Field,
) -> Result<FunctionValueType, String> {
    if !source.name().eq_ignore_ascii_case(&storage.name)
        || !field.name().eq_ignore_ascii_case(&storage.name)
    {
        return Err("Iceberg statistics input name differs from its authoritative field".into());
    }
    if source.is_nullable() == storage.required
        || field.is_nullable() != source.is_nullable()
        || field.data_type() != source.data_type()
    {
        return Err(
            "Iceberg statistics input nullability differs from its authoritative field".into(),
        );
    }
    let logical = match storage.field_type.as_ref() {
        Type::Primitive(PrimitiveType::Uuid) => {
            if field.data_type() != &DataType::FixedSizeBinary(16) {
                return Err(
                    "Iceberg UUID statistics input requires its exact 16-byte carrier".into(),
                );
            }
            ValueLogicalType::Uuid
        }
        Type::Primitive(PrimitiveType::Variant) => {
            if field.data_type() != &DataType::LargeBinary {
                return Err(
                    "Iceberg VARIANT statistics input requires its exact LargeBinary SQL carrier"
                        .into(),
                );
            }
            ValueLogicalType::Variant
        }
        _ => {
            let declared = novarocks_type_contract::field_logical_type(source)
                .map_err(|error| error.to_string())?;
            match (storage.field_type.as_ref(), declared) {
                (_, ValueLogicalType::Physical) => ValueLogicalType::Physical,
                (
                    Type::Primitive(PrimitiveType::Binary),
                    ValueLogicalType::Hll | ValueLogicalType::Bitmap,
                ) => declared,
                (Type::Primitive(PrimitiveType::Fixed(16)), ValueLogicalType::LargeInt) => declared,
                _ => {
                    return Err(
                        "Iceberg statistics source declaration conflicts with its storage type"
                            .into(),
                    );
                }
            }
        }
    };
    if source.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
        && novarocks_type_contract::field_logical_type(source).map_err(|error| error.to_string())?
            != logical
    {
        return Err("Iceberg statistics source declaration conflicts with its storage type".into());
    }
    // The authoritative storage identity need not be duplicated in Arrow
    // metadata. An explicit conflicting declaration is never silently erased.
    if field.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
        && novarocks_type_contract::field_logical_type(field).map_err(|error| error.to_string())?
            != logical
    {
        return Err(
            "Iceberg statistics input logical identity conflicts with its authoritative field"
                .into(),
        );
    }
    FunctionValueType::try_with_logical_type(
        field.data_type().clone(),
        field.is_nullable(),
        logical,
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn uuid_comes_from_storage_type_and_fixed16_stays_physical() {
        let field = Field::new("u", DataType::FixedSizeBinary(16), true);
        let uuid = NestedField::optional(7, "u", Type::Primitive(PrimitiveType::Uuid));
        let fixed = NestedField::optional(8, "u", Type::Primitive(PrimitiveType::Fixed(16)));
        assert_eq!(
            statistics_input_value_type(&uuid, &field, &field)
                .unwrap()
                .logical_type,
            ValueLogicalType::Uuid
        );
        assert_eq!(
            statistics_input_value_type(&fixed, &field, &field)
                .unwrap()
                .logical_type,
            ValueLogicalType::Physical
        );
    }

    #[test]
    fn conflicting_uuid_carrier_nullability_and_identity_are_rejected() {
        let uuid = NestedField::optional(7, "u", Type::Primitive(PrimitiveType::Uuid));
        for field in [
            Field::new("u", DataType::Utf8, true),
            Field::new("u", DataType::FixedSizeBinary(15), true),
            Field::new("u", DataType::FixedSizeBinary(16), false),
            Field::new("u", DataType::FixedSizeBinary(16), true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "largeint".into())].into()),
        ] {
            assert!(statistics_input_value_type(&uuid, &field, &field).is_err());
        }
        let field = Field::new("u", DataType::FixedSizeBinary(16), true);
        let fixed = NestedField::optional(8, "u", Type::Primitive(PrimitiveType::Fixed(16)));
        let retagged = Field::new("u", DataType::FixedSizeBinary(16), true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "uuid".into())].into());
        assert!(statistics_input_value_type(&fixed, &field, &retagged).is_err());
        assert_eq!(
            statistics_input_value_type(&uuid, &field, &retagged)
                .unwrap()
                .logical_type,
            ValueLogicalType::Uuid
        );
    }

    #[test]
    fn variant_storage_identity_does_not_come_from_large_binary_carrier() {
        let variant = NestedField::optional(7, "v", Type::Primitive(PrimitiveType::Variant));
        let binary = NestedField::optional(8, "v", Type::Primitive(PrimitiveType::Binary));
        let field = Field::new("v", DataType::LargeBinary, true);
        assert_eq!(
            statistics_input_value_type(&variant, &field, &field)
                .unwrap()
                .logical_type,
            ValueLogicalType::Variant
        );
        assert_eq!(
            statistics_input_value_type(&binary, &field, &field)
                .unwrap()
                .logical_type,
            ValueLogicalType::Physical
        );
        let retagged = field
            .clone()
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "variant".into())].into());
        assert_eq!(
            statistics_input_value_type(&variant, &field, &retagged)
                .unwrap()
                .logical_type,
            ValueLogicalType::Variant
        );
        assert!(statistics_input_value_type(&binary, &field, &retagged).is_err());
        let foreign = field
            .clone()
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "object".into())].into());
        assert!(statistics_input_value_type(&variant, &retagged, &foreign).is_err());
        assert!(
            statistics_input_value_type(
                &variant,
                &retagged,
                &Field::new("v", DataType::Binary, true)
            )
            .is_err()
        );
    }

    #[test]
    fn physical_projection_preserves_actual_nested_field_facts() {
        let child = Arc::new(
            Field::new("actual_item", DataType::Utf8, false)
                .with_metadata([("provider.field-id".into(), "91".into())].into()),
        );
        let data_type = DataType::List(child);
        let field = Field::new("items", data_type.clone(), true);
        let storage = NestedField::optional(
            7,
            "items",
            Type::List(crate::iceberg::spec::ListType::new(Arc::new(
                NestedField::required(8, "element", Type::Primitive(PrimitiveType::String)),
            ))),
        );
        let result = statistics_input_value_type(&storage, &field, &field).unwrap();
        assert_eq!(result.data_type, data_type);
        assert_eq!(result.logical_type, ValueLogicalType::Physical);
        assert!(result.nullable);
    }
}
