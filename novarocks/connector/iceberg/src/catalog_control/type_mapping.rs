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

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorDataType, ConnectorDefaultValue, ConnectorStructField,
};

use crate::iceberg::spec::{
    ListType, Literal, MapType, NestedField, PrimitiveLiteral, PrimitiveType, StructType, Type,
};

pub(crate) fn schema_fields(
    columns: &[ConnectorColumnDefinition],
) -> Result<Vec<Arc<NestedField>>, String> {
    let limits = novarocks_types::logical_type::LogicalTypeLimits::default();
    let mut nodes = 0usize;
    let mut text = 0usize;
    for column in columns {
        let usage = column.data_type.validate(limits)?;
        nodes = nodes
            .checked_add(usage.nodes)
            .ok_or("mutation schema node budget overflow")?;
        text = text
            .checked_add(usage.text_bytes)
            .and_then(|n| n.checked_add(column.name.len()))
            .ok_or("mutation schema text budget overflow")?;
        if nodes > limits.max_nodes || text > limits.max_text_bytes {
            return Err("mutation schema exceeds its complete recursive budget".into());
        }
    }
    let mut next_id = i32::try_from(columns.len())
        .map_err(|_| "too many Iceberg columns".to_string())?
        .checked_add(1)
        .ok_or_else(|| "too many Iceberg columns".to_string())?;
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let id =
                i32::try_from(index + 1).map_err(|_| "too many Iceberg columns".to_string())?;
            column_field(id, column, &mut next_id).map(Arc::new)
        })
        .collect()
}

pub(crate) fn column_field(
    id: i32,
    column: &ConnectorColumnDefinition,
    next_id: &mut i32,
) -> Result<NestedField, String> {
    column.data_type.validate(Default::default())?;
    let field_type = iceberg_type(&column.data_type, next_id)?;
    let mut field = NestedField::new(
        id,
        column.name.as_ref(),
        field_type.clone(),
        !column.nullable,
    );
    if let Some(default) = &column.default
        && let Some(literal) = default_literal(default, &field_type)?
    {
        field = field
            .with_initial_default(literal.clone())
            .with_write_default(literal);
    }
    Ok(field)
}

pub(crate) fn iceberg_type(
    data_type: &ConnectorDataType,
    next_id: &mut i32,
) -> Result<Type, String> {
    data_type.validate(Default::default())?;
    iceberg_type_inner(data_type, next_id, true)
}

fn iceberg_type_inner(
    data_type: &ConnectorDataType,
    next_id: &mut i32,
    root: bool,
) -> Result<Type, String> {
    if !root
        && matches!(
            data_type,
            ConnectorDataType::Bitmap | ConnectorDataType::Hll
        )
    {
        return Err(
            "Iceberg mutation cannot preserve nested Bitmap/Hll without an exact logical marker"
                .into(),
        );
    }
    let primitive = |value| Ok(Type::Primitive(value));
    match data_type {
        ConnectorDataType::Boolean => primitive(PrimitiveType::Boolean),
        ConnectorDataType::Int8 | ConnectorDataType::Int16 | ConnectorDataType::Int32 => {
            primitive(PrimitiveType::Int)
        }
        ConnectorDataType::Int64 => primitive(PrimitiveType::Long),
        ConnectorDataType::LargeInt => primitive(PrimitiveType::Fixed(
            u64::try_from(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
                .expect("positive LargeInt width"),
        )),
        ConnectorDataType::Float32 => primitive(PrimitiveType::Float),
        ConnectorDataType::Float64 => primitive(PrimitiveType::Double),
        ConnectorDataType::Decimal {
            bits,
            precision,
            scale,
        } => {
            if *bits != 128 {
                return Err(
                    "Iceberg mutation cannot preserve requested decimal carrier width".into(),
                );
            }
            if *scale < 0
                || u8::try_from(*scale)
                    .ok()
                    .is_some_and(|scale| scale > *precision)
            {
                return Err(format!("invalid DECIMAL({precision},{scale})"));
            }
            Type::decimal(
                u32::from(*precision),
                u32::try_from(*scale).unwrap_or_default(),
            )
            .map_err(|error| format!("invalid Iceberg DECIMAL({precision},{scale}): {error}"))
        }
        ConnectorDataType::Utf8 | ConnectorDataType::Json => primitive(PrimitiveType::String),
        ConnectorDataType::Binary | ConnectorDataType::Bitmap | ConnectorDataType::Hll => {
            primitive(PrimitiveType::Binary)
        }
        ConnectorDataType::Date32 => primitive(PrimitiveType::Date),
        ConnectorDataType::Timestamp { unit, timezone } => {
            use arrow::datatypes::TimeUnit;
            // Existing metadata-table expressions carry UTC. Iceberg stores
            // that instant family; its declared SQL read mapping normalizes
            // the label back to None. Other labels have no exact mapping.
            if timezone.as_deref().is_some_and(|zone| zone != "UTC") {
                return Err("Iceberg mutation cannot preserve a non-UTC timezone label".into());
            }
            match (unit, timezone.is_some()) {
                (TimeUnit::Microsecond, false) => primitive(PrimitiveType::Timestamp),
                (TimeUnit::Nanosecond, false) => primitive(PrimitiveType::TimestampNs),
                (TimeUnit::Microsecond, true) => primitive(PrimitiveType::Timestamptz),
                (TimeUnit::Nanosecond, true) => primitive(PrimitiveType::TimestamptzNs),
                _ => Err("Iceberg cannot preserve the requested timestamp precision".into()),
            }
        }
        ConnectorDataType::Time {
            bits: 64,
            unit: arrow::datatypes::TimeUnit::Microsecond,
        } => primitive(PrimitiveType::Time),
        ConnectorDataType::FixedSizeBinary(16) | ConnectorDataType::Uuid => Err(
            "Iceberg mutation cannot preserve the requested logical domain through the existing LargeInt read mapping".into(),
        ),
        ConnectorDataType::FixedSizeBinary(width) => {
            primitive(PrimitiveType::Fixed(u64::from(*width)))
        }
        ConnectorDataType::Variant => primitive(PrimitiveType::Variant),
        ConnectorDataType::Array {
            element,
            fixed_length,
        } => {
            if fixed_length.is_some() {
                return Err("Iceberg cannot preserve a fixed-length array constraint".into());
            }
            let element_id = allocate_id(next_id)?;
            let element_type = iceberg_type_inner(&element.data_type, next_id, false)?;
            Ok(Type::List(ListType::new(Arc::new(
                NestedField::list_element(element_id, element_type, !element.nullable),
            ))))
        }
        ConnectorDataType::Map { key, value } => {
            // The existing engine read carrier widens map keys to nullable.
            // Iceberg still owns a required key; the writer validates actual
            // values. Keep this declared read/write adaptation explicit.
            let key_id = allocate_id(next_id)?;
            let value_id = allocate_id(next_id)?;
            let key_type = iceberg_type_inner(&key.data_type, next_id, false)?;
            let value_type = iceberg_type_inner(&value.data_type, next_id, false)?;
            Ok(Type::Map(MapType::new(
                Arc::new(NestedField::map_key_element(key_id, key_type)),
                Arc::new(NestedField::map_value_element(
                    value_id,
                    value_type,
                    !value.nullable,
                )),
            )))
        }
        ConnectorDataType::Struct(fields) => Ok(Type::Struct(StructType::new(
            fields
                .iter()
                .map(|field| struct_field(field, next_id).map(Arc::new))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        other => Err(format!(
            "Iceberg cannot preserve mutation logical type {other:?}"
        )),
    }
}

fn struct_field(field: &ConnectorStructField, next_id: &mut i32) -> Result<NestedField, String> {
    let id = allocate_id(next_id)?;
    let field_type = iceberg_type_inner(&field.data_type, next_id, false)?;
    Ok(NestedField::new(
        id,
        field.name.as_str(),
        field_type,
        !field.nullable,
    ))
}

fn allocate_id(next_id: &mut i32) -> Result<i32, String> {
    let id = *next_id;
    *next_id = next_id
        .checked_add(1)
        .ok_or_else(|| "Iceberg field ID space exhausted".to_string())?;
    Ok(id)
}

/// An empty-collection default arrives as the literal text the statement wrote,
/// because the neutral default vocabulary has no collection variant. Only the
/// empty collection is admitted, matching what the read path can materialize.
fn empty_collection_default(text: &str, field_type: &Type) -> Result<Option<Literal>, String> {
    let trimmed: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    match field_type {
        Type::List(_) if trimmed == "[]" => Ok(Some(Literal::List(Vec::new()))),
        Type::Map(_) if trimmed == "{}" => {
            Ok(Some(Literal::Map(crate::iceberg::spec::Map::default())))
        }
        Type::List(_) | Type::Map(_) => Err(format!(
            "only an empty collection default is supported for Iceberg type {field_type}"
        )),
        _ => Ok(None),
    }
}

pub(crate) fn default_literal(
    value: &ConnectorDefaultValue,
    field_type: &Type,
) -> Result<Option<Literal>, String> {
    if let ConnectorDefaultValue::String(text) = value
        && matches!(field_type, Type::List(_) | Type::Map(_))
    {
        return empty_collection_default(text, field_type);
    }
    let primitive = match (value, field_type) {
        (ConnectorDefaultValue::Null, _) => return Ok(None),
        (ConnectorDefaultValue::Bool(value), Type::Primitive(PrimitiveType::Boolean)) => {
            PrimitiveLiteral::Boolean(*value)
        }
        (ConnectorDefaultValue::Int(value), Type::Primitive(PrimitiveType::Int)) => {
            PrimitiveLiteral::Int(i32::try_from(*value).map_err(|_| "INT default is out of range")?)
        }
        (ConnectorDefaultValue::Int(value), Type::Primitive(PrimitiveType::Long)) => {
            PrimitiveLiteral::Long(*value)
        }
        (ConnectorDefaultValue::Float(value), Type::Primitive(PrimitiveType::Float)) => {
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(*value as f32))
        }
        (ConnectorDefaultValue::Float(value), Type::Primitive(PrimitiveType::Double)) => {
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(*value))
        }
        (
            ConnectorDefaultValue::Decimal { unscaled, scale },
            Type::Primitive(PrimitiveType::Decimal {
                scale: expected_scale,
                ..
            }),
        ) if u32::try_from(*scale).ok() == Some(*expected_scale) => {
            PrimitiveLiteral::Int128(*unscaled)
        }
        (ConnectorDefaultValue::String(value), Type::Primitive(PrimitiveType::String)) => {
            PrimitiveLiteral::String(value.to_string())
        }
        (ConnectorDefaultValue::Binary(value), Type::Primitive(PrimitiveType::Binary)) => {
            PrimitiveLiteral::Binary(value.to_vec())
        }
        (ConnectorDefaultValue::Binary(value), Type::Primitive(PrimitiveType::Fixed(width)))
            if usize::try_from(*width).ok() == Some(value.len()) =>
        {
            PrimitiveLiteral::Binary(value.to_vec())
        }
        (ConnectorDefaultValue::Date(value), Type::Primitive(PrimitiveType::Date)) => {
            PrimitiveLiteral::Int(*value)
        }
        (ConnectorDefaultValue::DateTime(value), Type::Primitive(PrimitiveType::Timestamp))
        | (ConnectorDefaultValue::DateTime(value), Type::Primitive(PrimitiveType::TimestampNs))
        | (ConnectorDefaultValue::DateTime(value), Type::Primitive(PrimitiveType::Time)) => {
            PrimitiveLiteral::Long(*value)
        }
        _ => {
            return Err(format!(
                "connector default does not match Iceberg type {field_type}"
            ));
        }
    };
    Ok(Some(Literal::Primitive(primitive)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn connector_schema_mapping_preserves_nested_nullability_and_unique_ids() {
        let columns = vec![ConnectorColumnDefinition {
            name: "payload".into(),
            data_type: ConnectorDataType::Struct(vec![ConnectorStructField {
                name: "items".into(),
                data_type: ConnectorDataType::Array {
                    element: Box::new(novarocks_types::logical_type::LogicalValue {
                        data_type: ConnectorDataType::LargeInt,
                        nullable: true,
                    }),
                    fixed_length: None,
                },
                nullable: false,
            }]),
            nullable: true,
            aggregation: None,
            default: None,
        }];
        let fields = schema_fields(&columns).expect("schema fields");
        assert_eq!(fields[0].id, 1);
        assert!(!fields[0].required);
        let Type::Struct(struct_type) = fields[0].field_type.as_ref() else {
            panic!("expected struct");
        };
        assert!(struct_type.fields()[0].required);
        let Type::List(list_type) = struct_type.fields()[0].field_type.as_ref() else {
            panic!("expected list");
        };
        assert_ne!(struct_type.fields()[0].id, list_type.element_field.id);
        assert_eq!(
            list_type.element_field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Fixed(16))
        );
    }

    #[test]
    fn connector_defaults_are_checked_against_the_authoritative_type() {
        let mismatch = default_literal(
            &ConnectorDefaultValue::Binary(Bytes::from_static(b"abc")),
            &Type::Primitive(PrimitiveType::Fixed(16)),
        )
        .expect_err("fixed default width mismatch");
        assert!(mismatch.contains("does not match"), "{mismatch}");

        let decimal = ConnectorDataType::Decimal {
            bits: 128,
            precision: 10,
            scale: -1,
        };
        assert!(iceberg_type(&decimal, &mut 1).is_err());
    }
    #[test]
    fn mutation_refuses_unrepresentable_constraints_and_recursive_budget() {
        use novarocks_types::logical_type::LogicalValue;
        let fixed = ConnectorDataType::Array {
            element: Box::new(LogicalValue {
                data_type: ConnectorDataType::Int64,
                nullable: false,
            }),
            fixed_length: Some(2),
        };
        assert!(
            iceberg_type(&fixed, &mut 1)
                .unwrap_err()
                .contains("fixed-length")
        );
        let narrow = ConnectorDataType::Array {
            element: Box::new(LogicalValue {
                data_type: ConnectorDataType::Int8,
                nullable: true,
            }),
            fixed_length: None,
        };
        assert!(iceberg_type(&narrow, &mut 1).is_ok());
        let mut deep = ConnectorDataType::Int64;
        for _ in 0..65 {
            deep = ConnectorDataType::Array {
                element: Box::new(LogicalValue {
                    data_type: deep,
                    nullable: true,
                }),
                fixed_length: None,
            };
        }
        assert!(iceberg_type(&deep, &mut 1).is_err());
    }
    #[test]
    fn mutation_rejects_logical_domains_without_exact_provider_restore() {
        use novarocks_types::logical_type::LogicalValue;
        for root in [
            ConnectorDataType::Uuid,
            ConnectorDataType::FixedSizeBinary(16),
        ] {
            assert!(
                iceberg_type(&root, &mut 1)
                    .unwrap_err()
                    .contains("cannot preserve")
            );
        }
        for nested in [
            ConnectorDataType::Bitmap,
            ConnectorDataType::Hll,
            ConnectorDataType::Uuid,
            ConnectorDataType::FixedSizeBinary(16),
        ] {
            let wrappers = [
                ConnectorDataType::Array {
                    element: Box::new(LogicalValue {
                        data_type: nested.clone(),
                        nullable: true,
                    }),
                    fixed_length: None,
                },
                ConnectorDataType::Map {
                    key: Box::new(LogicalValue {
                        data_type: ConnectorDataType::Utf8,
                        nullable: false,
                    }),
                    value: Box::new(LogicalValue {
                        data_type: nested.clone(),
                        nullable: true,
                    }),
                },
                ConnectorDataType::Struct(vec![ConnectorStructField {
                    name: "value".into(),
                    data_type: nested,
                    nullable: true,
                }]),
            ];
            for wrapper in wrappers {
                let columns = [ConnectorColumnDefinition {
                    name: "payload".into(),
                    data_type: wrapper,
                    nullable: true,
                    aggregation: None,
                    default: None,
                }];
                assert!(
                    schema_fields(&columns)
                        .unwrap_err()
                        .contains("cannot preserve")
                );
            }
        }
    }

    #[test]
    fn mutation_keeps_root_bitmap_hll_and_recursive_largeint_domains() {
        for logical in [ConnectorDataType::Bitmap, ConnectorDataType::Hll] {
            assert_eq!(
                iceberg_type(&logical, &mut 1).unwrap(),
                Type::Primitive(PrimitiveType::Binary)
            );
        }
        assert_eq!(
            iceberg_type(&ConnectorDataType::LargeInt, &mut 1).unwrap(),
            Type::Primitive(PrimitiveType::Fixed(16))
        );
        assert_eq!(
            iceberg_type(&ConnectorDataType::FixedSizeBinary(8), &mut 1).unwrap(),
            Type::Primitive(PrimitiveType::Fixed(8))
        );
        // The recursive LargeInt preservation path is independently asserted
        // by connector_schema_mapping_preserves_nested_nullability_and_unique_ids.
    }

    #[test]
    fn mutation_utc_timestamp_has_explicit_provider_and_read_normalization() {
        use arrow::datatypes::{DataType, TimeUnit};
        for (unit, physical) in [
            (TimeUnit::Microsecond, PrimitiveType::Timestamptz),
            (TimeUnit::Nanosecond, PrimitiveType::TimestamptzNs),
        ] {
            let frozen = ConnectorDataType::Timestamp {
                unit,
                timezone: Some("UTC".into()),
            };
            assert_eq!(
                iceberg_type(&frozen, &mut 1).unwrap(),
                Type::Primitive(physical)
            );
            let schema = crate::iceberg::spec::Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "ts",
                    iceberg_type(&frozen, &mut 2).unwrap(),
                ))])
                .build()
                .unwrap();
            let read = crate::schema_mapping::sql_read_schema_from_iceberg(&schema).unwrap();
            assert_eq!(read.field(0).data_type(), &DataType::Timestamp(unit, None));
        }
        let named = ConnectorDataType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("Europe/Paris".into()),
        };
        assert!(
            iceberg_type(&named, &mut 1)
                .unwrap_err()
                .contains("non-UTC")
        );
    }
}

/// Obtain the same fresh field IDs the SDK will assign during table creation.
/// This is a local metadata construction, before any catalog effect; using the
/// SDK allocator avoids a second traversal order becoming an identity authority.
pub(crate) fn creation_schema(
    columns: &[ConnectorColumnDefinition],
) -> Result<crate::iceberg::spec::Schema, String> {
    use crate::iceberg::spec::{
        FormatVersion, PartitionSpec, Schema, SortOrder, TableMetadataBuilder,
    };
    let schema = Schema::builder()
        .with_fields(schema_fields(columns)?)
        .build()
        .map_err(|e| e.to_string())?;
    let metadata = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://schema-allocation".into(),
        FormatVersion::V3,
        Default::default(),
    )
    .map_err(|e| e.to_string())?
    .build()
    .map_err(|e| e.to_string())?
    .metadata;
    Ok(metadata.current_schema().as_ref().clone())
}
pub(crate) fn creation_domains(
    schema: &crate::iceberg::spec::Schema,
    columns: &[ConnectorColumnDefinition],
) -> Result<crate::field_domain::FieldDomains, String> {
    if schema.as_struct().fields().len() != columns.len() {
        return Err("creation schema arity differs".into());
    }
    let mut domains = crate::field_domain::FieldDomains::new();
    for (field, column) in schema.as_struct().fields().iter().zip(columns) {
        domains.extend(
            crate::field_domain::requested_field_domains(field, &column.data_type)
                .map_err(|e| e.to_string())?,
        );
    }
    Ok(domains)
}

pub(crate) fn validate_creation_metadata(
    metadata: &crate::iceberg::spec::TableMetadata,
    requested_schema: &crate::iceberg::spec::Schema,
    requested_domains: &crate::field_domain::FieldDomains,
) -> Result<(), String> {
    if metadata.current_schema().as_struct() != requested_schema.as_struct() {
        return Err("created Iceberg schema differs from the exact requested field tree".into());
    }
    let observed =
        crate::field_domain::metadata_declarations(metadata).map_err(|e| e.to_string())?;
    if observed.fields() != requested_domains {
        return Err("created Iceberg logical domains differ from the requested field IDs".into());
    }
    Ok(())
}
