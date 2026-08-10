// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Provider-owned projection from an Iceberg schema into bounded connector
//! table-definition facts. Core renders SQL from these facts and never reads
//! an Iceberg table or type.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    ConnectorError, ConnectorRequestContext, ConnectorTableDefinitionColumn,
    ConnectorTableDefinitionFacts, ConnectorTableDefinitionStructField,
    ConnectorTableDefinitionType, ConnectorTablePlanningFacts,
};

use crate::iceberg::spec::{PrimitiveType, Schema, Type};

pub fn table_definition_facts(
    iceberg_schema: &Schema,
    arrow_schema: &SchemaRef,
    planning_facts: &ConnectorTablePlanningFacts,
    table_comment: Option<&str>,
    context: &ConnectorRequestContext,
) -> Result<ConnectorTableDefinitionFacts, ConnectorError> {
    let columns = iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                    "Iceberg table definition ordinal does not fit u32",
                )
            })?;
            Ok(ConnectorTableDefinitionColumn::new(
                ordinal,
                definition_type(&field.field_type),
                !field.required,
                field.doc.as_deref().map(Arc::from),
            ))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;

    ConnectorTableDefinitionFacts::try_new(
        arrow_schema,
        planning_facts,
        columns,
        table_comment.map(Arc::from),
        context,
    )
}

fn definition_type(data_type: &Type) -> ConnectorTableDefinitionType {
    match data_type {
        Type::Primitive(PrimitiveType::Boolean) => ConnectorTableDefinitionType::Boolean,
        Type::Primitive(PrimitiveType::Int) => ConnectorTableDefinitionType::Int,
        Type::Primitive(PrimitiveType::Long) => ConnectorTableDefinitionType::BigInt,
        Type::Primitive(PrimitiveType::Float) => ConnectorTableDefinitionType::Float,
        Type::Primitive(PrimitiveType::Double) => ConnectorTableDefinitionType::Double,
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            ConnectorTableDefinitionType::Decimal {
                precision: *precision,
                scale: *scale,
            }
        }
        Type::Primitive(PrimitiveType::Date) => ConnectorTableDefinitionType::Date,
        Type::Primitive(PrimitiveType::Time) => ConnectorTableDefinitionType::Time,
        Type::Primitive(PrimitiveType::Timestamp) | Type::Primitive(PrimitiveType::Timestamptz) => {
            ConnectorTableDefinitionType::DateTime
        }
        Type::Primitive(PrimitiveType::TimestampNs)
        | Type::Primitive(PrimitiveType::TimestamptzNs) => ConnectorTableDefinitionType::DateTimeNs,
        Type::Primitive(PrimitiveType::String) | Type::Primitive(PrimitiveType::Uuid) => {
            ConnectorTableDefinitionType::String
        }
        Type::Primitive(PrimitiveType::Fixed(length)) => ConnectorTableDefinitionType::Binary {
            fixed_length: Some(*length),
        },
        Type::Primitive(PrimitiveType::Binary) => {
            ConnectorTableDefinitionType::Binary { fixed_length: None }
        }
        Type::Primitive(PrimitiveType::Variant) => ConnectorTableDefinitionType::Variant,
        Type::List(list) => ConnectorTableDefinitionType::Array(Box::new(definition_type(
            &list.element_field.field_type,
        ))),
        Type::Map(map) => ConnectorTableDefinitionType::Map(
            Box::new(definition_type(&map.key_field.field_type)),
            Box::new(definition_type(&map.value_field.field_type)),
        ),
        Type::Struct(struct_type) => ConnectorTableDefinitionType::Struct(
            struct_type
                .fields()
                .iter()
                .map(|field| {
                    ConnectorTableDefinitionStructField::new(
                        Arc::<str>::from(field.name.as_str()),
                        definition_type(&field.field_type),
                    )
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::iceberg::spec::{NestedField, StructType};

    use super::*;

    #[test]
    fn projects_fixed_nested_uuid_and_timestamp_types() {
        let nested = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::optional(
                1,
                "fixed_value",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::optional(
                2,
                "uuid_value",
                Type::Primitive(PrimitiveType::Uuid),
            )),
            Arc::new(NestedField::optional(
                3,
                "timestamp_value",
                Type::Primitive(PrimitiveType::TimestamptzNs),
            )),
            Arc::new(NestedField::optional(
                4,
                "decimal_value",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 18,
                    scale: 4,
                }),
            )),
        ]));

        assert_eq!(
            definition_type(&nested),
            ConnectorTableDefinitionType::Struct(vec![
                ConnectorTableDefinitionStructField::new(
                    "fixed_value",
                    ConnectorTableDefinitionType::Binary {
                        fixed_length: Some(16),
                    },
                ),
                ConnectorTableDefinitionStructField::new(
                    "uuid_value",
                    ConnectorTableDefinitionType::String,
                ),
                ConnectorTableDefinitionStructField::new(
                    "timestamp_value",
                    ConnectorTableDefinitionType::DateTimeNs,
                ),
                ConnectorTableDefinitionStructField::new(
                    "decimal_value",
                    ConnectorTableDefinitionType::Decimal {
                        precision: 18,
                        scale: 4,
                    },
                ),
            ])
        );
    }
}
