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

//! Provider-owned projection of frozen Iceberg metadata into bounded SPI facts.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableColumnPlanningFact, ConnectorTableColumnRole,
    ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
    ConnectorTableForeignKeyConstraint, ConnectorTableIdentity, ConnectorTablePlanningFacts,
    ConnectorTableUniqueConstraint,
};

/// Derives the planning facts exposed by `ConnectorMetadata::load_table`.
///
/// The serialized metadata is parsed only inside the Iceberg provider.  The
/// returned facts intentionally contain no table UUID, snapshot, file, or
/// provider payload detail.
pub struct IcebergTablePlanningFactsInput<'a> {
    pub schema: &'a SchemaRef,
    pub metadata_columns: &'a [String],
    pub hidden_columns: &'a [String],
    pub logical_type_columns: &'a BTreeMap<String, String>,
    pub serialized_metadata: Option<&'a str>,
    pub namespace: &'a Arc<str>,
    pub instance_id: &'a ConnectorInstanceId,
    pub context: &'a ConnectorRequestContext,
}

pub fn table_planning_facts(
    input: IcebergTablePlanningFactsInput<'_>,
) -> Result<ConnectorTablePlanningFacts, ConnectorError> {
    let column_facts = input
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let name = field.name().to_ascii_lowercase();
            let visibility = if field
                .metadata()
                .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                || input
                    .hidden_columns
                    .iter()
                    .any(|hidden| hidden.eq_ignore_ascii_case(field.name()))
            {
                ConnectorTableColumnVisibility::Hidden
            } else {
                ConnectorTableColumnVisibility::Sql
            };
            let semantic_kind = match input.logical_type_columns.get(&name).map(String::as_str) {
                Some("bitmap") => ConnectorTableColumnSemanticKind::Bitmap,
                Some("hll") => ConnectorTableColumnSemanticKind::Hll,
                _ => ConnectorTableColumnSemanticKind::None,
            };
            let role = if input
                .metadata_columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(field.name()))
            {
                ConnectorTableColumnRole::RowLineageSystem
            } else {
                ConnectorTableColumnRole::Ordinary
            };
            Ok(ConnectorTableColumnPlanningFact::new(
                u32::try_from(ordinal).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg schema ordinal does not fit connector planning facts",
                    )
                })?,
                visibility,
                semantic_kind,
                role,
            ))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let (unique_constraints, foreign_key_constraints) = input
        .serialized_metadata
        .and_then(|serialized| {
            serde_json::from_str::<crate::iceberg::spec::TableMetadata>(serialized).ok()
        })
        .map(|metadata| {
            iceberg_constraint_facts(
                input.schema,
                metadata.properties(),
                input.namespace,
                input.instance_id,
            )
        })
        .unwrap_or_default();
    ConnectorTablePlanningFacts::try_new(
        input.schema,
        column_facts,
        unique_constraints,
        foreign_key_constraints,
        input.context,
    )
}

fn iceberg_constraint_facts(
    schema: &SchemaRef,
    properties: &HashMap<String, String>,
    namespace: &Arc<str>,
    instance_id: &ConnectorInstanceId,
) -> (
    Vec<ConnectorTableUniqueConstraint>,
    Vec<ConnectorTableForeignKeyConstraint>,
) {
    let ordinals = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| (field.name().to_ascii_lowercase(), ordinal as u32))
        .collect::<HashMap<_, _>>();
    let unique_constraints = properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("unique_constraints"))
        .into_iter()
        .flat_map(|(_, value)| value.split(';'))
        .filter_map(parse_constraint_columns)
        .filter_map(|columns| {
            columns
                .iter()
                .map(|column| ordinals.get(column).copied())
                .collect::<Option<Vec<_>>>()
                .map(ConnectorTableUniqueConstraint::new)
        })
        .collect();
    let foreign_key_constraints = properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("foreign_key_constraints"))
        .into_iter()
        .flat_map(|(_, value)| value.split(';'))
        .filter_map(parse_foreign_key_constraint)
        .filter_map(|(local_columns, referenced_table, referenced_columns)| {
            let local_column_ordinals = local_columns
                .iter()
                .map(|column| ordinals.get(column).copied())
                .collect::<Option<Vec<_>>>()?;
            let referenced_table =
                connector_table_identity(&referenced_table, namespace, instance_id)?;
            Some(ConnectorTableForeignKeyConstraint::new(
                local_column_ordinals,
                referenced_table,
                referenced_columns.into_iter().map(Arc::from).collect(),
            ))
        })
        .collect();
    (unique_constraints, foreign_key_constraints)
}

fn parse_constraint_columns(raw: &str) -> Option<Vec<String>> {
    let segment = if let Some(open) = raw.find('(') {
        let close = raw[open + 1..].find(')')? + open + 1;
        &raw[open + 1..close]
    } else {
        raw
    };
    let columns = segment
        .split(',')
        .map(normalize_identifier)
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>();
    (!columns.is_empty()).then_some(columns)
}

fn parse_foreign_key_constraint(raw: &str) -> Option<(Vec<String>, String, Vec<String>)> {
    let raw = raw.trim().trim_end_matches(';').trim();
    let references_idx = raw.to_ascii_lowercase().find("references")?;
    let local_columns = parse_constraint_columns(raw[..references_idx].trim())?;
    let right = raw[references_idx + "references".len()..].trim();
    let open = right.find('(')?;
    let referenced_columns = parse_constraint_columns(right)?;
    let referenced_table = right[..open].trim().to_string();
    (!referenced_table.is_empty()).then_some((local_columns, referenced_table, referenced_columns))
}

fn connector_table_identity(
    raw: &str,
    namespace: &Arc<str>,
    instance_id: &ConnectorInstanceId,
) -> Option<ConnectorTableIdentity> {
    let parts = raw
        .split('.')
        .map(normalize_identifier)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let (instance_id, namespace, table) = match parts.as_slice() {
        [table] => (
            instance_id.clone(),
            namespace.clone(),
            Arc::from(table.as_str()),
        ),
        [namespace, table] => (
            instance_id.clone(),
            Arc::from(namespace.as_str()),
            Arc::from(table.as_str()),
        ),
        [catalog, namespace, table] => (
            ConnectorInstanceId::parse(catalog).ok()?,
            Arc::from(namespace.as_str()),
            Arc::from(table.as_str()),
        ),
        _ => return None,
    };
    Some(ConnectorTableIdentity {
        instance_id,
        namespace,
        table,
    })
}

fn normalize_identifier(value: &str) -> String {
    value
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorRequestContext, ConnectorTableColumnRole,
        ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("valid request context")
    }

    #[test]
    fn maps_frozen_iceberg_columns_without_provider_identity() {
        let mut hidden_metadata = std::collections::HashMap::new();
        hidden_metadata.insert(
            CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("row_id", DataType::Int64, false),
            Field::new("internal", DataType::Utf8, true).with_metadata(hidden_metadata),
        ]));
        let metadata_columns = vec!["row_id".to_string()];
        let logical_type_columns =
            BTreeMap::from([(String::from("payload"), String::from("bitmap"))]);
        let namespace = Arc::from("db");
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let context = context();
        let facts = table_planning_facts(IcebergTablePlanningFactsInput {
            schema: &schema,
            metadata_columns: &metadata_columns,
            hidden_columns: &[],
            logical_type_columns: &logical_type_columns,
            serialized_metadata: None,
            namespace: &namespace,
            instance_id: &instance_id,
            context: &context,
        })
        .expect("planning facts");

        assert_eq!(
            facts.column_facts()[0].semantic_kind(),
            ConnectorTableColumnSemanticKind::Bitmap
        );
        assert_eq!(
            facts.column_facts()[1].role(),
            ConnectorTableColumnRole::RowLineageSystem
        );
        assert_eq!(
            facts.column_facts()[2].visibility(),
            ConnectorTableColumnVisibility::Hidden
        );
        assert!(facts.unique_constraints().is_empty());
        assert!(facts.foreign_key_constraints().is_empty());
    }
}
