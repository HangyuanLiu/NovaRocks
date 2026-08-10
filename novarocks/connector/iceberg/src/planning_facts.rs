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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableColumnPlanningFact, ConnectorTableColumnRole,
    ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
    ConnectorTableForeignKeyConstraint, ConnectorTableIdentity, ConnectorTablePlanningFacts,
    ConnectorTableUniqueConstraint,
};

use crate::scan_model::{IcebergDataFileInfo, IcebergDeleteFileContent, IcebergTableInfo};

/// Validate the Iceberg-owned delete facts sealed into planned data files.
///
/// This is deliberately a provider-side validation step: generic planning
/// carries the opaque file facts but must neither infer Iceberg equality-delete
/// identity nor reinterpret table field IDs.  Callers invoke it before a
/// split is frozen for the execution host.
pub fn validate_planned_files(
    table: Option<&IcebergTableInfo>,
    files: &[IcebergDataFileInfo],
) -> Result<(), ConnectorError> {
    for file in files {
        crate::delete_file::validate_delete_apply_cost(file)?;
    }
    let Some(table) = table else {
        return Ok(());
    };

    let mut schema_by_id = BTreeMap::new();
    let mut schema_by_name = BTreeMap::new();
    for field in &table.schema.fields {
        if schema_by_id
            .insert(field.field_id, field.name.clone())
            .is_some()
        {
            return corrupt(format!(
                "Iceberg table schema has duplicate field id {} for table {}",
                field.field_id, table.table
            ));
        }
        if schema_by_name
            .insert(field.name.to_ascii_lowercase(), field.name.clone())
            .is_some()
        {
            return corrupt(format!(
                "Iceberg table schema has duplicate field name {} for table {}",
                field.name, table.table
            ));
        }
    }

    for file in files {
        for delete in &file.delete_files {
            if delete.file_content != IcebergDeleteFileContent::Equality {
                continue;
            }

            let mut ids_seen = BTreeSet::new();
            let mut resolved_ids = Vec::new();
            for field_id in &delete.equality_field_ids {
                if !ids_seen.insert(*field_id) {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} has duplicate equality field id {}",
                        delete.path, field_id
                    ));
                }
                let name = schema_by_id.get(field_id).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "Iceberg equality-delete file {} references unknown field id {} in table {}",
                            delete.path, field_id, table.table
                        ),
                    )
                })?;
                resolved_ids.push(name.to_ascii_lowercase());
            }

            let mut names_seen = BTreeSet::new();
            let mut resolved_names = Vec::new();
            for name in &delete.equality_column_names {
                let normalized = name.to_ascii_lowercase();
                if !names_seen.insert(normalized.clone()) {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} has duplicate equality column name {}",
                        delete.path, name
                    ));
                }
                let canonical = schema_by_name.get(&normalized).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "Iceberg equality-delete file {} references unknown equality column {} in table {}",
                            delete.path, name, table.table
                        ),
                    )
                })?;
                resolved_names.push(canonical.to_ascii_lowercase());
            }

            match (resolved_ids.is_empty(), resolved_names.is_empty()) {
                (true, true) => {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} has no equality field identity",
                        delete.path
                    ));
                }
                (false, false)
                    if resolved_ids.iter().collect::<BTreeSet<_>>()
                        != resolved_names.iter().collect::<BTreeSet<_>>() =>
                {
                    return corrupt(format!(
                        "Iceberg equality-delete file {} field id/name mismatch: ids={resolved_ids:?} names={resolved_names:?}",
                        delete.path
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn corrupt<T>(message: String) -> Result<T, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        message,
    ))
}

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
    use crate::scan_model::{
        IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
        IcebergDeleteFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
    };

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

    #[test]
    fn rejects_duplicate_equality_delete_field_ids_before_split_freeze() {
        let table = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 1,
            location: "s3://warehouse/db/t".to_string(),
            schema: IcebergSchemaDef {
                fields: vec![IcebergSchemaFieldDef {
                    field_id: 7,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                }],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let mut file = IcebergDataFileInfo::for_test("data.parquet", 10, 1);
        file.delete_files.push(IcebergDeleteFileInfo {
            path: "eq-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: None,
            partition_spec_id: None,
            partition_key: None,
            equality_column_names: Vec::new(),
            equality_field_ids: vec![7, 7],
        });

        let error = validate_planned_files(Some(&table), &[file])
            .expect_err("duplicate equality field identity must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("duplicate equality field id 7"));
    }
}
