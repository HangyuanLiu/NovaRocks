// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to You under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Iceberg control-plane file planning.
//!
//! This module owns static file pruning and validation of the provider facts
//! that will be sealed into opaque splits. Generic query preparation may
//! supply SQL facts, but it must not decode the resulting split payloads.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use novarocks_connector_iceberg::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergTableInfo,
};

pub(crate) fn validate_planned_files(
    table: Option<&IcebergTableInfo>,
    files: &[IcebergDataFileInfo],
) -> Result<(), ConnectorError> {
    for file in files {
        novarocks_connector_iceberg::delete_file::validate_delete_apply_cost(file)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use novarocks_connector_iceberg::scan_model::{
        IcebergDeleteFileFormat, IcebergDeleteFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef,
    };

    /// The same field identities the planned-files fixtures publish: field IDs
    /// are the frozen schema ordinal plus one, so `3` names `v`, not `id`.
    fn table_info() -> IcebergTableInfo {
        let fields = ["id", "category", "v"]
            .into_iter()
            .enumerate()
            .map(|(ordinal, name)| IcebergSchemaFieldDef {
                field_id: i32::try_from(ordinal + 1).expect("schema field ID"),
                name: name.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            })
            .collect();
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("fixture-test_table".to_string()),
            current_snapshot_id: Some(1),
            schema_id: 1,
            location: "s3://fixture/test_db/test_table".to_string(),
            schema: IcebergSchemaDef { fields },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn delete_file(
        path: &str,
        file_content: IcebergDeleteFileContent,
        equality_column_names: Vec<&str>,
        equality_field_ids: Vec<i32>,
    ) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: path.to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            equality_column_names: equality_column_names
                .into_iter()
                .map(str::to_string)
                .collect(),
            equality_field_ids,
        }
    }

    fn equality_delete_file(
        equality_column_names: Vec<&str>,
        equality_field_ids: Vec<i32>,
    ) -> IcebergDeleteFileInfo {
        delete_file(
            "s3://bucket/eq-delete.parquet",
            IcebergDeleteFileContent::Equality,
            equality_column_names,
            equality_field_ids,
        )
    }

    fn data_file(delete_files: Vec<IcebergDeleteFileInfo>) -> IcebergDataFileInfo {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 128, 10);
        file.partition_spec_id = Some(0);
        file.partition_key = Some("Struct([])".to_string());
        file.data_sequence_number = Some(1);
        file.delete_files = delete_files;
        file
    }

    fn validation_error(files: Vec<IcebergDataFileInfo>) -> String {
        match validate_planned_files(Some(&table_info()), &files) {
            Ok(()) => panic!("invalid Iceberg planned files must fail validation"),
            Err(error) => error.to_string(),
        }
    }

    /// The name-keyed counterpart of the unknown-field-id case below: an
    /// equality column absent from the frozen schema is equally unresolvable.
    #[test]
    fn equality_delete_unknown_column_name_is_native_planning_error() {
        let err = validation_error(vec![data_file(vec![equality_delete_file(
            vec!["not_a_column"],
            Vec::new(),
        )])]);

        assert!(
            err.contains("unknown equality column not_a_column"),
            "{err}"
        );
    }

    /// An equality key whose field ID is absent from the frozen table schema is
    /// unresolvable provider metadata, so planning must refuse it rather than
    /// seal an unreadable key into a split.
    #[test]
    fn equality_delete_unknown_field_id_is_native_planning_error() {
        let err = validation_error(vec![data_file(vec![equality_delete_file(
            Vec::new(),
            vec![99],
        )])]);

        assert!(err.contains("unknown field id 99"), "{err}");
    }

    /// A repeated equality identity is ambiguous whether it repeats by field ID
    /// or by column name in a different case.
    #[test]
    fn equality_delete_duplicate_identity_is_native_planning_error() {
        for delete in [
            equality_delete_file(Vec::new(), vec![3, 3]),
            equality_delete_file(vec!["category", "CATEGORY"], Vec::new()),
        ] {
            let err = validation_error(vec![data_file(vec![delete])]);
            assert!(err.contains("duplicate equality"), "{err}");
        }
    }

    /// When a delete file carries both forms of identity they must agree: field
    /// ID `3` names `v`, so pairing it with the column name `id` is corrupt
    /// provider metadata rather than something planning may reconcile.
    #[test]
    fn equality_delete_field_id_and_name_mismatch_is_native_planning_error() {
        let err = validation_error(vec![data_file(vec![equality_delete_file(
            vec!["id"],
            vec![3],
        )])]);

        assert!(err.contains("field id/name mismatch"), "{err}");
    }

    /// The bounded delete-apply cost is enforced before any split is frozen, so
    /// a data file with more delete files than a reader may apply must fail
    /// planning instead of reaching execution.
    #[test]
    fn excessive_delete_apply_cost_is_native_planning_error() {
        let deletes = (0..1025)
            .map(|idx| {
                delete_file(
                    &format!("s3://bucket/delete-{idx}.parquet"),
                    IcebergDeleteFileContent::Position,
                    Vec::new(),
                    Vec::new(),
                )
            })
            .collect();

        let err = validation_error(vec![data_file(deletes)]);

        assert!(err.contains("too many Iceberg delete files"), "{err}");
    }
}
