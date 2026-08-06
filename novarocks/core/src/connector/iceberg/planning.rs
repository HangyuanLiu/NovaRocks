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
        super::reader::validate_delete_apply_cost(file)?;
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
