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

use std::collections::HashSet;

use crate::runtime::scan_range;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct StarRocksStorageColumnDescriptor {
    pub(crate) name: String,
    pub(crate) unique_id: i32,
    pub(crate) default_value: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(not(feature = "compat"), allow(dead_code))]
pub(crate) enum StarRocksKeysTypeDescriptor {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct StarRocksColumnSchemaDescriptor {
    pub(crate) unique_id: i32,
    pub(crate) name: Option<String>,
    pub(crate) physical_type: String,
    pub(crate) is_key: bool,
    pub(crate) aggregation: Option<String>,
    pub(crate) nullable: bool,
    pub(crate) default_value: Option<String>,
    pub(crate) precision: Option<i32>,
    pub(crate) scale: Option<i32>,
    pub(crate) visible: bool,
    pub(crate) children: Vec<StarRocksColumnSchemaDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct StarRocksTabletSchemaDescriptor {
    pub(crate) schema_id: i64,
    pub(crate) keys_type: StarRocksKeysTypeDescriptor,
    pub(crate) num_short_key_columns: Option<i32>,
    pub(crate) sort_key_idxes: Vec<u32>,
    pub(crate) sort_key_unique_ids: Vec<u32>,
    pub(crate) columns: Vec<StarRocksColumnSchemaDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct StarRocksScanSourceDescriptor {
    pub(crate) catalog_name: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) storage_columns: Vec<StarRocksStorageColumnDescriptor>,
    pub(crate) tablet_schema: StarRocksTabletSchemaDescriptor,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedNativeStarRocksScan {
    pub(crate) ranges: Vec<scan_range::ScanRangeParams>,
    pub(crate) source: StarRocksScanSourceDescriptor,
}

pub(crate) fn validate_starrocks_source_descriptor(
    node_id: i32,
    expected_db_id: i64,
    expected_table_id: i64,
    descriptor: &StarRocksScanSourceDescriptor,
) -> Result<(), String> {
    if descriptor.db_id != expected_db_id || descriptor.table_id != expected_table_id {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native source identity mismatch: plan=({expected_db_id}, {expected_table_id}) descriptor=({}, {})",
            descriptor.db_id, descriptor.table_id
        ));
    }
    if descriptor.catalog_name.trim().is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native source catalog_name must not be empty"
        ));
    }
    for (field, value) in [
        ("db_id", descriptor.db_id),
        ("table_id", descriptor.table_id),
        ("schema_id", descriptor.schema_id),
    ] {
        if value <= 0 {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native source {field} must be positive, got {value}"
            ));
        }
    }
    if descriptor.tablet_schema.schema_id != descriptor.schema_id {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema id mismatch: source_schema_id={} current_schema_id={}",
            descriptor.schema_id, descriptor.tablet_schema.schema_id
        ));
    }
    if descriptor.tablet_schema.columns.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema columns must not be empty"
        ));
    }
    let mut current_names = HashSet::new();
    let mut current_unique_ids = HashSet::new();
    for column in &descriptor.tablet_schema.columns {
        let name = column.name.as_deref().unwrap_or_default().trim();
        if !current_names.insert(name.to_ascii_lowercase()) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native current schema contains duplicate column name {name}"
            ));
        }
        if !current_unique_ids.insert(column.unique_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native current schema contains duplicate unique_id {}",
                column.unique_id
            ));
        }
    }
    if let Some(count) = descriptor.tablet_schema.num_short_key_columns
        && (count < 0 || count as usize > descriptor.tablet_schema.columns.len())
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema num_short_key_columns out of range: {count}"
        ));
    }
    for unique_id in &descriptor.tablet_schema.sort_key_unique_ids {
        if !current_unique_ids.contains(&(*unique_id as i32)) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} native current schema sort_key_unique_ids references unknown unique_id {unique_id}"
            ));
        }
    }
    if descriptor
        .tablet_schema
        .sort_key_idxes
        .iter()
        .any(|index| *index as usize >= descriptor.tablet_schema.columns.len())
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema sort_key_idxes contains out-of-range index"
        ));
    }
    if !descriptor.tablet_schema.sort_key_idxes.is_empty()
        && !descriptor.tablet_schema.sort_key_unique_ids.is_empty()
        && (descriptor.tablet_schema.sort_key_idxes.len()
            != descriptor.tablet_schema.sort_key_unique_ids.len()
            || descriptor
                .tablet_schema
                .sort_key_idxes
                .iter()
                .zip(&descriptor.tablet_schema.sort_key_unique_ids)
                .any(|(index, unique_id)| {
                    descriptor.tablet_schema.columns[*index as usize].unique_id != *unique_id as i32
                }))
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native current schema sort key indexes and unique ids are inconsistent"
        ));
    }
    for column in &descriptor.tablet_schema.columns {
        validate_starrocks_schema_column(node_id, column, true)?;
    }
    if descriptor.storage_columns.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native source storage_columns must not be empty"
        ));
    }
    let mut names = HashSet::new();
    let mut unique_ids = HashSet::new();
    for column in &descriptor.storage_columns {
        let name = column.name.trim();
        if name.is_empty() {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage column name must not be empty"
            ));
        }
        if column.unique_id < 0 {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage column {name} unique_id must be non-negative, got {}",
                column.unique_id
            ));
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage columns contain duplicate name {name}"
            ));
        }
        if !unique_ids.insert(column.unique_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} storage columns contain duplicate unique_id {}",
                column.unique_id
            ));
        }
    }
    let current_visible_columns = descriptor
        .tablet_schema
        .columns
        .iter()
        .filter(|column| column.visible)
        .map(|column| {
            (
                column
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                column.unique_id,
                column.default_value.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    let storage_columns = descriptor
        .storage_columns
        .iter()
        .map(|column| {
            (
                column.name.to_ascii_lowercase(),
                column.unique_id,
                column.default_value.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    if current_visible_columns != storage_columns {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} native storage_columns do not match current schema visible columns"
        ));
    }
    Ok(())
}

fn validate_starrocks_schema_column(
    node_id: i32,
    column: &StarRocksColumnSchemaDescriptor,
    top_level: bool,
) -> Result<(), String> {
    let name = column.name.as_deref().map(str::trim).unwrap_or_default();
    if top_level && name.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema top-level column name must not be empty"
        ));
    }
    if top_level && column.unique_id < 0 {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} unique_id must be non-negative"
        ));
    }
    let physical_type = column.physical_type.trim().to_ascii_uppercase();
    if physical_type.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} physical_type must not be empty"
        ));
    }
    let expected_children = match physical_type.as_str() {
        "ARRAY" => Some(1),
        "MAP" => Some(2),
        "STRUCT" => None,
        _ => Some(0),
    };
    if let Some(expected) = expected_children
        && column.children.len() != expected
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} type {physical_type} requires {expected} children, got {}",
            column.children.len()
        ));
    }
    if physical_type == "STRUCT" && column.children.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {name} STRUCT requires at least one child"
        ));
    }
    if physical_type == "STRUCT" {
        let mut child_names = HashSet::new();
        let mut positive_child_ids = HashSet::new();
        for child in &column.children {
            let child_name = child.name.as_deref().map(str::trim).unwrap_or_default();
            if child_name.is_empty() {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} current schema STRUCT column {name} child name must not be empty"
                ));
            }
            if !child_names.insert(child_name.to_ascii_lowercase()) {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} current schema STRUCT column {name} contains duplicate child name {child_name}"
                ));
            }
            if child.unique_id >= 0 && !positive_child_ids.insert(child.unique_id) {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} current schema STRUCT column {name} contains duplicate positive child unique_id {}",
                    child.unique_id
                ));
            }
        }
    }
    for child in &column.children {
        validate_starrocks_schema_column(node_id, child, false)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_starrocks_tablet_schema_descriptor(
    schema_id: i64,
    columns: &[StarRocksStorageColumnDescriptor],
) -> StarRocksTabletSchemaDescriptor {
    StarRocksTabletSchemaDescriptor {
        schema_id,
        keys_type: StarRocksKeysTypeDescriptor::Duplicate,
        num_short_key_columns: Some(1.min(columns.len()) as i32),
        sort_key_idxes: if columns.is_empty() { vec![] } else { vec![0] },
        sort_key_unique_ids: columns
            .first()
            .map(|column| column.unique_id as u32)
            .into_iter()
            .collect(),
        columns: columns
            .iter()
            .enumerate()
            .map(|(index, column)| StarRocksColumnSchemaDescriptor {
                unique_id: column.unique_id,
                name: Some(column.name.clone()),
                physical_type: "BIGINT".to_string(),
                is_key: index == 0,
                aggregation: None,
                nullable: true,
                default_value: column.default_value.clone(),
                precision: None,
                scale: None,
                visible: true,
                children: vec![],
            })
            .collect(),
    }
}

#[cfg(test)]
pub(crate) fn test_starrocks_tablet_schema_descriptor_for_column(
    schema_id: i64,
    name: &str,
    unique_id: i32,
    default_value: Option<&str>,
) -> StarRocksTabletSchemaDescriptor {
    test_starrocks_tablet_schema_descriptor(
        schema_id,
        &[StarRocksStorageColumnDescriptor {
            name: name.to_string(),
            unique_id,
            default_value: default_value.map(str::to_string),
        }],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_descriptor() -> StarRocksScanSourceDescriptor {
        StarRocksScanSourceDescriptor {
            catalog_name: "default_catalog".to_string(),
            db_id: 1,
            table_id: 2,
            schema_id: 3,
            storage_columns: vec![StarRocksStorageColumnDescriptor {
                name: "id".to_string(),
                unique_id: 11,
                default_value: None,
            }],
            tablet_schema: test_starrocks_tablet_schema_descriptor_for_column(3, "id", 11, None),
        }
    }

    #[test]
    fn connector_model_validates_starrocks_source_before_encoding() {
        validate_starrocks_source_descriptor(7, 1, 2, &valid_descriptor())
            .expect("valid connector-owned StarRocks descriptor");

        let mut identity_mismatch = valid_descriptor();
        identity_mismatch.table_id = 99;
        let err = validate_starrocks_source_descriptor(7, 1, 2, &identity_mismatch)
            .expect_err("identity mismatch must fail at connector boundary");
        assert_eq!(
            err,
            "StarRocks ScanNode node_id=7 native source identity mismatch: plan=(1, 2) descriptor=(1, 99)"
        );

        let mut empty_catalog = valid_descriptor();
        empty_catalog.catalog_name.clear();
        let err = validate_starrocks_source_descriptor(7, 1, 2, &empty_catalog)
            .expect_err("empty catalog must fail at connector boundary");
        assert_eq!(
            err,
            "StarRocks ScanNode node_id=7 native source catalog_name must not be empty"
        );

        let mut invalid_schema = valid_descriptor();
        invalid_schema.schema_id = 0;
        invalid_schema.tablet_schema.schema_id = 0;
        let err = validate_starrocks_source_descriptor(7, 1, 2, &invalid_schema)
            .expect_err("invalid schema id must fail at connector boundary");
        assert_eq!(
            err,
            "StarRocks ScanNode node_id=7 native source schema_id must be positive, got 0"
        );

        let mut duplicate_storage_id = valid_descriptor();
        duplicate_storage_id
            .storage_columns
            .push(StarRocksStorageColumnDescriptor {
                name: "flag".to_string(),
                unique_id: 11,
                default_value: None,
            });
        let err = validate_starrocks_source_descriptor(7, 1, 2, &duplicate_storage_id)
            .expect_err("duplicate storage id must fail at connector boundary");
        assert_eq!(
            err,
            "StarRocks ScanNode node_id=7 storage columns contain duplicate unique_id 11"
        );
    }
}
