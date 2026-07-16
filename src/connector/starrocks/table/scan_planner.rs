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

use std::any::Any;
use std::collections::HashSet;

use crate::connector::scan_planning::{ConnectorScanHandle, ConnectorSplit, ScanHandle, Split};
use crate::service::grpc_client::proto::starrocks::{ColumnPb, KeysType, TabletSchemaPb};

const CONNECTOR_ID: &str = "starrocks";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksTableHandle {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
}

impl crate::connector::scan_planning::ConnectorTableHandle for StarRocksTableHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksSplit {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksStorageColumn {
    pub(crate) name: String,
    pub(crate) unique_id: i32,
    pub(crate) default_value: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StarRocksNativeKeysType {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksNativeColumnSchema {
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
    pub(crate) children: Vec<StarRocksNativeColumnSchema>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksNativeTabletSchema {
    pub(crate) schema_id: i64,
    pub(crate) keys_type: StarRocksNativeKeysType,
    pub(crate) num_short_key_columns: Option<i32>,
    pub(crate) sort_key_idxes: Vec<u32>,
    pub(crate) sort_key_unique_ids: Vec<u32>,
    pub(crate) columns: Vec<StarRocksNativeColumnSchema>,
}

#[cfg(test)]
pub(crate) fn test_native_tablet_schema(
    schema_id: i64,
    columns: &[StarRocksStorageColumn],
) -> StarRocksNativeTabletSchema {
    StarRocksNativeTabletSchema {
        schema_id,
        keys_type: StarRocksNativeKeysType::Duplicate,
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
            .map(|(index, column)| StarRocksNativeColumnSchema {
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
                children: Vec::new(),
            })
            .collect(),
    }
}

#[cfg(test)]
pub(crate) fn test_native_tablet_schema_for_column(
    schema_id: i64,
    name: &str,
    unique_id: i32,
    default_value: Option<&str>,
) -> StarRocksNativeTabletSchema {
    test_native_tablet_schema(
        schema_id,
        &[StarRocksStorageColumn {
            name: name.to_string(),
            unique_id,
            default_value: default_value.map(str::to_string),
        }],
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksNativeScanSource {
    pub(crate) catalog_name: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) storage_columns: Vec<StarRocksStorageColumn>,
    pub(crate) tablet_schema: StarRocksNativeTabletSchema,
}

impl ConnectorSplit for StarRocksSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksScanHandle {
    pub(crate) table: StarRocksTableHandle,
    pub(crate) schema_id: i64,
    pub(crate) storage_columns: Vec<StarRocksStorageColumn>,
    pub(crate) tablet_schema: StarRocksNativeTabletSchema,
}

impl StarRocksScanHandle {
    pub(crate) fn native_source(&self) -> StarRocksNativeScanSource {
        StarRocksNativeScanSource {
            catalog_name: super::INTERNAL_CATALOG_NAME.to_string(),
            db_id: self.table.db_id,
            table_id: self.table.table_id,
            schema_id: self.schema_id,
            storage_columns: self.storage_columns.clone(),
            tablet_schema: self.tablet_schema.clone(),
        }
    }
}

fn native_keys_type(raw: Option<i32>) -> Result<StarRocksNativeKeysType, String> {
    match raw.and_then(|value| KeysType::try_from(value).ok()) {
        Some(KeysType::DupKeys) => Ok(StarRocksNativeKeysType::Duplicate),
        Some(KeysType::UniqueKeys) => Ok(StarRocksNativeKeysType::Unique),
        Some(KeysType::AggKeys) => Ok(StarRocksNativeKeysType::Aggregate),
        Some(KeysType::PrimaryKeys) => Ok(StarRocksNativeKeysType::Primary),
        None => Err(format!(
            "StarRocks tablet schema keys_type is missing or unknown: {raw:?}"
        )),
    }
}

fn native_column_schema(
    column: &ColumnPb,
    top_level: bool,
) -> Result<StarRocksNativeColumnSchema, String> {
    let name = column
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    if top_level && name.is_none() {
        return Err("StarRocks top-level tablet schema column name must not be empty".to_string());
    }
    if top_level && column.unique_id < 0 {
        return Err(format!(
            "StarRocks tablet schema column {} unique_id must be non-negative, got {}",
            name.as_deref().unwrap_or("<unnamed>"),
            column.unique_id
        ));
    }
    let physical_type = column.r#type.trim().to_ascii_uppercase();
    if physical_type.is_empty() {
        return Err(format!(
            "StarRocks tablet schema column {} physical type must not be empty",
            name.as_deref().unwrap_or("<unnamed>")
        ));
    }
    let is_key = column.is_key.ok_or_else(|| {
        format!(
            "StarRocks tablet schema column {} is missing required is_key",
            name.as_deref().unwrap_or("<unnamed>")
        )
    })?;
    let nullable = column.is_nullable.ok_or_else(|| {
        format!(
            "StarRocks tablet schema column {} is missing required is_nullable",
            name.as_deref().unwrap_or("<unnamed>")
        )
    })?;
    let visible = column.visible.ok_or_else(|| {
        format!(
            "StarRocks tablet schema column {} is missing required visible",
            name.as_deref().unwrap_or("<unnamed>")
        )
    })?;
    let aggregation = column
        .aggregation
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_uppercase);
    if column.aggregation.is_some() && aggregation.is_none() {
        return Err(format!(
            "StarRocks tablet schema column {} aggregation must not be empty when present",
            name.as_deref().unwrap_or("<unnamed>")
        ));
    }
    let default_value = column
        .default_value
        .as_ref()
        .map(|value| {
            String::from_utf8(value.clone()).map_err(|err| {
                format!(
                    "StarRocks tablet schema column {} default_value is not valid UTF-8: {err}",
                    name.as_deref().unwrap_or("<unnamed>")
                )
            })
        })
        .transpose()?;
    let children = column
        .children_columns
        .iter()
        .map(|child| native_column_schema(child, false))
        .collect::<Result<Vec<_>, _>>()?;
    let expected_children = match physical_type.as_str() {
        "ARRAY" => Some(1),
        "MAP" => Some(2),
        "STRUCT" => None,
        _ => Some(0),
    };
    if let Some(expected) = expected_children
        && children.len() != expected
    {
        return Err(format!(
            "StarRocks tablet schema column {} type {} requires {} children, got {}",
            name.as_deref().unwrap_or("<unnamed>"),
            physical_type,
            expected,
            children.len()
        ));
    }
    if physical_type == "STRUCT" && children.is_empty() {
        return Err(format!(
            "StarRocks tablet schema column {} STRUCT requires at least one child",
            name.as_deref().unwrap_or("<unnamed>")
        ));
    }
    if physical_type == "STRUCT" {
        let mut child_names = HashSet::new();
        let mut positive_child_ids = HashSet::new();
        for child in &children {
            let child_name = child
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    format!(
                        "StarRocks STRUCT column {} child name must not be empty",
                        name.as_deref().unwrap_or("<unnamed>")
                    )
                })?;
            if !child_names.insert(child_name.to_ascii_lowercase()) {
                return Err(format!(
                    "StarRocks STRUCT column {} contains duplicate child name {child_name}",
                    name.as_deref().unwrap_or("<unnamed>")
                ));
            }
            if child.unique_id >= 0 && !positive_child_ids.insert(child.unique_id) {
                return Err(format!(
                    "StarRocks STRUCT column {} contains duplicate positive child unique_id {}",
                    name.as_deref().unwrap_or("<unnamed>"),
                    child.unique_id
                ));
            }
        }
    }
    Ok(StarRocksNativeColumnSchema {
        unique_id: column.unique_id,
        name,
        physical_type,
        is_key,
        aggregation,
        nullable,
        default_value,
        precision: column.precision,
        scale: column.frac,
        visible,
        children,
    })
}

fn native_tablet_schema(
    schema_id: i64,
    schema: &TabletSchemaPb,
) -> Result<StarRocksNativeTabletSchema, String> {
    if schema_id <= 0 || schema.id != Some(schema_id) {
        return Err(format!(
            "StarRocks tablet schema id mismatch: runtime_schema_id={schema_id} tablet_schema_id={:?}",
            schema.id
        ));
    }
    if schema.column.is_empty() {
        return Err("StarRocks tablet schema columns must not be empty".to_string());
    }
    let columns = schema
        .column
        .iter()
        .map(|column| native_column_schema(column, true))
        .collect::<Result<Vec<_>, _>>()?;
    let mut names = HashSet::new();
    let mut unique_ids = HashSet::new();
    for column in &columns {
        let name = column.name.as_deref().expect("top-level name validated");
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!(
                "StarRocks tablet schema columns contain duplicate name {name}"
            ));
        }
        if !unique_ids.insert(column.unique_id) {
            return Err(format!(
                "StarRocks tablet schema columns contain duplicate unique_id {}",
                column.unique_id
            ));
        }
    }
    if let Some(count) = schema.num_short_key_columns
        && (count < 0 || count as usize > columns.len())
    {
        return Err(format!(
            "StarRocks tablet schema num_short_key_columns out of range: {count}"
        ));
    }
    if schema
        .sort_key_idxes
        .iter()
        .any(|index| *index as usize >= columns.len())
    {
        return Err(
            "StarRocks tablet schema sort_key_idxes contains out-of-range index".to_string(),
        );
    }
    for unique_id in &schema.sort_key_unique_ids {
        if !unique_ids.contains(&(*unique_id as i32)) {
            return Err(format!(
                "StarRocks tablet schema sort_key_unique_ids references unknown unique_id {unique_id}"
            ));
        }
    }
    if !schema.sort_key_idxes.is_empty()
        && !schema.sort_key_unique_ids.is_empty()
        && (schema.sort_key_idxes.len() != schema.sort_key_unique_ids.len()
            || schema
                .sort_key_idxes
                .iter()
                .zip(&schema.sort_key_unique_ids)
                .any(|(index, unique_id)| columns[*index as usize].unique_id != *unique_id as i32))
    {
        return Err(
            "StarRocks tablet schema sort key indexes and unique ids are inconsistent".to_string(),
        );
    }
    Ok(StarRocksNativeTabletSchema {
        schema_id,
        keys_type: native_keys_type(schema.keys_type)?,
        num_short_key_columns: schema.num_short_key_columns,
        sort_key_idxes: schema.sort_key_idxes.clone(),
        sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
        columns,
    })
}

impl ConnectorScanHandle for StarRocksScanHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn starrocks_scan_handle(scan: &ScanHandle) -> Result<&StarRocksScanHandle, String> {
    scan.downcast_ref::<StarRocksScanHandle>()
        .ok_or_else(|| "expected StarRocksScanHandle for starrocks scan".to_string())
}

pub(crate) fn starrocks_split(split: &Split) -> Result<&StarRocksSplit, String> {
    split
        .downcast_ref::<StarRocksSplit>()
        .ok_or_else(|| "expected StarRocksSplit for starrocks split".to_string())
}

fn native_storage_columns(schema: &TabletSchemaPb) -> Result<Vec<StarRocksStorageColumn>, String> {
    let mut unique_ids = HashSet::new();
    let mut names = HashSet::new();
    let mut out = Vec::new();
    for column in schema
        .column
        .iter()
        .filter(|column| column.visible != Some(false))
    {
        let name = column
            .name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| "StarRocks storage column name must not be empty".to_string())?;
        if column.unique_id < 0 {
            return Err(format!(
                "StarRocks storage column {name} unique_id must be non-negative, got {}",
                column.unique_id
            ));
        }
        if !unique_ids.insert(column.unique_id) {
            return Err(format!(
                "StarRocks storage columns contain duplicate unique_id {}",
                column.unique_id
            ));
        }
        let normalized_name = name.to_ascii_lowercase();
        if !names.insert(normalized_name) {
            return Err(format!(
                "StarRocks storage columns contain duplicate name {name}"
            ));
        }
        let default_value = column
            .default_value
            .as_ref()
            .map(|value| {
                String::from_utf8(value.clone()).map_err(|err| {
                    format!(
                        "StarRocks storage column {name} default_value is not valid UTF-8: {err}"
                    )
                })
            })
            .transpose()?;
        out.push(StarRocksStorageColumn {
            name: name.to_string(),
            unique_id: column.unique_id,
            default_value,
        });
    }
    if out.is_empty() {
        return Err("StarRocks tablet schema has no visible storage columns".to_string());
    }
    Ok(out)
}

fn validate_runtime_snapshot(
    scan: &StarRocksScanHandle,
    runtime_db_id: i64,
    runtime_table_id: i64,
    runtime_schema_id: i64,
    runtime_storage_columns: &[StarRocksStorageColumn],
    runtime_tablet_schema: &StarRocksNativeTabletSchema,
) -> Result<(), String> {
    if runtime_db_id != scan.table.db_id || runtime_table_id != scan.table.table_id {
        return Err(format!(
            "StarRocks scan runtime identity drift for {}.{}: handle=({}, {}) runtime=({}, {})",
            scan.table.database,
            scan.table.table,
            scan.table.db_id,
            scan.table.table_id,
            runtime_db_id,
            runtime_table_id
        ));
    }
    if runtime_schema_id != scan.schema_id {
        return Err(format!(
            "StarRocks scan runtime schema drift for {}.{}: handle_schema_id={} runtime_schema_id={}",
            scan.table.database, scan.table.table, scan.schema_id, runtime_schema_id
        ));
    }
    if runtime_storage_columns != scan.storage_columns {
        return Err(format!(
            "StarRocks scan runtime storage metadata drift for {}.{} at schema_id={}",
            scan.table.database, scan.table.table, scan.schema_id
        ));
    }
    if runtime_tablet_schema != &scan.tablet_schema {
        return Err(format!(
            "StarRocks scan runtime tablet schema drift for {}.{} at schema_id={}",
            scan.table.database, scan.table.table, scan.schema_id
        ));
    }
    Ok(())
}

use std::sync::{Arc, Weak};

use crate::connector::scan_planning::{
    BeginScanContext, ConnectorScanPlanner, SplitPlanningContext, TableHandle,
};
use crate::engine::StandaloneState;

#[derive(Debug)]
pub(crate) struct StarRocksTableScanPlanner {
    state: Weak<StandaloneState>,
}

impl StarRocksTableScanPlanner {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }

    pub(crate) fn table_handle_from_source(
        database: &str,
        table: &str,
        db_id: i64,
        table_id: i64,
    ) -> TableHandle {
        TableHandle::new(
            CONNECTOR_ID,
            StarRocksTableHandle {
                database: database.to_string(),
                table: table.to_string(),
                db_id,
                table_id,
            },
        )
    }
}

impl ConnectorScanPlanner for StarRocksTableScanPlanner {
    fn name(&self) -> &'static str {
        CONNECTOR_ID
    }

    fn begin_scan(&self, table: TableHandle, _ctx: BeginScanContext) -> Result<ScanHandle, String> {
        let table = table
            .downcast_ref::<StarRocksTableHandle>()
            .ok_or_else(|| "expected StarRocksTableHandle for starrocks scan".to_string())?
            .clone();
        let state = self.state()?;
        let catalog = state
            .starrocks_table
            .read()
            .map_err(|e| format!("starrocks table catalog read lock poisoned: {e}"))?;
        let runtime = catalog.table(&table.database, &table.table)?;
        if runtime.table.db_id != table.db_id || runtime.table.table_id != table.table_id {
            return Err(format!(
                "StarRocks scan table identity mismatch for {}.{}: planned=({}, {}) runtime=({}, {})",
                table.database,
                table.table,
                table.db_id,
                table.table_id,
                runtime.table.db_id,
                runtime.table.table_id
            ));
        }
        let schema_id = runtime.table.current_schema_id;
        Ok(ScanHandle::new(
            CONNECTOR_ID,
            StarRocksScanHandle {
                table,
                schema_id,
                storage_columns: native_storage_columns(&runtime.tablet_schema)?,
                tablet_schema: native_tablet_schema(schema_id, &runtime.tablet_schema)?,
            },
        ))
    }

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        _ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String> {
        let scan = starrocks_scan_handle(scan)?;
        let state = self.state()?;
        let catalog = state
            .starrocks_table
            .read()
            .map_err(|e| format!("starrocks table catalog read lock poisoned: {e}"))?;
        let runtime = catalog.table(&scan.table.database, &scan.table.table)?;
        let runtime_storage_columns = native_storage_columns(&runtime.tablet_schema)?;
        let runtime_tablet_schema =
            native_tablet_schema(runtime.table.current_schema_id, &runtime.tablet_schema)?;
        validate_runtime_snapshot(
            scan,
            runtime.table.db_id,
            runtime.table.table_id,
            runtime.table.current_schema_id,
            &runtime_storage_columns,
            &runtime_tablet_schema,
        )?;
        Ok(super::catalog::starrocks_scan_tablets(runtime)
            .into_iter()
            .map(|tablet| {
                Split::new(
                    CONNECTOR_ID,
                    StarRocksSplit {
                        tablet_id: tablet.tablet_id,
                        partition_id: tablet.partition_id,
                        version: tablet.version,
                    },
                )
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;

    use prost::Message;

    use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};
    use crate::connector::starrocks::table::catalog::StarRocksTableCatalog;
    use crate::connector::starrocks::table::config::StarRocksTableConfig;
    use crate::connector::starrocks::table::model::{
        StarRocksGlobalMeta, StarRocksIndexState, StarRocksPartitionState, StarRocksTableKind,
        StarRocksTableSnapshot, StarRocksTableState, StoredStarRocksDatabase, StoredStarRocksIndex,
        StoredStarRocksPartition, StoredStarRocksSchema, StoredStarRocksTable,
        StoredStarRocksTablet,
    };
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::service::grpc_client::proto::starrocks::{ColumnPb, TabletSchemaPb};

    fn live_scan_test_state() -> Arc<StandaloneState> {
        let tablet_schema = TabletSchemaPb {
            id: Some(30),
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("id".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                is_nullable: Some(false),
                visible: Some(true),
                ..Default::default()
            }],
            num_short_key_columns: Some(1),
            sort_key_idxes: vec![0],
            sort_key_unique_ids: vec![1],
            ..Default::default()
        };
        let snapshot = StarRocksTableSnapshot {
            global: StarRocksGlobalMeta {
                warehouse_uri: "s3://warehouse".to_string(),
                ..Default::default()
            },
            databases: vec![StoredStarRocksDatabase {
                db_id: 10,
                name: "default".to_string(),
            }],
            tables: vec![StoredStarRocksTable {
                table_id: 20,
                db_id: 10,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 30,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            }],
            schemas: vec![StoredStarRocksSchema {
                schema_id: 30,
                table_id: 20,
                schema_version: 1,
                tablet_schema_pb: tablet_schema.encode_to_vec(),
            }],
            partitions: vec![
                StoredStarRocksPartition {
                    partition_id: 100,
                    table_id: 20,
                    name: "active".to_string(),
                    visible_version: 7,
                    next_version: 8,
                    state: StarRocksPartitionState::Active,
                },
                StoredStarRocksPartition {
                    partition_id: 101,
                    table_id: 20,
                    name: "retired".to_string(),
                    visible_version: 9,
                    next_version: 10,
                    state: StarRocksPartitionState::Retired,
                },
            ],
            indexes: vec![
                StoredStarRocksIndex {
                    index_id: 200,
                    table_id: 20,
                    partition_id: 100,
                    index_type: "BASE".to_string(),
                    state: StarRocksIndexState::Active,
                },
                StoredStarRocksIndex {
                    index_id: 201,
                    table_id: 20,
                    partition_id: 100,
                    index_type: "ROLLUP".to_string(),
                    state: StarRocksIndexState::Retired,
                },
            ],
            tablets: vec![
                StoredStarRocksTablet {
                    tablet_id: 300,
                    partition_id: 100,
                    index_id: 200,
                    bucket_seq: 0,
                    tablet_root_path: "s3://warehouse/tablet_300".to_string(),
                },
                StoredStarRocksTablet {
                    tablet_id: 301,
                    partition_id: 101,
                    index_id: 200,
                    bucket_seq: 1,
                    tablet_root_path: "s3://warehouse/tablet_301".to_string(),
                },
                StoredStarRocksTablet {
                    tablet_id: 302,
                    partition_id: 100,
                    index_id: 201,
                    bucket_seq: 2,
                    tablet_root_path: "s3://warehouse/tablet_302".to_string(),
                },
            ],
            ..Default::default()
        };
        let config = StarRocksTableConfig {
            warehouse_uri: "s3://warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "iceberg".to_string(),
        };
        let starrocks = StarRocksTableCatalog::rebuild(Some(config), snapshot)
            .expect("rebuild live scan test catalog");
        Arc::new(StandaloneState {
            starrocks_table: RwLock::new(starrocks),
            ..StandaloneState::default()
        })
    }

    #[test]
    fn downcasts_starrocks_scan_and_split() {
        let scan = ScanHandle::new(
            CONNECTOR_ID,
            StarRocksScanHandle {
                table: StarRocksTableHandle {
                    database: "default".to_string(),
                    table: "orders".to_string(),
                    db_id: 10,
                    table_id: 20,
                },
                schema_id: 30,
                storage_columns: vec![StarRocksStorageColumn {
                    name: "id".to_string(),
                    unique_id: 1,
                    default_value: None,
                }],
                tablet_schema: test_native_tablet_schema_for_column(30, "id", 1, None),
            },
        );
        let splits = vec![Split::new(
            CONNECTOR_ID,
            StarRocksSplit {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            },
        )];

        validate_split_connectors(&scan, &splits).expect("same connector");
        assert_eq!(starrocks_scan_handle(&scan).expect("scan").schema_id, 30);
        assert_eq!(starrocks_split(&splits[0]).expect("split").tablet_id, 300);
    }

    #[test]
    fn real_planner_reads_live_tablets_and_visible_version_after_begin_scan() {
        let state = live_scan_test_state();
        let planner = StarRocksTableScanPlanner::new(&state);
        let table =
            StarRocksTableScanPlanner::table_handle_from_source("default", "orders", 10, 20);
        let scan = planner
            .begin_scan(table, BeginScanContext::default())
            .expect("begin real StarRocks scan");

        state
            .starrocks_table
            .write()
            .expect("StarRocks table catalog write lock")
            .advance_partition_version(100, 8)
            .expect("advance live visible version");

        let splits = planner
            .plan_splits(&scan, SplitPlanningContext::default())
            .expect("plan live StarRocks splits");
        assert_eq!(
            splits.len(),
            1,
            "retired partition/index must stay filtered"
        );
        assert_eq!(
            starrocks_split(&splits[0]).expect("StarRocks split"),
            &StarRocksSplit {
                tablet_id: 300,
                partition_id: 100,
                version: 8,
            }
        );
    }

    #[test]
    fn native_storage_columns_preserve_physical_identity_and_defaults() {
        let schema = TabletSchemaPb {
            column: vec![
                ColumnPb {
                    unique_id: 11,
                    name: Some("order_id".to_string()),
                    r#type: "BIGINT".to_string(),
                    default_value: None,
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 12,
                    name: Some("status".to_string()),
                    r#type: "VARCHAR".to_string(),
                    default_value: Some(b"new".to_vec()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let columns = native_storage_columns(&schema).expect("native storage columns");
        assert_eq!(
            columns,
            vec![
                StarRocksStorageColumn {
                    name: "order_id".to_string(),
                    unique_id: 11,
                    default_value: None,
                },
                StarRocksStorageColumn {
                    name: "status".to_string(),
                    unique_id: 12,
                    default_value: Some("new".to_string()),
                },
            ]
        );
    }

    #[test]
    fn native_storage_columns_reject_invalid_identity() {
        let invalid_cases = [
            (
                vec![ColumnPb {
                    unique_id: -1,
                    name: Some("id".to_string()),
                    r#type: "BIGINT".to_string(),
                    ..Default::default()
                }],
                "unique_id",
            ),
            (
                vec![ColumnPb {
                    unique_id: 1,
                    name: Some(String::new()),
                    r#type: "BIGINT".to_string(),
                    ..Default::default()
                }],
                "name",
            ),
            (
                vec![
                    ColumnPb {
                        unique_id: 1,
                        name: Some("id".to_string()),
                        r#type: "BIGINT".to_string(),
                        ..Default::default()
                    },
                    ColumnPb {
                        unique_id: 1,
                        name: Some("status".to_string()),
                        r#type: "VARCHAR".to_string(),
                        ..Default::default()
                    },
                ],
                "duplicate",
            ),
        ];

        for (columns, expected) in invalid_cases {
            let err = native_storage_columns(&TabletSchemaPb {
                column: columns,
                ..Default::default()
            })
            .expect_err("invalid storage column identity must fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn native_tablet_schema_preserves_read_planning_semantics() {
        let schema = TabletSchemaPb {
            id: Some(30),
            keys_type: Some(KeysType::AggKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(true),
                    is_nullable: Some(false),
                    visible: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("payload".to_string()),
                    r#type: "STRUCT".to_string(),
                    is_key: Some(false),
                    aggregation: Some("REPLACE".to_string()),
                    is_nullable: Some(true),
                    visible: Some(true),
                    children_columns: vec![ColumnPb {
                        unique_id: 3,
                        name: Some("value".to_string()),
                        r#type: "INT".to_string(),
                        is_key: Some(false),
                        is_nullable: Some(false),
                        visible: Some(true),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(1),
            sort_key_idxes: vec![0],
            sort_key_unique_ids: vec![1],
            ..Default::default()
        };

        let native = native_tablet_schema(30, &schema).expect("derive full native schema");
        assert_eq!(native.keys_type, StarRocksNativeKeysType::Aggregate);
        assert!(native.columns[0].is_key);
        assert!(!native.columns[0].visible);
        assert_eq!(native.columns[1].aggregation.as_deref(), Some("REPLACE"));
        assert_eq!(native.columns[1].children[0].physical_type, "INT");
        assert!(!native.columns[1].children[0].nullable);
        assert_eq!(native.sort_key_idxes, vec![0]);
        assert_eq!(native.sort_key_unique_ids, vec![1]);
    }

    #[test]
    fn native_tablet_schema_rejects_missing_semantic_flags_recursively() {
        fn complete_schema() -> TabletSchemaPb {
            TabletSchemaPb {
                id: Some(30),
                keys_type: Some(KeysType::DupKeys as i32),
                column: vec![ColumnPb {
                    unique_id: 1,
                    name: Some("payload".to_string()),
                    r#type: "STRUCT".to_string(),
                    is_key: Some(true),
                    is_nullable: Some(false),
                    visible: Some(true),
                    children_columns: vec![ColumnPb {
                        unique_id: 2,
                        name: Some("value".to_string()),
                        r#type: "INT".to_string(),
                        is_key: Some(false),
                        is_nullable: Some(true),
                        visible: Some(true),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                num_short_key_columns: Some(1),
                sort_key_idxes: vec![0],
                sort_key_unique_ids: vec![1],
                ..Default::default()
            }
        }

        let cases = [
            ("top-level is_key", "is_key", 0_usize, true),
            ("top-level is_nullable", "is_nullable", 0, true),
            ("top-level visible", "visible", 0, true),
            ("nested is_key", "is_key", 0, false),
            ("nested is_nullable", "is_nullable", 0, false),
            ("nested visible", "visible", 0, false),
        ];

        for (case_name, field, child_index, top_level) in cases {
            let mut schema = complete_schema();
            let column = if top_level {
                &mut schema.column[child_index]
            } else {
                &mut schema.column[0].children_columns[child_index]
            };
            match field {
                "is_key" => column.is_key = None,
                "is_nullable" => column.is_nullable = None,
                "visible" => column.visible = None,
                _ => unreachable!(),
            }

            let err = native_tablet_schema(30, &schema)
                .expect_err("missing native schema semantic flag must fail");
            assert!(
                err.contains(field),
                "{case_name}: expected {field} error, got {err}"
            );
        }
    }

    #[test]
    fn scan_snapshot_rejects_runtime_identity_or_schema_drift_before_splitting() {
        let scan = StarRocksScanHandle {
            table: StarRocksTableHandle {
                database: "default".to_string(),
                table: "orders".to_string(),
                db_id: 10,
                table_id: 20,
            },
            schema_id: 30,
            storage_columns: vec![StarRocksStorageColumn {
                name: "id".to_string(),
                unique_id: 1,
                default_value: None,
            }],
            tablet_schema: test_native_tablet_schema_for_column(30, "id", 1, None),
        };

        let identity_err = validate_runtime_snapshot(
            &scan,
            10,
            99,
            30,
            &scan.storage_columns,
            &scan.tablet_schema,
        )
        .expect_err("table identity drift must fail before split planning");
        assert!(identity_err.contains("identity drift"), "{identity_err}");

        let schema_err = validate_runtime_snapshot(
            &scan,
            10,
            20,
            31,
            &scan.storage_columns,
            &scan.tablet_schema,
        )
        .expect_err("schema drift must fail before split planning");
        assert!(schema_err.contains("schema drift"), "{schema_err}");

        let storage_err = validate_runtime_snapshot(
            &scan,
            10,
            20,
            30,
            &[StarRocksStorageColumn {
                name: "id".to_string(),
                unique_id: 2,
                default_value: None,
            }],
            &scan.tablet_schema,
        )
        .expect_err("storage metadata drift must fail before split planning");
        assert!(
            storage_err.contains("storage metadata drift"),
            "{storage_err}"
        );

        let mut semantic_drift = scan.tablet_schema.clone();
        semantic_drift.keys_type = StarRocksNativeKeysType::Aggregate;
        let semantic_err =
            validate_runtime_snapshot(&scan, 10, 20, 30, &scan.storage_columns, &semantic_drift)
                .expect_err("keys_type drift must fail before split planning");
        assert!(
            semantic_err.contains("tablet schema drift"),
            "{semantic_err}"
        );
    }
}
