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
