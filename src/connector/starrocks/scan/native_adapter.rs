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

use std::collections::{BTreeMap, HashSet};

use crate::common::types::UniqueId;
use crate::connector::starrocks::fe_v2_meta::{
    LakeScanTabletRef, LakeTableIdentity, lake_scan_execution_properties,
};
use crate::connector::starrocks::table::INTERNAL_CATALOG_NAME;
use crate::proto::{novarocks, plan};
use crate::runtime::query_context::QueryId;
use crate::service::grpc_client::proto::starrocks::{ColumnPb, KeysType, TabletSchemaPb};

use super::op::{LakeScanSchemaMeta, StarRocksScanRange, StarRocksSchemaColumnHint};

pub(crate) struct NativeStarRocksScanPreparation {
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) ranges: Vec<StarRocksScanRange>,
    pub(crate) lake_schema_meta: LakeScanSchemaMeta,
}

pub(crate) fn prepare_native_starrocks_scan(
    node_id: i32,
    scan: &plan::ScanNode,
    source: &plan::StarRocksTableSource,
    query_id: Option<UniqueId>,
    range_params: &[novarocks::ScanRangeParams],
) -> Result<NativeStarRocksScanPreparation, String> {
    let native_tablet_schema = validate_source(node_id, source)?;
    let ranges = decode_ranges(node_id, range_params)?;
    let tablet_refs = ranges
        .iter()
        .map(|range| LakeScanTabletRef {
            tablet_id: range.tablet_id,
            partition_id: range.partition_id.expect("validated partition_id"),
            version: range.version.expect("validated version"),
        })
        .collect::<Vec<_>>();
    let properties = lake_scan_execution_properties(
        query_id.map(|query_id| QueryId {
            hi: query_id.hi,
            lo: query_id.lo,
        }),
        None,
        &LakeTableIdentity {
            catalog: source.catalog_name.clone(),
            db_name: scan.database.clone(),
            table_name: scan
                .table
                .as_ref()
                .map(|table| table.name.clone())
                .unwrap_or_default(),
            db_id: source.db_id,
            table_id: source.table_id,
            schema_id: source.schema_id,
        },
        &tablet_refs,
    )
    .map_err(|err| {
        format!("StarRocks ScanNode node_id={node_id} resolve tablet paths failed: {err}")
    })?;
    Ok(NativeStarRocksScanPreparation {
        properties,
        ranges,
        lake_schema_meta: LakeScanSchemaMeta {
            db_id: source.db_id,
            table_id: source.table_id,
            schema_id: source.schema_id,
            fe_addr: None,
            query_id,
            native_tablet_schema: Some(native_tablet_schema),
            native_column_hints: Some(
                source
                    .storage_columns
                    .iter()
                    .map(|column| StarRocksSchemaColumnHint {
                        name: column.name.clone(),
                        unique_id: column.unique_id,
                        default_value: column.default_value.clone(),
                    })
                    .collect(),
            ),
        },
    })
}

fn validate_source(
    node_id: i32,
    source: &plan::StarRocksTableSource,
) -> Result<TabletSchemaPb, String> {
    if source.catalog_name.trim().is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} catalog_name must not be empty"
        ));
    }
    if source.catalog_name != INTERNAL_CATALOG_NAME {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} catalog_name must be {INTERNAL_CATALOG_NAME}, got {}",
            source.catalog_name
        ));
    }
    for (field, value) in [
        ("db_id", source.db_id),
        ("table_id", source.table_id),
        ("schema_id", source.schema_id),
    ] {
        if value <= 0 {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} {field} must be positive, got {value}"
            ));
        }
    }
    if source.storage_columns.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} storage_columns must not be empty"
        ));
    }
    let mut names = HashSet::new();
    let mut unique_ids = HashSet::new();
    for column in &source.storage_columns {
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
    decode_native_tablet_schema(node_id, source)
}

fn decode_native_tablet_schema(
    node_id: i32,
    source: &plan::StarRocksTableSource,
) -> Result<TabletSchemaPb, String> {
    let schema = source.current_schema.as_ref().ok_or_else(|| {
        format!("StarRocks ScanNode node_id={node_id} current_schema must be present")
    })?;
    if schema.schema_id != source.schema_id {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema id mismatch: source_schema_id={} current_schema_id={}",
            source.schema_id, schema.schema_id
        ));
    }
    let keys_type = match plan::StarRocksKeysType::try_from(schema.keys_type).ok() {
        Some(plan::StarRocksKeysType::StarrocksKeysTypeDuplicate) => KeysType::DupKeys,
        Some(plan::StarRocksKeysType::StarrocksKeysTypeUnique) => KeysType::UniqueKeys,
        Some(plan::StarRocksKeysType::StarrocksKeysTypeAggregate) => KeysType::AggKeys,
        Some(plan::StarRocksKeysType::StarrocksKeysTypePrimary) => KeysType::PrimaryKeys,
        _ => {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} current schema keys_type is missing or unknown: {}",
                schema.keys_type
            ));
        }
    };
    if schema.columns.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema columns must not be empty"
        ));
    }
    let columns = schema
        .columns
        .iter()
        .map(|column| decode_native_column_schema(node_id, column, true))
        .collect::<Result<Vec<_>, _>>()?;
    let mut names = HashSet::new();
    let mut unique_ids = HashSet::new();
    for column in &columns {
        let name = column.name.as_deref().expect("top-level name validated");
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} current schema contains duplicate column name {name}"
            ));
        }
        if !unique_ids.insert(column.unique_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} current schema contains duplicate unique_id {}",
                column.unique_id
            ));
        }
    }
    if let Some(count) = schema.num_short_key_columns
        && (count < 0 || count as usize > columns.len())
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema num_short_key_columns out of range: {count}"
        ));
    }
    if schema
        .sort_key_idxes
        .iter()
        .any(|index| *index as usize >= columns.len())
    {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema sort_key_idxes contains out-of-range index"
        ));
    }
    for unique_id in &schema.sort_key_unique_ids {
        if !unique_ids.contains(&(*unique_id as i32)) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} current schema sort_key_unique_ids references unknown unique_id {unique_id}"
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
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema sort key indexes and unique ids are inconsistent"
        ));
    }
    let visible_columns = columns
        .iter()
        .filter(|column| column.visible.unwrap_or(true))
        .map(|column| {
            (
                column
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                column.unique_id,
                column
                    .default_value
                    .as_deref()
                    .and_then(|value| std::str::from_utf8(value).ok()),
            )
        })
        .collect::<Vec<_>>();
    let storage_columns = source
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
    if visible_columns != storage_columns {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} storage_columns do not match current schema visible columns"
        ));
    }
    Ok(TabletSchemaPb {
        id: Some(schema.schema_id),
        keys_type: Some(keys_type as i32),
        column: columns,
        num_short_key_columns: schema.num_short_key_columns,
        sort_key_idxes: schema.sort_key_idxes.clone(),
        sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
        ..Default::default()
    })
}

fn decode_native_column_schema(
    node_id: i32,
    column: &plan::StarRocksColumnSchema,
    top_level: bool,
) -> Result<ColumnPb, String> {
    let name = column
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    if top_level && name.is_none() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema top-level column name must not be empty"
        ));
    }
    if top_level && column.unique_id < 0 {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {} unique_id must be non-negative, got {}",
            name.as_deref().unwrap_or("<unnamed>"),
            column.unique_id
        ));
    }
    let physical_type = column.physical_type.trim().to_ascii_uppercase();
    if physical_type.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {} physical_type must not be empty",
            name.as_deref().unwrap_or("<unnamed>")
        ));
    }
    let is_key = column.is_key.ok_or_else(|| {
        format!(
            "StarRocks ScanNode node_id={node_id} current schema column {} missing is_key",
            name.as_deref().unwrap_or("<unnamed>")
        )
    })?;
    let nullable = column.nullable.ok_or_else(|| {
        format!(
            "StarRocks ScanNode node_id={node_id} current schema column {} missing nullable",
            name.as_deref().unwrap_or("<unnamed>")
        )
    })?;
    let visible = column.visible.ok_or_else(|| {
        format!(
            "StarRocks ScanNode node_id={node_id} current schema column {} missing visible",
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
            "StarRocks ScanNode node_id={node_id} current schema column {} aggregation must not be empty when present",
            name.as_deref().unwrap_or("<unnamed>")
        ));
    }
    let children = column
        .children
        .iter()
        .map(|child| decode_native_column_schema(node_id, child, false))
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
            "StarRocks ScanNode node_id={node_id} current schema column {} type {physical_type} requires {expected} children, got {}",
            name.as_deref().unwrap_or("<unnamed>"),
            children.len()
        ));
    }
    if physical_type == "STRUCT" && children.is_empty() {
        return Err(format!(
            "StarRocks ScanNode node_id={node_id} current schema column {} STRUCT requires at least one child",
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
                        "StarRocks ScanNode node_id={node_id} STRUCT column {} child name must not be empty",
                        name.as_deref().unwrap_or("<unnamed>")
                    )
                })?;
            if !child_names.insert(child_name.to_ascii_lowercase()) {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} STRUCT column {} contains duplicate child name {child_name}",
                    name.as_deref().unwrap_or("<unnamed>")
                ));
            }
            if child.unique_id >= 0 && !positive_child_ids.insert(child.unique_id) {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} STRUCT column {} contains duplicate positive child unique_id {}",
                    name.as_deref().unwrap_or("<unnamed>"),
                    child.unique_id
                ));
            }
        }
    }
    Ok(ColumnPb {
        unique_id: column.unique_id,
        name,
        r#type: physical_type,
        is_key: Some(is_key),
        aggregation,
        is_nullable: Some(nullable),
        default_value: column
            .default_value
            .as_ref()
            .map(|value| value.as_bytes().to_vec()),
        precision: column.precision,
        frac: column.scale,
        visible: Some(visible),
        children_columns: children,
        ..Default::default()
    })
}

fn decode_ranges(
    node_id: i32,
    ranges: &[novarocks::ScanRangeParams],
) -> Result<Vec<StarRocksScanRange>, String> {
    let mut tablets = HashSet::new();
    let mut out = Vec::with_capacity(ranges.len());
    for (index, params) in ranges.iter().enumerate() {
        if params.has_more == Some(true) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} range index={index} does not support has_more=true"
            ));
        }
        if params.empty == Some(true) {
            continue;
        }
        let kind = params
            .range
            .as_ref()
            .and_then(|range| range.kind.as_ref())
            .ok_or_else(|| {
                format!("StarRocks ScanNode node_id={node_id} range index={index} missing kind")
            })?;
        let novarocks::scan_range::Kind::StarrocksTablet(range) = kind else {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} range index={index} expected StarRocks tablet range"
            ));
        };
        for (field, value) in [
            ("tablet_id", range.tablet_id),
            ("partition_id", range.partition_id),
            ("version", range.version),
        ] {
            if value <= 0 {
                return Err(format!(
                    "StarRocks ScanNode node_id={node_id} range index={index} {field} must be positive, got {value}"
                ));
            }
        }
        if !tablets.insert(range.tablet_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} has duplicate tablet_id={}",
                range.tablet_id
            ));
        }
        out.push(StarRocksScanRange {
            tablet_id: range.tablet_id,
            partition_id: Some(range.partition_id),
            version: Some(range.version),
        });
    }
    Ok(out)
}
