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

use crate::connector::ConnectorRegistry;
use crate::connector::scan_model::starrocks::{
    PlannedNativeStarRocksScan, StarRocksColumnSchemaDescriptor, StarRocksKeysTypeDescriptor,
    StarRocksScanSourceDescriptor, StarRocksStorageColumnDescriptor,
    StarRocksTabletSchemaDescriptor, validate_starrocks_source_descriptor,
};
use crate::connector::starrocks::schema::{
    StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
};
use crate::runtime::scan_range;
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;
use novarocks_spi::connector::ConnectorRequestContext;
use std::collections::HashSet;

pub(crate) fn plan_native_starrocks_scan_with_compat(
    scan_node_id: i32,
    scan: &PlanScanNode,
    connectors: &ConnectorRegistry,
    context: ConnectorRequestContext,
) -> Result<PlannedNativeStarRocksScan, String> {
    let ScanSource::StarRocks { db_id, table_id } = &scan.table.source else {
        return Err(format!(
            "native StarRocks scan planning node_id={scan_node_id} received non-StarRocks source"
        ));
    };
    for (field, value) in [("db_id", *db_id), ("table_id", *table_id)] {
        if value <= 0 {
            return Err(format!(
                "StarRocks ScanNode node_id={scan_node_id} {field} must be positive, got {value}"
            ));
        }
    }

    if context.cancellation().is_cancelled() {
        return Err("StarRocks scan planning was cancelled".to_string());
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err("StarRocks scan planning deadline elapsed".to_string());
    }
    let state = connectors.starrocks_table_state()?;
    let runtime = state
        .starrocks_table
        .read()
        .map_err(|error| format!("StarRocks table catalog read lock: {error}"))?
        .table(&scan.database, &scan.table.name)?
        .clone();
    if runtime.table.db_id != *db_id || runtime.table.table_id != *table_id {
        return Err(format!(
            "StarRocks ScanNode node_id={scan_node_id} table identity mismatch: source=({db_id}, {table_id}) runtime=({}, {})",
            runtime.table.db_id, runtime.table.table_id
        ));
    }

    let source = source_descriptor(&runtime)?;
    validate_starrocks_source_descriptor(scan_node_id, *db_id, *table_id, &source)?;

    let scan_tablets = super::catalog::starrocks_scan_tablets(&runtime);
    if scan_tablets.is_empty() {
        return Err(format!(
            "StarRocks table {}.{} has no selected tablet splits",
            scan.database, scan.table.name
        ));
    }
    let mut tablet_ids = HashSet::new();
    let mut ranges = Vec::with_capacity(scan_tablets.len());
    for tablet in scan_tablets {
        if !tablet_ids.insert(tablet.tablet_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={scan_node_id} has duplicate tablet_id={}",
                tablet.tablet_id
            ));
        }
        ranges.push(scan_range::ScanRangeParams::starrocks_tablet(
            tablet.tablet_id,
            tablet.partition_id,
            tablet.version,
        )?);
    }
    Ok(PlannedNativeStarRocksScan { ranges, source })
}

pub(crate) fn source_descriptor(
    runtime: &super::catalog::StarRocksTableRuntime,
) -> Result<StarRocksScanSourceDescriptor, String> {
    let storage_columns = runtime
        .tablet_schema
        .column
        .iter()
        .filter(|column| column.visible != Some(false))
        .map(|column| {
            let name = column
                .name
                .clone()
                .filter(|name| !name.trim().is_empty())
                .ok_or_else(|| "StarRocks storage column name must not be empty".to_string())?;
            Ok(StarRocksStorageColumnDescriptor {
                name,
                unique_id: column.unique_id,
                default_value: column
                    .default_value
                    .as_ref()
                    .map(|value| String::from_utf8(value.clone()))
                    .transpose()
                    .map_err(|error| {
                        format!("StarRocks storage column default is not valid UTF-8: {error}")
                    })?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(StarRocksScanSourceDescriptor {
        catalog_name: super::INTERNAL_CATALOG_NAME.to_string(),
        db_id: runtime.table.db_id,
        table_id: runtime.table.table_id,
        schema_id: runtime.table.current_schema_id,
        storage_columns,
        tablet_schema: tablet_schema_descriptor(runtime.tablet_schema.clone()),
    })
}

fn tablet_schema_descriptor(schema: StarRocksTabletSchema) -> StarRocksTabletSchemaDescriptor {
    StarRocksTabletSchemaDescriptor {
        schema_id: schema.id.unwrap_or_default(),
        keys_type: match schema.keys_type {
            Some(StarRocksKeysType::Duplicate) => StarRocksKeysTypeDescriptor::Duplicate,
            Some(StarRocksKeysType::Unique) => StarRocksKeysTypeDescriptor::Unique,
            Some(StarRocksKeysType::Aggregate) => StarRocksKeysTypeDescriptor::Aggregate,
            Some(StarRocksKeysType::Primary) => StarRocksKeysTypeDescriptor::Primary,
            None => StarRocksKeysTypeDescriptor::Duplicate,
        },
        num_short_key_columns: schema.num_short_key_columns,
        sort_key_idxes: schema.sort_key_idxes,
        sort_key_unique_ids: schema.sort_key_unique_ids,
        columns: schema
            .column
            .into_iter()
            .map(column_schema_descriptor)
            .collect(),
    }
}

fn column_schema_descriptor(column: StarRocksColumnSchema) -> StarRocksColumnSchemaDescriptor {
    StarRocksColumnSchemaDescriptor {
        unique_id: column.unique_id,
        name: column.name,
        physical_type: column.r#type,
        is_key: column.is_key.unwrap_or(false),
        aggregation: column.aggregation,
        nullable: column.is_nullable.unwrap_or(true),
        default_value: column
            .default_value
            .map(|value| String::from_utf8_lossy(&value).into_owned()),
        precision: column.precision,
        scale: column.frac,
        visible: column.visible.unwrap_or(true),
        children: column
            .children_columns
            .into_iter()
            .map(column_schema_descriptor)
            .collect(),
    }
}
