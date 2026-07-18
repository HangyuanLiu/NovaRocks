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

use crate::connector::ConnectorRegistry;
use crate::connector::scan_model::starrocks::{
    PlannedNativeStarRocksScan, StarRocksColumnSchemaDescriptor, StarRocksKeysTypeDescriptor,
    StarRocksScanSourceDescriptor, StarRocksStorageColumnDescriptor,
    StarRocksTabletSchemaDescriptor, validate_starrocks_source_descriptor,
};
use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext};
use crate::connector::starrocks::table::scan_planner::{
    StarRocksNativeColumnSchema, StarRocksNativeKeysType, StarRocksNativeTabletSchema,
    StarRocksTableScanPlanner, starrocks_scan_handle, starrocks_split,
};
use crate::runtime::scan_range;
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

pub(crate) fn plan_native_starrocks_scan_with_compat(
    scan_node_id: i32,
    scan: &PlanScanNode,
    connectors: &ConnectorRegistry,
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

    let planner = connectors.scan_planner("starrocks")?;
    let table_handle = StarRocksTableScanPlanner::table_handle_from_source(
        &scan.database,
        &scan.table.name,
        *db_id,
        *table_id,
    );
    let scan_handle = planner.begin_scan(table_handle, BeginScanContext::default())?;
    let handle = starrocks_scan_handle(&scan_handle)?;
    if handle.table.db_id != *db_id || handle.table.table_id != *table_id {
        return Err(format!(
            "StarRocks ScanNode node_id={scan_node_id} planned scan handle identity mismatch: source=({db_id}, {table_id}) handle=({}, {})",
            handle.table.db_id, handle.table.table_id
        ));
    }

    let native_source = handle.native_source();
    let source = StarRocksScanSourceDescriptor {
        catalog_name: native_source.catalog_name,
        db_id: native_source.db_id,
        table_id: native_source.table_id,
        schema_id: native_source.schema_id,
        storage_columns: native_source
            .storage_columns
            .into_iter()
            .map(|column| StarRocksStorageColumnDescriptor {
                name: column.name,
                unique_id: column.unique_id,
                default_value: column.default_value,
            })
            .collect(),
        tablet_schema: tablet_schema_descriptor(native_source.tablet_schema),
    };
    validate_starrocks_source_descriptor(scan_node_id, *db_id, *table_id, &source)?;

    let splits = planner.plan_splits(&scan_handle, SplitPlanningContext::default())?;
    if splits.is_empty() {
        return Err(format!(
            "StarRocks table {}.{} has no selected tablet splits",
            scan.database, scan.table.name
        ));
    }
    let mut tablets = HashSet::new();
    let mut ranges = Vec::with_capacity(splits.len());
    for split in &splits {
        let split = starrocks_split(split)?;
        if !tablets.insert(split.tablet_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={scan_node_id} has duplicate tablet_id={}",
                split.tablet_id
            ));
        }
        ranges.push(scan_range::ScanRangeParams::starrocks_tablet(
            split.tablet_id,
            split.partition_id,
            split.version,
        )?);
    }
    Ok(PlannedNativeStarRocksScan { ranges, source })
}

fn tablet_schema_descriptor(
    schema: StarRocksNativeTabletSchema,
) -> StarRocksTabletSchemaDescriptor {
    StarRocksTabletSchemaDescriptor {
        schema_id: schema.schema_id,
        keys_type: match schema.keys_type {
            StarRocksNativeKeysType::Duplicate => StarRocksKeysTypeDescriptor::Duplicate,
            StarRocksNativeKeysType::Unique => StarRocksKeysTypeDescriptor::Unique,
            StarRocksNativeKeysType::Aggregate => StarRocksKeysTypeDescriptor::Aggregate,
            StarRocksNativeKeysType::Primary => StarRocksKeysTypeDescriptor::Primary,
        },
        num_short_key_columns: schema.num_short_key_columns,
        sort_key_idxes: schema.sort_key_idxes,
        sort_key_unique_ids: schema.sort_key_unique_ids,
        columns: schema
            .columns
            .into_iter()
            .map(column_schema_descriptor)
            .collect(),
    }
}

fn column_schema_descriptor(
    column: StarRocksNativeColumnSchema,
) -> StarRocksColumnSchemaDescriptor {
    StarRocksColumnSchemaDescriptor {
        unique_id: column.unique_id,
        name: column.name,
        physical_type: column.physical_type,
        is_key: column.is_key,
        aggregation: column.aggregation,
        nullable: column.nullable,
        default_value: column.default_value,
        precision: column.precision,
        scale: column.scale,
        visible: column.visible,
        children: column
            .children
            .into_iter()
            .map(column_schema_descriptor)
            .collect(),
    }
}
