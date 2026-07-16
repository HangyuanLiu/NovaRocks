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

use crate::runtime::scan_range;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksStorageColumnDescriptor {
    pub(crate) name: String,
    pub(crate) unique_id: i32,
    pub(crate) default_value: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(feature = "compat"), allow(dead_code))]
pub(crate) enum StarRocksKeysTypeDescriptor {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksTabletSchemaDescriptor {
    pub(crate) schema_id: i64,
    pub(crate) keys_type: StarRocksKeysTypeDescriptor,
    pub(crate) num_short_key_columns: Option<i32>,
    pub(crate) sort_key_idxes: Vec<u32>,
    pub(crate) sort_key_unique_ids: Vec<u32>,
    pub(crate) columns: Vec<StarRocksColumnSchemaDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksScanSourceDescriptor {
    pub(crate) catalog_name: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) storage_columns: Vec<StarRocksStorageColumnDescriptor>,
    pub(crate) tablet_schema: StarRocksTabletSchemaDescriptor,
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

#[cfg(feature = "compat")]
pub(crate) fn starrocks_tablet_schema_descriptor(
    schema: crate::connector::starrocks::table::scan_planner::StarRocksNativeTabletSchema,
) -> StarRocksTabletSchemaDescriptor {
    use crate::connector::starrocks::table::scan_planner::StarRocksNativeKeysType;

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
            .map(starrocks_column_schema_descriptor)
            .collect(),
    }
}

#[cfg(feature = "compat")]
fn starrocks_column_schema_descriptor(
    column: crate::connector::starrocks::table::scan_planner::StarRocksNativeColumnSchema,
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
            .map(starrocks_column_schema_descriptor)
            .collect(),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedNativeStarRocksScan {
    pub(crate) ranges: Vec<scan_range::ScanRangeParams>,
    pub(crate) source: StarRocksScanSourceDescriptor,
}

pub(crate) fn plan_native_starrocks_scan_node(
    scan_node_id: i32,
    scan: &crate::sql::planner::payload::PlanScanNode,
    connectors: &crate::connector::ConnectorRegistry,
) -> Result<PlannedNativeStarRocksScan, String> {
    #[cfg(not(feature = "compat"))]
    {
        let _ = (scan_node_id, scan, connectors);
        Err("StarRocks native scan planning requires feature compat".to_string())
    }
    #[cfg(feature = "compat")]
    {
        use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext};
        use crate::connector::starrocks::table::scan_planner::{
            StarRocksTableScanPlanner, starrocks_scan_handle, starrocks_split,
        };
        use crate::sql::planner::table::ScanSource;

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
            tablet_schema: starrocks_tablet_schema_descriptor(native_source.tablet_schema),
        };
        let splits = planner.plan_splits(&scan_handle, SplitPlanningContext::default())?;
        if splits.is_empty() {
            return Err(format!(
                "StarRocks table {}.{} has no selected tablet splits",
                scan.database, scan.table.name
            ));
        }
        let mut tablets = std::collections::HashSet::new();
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
}
