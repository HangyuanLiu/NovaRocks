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

#[cfg(all(test, feature = "compat"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::connector::scan_planning::{ConnectorScanPlanner, ScanHandle, Split, TableHandle};
    use crate::connector::starrocks::table::scan_planner::{
        StarRocksScanHandle, StarRocksSplit, StarRocksStorageColumn, StarRocksTableHandle,
        live_scan_test_state, test_native_tablet_schema_for_column,
    };
    use crate::runtime::scan_range::{ScanRange, StarRocksTabletScanRange};
    use crate::sql::planner::table::TableDef;

    fn scan_node(db_id: i64, table_id: i64) -> PlanScanNode {
        PlanScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "orders".to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::StarRocks { db_id, table_id },
            },
            alias: None,
            columns: Vec::new(),
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn registry_with(planner: Arc<dyn ConnectorScanPlanner>) -> ConnectorRegistry {
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_planner(planner);
        registry
    }

    #[derive(Debug)]
    struct FixturePlanner {
        handle_table: StarRocksTableHandle,
        schema_id: i64,
        splits: Vec<StarRocksSplit>,
    }

    impl FixturePlanner {
        fn valid() -> Self {
            Self {
                handle_table: StarRocksTableHandle {
                    database: "default".to_string(),
                    table: "orders".to_string(),
                    db_id: 10,
                    table_id: 20,
                },
                schema_id: 30,
                splits: vec![StarRocksSplit {
                    tablet_id: 300,
                    partition_id: 100,
                    version: 7,
                }],
            }
        }
    }

    impl ConnectorScanPlanner for FixturePlanner {
        fn name(&self) -> &'static str {
            "starrocks"
        }

        fn begin_scan(
            &self,
            _table: TableHandle,
            _ctx: BeginScanContext,
        ) -> Result<ScanHandle, String> {
            Ok(ScanHandle::new(
                "starrocks",
                StarRocksScanHandle {
                    table: self.handle_table.clone(),
                    schema_id: self.schema_id,
                    storage_columns: vec![StarRocksStorageColumn {
                        name: "id".to_string(),
                        unique_id: 1,
                        default_value: None,
                    }],
                    tablet_schema: test_native_tablet_schema_for_column(
                        self.schema_id,
                        "id",
                        1,
                        None,
                    ),
                },
            ))
        }

        fn plan_splits(
            &self,
            _scan: &ScanHandle,
            _ctx: SplitPlanningContext,
        ) -> Result<Vec<Split>, String> {
            Ok(self
                .splits
                .iter()
                .cloned()
                .map(|split| Split::new("starrocks", split))
                .collect())
        }
    }

    #[test]
    fn real_adapter_plans_live_tablet_version_and_native_schema() {
        let state = live_scan_test_state();
        let registry = registry_with(Arc::new(StarRocksTableScanPlanner::new(&state)));

        let planned = plan_native_starrocks_scan_with_compat(7, &scan_node(10, 20), &registry)
            .expect("plan native StarRocks scan through the real planner");

        assert_eq!(planned.source.db_id, 10);
        assert_eq!(planned.source.table_id, 20);
        assert_eq!(planned.source.schema_id, 30);
        assert_eq!(planned.source.storage_columns[0].name, "id");
        assert_eq!(
            planned
                .ranges
                .iter()
                .map(|range| match &range.range {
                    ScanRange::StarRocksTablet(range) => range.clone(),
                    other => panic!("expected StarRocks tablet range, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            vec![StarRocksTabletScanRange {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            }]
        );
    }

    #[test]
    fn adapter_rejects_planned_handle_identity_mismatch() {
        let mut planner = FixturePlanner::valid();
        planner.handle_table.table_id = 99;
        let registry = registry_with(Arc::new(planner));

        let error = plan_native_starrocks_scan_with_compat(7, &scan_node(10, 20), &registry)
            .expect_err("identity mismatch must fail closed");

        assert!(
            error.contains("planned scan handle identity mismatch"),
            "{error}"
        );
    }

    #[test]
    fn adapter_rejects_duplicate_tablet_splits() {
        let mut planner = FixturePlanner::valid();
        planner.splits.push(StarRocksSplit {
            tablet_id: 300,
            partition_id: 101,
            version: 8,
        });
        let registry = registry_with(Arc::new(planner));

        let error = plan_native_starrocks_scan_with_compat(7, &scan_node(10, 20), &registry)
            .expect_err("duplicate tablet must fail closed");

        assert!(error.contains("duplicate tablet_id=300"), "{error}");
    }

    #[test]
    fn adapter_rejects_zero_splits() {
        let mut planner = FixturePlanner::valid();
        planner.splits.clear();
        let registry = registry_with(Arc::new(planner));

        let error = plan_native_starrocks_scan_with_compat(7, &scan_node(10, 20), &registry)
            .expect_err("empty split planning must fail closed");

        assert!(error.contains("has no selected tablet splits"), "{error}");
    }

    #[test]
    fn adapter_propagates_descriptor_validation_errors() {
        let mut planner = FixturePlanner::valid();
        planner.schema_id = 0;
        let registry = registry_with(Arc::new(planner));

        let error = plan_native_starrocks_scan_with_compat(7, &scan_node(10, 20), &registry)
            .expect_err("invalid native descriptor must fail validation");

        assert!(
            error.contains("native source schema_id must be positive"),
            "{error}"
        );
    }
}
