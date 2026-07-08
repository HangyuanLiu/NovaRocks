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

use crate::connector::scan_planning::{ConnectorScanHandle, ConnectorSplit, ScanHandle, Split};

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

impl ConnectorSplit for StarRocksSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksScanHandle {
    pub(crate) table: StarRocksTableHandle,
    pub(crate) schema_id: i64,
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
        Ok(ScanHandle::new(
            CONNECTOR_ID,
            StarRocksScanHandle {
                table,
                schema_id: runtime.table.current_schema_id,
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
        let layout = super::catalog::starrocks_table_physical_layout(runtime)?;
        Ok(layout
            .tablets
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
    use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};

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
}
