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
use crate::connector::scan_model::starrocks::PlannedNativeStarRocksScan;
use crate::sql::planner::payload::PlanScanNode;

pub(crate) fn plan_native_starrocks_scan(
    scan_node_id: i32,
    scan: &PlanScanNode,
    connectors: &ConnectorRegistry,
) -> Result<PlannedNativeStarRocksScan, String> {
    #[cfg(not(feature = "compat"))]
    {
        let _ = (scan_node_id, scan, connectors);
        Err("StarRocks native scan planning requires feature compat".to_string())
    }
    #[cfg(feature = "compat")]
    {
        crate::connector::starrocks::table::scan_adapter::plan_native_starrocks_scan_with_compat(
            scan_node_id,
            scan,
            connectors,
        )
    }
}
