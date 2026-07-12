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

use super::*;
use crate::sql::planner::payload::PlanValuesNode;
use crate::sql::planner::physical::{PhysicalPlanKind, PhysicalPlanStats, PlannerConfidence};
use std::collections::HashMap;

#[test]
fn pipeline_builds_distributed_plan_from_physical_values() {
    let physical = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
        output_columns: vec![],
        stats: PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Exact,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: vec![],
    };

    let distributed = build_distributed_plan(physical).expect("build DistributedPlan");
    assert_eq!(distributed.fragments.len(), 1);
    assert_eq!(distributed.root_fragment_id, 0);
}
