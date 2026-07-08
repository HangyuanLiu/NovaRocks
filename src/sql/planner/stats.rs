#![allow(dead_code)]
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

use std::collections::HashMap;

use crate::sql::column_id::ColumnId;

/// Cost display weights (sunk from optimizer; EXPLAIN formats a scalar total
/// from the planner-owned PlannerCostEstimate using these).
pub(crate) const DEFAULT_CPU_COST_WEIGHT: f64 = 0.5;
pub(crate) const DEFAULT_MEMORY_COST_WEIGHT: f64 = 2.0;
pub(crate) const DEFAULT_NETWORK_COST_WEIGHT: f64 = 1.5;

/// Row-count sentinel ceiling for EXPLAIN stats trailer.
pub(crate) const MAX_ROW_COUNT: f64 = 1e15;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) enum PlannerConfidence {
    #[default]
    Fallback,
    Estimated,
    Exact,
    Measured,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PlannerColumnStatistic {
    pub min_value: f64,
    pub max_value: f64,
    pub nulls_fraction: f64,
    pub average_row_size: f64,
    pub ndv: Option<f64>,
    pub confidence: PlannerConfidence,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct PlannerCostEstimate {
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub network_cost: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PlannerBroadcastDecision {
    pub feasible: bool,
    pub forced: bool,
    pub build_bytes: f64,
    pub hash_table_bytes: f64,
    pub effective_backend_count: f64,
    pub risk_adj_fanout_bytes: f64,
    pub per_node_budget_bytes: f64,
    pub cluster_network_budget_bytes: f64,
    pub risk_multiplier: f64,
    pub reject_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalPlanStats {
    pub output_row_count: f64,
    pub row_count_confidence: PlannerConfidence,
    pub column_statistics: HashMap<ColumnId, PlannerColumnStatistic>,
    pub cost_estimate: Option<PlannerCostEstimate>,
    pub broadcast_decision: Option<PlannerBroadcastDecision>,
}
