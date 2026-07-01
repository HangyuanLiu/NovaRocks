#![allow(dead_code)]

use std::collections::HashMap;

use crate::sql::column_id::ColumnId;

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
