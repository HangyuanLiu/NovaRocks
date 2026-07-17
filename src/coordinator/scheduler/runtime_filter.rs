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

//! Scheduler-stage runtime-filter plan metadata.

use std::collections::HashMap;

use crate::sql::planner::distributed::FragmentId;
use crate::sql::planner::distributed::runtime_filter::RuntimeFilterGraphProjection;
use crate::sql::planner::physical::JoinExecutionMode;

#[derive(Clone, Debug)]
pub(crate) struct PlannedRuntimeFilter {
    pub filter_id: i32,
    pub build_plan_node_id: i32,
    pub probe_target_node_ids: Vec<i32>,
    pub has_remote_targets: bool,
    pub execution_mode: JoinExecutionMode,
    pub expr_order: i32,
}

pub(crate) struct RuntimeFilterPlanResult {
    pub(crate) all_filters: HashMap<i32, PlannedRuntimeFilter>,
    pub(crate) build_side_filters: HashMap<FragmentId, Vec<i32>>,
    pub(crate) probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>>,
}

pub(crate) fn plan_runtime_filters(
    projection: &RuntimeFilterGraphProjection,
) -> Result<Option<RuntimeFilterPlanResult>, String> {
    runtime_filter_plan(projection)
}

fn runtime_filter_plan(
    projection: &RuntimeFilterGraphProjection,
) -> Result<Option<RuntimeFilterPlanResult>, String> {
    let mut all_filters = HashMap::new();
    let mut build_side_filters: HashMap<FragmentId, Vec<i32>> = HashMap::new();
    let mut probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>> = HashMap::new();
    for ((fragment_id, node_id), builds) in projection.builds() {
        for build in builds {
            let expr_order = i32::try_from(build.expr_order).map_err(|_| {
                format!(
                    "runtime filter channel {} expression order {} does not fit i32",
                    build.channel_id.get(),
                    build.expr_order
                )
            })?;
            let mut targets = projection
                .probes()
                .flat_map(|((target_fragment_id, target_node_id), probes)| {
                    probes
                        .iter()
                        .filter(|probe| probe.channel_id == build.channel_id)
                        .map(move |_| (*target_fragment_id, *target_node_id))
                })
                .collect::<Vec<_>>();
            targets.sort_unstable();
            targets.dedup();
            all_filters.insert(
                build.filter_id,
                PlannedRuntimeFilter {
                    filter_id: build.filter_id,
                    build_plan_node_id: *node_id,
                    probe_target_node_ids: targets.iter().map(|(_, node_id)| *node_id).collect(),
                    has_remote_targets: targets
                        .iter()
                        .any(|(target_fragment_id, _)| *target_fragment_id != *fragment_id),
                    execution_mode: build.execution_mode,
                    expr_order,
                },
            );
            build_side_filters
                .entry(*fragment_id)
                .or_default()
                .push(build.filter_id);
            for (target_fragment_id, target_node_id) in targets {
                probe_side_filters
                    .entry(target_fragment_id)
                    .or_default()
                    .push((build.filter_id, target_node_id));
            }
        }
    }

    if all_filters.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RuntimeFilterPlanResult {
            all_filters,
            build_side_filters,
            probe_side_filters,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::runtime_filter::project_runtime_filters;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    fn column() -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: PlannerConfidence::Exact,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    #[test]
    fn empty_graph_produces_no_runtime_filter_plan() {
        let probe = DistributedNode {
            node_id: 11,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![column()],
            }),
        };
        let root = DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![probe],
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![column()],
            }),
        };
        let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root,
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: vec![column()],
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        assert!(plan.runtime_filter_graph().is_empty());
        let projection =
            project_runtime_filters(&plan).expect("project empty runtime-filter graph");
        assert!(
            plan_runtime_filters(&projection)
                .expect("plan empty runtime-filter projection")
                .is_none()
        );
    }
}
