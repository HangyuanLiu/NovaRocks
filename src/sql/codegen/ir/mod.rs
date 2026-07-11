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

//! Planner-owned DistributedPlan IR and native fragment builder.

pub(crate) mod explain;
pub(crate) mod fragment_build;
pub(crate) mod kind;

pub(crate) use explain::{explain_distributed_plan, explain_distributed_plan_analyze};
pub(crate) use fragment_build::lower_distributed_plan;

fn validate_global_node_ids(
    plan: &crate::sql::planner::distributed::DistributedPlan,
) -> Result<(), String> {
    fn visit(
        node: &crate::sql::planner::distributed::DistributedNode,
        fragment_id: crate::sql::planner::distributed::FragmentId,
        owners: &mut std::collections::HashMap<i32, crate::sql::planner::distributed::FragmentId>,
    ) -> Result<(), String> {
        if let Some(previous_fragment_id) = owners.insert(node.node_id, fragment_id) {
            return Err(format!(
                "DistributedPlan contains duplicate node_id={} in fragments {} and {}",
                node.node_id, previous_fragment_id, fragment_id
            ));
        }
        for child in &node.children {
            visit(child, fragment_id, owners)?;
        }
        Ok(())
    }

    let mut owners = std::collections::HashMap::new();
    for fragment in &plan.fragments {
        visit(&fragment.root, fragment.fragment_id, &mut owners)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    fn values_node(
        fragment_id: u32,
        node_id: i32,
    ) -> crate::sql::planner::distributed::DistributedNode {
        crate::sql::planner::distributed::DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: crate::sql::planner::physical::PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: crate::sql::planner::physical::PlannerConfidence::Fallback,
                column_statistics: std::collections::HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: crate::sql::planner::distributed::DistributedNodeKind::Values(
                crate::sql::planner::payload::PlanValuesNode {
                    rows: Vec::new(),
                    columns: Vec::new(),
                },
            ),
        }
    }

    #[test]
    fn bridge2_owner_modules_are_split_into_files() {
        for module_file in [
            "distributed/fragment.rs",
            "distributed/node.rs",
            "distributed/build/fragment_cut.rs",
            "distributed/build/lowering.rs",
            "distributed/build/mod.rs",
            "distributed/build/runtime_filter_wire.rs",
        ] {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/sql/planner")
                .join(module_file);
            assert!(path.is_file(), "{} should exist", path.display());
        }
    }

    #[test]
    fn distributed_plan_rejects_duplicate_node_id_across_fragments() {
        let fragments = [0, 1]
            .into_iter()
            .map(
                |fragment_id| crate::sql::planner::distributed::PlanFragment {
                    fragment_id,
                    root: values_node(fragment_id, 7),
                    data_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(
                    ),
                    output_partition:
                        crate::sql::planner::distributed::DataPartition::unpartitioned(),
                    sink: crate::sql::planner::distributed::DataSink::Noop,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            )
            .collect();
        let plan = crate::sql::planner::distributed::DistributedPlan {
            fragments,
            root_fragment_id: 0,
            edges: Vec::new(),
        };

        let err = super::validate_global_node_ids(&plan)
            .expect_err("node ids are global descriptor keys and must be unique");
        assert!(err.contains("duplicate node_id=7"), "{err}");
        assert!(err.contains("fragments 0 and 1"), "{err}");
    }
}
