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
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::{JoinKind, OutputColumn};
use crate::sql::optimizer::operator::{
    JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ValuesOp,
};
use crate::sql::optimizer::optimized_tree::{
    JoinExecutionDistribution, OptimizedOperatorNode, PlanExecutionProps, attach_scalar_arena,
};
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::statistics::Statistics;
use crate::sql::planner::distributed::DistributedNode;
use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
use crate::sql::planner::payload::PlanValuesNode;
use crate::sql::planner::physical::{PhysicalPlanKind, PhysicalPlanStats, PlannerConfidence};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::Arc;

fn int_col(column_id: ColumnId, name: &str) -> OutputColumn {
    OutputColumn {
        column_id,
        name: name.to_string(),
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    }
}

fn column_ref(column_id: ColumnId, name: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id,
            qualifier: None,
            column: name.to_string(),
        },
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn values_node(columns: Vec<OutputColumn>) -> OptimizedOperatorNode {
    OptimizedOperatorNode {
        op: Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: columns.clone(),
        }),
        children: vec![],
        output_columns: columns,
        stats: Statistics::default(),
        explain_stats: Default::default(),
        execution_props: PlanExecutionProps::default(),
    }
}

fn has_build_rf(node: &DistributedNode) -> bool {
    !node.build_runtime_filters.is_empty() || node.children.iter().any(has_build_rf)
}

fn has_probe_rf(node: &DistributedNode) -> bool {
    !node.probe_runtime_filters.is_empty() || node.children.iter().any(has_probe_rf)
}

fn broadcast_hash_join_without_optimizer_rf_annotations() -> OptimizedOperatorNode {
    let probe_id = ColumnId::new_for_test(1);
    let build_id = ColumnId::new_for_test(2);
    let probe_col = int_col(probe_id, "probe_key");
    let build_col = int_col(build_id, "build_key");
    let mut scalars = ScalarArena::new();
    let left = intern_typed(&mut scalars, &column_ref(probe_id, "probe_key"));
    let right = intern_typed(&mut scalars, &column_ref(build_id, "build_key"));
    let mut plan = OptimizedOperatorNode {
        op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left,
                right,
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        }),
        children: vec![
            values_node(vec![probe_col.clone()]),
            values_node(vec![build_col.clone()]),
        ],
        output_columns: vec![probe_col, build_col],
        stats: Statistics::default(),
        explain_stats: Default::default(),
        execution_props: PlanExecutionProps {
            join_distribution: Some(JoinExecutionDistribution::Broadcast),
            ..PlanExecutionProps::default()
        },
    };
    attach_scalar_arena(&mut plan, Arc::new(scalars));
    plan
}

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

#[test]
fn pipeline_places_runtime_filters_before_distributed_build() {
    let optimizer = broadcast_hash_join_without_optimizer_rf_annotations();
    let physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimizer)
        .expect("convert optimizer physical plan");
    let distributed = build_distributed_plan(physical).expect("build DistributedPlan");
    let root = &distributed.fragments[distributed.root_fragment_id as usize].root;

    assert!(
        has_build_rf(root),
        "distributed plan should contain build RF"
    );
    assert!(
        has_probe_rf(root),
        "distributed plan should contain probe RF"
    );
}
