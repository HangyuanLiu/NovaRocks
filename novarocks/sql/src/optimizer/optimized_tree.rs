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

//! Optimized physical operator tree extracted from the Memo.

use std::sync::Arc;

use crate::common::OutputColumn;
use crate::optimizer::cost::BroadcastDecision;
use crate::optimizer::operator::Operator;
use crate::optimizer::property::PhysicalPropertySet;
use crate::optimizer::scalar::ScalarArena;
use crate::optimizer::statistics::{CostEstimate, Statistics};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinExecutionDistribution {
    Broadcast,
    Partitioned,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    Colocate,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanExecutionProps {
    pub output_property: PhysicalPropertySet,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub child_output_properties: Vec<PhysicalPropertySet>,
    pub join_distribution: Option<JoinExecutionDistribution>,
    /// Shared scalar arena that owns all `ScalarId` handles referenced by this
    /// optimizer physical tree. Attached after extraction so codegen can materialize the
    /// scalar handles at its TypedExpr boundary.
    pub scalar_arena: Option<Arc<ScalarArena>>,
}

impl Default for PlanExecutionProps {
    fn default() -> Self {
        Self {
            output_property: PhysicalPropertySet::any(),
            child_output_properties: Vec::new(),
            join_distribution: None,
            scalar_arena: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OptimizerExplainStats {
    pub cost_estimate: Option<CostEstimate>,
    pub broadcast_decision: Option<BroadcastDecision>,
}

/// A node in the optimized physical operator tree produced by `extract_best`.
#[derive(Clone, Debug)]
pub(crate) struct OptimizedOperatorNode {
    pub op: Operator,
    pub children: Vec<OptimizedOperatorNode>,
    pub stats: Statistics,
    pub explain_stats: OptimizerExplainStats,
    pub output_columns: Vec<OutputColumn>,
    pub execution_props: PlanExecutionProps,
}

pub(crate) fn attach_scalar_arena(root: &mut OptimizedOperatorNode, arena: Arc<ScalarArena>) {
    root.execution_props.scalar_arena = Some(Arc::clone(&arena));
    for child in &mut root.children {
        attach_scalar_arena(child, Arc::clone(&arena));
    }
}

#[cfg(test)]
mod execution_prop_tests {
    use super::*;

    #[test]
    fn physical_node_carries_execution_properties() {
        let node = OptimizedOperatorNode {
            op: make_test_op(),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::optimizer::optimized_tree::OptimizerExplainStats::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: crate::optimizer::property::PhysicalPropertySet::broadcast(),
                child_output_properties: vec![
                    crate::optimizer::property::PhysicalPropertySet::any(),
                ],
                join_distribution: Some(JoinExecutionDistribution::Broadcast),
                scalar_arena: None,
            },
        };

        assert_eq!(
            node.execution_props.join_distribution,
            Some(JoinExecutionDistribution::Broadcast)
        );
        assert_eq!(
            node.execution_props.output_property.distribution,
            crate::optimizer::property::DistributionSpec::Broadcast
        );
    }

    fn make_test_op() -> Operator {
        use crate::optimizer::operator::ValuesOp;
        Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        })
    }
}
