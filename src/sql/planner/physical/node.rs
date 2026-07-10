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

//! Planner-owned physical plan nodes.

use crate::sql::analysis::{JoinKind, OutputColumn, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::planner::payload::{
    AggregateCall, PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
    PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
};
use crate::sql::planner::physical::{
    AggMode, AggregateOutputLayout, HashSource, JoinDistribution, JoinExecutionMode,
    PhysicalPlanStats, TopNPhase,
};
use crate::sql::planner::runtime_filter::{RuntimeFilterBuildIntent, RuntimeFilterProbeIntent};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalTopNNode {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashAggregateNode {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub is_merge: Vec<bool>,
    pub output_layout: AggregateOutputLayout,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinNode {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<PhysicalHashJoinEqCondition>,
    pub other_condition: Option<TypedExpr>,
    pub distribution: JoinDistribution,
    pub execution_mode: Option<JoinExecutionMode>,
    pub build_runtime_filters: Vec<RuntimeFilterBuildIntent>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinEqCondition {
    pub left: TypedExpr,
    pub right: TypedExpr,
    pub null_safe: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalNestLoopJoinNode {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanSetOpKind {
    UnionAll,
    UnionDistinct,
    Intersect,
    Except,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalSetOpNode {
    pub kind: PlanSetOpKind,
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedChangeEventExpandNode {
    pub(crate) events: Vec<DistributedChangeEventSpec>,
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) change_op_column_id: ColumnId,
    pub(crate) data_route_column_id: Option<ColumnId>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedChangeEventSpec {
    pub(crate) predicate: Option<TypedExpr>,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) assignments: Vec<DistributedChangeEventOutputExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedChangeEventOutputExpr {
    pub(crate) output_column_id: ColumnId,
    pub(crate) expr: Option<TypedExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalPlanNode {
    pub kind: PhysicalPlanKind,
    pub children: Vec<PhysicalPlanNode>,
    pub output_columns: Vec<OutputColumn>,
    pub stats: PhysicalPlanStats,
    pub probe_runtime_filters: Vec<RuntimeFilterProbeIntent>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum PhysicalPlanKind {
    Scan(PlanScanNode),
    Filter(PlanFilterNode),
    Project(PlanProjectNode),
    Sort(PlanSortNode),
    Limit(PlanLimitNode),
    Values(PlanValuesNode),
    Repeat(PlanRepeatNode),
    Window(PlanWindowNode),
    GenerateSeries(PlanGenerateSeriesNode),
    TableFunction(PlanTableFunctionNode),
    AssertOneRow(PlanAssertOneRowNode),
    TopN(PhysicalTopNNode),
    HashAggregate(Box<PhysicalHashAggregateNode>),
    HashJoin(Box<PhysicalHashJoinNode>),
    NestLoopJoin(PhysicalNestLoopJoinNode),
    SetOp(PhysicalSetOpNode),
    ChangeEventExpand(DistributedChangeEventExpandNode),
    CTEAnchor(PlanCTEAnchorNode),
    CTEProduce(PlanCTEProduceNode),
    CTEConsume(PlanCTEConsumeNode),
    Redistribute(RedistributeNode),
}

impl PhysicalPlanKind {
    #[cfg(test)]
    pub(crate) fn variant_names_for_test() -> &'static [&'static str] {
        &[
            "Scan",
            "Filter",
            "Project",
            "Sort",
            "Limit",
            "Values",
            "Repeat",
            "Window",
            "GenerateSeries",
            "TableFunction",
            "AssertOneRow",
            "TopN",
            "HashAggregate",
            "HashJoin",
            "NestLoopJoin",
            "SetOp",
            "ChangeEventExpand",
            "CTEAnchor",
            "CTEProduce",
            "CTEConsume",
            "Redistribute",
        ]
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct RedistributeNode {
    pub mode: RedistributeMode,
    pub partition_exprs: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RedistributeMode {
    Gather,
    Hash {
        cols: Vec<ColumnId>,
        source: HashSource,
    },
    Broadcast,
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    #[test]
    fn physical_plan_kind_set_op_uses_plan_scoped_kind() {
        let set_op = PhysicalPlanKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: vec![],
            child_output_columns: vec![vec![], vec![]],
        });

        let PhysicalPlanKind::SetOp(node) = set_op else {
            panic!("expected SetOp");
        };
        assert_eq!(node.kind, PlanSetOpKind::UnionAll);
        assert_eq!(node.child_output_columns.len(), 2);
    }

    #[test]
    fn physical_plan_kind_has_redistribute_but_no_exchange() {
        fn accepts_physical(_: PhysicalPlanKind) {}

        accepts_physical(PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: vec![],
            output_columns: vec![],
        }));

        assert!(
            !PhysicalPlanKind::variant_names_for_test().contains(&"Exchange"),
            "Exchange belongs to DistributedPlan, not PhysicalPlanKind"
        );
    }

    #[test]
    fn redistribute_mode_variants_are_frozen() {
        fn _exhaustive(mode: &RedistributeMode) {
            match mode {
                RedistributeMode::Gather => {}
                RedistributeMode::Hash { .. } => {}
                RedistributeMode::Broadcast => {}
            }
        }
    }

    #[test]
    fn physical_plan_kind_has_no_exchange_variant() {
        assert!(
            !PhysicalPlanKind::variant_names_for_test().contains(&"Exchange"),
            "Exchange must not be a PhysicalPlanKind variant"
        );
    }
}
