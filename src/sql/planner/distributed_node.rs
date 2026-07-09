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

use crate::sql::analysis::OutputColumn;
use crate::sql::codegen::FragmentId;
use crate::sql::planner::plan::{
    DistributedChangeEventExpandNode, ExchangeFlavor, PhysicalHashAggregateNode,
    PhysicalHashJoinNode, PhysicalNestLoopJoinNode, PhysicalPlanKind, PhysicalSetOpNode,
    PhysicalTopNNode, PlanAssertOneRowNode, PlanFilterNode, PlanGenerateSeriesNode,
    PlanProjectNode, PlanRepeatNode, PlanScanNode, PlanSetOpKind, PlanSortNode,
    PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
};
use crate::sql::planner::runtime_filter::{WiredRuntimeFilterBuild, WiredRuntimeFilterProbe};
use crate::sql::planner::{DataPartition, PhysicalPlanStats};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ExchangeReceiver {
    pub partition: DataPartition,
    pub source_fragment_id: FragmentId,
    pub output_columns: Vec<OutputColumn>,
    pub output_qualifier: Option<String>,
    pub flavor: ExchangeFlavor,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum DistributedPayload {
    Physical(PhysicalPlanKind),
    Exchange(ExchangeReceiver),
}

/// Distributed-stage legal node kinds.
///
/// This shares leaf payload structs with `PhysicalPlanKind`, but excludes
/// `Limit`, `CTEAnchor`, `CTEProduce`, `CTEConsume`, and `Redistribute` because
/// those payloads are consumed or expanded while cutting fragments. `Exchange`
/// is overlay-only. `SetOp { UnionDistinct }` is still rejected during M1
/// conversion and must be rewritten before fragmentation.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum DistributedNodeKind {
    Scan(PlanScanNode),
    Filter(PlanFilterNode),
    Project(PlanProjectNode),
    Sort(PlanSortNode),
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
    Exchange(ExchangeReceiver),
}

impl DistributedNodeKind {
    #[cfg(test)]
    pub(crate) fn shared_variant_names_for_test() -> &'static [&'static str] {
        &[
            "Scan",
            "Filter",
            "Project",
            "Sort",
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
        ]
    }
}

fn non_distributable_payload(name: &str) -> String {
    format!(
        "{name} is not a distributable payload; it is consumed or expanded during fragment cutting"
    )
}

pub(crate) fn distributed_kind_from_physical(
    kind: PhysicalPlanKind,
) -> Result<DistributedNodeKind, String> {
    match kind {
        PhysicalPlanKind::Scan(node) => Ok(DistributedNodeKind::Scan(node)),
        PhysicalPlanKind::Filter(node) => Ok(DistributedNodeKind::Filter(node)),
        PhysicalPlanKind::Project(node) => Ok(DistributedNodeKind::Project(node)),
        PhysicalPlanKind::Sort(node) => Ok(DistributedNodeKind::Sort(node)),
        PhysicalPlanKind::Limit(_) => Err(non_distributable_payload("Limit")),
        PhysicalPlanKind::Values(node) => Ok(DistributedNodeKind::Values(node)),
        PhysicalPlanKind::Repeat(node) => Ok(DistributedNodeKind::Repeat(node)),
        PhysicalPlanKind::Window(node) => Ok(DistributedNodeKind::Window(node)),
        PhysicalPlanKind::GenerateSeries(node) => Ok(DistributedNodeKind::GenerateSeries(node)),
        PhysicalPlanKind::TableFunction(node) => Ok(DistributedNodeKind::TableFunction(node)),
        PhysicalPlanKind::AssertOneRow(node) => Ok(DistributedNodeKind::AssertOneRow(node)),
        PhysicalPlanKind::TopN(node) => Ok(DistributedNodeKind::TopN(node)),
        PhysicalPlanKind::HashAggregate(node) => Ok(DistributedNodeKind::HashAggregate(node)),
        PhysicalPlanKind::HashJoin(node) => Ok(DistributedNodeKind::HashJoin(node)),
        PhysicalPlanKind::NestLoopJoin(node) => Ok(DistributedNodeKind::NestLoopJoin(node)),
        PhysicalPlanKind::SetOp(node) if matches!(node.kind, PlanSetOpKind::UnionDistinct) => Err(
            "SetOp { UnionDistinct } must be rewritten to gather+distinct before fragmentation"
                .to_string(),
        ),
        PhysicalPlanKind::SetOp(node) => Ok(DistributedNodeKind::SetOp(node)),
        PhysicalPlanKind::ChangeEventExpand(node) => {
            Ok(DistributedNodeKind::ChangeEventExpand(node))
        }
        PhysicalPlanKind::CTEAnchor(_) => Err(non_distributable_payload("CTEAnchor")),
        PhysicalPlanKind::CTEProduce(_) => Err(non_distributable_payload("CTEProduce")),
        PhysicalPlanKind::CTEConsume(_) => Err(non_distributable_payload("CTEConsume")),
        PhysicalPlanKind::Redistribute(_) => Err(non_distributable_payload("Redistribute")),
    }
}

pub(crate) fn distributed_kind_to_physical(kind: &DistributedNodeKind) -> PhysicalPlanKind {
    match kind {
        DistributedNodeKind::Scan(node) => PhysicalPlanKind::Scan(node.clone()),
        DistributedNodeKind::Filter(node) => PhysicalPlanKind::Filter(node.clone()),
        DistributedNodeKind::Project(node) => PhysicalPlanKind::Project(node.clone()),
        DistributedNodeKind::Sort(node) => PhysicalPlanKind::Sort(node.clone()),
        DistributedNodeKind::Values(node) => PhysicalPlanKind::Values(node.clone()),
        DistributedNodeKind::Repeat(node) => PhysicalPlanKind::Repeat(node.clone()),
        DistributedNodeKind::Window(node) => PhysicalPlanKind::Window(node.clone()),
        DistributedNodeKind::GenerateSeries(node) => PhysicalPlanKind::GenerateSeries(node.clone()),
        DistributedNodeKind::TableFunction(node) => PhysicalPlanKind::TableFunction(node.clone()),
        DistributedNodeKind::AssertOneRow(node) => PhysicalPlanKind::AssertOneRow(node.clone()),
        DistributedNodeKind::TopN(node) => PhysicalPlanKind::TopN(node.clone()),
        DistributedNodeKind::HashAggregate(node) => PhysicalPlanKind::HashAggregate(node.clone()),
        DistributedNodeKind::HashJoin(node) => PhysicalPlanKind::HashJoin(node.clone()),
        DistributedNodeKind::NestLoopJoin(node) => PhysicalPlanKind::NestLoopJoin(node.clone()),
        DistributedNodeKind::SetOp(node) => PhysicalPlanKind::SetOp(node.clone()),
        DistributedNodeKind::ChangeEventExpand(node) => {
            PhysicalPlanKind::ChangeEventExpand(node.clone())
        }
        DistributedNodeKind::Exchange(_) => unreachable!(
            "DistributedNodeKind::Exchange is overlay-only; callers must handle Exchange separately"
        ),
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedNode {
    pub node_id: i32,
    pub fragment_id: FragmentId,
    pub tuple_ids: Vec<i32>,
    pub nullable_tuple_ids: Vec<i32>,
    pub limit: i64,
    pub build_runtime_filters: Vec<WiredRuntimeFilterBuild>,
    pub probe_runtime_filters: Vec<WiredRuntimeFilterProbe>,
    pub children: Vec<DistributedNode>,
    pub stats: PhysicalPlanStats,
    pub payload: DistributedPayload,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::sql::planner::plan::{PhysicalPlanKind, PlanValuesNode};
    use crate::sql::planner::{PhysicalPlanStats, PlannerConfidence};

    #[test]
    fn distributed_node_can_wrap_physical_values_payload() {
        let node = DistributedNode {
            node_id: 1,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: vec![],
            limit: -1,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: vec![],
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedPayload::Physical(PhysicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            })),
        };

        assert!(matches!(node.payload, DistributedPayload::Physical(_)));
    }

    #[test]
    fn distributed_node_kind_covers_distributable_physical_subset() {
        use std::collections::BTreeSet;
        let physical: BTreeSet<&str> =
            crate::sql::planner::plan::PhysicalPlanKind::variant_names_for_test()
                .iter()
                .copied()
                .collect();
        let non_distributable: BTreeSet<&str> = [
            "Limit",
            "CTEAnchor",
            "CTEProduce",
            "CTEConsume",
            "Redistribute",
        ]
        .into_iter()
        .collect();
        let expected_shared: BTreeSet<&str> =
            physical.difference(&non_distributable).copied().collect();
        let distributed_shared: BTreeSet<&str> =
            DistributedNodeKind::shared_variant_names_for_test()
                .iter()
                .copied()
                .collect();
        assert_eq!(distributed_shared, expected_shared);
    }
}
