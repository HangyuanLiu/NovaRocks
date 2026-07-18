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

use std::collections::BTreeSet;

use crate::sql::planner::distributed::FragmentEdgeKind;

pub(super) fn sealed_cte_projection(
    edges: &[crate::sql::planner::distributed::FragmentEdge],
    fragment: &crate::sql::planner::distributed::PlanFragment,
) -> Result<
    (
        Option<crate::sql::analysis::cte::CteId>,
        Vec<(
            crate::sql::analysis::cte::CteId,
            i32,
            Vec<crate::sql::column_id::ColumnId>,
        )>,
    ),
    String,
> {
    let producer_ids = edges
        .iter()
        .filter_map(|edge| {
            (edge.source_fragment_id == fragment.fragment_id)
                .then_some(&edge.edge_kind)
                .and_then(|kind| match kind {
                    FragmentEdgeKind::CteMulticast { cte_id, .. } => Some(*cte_id),
                    _ => None,
                })
        })
        .collect::<BTreeSet<_>>();
    let cte_id = match producer_ids.len() {
        0 => None,
        1 => producer_ids.iter().next().copied(),
        _ => {
            return Err(format!(
                "prepared fragment {} has multiple sealed CTE producer ids {producer_ids:?}",
                fragment.fragment_id
            ));
        }
    };
    if fragment.cte_id != cte_id {
        return Err(format!(
            "prepared fragment {} CTE producer mismatch: declared={:?} sealed={cte_id:?}",
            fragment.fragment_id, fragment.cte_id
        ));
    }
    let mut consumers = edges
        .iter()
        .filter_map(|edge| match &edge.edge_kind {
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } if edge.target_fragment_id == fragment.fragment_id => Some((
                *cte_id,
                edge.target_exchange_node_id,
                receive_producer_column_ids.clone(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    consumers.sort();
    let mut declared = fragment.cte_exchange_nodes.clone();
    declared.sort();
    if declared != consumers {
        return Err(format!(
            "prepared fragment {} CTE consumers mismatch: declared={declared:?} sealed={consumers:?}",
            fragment.fragment_id
        ));
    }
    Ok((cte_id, consumers))
}

#[cfg(test)]
mod tests {
    use super::sealed_cte_projection;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
    };

    #[test]
    fn sealed_cte_multicast_projection_sorts_edges_and_preserves_receive_occurrence_order() {
        let cte_id = 42;
        let first_column = ColumnId::new_for_test(1);
        let second_column = ColumnId::new_for_test(2);
        let third_column = ColumnId::new_for_test(3);
        let fourth_column = ColumnId::new_for_test(4);
        let mut fragment = super::super::test_support::result_plan().fragments()[0].clone();
        fragment.cte_exchange_nodes = vec![
            (cte_id, 11, vec![fourth_column, second_column]),
            (cte_id, 3, vec![third_column, first_column]),
        ];
        let edges = vec![
            FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: fragment.fragment_id,
                target_exchange_node_id: 11,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids: vec![fourth_column, second_column],
                },
                output_slot_ids: vec![2],
            },
            FragmentEdge {
                source_fragment_id: 2,
                target_fragment_id: fragment.fragment_id,
                target_exchange_node_id: 3,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids: vec![third_column, first_column],
                },
                output_slot_ids: vec![1],
            },
        ];

        let (producer, consumers) =
            sealed_cte_projection(&edges, &fragment).expect("sealed CTE projection");

        assert_eq!(producer, None);
        assert_eq!(
            consumers,
            vec![
                (cte_id, 3, vec![third_column, first_column]),
                (cte_id, 11, vec![fourth_column, second_column]),
            ]
        );
    }
}
