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

mod projection;
pub(crate) mod scan;
mod scan_preparation;

use std::collections::{BTreeMap, BTreeSet};

use crate::connector::ConnectorRegistry;
use crate::sql::catalog::ScanSource;
use crate::sql::planner::distributed::{
    BoundaryContract, BoundaryKind, DistributedNode, DistributedNodeKind, FragmentEdgeKind,
    FragmentId,
};

#[cfg(test)]
pub(crate) use projection::prepared_fragment_set_for_test;
pub(crate) use projection::{
    FragmentSchedulingView, PreparedFragment, PreparedFragmentRole, PreparedFragmentSet,
    PreparedOutputColumn,
};
#[cfg(test)]
pub(crate) use scan_preparation::build_iceberg_metadata_scan_range_params;
use scan_preparation::prepare_scan_bindings;

pub(crate) fn prepare_fragments(
    plan: &crate::sql::planner::distributed::DistributedPlan,
    connectors: &ConnectorRegistry,
    resolver: Option<&dyn scan::ScanBindingResolver>,
) -> Result<PreparedFragmentSet, String> {
    let sealed_ids = plan
        .fragments()
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<BTreeSet<_>>();
    let topological_fragment_order = plan.topology().topological_fragment_order().to_vec();
    let ordered_ids = topological_fragment_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered_ids.len() != topological_fragment_order.len() || ordered_ids != sealed_ids {
        return Err(format!(
            "prepared fragment topology order {topological_fragment_order:?} does not match sealed fragment ids {sealed_ids:?}"
        ));
    }
    let execution_anchor_fragment_id = plan.topology().execution_anchor_fragment_id();
    if !sealed_ids.contains(&execution_anchor_fragment_id) {
        return Err(format!(
            "prepared execution anchor fragment {execution_anchor_fragment_id} is not among sealed fragment ids {sealed_ids:?}"
        ));
    }
    let result_fragment_id = plan.topology().result_fragment_id();
    let terminal_write_fragment_ids = plan
        .topology()
        .terminal_write_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let producer_fragment_ids = plan
        .topology()
        .producer_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    validate_topology_roles(
        &sealed_ids,
        result_fragment_id,
        &terminal_write_fragment_ids,
        &producer_fragment_ids,
        execution_anchor_fragment_id,
    )?;
    let write_contract_fragment_ids = sealed_ids
        .iter()
        .copied()
        .filter(|&fragment_id| {
            plan.write_contracts()
                .iceberg_write_output(fragment_id)
                .is_some()
        })
        .collect::<BTreeSet<_>>();
    let boundary_contracts = validate_and_group_boundary_contracts(
        result_fragment_id,
        &write_contract_fragment_ids,
        plan.edges(),
        plan.boundaries().contracts(),
        &sealed_ids,
    )?;
    let scan_bindings = prepare_scan_bindings(plan, connectors, resolver)?;

    let mut by_fragment = BTreeMap::new();
    let mut expected_range_keys = BTreeSet::new();
    let mut expected_binding_node_ids = BTreeSet::new();
    let mut expected_starrocks_node_ids = BTreeSet::new();
    for fragment in plan.fragments() {
        let mut scan_nodes = Vec::new();
        collect_scan_nodes(fragment.fragment_id, &fragment.root, &mut scan_nodes);
        scan_nodes.sort_by_key(|(node_id, _)| *node_id);
        for (node_id, source) in &scan_nodes {
            expected_range_keys.insert((fragment.fragment_id, *node_id));
            if scan_bindings
                .scan_ranges(fragment.fragment_id, *node_id)
                .is_none()
            {
                return Err(format!(
                    "prepared fragment missing scan ranges fragment_id={} node_id={node_id}",
                    fragment.fragment_id
                ));
            }
            match source {
                ScanSource::IcebergMetadataTable { .. } => {}
                ScanSource::StarRocks { .. } => {
                    expected_starrocks_node_ids.insert(*node_id);
                    if scan_bindings.starrocks_source(*node_id).is_none() {
                        return Err(format!(
                            "prepared fragment missing StarRocks source fragment_id={} node_id={node_id}",
                            fragment.fragment_id
                        ));
                    }
                }
                _ => {
                    expected_binding_node_ids.insert(*node_id);
                    if scan_bindings.binding(*node_id).is_none() {
                        return Err(format!(
                            "prepared fragment missing scan binding fragment_id={} node_id={node_id}",
                            fragment.fragment_id
                        ));
                    }
                }
            }
        }
        let scan_node_ids = scan_nodes.into_iter().map(|(node_id, _)| node_id).collect();
        let execution_role = if result_fragment_id == Some(fragment.fragment_id) {
            PreparedFragmentRole::Result
        } else if terminal_write_fragment_ids.contains(&fragment.fragment_id) {
            PreparedFragmentRole::TerminalWrite
        } else {
            PreparedFragmentRole::NonTerminal
        };
        // Query-path output is finalized by FragmentEdgeOutputCatalog. Iceberg
        // target-schema output belongs to WriteContractCatalog and is not a
        // fetch/result projection. The two sealed catalogs are complementary:
        // exactly write fragments must be absent from fragment-edge outputs.
        let sealed_output_columns = plan
            .fragment_edge_outputs()
            .fragment_output_columns(fragment.fragment_id);
        let output_columns = match (
            write_contract_fragment_ids.contains(&fragment.fragment_id),
            sealed_output_columns,
        ) {
            (true, None) => Vec::new(),
            (true, Some(_)) => {
                return Err(format!(
                    "prepared sealed output mismatch fragment_id={}: Iceberg write fragment unexpectedly has FragmentEdgeOutputCatalog output",
                    fragment.fragment_id
                ));
            }
            (false, Some(columns)) => columns
                .iter()
                .map(|column| PreparedOutputColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
            (false, None) => {
                return Err(format!(
                    "prepared sealed output mismatch fragment_id={}: non-write fragment is missing FragmentEdgeOutputCatalog output",
                    fragment.fragment_id
                ));
            }
        };
        let (cte_id, cte_exchange_nodes) = sealed_cte_projection(plan.edges(), fragment)?;
        let contracts = boundary_contracts
            .get(&fragment.fragment_id)
            .cloned()
            .unwrap_or_default();
        let prepared = projection::prepared_fragment(
            fragment.fragment_id,
            scan_node_ids,
            execution_role,
            output_columns,
            cte_id,
            cte_exchange_nodes,
            contracts,
        );
        if by_fragment.insert(fragment.fragment_id, prepared).is_some() {
            return Err(format!(
                "duplicate prepared fragment id={}",
                fragment.fragment_id
            ));
        }
    }

    validate_binding_keys(
        "scan ranges",
        &expected_range_keys,
        &scan_bindings.scan_range_keys().collect(),
    )?;
    validate_binding_keys(
        "scan bindings",
        &expected_binding_node_ids,
        &scan_bindings.binding_node_ids().collect(),
    )?;
    validate_binding_keys(
        "StarRocks descriptors",
        &expected_starrocks_node_ids,
        &scan_bindings.starrocks_source_node_ids().collect(),
    )?;

    Ok(PreparedFragmentSet::new(
        by_fragment,
        scan_bindings,
        topological_fragment_order,
        execution_anchor_fragment_id,
        plan.edges().to_vec(),
    ))
}

fn validate_topology_roles(
    sealed_ids: &BTreeSet<FragmentId>,
    result_fragment_id: Option<FragmentId>,
    terminal_write_fragment_ids: &BTreeSet<FragmentId>,
    producer_fragment_ids: &BTreeSet<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
) -> Result<(), String> {
    for (label, ids) in [
        ("terminal write", terminal_write_fragment_ids),
        ("producer", producer_fragment_ids),
    ] {
        if !ids.is_subset(sealed_ids) {
            return Err(format!(
                "prepared {label} fragment ids {ids:?} are not a subset of sealed fragment ids {sealed_ids:?}"
            ));
        }
    }
    if let Some(result_fragment_id) = result_fragment_id {
        if !sealed_ids.contains(&result_fragment_id) {
            return Err(format!(
                "prepared result fragment {result_fragment_id} is not among sealed fragment ids {sealed_ids:?}"
            ));
        }
        if terminal_write_fragment_ids.contains(&result_fragment_id)
            || producer_fragment_ids.contains(&result_fragment_id)
        {
            return Err(format!(
                "prepared result fragment {result_fragment_id} overlaps terminal-write or producer roles"
            ));
        }
    }
    if !terminal_write_fragment_ids.is_disjoint(producer_fragment_ids) {
        return Err(format!(
            "prepared terminal-write and producer roles overlap: terminal={terminal_write_fragment_ids:?} producer={producer_fragment_ids:?}"
        ));
    }
    let classified = producer_fragment_ids
        .iter()
        .chain(terminal_write_fragment_ids)
        .copied()
        .chain(result_fragment_id)
        .collect::<BTreeSet<_>>();
    let allowed_unclassified = BTreeSet::from([execution_anchor_fragment_id]);
    let unclassified = sealed_ids
        .difference(&classified)
        .copied()
        .collect::<BTreeSet<_>>();
    if !unclassified.is_subset(&allowed_unclassified) {
        return Err(format!(
            "prepared fragments have no sealed topology role: {unclassified:?}"
        ));
    }
    Ok(())
}

type BoundaryKey = (FragmentId, Option<i32>, BoundaryKind);

fn validate_and_group_boundary_contracts(
    result_fragment_id: Option<FragmentId>,
    write_contract_fragment_ids: &BTreeSet<FragmentId>,
    edges: &[crate::sql::planner::distributed::FragmentEdge],
    contracts: &[BoundaryContract],
    sealed_ids: &BTreeSet<FragmentId>,
) -> Result<BTreeMap<FragmentId, Vec<BoundaryContract>>, String> {
    let mut expected = BTreeSet::<BoundaryKey>::new();
    if let Some(fragment_id) = result_fragment_id {
        expected.insert((fragment_id, None, BoundaryKind::ResultOutput));
    }
    for &fragment_id in write_contract_fragment_ids {
        expected.insert((fragment_id, None, BoundaryKind::IcebergWriteInput));
    }
    for edge in edges {
        expected.insert((
            edge.source_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeSend,
        ));
        expected.insert((
            edge.target_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeReceive,
        ));
        if matches!(
            edge.edge_kind,
            FragmentEdgeKind::IcebergChangeStreamRouter { .. }
        ) {
            expected.insert((
                edge.source_fragment_id,
                None,
                BoundaryKind::ChangeStreamRouterInput,
            ));
        }
    }

    let mut actual = BTreeMap::<BoundaryKey, &BoundaryContract>::new();
    let mut occurrences = BTreeSet::new();
    for contract in contracts {
        let key = (contract.fragment_id, contract.node_id, contract.kind);
        if !sealed_ids.contains(&contract.fragment_id) {
            return Err(format!(
                "prepared boundary {key:?} references unknown fragment id"
            ));
        }
        if actual.insert(key, contract).is_some() {
            return Err(format!(
                "prepared boundary group occurs more than once: {key:?}"
            ));
        }
        for (ordinal, column) in contract.columns.iter().enumerate() {
            if column.output_ordinal != ordinal {
                return Err(format!(
                    "prepared boundary {key:?} column ordinal mismatch: expected={ordinal} actual={}",
                    column.output_ordinal
                ));
            }
            if !occurrences.insert(column.execution_column_id) {
                return Err(format!(
                    "prepared boundary occurrence id={} is duplicated",
                    column.execution_column_id.value()
                ));
            }
        }
    }
    let actual_keys = actual.keys().copied().collect::<BTreeSet<_>>();
    if actual_keys != expected {
        return Err(format!(
            "prepared boundary groups mismatch: expected={expected:?} actual={actual_keys:?} missing={:?} unknown={:?}",
            expected.difference(&actual_keys).collect::<Vec<_>>(),
            actual_keys.difference(&expected).collect::<Vec<_>>()
        ));
    }
    for edge in edges {
        let send = actual[&(
            edge.source_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeSend,
        )];
        let receive = actual[&(
            edge.target_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeReceive,
        )];
        if send.columns.len() != receive.columns.len()
            || send
                .columns
                .iter()
                .zip(&receive.columns)
                .any(|(send, receive)| {
                    send.column_id != receive.column_id
                        || send.output_ordinal != receive.output_ordinal
                        || send.name != receive.name
                        || send.data_type != receive.data_type
                        || send.nullable != receive.nullable
                        || send.is_internal != receive.is_internal
                })
        {
            return Err(format!(
                "prepared exchange boundary columns differ for target fragment={} node_id={}",
                edge.target_fragment_id, edge.target_exchange_node_id
            ));
        }
    }

    let mut by_fragment = BTreeMap::<FragmentId, Vec<BoundaryContract>>::new();
    for contract in contracts {
        by_fragment
            .entry(contract.fragment_id)
            .or_default()
            .push(contract.clone());
    }
    Ok(by_fragment)
}

fn sealed_cte_projection(
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

fn validate_binding_keys<T>(
    label: &str,
    expected: &BTreeSet<T>,
    actual: &BTreeSet<T>,
) -> Result<(), String>
where
    T: Copy + Ord + std::fmt::Debug,
{
    if actual == expected {
        return Ok(());
    }
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
    Err(format!(
        "prepared {label} mismatch: expected={expected:?} actual={actual:?} missing={missing:?} unknown={unknown:?}"
    ))
}

fn collect_scan_nodes<'a>(
    fragment_id: FragmentId,
    node: &'a DistributedNode,
    out: &mut Vec<(i32, &'a ScanSource)>,
) {
    if let DistributedNodeKind::Scan(scan) = &node.payload {
        out.push((node.node_id, &scan.table.source));
    }
    for child in &node.children {
        if child.fragment_id == fragment_id {
            collect_scan_nodes(fragment_id, child, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    fn result_plan() -> crate::sql::planner::distributed::DistributedPlan {
        let columns = vec![
            OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: "a".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "b".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                is_internal: false,
            },
        ];
        let fragment = PlanFragment {
            fragment_id: 7,
            root: DistributedNode {
                node_id: 70,
                fragment_id: 7,
                tuple_ids: vec![70],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: PhysicalPlanStats {
                    output_row_count: 0.0,
                    row_count_confidence: PlannerConfidence::Fallback,
                    column_statistics: Default::default(),
                    cost_estimate: None,
                    broadcast_decision: None,
                },
                payload: DistributedNodeKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: columns.clone(),
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![fragment],
            root_fragment_id: 7,
            edges: Vec::new(),
            runtime_filter_graph: crate::runtime_filter::model::graph::RuntimeFilterGraph::default(),
        }
    }

    fn write_plan() -> crate::sql::planner::distributed::DistributedPlan {
        let columns = vec![OutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }];
        let mut sink_spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        sink_spec.target_table.columns[0].data_type = DataType::Int64;
        sink_spec.target_columns[0].data_type = DataType::Int64;
        let fragment = PlanFragment {
            fragment_id: 9,
            root: DistributedNode {
                node_id: 90,
                fragment_id: 9,
                tuple_ids: vec![90],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: PhysicalPlanStats {
                    output_row_count: 0.0,
                    row_count_confidence: PlannerConfidence::Fallback,
                    column_statistics: Default::default(),
                    cost_estimate: None,
                    broadcast_decision: None,
                },
                payload: DistributedNodeKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: columns.clone(),
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::IcebergWrite(
                crate::sql::planner::distributed::write::sink::IcebergWriteFragmentSink {
                    descriptor_database: "default".to_string(),
                    spec: sink_spec,
                    input: crate::sql::planner::distributed::write::sink::IcebergWriteInputBinding::RootOutputByOrdinal,
                },
            ),
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![fragment],
            root_fragment_id: 9,
            edges: Vec::new(),
            runtime_filter_graph: crate::runtime_filter::model::graph::RuntimeFilterGraph::default(),
        }
    }

    #[test]
    fn production_preparation_accepts_write_without_query_output_contract() {
        let plan = write_plan();
        assert!(
            plan.fragment_edge_outputs()
                .fragment_output_columns(9)
                .is_none()
        );
        let prepared = prepare_fragments(&plan, &crate::connector::ConnectorRegistry::new(), None)
            .expect("sealed write output absence is legal");
        assert!(
            prepared
                .fragment(9)
                .expect("prepared writer")
                .boundary_projection()
                .output_columns()
                .is_empty()
        );
    }

    #[test]
    fn production_preparation_rejects_missing_non_write_output_contract() {
        let mut plan = result_plan();
        crate::sql::planner::distributed::test_support::remove_fragment_output_for_test(
            &mut plan, 7,
        );
        let error =
            match prepare_fragments(&plan, &crate::connector::ConnectorRegistry::new(), None) {
                Ok(_) => {
                    panic!("non-write output absence must fail through production preparation")
                }
                Err(error) => error,
            };
        assert_eq!(
            error,
            "prepared sealed output mismatch fragment_id=7: non-write fragment is missing FragmentEdgeOutputCatalog output"
        );
    }

    fn validate_contracts(
        plan: &crate::sql::planner::distributed::DistributedPlan,
        contracts: &[BoundaryContract],
    ) -> Result<BTreeMap<FragmentId, Vec<BoundaryContract>>, String> {
        validate_and_group_boundary_contracts(
            plan.topology().result_fragment_id(),
            &BTreeSet::new(),
            plan.edges(),
            contracts,
            &plan
                .fragments()
                .iter()
                .map(|fragment| fragment.fragment_id)
                .collect(),
        )
    }

    #[test]
    fn malformed_boundary_groups_and_occurrences_use_production_validation() {
        let plan = result_plan();
        let valid = plan.boundaries().contracts();
        validate_contracts(&plan, valid).expect("sealed boundary catalog");

        let missing = validate_contracts(&plan, &[]).expect_err("missing group must fail");
        assert!(missing.contains("boundary groups mismatch"), "{missing}");

        let duplicate = vec![valid[0].clone(), valid[0].clone()];
        let duplicate_error =
            validate_contracts(&plan, &duplicate).expect_err("duplicate group must fail");
        assert!(
            duplicate_error.contains("boundary group occurs more than once"),
            "{duplicate_error}"
        );

        let mut unknown = valid.to_vec();
        unknown[0].node_id = Some(999);
        let unknown_error =
            validate_contracts(&plan, &unknown).expect_err("unknown group must fail");
        assert!(unknown_error.contains("unknown="), "{unknown_error}");

        let mut duplicate_occurrence = valid.to_vec();
        duplicate_occurrence[0].columns[1].execution_column_id =
            duplicate_occurrence[0].columns[0].execution_column_id;
        let occurrence_error = validate_contracts(&plan, &duplicate_occurrence)
            .expect_err("duplicate occurrence must fail");
        assert!(
            occurrence_error.contains("occurrence id=") && occurrence_error.contains("duplicated"),
            "{occurrence_error}"
        );
    }
}
