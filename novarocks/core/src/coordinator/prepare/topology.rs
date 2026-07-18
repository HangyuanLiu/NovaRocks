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

#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeSet;

use crate::sql::planner::distributed::{DistributedNode, DistributedNodeKind, FragmentId};
use crate::sql::planner::table::ScanSource;

pub(super) fn validate_topology_roles(
    sealed_ids: &BTreeSet<FragmentId>,
    result_fragment_id: Option<FragmentId>,
    terminal_write_fragment_ids: &BTreeSet<FragmentId>,
    producer_fragment_ids: &BTreeSet<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) = UNCLASSIFIED_TOPOLOGY_ROLE_OVERRIDE.with(|slot| {
        slot.get().map(|execution_anchor_fragment_id| {
            validate_topology_roles_impl(
                sealed_ids,
                None,
                &BTreeSet::new(),
                &BTreeSet::new(),
                execution_anchor_fragment_id,
            )
        })
    }) {
        return result;
    }

    validate_topology_roles_impl(
        sealed_ids,
        result_fragment_id,
        terminal_write_fragment_ids,
        producer_fragment_ids,
        execution_anchor_fragment_id,
    )
}

fn validate_topology_roles_impl(
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

#[cfg(test)]
thread_local! {
    static UNCLASSIFIED_TOPOLOGY_ROLE_OVERRIDE: Cell<Option<FragmentId>> = const { Cell::new(None) };
}

#[cfg(test)]
fn with_unclassified_topology_role_for_test<T>(
    execution_anchor_fragment_id: FragmentId,
    operation: impl FnOnce() -> T,
) -> T {
    struct ResetOverride;

    impl Drop for ResetOverride {
        fn drop(&mut self) {
            UNCLASSIFIED_TOPOLOGY_ROLE_OVERRIDE.with(|slot| slot.set(None));
        }
    }

    UNCLASSIFIED_TOPOLOGY_ROLE_OVERRIDE.with(|slot| {
        assert!(
            slot.replace(Some(execution_anchor_fragment_id)).is_none(),
            "topology role override must not be nested"
        );
    });
    let reset = ResetOverride;
    let result = operation();
    drop(reset);
    result
}

pub(super) fn validate_binding_keys<T>(
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

pub(super) fn collect_scan_nodes<'a>(
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
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;

    use super::with_unclassified_topology_role_for_test;
    use crate::catalog::schema::ColumnDef;
    use crate::coordinator::prepare::scan::{ResolvedScanExecution, ScanBindingResolver};
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan,
        PlanFragment,
    };
    use crate::sql::planner::payload::PlanScanNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use crate::sql::planner::table::{IcebergMvTargetLocatorScan, ScanSource, TableDef};

    struct CountingResolver {
        calls: AtomicUsize,
    }

    impl ScanBindingResolver for CountingResolver {
        fn resolve_scan(
            &self,
            _node_id: i32,
            _scan: &PlanScanNode,
        ) -> Result<Option<ResolvedScanExecution>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err("resolver must not run before topology validation".to_string())
        }
    }

    fn resolver_required_scan_plan() -> DistributedPlan {
        let output = OutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        };
        let root = DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: vec![10],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: PhysicalPlanStats {
                output_row_count: 1.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "test_table".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::IcebergMvTargetLocator(IcebergMvTargetLocatorScan {
                        catalog: "test_catalog".to_string(),
                        database: "test_db".to_string(),
                        table: "test_table".to_string(),
                        target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                        target_snapshot_id: Some(7),
                        apply_key_column: "id".to_string(),
                        branch_id_column: None,
                    }),
                },
                alias: None,
                columns: vec![output.clone()],
                predicates: Vec::new(),
                required_columns: Some(vec!["id".to_string()]),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root,
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: vec![output],
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
            runtime_filter_graph: RuntimeFilterGraph::default(),
        }
    }

    #[test]
    fn topology_role_validation_precedes_scan_resolver() {
        let plan = resolver_required_scan_plan();
        let resolver = CountingResolver {
            calls: AtomicUsize::new(0),
        };

        let result = with_unclassified_topology_role_for_test(99, || {
            super::super::prepare_fragments(
                &plan,
                &crate::connector::ConnectorRegistry::new(),
                Some(&resolver),
            )
        });
        let error = match result {
            Ok(_) => panic!("invalid topology role must fail before scan preparation"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            "prepared fragments have no sealed topology role: {0}"
        );
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    }
}
