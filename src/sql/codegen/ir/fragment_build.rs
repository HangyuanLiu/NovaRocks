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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::DataType;

use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext};
use crate::runtime::scan_range;
use crate::sql::analysis::OutputColumn as AnalysisOutputColumn;
use crate::sql::catalog::{IcebergDataFileBinding, ScanSource, TableDef};
use crate::sql::codegen::boundary_schema::{
    BoundaryKind, BoundarySchemaReport, output_columns_to_boundary_columns,
};
use crate::sql::codegen::runtime_filter::PlannedRuntimeFilter;
use crate::sql::codegen::scan::connector::{
    ConnectorScanContext, StarRocksScanSourceDescriptor, plan_native_starrocks_scan_node,
    to_native_file_scan,
};
use crate::sql::codegen::{
    FragmentOutputKind, FragmentSchedulingMetadata, MultiFragmentBuildResult, OutputColumn,
    RuntimeFilterPlanResult,
};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    DataPartition, DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor,
    FragmentEdgeKind, FragmentId, FragmentStreamKind, PartitionKind, PlanFragment,
};
use crate::sql::planner::payload::PlanScanNode;

pub(crate) fn lower_distributed_plan(
    dp: &DistributedPlan,
    catalog: &dyn crate::sql::catalog::CatalogProvider,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<MultiFragmentBuildResult, String> {
    let _ = catalog;
    validate_distributed_plan(dp)?;

    let mut refreshed = refresh_distributed_plan_for_fragment_build(dp, mv_refresh_ctx)?;
    lower_native_fragment_edges(&mut refreshed)?;
    let native_scan_planning =
        build_native_scan_ranges(&mut refreshed, connectors, mv_refresh_ctx)?;
    let native_scan_ranges = &native_scan_planning.scan_ranges;

    let mut fragment_schedules = Vec::with_capacity(refreshed.fragments.len());
    for fragment in &refreshed.fragments {
        let output_columns = fragment
            .output_columns
            .iter()
            .map(output_column_for_boundary)
            .collect::<Vec<_>>();
        let boundary_schemas = vec![result_root_boundary_schema_report(
            fragment.fragment_id,
            fragment.root.node_id,
            &output_columns,
        )];
        let has_scan_nodes = distributed_node_has_scan(&fragment.root);
        let output_kind = fragment_output_kind(&fragment.sink);
        let native_scan_ranges = native_scan_ranges
            .get(&fragment.fragment_id)
            .cloned()
            .unwrap_or_default();

        fragment_schedules.push(FragmentSchedulingMetadata {
            fragment_id: fragment.fragment_id,
            has_scan_nodes,
            output_kind,
            native_scan_ranges,
            output_columns,
            boundary_schemas,
            cte_id: fragment.cte_id,
            cte_exchange_nodes: fragment.cte_exchange_nodes.clone(),
        });
    }

    let mut boundary_schemas = fragment_schedules
        .iter()
        .flat_map(|fragment| fragment.boundary_schemas.clone())
        .collect::<Vec<_>>();
    boundary_schemas.extend(edge_boundary_schemas(&refreshed)?);

    let encoded = crate::sql::codegen::proto_encode::plan::encode_distributed_plan_with_context(
        &refreshed,
        crate::sql::codegen::proto_encode::plan::NativePlanEncodeContext {
            mv_refresh_ctx,
            starrocks_scan_sources: Some(&native_scan_planning.starrocks_scan_sources),
        },
    )?;
    let mut native_fragments = BTreeMap::new();
    for fragment in encoded.fragments {
        let fragment_id = fragment.fragment_id;
        if native_fragments.insert(fragment_id, fragment).is_some() {
            return Err(format!(
                "native fragment build encoded duplicate fragment id={fragment_id}"
            ));
        }
    }
    validate_native_fragment_ownership(
        &native_fragments,
        &fragment_schedules,
        refreshed.root_fragment_id,
    )?;

    Ok(MultiFragmentBuildResult {
        fragment_schedules,
        native_fragments,
        root_fragment_id: refreshed.root_fragment_id,
        edges: refreshed.edges.clone(),
        boundary_schemas,
        rf_plan: runtime_filter_plan(&refreshed),
    })
}

fn validate_native_fragment_ownership(
    native_fragments: &BTreeMap<FragmentId, crate::proto::plan::PlanFragment>,
    fragment_schedules: &[FragmentSchedulingMetadata],
    root_fragment_id: FragmentId,
) -> Result<(), String> {
    let native_ids = native_fragments.keys().copied().collect::<BTreeSet<_>>();
    let schedule_ids = fragment_schedules
        .iter()
        .map(|schedule| schedule.fragment_id)
        .collect::<BTreeSet<_>>();
    if schedule_ids.len() != fragment_schedules.len() {
        return Err("native fragment build produced duplicate schedule fragment ids".to_string());
    }
    if !native_ids.contains(&root_fragment_id) {
        return Err(format!(
            "native fragment build is missing root fragment id={root_fragment_id}"
        ));
    }
    if native_ids != schedule_ids {
        return Err(format!(
            "native fragment ids {native_ids:?} do not match schedule ids {schedule_ids:?}"
        ));
    }
    Ok(())
}

fn refresh_distributed_plan_for_fragment_build(
    dp: &DistributedPlan,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<DistributedPlan, String> {
    let mut out = dp.clone();
    for fragment in &mut out.fragments {
        refresh_distributed_node_scan_tables_for_native(&mut fragment.root, mv_refresh_ctx)?;
    }
    Ok(out)
}

fn lower_native_fragment_edges(dp: &mut DistributedPlan) -> Result<(), String> {
    let fragments_by_id: BTreeMap<FragmentId, &PlanFragment> = dp
        .fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();
    let mut stream_source_partitions = BTreeMap::new();
    let mut router_target_partitions = BTreeMap::new();
    for edge in &mut dp.edges {
        let edge_context = format!(
            "{} edge source_fragment_id={} target_fragment_id={} target_exchange_node_id={}",
            fragment_edge_kind_label(&edge.edge_kind),
            edge.source_fragment_id,
            edge.target_fragment_id,
            edge.target_exchange_node_id
        );
        let exchange = target_exchange_for_edge(&fragments_by_id, edge)?;
        match &edge.edge_kind {
            FragmentEdgeKind::Stream => {
                let source = fragments_by_id
                    .get(&edge.source_fragment_id)
                    .ok_or_else(|| {
                        format!(
                            "lower_distributed_plan edge references missing source fragment id={}",
                            edge.source_fragment_id
                        )
                    })?;
                let output_columns = if exchange.output_columns.is_empty() {
                    &source.output_columns
                } else {
                    &exchange.output_columns
                };
                edge.output_partition = exchange.partition.clone();
                edge.stream_kind = canonical_fragment_stream_kind(
                    &edge.output_partition,
                    edge.stream_kind,
                    &edge_context,
                )?;
                edge.output_slot_ids = native_output_column_ids(output_columns, "stream edge")?;
                if stream_source_partitions
                    .insert(edge.source_fragment_id, edge.output_partition.clone())
                    .is_some()
                {
                    return Err(format!(
                        "lower_distributed_plan stream source fragment id={} has multiple outgoing stream edges",
                        edge.source_fragment_id
                    ));
                }
            }
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } => {
                if receive_producer_column_ids.len() != exchange.output_columns.len() {
                    return Err(format!(
                        "lower_distributed_plan CTE multicast receive/output arity mismatch for cte_id={}",
                        cte_id
                    ));
                }
                edge.output_partition = exchange.partition.clone();
                edge.stream_kind = canonical_fragment_stream_kind(
                    &edge.output_partition,
                    edge.stream_kind,
                    &edge_context,
                )?;
                edge.output_slot_ids = receive_producer_column_ids
                    .iter()
                    .map(|column_id| native_output_column_id(*column_id, "CTE multicast edge"))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id,
                branch_id,
                branch_kind,
            } => {
                let source = fragments_by_id.get(&edge.source_fragment_id).ok_or_else(|| {
                    format!(
                        "lower_distributed_plan router edge references missing source fragment id={}",
                        edge.source_fragment_id
                    )
                })?;
                let crate::sql::planner::distributed::DataSink::IcebergChangeStreamRouter(router) =
                    &source.sink
                else {
                    return Err(format!(
                        "lower_distributed_plan router edge source fragment id={} does not use Iceberg change-stream router sink",
                        edge.source_fragment_id
                    ));
                };
                let route = router
                    .branches
                    .iter()
                    .find(|route| {
                        router.group_id == *router_group_id
                            && route.branch_id == *branch_id
                            && route.branch_kind == *branch_kind
                            && route.target_fragment_id == edge.target_fragment_id
                            && route.target_exchange_node_id == edge.target_exchange_node_id
                    })
                    .ok_or_else(|| {
                        format!(
                            "lower_distributed_plan router edge source={} group={} branch_id={} branch_kind={:?} has no matching planner route",
                            edge.source_fragment_id, router_group_id, branch_id, branch_kind
                        )
                    })?;
                edge.output_slot_ids = route
                    .output_ordinals
                    .iter()
                    .map(|ordinal| {
                        source.output_columns.get(*ordinal).ok_or_else(|| {
                            format!(
                                "native router edge output ordinal {ordinal} is out of range for fragment {}",
                                edge.source_fragment_id
                            )
                        })
                    })
                    .map(|column| {
                        column.and_then(|column| {
                            native_output_column_id(column.column_id, "router edge")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                edge.output_partition = native_output_partition_for_ordinals(
                    &source.output_columns,
                    &route.output_partition_ordinals,
                    &format!("branch {:?} partition", route.branch_kind),
                )?;
                edge.stream_kind = canonical_fragment_stream_kind(
                    &edge.output_partition,
                    edge.stream_kind,
                    &edge_context,
                )?;
                let target = (edge.target_fragment_id, edge.target_exchange_node_id);
                if let Some(existing) = router_target_partitions.get(&target)
                    && !native_partitions_equal(existing, &edge.output_partition)?
                {
                    return Err(format!(
                        "lower_distributed_plan router edges have conflicting partitions for target Exchange node_id={} in fragment id={}",
                        edge.target_exchange_node_id, edge.target_fragment_id
                    ));
                }
                router_target_partitions.insert(target, edge.output_partition.clone());
            }
        }
    }
    drop(fragments_by_id);
    for (fragment_id, output_partition) in stream_source_partitions {
        let source = dp
            .fragments
            .iter_mut()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .ok_or_else(|| {
                format!(
                    "lower_distributed_plan edge references missing source fragment id={fragment_id}"
                )
            })?;
        source.output_partition = output_partition;
    }
    for ((fragment_id, exchange_node_id), output_partition) in router_target_partitions {
        let target = dp
            .fragments
            .iter_mut()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .ok_or_else(|| {
                format!(
                    "lower_distributed_plan router edge references missing target fragment id={fragment_id}"
                )
            })?;
        let exchange = find_exchange_node_mut(&mut target.root, exchange_node_id).ok_or_else(|| {
            format!(
                "lower_distributed_plan router edge target_exchange_node_id={exchange_node_id} not found in target fragment id={fragment_id}"
            )
        })?;
        let DistributedNodeKind::Exchange(exchange) = &mut exchange.payload else {
            return Err(format!(
                "lower_distributed_plan router edge target_exchange_node_id={exchange_node_id} in target fragment id={fragment_id} must target Exchange"
            ));
        };
        exchange.partition = output_partition;
    }
    Ok(())
}

fn native_partitions_equal(left: &DataPartition, right: &DataPartition) -> Result<bool, String> {
    Ok(
        crate::sql::codegen::proto_encode::plan::encode_data_partition(left)?
            == crate::sql::codegen::proto_encode::plan::encode_data_partition(right)?,
    )
}

fn native_output_column_ids(
    columns: &[AnalysisOutputColumn],
    context: &str,
) -> Result<Vec<i32>, String> {
    columns
        .iter()
        .map(|column| native_output_column_id(column.column_id, context))
        .collect()
}

pub(super) fn native_output_partition_for_ordinals(
    output_columns: &[AnalysisOutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<DataPartition, String> {
    if ordinals.is_empty() {
        return Ok(DataPartition::unpartitioned());
    }

    let exprs = ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            output_columns
                .get(ordinal)
                .ok_or_else(|| format!("{label} ordinal {ordinal} is out of range"))
                .map(|column| crate::sql::analysis::TypedExpr {
                    kind: crate::sql::analysis::ExprKind::ColumnRef {
                        column_id: column.column_id,
                        qualifier: None,
                        column: column.name.clone(),
                    },
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DataPartition {
        kind: PartitionKind::Hash,
        exprs,
    })
}

fn native_output_column_id(column_id: ColumnId, context: &str) -> Result<i32, String> {
    i32::try_from(column_id.0).map_err(|_| {
        format!(
            "native {context} column {} cannot convert to output slot id",
            column_id.0
        )
    })
}

fn refresh_distributed_node_scan_tables_for_native(
    node: &mut DistributedNode,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<(), String> {
    if let DistributedNodeKind::Scan(scan) = &mut node.payload {
        let refresh_only_source = is_refresh_only_scan_source(&scan.table.source);
        let native_projected_names = native_refresh_scan_projected_names(&scan.table.source);
        let refreshed_table = refresh_scan_table_for_codegen(mv_refresh_ctx, &scan.table)?;
        if let Some(projected_names) = native_projected_names {
            scan.required_columns = Some(merge_required_columns_with_projected(
                scan.required_columns.take(),
                &projected_names,
            ));
        } else if refresh_only_source {
            scan.columns = scan_output_columns_for_refreshed_table(scan, &refreshed_table);
        }
        scan.table = refreshed_table;
    }
    for child in &mut node.children {
        refresh_distributed_node_scan_tables_for_native(child, mv_refresh_ctx)?;
    }
    Ok(())
}

fn is_refresh_only_scan_source(source: &ScanSource) -> bool {
    matches!(
        source,
        ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergMvTargetState(_)
            | ScanSource::IcebergMvTargetLocator(_)
    )
}

pub(super) fn native_refresh_scan_projected_names(source: &ScanSource) -> Option<Vec<String>> {
    match source {
        ScanSource::IcebergMvTargetState(scan) => Some(projected_target_state_column_names(scan)),
        ScanSource::IcebergMvTargetLocator(scan) => {
            Some(projected_target_locator_column_names(scan))
        }
        _ => None,
    }
}

fn scan_output_columns_for_refreshed_table(
    scan: &PlanScanNode,
    table: &TableDef,
) -> Vec<AnalysisOutputColumn> {
    let mut out = Vec::new();
    for column in table
        .columns
        .iter()
        .chain(table.iceberg_row_lineage_metadata_columns.iter())
    {
        if let Some(output_column) = scan
            .columns
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&column.name))
        {
            out.push(output_column.clone());
        }
    }
    for variant_column in &scan.variant_columns {
        if let Some(output_column) = scan
            .columns
            .iter()
            .find(|column| column.column_id == variant_column.synthetic_column_id)
        {
            out.push(output_column.clone());
        }
    }
    out
}

pub(super) fn merge_required_columns_with_projected(
    existing: Option<Vec<String>>,
    projected_names: &[String],
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for name in projected_names
        .iter()
        .cloned()
        .chain(existing.unwrap_or_default())
    {
        if seen.insert(name.to_lowercase()) {
            out.push(name);
        }
    }
    out
}

fn refresh_scan_table_for_codegen(
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    table: &TableDef,
) -> Result<TableDef, String> {
    match &table.source {
        ScanSource::IcebergVersionTable {
            table: iceberg_table,
            snapshot_id,
        } => {
            let refresh_ctx = mv_refresh_ctx
                .ok_or_else(|| "Iceberg version scan requires MV refresh context".to_string())?;
            let mut out = table.clone();
            out.source = refresh_ctx.version_scan_source(iceberg_table, *snapshot_id)?;
            Ok(out)
        }
        ScanSource::IcebergMvTargetState(scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-state scan requires MV refresh context".to_string()
            })?;
            let mut out = table.clone();
            let projected = projected_target_state_column_names(scan);
            retain_projected_iceberg_columns(&mut out, &projected);
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                true,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                false,
            );
            reorder_refresh_table_columns_by_projected_names(&mut out, &projected)?;
            out.source = refresh_ctx.target_state_scan_source(scan)?;
            reject_target_state_equality_deletes(&out.source)?;
            Ok(out)
        }
        ScanSource::IcebergMvTargetLocator(scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-locator scan requires MV refresh context".to_string()
            })?;
            let mut out = table.clone();
            let projected = projected_target_locator_column_names(scan);
            retain_projected_iceberg_columns(&mut out, &projected);
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                true,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                false,
            );
            reorder_refresh_table_columns_by_projected_names(&mut out, &projected)?;
            out.source = refresh_ctx.target_locator_scan_source(scan)?;
            reject_target_state_equality_deletes(&out.source)?;
            Ok(out)
        }
        _ => Ok(table.clone()),
    }
}

fn retain_projected_iceberg_columns(table: &mut TableDef, projected: &[String]) {
    table.columns.retain(|column| {
        projected
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column.name))
    });
    table.iceberg_row_lineage_metadata_columns.retain(|column| {
        projected
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column.name))
    });
}

fn ensure_iceberg_metadata_column(
    table: &mut TableDef,
    projected: &[String],
    name: &str,
    data_type: DataType,
    nullable: bool,
) {
    if !projected
        .iter()
        .any(|projected_name| projected_name.eq_ignore_ascii_case(name))
    {
        return;
    }
    if table
        .columns
        .iter()
        .chain(table.iceberg_row_lineage_metadata_columns.iter())
        .any(|column| column.name.eq_ignore_ascii_case(name))
    {
        return;
    }
    table
        .iceberg_row_lineage_metadata_columns
        .push(crate::sql::catalog::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        });
}

fn reorder_refresh_table_columns_by_projected_names(
    table: &mut TableDef,
    projected: &[String],
) -> Result<(), String> {
    let physical = table.columns.clone();
    let metadata = table.iceberg_row_lineage_metadata_columns.clone();
    let mut next_physical = Vec::new();
    let mut next_metadata = Vec::new();
    let mut seen = BTreeSet::new();

    for name in projected {
        let key = name.to_lowercase();
        if !seen.insert(key) {
            continue;
        }
        if let Some(column) = physical
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
        {
            next_physical.push(column.clone());
            continue;
        }
        if let Some(column) = metadata
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
        {
            next_metadata.push(column.clone());
            continue;
        }
        return Err(format!(
            "refresh-only scan table `{}` cannot resolve projected column `{}`",
            table.name, name
        ));
    }

    table.columns = next_physical;
    table.iceberg_row_lineage_metadata_columns = next_metadata;
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct NativeScanPlanningResult {
    scan_ranges: BTreeMap<FragmentId, BTreeMap<i32, Vec<scan_range::ScanRangeParams>>>,
    starrocks_scan_sources: BTreeMap<i32, StarRocksScanSourceDescriptor>,
}

fn build_native_scan_ranges(
    dp: &mut DistributedPlan,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<NativeScanPlanningResult, String> {
    let mut out = BTreeMap::new();
    let mut starrocks_scan_sources = BTreeMap::new();
    for fragment in &mut dp.fragments {
        let mut per_node = BTreeMap::new();
        collect_native_scan_ranges(
            fragment.fragment_id,
            &mut fragment.root,
            connectors,
            mv_refresh_ctx,
            &mut per_node,
            &mut starrocks_scan_sources,
        )?;
        out.insert(fragment.fragment_id, per_node);
    }
    Ok(NativeScanPlanningResult {
        scan_ranges: out,
        starrocks_scan_sources,
    })
}

fn collect_native_scan_ranges(
    fragment_id: FragmentId,
    node: &mut DistributedNode,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    out: &mut BTreeMap<i32, Vec<scan_range::ScanRangeParams>>,
    starrocks_scan_sources: &mut BTreeMap<i32, StarRocksScanSourceDescriptor>,
) -> Result<(), String> {
    if let DistributedNodeKind::Scan(scan) = &mut node.payload {
        let (ranges, starrocks_source) =
            native_scan_ranges_for_scan(node.node_id, scan, connectors, mv_refresh_ctx)?;
        out.insert(node.node_id, ranges);
        if let Some(source) = starrocks_source
            && starrocks_scan_sources
                .insert(node.node_id, source)
                .is_some()
        {
            return Err(format!(
                "native scan planning duplicate StarRocks scan node_id={}",
                node.node_id
            ));
        }
    }
    for child in &mut node.children {
        if child.fragment_id == fragment_id {
            collect_native_scan_ranges(
                fragment_id,
                child,
                connectors,
                mv_refresh_ctx,
                out,
                starrocks_scan_sources,
            )?;
        }
    }
    Ok(())
}

fn native_scan_ranges_for_scan(
    scan_node_id: i32,
    scan: &mut PlanScanNode,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<
    (
        Vec<scan_range::ScanRangeParams>,
        Option<StarRocksScanSourceDescriptor>,
    ),
    String,
> {
    match &scan.table.source {
        ScanSource::StarRocks { .. } => {
            let planned = plan_native_starrocks_scan_node(scan_node_id, scan, connectors)?;
            Ok((planned.ranges, Some(planned.source)))
        }
        ScanSource::IcebergDataFiles { .. } => {
            build_iceberg_scan_ranges_from_source(scan_node_id, scan, &scan.table.source, None)
                .and_then(|handle| plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle))
                .map(|ranges| (ranges, None))
        }
        ScanSource::IcebergMetadataTable { .. } | ScanSource::IcebergDeltaTable { .. } => {
            Ok((vec![build_iceberg_metadata_scan_range_params()], None))
        }
        ScanSource::IcebergVersionTable { table, snapshot_id } => {
            let refresh_ctx = mv_refresh_ctx
                .ok_or_else(|| "Iceberg version scan requires MV refresh context".to_string())?;
            let source = refresh_ctx.version_scan_source(table, *snapshot_id)?;
            let handle = build_iceberg_scan_ranges_from_source(scan_node_id, scan, &source, None)?;
            plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle)
                .map(|ranges| (ranges, None))
        }
        ScanSource::IcebergMvTargetState(target_scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-state scan requires MV refresh context".to_string()
            })?;
            let source = refresh_ctx.target_state_scan_source(target_scan)?;
            reject_target_state_equality_deletes(&source)?;
            let handle = build_iceberg_scan_ranges_from_source(
                scan_node_id,
                scan,
                &source,
                Some(projected_target_state_column_names(target_scan)),
            )?;
            plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle)
                .map(|ranges| (ranges, None))
        }
        ScanSource::IcebergMvTargetLocator(target_scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-locator scan requires MV refresh context".to_string()
            })?;
            let source = refresh_ctx.target_locator_scan_source(target_scan)?;
            reject_target_state_equality_deletes(&source)?;
            let handle = build_iceberg_scan_ranges_from_source(
                scan_node_id,
                scan,
                &source,
                Some(projected_target_locator_column_names(target_scan)),
            )?;
            plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle)
                .map(|ranges| (ranges, None))
        }
    }
}

fn build_iceberg_scan_ranges_from_source(
    scan_node_id: i32,
    scan: &PlanScanNode,
    source: &ScanSource,
    column_names: Option<Vec<String>>,
) -> Result<crate::connector::scan_planning::TableHandle, String> {
    let ScanSource::IcebergDataFiles {
        table,
        files,
        binding,
        ..
    } = source
    else {
        return Err("refresh-only scan source did not resolve to Iceberg data files".to_string());
    };
    let column_names = column_names.unwrap_or_else(|| effective_scan_column_names(scan));
    let handle = match binding {
        IcebergDataFileBinding::ExplicitFiles => {
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                &table.catalog,
                &table.namespace,
                &table.table,
                table.current_snapshot_id,
                table.clone(),
                files.clone(),
                column_names,
            )
        }
        IcebergDataFileBinding::CurrentSnapshot => {
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
                &table.catalog,
                &table.namespace,
                &table.table,
                table.clone(),
                column_names,
            )
        }
    };
    let _ = scan_node_id;
    Ok(handle)
}

fn plan_iceberg_scan_ranges(
    connectors: &crate::connector::ConnectorRegistry,
    scan_node_id: i32,
    scan: &mut PlanScanNode,
    table_handle: crate::connector::scan_planning::TableHandle,
) -> Result<Vec<scan_range::ScanRangeParams>, String> {
    let ScanSource::IcebergDataFiles { table, .. } = &scan.table.source else {
        return Err("Iceberg scan range source must be Iceberg data files".to_string());
    };
    let table = table.clone();
    let planner = connectors.scan_planner("iceberg")?;
    let scan_handle = planner.begin_scan(table_handle, BeginScanContext::default())?;
    let splits = planner.plan_splits(&scan_handle, SplitPlanningContext::default())?;
    let equality_required = equality_delete_required_columns(scan_node_id, &table, &splits)?;
    if !equality_required.is_empty() {
        let existing_required = match scan.required_columns.take() {
            Some(required) => required,
            None => unrestricted_scan_required_columns(scan),
        };
        scan.required_columns = Some(merge_required_columns_with_additional(
            Some(existing_required),
            &equality_required,
        ));
    }
    let plan = to_native_file_scan(
        planner.name(),
        &scan_handle,
        &splits,
        ConnectorScanContext {
            min_max_predicates: native_scan_min_max_predicates(&scan.predicates),
            columns: scan.table.columns.clone(),
        },
    )?;
    Ok(plan.scan_ranges)
}

fn native_scan_min_max_predicates(
    predicates: &[crate::sql::analysis::TypedExpr],
) -> Vec<crate::common::min_max_predicate::MinMaxPredicate> {
    let mut out = Vec::new();
    for predicate in predicates {
        collect_native_min_max_predicates(predicate, &mut out);
    }
    out
}

fn collect_native_min_max_predicates(
    expr: &crate::sql::analysis::TypedExpr,
    out: &mut Vec<crate::common::min_max_predicate::MinMaxPredicate>,
) {
    use crate::sql::analysis::{BinOp, ExprKind};

    match &expr.kind {
        ExprKind::Nested(inner) => collect_native_min_max_predicates(inner, out),
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_native_min_max_predicates(left, out);
            collect_native_min_max_predicates(right, out);
        }
        ExprKind::BinaryOp { left, op, right } => {
            if let Some(predicate) = native_min_max_comparison(left, *op, right) {
                out.push(predicate);
            } else if let Some(predicate) =
                native_min_max_comparison(right, reverse_comparison(*op), left)
            {
                out.push(predicate);
            }
        }
        _ => {}
    }
}

fn reverse_comparison(op: crate::sql::analysis::BinOp) -> crate::sql::analysis::BinOp {
    use crate::sql::analysis::BinOp;
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        other => other,
    }
}

fn native_min_max_comparison(
    column: &crate::sql::analysis::TypedExpr,
    op: crate::sql::analysis::BinOp,
    literal: &crate::sql::analysis::TypedExpr,
) -> Option<crate::common::min_max_predicate::MinMaxPredicate> {
    use crate::common::min_max_predicate::MinMaxPredicate;
    use crate::sql::analysis::{BinOp, ExprKind};

    let ExprKind::ColumnRef { column: name, .. } = &column.kind else {
        return None;
    };
    if column.data_type != literal.data_type {
        return None;
    }
    let value = native_min_max_literal(literal)?;
    Some(match op {
        BinOp::Eq => MinMaxPredicate::Eq {
            column: name.clone(),
            value,
        },
        BinOp::Lt => MinMaxPredicate::Lt {
            column: name.clone(),
            value,
        },
        BinOp::Le => MinMaxPredicate::Le {
            column: name.clone(),
            value,
        },
        BinOp::Gt => MinMaxPredicate::Gt {
            column: name.clone(),
            value,
        },
        BinOp::Ge => MinMaxPredicate::Ge {
            column: name.clone(),
            value,
        },
        _ => return None,
    })
}

fn native_min_max_literal(
    expr: &crate::sql::analysis::TypedExpr,
) -> Option<crate::common::min_max_predicate::MinMaxPredicateValue> {
    use crate::common::min_max_predicate::MinMaxPredicateValue;
    use crate::sql::analysis::{ExprKind, LiteralValue};
    use arrow::datatypes::{DataType, TimeUnit};

    let ExprKind::Literal(literal) = &expr.kind else {
        return None;
    };
    match (&expr.data_type, literal) {
        (DataType::Boolean, LiteralValue::Bool(value)) => {
            Some(MinMaxPredicateValue::Boolean(*value))
        }
        (DataType::Int8 | DataType::Int16 | DataType::Int32, LiteralValue::Int(value)) => {
            i32::try_from(*value).ok().map(MinMaxPredicateValue::Int32)
        }
        (DataType::Int64, LiteralValue::Int(value)) => Some(MinMaxPredicateValue::Int64(*value)),
        (DataType::Float32, LiteralValue::Float(value)) if value.is_finite() => {
            Some(MinMaxPredicateValue::Float(*value as f32))
        }
        (DataType::Float64, LiteralValue::Float(value)) if value.is_finite() => {
            Some(MinMaxPredicateValue::Double(*value))
        }
        (DataType::Utf8 | DataType::LargeUtf8, LiteralValue::String(value)) => {
            Some(MinMaxPredicateValue::ByteArray(value.as_bytes().to_vec()))
        }
        (DataType::Binary | DataType::LargeBinary, LiteralValue::Binary(value)) => {
            Some(MinMaxPredicateValue::ByteArray(value.clone()))
        }
        (DataType::Date32, LiteralValue::Int(value)) => {
            i32::try_from(*value).ok().map(MinMaxPredicateValue::Date32)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), LiteralValue::Int(value)) => {
            Some(MinMaxPredicateValue::DateTimeMicros(*value))
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), LiteralValue::Int(value)) => {
            Some(MinMaxPredicateValue::DateTimeNanos(*value))
        }
        _ => None,
    }
}

fn merge_required_columns_with_additional(
    existing: Option<Vec<String>>,
    additional: &[String],
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for name in existing
        .unwrap_or_default()
        .into_iter()
        .chain(additional.iter().cloned())
    {
        if seen.insert(name.to_ascii_lowercase()) {
            out.push(name);
        }
    }
    out
}

fn unrestricted_scan_required_columns(scan: &PlanScanNode) -> Vec<String> {
    let table_columns = scan
        .table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let scan_columns = scan
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    merge_required_columns_with_additional(Some(table_columns), &scan_columns)
}

fn equality_delete_required_columns(
    scan_node_id: i32,
    table: &crate::sql::catalog::IcebergTableInfo,
    splits: &[crate::connector::scan_planning::Split],
) -> Result<Vec<String>, String> {
    let mut schema_by_id = BTreeMap::new();
    let mut schema_by_name = BTreeMap::new();
    for field in &table.schema.fields {
        if schema_by_id
            .insert(field.field_id, field.name.clone())
            .is_some()
        {
            return Err(format!(
                "Iceberg ScanNode node_id={scan_node_id} table schema has duplicate field id {}",
                field.field_id
            ));
        }
        let normalized = field.name.to_ascii_lowercase();
        if schema_by_name
            .insert(normalized, field.name.clone())
            .is_some()
        {
            return Err(format!(
                "Iceberg ScanNode node_id={scan_node_id} table schema has duplicate field name {}",
                field.name
            ));
        }
    }

    let mut required = Vec::new();
    let mut required_seen = BTreeSet::new();
    for split in splits {
        let file = crate::connector::iceberg::scan_planner::iceberg_split(split)?;
        for delete in &file.data_file.delete_files {
            if delete.file_content != crate::sql::catalog::IcebergDeleteFileContent::Equality {
                continue;
            }

            let mut resolved_ids = Vec::new();
            let mut ids_seen = BTreeSet::new();
            for field_id in &delete.equality_field_ids {
                if !ids_seen.insert(*field_id) {
                    return Err(format!(
                        "Iceberg equality-delete file {} has duplicate equality field id {}",
                        delete.path, field_id
                    ));
                }
                let name = schema_by_id.get(field_id).ok_or_else(|| {
                    format!(
                        "Iceberg equality-delete file {} references unknown field id {} in table {}",
                        delete.path, field_id, table.table
                    )
                })?;
                resolved_ids.push(name.clone());
            }

            let mut resolved_names = Vec::new();
            let mut names_seen = BTreeSet::new();
            for name in &delete.equality_column_names {
                let normalized = name.to_ascii_lowercase();
                if !names_seen.insert(normalized.clone()) {
                    return Err(format!(
                        "Iceberg equality-delete file {} has duplicate equality column name {}",
                        delete.path, name
                    ));
                }
                let canonical = schema_by_name.get(&normalized).ok_or_else(|| {
                    format!(
                        "Iceberg equality-delete file {} references unknown equality column {} in table {}",
                        delete.path, name, table.table
                    )
                })?;
                resolved_names.push(canonical.clone());
            }

            let columns = match (resolved_ids.is_empty(), resolved_names.is_empty()) {
                (true, true) => {
                    return Err(format!(
                        "Iceberg equality-delete file {} has no equality field identity",
                        delete.path
                    ));
                }
                (false, false) => {
                    let ids = resolved_ids
                        .iter()
                        .map(|name| name.to_ascii_lowercase())
                        .collect::<BTreeSet<_>>();
                    let names = resolved_names
                        .iter()
                        .map(|name| name.to_ascii_lowercase())
                        .collect::<BTreeSet<_>>();
                    if ids != names {
                        return Err(format!(
                            "Iceberg equality-delete file {} field id/name mismatch: ids={resolved_ids:?} names={resolved_names:?}",
                            delete.path
                        ));
                    }
                    resolved_ids
                }
                (false, true) => resolved_ids,
                (true, false) => resolved_names,
            };
            for name in columns {
                if required_seen.insert(name.to_ascii_lowercase()) {
                    required.push(name);
                }
            }
        }
    }
    Ok(required)
}

fn effective_scan_column_names(scan: &PlanScanNode) -> Vec<String> {
    scan.required_columns.clone().unwrap_or_else(|| {
        scan.table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    })
}

fn build_iceberg_metadata_scan_range_params() -> scan_range::ScanRangeParams {
    scan_range::ScanRangeParams::file(scan_range::FileScanRange {
        file_format: scan_range::FileFormat::Parquet,
        full_path: Some("iceberg-metadata".to_string()),
        relative_path: None,
        table_id: None,
        offset: 0,
        length: 0,
        file_length: 0,
        delete_files: Vec::new(),
        deletion_vector_descriptor: None,
        first_row_id: None,
        data_sequence_number: None,
        modification_time: None,
        datacache_options: None,
        included_positions: Vec::new(),
        serialized_split: Some(String::new()),
        use_iceberg_jni_metadata_reader: true,
        ivm_change_op: None,
        file_pruning_min_max_values: None,
    })
}

fn projected_target_state_column_names(
    scan: &crate::sql::catalog::IcebergMvTargetStateScan,
) -> Vec<String> {
    let mut names = Vec::new();
    push_unique_projected_name(&mut names, &scan.row_id_column_name);
    for name in scan
        .group_key_names
        .iter()
        .chain(scan.aggregate_state_names.iter())
    {
        push_unique_projected_name(&mut names, name);
    }
    if let crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
        branch_scope: Some(scope),
        ..
    } = &scan.row_filter
    {
        push_unique_projected_name(&mut names, &scope.branch_id_column_name);
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_projected_name(&mut names, name);
    }
    names
}

fn projected_target_locator_column_names(
    scan: &crate::sql::catalog::IcebergMvTargetLocatorScan,
) -> Vec<String> {
    let mut names = vec![scan.apply_key_column.clone()];
    if let Some(branch_id_column) = &scan.branch_id_column
        && !names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(branch_id_column))
    {
        names.push(branch_id_column.clone());
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_projected_name(&mut names, name);
    }
    names
}

fn push_unique_projected_name(names: &mut Vec<String>, name: &str) {
    if !names
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(name))
    {
        names.push(name.to_string());
    }
}

fn reject_target_state_equality_deletes(source: &ScanSource) -> Result<(), String> {
    let ScanSource::IcebergDataFiles { files, .. } = source else {
        return Ok(());
    };
    let has_equality_delete = files.iter().any(|file| {
        file.delete_files.iter().any(|delete_file| {
            delete_file.file_content == crate::sql::catalog::IcebergDeleteFileContent::Equality
        })
    });
    if has_equality_delete {
        return Err("Iceberg target-state scan does not support equality deletes yet".to_string());
    }
    Ok(())
}

fn output_column_for_boundary(column: &AnalysisOutputColumn) -> OutputColumn {
    OutputColumn {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn result_root_boundary_schema_report(
    fragment_id: FragmentId,
    root_node_id: i32,
    output_columns: &[OutputColumn],
) -> BoundarySchemaReport {
    BoundarySchemaReport {
        fragment_id: Some(fragment_id as i32),
        node_id: root_node_id,
        boundary_kind: BoundaryKind::ResultRoot,
        columns: output_columns_to_boundary_columns(output_columns),
    }
}

fn edge_boundary_schemas(dp: &DistributedPlan) -> Result<Vec<BoundarySchemaReport>, String> {
    let fragments_by_id: BTreeMap<FragmentId, &PlanFragment> = dp
        .fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();
    let mut reports = Vec::with_capacity(dp.edges.len() * 2);
    for edge in &dp.edges {
        let source = fragments_by_id
            .get(&edge.source_fragment_id)
            .ok_or_else(|| {
                format!(
                    "lower_distributed_plan edge references missing source fragment id={}",
                    edge.source_fragment_id
                )
            })?;
        if !fragments_by_id.contains_key(&edge.target_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            ));
        }
        let exchange = target_exchange_for_edge(&fragments_by_id, edge)?;
        let edge_output_columns = match edge.edge_kind {
            FragmentEdgeKind::CteMulticast { .. } | FragmentEdgeKind::Stream => {
                if exchange.output_columns.is_empty() {
                    &source.output_columns
                } else {
                    &exchange.output_columns
                }
            }
            FragmentEdgeKind::IcebergChangeStreamRouter { .. } => &exchange.output_columns,
        };
        let output_columns = edge_output_columns
            .iter()
            .map(output_column_for_boundary)
            .collect::<Vec<_>>();
        let columns = output_columns_to_boundary_columns(&output_columns);
        reports.push(BoundarySchemaReport {
            fragment_id: Some(edge.source_fragment_id as i32),
            node_id: edge.target_exchange_node_id,
            boundary_kind: BoundaryKind::ExchangeSender,
            columns: columns.clone(),
        });
        reports.push(BoundarySchemaReport {
            fragment_id: Some(edge.target_fragment_id as i32),
            node_id: edge.target_exchange_node_id,
            boundary_kind: BoundaryKind::ExchangeReceiver,
            columns,
        });
    }
    Ok(reports)
}

fn runtime_filter_plan(dp: &DistributedPlan) -> Option<RuntimeFilterPlanResult> {
    let mut all_filters = HashMap::new();
    let mut build_side_filters: HashMap<FragmentId, Vec<i32>> = HashMap::new();
    let mut probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>> = HashMap::new();
    let mut probe_targets: HashMap<i32, Vec<(FragmentId, i32)>> = HashMap::new();

    for fragment in &dp.fragments {
        collect_runtime_filter_probe_targets(
            fragment.fragment_id,
            &fragment.root,
            &mut probe_targets,
        );
    }
    for fragment in &dp.fragments {
        collect_runtime_filter_builds(
            fragment.fragment_id,
            &fragment.root,
            &probe_targets,
            &mut all_filters,
            &mut build_side_filters,
            &mut probe_side_filters,
        );
    }

    if all_filters.is_empty() {
        None
    } else {
        Some(RuntimeFilterPlanResult {
            all_filters,
            build_side_filters,
            probe_side_filters,
        })
    }
}

fn collect_runtime_filter_probe_targets(
    fragment_id: FragmentId,
    node: &DistributedNode,
    out: &mut HashMap<i32, Vec<(FragmentId, i32)>>,
) {
    for probe in &node.probe_runtime_filters {
        out.entry(probe.intent.filter_id)
            .or_default()
            .push((fragment_id, node.node_id));
    }
    for child in &node.children {
        collect_runtime_filter_probe_targets(fragment_id, child, out);
    }
}

fn collect_runtime_filter_builds(
    fragment_id: FragmentId,
    node: &DistributedNode,
    probe_targets: &HashMap<i32, Vec<(FragmentId, i32)>>,
    all_filters: &mut HashMap<i32, PlannedRuntimeFilter>,
    build_side_filters: &mut HashMap<FragmentId, Vec<i32>>,
    probe_side_filters: &mut HashMap<FragmentId, Vec<(i32, i32)>>,
) {
    for build in &node.build_runtime_filters {
        let targets = probe_targets
            .get(&build.intent.filter_id)
            .cloned()
            .unwrap_or_default();
        let probe_target_node_ids = targets.iter().map(|(_, node_id)| *node_id).collect();
        let has_remote_targets = targets
            .iter()
            .any(|(target_fragment_id, _)| *target_fragment_id != fragment_id);
        all_filters.insert(
            build.intent.filter_id,
            PlannedRuntimeFilter {
                filter_id: build.intent.filter_id,
                build_plan_node_id: node.node_id,
                probe_target_node_ids,
                has_remote_targets,
                execution_mode: build.intent.execution_mode,
                expr_order: i32::try_from(build.intent.expr_order).unwrap_or(i32::MAX),
            },
        );
        build_side_filters
            .entry(fragment_id)
            .or_default()
            .push(build.intent.filter_id);
        for (target_fragment_id, target_node_id) in targets {
            probe_side_filters
                .entry(target_fragment_id)
                .or_default()
                .push((build.intent.filter_id, target_node_id));
        }
    }
    for child in &node.children {
        collect_runtime_filter_builds(
            fragment_id,
            child,
            probe_targets,
            all_filters,
            build_side_filters,
            probe_side_filters,
        );
    }
}

pub(super) fn validate_distributed_plan(dp: &DistributedPlan) -> Result<(), String> {
    super::validate_global_node_ids(dp)?;
    if dp.fragments.is_empty() {
        return Err("lower_distributed_plan requires at least one fragment".to_string());
    }

    let mut fragments_by_id = BTreeMap::new();
    for fragment in &dp.fragments {
        if fragments_by_id
            .insert(fragment.fragment_id, fragment)
            .is_some()
        {
            return Err(format!(
                "lower_distributed_plan duplicate fragment id={}",
                fragment.fragment_id
            ));
        }
    }

    for fragment in &dp.fragments {
        ensure_unpartitioned("data_partition", &fragment.data_partition)?;
        if fragment.output_exprs.is_some() {
            return Err(format!(
                "lower_distributed_plan does not support fragment output_exprs for fragment id={}",
                fragment.fragment_id
            ));
        }
        validate_node_fragment_ownership(fragment.fragment_id, &fragment.root)?;

        if fragment.fragment_id == dp.root_fragment_id {
            if !matches!(
                fragment.sink,
                crate::sql::planner::distributed::DataSink::Result
                    | crate::sql::planner::distributed::DataSink::IcebergWrite(_)
                    | crate::sql::planner::distributed::DataSink::IcebergChangeStreamRouter(_)
            ) {
                return Err(format!(
                    "lower_distributed_plan root fragment id={} must use result, Iceberg write, or Iceberg change-stream router sink",
                    fragment.fragment_id
                ));
            }
            ensure_unpartitioned("root output_partition", &fragment.output_partition)?;
        } else if !matches!(
            fragment.sink,
            crate::sql::planner::distributed::DataSink::Noop
                | crate::sql::planner::distributed::DataSink::IcebergWrite(_)
        ) {
            return Err(format!(
                "lower_distributed_plan non-root fragment id={} must use noop or Iceberg write sink",
                fragment.fragment_id
            ));
        }
    }

    if !fragments_by_id.contains_key(&dp.root_fragment_id) {
        return Err(format!(
            "lower_distributed_plan root fragment id={} was not found",
            dp.root_fragment_id
        ));
    }

    for edge in &dp.edges {
        if !fragments_by_id.contains_key(&edge.source_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing source fragment id={}",
                edge.source_fragment_id
            ));
        }
        if !fragments_by_id.contains_key(&edge.target_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            ));
        }
        target_exchange_for_edge(&fragments_by_id, edge)?;
    }
    Ok(())
}

fn validate_node_fragment_ownership(
    fragment_id: FragmentId,
    node: &DistributedNode,
) -> Result<(), String> {
    if node.fragment_id != fragment_id {
        return Err(format!(
            "lower_distributed_plan fragment id={} contains node_id={} with fragment_id={}",
            fragment_id, node.node_id, node.fragment_id
        ));
    }
    for child in &node.children {
        validate_node_fragment_ownership(fragment_id, child)?;
    }
    Ok(())
}

fn ensure_unpartitioned(label: &str, partition: &DataPartition) -> Result<(), String> {
    if !matches!(partition.kind, PartitionKind::Unpartitioned) || !partition.exprs.is_empty() {
        return Err(format!(
            "lower_distributed_plan supports only unpartitioned {label}"
        ));
    }
    Ok(())
}

fn target_exchange_for_edge<'a>(
    fragments_by_id: &BTreeMap<FragmentId, &'a PlanFragment>,
    edge: &crate::sql::planner::distributed::FragmentEdge,
) -> Result<&'a crate::sql::planner::distributed::ExchangeReceiver, String> {
    let target = fragments_by_id
        .get(&edge.target_fragment_id)
        .ok_or_else(|| {
            format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            )
        })?;
    let exchange = find_exchange_node(&target.root, edge.target_exchange_node_id).ok_or_else(|| {
        format!(
            "lower_distributed_plan edge target_exchange_node_id={} not found in target fragment id={}",
            edge.target_exchange_node_id, edge.target_fragment_id
        )
    })?;
    let DistributedNodeKind::Exchange(exchange) = &exchange.payload else {
        return Err(format!(
            "lower_distributed_plan edge target_exchange_node_id={} in target fragment id={} must target Exchange",
            edge.target_exchange_node_id, edge.target_fragment_id
        ));
    };
    if edge.source_fragment_id != exchange.source_fragment_id {
        return Err(format!(
            "lower_distributed_plan {} edge source_fragment_id={} does not match Exchange source_fragment_id={} for target_exchange_node_id={} in target fragment id={}",
            fragment_edge_kind_label(&edge.edge_kind),
            edge.source_fragment_id,
            exchange.source_fragment_id,
            edge.target_exchange_node_id,
            edge.target_fragment_id
        ));
    }
    validate_exchange_partition(&exchange.partition)?;
    match (&edge.edge_kind, &exchange.flavor) {
        (FragmentEdgeKind::Stream, ExchangeFlavor::Distribution)
        | (FragmentEdgeKind::Stream, ExchangeFlavor::LimitOffset { .. })
        | (FragmentEdgeKind::Stream, ExchangeFlavor::TopNSplit { .. }) => {}
        (
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            },
            ExchangeFlavor::CteMulticast {
                cte_id: exchange_cte_id,
                receive_producer_column_ids: exchange_ids,
            },
        ) => {
            if cte_id != exchange_cte_id || receive_producer_column_ids != exchange_ids {
                return Err(format!(
                    "lower_distributed_plan CTE multicast edge metadata does not match Exchange metadata for target_exchange_node_id={} in target fragment id={}",
                    edge.target_exchange_node_id, edge.target_fragment_id
                ));
            }
        }
        (FragmentEdgeKind::IcebergChangeStreamRouter { .. }, ExchangeFlavor::Distribution) => {}
        (FragmentEdgeKind::Stream, _) => {
            return Err(format!(
                "lower_distributed_plan stream edge target_exchange_node_id={} in target fragment id={} must target stream Exchange",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
        (FragmentEdgeKind::CteMulticast { .. }, _) => {
            return Err(format!(
                "lower_distributed_plan CTE multicast edge target_exchange_node_id={} in target fragment id={} must target Exchange(CteMulticast)",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
        (FragmentEdgeKind::IcebergChangeStreamRouter { .. }, _) => {
            return Err(format!(
                "lower_distributed_plan Iceberg change-stream router edge target_exchange_node_id={} in target fragment id={} must target Exchange(Distribution)",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
    }
    Ok(exchange)
}

fn fragment_edge_kind_label(edge_kind: &FragmentEdgeKind) -> &'static str {
    match edge_kind {
        FragmentEdgeKind::Stream => "stream",
        FragmentEdgeKind::CteMulticast { .. } => "CTE multicast",
        FragmentEdgeKind::IcebergChangeStreamRouter { .. } => "Iceberg change-stream router",
    }
}

fn validate_exchange_partition(partition: &DataPartition) -> Result<(), String> {
    if matches!(partition.kind, PartitionKind::Hash) && partition.exprs.is_empty() {
        return Err(
            "DistributedPlan HASH Exchange has no native partition expressions".to_string(),
        );
    }
    Ok(())
}

fn find_exchange_node(node: &DistributedNode, node_id: i32) -> Option<&DistributedNode> {
    if node.node_id == node_id {
        return Some(node);
    }
    for child in &node.children {
        if let Some(found) = find_exchange_node(child, node_id) {
            return Some(found);
        }
    }
    None
}

fn find_exchange_node_mut(
    node: &mut DistributedNode,
    node_id: i32,
) -> Option<&mut DistributedNode> {
    if node.node_id == node_id {
        return Some(node);
    }
    node.children
        .iter_mut()
        .find_map(|child| find_exchange_node_mut(child, node_id))
}

fn distributed_node_has_scan(node: &DistributedNode) -> bool {
    matches!(node.payload, DistributedNodeKind::Scan(_))
        || node.children.iter().any(distributed_node_has_scan)
}

fn fragment_output_kind(sink: &crate::sql::planner::distributed::DataSink) -> FragmentOutputKind {
    match sink {
        crate::sql::planner::distributed::DataSink::Result => FragmentOutputKind::Result,
        crate::sql::planner::distributed::DataSink::IcebergWrite(_) => {
            FragmentOutputKind::TerminalWrite
        }
        crate::sql::planner::distributed::DataSink::Noop
        | crate::sql::planner::distributed::DataSink::IcebergChangeStreamRouter(_) => {
            FragmentOutputKind::NonTerminal
        }
    }
}

fn canonical_fragment_stream_kind(
    partition: &DataPartition,
    planned_stream_kind: FragmentStreamKind,
    context: &str,
) -> Result<FragmentStreamKind, String> {
    let valid = matches!(
        (partition.kind, planned_stream_kind),
        (
            PartitionKind::Unpartitioned,
            FragmentStreamKind::Gather | FragmentStreamKind::Broadcast
        ) | (PartitionKind::Random, FragmentStreamKind::Other)
            | (PartitionKind::Hash, FragmentStreamKind::Partitioned)
    );
    if valid {
        Ok(planned_stream_kind)
    } else {
        Err(format!(
            "{context} has invalid stream/partition combination: partition_kind={:?} stream_kind={planned_stream_kind:?}; allowed combinations are Unpartitioned+Gather, Unpartitioned+Broadcast, Random+Other, and Hash+Partitioned",
            partition.kind
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::ConnectorRegistry;
    use crate::sql::analysis::cte::CteId;
    use crate::sql::analysis::{ExprKind, OutputColumn as AnalysisOutputColumn, TypedExpr};
    use crate::sql::catalog::{CatalogProvider, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    struct EmptyCatalog;

    impl CatalogProvider for EmptyCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            Err(format!("unexpected table lookup {database}.{table}"))
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn output_col(id: u32, name: &str) -> AnalysisOutputColumn {
        AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn physical_values_node(
        fragment_id: FragmentId,
        node_id: i32,
        columns: Vec<AnalysisOutputColumn>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
        }
    }

    #[derive(Debug)]
    struct PlannedIcebergFiles {
        files: Vec<crate::sql::catalog::IcebergDataFileInfo>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner for PlannedIcebergFiles {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            _ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            let table = table
                .downcast_ref::<crate::connector::iceberg::scan_planner::IcebergTableHandle>()
                .ok_or_else(|| "PlannedIcebergFiles expected IcebergTableHandle".to_string())?
                .clone();
            Ok(crate::connector::scan_planning::ScanHandle::new(
                "iceberg",
                crate::connector::iceberg::scan_planner::IcebergScanHandle { table },
            ))
        }

        fn plan_splits(
            &self,
            _scan: &crate::connector::scan_planning::ScanHandle,
            _ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            Ok(self
                .files
                .iter()
                .cloned()
                .map(|data_file| {
                    crate::connector::scan_planning::Split::new(
                        "iceberg",
                        crate::connector::iceberg::scan_planner::IcebergSplit { data_file },
                    )
                })
                .collect())
        }
    }

    fn iceberg_schema_field(
        field_id: i32,
        name: &str,
    ) -> crate::sql::catalog::IcebergSchemaFieldDef {
        crate::sql::catalog::IcebergSchemaFieldDef {
            field_id,
            name: name.to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        }
    }

    fn iceberg_table_info() -> crate::sql::catalog::IcebergTableInfo {
        crate::sql::catalog::IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "s3://bucket/test_table".to_string(),
            schema: crate::sql::catalog::IcebergSchemaDef {
                fields: vec![
                    iceberg_schema_field(1, "id"),
                    iceberg_schema_field(3, "category"),
                ],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn equality_delete_file(
        equality_column_names: Vec<&str>,
        equality_field_ids: Vec<i32>,
    ) -> crate::sql::catalog::IcebergDeleteFileInfo {
        crate::sql::catalog::IcebergDeleteFileInfo {
            path: "s3://bucket/eq-delete.parquet".to_string(),
            file_format: crate::sql::catalog::IcebergDeleteFileFormat::Parquet,
            file_content: crate::sql::catalog::IcebergDeleteFileContent::Equality,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            equality_column_names: equality_column_names
                .into_iter()
                .map(str::to_string)
                .collect(),
            equality_field_ids,
        }
    }

    fn iceberg_data_file(
        delete_files: Vec<crate::sql::catalog::IcebergDeleteFileInfo>,
    ) -> crate::sql::catalog::IcebergDataFileInfo {
        crate::sql::catalog::IcebergDataFileInfo {
            path: "s3://bucket/data.parquet".to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files,
            manifest_path: None,
            partition_values: Vec::new(),
        }
    }

    fn iceberg_i32_stats_file(
        path: &str,
        min: i32,
        max: i32,
    ) -> crate::sql::catalog::IcebergDataFileInfo {
        let mut file = iceberg_data_file(Vec::new());
        file.path = path.to_string();
        file.column_stats = Some(HashMap::from([(
            "id".to_string(),
            crate::sql::catalog::IcebergColumnStats {
                null_count: Some(0),
                value_count: Some(10),
                column_size: None,
                lower_bound: Some(min.to_le_bytes().to_vec()),
                upper_bound: Some(max.to_le_bytes().to_vec()),
            },
        )]));
        file
    }

    fn iceberg_identity_partition_file(
        path: &str,
        id: i32,
    ) -> crate::sql::catalog::IcebergDataFileInfo {
        let mut file = iceberg_data_file(Vec::new());
        file.path = path.to_string();
        file.partition_key = Some(format!("Struct([{id}])"));
        file.partition_values = vec![crate::sql::catalog::IcebergPartitionFieldValue {
            source_column: "id".to_string(),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: Some(crate::sql::catalog::IcebergPartitionValue::Int32(id)),
        }];
        file
    }

    fn position_delete_file(path: &str) -> crate::sql::catalog::IcebergDeleteFileInfo {
        crate::sql::catalog::IcebergDeleteFileInfo {
            path: path.to_string(),
            file_format: crate::sql::catalog::IcebergDeleteFileFormat::Parquet,
            file_content: crate::sql::catalog::IcebergDeleteFileContent::Position,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            equality_column_names: Vec::new(),
            equality_field_ids: Vec::new(),
        }
    }

    fn iceberg_scan_plan(required_columns: Option<Vec<&str>>) -> DistributedPlan {
        iceberg_scan_plan_with_outputs(required_columns, &["id"])
    }

    fn iceberg_scan_plan_with_outputs(
        required_columns: Option<Vec<&str>>,
        output_names: &[&str],
    ) -> DistributedPlan {
        let id = AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        };
        let category = AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(3),
            name: "category".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        };
        let all_outputs = [id, category];
        let output_columns = output_names
            .iter()
            .map(|name| {
                all_outputs
                    .iter()
                    .find(|column| column.name == *name)
                    .unwrap_or_else(|| panic!("unknown Iceberg scan test output {name}"))
                    .clone()
            })
            .collect::<Vec<_>>();
        let table = TableDef {
            name: "ice_t".to_string(),
            columns: vec![
                crate::sql::catalog::ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "category".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::IcebergDataFiles {
                table: iceberg_table_info(),
                files: Vec::new(),
                cloud_properties: BTreeMap::new(),
                binding: IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let scan = DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: vec![10],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table,
                alias: None,
                columns: output_columns.clone(),
                predicates: Vec::new(),
                required_columns: required_columns
                    .map(|columns| columns.into_iter().map(str::to_string).collect()),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
        };
        DistributedPlan {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: scan,
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: crate::sql::planner::distributed::DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
        }
    }

    fn set_iceberg_scan_predicates(plan: &mut DistributedPlan, predicates: Vec<TypedExpr>) {
        let DistributedNodeKind::Scan(scan) = &mut plan.fragments[0].root.payload else {
            panic!("root must be scan");
        };
        scan.predicates = predicates;
    }

    fn id_eq_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId::new_for_test(1),
                        qualifier: Some("ice_t".to_string()),
                        column: "id".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                }),
                op: crate::sql::analysis::BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(value)),
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn iceberg_registry(files: Vec<crate::sql::catalog::IcebergDataFileInfo>) -> ConnectorRegistry {
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_planner(std::sync::Arc::new(PlannedIcebergFiles { files }));
        registry
    }

    fn native_root_scan(result: &MultiFragmentBuildResult) -> &crate::proto::plan::ScanNode {
        let root = result.native_fragments[&result.root_fragment_id]
            .root
            .as_ref()
            .expect("root node");
        let crate::proto::plan::distributed_node::Payload::Physical(physical) =
            root.payload.as_ref().expect("root payload")
        else {
            panic!("root must be physical");
        };
        let crate::proto::plan::plan_node::Kind::Scan(scan) =
            physical.kind.as_ref().expect("physical kind")
        else {
            panic!("root must be scan");
        };
        scan
    }

    fn native_file_ranges(
        result: &MultiFragmentBuildResult,
    ) -> &[crate::runtime::scan_range::ScanRangeParams] {
        result.fragment_schedules[0]
            .native_scan_ranges
            .get(&10)
            .map(Vec::as_slice)
            .expect("scan node ranges")
    }

    fn native_file_range(
        range: &crate::runtime::scan_range::ScanRangeParams,
    ) -> &crate::runtime::scan_range::FileScanRange {
        match &range.range {
            crate::runtime::scan_range::ScanRange::File(file) => file,
            crate::runtime::scan_range::ScanRange::StarRocksTablet(_) => {
                panic!("expected file range")
            }
        }
    }

    #[test]
    fn equality_delete_field_ids_are_merged_into_native_required_columns() {
        let plan = iceberg_scan_plan(Some(vec!["id"]));
        let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
            Vec::new(),
            vec![3],
        )])]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build native Iceberg scan");

        assert_eq!(
            native_root_scan(&result).required_columns,
            vec!["id", "category"]
        );
    }

    #[test]
    fn equality_delete_column_names_are_merged_into_native_required_columns() {
        let plan = iceberg_scan_plan(Some(vec!["id"]));
        let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
            vec!["category"],
            Vec::new(),
        )])]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build native Iceberg scan");

        assert_eq!(
            native_root_scan(&result).required_columns,
            vec!["id", "category"]
        );
    }

    #[test]
    fn equality_delete_key_from_planned_splits_is_hidden_from_query_projection() {
        let plan = iceberg_scan_plan(Some(vec!["id"]));
        let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
            Vec::new(),
            vec![3],
        )])]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build native Iceberg scan");
        let scan = native_root_scan(&result);

        assert_eq!(scan.required_columns, vec!["id", "category"]);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id"]
        );
    }

    #[test]
    fn equality_delete_with_unrestricted_non_key_projection_preserves_full_read_layout() {
        let plan = iceberg_scan_plan_with_outputs(None, &["id"]);
        let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
            Vec::new(),
            vec![3],
        )])]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build unrestricted native Iceberg scan");
        let scan = native_root_scan(&result);

        assert_eq!(scan.required_columns, vec!["id", "category"]);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id"]
        );
        let table = scan.table.as_ref().expect("native scan table");
        assert!(
            table.columns.iter().any(|column| column.name == "category"),
            "hidden equality key must be materializable from the table schema"
        );
    }

    #[test]
    fn equality_delete_with_unrestricted_select_all_preserves_all_query_outputs() {
        let plan = iceberg_scan_plan_with_outputs(None, &["id", "category"]);
        let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
            Vec::new(),
            vec![3],
        )])]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build unrestricted SELECT * Iceberg scan");
        let scan = native_root_scan(&result);

        assert_eq!(scan.required_columns, vec!["id", "category"]);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "category"]
        );
    }

    #[test]
    fn equality_delete_unknown_field_id_is_native_planning_error() {
        let plan = iceberg_scan_plan(Some(vec!["id"]));
        let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
            Vec::new(),
            vec![99],
        )])]);

        let err = match lower_distributed_plan(&plan, &EmptyCatalog, &registry, None) {
            Ok(_) => panic!("unknown equality field id must fail"),
            Err(err) => err,
        };

        assert!(err.contains("unknown field id 99"), "{err}");
    }

    #[test]
    fn equality_delete_duplicate_identity_is_native_planning_error() {
        for delete_file in [
            equality_delete_file(Vec::new(), vec![3, 3]),
            equality_delete_file(vec!["category", "CATEGORY"], Vec::new()),
        ] {
            let plan = iceberg_scan_plan(Some(vec!["id"]));
            let registry = iceberg_registry(vec![iceberg_data_file(vec![delete_file])]);

            let err = match lower_distributed_plan(&plan, &EmptyCatalog, &registry, None) {
                Ok(_) => panic!("duplicate equality identity must fail"),
                Err(err) => err,
            };
            assert!(err.contains("duplicate equality"), "{err}");
        }
    }

    #[test]
    fn equality_delete_field_id_and_name_mismatch_is_native_planning_error() {
        let plan = iceberg_scan_plan(Some(vec!["id"]));
        let registry = iceberg_registry(vec![iceberg_data_file(vec![equality_delete_file(
            vec!["id"],
            vec![3],
        )])]);

        let err = match lower_distributed_plan(&plan, &EmptyCatalog, &registry, None) {
            Ok(_) => panic!("equality id/name mismatch must fail"),
            Err(err) => err,
        };

        assert!(err.contains("field id/name mismatch"), "{err}");
    }

    #[test]
    fn native_iceberg_scan_predicate_prunes_file_stats_for_id_12() {
        let mut plan = iceberg_scan_plan(None);
        set_iceberg_scan_predicates(&mut plan, vec![id_eq_literal(12)]);
        let registry = iceberg_registry(vec![
            iceberg_i32_stats_file("s3://bucket/id-1-5.parquet", 1, 5),
            iceberg_i32_stats_file("s3://bucket/id-10-20.parquet", 10, 20),
        ]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build native Iceberg scan");
        let ranges = native_file_ranges(&result);

        assert_eq!(ranges.len(), 1);
        assert_eq!(
            native_file_range(&ranges[0]).full_path.as_deref(),
            Some("s3://bucket/id-10-20.parquet")
        );
    }

    #[test]
    fn native_iceberg_scan_predicate_prunes_identity_partition_for_id_12() {
        let mut plan = iceberg_scan_plan(None);
        set_iceberg_scan_predicates(&mut plan, vec![id_eq_literal(12)]);
        let registry = iceberg_registry(vec![
            iceberg_identity_partition_file("s3://bucket/id-1.parquet", 1),
            iceberg_identity_partition_file("s3://bucket/id-12.parquet", 12),
        ]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build native Iceberg scan");
        let ranges = native_file_ranges(&result);

        assert_eq!(ranges.len(), 1);
        assert_eq!(
            native_file_range(&ranges[0]).full_path.as_deref(),
            Some("s3://bucket/id-12.parquet")
        );
    }

    #[test]
    fn native_iceberg_scan_splits_large_plain_file() {
        let plan = iceberg_scan_plan(None);
        let mut file = iceberg_data_file(Vec::new());
        file.path = "s3://bucket/large.parquet".to_string();
        file.size = 300 * 1024 * 1024;
        let registry = iceberg_registry(vec![file]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("build native Iceberg scan");
        let ranges = native_file_ranges(&result);

        assert_eq!(ranges.len(), 3);
        assert_eq!(native_file_range(&ranges[0]).offset, 0);
        assert_eq!(native_file_range(&ranges[1]).offset, 128 * 1024 * 1024);
        assert_eq!(native_file_range(&ranges[2]).offset, 256 * 1024 * 1024);
        assert_eq!(native_file_range(&ranges[2]).length, 44 * 1024 * 1024);
    }

    #[test]
    fn native_iceberg_scan_rejects_excessive_delete_apply_cost() {
        let plan = iceberg_scan_plan(None);
        let delete_files = (0..1025)
            .map(|idx| position_delete_file(&format!("s3://bucket/delete-{idx}.parquet")))
            .collect();
        let registry = iceberg_registry(vec![iceberg_data_file(delete_files)]);

        let err = match lower_distributed_plan(&plan, &EmptyCatalog, &registry, None) {
            Ok(_) => panic!("delete-heavy scan must fail"),
            Err(err) => err,
        };

        assert!(err.contains("too many Iceberg delete files"), "{err}");
    }

    #[test]
    fn native_iceberg_scan_unsupported_predicate_does_not_guess_pruning() {
        let mut plan = iceberg_scan_plan(None);
        set_iceberg_scan_predicates(
            &mut plan,
            vec![TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: "abs".to_string(),
                    args: vec![id_eq_literal(12)],
                    distinct: false,
                },
                data_type: DataType::Boolean,
                nullable: false,
            }],
        );
        let registry = iceberg_registry(vec![
            iceberg_i32_stats_file("s3://bucket/id-1-5.parquet", 1, 5),
            iceberg_i32_stats_file("s3://bucket/id-10-20.parquet", 10, 20),
        ]);

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &registry, None)
            .expect("unsupported pruning predicate must preserve scan semantics");

        assert_eq!(native_file_ranges(&result).len(), 2);
    }

    #[test]
    fn planner_broadcast_edge_remains_broadcast_through_builder_and_scheduling() {
        use crate::sql::planner::physical::{
            PhysicalPlanKind, PhysicalPlanNode, RedistributeMode, RedistributeNode,
        };

        let columns = vec![output_col(1, "k")];
        let values = PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: columns.clone(),
            }),
            children: Vec::new(),
            output_columns: columns.clone(),
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let broadcast = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: Vec::new(),
                output_columns: columns.clone(),
            }),
            children: vec![values],
            output_columns: columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let planned = crate::sql::planner::distributed::build::build_distributed_plan(&broadcast)
            .expect("planner broadcast DistributedPlan");
        assert_eq!(planned.edges[0].stream_kind, FragmentStreamKind::Broadcast);
        assert!(matches!(
            planned.edges[0].output_partition.kind,
            PartitionKind::Unpartitioned
        ));

        let mut result =
            lower_distributed_plan(&planned, &EmptyCatalog, &ConnectorRegistry::new(), None)
                .expect("native fragment build");
        assert_eq!(result.edges[0].stream_kind, FragmentStreamKind::Broadcast);
        assert!(matches!(
            result.edges[0].output_partition.kind,
            PartitionKind::Unpartitioned
        ));

        let target_fragment_id = result.edges[0].target_fragment_id;
        for schedule in &mut result.fragment_schedules {
            schedule.output_kind = if schedule.fragment_id == target_fragment_id {
                FragmentOutputKind::TerminalWrite
            } else {
                FragmentOutputKind::NonTerminal
            };
            if schedule.fragment_id == target_fragment_id {
                schedule.has_scan_nodes = true;
                schedule.native_scan_ranges.insert(
                    99,
                    vec![
                        build_iceberg_metadata_scan_range_params(),
                        build_iceberg_metadata_scan_range_params(),
                        build_iceberg_metadata_scan_range_params(),
                    ],
                );
            }
        }
        let scheduler = crate::runtime::scheduler::FragmentScheduler::new(vec![
            "127.0.0.1:19001".parse().unwrap(),
            "127.0.0.1:19002".parse().unwrap(),
            "127.0.0.1:19003".parse().unwrap(),
        ]);
        let scheduling = scheduler
            .assign(
                &result.fragment_schedules,
                &result.edges,
                crate::common::types::UniqueId { hi: 1, lo: 7 },
            )
            .expect("schedule broadcast plan");
        assert_eq!(
            scheduling.by_fragment[&target_fragment_id].len(),
            3,
            "broadcast input must not collapse a target with three scan ranges to Gather"
        );
    }

    #[test]
    fn random_partition_with_other_stream_kind_remains_other() {
        let mut plan = stream_exchange_plan(ExchangeFlavor::Distribution);
        let DistributedNodeKind::Exchange(exchange) = &mut plan.fragments[1].root.payload else {
            panic!("target must be exchange");
        };
        exchange.partition = DataPartition {
            kind: PartitionKind::Random,
            exprs: Vec::new(),
        };
        plan.edges[0].stream_kind = FragmentStreamKind::Other;

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &ConnectorRegistry::new(), None)
            .expect("build random stream");

        assert_eq!(result.edges[0].stream_kind, FragmentStreamKind::Other);
        assert!(matches!(
            result.edges[0].output_partition.kind,
            PartitionKind::Random
        ));
    }

    #[test]
    fn unpartitioned_stream_rejects_other_and_partitioned_with_edge_context() {
        for stream_kind in [FragmentStreamKind::Other, FragmentStreamKind::Partitioned] {
            let mut plan = stream_exchange_plan(ExchangeFlavor::Distribution);
            plan.edges[0].stream_kind = stream_kind;

            let err =
                match lower_distributed_plan(&plan, &EmptyCatalog, &ConnectorRegistry::new(), None)
                {
                    Ok(_) => {
                        panic!("Unpartitioned edge with stream kind {stream_kind:?} must fail")
                    }
                    Err(err) => err,
                };

            assert!(err.contains("Unpartitioned"), "{err}");
            assert!(err.contains(&format!("{stream_kind:?}")), "{err}");
            assert!(err.contains("source_fragment_id=1"), "{err}");
            assert!(err.contains("target_fragment_id=0"), "{err}");
            assert!(err.contains("target_exchange_node_id=20"), "{err}");
        }
    }

    #[test]
    fn legal_stream_partition_kind_combinations_remain_unchanged() {
        let cases = [
            (DataPartition::unpartitioned(), FragmentStreamKind::Gather),
            (
                DataPartition::unpartitioned(),
                FragmentStreamKind::Broadcast,
            ),
            (
                DataPartition {
                    kind: PartitionKind::Random,
                    exprs: Vec::new(),
                },
                FragmentStreamKind::Other,
            ),
            (
                DataPartition {
                    kind: PartitionKind::Hash,
                    exprs: vec![column_ref(1, "k")],
                },
                FragmentStreamKind::Partitioned,
            ),
        ];

        for (partition, stream_kind) in cases {
            let mut plan = stream_exchange_plan(ExchangeFlavor::Distribution);
            let DistributedNodeKind::Exchange(exchange) = &mut plan.fragments[1].root.payload
            else {
                panic!("target must be exchange");
            };
            exchange.partition = partition.clone();
            plan.edges[0].stream_kind = stream_kind;

            let result =
                lower_distributed_plan(&plan, &EmptyCatalog, &ConnectorRegistry::new(), None)
                    .unwrap_or_else(|err| {
                        panic!(
                            "legal stream combination {:?}+{stream_kind:?} must lower: {err}",
                            partition.kind
                        )
                    });

            assert_eq!(result.edges[0].stream_kind, stream_kind);
            assert_eq!(
                std::mem::discriminant(&result.edges[0].output_partition.kind),
                std::mem::discriminant(&partition.kind)
            );
        }
    }

    fn stream_exchange_plan(flavor: ExchangeFlavor) -> DistributedPlan {
        let columns = vec![output_col(1, "k")];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Noop,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: columns.clone(),
                    output_qualifier: None,
                    flavor,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        DistributedPlan {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: Vec::new(),
            }],
        }
    }

    #[test]
    fn lower_distributed_plan_accepts_stream_limit_and_topn_exchange_flavors() {
        let cases = vec![
            (
                "limit_offset",
                ExchangeFlavor::LimitOffset {
                    limit: Some(1),
                    offset: Some(0),
                },
            ),
            (
                "topn_split",
                ExchangeFlavor::TopNSplit {
                    items: Vec::new(),
                    limit: Some(1),
                    offset: Some(0),
                },
            ),
        ];

        for (label, flavor) in cases {
            let dp = stream_exchange_plan(flavor);
            lower_distributed_plan(&dp, &EmptyCatalog, &ConnectorRegistry::new(), None)
                .unwrap_or_else(|err| panic!("{label} stream exchange should lower: {err}"));
        }
    }

    #[test]
    fn stream_edge_normalization_keeps_schedule_sink_and_receiver_partition_consistent() {
        let mut dp = stream_exchange_plan(ExchangeFlavor::Distribution);
        dp.fragments[0].output_partition = DataPartition {
            kind: PartitionKind::Random,
            exprs: Vec::new(),
        };
        let expected_partition = DataPartition {
            kind: PartitionKind::Hash,
            exprs: vec![column_ref(1, "k")],
        };
        let DistributedNodeKind::Exchange(exchange) = &mut dp.fragments[1].root.payload else {
            panic!("consumer must be an exchange");
        };
        exchange.partition = expected_partition.clone();
        dp.edges[0].stream_kind = FragmentStreamKind::Partitioned;

        let result = lower_distributed_plan(&dp, &EmptyCatalog, &ConnectorRegistry::new(), None)
            .expect("native fragment build");

        assert!(matches!(
            result.edges[0].output_partition.kind,
            PartitionKind::Hash
        ));
        assert_eq!(result.edges[0].output_partition.exprs.len(), 1);
        assert_eq!(result.edges[0].stream_kind, FragmentStreamKind::Partitioned);
        let source = result.native_fragments.get(&1).expect("source fragment");
        let source_partition = source
            .output_partition
            .as_ref()
            .expect("source output partition");
        assert_eq!(
            source_partition.kind,
            crate::proto::plan::PartitionKind::Hash as i32
        );
        assert_eq!(source_partition.exprs.len(), 1);
        let sink_partition = match source
            .sink
            .as_ref()
            .and_then(|sink| sink.kind.as_ref())
            .expect("source stream sink")
        {
            crate::proto::plan::data_sink::Kind::DataStream(sink) => sink
                .output_partition
                .as_ref()
                .expect("stream sink output partition"),
            other => panic!("expected stream sink, got {other:?}"),
        };
        let canonical_edge_partition =
            crate::sql::codegen::proto_encode::plan::encode_data_partition(
                &result.edges[0].output_partition,
            )
            .expect("encode canonical edge partition");
        assert_eq!(sink_partition, &canonical_edge_partition);
        assert_eq!(source_partition, &canonical_edge_partition);

        let target = result.native_fragments.get(&0).expect("target fragment");
        let receiver = match target
            .root
            .as_ref()
            .and_then(|root| root.payload.as_ref())
            .expect("target exchange receiver")
        {
            crate::proto::plan::distributed_node::Payload::Exchange(exchange) => exchange,
            other => panic!("expected exchange receiver, got {other:?}"),
        };
        assert_eq!(
            receiver.partition_type,
            crate::proto::plan::PartitionKind::Hash as i32
        );
        assert_eq!(receiver.partition_exprs.len(), 1);
    }

    #[test]
    fn router_edge_rebuilds_partition_and_stream_kind_from_route_ordinals() {
        let output_columns = vec![
            output_col(1, "op"),
            output_col(2, "route"),
            output_col(3, "delete_id"),
        ];
        let dp = DistributedPlan {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: physical_values_node(0, 10, output_columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: crate::sql::planner::distributed::DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
        };
        let mut branch =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
        branch.output_partition_ordinals = vec![2];
        branch.sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::unpartitioned_metadata_json(),
        );
        let dag =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(Some(0), None, vec![branch]);
        let mut planned =
            crate::sql::planner::distributed::write::plan::with_iceberg_change_stream_write(
                dp, "test_db", dag,
            )
            .expect("plan change-stream write")
            .distributed_plan;
        let target_fragment_id = planned.edges[0].target_fragment_id;
        let target_exchange_node_id = planned.edges[0].target_exchange_node_id;
        let target = planned
            .fragments
            .iter_mut()
            .find(|fragment| fragment.fragment_id == target_fragment_id)
            .expect("router target fragment");
        assert_eq!(target.root.node_id, target_exchange_node_id);
        let DistributedNodeKind::Exchange(exchange) = &mut target.root.payload else {
            panic!("router target must be Exchange");
        };
        exchange.partition = DataPartition::unpartitioned();

        let result =
            lower_distributed_plan(&planned, &EmptyCatalog, &ConnectorRegistry::new(), None)
                .expect("native fragment build");

        let edge = &result.edges[0];
        assert!(matches!(edge.output_partition.kind, PartitionKind::Hash));
        assert_eq!(edge.output_partition.exprs.len(), 1);
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &edge.output_partition.exprs[0].kind
        else {
            panic!("expected router HASH partition column ref");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(3));
        assert_eq!(column, "delete_id");
        assert_eq!(edge.stream_kind, FragmentStreamKind::Partitioned);
        let source = result
            .native_fragments
            .get(&edge.source_fragment_id)
            .expect("router source fragment");
        let route_partition = match source
            .sink
            .as_ref()
            .and_then(|sink| sink.kind.as_ref())
            .expect("router sink")
        {
            crate::proto::plan::data_sink::Kind::IcebergChangeStreamRouter(router) => router
                .branches[0]
                .output_partition
                .as_ref()
                .expect("router route partition"),
            other => panic!("expected router sink, got {other:?}"),
        };
        assert_eq!(
            route_partition.kind,
            crate::proto::plan::PartitionKind::Hash as i32
        );
        assert_eq!(route_partition.exprs.len(), 1);

        let target = result
            .native_fragments
            .get(&edge.target_fragment_id)
            .expect("router target fragment");
        let receiver = match target
            .root
            .as_ref()
            .and_then(|root| root.payload.as_ref())
            .expect("router target exchange receiver")
        {
            crate::proto::plan::distributed_node::Payload::Exchange(exchange) => exchange,
            other => panic!("expected router exchange receiver, got {other:?}"),
        };
        assert_eq!(receiver.partition_type, route_partition.kind);
        assert_eq!(receiver.partition_exprs, route_partition.exprs);
    }

    #[test]
    fn router_edges_reject_conflicting_partitions_for_the_same_receiver() {
        let output_columns = vec![
            output_col(1, "op"),
            output_col(2, "route"),
            output_col(3, "delete_id"),
        ];
        let dp = DistributedPlan {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: physical_values_node(0, 10, output_columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: crate::sql::planner::distributed::DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
        };
        let mut branch =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
        branch.output_partition_ordinals = vec![2];
        branch.sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::unpartitioned_metadata_json(),
        );
        let dag =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(Some(0), None, vec![branch]);
        let mut planned =
            crate::sql::planner::distributed::write::plan::with_iceberg_change_stream_write(
                dp, "test_db", dag,
            )
            .expect("plan change-stream write")
            .distributed_plan;

        let first_edge = planned.edges[0].clone();
        let source = planned
            .fragments
            .iter_mut()
            .find(|fragment| fragment.fragment_id == first_edge.source_fragment_id)
            .expect("router source fragment");
        let crate::sql::planner::distributed::DataSink::IcebergChangeStreamRouter(router) =
            &mut source.sink
        else {
            panic!("source must use router sink");
        };
        let mut second_route = router.branches[0].clone();
        second_route.branch_id = 99;
        second_route.output_ordinals = vec![1];
        second_route.output_partition_ordinals = vec![1];
        router.branches.push(second_route);
        let mut second_edge = first_edge;
        second_edge.edge_kind = FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id: router.group_id,
            branch_id: 99,
            branch_kind: router.branches[1].branch_kind,
        };
        planned.edges.push(second_edge);

        let err = match lower_distributed_plan(
            &planned,
            &EmptyCatalog,
            &ConnectorRegistry::new(),
            None,
        ) {
            Ok(_) => panic!("one receiver cannot have conflicting route partitions"),
            Err(err) => err,
        };
        assert!(
            err.contains("conflicting partitions for target Exchange"),
            "{err}"
        );
    }

    #[test]
    fn shared_edge_validation_rejects_source_mismatch_and_empty_hash_partition() {
        let mut source_mismatch = stream_exchange_plan(ExchangeFlavor::Distribution);
        let DistributedNodeKind::Exchange(exchange) =
            &mut source_mismatch.fragments[1].root.payload
        else {
            panic!("consumer must be an exchange");
        };
        exchange.source_fragment_id = 42;
        let err = validate_distributed_plan(&source_mismatch)
            .expect_err("edge and exchange source mismatch must fail");
        assert!(
            err.contains(
                "stream edge source_fragment_id=1 does not match Exchange source_fragment_id=42"
            ),
            "{err}"
        );

        let mut empty_hash = stream_exchange_plan(ExchangeFlavor::Distribution);
        let DistributedNodeKind::Exchange(exchange) = &mut empty_hash.fragments[1].root.payload
        else {
            panic!("consumer must be an exchange");
        };
        exchange.partition = DataPartition {
            kind: PartitionKind::Hash,
            exprs: Vec::new(),
        };
        let err = validate_distributed_plan(&empty_hash)
            .expect_err("empty HASH exchange partition must fail");
        assert!(
            err.contains("DistributedPlan HASH Exchange has no native partition expressions"),
            "{err}"
        );
    }

    #[test]
    fn lower_distributed_plan_owns_native_fragments_matching_schedules_and_root() {
        let dp = stream_exchange_plan(ExchangeFlavor::LimitOffset {
            limit: Some(1),
            offset: Some(0),
        });

        let result = lower_distributed_plan(&dp, &EmptyCatalog, &ConnectorRegistry::new(), None)
            .expect("native fragment build");
        let fragment_ids = result
            .native_fragments
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let schedule_ids = result
            .fragment_schedules
            .iter()
            .map(|schedule| schedule.fragment_id)
            .collect::<BTreeSet<_>>();

        assert_eq!(fragment_ids, schedule_ids);
        assert!(fragment_ids.contains(&result.root_fragment_id));
    }

    #[test]
    fn lower_distributed_plan_owns_native_write_sink_shape() {
        let columns = vec![output_col(1, "id")];
        let mut sink_spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        sink_spec.target_table.columns[0].data_type = DataType::Int64;
        sink_spec.target_columns[0].data_type = DataType::Int64;
        let fragment = PlanFragment {
            fragment_id: 0,
            root: physical_values_node(0, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::IcebergWrite(
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
        let plan = DistributedPlan {
            fragments: vec![fragment],
            root_fragment_id: 0,
            edges: Vec::new(),
        };

        let result = lower_distributed_plan(&plan, &EmptyCatalog, &ConnectorRegistry::new(), None)
            .expect("native write fragment build");
        assert_eq!(
            result.fragment_schedules[0].output_kind,
            FragmentOutputKind::TerminalWrite
        );
        let sink = result.native_fragments[&0]
            .sink
            .as_ref()
            .expect("native sink");
        assert!(matches!(
            sink.kind,
            Some(crate::proto::plan::data_sink::Kind::IcebergWrite(_))
        ));
    }

    #[test]
    fn native_fragment_ownership_rejects_missing_fragment_and_root() {
        let dp = stream_exchange_plan(ExchangeFlavor::LimitOffset {
            limit: Some(1),
            offset: Some(0),
        });
        let mut result =
            lower_distributed_plan(&dp, &EmptyCatalog, &ConnectorRegistry::new(), None)
                .expect("native fragment build");

        result.native_fragments.remove(&1);
        let err = validate_native_fragment_ownership(
            &result.native_fragments,
            &result.fragment_schedules,
            result.root_fragment_id,
        )
        .expect_err("missing scheduled fragment must be rejected");
        assert!(err.contains("native fragment ids"), "{err}");

        result.native_fragments.insert(
            1,
            crate::proto::plan::PlanFragment {
                fragment_id: 1,
                ..Default::default()
            },
        );
        result.native_fragments.remove(&result.root_fragment_id);
        let err = validate_native_fragment_ownership(
            &result.native_fragments,
            &result.fragment_schedules,
            result.root_fragment_id,
        )
        .expect_err("missing root fragment must be rejected");
        assert!(err.contains("root fragment"), "{err}");
    }

    #[test]
    fn lower_distributed_plan_lowers_cte_multicast_edge_output_slots_to_requested_producer_columns()
    {
        let cte_id: CteId = 7;
        let producer_columns = vec![
            output_col(1, "k"),
            output_col(2, "v"),
            output_col(3, "payload"),
        ];
        let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
        let receive_producer_column_ids =
            vec![producer_columns[0].column_id, producer_columns[2].column_id];

        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, producer_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Noop,
            output_exprs: None,
            output_columns: producer_columns,
            cte_id: Some(cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: receive_columns.clone(),
                    output_qualifier: Some("c".to_string()),
                    flavor: ExchangeFlavor::CteMulticast {
                        cte_id,
                        receive_producer_column_ids: receive_producer_column_ids.clone(),
                    },
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Result,
            output_exprs: None,
            output_columns: receive_columns,
            cte_id: None,
            cte_exchange_nodes: vec![(
                cte_id,
                exchange_node_id,
                receive_producer_column_ids.clone(),
            )],
        };
        let dp = DistributedPlan {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                },
                output_slot_ids: Vec::new(),
            }],
        };

        let result = lower_distributed_plan(&dp, &EmptyCatalog, &ConnectorRegistry::new(), None)
            .expect("native lower plan");

        assert_eq!(result.edges[0].output_slot_ids, vec![1, 3]);
        let native_consumer = result
            .native_fragments
            .get(&consumer_fragment_id)
            .expect("encoded native consumer");
        assert_eq!(native_consumer.cte_exchange_nodes[0].column_ids, vec![1, 3]);
    }
}
