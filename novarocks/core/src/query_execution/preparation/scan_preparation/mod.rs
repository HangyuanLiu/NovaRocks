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

use crate::connector::ConnectorRegistry;
use crate::connector::scan_model::starrocks::PlannedNativeStarRocksScan;
use crate::query_execution::preparation::scan::{
    ResolvedIcebergFileScan, ResolvedScanBinding, ResolvedScanExecution, ScanBindingResolver,
    ScanExecutionBindings,
};
use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, DistributedPlan, FragmentId,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

mod iceberg;
mod projection;
mod pruning;

pub(crate) use iceberg::build_iceberg_metadata_scan_range_params;
use iceberg::plan_iceberg_file_ranges;
use projection::{resolve_effective_required_reads, resolve_physical_columns};

pub(super) fn prepare_scan_bindings(
    plan: &DistributedPlan,
    connectors: &ConnectorRegistry,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<ScanExecutionBindings, String> {
    let mut bindings = ScanExecutionBindings::default();
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    for fragment in plan.fragments() {
        collect_scan_bindings(
            fragment.fragment_id,
            &fragment.root,
            connectors,
            context,
            resolver,
            &mut seen_scan_node_ids,
            &mut bindings,
        )?;
    }
    Ok(bindings)
}

fn collect_scan_bindings(
    fragment_id: FragmentId,
    node: &DistributedNode,
    connectors: &ConnectorRegistry,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    resolver: Option<&dyn ScanBindingResolver>,
    seen_scan_node_ids: &mut std::collections::BTreeSet<i32>,
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    if let DistributedNodeKind::Scan(scan) = &node.payload {
        if !seen_scan_node_ids.insert(node.node_id) {
            return Err(format!("duplicate scan node_id={}", node.node_id));
        }
        prepare_scan_node(
            fragment_id,
            node.node_id,
            scan,
            connectors,
            context,
            resolver,
            bindings,
        )?;
    }
    for child in &node.children {
        if child.fragment_id == fragment_id {
            collect_scan_bindings(
                fragment_id,
                child,
                connectors,
                context,
                resolver,
                seen_scan_node_ids,
                bindings,
            )?;
        }
    }
    Ok(())
}

fn prepare_scan_node(
    fragment_id: FragmentId,
    node_id: i32,
    scan: &PlanScanNode,
    connectors: &ConnectorRegistry,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    resolver: Option<&dyn ScanBindingResolver>,
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    let execution = match &scan.table.source {
        ScanSource::IcebergDataFiles {
            table,
            files,
            cloud_properties,
            binding,
        } => ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
            table: table.clone(),
            files: files.clone(),
            cloud_properties: cloud_properties.clone(),
            binding: *binding,
        }),
        ScanSource::IcebergMetadataTable { .. } => {
            return bindings.insert_scan_ranges(
                fragment_id,
                node_id,
                vec![build_iceberg_metadata_scan_range_params()],
            );
        }
        ScanSource::StarRocks { .. } => {
            #[cfg(feature = "compat")]
            {
                let planned =
                    crate::connector::starrocks::table::scan_adapter::plan_native_starrocks_scan_with_compat(
                    node_id, scan, connectors, context.clone(),
                )?;
                return store_planned_starrocks_scan(fragment_id, node_id, planned, bindings);
            }
            #[cfg(not(feature = "compat"))]
            {
                return Err("StarRocks native scan planning requires feature compat".to_string());
            }
        }
        source if scan_source_requires_resolver(source) => {
            let source_context = scan_source_context(source);
            let resolver = resolver.ok_or_else(|| {
                format!(
                    "scan source {source_context} node_id={node_id} requires scan binding resolver"
                )
            })?;
            resolver
                .resolve_scan(node_id, scan)
                .map_err(|err| {
                    format!(
                        "scan binding resolver failed for required source {source_context} node_id={node_id}: {err}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "scan binding resolver returned no binding for required source {source_context} node_id={node_id}"
                    )
                })?
        }
        source => {
            return Err(format!(
                "scan preparation does not yet support source {source:?} for node_id={node_id}"
            ));
        }
    };
    validate_resolved_execution_kind(node_id, &scan.table.source, &execution)?;
    reject_target_equality_deletes(node_id, &scan.table.source, &execution)?;
    let physical_columns = resolve_physical_columns(node_id, scan)?;
    let (ranges, equality_required) = match &execution {
        ResolvedScanExecution::IcebergFiles(_) => {
            plan_iceberg_file_ranges(connectors, context.clone(), scan, &execution)
                .map_err(|err| format!("scan preparation node_id={node_id}: {err}"))?
        }
        ResolvedScanExecution::IcebergDelta(_) => {
            (vec![build_iceberg_metadata_scan_range_params()], Vec::new())
        }
    };
    let required_reads = resolve_effective_required_reads(node_id, scan, &equality_required)?;
    bindings.insert_binding(ResolvedScanBinding {
        node_id,
        execution,
        physical_columns,
        required_reads,
    })?;
    bindings.insert_scan_ranges(fragment_id, node_id, ranges)
}

fn store_planned_starrocks_scan(
    fragment_id: FragmentId,
    node_id: i32,
    planned: PlannedNativeStarRocksScan,
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    if bindings.scan_ranges(fragment_id, node_id).is_some()
        || bindings.starrocks_source(node_id).is_some()
    {
        return Err(format!(
            "duplicate StarRocks scan planning fragment_id={fragment_id} node_id={node_id}"
        ));
    }
    bindings.insert_starrocks_source(node_id, planned.source)?;
    bindings.insert_scan_ranges(fragment_id, node_id, planned.ranges)
}

fn validate_resolved_execution_kind(
    node_id: i32,
    source: &ScanSource,
    execution: &ResolvedScanExecution,
) -> Result<(), String> {
    let valid = match source {
        ScanSource::IcebergDeltaTable { .. } => {
            matches!(execution, ResolvedScanExecution::IcebergDelta(_))
        }
        ScanSource::IcebergDataFiles { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::IcebergMvTargetState(_)
        | ScanSource::IcebergMvTargetLocator(_) => {
            matches!(execution, ResolvedScanExecution::IcebergFiles(_))
        }
        ScanSource::StarRocks { .. } | ScanSource::IcebergMetadataTable { .. } => true,
    };
    if valid {
        return Ok(());
    }
    let required = if matches!(source, ScanSource::IcebergDeltaTable { .. }) {
        "IcebergDelta"
    } else {
        "IcebergFiles"
    };
    Err(format!(
        "scan source {} node_id={node_id} requires {required} execution",
        scan_source_kind(source)
    ))
}

fn reject_target_equality_deletes(
    node_id: i32,
    source: &ScanSource,
    execution: &ResolvedScanExecution,
) -> Result<(), String> {
    let target_kind = match source {
        ScanSource::IcebergMvTargetState(_) => "target-state",
        ScanSource::IcebergMvTargetLocator(_) => "target-locator",
        _ => return Ok(()),
    };
    let ResolvedScanExecution::IcebergFiles(files) = execution else {
        return Err(format!(
            "Iceberg {target_kind} scan node_id={node_id} requires IcebergFiles execution"
        ));
    };
    if files.files.iter().any(|file| {
        file.delete_files.iter().any(|delete| {
            delete.file_content
                == crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Equality
        })
    }) {
        return Err(format!(
            "Iceberg {target_kind} scan node_id={node_id} does not support equality deletes yet"
        ));
    }
    Ok(())
}

fn scan_source_requires_resolver(source: &ScanSource) -> bool {
    matches!(
        source,
        ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergMvTargetState(_)
            | ScanSource::IcebergMvTargetLocator(_)
            | ScanSource::IcebergDeltaTable { .. }
    )
}

fn scan_source_kind(source: &ScanSource) -> &'static str {
    match source {
        ScanSource::StarRocks { .. } => "StarRocks",
        ScanSource::IcebergDataFiles { .. } => "IcebergDataFiles",
        ScanSource::IcebergMetadataTable { .. } => "IcebergMetadataTable",
        ScanSource::IcebergDeltaTable { .. } => "IcebergDeltaTable",
        ScanSource::IcebergVersionTable { .. } => "IcebergVersionTable",
        ScanSource::IcebergMvTargetState(_) => "IcebergMvTargetState",
        ScanSource::IcebergMvTargetLocator(_) => "IcebergMvTargetLocator",
    }
}

fn scan_source_context(source: &ScanSource) -> String {
    match source {
        ScanSource::IcebergDeltaTable {
            from_snapshot_id,
            to_snapshot_id,
            ..
        } => format!(
            "IcebergDeltaTable from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}"
        ),
        _ => scan_source_kind(source).to_string(),
    }
}

#[cfg(test)]
mod tests;
