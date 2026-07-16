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
use crate::connector::iceberg::scan_model::IcebergDataFileBinding;
use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext, TableHandle};
use crate::coordinator::prepare::scan::{
    ResolvedIcebergFileScan, ResolvedReadColumn, ResolvedReadReason, ResolvedScanBinding,
    ResolvedScanColumn, ResolvedScanColumnKind, ResolvedScanExecution, ScanBindingResolver,
    ScanExecutionBindings,
};
use crate::sql::codegen::scan::connector::{
    ConnectorScanContext, PlannedNativeStarRocksScan, plan_native_starrocks_scan_node,
    to_native_file_scan,
};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, DistributedPlan, FragmentId,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

pub(super) fn prepare_scan_bindings(
    plan: &DistributedPlan,
    connectors: &ConnectorRegistry,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<ScanExecutionBindings, String> {
    let mut bindings = ScanExecutionBindings::default();
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    for fragment in plan.fragments() {
        collect_scan_bindings(
            fragment.fragment_id,
            &fragment.root,
            connectors,
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
            let planned = plan_native_starrocks_scan_node(node_id, scan, connectors)?;
            return store_planned_starrocks_scan(fragment_id, node_id, planned, bindings);
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
            plan_iceberg_file_ranges(connectors, scan, &execution)
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

pub(crate) fn build_iceberg_metadata_scan_range_params()
-> crate::runtime::scan_range::ScanRangeParams {
    use crate::runtime::scan_range::{FileFormat, FileScanRange, ScanRangeParams};

    ScanRangeParams::file(FileScanRange {
        file_format: FileFormat::Parquet,
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

fn resolve_physical_columns(
    node_id: i32,
    scan: &PlanScanNode,
) -> Result<Vec<ResolvedScanColumn>, String> {
    if let Some(projected_names) = refresh_scan_projected_names(&scan.table.source) {
        return projected_names
            .into_iter()
            .map(|name| {
                let planner = scan
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| {
                        format!(
                            "scan binding node_id={node_id} cannot resolve projected planner column '{name}' in table '{}'",
                            scan.table.name
                        )
                    })?;
                let (source, kind) = resolved_source_column(scan, &name).ok_or_else(|| {
                    format!(
                        "scan binding node_id={node_id} cannot resolve projected physical column '{name}' in table '{}'",
                        scan.table.name
                    )
                })?;
                Ok(ResolvedScanColumn {
                    planner: planner.clone(),
                    source: source.clone(),
                    kind,
                })
            })
            .collect();
    }

    let keep_only_resolved = matches!(scan.table.source, ScanSource::IcebergVersionTable { .. });
    scan.columns
        .iter()
        .filter(|planner| !is_variant_synthetic_column(scan, planner.column_id))
        .filter_map(|planner| {
            let Some((source, kind)) = resolved_source_column(scan, &planner.name) else {
                return if keep_only_resolved {
                    None
                } else {
                    Some(Err(format!(
                        "scan binding node_id={node_id} cannot resolve planner physical column '{}' in table '{}'",
                        planner.name, scan.table.name
                    )))
                };
            };
            Some(Ok(ResolvedScanColumn {
                planner: planner.clone(),
                source: source.clone(),
                kind,
            }))
        })
        .collect()
}

fn refresh_scan_projected_names(source: &ScanSource) -> Option<Vec<String>> {
    match source {
        ScanSource::IcebergMvTargetState(scan) => Some(projected_target_state_column_names(scan)),
        ScanSource::IcebergMvTargetLocator(scan) => {
            Some(projected_target_locator_column_names(scan))
        }
        _ => None,
    }
}

pub(crate) fn projected_target_state_column_names(
    scan: &crate::sql::planner::table::IcebergMvTargetStateScan,
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
    if let crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
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

pub(crate) fn projected_target_locator_column_names(
    scan: &crate::sql::planner::table::IcebergMvTargetLocatorScan,
) -> Vec<String> {
    let mut names = vec![scan.apply_key_column.clone()];
    if let Some(branch_id_column) = &scan.branch_id_column {
        push_unique_projected_name(&mut names, branch_id_column);
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

fn resolve_effective_required_reads(
    node_id: i32,
    scan: &PlanScanNode,
    equality_required: &[String],
) -> Result<Vec<ResolvedReadColumn>, String> {
    let required_names = match refresh_scan_projected_names(&scan.table.source) {
        Some(projected) => {
            merge_required_columns_with_projected(scan.required_columns.clone(), &projected)
        }
        None => scan.required_columns.clone().unwrap_or_else(|| {
            scan.columns
                .iter()
                .filter(|column| {
                    !matches!(scan.table.source, ScanSource::IcebergVersionTable { .. })
                        || resolved_source_column(scan, &column.name).is_some()
                })
                .map(|column| column.name.clone())
                .collect()
        }),
    }
    .into_iter()
    .filter(|name| !is_variant_synthetic_name(scan, name))
    .collect::<Vec<_>>();
    let mut reads = required_names
        .into_iter()
        .map(|name| {
            let (source, _) = resolved_source_column(scan, &name).ok_or_else(|| {
                format!(
                    "scan binding node_id={node_id} cannot resolve required physical column '{name}' in table '{}'",
                    scan.table.name
                )
            })?;
            let planner = scan
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&name))
                .ok_or_else(|| {
                    format!(
                        "scan binding node_id={node_id} required physical column '{name}' has no planner ColumnId"
                    )
                })?;
            Ok(ResolvedReadColumn {
                planner_column_id: Some(planner.column_id),
                source: source.clone(),
                reason: ResolvedReadReason::PlannerRequiredOrOutput,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    for name in equality_required {
        if reads
            .iter()
            .any(|read| read.source.name.eq_ignore_ascii_case(name))
        {
            continue;
        }
        let (source, _) = resolved_source_column(scan, name).ok_or_else(|| {
            format!(
                "scan binding node_id={node_id} cannot resolve equality-delete physical column '{name}' in table '{}'",
                scan.table.name
            )
        })?;
        if let Some(planner) = scan
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
        {
            reads.push(ResolvedReadColumn {
                planner_column_id: Some(planner.column_id),
                source: source.clone(),
                reason: ResolvedReadReason::PlannerRequiredOrOutput,
            });
        } else {
            reads.push(ResolvedReadColumn {
                planner_column_id: None,
                source: source.clone(),
                reason: ResolvedReadReason::EqualityDeleteKey,
            });
        }
    }
    Ok(reads)
}

pub(crate) fn merge_required_columns_with_projected(
    existing: Option<Vec<String>>,
    projected_names: &[String],
) -> Vec<String> {
    use std::collections::BTreeSet;

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

fn resolved_source_column<'a>(
    scan: &'a PlanScanNode,
    name: &str,
) -> Option<(
    &'a crate::catalog::schema::ColumnDef,
    ResolvedScanColumnKind,
)> {
    if let Some(column) = scan
        .table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
    {
        return Some((column, ResolvedScanColumnKind::PhysicalTableColumn));
    }
    scan.table
        .iceberg_row_lineage_metadata_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .map(|column| (column, ResolvedScanColumnKind::IcebergMetadataColumn))
}

fn plan_iceberg_file_ranges(
    connectors: &ConnectorRegistry,
    scan: &PlanScanNode,
    execution: &ResolvedScanExecution,
) -> Result<
    (
        Vec<crate::runtime::scan_range::ScanRangeParams>,
        Vec<String>,
    ),
    String,
> {
    let ResolvedScanExecution::IcebergFiles(files) = execution else {
        return Err("Iceberg file range planning requires IcebergFiles execution".to_string());
    };
    let base_column_names = effective_scan_column_names(scan);
    let table_handle = iceberg_table_handle(files, base_column_names.clone());
    let planner = connectors.scan_planner("iceberg")?;
    let mut scan_handle = planner.begin_scan(table_handle, BeginScanContext::default())?;
    let splits = planner.plan_splits(&scan_handle, SplitPlanningContext::default())?;
    let equality_required = equality_delete_required_columns(&files.table, &splits)?;
    let effective_column_names =
        merge_effective_column_names(base_column_names.clone(), &equality_required);
    if effective_column_names != base_column_names {
        scan_handle = planner.begin_scan(
            iceberg_table_handle(files, effective_column_names),
            BeginScanContext::default(),
        )?;
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
    Ok((plan.scan_ranges, equality_required))
}

fn merge_effective_column_names(existing: Vec<String>, additional: &[String]) -> Vec<String> {
    use std::collections::BTreeSet;

    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for name in existing.into_iter().chain(additional.iter().cloned()) {
        if seen.insert(name.to_ascii_lowercase()) {
            out.push(name);
        }
    }
    out
}

pub(crate) fn equality_delete_required_columns(
    table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
    splits: &[crate::connector::scan_planning::Split],
) -> Result<Vec<String>, String> {
    use std::collections::{BTreeMap, BTreeSet};

    let mut schema_by_id = BTreeMap::new();
    let mut schema_by_name = BTreeMap::new();
    for field in &table.schema.fields {
        if schema_by_id
            .insert(field.field_id, field.name.clone())
            .is_some()
        {
            return Err(format!(
                "Iceberg ScanNode table schema has duplicate field id {} for table {}",
                field.field_id, table.table
            ));
        }
        let normalized = field.name.to_ascii_lowercase();
        if schema_by_name
            .insert(normalized, field.name.clone())
            .is_some()
        {
            return Err(format!(
                "Iceberg ScanNode table schema has duplicate field name {} for table {}",
                field.name, table.table
            ));
        }
    }

    let mut required = Vec::new();
    let mut required_seen = BTreeSet::new();
    for split in splits {
        let file = crate::connector::iceberg::scan_planner::iceberg_split(split)?;
        for delete in &file.data_file.delete_files {
            if delete.file_content
                != crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Equality
            {
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

pub(crate) fn native_scan_min_max_predicates(
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

fn iceberg_table_handle(files: &ResolvedIcebergFileScan, column_names: Vec<String>) -> TableHandle {
    match files.binding {
        IcebergDataFileBinding::ExplicitFiles => {
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                &files.table.catalog,
                &files.table.namespace,
                &files.table.table,
                files.table.current_snapshot_id,
                files.table.clone(),
                files.files.clone(),
                column_names,
            )
        }
        IcebergDataFileBinding::CurrentSnapshot => {
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
                &files.table.catalog,
                &files.table.namespace,
                &files.table.table,
                files.table.clone(),
                column_names,
            )
        }
    }
}

pub(crate) fn effective_scan_column_names(scan: &PlanScanNode) -> Vec<String> {
    if let Some(projected) = refresh_scan_projected_names(&scan.table.source) {
        return merge_required_columns_with_projected(scan.required_columns.clone(), &projected);
    }
    let mut names = scan.required_columns.clone().unwrap_or_else(|| {
        scan.table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    });
    names.retain(|name| !is_variant_synthetic_name(scan, name));
    for variant in &scan.variant_columns {
        push_unique_projected_name(&mut names, &variant.source_column);
    }
    names
}

fn is_variant_synthetic_column(scan: &PlanScanNode, column_id: ColumnId) -> bool {
    scan.variant_columns
        .iter()
        .any(|variant| variant.synthetic_column_id == column_id)
}

fn is_variant_synthetic_name(scan: &PlanScanNode, name: &str) -> bool {
    scan.variant_columns.iter().any(|variant| {
        variant.synthetic_column.eq_ignore_ascii_case(name)
            || scan.columns.iter().any(|column| {
                column.column_id == variant.synthetic_column_id
                    && column.name.eq_ignore_ascii_case(name)
            })
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::DataType;

    use super::{collect_scan_bindings, prepare_scan_bindings, store_planned_starrocks_scan};
    use crate::catalog::schema::ColumnDef;
    use crate::connector::ConnectorRegistry;
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef,
        IcebergTableInfo,
    };
    use crate::connector::scan_planning::{
        BeginScanContext, ConnectorScanPlanner, ScanHandle, Split, SplitPlanningContext,
        TableHandle,
    };
    use crate::coordinator::prepare::scan::{
        ResolvedIcebergDeltaScan, ResolvedIcebergFileScan, ResolvedReadReason,
        ResolvedScanExecution, ScanBindingResolver,
    };
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan,
        PlanFragment,
    };
    use crate::sql::planner::payload::PlanScanNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use crate::sql::planner::table::{ScanSource, TableDef};

    #[derive(Debug)]
    struct PlannedIcebergFiles {
        files: Vec<IcebergDataFileInfo>,
        seen_column_names: Option<Arc<Mutex<Vec<Vec<String>>>>>,
    }

    impl ConnectorScanPlanner for PlannedIcebergFiles {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn begin_scan(
            &self,
            table: TableHandle,
            _ctx: BeginScanContext,
        ) -> Result<ScanHandle, String> {
            let table = table
                .downcast_ref::<crate::connector::iceberg::scan_planner::IcebergTableHandle>()
                .ok_or_else(|| "expected IcebergTableHandle".to_string())?
                .clone();
            if let Some(seen) = &self.seen_column_names {
                seen.lock()
                    .expect("seen column names lock")
                    .push(table.column_names.clone());
            }
            Ok(ScanHandle::new(
                "iceberg",
                crate::connector::iceberg::scan_planner::IcebergScanHandle { table },
            ))
        }

        fn plan_splits(
            &self,
            _scan: &ScanHandle,
            _ctx: SplitPlanningContext,
        ) -> Result<Vec<Split>, String> {
            Ok(self
                .files
                .iter()
                .cloned()
                .map(|data_file| {
                    Split::new(
                        "iceberg",
                        crate::connector::iceberg::scan_planner::IcebergSplit { data_file },
                    )
                })
                .collect())
        }
    }

    struct RejectResolver;

    impl ScanBindingResolver for RejectResolver {
        fn resolve_scan(
            &self,
            node_id: i32,
            _scan: &PlanScanNode,
        ) -> Result<Option<ResolvedScanExecution>, String> {
            panic!("ordinary Iceberg scan unexpectedly invoked resolver for node {node_id}")
        }
    }

    struct StaticResolver {
        execution: ResolvedScanExecution,
    }

    impl ScanBindingResolver for StaticResolver {
        fn resolve_scan(
            &self,
            _node_id: i32,
            _scan: &PlanScanNode,
        ) -> Result<Option<ResolvedScanExecution>, String> {
            Ok(Some(self.execution.clone()))
        }
    }

    struct ErrorResolver;

    impl ScanBindingResolver for ErrorResolver {
        fn resolve_scan(
            &self,
            _node_id: i32,
            _scan: &PlanScanNode,
        ) -> Result<Option<ResolvedScanExecution>, String> {
            Err("boom".to_string())
        }
    }

    struct EmptyResolver;

    impl ScanBindingResolver for EmptyResolver {
        fn resolve_scan(
            &self,
            _node_id: i32,
            _scan: &PlanScanNode,
        ) -> Result<Option<ResolvedScanExecution>, String> {
            Ok(None)
        }
    }

    fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn source_column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_table() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "s3://bucket/test_table".to_string(),
            schema: IcebergSchemaDef {
                fields: vec![
                    IcebergSchemaFieldDef {
                        field_id: 1,
                        name: "id".to_string(),
                        initial_default: None,
                        write_default: None,
                        initial_default_json: None,
                        write_default_json: None,
                        children: Vec::new(),
                    },
                    IcebergSchemaFieldDef {
                        field_id: 3,
                        name: "category".to_string(),
                        initial_default: None,
                        write_default: None,
                        initial_default_json: None,
                        write_default_json: None,
                        children: Vec::new(),
                    },
                ],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn data_file(path: &str) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: Vec::new(),
            manifest_path: None,
            partition_values: Vec::new(),
        }
    }

    fn data_file_with_i32_stats(path: &str, min: i32, max: i32) -> IcebergDataFileInfo {
        let mut file = data_file(path);
        file.column_stats = Some(HashMap::from([(
            "id".to_string(),
            crate::connector::iceberg::scan_model::IcebergColumnStats {
                null_count: Some(0),
                value_count: Some(10),
                column_size: None,
                lower_bound: Some(min.to_le_bytes().to_vec()),
                upper_bound: Some(max.to_le_bytes().to_vec()),
            },
        )]));
        file
    }

    fn equality_delete_file(
        equality_column_names: Vec<&str>,
        equality_field_ids: Vec<i32>,
    ) -> crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
        crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
            path: "s3://bucket/eq-delete.parquet".to_string(),
            file_format: crate::connector::iceberg::scan_model::IcebergDeleteFileFormat::Parquet,
            file_content: crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Equality,
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

    fn id_eq(value: i64) -> crate::sql::analysis::TypedExpr {
        use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};

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
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(value)),
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn scan_node(node_id: i32, binding: IcebergDataFileBinding) -> DistributedNode {
        let output = column(1, "id", DataType::Int32, false);
        let table = TableDef {
            name: "ice_t".to_string(),
            columns: vec![source_column("id", DataType::Int32, false)],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::IcebergDataFiles {
                table: iceberg_table(),
                files: vec![data_file("s3://bucket/explicit.parquet")],
                cloud_properties: BTreeMap::new(),
                binding,
            },
        };
        DistributedNode {
            node_id,
            fragment_id: 0,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: PhysicalPlanStats {
                output_row_count: 10.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table,
                alias: None,
                columns: vec![output],
                predicates: Vec::new(),
                required_columns: Some(vec!["id".to_string()]),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
        }
    }

    fn plan(root: DistributedNode) -> DistributedPlan {
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root,
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: vec![column(1, "id", DataType::Int32, false)],
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        }
    }

    fn registry(files: Vec<IcebergDataFileInfo>) -> ConnectorRegistry {
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_planner(Arc::new(PlannedIcebergFiles {
            files,
            seen_column_names: None,
        }));
        registry
    }

    fn recording_registry(
        files: Vec<IcebergDataFileInfo>,
    ) -> (ConnectorRegistry, Arc<Mutex<Vec<Vec<String>>>>) {
        let seen_column_names = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_planner(Arc::new(PlannedIcebergFiles {
            files,
            seen_column_names: Some(Arc::clone(&seen_column_names)),
        }));
        (registry, seen_column_names)
    }

    fn resolved_files(files: Vec<IcebergDataFileInfo>) -> ResolvedScanExecution {
        ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
            table: iceberg_table(),
            files,
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::ExplicitFiles,
        })
    }

    fn resolved_delta() -> ResolvedScanExecution {
        ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
            runtime_plan: crate::sql::codegen::scan::iceberg_delta::IcebergDeltaScanRuntimePlan {
                table_location: "s3://bucket/test_table".to_string(),
                data_columns: Vec::new(),
                cloud_properties: BTreeMap::new(),
                change_files: Vec::new(),
                delete_side: None,
            },
        })
    }

    fn replace_scan_source(root: &mut DistributedNode, source: ScanSource) {
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table.source = source;
    }

    #[test]
    fn ordinary_current_snapshot_is_immutable_and_does_not_invoke_resolver() {
        let plan = plan(scan_node(10, IcebergDataFileBinding::CurrentSnapshot));
        let before = format!("{plan:#?}");
        let bindings = prepare_scan_bindings(
            &plan,
            &registry(vec![data_file("s3://bucket/current.parquet")]),
            Some(&RejectResolver),
        )
        .expect("prepare current-snapshot scan");

        assert_eq!(format!("{plan:#?}"), before);
        assert!(bindings.binding(10).is_some());
        assert_eq!(bindings.scan_ranges(0, 10).expect("ranges").len(), 1);
    }

    #[test]
    fn explicit_files_preserve_native_split_ranges() {
        let plan = plan(scan_node(10, IcebergDataFileBinding::ExplicitFiles));
        let bindings = prepare_scan_bindings(
            &plan,
            &registry(vec![data_file("s3://bucket/explicit.parquet")]),
            None,
        )
        .expect("prepare explicit scan");
        let ranges = bindings.scan_ranges(0, 10).expect("ranges");

        assert_eq!(ranges.len(), 1);
        let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
            panic!("expected file range");
        };
        assert_eq!(
            file.full_path.as_deref(),
            Some("s3://bucket/explicit.parquet")
        );
        assert_eq!(file.offset, 0);
        assert_eq!(file.length, 128);
    }

    #[test]
    fn duplicate_scan_node_defense_reports_exact_error() {
        let root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
        let registry = registry(vec![data_file("s3://bucket/explicit.parquet")]);
        let mut seen_scan_node_ids = std::collections::BTreeSet::new();
        let mut bindings = crate::coordinator::prepare::scan::ScanExecutionBindings::default();

        collect_scan_bindings(
            0,
            &root,
            &registry,
            None,
            &mut seen_scan_node_ids,
            &mut bindings,
        )
        .expect("first scan preparation");
        let err = collect_scan_bindings(
            0,
            &root,
            &registry,
            None,
            &mut seen_scan_node_ids,
            &mut bindings,
        )
        .expect_err("duplicate scan node must fail before re-planning");

        assert_eq!(err, "duplicate scan node_id=10");
    }

    #[test]
    fn metadata_scan_uses_native_sentinel_range() {
        let mut root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
        replace_scan_source(
            &mut root,
            ScanSource::IcebergMetadataTable {
                table: iceberg_table(),
                metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType::Snapshots,
                serialized_table: "{}".to_string(),
                cloud_properties: BTreeMap::new(),
                metadata_payload: None,
            },
        );

        let bindings = prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), None)
            .expect("prepare metadata scan");
        let ranges = bindings.scan_ranges(0, 10).expect("metadata ranges");

        assert_eq!(ranges.len(), 1);
        let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
            panic!("expected metadata file range");
        };
        assert_eq!(file.full_path.as_deref(), Some("iceberg-metadata"));
        assert!(file.use_iceberg_jni_metadata_reader);
        assert!(bindings.binding(10).is_none());
    }

    #[test]
    fn ordinary_iceberg_scan_preserves_min_max_pruning() {
        let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.predicates = vec![id_eq(12)];
        let bindings = prepare_scan_bindings(
            &plan(root),
            &registry(vec![
                data_file_with_i32_stats("s3://bucket/id-1-5.parquet", 1, 5),
                data_file_with_i32_stats("s3://bucket/id-10-20.parquet", 10, 20),
            ]),
            None,
        )
        .expect("prepare pruned scan");
        let ranges = bindings.scan_ranges(0, 10).expect("ranges");

        assert_eq!(ranges.len(), 1);
        let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
            panic!("expected file range");
        };
        assert_eq!(
            file.full_path.as_deref(),
            Some("s3://bucket/id-10-20.parquet")
        );
    }

    #[test]
    fn refresh_only_sources_require_resolver_with_kind_and_node_id() {
        for (source, expected_kind) in [
            (
                ScanSource::IcebergVersionTable {
                    table: iceberg_table(),
                    snapshot_id: 6,
                },
                "IcebergVersionTable",
            ),
            (
                ScanSource::IcebergDeltaTable {
                    table: iceberg_table(),
                    from_snapshot_id: 6,
                    to_snapshot_id: 7,
                },
                "IcebergDeltaTable",
            ),
        ] {
            let mut root = scan_node(37, IcebergDataFileBinding::ExplicitFiles);
            replace_scan_source(&mut root, source);

            let err = match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), None) {
                Ok(_) => panic!("{expected_kind} without resolver must fail"),
                Err(err) => err,
            };

            assert!(err.contains("requires scan binding resolver"), "{err}");
            assert!(err.contains(expected_kind), "{err}");
            assert!(err.contains("node_id=37"), "{err}");
        }
    }

    #[test]
    fn resolver_error_reports_source_kind_node_id_and_cause() {
        let mut root = scan_node(47, IcebergDataFileBinding::ExplicitFiles);
        replace_scan_source(
            &mut root,
            ScanSource::IcebergVersionTable {
                table: iceberg_table(),
                snapshot_id: 6,
            },
        );

        let err = match prepare_scan_bindings(
            &plan(root),
            &ConnectorRegistry::new(),
            Some(&ErrorResolver),
        ) {
            Ok(_) => panic!("resolver error must fail preparation"),
            Err(err) => err,
        };

        assert_eq!(
            err,
            "scan binding resolver failed for required source IcebergVersionTable node_id=47: boom"
        );
    }

    #[test]
    fn resolver_ok_none_reports_exact_required_source_error() {
        let mut root = scan_node(48, IcebergDataFileBinding::ExplicitFiles);
        replace_scan_source(
            &mut root,
            ScanSource::IcebergVersionTable {
                table: iceberg_table(),
                snapshot_id: 6,
            },
        );

        let err = match prepare_scan_bindings(
            &plan(root),
            &ConnectorRegistry::new(),
            Some(&EmptyResolver),
        ) {
            Ok(_) => panic!("empty resolver result must fail preparation"),
            Err(err) => err,
        };

        assert_eq!(
            err,
            "scan binding resolver returned no binding for required source IcebergVersionTable node_id=48"
        );
    }

    #[test]
    fn resolver_failure_precedes_invalid_physical_projection() {
        let mut root = scan_node(49, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.columns[0].name = "missing".to_string();
        scan.table.source = ScanSource::IcebergVersionTable {
            table: iceberg_table(),
            snapshot_id: 6,
        };

        let err = match prepare_scan_bindings(
            &plan(root),
            &ConnectorRegistry::new(),
            Some(&ErrorResolver),
        ) {
            Ok(_) => panic!("resolver error must win over physical projection error"),
            Err(err) => err,
        };

        assert_eq!(
            err,
            "scan binding resolver failed for required source IcebergVersionTable node_id=49: boom"
        );
    }

    #[test]
    fn version_scan_without_required_columns_reads_only_mappable_outputs_immutably() {
        let mut root = scan_node(37, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.required_columns = None;
        scan.columns
            .push(column(99, "stale_planner_only", DataType::Utf8, true));
        scan.table.source = ScanSource::IcebergVersionTable {
            table: iceberg_table(),
            snapshot_id: 6,
        };
        let plan = plan(root);
        let before = format!("{plan:#?}");
        let resolver = StaticResolver {
            execution: resolved_files(vec![data_file("s3://bucket/version-6.parquet")]),
        };

        let bindings = prepare_scan_bindings(
            &plan,
            &registry(vec![data_file("s3://bucket/version-6.parquet")]),
            Some(&resolver),
        )
        .expect("prepare version scan");
        let binding = bindings.binding(37).expect("version binding");

        assert_eq!(binding.physical_columns.len(), 1);
        assert_eq!(
            binding.physical_columns[0].planner.column_id,
            ColumnId::new_for_test(1)
        );
        assert_eq!(binding.physical_columns[0].source.name, "id");
        assert_eq!(binding.required_reads.len(), 1);
        assert_eq!(binding.required_reads[0].source.name, "id");
        assert_eq!(
            binding.required_reads[0].planner_column_id,
            Some(ColumnId::new_for_test(1))
        );
        assert_eq!(format!("{plan:#?}"), before);
    }

    #[test]
    fn projected_required_column_merge_preserves_unicode_case_deduplication() {
        let merged = super::merge_required_columns_with_projected(
            Some(vec!["äpfel".to_string()]),
            &["ÄPFEL".to_string()],
        );

        assert_eq!(merged, vec!["ÄPFEL"]);
    }

    #[test]
    fn target_locator_projection_preserves_planner_ids_and_metadata_contract() {
        use crate::exec::row_position::{
            ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
            ICEBERG_ROW_POS_COL,
        };

        let mut root = scan_node(37, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table
            .columns
            .push(source_column("extra", DataType::Utf8, true));
        scan.table.iceberg_row_lineage_metadata_columns = vec![
            source_column(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            source_column(ICEBERG_ROW_POS_COL, DataType::Int64, false),
            source_column(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            source_column(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ];
        scan.columns = vec![
            column(1, "id", DataType::Int32, false),
            column(2, "extra", DataType::Utf8, true),
            column(11, ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            column(12, ICEBERG_ROW_POS_COL, DataType::Int64, false),
            column(13, ICEBERG_ROW_ID_COL, DataType::Int64, false),
            column(14, ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ];
        scan.table.source = ScanSource::IcebergMvTargetLocator(
            crate::sql::planner::table::IcebergMvTargetLocatorScan {
                catalog: "test_catalog".to_string(),
                database: "test_db".to_string(),
                table: "test_table".to_string(),
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                apply_key_column: "id".to_string(),
                branch_id_column: None,
            },
        );
        let resolver = StaticResolver {
            execution: resolved_files(vec![data_file("s3://bucket/target-6.parquet")]),
        };

        let (registry, seen_column_names) =
            recording_registry(vec![data_file("s3://bucket/target-6.parquet")]);
        let bindings = prepare_scan_bindings(&plan(root), &registry, Some(&resolver))
            .expect("prepare target locator scan");
        let binding = bindings.binding(37).expect("binding");
        let physical = &binding.physical_columns;

        assert_eq!(
            physical
                .iter()
                .map(|column| (column.source.name.as_str(), column.planner.column_id))
                .collect::<Vec<_>>(),
            vec![
                ("id", ColumnId::new_for_test(1)),
                (ICEBERG_FILE_PATH_COL, ColumnId::new_for_test(11)),
                (ICEBERG_ROW_POS_COL, ColumnId::new_for_test(12)),
                (ICEBERG_ROW_ID_COL, ColumnId::new_for_test(13)),
                (ICEBERG_LAST_UPDATED_SEQ_COL, ColumnId::new_for_test(14)),
            ]
        );
        assert_eq!(
            binding
                .required_reads
                .iter()
                .map(|read| read.source.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "id",
                ICEBERG_FILE_PATH_COL,
                ICEBERG_ROW_POS_COL,
                ICEBERG_ROW_ID_COL,
                ICEBERG_LAST_UPDATED_SEQ_COL,
            ]
        );
        assert_eq!(
            seen_column_names
                .lock()
                .expect("seen column names lock")
                .last()
                .cloned(),
            Some(vec![
                "id".to_string(),
                ICEBERG_FILE_PATH_COL.to_string(),
                ICEBERG_ROW_POS_COL.to_string(),
                ICEBERG_ROW_ID_COL.to_string(),
                ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
            ])
        );
    }

    #[test]
    fn target_state_projection_keeps_declared_columns_and_row_lineage_ids() {
        use crate::exec::row_position::{
            ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
            ICEBERG_ROW_POS_COL,
        };

        let mut root = scan_node(38, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table.columns.extend([
            source_column("agg", DataType::Binary, true),
            source_column("extra", DataType::Utf8, true),
        ]);
        scan.table.iceberg_row_lineage_metadata_columns = vec![
            source_column(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            source_column(ICEBERG_ROW_POS_COL, DataType::Int64, false),
            source_column(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            source_column(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ];
        scan.columns = vec![
            column(1, "id", DataType::Int32, false),
            column(3, "agg", DataType::Binary, true),
            column(4, "extra", DataType::Utf8, true),
            column(11, ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            column(12, ICEBERG_ROW_POS_COL, DataType::Int64, false),
            column(13, ICEBERG_ROW_ID_COL, DataType::Int64, false),
            column(14, ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ];
        scan.table.source =
            ScanSource::IcebergMvTargetState(crate::sql::planner::table::IcebergMvTargetStateScan {
                catalog: "test_catalog".to_string(),
                database: "test_db".to_string(),
                table: "test_table".to_string(),
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                aggregate_state_layout_version: 1,
                columns: scan.table.columns.clone(),
                group_key_names: vec!["id".to_string()],
                aggregate_state_names: vec!["agg".to_string()],
                physical_column_names: vec!["id".to_string(), "agg".to_string()],
                row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                row_filter: crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                    row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                    branch_scope: None,
                },
                partition_constraint:
                    crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
            });
        let resolver = StaticResolver {
            execution: resolved_files(vec![data_file("s3://bucket/target-state-6.parquet")]),
        };

        let bindings = prepare_scan_bindings(
            &plan(root),
            &registry(vec![data_file("s3://bucket/target-state-6.parquet")]),
            Some(&resolver),
        )
        .expect("prepare target-state scan");
        let physical = &bindings.binding(38).expect("binding").physical_columns;

        assert_eq!(
            physical
                .iter()
                .map(|column| (column.source.name.as_str(), column.planner.column_id))
                .collect::<Vec<_>>(),
            vec![
                (ICEBERG_ROW_ID_COL, ColumnId::new_for_test(13)),
                ("id", ColumnId::new_for_test(1)),
                ("agg", ColumnId::new_for_test(3)),
                (ICEBERG_FILE_PATH_COL, ColumnId::new_for_test(11)),
                (ICEBERG_ROW_POS_COL, ColumnId::new_for_test(12)),
                (ICEBERG_LAST_UPDATED_SEQ_COL, ColumnId::new_for_test(14)),
            ]
        );
    }

    #[test]
    fn hidden_equality_key_is_sidecar_read_without_plan_mutation() {
        let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table
            .columns
            .push(source_column("category", DataType::Utf8, true));
        let plan = plan(root);
        let before = format!("{plan:#?}");
        let mut file = data_file("s3://bucket/data.parquet");
        file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];

        let (registry, seen_column_names) = recording_registry(vec![file]);
        let bindings =
            prepare_scan_bindings(&plan, &registry, None).expect("prepare equality-delete scan");
        let binding = bindings.binding(10).expect("binding");

        assert_eq!(format!("{plan:#?}"), before);
        assert_eq!(binding.required_reads.len(), 2);
        assert_eq!(
            binding.required_reads[0].planner_column_id,
            Some(ColumnId::new_for_test(1))
        );
        assert_eq!(
            binding.required_reads[0].reason,
            ResolvedReadReason::PlannerRequiredOrOutput
        );
        assert_eq!(binding.required_reads[1].source.name, "category");
        assert_eq!(binding.required_reads[1].planner_column_id, None);
        assert_eq!(
            binding.required_reads[1].reason,
            ResolvedReadReason::EqualityDeleteKey
        );
        let ranges = bindings.scan_ranges(0, 10).expect("ranges");
        let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
            panic!("expected file range");
        };
        assert_eq!(file.delete_files.len(), 1);
        assert_eq!(
            file.delete_files[0].file_content,
            crate::runtime::scan_range::IcebergFileContent::EqualityDeletes
        );
        assert_eq!(
            seen_column_names
                .lock()
                .expect("seen column names lock")
                .last()
                .cloned(),
            Some(vec!["id".to_string(), "category".to_string()])
        );
    }

    #[test]
    fn variant_synthetic_output_is_not_prepared_as_a_physical_column() {
        let mut root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table.columns = vec![source_column("v", DataType::LargeBinary, false)];
        let ScanSource::IcebergDataFiles { table, .. } = &mut scan.table.source else {
            panic!("expected Iceberg data-file source");
        };
        table.schema.fields = vec![IcebergSchemaFieldDef {
            field_id: 101,
            name: "v".to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        }];
        scan.columns = vec![
            column(1, "v", DataType::LargeBinary, false),
            OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "__nr_var_v_0".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: true,
            },
        ];
        scan.required_columns = Some(vec!["__nr_var_v_0".to_string()]);
        scan.variant_columns = vec![crate::sql::common::ScanVariantColumn {
            source_column_id: ColumnId::new_for_test(1),
            source_column: "v".to_string(),
            synthetic_column_id: ColumnId::new_for_test(2),
            synthetic_column: "__nr_var_v_0".to_string(),
            canonical_path: "$.a.b".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        }];

        let (registry, seen_column_names) =
            recording_registry(vec![data_file("s3://bucket/variant.parquet")]);
        let bindings = prepare_scan_bindings(&plan(root), &registry, None)
            .expect("prepare bound VARIANT scan");
        let binding = bindings.binding(10).expect("binding");
        assert_eq!(binding.physical_columns.len(), 1);
        assert_eq!(binding.physical_columns[0].source.name, "v");
        assert!(binding.required_reads.is_empty());
        assert_eq!(
            seen_column_names
                .lock()
                .expect("seen column names lock")
                .last()
                .cloned(),
            Some(vec!["v".to_string()])
        );
    }

    #[test]
    fn equality_key_already_in_planner_output_keeps_column_id() {
        let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table
            .columns
            .push(source_column("category", DataType::Utf8, true));
        scan.columns
            .push(column(3, "category", DataType::Utf8, true));
        let mut file = data_file("s3://bucket/data.parquet");
        file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];

        let bindings = prepare_scan_bindings(&plan(root), &registry(vec![file]), None)
            .expect("prepare equality-delete output scan");
        let category = bindings
            .binding(10)
            .expect("binding")
            .required_reads
            .iter()
            .find(|read| read.source.name == "category")
            .expect("category read");

        assert_eq!(category.planner_column_id, Some(ColumnId::new_for_test(3)));
        assert_eq!(category.reason, ResolvedReadReason::PlannerRequiredOrOutput);
    }

    #[test]
    fn target_state_and_locator_reject_equality_deletes() {
        use crate::exec::row_position::{
            ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
            ICEBERG_ROW_POS_COL,
        };

        let sources = [
            (
                ScanSource::IcebergMvTargetLocator(
                    crate::sql::planner::table::IcebergMvTargetLocatorScan {
                        catalog: "test_catalog".to_string(),
                        database: "test_db".to_string(),
                        table: "test_table".to_string(),
                        target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                        target_snapshot_id: Some(6),
                        apply_key_column: "id".to_string(),
                        branch_id_column: None,
                    },
                ),
                "target-locator",
            ),
            (
                ScanSource::IcebergMvTargetState(crate::sql::planner::table::IcebergMvTargetStateScan {
                    catalog: "test_catalog".to_string(),
                    database: "test_db".to_string(),
                    table: "test_table".to_string(),
                    target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                    target_snapshot_id: Some(6),
                    aggregate_state_layout_version: 1,
                    columns: vec![source_column("id", DataType::Int32, false)],
                    group_key_names: vec!["id".to_string()],
                    aggregate_state_names: Vec::new(),
                    physical_column_names: vec!["id".to_string()],
                    row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                    row_filter:
                        crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                            row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                            branch_scope: None,
                        },
                    partition_constraint:
                        crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
                }),
                "target-state",
            ),
        ];

        for (source, expected_kind) in sources {
            let mut root = scan_node(39, IcebergDataFileBinding::ExplicitFiles);
            let DistributedNodeKind::Scan(scan) = &mut root.payload else {
                panic!("test root must be a scan");
            };
            scan.table
                .columns
                .push(source_column("category", DataType::Utf8, true));
            scan.table.iceberg_row_lineage_metadata_columns = vec![
                source_column(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
                source_column(ICEBERG_ROW_POS_COL, DataType::Int64, false),
                source_column(ICEBERG_ROW_ID_COL, DataType::Int64, false),
                source_column(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
            ];
            scan.columns.extend([
                column(11, ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
                column(12, ICEBERG_ROW_POS_COL, DataType::Int64, false),
                column(13, ICEBERG_ROW_ID_COL, DataType::Int64, false),
                column(14, ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
            ]);
            scan.table.source = source;
            let mut file = data_file("s3://bucket/target-data.parquet");
            file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];
            let resolver = StaticResolver {
                execution: resolved_files(vec![file.clone()]),
            };

            let err =
                match prepare_scan_bindings(&plan(root), &registry(vec![file]), Some(&resolver)) {
                    Ok(_) => panic!("{expected_kind} equality-delete scan must fail"),
                    Err(err) => err,
                };

            assert!(err.contains(expected_kind), "{err}");
            assert!(err.contains("does not support equality deletes"), "{err}");
        }
    }

    #[test]
    fn delta_scan_uses_resolved_payload_and_sentinel_range() {
        let mut root = scan_node(40, IcebergDataFileBinding::ExplicitFiles);
        replace_scan_source(
            &mut root,
            ScanSource::IcebergDeltaTable {
                table: iceberg_table(),
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
        );
        let resolver = StaticResolver {
            execution: resolved_delta(),
        };

        let bindings =
            prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&resolver))
                .expect("prepare delta scan");

        assert!(matches!(
            bindings.binding(40).expect("binding").execution,
            ResolvedScanExecution::IcebergDelta(_)
        ));
        let ranges = bindings.scan_ranges(0, 40).expect("delta ranges");
        assert_eq!(ranges.len(), 1);
        let crate::runtime::scan_range::ScanRange::File(file) = &ranges[0].range else {
            panic!("expected delta sentinel range");
        };
        assert_eq!(file.full_path.as_deref(), Some("iceberg-metadata"));
        assert!(file.use_iceberg_jni_metadata_reader);
    }

    #[test]
    fn resolver_execution_kind_must_match_semantic_source() {
        let mut version = scan_node(41, IcebergDataFileBinding::ExplicitFiles);
        replace_scan_source(
            &mut version,
            ScanSource::IcebergVersionTable {
                table: iceberg_table(),
                snapshot_id: 6,
            },
        );
        let resolver = StaticResolver {
            execution: resolved_delta(),
        };

        let err =
            match prepare_scan_bindings(&plan(version), &ConnectorRegistry::new(), Some(&resolver))
            {
                Ok(_) => panic!("version scan must reject delta execution"),
                Err(err) => err,
            };

        assert!(err.contains("IcebergVersionTable"), "{err}");
        assert!(err.contains("requires IcebergFiles execution"), "{err}");
        assert!(err.contains("node_id=41"), "{err}");
    }

    #[test]
    fn starrocks_planning_result_stores_ranges_and_source_descriptor() {
        use crate::sql::codegen::scan::connector::{
            PlannedNativeStarRocksScan, StarRocksScanSourceDescriptor,
            StarRocksStorageColumnDescriptor, test_starrocks_tablet_schema_descriptor,
        };

        let storage_columns = vec![StarRocksStorageColumnDescriptor {
            name: "id".to_string(),
            unique_id: 1,
            default_value: None,
        }];
        let planned = PlannedNativeStarRocksScan {
            ranges: vec![
                crate::runtime::scan_range::ScanRangeParams::starrocks_tablet(300, 100, 7)
                    .expect("tablet range"),
            ],
            source: StarRocksScanSourceDescriptor {
                catalog_name: "default_catalog".to_string(),
                db_id: 10,
                table_id: 20,
                schema_id: 30,
                storage_columns: storage_columns.clone(),
                tablet_schema: test_starrocks_tablet_schema_descriptor(30, &storage_columns),
            },
        };
        let mut bindings = crate::coordinator::prepare::scan::ScanExecutionBindings::default();

        store_planned_starrocks_scan(0, 42, planned, &mut bindings)
            .expect("store StarRocks planning result");

        let ranges = bindings.scan_ranges(0, 42).expect("ranges");
        assert_eq!(ranges.len(), 1);
        let crate::runtime::scan_range::ScanRange::StarRocksTablet(range) = &ranges[0].range else {
            panic!("expected tablet range");
        };
        assert_eq!(range.tablet_id, 300);
        let source = bindings.starrocks_source(42).expect("source descriptor");
        assert_eq!(source.db_id, 10);
        assert_eq!(source.table_id, 20);
        assert_eq!(source.schema_id, 30);
        assert!(bindings.binding(42).is_none());

        let duplicate = PlannedNativeStarRocksScan {
            ranges: Vec::new(),
            source: StarRocksScanSourceDescriptor {
                catalog_name: "other_catalog".to_string(),
                db_id: 11,
                table_id: 21,
                schema_id: 31,
                storage_columns: Vec::new(),
                tablet_schema: test_starrocks_tablet_schema_descriptor(31, &[]),
            },
        };
        let err = store_planned_starrocks_scan(0, 42, duplicate, &mut bindings)
            .expect_err("duplicate StarRocks planning must fail before partial insertion");
        assert_eq!(
            err,
            "duplicate StarRocks scan planning fragment_id=0 node_id=42"
        );
    }

    #[test]
    fn physical_projection_missing_type_and_nullability_mismatches_fail_fast() {
        let mut missing = scan_node(43, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut missing.payload else {
            panic!("test root must be a scan");
        };
        scan.columns[0].name = "missing".to_string();

        let mut type_mismatch = scan_node(44, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut type_mismatch.payload else {
            panic!("test root must be a scan");
        };
        scan.columns[0].data_type = DataType::Int64;

        let mut nullability_mismatch = scan_node(45, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut nullability_mismatch.payload else {
            panic!("test root must be a scan");
        };
        scan.columns[0].nullable = true;

        for (root, expected) in [
            (missing, "cannot resolve planner physical column 'missing'"),
            (type_mismatch, "type mismatch"),
            (nullability_mismatch, "nullability mismatch"),
        ] {
            let err = match prepare_scan_bindings(
                &plan(root),
                &registry(vec![data_file("s3://bucket/data.parquet")]),
                None,
            ) {
                Ok(_) => panic!("physical projection mismatch must fail: {expected}"),
                Err(err) => err,
            };
            assert!(err.contains(expected), "{err}");
            assert!(err.contains("node_id="), "{err}");
        }
    }

    #[test]
    fn invalid_equality_identity_fails_fast_with_scan_node_context() {
        for (delete, expected) in [
            (
                equality_delete_file(Vec::new(), vec![99]),
                "unknown field id 99",
            ),
            (
                equality_delete_file(Vec::new(), vec![3, 3]),
                "duplicate equality field id 3",
            ),
            (
                equality_delete_file(vec!["category", "CATEGORY"], Vec::new()),
                "duplicate equality column name",
            ),
            (
                equality_delete_file(vec!["missing"], Vec::new()),
                "unknown equality column missing",
            ),
            (
                equality_delete_file(vec!["id"], vec![3]),
                "field id/name mismatch",
            ),
        ] {
            let mut root = scan_node(46, IcebergDataFileBinding::CurrentSnapshot);
            let DistributedNodeKind::Scan(scan) = &mut root.payload else {
                panic!("test root must be a scan");
            };
            scan.table
                .columns
                .push(source_column("category", DataType::Utf8, true));
            let mut file = data_file("s3://bucket/data.parquet");
            file.delete_files = vec![delete];

            let err = match prepare_scan_bindings(&plan(root), &registry(vec![file]), None) {
                Ok(_) => panic!("invalid equality identity must fail: {expected}"),
                Err(err) => err,
            };

            assert!(err.contains(expected), "{err}");
            assert!(err.contains("node_id=46"), "{err}");
        }
    }
}
