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
use crate::connector::iceberg::scan_range::IcebergScanRangeContext;
use crate::connector::scan_model::starrocks::PlannedNativeStarRocksScan;
use crate::connector::scan_planning::starrocks::plan_native_starrocks_scan;
use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext, TableHandle};
use crate::coordinator::prepare::scan::{
    ResolvedIcebergFileScan, ResolvedScanBinding, ResolvedScanExecution, ScanBindingResolver,
    ScanExecutionBindings,
};
use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, DistributedPlan, FragmentId,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

mod projection;

use projection::{
    effective_scan_column_names, resolve_effective_required_reads, resolve_physical_columns,
};

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
            let planned = plan_native_starrocks_scan(node_id, scan, connectors)?;
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
    let equality_required =
        crate::connector::iceberg::scan_range::equality_delete_required_columns(
            &files.table,
            &splits,
        )?;
    let effective_column_names =
        merge_effective_column_names(base_column_names.clone(), &equality_required);
    if effective_column_names != base_column_names {
        scan_handle = planner.begin_scan(
            iceberg_table_handle(files, effective_column_names),
            BeginScanContext::default(),
        )?;
    }
    let plan = crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges(
        &scan_handle,
        &splits,
        IcebergScanRangeContext {
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

#[cfg(test)]
mod tests;
