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
use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext, TableHandle};
use crate::coordinator::prepare::scan::{ResolvedIcebergFileScan, ResolvedScanExecution};
use crate::sql::planner::payload::PlanScanNode;

use super::projection::effective_scan_column_names;
use super::pruning::native_scan_min_max_predicates;

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
        candidate_node: None,
        included_positions: Vec::new(),
        serialized_split: Some(String::new()),
        use_iceberg_jni_metadata_reader: true,
        ivm_change_op: None,
        file_pruning_min_max_values: None,
    })
}

pub(super) fn plan_iceberg_file_ranges(
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
