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
use crate::connector::iceberg::scan_model::IcebergDataFileInfo;
use crate::exec::node::iceberg_delta_scan::DeltaSourceRole;
use crate::query_execution::preparation::scan::{PlannedConnectorRead, ResolvedScanExecution};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

use super::projection::effective_scan_column_names;

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

/// Plans executable opaque splits through the real Iceberg connector instance.
/// Native scheduling owns only the resulting SPI identities and byte-size
/// hints; it must not lower these splits back into `FileScanRange`.
pub(super) fn plan_iceberg_connector_read(
    connectors: &ConnectorRegistry,
    context: novarocks_spi::connector::ConnectorRequestContext,
    scan: &PlanScanNode,
    execution: &ResolvedScanExecution,
) -> Result<PlannedConnectorRead, String> {
    let ResolvedScanExecution::IcebergFiles(files) = execution else {
        return Err("Iceberg connector planning requires IcebergFiles execution".to_string());
    };
    let projection = effective_scan_column_names(scan)
        .iter()
        .filter_map(|name| {
            files
                .table
                .schema
                .fields
                .iter()
                .position(|field| field.name.eq_ignore_ascii_case(name))
        })
        .collect::<Vec<_>>();
    let planned = crate::connector::iceberg::provider::plan_native_iceberg_read(
        connectors,
        context,
        &files.table,
        files.binding,
        &files.files,
        &projection,
    )?;
    Ok(PlannedConnectorRead {
        declaration: planned.declaration,
        scan: planned.scan,
        splits: planned.splits,
        batch: planned.batch,
    })
}

/// Plans the subset of an Iceberg delta scan that contains only newly added
/// data files through the ordinary provider reader.  Delete-side delta roles
/// continue through the dedicated delta reader until its provider-owned split
/// implementation is installed; this guard prevents an incomplete cutover
/// from silently dropping change rows.
pub(super) fn try_plan_iceberg_delta_connector_read(
    connectors: &ConnectorRegistry,
    context: novarocks_spi::connector::ConnectorRequestContext,
    scan: &PlanScanNode,
    execution: &ResolvedScanExecution,
) -> Result<Option<PlannedConnectorRead>, String> {
    let ResolvedScanExecution::IcebergDelta(delta) = execution else {
        return Err("Iceberg delta connector planning requires IcebergDelta execution".to_string());
    };
    let ScanSource::IcebergDeltaTable { table, .. } = &scan.table.source else {
        return Err(
            "Iceberg delta connector planning requires IcebergDeltaTable source".to_string(),
        );
    };
    if delta
        .runtime_plan
        .change_files
        .iter()
        .any(|file| !matches!(&file.role, DeltaSourceRole::DataFile))
    {
        return Ok(None);
    }
    let files = delta
        .runtime_plan
        .change_files
        .iter()
        .map(|file| IcebergDataFileInfo {
            path: file.path.clone(),
            size: file.size,
            row_count: None,
            column_stats: None,
            partition_spec_id: file.partition_spec_id,
            partition_key: file.partition_key.clone(),
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            ivm_change_op: Some(1),
            included_positions: None,
            delete_files: Vec::new(),
            manifest_path: None,
            partition_values: Vec::new(),
        })
        .collect::<Vec<_>>();
    let planned = crate::connector::iceberg::provider::plan_native_iceberg_read(
        connectors,
        context,
        table,
        crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
        &files,
        &[],
    )?;
    Ok(Some(PlannedConnectorRead {
        declaration: planned.declaration,
        scan: planned.scan,
        splits: planned.splits,
        batch: planned.batch,
    }))
}
