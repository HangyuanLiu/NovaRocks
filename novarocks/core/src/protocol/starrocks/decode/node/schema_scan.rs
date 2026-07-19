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
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::connector::schema::{BeSchemaTable, SchemaScanContext, SchemaScanOp, SchemaTable};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::node::scan::ScanNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::novarocks_logging::warn;
use crate::protocol::starrocks::decode::layout::{
    Layout, chunk_schema_for_layout, schema_for_layout,
};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::thrift::descriptors;
use crate::thrift::plan_nodes;

use super::local_rf_waiting_set;

/// Lower a SCHEMA_SCAN_NODE to an empty `ValuesNode`.
///
/// This unblocks FE internal maintenance jobs that reference information_schema
/// while we incrementally align full schema-scan semantics.
pub(crate) fn lower_schema_scan_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    scan_assignments: Option<&super::StarRocksScanRangeAssignments>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Lowered, String> {
    let schema_scan = node
        .schema_scan_node
        .as_ref()
        .ok_or_else(|| "SCHEMA_SCAN_NODE missing schema_scan_node payload".to_string())?;

    if let Some(table) = SchemaTable::from_table_name(&schema_scan.table_name) {
        return match &table {
            SchemaTable::Be(BeSchemaTable::Unsupported(name)) => {
                Err(format!("unsupported be schema table {name}"))
            }
            _ => {
                let require_scan_ranges = schema_table_requires_scan_ranges(&table);
                lower_supported_schema_scan_node(
                    node,
                    out_layout,
                    desc_tbl,
                    scan_assignments,
                    external_dependencies,
                    table,
                    require_scan_ranges,
                )
            }
        };
    }

    let chunk = if out_layout.order.is_empty() {
        Chunk::new_with_chunk_schema(
            RecordBatch::new_empty(Arc::new(Schema::empty())),
            Arc::new(ChunkSchema::empty()),
        )
    } else {
        let desc_tbl =
            desc_tbl.ok_or_else(|| "SCHEMA_SCAN_NODE requires desc_tbl for schema".to_string())?;
        let schema = schema_for_layout(desc_tbl, out_layout)?;
        let chunk_schema = chunk_schema_for_layout(desc_tbl, out_layout)?;
        Chunk::new_with_chunk_schema(RecordBatch::new_empty(schema), chunk_schema)
    };
    warn!(
        "SCHEMA_SCAN_NODE is lowered to empty values for table_name={} db={:?}",
        schema_scan.table_name, schema_scan.db
    );
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        },
        layout: out_layout.clone(),
    })
}

fn schema_table_requires_scan_ranges(table: &SchemaTable) -> bool {
    matches!(
        table,
        SchemaTable::Be(
            BeSchemaTable::TabletWriteLog
                | BeSchemaTable::Txns
                | BeSchemaTable::Compactions
                | BeSchemaTable::CloudNativeCompactions
                | BeSchemaTable::Configs
                | BeSchemaTable::DatacacheMetrics
                | BeSchemaTable::Logs
                | BeSchemaTable::Tablets
                | BeSchemaTable::Threads
                | BeSchemaTable::Bvars
        )
    )
}

fn lower_supported_schema_scan_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    scan_assignments: Option<&super::StarRocksScanRangeAssignments>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
    table: SchemaTable,
    require_scan_ranges: bool,
) -> Result<Lowered, String> {
    let schema_scan = node
        .schema_scan_node
        .as_ref()
        .ok_or_else(|| "SCHEMA_SCAN_NODE missing schema_scan_node payload".to_string())?;
    let output_chunk_schema = if out_layout.order.is_empty() {
        Arc::new(ChunkSchema::empty())
    } else {
        let desc_tbl =
            desc_tbl.ok_or_else(|| "SCHEMA_SCAN_NODE requires desc_tbl for schema".to_string())?;
        chunk_schema_for_layout(desc_tbl, out_layout)?
    };
    let context = SchemaScanContext::from_thrift(schema_scan);
    let should_scan = if require_scan_ranges {
        schema_scan_selected_for_current_fragment(node.node_id, scan_assignments)?
    } else {
        schema_scan_selected_if_present(node.node_id, scan_assignments)?
    };
    let scan = ScanNode::new(Arc::new(SchemaScanOp::new(
        table,
        context,
        output_chunk_schema.clone(),
        should_scan,
        external_dependencies
            .and_then(|draft| draft.frontend_endpoint())
            .cloned(),
    )))
    .with_node_id(node.node_id)
    .with_output_chunk_schema(output_chunk_schema)
    .with_local_rf_waiting_set(local_rf_waiting_set(node));
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout.clone(),
    })
}

fn schema_scan_selected_for_current_fragment(
    node_id: i32,
    scan_assignments: Option<&super::StarRocksScanRangeAssignments>,
) -> Result<bool, String> {
    let scan_assignments = scan_assignments.ok_or_else(|| {
        "SCHEMA_SCAN_NODE for be_* tables requires exec_params.per_node_scan_ranges".to_string()
    })?;
    let scan_ranges = scan_assignments
        .get(&node_id)
        .ok_or_else(|| format!("missing per_node_scan_ranges for node_id={node_id}"))?;
    if scan_ranges
        .iter()
        .any(|scan_range| scan_range.has_more.unwrap_or(false))
    {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node_id
        ));
    }
    if scan_ranges.is_empty() {
        return Ok(true);
    }
    Ok(scan_ranges
        .iter()
        .any(|scan_range| !scan_range.empty.unwrap_or(false)))
}

fn schema_scan_selected_if_present(
    node_id: i32,
    scan_assignments: Option<&super::StarRocksScanRangeAssignments>,
) -> Result<bool, String> {
    let Some(scan_assignments) = scan_assignments else {
        return Ok(true);
    };
    let Some(scan_ranges) = scan_assignments.get(&node_id) else {
        return Ok(true);
    };
    if scan_ranges
        .iter()
        .any(|scan_range| scan_range.has_more.unwrap_or(false))
    {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node_id
        ));
    }
    if scan_ranges.is_empty() {
        return Ok(true);
    }
    Ok(scan_ranges
        .iter()
        .any(|scan_range| !scan_range.empty.unwrap_or(false)))
}
