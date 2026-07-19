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
use crate::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
use crate::exec::node::scan::ScanNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::novarocks_logging::warn;
use crate::protocol::starrocks::decode::layout::{
    Layout, chunk_schema_for_layout, schema_for_layout,
};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::runtime::fragment::instance::ScanAssignments;
use crate::runtime::scan_range::ScanRange;
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
    scan_assignments: Option<&ScanAssignments>,
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

pub(crate) fn supported_schema_scan_requires_ranges(node: &plan_nodes::TPlanNode) -> Option<bool> {
    let schema_scan = node.schema_scan_node.as_ref()?;
    let table = SchemaTable::from_table_name(&schema_scan.table_name)?;
    if matches!(table, SchemaTable::Be(BeSchemaTable::Unsupported(_))) {
        return None;
    }
    Some(schema_table_requires_scan_ranges(&table))
}

fn lower_supported_schema_scan_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    scan_assignments: Option<&ScanAssignments>,
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
    let _ = require_scan_ranges;
    let should_scan = schema_scan_selected(node.node_id, scan_assignments)?;
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

fn schema_scan_selected(
    node_id: i32,
    scan_assignments: Option<&ScanAssignments>,
) -> Result<bool, String> {
    let assignments = scan_assignments
        .ok_or_else(|| "SCHEMA_SCAN_NODE requires typed scan assignments".to_string())?;
    let assignment = assignments
        .get(&FragmentNodeId::new(node_id))
        .ok_or_else(|| format!("SCHEMA_SCAN_NODE node_id={node_id} missing typed assignment"))?;
    if assignment.kind() != ScanAssignmentKind::SchemaSelection {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={node_id} expected SchemaSelection assignment, got {:?}",
            assignment.kind()
        ));
    }
    let [range] = assignment.ranges() else {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={node_id} requires exactly one selection range"
        ));
    };
    let ScanRange::SchemaSelection(selection) = &range.range else {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={node_id} assignment payload is not SchemaSelection"
        ));
    };
    Ok(selection.selected)
}
