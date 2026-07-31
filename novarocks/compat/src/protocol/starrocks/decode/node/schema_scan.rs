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

use crate::protocol::starrocks::decode::layout::{
    Layout, chunk_schema_for_layout, schema_for_layout,
};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::thrift::descriptors;
use crate::thrift::plan_nodes;
use novarocks::connector::schema::{
    BeSchemaTable, SchemaFrontend, SchemaScanContext, SchemaScanSource, SchemaTable,
    SchemaUserIdentity, SchemaUserRoles,
};
use novarocks::exec::chunk::{Chunk, ChunkSchema};
use novarocks::exec::fragment::program::ScanAssignmentKind;
use novarocks::exec::node::scan::{BoundScanRanges, ScanNode};
use novarocks::exec::node::values::ValuesNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::novarocks_logging::warn;
use novarocks::runtime::scan_range::ScanRange;

use super::ScanRangeCarrier;

/// Lower a SCHEMA_SCAN_NODE to an empty `ValuesNode`.
///
/// This unblocks FE internal maintenance jobs that reference information_schema
/// while we incrementally align full schema-scan semantics.
pub(crate) fn lower_schema_scan_node(
    node: &plan_nodes::TPlanNode,
    out_layout: &Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    scan_ranges: Option<ScanRangeCarrier>,
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
                    scan_ranges,
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
    scan_ranges: Option<ScanRangeCarrier>,
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
    let context = decode_schema_scan_context(schema_scan);
    let _ = require_scan_ranges;
    let should_scan = schema_scan_selected(node.node_id, scan_ranges)?;
    let source = SchemaScanSource::new(
        table,
        context,
        output_chunk_schema.clone(),
        external_dependencies
            .and_then(|draft| draft.frontend_endpoint())
            .cloned(),
        external_dependencies.and_then(|draft| draft.schema_load_provider()),
    );
    // Route the enriched selection to the instance; bind at materialize time.
    if let Some(scan_ranges) = scan_ranges {
        scan_ranges.capture(
            node.node_id,
            BoundScanRanges::SchemaSelection { should_scan },
        );
    }
    let scan = ScanNode::new(Arc::new(source))
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_chunk_schema);
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout.clone(),
    })
}

fn decode_schema_scan_context(node: &plan_nodes::TSchemaScanNode) -> SchemaScanContext {
    SchemaScanContext {
        table_name: node.table_name.trim().to_ascii_lowercase(),
        db: normalize_optional_string(node.db.as_ref()),
        table: normalize_optional_string(node.table.as_ref()),
        wild: normalize_optional_string(node.wild.as_ref()),
        user: normalize_optional_string(node.user.as_ref()),
        ip: normalize_optional_string(node.ip.as_ref()),
        port: node.port.filter(|value| *value > 0),
        thread_id: node.thread_id.filter(|value| *value >= 0),
        user_ip: normalize_optional_string(node.user_ip.as_ref()),
        current_user_ident: node
            .current_user_ident
            .as_ref()
            .map(|identity| SchemaUserIdentity {
                username: identity.username.clone(),
                host: identity.host.clone(),
                is_domain: identity.is_domain,
                is_ephemeral: identity.is_ephemeral,
                current_role_ids: identity
                    .current_role_ids
                    .as_ref()
                    .map(|roles| SchemaUserRoles {
                        role_id_list: roles.role_id_list.clone(),
                    }),
            }),
        catalog_name: normalize_optional_string(node.catalog_name.as_ref()),
        table_id: node.table_id.filter(|value| *value > 0),
        partition_id: node.partition_id.filter(|value| *value > 0),
        tablet_id: node.tablet_id.filter(|value| *value > 0),
        txn_id: node.txn_id.filter(|value| *value > 0),
        job_id: node.job_id.filter(|value| *value >= 0),
        label: normalize_optional_string(node.label.as_ref()),
        type_: normalize_optional_string(node.type_.as_ref())
            .map(|value| value.to_ascii_uppercase()),
        state: normalize_optional_string(node.state.as_ref())
            .map(|value| value.to_ascii_uppercase()),
        limit: node.limit.filter(|value| *value >= 0),
        log_start_ts: node.log_start_ts.filter(|value| *value > 0),
        log_end_ts: node.log_end_ts.filter(|value| *value > 0),
        log_level: normalize_optional_string(node.log_level.as_ref())
            .map(|value| value.to_ascii_uppercase()),
        log_pattern: normalize_optional_string(node.log_pattern.as_ref()),
        log_limit: node.log_limit.filter(|value| *value > 0),
        frontends: node
            .frontends
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|frontend| SchemaFrontend {
                id: frontend.id.clone(),
                ip: frontend.ip.clone(),
                http_port: frontend.http_port,
            })
            .collect(),
    }
}

fn normalize_optional_string(value: Option<&String>) -> Option<String> {
    value
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
}

fn schema_scan_selected(
    node_id: i32,
    scan_ranges: Option<ScanRangeCarrier>,
) -> Result<bool, String> {
    let scan_ranges = scan_ranges
        .ok_or_else(|| "SCHEMA_SCAN_NODE requires typed scan assignments".to_string())?;
    let (assignment_kind, assignment_ranges) = scan_ranges
        .get(node_id)
        .ok_or_else(|| format!("SCHEMA_SCAN_NODE node_id={node_id} missing typed assignment"))?;
    if assignment_kind != ScanAssignmentKind::SchemaSelection {
        return Err(format!(
            "SCHEMA_SCAN_NODE node_id={node_id} expected SchemaSelection assignment, got {assignment_kind:?}",
        ));
    }
    let [range] = assignment_ranges else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thrift::types;

    #[test]
    fn schema_scan_wire_identity_and_frontends_decode_to_domain_context() {
        let node = plan_nodes::TSchemaScanNode::new(
            7,
            "  Fe_Metrics  ".to_string(),
            Some("  db1  ".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(types::TUserIdentity::new(
                Some("alice".to_string()),
                Some("example.com".to_string()),
                Some(true),
                Some(false),
                Some(types::TUserRoles::new(Some(vec![11, 12]))),
            )),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(vec![plan_nodes::TFrontend::new(
                Some("fe-1".to_string()),
                Some("10.0.0.1".to_string()),
                Some(8030),
            )]),
            Some("  default_catalog  ".to_string()),
        );

        let context = decode_schema_scan_context(&node);

        assert_eq!(context.table_name, "fe_metrics");
        assert_eq!(context.db.as_deref(), Some("db1"));
        assert_eq!(context.catalog_name.as_deref(), Some("default_catalog"));
        let identity = context.current_user_ident.expect("identity");
        assert_eq!(identity.username.as_deref(), Some("alice"));
        assert_eq!(identity.host.as_deref(), Some("example.com"));
        assert_eq!(identity.is_domain, Some(true));
        assert_eq!(identity.is_ephemeral, Some(false));
        assert_eq!(
            identity.current_role_ids.expect("roles").role_id_list,
            Some(vec![11, 12])
        );
        assert_eq!(context.frontends.len(), 1);
        assert_eq!(context.frontends[0].id.as_deref(), Some("fe-1"));
        assert_eq!(context.frontends[0].ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(context.frontends[0].http_port, Some(8030));
    }
}
