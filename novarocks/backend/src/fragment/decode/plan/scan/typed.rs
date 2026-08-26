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

//! Fragment-native decoding for the typed connector scan source.
//!
//! Structural validation of the carrier belongs to the protocol layer: this
//! decoder parses the wire message once through
//! [`ConnectorTableScanSource::parse`] and never re-checks what that parse
//! already proved. What is left here is the fragment-local work the protocol
//! layer cannot do: deciding which relation kinds this backend slice reads,
//! agreeing the scan's ordered assignments with the plan node's declared
//! output, resolving the installed typed provider for exactly this binding
//! generation, and assembling the execution scan node.

use std::sync::Arc;

use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::scan::BoundScanRanges;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto::connector_read::{ConnectorRelation, ConnectorRelationKind};
use novarocks_proto::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{connector_read as dto, plan};
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
};

use crate::connector::typed_registry::TypedConnectorProviderRegistry;
use crate::connector::typed_runtime::TypedConnectorScanSource;

use super::super::context::NativePlanDecodeContext;
use super::super::error::NativeFragmentLeafDecodeError;
use super::super::node::DecodedNode;
use super::common::{DecodedScanOutputColumns, lower_scan_predicate, parse_scan_limit};

/// Bytes of a connector instance incarnation, mirrored from the wire contract.
const INSTANCE_INCARNATION_BYTES: usize = 16;

/// Lower one `ScanSource.typed_connector_read` into an execution scan node.
pub(super) fn lower_typed_connector_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &dto::ConnectorTableScanSource,
    output_columns: &DecodedScanOutputColumns,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
    // One parse, and the only one: presence, bounds, known enums, uniqueness,
    // and the carrier's cross-field rules are the protocol layer's contract.
    let scan_source = novarocks_proto::connector_read::ConnectorTableScanSource::parse(
        source.clone(),
        FieldPath::root("typed_connector_read"),
    )
    .map_err(leaf_from_protocol)?;

    let table = scan_source.table();
    // Closed relation set: every kind this slice does not read is a stable
    // typed refusal naming that kind, never a `_` arm that would silently
    // accept the next relation someone adds.
    match table.relation() {
        ConnectorRelation::Table(_) => {}
        ConnectorRelation::TableFunction(_)
        | ConnectorRelation::ChangeWindow(_)
        | ConnectorRelation::SystemTable(_)
        | ConnectorRelation::TableExecute(_)
        | ConnectorRelation::MergeTable(_) => {
            return Err(unsupported_relation(table.relation_kind()));
        }
    }

    check_assignments_match_output(&scan_source, output_columns)?;

    let binding_key = binding_key(table)?;
    let inputs = typed_scan_runtime_inputs(ctx)?;
    let providers = inputs.providers.resolve(&binding_key).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "table",
            error.to_string(),
        )
        .append_field("catalog_name")
    })?;

    let layout = output_columns.layout();
    let output_schema = output_columns.output_schema();
    let predicate = lower_scan_predicate(scan, arena, &layout, ctx)?;
    let scan_source = TypedConnectorScanSource::new(
        scan_source,
        providers.page_source(),
        inputs.session,
        inputs.request,
        inputs.queues,
        node.node_id,
        output_schema.slot_ids().to_vec(),
    );

    // A typed scan carries no frozen range: its work arrives as splits on the
    // task-update queue, so the range binding is empty by construction.
    ctx.capture_scan_ranges(node.node_id, BoundScanRanges::None);
    let scan_node = novarocks_execution::exec::node::scan::ScanNode::new(Arc::new(scan_source))
        .with_node_id(node.node_id)
        .with_output_chunk_schema(Arc::clone(&output_schema))
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        // A task scan may legally start with zero splits, so an empty morsel
        // set must not be padded into a synthetic one.
        .with_accept_empty_scan_ranges(true);
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

/// The fragment-local runtime inputs a typed scan needs beyond its carrier.
struct TypedScanRuntimeInputs {
    providers: Arc<TypedConnectorProviderRegistry>,
    queues: Arc<novarocks_execution::connector::TaskAttemptSplitQueues>,
    session: novarocks_spi::connector::read_stack::ConnectorSession,
    request: novarocks_spi::connector::ConnectorRequestContext,
}

/// Resolve the typed scan's runtime inputs from the fragment decode context.
///
/// The bundle is supplied by the fragment runtime at submission time. Refusing
/// when it is absent keeps the boundary fail-closed: the alternative would be a
/// scan bound to a registry it invented, which is exactly the fallback this
/// stack removes.
fn typed_scan_runtime_inputs(
    ctx: &NativePlanDecodeContext,
) -> Result<TypedScanRuntimeInputs, NativeFragmentLeafDecodeError> {
    let runtime = ctx.typed_scan_runtime().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "typed_connector_runtime",
            "typed connector scans require the typed provider registry, the task attempt split \
             queues, and the connector session in the fragment decode context",
        )
    })?;
    let (_, query_expire) =
        novarocks_execution::runtime::query_options::query_expire_durations(ctx.query_options());
    // Budgets here bound the connector's own request accounting, not the wire:
    // the carrier was already bounded by the protocol layer before it arrived.
    let request = novarocks_spi::connector::ConnectorRequestContext::try_new(
        std::time::Instant::now() + query_expire,
        ctx.connector_cancellation()?,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "typed_connector_runtime",
            error.to_string(),
        )
    })?;
    Ok(TypedScanRuntimeInputs {
        providers: runtime.providers(),
        queues: runtime.queues(),
        session: runtime.session(),
        request,
    })
}

/// Agree the carrier's ordered assignments with the plan node's output.
///
/// The two orders are one contract: `assignments[i]` produces page channel `i`,
/// and the decoded output binds slot `i` to that channel. A disagreement would
/// silently read one column into another column's slot.
fn check_assignments_match_output(
    scan_source: &novarocks_proto::connector_read::ConnectorTableScanSource,
    output_columns: &DecodedScanOutputColumns,
) -> Result<(), NativeFragmentLeafDecodeError> {
    let assignments = scan_source.assignments();
    let columns = output_columns.columns();
    if assignments.len() != columns.len() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "assignments",
            format!(
                "typed connector scan assigns {} columns but the plan node declares {} output columns",
                assignments.len(),
                columns.len()
            ),
        ));
    }
    for (index, (assignment, column)) in assignments.iter().zip(columns).enumerate() {
        if !assignment.variable().eq_ignore_ascii_case(&column.name) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "assignments",
                format!(
                    "typed connector scan assignment `{}` does not name output column `{}`",
                    assignment.variable(),
                    column.name
                ),
            )
            .append_index(index));
        }
    }
    Ok(())
}

/// The exact connector binding generation this relation belongs to.
fn binding_key(
    table: &novarocks_proto::connector_read::CatalogTableHandle,
) -> Result<ConnectorExecutionBindingKey, NativeFragmentLeafDecodeError> {
    // The carrier already proved the catalog name is a normalized instance id
    // and the incarnation is exactly 16 bytes; both conversions below therefore
    // restate the same fact in the SPI's own types rather than re-validating.
    let instance_id =
        ConnectorInstanceId::try_from_canonical(table.catalog_name()).map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "table",
                error.to_string(),
            )
            .append_field("catalog_name")
        })?;
    let incarnation = ConnectorInstanceIncarnation::from_bytes(table.instance_incarnation());
    debug_assert_eq!(
        table.instance_incarnation().len(),
        INSTANCE_INCARNATION_BYTES
    );
    Ok(ConnectorExecutionBindingKey {
        instance_id,
        incarnation,
    })
}

/// The stable refusal for a relation kind this slice does not read yet.
fn unsupported_relation(kind: ConnectorRelationKind) -> NativeFragmentLeafDecodeError {
    let named = match kind {
        // Reached only if the supported arm above is ever narrowed; keeping it
        // in this closed match is what makes a new relation kind a compile
        // error at both sites rather than a silent acceptance here.
        ConnectorRelationKind::Table => "table",
        ConnectorRelationKind::TableFunction => "table_function",
        ConnectorRelationKind::ChangeWindow => "change_window",
        ConnectorRelationKind::SystemTable => "system_table",
        ConnectorRelationKind::TableExecute => "table_execute",
        ConnectorRelationKind::MergeTable => "merge_table",
    };
    NativeFragmentLeafDecodeError::at_field(
        ProtocolErrorKind::Unsupported,
        "table",
        format!("typed connector scan relation `{named}` is not admitted yet"),
    )
}

/// Carry a protocol refusal into the fragment decoder without rebuilding it.
///
/// The protocol error already names its own field path, and the leaf error can
/// only append static field names and indexes, so the exact path travels in the
/// detail while the kind and the carrier's own root are preserved.
fn leaf_from_protocol(error: ProtocolError) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_collection(error.kind(), error.to_string())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_proto_models::common;

    use crate::connector::typed_runtime::test_support;

    use super::super::super::node::decode_node;
    use super::*;

    /// Build a distributed scan node whose source is the typed carrier.
    fn typed_scan_node(
        source: dto::ConnectorTableScanSource,
        columns: Vec<common::OutputColumn>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: vec![plan::ColumnDef {
                            name: "id".to_string(),
                            data_type: Some(
                                crate::fragment::decode::type_decode::encode_type(&DataType::Int64)
                                    .expect("encode type"),
                            ),
                            nullable: true,
                            write_default_json: None,
                            logical_type: None,
                        }],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::TypedConnectorRead(source)),
                        }),
                    }),
                    alias: None,
                    columns,
                    predicates: Vec::new(),
                    required_columns: Vec::new(),
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn output_column(column_id: u32, name: &str) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(
                crate::fragment::decode::type_decode::encode_type(&DataType::Int64)
                    .expect("encode type"),
            ),
            nullable: true,
            is_internal: false,
        }
    }

    fn decode_error(
        source: dto::ConnectorTableScanSource,
        columns: Vec<common::OutputColumn>,
    ) -> novarocks_proto::ProtocolError {
        let node = typed_scan_node(source, columns);
        let error = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("typed connector decoding must refuse");
        error.protocol().expect("protocol error").clone()
    }

    /// Replace the carrier's relation while keeping everything else valid.
    fn scan_with_relation(
        relation: dto::catalog_table_handle::Relation,
    ) -> dto::ConnectorTableScanSource {
        let mut source = test_support::scan_source_proto();
        let table = source.table.as_mut().expect("carrier table");
        table.relation = Some(relation);
        source
    }

    #[test]
    fn typed_scan_decode_without_a_runtime_fails_closed() {
        // A decode context that cannot supply the runtime must refuse rather
        // than bind the scan to a registry it invented.
        let error = decode_error(
            test_support::scan_source_proto(),
            vec![output_column(1, "id")],
        );
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert!(
            error
                .path()
                .to_string()
                .ends_with("typed_connector_read.typed_connector_runtime"),
            "unexpected path: {}",
            error.path()
        );
    }

    #[test]
    fn typed_scan_decode_binds_a_table_relation_once_the_runtime_is_supplied() {
        let node = typed_scan_node(
            test_support::scan_source_proto(),
            vec![output_column(1, "id")],
        );
        struct NeverCancelled;
        impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
            fn is_cancelled(&self) -> bool {
                false
            }
        }
        let ctx = NativePlanDecodeContext::default()
            .with_connector_cancellation(std::sync::Arc::new(NeverCancelled))
            .with_typed_scan_runtime(Some(test_support::typed_scan_runtime()));
        decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect("a supplied runtime binds the typed scan");
    }

    #[test]
    fn typed_scan_decode_rejects_an_assignment_count_mismatch() {
        let error = decode_error(
            test_support::scan_source_proto(),
            vec![output_column(1, "id"), output_column(2, "flag")],
        );
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert!(
            error.detail().contains("assigns 1 columns"),
            "unexpected detail: {}",
            error.detail()
        );
    }

    #[test]
    fn typed_scan_decode_rejects_an_assignment_order_mismatch() {
        let error = decode_error(
            test_support::scan_source_proto(),
            vec![output_column(1, "other")],
        );
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert!(
            error.path().to_string().ends_with("assignments[0]"),
            "unexpected path: {}",
            error.path()
        );
    }

    #[test]
    fn typed_scan_decode_refuses_every_relation_kind_it_does_not_read_yet() {
        let cases = [
            (
                dto::catalog_table_handle::Relation::TableFunction(
                    dto::ConnectorTableFunctionHandle {
                        handle: Some(
                            dto::connector_table_function_handle::Handle::IcebergTableChanges(
                                dto::TableChangesFunctionHandle {
                                    schema_table_name: Some(test_support::schema_table_name()),
                                    table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                                    columns: vec![test_support::iceberg_column_handle(1)],
                                    name_mapping_json: None,
                                    start_snapshot_id: 3,
                                    end_snapshot_id: 9,
                                },
                            ),
                        ),
                    },
                ),
                "table_function",
            ),
            (
                dto::catalog_table_handle::Relation::ChangeWindow(
                    dto::ConnectorChangeWindowHandle {
                        handle: Some(dto::connector_change_window_handle::Handle::Iceberg(
                            dto::IcebergChangeWindowHandle {
                                schema_table_name: Some(test_support::schema_table_name()),
                                table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                                columns: vec![test_support::iceberg_column_handle(1)],
                                name_mapping_json: None,
                                from_snapshot_id_exclusive: 3,
                                to_snapshot_id_inclusive: 9,
                            },
                        )),
                    },
                ),
                "change_window",
            ),
            (
                dto::catalog_table_handle::Relation::SystemTable(
                    dto::ConnectorSystemTableReference {
                        reference: Some(dto::connector_system_table_reference::Reference::Iceberg(
                            dto::IcebergSystemTableReference {
                                schema_table_name: Some(test_support::schema_table_name()),
                                system_table_type: dto::IcebergSystemTableType::Files as i32,
                                metadata_file_location:
                                    "s3://bucket/warehouse/db/t/metadata/v3.json".to_owned(),
                                table_uuid: "6b1c2f0a-9d4e-4f7b-8a31-0c5d7e9f1234".to_owned(),
                                snapshot_id: Some(11),
                            },
                        )),
                    },
                ),
                "system_table",
            ),
            (
                dto::catalog_table_handle::Relation::TableExecute(
                    dto::ConnectorTableExecuteHandle {
                        handle: Some(dto::connector_table_execute_handle::Handle::Iceberg(
                            dto::IcebergTableExecuteHandle {
                                schema_table_name: Some(test_support::schema_table_name()),
                                procedure_id: dto::IcebergProcedureId::Optimize as i32,
                                table_location: "s3://bucket/warehouse/db/t".to_owned(),
                                procedure_handle: Some(
                                    dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(
                                        dto::IcebergOptimizeHandle {
                                            table_handle: Some(test_support::iceberg_table_handle()),
                                            min_file_size_bytes: 1024,
                                        },
                                    ),
                                ),
                            },
                        )),
                    },
                ),
                "table_execute",
            ),
            (
                dto::catalog_table_handle::Relation::MergeTable(dto::ConnectorMergeTableHandle {
                    handle: Some(dto::connector_merge_table_handle::Handle::Iceberg(
                        dto::IcebergMergeTableHandle {
                            table_handle: Some(test_support::iceberg_table_handle()),
                            insert_table_handle: Some(dto::IcebergInsertTableHandle {
                                schema_table_name: Some(test_support::schema_table_name()),
                                table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                                table_location: "s3://bucket/warehouse/db/t".to_owned(),
                                format_version: 2,
                                spec_id: Some(0),
                            }),
                        },
                    )),
                }),
                "merge_table",
            ),
        ];
        for (relation, expected) in cases {
            let error = decode_error(scan_with_relation(relation), vec![output_column(1, "id")]);
            assert_eq!(error.kind(), ProtocolErrorKind::Unsupported);
            assert!(
                error.detail().contains(expected),
                "expected `{expected}` in: {}",
                error.detail()
            );
            assert!(
                error
                    .path()
                    .to_string()
                    .ends_with("typed_connector_read.table"),
                "unexpected path: {}",
                error.path()
            );
        }
    }

    #[test]
    fn typed_scan_decode_reports_a_structurally_invalid_carrier_as_a_protocol_refusal() {
        let mut source = test_support::scan_source_proto();
        source.max_batch_rows = 0;
        let error = decode_error(source, vec![output_column(1, "id")]);
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert!(
            error.detail().contains("max_batch_rows"),
            "unexpected detail: {}",
            error.detail()
        );
    }
}
