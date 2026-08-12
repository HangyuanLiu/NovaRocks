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

use std::collections::{HashMap, HashSet};

use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;

use super::super::expr::encode_sort_items;
use super::output::{encode_output_column, encode_output_columns};
use super::type_mapping::{encode_edge_partition_type, encode_sql_type};
use super::{NativePlanEncodeContext, encode_exprs, optional_context_ref};
use crate::protocol::native::type_mapping::encode_type;
use crate::query_execution::preparation::scan::{
    ResolvedScanBinding, ResolvedScanColumnKind, ResolvedScanExecution,
};
use crate::sql::plan_read::table as table_model;
use crate::sql::plan_read::{
    ColumnId, ExchangeFlavor, ExchangeReceiver, OutputColumn as AnalysisOutputColumn, PlanScanNode,
    ScanVariantColumn,
};
use novarocks_protocol::{common, plan};

pub(super) fn encode_scan_node(
    src: &PlanScanNode,
    node_id: i32,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanNode, String> {
    let binding = scan_binding_for_source(node_id, &src.table.source, ctx)?;
    let columns = match binding {
        Some(binding) => encode_bound_scan_output_columns(src, binding)?,
        None => encode_output_columns(&src.columns)?,
    };
    let required_columns = binding.map_or_else(
        || src.required_columns.clone().unwrap_or_default(),
        |binding| encode_bound_required_columns(src, binding),
    );
    // Connector planning may remove only predicates explicitly negotiated as
    // Exact. All other scan sources, and PruningOnly/Unsupported connector
    // predicates, retain the original Core residuals.
    let residual_predicates = optional_context_ref(ctx.scan_bindings)
        .and_then(|bindings| bindings.connector_read_for_node(node_id))
        .map(|planned| planned.residual_predicates.as_slice())
        .unwrap_or(&src.predicates);
    Ok(plan::ScanNode {
        database: src.database.clone(),
        table: Some(encode_table_def_with_context(
            &src.table,
            Some(node_id),
            Some(&src.columns),
            Some(&columns),
            Some(&required_columns),
            Some(&src.variant_columns),
            binding,
            ctx,
        )?),
        alias: src.alias.clone(),
        columns,
        predicates: encode_exprs(residual_predicates)?,
        required_columns,
        dict_columns: Vec::new(),
        variant_columns: src
            .variant_columns
            .iter()
            .map(|column| {
                Ok(plan::ScanVariantColumn {
                    source_column_id: column.source_column_id.0,
                    source_column: column.source_column.clone(),
                    synthetic_column_id: column.synthetic_column_id.0,
                    synthetic_column: column.synthetic_column.clone(),
                    canonical_path: column.canonical_path.clone(),
                    requested_type: Some(encode_type(&column.requested_type)?),
                    strict: column.strict,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        mv_rewritten_from: src.mv_rewritten_from.clone(),
    })
}

fn encode_bound_scan_output_columns(
    src: &PlanScanNode,
    binding: &ResolvedScanBinding,
) -> Result<Vec<common::OutputColumn>, String> {
    let physical_by_planner_id = binding
        .physical_columns
        .iter()
        .map(|column| (column.planner.column_id, column))
        .collect::<HashMap<_, _>>();
    let synthetic_ids = src
        .variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect::<HashSet<_>>();
    let mut encoded = Vec::with_capacity(src.columns.len());
    let mut seen_physical_ids = HashSet::new();
    for column in &src.columns {
        if let Some(bound) = physical_by_planner_id.get(&column.column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
            seen_physical_ids.insert(column.column_id);
        } else if synthetic_ids.contains(&column.column_id) {
            encoded.push(encode_output_column(column)?);
        }
    }
    for bound in &binding.physical_columns {
        if seen_physical_ids.insert(bound.planner.column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
        }
    }
    Ok(encoded)
}

fn encode_bound_required_columns(src: &PlanScanNode, binding: &ResolvedScanBinding) -> Vec<String> {
    let mut required = binding
        .required_reads
        .iter()
        .map(|read| read.source.name.clone())
        .collect::<Vec<_>>();
    for variant in &src.variant_columns {
        let required_by_planner = src.required_columns.as_ref().is_none_or(|columns| {
            columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        });
        if required_by_planner
            && !required
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        {
            required.push(variant.synthetic_column.clone());
        }
    }
    required
}

fn encode_bound_scan_output_column(
    column: &crate::query_execution::preparation::scan::ResolvedScanColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: column.planner.column_id.0,
        name: column.source.name.clone(),
        r#type: Some(encode_type(&column.source.data_type)?),
        nullable: column.source.nullable,
        is_internal: column.planner.is_internal,
    })
}

/// Encode an exchange receiver. `output_columns` is the receiver's finalized
/// wire schema: for a stream-edge target it is the planner's reconciled edge
/// projection (kept equal to what the sender sends); otherwise it is the
/// receiver's own declared columns.
pub(super) fn encode_exchange_receiver(
    src: &ExchangeReceiver,
    output_columns: &[AnalysisOutputColumn],
) -> Result<plan::ExchangeReceiver, String> {
    Ok(plan::ExchangeReceiver {
        partition_type: encode_edge_partition_type(&src.partition),
        partition_exprs: encode_exprs(&src.partition.exprs)?,
        source_fragment_id: src.source_fragment_id,
        output_columns: encode_output_columns(output_columns)?,
        output_qualifier: src.output_qualifier.clone(),
        flavor: Some(encode_exchange_flavor(&src.flavor)?),
    })
}

fn encode_exchange_flavor(src: &ExchangeFlavor) -> Result<plan::ExchangeFlavor, String> {
    use plan::exchange_flavor::Kind;

    Ok(plan::ExchangeFlavor {
        kind: Some(match src {
            ExchangeFlavor::Distribution => Kind::Distribution(true),
            ExchangeFlavor::LimitOffset { limit, offset } => {
                Kind::LimitOffset(plan::LimitOffsetFlavor {
                    limit: *limit,
                    offset: *offset,
                })
            }
            ExchangeFlavor::TopNSplit {
                items,
                limit,
                offset,
            } => Kind::TopnSplit(plan::TopNSplitFlavor {
                items: encode_sort_items(items)?,
                limit: *limit,
                offset: *offset,
            }),
            ExchangeFlavor::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } => Kind::CteMulticast(plan::CteMulticastFlavor {
                cte_id: *cte_id,
                receive_producer_column_ids: receive_producer_column_ids
                    .iter()
                    .map(|id| id.0)
                    .collect(),
            }),
        }),
    })
}

pub(super) fn encode_table_def_with_context(
    src: &table_model::TableDef,
    scan_node_id: Option<i32>,
    scan_columns: Option<&[AnalysisOutputColumn]>,
    scan_output_columns: Option<&[common::OutputColumn]>,
    scan_required_columns: Option<&[String]>,
    scan_variant_columns: Option<&[ScanVariantColumn]>,
    binding: Option<&ResolvedScanBinding>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::TableDef, String> {
    let (columns, metadata_columns) = match binding {
        Some(binding) if scan_source_requires_resolved_binding(&src.source) => {
            resolved_binding_table_columns(binding)
        }
        Some(binding) => merged_bound_table_columns(src, scan_columns.unwrap_or_default(), binding),
        None => (
            src.columns.clone(),
            src.iceberg_row_lineage_metadata_columns.clone(),
        ),
    };
    Ok(plan::TableDef {
        name: src.name.clone(),
        columns: columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        iceberg_row_lineage_metadata_columns: metadata_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        source: Some(encode_scan_source(
            &src.source,
            scan_node_id,
            scan_columns,
            scan_output_columns,
            scan_required_columns,
            scan_variant_columns.unwrap_or_default(),
            binding,
            ctx,
        )?),
    })
}

fn scan_source_requires_resolved_binding(source: &table_model::ScanSource) -> bool {
    matches!(source, table_model::ScanSource::Sql(_))
}

fn resolved_binding_table_columns(
    binding: &ResolvedScanBinding,
) -> (
    Vec<novarocks_catalog::schema::ColumnDef>,
    Vec<novarocks_catalog::schema::ColumnDef>,
) {
    let mut columns = Vec::new();
    let mut metadata_columns = Vec::new();
    let mut seen = HashSet::new();

    for bound in &binding.physical_columns {
        if !seen.insert(bound.source.name.to_ascii_lowercase()) {
            continue;
        }
        match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => columns.push(bound.source.clone()),
            ResolvedScanColumnKind::IcebergMetadataColumn => {
                metadata_columns.push(bound.source.clone())
            }
        }
    }
    for read in &binding.required_reads {
        if seen.insert(read.source.name.to_ascii_lowercase()) {
            columns.push(read.source.clone());
        }
    }

    (columns, metadata_columns)
}

fn merged_bound_table_columns(
    src: &table_model::TableDef,
    scan_columns: &[AnalysisOutputColumn],
    binding: &ResolvedScanBinding,
) -> (
    Vec<novarocks_catalog::schema::ColumnDef>,
    Vec<novarocks_catalog::schema::ColumnDef>,
) {
    let mut columns = src.columns.clone();
    let mut metadata_columns = src.iceberg_row_lineage_metadata_columns.clone();
    for bound in &binding.physical_columns {
        let target = match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => &mut columns,
            ResolvedScanColumnKind::IcebergMetadataColumn => &mut metadata_columns,
        };
        let planner_source_name = scan_columns
            .iter()
            .find(|column| column.column_id == bound.planner.column_id)
            .map(|column| column.name.as_str());
        overlay_bound_column(
            target,
            &bound.planner.name,
            planner_source_name,
            &bound.source,
        );
    }
    for read in &binding.required_reads {
        if replace_column_by_name(&mut columns, &read.source)
            || replace_column_by_name(&mut metadata_columns, &read.source)
        {
            continue;
        }
        columns.push(read.source.clone());
    }
    (columns, metadata_columns)
}

fn overlay_bound_column(
    columns: &mut Vec<novarocks_catalog::schema::ColumnDef>,
    planner_name: &str,
    planner_source_name: Option<&str>,
    source: &novarocks_catalog::schema::ColumnDef,
) {
    if let Some(index) = columns.iter().position(|column| {
        column.name.eq_ignore_ascii_case(planner_name)
            || planner_source_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name))
            || column.name.eq_ignore_ascii_case(&source.name)
    }) {
        columns[index] = source.clone();
    } else {
        columns.push(source.clone());
    }
}

fn replace_column_by_name(
    columns: &mut [novarocks_catalog::schema::ColumnDef],
    source: &novarocks_catalog::schema::ColumnDef,
) -> bool {
    let Some(column) = columns
        .iter_mut()
        .find(|column| column.name.eq_ignore_ascii_case(&source.name))
    else {
        return false;
    };
    *column = source.clone();
    true
}

pub(super) fn encode_column_def(
    src: &novarocks_catalog::schema::ColumnDef,
) -> Result<plan::ColumnDef, String> {
    Ok(plan::ColumnDef {
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        // Deprecated wire field: no decoder consumes `write_default_json`, so
        // the native encoder never fills it. Write defaults reach execution
        // through the connector-owned provider schema instead.
        write_default_json: None,
        logical_type: src.logical_type.as_ref().map(encode_sql_type).transpose()?,
    })
}

fn scan_binding_for_source<'a>(
    node_id: i32,
    source: &table_model::ScanSource,
    ctx: &'a NativePlanEncodeContext<'_>,
) -> Result<Option<&'a ResolvedScanBinding>, String> {
    let binding =
        optional_context_ref(ctx.scan_bindings).and_then(|bindings| bindings.binding(node_id));
    let required = scan_source_requires_resolved_binding(source);
    if required && binding.is_none() {
        return Err(match source {
            table_model::ScanSource::Sql(table_model::SqlScanSource {
                kind:
                    table_model::SqlScanKind::Delta {
                        from_snapshot_id,
                        to_snapshot_id,
                        ..
                    },
                ..
            }) => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={} from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}",
                scan_source_kind(source)
            ),
            _ => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={}",
                scan_source_kind(source)
            ),
        });
    }
    let Some(binding) = binding else {
        return Ok(None);
    };
    if binding.node_id != node_id {
        return Err(format!(
            "native scan encoder binding node mismatch: requested node_id={node_id}, binding node_id={}",
            binding.node_id
        ));
    }
    let valid_execution = match source {
        table_model::ScanSource::Sql(source) => match source.kind {
            table_model::SqlScanKind::ConnectorRead => {
                matches!(binding.execution, ResolvedScanExecution::ConnectorRead)
            }
            table_model::SqlScanKind::Delta { .. } => {
                matches!(
                    binding.execution,
                    ResolvedScanExecution::SealedConnectorScan(_)
                )
            }
            table_model::SqlScanKind::Data { .. }
            | table_model::SqlScanKind::FrozenInputSet { .. }
            | table_model::SqlScanKind::MvTargetState { .. }
            | table_model::SqlScanKind::MvTargetLocator { .. } => {
                matches!(
                    binding.execution,
                    ResolvedScanExecution::AdmittedConnectorRead(_)
                )
            }
            table_model::SqlScanKind::Metadata { .. } => {
                matches!(
                    binding.execution,
                    ResolvedScanExecution::AdmittedConnectorRead(_)
                )
            }
        },
    };
    if !valid_execution {
        return Err(format!(
            "native scan encoder execution variant mismatch for node_id={node_id} source={}: binding={}",
            scan_source_kind(source),
            resolved_execution_kind(&binding.execution)
        ));
    }
    Ok(Some(binding))
}

fn scan_source_kind(source: &table_model::ScanSource) -> &'static str {
    match source {
        table_model::ScanSource::Sql(source) => match source.kind {
            table_model::SqlScanKind::ConnectorRead => "SqlConnectorRead",
            table_model::SqlScanKind::Data { .. } => "SqlData",
            table_model::SqlScanKind::FrozenInputSet { .. } => "SqlFrozenInputSet",
            table_model::SqlScanKind::Metadata { .. } => "SqlMetadata",
            table_model::SqlScanKind::Delta { .. } => "SqlDelta",
            table_model::SqlScanKind::MvTargetState { .. } => "SqlMvTargetState",
            table_model::SqlScanKind::MvTargetLocator { .. } => "SqlMvTargetLocator",
        },
    }
}

fn resolved_execution_kind(execution: &ResolvedScanExecution) -> &'static str {
    match execution {
        ResolvedScanExecution::ConnectorRead => "ConnectorRead",
        ResolvedScanExecution::AdmittedConnectorRead(_) => "AdmittedConnectorRead",
        ResolvedScanExecution::SealedConnectorScan(_) => "SealedConnectorScan",
    }
}

fn encode_scan_source(
    src: &table_model::ScanSource,
    scan_node_id: Option<i32>,
    scan_analysis_columns: Option<&[AnalysisOutputColumn]>,
    scan_output_columns: Option<&[common::OutputColumn]>,
    scan_required_columns: Option<&[String]>,
    scan_variant_columns: &[ScanVariantColumn],
    binding: Option<&ResolvedScanBinding>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanSource, String> {
    use plan::scan_source::Kind;

    if let Some(planned) = scan_node_id.and_then(|node_id| {
        optional_context_ref(ctx.scan_bindings)
            .and_then(|bindings| bindings.connector_read_for_node(node_id))
    }) {
        return Ok(plan::ScanSource {
            kind: Some(Kind::ConnectorRead(plan::ConnectorReadSource {
                instance_id: planned
                    .declaration
                    .descriptor()
                    .instance_id
                    .as_str()
                    .to_string(),
                instance_incarnation: planned.declaration.incarnation().to_bytes().to_vec(),
                scan_payload: planned.scan.handle().payload().to_vec(),
                splits: Vec::new(),
                max_batch_rows: u64::try_from(planned.batch.max_rows.get())
                    .map_err(|_| "connector batch row budget does not fit u64".to_string())?,
                max_batch_bytes: u64::try_from(planned.batch.max_bytes.get())
                    .map_err(|_| "connector batch byte budget does not fit u64".to_string())?,
                max_handle_payload_bytes: u64::try_from(
                    novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                )
                .map_err(|_| "connector handle payload budget does not fit u64".to_string())?,
                max_total_payload_bytes: u64::try_from(
                    novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
                )
                .map_err(|_| "connector total payload budget does not fit u64".to_string())?,
                expected_schema_ipc: encode_connector_expected_schema_ipc(
                    scan_output_columns.unwrap_or_default(),
                    scan_analysis_columns.unwrap_or_default(),
                    scan_required_columns.unwrap_or_default(),
                    scan_variant_columns,
                    binding,
                    Some(planned.scan.output_schema()),
                )?,
            })),
        });
    }

    let source_kind = scan_source_kind(src);
    Err(format!(
        "native SQL scan node_id={} source={} must be materialized as ConnectorReadSource before encoding",
        scan_node_id
            .map(|node_id| node_id.to_string())
            .unwrap_or_else(|| "<none>".to_string()),
        source_kind,
    ))
}

fn encode_connector_expected_schema_ipc(
    output_columns: &[common::OutputColumn],
    analysis_columns: &[AnalysisOutputColumn],
    required_columns: &[String],
    variant_columns: &[ScanVariantColumn],
    binding: Option<&ResolvedScanBinding>,
    provider_schema: Option<&arrow::datatypes::SchemaRef>,
) -> Result<Vec<u8>, String> {
    let required = (!required_columns.is_empty()).then(|| {
        required_columns
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<_>>()
    });
    let synthetic_ids = variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect::<HashSet<_>>();
    let required_variant_source_ids = variant_columns
        .iter()
        .filter(|column| {
            required.as_ref().is_none_or(|required| {
                required.contains(&column.synthetic_column.to_ascii_lowercase())
            })
        })
        .map(|column| column.source_column_id)
        .collect::<HashSet<_>>();
    let selected = output_columns
        .iter()
        .filter(|column| {
            !synthetic_ids.contains(&ColumnId(column.column_id))
                && (required
                    .as_ref()
                    .is_none_or(|required| required.contains(&column.name.to_ascii_lowercase()))
                    || required_variant_source_ids.contains(&ColumnId(column.column_id)))
        })
        .map(|column| {
            let domain_column = binding
                .and_then(|binding| {
                    binding
                        .physical_columns
                        .iter()
                        .find(|bound| bound.planner.column_id.0 == column.column_id)
                        .map(|bound| (&bound.source.data_type, bound.source.nullable))
                })
                .or_else(|| {
                    analysis_columns
                        .iter()
                        .find(|candidate| candidate.column_id.0 == column.column_id)
                        .map(|candidate| (&candidate.data_type, candidate.nullable))
                })
                .ok_or_else(|| {
                    format!(
                        "ConnectorReadSource output column {} is missing its domain type",
                        column.column_id
                    )
                })?;
            Ok::<Field, String>(Field::new(
                &column.name,
                domain_column.0.clone(),
                domain_column.1,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let selected_schema = Schema::new(selected);
    let schema = if let Some(provider_schema) = provider_schema {
        // The read provider owns field metadata such as Iceberg field IDs and
        // initial defaults. A physical scan can also carry execution-only
        // columns (for example DML equality keys), so retain provider fields
        // only where the native output actually consumes the same field.
        Schema::new(
            selected_schema
                .fields()
                .iter()
                .map(|selected| {
                    provider_schema
                        .fields()
                        .iter()
                        .find(|provider| {
                            provider.name() == selected.name()
                                && provider.is_nullable() == selected.is_nullable()
                                && provider.data_type() == selected.data_type()
                        })
                        .cloned()
                        .unwrap_or_else(|| selected.clone())
                })
                .collect::<Vec<_>>(),
        )
    } else {
        selected_schema
    };
    let mut writer = StreamWriter::try_new(Vec::new(), &schema)
        .map_err(|error| format!("encode ConnectorReadSource expected schema: {error}"))?;
    writer
        .finish()
        .map_err(|error| format!("finish ConnectorReadSource expected schema: {error}"))?;
    let bytes = writer.get_ref().clone();
    if bytes.len() > novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(format!(
            "ConnectorReadSource expected schema exceeds {} bytes",
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;

    use super::encode_connector_expected_schema_ipc;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use novarocks_protocol::common;

    #[test]
    fn connector_expected_schema_uses_domain_columns_not_encoded_type_desc() {
        let bytes = encode_connector_expected_schema_ipc(
            &[common::OutputColumn {
                column_id: 7,
                name: "id".to_string(),
                r#type: None,
                nullable: false,
                is_internal: false,
            }],
            &[OutputColumn {
                column_id: ColumnId(7),
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            &[],
            &[],
            None,
            None,
        )
        .expect("domain schema should encode without a protobuf type descriptor");

        assert!(!bytes.is_empty());
    }

    #[test]
    fn spi5b_connector_expected_schema_preserves_provider_field_metadata() {
        let provider_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true).with_metadata(HashMap::from([(
                "novarocks.iceberg.initial_default".to_string(),
                "9".to_string(),
            )])),
        ]));
        let bytes = encode_connector_expected_schema_ipc(
            &[common::OutputColumn {
                column_id: 7,
                name: "value".to_string(),
                r#type: None,
                nullable: true,
                is_internal: false,
            }],
            &[OutputColumn {
                column_id: ColumnId(7),
                name: "value".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                is_internal: false,
            }],
            &[],
            &[],
            None,
            Some(&provider_schema),
        )
        .expect("provider schema should encode");
        let decoded = StreamReader::try_new(Cursor::new(bytes), None)
            .expect("decode provider schema")
            .schema();
        assert_eq!(
            decoded.fields()[0]
                .metadata()
                .get("novarocks.iceberg.initial_default"),
            Some(&"9".to_string())
        );
    }
}
