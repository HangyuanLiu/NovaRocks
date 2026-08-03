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

use crate::engine::query_planning::bindings::{QueryScanMaterialization, QueryTableBindingStore};
use crate::query_execution::preparation::scan::{
    ResolvedIcebergFileScan, ResolvedIcebergMetadataScan, ResolvedScanBinding,
    ResolvedScanExecution, ScanBindingResolver, ScanExecutionBindings,
};
use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, DistributedPlan, FragmentId,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

mod iceberg;
mod projection;
mod pruning;
mod static_predicate;

pub(crate) use iceberg::build_iceberg_metadata_scan_range_params;
use iceberg::{plan_iceberg_connector_read, plan_iceberg_delta_connector_read};
use projection::{resolve_effective_required_reads, resolve_physical_columns};
use static_predicate::lower_static_connector_predicates;

/// Immutable scan-planning choices derived from the session before connector
/// negotiation begins. Keeping this outside the native carrier makes an
/// explicit disabled setting a safe FE-side rollback.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScanPreparationOptions {
    pub(crate) enable_connector_static_predicate_pushdown: bool,
    /// Parallelism is frozen at statement admission from the live backend
    /// topology. Providers use it only while producing opaque splits; it is
    /// never rediscovered from mutable membership later in the request.
    pub(crate) connector_target_parallelism: std::num::NonZeroUsize,
    /// An internal/test-only hard cap. Production admission deliberately does
    /// not expose this as a user setting.
    pub(crate) connector_max_split_bytes: Option<std::num::NonZeroU64>,
}

impl ScanPreparationOptions {
    pub(crate) fn new(
        enable_connector_static_predicate_pushdown: bool,
        connector_target_parallelism: std::num::NonZeroUsize,
        connector_max_split_bytes: Option<std::num::NonZeroU64>,
    ) -> Self {
        Self {
            enable_connector_static_predicate_pushdown,
            connector_target_parallelism,
            connector_max_split_bytes,
        }
    }

    #[cfg(test)]
    pub(crate) fn single_backend_fixture() -> Self {
        Self::new(
            true,
            std::num::NonZeroUsize::new(1).expect("one is non-zero"),
            None,
        )
    }
}

pub(super) fn prepare_scan_bindings(
    plan: &DistributedPlan,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn ScanBindingResolver>,
    options: ScanPreparationOptions,
) -> Result<ScanExecutionBindings, String> {
    let mut bindings = ScanExecutionBindings::default();
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    for fragment in plan.fragments() {
        collect_scan_bindings(
            fragment.fragment_id,
            &fragment.root,
            controls,
            context,
            query_table_bindings,
            resolver,
            options,
            &mut seen_scan_node_ids,
            &mut bindings,
        )?;
    }
    Ok(bindings)
}

fn collect_scan_bindings(
    fragment_id: FragmentId,
    node: &DistributedNode,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn ScanBindingResolver>,
    options: ScanPreparationOptions,
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
            controls,
            context,
            query_table_bindings,
            resolver,
            options,
            bindings,
        )?;
    }
    for child in &node.children {
        if child.fragment_id == fragment_id {
            collect_scan_bindings(
                fragment_id,
                child,
                controls,
                context,
                query_table_bindings,
                resolver,
                options,
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
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn ScanBindingResolver>,
    options: ScanPreparationOptions,
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    let execution = match &scan.table.source {
        ScanSource::Sql(source) => match source.kind {
            crate::sql::planner::table::SqlScanKind::Data { .. }
            | crate::sql::planner::table::SqlScanKind::FrozenInputSet { .. } => {
                let query_table_bindings = query_table_bindings.ok_or_else(|| {
                    format!(
                        "SQL scan node_id={node_id} has binding token but no query-local binding store"
                    )
                })?;
                let materialization = query_table_bindings
                    .scan_materialization(source.binding)?
                    .ok_or_else(|| {
                        format!(
                            "SQL scan binding for '{}.{}.{}' has no scan materialization",
                            source.table.catalog, source.table.namespace, source.table.table
                        )
                    })?;
                let QueryScanMaterialization::IcebergDataFiles {
                    table,
                    files,
                    binding,
                } = materialization
                else {
                    return Err(format!(
                        "SQL data scan binding for '{}.{}.{}' has non-data materialization",
                        source.table.catalog, source.table.namespace, source.table.table
                    ));
                };
                ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
                    table,
                    files,
                    binding,
                })
            }
            crate::sql::planner::table::SqlScanKind::Metadata { .. } => {
                let query_table_bindings = query_table_bindings.ok_or_else(|| {
                    format!(
                        "SQL metadata scan node_id={node_id} has binding token but no query-local binding store"
                    )
                })?;
                let materialization = query_table_bindings
                    .scan_materialization(source.binding)?
                    .ok_or_else(|| {
                        format!(
                            "SQL metadata scan binding for '{}.{}.{}' has no scan materialization",
                            source.table.catalog, source.table.namespace, source.table.table
                        )
                    })?;
                let QueryScanMaterialization::IcebergMetadata {
                    table,
                    metadata_table_type,
                    serialized_table,
                    metadata_payload,
                } = materialization
                else {
                    return Err(format!(
                        "SQL metadata scan binding for '{}.{}.{}' has non-metadata materialization",
                        source.table.catalog, source.table.namespace, source.table.table
                    ));
                };
                ResolvedScanExecution::IcebergMetadata(ResolvedIcebergMetadataScan {
                    table,
                    metadata_table_type,
                    serialized_table,
                    metadata_payload,
                })
            }
            crate::sql::planner::table::SqlScanKind::ConnectorRead
            | crate::sql::planner::table::SqlScanKind::Delta { .. }
            | crate::sql::planner::table::SqlScanKind::MvTargetState { .. }
            | crate::sql::planner::table::SqlScanKind::MvTargetLocator { .. } => {
                let source_context = scan_source_context(&scan.table.source);
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
        },
        ScanSource::IcebergDataFiles {
            table,
            files,
            binding,
            ..
        } => match query_table_bindings {
            Some(bindings) => {
                let binding_id = bindings
                    .iceberg_data_file_binding_id(table, *binding)
                    .ok_or_else(|| {
                        format!(
                            "scan preparation has no query-local token for '{}.{}.{}'",
                            table.catalog, table.namespace, table.table
                        )
                    })?;
                let materialization =
                    bindings.scan_materialization(binding_id)?.ok_or_else(|| {
                        format!(
                            "query binding for '{}.{}.{}' has no scan materialization",
                            table.catalog, table.namespace, table.table
                        )
                    })?;
                let QueryScanMaterialization::IcebergDataFiles {
                    table,
                    files,
                    binding,
                } = materialization
                else {
                    return Err(format!(
                        "query binding for '{}.{}.{}' has non-data materialization",
                        table.catalog, table.namespace, table.table
                    ));
                };
                ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
                    table,
                    files,
                    binding,
                })
            }
            // Compatibility coverage for synthetic unit fixtures that do not
            // model request admission. Production compiler callers always
            // retain a binding store and therefore use the exact materialized
            // provider facts above.
            None => ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
                table: table.clone(),
                files: files.clone(),
                binding: *binding,
            }),
        },
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
    let (ranges, equality_required, connector_read) = match &execution {
        ResolvedScanExecution::ConnectorRead => {
            let resolver = resolver.ok_or_else(|| {
                format!("connector-pinned scan node_id={node_id} requires a scan binding resolver")
            })?;
            let read = resolver
                .resolve_connector_read(node_id, scan)
                .map_err(|error| {
                    format!(
                        "scan binding resolver failed to provide connector read for node_id={node_id}: {error}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "scan binding resolver returned no connector read for connector-pinned node_id={node_id}"
                    )
                })?;
            (Vec::new(), Vec::new(), Some(read))
        }
        ResolvedScanExecution::IcebergFiles(files) => {
            // Design: ADR-0018 (docs/adr/ADR-0018-static-connector-predicate-disposition.md)
            let static_predicates = options
                .enable_connector_static_predicate_pushdown
                .then(|| {
                    let connector_schema_fields = files
                        .table
                        .schema
                        .fields
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>();
                    lower_static_connector_predicates(scan, &connector_schema_fields)
                })
                .unwrap_or_default();
            let exact_lease = query_table_bindings
                .map(|bindings| exact_query_binding_lease_for_source(bindings, &scan.table.source))
                .transpose()?;
            let planned = plan_iceberg_connector_read(
                controls,
                exact_lease,
                context.clone(),
                scan,
                &execution,
                static_predicates,
                options.connector_target_parallelism,
                options.connector_max_split_bytes,
            )
            .map_err(|err| format!("scan preparation node_id={node_id}: {err}"))?;
            // The provider reader projects physical equality keys internally
            // and drops them before delivery. Core therefore never owns a
            // hidden Iceberg delete column or file range.
            (Vec::new(), Vec::new(), Some(planned))
        }
        ResolvedScanExecution::IcebergMetadata(_) => (
            vec![build_iceberg_metadata_scan_range_params()],
            Vec::new(),
            None,
        ),
        ResolvedScanExecution::IcebergDelta(_) => {
            let ScanSource::IcebergDeltaTable { .. } = &scan.table.source else {
                return Err(format!(
                    "scan preparation node_id={node_id}: IcebergDelta execution requires IcebergDeltaTable source"
                ));
            };
            let exact_lease = query_table_bindings
                .map(|bindings| exact_query_binding_lease_for_source(bindings, &scan.table.source))
                .transpose()?;
            let planned = plan_iceberg_delta_connector_read(
                controls,
                exact_lease,
                context.clone(),
                scan,
                &execution,
                options.connector_target_parallelism,
                options.connector_max_split_bytes,
            )
            .map_err(|err| format!("scan preparation node_id={node_id}: {err}"))?;
            (Vec::new(), Vec::new(), Some(planned))
        }
    };
    let required_reads = resolve_effective_required_reads(node_id, scan, &equality_required)?;
    bindings.insert_binding(ResolvedScanBinding {
        node_id,
        execution,
        physical_columns,
        required_reads,
    })?;
    if let Some(connector_read) = connector_read {
        bindings.insert_connector_read(fragment_id, node_id, connector_read)?;
    }
    bindings.insert_scan_ranges(fragment_id, node_id, ranges)
}

/// Once SQL catalog resolution selected a query binding, preparation must use
/// that same connector generation.  A missing binding is a contract error,
/// not permission to reacquire `current` and silently mix metadata versions.
fn exact_query_binding_lease_for_source(
    bindings: &QueryTableBindingStore,
    source: &ScanSource,
) -> Result<novarocks_spi::connector::ConnectorControlPlanningLease, String> {
    let (binding_id, expected_catalog, source_name) = match source {
        ScanSource::Sql(source) => (
            Some(source.binding),
            source.table.catalog.as_str(),
            format!(
                "'{}.{}.{}'",
                source.table.catalog, source.table.namespace, source.table.table
            ),
        ),
        ScanSource::IcebergDataFiles { table, binding, .. } => (
            bindings.iceberg_data_file_binding_id(table, *binding),
            table.catalog.as_str(),
            format!("'{}.{}.{}'", table.catalog, table.namespace, table.table),
        ),
        ScanSource::IcebergDeltaTable { table, .. } => (
            bindings.iceberg_data_file_binding_id(
                table,
                crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
            ),
            table.catalog.as_str(),
            format!("'{}.{}.{}'", table.catalog, table.namespace, table.table),
        ),
        source => {
            return Err(format!(
                "scan preparation cannot recover an exact query binding from source {}",
                scan_source_kind(source)
            ));
        }
    };
    let binding_id = binding_id
        .ok_or_else(|| format!("scan preparation has no exact query binding for {source_name}"))?;
    let lease = bindings.planning_lease(binding_id)?.ok_or_else(|| {
        format!("query binding for {source_name} has no connector planning lease")
    })?;
    if lease.binding().descriptor().instance_id.as_str() != expected_catalog {
        return Err(format!(
            "query binding lease owner '{:?}' does not match scan catalog '{}'",
            lease.binding().descriptor().instance_id,
            expected_catalog
        ));
    }
    Ok(lease)
}

fn validate_resolved_execution_kind(
    node_id: i32,
    source: &ScanSource,
    execution: &ResolvedScanExecution,
) -> Result<(), String> {
    let valid = match source {
        ScanSource::Sql(source) => match source.kind {
            crate::sql::planner::table::SqlScanKind::ConnectorRead => {
                matches!(execution, ResolvedScanExecution::ConnectorRead)
            }
            crate::sql::planner::table::SqlScanKind::Delta { .. } => {
                matches!(execution, ResolvedScanExecution::IcebergDelta(_))
            }
            crate::sql::planner::table::SqlScanKind::Data { .. }
            | crate::sql::planner::table::SqlScanKind::FrozenInputSet { .. }
            | crate::sql::planner::table::SqlScanKind::MvTargetState { .. }
            | crate::sql::planner::table::SqlScanKind::MvTargetLocator { .. } => {
                matches!(execution, ResolvedScanExecution::IcebergFiles(_))
            }
            crate::sql::planner::table::SqlScanKind::Metadata { .. } => {
                matches!(execution, ResolvedScanExecution::IcebergMetadata(_))
            }
        },
        ScanSource::ConnectorPinned => matches!(execution, ResolvedScanExecution::ConnectorRead),
        ScanSource::IcebergDeltaTable { .. } => {
            matches!(execution, ResolvedScanExecution::IcebergDelta(_))
        }
        ScanSource::IcebergDataFiles { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::MvTargetState(_)
        | ScanSource::MvTargetLocator(_) => {
            matches!(execution, ResolvedScanExecution::IcebergFiles(_))
        }
    };
    if valid {
        return Ok(());
    }
    let required = if matches!(source, ScanSource::ConnectorPinned) {
        "ConnectorRead"
    } else if matches!(source, ScanSource::IcebergDeltaTable { .. }) {
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
        ScanSource::MvTargetState(_) => "target-state",
        ScanSource::MvTargetLocator(_) => "target-locator",
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
        ScanSource::ConnectorPinned
            | ScanSource::IcebergVersionTable { .. }
            | ScanSource::MvTargetState(_)
            | ScanSource::MvTargetLocator(_)
            | ScanSource::IcebergDeltaTable { .. }
    )
}

fn scan_source_kind(source: &ScanSource) -> &'static str {
    match source {
        ScanSource::Sql(source) => match source.kind {
            crate::sql::planner::table::SqlScanKind::ConnectorRead => "SqlConnectorRead",
            crate::sql::planner::table::SqlScanKind::Data { .. } => "SqlData",
            crate::sql::planner::table::SqlScanKind::FrozenInputSet { .. } => "SqlFrozenInputSet",
            crate::sql::planner::table::SqlScanKind::Metadata { .. } => "SqlMetadata",
            crate::sql::planner::table::SqlScanKind::Delta { .. } => "SqlDelta",
            crate::sql::planner::table::SqlScanKind::MvTargetState { .. } => "SqlMvTargetState",
            crate::sql::planner::table::SqlScanKind::MvTargetLocator { .. } => "SqlMvTargetLocator",
        },
        ScanSource::ConnectorPinned => "ConnectorPinned",
        ScanSource::IcebergDataFiles { .. } => "IcebergDataFiles",
        ScanSource::IcebergDeltaTable { .. } => "IcebergDeltaTable",
        ScanSource::IcebergVersionTable { .. } => "IcebergVersionTable",
        ScanSource::MvTargetState(_) => "IcebergMvTargetState",
        ScanSource::MvTargetLocator(_) => "IcebergMvTargetLocator",
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
