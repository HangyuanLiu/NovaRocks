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
        ScanSource::Sql(source) => match &source.kind {
            crate::sql::planner::table::SqlScanKind::Data { .. }
            | crate::sql::planner::table::SqlScanKind::FrozenInputSet {
                version: crate::sql::planner::table::SqlTableVersionSelector::Current,
            } => {
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
            crate::sql::planner::table::SqlScanKind::FrozenInputSet {
                version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
            } => {
                let query_table_bindings = query_table_bindings.ok_or_else(|| {
                    format!(
                        "SQL frozen scan node_id={node_id} has binding token but no query-local binding store"
                    )
                })?;
                let materialization = query_table_bindings
                    .frozen_snapshot_materialization(source.binding, *snapshot_id)?;
                let QueryScanMaterialization::IcebergDataFiles {
                    table,
                    files,
                    binding,
                } = materialization
                else {
                    return Err(format!(
                        "SQL frozen scan binding for '{}.{}.{}' has non-data materialization",
                        source.table.catalog, source.table.namespace, source.table.table
                    ));
                };
                ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
                    table,
                    files,
                    binding,
                })
            }
            crate::sql::planner::table::SqlScanKind::FrozenInputSet {
                version:
                    crate::sql::planner::table::SqlTableVersionSelector::TimestampMillis(timestamp),
            } => {
                return Err(format!(
                    "SQL frozen scan node_id={node_id} has timestamp selector {timestamp} without an admitted snapshot file set"
                ));
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
            crate::sql::planner::table::SqlScanKind::MvTargetState { facts } => {
                resolve_frozen_mv_target_scan(
                    node_id,
                    source,
                    query_table_bindings,
                    &facts.target_table_uuid,
                    facts.target_snapshot_id,
                    "target-state",
                    Some(&facts.partition_constraint),
                )?
            }
            crate::sql::planner::table::SqlScanKind::MvTargetLocator { facts } => {
                resolve_frozen_mv_target_scan(
                    node_id,
                    source,
                    query_table_bindings,
                    &facts.target_table_uuid,
                    facts.target_snapshot_id,
                    "target-locator",
                    None,
                )?
            }
            crate::sql::planner::table::SqlScanKind::ConnectorRead
            | crate::sql::planner::table::SqlScanKind::Delta { .. } => {
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
            let ScanSource::Sql(source) = &scan.table.source;
            if !matches!(
                source.kind,
                crate::sql::planner::table::SqlScanKind::Delta { .. }
            ) {
                return Err(format!(
                    "scan preparation node_id={node_id}: IcebergDelta execution requires a SQL delta source"
                ));
            }
            let query_table_bindings = query_table_bindings.ok_or_else(|| {
                format!(
                    "SQL delta scan node_id={node_id} has binding token but no query-local binding store"
                )
            })?;
            let materialization = query_table_bindings
                .scan_materialization(source.binding)?
                .ok_or_else(|| {
                    format!(
                        "SQL delta scan binding for '{}.{}.{}' has no scan materialization",
                        source.table.catalog, source.table.namespace, source.table.table
                    )
                })?;
            let QueryScanMaterialization::IcebergDataFiles { table, .. } = materialization else {
                return Err(format!(
                    "SQL delta scan binding for '{}.{}.{}' has non-data materialization",
                    source.table.catalog, source.table.namespace, source.table.table
                ));
            };
            let exact_lease =
                exact_query_binding_lease_for_source(query_table_bindings, &scan.table.source)?;
            let planned = plan_iceberg_delta_connector_read(
                exact_lease,
                context.clone(),
                &table,
                &scan.predicates,
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

/// Recover an IMV target scan only from its admitted query-local token.  The
/// target-state and target-locator lanes deliberately share the same frozen
/// table/file materialization, so preparation never resolves another target
/// generation or invokes the legacy MV scan resolver.
fn resolve_frozen_mv_target_scan(
    node_id: i32,
    source: &crate::sql::planner::table::SqlScanSource,
    query_table_bindings: Option<&QueryTableBindingStore>,
    expected_uuid: &str,
    expected_snapshot_id: Option<i64>,
    lane: &str,
    target_state_partition_constraint: Option<
        &crate::sql::planner::table::SqlMvTargetStatePartitionConstraint,
    >,
) -> Result<ResolvedScanExecution, String> {
    let query_table_bindings = query_table_bindings.ok_or_else(|| {
        format!(
            "SQL MV {lane} scan node_id={node_id} has binding token but no query-local binding store"
        )
    })?;
    let materialization = query_table_bindings
        .scan_materialization(source.binding)?
        .ok_or_else(|| {
            format!(
                "SQL MV {lane} scan binding for '{}.{}.{}' has no frozen target materialization",
                source.table.catalog, source.table.namespace, source.table.table
            )
        })?;
    let QueryScanMaterialization::IcebergMvTarget {
        table,
        files,
        binding,
        target_table_uuid,
        frozen_snapshot_id,
        target_state_partition_filter,
        target_partition_contract,
    } = materialization
    else {
        return Err(format!(
            "SQL MV {lane} scan binding for '{}.{}.{}' has non-target materialization",
            source.table.catalog, source.table.namespace, source.table.table
        ));
    };
    if !table.catalog.eq_ignore_ascii_case(&source.table.catalog)
        || !table
            .namespace
            .eq_ignore_ascii_case(&source.table.namespace)
        || !table.table.eq_ignore_ascii_case(&source.table.table)
    {
        return Err(format!(
            "SQL MV {lane} scan node_id={node_id} identity does not match its frozen target binding"
        ));
    }
    if target_table_uuid != expected_uuid || frozen_snapshot_id != expected_snapshot_id {
        return Err(format!(
            "SQL MV {lane} scan node_id={node_id} target UUID or snapshot does not match its frozen binding"
        ));
    }
    let files = match target_state_partition_constraint {
        Some(
            crate::sql::planner::table::SqlMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired,
        ) => filter_frozen_mv_target_state_files(
            files,
            &target_state_partition_filter,
            target_partition_contract.as_ref(),
            node_id,
        )?,
        Some(crate::sql::planner::table::SqlMvTargetStatePartitionConstraint::Unpartitioned)
        | None => files,
    };
    Ok(ResolvedScanExecution::IcebergFiles(
        ResolvedIcebergFileScan {
            table,
            files,
            binding,
        },
    ))
}

/// Apply the admitted affected-partition allow-list to the already frozen MV
/// target files.  The SQL plan deliberately carries only the requirement for
/// an allow-list; the keys and partition contract remain application-owned
/// binding facts.  This must not consult a catalog, a provider, or a current
/// connector generation.
fn filter_frozen_mv_target_state_files(
    files: Vec<novarocks_connector_iceberg::scan_model::IcebergDataFileInfo>,
    filter: &crate::mv::model::TargetPartitionFilter,
    contract: Option<&crate::mv::persistence::schema::MvPartitionContract>,
    node_id: i32,
) -> Result<Vec<novarocks_connector_iceberg::scan_model::IcebergDataFileInfo>, String> {
    let crate::mv::model::TargetPartitionFilter::AllowList(allow_list) = filter else {
        return Ok(files);
    };
    if allow_list.is_empty() {
        return Ok(Vec::new());
    }
    let contract = contract.ok_or_else(|| {
        format!(
            "SQL MV target-state scan node_id={node_id} requires an affected-partition allow-list but its frozen binding has no target partition contract"
        )
    })?;
    files
        .into_iter()
        .filter_map(|file| match frozen_mv_target_partition_key(contract, &file) {
            Ok(key) if allow_list.contains(&key) => Some(Ok(file)),
            Ok(_) => None,
            Err(err) => Some(Err(format!(
                "SQL MV target-state scan node_id={node_id} cannot map frozen target file {} partition: {err}",
                file.path
            ))),
        })
        .collect()
}

fn frozen_mv_target_partition_key(
    contract: &crate::mv::persistence::schema::MvPartitionContract,
    file: &novarocks_connector_iceberg::scan_model::IcebergDataFileInfo,
) -> Result<crate::mv::model::MvPartitionKey, String> {
    let spec_id = file
        .partition_spec_id
        .ok_or_else(|| format!("target file {} is missing partition spec id", file.path))?;
    let mut fields = Vec::with_capacity(contract.fields.len());
    for partition_field in &contract.fields {
        let expected_transform = frozen_mv_target_transform_text(&partition_field.transform)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} uses unsupported void transform",
                    partition_field.partition_field_name
                )
            })?;
        let value = file
            .partition_values
            .iter()
            .find(|value| {
                value
                    .source_column
                    .eq_ignore_ascii_case(&partition_field.source_column_name)
                    && value.transform.eq_ignore_ascii_case(&expected_transform)
            })
            .or_else(|| {
                file.partition_values.iter().find(|value| {
                    value
                        .field_name
                        .eq_ignore_ascii_case(&partition_field.partition_field_name)
                        && value.transform.eq_ignore_ascii_case(&expected_transform)
                })
            })
            .ok_or_else(|| {
                format!(
                    "target file {} has no partition value for {} with transform {}",
                    file.path, partition_field.partition_field_name, expected_transform
                )
            })?;
        fields.push(crate::mv::model::MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            frozen_mv_target_partition_value(value)?,
        ));
    }
    Ok(crate::mv::model::MvPartitionKey::new(spec_id, fields))
}

fn frozen_mv_target_partition_value(
    value: &novarocks_connector_iceberg::scan_model::IcebergPartitionFieldValue,
) -> Result<crate::mv::model::MvPartitionValue, String> {
    use novarocks_connector_iceberg::scan_model::IcebergPartitionValue;

    match &value.value {
        None => Ok(crate::mv::model::MvPartitionValue::Null),
        Some(IcebergPartitionValue::Boolean(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Int32(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Int64(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Float(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Double(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::String(value)) => {
            Ok(crate::mv::model::MvPartitionValue::String(value.clone()))
        }
        Some(IcebergPartitionValue::Binary(_)) => Err(format!(
            "target partition field {} has unsupported binary value",
            value.field_name
        )),
    }
}

fn frozen_mv_target_transform_text(
    transform: &crate::mv::persistence::schema::MvPartitionTransformContract,
) -> Option<String> {
    use crate::mv::persistence::schema::MvPartitionTransformContract;

    match transform {
        MvPartitionTransformContract::Identity => Some("identity".to_string()),
        MvPartitionTransformContract::Year => Some("year".to_string()),
        MvPartitionTransformContract::Month => Some("month".to_string()),
        MvPartitionTransformContract::Day => Some("day".to_string()),
        MvPartitionTransformContract::Hour => Some("hour".to_string()),
        MvPartitionTransformContract::Bucket { num_buckets } => {
            Some(format!("bucket({num_buckets})"))
        }
        MvPartitionTransformContract::Truncate { width } => Some(format!("truncate({width})")),
        MvPartitionTransformContract::Void => None,
    }
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
        ScanSource::Sql(source) => match &source.kind {
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
    };
    if valid {
        return Ok(());
    }
    let required = match source {
        ScanSource::Sql(sql_source) => match sql_source.kind {
            crate::sql::planner::table::SqlScanKind::ConnectorRead => "ConnectorRead",
            crate::sql::planner::table::SqlScanKind::Delta { .. } => "IcebergDelta",
            crate::sql::planner::table::SqlScanKind::Metadata { .. } => "IcebergMetadata",
            crate::sql::planner::table::SqlScanKind::Data { .. }
            | crate::sql::planner::table::SqlScanKind::FrozenInputSet { .. }
            | crate::sql::planner::table::SqlScanKind::MvTargetState { .. }
            | crate::sql::planner::table::SqlScanKind::MvTargetLocator { .. } => "IcebergFiles",
        },
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
        ScanSource::Sql(crate::sql::planner::table::SqlScanSource {
            kind: crate::sql::planner::table::SqlScanKind::MvTargetState { .. },
            ..
        }) => "target-state",
        ScanSource::Sql(crate::sql::planner::table::SqlScanSource {
            kind: crate::sql::planner::table::SqlScanKind::MvTargetLocator { .. },
            ..
        }) => "target-locator",
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
                == novarocks_connector_iceberg::scan_model::IcebergDeleteFileContent::Equality
        })
    }) {
        return Err(format!(
            "Iceberg {target_kind} scan node_id={node_id} does not support equality deletes yet"
        ));
    }
    Ok(())
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
    }
}

fn scan_source_context(source: &ScanSource) -> String {
    match source {
        ScanSource::Sql(sql_source) => match sql_source.kind {
            crate::sql::planner::table::SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
            } => format!(
                "SqlDelta from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}"
            ),
            _ => scan_source_kind(source).to_string(),
        },
    }
}

#[cfg(test)]
mod tests;
