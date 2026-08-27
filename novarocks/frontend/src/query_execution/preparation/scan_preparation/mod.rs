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

//! Frontend scan preparation for the typed connector read stack.
//!
//! Preparation freezes one relation per SQL scan and hands the connector's own
//! split manager on to the execution round. It deliberately enumerates no
//! split: the typed carrier has no split list, and a count taken here would
//! pin the query's parallelism to whatever enumeration happened to produce
//! first.
// Design: ADR-0114 (docs/adr/ADR-0114-trino-aligned-typed-connector-read-stack.md)

use std::sync::Arc;

use crate::catalog_application::query_bindings::{
    QueryScanMaterialization, QueryTableBindingStore,
};
use crate::catalog_application::query_materializer::metadata_table_alias_suffix;
use crate::connector::typed_control_registry::{
    TypedConnectorControl, TypedConnectorControlRegistry,
};
use crate::query_execution::connector_domain::CatalogHandle;
use crate::query_execution::preparation::scan::{
    PreparedTypedConnectorScan, ResolvedScanBinding, ResolvedScanColumn, ResolvedScanExecution,
    ScanBindingResolver, ScanExecutionBindings,
};
use crate::query_execution::preparation::typed_scan::{TypedRelationFreeze, prepare_typed_scan};
use novarocks_proto::connector_read::{
    ConnectorRelationKind, TypedChangeWindow, TypedRelationVersion,
};
use novarocks_spi::connector::read_stack::{ConnectorSession, SchemaTableName};
use novarocks_spi::connector::{ConnectorExecutionBindingKey, ConnectorReadSelector};
use novarocks_sql::plan_read::PlanScanNode;
use novarocks_sql::plan_read::{DistributedNode, DistributedNodeKind, DistributedPlan, FragmentId};
use novarocks_sql::planning::query_execution::{
    SqlRuntimeFilterSourceScanRequest, SqlScanPreparationCategory, SqlScanPreparationFacts,
    scan_preparation_facts,
};

mod projection;
mod pruning;

use projection::{resolve_effective_required_reads, resolve_read_physical_columns};

/// A `DistributedNode::limit` of this value means the node declares no limit.
const NO_NODE_LIMIT: i64 = -1;

/// The typed connector inputs one statement's scan preparation needs.
///
/// Both halves are frozen with the statement: the registry answers exactly the
/// binding generation admission chose, and the session is the query's own
/// identity. The session deliberately carries no credential — a connector
/// authenticates through its installed control, never through a value the
/// planner hands it.
#[derive(Clone)]
pub(crate) struct TypedScanPreparation {
    control: Arc<TypedConnectorControlRegistry>,
    session: ConnectorSession,
}

impl TypedScanPreparation {
    pub(crate) fn new(
        control: Arc<TypedConnectorControlRegistry>,
        session: ConnectorSession,
    ) -> Self {
        Self { control, session }
    }
}

/// Immutable scan-planning choices derived from the session before connector
/// negotiation begins.
#[derive(Clone)]
pub(crate) struct ScanPreparationOptions {
    #[allow(
        dead_code,
        reason = "The typed stack negotiates its own predicate pushdown inside prepare_typed_scan; the setting survives only for the callers that still construct these options."
    )]
    enable_connector_static_predicate_pushdown: bool,
    /// Parallelism was frozen at statement admission only to size an eager
    /// split set. The typed stack enumerates lazily, so instance counts are
    /// read from the live backend topology by the scheduler instead.
    #[allow(
        dead_code,
        reason = "No frozen split set remains to size; kept so the existing construction sites need no change while the field's last reader is gone."
    )]
    connector_target_parallelism: std::num::NonZeroUsize,
    /// An internal/test-only hard cap on eager split size. Nothing splits
    /// eagerly any more.
    #[allow(
        dead_code,
        reason = "Split sizing moved to the connector's own lazy enumeration; kept so the existing construction sites need no change."
    )]
    connector_max_split_bytes: Option<std::num::NonZeroU64>,
    /// The typed control registry and session. It is absent only while the
    /// composition root has not threaded it through this call path; a scan
    /// that needs it then fails closed rather than reaching a fallback.
    typed: Option<TypedScanPreparation>,
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
            typed: None,
        }
    }

    /// Attach the statement's typed connector control and session.
    pub(crate) fn with_typed_connector_control(
        mut self,
        control: Arc<TypedConnectorControlRegistry>,
        session: ConnectorSession,
    ) -> Self {
        self.typed = Some(TypedScanPreparation::new(control, session));
        self
    }

    fn typed(&self) -> Result<&TypedScanPreparation, String> {
        self.typed.as_ref().ok_or_else(|| {
            "typed connector scan preparation requires the statement's typed control registry \
             and connector session; neither has a default"
                .to_string()
        })
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

/// Scan preparation carries the complete frozen planning context explicitly.
pub(super) fn prepare_scan_bindings(
    plan: &DistributedPlan,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn ScanBindingResolver>,
    options: &ScanPreparationOptions,
    runtime_filter_scans: &[SqlRuntimeFilterSourceScanRequest],
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
            runtime_filter_scans,
            &mut seen_scan_node_ids,
            &mut bindings,
        )?;
    }
    Ok(bindings)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Recursive scan preparation carries the complete frozen planning context explicitly."
)]
#[expect(
    clippy::only_used_in_recursion,
    reason = "Fragment identity is intentionally forwarded only while recursively walking the distributed plan."
)]
fn collect_scan_bindings(
    fragment_id: FragmentId,
    node: &DistributedNode,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn ScanBindingResolver>,
    options: &ScanPreparationOptions,
    runtime_filter_scans: &[SqlRuntimeFilterSourceScanRequest],
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
            node.limit,
            scan,
            context,
            query_table_bindings,
            resolver,
            options,
            runtime_filter_scans,
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
                runtime_filter_scans,
                seen_scan_node_ids,
                bindings,
            )?;
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "Scan preparation keeps independently-owned planning and execution bindings explicit."
)]
fn prepare_scan_node(
    fragment_id: FragmentId,
    node_id: i32,
    node_limit: i64,
    scan: &PlanScanNode,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn ScanBindingResolver>,
    options: &ScanPreparationOptions,
    runtime_filter_scans: &[SqlRuntimeFilterSourceScanRequest],
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    let facts = scan_preparation_facts(scan);
    let execution = match facts.category() {
        SqlScanPreparationCategory::AdmittedData
        | SqlScanPreparationCategory::AdmittedFrozenCurrent => {
            let query_table_bindings = query_table_bindings.ok_or_else(|| {
                format!(
                    "SQL scan node_id={node_id} has binding token but no query-local binding store"
                )
            })?;
            let materialization = query_table_bindings
                .scan_materialization(facts.binding())?
                .ok_or_else(|| {
                    format!(
                        "SQL scan binding for '{}.{}.{}' has no scan materialization",
                        facts.identity().catalog(),
                        facts.identity().namespace(),
                        facts.identity().table()
                    )
                })?;
            ResolvedScanExecution::AdmittedConnectorRead(materialization)
        }
        SqlScanPreparationCategory::AdmittedFrozenSnapshot => {
            let query_table_bindings = query_table_bindings.ok_or_else(|| {
                    format!(
                        "SQL frozen scan node_id={node_id} has binding token but no query-local binding store"
                    )
                })?;
            let snapshot_id = facts.frozen_snapshot_id().ok_or_else(|| {
                format!("SQL frozen scan node_id={node_id} has no admitted snapshot file set")
            })?;
            let materialization = query_table_bindings
                .frozen_snapshot_materialization(facts.binding(), snapshot_id)?;
            ResolvedScanExecution::AdmittedConnectorRead(materialization)
        }
        SqlScanPreparationCategory::FrozenTimestampWithoutAdmittedSnapshot => {
            let timestamp = facts.frozen_timestamp_millis().ok_or_else(|| {
                format!("SQL frozen scan node_id={node_id} has no admitted timestamp selector")
            })?;
            return Err(format!(
                "SQL frozen scan node_id={node_id} has timestamp selector {timestamp} without an admitted snapshot file set"
            ));
        }
        // A metadata table is a system relation of its base table. It reads
        // through the same admitted materialization as an ordinary scan; only
        // the relation family the connector freezes differs.
        SqlScanPreparationCategory::AdmittedMetadata => {
            let query_table_bindings = query_table_bindings.ok_or_else(|| {
                format!(
                    "SQL metadata scan node_id={node_id} has binding token but no query-local binding store"
                )
            })?;
            let materialization = query_table_bindings
                .scan_materialization(facts.binding())?
                .ok_or_else(|| {
                    format!(
                        "SQL metadata scan binding for '{}.{}.{}' has no scan materialization",
                        facts.identity().catalog(),
                        facts.identity().namespace(),
                        facts.identity().table()
                    )
                })?;
            ResolvedScanExecution::AdmittedSystemTable(materialization)
        }
        // A change-window read is its own relation family. The two endpoints
        // are stated by the scan; the exact query-local admission that names
        // the relation comes from the resolver, never from a fresh lookup.
        SqlScanPreparationCategory::Delta => {
            let source_context = facts.source_context();
            let resolver = resolver.ok_or_else(|| {
                format!(
                    "scan source {source_context} node_id={node_id} requires scan binding resolver"
                )
            })?;
            resolver
                .resolve_scan(node_id, scan)
                .map_err(|error| {
                    format!(
                        "scan binding resolver failed for required source {source_context} node_id={node_id}: {error}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "scan binding resolver returned no binding for required source {source_context} node_id={node_id}"
                    )
                })?
        }
        // Both MV target lanes share one frozen materialization and lower to
        // an ordinary pinned DATA scan; the lane itself carries no execution
        // difference the typed stack can observe.
        SqlScanPreparationCategory::MvTargetState => {
            resolve_frozen_mv_target_scan(node_id, &facts, query_table_bindings, "target-state")?
        }
        SqlScanPreparationCategory::MvTargetLocator => {
            resolve_frozen_mv_target_scan(node_id, &facts, query_table_bindings, "target-locator")?
        }
        // A read whose provider handle was pinned before generic preparation
        // is opaque by construction: there is no relation name or version left
        // to freeze typed, so this lane must fail closed rather than reach the
        // opaque carrier it used to emit.
        SqlScanPreparationCategory::ConnectorRead => {
            let _ = resolver;
            return Err(format!(
                "scan source {} node_id={node_id} is a pre-pinned opaque connector read, which the typed connector scan stack does not admit",
                facts.source_kind_label()
            ));
        }
    };
    validate_resolved_execution_kind(node_id, &facts, &execution)?;
    // The connector produces exactly the scan's physical columns. A synthetic
    // output — a VARIANT path column — is deliberately not one of them: the
    // backend materializes those on top of the physical read slots, so
    // offering one to the connector would ask for a column it does not have.
    // What the connector is asked to produce, and therefore what this scan
    // outputs: the node's own projection, never every column the relation has.
    let physical_columns = resolve_read_physical_columns(node_id, scan)?;
    // The runtime filters this scan must offer the connector. They are resolved
    // before the relation is frozen so the scan carrier declares them itself; a
    // filter added afterwards would never reach the reader.
    // Resolved against the same list the assignments are built from, so a
    // filter can only ever name a column this scan actually reads.
    let dynamic_filters = scan_dynamic_filters(
        fragment_id,
        node_id,
        &physical_columns,
        runtime_filter_scans,
    )?;
    let dynamic_filters = dynamic_filters.as_slice();
    let (ranges, equality_required, typed_scan) = match &execution {
        // The opaque lane is produced only by the pre-pinned source refused
        // above; every variant is named so that adding a lane is a compile
        // error here rather than a silent opaque read.
        ResolvedScanExecution::ConnectorRead => {
            return Err(format!(
                "scan preparation node_id={node_id}: a pre-pinned opaque connector read has no typed lowering"
            ));
        }
        ResolvedScanExecution::AdmittedSystemTable(materialization) => {
            let prepared = prepare_typed_connector_scan(
                node_id,
                node_limit,
                scan,
                &physical_columns,
                &facts,
                materialization,
                TypedRelationFreeze::SystemTable,
                context,
                options,
                dynamic_filters,
            )?;
            (Vec::new(), Vec::new(), prepared)
        }
        ResolvedScanExecution::AdmittedChangeWindow(materialization) => {
            let window = facts.delta_window().ok_or_else(|| {
                format!(
                    "scan preparation node_id={node_id}: a change-window scan requires a SQL delta source stating both endpoints"
                )
            })?;
            let prepared = prepare_typed_connector_scan(
                node_id,
                node_limit,
                scan,
                &physical_columns,
                &facts,
                materialization,
                TypedRelationFreeze::ChangeWindow(TypedChangeWindow::new(
                    window.from_snapshot_id(),
                    window.to_snapshot_id(),
                )),
                context,
                options,
                dynamic_filters,
            )?;
            (Vec::new(), Vec::new(), prepared)
        }
        ResolvedScanExecution::AdmittedConnectorRead(materialization) => {
            let version = typed_relation_version(node_id, &facts, materialization.selector)?;
            let prepared = prepare_typed_connector_scan(
                node_id,
                node_limit,
                scan,
                &physical_columns,
                &facts,
                materialization,
                TypedRelationFreeze::Table {
                    version,
                    reference: None,
                },
                context,
                options,
                dynamic_filters,
            )?;
            (Vec::new(), Vec::new(), prepared)
        }
    };
    let required_reads = resolve_effective_required_reads(node_id, scan, &equality_required)?;
    bindings.insert_binding(ResolvedScanBinding {
        node_id,
        execution,
        physical_columns,
        required_reads,
    })?;
    bindings.insert_typed_scan(fragment_id, node_id, typed_scan)?;
    bindings.insert_scan_ranges(fragment_id, node_id, ranges)
}

/// Lower one admitted relation onto the typed connector read stack.
///
/// The exact-generation rule is unchanged: the relation is frozen through the
/// control installed for the binding generation the materialization's planning
/// lease holds, never through a name-based or "current" lookup. `freeze` names
/// the relation family the lane asked for, and the connector's answer must be
/// exactly that family.
#[expect(
    clippy::too_many_arguments,
    reason = "Every argument is a distinct frozen fact of one scan; grouping them would hide which of them the connector sees."
)]
fn prepare_typed_connector_scan(
    node_id: i32,
    node_limit: i64,
    scan: &PlanScanNode,
    physical_columns: &[ResolvedScanColumn],
    facts: &SqlScanPreparationFacts,
    materialization: &QueryScanMaterialization,
    freeze: TypedRelationFreeze<'_>,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    options: &ScanPreparationOptions,
    dynamic_filters: &[(u32, String)],
) -> Result<PreparedTypedConnectorScan, String> {
    let typed = options.typed()?;
    let binding = materialization.planning_lease.binding();
    if materialization.table.owner() != &binding.descriptor().instance_id {
        return Err(format!(
            "typed connector scan node_id={node_id} has a table handle owned by another instance than its planning lease"
        ));
    }
    let binding_key = ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    if binding_key.instance_id.as_str() != facts.identity().catalog() {
        return Err(format!(
            "typed connector scan node_id={node_id} resolves instance '{}' but the scan names catalog '{}'",
            binding_key.instance_id.as_str(),
            facts.identity().catalog()
        ));
    }
    let control: TypedConnectorControl = typed
        .control
        .resolve(&binding_key)
        .map_err(|error| format!("typed connector scan node_id={node_id}: {error}"))?;
    let declaration = binding
        .execution_declaration(context)
        .map_err(|error| format!("typed connector scan node_id={node_id}: {error}"))?;
    let relation_name = typed_relation_name(node_id, facts, freeze)?;
    let relation = SchemaTableName::try_new(facts.identity().namespace(), &relation_name).map_err(
        |error| {
            format!(
                "typed connector scan node_id={node_id} cannot name relation '{}': {error}",
                facts.identity().fqn()
            )
        },
    )?;
    let catalog = CatalogHandle::new(
        binding_key.instance_id.as_str(),
        binding_key.incarnation.to_bytes(),
    );
    let prepared = prepare_typed_scan(
        &typed.session,
        catalog,
        &control,
        node_id,
        scan,
        physical_columns,
        &relation,
        freeze,
        node_scan_limit(node_limit),
        dynamic_filters,
    )
    .map_err(|error| format!("scan preparation node_id={node_id}: {error}"))?;
    // The relation family the connector actually froze must be the family this
    // lane asked it to freeze. Anything else would hand the reader a relation
    // it has no contract for -- a change-window request answered with a table
    // handle would silently read the whole relation.
    let frozen_kind = prepared.table_scan.table().relation_kind();
    if frozen_kind != freeze.relation_kind() {
        return Err(format!(
            "typed connector scan node_id={node_id} on '{}' asked the connector to freeze relation kind `{}` but it froze `{}`",
            facts.identity().fqn(),
            relation_kind_name(freeze.relation_kind()),
            relation_kind_name(frozen_kind)
        ));
    }
    let residual_predicates = prepared
        .residual_ordinals
        .iter()
        .map(|ordinal| {
            scan.predicates.get(*ordinal).cloned().ok_or_else(|| {
                format!(
                    "typed connector scan node_id={node_id} reported residual conjunct {ordinal}, which the scan does not have"
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PreparedTypedConnectorScan {
        declaration,
        prepared,
        residual_predicates,
        planning_lease: materialization.planning_lease.clone(),
    })
}

/// The runtime filters one scan offers the connector, as `(filter id, scan
/// output column name)` pairs.
///
/// A scan-domain request names one planner column of this scan, and the typed
/// scan binds the filter to that output's assignment — so the filter reaches
/// the reader as the connector's own `ColumnHandle`, never as an ordinal into
/// some provider schema. A request naming a column this scan does not output is
/// refused here: dropping it would leave the filter's producer waiting on a
/// consumer that never applies it.
fn scan_dynamic_filters(
    fragment_id: FragmentId,
    node_id: i32,
    physical_columns: &[ResolvedScanColumn],
    requests: &[SqlRuntimeFilterSourceScanRequest],
) -> Result<Vec<(u32, String)>, String> {
    requests
        .iter()
        .filter(|request| request.fragment_id == fragment_id && request.node_id == node_id)
        .map(|request| {
            let matched = physical_columns
                .iter()
                .filter(|column| column.planner.column_id == request.column_id)
                .collect::<Vec<_>>();
            let [column] = matched.as_slice() else {
                return Err(format!(
                    "runtime filter binding id={} scan-domain target column id {} does not resolve to exactly one physical output of scan node_id={node_id}",
                    request.binding_id, request.column_id
                ));
            };
            Ok((request.binding_id, column.planner.name.clone()))
        })
        .collect()
}

/// The relation name one lane asks the connector to freeze.
///
/// Every family but a system relation names the table itself. A system
/// relation is addressed by the connector's own `<table>$<SUFFIX>` spelling,
/// which is the only thing that tells it which one to materialize, so the
/// suffix comes from the query materializer's single suffix vocabulary rather
/// than from a second spelling built here.
fn typed_relation_name(
    node_id: i32,
    facts: &SqlScanPreparationFacts,
    freeze: TypedRelationFreeze<'_>,
) -> Result<String, String> {
    match freeze {
        TypedRelationFreeze::Table { .. } | TypedRelationFreeze::ChangeWindow(_) => {
            Ok(facts.identity().table().to_string())
        }
        TypedRelationFreeze::SystemTable => {
            let kind = facts.metadata_table_kind().ok_or_else(|| {
                format!(
                    "typed connector scan node_id={node_id} on '{}' is a metadata scan with no metadata table kind",
                    facts.identity().fqn()
                )
            })?;
            Ok(format!(
                "{}${}",
                facts.identity().table(),
                metadata_table_alias_suffix(kind)
            ))
        }
    }
}

/// The typed relation version an admitted selector names.
fn typed_relation_version(
    node_id: i32,
    facts: &SqlScanPreparationFacts,
    selector: ConnectorReadSelector,
) -> Result<TypedRelationVersion, String> {
    match selector {
        ConnectorReadSelector::Current => Ok(TypedRelationVersion::Current),
        ConnectorReadSelector::SnapshotId(snapshot_id) => {
            Ok(TypedRelationVersion::SnapshotId(snapshot_id))
        }
        // A timestamp is resolved to a snapshot at admission. Reaching the
        // connector with one would ask it to re-resolve the pin this query
        // already froze.
        ConnectorReadSelector::TimestampMicros(timestamp) => Err(format!(
            "typed connector scan node_id={node_id} on '{}' carries unresolved timestamp selector {timestamp}; admission must pin a snapshot first",
            facts.identity().fqn()
        )),
    }
}

/// The limit a scan node offers the connector, if it declares one.
fn node_scan_limit(node_limit: i64) -> Option<u64> {
    (node_limit != NO_NODE_LIMIT)
        .then(|| u64::try_from(node_limit).ok())
        .flatten()
}

/// The stable wire vocabulary for one relation family.
///
/// Every kind is named so a new one is a compile error here rather than a
/// relation that reports itself under some other family's name.
const fn relation_kind_name(kind: ConnectorRelationKind) -> &'static str {
    match kind {
        ConnectorRelationKind::Table => "table",
        ConnectorRelationKind::TableFunction => "table_function",
        ConnectorRelationKind::ChangeWindow => "change_window",
        ConnectorRelationKind::SystemTable => "system_table",
        ConnectorRelationKind::TableExecute => "table_execute",
        ConnectorRelationKind::MergeTable => "merge_table",
    }
}

/// Recover an IMV target scan only from its admitted query-local token.  The
/// target-state and target-locator lanes deliberately share the same frozen
/// table/file materialization, so preparation never resolves another target
/// generation or invokes the legacy MV scan resolver.
fn resolve_frozen_mv_target_scan(
    node_id: i32,
    facts: &SqlScanPreparationFacts,
    query_table_bindings: Option<&QueryTableBindingStore>,
    lane: &str,
) -> Result<ResolvedScanExecution, String> {
    let target = facts.mv_target().ok_or_else(|| {
        format!("SQL MV {lane} scan node_id={node_id} has no immutable target facts")
    })?;
    let query_table_bindings = query_table_bindings.ok_or_else(|| {
        format!(
            "SQL MV {lane} scan node_id={node_id} has binding token but no query-local binding store"
        )
    })?;
    let binding = query_table_bindings.binding(facts.binding())?;
    let materialization = binding.mv_target_read.as_ref().ok_or_else(|| {
        format!(
            "SQL MV {lane} scan binding for '{}.{}.{}' has no frozen target materialization",
            facts.identity().catalog(),
            facts.identity().namespace(),
            facts.identity().table()
        )
    })?;
    if materialization.target_table_uuid != target.target_table_uuid()
        || materialization.frozen_snapshot_id != target.target_snapshot_id()
    {
        return Err(format!(
            "SQL MV {lane} scan node_id={node_id} target UUID or snapshot does not match its frozen binding"
        ));
    }
    let connector_read = match target.use_affected_partitions() {
        true => &materialization.affected_partitions,
        false => &materialization.full,
    };
    Ok(ResolvedScanExecution::AdmittedConnectorRead(
        connector_read.clone(),
    ))
}

fn validate_resolved_execution_kind(
    node_id: i32,
    facts: &SqlScanPreparationFacts,
    execution: &ResolvedScanExecution,
) -> Result<(), String> {
    let valid = match facts.category() {
        SqlScanPreparationCategory::ConnectorRead => {
            matches!(execution, ResolvedScanExecution::ConnectorRead)
        }
        SqlScanPreparationCategory::Delta => {
            matches!(execution, ResolvedScanExecution::AdmittedChangeWindow(_))
        }
        SqlScanPreparationCategory::AdmittedMetadata => {
            matches!(execution, ResolvedScanExecution::AdmittedSystemTable(_))
        }
        SqlScanPreparationCategory::AdmittedData
        | SqlScanPreparationCategory::AdmittedFrozenCurrent
        | SqlScanPreparationCategory::AdmittedFrozenSnapshot
        | SqlScanPreparationCategory::MvTargetState
        | SqlScanPreparationCategory::MvTargetLocator => {
            matches!(execution, ResolvedScanExecution::AdmittedConnectorRead(_))
        }
        SqlScanPreparationCategory::FrozenTimestampWithoutAdmittedSnapshot => false,
    };
    if valid {
        return Ok(());
    }
    let required = match facts.category() {
        SqlScanPreparationCategory::ConnectorRead => "ConnectorRead",
        SqlScanPreparationCategory::Delta => "AdmittedChangeWindow",
        SqlScanPreparationCategory::AdmittedMetadata => "AdmittedSystemTable",
        SqlScanPreparationCategory::AdmittedData
        | SqlScanPreparationCategory::AdmittedFrozenCurrent
        | SqlScanPreparationCategory::AdmittedFrozenSnapshot
        | SqlScanPreparationCategory::MvTargetState
        | SqlScanPreparationCategory::MvTargetLocator => "AdmittedConnectorRead",
        SqlScanPreparationCategory::FrozenTimestampWithoutAdmittedSnapshot => {
            "admitted frozen snapshot"
        }
    };
    Err(format!(
        "scan source {} node_id={node_id} requires {required} execution",
        facts.source_kind_label()
    ))
}

#[cfg(test)]
mod tests;
