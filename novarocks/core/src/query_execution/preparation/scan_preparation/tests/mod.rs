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

mod dispatch;
mod iceberg;
mod projection;

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;

use crate::connector::ConnectorRegistry;
use crate::connector::scan_model::{FixtureDeleteFile, FixtureScanFile};
use crate::query_execution::preparation::scan::{
    ResolvedReadReason, ResolvedScanExecution, ScanBindingResolver,
};
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, PlanFragment,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

fn prepare_scan_bindings(
    plan: &DistributedPlan,
    connectors: &ConnectorRegistry,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    prepare_scan_bindings_with_controls(plan, &controls, resolver)
}

/// Prepare a tokenized SQL scan against a caller-owned control resolver, for
/// tests that must hold the same resolver across admission and preparation.
fn prepare_scan_bindings_with_controls(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let query_bindings = fixture_query_table_bindings(plan, controls);
    super::prepare_scan_bindings(
        plan,
        controls,
        &crate::connector::test_request_context(),
        Some(&query_bindings),
        resolver,
        super::ScanPreparationOptions::single_backend_fixture(),
    )
}

/// The shared fixture deliberately allocates the same token that the SQL
/// test scan carrier embeds. Concrete read units remain in the provider-owned
/// opaque handle, never in `TableDef::source`.
fn fixture_query_table_bindings(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
) -> crate::query_execution::planning::bindings::QueryTableBindingStore {
    use crate::query_execution::planning::bindings::{
        QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
    };
    use crate::sql::planner::table::{SqlScanKind, SqlScanSource, SqlTableIdentity};
    use novarocks_spi::connector::{
        ConnectorControlResolver, ConnectorInstanceId, ConnectorTableIdentity,
        ConnectorTableRequest, ConnectorTableResolution,
    };

    let scan = plan
        .fragments()
        .iter()
        .find_map(|fragment| match &fragment.root.payload {
            DistributedNodeKind::Scan(scan) => Some(scan),
            _ => None,
        })
        .expect("shared fixture plan must have a root scan");
    let ScanSource::Sql(source) = &scan.table.source;
    let store = QueryTableBindingStore::try_new_with_scope_for_test(
        NonZeroU64::new(1).expect("fixture scope"),
    );
    if matches!(source.kind, SqlScanKind::ConnectorRead) {
        // This source kind is supplied by its dedicated resolver tests;
        // no catalog admission is expected before resolver dispatch.
        return store;
    }
    let planning_lease = controls
        .acquire_current(
            &ConnectorInstanceId::parse(&source.table.catalog)
                .expect("fixture catalog must be a valid connector instance"),
        )
        .ok();
    if planning_lease.is_none() && matches!(&source.kind, SqlScanKind::Delta { .. }) {
        // Resolver-only negative tests deliberately omit connector admission so
        // they can assert the resolver error before generic read planning.
        return store;
    }
    let source = source.clone();
    let planner = scan.table.clone();

    store
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base(
                &source.table.catalog,
                &source.table.namespace,
                &source.table.table,
            ),
            |binding| {
                let mut resolved_planner = planner.clone();
                resolved_planner.source = ScanSource::Sql(SqlScanSource::new(
                    binding,
                    SqlTableIdentity {
                        catalog: source.table.catalog.clone(),
                        namespace: source.table.namespace.clone(),
                        table: source.table.table.clone(),
                    },
                    source.kind.clone(),
                ));
                let lease = planning_lease.clone().ok_or_else(|| {
                    "scan fixture must acquire an exact connector lease".to_string()
                })?;
                let metadata = lease
                    .binding()
                    .metadata()
                    .load_table(ConnectorTableRequest {
                        table: ConnectorTableIdentity {
                            instance_id: ConnectorInstanceId::parse(&source.table.catalog)
                                .expect("fixture catalog must be valid"),
                            namespace: Arc::from(source.table.namespace.as_str()),
                            table: Arc::from(source.table.table.as_str()),
                        },
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        context: crate::connector::test_request_context(),
                    })
                    .map_err(|error| error.to_string())?;
                let scan_materialization = QueryScanMaterialization {
                    table: metadata.table,
                    schema: metadata.schema,
                    selector: novarocks_spi::connector::ConnectorReadSelector::Current,
                    statistics_pin: None,
                    planning_lease: lease.clone(),
                };
                let frozen_snapshot_materializations = match &source.kind {
                    SqlScanKind::FrozenInputSet {
                        version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(
                            snapshot_id,
                        ),
                    } => {
                        let lease = planning_lease.clone().ok_or_else(|| {
                            "frozen scan fixture must acquire an exact connector lease".to_string()
                        })?;
                        let metadata = lease
                            .binding()
                            .metadata()
                            .load_table(ConnectorTableRequest {
                                table: ConnectorTableIdentity {
                                    instance_id: ConnectorInstanceId::parse(&source.table.catalog)
                                        .expect("fixture catalog must be valid"),
                                    namespace: Arc::from(source.table.namespace.as_str()),
                                    table: Arc::from(source.table.table.as_str()),
                                },
                                resolution: ConnectorTableResolution::StrictBaseTable,
                                context: crate::connector::test_request_context(),
                            })
                            .map_err(|error| error.to_string())?;
                        std::collections::BTreeMap::from([(
                            *snapshot_id,
                            QueryScanMaterialization {
                                table: metadata.table,
                                schema: metadata.schema,
                                selector: novarocks_spi::connector::ConnectorReadSelector::SnapshotId(
                                    *snapshot_id,
                                ),
                                statistics_pin: None,
                                planning_lease: lease,
                            },
                        )])
                    }
                    _ => std::collections::BTreeMap::new(),
                };
                Ok(QueryTableBinding {
                    resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                        Some(&source.table.catalog),
                        &source.table.namespace,
                        resolved_planner,
                    ),
                    statistics_pin: None,
                    admission: planning_lease
                        .clone()
                        .map(crate::query_execution::planning::bindings::QueryTableBindingAdmission::Exact)
                        .unwrap_or(crate::query_execution::planning::bindings::QueryTableBindingAdmission::Local),
                    scan_materialization: Some(scan_materialization.clone()),
                    mv_target_read: match &source.kind {
                        SqlScanKind::MvTargetState { facts } => Some(
                            crate::query_execution::planning::bindings::MvTargetReadAdmission {
                                full: scan_materialization.clone(),
                                affected_partitions: scan_materialization.clone(),
                                target_table_uuid: facts.target_table_uuid.clone(),
                                frozen_snapshot_id: facts.target_snapshot_id,
                            },
                        ),
                        SqlScanKind::MvTargetLocator { facts } => Some(
                            crate::query_execution::planning::bindings::MvTargetReadAdmission {
                                full: scan_materialization.clone(),
                                affected_partitions: scan_materialization.clone(),
                                target_table_uuid: facts.target_table_uuid.clone(),
                                frozen_snapshot_id: facts.target_snapshot_id,
                            },
                        ),
                        _ => None,
                    },
                    write_target_admission: None,
                    frozen_snapshot_materializations,
                    admitted_change_scans: std::collections::BTreeMap::new(),
                })
            },
        )
        .expect("fixture query binding");
    store
}

struct StaticResolver {
    execution: ResolvedScanExecution,
}

impl ScanBindingResolver for StaticResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Ok(Some(self.execution.clone()))
    }
}

fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type,
        nullable,
        is_internal: false,
    }
}

fn source_column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default: None,
        logical_type: None,
    }
}

/// The SQL table identity that [`crate::sql::planner::table::test_sql_scan_source`]
/// embeds, restated so tests can admit a binding for the same three-part name
/// without naming a provider.
struct FixtureTableIdentity {
    catalog: String,
    namespace: String,
    table: String,
}

fn fixture_table_identity() -> FixtureTableIdentity {
    FixtureTableIdentity {
        catalog: "test_catalog".to_string(),
        namespace: "test_db".to_string(),
        table: "test_table".to_string(),
    }
}

fn data_file(path: &str) -> FixtureScanFile {
    let mut file = FixtureScanFile::new(path);
    file.partition_spec_id = Some(0);
    file.sequence_number = Some(1);
    file
}

fn equality_delete_file(
    equality_column_names: Vec<&str>,
    equality_field_ids: Vec<i32>,
) -> FixtureDeleteFile {
    FixtureDeleteFile::equality(
        "s3://bucket/eq-delete.parquet",
        &equality_column_names,
        &equality_field_ids,
    )
}

fn scan_node(node_id: i32) -> DistributedNode {
    let output = column(1, "id", DataType::Int32, false);
    let table = TableDef {
        name: "ice_t".to_string(),
        columns: vec![source_column("id", DataType::Int32, false)],
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::Data {
                version: crate::sql::planner::table::SqlTableVersionSelector::Current,
            },
        ),
    };
    DistributedNode {
        node_id,
        fragment_id: 0,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: PhysicalPlanStats {
            output_row_count: 10.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table,
            alias: None,
            columns: vec![output],
            predicates: Vec::new(),
            required_columns: Some(vec!["id".to_string()]),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
    }
}

fn plan(root: DistributedNode) -> DistributedPlan {
    crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: vec![column(1, "id", DataType::Int32, false)],
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    }
}

fn registry(files: Vec<FixtureScanFile>) -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        None,
    );
    registry
}

/// Register read units per table name, so a test can plan a scan against a
/// table the fixture deliberately has no units for.
fn registry_for_tables(files_by_table: HashMap<String, Vec<FixtureScanFile>>) -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    crate::connector::scan_model::register_planned_table_files_fixture(
        &registry,
        "test_catalog",
        files_by_table,
        None,
    );
    registry
}

fn recording_registry(
    files: Vec<FixtureScanFile>,
) -> (ConnectorRegistry, Arc<Mutex<Vec<Vec<usize>>>>) {
    let seen_projections = Arc::new(Mutex::new(Vec::new()));
    let registry = ConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        Some(Arc::clone(&seen_projections)),
    );
    (registry, seen_projections)
}

/// Seal a change-window scan on the neutral read fixture, the way an
/// application does while it still holds the exact lease.
///
/// The scan is minted from its own binding of the same catalog. That is enough
/// for preparation to accept it, because the fixture pins one incarnation per
/// catalog, and it keeps the sealed handle decodable by whichever registration
/// the test later plans against.
fn fixture_sealed_change_scan(
    catalog: &str,
    table: &str,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> novarocks_spi::connector::ConnectorScan {
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorChangeWindow,
        ConnectorControlPlanningLease, ConnectorInstanceId, ConnectorReadPurpose,
        ConnectorScanSelection, ConnectorTableIdentity, ConnectorTableRequest,
        ConnectorTableResolution,
    };

    let lease = ConnectorControlPlanningLease::new(
        Arc::new(crate::connector::scan_model::planned_files_fixture_binding(
            catalog,
            HashMap::new(),
            None,
        )),
        || {},
    );
    let context = crate::connector::test_request_context();
    let metadata = lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse(catalog).expect("fixture instance ID"),
                namespace: Arc::from("db"),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .expect("fixture table metadata");
    let projection = (0..metadata.schema.fields().len()).collect();
    lease
        .binding()
        .planning()
        .begin_scan(
            &metadata.table,
            ConnectorBeginScanRequest {
                projection,
                static_predicates: Vec::new(),
                selection: ConnectorScanSelection::ChangeWindow(ConnectorChangeWindow::new(
                    from_snapshot_id,
                    to_snapshot_id,
                )),
                purpose: ConnectorReadPurpose::Query,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: std::num::NonZeroUsize::new(4096).expect("nonzero rows"),
                    max_bytes: std::num::NonZeroUsize::new(context.max_handle_payload_bytes())
                        .expect("nonzero bytes"),
                },
                context,
            },
        )
        .expect("fixture change-window scan")
}

fn resolved_delta() -> ResolvedScanExecution {
    ResolvedScanExecution::SealedConnectorScan(fixture_sealed_change_scan(
        "test_catalog",
        "orders",
        6,
        7,
    ))
}

fn resolved_data_delta() -> ResolvedScanExecution {
    resolved_delta()
}

fn replace_scan_source(root: &mut DistributedNode, source: ScanSource) {
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table.source = source;
}
