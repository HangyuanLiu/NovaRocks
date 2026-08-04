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

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;

use super::super::boundary_schema::{BoundaryKind, BoundarySchemaColumn, project_boundary_reports};
use super::*;
use crate::connector::ConnectorRegistry;
use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{ExprKind, OutputColumn as AnalysisOutputColumn, TypedExpr};
use crate::sql::catalog::PlannerTableProvider;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    BoundaryContract, BoundaryKind as PlannerBoundaryKind, DataPartition, DistributedNode,
    DistributedNodeKind, ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind,
    FragmentId, FragmentStreamKind, PartitionKind, PlanFragment,
};
use crate::sql::planner::payload::{PlanScanNode, PlanValuesNode};
use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
use crate::sql::planner::table::{ScanSource, SqlScanSource, SqlTableIdentity, TableDef};
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};

struct EmptyCatalog;

impl PlannerTableProvider for EmptyCatalog {
    fn resolve_table_for_analysis(
        &self,
        _catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
        Err(format!("unexpected table lookup {database}.{table}"))
    }
}

fn stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 0.0,
        row_count_confidence: PlannerConfidence::Fallback,
        column_statistics: HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn output_col(id: u32, name: &str) -> AnalysisOutputColumn {
    AnalysisOutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    }
}

fn physical_values_node(
    fragment_id: FragmentId,
    node_id: i32,
    columns: Vec<AnalysisOutputColumn>,
) -> DistributedNode {
    DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: DistributedNodeKind::Values(PlanValuesNode {
            rows: Vec::new(),
            columns,
        }),
    }
}

fn iceberg_schema_field(
    field_id: i32,
    name: &str,
) -> crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
    crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
        field_id,
        name: name.to_string(),
        initial_default: None,
        write_default: None,
        initial_default_json: None,
        write_default_json: None,
        children: Vec::new(),
    }
}

fn iceberg_table_info() -> crate::connector::iceberg::scan_model::IcebergTableInfo {
    crate::connector::iceberg::scan_model::IcebergTableInfo {
        catalog: "test_catalog".to_string(),
        namespace: "test_db".to_string(),
        table: "test_table".to_string(),
        table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        current_snapshot_id: Some(7),
        schema_id: 1,
        location: "s3://bucket/test_table".to_string(),
        schema: crate::connector::iceberg::scan_model::IcebergSchemaDef {
            fields: vec![
                iceberg_schema_field(1, "id"),
                iceberg_schema_field(3, "category"),
            ],
        },
        serialized_metadata: None,
        serialized_metadata_rows: None,
    }
}

/// Build-only tests model the application admission boundary explicitly: the
/// sealed SQL source carries a token, while the exact provider lease and scan
/// facts stay in a request-local binding store.
pub(super) fn fixture_query_table_bindings(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
) -> Option<crate::engine::query_planning::bindings::QueryTableBindingStore> {
    use crate::engine::query_planning::bindings::{
        QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
    };

    let scan = plan
        .fragments()
        .iter()
        .find_map(|fragment| match &fragment.root.payload {
            DistributedNodeKind::Scan(scan) => Some(scan),
            _ => None,
        })?;
    let ScanSource::Sql(source) = &scan.table.source;
    let planning_lease = controls
        .acquire_current(
            &ConnectorInstanceId::parse(&source.table.catalog)
                .expect("fixture catalog must be a valid connector instance"),
        )
        .ok();
    let source = source.clone();
    let planner = scan.table.clone();
    let store = QueryTableBindingStore::try_new_with_scope_for_test(
        NonZeroU64::new(1).expect("fixture scope"),
    );
    store
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base("test_catalog", "test_db", "test_table"),
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
                Ok(QueryTableBinding {
                    resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                        Some(&source.table.catalog),
                        &source.table.namespace,
                        resolved_planner,
                    ),
                    statistics_pin: None,
                    planning_lease: planning_lease.clone(),
                    scan_materialization: Some(QueryScanMaterialization::IcebergDataFiles {
                        table: iceberg_table_info(),
                        files: Vec::new(),
                        binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                    }),
                    frozen_snapshot_files: std::collections::BTreeMap::new(),
                    delta_runtime_plans: std::collections::BTreeMap::new(),
                })
            },
        )
        .expect("fixture query binding");
    Some(store)
}

fn iceberg_scan_plan(required_columns: Option<Vec<&str>>) -> DistributedPlan {
    iceberg_scan_plan_with_outputs(required_columns, &["id"])
}

fn iceberg_scan_plan_with_outputs(
    required_columns: Option<Vec<&str>>,
    output_names: &[&str],
) -> DistributedPlan {
    let id = AnalysisOutputColumn {
        column_id: ColumnId::new_for_test(1),
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: false,
    };
    let category = AnalysisOutputColumn {
        column_id: ColumnId::new_for_test(3),
        name: "category".to_string(),
        data_type: DataType::Utf8,
        nullable: true,
        is_internal: false,
    };
    let all_outputs = [id, category];
    let output_columns = output_names
        .iter()
        .map(|name| {
            all_outputs
                .iter()
                .find(|column| column.name == *name)
                .unwrap_or_else(|| panic!("unknown Iceberg scan test output {name}"))
                .clone()
        })
        .collect::<Vec<_>>();
    let table = TableDef {
        name: "ice_t".to_string(),
        columns: vec![
            novarocks_catalog::schema::ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            novarocks_catalog::schema::ColumnDef {
                name: "category".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ],
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::Data {
                version: crate::sql::planner::table::SqlTableVersionSelector::Current,
            },
        ),
    };
    let scan = DistributedNode {
        node_id: 10,
        fragment_id: 0,
        tuple_ids: vec![10],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table,
            alias: None,
            columns: output_columns.clone(),
            predicates: Vec::new(),
            required_columns: required_columns
                .map(|columns| columns.into_iter().map(str::to_string).collect()),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
    };
    crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: scan,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Result,
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    }
}

fn native_file_range(
    range: &crate::runtime::scan_range::ScanRangeParams,
) -> &crate::runtime::scan_range::FileScanRange {
    match &range.range {
        crate::runtime::scan_range::ScanRange::File(file) => file,
        crate::runtime::scan_range::ScanRange::BrokerFile(_) => {
            panic!("expected native file range, got StarRocks broker-file range")
        }
        crate::runtime::scan_range::ScanRange::SchemaSelection(_) => {
            panic!("expected native file range, got StarRocks schema selection")
        }
    }
}

fn stream_exchange_plan(flavor: ExchangeFlavor) -> DistributedPlan {
    let columns = vec![output_col(1, "k")];
    let producer_fragment_id = 1;
    let consumer_fragment_id = 0;
    let exchange_node_id = 20;
    let producer_fragment = PlanFragment {
        fragment_id: producer_fragment_id,
        root: physical_values_node(producer_fragment_id, 10, columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Noop,
        output_exprs: None,
        output_columns: columns.clone(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let consumer_fragment = PlanFragment {
        fragment_id: consumer_fragment_id,
        root: DistributedNode {
            node_id: exchange_node_id,
            fragment_id: consumer_fragment_id,
            tuple_ids: vec![exchange_node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: producer_fragment_id,
                output_columns: columns.clone(),
                output_qualifier: None,
                flavor,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: crate::sql::planner::distributed::DataSink::Result,
        output_exprs: None,
        output_columns: columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![producer_fragment, consumer_fragment],
        root_fragment_id: consumer_fragment_id,
        runtime_filter_graph: Default::default(),
        edges: vec![FragmentEdge {
            source_fragment_id: producer_fragment_id,
            target_fragment_id: consumer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![1],
        }],
    }
}

fn finalized_router_plan() -> DistributedPlan {
    let output_columns = vec![
        output_col(1, "op"),
        output_col(2, "route"),
        output_col(3, "delete_id"),
    ];
    let dp = crate::sql::planner::distributed::test_support::distributed_plan_draft_builder_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: physical_values_node(0, 10, output_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::distributed::DataSink::Result,
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };
    let mut branch =
        crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
    branch.output_partition_ordinals = vec![2];
    let dag =
        crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![branch],
        );
    crate::sql::planner::distributed::write::plan::finalize_sql_change_stream_test_plan(dp, dag)
        .expect("plan change-stream write")
}

mod boundary;
mod preparation;
mod scan;
mod topology;
