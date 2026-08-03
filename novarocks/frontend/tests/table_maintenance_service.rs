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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use bytes::Bytes;
use novarocks::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceRequestContext,
    MaintenanceStatementResult, MaintenanceTarget, OptimizeSubmission, TableMaintenanceEngine,
    TableMaintenanceService,
};
use novarocks_frontend::table_maintenance::FrontendTableMaintenanceService;
use novarocks_frontend::table_maintenance::model::{
    MetadataMaintenanceExactOwner, MetadataMaintenanceOperationCreate,
    MetadataMaintenanceOperationKind, MetadataMaintenanceOperationState,
    MetadataMaintenancePlanPayload, OptimizeJobOutcome,
};
use novarocks_frontend::table_maintenance::repository::{
    MetadataMaintenanceOperationRepository, OptimizeJobRepository,
    metadata_maintenance_payload_digest,
};
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorMetadataMaintenanceOperation,
    ConnectorMetadataMaintenancePlan, ConnectorMetadataMaintenancePlanSummary,
    ConnectorMetadataMaintenancePlanningRequest, ConnectorMutationOperationId,
    ConnectorRequestContext, ConnectorTableHandle,
};
use novarocks_spi::state_store::{FeDeploymentView, StateStore};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use tempfile::TempDir;

#[derive(Default)]
struct FakeMaintenanceEngine {
    resolved_name_parts: Mutex<Vec<Vec<String>>>,
    guarded_targets: Mutex<Vec<MaintenanceTarget>>,
    requests: Mutex<Vec<MaintenanceActionRequest>>,
    recovered_plans: Mutex<Vec<ConnectorMetadataMaintenancePlan>>,
}

struct NotCancelled;

impl ConnectorCancellation for NotCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

impl FakeMaintenanceEngine {
    fn requests(&self) -> Vec<MaintenanceActionRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn resolved_name_parts(&self) -> Vec<Vec<String>> {
        self.resolved_name_parts.lock().unwrap().clone()
    }

    fn guarded_targets(&self) -> Vec<MaintenanceTarget> {
        self.guarded_targets.lock().unwrap().clone()
    }

    fn recovered_plans(&self) -> Vec<ConnectorMetadataMaintenancePlan> {
        self.recovered_plans.lock().unwrap().clone()
    }
}

impl TableMaintenanceEngine for FakeMaintenanceEngine {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String> {
        self.resolved_name_parts
            .lock()
            .unwrap()
            .push(name_parts.to_vec());
        let default_catalog = context.current_catalog.unwrap_or("default_catalog");
        match name_parts {
            [table] => Ok(target(default_catalog, context.current_database, table)),
            [namespace, table] => Ok(target(default_catalog, namespace, table)),
            [catalog, namespace, table] => Ok(target(catalog, namespace, table)),
            _ => Err(format!(
                "unsupported table name with {} parts",
                name_parts.len()
            )),
        }
    }

    fn reject_user_action_on_mv(&self, target: &MaintenanceTarget) -> Result<(), String> {
        self.guarded_targets.lock().unwrap().push(target.clone());
        if target.table == "mv_table" {
            Err("table maintenance is not allowed on materialized view storage table".to_string())
        } else {
            Ok(())
        }
    }

    fn current_snapshot_id(&self, _target: &MaintenanceTarget) -> Result<i64, String> {
        Ok(777)
    }

    fn execute_action(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        let outcome = match &request {
            MaintenanceActionRequest::RewriteDataFiles { .. } => {
                MaintenanceActionOutcome::RewriteDataFiles {
                    target_snapshot_id: Some(900),
                    rewritten_data_files_count: 2,
                    added_data_files_count: 1,
                    rewritten_bytes_count: 4096,
                    failed_data_files_count: 0,
                    removed_delete_files_count: 3,
                    output_record_count: 88,
                }
            }
            MaintenanceActionRequest::RewriteManifests { .. } => {
                MaintenanceActionOutcome::RewriteManifests {
                    rewritten_manifests_count: 4,
                    added_manifests_count: 2,
                }
            }
            MaintenanceActionRequest::ExpireSnapshots { .. } => {
                MaintenanceActionOutcome::ExpireSnapshots {
                    deleted_data_files_count: Some(1),
                    deleted_position_delete_files_count: Some(2),
                    deleted_equality_delete_files_count: None,
                    deleted_manifest_files_count: Some(3),
                    deleted_manifest_lists_count: Some(4),
                    deleted_statistics_files_count: None,
                }
            }
            MaintenanceActionRequest::RemoveOrphanFiles { .. } => {
                MaintenanceActionOutcome::RemoveOrphanFiles {
                    orphan_file_locations: vec![
                        "s3://warehouse/db/t/a.parquet".to_string(),
                        "s3://warehouse/db/t/b.parquet".to_string(),
                    ],
                }
            }
            MaintenanceActionRequest::RewritePositionDeleteFiles { .. } => {
                MaintenanceActionOutcome::RewritePositionDeleteFiles {
                    rewritten_delete_files_count: 5,
                    added_delete_files_count: 2,
                    rewritten_bytes_count: 8192,
                    added_bytes_count: 2048,
                }
            }
        };
        self.requests.lock().unwrap().push(request);
        Ok(outcome)
    }

    fn reconcile_metadata_maintenance(
        &self,
        _target: &MaintenanceTarget,
        plan: ConnectorMetadataMaintenancePlan,
    ) -> Result<novarocks::connector::metadata_maintenance::CompletedMetadataMaintenance, String>
    {
        self.recovered_plans.lock().unwrap().push(plan);
        Err("recorded exact generation is unavailable".to_string())
    }
}

fn target(catalog: &str, namespace: &str, table: &str) -> MaintenanceTarget {
    MaintenanceTarget {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
    }
}

fn context() -> MaintenanceRequestContext<'static> {
    MaintenanceRequestContext {
        current_catalog: Some("session_catalog"),
        current_database: "session_db",
    }
}

fn sqlite_config(path: &Path) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "table-maintenance-service-test".to_string(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "table-maintenance-service-fe".to_string(),
        },
    }
}

async fn open_sqlite(path: &Path) -> (StateStoreHost, Arc<dyn StateStore>) {
    let registry = builtin_state_store_provider_registry().expect("built-in provider registry");
    let host = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: sqlite_config(path),
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"table-maintenance-service-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite state store host");
    let store = host.state_store().expect("SQLite state store exposure");
    (host, store)
}

async fn durable_service() -> (
    TempDir,
    StateStoreHost,
    Arc<dyn StateStore>,
    FrontendTableMaintenanceService,
) {
    let temp = TempDir::new().expect("create temp directory");
    let (host, store) = open_sqlite(&temp.path().join("state.sqlite")).await;
    let service = FrontendTableMaintenanceService::open(
        Some(Arc::clone(&store)),
        tokio::runtime::Handle::current(),
    )
    .await
    .expect("open table-maintenance service");
    (temp, host, store, service)
}

fn connector_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(5),
        Arc::new(NotCancelled),
        1024,
        1024,
    )
    .expect("connector request context")
}

fn expect_ok(result: Option<MaintenanceStatementResult>) {
    assert!(matches!(result, Some(MaintenanceStatementResult::Ok)));
}

fn expect_query(
    result: Option<MaintenanceStatementResult>,
) -> novarocks::runtime::query_result::QueryResult {
    let Some(MaintenanceStatementResult::Query(result)) = result else {
        panic!("expected query result");
    };
    result
}

fn column_names(result: &novarocks::runtime::query_result::QueryResult) -> Vec<&str> {
    result
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn non_maintenance_sql_is_not_claimed() {
    let service = FrontendTableMaintenanceService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeMaintenanceEngine::default();

    assert!(
        service
            .try_handle_statement(&engine, "SELECT 1", context())
            .unwrap()
            .is_none()
    );
    assert!(engine.resolved_name_parts().is_empty());
    assert!(engine.requests().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_alter_statements_dispatch_typed_actions_and_optimize_only_enqueues() {
    let (_temp, _host, _store, service) = durable_service().await;
    let engine = FakeMaintenanceEngine::default();

    expect_ok(
        service
            .try_handle_statement(
                &engine,
                "ALTER TABLE ice.db.orders REWRITE MANIFESTS",
                context(),
            )
            .unwrap(),
    );
    expect_ok(
        service
            .try_handle_statement(
                &engine,
                "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS \
                 OLDER THAN '2026-01-01 00:00:00' RETAIN LAST 2",
                context(),
            )
            .unwrap(),
    );
    expect_ok(
        service
            .try_handle_statement(
                &engine,
                "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES OLDER THAN 1767225600000",
                context(),
            )
            .unwrap(),
    );
    expect_ok(
        service
            .try_handle_statement(&engine, "ALTER TABLE ice.db.orders OPTIMIZE", context())
            .unwrap(),
    );

    assert_eq!(
        engine.requests(),
        vec![
            MaintenanceActionRequest::RewriteManifests {
                target: target("ice", "db", "orders"),
                use_caching: None,
                spec_id: None,
            },
            MaintenanceActionRequest::ExpireSnapshots {
                target: target("ice", "db", "orders"),
                older_than_ms: Some(1_767_225_600_000),
                retain_last: Some(2),
            },
            MaintenanceActionRequest::RemoveOrphanFiles {
                target: target("ice", "db", "orders"),
                older_than_ms: 1_767_225_600_000,
            },
        ]
    );
    assert_eq!(engine.guarded_targets().len(), 4);

    let show = expect_query(
        service
            .try_handle_statement(
                &engine,
                "SHOW ALTER TABLE OPTIMIZE",
                MaintenanceRequestContext {
                    current_catalog: Some("ice"),
                    current_database: "db",
                },
            )
            .unwrap(),
    );
    assert_eq!(show.row_count(), 1);
    let batch = &show.chunks[0].batch;
    let states = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let snapshots = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(states.value(0), "PENDING");
    assert_eq!(snapshots.value(0), "777");
}

#[tokio::test(flavor = "multi_thread")]
async fn show_uses_repository_snapshot_and_preserves_legacy_wire_shape() {
    let (_temp, _host, store, service) = durable_service().await;
    let engine = FakeMaintenanceEngine::default();
    expect_ok(
        service
            .try_handle_statement(&engine, "ALTER TABLE ice.db.orders OPTIMIZE", context())
            .unwrap(),
    );

    let repository = OptimizeJobRepository::open(store).await.unwrap();
    let job = repository.list().await.unwrap().pop().unwrap();
    repository.claim(job.job_id, 200).await.unwrap().unwrap();
    repository
        .record_outcome(
            job.job_id,
            OptimizeJobOutcome {
                target_snapshot_id: Some(900),
                rewritten_data_files: 4,
                deleted_data_files: 3,
                added_data_files: 2,
                output_record_count: 88,
            },
        )
        .await
        .unwrap();
    repository.finish(job.job_id, 300).await.unwrap();

    let result = expect_query(
        service
            .try_handle_statement(
                &engine,
                "SHOW ALTER TABLE OPTIMIZE FROM ice.db \
                 WHERE TableName = 'orders' ORDER BY CreateTime DESC LIMIT 1",
                context(),
            )
            .unwrap(),
    );

    assert_eq!(
        column_names(&result),
        vec![
            "JobId",
            "TableName",
            "State",
            "CreateTime",
            "FinishTime",
            "Msg",
            "BaseSnapshotId",
            "TargetSnapshotId",
            "InputDataFiles",
            "OutputDataFiles",
            "InputDeleteFiles",
            "OutputDeleteFiles",
        ]
    );
    assert!(
        result
            .columns
            .iter()
            .all(|column| column.data_type == arrow::datatypes::DataType::Utf8 && !column.nullable)
    );
    let batch = &result.chunks[0].batch;
    let values = (0..batch.num_columns())
        .map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(values[0], "1");
    assert_eq!(values[1], "orders");
    assert_eq!(values[2], "FINISHED");
    assert!(values[3].parse::<i64>().unwrap() > 0);
    assert_eq!(values[4], "300");
    assert_eq!(
        values[5],
        "rewrote 4 data files and 3 delete files into 2 data files (88 rows)"
    );
    assert_eq!(&values[6..], ["777", "900", "4", "2", "3", "0"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_reconciles_only_the_persisted_exact_generation() {
    let (_temp, _host, store, service) = durable_service().await;
    let owner = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    };
    let operation_id = ConnectorMutationOperationId::from_bytes([9; 16]);
    let operation = ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(
        ConnectorTableHandle::try_new(owner.instance_id.clone(), Bytes::from_static(b"table"))
            .unwrap(),
    )
    .unwrap();
    let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
        operation_id,
        owner.clone(),
        operation,
        connector_context(),
    )
    .unwrap();
    let plan = ConnectorMetadataMaintenancePlan::try_new(
        &request,
        [3; 32],
        ConnectorMetadataMaintenancePlanSummary::new(1, 2, 3, 4, 5),
        Bytes::from_static(b"plan"),
    )
    .unwrap();
    let durable_id = uuid::Uuid::from_bytes(operation_id.to_bytes());
    let repository = MetadataMaintenanceOperationRepository::open(store)
        .await
        .unwrap();
    repository
        .create(MetadataMaintenanceOperationCreate {
            operation_id: durable_id,
            target: target("ice", "db", "orders"),
            owner: MetadataMaintenanceExactOwner {
                instance_id: owner.instance_id.as_str().to_string(),
                incarnation_id: uuid::Uuid::from_bytes(owner.incarnation.to_bytes()),
            },
            kind: MetadataMaintenanceOperationKind::RewriteMetadataLayout,
            request_digest: plan.request_digest(),
            request_payload_digest: metadata_maintenance_payload_digest(plan.provider_payload()),
            base_state_digest: plan.state_digest(),
            request_payload: plan.provider_payload().to_vec(),
            created_at_ms: 1,
        })
        .await
        .unwrap();
    repository
        .start(
            durable_id,
            MetadataMaintenancePlanPayload {
                plan_digest: plan.plan_digest(),
                payload_digest: metadata_maintenance_payload_digest(plan.provider_payload()),
                payload: plan.provider_payload().to_vec(),
                summary: [
                    plan.summary().source_items(),
                    plan.summary().replacement_items(),
                    plan.summary().candidate_versions(),
                    plan.summary().cleanup_candidates(),
                    plan.summary().total_bytes(),
                ],
            },
            2,
        )
        .await
        .unwrap();

    let engine = Arc::new(FakeMaintenanceEngine::default());
    let engine_port: Arc<dyn TableMaintenanceEngine> = engine.clone();
    service.start(engine_port).unwrap();

    let recovered = engine.recovered_plans();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].owner(), &owner);
    assert_eq!(recovered[0].operation_id(), operation_id);
    assert!(engine.requests().is_empty());
    assert_eq!(
        repository.list().await.unwrap()[0].state,
        MetadataMaintenanceOperationState::Unresolved
    );
    service.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn spark_procedures_dispatch_typed_actions_and_preserve_result_schemas() {
    let service = FrontendTableMaintenanceService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeMaintenanceEngine::default();
    let cases = [
        (
            "CALL ice.system.rewrite_data_files(\
             table => 'db.orders', options => map('rewrite-all', 'true'))",
            vec![
                "rewritten_data_files_count",
                "added_data_files_count",
                "rewritten_bytes_count",
                "failed_data_files_count",
                "removed_delete_files_count",
            ],
        ),
        (
            "CALL ice.system.rewrite_manifests(\
             table => 'db.orders', use_caching => false, spec_id => 7)",
            vec!["rewritten_manifests_count", "added_manifests_count"],
        ),
        (
            "CALL ice.system.expire_snapshots(\
             table => 'db.orders', older_than => TIMESTAMP '2026-01-01 00:00:00', \
             retain_last => 2)",
            vec![
                "deleted_data_files_count",
                "deleted_position_delete_files_count",
                "deleted_equality_delete_files_count",
                "deleted_manifest_files_count",
                "deleted_manifest_lists_count",
                "deleted_statistics_files_count",
            ],
        ),
        (
            "CALL ice.system.rewrite_position_delete_files(\
             table => 'db.orders', options => map('rewrite-all', 'true'), \
             where => 'id > 10')",
            vec![
                "rewritten_delete_files_count",
                "added_delete_files_count",
                "rewritten_bytes_count",
                "added_bytes_count",
            ],
        ),
    ];

    for (sql, expected_columns) in cases {
        let result = expect_query(
            service
                .try_handle_statement(&engine, sql, context())
                .unwrap(),
        );
        assert_eq!(column_names(&result), expected_columns);
    }

    assert_eq!(
        engine.resolved_name_parts(),
        vec![vec!["ice".to_string(), "db".to_string(), "orders".to_string()]; 4]
    );
    assert_eq!(
        engine.requests(),
        vec![
            MaintenanceActionRequest::RewriteDataFiles {
                target: target("ice", "db", "orders"),
                base_snapshot_id: 777,
                job_id: None,
                options: BTreeMap::from([("rewrite-all".to_string(), "true".to_string())]),
                branch: None,
                where_clause: None,
            },
            MaintenanceActionRequest::RewriteManifests {
                target: target("ice", "db", "orders"),
                use_caching: Some(false),
                spec_id: Some(7),
            },
            MaintenanceActionRequest::ExpireSnapshots {
                target: target("ice", "db", "orders"),
                older_than_ms: Some(1_767_225_600_000),
                retain_last: Some(2),
            },
            MaintenanceActionRequest::RewritePositionDeleteFiles {
                target: target("ice", "db", "orders"),
                options: BTreeMap::from([("rewrite-all".to_string(), "true".to_string())]),
                where_clause: Some("id > 10".to_string()),
            },
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_orphan_files_requires_frontend_state_store_before_dispatch() {
    let service = FrontendTableMaintenanceService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeMaintenanceEngine::default();

    let error = service
        .try_handle_statement(
            &engine,
            "CALL ice.system.remove_orphan_files(\
             table => 'db.orders', older_than => TIMESTAMP '2026-01-01 00:00:00')",
            context(),
        )
        .unwrap_err();

    assert_eq!(
        error,
        "connector orphan cleanup requires frontend StateStore"
    );
    assert!(engine.requests().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn action_exclusive_options_do_not_leak_between_variants() {
    let service = FrontendTableMaintenanceService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeMaintenanceEngine::default();

    let error = service
        .try_handle_statement(
            &engine,
            "CALL ice.system.rewrite_manifests(\
             table => 'db.orders', options => map('rewrite-all', 'true'))",
            context(),
        )
        .unwrap_err();
    assert_eq!(
        error,
        "unsupported argument `options` for Iceberg system procedure `rewrite_manifests`"
    );
    assert!(engine.requests().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn user_actions_are_rejected_by_mv_guard_before_dispatch() {
    let service = FrontendTableMaintenanceService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeMaintenanceEngine::default();

    let error = service
        .try_handle_statement(
            &engine,
            "CALL ice.system.rewrite_manifests(table => 'db.mv_table')",
            context(),
        )
        .unwrap_err();

    assert_eq!(
        error,
        "table maintenance is not allowed on materialized view storage table"
    );
    assert_eq!(
        engine.guarded_targets(),
        vec![target("ice", "db", "mv_table")]
    );
    assert!(engine.requests().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_calls_bypass_sql_parser_but_use_typed_engine_and_duplicate_result() {
    let (_temp, _host, _store, service) = durable_service().await;
    let engine = FakeMaintenanceEngine::default();
    let mv_target = target("ice", "db", "mv_table");
    let request = MaintenanceActionRequest::RewriteManifests {
        target: mv_target.clone(),
        use_caching: Some(true),
        spec_id: None,
    };

    assert_eq!(
        service
            .execute_automatic_action(&engine, request.clone())
            .unwrap(),
        MaintenanceActionOutcome::RewriteManifests {
            rewritten_manifests_count: 4,
            added_manifests_count: 2,
        }
    );
    assert_eq!(engine.requests(), vec![request]);
    assert!(engine.guarded_targets().is_empty());

    assert_eq!(
        service
            .submit_automatic_optimize(&engine, mv_target.clone())
            .unwrap(),
        OptimizeSubmission::Submitted { job_id: 1 }
    );
    assert_eq!(
        service
            .submit_automatic_optimize(&engine, mv_target)
            .unwrap(),
        OptimizeSubmission::AlreadyActive
    );
    assert!(engine.guarded_targets().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_state_store_only_blocks_repository_backed_operations() {
    let service = FrontendTableMaintenanceService::open(None, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let engine = FakeMaintenanceEngine::default();

    expect_ok(
        service
            .try_handle_statement(
                &engine,
                "ALTER TABLE ice.db.orders REWRITE MANIFESTS",
                context(),
            )
            .unwrap(),
    );
    service
        .execute_automatic_action(
            &engine,
            MaintenanceActionRequest::RewriteManifests {
                target: target("ice", "db", "orders"),
                use_caching: None,
                spec_id: None,
            },
        )
        .unwrap();

    let resolved_before_optimize = engine.resolved_name_parts().len();
    let guarded_before_optimize = engine.guarded_targets().len();
    let optimize = service
        .try_handle_statement(
            &engine,
            "ALTER TABLE too.many.name.parts OPTIMIZE",
            context(),
        )
        .unwrap_err();
    let show = service
        .try_handle_statement(&engine, "SHOW ALTER TABLE OPTIMIZE", context())
        .unwrap_err();
    let automatic = service
        .submit_automatic_optimize(&engine, target("ice", "db", "orders"))
        .unwrap_err();
    assert_eq!(
        optimize,
        "ALTER TABLE OPTIMIZE requires frontend StateStore"
    );
    assert_eq!(engine.resolved_name_parts().len(), resolved_before_optimize);
    assert_eq!(engine.guarded_targets().len(), guarded_before_optimize);
    assert!(show.contains("StateStore"), "{show}");
    assert!(automatic.contains("StateStore"), "{automatic}");
}

#[tokio::test(flavor = "multi_thread")]
async fn user_duplicate_optimize_remains_a_compatible_string_error() {
    let (_temp, _host, _store, service) = durable_service().await;
    let engine = FakeMaintenanceEngine::default();
    expect_ok(
        service
            .try_handle_statement(&engine, "ALTER TABLE ice.db.orders OPTIMIZE", context())
            .unwrap(),
    );

    let error = service
        .try_handle_statement(&engine, "ALTER TABLE ice.db.orders OPTIMIZE", context())
        .unwrap_err();

    assert!(error.starts_with("ALTER TABLE OPTIMIZE: create iceberg optimize job failed:"));
    assert!(error.contains("ice.db.orders"));
    assert!(error.contains("active job"));
}
