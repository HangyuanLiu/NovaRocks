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

use bytes::Bytes;
use novarocks_frontend::OperationId;
use novarocks_frontend::dml::{DmlErrorKind, DmlOperationId};
use novarocks_frontend::state_store::coordination::{ControlPlaneMode, IncarnationGate};
use novarocks_frontend::view::repository::database_key;
use novarocks_frontend::view::{
    CreateExternalViewRequest, ExternalViewResolution, ResolvedExternalView, ViewColumnDefinition,
    ViewEngine, ViewRequestContext, ViewService, ViewSqlDialect, ViewStatementResult, ViewTarget,
};
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
    FrontendApplicationHost, FrontendExecutionConfig,
};
use novarocks_parser::parse as parse_typed_statement;
use novarocks_spi::state_store::{CommitOutcome, Key, Precondition, TransactionId, Value};
use sqlparser::ast::{Query, Statement};
use sqlparser::parser::Parser;
use std::sync::Arc;
use std::time::Duration;
mod common;
use common::state_store_fixture;
use tempfile::TempDir;
use uuid::Uuid;

fn execution_config() -> FrontendExecutionConfig {
    FrontendExecutionConfig::new("127.0.0.1", 19090, std::num::NonZeroUsize::new(1).unwrap())
}

async fn open_host(
    input: Option<novarocks_frontend::StateStoreHostInput>,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    let registry = state_store_fixture::registry();
    FrontendApplicationHost::open_with_factories_and_state_store_registry(
        input,
        &registry,
        execution_config(),
        backend_config(),
        Vec::new(),
        tokio::runtime::Handle::current(),
    )
    .await
}

fn backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks_types::ClusterRole::AllInOne,
        Vec::new(),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid all-in-one backend config")
}

fn fe_backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks_types::ClusterRole::Fe,
        Vec::new(),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid FE backend config")
}

struct SessionViewEngine;

impl ViewEngine for SessionViewEngine {
    fn resolve_external_view(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ExternalViewResolution, String> {
        unreachable!("session view must not resolve external views")
    }

    fn create_external_view(
        &self,
        _request: CreateExternalViewRequest,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), String> {
        unreachable!("session view must not create external views")
    }

    fn drop_external_view(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
        _policy: novarocks_spi::connector::DropPolicy,
    ) -> Result<(), String> {
        unreachable!("session view must not drop external views")
    }

    fn load_external_view(
        &self,
        _target: &ViewTarget,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, String> {
        Ok(None)
    }

    fn list_external_views(
        &self,
        _catalog: &str,
        _database: &str,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<String>, String> {
        unreachable!("session view must not list external views")
    }

    fn analyze_external_view(
        &self,
        _catalog: &str,
        _database: &str,
        _query: &Query,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, String> {
        unreachable!("session view must not analyze external views")
    }
}

fn view_context() -> ViewRequestContext<'static> {
    ViewRequestContext {
        current_catalog: None,
        current_database: "db",
        connector_context: None,
    }
}

fn execute_view_statement(
    service: &dyn ViewService,
    engine: &dyn ViewEngine,
    sql: &str,
    context: ViewRequestContext<'_>,
) -> Result<ViewStatementResult, String> {
    let statements = parse_typed_statement(sql).map_err(|error| error.to_string())?;
    let [novarocks_parser::ast::Statement::View(statement)] = statements.as_slice() else {
        return Err("expected typed View statement".to_string());
    };
    service.execute_statement(engine, statement, context)
}

fn parse_query(sql: &str) -> Box<Query> {
    let mut parser = Parser::new(&ViewSqlDialect).try_with_sql(sql).unwrap();
    match parser.parse_statement().unwrap() {
        Statement::Query(query) => query,
        other => panic!("expected query, got {other:?}"),
    }
}

fn state_store_input() -> novarocks_frontend::StateStoreHostInput {
    state_store_fixture::input(format!("frontend-cluster-{}", Uuid::now_v7()))
}

fn sqlite_config(_temp: &TempDir) -> novarocks_frontend::StateStoreHostInput {
    state_store_input()
}

#[tokio::test]
async fn host_exposes_one_statistics_service_identity() {
    let host = open_host(None).await.expect("host");
    let first = host.statistics_application_service();
    let second = host.statistics_application_service();
    assert!(Arc::ptr_eq(&first, &second));
    let first_application = host.statistics_application_service();
    let second_application = host.statistics_application_service();
    assert!(Arc::ptr_eq(&first_application, &second_application));
    drop(first_application);
    drop(second_application);
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn host_exposes_one_dml_service_identity() {
    let host = open_host(None).await.expect("host");
    let first = host.dml_service();
    let second = host.dml_service();
    assert!(Arc::ptr_eq(&first, &second));
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn absent_state_store_builds_dml_service_with_disabled_journal() {
    let host = open_host(None).await.expect("host");
    let error = host
        .dml_service()
        .load_operation(DmlOperationId::new_v7())
        .expect_err("disabled DML journal must reject operation access");
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(
        error
            .to_string()
            .contains("state store is required for Iceberg DML"),
        "{error}"
    );
    host.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_reopens_dml_journal_after_shutdown() {
    let config = state_store_input();
    let host = open_host(Some(config.clone())).await.expect("first host");
    assert!(
        host.dml_service()
            .list_unfinished_operations()
            .expect("first DML journal")
            .is_empty()
    );
    host.shutdown().await.expect("first shutdown");

    let reopened = open_host(Some(config)).await.expect("reopened host");
    assert!(
        reopened
            .dml_service()
            .list_unfinished_operations()
            .expect("reopened DML journal")
            .is_empty()
    );
    reopened.shutdown().await.expect("reopened shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_bootstraps_once_and_preserves_reconciling_mode() {
    let config = state_store_input();
    let host = open_host(Some(config.clone())).await.expect("first host");
    let store = host.state_store().expect("configured StateStore");
    let gate = IncarnationGate::new(store);
    let initial = gate.load().await.expect("bootstrapped control plane");
    assert_eq!(initial.mode(), ControlPlaneMode::WriteOpen);
    let reconciling = gate
        .begin_restore(&initial, OperationId::new_v7())
        .await
        .expect("close writes for restore");
    drop(gate);
    host.shutdown().await.expect("first shutdown");

    let reopened = open_host(Some(config)).await.expect("reopened host");
    let gate = IncarnationGate::new(reopened.state_store().expect("reopened StateStore"));
    let preserved = gate.load().await.expect("preserved control plane");
    assert_eq!(preserved.mode(), ControlPlaneMode::Reconciling);
    assert_eq!(preserved.incarnation(), reconciling.incarnation());
    drop(gate);
    reopened.shutdown().await.expect("reopened shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_config_opens_disabled_host() {
    let host = open_host(None)
        .await
        .expect("absent state store configuration must open a disabled host");

    assert!(host.state_store().is_none());
    assert_eq!(host.state_store_provider_id(), None);
    assert!(
        matches!(
            parse_typed_statement("SELECT 1")
                .expect("the parser-owned Query AST should construct SELECT")
                .as_slice(),
            [novarocks_parser::ast::Statement::Query(_)]
        ),
        "ordinary SQL must remain a typed Query, not a typed maintenance statement"
    );
    let _query_execution = host.query_execution_service();
    let _backend_activity = host.backend_query_activity();
    let _backend_event_sink = host.backend_query_event_sink();
    assert!(
        execute_view_statement(
            host.view_service().as_ref(),
            &SessionViewEngine,
            "CREATE VIEW memory_view AS SELECT 1",
            view_context(),
        )
        .is_ok()
    );
    host.shutdown()
        .await
        .expect("disabled host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fe_without_state_store_fails_before_frontend_services_open() {
    let error = match FrontendApplicationHost::open(
        None,
        execution_config(),
        fe_backend_config(),
        tokio::runtime::Handle::current(),
    )
    .await
    {
        Ok(host) => {
            host.shutdown().await.expect("shutdown unexpected FE host");
            panic!("role=fe must not open without StateStore membership authority");
        }
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        FrontendApplicationErrorKind::ClusterBackendOpen
    );
    assert!(error.to_string().contains("requires StateStore"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_host_opens_store_with_single_fe_view() {
    let host = open_host(Some(state_store_input()))
        .await
        .expect("test StateStore host must open");

    let store = host
        .state_store()
        .expect("configured SQLite host must expose its state store");
    assert_eq!(
        host.state_store_provider_id(),
        Some(state_store_fixture::TEST_STATE_STORE_PROVIDER_ID)
    );
    assert!(
        store.identity().await.is_ok(),
        "test deployment view must allow StateStore access"
    );
    drop(store);

    host.shutdown()
        .await
        .expect("SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregistered_provider_fails_before_store_open() {
    let input = state_store_input();
    let error = match FrontendApplicationHost::open(
        Some(input),
        execution_config(),
        backend_config(),
        tokio::runtime::Handle::current(),
    )
    .await
    {
        Ok(host) => {
            host.shutdown().await.expect("shutdown unexpected host");
            panic!("an unregistered test provider must fail before store I/O");
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::StateStoreHost);
    assert!(error.to_string().contains("ProviderNotRegistered"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_open_releases_partial_resources() {
    let host = open_host(Some(state_store_input()))
        .await
        .expect("test provider opens without retaining partial resources");
    host.shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_sqlite_deployment_lock() {
    let config = state_store_input();
    let host = open_host(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    host.shutdown()
        .await
        .expect("host shutdown must release the SQLite deployment lock");
    let reopened = open_host(Some(config))
        .await
        .expect("SQLite deployment must reopen after shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_test_provider_allows_multiple_live_hosts() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    let second = open_host(Some(config.clone()))
        .await
        .expect("shared test provider permits a second live host");
    second.shutdown().await.expect("second host shutdown");

    host.shutdown()
        .await
        .expect("first host shutdown must succeed");
    let reopened = open_host(Some(config))
        .await
        .expect("same test deployment reopens after explicit shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_host_restores_views_through_its_service_after_reopen() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    execute_view_statement(
        host.view_service().as_ref(),
        &SessionViewEngine,
        "CREATE VIEW durable_view AS SELECT 42 AS answer",
        view_context(),
    )
    .expect("host view service must persist the view");
    host.shutdown().await.expect("first host shutdown");

    let reopened = open_host(Some(config))
        .await
        .expect("configured host must reopen");
    let mut query = parse_query("SELECT * FROM durable_view");
    reopened
        .view_service()
        .rewrite_query(&SessionViewEngine, &mut query, view_context())
        .expect("reopened host must restore the view");
    assert_eq!(
        query.to_string(),
        "SELECT * FROM (SELECT 42 AS answer) durable_view"
    );
    reopened.shutdown().await.expect("reopened host shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_view_record_fails_host_open_at_the_view_service_boundary() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    let store = host.state_store().expect("configured host state store");
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "seed corrupt frontend view record",
        )
        .await
        .expect("begin corrupt record write");
    transaction
        .put(
            database_key("default_catalog", "db").expect("view database key"),
            Value::try_from(Bytes::from_static(b"not-json")).expect("corrupt value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    let error = match open_host(Some(config)).await {
        Ok(_) => panic!("corrupt durable view metadata must reject host open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::ViewServiceOpen);
    assert!(error.to_string().contains("decode frontend view database"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_open_failure_precedes_table_maintenance_open_failure() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    let store = host.state_store().expect("configured host state store");
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "seed corrupt frontend application records",
        )
        .await
        .expect("begin corrupt record write");
    transaction
        .put(
            database_key("default_catalog", "db").expect("view database key"),
            Value::try_from(Bytes::from_static(b"not-json")).expect("corrupt view value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt view record");
    transaction
        .put(
            Key::try_from(Bytes::from_static(
                b"novarocks/frontend/table-maintenance/v1/jobs/0000000000000001",
            ))
            .expect("maintenance job key"),
            Value::try_from(Bytes::from_static(b"not-json")).expect("corrupt maintenance value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt maintenance record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    let error = match open_host(Some(config)).await {
        Ok(_) => panic!("corrupt durable application metadata must reject host open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::ViewServiceOpen);
    assert!(error.to_string().contains("decode frontend view database"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_table_maintenance_record_fails_open_and_releases_partial_resources() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open");
    let store = host.state_store().expect("configured host state store");
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "seed corrupt frontend table-maintenance record",
        )
        .await
        .expect("begin corrupt record write");
    transaction
        .put(
            Key::try_from(Bytes::from_static(
                b"novarocks/frontend/table-maintenance/v1/jobs/0000000000000001",
            ))
            .expect("maintenance job key"),
            Value::try_from(Bytes::from_static(b"not-json")).expect("corrupt maintenance value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt maintenance record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    for _ in 0..2 {
        let error = match open_host(Some(config.clone())).await {
            Ok(_) => panic!("corrupt maintenance metadata must reject host open"),
            Err(error) => error,
        };
        assert_eq!(
            error.kind(),
            FrontendApplicationErrorKind::TableMaintenanceServiceOpen
        );
        assert!(
            error
                .to_string()
                .contains("open frontend optimize job repository")
        );
    }
}
