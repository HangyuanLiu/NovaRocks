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
use novarocks::engine::view::{
    CreateExternalViewRequest, ResolvedExternalView, ViewColumnDefinition, ViewEngine,
    ViewRequestContext, ViewSqlDialect, ViewTarget,
};
use novarocks_frontend::view::repository::database_key;
use novarocks_frontend::{FrontendApplicationErrorKind, FrontendApplicationHost};
use novarocks_spi::state_store::{CommitOutcome, Precondition, TransactionId, Value};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
};
use sqlparser::ast::{Query, Statement};
use sqlparser::parser::Parser;
use tempfile::TempDir;
use uuid::Uuid;

struct SessionViewEngine;

impl ViewEngine for SessionViewEngine {
    fn validate_iceberg_catalog(&self, _catalog: &str) -> Result<(), String> {
        unreachable!("session view must not access an external catalog")
    }

    fn is_rest_iceberg_catalog(&self, _catalog: &str) -> bool {
        false
    }

    fn table_exists(&self, _target: &ViewTarget) -> Result<bool, String> {
        unreachable!("session view must not probe external tables")
    }

    fn view_exists(&self, _target: &ViewTarget) -> Result<bool, String> {
        unreachable!("session view must not probe external views")
    }

    fn create_external_view(&self, _request: CreateExternalViewRequest) -> Result<(), String> {
        unreachable!("session view must not create external views")
    }

    fn drop_external_view(&self, _target: &ViewTarget) -> Result<(), String> {
        unreachable!("session view must not drop external views")
    }

    fn load_external_view(
        &self,
        _target: &ViewTarget,
    ) -> Result<Option<ResolvedExternalView>, String> {
        Ok(None)
    }

    fn list_external_views(&self, _catalog: &str, _database: &str) -> Result<Vec<String>, String> {
        unreachable!("session view must not list external views")
    }

    fn analyze_external_view(
        &self,
        _catalog: &str,
        _database: &str,
        _query: &Query,
    ) -> Result<Vec<ViewColumnDefinition>, String> {
        unreachable!("session view must not analyze external views")
    }
}

fn view_context() -> ViewRequestContext<'static> {
    ViewRequestContext {
        current_catalog: None,
        current_database: "db",
    }
}

fn parse_query(sql: &str) -> Box<Query> {
    let mut parser = Parser::new(&ViewSqlDialect).try_with_sql(sql).unwrap();
    match parser.parse_statement().unwrap() {
        Statement::Query(query) => query,
        other => panic!("expected query, got {other:?}"),
    }
}

fn sqlite_config(temp: &TempDir) -> StateStoreAppConfig {
    StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: "frontend-cluster".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Sqlite {
                path: temp.path().join("state-store.sqlite"),
                deployment_owner: "frontend-fe".to_owned(),
            },
        },
        mysql_client: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_config_opens_disabled_host() {
    let host = FrontendApplicationHost::open(None)
        .await
        .expect("absent state store configuration must open a disabled host");

    assert!(host.state_store().is_none());
    assert!(
        host.view_service()
            .try_handle_statement(
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
async fn sqlite_host_opens_store_with_single_fe_view() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let host = FrontendApplicationHost::open(Some(sqlite_config(&temp)))
        .await
        .expect("SQLite host must open its state store");

    let store = host
        .state_store()
        .expect("configured SQLite host must expose its state store");
    assert!(
        store.identity().await.is_ok(),
        "single-FE deployment view must allow SQLite store access"
    );

    host.shutdown()
        .await
        .expect("SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_provider_fails_before_store_open() {
    let mysql_config = StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: "frontend-cluster".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Mysql {
                database: "frontend_control_plane".to_owned(),
            },
        },
        mysql_client: None,
    };
    let foundationdb_config = StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: "frontend-cluster".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Foundationdb {
                cluster_file: "/definitely/not/an/fdb/cluster-file".into(),
                keyspace_id: Uuid::nil(),
            },
        },
        mysql_client: None,
    };

    for config in [mysql_config, foundationdb_config] {
        let error = match FrontendApplicationHost::open(Some(config)).await {
            Ok(_) => panic!("deferred provider must be rejected before runtime or store I/O"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), FrontendApplicationErrorKind::DeploymentSource);
        assert!(error.to_string().contains("UnsupportedProvider"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_open_releases_partial_resources() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let non_directory_parent = temp.path().join("not-a-directory");
    std::fs::write(&non_directory_parent, b"not a directory")
        .expect("create regular file for SQLite parent failure");
    let config = StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: "frontend-cluster".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Sqlite {
                path: non_directory_parent.join("state-store.sqlite"),
                deployment_owner: "frontend-fe".to_owned(),
            },
        },
        mysql_client: None,
    };

    let error = match FrontendApplicationHost::open(Some(config.clone())).await {
        Ok(_) => panic!("unopenable SQLite path must fail host initialization"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::StoreOpen);

    let host = FrontendApplicationHost::open(Some(sqlite_config(&temp)))
        .await
        .expect("failed open must not retain partial runtime resources");
    host.shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_sqlite_deployment_lock() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = FrontendApplicationHost::open(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    host.shutdown()
        .await
        .expect("host shutdown must release the SQLite deployment lock");
    let reopened = FrontendApplicationHost::open(Some(config))
        .await
        .expect("SQLite deployment must reopen after shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_is_required_to_reopen_same_deployment() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = FrontendApplicationHost::open(Some(config.clone()))
        .await
        .expect("first SQLite host must open");

    let error = match FrontendApplicationHost::open(Some(config.clone())).await {
        Ok(_) => panic!("second live SQLite host must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::StoreOpen);

    host.shutdown()
        .await
        .expect("first host shutdown must succeed");
    let reopened = FrontendApplicationHost::open(Some(config))
        .await
        .expect("same SQLite deployment must reopen after explicit shutdown");
    reopened
        .shutdown()
        .await
        .expect("reopened SQLite host shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_host_restores_views_through_its_service_after_reopen() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = FrontendApplicationHost::open(Some(config.clone()))
        .await
        .expect("configured host must open");
    host.view_service()
        .try_handle_statement(
            &SessionViewEngine,
            "CREATE VIEW durable_view AS SELECT 42 AS answer",
            view_context(),
        )
        .expect("host view service must persist the view");
    host.shutdown().await.expect("first host shutdown");

    let reopened = FrontendApplicationHost::open(Some(config))
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
    let host = FrontendApplicationHost::open(Some(config.clone()))
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

    let error = match FrontendApplicationHost::open(Some(config)).await {
        Ok(_) => panic!("corrupt durable view metadata must reject host open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::ViewServiceOpen);
    assert!(error.to_string().contains("decode frontend view database"));
}
