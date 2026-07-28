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
use novarocks::mv::repository::MvRepositoryErrorKind;
use novarocks_frontend::mv::repository::key::definition_by_id_key;
use novarocks_frontend::{
    FrontendApplicationErrorKind, FrontendApplicationHost, FrontendGrpcEndpointOwnership,
    FrontendServerConfig, run_frontend_server_until_shutdown,
};
use novarocks_spi::state_store::{CommitOutcome, Precondition, TransactionId, Value};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHostConfig, StateStoreLimitOverrides,
    StateStoreProviderConfig,
};
use tempfile::TempDir;
use uuid::Uuid;

fn sqlite_config(temp: &TempDir) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "frontend-mv-host".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path: temp.path().join("state-store.sqlite"),
                    deployment_owner: "frontend-fe".to_owned(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_sqlite_opens_and_reopens_mv_repository() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);

    let host = FrontendApplicationHost::open(Some(config.clone()))
        .await
        .expect("configured host must open its MV repository");
    assert!(host.mv_repository().availability().is_available());
    let repository = host.mv_repository();
    drop(repository);
    host.shutdown()
        .await
        .expect("shutdown must release MV repository first");

    let reopened = FrontendApplicationHost::open(Some(config))
        .await
        .expect("same SQLite store must reopen its MV repository");
    assert!(reopened.mv_repository().availability().is_available());
    reopened.shutdown().await.expect("reopened host shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_state_store_keeps_host_available_but_mv_unavailable() {
    let host = FrontendApplicationHost::open(None)
        .await
        .expect("host without StateStore remains available");
    assert!(!host.mv_repository().availability().is_available());
    host.shutdown().await.expect("disabled host shutdown");
}

#[tokio::test]
async fn current_thread_runtime_rejects_sync_mv_repository_calls_without_panicking() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let host = FrontendApplicationHost::open(Some(sqlite_config(&temp)))
        .await
        .expect("configured host");
    let repository = host.mv_repository();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        repository.list_definitions()
    }));
    let error = result
        .expect("current-thread repository call must not panic")
        .expect_err("current-thread repository call must return an error");
    assert_eq!(error.kind(), MvRepositoryErrorKind::InvalidRequest);
    assert!(error.message().contains("current-thread Tokio runtime"));

    drop(repository);
    host.shutdown().await.expect("host shutdown");
}

#[tokio::test]
async fn current_thread_server_start_returns_mv_repository_error_before_bind() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let mut config = novarocks::common::app_config::NovaRocksConfig::default();
    config.cluster.role = novarocks::common::app_config::ClusterRole::Fe;
    config.cluster.backends.clear();
    config.state_store = Some(sqlite_config(&temp).state_store);

    let error = run_frontend_server_until_shutdown(
        FrontendServerConfig {
            config,
            config_path: None,
            port_override: Some(0),
            grpc_endpoint: FrontendGrpcEndpointOwnership::ExternallyHosted,
        },
        std::future::pending::<()>(),
    )
    .await
    .expect_err("current-thread startup must reject the synchronous MV repository call");

    assert_eq!(error.kind(), FrontendApplicationErrorKind::Server);
    assert!(error.to_string().contains("current-thread Tokio runtime"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_mv_record_fails_open_and_releases_provider_for_retry() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = sqlite_config(&temp);
    let host = FrontendApplicationHost::open(Some(config.clone()))
        .await
        .expect("configured host");
    let store = host.state_store().expect("configured StateStore");
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "seed corrupt MV record",
        )
        .await
        .expect("begin corrupt MV write");
    transaction
        .put(
            definition_by_id_key(1).expect("MV definition key"),
            Value::try_from(Bytes::from_static(b"not-an-mv-envelope")).expect("value"),
            Precondition::Absent,
        )
        .await
        .expect("stage corrupt MV record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
    drop(store);
    host.shutdown().await.expect("seed host shutdown");

    let error = match FrontendApplicationHost::open(Some(config.clone())).await {
        Ok(_) => panic!("corrupt MV metadata must reject host open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::MvServiceOpen);

    let retry = match FrontendApplicationHost::open(Some(config)).await {
        Ok(_) => panic!("retry must reach MV validation instead of retaining provider lock"),
        Err(error) => error,
    };
    assert_eq!(retry.kind(), FrontendApplicationErrorKind::MvServiceOpen);
}
