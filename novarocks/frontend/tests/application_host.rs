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

use novarocks_frontend::{FrontendApplicationErrorKind, FrontendApplicationHost};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
};
use tempfile::TempDir;

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
    let config = StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: "frontend-cluster".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Mysql {
                database: "frontend_control_plane".to_owned(),
            },
        },
        mysql_client: None,
    };

    let error = match FrontendApplicationHost::open(Some(config)).await {
        Ok(_) => panic!("deferred provider must be rejected before runtime or store I/O"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), FrontendApplicationErrorKind::DeploymentSource);
    assert!(error.to_string().contains("UnsupportedProvider"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_open_releases_partial_resources() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: "frontend-cluster".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Sqlite {
                path: ":memory:".into(),
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
