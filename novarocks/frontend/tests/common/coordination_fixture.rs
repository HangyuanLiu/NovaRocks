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

//! Real frontend coordination backed by a temporary SQLite StateStore.
//!
//! Distributed write is fenced, and a fence can only be minted from a live
//! coordination lease. A route test that composed no coordination could
//! therefore never dispatch. Rather than reach for the crate-internal
//! test-only fence — which exists precisely so that a fence can never be minted
//! without a lease — these tests bring up the genuine coordination runtime and
//! exercise the production path.

#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use novarocks_frontend::coordination::FrontendCoordinationRuntime;
use novarocks_spi::state_store::{FeDeploymentView, StateStore};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

/// Keeps the store host and its temp dir alive for the lifetime of a test.
pub struct CoordinationFixture {
    pub coordination: Arc<FrontendCoordinationRuntime>,
    pub store: Arc<dyn StateStore>,
    _host: StateStoreHost,
    _dir: tempfile::TempDir,
}

pub async fn open(cluster_id: &str) -> CoordinationFixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("state-store.sqlite");
    let registry = builtin_state_store_provider_registry().expect("provider registry");
    let host = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: cluster_id.to_string(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path,
                        deployment_owner: format!("{cluster_id}-fe"),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"dml-route-test-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open state store host");
    let store = host.state_store().expect("StateStore exposure");
    let coordination = FrontendCoordinationRuntime::open(Arc::clone(&store))
        .await
        .expect("open frontend coordination");
    CoordinationFixture {
        coordination: Arc::new(coordination),
        store,
        _host: host,
        _dir: dir,
    }
}

/// Blocking variant for the synchronous route tests.
///
/// Owns its runtime so the coordination lease outlives the service built from
/// it; dropping the fixture tears both down together.
pub struct BlockingCoordination {
    pub coordination: Arc<FrontendCoordinationRuntime>,
    runtime: tokio::runtime::Runtime,
    _fixture: CoordinationFixture,
}

impl BlockingCoordination {
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }
}

pub fn open_blocking(cluster_id: &str) -> BlockingCoordination {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let fixture = runtime.block_on(open(cluster_id));
    BlockingCoordination {
        coordination: Arc::clone(&fixture.coordination),
        runtime,
        _fixture: fixture,
    }
}
