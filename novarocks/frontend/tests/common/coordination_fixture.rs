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
use novarocks_frontend::dml::state_store_journal::StateStoreOperationJournal;
use novarocks_frontend::dml::{OperationJournal, OperationState, StoredOperation};
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
}

pub async fn open(cluster_id: &str) -> CoordinationFixture {
    // The directory deliberately outlives the fixture: a service built from it
    // may be dropped before the test reads the journal back, and a deleted
    // database would then look like a journal failure rather than the fixture
    // teardown it actually is. The test binary's temp files are reclaimed by the
    // OS.
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let path = dir.join("state-store.sqlite");
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
    }
}

/// Blocking variant for the synchronous route tests.
///
/// Owns its runtime so the coordination lease outlives the service built from
/// it; dropping the fixture tears both down together.
pub struct BlockingCoordination {
    pub coordination: Arc<FrontendCoordinationRuntime>,
    pub journal: Arc<StateStoreOperationJournal>,
    _fixture: CoordinationFixture,
}

impl BlockingCoordination {
    pub fn handle(&self) -> tokio::runtime::Handle {
        shared_runtime().handle().clone()
    }
}

/// One runtime for the whole test binary.
///
/// A per-fixture runtime would be torn down with the service that borrowed it,
/// and any later read-back of the journal would then fail with a dead blocking
/// worker. The store itself still lives and dies with each fixture.
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    })
}

pub fn open_blocking(cluster_id: &str) -> BlockingCoordination {
    let runtime = shared_runtime();
    let fixture = runtime.block_on(open(cluster_id));
    let journal = runtime.block_on(async {
        StateStoreOperationJournal::open(
            Arc::clone(&fixture.store),
            tokio::runtime::Handle::current(),
        )
        .await
        .expect("open DML journal")
    });
    BlockingCoordination {
        coordination: Arc::clone(&fixture.coordination),
        journal: Arc::new(journal),
        _fixture: fixture,
    }
}

/// Read-back helpers the route tests used to get from their fake journal.
///
/// They now come from the real journal instead, so the assertions describe
/// durable state rather than an in-memory stand-in.
pub trait JournalInspect {
    fn states(&self) -> Vec<OperationState>;
    fn only_operation(&self) -> StoredOperation;
}

impl<T: OperationJournal + ?Sized> JournalInspect for T {
    fn states(&self) -> Vec<OperationState> {
        self.list_operations()
            .expect("list operations")
            .into_iter()
            .map(|operation| operation.state)
            .collect()
    }

    fn only_operation(&self) -> StoredOperation {
        let mut operations = self.list_operations().expect("list operations");
        assert_eq!(operations.len(), 1, "expected exactly one DML operation");
        operations.remove(0)
    }
}
