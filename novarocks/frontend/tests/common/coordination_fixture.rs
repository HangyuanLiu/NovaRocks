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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::state_store_fixture;
use novarocks_frontend::StateStoreHost;
use novarocks_frontend::coordination::FrontendCoordinationRuntime;
use novarocks_frontend::dml::state_store_journal::StateStoreOperationJournal;
use novarocks_frontend::dml::{OperationJournal, OperationState, StoredOperation};
use novarocks_spi::state_store::StateStore;

/// Keeps the store host and its temp dir alive for the lifetime of a test.
pub struct CoordinationFixture {
    pub coordination: Arc<FrontendCoordinationRuntime>,
    pub store: Arc<dyn StateStore>,
    _host: StateStoreHost,
}

pub async fn open(cluster_id: &str) -> CoordinationFixture {
    let host = state_store_fixture::open(cluster_id).await;
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
    static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);
    let isolated_cluster_id = format!(
        "{cluster_id}-{}",
        NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    );
    let fixture = runtime.block_on(open(&isolated_cluster_id));
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
