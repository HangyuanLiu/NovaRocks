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

//! Race and fault behaviour of the frontend table-maintenance authority.
//!
//! These cases exercise the CP-4A invariants that only show up when two
//! attempts compete for the same table: a stale attempt must not write back,
//! and a lost lease must stop new external work without erasing what already
//! happened. Two `LeaseManager` actors over one SQLite StateStore stand in for
//! two frontends; this proves the abstraction, not a live 2FE deployment.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::maintenance::MaintenanceTarget;
use novarocks_frontend::table_maintenance::coordination::{
    MaintenanceAcquireOutcome, MaintenanceCoordination, MaintenanceLeaseAttempt,
    new_maintenance_holder_id,
};
use novarocks_frontend::table_maintenance::model::{CleanupOperationCreate, OptimizeJobCreate};
use novarocks_frontend::table_maintenance::repository::{
    CleanupOperationRepository, OptimizeJobRepository, RepositoryErrorKind, cleanup_payload_digest,
};
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_spi::state_store::{FeDeploymentView, StateStore};
use novarocks_state_store::coordination::{
    ClockHealth, CoordinationError, IncarnationGate, LeaseClock, LeaseManager, LeaseSettings,
};
use novarocks_state_store::{
    OperationId, StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use tempfile::TempDir;
use uuid::Uuid;

const BASE_CLOCK_MS: u64 = 100_000;

/// Deterministic clock: the harness advances it explicitly so lease expiry and
/// takeover are decided by the test, not by wall-clock timing.
#[derive(Debug)]
struct ManualClock {
    now_ms: std::sync::atomic::AtomicU64,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now_ms: std::sync::atomic::AtomicU64::new(BASE_CLOCK_MS),
        }
    }

    fn advance(&self, millis: u64) {
        self.now_ms
            .fetch_add(millis, std::sync::atomic::Ordering::SeqCst);
    }
}

impl LeaseClock for ManualClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        Ok(self.now_ms.load(std::sync::atomic::Ordering::SeqCst))
    }

    fn monotonic_time_millis(&self) -> u64 {
        self.now_ms.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn health(&self) -> ClockHealth {
        ClockHealth::Healthy
    }
}

fn sqlite_config(path: &Path) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "table-maintenance-coordination-test".to_string(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "table-maintenance-coordination-fe".to_string(),
        },
    }
}

async fn open_sqlite(path: &Path) -> Arc<dyn StateStore> {
    let registry = builtin_state_store_provider_registry().expect("built-in provider registry");
    StateStoreHost::open(
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
            topology_revision: Bytes::from_static(b"table-maintenance-coordination-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite state store")
    .state_store()
    .expect("SQLite state store exposure")
}

/// One simulated frontend: its own holder identity and its own clock, sharing
/// only the StateStore with its peer.
struct Frontend {
    coordination: MaintenanceCoordination,
    clock: Arc<ManualClock>,
}

async fn frontend(store: &Arc<dyn StateStore>) -> Frontend {
    let gate = IncarnationGate::new(Arc::clone(store));
    if gate.load().await.is_err() {
        gate.bootstrap(OperationId::new_v7())
            .await
            .expect("bootstrap control incarnation");
    }
    let clock = Arc::new(ManualClock::new());
    let manager = LeaseManager::new(
        Arc::clone(store),
        new_maintenance_holder_id().expect("process holder"),
        Arc::clone(&clock) as Arc<dyn LeaseClock>,
        LeaseSettings::new(
            Duration::from_millis(1_000),
            Duration::from_millis(400),
            Duration::ZERO,
            Duration::from_millis(10),
        )
        .expect("lease settings"),
    )
    .expect("lease manager");
    Frontend {
        coordination: MaintenanceCoordination::new(
            gate,
            manager,
            tokio::runtime::Handle::current(),
        ),
        clock,
    }
}

async fn acquire(frontend: &Frontend, target: &MaintenanceTarget) -> MaintenanceLeaseAttempt {
    match frontend
        .coordination
        .acquire(target)
        .await
        .expect("acquire must not fail")
    {
        MaintenanceAcquireOutcome::Acquired(attempt) => attempt,
        MaintenanceAcquireOutcome::Contended(_) => panic!("target unexpectedly contended"),
        MaintenanceAcquireOutcome::AwaitingTakeover(_) => {
            panic!("target unexpectedly awaiting takeover")
        }
    }
}

fn target(table: &str) -> MaintenanceTarget {
    MaintenanceTarget {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: table.to_string(),
    }
}

fn object_id() -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(Bytes::from_static(b"test-object-id")).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_one_frontend_holds_dispatch_authority_for_a_table() {
    let temp = TempDir::new().expect("temporary directory");
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    let first = frontend(&store).await;
    let second = frontend(&store).await;
    let orders = target("orders");

    let held = acquire(&first, &orders).await;

    // The peer cannot take a live target, but an unrelated table is free.
    assert!(matches!(
        second
            .coordination
            .acquire(&orders)
            .await
            .expect("contended acquire is not an error"),
        MaintenanceAcquireOutcome::Contended(_) | MaintenanceAcquireOutcome::AwaitingTakeover(_)
    ));
    let _other = acquire(&second, &target("customers")).await;

    // After expiry the peer may take over, and the old attempt loses authority.
    second.clock.advance(5_000);
    let taken_over = loop {
        match second
            .coordination
            .acquire(&orders)
            .await
            .expect("takeover acquire is not an error")
        {
            MaintenanceAcquireOutcome::Acquired(attempt) => break attempt,
            MaintenanceAcquireOutcome::Contended(_)
            | MaintenanceAcquireOutcome::AwaitingTakeover(_) => {
                second.clock.advance(1_000);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    assert_ne!(held.attempt_id(), taken_over.attempt_id());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_taken_over_attempt_cannot_write_back_optimize_state() {
    let temp = TempDir::new().expect("temporary directory");
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    let repository = OptimizeJobRepository::open(Arc::clone(&store))
        .await
        .expect("open optimize repository");
    let first = frontend(&store).await;
    let second = frontend(&store).await;
    let orders = target("orders");

    let stale = acquire(&first, &orders).await;
    let job = repository
        .create_admitted(
            OptimizeJobCreate {
                target: orders.clone(),
                object_id: object_id(),
                base_snapshot_id: 10,
                created_at_ms: 100,
            },
            first
                .coordination
                .admit_writes()
                .await
                .expect("admit intent"),
        )
        .await
        .expect("create optimize job");
    let stale_authority = stale.durable_authority().await.expect("stale authority");
    repository
        .claim_fenced(
            job.job_id,
            200,
            stale_authority.clone(),
            stale.fence_validator(),
        )
        .await
        .expect("claim under the live attempt")
        .expect("pending job is claimable");

    // The peer takes the table over after the first lease expires.
    second.clock.advance(5_000);
    let fresh = loop {
        match second
            .coordination
            .acquire(&orders)
            .await
            .expect("takeover acquire is not an error")
        {
            MaintenanceAcquireOutcome::Acquired(attempt) => break attempt,
            _ => {
                second.clock.advance(1_000);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };

    // Every write-back path of the stale attempt is refused.
    for error in [
        repository
            .record_outcome_fenced(
                job.job_id,
                novarocks_frontend::table_maintenance::model::OptimizeJobOutcome {
                    target_snapshot_id: Some(11),
                    rewritten_data_files: 1,
                    deleted_data_files: 0,
                    added_data_files: 1,
                    output_record_count: 5,
                },
                stale_authority.clone(),
                stale.fence_validator(),
            )
            .await
            .expect_err("a taken-over attempt must not record an outcome"),
        repository
            .finish_fenced(
                job.job_id,
                300,
                stale_authority.clone(),
                stale.fence_validator(),
            )
            .await
            .expect_err("a taken-over attempt must not finish the job"),
        repository
            .fail_fenced(
                job.job_id,
                300,
                "stale".to_string(),
                stale_authority.clone(),
                stale.fence_validator(),
            )
            .await
            .expect_err("a taken-over attempt must not fail the job"),
        repository
            .release_undispatched_fenced(
                job.job_id,
                stale_authority.clone(),
                stale.fence_validator(),
            )
            .await
            .expect_err("a taken-over attempt must not requeue the job"),
    ] {
        assert_eq!(error.kind(), RepositoryErrorKind::AuthorityLost);
    }

    // The new owner converges the same record.
    let fresh_authority = fresh.durable_authority().await.expect("fresh authority");
    repository
        .release_undispatched_fenced(job.job_id, fresh_authority, fresh.fence_validator())
        .await
        .expect("the live attempt owns recovery");
    let requeued = repository
        .list_pending()
        .await
        .expect("list pending")
        .into_iter()
        .find(|pending| pending.job_id == job.job_id)
        .expect("the undispatched job returns to the queue");
    assert_eq!(requeued.dispatched_child, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_lease_cannot_prepare_another_destructive_cleanup_batch() {
    let temp = TempDir::new().expect("temporary directory");
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    let repository = CleanupOperationRepository::open(Arc::clone(&store))
        .await
        .expect("open cleanup repository");
    let first = frontend(&store).await;
    let second = frontend(&store).await;
    let orphans = target("orphans");

    let stale = acquire(&first, &orphans).await;
    let operation_id = Uuid::now_v7();
    repository
        .create_admitted(
            CleanupOperationCreate {
                operation_id,
                target: orphans.clone(),
                object_id: object_id(),
                owner:
                    novarocks_frontend::table_maintenance::model::MetadataMaintenanceExactOwner {
                        instance_id: "ice".to_string(),
                        incarnation_id: Uuid::now_v7(),
                    },
                request_digest: [7u8; 32],
                older_than_ms: 1,
                created_at_ms: 100,
            },
            first
                .coordination
                .admit_writes()
                .await
                .expect("admit intent"),
        )
        .await
        .expect("create cleanup operation");
    let stale_authority = stale.durable_authority().await.expect("stale authority");

    second.clock.advance(5_000);
    loop {
        match second
            .coordination
            .acquire(&orphans)
            .await
            .expect("takeover acquire is not an error")
        {
            MaintenanceAcquireOutcome::Acquired(_) => break,
            _ => {
                second.clock.advance(1_000);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    // Preparing a batch is the transition that authorizes a delete. After the
    // takeover the stale attempt cannot reach it, so cleanup can only ever be
    // reconciled by the new owner, never re-planned by the old one.
    let error = repository
        .prepare_batch_fenced(
            operation_id,
            novarocks_frontend::table_maintenance::model::CleanupBatchCheckpoint {
                ordinal: 0,
                prepared_handle_digest: cleanup_payload_digest(b"prepared"),
                prepared_handle: b"prepared".to_vec(),
                receipt_handle_digest: None,
                receipt_handle: None,
                deleted_count: 0,
                already_absent_count: 0,
                failed_count: 0,
                unknown_count: 0,
            },
            400,
            stale_authority.clone(),
            stale.fence_validator(),
        )
        .await
        .expect_err("a taken-over attempt must not prepare a destructive batch");
    assert_eq!(error.kind(), RepositoryErrorKind::AuthorityLost);

    // The lease loss is observable to the losing frontend, which is what stops
    // its in-flight dispatch loop before the next provider call.
    tokio::time::timeout(Duration::from_secs(5), async {
        while stale.ensure_active().is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the stale attempt observes that it lost authority");
}
