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

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::engine::table_maintenance::MaintenanceTarget;
use novarocks_frontend::table_maintenance::model::{
    MetadataMaintenanceExactOwner, MetadataMaintenanceOpaquePayload,
    MetadataMaintenanceOperationCreate, MetadataMaintenanceOperationKind,
    MetadataMaintenanceOperationState, MetadataMaintenancePlanPayload, OptimizeJobCreate,
};
use novarocks_frontend::table_maintenance::repository::{
    MetadataMaintenanceOperationRepository, OptimizeJobRepository, RepositoryErrorKind,
    metadata_maintenance_payload_digest,
};
use novarocks_spi::state_store::{FeDeploymentView, StateStore};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use tempfile::TempDir;
use uuid::Uuid;

fn sqlite_config(path: &Path) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "table-maintenance-operation-repository-test".to_string(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "table-maintenance-operation-repository-fe".to_string(),
        },
    }
}

async fn fixture() -> (
    TempDir,
    Arc<dyn StateStore>,
    MetadataMaintenanceOperationRepository,
) {
    let temp = TempDir::new().unwrap();
    let store = StateStoreHost::open(
        &builtin_state_store_provider_registry().unwrap(),
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: sqlite_config(&temp.path().join("state.sqlite")),
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"metadata-maintenance-operation-test"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap()
    .state_store()
    .unwrap();
    let repository = MetadataMaintenanceOperationRepository::open(Arc::clone(&store))
        .await
        .unwrap();
    (temp, store, repository)
}

fn target() -> MaintenanceTarget {
    MaintenanceTarget {
        catalog: "rest".to_string(),
        namespace: "db".to_string(),
        table: "orders".to_string(),
    }
}

fn create(operation_id: Uuid) -> MetadataMaintenanceOperationCreate {
    let request_payload = br#"{"operation":"rewrite-metadata-layout"}"#.to_vec();
    MetadataMaintenanceOperationCreate {
        operation_id,
        target: target(),
        owner: MetadataMaintenanceExactOwner {
            instance_id: "iceberg_rest".to_string(),
            incarnation_id: Uuid::now_v7(),
        },
        kind: MetadataMaintenanceOperationKind::RewriteMetadataLayout,
        // The SPI semantic request digest is intentionally independent from
        // the opaque durable payload checksum.
        request_digest: [3; 32],
        request_payload_digest: metadata_maintenance_payload_digest(&request_payload),
        base_state_digest: [9; 32],
        request_payload,
        created_at_ms: 10,
    }
}

fn opaque(payload: &[u8]) -> MetadataMaintenanceOpaquePayload {
    MetadataMaintenanceOpaquePayload {
        digest: metadata_maintenance_payload_digest(payload),
        payload: payload.to_vec(),
    }
}

#[tokio::test]
async fn persists_plan_before_running_and_releases_terminal_fence() {
    let (_temp, _store, repository) = fixture().await;
    let operation_id = Uuid::now_v7();
    let created = repository.create(create(operation_id)).await.unwrap();
    assert_eq!(created.state, MetadataMaintenanceOperationState::Pending);
    assert!(repository.has_active_target(&target()).await.unwrap());

    let plan_payload = br#"{"base":"a","artifact":"b"}"#.to_vec();
    let started = repository
        .start(
            operation_id,
            MetadataMaintenancePlanPayload {
                plan_digest: [4; 32],
                payload_digest: metadata_maintenance_payload_digest(&plan_payload),
                payload: plan_payload.clone(),
            },
            11,
        )
        .await
        .unwrap();
    assert_eq!(started.state, MetadataMaintenanceOperationState::Running);
    assert_eq!(
        repository
            .load_plan(operation_id)
            .await
            .unwrap()
            .unwrap()
            .payload,
        plan_payload
    );

    let finished = repository
        .finish(operation_id, opaque(b"receipt"), 12)
        .await
        .unwrap();
    assert_eq!(finished.state, MetadataMaintenanceOperationState::Finished);
    assert!(!repository.has_active_target(&target()).await.unwrap());
    assert_eq!(
        repository
            .load_receipt(operation_id)
            .await
            .unwrap()
            .unwrap()
            .payload,
        b"receipt"
    );
}

#[tokio::test]
async fn same_operation_replays_only_the_same_request() {
    let (_temp, _store, repository) = fixture().await;
    let operation_id = Uuid::now_v7();
    let request = create(operation_id);
    repository.create(request.clone()).await.unwrap();
    assert_eq!(
        repository.create(request).await.unwrap().operation_id,
        operation_id
    );

    let mut conflict = create(operation_id);
    conflict.base_state_digest = [7; 32];
    let error = repository.create(conflict).await.unwrap_err();
    assert_eq!(error.kind(), RepositoryErrorKind::InvalidTransition);
}

#[tokio::test]
async fn unresolved_operation_retains_the_table_fence() {
    let (_temp, _store, repository) = fixture().await;
    let operation_id = Uuid::now_v7();
    repository.create(create(operation_id)).await.unwrap();
    let plan = b"plan".to_vec();
    repository
        .start(
            operation_id,
            MetadataMaintenancePlanPayload {
                plan_digest: [4; 32],
                payload_digest: metadata_maintenance_payload_digest(&plan),
                payload: plan,
            },
            11,
        )
        .await
        .unwrap();
    repository
        .mark_reconcile_pending(operation_id, opaque(b"possible-catalog-arrival"))
        .await
        .unwrap();
    let unresolved = repository
        .mark_unresolved(operation_id, "exact generation retired".to_string(), 12)
        .await
        .unwrap();
    assert_eq!(
        unresolved.state,
        MetadataMaintenanceOperationState::Unresolved
    );
    assert!(repository.has_active_target(&target()).await.unwrap());
    let error = repository.create(create(Uuid::now_v7())).await.unwrap_err();
    assert_eq!(error.kind(), RepositoryErrorKind::AlreadyActive);
}

#[tokio::test]
async fn v1_optimize_and_v2_metadata_operations_are_mutually_exclusive() {
    let (_temp, store, repository) = fixture().await;
    let optimize = OptimizeJobRepository::open(Arc::clone(&store))
        .await
        .unwrap();
    optimize
        .create(OptimizeJobCreate {
            target: target(),
            base_snapshot_id: 1,
            created_at_ms: 10,
        })
        .await
        .unwrap();
    let error = repository.create(create(Uuid::now_v7())).await.unwrap_err();
    assert_eq!(error.kind(), RepositoryErrorKind::AlreadyActive);
}
