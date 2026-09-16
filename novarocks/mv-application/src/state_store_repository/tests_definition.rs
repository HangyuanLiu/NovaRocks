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

use super::StateStoreMvRepository;
use crate::persistence::identity::ObjectIdentity;
use crate::persistence::test_support::ProjectionFixture;
use crate::product::MvTarget;
use crate::repository::{
    DeleteMvProjectionRequest, MvProjectionRequest, MvRepository, MvRepositoryErrorKind,
    ReplaceMvProjectionRequest,
};
use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_state_store_api::{CommitOutcome, Key, Precondition, StateStore, Value};
use novarocks_state_store_runtime::StateStoreRunPolicy;
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use std::sync::Arc;
pub(crate) async fn repository() -> (Arc<InMemoryStateStore>, Arc<StateStoreMvRepository>) {
    let store = Arc::new(InMemoryStateStore::new(format!(
        "mv-accelerator-test-{}",
        uuid::Uuid::now_v7()
    )));
    let repository = StateStoreMvRepository::open(
        Arc::clone(&store) as Arc<dyn novarocks_state_store_api::StateStore>,
        StateStoreRunPolicy::default(),
    )
    .await
    .expect("open in-memory MV Accelerator repository");
    (store, repository)
}

pub(crate) fn object_id(bytes: &[u8]) -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(Bytes::copy_from_slice(bytes)).expect("bounded object ID")
}

pub(crate) fn target(table: &str) -> MvTarget {
    MvTarget::from_parts(Some("ice"), "sales", table)
}

pub(crate) fn projection_request(
    table: &str,
    object: &[u8],
    snapshot_id: i64,
    dependency: &str,
) -> MvProjectionRequest {
    let mut fixture = ProjectionFixture::new(target(table), Some(snapshot_id));
    fixture.object_id = object_id(object);
    for relation in &mut fixture.definition.relation_occurrences {
        relation.relation_at_binding = dependency.into();
        relation.object_id =
            ObjectIdentity::try_new(format!("base-{dependency}").into_bytes()).unwrap();
    }
    for source in &mut fixture.publication.as_mut().unwrap().inputs {
        source.object_id =
            ObjectIdentity::try_new(format!("base-{dependency}").into_bytes()).unwrap();
    }
    fixture.build().unwrap().into()
}

#[tokio::test]
async fn reopening_the_repository_retains_the_exact_lake_source_projection() {
    let (store, repository) = repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("retained", b"retained-object", 9, "orders"),
        )
        .await
        .unwrap();
    drop(repository);

    let reopened = StateStoreMvRepository::open(
        store as Arc<dyn novarocks_state_store_api::StateStore>,
        StateStoreRunPolicy::default(),
    )
    .await
    .expect("reopen MV Accelerator repository");
    assert_eq!(
        reopened
            .load_by_id(created.projection.mv_id)
            .await
            .unwrap()
            .unwrap()
            .projection,
        created.projection
    );
}

#[tokio::test]
async fn whole_projection_cas_replaces_root_target_and_dependency_indexes() {
    let (_host, repository) = repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("orders_mv", b"object-a", 11, "orders"),
        )
        .await
        .expect("create projection");
    let stale_version = created.version.clone();

    let replaced = repository
        .replace_projection(
            uuid::Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: created.projection.mv_id,
                expected_version: created.version,
                projection: projection_request("orders_mv_v2", b"object-a", 12, "customers"),
            },
        )
        .await
        .expect("replace projection");

    assert!(
        repository
            .find_by_target(&target("orders_mv"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        repository
            .find_by_target(&target("orders_mv_v2"))
            .await
            .unwrap()
            .unwrap(),
        replaced
    );
    let dependencies = repository
        .list_dependencies_by_downstream(replaced.projection.mv_id)
        .await
        .unwrap();
    assert_eq!(dependencies.len(), 2);
    assert_eq!(dependencies[0].upstream.name, "customers");

    let error = repository
        .replace_projection(
            uuid::Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: replaced.projection.mv_id,
                expected_version: stale_version,
                projection: projection_request("stale", b"object-a", 13, "orders"),
            },
        )
        .await
        .expect_err("stale CAS must fail");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
}

#[tokio::test]
async fn replacement_target_conflict_rolls_back_the_whole_projection() {
    let (_host, repository) = repository().await;
    let first = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("first", b"object-first", 21, "orders"),
        )
        .await
        .unwrap();
    repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("second", b"object-second", 22, "customers"),
        )
        .await
        .unwrap();

    assert!(
        repository
            .replace_projection(
                uuid::Uuid::now_v7(),
                ReplaceMvProjectionRequest {
                    mv_id: first.projection.mv_id,
                    expected_version: first.version.clone(),
                    projection: projection_request("second", b"object-first", 23, "lineitem",),
                },
            )
            .await
            .is_err()
    );
    assert_eq!(
        repository.load_by_id(first.projection.mv_id).await.unwrap(),
        Some(first)
    );
}

#[tokio::test]
async fn delete_requires_exact_object_source_and_version() {
    let (_host, repository) = repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("delete_me", b"object-live", 31, "orders"),
        )
        .await
        .unwrap();
    let mut stale_source = created.projection.facts.source_revision().clone();
    stale_source.target_object_id = object_id(b"object-recreated");
    let error = repository
        .delete_projection(
            uuid::Uuid::now_v7(),
            DeleteMvProjectionRequest {
                mv_id: created.projection.mv_id,
                expected_version: created.version.clone(),
                expected_source_revision: stale_source,
            },
        )
        .await
        .expect_err("logical name cannot authorize deletion");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert!(
        repository
            .load_by_id(created.projection.mv_id)
            .await
            .unwrap()
            .is_some()
    );

    repository
        .delete_projection(
            uuid::Uuid::now_v7(),
            DeleteMvProjectionRequest {
                mv_id: created.projection.mv_id,
                expected_version: created.version,
                expected_source_revision: created.projection.facts.source_revision().clone(),
            },
        )
        .await
        .expect("exact guarded delete");
    assert!(
        repository
            .find_by_target(&target("delete_me"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn whole_family_wipe_allows_internal_id_reallocation() {
    let (_host, repository) = repository().await;
    let first = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("before_wipe", b"object-before", 41, "orders"),
        )
        .await
        .unwrap();
    repository
        .wipe_accelerator(uuid::Uuid::now_v7())
        .await
        .expect("wipe current Accelerator family");
    let rebuilt = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("after_wipe", b"object-after", 42, "orders"),
        )
        .await
        .unwrap();
    assert_eq!(first.projection.mv_id, rebuilt.projection.mv_id);
}

#[tokio::test]
async fn whole_family_wipe_removes_an_unknown_current_record_without_decoding_it() {
    let (store, repository) = repository().await;
    let key = Key::try_from(Bytes::from_static(
        b"novarocks/frontend/mv/accelerator/v3/unknown/future-record",
    ))
    .unwrap();
    let value = Value::try_from(Bytes::from_static(b"opaque-corrupt-record")).unwrap();
    // The store issues the attempt this write is authorised by; a test cannot
    // mint one, which is exactly the property the new contract is after.
    let (attempt, _observation) = store
        .attempts()
        .reserve()
        .expect("reserve one write attempt");
    let mut transaction = store
        .begin_write(attempt, "inject unknown MV Accelerator record")
        .await
        .unwrap();
    transaction
        .put(key.clone(), value, Precondition::Absent)
        .await
        .unwrap();
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));

    repository
        .wipe_accelerator(uuid::Uuid::now_v7())
        .await
        .expect("wipe opaque current record");
    let mut read = store.begin_read().await.unwrap();
    assert!(read.get(&key).await.unwrap().is_none());
    read.abort().await.unwrap();
}
