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

use super::tests_definition;

use crate::repository::{MvRepository, ReplaceMvProjectionRequest};

#[tokio::test]
async fn dependency_indexes_are_replaced_only_with_the_root_projection_cas() {
    let (_host, repository) = tests_definition::repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_request("dependency", b"dependency-object", 51, "orders"),
        )
        .await
        .unwrap();
    let upstream = created.projection.facts.dependencies()[0].relation.clone();
    assert_eq!(
        repository
            .list_dependencies_by_downstream(created.projection.mv_id)
            .await
            .unwrap()
            .len(),
        2
    );

    let replacement =
        tests_definition::projection_request("dependency", b"dependency-object", 52, "customers");
    let replaced = repository
        .replace_projection(
            uuid::Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: created.projection.mv_id,
                expected_version: created.version,
                projection: replacement,
            },
        )
        .await
        .unwrap();
    let dependencies = repository
        .list_dependencies_by_downstream(replaced.projection.mv_id)
        .await
        .unwrap();
    assert_eq!(dependencies.len(), 2);
    assert_eq!(dependencies[0].upstream.name, "customers");
    assert_ne!(
        replaced.projection.facts.dependencies()[0].relation,
        upstream
    );
    repository
        .ensure_no_downstream_dependencies(&dependencies[0].upstream)
        .await
        .expect_err("upstream guard must observe the symmetric index");
}

#[tokio::test]
async fn exact_object_inventory_classification_is_independent_of_load_order() {
    use crate::dependency::MvDependencyObjectType;
    for upstream_first in [false, true] {
        let (_, repository) = tests_definition::repository().await;
        let upstream_request =
            || tests_definition::projection_request("orders", b"base-orders", 2, "other");
        let first = if upstream_first {
            Some(
                repository
                    .create_projection(uuid::Uuid::now_v7(), upstream_request())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let downstream = repository
            .create_projection(
                uuid::Uuid::now_v7(),
                tests_definition::projection_request("downstream", b"downstream", 3, "orders"),
            )
            .await
            .unwrap();
        if !upstream_first {
            let rows = repository
                .list_dependencies_by_downstream(downstream.projection.mv_id)
                .await
                .unwrap();
            assert!(
                rows.iter()
                    .all(|row| row.upstream.object_type == MvDependencyObjectType::Table)
            );
        }
        let upstream = match first {
            Some(value) => value,
            None => repository
                .create_projection(uuid::Uuid::now_v7(), upstream_request())
                .await
                .unwrap(),
        };
        let rows = repository
            .list_dependencies_by_downstream(downstream.projection.mv_id)
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "repeated relation occurrences remain distinct"
        );
        assert!(
            rows.iter()
                .all(|row| row.upstream.object_type == MvDependencyObjectType::MaterializedView)
        );
        repository
            .replace_projection(
                uuid::Uuid::now_v7(),
                ReplaceMvProjectionRequest {
                    mv_id: upstream.projection.mv_id,
                    expected_version: upstream.version,
                    projection: tests_definition::projection_request(
                        "orders",
                        b"replacement-object",
                        4,
                        "other",
                    ),
                },
            )
            .await
            .unwrap();
        let rows = repository
            .list_dependencies_by_downstream(downstream.projection.mv_id)
            .await
            .unwrap();
        assert!(
            rows.iter()
                .all(|row| row.upstream.object_type == MvDependencyObjectType::Table),
            "same FQN with another exact object is not the old MV dependency"
        );
    }
}

#[tokio::test]
async fn incomplete_or_failed_inventory_never_returns_external_classification() {
    use bytes::Bytes;
    use novarocks_state_store_api::{
        CommitOutcome, Precondition, StateStore, StateStoreError, StateStoreErrorKind, Value,
    };
    use novarocks_state_store_runtime::StateStoreRunPolicy;
    use novarocks_state_store_testkit::conformance::FaultInjectingStateStore;
    let (store, repository) = tests_definition::repository().await;
    let downstream = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_request("downstream", b"downstream", 3, "orders"),
        )
        .await
        .unwrap();
    let fault = FaultInjectingStateStore::new(store.clone());
    let failing =
        super::StateStoreMvRepository::open(fault.clone(), StateStoreRunPolicy::default())
            .await
            .unwrap();
    fault.fail_next_operation(StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "inventory unavailable",
    ));
    assert!(
        failing
            .list_dependencies_by_downstream(downstream.projection.mv_id)
            .await
            .is_err()
    );
    let key = super::key::projection_by_id_key(99).unwrap();
    let (attempt, _) = store.attempts().reserve().unwrap();
    let mut write = store
        .begin_write(attempt, "inject unreadable inventory root")
        .await
        .unwrap();
    write
        .put(
            key,
            Value::try_from(Bytes::from_static(b"corrupt root")).unwrap(),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert!(matches!(write.commit().await, CommitOutcome::Committed(_)));
    assert!(
        repository
            .list_dependencies_by_downstream(downstream.projection.mv_id)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exact_catalog_object_match_is_not_lost_when_binding_name_differs() {
    use crate::dependency::{MvDependencyObjectType, MvDependencyStorageEngine};
    let (_, repository) = tests_definition::repository().await;
    repository
        .create_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_request("renamed_orders", b"base-orders", 1, "other"),
        )
        .await
        .unwrap();
    let downstream = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_request("downstream", b"downstream", 3, "orders"),
        )
        .await
        .unwrap();
    let rows = repository
        .list_dependencies_by_downstream(downstream.projection.mv_id)
        .await
        .unwrap();
    assert!(rows.iter().all(|row| row.upstream.object_type
        == MvDependencyObjectType::MaterializedView
        && row.upstream.storage_engine == MvDependencyStorageEngine::Iceberg));
    assert!(
        rows.iter().all(|row| row.upstream.name == "orders"),
        "D retains its binding name"
    );
}

#[tokio::test]
async fn missing_occurrence_index_is_detected_against_the_same_inventory_snapshot() {
    use novarocks_state_store_api::{CommitOutcome, Precondition, StateStore};
    let (store, repository) = tests_definition::repository().await;
    let downstream = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_request("downstream", b"downstream", 3, "orders"),
        )
        .await
        .unwrap();
    let rows = repository
        .list_dependencies_by_downstream(downstream.projection.mv_id)
        .await
        .unwrap();
    let row = &rows[0];
    let key = super::key::dependency_by_downstream_key(
        row.downstream_mv_id,
        &row.upstream,
        row.occurrence_id,
    )
    .unwrap();
    let (attempt, _) = store.attempts().reserve().unwrap();
    let mut write = store
        .begin_write(attempt, "remove one accelerator dependency row")
        .await
        .unwrap();
    let record = write.get(&key).await.unwrap().unwrap();
    write
        .delete(key, Precondition::Version(record.version))
        .await
        .unwrap();
    assert!(matches!(write.commit().await, CommitOutcome::Committed(_)));
    assert!(
        repository
            .list_dependencies_by_downstream(downstream.projection.mv_id)
            .await
            .unwrap_err()
            .to_string()
            .contains("incomplete")
    );
}
