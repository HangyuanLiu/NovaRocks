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

use std::collections::{BTreeMap, BTreeSet};

use novarocks::meta::keys::NS_MV;
use novarocks::meta::repository::mv::{
    BeginIcebergMvRefreshRequest, CreateMvDefinitionRequest, CreateMvDependencyRequest,
    MvMetaRepository, MvPartitionRefreshStatus, MvRefreshFinalizeRequest, MvRefreshState,
    RecordFailedMvPartitionStatesRequest, RecordPublishCommitRequest, RecordStagingCommitRequest,
    RefreshExternalOutcome, ReplaceMvPartitionStatesRequest, UpdateMvRefreshMetadataRequest,
    UpdateStarRocksMvRefreshSummaryRequest,
};
use novarocks::meta::{MetaKeyPrefix, MetaStoreProvider, SqliteMetaStoreProvider};
use novarocks::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks::mv::persistence::definition::StoredMvRefreshPolicy;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn request(target: &str) -> CreateMvDefinitionRequest {
    CreateMvDefinitionRequest {
        select_sql: format!("SELECT id FROM ice.sales.{target}"),
        base_table_refs: vec![format!("ice.sales.{target}")],
        primary_key_columns: vec!["id".to_string()],
        storage_engine: "iceberg".to_string(),
        target_catalog: Some("ice".to_string()),
        target_namespace: Some("analytics".to_string()),
        target_table: Some(target.to_string()),
        schema_contract: None,
        partition_spec: None,
        created_at_ms: 1_700_000_000_000,
    }
}

fn iceberg_table(namespace: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some("ice".to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

fn mv_records(
    provider: &SqliteMetaStoreProvider,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let read = provider.begin_read()?;
    let mut records = read
        .scan(
            &MetaKeyPrefix::new(NS_MV, std::iter::empty::<&str>())?,
            None,
        )?
        .into_iter()
        .map(|record| {
            (
                record.key.canonical_path(),
                record.kind.as_str().to_string(),
            )
        })
        .collect::<Vec<_>>();
    records.sort();
    Ok(records)
}

fn create_definition(
    provider: &SqliteMetaStoreProvider,
    repository: &MvMetaRepository,
    target: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut txn = provider.begin_write("create MV contract fixture")?;
    let definition = repository.create_definition(txn.as_mut(), request(target))?;
    txn.commit()?;
    Ok(definition.mv_id)
}

#[test]
fn definition_ledger_freezes_compound_records_and_public_reads() -> TestResult {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository;

    let first_id = create_definition(&provider, &repository, "orders_mv")?;
    assert_eq!(first_id, 1);
    assert_eq!(
        mv_records(&provider)?,
        vec![
            ("by-id/1".to_string(), "mv.definition".to_string()),
            (
                "by-target/ice/analytics/orders_mv".to_string(),
                "mv.target_lookup".to_string(),
            ),
        ]
    );

    {
        let mut txn = provider.begin_write("reserve and create explicit MV")?;
        repository.reserve_definition_id(txn.as_mut(), 3)?;
        repository.create_definition_with_id(txn.as_mut(), 3, request("lineitem_mv"))?;
        txn.commit()?;
    }
    let fourth_id = create_definition(&provider, &repository, "customer_mv")?;
    assert_eq!(fourth_id, 4, "reserved IDs advance the shared allocator");

    {
        let mut txn = provider.begin_write("update MV metadata")?;
        let updated = repository.update_refresh_metadata(
            txn.as_mut(),
            UpdateMvRefreshMetadataRequest {
                mv_id: first_id,
                refresh_policy: StoredMvRefreshPolicy::AsyncInterval,
                refresh_paused: true,
                refresh_interval_ms: Some(60_000),
                max_staleness_ms: Some(120_000),
                last_scheduler_error: Some("scheduler paused".to_string()),
                next_refresh_after_ms: Some(1_700_000_060_000),
            },
        )?;
        assert_eq!(updated.refresh_policy, StoredMvRefreshPolicy::AsyncInterval);
        txn.commit()?;
    }
    {
        let mut txn = provider.begin_write("set rebuilt MV watermark")?;
        repository.set_rebuilt_refresh_watermark(
            txn.as_mut(),
            first_id,
            BTreeMap::from([("ice.sales.orders".to_string(), 7)]),
            BTreeMap::from([("ice.sales.orders".to_string(), "uuid-7".to_string())]),
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let loaded = repository
            .load_by_id(read.as_ref(), first_id)?
            .expect("definition by id");
        assert_eq!(loaded.last_refresh_snapshots["ice.sales.orders"], 7);
        assert_eq!(
            repository
                .load_versioned_by_id(read.as_ref(), first_id)?
                .expect("versioned definition")
                .value,
            loaded
        );
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ICE", "ANALYTICS", "ORDERS_MV")?
                .expect("case-normalized target")
                .mv_id,
            first_id
        );
        assert_eq!(
            repository
                .list_definitions(read.as_ref())?
                .into_iter()
                .map(|definition| definition.mv_id)
                .collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
    }

    {
        let mut txn = provider.begin_write("drop MV definitions")?;
        assert!(repository.drop_by_target(txn.as_mut(), "ice", "analytics", "orders_mv")?);
        assert!(repository.drop_by_id(txn.as_mut(), 3)?);
        assert!(repository.drop_by_id(txn.as_mut(), 4)?);
        txn.commit()?;
    }
    assert!(mv_records(&provider)?.is_empty());
    Ok(())
}

#[test]
fn refresh_ledger_freezes_state_transitions_and_compound_definition_updates() -> TestResult {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository;
    let mv_id = create_definition(&provider, &repository, "orders_mv")?;

    let simple_refresh_id = {
        let mut txn = provider.begin_write("begin simple refresh")?;
        let refresh = repository.begin_refresh_intent(
            txn.as_mut(),
            mv_id,
            BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
        )?;
        txn.commit()?;
        refresh.refresh_id
    };
    assert_eq!(
        mv_records(&provider)?,
        vec![
            ("by-id/1".to_string(), "mv.definition".to_string()),
            (
                "by-target/ice/analytics/orders_mv".to_string(),
                "mv.target_lookup".to_string(),
            ),
            ("refresh/1".to_string(), "mv.refresh".to_string()),
        ]
    );

    {
        let mut txn = provider.begin_write("record external outcome")?;
        repository.record_external_commit_outcome(
            txn.as_mut(),
            simple_refresh_id,
            RefreshExternalOutcome {
                target_snapshot_id: Some(20),
                commit_id: "commit-20".to_string(),
            },
        )?;
        txn.commit()?;
    }
    {
        let mut txn = provider.begin_write("finalize simple refresh")?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id: simple_refresh_id,
                rows: 5,
                base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
                base_table_uuids: BTreeMap::from([(
                    "ice.sales.orders".to_string(),
                    "uuid-orders".to_string(),
                )]),
                target_snapshot_id: Some(20),
            },
        )?;
        txn.commit()?;
    }
    {
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), simple_refresh_id)?
            .expect("simple refresh");
        assert_eq!(refresh.state, MvRefreshState::Finalized);
        let definition = repository
            .load_by_id(read.as_ref(), mv_id)?
            .expect("definition");
        assert!(!definition.refresh_in_progress);
        assert_eq!(definition.active_refresh_id, None);
        assert_eq!(definition.last_refresh_rows, Some(5));
        assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(20));
        assert!(
            repository
                .list_unfinished_refreshes(read.as_ref())?
                .is_empty()
        );
    }
    {
        let mut txn = provider.begin_write("update refresh summary")?;
        assert!(repository.update_starrocks_refresh_summary_if_present(
            txn.as_mut(),
            UpdateStarRocksMvRefreshSummaryRequest {
                mv_id,
                last_refresh_ms: 200,
                last_refresh_rows: 7,
                base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 12)]),
                base_table_uuids: BTreeMap::from([(
                    "ice.sales.orders".to_string(),
                    "uuid-orders".to_string(),
                )]),
            },
        )?);
        txn.commit()?;
    }

    let staged_refresh_id = {
        let mut txn = provider.begin_write("begin branch-staged refresh")?;
        let refresh = repository.begin_iceberg_refresh_intent(
            txn.as_mut(),
            BeginIcebergMvRefreshRequest {
                mv_id,
                operation_id: Some(91),
                target_catalog: "ice".to_string(),
                target_namespace: "analytics".to_string(),
                target_table: "orders_mv".to_string(),
                staging_branch: "nr_refresh_91".to_string(),
                expected_main_snapshot_id: Some(20),
                base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 11)]),
                marker_token: "marker-91".to_string(),
            },
        )?;
        txn.commit()?;
        refresh.refresh_id
    };
    {
        let read = provider.begin_read()?;
        assert_eq!(
            repository
                .list_unfinished_branch_staged_iceberg_refreshes(read.as_ref())?
                .into_iter()
                .map(|refresh| refresh.refresh_id)
                .collect::<Vec<_>>(),
            vec![staged_refresh_id]
        );
    }
    {
        let mut txn = provider.begin_write("record staging commit")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id: staged_refresh_id,
                staging_snapshot_id: 21,
                rows: 6,
                base_table_uuids: BTreeMap::from([(
                    "ice.sales.orders".to_string(),
                    "uuid-orders".to_string(),
                )]),
            },
        )?;
        txn.commit()?;
    }
    {
        let mut txn = provider.begin_write("record publish commit")?;
        repository.record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id: staged_refresh_id,
                published_snapshot_id: 22,
            },
        )?;
        txn.commit()?;
    }
    {
        let mut txn = provider.begin_write("finalize branch-staged refresh")?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id: staged_refresh_id,
                rows: 6,
                base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 11)]),
                base_table_uuids: BTreeMap::from([(
                    "ice.sales.orders".to_string(),
                    "uuid-orders".to_string(),
                )]),
                target_snapshot_id: Some(22),
            },
        )?;
        txn.commit()?;
    }

    let aborted_refresh_id = {
        let mut txn = provider.begin_write("begin refresh to clear")?;
        let refresh = repository.begin_refresh_intent(txn.as_mut(), mv_id, BTreeMap::new())?;
        assert!(repository.clear_refresh_progress(txn.as_mut(), mv_id)?);
        txn.commit()?;
        refresh.refresh_id
    };
    {
        let read = provider.begin_read()?;
        assert_eq!(
            repository
                .load_refresh(read.as_ref(), aborted_refresh_id)?
                .expect("aborted refresh")
                .state,
            MvRefreshState::Aborted
        );
        assert_eq!(
            repository
                .load_by_id(read.as_ref(), mv_id)?
                .expect("definition")
                .active_refresh_id,
            None
        );
    }

    let unknown_refresh_id = {
        let mut txn = provider.begin_write("begin refresh for commit-unknown")?;
        let refresh = repository.begin_refresh_intent(txn.as_mut(), mv_id, BTreeMap::new())?;
        repository.mark_refresh_commit_unknown(txn.as_mut(), refresh.refresh_id)?;
        txn.commit()?;
        refresh.refresh_id
    };
    {
        let read = provider.begin_read()?;
        let unfinished = repository.list_unfinished_refreshes(read.as_ref())?;
        assert_eq!(unfinished.len(), 1);
        assert_eq!(unfinished[0].refresh_id, unknown_refresh_id);
        assert_eq!(unfinished[0].state, MvRefreshState::CommitUnknown);
        let definition = repository
            .load_by_id(read.as_ref(), mv_id)?
            .expect("definition");
        assert_eq!(definition.active_refresh_id, Some(unknown_refresh_id));
        assert!(definition.refresh_in_progress);
    }
    {
        let mut txn = provider.begin_write("reject clearing commit-unknown refresh")?;
        let err = repository
            .clear_refresh_progress(txn.as_mut(), mv_id)
            .expect_err("commit-unknown state must not be guessed aborted");
        assert!(
            err.to_string().contains("is commit-unknown"),
            "unexpected error: {err}"
        );
        txn.abort()?;
    }
    Ok(())
}

#[test]
fn partition_ledger_freezes_replace_fail_clear_and_snapshot_adoption() -> TestResult {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository;
    let mv_id = create_definition(&provider, &repository, "orders_mv")?;

    let refresh_id = {
        let mut txn = provider.begin_write("seed refresh baseline")?;
        let refresh = repository.begin_refresh_intent(txn.as_mut(), mv_id, BTreeMap::new())?;
        repository.record_external_commit_outcome(
            txn.as_mut(),
            refresh.refresh_id,
            RefreshExternalOutcome {
                target_snapshot_id: Some(10),
                commit_id: "commit-10".to_string(),
            },
        )?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id: refresh.refresh_id,
                rows: 1,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: BTreeMap::new(),
                target_snapshot_id: Some(10),
            },
        )?;
        txn.commit()?;
        refresh.refresh_id
    };

    {
        let mut txn = provider.begin_write("replace partition states")?;
        repository.replace_partition_states(
            txn.as_mut(),
            ReplaceMvPartitionStatesRequest {
                mv_id,
                partition_keys: BTreeSet::from([
                    "region=east".to_string(),
                    "region=west/slash".to_string(),
                ]),
                last_refresh_ms: 100,
                base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 7)]),
                target_snapshot_id: Some(10),
                last_refresh_id: refresh_id,
                max_entries: 10,
            },
        )?;
        txn.commit()?;
    }
    assert_eq!(
        mv_records(&provider)?,
        vec![
            ("by-id/1".to_string(), "mv.definition".to_string()),
            (
                "by-target/ice/analytics/orders_mv".to_string(),
                "mv.target_lookup".to_string(),
            ),
            (
                "partition-state/1/region%3Deast".to_string(),
                "mv.partition_state".to_string(),
            ),
            (
                "partition-state/1/region%3Dwest%2Fslash".to_string(),
                "mv.partition_state".to_string(),
            ),
            ("refresh/1".to_string(), "mv.refresh".to_string()),
        ]
    );

    {
        let mut txn = provider.begin_write("replace with failed partition state")?;
        repository.record_failed_partition_states(
            txn.as_mut(),
            RecordFailedMvPartitionStatesRequest {
                mv_id,
                partition_keys: BTreeSet::from(["region=east".to_string()]),
                failure_message: "writer failed".to_string(),
                last_refresh_ms: 101,
                base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 8)]),
                target_snapshot_id: Some(10),
                last_refresh_id: refresh_id + 1,
                max_entries: 10,
            },
        )?;
        txn.commit()?;
    }
    {
        let read = provider.begin_read()?;
        let states = repository.list_partition_states(read.as_ref(), mv_id)?;
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].status, MvPartitionRefreshStatus::Failed);
        assert_eq!(states[0].failure_message.as_deref(), Some("writer failed"));
    }

    {
        let mut txn = provider.begin_write("adopt compacted target snapshot")?;
        assert!(repository.adopt_target_compaction_snapshot(
            txn.as_mut(),
            "ice",
            "analytics",
            "orders_mv",
            10,
            11,
        )?);
        txn.commit()?;
    }
    {
        let read = provider.begin_read()?;
        assert_eq!(
            repository
                .load_by_id(read.as_ref(), mv_id)?
                .expect("definition")
                .last_refreshed_iceberg_snapshot_id,
            Some(11)
        );
    }

    {
        let mut txn = provider.begin_write("clear partition states")?;
        assert!(repository.clear_partition_states(txn.as_mut(), mv_id)?);
        txn.commit()?;
    }
    assert!(
        mv_records(&provider)?
            .iter()
            .all(|(path, _)| !path.starts_with("partition-state/"))
    );
    let read = provider.begin_read()?;
    assert!(
        repository
            .list_partition_states(read.as_ref(), mv_id)?
            .is_empty()
    );
    assert!(
        !repository
            .load_by_id(read.as_ref(), mv_id)?
            .expect("definition")
            .partition_state_complete
    );
    Ok(())
}

#[test]
fn dependency_ledger_freezes_both_indexes_replace_delete_guard_and_drop_cleanup() -> TestResult {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository;
    let first_mv_id = create_definition(&provider, &repository, "orders_mv")?;
    let second_mv_id = create_definition(&provider, &repository, "lineitem_mv")?;
    let orders = iceberg_table("sales", "orders");
    let customers = iceberg_table("sales", "customers");

    {
        let mut txn = provider.begin_write("write dependency indexes")?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            first_mv_id,
            vec![
                CreateMvDependencyRequest {
                    upstream: orders.clone(),
                    created_at_ms: 10,
                },
                CreateMvDependencyRequest {
                    upstream: customers.clone(),
                    created_at_ms: 11,
                },
            ],
        )?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            second_mv_id,
            vec![CreateMvDependencyRequest {
                upstream: orders.clone(),
                created_at_ms: 12,
            }],
        )?;
        txn.commit()?;
    }

    let records = mv_records(&provider)?;
    let dependency_records = records
        .iter()
        .filter(|(path, _)| path.starts_with("dependency/"))
        .collect::<Vec<_>>();
    assert_eq!(dependency_records.len(), 6, "three edges have two indexes");
    assert!(
        dependency_records
            .iter()
            .all(|(_, kind)| kind == "mv.dependency")
    );

    {
        let read = provider.begin_read()?;
        assert_eq!(
            repository
                .list_dependencies_by_downstream(read.as_ref(), first_mv_id)?
                .into_iter()
                .map(|dependency| dependency.upstream.name)
                .collect::<Vec<_>>(),
            vec!["customers", "orders"]
        );
        assert_eq!(
            repository
                .list_downstream_dependencies(read.as_ref(), &orders)?
                .into_iter()
                .map(|dependency| dependency.downstream_mv_id)
                .collect::<Vec<_>>(),
            vec![first_mv_id, second_mv_id]
        );
        let err = repository
            .ensure_no_downstream_dependencies(read.as_ref(), &orders)
            .expect_err("drop guard must report both downstream definitions");
        assert_eq!(
            err.to_string(),
            "metadata repository conflict: ice.sales.orders has downstream materialized views: 1, 2"
        );
    }

    {
        let mut txn = provider.begin_write("replace dependency indexes")?;
        repository.replace_dependencies_for_mv(
            txn.as_mut(),
            first_mv_id,
            vec![CreateMvDependencyRequest {
                upstream: customers.clone(),
                created_at_ms: 20,
            }],
        )?;
        txn.commit()?;
    }
    {
        let read = provider.begin_read()?;
        assert_eq!(
            repository
                .list_downstream_dependencies(read.as_ref(), &orders)?
                .into_iter()
                .map(|dependency| dependency.downstream_mv_id)
                .collect::<Vec<_>>(),
            vec![second_mv_id]
        );
        assert_eq!(
            repository
                .list_downstream_dependencies(read.as_ref(), &customers)?
                .into_iter()
                .map(|dependency| dependency.downstream_mv_id)
                .collect::<Vec<_>>(),
            vec![first_mv_id]
        );
    }

    {
        let mut txn = provider.begin_write("delete and drop dependency owners")?;
        repository.delete_dependencies_for_mv(txn.as_mut(), second_mv_id)?;
        assert!(repository.drop_by_id(txn.as_mut(), first_mv_id)?);
        txn.commit()?;
    }
    assert!(
        mv_records(&provider)?
            .iter()
            .all(|(path, _)| !path.starts_with("dependency/"))
    );
    Ok(())
}
