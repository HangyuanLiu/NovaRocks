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
use novarocks::meta::repository::mv::MvMetaRepository;
use novarocks::meta::{MetaKeyPrefix, MetaStoreProvider, SqliteMetaStoreProvider};
use novarocks::mv::application::{
    CreatedMvTarget, MvApplicationErrorKind, MvApplicationService, MvApplicationStatement,
    MvCreateRefreshPolicy, MvCreateStatement, MvEngine, MvEngineError, MvRequestContext,
    PrepareMvCreateRequest, PreparedMvCreate, PreparedMvDefinition,
    UnavailableMvApplicationService,
};
use novarocks::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks::mv::persistence::definition::{
    CreateMvDefinitionRequest, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
use novarocks::mv::persistence::dependency::StoredMvDependency;
use novarocks::mv::persistence::partition::{
    MvPartitionRefreshStatus, RecordFailedMvPartitionStatesRequest,
    ReplaceMvPartitionStatesRequest, StoredMvPartitionState, UpdateMvPartitionContractRequest,
};
use novarocks::mv::persistence::refresh::{
    BeginIcebergMvRefreshRequest, MvRefreshFinalizeRequest, MvRefreshState,
    RecordPublishCommitRequest, RecordStagingCommitRequest, RefreshExternalOutcome,
    StoredMvRefresh, UpdateStarRocksMvRefreshSummaryRequest,
};
use novarocks::mv::persistence::schema::{
    ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
    ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
    MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
    TargetContract, TargetVisibleColumn,
};
use novarocks::mv::repository::{
    CreateMvDependencyRequest, CreateMvRepositoryRequest, FinalizeMvRefreshWithPartitionsRequest,
    InitialMvRefreshConfiguration, MvRepository, MvRepositoryError, MvRepositoryErrorKind,
    MvTarget,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use uuid::Uuid;

#[path = "support/domain_only_mv_repository.rs"]
mod domain_only_mv_repository;
#[path = "support/legacy_mv_repository_adapter.rs"]
mod legacy_mv_repository_adapter;

use domain_only_mv_repository::DomainOnlyMvRepository;
use legacy_mv_repository_adapter::LegacyMvRepositoryAdapter;

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

fn sample_partition_contract(spec_id: i32) -> MvPartitionContract {
    MvPartitionContract {
        target_spec_id: spec_id,
        fields: vec![MvPartitionFieldContract {
            partition_field_id: 1000 + spec_id,
            partition_field_name: if spec_id == 3 {
                "id_bucket_16".to_string()
            } else {
                "id".to_string()
            },
            source_target_field_id: 10,
            source_column_name: "id".to_string(),
            transform: if spec_id == 3 {
                MvPartitionTransformContract::Bucket { num_buckets: 16 }
            } else {
                MvPartitionTransformContract::Identity
            },
        }],
    }
}

fn sample_schema_contract(partition: MvPartitionContract) -> MvSchemaContract {
    MvSchemaContract {
        contract_version: 1,
        base: BaseContract {
            table_fqn: "ice.sales.orders".to_string(),
            table_uuid: "11111111-1111-1111-1111-111111111111".to_string(),
            alias_at_create: Some("orders".to_string()),
            schema_id_at_create: 7,
            schema_at_create: BaseSchemaSnapshot {
                fields: vec![BaseFieldRecord {
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    type_signature: "long".to_string(),
                    required: true,
                }],
            },
        },
        bases: vec![],
        output: OutputContract {
            columns: vec![OutputColumnLineage {
                expression: ExpressionLineage {
                    kind: ExpressionKind::Column,
                    referenced_base_field_ids: vec![1],
                    referenced_base_fields: vec![],
                },
            }],
            filter: None,
        },
        join: None,
        aggregate: None,
        branch: None,
        target: TargetContract {
            table_fqn: "ice.analytics.orders_mv".to_string(),
            table_uuid: "22222222-2222-2222-2222-222222222222".to_string(),
            schema_id_at_create: 11,
            visible_columns: vec![TargetVisibleColumn {
                output_name: "id".to_string(),
                target_field_id: 10,
                type_signature: "long".to_string(),
                nullable: false,
            }],
            hidden_apply_key: HiddenApplyKeyContract {
                column_name: "__nova_base_row_id".to_string(),
                target_field_id: 99,
                source: ApplyKeySource::BaseRowId,
            },
            partition: Some(partition),
        },
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
        let read = provider.begin_read()?;
        let definition = repository
            .load_by_id(read.as_ref(), first_id)?
            .expect("created definition");
        assert_eq!(definition.select_sql, "SELECT id FROM ice.sales.orders_mv");
        assert_eq!(definition.base_table_refs, vec!["ice.sales.orders_mv"]);
        assert_eq!(definition.primary_key_columns, vec!["id"]);
        assert_eq!(definition.storage_engine, "iceberg");
        assert_eq!(definition.target_catalog.as_deref(), Some("ice"));
        assert_eq!(definition.target_namespace.as_deref(), Some("analytics"));
        assert_eq!(definition.target_table.as_deref(), Some("orders_mv"));
        assert_eq!(definition.created_at_ms, 1_700_000_000_000);
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("target index"),
            definition
        );
    }

    {
        let mut txn = provider.begin_write("reserve and create explicit MV")?;
        repository.reserve_definition_id(txn.as_mut(), 3)?;
        repository.create_definition_with_id(txn.as_mut(), 3, request("lineitem_mv"))?;
        txn.commit()?;
    }
    let fourth_id = create_definition(&provider, &repository, "customer_mv")?;
    assert_eq!(fourth_id, 4, "reserved IDs advance the shared allocator");
    {
        let read = provider.begin_read()?;
        let definitions = repository.list_definitions(read.as_ref())?;
        assert_eq!(
            definitions
                .iter()
                .map(|definition| (definition.mv_id, definition.target_table.as_deref()))
                .collect::<Vec<_>>(),
            vec![
                (1, Some("orders_mv")),
                (3, Some("lineitem_mv")),
                (4, Some("customer_mv")),
            ]
        );
        for definition in definitions {
            assert_eq!(
                repository
                    .find_by_target(
                        read.as_ref(),
                        definition.target_catalog.as_deref().unwrap(),
                        definition.target_namespace.as_deref().unwrap(),
                        definition.target_table.as_deref().unwrap(),
                    )?
                    .expect("companion target index"),
                definition
            );
        }
    }

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
        let read = provider.begin_read()?;
        let updated = repository
            .load_by_id(read.as_ref(), first_id)?
            .expect("updated definition");
        assert_eq!(updated.refresh_policy, StoredMvRefreshPolicy::AsyncInterval);
        assert!(updated.refresh_paused);
        assert_eq!(updated.refresh_interval_ms, Some(60_000));
        assert_eq!(updated.max_staleness_ms, Some(120_000));
        assert_eq!(
            updated.last_scheduler_error.as_deref(),
            Some("scheduler paused")
        );
        assert_eq!(updated.next_refresh_after_ms, Some(1_700_000_060_000));
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("target index after metadata update"),
            updated
        );
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
            loaded.last_refresh_table_uuids["ice.sales.orders"],
            "uuid-7"
        );
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
                .expect("case-normalized target"),
            loaded
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
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), simple_refresh_id)?
            .expect("simple refresh intent");
        assert_eq!(refresh.mv_id, mv_id);
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(refresh.target_snapshots["ice.sales.orders"], 10);
        assert_eq!(refresh.external_outcome, None);
        let definition = repository
            .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
            .expect("target index while refresh is active");
        assert!(definition.refresh_in_progress);
        assert_eq!(definition.active_refresh_id, Some(simple_refresh_id));
        assert_eq!(
            definition.refresh_target_snapshots,
            refresh.target_snapshots
        );
    }

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
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), simple_refresh_id)?
            .expect("refresh after external outcome");
        assert_eq!(refresh.state, MvRefreshState::PublishCommitted);
        assert_eq!(
            refresh.external_outcome,
            Some(RefreshExternalOutcome {
                target_snapshot_id: Some(20),
                commit_id: "commit-20".to_string(),
            })
        );
        let definition = repository
            .load_by_id(read.as_ref(), mv_id)?
            .expect("definition after external outcome");
        assert!(definition.refresh_in_progress);
        assert_eq!(definition.active_refresh_id, Some(simple_refresh_id));
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
        assert_eq!(refresh.rows, None);
        assert_eq!(refresh.target_snapshots["ice.sales.orders"], 10);
        assert!(refresh.base_table_uuids.is_empty());
        assert_eq!(
            refresh
                .external_outcome
                .as_ref()
                .and_then(|outcome| outcome.target_snapshot_id),
            Some(20)
        );
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
    {
        let read = provider.begin_read()?;
        let definition = repository
            .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
            .expect("definition after summary update");
        assert_eq!(definition.last_refresh_ms, Some(200));
        assert_eq!(definition.last_refresh_rows, Some(7));
        assert_eq!(definition.last_refresh_snapshots["ice.sales.orders"], 12);
        assert_eq!(
            definition.last_refresh_table_uuids["ice.sales.orders"],
            "uuid-orders"
        );
        assert_eq!(
            repository
                .load_refresh(read.as_ref(), simple_refresh_id)?
                .expect("finalized refresh remains unchanged")
                .state,
            MvRefreshState::Finalized
        );
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
        let refresh = repository
            .load_refresh(read.as_ref(), staged_refresh_id)?
            .expect("branch-staged refresh intent");
        assert_eq!(refresh.mv_id, mv_id);
        assert_eq!(refresh.operation_id, Some(91));
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(refresh.target_catalog.as_deref(), Some("ice"));
        assert_eq!(refresh.target_namespace.as_deref(), Some("analytics"));
        assert_eq!(refresh.target_table.as_deref(), Some("orders_mv"));
        assert_eq!(refresh.staging_branch.as_deref(), Some("nr_refresh_91"));
        assert_eq!(refresh.expected_main_snapshot_id, Some(20));
        assert_eq!(
            refresh.marker.as_ref().map(|marker| marker.token.as_str()),
            Some("marker-91")
        );
        assert_eq!(refresh.target_snapshots["ice.sales.orders"], 11);
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
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), staged_refresh_id)?
            .expect("staging commit");
        assert_eq!(refresh.state, MvRefreshState::StagingCommitted);
        assert_eq!(refresh.staging_snapshot_id, Some(21));
        assert_eq!(refresh.rows, Some(6));
        assert_eq!(refresh.base_table_uuids["ice.sales.orders"], "uuid-orders");
        let definition = repository
            .load_by_id(read.as_ref(), mv_id)?
            .expect("definition after staging commit");
        assert!(definition.refresh_in_progress);
        assert_eq!(definition.active_refresh_id, Some(staged_refresh_id));
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
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), staged_refresh_id)?
            .expect("publish commit");
        assert_eq!(refresh.state, MvRefreshState::PublishCommitted);
        assert_eq!(refresh.published_snapshot_id, Some(22));
        assert_eq!(
            refresh
                .external_outcome
                .as_ref()
                .and_then(|outcome| outcome.target_snapshot_id),
            Some(22)
        );
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("definition after publish commit")
                .active_refresh_id,
            Some(staged_refresh_id)
        );
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
    {
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), staged_refresh_id)?
            .expect("finalized staged refresh");
        assert_eq!(refresh.state, MvRefreshState::Finalized);
        assert_eq!(refresh.rows, Some(6));
        assert_eq!(refresh.published_snapshot_id, Some(22));
        let definition = repository
            .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
            .expect("definition after finalization");
        assert!(!definition.refresh_in_progress);
        assert_eq!(definition.active_refresh_id, None);
        assert_eq!(definition.last_refresh_rows, Some(6));
        assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(22));
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
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("target index after clear")
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
    let old_partition = sample_partition_contract(3);
    let new_partition = sample_partition_contract(4);
    let mv_id = {
        let mut txn = provider.begin_write("create partitioned MV definition")?;
        let mut create = request("orders_mv");
        create.partition_spec = Some(old_partition.clone());
        create.schema_contract = Some(sample_schema_contract(old_partition));
        let definition = repository.create_definition(txn.as_mut(), create)?;
        txn.commit()?;
        definition.mv_id
    };
    let revision_before_partition_update = {
        let read = provider.begin_read()?;
        repository
            .load_versioned_by_id(read.as_ref(), mv_id)?
            .expect("versioned definition before partition update")
            .record_revision
    };
    {
        let mut txn = provider.begin_write("update partition contract")?;
        let updated = repository.update_partition_contract(
            txn.as_mut(),
            UpdateMvPartitionContractRequest {
                mv_id,
                partition_spec: new_partition.clone(),
            },
        )?;
        assert_eq!(updated.partition_spec.as_ref(), Some(&new_partition));
        assert_eq!(
            updated
                .schema_contract
                .as_ref()
                .and_then(|contract| contract.target.partition.as_ref()),
            Some(&new_partition)
        );
        assert!(!updated.partition_state_complete);
        txn.commit()?;
    }
    {
        let read = provider.begin_read()?;
        let versioned = repository
            .load_versioned_by_id(read.as_ref(), mv_id)?
            .expect("versioned definition after partition update");
        assert_ne!(
            versioned.record_revision, revision_before_partition_update,
            "partition contract update must persist a new definition revision"
        );
        assert_eq!(
            versioned.value.partition_spec.as_ref(),
            Some(&new_partition)
        );
        assert_eq!(
            versioned
                .value
                .schema_contract
                .as_ref()
                .and_then(|contract| contract.target.partition.as_ref()),
            Some(&new_partition)
        );
        assert!(!versioned.value.partition_state_complete);
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("target lookup after partition update"),
            versioned.value,
            "target lookup must resolve the updated persisted definition"
        );
        assert_eq!(
            mv_records(&provider)?,
            vec![
                ("by-id/1".to_string(), "mv.definition".to_string()),
                (
                    "by-target/ice/analytics/orders_mv".to_string(),
                    "mv.target_lookup".to_string(),
                ),
            ],
            "updating the definition must leave the target index intact"
        );
    }

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
        let read = provider.begin_read()?;
        let states = repository.list_partition_states(read.as_ref(), mv_id)?;
        assert_eq!(
            states
                .iter()
                .map(|state| state.partition_key.as_str())
                .collect::<Vec<_>>(),
            vec!["region=east", "region=west/slash"]
        );
        for state in &states {
            assert_eq!(state.mv_id, mv_id);
            assert_eq!(state.status, MvPartitionRefreshStatus::Fresh);
            assert_eq!(state.last_refresh_ms, Some(100));
            assert_eq!(state.base_snapshots["ice.sales.orders"], 7);
            assert_eq!(state.target_snapshot_id, Some(10));
            assert_eq!(state.last_refresh_id, Some(refresh_id));
            assert_eq!(state.failure_message, None);
        }
        let definition = repository
            .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
            .expect("definition after partition replacement");
        assert!(definition.partition_state_complete);
        assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(10));
    }

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
        assert_eq!(states[0].mv_id, mv_id);
        assert_eq!(states[0].partition_key, "region=east");
        assert_eq!(states[0].status, MvPartitionRefreshStatus::Failed);
        assert_eq!(states[0].failure_message.as_deref(), Some("writer failed"));
        assert_eq!(states[0].last_refresh_ms, Some(101));
        assert_eq!(states[0].base_snapshots["ice.sales.orders"], 8);
        assert_eq!(states[0].target_snapshot_id, Some(10));
        assert_eq!(states[0].last_refresh_id, Some(refresh_id + 1));
        let definition = repository
            .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
            .expect("definition after failed partition write");
        assert!(definition.partition_state_complete);
        assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(10));
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
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("target index after compaction adoption")
                .last_refreshed_iceberg_snapshot_id,
            Some(11)
        );
        let states = repository.list_partition_states(read.as_ref(), mv_id)?;
        assert_eq!(states.len(), 1);
        assert_eq!(
            states[0].target_snapshot_id,
            Some(10),
            "compaction adoption updates the definition watermark, not partition-state DTOs"
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
    assert_eq!(
        repository
            .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
            .expect("target index after partition clear")
            .partition_state_complete,
        false
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
            repository.list_dependencies_by_downstream(read.as_ref(), first_mv_id)?,
            vec![
                StoredMvDependency {
                    downstream_mv_id: first_mv_id,
                    upstream: customers.clone(),
                    created_at_ms: 11,
                },
                StoredMvDependency {
                    downstream_mv_id: first_mv_id,
                    upstream: orders.clone(),
                    created_at_ms: 10,
                },
            ]
        );
        assert_eq!(
            repository.list_downstream_dependencies(read.as_ref(), &orders)?,
            vec![
                StoredMvDependency {
                    downstream_mv_id: first_mv_id,
                    upstream: orders.clone(),
                    created_at_ms: 10,
                },
                StoredMvDependency {
                    downstream_mv_id: second_mv_id,
                    upstream: orders.clone(),
                    created_at_ms: 12,
                },
            ]
        );
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("first target index")
                .mv_id,
            first_mv_id
        );
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "lineitem_mv")?
                .expect("second target index")
                .mv_id,
            second_mv_id
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
            repository.list_downstream_dependencies(read.as_ref(), &orders)?,
            vec![StoredMvDependency {
                downstream_mv_id: second_mv_id,
                upstream: orders.clone(),
                created_at_ms: 12,
            }]
        );
        assert_eq!(
            repository.list_downstream_dependencies(read.as_ref(), &customers)?,
            vec![StoredMvDependency {
                downstream_mv_id: first_mv_id,
                upstream: customers.clone(),
                created_at_ms: 20,
            }]
        );
        assert_eq!(
            repository.list_dependencies_by_downstream(read.as_ref(), first_mv_id)?,
            vec![StoredMvDependency {
                downstream_mv_id: first_mv_id,
                upstream: customers.clone(),
                created_at_ms: 20,
            }]
        );
        assert_eq!(
            repository
                .find_by_target(read.as_ref(), "ice", "analytics", "orders_mv")?
                .expect("first target index after dependency replacement")
                .mv_id,
            first_mv_id
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

struct ProviderNeutralFakeEngine;

impl MvEngine for ProviderNeutralFakeEngine {
    fn prepare_create(
        &self,
        _request: PrepareMvCreateRequest<'_>,
        _repository: &dyn MvRepository,
    ) -> Result<PreparedMvCreate, MvEngineError> {
        panic!("unavailable application service must not call prepare_create")
    }

    fn create_target(
        &self,
        _plan: &PreparedMvCreate,
        _operation_id: Uuid,
    ) -> Result<CreatedMvTarget, MvEngineError> {
        panic!("unavailable application service must not create a target")
    }

    fn inspect_created_target(
        &self,
        _plan: &PreparedMvCreate,
        _target: &CreatedMvTarget,
    ) -> Result<PreparedMvDefinition, MvEngineError> {
        panic!("unavailable application service must not inspect a target")
    }

    fn sync_target_descriptor(
        &self,
        _target: &CreatedMvTarget,
        _definition: &novarocks::mv::persistence::definition::StoredMvDefinition,
    ) -> Result<(), MvEngineError> {
        panic!("unavailable application service must not sync a descriptor")
    }

    fn register_target(&self, _target: &CreatedMvTarget) -> Result<(), MvEngineError> {
        panic!("unavailable application service must not register a target")
    }

    fn drop_created_target(&self, _target: &CreatedMvTarget) -> Result<(), MvEngineError> {
        panic!("unavailable application service must not drop a target")
    }
}

#[test]
fn core_ports_are_provider_neutral_and_unavailable_create_has_stable_error() {
    fn accepts_repository(_repository: &dyn MvRepository) {}
    accepts_repository(&DomainOnlyMvRepository);
    assert_eq!(
        DomainOnlyMvRepository.availability(),
        novarocks::mv::repository::MvRepositoryAvailability::Available
    );

    let query = Parser::parse_sql(&GenericDialect, "SELECT id FROM orders")
        .expect("query")
        .pop()
        .and_then(|statement| match statement {
            sqlparser::ast::Statement::Query(query) => Some(*query),
            _ => None,
        })
        .expect("SELECT query");
    let statement = MvApplicationStatement::Create(MvCreateStatement {
        name_parts: vec!["orders_mv".to_string()],
        if_not_exists: false,
        partition_by: None,
        distribution: None,
        refresh_policy: MvCreateRefreshPolicy::Manual,
        select_sql: "SELECT id FROM orders".to_string(),
        select_query: query,
        properties: vec![("storage_engine".to_string(), "iceberg".to_string())],
        primary_key: None,
    });

    let error = UnavailableMvApplicationService
        .try_handle_statement(
            &ProviderNeutralFakeEngine,
            &statement,
            MvRequestContext {
                current_catalog: Some("ice"),
                current_database: "analytics",
            },
        )
        .expect_err("CREATE must report the missing StateStore-backed service");
    assert_eq!(error.kind(), MvApplicationErrorKind::Unavailable);
    assert_eq!(
        error.to_string(),
        "materialized view service requires [state_store]"
    );

    let repository_error = MvRepositoryError::new(
        MvRepositoryErrorKind::KnownCommittedFinalizeFailed,
        "descriptor sync failed after commit",
    );
    assert_eq!(
        repository_error.kind(),
        MvRepositoryErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(
        repository_error.message(),
        "descriptor sync failed after commit"
    );

    let canonical_refresh: Option<StoredMvRefresh> = None;
    let canonical_partition: Option<StoredMvPartitionState> = None;
    assert!(canonical_refresh.is_none());
    assert!(canonical_partition.is_none());
}

#[test]
fn legacy_adapter_runs_compound_repository_ledger_through_port() -> TestResult {
    let dir = tempfile::tempdir()?;
    let adapter = LegacyMvRepositoryAdapter::open(dir.path().join("meta.sqlite"))?;
    let repository: &dyn MvRepository = &adapter;
    let dependency = iceberg_table("sales", "orders");

    let definition = repository.create(
        Uuid::from_u128(1),
        CreateMvRepositoryRequest {
            definition: request("orders_mv"),
            refresh: InitialMvRefreshConfiguration {
                policy: StoredMvRefreshPolicy::AsyncInterval,
                paused: false,
                interval_ms: Some(60_000),
                max_staleness_ms: Some(120_000),
                next_refresh_after_ms: Some(1_700_000_060_000),
            },
            dependencies: vec![CreateMvDependencyRequest {
                upstream: dependency.clone(),
                created_at_ms: 1_700_000_000_000,
            }],
        },
    )?;
    assert_eq!(
        definition.refresh_policy,
        StoredMvRefreshPolicy::AsyncInterval
    );
    assert_eq!(
        repository
            .find_by_target(&MvTarget {
                catalog: Some("ice".to_string()),
                database: "analytics".to_string(),
                name: "orders_mv".to_string(),
            })?
            .expect("target lookup")
            .mv_id,
        definition.mv_id
    );
    assert_eq!(
        repository
            .list_dependencies_by_downstream(definition.mv_id)?
            .len(),
        1
    );

    let refresh = repository.begin_refresh_intent(
        definition.mv_id,
        BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
    )?;
    repository.record_external_commit_outcome(
        refresh.refresh_id,
        RefreshExternalOutcome {
            target_snapshot_id: Some(20),
            commit_id: "commit-20".to_string(),
        },
    )?;
    repository.finalize_refresh_with_partitions(FinalizeMvRefreshWithPartitionsRequest {
        refresh: MvRefreshFinalizeRequest {
            refresh_id: refresh.refresh_id,
            rows: 5,
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "uuid-orders".to_string(),
            )]),
            target_snapshot_id: Some(20),
        },
        partitions: Some(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["region=east".to_string()]),
            last_refresh_ms: 1_700_000_120_000,
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
            target_snapshot_id: Some(20),
            last_refresh_id: refresh.refresh_id,
            max_entries: 10,
        }),
    })?;

    assert_eq!(
        repository
            .load_refresh(refresh.refresh_id)?
            .expect("refresh")
            .state,
        MvRefreshState::Finalized
    );
    assert_eq!(
        repository.list_partition_states(definition.mv_id)?,
        vec![StoredMvPartitionState {
            mv_id: definition.mv_id,
            partition_key: "region=east".to_string(),
            status: MvPartitionRefreshStatus::Fresh,
            last_refresh_ms: Some(1_700_000_120_000),
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
            target_snapshot_id: Some(20),
            last_refresh_id: Some(refresh.refresh_id),
            failure_message: None,
        }]
    );
    repository.ensure_no_downstream_dependencies(&iceberg_table("sales", "customers"))?;
    assert!(repository.drop_by_id(definition.mv_id)?);
    assert!(repository.list_definitions()?.is_empty());
    Ok(())
}
