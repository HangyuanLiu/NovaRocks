use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::mv::persistence::partition::ReplaceMvPartitionStatesRequest;
use novarocks::mv::persistence::refresh::{
    MvRefreshFinalizeRequest, MvRefreshState, RecordPublishCommitRequest,
    RecordStagingCommitRequest,
};
use novarocks::mv::repository::{MvRepository, MvRepositoryErrorKind};
use novarocks_frontend::mv::repository::StateStoreMvRepository;
use novarocks_spi::state_store::FeDeploymentView;
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

#[path = "mv_repository_definition.rs"]
mod definition_support;

fn repository() -> (
    tempfile::TempDir,
    tokio::runtime::Runtime,
    StateStoreHost,
    Arc<StateStoreMvRepository>,
) {
    let temp = tempfile::tempdir().expect("temporary StateStore directory");
    let runtime = tokio::runtime::Runtime::new().expect("repository runtime");
    let registry = builtin_state_store_provider_registry().expect("built-in StateStore providers");
    let host = runtime
        .block_on(StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "mv-refresh-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: "mv-refresh-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"mv-refresh-test-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        ))
        .expect("open SQLite StateStore host");
    let repository = runtime
        .block_on(StateStoreMvRepository::open(
            host.state_store().expect("host exposes StateStore"),
            runtime.handle().clone(),
        ))
        .expect("open MV repository");
    (temp, runtime, host, repository)
}

#[test]
fn refresh_lifecycle_persists_transitions_and_finalizes_definition() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_refresh"),
        )
        .expect("create definition");
    let base_snapshots = BTreeMap::from([("ice.sales.orders".to_string(), 42)]);
    let refresh = repository
        .begin_refresh_intent(definition.mv_id, base_snapshots.clone())
        .expect("begin refresh");
    assert_eq!(refresh.state, MvRefreshState::IntentCreated);
    repository
        .record_staging_commit(RecordStagingCommitRequest {
            refresh_id: refresh.refresh_id,
            staging_snapshot_id: 43,
            rows: 7,
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "uuid-1".to_string(),
            )]),
        })
        .expect("record staging commit");
    repository
        .record_publish_commit(RecordPublishCommitRequest {
            refresh_id: refresh.refresh_id,
            published_snapshot_id: 44,
        })
        .expect("record publish commit");
    repository
        .finalize_refresh(MvRefreshFinalizeRequest {
            refresh_id: refresh.refresh_id,
            rows: 7,
            base_snapshots: base_snapshots.clone(),
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "uuid-1".to_string(),
            )]),
            target_snapshot_id: Some(44),
        })
        .expect("finalize refresh");
    assert_eq!(
        repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .expect("refresh exists")
            .state,
        MvRefreshState::Finalized
    );
    let stored = repository
        .load_by_id(definition.mv_id)
        .expect("load definition")
        .expect("definition exists");
    assert!(!stored.refresh_in_progress);
    assert_eq!(stored.last_refreshed_iceberg_snapshot_id, Some(44));
}

#[test]
fn unfinished_refreshes_preserve_conflict_and_commit_unknown_guards() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_refresh_guard"),
        )
        .expect("create definition");
    let refresh = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin refresh");
    let duplicate = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect_err("second intent conflicts");
    assert_eq!(duplicate.kind(), MvRepositoryErrorKind::Conflict);
    repository
        .mark_refresh_commit_unknown(refresh.refresh_id)
        .expect("mark unknown");
    assert_eq!(
        repository
            .list_unfinished_refreshes()
            .expect("list unfinished"),
        vec![
            repository
                .load_refresh(refresh.refresh_id)
                .expect("load refresh")
                .expect("refresh exists")
        ]
    );
    let clear = repository
        .clear_refresh_progress(definition.mv_id)
        .expect_err("commit unknown cannot be cleared");
    assert_eq!(clear.kind(), MvRepositoryErrorKind::Conflict);
}

#[test]
fn refresh_commands_return_not_found_for_missing_definition_and_refresh() {
    let (_temp, _runtime, _host, repository) = repository();
    assert_eq!(
        repository
            .begin_refresh_intent(99, BTreeMap::new())
            .expect_err("missing definition")
            .kind(),
        MvRepositoryErrorKind::NotFound
    );
    assert_eq!(
        repository
            .record_staging_commit(RecordStagingCommitRequest {
                refresh_id: 99,
                staging_snapshot_id: 1,
                rows: 1,
                base_table_uuids: BTreeMap::new(),
            })
            .expect_err("missing refresh")
            .kind(),
        MvRepositoryErrorKind::NotFound
    );
}

#[test]
fn dropping_a_finalized_mv_removes_refresh_and_partition_records_before_reopen() {
    let (_temp, runtime, host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_drop_cleanup"),
        )
        .expect("create definition");
    let refresh = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin refresh");
    repository
        .record_staging_commit(RecordStagingCommitRequest {
            refresh_id: refresh.refresh_id,
            staging_snapshot_id: 1,
            rows: 1,
            base_table_uuids: BTreeMap::new(),
        })
        .expect("stage refresh");
    repository
        .record_publish_commit(RecordPublishCommitRequest {
            refresh_id: refresh.refresh_id,
            published_snapshot_id: 2,
        })
        .expect("publish refresh");
    repository
        .finalize_refresh(MvRefreshFinalizeRequest {
            refresh_id: refresh.refresh_id,
            rows: 1,
            base_snapshots: BTreeMap::new(),
            base_table_uuids: BTreeMap::new(),
            target_snapshot_id: Some(2),
        })
        .expect("finalize refresh");
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: ["p1".to_string()].into(),
            last_refresh_ms: 1,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: Some(2),
            last_refresh_id: refresh.refresh_id,
            max_entries: 10,
        })
        .expect("persist partition");
    assert!(repository.drop_by_id(definition.mv_id).expect("drop MV"));
    drop(repository);
    let reopened = runtime
        .block_on(StateStoreMvRepository::open(
            host.state_store().expect("StateStore"),
            runtime.handle().clone(),
        ))
        .expect("reopen after cleanup");
    assert!(
        reopened
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .is_none()
    );
    assert!(
        reopened
            .list_partition_states(definition.mv_id)
            .expect("list removed partition states")
            .is_empty()
    );
}
