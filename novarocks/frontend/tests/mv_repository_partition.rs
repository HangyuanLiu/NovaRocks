use std::collections::{BTreeMap, BTreeSet};

use novarocks::mv::persistence::partition::{
    MvPartitionRefreshStatus, RecordFailedMvPartitionStatesRequest, ReplaceMvPartitionStatesRequest,
};
use novarocks::mv::repository::MvRepository;

#[path = "mv_repository_definition.rs"]
mod definition_support;

#[test]
fn partition_replacement_is_ordered_and_marks_the_definition_complete() {
    let (_temp, _runtime, _host, repository) = definition_support::repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_partition"),
        )
        .expect("create definition");
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["p2".to_string(), "p1".to_string()]),
            last_refresh_ms: 10,
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 42)]),
            target_snapshot_id: Some(44),
            last_refresh_id: 1,
            max_entries: 2,
        })
        .expect("replace partition states");
    let states = repository
        .list_partition_states(definition.mv_id)
        .expect("list partition states");
    assert_eq!(states.len(), 2);
    assert_eq!(states[0].partition_key, "p1");
    assert_eq!(states[0].status, MvPartitionRefreshStatus::Fresh);
    assert!(
        repository
            .load_by_id(definition.mv_id)
            .expect("load definition")
            .expect("definition exists")
            .partition_state_complete
    );
}

#[test]
fn partition_limits_clear_existing_state_without_staging_partial_records() {
    let (_temp, _runtime, _host, repository) = definition_support::repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_partition_limit"),
        )
        .expect("create definition");
    repository
        .record_failed_partition_states(RecordFailedMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["p1".to_string()]),
            failure_message: "injected failure".to_string(),
            last_refresh_ms: 11,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: None,
            last_refresh_id: 1,
            max_entries: 1,
        })
        .expect("record failed partition");
    assert_eq!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list failed partition")[0]
            .status,
        MvPartitionRefreshStatus::Failed
    );
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["p2".to_string(), "p3".to_string()]),
            last_refresh_ms: 12,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: None,
            last_refresh_id: 2,
            max_entries: 1,
        })
        .expect("oversized replacement marks incomplete");
    assert!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list cleared states")
            .is_empty()
    );
    assert!(
        !repository
            .load_by_id(definition.mv_id)
            .expect("load definition")
            .expect("definition exists")
            .partition_state_complete
    );
}
