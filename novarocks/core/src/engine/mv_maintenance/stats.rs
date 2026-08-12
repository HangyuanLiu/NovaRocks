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

//! Collects per-table maintenance facts through the neutral connector surface:
//! snapshot list, current-snapshot summary counters, typed maintenance policy
//! facts, references, the provider-signed compaction count, and the
//! downstream-consumer floor that protects incremental MV lineage.
//!
//! Nothing here interprets a storage format. The provider signs every fact; the
//! frontend owns every policy decision made from them.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableResolution,
};

use crate::engine::StandaloneState;
use crate::mv::persistence::definition::StoredMvDefinition;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotInfo {
    pub(crate) snapshot_id: i64,
    pub(crate) timestamp_ms: i64,
}

/// Provider facts only. Frontend owns every policy decision and retry state.
#[derive(Clone, Debug, Default)]
pub(crate) struct TableMaintenanceStats {
    pub(crate) current_snapshot_id: Option<i64>,
    pub(crate) snapshots: Vec<SnapshotInfo>,
    pub(crate) total_data_files: Option<u64>,
    pub(crate) max_compactable_data_files: Option<u64>,
    pub(crate) total_files_size_bytes: Option<u64>,
    pub(crate) total_delete_files: Option<u64>,
    /// Typed maintenance policy facts declared by the table. `None` means the
    /// table declares no usable value for that key; defaults and clamping are
    /// policy and belong to the frontend, never to this fact layer.
    pub(crate) maintenance_enabled: Option<bool>,
    pub(crate) expire_max_snapshot_age_ms: Option<i64>,
    pub(crate) expire_min_snapshots_to_keep: Option<u32>,
    pub(crate) target_file_size_bytes: Option<i64>,
    pub(crate) non_default_reference_count: usize,
    pub(crate) downstream_floor_ts_ms: Option<i64>,
    pub(crate) downstream_floor_unknown: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownstreamFloor {
    pub(crate) floor_ts_ms: Option<i64>,
    pub(crate) unknown: bool,
}

/// Minimum consumed-snapshot timestamp across all MV definitions that read
/// `table_fqn` incrementally (committed positions and in-flight pins). A
/// consumer pointing at a snapshot we cannot resolve marks the floor unknown,
/// which blocks expire for safety.
pub(crate) fn downstream_floor(
    definitions: &[StoredMvDefinition],
    table_fqn: &str,
    snapshot_ts_by_id: &BTreeMap<i64, i64>,
) -> DownstreamFloor {
    let mut floor_ts: Option<i64> = None;
    let mut unknown = false;
    let mut consider = |snapshot_id: i64| match snapshot_ts_by_id.get(&snapshot_id) {
        Some(ts) => floor_ts = Some(floor_ts.map_or(*ts, |f| f.min(*ts))),
        None => unknown = true,
    };
    for definition in definitions {
        if let Some(id) = definition.last_refresh_snapshots.get(table_fqn) {
            consider(*id);
        }
        if let Some(id) = definition.refresh_target_snapshots.get(table_fqn) {
            consider(*id);
        }
    }
    DownstreamFloor {
        floor_ts_ms: floor_ts,
        unknown,
    }
}

/// Read one MV storage table's maintenance facts through the neutral surface.
/// `definitions` is the full MV list from the same pass, used for the floor.
///
/// The compaction observation runs first on purpose. Answering it forces the
/// provider to discard its cached table and re-read the catalog, and the
/// provider repopulates that cache with what it read, so the metadata load
/// below observes the very table version the count was taken from. Reversing
/// the two would let the projected facts describe an older table than the
/// count, and would drop the forced refresh this pass has always performed.
pub(crate) fn collect_table_stats(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
    definitions: &[StoredMvDefinition],
) -> Result<TableMaintenanceStats, String> {
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let identity = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from(namespace),
        table: Arc::from(table),
    };

    let max_compactable_data_files =
        crate::connector::metadata_maintenance::read_max_compactable_data_files(
            state.connector_control.as_ref(),
            &instance_id,
            identity,
            context.clone(),
        )
        .map_err(|error| {
            format!(
                "observe {catalog}.{namespace}.{table} compaction groups for maintenance: {error}"
            )
        })?;

    let exact_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        catalog,
    )?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        context.clone(),
        namespace,
        table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let observed = state
        .mv_storage_observation
        .observe_maintenance_metadata(&exact_lease, &metadata, context)
        .map_err(|error| {
            format!("observe {catalog}.{namespace}.{table} maintenance metadata: {error}")
        })?;

    let snapshots: Vec<SnapshotInfo> = observed
        .snapshots()
        .iter()
        .map(|snapshot| SnapshotInfo {
            snapshot_id: snapshot.snapshot_id,
            timestamp_ms: snapshot.timestamp_ms,
        })
        .collect();
    let snapshot_ts_by_id: BTreeMap<i64, i64> = snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id, snapshot.timestamp_ms))
        .collect();

    let fqn = format!("{catalog}.{namespace}.{table}");
    let floor = downstream_floor(definitions, &fqn, &snapshot_ts_by_id);
    let policy = *observed.policy();

    Ok(TableMaintenanceStats {
        current_snapshot_id: observed.current_snapshot_id(),
        snapshots,
        total_data_files: observed.total_data_files(),
        max_compactable_data_files,
        total_files_size_bytes: observed.total_files_size_bytes(),
        total_delete_files: observed.total_delete_files(),
        maintenance_enabled: policy.maintenance_enabled,
        expire_max_snapshot_age_ms: policy.expire_max_snapshot_age_ms,
        expire_min_snapshots_to_keep: policy.expire_min_snapshots_to_keep,
        target_file_size_bytes: policy.target_file_size_bytes,
        non_default_reference_count: observed.non_default_reference_count(),
        downstream_floor_ts_ms: floor.floor_ts_ms,
        downstream_floor_unknown: floor.unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
    use std::collections::BTreeMap;

    fn definition_with_consumed(fqn: &str, snapshot_id: i64) -> StoredMvDefinition {
        let mut last_refresh_snapshots = BTreeMap::new();
        last_refresh_snapshots.insert(fqn.to_string(), snapshot_id);
        StoredMvDefinition {
            mv_id: 1,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec![fqn.to_string()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("analytics".to_string()),
            target_table: Some("mv_x".to_string()),
            schema_contract: None,
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots,
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn floor_is_min_consumed_snapshot_timestamp() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        ts_by_id.insert(20, 2_000);
        let defs = vec![
            definition_with_consumed("ice.sales.t", 20),
            definition_with_consumed("ice.sales.t", 10),
        ];
        let floor = downstream_floor(&defs, "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_is_none_without_consumers() {
        let defs = vec![definition_with_consumed("ice.sales.other", 10)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: None,
                unknown: false
            }
        );
    }

    #[test]
    fn floor_unknown_when_consumed_snapshot_missing_from_metadata() {
        let defs = vec![definition_with_consumed("ice.sales.t", 99)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert!(floor.unknown);
    }

    #[test]
    fn floor_considers_in_progress_refresh_pins() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        let mut def = definition_with_consumed("ice.sales.other", 1);
        def.refresh_target_snapshots
            .insert("ice.sales.t".to_string(), 10);
        let floor = downstream_floor(&[def], "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_takes_min_across_both_maps_for_same_table() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        ts_by_id.insert(20, 2_000);
        // One consumer: committed at snapshot 20, with an in-flight refresh
        // pinned at the older snapshot 10. The floor must reflect the older pin.
        let mut def = definition_with_consumed("ice.sales.t", 20);
        def.refresh_target_snapshots
            .insert("ice.sales.t".to_string(), 10);
        let floor = downstream_floor(&[def], "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    // ---------------------------------------------------------------------
    // End-to-end evidence over a real Iceberg table.
    //
    // `collect_table_stats` reads nothing itself: it asks the provider for the
    // compaction count, loads the table on a planning lease, and projects the
    // observation. The cases below commit real snapshots through a Hadoop
    // warehouse and assert the returned facts against the table those commits
    // actually produced, so a projection that silently drops, defaults, or
    // clamps a fact cannot pass.
    // ---------------------------------------------------------------------

    const TEST_CATALOG: &str = "ice";
    const TEST_NAMESPACE: &str = "sales";

    struct MaintenanceStatsFixture {
        state: Arc<StandaloneState>,
        _warehouse: tempfile::TempDir,
    }

    fn open_hadoop_iceberg_fixture() -> MaintenanceStatsFixture {
        let warehouse = tempfile::TempDir::new().expect("warehouse tempdir");
        let state = Arc::new(StandaloneState {
            // Every projected fact travels this port, and the Core default is
            // fail-closed on purpose. Install the same Iceberg observation the
            // Server composition root installs, otherwise the call fails on the
            // missing port instead of on its own behaviour.
            mv_storage_observation: Arc::new(
                crate::engine::mv::schema_validation_adapter::TestIcebergMvStorageObservationAdapter::default(),
            ),
            ..StandaloneState::default()
        });
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(
                    TEST_CATALOG,
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                        (
                            "iceberg.catalog.warehouse".to_string(),
                            warehouse.path().display().to_string(),
                        ),
                    ],
                )
                .expect("create Hadoop catalog");
        }
        let entry = catalog_entry(&state);
        crate::connector::iceberg::catalog::registry::create_namespace(&entry, TEST_NAMESPACE)
            .expect("create namespace");
        crate::engine::register_iceberg_control_binding(&state, TEST_CATALOG)
            .expect("register Iceberg control binding");
        MaintenanceStatsFixture {
            state,
            _warehouse: warehouse,
        }
    }

    fn catalog_entry(
        state: &Arc<StandaloneState>,
    ) -> crate::connector::iceberg::catalog::registry::IcebergCatalogEntry {
        state
            .iceberg_catalogs
            .read()
            .expect("iceberg catalogs")
            .get(TEST_CATALOG)
            .expect("catalog entry")
    }

    fn create_fact_table(state: &Arc<StandaloneState>, table: &str, properties: &[(&str, &str)]) {
        let entry = catalog_entry(state);
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "region".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        let properties = properties
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            TEST_NAMESPACE,
            table,
            &columns,
            None,
            &[],
            &properties,
        )
        .expect("create Iceberg fact table");
    }

    /// Commit one row as its own snapshot and return the snapshot it produced.
    fn commit_one_row(state: &Arc<StandaloneState>, table: &str, id: i64, region: &str) -> i64 {
        let entry = catalog_entry(state);
        let rows = vec![vec![
            crate::sql::Literal::Int(id),
            crate::sql::Literal::String(region.to_string()),
        ]];
        crate::connector::iceberg::catalog::registry::insert_rows(
            &entry,
            TEST_NAMESPACE,
            table,
            &rows,
        )
        .expect("commit Iceberg rows");
        // Iceberg stamps a snapshot with wall-clock milliseconds. Separate the
        // commits so the timestamps this test reasons about are distinguishable
        // rather than accidentally equal.
        std::thread::sleep(std::time::Duration::from_millis(5));
        current_table(state, table)
            .metadata()
            .current_snapshot()
            .expect("committed snapshot")
            .snapshot_id()
    }

    /// Read the table straight from the catalog, bypassing any cached
    /// generation, so assertions compare against what storage really holds.
    fn current_table(
        state: &Arc<StandaloneState>,
        table: &str,
    ) -> novarocks_connector_iceberg::iceberg::table::Table {
        let entry = catalog_entry(state);
        entry.invalidate_table_cache(TEST_NAMESPACE, table);
        crate::connector::iceberg::catalog::registry::load_table(&entry, TEST_NAMESPACE, table)
            .expect("load Iceberg table")
            .into_table()
    }

    fn summary_u64(state: &Arc<StandaloneState>, table: &str, key: &str) -> Option<u64> {
        current_table(state, table)
            .metadata()
            .current_snapshot()
            .and_then(|snapshot| {
                snapshot
                    .summary()
                    .additional_properties
                    .get(key)
                    .and_then(|value| value.trim().parse::<u64>().ok())
            })
    }

    #[test]
    fn collect_table_stats_reports_the_committed_snapshot_facts_of_a_real_table() {
        let fixture = open_hadoop_iceberg_fixture();
        let state = &fixture.state;
        create_fact_table(state, "orders", &[]);
        let first = commit_one_row(state, "orders", 1, "east");
        let second = commit_one_row(state, "orders", 2, "east");
        let third = commit_one_row(state, "orders", 3, "east");

        let stats = collect_table_stats(state, TEST_CATALOG, TEST_NAMESPACE, "orders", &[])
            .expect("collect maintenance stats");

        // 1. The current snapshot is the last commit, and it is what the table
        //    itself reports.
        let table = current_table(state, "orders");
        let metadata = table.metadata();
        assert_eq!(stats.current_snapshot_id, Some(third));
        assert_eq!(stats.current_snapshot_id, metadata.current_snapshot_id());

        // 2. One snapshot per commit, each carrying the table's own timestamp.
        let observed: Vec<(i64, i64)> = stats
            .snapshots
            .iter()
            .map(|snapshot| (snapshot.snapshot_id, snapshot.timestamp_ms))
            .collect();
        let mut expected: Vec<(i64, i64)> = metadata
            .snapshots()
            .map(|snapshot| (snapshot.snapshot_id(), snapshot.timestamp_ms()))
            .collect();
        expected.sort_by_key(|(snapshot_id, _)| *snapshot_id);
        assert_eq!(expected.len(), 3);
        assert_eq!(observed, expected);
        let observed_ids: std::collections::BTreeSet<i64> =
            observed.iter().map(|(id, _)| *id).collect();
        let committed_ids: std::collections::BTreeSet<i64> =
            [first, second, third].into_iter().collect();
        assert_eq!(observed_ids, committed_ids);
        let ts_by_id: BTreeMap<i64, i64> = observed.iter().copied().collect();
        assert!(
            ts_by_id[&first] < ts_by_id[&second] && ts_by_id[&second] < ts_by_id[&third],
            "commit order must be visible in the observed timestamps: {ts_by_id:?}"
        );

        // 3. Summary counters come from the current snapshot summary, unread
        //    and uninterpreted.
        assert_eq!(
            stats.total_data_files,
            summary_u64(state, "orders", "total-data-files")
        );
        assert_eq!(
            stats.total_delete_files,
            summary_u64(state, "orders", "total-delete-files")
        );
        assert_eq!(stats.total_delete_files, Some(0));
        let files_size = summary_u64(state, "orders", "total-files-size")
            .expect("current snapshot summary carries total-files-size");
        assert!(files_size > 0, "a committed data file occupies bytes");
        assert_eq!(stats.total_files_size_bytes, Some(files_size));
        // The `total-*` counters are the current snapshot's own summary,
        // verbatim. This append path stamps each commit with that commit's
        // counts instead of rolling the table totals forward, so the summary
        // says one data file while three are live. That divergence is the
        // point: the summary counter and the enumerated compaction count are
        // different facts, and this layer must forward each unchanged rather
        // than reconcile them. If the append path ever starts accumulating,
        // the equality above still holds and only this pin needs revisiting.
        assert_eq!(stats.total_data_files, Some(1));

        // 4. The provider-signed compaction count is a live enumeration, not a
        //    summary read: three data files sharing one compaction group
        //    (unpartitioned, no row lineage) answer three.
        assert_eq!(stats.max_compactable_data_files, Some(3));

        // 5. A table that declares no maintenance property gets no value; the
        //    fact layer must not invent a default.
        assert_eq!(stats.maintenance_enabled, None);
        assert_eq!(stats.expire_max_snapshot_age_ms, None);
        assert_eq!(stats.expire_min_snapshots_to_keep, None);
        assert_eq!(stats.target_file_size_bytes, None);

        // 6. Only `main` exists.
        assert_eq!(stats.non_default_reference_count, 0);

        // 7. No MV consumes this table, so nothing pins retention.
        assert_eq!(stats.downstream_floor_ts_ms, None);
        assert!(!stats.downstream_floor_unknown);
    }

    #[test]
    fn collect_table_stats_counts_references_other_than_the_default_branch() {
        let fixture = open_hadoop_iceberg_fixture();
        let state = &fixture.state;
        create_fact_table(state, "branched", &[]);
        let snapshot = commit_one_row(state, "branched", 1, "east");

        let before = collect_table_stats(state, TEST_CATALOG, TEST_NAMESPACE, "branched", &[])
            .expect("collect maintenance stats");
        assert_eq!(before.non_default_reference_count, 0, "only `main` exists");

        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        session
            .execute_in_context(
                &format!(
                    "ALTER TABLE {TEST_CATALOG}.{TEST_NAMESPACE}.branched \
                     CREATE BRANCH dev AS OF VERSION {snapshot}"
                ),
                None,
                TEST_NAMESPACE,
                None,
            )
            .expect("create Iceberg branch");

        let after = collect_table_stats(state, TEST_CATALOG, TEST_NAMESPACE, "branched", &[])
            .expect("collect maintenance stats");
        // `dev` is counted, `main` is not. The branch points at an existing
        // snapshot, so the snapshot list itself is unchanged.
        assert_eq!(after.non_default_reference_count, 1);
        assert_eq!(after.snapshots, before.snapshots);
        assert_eq!(after.current_snapshot_id, Some(snapshot));
    }

    #[test]
    fn collect_table_stats_projects_declared_policy_without_default_or_clamp() {
        let fixture = open_hadoop_iceberg_fixture();
        let state = &fixture.state;
        create_fact_table(
            state,
            "policy_orders",
            &[
                ("novarocks.maintenance.enabled", "false"),
                ("history.expire.max-snapshot-age-ms", "3600000"),
                ("write.target-file-size-bytes", "1048576"),
            ],
        );
        commit_one_row(state, "policy_orders", 1, "east");

        let stats = collect_table_stats(state, TEST_CATALOG, TEST_NAMESPACE, "policy_orders", &[])
            .expect("collect maintenance stats");

        assert_eq!(stats.maintenance_enabled, Some(false));
        assert_eq!(stats.expire_max_snapshot_age_ms, Some(3_600_000));
        assert_eq!(stats.target_file_size_bytes, Some(1_048_576));
        // Declared by nobody: still absent. A default injected here would be a
        // policy decision made in the fact layer.
        assert_eq!(stats.expire_min_snapshots_to_keep, None);
    }

    #[test]
    fn collect_table_stats_floors_retention_at_the_snapshot_an_mv_consumes() {
        let fixture = open_hadoop_iceberg_fixture();
        let state = &fixture.state;
        create_fact_table(state, "consumed", &[]);
        let first = commit_one_row(state, "consumed", 1, "east");
        commit_one_row(state, "consumed", 2, "east");
        let third = commit_one_row(state, "consumed", 3, "east");
        let fqn = format!("{TEST_CATALOG}.{TEST_NAMESPACE}.consumed");

        // A consumer committed at the newest snapshot floors retention there,
        // not at the oldest snapshot the table still retains.
        let consumer = definition_with_consumed(&fqn, third);
        let stats = collect_table_stats(
            state,
            TEST_CATALOG,
            TEST_NAMESPACE,
            "consumed",
            std::slice::from_ref(&consumer),
        )
        .expect("collect maintenance stats");
        let ts_by_id: BTreeMap<i64, i64> = stats
            .snapshots
            .iter()
            .map(|snapshot| (snapshot.snapshot_id, snapshot.timestamp_ms))
            .collect();
        assert_eq!(stats.downstream_floor_ts_ms, Some(ts_by_id[&third]));
        assert!(!stats.downstream_floor_unknown);
        assert!(
            ts_by_id[&first] < ts_by_id[&third],
            "the floor must be discriminating: {ts_by_id:?}"
        );

        // A consumer pinned to a snapshot this table cannot resolve leaves the
        // floor unknown, which is what blocks expire.
        let missing = definition_with_consumed(&fqn, third.wrapping_add(1));
        assert!(!ts_by_id.contains_key(&third.wrapping_add(1)));
        let stats = collect_table_stats(
            state,
            TEST_CATALOG,
            TEST_NAMESPACE,
            "consumed",
            std::slice::from_ref(&missing),
        )
        .expect("collect maintenance stats");
        assert!(stats.downstream_floor_unknown);
    }
}
