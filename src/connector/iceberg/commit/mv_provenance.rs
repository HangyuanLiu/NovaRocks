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

//! W3a spike: can iceberg-rust 0.9.0 commit a *data-free* snapshot?
//!
//! W3a's "result unchanged but watermark advances" case needs a
//! metadata-only refresh: a brand-new snapshot that adds / deletes ZERO
//! data files, yet (a) advances `metadata.current_snapshot()` to a new
//! snapshot id and (b) carries a custom `provenance.v1` payload in
//! `summary.additional_properties`.
//!
//! This module hosts a spike test (`#[cfg(test)]`) only — no production
//! code. The verdict lives in the test assertions and in the SPIKE report.
//!
//! Findings summary (see the tests for the exact API surface):
//!
//! * Candidate A — empty `fast_append` with snapshot properties: WORKS.
//!   iceberg-rust 0.9.0 explicitly permits a snapshot with no added data
//!   files as long as `snapshot_properties` is non-empty (the workaround
//!   for apache/iceberg-rust#1548 in `transaction::snapshot`). The
//!   `FastAppendOperation` carries forward the parent snapshot's manifests,
//!   so existing data is preserved. NOTE: the repo's own `FastAppendCommit`
//!   wrapper short-circuits empty input to a no-op, so a data-free commit
//!   must drive the crate's `Transaction::fast_append()` action directly.
//!
//! * Candidate B — low-level `TableUpdate::AddSnapshot` + `SetSnapshotRef`
//!   via `Catalog::update_table`: ALSO WORKS, with one V3 caveat. This is
//!   the same primitive `mv_refresh_ref::publish_staging_branch_to_main`
//!   already uses. We hand-build a `Snapshot` that reuses the parent's
//!   manifest-list path (zero new files) and a hand-written `Summary`, then
//!   commit it. CAVEAT: on a format-version >= 3 table the catalog rejects
//!   the snapshot with `DataInvalid => first-row-id is null` unless a
//!   row-lineage range is set. A data-free snapshot assigns zero rows, so
//!   `with_row_range(effective_next_row_id, 0)` satisfies the requirement.

#[cfg(test)]
mod spike_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::spec::{Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary};
    use iceberg::table::Table;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, TableCommit, TableRequirement, TableUpdate};
    use uuid::Uuid;

    use crate::connector::iceberg::commit::test_helpers::{
        IcebergTestFixture, v3_table_with_n_data_files,
    };

    const SPIKE_PROP: &str = "novarocks.spike";

    /// Load the current snapshot id, panicking with a clear message if the
    /// table has no current snapshot (the fixture always seeds S0).
    async fn current_snapshot_id(catalog: &dyn Catalog, fixture: &IcebergTestFixture) -> i64 {
        let table = catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table");
        table
            .metadata()
            .current_snapshot()
            .expect("table must have a current snapshot")
            .snapshot_id()
    }

    /// Count live data files reachable from the current snapshot's manifest
    /// list. Used to prove a data-free commit preserves existing data.
    async fn live_data_file_count(table: &Table) -> usize {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return 0;
        };
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .expect("load manifest list");
        let mut count = 0;
        for entry in manifest_list.entries() {
            let manifest = entry
                .load_manifest(table.file_io())
                .await
                .expect("load manifest");
            for e in manifest.entries() {
                if e.is_alive() {
                    count += 1;
                }
            }
        }
        count
    }

    /// Candidate A — empty `fast_append` carrying only snapshot properties.
    ///
    /// Drives iceberg-rust's built-in `Transaction::fast_append()` action
    /// with NO `add_data_files` but a non-empty `set_snapshot_properties`,
    /// then commits through the catalog. Asserts the current snapshot
    /// advanced, the custom summary property survived a catalog reload, and
    /// the pre-existing data file is still live.
    #[tokio::test]
    async fn candidate_a_empty_fast_append_advances_current_and_carries_summary() {
        let fixture = v3_table_with_n_data_files(1).await;
        let catalog = fixture.catalog.clone();

        let s0 = fixture
            .table
            .metadata()
            .current_snapshot()
            .expect("fixture seeds S0")
            .snapshot_id();
        let data_before = live_data_file_count(&fixture.table).await;
        assert_eq!(data_before, 1, "fixture should seed exactly 1 data file");

        // Reload a fresh handle so the transaction sees the seeded snapshot.
        let table = catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table before empty append");

        let mut props = HashMap::new();
        props.insert(SPIKE_PROP.to_string(), "1".to_string());

        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .set_snapshot_properties(props)
            .set_commit_uuid(Uuid::new_v4());
        let tx = action.apply(tx).expect("empty fast_append apply");
        let _committed = tx
            .commit(catalog.as_ref())
            .await
            .expect("empty fast_append commit must succeed");

        // Reload through the catalog to prove the change is durable, not just
        // reflected in the returned in-memory handle.
        let reloaded = catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload after empty append");
        let s1 = reloaded
            .metadata()
            .current_snapshot()
            .expect("current snapshot after empty append")
            .snapshot_id();

        assert_ne!(s1, s0, "current snapshot must advance to a new id");
        assert_eq!(
            reloaded
                .metadata()
                .snapshot_by_id(s1)
                .expect("S1 present")
                .summary()
                .additional_properties
                .get(SPIKE_PROP)
                .map(String::as_str),
            Some("1"),
            "custom summary property must survive a catalog reload"
        );
        assert_eq!(
            reloaded
                .metadata()
                .snapshot_by_id(s1)
                .unwrap()
                .summary()
                .operation,
            Operation::Append,
            "empty fast_append records an Append operation"
        );

        // Data must be untouched: the pre-existing file remains live.
        let data_after = live_data_file_count(&reloaded).await;
        assert_eq!(
            data_after, data_before,
            "data-free append must preserve existing data files"
        );
    }

    /// Candidate B — low-level `AddSnapshot` + `SetSnapshotRef` via
    /// `Catalog::update_table`, reusing the parent snapshot's manifest list
    /// (zero new files). This mirrors
    /// `mv_refresh_ref::publish_staging_branch_to_main` but hand-builds a
    /// brand-new snapshot instead of re-pointing at an existing one.
    #[tokio::test]
    async fn candidate_b_zero_delta_add_snapshot_advances_current_and_carries_summary() {
        let fixture = v3_table_with_n_data_files(1).await;
        let catalog = fixture.catalog.clone();

        let s0 = current_snapshot_id(catalog.as_ref(), &fixture).await;
        let base = catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload base table");
        let base_meta = base.metadata();
        let parent = base_meta
            .snapshot_by_id(s0)
            .expect("parent snapshot present");

        let data_before = live_data_file_count(&base).await;
        assert_eq!(data_before, 1, "fixture should seed exactly 1 data file");

        // Reuse the parent's manifest-list path verbatim: the new snapshot
        // references the exact same set of data manifests, so it adds and
        // deletes ZERO files while still being a distinct snapshot.
        let manifest_list_path = parent.manifest_list().to_string();
        let new_snapshot_id = super::super::helpers::generate_snapshot_id();
        let new_seq = base_meta.last_sequence_number() + 1;

        let mut additional_properties: HashMap<String, String> = HashMap::new();
        additional_properties.insert(SPIKE_PROP.to_string(), "1".to_string());
        // Carry forward the parent's total-* so the summary stays consistent.
        for (k, v) in parent.summary().additional_properties.iter() {
            if k.starts_with("total-") {
                additional_properties.insert(k.clone(), v.clone());
            }
        }
        let summary = Summary {
            operation: Operation::Append,
            additional_properties,
        };

        // V3 tables require every snapshot to carry a row-lineage range;
        // the catalog rejects `first-row-id == null` for format-version >= 3.
        // A data-free snapshot assigns zero new rows, so the range width is 0
        // and `first_row_id` is simply the current next-row-id floor.
        let first_row_id =
            super::super::helpers::effective_next_row_id(base_meta).expect("effective next row id");

        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(Some(s0))
            .with_sequence_number(new_seq)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(base_meta.current_schema_id())
            .with_row_range(first_row_id, 0)
            .build();

        let commit = TableCommit::builder()
            .ident(fixture.table_ident.clone())
            .updates(vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".to_string(),
                    reference: SnapshotReference {
                        snapshot_id: new_snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ])
            .requirements(vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: Some(s0),
            }])
            .build();

        catalog
            .update_table(commit)
            .await
            .expect("zero-delta AddSnapshot commit must succeed");

        let reloaded = catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload after zero-delta commit");
        let s1 = reloaded
            .metadata()
            .current_snapshot()
            .expect("current snapshot after zero-delta commit")
            .snapshot_id();

        assert_ne!(s1, s0, "current snapshot must advance to a new id");
        assert_eq!(s1, new_snapshot_id, "current snapshot is our new snapshot");
        assert_eq!(
            reloaded
                .metadata()
                .snapshot_by_id(s1)
                .expect("S1 present")
                .summary()
                .additional_properties
                .get(SPIKE_PROP)
                .map(String::as_str),
            Some("1"),
            "custom summary property must survive a catalog reload"
        );

        let data_after = live_data_file_count(&reloaded).await;
        assert_eq!(
            data_after, data_before,
            "zero-delta commit must preserve existing data files"
        );
    }
}
