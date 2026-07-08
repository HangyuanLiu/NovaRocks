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

use std::collections::BTreeMap;

use novarocks::meta::repository::mv::{
    BeginIcebergMvRefreshRequest, CreateMvDefinitionRequest, MvMetaRepository,
    MvRefreshFinalizeRequest, RecordPublishCommitRequest, RecordStagingCommitRequest,
};
use novarocks::meta::{MetaStoreProvider, SqliteMetaStoreProvider};

#[test]
fn refresh_transaction_can_recover_after_external_commit_before_finalize()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = MvMetaRepository::default();

    let mut txn = provider.begin_write("create mv and intent")?;
    let mv = repo.create_definition(
        &mut *txn,
        CreateMvDefinitionRequest {
            select_sql: "select id from ice.ns.orders".to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: vec!["id".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("ns".to_string()),
            target_table: Some("orders_mv".to_string()),
            schema_contract: None,
            partition_spec: None,
            created_at_ms: 1,
        },
    )?;
    let intent = repo.begin_iceberg_refresh_intent(
        &mut *txn,
        BeginIcebergMvRefreshRequest {
            mv_id: mv.mv_id,
            operation_id: None,
            target_catalog: "ice".to_string(),
            target_namespace: "ns".to_string(),
            target_table: "orders_mv".to_string(),
            staging_branch: "__nova_mv_refresh_1_42".to_string(),
            expected_main_snapshot_id: None,
            base_snapshots: BTreeMap::new(),
            marker_token: "marker-42".to_string(),
        },
    )?;
    txn.commit()?;

    let mut txn = provider.begin_write("record staging commit")?;
    repo.record_staging_commit(
        &mut *txn,
        RecordStagingCommitRequest {
            refresh_id: intent.refresh_id,
            staging_snapshot_id: 42,
            rows: 10,
            base_table_uuids: BTreeMap::new(),
        },
    )?;
    txn.commit()?;

    let mut txn = provider.begin_write("record publish commit")?;
    repo.record_publish_commit(
        &mut *txn,
        RecordPublishCommitRequest {
            refresh_id: intent.refresh_id,
            published_snapshot_id: 42,
        },
    )?;
    txn.commit()?;

    let mut txn = provider.begin_write("recover finalize")?;
    repo.finalize_refresh(
        &mut *txn,
        MvRefreshFinalizeRequest {
            refresh_id: intent.refresh_id,
            rows: 10,
            base_snapshots: BTreeMap::new(),
            base_table_uuids: BTreeMap::new(),
            target_snapshot_id: Some(42),
        },
    )?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let mv_after = repo.load_by_id(&*read, mv.mv_id)?.expect("mv");
    assert_eq!(mv_after.last_refreshed_iceberg_snapshot_id, Some(42));
    Ok(())
}
