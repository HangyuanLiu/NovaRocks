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

use novarocks::meta::keys::{NS_ICEBERG_OPERATION, NS_JOB, NS_STARROCKS_TXN};
use novarocks::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergCleanupOutcomeRecord, IcebergCommitOutcomeRecord,
    IcebergOperationFactUpdate, IcebergOperationFailureKind, IcebergOperationFailureRecord,
    IcebergOperationKind, IcebergOperationNextAction, IcebergOperationRepository,
    IcebergOperationState, IcebergOperationTarget, IcebergRecoveryEvidenceRecord,
    StoredIcebergOperation,
};
use novarocks::meta::repository::job::{
    CreateEraseJobRequest, CreateIcebergOptimizeJobRequest, IcebergOptimizeJobOutcome,
    IcebergOptimizeJobState, JobMetaRepository, JobState,
};
use novarocks::meta::repository::starrocks_table::{
    CreateStarRocksColumnRequest, CreateStarRocksDatabaseRequest,
    CreateStarRocksTableLayoutRequest, CreateStarRocksTableRequest, StageStarRocksMvRefreshRequest,
    StageStarRocksTruncateRequest, StarRocksIndexState, StarRocksPartitionState,
    StarRocksTableKind, StarRocksTableMetaRepository, StarRocksTableState,
};
use novarocks::meta::repository::starrocks_txn::{
    StarRocksTxnRepository, StarRocksTxnState, StoredStarRocksTxn,
};
use novarocks::meta::repository::{
    RepositoryError, RepositoryErrorKind, decode_payload_for_kind, encode_record_payload, id_scopes,
};
use novarocks::meta::{
    ExpectedRevision, MetaKey, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
    SqliteMetaStoreProvider,
};
use std::collections::BTreeMap;

fn create_starrocks_table_with_partition(
    provider: &SqliteMetaStoreProvider,
    repository: &StarRocksTableMetaRepository,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    create_named_starrocks_table_with_partition(provider, repository, "orders")
}

fn create_named_starrocks_table_with_partition(
    provider: &SqliteMetaStoreProvider,
    repository: &StarRocksTableMetaRepository,
    table_name: &str,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    let mut txn = provider.begin_write("create StarRocks table objects")?;
    let database = repository.create_database(
        txn.as_mut(),
        CreateStarRocksDatabaseRequest {
            name: format!("{table_name}_db"),
        },
    )?;
    let table = repository.create_table(
        txn.as_mut(),
        CreateStarRocksTableRequest {
            db_id: database.db_id,
            name: table_name.to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 10,
            state: StarRocksTableState::Active,
            kind: StarRocksTableKind::Table,
        },
    )?;
    let partition = repository.create_partition(txn.as_mut(), table.table_id, table_name, 1)?;
    txn.commit()?;
    Ok((table.table_id, partition.partition_id))
}

fn put_starrocks_txn_record(
    txn: &mut dyn novarocks::meta::MetaWriteTxn,
    starrocks_txn: StoredStarRocksTxn,
) -> Result<(), Box<dyn std::error::Error>> {
    txn.put(MetaRecordPut::new(
        MetaKey::new(NS_STARROCKS_TXN, [starrocks_txn.txn_id.to_string()])?,
        MetaRecordKind::new("starrocks.txn")?,
        ExpectedRevision::NotExists,
        encode_record_payload("starrocks.txn", &starrocks_txn)?,
    ))?;
    Ok(())
}

#[test]
fn repository_avro_payload_round_trips_iceberg_operation_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let value = StoredIcebergOperation {
        operation_id: 42,
        operation_kind: IcebergOperationKind::Maintenance,
        operation_subkind: Some("MV_REPARTITION".to_string()),
        target: IcebergOperationTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_sales".to_string(),
            ref_name: Some("main".to_string()),
        },
        state: IcebergOperationState::CommitUnknown,
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: Some(7),
        base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 3)]),
        staged_artifacts: vec!["s3://warehouse/mv/_staging/a.parquet".to_string()],
        commit_request: Some("commit-request-json".to_string()),
        commit_outcome: Some(IcebergCommitOutcomeRecord {
            snapshot_id: 1001,
            written_manifest_paths: vec![
                "s3://warehouse/mv/metadata/manifest-a.avro".to_string(),
                "s3://warehouse/mv/metadata/manifest-b.avro".to_string(),
            ],
        }),
        cleanup_outcome: Some(IcebergCleanupOutcomeRecord {
            attempted: true,
            error_count: 1,
            error_paths: vec!["s3://warehouse/mv/_staging/orphan.parquet".to_string()],
        }),
        recovery_evidence: Some(IcebergRecoveryEvidenceRecord {
            table_ident: "ice.analytics.mv_sales".to_string(),
            commit_op_kind: "fast_append".to_string(),
            base_snapshot_id: Some(7),
            base_sequence_number: Some(11),
            staging_dir: "s3://warehouse/mv/_staging/attempt-1".to_string(),
        }),
        failure: Some(IcebergOperationFailureRecord {
            kind: IcebergOperationFailureKind::Unknown,
            message: "commit status is unknown".to_string(),
            next_action: IcebergOperationNextAction::ManualInspect,
        }),
        created_at_ms: 1000,
        updated_at_ms: 1200,
        finished_at_ms: None,
    };

    let payload = encode_record_payload("iceberg.operation", &value)?;
    assert_eq!(payload.encoding, novarocks::meta::MetaPayloadEncoding::Avro);
    assert_eq!(payload.schema_id, 2);
    assert_eq!(payload.schema_fingerprint.len(), 16);

    let decoded: StoredIcebergOperation = decode_payload_for_kind("iceberg.operation", &payload)?;
    assert_eq!(decoded.operation_kind, IcebergOperationKind::Maintenance);
    assert_eq!(decoded.operation_subkind.as_deref(), Some("MV_REPARTITION"));
    assert_eq!(
        decoded
            .commit_outcome
            .as_ref()
            .expect("commit outcome should round-trip")
            .snapshot_id,
        1001
    );
    assert_eq!(
        decoded
            .cleanup_outcome
            .as_ref()
            .expect("cleanup outcome should round-trip")
            .error_paths[0],
        "s3://warehouse/mv/_staging/orphan.parquet"
    );
    assert_eq!(
        decoded
            .recovery_evidence
            .as_ref()
            .expect("recovery evidence should round-trip")
            .base_sequence_number,
        Some(11)
    );
    assert_eq!(
        decoded
            .failure
            .as_ref()
            .expect("failure should round-trip")
            .next_action,
        IcebergOperationNextAction::ManualInspect
    );
    assert_eq!(decoded, value);
    Ok(())
}

#[test]
fn repository_id_scopes_are_stable_strings() {
    assert_eq!(id_scopes::starrocks_db().as_str(), "starrocks.db");
    assert_eq!(id_scopes::starrocks_table().as_str(), "starrocks.table");
    assert_eq!(
        id_scopes::starrocks_partition().as_str(),
        "starrocks.partition"
    );
    assert_eq!(id_scopes::starrocks_index().as_str(), "starrocks.index");
    assert_eq!(id_scopes::starrocks_tablet().as_str(), "starrocks.tablet");
    assert_eq!(id_scopes::starrocks_txn().as_str(), "starrocks.txn");
    assert_eq!(id_scopes::mv_id().as_str(), "mv.id");
    assert_eq!(id_scopes::refresh_id().as_str(), "refresh.id");
    assert_eq!(id_scopes::erase_job().as_str(), "job.erase");
    assert_eq!(id_scopes::iceberg_operation().as_str(), "iceberg.operation");
}

#[test]
fn repository_namespaces_are_stable_strings() {
    assert_eq!(NS_STARROCKS_TXN, "starrocks.txn");
    assert_eq!(NS_JOB, "job");
    assert_eq!(NS_ICEBERG_OPERATION, "iceberg.operation");
}

#[test]
fn repository_error_display_is_domain_facing() {
    let err = RepositoryError::conflict("StarRocks txn state changed");
    assert_eq!(
        err.to_string(),
        "metadata repository conflict: StarRocks txn state changed"
    );
}

fn create_committing_iceberg_operation(
    provider: &SqliteMetaStoreProvider,
    repository: &IcebergOperationRepository,
) -> Result<i64, Box<dyn std::error::Error>> {
    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::InsertAppend,
                operation_subkind: None,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: Some(10),
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: vec!["s3://warehouse/orders/_staging/a.parquet".to_string()],
                created_at_ms: 1000,
            },
        )?;
        txn.commit()?;
        stored.operation_id
    };

    {
        let mut txn = provider.begin_write("transition iceberg operation to committing")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committing,
            1100,
        )?;
        txn.commit()?;
    }

    Ok(operation_id)
}

#[test]
fn iceberg_operation_repository_create_load_and_list_unfinished()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::MvRefresh,
                operation_subkind: None,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "analytics".to_string(),
                    table: "mv_sales".to_string(),
                    ref_name: Some("main".to_string()),
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: Some(42),
                base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 7)]),
                staged_artifacts: vec!["s3://warehouse/mv/_staging/a.parquet".to_string()],
                created_at_ms: 1000,
            },
        )?;
        assert_eq!(stored.state, IcebergOperationState::Preparing);
        assert_eq!(stored.created_at_ms, 1000);
        assert_eq!(stored.updated_at_ms, 1000);
        assert_eq!(stored.finished_at_ms, None);
        txn.commit()?;
        stored.operation_id
    };

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.operation_id, operation_id);
    assert_eq!(loaded.operation_kind, IcebergOperationKind::MvRefresh);
    assert_eq!(loaded.target.catalog, "ice");
    assert_eq!(loaded.target.namespace, "analytics");
    assert_eq!(loaded.target.table, "mv_sales");
    assert_eq!(loaded.target.ref_name.as_deref(), Some("main"));
    assert_eq!(loaded.base_snapshot_id, Some(42));
    assert_eq!(loaded.base_snapshot_map["ice.sales.orders"], 7);
    assert_eq!(loaded.staged_artifacts.len(), 1);

    let unfinished = repository.list_unfinished_operations(read.as_ref())?;
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].operation_id, operation_id);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_commit_request() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg repartition operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::Maintenance,
                operation_subkind: Some("MV_REPARTITION".to_string()),
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "analytics".to_string(),
                    table: "mv_orders".to_string(),
                    ref_name: Some("__nova_mv_repartition_1".to_string()),
                },
                attempt_id: "mv-repartition-1".to_string(),
                base_snapshot_id: Some(42),
                base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 7)]),
                staged_artifacts: vec!["branch:__nova_mv_repartition_1".to_string()],
                created_at_ms: 1000,
            },
        )?;
        txn.commit()?;
        stored.operation_id
    };

    let commit_request = r#"{"kind":"MV_REPARTITION"}"#.to_string();
    {
        let mut txn = provider.begin_write("record iceberg operation commit request")?;
        repository.record_commit_request(
            txn.as_mut(),
            operation_id,
            commit_request.clone(),
            1200,
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(
        loaded.commit_request.as_deref(),
        Some(commit_request.as_str())
    );
    assert_eq!(loaded.updated_at_ms, 1200);
    assert_eq!(loaded.state, IcebergOperationState::Preparing);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_commit_unknown_fact_without_finishing()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let recovery = IcebergRecoveryEvidenceRecord {
        table_ident: "ice.sales.orders".to_string(),
        commit_op_kind: "fast_append".to_string(),
        base_snapshot_id: Some(10),
        base_sequence_number: Some(33),
        staging_dir: "s3://warehouse/orders/_staging".to_string(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::Unknown,
        message: "commit status is unknown".to_string(),
        next_action: IcebergOperationNextAction::ManualInspect,
    };

    {
        let mut txn = provider.begin_write("record commit unknown iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: Some(recovery.clone()),
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::CommitUnknown);
    assert_eq!(loaded.commit_outcome, None);
    assert_eq!(loaded.cleanup_outcome, None);
    assert_eq!(loaded.recovery_evidence, Some(recovery));
    assert_eq!(loaded.failure, Some(failure));
    assert_eq!(loaded.updated_at_ms, 1200);
    assert_eq!(loaded.finished_at_ms, None);

    Ok(())
}

#[test]
fn iceberg_operation_repository_preserves_commit_unknown_evidence_when_recovered_to_committed()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let recovery = IcebergRecoveryEvidenceRecord {
        table_ident: "ice.sales.orders".to_string(),
        commit_op_kind: "fast_append".to_string(),
        base_snapshot_id: Some(10),
        base_sequence_number: Some(33),
        staging_dir: "s3://warehouse/orders/_staging".to_string(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::Unknown,
        message: "commit status is unknown".to_string(),
        next_action: IcebergOperationNextAction::ManualInspect,
    };

    {
        let mut txn = provider.begin_write("record commit unknown iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: Some(recovery.clone()),
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let commit_outcome = IcebergCommitOutcomeRecord {
        snapshot_id: 55,
        written_manifest_paths: vec!["s3://warehouse/orders/metadata/m0.avro".to_string()],
    };

    {
        let mut txn = provider.begin_write("recover commit unknown as committed")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: Some(commit_outcome.clone()),
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                now_ms: 1300,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::Committed);
    assert_eq!(loaded.commit_outcome, Some(commit_outcome));
    assert_eq!(loaded.cleanup_outcome, None);
    assert_eq!(loaded.recovery_evidence, Some(recovery));
    assert_eq!(loaded.failure, Some(failure));
    assert_eq!(loaded.updated_at_ms, 1300);
    assert_eq!(loaded.finished_at_ms, None);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_known_uncommitted_cleanup_and_finishes()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 2,
        error_paths: vec![
            "s3://warehouse/orders/_staging/a.parquet".to_string(),
            "s3://warehouse/orders/_staging/b.parquet".to_string(),
        ],
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::KnownUncommitted,
        message: "commit rejected before metadata update".to_string(),
        next_action: IcebergOperationNextAction::RetryAbort,
    };

    {
        let mut txn = provider.begin_write("record known uncommitted iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(cleanup.clone()),
                recovery_evidence: None,
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::FailedKnownUncommitted);
    assert_eq!(loaded.commit_outcome, None);
    assert_eq!(loaded.cleanup_outcome, Some(cleanup));
    assert_eq!(loaded.recovery_evidence, None);
    assert_eq!(loaded.failure, Some(failure));
    assert_eq!(loaded.updated_at_ms, 1200);
    assert_eq!(loaded.finished_at_ms, Some(1200));

    Ok(())
}

#[test]
fn iceberg_operation_repository_refines_cleanup_on_committed_and_commit_unknown_states()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let committed_operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let commit_outcome = IcebergCommitOutcomeRecord {
        snapshot_id: 55,
        written_manifest_paths: vec!["s3://warehouse/orders/metadata/m0.avro".to_string()],
    };
    {
        let mut txn = provider.begin_write("record committed iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: committed_operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: Some(commit_outcome.clone()),
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let committed_cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 0,
        error_paths: Vec::new(),
    };
    {
        let mut txn = provider.begin_write("record committed cleanup refinement")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: committed_operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: None,
                cleanup_outcome: Some(committed_cleanup.clone()),
                recovery_evidence: None,
                failure: None,
                now_ms: 1300,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("reject committed failure injection")?;
        let err = repository
            .record_operation_fact(
                txn.as_mut(),
                IcebergOperationFactUpdate {
                    operation_id: committed_operation_id,
                    state: IcebergOperationState::Committed,
                    commit_outcome: None,
                    cleanup_outcome: Some(committed_cleanup.clone()),
                    recovery_evidence: None,
                    failure: Some(IcebergOperationFailureRecord {
                        kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
                        message: "unexpected failure injection".to_string(),
                        next_action: IcebergOperationNextAction::RetryFinalize,
                    }),
                    now_ms: 1350,
                },
            )
            .expect_err("cleanup refinement must not inject a new failure");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let unknown_operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let recovery = IcebergRecoveryEvidenceRecord {
        table_ident: "ice.sales.orders".to_string(),
        commit_op_kind: "fast_append".to_string(),
        base_snapshot_id: Some(10),
        base_sequence_number: Some(33),
        staging_dir: "s3://warehouse/orders/_staging".to_string(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::Unknown,
        message: "commit status is unknown".to_string(),
        next_action: IcebergOperationNextAction::ManualInspect,
    };
    {
        let mut txn = provider.begin_write("record commit unknown iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: unknown_operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: Some(recovery.clone()),
                failure: Some(failure.clone()),
                now_ms: 1400,
            },
        )?;
        txn.commit()?;
    }

    let unknown_cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 1,
        error_paths: vec!["s3://warehouse/orders/_staging/orphan.parquet".to_string()],
    };
    {
        let mut txn = provider.begin_write("record commit unknown cleanup refinement")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: unknown_operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: Some(unknown_cleanup.clone()),
                recovery_evidence: None,
                failure: None,
                now_ms: 1500,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let committed = repository
        .load_operation(read.as_ref(), committed_operation_id)?
        .expect("committed operation should exist");
    assert_eq!(committed.state, IcebergOperationState::Committed);
    assert_eq!(committed.commit_outcome, Some(commit_outcome));
    assert_eq!(committed.cleanup_outcome, Some(committed_cleanup));
    assert_eq!(committed.updated_at_ms, 1300);

    let unknown = repository
        .load_operation(read.as_ref(), unknown_operation_id)?
        .expect("unknown operation should exist");
    assert_eq!(unknown.state, IcebergOperationState::CommitUnknown);
    assert_eq!(unknown.cleanup_outcome, Some(unknown_cleanup));
    assert_eq!(unknown.recovery_evidence, Some(recovery));
    assert_eq!(unknown.failure, Some(failure));
    assert_eq!(unknown.updated_at_ms, 1500);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_same_state_fact_replay_and_cleanup_refinement()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let cleanup = IcebergCleanupOutcomeRecord {
        attempted: false,
        error_count: 0,
        error_paths: Vec::new(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::KnownUncommitted,
        message: "commit failed before metadata update".to_string(),
        next_action: IcebergOperationNextAction::RetryAbort,
    };

    {
        let mut txn = provider.begin_write("record known uncommitted fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(cleanup.clone()),
                recovery_evidence: None,
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("replay known uncommitted fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(cleanup.clone()),
                recovery_evidence: None,
                failure: Some(failure.clone()),
                now_ms: 1300,
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let loaded = repository
            .load_operation(read.as_ref(), operation_id)?
            .expect("operation should exist");
        assert_eq!(loaded.updated_at_ms, 1200);
        assert_eq!(loaded.finished_at_ms, Some(1200));
    }

    let refined_cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 0,
        error_paths: Vec::new(),
    };
    let refined_failure = IcebergOperationFailureRecord {
        next_action: IcebergOperationNextAction::None,
        ..failure.clone()
    };

    {
        let mut txn = provider.begin_write("refine known uncommitted cleanup fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(refined_cleanup.clone()),
                recovery_evidence: None,
                failure: Some(refined_failure.clone()),
                now_ms: 1400,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("reject conflicting known uncommitted failure fact")?;
        let err = repository
            .record_operation_fact(
                txn.as_mut(),
                IcebergOperationFactUpdate {
                    operation_id,
                    state: IcebergOperationState::FailedKnownUncommitted,
                    commit_outcome: None,
                    cleanup_outcome: Some(refined_cleanup.clone()),
                    recovery_evidence: None,
                    failure: Some(IcebergOperationFailureRecord {
                        message: "different primary failure".to_string(),
                        ..refined_failure.clone()
                    }),
                    now_ms: 1500,
                },
            )
            .expect_err("same-state refinement must not replace the primary failure");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        assert!(
            err.to_string()
                .contains("conflicting Iceberg operation fact replay"),
            "{err}"
        );
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::FailedKnownUncommitted);
    assert_eq!(loaded.cleanup_outcome, Some(refined_cleanup));
    assert_eq!(loaded.failure, Some(refined_failure));
    assert_eq!(loaded.updated_at_ms, 1400);
    assert_eq!(loaded.finished_at_ms, Some(1200));

    Ok(())
}

#[test]
fn iceberg_operation_repository_preserves_commit_outcome_on_finalize_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let commit_outcome = IcebergCommitOutcomeRecord {
        snapshot_id: 55,
        written_manifest_paths: vec!["s3://warehouse/orders/metadata/m0.avro".to_string()],
    };

    {
        let mut txn = provider.begin_write("record committed iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: Some(commit_outcome.clone()),
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("transition iceberg operation to finalizing")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Finalizing,
            1300,
        )?;
        txn.commit()?;
    }

    let finalize_failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
        message: "mv metadata update failed".to_string(),
        next_action: IcebergOperationNextAction::RetryFinalize,
    };

    {
        let mut txn = provider.begin_write("record finalize failure iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FinalizeFailedKnownCommitted,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: Some(finalize_failure.clone()),
                now_ms: 1400,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(
        loaded.state,
        IcebergOperationState::FinalizeFailedKnownCommitted
    );
    assert_eq!(loaded.commit_outcome, Some(commit_outcome));
    assert_eq!(loaded.cleanup_outcome, None);
    assert_eq!(loaded.failure, Some(finalize_failure));
    assert_eq!(loaded.updated_at_ms, 1400);
    assert_eq!(loaded.finished_at_ms, None);

    Ok(())
}

#[test]
fn iceberg_operation_repository_finished_operations_are_not_unfinished()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::InsertAppend,
                operation_subkind: None,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: None,
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: Vec::new(),
                created_at_ms: 1000,
            },
        )?;
        txn.commit()?;
        stored.operation_id
    };

    {
        let mut txn = provider.begin_write("transition iceberg operation")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committing,
            1100,
        )?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committed,
            1200,
        )?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Finalized,
            1300,
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("replay finalized iceberg operation")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Finalized,
            1400,
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::Finalized);
    assert_eq!(loaded.updated_at_ms, 1300);
    assert_eq!(loaded.finished_at_ms, Some(1300));
    assert!(
        repository
            .list_unfinished_operations(read.as_ref())?
            .is_empty()
    );

    Ok(())
}

#[test]
fn job_repository_claim_finish_and_fail_are_state_checked() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let job_id = {
        let mut txn = provider.begin_write("create erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        assert_eq!(job.table_id, 10);
        assert_eq!(job.partition_id, Some(20));
        assert_eq!(job.root_path, "s3://bucket/db/table/partition");
        assert_eq!(job.state, JobState::Pending);
        assert_eq!(job.retry_at_ms, None);
        assert_eq!(job.updated_at_ms, 1000);
        assert_eq!(job.last_error, None);
        txn.commit()?;
        job.job_id
    };

    {
        let mut txn = provider.begin_write("claim and fail erase job")?;
        assert!(repository.claim_erase_job(txn.as_mut(), job_id, 1100)?);
        repository.fail_erase_job(
            txn.as_mut(),
            job_id,
            "object delete failed".to_string(),
            Some(1150),
            1120,
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("retry erase job")?;
        assert!(repository.claim_erase_job(txn.as_mut(), job_id, 1150)?);
        repository.finish_erase_job(txn.as_mut(), job_id, 1200)?;
        repository.finish_erase_job(txn.as_mut(), job_id, 1210)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), job_id)?
        .expect("erase job should exist");
    assert_eq!(job.state, JobState::Finished);
    assert_eq!(job.retry_at_ms, None);
    assert_eq!(job.updated_at_ms, 1200);
    assert_eq!(job.last_error, None);

    Ok(())
}

#[test]
fn job_repository_fail_requires_running_and_can_update_failed_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let pending_id = {
        let mut txn = provider.begin_write("create pending erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        let err = repository
            .fail_erase_job(
                txn.as_mut(),
                job.job_id,
                "not running".to_string(),
                Some(1300),
                1200,
            )
            .expect_err("pending erase job should not fail");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        txn.commit()?;
        job.job_id
    };

    {
        let read = provider.begin_read()?;
        let job = repository
            .load_erase_job(read.as_ref(), pending_id)?
            .expect("pending job should exist");
        assert_eq!(job.state, JobState::Pending);
        assert_eq!(job.updated_at_ms, 1000);
    }

    let failed_id = {
        let mut txn = provider.begin_write("fail erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 11,
                partition_id: None,
                root_path: "s3://bucket/db/table".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), job.job_id, 1100)?);
        repository.fail_erase_job(
            txn.as_mut(),
            job.job_id,
            "first failure".to_string(),
            Some(1300),
            1200,
        )?;
        repository.fail_erase_job(
            txn.as_mut(),
            job.job_id,
            "retry later".to_string(),
            Some(1400),
            1250,
        )?;
        txn.commit()?;
        job.job_id
    };

    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), failed_id)?
        .expect("failed job should exist");
    assert_eq!(job.state, JobState::Failed);
    assert_eq!(job.retry_at_ms, Some(1400));
    assert_eq!(job.updated_at_ms, 1250);
    assert_eq!(job.last_error.as_deref(), Some("retry later"));

    Ok(())
}

#[test]
fn job_repository_claim_failed_honors_retry_at() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let job_id = {
        let mut txn = provider.begin_write("create failed erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), job.job_id, 1100)?);
        repository.fail_erase_job(
            txn.as_mut(),
            job.job_id,
            "retry later".to_string(),
            Some(1500),
            1200,
        )?;
        txn.commit()?;
        job.job_id
    };

    {
        let mut txn = provider.begin_write("claim failed job before retry")?;
        assert!(!repository.claim_erase_job(txn.as_mut(), job_id, 1400)?);
        txn.commit()?;
    }
    {
        let read = provider.begin_read()?;
        let job = repository
            .load_erase_job(read.as_ref(), job_id)?
            .expect("failed job should exist");
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.retry_at_ms, Some(1500));
        assert_eq!(job.updated_at_ms, 1200);
        assert_eq!(job.last_error.as_deref(), Some("retry later"));
    }

    {
        let mut txn = provider.begin_write("claim failed job after retry")?;
        assert!(repository.claim_erase_job(txn.as_mut(), job_id, 1500)?);
        txn.commit()?;
    }
    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), job_id)?
        .expect("running job should exist");
    assert_eq!(job.state, JobState::Running);
    assert_eq!(job.retry_at_ms, None);
    assert_eq!(job.updated_at_ms, 1500);
    assert_eq!(job.last_error, None);

    Ok(())
}

#[test]
fn job_repository_lists_pending_and_due_failed_jobs() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let (
        pending_id,
        failed_none_retry_id,
        failed_due_id,
        failed_future_id,
        running_id,
        finished_id,
    ) = {
        let mut txn = provider.begin_write("create runnable erase jobs")?;
        let pending = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/pending".to_string(),
                now_ms: 1000,
            },
        )?;
        let failed_none_retry = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 11,
                partition_id: Some(21),
                root_path: "s3://bucket/db/table/failed-none".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), failed_none_retry.job_id, 1010)?);
        repository.fail_erase_job(
            txn.as_mut(),
            failed_none_retry.job_id,
            "retry immediately".to_string(),
            None,
            1020,
        )?;
        let failed_due = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 12,
                partition_id: Some(22),
                root_path: "s3://bucket/db/table/failed-due".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), failed_due.job_id, 1010)?);
        repository.fail_erase_job(
            txn.as_mut(),
            failed_due.job_id,
            "due".to_string(),
            Some(1100),
            1020,
        )?;
        let failed_future = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 13,
                partition_id: Some(23),
                root_path: "s3://bucket/db/table/failed-future".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), failed_future.job_id, 1010)?);
        repository.fail_erase_job(
            txn.as_mut(),
            failed_future.job_id,
            "future".to_string(),
            Some(1300),
            1020,
        )?;
        let running = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 14,
                partition_id: Some(24),
                root_path: "s3://bucket/db/table/running".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), running.job_id, 1010)?);
        let finished = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 15,
                partition_id: Some(25),
                root_path: "s3://bucket/db/table/finished".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), finished.job_id, 1010)?);
        repository.finish_erase_job(txn.as_mut(), finished.job_id, 1020)?;
        txn.commit()?;
        (
            pending.job_id,
            failed_none_retry.job_id,
            failed_due.job_id,
            failed_future.job_id,
            running.job_id,
            finished.job_id,
        )
    };

    let read = provider.begin_read()?;
    let runnable_ids = repository
        .list_runnable_erase_jobs(read.as_ref(), 1200)?
        .into_iter()
        .map(|job| job.job_id)
        .collect::<Vec<_>>();
    assert_eq!(
        runnable_ids,
        vec![pending_id, failed_none_retry_id, failed_due_id]
    );
    assert!(!runnable_ids.contains(&failed_future_id));
    assert!(!runnable_ids.contains(&running_id));
    assert!(!runnable_ids.contains(&finished_id));

    Ok(())
}

#[test]
fn job_repository_claim_finished_returns_false_without_change()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let job_id = {
        let mut txn = provider.begin_write("create and finish erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), job.job_id, 1100)?);
        repository.finish_erase_job(txn.as_mut(), job.job_id, 1200)?;
        txn.commit()?;
        job.job_id
    };

    {
        let mut txn = provider.begin_write("claim finished erase job")?;
        assert!(!repository.claim_erase_job(txn.as_mut(), job_id, 1300)?);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), job_id)?
        .expect("erase job should exist");
    assert_eq!(job.state, JobState::Finished);
    assert_eq!(job.updated_at_ms, 1200);

    Ok(())
}

#[test]
fn job_repository_finish_pending_returns_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let mut txn = provider.begin_write("finish pending erase job")?;
    let job = repository.create_erase_job(
        txn.as_mut(),
        CreateEraseJobRequest {
            table_id: 10,
            partition_id: Some(20),
            root_path: "s3://bucket/db/table/partition".to_string(),
            now_ms: 1000,
        },
    )?;
    let err = repository
        .finish_erase_job(txn.as_mut(), job.job_id, 1200)
        .expect_err("pending erase job should not finish");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}

#[test]
fn starrocks_table_repository_creates_database_table_and_active_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = StarRocksTableMetaRepository::default();

    {
        let mut txn = provider.begin_write("create StarRocks table objects")?;
        let database = repository.create_database(
            txn.as_mut(),
            CreateStarRocksDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = repository.create_table(
            txn.as_mut(),
            CreateStarRocksTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: StarRocksTableState::Creating,
                kind: StarRocksTableKind::MaterializedView,
            },
        )?;
        repository.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = repository.load_snapshot(read.as_ref())?;
    assert_eq!(snapshot.databases.len(), 1);
    assert_eq!(snapshot.tables.len(), 1);
    assert_eq!(snapshot.partitions.len(), 1);
    assert!(snapshot.schemas.is_empty());
    assert!(snapshot.columns.is_empty());
    assert!(snapshot.indexes.is_empty());
    assert!(snapshot.tablets.is_empty());

    assert_eq!(snapshot.databases[0].name, "db1");
    assert_eq!(snapshot.tables[0].db_id, snapshot.databases[0].db_id);
    assert_eq!(snapshot.tables[0].name, "orders");
    assert_eq!(snapshot.tables[0].keys_type, "DUP_KEYS");
    assert_eq!(snapshot.tables[0].bucket_num, 2);
    assert_eq!(snapshot.tables[0].current_schema_id, 10);
    assert_eq!(snapshot.tables[0].state, StarRocksTableState::Creating);
    assert_eq!(
        snapshot.tables[0].kind,
        StarRocksTableKind::MaterializedView
    );
    assert_eq!(snapshot.partitions[0].table_id, snapshot.tables[0].table_id);
    assert_eq!(snapshot.partitions[0].name, "orders");
    assert_eq!(
        snapshot.partitions[0].state,
        StarRocksPartitionState::Active
    );
    assert_eq!(snapshot.partitions[0].visible_version, 1);
    assert_eq!(snapshot.partitions[0].next_version, 2);

    Ok(())
}

#[test]
fn starrocks_table_repository_rejects_duplicate_table_name()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = StarRocksTableMetaRepository::default();

    let err = {
        let mut txn = provider.begin_write("create duplicate StarRocks table")?;
        let database = repository.create_database(
            txn.as_mut(),
            CreateStarRocksDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        repository.create_table(
            txn.as_mut(),
            CreateStarRocksTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            },
        )?;
        repository
            .create_table(
                txn.as_mut(),
                CreateStarRocksTableRequest {
                    db_id: database.db_id,
                    name: "ORDERS".to_string(),
                    keys_type: "DUP_KEYS".to_string(),
                    bucket_num: 2,
                    current_schema_id: 10,
                    state: StarRocksTableState::Active,
                    kind: StarRocksTableKind::Table,
                },
            )
            .expect_err("case-insensitive duplicate table name should fail")
    };

    assert!(err.to_string().contains("already exists"));

    Ok(())
}

#[test]
fn starrocks_table_repository_drops_table_and_purges_owned_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let starrocks_table_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();
    let job_repo = JobMetaRepository::default();

    let (table_id, _partition_id, bootstrap_txn_id) = {
        let mut txn = provider.begin_write("create StarRocks table layout")?;
        let database = starrocks_table_repo.get_or_create_database(txn.as_mut(), "analytics")?;
        let created = starrocks_table_repo.create_table_layout(
            txn.as_mut(),
            CreateStarRocksTableLayoutRequest {
                db_id: database.db_id,
                table_name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                kind: StarRocksTableKind::Table,
                schema_version: 0,
                tablet_schema_pb: vec![1, 2, 3],
                columns: vec![CreateStarRocksColumnRequest {
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                }],
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        let bootstrap_txn = txn_repo.record_visible_bootstrap(
            txn.as_mut(),
            created.table.table_id,
            created.partition.partition_id,
        )?;
        txn.commit()?;
        (
            created.table.table_id,
            created.partition.partition_id,
            bootstrap_txn.txn_id,
        )
    };

    {
        let mut txn = provider.begin_write("drop StarRocks table")?;
        txn_repo.ensure_no_inflight_for_table(txn.as_ref(), table_id)?;
        starrocks_table_repo.mark_table_dropping(txn.as_mut(), table_id)?;
        job_repo.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id,
                partition_id: None,
                root_path: "s3://bucket/warehouse/db_1/table_1".to_string(),
                now_ms: 1000,
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let snapshot = starrocks_table_repo.load_snapshot(read.as_ref())?;
        assert_eq!(snapshot.tables[0].state, StarRocksTableState::Dropping);
        assert_eq!(
            snapshot.partitions[0].state,
            StarRocksPartitionState::Retired
        );
        assert_eq!(snapshot.indexes.len(), 1);
        let jobs = job_repo.list_runnable_erase_jobs(read.as_ref(), 1000)?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].table_id, table_id);
        assert_eq!(jobs[0].partition_id, None);
    }

    {
        let mut txn = provider.begin_write("purge dropped StarRocks table")?;
        txn_repo.delete_for_table(txn.as_mut(), table_id)?;
        starrocks_table_repo.purge_retired_table_metadata(txn.as_mut(), table_id)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = starrocks_table_repo.load_snapshot(read.as_ref())?;
    assert!(snapshot.tables.is_empty());
    assert!(snapshot.schemas.is_empty());
    assert!(snapshot.columns.is_empty());
    assert!(snapshot.partitions.is_empty());
    assert!(snapshot.indexes.is_empty());
    assert!(snapshot.tablets.is_empty());
    assert!(txn_repo.load(read.as_ref(), bootstrap_txn_id)?.is_none());

    Ok(())
}

#[test]
fn starrocks_table_repository_stages_activates_and_purges_truncate_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let starrocks_table_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();
    let job_repo = JobMetaRepository::default();

    let (table_id, db_id, old_partition_id) = {
        let mut txn = provider.begin_write("create StarRocks table layout")?;
        let database = starrocks_table_repo.get_or_create_database(txn.as_mut(), "analytics")?;
        let created = starrocks_table_repo.create_table_layout(
            txn.as_mut(),
            CreateStarRocksTableLayoutRequest {
                db_id: database.db_id,
                table_name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                kind: StarRocksTableKind::Table,
                schema_version: 0,
                tablet_schema_pb: vec![1, 2, 3],
                columns: vec![CreateStarRocksColumnRequest {
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                }],
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        (
            created.table.table_id,
            database.db_id,
            created.partition.partition_id,
        )
    };

    let staged = {
        let mut txn = provider.begin_write("stage truncate partition")?;
        txn_repo.ensure_no_inflight_for_table(txn.as_ref(), table_id)?;
        let staged = starrocks_table_repo.stage_truncate_partition(
            txn.as_mut(),
            StageStarRocksTruncateRequest {
                table_id,
                db_id,
                bucket_num: 2,
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        staged
    };

    {
        let read = provider.begin_read()?;
        let snapshot = starrocks_table_repo.load_snapshot(read.as_ref())?;
        assert!(snapshot.partitions.iter().any(|partition| {
            partition.partition_id == staged.partition_id
                && partition.state == StarRocksPartitionState::Creating
        }));
        assert_eq!(staged.tablet_ids.len(), 2);
        assert_eq!(
            staged.partition_root_path,
            format!(
                "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{}",
                staged.partition_id
            )
        );
    }

    {
        let mut txn = provider.begin_write("activate truncate partition")?;
        starrocks_table_repo.activate_truncate_partition(
            txn.as_mut(),
            table_id,
            old_partition_id,
            staged.partition_id,
            staged.index_id,
        )?;
        job_repo.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id,
                partition_id: Some(old_partition_id),
                root_path: format!(
                    "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{old_partition_id}"
                ),
                now_ms: 1100,
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let snapshot = starrocks_table_repo.load_snapshot(read.as_ref())?;
        let old_partition = snapshot
            .partitions
            .iter()
            .find(|partition| partition.partition_id == old_partition_id)
            .expect("old partition");
        let new_partition = snapshot
            .partitions
            .iter()
            .find(|partition| partition.partition_id == staged.partition_id)
            .expect("new partition");
        assert_eq!(old_partition.state, StarRocksPartitionState::Retired);
        assert_eq!(new_partition.state, StarRocksPartitionState::Active);
        assert_eq!(new_partition.visible_version, 1);
        let jobs = job_repo.list_runnable_erase_jobs(read.as_ref(), 1100)?;
        assert_eq!(jobs[0].partition_id, Some(old_partition_id));
    }

    {
        let mut txn = provider.begin_write("purge retired truncate partition")?;
        txn_repo.delete_for_partition(txn.as_mut(), old_partition_id)?;
        starrocks_table_repo.purge_retired_partition_metadata(txn.as_mut(), old_partition_id)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = starrocks_table_repo.load_snapshot(read.as_ref())?;
    assert!(
        snapshot
            .partitions
            .iter()
            .all(|partition| partition.partition_id != old_partition_id)
    );
    assert!(
        snapshot
            .tablets
            .iter()
            .all(|tablet| tablet.partition_id != old_partition_id)
    );
    assert!(
        snapshot
            .indexes
            .iter()
            .all(|index| index.partition_id != old_partition_id)
    );

    Ok(())
}

#[test]
fn starrocks_table_repository_stages_and_activates_mv_refresh_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let starrocks_table_repo = StarRocksTableMetaRepository::default();
    let job_repo = JobMetaRepository::default();

    let (table_id, db_id, old_partition_id) = {
        let mut txn = provider.begin_write("create StarRocks MV layout")?;
        let database = starrocks_table_repo.get_or_create_database(txn.as_mut(), "analytics")?;
        let created = starrocks_table_repo.create_table_layout(
            txn.as_mut(),
            CreateStarRocksTableLayoutRequest {
                db_id: database.db_id,
                table_name: "orders_mv".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                kind: StarRocksTableKind::MaterializedView,
                schema_version: 0,
                tablet_schema_pb: vec![1, 2, 3],
                columns: vec![CreateStarRocksColumnRequest {
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                }],
                partition_name: "orders_mv".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        (
            created.table.table_id,
            database.db_id,
            created.partition.partition_id,
        )
    };

    let staged = {
        let mut txn = provider.begin_write("stage StarRocks MV refresh partition")?;
        let staged = starrocks_table_repo.stage_mv_refresh_partition(
            txn.as_mut(),
            StageStarRocksMvRefreshRequest {
                table_id,
                db_id,
                bucket_num: 2,
                partition_name: "orders_mv".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        staged
    };

    {
        let mut txn = provider.begin_write("reject overlapping StarRocks MV refresh")?;
        let err = starrocks_table_repo
            .stage_mv_refresh_partition(
                txn.as_mut(),
                StageStarRocksMvRefreshRequest {
                    table_id,
                    db_id,
                    bucket_num: 2,
                    partition_name: "orders_mv".to_string(),
                    warehouse_uri: "s3://bucket/warehouse".to_string(),
                },
            )
            .expect_err("creating partition should block overlapping refresh");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    {
        let read = provider.begin_read()?;
        let snapshot = starrocks_table_repo.load_snapshot(read.as_ref())?;
        let staged_partition = snapshot
            .partitions
            .iter()
            .find(|partition| partition.partition_id == staged.partition_id)
            .expect("staged partition");
        assert_eq!(staged_partition.state, StarRocksPartitionState::Creating);
        assert_eq!(staged.tablet_ids.len(), 2);
        assert_eq!(
            staged.partition_root_path,
            format!(
                "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{}",
                staged.partition_id
            )
        );
    }

    {
        let mut txn = provider.begin_write("activate StarRocks MV refresh partition")?;
        starrocks_table_repo.activate_mv_refresh_partition(
            txn.as_mut(),
            table_id,
            old_partition_id,
            staged.partition_id,
            staged.index_id,
        )?;
        job_repo.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id,
                partition_id: Some(old_partition_id),
                root_path: format!(
                    "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{old_partition_id}"
                ),
                now_ms: 1100,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = starrocks_table_repo.load_snapshot(read.as_ref())?;
    let old_partition = snapshot
        .partitions
        .iter()
        .find(|partition| partition.partition_id == old_partition_id)
        .expect("old partition");
    let new_partition = snapshot
        .partitions
        .iter()
        .find(|partition| partition.partition_id == staged.partition_id)
        .expect("new partition");
    assert_eq!(old_partition.state, StarRocksPartitionState::Retired);
    assert_eq!(new_partition.state, StarRocksPartitionState::Active);
    assert_eq!(new_partition.visible_version, 2);
    assert_eq!(new_partition.next_version, 3);
    let new_index = snapshot
        .indexes
        .iter()
        .find(|index| index.index_id == staged.index_id)
        .expect("new index");
    assert_eq!(new_index.state, StarRocksIndexState::Active);
    let jobs = job_repo.list_runnable_erase_jobs(read.as_ref(), 1100)?;
    assert_eq!(jobs[0].partition_id, Some(old_partition_id));

    Ok(())
}

#[test]
fn starrocks_txn_repository_prepare_written_visible_advances_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();

    let (table_id, partition_id) = {
        let mut txn = provider.begin_write("create StarRocks table objects")?;
        let database = meta_repo.create_database(
            txn.as_mut(),
            CreateStarRocksDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = meta_repo.create_table(
            txn.as_mut(),
            CreateStarRocksTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            },
        )?;
        let partition = meta_repo.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
        (table.table_id, partition.partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("commit StarRocks table txn")?;
        let starrocks_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        assert_eq!(starrocks_txn.table_id, table_id);
        assert_eq!(starrocks_txn.partition_id, partition_id);
        assert_eq!(starrocks_txn.base_version, 1);
        assert_eq!(starrocks_txn.commit_version, 2);
        assert_eq!(starrocks_txn.state, StarRocksTxnState::Prepared);
        txn_repo.mark_written(txn.as_mut(), starrocks_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), starrocks_txn.txn_id)?;
        txn.commit()?;
        starrocks_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("StarRocks txn should persist");
    assert_eq!(loaded.state, StarRocksTxnState::Visible);

    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 2);
    assert_eq!(partition.next_version, 3);

    Ok(())
}

#[test]
fn starrocks_txn_repository_abort_does_not_advance_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();

    let (table_id, partition_id) = {
        let mut txn = provider.begin_write("create StarRocks table objects")?;
        let database = meta_repo.create_database(
            txn.as_mut(),
            CreateStarRocksDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = meta_repo.create_table(
            txn.as_mut(),
            CreateStarRocksTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            },
        )?;
        let partition = meta_repo.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
        (table.table_id, partition.partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("abort StarRocks table txn")?;
        let starrocks_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_aborted(txn.as_mut(), starrocks_txn.txn_id)?;
        txn.commit()?;
        starrocks_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("StarRocks txn should persist");
    assert_eq!(loaded.state, StarRocksTxnState::Aborted);

    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 1);

    Ok(())
}

#[test]
fn starrocks_txn_repository_mark_written_is_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();
    let (table_id, partition_id) = create_starrocks_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("retry mark written")?;
        let starrocks_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), starrocks_txn.txn_id)?;
        txn_repo.mark_written(txn.as_mut(), starrocks_txn.txn_id)?;
        txn.commit()?;
        starrocks_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("StarRocks txn should persist");
    assert_eq!(loaded.state, StarRocksTxnState::Written);

    Ok(())
}

#[test]
fn starrocks_txn_repository_mark_visible_is_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();
    let (table_id, partition_id) = create_starrocks_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("retry mark visible")?;
        let starrocks_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), starrocks_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), starrocks_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), starrocks_txn.txn_id)?;
        txn_repo.mark_written(txn.as_mut(), starrocks_txn.txn_id)?;
        txn.commit()?;
        starrocks_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("StarRocks txn should persist");
    assert_eq!(loaded.state, StarRocksTxnState::Visible);
    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 2);
    assert_eq!(partition.next_version, 3);

    Ok(())
}

#[test]
fn starrocks_txn_repository_rejects_illegal_commit_version()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();
    let (table_id, partition_id) = create_starrocks_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("create invalid StarRocks txn")?;
        let txn_id = txn.allocate_id(id_scopes::starrocks_txn())?;
        put_starrocks_txn_record(
            txn.as_mut(),
            StoredStarRocksTxn {
                txn_id,
                table_id,
                partition_id,
                base_version: 1,
                commit_version: 3,
                state: StarRocksTxnState::Written,
                retry_at_ms: None,
                updated_at_ms: 0,
            },
        )?;
        txn.commit()?;
        txn_id
    };

    let mut txn = provider.begin_write("mark invalid StarRocks txn visible")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("illegal commit version should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Provider);

    Ok(())
}

#[test]
fn starrocks_txn_repository_rejects_partition_table_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();

    let (table_id, other_partition_id) = {
        let (table_id, _) = create_starrocks_table_with_partition(&provider, &meta_repo)?;
        let (other_table_id, other_partition_id) =
            create_named_starrocks_table_with_partition(&provider, &meta_repo, "lineitem")?;
        assert_ne!(table_id, other_table_id);
        (table_id, other_partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("create mismatched StarRocks txn")?;
        let txn_id = txn.allocate_id(id_scopes::starrocks_txn())?;
        put_starrocks_txn_record(
            txn.as_mut(),
            StoredStarRocksTxn {
                txn_id,
                table_id,
                partition_id: other_partition_id,
                base_version: 1,
                commit_version: 2,
                state: StarRocksTxnState::Written,
                retry_at_ms: None,
                updated_at_ms: 0,
            },
        )?;
        txn.commit()?;
        txn_id
    };

    let mut txn = provider.begin_write("mark mismatched StarRocks txn visible")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("partition table mismatch should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}

#[test]
fn starrocks_txn_repository_rejects_partition_next_version_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = StarRocksTableMetaRepository::default();
    let txn_repo = StarRocksTxnRepository::default();
    let (table_id, partition_id) = create_starrocks_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("prepare StarRocks txn with stale partition next")?;
        let starrocks_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), starrocks_txn.txn_id)?;
        let (revision, mut partition) = meta_repo
            .load_versioned_partition(txn.as_ref(), partition_id)?
            .expect("partition should persist");
        partition.next_version = 99;
        meta_repo.update_partition_exact(txn.as_mut(), &partition, revision)?;
        txn.commit()?;
        starrocks_txn.txn_id
    };

    let mut txn = provider.begin_write("mark StarRocks txn visible with stale partition next")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("partition next_version mismatch should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}
