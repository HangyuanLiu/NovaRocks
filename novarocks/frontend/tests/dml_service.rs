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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_frontend::dml::{
    CoordinatedWriteReport, DmlErrorKind, DmlService, OperationKind, OperationState,
    OperationTarget, StateStoreOperationJournal, WriteExecutor, WriteTransactionSpec,
};
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorWriteAbortOutcome,
    ConnectorWriteReceipt, ExternalMutationEffect, ExternalMutationFinalization,
    ExternalMutationOutcome,
};
use novarocks_spi::state_store::{FeDeploymentView, StateStore};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

struct FakeExecutor;

impl WriteExecutor for FakeExecutor {
    type CommitHandle = ();
    type AbortHandle = std::convert::Infallible;

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<()>, String> {
        Ok(CoordinatedWriteReport::CommitRequired(()))
    }

    fn abort(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::AbortHandle,
    ) -> Result<ConnectorWriteAbortOutcome, String> {
        match *handle {}
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        _handle: &(),
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt(b"commit-555"),
            finalization: ExternalMutationFinalization::Complete,
        })
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        Ok(())
    }
}

struct KnownCommittedCommitErrorExecutor;

impl WriteExecutor for KnownCommittedCommitErrorExecutor {
    type CommitHandle = ();
    type AbortHandle = std::convert::Infallible;

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<()>, String> {
        Ok(CoordinatedWriteReport::CommitRequired(()))
    }

    fn abort(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::AbortHandle,
    ) -> Result<ConnectorWriteAbortOutcome, String> {
        match *handle {}
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        _handle: &(),
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt(b"commit-777"),
            finalization: ExternalMutationFinalization::Complete,
        })
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        Err("finalize failed after provider commit".to_string())
    }
}

fn receipt(bytes: &'static [u8]) -> ConnectorWriteReceipt {
    ConnectorWriteReceipt::try_new(Bytes::from_static(bytes)).expect("test receipt")
}

async fn open_journal(
    path: &std::path::Path,
) -> (
    StateStoreHost,
    Arc<dyn StateStore>,
    StateStoreOperationJournal,
) {
    let registry = builtin_state_store_provider_registry().expect("provider registry");
    let host = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "dml-service-test".to_string(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path: path.to_path_buf(),
                        deployment_owner: "dml-service-fe".to_string(),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"dml-service-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite StateStore");
    let store = host.state_store().expect("StateStore exposure");
    let journal =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("open DML journal");
    (host, store, journal)
}

#[tokio::test(flavor = "multi_thread")]
async fn dml_service_commits_over_real_state_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_host, _store, journal) = open_journal(&dir.path().join("state.sqlite")).await;
    let service = DmlService::new(Arc::new(journal));

    let spec = WriteTransactionSpec {
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: None,
        },
        operation_kind: OperationKind::InsertAppend,
        operation_subkind: None,
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: None,
        base_snapshot_map: BTreeMap::new(),
    };

    let outcome = service
        .run_write(spec, &FakeExecutor)
        .expect("write succeeds");
    let id = outcome.operation_id.expect("committed operation id");
    assert_eq!(outcome.committed_receipt, Some(receipt(b"commit-555")));

    let stored = service
        .load_operation(id)
        .unwrap()
        .expect("operation persisted");
    assert_eq!(stored.state, OperationState::Finalized);
    assert!(service.list_unfinished_operations().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn known_committed_commit_error_persists_retry_finalize_fact_over_real_state_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_host, _store, journal) = open_journal(&dir.path().join("state.sqlite")).await;
    let service = DmlService::new(Arc::new(journal));

    let spec = WriteTransactionSpec {
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: None,
        },
        operation_kind: OperationKind::InsertAppend,
        operation_subkind: None,
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: Some(10),
        base_snapshot_map: BTreeMap::new(),
    };

    let error = service
        .run_write(spec, &KnownCommittedCommitErrorExecutor)
        .expect_err("known-committed finalize failure must remain an error");
    assert_eq!(error.kind(), DmlErrorKind::CommittedButUnfinalized);
    assert!(
        error
            .to_string()
            .contains("post-commit finalization failed")
    );

    let stored = service
        .list_unfinished_operations()
        .unwrap()
        .into_iter()
        .next()
        .expect("operation persisted");
    assert_eq!(stored.state, OperationState::FinalizeFailedKnownCommitted);
    let novarocks_frontend::dml::OperationPayload::ConnectorWriteLifecycle(
        novarocks_frontend::dml::ConnectorWriteLifecycleRecord::KnownCommitted {
            receipt_wire,
            finalization: novarocks_frontend::dml::ConnectorWriteFinalizationRecord::Failed(failure),
        },
    ) = stored.payload
    else {
        panic!("expected provider-neutral known-committed terminal fact");
    };
    assert_eq!(
        receipt_wire.try_decode().expect("decode receipt"),
        receipt(b"commit-777")
    );
    assert_eq!(
        failure.kind,
        novarocks_frontend::dml::ConnectorWriteFailureKind::Internal
    );
    assert_eq!(failure.message, "finalize failed after provider commit");
}
