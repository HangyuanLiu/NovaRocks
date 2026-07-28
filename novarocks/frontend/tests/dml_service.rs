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
use std::sync::Arc;

use novarocks::meta::{MetaStoreProvider, SqliteMetaStoreProvider};
use novarocks_frontend::dml::{
    CommitOutcome, CommitServiceError, CoordinatedWriteReport, DmlService,
    MetaStoreOperationJournal, OperationKind, OperationState, OperationTarget, WriteExecutor,
    WriteTransactionSpec,
};

struct FakeExecutor;

impl WriteExecutor for FakeExecutor {
    type CommitHandle = ();

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<()>, String> {
        Ok(CoordinatedWriteReport::Committable(()))
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        _handle: &(),
    ) -> Result<CommitOutcome, CommitServiceError> {
        Ok(CommitOutcome {
            new_snapshot_id: 555,
            written_manifest_paths: vec![],
        })
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        Ok(())
    }
}

#[test]
fn dml_service_commits_over_real_meta_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider: Arc<dyn MetaStoreProvider> =
        Arc::new(SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite")).expect("provider"));
    let service = DmlService::new(Arc::new(MetaStoreOperationJournal::new(provider)));

    let spec = WriteTransactionSpec {
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: None,
        },
        operation_kind: OperationKind::InsertAppend,
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: None,
        base_snapshot_map: BTreeMap::new(),
    };

    let outcome = service
        .run_write(spec, &FakeExecutor)
        .expect("write succeeds");
    let id = outcome.operation_id.expect("committed operation id");
    assert_eq!(outcome.committed_snapshot_id, Some(555));

    let stored = service
        .load_operation(id)
        .unwrap()
        .expect("operation persisted");
    assert_eq!(stored.state, OperationState::Finalized);
    assert_eq!(stored.commit_outcome.unwrap().snapshot_id, 555);
    assert!(service.list_unfinished_operations().unwrap().is_empty());
}
