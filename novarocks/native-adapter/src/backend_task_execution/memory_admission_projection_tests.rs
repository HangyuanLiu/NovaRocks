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

//! Exact real admission refusal projected at the existing task-resource boundary.
use super::memory_admission_failure_to_host;
use crate::native_fragment_query::{NativeFragmentAdmissionError, NativeFragmentQueryRuntime};
use novarocks_execution_contract::{TaskFailureCategory, TaskIdentity};
use novarocks_memory::{AuthorityConfig, CapacityError, MemoryAuthority};
use novarocks_types::UniqueId;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_worker::query_context::{QueryContextManager, QueryMemoryAccountError};
use std::{sync::Arc, time::Duration};

#[test]
fn mem_a1_v2_actual_none_metadata_refusal_uses_existing_task_resource_category() {
    let mut config = AuthorityConfig::new(1 << 30, 1 << 29, 1 << 29);
    config.max_accounts = 1;
    let authority = Arc::new(MemoryAuthority::new(config).unwrap());
    let runtime =
        NativeFragmentQueryRuntime::new_for_test(QueryContextManager::new_for_test(), authority);
    let execution =
        QueryExecutionId::new(QueryId::new(97401, 97402), AttemptId::new(1).unwrap()).unwrap();
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    let error = runtime
        .prepare_admission_execution_typed(
            execution,
            UniqueId::new(97403, 97404),
            Duration::from_secs(30),
            Duration::from_secs(30),
            None,
            None,
        )
        .err()
        .unwrap();
    assert!(matches!(
        &error,
        NativeFragmentAdmissionError::MemoryAccount(QueryMemoryAccountError::Capacity(
            CapacityError::MetadataExhausted { .. }
        ))
    ));
    let full = format!("task {identity} could not be admitted: {error}");
    let projected = memory_admission_failure_to_host(identity, &error);
    assert_eq!(projected.category(), TaskFailureCategory::ResourceExhausted);
    // The real metadata diagnostic fits the existing bounded task vocabulary.
    assert_eq!(projected.detail().as_str(), full);
}
