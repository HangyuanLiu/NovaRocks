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

#[test]
fn read_schema_construction_failure_keeps_resource_and_local_model_categories() {
    use super::{CompiledPackageError, compiled_package_rejection};
    use novarocks_plan_codec::physical_package_v2::PackageDecodeError;
    use novarocks_plan_codec::physical_public_source_schema_v2::PublicSourceSchemaCodecError;
    use novarocks_plan_codec::physical_read_scan_v2::ReadScanCodecError;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    let execution =
        QueryExecutionId::new(QueryId::new(97411, 97412), AttemptId::new(1).unwrap()).unwrap();
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    // The real checked Layout author refuses this array before allocation.
    let arithmetic = std::alloc::Layout::array::<u64>(usize::MAX)
        .map_err(|_| LayoutResourceError::Arithmetic)
        .unwrap_err();
    for (scan, category) in [
        (
            ReadScanCodecError::ConstructionResource(arithmetic),
            TaskFailureCategory::ResourceExhausted,
        ),
        (
            ReadScanCodecError::InvalidSourceSchema(PublicSourceSchemaCodecError::Layout(
                arithmetic,
            )),
            TaskFailureCategory::ResourceExhausted,
        ),
        (
            ReadScanCodecError::ConstructionResource(LayoutResourceError::SourceModel),
            TaskFailureCategory::Internal,
        ),
        (
            ReadScanCodecError::InvalidSourceSchema(PublicSourceSchemaCodecError::SourceModel(
                "locked source model is unavailable",
            )),
            TaskFailureCategory::Internal,
        ),
    ] {
        let cause = CompiledPackageError::Decode(PackageDecodeError::Scan(scan));
        let detail = format!("task {identity} package could not be compiled: {cause}");
        let projected = compiled_package_rejection(identity, cause);
        assert_eq!(projected.category(), category);
        assert_eq!(projected.detail().as_str(), detail);
    }
}

#[test]
fn actual_schema_tracker_refusal_is_projected_as_resource_without_text_inference() {
    use novarocks_execution::exec::pipeline::operator::input_construction::{
        ConstructionFailure, InputConstructionRequestHost, runtime::TrackedInputConstructionHost,
    };
    use novarocks_execution::runtime::{fragment::ExecutionFailure, mem_tracker::MemTracker};
    let tracker = MemTracker::new_root("typed schema limit");
    tracker.install_limit_once(1).unwrap();
    let host = TrackedInputConstructionHost::new(tracker.clone());
    let cause = host
        .request(std::alloc::Layout::array::<u64>(2).unwrap())
        .unwrap_err();
    assert!(matches!(&cause, ConstructionFailure::Memory(_)));
    assert_eq!(tracker.current(), 0, "unconstructed request is balanced");
    let original = cause.to_string();
    let failure = ExecutionFailure::from(cause);
    assert_eq!(failure.to_string(), original);
    assert_eq!(
        super::failure_cause_category(failure.cause(), TaskFailureCategory::Execution),
        TaskFailureCategory::ResourceExhausted
    );
}

#[test]
fn compiled_capability_refusal_does_not_reclassify_content_or_control_failures() {
    use super::{CompiledPackageError, compiled_package_rejection};
    use novarocks_type_contract::CompileControlError;
    let identity = TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(97421, 97422), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    for (error, expected) in [
        (
            CompiledPackageError::Unsupported("FULL OUTER hash join".into()),
            TaskFailureCategory::Unsupported,
        ),
        (
            CompiledPackageError::Refused("unsupported diagnostic from malformed package".into()),
            TaskFailureCategory::Protocol,
        ),
        (
            CompiledPackageError::Control(CompileControlError::ResourceExhausted),
            TaskFailureCategory::ResourceExhausted,
        ),
        (
            CompiledPackageError::Control(CompileControlError::Cancelled),
            TaskFailureCategory::Execution,
        ),
        (
            CompiledPackageError::Control(CompileControlError::DeadlineExceeded),
            TaskFailureCategory::Execution,
        ),
    ] {
        assert_eq!(
            compiled_package_rejection(identity, error).category(),
            expected
        );
    }
}
