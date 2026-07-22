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

// Re-export the proven core operation types under DML-owned names. DML-1 reuses
// these to stay zero-core-change; CP-3 later reshapes them into a typed
// phase-variant state machine without changing this module's port shapes.
pub use novarocks::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest as CreatePreparingRequest, IcebergCleanupOutcomeRecord,
    IcebergCommitOutcomeRecord, IcebergOperationFailureKind, IcebergOperationFailureRecord,
    IcebergOperationKind as OperationKind, IcebergOperationNextAction,
    IcebergOperationState as OperationState, IcebergOperationTarget as OperationTarget,
    IcebergRecoveryEvidenceRecord, StoredIcebergOperation as StoredOperation,
};

// The commit-result taxonomy the executor seam speaks. Re-exported so that
// WriteExecutor implementors (DML-2, tests) can name these types via
// `novarocks_frontend::dml`.
pub use novarocks::connector::iceberg::commit::{
    CleanupAttempt, CommitOpKind, CommitOutcome, CommitServiceError, RecoveryEvidence,
};

/// A durable fact recorded against an operation after a lifecycle step. Mirrors
/// core's crate-private `IcebergOperationFact`, re-authored DML-side (the core
/// module is `pub(crate)`), reusing the public record types above.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationFact {
    pub state: OperationState,
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    pub failure: Option<IcebergOperationFailureRecord>,
}

/// Declarative description of one Iceberg write transaction. SQL flows (DML-2+)
/// build this; the runner owns the lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteTransactionSpec {
    pub target: OperationTarget,
    pub operation_kind: OperationKind,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
}

/// Outcome of a successful (or empty/no-op) write transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteTransactionOutcome {
    /// `Some` for committed writes; `None` for empty/no-op writes.
    pub operation_id: Option<i64>,
    /// `Some` for committed writes.
    pub committed_snapshot_id: Option<i64>,
}
