// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Engine-owned Iceberg write transaction runner.
//!
//! The runner is the default boundary for user-level Iceberg SQL writes that
//! need coordinated file output, metadata commit, lifecycle persistence, and
//! post-commit finalization. It drives the Iceberg operation state machine and
//! persists facts via the operation repository, delegating the side-effecting
//! steps (running the coordinated write, calling the typed commit service,
//! finalization) to an [`IcebergWriteTransactionExecutor`]. PR-1 ships the
//! runner + fake-backed tests; the real executor and SQL routing land in PR-2.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome, CommitServiceError};
use crate::engine::StandaloneState;
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::query_result::QueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;

/// How the runner should commit the collected writer output.
pub(crate) struct IcebergWriteCommitPolicy {
    pub(crate) commit_op_kind: CommitOpKind,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) base_snapshot_map: BTreeMap<String, i64>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

/// SQL-specific validation captured at spec-build time. Consumed by the
/// executor's write step (the runner itself does not validate). Grown in PR-2.
pub(crate) struct IcebergWriteValidationPolicy {
    /// Branch writes require Iceberg format v3.
    pub(crate) require_v3_for_branch: bool,
}

/// What the write produces. The runner does not execute the source; the
/// executor does. Variants are filled out as flows are cut over in PR-2+.
pub(crate) enum IcebergWriteSource {
    /// Rows produced by a coordinated query/mutation plan.
    CoordinatedPlan,
}

/// A complete description of one Iceberg write transaction. SQL flows build
/// this; the runner owns the lifecycle.
pub(crate) struct IcebergWriteTransactionSpec {
    pub(crate) target: IcebergOperationTarget,
    pub(crate) operation_kind: IcebergOperationKind,
    pub(crate) attempt_id: String,
    pub(crate) commit: IcebergWriteCommitPolicy,
    pub(crate) validation: IcebergWriteValidationPolicy,
    pub(crate) source: IcebergWriteSource,
}

/// Outcome of a successful (or empty/no-op) transaction.
#[derive(Debug)]
pub(crate) struct IcebergWriteTransactionOutcome {
    pub(crate) query_result: QueryResult,
    /// `Some` for committed writes; `None` for empty/no-op writes.
    pub(crate) operation_id: Option<i64>,
    /// `Some` for committed writes.
    pub(crate) committed_snapshot_id: Option<i64>,
}

/// The side-effecting dependencies of a write transaction. Real implementation
/// (PR-2) wraps the execution coordinator + typed commit service + cache/dict
/// finalization; tests inject a fake.
pub(crate) trait IcebergWriteTransactionExecutor {
    /// Run the coordinated writer plan, returning the writer outcome.
    fn run_coordinated_write(
        &self,
        spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String>;

    /// Commit the collected writer output through the typed commit service.
    fn commit(
        &self,
        spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError>;

    /// Post-commit finalization (cache invalidation, dictionary stale marking).
    fn finalize(&self, spec: &IcebergWriteTransactionSpec) -> Result<(), String>;
}

/// Current time in unix milliseconds for operation-record timestamps.
fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
