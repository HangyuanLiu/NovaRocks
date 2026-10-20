// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Supervision of the original statement-owned Native attempt.
//!
//! This port owns no Task/SQL conclusion, actor gate, retry policy, or publication
//! authority. The trusted FE author projects actual local exits and exact remote
//! closure facts from the same owner which started that work.

use super::NativeAttemptConvergence;
use novarocks_execution_contract::QueryContextRef;
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{CancellationView, WorkId};
use std::time::Instant;
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum StatementNativeLocalHolder {
    RootPolls,
    InternalCpu,
    SplitSources,
    NativeOperations,
    StatusSubscriptions,
}

/// A trusted domain projection of an original local holder's closure. The
/// actual-exit constructor requires its real join/exit receipt. Native caller
/// capability return is a distinct disposition under the same original owner.
/// Cancellation, timeout, Context convergence, or an ACK alone is insufficient.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatementNativeLocalExit {
    holder: StatementNativeLocalHolder,
    kind: StatementNativeLocalClosureKind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementNativeLocalClosureKind {
    ActualExited,
    OriginalProcessOwnerReturn,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementNativeLocalReturnError {
    HolderRequiresActualExit,
}
impl StatementNativeLocalExit {
    pub const fn after_actual_exit(holder: StatementNativeLocalHolder) -> Self {
        Self {
            holder,
            kind: StatementNativeLocalClosureKind::ActualExited,
        }
    }
    /// The trusted FE author returned its caller capabilities to the SAME
    /// pre-existing Native record. That process owner still owes the actual
    /// IO/body/subscription join under its original child responsibility.
    /// This is custody, not network actual exit, and grants no new authority.
    pub fn after_original_process_owner_return(
        holder: StatementNativeLocalHolder,
    ) -> Result<Self, StatementNativeLocalReturnError> {
        if !matches!(
            holder,
            StatementNativeLocalHolder::NativeOperations
                | StatementNativeLocalHolder::StatusSubscriptions
        ) {
            return Err(StatementNativeLocalReturnError::HolderRequiresActualExit);
        }
        Ok(Self {
            holder,
            kind: StatementNativeLocalClosureKind::OriginalProcessOwnerReturn,
        })
    }
    pub const fn kind(self) -> StatementNativeLocalClosureKind {
        self.kind
    }
    pub const fn holder(self) -> StatementNativeLocalHolder {
        self.holder
    }
}

/// Move-only closure evidence, authored by the original FE owner. The receiving
/// registry checks the exact execution, original WorkId, and complete expected
/// remote-context and local-holder sets. It never reconstructs a missing fact.
#[derive(Debug)]
pub struct StatementNativeClosure {
    pub(crate) execution: QueryExecutionId,
    pub(crate) scope: WorkId,
    pub(crate) remote: StatementNativeRemoteClosure,
    pub(crate) local: Box<[StatementNativeLocalExit]>,
}
impl StatementNativeClosure {
    pub fn after_actual_exit(
        execution: QueryExecutionId,
        scope: WorkId,
        remote: StatementNativeRemoteClosure,
        local: impl Into<Box<[StatementNativeLocalExit]>>,
    ) -> Self {
        Self {
            execution,
            scope,
            remote,
            local: local.into(),
        }
    }
}

/// Remote observation ends separately from local actual exits. Unknown never
/// supplies WorkerStopped, a complete RF contribution, or replay permission.
#[derive(Debug)]
pub enum StatementNativeRemoteClosure {
    Evidence(NativeAttemptConvergence),
    TrackingEnded {
        exact_contexts: Box<[QueryContextRef]>,
    },
}

#[derive(Clone)]
pub struct StatementNativeConvergenceRequest {
    execution: QueryExecutionId,
    scope: WorkId,
    cancellation: CancellationView,
    remote_observation_deadline: Instant,
}
impl StatementNativeConvergenceRequest {
    pub(crate) fn new(
        execution: QueryExecutionId,
        scope: WorkId,
        cancellation: CancellationView,
        remote_observation_deadline: Instant,
    ) -> Self {
        Self {
            execution,
            scope,
            cancellation,
            remote_observation_deadline,
        }
    }
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    pub const fn scope(&self) -> WorkId {
        self.scope
    }
    pub fn cancellation(&self) -> &CancellationView {
        &self.cancellation
    }
    pub const fn remote_observation_deadline(&self) -> Instant {
        self.remote_observation_deadline
    }
}

/// Process-health failure: this original owner cannot prove a local exit.
/// It is not a domain query failure and cannot change Task/SQL conclusions.
/// The original typed source is retained by the process supervision owner.
#[derive(Clone, Debug)]
pub struct StatementNativeHealthFailure {
    detail: &'static str,
    cause: Arc<dyn std::error::Error + Send + Sync>,
}
impl StatementNativeHealthFailure {
    pub fn actual_exit_unproven(
        detail: &'static str,
        cause: Arc<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self { detail, cause }
    }
    pub const fn detail(&self) -> &'static str {
        self.detail
    }
    pub fn cause(&self) -> &(dyn std::error::Error + Send + Sync) {
        self.cause.as_ref()
    }
    pub(crate) fn into_parts(self) -> (&'static str, Arc<dyn std::error::Error + Send + Sync>) {
        (self.detail, self.cause)
    }
}
pub type StatementNativeConvergenceFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<StatementNativeClosure, StatementNativeHealthFailure>>
            + Send
            + 'a,
    >,
>;

/// The one original statement Native owner, retained in its original Box.
///
/// Convergence borrows this owner. All started-operation receipts and held
/// payloads must remain in it or their original acknowledged owners before an
/// await. Dropping the borrowed future or catching a poll panic leaves the same
/// owner resumable for cleanup. A first failure must already have been submitted
/// to its real conclusion owner; convergence cannot publish a different result.
///
/// Return a process-health failure if an original holder's exit is unprovable.
/// The registry retains this same owner without repeated polling; only explicit
/// process exit can then abandon it. Ordinary domain errors with proven actual
/// exits belong to existing diagnostics and still permit closure evidence.
///
/// `local_holders` declares every FE-local asynchronous family this owner may
/// start, including families which have not started yet. An unused family still
/// needs its author's exact no-live-work check before an exit fact is authored.
pub trait StatementNativeAttemptOwner: Send + 'static {
    fn execution(&self) -> QueryExecutionId;
    fn contexts(&self) -> &[QueryContextRef];
    fn local_holders(&self) -> &[StatementNativeLocalHolder];
    fn request_shutdown_cleanup(&mut self);
    fn converge(
        &mut self,
        request: StatementNativeConvergenceRequest,
    ) -> StatementNativeConvergenceFuture<'_>;
}
