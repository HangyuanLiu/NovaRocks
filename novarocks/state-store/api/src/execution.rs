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

//! Borrowed execution contract for a provider's original blocking source.
//!
//! No executor, Root, permission, task, retry policy, or storage algorithm lives
//! here. Implementations must be installed in the existing role/Action owner
//! before borrowed API futures can request a phase. In particular, a Weak slot
//! alone is not custody and dropping a caller does not prove physical exit.

use std::{
    any::Any,
    error::Error,
    sync::{Arc, Weak},
    task::{Context, Poll},
    time::Instant,
};

use crate::{InstanceScope, StateStoreError, StateStoreErrorKind, StateStoreProviderId};

/// Purpose identity, never execution authority. Opening is accepted only by
/// the selected Host's private opening port; it cannot authorize transactions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreBlockingOrigin {
    Opening(StateStoreProviderId),
    Instance(InstanceScope),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreBlockingPhase {
    Open,
    BeginRead,
    BeginWrite,
    Get,
    Range,
    Put,
    Delete,
    Commit,
    Rollback,
    EvidenceRead,
    EvidenceRelease,
}

/// Original-owned serial request revision. A stale phase must never cause a
/// second commit or be treated as a new permission to run the original call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateStoreBlockingRequest {
    pub phase: StateStoreBlockingPhase,
    pub revision: std::num::NonZeroU64,
}

/// The private concrete provider type supplies its actual Any TypeId. Input,
/// connection, attempt, output, and evidence receiver remain stable fields of
/// this same original. Implementations execute only their existing algorithm.
pub trait StateStoreBlockingOriginal: Any + Send + Sync {
    fn origin(&self) -> StateStoreBlockingOrigin;
    fn requested(&self) -> Option<StateStoreBlockingRequest>;
    /// Called only by the accepted physical worker for this exact request.
    /// Must reject a stale request inside the original without redispatch.
    fn run_original_phase(&self, request: StateStoreBlockingRequest);
    /// Nonblocking stop request. Transaction abandonment/rollback obligation
    /// is a separate original-owned fact, not implied by a method F dropping.
    fn request_interrupt(&self);
}

/// Preserves the actual executor/provider source without formatting or
/// replacing it with a domain string. Business results stay in Original;
/// this carrier reports physical admission/health/exit failures only.
pub type StateStoreExecutionFailure = Arc<dyn Error + Send + Sync>;

/// These are distinct proofs. NeverPublishedReturned requires normal return
/// from destruction of the same unpublished call and reservation; it is not a
/// worker-exit event and cannot resolve an uncertain publication as unstarted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreBlockingExit {
    NeverPublishedReturned,
    ActualExited,
}

/// Borrower of a receiving record already strongly held by the original
/// role/Action, then moved unchanged into its residual. A slot must not strongly
/// point back to that owner or turn the provider original into a strong cycle.
/// No owner lock may span reserve, physical call, foreign poll/drop, or notify.
pub trait StateStoreBlockingSlot: Send {
    /// Advances only the same original request. Full waits on the original
    /// executor queue with the captured stop/deadline; it is not a new quota.
    /// Success means the physical result is stable in the original receiver,
    /// not that the worker's call/result/destructor has actually exited.
    fn poll_phase(
        &mut self,
        cx: &mut Context<'_>,
        request: StateStoreBlockingRequest,
    ) -> Poll<Result<(), StateStoreExecutionFailure>>;
    fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
        request: StateStoreBlockingRequest,
    ) -> Poll<Result<StateStoreBlockingExit, StateStoreExecutionFailure>>;
}

/// Concrete ports validate selected private TypeId, origin/instance, phase,
/// and captured original authority before mutation. Opening uses only its Host
/// process owner and ordinary executor. Statement/Job ports require their same
/// positive Root/source; an Opening port is never a missing-context fallback.
pub trait StateStoreExecutionPort: Send + Sync {
    /// Before returning a borrower in target, install the strong Original plus
    /// actual physical receiving record into existing stable owner custody.
    /// Refusal preserves target and original. This method performs no I/O or
    /// physical publication. Deadline is the captured original absolute value;
    /// cancellation comes from that same port's original owner, never a new F.
    fn bind_original_in(
        &self,
        original: &Arc<dyn StateStoreBlockingOriginal>,
        deadline: Instant,
        target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure>;
}

/// Weak borrower of an already-installed port, not a permission constructor.
/// The real role/Action must keep the port and its strong custody alive through
/// rollback/evidence/actual exit, including after this context or outer F drops.
#[derive(Clone)]
pub struct StateStoreExecutionContext {
    port: Weak<dyn StateStoreExecutionPort>,
    deadline: Instant,
}
impl StateStoreExecutionContext {
    pub fn new(port: &Arc<dyn StateStoreExecutionPort>, deadline: Instant) -> Self {
        Self {
            port: Arc::downgrade(port),
            deadline,
        }
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn bind_original_in(
        &self,
        original: &Arc<dyn StateStoreBlockingOriginal>,
        target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        if target.is_some() {
            return Err(Arc::new(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store original receiving slot is not empty",
            )));
        }
        let port = self
            .port
            .upgrade()
            .ok_or_else(|| -> StateStoreExecutionFailure {
                Arc::new(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "state store original execution owner is unavailable",
                ))
            })?;
        port.bind_original_in(original, self.deadline, target)
    }
}
