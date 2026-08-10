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

//! Frontend-owned operation authority for durable DML mutations.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use async_trait::async_trait;
use novarocks_spi::state_store::{StateStore, TransactionId, WriteTransaction};
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, CoordinationError, CoordinationErrorKind, LeaseGuard, WriteAdmission,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::coordination::FrontendCoordinationRuntime;
use crate::dml::error::DmlError;
use crate::dml::journal::{
    DmlIntentAdmissionValidator, DmlMutationAuthority, DmlMutationAuthorityValidator,
    OperationJournal, dml_operation_resource_key,
};
use crate::dml::model::{
    AddFilesMutationRequest, DML_COORDINATION_RESOURCE_CODEC_VERSION,
    DML_FOREGROUND_RECOVERY_VISIBILITY_MS, DmlCoordinationClaimRequest, DmlCoordinationProvenance,
    DmlFencingTokenV1, DmlOperationId, DmlRecoveryDueRescheduleRequest, OperationFact,
    OperationMutationRequest, OperationPayload, OperationState, StatementNextAction,
    StoredOperation,
};
use crate::dml::now_unix_millis;

#[derive(Clone)]
// Design: ADR-0051 (docs/adr/ADR-0051-frontend-dml-operation-authority-boundary.md)
pub(crate) struct DmlCoordinator {
    frontend: Arc<FrontendCoordinationRuntime>,
    runtime: Handle,
    closing: Arc<AtomicBool>,
    active: Arc<StdMutex<BTreeMap<DmlOperationId, Weak<DmlOperationAuthorityInner>>>>,
}

impl DmlCoordinator {
    pub(crate) fn new(frontend: Arc<FrontendCoordinationRuntime>, runtime: Handle) -> Self {
        Self {
            frontend,
            runtime,
            closing: Arc::new(AtomicBool::new(false)),
            active: Arc::new(StdMutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn admission(&self) -> Result<Arc<dyn DmlIntentAdmissionValidator>, DmlError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is shutting down",
            ));
        }
        let admission = self.blocking(async {
            self.frontend
                .admit_writes()
                .await
                .map_err(map_coordination_error)
        })?;
        Ok(Arc::new(CurrentDmlAdmission { admission }))
    }

    pub(crate) fn claim_foreground(
        &self,
        journal: Arc<dyn OperationJournal>,
        operation: StoredOperation,
    ) -> Result<ActiveDmlOperation, DmlError> {
        let admission = self.admission()?;
        self.claim_inner(journal, operation, Some(admission))
    }

    pub(crate) fn claim_recovery(
        &self,
        journal: Arc<dyn OperationJournal>,
        operation: StoredOperation,
    ) -> Result<ActiveDmlOperation, DmlError> {
        self.claim_inner(journal, operation, None)
    }

    fn claim_inner(
        &self,
        journal: Arc<dyn OperationJournal>,
        operation: StoredOperation,
        admission: Option<Arc<dyn DmlIntentAdmissionValidator>>,
    ) -> Result<ActiveDmlOperation, DmlError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is shutting down",
            )
            .with_operation_id(operation.operation_id));
        }
        let attempt_uuid = Uuid::now_v7();
        let attempt = AttemptId::try_from(attempt_uuid).map_err(map_coordination_error)?;
        let acquire_operation_uuid = Uuid::now_v7();
        let acquire_operation_id = OperationId::from(acquire_operation_uuid);
        let resource = dml_operation_resource_key(operation.operation_id)?;
        let manager = self.frontend.lease_manager();
        let outcome = self.blocking(async {
            match manager
                .acquire(resource.clone(), attempt, acquire_operation_id)
                .await
            {
                Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => manager
                    .recover_acquire(resource, attempt, acquire_operation_id)
                    .await
                    .map_err(map_coordination_error),
                Ok(outcome) => Ok(outcome),
                Err(error) => Err(map_coordination_error(error)),
            }
        })?;
        let guard = match outcome {
            AcquireOutcome::Acquired(guard) => guard,
            AcquireOutcome::Contended(observation) => {
                return Err(DmlError::coordination_contended(format!(
                    "DML operation lease is contended; retry after {:?}",
                    observation.retry_after()
                ))
                .with_operation_id(operation.operation_id));
            }
            AcquireOutcome::AwaitingTakeover(observation) => {
                return Err(DmlError::coordination_contended(format!(
                    "DML operation lease awaits takeover observation; retry after {:?}",
                    observation.retry_after()
                ))
                .with_operation_id(operation.operation_id));
            }
        };

        let inner = Arc::new(DmlOperationAuthorityInner {
            operation_id: operation.operation_id,
            coordination_attempt_id: attempt_uuid,
            guard: Arc::new(Mutex::new(guard)),
            lost: AtomicBool::new(false),
            released: AtomicBool::new(false),
            stop: watch::channel(false).0,
            renewal: StdMutex::new(None),
        });
        let authority = DmlOperationAuthority {
            inner: Arc::clone(&inner),
            runtime: self.runtime.clone(),
            active: Arc::downgrade(&self.active),
            closing: Arc::clone(&self.closing),
            store: self.frontend.store(),
        };
        let token = self.blocking(async {
            let guard = inner.guard.lock().await;
            DmlFencingTokenV1::try_from_token(guard.token()).map_err(DmlError::journal_corruption)
        })?;
        let provenance = DmlCoordinationProvenance {
            resource_codec_version: DML_COORDINATION_RESOURCE_CODEC_VERSION,
            holder_id: self.frontend.holder_uuid(),
            coordination_attempt_id: attempt_uuid,
            fencing_token: token,
            acquired_at_ms: now_unix_millis(),
        };
        let claim = DmlCoordinationClaimRequest {
            operation_id: operation.operation_id,
            expected_revision: operation.revision,
            mutation_id: Uuid::now_v7(),
            provenance,
            recovery_due_at_ms: now_unix_millis()
                .saturating_add(DML_FOREGROUND_RECOVERY_VISIBILITY_MS),
        };
        let journal_authority = authority.journal_authority()?;
        let claim_result = match admission {
            Some(admission) => {
                journal.claim_operation_admitted(claim, admission, journal_authority)
            }
            None => journal.claim_operation(claim, journal_authority),
        };
        let claimed = match claim_result {
            Ok(claimed) => claimed,
            Err(error) => {
                let _ = authority.release();
                return Err(error.with_operation_id(operation.operation_id));
            }
        };
        authority.start_renewal();
        {
            let mut active = self
                .active
                .lock()
                .expect("DML authority registry lock poisoned");
            if self.closing.load(Ordering::Acquire) {
                drop(active);
                let _ = authority.release();
                return Err(DmlError::coordination_unresolved(
                    "frontend DML coordination shut down while claiming an operation",
                )
                .with_operation_id(operation.operation_id));
            }
            active.insert(operation.operation_id, Arc::downgrade(&inner));
        }
        Ok(ActiveDmlOperation {
            journal,
            stored: claimed,
            authority: Some(authority),
        })
    }

    pub(crate) async fn shutdown(&self) -> Result<(), DmlError> {
        self.closing.store(true, Ordering::Release);
        let authorities = self
            .active
            .lock()
            .expect("DML authority registry lock poisoned")
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for inner in &authorities {
            inner.lost.store(true, Ordering::Release);
            inner.stop.send_replace(true);
        }
        let mut first_error = None;
        for inner in authorities {
            let authority = DmlOperationAuthority {
                inner,
                runtime: self.runtime.clone(),
                active: Arc::downgrade(&self.active),
                closing: Arc::clone(&self.closing),
                store: self.frontend.store(),
            };
            if let Err(error) = authority.release_async().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        self.active
            .lock()
            .expect("DML authority registry lock poisoned")
            .clear();
        first_error.map_or(Ok(()), Err)
    }

    fn blocking<T>(
        &self,
        future: impl Future<Output = Result<T, DmlError>>,
    ) -> Result<T, DmlError> {
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::coordination_unresolved(
                    "DML coordination cannot block a current-thread Tokio runtime",
                ))
            }
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        }
    }
}

struct CurrentDmlAdmission {
    admission: WriteAdmission,
}

#[async_trait]
impl DmlIntentAdmissionValidator for CurrentDmlAdmission {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        self.admission
            .validate_in(transaction)
            .await
            .map_err(map_coordination_error)
    }
}

#[derive(Clone)]
pub(crate) struct DmlOperationAuthority {
    inner: Arc<DmlOperationAuthorityInner>,
    runtime: Handle,
    active: Weak<StdMutex<BTreeMap<DmlOperationId, Weak<DmlOperationAuthorityInner>>>>,
    closing: Arc<AtomicBool>,
    store: Arc<dyn StateStore>,
}

struct DmlOperationAuthorityInner {
    operation_id: DmlOperationId,
    coordination_attempt_id: Uuid,
    guard: Arc<Mutex<LeaseGuard>>,
    lost: AtomicBool,
    released: AtomicBool,
    stop: watch::Sender<bool>,
    renewal: StdMutex<Option<JoinHandle<()>>>,
}

impl DmlOperationAuthority {
    fn abandon(&self) {
        if self.inner.released.load(Ordering::Acquire) {
            return;
        }
        self.inner.lost.store(true, Ordering::Release);
        self.inner.stop.send_replace(true);
        let this = self.clone();
        self.runtime.spawn(async move {
            if let Err(error) = this.release_async().await {
                tracing::warn!(
                    operation_id = %this.inner.operation_id,
                    error = %error,
                    "best-effort DML authority release after operation drop failed"
                );
            }
        });
    }

    pub(crate) fn check_before_dispatch(&self) -> Result<(), DmlError> {
        if self.closing.load(Ordering::Acquire)
            || self.inner.lost.load(Ordering::Acquire)
            || self.inner.released.load(Ordering::Acquire)
        {
            return Err(DmlError::coordination_lost(
                "DML operation authority is no longer current",
            )
            .with_operation_id(self.inner.operation_id));
        }
        let runtime = self.runtime.clone();
        let this = self.clone();
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::coordination_unresolved(
                    "DML coordination cannot block a current-thread Tokio runtime",
                )
                .with_operation_id(self.inner.operation_id))
            }
            Ok(_) => tokio::task::block_in_place(|| runtime.block_on(this.check_current_async())),
            Err(_) => runtime.block_on(this.check_current_async()),
        }
    }

    async fn check_current_async(&self) -> Result<(), DmlError> {
        let mut transaction = self
            .store
            .begin_write(
                TransactionId::from(Uuid::now_v7()),
                "validate DML operation authority before provider dispatch",
            )
            .await
            .map_err(DmlError::coordination_unresolved)?;
        let guard = self.inner.guard.lock().await;
        let validation = guard
            .fence()
            .validate_in(transaction.as_mut())
            .await
            .map_err(map_coordination_error);
        drop(guard);
        let abort = transaction
            .abort()
            .await
            .map_err(DmlError::coordination_unresolved);
        if validation.is_err() {
            self.inner.lost.store(true, Ordering::Release);
        }
        validation
            .and(abort)
            .map_err(|error| error.with_operation_id(self.inner.operation_id))
    }

    pub(crate) fn journal_authority(&self) -> Result<DmlMutationAuthority, DmlError> {
        DmlMutationAuthority::try_new(
            self.inner.coordination_attempt_id,
            Arc::new(CurrentDmlLeaseFence {
                guard: Arc::clone(&self.inner.guard),
                lost: Arc::downgrade(&self.inner),
            }),
        )
    }

    fn start_renewal(&self) {
        let weak = Arc::downgrade(&self.inner);
        let mut stop = self.inner.stop.subscribe();
        let handle = self.runtime.spawn(async move {
            loop {
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let renew_after = {
                    let guard = inner.guard.lock().await;
                    guard.renew_after()
                };
                drop(inner);
                tokio::select! {
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                    () = tokio::time::sleep(renew_after) => {}
                }
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let operation_id = OperationId::new_v7();
                let result = {
                    let mut guard = inner.guard.lock().await;
                    match guard.renew(operation_id).await {
                        Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                            guard.recover_renew(operation_id).await
                        }
                        result => result,
                    }
                };
                if result.is_err() {
                    inner.lost.store(true, Ordering::Release);
                    return;
                }
            }
        });
        *self
            .inner
            .renewal
            .lock()
            .expect("DML renewal task lock poisoned") = Some(handle);
    }

    pub(crate) fn release(&self) -> Result<(), DmlError> {
        let runtime = self.runtime.clone();
        let this = self.clone();
        match Handle::try_current() {
            Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(DmlError::coordination_unresolved(
                    "DML coordination cannot block a current-thread Tokio runtime",
                )
                .with_operation_id(self.inner.operation_id))
            }
            Ok(_) => tokio::task::block_in_place(|| runtime.block_on(this.release_async())),
            Err(_) => runtime.block_on(this.release_async()),
        }
    }

    async fn release_async(&self) -> Result<(), DmlError> {
        if self.inner.released.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.stop.send_replace(true);
        let renewal = self
            .inner
            .renewal
            .lock()
            .expect("DML renewal task lock poisoned")
            .take();
        if let Some(renewal) = renewal {
            let _ = renewal.await;
        }
        let operation_id = OperationId::new_v7();
        let result = {
            let mut guard = self.inner.guard.lock().await;
            match guard.release(operation_id).await {
                Err(error) if error.kind() == CoordinationErrorKind::CommitUncertain => {
                    guard.recover_release(operation_id).await
                }
                result => result,
            }
        };
        if let Some(active) = self.active.upgrade() {
            let mut active = active.lock().expect("DML authority registry lock poisoned");
            let remove = active
                .get(&self.inner.operation_id)
                .and_then(Weak::upgrade)
                .is_some_and(|current| Arc::ptr_eq(&current, &self.inner));
            if remove {
                active.remove(&self.inner.operation_id);
            }
        }
        result.map_err(|error| {
            map_coordination_error(error).with_operation_id(self.inner.operation_id)
        })
    }
}

struct CurrentDmlLeaseFence {
    guard: Arc<Mutex<LeaseGuard>>,
    lost: Weak<DmlOperationAuthorityInner>,
}

#[async_trait]
impl DmlMutationAuthorityValidator for CurrentDmlLeaseFence {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        let guard = self.guard.lock().await;
        let result = guard.fence().validate_in(transaction).await;
        if result.is_err()
            && let Some(inner) = self.lost.upgrade()
        {
            inner.lost.store(true, Ordering::Release);
        }
        result.map_err(map_coordination_error)
    }
}

pub(crate) struct ActiveDmlOperation {
    pub(crate) journal: Arc<dyn OperationJournal>,
    pub(crate) stored: StoredOperation,
    authority: Option<DmlOperationAuthority>,
}

impl Drop for ActiveDmlOperation {
    fn drop(&mut self) {
        if let Some(authority) = &self.authority {
            authority.abandon();
        }
    }
}

impl ActiveDmlOperation {
    pub(crate) fn legacy(journal: Arc<dyn OperationJournal>, stored: StoredOperation) -> Self {
        Self {
            journal,
            stored,
            authority: None,
        }
    }

    pub(crate) fn operation_id(&self) -> DmlOperationId {
        self.stored.operation_id
    }

    pub(crate) fn check_before_dispatch(&self) -> Result<(), DmlError> {
        self.authority
            .as_ref()
            .map_or(Ok(()), DmlOperationAuthority::check_before_dispatch)
    }

    pub(crate) fn transition(
        &mut self,
        to: OperationState,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        self.check_before_dispatch()?;
        let recovery_due_at_ms =
            self.effective_recovery_due(to, &self.stored.payload, recovery_due_at_ms);
        if self.authority.is_none() {
            self.journal.transition(self.operation_id(), to)?;
            self.reload()?;
            return Ok(());
        }
        self.stored = self
            .journal
            .transition_authorized(
                self.operation_id(),
                self.stored.revision,
                Uuid::now_v7(),
                to,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn record_fact(
        &mut self,
        fact: OperationFact,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let payload = OperationPayload::ConnectorWriteLifecycle(fact.lifecycle.clone());
        let recovery_due_at_ms =
            self.effective_recovery_due(fact.state, &payload, recovery_due_at_ms);
        if self.authority.is_none() {
            self.journal.record_fact(self.operation_id(), fact)?;
            self.reload()?;
            return Ok(());
        }
        self.stored = self
            .journal
            .record_fact_authorized(
                self.operation_id(),
                self.stored.revision,
                Uuid::now_v7(),
                fact,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn mutate_statement(
        &mut self,
        state: OperationState,
        payload: OperationPayload,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let recovery_due_at_ms = self.effective_recovery_due(state, &payload, recovery_due_at_ms);
        if self.authority.is_none() {
            self.stored = self
                .journal
                .mutate_statement_operation(OperationMutationRequest {
                    operation_id: self.operation_id(),
                    expected_revision: self.stored.revision,
                    mutation_id: Uuid::now_v7(),
                    state,
                    payload,
                })?;
            return Ok(());
        }
        self.stored = self
            .journal
            .mutate_statement_operation_authorized(
                OperationMutationRequest {
                    operation_id: self.operation_id(),
                    expected_revision: self.stored.revision,
                    mutation_id: Uuid::now_v7(),
                    state,
                    payload,
                },
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn apply_add_files_mutation(
        &mut self,
        mut request: AddFilesMutationRequest,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let recovery_due_at_ms = self.effective_recovery_due(
            request.operation.state,
            &request.operation.payload,
            recovery_due_at_ms,
        );
        request.operation.expected_revision = self.stored.revision;
        request.operation.mutation_id = Uuid::now_v7();
        if self.authority.is_none() {
            self.stored = self.journal.apply_add_files_mutation(request)?;
            return Ok(());
        }
        self.stored = self
            .journal
            .apply_add_files_mutation_authorized(
                request,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn reschedule_recovery_due(
        &mut self,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        self.stored = self
            .journal
            .reschedule_recovery_due(
                DmlRecoveryDueRescheduleRequest {
                    operation_id: self.operation_id(),
                    expected_revision: self.stored.revision,
                    mutation_id: Uuid::now_v7(),
                    recovery_due_at_ms,
                },
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    pub(crate) fn release(&self) -> Result<(), DmlError> {
        self.authority
            .as_ref()
            .map_or(Ok(()), DmlOperationAuthority::release)
    }

    fn journal_authority(&self) -> Result<DmlMutationAuthority, DmlError> {
        self.authority
            .as_ref()
            .ok_or_else(|| {
                DmlError::coordination_unresolved("DML operation has no coordination authority")
            })?
            .journal_authority()
    }

    fn reload(&mut self) -> Result<(), DmlError> {
        self.stored = self.journal.load(self.operation_id())?.ok_or_else(|| {
            DmlError::journal_unresolved(format!(
                "DML operation {} cannot be read back after mutation",
                self.operation_id()
            ))
        })?;
        Ok(())
    }

    fn effective_recovery_due(
        &self,
        state: OperationState,
        payload: &OperationPayload,
        requested: Option<i64>,
    ) -> Option<i64> {
        if !operation_requires_recovery(state, payload) {
            return None;
        }
        requested.or(self.stored.recovery_due_at_ms).or_else(|| {
            Some(now_unix_millis().saturating_add(DML_FOREGROUND_RECOVERY_VISIBILITY_MS))
        })
    }
}

fn operation_requires_recovery(state: OperationState, payload: &OperationPayload) -> bool {
    if !state.is_finished() {
        return true;
    }
    match payload {
        OperationPayload::ConnectorWriteLifecycle(_) => false,
        OperationPayload::CtasSaga(record) => record.next_action != StatementNextAction::None,
        OperationPayload::TruncateLifecycle(record) => {
            record.next_action != StatementNextAction::None
        }
        OperationPayload::AddFilesLifecycle(record) => {
            record.next_action != StatementNextAction::None
        }
    }
}

fn map_coordination_error(error: CoordinationError) -> DmlError {
    match error.kind() {
        CoordinationErrorKind::WriteClosed | CoordinationErrorKind::NotBootstrapped => {
            DmlError::admission(error)
        }
        CoordinationErrorKind::FenceLost
        | CoordinationErrorKind::IncarnationChanged
        | CoordinationErrorKind::ClockUnsafe => DmlError::coordination_lost(error),
        CoordinationErrorKind::CommitUncertain
        | CoordinationErrorKind::OperationNotCommitted
        | CoordinationErrorKind::StoreUnavailable => DmlError::coordination_unresolved(error),
        CoordinationErrorKind::InvalidRequest
        | CoordinationErrorKind::LimitExceeded
        | CoordinationErrorKind::EpochExhausted
        | CoordinationErrorKind::IncarnationExhausted
        | CoordinationErrorKind::Corruption => DmlError::journal_corruption(error),
    }
}
