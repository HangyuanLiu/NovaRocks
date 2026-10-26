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

//! Original Provider custody borrowed by the existing CREATE product transition.

use super::{CurrentProjectionMetadataCpu, CurrentProjectionRegistration};
use crate::mv::domain::{
    application::{MvApplicationError, MvApplicationErrorKind, MvCreateProjectionSeed},
    iceberg_refresh::{IcebergMvCorePorts, IcebergMvCreatePlan},
};
use crate::query_execution::{
    maintenance::io_admission::MetadataIoAdmission,
    mv_assembly::{
        activation_cpu::{
            MvWriteActivationOriginal, MvWriteActivationOutput, MvWriteActivationRequest,
        },
        create_publication::{CreatePublicationInput, CreatePublicationOutput, EmptyCreateWrite},
        refresh_preparation::driver::{MvRefreshTurnCustody, MvTurnFailure},
    },
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_mv_application::{
    management::{
        CreateIntent, EffectDisposition, EffectIdentity, EffectScope, ManagedMvTarget,
        ManagementAdmissionError, ManagementEntrance, ManagementEntranceLease, ManagementRequest,
    },
    persistence::create_documents::MvCreateDocuments,
    product::{MvCreatePublicationOutcome, MvCreatedTarget, MvStagedTarget, MvTarget},
};
use novarocks_query_application::{
    api::{StatementExternalEffectDisposition, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmission, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorDocumentStorageLease, KeyedCreateIntent,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorControlPlanningLease, ConnectorError, ConnectorMutationFailure,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorStagedCreateAbortOutcome,
    ConnectorStagedCreateAbortRequest, ConnectorStagedCreateLease,
    ConnectorStagedCreateNeededCommitted, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePrepareRequest, ConnectorStagedCreatePublication,
    ConnectorStagedCreatePublicationPayload, ConnectorStagedCreatePublicationReceiving,
    ConnectorStagedCreatePublishOutcome, ConnectorStagedCreatePublishRequest,
    ConnectorStagedTableHandle, ConnectorTableIdentity, CreatePolicy, ExternalMutationEffect,
    ExternalMutationFinalization, LakePublicationId,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use std::{
    future::{Future, poll_fn},
    mem::ManuallyDrop,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

pub(crate) struct CreateOriginalInput {
    pub(crate) plan: Arc<IcebergMvCreatePlan>,
    pub(crate) seed: Arc<MvCreateProjectionSeed>,
    pub(crate) ports: IcebergMvCorePorts,
    pub(crate) planning: ConnectorControlPlanningLease,
    pub(crate) operation_id: uuid::Uuid,
    pub(crate) context: ConnectorRequestContext,
    pub(crate) cancellation: QueryCancellationView,
    pub(crate) io: Arc<MetadataIoAdmission>,
    pub(crate) cpu: MvRefreshTurnCustody,
    // The Action retains the strong same-Root StateStore owner independently.
    pub(crate) requests: Arc<dyn StateStoreExecutionRequests>,
}
#[derive(Clone, Debug)]
pub(crate) struct CreatePhaseFailure {
    pub(crate) service: QueryServiceError,
    pub(crate) domain: Option<MvApplicationError>,
}
impl From<MvApplicationError> for CreatePhaseFailure {
    fn from(domain: MvApplicationError) -> Self {
        let service = crate::query::product_command_service_error(
            crate::mv::command::mv_application_command_error(domain.clone()),
        );
        Self {
            service,
            domain: Some(domain),
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Prepare,
    Stage,
    Publish,
    Abort,
    Project,
    Register,
}
type OriginalFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
struct OriginalCall {
    future: Option<ManuallyDrop<OriginalFuture>>,
    phase: Option<Phase>,
    borrowed: bool,
    drop_started: bool,
    poisoned: bool,
}
impl Default for OriginalCall {
    fn default() -> Self {
        Self {
            future: None,
            phase: None,
            borrowed: false,
            drop_started: false,
            poisoned: false,
        }
    }
}
/// This header is installed in the Action before its first phase borrower.
/// Its Future owns only an Arc of State, which has no back-reference to this
/// header or Future. Unknown custody therefore creates no strong cycle.
pub(crate) struct CreateOriginal {
    state: Arc<State>,
    call: Mutex<OriginalCall>,
    health: StatementNativeHealthFailure,
    poisoned: AtomicBool,
    physical_exit: AtomicBool,
    physical_failure: Mutex<Option<StatementNativeHealthFailure>>,
    close_refusal: Mutex<Option<ConnectorError>>,
}
struct State {
    body: Mutex<Option<Box<Body>>>,
    response: Mutex<Option<Result<(), CreatePhaseFailure>>>,
    publication: ConnectorStagedCreatePublicationReceiving,
    stage_raw: Mutex<Option<Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>>>,
    abort_raw: Mutex<Option<Result<ConnectorStagedCreateAbortOutcome, ConnectorError>>>,
    current: CurrentProjectionRegistration,
}
struct Body {
    input: CreateOriginalInput,
    entrance: Arc<ManagementEntrance>,
    catalog: CatalogHandle,
    table: ConnectorTableIdentity,
    target: MvTarget,
    operation: ConnectorMutationOperationId,
    documents_lease: ConnectorDocumentStorageLease,
    staged_lease: ConnectorStagedCreateLease,
    intent: CreateIntent,
    management: Option<Result<ManagementEntranceLease, ManagementAdmissionError>>,
    admission: Option<ConnectorDocumentManagementAdmission>,
    request: Option<ConnectorStagedCreatePrepareRequest>,
    stage_result: Option<Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>>,
    handle: Option<ConnectorStagedTableHandle>,
    cpu_result: Option<Result<MvWriteActivationOutput, MvApplicationError>>,
    empty: EmptyCreateWrite,
    keyed: Option<KeyedCreateIntent>,
    documents: Option<MvCreateDocuments>,
    publication: Option<ConnectorStagedCreatePublication>,
    publication_result: Option<Result<ConnectorStagedCreatePublishOutcome, ConnectorError>>,
    abort_result: Option<Result<ConnectorStagedCreateAbortOutcome, ConnectorError>>,
    terminal: Option<Result<(), CreatePhaseFailure>>,
    project: Option<ProjectInput>,
    projected: bool,
}
struct ProjectInput {
    requests: Arc<dyn StateStoreExecutionRequests>,
    metadata_cpu: Arc<CurrentProjectionMetadataCpu>,
}
struct BodyLoan<'a> {
    state: &'a State,
    body: Option<Box<Body>>,
}
impl std::ops::Deref for BodyLoan<'_> {
    type Target = Body;
    fn deref(&self) -> &Body {
        self.body.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for BodyLoan<'_> {
    fn deref_mut(&mut self) -> &mut Body {
        self.body.as_mut().unwrap()
    }
}
impl Drop for BodyLoan<'_> {
    fn drop(&mut self) {
        let body = self.body.take().unwrap();
        let mut slot = self.state.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same original CREATE Body receiver");
        *slot = Some(body);
    }
}
impl State {
    fn try_loan(&self) -> Option<BodyLoan<'_>> {
        let body = self.body.lock().unwrap_or_else(|p| p.into_inner()).take()?;
        Some(BodyLoan {
            state: self,
            body: Some(body),
        })
    }
    fn loan(&self) -> BodyLoan<'_> {
        self.try_loan().expect("one original CREATE Body borrower")
    }
    async fn run(&self, phase: Phase) -> Result<(), CreatePhaseFailure> {
        let mut body = self.loan();
        match phase {
            Phase::Prepare => body.prepare().await,
            Phase::Stage => body.stage(self).await,
            Phase::Publish => body.publish(self).await,
            Phase::Abort => body.abort(self).await,
            Phase::Project => body.project(self).await,
            Phase::Register => body.register(),
        }
    }
}
/// No prepared handle, lease, session or business result travels in this Ready
/// value. Dropping a borrower leaves the same original Future in the header.
pub(crate) struct CreatePhaseFuture {
    source: Arc<CreateOriginal>,
}
impl Future for CreatePhaseFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.source.poll_progress(cx)
    }
}
struct CallLoan<'a> {
    source: &'a CreateOriginal,
    future: Option<ManuallyDrop<OriginalFuture>>,
    normal: bool,
}
impl Drop for CallLoan<'_> {
    fn drop(&mut self) {
        let mut call = self.source.call.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            call.future.is_none() && call.borrowed,
            "same original CREATE F receiver"
        );
        call.future = self.future.take();
        call.borrowed = false;
        if !self.normal {
            call.poisoned = true;
            self.source.poisoned.store(true, Ordering::Release)
        }
    }
}
struct CloseGuard<'a> {
    source: &'a CreateOriginal,
    normal: bool,
}
impl Drop for CloseGuard<'_> {
    fn drop(&mut self) {
        if !self.normal {
            self.source.poisoned.store(true, Ordering::Release)
        }
    }
}
#[derive(Debug)]
struct OriginalExitUnproven;
impl std::fmt::Display for OriginalExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original CREATE phase exit is unproven")
    }
}
impl std::error::Error for OriginalExitUnproven {}

impl CreateOriginal {
    pub(crate) fn new(mut input: CreateOriginalInput) -> Result<Arc<Self>, MvApplicationError> {
        let descriptor = input.planning.binding().descriptor();
        if descriptor.instance_id.as_str() != input.plan.target.catalog
            || descriptor.provider_id.as_str() != "iceberg"
        {
            return Err(invalid(
                "CREATE plan does not name the selected Iceberg generation",
            ));
        }
        let catalog = input
            .planning
            .binding()
            .catalog_handle()
            .map_err(provider)?
            .clone();
        let entrance = input.ports.management_entrance().map_err(invalid)?.clone();
        let table = ConnectorTableIdentity {
            instance_id: descriptor.instance_id.clone(),
            namespace: Arc::from(input.plan.target.namespace.as_str()),
            table: Arc::from(input.plan.target.table.as_str()),
        };
        let target = MvTarget::try_new(
            Some(input.plan.target.catalog.clone()),
            input.plan.target.namespace.clone(),
            input.plan.target.table.clone(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        let operation = ConnectorMutationOperationId::from_bytes(*input.operation_id.as_bytes());
        let intent = CreateIntent::try_new(
            catalog.clone(),
            table.clone(),
            EffectIdentity::from_bytes(*input.operation_id.as_bytes()),
        )
        .map_err(|e| invalid(format!("freeze CREATE intent: {e:?}")))?;
        let documents_lease = input
            .planning
            .derive_document_storage_lease()
            .map_err(provider)?;
        let staged_lease = input
            .planning
            .derive_staged_create_lease()
            .map_err(provider)?;
        input.context = input.context.with_io_admission(input.io.clone());
        // All rejection above precedes installing any management or Provider work.
        Ok(Arc::new(Self {
            state: Arc::new(State {
                body: Mutex::new(Some(Box::new(Body {
                    input,
                    entrance,
                    catalog,
                    table,
                    target,
                    operation,
                    documents_lease,
                    staged_lease,
                    intent,
                    management: None,
                    admission: None,
                    request: None,
                    stage_result: None,
                    handle: None,
                    cpu_result: None,
                    empty: EmptyCreateWrite::new(),
                    keyed: None,
                    documents: None,
                    publication: None,
                    publication_result: None,
                    abort_result: None,
                    terminal: None,
                    project: None,
                    projected: false,
                }))),
                response: Mutex::new(None),
                publication: ConnectorStagedCreatePublicationReceiving::default(),
                stage_raw: Mutex::new(None),
                abort_raw: Mutex::new(None),
                current: CurrentProjectionRegistration::new(),
            }),
            call: Mutex::new(OriginalCall::default()),
            poisoned: AtomicBool::new(false),
            physical_exit: AtomicBool::new(false),
            physical_failure: Mutex::new(None),
            close_refusal: Mutex::new(None),
            health: StatementNativeHealthFailure::actual_exit_unproven(
                "CREATE phase actual exit is unproven",
                Arc::new(OriginalExitUnproven),
            ),
        }))
    }
    fn phase(self: &Arc<Self>, phase: Phase) -> CreatePhaseFuture {
        assert!(
            !self.poisoned.load(Ordering::Acquire),
            "poisoned CREATE Source cannot start another phase"
        );
        assert!(
            self.state
                .response
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none(),
            "receive original CREATE response first"
        );
        let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            call.future.is_none() && !call.borrowed && call.phase.is_none(),
            "one CREATE phase borrower"
        );
        let state = self.state.clone();
        call.future = Some(ManuallyDrop::new(Box::pin(async move {
            let result = state.run(phase).await;
            *state.response.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
        })));
        call.phase = Some(phase);
        self.physical_exit.store(false, Ordering::Release);
        call.drop_started = false;
        CreatePhaseFuture {
            source: self.clone(),
        }
    }
    pub(crate) fn prepare(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Prepare)
    }
    pub(crate) fn stage(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Stage)
    }
    pub(crate) fn publish(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Publish)
    }
    pub(crate) fn abort(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Abort)
    }
    pub(crate) fn register(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Register)
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        requests: Arc<dyn StateStoreExecutionRequests>,
        metadata_cpu: Arc<CurrentProjectionMetadataCpu>,
    ) -> CreatePhaseFuture {
        {
            let mut body = self.state.loan();
            assert!(body.project.is_none(), "one CREATE Current invocation");
            body.project = Some(ProjectInput {
                requests,
                metadata_cpu,
            });
        }
        self.phase(Phase::Project)
    }
    pub(crate) fn poll_progress(&self, cx: &mut Context<'_>) -> Poll<()> {
        let future = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            if call.poisoned || call.borrowed || call.drop_started {
                return Poll::Pending;
            }
            let Some(future) = call.future.take() else {
                return Poll::Ready(());
            };
            call.borrowed = true;
            future
        };
        let mut loan = CallLoan {
            source: self,
            future: Some(future),
            normal: false,
        };
        match loan.future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                loan.normal = true;
                Poll::Pending
            }
            Poll::Ready(()) => {
                // State.run restored the Body and received its result before
                // this potentially foreign destructor. Never drop it twice.
                self.call
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .drop_started = true;
                unsafe { ManuallyDrop::drop(loan.future.as_mut().unwrap()) };
                loan.future.take();
                {
                    let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                    call.drop_started = false;
                    call.phase = None;
                }
                loan.normal = true;
                Poll::Ready(())
            }
        }
    }
    pub(crate) fn take_phase_result_in(
        &self,
        target: &mut Option<Result<(), CreatePhaseFailure>>,
    ) -> Result<(), MvApplicationError> {
        if target.is_some() {
            return Err(invalid("CREATE phase result target is occupied"));
        }
        let call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        if call.phase.is_some() || call.borrowed || call.poisoned {
            return Err(invalid("CREATE phase has not actually exited"));
        }
        drop(call);
        let mut slot = self
            .state
            .response
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            return Err(invalid("CREATE phase has no original response"));
        }
        *target = slot.take();
        Ok(())
    }
    /// The normal same-lease outcome retains exact evidence for its existing
    /// late-adjudication author. Reading it grants no new dispatch or deletion.
    pub(crate) fn stage_outcome(
        &self,
    ) -> Option<Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>> {
        self.state
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|body| body.stage_result.clone())
    }
    pub(crate) fn abort_outcome(
        &self,
    ) -> Option<Result<ConnectorStagedCreateAbortOutcome, ConnectorError>> {
        self.state
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|body| body.abort_result.clone())
    }
    pub(crate) fn staged_target(&self) -> Result<MvStagedTarget, MvApplicationError> {
        let body = self
            .state
            .try_loan()
            .ok_or_else(|| invalid("CREATE Body is still borrowed"))?;
        if body.handle.is_none() {
            return Err(invalid("CREATE has no exact staged target"));
        }
        Ok(MvStagedTarget {
            target: body.target.clone(),
            staged_operation_id: body.input.operation_id,
        })
    }
    pub(crate) fn created_target(&self) -> Result<Option<MvCreatedTarget>, MvApplicationError> {
        if self
            .state
            .publication
            .observation()
            .applied_receipt()
            .is_none()
        {
            return Ok(None);
        }
        let body = self
            .state
            .try_loan()
            .ok_or_else(|| invalid("CREATE Body is still borrowed"))?;
        let object = body
            .handle
            .as_ref()
            .and_then(|h| h.document_target())
            .ok_or_else(|| invalid("applied CREATE has no exact prepared target"))?
            .object_id()
            .clone();
        Ok(Some(MvCreatedTarget {
            target: body.target.clone(),
            object_id: object,
        }))
    }
    pub(crate) fn product_publication_outcome(
        &self,
    ) -> Result<Option<MvCreatePublicationOutcome>, MvApplicationError> {
        let observation = self.state.publication.observation();
        if observation.validation() != Some(Ok(())) {
            return Ok(None);
        }
        match observation.raw_outcome() {
            Some(Ok(ConnectorStagedCreatePublishOutcome::Applied { .. })) => self
                .created_target()
                .map(|created| created.map(MvCreatePublicationOutcome::Applied)),
            Some(Ok(ConnectorStagedCreatePublishOutcome::NoOp { .. })) => {
                Ok(Some(MvCreatePublicationOutcome::NoOp))
            }
            _ => Ok(None),
        }
    }
    pub(crate) fn needed_committed(&self) -> Option<ConnectorStagedCreateNeededCommitted> {
        self.state.publication.observation().needed_committed()
    }
    pub(crate) fn publish_outcome(
        &self,
    ) -> Option<Result<ConnectorStagedCreatePublishOutcome, ConnectorError>> {
        self.state.publication.observation().raw_outcome()
    }
    pub(crate) fn existing_finalization_context(
        &self,
    ) -> Option<Result<ConnectorRequestContext, ConnectorError>> {
        self.state.publication.observation().finalization_context()
    }
    pub(crate) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        if self
            .state
            .publication
            .observation()
            .applied_receipt()
            .is_some()
        {
            return Some(StatementExternalEffectDisposition::KnownCommitted);
        }
        if self
            .state
            .publication
            .observation()
            .confirmed_not_dispatched()
        {
            return Some(StatementExternalEffectDisposition::KnownUncommitted);
        }
        match self.publish_outcome() {
            Some(Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { .. })) => {
                Some(StatementExternalEffectDisposition::CommitUnknown)
            }
            Some(Ok(
                ConnectorStagedCreatePublishOutcome::Conflict { .. }
                | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. },
            )) => Some(StatementExternalEffectDisposition::KnownUncommitted),
            _ => None,
        }
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        if self.poisoned.load(Ordering::Acquire)
            || self.state.publication.observation().foreign_exit_unproven()
        {
            return Some(self.health.clone());
        }
        if let Some(health) = self
            .physical_failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .cloned()
        {
            return Some(health);
        }
        if let Some(health) = self.state.current.local_health_failure() {
            return Some(health);
        }
        let body = self.state.body.lock().unwrap_or_else(|p| p.into_inner());
        body.as_ref().and_then(|body| {
            body.empty
                .local_health_failure()
                .or_else(|| body.input.cpu.local_health_failure())
        })
    }
    pub(crate) fn physical_exit_observed(&self) -> bool {
        self.physical_exit.load(Ordering::Acquire)
    }
    /// Ordinary same-Map observation failure, distinct from actual-exit health.
    pub(crate) fn close_refusal(&self) -> Option<ConnectorError> {
        self.close_refusal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .cloned()
    }
    pub(crate) fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut foreign = CloseGuard {
            source: self,
            normal: false,
        };
        let result = self.poll_closed_original(cx);
        foreign.normal = true;
        result
    }
    fn poll_closed_original(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        if self.poll_progress(cx).is_pending() {
            return Poll::Pending;
        }
        let Some(mut body) = self.state.try_loan() else {
            return Poll::Pending;
        };
        if body.empty.poll_closed(cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(publication) = body.publication.as_mut() {
            if publication.poll_closed(cx).is_pending() {
                return Poll::Pending;
            }
        }
        if self
            .state
            .current
            .poll_registration_physical_closed(cx)
            .is_pending()
        {
            return Poll::Pending;
        }
        match body.input.cpu.poll_closed(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(health)) => {
                let rejected = {
                    let mut slot = self
                        .physical_failure
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if slot.is_none() {
                        *slot = Some(health);
                        None
                    } else {
                        Some(health)
                    }
                };
                drop(rejected);
                return Poll::Pending;
            }
            Poll::Pending => return Poll::Pending,
        }
        // Release actual CPU/gate/MIO resources even if the original Provider
        // graph remains Unknown. Retained graph/payload is a separate lifetime.
        match body.input.io.poll_actual_quiescent_continuing(cx) {
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(health)) => {
                let rejected = {
                    let mut slot = self
                        .physical_failure
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if slot.is_none() {
                        *slot = Some(health);
                        None
                    } else {
                        Some(health)
                    }
                };
                drop(rejected);
                return Poll::Pending;
            }
            Poll::Pending => return Poll::Pending,
        }
        self.physical_exit.store(true, Ordering::Release);
        // Current may retain its original catalog mutation inventory after its
        // physical children exit. It still belongs to this same Source.
        if self.state.current.poll_registration_closed(cx).is_pending() {
            return Poll::Pending;
        }
        // The exact same Map has received the aggregate before provider_exited,
        // but its generation can retire when the last lease disappears. Keep
        // THIS Source/lease in the existing Root residual until disposed; never
        // use current-generation presence as an independent owner or add a cycle.
        if let Some(handle) = body.handle.as_ref() {
            match body.staged_lease.cleanup_responsibility(handle) {
                Ok(novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed) => {
                    Poll::Ready(())
                }
                Ok(novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained) => {
                    Poll::Pending
                }
                Err(error) => {
                    let rejected = {
                        let mut slot = self.close_refusal.lock().unwrap_or_else(|p| p.into_inner());
                        if slot.is_none() {
                            *slot = Some(error);
                            None
                        } else {
                            Some(error)
                        }
                    };
                    drop(rejected);
                    Poll::Pending
                }
            }
        } else if matches!(
            body.stage_result.as_ref(),
            Some(Err(_))
                | Some(Ok(
                    ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. }
                        | ConnectorStagedCreatePrepareOutcome::AppliedButUnusable { .. }
                ))
        ) {
            // No exact handle exists yet. The actual stage author retained its
            // StageUnknown/StageAppliedUnusable row; absence grants no disposal.
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl Body {
    async fn prepare(&mut self) -> Result<(), CreatePhaseFailure> {
        if self.management.is_some() {
            return Err(invalid("CREATE management was already requested").into());
        }
        let entrance = self.entrance.clone();
        let cancellation = self.input.cancellation.clone();
        let mut wait = Box::pin(entrance.acquire_async(
            ManagementRequest::for_create_intent(
                self.intent.clone(),
                EffectScope::CATALOG_AND_OBJECT_DELETION,
            ),
            async move {
                let _ = cancellation.cancelled().await;
            },
        ));
        poll_fn(|cx| match wait.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.management = Some(result);
                Poll::Ready(())
            }
        })
        .await;
        drop(wait);
        self.management
            .as_ref()
            .unwrap()
            .as_ref()
            .map_err(|error| {
                MvApplicationError::new(
                    if matches!(
                        error,
                        ManagementAdmissionError::Cancelled | ManagementAdmissionError::Stopping
                    ) {
                        MvApplicationErrorKind::ShutdownCancelled
                    } else {
                        MvApplicationErrorKind::Engine
                    },
                    format!("admit CREATE: {error:?}"),
                )
            })?;
        self.admission = Some(
            self.documents_lease
                .admit_management(
                    ConnectorDocumentManagementAdmissionRequest::try_new(
                        self.documents_lease.owner().clone(),
                        self.catalog.clone(),
                        self.operation,
                        self.table.clone(),
                        None,
                        ConnectorDocumentManagementOperation::Create,
                        self.input.context.clone(),
                    )
                    .map_err(provider)?,
                )
                .map_err(provider)?,
        );
        Ok(())
    }
    async fn cpu(&mut self, input: CreatePublicationInput) -> Result<(), CreatePhaseFailure> {
        self.cpu_result = None;
        self.input
            .cpu
            .install_activation(MvWriteActivationOriginal::new(
                MvWriteActivationRequest::Create(input),
            ))
            .map_err(|_| invalid("CREATE CPU original receiving is occupied"))?;
        poll_fn(|cx| self.input.cpu.poll_ready(cx))
            .await
            .map_err(turn_failure)?;
        self.input
            .cpu
            .activation_original()
            .take_output_in(&mut self.cpu_result)
            .map_err(invalid)?;
        self.input.cpu.retire_transferred().map_err(invalid)?;
        match self.cpu_result.as_ref().unwrap() {
            Ok(_) => Ok(()),
            Err(error) => Err(error.clone().into()),
        }
    }
    async fn stage(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        if self.request.is_some() || self.stage_result.is_some() || self.handle.is_some() {
            return Err(invalid("CREATE stage was already requested").into());
        }
        self.cpu(CreatePublicationInput::StageDeclarations {
            plan: self.input.plan.clone(),
        })
        .await?;
        if !matches!(
            self.cpu_result.as_ref(),
            Some(Ok(MvWriteActivationOutput::Create(
                CreatePublicationOutput::StageDeclarations { .. }
            )))
        ) {
            return Err(invalid("CREATE declaration turn returned another output").into());
        }
        let Some(Ok(MvWriteActivationOutput::Create(CreatePublicationOutput::StageDeclarations {
            columns,
            partitioning,
            properties,
        }))) = self.cpu_result.take()
        else {
            unreachable!()
        };
        self.request = Some(
            self.staged_lease
                .prepare_document_managed_request(
                    LakePublicationId::try_from_uuid(self.input.operation_id)
                        .map_err(|e| invalid(e.to_string()))?,
                    self.operation,
                    self.table.clone(),
                    columns,
                    partitioning,
                    properties,
                    CreatePolicy::FailIfExists,
                    self.admission
                        .as_ref()
                        .ok_or_else(|| invalid("CREATE management admission is absent"))?
                        .clone(),
                    self.input.context.clone(),
                )
                .map_err(provider)?,
        );
        self.management_mut()?
            .mark_create_intent_dispatched(super::now_management_timestamp().map_err(invalid)?)
            .map_err(|e| invalid(format!("record CREATE intent: {e:?}")))?;
        let result = self
            .staged_lease
            .prepare_async(self.request.as_ref().unwrap().clone(), &state.stage_raw)
            .await;
        self.stage_result = Some(result);
        match self.stage_result.as_ref().unwrap() {
            Ok(ConnectorStagedCreatePrepareOutcome::Prepared {handle,..})=>{self.handle=Some(handle.clone());Ok(())},
            Ok(ConnectorStagedCreatePrepareOutcome::Conflict {failure}|ConnectorStagedCreatePrepareOutcome::KnownUncommitted {failure})=>{
                let error=mutation(MvApplicationErrorKind::Engine,failure);self.record_terminal(EffectDisposition::KnownUncommitted)?;Err(error.into())
            }
            Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {failure,..})=>Err(mutation(MvApplicationErrorKind::CommitUnknown,failure).into()),
            Ok(ConnectorStagedCreatePrepareOutcome::AppliedButUnusable {..})=>Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,"CREATE stage was applied but is unusable; its original responsibility is retained").into()),
            Err(error)=>Err(MvApplicationError::from_connector(MvApplicationErrorKind::CommitUnknown,error.clone()).into()),
        }
    }
    async fn publish(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        if self.publication.is_some() {
            return Err(invalid("CREATE publication was already requested").into());
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| invalid("CREATE has no staged target"))?
            .clone();
        self.cpu(CreatePublicationInput::Publication {
            plan: self.input.plan.clone(),
            seed: self.input.seed.clone(),
            staged: self.staged_lease.clone(),
            handle: handle.clone(),
            admission: self.admission.as_ref().unwrap().clone(),
            owner: self.entrance.owner().clone(),
            incarnation: self.entrance.incarnation().clone(),
            context: self.input.context.clone(),
        })
        .await?;
        if !matches!(
            self.cpu_result.as_ref(),
            Some(Ok(MvWriteActivationOutput::Create(
                CreatePublicationOutput::Publication { .. }
            )))
        ) {
            return Err(invalid("CREATE publication turn returned another output").into());
        }
        let Some(Ok(MvWriteActivationOutput::Create(CreatePublicationOutput::Publication {
            begin,
            intent,
            documents,
        }))) = self.cpu_result.take()
        else {
            unreachable!()
        };
        self.keyed = Some(intent);
        self.documents = Some(documents);
        let host = self
            .input
            .ports
            .typed_connector_control()
            .map_err(invalid)?
            .clone();
        if let Err(error) = self
            .empty
            .seal(
                &host,
                &self.input.planning,
                &self.staged_lease,
                &handle,
                begin,
            )
            .await
        {
            let kind = if self.empty.allows_staged_abort() {
                MvApplicationErrorKind::Engine
            } else {
                MvApplicationErrorKind::CommitUnknown
            };
            return Err(MvApplicationError::from_connector(kind, error).into());
        }
        let proof = self
            .empty
            .proof()
            .ok_or_else(|| invalid("CREATE empty write has no original proof"))?
            .clone();
        // The caller-owned receiving installs the strong Provider Source
        // before any original publish Future can be polled.
        self.publication = Some(
            self.staged_lease
                .begin_publish_original(
                    ConnectorStagedCreatePublishRequest {
                        operation_id: self.operation,
                        handle,
                        write: proof,
                        payload: ConnectorStagedCreatePublicationPayload::KeyedDocuments(
                            self.keyed.as_ref().unwrap().clone(),
                        ),
                        context: self.input.context.clone(),
                    },
                    &state.publication,
                )
                .map_err(provider)?,
        );
        let result = poll_fn(|cx| self.publication.as_mut().unwrap().poll_result(cx)).await;
        self.publication_result = Some(result);
        // Only these small immutable receipts/failures are cloned. All unique
        // stage/session/input/Provider custody remains in its original fields.
        match self.publication_result.as_ref().unwrap().clone() {
            Ok(ConnectorStagedCreatePublishOutcome::Applied { finalization, .. }) => {
                self.record_applied_terminal().map_err(committed_failure)?;
                match finalization {
                    ExternalMutationFinalization::Complete => Ok(()),
                    ExternalMutationFinalization::Failed(failure) => Err(mutation(
                        MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                        &failure,
                    )
                    .into()),
                }
            }
            // The product's explicit NoOp verdict decides Ack/FailIfExists.
            // It cannot borrow a created target, Current, or a delete right.
            Ok(ConnectorStagedCreatePublishOutcome::NoOp { finalization, .. }) => {
                self.record_terminal(EffectDisposition::KnownUncommitted)?;
                match finalization {
                    ExternalMutationFinalization::Complete => Ok(()),
                    ExternalMutationFinalization::Failed(failure) => {
                        Err(mutation(MvApplicationErrorKind::Engine, &failure).into())
                    }
                }
            }
            Ok(
                ConnectorStagedCreatePublishOutcome::Conflict { failure, .. }
                | ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure, .. },
            ) => Err(mutation(MvApplicationErrorKind::Engine, &failure).into()),
            Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, .. }) => {
                Err(mutation(MvApplicationErrorKind::CommitUnknown, &failure).into())
            }
            Err(error) => {
                let observation = state.publication.observation();
                let kind = if observation.applied_receipt().is_some() {
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed
                } else if observation.confirmed_not_dispatched() {
                    MvApplicationErrorKind::Engine
                } else {
                    MvApplicationErrorKind::CommitUnknown
                };
                Err(MvApplicationError::from_connector(kind, error).into())
            }
        }
    }
    fn management_mut(&mut self) -> Result<&mut ManagementEntranceLease, MvApplicationError> {
        self.management
            .as_mut()
            .and_then(|r| r.as_mut().ok())
            .ok_or_else(|| invalid("CREATE has no original management lease"))
    }
    fn record_terminal(
        &mut self,
        disposition: EffectDisposition,
    ) -> Result<(), CreatePhaseFailure> {
        if let Some(result) = &self.terminal {
            return result.clone();
        }
        let result = self
            .management_mut()?
            .record_terminal_in_place(disposition)
            .map_err(|e| {
                CreatePhaseFailure::from(invalid(format!("record CREATE terminal: {e:?}")))
            });
        self.terminal = Some(result.clone());
        result
    }
    fn record_applied_terminal(&mut self) -> Result<(), CreatePhaseFailure> {
        if self.terminal.is_some() {
            return self.terminal.as_ref().unwrap().clone();
        }
        let object = self
            .handle
            .as_ref()
            .and_then(|h| h.document_target())
            .ok_or_else(|| invalid("applied CREATE has no exact object"))?
            .object_id()
            .clone();
        let target = ManagedMvTarget::try_new(self.catalog.clone(), self.table.clone(), object)
            .map_err(|e| invalid(format!("bind CREATE actual target: {e:?}")))?;
        self.management_mut()?
            .late_bind_create_target(target)
            .map_err(|e| invalid(format!("late bind CREATE responsibility: {e:?}")))?;
        self.record_terminal(EffectDisposition::KnownCommitted)
    }
    async fn abort(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        if state.publication.observation().applied_receipt().is_some() {
            return Err(invalid("an applied CREATE cannot abort its original stage").into());
        }
        if self.abort_result.is_some() {
            return Err(invalid("CREATE abort was already requested").into());
        }
        match self.publication_result.as_ref() {
            Some(Ok(
                ConnectorStagedCreatePublishOutcome::Conflict { .. }
                | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. },
            )) => {}
            Some(Err(_)) if state.publication.observation().confirmed_not_dispatched() => {}
            None if self.publication.is_none() && self.empty.allows_staged_abort() => {}
            _ => {
                return Err(
                    invalid("CREATE stage has no original KnownUncommitted abort proof").into(),
                );
            }
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| invalid("CREATE abort has no stage"))?
            .clone();
        self.abort_result = Some(
            self.staged_lease
                .abort_async(
                    ConnectorStagedCreateAbortRequest {
                        operation_id: self.operation,
                        handle,
                        write: self.empty.proof().cloned(),
                        context: self.input.context.clone(),
                    },
                    &state.abort_raw,
                )
                .await,
        );
        match self.abort_result.as_ref().unwrap().clone() {
            Ok(ConnectorStagedCreateAbortOutcome::Aborted { finalization, .. }) => {
                self.record_terminal(EffectDisposition::KnownUncommitted)?;
                match finalization {
                    ExternalMutationFinalization::Complete => Ok(()),
                    ExternalMutationFinalization::Failed(failure) => {
                        Err(mutation(MvApplicationErrorKind::Engine, &failure).into())
                    }
                }
            }
            Ok(ConnectorStagedCreateAbortOutcome::KnownUncommitted { failure }) => {
                Err(mutation(MvApplicationErrorKind::Engine, &failure).into())
            }
            Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown { failure, .. }) => {
                Err(mutation(MvApplicationErrorKind::CommitUnknown, &failure).into())
            }
            Err(error) => Err(provider(error.clone()).into()),
        }
    }
    async fn project(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        let needed = state
            .publication
            .observation()
            .needed_committed()
            .ok_or_else(|| invalid("CREATE Current requires actual Needed"))?;
        if needed.effect != ExternalMutationEffect::Applied
            || state.publication.observation().applied_receipt().is_none()
        {
            return Err(invalid("CREATE NoOp or Unknown cannot install Current").into());
        }
        // Management terminal precedes Current. A failed Current may not
        // leave a committed create masquerading as unsettled/uncommitted.
        self.record_applied_terminal().map_err(committed_failure)?;
        let context = state
            .publication
            .observation()
            .finalization_context()
            .ok_or_else(|| invalid("CREATE has no original finite finalization Context"))?
            .map_err(provider)?;
        let project = self
            .project
            .as_ref()
            .ok_or_else(|| invalid("CREATE Current has no existing-obligation borrower"))?;
        let view = self
            .input
            .ports
            .readiness()
            .in_requests(project.requests.clone());
        let result = super::install_created_current_projection_async_in(
            &self.entrance,
            &view,
            self.input.ports.connector_control(),
            self.catalog.clone(),
            self.target.clone(),
            self.input.operation_id,
            context,
            &project.metadata_cpu,
            &state.current,
        )
        .await;
        match result {
            Ok(()) => {
                self.projected = true;
                Ok(())
            }
            Err(error) => {
                let mut failure = MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    error.to_string(),
                );
                if let Some(cause) = error.original_failure() {
                    failure = failure.with_original_failure(cause.clone())
                }
                Err(failure.into())
            }
        }
    }
    fn register(&self) -> Result<(), CreatePhaseFailure> {
        if !self.projected {
            return Err(
                invalid("CREATE registration precedes its actual Current installation").into(),
            );
        }
        // The existing registration author only confirms that the catalog's
        // current generation is published; it does not mutate a table cache.
        let instance =
            novarocks_spi::connector::ConnectorInstanceId::parse(&self.input.plan.target.catalog)
                .map_err(|e| invalid(e.to_string()))?;
        novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            self.input.ports.connector_control(),
            &instance,
        )
        .map(|_| ())
        .map_err(|e| provider(e).into())
    }
}
fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}
fn provider(error: ConnectorError) -> MvApplicationError {
    MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
}
fn mutation(
    kind: MvApplicationErrorKind,
    failure: &ConnectorMutationFailure,
) -> MvApplicationError {
    if let Some(error) = failure.connector_source() {
        MvApplicationError::from_connector(kind, error.clone())
    } else {
        let cause = SharedFailureCause::new(failure.clone());
        MvApplicationError::new(kind, cause.detail()).with_original_failure(cause)
    }
}
fn turn_failure(error: MvTurnFailure) -> CreatePhaseFailure {
    match error {
        MvTurnFailure::Admission(error) => {
            let cause = SharedFailureCause::new(error);
            CreatePhaseFailure {
                service: QueryServiceError::from_failure_cause(cause),
                domain: None,
            }
        }
        MvTurnFailure::Cpu(error) => CreatePhaseFailure {
            service: QueryServiceError::from_execution_failure(error),
            domain: None,
        },
        MvTurnFailure::Health(_) => CreatePhaseFailure {
            service: QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "CREATE CPU original exit is unproven",
            ),
            domain: None,
        },
    }
}

fn committed_failure(error: CreatePhaseFailure) -> CreatePhaseFailure {
    let Some(domain) = error.domain else {
        return error;
    };
    let mut known = MvApplicationError::new(
        MvApplicationErrorKind::KnownCommittedFinalizeFailed,
        domain.message(),
    )
    .with_compile_control(domain.compile_control_error())
    .with_unavailable_implementation(domain.unavailable_implementation().cloned());
    if let Some(cause) = domain.original_failure() {
        known = known.with_original_failure(cause.clone())
    }
    known.into()
}

#[cfg(test)]
mod original_call_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    // Exercise the actual header/loan/Drop author only. This fixture has no
    // Body and grants no Root, Provider, CPU, management or publication rights.
    struct Probe {
        state: Arc<State>,
        ready: Arc<AtomicBool>,
        dropped: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }
    impl Future for Probe {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if !self.ready.load(Ordering::Acquire) {
                return Poll::Pending;
            }
            *self.state.response.lock().unwrap() =
                Some(Err(invalid("original probe result").into()));
            Poll::Ready(())
        }
    }
    #[derive(Debug)]
    struct ActualDropPanic;
    impl Drop for Probe {
        fn drop(&mut self) {
            assert!(
                self.state.response.lock().unwrap().is_some(),
                "actual result precedes original Drop"
            );
            self.dropped.fetch_add(1, Ordering::SeqCst);
            if self.panic_on_drop {
                std::panic::panic_any(ActualDropPanic)
            }
        }
    }
    fn fixture(panic_on_drop: bool) -> (Arc<CreateOriginal>, Arc<AtomicBool>, Arc<AtomicUsize>) {
        let state = Arc::new(State {
            body: Mutex::new(None),
            response: Mutex::new(None),
            publication: ConnectorStagedCreatePublicationReceiving::default(),
            stage_raw: Mutex::new(None),
            abort_raw: Mutex::new(None),
            current: CurrentProjectionRegistration::new(),
        });
        let ready = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        let future: OriginalFuture = Box::pin(Probe {
            state: state.clone(),
            ready: ready.clone(),
            dropped: dropped.clone(),
            panic_on_drop,
        });
        (
            Arc::new(CreateOriginal {
                state,
                call: Mutex::new(OriginalCall {
                    future: Some(ManuallyDrop::new(future)),
                    phase: Some(Phase::Prepare),
                    ..Default::default()
                }),
                health: StatementNativeHealthFailure::actual_exit_unproven(
                    "probe original exit unproven",
                    Arc::new(OriginalExitUnproven),
                ),
                poisoned: AtomicBool::new(false),
                physical_exit: AtomicBool::new(false),
                physical_failure: Mutex::new(None),
                close_refusal: Mutex::new(None),
            }),
            ready,
            dropped,
        )
    }
    #[test]
    fn dropping_phase_borrower_keeps_original_input_until_normal_actual_drop() {
        let (source, ready, dropped) = fixture(false);
        let mut borrower = CreatePhaseFuture {
            source: source.clone(),
        };
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(Pin::new(&mut borrower).poll(&mut cx).is_pending());
        drop(borrower);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert!(source.state.response.lock().unwrap().is_none());
        ready.store(true, Ordering::Release);
        assert!(source.poll_progress(&mut cx).is_ready());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let mut received = None;
        source.take_phase_result_in(&mut received).unwrap();
        assert!(received.unwrap().is_err());
        assert!(!source.poisoned.load(Ordering::Acquire));
        assert!(source.poll_progress(&mut cx).is_ready());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn original_drop_panic_keeps_received_cause_and_prevents_repoll_or_second_drop() {
        let (source, ready, dropped) = fixture(true);
        ready.store(true, Ordering::Release);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            source.poll_progress(&mut cx)
        }))
        .unwrap_err();
        assert!(panic.is::<ActualDropPanic>());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(
            source
                .state
                .response
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_err()
        );
        assert!(source.local_health_failure().is_some());
        assert!(source.poll_closed(&mut cx).is_pending());
        assert!(source.poll_progress(&mut cx).is_pending());
        let mut received = None;
        assert!(source.take_phase_result_in(&mut received).is_err());
        assert!(received.is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}
