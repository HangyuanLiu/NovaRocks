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

//! One original MV step retains each concrete phase and the product transition.

mod source;

use super::{CommandInput, MvPhaseFailure, MvWake, StepRequest, native::MvNativePhase, service};
use crate::query::statement_publication::StatementPublication;
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use crate::state_store::execution::StateStoreOriginalOwner;
use novarocks_mv_application::{
    management::EffectDisposition,
    ports::{MvProviderFailure, MvProviderFailureKind, MvRefreshProgressPort},
    product::{MvProductError, MvRefreshAttemptIdentity, MvTarget},
    publication::MvRefreshPublicationFinalizationFacts,
    service::MvRefreshTransition,
};
use novarocks_query_application::{
    api::{StatementExternalEffectDisposition, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_control::GovernedActionContext,
    session_error::QueryServiceError,
};
use novarocks_spi::connector::{
    ConnectorRequestContext, LakePublicationDisposition, LakePublicationFamily,
    LakePublicationTarget,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use source::{PhaseFuture, StepPreparedKind, StepSource};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

/// Both step kinds borrow the same command Root and stable retired slots.
pub(super) enum MvStepOriginal {
    Refresh(MvRefreshStepOriginal),
    Control(super::control::MvControlStepOriginal),
    Create(super::control_create::MvCreateStepOriginal),
}
impl MvStepOriginal {
    pub(super) fn new(
        input: Arc<CommandInput>,
        io: Arc<MetadataIoAdmission>,
        store: &StateStoreOriginalOwner,
        request: StepRequest,
        context: ConnectorRequestContext,
        wake: Arc<MvWake>,
    ) -> Result<Self, QueryServiceError> {
        match request {
            StepRequest::Create { target, statement } => {
                super::control_create::MvCreateStepOriginal::new(
                    input, io, store, target, statement, context, wake,
                )
                .map(Self::Create)
            }
            StepRequest::Control(request) => {
                super::control::MvControlStepOriginal::new(input, io, store, request, context, wake)
                    .map(Self::Control)
            }
            refresh => MvRefreshStepOriginal::new(input, io, store, refresh, context, wake)
                .map(Self::Refresh),
        }
    }
    pub(super) fn physical_exit_observed(&self) -> bool {
        match self {
            Self::Refresh(step) => step.execution.closed,
            Self::Control(step) => step.physical_exit_observed(),
            Self::Create(step) => step.physical_exit_observed(),
        }
    }
    pub(super) fn performed_publication(&self) -> bool {
        match self {
            Self::Refresh(step) => step.performed_publication(),
            Self::Control(step) => step.performed_publication(),
            Self::Create(step) => step.performed_publication(),
        }
    }
    pub(super) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        match self {
            Self::Refresh(step) => step.external_effect_disposition(),
            Self::Control(step) => step.external_effect_disposition(),
            Self::Create(step) => step.external_effect_disposition(),
        }
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        match self {
            Self::Refresh(step) => step.local_health_failure(),
            Self::Control(step) => step.local_health_failure(),
            Self::Create(step) => step.local_health_failure(),
        }
    }
    pub(super) fn poll_business(
        &mut self,
        store: &StateStoreOriginalOwner,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        match self {
            Self::Refresh(step) => step.poll_business(store, owner, cx),
            Self::Control(step) => step.poll_business(store, owner, cx),
            Self::Create(step) => step.poll_business(store, owner, cx),
        }
    }
    pub(super) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        match self {
            Self::Refresh(step) => step.poll_closed(cx),
            Self::Control(step) => step.poll_closed(cx),
            Self::Create(step) => step.poll_closed(cx),
        }
    }
}

pub(super) struct MvRefreshStepOriginal {
    input: Arc<CommandInput>,
    request: Option<StepRequest>,
    attempt: MvRefreshAttemptIdentity,
    io: Arc<MetadataIoAdmission>,
    requests: Arc<dyn StateStoreExecutionRequests>,
    context: ConnectorRequestContext,
    wake: Arc<MvWake>,
    transition: Option<MvRefreshTransition>,
    execution: StepExecution,
    prepared: bool,
    prepared_kind: Option<StepPreparedKind>,
    selected: Option<Result<(), MvPhaseFailure>>,
}

#[derive(Clone, Copy)]
enum Phase {
    Activate,
    Native,
    PreCommit,
    Publish,
    Received,
}

/// The product only borrows this concrete original. Its raw publication Call,
/// Source, Native observer and received outcomes never enter a polling Future.
struct StepExecution {
    root: novarocks_workload_control::WorkScope,
    source: Option<Arc<StepSource>>,
    future: Option<PhaseFuture>,
    phase_result: Option<Result<(), MvPhaseFailure>>,
    future_busy: bool,
    retained_health: StatementNativeHealthFailure,
    native: Option<MvNativePhase>,
    publication: Option<StatementPublication>,
    call_context: Option<ConnectorRequestContext>,
    published: Option<Result<MvRefreshPublicationFinalizationFacts, MvProviderFailure>>,
    projected: Option<Result<(), MvProviderFailure>>,
    failure: Option<MvPhaseFailure>,
    phase: Phase,
    projection_started: bool,
    closing_started: bool,
    terminal_recorded: bool,
    terminal_record_failure: Option<QueryServiceError>,
    closed: bool,
}
impl MvRefreshStepOriginal {
    pub(super) fn new(
        input: Arc<CommandInput>,
        io: Arc<MetadataIoAdmission>,
        store: &StateStoreOriginalOwner,
        request: StepRequest,
        context: ConnectorRequestContext,
        wake: Arc<MvWake>,
    ) -> Result<Self, QueryServiceError> {
        if let StepRequest::Unsupported { display_name } = &request {
            return Err(service(format!(
                "REFRESH MATERIALIZED VIEW is only supported for Iceberg-backed materialized views: {}",
                display_name.trim_start_matches("mv:")
            )));
        }
        // All concrete phases, including the original publication Call,
        // borrow this same already-positive MIO through its weak port.
        let context = context.with_io_admission(io.clone());
        let requests = Arc::new(
            store
                .execution_requests(
                    input.state_store.transaction_deadline,
                    Some(context.deadline()),
                )
                .map_err(QueryServiceError::from_failure_cause)?,
        );
        let root = io
            .root_capacity()
            .map_err(super::original_error)?
            .root_scope()
            .clone();
        let attempt = input.product.reserve_refresh_attempt();
        Ok(Self {
            input,
            request: Some(request),
            attempt,
            io,
            requests,
            context,
            wake,
            transition: None,
            prepared: false,
            prepared_kind: None,
            selected: None,
            execution: StepExecution {
                root,
                source: None,
                future: None,
                phase_result: None,
                future_busy: false,
                retained_health: StatementNativeHealthFailure::actual_exit_unproven(
                    "MV original phase poll or destructor did not return normally",
                    Arc::new(super::MvActionExitUnproven),
                ),
                native: None,
                publication: None,
                call_context: None,
                published: None,
                projected: None,
                failure: None,
                phase: Phase::Activate,
                projection_started: false,
                closing_started: false,
                terminal_recorded: false,
                terminal_record_failure: None,
                closed: false,
            },
        })
    }
    pub(super) fn performed_publication(&self) -> bool {
        self.selected.as_ref().is_some_and(Result::is_ok)
            && matches!(
                self.prepared_kind,
                Some(StepPreparedKind::Data | StepPreparedKind::MetadataOnly)
            )
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.execution.local_health_failure()
    }
    pub(super) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        let source = self
            .execution
            .source
            .as_ref()
            .and_then(|source| source.external_effect_disposition());
        let publication =
            self.execution
                .publication
                .as_ref()
                .and_then(|p| match p.received_committed_effect() {
                    Some(novarocks_spi::connector::ExternalMutationEffect::Applied) => {
                        Some(StatementExternalEffectDisposition::KnownCommitted)
                    }
                    Some(novarocks_spi::connector::ExternalMutationEffect::NoOp) => None,
                    None => match p.disposition() {
                        Some(LakePublicationDisposition::KnownUncommitted) => {
                            Some(StatementExternalEffectDisposition::KnownUncommitted)
                        }
                        Some(LakePublicationDisposition::CommitUnknown) => {
                            Some(StatementExternalEffectDisposition::CommitUnknown)
                        }
                        _ => None,
                    },
                });
        super::fold_effect(source, publication)
    }
    fn select(
        &mut self,
        mut result: Result<(), MvPhaseFailure>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        if let Some(publication) = &mut self.execution.publication
            && publication.disposition().is_some()
        {
            if let Err(error) = &result
                && publication.committed()
                && publication.local_finalization.is_none()
            {
                // The original typed primary remains in result. This
                // sidecar records only the necessary-finalization verdict.
                publication.local_finalization = Some(Err(error.service.to_string()));
            }
            if let Err(error) = publication.freeze_terminal()
                && result.is_ok()
            {
                result = Err(error.into());
            }
            // A Provider NoOp is not proof that this MV's declared P was
            // published. Keep its canonical raw fact, without rendering a
            // false MV KnownCommitted/do-not-retry marker to the client.
            if publication.received_committed_effect()
                != Some(novarocks_spi::connector::ExternalMutationEffect::NoOp)
                && let Some(terminal) = publication.attempt.terminal()
            {
                result = result.map_err(|mut failure| {
                    failure.service = failure
                        .service
                        .attach_publication_terminal(terminal.clone());
                    failure
                });
            }
        }
        if let Some(source) = &self.execution.source {
            // The logical per-target activity ends here. Its independent GC,
            // actual Native receipt and unknown publication stay in this Source.
            if let Err(error) = source.release_business_activity()
                && result.is_ok()
            {
                result = Err(error.into());
            }
        }
        self.selected = Some(result.clone());
        Poll::Ready(result)
    }
    pub(super) fn poll_business(
        &mut self,
        store: &StateStoreOriginalOwner,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MvPhaseFailure>> {
        self.execution.poll_live_tails(cx);
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(result) = &self.selected {
            return Poll::Ready(result.clone());
        }
        if self.execution.source.is_none() {
            let cancellation =
                QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
            let original = self
                .io
                .original_result_scope(&self.context)
                .map_err(super::original_error)?;
            let source = StepSource::new(
                self.input.clone(),
                self.request.take().expect("one original MV step request"),
                self.attempt.clone(),
                self.context.clone(),
                self.io.clone(),
                self.requests.clone(),
                cancellation,
                self.wake.clone(),
                original,
            )?;
            // Source is installed before the first borrowed Future is created.
            self.execution.source = Some(source.clone());
            self.execution.future = Some(source.prepare());
        }
        if !self.prepared {
            match self.execution.poll_phase(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return self.select(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let source = self.execution.source.as_ref().unwrap();
            self.prepared_kind = source.prepared_kind()?;
            if matches!(self.prepared_kind, Some(StepPreparedKind::NoOp)) {
                let cancellation = QueryCancellationView::governed(
                    owner.cancellation().clone(),
                    owner.timeout_ms(),
                );
                return self.select(
                    cancellation
                        .reason()
                        .map(crate::query::cancellation_error)
                        .map_or(Ok(()), |error| Err(error.into())),
                );
            }
            let (target, attempt) = source
                .refresh_identity()?
                .ok_or_else(|| service("MV prepared publication has no original identity"))?;
            if attempt != self.attempt {
                return self.select(Err(
                    service("MV preparation replaced its original attempt").into()
                ));
            }
            self.transition = Some(MvRefreshTransition::new(target, attempt));
            self.prepared = true;
        }
        let mut port = StepPort {
            execution: &mut self.execution,
            input: &self.input,
            io: &self.io,
            store,
            context: &self.context,
            owner,
        };
        if self
            .input
            .product
            .poll_refresh(cx, self.transition.as_mut().unwrap(), &mut port)
            .is_pending()
        {
            return Poll::Pending;
        }
        let product_result = self
            .transition
            .as_ref()
            .unwrap()
            .result()
            .expect("ready original product result");
        let result = match product_result {
            Ok(_) => Ok(()),
            Err(error) => Err(self
                .execution
                .failure
                .clone()
                .unwrap_or_else(|| product_failure(error.clone()))),
        };
        self.select(result)
    }
    pub(super) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.execution.poll_closed(&self.context, cx)
    }
}
impl StepExecution {
    fn poll_live_tails(&mut self, cx: &mut Context<'_>) {
        // A received business fact permits necessary Current work while its
        // original Call continues cleanup. Neither tail can gate that work.
        if let Some(publication) = self.publication.as_mut()
            && publication.committed_needed().is_some()
        {
            let _ = publication.poll_retirement(cx);
        }
        if let Some(native) = self.native.as_mut() {
            native.poll_ready_tail(cx);
        }
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.future_busy
            .then(|| self.retained_health.clone())
            .or_else(|| self.source.as_ref().and_then(|s| s.local_health_failure()))
            .or_else(|| {
                self.native
                    .as_ref()
                    .and_then(MvNativePhase::local_health_failure)
            })
            .or_else(|| {
                self.publication
                    .as_ref()
                    .and_then(StatementPublication::local_health_failure)
            })
    }
    fn poll_phase(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), MvPhaseFailure>> {
        if self.future_busy {
            return Poll::Pending;
        }
        if self.future.is_none() {
            return Poll::Ready(Err(
                service("MV original phase has no installed Future").into()
            ));
        }
        self.future_busy = true;
        if self.future.as_mut().unwrap().as_mut().poll(cx).is_pending() {
            self.future_busy = false;
            return Poll::Pending;
        }
        // Receive before foreign Drop. An unwind leaves the exact received
        // result and all unique Source fields in this same original step.
        if let Err(error) = self
            .source
            .as_ref()
            .unwrap()
            .take_phase_result_in(&mut self.phase_result)
        {
            self.phase_result = Some(Err(error.into()));
        }
        drop(self.future.take());
        self.future_busy = false;
        Poll::Ready(self.phase_result.take().expect("original phase response"))
    }
    fn fail(&mut self, error: impl Into<MvPhaseFailure>) {
        self.failure.get_or_insert(error.into());
    }
    fn reject_execution(&mut self, error: impl Into<MvPhaseFailure>) {
        let error = error.into();
        if let Some(source) = &self.source
            && let Ok(context) = source.publication_context()
        {
            self.call_context = Some(context);
        }
        self.fail(error.clone());
        self.published = Some(Err(provider_error(
            error.service,
            self.publication.as_ref(),
        )));
    }
    fn poll_closed(
        &mut self,
        _context: &ConnectorRequestContext,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        // Every independent receipt advances on every turn. Remote Native
        // closure cannot prevent the already-created Call's GC from polling.
        let mut pending = false;
        if let Some(publication) = self.publication.as_mut() {
            pending |= publication.poll_retirement(cx).is_pending();
        }
        let native_closed = match self.native.as_mut() {
            Some(native) => match native.poll_closed(cx) {
                Poll::Pending => {
                    pending = true;
                    false
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => true,
            },
            None => true,
        };
        // Failed Native work must actually exit before revoking/cleaning its
        // write session. The UI-selected failure is not a writer-close proof.
        if self.failure.is_some()
            && native_closed
            && let Some(source) = &self.source
            && let Some(session) = source.session().map_err(|_| self.retained_health.clone())?
        {
            if self.publication.is_none() {
                // The identity is created from the same original target
                // before any provider call; no target is reconstructed.
                if let Some((target, attempt)) = source
                    .refresh_identity()
                    .map_err(|_| self.retained_health.clone())?
                {
                    self.publication = Some(
                        new_publication(&target, &attempt)
                            .map_err(|_| self.retained_health.clone())?,
                    );
                }
            }
            if let Some(publication) = &mut self.publication {
                if self.call_context.is_none() {
                    self.call_context = Some(
                        source
                            .publication_context()
                            .map_err(|_| self.retained_health.clone())?,
                    );
                }
                if publication
                    .poll_settle(
                        Some(&session),
                        self.call_context.as_ref().unwrap(),
                        true,
                        cx,
                    )
                    .is_pending()
                {
                    pending = true;
                }
                pending |= publication.poll_retirement(cx).is_pending();
            }
        }
        if self.future.is_some() && self.poll_phase(cx).is_pending() {
            pending = true;
        }
        if !pending
            && !self.closing_started
            && self.future.is_none()
            && let Some(source) = &self.source
        {
            self.future = Some(source.close());
            self.closing_started = true;
            if self.poll_phase(cx).is_pending() {
                pending = true;
            }
        }
        if let Some(source) = &self.source {
            match source.poll_closed(cx) {
                Poll::Pending => pending = true,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
        }
        if !pending && !self.terminal_recorded {
            let terminal = self.publication.as_ref().and_then(|publication| {
                if publication.received_committed_effect()
                    == Some(novarocks_spi::connector::ExternalMutationEffect::NoOp)
                {
                    // No new MV publication was applied. This does not rewrite
                    // the Provider's canonical NoOp disposition.
                    Some(EffectDisposition::KnownUncommitted)
                } else {
                    publication
                        .disposition()
                        .and_then(|disposition| match disposition {
                            LakePublicationDisposition::KnownCommitted => {
                                Some(EffectDisposition::KnownCommitted)
                            }
                            LakePublicationDisposition::KnownUncommitted => {
                                Some(EffectDisposition::KnownUncommitted)
                            }
                            LakePublicationDisposition::CommitUnknown => None,
                        })
                }
            });
            if let (Some(source), Some(terminal)) = (&self.source, terminal) {
                // All original CPU borrowers have now actually exited. A
                // projection failure cannot rewrite an Applied commit as Unknown.
                match source.record_terminal(terminal) {
                    Ok(()) => self.terminal_recorded = true,
                    Err(error) => {
                        self.terminal_record_failure.get_or_insert(error);
                        return Poll::Pending;
                    }
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            self.closed = true;
            Poll::Ready(Ok(()))
        }
    }
}

struct StepPort<'a, 'owner> {
    execution: &'a mut StepExecution,
    input: &'a Arc<CommandInput>,
    io: &'a Arc<MetadataIoAdmission>,
    store: &'a StateStoreOriginalOwner,
    context: &'a ConnectorRequestContext,
    owner: &'a mut GovernedActionContext<'owner>,
}
impl MvRefreshProgressPort for StepPort<'_, '_> {
    fn poll_execute_refresh(
        &mut self,
        cx: &mut Context<'_>,
        _target: &MvTarget,
        _attempt: &MvRefreshAttemptIdentity,
    ) -> Poll<()> {
        if self.execution.published.is_some() {
            return Poll::Ready(());
        }
        loop {
            let source = self.execution.source.as_ref().unwrap().clone();
            match self.execution.phase {
                Phase::Activate => {
                    if self.execution.future.is_none() {
                        self.execution.future = Some(source.activate());
                    }
                    match self.execution.poll_phase(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                        Poll::Ready(Ok(())) => {}
                    }
                    match source.publication_context() {
                        Ok(context) => self.execution.call_context = Some(context),
                        Err(error) => {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                    }
                    let kind = match source.prepared_kind() {
                        Ok(Some(kind)) => kind,
                        Ok(None) => {
                            self.execution.reject_execution(service(
                                "MV activation lost its prepared work kind",
                            ));
                            return Poll::Ready(());
                        }
                        Err(error) => {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                    };
                    if matches!(kind, StepPreparedKind::Data) {
                        let mut native = None;
                        if let Err(error) = source.take_native_in(&mut native) {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                        let Some((initial, assembly)) = native else {
                            self.execution.reject_execution(service(
                                "MV data activation has no original assembly",
                            ));
                            return Poll::Ready(());
                        };
                        self.execution.native = Some(MvNativePhase::new(
                            self.execution.root.clone(),
                            initial,
                            self.input.services.launcher.clone(),
                            assembly,
                            #[cfg(novarocks_dev_controls)]
                            None,
                        ));
                        self.execution.phase = Phase::Native;
                    } else if matches!(kind, StepPreparedKind::MetadataOnly) {
                        self.execution.phase = Phase::PreCommit;
                    } else {
                        self.execution
                            .reject_execution(service("MV NoOp entered provider activation"));
                        return Poll::Ready(());
                    }
                }
                Phase::Native => {
                    match self.execution.native.as_mut().unwrap().poll_ready(
                        &(
                            self.input.services.blocking.clone(),
                            self.input.services.data.clone(),
                        ),
                        self.io,
                        self.owner,
                        cx,
                    ) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                        Poll::Ready(Ok(())) => self.execution.phase = Phase::PreCommit,
                    }
                }
                Phase::PreCommit => {
                    if self.execution.future.is_none() {
                        self.execution.future = Some(source.precommit());
                    }
                    match self.execution.poll_phase(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                        Poll::Ready(Ok(())) => {}
                    }
                    if let Err(error) = source.mark_dispatched() {
                        self.execution.reject_execution(error);
                        return Poll::Ready(());
                    }
                    let publication = source.refresh_identity().and_then(|identity| {
                        let (target, attempt) = identity
                            .ok_or_else(|| service("MV publication lost its original identity"))?;
                        new_publication(&target, &attempt)
                    });
                    match publication {
                        Ok(publication) => self.execution.publication = Some(publication),
                        Err(error) => {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                    }
                    self.execution.phase = Phase::Publish;
                }
                Phase::Publish => {
                    let session = match source.session() {
                        Ok(Some(session)) => session,
                        Ok(None) => {
                            self.execution.reject_execution(service(
                                "MV publication has no received session",
                            ));
                            return Poll::Ready(());
                        }
                        Err(error) => {
                            self.execution.reject_execution(error);
                            return Poll::Ready(());
                        }
                    };
                    let publication = self.execution.publication.as_mut().unwrap();
                    if publication
                        .poll_settle(
                            Some(&session),
                            self.execution
                                .call_context
                                .as_ref()
                                .expect("original MV begin context"),
                            false,
                            cx,
                        )
                        .is_pending()
                    {
                        return Poll::Pending;
                    }
                    if let Err(error) = publication.select(None) {
                        self.execution.reject_execution(error);
                        return Poll::Ready(());
                    }
                    let Some(needed) = publication.committed_needed_arc() else {
                        self.execution.reject_execution(service(
                            "MV publication has no actual known-committed fact",
                        ));
                        return Poll::Ready(());
                    };
                    if let Err(error) = source.receive_committed(&needed) {
                        self.execution.reject_execution(error);
                        return Poll::Ready(());
                    }
                    match source.published_facts() {
                        Ok(Some(facts)) => self.execution.published = Some(Ok(facts)),
                        Ok(None) => self.execution.reject_execution(service(
                            "MV committed publication has no finalization facts",
                        )),
                        Err(error) => self.execution.reject_execution(error),
                    }
                    self.execution.phase = Phase::Received;
                    return Poll::Ready(());
                }
                Phase::Received => return Poll::Ready(()),
            }
        }
    }
    fn execution_outcome(
        &self,
    ) -> Option<Result<&MvRefreshPublicationFinalizationFacts, &MvProviderFailure>> {
        self.execution.published.as_ref().map(|r| r.as_ref())
    }
    fn poll_project_known_committed(
        &mut self,
        cx: &mut Context<'_>,
        _target: &MvTarget,
    ) -> Poll<()> {
        if self.execution.projected.is_some() {
            return Poll::Ready(());
        }
        let source = self.execution.source.as_ref().unwrap().clone();
        if !self.execution.projection_started {
            let result: Result<(), QueryServiceError> = (|| {
                let publication = self.execution.publication.as_ref().unwrap();
                let needed = publication
                    .committed_needed_arc()
                    .ok_or_else(|| service("MV projection lost the received commit"))?;
                let finite = publication
                    .existing_finalization_context()
                    .map_err(super::original_error)?
                    .cloned()
                    .ok_or_else(|| {
                        service("MV necessary finalization has no original finite context")
                    })?;
                let obligation = self
                    .io
                    .existing_metadata_obligation(&self.execution.root, &finite)
                    .map_err(QueryServiceError::from_failure_cause)?;
                let requests = self
                    .store
                    .existing_obligation_requests(
                        self.input.state_store.transaction_deadline,
                        obligation,
                    )
                    .map_err(QueryServiceError::from_failure_cause)?;
                // The exact Applied publication is already interpreted and
                // validated by the product. Close its original management
                // responsibility BEFORE requesting Current convergence: that
                // observation consumes the pending committed effect itself.
                // The precommit CPU borrower has actually exited at this cut.
                source.record_terminal(EffectDisposition::KnownCommitted)?;
                self.execution.terminal_recorded = true;
                self.execution.future = Some(source.project(needed, finite, requests));
                self.execution.projection_started = true;
                Ok(())
            })();
            if let Err(error) = result {
                self.execution.fail(error.clone());
                self.execution.projected = Some(Err(provider_error(
                    error,
                    self.execution.publication.as_ref(),
                )));
                return Poll::Ready(());
            }
        }
        let result = match self.execution.poll_phase(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        match result {
            Ok(()) => {
                self.execution.projected = Some(Ok(()));
                self.execution
                    .publication
                    .as_mut()
                    .unwrap()
                    .local_finalization = Some(Ok(()));
            }
            Err(error) => {
                self.execution
                    .publication
                    .as_mut()
                    .unwrap()
                    .local_finalization = Some(Err(error.service.to_string()));
                self.execution.fail(error.clone());
                self.execution.projected = Some(Err(provider_error(
                    error.service,
                    self.execution.publication.as_ref(),
                )));
            }
        }
        if let Err(error) = self
            .execution
            .publication
            .as_mut()
            .unwrap()
            .freeze_terminal()
        {
            self.execution.fail(error);
        }
        Poll::Ready(())
    }
    fn projection_outcome(&self) -> Option<&Result<(), MvProviderFailure>> {
        self.execution.projected.as_ref()
    }
}
fn new_publication(
    target: &MvTarget,
    attempt: &MvRefreshAttemptIdentity,
) -> Result<StatementPublication, QueryServiceError> {
    let catalog = target
        .catalog()
        .ok_or_else(|| service("MV publication requires an exact catalog"))?;
    let target = LakePublicationTarget::try_new(
        catalog.to_owned(),
        target.namespace().to_owned(),
        Some(target.name().to_owned()),
        None,
    )
    .map_err(super::original_error)?;
    Ok(StatementPublication::new(
        crate::dml::attempt::DmlPublicationAttempt::new(
            attempt.publication_id,
            LakePublicationFamily::MaterializedViewRefresh,
            target,
            None,
        ),
    ))
}
fn provider_error(
    error: QueryServiceError,
    publication: Option<&StatementPublication>,
) -> MvProviderFailure {
    let no_effect = publication.and_then(StatementPublication::received_committed_effect)
        == Some(novarocks_spi::connector::ExternalMutationEffect::NoOp);
    let kind = match publication.and_then(StatementPublication::disposition) {
        Some(LakePublicationDisposition::KnownCommitted) if !no_effect => {
            MvProviderFailureKind::KnownCommittedFinalizeFailed
        }
        Some(LakePublicationDisposition::CommitUnknown) => MvProviderFailureKind::CommitUnknown,
        _ => MvProviderFailureKind::KnownUncommitted,
    };
    let mut failure = MvProviderFailure::new(kind, error.to_string())
        .with_compile_control(error.compile_control_error());
    if let Some(cause) = error.failure_cause() {
        failure = failure.with_original_failure(cause.clone());
    }
    failure
}
pub(super) fn product_failure(error: MvProductError) -> MvPhaseFailure {
    let mut domain =
        crate::mv::domain::application::MvApplicationError::from_refresh_product(&error);
    let service = product_error(error);
    // A plain Product failure has an actual typed author at the service
    // boundary. Reuse that same cause rather than capturing it a second time.
    if domain.original_failure().is_none()
        && let Some(cause) = service.failure_cause()
    {
        domain = domain.with_original_failure(cause.clone());
    }
    MvPhaseFailure {
        service,
        domain: Some(domain),
    }
}

fn product_error(error: MvProductError) -> QueryServiceError {
    if let Some(cause) = error.original_failure() {
        return QueryServiceError::from_failure_cause(cause.clone())
            .with_compile_control(error.compile_control_error());
    }
    if let Some(control) = error.compile_control_error() {
        return QueryServiceError::from_compile_control(control);
    }
    if let Some(analyze) = error.unavailable_implementation() {
        return QueryServiceError::from_user_error(analyze.to_user_error(None));
    }
    QueryServiceError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
        error,
    ))
}

#[cfg(test)]
mod product_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
    use novarocks_mv_application::product::MvProductErrorKind;

    #[test]
    fn product_failure_keeps_its_actual_owner_and_corruption_fact() {
        let original =
            MvProductError::new(MvProductErrorKind::Corruption, "original MV proof differs");
        let fact = original.failure_fact();
        let selected = product_error(original);
        let cause = selected.failure_cause().expect("actual product cause");
        let received = cause
            .downcast_ref::<MvProductError>()
            .expect("same typed product owner");
        assert_eq!(received.kind(), MvProductErrorKind::Corruption);
        assert_eq!(received.message(), "original MV proof differs");
        assert_eq!(fact.class(), FailureClass::Data);
        assert_eq!(cause.failure_fact(), fact);
        assert_eq!(selected.execution_failure().unwrap().fact(), fact);
        assert!(selected.compile_control_error().is_none());
        assert!(
            selected.external_effect_disposition().is_none(),
            "classification alone is no commit proof"
        );
    }

    #[test]
    fn product_failure_keeps_original_domain_and_real_compile_sidecar() {
        let cause = SharedFailureCause::new(novarocks_workload_control::WorkError::Capacity(
            "original MV Root budget",
        ));
        let control = novarocks_type_contract::CompileControlError::DeadlineExceeded;
        let product = MvProductError::new(MvProductErrorKind::Unavailable, "domain failed")
            .with_original_failure(cause.clone())
            .with_compile_control(Some(control.clone()));
        assert_eq!(product.failure_fact(), cause.failure_fact());
        let selected = product_error(product);
        assert_eq!(selected.failure_cause(), Some(&cause));
        assert_eq!(
            selected.execution_failure().unwrap().fact(),
            cause.failure_fact()
        );
        assert_eq!(selected.compile_control_error(), Some(control));
    }

    #[test]
    fn product_public_projection_keeps_original_cause_without_exposing_private_detail() {
        use novarocks_query_application::api::StatementExternalEffectDisposition;
        // The source owns the private diagnostic; this closed producer authors
        // the safe detail independently. SafeDetail bounds text, not secrets.
        let cause = SharedFailureCause::from_static_detail(
            novarocks_workload_control::WorkError::Capacity(
                "private original Provider responsibility detail",
            ),
            "MV resource capacity is unavailable",
        );
        assert_eq!(
            cause
                .downcast_ref::<novarocks_workload_control::WorkError>()
                .expect("same private capacity source")
                .to_string(),
            "Work capacity exhausted: private original Provider responsibility detail",
        );
        let original_fact = cause.failure_fact();
        assert_eq!(original_fact.class(), FailureClass::Resource);
        for (kind, disposition) in [
            (
                MvProductErrorKind::CommitUnknown,
                StatementExternalEffectDisposition::CommitUnknown,
            ),
            (
                MvProductErrorKind::KnownCommittedFinalizeFailed,
                StatementExternalEffectDisposition::KnownCommitted,
            ),
        ] {
            let phase = product_failure(
                MvProductError::new(kind, "private original Product detail")
                    .with_original_failure(cause.clone()),
            );
            assert_eq!(phase.service.failure_cause(), Some(&cause));
            assert_eq!(
                phase.domain.as_ref().unwrap().original_failure(),
                Some(&cause),
            );
            assert_eq!(
                phase.service.execution_failure().unwrap().fact(),
                original_fact
            );
            assert_eq!(
                phase.service.execution_failure().unwrap().detail().as_str(),
                "MV resource capacity is unavailable",
            );
            // Error classification does not mint a publication fact. The Root
            // independently attaches its observed effect through this public API.
            assert!(phase.service.external_effect_disposition().is_none());
            let public = phase
                .service
                .attach_external_effect_disposition(disposition);
            assert_eq!(public.failure_cause(), Some(&cause));
            assert_eq!(public.execution_failure().unwrap().fact(), original_fact);
            assert_eq!(public.external_effect_disposition(), Some(disposition));
            assert!(public.message().contains("do not retry statement"));
            assert!(
                public
                    .message()
                    .contains("MV resource capacity is unavailable")
            );
            assert!(!public.message().contains("private original Provider"));
            assert!(!public.message().contains("private original Product"));
            assert!(public.compile_control_error().is_none());
            assert!(public.publication_terminal().is_none());
        }
    }

    #[test]
    fn selected_domain_failure_keeps_policy_and_cause_after_service_projection() {
        use super::super::MvCommandCompletionReceiving;
        use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
        let cause = SharedFailureCause::new(novarocks_workload_control::WorkError::Capacity(
            "original scheduled MV observation",
        ));
        let original = MvApplicationError::new(
            MvApplicationErrorKind::Unavailable,
            "scheduled MV observation unavailable",
        )
        .with_original_failure(cause.clone());
        let phase = source::domain_error(original.clone());
        assert_eq!(phase.service.failure_cause(), Some(&cause));
        let completion = MvCommandCompletionReceiving::default();
        // Creating an earlier, unselected domain failure is not a command
        // result. Neither it nor a successful phase installs failure policy.
        let _earlier_cleanup = source::domain_error(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            "unselected cleanup diagnostic",
        ));
        completion.receive_selected_failure(&Ok(()));
        assert!(completion.observed_failure().is_none());
        completion.receive_selected_failure(&Err(phase));
        let observed = completion.observed_failure().unwrap();
        assert_eq!(observed, &original);
        assert_eq!(observed.kind(), MvApplicationErrorKind::Unavailable);
        assert_eq!(observed.original_failure(), Some(&cause));
        // A later retirement diagnostic cannot overwrite the chosen primary.
        completion.receive_selected_failure(&Err(_earlier_cleanup));
        assert_eq!(completion.observed_failure(), Some(&original));
        assert!(completion.observed().is_none());
    }

    #[test]
    fn service_failure_without_a_domain_keeps_none() {
        let cause = SharedFailureCause::new(novarocks_workload_control::WorkError::Released);
        let phase = MvPhaseFailure::from(QueryServiceError::from_failure_cause(cause.clone()));
        assert!(phase.domain.is_none());
        assert_eq!(phase.service.failure_cause(), Some(&cause));
        let completion = super::super::MvCommandCompletionReceiving::default();
        completion.receive_selected_failure(&Err(phase));
        assert!(completion.observed_failure().is_none());
    }

    #[test]
    fn plain_product_phase_reuses_the_actual_product_cause() {
        let phase = product_failure(MvProductError::new(
            MvProductErrorKind::Corruption,
            "original publication proof differs",
        ));
        let source = phase.service.failure_cause().unwrap();
        assert!(source.downcast_ref::<MvProductError>().is_some());
        let domain = phase.domain.unwrap();
        assert_eq!(domain.original_failure(), Some(source));
        assert_eq!(
            domain.kind(),
            crate::mv::domain::application::MvApplicationErrorKind::Engine
        );
        assert!(phase.service.external_effect_disposition().is_none());
    }

    #[test]
    fn product_phase_keeps_the_existing_transient_domain_mapping() {
        let cause = SharedFailureCause::new(novarocks_workload_control::WorkError::Capacity(
            "original MV product observation",
        ));
        let original = MvProductError::new(MvProductErrorKind::Unavailable, "domain unavailable")
            .with_original_failure(cause.clone());
        let phase = product_failure(original);
        assert_eq!(phase.service.failure_cause(), Some(&cause));
        let domain = phase.domain.unwrap();
        assert_eq!(
            domain.kind(),
            crate::mv::domain::application::MvApplicationErrorKind::Unavailable
        );
        assert_eq!(domain.original_failure(), Some(&cause));
        assert_eq!(
            domain.original_failure().unwrap().failure_fact(),
            cause.failure_fact()
        );
    }

    #[test]
    fn product_missing_pure_implementation_keeps_the_analyzer_user_code() {
        let overload =
            novarocks_functions::FunctionOverloadId::try_new("fixture/missing/mv-product-pure-v1")
                .unwrap();
        let novarocks_sql::compiler::SqlCompileError::Analyze(analyze) =
            novarocks_sql::compiler::SqlCompileError::from(
                novarocks_functions::FunctionBindingError::UnavailableImplementation(overload),
            )
        else {
            panic!("the actual SQL author keeps missing-implementation identity");
        };
        let expected = analyze.code();
        let product = MvProductError::new(
            MvProductErrorKind::Unavailable,
            "pure implementation unavailable",
        )
        .with_unavailable_implementation(Some(analyze));
        let selected = product_error(product);
        assert_eq!(
            selected.user_error().unwrap().code().as_str(),
            expected.as_str()
        );
        assert!(selected.failure_cause().is_none());
        assert!(selected.compile_control_error().is_none());
    }
}
