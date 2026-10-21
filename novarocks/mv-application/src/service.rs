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

//! Product-owned process lifecycle for materialized-view activity and workers.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::activity::{
    MvActivityAdmissionError, MvActivityGate, MvActivityLease, MvActivityOwner, MvActivityTicket,
};
use crate::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvDropCatalogRegistrationPort,
    MvDropProjectionPort, MvDropProviderPort, MvProviderFailure, MvRefreshExecutionPort,
};
use crate::process_runtime::{
    MvBackgroundRuntimeLifecycleError, MvBackgroundRuntimeOwner, MvBackgroundRuntimeStart,
    ProcessRuntime,
};
use crate::product::{
    MvCommand, MvCreateCommand, MvOperationContext, MvProductError, MvProductErrorKind,
    MvProductResult, MvRefreshAttemptIdentity, MvStagedTarget, MvTarget,
};
use crate::publication::MvRefreshPublicationFinalizationFacts;
use crate::readiness::{MvDropReadiness, MvKeyedReadinessService, MvRuntimePublicationLease};
use crate::repository::{MvRepositoryError, MvRepositoryErrorKind};
use crate::scheduler::{MvRefreshScheduler, MvSchedulerConfig};

/// The one process-local MV owner for activity admission and background-worker
/// lifecycle. Hosts may inject effect callbacks, but cannot own a parallel
/// gate, shutdown state, or worker supervisor.
pub struct MvKeyedProductService {
    activity_gate: MvActivityGate,
    background: MvBackgroundRuntimeOwner,
    scheduler_config: MvSchedulerConfig,
    scheduler: Mutex<MvRefreshScheduler>,
    readiness: Option<MvKeyedReadinessService>,
    management: Option<Arc<crate::management::ManagementEntrance>>,
    continuation: Option<Arc<crate::management::ManagementContinuationService>>,
}

impl Default for MvKeyedProductService {
    fn default() -> Self {
        Self::new(MvSchedulerConfig::default())
    }
}

impl MvKeyedProductService {
    /// Construct the one product owner for this process' MV scheduler state.
    /// The host may use the tick interval to arrange a wakeup, but the queue,
    /// source revisions, retry ledger and terminal transitions remain here.
    pub fn new(scheduler_config: MvSchedulerConfig) -> Self {
        Self {
            activity_gate: MvActivityGate::default(),
            background: MvBackgroundRuntimeOwner::default(),
            scheduler: Mutex::new(MvRefreshScheduler::new(scheduler_config.clone())),
            scheduler_config,
            readiness: None,
            management: None,
            continuation: None,
        }
    }

    /// Borrow the product-owned authority for exact provider-port adaptation.
    pub fn management_entrance(&self) -> Option<Arc<crate::management::ManagementEntrance>> {
        self.management.clone()
    }

    /// The single process-local owner of management continuation. Absent on
    /// unit-only products, which have no management authority to continue.
    pub fn management_continuation(
        &self,
    ) -> Option<Arc<crate::management::ManagementContinuationService>> {
        self.continuation.clone()
    }

    pub fn scheduler_tick_interval_ms(&self) -> u64 {
        self.scheduler_config.tick_interval_ms()
    }

    /// Run one provider-observation adapter transition against the single
    /// product scheduler. The callback receives no clone or handle that could
    /// retain scheduler state outside this process product owner.
    pub fn with_refresh_scheduler<T>(
        &self,
        operation: impl FnOnce(&mut MvRefreshScheduler) -> T,
    ) -> T {
        let mut scheduler = self
            .scheduler
            .lock()
            .expect("MV product scheduler state lock poisoned");
        operation(&mut scheduler)
    }

    /// Reserve the one immutable identity for a product refresh publication.
    /// Query and provider adapters may derive their wire values from it but
    /// cannot mint a second identity for the same product transition.
    pub fn reserve_refresh_attempt(&self) -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity::reserve()
    }

    /// Begin the product-owned process publication lifetime before any
    /// provider effect. The returned lease releases the exact target and
    /// publication identity when the refresh transition unwinds.
    pub fn begin_refresh_publication(
        &self,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
    ) -> Result<MvRuntimePublicationLease, MvProductError> {
        let readiness = self.readiness.as_ref().ok_or_else(|| {
            MvProductError::new(
                MvProductErrorKind::Unavailable,
                "MV product has no configured readiness runtime",
            )
        })?;
        readiness
            .begin_publication(target.clone(), attempt.publication_id)
            .map_err(readiness_error)
    }

    /// Execute and finalize one product-owned refresh transition. The outer
    /// port is consumed once, so it cannot retain a parallel lifecycle or
    /// replay a publication after the product has classified its outcome.
    pub fn execute_refresh<'a>(
        &self,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
        execution: Box<dyn MvRefreshExecutionPort + 'a>,
    ) -> Result<MvProductResult, MvProductError> {
        let _publication = self.begin_refresh_publication(target, attempt)?;
        let known_committed = execution
            .execute_refresh(target, attempt)
            .map_err(MvProviderFailure::into_product_error)?;
        validate_published_refresh(target, attempt, known_committed.finalization_facts())?;
        known_committed
            .project_known_committed(target)
            .map_err(known_committed_finalize_failure)?;
        Ok(MvProductResult::Acknowledged)
    }

    /// Execute the product-owned CREATE state machine. The outer adapter owns
    /// SQL lowering and provider capabilities, while this product owns the
    /// effect order and the distinction between pre-commit cleanup and a
    /// known-committed finalization failure.
    pub fn create(
        &self,
        operation: MvOperationContext,
        create: MvCreateCommand,
        provider: &dyn MvCreateProviderPort,
        catalog_registration: &dyn MvCreateCatalogRegistrationPort,
    ) -> Result<MvProductResult, MvProductError> {
        let command = MvCommand::Create(create);
        let target = match &command {
            MvCommand::Create(create) => create.target.clone(),
            MvCommand::Drop { .. } | MvCommand::Refresh { .. } | MvCommand::Show { .. } => {
                unreachable!("CREATE product path constructed a non-CREATE command")
            }
        };
        // Staging has no catalog-visible effect, so a failure here ends the
        // statement with nothing to compensate.
        let staged = provider
            .stage_target(operation, &command)
            .map_err(MvProviderFailure::into_product_error)?;
        if staged.target != target {
            return Err(MvProductError::new(
                MvProductErrorKind::InvalidRequest,
                "MV CREATE provider staged a different target than the command named",
            ));
        }
        // The one atomic point: the target and its canonical documents become
        // visible together.
        let created = match provider.publish_staged_target(operation, &staged) {
            Ok(created) => created,
            Err(primary) => {
                return Err(settle_unpublished_stage(
                    operation, provider, &staged, primary,
                ));
            }
        };
        if created.target != target {
            return Err(MvProductError::new(
                MvProductErrorKind::Corruption,
                "MV CREATE publish reported a different target than it staged",
            ));
        }
        // Everything below is post-commit. The create is done and must not be
        // undone by a finalization failure.
        provider
            .install_created_projection(operation, &created)
            .map_err(known_committed_finalize_failure)?;
        catalog_registration
            .register_target(operation, &created)
            .map_err(known_committed_finalize_failure)?;
        Ok(MvProductResult::Created(created))
    }

    /// Execute the product-owned DROP state machine. The projection port
    /// supplies only exact durable facts; provider and catalog ports carry the
    /// outer effects without owning their order or terminal interpretation.
    pub fn drop<G>(
        &self,
        operation: MvOperationContext,
        target: MvTarget,
        if_exists: bool,
        projection: &dyn MvDropProjectionPort<G>,
        provider: &dyn MvDropProviderPort,
        catalog_registration: &dyn MvDropCatalogRegistrationPort,
    ) -> Result<MvProductResult, MvProductError> {
        match projection
            .prepare_drop(operation, &target, if_exists)
            .map_err(MvProviderFailure::into_product_error)?
        {
            MvDropReadiness::AlreadyAbsent => Ok(MvProductResult::Acknowledged),
            MvDropReadiness::ReadyToDrop(guard) => {
                provider
                    .drop_target(operation, &target)
                    .map_err(MvProviderFailure::into_product_error)?;
                projection
                    .delete_after_provider_drop(operation, guard)
                    .map_err(known_committed_finalize_failure)?;
                catalog_registration
                    .unregister_target(operation, &target)
                    .map_err(known_committed_finalize_failure)?;
                Ok(MvProductResult::Dropped)
            }
        }
    }

    pub fn acquire_foreground(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
        cancelled: impl Fn() -> bool,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        self.activity_gate
            .acquire_foreground(target, owner, cancelled)
    }

    /// Register background work with the shared product FIFO gate. The host
    /// retains only its effect callback and must release the returned ticket or
    /// lease; the product retains the state machine itself.
    pub fn request_activity(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, crate::activity::MvActivityGateError> {
        self.activity_gate.request(target, owner)
    }

    pub fn begin_background_start(
        &self,
    ) -> Result<MvBackgroundRuntimeStart<'_>, MvBackgroundRuntimeLifecycleError> {
        self.background.begin_start()
    }

    pub fn begin_stopping(&self) {
        self.activity_gate.begin_stopping();
    }

    pub async fn shutdown_background_workers_until(&self, deadline: Instant) -> Result<(), String> {
        self.background.shutdown_until(deadline).await
    }

    pub fn request_background_stop_for_process_exit(&self) {
        self.background.request_stop_for_process_exit();
    }
}

/// Settle a publish that did not report a committed target.
///
/// A staged target is discarded only when the publish proved it never became
/// visible. An unknown publication keeps its stage: aborting it could delete
/// the objects of a create that actually succeeded, and re-running it could
/// publish a second time. That case stays unsettled for the management owner
/// to adjudicate against the provider.
fn settle_unpublished_stage(
    operation: MvOperationContext,
    provider: &dyn MvCreateProviderPort,
    staged: &MvStagedTarget,
    primary: MvProviderFailure,
) -> MvProductError {
    let primary = primary.into_product_error();
    if matches!(
        primary.kind(),
        MvProductErrorKind::CommitUnknown | MvProductErrorKind::KnownCommittedFinalizeFailed
    ) {
        return primary;
    }
    match provider.abort_staged_target(operation, staged) {
        Ok(()) => primary,
        // A failed abort is a second failure; preserve its complete diagnostic
        // instead of replacing it with the original pure admission cause.
        Err(abort) => MvProductError::new(
            primary.kind(),
            format!("{}; staged target abort failed: {abort}", primary.message()),
        )
        .with_compile_control(primary.compile_control_error()),
    }
}

fn known_committed_finalize_failure(failure: MvProviderFailure) -> MvProductError {
    // Publication is already committed: this remains a finalization failure,
    // even when its diagnostic originated in SQL preparation.
    MvProductError::new(
        MvProductErrorKind::KnownCommittedFinalizeFailed,
        failure.message(),
    )
    .with_compile_control(failure.compile_control_error())
}

fn validate_published_refresh(
    target: &MvTarget,
    attempt: &MvRefreshAttemptIdentity,
    published: &MvRefreshPublicationFinalizationFacts,
) -> Result<(), MvProductError> {
    let intent = published.intent();
    if intent.publication_id() != attempt.publication_id {
        return Err(MvProductError::new(
            MvProductErrorKind::Corruption,
            "MV known-committed refresh facts do not match the reserved publication identity",
        ));
    }
    if target.catalog() != Some(intent.target_catalog())
        || target.namespace() != intent.target_namespace()
        || target.name() != intent.target_name()
    {
        return Err(MvProductError::new(
            MvProductErrorKind::Corruption,
            "MV known-committed refresh facts do not match the product target",
        ));
    }
    Ok(())
}

fn readiness_error(error: MvRepositoryError) -> MvProductError {
    let kind = match error.kind() {
        MvRepositoryErrorKind::InvalidRequest => MvProductErrorKind::InvalidRequest,
        MvRepositoryErrorKind::NotFound => MvProductErrorKind::TargetReplaced,
        MvRepositoryErrorKind::Conflict => MvProductErrorKind::Conflict,
        MvRepositoryErrorKind::Corruption => MvProductErrorKind::Corruption,
        MvRepositoryErrorKind::Unavailable => MvProductErrorKind::Unavailable,
        MvRepositoryErrorKind::CommitUnknown => MvProductErrorKind::CommitUnknown,
    };
    MvProductError::new(kind, error.to_string())
}

impl MvKeyedProductService {
    pub fn new_with_keyed_management_continuation(
        config: MvSchedulerConfig,
        repository: Arc<crate::state_store_repository::StateStoreMvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, novarocks_spi::connector::LakePublicationId>>,
        management: Arc<crate::management::ManagementEntrance>,
        policy: crate::management::RemoteEffectPolicy,
    ) -> Self {
        let mut service = Self::new(config);
        service.readiness = Some(MvKeyedReadinessService::new(repository, runtime));
        service.continuation = Some(Arc::new(
            crate::management::ManagementContinuationService::new(
                management.as_ref().clone(),
                policy,
            ),
        ));
        service.management = Some(management);
        service
    }

    pub fn keyed_readiness_service(&self) -> Option<crate::readiness::MvKeyedReadinessService> {
        self.readiness.clone()
    }
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
