// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frontend execution of a lake-authoritative MV publication.

use std::sync::{Arc, RwLock};
#[cfg(debug_assertions)]
use std::time::{Duration, Instant};

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::mv::domain::application::{
    MvApplicationError, MvApplicationErrorKind, MvStatementResult,
};
use crate::mv::domain::readiness::MvReadinessPort;
use crate::native::fragment_encoder::encode_native_fragment_bundle;
use crate::query_execution::ConnectorWriteCompletion;
use crate::query_execution::contract::ConnectorWriteExecutionRegistration;
use crate::query_execution::mv_assembly::refresh_artifact::MvRefreshCommittedFacts;
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshAttemptIdentity, PreparedMvRefresh, PreparedMvRefreshWork, PreparedMvRefreshWrite,
};
use crate::query_execution::mv_native_write::{
    MvRefreshProviderActivation, MvRefreshProviderActivationSink, PreparedMvNativeWriteAssembly,
};
use crate::query_execution::service::QueryExecutionService;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorControlRegistry, ConnectorExecutionBindingKey,
    ConnectorInstanceId, ConnectorMutationOperationId, ConnectorMvMetadataOnlyBaseFact,
    ConnectorMvMetadataOnlyProvenance, ConnectorRefAction, ConnectorRefKind,
    ConnectorRefreshPublicationGuard, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteReceipt, CreateOrReplacePolicy, ExternalMutationFinalization,
    ExternalMutationOutcome,
};

#[derive(Clone)]
pub(super) struct FrontendMvRefreshDependencies {
    pub(super) query_execution: QueryExecutionService,
    pub(super) connector_control: Arc<dyn ConnectorControlRegistry>,
    pub(super) provider_activation: Arc<FrontendMvRefreshProviderActivationPort>,
    pub(super) readiness: Arc<MvReadinessPort>,
}

pub(crate) struct FrontendMvRefreshProviderActivationPort {
    activation: RwLock<Option<Arc<dyn MvRefreshProviderActivation>>>,
}

impl FrontendMvRefreshProviderActivationPort {
    pub(crate) fn new() -> Self {
        Self {
            activation: RwLock::new(None),
        }
    }

    fn get(&self) -> Result<Arc<dyn MvRefreshProviderActivation>, MvApplicationError> {
        self.activation
            .read()
            .map_err(|_| unavailable("MV refresh provider activation lock is poisoned"))?
            .clone()
            .ok_or_else(|| unavailable("MV refresh provider activation is unavailable"))
    }

    fn bind(&self, activation: Arc<dyn MvRefreshProviderActivation>) -> Result<(), String> {
        let mut slot = self
            .activation
            .write()
            .map_err(|_| "MV refresh provider activation lock is poisoned".to_string())?;
        if slot.is_some() {
            return Err("MV refresh provider activation is already bound".to_string());
        }
        *slot = Some(activation);
        Ok(())
    }

    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
        write: &novarocks_spi::connector::ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedMvNativeWriteAssembly, MvApplicationError> {
        self.get()?
            .activate_write(prepared, planning, write, execution)
            .map_err(invalid)
    }

    fn validate_write_commit(
        &self,
        intent: crate::query_execution::mv_assembly::refresh_artifact::MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, MvApplicationError> {
        self.get()?
            .interpret_write_commit(intent, receipt)
            .map_err(invalid)
    }

    fn observe_published_package(
        &self,
        planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        snapshot: i64,
        context: &ConnectorRequestContext,
    ) -> Result<crate::mv::domain::storage_observation::MvLakePackageObservation, MvApplicationError>
    {
        self.get()?
            .observe_published_package(planning, table, snapshot, context)
            .map_err(|error| error.to_string())
            .and_then(|package| {
                crate::mv::domain::storage_observation::lake_package_from_spi(package)
                    .map_err(|error| error.to_string())
            })
            .map_err(|error| {
                MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    error.to_string(),
                )
            })
    }
}

impl MvRefreshProviderActivationSink for FrontendMvRefreshProviderActivationPort {
    fn bind_mv_refresh_provider_activation(
        &self,
        activation: Arc<dyn MvRefreshProviderActivation>,
    ) -> Result<(), String> {
        self.bind(activation)
    }
}

pub(super) fn execute(
    dependencies: &FrontendMvRefreshDependencies,
    refresh: PreparedMvRefresh,
    context: ConnectorRequestContext,
    execution: &QueryExecutionContext,
) -> Result<MvStatementResult, MvApplicationError> {
    if matches!(refresh.work, PreparedMvRefreshWork::NoOp) {
        return Ok(MvStatementResult::Ok);
    }
    let target = crate::mv::domain::repository::MvTarget {
        catalog: refresh.finalize.target.catalog.clone(),
        database: refresh.finalize.target.database.clone(),
        name: refresh.finalize.target.name.clone(),
    };
    let _runtime_publication = dependencies
        .readiness
        .begin_publication(&target, refresh.attempt.publication_id)
        .map_err(repository_error)?;
    let catalog = refresh
        .finalize
        .target
        .catalog
        .as_deref()
        .ok_or_else(|| invalid("MV refresh requires an explicit connector catalog"))?;
    let instance_id =
        ConnectorInstanceId::parse(catalog).map_err(|error| invalid(error.to_string()))?;
    let planning = dependencies
        .connector_control
        .acquire_current(&instance_id)
        .map_err(|error| unavailable(error.to_string()))?;
    if (ConnectorExecutionBindingKey {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        incarnation: planning.binding().incarnation(),
    }) != refresh.observed_binding
    {
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::CommitUnknown,
            "MV refresh connector generation changed after SQL preparation",
        ));
    }
    match refresh.work {
        PreparedMvRefreshWork::NoOp => unreachable!("no-op returned above"),
        PreparedMvRefreshWork::MetadataOnly { intent } => execute_metadata_only(
            dependencies,
            &planning,
            refresh.attempt,
            refresh.finalize,
            intent,
            context,
        ),
        PreparedMvRefreshWork::DataProducing { write } => execute_data(
            dependencies,
            &planning,
            refresh.attempt,
            refresh.finalize,
            write,
            context,
            execution,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_data(
    dependencies: &FrontendMvRefreshDependencies,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: MvRefreshAttemptIdentity,
    finalize: novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    prepared: PreparedMvRefreshWrite,
    context: ConnectorRequestContext,
    execution: &QueryExecutionContext,
) -> Result<MvStatementResult, MvApplicationError> {
    if prepared.operation_id() != attempt.write_operation_id() {
        return Err(invalid(
            "SQL-prepared MV write does not use its Lake publication identity",
        ));
    }
    let intent = prepared.publication_intent().clone();
    if intent.partition_spec_replacement().is_none() {
        create_data_staging_branch(planning, &attempt, &finalize, &intent, context.clone())?;
    }
    let write_lease = planning
        .derive_write_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let assembly = dependencies.provider_activation.activate_write(
        prepared,
        planning,
        &write_lease,
        execution,
    )?;
    let bundle = encode_native_fragment_bundle(assembly.native_encoding().encoding_view())
        .map_err(invalid)?;
    let write = assembly.finish(bundle).map_err(invalid)?;
    if write.write_operation_id() != attempt.write_operation_id() {
        return Err(invalid(
            "MV native write changed its Lake publication identity",
        ));
    }
    let session = dependencies
        .query_execution
        .begin_write_operation(write.registration(), write_lease)
        .map_err(|error| invalid(error.to_string()))?;
    let registration =
        ConnectorWriteExecutionRegistration::try_new(session, write.write_cohort_id())
            .map_err(|error| invalid(error.to_string()))?;
    let outcome = dependencies
        .query_execution
        .execute(
            write
                .into_request(execution, registration)
                .map_err(|error| invalid(error.to_string()))?,
        )
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
        })?
        .into_write()
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
        })?;
    let (result, direct, abort, completion) = outcome.into_parts_with_connector();
    if !result.columns.is_empty() || !result.chunks.is_empty() || direct.is_some() {
        return Err(invalid(
            "MV refresh write returned an invalid terminal payload",
        ));
    }
    if let Some(abort) = abort {
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            format!("MV refresh distributed write aborted: {}", abort.reason()),
        ));
    }
    let receipt = commit_known(
        &completion
            .ok_or_else(|| invalid("MV refresh write completed without connector reports"))?,
        context.clone(),
    )?;
    let committed = dependencies
        .provider_activation
        .validate_write_commit(intent, &receipt)?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::WriteCommitted)?;
    let publication_version = if committed.intent().partition_spec_replacement().is_some() {
        committed.committed_version().clone()
    } else {
        publish_data_staging_branch(planning, &attempt, &finalize, &committed, context.clone())?
    };
    wait_for_mv_recovery_phase(MvRecoveryPhase::PublicationCommitted)?;
    let snapshot = publication_version.snapshot_id().ok_or_else(|| {
        MvApplicationError::new(
            MvApplicationErrorKind::KnownCommittedFinalizeFailed,
            "MV publication completed without a snapshot ID",
        )
    })?;
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.into(),
        table: finalize.target.name.into(),
    };
    let package = dependencies
        .provider_activation
        .observe_published_package(planning, &table, snapshot, &context)?;
    wait_for_known_committed_before_projector_cas(&attempt.publication_id)?;
    dependencies
        .readiness
        .project_observed(*attempt.publication_id.as_uuid(), &package)
        .map_err(repository_error)?;
    Ok(MvStatementResult::Ok)
}

fn create_data_staging_branch(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: &MvRefreshAttemptIdentity,
    finalize: &novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    intent: &crate::query_execution::mv_assembly::refresh_artifact::MvRefreshPublicationIntent,
    context: ConnectorRequestContext,
) -> Result<(), MvApplicationError> {
    if finalize.target_table_uuid.is_empty() {
        return Err(invalid(
            "data-producing MV refresh requires a frozen target table UUID",
        ));
    }
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.clone().into(),
        table: finalize.target.name.clone().into(),
    };
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let operation_id =
        ConnectorMutationOperationId::from_bytes(*attempt.publication_id.as_uuid().as_bytes());
    require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::AlterRef {
                table,
                action: ConnectorRefAction::Create {
                    kind: ConnectorRefKind::Branch,
                    name: attempt.staging_branch().into(),
                    snapshot_id: intent.expected_target_snapshot_id(),
                    policy: CreateOrReplacePolicy::FailIfExists,
                    expected_table_uuid: Some(finalize.target_table_uuid.clone().into()),
                },
            },
            context,
        ),
        "create data-producing MV staging branch",
    )?;
    Ok(())
}

fn publish_data_staging_branch(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: &MvRefreshAttemptIdentity,
    finalize: &novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    committed: &MvRefreshCommittedFacts,
    context: ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorCommittedVersion, MvApplicationError> {
    if finalize.target_table_uuid.is_empty() {
        return Err(invalid(
            "data-producing MV publication requires a frozen target table UUID",
        ));
    }
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.clone().into(),
        table: finalize.target.name.clone().into(),
    };
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let operation_id =
        ConnectorMutationOperationId::from_bytes(*attempt.publication_id.as_uuid().as_bytes());
    let published = require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::AlterRef {
                table,
                action: ConnectorRefAction::FastForwardBranch {
                    source_branch: attempt.staging_branch().into(),
                    target_branch: Arc::from("main"),
                    committed_version: committed.committed_version().clone(),
                    expected_target_snapshot_id: committed.intent().expected_target_snapshot_id(),
                    expected_table_uuid: finalize.target_table_uuid.clone().into(),
                    guard: ConnectorRefreshPublicationGuard::new(attempt.publication_id),
                },
            },
            context,
        ),
        "publish data-producing MV staging branch",
    )?;
    published
        .receipt
        .committed_version()
        .cloned()
        .ok_or_else(|| invalid("data-producing MV publication committed without a version"))
}

fn execute_metadata_only(
    dependencies: &FrontendMvRefreshDependencies,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: MvRefreshAttemptIdentity,
    finalize: novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    intent: crate::query_execution::mv_assembly::refresh_artifact::MvRefreshPublicationIntent,
    context: ConnectorRequestContext,
) -> Result<MvStatementResult, MvApplicationError> {
    if intent.publication_id() != attempt.publication_id {
        return Err(invalid(
            "SQL-prepared metadata-only refresh changed its Lake publication identity",
        ));
    }
    let expected_table_uuid = finalize.target_table_uuid;
    if expected_table_uuid.is_empty() {
        return Err(invalid(
            "metadata-only MV refresh requires a frozen target table UUID",
        ));
    }
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.into(),
        table: finalize.target.name.into(),
    };
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let operation_id =
        ConnectorMutationOperationId::from_bytes(*attempt.publication_id.as_uuid().as_bytes());
    let staging_branch: Arc<str> = attempt.staging_branch().into();
    require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::AlterRef {
                table: table.clone(),
                action: ConnectorRefAction::Create {
                    kind: ConnectorRefKind::Branch,
                    name: Arc::clone(&staging_branch),
                    snapshot_id: intent.expected_target_snapshot_id(),
                    policy: CreateOrReplacePolicy::FailIfExists,
                    expected_table_uuid: Some(expected_table_uuid.clone().into()),
                },
            },
            context.clone(),
        ),
        "create metadata-only MV staging branch",
    )?;
    let provenance = ConnectorMvMetadataOnlyProvenance {
        publication_id: attempt.publication_id,
        bases: intent
            .bases()
            .iter()
            .map(|base| ConnectorMvMetadataOnlyBaseFact {
                table: base.table_fqn().into(),
                object_id: base.table_object_id().clone(),
                from_snapshot_id: base.from_snapshot(),
                to_snapshot_id: base.to_snapshot(),
            })
            .collect(),
        definition_fingerprint: intent.definition_fingerprint().into(),
    };
    let staged = require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::StageMvMetadataOnlySnapshot {
                table: table.clone(),
                expected_table_uuid: expected_table_uuid.clone().into(),
                expected_main_snapshot_id: intent.expected_target_snapshot_id(),
                staging_branch: Arc::clone(&staging_branch),
                expected_staging_snapshot_id: intent.expected_target_snapshot_id(),
                provenance,
            },
            context.clone(),
        ),
        "stage metadata-only MV snapshot",
    )?;
    let staged_version = staged
        .receipt
        .committed_version()
        .cloned()
        .ok_or_else(|| invalid("metadata-only MV staging committed without a version"))?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::WriteCommitted)?;
    let published = require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::AlterRef {
                table: table.clone(),
                action: ConnectorRefAction::FastForwardBranch {
                    source_branch: staging_branch,
                    target_branch: Arc::from("main"),
                    committed_version: staged_version,
                    expected_target_snapshot_id: intent.expected_target_snapshot_id(),
                    expected_table_uuid: expected_table_uuid.into(),
                    guard: ConnectorRefreshPublicationGuard::new(attempt.publication_id),
                },
            },
            context.clone(),
        ),
        "publish metadata-only MV snapshot",
    )?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::PublicationCommitted)?;
    let snapshot = published
        .receipt
        .committed_version()
        .and_then(novarocks_spi::connector::ConnectorCommittedVersion::snapshot_id)
        .ok_or_else(|| invalid("metadata-only MV publication committed without a snapshot ID"))?;
    let package = dependencies
        .provider_activation
        .observe_published_package(planning, &table, snapshot, &context)?;
    wait_for_known_committed_before_projector_cas(&attempt.publication_id)?;
    dependencies
        .readiness
        .project_observed(*attempt.publication_id.as_uuid(), &package)
        .map_err(repository_error)?;
    Ok(MvStatementResult::Ok)
}

/// Debug-only runner seam for the two durable MV recovery windows that are
/// observable around the publication fence. Production builds never block on
/// this filesystem trigger, and debug deployments do so only when the runner
/// has supplied the exact fault root and trigger file.
#[derive(Clone, Copy)]
enum MvRecoveryPhase {
    WriteCommitted,
    PublicationCommitted,
}

#[cfg(debug_assertions)]
impl MvRecoveryPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::WriteCommitted => "write-committed",
            Self::PublicationCommitted => "publication-committed",
        }
    }
}

#[cfg(debug_assertions)]
fn wait_for_mv_recovery_phase(phase: MvRecoveryPhase) -> Result<(), MvApplicationError> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(());
    };
    let phase = phase.as_str();
    let trigger = root.join(format!("mv-refresh-at-{phase}.trigger"));
    let contents = match std::fs::read_to_string(&trigger) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(unavailable(format!(
                "read runner-owned MV recovery trigger {}: {error}",
                trigger.display()
            )));
        }
    };
    let mut fields = contents.lines().filter_map(|line| line.split_once('='));
    let Some(("token", token)) = fields.next() else {
        return Err(invalid("MV recovery trigger has no token"));
    };
    if token.is_empty() || fields.next().is_some() {
        return Err(invalid("MV recovery trigger has invalid contents"));
    }
    eprintln!("NOVAROCKS_MV_RECOVERY_PHASE phase={phase} token={token}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while trigger.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if trigger.exists() {
        return Err(unavailable(format!(
            "timed out waiting for runner-owned MV recovery barrier at phase {phase}"
        )));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn wait_for_mv_recovery_phase(_phase: MvRecoveryPhase) -> Result<(), MvApplicationError> {
    Ok(())
}

/// Debug-only runner seam: the lake publication is known committed and its
/// immutable package has been read, but the Accelerator projector has not yet
/// entered its CAS. This is deliberately not a query-lifecycle phase.
#[cfg(debug_assertions)]
fn wait_for_known_committed_before_projector_cas(
    publication_id: &novarocks_spi::connector::LakePublicationId,
) -> Result<(), MvApplicationError> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(());
    };
    let trigger = novarocks_failpoint::mv_known_committed_before_projector_cas_trigger_path(&root);
    if !trigger.exists() {
        return Ok(());
    }
    let marker = novarocks_failpoint::mv_known_committed_before_projector_cas_marker_path(&root);
    let contents = format!(
        "publication_id={}\nphase=known-committed-before-projector-cas\n",
        publication_id.as_uuid()
    );
    std::fs::write(&marker, contents).map_err(|error| {
        unavailable(format!(
            "write runner-owned MV projector barrier marker {}: {error}",
            marker.display()
        ))
    })?;
    eprintln!(
        "NOVAROCKS_MV_PROJECTOR_PHASE publication_id={} phase=known-committed-before-projector-cas action=kill_fe",
        publication_id.as_uuid()
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while trigger.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if trigger.exists() {
        return Err(unavailable(
            "timed out waiting for runner-owned MV projector barrier release",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn wait_for_known_committed_before_projector_cas(
    _publication_id: &novarocks_spi::connector::LakePublicationId,
) -> Result<(), MvApplicationError> {
    Ok(())
}

fn require_catalog_commit(
    resolution: crate::connector::mutation::ResolvedCatalogMutation,
    operation: &str,
) -> Result<crate::connector::mutation::CompletedCatalogMutation, MvApplicationError> {
    match resolution {
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed) => {
            match &completed.finalization {
                ExternalMutationFinalization::Complete => Ok(completed),
                ExternalMutationFinalization::Failed(error) => Err(MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    format!("{operation} finalization failed: {error}"),
                )),
            }
        }
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted { failure } => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                format!("{operation} was not committed: {failure}"),
            ))
        }
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,
                format!("{operation} outcome is unknown: {failure}"),
            ))
        }
        crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { error, .. } => {
            Err(invalid(format!("{operation} contract failed: {error}")))
        }
    }
}

fn commit_known(
    completion: &ConnectorWriteCompletion,
    context: ConnectorRequestContext,
) -> Result<ConnectorWriteReceipt, MvApplicationError> {
    match completion.session().commit(context).map_err(|error| {
        MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
    })? {
        ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } => match finalization {
            ExternalMutationFinalization::Complete => Ok(receipt),
            ExternalMutationFinalization::Failed(error) => Err(MvApplicationError::new(
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                error.to_string(),
            )),
        },
        ExternalMutationOutcome::KnownUncommitted { failure } => Err(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            failure.to_string(),
        )),
        ExternalMutationOutcome::CommitUnknown { failure, .. } => Err(MvApplicationError::new(
            MvApplicationErrorKind::CommitUnknown,
            format!("MV refresh commit outcome is unknown: {failure}"),
        )),
    }
}

fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}
fn unavailable(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Unavailable, message)
}
fn repository_error(error: crate::mv::domain::repository::MvRepositoryError) -> MvApplicationError {
    let kind = match error.kind() {
        crate::mv::domain::repository::MvRepositoryErrorKind::Conflict => {
            MvApplicationErrorKind::AlreadyActive
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::NotFound => {
            MvApplicationErrorKind::TargetGone
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::Corruption => {
            MvApplicationErrorKind::Corruption
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::CommitUnknown => {
            MvApplicationErrorKind::CommitUnknown
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::Unavailable => {
            MvApplicationErrorKind::Unavailable
        }
        crate::mv::domain::repository::MvRepositoryErrorKind::InvalidRequest => {
            MvApplicationErrorKind::Repository
        }
    };
    MvApplicationError::new(kind, error.to_string())
}
