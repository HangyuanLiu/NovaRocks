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

//! The atomic point of MV CREATE.
//!
//! A staged target is invisible: it has no catalog entry until it is
//! published, and the publish carries the canonical definition, interpretation
//! and configuration documents in the same commit. There is deliberately no
//! visible empty table in between and no descriptor written afterwards.
//!
//! The publish writes no data and initially carries no snapshot. Later slot
//! changes may leave an empty parent snapshot; a missing P remains a first
//! refresh witness only when both file totals are known zero.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_mv_application::management::{
    CreateIntent, EffectDisposition, EffectIdentity, EffectResponsibility, EffectScope,
    ManagedMvTarget, ManagementEntrance, ManagementEntranceLease, ManagementRequest,
    ManagementTimestamp, ProcessIncarnation,
};
use novarocks_mv_application::persistence::codec::{DefinitionDocument, InterpretationDocument};
use novarocks_mv_application::persistence::create_documents::MvCreateDocuments;
use novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision;
use novarocks_mv_application::persistence::publication_facts::MvPublicationInputs;
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmission, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorDocumentStorageLease,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorColumnDefinition, ConnectorControlPlanningLease,
    ConnectorExactSemanticRevision, ConnectorMutationOperationId, ConnectorPartitionTransform,
    ConnectorRequestContext, ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreateLease, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePublicationPayload, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedTableHandle, ConnectorStagedWriteProof,
    ConnectorTableIdentity, ConnectorTableObjectId, CreatePolicy, LakePublicationId,
};

#[path = "staged_create/current_metadata_cpu.rs"]
mod current_metadata_cpu;
pub(crate) use current_metadata_cpu::CurrentProjectionMetadataCpu;

/// How a staged publish ended, in the product's own vocabulary.
///
/// `Unknown` is deliberately distinct from a failure: the create may have
/// succeeded, so its stage must not be aborted and its publish must not be
/// retried.
#[derive(Debug)]
pub(crate) enum StagedPublishOutcome {
    Published(ConnectorTableObjectId),
    CommittedFinalizeFailed(String),
    NotPublished(String),
    Unknown(String),
}

/// One invisible staged CREATE target and everything that must settle with it.
///
/// The management lease is held for the whole stage: it is the single business
/// write admission, and it records the create intent before the first staged
/// provider call so a lost response still leaves a responsibility behind.
pub(crate) struct StagedMvCreateTarget {
    management: ManagementEntranceLease,
    planning_lease: ConnectorControlPlanningLease,
    document_lease: ConnectorDocumentStorageLease,
    staged_lease: ConnectorStagedCreateLease,
    handle: ConnectorStagedTableHandle,
    admission: ConnectorDocumentManagementAdmission,
    catalog_handle: CatalogHandle,
    table: ConnectorTableIdentity,
    operation_id: ConnectorMutationOperationId,
}

/// Everything the provider needs to stage one target.
pub(crate) struct StageMvCreateRequest<'a> {
    pub planning_lease: ConnectorControlPlanningLease,
    pub table: ConnectorTableIdentity,
    pub columns: Vec<ConnectorColumnDefinition>,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
    pub operation_id: uuid::Uuid,
    pub context: &'a ConnectorRequestContext,
}

/// Reserve the create intent, admit the document management operation, and
/// stage the target. Nothing is catalog-visible when this returns.
pub(crate) fn stage_mv_create_target(
    entrance: &ManagementEntrance,
    request: StageMvCreateRequest<'_>,
) -> Result<StagedMvCreateTarget, String> {
    let catalog_handle = request
        .planning_lease
        .binding()
        .catalog_handle()
        .map_err(|error| format!("bind MV CREATE catalog generation: {error}"))?
        .clone();
    let operation_id = ConnectorMutationOperationId::from_bytes(*request.operation_id.as_bytes());
    let intent = CreateIntent::try_new(
        catalog_handle.clone(),
        request.table.clone(),
        EffectIdentity::from_bytes(*request.operation_id.as_bytes()),
    )
    .map_err(|error| format!("freeze MV CREATE intent: {error:?}"))?;

    // The single business write admission. It serializes against every other
    // management effect on this target and refuses a CREATE whose logical
    // target is already present or already unsettled.
    let cancelled_context = request.context.clone();
    let mut management = entrance
        .acquire(
            ManagementRequest::for_create_intent(intent, EffectScope::CATALOG_AND_OBJECT_DELETION),
            || cancelled_context.is_cancelled(),
        )
        .map_err(|error| format!("admit MV CREATE through the management entrance: {error:?}"))?;

    let document_lease = request
        .planning_lease
        .derive_document_storage_lease()
        .map_err(|error| format!("derive MV CREATE document storage lease: {error}"))?;
    let admission = document_lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                document_lease.owner().clone(),
                catalog_handle.clone(),
                operation_id,
                request.table.clone(),
                // A CREATE asserts the target is absent, so there is no exact
                // object to expect yet.
                None,
                ConnectorDocumentManagementOperation::Create,
                request.context.clone(),
            )
            .map_err(|error| format!("build MV CREATE document admission: {error}"))?,
        )
        .map_err(|error| format!("admit MV CREATE document management: {error}"))?;

    let staged_lease = request
        .planning_lease
        .derive_staged_create_lease()
        .map_err(|error| format!("derive MV CREATE staged lease: {error}"))?;
    // The statement identity is one value: the staged operation, its
    // publication and the create intent are the same effect, and the provider
    // refuses a stage whose operation and publication disagree.
    let publication_id =
        LakePublicationId::try_from_uuid(request.operation_id).map_err(|error| {
            format!("MV CREATE statement identity is not a publication ID: {error}")
        })?;
    let prepare = staged_lease
        .prepare_document_managed_request(
            publication_id,
            operation_id,
            request.table.clone(),
            request.columns,
            request.partitioning,
            request.properties,
            CreatePolicy::FailIfExists,
            admission.clone(),
            request.context.clone(),
        )
        .map_err(|error| format!("build MV CREATE staged request: {error}"))?;

    // Recorded before the first staged provider call: from here on a lost
    // response leaves an unbound create responsibility behind rather than a
    // silently abandoned stage.
    management
        .mark_create_intent_dispatched(now_management_timestamp()?)
        .map_err(|error| format!("record the MV CREATE intent responsibility: {error:?}"))?;

    match staged_lease.prepare(prepare) {
        Ok(ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. }) => {
            Ok(StagedMvCreateTarget {
                management,
                planning_lease: request.planning_lease,
                document_lease,
                staged_lease,
                handle,
                admission,
                catalog_handle,
                table: request.table,
                operation_id,
            })
        }
        Ok(ConnectorStagedCreatePrepareOutcome::Conflict { failure })
        | Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }) => {
            settle_known_uncommitted(management, "stage", failure.message())
        }
        Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown { failure, .. }) => Err(format!(
            "MV CREATE staging outcome is unknown and its intent stays unsettled: {}",
            failure.message()
        )),
        Ok(ConnectorStagedCreatePrepareOutcome::AppliedButUnusable { finalization, .. }) => {
            Err(format!(
                "MV CREATE staging was applied but is unusable; its intent stays unsettled: {finalization:?}"
            ))
        }
        // The retained lease conservatively locks a provider error after
        // dispatch. Only the typed negative arms above release responsibility.
        Err(error) => Err(format!(
            "MV CREATE staging failed without negative evidence; its intent stays unsettled: {error}"
        )),
    }
}

impl StagedMvCreateTarget {
    pub(crate) const fn handle(&self) -> &ConnectorStagedTableHandle {
        &self.handle
    }

    pub(crate) const fn planning_lease(&self) -> &ConnectorControlPlanningLease {
        &self.planning_lease
    }

    pub(crate) const fn staged_lease(&self) -> &ConnectorStagedCreateLease {
        &self.staged_lease
    }

    pub(crate) const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }

    /// Publish the staged target and its canonical documents in one commit.
    ///
    /// `write` must be the sealed empty staged write: MV CREATE publishes no
    /// data, so the created target carries no snapshot.
    pub(crate) fn publish(
        self,
        documents: &MvCreateDocuments,
        binding: novarocks_mv_application::persistence::codec::keyed::BindingDocument,
        write: ConnectorStagedWriteProof,
        owner: &novarocks_mv_application::management::DeploymentOwner,
        incarnation: &ProcessIncarnation,
        context: &ConnectorRequestContext,
    ) -> StagedPublishOutcome {
        let Self {
            management,
            document_lease,
            staged_lease,
            handle,
            admission,
            catalog_handle,
            table,
            operation_id,
            planning_lease: _,
        } = self;
        // The staged lease retains this exact provider generation through publication.
        drop(document_lease);
        let Some(prepared_target) = handle.document_target() else {
            return settle_publish_known_uncommitted(
                management,
                "staged MV CREATE target carries no prepared document binding".to_string(),
            );
        };
        // The publish asserts the target did not exist and commits the exact
        // staged object, so the created object is the one staging prepared.
        let object_id = prepared_target.object_id().clone();
        let keyed =
            match novarocks_mv_application::persistence::keyed_documents::keyed_create_document_set(
                &documents.definition,
                &documents.interpretation,
                &documents.configuration,
                binding,
                prepared_target,
                owner,
                incarnation,
            ) {
                Ok(value) => value,
                Err(error) => {
                    return settle_publish_known_uncommitted(
                        management,
                        format!("encode keyed MV CREATE documents: {error}"),
                    );
                }
            };
        let intent = novarocks_spi::connector::document_storage::KeyedCreateIntent {
            target: prepared_target.clone(),
            admission,
            documents: keyed,
        };
        let outcome = staged_lease.publish(ConnectorStagedCreatePublishRequest {
            operation_id,
            handle,
            write,
            payload: ConnectorStagedCreatePublicationPayload::KeyedDocuments(intent),
            context: context.clone(),
        });
        match outcome {
            Ok(ConnectorStagedCreatePublishOutcome::Applied { finalization, .. })
            | Ok(ConnectorStagedCreatePublishOutcome::NoOp { finalization, .. }) => {
                let settled = settle_published(management, catalog_handle, table, object_id);
                match finalization {
                    novarocks_spi::connector::ExternalMutationFinalization::Complete => settled,
                    novarocks_spi::connector::ExternalMutationFinalization::Failed(failure) => {
                        let mut message = format!(
                            "MV CREATE committed but provider finalization failed: {failure}"
                        );
                        if let StagedPublishOutcome::CommittedFinalizeFailed(local) = settled {
                            message.push_str(&format!("; {local}"));
                        }
                        StagedPublishOutcome::CommittedFinalizeFailed(message)
                    }
                }
            }
            Ok(ConnectorStagedCreatePublishOutcome::Conflict { failure, cleanup })
            | Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure, cleanup }) => {
                settle_publish_known_uncommitted(
                    management,
                    crate::connector::mutation::known_uncommitted_message(failure, &cleanup),
                )
            }
            Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, .. }) => {
                StagedPublishOutcome::Unknown(format!(
                    "MV CREATE publication outcome is unknown: {}",
                    failure.message()
                ))
            }
            Err(error) => settle_publish_known_uncommitted(management, error.to_string()),
        }
    }

    /// Discard a stage proven never to have been published.
    pub(crate) fn abort(self, context: &ConnectorRequestContext) -> Result<(), String> {
        let Self {
            management,
            staged_lease,
            handle,
            operation_id,
            ..
        } = self;
        let outcome = staged_lease.abort(ConnectorStagedCreateAbortRequest {
            operation_id,
            handle,
            write: None,
            context: context.clone(),
        });
        match outcome {
            Ok(ConnectorStagedCreateAbortOutcome::Aborted { finalization, .. }) => {
                let terminal = record_terminal(management, EffectDisposition::KnownUncommitted);
                match finalization {
                    novarocks_spi::connector::ExternalMutationFinalization::Complete => terminal,
                    novarocks_spi::connector::ExternalMutationFinalization::Failed(failure) => {
                        Err(format!(
                            "MV CREATE staged abort cleanup failed: {failure}{}",
                            terminal.err().map(|e| format!("; {e}")).unwrap_or_default()
                        ))
                    }
                }
            }
            Ok(ConnectorStagedCreateAbortOutcome::KnownUncommitted { failure }) => {
                let terminal = record_terminal(management, EffectDisposition::KnownUncommitted);
                Err(format!(
                    "MV CREATE staged abort did not complete: {failure}{}",
                    terminal.err().map(|e| format!("; {e}")).unwrap_or_default()
                ))
            }
            Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown { failure, .. }) => Err(format!(
                "MV CREATE staged abort outcome is unknown: {}",
                failure.message()
            )),
            Err(error) => Err(format!("abort MV CREATE staged target: {error}")),
        }
    }
}

fn settle_published(
    management: ManagementEntranceLease,
    catalog_handle: CatalogHandle,
    table: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
) -> StagedPublishOutcome {
    let mut management = management;
    let bound = ManagedMvTarget::try_new(catalog_handle, table, object_id.clone())
        .map_err(|error| format!("bind the published MV CREATE target: {error:?}"))
        .and_then(|target| {
            management
                .late_bind_create_target(target)
                .map_err(|error| format!("late-bind the MV CREATE responsibility: {error:?}"))
        });
    if let Err(error) = bound {
        return StagedPublishOutcome::CommittedFinalizeFailed(error);
    }
    match record_terminal(management, EffectDisposition::KnownCommitted) {
        Ok(()) => StagedPublishOutcome::Published(object_id),
        // The create is committed either way; only the responsibility record
        // failed to close, which must not be reported as a failed create.
        Err(error) => StagedPublishOutcome::CommittedFinalizeFailed(error),
    }
}

fn settle_known_uncommitted<T>(
    management: ManagementEntranceLease,
    phase: &str,
    message: impl std::fmt::Display,
) -> Result<T, String> {
    let message = format!("MV CREATE {phase} did not commit: {message}");
    match record_terminal(management, EffectDisposition::KnownUncommitted) {
        Ok(()) => Err(message),
        Err(error) => Err(format!("{message}; {error}")),
    }
}

fn settle_publish_known_uncommitted(
    management: ManagementEntranceLease,
    message: String,
) -> StagedPublishOutcome {
    match record_terminal(management, EffectDisposition::KnownUncommitted) {
        Ok(()) => StagedPublishOutcome::NotPublished(message),
        Err(error) => StagedPublishOutcome::NotPublished(format!("{message}; {error}")),
    }
}

fn record_terminal(
    management: ManagementEntranceLease,
    disposition: EffectDisposition,
) -> Result<(), String> {
    management
        .record_terminal(disposition)
        .map_err(|error| format!("record the MV CREATE terminal responsibility: {error:?}"))
}

pub(crate) fn now_management_timestamp() -> Result<ManagementTimestamp, String> {
    now_unix_millis().map(ManagementTimestamp::from_unix_millis)
}

fn now_unix_millis() -> Result<u64, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_string())?;
    u64::try_from(now.as_millis()).map_err(|_| "system clock exceeds u64 milliseconds".to_string())
}

/// Install the Current projection for a target this statement just created.
///
/// The create is already committed, so this only converges the management
/// state onto it and installs the projection. It must never re-create or
/// delete the target, and a failure here is a finalization failure.
pub(crate) fn install_created_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        // A created MV has published no output to read back.
        None,
        MvConvergence::CommittedEffect,
        context,
        "created",
    )
}

/// Re-observe a committed slot change and reopen management from the same
/// keyed Current observation.
pub(crate) fn install_configured_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        None,
        MvConvergence::CommittedEffect,
        context,
        "configured",
    )
}

/// Refresh an already admitted target from a fresh provider observation.
/// This does not claim a local committed effect or reopen a closed entrance.
pub(crate) fn install_admitted_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        None,
        MvConvergence::AdmittedCurrent,
        context,
        "observed",
    )
}

/// Renew the exact metadata generation after external metadata maintenance.
/// The actual installation read must retain every current MV carrier fact.
pub(crate) fn install_metadata_only_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    expected: std::sync::Arc<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection,
    >,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        expected.target(),
        uuid::Uuid::now_v7(),
        None,
        MvConvergence::MetadataOnlyCurrent { expected },
        context,
        "metadata-maintained",
    )
}

pub(crate) fn metadata_only_current_matches(
    expected: &novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection,
    observed: &novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
) -> bool {
    let source = expected.source_revision();
    observed.view.table == source.target
        && observed.view.object_id == source.target_object_id
        && observed.view.current_snapshot == expected.metadata_version().snapshot_id()
        && observed.publication_snapshot == expected.publication_snapshot()
        && observed.deployment_owner == source.deployment_owner
        && observed.process_incarnation == source.process_incarnation
        && observed.configuration_revision == source.configuration_revision
        && observed.publication_revision == source.publication_revision
        && &observed.definition == expected.definition()
        && &observed.interpretation == expected.interpretation()
        && &observed.versions == expected.versions()
        && &observed.configuration == expected.configuration()
        && observed.publication.as_ref() == expected.publication()
}

/// Install the Current projection of a target this statement just published.
///
/// The publication wrote P into the same commit as its rows, so the projection
/// is read back from that committed document set. The observation must retain
/// the exact carrier snapshot this statement committed.
pub(crate) fn install_published_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    published: PublishedOutput,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        Some(published),
        MvConvergence::CommittedEffect,
        context,
        "published",
    )
}

/// Reopen management on a target whose previous writer an operator has
/// declared isolated.
///
/// The declaration is already spent by the time this runs: what remains is the
/// exact re-observation it permitted, and installing what that observation
/// finds. The barrier the previous incarnation left is cleared by the install,
/// not by the declaration.
pub(crate) fn readmit_declared_target(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    previous_incarnation: ProcessIncarnation,
    permits: Vec<novarocks_mv_application::management::ReadmissionPermit>,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        // Readmission observes the current output without publishing one.
        None,
        MvConvergence::Readmission {
            previous_incarnation,
            permits,
        },
        context,
        "readmitted",
    )
}

/// The output one publication committed, as the committing statement knows it.
///
/// The observation that installs the projection has to read back this exact
/// output: a target that has moved on to a later publication is not the one
/// this statement published.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PublishedOutput {
    pub(crate) snapshot_id: i64,
}

fn install_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    published: Option<PublishedOutput>,
    convergence: MvConvergence,
    context: ConnectorRequestContext,
    effect: &str,
) -> Result<(), String> {
    let request = novarocks_mv_application::readiness::MvCurrentProjectionRequest::try_new(
        catalog,
        target,
        context,
        novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
    )
    .map_err(|error| format!("prepare the {effect} MV Current observation: {error}"))?;
    let source = ManagedTargetCurrentSource {
        entrance,
        connector_control,
        published,
        convergence,
    };
    readiness
        .observe_current_and_install(operation_id, request, &source)
        .map(|_| ())
        .map_err(|error| format!("install the {effect} MV Current projection: {error}"))
}

/// Observe and install through the caller's original request view. The caller
/// keeps `registration` and this exact F in its stable Action/Management source;
/// a returned business result does not close the registration child.
async fn install_current_projection_async_in(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    published: Option<PublishedOutput>,
    convergence: MvConvergence,
    context: ConnectorRequestContext,
    metadata_cpu: &CurrentProjectionMetadataCpu,
    registration: &CurrentProjectionRegistration,
) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
    let request = novarocks_mv_application::readiness::MvCurrentProjectionRequest::try_new(
        catalog,
        target,
        context,
        novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
    )?;
    let source = OriginalManagedTargetCurrentSource {
        entrance,
        connector_control,
        published,
        convergence,
        metadata_cpu,
        registration,
    };
    readiness
        .observe_current_and_install(operation_id, request, &source)
        .await
        .map(|_| ())
}

pub(crate) async fn install_created_current_projection_async_in(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    context: ConnectorRequestContext,
    metadata_cpu: &CurrentProjectionMetadataCpu,
    registration: &CurrentProjectionRegistration,
) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
    install_current_projection_async_in(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        None,
        MvConvergence::CommittedEffect,
        context,
        metadata_cpu,
        registration,
    )
    .await
}

pub(crate) async fn install_configured_current_projection_async_in(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    context: ConnectorRequestContext,
    metadata_cpu: &CurrentProjectionMetadataCpu,
    registration: &CurrentProjectionRegistration,
) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
    install_current_projection_async_in(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        None,
        MvConvergence::CommittedEffect,
        context,
        metadata_cpu,
        registration,
    )
    .await
}

pub(crate) async fn install_admitted_current_projection_async_in(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    context: ConnectorRequestContext,
    metadata_cpu: &CurrentProjectionMetadataCpu,
    registration: &CurrentProjectionRegistration,
) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
    install_current_projection_async_in(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        None,
        MvConvergence::AdmittedCurrent,
        context,
        metadata_cpu,
        registration,
    )
    .await
}

pub(crate) async fn install_published_current_projection_async_in(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    published: PublishedOutput,
    context: ConnectorRequestContext,
    metadata_cpu: &CurrentProjectionMetadataCpu,
    registration: &CurrentProjectionRegistration,
) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
    install_current_projection_async_in(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        Some(published),
        MvConvergence::CommittedEffect,
        context,
        metadata_cpu,
        registration,
    )
    .await
}

pub(crate) async fn install_metadata_only_current_projection_async_in(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    expected: Arc<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection,
    >,
    context: ConnectorRequestContext,
    metadata_cpu: &CurrentProjectionMetadataCpu,
    registration: &CurrentProjectionRegistration,
) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
    install_current_projection_async_in(
        entrance,
        readiness,
        connector_control,
        catalog,
        expected.target(),
        uuid::Uuid::now_v7(),
        None,
        MvConvergence::MetadataOnlyCurrent { expected },
        context,
        metadata_cpu,
        registration,
    )
    .await
}

pub(crate) async fn readmit_declared_target_async_in(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    previous_incarnation: ProcessIncarnation,
    permits: Vec<novarocks_mv_application::management::ReadmissionPermit>,
    context: ConnectorRequestContext,
    metadata_cpu: &CurrentProjectionMetadataCpu,
    registration: &CurrentProjectionRegistration,
) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
    install_current_projection_async_in(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        None,
        MvConvergence::Readmission {
            previous_incarnation,
            permits,
        },
        context,
        metadata_cpu,
        registration,
    )
    .await
}

/// Observe one target through the same management entrance, using the
/// convergence requirement appropriate to its current admission state.
///
/// The same sealed observation both mints the management admission and
/// supplies the documents, so readiness can never be installed from one read
/// and admitted by another.
struct ManagedTargetCurrentSource<'a> {
    entrance: &'a ManagementEntrance,
    connector_control: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    /// The output this observation must read back, absent when the effect
    /// published nothing.
    published: Option<PublishedOutput>,
    convergence: MvConvergence,
}

/// Why management is being reopened, which decides how the entrance is asked.
#[derive(Clone, Debug)]
enum MvConvergence {
    /// A manageable target remains admitted after a configuration observation
    /// or resolved slot change; its live admission is renewed from Current.
    AdmittedCurrent,
    /// Only metadata generation may change while retaining the same carrier.
    MetadataOnlyCurrent {
        expected: std::sync::Arc<
            novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection,
        >,
    },
    /// An effect this process committed must be re-observed before management
    /// reopens.
    CommittedEffect,
    /// A previous writer's unresolved effects were declared unable to land,
    /// and the target is being readmitted under this process. The permits are
    /// what the declaration bought: the observation cannot begin until every
    /// barrier they cover has been handed back to the state that holds it.
    Readmission {
        previous_incarnation: ProcessIncarnation,
        permits: Vec<novarocks_mv_application::management::ReadmissionPermit>,
    },
}

pub(crate) fn keyed_request(
    lease: &ConnectorDocumentStorageLease,
    table: ConnectorTableIdentity,
    object: ConnectorTableObjectId,
    context: ConnectorRequestContext,
) -> novarocks_spi::connector::KeyedDocumentObservationRequest {
    novarocks_spi::connector::KeyedDocumentObservationRequest {
        owner: lease.owner().clone(),
        catalog_handle: lease.catalog_handle().clone(),
        target: table,
        object_id: object,
        context,
    }
}

impl ManagedTargetCurrentSource<'_> {
    fn register_incarnation(
        &self,
        state: &mut novarocks_mv_application::management::ManagementObservationState,
        lease: &ConnectorControlPlanningLease,
        documents: &ConnectorDocumentStorageLease,
        observed: &novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        let operation_id =
            ConnectorMutationOperationId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let admission = documents
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    documents.owner().clone(),
                    documents.catalog_handle().clone(),
                    operation_id,
                    observed.view.table.clone(),
                    Some(observed.view.object_id.clone()),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    context.clone(),
                )
                .map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
        let intent=novarocks_spi::connector::KeyedMutationIntent {
            admission, expected_metadata:observed.view.metadata_version.clone(),
            expected_snapshot:observed.view.current_snapshot,
            mutation:novarocks_spi::connector::KeyedMutation::SlotChange(
                novarocks_mv_application::persistence::keyed_documents::keyed_management_slot_change(
                    self.entrance.owner(),self.entrance.incarnation()).map_err(|e|e.to_string())?),
        };
        let responsibility = EffectResponsibility::new(
            EffectIdentity::from_bytes(operation_id.to_bytes()),
            observed.management_target.clone(),
            self.entrance.incarnation().clone(),
            EffectScope::CATALOG_COMMIT,
            now_management_timestamp()?,
        );
        let mutation = lease.derive_mutation_lease().map_err(|e| e.to_string())?;
        let resolved = crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::KeyedDocuments { intent },
            context.clone(),
        );
        let disposition = super::iceberg_refresh::configuration_effect_disposition(&resolved);
        state
            .record_registration_terminal(responsibility.record_terminal(disposition))
            .map_err(|e| format!("record MV registration: {e:?}"))?;
        match disposition {
            EffectDisposition::KnownCommitted => Ok(()),
            EffectDisposition::KnownUncommitted => {
                Err("MV registration slot change did not commit".into())
            }
            EffectDisposition::CommitUnknown => Err(
                "MV registration slot change outcome is unknown; the target requires readmission"
                    .into(),
            ),
        }
    }
}

fn validate_current_capture_target(
    table: &ConnectorTableIdentity,
    binding: &novarocks_spi::connector::ConnectorTableObjectBinding,
) -> Result<(), String> {
    if &binding.metadata.identity != table {
        return Err("MV provider bound a different target".into());
    }
    Ok(())
}

fn validate_current_publication_carrier(
    published: Option<PublishedOutput>,
    observed: &novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
) -> Result<(), String> {
    if let Some(published) = published
        && observed.publication_snapshot != Some(published.snapshot_id)
    {
        return Err(
            "MV Current observation did not retain the committed publication carrier".into(),
        );
    }
    Ok(())
}

fn validate_readmission_capture(
    table: &ConnectorTableIdentity,
    binding: &novarocks_spi::connector::ConnectorTableObjectBinding,
    original: &novarocks_mv_application::persistence::identity::ObjectIdentity,
) -> Result<(), String> {
    if &binding.metadata.identity != table
        || !novarocks_mv_application::persistence::exact_revision::persisted_object_names(
            original,
            &binding.object_id,
        )
        .map_err(|error| error.to_string())?
    {
        return Err("same-name relation was rebuilt with a different object identity".into());
    }
    Ok(())
}

/// The original Action's registration custody. The Action installs this slot
/// before polling Current and retains it independently of the borrowed install
/// future. Business Needed facts never retire this slot or its opaque owner.
pub(crate) struct CurrentProjectionRegistration {
    capture: Arc<current_metadata_cpu::CurrentMetadataCapture>,
    receiving: Arc<novarocks_spi::connector::ConnectorCatalogMutationReceiving>,
    preparation: std::sync::OnceLock<CurrentRegistrationPreparation>,
    admission: std::sync::OnceLock<ConnectorDocumentManagementAdmission>,
    call: std::sync::Mutex<CurrentRegistrationCall>,
    state:
        std::sync::Mutex<Option<novarocks_mv_application::management::ManagementObservationState>>,
    health: novarocks_query_application::api::StatementNativeHealthFailure,
}

struct CurrentRegistrationPreparation {
    planning: ConnectorControlPlanningLease,
    documents: ConnectorDocumentStorageLease,
    observed:
        novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
    context: ConnectorRequestContext,
    operation_id: ConnectorMutationOperationId,
    responsibility: EffectResponsibility,
}

type CurrentRegistrationFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<(), novarocks_spi::connector::ConnectorError>>
            + Send,
    >,
>;

#[derive(Default)]
struct CurrentRegistrationCall {
    current_claimed: bool,
    call_installed: bool,
    future: Option<CurrentRegistrationFuture>,
    result: Option<Result<(), novarocks_spi::connector::ConnectorError>>,
    in_foreign: bool,
    actual_exit: bool,
    terminal_recorded: bool,
}

#[derive(Debug)]
struct CurrentRegistrationExitUnproven;
impl std::fmt::Display for CurrentRegistrationExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV Current registration local exit is unproven")
    }
}
impl std::error::Error for CurrentRegistrationExitUnproven {}

// A poll loan returns the very same F even during unwinding. Foreign poll and
// Drop are never performed under the receiving mutex. Sticky in_foreign means
// raw panic cannot turn into a retry or an ordinary business error.
struct CurrentRegistrationCallLoan<'a> {
    original: &'a CurrentProjectionRegistration,
    future: Option<CurrentRegistrationFuture>,
}
impl Drop for CurrentRegistrationCallLoan<'_> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let mut call = self.original.call.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                call.future.is_none(),
                "same original registration F receiving"
            );
            call.future = Some(future);
        }
    }
}

struct CurrentRegistrationStateLoan<'a> {
    original: &'a CurrentProjectionRegistration,
    state: Option<novarocks_mv_application::management::ManagementObservationState>,
}
impl std::ops::Deref for CurrentRegistrationStateLoan<'_> {
    type Target = novarocks_mv_application::management::ManagementObservationState;
    fn deref(&self) -> &Self::Target {
        self.state.as_ref().expect("original Current state")
    }
}
impl std::ops::DerefMut for CurrentRegistrationStateLoan<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state.as_mut().expect("original Current state")
    }
}
impl Drop for CurrentRegistrationStateLoan<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let mut receiving = self
                .original
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(receiving.is_none(), "same original Current state receiving");
            *receiving = Some(state);
        }
    }
}

fn current_projection_conflict(
    message: impl Into<String>,
) -> novarocks_mv_application::readiness::MvProjectionError {
    novarocks_mv_application::readiness::MvProjectionError::new(
        novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,
        message,
    )
}
fn current_projection_connector(
    error: novarocks_spi::connector::ConnectorError,
) -> novarocks_mv_application::readiness::MvProjectionError {
    novarocks_mv_application::readiness::MvProjectionError::from(
        novarocks_mv_application::persistence::documents::MvDocumentError::Connector(error),
    )
}
fn current_projection_observation(
    error: novarocks_mv_application::management::ManagementObservationFailure,
) -> novarocks_mv_application::readiness::MvProjectionError {
    let failure = current_projection_conflict(error.to_string());
    match error.original_failure() {
        Some(original) => failure.with_original_failure(original.clone()),
        None => failure,
    }
}
fn current_projection_mutation(
    error: novarocks_spi::connector::ConnectorMutationFailure,
) -> novarocks_mv_application::readiness::MvProjectionError {
    if let Some(original) = error.connector_source() {
        return current_projection_connector(original.clone());
    }
    let original = novarocks_failure_contract::SharedFailureCause::new(error);
    current_projection_conflict(original.detail()).with_original_failure(original)
}

impl CurrentProjectionRegistration {
    fn claim_current(&self) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
        let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        if call.current_claimed {
            return Err(current_projection_conflict(
                "MV Current original was already claimed",
            ));
        }
        call.current_claimed = true;
        Ok(())
    }
    pub(crate) fn new() -> Self {
        Self {
            capture: current_metadata_cpu::CurrentMetadataCapture::new(),
            receiving: Arc::new(novarocks_spi::connector::ConnectorCatalogMutationReceiving::new()),
            preparation: std::sync::OnceLock::new(),
            admission: std::sync::OnceLock::new(),
            call: std::sync::Mutex::new(CurrentRegistrationCall::default()),
            state: std::sync::Mutex::new(None),
            health:
                novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "MV Current registration local exit is unproven",
                    Arc::new(CurrentRegistrationExitUnproven),
                ),
        }
    }
    pub(crate) fn observation(
        &self,
    ) -> novarocks_spi::connector::ConnectorCatalogMutationObservation<'_> {
        self.receiving.observation()
    }
    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        if let Some(failure) = self.capture.local_health_failure() {
            return Some(failure);
        }
        let unproven = self
            .call
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .in_foreign;
        (unproven || self.receiving.foreign_exit_unproven()).then(|| self.health.clone())
    }
    fn install_state(
        &self,
        state: novarocks_mv_application::management::ManagementObservationState,
    ) -> Result<
        CurrentRegistrationStateLoan<'_>,
        novarocks_mv_application::readiness::MvProjectionError,
    > {
        let mut slot = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(current_projection_conflict(
                "MV Current state receiving is already occupied",
            ));
        }
        *slot = Some(state);
        Ok(CurrentRegistrationStateLoan {
            original: self,
            state: slot.take(),
        })
    }
    pub(crate) fn poll_progress(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        use std::task::Poll;
        let _ = self.capture.poll_progress(cx);
        let future = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            if call.in_foreign {
                return Poll::Pending;
            }
            if call.actual_exit {
                return Poll::Ready(());
            }
            let Some(future) = call.future.take() else {
                return Poll::Ready(());
            };
            call.in_foreign = true;
            future
        };
        let mut loan = CurrentRegistrationCallLoan {
            original: self,
            future: Some(future),
        };
        let poll = loan
            .future
            .as_mut()
            .expect("same registration F")
            .as_mut()
            .poll(cx);
        match poll {
            Poll::Pending => {
                // Restore before another observer can see a pollable state.
                {
                    let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                    call.future = loan.future.take();
                    call.in_foreign = false;
                }
                Poll::Pending
            }
            Poll::Ready(result) => {
                self.call.lock().unwrap_or_else(|p| p.into_inner()).result = Some(result);
                // Raw result is stable before its original F's destructor.
                // A destructor panic keeps actual_exit false and sticky health.
                drop(loan.future.take());
                let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                call.actual_exit = true;
                call.in_foreign = false;
                Poll::Ready(())
            }
        }
    }
    /// Original capture and registration children, never the whole Current/StateStore phase.
    /// Unknown and failed cleanup keep their exact original owner retained.
    pub(crate) fn poll_registration_closed(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        use novarocks_spi::connector::{
            ExternalMutationFinalization as F, ExternalMutationOutcome as O,
        };
        use std::task::Poll;
        let _ = self.poll_progress(cx);
        if self.capture.poll_closed(cx).is_pending() {
            return Poll::Pending;
        }
        let call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        if call.in_foreign {
            return Poll::Pending;
        }
        if !call.call_installed {
            return Poll::Ready(());
        }
        if !call.actual_exit {
            return Poll::Pending;
        }
        drop(call);
        let observation = self.receiving.observation();
        if !observation.dispatched() {
            return Poll::Ready(());
        }
        if !observation.provider_exited() || self.receiving.foreign_exit_unproven() {
            return Poll::Pending;
        }
        match observation.validated_outcome() {
            Some(Ok(O::KnownCommitted {
                cleanup: F::Complete,
                ..
            }))
            | Some(Ok(O::KnownUncommitted {
                cleanup: F::Complete,
                ..
            })) => Poll::Ready(()),
            _ => Poll::Pending,
        }
    }
    fn poll_needed(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<(
        EffectDisposition,
        Result<(), novarocks_mv_application::readiness::MvProjectionError>,
    )> {
        use crate::connector::mutation::{
            MutationDispatchState as D, ResolvedCatalogMutation as R,
        };
        use novarocks_spi::connector::{
            ExternalMutationFinalization as F, ExternalMutationOutcome as O,
        };
        use std::task::Poll;
        let _ = self.poll_progress(cx);
        // Borrow the source facts only long enough to clone actual values.
        // No formatting, callback or owner destruction occurs under this lock.
        let (needed, validated, dispatched) = {
            let observation = self.receiving.observation();
            (
                observation.needed_committed().cloned(),
                observation
                    .validated_outcome()
                    .map(|outcome| outcome.map(Clone::clone).map_err(Clone::clone)),
                observation.dispatched(),
            )
        };
        if let Some(needed) = needed {
            let result = match needed.finalization() {
                F::Complete => Ok(()),
                F::Failed(failure) => Err(current_projection_mutation(failure.clone())),
            };
            return Poll::Ready((EffectDisposition::KnownCommitted, result));
        }
        let resolved = match validated {
            Some(Ok(O::KnownCommitted {
                finalization,
                cleanup,
                ..
            })) => R::KnownCommitted(crate::connector::mutation::CompletedCatalogMutation {
                finalization,
                cleanup,
            }),
            Some(Ok(O::KnownUncommitted { failure, cleanup })) => {
                R::KnownUncommitted { failure, cleanup }
            }
            Some(Ok(O::CommitUnknown { failure, evidence })) => {
                R::CommitUnknown { failure, evidence }
            }
            Some(Err(error)) => R::ContractFailure {
                error,
                dispatch: if dispatched {
                    D::PossiblyDispatched
                } else {
                    D::ConfirmedNotDispatched
                },
            },
            None => {
                let error = self
                    .call
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .result
                    .as_ref()
                    .and_then(|result| result.as_ref().err())
                    .cloned();
                let Some(error) = error else {
                    return Poll::Pending;
                };
                R::ContractFailure {
                    error,
                    dispatch: if dispatched {
                        D::PossiblyDispatched
                    } else {
                        D::ConfirmedNotDispatched
                    },
                }
            }
        };
        let disposition = super::iceberg_refresh::configuration_effect_disposition(&resolved);
        let result = match resolved {
            R::KnownCommitted(completed) => match completed.finalization {
                F::Complete => Ok(()),
                F::Failed(failure) => Err(current_projection_mutation(failure)),
            },
            R::KnownUncommitted { failure, .. } | R::CommitUnknown { failure, .. } => {
                Err(current_projection_mutation(failure))
            }
            R::ContractFailure { error, .. } => Err(current_projection_connector(error)),
        };
        Poll::Ready((disposition, result))
    }
}

/// Original governed borrower. The old synchronous Source remains unchanged
/// until its callers move to an Action with this stable registration slot.
struct OriginalManagedTargetCurrentSource<'a> {
    entrance: &'a ManagementEntrance,
    connector_control: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    published: Option<PublishedOutput>,
    convergence: MvConvergence,
    metadata_cpu: &'a CurrentProjectionMetadataCpu,
    registration: &'a CurrentProjectionRegistration,
}

impl OriginalManagedTargetCurrentSource<'_> {
    async fn register_incarnation(
        &self,
        state: &mut novarocks_mv_application::management::ManagementObservationState,
        lease: &ConnectorControlPlanningLease,
        documents: &ConnectorDocumentStorageLease,
        observed: &novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        context: &ConnectorRequestContext,
    ) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
        let operation_id =
            ConnectorMutationOperationId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let preparation = CurrentRegistrationPreparation {
            planning: lease.clone(),
            documents: documents.clone(),
            observed: observed.clone(),
            context: context.clone(),
            operation_id,
            responsibility: EffectResponsibility::new(
                EffectIdentity::from_bytes(operation_id.to_bytes()),
                observed.management_target.clone(),
                self.entrance.incarnation().clone(),
                EffectScope::CATALOG_COMMIT,
                now_management_timestamp().map_err(current_projection_conflict)?,
            ),
        };
        if self.registration.preparation.set(preparation).is_err() {
            return Err(current_projection_conflict(
                "MV Current registration original was already prepared",
            ));
        }
        let original = self
            .registration
            .preparation
            .get()
            .expect("original registration preparation");
        let admission = original
            .documents
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    original.documents.owner().clone(),
                    original.documents.catalog_handle().clone(),
                    original.operation_id,
                    original.observed.view.table.clone(),
                    Some(original.observed.view.object_id.clone()),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    original.context.clone(),
                )
                .map_err(current_projection_connector)?,
            )
            .map_err(current_projection_connector)?;
        // Retain the actual token before any further fallible pure formation.
        // No temporary token destructor can overwrite the original refusal.
        if self.registration.admission.set(admission).is_err() {
            unreachable!("single original registration document admission");
        }
        let intent = novarocks_spi::connector::KeyedMutationIntent {
            admission: self.registration.admission.get().expect("same original registration admission").clone(), expected_metadata: original.observed.view.metadata_version.clone(),
            expected_snapshot: original.observed.view.current_snapshot,
            mutation: novarocks_spi::connector::KeyedMutation::SlotChange(
                novarocks_mv_application::persistence::keyed_documents::keyed_management_slot_change(
                    self.entrance.owner(), self.entrance.incarnation(),
                ).map_err(novarocks_mv_application::readiness::MvProjectionError::from)?),
        };
        let mutation = original
            .planning
            .derive_mutation_lease()
            .map_err(current_projection_connector)?;
        let receiving = Arc::clone(&self.registration.receiving);
        let context = original.context.clone();
        // The original future is installed before its first factory/poll. It
        // owns only exact input/receiving aliases, never its outer Source.
        let future: CurrentRegistrationFuture = Box::pin(async move {
            mutation
                .execute_operation_async_in(
                    operation_id,
                    novarocks_spi::connector::ConnectorCatalogMutationOperation::KeyedDocuments {
                        intent,
                    },
                    context,
                    receiving.as_ref(),
                )
                .await
        });
        {
            let mut call = self
                .registration
                .call
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                !call.call_installed && call.future.is_none(),
                "one original registration call"
            );
            call.future = Some(future);
            call.call_installed = true;
        }
        let (disposition, result) =
            std::future::poll_fn(|cx| self.registration.poll_needed(cx)).await;
        let recorded = self
            .registration
            .call
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .terminal_recorded;
        if !recorded {
            state
                .record_registration_terminal(
                    original.responsibility.clone().record_terminal(disposition),
                )
                .map_err(|error| {
                    current_projection_conflict(format!("record MV registration: {error:?}"))
                })?;
            self.registration
                .call
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .terminal_recorded = true;
        }
        result
    }
}

#[async_trait::async_trait]
impl novarocks_mv_application::readiness::MvKeyedCurrentProjectionSource
    for OriginalManagedTargetCurrentSource<'_>
{
    async fn observe_current_keyed(
        &self,
        request: novarocks_mv_application::readiness::MvCurrentProjectionRequest,
    ) -> Result<
        (
            novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
            novarocks_mv_application::management::MvCurrentManagementAdmission,
        ),
        novarocks_mv_application::readiness::MvProjectionError,
    > {
        use novarocks_mv_application::persistence::keyed_documents::decode_current_keyed_documents;
        use novarocks_mv_application::readiness::{MvProjectionError, MvProjectionErrorKind};
        let conflict = |e: String| MvProjectionError::new(MvProjectionErrorKind::SourceConflict, e);
        self.registration.claim_current()?;
        let catalog = request
            .target()
            .catalog()
            .ok_or_else(|| conflict("MV target has no catalog".into()))?;
        let instance = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
            .map_err(|error| conflict(error.to_string()))?;
        let lease = self
            .connector_control
            .acquire_current(&instance)
            .map_err(current_projection_connector)?;
        if lease
            .binding()
            .catalog_handle()
            .map_err(current_projection_connector)?
            != request.catalog()
        {
            return Err(conflict(
                "MV catalog generation changed before Current observation".into(),
            ));
        }
        let table = ConnectorTableIdentity {
            instance_id: instance,
            namespace: request.target().namespace().into(),
            table: request.target().name().into(),
        };
        let binding = self
            .registration
            .capture
            .capture(
                &lease,
                self.metadata_cpu,
                novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                    table: table.clone(),
                    resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                    selector: novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                    context: request.context().clone(),
                },
            )
            .await
            .map_err(current_metadata_cpu::CaptureFailure::into_projection)?;
        validate_current_capture_target(&table, &binding).map_err(conflict)?;
        let documents = lease
            .derive_document_storage_lease()
            .map_err(current_projection_connector)?;
        let observation = keyed_request(
            &documents,
            table.clone(),
            binding.object_id.clone(),
            request.context().clone(),
        );
        let state = match &self.convergence {
            MvConvergence::AdmittedCurrent | MvConvergence::MetadataOnlyCurrent { .. } => {
                let (observed, admission, _) = self
                    .entrance
                    .observe_keyed_admission_async(&documents, observation, request.decode_budget())
                    .await
                    .map_err(MvProjectionError::from)?;
                if let MvConvergence::MetadataOnlyCurrent { expected } = &self.convergence
                    && !metadata_only_current_matches(expected, &observed)
                {
                    return Err(conflict(
                        "MV carrier changed during metadata-only Current renewal".into(),
                    ));
                }
                return Ok((observed, admission));
            }
            MvConvergence::CommittedEffect => self.entrance.begin_committed_convergence(
                &table,
                novarocks_mv_application::management::ManagementContinuation::SameOwner {
                    previous_incarnation: self.entrance.incarnation().clone(),
                },
            ),
            MvConvergence::Readmission {
                previous_incarnation,
                ..
            } => self.entrance.begin_readmission(
                &table,
                novarocks_mv_application::management::ManagementContinuation::SameOwner {
                    previous_incarnation: previous_incarnation.clone(),
                },
            ),
        }
        .map_err(|e| conflict(format!("begin MV Current convergence: {e:?}")))?;
        let mut state = self.registration.install_state(state)?;
        if let MvConvergence::Readmission { permits, .. } = &self.convergence {
            for permit in permits {
                state
                    .accept_readmission_permit(permit.clone())
                    .map_err(|e| conflict(format!("accept MV readmission permit: {e:?}")))?;
            }
        }
        let pending = state
            .begin_current_observation(
                novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(
                    *uuid::Uuid::now_v7().as_bytes(),
                ),
            )
            .map_err(|e| conflict(format!("begin MV observation: {e:?}")))?;
        let (phase, view) = state
            .complete_keyed_current_observation_async(pending, &documents, observation.clone())
            .await
            .map_err(current_projection_observation)?;
        let mut observed = decode_current_keyed_documents(
            view,
            request.catalog().clone(),
            request.decode_budget(),
        )
        .map_err(MvProjectionError::from)?;
        let convergence = self
            .entrance
            .resolve_pending_slot_changes_async(&documents, observation.clone())
            .await
            .map_err(current_projection_connector)?;
        if matches!(&self.convergence, MvConvergence::Readmission { .. }) {
            for occurrence in &observed.definition.relation_occurrences {
                let source_instance = novarocks_spi::connector::ConnectorInstanceId::parse(
                    &occurrence.catalog_at_binding,
                )
                .map_err(|error| conflict(error.to_string()))?;
                let source = self
                    .connector_control
                    .acquire_current(&source_instance)
                    .map_err(current_projection_connector)?;
                let source_table = ConnectorTableIdentity {
                    instance_id: source_instance,
                    namespace: occurrence.namespace_at_binding.clone().into(),
                    table: occurrence.relation_at_binding.clone().into(),
                };
                let live = self
                    .registration
                    .capture
                    .capture(
                        &source,
                        self.metadata_cpu,
                        novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                            table: source_table.clone(),
                            resolution:
                                novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                            selector:
                                novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                            context: request.context().clone(),
                        },
                    )
                    .await
                    .map_err(current_metadata_cpu::CaptureFailure::into_projection)?;
                validate_readmission_capture(&source_table, &live, &occurrence.object_id)
                    .map_err(conflict)?;
            }
        }
        if matches!(
            phase,
            novarocks_mv_application::management::ManagementObservationPhase::RegistrationRequired(
                _
            )
        ) {
            self.register_incarnation(&mut state, &lease, &documents, &observed, request.context())
                .await?;
            let registered_request = keyed_request(
                &documents,
                table.clone(),
                binding.object_id.clone(),
                request.context().clone().after_external_effect(),
            );
            let pending=state.begin_current_observation(novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes())).map_err(|e|conflict(format!("begin registered MV observation: {e:?}")))?;
            let (_, view) = state
                .complete_keyed_current_observation_async(
                    pending,
                    &documents,
                    registered_request.clone(),
                )
                .await
                .map_err(current_projection_observation)?;
            self.entrance
                .resolve_pending_slot_changes_async(&documents, registered_request.clone())
                .await
                .map_err(current_projection_connector)?;
            observed = decode_current_keyed_documents(
                view,
                request.catalog().clone(),
                request.decode_budget(),
            )
            .map_err(MvProjectionError::from)?;
        } else if convergence.reinstall_current_projection {
            let pending=state.begin_current_observation(novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes())).map_err(|e|conflict(format!("begin resolved MV observation: {e:?}")))?;
            let (_, view) = state
                .complete_keyed_current_observation_async(pending, &documents, observation)
                .await
                .map_err(current_projection_observation)?;
            observed = decode_current_keyed_documents(
                view,
                request.catalog().clone(),
                request.decode_budget(),
            )
            .map_err(MvProjectionError::from)?;
        }
        validate_current_publication_carrier(self.published, &observed).map_err(conflict)?;
        let admission = self
            .entrance
            .install_observed_target(
                &state,
                observed.management_dependencies(lease.control_runtime_id()),
            )
            .map_err(|e| conflict(format!("install MV admission: {e:?}")))?;
        Ok((observed, admission))
    }
}

#[async_trait::async_trait]
impl novarocks_mv_application::readiness::MvKeyedCurrentProjectionSource
    for ManagedTargetCurrentSource<'_>
{
    async fn observe_current_keyed(
        &self,
        request: novarocks_mv_application::readiness::MvCurrentProjectionRequest,
    ) -> Result<
        (
            novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
            novarocks_mv_application::management::MvCurrentManagementAdmission,
        ),
        novarocks_mv_application::readiness::MvProjectionError,
    > {
        use novarocks_mv_application::persistence::keyed_documents::decode_current_keyed_documents;
        use novarocks_mv_application::readiness::{MvProjectionError, MvProjectionErrorKind};
        let conflict = |e: String| MvProjectionError::new(MvProjectionErrorKind::SourceConflict, e);
        let catalog = request
            .target()
            .catalog()
            .ok_or_else(|| conflict("MV target has no catalog".into()))?;
        let instance = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
            .map_err(|e| conflict(e.to_string()))?;
        let lease = self
            .connector_control
            .acquire_current(&instance)
            .map_err(|e| conflict(e.to_string()))?;
        if lease
            .binding()
            .catalog_handle()
            .map_err(|e| conflict(e.to_string()))?
            != request.catalog()
        {
            return Err(conflict(
                "MV catalog generation changed before Current observation".into(),
            ));
        }
        let table = ConnectorTableIdentity {
            instance_id: instance,
            namespace: request.target().namespace().into(),
            table: request.target().name().into(),
        };
        let binding = lease
            .binding()
            .metadata()
            .capture_table_object_binding(
                novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                    table: table.clone(),
                    resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                    selector: novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                    context: request.context().clone(),
                },
            )
            .map_err(|e| conflict(e.to_string()))?;
        validate_current_capture_target(&table, &binding).map_err(conflict)?;
        let documents = lease
            .derive_document_storage_lease()
            .map_err(|e| conflict(e.to_string()))?;
        let observation = keyed_request(
            &documents,
            table.clone(),
            binding.object_id.clone(),
            request.context().clone(),
        );
        let mut state = match &self.convergence {
            MvConvergence::AdmittedCurrent | MvConvergence::MetadataOnlyCurrent { .. } => {
                let (observed, admission, _) = self
                    .entrance
                    .observe_keyed_admission(&documents, observation, request.decode_budget())
                    .map_err(|e| conflict(format!("observe admitted MV Current: {e}")))?;
                if let MvConvergence::MetadataOnlyCurrent { expected } = &self.convergence
                    && !metadata_only_current_matches(expected, &observed)
                {
                    return Err(conflict(
                        "MV carrier changed during metadata-only Current renewal".into(),
                    ));
                }
                return Ok((observed, admission));
            }
            MvConvergence::CommittedEffect => self.entrance.begin_committed_convergence(
                &table,
                novarocks_mv_application::management::ManagementContinuation::SameOwner {
                    previous_incarnation: self.entrance.incarnation().clone(),
                },
            ),
            MvConvergence::Readmission {
                previous_incarnation,
                ..
            } => self.entrance.begin_readmission(
                &table,
                novarocks_mv_application::management::ManagementContinuation::SameOwner {
                    previous_incarnation: previous_incarnation.clone(),
                },
            ),
        }
        .map_err(|e| conflict(format!("begin MV Current convergence: {e:?}")))?;
        if let MvConvergence::Readmission { permits, .. } = &self.convergence {
            for permit in permits {
                state
                    .accept_readmission_permit(permit.clone())
                    .map_err(|e| conflict(format!("accept MV readmission permit: {e:?}")))?;
            }
        }
        let pending = state
            .begin_current_observation(
                novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(
                    *uuid::Uuid::now_v7().as_bytes(),
                ),
            )
            .map_err(|e| conflict(format!("begin MV observation: {e:?}")))?;
        let (phase, view) = state
            .complete_keyed_current_observation(pending, &documents, observation.clone())
            .map_err(|e| conflict(format!("complete MV observation: {e:?}")))?;
        let mut observed = decode_current_keyed_documents(
            view,
            request.catalog().clone(),
            request.decode_budget(),
        )
        .map_err(|e| conflict(e.to_string()))?;
        let convergence = self
            .entrance
            .resolve_pending_slot_changes(&documents, observation.clone())
            .map_err(|e| conflict(e.to_string()))?;
        if matches!(&self.convergence, MvConvergence::Readmission { .. }) {
            for occurrence in &observed.definition.relation_occurrences {
                let source_instance = novarocks_spi::connector::ConnectorInstanceId::parse(
                    &occurrence.catalog_at_binding,
                )
                .map_err(|e| conflict(e.to_string()))?;
                let source = self
                    .connector_control
                    .acquire_current(&source_instance)
                    .map_err(|e| conflict(e.to_string()))?;
                let source_table = ConnectorTableIdentity {
                    instance_id: source_instance,
                    namespace: occurrence.namespace_at_binding.clone().into(),
                    table: occurrence.relation_at_binding.clone().into(),
                };
                let live = source
                    .binding()
                    .metadata()
                    .capture_table_object_binding(
                        novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                            table: source_table.clone(),
                            resolution:
                                novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                            selector:
                                novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                            context: request.context().clone(),
                        },
                    )
                    .map_err(|e| conflict(e.to_string()))?;
                validate_readmission_capture(&source_table, &live, &occurrence.object_id)
                    .map_err(conflict)?;
            }
        }
        if matches!(
            phase,
            novarocks_mv_application::management::ManagementObservationPhase::RegistrationRequired(
                _
            )
        ) {
            self.register_incarnation(&mut state, &lease, &documents, &observed, request.context())
                .map_err(conflict)?;
            let registered_request = keyed_request(
                &documents,
                table.clone(),
                binding.object_id.clone(),
                request.context().clone().after_external_effect(),
            );
            let pending=state.begin_current_observation(novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes())).map_err(|e|conflict(format!("begin registered MV observation: {e:?}")))?;
            let (_, view) = state
                .complete_keyed_current_observation(pending, &documents, registered_request.clone())
                .map_err(|e| conflict(format!("complete registered MV observation: {e:?}")))?;
            self.entrance
                .resolve_pending_slot_changes(&documents, registered_request.clone())
                .map_err(|e| conflict(e.to_string()))?;
            observed = decode_current_keyed_documents(
                view,
                request.catalog().clone(),
                request.decode_budget(),
            )
            .map_err(|e| conflict(e.to_string()))?;
        } else if convergence.reinstall_current_projection {
            let pending=state.begin_current_observation(novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes())).map_err(|e|conflict(format!("begin resolved MV observation: {e:?}")))?;
            let (_, view) = state
                .complete_keyed_current_observation(pending, &documents, observation)
                .map_err(|e| conflict(format!("complete resolved MV observation: {e:?}")))?;
            observed = decode_current_keyed_documents(
                view,
                request.catalog().clone(),
                request.decode_budget(),
            )
            .map_err(|e| conflict(e.to_string()))?;
        }
        validate_current_publication_carrier(self.published, &observed).map_err(conflict)?;
        let admission = self
            .entrance
            .install_observed_target(
                &state,
                observed.management_dependencies(lease.control_runtime_id()),
            )
            .map_err(|e| conflict(format!("install MV admission: {e:?}")))?;
        Ok((observed, admission))
    }
}

/// One admitted MV publication: the management lease it commits under and the
/// provider admission its declaration is built from.
///
/// The lease is held for the whole publication. Dropping it without an
/// explicit terminal records `CommitUnknown`, which is the conservative answer
/// when nobody could say whether the commit happened.
pub(crate) struct AdmittedMvPublication {
    management: ManagementEntranceLease,
    admission: ConnectorDocumentManagementAdmission,
    /// The exact target and incarnation this publication took responsibility
    /// under, so the responsibility it dispatches cannot name another.
    managed_target: ManagedMvTarget,
    incarnation: ProcessIncarnation,
    /// The exact D/L generation the entrance admitted this publication
    /// against. P references both revisions, so they are retained here rather
    /// than re-read at commit time, where they could name a later generation
    /// than the one this publication was admitted for.
    source_revision: MvAcceleratorSourceRevision,
    /// Logical cardinality of that same admitted P, not physical file totals.
    previous_logical_rows: Option<u64>,
    definition: DefinitionDocument,
    interpretation: InterpretationDocument,
    versions: novarocks_mv_application::persistence::keyed_documents::MvKeyedVersions,
    entrance: ManagementEntrance,
    repartitioned: bool,
}

impl AdmittedMvPublication {
    pub(crate) const fn admission(&self) -> &ConnectorDocumentManagementAdmission {
        &self.admission
    }

    /// Reobserve provider Current after computation, while this entrance still
    /// excludes conflicting local management writes. The installed projection
    /// alone cannot detect an external change to D, L, or P at the same main.
    /// C is independently mutable and is never copied into this publication.
    pub(crate) fn recheck_current_dependencies(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        use novarocks_spi::connector::{
            ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
            ConnectorTableResolution,
        };

        // Computation and staging have completed. Reusing the admitted
        // request scope would replay its pre-compute metadata cache instead
        // of observing provider Current again.
        let context = context.clone().after_external_effect();
        let binding = planning_lease
            .binding()
            .metadata()
            .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
                table: self.source_revision.target.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: context.clone(),
            })
            .map_err(|error| format!("rebind Current MV publication target: {error}"))?;
        self.validate_current_dependency_binding(&binding)?;
        let document_lease = planning_lease
            .derive_document_storage_lease()
            .map_err(|error| format!("derive Current MV publication document lease: {error}"))?;
        let request = keyed_request(
            &document_lease,
            self.source_revision.target.clone(),
            binding.object_id,
            context,
        );
        let (documents,_)=novarocks_mv_application::persistence::keyed_documents::observe_current_keyed_documents_with_management(
            &document_lease,request,novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),&self.entrance)
            .map_err(|e|format!("reobserve Current MV publication documents: {e}"))?;
        self.validate_current_dependency_documents(&documents)
    }

    /// Reobserve the same admitted target and D/L/P through the original
    /// asynchronous provider authors. The caller retains this publication and
    /// its original future; no Current/StateStore authority is manufactured.
    pub(crate) async fn recheck_current_dependencies_async(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
        metadata_cpu: &CurrentProjectionMetadataCpu,
        registration: &CurrentProjectionRegistration,
    ) -> Result<(), super::application::MvApplicationError> {
        use super::application::{MvApplicationError, MvApplicationErrorKind};
        use novarocks_spi::connector::{
            ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
            ConnectorTableResolution,
        };
        let connector =
            |error| MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error);
        let domain =
            |message: String| MvApplicationError::new(MvApplicationErrorKind::Engine, message);
        let context = context.clone().after_external_effect();
        let binding = registration
            .capture
            .capture(
                planning_lease,
                metadata_cpu,
                ConnectorTableObjectCaptureRequest {
                    table: self.source_revision.target.clone(),
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    selector: ConnectorTableObjectSelector::Current,
                    context: context.clone(),
                },
            )
            .await
            .map_err(current_metadata_cpu::CaptureFailure::into_application)?;
        self.validate_current_dependency_binding(&binding)
            .map_err(domain)?;
        let document_lease = planning_lease
            .derive_document_storage_lease()
            .map_err(connector)?;
        let request = keyed_request(
            &document_lease,
            self.source_revision.target.clone(),
            binding.object_id,
            context,
        );
        let (documents, _) = novarocks_mv_application::persistence::keyed_documents::observe_current_keyed_documents_with_management_async(
            &document_lease, request,
            novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
            &self.entrance,
        ).await.map_err(|error| {
            // The producer owns every Connector/Keyed failure fact. Pure
            // document validation remains a domain refusal without a source.
            let projection = novarocks_mv_application::readiness::MvProjectionError::from(error);
            let failure = domain(projection.to_string());
            match projection.original_failure() {
                Some(original) => failure.with_original_failure(original.clone()),
                None => failure,
            }
        })?;
        self.validate_current_dependency_documents(&documents)
            .map_err(domain)
    }

    fn validate_current_dependency_binding(
        &self,
        binding: &novarocks_spi::connector::ConnectorTableObjectBinding,
    ) -> Result<(), String> {
        if binding.metadata.identity != self.source_revision.target
            || binding.object_id != self.source_revision.target_object_id
        {
            return Err("MV publication target changed after computation".to_string());
        }
        Ok(())
    }

    fn validate_current_dependency_documents(
        &self,
        documents: &novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
    ) -> Result<(), String> {
        if documents.management_target != self.managed_target
            || documents.deployment_owner != self.source_revision.deployment_owner
            || documents.process_incarnation != self.incarnation
        {
            return Err("MV publication ownership changed after computation".into());
        }
        if documents.versions.definition_revision() != self.source_revision.definition_revision
            || documents.versions.interpretation_revision()
                != self.source_revision.interpretation_revision
            || documents.publication_revision != self.source_revision.publication_revision
        {
            return Err(
                "MV definition, interpretation or publication changed after computation".into(),
            );
        }
        Ok(())
    }

    /// Take responsibility for this publication's commit.
    ///
    /// Called immediately before the provider call that commits, because that
    /// is the moment after which nobody can say the effect did not happen.
    /// Until then a failed statement leaves the target untouched and still
    /// admitting; afterwards every path must reach an explicit terminal.
    pub(crate) fn mark_dispatched(
        &mut self,
        publication_id: LakePublicationId,
    ) -> Result<(), String> {
        self.management
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes(publication_id.to_bytes()),
                self.managed_target.clone(),
                self.incarnation.clone(),
                EffectScope::CATALOG_COMMIT,
                now_management_timestamp()?,
            ))
            .map_err(|error| format!("dispatch the MV publication effect: {error:?}"))
    }

    pub(crate) const fn definition(&self) -> &DefinitionDocument {
        &self.definition
    }

    /// A managed repartition's provider preview supplies the exact opaque
    /// partition identities L will bind after the same-session target commit.
    /// The prior L remains the entrance dependency until that commit settles.
    pub(crate) fn set_repartition_partitioning(
        &mut self,
        preview: &novarocks_spi::connector::ConnectorManagedPartitionSpecPreview,
    ) -> Result<(), String> {
        use novarocks_mv_application::persistence::codec::TargetPartitionFieldBinding;
        use novarocks_mv_application::persistence::identity::PartitionSpecVersion;

        if self.repartitioned {
            return Err("MV repartition interpretation was already prepared".to_string());
        }
        self.interpretation.target.partition_spec_version =
            PartitionSpecVersion::try_new(preview.exact_partition_spec_version().to_vec())
                .map_err(|error| format!("bind MV repartition spec version: {error}"))?;
        self.interpretation.target.partition_fields = preview
            .exact_partition_fields()
            .iter()
            .map(TargetPartitionFieldBinding::try_from)
            .collect::<Result<_, _>>()?;
        self.repartitioned = true;
        Ok(())
    }

    /// Complete P from the pinned watermark and the target's post-write
    /// logical cardinality. A repartition publishes its new L in the same set.
    ///
    /// The set is built here rather than by the caller because P's two
    /// references are to the D and L this publication was admitted against,
    /// and those are exactly what this lease holds.
    pub(crate) fn publication_document_set(
        &self,
        publication_id: LakePublicationId,
        inputs: &MvPublicationInputs,
        logical_result_rows: Option<u64>,
    ) -> Result<novarocks_spi::connector::KeyedPublication, String> {
        let versions = if self.repartitioned {
            novarocks_mv_application::persistence::keyed_documents::build_keyed_versions(
                &self.definition,
                &self.interpretation,
                self.versions.binding_document.clone(),
            )
            .map_err(|e| e.to_string())?
        } else {
            self.versions.clone()
        };
        if !self.repartitioned
            && (versions.definition_revision() != self.source_revision.definition_revision
                || versions.interpretation_revision()
                    != self.source_revision.interpretation_revision)
        {
            return Err("MV publication D/L identities changed after admission".into());
        }
        let publication =
            novarocks_mv_application::persistence::codec::keyed::KeyedPublicationDocument {
                publication_prepared_at_ms: now_unix_millis()?,
                publication_id:
                    novarocks_mv_application::persistence::identity::PublicationIdentity::try_new(
                        publication_id.to_bytes().to_vec(),
                    )
                    .map_err(|e| e.to_string())?,
                definition_revision: versions.definition_revision(),
                interpretation_revision: versions.interpretation_revision(),
                inputs: inputs.inputs().to_vec(),
                logical_result_rows,
            };
        novarocks_mv_application::persistence::keyed_documents::keyed_publication_document_set(
            &versions,
            &self.definition,
            &publication,
        )
        .map_err(|e| format!("encode MV publication record: {e}"))
    }

    /// Close either the local admission or the dispatched publication with
    /// its actual outcome. A pre-dispatch refusal owns no external effect.
    pub(crate) fn record_terminal(self, disposition: EffectDisposition) -> Result<(), String> {
        self.management
            .record_publication_terminal(disposition)
            .map_err(|error| {
                format!("record the MV publication terminal responsibility: {error:?}")
            })
    }
}

/// One admitted data-producing publication.
///
/// A publication that writes rows knows its whole input watermark before it
/// runs, and only its result afterwards. Pairing the lease with the frozen
/// watermark keeps the two halves of P together from admission to commit, so
/// the commit point has nothing left to look up.
pub(crate) struct AdmittedMvDataPublication {
    publication: AdmittedMvPublication,
    inputs: MvPublicationInputs,
}

impl AdmittedMvDataPublication {
    pub(crate) const fn previous_logical_rows(&self) -> Option<u64> {
        self.publication.previous_logical_rows
    }

    /// Freeze the watermark against the D this publication was admitted for.
    pub(crate) fn try_new(
        publication: AdmittedMvPublication,
        revision_by_occurrence: &BTreeMap<u32, ConnectorExactSemanticRevision>,
    ) -> Result<Self, String> {
        let inputs = MvPublicationInputs::freeze(publication.definition(), revision_by_occurrence)?;
        Ok(Self {
            publication,
            inputs,
        })
    }

    /// Take responsibility for this publication's commit, immediately before
    /// the provider call that performs it.
    pub(crate) fn mark_dispatched(
        &mut self,
        publication_id: LakePublicationId,
    ) -> Result<(), String> {
        self.publication.mark_dispatched(publication_id)
    }

    pub(crate) fn recheck_current_dependencies(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        self.publication
            .recheck_current_dependencies(planning_lease, context)
    }

    pub(crate) async fn recheck_current_dependencies_async(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
        metadata_cpu: &CurrentProjectionMetadataCpu,
        registration: &CurrentProjectionRegistration,
    ) -> Result<(), super::application::MvApplicationError> {
        self.publication
            .recheck_current_dependencies_async(planning_lease, context, metadata_cpu, registration)
            .await
    }

    /// The P-only document set this publication commits.
    pub(crate) fn publication_document_set(
        &self,
        publication_id: LakePublicationId,
        logical_result_rows: Option<u64>,
    ) -> Result<novarocks_spi::connector::KeyedPublication, String> {
        self.publication
            .publication_document_set(publication_id, &self.inputs, logical_result_rows)
    }

    pub(crate) fn record_terminal(self, disposition: EffectDisposition) -> Result<(), String> {
        self.publication.record_terminal(disposition)
    }
}

impl std::fmt::Debug for AdmittedMvDataPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdmittedMvDataPublication")
            .field("inputs", &self.inputs)
            .finish_non_exhaustive()
    }
}

/// The preparation Action owns this receiving slot before polling admission.
/// Its future only borrows the slot; neither a refusal nor an unwinding foreign
/// admission call can drop the only granted management lease.
///
/// The Action must retain the slot through the original future's actual exit
/// (including its raw-health path). This slot does not grant publication or
/// Current authority: only the original entrance and document lease do that.
pub(crate) struct MvPublicationAdmissionReceiving {
    input: std::sync::OnceLock<MvPublicationAdmissionInput>,
    document: std::sync::OnceLock<MvPublicationAdmissionDocument>,
    body: std::sync::Mutex<MvPublicationAdmissionBody>,
}

struct MvPublicationAdmissionInput {
    entrance: ManagementEntrance,
    planning: ConnectorControlPlanningLease,
    projection: novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
    publication_id: LakePublicationId,
    context: ConnectorRequestContext,
}

struct MvPublicationAdmissionDocument {
    lease: ConnectorDocumentStorageLease,
    request: ConnectorDocumentManagementAdmissionRequest,
}

#[derive(Default)]
struct MvPublicationAdmissionBody {
    claimed: bool,
    management: Option<ManagementEntranceLease>,
    prepared: Option<MvPublicationAdmissionParts>,
    result: Option<Result<AdmittedMvPublication, super::application::MvApplicationError>>,
    taken: bool,
}

// All potentially failing or allocating preparation precedes the final move
// of the original management lease. Finishing these parts only moves fields.
struct MvPublicationAdmissionParts {
    managed_target: ManagedMvTarget,
    incarnation: ProcessIncarnation,
    source_revision: MvAcceleratorSourceRevision,
    previous_logical_rows: Option<u64>,
    definition: DefinitionDocument,
    interpretation: InterpretationDocument,
    versions: novarocks_mv_application::persistence::keyed_documents::MvKeyedVersions,
    entrance: ManagementEntrance,
}

impl MvPublicationAdmissionParts {
    fn finish(
        self,
        management: ManagementEntranceLease,
        admission: ConnectorDocumentManagementAdmission,
    ) -> AdmittedMvPublication {
        AdmittedMvPublication {
            management,
            admission,
            managed_target: self.managed_target,
            incarnation: self.incarnation,
            source_revision: self.source_revision,
            previous_logical_rows: self.previous_logical_rows,
            definition: self.definition,
            interpretation: self.interpretation,
            versions: self.versions,
            entrance: self.entrance,
            repartitioned: false,
        }
    }
}

fn publication_admission_protocol(message: &'static str) -> super::application::MvApplicationError {
    super::application::MvApplicationError::new(
        super::application::MvApplicationErrorKind::Engine,
        message,
    )
}

impl MvPublicationAdmissionReceiving {
    pub(crate) fn new() -> Self {
        Self {
            input: std::sync::OnceLock::new(),
            document: std::sync::OnceLock::new(),
            body: std::sync::Mutex::new(MvPublicationAdmissionBody::default()),
        }
    }

    fn claim(&self) -> Result<(), super::application::MvApplicationError> {
        let mut body = self
            .body
            .lock()
            .expect("MV publication admission receiving");
        if body.claimed {
            return Err(publication_admission_protocol(
                "MV publication admission receiving was already claimed",
            ));
        }
        body.claimed = true;
        Ok(())
    }

    fn refuse(&self, error: super::application::MvApplicationError) {
        let mut body = self
            .body
            .lock()
            .expect("MV publication admission receiving");
        // Do not retire the management turn while the failure is being passed
        // back to the Action. Its stable slot remains the only lease owner.
        assert!(body.result.is_none() && !body.taken);
        body.result = Some(Err(error));
    }

    pub(crate) fn take_admitted_in(
        &self,
        target: &mut Option<AdmittedMvPublication>,
    ) -> Result<(), super::application::MvApplicationError> {
        if target.is_some() {
            return Err(publication_admission_protocol(
                "MV publication admission target is not empty",
            ));
        }
        let mut body = self
            .body
            .lock()
            .expect("MV publication admission receiving");
        match body.result.as_ref() {
            Some(Err(error)) => return Err(error.clone()),
            None => {
                return Err(publication_admission_protocol(
                    "MV publication admission has no unconsumed result",
                ));
            }
            Some(Ok(_)) => {}
        }
        // No fallible validation, foreign call, or destructor follows take.
        let Some(Ok(admitted)) = body.result.take() else {
            unreachable!("checked MV admission result")
        };
        *target = Some(admitted);
        body.taken = true;
        Ok(())
    }
}

/// Borrow the Action's original receiving slot and cancellation/deadline
/// future. The only await is the original management FIFO. Provider document
/// admission is a pure capability call, with its original request and lease
/// already retained before entering the foreign author.
pub(crate) async fn admit_mv_publication_async_in(
    entrance: &ManagementEntrance,
    planning: &ConnectorControlPlanningLease,
    projection: &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
    publication_id: LakePublicationId,
    context: &ConnectorRequestContext,
    cancelled: impl std::future::Future<Output = ()> + Send,
    receiving: &MvPublicationAdmissionReceiving,
) -> Result<(), super::application::MvApplicationError> {
    use super::application::{MvApplicationError, MvApplicationErrorKind};
    receiving.claim()?;
    let input = MvPublicationAdmissionInput {
        entrance: entrance.clone(),
        planning: planning.clone(),
        projection: projection.clone(),
        publication_id,
        context: context.clone(),
    };
    if receiving.input.set(input).is_err() {
        unreachable!("single claimed MV admission input")
    }
    let input = receiving.input.get().expect("installed MV admission input");
    let run: Result<(), super::application::MvApplicationError> = async {
        let catalog = input
            .planning
            .binding()
            .catalog_handle()
            .map_err(|error| {
                MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
            })?
            .clone();
        let source = input.projection.facts.source_revision();
        let request = ManagementRequest::try_new(
            catalog.clone(),
            source.target.clone(),
            Some(source.target_object_id.clone()),
            ConnectorDocumentManagementOperation::Publication,
            Some(
                input
                    .projection
                    .facts
                    .management_dependencies(input.planning.control_runtime_id()),
            ),
            EffectScope::CATALOG_COMMIT,
        )
        .map_err(|error| {
            MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                format!("build the MV publication admission request: {error:?}"),
            )
        })?;
        let management = input
            .entrance
            .acquire_async(request, cancelled)
            .await
            .map_err(|error| {
                MvApplicationError::new(
                    MvApplicationErrorKind::Engine,
                    format!("admit the MV publication through the management entrance: {error:?}"),
                )
            })?;
        // This is the first operation after the gate returns its lease. The
        // subsequent document author cannot unwind through a temporary owner.
        receiving
            .body
            .lock()
            .expect("MV publication admission receiving")
            .management = Some(management);
        let document_lease = input
            .planning
            .derive_document_storage_lease()
            .map_err(|error| {
                MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
            })?;
        let request = ConnectorDocumentManagementAdmissionRequest::try_new(
            document_lease.owner().clone(),
            catalog.clone(),
            ConnectorMutationOperationId::from_bytes(input.publication_id.to_bytes()),
            source.target.clone(),
            Some(source.target_object_id.clone()),
            ConnectorDocumentManagementOperation::Publication,
            input.context.clone(),
        )
        .map_err(|error| {
            MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
        })?;
        let managed_target = ManagedMvTarget::try_new(
            catalog,
            source.target.clone(),
            source.target_object_id.clone(),
        )
        .map_err(|error| {
            MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                format!("name the MV publication target: {error:?}"),
            )
        })?;
        let parts = MvPublicationAdmissionParts {
            managed_target,
            incarnation: input.entrance.incarnation().clone(),
            source_revision: source.clone(),
            previous_logical_rows: input
                .projection
                .facts
                .publication()
                .map_or(Some(0), |publication| publication.logical_result_rows),
            definition: input.projection.facts.definition().clone(),
            interpretation: input.projection.facts.interpretation().clone(),
            versions: input.projection.facts.versions().clone(),
            entrance: input.entrance.clone(),
        };
        receiving
            .body
            .lock()
            .expect("MV publication admission receiving")
            .prepared = Some(parts);
        if receiving
            .document
            .set(MvPublicationAdmissionDocument {
                lease: document_lease,
                request,
            })
            .is_err()
        {
            unreachable!("single claimed MV document admission")
        }
        let document = receiving
            .document
            .get()
            .expect("installed MV document admission");
        // Foreign provider callback and all error projection are outside the
        // receiving lock. On panic the original input, request and lease stay.
        let admission = document
            .lease
            .admit_management(document.request.clone())
            .map_err(|error| {
                MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
            })?;
        let mut body = receiving
            .body
            .lock()
            .expect("MV publication admission receiving");
        let parts = body.prepared.take().expect("prepared MV admission parts");
        let management = body
            .management
            .take()
            .expect("original MV management lease");
        body.result = Some(Ok(parts.finish(management, admission)));
        Ok(())
    }
    .await;
    if let Err(error) = &run {
        receiving.refuse(error.clone());
    }
    run
}

/// Admit one MV publication against the exact installed generation.
///
/// The entrance refuses a publication whose target object or document
/// dependencies no longer match what is installed, so a long computation
/// cannot publish onto a target that moved underneath it.
pub(crate) fn admit_mv_publication(
    entrance: &ManagementEntrance,
    planning_lease: &ConnectorControlPlanningLease,
    projection: &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
    publication_id: LakePublicationId,
    context: &ConnectorRequestContext,
) -> Result<AdmittedMvPublication, String> {
    let catalog_handle = planning_lease
        .binding()
        .catalog_handle()
        .map_err(|error| format!("bind MV publication catalog generation: {error}"))?
        .clone();
    let source = projection.facts.source_revision();
    let table = source.target.clone();
    let object_id = source.target_object_id.clone();
    let operation_id = ConnectorMutationOperationId::from_bytes(publication_id.to_bytes());
    let cancelled_context = context.clone();
    let management = entrance
        .acquire(
            ManagementRequest::try_new(
                catalog_handle.clone(),
                table.clone(),
                Some(object_id.clone()),
                ConnectorDocumentManagementOperation::Publication,
                Some(
                    projection
                        .facts
                        .management_dependencies(planning_lease.control_runtime_id()),
                ),
                EffectScope::CATALOG_COMMIT,
            )
            .map_err(|error| format!("build the MV publication admission request: {error:?}"))?,
            || cancelled_context.is_cancelled(),
        )
        .map_err(|error| {
            format!("admit the MV publication through the management entrance: {error:?}")
        })?;
    let document_lease = planning_lease
        .derive_document_storage_lease()
        .map_err(|error| format!("derive MV publication document lease: {error}"))?;
    let admission = document_lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                document_lease.owner().clone(),
                catalog_handle,
                operation_id,
                table,
                Some(object_id),
                ConnectorDocumentManagementOperation::Publication,
                context.clone(),
            )
            .map_err(|error| format!("build MV publication document admission: {error}"))?,
        )
        .map_err(|error| format!("admit MV publication document management: {error}"))?;
    Ok(MvPublicationAdmissionParts {
        managed_target: ManagedMvTarget::try_new(
            planning_lease
                .binding()
                .catalog_handle()
                .map_err(|error| format!("bind MV publication catalog generation: {error}"))?
                .clone(),
            source.target.clone(),
            source.target_object_id.clone(),
        )
        .map_err(|error| format!("name the MV publication target: {error:?}"))?,
        incarnation: entrance.incarnation().clone(),
        source_revision: source.clone(),
        previous_logical_rows: projection
            .facts
            .publication()
            .map_or(Some(0), |publication| publication.logical_result_rows),
        definition: projection.facts.definition().clone(),
        interpretation: projection.facts.interpretation().clone(),
        versions: projection.facts.versions().clone(),
        entrance: entrance.clone(),
    }
    .finish(management, admission))
}

#[cfg(test)]
#[path = "staged_create/admitted_current_tests.rs"]
mod admitted_current_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    struct RefusingStage {
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        entrance: ManagementEntrance,
        mode: u8,
        prepares: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl novarocks_spi::connector::ConnectorStagedCreate for RefusingStage {
        fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
            self.incarnation
        }
        fn prepare(
            &self,
            request: novarocks_spi::connector::ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, novarocks_spi::connector::ConnectorError>
        {
            use novarocks_spi::connector::*;
            self.prepares
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                self.entrance.management_phase(&request.table),
                novarocks_mv_application::management::MvManagementPhase::AwaitingCreateBinding
            );
            let failure = ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                "injected stage failure",
            );
            match self.mode {
                0 => Ok(ConnectorStagedCreatePrepareOutcome::AppliedButUnusable {
                    receipt: ConnectorStagedCreateReceipt::try_new(
                        request.owner,
                        request.operation_id,
                        ConnectorStagedCreateReceiptPhase::Prepared,
                        ExternalMutationEffect::Applied,
                        Bytes::new(),
                    )
                    .unwrap(),
                    finalization: ExternalMutationFinalization::Failed(failure),
                }),
                1 => Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "injected provider dispatch failure",
                )),
                2 => Ok(ConnectorStagedCreatePrepareOutcome::Conflict { failure }),
                3 => Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }),
                _ => unreachable!(),
            }
        }
        fn plan_write(
            &self,
            _: novarocks_spi::connector::ConnectorStagedWritePlanningRequest,
        ) -> Result<
            novarocks_spi::connector::ConnectorStagedWritePlanningBinding,
            novarocks_spi::connector::ConnectorError,
        > {
            panic!("failed stage cannot plan writes")
        }
        fn bind_write(
            &self,
            _: ConnectorStagedTableHandle,
            _: ConnectorStagedWriteProof,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            panic!("failed stage cannot bind writes")
        }
        fn publish(
            &self,
            _: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, novarocks_spi::connector::ConnectorError>
        {
            panic!("failed stage cannot publish")
        }
        fn abort(
            &self,
            _: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, novarocks_spi::connector::ConnectorError>
        {
            panic!("failed stage cannot abort")
        }
        fn adjudicate_publication(
            &self,
            _: novarocks_spi::connector::ConnectorStagedCreatePublicationAdjudicationRequest,
        ) -> Result<
            novarocks_spi::connector::ConnectorStagedCreatePublicationAdjudicationOutcome,
            novarocks_spi::connector::ConnectorError,
        > {
            panic!("failed stage is not final publication")
        }
    }
    #[test]
    fn a_non_negative_stage_failure_retains_the_unbound_create_responsibility() {
        use super::super::test_admission::{AdmittingManagement, catalog_properties};
        use novarocks_mv_application::management::{
            DeploymentOwner, ManagementAdmissionError, MvManagementPhase,
        };
        use novarocks_spi::connector::*;
        for mode in 0..4 {
            let instance_id = ConnectorInstanceId::parse("iceberg").unwrap();
            let base = novarocks_catalog_application::test_support::test_control_binding_for(
                instance_id.clone(),
                8,
            );
            let descriptor = base.descriptor().clone();
            let incarnation = base.incarnation();
            let entrance = ManagementEntrance::new(
                DeploymentOwner::parse("deployment-test").unwrap(),
                ProcessIncarnation::parse("process-test").unwrap(),
            );
            let prepares = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let stage = Arc::new(RefusingStage {
                descriptor: descriptor.clone(),
                incarnation,
                entrance: entrance.clone(),
                mode,
                prepares: prepares.clone(),
            });
            let documents = ConnectorDocumentStorageBinding::try_new(
                descriptor.clone(),
                incarnation,
                None,
                Some(Arc::new(AdmittingManagement {
                    descriptor: descriptor.clone(),
                    incarnation,
                })),
            )
            .unwrap();
            let binding = ConnectorControlBinding::try_new_with_all_capabilities_and_staged_create(
                descriptor,
                incarnation,
                base.metadata().clone(),
                base.planning().clone(),
                base.execution_distribution().clone(),
                None,
                None,
                None,
                Some(stage),
                None,
                None,
            )
            .unwrap()
            .with_catalog_properties(catalog_properties(&instance_id))
            .unwrap()
            .try_with_document_storage(Some(documents))
            .unwrap();
            let planning_lease = ConnectorControlPlanningLease::new(Arc::new(binding), || {});
            let catalog = planning_lease.binding().catalog_handle().unwrap().clone();
            let table = ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("new_mv"),
            };
            let operation_id = uuid::Uuid::now_v7();
            let intent = CreateIntent::try_new(
                catalog.clone(),
                table.clone(),
                EffectIdentity::from_bytes(*operation_id.as_bytes()),
            )
            .unwrap();
            let context = ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
                ConnectorStopOwner::new().view(),
                4096,
                65536,
            )
            .unwrap();
            let result = stage_mv_create_target(
                &entrance,
                StageMvCreateRequest {
                    planning_lease,
                    table: table.clone(),
                    columns: vec![],
                    partitioning: vec![],
                    properties: BTreeMap::new(),
                    operation_id,
                    context: &context,
                },
            );
            assert!(result.is_err());
            assert_eq!(prepares.load(std::sync::atomic::Ordering::SeqCst), 1);
            let replacement = CreateIntent::try_new(
                catalog,
                table.clone(),
                EffectIdentity::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
            )
            .unwrap();
            let next = || {
                entrance.acquire(
                    ManagementRequest::for_create_intent(
                        replacement.clone(),
                        EffectScope::CATALOG_COMMIT,
                    ),
                    || false,
                )
            };
            if mode < 2 {
                assert_eq!(
                    entrance.management_phase(&table),
                    MvManagementPhase::AwaitingCreateBinding
                );
                assert!(entrance.begin_unbound_create_observation(&intent).is_ok());
                assert_eq!(
                    next().err().unwrap(),
                    ManagementAdmissionError::EffectUnsettled
                );
            } else {
                assert_eq!(
                    entrance.management_phase(&table),
                    MvManagementPhase::NotObserved
                );
                assert!(entrance.begin_unbound_create_observation(&intent).is_err());
                assert!(next().is_ok());
            }
        }
    }
    fn admission_wait_fixture() -> (
        ManagementEntrance,
        ConnectorControlPlanningLease,
        novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
        ConnectorRequestContext,
    ) {
        use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
        use novarocks_mv_application::product::MvTarget;
        use novarocks_spi::connector::*;
        let facts = super::super::test_admission::sample_keyed_projection(
            MvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(17),
        );
        let source = facts.source_revision();
        let entrance = ManagementEntrance::new(
            source.deployment_owner.clone(),
            source.process_incarnation.clone(),
        );
        let instance = source.target.instance_id.clone();
        let binding = novarocks_catalog_application::test_support::test_control_binding_for(
            instance.clone(),
            8,
        )
        .with_catalog_properties(super::super::test_admission::catalog_properties(&instance))
        .unwrap();
        let planning = ConnectorControlPlanningLease::new(Arc::new(binding), || {});
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            4096,
            65536,
        )
        .unwrap();
        (
            entrance,
            planning,
            StoredKeyedMvProjection { mv_id: 1, facts },
            context,
        )
    }

    fn admission_blocker_request(
        planning: &ConnectorControlPlanningLease,
        projection: &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
    ) -> ManagementRequest {
        ManagementRequest::for_create_intent(
            CreateIntent::try_new(
                planning.binding().catalog_handle().unwrap().clone(),
                projection.facts.source_revision().target.clone(),
                EffectIdentity::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
            )
            .unwrap(),
            EffectScope::CATALOG_COMMIT,
        )
    }

    #[tokio::test]
    async fn publication_admission_wait_keeps_original_input_and_caller_cancellation() {
        use std::future::{Future, pending};
        use std::task::{Context, Poll};
        let (entrance, planning, projection, context) = admission_wait_fixture();
        let blocker = entrance
            .acquire_async(admission_blocker_request(&planning, &projection), pending())
            .await
            .unwrap();
        let receiving = MvPublicationAdmissionReceiving::new();
        let publication_id = LakePublicationId::try_from_uuid(uuid::Uuid::now_v7()).unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let mut future = std::pin::pin!(admit_mv_publication_async_in(
            &entrance,
            &planning,
            &projection,
            publication_id,
            &context,
            async move { stopped.await.unwrap() },
            &receiving,
        ));
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            receiving.input.get().unwrap().publication_id,
            publication_id
        );
        assert_eq!(&receiving.input.get().unwrap().projection, &projection);
        assert!(receiving.body.lock().unwrap().management.is_none());
        assert!(receiving.document.get().is_none());
        // Reentry cannot make a second ticket or replace the original input.
        assert!(
            admit_mv_publication_async_in(
                &entrance,
                &planning,
                &projection,
                publication_id,
                &context,
                pending(),
                &receiving,
            )
            .await
            .is_err()
        );
        stop.send(()).unwrap();
        let original = future.await.unwrap_err();
        let mut target = None;
        assert_eq!(
            receiving.take_admitted_in(&mut target).unwrap_err(),
            original
        );
        assert!(target.is_none());
        assert!(receiving.document.get().is_none());
        drop(blocker);
        // The cancelled admission's original FIFO ticket has really retired.
        let next = entrance
            .acquire_async(admission_blocker_request(&planning, &projection), pending())
            .await
            .unwrap();
        drop(next);
    }

    #[tokio::test]
    async fn publication_admission_refusal_retains_same_cause_and_granted_turn() {
        use super::super::application::{MvApplicationError, MvApplicationErrorKind};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let (entrance, planning, projection, _) = admission_wait_fixture();
        let granted = entrance
            .acquire_async(
                admission_blocker_request(&planning, &projection),
                std::future::pending(),
            )
            .await
            .unwrap();
        let receiving = MvPublicationAdmissionReceiving::new();
        receiving.claim().unwrap();
        // Exercise lease custody using a real, undispatched entrance turn;
        // this does not fabricate a publication admission or installed Current.
        receiving.body.lock().unwrap().management = Some(granted);
        let failure = MvApplicationError::from_connector(
            MvApplicationErrorKind::Engine,
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "original admission refusal",
            ),
        );
        let cause = failure.original_failure().unwrap().clone();
        receiving.refuse(failure);
        let mut next = entrance
            .request(
                admission_blocker_request(&planning, &projection),
                novarocks_mv_application::activity::MvActivityOwner::Create,
            )
            .unwrap();
        assert!(next.try_acquire().unwrap().is_none());
        let mut target = None;
        let observed = receiving.take_admitted_in(&mut target).unwrap_err();
        assert_eq!(observed.original_failure(), Some(&cause));
        assert!(receiving.claim().is_err());
        assert!(next.try_acquire().unwrap().is_none());
        drop(receiving);
        let actual_next = next
            .try_acquire()
            .unwrap()
            .expect("lease exits only with its receiving owner");
        drop(actual_next);
    }
}
