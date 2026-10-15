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
        Err(error) => settle_known_uncommitted(management, "stage", error.to_string()),
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

#[allow(clippy::too_many_arguments)]
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
        if binding.metadata.identity != table {
            return Err(conflict("MV provider bound a different target".into()));
        }
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
                if let MvConvergence::MetadataOnlyCurrent { expected } = &self.convergence {
                    if !metadata_only_current_matches(expected, &observed) {
                        return Err(conflict(
                            "MV carrier changed during metadata-only Current renewal".into(),
                        ));
                    }
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
                if live.metadata.identity!=source_table || !novarocks_mv_application::persistence::exact_revision::persisted_object_names(&occurrence.object_id,&live.object_id).map_err(|e|conflict(e.to_string()))? {
                    return Err(conflict("same-name relation was rebuilt with a different object identity".into()));
                }
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
        if let Some(published) = self.published {
            if observed.publication_snapshot != Some(published.snapshot_id) {
                return Err(conflict(
                    "MV Current observation did not retain the committed publication carrier"
                        .into(),
                ));
            }
        }
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
        if binding.metadata.identity != self.source_revision.target
            || binding.object_id != self.source_revision.target_object_id
        {
            return Err("MV publication target changed after computation".to_string());
        }
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

    /// Complete P from the watermark this refresh pinned and what its writers
    /// produced. A repartition publishes its new L beside P in the same set.
    ///
    /// The set is built here rather than by the caller because P's two
    /// references are to the D and L this publication was admitted against,
    /// and those are exactly what this lease holds.
    pub(crate) fn publication_document_set(
        &self,
        publication_id: LakePublicationId,
        inputs: &MvPublicationInputs,
        logical_result_rows: u64,
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
                logical_result_rows: Some(logical_result_rows),
            };
        novarocks_mv_application::persistence::keyed_documents::keyed_publication_document_set(
            &versions,
            &self.definition,
            &publication,
        )
        .map_err(|e| format!("encode MV publication record: {e}"))
    }

    /// Close the publication's responsibility with the outcome the provider
    /// actually reported.
    pub(crate) fn record_terminal(self, disposition: EffectDisposition) -> Result<(), String> {
        record_terminal(self.management, disposition)
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

    /// The P-only document set this publication commits.
    pub(crate) fn publication_document_set(
        &self,
        publication_id: LakePublicationId,
        logical_result_rows: u64,
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
    Ok(AdmittedMvPublication {
        management,
        admission,
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
        definition: projection.facts.definition().clone(),
        interpretation: projection.facts.interpretation().clone(),
        versions: projection.facts.versions().clone(),
        entrance: entrance.clone(),
        repartitioned: false,
    })
}

#[cfg(test)]
#[path = "staged_create/admitted_current_tests.rs"]
mod admitted_current_tests;
