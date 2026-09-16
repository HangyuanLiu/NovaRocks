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
//! The publish writes no data, so the created target has no snapshot. That is
//! the never-published witness the document projection reads: a target with a
//! current snapshot and no publication is corruption, not a fresh MV.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_mv_application::management::{
    CreateIntent, EffectDisposition, EffectIdentity, EffectScope, ManagedMvTarget,
    ManagementEntrance, ManagementEntranceLease, ManagementRequest, ManagementTimestamp,
};
use novarocks_mv_application::persistence::create_documents::MvCreateDocuments;
use novarocks_mv_application::persistence::documents::create_document_set;
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentCreatePublicationIntent, ConnectorDocumentManagementAdmission,
    ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
    ConnectorDocumentStorageLease, ConnectorManagedObjectMarker, ConnectorPrepareDocumentsRequest,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorColumnDefinition, ConnectorControlPlanningLease,
    ConnectorMutationOperationId, ConnectorPartitionTransform, ConnectorRequestContext,
    ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
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
            || cancelled_context.cancellation().is_cancelled(),
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
    let prepare = staged_lease
        .prepare_document_managed_request(
            LakePublicationId::new_v7(),
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
        write: ConnectorStagedWriteProof,
        marker: ConnectorManagedObjectMarker,
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
        let prepared_documents = match prepare_create_documents(
            documents,
            prepared_target,
            admission,
            &document_lease,
            context,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                return settle_publish_known_uncommitted(
                    management,
                    format!("prepare MV CREATE documents: {error}"),
                );
            }
        };
        let intent = match ConnectorDocumentCreatePublicationIntent::try_new(
            &handle,
            prepared_documents,
            marker,
        ) {
            Ok(intent) => intent,
            Err(error) => {
                return settle_publish_known_uncommitted(
                    management,
                    format!("build MV CREATE publication intent: {error}"),
                );
            }
        };
        let outcome = staged_lease.publish(ConnectorStagedCreatePublishRequest {
            operation_id,
            handle,
            write,
            payload: ConnectorStagedCreatePublicationPayload::ApplicationDocuments(intent),
            context: context.clone(),
        });
        match outcome {
            Ok(ConnectorStagedCreatePublishOutcome::Applied { .. })
            | Ok(ConnectorStagedCreatePublishOutcome::NoOp { .. }) => {
                settle_published(management, catalog_handle, table, object_id)
            }
            Ok(ConnectorStagedCreatePublishOutcome::Conflict { failure })
            | Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure }) => {
                settle_publish_known_uncommitted(management, failure.message().to_string())
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
            Ok(ConnectorStagedCreateAbortOutcome::Aborted { .. })
            | Ok(ConnectorStagedCreateAbortOutcome::KnownUncommitted { .. }) => {
                record_terminal(management, EffectDisposition::KnownUncommitted)
            }
            Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown { failure, .. }) => Err(format!(
                "MV CREATE staged abort outcome is unknown: {}",
                failure.message()
            )),
            Err(error) => Err(format!("abort MV CREATE staged target: {error}")),
        }
    }
}

/// Encode D/L/C for this exact staged target and hand them to the provider.
fn prepare_create_documents(
    documents: &MvCreateDocuments,
    prepared_target: &novarocks_spi::connector::ConnectorPreparedCreateDocumentTarget,
    admission: ConnectorDocumentManagementAdmission,
    document_lease: &ConnectorDocumentStorageLease,
    context: &ConnectorRequestContext,
) -> Result<novarocks_spi::connector::document_storage::ConnectorPreparedDocumentSet, String> {
    let set = create_document_set(
        &documents.definition,
        &documents.interpretation,
        &documents.configuration,
        prepared_target,
    )
    .map_err(|error| error.to_string())?;
    let request = ConnectorPrepareDocumentsRequest::try_new(admission, set, context.clone())
        .map_err(|error| error.to_string())?;
    document_lease
        .prepare_documents(request)
        .map_err(|error| error.to_string())
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
        return StagedPublishOutcome::Unknown(error);
    }
    match record_terminal(management, EffectDisposition::KnownCommitted) {
        Ok(()) => StagedPublishOutcome::Published(object_id),
        // The create is committed either way; only the responsibility record
        // failed to close, which must not be reported as a failed create.
        Err(error) => StagedPublishOutcome::Unknown(error),
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

fn now_management_timestamp() -> Result<ManagementTimestamp, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_string())?;
    u64::try_from(now.as_millis())
        .map(ManagementTimestamp::from_unix_millis)
        .map_err(|_| "system clock exceeds u64 milliseconds".to_string())
}
