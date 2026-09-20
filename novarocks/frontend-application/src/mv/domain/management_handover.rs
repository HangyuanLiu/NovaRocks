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

//! Handing one materialized view's ownership to another deployment.
//!
//! The managed marker on the target is what says which deployment may write
//! it, so a handover is a commit against the target itself, not a local
//! setting. Two things must be true before that commit, and this module
//! refuses rather than arranging either of them on its own: nothing of this
//! process's own may be in flight against the target, and the operator must
//! quote a challenge this process issued from the status it is acting on.
//!
//! The replacement marker keeps the incarnation it observed. A handover says
//! who owns the target, not who wrote it last, and leaving the previous
//! writer's incarnation in place is what makes the taking deployment go
//! through its own resume -- it sees an incarnation that is not its own and
//! has to declare that writer isolated before it manages anything. Rewriting
//! it here would hand over a target that looks like the new owner already
//! wrote it.

use novarocks_mv_application::management::{
    DeploymentOwner, EffectDisposition, EffectIdentity, EffectResponsibility, EffectScope,
    ManagedMvTarget, ManagementEntrance, ManagementRequest, ManagementTimestamp, MvManagementPhase,
};
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
    ConnectorDocumentObservationRequest, ConnectorDocumentStorageBudget,
    ConnectorDocumentStorageLimits, ConnectorDocumentUpdateIntent, ConnectorManagedObjectMarker,
    ConnectorManagedObjectMarkerChange, ConnectorPrepareDocumentsRequest,
};
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution,
};

use crate::mv::domain::management_call::ManagementCallTarget;

/// What a handover statement actually did to the target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MvOwnerHandoverOutcome {
    /// The marker now names the requested deployment. The previous owner is
    /// reported because an operator running this against the wrong target
    /// needs to see whose object they just moved.
    HandedOver { previous_owner: String },
    /// The target already named the requested deployment, so there was
    /// nothing to commit. Repeating a statement whose response was lost must
    /// not look like a second handover.
    AlreadyOwned,
}

/// Rewrite one target's managed owner to `new_owner`.
pub(crate) fn hand_over_managed_target(
    entrance: &ManagementEntrance,
    connector_control: &dyn ConnectorControlResolver,
    target: &ManagementCallTarget,
    table: &ConnectorTableIdentity,
    new_owner: &DeploymentOwner,
    context: ConnectorRequestContext,
) -> Result<MvOwnerHandoverOutcome, String> {
    let phase = entrance.management_phase(table);
    refuse_unready_phase(phase)?;

    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("parse the handover MV catalog identity: {error}"))?;
    let lease = connector_control
        .acquire_current(&instance_id)
        .map_err(|error| format!("acquire the handover MV catalog generation: {error}"))?;
    let catalog_handle = lease
        .binding()
        .catalog_handle()
        .map_err(|error| format!("bind the handover MV catalog generation: {error}"))?
        .clone();
    let binding = lease
        .binding()
        .metadata()
        .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
            table: table.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context.clone(),
        })
        .map_err(|error| format!("bind the handover MV target object: {error}"))?;
    if binding.metadata.identity != *table {
        return Err("MV provider bound a different logical target for the handover".to_string());
    }

    let documents_lease = lease
        .derive_document_storage_lease()
        .map_err(|error| format!("derive the handover MV document lease: {error}"))?;
    let observation_request = ConnectorDocumentObservationRequest::try_new(
        documents_lease.owner().clone(),
        documents_lease.catalog_handle().clone(),
        table.clone(),
        binding.object_id.clone(),
        ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
        context.clone(),
    )
    .map_err(|error| format!("build the handover MV observation request: {error}"))?;
    let (observation, documents) =
        novarocks_mv_application::persistence::documents::observe_current_management_document_set(
            &documents_lease,
            observation_request,
            novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
        )
        .map_err(|error| format!("observe the handover MV documents: {error}"))?
        .into_parts();

    let managed_target = ManagedMvTarget::from_observation(&observation)
        .map_err(|error| format!("name the handover MV target: {error:?}"))?;
    if observation.marker().owner() == new_owner.as_str() {
        return Ok(MvOwnerHandoverOutcome::AlreadyOwned);
    }
    let previous_owner = observation.marker().owner().to_string();

    // Taking the entrance is what closes admission, and it is taken before the
    // intent is built so no part of the handover is prepared against a target
    // another statement is already writing. A target this process has never
    // observed has no admission to close: that is the taking deployment's
    // case, where there is nothing local to protect.
    let mut management = match phase {
        MvManagementPhase::NotObserved => None,
        _ => {
            let cancelled = context.clone();
            Some(
                entrance
                    .acquire(
                        ManagementRequest::try_new(
                            catalog_handle.clone(),
                            table.clone(),
                            Some(binding.object_id.clone()),
                            ConnectorDocumentManagementOperation::SingleTargetUpdate,
                            Some(documents.management_dependencies(lease.control_runtime_id())),
                            EffectScope::CATALOG_COMMIT,
                        )
                        .map_err(|error| {
                            format!("build the handover admission request: {error:?}")
                        })?,
                        || cancelled.cancellation().is_cancelled(),
                    )
                    .map_err(|error| format!("close MV management for the handover: {error:?}"))?,
            )
        }
    };

    let operation_id = ConnectorMutationOperationId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
    let intent = build_owner_update_intent(
        &documents_lease,
        &observation,
        &documents,
        catalog_handle,
        operation_id,
        new_owner,
        &context,
    )?;

    let responsibility = EffectResponsibility::new(
        EffectIdentity::from_bytes(operation_id.to_bytes()),
        managed_target,
        entrance.incarnation().clone(),
        EffectScope::CATALOG_COMMIT,
        now_management_timestamp()?,
    );
    if let Some(management) = management.as_mut() {
        management
            .mark_dispatched(responsibility)
            .map_err(|error| format!("mark the MV handover dispatched: {error:?}"))?;
    }

    let mutation = lease
        .derive_mutation_lease()
        .map_err(|error| format!("derive the MV handover mutation lease: {error}"))?;
    let resolved = crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
        &mutation,
        operation_id,
        novarocks_spi::connector::ConnectorCatalogMutationOperation::UpdateApplicationDocuments {
            intent,
        },
        context,
    );
    let disposition = disposition_of(&resolved);
    if let Some(management) = management {
        management
            .record_terminal(disposition)
            .map_err(|error| format!("record the MV handover terminal: {error:?}"))?;
    }
    match disposition {
        EffectDisposition::KnownCommitted => {
            Ok(MvOwnerHandoverOutcome::HandedOver { previous_owner })
        }
        // A handover that did not commit changed nothing, and one whose
        // outcome was lost changed nothing this process may assume. Both are
        // errors for the caller; only the second leaves a barrier behind, and
        // the entrance already holds it.
        EffectDisposition::KnownUncommitted => Err(
            "MV owner handover did not commit; the target still names its previous owner"
                .to_string(),
        ),
        EffectDisposition::CommitUnknown => Err(
            "MV owner handover outcome is unknown; read novarocks_mv_management_status again \
             before acting on this target"
                .to_string(),
        ),
    }
}

/// A handover may only start from a target nothing here is holding.
///
/// Every refusal names the statement that clears it, because the phases differ
/// in what an operator has to do next and an operator who is told only "not
/// now" will retry the same statement.
fn refuse_unready_phase(phase: MvManagementPhase) -> Result<(), String> {
    match phase {
        MvManagementPhase::Manageable | MvManagementPhase::NotObserved => Ok(()),
        MvManagementPhase::Managing => Err(
            "MV owner handover refused: a management write holds this target right now".to_string(),
        ),
        MvManagementPhase::AwaitingEffectSettlement { unsettled } => Err(format!(
            "MV owner handover refused: {unsettled} unresolved effect(s) must be settled first \
             with novarocks_mv_resume_management"
        )),
        MvManagementPhase::AwaitingConvergence | MvManagementPhase::AwaitingObservation => Err(
            "MV owner handover refused: this target owes a fresh observation before its owner \
             can change"
                .to_string(),
        ),
        MvManagementPhase::AwaitingCreateBinding => Err(
            "MV owner handover refused: this target's CREATE has no bound object yet".to_string(),
        ),
        MvManagementPhase::Stopping => {
            Err("MV owner handover refused: this process is stopping".to_string())
        }
    }
}

/// The update that carries the new owner and nothing else.
///
/// It republishes the configuration document unchanged. C is the one document
/// an owner may rewrite without changing what the view computes, so the commit
/// states exactly "the owner changed and nothing else did".
fn build_owner_update_intent(
    documents_lease: &novarocks_spi::connector::document_storage::ConnectorDocumentStorageLease,
    observation: &novarocks_spi::connector::document_storage::ConnectorDocumentManagementObservation,
    documents: &novarocks_mv_application::persistence::documents::MvObservedCurrentDocuments,
    catalog_handle: novarocks_spi::connector::CatalogHandle,
    operation_id: ConnectorMutationOperationId,
    new_owner: &DeploymentOwner,
    context: &ConnectorRequestContext,
) -> Result<ConnectorDocumentUpdateIntent, String> {
    let admission = documents_lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                documents_lease.owner().clone(),
                catalog_handle,
                operation_id,
                observation.target().clone(),
                Some(observation.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                context.clone(),
            )
            .map_err(|error| format!("build the MV handover admission: {error}"))?,
        )
        .map_err(|error| format!("admit the MV handover documents: {error}"))?;
    let prepared = documents_lease
        .prepare_documents(
            ConnectorPrepareDocumentsRequest::try_new(
                admission,
                novarocks_mv_application::persistence::documents::configuration_document_set(
                    documents.configuration(),
                )
                .map_err(|error| format!("encode the MV handover set: {error}"))?,
                context.clone(),
            )
            .map_err(|error| format!("build the MV handover request: {error}"))?,
        )
        .map_err(|error| format!("prepare the MV handover documents: {error}"))?;
    ConnectorDocumentUpdateIntent::try_new(
        prepared,
        observation.clone(),
        ConnectorManagedObjectMarkerChange::Replace {
            expected: observation.marker().clone(),
            replacement: ConnectorManagedObjectMarker::try_new(
                observation.marker().kind(),
                new_owner.as_str(),
                observation.marker().incarnation(),
            )
            .map_err(|error| format!("build the MV handover marker: {error}"))?,
        },
    )
    .map_err(|error| format!("build the MV handover intent: {error}"))
}

fn disposition_of(
    resolved: &crate::connector::mutation::ResolvedCatalogMutation,
) -> EffectDisposition {
    match resolved {
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(_) => {
            EffectDisposition::KnownCommitted
        }
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted { .. }
        | crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { .. } => {
            EffectDisposition::KnownUncommitted
        }
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { .. } => {
            EffectDisposition::CommitUnknown
        }
    }
}

fn now_management_timestamp() -> Result<ManagementTimestamp, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_string())?;
    u64::try_from(now.as_millis())
        .map(ManagementTimestamp::from_unix_millis)
        .map_err(|_| "system clock exceeds u64 milliseconds".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_settled_or_unobserved_target_may_be_handed_over() {
        refuse_unready_phase(MvManagementPhase::Manageable).expect("a settled target may move");
        refuse_unready_phase(MvManagementPhase::NotObserved)
            .expect("a target this process never observed is the taking side's case");
    }

    #[test]
    fn every_refusal_names_what_clears_it() {
        let settlement =
            refuse_unready_phase(MvManagementPhase::AwaitingEffectSettlement { unsettled: 2 })
                .expect_err("an unresolved effect blocks a handover");
        assert!(
            settlement.contains("novarocks_mv_resume_management"),
            "{settlement}"
        );
        assert!(settlement.contains('2'), "{settlement}");

        for (phase, expected) in [
            (MvManagementPhase::Managing, "holds this target right now"),
            (
                MvManagementPhase::AwaitingConvergence,
                "owes a fresh observation",
            ),
            (
                MvManagementPhase::AwaitingObservation,
                "owes a fresh observation",
            ),
            (
                MvManagementPhase::AwaitingCreateBinding,
                "no bound object yet",
            ),
            (MvManagementPhase::Stopping, "process is stopping"),
        ] {
            let error = refuse_unready_phase(phase)
                .expect_err("only a settled or unobserved target may be handed over");
            assert!(error.contains(expected), "{phase:?}: {error}");
        }
    }
}
