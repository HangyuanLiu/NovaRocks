// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Keyed channels share the canonical commit operation and catalog transaction.
use super::*;
use crate::catalog::SnapshotLoading;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::{
    self, OwnerPublisher, PinnedExpiryPublisher, Publisher, RetryPolicy, TransitionalPublisher,
};
use crate::commit::model::{
    BaseIdentity, Dependency, FileChanges, FrozenRequest, IsolationLevel, OperationIntent,
    OperationIntentParts, OperationToken, RequestShape, StartSnapshot, TableTarget,
};
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::slot_change::{SlotChangePreparer, document_error};
use crate::commit::staging::{PreparedChange, Preparer, StagedView, StagingBase};
use crate::document_storage::{keyed, observation};
use novarocks_spi::connector::{
    ConnectorProviderBindingKey, KeyedDocumentObservationRequest, KeyedMarkerWrite, KeyedMutation,
    KeyedMutationIntent,
};

struct MarkerPreparer(KeyedMarkerWrite);
#[async_trait::async_trait]
impl Preparer for MarkerPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        let updates =
            keyed::write::marker_properties(view.metadata(), &self.0).map_err(document_error)?;
        Ok(PreparedChange {
            requirements: vec![],
            updates: vec![TableUpdate::SetProperties { updates }],
        })
    }
}

pub(super) fn execute(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    documents: &KeyedMutationIntent,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let owner = ConnectorProviderBindingKey {
        instance_id: provider.descriptor().instance_id.clone(),
        incarnation: provider.incarnation(),
    };
    documents.validate_for(&owner, request.operation_id)?;
    crate::document_storage::validate_management_admission(&documents.admission)?;
    let target = documents.admission.target();
    let object_id = documents
        .admission
        .expected_object_id()
        .ok_or_else(|| invalid("keyed mutation requires its exact table object"))?;
    let observation_request = KeyedDocumentObservationRequest {
        owner,
        catalog_handle: documents.admission.catalog_handle().clone(),
        target: target.clone(),
        object_id: object_id.clone(),
        context: request.context.clone(),
    };
    let (metadata, metadata_location, prefix) = match &documents.mutation {
        KeyedMutation::Expiry(expiry) => {
            let (metadata, change) = keyed::expiry::prepare(&observation_request, expiry)?;
            let location = observation::metadata_location(&documents.expected_metadata)?;
            (metadata, location, change)
        }
        _ => {
            let physical = provider
                .runtime()
                .observe_table_classified_for_request_with_snapshots(
                    &target.namespace,
                    &target.table,
                    &request.context,
                    SnapshotLoading::Refs,
                )
                .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
            let table = physical.into_table();
            observation::validate_expected_object(object_id, table.metadata())?;
            keyed::view::parse_properties(table.metadata().properties())
                .map_err(observation::keyed_read_error)?;
            if observation::committed_version(&table)? != documents.expected_metadata
                || table.metadata().current_snapshot_id() != documents.expected_snapshot
            {
                return Ok(known_conflict(
                    "keyed document mutation observation has changed",
                ));
            }
            let location = table
                .metadata_location()
                .ok_or_else(|| invalid("keyed mutation base has no metadata location"))?
                .to_string();
            (
                table.metadata().clone(),
                location,
                PreparedChange::default(),
            )
        }
    };
    observation::validate_expected_object(object_id, &metadata)?;
    if metadata.current_snapshot_id() != documents.expected_snapshot {
        return Ok(known_conflict(
            "keyed document mutation snapshot does not match its observation",
        ));
    }
    let operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(request.operation_id),
        metadata.location(),
        provider.runtime().resources().planning_binding().clone(),
        request.context.clone(),
        provider.runtime().resources().catalog_runtime().clone(),
        OperationLimits::default(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let shape = match &documents.mutation {
        KeyedMutation::Markers(_) => RequestShape::MetadataOnly,
        KeyedMutation::SlotChange(_) => RequestShape::SnapshotProducing,
        KeyedMutation::Expiry(_) => RequestShape::DocumentExpiry,
    };
    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: table_ident(target).map_err(invalid)?,
            uuid: Some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: metadata.current_snapshot().map(|snapshot| StartSnapshot {
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
        }),
        changes: FileChanges::default(),
        dependencies: vec![if shape == RequestShape::SnapshotProducing {
            Dependency::RefUnchanged
        } else {
            Dependency::NoReadDependency
        }],
        isolation: IsolationLevel::Snapshot,
        shape,
        summary: BTreeMap::new(),
        token: operation.token(),
    })
    .map_err(|error| invalid(error.to_string()))?
    .with_snapshot_loading(SnapshotLoading::Refs);
    let policy = RetryPolicy::from_properties(metadata.properties())
        .map_err(|error| invalid(error.to_string()))?;
    let journal = PublicationJournal::new(operation.token());
    let preflight_journal = journal.clone();
    let preflight_provider = provider.clone();
    let preflight_documents = documents.clone();
    let preflight_request = request.clone();
    let preflight = Arc::new(
        move |frozen: &FrozenRequest, operation: &IcebergCommitOperation| {
            let BaseIdentity::Existing { uuid, parent, .. } = frozen.base() else {
                return Err(document_error(internal(
                    "keyed mutation acquired a create base",
                )));
            };
            let target = preflight_documents.admission.target();
            let operation_id = preflight_request.operation_id.to_bytes();
            let target = match &preflight_documents.mutation {
                KeyedMutation::Markers(_) => {
                    let properties = frozen
                        .updates()
                        .iter()
                        .filter_map(|update| match update {
                            TableUpdate::SetProperties { updates } => Some(updates),
                            _ => None,
                        })
                        .flat_map(|updates| updates.iter())
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    IcebergMutationEvidenceTarget::KeyedMarkers {
                        namespace: target.namespace.to_string(),
                        table: target.table.to_string(),
                        table_uuid: uuid.to_string(),
                        operation_id,
                        properties,
                    }
                }
                KeyedMutation::SlotChange(change) => {
                    let snapshot = frozen.ref_snapshot_after("main").ok_or_else(|| {
                        document_error(internal("slot mutation has no selected snapshot"))
                    })?;
                    IcebergMutationEvidenceTarget::KeyedSlotChange {
                        namespace: target.namespace.to_string(),
                        table: target.table.to_string(),
                        table_uuid: uuid.to_string(),
                        operation_id,
                        asserted_parent: *parent,
                        preselected_snapshot: snapshot,
                        slots: change
                            .slots
                            .keys()
                            .map(|slot| (slot.owner.clone(), slot.name.clone()))
                            .collect(),
                    }
                }
                KeyedMutation::Expiry(_) => IcebergMutationEvidenceTarget::KeyedExpiry {
                    namespace: target.namespace.to_string(),
                    table: target.table.to_string(),
                    table_uuid: uuid.to_string(),
                    operation_id,
                },
            };
            let evidence = evidence(
                &preflight_provider,
                preflight_request.operation_id,
                preflight_request.operation.kind(),
                target,
            )
            .map_err(document_error)?;
            let receipt = receipt(
                &preflight_provider,
                preflight_request.operation_id,
                preflight_request.operation.kind(),
            )
            .map_err(document_error)?;
            preflight_journal.record(evidence, receipt, frozen)?;
            let mut written = 0usize;
            let mut deleted = 0usize;
            let mut record_bytes = 0usize;
            for update in frozen.updates() {
                match update {
                    TableUpdate::SetProperties { updates } => {
                        written += updates
                            .keys()
                            .filter(|key| key.starts_with(keyed::keys::PREFIX))
                            .count()
                    }
                    TableUpdate::RemoveProperties { removals } => {
                        deleted += removals
                            .iter()
                            .filter(|key| key.starts_with(keyed::keys::PREFIX))
                            .count()
                    }
                    TableUpdate::AddSnapshot { snapshot } => {
                        record_bytes += snapshot
                            .summary()
                            .additional_properties
                            .iter()
                            .filter(|(key, _)| key.starts_with(keyed::keys::RECORD_PREFIX))
                            .map(|(key, value)| key.len() + value.len())
                            .sum::<usize>();
                    }
                    _ => {}
                }
            }
            tracing::info!(
                private_keys_written = written,
                private_keys_deleted = deleted,
                snapshot_record_bytes = record_bytes,
                "Prepared keyed document mutation"
            );
            Ok(())
        },
    );
    let owner_publisher: Arc<dyn Publisher> = Arc::new(OwnerPublisher {
        target: TransitionalPublisher {
            catalog: Arc::clone(provider.runtime().novarocks_catalog()),
            ident: intent.target().ident.clone(),
            target_ref: "main".into(),
            evidence: crate::catalog::error::CatalogCommitEvidence::for_target(
                target.table.to_string(),
            )
            .with_target_uuid(metadata.uuid().to_string()),
            recovery_preflight: preflight,
        },
        operation: operation.token(),
        marker: None,
    });
    let inner: Arc<dyn Publisher> = if shape == RequestShape::DocumentExpiry {
        Arc::new(PinnedExpiryPublisher {
            inner: owner_publisher,
            base: StagingBase::Existing {
                metadata: metadata.clone(),
                metadata_location,
            },
        })
    } else {
        owner_publisher
    };
    let publisher = journal.observe(inner);
    let preparer: Option<Box<dyn Preparer + Send + Sync>> = match &documents.mutation {
        KeyedMutation::Markers(markers) => Some(Box::new(MarkerPreparer(markers.clone()))),
        KeyedMutation::SlotChange(change) => Some(Box::new(SlotChangePreparer {
            change: change.clone(),
        })),
        KeyedMutation::Expiry(_) => None,
    };
    let run_operation = operation.clone();
    let result = operation.runtime().block_on(async move {
        let preparers: Vec<&dyn Preparer> = preparer
            .as_ref()
            .map(|preparer| preparer.as_ref() as &dyn Preparer)
            .into_iter()
            .collect();
        attempt::run(
            &run_operation,
            &intent,
            &prefix,
            &preparers,
            &publisher,
            policy,
        )
        .await
    });
    let mut outcome = match result {
        Ok(report) => journal.project(report),
        Err(message) => {
            let recovery_operation = operation.clone();
            let recovery_journal = journal.clone();
            match operation.runtime().block_on(async move {
                recovery_journal
                    .recover_bridge(&recovery_operation, message)
                    .await
            }) {
                Ok(outcome) => outcome,
                Err(message) => journal.bridge_failure(&operation, message),
            }
        }
    };
    if shape != RequestShape::DocumentExpiry
        && let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } = &mut outcome
    {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&target.namespace, &target.table);
        let refreshed = provider
            .runtime()
            .observe_table_classified_for_request_with_snapshots(
                &target.namespace,
                &target.table,
                &request.context,
                SnapshotLoading::Refs,
            )
            .map_err(|(kind, message)| ConnectorError::new(kind, message))
            .and_then(|physical| observation::committed_version(&physical.into_table()));
        let (version, completion) = match refreshed {
            Ok(version) => (Some(version), finalization.clone()),
            Err(error) => (None, ExternalMutationFinalization::Failed(failure(&error))),
        };
        (*receipt, *finalization) = application_document_committed_receipt(
            provider,
            request.operation_id,
            request.operation.kind(),
            None,
            version,
            completion,
        );
    }
    Ok(outcome)
}
