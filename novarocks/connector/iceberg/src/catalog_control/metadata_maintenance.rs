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

//! Iceberg control-plane implementation of metadata-maintenance SPI.
//!
//! This module deliberately owns catalog objects only on the FE.  Its plan is
//! opaque to generic core and its execute path never opens a BE writer or
//! emits a fragment carrier.

use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::prepared::CatalogPublisher;
use crate::commit::attempt::{self, RetryPolicy};
use crate::commit::model::{
    AttemptToken, BaseIdentity, Dependency, FileChanges, FrozenRequest, IsolationLevel,
    OperationIntent, OperationIntentParts, OperationToken, RequestShape, TableTarget,
};
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::snapshot_lifecycle_helpers::reclamation::ReclamationReport;
use crate::commit::snapshot_lifecycle_helpers::retention::RetentionPlan;
use crate::commit::staging::PreparedChange;
use novarocks_spi::connector::{ConnectorOperationControl, ConnectorRequestContext};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorMaxCompactableDataFiles, ConnectorMaxCompactableDataFilesRequest,
    ConnectorMetadataMaintenance, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReceiptSummary,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorProviderBindingKey, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::commit::snapshot_lifecycle_helpers::rewrite_manifests::RewriteManifestsPreparer;
use crate::metadata::IcebergTablePayload;
use crate::metadata_context::IcebergMetadataContext;

const PAYLOAD_VERSION: u16 = 2;
const MARKER_VERSION: u16 = 2;
const MARKER_PROPERTY: &str = "novarocks.connector.maintenance.v2";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.intent.v2\0";
const IDENTITY_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.identity.v2\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenancePlanPayloadV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    evaluation_time_ms: i64,
    operation_kind: String,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceMarkerV1 {
    version: u16,
    identity_digest_hex: String,
    incarnation_hex: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    intent_digest_hex: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceReceiptV1 {
    version: u16,
    snapshot_id: Option<i64>,
    attempt_id_hex: String,
    attempt_ordinal: u32,
    base_snapshot_id: Option<i64>,
    metadata_location_digest_hex: String,
    frozen_request_digest_hex: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceEvidenceV1 {
    version: u16,
    namespace: String,
    table: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    intent_digest_hex: String,
    identity_digest_hex: String,
}

#[derive(Clone)]
struct TerminalRecord {
    plan_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
}

/// FE-only Iceberg adapter.  The registry contains process-local catalog
/// clients and credentials; none cross the SPI payload boundary.
pub(crate) struct IcebergMetadataMaintenanceAdapter {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergMetadataContext>,
    terminal: Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>,
    active: Mutex<HashSet<ConnectorMutationOperationId>>,
}

impl IcebergMetadataMaintenanceAdapter {
    pub(crate) fn new(
        key: ConnectorProviderBindingKey,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            runtime,
            terminal: Mutex::new(HashMap::new()),
            active: Mutex::new(HashSet::new()),
        })
    }

    fn ensure_owner(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg metadata maintenance does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn plan_payload(
        &self,
        request: &ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<
        (
            IcebergMetadataMaintenancePlanPayloadV1,
            [u8; 32],
            ConnectorMetadataMaintenancePlanSummary,
        ),
        ConnectorError,
    > {
        let (namespace, table_name) = decode_base_table_target(request.operation().table())?;
        self.runtime
            .control_state()
            .invalidate_table_cache(&namespace, &table_name);
        let loaded = self
            .runtime
            .observe_table_classified_for_request(&namespace, &table_name, &request.context)
            .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
        let table = loaded.into_table();
        let metadata = table.metadata();
        let (older_than_ms, retain_last) = match request.operation() {
            ConnectorMetadataMaintenanceOperation::RewriteMetadataLayout { .. } => (None, None),
            ConnectorMetadataMaintenanceOperation::ExpireTableVersions {
                older_than_ms,
                retain_last,
                ..
            } => (*older_than_ms, *retain_last),
        };
        let payload = IcebergMetadataMaintenancePlanPayloadV1 {
            version: PAYLOAD_VERSION,
            namespace,
            table: table_name,
            table_uuid: metadata.uuid().to_string(),
            evaluation_time_ms: now_ms()?,
            operation_kind: request.operation().kind().to_string(),
            older_than_ms,
            retain_last,
        };
        // Only immutable operation intent belongs to the plan digest. Current
        // schema, spec, metadata location and ref head belong to each attempt.
        let intent_digest = intent_digest(&payload)?;
        let summary = ConnectorMetadataMaintenancePlanSummary::new(0, 0, 0, 0, 0);
        Ok((payload, intent_digest, summary))
    }

    fn payload_for_plan(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
    ) -> Result<IcebergMetadataMaintenancePlanPayloadV1, ConnectorError> {
        let payload: IcebergMetadataMaintenancePlanPayloadV1 =
            decode_canonical_json(plan.provider_payload(), "Iceberg metadata maintenance plan")?;
        validate_payload_for_plan(plan, &payload)?;
        Ok(payload)
    }

    fn marker(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
    ) -> IcebergMetadataMaintenanceMarkerV1 {
        IcebergMetadataMaintenanceMarkerV1 {
            version: MARKER_VERSION,
            identity_digest_hex: hex_encode(identity_digest(&self.descriptor, &self.key, plan)),
            incarnation_hex: hex_encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex_encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(plan.request_digest()),
            plan_digest_hex: hex_encode(plan.plan_digest()),
            intent_digest_hex: hex_encode(plan.intent_digest()),
        }
    }

    fn evidence(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let evidence = IcebergMetadataMaintenanceEvidenceV1 {
            version: PAYLOAD_VERSION,
            namespace: payload.namespace.clone(),
            table: payload.table.clone(),
            operation_id_hex: hex_encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(plan.request_digest()),
            plan_digest_hex: hex_encode(plan.plan_digest()),
            intent_digest_hex: hex_encode(plan.intent_digest()),
            identity_digest_hex: hex_encode(identity_digest(&self.descriptor, &self.key, plan)),
        };
        ExternalMutationEvidence::try_new(
            PAYLOAD_VERSION,
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            canonical_json(&evidence, "Iceberg metadata maintenance evidence")?,
        )
    }

    fn replay_terminal(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        original: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } = &original else {
            return Ok(original);
        };
        let Some((journal, entry)) =
            PublicationJournal::<MaintenancePublication>::from_evidence(evidence)?
        else {
            return Ok(original);
        };
        if evidence.operation_id() != plan.operation_id()
            || evidence.incarnation() != self.key.incarnation
            || evidence.descriptor() != &self.descriptor
            || evidence.operation_kind() != plan.operation_kind()
        {
            return Err(invalid(
                "Maintenance replay evidence differs from the exact operation owner",
            ));
        }
        if matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::KnownCommitted { .. }
        ) {
            return Ok(public_outcome(journal.catalog_entry_outcome(entry)));
        }
        let payload = self.payload_for_plan(plan)?;
        let descriptor = journal
            .entry_descriptor(entry)
            .ok_or_else(|| invalid("Maintenance replay lost the exact descriptor"))?;
        let target = CatalogTableName::new(payload.namespace.clone(), payload.table.clone());
        let binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(context.clone());
        let catalog = Arc::clone(self.runtime.novarocks_catalog());
        let read_target = target.clone();
        let observed = match self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                catalog
                    .load_complete_metadata_for_read(read_target, binding, None)
                    .await
            }) {
            Ok(Ok(crate::catalog::CatalogMetadataLoad::Complete { base, .. })) => base,
            _ => return Ok(original),
        };
        let Some(observation) = descriptor
            .attribution
            .observe_table_metadata(&observed.metadata)
        else {
            return Ok(original);
        };
        if !descriptor.attribution.matches_observation(&observation) {
            return Ok(original);
        }
        journal
            .append_positive_proof_with_finalization(
                descriptor.key,
                observation,
                self.runtime
                    .novarocks_catalog()
                    .positive_attribution_finalization(),
            )
            .map_err(iceberg_error)?;
        if let ExternalMutationOutcome::KnownCommitted {
            receipt,
            effect: ExternalMutationEffect::Applied,
            ..
        } = journal.catalog_entry_outcome(entry)
        {
            if let Some((operation, roots)) = receipt.artifact_cleanup {
                // IRU-5 owned-output cleanup is separate from snapshot GC. It
                // retains the original operation and exact frozen roots.
                let cleanup = self
                    .runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move { operation.cleanup_after_commit(&roots).await });
                let finalization = match cleanup {
                    Ok(report) => report.finalization(),
                    Err(message) => {
                        ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Unavailable,
                            message,
                        ))
                    }
                };
                journal.record_entry_finalization(entry, finalization);
            }
            if let Some(retired) = receipt.retired {
                let mut proof = match journal.commit_outcome(entry) {
                    crate::catalog::error::CatalogOutcome::KnownCommitted { receipt, .. } => {
                        receipt
                    }
                    _ => unreachable!("positive proof is recorded"),
                };
                // This is a fresh diagnostic chain starting point, never a
                // substitute verdict or a planning-base predecessor claim.
                proof.metadata_location = Some(Arc::from(observed.metadata_location));
                let marker = String::from_utf8(
                    canonical_json(&self.marker(plan, &payload), "maintenance marker")?.to_vec(),
                )
                .map_err(|e| internal(e.to_string()))?;
                let owner = Arc::clone(&self.runtime);
                let gc_context = context.clone();
                let result = self
                    .runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move {
                        crate::commit::snapshot_lifecycle_helpers::reclamation::reclaim(
                            owner.as_ref(),
                            target,
                            &retired,
                            &marker,
                            &proof,
                            &gc_context,
                        )
                        .await
                    });
                record_reclamation_result(&journal, entry, &receipt.reclamation, result);
            }
        }
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        Ok(public_outcome(journal.catalog_entry_outcome(entry)))
    }

    fn execute_once(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        context.check_active()?;
        let token = OperationToken::from_mutation(plan.operation_id());
        let runtime = self.runtime.resources().catalog_runtime();
        let initial = runtime.observe_preparation(
            || {
                self.runtime
                    .observe_table_classified_for_request(
                        &payload.namespace,
                        &payload.table,
                        context,
                    )
                    .map_err(|(kind, message)| ConnectorError::new(kind, message))
            },
            connector_failure,
        )?;
        if initial.table.metadata().uuid().to_string() != payload.table_uuid {
            return Ok(rejected(invalid(
                "Iceberg maintenance table UUID was replaced",
            )));
        }
        let policy = RetryPolicy::from_properties(initial.table.metadata().properties())
            .map_err(iceberg_error)?;
        let context = context.clone().with_earlier_deadline(
            Instant::now()
                .checked_add(policy.total_timeout())
                .ok_or_else(|| invalid("Maintenance retry deadline overflows monotonic time"))?,
        );
        let context = &context;
        let marker = String::from_utf8(
            canonical_json(&self.marker(plan, payload), "maintenance marker")?.to_vec(),
        )
        .map_err(|error| internal(error.to_string()))?;
        let ident = initial.table.identifier().clone();
        let operation = IcebergCommitOperation::new(
            token,
            initial.table.metadata().location(),
            self.runtime.resources().planning_binding().clone(),
            context.clone(),
            runtime.clone(),
            OperationLimits::default(),
        )
        .map_err(iceberg_error)?;
        let journal = PublicationJournal::for_runtime(token, runtime);
        if plan.operation_kind() == novarocks_spi::connector::EXPIRE_TABLE_VERSIONS_KIND {
            return self.execute_expire(
                plan, payload, context, &operation, &journal, policy, ident, marker,
            );
        }
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: ident.clone(),
                uuid: Some(initial.table.metadata().uuid()),
            },
            target_ref: "main".into(),
            start: None,
            changes: FileChanges::default(),
            dependencies: vec![Dependency::NoReadDependency],
            isolation: IsolationLevel::Serializable,
            shape: RequestShape::SnapshotProducing,
            summary: BTreeMap::from([(MARKER_PROPERTY.into(), marker)]),
            token,
        })
        .map_err(iceberg_error)?;
        let evidence = self.evidence(plan, payload)?;
        let seed = ReceiptSeed {
            descriptor: self.descriptor.clone(),
            incarnation: self.key.incarnation,
            plan: plan.clone(),
        };
        let preflight_journal = journal.clone();
        let publisher = journal.observe_prepared(Arc::new(CatalogPublisher {
            catalog_operation: CatalogOperation::RewriteManifests,
            catalog: Arc::clone(self.runtime.novarocks_catalog()),
            ident,
            target_ref: "main".into(),
            operation: token,
            recovery_preflight: Arc::new(move |request, operation| {
                let receipt = seed
                    .for_request(request, rewrite_summary(request))
                    .map_err(as_iceberg_error)?;
                preflight_journal.record(
                    evidence.clone(),
                    MaintenancePublication {
                        receipt,
                        retired: None,
                        artifact_cleanup: Some((
                            operation.clone(),
                            request
                                .artifacts()
                                .attempt_owned()
                                .iter()
                                .chain(request.artifacts().operation_references())
                                .cloned()
                                .collect(),
                        )),
                        reclamation: Arc::new(Mutex::new(None)),
                    },
                    request,
                )
            }),
        }));
        let preparer = RewriteManifestsPreparer::new();
        let run_operation = operation.clone();
        let result = runtime.block_on(async move {
            attempt::run(
                &run_operation,
                &intent,
                &PreparedChange::default(),
                &[&preparer],
                &publisher,
                policy,
            )
            .await
        });
        let outcome = match result {
            Ok(report) => public_outcome(journal.project(report)),
            Err(message) => {
                let recovery_journal = journal.clone();
                let recovery_operation = operation.clone();
                public_outcome(
                    runtime
                        .block_on(async move {
                            recovery_journal
                                .recover_bridge(&recovery_operation, message)
                                .await
                        })
                        .map_err(unavailable)?,
                )
            }
        };
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_expire(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        context: &ConnectorRequestContext,
        operation: &IcebergCommitOperation,
        journal: &PublicationJournal<MaintenancePublication>,
        policy: RetryPolicy,
        ident: crate::iceberg::TableIdent,
        marker: String,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        let started = Instant::now();
        let runtime = self.runtime.resources().catalog_runtime();
        let mut ordinal = 0;
        loop {
            if started.elapsed() >= policy.total_timeout() {
                return Ok(rejected(deadline("Maintenance retry budget expired")));
            }
            let prepared = runtime.observe_preparation(
                || {
                    context.check_active()?;
                    let (guard, loaded) = self.runtime.coordinate_table_for_request(
                        &payload.namespace,
                        &payload.table,
                        context,
                    )?;
                    let metadata = loaded.table.metadata();
                    if metadata.uuid().to_string() != payload.table_uuid {
                        return Err(invalid("Iceberg expiration table UUID was replaced"));
                    }
                    let retention = RetentionPlan::compute(
                        metadata,
                        payload.evaluation_time_ms,
                        payload.older_than_ms,
                        payload.retain_last,
                    )
                    .map_err(iceberg_error)?;
                    let mut updates = retention.updates();
                    if !retention.is_empty() {
                        updates.push(crate::iceberg::TableUpdate::SetProperties {
                            updates: HashMap::from([(MARKER_PROPERTY.into(), marker.clone())]),
                        });
                    }
                    let location = loaded.table.metadata_location().ok_or_else(|| {
                        invalid("Expiration requires an authoritative metadata location")
                    })?;
                    let request = crate::commit::staging::freeze_existing_change(
                        ident.clone(),
                        "main",
                        metadata,
                        location,
                        RequestShape::RefRetention,
                        AttemptToken::new(operation.token(), ordinal),
                        PreparedChange {
                            requirements: retention.requirements.clone(),
                            updates,
                        },
                        context,
                    )
                    .map_err(iceberg_error)?;
                    Ok((guard, retention, request))
                },
                connector_failure,
            );
            let (guard, retention, request) = match prepared {
                Ok(prepared) => prepared,
                Err(error) => return Ok(rejected(error)),
            };
            if started.elapsed() >= policy.total_timeout() {
                return Ok(rejected(deadline(
                    "Maintenance retry budget expired after preparation",
                )));
            }
            let receipt = ReceiptSeed {
                descriptor: self.descriptor.clone(),
                incarnation: self.key.incarnation,
                plan: plan.clone(),
            }
            .for_request(
                &request,
                ConnectorMetadataMaintenanceReceiptSummary {
                    affected_versions: retention.removed_snapshots.len() as u64,
                    ..Default::default()
                },
            )?;
            journal
                .record(
                    self.evidence(plan, payload)?,
                    MaintenancePublication {
                        receipt,
                        retired: Some(Arc::new(retention.clone())),
                        artifact_cleanup: None,
                        reclamation: Arc::new(Mutex::new(None)),
                    },
                    &request,
                )
                .map_err(iceberg_error)?;
            let catalog = Arc::clone(self.runtime.novarocks_catalog());
            let dispatch_journal = journal.clone();
            let preparation = context.clone();
            let evidence_policy = catalog.evidence_policy();
            let published = runtime.block_on(async move {
                dispatch_journal
                    .publish_once(
                        preparation,
                        evidence_policy,
                        move |descriptor, generation, preparation| async move {
                            catalog
                                .prepare_frozen_publication(
                                    request,
                                    CatalogOperation::ExpireSnapshots,
                                    None,
                                    descriptor,
                                    generation,
                                    preparation,
                                )
                                .await
                        },
                    )
                    .await
            });
            // The in-process table lock covers the fresh metadata decision and
            // dispatch conclusion, never backoff, manifest scans, or deletion.
            drop(guard);
            let outcome = match published {
                Ok(Ok(entry)) => public_outcome(journal.catalog_entry_outcome(entry)),
                Ok(Err(error)) => public_outcome(journal.catalog_bridge_failure(error.to_string())),
                Err(message) => public_outcome(journal.catalog_bridge_failure(message.to_string())),
            };
            if let ExternalMutationOutcome::KnownUncommitted { ref failure, .. } = outcome {
                if failure.kind() == ConnectorMutationFailureKind::Conflict
                    && ordinal < policy.retries()
                {
                    let wait = policy.backoff(ordinal);
                    if wait >= policy.total_timeout().saturating_sub(started.elapsed()) {
                        return Ok(outcome);
                    }
                    if let Err(error) = runtime
                        .block_on({
                            let waiting = operation.clone();
                            async move { waiting.pause(wait).await }
                        })
                        .map_err(unavailable)
                        .and_then(|r| r.map_err(iceberg_error))
                    {
                        return Ok(rejected(error));
                    }
                    ordinal += 1;
                    continue;
                }
            }
            self.runtime
                .control_state()
                .invalidate_table_cache(&payload.namespace, &payload.table);
            // Reclamation consumes only the exact Applied entry; it owns an
            // independent finite grant and cannot alter the publication fact.
            if matches!(
                outcome,
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ) {
                let entry = journal
                    .current_entry()
                    .expect("Applied expiration has an entry");
                let proof = match journal.commit_outcome(entry) {
                    crate::catalog::error::CatalogOutcome::KnownCommitted { receipt, .. } => {
                        receipt
                    }
                    _ => unreachable!("Applied entry retains its proof"),
                };
                let report_slot = match journal.catalog_entry_outcome(entry) {
                    ExternalMutationOutcome::KnownCommitted { receipt, .. } => receipt.reclamation,
                    _ => unreachable!("Applied entry retains its receipt"),
                };
                let reclamation_owner = Arc::clone(&self.runtime);
                let target =
                    CatalogTableName::new(payload.namespace.clone(), payload.table.clone());
                let reclamation_context = context.clone();
                let result = runtime.block_on(async move {
                    crate::commit::snapshot_lifecycle_helpers::reclamation::reclaim(
                        reclamation_owner.as_ref(),
                        target,
                        &retention,
                        &marker,
                        &proof,
                        &reclamation_context,
                    )
                    .await
                });
                record_reclamation_result(journal, entry, &report_slot, result);
                return Ok(public_outcome(journal.catalog_entry_outcome(entry)));
            }
            return Ok(outcome);
        }
    }
}

impl ConnectorMetadataMaintenance for IcebergMetadataMaintenanceAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn plan_maintenance(
        &self,
        request: ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
        request.validate()?;
        self.ensure_owner(request.owner())?;
        let (namespace, table) = decode_base_table_target(request.operation().table())?;
        let operation = match request.operation() {
            ConnectorMetadataMaintenanceOperation::ExpireTableVersions { .. } => {
                CatalogOperation::ExpireSnapshots
            }
            ConnectorMetadataMaintenanceOperation::RewriteMetadataLayout { .. } => {
                CatalogOperation::RewriteManifests
            }
        };
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                operation,
                CatalogTableName::new(namespace, table),
                request.context.initiation(),
            ))
            .map_err(connector_unsupported)?;
        let (payload, intent_digest, summary) = self.plan_payload(&request)?;
        let provider_payload = canonical_json(&payload, "Iceberg metadata maintenance plan")?;
        let plan = ConnectorMetadataMaintenancePlan::try_new(
            &request,
            intent_digest,
            summary,
            provider_payload,
        )?;
        Ok(plan)
    }

    fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        let terminal = self
            .terminal
            .lock()
            .map_err(|_| internal("Maintenance terminal set poisoned"))?
            .get(&request.plan.operation_id())
            .cloned();
        if let Some(terminal) = terminal {
            if terminal.plan_digest != request.plan.plan_digest() {
                return Err(invalid("Maintenance replay changes the fixed plan"));
            }
            if !matches!(
                terminal.outcome,
                ExternalMutationOutcome::CommitUnknown { .. }
            ) {
                return Ok(terminal.outcome);
            }
            let mut active = self
                .active
                .lock()
                .map_err(|_| internal("Maintenance active set poisoned"))?;
            if !active.insert(request.plan.operation_id()) {
                return Err(invalid(
                    "Maintenance operation already has an active execution",
                ));
            }
            drop(active);
            let _claim = ActiveExecution {
                active: &self.active,
                operation: request.plan.operation_id(),
            };
            let outcome =
                self.replay_terminal(&request.plan, terminal.outcome, &request.context)?;
            self.terminal
                .lock()
                .map_err(|_| internal("Maintenance terminal set poisoned"))?
                .insert(
                    request.plan.operation_id(),
                    TerminalRecord {
                        plan_digest: request.plan.plan_digest(),
                        outcome: outcome.clone(),
                    },
                );
            return Ok(outcome);
        }
        let payload = self.payload_for_plan(&request.plan)?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| internal("Maintenance active set poisoned"))?;
        if !active.insert(request.plan.operation_id()) {
            return Err(invalid(
                "Maintenance operation already has an active execution",
            ));
        }
        drop(active);
        let _claim = ActiveExecution {
            active: &self.active,
            operation: request.plan.operation_id(),
        };
        let terminal = self
            .terminal
            .lock()
            .map_err(|_| internal("Maintenance terminal set poisoned"))?
            .get(&request.plan.operation_id())
            .cloned();
        if let Some(terminal) = terminal {
            if terminal.plan_digest != request.plan.plan_digest() {
                return Err(invalid("Maintenance replay changes the fixed plan"));
            }
            return self.replay_terminal(&request.plan, terminal.outcome, &request.context);
        }
        let outcome = self.execute_once(&request.plan, &payload, &request.context)?;
        self.terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance terminal lock: {error}")))?
            .insert(
                request.plan.operation_id(),
                TerminalRecord {
                    plan_digest: request.plan.plan_digest(),
                    outcome: outcome.clone(),
                },
            );
        Ok(outcome)
    }

    fn read_max_compactable_data_files(
        &self,
        request: ConnectorMaxCompactableDataFilesRequest,
    ) -> Result<ConnectorMaxCompactableDataFiles, ConnectorError> {
        let (namespace, table_name) = decode_base_table_target(&request.table)?;
        // The observation feeds a maintenance decision, so it must see the
        // current table state rather than a cached generation snapshot.
        self.runtime
            .control_state()
            .invalidate_table_cache(&namespace, &table_name);
        let physical = self
            .runtime
            .load_table_for_request(&namespace, &table_name, &request.context)
            .map_err(|error| unavailable(format!("load Iceberg table for observation: {error}")))?;
        let preserve_row_lineage =
            crate::schema_facts::row_lineage_enabled(physical.table.metadata());
        let table = physical.into_table();
        let stats = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::commit::current_live_data_file_compaction_stats(
                    &table,
                    table.file_io(),
                    preserve_row_lineage,
                )
                .await
            })
            .map_err(unavailable)?
            .map_err(|error| {
                unavailable(format!(
                    "observe Iceberg table {namespace}.{table_name} compaction groups: {error}"
                ))
            })?;
        let value = u64::try_from(stats.max_compactable_data_files).map_err(|_| {
            internal(format!(
                "Iceberg table {namespace}.{table_name} compactable file count overflow"
            ))
        })?;
        Ok(ConnectorMaxCompactableDataFiles::new(Some(value)))
    }
}

struct ActiveExecution<'a> {
    active: &'a Mutex<HashSet<ConnectorMutationOperationId>>,
    operation: ConnectorMutationOperationId,
}
impl Drop for ActiveExecution<'_> {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.operation);
    }
}
// The original journal retains candidate descriptors even for Unknown. This
// state is process-local and is never reconstructed from a wire-only marker.
#[derive(Clone)]
pub(crate) struct MaintenancePublication {
    receipt: ConnectorMetadataMaintenanceReceipt,
    retired: Option<Arc<RetentionPlan>>,
    artifact_cleanup: Option<(
        IcebergCommitOperation,
        Vec<crate::commit::model::ObjectIdentity>,
    )>,
    reclamation: Arc<Mutex<Option<ReclamationReport>>>,
}
fn public_outcome(
    outcome: ExternalMutationOutcome<MaintenancePublication>,
) -> ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            mut finalization,
        } => {
            let report = receipt
                .reclamation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let mut public = receipt.receipt;
            if let Some(report) = report {
                let mut summary = public.summary();
                summary.cleanup_succeeded = report.deleted as u64;
                summary.cleanup_failed = report.failed as u64;
                match ConnectorMetadataMaintenanceReceipt::try_new(
                    public.descriptor().clone(),
                    public.incarnation(),
                    public.operation_id(),
                    public.operation_kind(),
                    public.request_digest(),
                    public.plan_digest(),
                    public.intent_digest(),
                    summary,
                    public.provider_payload().clone(),
                ) {
                    Ok(enriched) => public = enriched,
                    Err(error) => {
                        if matches!(finalization, ExternalMutationFinalization::Complete) {
                            finalization =
                                ExternalMutationFinalization::Failed(connector_failure(&error));
                        }
                    }
                }
            }
            ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt: public,
                finalization,
            }
        }
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            ExternalMutationOutcome::CommitUnknown { failure, evidence }
        }
    }
}

fn record_reclamation_result(
    journal: &PublicationJournal<MaintenancePublication>,
    entry: crate::commit::attempt::observation::JournalEntryRef,
    slot: &Mutex<Option<ReclamationReport>>,
    result: std::result::Result<ReclamationReport, String>,
) {
    let failure = match result {
        Ok(report) => {
            let failure = report.diagnostic_failure();
            *slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
            failure
        }
        Err(message) => Some(unavailable(format!(
            "Reclamation bridge exited without a complete progress report: {message}"
        ))),
    };
    if let Some(error) = failure {
        journal.record_entry_finalization(
            entry,
            ExternalMutationFinalization::Failed(connector_failure(&error)),
        );
    }
}

struct ReceiptSeed {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: novarocks_spi::connector::ProviderBindingEpoch,
    plan: ConnectorMetadataMaintenancePlan,
}
impl ReceiptSeed {
    fn for_request(
        &self,
        request: &FrozenRequest,
        summary: ConnectorMetadataMaintenanceReceiptSummary,
    ) -> Result<ConnectorMetadataMaintenanceReceipt, ConnectorError> {
        let BaseIdentity::Existing {
            parent,
            metadata_location,
            ..
        } = request.base()
        else {
            return Err(invalid("Maintenance requires an existing table base"));
        };
        let descriptor =
            crate::commit::publication_descriptor::PublicationDescriptor::frozen(request)
                .map_err(iceberg_error)?;
        let attempt = request.artifacts().attempt();
        ConnectorMetadataMaintenanceReceipt::try_new(
            self.descriptor.clone(),
            self.incarnation,
            self.plan.operation_id(),
            self.plan.operation_kind(),
            self.plan.request_digest(),
            self.plan.plan_digest(),
            self.plan.intent_digest(),
            summary,
            canonical_json(
                &IcebergMetadataMaintenanceReceiptV1 {
                    version: PAYLOAD_VERSION,
                    snapshot_id: request.ref_snapshot_after("main").or(*parent),
                    attempt_id_hex: hex_encode(attempt.nonce_bytes()),
                    attempt_ordinal: attempt.ordinal(),
                    base_snapshot_id: *parent,
                    metadata_location_digest_hex: hex_encode(metadata_location_digest(Some(
                        metadata_location,
                    ))),
                    frozen_request_digest_hex: hex_encode(descriptor.fingerprint),
                },
                "maintenance receipt",
            )?,
        )
    }
}
fn rewrite_summary(request: &FrozenRequest) -> ConnectorMetadataMaintenanceReceiptSummary {
    let mut summary = ConnectorMetadataMaintenanceReceiptSummary::default();
    for update in request.updates() {
        if let crate::iceberg::TableUpdate::AddSnapshot { snapshot } = update {
            let properties = &snapshot.summary().additional_properties;
            summary.rewritten_items = properties
                .get("replaced-manifests-count")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            summary.added_items = properties
                .get("added-manifests-count")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    summary
}
fn connector_failure(error: &ConnectorError) -> ConnectorMutationFailure {
    attempt::before_dispatch_failure(&as_iceberg_error(error.clone()))
}
fn rejected(error: ConnectorError) -> ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: connector_failure(&error),
        cleanup: ExternalMutationFinalization::Complete,
    }
}
fn as_iceberg_error(error: ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, error.to_string())
        .with_source(error)
}
fn iceberg_error(error: crate::iceberg::Error) -> ConnectorError {
    // Preserve typed cancellation/deadline facts instead of classifying text.
    let failure = attempt::before_dispatch_failure(&error);
    let kind = match failure.kind() {
        ConnectorMutationFailureKind::Cancelled => ConnectorErrorKind::Cancelled,
        ConnectorMutationFailureKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        ConnectorMutationFailureKind::Unsupported => ConnectorErrorKind::Unsupported,
        ConnectorMutationFailureKind::InvalidRequest | ConnectorMutationFailureKind::Conflict => {
            ConnectorErrorKind::InvalidRequest
        }
        _ => ConnectorErrorKind::Unavailable,
    };
    ConnectorError::new(kind, error.to_string())
}
fn deadline(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::DeadlineExceeded, message)
}
fn now_ms() -> Result<i64, ConnectorError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| internal(e.to_string()))?
            .as_millis(),
    )
    .map_err(|_| internal("Maintenance clock overflow"))
}

fn decode_base_table_target(
    handle: &novarocks_spi::connector::ConnectorTableHandle,
) -> Result<(String, String), ConnectorError> {
    let target: IcebergTablePayload = serde_json::from_slice(handle.payload())
        .map_err(|error| invalid(format!("decode Iceberg maintenance table handle: {error}")))?;
    if target.metadata_table_type.is_some() {
        return Err(invalid(
            "Iceberg metadata maintenance requires a base table handle",
        ));
    }
    Ok((target.namespace, target.table))
}

fn validate_payload_for_plan(
    plan: &ConnectorMetadataMaintenancePlan,
    payload: &IcebergMetadataMaintenancePlanPayloadV1,
) -> Result<(), ConnectorError> {
    if payload.version != PAYLOAD_VERSION
        || payload.operation_kind != plan.operation_kind()
        || payload.evaluation_time_ms <= 0
        || intent_digest(payload)? != plan.intent_digest()
    {
        return Err(invalid(
            "Iceberg metadata maintenance plan payload does not match SPI plan",
        ));
    }
    Ok(())
}

fn intent_digest(
    payload: &IcebergMetadataMaintenancePlanPayloadV1,
) -> Result<[u8; 32], ConnectorError> {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    hasher.update(canonical_json(payload, "maintenance immutable intent")?);
    Ok(hasher.finalize().into())
}

fn metadata_location_digest(value: Option<&str>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    digest_bytes(&mut hasher, value.unwrap_or_default().as_bytes());
    hasher.finalize().into()
}

fn identity_digest(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    plan: &ConnectorMetadataMaintenancePlan,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DOMAIN);
    digest_bytes(&mut hasher, descriptor.provider_id.as_str().as_bytes());
    digest_bytes(&mut hasher, descriptor.instance_id.as_str().as_bytes());
    hasher.update(key.incarnation.to_bytes());
    hasher.update(plan.operation_id().to_bytes());
    digest_bytes(&mut hasher, plan.operation_kind().as_bytes());
    hasher.update(plan.request_digest());
    hasher.update(plan.plan_digest());
    hasher.finalize().into()
}

fn canonical_json<T: Serialize>(value: &T, label: &str) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode {label}: {error}")))
}

fn decode_canonical_json<T>(payload: &[u8], label: &str) -> Result<T, ConnectorError>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let decoded: T = serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode {label}: {error}")))?;
    if canonical_json(&decoded, label)?.as_ref() != payload {
        return Err(invalid(format!("{label} is not canonical JSON v1")));
    }
    Ok(decoded)
}

fn decode_digest(value: &str) -> Result<[u8; 32], ConnectorError> {
    let bytes = hex_decode(value).ok_or_else(|| invalid("maintenance digest is not hex"))?;
    bytes
        .try_into()
        .map_err(|_| invalid("maintenance digest has invalid length"))
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(ALPHABET[(byte >> 4) as usize] as char);
        encoded.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let high = (digits[0] as char).to_digit(16)? as u8;
            let low = (digits[1] as char).to_digit(16)? as u8;
            Some((high << 4) | low)
        })
        .collect()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<String>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message.into())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorMetadataMaintenanceExecuteRequest,
        ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlanningRequest,
        ConnectorProviderId, ConnectorTableHandle, ProviderBindingEpoch,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    fn context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("context")
    }

    fn adapter() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergMetadataMaintenanceAdapter,
        ConnectorProviderBindingKey,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        let key = ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        };
        runtime
            .novarocks_catalog()
            .bind_publication_generation(key.incarnation)
            .expect("test generation");
        let adapter =
            IcebergMetadataMaintenanceAdapter::new(key.clone(), runtime).expect("adapter");
        (executor, warehouse, adapter, key)
    }

    fn plan(
        key: ConnectorProviderBindingKey,
        operation_id: ConnectorMutationOperationId,
        table_payload: &'static [u8],
    ) -> ConnectorMetadataMaintenancePlan {
        let table = ConnectorTableHandle::try_new(
            key.instance_id.clone(),
            Bytes::from_static(table_payload),
        )
        .expect("table handle");
        let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
            operation_id,
            key,
            ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(table)
                .expect("operation"),
            context(),
        )
        .expect("planning request");
        ConnectorMetadataMaintenancePlan::try_new(
            &request,
            [3; 32],
            ConnectorMetadataMaintenancePlanSummary::new(1, 2, 3, 4, 5),
            Bytes::from_static(b"provider-plan"),
        )
        .expect("plan")
    }

    fn receipt(
        adapter: &IcebergMetadataMaintenanceAdapter,
        plan: &ConnectorMetadataMaintenancePlan,
    ) -> ConnectorMetadataMaintenanceReceipt {
        ConnectorMetadataMaintenanceReceipt::try_new(
            adapter.descriptor.clone(),
            adapter.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.intent_digest(),
            ConnectorMetadataMaintenanceReceiptSummary::default(),
            Bytes::from_static(b"receipt"),
        )
        .expect("receipt")
    }

    #[test]
    fn maintenance_hex_codec_is_canonical_and_strict() {
        let digest = Sha256::digest(b"maintenance");
        let encoded = hex_encode(digest);
        assert_eq!(encoded.len(), 64);
        assert_eq!(hex_decode(&encoded).as_deref(), Some(digest.as_slice()));
        assert!(hex_decode("0").is_none());
        assert!(hex_decode("xy").is_none());
    }

    fn create_local_table(
        executor: &tokio::runtime::Runtime,
        adapter: &IcebergMetadataMaintenanceAdapter,
    ) -> crate::iceberg::table::Table {
        create_local_table_with_version(executor, adapter, crate::iceberg::spec::FormatVersion::V2)
    }

    fn create_local_table_with_version(
        executor: &tokio::runtime::Runtime,
        adapter: &IcebergMetadataMaintenanceAdapter,
        version: crate::iceberg::spec::FormatVersion,
    ) -> crate::iceberg::table::Table {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use crate::iceberg::{NamespaceIdent, TableCreation};

        executor.block_on(async {
            use crate::catalog::CatalogNamespaceName;
            use crate::catalog::catalog_request::CatalogPublicationRequest;
            use crate::catalog::error::CatalogOutcome;
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                &adapter.runtime,
                adapter.key.incarnation,
                CatalogPublicationRequest::CreateNamespace {
                    namespace: CatalogNamespaceName::new("maintenance_test"),
                    properties: HashMap::new(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                &adapter.runtime,
                adapter.key.incarnation,
                CatalogPublicationRequest::CreateTable {
                    namespace: CatalogNamespaceName::new("maintenance_test"),
                    creation: TableCreation::builder()
                        .name("t".into())
                        .schema(schema)
                        .format_version(version)
                        .build(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            crate::catalog::admission_test_support::load_static_fixture_table(
                &adapter.runtime,
                &crate::iceberg::TableIdent::new(
                    NamespaceIdent::new("maintenance_test".into()),
                    "t".into(),
                ),
            )
            .await
        })
    }

    fn append_local_file(
        executor: &tokio::runtime::Runtime,
        adapter: &IcebergMetadataMaintenanceAdapter,
        ident: &crate::iceberg::TableIdent,
        name: &str,
    ) {
        executor.block_on(append_fixture_file(adapter, ident, name));
    }

    async fn append_fixture_file(
        adapter: &IcebergMetadataMaintenanceAdapter,
        ident: &crate::iceberg::TableIdent,
        name: &str,
    ) {
        use crate::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
        let table = crate::catalog::admission_test_support::load_static_fixture_table(
            &adapter.runtime,
            ident,
        )
        .await;
        let location = format!("{}/data/{name}.parquet", table.metadata().location());
        table
            .file_io()
            .new_output(&location)
            .expect("data output")
            .write(Bytes::from_static(b"test data"))
            .await
            .expect("write data file");
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(location)
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(9)
            .partition(Struct::empty())
            .build()
            .expect("synthetic data file");
        let contents = vec![
            crate::commit::model::AddedContent::new_logical_data(
                file,
                table.metadata().default_partition_spec_id(),
            )
            .unwrap(),
        ];
        let provider = crate::metadata::IcebergMetadata::new(
            adapter.descriptor.clone(),
            adapter.key.incarnation,
            Arc::clone(&adapter.runtime),
        );
        crate::commit::run::append_contents_for_test(
            provider,
            table,
            contents,
            "main".into(),
            BTreeMap::new(),
        )
        .await
        .expect("publish fixture append through its owner");
    }

    fn local_planning_request(
        key: &ConnectorProviderBindingKey,
        expire: bool,
    ) -> ConnectorMetadataMaintenancePlanningRequest {
        let table = ConnectorTableHandle::try_new(
            key.instance_id.clone(),
            Bytes::from_static(br#"{"namespace":"maintenance_test","table":"t","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#),
        )
        .expect("table handle");
        let operation = if expire {
            ConnectorMetadataMaintenanceOperation::expire_table_versions(
                table,
                Some(i64::MAX),
                Some(1),
            )
        } else {
            ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(table)
        }
        .expect("operation");
        ConnectorMetadataMaintenancePlanningRequest::try_new(
            ConnectorMutationOperationId::new(),
            key.clone(),
            operation,
            context(),
        )
        .expect("planning request")
    }

    fn inventory_without_private_artifacts(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        fn visit(path: &std::path::Path, inventory: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(path).expect("table inventory") {
                let entry = entry.expect("table entry");
                assert_ne!(
                    entry.file_name(),
                    "_novarocks",
                    "private maintenance artifact"
                );
                inventory.push(entry.path());
                if entry.file_type().expect("entry type").is_dir() {
                    visit(&entry.path(), inventory);
                }
            }
        }
        let mut inventory = Vec::new();
        visit(root, &mut inventory);
        inventory.sort();
        inventory
    }

    // Responsibility: real Hadoop/FileIO adapter behavior, including no-op and
    // rejected plans. SQL/system tests own the distributed product surface.
    #[test]
    fn metadata_maintenance_keeps_plans_and_receipts_out_of_table_locations() {
        let (executor, _warehouse, adapter, key) = adapter();
        let table = create_local_table(&executor, &adapter);
        let location =
            novarocks_fs::FsLocation::parse(table.metadata().location()).expect("table location");
        let root = std::path::PathBuf::from(location.path());
        for expire in [false, true] {
            let before = inventory_without_private_artifacts(&root);
            let plan = adapter
                .plan_maintenance(local_planning_request(&key, expire))
                .expect("no-op plan");
            assert_eq!(inventory_without_private_artifacts(&root), before);
            let payload: IcebergMetadataMaintenancePlanPayloadV1 =
                decode_canonical_json(plan.provider_payload(), "test plan").expect("plan payload");
            assert!(payload.evaluation_time_ms > 0);
            assert_eq!(payload.table_uuid, table.metadata().uuid().to_string());
            let outcome = adapter
                .execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                        .expect("execute request"),
                )
                .expect("no-op execute");
            let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
                panic!("no-op maintenance must complete")
            };
            assert_eq!(
                receipt.summary(),
                ConnectorMetadataMaintenanceReceiptSummary::default()
            );
            assert_eq!(inventory_without_private_artifacts(&root), before);
        }

        append_local_file(&executor, &adapter, table.identifier(), "a");
        append_local_file(&executor, &adapter, table.identifier(), "b");
        for expire in [false, true] {
            let before = inventory_without_private_artifacts(&root);
            let plan = adapter
                .plan_maintenance(local_planning_request(&key, expire))
                .expect("maintenance plan");
            assert_eq!(inventory_without_private_artifacts(&root), before);
            let outcome = adapter
                .execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                        .expect("execute request"),
                )
                .expect("maintenance execute");
            let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
                panic!("maintenance must commit")
            };
            if expire {
                assert!(receipt.summary().affected_versions > 0);
            } else {
                assert!(receipt.summary().rewritten_items > 0);
            }
            let payload: serde_json::Value =
                serde_json::from_slice(receipt.provider_payload()).expect("receipt JSON");
            assert_eq!(payload.as_object().expect("receipt object").len(), 7);
            assert_eq!(
                payload["frozen_request_digest_hex"].as_str().unwrap().len(),
                64
            );
            assert_eq!(payload["attempt_id_hex"].as_str().unwrap().len(), 32);
            assert!(payload.get("snapshot_id").is_some());
            inventory_without_private_artifacts(&root);
        }
    }

    #[test]
    fn metadata_maintenance_rejected_payload_leaves_no_artifacts_and_fresh_attempt_accepts_advance()
    {
        let (executor, _warehouse, adapter, key) = adapter();
        let table = create_local_table(&executor, &adapter);
        let location =
            novarocks_fs::FsLocation::parse(table.metadata().location()).expect("table location");
        let root = std::path::PathBuf::from(location.path());
        for expire in [false, true] {
            let request = local_planning_request(&key, expire);
            let plan = adapter.plan_maintenance(request.clone()).expect("plan");
            let before = inventory_without_private_artifacts(&root);
            let mut payload: IcebergMetadataMaintenancePlanPayloadV1 =
                decode_canonical_json(plan.provider_payload(), "test plan").expect("payload");
            payload.operation_kind = "mismatched-operation".to_string();
            let invalid_plan = ConnectorMetadataMaintenancePlan::try_new(
                &request,
                plan.intent_digest(),
                plan.summary(),
                canonical_json(&payload, "test invalid plan").expect("encode payload"),
            )
            .expect("invalid provider plan");
            let error = adapter
                .execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(invalid_plan, context())
                        .expect("execute request"),
                )
                .expect_err("provider payload must be validated without a cache");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(inventory_without_private_artifacts(&root), before);

            append_local_file(
                &executor,
                &adapter,
                table.identifier(),
                if expire {
                    "expire-conflict"
                } else {
                    "rewrite-conflict"
                },
            );
            let fixed_payload = plan.provider_payload().clone();
            let fixed_intent = plan.intent_digest();
            let execute_plan = plan.clone();
            let outcome = adapter
                .execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(execute_plan, context())
                        .expect("execute request"),
                )
                .expect("fresh policy attempt outcome");
            assert!(matches!(
                outcome,
                ExternalMutationOutcome::KnownCommitted { .. }
            ));
            assert_eq!(plan.provider_payload(), &fixed_payload);
            assert_eq!(plan.intent_digest(), fixed_intent);
            inventory_without_private_artifacts(&root);
        }
    }

    #[test]
    fn v3_empty_maintenance_is_noop_without_publication_or_artifacts() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table_with_version(
            &executor,
            &base,
            crate::iceberg::spec::FormatVersion::V3,
        );
        let location = novarocks_fs::FsLocation::parse(table.metadata().location()).unwrap();
        let before = inventory_without_private_artifacts(std::path::Path::new(location.path()));
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let actual = recorded_adapter(
            &base,
            Arc::new(move |_, _| {
                seen.fetch_add(1, Ordering::SeqCst);
            }),
        );
        let lease = real_maintenance_lease(actual);
        for expire in [false, true] {
            let plan = lease
                .plan_maintenance(local_planning_request(&key, expire))
                .unwrap();
            let outcome = lease
                .execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context()).unwrap(),
                )
                .unwrap();
            let ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt,
                finalization: ExternalMutationFinalization::Complete,
            } = outcome
            else {
                panic!("empty V3 must be a complete no-op")
            };
            assert_eq!(
                receipt.summary(),
                ConnectorMetadataMaintenanceReceiptSummary::default()
            );
            assert_eq!(
                inventory_without_private_artifacts(std::path::Path::new(location.path())),
                before
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn v3_expiry_publishes_one_source_uuid_and_keeps_exactly_the_original_head() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table_with_version(
            &executor,
            &base,
            crate::iceberg::spec::FormatVersion::V3,
        );
        append_local_file(&executor, &base, table.identifier(), "first");
        append_local_file(&executor, &base, table.identifier(), "second");
        let loaded = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            ),
        );
        let head = loaded.metadata().current_snapshot_id().unwrap();
        let uuid = loaded.metadata().uuid();
        assert_eq!(loaded.metadata().snapshots().len(), 2);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let actual = recorded_adapter(
            &base,
            Arc::new(move |requirements, _| {
                assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
                assert_eq!(
                    requirements
                        .iter()
                        .filter_map(|r| {
                            if let crate::iceberg::TableRequirement::UuidMatch { uuid } = r {
                                Some(*uuid)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                    vec![uuid]
                );
            }),
        );
        let lease = real_maintenance_lease(actual);
        let plan = lease
            .plan_maintenance(local_planning_request(&key, true))
            .unwrap();
        let outcome = lease
            .execute(ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context()).unwrap())
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        } = outcome
        else {
            panic!("V3 expiry must commit")
        };
        assert_eq!(receipt.summary().affected_versions, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let after = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            ),
        );
        assert_eq!(
            after.metadata().format_version(),
            crate::iceberg::spec::FormatVersion::V3
        );
        assert_eq!(after.metadata().current_snapshot_id(), Some(head));
        assert_eq!(
            after
                .metadata()
                .snapshots()
                .map(|snapshot| snapshot.snapshot_id())
                .collect::<Vec<_>>(),
            vec![head]
        );
    }

    fn recorded_adapter(
        adapter: &IcebergMetadataMaintenanceAdapter,
        recorder: Arc<
            dyn Fn(&[crate::iceberg::TableRequirement], &[crate::iceberg::TableUpdate])
                + Send
                + Sync,
        >,
    ) -> IcebergMetadataMaintenanceAdapter {
        let owner = crate::catalog::admission_test_support::all_admitted_recording(
            Arc::clone(adapter.runtime.novarocks_catalog()),
            recorder,
        );
        let runtime = IcebergMetadataContext::with_catalog_for_test(
            adapter.runtime.control_state().clone(),
            adapter.runtime.resources().clone(),
            owner,
        );
        IcebergMetadataMaintenanceAdapter::new(adapter.key.clone(), Arc::new(runtime)).unwrap()
    }
    fn real_maintenance_lease(
        adapter: IcebergMetadataMaintenanceAdapter,
    ) -> novarocks_spi::connector::ConnectorMetadataMaintenanceLease {
        let descriptor = adapter.descriptor.clone();
        let epoch = adapter.key.incarnation;
        let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
            descriptor.clone(),
            epoch,
            Arc::clone(&adapter.runtime),
        ));
        novarocks_spi::connector::ConnectorMetadataMaintenanceLease::new(
            descriptor,
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            epoch,
            metadata,
            Arc::new(adapter),
            || {},
        )
        .expect("real metadata and maintenance adapter share the exact generation")
    }

    fn competing_adapter(
        base: &IcebergMetadataMaintenanceAdapter,
    ) -> Arc<IcebergMetadataMaintenanceAdapter> {
        Arc::new(
            IcebergMetadataMaintenanceAdapter::new(base.key.clone(), Arc::clone(&base.runtime))
                .unwrap(),
        )
    }

    fn foreign_properties(
        runtime: crate::resources::IcebergCatalogRuntime,
        catalog: Arc<IcebergMetadataMaintenanceAdapter>,
        ident: crate::iceberg::TableIdent,
        properties: HashMap<String, String>,
    ) {
        runtime
            .block_on(async move {
                let table = crate::catalog::admission_test_support::load_static_fixture_table(
                    &catalog.runtime,
                    &ident,
                )
                .await;
                let operation = OperationToken::from_mutation(ConnectorMutationOperationId::new());
                let preparation = context();
                let frozen = crate::commit::staging::freeze_existing_change(
                    ident,
                    "main",
                    table.metadata(),
                    table.metadata_location().unwrap(),
                    RequestShape::MetadataOnly,
                    AttemptToken::new(operation, 0),
                    PreparedChange {
                        requirements: vec![],
                        updates: vec![crate::iceberg::TableUpdate::SetProperties {
                            updates: properties,
                        }],
                    },
                    &preparation,
                )
                .unwrap();
                let outcome = crate::catalog::admission_test_support::publish_frozen(
                    Arc::clone(catalog.runtime.novarocks_catalog()),
                    catalog.descriptor.clone(),
                    catalog.key.incarnation,
                    frozen,
                    CatalogOperation::AlterProperties,
                    preparation,
                    None,
                )
                .await;
                assert!(
                    matches!(
                        outcome,
                        crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                    ),
                    "{outcome:?}"
                );
            })
            .unwrap();
    }
    #[test]
    fn expire_conflict_reloads_policy_without_accumulating_old_candidates_or_changing_fixed_intent()
    {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table(&executor, &base);
        append_local_file(&executor, &base, table.identifier(), "first");
        append_local_file(&executor, &base, table.identifier(), "second");
        let source_uuid = table.metadata().uuid();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let catalog = competing_adapter(&base);
        let runtime = base.runtime.resources().catalog_runtime().clone();
        let ident = table.identifier().clone();
        let adapter = recorded_adapter(
            &base,
            Arc::new(move |requirements, updates| {
                assert_eq!(
                    requirements
                        .iter()
                        .filter_map(|r| {
                            if let crate::iceberg::TableRequirement::UuidMatch { uuid } = r {
                                Some(*uuid)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                    vec![source_uuid]
                );
                assert!(updates.iter().any(|update|matches!(update,crate::iceberg::TableUpdate::RemoveSnapshots{snapshot_ids} if snapshot_ids.len()==1)));
                assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
                foreign_properties(
                    runtime.clone(),
                    catalog.clone(),
                    ident.clone(),
                    HashMap::from([("history.expire.min-snapshots-to-keep".into(), "3".into())]),
                );
            }),
        );
        let lease = real_maintenance_lease(adapter);
        let mut request = local_planning_request(&key, true);
        // Use table defaults so the confirmed-conflict retry observes the new
        // min-keep policy instead of a caller override.
        request = ConnectorMetadataMaintenancePlanningRequest::try_new(
            request.operation_id(),
            key.clone(),
            ConnectorMetadataMaintenanceOperation::expire_table_versions(
                request.operation().table().clone(),
                Some(i64::MAX),
                None,
            )
            .unwrap(),
            context(),
        )
        .unwrap();
        let plan = lease.plan_maintenance(request).unwrap();
        let payload = plan.provider_payload().clone();
        let intent = plan.intent_digest();
        let outcome = lease
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context())
                    .unwrap(),
            )
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            effect, receipt, ..
        } = outcome
        else {
            panic!("fresh policy should produce a real no-op")
        };
        assert_eq!(effect, ExternalMutationEffect::NoOp);
        assert_eq!(receipt.summary().affected_versions, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let attempt: serde_json::Value =
            serde_json::from_slice(receipt.provider_payload()).unwrap();
        assert_eq!(attempt["attempt_ordinal"], 1);
        assert_eq!(plan.provider_payload(), &payload);
        assert_eq!(plan.intent_digest(), intent);
        assert_eq!(receipt.plan_digest(), plan.plan_digest());
        assert_eq!(receipt.intent_digest(), intent);
        let loaded = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            ),
        );
        assert_eq!(loaded.metadata().snapshots().len(), 2);
    }
    #[test]
    fn rewrite_append_conflict_reuses_operation_manifest_and_rebuilds_parent_and_exact_receipt() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table(&executor, &base);
        append_local_file(&executor, &base, table.identifier(), "first");
        append_local_file(&executor, &base, table.identifier(), "second");
        let source_uuid = table.metadata().uuid();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let observed = captured.clone();
        let catalog = competing_adapter(&base);
        let runtime = base.runtime.resources().catalog_runtime().clone();
        let ident = table.identifier().clone();
        let adapter = recorded_adapter(
            &base,
            Arc::new(move |requirements, updates| {
                assert_eq!(
                    requirements
                        .iter()
                        .filter_map(|r| {
                            if let crate::iceberg::TableRequirement::UuidMatch { uuid } = r {
                                Some(*uuid)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                    vec![source_uuid]
                );
                observed
                    .lock()
                    .unwrap()
                    .push((requirements.to_vec(), updates.to_vec()));
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    let catalog = catalog.clone();
                    let ident = ident.clone();
                    runtime
                        .block_on(async move {
                            append_fixture_file(&catalog, &ident, "concurrent").await;
                        })
                        .unwrap();
                }
            }),
        );
        let lease = real_maintenance_lease(adapter);
        let plan = lease
            .plan_maintenance(local_planning_request(&key, false))
            .unwrap();
        let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
        let outcome = lease
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context())
                    .unwrap(),
            )
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization,
        } = outcome
        else {
            panic!("rewrite should commit after the bounded append conflict")
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let observations = captured.lock().unwrap();
        let parent = |updates: &[crate::iceberg::TableUpdate]| {
            updates
                .iter()
                .find_map(|update| {
                    if let crate::iceberg::TableUpdate::AddSnapshot { snapshot } = update {
                        Some(snapshot.parent_snapshot_id())
                    } else {
                        None
                    }
                })
                .unwrap()
        };
        assert_ne!(parent(&observations[0].1), parent(&observations[1].1));
        let last = &observations[1];
        let expected = serde_json::json!({"identifier":table.identifier(),"requirements":last.0,"updates":last.1});
        let digest = Sha256::digest(serde_json::to_vec(&expected).unwrap());
        let actual: serde_json::Value = serde_json::from_slice(receipt.provider_payload()).unwrap();
        assert_eq!(actual["frozen_request_digest_hex"], hex_encode(digest));
        assert_eq!(actual["attempt_ordinal"], 1);
        assert_eq!(receipt.intent_digest(), plan.intent_digest());
        assert_eq!(receipt.plan_digest(), plan.plan_digest());
        let loaded = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            ),
        );
        assert_eq!(loaded.metadata().snapshots().len(), 4);
        let list = executor
            .block_on(crate::manifest::load_manifest_list(
                loaded.metadata().current_snapshot().unwrap(),
                loaded.file_io(),
                loaded.metadata(),
            ))
            .unwrap();
        assert_eq!(list.entries().len(), 2);
        let operation_manifests = list
            .entries()
            .iter()
            .filter(|m| m.manifest_path.contains(&operation))
            .count();
        assert_eq!(operation_manifests, 1);
        let location = novarocks_fs::FsLocation::parse(loaded.metadata().location()).unwrap();
        let owned = inventory_without_private_artifacts(std::path::Path::new(location.path()))
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(&operation)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            owned
                .iter()
                .filter(|p| !p
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains("-attempt-"))
                .count(),
            1
        );
        assert_eq!(
            owned.len(),
            2,
            "one reused Operation manifest and one successful Attempt list remain"
        );
    }
    #[test]
    fn rewrite_selected_manifest_replacement_replans_to_noop_and_cleans_unadopted_operation_roots()
    {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table(&executor, &base);
        append_local_file(&executor, &base, table.identifier(), "first");
        append_local_file(&executor, &base, table.identifier(), "second");
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let installed = Arc::new(Mutex::new(None));
        let observed = installed.clone();
        let catalog = competing_adapter(&base);
        let runtime = base.runtime.resources().catalog_runtime().clone();
        let ident = table.identifier().clone();
        let adapter = recorded_adapter(
            &base,
            Arc::new(move |_, updates| {
                assert!(updates.iter().any(|update| matches!(
                    update,
                    crate::iceberg::TableUpdate::AddSnapshot { .. }
                )));
                assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
                let catalog = catalog.clone();
                let ident = ident.clone();
                let installed = runtime
                    .block_on(async move {
                        let plan = catalog
                            .plan_maintenance(local_planning_request(&catalog.key, false))
                            .unwrap();
                        let outcome = catalog
                            .execute(
                                ConnectorMetadataMaintenanceExecuteRequest::try_new(
                                    plan,
                                    context(),
                                )
                                .unwrap(),
                            )
                            .unwrap();
                        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome
                        else {
                            panic!("external rewrite must actually publish")
                        };
                        assert_eq!(receipt.summary().rewritten_items, 2);
                        let after =
                            crate::catalog::admission_test_support::load_static_fixture_table(
                                &catalog.runtime,
                                &ident,
                            )
                            .await;
                        let location = after.metadata_location().unwrap().to_owned();
                        let bytes = after
                            .file_io()
                            .new_input(&location)
                            .unwrap()
                            .read()
                            .await
                            .unwrap();
                        (
                            location,
                            bytes.to_vec(),
                            after.metadata().current_snapshot_id().unwrap(),
                        )
                    })
                    .unwrap();
                *observed.lock().unwrap() = Some(installed);
            }),
        );
        let lease = real_maintenance_lease(adapter);
        let plan = lease
            .plan_maintenance(local_planning_request(&key, false))
            .unwrap();
        let payload = plan.provider_payload().clone();
        let intent = plan.intent_digest();
        let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
        let outcome = lease
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context())
                    .unwrap(),
            )
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt,
            finalization,
        } = outcome
        else {
            panic!("fresh single-manifest selection must be NoOp")
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            receipt.summary(),
            ConnectorMetadataMaintenanceReceiptSummary::default()
        );
        assert_eq!(plan.provider_payload(), &payload);
        assert_eq!(plan.intent_digest(), intent);
        assert_eq!(receipt.intent_digest(), intent);
        assert_eq!(receipt.plan_digest(), plan.plan_digest());
        let (location, bytes, external_head) = installed.lock().unwrap().clone().unwrap();
        let attempt: IcebergMetadataMaintenanceReceiptV1 =
            decode_canonical_json(receipt.provider_payload(), "actual retry receipt").unwrap();
        assert_eq!(attempt.attempt_ordinal, 1);
        assert_eq!(attempt.base_snapshot_id, Some(external_head));
        // Independently encode the receipt's specified domain and length frame
        // around the actual competitor location; this is not an unframed hash.
        let mut framed_location = b"novarocks.iceberg.metadata-maintenance.intent.v2\0".to_vec();
        framed_location.extend_from_slice(&(location.len() as u64).to_be_bytes());
        framed_location.extend_from_slice(location.as_bytes());
        assert_eq!(
            attempt.metadata_location_digest_hex,
            hex_encode(Sha256::digest(&framed_location))
        );
        let loaded = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            ),
        );
        assert_eq!(loaded.metadata().snapshots().len(), 3);
        assert_eq!(loaded.metadata().current_snapshot_id(), Some(external_head));
        assert_eq!(loaded.metadata_location(), Some(location.as_str()));
        assert_eq!(
            executor
                .block_on(loaded.file_io().new_input(&location).unwrap().read())
                .unwrap()
                .as_ref(),
            bytes.as_slice()
        );
        let list = executor
            .block_on(crate::manifest::load_manifest_list(
                loaded.metadata().current_snapshot().unwrap(),
                loaded.file_io(),
                loaded.metadata(),
            ))
            .unwrap();
        assert_eq!(list.entries().len(), 1);
        let files = executor
            .block_on(crate::manifest::load_manifest(
                &list.entries()[0],
                loaded.file_io(),
            ))
            .unwrap();
        let live = files
            .entries()
            .iter()
            .filter(|entry| entry.status != crate::iceberg::spec::ManifestStatus::Deleted)
            .map(|entry| entry.data_file().file_path().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            live,
            std::collections::BTreeSet::from([
                format!("{}/data/first.parquet", loaded.metadata().location()),
                format!("{}/data/second.parquet", loaded.metadata().location()),
            ])
        );
        let root = novarocks_fs::FsLocation::parse(loaded.metadata().location()).unwrap();
        assert!(
            inventory_without_private_artifacts(std::path::Path::new(root.path()))
                .iter()
                .all(|path| !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(&operation)),
            "the invalidated Operation manifest and unsuccessful Attempt list must be collected"
        );
    }

    #[test]
    fn rewrite_uuid_replacement_stops_before_a_second_dispatch_and_keeps_replacement_bytes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table(&executor, &base);
        append_local_file(&executor, &base, table.identifier(), "first");
        append_local_file(&executor, &base, table.identifier(), "second");
        let original_uuid = table.metadata().uuid();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let replacement = Arc::new(Mutex::new(None));
        let observed = replacement.clone();
        let catalog = competing_adapter(&base);
        let runtime = base.runtime.resources().catalog_runtime().clone();
        let ident = table.identifier().clone();
        let adapter = recorded_adapter(
            &base,
            Arc::new(move |requirements, _| {
                assert!(
                    requirements.contains(&crate::iceberg::TableRequirement::UuidMatch {
                        uuid: original_uuid,
                    })
                );
                assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
                let catalog = catalog.clone();
                let ident = ident.clone();
                let replaced = runtime
                    .block_on(async move {
                        use crate::catalog::catalog_request::CatalogPublicationRequest;
                        let before =
                            crate::catalog::admission_test_support::load_static_fixture_table(
                                &catalog.runtime,
                                &ident,
                            )
                            .await;
                        let outcome =
                            crate::catalog::admission_test_support::publish_catalog_fixture(
                                &catalog.runtime,
                                catalog.key.incarnation,
                                CatalogPublicationRequest::DropTable {
                                    table: crate::catalog::CatalogTableName::from_identifier(
                                        &ident,
                                    ),
                                    no_op_if_missing: false,
                                    purge: false,
                                    capture: None,
                                },
                            )
                            .await;
                        assert!(
                            matches!(
                                outcome,
                                crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                            ),
                            "{outcome:?}"
                        );
                        let outcome =
                            crate::catalog::admission_test_support::publish_catalog_fixture(
                                &catalog.runtime,
                                catalog.key.incarnation,
                                CatalogPublicationRequest::CreateTable {
                                    namespace: crate::catalog::CatalogNamespaceName::new(
                                        ident.namespace().join("."),
                                    ),
                                    creation: crate::iceberg::TableCreation::builder()
                                        .name(ident.name().to_owned())
                                        .schema(before.metadata().current_schema().as_ref().clone())
                                        .format_version(before.metadata().format_version())
                                        .build(),
                                    no_op_if_exists: false,
                                },
                            )
                            .await;
                        assert!(
                            matches!(
                                outcome,
                                crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                            ),
                            "{outcome:?}"
                        );
                        let after =
                            crate::catalog::admission_test_support::load_static_fixture_table(
                                &catalog.runtime,
                                &ident,
                            )
                            .await;
                        assert_ne!(after.metadata().uuid(), original_uuid);
                        let location = after.metadata_location().unwrap().to_owned();
                        let bytes = after
                            .file_io()
                            .new_input(&location)
                            .unwrap()
                            .read()
                            .await
                            .unwrap();
                        (after.metadata().uuid(), location, bytes.to_vec())
                    })
                    .unwrap();
                *observed.lock().unwrap() = Some(replaced);
            }),
        );
        let lease = real_maintenance_lease(adapter);
        let plan = lease
            .plan_maintenance(local_planning_request(&key, false))
            .unwrap();
        let payload = plan.provider_payload().clone();
        let intent = plan.intent_digest();
        let fixed: IcebergMetadataMaintenancePlanPayloadV1 =
            decode_canonical_json(&payload, "fixed original target").unwrap();
        assert_eq!(fixed.table_uuid, original_uuid.to_string());
        let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
        let outcome = lease
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context())
                    .unwrap(),
            )
            .unwrap();
        assert!(
            matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
            if failure.kind() == ConnectorMutationFailureKind::Conflict
                && cleanup == ExternalMutationFinalization::Complete)
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "foreign UUID must stop before a second mutation"
        );
        assert_eq!(plan.provider_payload(), &payload);
        assert_eq!(plan.intent_digest(), intent);
        let (replacement_uuid, location, bytes) = replacement.lock().unwrap().clone().unwrap();
        let after = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            ),
        );
        assert_eq!(after.metadata().uuid(), replacement_uuid);
        assert!(after.metadata().snapshots().next().is_none());
        assert_eq!(after.metadata_location(), Some(location.as_str()));
        assert_eq!(
            executor
                .block_on(after.file_io().new_input(&location).unwrap().read())
                .unwrap()
                .as_ref(),
            bytes.as_slice()
        );
        let root = novarocks_fs::FsLocation::parse(after.metadata().location()).unwrap();
        assert!(
            inventory_without_private_artifacts(std::path::Path::new(root.path()))
                .iter()
                .all(|path| !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(&operation)),
            "terminal UUID replacement must clean only the original operation's private artifacts"
        );
        for name in ["first", "second"] {
            assert_eq!(
                std::fs::read(
                    std::path::Path::new(root.path())
                        .join("data")
                        .join(format!("{name}.parquet"))
                )
                .unwrap(),
                b"test data"
            );
        }
    }

    #[test]
    fn rewrite_real_lost_reply_keeps_one_dispatch_and_reconciles_the_original_journal() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table(&executor, &base);
        append_local_file(&executor, &base, table.identifier(), "first");
        append_local_file(&executor, &base, table.identifier(), "second");
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let frozen = Arc::new(Mutex::new(Vec::new()));
        let observed = frozen.clone();
        let owner = crate::catalog::admission_test_support::all_admitted_recording_with_lost_reply(
            Arc::clone(base.runtime.novarocks_catalog()),
            Arc::new(move |requirements, updates| {
                seen.fetch_add(1, Ordering::SeqCst);
                observed
                    .lock()
                    .unwrap()
                    .push((requirements.to_vec(), updates.to_vec()));
            }),
        );
        let runtime = IcebergMetadataContext::with_catalog_for_test(
            base.runtime.control_state().clone(),
            base.runtime.resources().clone(),
            owner,
        );
        let actual =
            IcebergMetadataMaintenanceAdapter::new(key.clone(), Arc::new(runtime)).unwrap();
        let lease = real_maintenance_lease(actual);
        let plan = lease
            .plan_maintenance(local_planning_request(&key, false))
            .unwrap();
        let fixed_payload = plan.provider_payload().clone();
        let fixed_intent = plan.intent_digest();
        let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
        let execute = || {
            ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap()
        };
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            lease.execute(execute()).unwrap()
        else {
            panic!("the real Applied reply was lost before its journal observed the proof")
        };
        assert_eq!(calls.load(Ordering::SeqCst), 1, "Unknown must not retry");
        let (journal, entry) =
            PublicationJournal::<MaintenancePublication>::from_evidence(&evidence)
                .unwrap()
                .expect("the actual original journal is retained");
        let descriptor = journal.entry_descriptor(entry).unwrap();
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        let loaded = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            ),
        );
        assert_eq!(
            loaded.metadata().snapshots().len(),
            3,
            "actual Hadoop metadata changed once"
        );
        let current = loaded.metadata().current_snapshot().unwrap();
        let marker: IcebergMetadataMaintenanceMarkerV1 = decode_canonical_json(
            current.summary().additional_properties[MARKER_PROPERTY].as_bytes(),
            "actual snapshot marker",
        )
        .unwrap();
        assert_eq!(
            marker.operation_id_hex,
            hex_encode(plan.operation_id().to_bytes())
        );
        assert_eq!(marker.intent_digest_hex, hex_encode(fixed_intent));
        let snapshot = current.snapshot_id();
        let location = loaded.metadata_location().unwrap().to_owned();
        let metadata_bytes = executor
            .block_on(loaded.file_io().new_input(&location).unwrap().read())
            .unwrap();
        let root = novarocks_fs::FsLocation::parse(loaded.metadata().location()).unwrap();
        let inventory = inventory_without_private_artifacts(std::path::Path::new(root.path()));
        let private = inventory
            .iter()
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(&operation)
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            private.len(),
            2,
            "Unknown retains the real Operation manifest and Attempt list"
        );
        let list = executor
            .block_on(crate::manifest::load_manifest_list(
                current,
                loaded.file_io(),
                loaded.metadata(),
            ))
            .unwrap();
        assert_eq!(list.entries().len(), 1);
        assert!(list.entries()[0].manifest_path.contains(&operation));
        assert!(current.manifest_list().contains(&operation));
        let captured = frozen.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let sent = serde_json::json!({"identifier":table.identifier(),"requirements":captured[0].0,"updates":captured[0].1});
        let expected_digest = hex_encode(Sha256::digest(serde_json::to_vec(&sent).unwrap()));
        drop(captured);
        let mut committed_receipt = None;
        for _ in 0..2 {
            let ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt,
                finalization: ExternalMutationFinalization::Failed(failure),
            } = lease.execute(execute()).unwrap()
            else {
                panic!("read-only marker attribution must refine that original Unknown")
            };
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "reconcile must not republish"
            );
            assert_eq!(plan.provider_payload(), &fixed_payload);
            assert_eq!(plan.intent_digest(), fixed_intent);
            assert_eq!(receipt.plan_digest(), plan.plan_digest());
            assert_eq!(receipt.intent_digest(), fixed_intent);
            let facts: IcebergMetadataMaintenanceReceiptV1 =
                decode_canonical_json(receipt.provider_payload(), "retained actual receipt")
                    .unwrap();
            assert_eq!(
                facts.attempt_ordinal, 0,
                "a positive replay is not a fresh attempt"
            );
            assert_eq!(facts.snapshot_id, Some(snapshot));
            assert_eq!(facts.frozen_request_digest_hex, expected_digest);
            assert_eq!(journal.entry_descriptor(entry).unwrap().key, descriptor.key);
            assert!(matches!(
                journal.catalog_entry_outcome(entry),
                ExternalMutationOutcome::KnownCommitted { .. }
            ));
            // Attribution proves publication, but the original owner cache
            // finalizer was dropped with the reply and was never observed.
            assert!(failure.message().contains("finalization"));
            if let Some(expected) = &committed_receipt {
                assert_eq!(&receipt, expected);
            } else {
                committed_receipt = Some(receipt);
            }
            assert_eq!(
                inventory_without_private_artifacts(std::path::Path::new(root.path())),
                inventory,
                "positive attribution preserves the exact physical roots without a new publication"
            );
            assert_eq!(
                executor
                    .block_on(loaded.file_io().new_input(&location).unwrap().read())
                    .unwrap(),
                metadata_bytes
            );
        }
        for name in ["first", "second"] {
            assert_eq!(
                std::fs::read(
                    std::path::Path::new(root.path())
                        .join("data")
                        .join(format!("{name}.parquet"))
                )
                .unwrap(),
                b"test data"
            );
        }
    }

    #[test]
    fn bounded_confirmed_conflicts_stop_without_metadata_effect_or_operation_artifact_leaks() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (executor, _warehouse, base, key) = adapter();
        let table = create_local_table(&executor, &base);
        append_local_file(&executor, &base, table.identifier(), "first");
        append_local_file(&executor, &base, table.identifier(), "second");
        let catalog = competing_adapter(&base);
        let runtime = base.runtime.resources().catalog_runtime().clone();
        foreign_properties(
            runtime.clone(),
            catalog.clone(),
            table.identifier().clone(),
            HashMap::from([
                ("commit.retry.num-retries".into(), "2".into()),
                ("commit.retry.min-wait-ms".into(), "1".into()),
                ("commit.retry.max-wait-ms".into(), "1".into()),
            ]),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let ident = table.identifier().clone();
        let adapter = recorded_adapter(
            &base,
            Arc::new(move |_, _| {
                let n = seen.fetch_add(1, Ordering::SeqCst);
                foreign_properties(
                    runtime.clone(),
                    catalog.clone(),
                    ident.clone(),
                    HashMap::from([("external-interference".into(), n.to_string())]),
                );
            }),
        );
        let plan = adapter
            .plan_maintenance(local_planning_request(&key, false))
            .unwrap();
        let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
        let outcome = adapter
            .execute(ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context()).unwrap())
            .unwrap();
        assert!(
            matches!(outcome,ExternalMutationOutcome::KnownUncommitted{failure,..} if failure.kind()==ConnectorMutationFailureKind::Conflict)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let location = novarocks_fs::FsLocation::parse(table.metadata().location()).unwrap();
        assert!(
            inventory_without_private_artifacts(std::path::Path::new(location.path()))
                .iter()
                .all(|path| !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(&operation))
        );
    }
    #[test]
    fn terminal_replay_preserves_failed_finalization() {
        let (_executor, _warehouse, adapter, key) = adapter();
        let plan = plan(key, ConnectorMutationOperationId::new(), b"table-a");
        let expected_receipt = receipt(&adapter, &plan);
        let expected_failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "cleanup response was lost",
        );
        adapter.terminal.lock().expect("terminal").insert(
            plan.operation_id(),
            TerminalRecord {
                plan_digest: plan.plan_digest(),
                outcome: ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: expected_receipt.clone(),
                    finalization: ExternalMutationFinalization::Failed(expected_failure.clone()),
                },
            },
        );

        let outcome = adapter
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                    .expect("execute request"),
            )
            .expect("terminal replay");
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Failed(failure),
            ..
        } = outcome
        else {
            panic!("terminal replay must preserve failed finalization")
        };
        assert_eq!(receipt, expected_receipt);
        assert_eq!(failure.kind(), expected_failure.kind());
        assert_eq!(failure.message(), expected_failure.message());
    }

    #[test]
    fn response_loss_terminal_replay_is_idempotent_and_plan_bound() {
        let (executor, _warehouse, adapter, key) = adapter();
        let table = create_local_table(&executor, &adapter);
        let location = novarocks_fs::FsLocation::parse(table.metadata().location()).unwrap();
        let before = inventory_without_private_artifacts(std::path::Path::new(location.path()));
        let operation_id = ConnectorMutationOperationId::new();
        let original_request = local_planning_request(&key, false);
        let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
            operation_id,
            key.clone(),
            original_request.operation().clone(),
            context(),
        )
        .unwrap();
        let exact_plan = adapter.plan_maintenance(request.clone()).unwrap();
        let evidence = ExternalMutationEvidence::try_new(
            PAYLOAD_VERSION,
            adapter.descriptor.clone(),
            adapter.key.incarnation,
            operation_id,
            exact_plan.operation_kind(),
            Bytes::from_static(b"response-loss-evidence"),
        )
        .expect("evidence");
        adapter.terminal.lock().expect("terminal").insert(
            operation_id,
            TerminalRecord {
                plan_digest: exact_plan.plan_digest(),
                outcome: ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "commit response was lost",
                    ),
                    evidence: evidence.clone(),
                },
            },
        );

        for _ in 0..2 {
            let outcome = adapter
                .execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(
                        exact_plan.clone(),
                        context(),
                    )
                    .expect("execute request"),
                )
                .expect("unknown replay");
            let ExternalMutationOutcome::CommitUnknown {
                evidence: replayed, ..
            } = outcome
            else {
                panic!("response-loss replay must remain unknown")
            };
            assert_eq!(replayed.digest(), evidence.digest());
        }

        // This is a fully valid competing provider plan for the same operation,
        // rather than an undecodable test payload which can fail another gate.
        let mut competing_payload: IcebergMetadataMaintenancePlanPayloadV1 =
            decode_canonical_json(exact_plan.provider_payload(), "test fixed intent").unwrap();
        competing_payload.evaluation_time_ms += 1;
        let conflicting = ConnectorMetadataMaintenancePlan::try_new(
            &request,
            intent_digest(&competing_payload).unwrap(),
            exact_plan.summary(),
            canonical_json(&competing_payload, "test competing intent").unwrap(),
        )
        .unwrap();
        adapter
            .payload_for_plan(&conflicting)
            .expect("competing provider plan is valid");
        assert_eq!(conflicting.request_digest(), exact_plan.request_digest());
        assert_ne!(conflicting.intent_digest(), exact_plan.intent_digest());
        assert_ne!(conflicting.plan_digest(), exact_plan.plan_digest());
        let error = adapter
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(conflicting, context())
                    .expect("execute request"),
            )
            .expect_err("same operation with a different plan must conflict");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(error.message(), "Maintenance replay changes the fixed plan");
        // A malformed payload without a terminal entry remains a distinct
        // provider-payload validation failure and never claims this operation.
        let malformed = plan(
            key,
            ConnectorMutationOperationId::new(),
            b"invalid-table-handle",
        );
        let malformed_error = adapter
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(malformed, context()).unwrap(),
            )
            .expect_err("malformed payload must fail before publication");
        assert_eq!(malformed_error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            malformed_error.message().contains("decode"),
            "{malformed_error}"
        );
        assert_eq!(
            inventory_without_private_artifacts(std::path::Path::new(location.path())),
            before
        );
        assert_eq!(
            adapter
                .terminal
                .lock()
                .unwrap()
                .get(&operation_id)
                .unwrap()
                .plan_digest,
            exact_plan.plan_digest()
        );
    }

    #[test]
    fn foreign_generation_is_rejected_before_catalog_access() {
        let (_executor, _warehouse, adapter, key) = adapter();
        let foreign_key = ConnectorProviderBindingKey {
            instance_id: key.instance_id,
            incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
        };
        let foreign = plan(
            foreign_key,
            ConnectorMutationOperationId::new(),
            b"foreign-table",
        );
        let error = adapter
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(foreign, context())
                    .expect("execute request"),
            )
            .expect_err("foreign generation must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("exact connector generation"));
    }

    #[test]
    fn provider_descriptor_remains_exact_for_terminal_receipts() {
        let (_executor, _warehouse, adapter, key) = adapter();
        assert_eq!(
            adapter.descriptor.provider_id,
            ConnectorProviderId::parse("iceberg").expect("provider")
        );
        assert_eq!(adapter.descriptor.instance_id, key.instance_id);
    }

    fn admission_runtime(
        catalog_type: &str,
    ) -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergMetadataContext>,
        std::net::TcpListener,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HMS probe listener");
        listener.set_nonblocking(true).expect("nonblocking probe");
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), catalog_type.to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
        ];
        if catalog_type == "hive" {
            properties.push((
                "hive.metastore.uris".to_string(),
                format!(
                    "thrift://{}",
                    listener.local_addr().expect("listener address")
                ),
            ));
        }
        let configuration = crate::catalog_config::parse_catalog_configuration("ice", &properties)
            .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        (executor, warehouse, runtime, listener)
    }

    fn admission_context(catalog_type: &str) -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("context")
        .with_initiation(if catalog_type == "hive" {
            novarocks_spi::connector::ConnectorRequestInitiation::Statement
        } else {
            novarocks_spi::connector::ConnectorRequestInitiation::Background
        })
    }

    fn admission_table(
        instance: novarocks_spi::connector::ConnectorInstanceId,
    ) -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(instance, Bytes::from_static(br#"{"namespace":"db","table":"absent","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).expect("table handle")
    }

    fn assert_admission_left_no_io(
        warehouse: &tempfile::TempDir,
        listener: &std::net::TcpListener,
    ) {
        assert_eq!(
            std::fs::read_dir(warehouse.path())
                .expect("warehouse inventory")
                .count(),
            0
        );
        let error = listener
            .accept()
            .expect_err("admission must not connect to HMS");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn planning_refuses_hms_and_background_hadoop_before_maintenance_artifacts() {
        for catalog_type in ["hive", "hadoop"] {
            let (_executor, warehouse, runtime, listener) = admission_runtime(catalog_type);
            let key = ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
                incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
            };
            let adapter =
                IcebergMetadataMaintenanceAdapter::new(key.clone(), runtime).expect("adapter");
            for operation in [
                ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(admission_table(
                    key.instance_id.clone(),
                ))
                .expect("rewrite manifests"),
                ConnectorMetadataMaintenanceOperation::expire_table_versions(
                    admission_table(key.instance_id.clone()),
                    None,
                    Some(1),
                )
                .expect("expire snapshots"),
            ] {
                let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
                    ConnectorMutationOperationId::new(),
                    key.clone(),
                    operation,
                    admission_context(catalog_type),
                )
                .expect("request");
                let error = adapter
                    .plan_maintenance(request)
                    .expect_err("catalog admission must refuse planning");
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert!(error.to_string().contains(if catalog_type == "hive" {
                    "read-only compatibility entry"
                } else {
                    "background"
                }));
                assert_admission_left_no_io(&warehouse, &listener);
            }
        }
    }
}
