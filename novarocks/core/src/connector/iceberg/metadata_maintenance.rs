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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use iceberg::NamespaceIdent;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorMetadataMaintenance, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReceiptSummary,
    ConnectorMetadataMaintenanceReconcileRequest, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::catalog::registry::{
    IcebergCatalogEntry, IcebergCatalogRegistry, block_on_iceberg, build_iceberg_catalog,
    load_table,
};
use super::commit::expire_snapshots::{ExpireParams, run_expire_snapshots_once_with_marker};
use super::commit::rewrite_manifests::run_rewrite_manifests_once_with_marker;
use super::provider::decode_data_mutation_table_target;

const PAYLOAD_VERSION: u16 = 1;
const MARKER_VERSION: u16 = 1;
const MARKER_PROPERTY: &str = "novarocks.connector.maintenance.v1";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.state.v1\0";
const IDENTITY_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.identity.v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenancePlanPayloadV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    metadata_location_digest_hex: String,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
    operation_kind: String,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
    artifact_location: String,
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
    state_digest_hex: String,
    base_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceReceiptV1 {
    version: u16,
    snapshot_id: Option<i64>,
    artifact_location: String,
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
    state_digest_hex: String,
    identity_digest_hex: String,
}

#[derive(Clone)]
struct CachedPlan {
    request_digest: [u8; 32],
    plan: ConnectorMetadataMaintenancePlan,
    payload: IcebergMetadataMaintenancePlanPayloadV1,
}

#[derive(Clone)]
struct TerminalRecord {
    plan_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
}

/// FE-only Iceberg adapter.  The registry contains process-local catalog
/// clients and credentials; none cross the SPI payload boundary.
pub(crate) struct IcebergMetadataMaintenanceAdapter {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    instance_id: novarocks_spi::connector::ConnectorInstanceId,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
    plans: Mutex<HashMap<ConnectorMutationOperationId, CachedPlan>>,
    terminal: Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>,
}

impl IcebergMetadataMaintenanceAdapter {
    pub(crate) fn new_registered(
        key: ConnectorExecutionBindingKey,
        instance_id: novarocks_spi::connector::ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            instance_id,
            registry,
            plans: Mutex::new(HashMap::new()),
            terminal: Mutex::new(HashMap::new()),
        })
    }

    fn ensure_owner(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg metadata maintenance does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn entry(&self) -> Result<IcebergCatalogEntry, ConnectorError> {
        self.registry
            .read()
            .map_err(|error| internal(format!("Iceberg maintenance registry lock: {error}")))?
            .get(self.instance_id.as_str())
            .map_err(|error| unavailable(error.to_string()))
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
        let (namespace, table_name) =
            decode_data_mutation_table_target(request.operation().table())?;
        let entry = self.entry()?;
        entry.invalidate_table_cache(&namespace, &table_name);
        let loaded = load_table(&entry, &namespace, &table_name)
            .map_err(|error| unavailable(format!("load Iceberg table for maintenance: {error}")))?;
        let table = loaded.table;
        let metadata = table.metadata();
        let (older_than_ms, retain_last) = match request.operation() {
            ConnectorMetadataMaintenanceOperation::RewriteMetadataLayout { .. } => (None, None),
            ConnectorMetadataMaintenanceOperation::ExpireTableVersions {
                older_than_ms,
                retain_last,
                ..
            } => (*older_than_ms, *retain_last),
        };
        let state_digest = state_digest(
            metadata.uuid().to_string().as_bytes(),
            table.metadata_location(),
            metadata
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
        );
        let payload_seed = format!(
            "{}:{}:{}",
            hex::encode(request.operation_id().to_bytes()),
            request.operation().kind(),
            hex::encode(state_digest)
        );
        let artifact_location = maintenance_artifact_location(
            table.metadata_location(),
            request.operation().kind(),
            &payload_seed,
        )?;
        let payload = IcebergMetadataMaintenancePlanPayloadV1 {
            version: PAYLOAD_VERSION,
            namespace,
            table: table_name,
            table_uuid: metadata.uuid().to_string(),
            metadata_location_digest_hex: hex::encode(metadata_location_digest(
                table.metadata_location(),
            )),
            base_snapshot_id: metadata
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            schema_id: metadata.current_schema_id(),
            default_spec_id: metadata.default_partition_spec_id(),
            operation_kind: request.operation().kind().to_string(),
            older_than_ms,
            retain_last,
            artifact_location,
        };
        let artifact = canonical_json(&payload, "Iceberg metadata maintenance artifact")?;
        if artifact.len() > 1024 * 1024 {
            return Err(resource_exhausted(
                "Iceberg maintenance artifact part exceeds 1 MiB",
            ));
        }
        let output = table
            .file_io()
            .new_output(&payload.artifact_location)
            .map_err(|error| unavailable(format!("create maintenance artifact: {error}")))?;
        block_on_iceberg(async move { output.write(artifact).await })
            .map_err(unavailable)?
            .map_err(|error| unavailable(format!("write maintenance artifact: {error}")))?;
        let summary = ConnectorMetadataMaintenancePlanSummary::new(
            u64::from(payload.base_snapshot_id.is_some()),
            0,
            0,
            0,
            0,
        );
        Ok((payload, state_digest, summary))
    }

    fn payload_for_plan(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
    ) -> Result<IcebergMetadataMaintenancePlanPayloadV1, ConnectorError> {
        if let Some(cached) = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance plan lock: {error}")))?
            .get(&plan.operation_id())
            .cloned()
        {
            if cached.plan.plan_digest() == plan.plan_digest() {
                return Ok(cached.payload);
            }
            return Err(invalid(
                "Iceberg metadata maintenance plan digest conflicts",
            ));
        }
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
            identity_digest_hex: hex::encode(identity_digest(&self.descriptor, &self.key, plan)),
            incarnation_hex: hex::encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex::encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex::encode(plan.request_digest()),
            plan_digest_hex: hex::encode(plan.plan_digest()),
            state_digest_hex: hex::encode(plan.state_digest()),
            base_snapshot_id: payload.base_snapshot_id,
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
            operation_id_hex: hex::encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex::encode(plan.request_digest()),
            plan_digest_hex: hex::encode(plan.plan_digest()),
            state_digest_hex: hex::encode(plan.state_digest()),
            identity_digest_hex: hex::encode(identity_digest(&self.descriptor, &self.key, plan)),
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

    fn lookup_marker(
        &self,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        marker: &IcebergMetadataMaintenanceMarkerV1,
    ) -> Result<MarkerLookup, ConnectorError> {
        let entry = self.entry()?;
        entry.invalidate_table_cache(&payload.namespace, &payload.table);
        let loaded = load_table(&entry, &payload.namespace, &payload.table).map_err(|error| {
            unavailable(format!("load Iceberg table for marker lookup: {error}"))
        })?;
        let metadata = loaded.table.metadata();
        if let Some(raw) = metadata.properties().get(MARKER_PROPERTY) {
            let stored: IcebergMetadataMaintenanceMarkerV1 =
                decode_canonical_json(raw.as_bytes(), "Iceberg metadata maintenance marker")?;
            if stored.operation_id_hex == marker.operation_id_hex {
                return Ok(
                    if stored.identity_digest_hex == marker.identity_digest_hex {
                        MarkerLookup::Matching {
                            snapshot_id: metadata
                                .current_snapshot()
                                .map(|snapshot| snapshot.snapshot_id()),
                        }
                    } else {
                        MarkerLookup::Conflicting
                    },
                );
            }
        }
        for snapshot in metadata.snapshots() {
            if let Some(raw) = snapshot
                .summary()
                .additional_properties
                .get(MARKER_PROPERTY)
            {
                let stored: IcebergMetadataMaintenanceMarkerV1 =
                    decode_canonical_json(raw.as_bytes(), "Iceberg metadata maintenance marker")?;
                if stored.operation_id_hex == marker.operation_id_hex {
                    return Ok(
                        if stored.identity_digest_hex == marker.identity_digest_hex {
                            MarkerLookup::Matching {
                                snapshot_id: Some(snapshot.snapshot_id()),
                            }
                        } else {
                            MarkerLookup::Conflicting
                        },
                    );
                }
            }
        }
        Ok(MarkerLookup::Missing)
    }

    fn execute_once(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        marker: &IcebergMetadataMaintenanceMarkerV1,
    ) -> Result<ConnectorMetadataMaintenanceReceipt, ExecFailure> {
        let entry = self.entry().map_err(ExecFailure::KnownUncommitted)?;
        entry.invalidate_table_cache(&payload.namespace, &payload.table);
        let catalog = build_iceberg_catalog(&entry)
            .map_err(|error| ExecFailure::KnownUncommitted(unavailable(error.to_string())))?;
        let loaded = load_table(&entry, &payload.namespace, &payload.table)
            .map_err(|error| ExecFailure::KnownUncommitted(unavailable(error.to_string())))?;
        validate_frozen_state(&loaded.table, plan, payload)
            .map_err(ExecFailure::KnownUncommitted)?;
        let marker_bytes = canonical_json(marker, "Iceberg metadata maintenance marker")
            .map_err(ExecFailure::KnownUncommitted)?;
        let marker_text = String::from_utf8(marker_bytes.to_vec()).map_err(|error| {
            ExecFailure::KnownUncommitted(internal(format!("marker UTF-8: {error}")))
        })?;
        let action = match plan.operation_kind() {
            novarocks_spi::connector::REWRITE_METADATA_LAYOUT_KIND => {
                block_on_iceberg(run_rewrite_manifests_once_with_marker(
                    catalog,
                    iceberg::TableIdent::new(
                        NamespaceIdent::new(payload.namespace.clone()),
                        payload.table.clone(),
                    ),
                    Some(marker_text),
                ))
                .map_err(|error| ExecFailure::Unknown(unavailable(error)))
                .and_then(|result| result.map(|_| ()).map_err(classify_iceberg_error))
            }
            novarocks_spi::connector::EXPIRE_TABLE_VERSIONS_KIND => {
                block_on_iceberg(run_expire_snapshots_once_with_marker(
                    catalog,
                    iceberg::TableIdent::new(
                        NamespaceIdent::new(payload.namespace.clone()),
                        payload.table.clone(),
                    ),
                    ExpireParams {
                        older_than_ms: payload.older_than_ms,
                        retain_last: payload.retain_last,
                    },
                    Some(marker_text),
                ))
                .map_err(|error| ExecFailure::Unknown(unavailable(error)))
                .and_then(|result| result.map(|_| ()).map_err(classify_iceberg_error))
            }
            _ => Err(ExecFailure::KnownUncommitted(invalid(
                "unsupported metadata maintenance operation",
            ))),
        };
        action?;
        entry.invalidate_table_cache(&payload.namespace, &payload.table);
        let committed =
            load_table(&entry, &payload.namespace, &payload.table).map_err(|error| {
                ExecFailure::Unknown(unavailable(format!(
                    "reload committed Iceberg table: {error}"
                )))
            })?;
        let snapshot_id = committed
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        ConnectorMetadataMaintenanceReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            ConnectorMetadataMaintenanceReceiptSummary::default(),
            canonical_json(
                &IcebergMetadataMaintenanceReceiptV1 {
                    version: PAYLOAD_VERSION,
                    snapshot_id,
                    artifact_location: payload.artifact_location.clone(),
                },
                "Iceberg metadata maintenance receipt",
            )
            .map_err(ExecFailure::KnownUncommitted)?,
        )
        .map_err(ExecFailure::KnownUncommitted)
    }
}

impl ConnectorMetadataMaintenance for IcebergMetadataMaintenanceAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_maintenance(
        &self,
        request: ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
        request.validate()?;
        self.ensure_owner(request.owner())?;
        let mut plans = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance plan lock: {error}")))?;
        if let Some(existing) = plans.get(&request.operation_id()) {
            if existing.request_digest == request.request_digest() {
                return Ok(existing.plan.clone());
            }
            return Err(invalid(
                "Iceberg metadata maintenance operation was replayed with a different request",
            ));
        }
        let (payload, state_digest, summary) = self.plan_payload(&request)?;
        let provider_payload = canonical_json(&payload, "Iceberg metadata maintenance plan")?;
        let plan = ConnectorMetadataMaintenancePlan::try_new(
            &request,
            state_digest,
            summary,
            provider_payload,
        )?;
        plans.insert(
            request.operation_id(),
            CachedPlan {
                request_digest: request.request_digest(),
                plan: plan.clone(),
                payload,
            },
        );
        Ok(plan)
    }

    fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        if let Some(terminal) = self
            .terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance terminal lock: {error}")))?
            .get(&request.plan.operation_id())
            .cloned()
        {
            if terminal.plan_digest == request.plan.plan_digest() {
                return Ok(terminal.outcome);
            }
            return Err(invalid(
                "Iceberg metadata maintenance execute conflicts with terminal plan",
            ));
        }
        let payload = self.payload_for_plan(&request.plan)?;
        let marker = self.marker(&request.plan, &payload);
        let outcome = match self.lookup_marker(&payload, &marker)? {
            MarkerLookup::Matching { snapshot_id } => ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt_from_marker(&request.plan, &payload, snapshot_id)?,
                finalization: ExternalMutationFinalization::Complete,
            },
            MarkerLookup::Conflicting => ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg maintenance marker conflicts with operation",
                ),
                evidence: self.evidence(&request.plan, &payload)?,
            },
            MarkerLookup::Missing => match self.execute_once(&request.plan, &payload, &marker) {
                Ok(receipt) => ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                },
                Err(ExecFailure::KnownUncommitted(error)) => {
                    ExternalMutationOutcome::KnownUncommitted {
                        failure: failure(ConnectorMutationFailureKind::Conflict, error.to_string()),
                    }
                }
                Err(ExecFailure::Unknown(error)) => ExternalMutationOutcome::CommitUnknown {
                    failure: failure(ConnectorMutationFailureKind::Unavailable, error.to_string()),
                    evidence: self.evidence(&request.plan, &payload)?,
                },
            },
        };
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

    fn reconcile(
        &self,
        request: ConnectorMetadataMaintenanceReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        let payload = self.payload_for_plan(&request.plan)?;
        let marker = self.marker(&request.plan, &payload);
        match self.lookup_marker(&payload, &marker)? {
            MarkerLookup::Matching { snapshot_id } => Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt_from_marker(&request.plan, &payload, snapshot_id)?,
                finalization: ExternalMutationFinalization::Complete,
            }),
            MarkerLookup::Conflicting => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg maintenance marker conflicts during reconcile",
                ),
                evidence: request
                    .evidence
                    .unwrap_or(self.evidence(&request.plan, &payload)?),
            }),
            MarkerLookup::Missing => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Unavailable,
                    "Iceberg maintenance marker is not visible",
                ),
                evidence: request
                    .evidence
                    .unwrap_or(self.evidence(&request.plan, &payload)?),
            }),
        }
    }
}

impl IcebergMetadataMaintenanceAdapter {
    fn receipt_from_marker(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        snapshot_id: Option<i64>,
    ) -> Result<ConnectorMetadataMaintenanceReceipt, ConnectorError> {
        ConnectorMetadataMaintenanceReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            ConnectorMetadataMaintenanceReceiptSummary::default(),
            canonical_json(
                &IcebergMetadataMaintenanceReceiptV1 {
                    version: PAYLOAD_VERSION,
                    snapshot_id,
                    artifact_location: payload.artifact_location.clone(),
                },
                "Iceberg metadata maintenance receipt",
            )?,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerLookup {
    Matching { snapshot_id: Option<i64> },
    Conflicting,
    Missing,
}

enum ExecFailure {
    KnownUncommitted(ConnectorError),
    Unknown(ConnectorError),
}

fn validate_payload_for_plan(
    plan: &ConnectorMetadataMaintenancePlan,
    payload: &IcebergMetadataMaintenancePlanPayloadV1,
) -> Result<(), ConnectorError> {
    if payload.version != PAYLOAD_VERSION
        || payload.operation_kind != plan.operation_kind()
        || payload.metadata_location_digest_hex.len() != 64
        || payload.artifact_location.len()
            > novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_PATH_BYTES
    {
        return Err(invalid(
            "Iceberg metadata maintenance plan payload does not match SPI plan",
        ));
    }
    Ok(())
}

fn validate_frozen_state(
    table: &iceberg::table::Table,
    plan: &ConnectorMetadataMaintenancePlan,
    payload: &IcebergMetadataMaintenancePlanPayloadV1,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    let actual = state_digest(
        metadata.uuid().to_string().as_bytes(),
        table.metadata_location(),
        metadata
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id()),
        metadata.current_schema_id(),
        metadata.default_partition_spec_id(),
    );
    if actual != plan.state_digest()
        || metadata.uuid().to_string() != payload.table_uuid
        || metadata_location_digest(table.metadata_location())
            != decode_digest(&payload.metadata_location_digest_hex)?
    {
        return Err(invalid(
            "Iceberg metadata maintenance plan base state has changed",
        ));
    }
    Ok(())
}

fn state_digest(
    table_uuid: &[u8],
    metadata_location: Option<&str>,
    snapshot_id: Option<i64>,
    schema_id: i32,
    spec_id: i32,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    digest_bytes(&mut hasher, table_uuid);
    digest_bytes(
        &mut hasher,
        metadata_location.unwrap_or_default().as_bytes(),
    );
    hasher.update(snapshot_id.unwrap_or_default().to_be_bytes());
    hasher.update([u8::from(snapshot_id.is_some())]);
    hasher.update(schema_id.to_be_bytes());
    hasher.update(spec_id.to_be_bytes());
    hasher.finalize().into()
}

fn metadata_location_digest(value: Option<&str>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    digest_bytes(&mut hasher, value.unwrap_or_default().as_bytes());
    hasher.finalize().into()
}

fn identity_digest(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorExecutionBindingKey,
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

fn maintenance_artifact_location(
    metadata_location: Option<&str>,
    operation_kind: &str,
    seed: &str,
) -> Result<String, ConnectorError> {
    let location =
        metadata_location.ok_or_else(|| invalid("Iceberg table has no metadata location"))?;
    let (root, _) = location
        .rsplit_once("/metadata/")
        .ok_or_else(|| invalid("Iceberg metadata location has no table metadata directory"))?;
    let digest = Sha256::digest(seed.as_bytes());
    let path = format!(
        "{root}/_novarocks/maintenance/v1/{operation_kind}/{}/plan.json",
        hex::encode(digest)
    );
    if path.len() > novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_PATH_BYTES {
        return Err(resource_exhausted(
            "Iceberg maintenance artifact path exceeds hard limit",
        ));
    }
    Ok(path)
}

fn classify_iceberg_error(error: iceberg::Error) -> ExecFailure {
    match error.kind() {
        iceberg::ErrorKind::CatalogCommitConflicts
        | iceberg::ErrorKind::PreconditionFailed
        | iceberg::ErrorKind::DataInvalid
        | iceberg::ErrorKind::TableNotFound => {
            ExecFailure::KnownUncommitted(invalid(error.to_string()))
        }
        iceberg::ErrorKind::Unexpected => ExecFailure::Unknown(unavailable(error.to_string())),
        _ => ExecFailure::KnownUncommitted(invalid(error.to_string())),
    }
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
    let bytes = hex::decode(value).map_err(|_| invalid("maintenance digest is not hex"))?;
    bytes
        .try_into()
        .map_err(|_| invalid("maintenance digest has invalid length"))
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

fn resource_exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message.into())
}
