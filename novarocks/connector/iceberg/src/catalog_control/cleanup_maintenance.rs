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

//! Exact-generation durable orphan cleanup.
//!
//! Candidate discovery occurs once during planning. Execute and reconcile use
//! only the immutable, content-addressed manifest and never list the table a
//! second time.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    BatchReceipt, BatchReceiptSummary, CandidatePage, ConnectorCleanupCandidate,
    ConnectorCleanupCandidatePageRequest, ConnectorCleanupExecuteRequest,
    ConnectorCleanupFinalizeRequest, ConnectorCleanupMaintenance, ConnectorCleanupOperationId,
    ConnectorCleanupPlan, ConnectorCleanupPlanSummary, ConnectorCleanupPlanningRequest,
    ConnectorCleanupPrepareRequest, ConnectorCleanupReconcileRequest, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, PreparedBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::cleanup_candidates::{
    ScannedFile, canonical_object_mtime_ms, collect_orphan_candidates,
};
use super::owned_ref_cleanup::{
    OwnedRefCandidate, collect_owned_ref_candidates, matches_owned_ref_candidate,
};
use crate::control_provider::IcebergTablePayload;
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::io::FileIO;

const ARTIFACT_VERSION: u16 = 2;
const MAX_RECORDS: usize = 262_144;
const MAX_PARTS: usize = 64;
const MAX_PART_BYTES: usize = 1024 * 1024;
const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_OBJECTS: usize = 1024;
const MAX_REASON_CHARS: usize = 1024;
const MANIFEST_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.manifest.v1\0";
const PART_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.part.v1\0";
const BATCH_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.batch.v1\0";
const RECEIPT_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.receipt.v1\0";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanPayload {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    artifact_root: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalManifest {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    older_than_ms: i64,
    phase: CleanupPhase,
    records: Vec<ManifestRecord>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum CleanupPhase {
    OwnedRefRetire,
    ObjectSweep,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRoot {
    version: u16,
    manifest_digest_hex: String,
    namespace: String,
    table: String,
    table_uuid: String,
    older_than_ms: i64,
    phase: CleanupPhase,
    record_count: u32,
    total_bytes: u64,
    parts: Vec<PartReference>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartReference {
    index: u16,
    digest_hex: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPart {
    version: u16,
    records: Vec<ManifestRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRecord {
    ordinal: u32,
    candidate: ManifestCandidate,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ManifestCandidate {
    Object {
        location: String,
        identity: ObjectIdentity,
    },
    OwnedRef {
        name: String,
        head_snapshot_id: i64,
        provenance_version: u16,
        created_at_ms: i64,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ObjectIdentity {
    Version {
        version: String,
        size: u64,
        mtime_ms: i64,
    },
    Etag {
        etag: String,
        size: u64,
        mtime_ms: i64,
    },
    SizeMtime {
        size: u64,
        mtime_ms: i64,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedPayload {
    version: u16,
    artifact_root: String,
    batch_ordinal: u32,
    first_ordinal: u32,
    record_count: u32,
    batch_digest_hex: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptPayload {
    version: u16,
    receipt_location: String,
    receipt_digest_hex: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptArtifact {
    version: u16,
    batch_digest_hex: String,
    records: Vec<ReceiptRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRecord {
    ordinal: u32,
    outcome: ObjectOutcome,
    reason: Option<String>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ObjectOutcome {
    Deleted,
    AlreadyAbsent,
    Failed,
    Unknown,
}

#[derive(Clone)]
struct CachedPlan {
    request_digest: [u8; 32],
    plan: ConnectorCleanupPlan,
}

pub struct IcebergCleanupMaintenanceAdapter {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergControlRuntime>,
    plans: Mutex<HashMap<ConnectorCleanupOperationId, CachedPlan>>,
}

impl IcebergCleanupMaintenanceAdapter {
    pub fn new(
        key: ConnectorExecutionBindingKey,
        runtime: Arc<IcebergControlRuntime>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
                instance_id: key.instance_id.clone(),
            },
            key,
            runtime,
            plans: Mutex::new(HashMap::new()),
        })
    }

    fn ensure_owner(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg cleanup does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn plan_payload(&self, plan: &ConnectorCleanupPlan) -> Result<PlanPayload, ConnectorError> {
        if let Some(cached) = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg cleanup plan cache lock poisoned"))?
            .get(&plan.operation_id())
            .cloned()
            && cached.plan.plan_digest() != plan.plan_digest()
        {
            return Err(invalid(
                "Iceberg cleanup operation conflicts with its generation-local plan",
            ));
        }
        let payload: PlanPayload = decode_canonical(plan.provider_payload(), "cleanup plan")?;
        if payload.version != ARTIFACT_VERSION
            || payload.artifact_root.is_empty()
            || !payload
                .artifact_root
                .ends_with(&hex_encode(plan.manifest_digest()))
        {
            return Err(corrupt("Iceberg cleanup plan payload is invalid"));
        }
        Ok(payload)
    }

    fn table_file_io(&self, payload: &PlanPayload) -> Result<(FileIO, String), ConnectorError> {
        self.runtime
            .control_state()
            .invalidate_table(&payload.namespace, &payload.table);
        let physical = self
            .runtime
            .load_table(&payload.namespace, &payload.table)
            .map_err(unavailable)?;
        if physical.table.metadata().uuid().to_string() != payload.table_uuid {
            return Err(corrupt(
                "Iceberg cleanup table incarnation no longer matches its frozen manifest",
            ));
        }
        Ok((
            physical.table.file_io().clone(),
            physical
                .table
                .metadata()
                .location()
                .trim_end_matches('/')
                .to_string(),
        ))
    }

    fn manifest(
        &self,
        plan: &ConnectorCleanupPlan,
        payload: &PlanPayload,
    ) -> Result<Vec<ManifestRecord>, ConnectorError> {
        let (file_io, table_location) = self.table_file_io(payload)?;
        let expected_prefix = format!(
            "{table_location}/_novarocks/maintenance/v4/orphan-cleanup/{}/",
            hex_encode(plan.operation_id().to_bytes())
        );
        if !payload.artifact_root.starts_with(&expected_prefix) {
            return Err(corrupt(
                "Iceberg cleanup artifact root does not match its frozen table",
            ));
        }
        read_manifest(
            &self.runtime,
            &file_io,
            &payload.artifact_root,
            plan.manifest_digest(),
        )
    }

    fn prepared_records(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
    ) -> Result<(PlanPayload, Vec<ManifestRecord>), ConnectorError> {
        let payload = self.plan_payload(plan)?;
        let evidence: PreparedPayload =
            decode_canonical(prepared.evidence_payload(), "cleanup prepared evidence")?;
        if evidence.version != ARTIFACT_VERSION
            || evidence.artifact_root != payload.artifact_root
            || evidence.batch_ordinal != prepared.batch_ordinal()
            || evidence.record_count == 0
            || evidence.record_count as usize > MAX_BATCH_OBJECTS
            || evidence.batch_digest_hex != hex_encode(prepared.batch_digest())
            || prepared.batch_ordinal() >= plan.summary().batch_count()
        {
            return Err(corrupt("Iceberg cleanup prepared evidence is invalid"));
        }
        let records = self.manifest(plan, &payload)?;
        let start = evidence.first_ordinal as usize;
        let end = start
            .checked_add(evidence.record_count as usize)
            .ok_or_else(|| corrupt("Iceberg cleanup batch range overflows"))?;
        let batch = records
            .get(start..end)
            .ok_or_else(|| corrupt("Iceberg cleanup batch exceeds its manifest"))?
            .to_vec();
        if batch_digest(&batch) != prepared.batch_digest() {
            return Err(corrupt("Iceberg cleanup batch digest is invalid"));
        }
        Ok((payload, batch))
    }

    fn existing_receipt(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
        payload: &PlanPayload,
    ) -> Result<Option<BatchReceipt>, ConnectorError> {
        let (file_io, _) = self.table_file_io(payload)?;
        let location = receipt_location(payload, prepared.batch_ordinal());
        let Some(bytes) = read_optional(&self.runtime, &file_io, &location, MAX_PART_BYTES * 2)?
        else {
            return Ok(None);
        };
        let artifact: ReceiptArtifact = decode_canonical(&bytes, "cleanup receipt")?;
        if artifact.version != ARTIFACT_VERSION
            || artifact.batch_digest_hex != hex_encode(prepared.batch_digest())
            || artifact.records.len() > MAX_BATCH_OBJECTS
        {
            return Err(corrupt("Iceberg cleanup receipt is invalid"));
        }
        let digest = domain_digest(RECEIPT_DOMAIN, &bytes);
        receipt_value(
            &self.descriptor,
            &self.key,
            plan,
            prepared,
            location,
            digest,
            &artifact.records,
        )
        .map(Some)
    }

    fn persist_receipt(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
        payload: &PlanPayload,
        records: Vec<ReceiptRecord>,
    ) -> Result<BatchReceipt, ConnectorError> {
        let (file_io, _) = self.table_file_io(payload)?;
        let location = receipt_location(payload, prepared.batch_ordinal());
        let bytes = canonical(&ReceiptArtifact {
            version: ARTIFACT_VERSION,
            batch_digest_hex: hex_encode(prepared.batch_digest()),
            records,
        })?;
        if bytes.len() > MAX_PART_BYTES * 2 {
            return Err(exhausted("Iceberg cleanup receipt exceeds 2 MiB"));
        }
        write_immutable(&self.runtime, &file_io, &location, bytes.clone())?;
        let artifact: ReceiptArtifact = decode_canonical(&bytes, "cleanup receipt")?;
        receipt_value(
            &self.descriptor,
            &self.key,
            plan,
            prepared,
            location,
            domain_digest(RECEIPT_DOMAIN, &bytes),
            &artifact.records,
        )
    }
}

impl ConnectorCleanupMaintenance for IcebergCleanupMaintenanceAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_cleanup(
        &self,
        request: ConnectorCleanupPlanningRequest,
    ) -> Result<ConnectorCleanupPlan, ConnectorError> {
        request.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.owner())?;
        if let Some(cached) = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg cleanup plan cache lock poisoned"))?
            .get(&request.operation_id())
            .cloned()
        {
            if cached.request_digest == request.request_digest() {
                return Ok(cached.plan);
            }
            return Err(invalid(
                "Iceberg cleanup operation was replayed with a different request",
            ));
        }
        let target: IcebergTablePayload =
            serde_json::from_slice(request.operation().table().payload())
                .map_err(|error| invalid(format!("decode Iceberg cleanup table: {error}")))?;
        if request.operation().table().owner() != &self.key.instance_id
            || target.metadata_table_type.is_some()
        {
            return Err(invalid("Iceberg cleanup requires an owned base table"));
        }
        self.runtime
            .control_state()
            .invalidate_table(&target.namespace, &target.table);
        let physical = self
            .runtime
            .load_table(&target.namespace, &target.table)
            .map_err(unavailable)?;
        let table = physical.table;
        let older_than_ms = request.operation().older_than_ms();
        // A ref retirement is a separate GC phase. Once a Catalog ref is
        // removed, the live set used by object discovery is stale by
        // definition; a later operation must reload metadata before sweeping.
        let owned_refs = collect_owned_ref_candidates(
            table.metadata(),
            &target.namespace,
            &target.table,
            older_than_ms,
        );
        let (phase, records) = if owned_refs.is_empty() {
            let table_for_scan = table.clone();
            let object_store = physical.object_store_config.clone();
            let scanned = self
                .runtime
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    collect_orphan_candidates(&table_for_scan, older_than_ms, object_store.as_ref())
                        .await
                })
                .map_err(unavailable)?
                .map_err(unavailable)?;
            (
                CleanupPhase::ObjectSweep,
                records_from_candidates(&scanned, &table, physical.object_store_config.as_ref())?,
            )
        } else {
            (
                CleanupPhase::OwnedRefRetire,
                owned_refs
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, candidate)| ManifestRecord {
                        ordinal: ordinal as u32,
                        candidate: ManifestCandidate::OwnedRef {
                            name: candidate.name,
                            head_snapshot_id: candidate.head_snapshot_id,
                            provenance_version: candidate.provenance_version,
                            created_at_ms: candidate.created_at_ms,
                        },
                    })
                    .collect(),
            )
        };
        if records.len() > MAX_RECORDS {
            return Err(exhausted("Iceberg cleanup manifest exceeds 262144 records"));
        }
        let logical = LogicalManifest {
            version: ARTIFACT_VERSION,
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            table_uuid: table.metadata().uuid().to_string(),
            older_than_ms,
            phase,
            records,
        };
        let logical_bytes = canonical(&logical)?;
        if logical_bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(exhausted("Iceberg cleanup manifest exceeds 64 MiB"));
        }
        let manifest_digest = domain_digest(MANIFEST_DOMAIN, &logical_bytes);
        let artifact_root = format!(
            "{}/_novarocks/maintenance/v4/orphan-cleanup/{}/{}",
            table.metadata().location().trim_end_matches('/'),
            hex_encode(request.operation_id().to_bytes()),
            hex_encode(manifest_digest)
        );
        let part_count = write_manifest(
            &self.runtime,
            table.file_io(),
            &artifact_root,
            manifest_digest,
            &logical,
        )?;
        let summary = ConnectorCleanupPlanSummary::try_new(
            logical.records.len() as u64,
            logical.records.iter().map(manifest_record_size).sum(),
            part_count,
            logical.records.len().div_ceil(MAX_BATCH_OBJECTS) as u32,
        )?;
        let state = state_digest(
            table.metadata().uuid().to_string().as_bytes(),
            table.metadata_location(),
            table.metadata().current_snapshot_id(),
            table.metadata().current_schema_id(),
            table.metadata().default_partition_spec_id(),
        );
        let payload = PlanPayload {
            version: ARTIFACT_VERSION,
            namespace: target.namespace,
            table: target.table,
            table_uuid: table.metadata().uuid().to_string(),
            artifact_root,
        };
        let plan = ConnectorCleanupPlan::try_new(
            &request,
            state,
            manifest_digest,
            summary,
            canonical(&payload)?,
        )?;
        self.plans
            .lock()
            .map_err(|_| internal("Iceberg cleanup plan cache lock poisoned"))?
            .insert(
                request.operation_id(),
                CachedPlan {
                    request_digest: request.request_digest(),
                    plan: plan.clone(),
                },
            );
        Ok(plan)
    }

    fn prepare_batch(
        &self,
        request: ConnectorCleanupPrepareRequest,
    ) -> Result<PreparedBatch, ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())?;
        let payload = self.plan_payload(&request.plan)?;
        let records = self.manifest(&request.plan, &payload)?;
        let start = request.batch_ordinal as usize * MAX_BATCH_OBJECTS;
        let end = (start + MAX_BATCH_OBJECTS).min(records.len());
        let batch = records
            .get(start..end)
            .filter(|batch| !batch.is_empty())
            .ok_or_else(|| invalid("Iceberg cleanup batch is outside its manifest"))?;
        let digest = batch_digest(batch);
        PreparedBatch::try_new(
            self.key.clone(),
            request.plan.operation_id(),
            request.plan.plan_digest(),
            request.plan.manifest_digest(),
            request.batch_ordinal,
            digest,
            canonical(&PreparedPayload {
                version: ARTIFACT_VERSION,
                artifact_root: payload.artifact_root,
                batch_ordinal: request.batch_ordinal,
                first_ordinal: start as u32,
                record_count: batch.len() as u32,
                batch_digest_hex: hex_encode(digest),
            })?,
        )
    }

    fn execute_batch(
        &self,
        request: ConnectorCleanupExecuteRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())?;
        let (payload, batch) = self.prepared_records(&request.plan, &request.prepared)?;
        if let Some(receipt) = self.existing_receipt(&request.plan, &request.prepared, &payload)? {
            return Ok(receipt);
        }
        let config = self.runtime.control_state().object_store_config();
        let outcomes = execute_frozen_batch(&self.runtime, &payload, &batch, config)?;
        self.persist_receipt(&request.plan, &request.prepared, &payload, outcomes)
    }

    fn reconcile_batch(
        &self,
        request: ConnectorCleanupReconcileRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())?;
        let (payload, batch) = self.prepared_records(&request.plan, &request.prepared)?;
        if let Some(receipt) = self.existing_receipt(&request.plan, &request.prepared, &payload)? {
            return Ok(receipt);
        }
        let config = self.runtime.control_state().object_store_config();
        let outcomes = reconcile_frozen_batch(&self.runtime, &payload, &batch, config)?;
        self.persist_receipt(&request.plan, &request.prepared, &payload, outcomes)
    }

    fn read_candidate_page(
        &self,
        request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<CandidatePage, ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())?;
        let payload = self.plan_payload(&request.plan)?;
        let records = self.manifest(&request.plan, &payload)?;
        let start = request.offset as usize;
        if start > records.len() {
            return Err(invalid("Iceberg cleanup page offset exceeds its manifest"));
        }
        let end = (start + request.limit as usize).min(records.len());
        CandidatePage::try_new(
            self.key.clone(),
            request.plan.operation_id(),
            request.plan.manifest_digest(),
            request.offset,
            records[start..end]
                .iter()
                .map(manifest_candidate_projection)
                .collect(),
            end == records.len(),
        )
    }

    fn finalize_terminal(
        &self,
        request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())
    }
}

fn records_from_candidates(
    files: &[ScannedFile],
    table: &crate::iceberg::table::Table,
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ManifestRecord>, ConnectorError> {
    let supports_version =
        crate::fs_io::resolve_access_for_location(table.metadata().location(), config)
            .map_err(unavailable)?
            .operator()
            .info()
            .full_capability()
            .delete_with_version;
    files
        .iter()
        .enumerate()
        .map(|(ordinal, file)| {
            let size = file
                .size
                .ok_or_else(|| invalid("orphan candidate has no reliable size"))?;
            if file.last_modified_ms == i64::MAX {
                return Err(invalid(
                    "orphan candidate has no reliable modification time",
                ));
            }
            let identity = match (&file.version, &file.etag) {
                (Some(version), _) if supports_version => ObjectIdentity::Version {
                    version: version.clone(),
                    size,
                    mtime_ms: file.last_modified_ms,
                },
                (_, Some(etag)) => ObjectIdentity::Etag {
                    etag: etag.clone(),
                    size,
                    mtime_ms: file.last_modified_ms,
                },
                _ => ObjectIdentity::SizeMtime {
                    size,
                    mtime_ms: file.last_modified_ms,
                },
            };
            Ok(ManifestRecord {
                ordinal: ordinal as u32,
                candidate: ManifestCandidate::Object {
                    location: file.path.clone(),
                    identity,
                },
            })
        })
        .collect()
}

fn manifest_candidate_projection(record: &ManifestRecord) -> ConnectorCleanupCandidate {
    match &record.candidate {
        ManifestCandidate::Object { location, .. } => ConnectorCleanupCandidate::Object {
            location: Arc::from(location.as_str()),
        },
        ManifestCandidate::OwnedRef {
            name,
            head_snapshot_id,
            provenance_version,
            created_at_ms,
        } => ConnectorCleanupCandidate::OwnedRef {
            name: Arc::from(name.as_str()),
            head_snapshot_id: *head_snapshot_id,
            provenance_version: *provenance_version,
            created_at_ms: *created_at_ms,
        },
    }
}

fn execute_frozen_batch(
    runtime: &IcebergControlRuntime,
    payload: &PlanPayload,
    batch: &[ManifestRecord],
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ReceiptRecord>, ConnectorError> {
    batch
        .iter()
        .map(|record| match &record.candidate {
            ManifestCandidate::OwnedRef {
                name,
                head_snapshot_id,
                provenance_version,
                created_at_ms,
                ..
            } => execute_owned_ref(
                runtime,
                payload,
                record.ordinal,
                name,
                *head_snapshot_id,
                *provenance_version,
                *created_at_ms,
            ),
            ManifestCandidate::Object { location, identity } => {
                let access = crate::fs_io::resolve_access_for_location(location, config)
                    .map_err(unavailable)?;
                let path = access.single_relative_path().map_err(invalid)?.to_string();
                let operator = access.operator();
                let matches = runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(stat_matches(
                        operator.clone(),
                        path.clone(),
                        identity.clone(),
                    ))
                    .map_err(unavailable)?;
                match matches {
                    Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => {
                        receipt(record.ordinal, ObjectOutcome::AlreadyAbsent, None)
                    }
                    Err(error) => receipt(
                        record.ordinal,
                        error_outcome(error.kind()),
                        Some(error.to_string()),
                    ),
                    Ok(false) => receipt(
                        record.ordinal,
                        ObjectOutcome::Failed,
                        Some("object identity changed before delete".to_string()),
                    ),
                    Ok(true) => {
                        let deleted = runtime
                            .resources()
                            .catalog_runtime()
                            .block_on(delete_exact(operator, path, identity.clone()))
                            .map_err(unavailable)?;
                        match deleted {
                            Ok(()) => receipt(record.ordinal, ObjectOutcome::Deleted, None),
                            Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => {
                                receipt(record.ordinal, ObjectOutcome::AlreadyAbsent, None)
                            }
                            Err(error) => receipt(
                                record.ordinal,
                                error_outcome(error.kind()),
                                Some(error.to_string()),
                            ),
                        }
                    }
                }
            }
        })
        .collect()
}

fn reconcile_frozen_batch(
    runtime: &IcebergControlRuntime,
    payload: &PlanPayload,
    batch: &[ManifestRecord],
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ReceiptRecord>, ConnectorError> {
    batch
        .iter()
        .map(|record| match &record.candidate {
            ManifestCandidate::OwnedRef {
                name,
                head_snapshot_id,
                provenance_version,
                created_at_ms,
                ..
            } => reconcile_owned_ref(
                runtime,
                payload,
                record.ordinal,
                name,
                *head_snapshot_id,
                *provenance_version,
                *created_at_ms,
            ),
            ManifestCandidate::Object { location, identity } => {
                let access = crate::fs_io::resolve_access_for_location(location, config)
                    .map_err(unavailable)?;
                let path = access.single_relative_path().map_err(invalid)?.to_string();
                let outcome = runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(stat_matches(access.operator(), path, identity.clone()))
                    .map_err(unavailable)?;
                match outcome {
                    Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => {
                        receipt(record.ordinal, ObjectOutcome::Deleted, None)
                    }
                    Err(error) => receipt(
                        record.ordinal,
                        ObjectOutcome::Unknown,
                        Some(error.to_string()),
                    ),
                    Ok(true) => receipt(
                        record.ordinal,
                        ObjectOutcome::Failed,
                        Some("object remains after uncertain delete".to_string()),
                    ),
                    Ok(false) => receipt(
                        record.ordinal,
                        ObjectOutcome::Failed,
                        Some("object identity changed after uncertain delete".to_string()),
                    ),
                }
            }
        })
        .collect()
}

fn execute_owned_ref(
    runtime: &IcebergControlRuntime,
    payload: &PlanPayload,
    ordinal: u32,
    name: &str,
    expected_head_snapshot_id: i64,
    provenance_version: u16,
    created_at_ms: i64,
) -> Result<ReceiptRecord, ConnectorError> {
    runtime
        .control_state()
        .invalidate_table(&payload.namespace, &payload.table);
    let physical = runtime
        .load_table(&payload.namespace, &payload.table)
        .map_err(unavailable)?;
    let expected = OwnedRefCandidate {
        name: name.to_string(),
        head_snapshot_id: expected_head_snapshot_id,
        provenance_version,
        created_at_ms,
    };
    if physical.table.metadata().uuid().to_string() != payload.table_uuid
        || !matches_owned_ref_candidate(
            physical.table.metadata(),
            &payload.namespace,
            &payload.table,
            &expected,
        )
    {
        return receipt(
            ordinal,
            ObjectOutcome::Failed,
            Some("owned ref provenance changed before exact retirement".to_string()),
        );
    }
    let catalog = Arc::clone(runtime.catalog());
    let namespace = payload.namespace.clone();
    let table = payload.table.clone();
    let table_uuid = payload.table_uuid.clone();
    let name = name.to_string();
    let outcome = runtime.resources().catalog_runtime().block_on(async move {
        crate::commit::drop_branch_if_exact(
            catalog.as_ref(),
            &namespace,
            &table,
            &table_uuid,
            &name,
            expected_head_snapshot_id,
        )
        .await
    });
    match outcome {
        Ok(Ok(crate::commit::ExactBranchDropOutcome::Retired)) => {
            receipt(ordinal, ObjectOutcome::Deleted, None)
        }
        Ok(Ok(crate::commit::ExactBranchDropOutcome::Abandoned)) => receipt(
            ordinal,
            ObjectOutcome::Failed,
            Some("owned ref changed before exact retirement".to_string()),
        ),
        Ok(Err(error)) | Err(error) => receipt(ordinal, ObjectOutcome::Unknown, Some(error)),
    }
}

fn reconcile_owned_ref(
    runtime: &IcebergControlRuntime,
    payload: &PlanPayload,
    ordinal: u32,
    name: &str,
    expected_head_snapshot_id: i64,
    provenance_version: u16,
    created_at_ms: i64,
) -> Result<ReceiptRecord, ConnectorError> {
    runtime
        .control_state()
        .invalidate_table(&payload.namespace, &payload.table);
    let physical = runtime
        .load_table(&payload.namespace, &payload.table)
        .map_err(unavailable)?;
    let metadata = physical.table.metadata();
    if metadata.uuid().to_string() != payload.table_uuid {
        return receipt(
            ordinal,
            ObjectOutcome::Failed,
            Some("cleanup table incarnation changed after uncertain ref retirement".to_string()),
        );
    }
    let expected = OwnedRefCandidate {
        name: name.to_string(),
        head_snapshot_id: expected_head_snapshot_id,
        provenance_version,
        created_at_ms,
    };
    match metadata.refs().get(name) {
        None => receipt(ordinal, ObjectOutcome::Deleted, None),
        Some(_)
            if !matches_owned_ref_candidate(
                metadata,
                &payload.namespace,
                &payload.table,
                &expected,
            ) =>
        {
            receipt(
                ordinal,
                ObjectOutcome::Failed,
                Some("owned ref provenance changed after uncertain retirement".to_string()),
            )
        }
        Some(reference)
            if reference.is_branch() && reference.snapshot_id == expected_head_snapshot_id =>
        {
            receipt(
                ordinal,
                ObjectOutcome::Failed,
                Some(
                    "owned ref remains at the observed head after uncertain retirement".to_string(),
                ),
            )
        }
        Some(_) => receipt(
            ordinal,
            ObjectOutcome::Failed,
            Some("owned ref changed after uncertain retirement".to_string()),
        ),
    }
}

async fn stat_matches(
    operator: crate::opendal::Operator,
    path: String,
    identity: ObjectIdentity,
) -> Result<bool, crate::opendal::Error> {
    let metadata = operator.stat(&path).await?;
    let size = metadata.content_length();
    let mtime = metadata
        .last_modified()
        .map(|value| canonical_object_mtime_ms(value.into_inner().as_millisecond()));
    Ok(match identity {
        ObjectIdentity::Version {
            version,
            size: expected,
            mtime_ms,
        } => {
            metadata.version() == Some(version.as_str())
                && size == expected
                && mtime == Some(mtime_ms)
        }
        ObjectIdentity::Etag {
            etag,
            size: expected,
            mtime_ms,
        } => metadata.etag() == Some(etag.as_str()) && size == expected && mtime == Some(mtime_ms),
        ObjectIdentity::SizeMtime {
            size: expected,
            mtime_ms,
        } => size == expected && mtime == Some(mtime_ms),
    })
}

async fn delete_exact(
    operator: crate::opendal::Operator,
    path: String,
    identity: ObjectIdentity,
) -> Result<(), crate::opendal::Error> {
    match identity {
        ObjectIdentity::Version { version, .. } => {
            operator.delete_with(&path).version(&version).await
        }
        _ => operator.delete(&path).await,
    }
}

fn write_manifest(
    runtime: &IcebergControlRuntime,
    file_io: &FileIO,
    root: &str,
    digest: [u8; 32],
    logical: &LogicalManifest,
) -> Result<u32, ConnectorError> {
    let parts = split_manifest_parts(&logical.records)?;
    let mut references = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let bytes = canonical(part)?;
        let part_digest = domain_digest(PART_DOMAIN, &bytes);
        write_immutable(
            runtime,
            file_io,
            &format!("{root}/part-{index:04}.json"),
            bytes,
        )?;
        references.push(PartReference {
            index: index as u16,
            digest_hex: hex_encode(part_digest),
        });
    }
    write_immutable(
        runtime,
        file_io,
        &format!("{root}/manifest.json"),
        canonical(&ManifestRoot {
            version: ARTIFACT_VERSION,
            manifest_digest_hex: hex_encode(digest),
            namespace: logical.namespace.clone(),
            table: logical.table.clone(),
            table_uuid: logical.table_uuid.clone(),
            older_than_ms: logical.older_than_ms,
            phase: logical.phase,
            record_count: logical.records.len() as u32,
            total_bytes: logical.records.iter().map(manifest_record_size).sum(),
            parts: references,
        })?,
    )?;
    Ok(parts.len() as u32)
}

fn read_manifest(
    runtime: &IcebergControlRuntime,
    file_io: &FileIO,
    root: &str,
    expected_digest: [u8; 32],
) -> Result<Vec<ManifestRecord>, ConnectorError> {
    let bytes = read(
        runtime,
        file_io,
        &format!("{root}/manifest.json"),
        64 * 1024,
    )?;
    let manifest: ManifestRoot = decode_canonical(&bytes, "cleanup manifest root")?;
    if manifest.version != ARTIFACT_VERSION
        || manifest.manifest_digest_hex != hex_encode(expected_digest)
        || manifest.parts.len() > MAX_PARTS
    {
        return Err(corrupt("Iceberg cleanup manifest root is invalid"));
    }
    let mut records = Vec::new();
    for (index, reference) in manifest.parts.iter().enumerate() {
        if reference.index as usize != index {
            return Err(corrupt("Iceberg cleanup manifest parts are unordered"));
        }
        let bytes = read(
            runtime,
            file_io,
            &format!("{root}/part-{index:04}.json"),
            MAX_PART_BYTES,
        )?;
        if domain_digest(PART_DOMAIN, &bytes) != decode_digest(&reference.digest_hex)? {
            return Err(corrupt("Iceberg cleanup manifest part digest is invalid"));
        }
        let part: ManifestPart = decode_canonical(&bytes, "cleanup manifest part")?;
        if part.version != ARTIFACT_VERSION {
            return Err(corrupt("Iceberg cleanup manifest part version is invalid"));
        }
        records.extend(part.records);
    }
    if records.len() != manifest.record_count as usize
        || records.len() > MAX_RECORDS
        || records.windows(2).any(|pair| {
            pair[0].ordinal + 1 != pair[1].ordinal
                || manifest_record_sort_key(&pair[0]) >= manifest_record_sort_key(&pair[1])
        })
    {
        return Err(corrupt("Iceberg cleanup manifest records are invalid"));
    }
    let logical = LogicalManifest {
        version: ARTIFACT_VERSION,
        namespace: manifest.namespace,
        table: manifest.table,
        table_uuid: manifest.table_uuid,
        older_than_ms: manifest.older_than_ms,
        phase: manifest.phase,
        records: records.clone(),
    };
    if domain_digest(MANIFEST_DOMAIN, &canonical(&logical)?) != expected_digest {
        return Err(corrupt("Iceberg cleanup manifest digest is invalid"));
    }
    Ok(records)
}

fn split_manifest_parts(records: &[ManifestRecord]) -> Result<Vec<ManifestPart>, ConnectorError> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    for record in records {
        let mut candidate = current.clone();
        candidate.push(record.clone());
        if canonical(&ManifestPart {
            version: ARTIFACT_VERSION,
            records: candidate.clone(),
        })?
        .len()
            <= MAX_PART_BYTES
        {
            current = candidate;
        } else if current.is_empty() {
            return Err(exhausted("Iceberg cleanup record exceeds 1 MiB"));
        } else {
            parts.push(ManifestPart {
                version: ARTIFACT_VERSION,
                records: std::mem::take(&mut current),
            });
            current.push(record.clone());
        }
    }
    if !current.is_empty() {
        parts.push(ManifestPart {
            version: ARTIFACT_VERSION,
            records: current,
        });
    }
    if parts.len() > MAX_PARTS {
        return Err(exhausted("Iceberg cleanup artifact exceeds 64 parts"));
    }
    Ok(parts)
}

fn write_immutable(
    runtime: &IcebergControlRuntime,
    file_io: &FileIO,
    location: &str,
    bytes: Bytes,
) -> Result<(), ConnectorError> {
    if let Some(existing) = read_optional(runtime, file_io, location, MAX_PART_BYTES * 2)? {
        if existing != bytes {
            return Err(corrupt(
                "Iceberg cleanup content-addressed artifact conflicts with existing content",
            ));
        }
        return Ok(());
    }
    let output = file_io
        .new_output(location)
        .map_err(|error| unavailable(error.to_string()))?;
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { output.write(bytes).await })
        .map_err(unavailable)?
        .map_err(|error| unavailable(error.to_string()))
}

fn read_optional(
    runtime: &IcebergControlRuntime,
    file_io: &FileIO,
    location: &str,
    max: usize,
) -> Result<Option<Bytes>, ConnectorError> {
    let input = file_io
        .new_input(location)
        .map_err(|error| unavailable(error.to_string()))?;
    let exists = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { input.exists().await })
        .map_err(unavailable)?
        .map_err(|error| unavailable(error.to_string()))?;
    if !exists {
        return Ok(None);
    }
    read(runtime, file_io, location, max).map(Some)
}

fn read(
    runtime: &IcebergControlRuntime,
    file_io: &FileIO,
    location: &str,
    max: usize,
) -> Result<Bytes, ConnectorError> {
    let input = file_io
        .new_input(location)
        .map_err(|error| unavailable(error.to_string()))?;
    let bytes = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { input.read().await })
        .map_err(unavailable)?
        .map_err(|error| unavailable(error.to_string()))?;
    if bytes.len() > max {
        return Err(exhausted("Iceberg cleanup artifact exceeds its size limit"));
    }
    Ok(bytes)
}

fn receipt_value(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorExecutionBindingKey,
    plan: &ConnectorCleanupPlan,
    prepared: &PreparedBatch,
    location: String,
    digest: [u8; 32],
    records: &[ReceiptRecord],
) -> Result<BatchReceipt, ConnectorError> {
    BatchReceipt::try_new(
        descriptor.clone(),
        key.clone(),
        plan.operation_id(),
        plan.plan_digest(),
        plan.manifest_digest(),
        prepared.batch_ordinal(),
        prepared.batch_digest(),
        receipt_summary(records)?,
        canonical(&ReceiptPayload {
            version: ARTIFACT_VERSION,
            receipt_location: location,
            receipt_digest_hex: hex_encode(digest),
        })?,
    )
}

fn receipt_summary(records: &[ReceiptRecord]) -> Result<BatchReceiptSummary, ConnectorError> {
    let mut deleted = 0_u32;
    let mut absent = 0_u32;
    let mut failed = 0_u32;
    let mut unknown = 0_u32;
    for record in records {
        match record.outcome {
            ObjectOutcome::Deleted => deleted = deleted.saturating_add(1),
            ObjectOutcome::AlreadyAbsent => absent = absent.saturating_add(1),
            ObjectOutcome::Failed => failed = failed.saturating_add(1),
            ObjectOutcome::Unknown => unknown = unknown.saturating_add(1),
        }
    }
    let summary = BatchReceiptSummary::new(deleted, absent, failed, unknown);
    if summary.total() != records.len() as u32 {
        return Err(corrupt("Iceberg cleanup receipt summary overflows"));
    }
    Ok(summary)
}

fn receipt(
    ordinal: u32,
    outcome: ObjectOutcome,
    reason: Option<String>,
) -> Result<ReceiptRecord, ConnectorError> {
    Ok(ReceiptRecord {
        ordinal,
        outcome,
        reason: reason.map(|reason| reason.chars().take(MAX_REASON_CHARS).collect()),
    })
}

fn error_outcome(kind: crate::opendal::ErrorKind) -> ObjectOutcome {
    match kind {
        crate::opendal::ErrorKind::PermissionDenied
        | crate::opendal::ErrorKind::ConditionNotMatch => ObjectOutcome::Failed,
        _ => ObjectOutcome::Unknown,
    }
}

fn receipt_location(payload: &PlanPayload, batch: u32) -> String {
    format!("{}/receipts/{batch:04}.json", payload.artifact_root)
}

fn batch_digest(records: &[ManifestRecord]) -> [u8; 32] {
    domain_digest(
        BATCH_DOMAIN,
        &canonical(records).expect("bounded manifest records are serializable"),
    )
}

fn identity_size(identity: &ObjectIdentity) -> u64 {
    match identity {
        ObjectIdentity::Version { size, .. }
        | ObjectIdentity::Etag { size, .. }
        | ObjectIdentity::SizeMtime { size, .. } => *size,
    }
}

fn manifest_record_size(record: &ManifestRecord) -> u64 {
    match &record.candidate {
        ManifestCandidate::Object { identity, .. } => identity_size(identity),
        // Catalog refs are metadata, not object-store bytes. Keeping this at
        // zero makes the summary an honest object-sweep byte estimate.
        ManifestCandidate::OwnedRef { .. } => 0,
    }
}

fn manifest_record_sort_key(record: &ManifestRecord) -> String {
    match &record.candidate {
        ManifestCandidate::Object { location, .. } => format!("object:{location}"),
        ManifestCandidate::OwnedRef { name, .. } => format!("owned_ref:{name}"),
    }
}

fn state_digest(
    uuid: &[u8],
    location: Option<&str>,
    snapshot: Option<i64>,
    schema: i32,
    spec: i32,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.orphan-cleanup.state.v1\0");
    hash.update(uuid);
    hash.update(location.unwrap_or_default().as_bytes());
    hash.update(snapshot.unwrap_or_default().to_be_bytes());
    hash.update(schema.to_be_bytes());
    hash.update(spec.to_be_bytes());
    hash.finalize().into()
}

fn domain_digest(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.finalize().into()
}

fn decode_digest(value: &str) -> Result<[u8; 32], ConnectorError> {
    hex_decode(value)
        .ok_or_else(|| corrupt("Iceberg cleanup digest is not hex"))?
        .try_into()
        .map_err(|_| corrupt("Iceberg cleanup digest has an invalid length"))
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

fn canonical<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, ConnectorError> {
    let value = serde_json::to_value(value)
        .map_err(|error| internal(format!("encode Iceberg cleanup JSON: {error}")))?;
    let mut bytes = Vec::new();
    write_canonical(&value, &mut bytes)?;
    Ok(Bytes::from(bytes))
}

fn decode_canonical<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    label: &str,
) -> Result<T, ConnectorError> {
    let value = serde_json::from_slice(bytes)
        .map_err(|error| corrupt(format!("decode {label}: {error}")))?;
    if canonical(&value)?.as_ref() != bytes {
        return Err(corrupt(format!("{label} is not canonical JSON")));
    }
    serde_json::from_value(value).map_err(|error| corrupt(format!("decode {label}: {error}")))
}

fn write_canonical(value: &Value, bytes: &mut Vec<u8>) -> Result<(), ConnectorError> {
    match value {
        Value::Null => bytes.extend_from_slice(b"null"),
        Value::Bool(value) => bytes.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => bytes.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => bytes.extend_from_slice(
            serde_json::to_string(value)
                .map_err(|error| internal(error.to_string()))?
                .as_bytes(),
        ),
        Value::Array(values) => {
            bytes.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                write_canonical(value, bytes)?;
            }
            bytes.push(b']');
        }
        Value::Object(values) => {
            bytes.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                bytes.extend_from_slice(
                    serde_json::to_string(key)
                        .map_err(|error| internal(error.to_string()))?
                        .as_bytes(),
                );
                bytes.push(b':');
                write_canonical(value, bytes)?;
            }
            bytes.push(b'}');
        }
    }
    Ok(())
}

fn validate_context(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message.into())
}

/// How one historical cleanup batch actually landed.
pub(crate) struct HistoricalCleanupCounts {
    pub deleted: u64,
    pub already_absent: u64,
    pub failed: u64,
    pub unknown: u64,
}

/// Classify a batch a dead generation dispatched, without deleting anything.
///
/// This is a read: it stats each object in the frozen batch and reports what it
/// finds. Nothing here plans, prepares or deletes, and the candidate set is
/// exactly the one the old attempt froze -- the manifest digest is what proves
/// the list read back is that same list, and the table UUID check is what stops
/// a dropped-and-recreated table from being classified against an old manifest.
pub(crate) fn classify_historical_cleanup_batch(
    runtime: &IcebergControlRuntime,
    operation_id: [u8; 16],
    plan_payload: &[u8],
    prepared_payload: &[u8],
    manifest_digest: [u8; 32],
) -> Result<HistoricalCleanupCounts, ConnectorError> {
    let payload: PlanPayload = decode_canonical(plan_payload, "cleanup plan")?;
    let prepared =
        PreparedBatch::try_from_wire_v1(bytes::Bytes::copy_from_slice(prepared_payload))?;
    let evidence: PreparedPayload =
        decode_canonical(prepared.evidence_payload(), "cleanup prepared evidence")?;
    if evidence.version != ARTIFACT_VERSION
        || evidence.artifact_root != payload.artifact_root
        || evidence.batch_ordinal != prepared.batch_ordinal()
        || evidence.record_count == 0
        || evidence.record_count as usize > MAX_BATCH_OBJECTS
        || evidence.batch_digest_hex != hex_encode(prepared.batch_digest())
    {
        return Err(corrupt("Iceberg cleanup prepared evidence is invalid"));
    }

    runtime
        .control_state()
        .invalidate_table(&payload.namespace, &payload.table);
    let physical = runtime
        .load_table(&payload.namespace, &payload.table)
        .map_err(unavailable)?;
    if physical.table.metadata().uuid().to_string() != payload.table_uuid {
        return Err(corrupt(
            "Iceberg cleanup table incarnation no longer matches its frozen manifest",
        ));
    }
    let table_location = physical.table.metadata().location().to_string();
    let expected_prefix = format!(
        "{table_location}/_novarocks/maintenance/v4/orphan-cleanup/{}/",
        hex_encode(operation_id)
    );
    if !payload.artifact_root.starts_with(&expected_prefix) {
        return Err(corrupt(
            "Iceberg cleanup artifact root does not match its frozen table",
        ));
    }
    let records = read_manifest(
        runtime,
        &physical.table.file_io().clone(),
        &payload.artifact_root,
        manifest_digest,
    )?;
    let start = evidence.first_ordinal as usize;
    let end = start
        .checked_add(evidence.record_count as usize)
        .ok_or_else(|| corrupt("Iceberg cleanup batch range overflows its manifest"))?;
    if end > records.len() {
        return Err(corrupt("Iceberg cleanup batch exceeds its frozen manifest"));
    }
    let batch = &records[start..end];

    let config = runtime.control_state().object_store_config();
    let outcomes = reconcile_frozen_batch(runtime, &payload, batch, config)?;
    let mut counts = HistoricalCleanupCounts {
        deleted: 0,
        already_absent: 0,
        failed: 0,
        unknown: 0,
    };
    for outcome in &outcomes {
        match outcome.outcome {
            ObjectOutcome::Deleted => counts.deleted += 1,
            ObjectOutcome::AlreadyAbsent => counts.already_absent += 1,
            ObjectOutcome::Failed => counts.failed += 1,
            ObjectOutcome::Unknown => counts.unknown += 1,
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorCleanupFinalizeRequest, ConnectorCleanupOperation,
        ConnectorCleanupPlanningRequest, ConnectorExecutionBindingKey, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableHandle,
    };

    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergControlResources;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            64 * 1024,
            256 * 1024,
        )
        .expect("context")
    }

    fn contract_values() -> (
        ConnectorInstanceDescriptor,
        ConnectorExecutionBindingKey,
        ConnectorCleanupPlan,
        PreparedBatch,
    ) {
        let instance_id = ConnectorInstanceId::parse("cleanup-test").expect("instance");
        let key = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([4; 16]),
        };
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: instance_id.clone(),
        };
        let request = ConnectorCleanupPlanningRequest::try_new(
            ConnectorCleanupOperationId::from_bytes([5; 16]),
            key.clone(),
            ConnectorCleanupOperation::remove_unreferenced_objects(
                ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
                    .expect("table"),
                1,
            )
            .expect("operation"),
            context(),
        )
        .expect("request");
        let plan = ConnectorCleanupPlan::try_new(
            &request,
            [6; 32],
            [7; 32],
            ConnectorCleanupPlanSummary::try_new(3, 30, 1, 1).expect("summary"),
            Bytes::from_static(b"plan"),
        )
        .expect("plan");
        let prepared = PreparedBatch::try_new(
            key.clone(),
            plan.operation_id(),
            plan.plan_digest(),
            plan.manifest_digest(),
            0,
            [8; 32],
            Bytes::from_static(b"prepared"),
        )
        .expect("prepared");
        (descriptor, key, plan, prepared)
    }

    fn local_runtime() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergControlRuntime>,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "cleanup-test",
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
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        (executor, warehouse, runtime)
    }

    fn size_mtime_identity(executor: &tokio::runtime::Runtime, location: &str) -> ObjectIdentity {
        let access = crate::fs_io::resolve_access_for_location(location, None).expect("access");
        let path = access.single_relative_path().expect("relative path");
        let metadata = executor
            .block_on(access.operator().stat(path))
            .expect("metadata");
        ObjectIdentity::SizeMtime {
            size: metadata.content_length(),
            mtime_ms: canonical_object_mtime_ms(
                metadata
                    .last_modified()
                    .expect("modification time")
                    .into_inner()
                    .as_millisecond(),
            ),
        }
    }

    fn object_record(
        ordinal: u32,
        location: impl Into<String>,
        identity: ObjectIdentity,
    ) -> ManifestRecord {
        ManifestRecord {
            ordinal,
            candidate: ManifestCandidate::Object {
                location: location.into(),
                identity,
            },
        }
    }

    fn test_payload() -> PlanPayload {
        PlanPayload {
            version: ARTIFACT_VERSION,
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            artifact_root: "file:///tmp/cleanup".to_string(),
        }
    }

    #[test]
    fn manifest_and_receipt_codecs_are_canonical_and_bounded() {
        let records = vec![
            object_record(
                0,
                "file:///tmp/a.parquet",
                ObjectIdentity::SizeMtime {
                    size: 10,
                    mtime_ms: 20,
                },
            ),
            object_record(
                1,
                "file:///tmp/b.parquet",
                ObjectIdentity::Etag {
                    etag: "etag-b".to_string(),
                    size: 20,
                    mtime_ms: 30,
                },
            ),
        ];
        let parts = split_manifest_parts(&records).expect("manifest parts");
        assert_eq!(parts.len(), 1);
        let encoded = canonical(&parts[0]).expect("encode manifest part");
        assert!(encoded.len() <= MAX_PART_BYTES);
        let decoded: ManifestPart =
            decode_canonical(&encoded, "manifest part").expect("decode manifest part");
        assert_eq!(decoded.records.len(), records.len());
        assert_eq!(batch_digest(&decoded.records), batch_digest(&records));

        let non_canonical = Bytes::from_static(b"{\"version\":1, \"records\":[]}");
        let error = match decode_canonical::<ManifestPart>(&non_canonical, "manifest part") {
            Ok(_) => panic!("whitespace must not be accepted as canonical JSON"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

        let oversized = object_record(
            0,
            "x".repeat(MAX_PART_BYTES + 1),
            ObjectIdentity::SizeMtime {
                size: 1,
                mtime_ms: 1,
            },
        );
        let error = match split_manifest_parts(&[oversized]) {
            Ok(_) => panic!("one record must not exceed a manifest part"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(
            MAX_RECORDS,
            MAX_BATCH_OBJECTS * novarocks_spi::connector::MAX_CONNECTOR_CLEANUP_BATCHES as usize
        );

        let bounded_reason = receipt(
            0,
            ObjectOutcome::Failed,
            Some("r".repeat(MAX_REASON_CHARS + 100)),
        )
        .expect("receipt record");
        assert_eq!(
            bounded_reason
                .reason
                .as_deref()
                .expect("reason")
                .chars()
                .count(),
            MAX_REASON_CHARS
        );
        let artifact = ReceiptArtifact {
            version: ARTIFACT_VERSION,
            batch_digest_hex: hex_encode([9; 32]),
            records: vec![bounded_reason],
        };
        let encoded = canonical(&artifact).expect("encode receipt");
        assert!(encoded.len() <= MAX_PART_BYTES * 2);
        let decoded: ReceiptArtifact =
            decode_canonical(&encoded, "receipt").expect("decode receipt");
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(
            receipt_summary(&decoded.records).expect("summary").failed(),
            1
        );

        let boundary_records = (0..MAX_BATCH_OBJECTS)
            .map(|ordinal| {
                receipt(ordinal as u32, ObjectOutcome::AlreadyAbsent, None)
                    .expect("boundary receipt")
            })
            .collect::<Vec<_>>();
        let boundary_bytes = canonical(&ReceiptArtifact {
            version: ARTIFACT_VERSION,
            batch_digest_hex: hex_encode([10; 32]),
            records: boundary_records,
        })
        .expect("encode boundary receipt");
        assert!(boundary_bytes.len() <= MAX_PART_BYTES * 2);
        let boundary: ReceiptArtifact =
            decode_canonical(&boundary_bytes, "boundary receipt").expect("decode boundary receipt");
        assert_eq!(boundary.records.len(), MAX_BATCH_OBJECTS);
        assert_eq!(
            receipt_summary(&boundary.records)
                .expect("boundary summary")
                .already_absent(),
            MAX_BATCH_OBJECTS as u32
        );
    }

    #[test]
    fn receipt_replay_and_terminal_finalization_are_idempotent() {
        let (descriptor, key, plan, prepared) = contract_values();
        let records = vec![
            receipt(0, ObjectOutcome::Deleted, None).expect("deleted"),
            receipt(
                1,
                ObjectOutcome::Failed,
                Some("identity changed".to_string()),
            )
            .expect("failed"),
            receipt(
                2,
                ObjectOutcome::Unknown,
                Some("response was lost".to_string()),
            )
            .expect("unknown"),
        ];
        let bytes = canonical(&ReceiptArtifact {
            version: ARTIFACT_VERSION,
            batch_digest_hex: hex_encode(prepared.batch_digest()),
            records: records.clone(),
        })
        .expect("receipt artifact");
        let digest = domain_digest(RECEIPT_DOMAIN, &bytes);
        let first = receipt_value(
            &descriptor,
            &key,
            &plan,
            &prepared,
            "file:///tmp/receipt.json".to_string(),
            digest,
            &records,
        )
        .expect("first receipt");
        let replay = receipt_value(
            &descriptor,
            &key,
            &plan,
            &prepared,
            "file:///tmp/receipt.json".to_string(),
            digest,
            &records,
        )
        .expect("replayed receipt");
        assert_eq!(first, replay);
        assert_eq!(first.summary().deleted(), 1);
        assert_eq!(first.summary().failed(), 1);
        assert_eq!(first.summary().unknown(), 1);
        first.validate().expect("receipt validates");

        let (_executor, _warehouse, runtime) = local_runtime();
        let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).expect("adapter");
        adapter
            .finalize_terminal(
                ConnectorCleanupFinalizeRequest::try_new(plan.clone(), context())
                    .expect("first finalization"),
            )
            .expect("first terminal finalization");
        adapter
            .finalize_terminal(
                ConnectorCleanupFinalizeRequest::try_new(plan, context())
                    .expect("replayed finalization"),
            )
            .expect("replayed terminal finalization");
    }

    #[test]
    fn reconcile_is_stable_and_execute_preserves_failure_outcomes() {
        let (executor, warehouse, runtime) = local_runtime();
        let path = warehouse.path().join("candidate.parquet");
        std::fs::write(&path, b"candidate").expect("write candidate");
        let location = format!("file://{}", path.display());
        let identity = size_mtime_identity(&executor, &location);
        let record = object_record(0, location.clone(), identity.clone());

        let payload = test_payload();
        let remains =
            reconcile_frozen_batch(&runtime, &payload, std::slice::from_ref(&record), None)
                .expect("reconcile remaining object");
        let remains_summary = receipt_summary(&remains).expect("remaining summary");
        assert_eq!(remains_summary.failed(), 1);
        assert_eq!(remains_summary.deleted(), 0);
        assert!(path.exists());

        let mismatched = object_record(
            0,
            location.clone(),
            match identity {
                ObjectIdentity::SizeMtime { size, mtime_ms } => ObjectIdentity::SizeMtime {
                    size: size + 1,
                    mtime_ms,
                },
                _ => unreachable!("local fixture uses size and mtime"),
            },
        );
        let failed = execute_frozen_batch(&runtime, &payload, &[mismatched], None)
            .expect("execute mismatched identity");
        let failed_summary = receipt_summary(&failed).expect("failure summary");
        assert_eq!(failed_summary.failed(), 1);
        assert_eq!(failed_summary.deleted(), 0);
        assert!(
            failed[0]
                .reason
                .as_deref()
                .expect("failure reason")
                .contains("identity changed")
        );
        assert!(path.exists(), "failed cleanup must not delete the object");

        std::fs::remove_file(&path).expect("remove candidate");
        let absent_once =
            reconcile_frozen_batch(&runtime, &payload, std::slice::from_ref(&record), None)
                .expect("first absent reconcile");
        let absent_replay = reconcile_frozen_batch(&runtime, &payload, &[record], None)
            .expect("replayed absent reconcile");
        assert_eq!(
            receipt_summary(&absent_once).expect("first absent summary"),
            BatchReceiptSummary::new(1, 0, 0, 0)
        );
        assert_eq!(
            receipt_summary(&absent_replay).expect("replayed absent summary"),
            BatchReceiptSummary::new(1, 0, 0, 0)
        );
    }
}
