// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

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
    BatchReceipt, BatchReceiptSummary, CandidatePage, ConnectorCleanupCandidatePageRequest,
    ConnectorCleanupExecuteRequest, ConnectorCleanupFinalizeRequest, ConnectorCleanupMaintenance,
    ConnectorCleanupOperationId, ConnectorCleanupPlan, ConnectorCleanupPlanSummary,
    ConnectorCleanupPlanningRequest, ConnectorCleanupPrepareRequest,
    ConnectorCleanupReconcileRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, PreparedBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::cleanup_candidates::{
    ScannedFile, canonical_object_mtime_ms, collect_orphan_candidates,
};
use crate::control_provider::IcebergTablePayload;
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::io::FileIO;

const ARTIFACT_VERSION: u16 = 1;
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
    records: Vec<ManifestRecord>,
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
    location: String,
    identity: ObjectIdentity,
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
        {
            if cached.plan.plan_digest() != plan.plan_digest() {
                return Err(invalid(
                    "Iceberg cleanup operation conflicts with its generation-local plan",
                ));
            }
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
            "{table_location}/_novarocks/maintenance/v3/orphan-cleanup/{}/",
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
        let table_for_scan = table.clone();
        let older_than_ms = request.operation().older_than_ms();
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
        let records =
            records_from_candidates(&scanned, &table, physical.object_store_config.as_ref())?;
        if records.len() > MAX_RECORDS {
            return Err(exhausted("Iceberg cleanup manifest exceeds 262144 records"));
        }
        let logical = LogicalManifest {
            version: ARTIFACT_VERSION,
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            table_uuid: table.metadata().uuid().to_string(),
            older_than_ms,
            records,
        };
        let logical_bytes = canonical(&logical)?;
        if logical_bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(exhausted("Iceberg cleanup manifest exceeds 64 MiB"));
        }
        let manifest_digest = domain_digest(MANIFEST_DOMAIN, &logical_bytes);
        let artifact_root = format!(
            "{}/_novarocks/maintenance/v3/orphan-cleanup/{}/{}",
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
            logical
                .records
                .iter()
                .map(|record| identity_size(&record.identity))
                .sum(),
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
        let outcomes = execute_frozen_batch(&self.runtime, &batch, config)?;
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
        let outcomes = reconcile_frozen_batch(&self.runtime, &batch, config)?;
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
                .map(|record| Arc::from(record.location.as_str()))
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
                location: file.path.clone(),
                identity,
            })
        })
        .collect()
}

fn execute_frozen_batch(
    runtime: &IcebergControlRuntime,
    batch: &[ManifestRecord],
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ReceiptRecord>, ConnectorError> {
    batch
        .iter()
        .map(|record| {
            let access = crate::fs_io::resolve_access_for_location(&record.location, config)
                .map_err(unavailable)?;
            let path = access.single_relative_path().map_err(invalid)?.to_string();
            let operator = access.operator();
            let matches = runtime
                .resources()
                .catalog_runtime()
                .block_on(stat_matches(
                    operator.clone(),
                    path.clone(),
                    record.identity.clone(),
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
                        .block_on(delete_exact(operator, path, record.identity.clone()))
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
        })
        .collect()
}

fn reconcile_frozen_batch(
    runtime: &IcebergControlRuntime,
    batch: &[ManifestRecord],
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ReceiptRecord>, ConnectorError> {
    batch
        .iter()
        .map(|record| {
            let access = crate::fs_io::resolve_access_for_location(&record.location, config)
                .map_err(unavailable)?;
            let path = access.single_relative_path().map_err(invalid)?.to_string();
            let outcome = runtime
                .resources()
                .catalog_runtime()
                .block_on(stat_matches(
                    access.operator(),
                    path,
                    record.identity.clone(),
                ))
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
        })
        .collect()
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
            record_count: logical.records.len() as u32,
            total_bytes: logical
                .records
                .iter()
                .map(|record| identity_size(&record.identity))
                .sum(),
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
            pair[0].ordinal + 1 != pair[1].ordinal || pair[0].location >= pair[1].location
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
    if value.len() % 2 != 0 {
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
