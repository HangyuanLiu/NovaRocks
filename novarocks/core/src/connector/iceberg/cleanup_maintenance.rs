// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Iceberg implementation of the FE-only orphan-cleanup SPI.
//!
//! The manifest is the destructive-operation boundary: candidate discovery is
//! performed exactly once during planning and all later prepare, execute, and
//! recovery work reads the same content-addressed artifact.  In particular a
//! failed response is never a reason to list, plan, or delete again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use iceberg::io::FileIO;
use opendal::ErrorKind as OpenDalErrorKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    BatchReceipt, BatchReceiptSummary, CandidatePage, ConnectorCleanupCandidatePageRequest,
    ConnectorCleanupExecuteRequest, ConnectorCleanupFinalizeRequest, ConnectorCleanupMaintenance,
    ConnectorCleanupOperationId, ConnectorCleanupPlan, ConnectorCleanupPlanSummary,
    ConnectorCleanupPlanningRequest, ConnectorCleanupPrepareRequest,
    ConnectorCleanupReconcileRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, ConnectorInstanceId, PreparedBatch,
};

use super::catalog::registry::{
    IcebergCatalogEntry, IcebergCatalogRegistry, block_on_iceberg, load_table,
};
use super::commit::remove_orphan_files::{
    ScannedFile, canonical_object_mtime_ms, collect_orphan_candidates,
};
use super::fs_io;
use super::provider::decode_data_mutation_table_target;

const ARTIFACT_VERSION: u16 = 1;
const MAX_ARTIFACT_PARTS: usize = 64;
const MAX_ARTIFACT_PART_BYTES: usize = 1024 * 1024;
const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_ARTIFACT_RECORDS: usize = 262_144;
const MAX_BATCH_OBJECTS: usize = 1024;
const MAX_REASON_BYTES: usize = 1024;

const MANIFEST_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.manifest.v1\0";
const PART_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.part.v1\0";
const BATCH_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.batch.v1\0";
const RECEIPT_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.receipt.v1\0";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergCleanupPlanPayloadV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    metadata_location_digest_hex: String,
    artifact_root: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestLogicalV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    older_than_ms: i64,
    records: Vec<ManifestRecordV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRootV1 {
    version: u16,
    manifest_digest_hex: String,
    namespace: String,
    table: String,
    table_uuid: String,
    older_than_ms: i64,
    record_count: u32,
    total_bytes: u64,
    parts: Vec<ArtifactPartRefV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactPartRefV1 {
    index: u16,
    digest_hex: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPartV1 {
    version: u16,
    records: Vec<ManifestRecordV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRecordV1 {
    ordinal: u32,
    location: String,
    identity: ObjectIdentityV1,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ObjectIdentityV1 {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedPayloadV1 {
    version: u16,
    artifact_root: String,
    batch_ordinal: u32,
    first_ordinal: u32,
    record_count: u32,
    batch_digest_hex: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptPayloadV1 {
    version: u16,
    receipt_root: String,
    receipt_digest_hex: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRootV1 {
    version: u16,
    batch_digest_hex: String,
    records: u32,
    parts: Vec<ArtifactPartRefV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptPartV1 {
    version: u16,
    records: Vec<ReceiptRecordV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRecordV1 {
    ordinal: u32,
    outcome: ObjectOutcomeV1,
    reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ObjectOutcomeV1 {
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

/// FE-local Iceberg capability.  Its cache is only an optimization: all
/// recovery paths reconstruct their work from the immutable artifact handle.
pub(crate) struct IcebergCleanupMaintenanceAdapter {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    instance_id: ConnectorInstanceId,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
    plans: Mutex<HashMap<ConnectorCleanupOperationId, CachedPlan>>,
}

impl IcebergCleanupMaintenanceAdapter {
    pub(crate) fn new_registered(
        key: ConnectorExecutionBindingKey,
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<Self, ConnectorError> {
        if key.instance_id != instance_id {
            return Err(invalid(
                "Iceberg cleanup instance does not match exact binding",
            ));
        }
        Ok(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
                instance_id: key.instance_id.clone(),
            },
            key,
            instance_id,
            registry,
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

    fn entry(&self) -> Result<IcebergCatalogEntry, ConnectorError> {
        self.registry
            .read()
            .map_err(|_| internal("Iceberg cleanup registry lock poisoned"))?
            .get(self.instance_id.as_str())
            .map_err(|error| unavailable(error.to_string()))
    }

    fn payload_for_plan(
        &self,
        plan: &ConnectorCleanupPlan,
    ) -> Result<IcebergCleanupPlanPayloadV1, ConnectorError> {
        if let Some(cached) = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg cleanup plan cache lock poisoned"))?
            .get(&plan.operation_id())
            .cloned()
        {
            if cached.plan.plan_digest() == plan.plan_digest() {
                return decode_canonical(&cached.plan.provider_payload(), "cached cleanup plan");
            }
            return Err(invalid(
                "Iceberg cleanup operation conflicts with cached plan",
            ));
        }
        let payload: IcebergCleanupPlanPayloadV1 =
            decode_canonical(plan.provider_payload(), "Iceberg cleanup plan")?;
        validate_plan_payload(plan, &payload)?;
        Ok(payload)
    }

    fn load_manifest(
        &self,
        plan: &ConnectorCleanupPlan,
        payload: &IcebergCleanupPlanPayloadV1,
    ) -> Result<Vec<ManifestRecordV1>, ConnectorError> {
        let entry = self.entry()?;
        let (file_io, table_location) = table_file_io(&entry, &payload.namespace, &payload.table)?;
        let expected_prefix = format!(
            "{}/_novarocks/maintenance/v3/orphan-cleanup/{}/",
            table_location.trim_end_matches('/'),
            hex::encode(plan.operation_id().to_bytes())
        );
        if !payload.artifact_root.starts_with(&expected_prefix) {
            return Err(invalid(
                "Iceberg cleanup artifact root does not match frozen table",
            ));
        }
        load_manifest_from_root(&file_io, &payload.artifact_root, plan.manifest_digest())
    }

    fn prepared_records(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
    ) -> Result<
        (
            IcebergCleanupPlanPayloadV1,
            PreparedPayloadV1,
            Vec<ManifestRecordV1>,
        ),
        ConnectorError,
    > {
        let payload = self.payload_for_plan(plan)?;
        let evidence: PreparedPayloadV1 = decode_canonical(
            prepared.evidence_payload(),
            "Iceberg cleanup prepared evidence",
        )?;
        validate_prepared_payload(plan, prepared, &payload, &evidence)?;
        let records = self.load_manifest(plan, &payload)?;
        let start = evidence.first_ordinal as usize;
        let end = start
            .checked_add(evidence.record_count as usize)
            .ok_or_else(|| invalid("Iceberg cleanup batch range overflows"))?;
        let batch = records
            .get(start..end)
            .ok_or_else(|| invalid("Iceberg cleanup batch exceeds frozen manifest"))?
            .to_vec();
        if batch_digest(&batch) != decode_hex_digest(&evidence.batch_digest_hex, "cleanup batch")? {
            return Err(corrupt("Iceberg cleanup batch digest is invalid"));
        }
        Ok((payload, evidence, batch))
    }

    fn receipt_for_records(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
        payload: &IcebergCleanupPlanPayloadV1,
        records: Vec<ReceiptRecordV1>,
    ) -> Result<BatchReceipt, ConnectorError> {
        let receipt_root = format!(
            "{}/receipts/{:04}",
            payload.artifact_root,
            prepared.batch_ordinal()
        );
        let receipt_digest = write_receipt_artifact(
            &table_file_io(&self.entry()?, &payload.namespace, &payload.table)?.0,
            &receipt_root,
            prepared.batch_digest(),
            &records,
        )?;
        let provider_payload = canonical(&ReceiptPayloadV1 {
            version: ARTIFACT_VERSION,
            receipt_root,
            receipt_digest_hex: hex::encode(receipt_digest),
        })?;
        let summary = receipt_summary(&records)?;
        BatchReceipt::try_new(
            self.descriptor.clone(),
            self.key.clone(),
            plan.operation_id(),
            plan.plan_digest(),
            plan.manifest_digest(),
            prepared.batch_ordinal(),
            prepared.batch_digest(),
            summary,
            provider_payload,
        )
    }

    fn existing_receipt(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
        payload: &IcebergCleanupPlanPayloadV1,
    ) -> Result<Option<BatchReceipt>, ConnectorError> {
        let root = format!(
            "{}/receipts/{:04}",
            payload.artifact_root,
            prepared.batch_ordinal()
        );
        let file_io = table_file_io(&self.entry()?, &payload.namespace, &payload.table)?.0;
        let Some((digest, records)) =
            read_receipt_artifact(&file_io, &root, prepared.batch_digest())?
        else {
            return Ok(None);
        };
        let provider_payload = canonical(&ReceiptPayloadV1 {
            version: ARTIFACT_VERSION,
            receipt_root: root,
            receipt_digest_hex: hex::encode(digest),
        })?;
        Ok(Some(BatchReceipt::try_new(
            self.descriptor.clone(),
            self.key.clone(),
            plan.operation_id(),
            plan.plan_digest(),
            plan.manifest_digest(),
            prepared.batch_ordinal(),
            prepared.batch_digest(),
            receipt_summary(&records)?,
            provider_payload,
        )?))
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
        let (namespace, table_name) =
            decode_data_mutation_table_target(request.operation().table())?;
        let entry = self.entry()?;
        entry.invalidate_table_cache(&namespace, &table_name);
        let loaded = load_table(&entry, &namespace, &table_name)
            .map_err(|error| unavailable(format!("load Iceberg cleanup table: {error}")))?;
        let table = loaded.table;
        let metadata = table.metadata();
        let collected = block_on_iceberg(collect_orphan_candidates(
            build_catalog(&entry)?,
            iceberg::TableIdent::from_strs([namespace.as_str(), table_name.as_str()])
                .map_err(|e| invalid(e.to_string()))?,
            request.operation().older_than_ms(),
            entry.object_store_config(),
        ))
        .map_err(unavailable)?
        .map_err(unavailable)?;
        let records =
            records_from_candidates(&collected.files, &table, entry.object_store_config())?;
        if records.len() > MAX_ARTIFACT_RECORDS {
            return Err(exhausted("Iceberg cleanup manifest exceeds 262144 records"));
        }
        let logical = ManifestLogicalV1 {
            version: ARTIFACT_VERSION,
            namespace: namespace.clone(),
            table: table_name.clone(),
            table_uuid: metadata.uuid().to_string(),
            older_than_ms: request.operation().older_than_ms(),
            records,
        };
        let logical_bytes = canonical(&logical)?;
        if logical_bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(exhausted("Iceberg cleanup manifest exceeds 64 MiB"));
        }
        let manifest_digest = domain_digest(MANIFEST_DOMAIN, &logical_bytes);
        let artifact_root = format!(
            "{}/_novarocks/maintenance/v3/orphan-cleanup/{}/{}",
            metadata.location().trim_end_matches('/'),
            hex::encode(request.operation_id().to_bytes()),
            hex::encode(manifest_digest),
        );
        write_manifest_artifact(
            &table.file_io().clone(),
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
            manifest_part_count(&logical.records)?,
            logical.records.len().div_ceil(MAX_BATCH_OBJECTS) as u32,
        )?;
        let base_state = state_digest(
            metadata.uuid().to_string().as_bytes(),
            table.metadata_location(),
            metadata
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
        );
        let payload = IcebergCleanupPlanPayloadV1 {
            version: ARTIFACT_VERSION,
            namespace,
            table: table_name,
            table_uuid: metadata.uuid().to_string(),
            metadata_location_digest_hex: hex::encode(metadata_location_digest(
                table.metadata_location(),
            )),
            artifact_root,
        };
        let plan = ConnectorCleanupPlan::try_new(
            &request,
            base_state,
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
        self.ensure_owner(request.plan.owner())?;
        let payload = self.payload_for_plan(&request.plan)?;
        let records = self.load_manifest(&request.plan, &payload)?;
        let first = request.batch_ordinal as usize * MAX_BATCH_OBJECTS;
        let end = (first + MAX_BATCH_OBJECTS).min(records.len());
        let batch = records
            .get(first..end)
            .ok_or_else(|| invalid("Iceberg cleanup batch is outside frozen manifest"))?;
        if batch.is_empty() {
            return Err(invalid("Iceberg cleanup batch is empty"));
        }
        let digest = batch_digest(batch);
        let evidence = PreparedPayloadV1 {
            version: ARTIFACT_VERSION,
            artifact_root: payload.artifact_root,
            batch_ordinal: request.batch_ordinal,
            first_ordinal: first as u32,
            record_count: batch.len() as u32,
            batch_digest_hex: hex::encode(digest),
        };
        PreparedBatch::try_new(
            self.key.clone(),
            request.plan.operation_id(),
            request.plan.plan_digest(),
            request.plan.manifest_digest(),
            request.batch_ordinal,
            digest,
            canonical(&evidence)?,
        )
    }

    fn execute_batch(
        &self,
        request: ConnectorCleanupExecuteRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        let (payload, _, batch) = self.prepared_records(&request.plan, &request.prepared)?;
        if let Some(receipt) = self.existing_receipt(&request.plan, &request.prepared, &payload)? {
            return Ok(receipt);
        }
        let entry = self.entry()?;
        let outcomes = execute_frozen_batch(&batch, entry.object_store_config())?;
        self.receipt_for_records(&request.plan, &request.prepared, &payload, outcomes)
    }

    fn reconcile_batch(
        &self,
        request: ConnectorCleanupReconcileRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        let (payload, _, batch) = self.prepared_records(&request.plan, &request.prepared)?;
        if let Some(receipt) = self.existing_receipt(&request.plan, &request.prepared, &payload)? {
            return Ok(receipt);
        }
        let entry = self.entry()?;
        let outcomes = reconcile_frozen_batch(&batch, entry.object_store_config())?;
        self.receipt_for_records(&request.plan, &request.prepared, &payload, outcomes)
    }

    fn read_candidate_page(
        &self,
        request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<CandidatePage, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        let payload = self.payload_for_plan(&request.plan)?;
        let records = self.load_manifest(&request.plan, &payload)?;
        let start = request.offset as usize;
        if start > records.len() {
            return Err(invalid("Iceberg cleanup page offset exceeds manifest"));
        }
        let end = (start + request.limit as usize).min(records.len());
        let locations = records[start..end]
            .iter()
            .map(|record| Arc::<str>::from(record.location.as_str()))
            .collect();
        CandidatePage::try_new(
            self.key.clone(),
            request.plan.operation_id(),
            request.plan.manifest_digest(),
            request.offset,
            locations,
            end == records.len(),
        )
    }

    fn finalize_terminal(
        &self,
        request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        // Retention is deliberately best effort.  The durable operation and SQL
        // projection already refer to immutable artifacts, so deleting them is
        // only a storage hygiene optimization and must not change the outcome.
        tracing::debug!(operation_id = ?request.plan.operation_id(), "Iceberg cleanup terminal artifact retention deferred");
        Ok(())
    }
}

fn records_from_candidates(
    files: &[ScannedFile],
    table: &iceberg::table::Table,
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ManifestRecordV1>, ConnectorError> {
    let supports_version = fs_io::resolve_access_for_location(table.metadata().location(), config)
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
                (Some(version), _) if supports_version => ObjectIdentityV1::Version {
                    version: version.clone(),
                    size,
                    mtime_ms: file.last_modified_ms,
                },
                (_, Some(etag)) => ObjectIdentityV1::Etag {
                    etag: etag.clone(),
                    size,
                    mtime_ms: file.last_modified_ms,
                },
                _ => ObjectIdentityV1::SizeMtime {
                    size,
                    mtime_ms: file.last_modified_ms,
                },
            };
            Ok(ManifestRecordV1 {
                ordinal: ordinal as u32,
                location: file.path.clone(),
                identity,
            })
        })
        .collect()
}

fn execute_frozen_batch(
    batch: &[ManifestRecordV1],
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ReceiptRecordV1>, ConnectorError> {
    batch
        .iter()
        .map(|record| {
            let access = fs_io::resolve_access_for_location(&record.location, config)
                .map_err(unavailable)?;
            let path = access.single_relative_path().map_err(invalid)?.to_string();
            let op = access.operator();
            let outcome = match block_on_iceberg(stat_matches(&op, &path, &record.identity)) {
                Err(error) => receipt(record.ordinal, ObjectOutcomeV1::Unknown, Some(error)),
                Ok(Ok(false)) => receipt(
                    record.ordinal,
                    ObjectOutcomeV1::Failed,
                    Some("object identity changed before delete".to_string()),
                ),
                Ok(Err(error)) if error.kind() == OpenDalErrorKind::NotFound => {
                    receipt(record.ordinal, ObjectOutcomeV1::AlreadyAbsent, None)
                }
                Ok(Err(error)) => receipt(
                    record.ordinal,
                    stat_error_outcome(error.kind()),
                    Some(error.to_string()),
                ),
                Ok(Ok(true)) => {
                    match block_on_iceberg(delete_exact(&op, &path, &record.identity)) {
                        Ok(Ok(())) => receipt(record.ordinal, ObjectOutcomeV1::Deleted, None),
                        Ok(Err(error)) if error.kind() == OpenDalErrorKind::NotFound => {
                            receipt(record.ordinal, ObjectOutcomeV1::AlreadyAbsent, None)
                        }
                        Ok(Err(error)) => receipt(
                            record.ordinal,
                            delete_error_outcome(error.kind()),
                            Some(error.to_string()),
                        ),
                        Err(error) => {
                            receipt(record.ordinal, ObjectOutcomeV1::Unknown, Some(error))
                        }
                    }
                }
            };
            outcome
        })
        .collect()
}

fn reconcile_frozen_batch(
    batch: &[ManifestRecordV1],
    config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<ReceiptRecordV1>, ConnectorError> {
    batch
        .iter()
        .map(|record| {
            let access = fs_io::resolve_access_for_location(&record.location, config)
                .map_err(unavailable)?;
            let path = access.single_relative_path().map_err(invalid)?.to_string();
            let op = access.operator();
            match block_on_iceberg(stat_matches(&op, &path, &record.identity)) {
                Err(error) => receipt(record.ordinal, ObjectOutcomeV1::Unknown, Some(error)),
                Ok(Err(error)) if error.kind() == OpenDalErrorKind::NotFound => {
                    receipt(record.ordinal, ObjectOutcomeV1::Deleted, None)
                }
                Ok(Ok(true)) => receipt(
                    record.ordinal,
                    ObjectOutcomeV1::Failed,
                    Some("object remains after uncertain delete".to_string()),
                ),
                Ok(Ok(false)) => receipt(
                    record.ordinal,
                    ObjectOutcomeV1::Failed,
                    Some("object identity changed after uncertain delete".to_string()),
                ),
                Ok(Err(error)) => receipt(
                    record.ordinal,
                    ObjectOutcomeV1::Unknown,
                    Some(error.to_string()),
                ),
            }
        })
        .collect()
}

async fn stat_matches(
    op: &opendal::Operator,
    path: &str,
    identity: &ObjectIdentityV1,
) -> Result<bool, opendal::Error> {
    let metadata = op.stat(path).await?;
    let size = metadata.content_length();
    let mtime = metadata
        .last_modified()
        .map(|time| canonical_object_mtime_ms(time.into_inner().as_millisecond()));
    Ok(match identity {
        ObjectIdentityV1::Version {
            version,
            size: expected_size,
            mtime_ms,
        } => {
            metadata.version() == Some(version.as_str())
                && size == *expected_size
                && mtime == Some(*mtime_ms)
        }
        ObjectIdentityV1::Etag {
            etag,
            size: expected_size,
            mtime_ms,
        } => {
            metadata.etag() == Some(etag.as_str())
                && size == *expected_size
                && mtime == Some(*mtime_ms)
        }
        ObjectIdentityV1::SizeMtime {
            size: expected_size,
            mtime_ms,
        } => size == *expected_size && mtime == Some(*mtime_ms),
    })
}

async fn delete_exact(
    op: &opendal::Operator,
    path: &str,
    identity: &ObjectIdentityV1,
) -> Result<(), opendal::Error> {
    match identity {
        ObjectIdentityV1::Version { version, .. } => op.delete_with(path).version(version).await,
        _ => op.delete(path).await,
    }
}

fn receipt(
    ordinal: u32,
    outcome: ObjectOutcomeV1,
    reason: Option<String>,
) -> Result<ReceiptRecordV1, ConnectorError> {
    let reason = reason.map(|value| truncate_reason(&value));
    Ok(ReceiptRecordV1 {
        ordinal,
        outcome,
        reason,
    })
}
fn stat_error_outcome(kind: OpenDalErrorKind) -> ObjectOutcomeV1 {
    match kind {
        OpenDalErrorKind::PermissionDenied | OpenDalErrorKind::ConditionNotMatch => {
            ObjectOutcomeV1::Failed
        }
        _ => ObjectOutcomeV1::Unknown,
    }
}
fn delete_error_outcome(kind: OpenDalErrorKind) -> ObjectOutcomeV1 {
    stat_error_outcome(kind)
}
fn truncate_reason(reason: &str) -> String {
    reason.chars().take(MAX_REASON_BYTES).collect()
}

fn write_manifest_artifact(
    file_io: &FileIO,
    root: &str,
    digest: [u8; 32],
    logical: &ManifestLogicalV1,
) -> Result<(), ConnectorError> {
    let parts = split_manifest_parts(&logical.records)?;
    let mut refs = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let bytes = canonical(part)?;
        let part_digest = domain_digest(PART_DOMAIN, &bytes);
        write_immutable(file_io, &format!("{root}/part-{index:04}.json"), bytes)?;
        refs.push(ArtifactPartRefV1 {
            index: index as u16,
            digest_hex: hex::encode(part_digest),
        });
    }
    let root_value = ManifestRootV1 {
        version: ARTIFACT_VERSION,
        manifest_digest_hex: hex::encode(digest),
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
        parts: refs,
    };
    write_immutable(
        file_io,
        &format!("{root}/manifest.json"),
        canonical(&root_value)?,
    )
}

fn load_manifest_from_root(
    file_io: &FileIO,
    root: &str,
    expected_digest: [u8; 32],
) -> Result<Vec<ManifestRecordV1>, ConnectorError> {
    let manifest: ManifestRootV1 = decode_canonical(
        &read_artifact(file_io, &format!("{root}/manifest.json"), 64 * 1024)?,
        "Iceberg cleanup manifest root",
    )?;
    if manifest.version != ARTIFACT_VERSION
        || manifest.manifest_digest_hex != hex::encode(expected_digest)
        || manifest.parts.len() > MAX_ARTIFACT_PARTS
    {
        return Err(corrupt("Iceberg cleanup manifest root is invalid"));
    }
    let mut records = Vec::new();
    for (index, reference) in manifest.parts.iter().enumerate() {
        if reference.index as usize != index {
            return Err(corrupt("Iceberg cleanup manifest parts are unordered"));
        }
        let bytes = read_artifact(
            file_io,
            &format!("{root}/part-{index:04}.json"),
            MAX_ARTIFACT_PART_BYTES,
        )?;
        if domain_digest(PART_DOMAIN, &bytes)
            != decode_hex_digest(&reference.digest_hex, "cleanup manifest part")?
        {
            return Err(corrupt("Iceberg cleanup manifest part digest is invalid"));
        }
        let part: ManifestPartV1 = decode_canonical(&bytes, "Iceberg cleanup manifest part")?;
        if part.version != ARTIFACT_VERSION {
            return Err(corrupt("Iceberg cleanup manifest part version is invalid"));
        }
        records.extend(part.records);
    }
    if records.len() != manifest.record_count as usize
        || records.len() > MAX_ARTIFACT_RECORDS
        || records.windows(2).any(|pair| {
            pair[0].ordinal + 1 != pair[1].ordinal || pair[0].location >= pair[1].location
        })
    {
        return Err(corrupt("Iceberg cleanup manifest records are invalid"));
    }
    let logical = ManifestLogicalV1 {
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

fn write_receipt_artifact(
    file_io: &FileIO,
    root: &str,
    batch_digest: [u8; 32],
    records: &[ReceiptRecordV1],
) -> Result<[u8; 32], ConnectorError> {
    let parts = split_receipt_parts(records)?;
    let mut refs = Vec::with_capacity(parts.len());
    let mut all = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        let bytes = canonical(part)?;
        let digest = domain_digest(PART_DOMAIN, &bytes);
        write_immutable(file_io, &format!("{root}/part-{index:04}.json"), bytes)?;
        refs.push(ArtifactPartRefV1 {
            index: index as u16,
            digest_hex: hex::encode(digest),
        });
        all.extend(part.records.clone());
    }
    let root_value = ReceiptRootV1 {
        version: ARTIFACT_VERSION,
        batch_digest_hex: hex::encode(batch_digest),
        records: all.len() as u32,
        parts: refs,
    };
    let root_bytes = canonical(&root_value)?;
    let digest = domain_digest(RECEIPT_DOMAIN, &root_bytes);
    write_immutable(file_io, &format!("{root}/manifest.json"), root_bytes)?;
    Ok(digest)
}

fn read_receipt_artifact(
    file_io: &FileIO,
    root: &str,
    batch_digest: [u8; 32],
) -> Result<Option<([u8; 32], Vec<ReceiptRecordV1>)>, ConnectorError> {
    let location = format!("{root}/manifest.json");
    let input = file_io.new_input(&location).map_err(unavailable)?;
    let exists = block_on_iceberg(async move { input.exists().await })
        .map_err(unavailable)?
        .map_err(unavailable)?;
    if !exists {
        return Ok(None);
    }
    let bytes = read_artifact(file_io, &location, 64 * 1024)?;
    let root_value: ReceiptRootV1 = decode_canonical(&bytes, "Iceberg cleanup receipt root")?;
    if root_value.version != ARTIFACT_VERSION
        || root_value.batch_digest_hex != hex::encode(batch_digest)
        || root_value.parts.len() > MAX_ARTIFACT_PARTS
    {
        return Err(corrupt("Iceberg cleanup receipt root is invalid"));
    }
    let digest = domain_digest(RECEIPT_DOMAIN, &bytes);
    let mut records = Vec::new();
    for (index, reference) in root_value.parts.iter().enumerate() {
        if reference.index as usize != index {
            return Err(corrupt("Iceberg cleanup receipt parts are unordered"));
        }
        let part_bytes = read_artifact(
            file_io,
            &format!("{root}/part-{index:04}.json"),
            MAX_ARTIFACT_PART_BYTES,
        )?;
        if domain_digest(PART_DOMAIN, &part_bytes)
            != decode_hex_digest(&reference.digest_hex, "cleanup receipt part")?
        {
            return Err(corrupt("Iceberg cleanup receipt part digest is invalid"));
        }
        let part: ReceiptPartV1 = decode_canonical(&part_bytes, "Iceberg cleanup receipt part")?;
        if part.version != ARTIFACT_VERSION {
            return Err(corrupt("Iceberg cleanup receipt part version is invalid"));
        }
        records.extend(part.records);
    }
    if records.len() != root_value.records as usize
        || records
            .windows(2)
            .any(|pair| pair[0].ordinal + 1 != pair[1].ordinal)
    {
        return Err(corrupt("Iceberg cleanup receipt records are invalid"));
    }
    Ok(Some((digest, records)))
}

fn split_manifest_parts(
    records: &[ManifestRecordV1],
) -> Result<Vec<ManifestPartV1>, ConnectorError> {
    split_parts(records, |records| ManifestPartV1 {
        version: ARTIFACT_VERSION,
        records,
    })
}
fn split_receipt_parts(records: &[ReceiptRecordV1]) -> Result<Vec<ReceiptPartV1>, ConnectorError> {
    split_parts(records, |records| ReceiptPartV1 {
        version: ARTIFACT_VERSION,
        records,
    })
}
fn split_parts<T: Clone, P: Serialize>(
    records: &[T],
    make: impl Fn(Vec<T>) -> P,
) -> Result<Vec<P>, ConnectorError> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    for record in records {
        let mut candidate = current.clone();
        candidate.push(record.clone());
        if canonical(&make(candidate.clone()))?.len() <= MAX_ARTIFACT_PART_BYTES {
            current = candidate;
            continue;
        }
        if current.is_empty() {
            return Err(exhausted("Iceberg cleanup artifact record exceeds 1 MiB"));
        }
        parts.push(make(std::mem::take(&mut current)));
        current.push(record.clone());
    }
    if !current.is_empty() {
        parts.push(make(current));
    }
    if parts.len() > MAX_ARTIFACT_PARTS {
        return Err(exhausted("Iceberg cleanup artifact exceeds 64 parts"));
    }
    Ok(parts)
}

fn manifest_part_count(records: &[ManifestRecordV1]) -> Result<u32, ConnectorError> {
    Ok(split_manifest_parts(records)?.len() as u32)
}
fn batch_digest(records: &[ManifestRecordV1]) -> [u8; 32] {
    domain_digest(
        BATCH_DOMAIN,
        &canonical(records).expect("manifest record serialization is bounded"),
    )
}

fn write_immutable(file_io: &FileIO, location: &str, bytes: Bytes) -> Result<(), ConnectorError> {
    let input = file_io.new_input(location).map_err(unavailable)?;
    let existing = block_on_iceberg(async move { input.exists().await })
        .map_err(unavailable)?
        .map_err(unavailable)?;
    if existing {
        let prior = read_artifact(file_io, location, MAX_ARTIFACT_PART_BYTES)?;
        if prior != bytes {
            return Err(corrupt(
                "Iceberg cleanup content-addressed artifact conflicts with existing content",
            ));
        }
        return Ok(());
    }
    let output = file_io.new_output(location).map_err(unavailable)?;
    block_on_iceberg(async move { output.write(bytes).await })
        .map_err(unavailable)?
        .map_err(unavailable)
}
fn read_artifact(file_io: &FileIO, location: &str, max: usize) -> Result<Bytes, ConnectorError> {
    let input = file_io.new_input(location).map_err(unavailable)?;
    let bytes = block_on_iceberg(async move { input.read().await })
        .map_err(unavailable)?
        .map_err(unavailable)?;
    if bytes.len() > max {
        return Err(exhausted(
            "Iceberg cleanup artifact part exceeds its size budget",
        ));
    }
    Ok(bytes)
}

fn table_file_io(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table: &str,
) -> Result<(FileIO, String), ConnectorError> {
    entry.invalidate_table_cache(namespace, table);
    let loaded = load_table(entry, namespace, table).map_err(unavailable)?;
    let table_location = loaded
        .table
        .metadata()
        .location()
        .trim_end_matches('/')
        .to_string();
    Ok((loaded.table.file_io().clone(), table_location))
}
fn build_catalog(entry: &IcebergCatalogEntry) -> Result<Arc<dyn iceberg::Catalog>, ConnectorError> {
    super::catalog::registry::build_iceberg_catalog(entry).map_err(unavailable)
}

fn validate_plan_payload(
    plan: &ConnectorCleanupPlan,
    payload: &IcebergCleanupPlanPayloadV1,
) -> Result<(), ConnectorError> {
    if payload.version != ARTIFACT_VERSION
        || payload.artifact_root.is_empty()
        || !payload
            .artifact_root
            .ends_with(&hex::encode(plan.manifest_digest()))
    {
        return Err(corrupt("Iceberg cleanup plan payload is invalid"));
    }
    Ok(())
}
fn validate_prepared_payload(
    plan: &ConnectorCleanupPlan,
    prepared: &PreparedBatch,
    payload: &IcebergCleanupPlanPayloadV1,
    evidence: &PreparedPayloadV1,
) -> Result<(), ConnectorError> {
    if evidence.version != ARTIFACT_VERSION
        || evidence.artifact_root != payload.artifact_root
        || evidence.batch_ordinal != prepared.batch_ordinal()
        || evidence.record_count == 0
        || evidence.record_count as usize > MAX_BATCH_OBJECTS
        || decode_hex_digest(&evidence.batch_digest_hex, "cleanup prepared batch")?
            != prepared.batch_digest()
        || prepared.batch_ordinal() >= plan.summary().batch_count()
    {
        return Err(corrupt("Iceberg cleanup prepared evidence is invalid"));
    }
    Ok(())
}
fn receipt_summary(records: &[ReceiptRecordV1]) -> Result<BatchReceiptSummary, ConnectorError> {
    let mut summary = BatchReceiptSummary::default();
    for record in records {
        match record.outcome {
            ObjectOutcomeV1::Deleted => {
                summary = BatchReceiptSummary::new(
                    summary.deleted() + 1,
                    summary.already_absent(),
                    summary.failed(),
                    summary.unknown(),
                )
            }
            ObjectOutcomeV1::AlreadyAbsent => {
                summary = BatchReceiptSummary::new(
                    summary.deleted(),
                    summary.already_absent() + 1,
                    summary.failed(),
                    summary.unknown(),
                )
            }
            ObjectOutcomeV1::Failed => {
                summary = BatchReceiptSummary::new(
                    summary.deleted(),
                    summary.already_absent(),
                    summary.failed() + 1,
                    summary.unknown(),
                )
            }
            ObjectOutcomeV1::Unknown => {
                summary = BatchReceiptSummary::new(
                    summary.deleted(),
                    summary.already_absent(),
                    summary.failed(),
                    summary.unknown() + 1,
                )
            }
        }
    }
    if summary.total() != records.len() as u32 {
        return Err(corrupt("Iceberg cleanup receipt summary overflows"));
    }
    Ok(summary)
}
fn identity_size(identity: &ObjectIdentityV1) -> u64 {
    match identity {
        ObjectIdentityV1::Version { size, .. }
        | ObjectIdentityV1::Etag { size, .. }
        | ObjectIdentityV1::SizeMtime { size, .. } => *size,
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
fn metadata_location_digest(location: Option<&str>) -> [u8; 32] {
    domain_digest(
        b"novarocks.iceberg.orphan-cleanup.metadata-location.v1\0",
        location.unwrap_or_default().as_bytes(),
    )
}
fn domain_digest(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.finalize().into()
}
fn decode_hex_digest(value: &str, subject: &str) -> Result<[u8; 32], ConnectorError> {
    let raw = hex::decode(value).map_err(|_| corrupt(format!("{subject} digest is not hex")))?;
    raw.try_into()
        .map_err(|_| corrupt(format!("{subject} digest has invalid length")))
}
fn canonical<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, ConnectorError> {
    let value = serde_json::to_value(value)
        .map_err(|error| internal(format!("encode Iceberg cleanup JSON: {error}")))?;
    let mut output = Vec::new();
    write_canonical(&value, &mut output)?;
    Ok(Bytes::from(output))
}
fn decode_canonical<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    label: &str,
) -> Result<T, ConnectorError> {
    let value = serde_json::from_slice(bytes)
        .map_err(|error| corrupt(format!("decode {label}: {error}")))?;
    let canonical_bytes = canonical(&value)?;
    if canonical_bytes.as_ref() != bytes {
        return Err(corrupt(format!("{label} is not canonical JSON")));
    }
    serde_json::from_value(value).map_err(|error| corrupt(format!("decode {label}: {error}")))
}
fn write_canonical(value: &Value, output: &mut Vec<u8>) -> Result<(), ConnectorError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => output.extend_from_slice(
            serde_json::to_string(value)
                .map_err(|error| internal(error.to_string()))?
                .as_bytes(),
        ),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                output.extend_from_slice(
                    serde_json::to_string(key)
                        .map_err(|error| internal(error.to_string()))?
                        .as_bytes(),
                );
                output.push(b':');
                write_canonical(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}
fn invalid(message: impl ToString) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.to_string())
}
fn corrupt(message: impl ToString) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.to_string())
}
fn unavailable(message: impl ToString) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.to_string())
}
fn exhausted(message: impl ToString) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message.to_string())
}
fn internal(message: impl ToString) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.to_string())
}
