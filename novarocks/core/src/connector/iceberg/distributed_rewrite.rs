// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Iceberg-owned planning facts for C1-backed distributed rewrites.
//!
//! This module freezes the input file ownership before a frontend asks C1 to
//! place a writer.  It deliberately serializes the detailed file list only to
//! a provider artifact: generic SPI transports its digest and a bounded group
//! handle, never Iceberg files or catalog state.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewriteOperation,
    ConnectorDistributedRewritePlan, ConnectorDistributedRewritePlanSummary,
    ConnectorDistributedRewritePlanningRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorWriteCohortId, ConnectorWriteIntent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info;
use super::catalog::registry::{
    DataFileWithStats, IcebergCatalogEntry, IcebergCatalogRegistry, block_on_iceberg,
    extract_data_files_with_stats, load_table,
};
use super::scan_model::{IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat};

pub(crate) const ARTIFACT_VERSION: u16 = 1;
pub(crate) const GROUP_PAYLOAD_VERSION: u16 = 1;
pub(crate) const REWRITE_ARTIFACT_MAX_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const REWRITE_ARTIFACT_MAX_GROUPS: usize = 4096;
pub(crate) const REWRITE_ARTIFACT_MAX_PARTS: usize = 64;
pub(crate) const REWRITE_ARTIFACT_MAX_PART_BYTES: usize = 1024 * 1024;
pub(crate) const REWRITE_ARTIFACT_MAX_ROOT_BYTES: usize = 64 * 1024;

const GROUP_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.group.v1\0";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.state.v1\0";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergRewritePlanPayloadV1 {
    version: u16,
    artifact_digest_hex: String,
    artifact_location: String,
    target_ref: String,
}

#[derive(Clone)]
pub(crate) struct PlannedIcebergDistributedRewrite {
    pub(crate) plan: ConnectorDistributedRewritePlan,
    pub(crate) artifact: IcebergFrozenRewriteArtifactV1,
    pub(crate) artifact_location: String,
}

/// Exact-generation FE planner.  Its cache is intentionally operation scoped:
/// the same request may replay, while a different request under the same
/// operation ID is rejected before any BE staging is possible.
pub(crate) struct IcebergDistributedRewritePlanner {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    instance_id: ConnectorInstanceId,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
    plans: Mutex<
        HashMap<
            novarocks_spi::connector::ConnectorWriteOperationId,
            PlannedIcebergDistributedRewrite,
        >,
    >,
}

impl IcebergDistributedRewritePlanner {
    pub(crate) fn new_registered(
        key: ConnectorExecutionBindingKey,
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        if instance_id != key.instance_id {
            return Err(invalid(
                "Iceberg distributed rewrite planner instance does not match key",
            ));
        }
        Ok(Self {
            key,
            descriptor,
            instance_id,
            registry,
            plans: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub(crate) fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    pub(crate) fn plan(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
        request.validate()?;
        if request.owner() != &self.key {
            return Err(invalid(
                "Iceberg distributed rewrite request has a foreign generation",
            ));
        }
        if let Some(existing) = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?
            .get(&request.operation_id())
            .cloned()
        {
            if existing.plan.request_digest() == request.request_digest() {
                return Ok(existing.plan);
            }
            return Err(invalid(
                "Iceberg distributed rewrite operation conflicts with cached plan",
            ));
        }

        let planned = self.build_plan(&request)?;
        let mut plans = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?;
        match plans.get(&request.operation_id()) {
            Some(existing) if existing.plan.request_digest() == request.request_digest() => {
                Ok(existing.plan.clone())
            }
            Some(_) => Err(invalid(
                "Iceberg distributed rewrite operation conflicts with cached plan",
            )),
            None => {
                let plan = planned.plan.clone();
                plans.insert(request.operation_id(), planned);
                Ok(plan)
            }
        }
    }

    pub(crate) fn planned(
        &self,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    ) -> Result<PlannedIcebergDistributedRewrite, ConnectorError> {
        self.plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg rewrite operation has no frozen plan",
                )
            })
    }

    fn entry(&self) -> Result<IcebergCatalogEntry, ConnectorError> {
        self.registry
            .read()
            .map_err(|_| internal("Iceberg distributed rewrite registry lock poisoned"))?
            .get(self.instance_id.as_str())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
            })
    }

    fn build_plan(
        &self,
        request: &ConnectorDistributedRewritePlanningRequest,
    ) -> Result<PlannedIcebergDistributedRewrite, ConnectorError> {
        let (namespace, table_name) =
            super::provider::decode_data_mutation_table_target(request.operation().table())?;
        let entry = self.entry()?;
        entry.invalidate_table_cache(&namespace, &table_name);
        let loaded = load_table(&entry, &namespace, &table_name).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
        })?;
        let table = loaded.table;
        let metadata = table.metadata();
        let base_snapshot_id = metadata
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        let files = extract_data_files_with_stats(&table)
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error))?;
        let groups = match request.operation() {
            ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
                plan_data_file_groups(files)?
            }
            ConnectorDistributedRewriteOperation::RewritePositionDeletes {
                rewrite_all,
                min_input_files,
                ..
            } => {
                if metadata.format_version() != iceberg::spec::FormatVersion::V3 {
                    return Err(invalid(
                        "Iceberg rewrite position delete files requires a format v3 table",
                    ));
                }
                plan_position_delete_groups(
                    files,
                    *rewrite_all,
                    min_input_files.unwrap_or(2) as usize,
                )?
            }
        };
        let artifact = IcebergFrozenRewriteArtifactV1 {
            version: ARTIFACT_VERSION,
            operation_kind: request.operation().kind().to_string(),
            namespace: namespace.clone(),
            table: table_name.clone(),
            table_uuid: metadata.uuid().to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id,
            schema_id: metadata.current_schema_id(),
            default_spec_id: metadata.default_partition_spec_id(),
            groups,
        };
        let artifact_bytes = artifact.canonical_bytes()?;
        let artifact_digest = artifact_digest(&artifact_bytes);
        let artifact_location = format!(
            "{}/_novarocks/maintenance/v2/distributed-rewrite/{}/{}",
            metadata.location(),
            hex::encode(request.operation_id().to_bytes()),
            hex::encode(artifact_digest),
        );
        write_frozen_artifact(
            table.file_io().clone(),
            &artifact,
            artifact_digest,
            &artifact_location,
        )?;

        let physical_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(metadata.current_schema()).map_err(|error| {
                internal(format!("convert Iceberg rewrite schema to Arrow: {error}"))
            })?,
        );
        let input_schema = rewrite_input_schema(request.operation(), physical_schema);
        let cohorts = cohort_plans_from_artifact(
            request,
            artifact_digest,
            &artifact_location,
            &artifact.groups,
            input_schema,
        )?;
        let state_digest = rewrite_state_digest(
            metadata.uuid().to_string().as_bytes(),
            table.metadata_location().ok_or_else(|| {
                invalid("Iceberg distributed rewrite table has no metadata location")
            })?,
            base_snapshot_id,
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
        );
        let summary = ConnectorDistributedRewritePlanSummary {
            groups: artifact.groups.len() as u64,
            input_data_files: artifact
                .groups
                .iter()
                .map(|group| group.data_files.len() as u64)
                .sum(),
            input_delete_files: artifact
                .groups
                .iter()
                .map(|group| {
                    (group.selected_position_delete_files.len()
                        + group.owned_data_delete_files.len()) as u64
                })
                .sum(),
            input_bytes: artifact
                .groups
                .iter()
                .flat_map(|group| group.data_files.iter())
                .map(|file| file.size.max(0) as u64)
                .sum(),
            expected_output_files: 0,
        };
        let payload = canonical_payload(&IcebergRewritePlanPayloadV1 {
            version: 1,
            artifact_digest_hex: hex::encode(artifact_digest),
            artifact_location: artifact_location.clone(),
            target_ref: "main".to_string(),
        })?;
        let plan = ConnectorDistributedRewritePlan::try_new(
            request,
            state_digest,
            artifact_digest,
            summary,
            payload,
            cohorts,
        )?;
        Ok(PlannedIcebergDistributedRewrite {
            plan,
            artifact,
            artifact_location,
        })
    }
}

/// The immutable, provider-private plan.  It is intentionally canonical JSON
/// so the artifact itself can be content-addressed and verified after an FE
/// restart without making its file list public SPI state.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergFrozenRewriteArtifactV1 {
    pub version: u16,
    pub operation_kind: String,
    pub namespace: String,
    pub table: String,
    pub table_uuid: String,
    pub target_ref: String,
    pub base_snapshot_id: Option<i64>,
    pub schema_id: i32,
    pub default_spec_id: i32,
    pub groups: Vec<IcebergFrozenRewriteGroupV1>,
}

/// Bounded root record.  It intentionally carries no selected files: those
/// remain in content-addressed parts below the provider-private root.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergFrozenRewriteArtifactRootV1 {
    version: u16,
    logical_artifact_digest_hex: String,
    operation_kind: String,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
    parts: Vec<IcebergFrozenRewriteArtifactPartRefV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergFrozenRewriteArtifactPartRefV1 {
    index: u16,
    digest_hex: String,
    location: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergFrozenRewriteArtifactPartV1 {
    version: u16,
    groups: Vec<IcebergFrozenRewriteGroupV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergFrozenRewriteGroupV1 {
    pub group_digest_hex: String,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub data_files: Vec<IcebergDataFileInfo>,
    /// Puffin deletion-vector inputs selected for a position-delete rewrite.
    /// Data rewrite groups leave this empty; any delete dependency remains
    /// attached to its data file as read-only scan input.
    pub selected_position_delete_files: Vec<String>,
    /// Delete files removed by a data rewrite. A shared dependency has exactly
    /// one canonical owner, while readers retain it until aggregate commit.
    #[serde(default)]
    pub owned_data_delete_files: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergRewriteGroupPayloadV1 {
    pub version: u16,
    pub group_digest_hex: String,
    pub artifact_digest_hex: String,
    pub artifact_location: String,
}

impl IcebergFrozenRewriteArtifactV1 {
    pub(crate) fn canonical_bytes(&self) -> Result<Bytes, ConnectorError> {
        if self.version != ARTIFACT_VERSION || self.groups.len() > REWRITE_ARTIFACT_MAX_GROUPS {
            return Err(invalid(
                "Iceberg rewrite artifact version or group count is invalid",
            ));
        }
        let bytes = canonical_json(self, "frozen Iceberg rewrite artifact")?;
        if bytes.len() > REWRITE_ARTIFACT_MAX_BYTES {
            return Err(exhausted("frozen Iceberg rewrite artifact exceeds 64 MiB"));
        }
        Ok(bytes)
    }
}

pub(crate) fn decode_group_payload(
    payload: &[u8],
) -> Result<IcebergRewriteGroupPayloadV1, ConnectorError> {
    let decoded: IcebergRewriteGroupPayloadV1 =
        decode_canonical_json(payload, "Iceberg distributed rewrite group payload")?;
    if decoded.version != GROUP_PAYLOAD_VERSION {
        return Err(invalid(
            "Iceberg distributed rewrite group payload version is unsupported",
        ));
    }
    decode_digest(&decoded.group_digest_hex, "Iceberg rewrite group")?;
    decode_digest(&decoded.artifact_digest_hex, "Iceberg rewrite artifact")?;
    if decoded.artifact_location.is_empty()
        || decoded.artifact_location.len() > 16 * 1024
        || decoded.artifact_location.ends_with('/')
    {
        return Err(invalid(
            "Iceberg distributed rewrite artifact location is invalid",
        ));
    }
    Ok(decoded)
}

/// Reassemble and verify the bounded provider artifact before exposing a
/// group to the Iceberg scan planner. Generic core only carries the opaque
/// group payload; this function is the single Iceberg-only decoder.
pub(crate) fn load_frozen_rewrite_group(
    file_io: &iceberg::io::FileIO,
    payload: &IcebergRewriteGroupPayloadV1,
) -> Result<IcebergFrozenRewriteGroupV1, ConnectorError> {
    let root_location = format!("{}/manifest.json", payload.artifact_location);
    let root_bytes = read_artifact_file(file_io, &root_location, REWRITE_ARTIFACT_MAX_ROOT_BYTES)?;
    let root: IcebergFrozenRewriteArtifactRootV1 =
        decode_canonical_json(&root_bytes, "Iceberg distributed rewrite artifact root")?;
    if root.version != ARTIFACT_VERSION
        || root.parts.is_empty()
        || root.parts.len() > REWRITE_ARTIFACT_MAX_PARTS
    {
        return Err(invalid(
            "Iceberg distributed rewrite artifact root is invalid",
        ));
    }
    let expected_digest = decode_digest(&payload.artifact_digest_hex, "Iceberg rewrite artifact")?;
    if root.logical_artifact_digest_hex != payload.artifact_digest_hex {
        return Err(invalid(
            "Iceberg distributed rewrite root has a foreign digest",
        ));
    }
    let mut groups = Vec::new();
    for (expected_index, part_ref) in root.parts.iter().enumerate() {
        if part_ref.index as usize != expected_index
            || part_ref.location
                != format!(
                    "{}/part-{expected_index:04}.json",
                    payload.artifact_location
                )
        {
            return Err(invalid(
                "Iceberg distributed rewrite part reference is invalid",
            ));
        }
        let bytes =
            read_artifact_file(file_io, &part_ref.location, REWRITE_ARTIFACT_MAX_PART_BYTES)?;
        if artifact_part_digest(&bytes)
            != decode_digest(&part_ref.digest_hex, "Iceberg rewrite part")?
        {
            return Err(invalid(
                "Iceberg distributed rewrite part digest is invalid",
            ));
        }
        let part: IcebergFrozenRewriteArtifactPartV1 =
            decode_canonical_json(&bytes, "Iceberg distributed rewrite artifact part")?;
        if part.version != ARTIFACT_VERSION || part.groups.is_empty() {
            return Err(invalid(
                "Iceberg distributed rewrite artifact part is invalid",
            ));
        }
        groups.extend(part.groups);
    }
    groups.sort_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex));
    if groups.len() > REWRITE_ARTIFACT_MAX_GROUPS
        || groups
            .windows(2)
            .any(|pair| pair[0].group_digest_hex == pair[1].group_digest_hex)
    {
        return Err(invalid(
            "Iceberg distributed rewrite artifact groups are invalid",
        ));
    }
    let logical = IcebergFrozenRewriteArtifactV1 {
        version: root.version,
        operation_kind: root.operation_kind,
        namespace: root.namespace,
        table: root.table,
        table_uuid: root.table_uuid,
        target_ref: root.target_ref,
        base_snapshot_id: root.base_snapshot_id,
        schema_id: root.schema_id,
        default_spec_id: root.default_spec_id,
        groups,
    };
    if artifact_digest(&logical.canonical_bytes()?) != expected_digest {
        return Err(invalid(
            "Iceberg distributed rewrite artifact digest is invalid",
        ));
    }
    logical
        .groups
        .into_iter()
        .find(|group| group.group_digest_hex == payload.group_digest_hex)
        .ok_or_else(|| invalid("Iceberg distributed rewrite artifact has no requested group"))
}

fn read_artifact_file(
    file_io: &iceberg::io::FileIO,
    location: &str,
    max_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    let input = file_io
        .new_input(location)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    let bytes = block_on_iceberg(async move { input.read().await })
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    if bytes.len() > max_bytes {
        return Err(exhausted(format!(
            "Iceberg distributed rewrite artifact file exceeds {max_bytes} bytes"
        )));
    }
    Ok(bytes)
}

/// Store the immutable provider artifact in a small root plus bounded parts.
/// The digest intentionally covers the logical, reassembled artifact, not
/// storage paths or process-local catalog state.  A single oversized group is
/// rejected rather than silently widening the carrier or flattening groups.
fn write_frozen_artifact(
    file_io: iceberg::io::FileIO,
    artifact: &IcebergFrozenRewriteArtifactV1,
    logical_digest: [u8; 32],
    root_location: &str,
) -> Result<(), ConnectorError> {
    let parts = split_artifact_parts(&artifact.groups)?;
    let mut refs = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let bytes = canonical_json(part, "frozen Iceberg rewrite artifact part")?;
        debug_assert!(bytes.len() <= REWRITE_ARTIFACT_MAX_PART_BYTES);
        let digest = artifact_part_digest(&bytes);
        let location = format!("{root_location}/part-{index:04}.json");
        let output = file_io.new_output(&location).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
        })?;
        block_on_iceberg(async move { output.write(bytes).await })
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
            })?
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
            })?;
        refs.push(IcebergFrozenRewriteArtifactPartRefV1 {
            index: index as u16,
            digest_hex: hex::encode(digest),
            location,
        });
    }
    let root = IcebergFrozenRewriteArtifactRootV1 {
        version: ARTIFACT_VERSION,
        logical_artifact_digest_hex: hex::encode(logical_digest),
        operation_kind: artifact.operation_kind.clone(),
        namespace: artifact.namespace.clone(),
        table: artifact.table.clone(),
        table_uuid: artifact.table_uuid.clone(),
        target_ref: artifact.target_ref.clone(),
        base_snapshot_id: artifact.base_snapshot_id,
        schema_id: artifact.schema_id,
        default_spec_id: artifact.default_spec_id,
        parts: refs,
    };
    let root_bytes = canonical_json(&root, "frozen Iceberg rewrite artifact root")?;
    if root_bytes.len() > REWRITE_ARTIFACT_MAX_ROOT_BYTES {
        return Err(exhausted("Iceberg rewrite artifact root exceeds 64 KiB"));
    }
    let output = file_io
        .new_output(&format!("{root_location}/manifest.json"))
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    block_on_iceberg(async move { output.write(root_bytes).await })
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    Ok(())
}

fn split_artifact_parts(
    groups: &[IcebergFrozenRewriteGroupV1],
) -> Result<Vec<IcebergFrozenRewriteArtifactPartV1>, ConnectorError> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    for group in groups {
        let mut candidate = current.clone();
        candidate.push(group.clone());
        let candidate_part = IcebergFrozenRewriteArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: candidate,
        };
        if canonical_json(&candidate_part, "frozen Iceberg rewrite artifact part")?.len()
            <= REWRITE_ARTIFACT_MAX_PART_BYTES
        {
            current = candidate_part.groups;
            continue;
        }
        if current.is_empty() {
            return Err(exhausted(
                "Iceberg rewrite group exceeds the 1 MiB artifact-part limit",
            ));
        }
        parts.push(IcebergFrozenRewriteArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: std::mem::take(&mut current),
        });
        current.push(group.clone());
    }
    if !current.is_empty() {
        parts.push(IcebergFrozenRewriteArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: current,
        });
    }
    if parts.len() > REWRITE_ARTIFACT_MAX_PARTS {
        return Err(exhausted(
            "Iceberg rewrite artifact exceeds the 64-part storage limit",
        ));
    }
    Ok(parts)
}

/// Build deterministic data-file rewrite groups.  A group owns every data
/// file it lists; delete dependencies remain nested under their owner file so
/// no delete file can cause cross-group ownership in a later aggregate commit.
pub(crate) fn plan_data_file_groups(
    files: Vec<DataFileWithStats>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    let files = files
        .into_iter()
        .map(data_file_with_stats_to_iceberg_data_file_info)
        .collect::<Vec<_>>();
    group_data_files(files, false, None)
}

/// Build deterministic deletion-vector rewrite groups.  Each group is keyed
/// by its referenced data file, and only V3 Puffin position-delete inputs are
/// selected.  V2 position deletes are intentionally not smuggled through this
/// route: the caller/provider must reject that table before staging.
pub(crate) fn plan_position_delete_groups(
    files: Vec<DataFileWithStats>,
    rewrite_all: bool,
    min_input_files: usize,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    let files = files
        .into_iter()
        .map(data_file_with_stats_to_iceberg_data_file_info)
        .collect::<Vec<_>>();
    group_data_files(files, rewrite_all, Some(min_input_files))
}

fn group_data_files(
    mut files: Vec<IcebergDataFileInfo>,
    rewrite_all: bool,
    position_delete_min_inputs: Option<usize>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    files.sort_by(|left, right| left.path.cmp(&right.path));
    if let Some(min_inputs) = position_delete_min_inputs {
        let mut groups = Vec::new();
        for file in files {
            let selected = file
                .delete_files
                .iter()
                .filter(|delete| {
                    matches!(delete.file_content, IcebergDeleteFileContent::Position)
                        && matches!(delete.file_format, IcebergDeleteFileFormat::Puffin)
                })
                .map(|delete| delete.path.clone())
                .collect::<Vec<_>>();
            if selected.is_empty() || (!rewrite_all && selected.len() < min_inputs) {
                continue;
            }
            let group_digest = position_group_digest(&file.path, &selected);
            groups.push(IcebergFrozenRewriteGroupV1 {
                group_digest_hex: hex::encode(group_digest),
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key.clone(),
                data_files: vec![file],
                selected_position_delete_files: selected,
                owned_data_delete_files: Vec::new(),
            });
        }
        return bounded_groups(groups);
    }

    let mut by_partition =
        BTreeMap::<(Option<i32>, Option<String>), Vec<IcebergDataFileInfo>>::new();
    for file in files {
        by_partition
            .entry((file.partition_spec_id, file.partition_key.clone()))
            .or_default()
            .push(file);
    }
    let mut groups = by_partition
        .into_iter()
        .map(|((partition_spec_id, partition_key), mut data_files)| {
            data_files.sort_by(|left, right| left.path.cmp(&right.path));
            let group_digest =
                data_group_digest(partition_spec_id, partition_key.as_deref(), &data_files);
            IcebergFrozenRewriteGroupV1 {
                group_digest_hex: hex::encode(group_digest),
                partition_spec_id,
                partition_key,
                data_files,
                selected_position_delete_files: Vec::new(),
                owned_data_delete_files: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    assign_data_delete_owners(&mut groups);
    bounded_groups(groups)
}

/// Assign every live delete file to one deterministic data-rewrite group.
/// Read sources retain all applicable delete dependencies; this map is used
/// only by the single aggregate replacement snapshot.
fn assign_data_delete_owners(groups: &mut [IcebergFrozenRewriteGroupV1]) {
    let mut owners = BTreeMap::<String, usize>::new();
    for (index, group) in groups.iter().enumerate() {
        for path in group
            .data_files
            .iter()
            .flat_map(|file| file.delete_files.iter().map(|delete| delete.path.as_str()))
        {
            match owners.get(path) {
                Some(existing) if groups[*existing].group_digest_hex <= group.group_digest_hex => {}
                _ => {
                    owners.insert(path.to_string(), index);
                }
            }
        }
    }
    for (path, owner) in owners {
        groups[owner].owned_data_delete_files.push(path);
    }
    for group in groups {
        group.owned_data_delete_files.sort();
        group.owned_data_delete_files.dedup();
    }
}

fn bounded_groups(
    mut groups: Vec<IcebergFrozenRewriteGroupV1>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    groups.sort_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex));
    if groups.len() > REWRITE_ARTIFACT_MAX_GROUPS {
        return Err(exhausted("Iceberg rewrite exceeds the 4096 cohort limit"));
    }
    if groups
        .windows(2)
        .any(|pair| pair[0].group_digest_hex == pair[1].group_digest_hex)
    {
        return Err(invalid("Iceberg rewrite group digest collision"));
    }
    Ok(groups)
}

pub(crate) fn cohort_plans_from_artifact(
    request: &ConnectorDistributedRewritePlanningRequest,
    artifact_digest: [u8; 32],
    artifact_location: &str,
    groups: &[IcebergFrozenRewriteGroupV1],
    input_schema: SchemaRef,
) -> Result<Vec<ConnectorDistributedRewriteCohortPlan>, ConnectorError> {
    let intent = match request.operation() {
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
            ConnectorWriteIntent::Overwrite
        }
        ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
            ConnectorWriteIntent::RowDelta
        }
    };
    groups
        .iter()
        .map(|group| {
            let group_digest = decode_digest(&group.group_digest_hex, "Iceberg rewrite group")?;
            let cohort_id = ConnectorWriteCohortId::derive(
                request.operation_id(),
                b"iceberg-distributed-rewrite-group",
                group_digest,
            )?;
            let payload = IcebergRewriteGroupPayloadV1 {
                version: GROUP_PAYLOAD_VERSION,
                group_digest_hex: group.group_digest_hex.clone(),
                artifact_digest_hex: hex::encode(artifact_digest),
                artifact_location: artifact_location.to_string(),
            };
            let payload = canonical_payload(&payload)?;
            let source_payload = decode_group_payload(&payload)?;
            let source = super::provider::frozen_rewrite_source_table_handle(
                request.operation().table(),
                request.operation(),
                source_payload,
            )?;
            ConnectorDistributedRewriteCohortPlan::try_new(
                cohort_id,
                source,
                intent,
                input_schema.clone(),
                arrow_schema_digest(&input_schema),
                payload,
                group_digest,
            )
        })
        .collect()
}

pub(crate) fn rewrite_input_schema(
    operation: &ConnectorDistributedRewriteOperation,
    physical_schema: SchemaRef,
) -> SchemaRef {
    match operation {
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => physical_schema,
        ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
            Arc::new(Schema::new(vec![
                Field::new("_file", DataType::Utf8, false),
                Field::new("_pos", DataType::Int64, false),
            ]))
        }
    }
}

pub(crate) fn artifact_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.artifact.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.finalize().into()
}

fn artifact_part_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.artifact-part.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.finalize().into()
}

fn rewrite_state_digest(
    table_uuid: &[u8],
    metadata_location: &str,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(STATE_DOMAIN);
    digest_bytes(&mut hash, table_uuid);
    digest_bytes(&mut hash, metadata_location.as_bytes());
    hash.update(base_snapshot_id.unwrap_or(-1).to_be_bytes());
    hash.update(schema_id.to_be_bytes());
    hash.update(default_spec_id.to_be_bytes());
    hash.finalize().into()
}

fn data_group_digest(
    spec_id: Option<i32>,
    partition_key: Option<&str>,
    files: &[IcebergDataFileInfo],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(GROUP_DOMAIN);
    hash.update(b"data\0");
    hash.update(spec_id.unwrap_or(-1).to_be_bytes());
    digest_bytes(&mut hash, partition_key.unwrap_or_default().as_bytes());
    for file in files {
        digest_bytes(&mut hash, file.path.as_bytes());
        hash.update(file.size.to_be_bytes());
        hash.update(file.row_count.unwrap_or(-1).to_be_bytes());
        for delete in &file.delete_files {
            digest_bytes(&mut hash, delete.path.as_bytes());
        }
    }
    hash.finalize().into()
}

fn position_group_digest(data_path: &str, delete_paths: &[String]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(GROUP_DOMAIN);
    hash.update(b"position-delete\0");
    digest_bytes(&mut hash, data_path.as_bytes());
    for delete in delete_paths {
        digest_bytes(&mut hash, delete.as_bytes());
    }
    hash.finalize().into()
}

fn arrow_schema_digest(schema: &SchemaRef) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.arrow-schema.v1\0");
    digest_bytes(&mut hash, format!("{schema:?}").as_bytes());
    hash.finalize().into()
}

fn canonical_payload<T: Serialize>(value: &T) -> Result<Bytes, ConnectorError> {
    canonical_json(value, "Iceberg rewrite payload")
}

/// Canonical JSON v1 used for persisted provider artifacts and bounded SPI
/// envelopes. `serde_json` may inherit insertion order from a map source, so
/// it is not sufficient by itself for a digest-bearing artifact. Sorting at
/// every object level also covers `IcebergDataFileInfo::column_stats`.
fn canonical_json<T: Serialize>(value: &T, label: &str) -> Result<Bytes, ConnectorError> {
    let value = serde_json::to_value(value)
        .map_err(|error| internal(format!("encode {label}: {error}")))?;
    let mut out = Vec::new();
    write_canonical_json(&value, &mut out)?;
    Ok(Bytes::from(out))
}

fn decode_canonical_json<T>(payload: &[u8], label: &str) -> Result<T, ConnectorError>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let decoded = serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode {label}: {error}")))?;
    if canonical_json(&decoded, label)?.as_ref() != payload {
        return Err(invalid(format!("{label} is not canonical JSON v1")));
    }
    Ok(decoded)
}

fn write_canonical_json(value: &Value, out: &mut Vec<u8>) -> Result<(), ConnectorError> {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(number) => out.extend_from_slice(number.to_string().as_bytes()),
        Value::String(string) => {
            let encoded = serde_json::to_string(string)
                .map_err(|error| internal(format!("encode canonical JSON string: {error}")))?;
            out.extend_from_slice(encoded.as_bytes());
        }
        Value::Array(values) => {
            out.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical_json(value, out)?;
            }
            out.push(b']');
        }
        Value::Object(values) => {
            out.push(b'{');
            let mut sorted = values.iter().collect::<Vec<_>>();
            sorted.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, value)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                let encoded = serde_json::to_string(key)
                    .map_err(|error| internal(format!("encode canonical JSON key: {error}")))?;
                out.extend_from_slice(encoded.as_bytes());
                out.push(b':');
                write_canonical_json(value, out)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}

fn decode_digest(value: &str, context: &str) -> Result<[u8; 32], ConnectorError> {
    let bytes =
        hex::decode(value).map_err(|error| invalid(format!("decode {context} digest: {error}")))?;
    bytes
        .try_into()
        .map_err(|_| invalid(format!("{context} digest has invalid length")))
}

fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::scan_model::{
        IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    };

    #[test]
    fn data_groups_are_partition_owned_and_stably_sorted() {
        let mut a = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
        a.partition_spec_id = Some(2);
        a.partition_key = Some("{\"day\":1}".to_string());
        let mut b = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 20, 2);
        b.partition_spec_id = Some(2);
        b.partition_key = Some("{\"day\":1}".to_string());
        let mut c = IcebergDataFileInfo::for_test("s3://bucket/c.parquet", 30, 3);
        c.partition_spec_id = Some(2);
        c.partition_key = Some("{\"day\":2}".to_string());

        let groups = group_data_files(vec![c, b, a], false, None).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .iter()
                .map(|group| group.data_files.len())
                .sum::<usize>(),
            3
        );
        assert!(
            groups
                .iter()
                .all(|group| group.selected_position_delete_files.is_empty())
        );
        for group in groups {
            assert!(
                group
                    .data_files
                    .windows(2)
                    .all(|pair| pair[0].path < pair[1].path)
            );
        }
    }

    #[test]
    fn data_rewrite_assigns_each_shared_delete_file_one_canonical_owner() {
        let delete = IcebergDeleteFileInfo {
            path: "s3://bucket/shared-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(8),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(3),
            partition_spec_id: Some(0),
            partition_key: None,
            equality_column_names: vec!["id".to_string()],
            equality_field_ids: vec![1],
        };
        let mut a = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
        a.partition_key = Some("{\"day\":1}".to_string());
        a.delete_files.push(delete.clone());
        let mut b = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 10, 1);
        b.partition_key = Some("{\"day\":2}".to_string());
        b.delete_files.push(delete);

        let groups = group_data_files(vec![b, a], false, None).expect("frozen groups");
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .iter()
                .flat_map(|group| group.owned_data_delete_files.iter())
                .filter(|path| path.as_str() == "s3://bucket/shared-delete.parquet")
                .count(),
            1
        );
    }

    #[test]
    fn artifact_digest_is_content_addressed() {
        assert_eq!(artifact_digest(b"same"), artifact_digest(b"same"));
        assert_ne!(artifact_digest(b"same"), artifact_digest(b"different"));
    }

    #[test]
    fn canonical_json_sorts_nested_map_keys() {
        let mut first = HashMap::new();
        first.insert("z".to_string(), vec![1_u8]);
        first.insert("a".to_string(), vec![2_u8]);
        let mut second = HashMap::new();
        second.insert("a".to_string(), vec![2_u8]);
        second.insert("z".to_string(), vec![1_u8]);
        assert_eq!(
            canonical_json(&first, "test").unwrap(),
            canonical_json(&second, "test").unwrap()
        );
    }

    #[test]
    fn artifact_parts_never_cross_the_fixed_part_limit() {
        let groups = (0..3)
            .map(|index| IcebergFrozenRewriteGroupV1 {
                group_digest_hex: format!("{index:064x}"),
                partition_spec_id: None,
                partition_key: None,
                data_files: vec![IcebergDataFileInfo::for_test(
                    &format!("file:///warehouse/{index}.parquet"),
                    1,
                    1,
                )],
                selected_position_delete_files: Vec::new(),
                owned_data_delete_files: Vec::new(),
            })
            .collect::<Vec<_>>();
        let parts = split_artifact_parts(&groups).unwrap();
        assert_eq!(parts.len(), 1);
        assert!(
            canonical_json(&parts[0], "test").unwrap().len() <= REWRITE_ARTIFACT_MAX_PART_BYTES
        );
    }
}
